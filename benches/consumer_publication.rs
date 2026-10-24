// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Same-binary controls for the outer embedded publication owner. Timing starts
//! before with_write and again inside its callback. Callback time is a measured
//! lower bound on exclusive ownership, not the inner selector's lock duration.

#[path = "consumer_publication/probes.rs"]
mod probes;

#[path = "consumer_publication/workload.rs"]
mod workload;

use hawdb::*;
use serde_json::{json, Value as Json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Barrier, Mutex};
use std::time::{Duration, Instant};

fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap()
}

fn latency(values: &[u64]) -> Json {
    if values.is_empty() {
        return json!({"count": 0, "samples_ns": values});
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let at = |percent: usize| sorted[(sorted.len() * percent).div_ceil(100) - 1];
    json!({"count": values.len(), "p50_ns": at(50), "p95_ns": at(95),
        "p99_ns": at(99), "max_ns": sorted.last(), "samples_ns": values})
}

fn exclusive<T>(
    owner: &DatabasePublicationOwner<Database>,
    f: impl FnOnce(&mut Database) -> T,
) -> (T, u64, u64) {
    let requested = Instant::now();
    owner
        .with_write(|database| {
            let entered = Instant::now();
            let wait = ns(requested);
            let result = f(database);
            Ok((result, wait, ns(entered)))
        })
        .unwrap()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Disabled,
    Legacy,
    Covered,
    ScopedBlocking,
    Detached,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Legacy => "legacy",
            Self::Covered => "covered",
            Self::ScopedBlocking => "scoped-blocking",
            Self::Detached => "detached",
        }
    }
}

fn update(database: &mut Database, writer: usize, value: usize) {
    database
        .query_sql_with_params(
            "UPDATE foreground SET value=$1 WHERE id=$2",
            &[
                Value::Int(value.try_into().unwrap()),
                Value::Int(writer.try_into().unwrap()),
            ],
        )
        .unwrap();
}

struct WriterStart {
    captured: Vec<SyncSender<()>>,
    committed: Mutex<Receiver<()>>,
}
impl WriterStart {
    fn release(&self) {
        for start in &self.captured {
            start.send(()).unwrap();
        }
        let committed = self.committed.lock().unwrap();
        for _ in &self.captured {
            committed.recv_timeout(Duration::from_secs(30)).unwrap();
        }
    }
}

fn attempt(
    owner: &DatabasePublicationOwner<Database>,
    consumer: &mut SearchProjectionConsumer,
    mode: Mode,
    committed: &AtomicUsize,
    probe: &probes::Probe,
    writer_start: Option<&WriterStart>,
) -> Json {
    let whole_phase = probe.begin("attempt");
    let mut observations = Vec::new();
    let old = consumer.projection().receipt().source_epoch;
    let started = Instant::now();
    let mut capture_wait = 0;
    let mut capture_held = 0;
    let mut preparation = 0;
    let mut captured_epoch = None;
    let mut staging_advance_calls = None;
    let mut source_at_publication = None;
    let mut reserve_wait = 0;
    let mut reserve_held = 0;
    let mut construction = 0;
    let mut construction_writes_before = 0;
    let mut construction_writes_after = 0;
    let (outcome, wait, held) = if mode == Mode::Legacy {
        exclusive(owner, |db| {
            workload::legacy(db, consumer);
            "published"
        })
    } else {
        let (session, wait, held) = exclusive(owner, |db| {
            workload::begin(db, consumer, mode != Mode::Covered)
        });
        capture_wait = wait;
        capture_held = held;
        captured_epoch = Some(session.source_epoch());
        if let Some(start) = writer_start {
            start.release();
        }
        let coverage_phase = probe.begin("coverage_preparation");
        let outside = Instant::now();
        let (covered, calls) = workload::prepare(session, mode != Mode::Covered);
        staging_advance_calls = Some(calls);
        preparation = ns(outside);
        observations.extend(probe.finish(coverage_phase));
        if mode == Mode::Detached {
            let (reserved, wait, held) = exclusive(owner, |db| {
                db.prepare_search_projection_consumer_covered_update(
                    consumer,
                    covered,
                    ProjectionSourceDurability::RequireKnownDurable,
                )
                .unwrap()
            });
            reserve_wait = wait;
            reserve_held = held;
            construction_writes_before = committed.load(Ordering::Acquire);
            let construction_phase = probe.begin("native_construction");
            let outside = Instant::now();
            let prepared = reserved.prepare();
            construction = ns(outside);
            observations.extend(probe.finish(construction_phase));
            construction_writes_after = committed.load(Ordering::Acquire);
            let (published, wait, held) = exclusive(owner, |db| {
                source_at_publication = Some(db.commit_epoch().unwrap());
                db.publish_search_projection_consumer_prepared_update(prepared)
                    .unwrap()
            });
            drop(published);
            ("published", wait, held)
        } else {
            exclusive(owner, |db| {
                source_at_publication = Some(db.commit_epoch().unwrap());
                match db.publish_search_projection_consumer_covered_update(
                    consumer,
                    covered,
                    ProjectionSourceDurability::RequireKnownDurable,
                ) {
                    Ok(_) => "published",
                    Err(ProjectionConsumerCatchUpError::Publication(
                        ProjectionPublicationError::Dependency(_),
                    )) => "post_e_refusal",
                    Err(error) => panic!(
                        "unexpected publication failure: {error}; base={old}, captured={captured_epoch:?}, current={source_at_publication:?}"
                    ),
                }
            })
        }
    };
    let new = consumer.projection().receipt().source_epoch;
    let (cleanup, cleanup_wait, cleanup_held) = exclusive(owner, |db| {
        db.cleanup_search_projection_consumer(consumer, &Default::default())
            .unwrap()
    });
    assert_eq!(cleanup.failed_generations, 0, "{cleanup:?}");
    observations.extend(probe.finish(whole_phase));
    json!({
        "phase_observations": observations,
        "outcome": outcome, "watermark_before": old, "watermark_after": new,
        "advanced": new > old, "elapsed_ns": ns(started),
        "captured_source_epoch": captured_epoch,
        "staging_advance_calls": staging_advance_calls,
        "staging_advance_call_limit": workload::STAGING_ADVANCE_CALL_LIMIT,
        "source_epoch_at_publication": source_at_publication,
        "committed_post_e_advance": new > old && captured_epoch.zip(source_at_publication).is_some_and(|(e, current)| current > e),
        "reservation_wait_plus_entry_ns": reserve_wait, "reservation_exclusive_callback_ns": reserve_held,
        "construction_outside_owner_ns": construction,
        "writes_before_native_construction": construction_writes_before,
        "writes_after_native_construction": construction_writes_after,
        "capture_wait_plus_entry_ns": capture_wait, "capture_exclusive_callback_ns": capture_held,
        "preparation_outside_owner_ns": preparation,
        "publication_wait_plus_entry_ns": wait, "publication_exclusive_callback_ns": held,
        "cleanup_wait_plus_entry_ns": cleanup_wait, "cleanup_exclusive_callback_ns": cleanup_held,
        "reclaimed_generations": cleanup.reclaimed_generations,
    })
}

struct Case {
    root: PathBuf,
    mode: Mode,
    documents: usize,
    payload: usize,
    writers: usize,
    writes: usize,
    quiet: usize,
    sustained: bool,
    capture_before_writers: bool,
    readers: usize,
    reads_per_reader: usize,
}

fn run(case: Case) -> Json {
    let Case {
        root,
        mode,
        documents,
        payload,
        writers,
        writes,
        quiet,
        sustained,
        capture_before_writers,
        readers,
        reads_per_reader,
    } = case;
    let publication_limit = if sustained { 4096 } else { 16 };
    let checkpoint_interval = 16_384;
    // create_dir refuses existing paths; cleanup is confined to this fixture.
    std::fs::create_dir(&root).unwrap();
    let retention_entries = writes + quiet + 64 + if sustained { publication_limit } else { 0 };
    let (database, mut consumer) =
        workload::setup(&root, documents, payload, writers, retention_entries);
    let initial = consumer.projection().receipt().source_epoch;
    let owner = DatabasePublicationOwner::new(database);
    let probe = probes::Probe {
        enabled: readers > 0,
        clock: Instant::now(),
    };
    let concurrent_phase = probe.begin("concurrent_phase");
    let ready = Barrier::new(writers + readers + 2);
    let (start_senders, start_receivers): (Vec<_>, Vec<_>) =
        (0..writers).map(|_| sync_channel(1)).unzip();
    let (first_commit, committed) = sync_channel(writers);
    let writer_start = WriterStart {
        captured: start_senders,
        committed: Mutex::new(committed),
    };
    let active = AtomicUsize::new(writers);
    let completed = AtomicUsize::new(0);
    let checkpoint_writes = AtomicUsize::new(0);
    let checkpoint_samples = Mutex::new(Vec::new());
    let (workers, read_workers, attempts, triggers, publisher_exit_active, elapsed, elapsed_all) =
        std::thread::scope(|scope| {
            let handles = start_receivers
                .into_iter()
                .enumerate()
                .map(|(writer, start)| {
                    let owner = &owner;
                    let ready = &ready;
                    let active = &active;
                    let completed = &completed;
                    let checkpoint_writes = &checkpoint_writes;
                    let checkpoint_samples = &checkpoint_samples;
                    let first_commit = first_commit.clone();
                    scope.spawn(move || {
                        let mut samples = Vec::new();
                        ready.wait();
                        if capture_before_writers {
                            start.recv_timeout(Duration::from_secs(30)).unwrap();
                        }
                        for sequence in (writer..writes).step_by(writers) {
                            let started = Instant::now();
                            let (_, wait, held) = exclusive(owner, |db| {
                                update(db, writer, sequence + 1);
                                if sustained {
                                    let count =
                                        checkpoint_writes.fetch_add(1, Ordering::Relaxed) + 1;
                                    if count.is_multiple_of(checkpoint_interval) {
                                        // Exercise normal maintenance instead of raising the live-view ceiling.
                                        let started = Instant::now();
                                        db.checkpoint().unwrap();
                                        checkpoint_samples
                                            .lock()
                                            .unwrap()
                                            .push((count, ns(started)));
                                    }
                                }
                            });
                            samples.push((ns(started), wait, held));
                            completed.fetch_add(1, Ordering::Release);
                            // Outside the measured write interval; writers continue immediately.
                            if capture_before_writers && sequence == writer {
                                first_commit.send(()).unwrap();
                            }
                        }
                        active.fetch_sub(1, Ordering::Release);
                        samples
                    })
                })
                .collect::<Vec<_>>();
            let read_handles = (0..readers)
                .map(|worker| {
                    let reader = probes::Reader {
                        owner: &owner,
                        barrier: &ready,
                        active_writers: &active,
                        committed_writes: &completed,
                        probe: &probe,
                        worker,
                        requests: reads_per_reader,
                        documents,
                        payload,
                    };
                    scope.spawn(move || reader.run())
                })
                .collect::<Vec<_>>();
            let publisher = scope.spawn(|| {
                ready.wait();
                let mut attempts = Vec::new();
                let mut triggers = Vec::new();
                for sequence in 1..=publication_limit {
                    if sustained && active.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    // All modes perform the same trigger writes. Each enabled
                    // attempt must build a new E even after foreground writers drain.
                    let (_, wait, held) = exclusive(&owner, |db| {
                        db.query_sql_with_params(
                            "UPDATE maintenance_trigger SET value=$1 WHERE id=0",
                            &[Value::Int(sequence.try_into().unwrap())],
                        )
                        .unwrap();
                    });
                    triggers.push((wait, held));
                    if capture_before_writers && sequence == 1 && mode == Mode::Disabled {
                        writer_start.release();
                    }
                    if mode != Mode::Disabled {
                        let before = completed.load(Ordering::Acquire);
                        let mut row = attempt(
                            &owner,
                            &mut consumer,
                            mode,
                            &completed,
                            &probe,
                            (capture_before_writers && sequence == 1).then_some(&writer_start),
                        );
                        row["writes_before"] = json!(before);
                        row["writes_after"] = json!(completed.load(Ordering::Acquire));
                        row["writers_active_after"] = json!(active.load(Ordering::Acquire));
                        attempts.push(row);
                    }
                }
                (attempts, triggers, active.load(Ordering::Acquire))
            });
            let started = Instant::now();
            ready.wait();
            let workers = handles
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>();
            let elapsed = ns(started);
            let (attempts, triggers, publisher_exit_active) = publisher.join().unwrap();
            let read_workers = read_handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>();
            (
                workers,
                read_workers,
                attempts,
                triggers,
                publisher_exit_active,
                elapsed,
                ns(started),
            )
        });
    let concurrent_observation = probe.finish(concurrent_phase);
    let read_results = probes::summarize(read_workers, &attempts);
    assert_eq!(
        read_results["requests"].as_u64().unwrap() as usize,
        readers * reads_per_reader
    );
    assert_eq!(completed.load(Ordering::Acquire), writes);
    let source_after_writers = owner.with_read(Database::commit_epoch).unwrap();
    assert_eq!(
        source_after_writers,
        initial + writes as u64 + triggers.len() as u64
    );
    if matches!(mode, Mode::ScopedBlocking | Mode::Detached) {
        if !sustained {
            assert_eq!(attempts.len(), 16);
        }
        assert!(attempts
            .iter()
            .all(|row| row["outcome"] == "published" && row["advanced"] == true));
    }
    owner
        .with_write(|db| {
            let rows = db
                .query_sql("SELECT id,value FROM foreground ORDER BY id")?
                .rows;
            assert_eq!(rows.len(), writers);
            for (writer, row) in rows.iter().enumerate() {
                let last = writer + ((writes - 1 - writer) / writers) * writers + 1;
                assert_eq!(row["id"], Value::Int(writer.try_into().unwrap()));
                assert_eq!(row["value"], Value::Int(last.try_into().unwrap()));
            }
            Ok(())
        })
        .unwrap();
    if capture_before_writers && mode == Mode::Detached {
        assert_eq!(attempts[0]["committed_post_e_advance"], true);
    }
    let watermark_after_writers = consumer.projection().receipt().source_epoch;
    let active_advances = attempts
        .iter()
        .filter(|row| row["advanced"] == true && row["writers_active_after"].as_u64().unwrap() > 0)
        .count();
    let mut quiet_samples = Vec::new();
    if mode != Mode::Disabled {
        // Successful publication timing is measured separately: rejected work
        // cannot serve as evidence of a faster successful critical section.
        for sequence in 0..quiet {
            owner
                .with_write(|db| {
                    update(db, 0, writes + sequence + 1);
                    Ok(())
                })
                .unwrap();
            let row = attempt(&owner, &mut consumer, mode, &completed, &probe, None);
            assert_eq!(row["outcome"], "published");
            assert_eq!(row["advanced"], true);
            quiet_samples.push(row);
        }
        assert_eq!(
            consumer.projection().receipt().source_epoch,
            owner.with_read(Database::commit_epoch).unwrap()
        );
    }
    let index = consumer.search_index().unwrap();
    assert_eq!(index.document_count(), documents);
    for id in 0..documents {
        assert_eq!(
            index.document(&format!("m{id}")).unwrap().content,
            "x".repeat(payload)
        );
    }
    let transaction_samples = workers.iter().flatten().map(|s| s.0).collect::<Vec<_>>();
    let waits = workers.iter().flatten().map(|s| s.1).collect::<Vec<_>>();
    let holds = workers.iter().flatten().map(|s| s.2).collect::<Vec<_>>();
    let publication = quiet_samples
        .iter()
        .map(|row| row["publication_exclusive_callback_ns"].as_u64().unwrap())
        .collect::<Vec<_>>();
    let checkpoint_samples = checkpoint_samples.into_inner().unwrap();
    if sustained {
        assert_eq!(checkpoint_writes.load(Ordering::Relaxed), writes);
        assert_eq!(checkpoint_samples.len(), writes / checkpoint_interval);
    }
    let post_e_advances = attempts
        .iter()
        .filter(|row| {
            row["committed_post_e_advance"] == true
                && row["writers_active_after"].as_u64().unwrap() > 0
        })
        .count();
    let publisher_cap_reached = sustained && publisher_exit_active > 0;
    let mut result = json!({
        "schema": "consumer-publication-qualification-v2", "mode": mode.name(),
        "capture_before_writers": capture_before_writers,
        "documents": documents, "payload_bytes_per_document": payload, "writers": writers, "writes": writes,
        "durability": "SyncOnEveryWrite", "automatic_checkpoint_max_age_seconds": 3600,
        "changefeed_retention_entries": retention_entries, "changefeed_retention_bytes": 64 << 20,
        "governor_memory_bytes": 512_u64 << 20, "background_tasks": 1, "io_slots": [4,1],
        "data_shape": "fixed Memory projection plus disjoint relational foreground counters and shared maintenance trigger writes",
        "control": if sustained {"separate fixed-work progress cohort; publish until writers finish or the explicit attempt ceiling is reached"} else {"same binary; scoped-blocking and detached perform the same full native construction and 16 concurrent-phase publications; disabled performs no projection maintenance"},
        "elapsed_concurrent_phase_ns": elapsed_all, "maintenance_trigger_samples_wait_hold_ns": triggers,
        "maintenance_trigger_writes": triggers.len(),
        "elapsed_writer_ns": elapsed, "writes_per_second": writes as f64 * 1e9 / elapsed as f64,
        "foreground_write": latency(&transaction_samples), "writer_wait_plus_entry": latency(&waits),
        "writer_exclusive_callback": latency(&holds), "quiescent_publication_exclusive_callback": latency(&publication),
        "source_epoch_before": initial, "source_epoch_after_concurrent_phase": source_after_writers,
        "watermark_after_concurrent_phase": watermark_after_writers, "watermark_advances_with_active_writers": active_advances,
        "publication_attempts": attempts, "quiescent_publication_samples": quiet_samples,
        "concurrent_phase_publication_attempts": attempts.len(),
        "simultaneous_committed_post_e_advances": post_e_advances,
        "writer_samples": workers, "verified_documents": documents,
        "simultaneous_progress_qualified": post_e_advances >= 2,
        "qualification_note": "Progress requires successful captured-E publication after a real post-E commit while writers remain active; repeated progress requires at least two such advances. Quiet samples are separate.",
        "lock_measurement": "wait includes entry overhead; exclusive callback is a lower bound on outer owner hold time, not inner selector mutex time",
    });
    result["foreground_read"] = read_results;
    result["readers"] = json!(readers);
    result["reads_per_reader"] = json!(reads_per_reader);
    result["concurrent_phase_observation"] = json!(concurrent_observation);
    result["sustained_workload"] = json!({
        "enabled": sustained, "publication_attempt_limit": publication_limit,
        "publisher_exit_active_writers": publisher_exit_active, "publisher_cap_reached": publisher_cap_reached,
        "checkpoint_every_foreground_writes": sustained.then_some(checkpoint_interval),
        "checkpoint_samples_write_count_elapsed_ns": checkpoint_samples,
        "checkpoint_admission": "caller-triggered blocking API; not a phase heap or governor-accounting claim",
        "qualified": sustained && post_e_advances >= 2 && !publisher_cap_reached,
    });
    drop(consumer);
    drop(owner);
    std::fs::remove_dir_all(root).unwrap();
    result
}

fn main() {
    let mut mode = Mode::Detached;
    let mut writers = 1;
    let mut documents = 32;
    let mut payload = 128;
    let mut writes = 4096;
    let mut quiet = 32;
    let mut sustained = false;
    let mut capture_before_writers = false;
    let mut readers = 0;
    let mut reads_per_reader = 512;
    let mut root = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--bench" {
            continue;
        }
        let value = args.next().expect("argument requires a value");
        match arg.as_str() {
            "--mode" => {
                mode = match value.as_str() {
                    "disabled" => Mode::Disabled,
                    "legacy" => Mode::Legacy,
                    "covered" => Mode::Covered,
                    "scoped-blocking" => Mode::ScopedBlocking,
                    "detached" => Mode::Detached,
                    _ => panic!("unknown publication mode"),
                }
            }
            "--writers" => writers = value.parse().unwrap(),
            "--readers" => readers = value.parse().unwrap(),
            "--reads-per-reader" => reads_per_reader = value.parse().unwrap(),
            "--documents" => documents = value.parse().unwrap(),
            "--payload" => payload = value.parse().unwrap(),
            "--writes" => writes = value.parse().unwrap(),
            "--quiet-samples" => quiet = value.parse().unwrap(),
            "--sustained" => sustained = value.parse().unwrap(),
            "--capture-before-writers" => capture_before_writers = value.parse().unwrap(),
            "--root" => root = Some(PathBuf::from(value)),
            _ => panic!("unknown argument: {arg}"),
        }
    }
    assert!([1, 4, 8].contains(&writers));
    assert!(readers <= 8 && (1..=4096).contains(&reads_per_reader));
    assert!(!sustained || readers == 0);
    assert!((1..=4096).contains(&documents) && (1..=2048).contains(&payload));
    assert!((writers..=100_000).contains(&writes) && (1..=1024).contains(&quiet));
    assert!(!sustained || mode == Mode::Detached);
    assert!(
        !capture_before_writers
            || (!sustained && readers == 0 && matches!(mode, Mode::Disabled | Mode::Detached))
    );
    let root = root.expect("--root must name a new fixture directory on the measured filesystem");
    println!(
        "{}",
        run(Case {
            root,
            mode,
            documents,
            payload,
            writers,
            writes,
            quiet,
            sustained,
            capture_before_writers,
            readers,
            reads_per_reader,
        })
    );
}
