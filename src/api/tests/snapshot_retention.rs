// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::api::DatabaseReadSnapshot;
use crate::{ConcurrentTransactionOptions, StorageResidencyMode, WalGroupCommitConfig};
use std::num::NonZeroU64;
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::{Duration, Instant};

fn fixture(path: &std::path::Path, mode: StorageResidencyMode) -> Database {
    let mut db = Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: mode,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 1, value: 0})").unwrap();
    db.query("CREATE (:Memory {id: 2, value: 0})").unwrap();
    db.query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    for id in 1..=2 {
        db.query_sql_with_params(
            "INSERT INTO records (id, body) VALUES ($1, $2)",
            &[Value::Int(id), Value::String("original".repeat(2048))],
        )
        .unwrap();
    }
    db.checkpoint().unwrap();
    assert!(db
        .storage_artifact_file_sizes()
        .unwrap()
        .iter()
        .any(|(name, bytes)| name.starts_with("relational-overflow-")
            && name.ends_with(".extents.hawdb")
            && *bytes > 0));
    db
}

fn generation(db: &Database) -> (u64, u64) {
    let view = db.published_read_view().unwrap();
    (
        view.physical_generation().unwrap().0,
        view.graph_generation().unwrap().0,
    )
}

fn assert_roots(db: &Database, expected: &BTreeSet<(u64, u64)>) {
    let files = db.storage_artifact_file_sizes().unwrap();
    for (prefix, graph) in [("canonical.", true), ("checkpoint.", false)] {
        let expected = expected
            .iter()
            .map(|&(checkpoint, canonical)| if graph { canonical } else { checkpoint })
            .collect::<BTreeSet<_>>();
        let actual = files
            .keys()
            .filter_map(|file| {
                file.strip_prefix(prefix)?
                    .strip_suffix(".hawdb")?
                    .parse::<u64>()
                    .ok()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected, "{prefix}");
    }
}

fn checkpoint_roots(db: &mut Database, pinned: &[(u64, u64)]) {
    let previous = generation(db);
    db.checkpoint().unwrap();
    let current = generation(db);
    assert!(current > previous);
    let expected = pinned.iter().copied().chain([previous, current]).collect();
    assert_roots(db, &expected);
}

fn assert_snapshot(snapshot: &mut DatabaseReadTransaction, value: i64, added_column: bool) {
    let row = snapshot
        .query("MATCH (m:Memory {id: 1}) RETURN m.value AS value")
        .unwrap();
    assert_eq!(row.rows[0]["value"], Value::Int(value));
    let body = snapshot
        .query_sql_with_params("SELECT body FROM records WHERE id = $1", &[Value::Int(1)])
        .unwrap();
    assert_eq!(
        body.rows[0]["body"],
        Value::String(if value == 0 {
            "original".repeat(2048)
        } else {
            format!("revision-{value}").repeat(2048)
        })
    );
    let unchanged = snapshot
        .query_sql("SELECT body FROM records WHERE id = 2")
        .unwrap();
    assert_eq!(
        unchanged.rows[0]["body"],
        Value::String("original".repeat(2048))
    );
    let column = snapshot.query_sql("SELECT marker FROM records WHERE id = 1");
    if added_column {
        assert_eq!(column.unwrap().rows[0]["marker"], Value::Null);
    } else {
        assert!(column
            .unwrap_err()
            .to_string()
            .contains("unknown relational column"));
    }
}

#[test]
fn mixed_snapshot_cohorts_reclaim_exact_roots_after_concurrent_final_drop() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("mixed_snapshot_cohorts_{mode:?}"));
        let mut db = fixture(&path, mode);
        let original_epoch = db.commit_epoch().unwrap();
        db.query_sql_with_params(
            "CREATE BRANCH kept FROM main AT REVISION $1 REQUEST KEY 'retained-cohort'",
            &[Value::Int(i64::try_from(original_epoch).unwrap())],
        )
        .unwrap();
        let first = DatabaseReadSnapshot(db.begin_read_transaction().unwrap());
        let first_generation = generation(&db);
        let start = Arc::new(Barrier::new(5));
        let retire = Arc::new(Barrier::new(5));
        let workers = (0..4)
            .map(|_| {
                let mut reader = first.fork(None).unwrap();
                let start = Arc::clone(&start);
                let retire = Arc::clone(&retire);
                std::thread::spawn(move || {
                    start.wait();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        assert_snapshot(&mut reader, 0, false);
                    }));
                    retire.wait();
                    drop(reader);
                    result.unwrap();
                })
            })
            .collect::<Vec<_>>();
        drop(first);
        let mut middle = None;
        let mut middle_generation = None;
        for value in 1..=3 {
            let mut tx = db.begin_transaction().unwrap();
            tx.query_with_params(
                "MATCH (m:Memory {id: 1}) SET m.value = $value",
                &BTreeMap::from([("value".into(), Value::Int(value))]),
            )
            .unwrap();
            tx.query_sql_with_params(
                "UPDATE records SET body = $1 WHERE id = $2",
                &[
                    Value::String(format!("revision-{value}").repeat(2048)),
                    Value::Int(1),
                ],
            )
            .unwrap();
            if value == 1 {
                tx.query_sql("ALTER TABLE records ADD COLUMN marker TEXT")
                    .unwrap();
            }
            tx.commit().unwrap();
            let pinned = [Some(first_generation), middle_generation]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            checkpoint_roots(&mut db, &pinned);
            if value == 1 {
                middle_generation = Some(generation(&db));
                middle = Some(db.begin_read_transaction().unwrap());
            }
        }
        let mut middle = middle.unwrap();
        assert_eq!(
            db.storage_reclamation_watermark()
                .unwrap()
                .oldest_reader_commit_epoch,
            Some(original_epoch)
        );
        start.wait();
        retire.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        // One shared registration retires after its final concurrent holder,
        // while the independent middle reader still protects its own view.
        assert_eq!(
            db.storage_reclamation_watermark()
                .unwrap()
                .oldest_reader_commit_epoch,
            Some(middle.commit_epoch())
        );
        checkpoint_roots(&mut db, &[middle_generation.unwrap()]);
        assert_snapshot(&mut middle, 1, true);
        drop(middle);
        assert_eq!(
            db.storage_reclamation_watermark()
                .unwrap()
                .oldest_reader_commit_epoch,
            None
        );
        checkpoint_roots(&mut db, &[]);
        let mut current = db.begin_read_transaction().unwrap();
        assert_snapshot(&mut current, 3, true);
        drop(current);
        // No handle ever leased the child: durable existence still protects it.
        db.query_sql("USE BRANCH kept").unwrap();
        let mut child = db.begin_read_transaction().unwrap();
        assert_snapshot(&mut child, 0, false);
        drop(child);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn result_budget_rollback_keeps_the_writer_snapshot_until_conflict_retirement() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("snapshot_budget_rollback_{mode:?}"));
        drop(fixture(&path, mode));
        let database = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: mode,
                max_read_result_rows: Some(1),
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let db = database.into_concurrent();
        let epoch = db.commit_epoch().unwrap();
        let old_generation = db
            .published_read_view()
            .unwrap()
            .graph_generation()
            .unwrap()
            .0;
        let mut writer = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        writer
            .query("MATCH (m:Memory {id: 1}) SET m.value = 7")
            .unwrap();
        let error = writer
            .query("MATCH (m:Memory) SET m.value = 99 RETURN m.id AS id")
            .unwrap_err();
        assert!(error.to_string().contains("row"), "{error}");
        for (id, value) in [(1, 7), (2, 0)] {
            let rows = writer
                .query_with_params(
                    "MATCH (m:Memory {id: $id}) RETURN m.value AS value",
                    &BTreeMap::from([("id".into(), Value::Int(id))]),
                )
                .unwrap();
            assert_eq!(rows.rows[0]["value"], Value::Int(value));
        }
        db.query("MATCH (m:Memory {id: 1}) SET m.value = 2")
            .unwrap();
        for _ in 0..3 {
            db.checkpoint().unwrap();
        }
        assert_eq!(
            db.storage_pressure_snapshot()
                .unwrap()
                .oldest_reader_commit_epoch,
            Some(epoch)
        );
        assert!(active_storage_root(&path)
            .join(format!("canonical.{old_generation}.hawdb"))
            .exists());
        let wal = read_test_wal(&path).unwrap();
        let before = db.commit_epoch().unwrap();
        assert!(writer
            .commit()
            .unwrap_err()
            .is_retryable_transaction_conflict());
        assert_eq!(db.commit_epoch().unwrap(), before);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
        db.checkpoint().unwrap();
        assert!(!active_storage_root(&path)
            .join(format!("canonical.{old_generation}.hawdb"))
            .exists());
        assert_eq!(
            db.storage_pressure_snapshot()
                .unwrap()
                .oldest_reader_commit_epoch,
            None
        );
        assert_eq!(
            db.query("MATCH (m:Memory {id: 1}) RETURN m.value AS value")
                .unwrap()
                .rows[0]["value"],
            Value::Int(2)
        );
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn governor() -> crate::RuntimeGovernor {
    crate::RuntimeGovernor::new(
        crate::RuntimeGovernorConfig {
            cpu_slot_limit: Some(NonZeroUsize::MIN),
            foreground_task_limit: Some(NonZeroUsize::MIN),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..crate::RuntimeGovernorConfig::shared_host()
        },
        crate::RuntimeResourceSnapshot::from_parts(
            crate::RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            crate::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        ),
        crate::IoConcurrencyBudget::new(2, 1),
    )
}

#[test]
fn idle_snapshot_releases_construction_admission_and_accepts_a_fresh_execution_context() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, value: 0})").unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(crate::RuntimeWorkRequest::foreground_query(
            16 * 1024 * 1024,
            1024 * 1024,
        ))
        .unwrap();
    let context = permit.bind_task_context(crate::RuntimeTaskContext::default());
    let mut reader = db.begin_read_transaction_with_context(&context).unwrap();
    let epoch = reader.commit_epoch();
    drop(context);
    drop(permit);
    let idle = governor.snapshot();
    assert_eq!(idle.active_foreground_tasks, 0);
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.admitted_memory_bytes, 0);
    assert_eq!(
        db.storage_reclamation_watermark()
            .unwrap()
            .oldest_reader_commit_epoch,
        Some(epoch)
    );
    db.query("MATCH (m:Memory {id: 1}) SET m.value = 1")
        .unwrap();
    let permit = governor
        .try_admit(crate::RuntimeWorkRequest::foreground_query(
            16 * 1024 * 1024,
            1024 * 1024,
        ))
        .unwrap();
    let context = permit.bind_task_context(crate::RuntimeTaskContext::default());
    let rows = reader
        .query_with_params_context(
            "MATCH (m:Memory {id: 1}) RETURN m.value AS value",
            &BTreeMap::new(),
            &context,
        )
        .unwrap();
    assert_eq!(rows.rows[0]["value"], Value::Int(0));
    drop(rows);
    drop(context);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(reader);
    assert_eq!(
        db.storage_reclamation_watermark()
            .unwrap()
            .oldest_reader_commit_epoch,
        None
    );
}

#[test]
fn queued_snapshot_survives_physical_reclamation_and_refunds_cancelled_admission() {
    for cancelled in [false, true] {
        let path = unique_test_dir(&format!("queued_snapshot_reclamation_{cancelled}"));
        let database = fixture(&path, StorageResidencyMode::OutOfCore);
        let group = WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap();
        let db = crate::ConcurrentDatabase::new_with_wal_group_commit(database, group);
        let governor = governor();
        let context = crate::RuntimeTaskContext::default();
        let before = db.commit_epoch().unwrap();
        let old_view = db.published_read_view().unwrap();
        let old_generation = old_view.graph_generation().unwrap().0;
        let old_checkpoint = old_view.physical_generation().unwrap().0;
        assert_ne!(old_checkpoint, old_generation);
        let mut writer = db
            .begin_admitted_transaction(
                ConcurrentTransactionOptions::optimistic(),
                governor
                    .try_admit(
                        crate::RuntimeWorkRequest::foreground_mutation(16 * 1024 * 1024)
                            .with_result_bytes(1024 * 1024),
                    )
                    .unwrap(),
                context.clone(),
            )
            .unwrap();
        writer
            .query("MATCH (m:Memory {id: 1}) SET m.value = 7")
            .unwrap();
        writer
            .query_sql("UPDATE records SET body = 'queued' WHERE id = 1")
            .unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        db.set_group_commit_enqueue_gate(Arc::clone(&gate)).unwrap();
        let thread = std::thread::spawn(move || writer.commit());
        let deadline = Instant::now() + Duration::from_secs(30);
        while db.wal_group_commit_snapshot().unwrap().submitted_commits == 0 {
            assert!(
                Instant::now() < deadline,
                "writer did not enter the real commit queue"
            );
            std::thread::yield_now();
        }
        for _ in 0..3 {
            db.checkpoint().unwrap();
        }
        assert_eq!(governor.snapshot().active_foreground_tasks, 1);
        assert!(active_storage_root(&path)
            .join(format!("canonical.{old_generation}.hawdb"))
            .exists());
        // G is still current, so C proves that the queued snapshot owns a pin.
        assert!(active_storage_root(&path)
            .join(format!("checkpoint.{old_checkpoint}.hawdb"))
            .exists());
        assert_eq!(db.commit_epoch().unwrap(), before);
        if cancelled {
            context.cancellation().cancel();
        }
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let result = thread.join().unwrap();
        if cancelled {
            assert!(result.unwrap_err().to_string().contains("cancelled"));
        } else {
            result.unwrap();
        }
        let idle = governor.snapshot();
        assert_eq!(idle.active_foreground_tasks, 0);
        assert_eq!(idle.active_cpu_slots, 0);
        assert_eq!(idle.admitted_memory_bytes, 0);
        let mut reader = db.begin_read_transaction().unwrap();
        assert_eq!(reader.commit_epoch(), before + u64::from(!cancelled));
        assert_eq!(
            reader
                .query("MATCH (m:Memory {id: 1}) RETURN m.value AS value")
                .unwrap()
                .rows[0]["value"],
            Value::Int(if cancelled { 0 } else { 7 })
        );
        let sql = reader
            .query_sql("SELECT body FROM records WHERE id = 1")
            .unwrap();
        assert_eq!(
            sql.rows[0]["body"],
            Value::String(if cancelled {
                "original".repeat(2048)
            } else {
                "queued".into()
            })
        );
        drop(reader);
        // A cancelled writer leaves the graph current. Advance it explicitly
        // before expecting reader retirement to make the old bundle reclaimable.
        db.query("CREATE (:RetentionProbe)").unwrap();
        db.checkpoint().unwrap();
        db.checkpoint().unwrap();
        assert!(!active_storage_root(&path)
            .join(format!("canonical.{old_generation}.hawdb"))
            .exists());
        assert!(!active_storage_root(&path)
            .join(format!("checkpoint.{old_checkpoint}.hawdb"))
            .exists());
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
