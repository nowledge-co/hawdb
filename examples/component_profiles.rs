// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual native search and strict durable-write observations. Phase timers
//! exclude validation/reporting; overlapping coordinator counters are not sums.

use hawdb::{
    ConcurrentDatabase, ConcurrentDatabaseMetrics, ConcurrentPhaseMetrics,
    ConcurrentTransactionOptions, Database, RuntimeMemoryReservation, RuntimeTaskContext,
    SearchDocument, SearchHit, SearchHitHydration, SearchMode, SearchOutOfCoreConfig,
    SearchOutOfCoreExecutionContext, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreMetrics, SearchOutOfCoreReader,
    SearchQueryOptions, SearchScoredCandidate, Value, VectorSearchExecutionOptions,
};
use hawdb_qos::{ProcessMemoryProfile, ProcessMemorySnapshot};
use serde_json::{json, Value as Json};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    io::Write,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const WARMUPS: usize = 3;
const LIMIT: usize = 10;
const PAYLOAD_BYTES: usize = 128;
const MEMORY_BYTES: u64 = 64 << 20;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(120);
const SEARCH_WORK_UNITS: usize = 100_000_000;

type ScoreSignature = (String, [u64; 6], [Option<usize>; 2]);

fn fixture_document(id: usize) -> SearchDocument {
    SearchDocument {
        id: format!("doc-{id:08}"),
        title: "component document".into(),
        content: format!(
            "common {} {}",
            if id.is_multiple_of(13) {
                "rare"
            } else {
                "topic"
            },
            "x".repeat(PAYLOAD_BYTES)
        ),
        embedding: Some(
            (0..8)
                .map(|dimension| if dimension == id % 8 { 1.0 } else { 0.1 })
                .collect(),
        ),
        metadata: BTreeMap::from([(
            "space_id".into(),
            if id.is_multiple_of(10) { "cold" } else { "hot" }.into(),
        )]),
    }
}

fn candidate_signature(hit: &SearchScoredCandidate) -> ScoreSignature {
    (
        hit.id.clone(),
        [
            hit.score.to_bits(),
            hit.vector_score.to_bits(),
            hit.text_score.to_bits(),
            hit.rrf_score.to_bits(),
            hit.vector_rrf_score.to_bits(),
            hit.text_rrf_score.to_bits(),
        ],
        [hit.vector_rank, hit.text_rank],
    )
}

fn hit_signature(hit: &SearchHit) -> ScoreSignature {
    (
        hit.id.clone(),
        [
            hit.score.to_bits(),
            hit.vector_score.to_bits(),
            hit.text_score.to_bits(),
            hit.rrf_score.to_bits(),
            hit.vector_rrf_score.to_bits(),
            hit.text_rrf_score.to_bits(),
        ],
        [hit.vector_rank, hit.text_rank],
    )
}

fn selection_signature(mut signature: ScoreSignature) -> ScoreSignature {
    // Document hydration recomputes raw vector relevance, even for lexical-only
    // RRF members. It does not change the selected score or rank contributions.
    signature.1[1] = 0;
    signature
}

fn emit(value: &Json) -> Result<()> {
    let mut output = std::io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn sha256(bytes: &[u8]) -> Option<String> {
    for (program, args) in [("shasum", vec!["-a", "256"]), ("sha256sum", vec![])] {
        let Ok(mut child) = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        else {
            continue;
        };
        let written = child.stdin.take().unwrap().write_all(bytes);
        let result = child.wait_with_output().ok()?;
        if written.is_ok() && result.status.success() {
            let value = String::from_utf8(result.stdout).ok()?;
            return value.split_whitespace().next().map(str::to_owned);
        }
    }
    None
}

fn environment() -> Json {
    let command = |program: &str, args: &[&str]| {
        Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    let binary_sha256 = std::env::current_exe().ok().and_then(|path| {
        for (program, args) in [("shasum", vec!["-a", "256"]), ("sha256sum", vec![])] {
            if let Ok(output) = Command::new(program).args(args).arg(&path).output()
                && output.status.success()
            {
                return String::from_utf8(output.stdout)
                    .ok()
                    .and_then(|text| text.split_whitespace().next().map(str::to_owned));
            }
        }
        None
    });
    json!({"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"debug_assertions":cfg!(debug_assertions),
        "observed_checkout_revision":command("git", &["rev-parse","HEAD"]),
        "observed_checkout_status":command("git", &["status","--porcelain"]),
        "toolchain":command("rustc", &["--version"]),"host_load":fs::read_to_string("/proc/loadavg").ok().or_else(||command("uptime",&[])),
        "cpu":command("sysctl",&["-n","machdep.cpu.brand_string"]).or_else(||command("lscpu",&[])),
        "available_parallelism":std::thread::available_parallelism().ok().map(NonZeroUsize::get),
        "command_args":std::env::args().collect::<Vec<_>>(),"binary_sha256":binary_sha256,
        "compiled_example_sha256":sha256(include_bytes!("component_profiles.rs")),
        "compiled_engine_revision":null,"source_note":"retain tested source/tree, build command and binary; runtime checkout is not engine attestation"})
}

fn directory() -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!(
        "hawdb-component-profile-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir(&path)?;
    emit(
        &json!({"event":"fixture_created","path":path,"failure_policy":"retain fixture on error"}),
    )?;
    Ok(path)
}

fn nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn memory(before: Option<ProcessMemorySnapshot>) -> Json {
    before.zip(ProcessMemorySnapshot::capture().ok()).map_or(Json::Null, |(before, after)| {
        let profile = ProcessMemoryProfile::between(before, after);
        json!({"start_rss_bytes":profile.capabilities.resident_memory.then_some(profile.start_resident_bytes),
            "rss_bytes":profile.capabilities.resident_memory.then_some(profile.steady_resident_bytes),
            "process_lifetime_peak_rss_bytes":profile.capabilities.resident_memory.then_some(profile.peak_resident_bytes),
            "resident_growth_bytes":profile.steady_resident_growth_bytes,"new_lifetime_peak_growth_bytes":profile.lifetime_peak_resident_growth_bytes,
            "minor_page_faults":profile.minor_page_faults,"major_page_faults":profile.major_page_faults,
            "allocator_peak_bytes":null,"scope":"process observations; lifetime peak is not this operation's peak"})
    })
}

fn search_metrics(metrics: &SearchOutOfCoreMetrics) -> Json {
    json!({"segment_range_reads":metrics.segment_range_reads,"segment_bytes_read":metrics.segment_bytes_read,
        "lexical_document_bytes_read":metrics.lexical_document_bytes_read,"metadata_segment_bytes_read":metrics.metadata_segment_bytes_read,
        "vector_bytes_read":metrics.vector_bytes_read,"hydration_segment_bytes_read":metrics.hydration_segment_bytes_read,
        "candidate_block_reads":metrics.candidate_block_reads,"candidate_bytes_read":metrics.candidate_bytes_read,
        "candidate_spill_bytes":metrics.candidate_spill_bytes,"peak_segment_document_bytes":metrics.peak_segment_document_bytes,
        "hydrated_documents":metrics.hydrated_documents,"hydrated_bytes":metrics.hydrated_bytes,
        "streamed_documents":metrics.streamed_documents,"streamed_body_bytes":metrics.streamed_body_bytes,
        "physical_device_io_bytes":null,"scope":"engine requested/decoded ranges, not physical device I/O"})
}

fn profile_search_call<T>(
    search: impl FnOnce(SearchOutOfCoreExecutionContext<'_>) -> Result<T>,
) -> Result<(T, u64)> {
    // Cloning a task preserves its absolute deadline and shared work usage.
    // Independent observations therefore need a new task before timing starts.
    let task = RuntimeTaskContext::with_timeout(SEARCH_TIMEOUT)
        .with_work_limit(SEARCH_WORK_UNITS)
        .with_memory_reservation(RuntimeMemoryReservation::new(MEMORY_BYTES, 1 << 20));
    let execution = SearchOutOfCoreExecutionContext {
        vector_execution_options: VectorSearchExecutionOptions::bounded(
            NonZeroUsize::MIN,
            MEMORY_BYTES as usize,
            Some(&task),
        ),
        ..Default::default()
    };
    let started = Instant::now();
    let output = search(execution)?;
    Ok((output, nanos(started)))
}

fn search(documents: usize, repeats: usize) -> Result<Json> {
    let path = directory()?;
    let started = Instant::now();
    let build_task = RuntimeTaskContext::with_timeout(Duration::from_secs(300))
        .with_memory_reservation(RuntimeMemoryReservation::new(192 << 20, 0));
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            max_segment_uncompressed_bytes: NonZeroU64::new(4 << 20).unwrap(),
            max_record_bytes: NonZeroU64::new(1 << 20).unwrap(),
            lexical_build_memory_bytes: NonZeroU64::new(8 << 20).unwrap(),
            ..Default::default()
        },
        build_task,
    )?;
    for id in 0..documents {
        writer.push(fixture_document(id))?;
    }
    let build = writer.finish()?;
    let fixture_setup_ns = nanos(started);
    let started = Instant::now();
    let reader = SearchOutOfCoreReader::open_with_config(
        &path,
        SearchOutOfCoreConfig {
            max_score_entries: NonZeroUsize::new(documents).unwrap(),
            max_hydrated_documents: NonZeroUsize::new(LIMIT).unwrap(),
            max_hydrated_bytes: NonZeroU64::new(1 << 20).unwrap(),
            max_vector_search_working_bytes: NonZeroUsize::new(MEMORY_BYTES as usize).unwrap(),
            spill_directory: path.join("scratch"),
            ..Default::default()
        },
    )?;
    let open_ns = nanos(started);
    assert_eq!(reader.document_count(), documents);
    let query = "common rare";
    let embedding = [1.0, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1];
    let options = || SearchQueryOptions {
        limit: LIMIT,
        offset: 0,
        rank_window: Some(64),
        metadata_filters: BTreeMap::from([("space_id".into(), "hot".into())]),
        fusion_weights: Default::default(),
        policy_epoch: None,
    };
    let mut samples = Vec::new();
    let mut controls = Vec::new();
    for (name, mode) in [("text", SearchMode::Text), ("hybrid", SearchMode::Hybrid)] {
        let (reference, _) = profile_search_call(|execution| {
            Ok(reader.search_candidates_with_context(
                query,
                Some(&embedding),
                mode,
                options(),
                execution,
            )?)
        })?;
        let expected = reference
            .result
            .hits
            .iter()
            .map(|hit| candidate_signature(&hit.scores))
            .collect::<Vec<_>>();
        assert!(!expected.is_empty() && expected.len() <= LIMIT);
        let ids = expected.iter().map(|hit| hit.0.clone()).collect::<Vec<_>>();
        let (full_reference, _) = profile_search_call(|execution| {
            Ok(reader.search_with_hydration(
                query,
                Some(&embedding),
                mode,
                options(),
                execution,
                SearchHitHydration::Documents,
            )?)
        })?;
        let expected_full = full_reference
            .result
            .hits
            .iter()
            .map(hit_signature)
            .collect::<Vec<_>>();
        assert_eq!(
            expected
                .iter()
                .cloned()
                .map(selection_signature)
                .collect::<Vec<_>>(),
            expected_full
                .iter()
                .cloned()
                .map(selection_signature)
                .collect::<Vec<_>>()
        );
        assert!(expected
            .iter()
            .chain(&expected_full)
            .all(|signature| signature
                .1
                .iter()
                .all(|bits| f64::from_bits(*bits).is_finite())));
        let enriched = expected
            .iter()
            .zip(&expected_full)
            .filter(|(candidate, full)| candidate.1[1] != full.1[1])
            .count();
        controls.push(json!({"mode":name,"generation":build.generation,"ordered_score_signature":expected,
            "ordered_hydrated_score_signature":expected_full,"hydration_vector_diagnostic_changes":enriched,
            "truncated":reference.result.truncated,"fallback_reasons":reference.result.fallback_reasons,
            "retrievers":reference.result.retrievers.iter().map(|r|json!({"name":r.name,"backend":r.backend,
                "generated_candidates":r.generated_candidate_count,"scored_documents":r.candidate_scan_scored_document_count,
                "posting_bytes_read":r.posting_bytes_read,"postings_visited":r.candidate_postings_visited,
                "blocks_skipped":r.candidate_blocks_skipped,"working_bytes":r.candidate_scan_admitted_working_bytes})).collect::<Vec<_>>()}));
        for round in 0..WARMUPS + repeats {
            for position in 0..3 {
                let phase = ["candidates", "hydrate_selected_ids", "combined_documents"]
                    [(position + round) % 3];
                emit(&json!({"event":"attempt_started","mode":name,"round":round,"phase":phase}))?;
                let before = ProcessMemorySnapshot::capture().ok();
                let (elapsed, metrics, observation) = match phase {
                    "candidates" => {
                        let (output, elapsed) = profile_search_call(|execution| {
                            Ok(reader.search_candidates_with_context(
                                query,
                                Some(&embedding),
                                mode,
                                options(),
                                execution,
                            )?)
                        })?;
                        let observation = memory(before);
                        assert_eq!(
                            output
                                .result
                                .hits
                                .iter()
                                .map(|h| candidate_signature(&h.scores))
                                .collect::<Vec<_>>(),
                            expected
                        );
                        assert_eq!(output.metrics.hydrated_documents, 0);
                        (elapsed, output.metrics, observation)
                    }
                    "hydrate_selected_ids" => {
                        let started = Instant::now();
                        let output = reader.hydrate_documents(&ids)?;
                        let elapsed = nanos(started);
                        let observation = memory(before);
                        assert_eq!(
                            output
                                .documents
                                .iter()
                                .map(|d| d.id.clone())
                                .collect::<Vec<_>>(),
                            ids
                        );
                        for document in &output.documents {
                            let id = document.id.strip_prefix("doc-").unwrap().parse::<usize>()?;
                            assert!(id < documents && !id.is_multiple_of(10));
                            assert_eq!(*document, fixture_document(id));
                        }
                        (elapsed, output.metrics, observation)
                    }
                    _ => {
                        let (output, elapsed) = profile_search_call(|execution| {
                            Ok(reader.search_with_hydration(
                                query,
                                Some(&embedding),
                                mode,
                                options(),
                                execution,
                                SearchHitHydration::Documents,
                            )?)
                        })?;
                        let observation = memory(before);
                        assert_eq!(
                            output
                                .result
                                .hits
                                .iter()
                                .map(hit_signature)
                                .collect::<Vec<_>>(),
                            expected_full
                        );
                        (elapsed, output.metrics, observation)
                    }
                };
                let sample = json!({"event":"sample","mode":name,"round":round,"warmup":round < WARMUPS,
                    "phase":phase,"position":position,"elapsed_ns":elapsed,"counters":search_metrics(&metrics),"memory":observation});
                emit(&sample)?;
                samples.push(sample);
            }
        }
    }
    drop(reader);
    fs::remove_dir_all(&path)?;
    Ok(
        json!({"mode":"search","fixture_setup_ns":fixture_setup_ns,"open_ns":open_ns,"documents":documents,
        "fixture":"common/rare lexical tokens; 90-percent hot Space; 8-dimensional vectors; deterministic IDs",
        "query":query,"limit":LIMIT,"rank_window":64,"warmups":WARMUPS,"repetitions":repeats,
        "ranking_call_budget":{"scope":"independent task per preflight, warmup and measured ranking call",
            "timeout_ms":SEARCH_TIMEOUT.as_millis(),"work_units":SEARCH_WORK_UNITS},
        "vector_working_byte_limit":MEMORY_BYTES,"controls":controls,"samples":samples,
        "scope":"warm immutable native reader; candidate selection, selected-ID hydration and combined calls are independent observations, not additive timings; backend is recorded; no recall/cold-device claim"}),
    )
}

fn phase(before: ConcurrentPhaseMetrics, after: ConcurrentPhaseMetrics) -> Json {
    json!({"count":after.count.saturating_sub(before.count),"total_ns":after.total_nanos.saturating_sub(before.total_nanos)})
}

fn coordinator(before: ConcurrentDatabaseMetrics, after: ConcurrentDatabaseMetrics) -> Json {
    json!({"sequencer_wait":phase(before.sequencer_wait,after.sequencer_wait),"sequencer_hold":phase(before.sequencer_hold,after.sequencer_hold),
        "commit_execution_including_sync":phase(before.commit_execution,after.commit_execution),
        "group_durability":phase(before.group_durability,after.group_durability),
        "published_read_view_capture":phase(before.read_publication,after.read_publication),
        "wal_encoding_ns":null,"individual_fsync_ns":null,"head_publication_only_ns":null,
        "scope":"completed inclusive/overlapping coordinator phases; disabled group counters do not mean zero fsync"})
}

fn verify(database: &ConcurrentDatabase, rows: usize) -> Result<()> {
    let output = database.query_sql("SELECT id, payload FROM events ORDER BY id")?;
    assert_eq!(output.rows.len(), rows);
    for (id, row) in output.rows.iter().enumerate() {
        assert_eq!(row["id"], Value::Int(id as i64));
        assert_eq!(row["payload"], Value::String("x".repeat(PAYLOAD_BYTES)));
    }
    Ok(())
}

fn logical_bytes(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            total = total.saturating_add(logical_bytes(&entry.path())?);
        } else if entry.file_type()?.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

fn writes(repeats: usize, batch: usize) -> Result<Json> {
    let path = directory()?;
    let started = Instant::now();
    let database = ConcurrentDatabase::new(Database::open(path.join("database"))?);
    database.query_sql("CREATE TABLE events (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")?;
    let fixture_setup_ns = nanos(started);
    database.enable_concurrency_metrics();
    let mut samples = Vec::new();
    for round in 0..WARMUPS + repeats {
        emit(&json!({"event":"attempt_started","mode":"write","round":round,"batch_rows":batch}))?;
        let memory_before = ProcessMemorySnapshot::capture().ok();
        let before = database.concurrency_metrics();
        let total = Instant::now();
        let started = Instant::now();
        let mut transaction =
            database.begin_transaction(ConcurrentTransactionOptions::optimistic())?;
        let begin_ns = nanos(started);
        let started = Instant::now();
        for offset in 0..batch {
            transaction.query_sql_with_params(
                "INSERT INTO events (id, payload) VALUES ($1, $2)",
                &[
                    Value::Int((round * batch + offset) as i64),
                    Value::String("x".repeat(PAYLOAD_BYTES)),
                ],
            )?;
        }
        let prepare_ns = nanos(started);
        let started = Instant::now();
        transaction.commit()?;
        let commit_ns = nanos(started);
        let total_ns = nanos(total);
        let observation = memory(memory_before);
        let counters = coordinator(before, database.concurrency_metrics());
        let sample = json!({"event":"sample","mode":"write","round":round,"warmup":round < WARMUPS,
            "batch_rows":batch,"begin_ns":begin_ns,"statement_preparation_ns":prepare_ns,
            "commit_sync_and_publication_ns":commit_ns,"total_ns":total_ns,"coordinator":counters,"memory":observation});
        emit(&sample)?;
        samples.push(sample);
    }
    let rows = (WARMUPS + repeats) * batch;
    verify(&database, rows)?;
    let bytes_before = logical_bytes(&path)?;
    let started = Instant::now();
    database.checkpoint()?;
    let checkpoint_ns = nanos(started);
    let bytes_after = logical_bytes(&path)?;
    drop(database);
    let started = Instant::now();
    let reopened = ConcurrentDatabase::new(Database::open(path.join("database"))?);
    let reopen_ns = nanos(started);
    verify(&reopened, rows)?;
    drop(reopened);
    fs::remove_dir_all(&path)?;
    Ok(
        json!({"mode":"write","durability":"default SyncOnEveryWrite","group_commit":"disabled default",
        "fixture_setup_ns":fixture_setup_ns,"checkpoint_ns":checkpoint_ns,"reopen_ns":reopen_ns,
        "logical_file_bytes_before_checkpoint":bytes_before,"logical_file_bytes_after_checkpoint":bytes_after,
        "batch_rows":batch,"verified_rows":rows,"warmups":WARMUPS,"repetitions":repeats,"samples":samples,
        "scope":"single writer, growing persistent table; complete rows verified before checkpoint and after reopen; no relaxed durability, crash/power-loss or p99 claim"}),
    )
}

fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        return Err("component profiles require the repository release profile".into());
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        return Err(
            "usage: component_profiles search DOCUMENTS REPEATS | write REPEATS BATCH_ROWS".into(),
        );
    }
    let first: usize = args[1].parse()?;
    let second: usize = args[2].parse()?;
    let environment = environment();
    let report = match args[0].as_str() {
        "search" if (64..=100_000).contains(&first) && (15..=100).contains(&second) => search(first,second)?,
        "write" if (15..=100).contains(&first) && (1..=64).contains(&second) => writes(first,second)?,
        _ => return Err("invalid mode or bounds: search documents 64..100000/repeats 15..100; write repeats 15..100/batch 1..64".into()),
    };
    emit(
        &json!({"event":"report","protocol":"hawdb-component-profiles-v1","environment":environment,"report":report}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranking_calls_do_not_share_work_or_cancellation() {
        for _ in 0..4 {
            profile_search_call(|execution| {
                let task = execution.vector_execution_options.task_context.unwrap();
                assert_eq!(task.work_units(), 0);
                assert!(task.checkpoint().is_ok());
                task.charge_work(SEARCH_WORK_UNITS)?;
                assert!(task.charge_work(1).is_err());
                task.cancellation().cancel();
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn ranking_calls_renew_the_absolute_deadline() {
        let (first_deadline, _) = profile_search_call(|execution| {
            Ok(execution
                .vector_execution_options
                .task_context
                .unwrap()
                .deadline()
                .unwrap())
        })
        .unwrap();
        let next_started = hawdb_core::time::Instant::now();
        profile_search_call(|execution| {
            let task = execution.vector_execution_options.task_context.unwrap();
            let deadline = task.deadline().unwrap();
            assert!(deadline >= next_started + SEARCH_TIMEOUT);
            assert!(task.check_deadline_at(first_deadline).is_ok());
            assert!(task.check_deadline_at(deadline).is_err());
            Ok(())
        })
        .unwrap();
    }
}
