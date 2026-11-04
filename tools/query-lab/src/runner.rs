// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::manifest::{Cell, Frozen, Manifest, MAX_ARTIFACT_BYTES};
use hawdb::{
    Database, DatabaseConfig, DatabaseReadTransaction, HawDBError, QueryStreamOptions,
    RuntimeMemoryReservation, RuntimeTaskContext, SqlQueryRequest,
};
use query_workloads::generator::DeterministicRng;
use query_workloads::smith::{Case, Mode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Completed,
    GenerationRefused,
    GeneratorInvalid,
    Unsupported,
    ExpectedRuntimeError,
    AdmissionRefused,
    TimedOut,
    Cancelled,
    Panic,
    EngineError,
    SemanticMismatch,
    NotRun,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub status: Status,
    pub error: Option<String>,
    pub snapshot_epoch: u64,
    pub end_to_end_ns: u64,
    pub full_result: Option<Json>,
    pub evidence: Option<Json>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub cell: usize,
    pub phase: String,
    pub position: usize,
    pub observation: Observation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replay {
    pub cell: usize,
    pub original_status: Status,
    pub replay_status: Status,
    pub same_signature: bool,
    pub replay_error: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub protocol: String,
    pub manifest_sha256: String,
    pub kind: String,
    pub environment: Json,
    pub fixture_setup_ns: u64,
    pub fixture_error: Option<Json>,
    pub attempts: Vec<Attempt>,
    pub failure_replays: Vec<Replay>,
    pub summary: Json,
    pub alternative_plan_coverage: bool,
    pub qualification_status: String,
}

fn elapsed(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
fn output_limits(m: &Manifest) -> QueryStreamOptions {
    QueryStreamOptions {
        max_rows: Some(m.limits.max_output_rows),
        max_payload_bytes: Some(m.limits.max_output_bytes),
    }
}
fn fixture(m: &Manifest, deadline: Instant) -> Result<Database, (Status, String)> {
    let mut db = Database::new_with_config(DatabaseConfig {
        // Input/intermediate work and delivered output have separate bounds.
        max_read_result_rows: Some(m.intermediate_row_limit),
        max_read_result_payload_bytes: Some(m.limits.max_output_bytes),
        ..DatabaseConfig::default()
    });
    let task = RuntimeTaskContext::with_timeout(deadline.saturating_duration_since(Instant::now()))
        .with_work_limit(m.work_limit)
        .with_memory_reservation(RuntimeMemoryReservation::new(
            m.memory_bytes,
            m.limits.max_output_bytes as u64,
        ));
    for statement in &m.fixture.setup {
        if Instant::now() >= deadline {
            return Err((Status::TimedOut, "fixture setup deadline".into()));
        }
        let parameters = statement.values();
        match catch_unwind(AssertUnwindSafe(|| {
            db.query_sql_request(
                SqlQueryRequest::new(&statement.sql)
                    .with_params(&parameters)
                    .with_task_context(&task),
            )
        })) {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err((error_status(&error), format!("fixture setup: {error}")))
            }
            Err(_) => return Err((Status::Panic, "fixture setup panicked".into())),
        }
    }
    Ok(db)
}
fn error_status(error: &HawDBError) -> Status {
    match error {
        HawDBError::Parse(_) | HawDBError::Semantic(_) => Status::GeneratorInvalid,
        HawDBError::CapabilityUnavailable { .. } | HawDBError::UnsupportedStorageFormat(_) => {
            Status::Unsupported
        }
        HawDBError::ReadBudgetExceeded(_) | HawDBError::ResourceBudgetExceeded(_) => {
            Status::AdmissionRefused
        }
        HawDBError::Execution(message)
            if message.starts_with("prepared relational query requires ")
                && message.contains("estimated bytes, exceeding query_memory_bytes ") =>
        {
            Status::AdmissionRefused
        }
        HawDBError::Execution(message)
            if message.starts_with("relational row scan exceeds row limit ") =>
        {
            Status::AdmissionRefused
        }
        HawDBError::Execution(message)
            if message.contains("deadline") || message.contains("timed out") =>
        {
            Status::TimedOut
        }
        HawDBError::Execution(message)
            if message.contains("cancelled") || message.contains("canceled") =>
        {
            Status::Cancelled
        }
        HawDBError::Execution(message)
            if message.contains("division by zero") || message.contains("overflow") =>
        {
            Status::ExpectedRuntimeError
        }
        _ => Status::EngineError,
    }
}
fn cost_json(cost: hawdb::RelationalJoinPlanningCost) -> Json {
    json!({"estimated_rows":cost.estimated_rows,"total":cost.cost,"cpu":cost.cpu,"random_access":cost.random_io,"sequential_access":cost.sequential_io,"output":cost.output_rows})
}
fn observe(
    read: &DatabaseReadTransaction,
    case: &Case,
    m: &Manifest,
    remaining: Duration,
) -> Observation {
    let parameters = case.statement.values();
    let task = RuntimeTaskContext::with_timeout(
        Duration::from_millis(m.attempt_timeout_ms).min(remaining),
    )
    .with_work_limit(m.work_limit)
    .with_memory_reservation(RuntimeMemoryReservation::new(
        m.memory_bytes,
        m.limits.max_output_bytes as u64,
    ));
    let started = Instant::now();
    let result = catch_unwind(AssertUnwindSafe(|| {
        read.query_sql_with_params_options_profiled_context(
            &case.statement.sql,
            &parameters,
            output_limits(m),
            &task,
        )
    }));
    let mut observation = Observation {
        status: Status::Completed,
        error: None,
        snapshot_epoch: read.commit_epoch(),
        end_to_end_ns: elapsed(started),
        full_result: None,
        evidence: None,
    };
    match result {
        Ok(Ok(output)) => {
            let profile = output.profile;
            let mut rows = output
                .output
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| (k.to_owned(), query_workloads::typed_value_json(v)))
                        .collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>();
            if !case.ordered {
                // Canonicalize the complete bag, retaining duplicates and NULLs.
                rows.sort_by_cached_key(|row| serde_json::to_string(row).expect("typed row"));
            }
            observation.full_result =
                Some(json!({"columns":output.output.rows.schema().columns(),"rows":rows}));
            let operators = profile.operator_cardinality_profiles.iter().map(|op| json!({
                "id":op.operator_id.get(),"operator":op.operator.as_str(),"table":op.table,
                "access":format!("{:?}",op.access_path),"estimated_rows":op.estimated_rows,"actual_rows":op.actual_rows,
                "fully_consumed":op.fully_consumed,"counter_scope":"total_operator_output"
            })).collect::<Vec<_>>();
            let shape = profile
                .operator_cardinality_profiles
                .iter()
                .map(|op| {
                    format!(
                        "{}:{}:{:?}:{}:{}",
                        op.operator.as_str(),
                        op.table,
                        op.access_path.kind,
                        op.access_path.name,
                        op.access_path.covering
                    )
                })
                .collect::<Vec<_>>();
            observation.evidence = Some(json!({
                "executed_access_shape":shape,"complete_physical_fingerprint":null,
                "identity_limit":"access/join profiles do not identify every downstream physical operator",
                "operator_cardinalities":operators,
                "cost":profile.join_planning.cost.map(cost_json),"cost_scope":"relational_access_and_join",
                "coefficient_policy":"engine_default_logical_weights","cost_policy_qualified":false,
                "parse_ns":profile.stage_timings.parse_nanos,"bind_ns":profile.stage_timings.bind_nanos,
                "plan_ns":profile.stage_timings.plan_nanos,"execute_ns":profile.stage_timings.execute_nanos,
                "complete_output_scope":"owned rows delivered by query call; host serialization excluded",
                "intermediate_rows":profile.intermediate_rows,"hydrated_rows":profile.hydrated_rows,
                "hydrated_compressed_bytes":profile.hydrated_compressed_bytes,"hydrated_decompressed_bytes":profile.hydrated_decompressed_bytes,
                "row_reads":{"rows_visited":profile.row_read.rows_visited,"logical_pages":profile.row_read.logical_pages,"requested_bytes":profile.row_read.logical_bytes,
                    "physical_bytes":profile.row_read.physical_bytes,"cache_hits":profile.row_read.cache_hits,"cache_misses":profile.row_read.cache_misses},
                "index_reads":profile.index_reads.iter().map(|r|json!({"table":r.table,"index":r.index,"lookups":r.lookups,"metadata_counts":r.metadata_count_lookups,
                    "rows_visited":r.rows_visited,"logical_pages":r.logical_pages,"requested_bytes":r.logical_bytes,"physical_bytes":r.physical_bytes,"cache_hits":r.cache_hits,"cache_misses":r.cache_misses})).collect::<Vec<_>>(),
                "blocking_memory":profile.blocking_operator_memory_reports.iter().map(|r|json!({
                    "operator":r.operator,"budget_bytes":r.budget_bytes,"peak_tracked_bytes":r.peak_tracked_bytes,
                    "input_rows":r.input_rows,"candidate_rows":r.candidate_rows,"replay_rows":r.replay_rows,
                    "repartitions":r.repartitions,"max_spill_bytes":r.max_spill_bytes,"max_spill_runs":r.max_spill_runs,
                    "spilled_bytes":r.spilled_bytes,"spill_run_count":r.spill_run_count,"spilled_rows":r.spilled_rows
                })).collect::<Vec<_>>(),"allocator_peak_bytes":null,"rss_bytes":null,
                "physical_bytes_are_device_io":false
            }));
        }
        Ok(Err(error)) => {
            observation.status = error_status(&error);
            observation.error = Some(error.to_string());
        }
        Err(payload) => {
            observation.status = Status::Panic;
            observation.error = Some(
                payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or("non-string panic".into()),
            );
        }
    }
    observation
}
fn omitted(epoch: u64, status: Status, reason: &str) -> Observation {
    Observation {
        status,
        error: Some(reason.into()),
        snapshot_epoch: epoch,
        end_to_end_ns: 0,
        full_result: None,
        evidence: None,
    }
}
fn cell_observation(
    read: &DatabaseReadTransaction,
    cell: &Cell,
    m: &Manifest,
    remaining: Duration,
) -> Observation {
    match &cell.case {
        Some(case) => observe(read, case, m, remaining),
        None => omitted(
            read.commit_epoch(),
            Status::GenerationRefused,
            cell.refusal.as_deref().unwrap_or("missing case"),
        ),
    }
}

pub fn replay(frozen: &Frozen, cell: usize) -> Result<Observation, String> {
    replay_before(
        frozen,
        cell,
        Instant::now() + Duration::from_millis(frozen.manifest.campaign_timeout_ms),
    )
}

fn replay_before(frozen: &Frozen, cell: usize, deadline: Instant) -> Result<Observation, String> {
    frozen.validate()?;
    let target = frozen
        .manifest
        .cells
        .get(cell)
        .ok_or("cell index is outside the frozen manifest")?;
    let db = match fixture(&frozen.manifest, deadline) {
        Ok(db) => db,
        Err((status, reason)) => return Ok(omitted(0, status, &reason)),
    };
    let read = db.begin_read_transaction().map_err(|e| e.to_string())?;
    Ok(cell_observation(
        &read,
        target,
        &frozen.manifest,
        deadline.saturating_duration_since(Instant::now()),
    ))
}

fn environment() -> Json {
    let command = |program: &str, args: &[&str]| {
        std::process::Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    };
    json!({"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "runner":std::env::var("HOSTNAME").ok(),"toolchain":command("rustc",&["--version"]),
        "observed_checkout_revision":command("git",&["rev-parse","HEAD"]),"observed_checkout_status":command("git",&["status","--porcelain"]),
        "source_provenance_note":"checkout observation is not a compiled-engine attestation; retain binary and source diff with report",
        "debug_assertions":cfg!(debug_assertions),"residency":"materialized","cache_cohort":"warm","device_cache_claim":null,
        "host_load":std::fs::read_to_string("/proc/loadavg").ok(),"features":"query-lab dependency: hawdb default-features=false; feature unification depends on caller"})
}

/// All generation and setup precede every observation. The immutable manifest
/// is the only query/data input; no generator is called from this path.
pub fn run(frozen: &Frozen, cost: bool) -> Result<Report, String> {
    run_with_progress(frozen, cost, |_| Ok(()))
}

/// Persist the start before entering the engine so an abort or external timeout
/// leaves an exact manifest/cell replay address. Observer work is outside timing.
pub fn run_with_progress(
    frozen: &Frozen,
    cost: bool,
    mut progress: impl FnMut(Json) -> Result<(), String>,
) -> Result<Report, String> {
    frozen.validate()?;
    let m = &frozen.manifest;
    let env = environment();
    let campaign_start = Instant::now();
    let deadline = campaign_start + Duration::from_millis(m.campaign_timeout_ms);

    let mut report = Report {
        protocol: "hawdb-query-lab-v1".into(),
        manifest_sha256: frozen.sha256.clone(),
        kind: if cost {
            "cost_baseline"
        } else {
            "sqlsmith_comparison"
        }
        .into(),
        environment: env,
        fixture_setup_ns: 0,
        fixture_error: None,
        attempts: vec![],
        failure_replays: vec![],
        summary: Json::Null,
        alternative_plan_coverage: false,
        qualification_status:
            "diagnostic_only; candidate enumeration and full physical identity remain unqualified"
                .into(),
    };
    progress(json!({"event":"fixture_started","manifest_sha256":frozen.sha256}))?;
    let db = match fixture(m, deadline) {
        Ok(db) => db,
        Err((status, reason)) => {
            report.fixture_setup_ns = elapsed(campaign_start);
            report.fixture_error = Some(json!({"status":status,"error":reason}));
            for (cell, phase) in schedule(m, cost) {
                report.attempts.push(Attempt {
                    cell,
                    phase,
                    position: report.attempts.len(),
                    observation: omitted(0, Status::NotRun, "fixture setup failed"),
                });
            }
            report.summary = summarize(m, &report.attempts, &[], cost);
            return Ok(report);
        }
    };
    report.fixture_setup_ns = elapsed(campaign_start);
    let read = db.begin_read_transaction().map_err(|e| e.to_string())?;
    let mut retained_bytes = 0usize;
    let mut stopped = None;
    let mut failed = BTreeSet::new();
    let mut references = BTreeMap::<usize, Json>::new();
    let mut shapes = BTreeMap::<usize, Json>::new();
    for (cell, phase) in schedule(m, cost) {
        progress(
            json!({"event":"attempt_started","cell":cell,"phase":phase,"position":report.attempts.len()}),
        )?;
        if Instant::now() >= deadline {
            stopped.get_or_insert("campaign deadline");
        }
        let mut observation = if let Some(reason) = stopped {
            omitted(read.commit_epoch(), Status::NotRun, reason)
        } else if failed.contains(&cell) {
            omitted(
                read.commit_epoch(),
                Status::NotRun,
                "previous attempt failed",
            )
        } else {
            cell_observation(
                &read,
                &m.cells[cell],
                m,
                deadline.saturating_duration_since(Instant::now()),
            )
        };
        if observation.status == Status::Completed {
            if let Some(expected) = references.get(&cell) {
                verify_repeat(&mut observation, expected, &shapes[&cell]);
            } else {
                references.insert(
                    cell,
                    observation
                        .full_result
                        .as_ref()
                        .expect("completed result")
                        .clone(),
                );
                shapes.insert(
                    cell,
                    observation.evidence.as_ref().unwrap()["executed_access_shape"].clone(),
                );
            }
        }
        if observation.status != Status::Completed && observation.status != Status::NotRun {
            failed.insert(cell);
            if observation.status == Status::Panic {
                stopped = Some("panic invalidated fixture custody");
            }
        }
        let bytes = serde_json::to_vec(&observation)
            .map_err(|e| e.to_string())?
            .len();
        retained_bytes = retained_bytes.saturating_add(bytes);
        // Keep this attempt's evidence intact; refuse subsequent attempts.
        if retained_bytes > MAX_ARTIFACT_BYTES as usize / 2 {
            stopped = Some("report payload budget");
        }
        progress(
            json!({"event":"attempt_finished","cell":cell,"phase":phase,"status":observation.status,"error":observation.error}),
        )?;
        report.attempts.push(Attempt {
            cell,
            phase: phase.clone(),
            position: report.attempts.len(),
            observation,
        });
    }
    for cell in failed {
        let original = &report
            .attempts
            .iter()
            .find(|a| {
                a.cell == cell
                    && !matches!(a.observation.status, Status::Completed | Status::NotRun)
            })
            .unwrap()
            .observation;
        if original.status == Status::GenerationRefused || Instant::now() >= deadline {
            continue;
        }
        progress(json!({"event":"replay_started","cell":cell}))?;
        let mut replayed = replay_before(frozen, cell, deadline)?;
        if let Some(expected) = references.get(&cell) {
            verify_repeat(&mut replayed, expected, &shapes[&cell]);
        }
        report.failure_replays.push(Replay {
            cell,
            original_status: original.status,
            replay_status: replayed.status,
            same_signature: original.status == replayed.status
                && original.error == replayed.error
                && (original.status != Status::SemanticMismatch
                    || original.full_result == replayed.full_result),
            replay_error: replayed.error,
        });
    }
    report.summary = summarize(m, &report.attempts, &report.failure_replays, cost);
    Ok(report)
}
fn verify_repeat(observation: &mut Observation, expected: &Json, shape: &Json) {
    if observation.status != Status::Completed {
        return;
    }
    if observation.full_result.as_ref() != Some(expected) {
        observation.status = Status::SemanticMismatch;
        observation.error = Some("complete typed result changed on the same snapshot".into());
    } else if observation
        .evidence
        .as_ref()
        .map(|e| &e["executed_access_shape"])
        != Some(shape)
    {
        observation.status = Status::EngineError;
        observation.error = Some("executed access shape changed after validation".into());
    }
}

fn schedule(m: &Manifest, cost: bool) -> Vec<(usize, String)> {
    let mut rounds = vec!["validation"];
    if cost {
        rounds.extend(std::iter::repeat_n("warmup", m.warmups));
        rounds.extend(std::iter::repeat_n("measurement", m.repetitions));
    }
    let mut random = DeterministicRng::new(m.schedule_seed);
    let mut schedule = vec![];
    for phase in rounds {
        let mut order = (0..m.cells.len()).collect::<Vec<_>>();
        if cost {
            for i in (1..order.len()).rev() {
                let j = (random.next_u64() % (i + 1) as u64) as usize;
                order.swap(i, j);
            }
        }
        schedule.extend(order.into_iter().map(|cell| (cell, phase.into())));
    }
    schedule
}

fn summarize(m: &Manifest, attempts: &[Attempt], replays: &[Replay], cost: bool) -> Json {
    let mut cohorts = vec![];
    for mode in [Mode::Template, Mode::Recursive] {
        let cells = m
            .cells
            .iter()
            .enumerate()
            .filter(|(_, c)| c.mode == mode)
            .map(|(i, _)| i)
            .collect::<BTreeSet<_>>();
        let observations = attempts
            .iter()
            .filter(|a| cells.contains(&a.cell) && a.phase == "validation")
            .collect::<Vec<_>>();
        let completed = observations
            .iter()
            .filter(|a| a.observation.status == Status::Completed)
            .count();
        let mut statuses = BTreeMap::new();
        let mut syntax = BTreeSet::new();
        let mut combinations = BTreeSet::new();
        let mut plans = BTreeSet::new();
        for a in &observations {
            *statuses
                .entry(format!("{:?}", a.observation.status))
                .or_insert(0usize) += 1;
            if a.observation.status == Status::Completed {
                if let Some(c) = &m.cells[a.cell].case {
                    syntax.extend(c.features.iter().cloned());
                    combinations.insert(c.features.iter().cloned().collect::<Vec<_>>());
                }
                if let Some(e) = &a.observation.evidence {
                    plans.insert(e["executed_access_shape"].to_string());
                }
            }
        }
        let failures = attempts
            .iter()
            .filter(|a| cells.contains(&a.cell))
            .filter(|a| {
                !matches!(
                    a.observation.status,
                    Status::Completed | Status::GenerationRefused | Status::NotRun
                )
            })
            .map(|a| a.cell)
            .collect::<BTreeSet<_>>()
            .len();
        let reproduced = replays
            .iter()
            .filter(|r| cells.contains(&r.cell) && r.same_signature)
            .count();
        cohorts.push(json!({"mode":mode,"manifest_cells":cells.len(),"completed":completed,"valid_execution_rate":completed as f64 / cells.len() as f64,
            "statuses":statuses,"executed_syntax":syntax,"executed_syntax_combinations":combinations,"executed_access_shapes":plans.len(),
            "failure_cases":failures,"failure_replays":replays.iter().filter(|r|cells.contains(&r.cell)).count(),"reproduced_failures":reproduced,
            "failure_replay_rate":if failures==0 {None}else{Some(reproduced as f64/failures as f64)},
            "semantic_correctness_proven":false}));
    }
    let timings = if cost {
        m.cells.iter().enumerate().map(|(cell,_)| {
        let samples=attempts.iter().filter(|a|a.cell==cell && a.phase=="measurement" && a.observation.status==Status::Completed).map(|a|a.observation.end_to_end_ns).collect::<Vec<_>>();
        let mut sorted=samples.clone();sorted.sort_unstable();
        json!({"cell":cell,"completed_samples":samples.len(),"required_samples":m.repetitions,
            "median_end_to_end_ns":if samples.len()==m.repetitions {sorted.get(sorted.len()/2).copied()}else{None},"regret":null})
    }).collect::<Vec<_>>()
    } else {
        vec![]
    };
    json!({"cohorts":cohorts,"timings":timings,"manifest_cells":m.cells.len(),"timing_scope":"profiled query call including preparation and owned output; serialization excluded",
        "pairing":"same fixture and case index across generator modes; different SQL is not an alternative-plan comparison"})
}

#[cfg(test)]
mod tests {
    use super::*;
    use query_workloads::smith::Limits;
    #[test]
    fn template_and_recursive_queries_execute_and_replay_on_fixed_fixture() {
        let frozen = Frozen::generate(7, 24, 64, Limits::default()).unwrap();
        let report = run(&frozen, false).unwrap();
        for a in &report.attempts {
            assert_eq!(
                a.observation.status,
                Status::Completed,
                "cell {} {:?}",
                a.cell,
                a.observation
            );
        }
        assert_eq!(report.attempts.len(), 48);
        assert_eq!(
            report.attempts[0].observation.full_result,
            replay(&frozen, 0).unwrap().full_result
        );
        assert_eq!(
            report.summary["cohorts"][0]["failure_replay_rate"],
            Json::Null
        );
    }
    #[test]
    fn invalid_queries_are_retained_and_replayed_as_failures() {
        let mut m = Frozen::generate(7, 1, 8, Limits::default())
            .unwrap()
            .manifest;
        m.cells[0].case.as_mut().unwrap().statement.sql = "SELECT missing FROM work_items".into();
        let report = run(&Frozen::seal(m).unwrap(), false).unwrap();
        assert_eq!(
            report.attempts[0].observation.status,
            Status::GeneratorInvalid
        );
        assert!(report.failure_replays[0].same_signature);
        assert_eq!(report.summary["cohorts"][0]["valid_execution_rate"], 0.0);
    }
    #[test]
    fn cost_sampling_freezes_inputs_snapshot_and_schedule() {
        let frozen = Frozen::generate(42, 6, 1024, Limits::cost_baseline()).unwrap();
        let before = serde_json::to_vec(&frozen).unwrap();
        let report = run(&frozen, true).unwrap();
        assert_eq!(report.attempts.len(), 12 * 19);
        let epoch = report.attempts[0].observation.snapshot_epoch;
        for a in &report.attempts {
            assert_eq!(a.observation.status, Status::Completed, "{a:?}");
            assert_eq!(a.observation.snapshot_epoch, epoch);
        }
        for timing in report.summary["timings"].as_array().unwrap() {
            assert_eq!(timing["completed_samples"], 15);
            assert!(timing["median_end_to_end_ns"].is_number());
            assert!(timing["regret"].is_null());
        }
        assert_eq!(before, serde_json::to_vec(&frozen).unwrap());
        let mut changed = frozen.manifest.clone();
        changed.schedule_seed += 1;
        assert_ne!(schedule(&frozen.manifest, true), schedule(&changed, true));
        assert_eq!(
            schedule(&frozen.manifest, true),
            schedule(&frozen.manifest, true)
        );
        assert!(!report.alternative_plan_coverage);
    }

    #[test]
    fn fixture_failure_retains_the_complete_denominator() {
        let mut m = Frozen::generate(7, 1, 8, Limits::default())
            .unwrap()
            .manifest;
        m.fixture.setup[0].sql = "CREATE TABLE".into();
        let report = run(&Frozen::seal(m).unwrap(), true).unwrap();
        assert!(report.fixture_error.is_some());
        assert_eq!(report.attempts.len(), 2 * 19);
        assert!(report
            .attempts
            .iter()
            .all(|a| a.observation.status == Status::NotRun));
        assert_eq!(report.summary["cohorts"][0]["valid_execution_rate"], 0.0);
        assert!(report.summary["timings"][0]["median_end_to_end_ns"].is_null());
    }

    #[test]
    fn complete_bags_keep_duplicates_and_nulls() {
        let mut m = Frozen::generate(7, 1, 8, Limits::default())
            .unwrap()
            .manifest;
        for cell in &mut m.cells {
            let c = cell.case.as_mut().unwrap();
            c.statement.sql = "SELECT label FROM work_items".into();
            c.statement.parameters.clear();
            c.ordered = false;
        }
        let report = run(&Frozen::seal(m).unwrap(), false).unwrap();
        let output = report.attempts[0].observation.full_result.as_ref().unwrap();
        let rows = output["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 8);
        assert_eq!(
            rows.iter().filter(|r| r["label"]["type"] == "null").count(),
            2
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["label"]["value"] == "label-1")
                .count(),
            2
        );
        assert_eq!(
            report.attempts[0].observation.full_result,
            report.attempts[1].observation.full_result
        );
    }

    #[test]
    fn refused_output_is_not_partial_success() {
        let mut m = Frozen::generate(
            7,
            1,
            8,
            Limits {
                max_output_rows: 1,
                ..Limits::default()
            },
        )
        .unwrap()
        .manifest;
        let c = m.cells[0].case.as_mut().unwrap();
        c.statement.sql = "SELECT id FROM work_items".into();
        c.statement.parameters.clear();
        let report = run(&Frozen::seal(m).unwrap(), true).unwrap();
        let attempts = report
            .attempts
            .iter()
            .filter(|a| a.cell == 0)
            .collect::<Vec<_>>();
        assert_eq!(attempts[0].observation.status, Status::AdmissionRefused);
        assert!(attempts[0].observation.full_result.is_none());
        assert!(attempts[1..]
            .iter()
            .all(|a| a.observation.status == Status::NotRun));
        assert!(report.failure_replays[0].same_signature);
        assert!(report.summary["timings"][0]["median_end_to_end_ns"].is_null());
    }
    #[test]
    fn repeated_execution_cannot_hide_a_changed_complete_result_or_plan() {
        let expected = json!({"rows":[{"value":{"type":"int","value":1}}]});
        let shape = json!(["scan"]);
        let mut observation = Observation {
            status: Status::Completed,
            error: None,
            snapshot_epoch: 1,
            end_to_end_ns: 1,
            full_result: Some(json!({"rows":[]})),
            evidence: Some(json!({"executed_access_shape":shape})),
        };
        verify_repeat(&mut observation, &expected, &shape);
        assert_eq!(observation.status, Status::SemanticMismatch);
        observation.status = Status::Completed;
        observation.full_result = Some(expected.clone());
        verify_repeat(&mut observation, &expected, &json!(["index"]));
        assert_eq!(observation.status, Status::EngineError);
    }
}
