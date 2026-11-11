// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded paired measurements for the existing legal bounded-order adapter.
//! Missing candidate families and timing scopes remain explicit in the report.

use crate::{
    cost_qualification::{full_result, CandidateKind},
    manifest::{digest, MAX_ARTIFACT_BYTES},
};
use hawdb::{
    optimizer::OptimizerSearchDirective, Database, OptimizerQualificationAttemptLimits,
    OptimizerQualificationCandidateRequest, QueryRequest, QueryStreamOptions,
    RuntimeMemoryReservation, RuntimeTaskContext, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::Path,
    time::{Duration, Instant},
};

pub const PROTOCOL: &str = "hawdb-bounded-order-paired-measurement-v1";
const STATEMENT: &str = "MATCH (n:Item) WHERE n.id >= $min RETURN n.sort_key AS sort_key, n.id AS id, n.payload AS payload ORDER BY sort_key, id SKIP $offset LIMIT 30";
const KINDS: [CandidateKind; 3] = [
    CandidateKind::Ordinary,
    CandidateKind::TopN,
    CandidateKind::SortThenLimit,
];
const MEMORY: u64 = 64 << 20;
const RESULT_MEMORY: u64 = 1 << 20;
const MAX_SAMPLES: usize = 30;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub protocol: String,
    pub rows: usize,
    pub warmups: usize,
    pub repetitions: usize,
    pub seed: u64,
    pub campaign_timeout_ms: u64,
    pub cases: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    pub name: String,
    pub min: usize,
    pub offset: usize,
    pub payload_bytes: usize,
}

impl Manifest {
    pub fn new(rows: usize) -> Result<Self, String> {
        let manifest = Self {
            protocol: PROTOCOL.into(),
            rows,
            warmups: 3,
            repetitions: 15,
            seed: 7,
            campaign_timeout_ms: 120_000,
            cases: vec![
                Cell {
                    name: "broad_duplicate_null".into(),
                    min: 0,
                    offset: 0,
                    payload_bytes: 8,
                },
                Cell {
                    name: "selective_wide_offset".into(),
                    min: rows.saturating_sub(64),
                    offset: 5,
                    payload_bytes: 1024,
                },
                Cell {
                    name: "empty".into(),
                    min: rows,
                    offset: 0,
                    payload_bytes: 8,
                },
            ],
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.protocol != PROTOCOL
            || !(64..=32_768).contains(&self.rows)
            || self.warmups != 3
            || !(15..=MAX_SAMPLES).contains(&self.repetitions)
            || self.campaign_timeout_ms == 0
            || self.campaign_timeout_ms > 300_000
            || self.cases.is_empty()
            || self.cases.len() > 8
        {
            return Err("invalid paired measurement protocol or campaign bounds".into());
        }
        for (index, cell) in self.cases.iter().enumerate() {
            if cell.name.is_empty()
                || cell.name.len() > 80
                || cell.min > self.rows
                || cell.offset > 64
                || cell.payload_bytes > 1024
                || self.cases[..index]
                    .iter()
                    .any(|earlier| earlier.name == cell.name)
            {
                return Err("invalid or duplicate paired measurement cell".into());
            }
        }
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let mut bytes = Vec::new();
        fs::File::open(path)
            .map_err(|e| e.to_string())?
            .take(MAX_ARTIFACT_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() as u64 > MAX_ARTIFACT_BYTES {
            return Err("manifest exceeds artifact budget".into());
        }
        let manifest: Self = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub round: usize,
    pub warmup: bool,
    pub position: usize,
    pub kind: CandidateKind,
    pub status: String,
    pub error: Option<String>,
    pub fingerprint: Option<String>,
    pub snapshot_epoch: Option<u64>,
    pub cost: Option<u64>,
    pub preparation_ns: Option<u64>,
    pub execution_ns: Option<u64>,
    pub work_units: Option<usize>,
    pub counters: Option<Json>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CellReport {
    pub name: String,
    pub status: String,
    pub error: Option<String>,
    pub snapshot_epoch: Option<u64>,
    pub fixture_setup_ns: u64,
    pub query: String,
    pub typed_parameters: Json,
    pub complete_result: Option<Json>,
    pub candidates: Json,
    pub attempts: Vec<Attempt>,
    pub diagnosis: Json,
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn inventory() -> Json {
    json!([
        {"family":"access_selection","status":"unsupported","reason":"no legal scan/index candidate selector in the current adapter"},
        {"family":"coverage","status":"unsupported","reason":"no legal covering/noncovering selector in the current adapter"},
        {"family":"ordering","status":"unsupported","reason":"no legal ordered-index/sort selector in the current adapter"},
        {"family":"bounded_ordering","frontend":"cypher","residency":"materialized","cohort":"warm","status":"supported"},
        {"family":"bounded_ordering","frontend":"sql","status":"unsupported","reason":"no same-snapshot SQL qualification adapter"},
        {"family":"bounded_ordering","cohort":"fresh_handle","status":"unsupported","reason":"immutable reopen adapter remains pending"},
        {"family":"bounded_ordering","residency":"out_of_core","status":"unsupported","reason":"persistent fixture adapter remains pending"}
    ])
}

fn fixture(rows: usize, width: usize, task: &RuntimeTaskContext) -> Result<Database, String> {
    let mut db = Database::new();
    for statement in [
        "CREATE NODE TABLE Item",
        "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT NOT NULL",
        "CREATE PROPERTY ON NODE TABLE Item(sort_key) TYPE INT",
        "CREATE PROPERTY ON NODE TABLE Item(payload) TYPE STRING",
    ] {
        db.query_request(QueryRequest::new(statement).with_task_context(task))
            .map_err(|e| e.to_string())?;
    }
    for start in (0..rows).step_by(256) {
        let values = (start..rows.min(start + 256))
            .map(|id| {
                Value::Map(BTreeMap::from([
                    ("id".into(), Value::Int(id as i64)),
                    (
                        "sort_key".into(),
                        if id % 11 == 0 {
                            Value::Null
                        } else {
                            Value::Int((id % 7) as i64)
                        },
                    ),
                    ("payload".into(), Value::String("x".repeat(width))),
                ]))
            })
            .collect();
        let params = BTreeMap::from([("rows".into(), Value::List(values))]);
        db.query_request(QueryRequest::new("UNWIND $rows AS r CREATE (:Item {id:r.id, sort_key:r.sort_key, payload:r.payload})")
            .with_params(&params).with_task_context(task)).map_err(|e| e.to_string())?;
    }
    Ok(db)
}

fn unrun(
    round: usize,
    warmup: bool,
    position: usize,
    kind: CandidateKind,
    status: &str,
    error: &str,
) -> Attempt {
    Attempt {
        round,
        warmup,
        position,
        kind,
        status: status.into(),
        error: Some(error.into()),
        fingerprint: None,
        snapshot_epoch: None,
        cost: None,
        preparation_ns: None,
        execution_ns: None,
        work_units: None,
        counters: None,
    }
}

fn cell(manifest: &Manifest, cell: &Cell, campaign: &RuntimeTaskContext) -> CellReport {
    let params = BTreeMap::from([
        ("min".into(), Value::Int(cell.min as i64)),
        ("offset".into(), Value::Int(cell.offset as i64)),
    ]);
    let mut report = CellReport {
        name: cell.name.clone(),
        status: "not_run".into(),
        error: None,
        snapshot_epoch: None,
        fixture_setup_ns: 0,
        query: STATEMENT.into(),
        typed_parameters: json!(params
            .iter()
            .map(|(name, value)| (name, query_workloads::typed_value_json(value)))
            .collect::<BTreeMap<_, _>>()),
        complete_result: None,
        candidates: Json::Null,
        attempts: vec![],
        diagnosis: Json::Null,
    };
    let setup = Instant::now();
    let db = fixture(manifest.rows, cell.payload_bytes, campaign);
    report.fixture_setup_ns = nanos(setup.elapsed());
    let mut read = match db
        .as_ref()
        .map_err(String::clone)
        .and_then(|db| db.begin_read_transaction().map_err(|e| e.to_string()))
    {
        Ok(read) => Some(read),
        Err(error) => {
            report.error = Some(error);
            None
        }
    };
    let limits =
        OptimizerQualificationAttemptLimits::new(Duration::from_secs(5), 10_000_000).unwrap();
    let mut frozen = BTreeMap::<CandidateKind, String>::new();
    let mut stopped = report.error.clone();
    // Round zero validates and freezes all complete plan identities before warmups.
    for round in 0..=manifest.warmups + manifest.repetitions {
        let warmup = round <= manifest.warmups;
        let order = (0..KINDS.len())
            .map(|position| KINDS[(position + (manifest.seed % 3) as usize + round) % 3])
            .collect::<Vec<_>>();
        if let Some(reason) = &stopped {
            for (position, kind) in order.into_iter().enumerate() {
                report
                    .attempts
                    .push(unrun(round, warmup, position, kind, "not_run", reason));
            }
            continue;
        }
        let requests = order
            .iter()
            .map(|kind| {
                let request = OptimizerQualificationCandidateRequest::new(kind.engine());
                frozen.get(kind).map_or(request, |identity| {
                    request.with_expected_plan_instance_fingerprint(identity)
                })
            })
            .collect::<Vec<_>>();
        let result = read
            .as_mut()
            .unwrap()
            .qualify_optimizer_bounded_order_candidate_requests(
                QueryRequest::new(STATEMENT)
                    .with_params(&params)
                    .with_task_context(campaign)
                    .with_output_limits(QueryStreamOptions {
                        max_rows: Some(30),
                        max_payload_bytes: Some(128 << 10),
                    }),
                &requests,
                limits,
                OptimizerSearchDirective::Memo,
            );
        match result {
            Err(error) => {
                let error = format!(
                    "candidate batch failed before complete attempt evidence was returned: {error}"
                );
                for (position, kind) in order.into_iter().enumerate() {
                    report.attempts.push(unrun(
                        round,
                        warmup,
                        position,
                        kind,
                        "batch_error",
                        &error,
                    ));
                }
                stopped = Some(error);
            }
            Ok(set) => {
                let crossed_snapshot = report
                    .snapshot_epoch
                    .is_some_and(|epoch| epoch != set.snapshot_epoch);
                if crossed_snapshot {
                    stopped = Some("candidate rounds crossed snapshot epochs".into());
                }
                report.snapshot_epoch.get_or_insert(set.snapshot_epoch);
                if round == 0 {
                    report.candidates = json!(set.candidates.iter().map(|candidate| {
                        let trace = &candidate.output.optimizer_trace;
                        let cost = trace.selected_plan_cost_breakdown;
                        let weights = candidate.output.cost_weights;
                        json!({"kind":candidate.kind.as_str(),"fingerprint":candidate.output.selected_plan_instance_fingerprint,
                            "same_plan_as":candidate.same_plan_as,"cost":{"estimated_rows":cost.estimated_rows,
                                "total":cost.cost,"cpu":cost.cpu,"random_access":cost.random_io,
                                "sequential_access":cost.sequential_io,"output":cost.output_rows},
                            "weights":{"cpu":weights.cpu,"random_access":weights.random_access,
                                "sequential_access":weights.sequential_access,"output":weights.output},
                            "properties":format!("{:?}",trace.selected_plan_properties),"statistics":trace.decisions,
                            "policy":candidate.output.cost_policy_id})
                    }).collect::<Vec<_>>());
                }
                for (position, (kind, candidate)) in
                    order.into_iter().zip(set.candidates).enumerate()
                {
                    let output = candidate.output;
                    let identity = output.selected_plan_instance_fingerprint;
                    frozen.entry(kind).or_insert_with(|| identity.clone());
                    let mut attempt = Attempt {
                        round,
                        warmup,
                        position,
                        kind,
                        status: "completed".into(),
                        error: None,
                        fingerprint: Some(identity),
                        snapshot_epoch: Some(set.snapshot_epoch),
                        cost: Some(output.optimizer_trace.selected_plan_cost_breakdown.cost),
                        preparation_ns: Some(nanos(output.preparation_elapsed)),
                        execution_ns: Some(nanos(output.execution_elapsed)),
                        work_units: Some(output.work_units),
                        counters: None,
                    };
                    match output.result {
                        Err(error) => {
                            attempt.status =
                                serde_json::to_value(crate::runner::error_status(&error))
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .into();
                            attempt.error = Some(error.to_string());
                        }
                        Ok(result) => {
                            let complete = full_result(&result);
                            if let Some(reference) = &report.complete_result {
                                if reference != &complete {
                                    attempt.status = "semantic_mismatch".into();
                                    attempt.error =
                                        Some("complete ordered typed result changed".into());
                                }
                            } else {
                                report.complete_result = Some(complete);
                            }
                            let memory = &result.execution_profile.pipeline_memory_report;
                            let operators = output.optimizer_trace.selected_plan_cardinality_estimates.iter().map(|estimate| {
                                let actual = result.execution_profile.operator_cardinality_profiles.iter().find(|actual|
                                    actual.operator_id == estimate.operator_id && actual.operator == estimate.operator);
                                json!({"id":estimate.operator_id.ordinal(),"operator":estimate.operator.as_str(),
                                    "estimated_rows":estimate.estimated_rows,"actual_rows":actual.and_then(|actual|actual.actual_rows),
                                    "scope":"total_operator_output; below-LIMIT work may be partially consumed"})
                            }).collect::<Vec<_>>();
                            attempt.counters = Some(
                                json!({"tracked_query_peak_bytes":memory.query_memory_peak_bytes,
                                "rss_bytes":memory.steady_resident_bytes,"process_lifetime_peak_rss_bytes":memory.peak_resident_bytes,
                                "allocator_peak_bytes":null,"intermediate_rows":memory.intermediate_rows,
                                "intermediate_payload_bytes":memory.intermediate_payload_bytes,"operators":operators,
                                "input_visits":null,"adjacency_probes":null,"device_io_bytes":null}),
                            );
                        }
                    }
                    if crossed_snapshot {
                        attempt.status = "semantic_mismatch".into();
                        attempt.error = Some("candidate rounds crossed snapshot epochs".into());
                    }
                    if attempt.status != "completed" {
                        stopped = attempt.error.clone();
                    }
                    report.attempts.push(attempt);
                }
            }
        }
    }
    report.status = if stopped.is_none() {
        "completed"
    } else {
        "failed"
    }
    .into();
    report.error = stopped;
    report.diagnosis = diagnose(&report.attempts, manifest.repetitions, manifest.seed);
    report
}

fn median(values: &[u64]) -> f64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] as f64 + values[middle] as f64) / 2.0
    } else {
        values[middle] as f64
    }
}

fn diagnose(attempts: &[Attempt], repeats: usize, seed: u64) -> Json {
    let measured = attempts.iter().filter(|a| !a.warmup).collect::<Vec<_>>();
    if measured.len() != repeats * KINDS.len()
        || measured.iter().any(|a| {
            a.status != "completed"
                || a.execution_ns.is_none_or(|time| time == 0)
                || a.fingerprint.is_none()
                || a.snapshot_epoch.is_none()
                || a.cost.is_none()
        })
    {
        return json!({"status":"incomplete","observed_regret":null,"reason":"missing, failed or zero-duration paired evidence"});
    }
    let series = |kind| {
        measured
            .iter()
            .filter(|a| a.kind == kind)
            .map(|a| a.execution_ns.unwrap())
            .collect::<Vec<_>>()
    };
    if KINDS.iter().any(|kind| series(*kind).len() != repeats) {
        return json!({"status":"incomplete","observed_regret":null,"reason":"unequal candidate sample counts"});
    }
    for kind in KINDS {
        let records = measured
            .iter()
            .copied()
            .filter(|a| a.kind == kind)
            .collect::<Vec<_>>();
        if records.iter().any(|record| {
            record.fingerprint != records[0].fingerprint || record.cost != records[0].cost
        }) || records.iter().enumerate().any(|(index, record)| {
            records[..index]
                .iter()
                .any(|other| other.round == record.round)
        }) {
            return json!({"status":"incomplete","observed_regret":null,"reason":"candidate identity, cost or paired round changed"});
        }
    }
    let ordinary = series(CandidateKind::Ordinary);
    let ordinary_record = measured
        .iter()
        .find(|a| a.kind == CandidateKind::Ordinary)
        .unwrap();
    if measured
        .iter()
        .any(|record| record.snapshot_epoch != ordinary_record.snapshot_epoch)
    {
        return json!({"status":"incomplete","observed_regret":null,"reason":"snapshot epochs differ"});
    }
    let ordinary_median = median(&ordinary);
    let mut best = ordinary_median;
    let mut seen = vec![ordinary_record.fingerprint.as_ref().unwrap()];
    let mut pairs = Vec::new();
    for kind in KINDS.into_iter().skip(1) {
        let record = measured.iter().find(|a| a.kind == kind).unwrap();
        if seen.contains(&record.fingerprint.as_ref().unwrap()) {
            continue;
        }
        seen.push(record.fingerprint.as_ref().unwrap());
        let alternative = series(kind);
        if alternative.len() != repeats || ordinary.len() != repeats {
            return json!({"status":"incomplete","observed_regret":null,"reason":"unequal paired sample counts"});
        }
        let value = median(&alternative);
        best = best.min(value);
        let mut state = seed | 1;
        let mut ratios = Vec::with_capacity(2000);
        for _ in 0..2000 {
            let mut left = Vec::with_capacity(repeats);
            let mut right = Vec::with_capacity(repeats);
            for _ in 0..repeats {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let index = (state % repeats as u64) as usize;
                left.push(ordinary[index]);
                right.push(alternative[index]);
            }
            ratios.push(median(&left) / median(&right));
        }
        ratios.sort_by(f64::total_cmp);
        let (low, high) = (ratios[50], ratios[1949]);
        let measured_order = if low > 1.05 {
            "alternative_faster"
        } else if high < 1.0 / 1.05 {
            "ordinary_faster"
        } else {
            "tie_or_inconclusive"
        };
        let inversion = (low > 1.05 && ordinary_record.cost < record.cost)
            || (high < 1.0 / 1.05 && ordinary_record.cost > record.cost);
        pairs.push(json!({"alternative":kind,"ordinary_median_ns":ordinary_median,"alternative_median_ns":value,
            "paired_ratio_ci95":[low,high],"measured_order":measured_order,"reliable_cost_inversion":inversion}));
    }
    json!({"status":"diagnostic","timing_scope":"admitted_execution_and_complete_result_materialization",
        "observed_regret":ordinary_median / best,"distinct_plans":seen.len(),"pairs":pairs,
        "tie_band_percent":5,"bootstrap_resamples":2000,"bootstrap_seed":seed,
        "p99_claim":false,"observed_cardinality_recost":"unavailable; output counters do not reconstruct input work"})
}

pub fn run(manifest: &Manifest, checked: bool) -> Result<Json, String> {
    manifest.validate()?;
    if !checked
        && (cfg!(debug_assertions) || option_env!("QUERY_LAB_BUILD_SOURCE_SHA256").is_none())
    {
        return Err("measurement requires a Cargo release build with its compiled source receipt; use cost-measure-check for correctness".into());
    }
    let campaign =
        RuntimeTaskContext::with_timeout(Duration::from_millis(manifest.campaign_timeout_ms))
            .with_work_limit(1_000_000_000)
            .with_memory_reservation(RuntimeMemoryReservation::new(MEMORY, RESULT_MEMORY));
    let mut binary = fs::File::open(std::env::current_exe().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let mut binary_hash = Sha256::new();
    let mut buffer = [0_u8; 64 << 10];
    loop {
        let n = binary.read(&mut buffer).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        binary_hash.update(&buffer[..n]);
    }
    let report = json!({"protocol":PROTOCOL,"manifest_sha256":digest(&serde_json::to_vec(manifest).map_err(|e| e.to_string())?),
        "manifest":manifest,"fixture_generator":"item_mod7_null_mod11_v1","statement":STATEMENT,
        "environment":crate::runner::environment(),"compiled_revision":option_env!("QUERY_LAB_BUILD_REVISION"),
        "compiled_rust_cargo_source_sha256":option_env!("QUERY_LAB_BUILD_SOURCE_SHA256"),
        "binary_sha256":binary_hash.finalize().iter().map(|byte|format!("{byte:02x}")).collect::<String>(),"checked_profile":cfg!(debug_assertions),
        "source_receipt_scope":"repository Rust, Cargo, C and header inputs; excludes external registry source; lockfiles retained",
        "family_inventory":inventory(),"candidate_limit":8,"memory_bytes":MEMORY,"result_memory_bytes":RESULT_MEMORY,
        "attempt_timeout_ms":5000,"attempt_work_units":10_000_000,"campaign_work_units":1_000_000_000,
        "cases":manifest.cases.iter().map(|c| cell(manifest,c,&campaign)).collect::<Vec<_>>(),
        "claim":if checked {"correctness_only"} else {"bounded_order_warm_materialized_diagnostic_only"},
        "full_four_family_qualification":false,"default_coefficients_changed":false});
    if serde_json::to_vec(&report)
        .map_err(|e| e.to_string())?
        .len() as u64
        > MAX_ARTIFACT_BYTES
    {
        return Err("measurement report exceeds artifact budget".into());
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_bounds_and_unsupported_inventory_are_explicit() {
        assert!(Manifest::new(0).is_err());
        let mut manifest = Manifest::new(64).unwrap();
        manifest.repetitions = MAX_SAMPLES + 1;
        assert!(manifest.validate().is_err());
        assert_eq!(inventory().as_array().unwrap().len(), 7);
    }

    #[test]
    fn failed_or_missing_candidates_cannot_produce_regret() {
        assert!(diagnose(&[], 15, 7)["observed_regret"].is_null());
    }

    #[test]
    fn diagnosis_deduplicates_physical_plans_and_detects_reliable_inversions() {
        let mut attempts = Vec::new();
        for round in 1..=15 {
            for (position, kind) in KINDS.into_iter().enumerate() {
                let alternative = kind == CandidateKind::SortThenLimit;
                let mut attempt = unrun(round, false, position, kind, "completed", "");
                attempt.error = None;
                attempt.fingerprint = Some(if alternative { "sort" } else { "top_n" }.into());
                attempt.snapshot_epoch = Some(7);
                attempt.cost = Some(if alternative { 10 } else { 20 });
                attempt.execution_ns = Some(if alternative { 2000 } else { 1000 });
                attempts.push(attempt);
            }
        }
        let diagnosis = diagnose(&attempts, 15, 7);
        assert_eq!(diagnosis["distinct_plans"], 2);
        assert_eq!(diagnosis["pairs"].as_array().unwrap().len(), 1);
        assert_eq!(diagnosis["observed_regret"], 1.0);
        assert_eq!(diagnosis["pairs"][0]["reliable_cost_inversion"], true);
        attempts[0].status = "admission_refused".into();
        assert!(diagnose(&attempts, 15, 7)["observed_regret"].is_null());
        attempts[0].status = "completed".into();
        attempts[0].execution_ns = Some(0);
        assert!(diagnose(&attempts, 15, 7)["observed_regret"].is_null());
    }

    #[test]
    fn paired_engine_run_keeps_full_results_and_complete_schedule() {
        let manifest = Manifest::new(64).unwrap();
        let task = RuntimeTaskContext::with_timeout(Duration::from_secs(30))
            .with_work_limit(1_000_000_000)
            .with_memory_reservation(RuntimeMemoryReservation::new(MEMORY, RESULT_MEMORY));
        for case in &manifest.cases {
            let report = cell(&manifest, case, &task);
            assert_eq!(
                report.status, "completed",
                "{}: {:?}",
                case.name, report.error
            );
            assert_eq!(report.attempts.len(), (1 + 3 + 15) * 3);
            let expected_rows = (manifest.rows - case.min)
                .saturating_sub(case.offset)
                .min(30);
            assert_eq!(
                report.complete_result.as_ref().unwrap()["rows"]
                    .as_array()
                    .unwrap()
                    .len(),
                expected_rows
            );
            assert_eq!(report.diagnosis["distinct_plans"], 2);
            assert!(!report.diagnosis["observed_regret"].is_null());
        }
    }
}
