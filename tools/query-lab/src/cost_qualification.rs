// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Phase-one correctness proof for bounded Sort/TopN candidate qualification.
//! The report intentionally contains no timing or regret claim.

use crate::manifest::{digest, MAX_ARTIFACT_BYTES};
use hawdb::optimizer::{
    OptimizerBoundedOrderAlternative, OptimizerSearchDirective, PlanCostWeights,
};
use hawdb::{
    Database, OptimizerQualificationAttemptLimits, OptimizerQualificationCandidateKind,
    OptimizerQualificationCandidateRequest, QueryRequest, QueryStreamOptions,
    RuntimeMemoryReservation, RuntimeTaskContext, Value, OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

pub const PROTOCOL: &str = "hawdb-optimizer-cost-qualification-phase1-v1";
const STATEMENT: &str = "MATCH (n:Item) WHERE n.id >= $min RETURN n.sort_key AS sort_key, n.id AS id ORDER BY sort_key, id LIMIT 30";
const FIXTURE_ROWS: usize = 64;
const CAMPAIGN_TIMEOUT_MS: u64 = 15_000;
const CAMPAIGN_WORK_UNITS: usize = 30_000_000;
const ATTEMPT_TIMEOUT_MS: u64 = 5_000;
const ATTEMPT_WORK_UNITS: usize = 10_000_000;
const MEMORY_BYTES: u64 = 64 << 20;
const RESULT_BYTES: u64 = 1 << 20;
const MAX_OUTPUT_ROWS: usize = 64;
const MAX_OUTPUT_PAYLOAD_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    Ordinary,
    TopN,
    SortThenLimit,
}

impl CandidateKind {
    fn engine(self) -> OptimizerQualificationCandidateKind {
        match self {
            Self::Ordinary => OptimizerQualificationCandidateKind::Ordinary,
            Self::TopN => OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::TopN,
            ),
            Self::SortThenLimit => OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::SortThenLimit,
            ),
        }
    }

    fn from_engine(kind: OptimizerQualificationCandidateKind) -> Self {
        match kind {
            OptimizerQualificationCandidateKind::Ordinary => Self::Ordinary,
            OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::TopN,
            ) => Self::TopN,
            OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::SortThenLimit,
            ) => Self::SortThenLimit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Completed,
    EngineError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignEvidence {
    pub fixture_digest: String,
    pub fixture_rows: usize,
    pub search: String,
    pub candidate_limit: usize,
    pub generated_candidates: usize,
    pub distinct_candidates: usize,
    pub candidate_order: Vec<CandidateKind>,
    pub campaign_timeout_ms: u64,
    pub campaign_work_units: usize,
    pub campaign_work_units_used: usize,
    pub attempt_timeout_ms: u64,
    pub attempt_work_units: usize,
    pub memory_bytes: u64,
    pub result_bytes: u64,
    pub result_bytes_used: usize,
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub claim: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryEvidence {
    pub family: String,
    pub statement: String,
    pub typed_parameters: BTreeMap<String, Json>,
    pub result_semantics: String,
    pub snapshot_epoch: u64,
    pub ordinary_selected_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostEvidence {
    pub estimated_rows: u64,
    pub total: u64,
    pub cpu: u64,
    pub random_access: u64,
    pub sequential_access: u64,
    pub output: u64,
    pub policy_id: String,
    pub weights: CostWeightsEvidence,
    pub scope: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostWeightsEvidence {
    pub cpu: u64,
    pub random_access: u64,
    pub sequential_access: u64,
    pub output: u64,
}

impl From<PlanCostWeights> for CostWeightsEvidence {
    fn from(weights: PlanCostWeights) -> Self {
        Self {
            cpu: weights.cpu,
            random_access: weights.random_access,
            sequential_access: weights.sequential_access,
            output: weights.output,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalPropertiesEvidence {
    pub distribution: String,
    pub ordering: Vec<String>,
    pub covering_fields: Vec<String>,
    pub scan_pruning: String,
    pub vector_precision: String,
    pub memory_budget: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CardinalityEvidence {
    pub operator_id: usize,
    pub operator: String,
    pub estimated_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptEvidence {
    pub schedule_position: usize,
    pub status: AttemptStatus,
    pub error: Option<String>,
    pub complete_result: Option<Json>,
    pub work_units: usize,
    pub retained_result_bytes: usize,
    pub timing_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEvidence {
    pub kind: CandidateKind,
    pub same_plan_as: Option<usize>,
    pub complete_physical_fingerprint: String,
    pub shape_fingerprint: String,
    pub properties: PhysicalPropertiesEvidence,
    pub cost: CostEvidence,
    pub cardinalities: Vec<CardinalityEvidence>,
    pub statistics_decisions: Vec<String>,
    pub attempt: AttemptEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub protocol: String,
    pub campaign: CampaignEvidence,
    pub query: QueryEvidence,
    pub candidates: Vec<CandidateEvidence>,
    pub complete_results_equal: bool,
    pub qualification_status: String,
}

fn parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([("min".into(), Value::Int(0))])
}

fn typed_parameters(parameters: &BTreeMap<String, Value>) -> BTreeMap<String, Json> {
    parameters
        .iter()
        .map(|(name, value)| (name.clone(), query_workloads::typed_value_json(value)))
        .collect()
}

fn fixture_digest() -> Result<String, String> {
    let fixture = json!({
        "schema":[
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT NOT NULL",
            "CREATE PROPERTY ON NODE TABLE Item(sort_key) TYPE INT NOT NULL"
        ],
        "rows":(0..FIXTURE_ROWS).map(|id| json!({"id":id,"sort_key":id % 7})).collect::<Vec<_>>()
    });
    Ok(digest(
        &serde_json::to_vec(&fixture).map_err(|error| error.to_string())?,
    ))
}

fn fixture() -> Result<Database, String> {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item")
        .map_err(|error| error.to_string())?;
    db.query("CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT NOT NULL")
        .map_err(|error| error.to_string())?;
    db.query("CREATE PROPERTY ON NODE TABLE Item(sort_key) TYPE INT NOT NULL")
        .map_err(|error| error.to_string())?;
    let rows = (0..FIXTURE_ROWS)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id as i64)),
                ("sort_key".into(), Value::Int((id % 7) as i64)),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {id:r.id, sort_key:r.sort_key})",
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .map_err(|error| error.to_string())?;
    Ok(db)
}

fn full_result(output: &hawdb::BoundedReadQueryOutput) -> Json {
    let rows = output
        .output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(name, value)| (name.to_string(), query_workloads::typed_value_json(value)))
                .collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    json!({"columns":output.output.rows.schema().columns(),"rows":rows})
}

fn validate_replay_source(report: &Report) -> Result<(), String> {
    if report.protocol != PROTOCOL
        || report.campaign.fixture_digest != fixture_digest()?
        || report.campaign.fixture_rows != FIXTURE_ROWS
        || report.campaign.candidate_limit != OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT
        || report.campaign.campaign_timeout_ms != CAMPAIGN_TIMEOUT_MS
        || report.campaign.campaign_work_units != CAMPAIGN_WORK_UNITS
        || report.campaign.attempt_timeout_ms != ATTEMPT_TIMEOUT_MS
        || report.campaign.attempt_work_units != ATTEMPT_WORK_UNITS
        || report.campaign.memory_bytes != MEMORY_BYTES
        || report.campaign.result_bytes != RESULT_BYTES
        || report.campaign.max_output_rows != MAX_OUTPUT_ROWS
        || report.campaign.max_output_payload_bytes != MAX_OUTPUT_PAYLOAD_BYTES
        || report.query.statement != STATEMENT
        || report.query.typed_parameters != typed_parameters(&parameters())
    {
        return Err("qualification report protocol, fixture, query, or bounds mismatch".into());
    }
    if report.candidates.is_empty()
        || report.candidates.len() > OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT
        || report.campaign.generated_candidates != report.candidates.len()
    {
        return Err(
            "qualification report candidate inventory is empty, incomplete, or over limit".into(),
        );
    }
    let kinds = report
        .candidates
        .iter()
        .map(|candidate| candidate.kind)
        .collect::<BTreeSet<_>>();
    if kinds.len() != report.candidates.len() || !kinds.contains(&CandidateKind::Ordinary) {
        return Err(
            "qualification report candidate inventory is duplicate or lacks ordinary".into(),
        );
    }
    if report
        .candidates
        .iter()
        .any(|candidate| candidate.attempt.work_units > ATTEMPT_WORK_UNITS)
    {
        return Err("qualification report candidate work exceeds the attempt limit".into());
    }
    let reported_work_units = report
        .candidates
        .iter()
        .try_fold(0usize, |total, candidate| {
            total.checked_add(candidate.attempt.work_units)
        })
        .ok_or_else(|| "qualification report candidate work total overflows usize".to_string())?;
    let reported_result_bytes = report
        .candidates
        .iter()
        .try_fold(0usize, |total, candidate| {
            total.checked_add(candidate.attempt.retained_result_bytes)
        })
        .ok_or_else(|| "qualification report retained result total overflows usize".to_string())?;
    let result_bytes = usize::try_from(RESULT_BYTES)
        .map_err(|_| "qualification result-memory limit does not fit usize".to_string())?;
    if report.campaign.candidate_order
        != report
            .candidates
            .iter()
            .map(|candidate| candidate.kind)
            .collect::<Vec<_>>()
        || report
            .candidates
            .iter()
            .enumerate()
            .any(|(position, candidate)| candidate.attempt.schedule_position != position)
        || report.campaign.campaign_work_units_used != reported_work_units
        || report.campaign.campaign_work_units_used > CAMPAIGN_WORK_UNITS
        || report.campaign.result_bytes_used != reported_result_bytes
        || report.campaign.result_bytes_used > result_bytes
    {
        return Err(
            "qualification report candidate schedule, work, or result-memory evidence is inconsistent"
                .into(),
        );
    }
    Ok(())
}

fn execute(expected: Option<&Report>) -> Result<Report, String> {
    if let Some(report) = expected {
        validate_replay_source(report)?;
    }
    let db = fixture()?;
    let mut read = db
        .begin_read_transaction()
        .map_err(|error| error.to_string())?;
    let parameters = parameters();
    let task = RuntimeTaskContext::with_timeout(Duration::from_millis(CAMPAIGN_TIMEOUT_MS))
        .with_work_limit(CAMPAIGN_WORK_UNITS)
        .with_memory_reservation(RuntimeMemoryReservation::new(MEMORY_BYTES, RESULT_BYTES));
    let attempt_limits = OptimizerQualificationAttemptLimits::new(
        Duration::from_millis(ATTEMPT_TIMEOUT_MS),
        ATTEMPT_WORK_UNITS,
    )
    .map_err(|error| error.to_string())?;
    let request = QueryRequest::new(STATEMENT)
        .with_params(&parameters)
        .with_task_context(&task)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(MAX_OUTPUT_ROWS),
            max_payload_bytes: Some(MAX_OUTPUT_PAYLOAD_BYTES),
        });
    let requested = match expected {
        None => vec![
            OptimizerQualificationCandidateRequest::new(CandidateKind::Ordinary.engine()),
            OptimizerQualificationCandidateRequest::new(CandidateKind::TopN.engine()),
            OptimizerQualificationCandidateRequest::new(CandidateKind::SortThenLimit.engine()),
        ],
        Some(report) => report
            .candidates
            .iter()
            .map(|candidate| {
                OptimizerQualificationCandidateRequest::new(candidate.kind.engine())
                    .with_expected_plan_instance_fingerprint(
                        &candidate.complete_physical_fingerprint,
                    )
            })
            .collect(),
    };
    let set = read
        .qualify_optimizer_bounded_order_candidate_requests(
            request,
            &requested,
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .map_err(|error| error.to_string())?;
    let complete_results_equal = set.verify_complete_results().is_ok();
    let ordinary_selected_fingerprint = set
        .candidates
        .iter()
        .find(|candidate| candidate.kind == OptimizerQualificationCandidateKind::Ordinary)
        .expect("engine inventory retains ordinary")
        .output
        .selected_plan_instance_fingerprint
        .clone();
    let candidates = set
        .candidates
        .into_iter()
        .enumerate()
        .map(|(schedule_position, candidate)| {
            let output = candidate.output;
            let trace = output.optimizer_trace;
            let cost = trace.selected_plan_cost_breakdown;
            let properties = trace.selected_plan_properties;
            let retained_result_bytes = output
                .result
                .as_ref()
                .map_or(0, |result| result.output.rows.retained_memory_bytes());
            let (status, error, result) = match output.result {
                Ok(output) => (AttemptStatus::Completed, None, Some(full_result(&output))),
                Err(error) => (AttemptStatus::EngineError, Some(error.to_string()), None),
            };
            CandidateEvidence {
                kind: CandidateKind::from_engine(candidate.kind),
                same_plan_as: candidate.same_plan_as,
                complete_physical_fingerprint: output.selected_plan_instance_fingerprint,
                shape_fingerprint: trace.selected_plan_fingerprint,
                properties: PhysicalPropertiesEvidence {
                    distribution: properties.distribution.as_str().into(),
                    ordering: properties.ordering,
                    covering_fields: properties.covering_fields,
                    scan_pruning: properties.scan_pruning.as_str().into(),
                    vector_precision: properties.vector_precision.as_str().into(),
                    memory_budget: properties.memory_budget.as_str().into(),
                },
                cost: CostEvidence {
                    estimated_rows: cost.estimated_rows,
                    total: cost.cost,
                    cpu: cost.cpu,
                    random_access: cost.random_io,
                    sequential_access: cost.sequential_io,
                    output: cost.output_rows,
                    policy_id: output.cost_policy_id.into(),
                    weights: output.cost_weights.into(),
                    scope: "selected_graph_physical_plan".into(),
                },
                cardinalities: trace
                    .selected_plan_cardinality_estimates
                    .into_iter()
                    .map(|estimate| CardinalityEvidence {
                        operator_id: estimate.operator_id.ordinal(),
                        operator: estimate.operator.as_str().into(),
                        estimated_rows: estimate.estimated_rows,
                    })
                    .collect(),
                statistics_decisions: trace.decisions,
                attempt: AttemptEvidence {
                    schedule_position,
                    status,
                    error,
                    complete_result: result,
                    work_units: output.work_units,
                    retained_result_bytes,
                    timing_ns: None,
                },
            }
        })
        .collect::<Vec<_>>();
    let successful = complete_results_equal
        && candidates
            .iter()
            .all(|candidate| candidate.attempt.status == AttemptStatus::Completed);
    Ok(Report {
        protocol: PROTOCOL.into(),
        campaign: CampaignEvidence {
            fixture_digest: fixture_digest()?,
            fixture_rows: FIXTURE_ROWS,
            search: "memo".into(),
            candidate_limit: set.candidate_limit,
            generated_candidates: candidates.len(),
            distinct_candidates: set.distinct_candidate_count,
            candidate_order: candidates.iter().map(|candidate| candidate.kind).collect(),
            campaign_timeout_ms: CAMPAIGN_TIMEOUT_MS,
            campaign_work_units: CAMPAIGN_WORK_UNITS,
            campaign_work_units_used: task.work_units(),
            attempt_timeout_ms: ATTEMPT_TIMEOUT_MS,
            attempt_work_units: ATTEMPT_WORK_UNITS,
            memory_bytes: MEMORY_BYTES,
            result_bytes: RESULT_BYTES,
            result_bytes_used: set.retained_result_bytes,
            max_output_rows: MAX_OUTPUT_ROWS,
            max_output_payload_bytes: MAX_OUTPUT_PAYLOAD_BYTES,
            claim: "correctness_only_no_timing_or_regret".into(),
        },
        query: QueryEvidence {
            family: "sort_top_n".into(),
            statement: STATEMENT.into(),
            typed_parameters: typed_parameters(&parameters),
            result_semantics: "ordered_complete_typed_rows".into(),
            snapshot_epoch: set.snapshot_epoch,
            ordinary_selected_fingerprint,
        },
        candidates,
        complete_results_equal,
        qualification_status: if successful {
            "completed".into()
        } else {
            "failed".into()
        },
    })
}

pub fn run() -> Result<Report, String> {
    execute(None)
}

pub fn replay(report: &Report) -> Result<Report, String> {
    let replayed = execute(Some(report))?;
    if &replayed != report {
        return Err("qualification replay differs from the retained report".into());
    }
    Ok(replayed)
}

pub fn load(path: &Path) -> Result<Report, String> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|error| error.to_string())?
        .take(MAX_ARTIFACT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_ARTIFACT_BYTES {
        return Err("qualification report exceeds artifact budget".into());
    }
    let report = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    validate_replay_source(&report)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_order_report_retains_complete_candidates_and_replays_exactly() {
        let report = run().unwrap();
        assert_eq!(report.protocol, PROTOCOL);
        assert_eq!(report.campaign.generated_candidates, 3);
        assert_eq!(report.campaign.distinct_candidates, 2);
        assert!(report.complete_results_equal);
        assert_eq!(report.qualification_status, "completed");
        assert_eq!(
            report.campaign.campaign_work_units_used,
            report
                .candidates
                .iter()
                .map(|candidate| candidate.attempt.work_units)
                .sum::<usize>()
        );
        assert_eq!(
            report.campaign.result_bytes_used,
            report
                .candidates
                .iter()
                .map(|candidate| candidate.attempt.retained_result_bytes)
                .sum::<usize>()
        );
        assert!(report.campaign.result_bytes_used > 0);
        assert!(report.campaign.result_bytes_used <= report.campaign.result_bytes as usize);
        assert!(report
            .candidates
            .iter()
            .all(|candidate| candidate.attempt.timing_ns.is_none()
                && candidate.attempt.work_units > 0
                && candidate.attempt.work_units <= report.campaign.attempt_work_units));
        assert_eq!(replay(&report).unwrap(), report);
    }

    #[test]
    fn replay_refuses_stale_duplicate_missing_and_over_limit_inventories() {
        let report = run().unwrap();

        let mut stale = report.clone();
        stale.candidates[0].complete_physical_fingerprint = "stale".into();
        assert!(replay(&stale).unwrap_err().contains("is stale"));

        let mut duplicate = report.clone();
        duplicate.candidates[1].kind = CandidateKind::Ordinary;
        assert!(replay(&duplicate).unwrap_err().contains("duplicate"));

        let mut missing = report.clone();
        missing.candidates.remove(0);
        missing.campaign.generated_candidates -= 1;
        assert!(replay(&missing).unwrap_err().contains("lacks ordinary"));

        let mut over_limit = report;
        while over_limit.candidates.len() <= OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT {
            over_limit.candidates.push(over_limit.candidates[0].clone());
        }
        over_limit.campaign.generated_candidates = over_limit.candidates.len();
        assert!(replay(&over_limit).unwrap_err().contains("over limit"));
    }

    #[test]
    fn replay_validation_refuses_overflowing_attempt_work_without_panicking() {
        let mut report = run().unwrap();
        report.candidates[0].attempt.work_units = usize::MAX;
        let error = validate_replay_source(&report).unwrap_err();
        assert!(error.contains("exceeds the attempt limit"));
    }
}
