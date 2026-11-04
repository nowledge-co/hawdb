// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Offline, zero-aware CE analysis. Never interprets result rows as the
//! cardinality of a different physical boundary, and never re-plans a query.

use crate::{
    manifest::{digest, Frozen},
    runner::{Report, Status},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};

mod compare;
pub use compare::{compare, Comparison};
#[cfg(test)]
mod tests;

pub const PROTOCOL: &str = "hawdb-cardinality-report-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseState {
    Observed,
    SetupFailed,
    MissingValidation,
    ExecutionFailed,
    RepeatedExecutionFailed,
    MissingResult,
    ResultCountMismatch,
    MissingOperators,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Coverage {
    Measured,
    ActualZero,
    EstimateZero,
    BothZero,
    Partial,
    Unobserved,
    CaseFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorObservation {
    pub id: usize,
    pub operator: String,
    pub table: String,
    pub access: String,
    pub estimated_rows: u64,
    pub actual_rows: Option<u64>,
    pub fully_consumed: bool,
    pub counter_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorError {
    pub observation: OperatorObservation,
    pub coverage: Coverage,
    /// Defined only when both row counts are positive and fully observed.
    pub q_error: Option<f64>,
    pub log2_bias: Option<f64>,
}

impl OperatorError {
    fn new(observation: OperatorObservation, case_valid: bool) -> Self {
        let coverage = if !case_valid {
            Coverage::CaseFailed
        } else if observation.actual_rows.is_none() {
            Coverage::Unobserved
        } else if !observation.fully_consumed {
            Coverage::Partial
        } else {
            match (observation.estimated_rows, observation.actual_rows.unwrap()) {
                (0, 0) => Coverage::BothZero,
                (_, 0) => Coverage::ActualZero,
                (0, _) => Coverage::EstimateZero,
                _ => Coverage::Measured,
            }
        };
        let log2_bias = (coverage == Coverage::Measured).then(|| {
            (observation.estimated_rows as f64).log2()
                - (observation.actual_rows.unwrap() as f64).log2()
        });
        let q_error = (coverage == Coverage::Measured).then(|| {
            let e = observation.estimated_rows as f64;
            let a = observation.actual_rows.unwrap() as f64;
            (e / a).max(a / e)
        });
        Self {
            observation,
            coverage,
            q_error,
            log2_bias,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseReport {
    pub cell: usize,
    pub case_id: String,
    pub cohort: String,
    pub family: String,
    pub state: CaseState,
    pub execution_status: Option<Status>,
    pub execution_error: Option<String>,
    pub other_attempt_failures: usize,
    pub expected_result_rows: Option<usize>,
    pub actual_result_rows: Option<usize>,
    pub result_sha256: Option<String>,
    pub access_shape: Option<Json>,
    pub operators: Vec<OperatorError>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Quantiles {
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

impl Quantiles {
    fn of(mut values: Vec<f64>) -> Self {
        values.sort_by(f64::total_cmp);
        let percentile =
            |p: usize| (!values.is_empty()).then(|| values[(values.len() * p).div_ceil(100) - 1]);
        Self {
            p50: percentile(50),
            p90: percentile(90),
            p95: percentile(95),
            p99: percentile(99),
            max: values.last().copied(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub cases: BTreeMap<CaseState, usize>,
    pub operators: BTreeMap<Coverage, usize>,
    pub underestimated: usize,
    pub overestimated: usize,
    pub exact_positive: usize,
    /// A visible floor-compatible observation, not asserted estimator provenance.
    pub estimate_one_actual_zero: usize,
    pub q_error: Quantiles,
}

impl Summary {
    fn from_operators<'a>(operators: impl Iterator<Item = &'a OperatorError>) -> Self {
        let mut summary = Self::default();
        let mut errors = vec![];
        for op in operators {
            *summary.operators.entry(op.coverage).or_default() += 1;
            if op.coverage == Coverage::ActualZero && op.observation.estimated_rows == 1 {
                summary.estimate_one_actual_zero += 1;
            }
            if let Some(q) = op.q_error {
                errors.push(q);
                match op
                    .observation
                    .estimated_rows
                    .cmp(&op.observation.actual_rows.unwrap())
                {
                    std::cmp::Ordering::Less => summary.underestimated += 1,
                    std::cmp::Ordering::Equal => summary.exact_positive += 1,
                    std::cmp::Ordering::Greater => summary.overestimated += 1,
                }
            }
        }
        summary.q_error = Quantiles::of(errors);
        summary
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GroupKey {
    pub cohort: String,
    pub family: String,
    pub operator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub key: GroupKey,
    pub summary: Summary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorstCase {
    pub cell: usize,
    pub case_id: String,
    pub operator_id: usize,
    pub estimated_rows: u64,
    pub actual_rows: u64,
    pub q_error: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardinalityReport {
    pub protocol: String,
    pub manifest_sha256: String,
    /// SHA-256 of the canonical typed run record, independent of file whitespace.
    pub run_record_sha256: String,
    pub environment: Json,
    pub scope: String,
    pub summary: Summary,
    pub groups: Vec<Group>,
    pub worst_positive_cases: Vec<WorstCase>,
    pub cases: Vec<CaseReport>,
}

pub fn analyze(frozen: &Frozen, run: &Report) -> Result<CardinalityReport, String> {
    frozen.validate()?;
    if run.protocol != "hawdb-query-lab-v1" || run.manifest_sha256 != frozen.sha256 {
        return Err("run protocol or frozen manifest identity mismatch".into());
    }
    let mut validations = BTreeMap::new();
    let mut later_failures = BTreeMap::<usize, usize>::new();
    let mut snapshot = None;
    for attempt in &run.attempts {
        if attempt.cell >= frozen.manifest.cells.len() {
            return Err("run contains a cell outside the frozen inventory".into());
        }
        if attempt.phase != "validation" {
            if attempt.observation.status != Status::Completed {
                *later_failures.entry(attempt.cell).or_default() += 1;
            }
            continue;
        }
        if validations
            .insert(attempt.cell, &attempt.observation)
            .is_some()
        {
            return Err("duplicate validation observation".into());
        }
        if attempt.observation.status == Status::Completed {
            let epoch = attempt.observation.snapshot_epoch;
            if snapshot
                .replace(epoch)
                .is_some_and(|previous| previous != epoch)
            {
                return Err("validation observations do not share one snapshot".into());
            }
        }
    }
    let mut cases = Vec::with_capacity(frozen.manifest.cells.len());
    for (cell, input) in frozen.manifest.cells.iter().enumerate() {
        let metadata = input.cardinality.as_ref();
        let observation = validations.get(&cell).copied();
        let other_attempt_failures = later_failures.get(&cell).copied().unwrap_or(0);
        let result = observation.and_then(|o| o.full_result.as_ref());
        let rows = result.and_then(|r| r.get("rows")).and_then(Json::as_array);
        let actual_result_rows = rows.map(Vec::len);
        let expected_result_rows = metadata.map(|m| m.expected_result_rows);
        let raw_operators = observation
            .and_then(|o| o.evidence.as_ref())
            .and_then(|e| e.get("operator_cardinalities"));
        let operators: Vec<OperatorObservation> = match raw_operators {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|e| format!("invalid operator evidence in cell {cell}: {e}"))?,
            None => vec![],
        };
        let mut ids = BTreeSet::new();
        for op in &operators {
            if op.id == 0 || !ids.insert(op.id) || op.counter_scope != "total_operator_output" {
                return Err(format!(
                    "duplicate/invalid operator identity or counter scope in cell {cell}"
                ));
            }
        }
        let state = if run.fixture_error.is_some() {
            CaseState::SetupFailed
        } else if observation.is_none() {
            CaseState::MissingValidation
        } else if observation.unwrap().status != Status::Completed {
            CaseState::ExecutionFailed
        } else if other_attempt_failures > 0 {
            CaseState::RepeatedExecutionFailed
        } else if rows.is_none() {
            CaseState::MissingResult
        } else if expected_result_rows.is_some() && expected_result_rows != actual_result_rows {
            CaseState::ResultCountMismatch
        } else if operators.is_empty() {
            CaseState::MissingOperators
        } else {
            CaseState::Observed
        };
        cases.push(CaseReport {
            cell,
            case_id: metadata.map_or_else(
                || format!("{}:{:?}", input.index, input.mode),
                |m| m.id.clone(),
            ),
            cohort: metadata.map_or_else(
                || format!("sqlsmith:{:?}", input.mode),
                |m| m.cohort.clone(),
            ),
            family: metadata.map_or_else(
                || {
                    input
                        .case
                        .as_ref()
                        .map_or("refused", |c| &c.template)
                        .to_string()
                },
                |m| m.family.clone(),
            ),
            state,
            execution_status: observation.map(|o| o.status),
            execution_error: observation.and_then(|o| o.error.clone()),
            other_attempt_failures,
            expected_result_rows,
            actual_result_rows,
            result_sha256: result
                .map(|r| serde_json::to_vec(r).map(|b| digest(&b)))
                .transpose()
                .map_err(|e| e.to_string())?,
            access_shape: observation
                .and_then(|o| o.evidence.as_ref())
                .and_then(|e| e.get("executed_access_shape"))
                .cloned()
                .filter(|v| !v.is_null()),
            operators: operators
                .into_iter()
                .map(|op| OperatorError::new(op, state == CaseState::Observed))
                .collect(),
        });
    }
    let mut summary = Summary::from_operators(cases.iter().flat_map(|c| &c.operators));
    let mut grouped = BTreeMap::<GroupKey, Vec<&OperatorError>>::new();
    let mut worst = vec![];
    for case in &cases {
        *summary.cases.entry(case.state).or_default() += 1;
        for op in &case.operators {
            grouped
                .entry(GroupKey {
                    cohort: case.cohort.clone(),
                    family: case.family.clone(),
                    operator: op.observation.operator.clone(),
                })
                .or_default()
                .push(op);
            if let Some(q_error) = op.q_error {
                worst.push(WorstCase {
                    cell: case.cell,
                    case_id: case.case_id.clone(),
                    operator_id: op.observation.id,
                    estimated_rows: op.observation.estimated_rows,
                    actual_rows: op.observation.actual_rows.unwrap(),
                    q_error,
                });
            }
        }
    }
    worst.sort_by(|a, b| {
        b.q_error
            .total_cmp(&a.q_error)
            .then(a.case_id.cmp(&b.case_id))
            .then(a.operator_id.cmp(&b.operator_id))
    });
    worst.truncate(10);
    Ok(CardinalityReport {
        protocol: PROTOCOL.into(),
        manifest_sha256: frozen.sha256.clone(),
        run_record_sha256: digest(&serde_json::to_vec(run).map_err(|e| e.to_string())?),
        environment: run.environment.clone(),
        scope: "selected access/join operator output; one validation per cell; positive-only Q-error without smoothing; zero cases separate; nearest-rank quantiles; no predicate-level trace, full-plan identity, or plan-quality claim".into(),
        summary,
        groups: grouped.into_iter().map(|(key, ops)| Group { key, summary: Summary::from_operators(ops.into_iter()) }).collect(),
        worst_positive_cases: worst,
        cases,
    })
}
