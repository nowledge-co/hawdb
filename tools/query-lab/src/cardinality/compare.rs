// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{analyze, CaseReport, CaseState, Coverage, Frozen, OperatorError, Quantiles, Report};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CasePairState {
    Aligned,
    CaseUnavailable,
    ResultChanged,
    AccessShapeChanged,
    OperatorSetChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Improved,
    Regressed,
    Unchanged,
    ZeroCaseChanged,
    Incomplete,
    ObservedRowsChanged,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorComparison {
    pub operator_id: usize,
    pub change: Change,
    pub baseline_q_error: Option<f64>,
    pub candidate_q_error: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CaseComparison {
    pub state: CasePairState,
    pub baseline: CaseReport,
    pub candidate: CaseReport,
    pub operators: Vec<OperatorComparison>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Source {
    pub run_record_sha256: String,
    pub environment: Json,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Comparison {
    pub protocol: String,
    pub manifest_sha256: String,
    pub baseline: Source,
    pub candidate: Source,
    pub scope: String,
    pub case_counts: BTreeMap<CasePairState, usize>,
    pub operator_counts: BTreeMap<Change, usize>,
    pub paired_baseline_q_error: Quantiles,
    pub paired_candidate_q_error: Quantiles,
    pub cases: Vec<CaseComparison>,
}

fn align(base: &CaseReport, candidate: &CaseReport) -> CasePairState {
    if base.state != CaseState::Observed || candidate.state != CaseState::Observed {
        return CasePairState::CaseUnavailable;
    }
    if base.result_sha256.is_none() || base.result_sha256 != candidate.result_sha256 {
        return CasePairState::ResultChanged;
    }
    if base.access_shape.is_none() || base.access_shape != candidate.access_shape {
        return CasePairState::AccessShapeChanged;
    }
    // The debug access descriptor also contains estimates. Its text must not
    // become an identity key, or every CE change would refuse comparison.
    let identities = |case: &CaseReport| {
        case.operators
            .iter()
            .map(|op| {
                let o = &op.observation;
                (
                    o.id,
                    o.operator.clone(),
                    o.table.clone(),
                    o.counter_scope.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    if identities(base) != identities(candidate) {
        CasePairState::OperatorSetChanged
    } else {
        CasePairState::Aligned
    }
}

fn compare_operator(base: &OperatorError, candidate: &OperatorError) -> Change {
    let incomplete = |op: &OperatorError| {
        matches!(
            op.coverage,
            Coverage::Partial | Coverage::Unobserved | Coverage::CaseFailed
        )
    };
    if incomplete(base) || incomplete(candidate) {
        return Change::Incomplete;
    }
    if base.observation.actual_rows != candidate.observation.actual_rows {
        return Change::ObservedRowsChanged;
    }
    match (base.q_error, candidate.q_error) {
        (Some(a), Some(b)) => match b.total_cmp(&a) {
            std::cmp::Ordering::Less => Change::Improved,
            std::cmp::Ordering::Equal => Change::Unchanged,
            std::cmp::Ordering::Greater => Change::Regressed,
        },
        _ if base.observation.estimated_rows == candidate.observation.estimated_rows => {
            Change::Unchanged
        }
        _ => Change::ZeroCaseChanged,
    }
}

pub fn compare(
    frozen: &Frozen,
    baseline: &Report,
    candidate: &Report,
) -> Result<Comparison, String> {
    let base = analyze(frozen, baseline)?;
    let candidate = analyze(frozen, candidate)?;
    let mut case_counts = BTreeMap::new();
    let mut operator_counts = BTreeMap::new();
    let mut base_errors = vec![];
    let mut candidate_errors = vec![];
    let mut cases = vec![];
    for (b, c) in base.cases.into_iter().zip(candidate.cases) {
        // Both inventories are reconstructed from the same validated manifest,
        // including missing observations. No intersection silently loses cases.
        let state = align(&b, &c);
        *case_counts.entry(state).or_default() += 1;
        let mut operators = vec![];
        if state == CasePairState::Aligned {
            for (left, right) in b.operators.iter().zip(&c.operators) {
                let change = compare_operator(left, right);
                *operator_counts.entry(change).or_default() += 1;
                if matches!(
                    change,
                    Change::Improved | Change::Regressed | Change::Unchanged
                ) && let (Some(a), Some(b)) = (left.q_error, right.q_error)
                {
                    base_errors.push(a);
                    candidate_errors.push(b);
                }
                operators.push(OperatorComparison {
                    operator_id: left.observation.id,
                    change,
                    baseline_q_error: left.q_error,
                    candidate_q_error: right.q_error,
                });
            }
        }
        cases.push(CaseComparison {
            state,
            baseline: b,
            candidate: c,
            operators,
        });
    }
    Ok(Comparison {
        protocol: "hawdb-cardinality-comparison-v1".into(),
        manifest_sha256: frozen.sha256.clone(),
        baseline: Source { run_record_sha256: base.run_record_sha256, environment: base.environment },
        candidate: Source { run_record_sha256: candidate.run_record_sha256, environment: candidate.environment },
        scope: "diagnostic only; identical frozen input and complete typed results; pair observed access/join boundaries with unchanged counters; access shape is not complete physical identity; positive Q-error quantiles use only paired observations; zero changes separate; no plan-quality or performance verdict".into(),
        case_counts,
        operator_counts,
        paired_baseline_q_error: Quantiles::of(base_errors),
        paired_candidate_q_error: Quantiles::of(candidate_errors),
        cases,
    })
}
