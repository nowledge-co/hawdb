// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::compare::{CasePairState, Change};
use super::*;
use crate::runner;
use serde::de::DeserializeOwned;
use serde_json::json;

fn copy<T: Serialize + DeserializeOwned>(value: &T) -> T {
    serde_json::from_value(serde_json::to_value(value).unwrap()).unwrap()
}

fn fixture() -> (Frozen, Report) {
    let frozen = Frozen::cardinality(7, 64).unwrap();
    let run = runner::run(&frozen, false).unwrap();
    (frozen, run)
}

fn first_operator(run: &mut Report) -> &mut Json {
    &mut run.attempts[0].observation.evidence.as_mut().unwrap()["operator_cardinalities"][0]
}

#[test]
fn directed_fixtures_match_independent_counts_and_replay() {
    for (seed, rows) in [(0, 64), (7, 128)] {
        let frozen = Frozen::cardinality(seed, rows).unwrap();
        let bytes = serde_json::to_vec(&frozen).unwrap();
        let loaded: Frozen = serde_json::from_slice(&bytes).unwrap();
        loaded.validate().unwrap();
        let run = runner::run(&loaded, false).unwrap();
        let report = analyze(&loaded, &run).unwrap();
        assert_eq!(report.cases.len(), 77);
        for case in &report.cases {
            assert_eq!(case.state, CaseState::Observed, "{case:?}");
            assert_eq!(case.actual_result_rows, case.expected_result_rows);
        }
        assert_eq!(serde_json::to_vec(&loaded).unwrap(), bytes);
        let cell = loaded.manifest.cells.len() - 3;
        assert_eq!(
            run.attempts[cell].observation.full_result,
            runner::replay(&loaded, cell).unwrap().full_result
        );
        let diff = compare(&loaded, &run, &run).unwrap();
        assert_eq!(diff.case_counts[&CasePairState::Aligned], 77);
        assert!(!diff.operator_counts.contains_key(&Change::Regressed));
        assert!(!diff.operator_counts.contains_key(&Change::Improved));
    }
}

#[test]
fn zero_partial_and_missing_counts_never_become_positive_q_error() {
    let op = |estimated_rows, actual_rows, fully_consumed| {
        OperatorError::new(
            OperatorObservation {
                id: 1,
                operator: "scan".into(),
                table: "t".into(),
                access: "scan".into(),
                estimated_rows,
                actual_rows,
                fully_consumed,
                counter_scope: "total_operator_output".into(),
            },
            true,
        )
    };
    for (e, a, complete, coverage) in [
        (0, Some(0), true, Coverage::BothZero),
        (1, Some(0), true, Coverage::ActualZero),
        (0, Some(10), true, Coverage::EstimateZero),
        (100, Some(10), false, Coverage::Partial),
        (100, None, true, Coverage::Unobserved),
    ] {
        let value = op(e, a, complete);
        assert_eq!(value.coverage, coverage);
        assert_eq!(value.q_error, None);
        assert_eq!(value.log2_bias, None);
    }
    let under = op(2, Some(8), true);
    let over = op(8, Some(2), true);
    assert_eq!(under.q_error, Some(4.0));
    assert_eq!(under.log2_bias, Some(-2.0));
    assert_eq!(over.log2_bias, Some(2.0));
    assert!(op(u64::MAX, Some(1), true).q_error.unwrap().is_finite());
    assert_eq!(Quantiles::of(vec![]).p99, None);
    assert_eq!(Quantiles::of(vec![1.0, 4.0, 2.0, 3.0]).p50, Some(2.0));
}

#[test]
fn reports_retain_missing_failed_and_wrong_result_cases_without_reweighting_rounds() {
    let (frozen, mut run) = fixture();
    let original = analyze(&frozen, &run).unwrap();
    let mut repeat = copy(&run.attempts[0]);
    repeat.phase = "measurement".into();
    run.attempts.push(repeat);
    assert_eq!(
        analyze(&frozen, &run).unwrap().summary.operators,
        original.summary.operators
    );
    run.attempts.last_mut().unwrap().observation.status = Status::SemanticMismatch;
    assert_eq!(
        analyze(&frozen, &run).unwrap().cases[0].state,
        CaseState::RepeatedExecutionFailed
    );
    run.attempts.last_mut().unwrap().observation.status = Status::Completed;
    run.attempts.remove(0);
    run.attempts[0].observation.status = Status::AdmissionRefused;
    let result = analyze(&frozen, &run).unwrap();
    assert_eq!(result.cases.len(), 77);
    assert_eq!(result.cases[0].state, CaseState::MissingValidation);
    assert_eq!(result.cases[1].state, CaseState::ExecutionFailed);
    assert!(result.cases[1]
        .operators
        .iter()
        .all(|o| o.q_error.is_none()));

    let (frozen, mut run) = fixture();
    run.attempts[0].observation.full_result.as_mut().unwrap()["rows"] = json!([]);
    let result = analyze(&frozen, &run).unwrap();
    assert_eq!(result.cases[0].state, CaseState::ResultCountMismatch);
    assert!(result.cases[0]
        .operators
        .iter()
        .all(|o| o.coverage == Coverage::CaseFailed));
    run.fixture_error = Some(json!({"error":"setup refused"}));
    assert_eq!(
        analyze(&frozen, &run).unwrap().summary.cases[&CaseState::SetupFailed],
        77
    );
}

#[test]
fn foreign_duplicate_or_misaligned_evidence_is_refused() {
    let (frozen, run) = fixture();
    let mut bad: Report = copy(&run);
    bad.manifest_sha256 = "other".into();
    assert!(analyze(&frozen, &bad).is_err());
    let mut bad: Report = copy(&run);
    bad.attempts.push(copy(&bad.attempts[0]));
    assert!(analyze(&frozen, &bad).is_err());
    let mut bad: Report = copy(&run);
    bad.attempts[0].cell = 1000;
    assert!(analyze(&frozen, &bad).is_err());
    let mut bad: Report = copy(&run);
    bad.attempts[0].observation.snapshot_epoch += 1;
    assert!(analyze(&frozen, &bad).is_err());
    let mut bad: Report = copy(&run);
    first_operator(&mut bad)["counter_scope"] = json!("per_probe");
    assert!(analyze(&frozen, &bad).is_err());
    let mut bad: Report = copy(&run);
    let extra = first_operator(&mut bad).clone();
    bad.attempts[0].observation.evidence.as_mut().unwrap()["operator_cardinalities"]
        .as_array_mut()
        .unwrap()
        .push(extra);
    assert!(analyze(&frozen, &bad).is_err());
}

#[test]
fn comparison_detects_estimate_changes_without_pairing_different_results_or_boundaries() {
    let (frozen, run) = fixture();
    let mut candidate: Report = copy(&run);
    first_operator(&mut candidate)["estimated_rows"] = json!(640);
    first_operator(&mut candidate)["access"] = json!("descriptor with changed estimate");
    let diff = compare(&frozen, &run, &candidate).unwrap();
    assert_eq!(diff.operator_counts[&Change::Regressed], 1);
    assert_eq!(diff.cases[0].operators[0].candidate_q_error, Some(10.0));
    let reverse = compare(&frozen, &candidate, &run).unwrap();
    assert_eq!(reverse.operator_counts[&Change::Improved], 1);

    first_operator(&mut candidate)["actual_rows"] = json!(1);
    assert_eq!(
        compare(&frozen, &run, &candidate).unwrap().operator_counts[&Change::ObservedRowsChanged],
        1
    );
    first_operator(&mut candidate)["fully_consumed"] = json!(false);
    assert_eq!(
        compare(&frozen, &run, &candidate).unwrap().operator_counts[&Change::Incomplete],
        1
    );
    candidate.attempts[0].observation.evidence.as_mut().unwrap()["executed_access_shape"] =
        json!(["changed"]);
    assert_eq!(
        compare(&frozen, &run, &candidate).unwrap().cases[0].state,
        CasePairState::AccessShapeChanged
    );
    candidate.attempts[0]
        .observation
        .full_result
        .as_mut()
        .unwrap()["rows"][0]["id"] = json!({"type":"int","value":999});
    assert_eq!(
        compare(&frozen, &run, &candidate).unwrap().cases[0].state,
        CasePairState::ResultChanged
    );
    candidate.attempts.remove(0);
    assert_eq!(
        compare(&frozen, &run, &candidate).unwrap().cases[0].state,
        CasePairState::CaseUnavailable
    );
}
