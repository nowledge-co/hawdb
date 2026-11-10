// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Seeded semantic clocks keep candidate comparisons and editable replay exact.

use super::*;
use hawdb::optimizer::{OptimizerBoundedOrderAlternative, OptimizerSearchDirective};
use hawdb::{
    OptimizerQualificationAttemptLimits, OptimizerQualificationCandidateKind,
    OptimizerQualificationCandidateRequest, QueryRequest,
};
use std::time::Duration;

pub(super) fn bounded_order(case: &Case, run: &mut Run) -> Result<()> {
    let mut db = Database::new();
    for ddl in [
        "CREATE NODE TABLE ClockRow",
        "CREATE PROPERTY ON NODE TABLE ClockRow(id) TYPE INT NOT NULL",
        "CREATE PROPERTY ON NODE TABLE ClockRow(key) TYPE INT",
        "CREATE PROPERTY ON NODE TABLE ClockRow(tz) TYPE STRING NOT NULL",
    ] {
        run.cypher(&mut db, "clock_schema", ddl, &[])?;
    }
    let mut model = Vec::new();
    for (id, value) in case.values.iter().enumerate() {
        let key = (value.rem_euclid(3) != 0).then(|| value.rem_euclid(5));
        run.cypher(
            &mut db,
            "clock_create",
            "CREATE (:ClockRow {id: $id, key: $key, tz: $tz})",
            &[
                ("id", Value::Int(id as i64)),
                ("key", key.map_or(Value::Null, Value::Int)),
                (
                    "tz",
                    text(if id.is_multiple_of(2) {
                        "+01:00"
                    } else {
                        "-05:30"
                    }),
                ),
            ],
        )?;
        model.push((key, id as i64));
    }
    let instant = hawdb::temporal::DateTime::from_timestamp(
        1_793_361_600 + case.variant as i64 * 86_400,
        (case.seed % 1_000) as u32,
    )
    .expect("bounded generated statement clock");
    let offset = (case.variant % 3) as usize;
    let limit = (case.variant % 5 + 1) as usize;
    let minimum = if case.variant.is_multiple_of(4) {
        16
    } else {
        0
    };
    let parameters = BTreeMap::from([
        ("min".into(), Value::Int(minimum)),
        ("offset".into(), Value::Int(offset as i64)),
        ("limit".into(), Value::Int(limit as i64)),
    ]);
    const QUERY: &str = "MATCH (n:ClockRow) WHERE n.id >= $min RETURN n.id AS id, n.key AS key, datetime() AS observed, datetime({timezone: n.tz}) AS row_clock, CASE WHEN true THEN CURRENT_TIMESTAMP() END AS stamp ORDER BY key ASC, id DESC SKIP $offset LIMIT $limit";
    let mut requests = [
        OptimizerQualificationCandidateRequest::new(OptimizerQualificationCandidateKind::Ordinary),
        OptimizerQualificationCandidateRequest::new(
            OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::TopN,
            ),
        ),
        OptimizerQualificationCandidateRequest::new(
            OptimizerQualificationCandidateKind::BoundedOrder(
                OptimizerBoundedOrderAlternative::SortThenLimit,
            ),
        ),
    ];
    if !case.variant.is_multiple_of(2) {
        requests.reverse();
    }
    let mut read = db
        .begin_read_transaction()
        .map_err(|e| error("clock_snapshot", e))?;
    let attempts = OptimizerQualificationAttemptLimits::new(Duration::from_secs(5), 100_000)
        .map_err(|e| error("clock_limits", e))?;
    let candidates = hawdb::temporal::with_statement_clock(instant, || {
        read.qualify_optimizer_bounded_order_candidate_requests(
            QueryRequest::new(QUERY).with_params(&parameters),
            &requests,
            attempts,
            OptimizerSearchDirective::Memo,
        )
    })
    .map_err(|e| error("clock_candidates", e))?;
    run.trace.push(json!({
        "phase": "clock_candidates", "query": QUERY,
        "parameters": crate::parameters_json(&parameters),
        "statement_clock": instant.to_rfc3339(), "snapshot_epoch": candidates.snapshot_epoch,
        "distinct_candidates": candidates.distinct_candidate_count,
        "candidates": candidates.candidates.iter().map(|candidate| json!({
            "kind": candidate.kind.as_str(), "same_plan_as": candidate.same_plan_as,
            "fingerprint": candidate.output.selected_plan_instance_fingerprint,
            "work_units": candidate.output.work_units,
        })).collect::<Vec<_>>(),
    }));
    candidates
        .verify_complete_results()
        .map_err(|e| error("clock_agreement", e))?;
    if candidates.distinct_candidate_count != 2 {
        return Err(error(
            "clock_diversity",
            candidates.distinct_candidate_count,
        ));
    }
    model.retain(|(_, id)| *id >= minimum);
    model.sort_by(|(left_key, left_id), (right_key, right_id)| {
        left_key
            .is_none()
            .cmp(&right_key.is_none())
            .then_with(|| left_key.cmp(right_key))
            .then_with(|| right_id.cmp(left_id))
    });
    let expected = model
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(key, id)| {
            let offset = if id % 2 == 0 { 3600 } else { -19_800 };
            let zoned =
                instant.with_timezone(&hawdb::temporal::FixedOffset::east_opt(offset).unwrap());
            row(&[
                ("id", Value::Int(id)),
                ("key", key.map_or(Value::Null, Value::Int)),
                ("observed", Value::DateTime(instant.fixed_offset())),
                ("row_clock", Value::DateTime(zoned)),
                ("stamp", Value::Int(instant.timestamp_nanos_opt().unwrap())),
            ])
        })
        .collect::<Vec<_>>();
    for candidate in &candidates.candidates {
        let actual = candidate
            .output
            .result
            .as_ref()
            .map_err(|e| error("clock_execution", e))?
            .output
            .rows
            .iter()
            .map(|row| row.to_owned_row())
            .collect::<Vec<_>>();
        run.equal("clock_bounded_order", &actual, &expected, true)?;
    }
    Ok(())
}
