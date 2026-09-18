// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[test]
fn satisfied_order_and_missing_reports_do_not_invent_execution_nodes() {
    let case = case(4, 4);
    let select = select(QUERIES[4]);
    let mut input = case.input();
    input.access_path.order_prefix_len = select.order_by.len();
    input.blocking_operator_memory_reports.clear();
    let result =
        format_relational_explain(&select, &[], input, true, limits(), |_, _, _, _, _| false)
            .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(
        result.rows[0]["id"],
        text("ProjectionExec_logical_projection")
    );
    for row in result.rows.iter() {
        assert_eq!(row["memory"], Value::Null);
        assert_eq!(row["disk"], Value::Null);
    }
}

#[test]
fn planning_diagnostics_preserve_attempt_order_cost_and_fallback() {
    use hawdb_optimizer::{
        RelationalJoinPlanningAttempt, RelationalJoinPlanningCost,
        RelationalJoinPlanningFallbackClass, RelationalJoinPlanningReason,
        RelationalJoinPlanningStrategy,
    };
    let cost = RelationalJoinPlanningCost {
        estimated_rows: 2,
        cost: 13,
        cpu: 3,
        random_io: 5,
        sequential_io: 7,
        output_rows: 11,
    };
    let mut outcome = case(0, 7).input().join_planning;
    outcome.strategy = RelationalJoinPlanningStrategy::CsgCmpMemo;
    outcome.reason = RelationalJoinPlanningReason::CostReordered;
    outcome.memo_groups = Some(0);
    outcome.memo_expressions = Some(9);
    outcome.selected_order = vec!["other".into(), "docs".into()];
    outcome.cost = Some(cost);
    outcome.attempts.push(RelationalJoinPlanningAttempt {
        strategy: RelationalJoinPlanningStrategy::CsgCmpMemo,
        status: RelationalJoinPlanningStatus::Fallback,
        reason: RelationalJoinPlanningReason::GroupBudgetExceeded,
        fallback_class: Some(RelationalJoinPlanningFallbackClass::Budget),
        memo_groups: Some(0),
        memo_expressions: Some(9),
        cost: Some(cost),
    });
    let expected = "join_order=cost_reordered, planning_strategy=csg_cmp_memo, planning_status=selected, planning_reason=cost_reordered, memo_groups=0, memo_expressions=9, max_groups=19, max_expressions=23, selected_order=[other,docs], attempts=[0:syntax_order:selected:explicit_syntax_order:fallback_class=none:memo_groups=unavailable:memo_expressions=unavailable:cost=unavailable;1:csg_cmp_memo:fallback:group_budget_exceeded:fallback_class=budget:memo_groups=0:memo_expressions=9:cost=13], parse_nanos=0, bind_nanos=0, plan_nanos=0, execute_nanos=0, estimated_rows=2, plan_cost=13, cpu=3, random_io=5, sequential_io=7, output_rows=11";
    assert_eq!(
        explain_join_planning(&outcome, RelationalSqlStageTimings::default()),
        expected
    );
    outcome.status = RelationalJoinPlanningStatus::Fallback;
    assert_eq!(
        explain_join_planning(&outcome, RelationalSqlStageTimings::default()),
        expected
            .replacen("join_order=cost_reordered", "join_order=syntax_fallback", 1)
            .replacen("planning_status=selected", "planning_status=fallback", 1)
    );
}
mod fixtures;
use fixtures::*;
use hawdb_executor::binding::map_payload_bytes;
use std::cell::Cell;

fn case(seed: usize, index: usize) -> Case {
    let bits = seed
        .wrapping_mul(0x9e37)
        .wrapping_add(index.wrapping_mul(0x85eb));
    Case {
        shape: index % QUERIES.len(),
        n: bits % 32,
        kind: (bits / 7) % 3,
        profile: bits & 1 != 0,
        estimated: [0, 1, 7, usize::MAX][(bits / 3) % 4],
        actual: [None, Some(0), Some(bits % 32), Some(usize::MAX)][(bits / 5) % 4],
        present: bits & 2 != 0,
        spilled: bits & 4 != 0,
    }
}

fn check_case(case: &Case) {
    let select = select(QUERIES[case.shape]);
    for analyze in [false, true] {
        for covered in [false, true] {
            let input = case.input();
            let mut expected = input.clone();
            expected.rows = case.expected_rows(analyze, covered);
            let called = Cell::new(0);
            let actual = format_relational_explain(
                &select,
                &[],
                input,
                analyze,
                limits(),
                |predicate, access, order, table, qualifier| {
                    called.set(called.get() + 1);
                    assert_eq!(predicate, select.selection.as_ref());
                    assert_eq!(access, &case.descriptor());
                    assert_eq!(order, &select.order_by);
                    assert_eq!(table, select.from.name);
                    assert_eq!(
                        qualifier,
                        select.from_alias.as_deref().unwrap_or(&select.from.name)
                    );
                    covered
                },
            )
            .unwrap();
            assert_eq!(called.get(), usize::from(select.selection.is_some()));
            assert_eq!(
                actual, expected,
                "shape={}, analyze={analyze}, covered={covered}",
                case.shape
            );
        }
    }
}

#[test]
fn complete_explain_frames_match_independent_oracle() {
    for seed in 0..4 {
        for index in 0..32 {
            check_case(&case(seed, index));
        }
    }
}

#[test]
#[ignore = "manual deterministic report-contract campaign"]
fn explain_report_differential_campaign() {
    for seed in 0..128 {
        for index in 0..64 {
            check_case(&case(seed, index));
        }
    }
}

#[test]
fn explain_output_limits_accept_exact_boundary_and_refuse_truncation() {
    for analyze in [false, true] {
        let case = case(7, 6);
        let select = select(QUERIES[case.shape]);
        let output = format_relational_explain(
            &select,
            &[],
            case.input(),
            analyze,
            limits(),
            |_, _, _, _, _| false,
        )
        .unwrap();
        let rows = output.rows.len();
        let bytes = output
            .rows
            .iter()
            .map(|row| map_payload_bytes(&row.to_owned_row()))
            .sum();
        let exact = RelationalQueryLimits {
            max_output_rows: rows,
            max_output_payload_bytes: bytes,
            ..limits()
        };
        assert_eq!(
            format_relational_explain(
                &select,
                &[],
                case.input(),
                analyze,
                exact,
                |_, _, _, _, _| false
            )
            .unwrap(),
            output
        );
        for (restricted, expected) in [
            (
                RelationalQueryLimits {
                    max_output_rows: rows - 1,
                    ..exact
                },
                format!("relational SQL output exceeds max_output_rows {}", rows - 1),
            ),
            (
                RelationalQueryLimits {
                    max_output_payload_bytes: bytes - 1,
                    ..exact
                },
                format!(
                    "relational SQL output exceeds max_output_payload_bytes {}",
                    bytes - 1
                ),
            ),
        ] {
            let error = format_relational_explain(
                &select,
                &[],
                case.input(),
                analyze,
                restricted,
                |_, _, _, _, _| false,
            )
            .unwrap_err();
            assert!(
                matches!(error, hawdb_core::HawDBError::Execution(message) if message == expected)
            );
        }
    }
}

#[test]
fn bounds_are_validated_before_coverage_and_output_admission() {
    for sql in [
        "SELECT x FROM docs WHERE x > 1 LIMIT $1",
        "SELECT x FROM docs WHERE x > 1 OFFSET $1",
    ] {
        for parameters in [vec![], vec![Value::Int(-1)]] {
            let select = select(sql);
            let error = format_relational_explain(
                &select,
                &parameters,
                case(0, 2).input(),
                false,
                RelationalQueryLimits {
                    max_output_rows: 0,
                    ..limits()
                },
                |_, _, _, _, _| panic!("invalid bounds must precede coverage"),
            )
            .unwrap_err();
            assert!(!error.to_string().contains("max_output_rows"));
        }
    }
}

#[test]
fn output_push_preserves_utf8_and_refusal_accounting() {
    let row = Row::from([("key".into(), text("é"))]);
    let exact = RelationalQueryLimits {
        max_output_rows: 1,
        max_output_payload_bytes: 5,
        ..limits()
    };
    let mut output = Vec::new();
    let mut bytes = 0;
    push_relational_output(row.clone(), &mut output, &mut bytes, exact).unwrap();
    assert_eq!(bytes, 5);
    let error = push_relational_output(row.clone(), &mut output, &mut bytes, exact).unwrap_err();
    assert!(
        matches!(error, hawdb_core::HawDBError::Execution(message) if message == "relational SQL output exceeds max_output_rows 1")
    );
    assert_eq!(bytes, 5);
    assert_eq!(output, vec![row.clone()]);
    output.clear();
    bytes = 0;
    let error = push_relational_output(
        row.clone(),
        &mut output,
        &mut bytes,
        RelationalQueryLimits {
            max_output_payload_bytes: 4,
            ..exact
        },
    )
    .unwrap_err();
    assert!(
        matches!(error, hawdb_core::HawDBError::Execution(message) if message == "relational SQL output exceeds max_output_payload_bytes 4")
    );
    assert!(output.is_empty());
    assert_eq!(bytes, 5);
    bytes = usize::MAX - 1;
    push_relational_output(
        row,
        &mut output,
        &mut bytes,
        RelationalQueryLimits {
            max_output_payload_bytes: usize::MAX,
            ..exact
        },
    )
    .unwrap();
    assert_eq!(bytes, usize::MAX);
    assert_eq!(output.len(), 1);
}

#[test]
fn report_identity_and_evidence_matching_are_exact() {
    let case = Case {
        kind: 2,
        ..case(3, 0)
    };
    let mut output = case.input();
    let descriptor = case.descriptor();
    let evidence = output.index_execution_evidence[0].clone();
    output.index_execution_evidence[0].table = "unrelated".into();
    assert!(relational_index_evidence(&output, "docs", &descriptor).is_none());
    output.index_execution_evidence.insert(0, evidence.clone());
    output.index_execution_evidence.push(evidence);
    assert!(std::ptr::eq(
        relational_index_evidence(&output, "docs", &descriptor).unwrap(),
        &output.index_execution_evidence[0]
    ));
    let full = RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        ..descriptor
    };
    assert!(relational_index_evidence(&output, "docs", &full).is_none());
    output.operator_cardinality_profiles = vec![case.profile(3, "other"), case.profile(0, "docs")];
    assert_eq!(
        relational_operator_cardinality_profile(&output, RelationalOperatorId::from_plan_index(0))
            .unwrap()
            .table,
        "docs"
    );
    assert!(relational_operator_cardinality_profile(
        &output,
        RelationalOperatorId::from_plan_index(1)
    )
    .is_none());
}

#[test]
fn expression_rendering_preserves_existing_diagnostic_text() {
    for (sql, expected) in [
        (
            "SELECT x FROM docs WHERE NOT (x = $1 OR x IS NULL)",
            "NOT ((x = $1 OR x IS NULL))",
        ),
        (
            "SELECT x FROM docs WHERE x IN (1, 2) AND x NOT IN (3, 4)",
            "(x IN (1, 2) AND x NOT IN (3, 4))",
        ),
        (
            "SELECT x FROM docs WHERE x IS NOT NULL AND x <> 2",
            "(x IS NOT NULL AND x != 2)",
        ),
        (
            "SELECT x FROM docs WHERE x NOT ILIKE 'a%' ESCAPE '!'",
            "x NOT ILIKE a% ESCAPE '!'",
        ),
        (
            "SELECT x FROM docs WHERE x LIKE 'a%' ESCAPE ''",
            "x LIKE a% ESCAPE ''",
        ),
        (
            "SELECT x FROM docs WHERE x <= 2 OR x >= 4",
            "(x <= 2 OR x >= 4)",
        ),
    ] {
        let query = select(sql);
        assert_eq!(
            explain_predicate(query.selection.as_ref().unwrap()),
            expected
        );
    }
    let query =
        select("SELECT COALESCE(SUM(x), 0), COUNT(DISTINCT x) FILTER (WHERE x > 1) FROM docs");
    assert_eq!(
        explain_aggregate_projections(&query.projection),
        "coalesce(sum(x)), count(x) FILTER (x > 1)"
    );
    let SelectProjection::Expression { expression, .. } = &query.projection[1] else {
        panic!("expected expression")
    };
    assert_eq!(
        explain_expression(expression),
        "count(DISTINCT x) FILTER (WHERE x > 1)"
    );
}

#[test]
fn single_count_distinct_shape_preserves_filter_and_alias() {
    let query = select("SELECT COUNT(DISTINCT d.x) FILTER (WHERE d.x > 1) AS total FROM docs d");
    let (column, alias, filter) = single_count_distinct_column(&query).unwrap();
    assert_eq!(column.name, "x");
    assert_eq!(column.qualifier.as_deref(), Some("d"));
    assert_eq!(alias, "total");
    assert_eq!(explain_predicate(filter.unwrap()), "d.x > 1");
    assert_eq!(
        single_count_distinct_column(&select(QUERIES[5])).unwrap().1,
        "count"
    );
    for sql in [
        "SELECT COUNT(x) FROM docs",
        "SELECT SUM(DISTINCT x) FROM docs",
        "SELECT COUNT(DISTINCT 1) FROM docs",
        "SELECT COUNT(DISTINCT x), x FROM docs",
        "SELECT COUNT(DISTINCT x) FROM docs GROUP BY x",
        "SELECT COUNT(DISTINCT x) FROM docs HAVING COUNT(*) > 1",
        "SELECT COUNT(DISTINCT x) FROM docs ORDER BY x",
        "SELECT * FROM docs",
    ] {
        assert!(
            single_count_distinct_column(&select(sql)).is_none(),
            "{sql}"
        );
    }
}

#[test]
fn explain_estimated_rows_never_render_zero() {
    assert_eq!(
        optional_estimated_rows_explain_value(Some(0)),
        Value::Int(1)
    );
    assert_eq!(optional_estimated_rows_explain_value(None), Value::Null);
    assert_eq!(optional_usize_explain_value(Some(0)), Value::Int(0));
}
