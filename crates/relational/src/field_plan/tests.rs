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
use hawdb_sql::SqlStatement;

const COLUMNS: [&str; 5] = ["id", "bucket", "amount", "body", "payload"];

fn state_with_order(order: &[usize; 5]) -> RelationalState {
    let mut state = RelationalState::default();
    for table in ["docs", "peers"] {
        let definitions = order
            .iter()
            .map(|index| match index {
                0 => "id BIGINT PRIMARY KEY",
                1 => "bucket BIGINT",
                2 => "amount BIGINT",
                3 => "body TEXT",
                4 => "payload BYTEA",
                _ => unreachable!(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let transaction = crate::compile_relational_statement_sql(
            &format!("CREATE TABLE {table} ({definitions})"),
            &[],
            &state,
        )
        .unwrap();
        state = state
            .stage_transaction(transaction, Default::default(), Default::default())
            .unwrap();
    }
    state
}

fn select(sql: &str) -> SelectStatement {
    match hawdb_sql::prepare_postgres_sql(sql).unwrap().statement {
        SqlStatement::Select(select) => select,
        _ => panic!("expected SELECT"),
    }
}

fn expected_fields(
    state: &RelationalState,
    masks: &[(&str, u8)],
) -> BTreeMap<String, Arc<[usize]>> {
    masks
        .iter()
        .map(|(table, mask)| {
            let schema = state.table_schema(table).unwrap();
            let ordinals = schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(ordinal, column)| {
                    let index = COLUMNS
                        .iter()
                        .position(|name| *name == column.name)
                        .unwrap();
                    (mask & (1 << index) != 0).then_some(ordinal)
                })
                .collect::<Vec<_>>();
            (table.to_string(), Arc::from(ordinals))
        })
        .collect()
}

fn assert_fields(
    sql: &str,
    state: &RelationalState,
    requested: &[(&str, u8)],
    scanned: &[(&str, u8)],
    metadata_only: &[(&str, u8)],
) {
    let select = select(sql);
    let actual_requested = plan_requested_fields(&select, state).unwrap();
    let actual_scanned = plan_scan_fields(&select, state).unwrap();
    assert_eq!(
        actual_requested,
        expected_fields(state, requested),
        "requested: {sql}"
    );
    assert_eq!(
        actual_scanned,
        expected_fields(state, scanned),
        "scan: {sql}"
    );
    for (fields, masks) in [(&actual_requested, requested), (&actual_scanned, scanned)] {
        let expected = masks
            .iter()
            .map(|(table, mask)| {
                let metadata = metadata_only
                    .iter()
                    .find(|(name, _)| name == table)
                    .map_or(0, |(_, mask)| *mask);
                (*table, *mask & !metadata)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            plan_scan_hydration_fields(&select, state, fields).unwrap(),
            expected_fields(state, &expected),
            "hydration: {sql}",
        );
    }
}

#[test]
fn field_sets_preserve_late_hydration_and_all_clause_inputs() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    for (sql, requested, scanned, metadata) in [
        ("SELECT body FROM docs", 8, 0, 0),
        ("SELECT body FROM docs WHERE bucket = 2 ORDER BY id", 11, 3, 0),
        ("SELECT count(body) FROM docs", 8, 8, 8),
        ("SELECT count(DISTINCT body) FROM docs", 8, 8, 0),
        ("SELECT octet_length(body) FROM docs", 8, 0, 8),
        ("SELECT sum(octet_length(body)) FROM docs", 8, 8, 8),
        ("SELECT coalesce(count(body), 0) FROM docs", 8, 8, 8),
        ("SELECT count(body), max(body) FROM docs", 8, 8, 0),
        ("SELECT count(body) FILTER (WHERE bucket > 0) FROM docs", 10, 10, 0),
        ("SELECT count(body) AS total FROM docs GROUP BY bucket HAVING sum(amount) > 0 ORDER BY total", 14, 14, 8),
        ("SELECT body FROM docs HAVING count(payload) > 0", 24, 24, 16),
        ("SELECT * FROM docs", 31, 0, 0),
        ("SELECT count(*) FROM docs", 0, 0, 0),
    ] {
        assert_fields(
            sql,
            &state,
            &[("docs", requested)],
            &[("docs", scanned)],
            &[("docs", metadata)],
        );
    }
}

#[test]
fn joins_preserve_table_union_qualification_and_empty_bindings() {
    let state = state_with_order(&[4, 2, 0, 3, 1]);
    assert_fields(
        "SELECT d.body, p.amount FROM docs AS d JOIN peers AS p ON d.id = p.id WHERE p.bucket > 0",
        &state,
        &[("docs", 9), ("peers", 7)],
        &[("docs", 1), ("peers", 3)],
        &[],
    );
    assert_fields(
        "SELECT count(d.body), count(p.payload) FROM docs AS d JOIN peers AS p ON d.id = p.id",
        &state,
        &[("docs", 9), ("peers", 17)],
        &[("docs", 9), ("peers", 17)],
        &[("docs", 8), ("peers", 16)],
    );
    assert_fields(
        "SELECT d.body FROM docs AS d JOIN docs AS p ON d.id = p.bucket ORDER BY p.amount",
        &state,
        &[("docs", 15)],
        &[("docs", 7)],
        &[],
    );
    assert_fields(
        "SELECT 1 FROM docs AS d JOIN peers AS p ON d.id = d.id",
        &state,
        &[("docs", 1), ("peers", 0)],
        &[("docs", 1), ("peers", 0)],
        &[],
    );
}

#[test]
fn order_alias_resolution_preserves_identity_and_ambiguity() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    let statement =
        select("SELECT body AS id, count(payload) AS total FROM docs ORDER BY id, total, docs.id");
    let first = resolve_relational_order_target(&statement, &statement.order_by[0]).unwrap();
    let RelationalOrderTarget::ProjectionColumn { column, alias } = first else {
        panic!("column alias must take priority over an unqualified input name");
    };
    assert_eq!(column.name, "body");
    assert_eq!(alias, "id");
    let SelectProjection::Expression { expression, .. } = &statement.projection[0] else {
        panic!("expected projection");
    };
    assert!(std::ptr::eq(column, expression.as_column().unwrap()));
    let RelationalOrderTarget::ProjectionExpression { expression, alias } =
        resolve_relational_order_target(&statement, &statement.order_by[1]).unwrap()
    else {
        panic!("expected expression alias");
    };
    assert_eq!(alias, "total");
    let SelectProjection::Expression {
        expression: projected,
        ..
    } = &statement.projection[1]
    else {
        panic!("expected projection");
    };
    assert!(std::ptr::eq(expression, projected));
    let RelationalOrderTarget::InputColumn(column) =
        resolve_relational_order_target(&statement, &statement.order_by[2]).unwrap()
    else {
        panic!("qualified names must bypass output aliases");
    };
    assert_eq!(column.qualifier.as_deref(), Some("docs"));
    assert_eq!(column.name, "id");
    assert_fields(
        "SELECT body AS id FROM docs ORDER BY id",
        &state,
        &[("docs", 8)],
        &[("docs", 8)],
        &[],
    );
    let ambiguous = select("SELECT body AS x, payload AS x FROM docs ORDER BY x");
    assert_eq!(
        resolve_relational_order_target(&ambiguous, &ambiguous.order_by[0]).unwrap_err(),
        HawDBError::Semantic("ambiguous relational ORDER BY alias x".into()),
    );
}

#[test]
fn field_planning_keeps_exact_errors_and_validation_order() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    for (sql, message) in [
        (
            "SELECT missing FROM absent",
            "unknown relational table absent",
        ),
        (
            "SELECT missing FROM docs JOIN absent ON docs.id = absent.id",
            "unknown relational table absent",
        ),
        (
            "SELECT missing FROM docs",
            "unknown relational column missing",
        ),
        (
            "SELECT id FROM docs JOIN peers ON docs.id = peers.id",
            "ambiguous relational column id",
        ),
        (
            "SELECT docs.id FROM docs d JOIN docs p ON d.id = p.id",
            "ambiguous relational column id",
        ),
        (
            "SELECT d.id FROM docs d JOIN peers d ON docs.id = peers.id",
            "ambiguous relational column id",
        ),
        (
            "SELECT body AS x, payload AS x FROM docs ORDER BY x",
            "ambiguous relational ORDER BY alias x",
        ),
    ] {
        let statement = select(sql);
        assert_eq!(
            plan_requested_fields(&statement, &state).unwrap_err(),
            HawDBError::Semantic(message.into()),
            "{sql}",
        );
        let all = expected_fields(&state, &[("docs", 31), ("peers", 31)]);
        assert_eq!(
            plan_scan_hydration_fields(&statement, &state, &all).unwrap_err(),
            HawDBError::Semantic(message.into()),
            "{sql}",
        );
    }
}

#[test]
fn metadata_exemption_never_overrides_value_consumers() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    for tail in [
        "WHERE body IS NOT NULL",
        "WHERE body LIKE 'x%'",
        "WHERE body IN ('x', 'y')",
        "GROUP BY body",
        "ORDER BY body",
    ] {
        assert_fields(
            &format!("SELECT count(body) FROM docs {tail}"),
            &state,
            &[("docs", 8)],
            &[("docs", 8)],
            &[],
        );
    }
    assert_fields(
        "SELECT count(body) FROM docs HAVING NOT (max(body) = 'x' OR max(payload) IS NULL)",
        &state,
        &[("docs", 24)],
        &[("docs", 24)],
        &[],
    );
}

#[test]
fn index_coverage_keeps_primary_keys_and_fails_closed() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    let schema = state.table_schema("docs").unwrap();
    for required in 0..32 {
        let fields = expected_fields(&state, &[("docs", required)]);
        let plan = RelationalFieldPlan::new(fields.clone(), fields.clone(), fields);
        for indexed in 0..32 {
            let columns = COLUMNS
                .iter()
                .enumerate()
                .filter(|(index, _)| indexed & (1 << index) != 0)
                .map(|(_, column)| column.to_string())
                .collect::<Vec<_>>();
            assert_eq!(
                plan.index_covers_table("docs", schema, &columns).unwrap(),
                required & !(indexed | 1) == 0,
                "required={required}, indexed={indexed}",
            );
        }
        assert!(!plan
            .index_covers_table("docs", schema, &["missing".into()])
            .unwrap());
        assert_eq!(
            plan.index_covers_table("absent", schema, &[]).unwrap_err(),
            HawDBError::StorageIntegrity(
                "relational query has no field plan for table absent".into()
            ),
        );
        assert!(plan.uses_any_table(&BTreeSet::from(["docs".into()])));
        assert!(!plan.uses_any_table(&BTreeSet::from(["peers".into()])));
    }
}

fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn composed_plans_keep_scan_output_and_hydration_boundaries() {
    let state = state_with_order(&[4, 3, 2, 1, 0]);
    for (sql, output, scan, hydration) in [
        ("SELECT body FROM docs", 8, 8, 8),
        ("SELECT body FROM docs ORDER BY id", 9, 1, 1),
        ("SELECT DISTINCT body FROM docs ORDER BY id", 9, 9, 9),
        ("SELECT body AS x FROM docs ORDER BY x", 8, 8, 8),
        (
            "SELECT coalesce(body, 'x') AS x FROM docs ORDER BY x",
            8,
            8,
            8,
        ),
        ("SELECT count(body) FROM docs", 8, 8, 0),
        ("SELECT octet_length(body) FROM docs", 8, 8, 0),
        ("SELECT bucket FROM docs GROUP BY bucket", 2, 2, 2),
        ("SELECT body FROM docs HAVING count(payload) > 0", 24, 24, 8),
    ] {
        let plan = plan_relational_field_plan(&select(sql), &state).unwrap();
        for (actual, expected) in [
            (plan.output_fields("docs").unwrap(), output),
            (plan.scan_fields("docs").unwrap(), scan),
            (plan.scan_hydration_fields("docs").unwrap(), hydration),
        ] {
            assert_eq!(
                actual,
                expected_fields(&state, &[("docs", expected)])["docs"].as_ref(),
                "{sql}"
            );
        }
        assert!(std::ptr::eq(
            plan.scan_fields("docs").unwrap(),
            plan.scan_fields["docs"].as_ref()
        ));
        for result in [
            plan.output_fields("absent"),
            plan.scan_fields("absent"),
            plan.scan_hydration_fields("absent"),
        ] {
            assert_eq!(
                result.unwrap_err(),
                HawDBError::StorageIntegrity(
                    "relational query has no field plan for table absent".into(),
                )
            );
        }
    }
}

#[test]
fn composed_planning_keeps_error_precedence_and_alias_short_circuit() {
    let state = state_with_order(&[0, 1, 2, 3, 4]);
    let statement = select(
        "SELECT coalesce(body, 'x') AS metric, body AS x, payload AS x FROM docs ORDER BY metric, x",
    );
    assert!(order_by_uses_expression_alias(&statement).unwrap());
    let result = plan_relational_field_plan(&statement, &state);
    assert!(matches!(result, Err(HawDBError::Semantic(message))
        if message == "ambiguous relational ORDER BY alias x"));
    let statement = select("SELECT missing FROM absent ORDER BY missing");
    assert!(matches!(plan_relational_field_plan(&statement, &state),
        Err(HawDBError::Semantic(message)) if message == "unknown relational table absent"));
}

#[test]
#[ignore = "explicit local relational field-planning differential campaign"]
fn field_planning_differential_campaign() {
    let mut exercised = [0usize; 10];
    for seed in 1..=128u64 {
        let mut random = seed;
        let mut order = [0, 1, 2, 3, 4];
        for end in (1..order.len()).rev() {
            let swap = next(&mut random) as usize % (end + 1);
            order.swap(end, swap);
        }
        let state = state_with_order(&order);
        for case in 0..64 {
            let shape = next(&mut random) as usize % exercised.len();
            exercised[shape] += 1;
            let (projection, mut requested, mut scanned, mut metadata, aggregate) = match shape {
                0 => ("d.body", 8, 0, 0, false),
                1 => ("count(d.body)", 8, 8, 8, true),
                2 => ("count(DISTINCT d.body)", 8, 8, 0, true),
                3 => ("octet_length(d.body)", 8, 0, 8, false),
                4 => ("sum(octet_length(d.body))", 8, 8, 8, true),
                5 => ("coalesce(count(d.body), 0)", 8, 8, 8, true),
                6 => ("max(d.body)", 8, 8, 0, true),
                7 => ("sum(d.amount)", 4, 4, 0, true),
                8 => ("count(d.body) FILTER (WHERE d.bucket > 0)", 10, 10, 0, true),
                9 => ("coalesce(d.body, 'fallback')", 8, 0, 0, false),
                _ => unreachable!(),
            };
            let mut sql = format!("SELECT {projection} AS metric FROM docs AS d");
            if next(&mut random) & 1 != 0 {
                sql.push_str(&format!(" WHERE d.bucket > {case}"));
                requested |= 2;
                scanned |= 2;
            } else if next(&mut random) & 1 != 0 {
                sql.push_str(" WHERE d.body IS NOT NULL");
                requested |= 8;
                scanned |= 8;
                metadata &= !8;
            }
            if aggregate && next(&mut random) & 1 != 0 {
                sql.push_str(" GROUP BY d.bucket");
                requested |= 2;
                scanned |= 2;
            }
            if next(&mut random) & 1 != 0 {
                sql.push_str(" ORDER BY metric");
                // ORDER BY adds the already specified projection input to the scan.
                scanned |= requested;
                if shape == 0 {
                    metadata = 0;
                }
            } else if !aggregate && next(&mut random) & 1 != 0 {
                sql.push_str(" ORDER BY d.id");
                requested |= 1;
                scanned |= 1;
            }
            assert_fields(
                &sql,
                &state,
                &[("docs", requested)],
                &[("docs", scanned)],
                &[("docs", metadata)],
            );
        }
    }
    assert!(exercised.iter().all(|count| *count > 0));
    eprintln!(
        "field planning: 128 seeds, 8192 cases, 32768 exact map comparisons; shapes={exercised:?}"
    );
}
