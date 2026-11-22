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

fn state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT, label TEXT)",
        "INSERT INTO numbers (id, n, label) VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, NULL)",
    ] {
        let transaction = compile_relational_statement_sql(sql, &[], &state).unwrap();
        state = state
            .stage_transaction(
                transaction,
                RelationalMutationLimits::default(),
                RelationalOverflowConfig::default(),
            )
            .unwrap();
    }
    state
}

fn query_with_limits(
    state: &RelationalState,
    sql: &str,
    parameters: &[Value],
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    query_with_resources(
        state,
        sql,
        parameters,
        limits,
        &hawdb_executor::ExecutionMemoryConfig::default(),
    )
}

fn query_with_resources(
    state: &RelationalState,
    sql: &str,
    parameters: &[Value],
    limits: RelationalQueryLimits,
    memory: &hawdb_executor::ExecutionMemoryConfig,
) -> Result<RelationalQueryOutput> {
    execute_relational_query_sql_with_runtime(
        sql,
        parameters,
        state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
            RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
        ),
        limits,
        memory,
        None,
    )
}

fn query(state: &RelationalState, sql: &str) -> RelationalQueryOutput {
    query_with_limits(state, sql, &[], batched_index_join_limits())
        .unwrap_or_else(|error| panic!("query failed: {sql}: {error}"))
}

fn values(output: &RelationalQueryOutput) -> Vec<Vec<Value>> {
    output.rows.value_rows().map(<[Value]>::to_vec).collect()
}

#[test]
fn values_and_derived_tables_preserve_types_nulls_duplicates_aliases_and_budgets() {
    let state = state();
    let output = query_with_limits(
        &state,
        "VALUES ($1, 'a'), ($2, NULL), ($1, 'a') ORDER BY column1, column2 NULLS FIRST",
        &[Value::Int(2), Value::Int(1)],
        batched_index_join_limits(),
    )
    .unwrap();
    assert_eq!(
        values(&output),
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::String("a".into())],
            vec![Value::Int(2), Value::String("a".into())],
        ]
    );
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "ValuesExec"));

    let output = query(
        &state,
        "SELECT input.id, input.label FROM \
         (VALUES (2, 'b'), (1, 'a'), (1, 'a')) input(id, label) \
         ORDER BY input.id, input.label",
    );
    assert_eq!(
        values(&output),
        vec![
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(1), Value::String("a".into())],
            vec![Value::Int(2), Value::String("b".into())],
        ]
    );
    assert!(output.access_path.name.contains("CTE Materialize"));

    assert_eq!(
        values(&query(
            &state,
            "WITH __hawdb_derived_0_100(v) AS (VALUES (99)) \
             SELECT input.id FROM (VALUES (2), (1)) AS input(id) ORDER BY id"
        )),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );

    let error = query_with_limits(
        &state,
        "VALUES (1), (2)",
        &[],
        RelationalQueryLimits {
            max_candidate_work: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_candidate_work"));

    let error = query_with_limits(
        &state,
        "VALUES (1), (2)",
        &[],
        RelationalQueryLimits {
            max_intermediate_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_intermediate_rows"));
}

#[test]
fn exists_and_in_subqueries_preserve_correlation_rows_duplicates_and_null_truth() {
    let state = state();
    assert_eq!(
        values(&query(
            &state,
            "SELECT outer_row.id FROM numbers AS outer_row \
             WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                           WHERE inner_row.n = outer_row.n \
                             AND inner_row.id <> outer_row.id) \
             ORDER BY outer_row.id"
        )),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT outer_row.id FROM numbers AS outer_row \
             WHERE NOT EXISTS (SELECT 1 FROM numbers AS inner_row \
                               WHERE inner_row.n = outer_row.n \
                                 AND inner_row.id <> outer_row.id) \
             ORDER BY outer_row.id"
        )),
        vec![vec![Value::Int(3)]]
    );

    let output = query_with_limits(
        &state,
        "SELECT id FROM numbers WHERE id IN \
         (SELECT id FROM numbers WHERE n = $1 UNION ALL SELECT id FROM numbers WHERE n = $1) \
         ORDER BY id",
        &[Value::Int(10)],
        batched_index_join_limits(),
    )
    .unwrap();
    assert_eq!(
        values(&output),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert!(output.access_path.name.contains("SubqueryPredicateExec"));
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "SubqueryPredicateExec"));

    assert_eq!(
        values(&query(
            &state,
            "SELECT id FROM numbers WHERE (n, label) IN \
             (SELECT n, label FROM numbers WHERE id <= 2) ORDER BY id"
        )),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert!(query(
        &state,
        "SELECT id FROM numbers WHERE id NOT IN (VALUES (NULL))"
    )
    .rows
    .is_empty());

    assert_eq!(
        values(&query(
            &state,
            "SELECT outer_row.id FROM numbers AS outer_row \
             WHERE outer_row.id = 1 OR EXISTS \
                   (SELECT 1 FROM numbers AS inner_row \
                    WHERE inner_row.id = outer_row.id AND inner_row.label IS NULL) \
             ORDER BY outer_row.id"
        )),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );

    assert!(query(
        &state,
        "SELECT id FROM numbers WHERE EXISTS (SELECT 1 FROM numbers WHERE id = 99)"
    )
    .rows
    .is_empty());
    assert_eq!(
        values(&query(
            &state,
            "SELECT id FROM numbers WHERE id NOT IN \
             (SELECT id FROM numbers WHERE id = 99) ORDER BY id"
        )),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT outer_row.id FROM numbers AS outer_row \
             WHERE EXISTS (SELECT 1 FROM numbers AS outer_row WHERE outer_row.id = 3) \
             ORDER BY outer_row.id"
        )),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT outer_row.id FROM numbers AS outer_row \
             WHERE EXISTS (SELECT 1 FROM numbers AS middle_row \
                           WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                                         WHERE inner_row.id = middle_row.id \
                                           AND outer_row.id = 1)) \
             ORDER BY outer_row.id"
        )),
        vec![vec![Value::Int(1)]]
    );

    let arity = query_with_limits(
        &state,
        "SELECT id FROM numbers WHERE id < 0 AND (id, label) IN (SELECT id FROM numbers)",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(arity.to_string().contains("left row has arity 2"));

    let incompatible = query_with_limits(
        &state,
        "SELECT id FROM numbers WHERE id IN (VALUES ('not-a-number'))",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(incompatible.to_string().contains("incompatible types"));

    let work = query_with_limits(
        &state,
        "SELECT outer_row.id FROM numbers AS outer_row \
         WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                       WHERE inner_row.n = outer_row.n)",
        &[],
        RelationalQueryLimits {
            max_candidate_work: 4,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(work.to_string().contains("max_candidate_work"));

    let row_limit = query_with_limits(
        &state,
        "SELECT id FROM numbers WHERE id IN (VALUES (1), (2)) ORDER BY id",
        &[],
        RelationalQueryLimits {
            max_output_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(row_limit.to_string().contains("max_output_rows 1"));

    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::MIN,
        ..hawdb_executor::ExecutionMemoryConfig::default()
    };
    let memory_error = query_with_resources(
        &state,
        "SELECT id FROM numbers WHERE id IN (VALUES (1), (2))",
        &[],
        batched_index_join_limits(),
        &memory,
    )
    .unwrap_err();
    assert_blocking_memory_refusal(&memory_error, "relational VALUES materialization");
}

#[test]
fn set_operations_preserve_duplicates_nulls_precedence_and_branch_tails() {
    let state = state();
    assert_eq!(
        values(&query(
            &state,
            "SELECT n FROM numbers UNION SELECT n FROM numbers ORDER BY n"
        )),
        vec![vec![Value::Int(10)], vec![Value::Int(20)]]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT n FROM numbers INTERSECT ALL SELECT 10 AS n FROM numbers ORDER BY n"
        )),
        vec![vec![Value::Int(10)], vec![Value::Int(10)]]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT label FROM numbers EXCEPT ALL SELECT NULL AS label ORDER BY label NULLS FIRST"
        )),
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("b".into())]
        ]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT 1 AS v UNION SELECT 2 AS v INTERSECT SELECT 2 AS v ORDER BY v"
        )),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_eq!(
        values(&query(
            &state,
            "(SELECT n FROM numbers ORDER BY id DESC LIMIT 1) UNION ALL SELECT 5 AS n ORDER BY n"
        )),
        vec![vec![Value::Int(5)], vec![Value::Int(20)]]
    );
}

#[test]
fn ctes_materialize_in_order_apply_aliases_and_shadow_catalog_tables() {
    let state = state();
    let output = query(
        &state,
        "WITH selected(v) AS (SELECT n FROM numbers WHERE id <= 2), \
         doubled(result) AS (SELECT v + v FROM selected) \
         SELECT result FROM doubled ORDER BY result",
    );
    assert_eq!(
        values(&output),
        vec![vec![Value::Int(20)], vec![Value::Int(20)]]
    );
    assert!(output.access_path.name.contains("CTE Materialize"));

    let output = query(
        &state,
        "WITH numbers(v) AS (SELECT 7) SELECT v FROM numbers",
    );
    assert_eq!(values(&output), vec![vec![Value::Int(7)]]);

    let output = query(
        &state,
        "WITH numbers(v) AS (SELECT n FROM numbers WHERE id = 1) SELECT v FROM numbers",
    );
    assert_eq!(values(&output), vec![vec![Value::Int(10)]]);

    let output = query(
        &state,
        "WITH scoped(v) AS (SELECT 1) SELECT v FROM scoped UNION ALL \
         (WITH scoped(v) AS (SELECT 2) SELECT v FROM scoped) ORDER BY v",
    );
    assert_eq!(
        values(&output),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );

    let output = query(
        &state,
        "WITH empty(v) AS (SELECT n FROM numbers WHERE id < 0) SELECT v FROM empty",
    );
    assert!(output.rows.is_empty());

    let output = query(
        &state,
        "WITH empty(v) AS (SELECT uuidv7() FROM numbers WHERE id < 0) \
         SELECT v FROM empty UNION ALL SELECT uuidv7() AS v",
    );
    assert_eq!(output.rows.len(), 1);
    let output_values = values(&output);
    assert!(matches!(
        output_values.as_slice(),
        [row] if matches!(row.as_slice(), [Value::Uuid(_)])
    ));

    let output = query_with_limits(
        &state,
        "WITH selected(v) AS (SELECT n FROM numbers) \
         SELECT v FROM selected ORDER BY v LIMIT 1",
        &[],
        RelationalQueryLimits {
            max_output_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap();
    assert_eq!(values(&output), vec![vec![Value::Int(10)]]);

    let output = query(&state, "WITH unused(v) AS (SELECT 1 / 0) SELECT 1 AS v");
    assert_eq!(values(&output), vec![vec![Value::Int(1)]]);
    assert!(!output.access_path.name.contains("CTE Materialize"));
    let error = query_with_limits(
        &state,
        "WITH unused(v) AS (SELECT id FROM missing) SELECT 1 AS v",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown relational table missing"),
        "unexpected error: {error}"
    );
}

#[test]
fn set_operations_resolve_numeric_common_types_and_fail_closed_on_shape_or_work() {
    let state = state();
    assert_eq!(
        values(&query(
            &state,
            "SELECT 1 AS v UNION ALL SELECT CAST(2.5 AS DOUBLE PRECISION) AS v ORDER BY v"
        )),
        vec![vec![Value::Float(1.0)], vec![Value::Float(2.5)]]
    );
    assert_eq!(
        values(&query(
            &state,
            "SELECT 1.5 AS v UNION ALL SELECT CAST(2.5 AS DOUBLE PRECISION) AS v ORDER BY v"
        )),
        vec![vec![Value::Float(1.5)], vec![Value::Float(2.5)]]
    );
    let error = query_with_limits(
        &state,
        "SELECT 1 AS a UNION SELECT 1 AS a, 2 AS b",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("arity 1 and 2"),
        "unexpected error: {error}"
    );

    let error = query_with_limits(
        &state,
        "SELECT 1 AS v UNION ALL SELECT 2 AS v",
        &[],
        RelationalQueryLimits {
            max_intermediate_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_intermediate_rows"));

    let error = query_with_limits(
        &state,
        "SELECT 1 AS v UNION ALL SELECT 2 AS v",
        &[],
        RelationalQueryLimits {
            max_output_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_output_rows 1"));

    let error = query_with_limits(
        &state,
        "SELECT 'wide' AS v UNION ALL SELECT 'rows' AS v",
        &[],
        RelationalQueryLimits {
            max_output_payload_bytes: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_output_payload_bytes 1"));

    let error = query_with_limits(
        &state,
        "SELECT 1 AS v UNION ALL SELECT 2 AS v",
        &[],
        RelationalQueryLimits {
            max_candidate_work: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("max_candidate_work"));

    let error = query_with_limits(
        &state,
        "SELECT 1 AS v UNION SELECT 'text' AS v",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("incompatible types"),
        "unexpected error: {error}"
    );

    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::MIN,
        ..hawdb_executor::ExecutionMemoryConfig::default()
    };
    let error = query_with_resources(
        &state,
        "SELECT n FROM numbers UNION SELECT n FROM numbers",
        &[],
        batched_index_join_limits(),
        &memory,
    )
    .unwrap_err();
    assert_blocking_memory_refusal(&error, "relational set operation");

    for (sql, expected) in [
        (
            "WITH duplicate AS (SELECT 1), duplicate AS (SELECT 2) SELECT * FROM duplicate",
            "specified more than once",
        ),
        (
            "WITH shaped(a, b) AS (SELECT 1) SELECT * FROM shaped",
            "declares 2 columns but returns 1",
        ),
        (
            "WITH shaped(a, a) AS (SELECT 1 AS x, 2 AS y) SELECT * FROM shaped",
            "repeats output column a",
        ),
        (
            "WITH first(v) AS (SELECT v FROM later), later(v) AS (SELECT 1) SELECT v FROM first",
            "unknown relational table later",
        ),
    ] {
        let error = query_with_limits(&state, sql, &[], batched_index_join_limits()).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error for {sql}: {error}"
        );
    }
}

#[test]
fn compound_explain_is_observable_and_plain_explain_does_not_execute() {
    let state = state();
    let output = query(
        &state,
        "EXPLAIN WITH broken(v) AS (SELECT 1 / 0) \
         SELECT v FROM broken UNION SELECT 2 AS v",
    );
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows.row(0).unwrap().get("id"),
        Some(&Value::String("CteMaterializeExec".into()))
    );
    assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");
    assert!(matches!(
        output.rows.row(0).unwrap().get("access object"),
        Some(Value::String(name)) if name.contains("CTE Materialize")
            && name.contains("SetOperation")
    ));
    let error = query_with_limits(
        &state,
        "EXPLAIN SELECT id FROM missing UNION SELECT 2 AS id",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unknown relational table missing"),
        "unexpected error: {error}"
    );

    let output = query(
        &state,
        "EXPLAIN ANALYZE WITH selected(v) AS (SELECT n FROM numbers WHERE id <= 2) \
         SELECT v FROM selected UNION SELECT 30 AS v",
    );
    assert_eq!(
        output.rows.row(0).unwrap().get("actRows"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        output.rows.row(0).unwrap().get("execution info"),
        Some(&Value::String("executed=true".into()))
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("memory"),
        Some(Value::String(memory)) if memory.contains("operators=")
            && memory.contains("budget=")
    ));

    let output = query(
        &state,
        "EXPLAIN WITH outer_cte(v) AS (SELECT 1) SELECT v FROM outer_cte UNION ALL \
         (WITH inner_cte(v) AS (SELECT 2) SELECT v FROM inner_cte)",
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("operator info"),
        Some(Value::String(info)) if info.contains("ctes=2")
            && info.contains("set_operations=1")
    ));

    let output = query(
        &state,
        "EXPLAIN SELECT outer_row.id FROM numbers AS outer_row \
         WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                       WHERE inner_row.n = outer_row.n)",
    );
    assert_eq!(
        output.rows.row(0).unwrap().get("id"),
        Some(&Value::String("SubqueryPredicateExec".into()))
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("operator info"),
        Some(Value::String(info)) if info.contains("subqueries=1")
            && info.contains("subquery_strategy=budgeted_nested_loop")
    ));
    assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");

    let output = query(
        &state,
        "EXPLAIN ANALYZE SELECT outer_row.id FROM numbers AS outer_row \
         WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                       WHERE inner_row.n = outer_row.n)",
    );
    assert_eq!(
        output.rows.row(0).unwrap().get("actRows"),
        Some(&Value::Int(3))
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("access object"),
        Some(Value::String(name)) if name.contains("SubqueryPredicateExec")
    ));

    let output = query(
        &state,
        "EXPLAIN ANALYZE SELECT n FROM numbers WHERE id < 0 UNION SELECT 1 AS n",
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("estRows"),
        Some(Value::Int(estimate)) if *estimate >= 1
    ));
}
