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
        "CREATE TABLE edges (id BIGINT PRIMARY KEY, parent BIGINT, child BIGINT)",
        "INSERT INTO edges (id, parent, child) VALUES (1, 1, 2), (2, 2, 3), (3, 3, 1), (4, 2, 4)",
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

#[test]
fn recursive_ctes_iterate_working_tables_and_preserve_union_semantics() {
    let state = state();
    let output = query_with_limits(
        &state,
        "WITH RECURSIVE walk(n) AS (\
             VALUES ($1) UNION ALL SELECT n + 1 FROM walk WHERE n < $2\
         ) SELECT n FROM walk ORDER BY n",
        &[Value::Int(1), Value::Int(4)],
        batched_index_join_limits(),
    )
    .unwrap();
    assert_eq!(
        values(&output),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
    assert_eq!(output.row_execution_evidence.recursive_working_rows_peak, 1);
    assert_eq!(output.row_execution_evidence.recursive_rows_emitted, 4);
    assert!(output.row_execution_evidence.recursive_iterations >= 4);
    assert!(output
        .blocking_operator_memory_reports
        .iter()
        .any(|report| report.operator == "RecursiveUnionExec"));

    assert_eq!(
        values(&query(
            &state,
            "WITH RECURSIVE reach(id) AS (\
                 VALUES (1) UNION \
                 SELECT edges.child FROM edges JOIN reach ON edges.parent = reach.id\
             ) SELECT id FROM reach ORDER BY id"
        )),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );

    assert_eq!(
        values(&query(
            &state,
            "WITH RECURSIVE duplicate(n) AS (\
                 VALUES (1), (1) UNION ALL SELECT n + 1 FROM duplicate WHERE n < 2\
             ) SELECT n FROM duplicate ORDER BY n"
        )),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(2)]
        ]
    );

    assert_eq!(
        values(&query(
            &state,
            "WITH RECURSIVE nullable(n) AS (\
                 VALUES (CAST(NULL AS BIGINT)) UNION SELECT n FROM nullable\
             ) SELECT n FROM nullable"
        )),
        vec![vec![Value::Null]]
    );
    assert_eq!(
        values(&query(
            &state,
            "WITH RECURSIVE start(n) AS (VALUES (1)), walk(n) AS (\
                 SELECT n FROM start UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
             ) SELECT n FROM walk ORDER BY n"
        )),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    let empty = query(
        &state,
        "WITH RECURSIVE empty(n) AS (\
             SELECT id FROM numbers WHERE id < 0 UNION ALL SELECT n + 1 FROM empty\
         ) SELECT n FROM empty",
    );
    assert!(empty.rows.is_empty());
    assert_eq!(empty.row_execution_evidence.recursive_iterations, 0);
}

#[test]
fn recursive_ctes_fail_closed_on_shape_work_iteration_output_and_memory_caps() {
    let state = state();
    for (sql, expected) in [
        (
            "WITH walk(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM walk) SELECT n FROM walk",
            "unknown relational table walk",
        ),
        (
            "WITH RECURSIVE walk(n) AS (SELECT n FROM walk) SELECT n FROM walk",
            "requires seed UNION [ALL] recursive term",
        ),
        (
            "WITH RECURSIVE walk(n) AS (SELECT n FROM walk UNION ALL VALUES (1)) SELECT n FROM walk",
            "not allowed in the non-recursive seed",
        ),
        (
            "WITH RECURSIVE walk(n) AS (VALUES (1) UNION ALL SELECT n AS a, n AS b FROM walk) SELECT n FROM walk",
            "seed arity 1 and recursive-term arity 2",
        ),
        (
            "WITH RECURSIVE walk(n) AS (VALUES (1) UNION ALL SELECT 'text' FROM walk) SELECT n FROM walk",
            "incompatible types",
        ),
    ] {
        let error = query_with_limits(&state, sql, &[], batched_index_join_limits()).unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error for {sql}: {error}"
        );
    }

    let total_work = query_with_limits(
        &state,
        "WITH RECURSIVE walk(n) AS (\
             VALUES (1) UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
         ) SELECT n FROM walk",
        &[],
        RelationalQueryLimits {
            max_candidate_work: 3,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(total_work
        .to_string()
        .contains("recursive_total_work(max_candidate_work)"));

    let working_rows = query_with_limits(
        &state,
        "WITH RECURSIVE fanout(n) AS (\
             VALUES (1) UNION ALL \
             SELECT numbers.id FROM numbers JOIN fanout ON fanout.n = 1 WHERE numbers.id <= 2\
         ) SELECT n FROM fanout",
        &[],
        RelationalQueryLimits {
            max_intermediate_rows: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(
        working_rows.to_string().contains("max_intermediate_rows"),
        "unexpected working-row refusal: {working_rows}"
    );

    let iteration = query_with_limits(
        &state,
        "WITH RECURSIVE forever(n) AS (\
             VALUES (1) UNION ALL SELECT n FROM forever\
         ) SELECT n FROM forever",
        &[],
        RelationalQueryLimits {
            max_output_rows: 2_048,
            max_output_payload_bytes: 1024 * 1024,
            max_intermediate_rows: 2_048,
            max_candidate_work: 1_000_000,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(iteration
        .to_string()
        .contains("recursive_iteration_limit 1024"));

    let output_rows = query_with_limits(
        &state,
        "WITH RECURSIVE walk(n) AS (\
             VALUES (1) UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
         ) SELECT n FROM walk",
        &[],
        RelationalQueryLimits {
            max_output_rows: 2,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(output_rows.to_string().contains("max_output_rows 2"));

    let output_payload = query_with_limits(
        &state,
        "WITH RECURSIVE wide(v, n) AS (\
             VALUES ('wide', 1) UNION ALL SELECT v, n + 1 FROM wide WHERE n < 2\
         ) SELECT v FROM wide",
        &[],
        RelationalQueryLimits {
            max_output_payload_bytes: 1,
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(output_payload
        .to_string()
        .contains("max_output_payload_bytes 1"));

    let cumulative_row_reads = query_with_limits(
        &state,
        "WITH RECURSIVE reach(id) AS (\
             VALUES (1) UNION \
             SELECT edges.child FROM edges JOIN reach ON edges.parent = reach.id\
         ) SELECT id FROM reach",
        &[],
        RelationalQueryLimits {
            row_read: hawdb_storage::relational::RelationalRowPageSnapshotReadLimits {
                demand: hawdb_storage::relational::RelationalRowPageDemandReadLimits {
                    max_rows: NonZeroUsize::new(4).unwrap(),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..batched_index_join_limits()
        },
    )
    .unwrap_err();
    assert!(
        cumulative_row_reads
            .to_string()
            .contains("row scan exceeds row limit"),
        "recursive iterations reset the cumulative row-read limit: {cumulative_row_reads}"
    );

    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(100).unwrap(),
        ..hawdb_executor::ExecutionMemoryConfig::default()
    };
    let memory_error = query_with_resources(
        &state,
        "WITH RECURSIVE walk(n) AS (\
             VALUES (1) UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
         ) SELECT n FROM walk",
        &[],
        batched_index_join_limits(),
        &memory,
    )
    .unwrap_err();
    let HawDBError::ResourceBudgetExceeded(cause) = memory_error else {
        panic!("recursive union lost its typed memory refusal: {memory_error}");
    };
    assert_eq!(cause.resource, hawdb_core::ResourceBudgetKind::MemoryBytes);
    assert_eq!(cause.scope, hawdb_core::ResourceBudgetScope::Operator);
    assert_eq!(cause.owner, "relational recursive union");
    assert_eq!(cause.memory_class, Some("blocking_state"));
    assert_eq!(cause.budget, 100);
    assert!(cause.used.saturating_add(cause.requested) > cause.budget);
}

#[test]
fn recursive_cte_explain_reports_plan_and_actual_work() {
    let state = state();
    let output = query(
        &state,
        "EXPLAIN WITH RECURSIVE walk(n) AS (\
             VALUES (1) UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
         ) SELECT n FROM walk",
    );
    assert_eq!(
        output.rows.row(0).unwrap().get("id"),
        Some(&Value::String("RecursiveUnionExec".into()))
    );
    assert!(matches!(
        output.rows.row(0).unwrap().get("operator info"),
        Some(Value::String(info)) if info.contains("recursive_ctes=1")
            && info.contains("recursive_strategy=working_table")
            && info.contains("recursive_iteration_limit=1024")
    ));
    assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");

    let analyzed = query(
        &state,
        "EXPLAIN ANALYZE WITH RECURSIVE walk(n) AS (\
             VALUES (1) UNION ALL SELECT n + 1 FROM walk WHERE n < 3\
         ) SELECT n FROM walk",
    );
    assert!(matches!(
        analyzed.rows.row(0).unwrap().get("execution info"),
        Some(Value::String(info)) if info.contains("recursive_iterations=3")
            && info.contains("recursive_working_rows_peak=1")
            && info.contains("recursive_rows_emitted=3")
            && info.contains("recursive_work=")
    ));
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
fn exists_binds_limits_and_skips_unneeded_projection() {
    let state = state();
    let sql = "SELECT id FROM numbers WHERE EXISTS (\
                   SELECT 1 / (id - id) FROM numbers LIMIT $1 OFFSET $2\
               ) ORDER BY id";
    for (limit, offset, expected) in [(0, 0, 0), (2, 1, 3), (2, 3, 0)] {
        let output = query_with_limits(
            &state,
            sql,
            &[Value::Int(limit), Value::Int(offset)],
            batched_index_join_limits(),
        )
        .unwrap();
        assert_eq!(output.rows.len(), expected, "LIMIT {limit} OFFSET {offset}");
    }
    assert!(query(
        &state,
        "SELECT id FROM numbers WHERE EXISTS (SELECT DISTINCT n FROM numbers OFFSET 2)"
    )
    .rows
    .is_empty());
    assert!(query(
        &state,
        "SELECT id FROM numbers WHERE EXISTS (\
             SELECT 1 FROM numbers INTERSECT SELECT 2 FROM numbers\
         )"
    )
    .rows
    .is_empty());
    assert_eq!(
        query(
            &state,
            "SELECT id FROM numbers WHERE EXISTS (SELECT COUNT(*) FROM numbers WHERE id = 99)"
        )
        .rows
        .len(),
        3
    );
}

#[test]
fn exists_binds_discarded_projection_and_order_without_evaluating_them() {
    let state = state();
    let ordered = "SELECT id FROM numbers WHERE EXISTS (\
        SELECT 1 / (id - id) FROM edges ORDER BY id\
    ) ORDER BY id";
    assert_eq!(
        values(&query(&state, ordered)),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]
    );
    for (inner, error_part) in [
        ("SELECT missing FROM edges", "column missing"),
        ("SELECT id FROM edges ORDER BY missing", "column missing"),
    ] {
        let sql = format!("SELECT id FROM numbers WHERE EXISTS ({inner})");
        let error = query_with_limits(&state, &sql, &[], batched_index_join_limits()).unwrap_err();
        assert!(
            matches!(error, HawDBError::Semantic(message) if message.contains(error_part)),
            "{sql}"
        );
    }
    let error = query_with_limits(
        &state,
        "SELECT id FROM numbers WHERE EXISTS (SELECT label + 1 FROM numbers)",
        &[],
        batched_index_join_limits(),
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("arithmetic requires numeric operands"));
}

#[test]
fn fromless_aggregate_subquery_filters_the_input_and_retains_the_global_group() {
    let state = state();
    let sql = "SELECT COUNT(*), SUM(7) WHERE EXISTS (SELECT id FROM numbers WHERE id = $1)";
    for (id, expected) in [
        (0, vec![Value::Int(0), Value::Null]),
        (1, vec![Value::Int(1), Value::Int(7)]),
    ] {
        let output =
            query_with_limits(&state, sql, &[Value::Int(id)], batched_index_join_limits()).unwrap();
        assert_eq!(values(&output), vec![expected]);
        assert!(output
            .blocking_operator_memory_reports
            .iter()
            .any(|report| {
                report.operator == "RelationalAggregateExec" && report.input_rows == id as usize
            }));
    }
}

#[test]
fn in_subquery_arity_resolves_visible_ctes_before_catalog_tables() {
    let state = state();
    for query_text in [
        "SELECT id FROM numbers WHERE id IN (\
             WITH numbers(wanted) AS (VALUES (1)) SELECT * FROM numbers\
         )",
        "SELECT id FROM numbers WHERE id IN (\
             WITH numbers(wanted) AS (VALUES (1)), copy AS (SELECT * FROM numbers)\
             SELECT * FROM copy\
         )",
    ] {
        assert_eq!(
            values(&query(&state, query_text)),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn cte_columns_shadow_outer_columns_in_subquery_binding() {
    let state = state();
    for predicate in ["parent = 4", "numbers.parent = 4"] {
        let sql = format!(
            "SELECT id FROM edges WHERE id IN (\
                WITH numbers(parent) AS (VALUES (4)) SELECT * FROM numbers WHERE {predicate}\
            ) ORDER BY id"
        );
        assert_eq!(values(&query(&state, &sql)), vec![vec![Value::Int(4)]]);
    }
    for sql in [
        "SELECT id FROM edges WHERE id IN (\
            WITH numbers AS (SELECT 4 AS parent), filtered AS (\
                SELECT * FROM numbers WHERE parent = 4\
            ) SELECT * FROM filtered\
        )",
        "SELECT id FROM edges WHERE id IN (\
            WITH numbers(wanted) AS (VALUES (4)) SELECT * FROM numbers WHERE parent = 2\
        )",
        "SELECT id FROM edges WHERE id IN (\
            WITH numbers(parent) AS (VALUES (4))\
            SELECT parent FROM numbers WHERE parent IN (SELECT * FROM numbers)\
        )",
    ] {
        assert_eq!(values(&query(&state, sql)), vec![vec![Value::Int(4)]]);
    }
}

#[test]
fn repeated_expensive_in_probes_promote_cached_rows_to_a_bounded_hash() {
    let state = state();
    let right = (100..132)
        .chain([1, 2, 3, 4])
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT id FROM edges WHERE id IN (VALUES {right}, (NULL)) ORDER BY id");
    let limits = RelationalQueryLimits {
        // Admits 37 RHS rows, scans of 33 and 34 candidates, a hash build,
        // and two probes. Four scans would use 176 units.
        max_candidate_work: 150,
        ..batched_index_join_limits()
    };
    let output = query_with_limits(&state, &sql, &[], limits).unwrap();
    assert_eq!(output.rows.len(), 4);
    assert!(output.access_path.name.contains("hashed membership"));
    let report = output
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "SubqueryPredicateExec")
        .unwrap();
    assert_eq!(report.input_rows, 144);
    assert!(report.peak_tracked_bytes > 0);

    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: std::num::NonZeroUsize::new(report.peak_tracked_bytes - 1)
            .unwrap(),
        ..hawdb_executor::ExecutionMemoryConfig::default()
    };
    let scan = query_with_resources(
        &state,
        &sql,
        &[],
        RelationalQueryLimits {
            max_candidate_work: 180,
            ..limits
        },
        &memory,
    )
    .unwrap();
    assert_eq!(scan.rows, output.rows);
    assert!(!scan.access_path.name.contains("hashed membership"));
    assert!(scan
        .blocking_operator_memory_reports
        .iter()
        .any(|report| { report.operator == "SubqueryPredicateExec" && report.input_rows == 176 }));

    for (predicate, expected) in [
        (
            "id IN (VALUES (50), (51), (4), (NULL))",
            vec![vec![Value::Int(4)]],
        ),
        ("id NOT IN (VALUES (50), (51), (4), (NULL))", Vec::new()),
    ] {
        let output = query(&state, &format!("SELECT id FROM edges WHERE {predicate}"));
        assert!(output.access_path.name.contains("hashed membership"));
        assert_eq!(values(&output), expected);
    }
}

#[test]
fn cheap_in_probes_stay_on_the_scan_and_fit_the_work_budget() {
    let state = state();
    let sql = "SELECT id FROM numbers WHERE n IN (VALUES (10), (20)) ORDER BY id";
    let limits = RelationalQueryLimits {
        // One RHS execution, two materialized rows, and 1 + 1 + 2 comparisons.
        // Building a hash table eagerly would use eight units.
        max_candidate_work: 7,
        ..batched_index_join_limits()
    };
    for output in [
        query_with_limits(&state, sql, &[], limits).unwrap(),
        query(&state, sql),
    ] {
        assert_eq!(output.rows.len(), 3);
        assert!(!output.access_path.name.contains("hashed membership"));
    }

    for predicate in [
        "label IN (VALUES ('a'), (NULL))",
        "NOT (label NOT IN (VALUES ('a'), (NULL)))",
        "id IN (VALUES (CAST(1 AS DOUBLE PRECISION)))",
    ] {
        let sql = format!("SELECT id FROM numbers WHERE {predicate}");
        assert_eq!(values(&query(&state, &sql)), vec![vec![Value::Int(1)]]);
    }
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
