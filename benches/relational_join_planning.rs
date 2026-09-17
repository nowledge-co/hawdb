use serde_json::json;
use skein::optimizer::RelationalAccessPathKind;
use skein::{
    Database, DatabaseConfig, DatabaseReadTransaction, ProfiledRelationalSqlQueryOutput,
    QueryStreamOptions, RelationalJoinPlanningStatus, RelationalJoinPlanningStrategy, Value,
};
use std::hint::black_box;

const MIN_TABLES: usize = 2;
const MAX_TABLES: usize = 8;
const WARMUPS: usize = 3;
const SAMPLES: usize = 31;
// Expressions include admitted physical implementations: three for the first
// filtered edge, then two for each additional edge. Forward probes cost
// 4 + 5 * (tables - 1); reverse primary-key probes cost 7 + 4 * (tables - 1).
// The reverse chain becomes cheaper at five tables.
const EXPECTED_PLAN_SIGNATURES: [(usize, usize, u64); MAX_TABLES - MIN_TABLES + 1] = [
    (3, 7, 9),
    (6, 16, 14),
    (10, 31, 19),
    (15, 54, 23),
    (21, 87, 27),
    (28, 132, 31),
    (36, 191, 35),
];

fn main() {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(64),
        ..DatabaseConfig::default()
    });
    seed_tables(&mut database);
    let read = database.begin_read_transaction();
    let results = (MIN_TABLES..=MAX_TABLES)
        .map(|table_count| measure(&read, table_count))
        .collect::<Vec<_>>();

    println!(
        "relational_join_planning {}",
        json!({
            "protocol": "skein-relational-join-planning-v2",
            "table_range": [MIN_TABLES, MAX_TABLES],
            "warmups": WARMUPS,
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn seed_tables(database: &mut Database) {
    database
        .query_sql("CREATE TABLE planning_t0 (id BIGINT PRIMARY KEY)")
        .expect("create planning_t0");
    database
        .query_sql("INSERT INTO planning_t0 (id) VALUES (1)")
        .expect("seed planning_t0");
    for table in 1..MAX_TABLES {
        database
            .query_sql(&format!(
                "CREATE TABLE planning_t{table} (\
                 id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL)"
            ))
            .unwrap_or_else(|error| panic!("create planning_t{table}: {error}"));
        database
            .query_sql(&format!(
                "CREATE INDEX idx_planning_t{table}_parent \
                 ON planning_t{table} (parent_id)"
            ))
            .unwrap_or_else(|error| panic!("index planning_t{table}: {error}"));
        database
            .query_sql(&format!(
                "INSERT INTO planning_t{table} (id, parent_id) VALUES (1, 1)"
            ))
            .unwrap_or_else(|error| panic!("seed planning_t{table}: {error}"));
    }
}

fn measure(read: &DatabaseReadTransaction, table_count: usize) -> serde_json::Value {
    let sql = join_sql(table_count);
    let cold = execute(read, &sql);
    assert_profile_contract(&cold, table_count);
    let cold_parse_nanos = cold.profile.stage_timings.parse_nanos;
    for _ in 1..WARMUPS {
        let warmup = execute(read, &sql);
        assert_profile_contract(&warmup, table_count);
        assert_eq!(warmup.profile.stage_timings.parse_nanos, 0);
    }

    let cache_before = read.relational_plan_template_cache_stats();
    let mut bind_samples = Vec::with_capacity(SAMPLES);
    let mut plan_samples = Vec::with_capacity(SAMPLES);
    let mut execute_samples = Vec::with_capacity(SAMPLES);
    let mut expected_signature = None;
    for _ in 0..SAMPLES {
        let profiled = execute(read, &sql);
        assert_profile_contract(&profiled, table_count);
        assert_eq!(profiled.profile.stage_timings.parse_nanos, 0);
        let planning = &profiled.profile.join_planning;
        let signature = (
            planning.memo_groups.expect("selected plan has memo groups"),
            planning
                .memo_expressions
                .expect("selected plan has memo expressions"),
            planning
                .cost
                .expect("selected plan has a canonical cost")
                .cost,
        );
        if let Some(expected) = expected_signature {
            assert_eq!(
                signature, expected,
                "planning signature changed within one run"
            );
        } else {
            expected_signature = Some(signature);
        }
        bind_samples.push(profiled.profile.stage_timings.bind_nanos);
        plan_samples.push(profiled.profile.stage_timings.plan_nanos);
        execute_samples.push(profiled.profile.stage_timings.execute_nanos);
        black_box(profiled.output.rows);
    }
    let cache_after = read.relational_plan_template_cache_stats();
    assert_eq!(cache_after.hits, cache_before.hits + SAMPLES as u64);
    let (memo_groups, memo_expressions, plan_cost) =
        expected_signature.expect("benchmark records at least one sample");
    assert_eq!(
        (memo_groups, memo_expressions, plan_cost),
        EXPECTED_PLAN_SIGNATURES[table_count - MIN_TABLES],
        "stable relational join planning signature changed"
    );

    json!({
        "tables": table_count,
        "sql_bytes": sql.len(),
        "cold_parse_ns": cold_parse_nanos,
        "bind_ns": percentiles(bind_samples),
        "plan_ns": percentiles(plan_samples),
        "execute_ns": percentiles(execute_samples),
        "memo_groups": memo_groups,
        "memo_expressions": memo_expressions,
        "plan_cost": plan_cost,
        "attempt_count": 1,
        "selected_strategy": RelationalJoinPlanningStrategy::CsgCmpMemo.as_str(),
        "selected_order": cold.profile.join_planning.selected_order,
        "access_paths": cold.profile.operator_cardinality_profiles.iter().map(|operator| {
            json!({
                "table": operator.table,
                "operator": operator.operator.as_str(),
                "access": operator.access_path.name,
                "covering": operator.access_path.covering,
            })
        }).collect::<Vec<_>>(),
    })
}

fn execute(read: &DatabaseReadTransaction, sql: &str) -> ProfiledRelationalSqlQueryOutput {
    read.query_sql_with_params_options_profiled(
        sql,
        &[Value::Int(1)],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(4 * 1024),
        },
    )
    .expect("execute relational join planning benchmark query")
}

fn assert_profile_contract(profiled: &ProfiledRelationalSqlQueryOutput, table_count: usize) {
    assert_eq!(profiled.output.rows.len(), 1);
    assert!(matches!(
        profiled.output.rows.row(0).and_then(|row| row.get("id")),
        Some(Value::Int(1))
    ));
    let planning = &profiled.profile.join_planning;
    let reverse = table_count >= 5;
    let mut tables = (0..table_count).collect::<Vec<_>>();
    if reverse {
        tables.reverse();
    }
    assert_eq!(
        planning.selected_order,
        tables
            .iter()
            .map(|table| format!("t{table}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        profiled.profile.operator_cardinality_profiles.len(),
        table_count
    );
    for (position, (operator, table)) in profiled
        .profile
        .operator_cardinality_profiles
        .iter()
        .zip(tables)
        .enumerate()
    {
        assert_eq!(operator.table, format!("planning_t{table}"));
        let expected_access = match (position == 0, reverse) {
            (true, true) => RelationalAccessPathKind::FullScan,
            (true, false) | (false, true) => RelationalAccessPathKind::PrimaryKey,
            (false, false) => RelationalAccessPathKind::Index,
        };
        assert_eq!(operator.access_path.kind, expected_access);
        assert_eq!(operator.actual_rows, Some(1));
    }
    assert_eq!(
        planning.strategy,
        RelationalJoinPlanningStrategy::CsgCmpMemo
    );
    assert_eq!(planning.status, RelationalJoinPlanningStatus::Selected);
    let [attempt] = planning.attempts.as_slice() else {
        panic!(
            "{table_count}-table successful CSG-CMP planning must not run a fallback strategy: {:?}",
            planning.attempts
        );
    };
    assert_eq!(attempt.strategy, RelationalJoinPlanningStrategy::CsgCmpMemo);
    assert_eq!(attempt.status, RelationalJoinPlanningStatus::Selected);
    assert!(attempt.cost.is_some());
}

fn join_sql(table_count: usize) -> String {
    assert!((MIN_TABLES..=MAX_TABLES).contains(&table_count));
    let mut sql = format!(
        "SELECT t{}.id AS id FROM planning_t0 AS t0",
        table_count - 1
    );
    for table in 1..table_count {
        sql.push_str(&format!(
            " INNER JOIN planning_t{table} AS t{table} \
             ON t{table}.parent_id = t{}.id",
            table - 1
        ));
    }
    sql.push_str(&format!(
        " WHERE t0.id = $1 ORDER BY t{}.id LIMIT 1",
        table_count - 1
    ));
    sql
}

fn percentiles(mut samples: Vec<u64>) -> serde_json::Value {
    samples.sort_unstable();
    json!({
        "p50": percentile(&samples, 50),
        "p95": percentile(&samples, 95),
        "p99": percentile(&samples, 99),
    })
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let index = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    samples[index]
}
