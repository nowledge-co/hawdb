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

use hawdb::{
    Database, DatabaseConfig, DatabaseReadTransaction, ProfiledRelationalSqlQueryOutput,
    QueryStreamOptions, RelationalOperatorKind,
};
use serde_json::json;
use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const DATASET_ROWS: usize = if cfg!(debug_assertions) { 128 } else { 1_024 };
const WARMUPS: usize = 3;
const SAMPLES: usize = 31;
const MAX_OUTPUT_ROWS: usize = DATASET_ROWS.saturating_mul(4).saturating_add(16);
const MAX_OUTPUT_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

const BATCHED_INDEX_SQL: &str = "SELECT o.id AS outer_id, i.id AS inner_id \
    FROM benchmark_batch_outer AS o \
    LEFT JOIN benchmark_batch_inner AS i \
    ON i.join_key = o.join_key AND o.id <> 'batch-00000000'";
const MERGE_SQL: &str = "SELECT l.id AS left_id, r.id AS right_id \
    FROM benchmark_merge_left AS l \
    INNER JOIN benchmark_merge_right AS r ON r.join_key = l.join_key \
    WHERE l.tenant = 'tenant-1'";
const HASH_SQL: &str = "SELECT l.id AS left_id, r.id AS right_id \
    FROM benchmark_hash_left AS l \
    INNER JOIN benchmark_hash_right AS r \
    ON r.join_key = l.join_key AND r.tag = l.tag AND r.value = 'keep' \
    WHERE l.tenant = 'tenant-1'";

#[derive(Clone, Copy)]
struct JoinCase {
    name: &'static str,
    sql: &'static str,
    expected_operator: RelationalOperatorKind,
    requires_spill: bool,
}

const REGULAR_CASES: [JoinCase; 3] = [
    JoinCase {
        name: "batched_index",
        sql: BATCHED_INDEX_SQL,
        expected_operator: RelationalOperatorKind::BatchedIndexNestedLoopLeftJoin,
        requires_spill: false,
    },
    JoinCase {
        name: "merge",
        sql: MERGE_SQL,
        expected_operator: RelationalOperatorKind::MergeJoin,
        requires_spill: false,
    },
    JoinCase {
        name: "hash",
        sql: HASH_SQL,
        expected_operator: RelationalOperatorKind::HashJoin,
        requires_spill: false,
    },
];

const GRACE_CASE: JoinCase = JoinCase {
    name: "grace_hash",
    sql: HASH_SQL,
    expected_operator: RelationalOperatorKind::HashJoin,
    requires_spill: true,
};

fn main() {
    let mut regular_database = Database::new();
    seed_regular_fixture(&mut regular_database);
    let regular_read = regular_database.begin_read_transaction();
    let mut results = REGULAR_CASES
        .into_iter()
        .map(|case| measure(&regular_read, case))
        .collect::<Vec<_>>();

    let spill_directory = benchmark_spill_directory();
    let _spill_directory = SpillDirectoryGuard(spill_directory.clone());
    let mut grace_database = Database::new_with_config(DatabaseConfig {
        execution_memory: grace_execution_memory(spill_directory),
        ..DatabaseConfig::default()
    });
    seed_grace_fixture(&mut grace_database);
    let grace_read = grace_database.begin_read_transaction();
    results.push(measure(&grace_read, GRACE_CASE));

    println!(
        "relational_join_execution {}",
        json!({
            "protocol": "hawdb-relational-join-execution-v1",
            "dataset_rows": DATASET_ROWS,
            "warmups": WARMUPS,
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn seed_regular_fixture(database: &mut Database) {
    for sql in [
        "CREATE TABLE benchmark_batch_outer (id TEXT PRIMARY KEY, join_key TEXT)",
        "CREATE TABLE benchmark_batch_inner (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
        "CREATE INDEX idx_benchmark_batch_inner_key ON benchmark_batch_inner (join_key)",
        "CREATE TABLE benchmark_merge_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT NOT NULL)",
        "CREATE TABLE benchmark_merge_right (id TEXT PRIMARY KEY, join_key TEXT NOT NULL, value TEXT NOT NULL)",
        "CREATE INDEX idx_benchmark_merge_left_tenant_key ON benchmark_merge_left (tenant, join_key)",
        "CREATE INDEX idx_benchmark_merge_right_key ON benchmark_merge_right (join_key)",
        "CREATE TABLE benchmark_hash_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT, tag TEXT NOT NULL)",
        "CREATE TABLE benchmark_hash_right (id TEXT PRIMARY KEY, join_key TEXT, tag TEXT NOT NULL, value TEXT NOT NULL)",
        "CREATE INDEX idx_benchmark_hash_left_tenant ON benchmark_hash_left (tenant)",
    ] {
        database
            .query_sql(sql)
            .unwrap_or_else(|error| panic!("seed benchmark schema: {error}"));
    }

    insert_batch_rows(database);
    insert_merge_rows(database);
    insert_hash_rows(database, DATASET_ROWS);
}

fn seed_grace_fixture(database: &mut Database) {
    for sql in [
        "CREATE TABLE benchmark_hash_left (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, join_key TEXT, tag TEXT NOT NULL)",
        "CREATE TABLE benchmark_hash_right (id TEXT PRIMARY KEY, join_key TEXT, tag TEXT NOT NULL, value TEXT NOT NULL)",
        "CREATE INDEX idx_benchmark_hash_left_tenant ON benchmark_hash_left (tenant)",
    ] {
        database
            .query_sql(sql)
            .unwrap_or_else(|error| panic!("seed Grace hash benchmark schema: {error}"));
    }
    insert_hash_rows(database, DATASET_ROWS);
}

fn insert_batch_rows(database: &mut Database) {
    let key_count = DATASET_ROWS.div_ceil(16).max(1);
    let outer_values = (0..DATASET_ROWS)
        .map(|ordinal| {
            format!(
                "('batch-{ordinal:08}', 'batch-key-{:04}')",
                ordinal % key_count
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_batch_outer (id, join_key) VALUES {outer_values}"
        ))
        .unwrap_or_else(|error| panic!("seed batched join outer rows: {error}"));

    let inner_values = (0..key_count)
        .map(|ordinal| {
            format!(
                "('batch-inner-{ordinal:08}', 'batch-key-{ordinal:04}', 'payload-{ordinal:04}')"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_batch_inner (id, join_key, value) VALUES {inner_values}"
        ))
        .unwrap_or_else(|error| panic!("seed batched join inner rows: {error}"));
}

fn insert_merge_rows(database: &mut Database) {
    let key_count = DATASET_ROWS.div_ceil(2).max(1);
    let left_values = (0..DATASET_ROWS)
        .map(|ordinal| {
            format!(
                "('merge-left-{ordinal:08}', 'tenant-1', 'merge-key-{:04}')",
                ordinal % key_count
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_merge_left (id, tenant, join_key) VALUES {left_values}"
        ))
        .unwrap_or_else(|error| panic!("seed merge join left rows: {error}"));

    let right_values = (0..key_count)
        .map(|ordinal| {
            format!(
                "('merge-right-{ordinal:08}', 'merge-key-{ordinal:04}', 'payload-{ordinal:04}')"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_merge_right (id, join_key, value) VALUES {right_values}"
        ))
        .unwrap_or_else(|error| panic!("seed merge join right rows: {error}"));
}

fn insert_hash_rows(database: &mut Database, rows: usize) {
    let left_values = (0..rows)
        .map(|ordinal| {
            format!(
                "('hash-left-{ordinal:08}', 'tenant-1', 'hash-key-{ordinal:08}', 'tag-{}')",
                ordinal % 2
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_hash_left (id, tenant, join_key, tag) VALUES {left_values}"
        ))
        .unwrap_or_else(|error| panic!("seed hash join left rows: {error}"));

    let right_values = (0..rows)
        .map(|ordinal| {
            format!(
                "('hash-right-{ordinal:08}', 'hash-key-{ordinal:08}', 'tag-{}', 'keep')",
                ordinal % 2
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    database
        .query_sql(&format!(
            "INSERT INTO benchmark_hash_right (id, join_key, tag, value) VALUES {right_values}"
        ))
        .unwrap_or_else(|error| panic!("seed hash join right rows: {error}"));
}

fn measure(read: &DatabaseReadTransaction, case: JoinCase) -> serde_json::Value {
    let cold = execute(read, case.sql);
    let cold_join = assert_profile_contract(&cold, case);
    let cold_parse_nanos = cold.profile.stage_timings.parse_nanos;

    for _ in 0..WARMUPS {
        let warmup = execute(read, case.sql);
        assert_profile_contract(&warmup, case);
        assert_eq!(warmup.profile.stage_timings.parse_nanos, 0);
    }

    let mut bind_samples = Vec::with_capacity(SAMPLES);
    let mut plan_samples = Vec::with_capacity(SAMPLES);
    let mut execute_samples = Vec::with_capacity(SAMPLES);
    let mut latest = None;
    for _ in 0..SAMPLES {
        let profiled = execute(read, case.sql);
        assert_profile_contract(&profiled, case);
        assert_eq!(profiled.profile.stage_timings.parse_nanos, 0);
        bind_samples.push(profiled.profile.stage_timings.bind_nanos);
        plan_samples.push(profiled.profile.stage_timings.plan_nanos);
        execute_samples.push(profiled.profile.stage_timings.execute_nanos);
        black_box(&profiled.output.rows);
        latest = Some(profiled);
    }
    let latest = latest.expect("benchmark records at least one sample");
    let join = assert_profile_contract(&latest, case);

    json!({
        "name": case.name,
        "sql_bytes": case.sql.len(),
        "cold_parse_ns": cold_parse_nanos,
        "bind_ns": percentiles(bind_samples),
        "plan_ns": percentiles(plan_samples),
        "execute_ns": percentiles(execute_samples),
        "output_rows": latest.output.rows.len(),
        "physical_join": {
            "operator_id": join.operator_id.get(),
            "operator": join.operator.as_str(),
            "table": join.table,
            "estimated_rows": join.estimated_rows,
            "actual_rows": join.actual_rows,
            "fully_consumed": join.fully_consumed,
        },
        "intermediate_rows": latest.profile.intermediate_rows,
        "index_reads": index_read_metrics(&latest),
        "row_read": {
            "runtime_path": latest.profile.row_read.runtime_path,
            "logical_pages": latest.profile.row_read.logical_pages,
            "physical_pages": latest.profile.row_read.physical_pages,
            "cache_hits": latest.profile.row_read.cache_hits,
            "cache_misses": latest.profile.row_read.cache_misses,
            "cache_admission_rejections": latest.profile.row_read.cache_admission_rejections,
        },
        "blocking_operators": latest.profile.blocking_operator_memory_reports.iter().map(|report| {
            json!({
                "operator": report.operator,
                "budget_bytes": report.budget_bytes,
                "peak_tracked_bytes": report.peak_tracked_bytes,
                "input_rows": report.input_rows,
                "spilled_rows": report.spilled_rows,
                "spilled_bytes": report.spilled_bytes,
                "spill_run_count": report.spill_run_count,
            })
        }).collect::<Vec<_>>(),
        "cold_physical_join": {
            "operator_id": cold_join.operator_id.get(),
            "operator": cold_join.operator.as_str(),
        },
    })
}

fn execute(read: &DatabaseReadTransaction, sql: &str) -> ProfiledRelationalSqlQueryOutput {
    read.query_sql_with_params_options_profiled(
        sql,
        &[],
        QueryStreamOptions {
            max_rows: Some(MAX_OUTPUT_ROWS),
            max_payload_bytes: Some(MAX_OUTPUT_PAYLOAD_BYTES),
        },
    )
    .unwrap_or_else(|error| panic!("execute relational join benchmark query: {error}"))
}

fn assert_profile_contract(
    profiled: &ProfiledRelationalSqlQueryOutput,
    case: JoinCase,
) -> &hawdb::RelationalOperatorCardinalityProfile {
    let join = profiled
        .profile
        .operator_cardinality_profiles
        .iter()
        .find(|profile| profile.operator == case.expected_operator)
        .unwrap_or_else(|| {
            panic!(
                "{} must use {}: {:?}",
                case.name,
                case.expected_operator.as_str(),
                profiled.profile.operator_cardinality_profiles
            )
        });
    assert!(
        join.actual_rows.is_some(),
        "{} join must report observed cardinality",
        case.name
    );
    assert!(
        join.fully_consumed,
        "{} join must fully consume its input",
        case.name
    );
    if case.requires_spill {
        assert!(
            profiled
                .profile
                .blocking_operator_memory_reports
                .iter()
                .any(|report| report.operator == "RelationalHashJoinGrace"
                    && report.spilled_rows > 0),
            "{} must emit Grace spill evidence",
            case.name
        );
    }
    join
}

fn index_read_metrics(profiled: &ProfiledRelationalSqlQueryOutput) -> serde_json::Value {
    let reads = &profiled.profile.index_reads;
    json!({
        "read_count": reads.len(),
        "logical_pages": reads.iter().map(|read| read.logical_pages).sum::<usize>(),
        "physical_pages": reads.iter().map(|read| read.physical_pages).sum::<usize>(),
        "cache_hits": reads.iter().map(|read| read.cache_hits).sum::<usize>(),
        "cache_misses": reads.iter().map(|read| read.cache_misses).sum::<usize>(),
        "cache_admission_rejections": reads.iter().map(|read| read.cache_admission_rejections).sum::<usize>(),
        "rows_visited": reads.iter().map(|read| read.rows_visited).sum::<usize>(),
    })
}

fn grace_execution_memory(spill_directory: PathBuf) -> hawdb_executor::ExecutionMemoryConfig {
    hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2 * 1024).expect("non-zero blocking budget"),
        max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024).expect("non-zero spill budget"),
        max_spill_runs: NonZeroUsize::new(64).expect("non-zero spill run budget"),
        max_total_spill_bytes: NonZeroU64::new(4 * 1024 * 1024)
            .expect("non-zero total spill budget"),
        max_total_spill_runs: NonZeroUsize::new(64).expect("non-zero total spill run budget"),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_directory,
        ..hawdb_executor::ExecutionMemoryConfig::default()
    }
}

fn benchmark_spill_directory() -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-relational-join-execution-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

struct SpillDirectoryGuard(PathBuf);

impl Drop for SpillDirectoryGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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
