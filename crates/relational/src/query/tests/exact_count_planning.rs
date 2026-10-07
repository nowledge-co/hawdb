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
use hawdb_storage::config::{DurabilityPolicy, RelationalIndexMode, WalReplayConfig};
use hawdb_storage::relational::{RelationalIndexReadLimits, RelationalKey, RelationalValue};
use hawdb_storage::relational_index_view::RelationalIndexReadViewBackendReport;
use hawdb_storage::schema::Catalog;
use hawdb_storage::store::GraphStore;

const SQL: &str = "SELECT id FROM docs WHERE bucket = $1 ORDER BY id";

struct Fixture {
    directory: std::path::PathBuf,
    store: Option<GraphStore>,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-exact-count-planning-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        {
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &directory,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..Default::default()
                },
            )
            .unwrap();
            for sql in [
                "CREATE TABLE docs (id BIGINT PRIMARY KEY, bucket BIGINT)",
                "CREATE INDEX docs_bucket ON docs (bucket)",
            ] {
                let transaction =
                    compile_relational_statement_sql(sql, &[], store.relational_state()).unwrap();
                store
                    .commit_relational_transaction(&mut catalog, transaction)
                    .unwrap();
            }
            for id in 0..65 {
                let bucket = if id < 31 { 0 } else { id - 30 };
                let transaction = compile_relational_statement_sql(
                    "INSERT INTO docs (id, bucket) VALUES ($1, $2)",
                    &[Value::Int(id), Value::Int(bucket)],
                    store.relational_state(),
                )
                .unwrap();
                store
                    .commit_relational_transaction(&mut catalog, transaction)
                    .unwrap();
            }
            store.checkpoint(&catalog).unwrap();
        }
        let store = GraphStore::open_with_durability_and_replay_config(
            &directory,
            &mut Catalog::default(),
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!store
            .relational_state()
            .materialized_index_postings_resident());
        Self {
            directory,
            store: Some(store),
        }
    }

    fn store(&self) -> &GraphStore {
        self.store.as_ref().unwrap()
    }

    fn query(
        &self,
        bucket: i64,
        index_read: RelationalIndexReadLimits,
    ) -> Result<RelationalQueryOutput> {
        execute_relational_query_sql_with_runtime(
            SQL,
            &[Value::Int(bucket)],
            self.store().relational_state(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Authoritative(self.store()),
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryLimits {
                max_output_rows: 128,
                index_read,
                ..batched_index_join_limits()
            },
            &hawdb_executor::ExecutionMemoryConfig::default(),
            None,
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.store.take());
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn persisted_full_key_planning_uses_value_specific_count_and_complete_rows() {
    let fixture = Fixture::new();
    let statistics = fixture
        .store()
        .relational_index_probe_statistics("docs", "docs_bucket", 1)
        .unwrap();
    assert_eq!(statistics.average_fanout(), 2);
    for bucket in [0, 1, 999] {
        let output = fixture.query(bucket, Default::default()).unwrap();
        let expected = (0..65)
            .filter(|id| (if *id < 31 { 0 } else { *id - 30 }) == bucket)
            .map(Value::Int)
            .collect::<Vec<_>>();
        assert_eq!(
            output
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(output.access_path.kind, RelationalAccessPathKind::Index);
        assert_eq!(output.access_path.estimated_rows, expected.len().max(1));
    }
}

#[test]
fn planning_metadata_and_execution_share_one_statement_page_allowance() {
    let fixture = Fixture::new();
    let (count, report) = fixture
        .store()
        .relational_index_exact_posting_count(
            "docs",
            "docs_bucket",
            &RelationalKey(vec![RelationalValue::BigInt(0)]),
            Default::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(count, 31);
    assert_eq!(report.rows_visited, 0);
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        panic!("checkpoint base")
    };
    assert!(report.pages_read > 0);
    let limits = RelationalIndexReadLimits {
        max_pages: NonZeroUsize::new(report.pages_read).unwrap(),
        ..Default::default()
    };
    let output = fixture.query(0, limits);
    assert!(
        matches!(output, Err(HawDBError::Execution(_))),
        "metadata consumes the statement allowance before execution: {output:?}"
    );
}

#[test]
fn metadata_evidence_and_estimates_follow_explain_and_execution() {
    let fixture = Fixture::new();
    let (_, metadata) = fixture
        .store()
        .relational_index_exact_posting_count(
            "docs",
            "docs_bucket",
            &RelationalKey(vec![RelationalValue::BigInt(0)]),
            Default::default(),
        )
        .unwrap()
        .unwrap();
    let metadata_identity = (
        metadata.visible_commit_epoch,
        metadata.root_set_digest.clone(),
    );
    let RelationalIndexReadViewBackendReport::Base(metadata) = metadata.backend else {
        panic!("base")
    };
    for prefix in ["", "EXPLAIN ", "EXPLAIN ANALYZE "] {
        let output = execute_relational_query_sql_with_runtime(
            &format!("{prefix}{SQL}"),
            &[Value::Int(0)],
            fixture.store().relational_state(),
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Authoritative(fixture.store()),
                RelationalRowReadMode::CanonicalMemory,
            ),
            RelationalQueryLimits {
                max_output_rows: 128,
                ..batched_index_join_limits()
            },
            &hawdb_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .unwrap();
        assert_eq!(output.access_path.estimated_rows, 31);
        let profiles = &output.operator_cardinality_profiles;
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].estimated_rows, 31);
        let evidence = &output.index_execution_evidence;
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].visible_commit_epoch, Some(metadata_identity.0));
        assert_eq!(
            evidence[0].root_set_digest.as_deref(),
            Some(metadata_identity.1.as_str())
        );
        let reads = if prefix == "EXPLAIN " { 1 } else { 2 };
        assert_eq!(evidence[0].lookups, reads);
        assert_eq!(evidence[0].logical_pages, reads * metadata.pages_read);
        assert_eq!(evidence[0].rows_visited, if reads == 1 { 0 } else { 31 });
        assert_eq!(profiles[0].actual_rows, (reads == 2).then_some(31));
    }
}

fn query_metadata_phase(
    fixture: &Fixture,
    prefix: &str,
    mode: RelationalIndexReadMode<'_, GraphStore>,
) -> RelationalQueryOutput {
    execute_relational_query_sql_with_runtime(
        &format!("{prefix}{SQL}"),
        &[Value::Int(0)],
        fixture.store().relational_state(),
        RelationalQueryReadModes::new(mode, RelationalRowReadMode::CanonicalMemory),
        RelationalQueryLimits {
            max_output_rows: 128,
            ..batched_index_join_limits()
        },
        &hawdb_executor::ExecutionMemoryConfig::default(),
        None,
    )
    .unwrap()
}

#[test]
fn metadata_phase_explain_does_not_claim_an_executed_index_backend() {
    let fixture = Fixture::new();
    for mode in [
        RelationalIndexReadMode::DemandPaged(fixture.store()),
        RelationalIndexReadMode::Authoritative(fixture.store()),
    ] {
        let output = query_metadata_phase(&fixture, "EXPLAIN ", mode);
        let evidence = &output.index_execution_evidence[0];
        assert_eq!(evidence.lookups, 1);
        assert!(evidence.logical_pages > 0);
        assert_eq!(evidence.rows_visited, 0);
        assert_eq!(evidence.demand_paged_lookups, 0);
        assert_eq!(evidence.authoritative_lookups, 0);
        assert_eq!(evidence.transaction_workspace_lookups, 0);
        assert_eq!(evidence.canonical_fallback_lookups, 0);
        assert_eq!(evidence.runtime_path(), "not_executed");
        assert!(output
            .operator_cardinality_profiles
            .iter()
            .all(|profile| profile.actual_rows.is_none()));
        assert!(output.rows.iter().any(|row| matches!(
            row.get("operator info"),
            Some(Value::String(info)) if info.contains("metadata_counts=1")
        )));
    }
}

#[test]
fn metadata_phase_execution_backend_counts_only_the_row_probe() {
    let fixture = Fixture::new();
    for (mode, runtime_path) in [
        (
            RelationalIndexReadMode::DemandPaged(fixture.store()),
            "demand_paged",
        ),
        (
            RelationalIndexReadMode::Authoritative(fixture.store()),
            "authoritative",
        ),
    ] {
        for prefix in ["", "EXPLAIN ANALYZE "] {
            let output = query_metadata_phase(&fixture, prefix, mode);
            let evidence = &output.index_execution_evidence[0];
            assert_eq!(evidence.lookups, 2);
            assert_eq!(evidence.rows_visited, 31);
            assert_eq!(
                evidence.demand_paged_lookups,
                usize::from(runtime_path == "demand_paged")
            );
            assert_eq!(
                evidence.authoritative_lookups,
                usize::from(runtime_path == "authoritative")
            );
            assert_eq!(evidence.transaction_workspace_lookups, 0);
            assert_eq!(evidence.canonical_fallback_lookups, 0);
            assert_eq!(evidence.runtime_path(), runtime_path);
            assert_eq!(
                output.operator_cardinality_profiles[0].actual_rows,
                Some(31)
            );
            if !prefix.is_empty() {
                assert!(output.rows.iter().any(|row| matches!(
                    row.get("operator info"),
                    Some(Value::String(info)) if info.contains("metadata_counts=1")
                )));
            }
        }
    }
}
