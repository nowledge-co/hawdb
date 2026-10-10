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
use crate::index_runtime::{RelationalIndexReadMode, RelationalIndexStoreReader};
use crate::query::{
    execute_relational_query_sql_with_runtime, RelationalQueryLimits, RelationalQueryReadModes,
};
use hawdb_optimizer::RelationalAccessPathKind;
use hawdb_storage::relational::{
    RelationalIndexRangeScan, RelationalIndexReadLimits, RelationalIndexShadowError,
};
use hawdb_storage::relational_index_view::{
    RelationalIndexProbeStatistics, RelationalIndexReadViewReport,
};

struct QuerySnapshotStore {
    reader: RefCell<Option<RelationalRowPageSnapshotReader>>,
    opens: Cell<usize>,
}

impl RelationalRowStoreReader for QuerySnapshotStore {
    type TransactionRows = ();

    fn open_relational_row_snapshot_reader(
        &self,
    ) -> Result<Option<RelationalRowPageSnapshotReader>> {
        self.opens.set(self.opens.get() + 1);
        Ok(self.reader.borrow_mut().take())
    }

    fn open_relational_transaction_row_snapshot_reader(
        &self,
        _rows: &(),
    ) -> Result<RelationalRowPageSnapshotReader> {
        self.open_relational_row_snapshot_reader()?
            .ok_or_else(|| HawDBError::Storage("query reader was opened twice".into()))
    }
}

impl RelationalIndexStoreReader for QuerySnapshotStore {
    fn relational_index_probe_statistics(
        &self,
        _table: &str,
        _index: &str,
        _prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        None
    }

    fn visit_relational_index_read_view_prefix_entries(
        &self,
        _table: &str,
        _index: &str,
        _prefix: &RelationalKey,
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }

    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        _table: &str,
        _index: &str,
        _prefixes: &[RelationalKey],
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }

    fn visit_relational_index_read_view_range_entries(
        &self,
        _table: &str,
        _index: &str,
        _scan: &RelationalIndexRangeScan,
        _limits: RelationalIndexReadLimits,
        _visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        None
    }
}

fn indexed_fixture() -> Fixture {
    let mut state = state();
    for id in 0..8 {
        apply(
            &mut state,
            "UPDATE docs SET bucket = $1 WHERE id = $2",
            &[Value::Int(id % 2), Value::Int(id)],
        );
    }
    apply(
        &mut state,
        "CREATE INDEX bucket_index ON docs (bucket)",
        &[],
    );
    Fixture::with_state(state)
}

fn limits() -> RelationalQueryLimits {
    RelationalQueryLimits {
        max_output_rows: 128,
        max_output_payload_bytes: 64 * 1024,
        max_intermediate_rows: 1024,
        max_candidate_work: 1024,
        hydration: Default::default(),
        index_read: Default::default(),
        row_read: Default::default(),
    }
}

#[test]
fn actual_query_costs_use_the_same_snapshot_reader_as_execution() {
    let fixture = indexed_fixture();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let sql = "SELECT body, id FROM docs WHERE bucket = $1";
    let canonical = execute_relational_query_sql_with_runtime(
        sql,
        &[Value::Int(0)],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<QuerySnapshotStore>::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        limits(),
        &memory,
        None,
    )
    .unwrap();
    assert_eq!(canonical.access_path.kind, RelationalAccessPathKind::Index);
    assert_eq!(canonical.access_path.estimated_rows, 4);
    assert_eq!(canonical.rows.len(), 4);
    for prefix in ["", "EXPLAIN ", "EXPLAIN ANALYZE "] {
        let reader = fixture.snapshot();
        let identity = reader.identity();
        let store = QuerySnapshotStore {
            reader: RefCell::new(Some(reader)),
            opens: Cell::new(0),
        };
        let output = execute_relational_query_sql_with_runtime(
            &format!("{prefix}{sql}"),
            &[Value::Int(0)],
            &fixture.state,
            RelationalQueryReadModes::new(
                RelationalIndexReadMode::Materialized,
                RelationalRowReadMode::Store(&store),
            ),
            limits(),
            &memory,
            None,
        )
        .unwrap();
        // Eight rows on one page: scan scalar 15+11+8=34, while
        // four noncovering point fetches cost 20+2*17+4+4=62.
        // Without snapshot setup, that index scalar is only 26 vs scan28.
        assert_eq!(output.access_path.kind, RelationalAccessPathKind::FullScan);
        assert_eq!(store.opens.get(), 1);
        assert!(store.reader.borrow().is_none());
        if prefix == "EXPLAIN " {
            assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");
            assert_eq!(output.row_execution_evidence.descriptor_reads, 0);
        } else {
            assert_eq!(output.row_execution_evidence.runtime_path, "snapshot_rows");
            assert_eq!(
                output.row_execution_evidence.base_generation,
                Some(identity.base_generation)
            );
            assert_eq!(
                output.row_execution_evidence.visible_commit_epoch,
                Some(identity.visible_commit_epoch)
            );
            if prefix.is_empty() {
                assert_eq!(output.rows, canonical.rows);
            }
        }
    }
    fixture.remove();
}

#[test]
fn aliased_join_contexts_reach_memo_profiles_and_decline_projection_rows() {
    let fixture = Fixture::new();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let sql = "SELECT a.body AS a_body, b.id AS b_id FROM docs a JOIN docs b ON b.id = a.id";
    let canonical = execute_relational_query_sql_with_runtime(
        sql,
        &[],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<QuerySnapshotStore>::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        limits(),
        &memory,
        None,
    )
    .unwrap();
    assert_eq!(canonical.rows.len(), 8);
    assert_eq!(canonical.join_planning.cost.unwrap().cost, 60);
    for mode in 0..3 {
        let store = QuerySnapshotStore {
            reader: RefCell::new(Some(fixture.snapshot())),
            opens: Cell::new(0),
        };
        let rows = ();
        let row_mode = match mode {
            0 => RelationalRowReadMode::Store(&store),
            1 => RelationalRowReadMode::Transaction {
                store: &store,
                rows: &rows,
            },
            _ => RelationalRowReadMode::ProjectionGeneration {
                store: &store,
                reader: &fixture.projection,
                tables: &fixture.tables,
            },
        };
        let output = execute_relational_query_sql_with_runtime(
            sql,
            &[],
            &fixture.state,
            RelationalQueryReadModes::new(RelationalIndexReadMode::Materialized, row_mode),
            limits(),
            &memory,
            None,
        )
        .unwrap();
        // Snapshot probe CPU47/random32/sequential11/output16 costs138.
        // Hash also re-fetches both snapshot locators for every match: its
        // CPU126/random64/sequential22/output16 costs292, so the same complete
        // query uses the probe and stays inside its unchanged 16-page budget.
        // Projection rows force scans and decline checkpoint context:
        // hash CPU is (8+4)*2+8+8*2+8=56, scalar88.
        let expected = if mode == 2 {
            hawdb_optimizer::PlanCostBreakdown::new(8, 56, 0, 16, 16)
        } else {
            hawdb_optimizer::PlanCostBreakdown::new(8, 47, 32, 11, 16)
        };
        assert_eq!(output.join_planning.cost.unwrap(), expected.into());
        assert_eq!(output.rows, canonical.rows);
        assert_eq!(store.opens.get(), 1);
        assert_eq!(output.operator_cardinality_profiles.len(), 2);
        assert_eq!(output.operator_cardinality_profiles[1].estimated_rows, 8);
        assert_eq!(output.operator_cardinality_profiles[1].actual_rows, Some(8));
    }
    fixture.remove();
}

#[test]
fn canceled_planning_does_not_open_and_failed_admission_drops_its_reader() {
    let fixture = indexed_fixture();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = RuntimeTaskContext::without_deadline(cancellation);
    let root_refs = Arc::strong_count(&fixture.root);
    let store = QuerySnapshotStore {
        reader: RefCell::new(Some(fixture.snapshot())),
        opens: Cell::new(0),
    };
    let error = execute_relational_query_sql_with_runtime(
        "SELECT body FROM docs",
        &[],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::Store(&store),
        ),
        limits(),
        &memory,
        Some(&task),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(store.opens.get(), 0);
    assert!(store.reader.borrow().is_some());
    drop(store);
    assert_eq!(Arc::strong_count(&fixture.root), root_refs);
    let store = QuerySnapshotStore {
        reader: RefCell::new(Some(fixture.snapshot())),
        opens: Cell::new(0),
    };
    let memory = hawdb_executor::ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::MIN,
        ..memory
    };
    let error = execute_relational_query_sql_with_runtime(
        "SELECT body FROM docs ORDER BY body",
        &[],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::Store(&store),
        ),
        limits(),
        &memory,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"));
    assert_eq!(store.opens.get(), 1);
    assert!(store.reader.borrow().is_none());
    assert_eq!(Arc::strong_count(&fixture.root), root_refs);
    fixture.remove();
}

#[test]
fn ordinary_explain_does_not_execute_a_canceled_task_across_row_modes() {
    let fixture = indexed_fixture();
    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = RuntimeTaskContext::without_deadline(cancellation);
    for mode in 0..5 {
        for prefix in ["EXPLAIN ", "", "EXPLAIN ANALYZE "] {
            let store = QuerySnapshotStore {
                reader: RefCell::new((mode != 1).then(|| fixture.snapshot())),
                opens: Cell::new(0),
            };
            let rows = ();
            let row_mode = match mode {
                0 => RelationalRowReadMode::CanonicalMemory,
                1 | 2 => RelationalRowReadMode::Store(&store),
                3 => RelationalRowReadMode::Transaction {
                    store: &store,
                    rows: &rows,
                },
                _ => RelationalRowReadMode::ProjectionGeneration {
                    store: &store,
                    reader: &fixture.projection,
                    tables: &fixture.tables,
                },
            };
            let output = execute_relational_query_sql_with_runtime(
                &format!("{prefix}SELECT body, id FROM docs WHERE bucket = $1"),
                &[Value::Int(0)],
                &fixture.state,
                RelationalQueryReadModes::new(RelationalIndexReadMode::Materialized, row_mode),
                limits(),
                &memory,
                Some(&task),
            );
            if prefix == "EXPLAIN " {
                let output = output.expect("ordinary EXPLAIN does not execute the canceled task");
                assert_eq!(store.opens.get(), usize::from(mode != 0));
                assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");
                assert_eq!(output.row_execution_evidence.descriptor_reads, 0);
                assert_eq!(output.row_execution_evidence.rows_visited, 0);
                assert!(output.index_execution_evidence.is_empty());
                assert!(output.rows.iter().all(|row| !row.contains_key("actRows")));
                assert_eq!(
                    output.access_path.kind,
                    if mode < 2 {
                        RelationalAccessPathKind::Index
                    } else {
                        RelationalAccessPathKind::FullScan
                    },
                );
            } else {
                assert!(output.unwrap_err().to_string().contains("cancelled"));
                assert_eq!(store.opens.get(), 0);
                assert_eq!(store.reader.borrow().is_some(), mode != 1);
            }
        }
    }
    fixture.remove();
}

#[test]
fn ordinary_explain_still_rejects_known_snapshot_poison() {
    let fixture = indexed_fixture();
    let reader = fixture.snapshot();
    let path = fixture
        .directory
        .join(hawdb_storage::relational::relational_row_page_artifact_file(1));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let mut hydration = hawdb_storage::relational::RelationalHydrationBudget::default();
    assert!(matches!(
        reader.point_projected(
            "docs",
            &key(0),
            &[2],
            Default::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(hawdb_storage::relational::RelationalRowPageSnapshotReadError::Corrupt(_))
    ));
    assert!(reader.is_poisoned());
    let store = QuerySnapshotStore {
        reader: RefCell::new(Some(reader)),
        opens: Cell::new(0),
    };
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = RuntimeTaskContext::without_deadline(cancellation);
    let error = execute_relational_query_sql_with_runtime(
        "EXPLAIN SELECT body FROM docs",
        &[],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::Materialized,
            RelationalRowReadMode::Store(&store),
        ),
        limits(),
        &hawdb_executor::ExecutionMemoryConfig::default(),
        Some(&task),
    )
    .unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(message) if message.contains("poisoned")));
    assert_eq!(store.opens.get(), 1);
    assert!(store.reader.borrow().is_none());
    fixture.remove();
}

#[test]
fn distinct_snapshot_layouts_stay_with_their_original_bindings_after_reorder() {
    let mut state = state();
    apply(
        &mut state,
        "CREATE TABLE others (id BIGINT PRIMARY KEY, bucket BIGINT, body TEXT)",
        &[],
    );
    for id in [0, 2] {
        apply(
            &mut state,
            "INSERT INTO others (id, bucket, body) VALUES ($1, 0, 'other')",
            &[Value::Int(id)],
        );
    }
    let fixture = Fixture::with_table_layouts(state, &[("docs", 8), ("others", 1)]);
    let task = RuntimeTaskContext::default();
    let reader = fixture.snapshot();
    let docs = reader
        .checkpoint_table_root("docs", &task)
        .unwrap()
        .unwrap();
    let others = reader
        .checkpoint_table_root("others", &task)
        .unwrap()
        .unwrap();
    assert_eq!((docs.row_count, docs.page_count), (8, 1));
    assert_eq!((others.row_count, others.page_count), (2, 2));
    drop(reader);

    let memory = hawdb_executor::ExecutionMemoryConfig::default();
    let sql = "SELECT a.body AS a_body, b.id AS b_id FROM docs a JOIN others b ON b.id = a.id";
    let canonical = execute_relational_query_sql_with_runtime(
        sql,
        &[],
        &fixture.state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<QuerySnapshotStore>::Materialized,
            RelationalRowReadMode::CanonicalMemory,
        ),
        limits(),
        &memory,
        None,
    )
    .unwrap();
    assert_eq!(canonical.join_planning.selected_order, ["b", "a"]);
    assert_eq!(
        canonical.join_planning.cost.unwrap(),
        hawdb_optimizer::PlanCostBreakdown::new(2, 8, 2, 2, 4).into()
    );
    assert_eq!(canonical.rows.len(), 2);
    assert_eq!(canonical.rows[0]["b_id"], Value::Int(0));
    assert_eq!(
        canonical.rows[0]["a_body"],
        Value::String("body-0-é".into())
    );
    assert_eq!(canonical.rows[1]["b_id"], Value::Int(2));
    assert_eq!(canonical.rows[1]["a_body"], Value::Null);

    for transaction in [false, true] {
        for prefix in ["", "EXPLAIN ", "EXPLAIN ANALYZE "] {
            let store = QuerySnapshotStore {
                reader: RefCell::new(Some(fixture.snapshot())),
                opens: Cell::new(0),
            };
            let rows = ();
            let mode = if transaction {
                RelationalRowReadMode::Transaction {
                    store: &store,
                    rows: &rows,
                }
            } else {
                RelationalRowReadMode::Store(&store)
            };
            let output = execute_relational_query_sql_with_runtime(
                &format!("{prefix}{sql}"),
                &[],
                &fixture.state,
                RelationalQueryReadModes::new(RelationalIndexReadMode::Materialized, mode),
                limits(),
                &memory,
                None,
            )
            .unwrap();
            assert!(output.join_planning.join_order_reordered());
            assert_eq!(output.join_planning.selected_order, ["b", "a"]);
            // Original binding b owns the two-page scan: CPU 2+4+3*2=12.
            // Original binding a owns both one-page points: CPU8/random8.
            // b's ordered metadata adds sequential6. Reassigning contexts by
            // selected order would report CPU19/random14/sequential5/scalar56.
            assert_eq!(
                output.join_planning.cost.unwrap(),
                hawdb_optimizer::PlanCostBreakdown::new(2, 20, 8, 8, 4).into()
            );
            assert_eq!(output.access_path.kind, RelationalAccessPathKind::FullScan);
            assert_eq!(output.join_access_paths.len(), 1);
            assert_eq!(
                output.join_access_paths[0].kind,
                RelationalAccessPathKind::PrimaryKey
            );
            assert_eq!(store.opens.get(), 1);
            assert!(store.reader.borrow().is_none());
            let profiles = &output.operator_cardinality_profiles;
            assert_eq!(profiles.len(), 2);
            assert_eq!(profiles[0].table, "others");
            assert_eq!(profiles[1].table, "docs");
            assert!(profiles.iter().all(|profile| profile.estimated_rows == 2));
            if prefix == "EXPLAIN " {
                assert!(profiles.iter().all(|profile| profile.actual_rows.is_none()));
                assert_eq!(output.row_execution_evidence.runtime_path, "not_executed");
                assert_eq!(output.row_execution_evidence.descriptor_reads, 0);
                assert_eq!(output.row_execution_evidence.rows_visited, 0);
            } else {
                assert!(profiles
                    .iter()
                    .all(|profile| profile.actual_rows == Some(2)));
                assert_eq!(output.row_execution_evidence.runtime_path, "snapshot_rows");
                if prefix.is_empty() {
                    assert_eq!(output.rows, canonical.rows);
                }
            }
        }
    }
    fixture.remove();
}
