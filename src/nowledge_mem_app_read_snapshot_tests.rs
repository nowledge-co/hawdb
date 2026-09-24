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

use super::{
    NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
    NowledgeMemGraphMode, NowledgeMemReadSnapshotBudget, NowledgeMemSearchProjection,
};
use crate::search::{SearchProjectionKind, SearchProjectionRow};
use crate::{Database, DatabaseConfig, SearchIndex, Value};
use std::collections::BTreeMap;

fn snapshot_governor() -> hawdb_qos::RuntimeGovernor {
    use hawdb_qos::{RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot};
    hawdb_qos::RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(8 << 30), None, None, None),
        ),
        hawdb_qos::IoConcurrencyBudget::new(8, 2),
    )
}

fn app_read_handle() -> NowledgeMemEmbeddedStoreHandle {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(16 * 1024),
        ..DatabaseConfig::default()
    });
    database
        .query("CREATE (:Memory {id: 'nearest', space_id: 'default', title: 'Nearest memory'})")
        .unwrap();
    database
        .query("CREATE (:Thread {id: 'thread-1', thread_id: 'logical-1', title: 'App thread'})")
        .unwrap();
    database
        .query_sql(
            "CREATE TABLE thread_messages (\
             content_message_id TEXT PRIMARY KEY, thread_storage_id TEXT NOT NULL, \
             order_index BIGINT NOT NULL, content TEXT NOT NULL)",
        )
        .unwrap();
    database
        .query_sql_with_params(
            "INSERT INTO thread_messages \
             (content_message_id, thread_storage_id, order_index, content) \
             VALUES ($1, $2, $3, $4)",
            &[
                Value::String("message-1".to_string()),
                Value::String("thread-1".to_string()),
                Value::Int(0),
                Value::String("App-owned relational payload".to_string()),
            ],
        )
        .unwrap();

    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        database,
        NowledgeMemGraphMode::WritableCutover,
        snapshot_governor(),
    );
    let mut index = SearchIndex::in_memory();
    for (external_id, embedding, space_id) in [
        ("nearest", vec![1.0, 0.0], "default"),
        ("farther", vec![0.0, 1.0], "other"),
    ] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.to_string(),
                title: external_id.to_string(),
                body: "App search candidate".to_string(),
                embedding: Some(embedding),
                source_id: None,
                metadata: BTreeMap::from([("space_id".to_string(), space_id.to_string())]),
            })
            .unwrap();
    }
    let projection = NowledgeMemSearchProjection::from_index(index);
    NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, Some(projection)))
}

#[test]
fn bounded_read_snapshot_coordinates_search_graph_and_relational_queries() {
    let handle = app_read_handle();
    let output = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let search = snapshot.query_cypher(
                    "CALL vector_search($embedding, topK := 2) YIELD id, score \
                     MATCH (m:Memory) WHERE m.space_id = $space_id \
                     RETURN m.id AS memory_id, score LIMIT 1",
                    &BTreeMap::from([
                        (
                            "embedding".to_string(),
                            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
                        ),
                        ("space_id".to_string(), Value::String("default".to_string())),
                    ]),
                    1,
                )?;
                let thread = snapshot.query_cypher(
                    "MATCH (t:Thread) WHERE t.thread_id = $thread_id OR t.id = $thread_id \
                     RETURN t.id AS id, t.title AS title LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("logical-1".to_string()),
                    )]),
                    1,
                )?;
                let messages = snapshot.query_sql(
                    "SELECT content_message_id, content FROM thread_messages \
                     WHERE thread_storage_id = $1 ORDER BY order_index LIMIT $2",
                    &[Value::String("thread-1".to_string()), Value::Int(1)],
                    1,
                )?;
                Ok((search, thread, messages, snapshot.report()))
            },
        )
        .unwrap();

    assert_eq!(
        output.0.rows[0].get("memory_id"),
        Some(&Value::String("nearest".to_string()))
    );
    assert_eq!(
        output.1.rows[0].get("id"),
        Some(&Value::String("thread-1".to_string()))
    );
    assert_eq!(
        output.2.rows[0].get("content_message_id"),
        Some(&Value::String("message-1".to_string()))
    );
    assert_eq!(output.3.max_rows, 4);
    assert_eq!(output.3.max_payload_bytes, 4096);
    assert_eq!(output.3.output_rows, 3);
    assert_eq!(output.3.cypher_statement_count, 2);
    assert_eq!(output.3.sql_statement_count, 1);
    assert_eq!(output.3.vector_seed_execution_count, 1);
    assert_eq!(output.3.remaining_rows, 1);
    assert!(output.3.search_projection_present);
    assert!(output.3.output_payload_bytes > 0);
    assert!(output.3.remaining_payload_bytes < 4096);
    let evidence = output.3.json();
    let encoded_evidence = evidence.to_string();
    assert_eq!(
        evidence["protocol"],
        "hawdb-nowledge-mem-read-snapshot-report-v1"
    );
    assert_eq!(evidence["cypher_statement_count"], 2);
    assert_eq!(evidence["sql_statement_count"], 1);
    assert_eq!(evidence["vector_seed_execution_count"], 1);
    assert!(!encoded_evidence.contains("nearest"));
    assert!(!encoded_evidence.contains("thread-1"));
    assert!(!encoded_evidence.contains("App-owned relational payload"));
}

#[test]
fn bounded_read_snapshot_reports_the_thread_detail_mixed_read_shape() {
    let handle = app_read_handle();
    let report = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let thread = snapshot.query_cypher(
                    "MATCH (t:Thread) WHERE t.thread_id = $thread_id OR t.id = $thread_id \
                     RETURN t.id AS id, t.title AS title LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("logical-1".to_string()),
                    )]),
                    1,
                )?;
                let storage_id = thread.rows[0]
                    .get("id")
                    .cloned()
                    .expect("thread id is projected");
                snapshot.query_sql(
                    "SELECT COUNT(*) AS total_messages FROM thread_messages \
                     WHERE thread_storage_id = $1",
                    std::slice::from_ref(&storage_id),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id, content FROM thread_messages \
                     WHERE thread_storage_id = $1 ORDER BY order_index LIMIT $2 OFFSET $3",
                    &[storage_id, Value::Int(1), Value::Int(0)],
                    1,
                )?;
                Ok(snapshot.report())
            },
        )
        .unwrap();

    assert_eq!(report.cypher_statement_count, 1);
    assert_eq!(report.sql_statement_count, 2);
    assert_eq!(report.vector_seed_execution_count, 0);
    assert_eq!(report.max_rows, 4);
    assert_eq!(report.max_payload_bytes, 4096);
    assert_eq!(report.output_rows, 3);
    assert_eq!(report.remaining_rows, 1);
    assert!(report.output_payload_bytes > 0);
    assert!(report.remaining_payload_bytes < 4096);
}

#[test]
fn bounded_read_snapshot_counts_only_successfully_budgeted_statements() {
    let handle = app_read_handle();
    let report = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot
                    .query_sql("SELECT missing_column FROM thread_messages", &[], 1)
                    .expect_err("invalid SQL must fail before evidence accounting");
                let after_failure = snapshot.report();
                assert_eq!(after_failure.sql_statement_count, 0);
                assert_eq!(after_failure.vector_seed_execution_count, 0);
                assert_eq!(after_failure.output_rows, 0);
                assert_eq!(after_failure.remaining_rows, 2);
                assert_eq!(after_failure.remaining_payload_bytes, 4096);

                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                Ok(snapshot.report())
            },
        )
        .unwrap();

    assert_eq!(report.cypher_statement_count, 1);
    assert_eq!(report.sql_statement_count, 0);
    assert_eq!(report.vector_seed_execution_count, 0);
    assert_eq!(report.output_rows, 1);
    assert_eq!(report.remaining_rows, 1);
}

#[test]
fn bounded_read_snapshot_enforces_the_cumulative_row_budget() {
    let handle = app_read_handle();
    let error = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id FROM thread_messages \
                     WHERE thread_storage_id = $1 LIMIT 1",
                    &[Value::String("thread-1".to_string())],
                    1,
                )?;
                Ok(())
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("exhausted max_rows"));
}

#[test]
fn bounded_read_snapshot_enforces_the_cumulative_payload_budget() {
    let handle = app_read_handle();
    let error = handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 12,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id LIMIT 1",
                    &BTreeMap::from([(
                        "thread_id".to_string(),
                        Value::String("thread-1".to_string()),
                    )]),
                    1,
                )?;
                snapshot.query_sql(
                    "SELECT content_message_id FROM thread_messages \
                     WHERE thread_storage_id = $1 LIMIT 1",
                    &[Value::String("thread-1".to_string())],
                    1,
                )?;
                Ok(())
            },
        )
        .unwrap_err();

    assert!(
        error.to_string().contains("max_output_payload_bytes 2"),
        "unexpected error: {error}"
    );
}

#[test]
fn user_keeps_a_stable_graph_and_sql_snapshot_while_a_writer_commits() {
    // Given a bounded graph/SQL snapshot and a real embedded writer.
    let handle = app_read_handle();
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let Ok(()) = start_rx.recv() else { return };
        let result = writer_handle.with_transaction(|transaction| {
            transaction.query("MATCH (t:Thread) SET t.title = 'Updated thread'")?;
            transaction.query_sql("UPDATE thread_messages SET content = 'Updated payload' WHERE content_message_id = 'message-1'")?;
            Ok(())
        });
        done_tx.send(result).unwrap();
    });
    let read = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| -> crate::Result<_> {
        let graph = snapshot.query_cypher(
            "MATCH (t:Thread) RETURN t.title AS title LIMIT 1",
            &BTreeMap::new(),
            1,
        )?;
        let sql = snapshot.query_sql("SELECT content FROM thread_messages LIMIT 1", &[], 1)?;
        Ok((graph, sql))
    };
    let result = handle.with_bounded_graph_read_snapshot(
        NowledgeMemReadSnapshotBudget {
            max_rows: 4,
            max_payload_bytes: 4096,
        },
        |snapshot| {
            let before = read(snapshot)?;
            let epoch = snapshot.commit_epoch();
            // When the writer commits before this callback returns.
            start_tx.send(()).unwrap();
            let committed = done_rx.recv_timeout(std::time::Duration::from_secs(5));
            // Return before joining on failure, so regression cannot deadlock cleanup.
            let committed = committed.map_err(|_| {
                crate::HawDBError::Execution("writer blocked behind an immutable snapshot".into())
            })?;
            committed?;
            // Then both planes retain the old values and epoch.
            assert_eq!(read(snapshot)?, before);
            assert_eq!(snapshot.commit_epoch(), epoch);
            assert!(!snapshot.report().search_projection_present);
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_source_graph_commit_epoch,
                None
            );
            Ok((before, epoch))
        },
    );
    drop(start_tx);
    writer.join().unwrap();
    let (before, epoch) = result.unwrap();
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let after = read(snapshot)?;
                assert_ne!(after.0, before.0);
                assert_ne!(after.1, before.1);
                assert!(snapshot.commit_epoch() > epoch);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn user_cannot_read_an_unpinned_projection_from_a_graph_snapshot() {
    let handle = app_read_handle();
    let query = "CALL vector_search($embedding, topK := 2) YIELD id, score \
                 MATCH (m:Memory) WHERE m.space_id = $space_id \
                 RETURN m.id AS memory_id, score LIMIT 1";
    let parameters = BTreeMap::from([
        (
            "embedding".into(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        ),
        ("space_id".into(), Value::String("default".into())),
    ]);
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    // Given a valid vector query against the configured projection.
    handle
        .with_bounded_read_snapshot(budget, |snapshot| {
            assert_eq!(snapshot.query_cypher(query, &parameters, 1)?.rows.len(), 1);
            Ok(())
        })
        .unwrap();
    // When the same query uses a graph-only snapshot, the projection is unavailable.
    handle
        .with_bounded_graph_read_snapshot(budget, |snapshot| {
            let error = snapshot.query_cypher(query, &parameters, 1).unwrap_err();
            assert!(matches!(error, crate::HawDBError::Storage(ref message)
            if message == "nowledge mem search projection is not configured"));
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_source_graph_commit_epoch,
                None
            );
            assert_eq!(
                snapshot
                    .report()
                    .search_projection_durable_source_graph_commit_epoch,
                None
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn user_retains_cumulative_budgets_without_a_projection_lock() {
    let handle = app_read_handle();
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot.query_cypher(
                    "MATCH (m:Memory) RETURN m.id AS value LIMIT 1",
                    &BTreeMap::new(),
                    1,
                )?;
                assert!(snapshot
                    .query_sql("SELECT content FROM thread_messages LIMIT 1", &[], 1)
                    .is_err());
                assert_eq!(snapshot.report().output_rows, 1);
                assert_eq!(snapshot.report().remaining_rows, 0);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn user_read_transaction_allows_a_writer_to_commit_before_it_finishes() {
    let handle = app_read_handle();
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let Ok(()) = start_rx.recv() else { return };
        let result = writer_handle.with_transaction(|transaction| {
            transaction.query("MATCH (t:Thread) SET t.title = 'Updated thread'")?;
            Ok(())
        });
        done_tx.send(result).unwrap();
    });
    let result = handle.with_read_transaction(4096, |snapshot| {
        let query = "MATCH (t:Thread) RETURN t.title AS title LIMIT 1";
        let before = snapshot.query(query)?;
        start_tx.send(()).unwrap();
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| {
                crate::HawDBError::Execution("writer blocked by read transaction".into())
            })??;
        assert_eq!(snapshot.query(query)?, before);
        Ok(())
    });
    drop(start_tx);
    writer.join().unwrap();
    result.unwrap();
}

fn read_canonical_pair(
    handle: &NowledgeMemEmbeddedStoreHandle,
    bounded: bool,
) -> crate::Result<(Value, Value, u64)> {
    let graph_query = "MATCH (t:Thread) RETURN t.title AS title LIMIT 1";
    let sql_query = "SELECT content FROM thread_messages LIMIT 1";
    let (graph, sql, epoch) = if bounded {
        handle.with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                Ok((
                    snapshot.query_cypher(graph_query, &BTreeMap::new(), 1)?,
                    snapshot.query_sql(sql_query, &[], 1)?,
                    snapshot.commit_epoch(),
                ))
            },
        )?
    } else {
        handle.with_read_transaction(4096, |snapshot| {
            Ok((
                snapshot.query(graph_query)?,
                snapshot.query_sql(sql_query)?,
                snapshot.commit_epoch(),
            ))
        })?
    };
    Ok((
        graph.rows[0]["title"].clone(),
        sql.rows[0]["content"].clone(),
        epoch,
    ))
}

#[test]
fn user_acquires_committed_graph_and_sql_while_writer_stages_then_observes_its_outcome() {
    for bounded in [false, true] {
        for commit in [false, true] {
            // Given a writer staging both canonical planes before the reader starts.
            let handle = app_read_handle();
            let before = read_canonical_pair(&handle, bounded).unwrap();
            let writer_handle = handle.clone();
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let writer = std::thread::spawn(move || {
                writer_handle.with_transaction(|transaction| {
                transaction.query("MATCH (t:Thread) SET t.title = 'Updated thread'")?;
                transaction.query_sql("UPDATE thread_messages SET content = 'Updated payload' WHERE content_message_id = 'message-1'")?;
                entered_tx.send(()).unwrap();
                if release_rx.recv().is_err() || !commit {
                    return Err(crate::HawDBError::Execution("rollback requested".into()));
                }
                Ok(())
            })
            });
            if let Err(error) = entered_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                drop(release_tx);
                panic!(
                    "writer did not stage: {error}; result: {:?}",
                    writer.join().unwrap()
                );
            }
            // When a new reader arrives, it completes before the writer is released.
            let reader_handle = handle.clone();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                let _ = result_tx.send(read_canonical_pair(&reader_handle, bounded));
            });
            let during = result_rx.recv_timeout(std::time::Duration::from_secs(5));
            // Always release and join before asserting, even on the old blocking implementation.
            let _ = release_tx.send(());
            let written = writer.join().unwrap();
            reader.join().unwrap();
            assert_eq!(
                during
                    .expect("new reader blocked behind staged writer")
                    .unwrap(),
                before
            );
            assert_eq!(written.is_ok(), commit);
            // Then a reader after the response sees the complete committed or rolled-back pair.
            let after = read_canonical_pair(&handle, bounded).unwrap();
            if commit {
                assert_eq!(after.0, Value::String("Updated thread".into()));
                assert_eq!(after.1, Value::String("Updated payload".into()));
                assert!(after.2 > before.2);
            } else {
                assert_eq!(after, before);
                handle
                    .with_transaction(|transaction| {
                        transaction.query("MATCH (t:Thread) SET t.title = 'After rollback'")?;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(
                    read_canonical_pair(&handle, bounded).unwrap().0,
                    Value::String("After rollback".into())
                );
            }
        }
    }
}

fn canonical_read_then<T>(
    handle: &NowledgeMemEmbeddedStoreHandle,
    bounded: bool,
    after_read: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<T> {
    if bounded {
        handle.with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                snapshot.query_cypher("MATCH (n) RETURN count(n) AS count", &BTreeMap::new(), 1)?;
                after_read()
            },
        )
    } else {
        handle.with_read_transaction(4096, |snapshot| {
            snapshot.query("MATCH (n) RETURN count(n) AS count")?;
            after_read()
        })
    }
}

#[test]
fn user_cannot_return_a_successful_snapshot_after_writer_unwinds() {
    for bounded in [false, true] {
        // Given a callback that has already obtained a valid old result.
        let handle = app_read_handle();
        let result = canonical_read_then(&handle, bounded, || {
            // When a writer unwinds after staging a change.
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _: crate::Result<()> = handle.with_transaction(|transaction| {
                    transaction.query("MATCH (t:Thread) SET t.title = 'Never published'")?;
                    panic!("writer failed");
                });
            }));
            assert!(panic.is_err());
            Ok(())
        });
        // Then the callback's late success and later acquisition both fail closed.
        let error = result.unwrap_err();
        assert!(error.to_string().contains("poisoned"), "{error}");
        assert!(handle.with_read_transaction(4096, |_| Ok(())).is_err());
    }
}

struct SnapshotTestRoot(std::path::PathBuf);

impl SnapshotTestRoot {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!(
            "hawdb_snapshot_publication_{}_{}_{}",
            std::process::id(),
            nanos,
            sequence
        )))
    }
}

impl Drop for SnapshotTestRoot {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn durable_snapshot_handle(root: &SnapshotTestRoot) -> NowledgeMemEmbeddedStoreHandle {
    let mut db = Database::open_with_config(
        &root.0,
        DatabaseConfig {
            storage_residency_mode: crate::store::StorageResidencyMode::OutOfCore,
            max_read_result_rows: Some(16),
            max_read_result_payload_bytes: Some(16 * 1024),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query("CREATE (:Record {id: 'committed'})").unwrap();
    db.checkpoint().unwrap();
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        db,
        NowledgeMemGraphMode::WritableCutover,
        snapshot_governor(),
    );
    NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None))
}

#[test]
fn user_keeps_shared_snapshot_pins_across_same_epoch_checkpoints_until_last_fork_drops() {
    // Given two forks of one published physical generation.
    let root = SnapshotTestRoot::new();
    let handle = durable_snapshot_handle(&root);
    let (permit_a, mut first) = handle.canonical_read_transaction(4096, None).unwrap();
    let (permit_b, mut second) = handle.canonical_read_transaction(4096, None).unwrap();
    let old_view = first.published_read_view();
    assert_eq!(second.published_read_view(), old_view);
    // When checkpoint changes only the physical generation, new acquisition refreshes its pin.
    handle.checkpoint().unwrap();
    let new_view = handle
        .with_read_transaction(4096, |tx| Ok(tx.published_read_view()))
        .unwrap();
    assert_eq!(
        new_view.visible_commit_epoch(),
        old_view.visible_commit_epoch()
    );
    assert_ne!(
        new_view.physical_generation(),
        old_view.physical_generation()
    );
    for _ in 0..3 {
        handle.checkpoint().unwrap();
    }
    assert!(root.0.join("canonical.1.hawdb").exists());
    assert!(!root.0.join("canonical.2.hawdb").exists());
    assert_eq!(
        first
            .query("MATCH (r:Record) RETURN r.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(first);
    drop(permit_a);
    handle.checkpoint().unwrap();
    assert!(root.0.join("canonical.1.hawdb").exists());
    assert_eq!(
        second
            .query("MATCH (r:Record) RETURN r.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
    // Then only the last fork releases the old generation for reclamation.
    drop(second);
    drop(permit_b);
    handle.checkpoint().unwrap();
    assert!(!root.0.join("canonical.1.hawdb").exists());
    assert!(!root.0.join("checkpoint.1.hawdb").exists());
}

#[test]
fn user_rejects_late_snapshot_success_after_post_wal_apply_failure() {
    for bounded in [false, true] {
        assert_late_snapshot_rejected(bounded, || crate::store::set_wal_apply_failpoint(Some(1)));
    }
}

#[test]
#[cfg(feature = "test-support")]
fn user_rejects_late_snapshot_success_after_uncertain_wal_append() {
    use crate::store::{set_wal_append_failpoint, WalAppendFailure};
    for failure in [WalAppendFailure::Rollback, WalAppendFailure::Sync] {
        for bounded in [false, true] {
            assert_late_snapshot_rejected(bounded, move || set_wal_append_failpoint(failure));
        }
    }
}

fn assert_late_snapshot_rejected(bounded: bool, failure: impl FnOnce() + Send + 'static) {
    use crate::store::set_wal_apply_failpoint;
    // Given a callback that has already read an intact committed snapshot.
    let root = SnapshotTestRoot::new();
    let handle = durable_snapshot_handle(&root);
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (failed_tx, failed_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        if start_rx.recv().is_err() {
            return;
        }
        let mut store = writer_handle.write_store().unwrap();
        let mut transaction = store.graph_mut().database_mut().begin_transaction();
        transaction.query("CREATE (:Record {id: 'new-a'})").unwrap();
        transaction.query("CREATE (:Record {id: 'new-b'})").unwrap();
        failure();
        let result = transaction.commit();
        set_wal_apply_failpoint(None);
        failed_tx.send(result).unwrap();
        // Retain the writer guard so publication cleanup cannot hide a copied poison flag.
        let _ = release_rx.recv();
    });
    let result = canonical_read_then(&handle, bounded, || {
        // When the writer fails but has not yet dropped its guard.
        start_tx.send(()).unwrap();
        let failure = failed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|error| crate::HawDBError::Execution(error.to_string()))?;
        assert!(failure.is_err());
        Ok(())
    });
    drop(start_tx);
    let _ = release_tx.send(());
    writer.join().unwrap();
    // Then even an already materialized result is rejected, as is new admission.
    let error = result.unwrap_err();
    assert!(error.to_string().contains("poisoned"), "{error}");
    assert!(handle
        .clone()
        .with_read_transaction(4096, |_| Ok(()))
        .is_err());
}

#[test]
#[cfg(feature = "test-support")]
fn user_can_read_and_commit_after_a_recoverable_partial_wal_append() {
    let root = SnapshotTestRoot::new();
    let handle = durable_snapshot_handle(&root);
    // Given a partial append whose rollback succeeds.
    let before = handle
        .with_read_transaction(4096, |tx| Ok(tx.commit_epoch()))
        .unwrap();
    let error = handle
        .with_transaction(|tx| {
            tx.query("CREATE (:Record {id: 'rolled-back'})")?;
            crate::store::set_wal_append_failpoint(crate::store::WalAppendFailure::PartialWrite);
            Ok(())
        })
        .unwrap_err();
    assert!(!matches!(error, crate::HawDBError::StorageIntegrity(_)));
    // When a new reader acquires the published snapshot, no partial mutation is visible.
    handle
        .with_read_transaction(4096, |tx| {
            assert_eq!(tx.commit_epoch(), before);
            assert_eq!(
                tx.query("MATCH (r:Record) RETURN r.id AS id")?.rows.len(),
                1
            );
            Ok(())
        })
        .unwrap();
    // Then a subsequent valid commit is published normally.
    handle
        .with_transaction(|tx| {
            tx.query("CREATE (:Record {id: 'later'})")?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        handle
            .with_read_transaction(4096, |tx| Ok(tx
                .query("MATCH (r:Record) RETURN r.id AS id")?
                .rows
                .len()))
            .unwrap(),
        2
    );
}

#[test]
fn user_releases_snapshot_admission_after_success_error_panic_and_budget_rejection() {
    let handle = app_read_handle();
    let governor = handle
        .read_store()
        .unwrap()
        .graph()
        .runtime_governor()
        .clone();
    for outcome in 0..4 {
        // Given the same governor used by writers and published snapshot readers.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle.with_bounded_graph_read_snapshot(
                NowledgeMemReadSnapshotBudget {
                    max_rows: if outcome == 3 { 17 } else { 1 },
                    max_payload_bytes: 4096,
                },
                |_| {
                    assert!(governor.snapshot().active_foreground_tasks > 0);
                    match outcome {
                        0 => Ok(()),
                        1 => Err(crate::HawDBError::Execution("callback cancelled".into())),
                        2 => panic!("read callback failed"),
                        _ => panic!("invalid budget reached callback"),
                    }
                },
            )
        }));
        // When the operation exits, its reservations are returned even on unwind or rejection.
        assert_eq!(result.is_err(), outcome == 2);
        if let Ok(result) = result {
            assert_eq!(result.is_ok(), outcome == 0);
        }
        let resources = governor.snapshot();
        assert_eq!(resources.active_foreground_tasks, 0);
        assert_eq!(resources.active_cpu_slots, 0);
        assert_eq!(resources.active_foreground_io_slots, 0);
        assert_eq!(resources.active_blocking_tasks, 0);
        assert_eq!(resources.admitted_memory_bytes, 0);
        // Then the healthy handle remains usable.
        handle.with_read_transaction(4096, |_| Ok(())).unwrap();
    }
    // Existing reservations must constrain acquisition from the published source too.
    let all_io = governor
        .try_admit(
            hawdb_qos::RuntimeWorkRequest::foreground_query(0, 0)
                .with_io_slots(governor.snapshot().limits.foreground_io_depth.get()),
        )
        .unwrap();
    assert!(handle
        .with_read_transaction::<()>(4096, |_| panic!("unadmitted callback"))
        .is_err());
    drop(all_io);
    handle.with_read_transaction(4096, |_| Ok(())).unwrap();
}

#[test]
fn user_can_reenter_snapshot_reads_from_admission_and_completion_telemetry() {
    use hawdb_qos::{RuntimeTelemetryEvent, RuntimeTelemetryEventKind};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc, Mutex};

    #[derive(Debug)]
    struct ReentrantTelemetry {
        target: RuntimeTelemetryEventKind,
        armed: AtomicBool,
        completed_during_callback: AtomicBool,
        start: mpsc::Sender<()>,
        done: Mutex<mpsc::Receiver<bool>>,
    }
    impl crate::TelemetrySink for ReentrantTelemetry {
        fn record_query(&self, _event: crate::QueryTelemetry<'_>) {}
        fn record_runtime(&self, event: RuntimeTelemetryEvent) {
            if event.kind == self.target && self.armed.swap(false, Ordering::SeqCst) {
                let _ = self.start.send(());
                let completed = self
                    .done
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap_or(false);
                self.completed_during_callback
                    .store(completed, Ordering::SeqCst);
            }
        }
    }

    for target in [
        RuntimeTelemetryEventKind::Admitted,
        RuntimeTelemetryEventKind::Completed,
    ] {
        // Given real host telemetry that reads through another clone of this handle.
        let handle = app_read_handle();
        let reader_handle = handle.clone();
        let (start, started) = mpsc::channel();
        let (finished, done) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            if started.recv().is_ok() {
                let result = reader_handle.with_read_transaction(4096, |tx| {
                    tx.query("MATCH (t:Thread) RETURN t.title AS title")
                });
                let _ = finished.send(result.is_ok());
            }
        });
        let sink = Arc::new(ReentrantTelemetry {
            target,
            armed: AtomicBool::new(false),
            completed_during_callback: AtomicBool::new(false),
            start,
            done: Mutex::new(done),
        });
        handle.set_telemetry_sink(Some(sink.clone())).unwrap();
        sink.armed.store(true, Ordering::SeqCst);
        // When admission succeeds or a row-budget error releases its admitted permit.
        let rejected = target == RuntimeTelemetryEventKind::Completed;
        let result = handle.with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: if rejected { 17 } else { 1 },
                max_payload_bytes: 4096,
            },
            |_| Ok(()),
        );
        handle.set_telemetry_sink(None).unwrap();
        let completed = sink.completed_during_callback.load(Ordering::SeqCst);
        drop(sink);
        // Join after the outer operation releases any lock, even on a failed regression.
        worker.join().unwrap();
        // Then the nested read completed before the synchronous telemetry callback returned.
        assert_eq!(result.is_err(), rejected);
        assert!(
            completed,
            "telemetry reentry waited for the publication lock: {target:?}"
        );
    }
}
