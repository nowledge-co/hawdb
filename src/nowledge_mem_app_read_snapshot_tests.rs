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
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::collections::BTreeMap;

#[test]
fn retrieval_from_search_preserves_admitted_memory_and_result_ceilings() {
    let mut database = Database::new();
    database
        .query("CREATE (:Memory {id: 'bounded', title: 'Budgeted memory'})")
        .unwrap();
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "bounded".into(),
            title: "Budgeted memory".into(),
            body: "search budget".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let request = crate::KnowledgeRetrievalRequest::nowledge_deep(
        "Budgeted",
        None,
        crate::search::SearchMode::Text,
        1,
        0,
        BTreeMap::new(),
    );
    for (memory, result, expected_error) in [
        (0, 4096, Some("requires admitted working memory")),
        (
            1 << 20,
            0,
            Some("requires a positive result payload budget"),
        ),
        (1 << 20, 4096, None),
    ] {
        let context = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(memory, result));
        let read = database
            .begin_read_transaction_with_context(&context)
            .unwrap();
        let search = index
            .search_with_report("Budgeted", None, crate::search::SearchMode::Text, 1)
            .unwrap();
        let output =
            read.try_retrieve_knowledge_from_search(search, index.projection_freshness(), &request);
        if let Some(message) = expected_error {
            assert!(output.unwrap_err().to_string().contains(message));
        } else {
            assert_eq!(output.unwrap().search.hits.len(), 1);
        }
    }
}

#[test]
fn search_helpers_preserve_their_pair_while_writers_and_fresh_helpers_advance() {
    use std::sync::mpsc;
    use std::time::Duration;

    for out_of_core in [false, true] {
        let root = SnapshotTestRoot::new();
        let handle = app_read_handle();
        if out_of_core {
            let mut store = handle.write_store().unwrap();
            let resident = store.search_projection.take().unwrap();
            let path = root.0.join("search");
            let mut writer =
                crate::SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
            for id in ["memory:farther", "memory:nearest"] {
                writer
                    .push(resident.index().document(id).unwrap().clone())
                    .unwrap();
            }
            writer.finish().unwrap();
            store.out_of_core_search_projection = Some(
                super::NowledgeMemOutOfCoreSearchProjection::open_incremental(
                    &path,
                    Default::default(),
                )
                .unwrap(),
            );
        }
        let request = crate::KnowledgeRetrievalRequest::nowledge_deep(
            "App",
            None,
            crate::SearchMode::Text,
            1,
            0,
            BTreeMap::from([("space_id".into(), "default".into())]),
        );
        let documents = vec!["memory:nearest".into()];
        let (start, started) = mpsc::channel();
        let (finished, done) = mpsc::channel();
        let writer_handle = handle.clone();
        let writer = std::thread::spawn(move || {
            if started.recv().is_ok() {
                let result = (|| {
                    writer_handle.with_transaction(|tx| {
                        tx.query("MATCH (m:Memory) SET m.title = 'Updated memory'")?;
                        Ok(())
                    })?;
                    writer_handle.apply_search_projection_delta_and_checkpoint(
                        crate::SearchProjectionDelta {
                            upserts: vec![SearchProjectionRow {
                                kind: SearchProjectionKind::Memory,
                                external_id: "nearest".into(),
                                title: "Updated projection".into(),
                                body: "App search candidate".into(),
                                embedding: Some(vec![1.0, 0.0]),
                                source_id: None,
                                metadata: BTreeMap::from([("space_id".into(), "default".into())]),
                            }],
                            ..Default::default()
                        },
                    )?;
                    Ok::<_, crate::HawDBError>(())
                })();
                let _ = finished.send(result);
            }
        });
        let result = handle.with_admitted_search(|old| {
            let before = old.retrieve(&request)?;
            assert_eq!(before.output.search.hits.len(), 1);
            assert_eq!(old.hydrate(&documents)?.documents[0].title, "nearest");
            start.send(()).unwrap();
            done.recv_timeout(Duration::from_secs(10)).map_err(|_| {
                crate::HawDBError::Execution("search helper blocked writer publication".into())
            })??;
            let fresh = handle.retrieve_knowledge_with_report(&request)?;
            assert!(fresh.output.graph_commit_epoch > before.output.graph_commit_epoch);
            assert_eq!(fresh.output.search.hits.len(), 1);
            assert_eq!(
                handle.search_projection_documents(&documents, 1)?[0].title,
                "Updated projection",
            );
            assert_eq!(old.hydrate(&documents)?.documents[0].title, "nearest");
            assert_eq!(
                old.retrieve(&request)?.output.graph_commit_epoch,
                before.output.graph_commit_epoch
            );
            let candidates = super::NowledgeMemSearchCandidateRequest::text("App", 1)
                .with_metadata_filters(BTreeMap::from([("space_id".into(), "default".into())]));
            assert_eq!(handle.search_candidates(&candidates)?.hits.len(), 1);
            assert_eq!(old.search_candidates(&candidates)?.result.hits.len(), 1,);
            Ok(())
        });
        drop(start);
        writer.join().unwrap();
        result.unwrap();
        assert_eq!(
            handle
                .runtime_governor_snapshot()
                .unwrap()
                .admitted_memory_bytes,
            0
        );
    }
}

#[path = "nowledge_mem_snapshot_scoring_tests.rs"]
mod scoring_tests;

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

pub(super) fn app_read_handle() -> NowledgeMemEmbeddedStoreHandle {
    app_read_handle_with_result_budget(16 * 1024)
}

pub(super) fn app_read_handle_with_result_budget(
    max_read_result_payload_bytes: usize,
) -> NowledgeMemEmbeddedStoreHandle {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(max_read_result_payload_bytes),
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
        .unwrap()
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
fn bounded_read_snapshot_uses_its_pinned_projection_for_text_seeds() {
    let handle = app_read_handle();
    handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 4,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let text = snapshot.query_cypher(
                    "CALL text_search($text, topK := 2) YIELD id, score \
                     RETURN id, score ORDER BY id",
                    &BTreeMap::from([("text".to_string(), Value::String("candidate".to_string()))]),
                    2,
                )?;
                assert_eq!(text.rows.len(), 2);
                assert!(text.rows.iter().all(
                    |row| matches!(row.get("score"), Some(Value::Float(score)) if *score > 0.0)
                ));

                let graph = snapshot.query_cypher(
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title LIMIT 1",
                    &BTreeMap::from([("id".to_string(), Value::String("nearest".to_string()))]),
                    1,
                )?;
                let relational = snapshot.query_sql(
                    "SELECT content FROM thread_messages WHERE content_message_id = $1 LIMIT 1",
                    &[Value::String("message-1".to_string())],
                    1,
                )?;
                assert_eq!(graph.rows.len(), 1);
                assert_eq!(relational.rows.len(), 1);
                assert_eq!(snapshot.report().output_rows, 4);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn resident_and_out_of_core_text_seeds_return_the_same_document_identities() {
    let root = SnapshotTestRoot::new();
    let search_path = root.0.join("search");
    let mut writer =
        crate::SearchOutOfCoreGenerationWriter::create(&search_path, Default::default()).unwrap();
    let mut resident = SearchIndex::in_memory();
    for external_id in ["first", "second"] {
        let row = SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: external_id.into(),
            title: "shared text".into(),
            body: "same document".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        };
        resident.upsert_projection_row(row.clone()).unwrap();
        writer.push(row.into_document()).unwrap();
    }
    writer.finish().unwrap();

    let graph = || {
        let mut database = Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(16),
            max_read_result_payload_bytes: Some(16 * 1024),
            ..Default::default()
        });
        database.query("CREATE (:Memory {id: 'first'})").unwrap();
        database.query("CREATE (:Memory {id: 'second'})").unwrap();
        NowledgeMemGraph::from_database_with_runtime_governor(
            database,
            NowledgeMemGraphMode::ShadowReadOnly,
            snapshot_governor(),
        )
    };
    let resident_handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
        graph(),
        Some(NowledgeMemSearchProjection::from_index(resident)),
    ))
    .unwrap();
    let out_of_core_handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph(),
            super::NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap(),
            Default::default(),
        ))
        .unwrap();
    let query = |handle: &NowledgeMemEmbeddedStoreHandle| {
        handle
            .with_bounded_read_snapshot(
                NowledgeMemReadSnapshotBudget {
                    max_rows: 2,
                    max_payload_bytes: 4096,
                },
                |snapshot| {
                    snapshot
                        .query_cypher(
                            "CALL text_search($text, topK := 2) YIELD id, score \
                             MATCH (m:Memory) RETURN id, m.id AS external_id ORDER BY id",
                            &BTreeMap::from([("text".into(), Value::String("shared".into()))]),
                            2,
                        )
                        .map(|result| result.rows)
                },
            )
            .unwrap()
    };
    let resident_rows = query(&resident_handle);
    assert_eq!(resident_rows.len(), 2);
    assert_eq!(
        resident_rows[0]["external_id"],
        Value::String("first".into())
    );
    assert_eq!(
        resident_rows[1]["external_id"],
        Value::String("second".into())
    );
    assert_eq!(query(&out_of_core_handle), resident_rows);
}

#[test]
fn bounded_text_seed_snapshot_retains_its_projection_during_a_writer_update() {
    let handle = app_read_handle();
    let writer_handle = handle.clone();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        if start_rx.recv().is_err() {
            return;
        }
        let result = (|| {
            let mut store = writer_handle.write_store()?;
            store
                .graph_mut()
                .database_mut()
                .query("MATCH (m:Memory {id: 'nearest'}) SET m.title = 'Updated memory'")?;
            store.graph_mut().database_mut().query_sql(
                "UPDATE thread_messages SET content = 'Updated payload' \
                 WHERE content_message_id = 'message-1'",
            )?;
            store
                .search_projection_mut()
                .unwrap()
                .index_mut()
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "nearest".into(),
                    title: "Updated memory".into(),
                    body: "Updated search candidate".into(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: None,
                    metadata: BTreeMap::from([("space_id".into(), "default".into())]),
                })?;
            crate::Result::Ok(())
        })();
        done_tx.send(result).unwrap();
    });
    let read = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| -> crate::Result<_> {
        let text = snapshot.query_cypher(
            "CALL text_search($text, topK := 2) YIELD id, score RETURN id ORDER BY id",
            &BTreeMap::from([("text".into(), Value::String("candidate".into()))]),
            2,
        )?;
        let graph = snapshot.query_cypher(
            "MATCH (m:Memory {id: 'nearest'}) RETURN m.title AS title LIMIT 1",
            &BTreeMap::new(),
            1,
        )?;
        let sql = snapshot.query_sql(
            "SELECT content FROM thread_messages WHERE content_message_id = 'message-1' LIMIT 1",
            &[],
            1,
        )?;
        Ok((text, graph, sql))
    };
    let result = handle.with_bounded_read_snapshot(
        NowledgeMemReadSnapshotBudget {
            max_rows: 8,
            max_payload_bytes: 4096,
        },
        |snapshot| {
            let before = read(snapshot)?;
            let epoch = snapshot.commit_epoch();
            start_tx.send(()).unwrap();
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("the retained text snapshot blocked writer publication")?;
            assert_eq!(read(snapshot)?, before);
            assert_eq!(snapshot.commit_epoch(), epoch);
            Ok(epoch)
        },
    );
    drop(start_tx);
    writer.join().unwrap();
    let epoch = result.unwrap();
    handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 3,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let text = snapshot.query_cypher(
                    "CALL text_search($text, topK := 1) YIELD id RETURN id",
                    &BTreeMap::from([("text".into(), Value::String("updated".into()))]),
                    1,
                )?;
                let graph = snapshot.query_cypher(
                    "MATCH (m:Memory {id: 'nearest'}) RETURN m.title AS title LIMIT 1",
                    &BTreeMap::new(),
                    1,
                )?;
                let sql = snapshot.query_sql(
                    "SELECT content FROM thread_messages WHERE content_message_id = 'message-1' LIMIT 1",
                    &[],
                    1,
                )?;
                assert_eq!(text.rows.len(), 1);
                assert_eq!(graph.rows[0]["title"], Value::String("Updated memory".into()));
                assert_eq!(sql.rows[0]["content"], Value::String("Updated payload".into()));
                assert!(snapshot.commit_epoch() > epoch);
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn bounded_read_snapshot_filters_text_seeds_before_top_k() {
    let handle = app_read_handle();
    handle
        .write_store()
        .unwrap()
        .graph_mut()
        .database_mut()
        .query("CREATE (:Memory {id: 'farther', space_id: 'other', title: 'Farther memory'})")
        .unwrap();
    let unfiltered = handle
        .search_candidates(&super::NowledgeMemSearchCandidateRequest::text(
            "candidate",
            1,
        ))
        .unwrap();
    let excluded_space = if unfiltered.hits[0].external_id.as_deref() == Some("nearest") {
        "other"
    } else {
        "default"
    };
    let expected = handle
        .search_candidates(
            &super::NowledgeMemSearchCandidateRequest::text("candidate", 1).with_metadata_filters(
                BTreeMap::from([("space_id".into(), excluded_space.into())]),
            ),
        )
        .unwrap();
    assert_eq!(expected.hits.len(), 1);
    assert_ne!(expected.hits[0].id, unfiltered.hits[0].id);
    let parameters = BTreeMap::from([
        ("text".into(), Value::String("candidate".into())),
        ("space_id".into(), Value::String(excluded_space.into())),
    ]);
    handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let result = snapshot.query_cypher(
                    "CALL text_search($text, topK := 1) YIELD id, score \
                     MATCH (m:Memory) WHERE m.space_id = $space_id \
                     RETURN m.id AS id, score",
                    &parameters,
                    1,
                )?;
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0].get("id"),
                    Some(&Value::String(
                        expected.hits[0].external_id.clone().unwrap()
                    ))
                );
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn graph_only_snapshot_refuses_text_seeds_without_a_pinned_projection() {
    let handle = app_read_handle();
    handle
        .with_bounded_graph_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            |snapshot| {
                let error = snapshot
                    .query_cypher(
                        "CALL text_search($text, topK := 1) YIELD id, score RETURN id",
                        &BTreeMap::from([("text".into(), Value::String("candidate".into()))]),
                        1,
                    )
                    .unwrap_err();
                assert!(matches!(error, crate::HawDBError::Storage(ref message)
                    if message == "nowledge mem search projection is not configured"));
                Ok(())
            },
        )
        .unwrap();
}

#[test]
fn bounded_graph_read_snapshot_exposes_profiled_query_memory_evidence() {
    let handle = app_read_handle();
    let context = RuntimeTaskContext::default();
    let report = handle
        .with_bounded_graph_read_snapshot_context(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 4096,
            },
            &context,
            |snapshot| {
                let query = snapshot.query_cypher_profiled(
                    "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
                    &BTreeMap::new(),
                    1,
                )?;
                assert_eq!(query.output.rows.len(), 1);
                let memory = &query.execution_profile.pipeline_memory_report;
                assert!(memory.query_memory_budget_bytes > 0);
                assert!(memory.query_memory_peak_bytes <= memory.query_memory_budget_bytes);
                assert_eq!(memory.output_rows, 1);
                Ok(snapshot.report())
            },
        )
        .unwrap();

    assert_eq!(report.cypher_statement_count, 1);
    assert_eq!(report.output_rows, 1);
    assert_eq!(report.remaining_rows, 1);
}

#[test]
fn bounded_graph_read_snapshot_propagates_cancellation_without_leaking_admission() {
    let handle = app_read_handle();
    let governor = handle
        .read_store()
        .unwrap()
        .graph()
        .runtime_governor()
        .clone();
    let cancellation = RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(cancellation.clone());
    let before = governor.snapshot();

    let error = handle
        .with_bounded_graph_read_snapshot_context(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            &context,
            |snapshot| {
                assert!(cancellation.cancel());
                snapshot
                    .query_cypher_profiled(
                        "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
                        &BTreeMap::new(),
                        1,
                    )
                    .map(|_| ())
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("cancelled"));
    let after = governor.snapshot();
    assert_eq!(after.admissions, before.admissions + 1);
    assert_eq!(after.completions, before.completions + 1);
    assert_eq!(after.active_foreground_tasks, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.active_blocking_tasks, 0);
    assert_eq!(after.admitted_memory_bytes, 0);
}

#[test]
fn bounded_graph_read_snapshot_propagates_deadline_without_leaking_admission() {
    let handle = app_read_handle();
    let governor = handle
        .read_store()
        .unwrap()
        .graph()
        .runtime_governor()
        .clone();
    let context = RuntimeTaskContext::with_timeout(std::time::Duration::ZERO);
    let before = governor.snapshot();

    let error = handle
        .with_bounded_graph_read_snapshot_context(
            NowledgeMemReadSnapshotBudget {
                max_rows: 1,
                max_payload_bytes: 4096,
            },
            &context,
            |snapshot| {
                snapshot
                    .query_cypher_profiled(
                        "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
                        &BTreeMap::new(),
                        1,
                    )
                    .map(|_| ())
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("deadline"));
    let after = governor.snapshot();
    assert_eq!(after.admissions, before.admissions + 1);
    assert_eq!(after.completions, before.completions + 1);
    assert_eq!(after.active_foreground_tasks, 0);
    assert_eq!(after.active_cpu_slots, 0);
    assert_eq!(after.active_blocking_tasks, 0);
    assert_eq!(after.admitted_memory_bytes, 0);
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
    assert!(matches!(error, crate::HawDBError::ReadBudgetExceeded(cause)
        if cause.resource == crate::ReadBudgetResource::Rows && cause.limit == 1));
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
    assert!(matches!(error, crate::HawDBError::ReadBudgetExceeded(cause)
        if cause.resource == crate::ReadBudgetResource::PayloadBytes && cause.limit == 2));

    handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 10,
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
                let before_failure = snapshot.report();
                assert_eq!(before_failure.output_payload_bytes, 10);
                assert_eq!(before_failure.remaining_payload_bytes, 0);
                let error = snapshot
                    .query_sql(
                        "SELECT content_message_id FROM thread_messages \
                         WHERE thread_storage_id = $1 LIMIT 1",
                        &[Value::String("thread-1".to_string())],
                        1,
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("exhausted max_payload_bytes"));
                assert!(matches!(error, crate::HawDBError::ReadBudgetExceeded(cause)
                    if cause.resource == crate::ReadBudgetResource::PayloadBytes && cause.limit == 10));
                assert_eq!(snapshot.report(), before_failure);
                Ok(())
            },
        )
        .unwrap();
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
    NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None)).unwrap()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotContextCase {
    Success,
    AnalyticsProfile,
    RowBudget,
    PayloadBudget,
    Cancellation,
    Deadline,
    MemoryReservation,
    MemoryRejection,
}

fn assert_read_only_snapshot_context(case: SnapshotContextCase) {
    for residency in [
        crate::store::StorageResidencyMode::Materialized,
        crate::store::StorageResidencyMode::OutOfCore,
    ] {
        let root = SnapshotTestRoot::new();
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            max_read_result_rows: Some(16),
            max_read_result_payload_bytes: Some(16 * 1024),
            ..DatabaseConfig::default()
        };
        let mut writer = Database::open_with_config(&root.0, config.clone()).unwrap();
        writer.query("CREATE (:Record {id: 'committed'})").unwrap();
        writer
            .query_sql("CREATE TABLE records (id TEXT PRIMARY KEY)")
            .unwrap();
        writer
            .query_sql("INSERT INTO records (id) VALUES ('committed')")
            .unwrap();
        #[cfg(feature = "graph-analytics")]
        writer
            .query("CALL project_graph('snapshot_context', ['Record'], [])")
            .unwrap();
        writer.checkpoint().unwrap();
        let epoch = writer.commit_epoch().unwrap();
        drop(writer);

        let hawdb_storage::branch_project::ProjectManifest::Branch(selector) =
            hawdb_storage::branch_project::inspect_project_manifest(&root.0).unwrap()
        else {
            panic!("expected a branch project")
        };
        let branch = root
            .0
            .join("branches")
            .join(selector.main_branch_id().as_uuid().to_string());
        let head =
            hawdb_storage::branch_head::read_branch_head(&branch.join("branch.head")).unwrap();
        let wal_path = branch.join(hawdb_storage::artifact_files::wal_generation_file(
            head.active_wal.generation,
        ));
        let wal = std::fs::read(&wal_path).unwrap();
        let read_only_config = DatabaseConfig {
            read_only: true,
            recovery_mode: crate::RecoveryMode::Strict,
            ..config
        };
        let database = Database::open_with_config(&root.0, read_only_config.clone()).unwrap();
        assert_eq!(
            database.storage_residency_report().unwrap().out_of_core,
            residency == crate::store::StorageResidencyMode::OutOfCore
        );
        let governor = snapshot_governor();
        let graph = NowledgeMemGraph::from_database_with_runtime_governor(
            database,
            NowledgeMemGraphMode::ShadowReadOnly,
            governor.clone(),
        );
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ))
        .unwrap();

        for pin_projection in [false, true] {
            for sql_first in [false, true] {
                let cancellation = RuntimeCancellationToken::new();
                let context = match case {
                    SnapshotContextCase::Deadline => {
                        RuntimeTaskContext::with_timeout(std::time::Duration::ZERO)
                    }
                    SnapshotContextCase::MemoryReservation
                    | SnapshotContextCase::MemoryRejection => RuntimeTaskContext::default()
                        .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(
                            if case == SnapshotContextCase::MemoryReservation {
                                32 * 1024 * 1024
                            } else {
                                64 * 1024
                            },
                            4096,
                        )),
                    _ => RuntimeTaskContext::without_deadline(cancellation.clone()),
                };
                let budget = NowledgeMemReadSnapshotBudget {
                    max_rows: if case == SnapshotContextCase::RowBudget {
                        1
                    } else {
                        4
                    },
                    max_payload_bytes: if case == SnapshotContextCase::PayloadBudget {
                        20
                    } else {
                        4096
                    },
                };
                let before = governor.snapshot();
                let operation = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| {
                    assert_eq!(snapshot.commit_epoch(), epoch);
                    assert_eq!(snapshot.report().search_projection_present, pin_projection);
                    if case == SnapshotContextCase::Cancellation {
                        assert!(cancellation.cancel());
                    }
                    let mut query = |sql| -> crate::Result<()> {
                        let output = if sql {
                            snapshot.query_sql("SELECT id FROM records LIMIT 1", &[], 1)?
                        } else {
                            let statement = if case == SnapshotContextCase::AnalyticsProfile {
                                "CALL page_rank('snapshot_context') RETURN node, pagerank_score"
                            } else {
                                "MATCH (r:Record) RETURN r.id AS id LIMIT 1"
                            };
                            let query =
                                snapshot.query_cypher_profiled(statement, &BTreeMap::new(), 1)?;
                            let memory = &query.execution_profile.pipeline_memory_report;
                            assert_eq!(memory.output_rows, 1);
                            assert!(
                                memory.query_memory_peak_bytes <= memory.query_memory_budget_bytes
                            );
                            if case == SnapshotContextCase::MemoryReservation {
                                assert_eq!(memory.query_memory_budget_bytes, 32 * 1024 * 1024);
                            }
                            query.output
                        };
                        assert_eq!(output.rows.len(), 1);
                        if !sql && case == SnapshotContextCase::AnalyticsProfile {
                            assert_eq!(output.rows[0]["node"], Value::Int(0));
                            // A dangling singleton retains only the teleport term.
                            assert_eq!(output.rows[0]["pagerank_score"], Value::Float(1.0 - 0.85));
                        } else {
                            assert_eq!(output.rows[0]["id"], Value::String("committed".into()));
                        }
                        Ok(())
                    };
                    query(sql_first)?;
                    query(!sql_first)?;
                    let report = snapshot.report();
                    assert_eq!(report.output_rows, 2);
                    assert_eq!(report.cypher_statement_count, 1);
                    assert_eq!(report.sql_statement_count, 1);
                    assert_eq!(report.remaining_rows, 2);
                    Ok(())
                };
                let result = if pin_projection {
                    handle.with_bounded_read_snapshot_context(budget, &context, operation)
                } else {
                    handle.with_bounded_graph_read_snapshot_context(budget, &context, operation)
                };
                match case {
                    SnapshotContextCase::Success
                    | SnapshotContextCase::AnalyticsProfile
                    | SnapshotContextCase::MemoryReservation => {
                        result.unwrap();
                    }
                    _ => {
                        let error = result.unwrap_err().to_string();
                        let expected = match case {
                            SnapshotContextCase::RowBudget => "exhausted max_rows",
                            SnapshotContextCase::PayloadBudget => "payload",
                            SnapshotContextCase::Cancellation => "cancelled",
                            SnapshotContextCase::Deadline => "deadline",
                            SnapshotContextCase::MemoryRejection => "query_memory_bytes 65536",
                            _ => unreachable!(),
                        };
                        assert!(error.contains(expected), "{case:?}: {error}");
                    }
                }
                let after = governor.snapshot();
                assert_eq!(after.admissions, before.admissions + 1);
                assert_eq!(after.completions, before.completions + 1);
                assert_eq!(after.active_foreground_tasks, 0);
                assert_eq!(after.active_background_tasks, 0);
                assert_eq!(after.active_cpu_slots, 0);
                assert_eq!(after.active_blocking_tasks, 0);
                assert_eq!(after.active_foreground_io_slots, 0);
                assert_eq!(after.active_background_io_slots, 0);
                assert_eq!(after.admitted_memory_bytes, 0);
                assert_eq!(after.queued_admission_waiters, 0);
                assert_eq!(
                    handle
                        .read_store()
                        .unwrap()
                        .graph()
                        .database()
                        .commit_epoch()
                        .unwrap(),
                    epoch
                );
                assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
            }
        }
        drop(handle);
        let reopened = Database::open_with_config(&root.0, read_only_config).unwrap();
        assert_eq!(reopened.commit_epoch().unwrap(), epoch);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    }
}

#[test]
fn bounded_snapshot_context_read_only_success_preserves_wal_and_epoch() {
    assert_read_only_snapshot_context(SnapshotContextCase::Success);
}

#[test]
#[cfg(feature = "graph-analytics")]
fn bounded_snapshot_context_read_only_analytics_exposes_same_execution_profile() {
    assert_read_only_snapshot_context(SnapshotContextCase::AnalyticsProfile);
}

#[test]
fn bounded_snapshot_context_read_only_row_budget_releases_admission() {
    assert_read_only_snapshot_context(SnapshotContextCase::RowBudget);
}

#[test]
fn bounded_snapshot_context_read_only_payload_budget_releases_admission() {
    assert_read_only_snapshot_context(SnapshotContextCase::PayloadBudget);
}

#[test]
fn bounded_snapshot_context_read_only_cancellation_releases_admission() {
    assert_read_only_snapshot_context(SnapshotContextCase::Cancellation);
}

#[test]
fn bounded_snapshot_context_read_only_deadline_releases_admission() {
    assert_read_only_snapshot_context(SnapshotContextCase::Deadline);
}

#[test]
fn bounded_snapshot_context_read_only_reservation_bounds_profile_memory() {
    assert_read_only_snapshot_context(SnapshotContextCase::MemoryReservation);
}

#[test]
fn bounded_snapshot_context_read_only_undersized_memory_releases_admission() {
    assert_read_only_snapshot_context(SnapshotContextCase::MemoryRejection);
}

#[test]
fn user_keeps_shared_snapshot_pins_across_same_epoch_checkpoints_until_last_fork_drops() {
    // Given two forks of one published physical generation.
    let root = SnapshotTestRoot::new();
    let handle = durable_snapshot_handle(&root);
    let (permit_a, mut first) = handle.canonical_read_transaction(4096, None).unwrap();
    let (permit_b, mut second) = handle.canonical_read_transaction(4096, None).unwrap();
    let old_view = first.published_read_view();
    let old_generation = old_view.physical_generation().unwrap().0;
    let hawdb_storage::branch_project::ProjectManifest::Branch(selector) =
        hawdb_storage::branch_project::inspect_project_manifest(&root.0).unwrap()
    else {
        panic!("expected a branch project")
    };
    let data = root
        .0
        .join("branches")
        .join(selector.main_branch_id().as_uuid().to_string())
        .join("data");
    assert_eq!(second.published_read_view(), old_view);
    // When checkpoint changes only the physical generation, new acquisition refreshes its pin.
    handle.checkpoint().unwrap();
    let new_view = handle
        .with_read_transaction(4096, |tx| Ok(tx.published_read_view()))
        .unwrap();
    let first_new_generation = new_view.physical_generation().unwrap().0;
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
    assert!(data
        .join(format!("canonical.{old_generation}.hawdb"))
        .exists());
    assert!(!data
        .join(format!("canonical.{first_new_generation}.hawdb"))
        .exists());
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
    assert!(data
        .join(format!("canonical.{old_generation}.hawdb"))
        .exists());
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
    assert!(!data
        .join(format!("canonical.{old_generation}.hawdb"))
        .exists());
    assert!(!data
        .join(format!("checkpoint.{old_generation}.hawdb"))
        .exists());
}

#[test]
fn failed_snapshot_publication_retains_the_last_generation_and_requires_reopen() {
    let root = SnapshotTestRoot::new();
    let handle = durable_snapshot_handle(&root);
    let clone = handle.clone();
    let (permit, mut reader) = handle.canonical_read_transaction(4096, None).unwrap();
    let before = reader.published_read_view();
    let committed = {
        let mut store = handle.write_store().unwrap();
        let database = store.graph_mut().database_mut();
        database
            .query("CREATE (:Record {id: 'new-commit'})")
            .unwrap();
        assert!(database.commit_epoch().unwrap() > before.visible_commit_epoch());
        // A real deferred handle cannot admit while the committed database
        // still owns main's writer lease. Capture must surface that failure.
        let pending = Database::open_with_config(&root.0, database.config().clone()).unwrap();
        let committed = std::mem::replace(database, pending);
        assert!(matches!(
            database.read_snapshot(),
            Err(crate::HawDBError::BranchBusy { .. })
        ));
        committed
    };
    // Publication failure must neither drop the previous pin nor expose its
    // stale contents as the newly committed current view.
    assert_eq!(
        handle
            .published_read
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .view
            .snapshot
            .begin_read_transaction(&hawdb_core::RuntimeTaskContext::default())
            .unwrap()
            .published_read_view(),
        before
    );
    assert_eq!(
        reader
            .query("MATCH (r:Record) RETURN r.id")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(matches!(
        handle.finish_canonical_read(Ok(())),
        Err(crate::HawDBError::Execution(reason)) if reason.contains("poisoned")
    ));
    for handle in [&handle, &clone] {
        assert!(matches!(
            handle.with_read_transaction(4096, |_| Ok(())),
            Err(crate::HawDBError::Execution(reason)) if reason.contains("poisoned")
        ));
    }
    // Normal writers cannot repair a failed publication. Even direct test-only
    // restoration must preserve the evidence until the handle is reopened.
    assert!(handle.write_store().is_err());
    {
        let mut store = handle.inner.write().unwrap();
        *store.graph_mut().database_mut() = committed;
    }
    assert!(matches!(
        handle.with_read_transaction(4096, |_| Ok(())),
        Err(crate::HawDBError::Execution(reason)) if reason.contains("poisoned")
    ));
    drop(reader);
    drop(permit);
    drop(clone);
    drop(handle);
    let mut reopened = Database::open(&root.0).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (r:Record) RETURN r.id")
            .unwrap()
            .rows
            .len(),
        2
    );
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
        let mut transaction = store
            .graph_mut()
            .database_mut()
            .begin_transaction()
            .unwrap();
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

#[test]
fn bounded_read_snapshot_projection_scan_accounts_complete_output_and_requires_pin() {
    let handle = app_read_handle();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    handle
        .with_bounded_read_snapshot(budget, |snapshot| {
            let mut documents = Vec::new();
            let report = snapshot.visit_search_projection(4096, |document| {
                documents.push(document.clone());
                Ok(())
            })?;
            assert_eq!(report.document_count, 2);
            assert_eq!(documents.len(), 2);
            // Complete scans share the snapshot's cumulative budget with all reads.
            snapshot.visit_search_projection(4096, |_| Ok(()))?;
            let mut calls = 0;
            assert!(snapshot
                .visit_search_projection(4096, |_| {
                    calls += 1;
                    Ok(())
                })
                .is_err());
            assert_eq!(calls, 0);
            Ok(())
        })
        .unwrap();
    handle
        .with_bounded_graph_read_snapshot(budget, |snapshot| {
            assert!(snapshot
                .visit_search_projection(4096, |_| panic!("graph-only scan callback"))
                .is_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn bounded_read_snapshot_projection_scan_observes_same_epoch_payload_and_bit_drift() {
    let handle = app_read_handle();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 16,
        max_payload_bytes: 16384,
    };
    let mut row = SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: "nearest".into(),
        title: "before".into(),
        body: "complete body".into(),
        embedding: Some(vec![1.0, -0.0]),
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "default".into())]),
    };
    let apply = |row| {
        handle
            .apply_search_projection_delta_and_checkpoint(crate::SearchProjectionDelta {
                upserts: vec![row],
                source_graph_commit_epoch: Some(42),
                ..Default::default()
            })
            .unwrap()
    };
    let capture = || {
        handle
            .with_bounded_read_snapshot(budget, |snapshot| {
                let mut documents = BTreeMap::new();
                let scan = snapshot.visit_search_projection(4096, |document| {
                    documents.insert(document.id.clone(), document.clone());
                    Ok(())
                })?;
                Ok((snapshot.report(), scan, documents))
            })
            .unwrap()
    };
    apply(row.clone());
    let before = capture();
    row.title = "after".into();
    row.metadata.insert("new_key".into(), "new_value".into());
    row.embedding = Some(vec![f32::from_bits(1.0_f32.to_bits() + 1), 0.0]);
    apply(row);
    let after = capture();
    assert_eq!(before.0.commit_epoch, after.0.commit_epoch);
    assert_eq!(
        before.0.search_projection_source_graph_commit_epoch,
        after.0.search_projection_source_graph_commit_epoch
    );
    assert_eq!(before.1.document_count, after.1.document_count);
    let before_document = &before.2["memory:nearest"];
    let after_document = &after.2["memory:nearest"];
    assert_ne!(before_document.title, after_document.title);
    assert_ne!(before_document.metadata, after_document.metadata);
    assert_ne!(
        before_document.embedding.as_ref().unwrap()[0].to_bits(),
        after_document.embedding.as_ref().unwrap()[0].to_bits()
    );
    assert_eq!(
        before_document.embedding.as_ref().unwrap()[1].to_bits(),
        (-0.0_f32).to_bits()
    );
    assert_eq!(
        after_document.embedding.as_ref().unwrap()[1].to_bits(),
        0.0_f32.to_bits()
    );
}

#[test]
fn bounded_read_snapshot_projection_scan_unwind_releases_native_pin() {
    let handle = app_read_handle();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = handle.with_bounded_read_snapshot(budget, |snapshot| {
            snapshot.visit_search_projection(4096, |_| panic!("visitor unwound"))
        });
    }));
    assert!(panic.is_err());
    assert_eq!(
        handle
            .with_bounded_read_snapshot(budget, |snapshot| {
                snapshot.visit_search_projection(4096, |_| Ok(()))
            })
            .unwrap()
            .document_count,
        2
    );
    // An exclusive store operation also succeeds after the visitor unwinds.
    handle
        .apply_search_projection_delta_and_checkpoint(crate::SearchProjectionDelta::default())
        .unwrap();
}

#[test]
fn bounded_read_snapshot_projection_scan_uses_out_of_core_pin_and_releases_admission() {
    let root = SnapshotTestRoot::new();
    let search_path = root.0.join("search");
    let row = SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: "ooc".into(),
        title: "complete".into(),
        body: "pinned persistent projection".into(),
        embedding: Some(vec![1.0, -0.0]),
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "team".into())]),
    };
    let document = row.into_document();
    let mut writer =
        crate::SearchOutOfCoreGenerationWriter::create(&search_path, Default::default()).unwrap();
    writer.push(document.clone()).unwrap();
    writer.finish().unwrap();
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(16),
            max_read_result_payload_bytes: Some(16384),
            ..Default::default()
        }),
        NowledgeMemGraphMode::ShadowReadOnly,
        snapshot_governor(),
    );
    let projection = super::NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph,
            projection,
            Default::default(),
        ))
        .unwrap();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    let mut observed = Vec::new();
    let report = handle
        .with_bounded_read_snapshot(budget, |snapshot| {
            let text = snapshot.query_cypher(
                "CALL text_search($text, topK := 1) YIELD id, score RETURN id, score",
                &BTreeMap::from([("text".into(), Value::String("persistent projection".into()))]),
                1,
            )?;
            assert_eq!(text.rows.len(), 1);
            snapshot.visit_search_projection(4096, |document| {
                observed.push(document.clone());
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(observed, vec![document]);
    assert_eq!(
        observed[0].embedding.as_ref().unwrap()[1].to_bits(),
        (-0.0_f32).to_bits()
    );
    assert_eq!(report.document_count, 1);
    assert!(report.artifact_generation.is_some());
    for cumulative in [
        NowledgeMemReadSnapshotBudget {
            max_rows: 1,
            ..budget
        },
        NowledgeMemReadSnapshotBudget {
            max_payload_bytes: report.payload_bytes,
            ..budget
        },
    ] {
        handle
            .with_bounded_read_snapshot(cumulative, |snapshot| {
                snapshot.visit_search_projection(4096, |_| Ok(()))?;
                let mut calls = 0;
                assert!(snapshot
                    .visit_search_projection(4096, |_| {
                        calls += 1;
                        Ok(())
                    })
                    .is_err());
                assert_eq!(calls, 0);
                Ok(())
            })
            .unwrap();
    }
    assert!(handle
        .with_bounded_read_snapshot(budget, |snapshot| snapshot
            .visit_search_projection(16, |_| panic!("scratch refusal callback")))
        .is_err());
    let runtime = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(runtime.admissions, runtime.completions);
    assert_eq!(runtime.admitted_memory_bytes, 0);
    assert_eq!(runtime.active_foreground_io_slots, 0);
    drop(handle);
}

#[test]
fn bounded_out_of_core_text_seed_filters_before_top_k() {
    let root = SnapshotTestRoot::new();
    let search_path = root.0.join("search");
    let mut writer =
        crate::SearchOutOfCoreGenerationWriter::create(&search_path, Default::default()).unwrap();
    for (id, space, title) in [
        ("other", "other", "graph graph graph graph graph"),
        ("team", "team", "graph"),
    ] {
        writer
            .push(
                SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: id.into(),
                    title: title.into(),
                    body: "storage".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::from([("space_id".into(), space.into())]),
                }
                .into_document(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(1024 * 1024),
        ..Default::default()
    });
    database
        .query("CREATE (:Memory {id: 'other', space_id: 'other'})")
        .unwrap();
    database
        .query("CREATE (:Memory {id: 'team', space_id: 'team'})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("text".into(), Value::String("graph".into())),
        ("space_id".into(), Value::String("team".into())),
    ]);
    let query = "CALL text_search($text, topK := 1) YIELD id, score \
                 MATCH (m:Memory) WHERE m.space_id = $space_id \
                 RETURN m.id AS id, score";
    let plan = database
        .explain_query_with_params(query, &parameters)
        .unwrap()
        .to_string();
    assert!(plan.contains("TextSeedScan"));
    assert!(plan.contains("top_k=1"));
    let pinned_reader = crate::SearchOutOfCoreReader::open(&search_path).unwrap();
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        database,
        NowledgeMemGraphMode::ShadowReadOnly,
        snapshot_governor(),
    );
    let projection = super::NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph,
            projection,
            Default::default(),
        ))
        .unwrap();
    let reported = handle
        .query_with_params_with_report_options(
            query,
            &parameters,
            super::NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(reported.output.rows.len(), 1);
    assert_eq!(reported.output.rows[0]["id"], Value::String("team".into()));
    assert!(reported
        .report
        .physical_operator_counts
        .contains_key("TextSeedScan"));
    let analyzed = handle
        .query_with_params_with_report(&format!("EXPLAIN ANALYZE {query}"), &parameters)
        .unwrap();
    assert_eq!(analyzed.output.rows.len(), 1);
    assert_eq!(analyzed.output.rows[0]["row_count"], Value::Int(1));
    for prefix in ["EXPLAIN", "EXPLAIN ANALYZE"] {
        handle
            .with_bounded_read_snapshot(
                NowledgeMemReadSnapshotBudget {
                    max_rows: 1,
                    max_payload_bytes: 1024 * 1024,
                },
                |snapshot| {
                    let output =
                        snapshot.query_cypher(&format!("{prefix} {query}"), &parameters, 1)?;
                    assert_eq!(output.rows.len(), 1);
                    assert_eq!(snapshot.report().output_rows, 1);
                    assert_eq!(
                        snapshot.report().output_payload_bytes,
                        output.rows.payload_bytes()
                    );
                    if prefix == "EXPLAIN ANALYZE" {
                        assert_eq!(output.rows[0]["row_count"], Value::Int(1));
                    }
                    Ok(())
                },
            )
            .unwrap();
    }
    handle
        .with_bounded_read_snapshot(
            NowledgeMemReadSnapshotBudget {
                max_rows: 2,
                max_payload_bytes: 8192,
            },
            |snapshot| {
                let result = snapshot.query_cypher_profiled(query, &parameters, 1)?;
                assert_eq!(result.output.rows.len(), 1);
                assert_eq!(
                    result.output.rows[0].get("id"),
                    Some(&Value::String("team".into()))
                );
                assert!(result
                    .execution_profile
                    .operator_cardinality_profiles
                    .iter()
                    .any(|profile| {
                        profile.operator.as_str() == "TextSeedScan"
                            && profile.actual_rows == Some(1)
                    }));
                let memory = &result.execution_profile.pipeline_memory_report;
                assert!(memory.query_memory_peak_bytes > 0);
                assert!(memory.query_memory_peak_bytes <= memory.query_memory_budget_bytes);
                crate::SearchOutOfCoreGenerationWriter::prepare_delta(
                    &pinned_reader,
                    crate::SearchProjectionDelta {
                        upserts: vec![SearchProjectionRow {
                            kind: SearchProjectionKind::Memory,
                            external_id: "team".into(),
                            title: "replacement".into(),
                            body: "storage".into(),
                            embedding: None,
                            source_id: None,
                            metadata: BTreeMap::from([("space_id".into(), "team".into())]),
                        }],
                        deletes: vec!["memory:other".into()],
                        ..Default::default()
                    },
                    Default::default(),
                )?
                .finish()?;
                let after_publication = snapshot.query_cypher(query, &parameters, 1)?;
                assert_eq!(
                    after_publication.rows[0].get("id"),
                    Some(&Value::String("team".into()))
                );
                assert_eq!(snapshot.report().output_rows, 2);
                Ok(())
            },
        )
        .unwrap();
    let current_reader = crate::SearchOutOfCoreReader::open(&search_path).unwrap();
    assert_ne!(pinned_reader.generation(), current_reader.generation());
}

#[test]
fn out_of_core_snapshot_allows_generation_publication_and_keeps_its_old_pair() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let root = SnapshotTestRoot::new();
    let path = root.0.join("search");
    let row = |title: &str| SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: "memory".into(),
        title: title.into(),
        body: "projection payload".into(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    };
    let mut writer =
        crate::SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
    writer.push(row("Before").into_document()).unwrap();
    writer.finish().unwrap();
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(16 * 1024),
        ..Default::default()
    });
    database
        .query("CREATE (:Memory {id: 'memory', title: 'Before'})")
        .unwrap();
    let graph = NowledgeMemGraph::from_database_with_runtime_governor(
        database,
        NowledgeMemGraphMode::WritableCutover,
        snapshot_governor(),
    );
    let projection =
        super::NowledgeMemOutOfCoreSearchProjection::open_incremental(&path, Default::default())
            .unwrap();
    let old_reader = Arc::downgrade(&projection.reader);
    let handle =
        NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new_with_out_of_core_search(
            graph,
            projection,
            Default::default(),
        ))
        .unwrap();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 8192,
    };
    let read_pair = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| -> crate::Result<_> {
        let graph = snapshot.query_cypher(
            "MATCH (m:Memory) RETURN m.title AS title",
            &BTreeMap::new(),
            1,
        )?;
        let mut titles = Vec::new();
        let report = snapshot.visit_search_projection(4096, |document| {
            titles.push(document.title.clone());
            Ok(())
        })?;
        Ok((
            graph.rows[0]["title"].clone(),
            titles,
            report.artifact_generation,
        ))
    };
    let writer_handle = handle.clone();
    let delta = crate::SearchProjectionDelta {
        upserts: vec![row("After")],
        ..Default::default()
    };
    let (start, started) = mpsc::channel();
    let (finished, done) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        if started.recv().is_ok() {
            let result = (|| {
                writer_handle.with_transaction(|tx| {
                    tx.query("MATCH (m:Memory) SET m.title = 'After'")
                        .map(|_| ())
                })?;
                writer_handle.apply_search_projection_delta_and_checkpoint(delta)
            })();
            let _ = finished.send(result);
        }
    });
    let result = handle.with_bounded_read_snapshot(budget, |snapshot| {
        let before = read_pair(snapshot)?;
        start.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(10)).map_err(|_| {
            crate::HawDBError::Execution("projection snapshot blocked publication".into())
        })??;
        let after = handle.with_bounded_read_snapshot(budget, read_pair)?;
        assert_eq!(before.0, Value::String("Before".into()));
        assert_eq!(before.1, vec!["Before"]);
        assert_eq!(after.0, Value::String("After".into()));
        assert_eq!(after.1, vec!["After"]);
        assert_ne!(before.2, after.2);
        assert_eq!(read_pair(snapshot)?, before);
        assert!(old_reader.upgrade().is_some());
        Ok(())
    });
    drop(start);
    writer.join().unwrap();
    result.unwrap();
    assert!(old_reader.upgrade().is_none());
    assert_eq!(
        handle
            .runtime_governor_snapshot()
            .unwrap()
            .admitted_memory_bytes,
        0
    );
}

#[test]
fn database_instance_identity_survives_committed_handle_writes() {
    let handle = app_read_handle();
    let writer = handle.clone();
    let identity = handle.database_instance_identity().unwrap();
    let epoch = handle.runtime_status().unwrap().graph_commit_epoch;
    writer
        .query_with_report("MATCH (m:Memory {id: 'nearest'}) SET m.title = 'Updated memory'")
        .unwrap();
    assert!(handle.runtime_status().unwrap().graph_commit_epoch > epoch);
    assert_eq!(handle.database_instance_identity().unwrap(), identity);
    assert_eq!(writer.database_instance_identity().unwrap(), identity);

    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 1,
        max_payload_bytes: 4096,
    };
    for pin_projection in [false, true] {
        let check = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| {
            assert_eq!(snapshot.database_instance_identity(), &identity);
            let output = snapshot.query_cypher(
                "MATCH (m:Memory {id: 'nearest'}) RETURN m.title AS title LIMIT 1",
                &BTreeMap::new(),
                1,
            )?;
            assert_eq!(output.rows.len(), 1);
            assert_eq!(
                output.rows[0].get("title"),
                Some(&Value::String("Updated memory".into()))
            );
            Ok(())
        };
        if pin_projection {
            handle.with_bounded_read_snapshot(budget, check).unwrap();
        } else {
            handle
                .with_bounded_graph_read_snapshot(budget, check)
                .unwrap();
        }
    }
}

#[test]
fn database_instance_identity_tracks_the_exact_published_read_view() {
    let handle = app_read_handle();
    let cloned = handle.clone();
    let old_identity = handle.database_instance_identity().unwrap();
    assert_eq!(cloned.database_instance_identity().unwrap(), old_identity);
    let replacement = app_read_handle();
    let replacement_identity = replacement.database_instance_identity().unwrap();
    assert_ne!(old_identity, replacement_identity);
    assert_eq!(
        handle.runtime_status().unwrap().graph_commit_epoch,
        replacement.runtime_status().unwrap().graph_commit_epoch
    );
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 4,
        max_payload_bytes: 4096,
    };
    {
        let mut writer = handle.write_store().unwrap();
        let mut replacement_writer = replacement.write_store().unwrap();
        std::mem::swap(
            writer.graph_mut().database_mut(),
            replacement_writer.graph_mut().database_mut(),
        );
        assert_eq!(
            writer.graph().database().instance_identity().unwrap(),
            replacement_identity
        );
        // The committed publication still owns the previous database snapshot.
        handle
            .with_bounded_graph_read_snapshot(budget, |snapshot| {
                assert_eq!(snapshot.database_instance_identity(), &old_identity);
                let rows = snapshot.query_cypher(
                    "MATCH (n:Memory) RETURN n.id AS id",
                    &BTreeMap::new(),
                    4,
                )?;
                assert_eq!(rows.rows.len(), 1);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(
        handle.database_instance_identity().unwrap(),
        replacement_identity
    );
    for pin_projection in [false, true] {
        let check = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| {
            assert_eq!(snapshot.database_instance_identity(), &replacement_identity);
            Ok(())
        };
        if pin_projection {
            handle.with_bounded_read_snapshot(budget, check).unwrap();
        } else {
            handle
                .with_bounded_graph_read_snapshot(budget, check)
                .unwrap();
        }
    }
}

#[test]
fn resident_snapshot_preserves_its_pair_while_writers_and_fresh_readers_advance() {
    use std::sync::mpsc;
    use std::time::Duration;

    let handle = app_read_handle();
    let budget = NowledgeMemReadSnapshotBudget {
        max_rows: 16,
        max_payload_bytes: 8192,
    };
    let read_pair = |snapshot: &mut super::NowledgeMemReadSnapshot<'_>| -> crate::Result<_> {
        let output = snapshot.query_cypher(
            "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
            &BTreeMap::from([("id".into(), Value::String("nearest".into()))]),
            1,
        )?;
        let mut title = None;
        snapshot.visit_search_projection(4096, |document| {
            if document.id == "memory:nearest" {
                title = Some(document.title.clone());
            }
            Ok(())
        })?;
        Ok((output.rows[0]["title"].clone(), title))
    };
    let writer_handle = handle.clone();
    let (start, started) = mpsc::channel();
    let (finished, done) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        if started.recv().is_ok() {
            let result = (|| {
                for revision in 1..=3 {
                    let title = format!("Revision {revision}");
                    writer_handle.with_transaction(|transaction| {
                        transaction.query_request(
                            crate::QueryRequest::new(
                                "MATCH (m:Memory {id: $id}) SET m.title = $title",
                            )
                            .with_params(&BTreeMap::from([
                                ("id".into(), Value::String("nearest".into())),
                                ("title".into(), Value::String(title.clone())),
                            ])),
                        )?;
                        Ok(())
                    })?;
                    writer_handle.apply_search_projection_delta_and_checkpoint(
                        crate::SearchProjectionDelta {
                            upserts: vec![SearchProjectionRow {
                                kind: SearchProjectionKind::Memory,
                                external_id: "nearest".into(),
                                title,
                                body: "replacement projection".into(),
                                embedding: Some(vec![1.0, 0.0]),
                                source_id: None,
                                metadata: BTreeMap::new(),
                            }],
                            ..Default::default()
                        },
                    )?;
                }
                Ok::<_, crate::HawDBError>(())
            })();
            let _ = finished.send(result);
        }
    });
    let result = handle.with_bounded_read_snapshot(budget, |snapshot| {
        let before = read_pair(snapshot)?;
        start.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(10)).map_err(|_| {
            crate::HawDBError::Execution("resident snapshot blocked writer progress".into())
        })??;
        let after = handle.with_bounded_read_snapshot(budget, read_pair)?;
        assert_eq!(
            before,
            (
                Value::String("Nearest memory".into()),
                Some("nearest".into())
            )
        );
        assert_eq!(
            after,
            (
                Value::String("Revision 3".into()),
                Some("Revision 3".into())
            )
        );
        assert_eq!(read_pair(snapshot)?, before);
        Ok(())
    });
    drop(start);
    writer.join().unwrap();
    result.unwrap();
    assert_eq!(
        handle
            .runtime_governor_snapshot()
            .unwrap()
            .admitted_memory_bytes,
        0
    );
}
