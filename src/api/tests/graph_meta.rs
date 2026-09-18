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

const GRAPH_META_ALGORITHM_STATE_QUERY: &str = "MATCH (m:GraphMeta) \
     WHERE m.meta_id = $meta_id \
     RETURN m.meta_id AS meta_id, id(m) AS node_id, \
     m.pagerank_applied AS pagerank_applied, \
     m.community_detection_applied AS community_detection_applied, \
     m.pagerank_computed_at AS pagerank_computed_at \
     ORDER BY node_id ASC LIMIT 1";

const GRAPH_META_FUTURE_STATE_QUERY: &str = "MATCH (m:GraphMeta) \
     WHERE m.meta_id = $meta_id \
     RETURN m.meta_id AS meta_id, id(m) AS node_id, \
     m.pagerank_applied AS pagerank_applied, \
     m.future_state_field AS future_state_field \
     ORDER BY node_id ASC LIMIT 1";

#[test]
fn stamps_graph_meta_batch_for_nowledge_algorithm_state() {
    let mut db = Database::new();
    db.query(
        "CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false, pagerank_iterations: 5})",
    )
    .unwrap();

    let output = db
        .stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([
                        ("pagerank_applied".to_string(), Value::Bool(true)),
                        (
                            "pagerank_algorithm".to_string(),
                            Value::String("pagerank".to_string()),
                        ),
                        ("pagerank_damping".to_string(), Value::Float(0.85)),
                        ("pagerank_iterations".to_string(), Value::Int(20)),
                        ("pagerank_computed_at".to_string(), Value::Int(100)),
                        ("updated_at".to_string(), Value::Int(101)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "community".to_string(),
                    assignments: BTreeMap::from([
                        ("community_detection_applied".to_string(), Value::Bool(true)),
                        (
                            "community_algorithm".to_string(),
                            Value::String("louvain".to_string()),
                        ),
                        ("community_resolution".to_string(), Value::Float(0.8)),
                        ("community_count".to_string(), Value::Int(3)),
                        (
                            "community_detection_computed_at".to_string(),
                            Value::Int(200),
                        ),
                        ("last_augmentation_at".to_string(), Value::Int(201)),
                        ("updated_at".to_string(), Value::Int(202)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([(
                        "pagerank_applied".to_string(),
                        Value::Bool(false),
                    )]),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.rows.len(), 3);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_property_count, 13);
    assert!(output.rows[0].updated);
    assert!(output.rows[1].created);
    assert!(output.rows[2].duplicate);
    assert!(output.rows[1].node_id.is_some());

    let rows = db
        .query("MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_algorithm AS algorithm, m.pagerank_damping AS damping, m.pagerank_iterations AS iterations, m.pagerank_computed_at AS computed_at, m.updated_at AS updated_at")
        .unwrap();
    assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
    assert_eq!(
        rows.rows[0].get("algorithm"),
        Some(&Value::String("pagerank".to_string()))
    );
    assert_eq!(rows.rows[0].get("damping"), Some(&Value::Float(0.85)));
    assert_eq!(rows.rows[0].get("iterations"), Some(&Value::Int(20)));
    assert_eq!(rows.rows[0].get("computed_at"), Some(&Value::Int(100)));
    assert_eq!(rows.rows[0].get("updated_at"), Some(&Value::Int(101)));

    let rows = db
        .query("MATCH (m:GraphMeta {meta_id: 'community'}) RETURN m.community_detection_applied AS applied, m.community_algorithm AS algorithm, m.community_resolution AS resolution, m.community_count AS count, m.community_detection_computed_at AS computed_at, m.last_augmentation_at AS augmented")
        .unwrap();
    assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
    assert_eq!(
        rows.rows[0].get("algorithm"),
        Some(&Value::String("louvain".to_string()))
    );
    assert_eq!(rows.rows[0].get("resolution"), Some(&Value::Float(0.8)));
    assert_eq!(rows.rows[0].get("count"), Some(&Value::Int(3)));
    assert_eq!(rows.rows[0].get("computed_at"), Some(&Value::Int(200)));
    assert_eq!(rows.rows[0].get("augmented"), Some(&Value::Int(201)));
}

#[test]
fn graph_meta_stamp_rejects_meta_id_assignment_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![KnowledgeGraphMetaStamp {
                meta_id: "main".to_string(),
                assignments: BTreeMap::from([(
                    "meta_id".to_string(),
                    Value::String("other".to_string()),
                )]),
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("cannot update meta_id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_graph_meta_stamp_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_graph_meta_stamp_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: false})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.stamp_knowledge_graph_meta_batch(&KnowledgeGraphMetaStampBatchRequest {
            stamps: vec![
                KnowledgeGraphMetaStamp {
                    meta_id: "main".to_string(),
                    assignments: BTreeMap::from([
                        ("pagerank_applied".to_string(), Value::Bool(true)),
                        ("pagerank_computed_at".to_string(), Value::Int(404)),
                    ]),
                },
                KnowledgeGraphMetaStamp {
                    meta_id: "community".to_string(),
                    assignments: BTreeMap::from([
                        (
                            "community_detection_applied".to_string(),
                            Value::Bool(false),
                        ),
                        (
                            "community_algorithm".to_string(),
                            Value::String(String::new()),
                        ),
                        ("community_resolution".to_string(), Value::Float(1.0)),
                        ("community_count".to_string(), Value::Int(0)),
                        ("community_detection_computed_at".to_string(), Value::Null),
                    ]),
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    assert!(wal.contains("create_node"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (m:GraphMeta {meta_id: 'main'}) RETURN m.pagerank_applied AS applied, m.pagerank_computed_at AS computed")
            .unwrap();
        assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(true)));
        assert_eq!(rows.rows[0].get("computed"), Some(&Value::Int(404)));
        let rows = db
            .query("MATCH (m:GraphMeta {meta_id: 'community'}) RETURN m.community_detection_applied AS applied, m.community_algorithm AS algorithm, m.community_resolution AS resolution, m.community_count AS count, m.community_detection_computed_at AS computed")
            .unwrap();
        assert_eq!(rows.rows[0].get("applied"), Some(&Value::Bool(false)));
        assert_eq!(
            rows.rows[0].get("algorithm"),
            Some(&Value::String(String::new()))
        );
        assert_eq!(rows.rows[0].get("resolution"), Some(&Value::Float(1.0)));
        assert_eq!(rows.rows[0].get("count"), Some(&Value::Int(0)));
        assert_eq!(rows.rows[0].get("computed"), Some(&Value::Null));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reads_graph_meta_by_meta_id_with_parameterized_cypher() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, community_detection_applied: false, pagerank_computed_at: 100})")
        .unwrap();

    let parameters = BTreeMap::from([("meta_id".to_string(), Value::String("main".to_string()))]);
    let mut read = db.begin_read_transaction();
    let output = read
        .query_with_params_bounded(GRAPH_META_ALGORITHM_STATE_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(read.commit_epoch(), 1);
    let meta = &output.rows[0];
    assert_eq!(
        meta.get("meta_id"),
        Some(&Value::String("main".to_string()))
    );
    assert_eq!(meta.get("pagerank_applied"), Some(&Value::Bool(true)));
    assert_eq!(
        meta.get("community_detection_applied"),
        Some(&Value::Bool(false))
    );
    assert_eq!(meta.get("pagerank_computed_at"), Some(&Value::Int(100)));

    let entries = read_test_plan_cache_metric(&read, "entries");
    let misses = read_test_plan_cache_metric(&read, "misses");
    let hits = read_test_plan_cache_metric(&read, "hits");
    let repeated_output = read
        .query_with_params_bounded(GRAPH_META_ALGORITHM_STATE_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(repeated_output, output);
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), entries);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), misses);
    assert!(read_test_plan_cache_metric(&read, "hits") > hits);

    let missing_parameters =
        BTreeMap::from([("meta_id".to_string(), Value::String("missing".to_string()))]);
    let missing = read
        .query_with_params_bounded(
            GRAPH_META_ALGORITHM_STATE_QUERY,
            &missing_parameters,
            Some(1),
        )
        .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn fixed_graph_meta_projections_support_state_growth_and_pinned_reads() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, community_detection_applied: false, pagerank_computed_at: 100, future_state_field: 'future'})")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let mut snapshot = db.begin_read_transaction();

    db.query("MATCH (m:GraphMeta {meta_id: 'main'}) SET m.future_state_field = 'late', m.extra_field = 'extra'")
        .unwrap();

    let parameters = BTreeMap::from([("meta_id".to_string(), Value::String("main".to_string()))]);
    let mut live = db.begin_read_transaction();
    let projected = live
        .query_with_params_bounded(GRAPH_META_FUTURE_STATE_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(live.commit_epoch(), db.store.commit_epoch());
    let meta = &projected.rows[0];
    assert_eq!(
        meta.get("meta_id"),
        Some(&Value::String("main".to_string()))
    );
    assert_eq!(meta.get("pagerank_applied"), Some(&Value::Bool(true)));
    assert_eq!(
        meta.get("future_state_field"),
        Some(&Value::String("late".to_string()))
    );
    assert!(!meta.contains_key("community_detection_applied"));
    assert!(!meta.contains_key("extra_field"));

    let entries = read_test_plan_cache_metric(&live, "entries");
    let misses = read_test_plan_cache_metric(&live, "misses");
    let hits = read_test_plan_cache_metric(&live, "hits");
    let repeated_projected = live
        .query_with_params_bounded(GRAPH_META_FUTURE_STATE_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(repeated_projected, projected);
    assert_eq!(read_test_plan_cache_metric(&live, "entries"), entries);
    assert_eq!(read_test_plan_cache_metric(&live, "misses"), misses);
    assert!(read_test_plan_cache_metric(&live, "hits") > hits);

    let snapshot_projected = snapshot
        .query_with_params_bounded(GRAPH_META_FUTURE_STATE_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(snapshot.commit_epoch(), graph_commit_epoch);
    assert_eq!(
        snapshot_projected.rows[0].get("future_state_field"),
        Some(&Value::String("future".to_string()))
    );

    let missing_parameters =
        BTreeMap::from([("meta_id".to_string(), Value::String("missing".to_string()))]);
    let missing = live
        .query_with_params_bounded(GRAPH_META_FUTURE_STATE_QUERY, &missing_parameters, Some(1))
        .unwrap();
    assert!(missing.rows.is_empty());
}

#[test]
fn graph_meta_empty_read_returns_no_rows_and_delete_rejects_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let parameters = BTreeMap::from([("meta_id".to_string(), Value::String(String::new()))]);
    let read = db
        .query_read_only_with_params_bounded(GRAPH_META_ALGORITHM_STATE_QUERY, &parameters, Some(1))
        .unwrap();
    assert!(read.rows.is_empty());

    let delete_error = db
        .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
            meta_id: String::new(),
        })
        .unwrap_err();
    assert!(delete_error.to_string().contains("non-empty meta id"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn graph_meta_read_parameters_cannot_change_query_shape_or_wal() {
    let path = unique_test_dir("graph_meta_read_parameters_cannot_change_query_shape_or_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let parameters = BTreeMap::from([(
            "meta_id".to_string(),
            Value::String("main') MATCH (n) RETURN n //".to_string()),
        )]);
        let output = db
            .query_read_only_with_params_bounded(
                GRAPH_META_FUTURE_STATE_QUERY,
                &parameters,
                Some(1),
            )
            .unwrap();
        assert!(output.rows.is_empty());
        assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn graph_meta_delete_missing_does_not_write_wal() {
    let path = unique_test_dir("graph_meta_delete_missing");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "missing".to_string(),
            })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert!(output.node_id.is_none());
        assert!(!output.matched);
        assert!(!output.deleted);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_graph_meta_delete_persists_and_replays() {
    let path = unique_test_dir("typed_graph_meta_delete_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true})")
            .unwrap();
        db.query("CREATE (:GraphMeta {meta_id: 'community', community_detection_applied: true})")
            .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .delete_knowledge_graph_meta(&KnowledgeGraphMetaRequest {
                meta_id: "main".to_string(),
            })
            .unwrap();
        assert!(output.matched);
        assert!(output.deleted);
        assert!(output.node_id.is_some());
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let db = Database::open(&path).unwrap();
        let main_parameters =
            BTreeMap::from([("meta_id".to_string(), Value::String("main".to_string()))]);
        let main = db
            .query_read_only_with_params_bounded(
                GRAPH_META_ALGORITHM_STATE_QUERY,
                &main_parameters,
                Some(1),
            )
            .unwrap();
        assert!(main.rows.is_empty());
        let community_parameters = BTreeMap::from([(
            "meta_id".to_string(),
            Value::String("community".to_string()),
        )]);
        let community = db
            .query_read_only_with_params_bounded(
                GRAPH_META_ALGORITHM_STATE_QUERY,
                &community_parameters,
                Some(1),
            )
            .unwrap();
        assert_eq!(community.rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}
