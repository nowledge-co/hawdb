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

#[test]
fn clears_community_assignments_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_community_1', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_community_1', community_id: 9})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_without_community'})")
        .unwrap();

    let scoped = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: vec!["Entity".to_string(), "Memory".to_string()],
        })
        .unwrap();
    assert_eq!(scoped.candidate_count, 2);
    assert_eq!(scoped.cleared_count, 2);
    assert_eq!(scoped.rows.len(), 2);
    assert!(scoped.rows.iter().all(|row| row.cleared));

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_community_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_community_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Source".to_string(),
                    external_id: "source_community_1".to_string(),
                },
            ],
            property_names: vec!["community_id".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[1].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[2].properties.get("community_id"),
        Some(&Some(Value::Int(9)))
    );

    let all = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: Vec::new(),
        })
        .unwrap();
    assert_eq!(all.candidate_count, 1);
    assert_eq!(all.cleared_count, 1);
    assert_eq!(
        all.rows[0].external_id,
        Some("source_community_1".to_string())
    );
    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: "source_community_1".to_string(),
            }],
            property_names: vec!["community_id".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("community_id"),
        Some(&Some(Value::Null))
    );
}

#[test]
fn community_assignment_clear_rejects_invalid_label_before_wal() {
    let path = unique_test_dir("community_assignment_clear_invalid_label");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let error = db
        .clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: vec!["".to_string()],
        })
        .unwrap_err();

    assert!(error.to_string().contains("node label identifier is empty"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_assignment_clear_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_assignment_clear_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_community_1', community_id: 7})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_community_1', community_id: 8})")
            .unwrap();
        let batch_count_before_clear = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.clear_knowledge_community_assignments(&KnowledgeCommunityAssignmentClearRequest {
            labels: Vec::new(),
        })
        .unwrap();
        let batch_count_after_clear = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_clear, batch_count_before_clear + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    {
        let db = Database::open(&path).unwrap();
        let rows = db
            .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
                entities: vec![
                    KnowledgeEntityRequest {
                        label: "Memory".to_string(),
                        external_id: "memory_community_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_community_1".to_string(),
                    },
                ],
                property_names: vec!["community_id".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("community_id"),
            Some(&Some(Value::Null))
        );
        assert_eq!(
            rows.rows[1].properties.get("community_id"),
            Some(&Some(Value::Null))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
