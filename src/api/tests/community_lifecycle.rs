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
fn updates_community_lifecycle_for_nowledge_detection_and_summary() {
    let mut db = Database::new();
    db.query("CREATE (:Community {id: 'community_existing', community_id: 7, name: 'Old Name', description: 'old description', ai_summary: 'old summary', member_count: 2})")
        .unwrap();

    let output = db
        .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
            creates: vec![
                KnowledgeCommunityCreate {
                    id: "community_new".to_string(),
                    community_id: 8,
                    name: "New Community".to_string(),
                    description: Value::String("new description".to_string()),
                    ai_summary: Value::Null,
                    member_count: 3,
                    resolution: 1.25,
                    created_at: Value::Int(100),
                    updated_at: Value::Int(100),
                },
                KnowledgeCommunityCreate {
                    id: "community_existing".to_string(),
                    community_id: 7,
                    name: "Existing Community".to_string(),
                    description: Value::String("ignored".to_string()),
                    ai_summary: Value::String("ignored".to_string()),
                    member_count: 2,
                    resolution: 1.0,
                    created_at: Value::Int(101),
                    updated_at: Value::Int(101),
                },
            ],
            summary_updates: vec![
                KnowledgeCommunitySummaryUpdate {
                    id: "community_existing".to_string(),
                    name: "Updated Name".to_string(),
                    description: Value::String("updated description".to_string()),
                    ai_summary: Value::String("updated summary".to_string()),
                    updated_at: Value::Int(200),
                },
                KnowledgeCommunitySummaryUpdate {
                    id: "missing_community".to_string(),
                    name: "Missing".to_string(),
                    description: Value::String("missing".to_string()),
                    ai_summary: Value::String("missing".to_string()),
                    updated_at: Value::Int(201),
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 1);
    assert_eq!(output.graph_commit_epoch_after, 2);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.already_exists_count, 1);
    assert_eq!(output.updated_count, 1);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.created_node_count, 1);
    assert_eq!(output.updated_property_count, 4);
    assert!(output.create_rows[0].created);
    assert_eq!(output.create_rows[0].node_id, Some(1));
    assert!(output.create_rows[1].already_exists);
    assert!(output.summary_update_rows[0].updated);
    assert!(output.summary_update_rows[1].missing);

    let created = db
        .query("MATCH (c:Community {id: 'community_new'}) RETURN c.community_id, c.name, c.description, c.ai_summary, c.member_count, c.algorithm, c.resolution, c.created_at, c.updated_at")
        .unwrap();
    assert_eq!(created.rows[0].get("c.community_id"), Some(&Value::Int(8)));
    assert_eq!(
        created.rows[0].get("c.name"),
        Some(&Value::String("New Community".to_string()))
    );
    assert_eq!(
        created.rows[0].get("c.description"),
        Some(&Value::String("new description".to_string()))
    );
    assert_eq!(created.rows[0].get("c.ai_summary"), Some(&Value::Null));
    assert_eq!(created.rows[0].get("c.member_count"), Some(&Value::Int(3)));
    assert_eq!(
        created.rows[0].get("c.algorithm"),
        Some(&Value::String("louvain".to_string()))
    );
    assert_eq!(
        created.rows[0].get("c.resolution"),
        Some(&Value::Float(1.25))
    );
    assert_eq!(created.rows[0].get("c.created_at"), Some(&Value::Int(100)));
    assert_eq!(created.rows[0].get("c.updated_at"), Some(&Value::Int(100)));

    let updated = db
        .query("MATCH (c:Community {id: 'community_existing'}) RETURN c.name, c.description, c.ai_summary, c.updated_at")
        .unwrap();
    assert_eq!(
        updated.rows[0].get("c.name"),
        Some(&Value::String("Updated Name".to_string()))
    );
    assert_eq!(
        updated.rows[0].get("c.description"),
        Some(&Value::String("updated description".to_string()))
    );
    assert_eq!(
        updated.rows[0].get("c.ai_summary"),
        Some(&Value::String("updated summary".to_string()))
    );
    assert_eq!(updated.rows[0].get("c.updated_at"), Some(&Value::Int(200)));
}

#[test]
fn community_lifecycle_rejects_invalid_create_before_wal() {
    let path = unique_test_dir("community_lifecycle_invalid_create_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Community {id: 'community_existing'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let error = db
            .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
                creates: vec![KnowledgeCommunityCreate {
                    id: "community_invalid".to_string(),
                    community_id: 1,
                    name: "Invalid".to_string(),
                    description: Value::String("invalid".to_string()),
                    ai_summary: Value::Null,
                    member_count: 1,
                    resolution: f64::NAN,
                    created_at: Value::Int(1),
                    updated_at: Value::Int(1),
                }],
                summary_updates: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("resolution must be finite"));
        assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_lifecycle_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_lifecycle_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Community {id: 'community_existing', name: 'Old'})")
            .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .update_knowledge_communities_batch(&KnowledgeCommunityLifecycleBatchRequest {
                creates: vec![KnowledgeCommunityCreate {
                    id: "community_new".to_string(),
                    community_id: 9,
                    name: "New".to_string(),
                    description: Value::String("new".to_string()),
                    ai_summary: Value::String("summary".to_string()),
                    member_count: 4,
                    resolution: 0.75,
                    created_at: Value::Int(10),
                    updated_at: Value::Int(10),
                }],
                summary_updates: vec![KnowledgeCommunitySummaryUpdate {
                    id: "community_existing".to_string(),
                    name: "Updated".to_string(),
                    description: Value::String("updated".to_string()),
                    ai_summary: Value::String("updated summary".to_string()),
                    updated_at: Value::Int(20),
                }],
            })
            .unwrap();
        assert_eq!(output.created_node_count, 1);
        assert_eq!(output.updated_count, 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let communities = db
            .query("MATCH (c:Community) RETURN count(c) AS total")
            .unwrap();
        assert_eq!(communities.rows[0].get("total"), Some(&Value::Int(2)));
        let updated = db
            .query("MATCH (c:Community {id: 'community_existing'}) RETURN c.name, c.updated_at")
            .unwrap();
        assert_eq!(
            updated.rows[0].get("c.name"),
            Some(&Value::String("Updated".to_string()))
        );
        assert_eq!(updated.rows[0].get("c.updated_at"), Some(&Value::Int(20)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
