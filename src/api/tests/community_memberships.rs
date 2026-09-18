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
fn creates_community_memberships_for_nowledge_entity_lifecycle() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Entity One'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_2', name: 'Entity Two'})")
        .unwrap();
    db.query("CREATE (:Community {id: 'community_1', name: 'Community One'})")
        .unwrap();

    let output = db
        .create_knowledge_community_memberships_batch(
            &KnowledgeCommunityMembershipCreateBatchRequest {
                memberships: vec![
                    KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_1".to_string(),
                        community_id: "community_1".to_string(),
                        strength: 0.7,
                        created_at: Value::Int(13),
                        properties: Value::String("{}".to_string()),
                    },
                    KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_2".to_string(),
                        community_id: "missing_community".to_string(),
                        strength: 0.9,
                        created_at: Value::Int(14),
                        properties: Value::String("{\"source\":\"test\"}".to_string()),
                    },
                ],
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 3);
    assert_eq!(output.graph_commit_epoch_after, 4);
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.matched_count, 1);
    assert_eq!(output.missing_endpoint_count, 1);
    assert_eq!(output.created_relationship_count, 1);
    assert!(output.rows[0].created);
    assert_eq!(output.rows[0].entity_node_id, Some(0));
    assert_eq!(output.rows[0].community_node_id, Some(2));
    assert!(!output.rows[1].created);
    assert_eq!(output.rows[1].entity_node_id, Some(1));
    assert_eq!(output.rows[1].community_node_id, None);

    let relationships = db
        .query("MATCH (e:Entity {id: 'entity_1'})-[r:BELONGS_TO]->(c:Community) RETURN c.id, r.strength, r.created_at, r.properties")
        .unwrap();
    assert_eq!(relationships.rows.len(), 1);
    assert_eq!(
        relationships.rows[0].get("c.id"),
        Some(&Value::String("community_1".to_string()))
    );
    assert_eq!(
        relationships.rows[0].get("r.strength"),
        Some(&Value::Float(0.7))
    );
    assert_eq!(
        relationships.rows[0].get("r.created_at"),
        Some(&Value::Int(13))
    );
    assert_eq!(
        relationships.rows[0].get("r.properties"),
        Some(&Value::String("{}".to_string()))
    );
    let nodes = db
        .query("MATCH (n) WHERE n.id IN ['entity_1', 'entity_2', 'community_1'] RETURN count(n) AS total")
        .unwrap();
    assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
}

#[test]
fn community_membership_create_rejects_invalid_input_before_wal() {
    let path = unique_test_dir("community_membership_create_invalid_before_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Community {id: 'community_1'})").unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let error = db
            .create_knowledge_community_memberships_batch(
                &KnowledgeCommunityMembershipCreateBatchRequest {
                    memberships: vec![KnowledgeCommunityMembershipCreate {
                        entity_id: "entity_1".to_string(),
                        community_id: "community_1".to_string(),
                        strength: f64::NAN,
                        created_at: Value::Int(13),
                        properties: Value::String("{}".to_string()),
                    }],
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("strength must be finite"));
        assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn typed_community_membership_create_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_community_membership_create_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
        db.query("CREATE (:Community {id: 'community_1'})").unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .create_knowledge_community_memberships_batch(
                &KnowledgeCommunityMembershipCreateBatchRequest {
                    memberships: vec![
                        KnowledgeCommunityMembershipCreate {
                            entity_id: "entity_1".to_string(),
                            community_id: "community_1".to_string(),
                            strength: 0.6,
                            created_at: Value::Int(21),
                            properties: Value::String("{}".to_string()),
                        },
                        KnowledgeCommunityMembershipCreate {
                            entity_id: "entity_2".to_string(),
                            community_id: "community_1".to_string(),
                            strength: 0.8,
                            created_at: Value::Int(22),
                            properties: Value::String("{}".to_string()),
                        },
                    ],
                },
            )
            .unwrap();
        assert_eq!(output.created_relationship_count, 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let relationships = db
            .query("MATCH (:Entity)-[r:BELONGS_TO]->(:Community) RETURN count(r) AS total")
            .unwrap();
        assert_eq!(relationships.rows[0].get("total"), Some(&Value::Int(2)));
        let nodes = db.query("MATCH (n) RETURN count(n) AS total").unwrap();
        assert_eq!(nodes.rows[0].get("total"), Some(&Value::Int(3)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
