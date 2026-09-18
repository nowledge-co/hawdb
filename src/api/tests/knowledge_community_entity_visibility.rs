use super::*;

#[test]
fn reads_community_entity_visibility_for_wiki_anchor_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_alpha', name: 'Alpha', entity_type: 'concept', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_beta', name: 'Beta', entity_type: 'person', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_gamma', name: 'Gamma', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_other', name: 'Other', community_id: 9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_alpha_new', metadata: '{\"rank\":1}', is_latest: true, lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_alpha_old', metadata: '{\"rank\":2}', is_latest: false, lifecycle_state: 'archived'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_gamma', metadata: '{\"rank\":3}'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source_alpha', metadata: '{\"ignored\":true}'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_alpha_new'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_alpha_old'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_gamma'}), (e:Entity {id: 'entity_gamma'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (s:Source {id: 'source_alpha'}), (e:Entity {id: 'entity_alpha'}) CREATE (s)-[:MENTIONS]->(e)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let visibility = db
        .query_community_entity_visibility_via_cypher(&KnowledgeCommunityEntityVisibilityRequest {
            community_ids: vec![Value::Int(7), Value::Int(8)],
            limit: 0,
        })
        .unwrap();

    assert_eq!(visibility.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(visibility.matched_entity_count, 3);
    assert_eq!(visibility.matched_row_count, 4);
    assert_eq!(visibility.returned_count, 4);
    assert_eq!(visibility.rows[0].community_id, Value::Int(7));
    assert_eq!(
        visibility.rows[0].entity_id.as_deref(),
        Some("entity_alpha")
    );
    assert_eq!(visibility.rows[0].entity_name.as_deref(), Some("Alpha"));
    assert_eq!(visibility.rows[0].entity_type.as_deref(), Some("concept"));
    assert_eq!(
        visibility.rows[0].memory_id.as_deref(),
        Some("memory_alpha_new")
    );
    assert_eq!(
        visibility.rows[0].memory_metadata,
        Some(Value::String("{\"rank\":1}".to_string()))
    );
    assert!(visibility.rows[0].memory_is_latest);
    assert_eq!(
        visibility.rows[0].memory_lifecycle_state.as_deref(),
        Some("active")
    );
    assert_eq!(
        visibility.rows[1].memory_id.as_deref(),
        Some("memory_alpha_old")
    );
    assert!(!visibility.rows[1].memory_is_latest);
    assert_eq!(visibility.rows[2].entity_id.as_deref(), Some("entity_beta"));
    assert_eq!(visibility.rows[2].memory_id, None);
    assert_eq!(visibility.rows[2].memory_node_id, None);
    assert!(visibility.rows[2].memory_is_latest);
    assert_eq!(visibility.rows[3].community_id, Value::Int(8));
    assert_eq!(
        visibility.rows[3].entity_id.as_deref(),
        Some("entity_gamma")
    );
    assert_eq!(
        visibility.rows[3].memory_id.as_deref(),
        Some("memory_gamma")
    );
    assert!(visibility.rows[3].memory_is_latest);

    let cached_visibility = db
        .query_community_entity_visibility_via_cypher(&KnowledgeCommunityEntityVisibilityRequest {
            community_ids: vec![Value::Int(7), Value::Int(8)],
            limit: 0,
        })
        .unwrap();
    assert_eq!(cached_visibility, visibility);
    let stats = db.plan_cache_stats();
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}

#[test]
fn community_entity_visibility_rejects_invalid_scope() {
    let db = Database::new();

    let empty_ids_error = db
        .query_community_entity_visibility_via_cypher(&KnowledgeCommunityEntityVisibilityRequest {
            community_ids: Vec::new(),
            limit: 0,
        })
        .unwrap_err();
    assert!(empty_ids_error
        .to_string()
        .contains("non-empty community ids"));

    let null_id_error = db
        .query_community_entity_visibility_via_cypher(&KnowledgeCommunityEntityVisibilityRequest {
            community_ids: vec![Value::Null],
            limit: 0,
        })
        .unwrap_err();
    assert!(null_id_error.to_string().contains("non-null community ids"));
}
