use super::*;

#[test]
fn reads_community_memories_for_wiki_ranking_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity_alpha', name: 'Alpha', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_beta', name: 'Beta', community_id: 7})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_gamma', name: 'Gamma', community_id: 8})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_other', name: 'Other', community_id: 9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_alpha', title: 'Alpha memory', content: 'Alpha content', unit_type: 'learning', metadata: '{\"rank\":1}', is_latest: true, lifecycle_state: 'active', importance: 0.8, created_at: 30, pagerank_score: 0.2, is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_beta', title: 'Beta memory', content: 'Beta content', metadata: '{\"rank\":2}', is_latest: false, lifecycle_state: 'archived', importance: 0.9, created_at: 20, pagerank_score: 0.9, is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_null_crystal', title: 'Gamma memory', content: 'Gamma content', importance: 0.7, created_at: 40})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_crystal', title: 'Crystal memory', importance: 1.0, created_at: 50, is_crystal: true})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'direct_alpha', title: 'Direct Alpha', content: 'Direct content', community_id: 7, metadata: '{\"direct\":true}', is_crystal: false, importance: 0.6, created_at: 50})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'direct_null', title: 'Direct Null', community_id: 8, importance: 0.4, created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'direct_crystal', title: 'Direct Crystal', community_id: 7, is_crystal: true, importance: 1.0, created_at: 60})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_alpha'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_alpha'}), (e:Entity {id: 'entity_beta'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_beta'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_null_crystal'}), (e:Entity {id: 'entity_gamma'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_crystal'}), (e:Entity {id: 'entity_alpha'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory_alpha'}), (e:Entity {id: 'entity_other'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let mentioned = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Int(7), Value::Int(8)],
            source: KnowledgeCommunityMemorySource::MentionedEntities,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::NullOrFalse,
            unit_types: Vec::new(),
            order: KnowledgeCommunityMemoryListOrder::CommunityBreadthImportanceCreatedAt,
            limit: 0,
        })
        .unwrap();

    assert_eq!(mentioned.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(mentioned.matched_row_count, 3);
    assert_eq!(mentioned.returned_count, 3);
    assert_eq!(
        mentioned
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["memory_alpha", "memory_beta", "memory_null_crystal"]
    );
    assert_eq!(mentioned.rows[0].community_id, Value::Int(7));
    assert_eq!(
        mentioned.rows[0].source,
        KnowledgeCommunityMemoryRowSource::MentionedEntities
    );
    assert_eq!(mentioned.rows[0].mention_breadth, 2);
    assert_eq!(
        mentioned.rows[0].entity_ids,
        vec!["entity_alpha".to_string(), "entity_beta".to_string()]
    );
    assert_eq!(mentioned.rows[0].title_or_empty, "Alpha memory");
    assert_eq!(mentioned.rows[0].content_or_empty, "Alpha content");
    assert_eq!(mentioned.rows[0].unit_type.as_deref(), Some("learning"));
    assert_eq!(
        mentioned.rows[0].metadata,
        Some(Value::String("{\"rank\":1}".to_string()))
    );
    assert!(mentioned.rows[0].is_latest);
    assert_eq!(mentioned.rows[0].lifecycle_state.as_deref(), Some("active"));
    assert_eq!(mentioned.rows[0].importance, Some(Value::Float(0.8)));
    assert_eq!(mentioned.rows[0].created_at, Some(Value::Int(30)));
    assert_eq!(mentioned.rows[0].is_crystal, Some(false));
    assert_eq!(mentioned.rows[0].pagerank_score, Some(Value::Float(0.2)));
    assert_eq!(mentioned.rows[1].mention_breadth, 1);
    assert!(!mentioned.rows[1].is_latest);
    assert_eq!(mentioned.rows[2].community_id, Value::Int(8));
    assert_eq!(mentioned.rows[2].is_crystal, None);

    let false_only = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Int(8)],
            source: KnowledgeCommunityMemorySource::MentionedEntities,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::FalseOnly,
            unit_types: Vec::new(),
            order: KnowledgeCommunityMemoryListOrder::EntityCountImportancePagerank,
            limit: 0,
        })
        .unwrap();
    assert_eq!(false_only.matched_row_count, 0);

    let direct = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Int(7), Value::Int(8)],
            source: KnowledgeCommunityMemorySource::DirectMemoryCommunity,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::NullOrFalse,
            unit_types: Vec::new(),
            order: KnowledgeCommunityMemoryListOrder::CommunityImportanceCreatedAt,
            limit: 0,
        })
        .unwrap();

    assert_eq!(
        direct
            .rows
            .iter()
            .map(|row| row.memory_id.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["direct_alpha", "direct_null"]
    );
    assert_eq!(
        direct.rows[0].source,
        KnowledgeCommunityMemoryRowSource::DirectMemoryCommunity
    );
    assert_eq!(direct.rows[0].mention_breadth, 0);
    assert!(direct.rows[0].entity_ids.is_empty());
    assert_eq!(direct.rows[0].content_or_empty, "Direct content");
    assert_eq!(direct.rows[1].community_id, Value::Int(8));
    assert!(direct.rows[1].is_latest);
}

#[test]
fn reads_community_memories_for_unit_type_filter_shape() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'type_entity_a', community_id: 17})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'type_entity_b', community_id: 18})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'type_memory_fact', title: 'Fact Memory', unit_type: 'fact', is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'type_memory_note', title: 'Note Memory', unit_type: 'note', is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'type_memory_decision', title: 'Decision Memory', unit_type: 'decision', is_crystal: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'type_memory_crystal', title: 'Crystal Memory', unit_type: 'fact', is_crystal: true})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'type_memory_fact'}), (e:Entity {id: 'type_entity_a'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'type_memory_note'}), (e:Entity {id: 'type_entity_a'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'type_memory_decision'}), (e:Entity {id: 'type_entity_a'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'type_memory_crystal'}), (e:Entity {id: 'type_entity_a'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'type_memory_note'}), (e:Entity {id: 'type_entity_b'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let graph_commit_epoch = db.store.commit_epoch();

    let output = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Int(17)],
            source: KnowledgeCommunityMemorySource::MentionedEntities,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::FalseOnly,
            unit_types: vec!["fact".to_string(), "note".to_string()],
            order: KnowledgeCommunityMemoryListOrder::EntityCountImportancePagerank,
            limit: 200,
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch, graph_commit_epoch);
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(output.matched_row_count, 2);
    assert_eq!(output.returned_count, 2);
    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| (
                row.memory_id.as_deref().unwrap(),
                row.title.as_deref().unwrap(),
                row.unit_type.as_deref().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("type_memory_fact", "Fact Memory", "fact"),
            ("type_memory_note", "Note Memory", "note")
        ]
    );
}

#[test]
fn community_memory_read_rejects_invalid_scope() {
    let db = Database::new();

    let empty_ids_error = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: Vec::new(),
            source: KnowledgeCommunityMemorySource::MentionedEntities,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::Any,
            unit_types: Vec::new(),
            order: KnowledgeCommunityMemoryListOrder::CommunityBreadthImportanceCreatedAt,
            limit: 0,
        })
        .unwrap_err();
    assert!(empty_ids_error
        .to_string()
        .contains("non-empty community ids"));

    let null_id_error = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Null],
            source: KnowledgeCommunityMemorySource::DirectMemoryCommunity,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::Any,
            unit_types: Vec::new(),
            order: KnowledgeCommunityMemoryListOrder::CommunityImportanceCreatedAt,
            limit: 0,
        })
        .unwrap_err();
    assert!(null_id_error.to_string().contains("non-null community ids"));

    let empty_unit_type_error = db
        .query_community_memories_via_cypher(&KnowledgeCommunityMemoryListRequest {
            community_ids: vec![Value::Int(1)],
            source: KnowledgeCommunityMemorySource::MentionedEntities,
            crystal_filter: KnowledgeCommunityMemoryCrystalFilter::Any,
            unit_types: vec![String::new()],
            order: KnowledgeCommunityMemoryListOrder::CommunityBreadthImportanceCreatedAt,
            limit: 0,
        })
        .unwrap_err();
    assert!(empty_unit_type_error
        .to_string()
        .contains("non-empty unit types"));
}

#[test]
fn community_memory_reads_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Entity {id: 'community-cache-entity', community_id: 21})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'community-cache-mentioned', title: 'Mentioned', unit_type: 'fact', is_crystal: false, importance: 0.7})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'community-cache-direct', title: 'Direct', unit_type: 'fact', community_id: 21, is_crystal: false, importance: 0.6})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'community-cache-mentioned'}), (e:Entity {id: 'community-cache-entity'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    let request = KnowledgeCommunityMemoryListRequest {
        community_ids: vec![Value::Int(21)],
        source: KnowledgeCommunityMemorySource::Both,
        crystal_filter: KnowledgeCommunityMemoryCrystalFilter::FalseOnly,
        unit_types: vec!["fact".to_string()],
        order: KnowledgeCommunityMemoryListOrder::CommunityBreadthImportanceCreatedAt,
        limit: 0,
    };

    let first = db.query_community_memories_via_cypher(&request).unwrap();
    let second = db.query_community_memories_via_cypher(&request).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.matched_row_count, 2);
    assert_eq!(first.returned_count, 2);
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| (row.memory_id.as_deref(), row.source))
            .collect::<Vec<_>>(),
        vec![
            (
                Some("community-cache-mentioned"),
                KnowledgeCommunityMemoryRowSource::MentionedEntities
            ),
            (
                Some("community-cache-direct"),
                KnowledgeCommunityMemoryRowSource::DirectMemoryCommunity
            )
        ]
    );
    let stats = db.plan_cache_stats();
    assert_eq!(stats.entries, 2);
    assert_eq!(stats.misses, 2);
    assert_eq!(stats.hits, 2);
}
