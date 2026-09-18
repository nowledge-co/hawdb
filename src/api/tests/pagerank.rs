use super::*;

#[test]
fn updates_and_clears_pagerank_scores_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_rank_2', title: 'Rank Two'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_rank_1', name: 'Entity One'})")
        .unwrap();
    db.query("CREATE (:Entity {name: 'Projected Entity', pagerank_score: 0.9})")
        .unwrap();

    let output = db
        .update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.42,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                    score: 0.84,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.99,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "missing".to_string(),
                    score: 0.1,
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.rows.len(), 4);
    assert_eq!(output.matched_count, 2);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.non_writable_count, 0);
    assert_eq!(output.updated_count, 2);
    assert!(output.rows[2].duplicate);
    assert!(!output.rows[3].matched);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                },
            ],
            property_names: vec!["pagerank_score".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("pagerank_score"),
        Some(&Some(Value::Float(0.42)))
    );
    assert_eq!(
        rows.rows[1].properties.get("pagerank_score"),
        Some(&Some(Value::Float(0.84)))
    );

    let clear = db
        .clear_knowledge_pagerank_scores(&KnowledgePageRankClearRequest {
            labels: vec!["Entity".to_string(), "Memory".to_string()],
        })
        .unwrap();
    assert_eq!(clear.graph_commit_epoch_before, 5);
    assert_eq!(clear.graph_commit_epoch_after, 6);
    assert_eq!(clear.candidate_count, 3);
    assert_eq!(clear.cleared_count, 2);
    assert_eq!(clear.non_writable_count, 1);
    assert_eq!(clear.rows.iter().filter(|row| row.cleared).count(), 2);
    assert_eq!(clear.rows.iter().filter(|row| row.non_writable).count(), 1);

    let rows = db
        .query_property_batch_via_cypher(&KnowledgePropertyBatchRequest {
            entities: vec![
                KnowledgeEntityRequest {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                },
                KnowledgeEntityRequest {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                },
            ],
            property_names: vec!["pagerank_score".to_string()],
        })
        .unwrap();
    assert_eq!(
        rows.rows[0].properties.get("pagerank_score"),
        Some(&Some(Value::Null))
    );
    assert_eq!(
        rows.rows[1].properties.get("pagerank_score"),
        Some(&Some(Value::Null))
    );
    let still_scored = db
        .query("MATCH (e:Entity) WHERE e.pagerank_score IS NOT NULL RETURN COUNT(e) AS total")
        .unwrap();
    assert_eq!(still_scored.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn pagerank_score_batch_rejects_invalid_score_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![KnowledgePageRankScoreUpdate {
                label: "Memory".to_string(),
                external_id: "memory_rank_1".to_string(),
                score: f64::NAN,
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("finite non-negative score"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_pagerank_score_batch_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_pagerank_score_batch_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_rank_1', title: 'Rank One'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'entity_rank_1', name: 'Entity One'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_pagerank_scores_batch(&KnowledgePageRankScoreBatchRequest {
            updates: vec![
                KnowledgePageRankScoreUpdate {
                    label: "Memory".to_string(),
                    external_id: "memory_rank_1".to_string(),
                    score: 0.42,
                },
                KnowledgePageRankScoreUpdate {
                    label: "Entity".to_string(),
                    external_id: "entity_rank_1".to_string(),
                    score: 0.84,
                },
            ],
        })
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
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
                        external_id: "memory_rank_1".to_string(),
                    },
                    KnowledgeEntityRequest {
                        label: "Entity".to_string(),
                        external_id: "entity_rank_1".to_string(),
                    },
                ],
                property_names: vec!["pagerank_score".to_string()],
            })
            .unwrap();
        assert_eq!(
            rows.rows[0].properties.get("pagerank_score"),
            Some(&Some(Value::Float(0.42)))
        );
        assert_eq!(
            rows.rows[1].properties.get("pagerank_score"),
            Some(&Some(Value::Float(0.84)))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

const PAGERANK_BASE_COUNT_QUERIES: [&str; 5] = [
    "MATCH (m:Memory) RETURN count(m) AS total",
    "MATCH (e:Entity) RETURN count(e) AS total",
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total",
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total",
    "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' RETURN count(r) AS total",
];

const PAGERANK_CHANGED_COUNT_QUERIES: [&str; 5] = [
    "MATCH (m:Memory) WHERE m.created_at > $cutoff OR m.updated_at > $cutoff RETURN count(m) AS total",
    "MATCH (e:Entity) WHERE e.created_at > $cutoff OR e.updated_at > $cutoff RETURN count(e) AS total",
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total",
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total",
    "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' AND (r.created_at > $cutoff OR r.updated_at > $cutoff) RETURN count(r) AS total",
];

const PAGERANK_ENTITY_MEMBERSHIP_QUERY: &str = "MATCH (e:Entity) WHERE e.id IN $external_ids \
     RETURN id(e) AS node_id, e.id AS external_id ORDER BY external_id";

const PAGERANK_MEMORY_VISIBILITY_QUERY: &str = "MATCH (m:Memory) WHERE m.id IN $memory_ids \
     RETURN id(m) AS node_id, m.id AS memory_id, m.metadata AS metadata, \
     COALESCE(m.is_latest, true) AS is_latest ORDER BY memory_id";

const PAGERANK_CENTRAL_ENTITY_QUERY: &str = "MATCH (e:Entity) WHERE e.id = $entity_id \
     RETURN id(e) AS node_id, e.name AS name LIMIT 1";

fn pagerank_count(
    read: &mut DatabaseReadTransaction,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> i64 {
    let output = read
        .query_with_params_bounded(query, parameters, Some(1))
        .unwrap();
    let Some(Value::Int(total)) = output.rows[0].get("total") else {
        panic!("expected integer pagerank count");
    };
    *total
}

fn pagerank_counts(read: &mut DatabaseReadTransaction, cutoff: i64) -> Vec<i64> {
    let empty = BTreeMap::new();
    let changed = BTreeMap::from([("cutoff".to_string(), Value::Int(cutoff))]);
    let mut counts = Vec::with_capacity(10);
    for query in PAGERANK_BASE_COUNT_QUERIES {
        counts.push(pagerank_count(read, query, &empty));
    }
    for query in PAGERANK_CHANGED_COUNT_QUERIES {
        counts.push(pagerank_count(read, query, &changed));
    }
    counts
}

#[test]
fn pagerank_plan_reads_use_parameterized_queries_on_one_snapshot() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'm1', created_at: 10, updated_at: 20, metadata: '{\"visible\":true}'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'm2', created_at: 120, updated_at: 130})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e1', name: 'Entity One', created_at: 15, updated_at: 25})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'e2', name: 'Entity Two', created_at: 140, updated_at: 150})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm1'}), (e:Entity {id: 'e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'm2'}), (e:Entity {id: 'e2'}) CREATE (m)-[:MENTIONS {created_at: 160}]->(e)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'e1'}), (b:Entity {id: 'e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'm1'}), (b:Memory {id: 'm2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'm2'}), (b:Memory {id: 'm1'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'inactive', created_at: 190}]->(b)")
        .unwrap();

    let mut read = db.begin_read_transaction();
    let snapshot_epoch = read.commit_epoch();
    db.query("CREATE (:Memory {id: 'after-snapshot', created_at: 200})")
        .unwrap();

    assert_eq!(
        pagerank_counts(&mut read, 100),
        vec![2, 2, 1, 2, 1, 1, 1, 1, 1, 1]
    );
    assert_eq!(read.commit_epoch(), snapshot_epoch);
    assert!(db.commit_epoch() > snapshot_epoch);
}

#[test]
fn pagerank_plan_queries_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(32),
        statement_summary_capacity: 32,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory-one', created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory-two', updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity-one', created_at: 30})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity-two', updated_at: 40})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'pagerank-cache-memory-one'}), (e:Entity {id: 'pagerank-cache-entity-one'}) CREATE (m)-[:MENTIONS {created_at: 50}]->(e)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'pagerank-cache-entity-one'}), (b:Entity {id: 'pagerank-cache-entity-two'}) CREATE (a)-[:RELATES_TO {updated_at: 60}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'pagerank-cache-memory-one'}), (b:Memory {id: 'pagerank-cache-memory-two'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 70}]->(b)")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let first = pagerank_counts(&mut read, 25);
    let second = pagerank_counts(&mut read, 25);
    assert_eq!(first, second);
    assert_eq!(first[0], 2);
    assert_eq!(first[1], 2);
    assert_eq!(first[3], 1);
    assert_eq!(first[4], 1);
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 10);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 10);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 10);
}

#[test]
fn pagerank_lookup_business_logic_uses_parameterized_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', metadata: '{\"space\":\"default\"}', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'm2'})").unwrap();
    db.query("CREATE (:Entity {id: 'e1', name: 'Central Entity'})")
        .unwrap();
    let graph_commit_epoch = db.commit_epoch();
    let mut read = db.begin_read_transaction();
    let membership_parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(vec![
            Value::String("e1".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let membership = read
        .query_with_params_bounded(
            PAGERANK_ENTITY_MEMBERSHIP_QUERY,
            &membership_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(read.commit_epoch(), graph_commit_epoch);
    assert_eq!(membership.rows.len(), 1);
    assert_eq!(
        membership.rows[0].get("external_id"),
        Some(&Value::String("e1".to_string()))
    );

    let visibility_parameters = BTreeMap::from([(
        "memory_ids".to_string(),
        Value::List(vec![
            Value::String("m1".to_string()),
            Value::String("m2".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let visibility = read
        .query_with_params_bounded(
            PAGERANK_MEMORY_VISIBILITY_QUERY,
            &visibility_parameters,
            Some(3),
        )
        .unwrap();
    assert_eq!(visibility.rows.len(), 2);
    assert_eq!(
        visibility.rows[0].get("metadata"),
        Some(&Value::String("{\"space\":\"default\"}".to_string()))
    );
    assert_eq!(
        visibility.rows[0].get("is_latest"),
        Some(&Value::Bool(false))
    );
    assert_eq!(
        visibility.rows[1].get("is_latest"),
        Some(&Value::Bool(true))
    );

    let central_parameters =
        BTreeMap::from([("entity_id".to_string(), Value::String("e1".to_string()))]);
    let central = read
        .query_with_params_bounded(PAGERANK_CENTRAL_ENTITY_QUERY, &central_parameters, Some(1))
        .unwrap();
    assert_eq!(central.rows.len(), 1);
    assert_eq!(
        central.rows[0].get("name"),
        Some(&Value::String("Central Entity".to_string()))
    );
    assert_eq!(read.commit_epoch(), graph_commit_epoch);
}

#[test]
fn pagerank_lookup_queries_use_query_runtime_plan_cache() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'pagerank-cache-memory', metadata: '{\"space\":\"default\"}', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'pagerank-cache-entity', name: 'Cache Entity'})")
        .unwrap();

    let membership_parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(vec![
            Value::String("pagerank-cache-entity".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let visibility_parameters = BTreeMap::from([(
        "memory_ids".to_string(),
        Value::List(vec![
            Value::String("pagerank-cache-memory".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let central_parameters = BTreeMap::from([(
        "entity_id".to_string(),
        Value::String("pagerank-cache-entity".to_string()),
    )]);
    let mut read = db.begin_read_transaction();
    for _ in 0..2 {
        read.query_with_params_bounded(
            PAGERANK_ENTITY_MEMBERSHIP_QUERY,
            &membership_parameters,
            Some(2),
        )
        .unwrap();
        read.query_with_params_bounded(
            PAGERANK_MEMORY_VISIBILITY_QUERY,
            &visibility_parameters,
            Some(2),
        )
        .unwrap();
        read.query_with_params_bounded(PAGERANK_CENTRAL_ENTITY_QUERY, &central_parameters, Some(1))
            .unwrap();
    }

    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 3);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 3);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 3);
}

#[test]
fn pagerank_read_queries_keep_user_values_in_parameters() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'e1', name: 'Safe'})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(vec![Value::String(
            "e1') MATCH (n) RETURN n //".to_string(),
        )]),
    )]);
    let output = read
        .query_with_params_bounded(PAGERANK_ENTITY_MEMBERSHIP_QUERY, &parameters, Some(1))
        .unwrap();
    assert!(output.rows.is_empty());

    let empty = BTreeMap::from([("external_ids".to_string(), Value::List(Vec::new()))]);
    let output = read
        .query_with_params_bounded(PAGERANK_ENTITY_MEMBERSHIP_QUERY, &empty, Some(1))
        .unwrap();
    assert!(output.rows.is_empty());
}
