use super::*;

#[test]
fn count_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One', deleted_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Memory {id: 3, title: 'Three', deleted_at: 'now'})")
        .unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m.deleted_at) AS deleted")
        .unwrap();
    assert_eq!(output.rows[0].get("deleted"), Some(&Value::Int(1)));
}

#[test]
fn count_distinct_property_ignores_duplicate_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note', status: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task', status: 'open'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, kind: 'task', status: 'open'})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) RETURN count(DISTINCT m.kind) AS kinds, count(DISTINCT m.status) AS statuses",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("kinds"), Some(&Value::Int(2)));
    assert_eq!(output.rows[0].get("statuses"), Some(&Value::Int(1)));
}

#[test]
fn avg_property_ignores_missing_null_and_non_numeric_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, is_crystal: false, decay_score_cached: 0.2})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2, is_crystal: false, decay_score_cached: 1})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, is_crystal: false, decay_score_cached: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 4, is_crystal: false, decay_score_cached: 'skip'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 5, is_crystal: true, decay_score_cached: 10.0})")
        .unwrap();

    let output = db
        .query(
            "MATCH (m:Memory) WHERE m.is_crystal = false RETURN count(m) AS total, avg(m.decay_score_cached) AS avg_decay",
        )
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(4)));
    let Some(Value::Float(avg_decay)) = output.rows[0].get("avg_decay") else {
        panic!("expected floating point average");
    };
    assert!((avg_decay - 0.6).abs() < f64::EPSILON);

    let output = db
        .query("MATCH (m:Memory) WHERE m.is_crystal = true RETURN avg(m.missing) AS avg_decay")
        .unwrap();
    assert_eq!(output.rows[0].get("avg_decay"), Some(&Value::Null));
}

#[test]
fn max_property_ignores_missing_and_null_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, updated_at: 5})").unwrap();
    db.query("CREATE (:Memory {id: 2, updated_at: null})")
        .unwrap();
    db.query("CREATE (:Memory {id: 3, updated_at: 9})").unwrap();
    db.query("CREATE (:Memory {id: 4})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(m), max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(m)"), Some(&Value::Int(4)));
    assert_eq!(
        output.rows[0].get("max(m.updated_at)"),
        Some(&Value::Int(9))
    );

    let output = db
        .query("MATCH (m:Missing) RETURN max(m.updated_at)")
        .unwrap();
    assert_eq!(output.rows[0].get("max(m.updated_at)"), Some(&Value::Null));
}

#[test]
fn aggregate_order_by_alias_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total ORDER BY total DESC LIMIT 1")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn grouped_count_aggregates_by_projected_property() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 2, kind: 'note'})").unwrap();
    db.query("CREATE (:Memory {id: 3, kind: 'task'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN m.kind AS kind, count(*) AS total ORDER BY total DESC, kind ASC")
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));
}

#[test]
fn aggregate_with_can_count_its_grouped_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'one', thread_id: 'shared', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'two', thread_id: 'shared', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'three', thread_id: 'other', space_id: ''})")
        .unwrap();

    let output = db
        .query(
            "MATCH (t:Thread) WITH CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END AS space_id, t.thread_id AS thread_id, MAX(t.id) AS representative_id RETURN COUNT(*) AS total",
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn grouped_count_aggregates_relationship_matches() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 1, kind: 'note'})-[:MENTIONS {weight: 3}]->(:Entity {id: 10, name: 'Rust'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 2, kind: 'note'})-[:MENTIONS {weight: 1}]->(:Entity {id: 11, name: 'Kuzu'})",
        )
        .unwrap();
    db.query(
            "CREATE (:Memory {id: 3, kind: 'task'})-[:MENTIONS {weight: 5}]->(:Entity {id: 12, name: 'Neo4j'})",
        )
        .unwrap();

    let output = db
            .query(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.kind AS kind, count(e) AS total ORDER BY kind ASC",
            )
            .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("kind"),
        Some(&Value::String("note".to_string()))
    );
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
    assert_eq!(
        output.rows[1].get("kind"),
        Some(&Value::String("task".to_string()))
    );
    assert_eq!(output.rows[1].get("total"), Some(&Value::Int(1)));

    let output = db
        .query("MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(*), min(r.weight)")
        .unwrap();
    assert_eq!(output.rows[0].get("count(*)"), Some(&Value::Int(3)));
    assert_eq!(output.rows[0].get("min(r.weight)"), Some(&Value::Int(1)));
}

#[test]
fn with_collect_distinct_property_returns_source_ids() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'crystal-1', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-2'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'source-1'})").unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query("MATCH (c:Memory {id: 'crystal-1'}), (s:Memory {id: 'source-1'}) CREATE (c)-[:SYNTHESIZED_FROM]->(s)")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'crystal-2', is_crystal: true})-[:SYNTHESIZED_FROM]->(:Memory {id: 'source-3'})",
    )
    .unwrap();

    let output = db
        .query_with_params(
            "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids",
            &BTreeMap::from([(
                "ids".to_string(),
                Value::List(vec![Value::String("crystal-1".to_string())]),
            )]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("c.id"),
        Some(&Value::String("crystal-1".to_string()))
    );
    assert_eq!(
        output.rows[0].get("source_ids"),
        Some(&Value::List(vec![
            Value::String("source-1".to_string()),
            Value::String("source-2".to_string()),
        ]))
    );
}

#[test]
fn with_count_and_collect_preserves_group_node_for_ordering() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'entity-1', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-2', community_id: 42})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-1', title: 'First', importance: 0.8})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-2', title: 'Second', importance: 0.9})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-1'}), (e:Entity {id: 'entity-2'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-2'}), (e:Entity {id: 'entity-1'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (e:Entity {community_id: $community_id})<-[:MENTIONS]-(m:Memory) WITH m, COUNT(e) AS entity_count, COLLECT(DISTINCT e.id) AS entity_ids RETURN m, entity_count, entity_ids ORDER BY entity_count DESC, m.importance DESC LIMIT 1",
            &BTreeMap::from([("community_id".to_string(), Value::Int(42))]),
        )
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(memory)) = output.rows[0].get("m") else {
        panic!("expected grouped memory node map");
    };
    assert_eq!(
        memory.get("id"),
        Some(&Value::String("memory-1".to_string()))
    );
    assert_eq!(output.rows[0].get("entity_count"), Some(&Value::Int(3)));
    assert_eq!(
        output.rows[0].get("entity_ids"),
        Some(&Value::List(vec![
            Value::String("entity-1".to_string()),
            Value::String("entity-2".to_string()),
        ]))
    );
}

#[test]
fn counts_single_node_matches() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'One'})").unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'Two'})").unwrap();
    db.query("CREATE (:Entity {id: 10, name: 'Rust'})").unwrap();

    let output = db
        .query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let explain = db
        .explain_query("MATCH (m:Memory) RETURN count(*) AS total")
        .unwrap();
    assert!(explain.trace.selected_plan.contains("NodeCountExec"));
    assert!(!explain.trace.selected_plan.contains("AggregateExec"));
}

#[test]
fn counts_relationship_expansion_matches() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 1, title: 'One'})-[:MENTIONS]->(:Entity {id: 10, name: 'Rust'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 2, title: 'Two'})-[:MENTIONS]->(:Entity {id: 11, name: 'Kuzu'})",
    )
    .unwrap();

    let output = db
        .query("MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN count(e) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));
}

#[test]
fn counts_unfiltered_relationship_types_from_the_exact_count_store() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 2})-[:MENTIONS]->(:Entity {id: 11})")
        .unwrap();
    db.query("CREATE (:Entity {id: 10})-[:RELATES_TO]->(:Entity {id: 11})")
        .unwrap();

    let output = db
        .query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(2)));

    let explain = db
        .explain_query("MATCH ()-[r:MENTIONS]->() RETURN count(r) AS total")
        .unwrap();
    assert!(explain
        .trace
        .selected_plan
        .contains("RelationshipCountExec"));
    assert!(!explain.trace.selected_plan.contains("AdjacencyExpandExec"));
    assert!(!explain.trace.selected_plan.contains("AggregateExec"));
}

#[test]
fn filtered_node_count_does_not_hydrate_unreferenced_payloads() {
    let execution_memory = crate::executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(512).unwrap(),
        ..crate::executor::ExecutionMemoryConfig::default()
    };
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory,
        ..DatabaseConfig::default()
    });
    db.query(&format!(
        "CREATE (:Memory {{id: 'memory-1', content: '{}'}})",
        "x".repeat(64 * 1024)
    ))
    .unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    let cypher = "MATCH (m:Memory {id: $id}) RETURN count(m) AS total";
    let parameters = BTreeMap::from([("id".to_string(), Value::String("memory-1".to_string()))]);

    let explain = db.explain_query_with_params(cypher, &parameters).unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("AggregateExec"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));
    assert!(physical_plan.contains("output=node_binding"));
    assert!(physical_plan.contains("properties=[\"id\"]"));
    assert!(!physical_plan.contains("content"));

    let output = db.query_with_params(cypher, &parameters).unwrap();
    assert_eq!(output.rows[0].get("total"), Some(&Value::Int(1)));
}

#[test]
fn node_detail_neighbor_counts_use_distinct_neighbors_and_edges() {
    let mut db = Database::new();
    db.query("CREATE (:Entity {id: 'n1', name: 'One'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'n2', name: 'Two'})")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n2'}) CREATE (a)-[:RELATES_TO]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n2'}), (b:Entity {id: 'n1'}) CREATE (a)-[:MENTIONS]->(b)")
        .unwrap();
    db.query("MATCH (a:Entity {id: 'n1'}), (b:Entity {id: 'n1'}) CREATE (a)-[:SELF]->(b)")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (n)-[r]-(neighbor)
                 WHERE n.id = $node_id
                 RETURN COUNT(DISTINCT neighbor), COUNT(r)",
            &BTreeMap::from([("node_id".to_string(), Value::String("n1".into()))]),
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT neighbor)"),
        Some(&Value::Int(2))
    );
    assert_eq!(output.rows[0].get("count(r)"), Some(&Value::Int(3)));
}

#[test]
fn converging_relationship_pattern_counts_distinct_source_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'target'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-1'})").unwrap();
    db.query("CREATE (:Memory {id: 'other-2'})").unwrap();
    db.query("CREATE (:Entity {id: 'shared'})").unwrap();
    db.query(
        "MATCH (m:Memory {id: 'target'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-1'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'other-2'}), (e:Entity {id: 'shared'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN other.id AS id
                 ORDER BY id ASC",
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([("id".to_string(), Value::String("other-1".to_string()))]),
            BTreeMap::from([("id".to_string(), Value::String("other-2".to_string()))]),
        ]
    );

    let output = db
        .query(
            "MATCH (other:Memory)-[:MENTIONS]->(:Entity)<-[:MENTIONS]-(m:Memory {id: 'target'})
                 WHERE other.id <> 'target'
                 RETURN COUNT(DISTINCT other)",
        )
        .unwrap();

    assert_eq!(
        output.rows[0].get("count(DISTINCT other)"),
        Some(&Value::Int(2))
    );
}
