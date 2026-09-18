use super::*;

const MEMORY_RELATED_ENTITY_NAMES_QUERY: &str = "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
     WHERE m.id IN $memory_ids AND e.name IS NOT NULL AND e.name <> '' \
     RETURN DISTINCT e.name AS name ORDER BY name ASC LIMIT $limit";

const THREAD_RELATED_ENTITY_NAMES_QUERY: &str =
    "MATCH (t:Thread {id: $thread_id})-[:COMPACTS_TO]->(m:Memory)-[:MENTIONS]->(e:Entity) \
     WHERE e.name IS NOT NULL AND e.name <> '' \
     RETURN DISTINCT e.name AS name ORDER BY name ASC LIMIT $limit";

#[test]
fn related_entity_names_use_fixed_memory_and_thread_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-b'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity-a', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-b', name: 'Beta'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-a'}), (e:Entity {id: 'entity-a'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-b'}), (e:Entity {id: 'entity-b'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread'})").unwrap();
    db.query("MATCH (t:Thread {id: 'thread'}), (m:Memory {id: 'memory-b'}) CREATE (t)-[:COMPACTS_TO]->(m)")
        .unwrap();
    let memory_parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![
                Value::String("memory-a".to_string()),
                Value::String("memory-b".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let thread_parameters = BTreeMap::from([
        ("thread_id".to_string(), Value::String("thread".to_string())),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let memory_names = snapshot
        .query_with_params_bounded(
            MEMORY_RELATED_ENTITY_NAMES_QUERY,
            &memory_parameters,
            Some(2),
        )
        .unwrap();
    let cached_memory_names = snapshot
        .query_with_params_bounded(
            MEMORY_RELATED_ENTITY_NAMES_QUERY,
            &memory_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(cached_memory_names, memory_names);
    assert_eq!(
        memory_names
            .rows
            .iter()
            .map(|row| row.get("name").cloned())
            .collect::<Vec<_>>(),
        vec![
            Some(Value::String("Alpha".to_string())),
            Some(Value::String("Beta".to_string())),
        ]
    );

    let thread_names = snapshot
        .query_with_params_bounded(
            THREAD_RELATED_ENTITY_NAMES_QUERY,
            &thread_parameters,
            Some(1),
        )
        .unwrap();
    assert_eq!(thread_names.rows.len(), 1);
    assert_eq!(
        thread_names.rows[0].get("name"),
        Some(&Value::String("Beta".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn related_entity_names_respect_query_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity-a', name: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity-b', name: 'Beta'})")
        .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (e:Entity {id: 'entity-a'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory'}), (e:Entity {id: 'entity-b'}) CREATE (m)-[:MENTIONS]->(e)",
    )
    .unwrap();
    let parameters = BTreeMap::from([
        (
            "memory_ids".to_string(),
            Value::List(vec![Value::String("memory".to_string())]),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_RELATED_ENTITY_NAMES_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Alpha".to_string()))
    );
}
