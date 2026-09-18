use super::*;

const MEMORY_EVOLVES_LATEST_QUERY: &str = "MATCH (old:Memory)-[:EVOLVES]->(new:Memory) \
     WHERE old.id IN $old_memory_ids \
     RETURN DISTINCT new.id AS new_memory_id, id(new) AS new_node_id, \
       new.is_latest AS new_is_latest \
     ORDER BY new_memory_id ASC, new_is_latest ASC, new_node_id ASC LIMIT $limit";

#[test]
fn memory_evolves_latest_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'old-a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'old-b', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'new-a', is_latest: false})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'new-b', is_latest: true})")
        .unwrap();
    db.query("CREATE (:Source {id: 'not-memory-target'})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old-a'}), (new:Memory {id: 'new-b'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old-a'}), (new:Memory {id: 'new-b'}) CREATE (old)-[:EVOLVES {duplicate: true}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old-b'}), (new:Memory {id: 'new-a'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old-a'}), (source:Source {id: 'not-memory-target'}) CREATE (old)-[:EVOLVES]->(source)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "old_memory_ids".to_string(),
            Value::List(vec![
                Value::String("old-a".to_string()),
                Value::String("old-b".to_string()),
                Value::String("missing".to_string()),
            ]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'new-c', is_latest: true})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old-b'}), (new:Memory {id: 'new-c'}) CREATE (old)-[:EVOLVES]->(new)")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_LATEST_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_LATEST_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("new_memory_id"),
        Some(&Value::String("new-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("new_is_latest"),
        Some(&Value::Bool(false))
    );
    assert_eq!(
        first.rows[1].get("new_memory_id"),
        Some(&Value::String("new-b".to_string()))
    );
    assert_eq!(first.rows[1].get("new_is_latest"), Some(&Value::Bool(true)));
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_evolves_latest_respects_query_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'old'})").unwrap();
    db.query("CREATE (:Memory {id: 'new-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'new-b'})").unwrap();
    db.query(
        "MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-a'}) CREATE (old)-[:EVOLVES]->(new)",
    )
    .unwrap();
    db.query(
        "MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-b'}) CREATE (old)-[:EVOLVES]->(new)",
    )
    .unwrap();
    let parameters = BTreeMap::from([
        (
            "old_memory_ids".to_string(),
            Value::List(vec![Value::String("old".to_string())]),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_LATEST_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("new_memory_id"),
        Some(&Value::String("new-a".to_string()))
    );
}
