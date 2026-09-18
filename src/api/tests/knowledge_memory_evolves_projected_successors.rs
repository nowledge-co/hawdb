use super::*;

const MEMORY_EVOLVES_SUCCESSORS_STABLE_QUERY: &str =
    "MATCH (old:Memory {id: $old_memory_id})-[r:EVOLVES]->(new:Memory) \
     RETURN old.id AS old_memory_id, id(old) AS old_node_id, \
       new.id AS new_memory_id, id(new) AS new_node_id, new.title AS title, \
       new.is_latest AS is_latest, new.future_memory_field AS future_memory_field, \
       new.updated_at AS updated_at, id(r) AS relationship_id, \
       r.content_relation AS content_relation, r.future_edge_field AS future_edge_field \
     ORDER BY new_memory_id ASC, new_node_id ASC, relationship_id ASC \
     SKIP $offset LIMIT $limit";

const MEMORY_EVOLVES_SUCCESSORS_UPDATED_QUERY: &str =
    "MATCH (old:Memory {id: $old_memory_id})-[r:EVOLVES]->(new:Memory) \
     RETURN old.id AS old_memory_id, id(old) AS old_node_id, \
       new.id AS new_memory_id, id(new) AS new_node_id, new.title AS title, \
       new.is_latest AS is_latest, new.future_memory_field AS future_memory_field, \
       new.updated_at AS updated_at, id(r) AS relationship_id, \
       r.content_relation AS content_relation, r.future_edge_field AS future_edge_field \
     ORDER BY updated_at DESC, new_memory_id ASC, new_node_id ASC, relationship_id ASC \
     SKIP $offset LIMIT $limit";

fn successor_parameters(old_memory_id: &str, offset: i64, limit: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "old_memory_id".to_string(),
            Value::String(old_memory_id.to_string()),
        ),
        ("offset".to_string(), Value::Int(offset)),
        ("limit".to_string(), Value::Int(limit)),
    ])
}

#[test]
fn memory_evolves_successors_use_one_fixed_bounded_query_per_parent() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'old'})").unwrap();
    db.query("CREATE (:Memory {id: 'new-a', title: 'A', is_latest: true, future_memory_field: 'new-a', updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'new-b', title: 'B', is_latest: false, future_memory_field: 'new-b', updated_at: 30})")
        .unwrap();
    db.query("CREATE (:Source {id: 'not-memory'})").unwrap();
    db.query("MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-b'}) CREATE (old)-[:EVOLVES {content_relation: 'supersedes', future_edge_field: 'edge-b'}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-a'}) CREATE (old)-[:EVOLVES {content_relation: 'replaces', future_edge_field: 'edge-a'}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-a'}) CREATE (old)-[:EVOLVES {content_relation: 'duplicate', future_edge_field: 'edge-a-dup'}]->(new)")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old'}), (source:Source {id: 'not-memory'}) CREATE (old)-[:EVOLVES {content_relation: 'ignored'}]->(source)")
        .unwrap();
    let parameters = successor_parameters("old", 0, 2);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'new-c', title: 'C', updated_at: 40})")
        .unwrap();
    db.query("MATCH (old:Memory {id: 'old'}), (new:Memory {id: 'new-c'}) CREATE (old)-[:EVOLVES {content_relation: 'late'}]->(new)")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_SUCCESSORS_STABLE_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_SUCCESSORS_STABLE_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("new_memory_id"),
        Some(&Value::String("new-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("future_memory_field"),
        Some(&Value::String("new-a".to_string()))
    );
    assert_eq!(
        first.rows[0].get("future_edge_field"),
        Some(&Value::String("edge-a".to_string()))
    );
    assert_eq!(
        first.rows[1].get("new_memory_id"),
        Some(&Value::String("new-a".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_evolves_successors_updated_order_pages_with_fixed_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'old'})").unwrap();
    for (memory_id, updated_at) in [("new-a", 10), ("new-b", 30), ("new-c", 20)] {
        db.query(&format!(
            "CREATE (:Memory {{id: '{memory_id}', updated_at: {updated_at}}})"
        ))
        .unwrap();
        db.query(&format!(
            "MATCH (old:Memory {{id: 'old'}}), (new:Memory {{id: '{memory_id}'}}) CREATE (old)-[:EVOLVES]->(new)"
        ))
        .unwrap();
    }
    let first_parameters = successor_parameters("old", 0, 1);
    let second_parameters = successor_parameters("old", 1, 2);
    let mut snapshot = db.begin_read_transaction();

    let first = snapshot
        .query_with_params_bounded(
            MEMORY_EVOLVES_SUCCESSORS_UPDATED_QUERY,
            &first_parameters,
            Some(1),
        )
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(
            MEMORY_EVOLVES_SUCCESSORS_UPDATED_QUERY,
            &second_parameters,
            Some(2),
        )
        .unwrap();

    assert_eq!(
        first.rows[0].get("new_memory_id"),
        Some(&Value::String("new-b".to_string()))
    );
    assert_eq!(
        second
            .rows
            .iter()
            .map(|row| row.get("new_memory_id").cloned())
            .collect::<Vec<_>>(),
        vec![
            Some(Value::String("new-c".to_string())),
            Some(Value::String("new-a".to_string())),
        ]
    );
}
