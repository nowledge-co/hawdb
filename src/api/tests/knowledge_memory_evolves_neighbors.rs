use super::*;

const MEMORY_EVOLVES_OUTGOING_QUERY: &str =
    "MATCH (a:Memory {id: $memory_id})-[r:EVOLVES]->(n:Memory) \
     RETURN a.id AS anchor_memory_id, id(a) AS anchor_node_id, \
       n.id AS neighbor_memory_id, id(n) AS neighbor_node_id, \
       n.title AS neighbor_title, n.is_latest AS neighbor_is_latest, \
       n.extra_status AS neighbor_extra_status, id(r) AS relationship_id, \
       r.content_relation AS content_relation, r.confidence AS confidence, \
       r.reviewed AS reviewed, r.reason AS reason \
     ORDER BY neighbor_memory_id ASC, neighbor_node_id ASC, relationship_id ASC \
     LIMIT $limit";

const MEMORY_EVOLVES_INCOMING_QUERY: &str =
    "MATCH (n:Memory)-[r:EVOLVES]->(a:Memory {id: $memory_id}) \
     RETURN a.id AS anchor_memory_id, id(a) AS anchor_node_id, \
       n.id AS neighbor_memory_id, id(n) AS neighbor_node_id, \
       n.title AS neighbor_title, n.is_latest AS neighbor_is_latest, \
       n.extra_status AS neighbor_extra_status, id(r) AS relationship_id, \
       r.content_relation AS content_relation, r.confidence AS confidence, \
       r.reviewed AS reviewed, r.reason AS reason \
     ORDER BY neighbor_memory_id ASC, neighbor_node_id ASC, relationship_id ASC \
     LIMIT $limit";

#[test]
fn memory_evolves_neighbors_use_fixed_directional_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'source', title: 'Source', is_latest: false})")
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'target', title: 'Target', is_latest: true, extra_status: 'ready'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'other', title: 'Other', is_latest: true})")
        .unwrap();
    db.query("CREATE (:Source {id: 'not-memory'})").unwrap();
    db.query("MATCH (a:Memory {id: 'source'}), (b:Memory {id: 'target'}) CREATE (a)-[:EVOLVES {content_relation: 'supersedes', confidence: 0.82, reviewed: true, reason: 'better evidence'}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'other'}), (b:Memory {id: 'target'}) CREATE (a)-[:EVOLVES {content_relation: 'confirms', confidence: 0.64, reviewed: false}]->(b)")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'source'}), (s:Source {id: 'not-memory'}) CREATE (a)-[:EVOLVES {content_relation: 'ignored'}]->(s)")
        .unwrap();
    let outgoing_parameters = BTreeMap::from([
        ("memory_id".to_string(), Value::String("source".to_string())),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let incoming_parameters = BTreeMap::from([
        ("memory_id".to_string(), Value::String("target".to_string())),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late', title: 'Late'})")
        .unwrap();
    db.query("MATCH (a:Memory {id: 'late'}), (b:Memory {id: 'target'}) CREATE (a)-[:EVOLVES {content_relation: 'late'}]->(b)")
        .unwrap();

    let outgoing = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_OUTGOING_QUERY, &outgoing_parameters, Some(1))
        .unwrap();
    let cached_outgoing = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_OUTGOING_QUERY, &outgoing_parameters, Some(1))
        .unwrap();
    assert_eq!(cached_outgoing, outgoing);
    assert_eq!(outgoing.rows.len(), 1);
    assert_eq!(
        outgoing.rows[0].get("neighbor_memory_id"),
        Some(&Value::String("target".to_string()))
    );
    assert_eq!(
        outgoing.rows[0].get("neighbor_extra_status"),
        Some(&Value::String("ready".to_string()))
    );
    assert_eq!(
        outgoing.rows[0].get("content_relation"),
        Some(&Value::String("supersedes".to_string()))
    );
    assert_eq!(
        outgoing.rows[0].get("confidence"),
        Some(&Value::Float(0.82))
    );
    assert_eq!(outgoing.rows[0].get("reviewed"), Some(&Value::Bool(true)));

    let incoming = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_INCOMING_QUERY, &incoming_parameters, Some(2))
        .unwrap();
    assert_eq!(incoming.rows.len(), 2);
    assert_eq!(
        incoming.rows[0].get("neighbor_memory_id"),
        Some(&Value::String("other".to_string()))
    );
    assert_eq!(
        incoming.rows[1].get("neighbor_memory_id"),
        Some(&Value::String("source".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);
}

#[test]
fn memory_evolves_neighbors_missing_anchor_returns_no_rows() {
    let db = Database::new();
    let parameters = BTreeMap::from([
        (
            "memory_id".to_string(),
            Value::String("missing".to_string()),
        ),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(MEMORY_EVOLVES_OUTGOING_QUERY, &parameters, Some(1))
        .unwrap();

    assert!(output.rows.is_empty());
}
