use super::*;

const LABEL_REGEX_MEMORY_CONNECTIONS_QUERY: &str = "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) \
     WHERE l.name =~ $pattern \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.title AS title, \
       m.importance AS importance, l.id AS label_id, id(l) AS label_node_id, \
       l.name AS label_name, count(r) AS label_connections \
     ORDER BY label_connections DESC, label_name ASC, memory_id ASC, \
       memory_node_id ASC, label_id ASC, label_node_id ASC \
     SKIP $offset LIMIT $limit";

#[test]
fn label_regex_memory_connections_use_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'memory-alpha', title: 'Alpha', importance: 0.9})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-beta', title: 'Beta', importance: 0.8})")
        .unwrap();
    db.query("CREATE (:Source {id: 'not-memory'})").unwrap();
    db.query("CREATE (:Label {id: 'label-alpha', name: 'regex-alpha'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label-beta', name: 'regex-beta'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-alpha'}), (l:Label {id: 'label-alpha'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-alpha'}), (l:Label {id: 'label-alpha'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-beta'}), (l:Label {id: 'label-beta'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();
    db.query("MATCH (s:Source {id: 'not-memory'}), (l:Label {id: 'label-alpha'}) CREATE (s)-[:HAS_LABEL]->(l)")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "pattern".to_string(),
            Value::String("^regex-(alpha|beta)$".to_string()),
        ),
        ("offset".to_string(), Value::Int(0)),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    db.query("CREATE (:Memory {id: 'late-memory'})").unwrap();
    db.query("MATCH (m:Memory {id: 'late-memory'}), (l:Label {id: 'label-alpha'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();

    let first = snapshot
        .query_with_params_bounded(LABEL_REGEX_MEMORY_CONNECTIONS_QUERY, &parameters, Some(2))
        .unwrap();
    let second = snapshot
        .query_with_params_bounded(LABEL_REGEX_MEMORY_CONNECTIONS_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("memory-alpha".to_string()))
    );
    assert_eq!(first.rows[0].get("label_connections"), Some(&Value::Int(2)));
    assert_eq!(first.rows[0].get("importance"), Some(&Value::Float(0.9)));
    assert_eq!(
        first.rows[1].get("memory_id"),
        Some(&Value::String("memory-beta".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&snapshot, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&snapshot, "hits"), 1);

    let budget_error = snapshot
        .query_with_params_bounded(LABEL_REGEX_MEMORY_CONNECTIONS_QUERY, &parameters, Some(1))
        .unwrap_err();
    assert!(
        budget_error
            .to_string()
            .contains("exceeding max_read_result_rows 1"),
        "{budget_error}"
    );

    let mut invalid_parameters = parameters.clone();
    invalid_parameters.insert("pattern".to_string(), Value::String("(".to_string()));
    let regex_error = snapshot
        .query_with_params_bounded(
            LABEL_REGEX_MEMORY_CONNECTIONS_QUERY,
            &invalid_parameters,
            Some(2),
        )
        .unwrap_err();
    assert!(regex_error.to_string().contains("regex"), "{regex_error}");
}

#[test]
fn label_regex_memory_connections_respect_query_offset() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-b'})").unwrap();
    db.query("CREATE (:Label {id: 'label', name: 'regex-label'})")
        .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-a'}), (l:Label {id: 'label'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    db.query(
        "MATCH (m:Memory {id: 'memory-b'}), (l:Label {id: 'label'}) CREATE (m)-[:HAS_LABEL]->(l)",
    )
    .unwrap();
    let parameters = BTreeMap::from([
        (
            "pattern".to_string(),
            Value::String("^regex-label$".to_string()),
        ),
        ("offset".to_string(), Value::Int(1)),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut snapshot = db.begin_read_transaction();

    let output = snapshot
        .query_with_params_bounded(LABEL_REGEX_MEMORY_CONNECTIONS_QUERY, &parameters, Some(1))
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
}
