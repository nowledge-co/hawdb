use super::*;

const SOURCE_EXACT_QUERY: &str = "MATCH (s:Source) WHERE s.id = $source_id \
     RETURN s.id AS source_id, id(s) AS node_id, s.original_name AS original_name, \
     s.title AS title, s.lifecycle_state AS lifecycle_state, \
     COALESCE(s.space_id, 'default') AS normalized_space_id LIMIT 1";

const SOURCE_COUNT_QUERY: &str = "MATCH (s:Source) \
     WHERE s.lifecycle_state = $lifecycle_state \
     RETURN count(s) AS matched_count";

const SOURCE_PAGE_QUERY: &str = "MATCH (s:Source) \
     WHERE s.lifecycle_state = $lifecycle_state \
     RETURN s.id AS source_id, id(s) AS node_id, s.title AS title, \
     COALESCE(s.space_id, 'default') AS normalized_space_id \
     ORDER BY source_id ASC LIMIT $limit";

const SOURCE_IDS_QUERY: &str = "MATCH (s:Source) \
     WHERE s.lifecycle_state = $lifecycle_state \
     RETURN s.id AS source_id ORDER BY source_id ASC LIMIT $limit";

const SOURCE_MEMORY_COUNT_QUERY: &str =
    "MATCH (m:Memory)-[:SOURCED_FROM]->(s:Source) WHERE s.id = $source_id \
     RETURN count(DISTINCT m) AS matched_count";

const SOURCE_MEMORY_PAGE_QUERY: &str =
    "MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source) WHERE s.id = $source_id \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, id(r) AS relationship_id, \
     m.title AS title, m.space_id AS raw_space_id, r.chunk_index AS chunk_index \
     ORDER BY chunk_index ASC, memory_id ASC LIMIT $limit";

const MEMORY_SOURCE_ATTRIBUTIONS_QUERY: &str =
    "MATCH (m:Memory)-[r:SOURCED_FROM]->(s:Source) WHERE m.id IN $memory_ids \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, s.id AS source_id, \
     id(s) AS source_node_id, id(r) AS relationship_id, \
     r.chunk_index AS chunk_index, s.title AS source_title \
     ORDER BY memory_id ASC, source_id ASC, relationship_id ASC";

#[test]
fn source_detail_count_page_and_ids_use_fixed_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Source {id: 'source-a', original_name: 'A', title: 'Alpha', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-b', original_name: 'B', title: 'Beta', lifecycle_state: 'active', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Source {id: 'source-c', lifecycle_state: 'deleted'})")
        .unwrap();
    let mut read = db.begin_read_transaction();

    let exact_parameters = BTreeMap::from([(
        "source_id".to_string(),
        Value::String("source-a".to_string()),
    )]);
    let exact = read
        .query_with_params_bounded(SOURCE_EXACT_QUERY, &exact_parameters, Some(1))
        .unwrap();
    assert_eq!(exact.rows.len(), 1);
    assert_eq!(
        exact.rows[0].get("normalized_space_id"),
        Some(&Value::String("default".to_string()))
    );

    let count_parameters = BTreeMap::from([(
        "lifecycle_state".to_string(),
        Value::String("active".to_string()),
    )]);
    let count = read
        .query_with_params_bounded(SOURCE_COUNT_QUERY, &count_parameters, Some(1))
        .unwrap();
    assert_eq!(count.rows[0].get("matched_count"), Some(&Value::Int(2)));

    let mut page_parameters = count_parameters.clone();
    page_parameters.insert("limit".to_string(), Value::Int(2));
    let first = read
        .query_with_params_bounded(SOURCE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(SOURCE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    let ids = read
        .query_with_params_bounded(SOURCE_IDS_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(ids.rows.len(), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 4);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 4);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn source_memory_reads_use_named_count_and_page_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source-a', title: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-a', title: 'A'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-b', title: 'B', space_id: 'team'})")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-a'}), (s:Source {id: 'source-a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 2}]->(s)")
        .unwrap();
    db.query("MATCH (m:Memory {id: 'memory-b'}), (s:Source {id: 'source-a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 1}]->(s)")
        .unwrap();
    let parameters = BTreeMap::from([(
        "source_id".to_string(),
        Value::String("source-a".to_string()),
    )]);
    let mut read = db.begin_read_transaction();

    let count = read
        .query_with_params_bounded(SOURCE_MEMORY_COUNT_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(count.rows[0].get("matched_count"), Some(&Value::Int(2)));
    let mut page_parameters = parameters;
    page_parameters.insert("limit".to_string(), Value::Int(2));
    let page = read
        .query_with_params_bounded(SOURCE_MEMORY_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert_eq!(
        page.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
}

#[test]
fn memory_source_attributions_are_bounded_by_input_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Source {id: 'source-a', title: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory-a'}), (s:Source {id: 'source-a'}) CREATE (m)-[:SOURCED_FROM {chunk_index: 1}]->(s)")
        .unwrap();
    let parameters = BTreeMap::from([(
        "memory_ids".to_string(),
        Value::List(vec![
            Value::String("memory-a".to_string()),
            Value::String("missing".to_string()),
        ]),
    )]);
    let mut read = db.begin_read_transaction();

    let output = read
        .query_with_params_bounded(MEMORY_SOURCE_ATTRIBUTIONS_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("source_id"),
        Some(&Value::String("source-a".to_string()))
    );
}
