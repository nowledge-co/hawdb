use super::*;

const THREAD_BY_LOGICAL_ID_QUERY: &str = "MATCH (t:Thread {thread_id: $thread_id}) \
     RETURN id(t) AS thread_node_id, t.id AS physical_thread_id LIMIT 1";

const THREAD_COMPACTED_MEMORY_PAGE_QUERY: &str =
    "MATCH (t:Thread)-[r:COMPACTS_TO]->(m:Memory) WHERE id(t) = $thread_node_id \
     RETURN t.id AS thread_id, t.thread_id AS thread_logical_id, \
       m.id AS memory_id, id(m) AS memory_node_id, id(r) AS relationship_id, \
       m.title AS title, m.content AS content, m.importance AS importance, \
       m.created_at AS created_at, m.metadata AS metadata, \
       r.compaction_method AS compaction_method, r.created_at AS relationship_created_at \
     ORDER BY importance DESC, created_at DESC, memory_id ASC, relationship_id ASC \
     LIMIT $limit";

const MEMORY_BY_ID_QUERY: &str =
    "MATCH (m:Memory {id: $memory_id}) RETURN id(m) AS memory_node_id LIMIT 1";

const MEMORY_COMPACTING_THREAD_PAGE_QUERY: &str =
    "MATCH (t:Thread)-[r:COMPACTS_TO]->(m:Memory) WHERE id(m) = $memory_node_id \
     RETURN m.id AS memory_id, t.id AS thread_id, id(t) AS thread_node_id, \
       t.thread_id AS thread_logical_id, t.title AS title, t.source AS source, \
       t.metadata AS metadata, t.space_id AS space_id, \
       id(r) AS relationship_id, r.compaction_method AS compaction_method \
     ORDER BY thread_logical_id ASC, thread_id ASC, relationship_id ASC LIMIT $limit";

#[test]
fn thread_compacted_memories_use_fixed_snapshot_pinned_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-a', title: 'Alpha', content: 'A', importance: 0.8, created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-b', title: 'Beta', content: 'B', importance: 0.9, created_at: 20})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Memory {id: 'memory-a'}) CREATE (t)-[:COMPACTS_TO {compaction_method: 'manual', created_at: 30}]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Memory {id: 'memory-b'}) CREATE (t)-[:COMPACTS_TO {compaction_method: 'automatic', created_at: 40}]->(m)")
        .unwrap();
    let identity_parameters = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String("logical-a".to_string()),
    )]);
    let mut read = db.begin_read_transaction();
    let identity = read
        .query_with_params_bounded(THREAD_BY_LOGICAL_ID_QUERY, &identity_parameters, Some(1))
        .unwrap();
    let thread_node_id = identity.rows[0].get("thread_node_id").unwrap().clone();
    let page_parameters = BTreeMap::from([
        ("thread_node_id".to_string(), thread_node_id),
        ("limit".to_string(), Value::Int(2)),
    ]);
    db.query("MATCH (m:Memory {id: 'memory-b'}) SET m.title = 'Changed'")
        .unwrap();

    let first = read
        .query_with_params_bounded(
            THREAD_COMPACTED_MEMORY_PAGE_QUERY,
            &page_parameters,
            Some(2),
        )
        .unwrap();
    let second = read
        .query_with_params_bounded(
            THREAD_COMPACTED_MEMORY_PAGE_QUERY,
            &page_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
    assert_eq!(
        first.rows[0].get("title"),
        Some(&Value::String("Beta".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn memory_compacting_threads_use_per_memory_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', title: 'Alpha', source: 'codex', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-b', thread_id: 'logical-b', title: 'Beta', source: 'slack', space_id: 'team'})")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Memory {id: 'memory-a'}) CREATE (t)-[:COMPACTS_TO {compaction_method: 'manual'}]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-b'}), (m:Memory {id: 'memory-a'}) CREATE (t)-[:COMPACTS_TO {compaction_method: 'automatic'}]->(m)")
        .unwrap();
    let identity_parameters = BTreeMap::from([(
        "memory_id".to_string(),
        Value::String("memory-a".to_string()),
    )]);
    let mut read = db.begin_read_transaction();
    let identity = read
        .query_with_params_bounded(MEMORY_BY_ID_QUERY, &identity_parameters, Some(1))
        .unwrap();
    let memory_node_id = identity.rows[0].get("memory_node_id").unwrap().clone();
    let page_parameters = BTreeMap::from([
        ("memory_node_id".to_string(), memory_node_id),
        ("limit".to_string(), Value::Int(2)),
    ]);

    let output = read
        .query_with_params_bounded(
            MEMORY_COMPACTING_THREAD_PAGE_QUERY,
            &page_parameters,
            Some(2),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("thread_id"),
        Some(&Value::String("thread-a".to_string()))
    );
    assert_eq!(
        output.rows[1].get("thread_id"),
        Some(&Value::String("thread-b".to_string()))
    );

    let missing = read
        .query_with_params_bounded(
            MEMORY_BY_ID_QUERY,
            &BTreeMap::from([(
                "memory_id".to_string(),
                Value::String("missing".to_string()),
            )]),
            Some(1),
        )
        .unwrap();
    assert!(missing.rows.is_empty());
}
