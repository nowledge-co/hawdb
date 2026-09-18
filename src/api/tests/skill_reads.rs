use super::*;

const SKILL_STAGE_PAGE_QUERY: &str = "MATCH (s:Skill) WHERE s.stage IN $stages \
     RETURN s.id AS skill_id, id(s) AS skill_node_id, s.name AS name, \
     s.title AS title, s.stage AS stage, s.version AS version, \
     s.updated_at AS updated_at ORDER BY updated_at DESC, skill_id ASC LIMIT $limit";

const SKILL_DETAIL_QUERY: &str = "MATCH (s:Skill) \
     WHERE s.id = $key OR s.id STARTS WITH $key OR s.id CONTAINS $key \
     RETURN s.id AS skill_id, id(s) AS skill_node_id, s.name AS name, \
     s.title AS title, s.stage AS stage, s.version AS version \
     ORDER BY skill_node_id ASC LIMIT 1";

const SKILL_MEMORY_QUERY: &str =
    "MATCH (s:Skill)-[r:SYNTHESIZED_FROM]->(m:Memory) WHERE s.id = $skill_id \
     RETURN s.id AS skill_id, id(s) AS skill_node_id, m.id AS memory_id, \
     id(m) AS memory_node_id, id(r) AS relationship_id, \
     m.title AS title, m.created_at AS created_at \
     ORDER BY created_at DESC, memory_id ASC LIMIT $limit";

const SKILL_THREAD_SOURCE_QUERY: &str =
    "MATCH (s:Skill)-[sm:SYNTHESIZED_FROM]->(m:Memory)<-[ct:COMPACTS_TO]-(t:Thread) \
     WHERE s.id = $skill_id \
     RETURN s.id AS skill_id, id(s) AS skill_node_id, m.id AS memory_id, \
     id(m) AS memory_node_id, id(sm) AS skill_memory_relationship_id, \
     t.id AS thread_id, id(t) AS thread_node_id, t.thread_id AS thread_logical_id, \
     t.title AS title, t.source AS source, id(ct) AS compacts_to_relationship_id \
     ORDER BY skill_memory_relationship_id ASC, compacts_to_relationship_id ASC LIMIT $limit";

#[test]
fn skill_list_and_detail_use_fixed_parameterized_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Skill {id: 'skill-a', name: 'A', title: 'Alpha', stage: 'active', version: 1, updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill-b', name: 'B', title: 'Beta', stage: 'active', version: 2, updated_at: 10})")
        .unwrap();
    db.query("CREATE (:Skill {id: 'skill-c', stage: 'retired', updated_at: 30})")
        .unwrap();
    let mut read = db.begin_read_transaction();
    let page_parameters = BTreeMap::from([
        (
            "stages".to_string(),
            Value::List(vec![Value::String("active".to_string())]),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);

    let first = read
        .query_with_params_bounded(SKILL_STAGE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(SKILL_STAGE_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("skill_id"),
        Some(&Value::String("skill-a".to_string()))
    );

    let detail = read
        .query_with_params_bounded(
            SKILL_DETAIL_QUERY,
            &BTreeMap::from([("key".to_string(), Value::String("skill-a".to_string()))]),
            Some(1),
        )
        .unwrap();
    assert_eq!(detail.rows.len(), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn skill_memory_read_is_bounded_by_skill_and_limit() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-a', title: 'A', created_at: 10})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory-b', title: 'B', created_at: 20})")
        .unwrap();
    db.query("MATCH (s:Skill {id: 'skill-a'}), (m:Memory {id: 'memory-a'}) CREATE (s)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (s:Skill {id: 'skill-a'}), (m:Memory {id: 'memory-b'}) CREATE (s)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    let parameters = BTreeMap::from([
        ("skill_id".to_string(), Value::String("skill-a".to_string())),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut read = db.begin_read_transaction();

    let output = read
        .query_with_params_bounded(SKILL_MEMORY_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("memory_id"),
        Some(&Value::String("memory-b".to_string()))
    );
}

#[test]
fn skill_thread_source_read_uses_one_bounded_path_query() {
    let mut db = Database::new();
    db.query("CREATE (:Skill {id: 'skill-a'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory-a'})").unwrap();
    db.query(
        "CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', title: 'A', source: 'cli'})",
    )
    .unwrap();
    db.query("MATCH (s:Skill {id: 'skill-a'}), (m:Memory {id: 'memory-a'}) CREATE (s)-[:SYNTHESIZED_FROM]->(m)")
        .unwrap();
    db.query("MATCH (t:Thread {id: 'thread-a'}), (m:Memory {id: 'memory-a'}) CREATE (t)-[:COMPACTS_TO]->(m)")
        .unwrap();
    let parameters = BTreeMap::from([
        ("skill_id".to_string(), Value::String("skill-a".to_string())),
        ("limit".to_string(), Value::Int(1)),
    ]);
    let mut read = db.begin_read_transaction();

    let output = read
        .query_with_params_bounded(SKILL_THREAD_SOURCE_QUERY, &parameters, Some(1))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("thread_id"),
        Some(&Value::String("thread-a".to_string()))
    );
}
