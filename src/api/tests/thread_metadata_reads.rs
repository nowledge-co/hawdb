use super::*;

const THREAD_PAGE_QUERY: &str = "MATCH (t:Thread) \
     WHERE t.source = $source \
       AND (t.space_id IS NULL OR t.space_id = '' OR t.space_id = $space_id) \
       AND t.thread_id IS NOT NULL AND t.thread_id <> '' \
       AND t.id > $after_id AND t.metadata CONTAINS $metadata_marker \
     RETURN t.id AS id, t.thread_id AS thread_id, t.title AS title, \
       t.summary AS summary, t.message_count AS message_count, \
       t.updated_at AS updated_at \
     ORDER BY updated_at DESC, id ASC LIMIT $limit";

const THREAD_SOURCES_QUERY: &str = "MATCH (t:Thread) \
     WHERE t.source IS NOT NULL AND t.source <> '' \
     RETURN DISTINCT t.source AS source ORDER BY source ASC LIMIT $limit";

const THREAD_DETAIL_QUERY: &str = "MATCH (t:Thread) \
     WHERE t.id = $id OR t.thread_id = $id \
     RETURN id(t) AS thread_node_id, t.id AS id, t.thread_id AS thread_id, \
       t.title AS title, t.summary AS summary, t.message_count AS message_count, \
       t.source AS source, t.created_at AS created_at, t.updated_at AS updated_at, \
       t.space_id AS space_id, t.project AS project, t.workspace AS workspace \
     ORDER BY thread_node_id ASC LIMIT 1";

const THREAD_SOURCE_LOOKUP_QUERY: &str = "MATCH (t:Thread) \
     WHERE (t.id = $key OR t.id STARTS WITH $key OR t.id CONTAINS $key) \
       AND t.source = $source \
     RETURN id(t) AS thread_node_id, t.id AS id, t.thread_id AS thread_id, \
       t.title AS title, t.summary AS summary, t.message_count AS message_count, \
       t.source AS source, t.created_at AS created_at, t.updated_at AS updated_at, \
       t.space_id AS space_id, t.project AS project, t.workspace AS workspace \
     ORDER BY thread_node_id ASC LIMIT 1";

const THREAD_IDENTITY_QUERY: &str = "MATCH (ti:ThreadIdentity {id: $identity_key}) \
     RETURN id(ti) AS identity_node_id, ti.thread_node_id AS thread_node_id, \
       ti.thread_id AS thread_id, ti.space_id AS space_id, ti.source AS source \
     LIMIT 1";

const THREAD_SYNC_QUERY: &str = "MATCH (t:Thread {id: $id}) \
     RETURN id(t) AS thread_node_id, COALESCE(t.title, '') AS title, \
       COALESCE(t.source, '') AS source, COALESCE(t.project, '') AS project, \
       COALESCE(t.workspace, '') AS workspace, \
       COALESCE(t.space_id, 'default') AS space_id LIMIT 1";

#[test]
fn thread_page_and_source_reads_use_fixed_bounded_queries() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Thread {id: 'thread-a', thread_id: 'logical-a', title: 'Alpha', source: 'codex', space_id: '', metadata: 'favorite:true', message_count: 2, updated_at: 20})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-b', thread_id: 'logical-b', title: 'Beta', source: 'codex', space_id: 'default', metadata: 'favorite:true', message_count: 4, updated_at: 30})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-c', thread_id: 'logical-c', source: 'slack', space_id: 'default', metadata: 'favorite:true', updated_at: 40})")
        .unwrap();
    let page_parameters = BTreeMap::from([
        ("source".to_string(), Value::String("codex".to_string())),
        ("space_id".to_string(), Value::String("default".to_string())),
        ("after_id".to_string(), Value::String(String::new())),
        (
            "metadata_marker".to_string(),
            Value::String("favorite".to_string()),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let source_parameters = BTreeMap::from([("limit".to_string(), Value::Int(2))]);
    let mut read = db.begin_read_transaction();

    let first = read
        .query_with_params_bounded(THREAD_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(THREAD_PAGE_QUERY, &page_parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("id"),
        Some(&Value::String("thread-b".to_string()))
    );

    let sources = read
        .query_with_params_bounded(THREAD_SOURCES_QUERY, &source_parameters, Some(2))
        .unwrap();
    assert_eq!(sources.rows.len(), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 2);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}

#[test]
fn thread_detail_and_source_lookup_are_bounded_and_snapshot_pinned() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'alpha-thread', thread_id: 'logical-a', title: 'Alpha', summary: 'Summary', source: 'codex', message_count: 3, created_at: 10, updated_at: 20, space_id: 'team', project: 'graph', workspace: 'local'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'alpha-other', source: 'slack', title: 'Other'})")
        .unwrap();
    let detail_parameters =
        BTreeMap::from([("id".to_string(), Value::String("logical-a".to_string()))]);
    let lookup_parameters = BTreeMap::from([
        ("key".to_string(), Value::String("alpha".to_string())),
        ("source".to_string(), Value::String("codex".to_string())),
    ]);
    let mut read = db.begin_read_transaction();
    db.query("MATCH (t:Thread {id: 'alpha-thread'}) SET t.title = 'Changed'")
        .unwrap();

    let detail = read
        .query_with_params_bounded(THREAD_DETAIL_QUERY, &detail_parameters, Some(1))
        .unwrap();
    assert_eq!(detail.rows.len(), 1);
    assert_eq!(
        detail.rows[0].get("title"),
        Some(&Value::String("Alpha".to_string()))
    );

    let lookup = read
        .query_with_params_bounded(THREAD_SOURCE_LOOKUP_QUERY, &lookup_parameters, Some(1))
        .unwrap();
    assert_eq!(lookup.rows.len(), 1);
    assert_eq!(
        lookup.rows[0].get("id"),
        Some(&Value::String("alpha-thread".to_string()))
    );
}

#[test]
fn thread_detail_identity_lookup_uses_bounded_union_seek() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'physical-a', thread_id: 'logical-a', title: 'Alpha'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'logical-b', thread_id: 'logical-a', title: 'Matches both'})")
        .unwrap();
    for id in 0..32 {
        db.query(&format!(
            "CREATE (:Thread {{id: 'physical-{id}', thread_id: 'logical-{id}', title: 'Filler {id}'}})"
        ))
        .unwrap();
    }
    db.query("CREATE INDEX ON :Thread(id)").unwrap();
    db.query("CREATE INDEX ON :Thread(thread_id)").unwrap();
    let parameters = BTreeMap::from([("id".to_string(), Value::String("logical-a".to_string()))]);

    let explain = db
        .explain_query_with_params(THREAD_DETAIL_QUERY, &parameters)
        .unwrap();
    let physical_plan = explain.physical_plan.explain(0);
    assert!(physical_plan.contains("IndexNodeUnionSeek"));
    assert!(physical_plan.contains("NodeProjectionScanExec"));

    let output = db
        .query_with_params(THREAD_DETAIL_QUERY, &parameters)
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Alpha".to_string()))
    );
}

#[test]
fn thread_identity_and_sync_reads_use_exact_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:ThreadIdentity {id: 'identity-a', thread_node_id: 'thread-a', thread_id: 'logical-a', space_id: '', source: 'codex'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'thread-a', title: 'Alpha', source: 'codex', project: 'graph', workspace: 'local', space_id: ''})")
        .unwrap();
    let identity_parameters = BTreeMap::from([(
        "identity_key".to_string(),
        Value::String("identity-a".to_string()),
    )]);
    let sync_parameters =
        BTreeMap::from([("id".to_string(), Value::String("thread-a".to_string()))]);
    let mut read = db.begin_read_transaction();

    let identity = read
        .query_with_params_bounded(THREAD_IDENTITY_QUERY, &identity_parameters, Some(1))
        .unwrap();
    assert_eq!(identity.rows.len(), 1);
    assert_eq!(
        identity.rows[0].get("thread_id"),
        Some(&Value::String("logical-a".to_string()))
    );

    let sync = read
        .query_with_params_bounded(THREAD_SYNC_QUERY, &sync_parameters, Some(1))
        .unwrap();
    assert_eq!(sync.rows.len(), 1);
    assert_eq!(
        sync.rows[0].get("space_id"),
        Some(&Value::String(String::new()))
    );
}
