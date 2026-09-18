use super::*;

#[test]
fn set_system_variables_configures_query_work_request() {
    let mut db = Database::new();

    let priority = db.query("SET system.work_priority = 'background'").unwrap();
    db.query("SET system.work_class = 'projection'").unwrap();
    db.query("SET system.estimated_operations = 128").unwrap();

    assert_eq!(
        priority.rows[0].get("name"),
        Some(&Value::String("system.work_priority".to_string()))
    );
    assert_eq!(
        priority.rows[0].get("value"),
        Some(&Value::String("background".to_string()))
    );
    assert_eq!(
        db.system_variables().work_priority,
        WorkPriority::Background
    );
    assert_eq!(db.system_variables().work_class, WorkClass::Projection);
    assert_eq!(db.system_variables().estimated_operations, 128);
    assert_eq!(
        db.query_work_request(),
        WorkRequest::background(WorkClass::Projection, 128)
    );
}

#[test]
fn set_system_variable_keyword_syntax_configures_query_work_request() {
    let mut db = Database::new();

    db.query("SET SYSTEM VARIABLE work_priority = 'background'")
        .unwrap();
    db.query("SET SYSTEM VARIABLE system.work_class = 'analytics'")
        .unwrap();
    db.query("SET SYSTEM VARIABLE estimated_operations = 32")
        .unwrap();

    assert_eq!(
        db.query_work_request(),
        WorkRequest::background(WorkClass::Analytics, 32)
    );
}

#[test]
fn set_system_variables_are_session_scoped() {
    let mut db = Database::new();
    db.query("SET system.work_class = 'query'").unwrap();
    assert_eq!(
        db.query_work_request(),
        WorkRequest::foreground(WorkClass::Query, 1)
    );

    {
        let mut session = db.session();
        session
            .query("SET system.work_priority = 'background'")
            .unwrap();
        session
            .query("SET system.work_class = 'analytics'")
            .unwrap();
        session
            .query("SET system.estimated_operations = 64")
            .unwrap();
        assert_eq!(
            session.query_work_request(),
            WorkRequest::background(WorkClass::Analytics, 64)
        );
    }

    assert_eq!(
        db.query_work_request(),
        WorkRequest::foreground(WorkClass::Query, 1)
    );
}

#[test]
fn cypher_system_hints_configure_single_query_work_request() {
    let mut db = Database::new();
    db.query("SET system.work_class = 'projection'").unwrap();

    let work_request = db
        .query_work_request_for(
            "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
             system.estimated_operations = 64 MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap();
    assert_eq!(
        work_request,
        WorkRequest::background(WorkClass::Analytics, 64)
    );
    assert_eq!(
        db.query_work_request(),
        WorkRequest::foreground(WorkClass::Projection, 1)
    );
}

#[test]
fn cypher_system_hints_are_session_relative() {
    let mut db = Database::new();
    let mut session = db.session();
    session
        .query("SET system.work_priority = 'background'")
        .unwrap();
    session.query("SET system.work_class = 'import'").unwrap();

    let work_request = session
        .query_work_request_for(
            "CYPHER system.work_class = 'analytics' system.estimated_operations = 32 \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap();
    assert_eq!(
        work_request,
        WorkRequest::background(WorkClass::Analytics, 32)
    );
    assert_eq!(
        session.query_work_request(),
        WorkRequest::background(WorkClass::Import, 1)
    );
}

#[test]
fn optimizer_search_hint_is_statement_scoped_on_one_read_snapshot() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph'})")
        .unwrap();
    let snapshot_epoch = db.commit_epoch();
    let mut snapshot = db.begin_read_transaction();

    let memo_cypher = "CYPHER system.optimizer_search = 'memo' \
                       MATCH (m:Memory) RETURN m.id AS id";
    let fallback_cypher = "CYPHER system.optimizer_search = 'direct_fallback' \
                           MATCH (m:Memory) RETURN m.id AS id";

    let memo_explain = snapshot.explain_query(memo_cypher).unwrap();
    let fallback_explain = snapshot.explain_query(fallback_cypher).unwrap();
    assert_eq!(memo_explain.trace.search_mode.as_str(), "memo");
    assert_eq!(
        fallback_explain.trace.search_mode.as_str(),
        "direct_fallback"
    );
    assert_eq!(
        memo_explain.plan_cache_lookup,
        PlanCacheLookup::Bypass(PlanCacheBypassReason::OptimizerDirective)
    );
    assert_eq!(
        fallback_explain.plan_cache_lookup,
        PlanCacheLookup::Bypass(PlanCacheBypassReason::OptimizerDirective)
    );
    assert!(fallback_explain
        .trace
        .warnings
        .iter()
        .all(|warning| !warning.contains("memo budget exceeded")));

    let memo = snapshot.query(memo_cypher).unwrap();
    let fallback = snapshot.query(fallback_cypher).unwrap();
    let auto_explain = snapshot
        .explain_query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(memo, fallback);
    assert_eq!(auto_explain.trace.search_mode.as_str(), "memo");
    drop(snapshot);
    assert_eq!(db.commit_epoch(), snapshot_epoch);
}

#[test]
fn optimizer_search_hint_fails_closed_for_invalid_or_conflicting_values() {
    let db = Database::new();

    let invalid = db
        .explain_query(
            "CYPHER system.optimizer_search = 'unknown' \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap_err();
    assert!(invalid
        .to_string()
        .contains("accepts auto, memo, or direct_fallback"));

    let duplicate = db
        .explain_query(
            "CYPHER system.optimizer_search = 'memo' \
             system.optimizer_search = 'direct_fallback' \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap_err();
    assert!(duplicate
        .to_string()
        .contains("duplicate CYPHER system hint system.optimizer_search"));
}

#[test]
fn optimizer_search_is_query_only_and_memo_respects_the_group_budget() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_optimizer_groups: Some(0),
        ..DatabaseConfig::default()
    });

    let set_error = db
        .query("SET system.optimizer_search = 'direct_fallback'")
        .unwrap_err();
    assert!(set_error
        .to_string()
        .contains("unknown system variable system.optimizer_search"));

    let memo_error = db
        .explain_query(
            "CYPHER system.optimizer_search = 'memo' \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap_err();
    let memo_error = memo_error.to_string();
    assert!(memo_error.contains("memo search directive requires"));
    assert!(memo_error.contains("max_groups is 0"));

    let fallback = db
        .explain_query(
            "CYPHER system.optimizer_search = 'direct_fallback' \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap();
    assert_eq!(fallback.trace.search_mode.as_str(), "direct_fallback");
}

#[test]
fn session_explain_reports_session_scoped_resource_intent() {
    let mut db = Database::new();
    let mut session = db.session();
    session
        .query("SET system.work_priority = 'background'")
        .unwrap();
    session.query("SET system.work_class = 'import'").unwrap();
    session
        .query("SET system.estimated_operations = 8")
        .unwrap();

    let explain = session
        .explain_query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();
    assert_eq!(
        explain.work_request,
        WorkRequest::background(WorkClass::Import, 8)
    );

    let hinted = session
        .explain_query(
            "CYPHER system.work_class = 'analytics' system.estimated_operations = 32 \
             MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap();
    assert_eq!(
        hinted.work_request,
        WorkRequest::background(WorkClass::Analytics, 32)
    );
}

#[test]
fn session_cypher_explain_reports_session_scoped_resource_intent() {
    let mut db = Database::new();
    let mut session = db.session();
    session
        .query("SET system.work_priority = 'background'")
        .unwrap();
    session.query("SET system.work_class = 'import'").unwrap();
    session
        .query("SET system.estimated_operations = 8")
        .unwrap();

    let output = session
        .query("EXPLAIN MATCH (m:Memory) RETURN m.id AS id")
        .unwrap();

    assert_eq!(output.rows.len(), 1);
    let Some(Value::Map(work_request)) = output.rows[0].get("work_request") else {
        panic!("expected work request map");
    };
    assert_eq!(
        work_request.get("priority"),
        Some(&Value::String("background".to_string()))
    );
    assert_eq!(
        work_request.get("class"),
        Some(&Value::String("import".to_string()))
    );
    assert_eq!(
        work_request.get("estimated_operations"),
        Some(&Value::Int(8))
    );
}

#[test]
fn session_explain_is_rejected_inside_active_transaction() {
    let mut db = Database::new();
    let mut session = db.session();
    session.query("BEGIN TRANSACTION").unwrap();

    let error = session
        .explain_query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("EXPLAIN is not allowed inside an active transaction"));

    let error = session
        .query("EXPLAIN MATCH (m:Memory) RETURN m.id AS id")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("EXPLAIN is not allowed inside an active transaction"));
}

#[test]
fn cypher_system_hints_allow_query_parameters_but_require_literal_hint_values() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1, title: 'Graph'})")
        .unwrap();

    let mut parameters = BTreeMap::new();
    parameters.insert("id".to_string(), Value::Int(1));
    let output = db
        .query_with_params(
            "CYPHER system.work_priority = 'background' MATCH (m:Memory {id: $id}) \
             RETURN m.title AS title",
            &parameters,
        )
        .unwrap();
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Graph".to_string()))
    );

    let error = db
        .query_work_request_for(
            "CYPHER system.work_priority = $priority MATCH (m:Memory) RETURN m.id AS id",
        )
        .unwrap_err();
    assert!(error.to_string().contains("requires a literal value"));
}

#[test]
fn set_system_variables_reject_invalid_values_without_wal() {
    let path = unique_test_dir("set_system_variables_invalid_without_wal");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id: 'stable'})").unwrap();
    let graph_commit_epoch = db.store.commit_epoch();
    let wal_before = read_test_wal(&path).unwrap();

    let bad_priority = db.query("SET system.work_priority = 'urgent'").unwrap_err();
    let bad_estimate = db
        .query("SET system.estimated_operations = -1")
        .unwrap_err();
    let unknown = db.query("SET system.unknown = 'x'").unwrap_err();

    assert!(bad_priority
        .to_string()
        .contains("foreground or background"));
    assert!(bad_estimate.to_string().contains("non-negative integer"));
    assert!(unknown.to_string().contains("unknown system variable"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch);
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
}

#[test]
fn set_system_variable_is_rejected_inside_transactions() {
    let mut db = Database::new();
    {
        let mut tx = db.begin_transaction();
        let error = tx
            .query("SET system.work_priority = 'background'")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("not allowed inside a transaction"));
        tx.rollback();
    }
    {
        let mut session = db.session();
        session.query("BEGIN TRANSACTION").unwrap();
        let error = session
            .query("SET system.work_priority = 'background'")
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("not allowed inside an active transaction"));
        session.query("ROLLBACK").unwrap();
    }

    let mut snapshot = db.begin_read_transaction();
    let error = snapshot
        .query("SET system.work_priority = 'background'")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("not allowed inside a read transaction"));
}
