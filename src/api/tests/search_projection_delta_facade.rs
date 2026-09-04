use super::*;

#[test]
fn database_facade_applies_search_projection_delta_without_background_admission() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Remove me"))
        .unwrap();

    let report = db
        .apply_search_projection_delta(
            &mut search_index,
            SearchProjectionDelta {
                upserts: vec![search_projection_row(
                    "new",
                    "Foreground projection",
                    "Caller requested incremental FTS update",
                )],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(report.action, "incremental_update");
    assert_eq!(report.operation_count, 2);
    assert!(search_index.document("memory:old").is_none());
    assert!(search_index.document("memory:new").is_some());
}

#[test]
fn database_facade_background_search_projection_delta_uses_qos_admission() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let delta = SearchProjectionDelta {
        upserts: vec![search_projection_row(
            "new",
            "Deferred projection",
            "Internal background FTS update",
        )],
        deletes: vec!["memory:old".to_string()],
        max_operations: Some(2),
        source_graph_commit_epoch: None,
    };

    let plan = db
        .search_projection_delta_background_work_plan(
            &delta,
            BackgroundWorkHint {
                recent_delta_operations: 2,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();
    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);

    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };
    let error = db
        .apply_background_search_projection_delta(
            &mut search_index,
            &policy,
            &LocalQosState::default(),
            delta,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}

#[test]
fn database_facade_scheduled_search_projection_delta_releases_background_budget() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert_projection_row(search_projection_row("old", "Old projection", "Keep me"))
        .unwrap();
    let scheduler = db.local_qos_scheduler();
    let error = db
        .apply_scheduled_background_search_projection_delta(
            &mut search_index,
            SearchProjectionDelta {
                upserts: vec![search_projection_row(
                    "new",
                    "Rejected by delta budget",
                    "Internal background FTS update",
                )],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(1),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("exceeded configured limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(search_index.document("memory:old").is_some());
    assert!(search_index.document("memory:new").is_none());
}
