use super::*;

#[test]
fn external_content_artifact_jobs_are_explicitly_outside_graph_kernel() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    assert_eq!(job.artifact_type, "content_artifact");
    assert_eq!(job.name, "source-1");
    assert_eq!(job.action, "parse");
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    let error = report.job.last_error.as_deref().unwrap();
    assert!(error.contains("outside the graph kernel"));
    assert!(error.contains("content artifact job runtime"));
    assert_eq!(
        report.output.rows[0].get("artifact_type"),
        Some(&Value::String("content_artifact".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("status"),
        Some(&Value::String("failed".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("error"),
        Some(&Value::String(error.to_string()))
    );
}

#[test]
fn pending_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    db.schedule_derived_artifact_rebuild();
    let first = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );
    let second = db.schedule_external_content_artifact_job("source-2", "parse");

    let pending_one = db.pending_external_content_artifact_jobs(1);
    assert_eq!(pending_one.len(), 1);
    assert_eq!(pending_one[0].id, first.id);
    assert_eq!(
        pending_one[0].payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let pending_all = db.pending_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        pending_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert!(db.pending_external_content_artifact_jobs(0).is_empty());

    let report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, first.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);

    let remaining = db.pending_external_content_artifact_jobs(8);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, second.id);
}

#[test]
fn failed_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let first_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "source-1 parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(first_failure.job.id, first.id);
    let second_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "source-2 parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(second_failure.job.id, second.id);
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );

    let failed_one = db.failed_external_content_artifact_jobs(1);
    assert_eq!(failed_one.len(), 1);
    assert_eq!(failed_one[0].id, first.id);
    assert_eq!(
        failed_one[0].payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let failed_all = db.failed_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        failed_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first.id, second.id]
    );
    assert!(db.failed_external_content_artifact_jobs(0).is_empty());

    db.retry_failed_external_content_artifact_job(first.id)
        .unwrap();
    let remaining_failed = db.failed_external_content_artifact_jobs(8);
    assert_eq!(remaining_failed.len(), 1);
    assert_eq!(remaining_failed[0].id, second.id);
}

#[test]
fn failed_external_content_artifact_jobs_can_be_filtered_by_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );

    db.run_next_external_content_artifact_job_with(|_| {
        Err(crate::error::SkeinError::Execution(
            "parse failed".to_string(),
        ))
    })
    .unwrap()
    .unwrap();
    db.run_next_external_content_artifact_job_with(|_| {
        Err(crate::error::SkeinError::Execution(
            "crawl failed".to_string(),
        ))
    })
    .unwrap()
    .unwrap();

    let parse_failed = db.failed_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_failed.len(), 1);
    assert_eq!(parse_failed[0].id, parse.id);
    assert_eq!(
        parse_failed[0].payload.get("content_uri"),
        Some(&Value::String(
            "file:///nowledge/source-parse.md".to_string()
        ))
    );
    let crawl_failed = db.failed_external_content_artifact_jobs_for_action("crawl", 8);
    assert_eq!(crawl_failed.len(), 1);
    assert_eq!(crawl_failed[0].id, crawl.id);
    assert!(db
        .failed_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());
    assert!(db
        .failed_external_content_artifact_jobs_for_action("parse", 0)
        .is_empty());
}

#[test]
fn succeeded_external_content_artifact_jobs_are_bounded_and_filtered() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let parse_report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "projection_ref".to_string(),
                        Value::String("search:v1".to_string()),
                    ),
                ])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_report.job.id, parse.id);

    let crawl_report = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "projection_ref".to_string(),
                        Value::String("crawl-log:v1".to_string()),
                    ),
                ])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_report.job.id, crawl.id);
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph.id);

    let succeeded_one = db.succeeded_external_content_artifact_jobs(1);
    assert_eq!(succeeded_one.len(), 1);
    assert_eq!(succeeded_one[0].id, parse.id);
    assert_eq!(succeeded_one[0].last_output, Some(parse_report.output));

    let succeeded_all = db.succeeded_external_content_artifact_jobs(usize::MAX);
    assert_eq!(
        succeeded_all.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![parse.id, crawl.id]
    );
    assert_eq!(succeeded_all[1].last_output, Some(crawl_report.output));
    assert!(db.succeeded_external_content_artifact_jobs(0).is_empty());

    let parse_succeeded = db.succeeded_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_succeeded.len(), 1);
    assert_eq!(parse_succeeded[0].id, parse.id);
    assert_eq!(
        parse_succeeded[0].last_output.as_ref().unwrap().rows[0].get("projection_ref"),
        Some(&Value::String("search:v1".to_string()))
    );
    assert!(db
        .succeeded_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());
    assert!(db
        .succeeded_external_content_artifact_jobs_for_action("parse", 0)
        .is_empty());
}

#[test]
fn external_content_artifact_job_summary_counts_runtime_work_only() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "crawl");
    db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let initial = db.external_content_artifact_job_summary();
    assert_eq!(initial.total, 2);
    assert_eq!(initial.pending, 2);
    assert_eq!(initial.running, 0);
    assert_eq!(initial.succeeded, 0);
    assert_eq!(initial.failed, 0);
    assert_eq!(
        initial.pending_by_action,
        BTreeMap::from([("crawl".to_string(), 1), ("parse".to_string(), 1)])
    );
    assert!(initial.failed_by_action.is_empty());
    assert_eq!(initial.next_pending_job_id, Some(first.id));
    assert_eq!(initial.oldest_failed_job_id, None);

    let initial_parse = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(initial_parse.total, 1);
    assert_eq!(initial_parse.pending, 1);
    assert_eq!(initial_parse.failed, 0);
    assert_eq!(
        initial_parse.pending_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert!(initial_parse.failed_by_action.is_empty());
    assert_eq!(initial_parse.next_pending_job_id, Some(first.id));
    assert_eq!(initial_parse.oldest_failed_job_id, None);

    let initial_crawl = db.external_content_artifact_job_summary_for_action("crawl");
    assert_eq!(initial_crawl.total, 1);
    assert_eq!(initial_crawl.pending, 1);
    assert_eq!(initial_crawl.next_pending_job_id, Some(second.id));
    let initial_embed = db.external_content_artifact_job_summary_for_action("embed");
    assert_eq!(initial_embed.total, 0);
    assert!(initial_embed.pending_by_action.is_empty());

    let failed = db
        .run_external_content_artifact_job_with(first.id, |_| {
            Err(crate::error::SkeinError::Execution(
                "parser runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(failed.job.status, DerivedArtifactJobStatus::Failed);
    let succeeded = db
        .run_external_content_artifact_job_with(second.id, |job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(succeeded.job.status, DerivedArtifactJobStatus::Succeeded);

    let after_run = db.external_content_artifact_job_summary();
    assert_eq!(after_run.total, 2);
    assert_eq!(after_run.pending, 0);
    assert_eq!(after_run.running, 0);
    assert_eq!(after_run.succeeded, 1);
    assert_eq!(after_run.failed, 1);
    assert!(after_run.pending_by_action.is_empty());
    assert_eq!(
        after_run.failed_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert_eq!(after_run.next_pending_job_id, None);
    assert_eq!(after_run.oldest_failed_job_id, Some(first.id));

    let parse_after_run = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(parse_after_run.total, 1);
    assert_eq!(parse_after_run.pending, 0);
    assert_eq!(parse_after_run.failed, 1);
    assert!(parse_after_run.pending_by_action.is_empty());
    assert_eq!(
        parse_after_run.failed_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert_eq!(parse_after_run.next_pending_job_id, None);
    assert_eq!(parse_after_run.oldest_failed_job_id, Some(first.id));

    let crawl_after_run = db.external_content_artifact_job_summary_for_action("crawl");
    assert_eq!(crawl_after_run.total, 1);
    assert_eq!(crawl_after_run.pending, 0);
    assert_eq!(crawl_after_run.succeeded, 1);
    assert_eq!(crawl_after_run.failed, 0);
    assert_eq!(crawl_after_run.next_pending_job_id, None);
    assert_eq!(crawl_after_run.oldest_failed_job_id, None);

    db.retry_failed_external_content_artifact_job(first.id)
        .unwrap();
    let after_retry = db.external_content_artifact_job_summary();
    assert_eq!(after_retry.total, 2);
    assert_eq!(after_retry.pending, 1);
    assert_eq!(after_retry.succeeded, 1);
    assert_eq!(after_retry.failed, 0);
    assert_eq!(
        after_retry.pending_by_action,
        BTreeMap::from([("parse".to_string(), 1)])
    );
    assert!(after_retry.failed_by_action.is_empty());
    assert_eq!(after_retry.next_pending_job_id, Some(first.id));
    assert_eq!(after_retry.oldest_failed_job_id, None);

    let parse_after_retry = db.external_content_artifact_job_summary_for_action("parse");
    assert_eq!(parse_after_retry.total, 1);
    assert_eq!(parse_after_retry.pending, 1);
    assert_eq!(parse_after_retry.failed, 0);
    assert_eq!(parse_after_retry.next_pending_job_id, Some(first.id));
    assert_eq!(parse_after_retry.oldest_failed_job_id, None);
}

#[test]
fn external_content_artifact_job_background_work_plan_is_rankable_by_action() {
    let mut db = Database::new();
    assert!(db
        .external_content_artifact_job_background_work_plan(BackgroundWorkHint::default(), 3)
        .is_none());
    assert!(db
        .external_content_artifact_job_background_work_plan_for_action(
            "parse",
            BackgroundWorkHint::default(),
            3,
        )
        .is_none());

    db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    assert!(db
        .external_content_artifact_job_background_work_plan(BackgroundWorkHint::default(), 3)
        .is_none());

    db.schedule_external_content_artifact_job("source-parse", "parse");
    db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let hint = BackgroundWorkHint {
        active_topic: true,
        recent_delta_operations: 5,
        tenant_budget_remaining_operations: Some(8),
        ..BackgroundWorkHint::default()
    };

    let any_plan = db
        .external_content_artifact_job_background_work_plan(hint.clone(), 3)
        .unwrap();
    assert_eq!(any_plan.request.class, WorkClass::Import);
    assert_eq!(any_plan.request.estimated_operations, 3);
    assert_eq!(any_plan.hint, hint);

    let parse_plan = db
        .external_content_artifact_job_background_work_plan_for_action(
            "parse",
            BackgroundWorkHint {
                query_probability_per_million: 42,
                tenant_budget_remaining_operations: Some(2),
                ..BackgroundWorkHint::default()
            },
            3,
        )
        .unwrap();
    assert_eq!(parse_plan.request.class, WorkClass::Import);
    assert_eq!(parse_plan.request.estimated_operations, 3);
    let decision =
        LocalQosPolicy::default().evaluate_background_work(&LocalQosState::default(), &parse_plan);
    assert!(matches!(
        decision.admission,
        QosAdmission::Defer { reason, .. } if reason.contains("tenant budget remaining 2")
    ));
    assert!(decision
        .reasons
        .iter()
        .any(|reason| reason.contains("tenant budget remaining 2")));

    assert!(db
        .external_content_artifact_job_background_work_plan_for_action(
            "embed",
            BackgroundWorkHint::default(),
            3,
        )
        .is_none());
}

#[test]
fn external_content_runtime_manifest_filters_claimable_jobs() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-parse.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("parse-sha".to_string())),
        ]),
    );
    db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );
    db.schedule_external_content_artifact_job("source-missing", "parse");

    let manifest = ExternalContentArtifactRuntimeManifest::new("markdown-parser")
        .with_runtime_version("1.0.0")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_required_payload_key("sha256")
        .with_estimated_operations(7);

    let pending = db.pending_external_content_artifact_jobs_for_runtime(&manifest, 8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, parse.id);
    assert_eq!(pending[0].action, "parse");
    assert_eq!(
        pending[0].payload.get("sha256"),
        Some(&Value::String("parse-sha".to_string()))
    );
    assert!(db
        .pending_external_content_artifact_jobs_for_runtime(&manifest, 0)
        .is_empty());
}

#[test]
fn external_content_runtime_manifest_exposes_import_work_plan() {
    let mut db = Database::new();
    assert!(db
        .external_content_artifact_job_background_work_plan_for_runtime(
            &ExternalContentArtifactRuntimeManifest::new("parser")
                .with_supported_action("parse")
                .with_required_payload_key("content_uri"),
            BackgroundWorkHint::default(),
        )
        .is_none());

    db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_estimated_operations(5);
    let plan = db
        .external_content_artifact_job_background_work_plan_for_runtime(
            &manifest,
            BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 10,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Import);
    assert_eq!(plan.request.estimated_operations, 5);
    let ranked = LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
    assert_eq!(ranked.len(), 1);
    assert!(matches!(ranked[0].decision.admission, QosAdmission::Admit));
}

#[test]
fn external_content_runtime_manifest_runs_only_claimable_jobs() {
    let mut db = Database::new();
    let missing = db.schedule_external_content_artifact_job("source-missing", "parse");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-parse.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("parse-sha".to_string())),
        ]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("https://example.invalid/source-crawl".to_string()),
            ),
            ("sha256".to_string(), Value::String("crawl-sha".to_string())),
        ]),
    );

    let manifest = ExternalContentArtifactRuntimeManifest::new("markdown-parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_required_payload_key("sha256")
        .with_estimated_operations(7);

    let report = db
        .run_next_external_content_artifact_job_for_runtime_with(&manifest, |job| {
            assert_eq!(job.id, parse.id);
            assert_eq!(job.action, "parse");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "runtime_name".to_string(),
                        Value::String("markdown-parser".to_string()),
                    ),
                ])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, parse.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].id, missing.id);
    assert_eq!(pending[1].id, crawl.id);
}

#[test]
fn failed_external_content_artifact_jobs_can_be_retried() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-1.md".to_string()),
        )]),
    );

    let failed = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "transient parser failure".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(failed.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(failed.job.attempts, 1);
    assert!(failed.job.last_output.is_none());
    assert!(failed
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("transient parser failure")));
    assert!(db.pending_external_content_artifact_jobs(8).is_empty());

    let retried = db
        .retry_failed_external_content_artifact_job(job.id)
        .unwrap();
    assert_eq!(retried.status, DerivedArtifactJobStatus::Pending);
    assert_eq!(retried.attempts, 1);
    assert!(retried.last_error.is_none());
    assert!(retried.last_output.is_none());
    assert_eq!(
        retried.payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );
    assert_eq!(db.pending_external_content_artifact_jobs(8)[0].id, job.id);

    let succeeded = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    ("attempts".to_string(), Value::Int(job.attempts as i64)),
                ])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(succeeded.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(succeeded.job.attempts, 2);
    assert!(succeeded.job.last_error.is_none());
    assert_eq!(succeeded.job.last_output, Some(succeeded.output.clone()));
    assert_eq!(
        succeeded.output.rows[0].get("attempts"),
        Some(&Value::Int(2))
    );
    assert!(db
        .retry_failed_external_content_artifact_job(job.id)
        .is_none());

    let projected_graph_job = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let projected_graph_failure = db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph_job.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );
    assert!(db
        .retry_failed_external_content_artifact_job(projected_graph_job.id)
        .is_none());
}

#[test]
fn failed_external_content_artifact_jobs_can_be_retried_by_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let crawl = db.schedule_external_content_artifact_job_with_payload(
        "source-crawl",
        "crawl",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("https://example.invalid/source-crawl".to_string()),
        )]),
    );

    let parse_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "parse runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_failure.job.id, parse.id);
    let crawl_failure = db
        .run_next_external_content_artifact_job_with(|_| {
            Err(crate::error::SkeinError::Execution(
                "crawl runtime failed".to_string(),
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_failure.job.id, crawl.id);

    assert!(db
        .retry_failed_external_content_artifact_job_for_action("parse", crawl.id)
        .is_none());
    let retried = db
        .retry_failed_external_content_artifact_job_for_action("parse", parse.id)
        .unwrap();
    assert_eq!(retried.status, DerivedArtifactJobStatus::Pending);
    assert_eq!(retried.action, "parse");
    assert_eq!(retried.attempts, 1);
    assert!(retried.last_error.is_none());
    assert_eq!(
        retried.payload.get("content_uri"),
        Some(&Value::String(
            "file:///nowledge/source-parse.md".to_string()
        ))
    );

    let parse_pending = db.pending_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_pending.len(), 1);
    assert_eq!(parse_pending[0].id, parse.id);
    assert!(db
        .failed_external_content_artifact_jobs_for_action("parse", 8)
        .is_empty());
    let crawl_failed = db.failed_external_content_artifact_jobs_for_action("crawl", 8);
    assert_eq!(crawl_failed.len(), 1);
    assert_eq!(crawl_failed[0].id, crawl.id);

    let mut graph_db = Database::new();
    let projected_graph_job = graph_db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let projected_graph_failure = graph_db.run_next_derived_artifact_job().unwrap().unwrap();
    assert_eq!(projected_graph_failure.job.id, projected_graph_job.id);
    assert_eq!(
        projected_graph_failure.job.status,
        DerivedArtifactJobStatus::Failed
    );
    assert!(graph_db
        .retry_failed_external_content_artifact_job_for_action("rebuild", projected_graph_job.id,)
        .is_none());
}

#[test]
fn caller_owned_content_artifact_runtime_can_complete_external_jobs() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([
            (
                "source_id".to_string(),
                Value::String("source-1".to_string()),
            ),
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-1.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("abc123".to_string())),
            (
                "target_projection".to_string(),
                Value::String("search".to_string()),
            ),
        ]),
    );
    assert_eq!(
        job.payload.get("content_uri"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );

    let report = db
        .run_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(job.attempts, 1);
            assert_eq!(
                job.payload.get("sha256"),
                Some(&Value::String("abc123".to_string()))
            );
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([
                    ("job_id".to_string(), Value::Int(job.id as i64)),
                    (
                        "artifact_type".to_string(),
                        Value::String(job.artifact_type.clone()),
                    ),
                    ("name".to_string(), Value::String(job.name.clone())),
                    ("action".to_string(), Value::String(job.action.clone())),
                    (
                        "published_projection".to_string(),
                        job.payload
                            .get("target_projection")
                            .cloned()
                            .unwrap_or(Value::String("unknown".to_string())),
                    ),
                    ("parsed_chunks".to_string(), Value::Int(2)),
                ])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(report.job.attempts, 1);
    assert!(report.job.last_error.is_none());
    assert_eq!(report.job.last_output, Some(report.output.clone()));
    assert_eq!(
        report.output.rows[0].get("published_projection"),
        Some(&Value::String("search".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("parsed_chunks"),
        Some(&Value::Int(2))
    );
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Succeeded
    );
    assert_eq!(
        db.derived_artifact_jobs()[0].last_output,
        Some(QueryOutput {
            rows: vec![BTreeMap::from([
                ("job_id".to_string(), Value::Int(job.id as i64)),
                (
                    "artifact_type".to_string(),
                    Value::String("content_artifact".to_string()),
                ),
                ("name".to_string(), Value::String("source-1".to_string())),
                ("action".to_string(), Value::String("parse".to_string())),
                (
                    "published_projection".to_string(),
                    Value::String("search".to_string()),
                ),
                ("parsed_chunks".to_string(), Value::Int(2)),
            ])]
            .into(),
        })
    );
    assert!(db
        .run_next_external_content_artifact_job_with(|_| unreachable!())
        .unwrap()
        .is_none());
}

#[test]
fn background_external_content_artifact_job_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-1", "parse");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .run_next_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
}

#[test]
fn background_external_content_artifact_job_can_run_next_job_for_action() {
    let mut db = Database::new();
    let parse = db.schedule_external_content_artifact_job("source-parse", "parse");
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");

    let report = db
        .run_next_background_external_content_artifact_job_for_action_with(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            "crawl",
            |job| {
                assert_eq!(job.id, crawl.id);
                assert_eq!(job.action, "crawl");
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                assert_eq!(job.attempts, 1);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])]
                    .into(),
                })
            },
            2,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, crawl.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].id, parse.id);
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].id, crawl.id);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Succeeded);
}

#[test]
fn background_external_content_artifact_job_for_action_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-parse", "parse");
    db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .run_next_background_external_content_artifact_job_for_action_with(
            &policy,
            &LocalQosState::default(),
            "crawl",
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[1].attempts, 0);
}

#[test]
fn scheduled_background_external_content_artifact_job_tracks_import_budget() {
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(2);
    let mut db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    db.schedule_external_content_artifact_job("source-1", "parse");
    let scheduler = db.local_qos_scheduler();

    let report = db
        .run_next_scheduled_background_external_content_artifact_job_with(
            |job| {
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                assert_eq!(job.attempts, 1);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])]
                    .into(),
                })
            },
            2,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn scheduled_background_external_content_artifact_job_defers_when_import_lane_is_full() {
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(4);
    let mut db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(8),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    db.schedule_external_content_artifact_job("source-parse", "parse");
    let scheduler = db.local_qos_scheduler();
    let running = scheduler
        .try_start(crate::WorkRequest::background(crate::WorkClass::Import, 3))
        .unwrap();

    let error = db
        .run_next_scheduled_background_external_content_artifact_job_for_action_with(
            "parse",
            |_| unreachable!(),
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("class limit 4"));
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        3
    );
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);

    running.finish();
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
fn scheduled_background_external_content_artifact_job_releases_budget_on_runtime_error() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-1", "parse");
    let scheduler = db.local_qos_scheduler();

    let report = db
        .run_next_scheduled_background_external_content_artifact_job_with(
            |_| {
                Err(crate::error::SkeinError::Execution(
                    "parser failed".to_string(),
                ))
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn background_external_content_artifact_job_can_run_specific_pending_job() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let policy = LocalQosPolicy::default();

    let report = db
        .run_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            second.id,
            |job| {
                assert_eq!(job.id, second.id);
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                Ok(QueryOutput {
                    rows: vec![BTreeMap::from([(
                        "job_id".to_string(),
                        Value::Int(job.id as i64),
                    )])]
                    .into(),
                })
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
}

#[test]
fn scheduled_background_external_content_artifact_job_defers_specific_pending_job() {
    let mut class_limits = [None; crate::WORK_CLASS_COUNT];
    class_limits[crate::WorkClass::Import.as_index()] = Some(4);
    let mut db = Database::new_with_config(DatabaseConfig {
        local_qos_policy: LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(8),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        },
        ..DatabaseConfig::default()
    });
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    let scheduler = db.local_qos_scheduler();
    let running = scheduler
        .try_start(crate::WorkRequest::background(crate::WorkClass::Import, 3))
        .unwrap();

    let error = db
        .run_scheduled_background_external_content_artifact_job_with(job.id, |_| unreachable!(), 2)
        .unwrap_err();

    assert!(error.to_string().contains("class limit 4"));
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        3
    );
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);

    running.finish();
}

#[test]
fn scheduled_background_external_content_artifact_job_releases_budget_on_specific_runtime_error() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job("source-1", "parse");
    let scheduler = db.local_qos_scheduler();

    let report = db
        .run_scheduled_background_external_content_artifact_job_with(
            job.id,
            |_| {
                Err(crate::error::SkeinError::Execution(
                    "parser failed".to_string(),
                ))
            },
            1,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, job.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );
}

#[test]
fn caller_owned_content_artifact_runtime_can_run_specific_pending_job() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job_with_payload(
        "source-2",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-2.md".to_string()),
        )]),
    );
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let report = db
        .run_external_content_artifact_job_with(second.id, |job| {
            assert_eq!(job.id, second.id);
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(
                job.payload.get("content_uri"),
                Some(&Value::String("file:///nowledge/source-2.md".to_string()))
            );
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);

    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
    assert!(db
        .run_external_content_artifact_job_with(second.id, |_| unreachable!())
        .unwrap()
        .is_none());
    assert!(db
        .run_external_content_artifact_job_with(projected_graph.id, |_| unreachable!())
        .unwrap()
        .is_none());
    assert!(db
        .run_external_content_artifact_job_with(99, |_| unreachable!())
        .unwrap()
        .is_none());

    let next = db
        .run_next_external_content_artifact_job_with(|job| {
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(next.job.id, first.id);
}

#[test]
fn caller_owned_content_artifact_runtime_can_complete_with_lineage_manifest() {
    let mut db = Database::new();
    let job = db.schedule_external_content_artifact_job_with_payload(
        "source-1",
        "parse",
        BTreeMap::from([
            (
                "content_uri".to_string(),
                Value::String("file:///nowledge/source-1.md".to_string()),
            ),
            ("sha256".to_string(), Value::String("input-sha".to_string())),
        ]),
    );

    let report = db
        .complete_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.status, DerivedArtifactJobStatus::Running);
            assert_eq!(
                job.payload.get("sha256"),
                Some(&Value::String("input-sha".to_string()))
            );
            Ok(ExternalContentArtifactJobCompletion::new("nowledge-parser")
                .with_runtime_version("0.3.7")
                .with_input_ref("file:///nowledge/source-1.md")
                .with_input_checksum("sha256:input-sha")
                .with_output_ref("projection://search/source-1")
                .with_output_checksum("sha256:projection-sha")
                .with_projection("search", "search:source-1:v2")
                .with_source_graph_commit_epoch(42)
                .with_rows_produced(3)
                .with_metadata("parser_mode", Value::String("markdown".to_string())))
        })
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, job.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(report.job.last_output, Some(report.output.clone()));
    let row = &report.output.rows[0];
    assert_eq!(row.get("job_id"), Some(&Value::Int(job.id as i64)));
    assert_eq!(
        row.get("runtime_name"),
        Some(&Value::String("nowledge-parser".to_string()))
    );
    assert_eq!(
        row.get("runtime_version"),
        Some(&Value::String("0.3.7".to_string()))
    );
    assert_eq!(
        row.get("input_ref"),
        Some(&Value::String("file:///nowledge/source-1.md".to_string()))
    );
    assert_eq!(
        row.get("input_checksum"),
        Some(&Value::String("sha256:input-sha".to_string()))
    );
    assert_eq!(
        row.get("output_ref"),
        Some(&Value::String("projection://search/source-1".to_string()))
    );
    assert_eq!(
        row.get("output_checksum"),
        Some(&Value::String("sha256:projection-sha".to_string()))
    );
    assert_eq!(
        row.get("projection_kind"),
        Some(&Value::String("search".to_string()))
    );
    assert_eq!(
        row.get("projection_ref"),
        Some(&Value::String("search:source-1:v2".to_string()))
    );
    assert_eq!(row.get("source_graph_commit_epoch"), Some(&Value::Int(42)));
    assert_eq!(row.get("rows_produced"), Some(&Value::Int(3)));
    assert_eq!(
        row.get("metadata"),
        Some(&Value::Map(BTreeMap::from([(
            "parser_mode".to_string(),
            Value::String("markdown".to_string())
        )])))
    );
}

#[test]
fn content_artifact_completion_runner_only_claims_external_jobs() {
    let mut db = Database::new();
    let projected_graph = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    let parse = db.schedule_external_content_artifact_job("source-parse", "parse");

    assert!(db
        .complete_external_content_artifact_job_with(projected_graph.id, |_| unreachable!())
        .unwrap()
        .is_none());

    let report = db
        .complete_external_content_artifact_job_with(parse.id, |_| {
            Ok(ExternalContentArtifactJobCompletion::new("parser").with_rows_produced(1))
        })
        .unwrap()
        .unwrap();
    assert_eq!(report.job.id, parse.id);
    assert_eq!(
        report.output.rows[0].get("runtime_version"),
        Some(&Value::Null)
    );
    assert_eq!(
        report.output.rows[0].get("rows_produced"),
        Some(&Value::Int(1))
    );
}

#[test]
fn background_content_artifact_completion_uses_qos_admission() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job("source-parse", "parse");
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = db
        .complete_next_background_external_content_artifact_job_with(
            &policy,
            &LocalQosState::default(),
            |_| unreachable!(),
            1,
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert!(jobs[0].last_output.is_none());
}

#[test]
fn background_content_artifact_completion_for_runtime_uses_manifest_claim_and_qos() {
    let mut db = Database::new();
    let missing = db.schedule_external_content_artifact_job("source-missing", "parse");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
        .with_supported_action("parse")
        .with_required_payload_key("content_uri")
        .with_estimated_operations(5);
    let policy = LocalQosPolicy {
        max_background_operations: Some(4),
        ..LocalQosPolicy::default()
    };

    let error = db
        .complete_next_background_external_content_artifact_job_for_runtime_with(
            &policy,
            &LocalQosState::default(),
            &manifest,
            |_| unreachable!(),
        )
        .unwrap_err();

    assert!(error.to_string().contains("deferred"));
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].id, missing.id);
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[1].id, parse.id);
    assert_eq!(jobs[1].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[1].attempts, 0);

    let report = db
        .complete_next_background_external_content_artifact_job_for_runtime_with(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            &manifest,
            |job| {
                assert_eq!(job.id, parse.id);
                Ok(ExternalContentArtifactJobCompletion::new("parser")
                    .with_projection("search", "search:source-parse")
                    .with_rows_produced(2))
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, parse.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, missing.id);
}

#[test]
fn scheduled_specific_content_artifact_completion_releases_import_budget() {
    let mut db = Database::new();
    let first = db.schedule_external_content_artifact_job("source-1", "parse");
    let second = db.schedule_external_content_artifact_job("source-2", "parse");
    let scheduler = db.local_qos_scheduler();

    let report = db
        .complete_scheduled_background_external_content_artifact_job_with(
            second.id,
            |job| {
                assert_eq!(job.id, second.id);
                assert_eq!(job.status, DerivedArtifactJobStatus::Running);
                Ok(ExternalContentArtifactJobCompletion::new("parser")
                    .with_projection("search", "search:source-2")
                    .with_rows_produced(2))
            },
            3,
        )
        .unwrap()
        .unwrap();

    assert_eq!(report.job.id, second.id);
    assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert_eq!(
        report.output.rows[0].get("projection_ref"),
        Some(&Value::String("search:source-2".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("rows_produced"),
        Some(&Value::Int(2))
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        scheduler.state().running_background_operations_by_class
            [crate::WorkClass::Import.as_index()],
        0
    );

    let pending = db.pending_external_content_artifact_jobs(8);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, first.id);
}

#[test]
fn caller_owned_content_artifact_runtime_can_poll_and_run_by_action() {
    let mut db = Database::new();
    let crawl = db.schedule_external_content_artifact_job("source-crawl", "crawl");
    let parse = db.schedule_external_content_artifact_job_with_payload(
        "source-parse",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("file:///nowledge/source-parse.md".to_string()),
        )]),
    );
    db.schedule_projected_graph_artifact_rebuild("MissingGraph");

    let parse_pending = db.pending_external_content_artifact_jobs_for_action("parse", 8);
    assert_eq!(parse_pending.len(), 1);
    assert_eq!(parse_pending[0].id, parse.id);
    assert!(db
        .pending_external_content_artifact_jobs_for_action("embed", 8)
        .is_empty());

    let parse_report = db
        .run_next_external_content_artifact_job_for_action_with("parse", |job| {
            assert_eq!(job.id, parse.id);
            assert_eq!(job.action, "parse");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(parse_report.job.id, parse.id);
    assert_eq!(parse_report.job.status, DerivedArtifactJobStatus::Succeeded);
    assert!(db
        .run_next_external_content_artifact_job_for_action_with("parse", |_| unreachable!())
        .unwrap()
        .is_none());

    let remaining = db.pending_external_content_artifact_jobs(8);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, crawl.id);
    let crawl_report = db
        .run_next_external_content_artifact_job_with(|job| {
            assert_eq!(job.id, crawl.id);
            assert_eq!(job.action, "crawl");
            Ok(QueryOutput {
                rows: vec![BTreeMap::from([(
                    "job_id".to_string(),
                    Value::Int(job.id as i64),
                )])]
                .into(),
            })
        })
        .unwrap()
        .unwrap();
    assert_eq!(crawl_report.job.id, crawl.id);
}

#[test]
fn graph_kernel_rejects_external_content_jobs_with_payload_intact() {
    let mut db = Database::new();
    db.schedule_external_content_artifact_job_with_payload(
        "source-2",
        "parse",
        BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("s3://bucket/source-2.pdf".to_string()),
        )]),
    );

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(
        report.job.payload.get("content_uri"),
        Some(&Value::String("s3://bucket/source-2.pdf".to_string()))
    );
    assert_eq!(
        report.output.rows[0].get("payload"),
        Some(&Value::Map(BTreeMap::from([(
            "content_uri".to_string(),
            Value::String("s3://bucket/source-2.pdf".to_string())
        )])))
    );
    assert!(report
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("outside the graph kernel")));
}
