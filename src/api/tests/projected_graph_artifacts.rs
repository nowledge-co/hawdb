use super::*;

#[test]
fn projects_graph_for_page_rank() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();
    db.query("MERGE (:Entity {id: 3, name: 'Leaf'})-[:MENTIONS]->(:Entity {id: 4, name: 'Other'})")
        .unwrap();

    let links = db.project_graph(Some("LINKS"));
    assert_eq!(links.node_count(), 4);
    assert_eq!(links.edge_count(), 2);
    assert_eq!(
        links
            .incoming_sources(NodeId(2))
            .unwrap()
            .collect::<Vec<_>>(),
        vec![NodeId(1)]
    );

    let all = db.project_graph(None);
    assert_eq!(all.edge_count(), 3);
    assert_eq!(
        all.incoming_sources(NodeId(3)).unwrap().collect::<Vec<_>>(),
        vec![NodeId(2)]
    );

    let missing = db.project_graph(Some("MISSING"));
    assert_eq!(missing.node_count(), 4);
    assert_eq!(missing.edge_count(), 0);
    assert!(missing
        .incoming_sources(NodeId(2))
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());

    let scores = links.page_rank(Default::default());
    assert_eq!(scores[0].node.0, 2);

    let projection = db
        .query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
        .unwrap();
    assert_eq!(projection.rows[0].get("node_count"), Some(&Value::Int(4)));
    assert_eq!(projection.rows[0].get("edge_count"), Some(&Value::Int(2)));

    let projection = db
        .query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
        .unwrap();
    assert_eq!(projection.rows[0].get("node_count"), Some(&Value::Int(3)));
    assert_eq!(projection.rows[0].get("edge_count"), Some(&Value::Int(1)));

    let output = db
            .query(
                "CALL page_rank('EntityGraph', dampingFactor := 0.85, maxIterations := 20) RETURN node, pagerank_score",
            )
            .unwrap();
    assert_eq!(output.rows[0].get("node"), Some(&Value::Int(2)));
    let Some(Value::Float(score)) = output.rows[0].get("pagerank_score") else {
        panic!("expected pagerank_score float");
    };
    assert!(*score > 0.0);

    let output = db
            .query_with_params(
                "CALL page_rank('EntityGraph', dampingFactor := $damping, maxIterations := $iterations) RETURN node, pagerank_score",
                &BTreeMap::from([
                    ("damping".to_string(), Value::Float(0.85)),
                    ("iterations".to_string(), Value::Int(20)),
                ]),
            )
            .unwrap();
    assert_eq!(output.rows[0].get("node"), Some(&Value::Int(2)));

    let error = db
            .query_with_params(
                "CALL page_rank('EntityGraph', maxIterations := $iterations) RETURN node, pagerank_score",
                &BTreeMap::from([("iterations".to_string(), Value::Float(2.5))]),
            )
            .unwrap_err();
    assert!(error
        .to_string()
        .contains("maxIterations must be a non-negative integer"));

    let output = db
        .query("CALL louvain('EntityGraph') RETURN node, louvain_id")
        .unwrap();
    assert!(output.rows.iter().any(|row| {
        row.get("node") == Some(&Value::Int(3)) && row.get("louvain_id") == Some(&Value::Int(3))
    }));

    let output = db
        .query("CALL louvain('EntityGraph', maxLevels := 2) RETURN node, level, louvain_id")
        .unwrap();
    assert!(output
        .rows
        .iter()
        .any(|row| row.get("level") == Some(&Value::Int(1))));

    let output = db
        .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
        .unwrap();
    assert_eq!(output.rows.len(), 3);
    assert!(output
        .rows
        .iter()
        .all(|row| row.get("node") != Some(&Value::Int(0))));

    let error = db
        .query("CALL page_rank('MissingGraph') RETURN node, pagerank_score")
        .unwrap_err();
    assert!(error.to_string().contains("does not exist"));

    let communities = links
        .louvain_communities(Default::default())
        .into_iter()
        .map(|assignment| (assignment.node, assignment.community))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(communities[&NodeId(0)], NodeId(0));
    assert_eq!(communities[&NodeId(1)], NodeId(0));
    assert_eq!(communities[&NodeId(2)], NodeId(0));
    assert_eq!(communities[&NodeId(3)], NodeId(3));
}

#[test]
fn read_transaction_projects_snapshot_graph() {
    let mut db = Database::new();
    db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
        .unwrap();

    let read_tx = db.begin_read_transaction();
    db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
        .unwrap();

    assert_eq!(read_tx.project_graph(Some("LINKS")).edge_count(), 1);
    assert_eq!(db.project_graph(Some("LINKS")).edge_count(), 2);
}

#[test]
fn projected_graph_definition_replays_from_wal() {
    let path = unique_test_dir("projected_graph_wal");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| row.get("node") != Some(&Value::Int(0))));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_graph_definition_survives_checkpoint() {
    let path = unique_test_dir("projected_graph_checkpoint");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    assert_eq!(read_test_wal(&path).unwrap(), "");
    let checkpoint = read_test_durable_text(&active_checkpoint_path(&path)).unwrap();
    assert!(checkpoint.contains("project_graph"));

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityOnlyGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| row.get("node") != Some(&Value::Int(0))));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_writes_projected_graph_artifacts() {
    let path = unique_test_dir("projected_graph_artifacts");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("MERGE (:Entity {id: 2, name: 'Mid'})-[:LINKS]->(:Entity {id: 3, name: 'Leaf'})")
            .unwrap();
        db.query("CALL project_graph('EntityOnlyGraph', ['Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact = read_test_durable_text(&path.join("projected_graphs.skein")).unwrap();
    assert!(artifact.contains("SKEIN_PROJECTED_GRAPHS_V1\n"));
    assert!(artifact.contains("artifact_version\t1\n"));
    assert!(artifact.contains("projection_epoch\t1\n"));
    assert!(artifact.contains("commit_epoch\t4\n"));
    assert!(artifact.contains("graph\t456e746974794f6e6c794772617068"));
    assert!(artifact.contains("nodes\t1,2\n"));
    assert!(artifact.contains("csr_offsets\t0,1,1\n"));
    assert!(artifact.contains("csr_targets\t1\n"));
    assert!(artifact.contains("csc_offsets\t0,0,1\n"));
    assert!(artifact.contains("csc_sources\t0\n"));
    assert!(artifact.contains("checksum\t"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projected_graph_status_reports_artifact_reuse_state() {
    let path = unique_test_dir("projected_graph_status");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].name, "EntityGraph");
        assert_eq!(statuses[0].projection_epoch, Some(1));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(2));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].projection_epoch, None);
        assert!(!statuses[0].reusable);
        assert_eq!(statuses[0].node_count, None);
        assert_eq!(statuses[0].edge_count, None);
        db.rebuild_projected_graph_artifacts().unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(3));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].node_count, Some(3));
        assert_eq!(statuses[0].edge_count, Some(1));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn derived_artifact_rebuild_reports_projected_graph_refresh() {
    let path = unique_test_dir("derived_artifact_rebuild");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        let before = db.projected_graph_statuses();
        assert_eq!(before.len(), 1);
        assert!(!before[0].reusable);

        let output = db.rebuild_derived_artifacts().unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("artifact_type"),
            Some(&Value::String("projected_graph".to_string()))
        );
        assert_eq!(
            output.rows[0].get("name"),
            Some(&Value::String("EntityGraph".to_string()))
        );
        assert_eq!(
            output.rows[0].get("action"),
            Some(&Value::String("rebuilt".to_string()))
        );
        assert_eq!(
            output.rows[0].get("before_reusable"),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
        assert_eq!(output.rows[0].get("projection_epoch"), Some(&Value::Int(2)));
        assert_eq!(output.rows[0].get("node_count"), Some(&Value::Int(3)));
        assert_eq!(output.rows[0].get("edge_count"), Some(&Value::Int(1)));
    }
    {
        let db = Database::open(&path).unwrap();
        let statuses = db.projected_graph_statuses();
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].reusable);
        assert_eq!(statuses[0].projection_epoch, Some(2));
        assert_eq!(statuses[0].node_count, Some(3));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn derived_artifact_job_rebuilds_projected_graphs() {
    let path = unique_test_dir("derived_artifact_job_success");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        let job = db.schedule_derived_artifact_rebuild();
        assert_eq!(job.id, 1);
        assert_eq!(job.status, DerivedArtifactJobStatus::Pending);
        assert_eq!(db.derived_artifact_jobs().len(), 1);

        let report = db.run_next_derived_artifact_job().unwrap().unwrap();
        assert_eq!(report.job.id, 1);
        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(report.job.attempts, 1);
        assert!(report.job.last_error.is_none());
        assert_eq!(report.output.rows.len(), 1);
        assert_eq!(
            report.output.rows[0].get("name"),
            Some(&Value::String("EntityGraph".to_string()))
        );
        assert_eq!(
            report.output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            db.derived_artifact_jobs()[0].status,
            DerivedArtifactJobStatus::Succeeded
        );
        assert!(db.run_next_derived_artifact_job().unwrap().is_none());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_derived_artifact_job_uses_qos_admission() {
    let path = unique_test_dir("background_derived_artifact_job_deferred");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Root'})").unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory'], [])")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Later'})")
            .unwrap();

        let job = db.schedule_derived_artifact_rebuild();
        assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };
        let error = db
            .run_next_background_derived_artifact_job(&policy, &LocalQosState::default(), 2)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        let jobs = db.derived_artifact_jobs();
        assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
        assert_eq!(jobs[0].attempts, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn background_derived_artifact_job_runs_when_qos_admits() {
    let path = unique_test_dir("background_derived_artifact_job_admitted");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let report = db
            .run_next_background_derived_artifact_job(
                &LocalQosPolicy::default(),
                &LocalQosState::default(),
                2,
            )
            .unwrap()
            .unwrap();

        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(report.job.attempts, 1);
        assert_eq!(report.output.rows.len(), 1);
        assert_eq!(
            report.output.rows[0].get("after_reusable"),
            Some(&Value::Bool(true))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_tracks_running_budget() {
    let path = unique_test_dir("scheduled_background_derived_artifact_job");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Memory {id: 3, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });

        let report = db
            .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 4)
            .unwrap()
            .unwrap();

        assert_eq!(report.job.status, DerivedArtifactJobStatus::Succeeded);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_defers_when_scheduler_is_full() {
    let path = unique_test_dir("scheduled_background_derived_artifact_job_full");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 1, title: 'Root'})").unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory'], [])")
            .unwrap();
        db.query("CREATE (:Memory {id: 2, title: 'Later'})")
            .unwrap();

        db.schedule_derived_artifact_rebuild();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(8),
            max_total_background_operations: Some(8),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(crate::WorkRequest::background(
                crate::WorkClass::Analytics,
                6,
            ))
            .unwrap();

        let error = db
            .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 4)
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(scheduler.state().running_background_operations, 6);
        assert_eq!(
            db.derived_artifact_jobs()[0].status,
            DerivedArtifactJobStatus::Pending
        );

        scheduler.finish(running);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scheduled_background_derived_artifact_job_releases_budget_on_execution_error() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });
    db.schedule_derived_artifact_rebuild();
    let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = db
        .run_next_scheduled_background_derived_artifact_job(&mut scheduler, 1)
        .unwrap_err();

    assert!(error.to_string().contains("read-only mode"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    let jobs = db.derived_artifact_jobs();
    assert_eq!(jobs[0].status, DerivedArtifactJobStatus::Pending);
    assert_eq!(jobs[0].attempts, 0);
}

#[test]
fn derived_artifact_job_reports_unknown_projected_graph_failure() {
    let mut db = Database::new();
    let job = db.schedule_projected_graph_artifact_rebuild("MissingGraph");
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);

    let report = db.run_next_derived_artifact_job().unwrap().unwrap();

    assert_eq!(report.job.status, DerivedArtifactJobStatus::Failed);
    assert_eq!(report.job.attempts, 1);
    assert!(report
        .job
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("unknown projected graph artifact")));
    assert_eq!(
        report.output.rows[0].get("status"),
        Some(&Value::String("failed".to_string()))
    );
    assert!(report.output.rows[0].get("error").is_some_and(|value| value
        == &Value::String(
            "semantic error: unknown projected graph artifact 'MissingGraph'".to_string()
        )));
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Failed
    );
}

#[test]
fn corrupt_projected_graph_artifacts_do_not_block_recovery() {
    let path = unique_test_dir("projected_graph_artifact_corrupt");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact_path = path.join("projected_graphs.skein");
    let artifact = read_test_durable_text(&artifact_path).unwrap();
    std::fs::write(
        &artifact_path,
        artifact.replace("csr_targets", "bad_targets"),
    )
    .unwrap();

    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query("CALL page_rank('EntityGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
    }
    assert!(!artifact_path.exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_open_ignores_corrupt_projected_graph_artifact_without_cleanup() {
    let path = unique_test_dir("projected_graph_artifact_corrupt_read_only");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("MERGE (:Memory {id: 1, title: 'Root'})-[:LINKS]->(:Entity {id: 2, name: 'Mid'})")
            .unwrap();
        db.query("CALL project_graph('EntityGraph', ['Memory', 'Entity'], ['LINKS'])")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let artifact_path = path.join("projected_graphs.skein");
    let artifact = read_test_durable_text(&artifact_path).unwrap();
    std::fs::write(
        &artifact_path,
        artifact.replace("csr_targets", "bad_targets"),
    )
    .unwrap();

    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let output = db
            .query("CALL page_rank('EntityGraph') RETURN node, pagerank_score")
            .unwrap();
        assert_eq!(output.rows.len(), 2);
    }
    assert!(artifact_path.exists());
    std::fs::remove_dir_all(path).unwrap();
}
