use super::*;

#[test]
fn interrupts_pending_and_running_augmentation_jobs_for_nowledge_orphans() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
        .unwrap();
    db.query(
        "CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed', message: 'done'})",
    )
    .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'failed_job', status: 'failed', message: 'old failure'})")
        .unwrap();

    let output = db
        .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
            error_message: "stale owner".to_string(),
            completed_at: Value::Int(300),
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.candidate_count, 2);
    assert_eq!(output.interrupted_count, 2);
    assert_eq!(output.updated_property_count, 8);
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0].job_id.as_deref(), Some("pending_job"));
    assert_eq!(output.rows[0].previous_status, "pending");
    assert_eq!(output.rows[1].job_id.as_deref(), Some("running_job"));
    assert_eq!(output.rows[1].previous_status, "running");

    let rows = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.message AS message, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
        .unwrap();
    let completed = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("completed_job".to_string())))
        .unwrap();
    assert_eq!(
        completed.get("status"),
        Some(&Value::String("completed".to_string()))
    );
    assert_eq!(
        completed.get("message"),
        Some(&Value::String("done".to_string()))
    );

    for job_id in ["pending_job", "running_job"] {
        let row = rows
            .rows
            .iter()
            .find(|row| row.get("id") == Some(&Value::String(job_id.to_string())))
            .unwrap();
        assert_eq!(
            row.get("status"),
            Some(&Value::String("failed".to_string()))
        );
        assert_eq!(
            row.get("message"),
            Some(&Value::String("Interrupted before completion".to_string()))
        );
        assert_eq!(
            row.get("error"),
            Some(&Value::String("stale owner".to_string()))
        );
        assert_eq!(row.get("completed_at"), Some(&Value::Int(300)));
    }
}

#[test]
fn augmentation_job_interrupt_without_candidates_does_not_write_wal() {
    let path = unique_test_dir("augmentation_job_interrupt_without_candidates");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let graph_commit_epoch_before = db.store.commit_epoch();
        let output = db
            .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
                error_message: "no candidates".to_string(),
                completed_at: Value::Int(301),
            })
            .unwrap();
        assert_eq!(output.graph_commit_epoch_before, graph_commit_epoch_before);
        assert_eq!(output.graph_commit_epoch_after, graph_commit_epoch_before);
        assert_eq!(output.candidate_count, 0);
        assert_eq!(output.interrupted_count, 0);
        assert_eq!(output.updated_property_count, 0);
    }
    let wal_after = read_test_wal(&path).unwrap();
    assert_eq!(wal_after, wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn augmentation_job_interrupt_rejects_empty_reason_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
            error_message: String::new(),
            completed_at: Value::Int(302),
        })
        .unwrap_err();

    assert!(error.to_string().contains("non-empty error message"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_augmentation_job_interrupt_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_augmentation_job_interrupt_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
            .unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
            .unwrap();
    }
    let setup_wal = read_test_wal(&path).unwrap();
    let setup_batch_count = setup_wal.matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .interrupt_knowledge_augmentation_jobs(&KnowledgeAugmentationJobInterruptRequest {
                error_message: "shutdown".to_string(),
                completed_at: Value::Int(303),
            })
            .unwrap();
        assert_eq!(output.interrupted_count, 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    assert_eq!(wal.matches("\tbatch\t").count(), setup_batch_count + 1);
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.message AS message, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for row in &rows.rows {
            assert_eq!(
                row.get("status"),
                Some(&Value::String("failed".to_string()))
            );
            assert_eq!(
                row.get("message"),
                Some(&Value::String("Interrupted before completion".to_string()))
            );
            assert_eq!(
                row.get("error"),
                Some(&Value::String("shutdown".to_string()))
            );
            assert_eq!(row.get("completed_at"), Some(&Value::Int(303)));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
