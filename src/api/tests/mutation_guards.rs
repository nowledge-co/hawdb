// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

#[test]
fn match_set_return_projects_updated_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Thread {id: 'storage-1', thread_id: 'logical-1', space_id: 'default'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'storage-2', thread_id: 'logical-2', space_id: 'default'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (t:Thread) WHERE t.thread_id IN $thread_ids SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id AS thread_id, t.space_id AS space_id",
            &BTreeMap::from([
                (
                    "thread_ids".to_string(),
                    Value::List(vec![
                        Value::String("logical-1".to_string()),
                        Value::String("logical-2".to_string()),
                    ]),
                ),
                (
                    "target_space_id".to_string(),
                    Value::String("archive".to_string()),
                ),
                ("updated_at".to_string(), Value::Int(42)),
            ]),
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![
            BTreeMap::from([
                (
                    "thread_id".to_string(),
                    Value::String("logical-1".to_string())
                ),
                ("space_id".to_string(), Value::String("archive".to_string())),
            ]),
            BTreeMap::from([
                (
                    "thread_id".to_string(),
                    Value::String("logical-2".to_string())
                ),
                ("space_id".to_string(), Value::String("archive".to_string())),
            ]),
        ]
    );
}

#[test]
fn match_set_return_counts_updated_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending-job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running-job', status: 'running'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'completed-job', status: 'completed'})")
        .unwrap();

    let output = db
        .query_with_params(
            "MATCH (j:AugmentationJob) WHERE j.status = 'pending' OR j.status = 'running' SET j.status = 'failed', j.error_message = $reason RETURN count(j)",
            &BTreeMap::from([(
                "reason".to_string(),
                Value::String("restart".to_string()),
            )]),
        )
        .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("count(j)".to_string(), Value::Int(2))])]
    );

    let status = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id, j.status ORDER BY j.job_id")
        .unwrap();
    assert_eq!(
        status.rows,
        vec![
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("completed-job".to_string())
                ),
                (
                    "j.status".to_string(),
                    Value::String("completed".to_string())
                ),
            ]),
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("pending-job".to_string())
                ),
                ("j.status".to_string(), Value::String("failed".to_string())),
            ]),
            BTreeMap::from([
                (
                    "j.job_id".to_string(),
                    Value::String("running-job".to_string())
                ),
                ("j.status".to_string(), Value::String("failed".to_string())),
            ]),
        ]
    );
}

#[test]
fn mutation_affected_row_limit_rejects_set_return_atomically() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(1).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {id: 'one', state: 'ready'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'two', state: 'ready'})")
        .unwrap();
    let epoch = db.statistics().computed_at_commit_epoch;

    let error = db
        .query("MATCH (t:Thread) SET t.state = 'changed' RETURN t.id AS id")
        .unwrap_err();

    assert!(error.to_string().contains("max_mutation_affected_rows 1"));
    assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
    let rows = db
        .query("MATCH (t:Thread) RETURN t.id AS id, t.state AS state ORDER BY id")
        .unwrap();
    assert!(rows
        .rows
        .iter()
        .all(|row| { row.get("state") == Some(&Value::String("ready".to_string())) }));
}

#[test]
fn mutation_payload_limit_rejects_set_return_before_commit() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_result_payload_bytes = std::num::NonZeroUsize::new(32).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {id: 'payload', state: 'ready'})")
        .unwrap();
    let epoch = db.statistics().computed_at_commit_epoch;

    let error = db
        .query("MATCH (t:Thread) SET t.state = 'this-payload-is-larger-than-the-configured-limit' RETURN t.state AS state")
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("max_mutation_result_payload_bytes 32"));
    assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
    let rows = db
        .query("MATCH (t:Thread {id: 'payload'}) RETURN t.state AS state")
        .unwrap();
    assert_eq!(
        rows.rows[0].get("state"),
        Some(&Value::String("ready".to_string()))
    );
}

#[test]
fn transaction_mutation_return_error_restores_the_statement_savepoint() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_result_payload_bytes = std::num::NonZeroUsize::new(32).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {id: 'payload', state: 'ready'})")
        .unwrap();

    let mut tx = db.begin_transaction();
    let error = tx
        .query("MATCH (t:Thread) SET t.state = 'this-payload-is-larger-than-the-configured-limit' RETURN t.state AS state")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("max_mutation_result_payload_bytes 32"));
    let staged = tx
        .query("MATCH (t:Thread {id: 'payload'}) RETURN t.state AS state")
        .unwrap();
    assert_eq!(
        staged.rows[0].get("state"),
        Some(&Value::String("ready".to_string()))
    );
    tx.rollback();
}

#[test]
fn mutation_count_return_uses_result_limit_independently_of_affected_rows() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_result_rows = std::num::NonZeroUsize::new(1).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {id: 'one', state: 'ready'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'two', state: 'ready'})")
        .unwrap();

    let output = db
        .query("MATCH (t:Thread) SET t.state = 'changed' RETURN count(t)")
        .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("count(t)".to_string(), Value::Int(2))])]
    );
}

#[test]
fn transaction_mutation_count_return_uses_projected_result_limit() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_result_rows = std::num::NonZeroUsize::new(1).unwrap();
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Thread {id: 'one', state: 'ready'})")
        .unwrap();
    db.query("CREATE (:Thread {id: 'two', state: 'ready'})")
        .unwrap();

    let mut tx = db.begin_transaction();
    let output = tx
        .query("MATCH (t:Thread) SET t.state = 'changed' RETURN count(t)")
        .unwrap();
    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("count(t)".to_string(), Value::Int(2))])]
    );
    tx.commit().unwrap();

    let changed = db
        .query("MATCH (t:Thread) WHERE t.state = 'changed' RETURN count(t)")
        .unwrap();
    assert_eq!(changed.rows[0].get("count(t)"), Some(&Value::Int(2)));
}

#[test]
fn transaction_operation_limit_rejects_the_complete_batch() {
    let mut config = DatabaseConfig::default();
    config.mutation_limits.max_operations = std::num::NonZeroUsize::new(1).unwrap();
    let mut db = Database::new_with_config(config);
    let mut tx = db.begin_transaction();
    tx.query("CREATE (:Memory {id: 'one'})").unwrap();
    tx.query("CREATE (:Memory {id: 'two'})").unwrap();

    let error = tx.commit().unwrap_err();

    assert!(error.to_string().contains("max_mutation_operations 1"));
    let rows = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap();
    assert!(rows.rows.is_empty());
}

#[test]
fn wal_batch_limit_rejects_transaction_before_append() {
    let path = unique_test_dir("wal_batch_limit_rejects_transaction_before_append");
    let config = DatabaseConfig {
        max_wal_batch_operations: Some(1),
        ..DatabaseConfig::default()
    };
    let epoch;
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        epoch = db.statistics().computed_at_commit_epoch;
        let mut tx = db.begin_transaction();
        tx.query("CREATE (:Memory {id: 'one'})").unwrap();
        tx.query("CREATE (:Memory {id: 'two'})").unwrap();

        let error = tx.commit().unwrap_err();

        assert!(error
            .to_string()
            .contains("WAL batch operation limit exceeded before append"));
        assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
        assert!(db
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }
    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
        assert!(db
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_record_byte_limit_rejects_mutation_before_append() {
    let path = unique_test_dir("wal_record_byte_limit_rejects_mutation_before_append");
    {
        let mut db = Database::open(&path).unwrap();
        db.checkpoint().unwrap();
    }
    let config = DatabaseConfig {
        max_wal_record_bytes: Some(256),
        ..DatabaseConfig::default()
    };
    let epoch;
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        epoch = db.statistics().computed_at_commit_epoch;
        let payload = "x".repeat(512);

        let error = db
            .query_with_params(
                "CREATE (:Memory {id: 'oversized', content: $payload})",
                &BTreeMap::from([("payload".to_string(), Value::String(payload))]),
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("WAL record byte limit exceeded before append"));
        assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
    }
    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_eq!(db.statistics().computed_at_commit_epoch, epoch);
        assert!(db
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_match_set_return() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let error = db
        .query("MATCH (t:Thread) SET t.space_id = 'archive' RETURN t.thread_id")
        .unwrap_err();
    assert!(error.to_string().contains("read-only mode"));
}

#[test]
fn read_only_database_rejects_transaction_mutations() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let mut tx = db.begin_transaction();
    let error = tx.query("CREATE (:Memory {id: 1})").unwrap_err();
    assert!(error.to_string().contains("read-only mode"));

    let commit = tx.commit().unwrap_err();
    assert!(commit.to_string().contains("read-only mode"));
}

#[test]
fn read_only_database_rejects_owned_maintenance_writes() {
    let mut db = Database::new_with_config(DatabaseConfig {
        read_only: true,
        ..DatabaseConfig::default()
    });

    let checkpoint = db.checkpoint().unwrap_err();
    assert!(checkpoint.to_string().contains("read-only mode"));

    let maintenance = db.run_schema_maintenance().unwrap_err();
    assert!(maintenance.to_string().contains("read-only mode"));

    let rebuild = db.rebuild_projected_graph_artifacts().unwrap_err();
    assert!(rebuild.to_string().contains("read-only mode"));

    let job = db.schedule_derived_artifact_rebuild();
    assert_eq!(job.status, DerivedArtifactJobStatus::Pending);
    let run = db.run_next_derived_artifact_job().unwrap_err();
    assert!(run.to_string().contains("read-only mode"));
    assert_eq!(
        db.derived_artifact_jobs()[0].status,
        DerivedArtifactJobStatus::Pending
    );
}
