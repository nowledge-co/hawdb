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
fn updates_augmentation_job_lifecycle_batch_for_nowledge_shapes() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'pending_job', status: 'pending'})")
        .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'running_progress', status: 'running', progress: 1.0, message: 'old'})")
        .unwrap();
    db.query(
        "CREATE (:AugmentationJob {job_id: 'running_complete', status: 'running', progress: 50.0})",
    )
    .unwrap();
    db.query("CREATE (:AugmentationJob {job_id: 'completed_job', status: 'completed'})")
        .unwrap();

    let output = db
        .update_knowledge_augmentation_jobs_batch(&KnowledgeAugmentationJobLifecycleBatchRequest {
            updates: vec![
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "created_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                        job_type: "pagerank".to_string(),
                        parameters: Value::String("{\"graph\":\"main\"}".to_string()),
                        created_at: Value::Int(100),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "pending_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkRunning {
                        started_at: Value::Int(101),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "running_progress".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::UpdateProgress {
                        progress: 42.5,
                        message: "halfway".to_string(),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "running_complete".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkCompleted {
                        result: Value::String("{\"ok\":true}".to_string()),
                        completed_at: Value::Int(102),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "completed_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                        error_message: "too late".to_string(),
                        completed_at: Value::Int(103),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "missing_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                        error_message: "missing".to_string(),
                        completed_at: Value::Int(104),
                    },
                },
                KnowledgeAugmentationJobLifecycleUpdate {
                    job_id: "created_job".to_string(),
                    transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                        job_type: "duplicate".to_string(),
                        parameters: Value::String("{}".to_string()),
                        created_at: Value::Int(105),
                    },
                },
            ],
        })
        .unwrap();

    assert_eq!(output.graph_commit_epoch_before, 4);
    assert_eq!(output.graph_commit_epoch_after, 5);
    assert_eq!(output.created_count, 1);
    assert_eq!(output.updated_count, 3);
    assert_eq!(output.missing_count, 1);
    assert_eq!(output.already_exists_count, 0);
    assert_eq!(output.status_mismatch_count, 1);
    assert_eq!(output.duplicate_count, 1);
    assert_eq!(output.updated_property_count, 20);
    assert!(output.rows[0].created);
    assert!(output.rows[1].updated);
    assert!(output.rows[4].status_mismatch);
    assert!(output.rows[5].missing);
    assert!(output.rows[6].duplicate);

    let rows = db
        .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.progress AS progress, j.message AS message, j.result AS result, j.error_message AS error, j.started_at AS started_at, j.completed_at AS completed_at, j.created_at AS created_at ORDER BY id ASC")
        .unwrap();
    let created = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("created_job".to_string())))
        .unwrap();
    assert_eq!(
        created.get("status"),
        Some(&Value::String("pending".to_string()))
    );
    assert_eq!(created.get("progress"), Some(&Value::Float(0.0)));
    assert_eq!(
        created.get("message"),
        Some(&Value::String("Job created".to_string()))
    );
    assert_eq!(created.get("created_at"), Some(&Value::Int(100)));

    let pending = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("pending_job".to_string())))
        .unwrap();
    assert_eq!(
        pending.get("status"),
        Some(&Value::String("running".to_string()))
    );
    assert_eq!(pending.get("started_at"), Some(&Value::Int(101)));

    let progress = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("running_progress".to_string())))
        .unwrap();
    assert_eq!(progress.get("progress"), Some(&Value::Float(42.5)));
    assert_eq!(
        progress.get("message"),
        Some(&Value::String("halfway".to_string()))
    );

    let completed = rows
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::String("running_complete".to_string())))
        .unwrap();
    assert_eq!(
        completed.get("status"),
        Some(&Value::String("completed".to_string()))
    );
    assert_eq!(completed.get("progress"), Some(&Value::Float(100.0)));
    assert_eq!(
        completed.get("message"),
        Some(&Value::String("Job completed successfully".to_string()))
    );
    assert_eq!(
        completed.get("result"),
        Some(&Value::String("{\"ok\":true}".to_string()))
    );
    assert_eq!(completed.get("completed_at"), Some(&Value::Int(102)));
}

#[test]
fn augmentation_job_progress_rejects_invalid_percentage_before_wal() {
    let mut db = Database::new();
    db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
        .unwrap();
    let graph_commit_epoch_before = db.store.commit_epoch();

    let error = db
        .update_knowledge_augmentation_jobs_batch(&KnowledgeAugmentationJobLifecycleBatchRequest {
            updates: vec![KnowledgeAugmentationJobLifecycleUpdate {
                job_id: "running_job".to_string(),
                transition: KnowledgeAugmentationJobLifecycleTransition::UpdateProgress {
                    progress: 101.0,
                    message: "bad".to_string(),
                },
            }],
        })
        .unwrap_err();

    assert!(error.to_string().contains("finite percentage"));
    assert_eq!(db.store.commit_epoch(), graph_commit_epoch_before);
}

#[test]
fn typed_augmentation_job_lifecycle_persists_as_one_wal_batch_and_replays() {
    let path = unique_test_dir("typed_augmentation_job_lifecycle_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:AugmentationJob {job_id: 'running_job', status: 'running'})")
            .unwrap();
        let batch_count_before_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        db.update_knowledge_augmentation_jobs_batch(
            &KnowledgeAugmentationJobLifecycleBatchRequest {
                updates: vec![
                    KnowledgeAugmentationJobLifecycleUpdate {
                        job_id: "created_job".to_string(),
                        transition: KnowledgeAugmentationJobLifecycleTransition::Create {
                            job_type: "louvain".to_string(),
                            parameters: Value::String("{}".to_string()),
                            created_at: Value::Int(10),
                        },
                    },
                    KnowledgeAugmentationJobLifecycleUpdate {
                        job_id: "running_job".to_string(),
                        transition: KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
                            error_message: "runtime failed".to_string(),
                            completed_at: Value::Int(20),
                        },
                    },
                ],
            },
        )
        .unwrap();
        let batch_count_after_update = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        assert_eq!(batch_count_after_update, batch_count_before_update + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("create_node"));
    assert!(wal.contains("set_node_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query("MATCH (j:AugmentationJob) RETURN j.job_id AS id, j.status AS status, j.job_type AS job_type, j.error_message AS error, j.completed_at AS completed_at ORDER BY id ASC")
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.rows[0].get("id"),
            Some(&Value::String("created_job".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("status"),
            Some(&Value::String("pending".to_string()))
        );
        assert_eq!(
            rows.rows[0].get("job_type"),
            Some(&Value::String("louvain".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("id"),
            Some(&Value::String("running_job".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("status"),
            Some(&Value::String("failed".to_string()))
        );
        assert_eq!(
            rows.rows[1].get("error"),
            Some(&Value::String("runtime failed".to_string()))
        );
        assert_eq!(rows.rows[1].get("completed_at"), Some(&Value::Int(20)));
    }
    std::fs::remove_dir_all(path).unwrap();
}
