// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A deferred local QoS background admission reaches the caller as a typed,
//! retryable `RuntimeAdmissionRefused`, never as a storage error (#1926). For
//! every database entry point: the refusal runs and writes nothing, the
//! handle stays usable and unpoisoned, and the same call succeeds once the
//! competing background work ends.

use super::*;
use crate::DerivedArtifactJob;

/// Background operations held by competing work in each case.
const HELD: usize = 100_000;

fn busy_policy() -> LocalQosPolicy {
    LocalQosPolicy {
        max_background_operations: None,
        max_total_background_operations: Some(HELD),
        ..LocalQosPolicy::default()
    }
}

fn busy_state() -> LocalQosState {
    LocalQosState {
        running_background_operations: HELD,
        ..LocalQosState::default()
    }
}

fn config() -> DatabaseConfig {
    DatabaseConfig {
        local_qos_policy: busy_policy(),
        ..DatabaseConfig::default()
    }
}

#[track_caller]
fn assert_deferred(db: &mut Database, error: &HawDBError) {
    assert!(
        matches!(
            error,
            HawDBError::RuntimeAdmissionRefused(crate::RuntimeAdmissionRefusal {
                code: "background_task_saturated",
                requested: 1..,
                available: 0,
                retryable: true,
            })
        ),
        "{error}"
    );
    assert!(!db.storage_handle_poisoned().unwrap());
    db.query("MATCH (n) RETURN count(n) AS count").unwrap();
}

/// The database's own scheduler is full: the call is deferred, then succeeds
/// once the competing work finishes.
#[track_caller]
fn scheduled<T>(db: &mut Database, mut run: impl FnMut(&mut Database) -> Result<T>) -> T {
    let scheduler = db.local_qos_scheduler();
    let held = scheduler
        .try_start(WorkRequest::background(WorkClass::Analytics, HELD))
        .unwrap();
    let error = run(db).err().expect("a full scheduler defers the work");
    assert_deferred(db, &error);
    assert_eq!(scheduler.state().running_background_operations, HELD);
    held.finish();
    let output = run(db).unwrap();
    assert_eq!(scheduler.state(), LocalQosState::default());
    output
}

/// The caller's background state is full: the call is deferred, then
/// succeeds against a state with free capacity.
#[track_caller]
fn admitted<T>(
    db: &mut Database,
    mut run: impl FnMut(&mut Database, &LocalQosPolicy, &LocalQosState) -> Result<T>,
) -> T {
    let error = run(db, &busy_policy(), &busy_state())
        .err()
        .expect("a full background state defers the work");
    assert_deferred(db, &error);
    run(db, &busy_policy(), &LocalQosState::default()).unwrap()
}

fn empty_output(_: &DerivedArtifactJob) -> Result<QueryOutput> {
    Ok(QueryOutput {
        rows: Vec::new().into(),
    })
}

#[test]
fn search_projection_entry_points_defer_typed_and_retry() {
    let mut db = Database::new_with_config(config());
    db.query("CREATE (:Memory {id: 'mem_1', title: 'mem_1', content: 'deferred work'})")
        .unwrap();
    let delta = || SearchProjectionDelta {
        upserts: vec![search_projection_row("new", "New", "Applied on retry")],
        deletes: vec!["memory:old".to_string()],
        max_operations: Some(2),
        source_graph_commit_epoch: None,
    };
    let fresh_index = || {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(search_projection_row("old", "Old", "Removed on retry"))
            .unwrap();
        index
    };

    let mut index = fresh_index();
    admitted(&mut db, |db, policy, state| {
        db.apply_background_search_projection_delta(&mut index, policy, state, delta())
    });
    assert!(index.document("memory:new").is_some());
    let mut index = fresh_index();
    scheduled(&mut db, |db| {
        db.apply_scheduled_background_search_projection_delta(&mut index, delta())
    });
    assert!(index.document("memory:new").is_some());

    let mut index = fresh_index();
    admitted(&mut db, |db, policy, state| {
        db.rebuild_background_search_projection(
            &mut index,
            policy,
            state,
            SearchRebuildOptions::default(),
        )
    });
    assert!(index.document("memory:old").is_none());
    let mut index = fresh_index();
    scheduled(&mut db, |db| {
        db.rebuild_scheduled_background_search_projection(
            &mut index,
            SearchRebuildOptions::default(),
        )
    });
    assert!(index.document("memory:old").is_none());

    admitted(&mut db, |db, policy, state| {
        db.repair_background_search_projection_metadata(
            &mut index,
            policy,
            state,
            MetadataRepairOptions::default(),
            1,
        )
    });
    scheduled(&mut db, |db| {
        db.repair_scheduled_background_search_projection_metadata(
            &mut index,
            MetadataRepairOptions::default(),
            1,
        )
    });

    let node_id = |db: &mut Database| match db
        .query("MATCH (m:Memory {id: 'mem_1'}) RETURN id(m) AS id")
        .unwrap()
        .rows[0]
        .get("id")
    {
        Some(Value::Int(id)) => *id as u64,
        other => panic!("unexpected node id {other:?}"),
    };
    let request = |db: &mut Database| SearchProjectionGraphDeltaRequest {
        upsert_node_ids: vec![node_id(db)],
        delete_document_ids: vec!["memory:old".to_string()],
        max_operations: Some(2),
        complete_through_graph_commit_epoch: Some(db.commit_epoch().unwrap()),
    };
    let mut index = fresh_index();
    admitted(&mut db, |db, policy, state| {
        let request = request(db);
        db.apply_background_search_projection_graph_delta(&mut index, policy, state, request)
    });
    assert!(index.document("memory:mem_1").is_some());
    let mut index = fresh_index();
    scheduled(&mut db, |db| {
        let request = request(db);
        db.apply_scheduled_background_search_projection_graph_delta(&mut index, request)
    });
    assert!(index.document("memory:mem_1").is_some());
}

#[test]
fn artifact_job_entry_points_defer_typed_and_retry() {
    let mut db = Database::new_with_config(config());
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let manifest = ExternalContentArtifactRuntimeManifest::new("parser")
        .with_supported_action("parse")
        .with_estimated_operations(1);

    db.schedule_external_content_artifact_job("source-1", "parse");
    admitted(&mut db, |db, policy, state| {
        db.run_next_background_external_content_artifact_job_with(policy, state, empty_output, 1)
    })
    .unwrap();
    db.schedule_external_content_artifact_job("source-2", "parse");
    scheduled(&mut db, |db| {
        db.run_next_scheduled_background_external_content_artifact_job_with(empty_output, 1)
    })
    .unwrap();

    db.schedule_external_content_artifact_job("source-3", "parse");
    admitted(&mut db, |db, policy, state| {
        db.run_next_background_external_content_artifact_job_for_action_with(
            policy,
            state,
            "parse",
            empty_output,
            1,
        )
    })
    .unwrap();
    db.schedule_external_content_artifact_job("source-4", "parse");
    scheduled(&mut db, |db| {
        db.run_next_scheduled_background_external_content_artifact_job_for_action_with(
            "parse",
            empty_output,
            1,
        )
    })
    .unwrap();

    db.schedule_external_content_artifact_job("source-5", "parse");
    admitted(&mut db, |db, policy, state| {
        db.run_next_background_external_content_artifact_job_for_runtime_with(
            policy,
            state,
            &manifest,
            empty_output,
        )
    })
    .unwrap();
    db.schedule_external_content_artifact_job("source-6", "parse");
    scheduled(&mut db, |db| {
        db.run_next_scheduled_background_external_content_artifact_job_for_runtime_with(
            &manifest,
            empty_output,
        )
    })
    .unwrap();

    let job = db.schedule_external_content_artifact_job("source-7", "parse");
    admitted(&mut db, |db, policy, state| {
        db.run_background_external_content_artifact_job_with(policy, state, job.id, empty_output, 1)
    })
    .unwrap();
    let job = db.schedule_external_content_artifact_job("source-8", "parse");
    scheduled(&mut db, |db| {
        db.run_scheduled_background_external_content_artifact_job_with(job.id, empty_output, 1)
    })
    .unwrap();
    assert!(db
        .derived_artifact_jobs()
        .iter()
        .all(|job| job.status == DerivedArtifactJobStatus::Succeeded));

    db.schedule_derived_artifact_rebuild();
    admitted(&mut db, |db, policy, state| {
        db.run_next_background_derived_artifact_job(policy, state, 1)
    })
    .unwrap();
    db.schedule_derived_artifact_rebuild();
    scheduled(&mut db, |db| {
        db.run_next_scheduled_background_derived_artifact_job(1)
    })
    .unwrap();
}

#[test]
fn schema_and_index_maintenance_entry_points_defer_typed_and_retry() {
    let backfill = || {
        let mut db = Database::new_with_config(config());
        db.query("CREATE NODE TABLE Memory").unwrap();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        db.query("CREATE (:Memory {id: 2})").unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE Memory(id) TYPE INT NOT NULL")
            .unwrap();
        db.query("ALTER PROPERTY ON NODE TABLE Memory(id) SET STATE BACKFILL")
            .unwrap();
        db
    };
    let advanced = |output: QueryOutput| assert!(!output.rows.is_empty());

    advanced(admitted(&mut backfill(), |db, policy, state| {
        db.run_background_schema_maintenance(policy, state, 2)
    }));
    advanced(admitted(&mut backfill(), |db, policy, state| {
        db.run_bounded_background_schema_maintenance(policy, state, 10)
    }));
    advanced(admitted(&mut backfill(), |db, policy, state| {
        db.run_planned_background_schema_maintenance(policy, state)
    }));
    advanced(scheduled(&mut backfill(), |db| {
        db.run_scheduled_background_schema_maintenance(2)
    }));
    advanced(scheduled(&mut backfill(), |db| {
        db.run_bounded_scheduled_background_schema_maintenance(10)
    }));

    let indexed = || {
        let mut db = Database::new_with_config(config());
        db.query("CREATE (:Memory {kind: 'note', source_id: 'a', title: 'Graph'})")
            .unwrap();
        db.query("CREATE (:Memory {kind: 'note', source_id: 'b', title: 'Vector'})")
            .unwrap();
        db.query("CREATE INDEX ON :Memory(kind, source_id)")
            .unwrap();
        db
    };
    advanced(admitted(&mut indexed(), |db, policy, state| {
        db.rebuild_bounded_background_property_index_projections(policy, state, 2)
    }));
    advanced(scheduled(&mut indexed(), |db| {
        db.rebuild_bounded_scheduled_background_property_index_projections(2)
    }));
}

#[test]
fn bootstrap_export_entry_points_defer_typed_and_retry() {
    for scheduled_path in [false, true] {
        let path = unique_test_dir(&format!("qos_refusal_bootstrap_{scheduled_path}"));
        {
            let mut db = Database::open_with_config(&path, config()).unwrap();
            db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
                .unwrap();
            let export = if scheduled_path {
                scheduled(&mut db, |db| {
                    db.prepare_scheduled_background_hawdb_lightning_bootstrap_export()
                })
            } else {
                admitted(&mut db, |db, policy, state| {
                    db.prepare_background_hawdb_lightning_bootstrap_export(policy, state)
                })
            };
            assert_eq!(export.manifest.node_count, 2);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn optimizer_statistics_refresh_entry_points_defer_typed_and_retry() {
    for scheduled_path in [false, true] {
        let path = unique_test_dir(&format!("qos_refusal_statistics_{scheduled_path}"));
        let spill_root = path.with_extension("spill");
        std::fs::create_dir_all(&spill_root).unwrap();
        {
            let mut db = Database::open_with_config(
                &path,
                DatabaseConfig {
                    storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
                    segment_cache_capacity_bytes: 1024 * 1024,
                    ..config()
                },
            )
            .unwrap();
            db.query("CREATE NODE TABLE Memory").unwrap();
            db.query("CREATE PROPERTY ON NODE TABLE Memory(kind) TYPE TEXT")
                .unwrap();
            for id in 0..4 {
                db.query(&format!("CREATE (:Memory {{id: {id}, kind: 'note'}})"))
                    .unwrap();
            }
            db.checkpoint().unwrap();
            db.query("CREATE INDEX ON :Memory(kind)").unwrap();
            let options = crate::OptimizerStatisticsRefreshOptions {
                memory_budget_bytes: 4096,
                max_spill_bytes: 1024 * 1024,
                max_spill_runs: 64,
                max_input_records: 1_000,
                max_generated_facts: 10_000,
                max_path_expansions: 1_000,
                spill_directory: spill_root.clone(),
            };
            let report = if scheduled_path {
                scheduled(&mut db, |db| {
                    db.refresh_scheduled_background_optimizer_statistics(
                        &options,
                        BackgroundWorkHint::default(),
                    )
                })
            } else {
                admitted(&mut db, |db, policy, state| {
                    db.refresh_background_optimizer_statistics(
                        policy,
                        state,
                        &options,
                        BackgroundWorkHint::default(),
                    )
                })
            };
            assert_eq!(report.unwrap().index_sample_count, 1);
        }
        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(spill_root).unwrap();
    }
}

#[test]
fn storage_checkpoint_entry_points_defer_typed_and_retry() {
    for scheduled_path in [false, true] {
        let path = unique_test_dir(&format!("qos_refusal_checkpoint_{scheduled_path}"));
        {
            let mut db = Database::open_with_config(
                &path,
                DatabaseConfig {
                    max_wal_replay_bytes: Some(32 * 1024),
                    ..config()
                },
            )
            .unwrap();
            let payload = "x".repeat(128);
            (0..256)
                .find(|id| {
                    db.query_with_params(
                        "CREATE (:Memory {id: $id, payload: $payload})",
                        &BTreeMap::from([
                            ("id".to_string(), Value::Int(*id)),
                            ("payload".to_string(), Value::String(payload.clone())),
                        ]),
                    )
                    .unwrap();
                    db.storage_pressure_snapshot()
                        .unwrap()
                        .recommends_checkpoint()
                })
                .expect("WAL should reach its soft pressure threshold");
            if scheduled_path {
                scheduled(&mut db, |db| {
                    db.checkpoint_scheduled_background(BackgroundWorkHint::default())
                });
            } else {
                admitted(&mut db, |db, policy, state| {
                    db.checkpoint_background(policy, state, BackgroundWorkHint::default())
                });
            }
            let after = db.storage_pressure_snapshot().unwrap();
            assert_eq!(after.current_commit_epoch, after.checkpoint_commit_epoch);
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}
