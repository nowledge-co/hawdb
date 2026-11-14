// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn setup_consumer(cold: bool) -> (Root, Database, SearchProjectionConsumer) {
    setup_consumer_with_governor(cold, None)
}

fn setup_consumer_with_governor(
    cold: bool,
    governor: Option<RuntimeGovernor>,
) -> (Root, Database, SearchProjectionConsumer) {
    let root = Root::new();
    let path = root.0.join("graph");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:Memory {id:'a',value:1})").unwrap();
    db.query_sql("CREATE TABLE records (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    db.query_sql("INSERT INTO records (id,body) VALUES ('a','old')")
        .unwrap();
    db.query_sql("CREATE TABLE unrelated (id TEXT PRIMARY KEY)")
        .unwrap();
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
    }
    if let Some(governor) = governor {
        db.set_runtime_governor(governor);
    }
    let consumer = db
        .create_search_projection_consumer(
            SearchProjectionConsumerId::new("graph-and-sql-v1").unwrap(),
            root.0.join("consumer"),
            SearchProjectionConsumerOptions::new(NonZeroU64::new(1000).unwrap()),
            |_, index| {
                index.upsert(doc("memory:a", "1"))?;
                index.upsert(doc("record:a", "old"))
            },
        )
        .unwrap();
    (root, db, consumer)
}

fn consumer_config() -> ProjectionConsumerCatchUpConfig {
    let base = config(0);
    ProjectionConsumerCatchUpConfig {
        reads: base.reads,
        feed: base.feed,
        mapping: base.mapping,
        sorting: base.sorting,
        task: RuntimeTaskContext::default(),
    }
}

fn consumer_stage<'a>(
    db: &mut Database,
    consumer: &'a SearchProjectionConsumer,
) -> ProjectionConsumerStagingSession<'a> {
    let mut session = db
        .begin_search_projection_consumer_catch_up(consumer, consumer_config(), delta_e)
        .unwrap();
    for _ in 0..10000 {
        if session.advance(one(), sort()).unwrap() == ProjectionStagingSessionStep::Complete {
            return session;
        }
    }
    panic!("Consumer staging did not complete");
}
fn consumer_covered(
    session: ProjectionConsumerStagingSession<'_>,
) -> ProjectionCoveredConsumerUpdate {
    let mut verification = session.verify_complete_source(caps(), full_e).unwrap();
    for _ in 0..10000 {
        if verification.advance(one(), sort()).unwrap() == ProjectionCoverageStep::Complete {
            return verification.finish().unwrap();
        }
    }
    panic!("Consumer coverage did not complete");
}
fn receipt_unchanged(
    db: &Database,
    consumer: &SearchProjectionConsumer,
    before: &hawdb::search::consumer::CheckpointReceipt,
) {
    assert_eq!(&consumer.projection().receipt(), before);
    assert_eq!(
        db.search_projection_consumer_status(consumer.id())
            .unwrap()
            .durable_complete_through_commit_epoch,
        Some(before.source_epoch)
    );
}

#[test]
fn registered_consumer_one_item_graph_sql_catch_up_matches_full_rebuild_and_reopen() {
    for cold in [false, true] {
        for aba in [false, true] {
            let (root, mut db, mut consumer) = setup_consumer(cold);
            let initial = consumer.projection().receipt();
            let permit = db
                .search_projection_consumer_read_permit(
                    RuntimeWorkRequest::foreground_query(16 << 20, 1 << 20).with_io_wave_slots(1),
                )
                .unwrap();
            let mut old = Some(consumer.pin(&permit).unwrap());
            drop(permit);
            let mut tx = db.begin_transaction().unwrap();
            for value in 2..8 {
                tx.query(&format!("MATCH (n:Memory) SET n.value={value}"))
                    .unwrap();
            }
            tx.commit().unwrap();
            db.query("MATCH (n:Memory) DETACH DELETE n").unwrap();
            db.query_sql("DELETE FROM records WHERE id='a'").unwrap();
            if aba {
                db.query("CREATE (:Memory {id:'a',value:9})").unwrap();
                db.query_sql("INSERT INTO records (id,body) VALUES ('a','final')")
                    .unwrap();
            }
            db.query_sql("INSERT INTO records (id,body) VALUES ('z','kept')")
                .unwrap();
            let e = db.commit_epoch().unwrap();
            let session = consumer_stage(&mut db, &consumer);
            assert_eq!(session.source_epoch(), e);
            receipt_unchanged(&db, &consumer, &initial);
            let covered = consumer_covered(session);
            receipt_unchanged(&db, &consumer, &initial);
            let report = db
                .publish_search_projection_consumer_covered_update(
                    &mut consumer,
                    covered,
                    ProjectionSourceDurability::RequireKnownDurable,
                )
                .unwrap();
            assert!(report.catch_up.complete);
            assert_eq!(report.catch_up.end_durable_epoch, Some(e));
            let expected = oracle(&mut db, &root, e);
            let ids = vec!["memory:a".into(), "record:a".into(), "record:z".into()];
            let expected_ids = if aba {
                ids.clone()
            } else {
                vec!["record:z".into()]
            };
            let expected_docs = expected.hydrate_documents(&expected_ids).unwrap().documents;
            let actual = consumer.search_index().unwrap();
            assert_eq!(actual.document_count(), expected.document_count());
            for id in &ids {
                assert_eq!(
                    actual.document(id),
                    expected_docs.iter().find(|doc| &doc.id == id)
                );
            }
            assert_eq!(
                old.as_ref().unwrap().report().source_epoch,
                initial.source_epoch
            );
            assert_eq!(old.as_ref().unwrap().report().document_count, 2);
            let permit = db
                .search_projection_consumer_read_permit(
                    RuntimeWorkRequest::foreground_query(16 << 20, 1 << 20).with_io_wave_slots(1),
                )
                .unwrap();
            let output = old.as_ref().unwrap().search_with_options(
                &permit,
                RuntimeTaskContext::default(),
                "old",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 4,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: Default::default(),
                    policy_epoch: None,
                },
            );
            if consumer
                .search_index()
                .unwrap()
                .runtime_capabilities()
                .is_enabled(RuntimeCapability::FullTextSearch)
            {
                let output = output.unwrap();
                assert_eq!(output.result().hits.len(), 1);
                assert_eq!(output.result().hits[0].id, "record:a");
            } else {
                assert!(matches!(
                    output,
                    Err(SearchQueryError::Query(HawDBError::CapabilityUnavailable {
                        capability: RuntimeCapability::FullTextSearch,
                    }))
                ));
            }
            drop(permit);
            let selected = consumer.projection().receipt();
            drop(consumer);
            drop(db);
            if cold && aba {
                // A second process can acquire ownership after all old runtime pins close.
                drop(old.take());
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "coverage::registered_consumer::registered_consumer_reopen_child",
                        "--nocapture",
                    ])
                    .env("HAWDB_COVERED_CONSUMER_REOPEN_ROOT", &root.0)
                    .env("HAWDB_COVERED_CONSUMER_REOPEN_EPOCH", e.to_string())
                    .env(
                        "HAWDB_COVERED_CONSUMER_REOPEN_REGISTRATION",
                        selected.binding.registration_uuid.to_string(),
                    )
                    .status()
                    .unwrap();
                assert!(status.success());
                assert_eq!(
                    std::fs::read_to_string(root.0.join("consumer-reopen.complete")).unwrap(),
                    selected.sha256
                );
            }
            // Other cases retain the old immutable pin across in-process reopen.
            let mut db = Database::open(root.0.join("graph")).unwrap();
            let consumer = db
                .open_or_create_search_projection_consumer(
                    SearchProjectionConsumerId::new("graph-and-sql-v1").unwrap(),
                    root.0.join("consumer"),
                    SearchProjectionConsumerOptions::new(NonZeroU64::new(1000).unwrap()),
                    |_, _| panic!("reopen must not rebuild"),
                )
                .unwrap();
            assert_eq!(consumer.projection().receipt(), selected);
            assert_eq!(
                selected.binding.registration_uuid,
                initial.binding.registration_uuid
            );
            assert_eq!(
                consumer.search_index().unwrap().document_count(),
                expected.document_count()
            );
            for id in &ids {
                assert_eq!(
                    consumer.search_index().unwrap().document(id),
                    expected_docs.iter().find(|doc| &doc.id == id)
                );
            }
            drop(old);
        }
    }
}

#[test]
fn registered_consumer_reopen_child() {
    let Ok(root) = std::env::var("HAWDB_COVERED_CONSUMER_REOPEN_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut db = Database::open(root.join("graph")).unwrap();
    let consumer = db
        .open_or_create_search_projection_consumer(
            SearchProjectionConsumerId::new("graph-and-sql-v1").unwrap(),
            root.join("consumer"),
            SearchProjectionConsumerOptions::new(NonZeroU64::new(1000).unwrap()),
            |_, _| panic!("fresh process must reopen the completed covered generation"),
        )
        .unwrap();
    let receipt = consumer.projection().receipt();
    assert_eq!(
        receipt.source_epoch.to_string(),
        std::env::var("HAWDB_COVERED_CONSUMER_REOPEN_EPOCH").unwrap()
    );
    assert_eq!(
        receipt.binding.registration_uuid.to_string(),
        std::env::var("HAWDB_COVERED_CONSUMER_REOPEN_REGISTRATION").unwrap()
    );
    let index = consumer.search_index().unwrap();
    assert_eq!(index.document_count(), 3);
    for (id, expected) in [
        ("memory:a", "9"),
        ("record:a", "final"),
        ("record:z", "kept"),
    ] {
        assert_eq!(index.document(id).unwrap().content, expected);
    }
    std::fs::write(root.join("consumer-reopen.complete"), receipt.sha256).unwrap();
}

#[test]
fn registered_consumer_post_e_source_conflict_preserves_receipt_and_retries() {
    let (_root, mut db, mut consumer) = setup_consumer(false);
    let before = consumer.projection().receipt();
    db.query("MATCH (n:Memory) SET n.value=2").unwrap();
    let session = consumer_stage(&mut db, &consumer);
    let e = session.source_epoch();
    db.query_sql("INSERT INTO unrelated (id) VALUES ('post-e')")
        .unwrap();
    let covered = consumer_covered(session);
    assert_eq!(covered.source_epoch(), e);
    assert!(matches!(
        db.publish_search_projection_consumer_covered_update(
            &mut consumer,
            covered,
            ProjectionSourceDurability::RequireKnownDurable,
        ),
        Err(ProjectionConsumerCatchUpError::Publication(
            ProjectionPublicationError::Dependency(_)
        ))
    ));
    receipt_unchanged(&db, &consumer, &before);
    let covered = consumer_covered(consumer_stage(&mut db, &consumer));
    db.publish_search_projection_consumer_covered_update(
        &mut consumer,
        covered,
        ProjectionSourceDurability::RequireKnownDurable,
    )
    .unwrap();
    assert_eq!(
        consumer
            .search_index()
            .unwrap()
            .document("memory:a")
            .unwrap()
            .content,
        "2"
    );
}

#[test]
fn registered_consumer_reclamation_preserves_continuously_invalidated_covered_base() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let (_root, mut db, mut consumer) = setup_consumer(false);
    let before = consumer.projection().receipt();
    db.query("MATCH (n:Memory) SET n.value=2").unwrap();
    let owner = Arc::new(DatabasePublicationOwner::new(db));
    let (write, writes) = mpsc::channel();
    let (committed, commits) = mpsc::channel();
    let writer_owner = Arc::clone(&owner);
    let writer = std::thread::spawn(move || {
        for sequence in writes {
            let epoch = writer_owner
                .with_write(|db| {
                    db.query_sql_with_params(
                        "INSERT INTO unrelated (id) VALUES ($1)",
                        &[Value::String(format!("writer-{sequence}"))],
                    )?;
                    db.commit_epoch()
                })
                .unwrap();
            committed.send(epoch).unwrap();
        }
    });
    for sequence in 0..8 {
        let session = owner
            .with_write(|db| {
                Ok(db.begin_search_projection_consumer_catch_up(
                    &consumer,
                    consumer_config(),
                    delta_e,
                ))
            })
            .unwrap()
            .unwrap();
        let captured = session.source_epoch();
        // The preparer cannot advance until a different thread has committed.
        // Starting a writer thread alone would not prove this ordering.
        write.send(sequence).unwrap();
        let committed = commits.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(committed > captured);
        let mut session = session;
        let mut complete = false;
        for _ in 0..10000 {
            if session.advance(one(), sort()).unwrap() == ProjectionStagingSessionStep::Complete {
                complete = true;
                break;
            }
        }
        assert!(complete, "Consumer staging did not complete");
        let covered = consumer_covered(session);
        let report = owner
            .with_write(|db| {
                Ok(db.cleanup_search_projection_consumer(&consumer, &Default::default()))
            })
            .unwrap()
            .unwrap();
        assert_eq!(report.failed_generations, 0, "{report:?}");
        assert_eq!(report.reclaimed_generations, 0, "{report:?}");
        let outcome = owner
            .with_write(|db| {
                Ok(db.publish_search_projection_consumer_covered_update(
                    &mut consumer,
                    covered,
                    ProjectionSourceDurability::RequireKnownDurable,
                ))
            })
            .unwrap();
        assert!(matches!(
            outcome,
            Err(ProjectionConsumerCatchUpError::Publication(
                ProjectionPublicationError::Dependency(_)
            ))
        ));
        owner
            .with_read(|db| {
                receipt_unchanged(db, &consumer, &before);
                Ok(())
            })
            .unwrap();
    }
    drop(write);
    writer.join().unwrap();

    // This quiescent drain proves retained evidence remains usable. It must not
    // be reported as simultaneous writer/watermark progress: all eight live
    // attempts above were refused by the current Database dependency.
    let covered = owner
        .with_write(|db| Ok(consumer_covered(consumer_stage(db, &consumer))))
        .unwrap();
    owner
        .with_write(|db| {
            Ok(db.publish_search_projection_consumer_covered_update(
                &mut consumer,
                covered,
                ProjectionSourceDurability::RequireKnownDurable,
            ))
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        consumer.projection().receipt().source_epoch,
        owner.with_read(Database::commit_epoch).unwrap()
    );
    assert_eq!(
        consumer
            .search_index()
            .unwrap()
            .document("memory:a")
            .unwrap()
            .content,
        "2"
    );
}

#[test]
fn registered_consumer_competing_candidate_and_foreign_registration_cannot_publish() {
    // Idle tokens keep their records and memory, while fresh independent work
    // must remain possible with only one background execution slot.
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            background_task_limit: Some(nz(1)),
            memory_budget_bytes: Some(256 << 20),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(2), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let (root, mut db, mut consumer) = setup_consumer_with_governor(false, Some(governor.clone()));
    let before = consumer.projection().receipt();
    db.query("MATCH (n:Memory) SET n.value=2").unwrap();
    let stale = consumer_covered(consumer_stage(&mut db, &consumer));
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    let mut other = db
        .create_search_projection_consumer(
            SearchProjectionConsumerId::new("other-v1").unwrap(),
            root.0.join("other"),
            SearchProjectionConsumerOptions::new(NonZeroU64::new(1000).unwrap()),
            |_, _| Ok(()),
        )
        .unwrap();
    assert!(matches!(
        db.publish_search_projection_consumer_covered_update(
            &mut other,
            stale,
            ProjectionSourceDurability::RequireKnownDurable,
        ),
        Err(ProjectionConsumerCatchUpError::Consumer(
            SearchProjectionConsumerError::Database(HawDBError::TransactionConflict { .. })
        ))
    ));
    receipt_unchanged(&db, &consumer, &before);
    let first = consumer_covered(consumer_stage(&mut db, &consumer));
    let stale = consumer_covered(consumer_stage(&mut db, &consumer));
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    db.publish_search_projection_consumer_covered_update(
        &mut consumer,
        first,
        ProjectionSourceDurability::RequireKnownDurable,
    )
    .unwrap();
    let selected = consumer.projection().receipt();
    assert!(matches!(
        db.publish_search_projection_consumer_covered_update(
            &mut consumer,
            stale,
            ProjectionSourceDurability::RequireKnownDurable,
        ),
        Err(ProjectionConsumerCatchUpError::Consumer(
            SearchProjectionConsumerError::Database(HawDBError::TransactionConflict { .. })
        ))
    ));
    receipt_unchanged(&db, &consumer, &selected);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
}

#[test]
fn registered_consumer_cancelled_work_never_advances_and_reopen_can_retry() {
    for after_coverage in [false, true] {
        let (root, mut db, mut consumer) = setup_consumer(false);
        let before = consumer.projection().receipt();
        db.query("MATCH (n:Memory) SET n.value=2").unwrap();
        let cancel = RuntimeCancellationToken::new();
        let mut cfg = consumer_config();
        cfg.task = RuntimeTaskContext::without_deadline(cancel.clone());
        let mut session = db
            .begin_search_projection_consumer_catch_up(&consumer, cfg, delta_e)
            .unwrap();
        if after_coverage {
            for _ in 0..10000 {
                if session.advance(one(), sort()).unwrap() == ProjectionStagingSessionStep::Complete
                {
                    break;
                }
            }
            let covered = consumer_covered(session);
            cancel.cancel();
            assert!(matches!(
                db.publish_search_projection_consumer_covered_update(
                    &mut consumer,
                    covered,
                    ProjectionSourceDurability::RequireKnownDurable,
                ),
                Err(ProjectionConsumerCatchUpError::Stopped(
                    hawdb::RuntimeCancellationReason::Cancelled
                ))
            ));
        } else {
            session.advance(one(), sort()).unwrap();
            cancel.cancel();
            assert!(session.advance(one(), sort()).is_err());
            assert!(session.verify_complete_source(caps(), full_e).is_err());
        }
        receipt_unchanged(&db, &consumer, &before);
        drop(consumer);
        drop(db);
        let mut db = Database::open(root.0.join("graph")).unwrap();
        let mut consumer = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new("graph-and-sql-v1").unwrap(),
                root.0.join("consumer"),
            )
            .unwrap();
        receipt_unchanged(&db, &consumer, &before);
        let covered = consumer_covered(consumer_stage(&mut db, &consumer));
        db.publish_search_projection_consumer_covered_update(
            &mut consumer,
            covered,
            ProjectionSourceDurability::Synchronize,
        )
        .unwrap();
    }
}

#[test]
fn registered_consumer_incomplete_wrong_mapping_and_mapping_budget_cannot_publish() {
    for (deleting, mapper) in [
        (true, omit_deletes as ProjectionMappingFunction),
        (false, wrong_body),
    ] {
        let (_root, mut db, consumer) = setup_consumer(false);
        let before = consumer.projection().receipt();
        let session = db
            .begin_search_projection_consumer_catch_up(&consumer, consumer_config(), mapper)
            .unwrap();
        assert!(session.verify_complete_source(caps(), full_e).is_err());
        if deleting {
            db.query("MATCH (n:Memory) DETACH DELETE n").unwrap();
        } else {
            db.query("MATCH (n:Memory) SET n.value=2").unwrap();
        }
        let mut session = db
            .begin_search_projection_consumer_catch_up(&consumer, consumer_config(), mapper)
            .unwrap();
        for _ in 0..10000 {
            if session.advance(one(), sort()).unwrap() == ProjectionStagingSessionStep::Complete {
                break;
            }
        }
        let mut coverage = session.verify_complete_source(caps(), full_e).unwrap();
        let mut refused = false;
        for _ in 0..10000 {
            if coverage.advance(one(), sort()).is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused);
        assert!(coverage.finish().is_err());
        receipt_unchanged(&db, &consumer, &before);
    }
    let (_root, mut db, consumer) = setup_consumer(false);
    let before = consumer.projection().receipt();
    db.query("MATCH (n:Memory) SET n.value=2").unwrap();
    let mut cfg = consumer_config();
    cfg.mapping.calls = nz(1);
    let mut session = db
        .begin_search_projection_consumer_catch_up(&consumer, cfg, delta_e)
        .unwrap();
    let mut refused = false;
    for _ in 0..10000 {
        if session.advance(one(), sort()).is_err() {
            refused = true;
            break;
        }
    }
    assert!(refused);
    drop(session);
    receipt_unchanged(&db, &consumer, &before);
}

#[test]
fn registered_consumer_empty_interval_is_complete_and_keeps_registration() {
    let (_root, mut db, mut consumer) = setup_consumer(false);
    let before = consumer.projection().receipt();
    let covered = consumer_covered(consumer_stage(&mut db, &consumer));
    db.publish_search_projection_consumer_covered_update(
        &mut consumer,
        covered,
        ProjectionSourceDurability::RequireKnownDurable,
    )
    .unwrap();
    let after = consumer.projection().receipt();
    assert_eq!(after.source_epoch, before.source_epoch);
    assert_eq!(
        after.binding.registration_uuid,
        before.binding.registration_uuid
    );
    assert_eq!(consumer.search_index().unwrap().document_count(), 2);
}

fn scoped_consumer_covered(
    session: ProjectionConsumerStagingSession<'_>,
) -> ProjectionCoveredConsumerUpdate {
    let mut coverage = session
        .verify_complete_source_in(
            caps(),
            full_e,
            hawdb::ProjectionSourceScope::Domains {
                node_labels: vec!["Memory".into()],
                relational_tables: vec!["records".into()],
            },
        )
        .unwrap();
    for _ in 0..10000 {
        if coverage.advance(one(), sort()).unwrap() == ProjectionCoverageStep::Complete {
            return coverage.finish().unwrap();
        }
    }
    panic!("scoped coverage did not complete");
}

#[test]
fn registered_consumer_scoped_detached_preparation_advances_with_real_writers() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    for cold in [false, true] {
        let (root, mut db, mut consumer) = setup_consumer(cold);
        db.query("CREATE (:Unrelated {value:0})").unwrap();
        let owner = Arc::new(DatabasePublicationOwner::new(db));
        let (send, receive) = mpsc::channel();
        let (done, completed) = mpsc::channel();
        let writer_owner = Arc::clone(&owner);
        let writer = std::thread::spawn(move || {
            for value in receive {
                let epoch = writer_owner
                    .with_write(|db| {
                        db.query_sql_with_params(
                            "INSERT INTO unrelated (id) VALUES ($1)",
                            &[Value::String(format!("writer-{value}"))],
                        )?;
                        db.query_with_params(
                            "MATCH (n:Unrelated) SET n.value=$value",
                            &BTreeMap::from([("value".into(), Value::Int(value))]),
                        )?;
                        db.commit_epoch()
                    })
                    .unwrap();
                done.send(epoch).unwrap();
            }
        });
        let mut previous = consumer.projection().receipt().source_epoch;
        for round in 1..=4 {
            owner
                .with_write(|db| {
                    db.query_with_params(
                        "MATCH (n:Memory) SET n.value=$value",
                        &BTreeMap::from([("value".into(), Value::Int(round))]),
                    )?;
                    Ok(())
                })
                .unwrap();
            let session = owner
                .with_write(|db| Ok(scoped_consumer_stage(db, &consumer)))
                .unwrap();
            let e = session.source_epoch();
            send.send(round * 3).unwrap();
            assert!(completed.recv_timeout(Duration::from_secs(30)).unwrap() > e);
            let covered = scoped_consumer_covered(session);
            let preparation = owner
                .with_write(|db| {
                    Ok(db.prepare_search_projection_consumer_covered_update(
                        &mut consumer,
                        covered,
                        ProjectionSourceDurability::RequireKnownDurable,
                    ))
                })
                .unwrap()
                .unwrap();
            // The reserved construction token holds no database guard. A real
            // writer must commit before construction is allowed to resume.
            send.send(round * 3 + 1).unwrap();
            assert!(completed.recv_timeout(Duration::from_secs(30)).unwrap() > e);
            let prepared = preparation.prepare();
            send.send(round * 3 + 2).unwrap();
            assert!(completed.recv_timeout(Duration::from_secs(30)).unwrap() > e);
            let published = owner
                .with_write(
                    |db| Ok(db.publish_search_projection_consumer_prepared_update(prepared)),
                )
                .unwrap()
                .unwrap();
            assert_eq!(published.report().catch_up.end_durable_epoch, Some(e));
            assert!(e > previous);
            previous = e;
            drop(published);
            assert_eq!(
                consumer
                    .search_index()
                    .unwrap()
                    .document("memory:a")
                    .unwrap()
                    .content,
                round.to_string()
            );
            owner
                .with_write(|db| {
                    let report = db
                        .cleanup_search_projection_consumer(&consumer, &Default::default())
                        .unwrap();
                    assert_eq!(report.failed_generations, 0);
                    Ok(())
                })
                .unwrap();
        }
        drop(send);
        writer.join().unwrap();
        let receipt = consumer.projection().receipt();
        drop(consumer);
        drop(owner);
        let mut db = Database::open(root.0.join("graph")).unwrap();
        let reopened = db
            .open_search_projection_consumer(
                &SearchProjectionConsumerId::new("graph-and-sql-v1").unwrap(),
                root.0.join("consumer"),
            )
            .unwrap();
        assert_eq!(reopened.projection().receipt(), receipt);
    }
}

#[test]
fn registered_consumer_scoped_prepared_source_conflict_and_drop_remain_retryable() {
    for mutation in [
        None,
        Some("MATCH (n:Memory) SET n.value=9"),
        Some("CREATE (:Memory {id:'b',value:3})"),
        Some("MATCH (n:Memory) DETACH DELETE n"),
    ] {
        let (_root, mut db, mut consumer) = setup_consumer(false);
        let before = consumer.projection().receipt();
        db.query("MATCH (n:Memory) SET n.value=2").unwrap();
        let covered = scoped_consumer_covered(scoped_consumer_stage(&mut db, &consumer));
        let prepared = db
            .prepare_search_projection_consumer_covered_update(
                &mut consumer,
                covered,
                ProjectionSourceDurability::RequireKnownDurable,
            )
            .unwrap()
            .prepare();
        if let Some(mutation) = mutation {
            db.query(mutation).unwrap();
            assert!(matches!(
                db.publish_search_projection_consumer_prepared_update(prepared),
                Err(ProjectionConsumerCatchUpError::Publication(
                    ProjectionPublicationError::Dependency(_)
                ))
            ));
        } else {
            drop(prepared);
        }
        assert_eq!(consumer.projection().receipt(), before);
        db.resolve_search_projection_consumer_publication(&mut consumer)
            .unwrap();
        receipt_unchanged(&db, &consumer, &before);
        let covered = scoped_consumer_covered(scoped_consumer_stage(&mut db, &consumer));
        let prepared = db
            .prepare_search_projection_consumer_covered_update(
                &mut consumer,
                covered,
                ProjectionSourceDurability::RequireKnownDurable,
            )
            .unwrap()
            .prepare();
        drop(
            db.publish_search_projection_consumer_prepared_update(prepared)
                .unwrap(),
        );
        assert_eq!(
            consumer.projection().receipt().source_epoch,
            db.commit_epoch().unwrap()
        );
    }
}

fn scoped_consumer_stage<'a>(
    db: &mut Database,
    consumer: &'a SearchProjectionConsumer,
) -> ProjectionConsumerStagingSession<'a> {
    let mut session = db
        .begin_search_projection_consumer_catch_up_in(
            consumer,
            consumer_config(),
            delta_e,
            hawdb::ProjectionSourceScope::Domains {
                node_labels: vec!["Memory".into()],
                relational_tables: vec!["records".into()],
            },
        )
        .unwrap();
    for _ in 0..10000 {
        if session.advance(one(), sort()).unwrap() == ProjectionStagingSessionStep::Complete {
            return session;
        }
    }
    panic!("scoped staging did not complete");
}

#[test]
fn registered_consumer_reserved_drop_retains_native_custody_until_resolution() {
    let (_root, mut db, mut consumer) = setup_consumer(false);
    let before = consumer.projection().receipt();
    db.query("MATCH (n:Memory) SET n.value=2").unwrap();
    let covered = scoped_consumer_covered(scoped_consumer_stage(&mut db, &consumer));
    let reserved = db
        .prepare_search_projection_consumer_covered_update(
            &mut consumer,
            covered,
            ProjectionSourceDurability::RequireKnownDurable,
        )
        .unwrap();
    drop(reserved);
    assert_eq!(consumer.projection().receipt(), before);
    db.resolve_search_projection_consumer_publication(&mut consumer)
        .unwrap();
    receipt_unchanged(&db, &consumer, &before);
    let covered = scoped_consumer_covered(scoped_consumer_stage(&mut db, &consumer));
    let prepared = db
        .prepare_search_projection_consumer_covered_update(
            &mut consumer,
            covered,
            ProjectionSourceDurability::RequireKnownDurable,
        )
        .unwrap()
        .prepare();
    drop(
        db.publish_search_projection_consumer_prepared_update(prepared)
            .unwrap(),
    );
}

#[test]
fn registered_consumer_scoped_prepared_sql_schema_and_source_fences() {
    for statement in [
        "UPDATE records SET body='changed' WHERE id='a'",
        "CREATE TABLE new_table (id TEXT PRIMARY KEY)",
        "foreign source",
    ] {
        let (_root, mut db, mut consumer) = setup_consumer(false);
        let before = consumer.projection().receipt();
        db.query("MATCH (n:Memory) SET n.value=2").unwrap();
        let covered = scoped_consumer_covered(scoped_consumer_stage(&mut db, &consumer));
        let prepared = db
            .prepare_search_projection_consumer_covered_update(
                &mut consumer,
                covered,
                ProjectionSourceDurability::RequireKnownDurable,
            )
            .unwrap()
            .prepare();
        let result = if statement == "foreign source" {
            let (_other_root, mut other, _other_consumer) = setup_consumer(false);
            other.publish_search_projection_consumer_prepared_update(prepared)
        } else {
            db.query_sql(statement).unwrap();
            db.publish_search_projection_consumer_prepared_update(prepared)
        };
        assert!(matches!(
            result,
            Err(ProjectionConsumerCatchUpError::Publication(_))
        ));
        db.resolve_search_projection_consumer_publication(&mut consumer)
            .unwrap();
        receipt_unchanged(&db, &consumer, &before);
    }
}

#[path = "embedded_consumer_coverage.rs"]
mod embedded;
