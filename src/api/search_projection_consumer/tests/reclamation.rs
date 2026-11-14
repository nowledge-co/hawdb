// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    DatabaseConfig, SearchProjectionConsumerCleanupOptions, SearchProjectionConsumerCleanupReport,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

fn generations(fixture: &Fixture) -> BTreeSet<PathBuf> {
    std::fs::read_dir(&fixture.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("consumer-birth.hawdb").exists())
        .collect()
}

fn advance(db: &mut Database, consumer: &mut SearchProjectionConsumer, update: u64) {
    append(db, update);
    assert!(catch_up(db, consumer).unwrap().catch_up.complete);
}

fn cleanup(
    db: &mut Database,
    consumer: &SearchProjectionConsumer,
) -> SearchProjectionConsumerCleanupReport {
    db.cleanup_search_projection_consumer(consumer, &Default::default())
        .unwrap()
}

#[test]
fn consumer_reclamation_progresses_at_strict_foreground_capacity() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        fixture.0.join("database"),
        DatabaseConfig {
            max_open_files: 128,
            strict_capacity_limit: true,
            ..Default::default()
        },
    )
    .unwrap();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let identifier = id("capacity-reclaim");
    let mut consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    advance(&mut db, &mut consumer, 2);
    advance(&mut db, &mut consumer, 3);
    let mut held = fixture.occupy_descriptor_target(&db);
    let before = db.file_descriptor_metrics().unwrap();
    let report = cleanup(&mut db, &consumer);
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert!(report.reclaimed_generations > 0, "{report:?}");
    assert!(path.exists());
    assert_eq!(consumer.search_index().unwrap().document_count(), 3);
    assert_eq!(
        db.file_descriptor_metrics().unwrap().budget_rejections,
        before.budget_rejections
    );
    db.unregister_search_projection_consumer(&identifier)
        .unwrap();
    drop(consumer);
    held.extend(fixture.occupy_descriptor_target(&db));
    let before_retry = db.file_descriptor_metrics().unwrap();
    let report = db
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert!(!report.retry_required, "{report:?}");
    assert!(generations(&fixture).is_empty());
    assert!(!path.exists());
    let after = db.file_descriptor_metrics().unwrap();
    assert_eq!(after.budget_rejections, before_retry.budget_rejections);
    assert_eq!(after.reserved, 0);
    assert!(after.high_water > after.limit);
    drop(held);
}

#[test]
fn consumer_reclamation_preserves_pins_initial_lease_and_selected_generation() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let identifier = id("physical-reclaim");
    let mut consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    advance(&mut db, &mut consumer, 2);
    let second = generations(&fixture)
        .into_iter()
        .find(|p| p != &path)
        .unwrap();
    let permit = db
        .search_projection_consumer_read_permit(
            hawdb_qos::RuntimeWorkRequest::foreground_query(64 * 1024 * 1024, 8 * 1024 * 1024)
                .with_io_wave_slots(1),
        )
        .unwrap();
    let pin = consumer.pin(&permit).unwrap();
    drop(permit);
    advance(&mut db, &mut consumer, 3);
    let third = generations(&fixture)
        .into_iter()
        .find(|p| p != &path && p != &second)
        .unwrap();
    advance(&mut db, &mut consumer, 4);
    let report = cleanup(&mut db, &consumer);
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert!(report.busy_generations > 0, "{report:?}");
    assert!(report.reclaimed_generations > 0, "{report:?}");
    assert!(second.join("search_projection.hawdb").exists());
    assert!(path.join("consumer-birth.hawdb").exists());
    assert!(!third.exists());
    assert_eq!(consumer.search_index().unwrap().document_count(), 4);
    let governor = db.consumer_governor().unwrap();
    let pinned_memory = governor.snapshot().admitted_memory_bytes;
    let pinned_fds = db.file_descriptor_metrics().unwrap().open;
    let query = db
        .search_projection_consumer_read_permit(
            hawdb_qos::RuntimeWorkRequest::foreground_query(64 * 1024 * 1024, 8 * 1024 * 1024)
                .with_io_wave_slots(1),
        )
        .unwrap();
    assert_eq!(
        pin.search_with_options(
            &query,
            RuntimeTaskContext::default(),
            "document",
            None,
            crate::SearchMode::Text,
            crate::SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: Default::default(),
                policy_epoch: None,
            }
        )
        .unwrap()
        .result()
        .hits
        .len(),
        2
    );
    drop(query);
    drop(pin);
    assert!(governor.snapshot().admitted_memory_bytes < pinned_memory);
    assert!(db.file_descriptor_metrics().unwrap().open < pinned_fds);
    let report = cleanup(&mut db, &consumer);
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert!(!second.exists());
    assert_eq!(generations(&fixture).len(), 2);

    // Registration and runtime custody are independent roots. Revocation does
    // not destroy a still-live logical lease or its selected runtime reader.
    db.unregister_search_projection_consumer(&identifier)
        .unwrap();
    let report = cleanup(&mut db, &consumer);
    assert!(report.busy_generations > 0, "{report:?}");
    assert!(path.exists());
    drop(consumer);
    drop(db);
    let mut reopened = fixture.database();
    let report = reopened
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert!(!report.retry_required, "{report:?}");
    assert!(generations(&fixture).is_empty());
    assert!(!path.exists());
}

#[test]
fn consumer_reclamation_retries_bounded_disk_intent_after_reopen() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let identifier = id("bounded-reclaim");
    let consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    db.unregister_search_projection_consumer(&identifier)
        .unwrap();
    drop(consumer);
    let options = SearchProjectionConsumerCleanupOptions {
        max_generations: NonZeroUsize::new(1).unwrap(),
        max_delete_attempts: NonZeroUsize::new(5).unwrap(),
        after: None,
    };
    let first = db
        .retry_search_projection_consumer_cleanup(&identifier, &path, &options)
        .unwrap();
    assert_eq!(first.failed_generations, 0, "{first:?}");
    assert!(first.progress_limited);
    assert!(first.delete_attempts <= 5);
    assert!(std::fs::read_dir(&fixture.0).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".consumer-reclaim-")));
    drop(db);
    for _ in 0..32 {
        let mut db = fixture.database();
        let report = db
            .retry_search_projection_consumer_cleanup(&identifier, &path, &options)
            .unwrap();
        assert_eq!(report.failed_generations, 0, "{report:?}");
        assert!(report.delete_attempts <= 5, "{report:?}");
        if !report.retry_required {
            break;
        }
    }
    assert!(!path.exists());
    assert!(!std::fs::read_dir(&fixture.0).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".consumer-reclaim-")));
}

#[test]
fn consumer_reclamation_keeps_unknown_evidence_and_surviving_consumers() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let identifier = id("blocked-reclaim");
    let consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    let other = db
        .create_search_projection_consumer(
            id("survivor"),
            fixture.0.join("survivor"),
            options(1000),
            initialize,
        )
        .unwrap();
    db.unregister_search_projection_consumer(&identifier)
        .unwrap();
    drop(consumer);
    let unknown = path.join("unexpected-directory");
    std::fs::create_dir(&unknown).unwrap();
    std::fs::write(unknown.join("evidence"), b"retain this evidence").unwrap();
    let before = std::fs::read(path.join("search_projection.hawdb")).unwrap();
    let governor = db.consumer_governor().unwrap();
    let admitted = governor.snapshot().admitted_memory_bytes;
    let descriptors = db.file_descriptor_metrics().unwrap().open;
    for _ in 0..3 {
        let report = db
            .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
            .unwrap();
        assert_eq!(report.failed_generations, 1, "{report:?}");
        assert_eq!(report.delete_attempts, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, admitted);
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(db.file_descriptor_metrics().unwrap().open, descriptors);
        assert_eq!(db.file_descriptor_metrics().unwrap().reserved, 0);
        assert!(report.retry_required);
        assert_eq!(
            std::fs::read(path.join("search_projection.hawdb")).unwrap(),
            before
        );
        assert_eq!(
            std::fs::read(unknown.join("evidence")).unwrap(),
            b"retain this evidence"
        );
        assert_eq!(other.search_index().unwrap().document_count(), 1);
    }
    // Only the fixture owns this injection; production never removes it.
    std::fs::remove_file(unknown.join("evidence")).unwrap();
    std::fs::remove_dir(unknown).unwrap();
    let report = db
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert_eq!(report.reclaimed_generations, 1);
    assert!(!path.exists());
    assert!(fixture.0.join("survivor/consumer-birth.hawdb").exists());
}

#[test]
fn consumer_reclamation_keeps_both_sides_of_unresolved_selection() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let identifier = id("pending-reclaim");
    let mut consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    advance(&mut db, &mut consumer, 2);
    append(&mut db, 3);
    PUBLICATION_FAILURE.set(Some((PublicationStage::AfterSelector, false)));
    assert!(catch_up(&mut db, &mut consumer).is_err());
    let before = generations(&fixture);
    drop(consumer);
    drop(db);
    let mut db = fixture.database();
    let report = db
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert_eq!(report.reclaimed_generations, 0);
    assert_eq!(generations(&fixture), before);
    let recovered = db
        .open_search_projection_consumer(&identifier, &path)
        .unwrap();
    assert_eq!(recovered.search_index().unwrap().document_count(), 3);
}

#[test]
fn consumer_reclamation_finishes_missing_marker_and_directory_from_durable_intent() {
    for remove_directory in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        append(&mut db, 1);
        let path = fixture.0.join("projection");
        let identifier = id("finalize-retry");
        let consumer = db
            .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
            .unwrap();
        db.unregister_search_projection_consumer(&identifier)
            .unwrap();
        drop(consumer);
        let options = SearchProjectionConsumerCleanupOptions {
            max_generations: NonZeroUsize::new(1).unwrap(),
            max_delete_attempts: NonZeroUsize::new(5).unwrap(),
            after: None,
        };
        let report = db
            .retry_search_projection_consumer_cleanup(&identifier, &path, &options)
            .unwrap();
        assert!(report.progress_limited, "{report:?}");
        assert_eq!(report.failed_generations, 0);
        drop(db);
        // Materialize the two post-unlink interruption states in this fixture.
        // This is restart-path coverage, not a simulated power-loss barrier proof.
        for entry in std::fs::read_dir(&path).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
        if remove_directory {
            std::fs::remove_dir(&path).unwrap();
        }
        let mut reopened = fixture.database();
        let report = reopened
            .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
            .unwrap();
        assert_eq!(report.failed_generations, 0, "{report:?}");
        assert_eq!(report.reclaimed_generations, 1, "{report:?}");
        assert!(!report.retry_required);
        assert!(!path.exists());
    }
}

#[test]
fn consumer_reclamation_preserves_closed_branch_registration() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let main_projection = db
        .create_search_projection_consumer(
            id("main-boundary"),
            fixture.0.join("main-projection"),
            options(1000),
            initialize,
        )
        .unwrap();
    drop(main_projection);
    let main = db.current_branch().unwrap().unwrap().info.id;
    let child = db
        .create_branch(crate::BranchCreateRequest {
            name: Some("durable-consumer".into()),
            parent: crate::BranchSelector::Id(main),
            expected_source_commit_epoch: db.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "consumer-reclamation-child".into(),
        })
        .unwrap();
    db.query_sql("USE BRANCH NAME 'durable-consumer'").unwrap();
    let path = fixture.0.join("projection");
    let identifier = id("branch-consumer");
    let mut consumer = db
        .create_search_projection_consumer(identifier.clone(), &path, options(1000), initialize)
        .unwrap();
    advance(&mut db, &mut consumer, 2);
    drop(consumer);
    db.query_sql("USE BRANCH NAME 'main'").unwrap();
    let before = generations(&fixture);
    assert_eq!(before.len(), 3);
    let report = db
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.failed_generations, 0, "{report:?}");
    assert_eq!(report.reclaimed_generations, 0);
    assert_eq!(generations(&fixture), before);
    drop(db);
    let mut reopened = fixture.database();
    reopened
        .query_sql(&format!("USE BRANCH ID '{}'", child.id))
        .unwrap();
    let report = reopened
        .retry_search_projection_consumer_cleanup(&identifier, &path, &Default::default())
        .unwrap();
    assert_eq!(report.retained_generations, 2, "{report:?}");
    let restored = reopened
        .open_search_projection_consumer(&identifier, &path)
        .unwrap();
    assert_eq!(restored.search_index().unwrap().document_count(), 2);
}
