// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::{SearchMode, SearchQueryOptions};
use hawdb_qos::RuntimeWorkRequest;

fn query_options() -> SearchQueryOptions {
    SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: Default::default(),
        policy_epoch: None,
    }
}

fn query_permit(db: &mut Database) -> hawdb_qos::RuntimePermit {
    db.search_projection_consumer_read_permit(
        RuntimeWorkRequest::foreground_query(64 * 1024 * 1024, 8 * 1024 * 1024)
            .with_io_wave_slots(1),
    )
    .unwrap()
}

#[test]
fn registered_stationary_transient_reopen_failure_does_not_require_rebuild() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let consumer = db
        .create_search_projection_consumer(id("retry-open"), &path, options(100), initialize)
        .unwrap();
    let before = consumer.projection().receipt();
    drop(consumer);
    PUBLICATION_FAILURE.set(Some((PublicationStage::BeforeReopen, false)));
    assert!(matches!(
        db.open_search_projection_consumer(&id("retry-open"), &path),
        Err(Error::Database(HawDBError::Storage(_)))
    ));
    assert_eq!(
        db.search_projection_consumer_status(&id("retry-open"))
            .unwrap()
            .state,
        State::Active
    );
    let mut reopened = db
        .open_search_projection_consumer(&id("retry-open"), &path)
        .unwrap();
    assert_eq!(reopened.projection().receipt(), before);
    assert_eq!(reopened.search_index().unwrap().document_count(), 1);

    // Startup must also propagate transient errors while reacquiring an
    // unresolved native publication, before the ordinary registered-open path.
    append(&mut db, 2);
    let epoch = db.commit_epoch().unwrap();
    PUBLICATION_FAILURE.set(Some((PublicationStage::AfterSelector, false)));
    assert!(matches!(
        catch_up(&mut db, &mut reopened),
        Err(Error::Database(HawDBError::Storage(_)))
    ));
    drop(reopened);
    drop(db);
    let mut db = fixture.database();
    assert!(db
        .runtime
        .get()
        .unwrap()
        .store
        .search_consumer_registry_snapshot()
        .unwrap()
        .pending
        .iter()
        .any(|pending| pending == "retry-open"));
    PUBLICATION_FAILURE.set(Some((PublicationStage::BeforeReopen, false)));
    assert!(matches!(
        db.open_or_create_search_projection_consumer(
            id("retry-open"),
            &path,
            options(100),
            |_, _| panic!("startup recovery must not reinitialize")
        ),
        Err(Error::Database(HawDBError::Storage(_)))
    ));
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id").unwrap().rows.len(),
        2
    );
    let recovered = db
        .open_or_create_search_projection_consumer(id("retry-open"), &path, options(100), |_, _| {
            panic!("retry must recover the original publication")
        })
        .unwrap();
    assert_eq!(recovered.projection().receipt().source_epoch, epoch);
    assert_eq!(recovered.search_index().unwrap().document_count(), 2);
    assert_eq!(
        db.search_projection_consumer_status(&id("retry-open"))
            .unwrap()
            .state,
        State::Active
    );
}

#[test]
fn registered_stationary_completed_ledger_rows_allow_more_than_256_catch_ups() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let mut consumer = db
        .create_search_projection_consumer(id("many-updates"), &path, options(10_000), initialize)
        .unwrap();
    // Isolate ledger capacity from the automatic checkpoint's periodic catalog
    // publication. Its concurrent publication contract has separate fixtures.
    let checkpoint_suspension = db.runtime.suspend_automatic_checkpoint().unwrap();
    let permit = query_permit(&mut db);
    let old = consumer.pin(&permit).unwrap();
    let mut operation = consumer
        .projection()
        .stationary_receipt()
        .unwrap()
        .binding()
        .operation;
    for update in 0..260 {
        db.query(&format!(
            "MATCH (m:Memory {{id: 'm1'}}) SET m.content = 'document update {update}'"
        ))
        .unwrap();
        assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
        let next = consumer
            .projection()
            .stationary_receipt()
            .unwrap()
            .binding()
            .operation;
        assert!(next > operation);
        operation = next;
        assert_eq!(consumer.search_index().unwrap().document_count(), 1);
    }
    assert_eq!(
        old.search_with_options(
            &permit,
            RuntimeTaskContext::default(),
            "document",
            None,
            SearchMode::Text,
            query_options()
        )
        .unwrap()
        .result()
        .hits
        .len(),
        1
    );
    drop(old);
    drop(permit);
    let before = consumer.projection().receipt();
    drop(consumer);
    drop(checkpoint_suspension);
    drop(db);
    let mut db = fixture.database();
    let reopened = db
        .open_search_projection_consumer(&id("many-updates"), &path)
        .unwrap();
    assert_eq!(reopened.projection().receipt(), before);
    assert_eq!(
        reopened
            .projection()
            .stationary_receipt()
            .unwrap()
            .binding()
            .operation,
        operation
    );
}

#[test]
fn registered_stationary_unresolved_ledger_capacity_is_a_resource_refusal() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    db.runtime
        .get_mut()
        .unwrap()
        .store
        .set_search_projection_database_identity(generate_uuidv7().unwrap());
    db.checkpoint().unwrap();
    for index in 0..256 {
        db.runtime
            .get()
            .unwrap()
            .store
            .reserve_search_consumer_allocation(
                &format!("unresolved-{index}"),
                &fixture.0.join(format!("debt-{index}")),
            )
            .unwrap();
    }
    assert!(matches!(
        db.runtime
            .get()
            .unwrap()
            .store
            .reserve_search_consumer_allocation("overflow", &fixture.0.join("overflow")),
        Err(HawDBError::ResourceBudgetExceeded(error))
            if error.resource == hawdb_core::ResourceBudgetKind::MetadataEntries
                && error.requested == 1 && error.used == 256 && error.budget == 256
    ));
    append(&mut db, 2);
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id").unwrap().rows.len(),
        2
    );
}

pub(super) fn assert_native_create_catch_up_and_reopen() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let mut consumer = db
        .create_search_projection_consumer(id("native"), &path, options(100), |snapshot, index| {
            initialize(snapshot, index)?;
            index.upsert(crate::SearchDocument {
                id: "external:vector".into(),
                title: "vector".into(),
                content: "embedding fixture".into(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: std::collections::BTreeMap::from([("kind".into(), "embedding".into())]),
            })
        })
        .unwrap();
    let permit = query_permit(&mut db);
    let old = consumer.pin(&permit).unwrap();
    let before = old
        .search_with_options(
            &permit,
            RuntimeTaskContext::default(),
            "document",
            None,
            SearchMode::Text,
            query_options(),
        )
        .unwrap();
    assert_eq!(before.result().hits.len(), 1);
    assert_eq!(before.result().hits[0].id, "memory:m1");
    drop(before);
    drop(permit);
    append(&mut db, 2);
    assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
    let permit = query_permit(&mut db);
    let new = consumer.pin(&permit).unwrap();
    assert!(new.report().source_epoch > old.report().source_epoch);
    for (pin, count) in [(&old, 1), (&new, 2)] {
        let result = pin
            .search_with_options(
                &permit,
                RuntimeTaskContext::default(),
                "document",
                None,
                SearchMode::Text,
                query_options(),
            )
            .unwrap();
        assert_eq!(result.result().hits.len(), count);
    }
    let vectors = new
        .search_with_options(
            &permit,
            RuntimeTaskContext::default(),
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                metadata_filters: std::collections::BTreeMap::from([(
                    "kind".into(),
                    "embedding".into(),
                )]),
                ..query_options()
            },
        )
        .unwrap();
    assert_eq!(vectors.result().hits.len(), 1);
    assert_eq!(vectors.result().hits[0].id, "external:vector");
    drop(vectors);
    drop(new);
    drop(consumer);
    // The original native reader survives owner closure, with no execution slot
    // retained by its immutable version.
    assert_eq!(
        old.search_with_options(
            &permit,
            RuntimeTaskContext::default(),
            "document",
            None,
            SearchMode::Text,
            query_options()
        )
        .unwrap()
        .result()
        .hits
        .len(),
        1
    );
    drop(old);
    drop(permit);
    drop(db);
    let mut db = fixture.database();
    let consumer = db
        .open_or_create_search_projection_consumer(id("native"), &path, options(100), |_, _| {
            panic!("completed native generation must reopen")
        })
        .unwrap();
    let permit = query_permit(&mut db);
    let pin = consumer.pin(&permit).unwrap();
    assert_eq!(pin.report().document_count, 3);
    assert_eq!(
        pin.search_with_options(
            &permit,
            RuntimeTaskContext::default(),
            "document",
            None,
            SearchMode::Text,
            query_options()
        )
        .unwrap()
        .result()
        .hits
        .len(),
        2
    );
}

#[test]
fn registered_stationary_native_create_catch_up_publish_reopen_keeps_old_pins() {
    assert_native_create_catch_up_and_reopen();
}

#[test]
fn registered_stationary_live_failure_resolves_original_epoch_after_later_writes() {
    for stage in [
        PublicationStage::BeforeSelector,
        PublicationStage::AfterSelector,
        PublicationStage::AfterSelectorBarrier,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        append(&mut db, 1);
        let mut consumer = db
            .create_search_projection_consumer(
                id("recover"),
                fixture.0.join("projection"),
                options(100),
                initialize,
            )
            .unwrap();
        let before = consumer.projection().receipt();
        let permit = query_permit(&mut db);
        let old = consumer.pin(&permit).unwrap();
        drop(permit);
        append(&mut db, 2);
        let selected_epoch = db.commit_epoch().unwrap();
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(catch_up(&mut db, &mut consumer).is_err());
        assert!(matches!(
            consumer.search_index(),
            Err(HawDBError::ProjectionBusy)
        ));
        assert!(matches!(
            db.search_projection_consumer_readiness(&consumer, None),
            Err(Error::Database(HawDBError::ProjectionBusy))
        ));
        append(&mut db, 3);
        db.resolve_search_projection_consumer_publication(&mut consumer)
            .unwrap();
        assert_eq!(
            consumer.projection().receipt().source_epoch,
            if stage == PublicationStage::BeforeSelector {
                before.source_epoch
            } else {
                selected_epoch
            }
        );
        let permit = query_permit(&mut db);
        assert_eq!(
            old.search_with_options(
                &permit,
                RuntimeTaskContext::default(),
                "document",
                None,
                SearchMode::Text,
                query_options()
            )
            .unwrap()
            .result()
            .hits
            .len(),
            1
        );
        drop(permit);
        assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
        assert_eq!(consumer.search_index().unwrap().document_count(), 3);
    }
}

#[test]
fn registered_stationary_restart_recovers_selected_creation_without_initializer() {
    for stage in [
        PublicationStage::AfterSelector,
        PublicationStage::AfterSelectorBarrier,
        PublicationStage::BeforeRegistry,
        PublicationStage::AfterRegistry,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        append(&mut db, 1);
        let path = fixture.0.join("projection");
        let initial_epoch = db.commit_epoch().unwrap();
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(db
            .create_search_projection_consumer(id("created"), &path, options(100), initialize)
            .is_err());
        append(&mut db, 2);
        drop(db);
        let mut db = fixture.database();
        let mut consumer = db
            .open_or_create_search_projection_consumer(
                id("created"),
                &path,
                options(999),
                |_, _| panic!("a lost response is not rollback"),
            )
            .unwrap();
        assert_eq!(consumer.search_index().unwrap().document_count(), 1);
        assert_eq!(
            db.search_projection_consumer_status(consumer.id())
                .unwrap()
                .expires_at_commit_epoch,
            initial_epoch + 100
        );
        assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
    }
}

#[cfg(unix)]
#[test]
fn registered_stationary_replaced_producer_parent_never_writes_the_foreign_namespace() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let epoch = db.commit_epoch().unwrap();
    let parent = fixture.0.join("target-parent");
    let moved = fixture.0.join("original-parent");
    std::fs::create_dir(&parent).unwrap();
    let destination = parent.join("projection");
    let error = db
        .create_search_projection_consumer(
            id("replacement"),
            &destination,
            options(100),
            |snapshot, index| {
                initialize(snapshot, index)?;
                std::fs::rename(&parent, &moved).unwrap();
                std::fs::create_dir(&parent).unwrap();
                std::fs::create_dir(&destination).unwrap();
                std::fs::create_dir(destination.join(".consumer-producer")).unwrap();
                std::fs::write(destination.join("keep"), b"foreign target").unwrap();
                Ok(())
            },
        )
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Database(HawDBError::StorageIntegrity(_))
    ));
    assert_eq!(
        std::fs::read(destination.join("keep")).unwrap(),
        b"foreign target"
    );
    assert_eq!(
        std::fs::read_dir(destination.join(".consumer-producer"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(matches!(
        db.search_projection_consumer_status(&id("replacement")),
        Err(Error::InvalidHandle)
    ));
    let recovered = db
        .create_search_projection_consumer(
            id("replacement"),
            fixture.0.join("retry"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(recovered.search_index().unwrap().document_count(), 1);
}
