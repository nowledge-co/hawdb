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
use std::collections::BTreeMap;

#[test]
fn startup_reuses_registration_across_queries_handles_and_processes() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    db.query_sql("CREATE TABLE lifecycle_values (id BIGINT PRIMARY KEY)")
        .unwrap();
    let path = fixture.0.join("projection");
    let mut consumer = db
        .open_or_create_search_projection_consumer(id("memory-v1"), &path, options(100), initialize)
        .unwrap();
    let original = consumer.projection().receipt();
    let before = std::fs::read(fixture.registry_path("database")).unwrap();
    let epoch = db.commit_epoch().unwrap();
    for _ in 0..8 {
        for index in [
            consumer.search_index().unwrap(),
            consumer.search_index().unwrap(),
        ] {
            assert_eq!(
                db.query("MATCH (m:Memory) RETURN m.id LIMIT 1")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert!(db
                .query_sql("SELECT id FROM lifecycle_values LIMIT 1")
                .unwrap()
                .rows
                .is_empty());
            assert_eq!(
                index
                    .search("document", None, crate::SearchMode::Text, 4)
                    .unwrap()
                    .len(),
                1
            );
        }
    }
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        before
    );
    for _ in 0..3 {
        drop(consumer);
        consumer = db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                &path,
                options(999),
                |_, _| panic!("startup must not initialize or renew an existing registration"),
            )
            .unwrap();
        assert_eq!(consumer.projection().receipt(), original);
        assert_eq!(
            db.runtime.get().unwrap().projection_consumers.records.len(),
            1
        );
        assert_eq!(
            std::fs::read(fixture.registry_path("database")).unwrap(),
            before
        );
    }
    append(&mut db, 2);
    assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
    let completed = consumer.projection().receipt();
    let before_restart = std::fs::read(fixture.registry_path("database")).unwrap();
    drop(consumer);
    drop(db);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "api::search_projection_consumer::tests::lifecycle::reopen_child",
            "--nocapture",
        ])
        .env("HAWDB_CONSUMER_LIFECYCLE_ROOT", &fixture.0)
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read(fixture.0.join("reopen-child.complete")).unwrap(),
        b"verified"
    );
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        before_restart
    );
    let mut db = fixture.database();
    let consumer = db
        .open_or_create_search_projection_consumer(id("memory-v1"), &path, options(100), |_, _| {
            panic!("must reopen")
        })
        .unwrap();
    assert_eq!(consumer.projection().receipt(), completed);
    assert_eq!(
        completed.binding.registration_uuid,
        original.binding.registration_uuid
    );
    assert!(db
        .search_projection_consumer_readiness(&consumer, None)
        .unwrap()
        .is_ready());
}

#[test]
fn reopen_child() {
    let Ok(root) = std::env::var("HAWDB_CONSUMER_LIFECYCLE_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    for _ in 0..3 {
        let mut db = Database::open(root.join("database")).unwrap();
        let consumer = db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                root.join("projection"),
                options(100),
                |_, _| panic!("must reopen"),
            )
            .unwrap();
        assert_eq!(consumer.search_index().unwrap().document_count(), 2);
        assert_eq!(
            db.runtime.get().unwrap().projection_consumers.records.len(),
            1
        );
        assert!(db
            .search_projection_consumer_readiness(&consumer, None)
            .unwrap()
            .is_ready());
    }
    // A successful process exit alone also permits zero selected tests.
    std::fs::write(root.join("reopen-child.complete"), b"verified").unwrap();
}

#[test]
fn branch_selection_does_not_adopt_the_parent_registration_or_projection() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let path = fixture.0.join("projection");
    let consumer = db
        .open_or_create_search_projection_consumer(id("memory-v1"), &path, options(100), initialize)
        .unwrap();
    let receipt = consumer.projection().receipt();
    let parent = db.current_branch().unwrap().unwrap().info.id;
    let registry = std::fs::read(fixture.registry_path("database")).unwrap();
    drop(consumer);
    let child = db
        .create_branch(crate::BranchCreateRequest {
            name: Some("child".into()),
            parent: crate::BranchSelector::Id(parent),
            expected_source_commit_epoch: db.commit_epoch().unwrap(),
            owner: None,
            idempotency_key: "consumer-child".into(),
        })
        .unwrap();
    db.use_branch(crate::BranchSelector::Id(child.id)).unwrap();
    assert!(db
        .open_or_create_search_projection_consumer(
            id("memory-v1"),
            &path,
            options(100),
            |_, _| panic!("a child must not adopt or overwrite the parent projection"),
        )
        .is_err());
    assert!(db
        .runtime
        .get()
        .unwrap()
        .projection_consumers
        .records
        .is_empty());
    db.use_branch(crate::BranchSelector::Id(parent)).unwrap();
    let consumer = db
        .open_or_create_search_projection_consumer(id("memory-v1"), &path, options(100), |_, _| {
            panic!("parent must reopen")
        })
        .unwrap();
    assert_eq!(consumer.projection().receipt(), receipt);
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        registry
    );
}

#[test]
fn removal_revokes_old_incarnation_and_preserves_projection_files() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut old = db
        .open_or_create_search_projection_consumer(
            id("memory-v1"),
            fixture.0.join("old"),
            options(100),
            initialize,
        )
        .unwrap();
    let old_uuid = old.projection().binding().registration_uuid;
    db.unregister_search_projection_consumer(old.id()).unwrap();
    let removed = std::fs::read(fixture.registry_path("database")).unwrap();
    db.unregister_search_projection_consumer(old.id()).unwrap();
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        removed
    );
    assert!(fixture.0.join("old/search_projection.hawdb").is_file());
    let new = db
        .open_or_create_search_projection_consumer(
            id("memory-v1"),
            fixture.0.join("new"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_ne!(new.projection().binding().registration_uuid, old_uuid);
    assert!(matches!(
        db.renew_search_projection_consumer(&old),
        Err(Error::InvalidHandle)
    ));
    assert!(matches!(
        catch_up(&mut db, &mut old),
        Err(Error::InvalidHandle)
    ));
    assert!(matches!(
        db.search_projection_consumer_readiness(&old, None),
        Err(Error::InvalidHandle)
    ));
    drop(old);
    assert!(matches!(
        db.open_or_create_search_projection_consumer(
            id("memory-v1"),
            fixture.0.join("old"),
            options(100),
            |_, _| panic!("must not adopt old incarnation"),
        ),
        Err(Error::InvalidHandle)
    ));
    assert!(db
        .search_projection_consumer_readiness(&new, None)
        .unwrap()
        .is_ready());
}

#[test]
fn uncertain_create_reconciles_durable_evidence_without_reinitializing() {
    for stage in [
        PublicationStage::BeforeRegistry,
        PublicationStage::AfterRegistry,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let other = db
            .open_or_create_search_projection_consumer(
                id("other"),
                fixture.0.join("other"),
                options(100),
                initialize,
            )
            .unwrap();
        let receipt = other.projection().receipt();
        let before = std::fs::read(fixture.registry_path("database")).unwrap();
        let path = fixture.0.join("projection");
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                &path,
                options(100),
                initialize,
            )
            .is_err());
        let attempted_uuid = db.runtime.get().unwrap().projection_consumers.records["memory-v1"]
            .registration_uuid
            .clone();
        assert!(path.join("search_projection.hawdb").exists());
        let retry = db.open_or_create_search_projection_consumer(
            id("memory-v1"),
            &path,
            options(100),
            |_, _| panic!("retry must not initialize over published evidence"),
        );
        let consumer = retry.unwrap();
        assert_eq!(
            consumer
                .projection()
                .binding()
                .registration_uuid
                .to_string(),
            attempted_uuid
        );
        assert!(db
            .search_projection_consumer_readiness(&consumer, None)
            .unwrap()
            .is_ready());
        if stage == PublicationStage::BeforeRegistry {
            assert_ne!(
                std::fs::read(fixture.registry_path("database")).unwrap(),
                before
            );
        }
        assert_eq!(other.projection().receipt(), receipt);
        db.renew_search_projection_consumer(&other).unwrap();
    }
}

#[test]
fn explicit_open_reconciles_uncertain_progress_without_advancing_the_receipt() {
    for stage in [
        PublicationStage::BeforeRegistry,
        PublicationStage::AfterRegistry,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let path = fixture.0.join("projection");
        let mut consumer = db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                &path,
                options(100),
                initialize,
            )
            .unwrap();
        let registration = consumer.projection().binding().registration_uuid;
        append(&mut db, 1);
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(catch_up(&mut db, &mut consumer).is_err());
        let receipt = consumer.projection().receipt();
        let registry = std::fs::read(fixture.registry_path("database")).unwrap();
        drop(consumer);
        let reopened = db.open_search_projection_consumer(&id("memory-v1"), &path);
        let consumer = reopened.unwrap();
        assert_eq!(consumer.projection().receipt(), receipt);
        assert_eq!(
            consumer.projection().binding().registration_uuid,
            registration
        );
        assert_eq!(consumer.search_index().unwrap().document_count(), 1);
        assert!(db
            .search_projection_consumer_readiness(&consumer, None)
            .unwrap()
            .is_ready());
        if stage == PublicationStage::AfterRegistry {
            assert_eq!(
                std::fs::read(fixture.registry_path("database")).unwrap(),
                registry
            );
        } else {
            assert_ne!(
                std::fs::read(fixture.registry_path("database")).unwrap(),
                registry
            );
        }
    }
}

#[test]
fn uncertain_removal_reloads_before_retry_without_losing_other_registrations() {
    for stage in [
        PublicationStage::BeforeRegistry,
        PublicationStage::AfterRegistry,
    ] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let other = db
            .open_or_create_search_projection_consumer(
                id("other"),
                fixture.0.join("other"),
                options(100),
                initialize,
            )
            .unwrap();
        let removed = db
            .open_or_create_search_projection_consumer(
                id("removed"),
                fixture.0.join("removed"),
                options(100),
                initialize,
            )
            .unwrap();
        let before = std::fs::read(fixture.registry_path("database")).unwrap();
        let receipt = other.projection().receipt();
        PUBLICATION_FAILURE.set(Some((stage, false)));
        assert!(db
            .unregister_search_projection_consumer(removed.id())
            .is_err());
        if stage == PublicationStage::BeforeRegistry {
            assert_eq!(
                std::fs::read(fixture.registry_path("database")).unwrap(),
                before
            );
        }
        db.unregister_search_projection_consumer(removed.id())
            .unwrap();
        let after = std::fs::read(fixture.registry_path("database")).unwrap();
        db.unregister_search_projection_consumer(removed.id())
            .unwrap();
        assert_eq!(
            std::fs::read(fixture.registry_path("database")).unwrap(),
            after
        );
        assert_eq!(
            db.runtime.get().unwrap().projection_consumers.records.len(),
            1
        );
        assert!(matches!(
            db.renew_search_projection_consumer(&removed),
            Err(Error::InvalidHandle)
        ));
        db.renew_search_projection_consumer(&other).unwrap();
        assert_eq!(other.projection().receipt(), receipt);
        assert!(fixture.0.join("removed/search_projection.hawdb").is_file());
    }
}

fn refused_read(snapshot: &mut DatabaseReadTransaction, cancel: bool) -> Result<()> {
    const QUERY: &str = "MATCH (m:Memory) RETURN m.id";
    if cancel {
        let context = hawdb_core::RuntimeTaskContext::default();
        context.cancellation().cancel();
        snapshot.query_with_params_streaming_context(
            QUERY,
            &BTreeMap::new(),
            crate::QueryStreamOptions::default(),
            &context,
            |_| panic!("cancelled read must not emit rows"),
        )?;
    } else {
        snapshot.query_with_params_bounded(QUERY, &BTreeMap::new(), Some(0))?;
    }
    panic!("read must refuse");
}

#[test]
fn initializer_and_catch_up_refusals_preserve_receipts_and_allow_explicit_retry() {
    for cancel in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        append(&mut db, 1);
        let other = db
            .open_or_create_search_projection_consumer(
                id("other"),
                fixture.0.join("other"),
                options(100),
                initialize,
            )
            .unwrap();
        let before = std::fs::read(fixture.registry_path("database")).unwrap();
        let other_receipt = other.projection().receipt();
        let path = fixture.0.join("projection");
        let error = db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                &path,
                options(100),
                |snapshot, _| refused_read(snapshot, cancel),
            )
            .unwrap_err();
        assert!(matches!(error, Error::Database(_)));
        if cancel {
            assert!(error.to_string().contains("cancelled"));
        }
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(fixture.registry_path("database")).unwrap(),
            before
        );
        assert_eq!(other.projection().receipt(), other_receipt);
        let mut consumer = db
            .open_or_create_search_projection_consumer(
                id("memory-v1"),
                &path,
                options(100),
                initialize,
            )
            .unwrap();
        append(&mut db, 2);
        let before = std::fs::read(fixture.registry_path("database")).unwrap();
        let receipt = consumer.projection().receipt();
        let error = db
            .catch_up_search_projection_consumer(&mut consumer, 32, 32, 8, |snapshot, _| {
                refused_read(snapshot, cancel)?;
                Ok(SearchProjectionRelationalDelta::default())
            })
            .unwrap_err();
        if cancel {
            assert!(error.to_string().contains("cancelled"));
        }
        assert_eq!(consumer.projection().receipt(), receipt);
        assert_eq!(other.projection().receipt(), other_receipt);
        assert_eq!(
            std::fs::read(fixture.registry_path("database")).unwrap(),
            before
        );
        assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
    }
}
