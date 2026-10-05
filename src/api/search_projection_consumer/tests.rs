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
use crate::SearchRebuildOptions;
use std::any::TypeId;
use std::num::NonZeroU64;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("hawdb-consumer-{}", generate_uuidv7().unwrap()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn database(&self) -> Database {
        Database::open(self.0.join("database")).unwrap()
    }

    fn registry_path(&self, project: &str) -> PathBuf {
        let root = self.0.join(project);
        let runtime_root =
            match hawdb_storage::branch_project::inspect_project_manifest(&root).unwrap() {
                hawdb_storage::branch_project::ProjectManifest::Branch(selector) => root
                    .join("branches")
                    .join(selector.main_branch_id().as_uuid().to_string())
                    .join("data"),
                _ => root,
            };
        runtime_root.join("projection_consumers.meta")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn id(value: &str) -> SearchProjectionConsumerId {
    SearchProjectionConsumerId::new(value).unwrap()
}
fn options(commits: u64) -> SearchProjectionConsumerOptions {
    SearchProjectionConsumerOptions::new(NonZeroU64::new(commits).unwrap())
}
fn initialize(snapshot: &mut DatabaseReadTransaction, index: &mut SearchIndex) -> Result<()> {
    snapshot
        .rebuild_search_projection(index, SearchRebuildOptions::default())
        .map(|_| ())
}
fn append(db: &mut Database, value: u64) {
    db.query(&format!(
        "CREATE (:Memory {{id: 'm{value}', content: 'document {value}'}})"
    ))
    .unwrap();
}
fn catch_up(
    db: &mut Database,
    consumer: &mut SearchProjectionConsumer,
) -> SearchProjectionConsumerResult<SearchProjectionConsumerCatchUpReport> {
    db.catch_up_search_projection_consumer(consumer, 32, 32, 8, |_, _| {
        Ok(SearchProjectionRelationalDelta::default())
    })
}

#[test]
fn search_projection_consumer_facade_preserves_owner_type_identity() {
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerId>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerId>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerOptions>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerOptions>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumer>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumer>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerState>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerState>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerRebuildReason>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerRebuildReason>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerStatus>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerStatus>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerReadiness>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerReadiness>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerCatchUpReport>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerCatchUpReport>()
    );
    assert_eq!(
        TypeId::of::<crate::SearchProjectionConsumerError>(),
        TypeId::of::<hawdb_search::projection_consumer::SearchProjectionConsumerError>()
    );
}

#[test]
fn creation_checkpoint_catch_up_and_reopen_use_real_receipts() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    append(&mut db, 1);
    let epoch = db.runtime.get().unwrap().store.commit_epoch();
    let mut consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(db.runtime.get().unwrap().store.commit_epoch(), epoch);
    assert_eq!(consumer.search_index().document_count(), 1);
    assert!(db
        .search_projection_consumer_readiness(&consumer, None)
        .unwrap()
        .is_ready());
    append(&mut db, 2);
    let report = catch_up(&mut db, &mut consumer).unwrap();
    assert_eq!(report.catch_up.applied_batch_count, 1);
    assert!(report.catch_up.complete);
    assert_eq!(consumer.search_index().document_count(), 2);
    assert_eq!(
        report.consumer.minimum_valid_consumer_commit_epoch,
        Some(db.runtime.get().unwrap().store.commit_epoch())
    );
    drop(consumer);
    db.checkpoint().unwrap();
    let identity = db
        .runtime
        .get()
        .unwrap()
        .store
        .search_projection_database_identity();
    drop(db);
    let mut db = fixture.database();
    assert_eq!(
        db.runtime
            .get()
            .unwrap()
            .store
            .search_projection_database_identity(),
        identity
    );
    let status = db.search_projection_consumer_status(&id("main")).unwrap();
    assert_eq!(status.state, State::Unverified);
    assert_eq!(status.minimum_valid_consumer_commit_epoch, None);
    let consumer = db
        .open_search_projection_consumer(&id("main"), fixture.0.join("projection"))
        .unwrap();
    assert_eq!(consumer.search_index().document_count(), 2);
    assert_eq!(
        db.search_projection_consumer_status(&id("main"))
            .unwrap()
            .state,
        State::Active
    );
}

#[test]
fn empty_source_epoch_zero_and_repeated_catch_up_are_valid() {
    let fixture = Fixture::new();
    let mut db = Database::default();
    db.runtime.get_mut().unwrap().store = crate::store::GraphStore::open(
        fixture.0.join("database"),
        &mut db.runtime.get_mut().unwrap().catalog,
    )
    .unwrap();
    let mut consumer = db
        .create_search_projection_consumer(
            id("empty"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(consumer.projection().receipt().source_epoch, 0);
    let before = std::fs::read(fixture.registry_path("database")).unwrap();
    let report = catch_up(&mut db, &mut consumer).unwrap();
    assert!(report.catch_up.complete);
    assert_eq!(report.catch_up.applied_batch_count, 0);
    assert_eq!(
        before,
        std::fs::read(fixture.registry_path("database")).unwrap()
    );
}

#[test]
fn initialization_failure_cleans_stage_and_preserves_existing_destination() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let result = db.create_search_projection_consumer(
        id("failed"),
        fixture.0.join("projection"),
        options(100),
        |_, index| {
            assert!(index.is_persistent());
            Err(HawDBError::Execution("incomplete initialization".into()))
        },
    );
    assert!(result.is_err());
    assert!(!fixture.0.join("projection").exists());
    assert!(!std::fs::read_dir(&fixture.0).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".stage")));
    std::fs::create_dir(fixture.0.join("projection")).unwrap();
    std::fs::write(fixture.0.join("projection/keep"), "untouched").unwrap();
    let result = db.create_search_projection_consumer(
        id("failed"),
        fixture.0.join("projection"),
        options(100),
        |_, _| panic!("must not initialize existing destination"),
    );
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(fixture.0.join("projection/keep")).unwrap(),
        b"untouched"
    );
}

#[test]
fn owned_projection_excludes_other_publishers_and_releases_lease_on_drop() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let consumer = db
        .create_search_projection_consumer(
            id("owned"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    assert!(consumer.search_index().checkpoint().is_err());
    assert!(consumer
        .search_index()
        .mark_full_reindex_needed("external")
        .is_err());
    assert!(SearchIndex::open(fixture.0.join("projection")).is_err());
    assert!(db
        .open_search_projection_consumer(&id("owned"), fixture.0.join("projection"))
        .is_err());
    drop(consumer);
    assert!(SearchIndex::open(fixture.0.join("projection")).is_err());
    db.open_search_projection_consumer(&id("owned"), fixture.0.join("projection"))
        .unwrap();
}

#[test]
fn hard_retention_floor_invalidates_only_the_lagging_consumer() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    db.runtime
        .get_mut()
        .unwrap()
        .store
        .set_max_search_projection_change_log_entries(Some(1));
    let mut lagging = db
        .create_search_projection_consumer(
            id("lagging"),
            fixture.0.join("lagging"),
            options(100),
            initialize,
        )
        .unwrap();
    let mut current = db
        .create_search_projection_consumer(
            id("current"),
            fixture.0.join("current"),
            options(100),
            initialize,
        )
        .unwrap();
    append(&mut db, 1);
    let first_epoch = db.runtime.get().unwrap().store.commit_epoch();
    catch_up(&mut db, &mut current).unwrap();
    append(&mut db, 2);
    let status = db
        .search_projection_consumer_status(&id("lagging"))
        .unwrap();
    assert!(matches!(
        status.state,
        State::RebuildRequired(Reason::RetentionLimitExceeded { .. })
    ));
    assert_eq!(
        db.search_projection_consumer_status(&id("current"))
            .unwrap()
            .state,
        State::Active
    );
    assert_eq!(
        status.minimum_valid_consumer_commit_epoch,
        Some(first_epoch)
    );
    assert!(matches!(
        catch_up(&mut db, &mut lagging),
        Err(Error::RebuildRequired(
            Reason::RetentionLimitExceeded { .. }
        ))
    ));
    catch_up(&mut db, &mut current).unwrap();
    assert_eq!(
        db.runtime
            .get()
            .unwrap()
            .store
            .search_projection_changefeed_status()
            .retained_mutation_count,
        1
    );
}

#[test]
fn expiry_is_inclusive_renewal_is_explicit_and_stale_handles_stay_revoked() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut old = db
        .create_search_projection_consumer(
            id("reusable"),
            fixture.0.join("old"),
            options(2),
            initialize,
        )
        .unwrap();
    append(&mut db, 1);
    let deadline = db.runtime.get().unwrap().store.commit_epoch() + 2;
    assert_eq!(
        db.renew_search_projection_consumer(&old)
            .unwrap()
            .expires_at_commit_epoch,
        deadline
    );
    append(&mut db, 2);
    append(&mut db, 3);
    assert!(matches!(
        db.renew_search_projection_consumer(&old),
        Err(Error::RebuildRequired(Reason::Expired {
            expires_at_commit_epoch
        })) if expires_at_commit_epoch == deadline
    ));
    db.unregister_search_projection_consumer(&id("reusable"))
        .unwrap();
    db.unregister_search_projection_consumer(&id("reusable"))
        .unwrap();
    let _new = db
        .create_search_projection_consumer(
            id("reusable"),
            fixture.0.join("new"),
            options(100),
            initialize,
        )
        .unwrap();
    assert!(matches!(
        catch_up(&mut db, &mut old),
        Err(Error::InvalidHandle)
    ));
    assert!(fixture.0.join("old/search_projection.hawdb").exists());
}

#[test]
fn active_wal_sync_group_rejects_consumer_writes_before_callbacks() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut consumer = db
        .create_search_projection_consumer(
            id("owned"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    db.begin_wal_sync_group().unwrap();
    append(&mut db, 1);
    assert!(matches!(
        db.renew_search_projection_consumer(&consumer),
        Err(Error::SourceNotDurable)
    ));
    assert!(matches!(
        db.catch_up_search_projection_consumer(&mut consumer, 1, 1, 1, |_, _| panic!(
            "must reject before hydration"
        )),
        Err(Error::SourceNotDurable)
    ));
    assert!(matches!(
        db.create_search_projection_consumer(
            id("other"),
            fixture.0.join("other"),
            options(100),
            |_, _| panic!("must reject before initialization")
        ),
        Err(Error::SourceNotDurable)
    ));
    db.finish_wal_sync_group().unwrap();
    assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
}

#[test]
fn identity_survives_database_rename_and_rejects_copied_registry() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    drop(consumer);
    drop(db);
    std::fs::rename(fixture.0.join("database"), fixture.0.join("moved")).unwrap();
    let mut db = Database::open(fixture.0.join("moved")).unwrap();
    let consumer = db
        .open_search_projection_consumer(&id("main"), fixture.0.join("projection"))
        .unwrap();
    drop(consumer);
    drop(db);
    let replacement = fixture.database();
    drop(replacement);
    let registry = fixture.registry_path("database");
    std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
    std::fs::copy(fixture.registry_path("moved"), registry).unwrap();
    let mut replacement = fixture.database();
    assert!(matches!(
        replacement.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
        Err(Error::RebuildRequired(Reason::DatabaseIdentityMismatch))
    ));
}

#[test]
fn registry_publication_error_never_adopts_the_unacknowledged_checkpoint() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    let old = std::fs::read(fixture.registry_path("database")).unwrap();
    append(&mut db, 1);
    PUBLICATION_FAILURE.set(Some((PublicationStage::BeforeRegistry, false)));
    assert!(catch_up(&mut db, &mut consumer).is_err());
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        old
    );
    assert!(!fixture
        .registry_path("database")
        .with_file_name("projection_consumers.meta.tmp")
        .exists());
    assert_eq!(
        consumer
            .search_index()
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        Some(db.runtime.get().unwrap().store.commit_epoch())
    );
    drop(consumer);
    drop(db);
    let mut db = fixture.database();
    assert!(matches!(
        db.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
        Err(Error::RebuildRequired(Reason::CheckpointMismatch))
    ));
}

#[test]
fn missing_and_corrupt_registry_leave_ordinary_database_writes_available() {
    for corrupt in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let consumer = db
            .create_search_projection_consumer(
                id("main"),
                fixture.0.join("projection"),
                options(100),
                initialize,
            )
            .unwrap();
        drop(consumer);
        drop(db);
        let registry = fixture.registry_path("database");
        if corrupt {
            std::fs::write(&registry, b"corrupt").unwrap();
        } else {
            std::fs::remove_file(&registry).unwrap();
        }
        let mut db = fixture.database();
        assert!(matches!(
            db.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
            Err(Error::RebuildRequired(Reason::RegistryUnavailable))
        ));
        append(&mut db, 1);
        let replacement = db
            .create_search_projection_consumer(
                id("main"),
                fixture.0.join("rebuilt"),
                options(100),
                initialize,
            )
            .unwrap();
        assert_eq!(replacement.search_index().document_count(), 1);
    }
}

#[test]
fn overflow_and_hydration_errors_do_not_publish_progress() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    assert!(db
        .create_search_projection_consumer(
            id("overflow"),
            fixture.0.join("overflow"),
            options(u64::MAX),
            |_, _| panic!("must reject overflow before initialization")
        )
        .is_err());
    let mut consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    append(&mut db, 1);
    let receipt = consumer.projection().receipt();
    let before = std::fs::read(fixture.registry_path("database")).unwrap();
    let result = db.catch_up_search_projection_consumer(&mut consumer, 1, 1, 1, |_, _| {
        Err(HawDBError::Execution("incomplete hydration".into()))
    });
    assert!(result.is_err());
    assert_eq!(consumer.projection().receipt(), receipt);
    assert_eq!(
        std::fs::read(fixture.registry_path("database")).unwrap(),
        before
    );
    assert_eq!(
        db.search_projection_consumer_status(&id("main"))
            .unwrap()
            .state,
        State::Active
    );
}

#[test]
fn consumer_crash_child() {
    let Ok(root) = std::env::var("HAWDB_CONSUMER_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let mut db = Database::open(root.join("database")).unwrap();
    let mut consumer = db
        .open_search_projection_consumer(&id("main"), root.join("projection"))
        .unwrap();
    let stage = match std::env::var("HAWDB_CONSUMER_CRASH_STAGE")
        .unwrap()
        .as_str()
    {
        "before_checkpoint" => PublicationStage::BeforeCheckpoint,
        "after_checkpoint" => PublicationStage::AfterCheckpoint,
        "after_registry" => PublicationStage::AfterRegistry,
        _ => panic!("unknown crash stage"),
    };
    PUBLICATION_FAILURE.set(Some((stage, true)));
    catch_up(&mut db, &mut consumer).unwrap();
    panic!("crash failpoint was not reached");
}

#[test]
fn process_crashes_observe_checkpoint_before_registry_ordering() {
    for stage in ["before_checkpoint", "after_checkpoint", "after_registry"] {
        let fixture = Fixture::new();
        let mut db = fixture.database();
        let consumer = db
            .create_search_projection_consumer(
                id("main"),
                fixture.0.join("projection"),
                options(100),
                initialize,
            )
            .unwrap();
        let old_epoch = consumer.projection().receipt().source_epoch;
        append(&mut db, 1);
        let source_epoch = db.runtime.get().unwrap().store.commit_epoch();
        drop(consumer);
        drop(db);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "api::search_projection_consumer::tests::consumer_crash_child",
                "--nocapture",
            ])
            .env("HAWDB_CONSUMER_CRASH_ROOT", &fixture.0)
            .env("HAWDB_CONSUMER_CRASH_STAGE", stage)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));
        let mut db = fixture.database();
        let opened = db.open_search_projection_consumer(&id("main"), fixture.0.join("projection"));
        if stage == "after_checkpoint" {
            assert!(matches!(
                opened,
                Err(Error::RebuildRequired(Reason::CheckpointMismatch))
            ));
        } else {
            let mut consumer = opened.unwrap();
            assert_eq!(
                consumer.projection().receipt().source_epoch,
                if stage == "before_checkpoint" {
                    old_epoch
                } else {
                    source_epoch
                }
            );
            assert!(catch_up(&mut db, &mut consumer).unwrap().catch_up.complete);
            assert_eq!(consumer.search_index().document_count(), 1);
        }
    }
}

#[test]
fn missing_mandatory_projection_artifact_requires_rebuild_without_repair() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    drop(consumer);
    drop(db);
    let descriptor = fixture
        .0
        .join("projection/search_projection_segments.hawdb");
    assert!(descriptor.exists());
    std::fs::remove_file(&descriptor).unwrap();
    let mut db = fixture.database();
    assert!(matches!(
        db.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
        Err(Error::RebuildRequired(Reason::CheckpointMismatch))
    ));
    assert!(!descriptor.exists());
}

#[test]
fn failed_registration_retry_preserves_other_durable_consumers() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let first = db
        .create_search_projection_consumer(
            id("first"),
            fixture.0.join("first"),
            options(100),
            initialize,
        )
        .unwrap();
    PUBLICATION_FAILURE.set(Some((PublicationStage::BeforeRegistry, false)));
    assert!(db
        .create_search_projection_consumer(
            id("failed"),
            fixture.0.join("failed"),
            options(100),
            initialize
        )
        .is_err());
    let second = db
        .create_search_projection_consumer(
            id("second"),
            fixture.0.join("second"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(
        db.runtime.get().unwrap().projection_consumers.records.len(),
        2
    );
    assert!(db
        .runtime
        .get()
        .unwrap()
        .projection_consumers
        .records
        .contains_key("first"));
    db.renew_search_projection_consumer(&first).unwrap();
    assert_eq!(
        db.search_projection_consumer_status(&id("first"))
            .unwrap()
            .state,
        State::Active
    );
    drop(second);
}

#[test]
fn consumer_registry_capacity_is_reclaimed_only_by_unregister() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    for index in 0..MAX_CONSUMERS {
        let name = format!("consumer-{index:02}");
        db.create_search_projection_consumer(
            id(&name),
            fixture.0.join(&name),
            options(1),
            initialize,
        )
        .unwrap();
    }
    append(&mut db, 1);
    assert!(matches!(
        db.search_projection_consumer_status(&id("consumer-00"))
            .unwrap()
            .state,
        State::RebuildRequired(Reason::Expired { .. })
    ));
    let result = db.create_search_projection_consumer(
        id("overflow"),
        fixture.0.join("overflow"),
        options(1000),
        |_, _| panic!("full registry must reject before initialization"),
    );
    assert!(matches!(result, Err(Error::RegistryFull)));
    db.unregister_search_projection_consumer(&id("consumer-00"))
        .unwrap();
    db.create_search_projection_consumer(
        id("replacement"),
        fixture.0.join("replacement"),
        options(1000),
        initialize,
    )
    .unwrap();
    assert_eq!(
        db.runtime.get().unwrap().projection_consumers.records.len(),
        MAX_CONSUMERS
    );
    assert!(
        std::fs::metadata(fixture.registry_path("database"))
            .unwrap()
            .len()
            <= 64 * 1024
    );
}

#[test]
fn acknowledgement_is_idempotent_monotonic_and_bound_to_the_complete_receipt() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    let receipt = consumer.projection().receipt();
    db.acknowledge_consumer_checkpoint(consumer.id(), &receipt)
        .unwrap();
    for variation in 0..5 {
        let mut invalid = receipt.clone();
        match variation {
            0 => invalid.source_epoch = db.runtime.get().unwrap().store.commit_epoch() + 1,
            1 => invalid.source_epoch -= 1,
            2 => invalid.sha256 = "0".repeat(64),
            3 => invalid.binding.checkpoint_uuid = generate_uuidv7().unwrap(),
            _ => invalid.binding.registration_uuid = generate_uuidv7().unwrap(),
        }
        assert!(matches!(
            db.acknowledge_consumer_checkpoint(consumer.id(), &invalid),
            Err(Error::RebuildRequired(Reason::CheckpointMismatch))
        ));
    }
    assert_eq!(
        db.search_projection_consumer_status(consumer.id())
            .unwrap()
            .durable_complete_through_commit_epoch,
        Some(receipt.source_epoch)
    );
}

#[test]
fn byte_retention_limit_does_not_wait_for_registry_io() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    db.runtime
        .get_mut()
        .unwrap()
        .store
        .set_max_search_projection_change_log_entries(None);
    db.runtime
        .get_mut()
        .unwrap()
        .store
        .set_max_search_projection_change_log_bytes(Some(1));
    let registry = fixture.registry_path("database");
    let old = std::fs::read(&registry).unwrap();
    append(&mut db, 1);
    let status = db.search_projection_consumer_status(consumer.id()).unwrap();
    assert!(status.changefeed.retained_bytes <= 1);
    assert!(matches!(
        status.state,
        State::RebuildRequired(Reason::RetentionLimitExceeded { .. })
    ));
    assert_eq!(std::fs::read(&registry).unwrap(), old);
    let readiness = db
        .search_projection_consumer_readiness(&consumer, None)
        .unwrap();
    assert!(!readiness.is_ready());
    assert_eq!(readiness.consumer.state, status.state);
    assert!(matches!(
        catch_up(&mut db, &mut consumer),
        Err(Error::RebuildRequired(
            Reason::RetentionLimitExceeded { .. }
        ))
    ));
}

#[test]
fn backup_restores_identity_and_rejects_a_cursor_ahead_of_the_restored_source() {
    let fixture = Fixture::new();
    let mut db = fixture.database();
    let mut consumer = db
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("projection"),
            options(100),
            initialize,
        )
        .unwrap();
    let identity = db
        .runtime
        .get()
        .unwrap()
        .store
        .search_projection_database_identity();
    db.backup_to(fixture.0.join("backup")).unwrap();
    append(&mut db, 1);
    catch_up(&mut db, &mut consumer).unwrap();
    drop(consumer);
    Database::restore_backup(fixture.0.join("backup"), fixture.0.join("restored")).unwrap();
    let mut restored = Database::open(fixture.0.join("restored")).unwrap();
    assert_eq!(
        restored
            .runtime
            .get()
            .unwrap()
            .store
            .search_projection_database_identity(),
        identity
    );
    assert!(matches!(
        restored.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
        Err(Error::RebuildRequired(Reason::RegistryUnavailable))
    ));
    drop(restored);
    std::fs::copy(
        fixture.registry_path("database"),
        fixture.registry_path("restored"),
    )
    .unwrap();
    let mut restored = Database::open(fixture.0.join("restored")).unwrap();
    assert!(matches!(
        restored.open_search_projection_consumer(&id("main"), fixture.0.join("projection")),
        Err(Error::RebuildRequired(Reason::SourceRewound))
    ));
}

#[test]
fn foreign_registry_reinitializes_only_after_the_new_projection_is_complete() {
    let fixture = Fixture::new();
    let mut source = fixture.database();
    append(&mut source, 1);
    let original = source
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("original"),
            options(100),
            initialize,
        )
        .unwrap();
    let source_identity = source
        .runtime
        .get()
        .unwrap()
        .store
        .search_projection_database_identity();
    let mut replacement = Database::open(fixture.0.join("replacement-database")).unwrap();
    append(&mut replacement, 2);
    drop(replacement);
    let foreign_bytes = std::fs::read(fixture.registry_path("database")).unwrap();
    let registry = fixture.registry_path("replacement-database");
    std::fs::write(&registry, &foreign_bytes).unwrap();
    let mut replacement = Database::open(fixture.0.join("replacement-database")).unwrap();
    let epoch = replacement.commit_epoch();
    assert!(matches!(
        replacement
            .search_projection_consumer_status(&id("main"))
            .unwrap()
            .state,
        State::RebuildRequired(Reason::DatabaseIdentityMismatch)
    ));
    assert!(replacement
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("failed"),
            options(100),
            |_, _| Err(HawDBError::Execution("incomplete mapping".into()))
        )
        .is_err());
    assert_eq!(std::fs::read(&registry).unwrap(), foreign_bytes);
    assert!(matches!(
        replacement
            .search_projection_consumer_status(&id("main"))
            .unwrap()
            .state,
        State::RebuildRequired(Reason::DatabaseIdentityMismatch)
    ));
    let rebuilt = replacement
        .create_search_projection_consumer(
            id("main"),
            fixture.0.join("rebuilt"),
            options(100),
            initialize,
        )
        .unwrap();
    assert_eq!(replacement.commit_epoch(), epoch);
    assert_ne!(
        replacement
            .runtime
            .get()
            .unwrap()
            .store
            .search_projection_database_identity(),
        source_identity
    );
    assert!(rebuilt.search_index().document("memory:m2").is_some());
    assert!(rebuilt.search_index().document("memory:m1").is_none());
    assert!(matches!(
        replacement.renew_search_projection_consumer(&original),
        Err(Error::RebuildRequired(Reason::DatabaseIdentityMismatch))
    ));
    drop(rebuilt);
    drop(replacement);
    let mut replacement = Database::open(fixture.0.join("replacement-database")).unwrap();
    replacement
        .open_search_projection_consumer(&id("main"), fixture.0.join("rebuilt"))
        .unwrap();
}

#[test]
fn stage_cleanup_preserves_unpublished_files_until_descriptor_capacity_returns() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    use hawdb_storage::file_io as counted;
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 4).unwrap();
    for explicit in [true, false] {
        let stage = fixture.0.join(format!("stage-{explicit}"));
        counted::create_dir(&stage).unwrap();
        let evidence = stage.join("unpublished");
        counted::write(&evidence, b"retain unpublished bytes").unwrap();
        let mut held = (0..4)
            .map(|_| counted::File::open(&evidence).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(project.metrics().open, 4);
        let rejections = project.metrics().budget_rejections;
        let guard = StageDirectory(stage.clone());
        if explicit {
            assert!(matches!(
                guard.cleanup(),
                Err(HawDBError::FileDescriptors(_))
            ));
        } else {
            drop(guard);
        }
        assert_eq!(
            std::fs::read(&evidence).unwrap(),
            b"retain unpublished bytes"
        );
        assert!(project.metrics().budget_rejections > rejections);
        assert_eq!(project.metrics().open, 4);
        assert_eq!(project.metrics().reserved, 0);
        drop(held.pop());
        StageDirectory(stage.clone()).cleanup().unwrap();
        assert!(!stage.exists());
        assert_eq!(project.metrics().open, 3);
        drop(held);
    }
    assert_eq!(project.metrics().open, 0);
    assert!(project.metrics().high_water <= 4);
}

#[test]
fn initializer_descriptor_rejection_preserves_typed_cause_and_source() {
    use hawdb_storage::file_io as counted;
    let fixture = Fixture::new();
    let root = fixture.0.join("database");
    let mut db = Database::open_with_config(
        &root,
        crate::DatabaseConfig {
            max_open_files: 32,
            ..crate::DatabaseConfig::default()
        },
    )
    .unwrap();
    append(&mut db, 1);
    let epoch = db.commit_epoch().unwrap();
    let branch = db.current_branch().unwrap().unwrap().info.id;
    let destination = root.join("projection");
    let project =
        hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_existing(&root, 32)
            .unwrap();
    let mut held = Vec::new();
    let mut evidence = None;
    let error = db
        .create_search_projection_consumer(id("fd-retry"), &destination, options(100), |_, _| {
            let stage = std::fs::read_dir(&root)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .ends_with(".stage")
                })
                .unwrap();
            let file = stage.join("unpublished");
            counted::write(&file, b"retain initializer evidence").unwrap();
            let error = loop {
                match counted::File::open(&file) {
                    Ok(file) => held.push(file),
                    Err(error) => break HawDBError::from(error),
                }
            };
            assert!(matches!(error, HawDBError::FileDescriptors(_)));
            assert_eq!(project.metrics().open, 32);
            evidence = Some(file);
            Err(error)
        })
        .unwrap_err();
    assert!(
        matches!(error, Error::Database(HawDBError::FileDescriptors(_))),
        "expected a typed FD failure, got {error:?}"
    );
    let evidence = evidence.unwrap();
    // Failed initialization releases its publication lock before cleanup;
    // that one returned descriptor lets the bounded walker remove the stage.
    assert!(!evidence.exists());
    assert!(!destination.exists());
    assert!(db.file_descriptor_metrics().unwrap().open < 32);
    assert_eq!(db.file_descriptor_metrics().unwrap().reserved, 0);
    drop(held);
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(db.current_branch().unwrap().unwrap().info.id, branch);
    assert!(db
        .query("MATCH (m:Memory) RETURN m.id AS id")
        .unwrap()
        .rows
        .iter()
        .any(|row| row["id"] == crate::Value::String("m1".into())));
    let consumer = db
        .create_search_projection_consumer(id("fd-retry"), &destination, options(100), initialize)
        .unwrap();
    assert!(consumer.search_index().document("memory:m1").is_some());
    assert!(db
        .search_projection_consumer_readiness(&consumer, None)
        .unwrap()
        .is_ready());
    assert!(db.file_descriptor_metrics().unwrap().high_water <= 32);
    drop(consumer);
    drop(db);
    let mut reopened = Database::open_with_config(
        &root,
        crate::DatabaseConfig {
            max_open_files: 32,
            ..crate::DatabaseConfig::default()
        },
    )
    .unwrap();
    let consumer = reopened
        .open_search_projection_consumer(&id("fd-retry"), &destination)
        .unwrap();
    assert!(consumer.search_index().document("memory:m1").is_some());
}
