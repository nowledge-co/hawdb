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

use hawdb::{
    Database, DurabilityPolicy, PreparedReadLimits, ProjectionGenerationBatchLimits,
    ProjectionGenerationBegin, ProjectionGenerationDigestBuilder, ProjectionGenerationError,
    ProjectionGenerationIdentity, ProjectionGenerationMember, ProjectionGenerationStore,
    ProjectionPublicationError, SealedProjectionGeneration, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

static NEXT_TEST_ROOT: AtomicU64 = AtomicU64::new(0);

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
                "hawdb_mvcc_publication_{}_{}_{}",
                std::process::id(),
                NEXT_TEST_ROOT.fetch_add(1, Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )))
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn limits() -> PreparedReadLimits {
    PreparedReadLimits {
        dependency_entries: NonZeroUsize::new(32).unwrap(),
        dependency_bytes: NonZeroUsize::new(4096).unwrap(),
        statement_rows: NonZeroUsize::new(32).unwrap(),
        statement_payload_bytes: NonZeroUsize::new(4096).unwrap(),
    }
}

fn fixture(policy: DurabilityPolicy) -> (TestRoot, Database) {
    let root = TestRoot::new();
    let mut db = Database::open_with_durability(&root.0, policy).unwrap();
    db.query("CREATE (:Memory {id: 'a', value: 1})").unwrap();
    db.query("CREATE (:Other {id: 'b', value: 1})").unwrap();
    (root, db)
}

fn read(db: &Database) -> hawdb::PreparedReadDependencies {
    let mut read = db.begin_prepared_read(limits()).unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("a".into()))]);
    let output = read
        .query_with_params(
            "MATCH (n:Memory) WHERE n.id = $id RETURN n.value AS value",
            &parameters,
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    let dependencies = read.finish().unwrap();
    assert!(!dependencies.requires_unchanged_database_epoch());
    dependencies
}

fn seal(
    store: &ProjectionGenerationStore,
    generation: &str,
    epoch: u64,
    expected: Option<&str>,
) -> SealedProjectionGeneration {
    let mut writer = store
        .begin_candidate(
            ProjectionGenerationBegin {
                identity: ProjectionGenerationIdentity {
                    projection: "test_mvcc".into(),
                    owner_key: b"workspace".to_vec(),
                    generation: generation.into(),
                },
                source_watermark: epoch,
                projection_version: 1,
                expected_head: expected.map(str::to_owned),
            },
            ProjectionGenerationBatchLimits::default(),
        )
        .unwrap();
    let member = ProjectionGenerationMember {
        collection: "values".into(),
        key: b"a".to_vec(),
        payload: b"one".to_vec(),
    };
    writer.append_batch(std::slice::from_ref(&member)).unwrap();
    let mut digest = ProjectionGenerationDigestBuilder::default();
    digest.update(&member).unwrap();
    writer.seal(digest.finish()).unwrap()
}

#[test]
fn disjoint_writer_commits_while_query_preparation_is_paused() {
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    let store = db.projection_generation_store().unwrap();
    let dependencies = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let worker = scope.spawn(move || {
            let output = preparation
                .query_with_params("MATCH (n:Memory) RETURN n.id AS id", &BTreeMap::new())
                .unwrap();
            assert_eq!(output.output.rows.len(), 1);
            ready_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            preparation.finish().unwrap()
        });
        ready_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        let epoch = db.commit_epoch().unwrap();
        db.query("MATCH (n:Other) SET n.value = 2").unwrap();
        assert!(db.commit_epoch().unwrap() > epoch);
        resume_tx.send(()).unwrap();
        worker.join().unwrap()
    });
    let staged = seal(&store, "g1", dependencies.source_epoch(), None);
    assert!(matches!(
        store.open_active("test_mvcc", b"workspace"),
        Err(ProjectionGenerationError::NotFound(_))
    ));
    let report = db
        .publish_prepared_projection(&dependencies, &staged)
        .unwrap();
    assert!(!report.idempotent);
    assert_eq!(
        store
            .open_active("test_mvcc", b"workspace")
            .unwrap()
            .manifest(),
        staged.manifest()
    );
}

#[test]
fn relevant_write_and_schema_change_reject_without_publishing() {
    for schema in [false, true] {
        let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        let dependencies = read(&db);
        let store = db.projection_generation_store().unwrap();
        let staged = seal(&store, "g1", dependencies.source_epoch(), None);
        db.query(if schema {
            "CREATE INDEX ON :Memory(id)"
        } else {
            "MATCH (n:Memory) SET n.value = 2"
        })
        .unwrap();
        assert!(matches!(
            db.publish_prepared_projection(&dependencies, &staged),
            Err(ProjectionPublicationError::Dependency(_))
        ));
        assert!(matches!(
            store.open_active("test_mvcc", b"workspace"),
            Err(ProjectionGenerationError::NotFound(_))
        ));
    }
}

#[test]
fn empty_predicate_result_still_detects_concurrent_insertion() {
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("absent".into()))]);
    let output = preparation
        .query_with_params(
            "MATCH (n:Memory) WHERE n.id = $id RETURN n.id AS id",
            &parameters,
        )
        .unwrap();
    assert!(output.output.rows.is_empty());
    let dependencies = preparation.finish().unwrap();
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "g1", dependencies.source_epoch(), None);
    db.query("CREATE (:Memory {id: 'absent'})").unwrap();
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn target_head_cas_rejects_another_prepared_candidate() {
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let dependencies = read(&db);
    let store = db.projection_generation_store().unwrap();
    let first = seal(&store, "g1", dependencies.source_epoch(), None);
    let second = seal(&store, "g2", dependencies.source_epoch(), None);
    db.publish_prepared_projection(&dependencies, &first)
        .unwrap();
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &second),
        Err(ProjectionPublicationError::Generation(
            ProjectionGenerationError::Conflict(_)
        ))
    ));
    assert_eq!(
        store
            .open_active("test_mvcc", b"workspace")
            .unwrap()
            .manifest(),
        first.manifest()
    );
}

#[test]
fn reopen_invalidates_uncommitted_source_but_resolves_committed_lost_response() {
    for committed in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        let dependencies = read(&db);
        let store = db.projection_generation_store().unwrap();
        let staged = seal(&store, "g1", dependencies.source_epoch(), None);
        if committed {
            db.publish_prepared_projection(&dependencies, &staged)
                .unwrap();
            db.query("MATCH (n:Memory) SET n.value = 2").unwrap();
        }
        drop(db);
        let mut reopened = Database::open(&root.0).unwrap();
        let result = reopened.publish_prepared_projection(&dependencies, &staged);
        if committed {
            assert!(result.unwrap().idempotent);
        } else {
            assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceChanged)
            ));
        }
    }
}

#[test]
fn watermark_and_source_durability_refusals_leave_head_absent() {
    for relaxed in [false, true] {
        let (_root, mut db) = fixture(if relaxed {
            DurabilityPolicy::SyncOnCheckpoint
        } else {
            DurabilityPolicy::SyncOnEveryWrite
        });
        let dependencies = read(&db);
        let store = db.projection_generation_store().unwrap();
        let staged = seal(
            &store,
            "g1",
            dependencies.source_epoch() + u64::from(!relaxed),
            None,
        );
        let result = db.publish_prepared_projection(&dependencies, &staged);
        if relaxed {
            assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceNotDurable)
            ));
        } else {
            assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceEpochMismatch { .. })
            ));
        }
        assert!(matches!(
            store.open_active("test_mvcc", b"workspace"),
            Err(ProjectionGenerationError::NotFound(_))
        ));
    }
}

#[test]
fn ignored_budget_or_unsupported_statement_error_cannot_produce_source_proof() {
    let (_root, db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let mut tiny = limits();
    tiny.statement_payload_bytes = NonZeroUsize::new(1).unwrap();
    let mut preparation = db.begin_prepared_read(tiny).unwrap();
    assert!(preparation
        .query_with_params("MATCH (n:Memory) RETURN n.id AS id", &BTreeMap::new())
        .is_err());
    assert!(preparation.finish().is_err());
    let mut unsupported = db.begin_prepared_read(limits()).unwrap();
    assert!(unsupported
        .query_with_params(
            "MATCH (n:Memory)-[:LINK]->(m:Memory) RETURN m.id AS id",
            &BTreeMap::new()
        )
        .is_err());
    assert!(unsupported.finish().is_err());
    // An adjacency predicate may hide beneath a node scan/filter instead of
    // appearing as a separate traversal operator. It needs its own collector.
    let mut predicate = db.begin_prepared_read(limits()).unwrap();
    assert!(predicate
        .query_with_params(
            "MATCH (n:Memory) WHERE NOT (n)-[:LINK]->(:Other) RETURN n.id AS id",
            &BTreeMap::new(),
        )
        .is_err());
    assert!(predicate.finish().is_err());
}

#[test]
fn explicit_source_barrier_allows_relaxed_publication_without_changing_future_commits() {
    use hawdb::ProjectionSourceDurability;
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnCheckpoint);
    let dependencies = read(&db);
    let store = db.projection_generation_store().unwrap();
    let first = seal(&store, "g1", dependencies.source_epoch(), None);
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &first),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
    db.publish_prepared_projection_with_source_durability(
        &dependencies,
        &first,
        ProjectionSourceDurability::Synchronize,
    )
    .unwrap();
    db.query("MATCH (n:Other) SET n.value = 2").unwrap();
    let later = read(&db);
    assert!(later.source_epoch() > dependencies.source_epoch());
    let second = seal(&store, "g2", later.source_epoch(), Some("g1"));
    assert!(matches!(
        db.publish_prepared_projection(&later, &second),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
    db.publish_prepared_projection_with_source_durability(
        &later,
        &second,
        ProjectionSourceDurability::Synchronize,
    )
    .unwrap();
    assert_eq!(
        store
            .open_active("test_mvcc", b"workspace")
            .unwrap()
            .manifest(),
        second.manifest()
    );
}

#[test]
fn reopening_with_sync_policy_does_not_certify_a_relaxed_wal_prefix() {
    use hawdb::ProjectionSourceDurability;
    let (root, db) = fixture(DurabilityPolicy::SyncOnCheckpoint);
    drop(db);
    let mut reopened = Database::open(&root.0).unwrap();
    let dependencies = read(&reopened);
    let store = reopened.projection_generation_store().unwrap();
    let staged = seal(&store, "g1", dependencies.source_epoch(), None);
    assert!(matches!(
        reopened.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
    reopened
        .publish_prepared_projection_with_source_durability(
            &dependencies,
            &staged,
            ProjectionSourceDurability::Synchronize,
        )
        .unwrap();
}

#[test]
fn completed_checkpoint_certifies_a_relaxed_source_prefix() {
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnCheckpoint);
    db.query("CHECKPOINT").unwrap();
    let dependencies = read(&db);
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "g1", dependencies.source_epoch(), None);
    db.publish_prepared_projection(&dependencies, &staged)
        .unwrap();
}

#[test]
fn dependency_conflict_is_rejected_before_synchronizing_source() {
    use hawdb::ProjectionSourceDurability;
    let (_root, mut db) = fixture(DurabilityPolicy::SyncOnCheckpoint);
    let dependencies = read(&db);
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "stale", dependencies.source_epoch(), None);
    db.query("MATCH (n:Memory) SET n.value = 2").unwrap();
    assert!(matches!(
        db.publish_prepared_projection_with_source_durability(
            &dependencies,
            &staged,
            ProjectionSourceDurability::Synchronize
        ),
        Err(ProjectionPublicationError::Dependency(_))
    ));
    let fresh = read(&db);
    let retry = seal(&store, "fresh", fresh.source_epoch(), None);
    assert!(matches!(
        db.publish_prepared_projection(&fresh, &retry),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
    assert!(matches!(
        store.open_active("test_mvcc", b"workspace"),
        Err(ProjectionGenerationError::NotFound(_))
    ));
}

fn indexed_fixture() -> (TestRoot, Database) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query("CREATE (:Memory {id: 'unrelated', value: 0})")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    (root, db)
}

#[test]
fn scalar_index_preparation_accepts_same_label_disjoint_write_before_first_query() {
    let (_root, mut db) = indexed_fixture();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    // Registration precedes knowledge of which property/value the later read
    // will use. Do not lose writes in this capture-to-query window.
    db.query("MATCH (n:Memory) WHERE n.id = 'unrelated' SET n.value = 2")
        .unwrap();
    preparation
        .query_with_params(
            "MATCH (n:Memory) WHERE n.id = $id RETURN n.value AS value",
            &BTreeMap::from([("id".into(), Value::String("a".into()))]),
        )
        .unwrap();
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_node_property_count(), 1);
    assert_eq!(dependencies.node_domain_count(), 0);
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "exact", dependencies.source_epoch(), None);
    db.publish_prepared_projection(&dependencies, &staged)
        .unwrap();
}

#[test]
fn scalar_index_read_detects_payload_update_in_capture_to_query_window() {
    let (_root, mut db) = indexed_fixture();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    db.query("MATCH (n:Memory) WHERE n.id = 'a' SET n.value = 9")
        .unwrap();
    let result = preparation
        .query_with_params(
            "MATCH (n:Memory) WHERE n.id = $id RETURN n.value AS value",
            &BTreeMap::from([("id".into(), Value::String("a".into()))]),
        )
        .unwrap();
    assert_eq!(result.output.rows[0]["value"], Value::Int(1));
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_node_property_count(), 1);
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "stale", dependencies.source_epoch(), None);
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn scalar_index_absence_detects_insert_and_move_into_key() {
    for inserted in [false, true] {
        let (_root, mut db) = indexed_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        let output = preparation
            .query_with_params(
                "MATCH (n:Memory) WHERE n.id = $id RETURN n.value AS value",
                &BTreeMap::from([("id".into(), Value::String("absent".into()))]),
            )
            .unwrap();
        assert!(output.output.rows.is_empty());
        let dependencies = preparation.finish().unwrap();
        assert_eq!(dependencies.exact_node_property_count(), 1);
        assert_eq!(dependencies.node_domain_count(), 0);
        db.query(if inserted {
            "CREATE (:Memory {id: 'absent', value: 1})"
        } else {
            "MATCH (n:Memory) WHERE n.id = 'unrelated' SET n.id = 'absent'"
        })
        .unwrap();
        let store = db.projection_generation_store().unwrap();
        let staged = seal(&store, "absent", dependencies.source_epoch(), None);
        assert!(matches!(
            db.publish_prepared_projection(&dependencies, &staged),
            Err(ProjectionPublicationError::Dependency(_))
        ));
    }
}

#[test]
fn scalar_index_read_detects_delete_and_move_out_of_key() {
    for deleted in [false, true] {
        let (_root, mut db) = indexed_fixture();
        let dependencies = read(&db);
        assert_eq!(dependencies.exact_node_property_count(), 1);
        db.query(if deleted {
            "MATCH (n:Memory) WHERE n.id = 'a' DELETE n"
        } else {
            "MATCH (n:Memory) WHERE n.id = 'a' SET n.id = 'moved'"
        })
        .unwrap();
        let store = db.projection_generation_store().unwrap();
        let staged = seal(&store, "removed", dependencies.source_epoch(), None);
        assert!(matches!(
            db.publish_prepared_projection(&dependencies, &staged),
            Err(ProjectionPublicationError::Dependency(_))
        ));
    }
}

#[test]
fn scalar_multi_seek_collects_all_absent_and_present_keys() {
    let (_root, mut db) = indexed_fixture();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    preparation
        .query_with_params(
            "MATCH (n:Memory) WHERE n.id IN $ids RETURN n.value AS value",
            &BTreeMap::from([(
                "ids".into(),
                Value::List(vec![
                    Value::String("a".into()),
                    Value::String("absent".into()),
                ]),
            )]),
        )
        .unwrap();
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_node_property_count(), 2);
    assert_eq!(dependencies.node_domain_count(), 0);
    db.query("CREATE (:Memory {id: 'absent', value: 5})")
        .unwrap();
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "multi", dependencies.source_epoch(), None);
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn exact_index_dependencies_preserve_out_of_core_disjoint_writes_with_large_bodies() {
    use hawdb::{DatabaseConfig, StorageResidencyMode};
    for related in [false, true] {
        let (root, mut db) = indexed_fixture();
        db.query_with_params(
            "MATCH (n:Memory) SET n.body = $body",
            &BTreeMap::from([("body".into(), Value::String("x".repeat(1024 * 1024)))]),
        )
        .unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(
            &root.0,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert!(db.storage_residency_report().unwrap().out_of_core);
        assert_eq!(db.storage_residency_report().unwrap().delta_node_count, 0);
        let dependencies = read(&db);
        assert_eq!(dependencies.exact_node_property_count(), 1);
        assert_eq!(dependencies.node_domain_count(), 0);
        assert_eq!(db.storage_residency_report().unwrap().delta_node_count, 0);
        db.query_with_params(
            "MATCH (n:Memory) WHERE n.id = $id SET n.value = 2",
            &BTreeMap::from([(
                "id".into(),
                Value::String(if related { "a" } else { "unrelated" }.into()),
            )]),
        )
        .unwrap();
        let store = db.projection_generation_store().unwrap();
        let staged = seal(&store, "cold", dependencies.source_epoch(), None);
        let result = db.publish_prepared_projection(&dependencies, &staged);
        if related {
            assert!(matches!(
                result,
                Err(ProjectionPublicationError::Dependency(_))
            ));
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn bounded_pipeline_with_outer_filter_cannot_produce_partial_point_proof() {
    let (_root, mut db) = indexed_fixture();
    let query = "MATCH (n:Memory) WITH n ORDER BY n.value ASC LIMIT 1 WITH n WHERE n.id = $id RETURN n.value AS value";
    let parameters = BTreeMap::from([("id".into(), Value::String("a".into()))]);
    assert!(db
        .query_with_params(query, &parameters)
        .unwrap()
        .rows
        .is_empty());
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    // This pipeline currently lowers to an unqualified GraphMatch program.
    // A proof for only the outer exact key would miss the other row's rank.
    assert!(preparation.query_with_params(query, &parameters).is_err());
    assert!(preparation.finish().is_err());
    db.query("MATCH (n:Memory) WHERE n.id = 'unrelated' SET n.value = 10")
        .unwrap();
    assert_eq!(
        db.query_with_params(query, &parameters).unwrap().rows.len(),
        1
    );
}

#[test]
fn disjunction_does_not_turn_one_possible_key_into_a_necessary_dependency() {
    let (_root, mut db) = indexed_fixture();
    let query = "MATCH (n:Memory) WHERE n.id = $id OR n.value = 0 RETURN n.value AS value";
    let parameters = BTreeMap::from([("id".into(), Value::String("a".into()))]);
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    assert_eq!(
        preparation
            .query_with_params(query, &parameters)
            .unwrap()
            .output
            .rows
            .len(),
        2
    );
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_node_property_count(), 0);
    assert_eq!(dependencies.node_domain_count(), 1);
    db.query("MATCH (n:Memory) WHERE n.id = 'unrelated' SET n.value = 10")
        .unwrap();
    let store = db.projection_generation_store().unwrap();
    let staged = seal(&store, "disjunction", dependencies.source_epoch(), None);
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn multiple_statements_share_one_dependency_budget_without_partial_proof() {
    let (_root, db) = indexed_fixture();
    let mut bounded = limits();
    bounded.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut preparation = db.begin_prepared_read(bounded).unwrap();
    let query = "MATCH (n:Memory) WHERE n.id = $id RETURN n.value AS value";
    preparation
        .query_with_params(
            query,
            &BTreeMap::from([("id".into(), Value::String("a".into()))]),
        )
        .unwrap();
    assert!(preparation
        .query_with_params(
            query,
            &BTreeMap::from([("id".into(), Value::String("unrelated".into()))])
        )
        .is_err());
    assert!(preparation.finish().is_err());
}

fn prepared_search_candidate(
    db: &Database,
    root: &std::path::Path,
    partitioned: bool,
) -> (
    hawdb::PreparedReadDependencies,
    hawdb::SearchOutOfCorePreparedGeneration,
) {
    let mut read = db.begin_prepared_read(limits()).unwrap();
    let result = read
        .query_with_params(
            "MATCH (n:Memory) RETURN n.id AS id, n.value AS value",
            &BTreeMap::new(),
        )
        .unwrap();
    let epoch = read.source_epoch();
    let dependencies = read.finish().unwrap();
    let mut writer = hawdb::SearchOutOfCoreGenerationWriter::create(
        root,
        hawdb::SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(epoch),
            max_content_documents: NonZeroUsize::new(if partitioned { 1 } else { 100 }).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    for row in result.output.rows {
        let Value::String(id) = &row["id"] else {
            panic!("expected string id")
        };
        let Value::Int(value) = row["value"] else {
            panic!("expected integer payload")
        };
        writer
            .push(hawdb::SearchDocument {
                id: id.clone(),
                title: id.clone(),
                content: value.to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    (dependencies, writer.prepare_publication().unwrap())
}

#[test]
fn source_validated_search_activates_complete_single_and_partitioned_candidates() {
    for partitioned in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query("CREATE (:Memory {id: 'c', value: 3})").unwrap();
        let search = root.0.join("derived_search");
        let (dependencies, candidate) = prepared_search_candidate(&db, &search, partitioned);
        let captured = dependencies.source_epoch();
        db.query("MATCH (n:Other) SET n.value = 2").unwrap();
        assert!(!search
            .join("search_projection.out_of_core.manifest.hawdb")
            .exists());
        let report = db
            .publish_prepared_search(&dependencies, candidate)
            .unwrap();
        assert_eq!(report.source_graph_commit_epoch, Some(captured));
        drop(db);
        let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
        assert_eq!(reader.source_graph_commit_epoch(), Some(captured));
        assert_eq!(reader.document_count(), 2);
        let docs = reader
            .hydrate_documents(&["a".into(), "c".into()])
            .unwrap()
            .documents;
        assert_eq!(
            docs.iter()
                .map(|doc| doc.content.as_str())
                .collect::<Vec<_>>(),
            ["1", "3"]
        );
        assert_eq!(
            report.published_content_segments,
            if partitioned { 2 } else { 1 }
        );
    }
}

#[test]
fn related_graph_mutations_reject_before_search_visibility_or_watermark() {
    for partitioned in [false, true] {
        let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query("CREATE (:Memory {id: 'c', value: 3})").unwrap();
        let search = root.0.join("derived_search");
        let (dependencies, candidate) = prepared_search_candidate(&db, &search, partitioned);
        db.query("MATCH (n:Memory) WHERE n.id = 'a' SET n.value = 2")
            .unwrap();
        assert!(matches!(
            db.publish_prepared_search(&dependencies, candidate),
            Err(ProjectionPublicationError::Dependency(_))
        ));
        assert!(hawdb::SearchOutOfCoreReader::open(&search).is_err());
        assert!(!std::fs::read_dir(&search).unwrap().any(|entry| entry
            .unwrap()
            .file_type()
            .unwrap()
            .is_dir()));
    }
}

#[test]
fn source_validated_search_requires_explicit_observed_durability() {
    use hawdb::ProjectionSourceDurability;
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnCheckpoint);
    let search = root.0.join("derived_search");
    let (dependencies, candidate) = prepared_search_candidate(&db, &search, false);
    assert!(matches!(
        db.publish_prepared_search(&dependencies, candidate),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
    assert!(hawdb::SearchOutOfCoreReader::open(&search).is_err());
    let (dependencies, candidate) = prepared_search_candidate(&db, &search, false);
    let epoch = dependencies.source_epoch();
    db.publish_prepared_search_with_source_durability(
        &dependencies,
        candidate,
        ProjectionSourceDurability::Synchronize,
    )
    .unwrap();
    let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
    assert_eq!(reader.source_graph_commit_epoch(), Some(epoch));
    db.query("MATCH (n:Other) SET n.value = 7").unwrap();
    let next_search = root.0.join("later_search");
    let (later, candidate) = prepared_search_candidate(&db, &next_search, false);
    assert!(matches!(
        db.publish_prepared_search(&later, candidate),
        Err(ProjectionPublicationError::SourceNotDurable)
    ));
}

#[test]
fn source_validated_search_rejects_missing_watermark_wrong_epoch_and_foreign_source() {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let (_foreign_root, mut foreign) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    for failure in ["missing", "epoch", "instance"] {
        let search = root.0.join(failure);
        let dependencies = read(&db);
        let watermark = match failure {
            "missing" => None,
            "epoch" => Some(dependencies.source_epoch() + 1),
            _ => Some(dependencies.source_epoch()),
        };
        let mut writer = hawdb::SearchOutOfCoreGenerationWriter::create(
            &search,
            hawdb::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: watermark,
                ..Default::default()
            },
        )
        .unwrap();
        writer
            .push(hawdb::SearchDocument {
                id: "a".into(),
                title: "a".into(),
                content: "1".into(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let candidate = writer.prepare_publication().unwrap();
        let result = if failure == "instance" {
            foreign.publish_prepared_search(&dependencies, candidate)
        } else {
            db.publish_prepared_search(&dependencies, candidate)
        };
        match failure {
            "missing" => assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceWatermarkMissing)
            )),
            "epoch" => assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceEpochMismatch { .. })
            )),
            _ => assert!(matches!(
                result,
                Err(ProjectionPublicationError::SourceChanged)
            )),
        }
        assert!(hawdb::SearchOutOfCoreReader::open(&search).is_err());
    }
}

#[test]
fn source_validated_incremental_search_keeps_the_backend_base_cas() {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let search = root.0.join("derived_search");
    let (dependencies, initial) = prepared_search_candidate(&db, &search, false);
    db.publish_prepared_search(&dependencies, initial).unwrap();
    let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
    let dependencies = read(&db);
    let epoch = dependencies.source_epoch();
    let prepare = || {
        hawdb::SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            hawdb::SearchProjectionDelta {
                upserts: vec![hawdb::SearchProjectionRow {
                    kind: hawdb::SearchProjectionKind::Memory,
                    external_id: "a".into(),
                    title: "a".into(),
                    body: "1".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["a".into()],
                source_graph_commit_epoch: Some(epoch),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .prepare_publication()
        .unwrap()
    };
    let first = prepare();
    let stale = prepare();
    let (delta, report, _) = db.publish_prepared_search(&dependencies, first).unwrap();
    assert_eq!(delta.deleted_documents, 1);
    assert!(matches!(
        db.publish_prepared_search(&dependencies, stale),
        Err(ProjectionPublicationError::Storage(
            hawdb::HawDBError::TransactionConflict { .. }
        ))
    ));
    drop(reader);
    let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(reader.source_graph_commit_epoch(), Some(epoch));
    assert_eq!(reader.document_count(), 1);
    assert_eq!(
        reader
            .hydrate_documents(&["memory:a".into()])
            .unwrap()
            .documents[0]
            .content,
        "1"
    );
}

#[test]
fn older_valid_source_proof_cannot_regress_an_already_newer_search_watermark() {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let old = read(&db);
    db.query("MATCH (n:Other) SET n.value = 9").unwrap();
    let search = root.0.join("derived_search");
    let (newer, candidate) = prepared_search_candidate(&db, &search, false);
    let report = db.publish_prepared_search(&newer, candidate).unwrap();
    assert!(newer.source_epoch() > old.source_epoch());
    // Capture the current Search base after it already covers a newer source.
    // Disjoint Graph stamps alone still validate the old proof; watermark
    // non-regression is a separate required publication condition.
    let mut writer = hawdb::SearchOutOfCoreGenerationWriter::create(
        &search,
        hawdb::SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(old.source_epoch()),
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .push(hawdb::SearchDocument {
            id: "a".into(),
            title: "a".into(),
            content: "1".into(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    let candidate = writer.prepare_publication().unwrap();
    assert_eq!(
        candidate.expected_active_generation(),
        Some(report.generation)
    );
    assert!(matches!(db.publish_prepared_search(&old, candidate),
        Err(ProjectionPublicationError::SourceWatermarkRegression { previous, staged })
        if previous == newer.source_epoch() && staged == old.source_epoch()));
    let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(
        reader.source_graph_commit_epoch(),
        Some(newer.source_epoch())
    );
}

#[test]
fn source_validated_search_does_not_infer_a_watermark_from_a_damaged_base() {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    let search = root.0.join("derived_search");
    let (dependencies, candidate) = prepared_search_candidate(&db, &search, false);
    db.publish_prepared_search(&dependencies, candidate)
        .unwrap();
    let selector = search.join("search_projection.out_of_core.manifest.hawdb");
    std::fs::write(&selector, b"damaged selector evidence").unwrap();
    let (dependencies, candidate) = prepared_search_candidate(&db, &search, false);
    assert!(matches!(
        db.publish_prepared_search(&dependencies, candidate),
        Err(ProjectionPublicationError::Storage(
            hawdb::HawDBError::StorageIntegrity(_)
        ))
    ));
    assert_eq!(
        std::fs::read(selector).unwrap(),
        b"damaged selector evidence"
    );
    assert!(!std::fs::read_dir(search).unwrap().any(|entry| entry
        .unwrap()
        .file_type()
        .unwrap()
        .is_dir()));
}
