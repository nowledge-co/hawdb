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
    Database, DurabilityPolicy, ProjectionGenerationError, ProjectionPublicationError, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

#[path = "support/mvcc_fixture.rs"]
mod support;
use support::{fixture, limits, seal, TestRoot};

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

fn relational_fixture() -> (TestRoot, Database) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query_sql(
        "CREATE TABLE records (tenant TEXT, id BIGINT, body TEXT, PRIMARY KEY (tenant, id))",
    )
    .unwrap();
    db.query_sql(
        "INSERT INTO records (tenant, id, body) VALUES ('w', 1, 'one'), ('w', 2, 'two'), ('other', 1, 'other')",
    )
    .unwrap();
    (root, db)
}

fn sql_point(preparation: &mut hawdb::PreparedRead, id: i64) -> hawdb::QueryOutput {
    preparation
        .query_sql_with_params(
            "SELECT r.body FROM public.records AS r WHERE r.tenant = $1 AND r.id = $2",
            &[Value::String("w".into()), Value::Int(id)],
        )
        .unwrap()
}

#[test]
fn relational_point_dependencies_cover_the_capture_to_first_query_gap() {
    for related in [false, true] {
        let (_root, mut db) = relational_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        db.query_sql(if related {
            "UPDATE records SET body = 'changed' WHERE tenant = 'w' AND id = 1"
        } else {
            "UPDATE records SET body = 'changed' WHERE tenant = 'w' AND id = 2"
        })
        .unwrap();
        let output = sql_point(&mut preparation, 1);
        assert_eq!(output.rows[0]["body"], Value::String("one".into()));
        let dependencies = preparation.finish().unwrap();
        assert_eq!(dependencies.exact_relational_row_count(), 1);
        assert!(!dependencies.requires_unchanged_database_epoch());
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "sql_point",
            dependencies.source_epoch(),
            None,
        );
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
fn absent_relational_primary_keys_detect_phantoms_without_rejecting_other_keys() {
    for related in [false, true] {
        let (_root, mut db) = relational_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        assert!(sql_point(&mut preparation, 99).rows.is_empty());
        let dependencies = preparation.finish().unwrap();
        db.query_sql(if related {
            "INSERT INTO records (tenant, id, body) VALUES ('w', 99, 'phantom')"
        } else {
            "INSERT INTO records (tenant, id, body) VALUES ('elsewhere', 99, 'disjoint')"
        })
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "sql_absent",
            dependencies.source_epoch(),
            None,
        );
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
fn relational_payload_deletion_aba_and_schema_changes_invalidate_point_reads() {
    for change in ["payload", "delete", "aba", "schema"] {
        let (_root, mut db) = relational_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        sql_point(&mut preparation, 1);
        let dependencies = preparation.finish().unwrap();
        match change {
            "delete" => {
                db.query_sql("DELETE FROM records WHERE tenant = 'w' AND id = 1")
                    .unwrap();
            }
            "schema" => {
                db.query_sql("ALTER TABLE records ADD COLUMN extra TEXT")
                    .unwrap();
            }
            _ => {
                db.query_sql("UPDATE records SET body = 'temporary' WHERE tenant = 'w' AND id = 1")
                    .unwrap();
                if change == "aba" {
                    db.query_sql("UPDATE records SET body = 'one' WHERE tenant = 'w' AND id = 1")
                        .unwrap();
                }
            }
        }
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "sql_changed",
            dependencies.source_epoch(),
            None,
        );
        assert!(matches!(
            db.publish_prepared_projection(&dependencies, &staged),
            Err(ProjectionPublicationError::Dependency(_))
        ));
    }
}

#[test]
fn relational_or_and_partial_composite_keys_retain_complete_table_domains() {
    for query in [
        "SELECT body FROM records WHERE tenant = 'w' OR id = 1",
        "SELECT body FROM records WHERE tenant = 'w'",
        "SELECT body FROM records WHERE tenant = 'w' AND id = NULL",
    ] {
        let (_root, mut db) = relational_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        preparation.query_sql_with_params(query, &[]).unwrap();
        let dependencies = preparation.finish().unwrap();
        assert_eq!(dependencies.exact_relational_row_count(), 0);
        assert!(!dependencies.requires_unchanged_database_epoch());
        assert_eq!(dependencies.relational_table_domain_count(), 1);
        db.query("MATCH (n:Other) SET n.value = 3").unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "sql_fallback",
            dependencies.source_epoch(),
            None,
        );
        db.publish_prepared_projection(&dependencies, &staged)
            .unwrap();
    }
}

#[test]
fn necessary_relational_primary_key_bound_survives_a_local_or_residual() {
    let (_root, mut db) = relational_fixture();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    let output = preparation.query_sql_with_params(
        "SELECT body FROM records WHERE tenant = $1 AND id = $2 AND (body = 'one' OR body = 'alternate') ORDER BY body LIMIT 1",
        &[Value::String("w".into()), Value::Int(1)],
    ).unwrap();
    assert_eq!(output.rows.len(), 1);
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_relational_row_count(), 1);
    db.query_sql("UPDATE records SET body = 'changed' WHERE tenant = 'other' AND id = 1")
        .unwrap();
    let staged = seal(
        &db.projection_generation_store().unwrap(),
        "sql_residual",
        dependencies.source_epoch(),
        None,
    );
    db.publish_prepared_projection(&dependencies, &staged)
        .unwrap();
}

#[test]
fn relational_uuid_dependencies_use_the_same_coercion_as_query_and_write_keys() {
    let (_root, mut db) = relational_fixture();
    db.query_sql("CREATE TABLE uuid_keys (id UUID PRIMARY KEY, body TEXT)")
        .unwrap();
    let id = "018f4e6a-7c1b-7cc8-8f4d-1234567890ab";
    db.query_sql_with_params(
        "INSERT INTO uuid_keys (id, body) VALUES ($1, 'one')",
        &[Value::String(id.into())],
    )
    .unwrap();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    assert_eq!(
        preparation
            .query_sql_with_params(
                "SELECT body FROM uuid_keys WHERE id = $1",
                &[Value::String(id.into())]
            )
            .unwrap()
            .rows
            .len(),
        1
    );
    let dependencies = preparation.finish().unwrap();
    assert_eq!(dependencies.exact_relational_row_count(), 1);
    db.query_sql_with_params(
        "UPDATE uuid_keys SET body = 'changed' WHERE id = $1",
        &[Value::Uuid(hawdb::Uuid::parse_str(id).unwrap())],
    )
    .unwrap();
    let staged = seal(
        &db.projection_generation_store().unwrap(),
        "sql_uuid",
        dependencies.source_epoch(),
        None,
    );
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn unsupported_or_failed_sql_cannot_escape_as_a_source_proof() {
    for sql in [
        "SELECT lower(body) FROM records WHERE tenant = 'w' AND id = 1",
        "SELECT * FROM records r JOIN records s ON r.id = s.id",
        "SELECT * FROM pg_tables",
        "SELECT body FROM records FOR UPDATE",
        "UPDATE records SET body = 'not applied' WHERE tenant = 'w' AND id = 1",
    ] {
        let (_root, db) = relational_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        assert!(
            preparation.query_sql_with_params(sql, &[]).is_err(),
            "{sql}"
        );
        assert!(preparation.finish().is_err(), "{sql}");
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .query_sql("SELECT body FROM records WHERE tenant = 'w' AND id = 1")
                .unwrap()
                .rows[0]["body"],
            Value::String("one".into())
        );
    }
}

#[test]
fn relational_reads_share_the_dependency_budget_and_enforce_input_output_limits() {
    let (_root, db) = relational_fixture();
    let mut budget = limits();
    budget.dependency_entries = NonZeroUsize::new(1).unwrap();
    let mut preparation = db.begin_prepared_read(budget).unwrap();
    sql_point(&mut preparation, 1);
    sql_point(&mut preparation, 1); // identical key deduplicates
    assert!(preparation
        .query_sql_with_params(
            "SELECT body FROM records WHERE tenant = 'w' AND id = 2",
            &[]
        )
        .is_err());
    assert!(preparation.finish().is_err());
    let mut preparation = db.begin_prepared_read(budget).unwrap();
    sql_point(&mut preparation, 1);
    assert!(preparation
        .query_with_params("MATCH (n:Memory) RETURN n.id AS id", &BTreeMap::new())
        .is_err());
    assert!(preparation.finish().is_err());
    for (sql, parameters) in [
        (
            "SELECT body FROM records WHERE tenant = $1 AND id = 1".to_string(),
            vec![Value::String("x".repeat(64 * 1024))],
        ),
        ("SELECT body FROM records".to_string(), vec![]),
    ] {
        let mut budget = limits();
        budget.statement_rows = NonZeroUsize::new(1).unwrap();
        let mut preparation = db.begin_prepared_read(budget).unwrap();
        assert!(preparation
            .query_sql_with_params(&sql, &parameters)
            .is_err());
        assert!(preparation.finish().is_err());
    }
}

#[test]
fn large_relational_primary_keys_use_an_observable_bounded_fallback() {
    let (_root, mut db) = relational_fixture();
    db.query_sql("CREATE TABLE large_keys (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    let key = Value::String("k".repeat(5000));
    db.query_sql_with_params(
        "INSERT INTO large_keys (id, body) VALUES ($1, 'one')",
        std::slice::from_ref(&key),
    )
    .unwrap();
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    assert_eq!(
        preparation
            .query_sql_with_params("SELECT body FROM large_keys WHERE id = $1", &[key])
            .unwrap()
            .rows
            .len(),
        1
    );
    let dependencies = preparation.finish().unwrap();
    assert!(!dependencies.requires_unchanged_database_epoch());
    assert_eq!(dependencies.relational_table_domain_count(), 1);
    assert_eq!(dependencies.exact_relational_row_count(), 0);
}

#[test]
fn prepared_relational_payload_and_statement_work_limits_fail_sticky() {
    let (_root, mut db) = relational_fixture();
    db.query_sql_with_params(
        "UPDATE records SET body = $1 WHERE tenant = 'w' AND id = 1",
        &[Value::String("x".repeat(8192))],
    )
    .unwrap();
    for query in [
        "SELECT body FROM records WHERE tenant = 'w' AND id = 1".to_string(),
        format!(
            "SELECT body FROM records WHERE tenant = 'w' AND id = 1 AND ({})",
            (0..600)
                .map(|_| "body = 'x'")
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
    ] {
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        assert!(preparation.query_sql_with_params(&query, &[]).is_err());
        assert!(preparation.finish().is_err());
    }
}

#[test]
fn relational_lexical_and_expression_admission_precede_recursive_ast_work() {
    let (_root, db) = relational_fixture();
    let deep = format!(
        "SELECT body FROM records WHERE {}",
        (0..600).map(|_| "id = 1").collect::<Vec<_>>().join(" OR ")
    );
    let escaped = format!(
        r"SELECT body FROM records WHERE body = E'escaped \' OR inside' OR {}",
        (0..600).map(|_| "id = 1").collect::<Vec<_>>().join(" OR ")
    );
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    assert!(preparation
        .query_sql_with_params(&escaped, &[])
        .unwrap_err()
        .to_string()
        .contains("recursive SQL syntax"));
    assert!(preparation.finish().is_err());
    let flat = format!(
        "SELECT body FROM records WHERE id IN ({})",
        (0..1500).map(|_| "1").collect::<Vec<_>>().join(",")
    );
    for (sql, message) in [
        (deep, "recursive SQL syntax"),
        (flat, "expression exceeding metadata work"),
    ] {
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        let error = preparation.query_sql_with_params(&sql, &[]).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert!(preparation.finish().is_err());
    }
    let sql = format!(
        "SELECT body FROM records WHERE tenant = 'w' AND id = 1 /* {} */ AND body != '{}'",
        "OR ".repeat(600),
        "OR ".repeat(600)
    );
    let mut preparation = db.begin_prepared_read(limits()).unwrap();
    assert_eq!(
        preparation
            .query_sql_with_params(&sql, &[])
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        preparation.finish().unwrap().exact_relational_row_count(),
        1
    );
}

#[test]
fn checkpointed_relational_points_preserve_disjoint_validation_with_unselected_large_payloads() {
    for related in [false, true] {
        let (root, mut db) = relational_fixture();
        db.query_sql("ALTER TABLE records ADD COLUMN payload TEXT")
            .unwrap();
        db.query_sql_with_params(
            "UPDATE records SET payload = $1 WHERE tenant = 'w' AND id = 1",
            &[Value::String("x".repeat(1024 * 1024))],
        )
        .unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(
            &root.0,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(db.storage_residency_report().unwrap().out_of_core);
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        assert_eq!(
            sql_point(&mut preparation, 1).rows[0]["body"],
            Value::String("one".into())
        );
        let dependencies = preparation.finish().unwrap();
        assert_eq!(dependencies.exact_relational_row_count(), 1);
        db.query_sql(if related {
            "UPDATE records SET body = 'changed' WHERE tenant = 'w' AND id = 1"
        } else {
            "UPDATE records SET body = 'changed' WHERE tenant = 'w' AND id = 2"
        })
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "sql_checkpointed",
            dependencies.source_epoch(),
            None,
        );
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

fn adjacency_fixture() -> (TestRoot, Database, i64) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query("CREATE (:Memory {id: 'other', value: 2})")
        .unwrap();
    db.query("CREATE (:Label {id: 'tag', name: 'x-tag'})")
        .unwrap();
    db.query(
        "MATCH (n:Memory), (l:Label) WHERE n.id = 'a' CREATE (n)-[:HAS_LABEL {weight: 1}]->(l)",
    )
    .unwrap();
    let value = db
        .query("MATCH (n:Memory) WHERE n.id = 'a' RETURN id(n) AS id")
        .unwrap();
    let Value::Int(id) = value.rows[0]["id"] else {
        panic!("expected id")
    };
    (root, db, id)
}

#[test]
fn prepared_internal_id_membership_tracks_absent_roots() {
    let (_root, mut db, id) = adjacency_fixture();
    let mut read = db.begin_prepared_read(limits()).unwrap();
    let result = read
        .query_with_params(
            "MATCH (n) WHERE id(n) IN $ids RETURN id(n) AS id",
            &BTreeMap::from([(
                "ids".into(),
                Value::List(vec![Value::Int(id), Value::Int(4)]),
            )]),
        )
        .unwrap();
    assert_eq!(result.output.rows.len(), 1);
    let dependencies = read.finish().unwrap();
    assert!(!dependencies.requires_unchanged_database_epoch());
    db.query("CREATE (:Memory {id: 'new'})").unwrap();
    let inserted = db
        .query("MATCH (n:Memory) WHERE n.id = 'new' RETURN id(n) AS id")
        .unwrap();
    assert_eq!(inserted.rows[0]["id"], Value::Int(4));
    let staged = seal(
        &db.projection_generation_store().unwrap(),
        "absent_node_id",
        dependencies.source_epoch(),
        None,
    );
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn prepared_internal_node_ids_allow_disjoint_writes_and_detect_payload_changes() {
    for related in [false, true] {
        let (_root, mut db, id) = adjacency_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        let result = preparation
            .query_with_params(
                "MATCH (n) WHERE id(n) = $id RETURN n.value AS value",
                &BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
        assert_eq!(result.output.rows[0]["value"], Value::Int(1));
        let dependencies = preparation.finish().unwrap();
        assert!(!dependencies.requires_unchanged_database_epoch());
        db.query(if related {
            "MATCH (n:Memory) WHERE n.id = 'a' SET n.value = 3"
        } else {
            "MATCH (n:Memory) WHERE n.id = 'other' SET n.value = 3"
        })
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "node_id",
            dependencies.source_epoch(),
            None,
        );
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
fn prepared_single_hop_reads_validate_neighbor_payloads_and_allow_unrelated_writes() {
    for related in [false, true] {
        let (_root, mut db, id) = adjacency_fixture();
        let mut preparation = db.begin_prepared_read(limits()).unwrap();
        let result = preparation
            .query_with_params(
                "MATCH (n)-[r:HAS_LABEL]-(l:Label) WHERE id(n) = $id RETURN l.name AS name",
                &BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
        assert_eq!(result.output.rows[0]["name"], Value::String("x-tag".into()));
        let dependencies = preparation.finish().unwrap();
        db.query(if related {
            "MATCH (n:Label) SET n.name = 'changed'"
        } else {
            "MATCH (n:Other) SET n.value = 3"
        })
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "one_hop",
            dependencies.source_epoch(),
            None,
        );
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

fn prepared_hop(db: &Database, query: &str, id: i64) -> (hawdb::PreparedReadDependencies, usize) {
    let mut read = db.begin_prepared_read(limits()).unwrap();
    let output = read
        .query_with_params(query, &BTreeMap::from([("id".into(), Value::Int(id))]))
        .unwrap();
    (read.finish().unwrap(), output.output.rows.len())
}

#[test]
fn empty_adjacency_reads_detect_new_edges_and_preserve_disjoint_roots() {
    for related in [false, true] {
        let (_root, mut db, _id) = adjacency_fixture();
        let other = db
            .query("MATCH (n:Memory) WHERE n.id = 'other' RETURN id(n) AS id")
            .unwrap()
            .rows[0]["id"]
            .clone();
        let Value::Int(other) = other else {
            panic!("expected id")
        };
        let (dependencies, rows) = prepared_hop(
            &db,
            "MATCH (n)-[:HAS_LABEL]->(l) WHERE id(n) = $id RETURN id(l) AS id",
            other,
        );
        assert_eq!(rows, 0);
        db.query_with_params(
            "MATCH (n:Memory), (l:Label) WHERE n.id = $id CREATE (n)-[:HAS_LABEL]->(l)",
            &BTreeMap::from([(
                "id".into(),
                Value::String(if related { "other" } else { "a" }.into()),
            )]),
        )
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "empty_hop",
            dependencies.source_epoch(),
            None,
        );
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
fn filtered_neighbors_and_relationships_remain_dependencies_before_they_match() {
    for edge in [false, true] {
        let (_root, mut db, id) = adjacency_fixture();
        let (dependencies, rows) = prepared_hop(
            &db,
            if edge {
                "MATCH (n)-[r:HAS_LABEL]->(l) WHERE id(n) = $id AND r.weight = 2 RETURN l.name AS name"
            } else {
                "MATCH (n)-[:HAS_LABEL]->(l) WHERE id(n) = $id AND l.name = 'changed' RETURN l.name AS name"
            },
            id,
        );
        assert_eq!(rows, 0);
        db.query(if edge {
            "MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) SET r.weight = 2"
        } else {
            "MATCH (n:Label) SET n.name = 'changed'"
        })
        .unwrap();
        if edge {
            assert_eq!(
                db.query("MATCH (n)-[r:HAS_LABEL]->(l) RETURN r.weight AS weight")
                    .unwrap()
                    .rows[0]["weight"],
                Value::Int(2)
            );
        }
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "filtered_hop",
            dependencies.source_epoch(),
            None,
        );
        let result = db.publish_prepared_projection(&dependencies, &staged);
        assert!(
            matches!(result, Err(ProjectionPublicationError::Dependency(_))),
            "edge={edge} result={result:?}"
        );
    }
}

#[test]
fn adjacency_dependencies_keep_incoming_and_outgoing_roots_distinct() {
    for incoming in [false, true] {
        let (_root, mut db, id) = adjacency_fixture();
        let (dependencies, rows) = prepared_hop(
            &db,
            if incoming {
                "MATCH (n)<-[:HAS_LABEL]-(l) WHERE id(n) = $id RETURN id(l) AS id"
            } else {
                "MATCH (n)-[:HAS_LABEL]->(l) WHERE id(n) = $id RETURN id(l) AS id"
            },
            id,
        );
        assert_eq!(rows, usize::from(!incoming));
        db.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'a' CREATE (l)-[:HAS_LABEL]->(n)")
            .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "directed_hop",
            dependencies.source_epoch(),
            None,
        );
        let result = db.publish_prepared_projection(&dependencies, &staged);
        if incoming {
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
fn checkpointed_adjacency_reads_compose_live_and_canonical_identities() {
    for related in [false, true] {
        let (root, mut db, id) = adjacency_fixture();
        db.query_with_params(
            "MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) SET r.payload = $payload",
            &BTreeMap::from([("payload".into(), Value::String("x".repeat(1024 * 1024)))]),
        )
        .unwrap();
        let body = db
            .query("MATCH (n)-[r:HAS_LABEL]->(l) RETURN r.payload AS payload")
            .unwrap();
        let Value::String(payload) = &body.rows[0]["payload"] else {
            panic!("expected large payload")
        };
        assert_eq!(payload.len(), 1024 * 1024);
        drop(body);
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(
            &root.0,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        db.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'a' CREATE (n)-[:HAS_LABEL]->(l)")
            .unwrap();
        let (dependencies, rows) = prepared_hop(
            &db,
            "MATCH (n)-[:HAS_LABEL]-(l:Label) WHERE id(n) = $id RETURN l.name AS name",
            id,
        );
        assert_eq!(rows, 2);
        db.query(if related {
            "MATCH (n:Label) SET n.name = 'changed'"
        } else {
            "MATCH (n:Other) SET n.value = 9"
        })
        .unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "cold_hop",
            dependencies.source_epoch(),
            None,
        );
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
fn adjacency_dependency_refusals_are_sticky_and_do_not_poison_source() {
    let (_root, mut db, id) = adjacency_fixture();
    let mut budget = limits();
    budget.dependency_entries = NonZeroUsize::new(2).unwrap();
    let mut read = db.begin_prepared_read(budget).unwrap();
    assert!(read
        .query_with_params(
            "MATCH (n)-[:HAS_LABEL]->(l) WHERE id(n) = $id RETURN id(l) AS id",
            &BTreeMap::from([("id".into(), Value::Int(id))])
        )
        .is_err());
    assert!(read.finish().is_err());
    let result = db
        .query("MATCH (n:Memory) WHERE n.id = 'a' RETURN n.value AS value")
        .unwrap();
    assert_eq!(result.rows[0]["value"], Value::Int(1));
}

#[test]
fn prepared_entity_functions_preserve_user_properties_in_hot_and_cold_reads() {
    for cold in [false, true] {
        for related in [false, true] {
            let (root, mut db, id) = adjacency_fixture();
            db.query("MATCH (n:Memory) WHERE n.id = 'a' SET n._id = 'user-id', n.labels = ['user-label']").unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(
                    &root.0,
                    hawdb::DatabaseConfig {
                        storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let output = read.query_with_params(
                "MATCH (n) WHERE id(n) = $id RETURN properties(n) AS properties, labels(n) AS labels, id(n) AS id",
                &BTreeMap::from([("id".into(), Value::Int(id))]),
            ).unwrap();
            let row = &output.output.rows[0];
            let Value::Map(properties) = &row["properties"] else {
                panic!("expected raw properties")
            };
            assert_eq!(properties["_id"], Value::String("user-id".into()));
            assert_eq!(
                properties["labels"],
                Value::List(vec![Value::String("user-label".into())])
            );
            assert_eq!(
                row["labels"],
                Value::List(vec![Value::String("Memory".into())])
            );
            assert_eq!(row["id"], Value::Int(id));
            let dependencies = read.finish().unwrap();
            assert!(!dependencies.requires_unchanged_database_epoch());
            db.query(if related {
                "MATCH (n:Memory) WHERE n.id = 'a' SET n._id = 'changed'"
            } else {
                "MATCH (n:Other) SET n.value = 7"
            })
            .unwrap();
            let staged = seal(
                &db.projection_generation_store().unwrap(),
                "raw_properties",
                dependencies.source_epoch(),
                None,
            );
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
}

#[test]
fn entity_functions_restore_native_bindings_and_handle_optional_nulls() {
    let (_root, mut db, _) = adjacency_fixture();
    let output = db.query("MATCH (n:Memory) WHERE n.id = 'a' WITH n AS entity RETURN properties(entity) AS properties, labels(entity) AS labels").unwrap();
    let Value::Map(properties) = &output.rows[0]["properties"] else {
        panic!("expected properties")
    };
    assert_eq!(properties["id"], Value::String("a".into()));
    assert_eq!(
        output.rows[0]["labels"],
        Value::List(vec![Value::String("Memory".into())])
    );
    let output = db.query("MATCH (n:Other) OPTIONAL MATCH (n)-[r:HAS_LABEL]->(l:Label) RETURN properties(r) AS properties, labels(l) AS labels").unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["properties"], Value::Null);
    assert_eq!(output.rows[0]["labels"], Value::Null);
    db.query(
        "MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) SET r._id = 'user-edge', r.type = 'user-type'",
    )
    .unwrap();
    let output = db.query("MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) RETURN properties(r) AS properties, type(r) AS kind").unwrap();
    let Value::Map(properties) = &output.rows[0]["properties"] else {
        panic!("expected raw relationship properties")
    };
    assert_eq!(properties["_id"], Value::String("user-edge".into()));
    assert_eq!(properties["type"], Value::String("user-type".into()));
    assert_eq!(output.rows[0]["kind"], Value::String("HAS_LABEL".into()));
    assert!(db
        .query("MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) RETURN labels(r) AS labels")
        .is_err());
}

#[test]
fn prepared_search_graph_hydration_query_preserves_unlabelled_roots() {
    let (_root, mut db, id) = adjacency_fixture();
    let query = "MATCH (n) WHERE id(n) IN $ids OPTIONAL MATCH (n)-[:HAS_LABEL]-(l:Label) RETURN id(n) AS node_id, properties(n) AS properties, labels(n) AS labels, COLLECT(DISTINCT l) AS business_labels";
    let parameters = BTreeMap::from([(
        "ids".into(),
        Value::List(vec![Value::Int(id), Value::Int(2)]),
    )]);
    let mut read = db.begin_prepared_read(limits()).unwrap();
    let output = read.query_with_params(query, &parameters).unwrap();
    assert_eq!(output.output.rows.len(), 2);
    for row in output.output.rows.iter() {
        let Value::List(labels) = &row["business_labels"] else {
            panic!("expected business label collection")
        };
        assert_eq!(labels.len(), usize::from(row["node_id"] == Value::Int(id)));
    }
    let dependencies = read.finish().unwrap();
    assert!(!dependencies.requires_unchanged_database_epoch());
    db.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'other' CREATE (n)-[:HAS_LABEL]->(l)")
        .unwrap();
    let verification = db
        .query("MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) WHERE n.id = 'other' RETURN id(r) AS id")
        .unwrap();
    assert_eq!(verification.rows.len(), 1);
    let staged = seal(
        &db.projection_generation_store().unwrap(),
        "optional_graph",
        dependencies.source_epoch(),
        None,
    );
    assert!(matches!(
        db.publish_prepared_projection(&dependencies, &staged),
        Err(ProjectionPublicationError::Dependency(_))
    ));
}

#[test]
fn prepared_graph_hydration_matches_search_semantics_and_source_validates_activation() {
    for cold in [false, true] {
        for related in [false, true] {
            let (root, mut db, id) = adjacency_fixture();
            db.query("MATCH (n:Memory) WHERE n.id = 'a' SET n._id = 'user-id', n.labels = ['user-label']").unwrap();
            db.query("CREATE (:Label {id: 'second', canonical_name: '', name: 'ABC'})")
                .unwrap();
            db.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'a' CREATE (l)-[:HAS_LABEL]->(n)")
                .unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(
                    &root.0,
                    hawdb::DatabaseConfig {
                        storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            let mut expected = db
                .build_search_projection_graph_delta(&hawdb::SearchProjectionGraphDeltaRequest {
                    upsert_node_ids: vec![id as u64, 2, 999],
                    ..Default::default()
                })
                .unwrap()
                .upserts;
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let mut rows = read
                .hydrate_search_projection_nodes(
                    &[id as u64, 2, 999, id as u64],
                    NonZeroUsize::new(65536).unwrap(),
                )
                .unwrap();
            rows.sort_by(|left, right| left.external_id.cmp(&right.external_id));
            expected.sort_by(|left, right| left.external_id.cmp(&right.external_id));
            assert_eq!(rows, expected);
            assert_eq!(rows.len(), 2);
            let dependencies = read.finish().unwrap();
            assert!(!dependencies.requires_unchanged_database_epoch());
            let search = root.0.join("hydrated_search");
            let mut writer = hawdb::SearchOutOfCoreGenerationWriter::create(
                &search,
                hawdb::SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(dependencies.source_epoch()),
                    ..Default::default()
                },
            )
            .unwrap();
            for row in rows {
                writer.push(row.into_document()).unwrap();
            }
            let candidate = writer.prepare_publication().unwrap();
            db.query(if related {
                "MATCH (n:Label) SET n.name = 'changed'"
            } else {
                "MATCH (n:Other) SET n.value = 9"
            })
            .unwrap();
            let result = db.publish_prepared_search(&dependencies, candidate);
            if related {
                assert!(matches!(
                    result,
                    Err(ProjectionPublicationError::Dependency(_))
                ));
                assert!(!search
                    .join("search_projection.out_of_core.manifest.hawdb")
                    .exists());
            } else {
                let report = result.unwrap();
                assert_eq!(
                    report.source_graph_commit_epoch,
                    Some(dependencies.source_epoch())
                );
                let reader = hawdb::SearchOutOfCoreReader::open(&search).unwrap();
                assert_eq!(reader.document_count(), 2);
                let documents = reader
                    .hydrate_documents(&["memory:a".into()])
                    .unwrap()
                    .documents;
                assert_eq!(documents[0].metadata["_id"], "user-id");
                assert_eq!(documents[0].metadata["labels"], "[\"ABC\",\"x-tag\"]");
            }
        }
    }
}

#[test]
fn prepared_graph_hydration_budgets_refuse_complete_output_and_remain_sticky() {
    for mode in ["root_limit", "id", "row", "statement_bytes", "output_bytes"] {
        let (_root, db, id) = adjacency_fixture();
        let mut budget = limits();
        if mode == "row" {
            budget.statement_rows = NonZeroUsize::new(1).unwrap();
        }
        if mode == "statement_bytes" {
            budget.statement_payload_bytes = NonZeroUsize::new(1).unwrap();
        }
        let mut read = db.begin_prepared_read(budget).unwrap();
        let ids = match mode {
            "root_limit" => vec![id as u64; 1025],
            "id" => vec![u64::MAX],
            _ => vec![id as u64, 2],
        };
        assert!(
            read.hydrate_search_projection_nodes(
                &ids,
                NonZeroUsize::new(if mode == "output_bytes" { 1 } else { 65536 }).unwrap()
            )
            .is_err(),
            "{mode}"
        );
        assert!(read
            .hydrate_search_projection_nodes(&[], NonZeroUsize::new(1).unwrap())
            .unwrap()
            .is_empty());
        assert!(read.finish().is_err(), "{mode}");
    }
}

#[test]
fn prepared_graph_hydration_tracks_first_label_edge_without_a_schema_intent() {
    for related in [false, true] {
        let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        let mut read = db.begin_prepared_read(limits()).unwrap();
        let rows = read
            .hydrate_search_projection_nodes(&[0, 1, 999], NonZeroUsize::new(65536).unwrap())
            .unwrap();
        assert_eq!(rows.len(), 1);
        let dependencies = read.finish().unwrap();
        assert!(!dependencies.requires_unchanged_database_epoch());
        db.query("CREATE (:Label {id: 'new-tag'})").unwrap();
        if related {
            db.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'a' CREATE (n)-[:HAS_LABEL]->(l)")
                .unwrap();
        }
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "catalog_absence",
            dependencies.source_epoch(),
            None,
        );
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
fn prepared_optional_hydration_refuses_unbounded_or_selected_roots() {
    let (_root, db, id) = adjacency_fixture();
    let parameters = BTreeMap::from([("id".into(), Value::Int(id))]);
    for query in [
        "MATCH (n) OPTIONAL MATCH (n)-[:HAS_LABEL]-(l:Label) RETURN id(n) AS id, COLLECT(l) AS labels",
        "MATCH (n) WITH n LIMIT 1 OPTIONAL MATCH (n)-[:HAS_LABEL]-(l:Label) WHERE id(n) = $id RETURN id(n) AS id, COLLECT(l) AS labels",
        "MATCH (n) WHERE id(n) = $id OR n.id = 'other' OPTIONAL MATCH (n)-[:HAS_LABEL]-(l:Label) RETURN id(n) AS id, COLLECT(l) AS labels",
    ] {
        db.explain_query_with_params(query, &parameters).unwrap();
        let mut read = db.begin_prepared_read(limits()).unwrap();
        assert!(read.query_with_params(query, &parameters).is_err(), "{query}");
        assert!(read.finish().is_err());
    }
}

#[test]
fn cold_changefeed_replays_complete_search_changes_against_full_rebuild() {
    fn copy_closed_fixture(source: &std::path::Path, target: &std::path::Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let destination = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_closed_fixture(&entry.path(), &destination);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                std::fs::copy(entry.path(), destination).unwrap();
            }
        }
    }
    // Each case has its own canonical files and WAL. Initialize and checkpoint
    // the identical baseline once, then copy it only after all handles close.
    let (baseline, mut db, _) = adjacency_fixture();
    db.checkpoint().unwrap();
    drop(db);
    for cold in [false, true] {
        for replay in [false, true] {
            for query in [
                "MATCH (n:Memory) WHERE n.id = 'a' SET n.title = 'changed-title'",
                "MATCH (n:Memory) WHERE n.id = 'a' SET n.id = 'renamed'",
                "MATCH (n:Memory) WHERE n.id = 'a' DETACH DELETE n",
                "MATCH (n:Label) SET n.name = 'changed-label'",
                "MATCH (n:Label) SET n.canonical_name = 'canonical-label'",
                "MATCH (n:Label) DETACH DELETE n",
                "MATCH (n:Memory), (l:Label) WHERE n.id = 'other' CREATE (n)-[:HAS_LABEL]->(l)",
                "MATCH (n:Memory)-[r:HAS_LABEL]->(l:Label) DELETE r",
            ] {
                let root = TestRoot::new();
                copy_closed_fixture(&baseline.0, &root.0);
                let config = hawdb::DatabaseConfig {
                    storage_residency_mode: if cold {
                        hawdb::StorageResidencyMode::OutOfCore
                    } else {
                        hawdb::StorageResidencyMode::Materialized
                    },
                    ..Default::default()
                };
                let mut db = Database::open_with_config(&root.0, config.clone()).unwrap();
                let mut incremental = hawdb::SearchIndex::default();
                db.rebuild_search_projection(&mut incremental, Default::default())
                    .unwrap();
                let cursor = db.commit_epoch().unwrap();
                db.query(query).unwrap();
                let committed = db.commit_epoch().unwrap();
                if replay {
                    drop(db);
                    db = Database::open_with_config(&root.0, config).unwrap();
                }
                let batch = db
                    .build_search_projection_change_batch_after(cursor, Some(100))
                    .unwrap()
                    .unwrap();
                assert_eq!(batch.complete_through_commit_epoch(), Some(committed));
                assert!(
                    batch.operation_count() > 0,
                    "cold={cold} replay={replay} {query}"
                );
                db.apply_search_projection_change_batch(
                    &mut incremental,
                    batch,
                    Default::default(),
                )
                .unwrap();
                let mut oracle = hawdb::SearchIndex::default();
                db.rebuild_search_projection(&mut oracle, Default::default())
                    .unwrap();
                assert_eq!(
                    incremental.document_count(),
                    oracle.document_count(),
                    "{query}"
                );
                for id in ["memory:a", "memory:renamed", "memory:other"] {
                    assert_eq!(
                        incremental.document(id),
                        oracle.document(id),
                        "cold={cold} replay={replay} {query} {id}"
                    );
                }
            }
        }
    }
}

#[test]
fn changefeed_captures_new_label_edges_and_published_ids_inside_one_transaction() {
    let (_root, mut db, _) = adjacency_fixture();
    let mut incremental = hawdb::SearchIndex::default();
    db.rebuild_search_projection(&mut incremental, Default::default())
        .unwrap();
    let cursor = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query("CREATE (:Label {id: 'new', name: 'new-label'})")
        .unwrap();
    transaction.query("MATCH (n:Memory), (l:Label) WHERE n.id = 'other' AND l.id = 'new' CREATE (n)-[:HAS_LABEL]->(l)").unwrap();
    transaction
        .query("MATCH (n:Memory) WHERE n.id = 'a' SET n.id = 'temporary'")
        .unwrap();
    transaction
        .query("MATCH (n:Memory) WHERE n.id = 'temporary' SET n.id = 'renamed'")
        .unwrap();
    transaction.commit().unwrap();
    let batch = db
        .build_search_projection_change_batch_after(cursor, Some(100))
        .unwrap()
        .unwrap();
    assert_eq!(batch.graph_delta().delete_document_ids, ["memory:a"]);
    assert!(batch.graph_delta().upsert_node_ids.contains(&2));
    db.apply_search_projection_change_batch(&mut incremental, batch, Default::default())
        .unwrap();
    let mut oracle = hawdb::SearchIndex::default();
    db.rebuild_search_projection(&mut oracle, Default::default())
        .unwrap();
    for id in [
        "memory:a",
        "memory:temporary",
        "memory:renamed",
        "memory:other",
    ] {
        assert_eq!(incremental.document(id), oracle.document(id), "{id}");
    }
}

#[test]
fn oversized_projection_preimages_require_rebuild_without_losing_canonical_commits() {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query_with_params(
        "MATCH (n:Memory) WHERE n.id = 'a' SET n.id = $id",
        &BTreeMap::from([("id".into(), Value::String("a".repeat(1024 * 1024)))]),
    )
    .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let config = hawdb::DatabaseConfig {
        storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    db = Database::open_with_config(&root.0, config.clone()).unwrap();
    let cursor = db.commit_epoch().unwrap();
    db.query("MATCH (n:Memory) SET n.id = 'small'").unwrap();
    let epoch = db.commit_epoch().unwrap();
    assert!(epoch > cursor);
    for checkpoint in [false, true] {
        let error = db
            .build_search_projection_change_batch_after(cursor, Some(100))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("full search projection rebuild required"));
        let output = db.query("MATCH (n:Memory) RETURN n.id AS id").unwrap();
        assert_eq!(output.rows[0]["id"], Value::String("small".into()));
        if checkpoint {
            db.checkpoint().unwrap();
        }
        drop(db);
        db = Database::open_with_config(&root.0, config.clone()).unwrap();
        assert_eq!(db.commit_epoch().unwrap(), epoch);
    }
    assert!(db
        .build_search_projection_change_batch_after(cursor, Some(100))
        .is_err());
    assert!(db
        .build_search_projection_change_batch_after(epoch, Some(100))
        .unwrap()
        .is_none());
}

#[test]
fn prepared_hydration_uses_internal_id_roots_without_owning_unrelated_bodies() {
    for cold in [false, true] {
        for business_labels in [false, true] {
            let root = TestRoot::new();
            let mut db = Database::open(&root.0).unwrap();
            db.query_with_params(
                "CREATE (:Memory {id: 'huge', body: $body})",
                &BTreeMap::from([("body".into(), Value::String("x".repeat(1024 * 1024 + 137)))]),
            )
            .unwrap();
            db.query("CREATE (:Memory {id: 'small', title: 'selected'})")
                .unwrap();
            let id = db
                .query("MATCH (n:Memory {id: 'small'}) RETURN id(n) AS id")
                .unwrap()
                .rows[0]["id"]
                .clone();
            if business_labels {
                db.query("CREATE (:Label {name: 'tag'})").unwrap();
                db.query(
                    "MATCH (n:Memory), (l:Label) WHERE n.id = 'small' CREATE (n)-[:HAS_LABEL]->(l)",
                )
                .unwrap();
            }
            db.checkpoint().unwrap();
            drop(db);
            let mut config = hawdb::DatabaseConfig {
                storage_residency_mode: if cold {
                    hawdb::StorageResidencyMode::OutOfCore
                } else {
                    hawdb::StorageResidencyMode::Materialized
                },
                ..Default::default()
            };
            config.execution_memory.query_memory_bytes = NonZeroUsize::new(128 * 1024).unwrap();
            config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(32 * 1024).unwrap();
            config.execution_memory.batch_payload_bytes = NonZeroUsize::new(32 * 1024).unwrap();
            let db = Database::open_with_config(&root.0, config).unwrap();
            let Value::Int(id) = id else {
                panic!("missing node ID")
            };
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let rows = read
                .hydrate_search_projection_nodes(
                    &[id as u64, id as u64, 999],
                    NonZeroUsize::new(4096).unwrap(),
                )
                .unwrap_or_else(|e| panic!("cold={cold} labels={business_labels}: {e}"));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].external_id, "small");
            let dependencies = read.finish().unwrap();
            assert!(!dependencies.requires_unchanged_database_epoch());
        }
    }
}

fn relational_prefix_fixture(cold: bool) -> (TestRoot, Database) {
    let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
    db.query_sql("CREATE TABLE thread_messages (content_message_id TEXT PRIMARY KEY, thread_storage_id TEXT, order_index BIGINT, body TEXT)").unwrap();
    db.query_sql("CREATE INDEX message_group ON thread_messages (thread_storage_id, order_index, content_message_id)").unwrap();
    db.query_sql("INSERT INTO thread_messages (content_message_id, thread_storage_id, order_index, body) VALUES ('a','g',1,'one'), ('b','g',2,'two'), ('c','other',1,'other')").unwrap();
    if cold {
        db.checkpoint().unwrap();
        drop(db);
        db = Database::open_with_config(
            &root.0,
            hawdb::DatabaseConfig {
                storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(db.storage_residency_report().unwrap().out_of_core);
    }
    (root, db)
}
fn relational_prefix_page(read: &mut hawdb::PreparedRead, group: &str) -> hawdb::QueryOutput {
    read.query_sql_with_params("SELECT m.content_message_id, m.body FROM public.thread_messages AS m WHERE m.thread_storage_id = $1 AND (m.order_index > $2 OR (m.order_index = $2 AND m.content_message_id > $3)) ORDER BY m.order_index ASC, m.content_message_id ASC LIMIT $4",&[Value::String(group.into()),Value::Int(0),Value::String(String::new()),Value::Int(1)]).unwrap()
}
#[test]
fn relational_prefix_capture_precedes_query_and_allows_disjoint_large_payload_writes() {
    for cold in [false, true] {
        for related in [false, true] {
            let (_root, mut db) = relational_prefix_fixture(cold);
            let mut read = db.begin_prepared_read(limits()).unwrap();
            db.query_sql_with_params(
                "UPDATE thread_messages SET body = $1 WHERE content_message_id = $2",
                &[
                    Value::String("x".repeat(1024 * 1024)),
                    Value::String(if related { "a" } else { "c" }.into()),
                ],
            )
            .unwrap();
            let output = relational_prefix_page(&mut read, "g");
            assert_eq!(output.rows.len(), 1);
            assert_eq!(output.rows[0]["body"], Value::String("one".into()));
            let dependencies = read.finish().unwrap();
            assert_eq!(dependencies.relational_index_prefix_count(), 1);
            assert_eq!(dependencies.exact_relational_row_count(), 0);
            assert!(!dependencies.requires_unchanged_database_epoch());
            let staged = seal(
                &db.projection_generation_store().unwrap(),
                "prefix_capture",
                dependencies.source_epoch(),
                None,
            );
            let result = db.publish_prepared_projection(&dependencies, &staged);
            if related {
                assert!(
                    matches!(result, Err(ProjectionPublicationError::Dependency(_))),
                    "cold={cold}"
                );
            } else {
                result.unwrap();
            }
        }
    }
}
#[test]
fn relational_prefix_pages_cover_membership_deletes_aba_off_page_and_absence() {
    for cold in [false, true] {
        for change in [
            "delete", "move_out", "move_in", "aba", "off_page", "empty", "schema",
        ] {
            let (_root, mut db) = relational_prefix_fixture(cold);
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let output =
                relational_prefix_page(&mut read, if change == "empty" { "missing" } else { "g" });
            assert_eq!(output.rows.is_empty(), change == "empty");
            let dependencies = read.finish().unwrap();
            assert_eq!(dependencies.relational_index_prefix_count(), 1);
            match change {
                "delete" => {
                    db.query_sql("DELETE FROM thread_messages WHERE content_message_id = 'a'")
                        .unwrap();
                }
                "move_out" | "aba" => {
                    db.query_sql("UPDATE thread_messages SET thread_storage_id = 'other' WHERE content_message_id = 'a'").unwrap();
                    if change == "aba" {
                        db.query_sql("UPDATE thread_messages SET thread_storage_id = 'g' WHERE content_message_id = 'a'").unwrap();
                    }
                }
                "move_in" => {
                    db.query_sql("UPDATE thread_messages SET thread_storage_id = 'g' WHERE content_message_id = 'c'").unwrap();
                }
                "off_page" => {
                    db.query_sql("UPDATE thread_messages SET body = 'changed' WHERE content_message_id = 'b'").unwrap();
                }
                "empty" => {
                    db.query_sql("INSERT INTO thread_messages (content_message_id, thread_storage_id, order_index, body) VALUES ('new','missing',1,'new')")
                        .unwrap();
                }
                "schema" => {
                    db.query_sql("ALTER TABLE thread_messages ADD COLUMN extra TEXT")
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let staged = seal(
                &db.projection_generation_store().unwrap(),
                "prefix_changed",
                dependencies.source_epoch(),
                None,
            );
            assert!(
                matches!(
                    db.publish_prepared_projection(&dependencies, &staged),
                    Err(ProjectionPublicationError::Dependency(_))
                ),
                "cold={cold} change={change}"
            );
        }
    }
}
#[test]
fn relational_prefix_selection_uses_table_domains_for_nonnecessary_bounds() {
    for query in [
        "SELECT body FROM thread_messages WHERE thread_storage_id = 'g' OR body = 'other'",
        "SELECT body FROM thread_messages WHERE NOT (thread_storage_id = 'g')",
        "SELECT body FROM thread_messages WHERE order_index = 1",
        "SELECT body FROM thread_messages WHERE thread_storage_id = NULL",
        "SELECT body FROM thread_messages WHERE body = 'one'",
    ] {
        let (_root, mut db) = relational_prefix_fixture(false);
        let mut read = db.begin_prepared_read(limits()).unwrap();
        read.query_sql_with_params(query, &[]).unwrap();
        let dependencies = read.finish().unwrap();
        assert!(!dependencies.requires_unchanged_database_epoch(), "{query}");
        assert_eq!(dependencies.relational_table_domain_count(), 1);
        assert_eq!(dependencies.relational_index_prefix_count(), 0);
        db.query("CREATE (:Unrelated {id:'new'})").unwrap();
        let staged = seal(
            &db.projection_generation_store().unwrap(),
            "prefix_fallback",
            dependencies.source_epoch(),
            None,
        );
        db.publish_prepared_projection(&dependencies, &staged)
            .unwrap();
    }
}

#[test]
fn relational_prefix_can_bind_a_complete_composite_leading_tuple() {
    for cold in [false, true] {
        for related in [false, true] {
            let (_root, mut db) = relational_prefix_fixture(cold);
            let mut read = db.begin_prepared_read(limits()).unwrap();
            let output=read.query_sql_with_params("SELECT body FROM thread_messages WHERE thread_storage_id = $1 AND order_index = $2",&[Value::String("g".into()),Value::Int(1)]).unwrap();
            assert_eq!(output.rows[0]["body"], Value::String("one".into()));
            let dependencies = read.finish().unwrap();
            assert_eq!(dependencies.relational_index_prefix_count(), 1);
            assert!(!dependencies.requires_unchanged_database_epoch());
            db.query_sql_with_params(
                "UPDATE thread_messages SET body = 'changed' WHERE content_message_id = $1",
                &[Value::String(if related { "a" } else { "b" }.into())],
            )
            .unwrap();
            let staged = seal(
                &db.projection_generation_store().unwrap(),
                "prefix_composite",
                dependencies.source_epoch(),
                None,
            );
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
}
#[test]
fn relational_prefix_float_and_oversized_equalities_use_complete_table_domains() {
    for large in [false, true] {
        let (_root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
        db.query_sql(if large {
            "CREATE TABLE unqualified_scope (id BIGINT PRIMARY KEY, group_value TEXT)"
        } else {
            "CREATE TABLE unqualified_scope (id BIGINT PRIMARY KEY, group_value DOUBLE PRECISION)"
        })
        .unwrap();
        db.query_sql("CREATE INDEX unsupported_group ON unqualified_scope (group_value)")
            .unwrap();
        let value = if large {
            Value::String("x".repeat(5000))
        } else {
            Value::Float(1.0)
        };
        db.query_sql_with_params(
            "INSERT INTO unqualified_scope (id, group_value) VALUES (1,$1)",
            std::slice::from_ref(&value),
        )
        .unwrap();
        let mut read = db.begin_prepared_read(limits()).unwrap();
        assert_eq!(
            read.query_sql_with_params(
                "SELECT id FROM unqualified_scope WHERE group_value = $1",
                &[value]
            )
            .unwrap()
            .rows
            .len(),
            1
        );
        let dependencies = read.finish().unwrap();
        assert_eq!(dependencies.relational_index_prefix_count(), 0);
        assert!(!dependencies.requires_unchanged_database_epoch());
        assert_eq!(dependencies.relational_table_domain_count(), 1);
    }
}

#[test]
fn relational_prefix_nullable_unique_suffix_requires_complete_nonnull_binding() {
    for cold in [false, true] {
        for constraint in [false, true] {
            let (root, mut db) = fixture(DurabilityPolicy::SyncOnEveryWrite);
            db.query_sql(if constraint {
                "CREATE TABLE nullable_groups (id BIGINT PRIMARY KEY, group_value TEXT, sort_value BIGINT, UNIQUE (group_value, sort_value))"
            } else {
                "CREATE TABLE nullable_groups (id BIGINT PRIMARY KEY, group_value TEXT, sort_value BIGINT)"
            }).unwrap();
            if !constraint {
                db.query_sql("CREATE UNIQUE INDEX nullable_group_index ON nullable_groups (group_value, sort_value)").unwrap();
            }
            db.query_sql("INSERT INTO nullable_groups (id, group_value, sort_value) VALUES (1,'g',NULL), (2,'g',1)").unwrap();
            db.query_sql("CREATE TABLE group_owners (group_value TEXT PRIMARY KEY)")
                .unwrap();
            db.query_sql("INSERT INTO group_owners (group_value) VALUES ('g')")
                .unwrap();
            if cold {
                db.checkpoint().unwrap();
                drop(db);
                db = Database::open_with_config(
                    &root.0,
                    hawdb::DatabaseConfig {
                        storage_residency_mode: hawdb::StorageResidencyMode::OutOfCore,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            let joined = db.query_sql("SELECT r.id FROM group_owners AS o INNER JOIN nullable_groups AS r ON r.group_value = o.group_value WHERE o.group_value = 'g' ORDER BY r.id").unwrap();
            assert_eq!(joined.rows.len(), 2, "cold={cold} constraint={constraint}");
            for complete in [false, true] {
                let mut read = db.begin_prepared_read(limits()).unwrap();
                let query = if complete {
                    "SELECT id FROM nullable_groups WHERE group_value = 'g' AND sort_value = 1"
                } else {
                    "SELECT id FROM nullable_groups WHERE group_value = 'g'"
                };
                let output = read.query_sql_with_params(query, &[]).unwrap();
                assert_eq!(output.rows.len(), if complete { 1 } else { 2 });
                let dependencies = read.finish().unwrap();
                // Only declared indexes participate in prepared prefix reads.
                let precise = complete && !constraint;
                assert!(!dependencies.requires_unchanged_database_epoch());
                assert_eq!(
                    dependencies.relational_table_domain_count(),
                    usize::from(!precise)
                );
                assert_eq!(
                    dependencies.relational_index_prefix_count(),
                    usize::from(precise)
                );
            }
        }
    }
}
