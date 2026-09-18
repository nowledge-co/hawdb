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
use crate::search::{SearchProjectionFreshness, SearchProjectionKind};
use crate::{
    hawdb_lightning_initial_import_advance_checkpoint,
    hawdb_lightning_initial_import_advance_durable_state_streaming,
    hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch,
    hawdb_lightning_initial_import_checkpoint_readiness,
    hawdb_lightning_initial_import_cutover_catch_up_report,
    hawdb_lightning_initial_import_decode_durable_state,
    hawdb_lightning_initial_import_document_identity_coverage,
    hawdb_lightning_initial_import_durable_state_report,
    hawdb_lightning_initial_import_encode_durable_state,
    hawdb_lightning_initial_import_search_projection_batch_report,
    hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities,
    hawdb_lightning_initial_import_session_bundle_readiness,
    hawdb_lightning_initial_import_session_report,
    hawdb_lightning_initial_import_source_bundle_readiness,
    parse_hawdb_lightning_graph_stream_export, CanonicalGraphSnapshotExport,
    CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode, CanonicalSnapshotRelationship,
    HawDBLightningBootstrapManifest, HawDBLightningInitialImportCheckpoint,
    HawDBLightningInitialImportCheckpointProgress, HawDBLightningInitialImportDocumentIdentity,
    HawDBLightningInitialImportIdempotencyKey, HawDBLightningInitialImportReadinessInputs,
    HawDBLightningInitialImportResumeActionKind, HawDBLightningRelationalStream,
};
use crate::{SearchProjectionDelta, SearchProjectionRow};

fn test_hawdb_lightning_checkpoint(
    manifest: &HawDBLightningBootstrapManifest,
) -> HawDBLightningInitialImportCheckpoint {
    HawDBLightningInitialImportCheckpoint {
        protocol_version: 1,
        import_id: "import-1".to_string(),
        task_id: "task-1".to_string(),
        fencing_token: "fence-1".to_string(),
        object_digest: "object-digest-1".to_string(),
        schema_checksum: manifest.schema_checksum,
        graph_stream_checksum: manifest.graph_stream_checksum,
        graph_stream_byte_len: manifest.graph_stream_byte_len,
        relational_stream_checksum: manifest.relational_stream_checksum,
        relational_stream_byte_len: manifest.relational_stream_byte_len,
        manifest_database_commit_epoch: manifest.database_commit_epoch,
        manifest_graph_commit_epoch: manifest.graph_commit_epoch,
        applied_graph_commit_epoch: manifest.graph_commit_epoch,
        applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        completed_batches: 4,
        total_batches: 4,
        document_identity_count: manifest.node_count + manifest.relationship_count,
    }
}

fn initial_import_document_identity(
    kind: SearchProjectionKind,
    document_id: &str,
) -> HawDBLightningInitialImportDocumentIdentity {
    HawDBLightningInitialImportDocumentIdentity {
        kind,
        document_id: document_id.to_string(),
    }
}

fn all_initial_import_document_identities() -> Vec<HawDBLightningInitialImportDocumentIdentity> {
    vec![
        initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
        initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
        initial_import_document_identity(SearchProjectionKind::Entity, "entity:1"),
        initial_import_document_identity(SearchProjectionKind::Source, "source:1"),
        initial_import_document_identity(SearchProjectionKind::SourceChunk, "source_chunk:1"),
        initial_import_document_identity(SearchProjectionKind::Community, "community:1"),
    ]
}

fn initial_import_projection_row(
    kind: SearchProjectionKind,
    external_id: &str,
) -> SearchProjectionRow {
    SearchProjectionRow {
        kind,
        external_id: external_id.to_string(),
        title: format!("title {external_id}"),
        body: format!("body {external_id}"),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn all_initial_import_projection_rows() -> Vec<SearchProjectionRow> {
    vec![
        initial_import_projection_row(SearchProjectionKind::Memory, "1"),
        initial_import_projection_row(SearchProjectionKind::Message, "1"),
        initial_import_projection_row(SearchProjectionKind::Entity, "1"),
        initial_import_projection_row(SearchProjectionKind::Source, "1"),
        initial_import_projection_row(SearchProjectionKind::SourceChunk, "1"),
        initial_import_projection_row(SearchProjectionKind::Community, "1"),
    ]
}

#[test]
fn canonical_snapshot_from_rows_derives_checksum_and_identity_audit() {
    let snapshot = CanonicalGraphSnapshotExport::from_rows(
        7,
        vec![CanonicalSnapshotNode {
            node_id: 1,
            stable_id: Some(Value::String("legacy:node:1".to_string())),
            labels: vec!["Memory".to_string()],
            properties: BTreeMap::from([("id".to_string(), Value::String("m1".to_string()))]),
        }],
        vec![],
    );

    assert_eq!(snapshot.graph_commit_epoch, 7);
    assert_ne!(snapshot.logical_checksum, 0);
    assert!(!snapshot.stable_identity.requires_stable_id_mapping);
    assert!(snapshot.validate().is_import_ready);
}

fn initial_import_projection_freshness(
    manifest: &HawDBLightningBootstrapManifest,
) -> SearchProjectionFreshness {
    SearchProjectionFreshness {
        document_count: manifest.node_count + manifest.relationship_count,
        import_source_graph_commit_epoch: Some(manifest.graph_commit_epoch),
        source_graph_commit_epoch: Some(manifest.graph_commit_epoch),
        durable_source_graph_commit_epoch: Some(manifest.graph_commit_epoch),
        has_uncheckpointed_changes: false,
        full_reindex_needed: false,
        full_reindex_reasons: Vec::new(),
        metadata_repair_needed: false,
        metadata_repair_reasons: Vec::new(),
        embedding_model: None,
        embedding_version: None,
        embedding_dimension: None,
    }
}

#[test]
fn canonical_snapshot_export_uses_pinned_read_transaction_state() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let read_tx = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'later', title: 'Later'})")
        .unwrap();

    let snapshot = read_tx.export_canonical_graph_snapshot();
    let validation = snapshot.validate();
    assert_eq!(snapshot.graph_commit_epoch, 1);
    assert_eq!(snapshot.nodes.len(), 2);
    assert_eq!(snapshot.relationships.len(), 1);
    assert!(validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.stable_identity_ready);
    assert!(snapshot.stable_identity.requires_stable_id_mapping);
    assert!(snapshot.stable_identity.nodes_without_stable_id.is_empty());
    assert_eq!(
        snapshot.stable_identity.relationships_without_stable_id,
        vec![0]
    );
    assert!(snapshot
        .stable_identity
        .duplicate_node_stable_ids
        .is_empty());
    assert_eq!(
        snapshot.nodes[0].stable_id,
        Some(Value::String("root".to_string()))
    );
    assert_eq!(snapshot.relationships[0].rel_type, "LINKS");
    assert_eq!(snapshot.relationships[0].source_node_id, 0);
    assert_eq!(snapshot.relationships[0].target_node_id, 1);
    assert_eq!(
        snapshot.relationships[0].properties.get("weight"),
        Some(&Value::Int(7))
    );
    assert!(snapshot.nodes.iter().any(|node| {
        node.labels == vec!["Memory".to_string()]
            && node.properties.get("id") == Some(&Value::String("root".to_string()))
    }));
    assert!(!snapshot
        .nodes
        .iter()
        .any(|node| { node.properties.get("id") == Some(&Value::String("later".to_string())) }));

    let latest = db.export_canonical_graph_snapshot();
    assert_eq!(latest.graph_commit_epoch, 2);
    assert_eq!(latest.nodes.len(), 3);
    assert_ne!(snapshot.logical_checksum, latest.logical_checksum);
}

#[test]
fn canonical_snapshot_export_reports_duplicate_stable_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'dup', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'dup', title: 'Second'})")
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();

    assert!(snapshot.stable_identity.requires_stable_id_mapping);
    assert_eq!(
        snapshot.stable_identity.duplicate_node_stable_ids,
        vec![Value::String("dup".to_string())]
    );
    assert!(snapshot.stable_identity.nodes_without_stable_id.is_empty());
}

#[test]
fn canonical_snapshot_export_validation_accepts_consistent_snapshot() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    let validation = snapshot.validate();

    assert!(validation.is_valid);
    assert!(validation.is_import_ready);
    assert!(validation.checksum_matches);
    assert!(validation.stable_identity_matches);
    assert!(validation.stable_identity_ready);
    assert_eq!(
        validation.expected_logical_checksum,
        snapshot.logical_checksum
    );
    assert!(validation.duplicate_node_ids.is_empty());
    assert!(validation.duplicate_relationship_ids.is_empty());
    assert!(validation.missing_sources.is_empty());
    assert!(validation.missing_targets.is_empty());
    assert!(
        !validation
            .expected_stable_identity
            .requires_stable_id_mapping
    );
}

#[test]
fn canonical_snapshot_export_validation_reports_corrupt_snapshot() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid'}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let mut snapshot = db.export_canonical_graph_snapshot();
    snapshot.nodes[1].node_id = snapshot.nodes[0].node_id;
    snapshot.relationships[0].target_node_id = 99;
    snapshot.relationships[0].stable_id = None;

    let validation = snapshot.validate();

    assert!(!validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.checksum_matches);
    assert!(!validation.stable_identity_matches);
    assert!(!validation.stable_identity_ready);
    assert_eq!(validation.duplicate_node_ids, vec![0]);
    assert!(validation.duplicate_relationship_ids.is_empty());
    assert!(validation.missing_sources.is_empty());
    assert_eq!(validation.missing_targets.len(), 1);
    assert_eq!(validation.missing_targets[0].relationship_id, 0);
    assert_eq!(validation.missing_targets[0].missing_node_id, 99);
    assert_eq!(
        validation
            .expected_stable_identity
            .relationships_without_stable_id,
        vec![0]
    );
}

#[test]
fn canonical_snapshot_stable_id_mapping_makes_export_import_ready() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
        )
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    assert!(snapshot.validate().is_valid);
    assert!(!snapshot.validate().is_import_ready);
    assert_eq!(
        snapshot.stable_identity.relationships_without_stable_id,
        vec![0]
    );

    let mapped = snapshot.with_stable_id_mapping(&CanonicalStableIdMapping {
        relationship_stable_ids: BTreeMap::from([(0, Value::String("rel-root-mid".to_string()))]),
        ..CanonicalStableIdMapping::default()
    });
    let validation = mapped.validate();

    assert!(validation.is_valid);
    assert!(validation.is_import_ready);
    assert!(validation.stable_identity_ready);
    assert!(mapped
        .stable_identity
        .relationships_without_stable_id
        .is_empty());
    assert_eq!(
        mapped.relationships[0].stable_id,
        Some(Value::String("rel-root-mid".to_string()))
    );
    assert_ne!(mapped.logical_checksum, snapshot.logical_checksum);
}

#[test]
fn canonical_snapshot_stable_id_mapping_rejects_duplicate_overlay() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'root'}), (e:Entity {id: 'mid'}) CREATE (m)-[:MENTIONS]->(e)")
        .unwrap();

    let snapshot = db.export_canonical_graph_snapshot();
    let mapped = snapshot.with_stable_id_mapping(&CanonicalStableIdMapping {
        relationship_stable_ids: BTreeMap::from([
            (0, Value::String("duplicate-rel".to_string())),
            (1, Value::String("duplicate-rel".to_string())),
        ]),
        ..CanonicalStableIdMapping::default()
    });
    let validation = mapped.validate();

    assert!(validation.is_valid);
    assert!(!validation.is_import_ready);
    assert!(!validation.stable_identity_ready);
    assert_eq!(
        validation
            .expected_stable_identity
            .duplicate_relationship_stable_ids,
        vec![Value::String("duplicate-rel".to_string())]
    );
}

#[test]
fn persisted_stable_id_mapping_survives_reopen_without_wal_write() {
    let path = unique_test_dir("persisted_stable_id_mapping");
    let first_stable_id = {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        let wal_before = read_test_wal(&path).unwrap();
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        let validation = snapshot.validate();
        let wal_after = read_test_wal(&path).unwrap();

        assert!(path.join("stable_ids.hawdb").exists());
        assert_eq!(wal_after, wal_before);
        assert!(validation.is_import_ready);
        assert!(validation.stable_identity_ready);
        snapshot.relationships[0].stable_id.clone().unwrap()
    };

    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(
            db.segment_cache_snapshot()
                .expect("durable database has a segment cache")
                .resident_bytes,
            0,
            "opening the stable identity header must not load mapping pages"
        );
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();

        assert_eq!(snapshot.relationships[0].stable_id, Some(first_stable_id));
        assert!(snapshot.validate().is_import_ready);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persisted_stable_id_mapping_covers_out_of_core_base_records() {
    let path = unique_test_dir("persisted_stable_id_mapping_out_of_core");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {name: 'Mid'})")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let first = {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: hawdb_storage::StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let residency = db.storage_residency_report();
        assert!(residency.out_of_core);
        assert_eq!(residency.delta_node_count, 0);
        assert_eq!(residency.delta_relationship_count, 0);
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();

        assert!(snapshot.validate().is_import_ready);
        assert!(snapshot.nodes.iter().all(|node| node.stable_id.is_some()));
        assert!(snapshot
            .relationships
            .iter()
            .all(|relationship| relationship.stable_id.is_some()));
        snapshot
    };

    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: hawdb_storage::StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let cache_before_export = db
            .segment_cache_snapshot()
            .expect("durable database has a segment cache")
            .resident_bytes;
        assert!(
            cache_before_export < hawdb_storage::DEFAULT_STABLE_IDENTITY_PAGE_BYTES as u64,
            "reopen must keep fixed-size stable identity pages cold"
        );
        let reopened = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        assert!(
            db.segment_cache_snapshot()
                .expect("durable database has a segment cache")
                .resident_bytes
                >= cache_before_export
                    .saturating_add(hawdb_storage::DEFAULT_STABLE_IDENTITY_PAGE_BYTES as u64),
            "explicit export must demand-load the stable identity page"
        );

        assert_eq!(reopened.logical_checksum, first.logical_checksum);
        assert_eq!(
            reopened
                .nodes
                .iter()
                .map(|node| node.stable_id.clone())
                .collect::<Vec<_>>(),
            first
                .nodes
                .iter()
                .map(|node| node.stable_id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            reopened
                .relationships
                .iter()
                .map(|relationship| relationship.stable_id.clone())
                .collect::<Vec<_>>(),
            first
                .relationships
                .iter()
                .map(|relationship| relationship.stable_id.clone())
                .collect::<Vec<_>>()
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persisted_stable_id_mapping_disambiguates_and_prunes_duplicate_property_ids() {
    let path = unique_test_dir("persisted_stable_id_mapping_duplicates");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'duplicate'})").unwrap();
        db.query("CREATE (:Entity {id: 'duplicate'})").unwrap();
        let raw = db.export_canonical_graph_snapshot();
        assert_eq!(
            raw.stable_identity.duplicate_node_stable_ids,
            vec![Value::String("duplicate".to_string())]
        );

        let mapped = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        assert!(mapped.validate().is_import_ready);
        assert_ne!(mapped.nodes[0].stable_id, mapped.nodes[1].stable_id);
        assert_eq!(
            db.store.stable_id_mapping().unwrap().node_stable_ids.len(),
            2
        );

        db.query("MATCH (n:Entity) SET n.id = 'unique'").unwrap();
        let unique = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        assert!(unique.validate().is_import_ready);
        assert_eq!(
            unique
                .nodes
                .iter()
                .map(|node| node.stable_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                Some(Value::String("duplicate".to_string())),
                Some(Value::String("unique".to_string())),
            ])
        );
        assert!(db
            .store
            .stable_id_mapping()
            .unwrap()
            .node_stable_ids
            .is_empty());
    }

    {
        let db = Database::open(&path).unwrap();
        assert!(db
            .store
            .stable_id_mapping()
            .unwrap()
            .node_stable_ids
            .is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn stable_id_mapping_backup_restore_preserves_logical_identity() {
    let path = unique_test_dir("stable_id_mapping_backup_source");
    let backup = unique_test_dir("stable_id_mapping_backup_image");
    let restored = unique_test_dir("stable_id_mapping_backup_restored");
    let expected = {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {title: 'Root'})-[:LINKS]->(:Entity {name: 'Mid'})")
            .unwrap();
        let snapshot = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        db.backup_to(&backup).unwrap();
        snapshot
    };

    Database::restore_backup(&backup, &restored).unwrap();
    {
        let mut db = Database::open(&restored).unwrap();
        let actual = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();

        assert_eq!(actual.logical_checksum, expected.logical_checksum);
        assert_eq!(actual.nodes, expected.nodes);
        assert_eq!(actual.relationships, expected.relationships);
    }

    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(backup).unwrap();
    std::fs::remove_dir_all(restored).unwrap();
}

#[test]
fn storage_scrub_detects_cold_stable_id_mapping_corruption() {
    let path = unique_test_dir("stable_id_mapping_scrub_corruption");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {title: 'Root'})-[:LINKS]->(:Entity {name: 'Mid'})")
            .unwrap();
        db.export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        db.checkpoint().unwrap();
    }

    let mut db = Database::open(&path).unwrap();
    assert!(
        db.segment_cache_snapshot()
            .expect("durable database has a segment cache")
            .resident_bytes
            < hawdb_storage::DEFAULT_STABLE_IDENTITY_PAGE_BYTES as u64,
        "stable identity pages must remain cold before scrub"
    );
    let mapping_path = path.join("stable_ids.hawdb");
    let artifact_path =
        hawdb_storage::stable_identity_generation_artifact_path(&mapping_path, 1).unwrap();
    let mut bytes = std::fs::read(&artifact_path).unwrap();
    *bytes.last_mut().expect("mapping contains one page") ^= 0x80;
    std::fs::write(&artifact_path, bytes).unwrap();

    let error = db
        .scrub_storage()
        .expect_err("scrub must inspect every stable identity page");
    assert!(
        error.to_string().contains("stable identity"),
        "unexpected scrub failure: {error}"
    );
    assert!(db.storage_handle_poisoned());

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn persisted_stable_id_mapping_respects_read_only_open() {
    let path = unique_test_dir("persisted_stable_id_mapping_read_only");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
            .unwrap();
    }

    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let error = db
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap_err();

        assert!(error.to_string().contains("read-only mode"));
        assert!(!path.join("stable_ids.hawdb").exists());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hawdb_lightning_bootstrap_manifest_reports_ready_database_export() {
    let path = unique_test_dir("hawdb_lightning_bootstrap_manifest");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
        let manifest = &export.manifest;

        assert_eq!(
            manifest.protocol_version,
            HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION
        );
        assert_eq!(manifest.graph_commit_epoch, 2);
        assert_eq!(manifest.database_commit_epoch, 2);
        assert_eq!(manifest.logical_checksum, export.snapshot.logical_checksum);
        assert_eq!(
            manifest.graph_stream_checksum,
            export.graph_stream.stream_checksum
        );
        assert_eq!(manifest.graph_stream_byte_len, export.graph_stream.byte_len);
        assert_eq!(
            manifest.relational_stream_checksum,
            export.relational_stream.stream_checksum
        );
        assert_eq!(
            manifest.relational_stream_byte_len,
            export.relational_stream.byte_len
        );
        assert_eq!(manifest.relational_table_count, 1);
        assert_eq!(manifest.relational_row_count, 1);
        assert_eq!(manifest.node_count, 2);
        assert_eq!(manifest.relationship_count, 1);
        assert_eq!(manifest.label_count, 2);
        assert_eq!(manifest.relationship_type_count, 1);
        assert_eq!(manifest.node_property_count, 4);
        assert_eq!(manifest.relationship_property_count, 1);
        assert!(manifest.validation.is_import_ready);
        assert!(manifest.relational_validation.is_valid);
        assert!(manifest.validation.stable_identity_ready);
        assert!(export.snapshot.relationships[0].stable_id.is_some());
        assert!(export
            .graph_stream
            .encoded
            .starts_with("HAWDB_LIGHTNING_GRAPH_STREAM_V1\n"));
        assert!(export.graph_stream.encoded.contains("\nchecksum\t"));
        let stream_validation = export
            .graph_stream
            .validate_against_manifest(&export.manifest);
        assert!(stream_validation.is_valid);
        assert!(stream_validation.endpoint_integrity);
        assert!(stream_validation.manifest_matches);

        let corrupted = export
            .graph_stream
            .encoded
            .replace("relationship\t0\t0\t1", "relationship\t0\t0\t99");
        let corrupted_validation =
            validate_hawdb_lightning_graph_stream(&corrupted, Some(&export.manifest));
        assert!(!corrupted_validation.is_valid);
        assert!(!corrupted_validation.checksum_matches);
        assert!(!corrupted_validation.endpoint_integrity);
        assert_eq!(corrupted_validation.missing_targets.len(), 1);
    }

    {
        let mut db = Database::open(&path).unwrap();
        let first = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
        db.query("CREATE (:Source {id: 'source-1', path: '/tmp/source.md'})")
            .unwrap();
        let second = db.prepare_hawdb_lightning_bootstrap_export().unwrap();

        assert_ne!(
            first.manifest.logical_checksum,
            second.manifest.logical_checksum
        );
        assert_ne!(
            first.manifest.schema_checksum,
            second.manifest.schema_checksum
        );
        assert!(second.manifest.validation.is_import_ready);
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hawdb_lightning_graph_stream_decodes_to_import_ready_snapshot() {
    let snapshot = CanonicalGraphSnapshotExport {
        graph_commit_epoch: 42,
        logical_checksum: 0,
        stable_identity: CanonicalSnapshotIdentityAudit {
            requires_stable_id_mapping: false,
            nodes_without_stable_id: Vec::new(),
            relationships_without_stable_id: Vec::new(),
            duplicate_node_stable_ids: Vec::new(),
            duplicate_relationship_stable_ids: Vec::new(),
        },
        nodes: vec![
            CanonicalSnapshotNode {
                node_id: 9,
                stable_id: Some(Value::String("memory".to_string())),
                labels: vec!["Memory".to_string()],
                properties: BTreeMap::from([
                    ("id".to_string(), Value::String("memory".to_string())),
                    ("importance".to_string(), Value::Float(0.75)),
                ]),
            },
            CanonicalSnapshotNode {
                node_id: 7,
                stable_id: Some(Value::String("source".to_string())),
                labels: vec!["Source".to_string()],
                properties: BTreeMap::from([
                    ("id".to_string(), Value::String("source".to_string())),
                    (
                        "metadata".to_string(),
                        Value::Map(BTreeMap::from([(
                            "tags".to_string(),
                            Value::List(vec![
                                Value::String("import".to_string()),
                                Value::String("bootstrap".to_string()),
                            ]),
                        )])),
                    ),
                ]),
            },
        ],
        relationships: vec![CanonicalSnapshotRelationship {
            relationship_id: 11,
            stable_id: Some(Value::String("rel-source-memory".to_string())),
            source_node_id: 9,
            target_node_id: 7,
            rel_type: "HAS_SOURCE".to_string(),
            properties: BTreeMap::from([("weight".to_string(), Value::Int(3))]),
        }],
    };
    let snapshot = CanonicalGraphSnapshotExport {
        logical_checksum: snapshot.validate().expected_logical_checksum,
        stable_identity: CanonicalSnapshotIdentityAudit {
            requires_stable_id_mapping: false,
            nodes_without_stable_id: Vec::new(),
            relationships_without_stable_id: Vec::new(),
            duplicate_node_stable_ids: Vec::new(),
            duplicate_relationship_stable_ids: Vec::new(),
        },
        ..snapshot
    };
    let relational_stream = HawDBLightningRelationalStream::from_state(
        snapshot.graph_commit_epoch,
        &hawdb_storage::RelationalState::default(),
    )
    .unwrap();
    let manifest = snapshot.hawdb_lightning_bootstrap_manifest(&relational_stream);
    let stream = snapshot.hawdb_lightning_graph_stream();

    let decoded =
        parse_hawdb_lightning_graph_stream_export(&stream.encoded, Some(&manifest)).unwrap();

    assert_eq!(decoded.graph_commit_epoch, 42);
    assert_eq!(decoded.logical_checksum, snapshot.logical_checksum);
    assert_eq!(decoded.nodes, snapshot.nodes);
    assert_eq!(decoded.relationships, snapshot.relationships);
    assert!(decoded.validate().is_import_ready);
}

#[test]
fn hawdb_lightning_graph_stream_decode_rejects_manifest_mismatch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let mut manifest = export.manifest.clone();
    manifest.graph_commit_epoch += 1;

    let error =
        parse_hawdb_lightning_graph_stream_export(&export.graph_stream.encoded, Some(&manifest))
            .unwrap_err();

    assert!(error.to_string().contains("graph stream manifest mismatch"));
}

#[test]
fn hawdb_lightning_initial_import_readiness_requires_graph_and_projection_watermarks() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let projection = SearchProjectionFreshness {
        document_count: 2,
        import_source_graph_commit_epoch: None,
        source_graph_commit_epoch: Some(1),
        durable_source_graph_commit_epoch: Some(1),
        has_uncheckpointed_changes: false,
        full_reindex_needed: false,
        full_reindex_reasons: Vec::new(),
        metadata_repair_needed: false,
        metadata_repair_reasons: Vec::new(),
        embedding_model: Some("test-embedding".to_string()),
        embedding_version: Some("v1".to_string()),
        embedding_dimension: Some(3),
    };

    let readiness = db.hawdb_lightning_initial_import_readiness(&manifest, Some(&projection));

    assert!(readiness.ready);
    assert!(readiness.graph_import_caught_up);
    assert!(readiness.projection_watermark_caught_up);
    assert!(readiness.projection_checkpointed);
    assert!(readiness.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_readiness_blocks_missing_or_stale_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    db.query("CREATE (:Memory {id: 'after-bootstrap'})")
        .unwrap();

    let missing = db.hawdb_lightning_initial_import_readiness(&manifest, None);
    assert!(!missing.ready);
    assert!(missing
        .blocker_codes
        .contains(&"search_projection_missing".to_string()));

    let stale_projection = SearchProjectionFreshness {
        document_count: 2,
        import_source_graph_commit_epoch: None,
        source_graph_commit_epoch: Some(1),
        durable_source_graph_commit_epoch: Some(1),
        has_uncheckpointed_changes: true,
        full_reindex_needed: false,
        full_reindex_reasons: Vec::new(),
        metadata_repair_needed: false,
        metadata_repair_reasons: Vec::new(),
        embedding_model: None,
        embedding_version: None,
        embedding_dimension: None,
    };

    let stale = db.hawdb_lightning_initial_import_readiness(&manifest, Some(&stale_projection));

    assert!(!stale.ready);
    assert_eq!(stale.target_graph_commit_epoch, 2);
    assert!(stale
        .blocker_codes
        .contains(&"search_projection_watermark_behind_graph".to_string()));
    assert!(stale
        .blocker_codes
        .contains(&"search_projection_not_checkpointed".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_checkpoint_readiness_accepts_matching_checkpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let checkpoint = test_hawdb_lightning_checkpoint(&manifest);

    let readiness = hawdb_lightning_initial_import_checkpoint_readiness(&manifest, &checkpoint);

    assert!(readiness.ready);
    assert!(readiness.idempotency_key_present);
    assert_eq!(
        readiness.idempotency_key,
        Some(HawDBLightningInitialImportIdempotencyKey {
            import_id: "import-1".to_string(),
            task_id: "task-1".to_string(),
            fencing_token: "fence-1".to_string(),
            object_digest: "object-digest-1".to_string(),
        })
    );
    assert!(readiness.checkpoint_matches_manifest);
    assert!(readiness.graph_checkpoint_caught_up);
    assert!(readiness.search_projection_applied_caught_up);
    assert!(readiness.search_projection_durable_caught_up);
    assert!(readiness.batches_complete);
    assert!(readiness.document_identities_present);
    assert!(readiness.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_checkpoint_readiness_blocks_stale_checkpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        schema_checksum: manifest.schema_checksum + 1,
        applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch - 1),
        durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch - 1),
        completed_batches: 1,
        total_batches: 2,
        document_identity_count: 0,
        ..test_hawdb_lightning_checkpoint(&manifest)
    };

    let readiness = hawdb_lightning_initial_import_checkpoint_readiness(&manifest, &checkpoint);

    assert!(!readiness.ready);
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_checkpoint_manifest_mismatch".to_string()));
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_search_projection_apply_behind_graph".to_string()));
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_search_projection_checkpoint_behind_graph".to_string()));
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_batches_incomplete".to_string()));
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_document_identities_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_checkpoint_readiness_requires_idempotency_key() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        task_id: String::new(),
        ..test_hawdb_lightning_checkpoint(&manifest)
    };

    let readiness = hawdb_lightning_initial_import_checkpoint_readiness(&manifest, &checkpoint);

    assert!(!readiness.ready);
    assert!(!readiness.idempotency_key_present);
    assert_eq!(readiness.idempotency_key, None);
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_checkpoint_idempotency_key_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_checkpoint_progress_advances_monotonically() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        applied_graph_commit_epoch: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        completed_batches: 1,
        total_batches: 4,
        document_identity_count: 1,
        ..test_hawdb_lightning_checkpoint(&manifest)
    };

    let report = hawdb_lightning_initial_import_advance_checkpoint(
        &manifest,
        &checkpoint,
        HawDBLightningInitialImportCheckpointProgress {
            applied_graph_commit_epoch: manifest.graph_commit_epoch,
            applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
            durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
            completed_batches: 4,
            total_batches: 4,
            document_identity_count: manifest.node_count + manifest.relationship_count,
        },
    );

    assert!(report.accepted);
    assert!(report.blocker_codes.is_empty());
    assert!(report.readiness.ready);
    assert_eq!(
        report.resume_action.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert_eq!(report.checkpoint.completed_batches, 4);
    assert_eq!(
        report.checkpoint.applied_search_projection_commit_epoch,
        Some(manifest.graph_commit_epoch)
    );
}

#[test]
fn hawdb_lightning_initial_import_checkpoint_progress_rejects_regressions() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 3,
        total_batches: 4,
        document_identity_count: 3,
        ..test_hawdb_lightning_checkpoint(&manifest)
    };

    let report = hawdb_lightning_initial_import_advance_checkpoint(
        &manifest,
        &checkpoint,
        HawDBLightningInitialImportCheckpointProgress {
            applied_graph_commit_epoch: 0,
            applied_search_projection_commit_epoch: None,
            durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch - 1),
            completed_batches: 2,
            total_batches: 1,
            document_identity_count: 2,
        },
    );

    assert!(!report.accepted);
    assert_eq!(report.checkpoint, checkpoint);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_graph_checkpoint_regressed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_apply_regressed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_checkpoint_regressed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_completed_batches_regressed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_total_batches_regressed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_completed_batches_exceed_total".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_document_identities_regressed".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_document_identity_coverage_accepts_all_projection_kinds() {
    let coverage = hawdb_lightning_initial_import_document_identity_coverage(&[
        initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
        initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
        initial_import_document_identity(SearchProjectionKind::Entity, "entity:1"),
        initial_import_document_identity(SearchProjectionKind::Source, "source:1"),
        initial_import_document_identity(SearchProjectionKind::SourceChunk, "source_chunk:1"),
        initial_import_document_identity(SearchProjectionKind::Community, "community:1"),
    ]);

    assert!(coverage.ready);
    assert_eq!(coverage.document_identity_count, 6);
    assert_eq!(coverage.unique_document_identity_count, 6);
    assert!(coverage.missing_kinds.is_empty());
    assert!(coverage.duplicate_document_ids.is_empty());
    assert_eq!(coverage.empty_document_id_count, 0);
    assert_eq!(coverage.kind_reports.len(), 6);
    assert!(coverage.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_document_identity_coverage_fails_closed_for_gaps() {
    let coverage = hawdb_lightning_initial_import_document_identity_coverage(&[
        initial_import_document_identity(SearchProjectionKind::Memory, "shared"),
        initial_import_document_identity(SearchProjectionKind::Message, "shared"),
        initial_import_document_identity(SearchProjectionKind::Entity, "entity:1"),
        initial_import_document_identity(SearchProjectionKind::Source, "source:1"),
        initial_import_document_identity(SearchProjectionKind::Community, ""),
    ]);

    assert!(!coverage.ready);
    assert_eq!(coverage.document_identity_count, 5);
    assert_eq!(coverage.unique_document_identity_count, 3);
    assert_eq!(
        coverage.missing_kinds,
        vec![SearchProjectionKind::SourceChunk]
    );
    assert_eq!(coverage.duplicate_document_ids, vec!["shared".to_string()]);
    assert_eq!(coverage.empty_document_id_count, 1);
    assert!(coverage
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
    assert!(coverage
        .blocker_codes
        .contains(&"initial_import_document_identity_duplicate".to_string()));
    assert!(coverage
        .blocker_codes
        .contains(&"initial_import_document_identity_empty".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_resume_action_tracks_start_resume_cutover_and_quarantine() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let manifest = db
        .prepare_hawdb_lightning_bootstrap_export()
        .unwrap()
        .manifest;

    let start = crate::hawdb_lightning_initial_import_resume_action(&manifest, None);
    assert_eq!(
        start.kind,
        HawDBLightningInitialImportResumeActionKind::Start
    );
    assert_eq!(start.next_batch, Some(0));
    assert_eq!(start.idempotency_key, None);

    let partial = HawDBLightningInitialImportCheckpoint {
        applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch - 1),
        durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch - 1),
        completed_batches: 2,
        total_batches: 4,
        document_identity_count: manifest.node_count,
        ..test_hawdb_lightning_checkpoint(&manifest)
    };
    let resume = crate::hawdb_lightning_initial_import_resume_action(&manifest, Some(&partial));
    assert_eq!(
        resume.kind,
        HawDBLightningInitialImportResumeActionKind::Resume
    );
    assert_eq!(resume.next_batch, Some(2));
    assert_eq!(
        resume.idempotency_key,
        Some(HawDBLightningInitialImportIdempotencyKey {
            import_id: "import-1".to_string(),
            task_id: "task-1".to_string(),
            fencing_token: "fence-1".to_string(),
            object_digest: "object-digest-1".to_string(),
        })
    );

    let complete = HawDBLightningInitialImportCheckpoint {
        applied_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(manifest.graph_commit_epoch),
        completed_batches: 4,
        document_identity_count: manifest.node_count + manifest.relationship_count,
        ..partial.clone()
    };
    let ready = crate::hawdb_lightning_initial_import_resume_action(&manifest, Some(&complete));
    assert_eq!(
        ready.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert_eq!(ready.next_batch, None);

    let mismatched = HawDBLightningInitialImportCheckpoint {
        graph_stream_checksum: manifest.graph_stream_checksum + 1,
        ..complete
    };
    let quarantine =
        crate::hawdb_lightning_initial_import_resume_action(&manifest, Some(&mismatched));
    assert_eq!(
        quarantine.kind,
        HawDBLightningInitialImportResumeActionKind::Quarantine
    );
    assert_eq!(quarantine.next_batch, None);

    let missing_idempotency = HawDBLightningInitialImportCheckpoint {
        import_id: String::new(),
        ..test_hawdb_lightning_checkpoint(&manifest)
    };
    let quarantine =
        crate::hawdb_lightning_initial_import_resume_action(&manifest, Some(&missing_idempotency));
    assert_eq!(
        quarantine.kind,
        HawDBLightningInitialImportResumeActionKind::Quarantine
    );
    assert!(quarantine
        .blocker_codes
        .contains(&"initial_import_checkpoint_idempotency_key_missing".to_string()));
    assert_eq!(quarantine.idempotency_key, None);
}

#[test]
fn hawdb_lightning_initial_import_plan_reports_ready_cutover() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let freshness = initial_import_projection_freshness(&export.manifest);
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);

    let plan = db.hawdb_lightning_initial_import_plan(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        Some(&freshness),
        Some(&checkpoint),
    );

    assert!(plan.ready_for_graph_import);
    assert!(plan.ready_for_cutover);
    assert!(plan.graph_stream_validation.is_valid);
    assert!(plan.decoded_snapshot_import_ready);
    assert_eq!(
        plan.decoded_graph_commit_epoch,
        Some(export.manifest.graph_commit_epoch)
    );
    assert_eq!(plan.decoded_node_count, Some(export.manifest.node_count));
    assert_eq!(
        plan.decoded_relationship_count,
        Some(export.manifest.relationship_count)
    );
    assert!(plan.target_readiness.ready);
    assert!(plan
        .checkpoint_readiness
        .as_ref()
        .is_some_and(|readiness| readiness.ready));
    assert_eq!(
        plan.resume_action.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert!(plan.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_plan_accepts_complete_document_identity_coverage() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let freshness = initial_import_projection_freshness(&export.manifest);
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let document_identities = all_initial_import_document_identities();

    let plan = db.hawdb_lightning_initial_import_plan_with_document_identities(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        Some(&freshness),
        Some(&checkpoint),
        &document_identities,
    );

    assert!(plan.ready_for_graph_import);
    assert!(plan.ready_for_cutover);
    assert!(plan
        .document_identity_coverage
        .as_ref()
        .is_some_and(|coverage| coverage.ready));
    assert!(plan.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_search_projection_batch_reports_checkpoint_progress() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 1,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let report = hawdb_lightning_initial_import_search_projection_batch_report(
        &export.manifest,
        Some(&checkpoint),
        &delta,
        0,
        1,
    );

    assert!(report.ready);
    assert!(report.checkpoint_present);
    assert!(report.checkpoint_matches_manifest);
    assert!(report.checkpoint_idempotency_key_present);
    assert!(report.total_batches_match_checkpoint);
    assert!(report.source_graph_commit_epoch_matches);
    assert!(report.batch_position_valid);
    assert!(report.operation_limit_ok);
    assert!(!report.empty_batch);
    assert_eq!(report.delete_count, 0);
    assert_eq!(report.operation_count, 6);
    assert!(report.document_identity_coverage.ready);
    assert_eq!(
        report.document_identity_coverage.observed_kinds,
        vec![
            SearchProjectionKind::Memory,
            SearchProjectionKind::Message,
            SearchProjectionKind::Entity,
            SearchProjectionKind::Source,
            SearchProjectionKind::SourceChunk,
            SearchProjectionKind::Community,
        ]
    );
    let progress = report
        .checkpoint_progress
        .expect("expected checkpoint progress");
    assert_eq!(
        progress.applied_search_projection_commit_epoch,
        Some(export.manifest.graph_commit_epoch)
    );
    assert_eq!(progress.durable_search_projection_commit_epoch, None);
    assert_eq!(progress.completed_batches, 1);
    assert_eq!(progress.total_batches, 1);
    assert_eq!(progress.document_identity_count, 6);
    assert!(report.checkpoint_progress_accepted);
    let progress_readiness = report
        .checkpoint_progress_readiness
        .as_ref()
        .expect("expected checkpoint progress readiness");
    assert!(!progress_readiness.ready);
    assert!(progress_readiness.search_projection_applied_caught_up);
    assert!(!progress_readiness.search_projection_durable_caught_up);
    assert_eq!(
        report
            .checkpoint_resume_action
            .as_ref()
            .expect("expected checkpoint resume action")
            .kind,
        HawDBLightningInitialImportResumeActionKind::Resume
    );
    assert!(report.checkpoint_progress_blocker_codes.is_empty());

    let advanced =
        hawdb_lightning_initial_import_advance_checkpoint(&export.manifest, &checkpoint, progress);
    assert!(advanced.accepted);
    assert_eq!(advanced.checkpoint.completed_batches, 1);
    assert_eq!(
        advanced.checkpoint.applied_search_projection_commit_epoch,
        Some(export.manifest.graph_commit_epoch)
    );
}

#[test]
fn hawdb_lightning_initial_import_search_projection_batch_reports_cutover_ready_progress() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 1,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let report = hawdb_lightning_initial_import_search_projection_batch_report(
        &export.manifest,
        Some(&checkpoint),
        &delta,
        0,
        1,
    );

    assert!(report.ready);
    assert!(report.checkpoint_progress_accepted);
    let progress_readiness = report
        .checkpoint_progress_readiness
        .as_ref()
        .expect("expected checkpoint progress readiness");
    assert!(progress_readiness.ready);
    assert!(progress_readiness.search_projection_applied_caught_up);
    assert!(progress_readiness.search_projection_durable_caught_up);
    assert_eq!(
        report
            .checkpoint_resume_action
            .as_ref()
            .expect("expected checkpoint resume action")
            .kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert!(report.checkpoint_progress_blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_durable_state_persists_partial_resume_progress() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 3,
        document_identity_count: 2,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let identities = vec![
        initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
        initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
    ];

    let report = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &identities,
    );

    assert!(report.persistable);
    assert!(!report.ready_for_cutover);
    assert_eq!(
        report.resume_action.kind,
        HawDBLightningInitialImportResumeActionKind::Resume
    );
    assert!(report
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
    let state = report.state.expect("expected persistable durable state");
    assert_eq!(
        state.source_fingerprint.graph_commit_epoch,
        export.manifest.graph_commit_epoch
    );
    assert_eq!(state.checkpoint.completed_batches, 1);
    assert_eq!(state.checkpoint.total_batches, 3);
    assert_eq!(state.document_identities, identities);
    assert!(!state.document_identity_coverage.ready);
}

#[test]
fn hawdb_lightning_initial_import_durable_state_reports_cutover_ready_checkpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let identities = all_initial_import_document_identities();

    let report = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &identities,
    );

    assert!(report.persistable);
    assert!(report.ready_for_cutover);
    assert!(report.checkpoint_readiness.ready);
    assert_eq!(
        report.resume_action.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert!(report.blocker_codes.is_empty());
    let state = report.state.expect("expected persistable durable state");
    assert_eq!(state.document_identity_coverage.document_identity_count, 6);
    assert_eq!(
        state.source_fingerprint.schema_checksum,
        export.manifest.schema_checksum
    );
}

#[test]
fn hawdb_lightning_initial_import_durable_state_codec_round_trips_json_string() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");

    let encoded = hawdb_lightning_initial_import_encode_durable_state(&state).unwrap();
    let decoded = db
        .hawdb_lightning_initial_import_decode_durable_state(&export.manifest, &encoded)
        .unwrap();

    assert!(decoded.ready);
    assert!(decoded.source_fingerprint_matches_manifest);
    assert_eq!(decoded.state, Some(state));
    assert!(decoded.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_durable_state_codec_blocks_source_mismatch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let mut encoded = hawdb_lightning_initial_import_encode_durable_state(&state).unwrap();
    let mut value = serde_json::from_str::<serde_json::Value>(&encoded).unwrap();
    value["source_fingerprint"]["schema_checksum"] = serde_json::json!(0);
    encoded = serde_json::to_string(&value).unwrap();

    let decoded = hawdb_lightning_initial_import_decode_durable_state(
        &export.manifest,
        &serde_json::to_string(&value).unwrap(),
    )
    .unwrap();
    let decoded_from_string = db
        .hawdb_lightning_initial_import_decode_durable_state(&export.manifest, &encoded)
        .unwrap();

    assert!(!decoded.ready);
    assert_eq!(decoded.state, None);
    assert!(!decoded.source_fingerprint_matches_manifest);
    assert!(decoded
        .blocker_codes
        .contains(&"initial_import_durable_state_codec_source_mismatch".to_string()));
    assert_eq!(decoded_from_string.blocker_codes, decoded.blocker_codes);
}

#[test]
fn hawdb_lightning_initial_import_durable_state_codec_redacts_malformed_json() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();

    let error = db
        .hawdb_lightning_initial_import_decode_durable_state(&export.manifest, "{")
        .unwrap_err()
        .to_string();

    assert_eq!(
        error,
        "semantic error: initial import durable state parse failed: invalid_json"
    );
}

#[test]
fn hawdb_lightning_initial_import_session_resumes_from_durable_state() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 3,
        document_identity_count: 2,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &[
            initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
            initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
        ],
    )
    .state
    .expect("expected persistable durable state");

    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        None,
        Some(&state),
    );

    assert!(session.ready_for_database_import);
    assert!(!session.ready_for_cutover);
    assert!(session.durable_state_present);
    assert!(session.durable_state_source_matches_manifest);
    assert_eq!(
        session.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::Resume
    );
    assert_eq!(session.next_action.next_batch, Some(1));
    assert!(session
        .blocker_codes
        .contains(&"search_projection_missing".to_string()));
    assert!(session
        .blocker_codes
        .contains(&"initial_import_checkpoint_incomplete".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_session_quarantines_mismatched_durable_state() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let mut state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    state.source_fingerprint.graph_stream_checksum = state
        .source_fingerprint
        .graph_stream_checksum
        .saturating_add(1);
    let freshness = initial_import_projection_freshness(&export.manifest);

    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
        Some(&state),
    );

    assert!(!session.ready_for_database_import);
    assert!(!session.ready_for_cutover);
    assert!(!session.durable_state_source_matches_manifest);
    assert_eq!(
        session.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::Quarantine
    );
    assert!(session.next_action.next_batch.is_none());
    assert!(session
        .blocker_codes
        .contains(&"initial_import_durable_state_source_mismatch".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_cutover_catch_up_accepts_matching_live_watermark() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);
    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
        Some(&durable_state),
    );

    let report = hawdb_lightning_initial_import_cutover_catch_up_report(
        &session,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
    );

    assert!(report.ready);
    assert!(report.session_ready_for_cutover);
    assert!(report.durable_state_present);
    assert!(report.graph_watermark_caught_up);
    assert!(report.search_projection_watermark_caught_up);
    assert_eq!(
        report.cutover_watermark,
        Some(export.manifest.graph_commit_epoch)
    );
    assert!(report.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_session_bundle_readiness_accepts_cutover_ready_session() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let source_bundle = hawdb_lightning_initial_import_source_bundle_readiness(
        &export.manifest,
        Some(&checkpoint),
        &[delta],
    );
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);
    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
        Some(&durable_state),
    );
    let catch_up = hawdb_lightning_initial_import_cutover_catch_up_report(
        &session,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
    );

    let readiness = hawdb_lightning_initial_import_session_bundle_readiness(
        &source_bundle,
        &session,
        Some(&catch_up),
    );

    assert!(readiness.ready);
    assert!(readiness.resumable);
    assert!(readiness.ready_for_cutover);
    assert!(readiness.source_bundle_ready);
    assert!(readiness.session_ready_for_cutover);
    assert!(readiness.catch_up_required);
    assert!(readiness.catch_up_present);
    assert!(readiness.catch_up_ready);
    assert_eq!(
        readiness.cutover_watermark,
        Some(export.manifest.graph_commit_epoch)
    );
    assert_eq!(
        readiness.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert!(readiness.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_session_bundle_readiness_requires_catch_up_for_cutover() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let source_bundle = hawdb_lightning_initial_import_source_bundle_readiness(
        &export.manifest,
        Some(&checkpoint),
        &[delta],
    );
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);
    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
        Some(&durable_state),
    );

    let readiness =
        hawdb_lightning_initial_import_session_bundle_readiness(&source_bundle, &session, None);

    assert!(!readiness.ready);
    assert!(readiness.resumable);
    assert!(!readiness.ready_for_cutover);
    assert!(readiness.catch_up_required);
    assert!(!readiness.catch_up_present);
    assert!(!readiness.catch_up_ready);
    assert!(readiness
        .blocker_codes
        .contains(&"initial_import_session_bundle_cutover_catch_up_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_startup_readiness_accepts_ready_session() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);

    let projection_batches = [delta];
    let report = db.hawdb_lightning_initial_import_startup_readiness(
        HawDBLightningInitialImportReadinessInputs {
            encoded_graph_stream: &export.graph_stream.encoded,
            encoded_relational_stream: &export.relational_stream.encoded,
            manifest: &export.manifest,
            projection_batches: &projection_batches,
            target_projection_freshness: Some(&freshness),
            live_projection_freshness: Some(&freshness),
        },
        Some(&durable_state),
    );

    assert!(report.ready);
    assert!(report.source_bundle.ready);
    assert!(report.session.ready_for_cutover);
    assert!(report
        .cutover_catch_up
        .as_ref()
        .is_some_and(|catch_up| catch_up.ready));
    assert!(report.readiness.ready_for_cutover);
    assert!(report.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_startup_readiness_blocks_live_projection_lag() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let target_freshness = initial_import_projection_freshness(&export.manifest);
    let mut live_freshness = target_freshness.clone();
    live_freshness.durable_source_graph_commit_epoch =
        Some(export.manifest.graph_commit_epoch.saturating_sub(1));

    let projection_batches = [delta];
    let report = db.hawdb_lightning_initial_import_startup_readiness(
        HawDBLightningInitialImportReadinessInputs {
            encoded_graph_stream: &export.graph_stream.encoded,
            encoded_relational_stream: &export.relational_stream.encoded,
            manifest: &export.manifest,
            projection_batches: &projection_batches,
            target_projection_freshness: Some(&target_freshness),
            live_projection_freshness: Some(&live_freshness),
        },
        Some(&durable_state),
    );

    assert!(!report.ready);
    assert!(report.source_bundle.ready);
    assert!(report.session.ready_for_cutover);
    assert!(report
        .cutover_catch_up
        .as_ref()
        .is_some_and(|catch_up| !catch_up.ready));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_live_search_projection_watermark_not_caught_up".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_session_bundle_cutover_catch_up_not_ready".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_recovery_readiness_resumes_encoded_state() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let encoded = hawdb_lightning_initial_import_encode_durable_state(&durable_state).unwrap();
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let freshness = initial_import_projection_freshness(&export.manifest);

    let projection_batches = [delta];
    let report = db.hawdb_lightning_initial_import_recovery_readiness(
        HawDBLightningInitialImportReadinessInputs {
            encoded_graph_stream: &export.graph_stream.encoded,
            encoded_relational_stream: &export.relational_stream.encoded,
            manifest: &export.manifest,
            projection_batches: &projection_batches,
            target_projection_freshness: Some(&freshness),
            live_projection_freshness: Some(&freshness),
        },
        Some(&encoded),
    );

    assert!(report.ready);
    assert!(report.durable_state_payload_present);
    assert!(report
        .durable_state_codec
        .as_ref()
        .is_some_and(|codec| codec.ready));
    assert_eq!(
        report.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::ReadyForCutover
    );
    assert!(report.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_recovery_readiness_quarantines_invalid_payload() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();

    let report = db.hawdb_lightning_initial_import_recovery_readiness(
        HawDBLightningInitialImportReadinessInputs {
            encoded_graph_stream: &export.graph_stream.encoded,
            encoded_relational_stream: &export.relational_stream.encoded,
            manifest: &export.manifest,
            projection_batches: &[],
            target_projection_freshness: None,
            live_projection_freshness: None,
        },
        Some("{"),
    );

    assert!(!report.ready);
    assert!(report.durable_state_payload_present);
    assert_eq!(report.durable_state_codec, None);
    assert_eq!(
        report.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::Quarantine
    );
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_codec_decode_failed".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_recovery_durable_state_quarantine_required".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_recovery_readiness_quarantines_source_mismatch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let mut value = serde_json::from_str::<serde_json::Value>(
        &hawdb_lightning_initial_import_encode_durable_state(&durable_state).unwrap(),
    )
    .unwrap();
    value["source_fingerprint"]["schema_checksum"] = serde_json::json!(0);
    let encoded = serde_json::to_string(&value).unwrap();

    let report = db.hawdb_lightning_initial_import_recovery_readiness(
        HawDBLightningInitialImportReadinessInputs {
            encoded_graph_stream: &export.graph_stream.encoded,
            encoded_relational_stream: &export.relational_stream.encoded,
            manifest: &export.manifest,
            projection_batches: &[],
            target_projection_freshness: None,
            live_projection_freshness: None,
        },
        Some(&encoded),
    );

    assert!(!report.ready);
    assert!(report
        .durable_state_codec
        .as_ref()
        .is_some_and(|codec| !codec.ready));
    assert_eq!(
        report.next_action.kind,
        HawDBLightningInitialImportResumeActionKind::Quarantine
    );
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_codec_source_mismatch".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_cutover_catch_up_blocks_live_mutation_lag() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);
    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        export.manifest.graph_commit_epoch,
        Some(&freshness),
        Some(&durable_state),
    );

    let report = hawdb_lightning_initial_import_cutover_catch_up_report(
        &session,
        export.manifest.graph_commit_epoch + 1,
        Some(&freshness),
    );

    assert!(!report.ready);
    assert!(report.graph_watermark_caught_up);
    assert!(!report.search_projection_watermark_caught_up);
    assert_eq!(report.cutover_watermark, None);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_live_search_projection_watermark_not_caught_up".to_string()));
}

#[test]
fn database_initial_import_cutover_catch_up_uses_current_graph_epoch() {
    let mut source = Database::new();
    source
        .query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let durable_state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable durable state");
    let freshness = initial_import_projection_freshness(&export.manifest);
    let mut target = Database::new();
    target
        .hawdb_lightning_initial_import_apply_with_document_identities(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            Some(&freshness),
            Some(&checkpoint),
            &all_initial_import_document_identities(),
        )
        .unwrap();
    let session = hawdb_lightning_initial_import_session_report(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        target.store.commit_epoch(),
        Some(&freshness),
        Some(&durable_state),
    );

    let report =
        target.hawdb_lightning_initial_import_cutover_catch_up_report(&session, Some(&freshness));

    assert!(report.ready);
    assert_eq!(report.live_graph_commit_epoch, target.store.commit_epoch());
    assert_eq!(report.cutover_watermark, Some(target.store.commit_epoch()));
}

#[test]
fn hawdb_lightning_initial_import_durable_state_rejects_unstable_checkpoint_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        import_id: String::new(),
        schema_checksum: export.manifest.schema_checksum + 1,
        completed_batches: 4,
        total_batches: 3,
        document_identity_count: 7,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let identities = all_initial_import_document_identities();

    let report = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &identities,
    );

    assert!(!report.persistable);
    assert!(!report.ready_for_cutover);
    assert!(report.state.is_none());
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_idempotency_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_manifest_mismatch".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_completed_batches_exceed_total".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_state_document_identity_regressed".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_durable_state_advances_with_search_projection_batch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 1,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let initial =
        hawdb_lightning_initial_import_durable_state_report(&export.manifest, &checkpoint, &[])
            .state
            .expect("expected persistable initial durable state");
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let report = hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch(
        &export.manifest,
        &initial,
        &delta,
        0,
        1,
    );

    assert!(report.ready);
    assert!(!report.idempotent_replay);
    assert!(report.batch_report.ready);
    assert!(report.durable_state_report.ready_for_cutover);
    let state = report
        .durable_state_report
        .state
        .expect("expected advanced durable state");
    assert_eq!(state.checkpoint.completed_batches, 1);
    assert_eq!(state.checkpoint.total_batches, 1);
    assert_eq!(
        state.checkpoint.applied_search_projection_commit_epoch,
        Some(export.manifest.graph_commit_epoch)
    );
    assert_eq!(state.checkpoint.document_identity_count, 6);
    assert_eq!(state.document_identities.len(), 6);
}

#[test]
fn hawdb_lightning_initial_import_streaming_batches_advance_before_final_coverage() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 2,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let initial =
        hawdb_lightning_initial_import_durable_state_report(&export.manifest, &checkpoint, &[])
            .state
            .expect("expected persistable initial durable state");
    let mut rows = all_initial_import_projection_rows();
    let first = SearchProjectionDelta {
        upserts: vec![rows.remove(0)],
        deletes: Vec::new(),
        max_operations: Some(1),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let first_report = hawdb_lightning_initial_import_advance_durable_state_streaming(
        &export.manifest,
        &initial,
        &first,
        0,
        2,
    );

    assert!(first_report.accepted);
    assert!(!first_report.completed);
    assert!(!first_report.ready_for_cutover);
    let after_first = first_report
        .durable_state_report
        .state
        .expect("expected persisted partial state");
    assert_eq!(after_first.checkpoint.completed_batches, 1);
    assert_eq!(after_first.document_identities.len(), 1);

    let second = SearchProjectionDelta {
        upserts: rows,
        deletes: Vec::new(),
        max_operations: Some(5),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let second_report = hawdb_lightning_initial_import_advance_durable_state_streaming(
        &export.manifest,
        &after_first,
        &second,
        1,
        2,
    );

    assert!(second_report.accepted);
    assert!(second_report.completed);
    assert!(second_report.ready_for_cutover);
    assert_eq!(
        second_report
            .durable_state_report
            .state
            .expect("expected final durable state")
            .document_identities
            .len(),
        6
    );
}

#[test]
fn hawdb_lightning_initial_import_durable_state_treats_completed_batch_as_idempotent_replay() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 1,
        document_identity_count: 6,
        applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let state = hawdb_lightning_initial_import_durable_state_report(
        &export.manifest,
        &checkpoint,
        &all_initial_import_document_identities(),
    )
    .state
    .expect("expected persistable initial durable state");
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let report = hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch(
        &export.manifest,
        &state,
        &delta,
        0,
        1,
    );

    assert!(report.ready);
    assert!(report.idempotent_replay);
    let replayed = report
        .durable_state_report
        .state
        .expect("expected replayed durable state");
    assert_eq!(replayed.checkpoint.completed_batches, 1);
    assert_eq!(replayed.checkpoint.document_identity_count, 6);
    assert_eq!(replayed.document_identities.len(), 6);
    assert!(replayed.document_identity_coverage.ready);
}

#[test]
fn hawdb_lightning_initial_import_durable_state_blocks_invalid_batch_advance() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 1,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let state =
        hawdb_lightning_initial_import_durable_state_report(&export.manifest, &checkpoint, &[])
            .state
            .expect("expected persistable initial durable state");
    let delta = SearchProjectionDelta {
        upserts: vec![initial_import_projection_row(
            SearchProjectionKind::Memory,
            "1",
        )],
        deletes: Vec::new(),
        max_operations: Some(1),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch + 1),
    };

    let report = hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch(
        &export.manifest,
        &state,
        &delta,
        0,
        1,
    );

    assert!(!report.ready);
    assert!(!report.idempotent_replay);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_epoch_mismatch".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_durable_batch_checkpoint_progress_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_search_projection_batch_accepts_cumulative_identities() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 1,
        total_batches: 3,
        document_identity_count: 4,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: vec![
            initial_import_projection_row(SearchProjectionKind::SourceChunk, "1"),
            initial_import_projection_row(SearchProjectionKind::Community, "1"),
        ],
        deletes: Vec::new(),
        max_operations: Some(2),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };
    let cumulative_identities = all_initial_import_document_identities();

    let report =
        hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities(
            &export.manifest,
            Some(&checkpoint),
            &delta,
            1,
            3,
            &cumulative_identities,
        );

    assert!(report.ready);
    assert!(report.checkpoint_present);
    assert!(report.total_batches_match_checkpoint);
    assert_eq!(report.operation_count, 2);
    assert_eq!(report.document_identity_coverage.document_identity_count, 6);
    let progress = report
        .checkpoint_progress
        .expect("expected checkpoint progress");
    assert_eq!(progress.completed_batches, 2);
    assert_eq!(progress.total_batches, 3);
    assert_eq!(progress.document_identity_count, 6);
}

#[test]
fn hawdb_lightning_initial_import_search_projection_batch_fails_closed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let delta = SearchProjectionDelta {
        upserts: vec![initial_import_projection_row(
            SearchProjectionKind::Memory,
            "1",
        )],
        deletes: vec!["memory:old".to_string()],
        max_operations: Some(1),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch + 1),
    };

    let report = hawdb_lightning_initial_import_search_projection_batch_report(
        &export.manifest,
        None,
        &delta,
        2,
        1,
    );

    assert!(!report.ready);
    assert!(!report.checkpoint_present);
    assert!(!report.checkpoint_matches_manifest);
    assert!(!report.checkpoint_idempotency_key_present);
    assert!(!report.total_batches_match_checkpoint);
    assert!(!report.source_graph_commit_epoch_matches);
    assert!(!report.batch_position_valid);
    assert!(!report.operation_limit_ok);
    assert_eq!(report.delete_count, 1);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_checkpoint_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_epoch_mismatch".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_position_invalid".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_limit_exceeded".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_has_deletes".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
    assert!(report.checkpoint_progress.is_none());
    assert!(!report.checkpoint_progress_accepted);
    assert!(report.checkpoint_progress_readiness.is_none());
    assert!(report.checkpoint_resume_action.is_none());
    assert!(report.checkpoint_progress_blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_search_projection_batch_rejects_checkpoint_total_mismatch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 2,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let delta = SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    };

    let report = hawdb_lightning_initial_import_search_projection_batch_report(
        &export.manifest,
        Some(&checkpoint),
        &delta,
        0,
        3,
    );

    assert!(!report.ready);
    assert!(report.checkpoint_present);
    assert!(report.checkpoint_matches_manifest);
    assert!(report.checkpoint_idempotency_key_present);
    assert!(!report.total_batches_match_checkpoint);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_total_mismatch".to_string()));
    assert!(report.checkpoint_progress.is_none());
    assert!(!report.checkpoint_progress_accepted);
    assert!(report.checkpoint_progress_readiness.is_none());
    assert!(report.checkpoint_resume_action.is_none());
    assert!(report.checkpoint_progress_blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_source_bundle_accepts_graph_and_projection_sources() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let checkpoint = HawDBLightningInitialImportCheckpoint {
        completed_batches: 0,
        total_batches: 1,
        document_identity_count: 0,
        applied_search_projection_commit_epoch: None,
        durable_search_projection_commit_epoch: None,
        ..test_hawdb_lightning_checkpoint(&export.manifest)
    };
    let projection_batches = vec![SearchProjectionDelta {
        upserts: all_initial_import_projection_rows(),
        deletes: Vec::new(),
        max_operations: Some(6),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
    }];

    let report = hawdb_lightning_initial_import_source_bundle_readiness(
        &export.manifest,
        Some(&checkpoint),
        &projection_batches,
    );

    assert!(report.ready);
    assert!(report.database_source_import_ready);
    assert!(report.checkpoint_present);
    assert_eq!(report.projection_batch_count, 1);
    assert_eq!(report.ready_projection_batch_count, 1);
    assert_eq!(report.total_batches, 1);
    assert_eq!(
        report.source_fingerprint.graph_commit_epoch,
        export.manifest.graph_commit_epoch
    );
    assert!(report.document_identity_coverage.ready);
    assert_eq!(report.document_identity_coverage.document_identity_count, 6);
    assert_eq!(report.batch_reports.len(), 1);
    assert!(report.batch_reports[0].checkpoint_progress_accepted);
}

#[test]
fn hawdb_lightning_initial_import_source_bundle_fails_closed_for_source_gaps() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let projection_batches = vec![SearchProjectionDelta {
        upserts: vec![
            initial_import_projection_row(SearchProjectionKind::Memory, "1"),
            initial_import_projection_row(SearchProjectionKind::Message, "1"),
        ],
        deletes: Vec::new(),
        max_operations: Some(2),
        source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch + 1),
    }];

    let report = hawdb_lightning_initial_import_source_bundle_readiness(
        &export.manifest,
        None,
        &projection_batches,
    );

    assert!(!report.ready);
    assert!(!report.checkpoint_present);
    assert_eq!(report.projection_batch_count, 1);
    assert_eq!(report.ready_projection_batch_count, 0);
    assert!(report
        .blocker_codes
        .contains(&"initial_import_source_bundle_checkpoint_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_checkpoint_missing".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_search_projection_batch_epoch_mismatch".to_string()));
    assert!(report
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_plan_blocks_incomplete_document_identity_coverage() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let freshness = initial_import_projection_freshness(&export.manifest);
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let document_identities = vec![
        initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
        initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
        initial_import_document_identity(SearchProjectionKind::Entity, "entity:1"),
        initial_import_document_identity(SearchProjectionKind::Source, "source:1"),
        initial_import_document_identity(SearchProjectionKind::Community, "community:1"),
    ];

    let plan = db.hawdb_lightning_initial_import_plan_with_document_identities(
        &export.graph_stream.encoded,
        &export.relational_stream.encoded,
        &export.manifest,
        Some(&freshness),
        Some(&checkpoint),
        &document_identities,
    );

    assert!(plan.ready_for_graph_import);
    assert!(!plan.ready_for_cutover);
    let coverage = plan.document_identity_coverage.as_ref().unwrap();
    assert!(!coverage.ready);
    assert!(coverage
        .missing_kinds
        .contains(&SearchProjectionKind::SourceChunk));
    assert!(plan
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_plan_fails_closed_for_invalid_stream_and_missing_checkpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root'})-[:LINKS {id: 'rel'}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let invalid_stream = export
        .graph_stream
        .encoded
        .replace("checksum\t", "bad-checksum\t");

    let plan = db.hawdb_lightning_initial_import_plan(
        &invalid_stream,
        &export.relational_stream.encoded,
        &export.manifest,
        None,
        None,
    );

    assert!(!plan.ready_for_graph_import);
    assert!(!plan.ready_for_cutover);
    assert!(!plan.graph_stream_validation.is_valid);
    assert!(!plan.decoded_snapshot_import_ready);
    assert_eq!(plan.decoded_graph_commit_epoch, None);
    assert_eq!(plan.checkpoint_readiness, None);
    assert_eq!(
        plan.resume_action.kind,
        HawDBLightningInitialImportResumeActionKind::Start
    );
    assert!(plan
        .blocker_codes
        .contains(&"hawdb_lightning_graph_stream_invalid".to_string()));
    assert!(plan
        .blocker_codes
        .contains(&"search_projection_missing".to_string()));
    assert!(plan
        .blocker_codes
        .contains(&"initial_import_checkpoint_missing".to_string()));
}

#[test]
fn hawdb_lightning_initial_import_apply_imports_database_state_into_empty_target() {
    let mut source = Database::new();
    source
        .query("CREATE (:Memory {id: 'root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid'})")
        .unwrap();
    source
        .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
        .unwrap();
    let large_body = "hawdb-lightning-payload".repeat(512);
    source
        .query_sql_with_params(
            "INSERT INTO public.messages (id, body) VALUES ($1, $2)",
            &[
                Value::String("message-1".to_string()),
                Value::String(large_body.clone()),
            ],
        )
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    assert_eq!(export.manifest.relational_table_count, 2);
    assert_eq!(export.manifest.relational_row_count, 2);
    assert_eq!(export.manifest.relational_overflow_segment_count, 1);
    let path = unique_test_dir("hawdb_lightning_initial_import_apply");
    {
        let mut target = Database::open(&path).unwrap();
        let report = target
            .hawdb_lightning_initial_import_apply(
                &export.graph_stream.encoded,
                &export.relational_stream.encoded,
                &export.manifest,
                None,
                None,
            )
            .unwrap();

        assert!(report.applied);
        assert!(!report.ready_for_cutover);
        assert_eq!(report.node_count, export.manifest.node_count);
        assert_eq!(
            report.relationship_count,
            export.manifest.relationship_count
        );
        assert_eq!(report.database_commit_epoch, 2);
        assert_eq!(report.relational_table_count, 2);
        assert_eq!(report.relational_row_count, 2);
        assert!(report.blocker_codes.is_empty());

        let imported = target
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();
        assert_eq!(imported.logical_checksum, export.snapshot.logical_checksum);
        assert_eq!(imported.stable_identity, export.snapshot.stable_identity);
        assert_eq!(imported.nodes, export.snapshot.nodes);
        assert_eq!(imported.relationships, export.snapshot.relationships);
        let rows = target
            .query_sql_with_params(
                "SELECT id, body FROM public.messages WHERE id = $1",
                &[Value::String("message-1".to_string())],
            )
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0]["body"], Value::String(large_body.clone()));
    }
    {
        let mut reopened = Database::open(&path).unwrap();
        let imported = reopened
            .export_canonical_graph_snapshot_with_persisted_stable_ids()
            .unwrap();

        assert_eq!(imported.logical_checksum, export.snapshot.logical_checksum);
        assert_eq!(imported.stable_identity, export.snapshot.stable_identity);
        let rows = reopened
            .query_sql("SELECT id, body FROM public.messages")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0]["body"], Value::String(large_body.clone()));

        let retry = reopened
            .hawdb_lightning_initial_import_apply(
                &export.graph_stream.encoded,
                &export.relational_stream.encoded,
                &export.manifest,
                None,
                None,
            )
            .unwrap();
        assert!(!retry.applied);
        assert!(retry.blocker_codes.is_empty());
        assert_eq!(retry.node_count, export.manifest.node_count);
        assert_eq!(retry.relationship_count, export.manifest.relationship_count);
        reopened.checkpoint().unwrap();
    }
    {
        let mut reopened = Database::open(&path).unwrap();
        let retry = reopened
            .hawdb_lightning_initial_import_apply(
                &export.graph_stream.encoded,
                &export.relational_stream.encoded,
                &export.manifest,
                None,
                None,
            )
            .unwrap();
        assert!(!retry.applied);
        assert!(retry.blocker_codes.is_empty());

        let mut corrupt_relational_stream = export.relational_stream.encoded.clone();
        *corrupt_relational_stream
            .last_mut()
            .expect("relational stream must not be empty") ^= 0xff;
        let corrupt_retry = reopened
            .hawdb_lightning_initial_import_apply(
                &export.graph_stream.encoded,
                &corrupt_relational_stream,
                &export.manifest,
                None,
                None,
            )
            .unwrap();
        assert!(!corrupt_retry.applied);
        assert!(corrupt_retry
            .blocker_codes
            .contains(&"hawdb_lightning_database_streams_not_import_ready".to_string()));

        let mut different_source = Database::new();
        different_source
            .query("CREATE (:Memory {id: 'other'})")
            .unwrap();
        let different_export = different_source
            .prepare_hawdb_lightning_bootstrap_export()
            .unwrap();
        let mismatch = reopened
            .hawdb_lightning_initial_import_apply(
                &different_export.graph_stream.encoded,
                &different_export.relational_stream.encoded,
                &different_export.manifest,
                None,
                None,
            )
            .unwrap();
        assert!(!mismatch.applied);
        assert!(mismatch
            .blocker_codes
            .contains(&"hawdb_lightning_initial_import_source_fingerprint_mismatch".to_string()));
    }
}

#[test]
fn hawdb_lightning_initial_import_rejects_corrupt_relational_stream_atomically() {
    let mut source = Database::new();
    source.query("CREATE (:Memory {id: 'root'})").unwrap();
    source
        .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let mut relational_stream = export.relational_stream.encoded.clone();
    let last = relational_stream
        .last_mut()
        .expect("relational stream must not be empty");
    *last ^= 0xff;
    let mut target = Database::new();

    let report = target
        .hawdb_lightning_initial_import_apply(
            &export.graph_stream.encoded,
            &relational_stream,
            &export.manifest,
            None,
            None,
        )
        .unwrap();

    assert!(!report.applied);
    assert_eq!(report.node_count, 0);
    assert_eq!(report.relational_table_count, 0);
    assert!(report
        .plan
        .blocker_codes
        .contains(&"hawdb_lightning_relational_stream_invalid".to_string()));
    assert!(target.export_canonical_graph_snapshot().nodes.is_empty());
    assert!(target.store.relational_state().is_empty());
}

#[test]
fn hawdb_lightning_initial_import_rejects_stream_without_engine_registry() {
    let mut source = Database::new();
    source.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let relational_stream = HawDBLightningRelationalStream::from_state(
        export.manifest.database_commit_epoch,
        &hawdb_storage::RelationalState::default(),
    )
    .unwrap();
    let manifest = export
        .snapshot
        .hawdb_lightning_bootstrap_manifest(&relational_stream);
    let mut target = Database::new();

    let error = target
        .hawdb_lightning_initial_import_apply(
            &export.graph_stream.encoded,
            &relational_stream.encoded,
            &manifest,
            None,
            None,
        )
        .unwrap_err();

    assert!(error.to_string().contains("registry table"));
    assert!(target.export_canonical_graph_snapshot().nodes.is_empty());
    assert!(target.store.relational_state().is_empty());
}

#[test]
fn hawdb_lightning_initial_import_apply_with_document_identities_reports_cutover_ready() {
    let mut source = Database::new();
    source
        .query("CREATE (:Memory {id: 'root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let freshness = initial_import_projection_freshness(&export.manifest);
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let document_identities = all_initial_import_document_identities();
    let mut target = Database::new();

    let report = target
        .hawdb_lightning_initial_import_apply_with_document_identities(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            Some(&freshness),
            Some(&checkpoint),
            &document_identities,
        )
        .unwrap();

    assert!(report.applied);
    assert!(report.ready_for_cutover);
    assert!(report
        .plan
        .document_identity_coverage
        .as_ref()
        .is_some_and(|coverage| coverage.ready));
    assert!(report.blocker_codes.is_empty());
}

#[test]
fn hawdb_lightning_initial_import_apply_with_document_identities_blocks_cutover_on_gaps() {
    let mut source = Database::new();
    source
        .query("CREATE (:Memory {id: 'root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let freshness = initial_import_projection_freshness(&export.manifest);
    let checkpoint = test_hawdb_lightning_checkpoint(&export.manifest);
    let document_identities = vec![
        initial_import_document_identity(SearchProjectionKind::Memory, "memory:1"),
        initial_import_document_identity(SearchProjectionKind::Message, "message:1"),
        initial_import_document_identity(SearchProjectionKind::Entity, "entity:1"),
        initial_import_document_identity(SearchProjectionKind::Source, "source:1"),
        initial_import_document_identity(SearchProjectionKind::Community, "community:1"),
    ];
    let mut target = Database::new();

    let report = target
        .hawdb_lightning_initial_import_apply_with_document_identities(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            Some(&freshness),
            Some(&checkpoint),
            &document_identities,
        )
        .unwrap();

    assert!(report.applied);
    assert!(!report.ready_for_cutover);
    assert_eq!(report.node_count, export.manifest.node_count);
    assert!(report
        .plan
        .blocker_codes
        .contains(&"initial_import_document_identity_kind_missing".to_string()));
    assert!(report
        .plan
        .document_identity_coverage
        .as_ref()
        .is_some_and(|coverage| !coverage.ready));
}

#[test]
fn hawdb_lightning_initial_import_apply_rejects_non_empty_target_without_writing() {
    let mut source = Database::new();
    source
        .query("CREATE (:Memory {id: 'root'})-[:LINKS {weight: 7}]->(:Entity {id: 'mid'})")
        .unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let mut target = Database::new();
    target.query("CREATE (:Memory {id: 'existing'})").unwrap();

    let report = target
        .hawdb_lightning_initial_import_apply(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            None,
            None,
        )
        .unwrap();

    assert!(!report.applied);
    assert_eq!(report.node_count, 1);
    assert_eq!(report.relationship_count, 0);
    assert!(report
        .blocker_codes
        .contains(&"hawdb_lightning_initial_import_target_not_empty".to_string()));
    let snapshot = target.export_canonical_graph_snapshot();
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(snapshot.relationships.len(), 0);
}

#[test]
fn hawdb_lightning_initial_import_rejects_non_empty_relational_target() {
    let mut source = Database::new();
    source.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let mut target = Database::new();
    target
        .query_sql("CREATE TABLE public.existing (id TEXT PRIMARY KEY)")
        .unwrap();

    let report = target
        .hawdb_lightning_initial_import_apply(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            None,
            None,
        )
        .unwrap();

    assert!(!report.applied);
    assert_eq!(report.node_count, 0);
    assert_eq!(report.relational_table_count, 1);
    assert!(report
        .blocker_codes
        .contains(&"hawdb_lightning_initial_import_target_not_empty".to_string()));
    assert!(target.export_canonical_graph_snapshot().nodes.is_empty());
    assert!(
        target
            .query_sql(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'existing'"
            )
            .unwrap()
            .rows
            .len()
            == 1
    );
}

#[test]
fn hawdb_lightning_initial_import_rejects_non_empty_graph_schema_target() {
    let mut source = Database::new();
    source.query("CREATE (:Memory {id: 'root'})").unwrap();
    let export = source.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let mut target = Database::new();
    target.query("CREATE NODE TABLE Existing").unwrap();

    let report = target
        .hawdb_lightning_initial_import_apply(
            &export.graph_stream.encoded,
            &export.relational_stream.encoded,
            &export.manifest,
            None,
            None,
        )
        .unwrap();

    assert!(!report.applied);
    assert_eq!(report.node_count, 0);
    assert!(report
        .blocker_codes
        .contains(&"hawdb_lightning_initial_import_target_not_empty".to_string()));
    assert!(target.catalog.label_id("Existing").is_some());
}

#[test]
fn hawdb_lightning_bootstrap_export_background_plan_uses_import_lane() {
    let mut db = Database::new();
    assert!(db
        .hawdb_lightning_bootstrap_export_background_work_plan(BackgroundWorkHint::default())
        .is_none());

    db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
        .unwrap();
    let plan = db
        .hawdb_lightning_bootstrap_export_background_work_plan(BackgroundWorkHint {
            active_topic: true,
            ..BackgroundWorkHint::default()
        })
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Import);
    assert_eq!(plan.request.estimated_operations, 3);
    assert!(plan.hint.active_topic);
}

#[test]
fn hawdb_lightning_bootstrap_export_background_plan_counts_relational_state() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
        .unwrap();

    let plan = db
        .hawdb_lightning_bootstrap_export_background_work_plan(BackgroundWorkHint::default())
        .expect("relational state must produce import work");

    assert_eq!(plan.request.class, WorkClass::Import);
    assert_eq!(plan.request.estimated_operations, 1);
}

#[test]
fn hawdb_lightning_background_bootstrap_export_uses_qos_without_gating_direct_export() {
    let path = unique_test_dir("hawdb_lightning_background_export_qos");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
            .unwrap();
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[WorkClass::Import.as_index()] = Some(0);
        let policy = LocalQosPolicy {
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let error = db
            .prepare_background_hawdb_lightning_bootstrap_export(&policy, &LocalQosState::default())
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("background HawDB Lightning bootstrap export deferred"));
        assert!(!path.join("stable_ids.hawdb").exists());

        let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
        assert_eq!(export.manifest.node_count, 2);
        assert_eq!(export.manifest.relationship_count, 1);
        assert!(path.join("stable_ids.hawdb").exists());
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hawdb_lightning_scheduled_background_bootstrap_export_releases_import_budget() {
    let path = unique_test_dir("hawdb_lightning_scheduled_background_export");
    {
        let mut class_limits = [None; crate::WORK_CLASS_COUNT];
        class_limits[WorkClass::Import.as_index()] = Some(5);
        let policy = LocalQosPolicy {
            max_background_operations: Some(5),
            max_total_background_operations: Some(5),
            max_background_operations_by_class: class_limits,
            ..LocalQosPolicy::default()
        };
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                local_qos_policy: policy,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        db.query("CREATE (:Memory {id: 'root'})-[:LINKS]->(:Entity {id: 'mid'})")
            .unwrap();
        let scheduler = db.local_qos_scheduler();

        let export = db
            .prepare_scheduled_background_hawdb_lightning_bootstrap_export()
            .unwrap();

        assert_eq!(export.manifest.node_count, 2);
        assert_eq!(export.manifest.relationship_count, 1);
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            scheduler.state().running_background_operations_by_class[WorkClass::Import.as_index()],
            0
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn hawdb_lightning_graph_stream_validation_skips_length_coded_metadata() {
    let mut db = Database::new();
    let root = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("root".to_string())),
                (
                    "content".to_string(),
                    Value::String("first line\nrelationship\t999\t1\t2".to_string()),
                ),
                (
                    "metadata".to_string(),
                    Value::Map(BTreeMap::from([(
                        "tags".to_string(),
                        Value::List(vec![
                            Value::String("alpha\nbeta".to_string()),
                            Value::Int(7),
                        ]),
                    )])),
                ),
            ]),
        )
        .unwrap();
    let target = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("target".to_string())),
                ("name".to_string(), Value::String("Target".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            root,
            target,
            "LINKS",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("relationship-id".to_string()),
                ),
                (
                    "note".to_string(),
                    Value::String("edge\nnode\t999".to_string()),
                ),
            ]),
        )
        .unwrap();

    let export = db.prepare_hawdb_lightning_bootstrap_export().unwrap();
    let validation = export
        .graph_stream
        .validate_against_manifest(&export.manifest);

    assert!(validation.is_valid, "{:?}", validation.errors);
    assert_eq!(validation.node_count, 2);
    assert_eq!(validation.relationship_count, 1);
    assert!(validation.endpoint_integrity);
}

#[test]
fn canonical_snapshot_export_matches_wal_and_checkpoint_recovery() {
    let path = unique_test_dir("canonical_snapshot_storage_equivalence");
    let live_snapshot = {
        let mut db = Database::open(&path).unwrap();
        db.query(
                "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS {id: 'edge-root-mid', weight: 7}]->(:Entity {id: 'mid', name: 'Mid'})",
            )
            .unwrap();
        db.query(
                "MATCH (m:Memory {id: 'root'}), (e:Entity {id: 'mid'}) CREATE (m)-[:MENTIONS {id: 'edge-root-mention'}]->(e)",
            )
            .unwrap();
        let snapshot = db.export_canonical_graph_snapshot();
        assert!(snapshot.validate().is_valid);
        snapshot
    };

    {
        let db = Database::open(&path).unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);
    }

    {
        let mut db = Database::open(&path).unwrap();
        db.checkpoint().unwrap();
        let checkpointed = db.export_canonical_graph_snapshot();
        assert_eq!(checkpointed, live_snapshot);
        assert!(checkpointed.validate().is_valid);
    }

    {
        let db = Database::open(&path).unwrap();
        let recovered = db.export_canonical_graph_snapshot();
        assert_eq!(recovered, live_snapshot);
        assert!(recovered.validate().is_valid);
    }
    std::fs::remove_dir_all(path).unwrap();
}
