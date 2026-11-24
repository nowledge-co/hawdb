// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[test]
fn checkpoint_publish_failpoints_recover_one_complete_generation() {
    for (stage, expected_checkpoint_epoch) in [
        (CheckpointPublishStage::CheckpointPersisted, None),
        (CheckpointPublishStage::WalPrepared, None),
        (CheckpointPublishStage::ManifestPublished, Some(1)),
    ] {
        let path = unique_test_dir(&format!("checkpoint_failpoint_{stage:?}"));
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            set_checkpoint_failpoint(Some(stage));
            let error = store.checkpoint(&catalog).unwrap_err();
            set_checkpoint_failpoint(None);
            assert!(error.to_string().contains("injected checkpoint failure"));
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(store.commit_epoch(), 1);
        assert_eq!(
            store.storage_recovery_report().checkpoint_epoch,
            expected_checkpoint_epoch
        );
        let memory = catalog.label_id("Memory").unwrap();
        assert_eq!(store.scan_nodes(Some(memory)).count(), 1);
        match expected_checkpoint_epoch {
            Some(generation) => {
                let durable = store.durable.as_ref().unwrap();
                assert_eq!(
                    durable
                        .relational_row_generation_artifacts
                        .unwrap()
                        .generation,
                    generation
                );
                assert_eq!(
                    durable
                        .relational_overflow_generation_artifacts
                        .unwrap()
                        .generation,
                    generation
                );
                assert!(path
                    .join(
                        hawdb_storage::canonical::canonical_segment_descriptor_page_file(
                            generation
                        )
                    )
                    .exists());
                assert!(path
                    .join(
                        hawdb_storage::canonical::canonical_segment_descriptor_root_file(
                            generation
                        )
                    )
                    .exists());
                assert!(path
                        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                            generation
                        ))
                        .exists());
                assert!(path
                        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                            generation
                        ))
                        .exists());
            }
            None => {
                assert!(!path
                    .join(
                        hawdb_storage::relational::relational_row_page_manifest_generation_file(1)
                    )
                    .exists());
                assert!(!path
                    .join(
                        hawdb_storage::relational::relational_overflow_manifest_generation_file(1)
                    )
                    .exists());
                assert!(!path
                    .join(hawdb_storage::canonical::canonical_segment_descriptor_page_file(1))
                    .exists());
                assert!(!path
                    .join(hawdb_storage::canonical::canonical_segment_descriptor_root_file(1))
                    .exists());
                assert!(!path
                        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(1))
                        .exists());
                assert!(!path
                        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(1))
                        .exists());
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn checkpoint_retirement_source_shares_failed_reclamation_and_retry_with_frontend() {
    let path = unique_test_dir("checkpoint_retirement_shared_debt");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 1..=2 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    let source = store.checkpoint_source();
    let mut candidate = source
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    drop(source);
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(4))]))
        .unwrap();
    let mut final_source = store.checkpoint_source();
    let expected = final_source.checkpoint_source_identity().unwrap();
    candidate.catch_up(&final_source).unwrap();
    candidate.finish_catch_up().unwrap();
    final_source
        .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None)
        .unwrap();
    let retired = store
        .adopt_selected_checkpoint(final_source, expected)
        .unwrap();
    drop(retired);

    // The owner retires files using a source, outside mutable frontend
    // access. A failed deletion must still appear in that frontend's
    // pressure/admission signals, rather than only in this temporary copy.
    let mut retirement_source = store.checkpoint_source();
    set_generation_reclamation_remove_failpoint(Some("checkpoint.1.hawdb".to_string()));
    let result =
        candidate.reclaim_published_generations(&mut retirement_source, &Default::default());
    set_generation_reclamation_remove_failpoint(None);
    result.unwrap();
    let pressure = store.storage_pressure_snapshot(None);
    assert!(pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 1);
    assert!(pressure.generation_reclamation_pending_bytes > 0);
    assert!(path.join("checkpoint.1.hawdb").exists());
    assert_eq!(store.commit_epoch(), 4);
    assert_eq!(store.scan_nodes(None).count(), 4);

    candidate
        .reclaim_published_generations(&mut retirement_source, &Default::default())
        .unwrap();
    let pressure = store.storage_pressure_snapshot(None);
    assert!(!pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 0);
    assert_eq!(pressure.generation_reclamation_pending_bytes, 0);
    assert!(!path.join("checkpoint.1.hawdb").exists());
    drop(candidate);
    drop(retirement_source);
    drop(store);
    let mut recovered_catalog = Catalog::default();
    let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 4);
    assert_eq!(recovered.scan_nodes(None).count(), 4);
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_deferred_reclamation_preserves_debt_and_rejects_other_runtime() {
    let path = unique_test_dir("checkpoint_deferred_debt");
    let other_path = unique_test_dir("checkpoint_deferred_other_runtime");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 1..=2 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    let mut selected = store.checkpoint_source();
    let expected = selected.checkpoint_source_identity().unwrap();
    let mut candidate = selected
        .prepare_checkpoint_candidate(&catalog)
        .unwrap()
        .unwrap();
    candidate.finish_catch_up().unwrap();
    selected
        .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None)
        .unwrap();
    drop(store.adopt_selected_checkpoint(selected, expected).unwrap());
    let mut selected = store.checkpoint_source();

    let stale_file = path.join("checkpoint.1.hawdb");
    let stale_bytes = std::fs::read(&stale_file).unwrap();
    candidate
        .defer_published_generation_reclamation(&mut selected)
        .unwrap();
    let unknown = store.storage_pressure_snapshot(None);
    assert!(unknown.generation_reclamation_retry_required);
    assert_eq!(unknown.generation_reclamation_pending_files, 0);
    assert_eq!(unknown.generation_reclamation_pending_bytes, 0);
    assert_eq!(std::fs::read(&stale_file).unwrap(), stale_bytes);

    set_generation_reclamation_remove_failpoint(Some("checkpoint.1.hawdb".to_string()));
    let result = candidate.reclaim_published_generations(&mut selected, &Default::default());
    set_generation_reclamation_remove_failpoint(None);
    result.unwrap();
    let known = store.storage_pressure_snapshot(None);
    assert!(known.generation_reclamation_retry_required);
    assert_eq!(known.generation_reclamation_pending_files, 1);
    assert_eq!(
        known.generation_reclamation_pending_bytes,
        stale_bytes.len() as u64
    );
    candidate
        .defer_published_generation_reclamation(&mut selected)
        .unwrap();
    let deferred = store.storage_pressure_snapshot(None);
    assert!(deferred.generation_reclamation_retry_required);
    assert_eq!(deferred.generation_reclamation_pending_files, 1);
    assert_eq!(
        deferred.generation_reclamation_pending_bytes,
        known.generation_reclamation_pending_bytes
    );
    assert_eq!(std::fs::read(&stale_file).unwrap(), stale_bytes);

    let mut other_catalog = Catalog::default();
    let mut other = GraphStore::open(&other_path, &mut other_catalog).unwrap();
    assert!(candidate
        .defer_published_generation_reclamation(&mut other)
        .is_err());
    assert!(
        !other
            .storage_pressure_snapshot(None)
            .generation_reclamation_retry_required
    );
    assert!(candidate
        .reclaim_published_generations(&mut other, &Default::default())
        .is_err());
    assert_eq!(std::fs::read(&stale_file).unwrap(), stale_bytes);

    candidate
        .reclaim_published_generations(&mut selected, &Default::default())
        .unwrap();
    let cleared = store.storage_pressure_snapshot(None);
    assert!(!cleared.generation_reclamation_retry_required);
    assert_eq!(cleared.generation_reclamation_pending_files, 0);
    assert_eq!(cleared.generation_reclamation_pending_bytes, 0);
    assert!(!stale_file.exists());
    drop(candidate);
    drop(selected);
    drop(store);
    drop(other);
    let reopened = GraphStore::open(&path, &mut catalog).unwrap();
    assert_eq!(reopened.commit_epoch(), 3);
    assert_eq!(reopened.scan_nodes(None).count(), 3);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
    std::fs::remove_dir_all(other_path).unwrap();
}

#[test]
fn checkpoint_reclamation_failure_is_reported_and_retried_after_publication() {
    let path = unique_test_dir("checkpoint_reclamation_retry");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 1..=2 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
        .unwrap();
    set_generation_reclamation_remove_failpoint(Some("checkpoint.1.hawdb".to_string()));
    let checkpoint_result = store.checkpoint(&catalog);
    set_generation_reclamation_remove_failpoint(None);
    checkpoint_result.unwrap();

    assert_eq!(store.commit_epoch(), 3);
    assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 3);
    assert_eq!(store.scan_nodes(None).count(), 3);
    assert!(path.join("checkpoint.1.hawdb").exists());
    let pressure = store.storage_pressure_snapshot(None);
    assert!(pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 1);
    assert!(pressure.generation_reclamation_pending_bytes > 0);
    assert!(pressure
        .reason_codes
        .contains(&hawdb_storage::pressure::StoragePressureReasonCode::GenerationReclamationDebt));

    drop(store);
    let mut recovered_catalog = Catalog::default();
    let mut recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
    assert_eq!(recovered.commit_epoch(), 3);
    assert_eq!(
        recovered.storage_recovery_report().checkpoint_epoch,
        Some(3)
    );
    assert_eq!(recovered.scan_nodes(None).count(), 3);

    recovered.checkpoint(&recovered_catalog).unwrap();
    assert!(!path.join("checkpoint.1.hawdb").exists());
    let pressure = recovered.storage_pressure_snapshot(None);
    assert!(!pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 0);
    assert_eq!(pressure.generation_reclamation_pending_bytes, 0);
    assert!(!pressure
        .reason_codes
        .contains(&hawdb_storage::pressure::StoragePressureReasonCode::GenerationReclamationDebt));

    drop(recovered);
    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
    assert_eq!(reopened.commit_epoch(), 3);
    assert_eq!(reopened.storage_recovery_report().checkpoint_epoch, Some(4));
    assert_eq!(reopened.scan_nodes(None).count(), 3);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_retains_one_previous_generation_before_reclaim() {
    let path = unique_test_dir("checkpoint_generation_reclaim");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 1..=3 {
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }

    assert!(!path.join("checkpoint.1.hawdb").exists());
    assert!(!path.join("wal.1.hawdb").exists());
    assert!(!path.join("canonical.1.hawdb").exists());
    assert!(!path.join("canonical.1.manifest.hawdb").exists());
    assert!(!path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_page_file(1))
        .exists());
    assert!(!path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_root_file(1))
        .exists());
    assert!(!path.join("adjacency.1.hawdb").exists());
    assert!(!path.join("adjacency.1.manifest.hawdb").exists());
    assert!(!path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(1))
        .exists());
    assert!(!path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(1))
        .exists());
    assert!(!path.join("properties.1.hawdb").exists());
    assert!(!path.join("properties.1.manifest.hawdb").exists());
    assert!(!path.join("property-index.1.hawdb").exists());
    assert!(!path.join("property-index.1.manifest.hawdb").exists());
    assert!(!path
        .join(hawdb_storage::relational::relational_row_page_manifest_generation_file(1))
        .exists());
    assert!(!path
        .join(hawdb_storage::relational::relational_overflow_manifest_generation_file(1))
        .exists());
    assert!(path.join("checkpoint.2.hawdb").exists());
    assert!(path.join("wal.2.hawdb").exists());
    assert!(path.join("canonical.2.hawdb").exists());
    assert!(path.join("canonical.2.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_page_file(2))
        .exists());
    assert!(path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_root_file(2))
        .exists());
    assert!(path.join("adjacency.2.hawdb").exists());
    assert!(!path.join("adjacency.2.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(2))
        .exists());
    assert!(path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(2))
        .exists());
    assert!(path.join("properties.2.hawdb").exists());
    assert!(path.join("properties.2.manifest.hawdb").exists());
    assert!(path.join("property-index.2.hawdb").exists());
    assert!(path.join("property-index.2.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::relational::relational_row_page_manifest_generation_file(2))
        .exists());
    assert!(path
        .join(hawdb_storage::relational::relational_overflow_manifest_generation_file(2))
        .exists());
    assert!(path.join("checkpoint.3.hawdb").exists());
    assert!(path.join("wal.3.hawdb").exists());
    assert!(path.join("canonical.3.hawdb").exists());
    assert!(path.join("canonical.3.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_page_file(3))
        .exists());
    assert!(path
        .join(hawdb_storage::canonical::canonical_segment_descriptor_root_file(3))
        .exists());
    assert!(path.join("adjacency.3.hawdb").exists());
    assert!(!path.join("adjacency.3.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(3))
        .exists());
    assert!(path
        .join(hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(3))
        .exists());
    assert!(path.join("properties.3.hawdb").exists());
    assert!(path.join("properties.3.manifest.hawdb").exists());
    assert!(path.join("property-index.3.hawdb").exists());
    assert!(path.join("property-index.3.manifest.hawdb").exists());
    assert!(path
        .join(hawdb_storage::relational::relational_row_page_manifest_generation_file(3))
        .exists());
    assert!(path
        .join(hawdb_storage::relational::relational_overflow_manifest_generation_file(3))
        .exists());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn reopen_after_checkpoint_uses_manifest_lsn_without_overwriting_wal() {
    let path = unique_test_dir("manifest_reopen_lsn");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
    }

    let wal = read_test_wal(&path).unwrap();
    assert!(wal.starts_with("2\tcreate_node\t"));
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
    assert_eq!(nodes.len(), 2);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_corrupt_manifest() {
    let path = unique_test_dir("corrupt_manifest");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let manifest_path = path.join("manifest.hawdb");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    std::fs::write(
        &manifest_path,
        manifest.replace("checkpoint_epoch\t1\n", "checkpoint_epoch\t2\n"),
    )
    .unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error.to_string().contains("manifest checksum mismatch"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_unsupported_manifest_storage_version() {
    let path = unique_test_dir("manifest_storage_version");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    rewrite_checksummed_file(
        &path.join("manifest.hawdb"),
        "version\thawdb-storage-v2\n",
        "version\thawdb-storage-v0\n",
        "manifest",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(hawdb_core::StorageFormatError::UnsupportedVersion {
            format: hawdb_core::STORAGE_MANIFEST_FORMAT,
            found: 0,
            supported: 2,
        })
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn single_storage_format_rejects_unpublished_manifest() {
    let path = unique_test_dir("unpublished_manifest_format");
    {
        let mut catalog = Catalog::default();
        GraphStore::open(&path, &mut catalog).unwrap();
    }
    rewrite_checksummed_file(
        &path.join("manifest.hawdb"),
        "HAWDB_MANIFEST_V1\n",
        "INVALID_MANIFEST_HEADER\n",
        "manifest",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(
        error.to_string().contains("missing the V1 format header"),
        "{error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn single_storage_format_rejects_incomplete_v1_manifest() {
    let path = unique_test_dir("incomplete_v1_manifest");
    {
        let mut catalog = Catalog::default();
        GraphStore::open(&path, &mut catalog).unwrap();
    }
    rewrite_checksummed_file(
        &path.join("manifest.hawdb"),
        "checkpoint_generation\tnone\n",
        "",
        "manifest",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error
        .to_string()
        .contains("manifest is missing required field: checkpoint_generation"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn storage_owned_manifest_preserves_checkpoint_binding_and_no_write_rejection() {
    let path = unique_test_dir("storage_owned_manifest");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let manifest_path = path.join("manifest.hawdb");
    let manifest: super::DurableManifest =
        hawdb_storage::durable_manifest::DurableManifest::load(&manifest_path).unwrap();
    let manifest: hawdb_storage::durable_manifest::DurableManifest = manifest;
    assert_eq!(manifest.checkpoint_generation, Some(1));
    assert!(manifest.relational_row_generation_artifacts.is_some());
    assert!(manifest.relational_overflow_generation_artifacts.is_some());
    assert_eq!(manifest.wal_path(&path), path.join("wal.1.hawdb"));
    rewrite_checksummed_file(
        &manifest_path,
        "wal_generation\t1\n",
        "wal_generation\t1\nwal_generation\t1\n",
        "manifest",
    );
    let before = fs::read(&manifest_path).unwrap();
    let expected = super::DurableManifest::load(&manifest_path)
        .unwrap_err()
        .to_string();
    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(matches!(&error, HawDBError::Storage(_)));
    assert!(error
        .to_string()
        .contains("duplicate field: wal_generation"));
    assert_eq!(error.to_string(), expected);
    assert_eq!(fs::read(&manifest_path).unwrap(), before);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn single_storage_format_rejects_manifestless_artifacts() {
    let path = unique_test_dir("artifacts_without_manifest");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("checkpoint.hawdb"), b"unpublished format").unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error
        .to_string()
        .contains("storage artifacts but no durable manifest"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_unsupported_checkpoint_storage_version() {
    let path = unique_test_dir("checkpoint_storage_version");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    rewrite_checksummed_file(
        &active_checkpoint_path(&path),
        "version\thawdb-storage-v1\n",
        "version\thawdb-storage-v0\n",
        "checkpoint",
    );

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(hawdb_core::StorageFormatError::UnsupportedVersion {
            format: hawdb_core::STORAGE_MANIFEST_FORMAT,
            found: 0,
            supported: 1,
        })
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn replays_property_index_from_wal() {
    let path = unique_test_dir("property_index_wal");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "id")
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::Int(1)),
                    ("title", Value::String("Graph foundations".to_string())),
                ]),
            )
            .unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store
            .seek_nodes_by_property(label, "id", &Value::Int(1))
            .collect::<Vec<_>>();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].properties.get("title"),
            Some(&Value::String("Graph foundations".to_string()))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rebuilds_property_index_from_checkpoint() {
    let path = unique_test_dir("property_index_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::Int(1)),
                    ("title", Value::String("Graph foundations".to_string())),
                ]),
            )
            .unwrap();
        // Declared after the write, so the checkpoint has to carry the
        // backfilled content and not just the descriptor.
        store
            .create_property_index(&mut catalog, "Memory", "title")
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store
            .seek_nodes_by_property(
                label,
                "title",
                &Value::String("Graph foundations".to_string()),
            )
            .collect::<Vec<_>>();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].properties.get("id"), Some(&Value::Int(1)));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn scan_pruning_uses_property_eq_index_for_unique_key() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:1".to_string())),
                ("title", Value::String("Graph foundations".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:2".to_string())),
                ("title", Value::String("Storage notes".to_string())),
            ]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "stable_id")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::Eq {
            property: "stable_id".to_string(),
            value: Value::String("memory:2".to_string()),
        }),
    );

    assert_eq!(scan.nodes.len(), 1);
    assert_eq!(
        scan.nodes[0].properties.get("title"),
        Some(&Value::String("Storage notes".to_string()))
    );
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyEq {
            property: "stable_id".to_string()
        }
    );
    assert_eq!(scan.report.target_kind, ScanPruningTargetKind::Node);
    assert_eq!(scan.report.label_id, Some(label));
    assert_eq!(scan.report.rel_type_id, None);
    assert!(scan.report.pruned);
    assert_eq!(scan.report.candidate_count_before_filter, 1);
    assert_eq!(scan.report.candidate_count_before_pruning, 2);
    assert_eq!(scan.report.pruned_candidate_count, 1);
    assert_eq!(scan.report.filtered_out_count, 0);
}

#[test]
fn scan_pruning_treats_in_values_as_enum_set() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for state in ["active", "deleted", "forgotten"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("state", Value::String(state.to_string()))]),
            )
            .unwrap();
    }

    store
        .create_property_index(&mut catalog, "Memory", "state")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::In {
            property: "state".to_string(),
            values: vec![
                Value::String("deleted".to_string()),
                Value::String("forgotten".to_string()),
            ],
        }),
    );
    let states = scan
        .nodes
        .iter()
        .map(|node| node.properties.get("state").unwrap().clone())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        states,
        BTreeSet::from([
            Value::String("deleted".to_string()),
            Value::String("forgotten".to_string()),
        ])
    );
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyIn {
            property: "state".to_string()
        }
    );
    assert_eq!(scan.report.candidate_count_before_filter, 2);
    assert_eq!(scan.report.candidate_count_before_pruning, 3);
    assert_eq!(scan.report.pruned_candidate_count, 1);
}

#[test]
fn scan_pruning_uses_property_exists_for_not_null_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("confidence", Value::Float(0.9))]),
        )
        .unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("confidence", Value::Null)]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "confidence")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::IsNotNull {
            property: "confidence".to_string(),
        }),
    );

    assert_eq!(scan.nodes.len(), 1);
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyExists {
            property: "confidence".to_string()
        }
    );
    assert!(scan.report.pruned);
    assert!(!scan.report.exact_empty);
    assert_eq!(scan.report.candidate_count_before_filter, 1);
    assert_eq!(scan.report.candidate_count_before_pruning, 3);
    assert_eq!(scan.report.pruned_candidate_count, 2);
    assert_eq!(scan.report.filtered_out_count, 0);
}

#[test]
fn scan_pruning_uses_property_missing_or_null_for_null_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("latest_at", Value::Int(10))]),
        )
        .unwrap();
    store
        .create_node(&mut catalog, "Memory", properties([]))
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("latest_at", Value::Null)]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "latest_at")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::IsNull {
            property: "latest_at".to_string(),
        }),
    );

    assert_eq!(scan.nodes.len(), 2);
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyMissingOrNull {
            property: "latest_at".to_string()
        }
    );
    assert!(scan.report.pruned);
    assert!(!scan.report.exact_empty);
    assert_eq!(scan.report.candidate_count_before_filter, 2);
    assert_eq!(scan.report.candidate_count_before_pruning, 3);
    assert_eq!(scan.report.pruned_candidate_count, 1);
    assert_eq!(scan.report.filtered_out_count, 0);
}

#[test]
fn scan_pruning_uses_default_if_null_eq_for_normalized_default_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(&mut catalog, "Thread", properties([]))
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String(String::new()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String("default".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String("team".to_string()))]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Thread", "space_id")
        .unwrap();
    let label = catalog.label_id("Thread").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::DefaultIfNullOrEq {
            property: "space_id".to_string(),
            empty: Value::String(String::new()),
            default: Value::String("default".to_string()),
            value: Value::String("default".to_string()),
            negated: false,
        }),
    );

    assert_eq!(scan.nodes.len(), 3);
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyDefaultIfNullEq {
            property: "space_id".to_string()
        }
    );
    assert!(scan.report.pruned);
    assert_eq!(scan.report.candidate_count_before_filter, 3);
    assert_eq!(scan.report.candidate_count_before_pruning, 4);
    assert_eq!(scan.report.pruned_candidate_count, 1);
    assert_eq!(scan.report.filtered_out_count, 0);
}

#[test]
fn scan_pruning_uses_default_if_null_not_eq_for_normalized_default_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(&mut catalog, "Thread", properties([]))
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String(String::new()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String("default".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Thread",
            properties([("space_id", Value::String("team".to_string()))]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Thread", "space_id")
        .unwrap();
    let label = catalog.label_id("Thread").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::DefaultIfNullOrEq {
            property: "space_id".to_string(),
            empty: Value::String(String::new()),
            default: Value::String("default".to_string()),
            value: Value::String("default".to_string()),
            negated: true,
        }),
    );

    assert_eq!(scan.nodes.len(), 1);
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyDefaultIfNullNotEq {
            property: "space_id".to_string()
        }
    );
    assert!(scan.report.pruned);
    assert_eq!(scan.report.candidate_count_before_filter, 1);
    assert_eq!(scan.report.candidate_count_before_pruning, 4);
    assert_eq!(scan.report.pruned_candidate_count, 3);
    assert_eq!(scan.report.filtered_out_count, 0);
}

#[test]
fn scan_pruning_uses_smallest_candidate_in_and_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:1".to_string())),
                ("state", Value::String("active".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:2".to_string())),
                ("state", Value::String("active".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:3".to_string())),
                ("state", Value::String("forgotten".to_string())),
            ]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "state")
        .unwrap();
    store
        .create_property_index(&mut catalog, "Memory", "stable_id")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::And(vec![
            PropertyFilter::In {
                property: "state".to_string(),
                values: vec![
                    Value::String("active".to_string()),
                    Value::String("forgotten".to_string()),
                ],
            },
            PropertyFilter::Eq {
                property: "stable_id".to_string(),
                value: Value::String("memory:2".to_string()),
            },
        ])),
    );

    assert_eq!(scan.nodes.len(), 1);
    assert_eq!(
        scan.report.strategy,
        ScanPruningStrategy::PropertyEq {
            property: "stable_id".to_string()
        }
    );
    assert_eq!(scan.report.candidate_count_before_filter, 1);
    assert_eq!(scan.report.candidate_count_before_pruning, 3);
    assert_eq!(scan.report.pruned_candidate_count, 2);
}

#[test]
fn scan_pruning_unions_prunable_or_branches() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for state in ["active", "deleted", "forgotten"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("state", Value::String(state.to_string()))]),
            )
            .unwrap();
    }

    store
        .create_property_index(&mut catalog, "Memory", "state")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::Or(vec![
            PropertyFilter::Eq {
                property: "state".to_string(),
                value: Value::String("active".to_string()),
            },
            PropertyFilter::Eq {
                property: "state".to_string(),
                value: Value::String("forgotten".to_string()),
            },
        ])),
    );

    assert_eq!(scan.nodes.len(), 2);
    assert_eq!(scan.report.strategy, ScanPruningStrategy::OrUnion);
    assert_eq!(scan.report.candidate_count_before_filter, 2);
    assert_eq!(scan.report.candidate_count_before_pruning, 3);
    assert_eq!(scan.report.pruned_candidate_count, 1);
    assert!(scan.report.pruned);
}

#[test]
fn scan_pruning_reports_exact_empty_for_empty_in_filter() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("state", Value::String("active".to_string()))]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "state")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::In {
            property: "state".to_string(),
            values: vec![],
        }),
    );

    assert!(scan.nodes.is_empty());
    assert_eq!(scan.report.strategy, ScanPruningStrategy::Empty);
    assert!(scan.report.exact_empty);
    assert_eq!(scan.report.candidate_count_before_filter, 0);
    assert_eq!(scan.report.candidate_count_before_pruning, 1);
    assert_eq!(scan.report.pruned_candidate_count, 1);
}

#[test]
fn scan_pruning_uses_property_range_for_numeric_and_iso_date_strings() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("importance", Value::Float(0.2)),
                ("updated_at", Value::String("2026-07-01".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("importance", Value::Float(0.7)),
                ("updated_at", Value::String("2026-07-15".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("importance", Value::Float(0.9)),
                ("updated_at", Value::String("2026-08-01".to_string())),
            ]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "importance")
        .unwrap();
    store
        .create_property_index(&mut catalog, "Memory", "updated_at")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let numeric_scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::Range {
            property: "importance".to_string(),
            lower: Some((Value::Float(0.5), true)),
            upper: Some((Value::Float(0.8), true)),
        }),
    );
    assert_eq!(numeric_scan.nodes.len(), 1);
    assert_eq!(
        numeric_scan.report.strategy,
        ScanPruningStrategy::PropertyRange {
            property: "importance".to_string()
        }
    );
    assert_eq!(numeric_scan.report.candidate_count_before_filter, 1);
    assert_eq!(numeric_scan.report.candidate_count_before_pruning, 3);
    assert_eq!(numeric_scan.report.pruned_candidate_count, 2);

    let date_scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::Range {
            property: "updated_at".to_string(),
            lower: Some((Value::String("2026-07-01".to_string()), true)),
            upper: Some((Value::String("2026-07-31".to_string()), true)),
        }),
    );
    assert_eq!(date_scan.nodes.len(), 2);
    assert_eq!(
        date_scan.report.strategy,
        ScanPruningStrategy::PropertyRange {
            property: "updated_at".to_string()
        }
    );
    assert_eq!(date_scan.report.candidate_count_before_filter, 2);
    assert_eq!(date_scan.report.candidate_count_before_pruning, 3);
    assert_eq!(date_scan.report.pruned_candidate_count, 1);
}

#[test]
fn relationship_scan_pruning_uses_property_equality_and_in_list() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    for state in ["active", "deleted", "archived"] {
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "RELATES_TO",
                properties([("lifecycle_state", Value::String(state.to_string()))]),
            )
            .unwrap();
    }
    let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();

    let eq_scan = store.scan_relationships_with_filter_pruning(
        Some(rel_type),
        Some(&PropertyFilter::Eq {
            property: "lifecycle_state".to_string(),
            value: Value::String("active".to_string()),
        }),
    );
    assert_eq!(eq_scan.relationships.len(), 1);
    assert_eq!(
        eq_scan.report.strategy,
        ScanPruningStrategy::PropertyEq {
            property: "lifecycle_state".to_string()
        }
    );
    assert_eq!(
        eq_scan.report.target_kind,
        ScanPruningTargetKind::Relationship
    );
    assert_eq!(eq_scan.report.label_id, None);
    assert_eq!(eq_scan.report.rel_type_id, Some(rel_type));
    assert_eq!(eq_scan.report.candidate_count_before_pruning, 3);
    assert_eq!(eq_scan.report.candidate_count_before_filter, 1);
    assert_eq!(eq_scan.report.pruned_candidate_count, 2);

    let in_scan = store.scan_relationships_with_filter_pruning(
        Some(rel_type),
        Some(&PropertyFilter::In {
            property: "lifecycle_state".to_string(),
            values: vec![
                Value::String("active".to_string()),
                Value::String("archived".to_string()),
            ],
        }),
    );
    assert_eq!(in_scan.relationships.len(), 2);
    assert_eq!(
        in_scan.report.strategy,
        ScanPruningStrategy::PropertyIn {
            property: "lifecycle_state".to_string()
        }
    );
    assert_eq!(in_scan.report.candidate_count_before_filter, 2);
    assert_eq!(in_scan.report.pruned_candidate_count, 1);
}

#[test]
fn relationship_scan_pruning_uses_property_range_and_tracks_updates() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    let old = store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.2))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.7))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.9))]),
        )
        .unwrap();
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

    let range_scan = store.scan_relationships_with_filter_pruning(
        Some(rel_type),
        Some(&PropertyFilter::Range {
            property: "confidence".to_string(),
            lower: Some((Value::Float(0.5), true)),
            upper: Some((Value::Float(0.8), true)),
        }),
    );
    assert_eq!(range_scan.relationships.len(), 1);
    assert_eq!(
        range_scan.report.strategy,
        ScanPruningStrategy::PropertyRange {
            property: "confidence".to_string()
        }
    );
    assert_eq!(range_scan.report.candidate_count_before_pruning, 3);
    assert_eq!(range_scan.report.candidate_count_before_filter, 1);
    assert_eq!(range_scan.report.pruned_candidate_count, 2);

    store.apply_set_relationship_property(old, "confidence".to_string(), Value::Float(0.75));
    let updated_scan = store.scan_relationships_with_filter_pruning(
        Some(rel_type),
        Some(&PropertyFilter::Range {
            property: "confidence".to_string(),
            lower: Some((Value::Float(0.5), true)),
            upper: Some((Value::Float(0.8), true)),
        }),
    );
    assert_eq!(updated_scan.relationships.len(), 2);
    assert_eq!(updated_scan.report.candidate_count_before_filter, 2);

    store.apply_delete_relationship(old);
    let deleted_scan = store.scan_relationships_with_filter_pruning(
        Some(rel_type),
        Some(&PropertyFilter::Eq {
            property: "confidence".to_string(),
            value: Value::Float(0.75),
        }),
    );
    assert!(deleted_scan.relationships.is_empty());
    assert_eq!(deleted_scan.report.candidate_count_before_filter, 0);
    assert_eq!(deleted_scan.report.pruned_candidate_count, 2);
}

#[test]
fn scan_pruning_falls_back_for_unindexed_string_contains() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("title", Value::String("Graph foundations".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("title", Value::String("Storage notes".to_string()))]),
        )
        .unwrap();

    store
        .create_property_index(&mut catalog, "Memory", "title")
        .unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let scan = store.scan_nodes_with_filter_pruning(
        &catalog,
        Some(label),
        Some(&PropertyFilter::Contains {
            property: "title".to_string(),
            value: "Graph".to_string(),
        }),
    );

    assert_eq!(scan.nodes.len(), 1);
    assert_eq!(scan.report.strategy, ScanPruningStrategy::FullLabelScan);
    assert!(!scan.report.pruned);
    assert_eq!(scan.report.candidate_count_before_filter, 2);
    assert_eq!(scan.report.candidate_count_before_pruning, 2);
    assert_eq!(scan.report.pruned_candidate_count, 0);
    assert_eq!(scan.report.filtered_out_count, 1);
}

#[test]
fn list_values_round_trip_through_checkpoint() {
    let path = unique_test_dir("list_value_checkpoint");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([(
                    "tags",
                    Value::List(vec![
                        Value::String("graph".to_string()),
                        Value::String("storage".to_string()),
                    ]),
                )]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let node = store.scan_nodes(None).next().unwrap();
        assert_eq!(
            node.properties.get("tags"),
            Some(&Value::List(vec![
                Value::String("graph".to_string()),
                Value::String("storage".to_string()),
            ]))
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn loads_projected_graph_artifact_from_checkpoint() {
    let path = unique_test_dir("projected_graph_artifact_cache");
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".to_string()],
        rel_types: vec!["LINKS".to_string()],
        relationship_predicates: BTreeMap::from([(
            "LINKS".to_string(),
            ProjectedRelationshipPredicate::Gte {
                property: "confidence".to_string(),
                value: Value::Float(0.7),
            },
        )]),
    };
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        let excluded = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "LINKS",
                properties([("confidence", Value::Float(0.7))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                excluded,
                "LINKS",
                properties([("confidence", Value::Float(0.69))]),
            )
            .unwrap();
        store
            .register_projected_graph("MemoryGraph", definition.clone())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    let artifact_bytes = std::fs::read(path.join("projected_graphs.hawdb")).unwrap();
    assert!(artifact_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let artifact = store
            .projected_graph_artifact("MemoryGraph", &definition)
            .unwrap();
        assert_eq!(artifact.node_count(), 3);
        assert_eq!(artifact.edge_count(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ignores_projected_graph_artifact_after_wal_replay_advances_epoch() {
    let path = unique_test_dir("projected_graph_artifact_stale");
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".to_string()],
        rel_types: vec!["LINKS".to_string()],
        relationship_predicates: BTreeMap::new(),
    };
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
            .unwrap();
        store
            .register_projected_graph("MemoryGraph", definition.clone())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
            .unwrap();
    }
    {
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(store
            .projected_graph_artifact("MemoryGraph", &definition)
            .is_none());
        let status = store
            .projected_graph_statuses()
            .into_iter()
            .find(|status| status.name == "MemoryGraph")
            .unwrap();
        assert!(!status.reusable);
        assert_eq!(status.projection_epoch, None);
        assert_eq!(status.commit_epoch, None);
        assert_eq!(status.node_count, None);
        assert_eq!(status.edge_count, None);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn doctor_repairs_torn_wal_tail_after_strict_open_rejects_it() {
    let path = unique_test_dir("torn_wal");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(active_wal_path(&path))
        .unwrap()
        .write_all(b"torn-entry-without-checksum")
        .unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_recovery(
        &path,
        &mut catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        crate::config::RecoveryMode::Strict,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("strict WAL recovery rejected torn tail"));
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    assert_eq!(
        plan.discarded_wal_tail_bytes,
        b"torn-entry-without-checksum".len() as u64
    );
    DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    let label = catalog.label_id("Memory").unwrap();
    let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
    assert_eq!(nodes.len(), 1);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn doctor_discards_torn_batch_wal_without_partial_path_recovery() {
    let path = unique_test_dir("torn_batch_wal");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_connected_nodes(
                &mut catalog,
                ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([("id", Value::Int(1))]),
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    target_label: "Entity".to_string(),
                    target_properties: properties([("id", Value::Int(10))]),
                },
            )
            .unwrap();
    }
    let wal_path = active_wal_path(&path);
    truncate_test_wal_tail(&wal_path, 8);

    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    assert!(store.scan_nodes(None).next().is_none());
    assert!(catalog.rel_type_id("MENTIONS").is_none());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_and_quarantines_checksum_corruption_before_valid_wal_suffix() {
    let path = unique_test_dir("wal_middle_corruption");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
    }
    let wal_path = active_wal_path(&path);
    // Flip one payload byte of the first record: mid-log corruption
    // ahead of a valid suffix must fail closed.
    let mut wal = std::fs::read(&wal_path).unwrap();
    let first_payload_offset =
        super::WAL_BINARY_FILE_HEADER_BYTES + super::wal_codec::frame::WAL_FRAGMENT_HEADER_BYTES;
    wal[first_payload_offset] ^= 0xff;
    std::fs::write(&wal_path, &wal).unwrap();

    for _ in 0..3 {
        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("WAL corruption at byte offset"));
        assert!(error.to_string().contains("checksum mismatch"));
    }
    assert_eq!(
        std::fs::read_dir(path.join("quarantine")).unwrap().count(),
        1
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_and_quarantines_checksum_corruption_at_wal_tail() {
    let path = unique_test_dir("wal_tail_checksum_corruption");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
    }
    let wal_path = active_wal_path(&path);
    // Flip the stored checksum of the final complete fragment chain:
    // a complete chain failing its checksum is corruption, never a
    // repairable torn tail.
    let mut corrupt_wal = std::fs::read(&wal_path).unwrap();
    corrupt_wal[super::WAL_BINARY_FILE_HEADER_BYTES] ^= 0xff;
    std::fs::write(&wal_path, &corrupt_wal).unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error.to_string().contains("WAL corruption at byte offset"));
    assert!(error.to_string().contains("checksum mismatch"));
    assert_eq!(std::fs::read(&wal_path).unwrap(), corrupt_wal);
    assert_eq!(
        std::fs::read_dir(path.join("quarantine")).unwrap().count(),
        1
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn corrupt_wal_quarantine_respects_the_configured_byte_bound() {
    let path = unique_test_dir("wal_quarantine_byte_bound");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
    }
    let wal_path = active_wal_path(&path);
    let valid_wal = std::fs::read(&wal_path).unwrap();
    let wal_len = valid_wal.len() as u64;
    let max_quarantine_bytes = wal_len.saturating_mul(2);
    let payload_offset =
        super::WAL_BINARY_FILE_HEADER_BYTES + super::wal_codec::frame::WAL_FRAGMENT_HEADER_BYTES;

    for variant in 0..3 {
        let mut corrupt_wal = valid_wal.clone();
        corrupt_wal[payload_offset + variant] ^= 0xff;
        std::fs::write(&wal_path, corrupt_wal).unwrap();
        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                max_quarantine_bytes,
                ..WalReplayConfig::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));

        let entries = std::fs::read_dir(path.join("quarantine"))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let retained_bytes = entries.iter().fold(0u64, |total, entry| {
            total.saturating_add(entry.metadata().unwrap().len())
        });
        assert!(retained_bytes <= max_quarantine_bytes);
        assert!(entries.len() <= 2);
    }

    assert_eq!(
        std::fs::read_dir(path.join("quarantine")).unwrap().count(),
        2
    );
    let corrupt_wal = std::fs::read(&wal_path).unwrap();
    let mut catalog = Catalog::default();
    let error = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            max_quarantine_bytes: 0,
            ..WalReplayConfig::default()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"));
    assert_eq!(std::fs::read(&wal_path).unwrap(), corrupt_wal);
    assert_eq!(
        std::fs::read_dir(path.join("quarantine")).unwrap().count(),
        0
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejects_and_quarantines_non_contiguous_wal_lsn() {
    let path = unique_test_dir("wal_lsn_gap");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
    }
    let wal_path = active_wal_path(&path);
    // Rebuild the WAL with well-formed framing but a gap in the LSN
    // sequence: the second record claims LSN 3 instead of 2.
    let mut cursor = match super::WalRecordCursor::open(&wal_path, None).unwrap() {
        super::WalOpenOutcome::Cursor(cursor) => cursor,
        _ => panic!("test WAL is missing its header"),
    };
    let generation = cursor.generation();
    let start_lsn = cursor.start_lsn();
    let mut entries = Vec::new();
    loop {
        match cursor.next().unwrap() {
            super::WalCursorEvent::Entry { entry, .. } => entries.push(entry),
            super::WalCursorEvent::Eof => break,
            _ => panic!("test WAL is damaged"),
        }
    }
    entries.last_mut().unwrap().lsn += 1;
    let mut rewritten = super::encode_binary_wal_header(generation, start_lsn);
    for (index, entry) in entries.iter().enumerate() {
        let payload = super::encode_binary_wal_record(entry, index as u64 + 1).unwrap();
        let position = rewritten.len() as u64 - super::WAL_BINARY_FILE_HEADER_BYTES as u64;
        rewritten.extend_from_slice(&super::frame_binary_wal_record(
            generation, &payload, position,
        ));
    }
    std::fs::write(&wal_path, rewritten).unwrap();

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error.to_string().contains("WAL LSN sequence mismatch"));
    assert_eq!(
        std::fs::read_dir(path.join("quarantine")).unwrap().count(),
        1
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mutation_rejects_over_depth_value_before_wal_append() {
    fn value_at_depth(depth: usize) -> Value {
        assert!(depth > 0);
        (1..depth).fold(Value::Null, |value, _| Value::List(vec![value]))
    }

    let path = unique_test_dir("wal_value_depth_admission");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let accepted_id = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("payload", value_at_depth(32))]),
        )
        .unwrap();
    let commit_epoch = store.commit_epoch();
    let wal_path = active_wal_path(&path);
    let wal_len = std::fs::metadata(&wal_path).unwrap().len();

    let error = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("payload", value_at_depth(33))]),
        )
        .unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)));
    assert!(error.to_string().contains("nesting exceeds 32"));
    assert_eq!(store.commit_epoch(), commit_epoch);
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len);

    store.checkpoint(&catalog).unwrap();
    drop(store);
    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
    assert!(reopened.node_owned(accepted_id).unwrap().is_some());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn manifest_fails_closed_for_missing_canonical_artifacts() {
    for missing in ["checkpoint", "wal", "manifest"] {
        let path = unique_test_dir(&format!("missing_{missing}"));
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        match missing {
            "checkpoint" => std::fs::remove_file(active_checkpoint_path(&path)).unwrap(),
            "wal" => std::fs::remove_file(active_wal_path(&path)).unwrap(),
            "manifest" => std::fs::remove_file(path.join("manifest.hawdb")).unwrap(),
            _ => unreachable!(),
        }

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(
            error.to_string().contains("missing")
                || error.to_string().contains("no durable manifest")
        );
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn doctor_recovers_complete_mem_shaped_batches_before_torn_tail() {
    let path = unique_test_dir("mem_shaped_batch_recovery");
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let old_memory = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("mem-old".to_string())),
                    ("is_latest", Value::Bool(true)),
                    ("updated_at", Value::Int(10)),
                ]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        store
            .set_node_properties_by_ids(
                &mut catalog,
                &[old_memory],
                &[
                    NodeSetAssignment {
                        property: "is_latest".to_string(),
                        value: NodeSetValue::Value(Value::Bool(false)),
                    },
                    NodeSetAssignment {
                        property: "updated_at".to_string(),
                        value: NodeSetValue::Value(Value::Int(20)),
                    },
                ],
            )
            .unwrap();
        store
            .create_connected_nodes(
                &mut catalog,
                ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([
                        ("id", Value::String("mem-new".to_string())),
                        ("is_latest", Value::Bool(true)),
                    ]),
                    rel_type: "EVOLVES".to_string(),
                    rel_properties: properties([
                        ("content_relation", Value::String("replaces".to_string())),
                        ("confidence", Value::Float(0.9)),
                    ]),
                    target_label: "Memory".to_string(),
                    target_properties: properties([
                        ("id", Value::String("mem-snapshot-old".to_string())),
                        ("is_latest", Value::Bool(false)),
                    ]),
                },
            )
            .unwrap();
        store
            .create_connected_nodes(
                &mut catalog,
                ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([(
                        "id",
                        Value::String("mem-torn-new".to_string()),
                    )]),
                    rel_type: "EVOLVES".to_string(),
                    rel_properties: properties([(
                        "content_relation",
                        Value::String("torn".to_string()),
                    )]),
                    target_label: "Memory".to_string(),
                    target_properties: properties([(
                        "id",
                        Value::String("mem-torn-old".to_string()),
                    )]),
                },
            )
            .unwrap();
    }

    let wal_path = active_wal_path(&path);
    truncate_test_wal_tail(&wal_path, 8);

    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    let repair = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    assert_eq!(repair.next_lsn_after_repair, 4);
    assert!(repair.discarded_wal_tail_bytes > 0);

    let mut catalog = Catalog::default();
    let store = GraphStore::open(&path, &mut catalog).unwrap();
    let report = store.storage_recovery_report();
    assert!(report.durable);
    assert_eq!(report.checkpoint_commit_epoch, Some(1));
    assert_eq!(report.wal_replay_start_lsn, Some(2));
    assert_eq!(report.next_lsn_after_replay, Some(4));
    assert_eq!(report.replayed_wal_entries, 2);
    assert!(!report.torn_tail_ignored);
    assert!(report.torn_tail_reason.is_none());
    assert_eq!(report.recovered_commit_epoch, 3);

    let memory_label = catalog.label_id("Memory").unwrap();
    let memories = store.scan_nodes(Some(memory_label)).collect::<Vec<_>>();
    let ids = memories
        .iter()
        .filter_map(|node| node.properties.get("id").cloned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        ids,
        BTreeSet::from([
            Value::String("mem-old".to_string()),
            Value::String("mem-new".to_string()),
            Value::String("mem-snapshot-old".to_string()),
        ])
    );
    let old = memories
        .iter()
        .find(|node| node.properties.get("id") == Some(&Value::String("mem-old".to_string())))
        .unwrap();
    assert_eq!(old.properties.get("is_latest"), Some(&Value::Bool(false)));
    assert_eq!(old.properties.get("updated_at"), Some(&Value::Int(20)));

    let evolves = catalog.rel_type_id("EVOLVES").unwrap();
    let relationships = store.scan_relationships(Some(evolves)).collect::<Vec<_>>();
    assert_eq!(relationships.len(), 1);
    assert_eq!(
        relationships[0].properties.get("content_relation"),
        Some(&Value::String("replaces".to_string()))
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn adaptive_histograms_use_medium_sample_for_medium_cardinality() {
    let nodes = histogram_nodes("score", 2_000);
    let statistics = compute_statistics(&nodes, &crate::cow::CowRelMap::default(), 1);
    let histogram = statistics
        .property_histograms
        .get(&(LabelId(0), "score".to_string()))
        .unwrap();

    assert_eq!(statistics.histogram_sample_limit, 512);
    assert_eq!(histogram.len(), 256);
    assert_eq!(histogram.first(), Some(&Value::Int(0)));
    assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
    assert_eq!(
        statistics
            .property_distinct_counts
            .get(&(LabelId(0), "score".to_string())),
        Some(&2_000)
    );
    assert_eq!(
        statistics
            .sampled_property_histograms
            .get(&(LabelId(0), "score".to_string())),
        Some(&true)
    );
}

#[test]
fn adaptive_histograms_use_max_sample_for_large_cardinality() {
    let nodes = histogram_nodes("score", 5_000);
    let statistics = compute_statistics(&nodes, &crate::cow::CowRelMap::default(), 1);
    let histogram = statistics
        .property_histograms
        .get(&(LabelId(0), "score".to_string()))
        .unwrap();

    assert_eq!(histogram.len(), 512);
    assert_eq!(histogram.first(), Some(&Value::Int(0)));
    assert_eq!(histogram.last(), Some(&Value::Int(4_999)));
}

#[test]
fn incremental_basic_statistics_match_full_recompute_after_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([("id", Value::String("stat-source".to_string()))]),
        )
        .unwrap();
    let target = store
        .create_node(
            &mut catalog,
            "Entity",
            properties([("id", Value::String("stat-target".to_string()))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.9))]),
        )
        .unwrap();

    let report = store.basic_statistics_consistency_report();
    assert!(report.ready);
    assert!(report.mismatched_fields.is_empty());
    assert_eq!(report.incremental, report.recomputed);
    assert_eq!(report.incremental.node_count, 2);
    assert_eq!(report.incremental.relationship_count, 1);
    assert_eq!(
        report
            .incremental
            .label_counts
            .get(&catalog.label_id("Memory").unwrap()),
        Some(&1)
    );
    assert_eq!(
        report
            .incremental
            .label_counts
            .get(&catalog.label_id("Entity").unwrap()),
        Some(&1)
    );
    assert_eq!(
        report
            .incremental
            .rel_type_counts
            .get(&catalog.rel_type_id("MENTIONS").unwrap()),
        Some(&1)
    );

    store
        .delete_nodes(
            &mut catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".to_string(),
                value: Value::String("stat-source".to_string()),
            }),
            true,
        )
        .unwrap();
    let report = store.basic_statistics_consistency_report();
    assert!(report.ready);
    assert!(report.mismatched_fields.is_empty());
    assert_eq!(report.incremental, report.recomputed);
    assert_eq!(report.incremental.node_count, 1);
    assert_eq!(report.incremental.relationship_count, 0);
    assert!(!report
        .incremental
        .label_counts
        .contains_key(&catalog.label_id("Memory").unwrap()));
    assert!(!report
        .incremental
        .rel_type_counts
        .contains_key(&catalog.rel_type_id("MENTIONS").unwrap()));
}

#[test]
fn degree_statistics_consistency_report_matches_full_recompute_after_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    let target_one = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
        .unwrap();
    let target_two = store
        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target_one,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target_two,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    let memory_label = catalog.label_id("Memory").unwrap();
    let entity_label = catalog.label_id("Entity").unwrap();
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

    let report = store.degree_statistics_consistency_report();
    assert!(report.ready);
    assert!(report.mismatched_keys.is_empty());
    assert_eq!(report.maintained, report.recomputed);
    assert_eq!(
        report
            .maintained
            .get(&DegreeStatisticsKey {
                label_id: memory_label,
                rel_type,
                direction: AdjacencyDirection::Outgoing,
            })
            .copied(),
        Some(DegreeStatisticsEntry {
            node_count: 1,
            non_zero_node_count: 1,
            relationship_count: 2,
            max_degree: 2,
            dense_node_count: 0,
        })
    );
    assert_eq!(
        report
            .maintained
            .get(&DegreeStatisticsKey {
                label_id: entity_label,
                rel_type,
                direction: AdjacencyDirection::Incoming,
            })
            .copied(),
        Some(DegreeStatisticsEntry {
            node_count: 2,
            non_zero_node_count: 2,
            relationship_count: 2,
            max_degree: 1,
            dense_node_count: 0,
        })
    );

    store
        .delete_relationships(
            &mut catalog,
            RelationshipDeleteRequest {
                source_label: "Memory".to_string(),
                filter: None,
                rel_type: "MENTIONS".to_string(),
                target_label: "Entity".to_string(),
                target_filter: Some(PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::Int(1),
                }),
                rel_filter: None,
            },
        )
        .unwrap();
    let report = store.degree_statistics_consistency_report();
    assert!(report.ready);
    assert_eq!(report.maintained, report.recomputed);
    assert_eq!(
        report
            .maintained
            .get(&DegreeStatisticsKey {
                label_id: memory_label,
                rel_type,
                direction: AdjacencyDirection::Outgoing,
            })
            .map(|entry| entry.relationship_count),
        Some(1)
    );
}

#[test]
fn degree_statistics_consistency_report_counts_dense_groups() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
        .unwrap();
    for id in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::Int(id as i64))]),
            )
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();
    }
    let report = store.degree_statistics_consistency_report();
    let memory_label = catalog.label_id("Memory").unwrap();
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

    assert!(report.ready);
    assert_eq!(
        report
            .maintained
            .get(&DegreeStatisticsKey {
                label_id: memory_label,
                rel_type,
                direction: AdjacencyDirection::Outgoing,
            })
            .map(|entry| (entry.max_degree, entry.dense_node_count)),
        Some((DENSE_ADJACENCY_DEGREE_THRESHOLD as u64, 1))
    );
}

#[test]
fn distinct_value_statistics_consistency_report_matches_index_after_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_property_index(&mut catalog, "Memory", "unit_type")
        .unwrap();
    store
        .create_property_index(&mut catalog, "Memory", "importance")
        .unwrap();
    let source = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("id", Value::String("memory:1".to_string())),
                ("unit_type", Value::String("note".to_string())),
                ("importance", Value::Float(0.2)),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("id", Value::String("memory:2".to_string())),
                ("unit_type", Value::String("task".to_string())),
                ("importance", Value::Float(0.7)),
            ]),
        )
        .unwrap();
    let target = store
        .create_node(
            &mut catalog,
            "Entity",
            properties([("id", Value::String("entity:1".to_string()))]),
        )
        .unwrap();
    let weak = store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.2))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.7))]),
        )
        .unwrap();

    let report = store.distinct_value_statistics_consistency_report(&catalog);
    let memory_label = catalog.label_id("Memory").unwrap();
    let rel_type = catalog.rel_type_id("MENTIONS").unwrap();
    assert!(report.ready);
    assert_eq!(
        report
            .maintained_property_distinct_counts
            .get(&(memory_label, "unit_type".to_string())),
        Some(&2)
    );
    assert_eq!(
        report
            .maintained_property_distinct_counts
            .get(&(memory_label, "importance".to_string())),
        Some(&2)
    );
    assert_eq!(
        report
            .maintained_rel_property_distinct_counts
            .get(&(rel_type, "confidence".to_string())),
        Some(&2)
    );

    store
        .set_node_property(
            &mut catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".to_string(),
                value: Value::String("memory:2".to_string()),
            }),
            "unit_type",
            Value::String("note".to_string()),
        )
        .unwrap();
    store.apply_set_relationship_property(weak, "confidence".to_string(), Value::Float(0.7));

    let report = store.distinct_value_statistics_consistency_report(&catalog);
    assert!(report.ready);
    assert!(report.mismatched_property_keys.is_empty());
    assert!(report.mismatched_rel_property_keys.is_empty());
    assert_eq!(
        report.maintained_property_distinct_counts,
        report.recomputed_property_distinct_counts
    );
    assert_eq!(
        report.maintained_rel_property_distinct_counts,
        report.recomputed_rel_property_distinct_counts
    );
    assert_eq!(
        report
            .maintained_property_distinct_counts
            .get(&(memory_label, "unit_type".to_string())),
        Some(&1)
    );
    assert_eq!(
        report
            .maintained_rel_property_distinct_counts
            .get(&(rel_type, "confidence".to_string())),
        Some(&1)
    );

    store.apply_delete_relationship(weak);
    let report = store.distinct_value_statistics_consistency_report(&catalog);
    assert!(report.ready);
    assert_eq!(
        report
            .maintained_rel_property_distinct_counts
            .get(&(rel_type, "confidence".to_string())),
        Some(&1)
    );
}

#[test]
fn property_index_consistency_report_matches_full_scan_after_mutations() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("id", Value::String("memory:1".to_string())),
                ("unit_type", Value::String("note".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("id", Value::String("memory:2".to_string())),
                ("unit_type", Value::String("task".to_string())),
            ]),
        )
        .unwrap();
    let target = store
        .create_node(
            &mut catalog,
            "Entity",
            properties([("id", Value::String("entity:1".to_string()))]),
        )
        .unwrap();
    let weak = store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.2))]),
        )
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            target,
            "MENTIONS",
            properties([("confidence", Value::Float(0.7))]),
        )
        .unwrap();

    let report = store.property_index_consistency_report(&catalog);
    assert!(report.ready);
    assert_eq!(
        report.node_index_entry_count,
        report.recomputed_node_index_entry_count
    );
    assert_eq!(
        report.node_index_reference_count,
        report.recomputed_node_index_reference_count
    );
    assert_eq!(
        report.relationship_index_entry_count,
        report.recomputed_relationship_index_entry_count
    );
    assert_eq!(
        report.relationship_index_reference_count,
        report.recomputed_relationship_index_reference_count
    );
    assert_eq!(report.missing_node_key_count, 0);
    assert_eq!(report.extra_node_key_count, 0);
    assert_eq!(report.mismatched_node_key_count, 0);
    assert_eq!(report.missing_relationship_key_count, 0);
    assert_eq!(report.extra_relationship_key_count, 0);
    assert_eq!(report.mismatched_relationship_key_count, 0);
    assert!(report.mismatched_node_keys.is_empty());
    assert!(report.mismatched_relationship_keys.is_empty());

    store
        .set_node_property(
            &mut catalog,
            "Memory",
            Some(&PropertyFilter::Eq {
                property: "id".to_string(),
                value: Value::String("memory:2".to_string()),
            }),
            "unit_type",
            Value::String("note".to_string()),
        )
        .unwrap();
    store.apply_set_relationship_property(weak, "confidence".to_string(), Value::Float(0.7));
    store.apply_delete_relationship(weak);

    let report = store.property_index_consistency_report(&catalog);
    assert!(report.ready);
    assert_eq!(
        report.node_index_entry_count,
        report.recomputed_node_index_entry_count
    );
    assert_eq!(
        report.node_index_reference_count,
        report.recomputed_node_index_reference_count
    );
    assert_eq!(
        report.relationship_index_entry_count,
        report.recomputed_relationship_index_entry_count
    );
    assert_eq!(
        report.relationship_index_reference_count,
        report.recomputed_relationship_index_reference_count
    );
    assert!(report.mismatched_node_keys.is_empty());
    assert!(report.mismatched_relationship_keys.is_empty());
}

#[test]
fn statistics_track_relationship_property_distinct_counts() {
    let relationships = (0..10)
        .map(|id| {
            (
                RelId(id),
                RelRecord {
                    id: RelId(id),
                    source: NodeId(id),
                    target: NodeId(id + 100),
                    rel_type: RelTypeId(0),
                    properties: properties([("weight", Value::Int((id % 4) as i64))]),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();

    let statistics = compute_statistics(&CowSegmentedMap::default(), &relationships, 1);

    assert_eq!(
        statistics
            .rel_property_distinct_counts
            .get(&(RelTypeId(0), "weight".to_string())),
        Some(&4)
    );
    assert_eq!(
        statistics
            .rel_property_histograms
            .get(&(RelTypeId(0), "weight".to_string()))
            .and_then(|values| values.first()),
        Some(&Value::Int(0))
    );
    assert_eq!(
        statistics
            .rel_property_histograms
            .get(&(RelTypeId(0), "weight".to_string()))
            .and_then(|values| values.last()),
        Some(&Value::Int(3))
    );
    assert_eq!(
        statistics
            .sampled_rel_property_histograms
            .get(&(RelTypeId(0), "weight".to_string())),
        Some(&false)
    );
}

#[test]
fn recovered_property_statistics_drop_ineligible_and_incomplete_groups() {
    let mut catalog = Catalog::default();
    let label_id = catalog.get_or_create_label("Metric");
    let rel_type_id = catalog.get_or_create_rel_type("MEASURES");
    let node_table = catalog.get_or_create_table(TableKind::Node, "Metric");
    let relationship_table = catalog.get_or_create_table(TableKind::Relationship, "MEASURES");
    catalog.get_or_create_property(node_table, "body", PropertyType::Text, true);
    catalog.get_or_create_property(relationship_table, "note", PropertyType::Text, true);
    let compact_node = (label_id, "score".to_string());
    let text_node = (label_id, "body".to_string());
    let incomplete_node = (label_id, "missing_marker".to_string());
    let empty_node = (label_id, "empty_histogram".to_string());
    let compact_relationship = (rel_type_id, "weight".to_string());
    let text_relationship = (rel_type_id, "note".to_string());
    let mut statistics = GraphStatistics {
        property_distinct_counts: BTreeMap::from([
            (compact_node.clone(), 2),
            (text_node.clone(), 1),
            (incomplete_node.clone(), 1),
            (empty_node.clone(), 0),
        ]),
        property_histograms: BTreeMap::from([
            (compact_node.clone(), vec![Value::Int(1), Value::Int(2)]),
            (
                text_node.clone(),
                vec![Value::String("payload".to_string())],
            ),
            (incomplete_node, vec![Value::Int(3)]),
            (empty_node.clone(), Vec::new()),
        ]),
        sampled_property_histograms: BTreeMap::from([
            (compact_node.clone(), false),
            (text_node, false),
            (empty_node, false),
        ]),
        rel_property_distinct_counts: BTreeMap::from([
            (compact_relationship.clone(), 1),
            (text_relationship.clone(), 1),
        ]),
        rel_property_histograms: BTreeMap::from([
            (compact_relationship.clone(), vec![Value::Float(0.5)]),
            (
                text_relationship.clone(),
                vec![Value::String("payload".to_string())],
            ),
        ]),
        sampled_rel_property_histograms: BTreeMap::from([
            (compact_relationship.clone(), false),
            (text_relationship, false),
        ]),
        ..GraphStatistics::default()
    };

    retain_supported_property_statistics(&mut statistics, Some(&catalog));

    assert_eq!(
        statistics.property_distinct_counts,
        BTreeMap::from([(compact_node.clone(), 2)])
    );
    assert_eq!(
        statistics
            .property_histograms
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![compact_node.clone()]
    );
    assert_eq!(
        statistics
            .sampled_property_histograms
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![compact_node]
    );
    assert_eq!(
        statistics.rel_property_distinct_counts,
        BTreeMap::from([(compact_relationship.clone(), 1)])
    );
    assert_eq!(
        statistics
            .rel_property_histograms
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![compact_relationship]
    );
}

#[test]
fn statistics_track_path_source_and_target_coverage() {
    let nodes: crate::cow::CowNodeMap = [
        (0, LabelId(0)),
        (1, LabelId(0)),
        (10, LabelId(1)),
        (11, LabelId(1)),
        (12, LabelId(1)),
        (20, LabelId(1)),
        (21, LabelId(1)),
    ]
    .into_iter()
    .map(|(id, label)| {
        (
            NodeId(id),
            NodeRecord {
                id: NodeId(id),
                labels: BTreeSet::from([label]),
                properties: BTreeMap::new(),
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
    .into();
    let relationships = [
        (0, 0, 10),
        (1, 0, 11),
        (2, 1, 11),
        (3, 1, 12),
        (4, 1, 12),
        (5, 10, 20),
        (6, 11, 20),
        (7, 12, 21),
    ]
    .into_iter()
    .map(|(id, source, target)| {
        (
            RelId(id),
            RelRecord {
                id: RelId(id),
                source: NodeId(source),
                target: NodeId(target),
                rel_type: RelTypeId(0),
                properties: BTreeMap::new(),
            },
        )
    })
    .collect::<BTreeMap<_, _>>()
    .into();

    let statistics = compute_statistics(&nodes, &relationships, 1);
    let path = (LabelId(0), RelTypeId(0), LabelId(1));

    assert_eq!(statistics.path_counts.get(&path), Some(&5));
    assert_eq!(statistics.path_source_distinct_counts.get(&path), Some(&2));
    assert_eq!(statistics.path_target_distinct_counts.get(&path), Some(&3));

    let two_hop_path = (LabelId(0), RelTypeId(0), LabelId(1), 2);
    assert_eq!(statistics.bounded_path_counts.get(&two_hop_path), Some(&5));
    assert_eq!(
        statistics
            .bounded_path_source_distinct_counts
            .get(&two_hop_path),
        Some(&2)
    );
    assert_eq!(
        statistics
            .bounded_path_target_distinct_counts
            .get(&two_hop_path),
        Some(&2)
    );
}

#[test]
fn adaptive_histograms_track_relationship_sampling() {
    let relationships = (0..2_000)
        .map(|id| {
            (
                RelId(id),
                RelRecord {
                    id: RelId(id),
                    source: NodeId(id),
                    target: NodeId(id + 100),
                    rel_type: RelTypeId(0),
                    properties: properties([("score", Value::Int(id as i64))]),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();

    let statistics = compute_statistics(&CowSegmentedMap::default(), &relationships, 1);
    let histogram = statistics
        .rel_property_histograms
        .get(&(RelTypeId(0), "score".to_string()))
        .unwrap();

    assert_eq!(histogram.len(), 256);
    assert_eq!(histogram.first(), Some(&Value::Int(0)));
    assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
    assert_eq!(
        statistics
            .sampled_rel_property_histograms
            .get(&(RelTypeId(0), "score".to_string())),
        Some(&true)
    );
}

#[test]
fn delta_validation_still_catches_touched_unique_violation() {
    let path = unique_test_dir("delta_unique_violation");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_unique_constraint(&mut catalog, "T", "k")
        .unwrap();
    store
        .create_node(&mut catalog, "T", properties([("k", Value::Int(1))]))
        .unwrap();
    // The second insert is the touched record; the first is untouched.
    // Delta validation must still compare against the full graph.
    let error = store
        .create_node(&mut catalog, "T", properties([("k", Value::Int(1))]))
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn ddl_ops_fall_back_to_full_validation() {
    let path = unique_test_dir("ddl_full_validation");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    // Pre-existing duplicates before the constraint exists.
    let first = store
        .create_node(&mut catalog, "T", properties([("k", Value::Int(1))]))
        .unwrap();
    store
        .create_node(&mut catalog, "T", properties([("k", Value::Int(1))]))
        .unwrap();
    // Register the constraint in the catalog directly so commit-time
    // validation sees the post-DDL schema. create_unique_constraint()
    // validates separately and would reject before ever reaching
    // validate_constraints_for_ops.
    let label_id = catalog.label_id("T").unwrap();
    catalog.get_or_create_unique_constraint(label_id, "k");
    // A commit batch mixing data and DDL must not enter delta validation:
    // the violation lives in records this commit did not touch.
    let ops = [WalOp::Batch(vec![
        WalOp::SetNodeProperty {
            id: first,
            property: "unrelated".to_string(),
            value: Value::Int(9),
        },
        WalOp::CreateUniqueConstraint {
            label: "T".to_string(),
            property: "k".to_string(),
        },
    ])];
    let error = store
        .validate_constraints_for_ops(&catalog, &ops)
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn delta_validation_catches_touched_vs_touched_unique_violation() {
    let path = unique_test_dir("delta_touched_pair");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_unique_constraint(&mut catalog, "T", "k")
        .unwrap();
    // Two colliding creates inside one commit: both records are touched,
    // so the conflict exists only within the touched set itself.
    let ops = [
        WalOp::CreateNode {
            id: NodeId(1),
            label: "T".to_string(),
            properties: properties([("k", Value::Int(1))]),
        },
        WalOp::CreateNode {
            id: NodeId(2),
            label: "T".to_string(),
            properties: properties([("k", Value::Int(1))]),
        },
    ];
    let error = store
        .validate_constraints_for_ops(&catalog, &ops)
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn delta_validation_catches_relationship_unique_violation() {
    let path = unique_test_dir("delta_rel_unique");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    let a = store
        .create_node(&mut catalog, "T", properties([]))
        .unwrap();
    let b = store
        .create_node(&mut catalog, "T", properties([]))
        .unwrap();
    store
        .create_relationship_unique_constraint(&mut catalog, "R", "k")
        .unwrap();
    store
        .create_relationship(&mut catalog, a, b, "R", properties([("k", Value::Int(1))]))
        .unwrap();
    let error = store
        .create_relationship(&mut catalog, a, b, "R", properties([("k", Value::Int(1))]))
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn delta_unique_validation_handles_bulk_touched_sets() {
    let path = unique_test_dir("delta_bulk_touched");
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_unique_constraint(&mut catalog, "T", "k")
        .unwrap();
    let mut ids = Vec::new();
    for i in 0..2000 {
        ids.push(
            store
                .create_node(&mut catalog, "T", properties([("k", Value::Int(i))]))
                .unwrap(),
        );
    }
    // A bulk SET touching every record stays correct: re-setting each
    // node to its own value must pass, while one colliding write must
    // still be caught among thousands of touched records.
    let rewrites: Vec<WalOp> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| WalOp::SetNodeProperty {
            id: *id,
            property: "k".to_string(),
            value: Value::Int(i as i64),
        })
        .collect();
    store
        .validate_constraints_for_ops(&catalog, &rewrites)
        .unwrap();

    let mut colliding = rewrites.clone();
    colliding.push(WalOp::SetNodeProperty {
        id: ids[0],
        property: "k".to_string(),
        value: Value::Int(1999),
    });
    let error = store
        .validate_constraints_for_ops(&catalog, &colliding)
        .unwrap_err();
    assert!(error.to_string().contains("unique constraint"));
    let _ = std::fs::remove_dir_all(&path);
}

/// Timing evidence for the delta unique validators: K=1 single-record
/// commits must stay far below the full scan, while K=N bulk commits must
/// not exceed the full validator's bound. Run with --ignored --nocapture.
#[test]
#[ignore = "timing probe for delta vs full unique validation"]
fn delta_unique_validation_scaling_probe() {
    use crate::graph_constraints::TouchedRecords;
    use std::time::Instant;
    for n in [1_000usize, 2_000] {
        let path = unique_test_dir(&format!("delta_unique_scaling_{n}"));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_unique_constraint(&mut catalog, "T", "k")
            .unwrap();
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            ids.push(
                store
                    .create_node(&mut catalog, "T", properties([("k", Value::Int(i as i64))]))
                    .unwrap(),
            );
        }
        // K=1: one touched record against the post-image.
        let single = TouchedRecords {
            nodes: BTreeSet::from([ids[n - 1]]),
            relationships: BTreeSet::new(),
        };
        let t = Instant::now();
        super::validate_unique_constraints(&catalog, &store.nodes).unwrap();
        let full_1 = t.elapsed();
        let t = Instant::now();
        super::validate_unique_constraints_for_records(&catalog, &store.nodes, &single).unwrap();
        let delta_1 = t.elapsed();
        // K=N: every record touched by a bulk SET on the constrained key.
        let bulk = TouchedRecords {
            nodes: ids.iter().copied().collect(),
            relationships: BTreeSet::new(),
        };
        let t = Instant::now();
        super::validate_unique_constraints_for_records(&catalog, &store.nodes, &bulk).unwrap();
        let delta_n = t.elapsed();
        eprintln!(
                "n={n} K=1: full={full_1:?} delta={delta_1:?} | K=N: delta={delta_n:?} (full={full_1:?})"
            );
        let _ = std::fs::remove_dir_all(&path);
    }
}

#[test]
fn wal_ops_touched_records_scopes_pure_data_and_rejects_ddl() {
    use super::wal_ops_touched_records;

    let data = [
        WalOp::CreateNode {
            id: NodeId(1),
            label: "T".to_string(),
            properties: BTreeMap::new(),
        },
        WalOp::SetNodeProperty {
            id: NodeId(2),
            property: "k".to_string(),
            value: Value::Int(1),
        },
    ];
    let touched = wal_ops_touched_records(&data).unwrap();
    assert!(touched.nodes.contains(&NodeId(1)));
    assert!(touched.nodes.contains(&NodeId(2)));
    assert!(touched.relationships.is_empty());

    let ddl = [
        WalOp::CreateNode {
            id: NodeId(1),
            label: "T".to_string(),
            properties: BTreeMap::new(),
        },
        WalOp::CreateUniqueConstraint {
            label: "T".to_string(),
            property: "k".to_string(),
        },
    ];
    assert!(wal_ops_touched_records(&ddl).is_none());
}
