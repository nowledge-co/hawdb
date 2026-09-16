use super::*;
use crate::durability::fail_durable_replace_for_destination;
use crate::relational::{
    RelationalHydrationBudget, RelationalKey, RelationalOverflowConfig,
    RelationalOverflowExtentInput, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRef, RelationalOverflowRootReader,
    RelationalRow, RelationalRowPageEntry, RelationalScalarType, RelationalValue,
};
use skein_integrity::integrity_digest;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn streamed_page_relocation_preserves_rows_epochs_and_overflow_references() {
    let directory = unique_test_dir("streamed-relocation");
    fs::create_dir_all(&directory).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let path = directory.join(relational_row_page_artifact_file(2));
    let mut writer = root::PageArtifactWriter::new(&path, config.page_limits).unwrap();
    let reference = *RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"relocated overflow payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap()
    .reference();
    let mut expected = Vec::new();
    for index in 0..3u64 {
        let mut original = page(index + 1, 1, 10, index as i64, index as i64);
        original.rows[0].row = RelationalRow::new(vec![
            RelationalValue::BigInt(index as i64),
            RelationalValue::Overflow(reference),
        ]);
        let original_bytes = original.encode(config.page_limits).unwrap();
        let mut relocated =
            ImmutableRelationalRowPage::decode(&original_bytes, config.page_limits).unwrap();
        relocated.generation = 2;
        let mut prepared = root::prepare_dirty_page(relocated, config.page_limits).unwrap();
        writer.write(&mut prepared).unwrap();
        assert_eq!(prepared.descriptor.logical_page_id, original.page_id);
        assert_eq!(prepared.descriptor.physical_generation, 2);
        assert_eq!(prepared.descriptor.physical_slot, index);
        assert_eq!(prepared.descriptor.source_commit_epoch, 10);
        assert_eq!(prepared.page.rows, original.rows);
        expected.push(prepared);
    }
    let (artifact, count) = writer.finish().unwrap();
    assert_eq!(count, 3);
    let bytes = fs::read(path).unwrap();
    assert_eq!(artifact.encoded_len, bytes.len() as u64);
    assert_eq!(
        artifact.encoded_crc32c,
        integrity_digest(&bytes).crc32c.get()
    );
    assert_eq!(artifact.encoded_sha256, integrity_digest(&bytes).sha256);
    for (slot, expected) in bytes
        .chunks_exact(config.page_limits.max_page_bytes.get())
        .zip(expected)
    {
        let digest = integrity_digest(slot);
        assert_eq!(
            expected.descriptor.slot_integrity.slot_crc32c,
            digest.crc32c.get()
        );
        assert_eq!(
            expected.descriptor.slot_integrity.slot_sha256,
            digest.sha256
        );
        assert_eq!(
            ImmutableRelationalRowPage::decode_slot(slot, config.page_limits).unwrap(),
            expected.page
        );
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streamed_page_writer_rejects_identity_drift_before_appending() {
    let directory = unique_test_dir("streamed-identity-drift");
    fs::create_dir_all(&directory).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let path = directory.join(relational_row_page_artifact_file(2));
    let mut writer = root::PageArtifactWriter::new(&path, config.page_limits).unwrap();
    for field in 0..3 {
        let mut prepared =
            root::prepare_dirty_page(page(1, 2, 10, 1, 2), config.page_limits).unwrap();
        match field {
            0 => prepared.descriptor.logical_page_id = page_id(2),
            1 => prepared.descriptor.physical_generation = 1,
            _ => prepared.descriptor.source_commit_epoch = 9,
        }
        assert!(matches!(
            writer.write(&mut prepared),
            Err(RelationalRowPagePublicationError::Corrupt(message))
                if message.contains("metadata changed")
        ));
    }
    let (artifact, count) = writer.finish().unwrap();
    assert_eq!(count, 0);
    assert_eq!(artifact.encoded_len, 0);
    assert_eq!(fs::metadata(path).unwrap().len(), 0);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn publish_last_root_round_trips_with_a_concrete_refinement_trace() {
    let directory = unique_test_dir("round-trip");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    let report = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    assert_eq!(report.dirty_pages_written, 2);
    assert_eq!(report.root_pages, 2);
    assert_eq!(report.reused_pages, 0);
    assert_eq!(report.events, COMPLETE_PUBLICATION_TRACE);
    assert_eq!(
        report.page_artifact_bytes,
        2 * config.page_limits.max_page_bytes.get() as u64
    );

    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().generation, 1);
    assert_eq!(reader.manifest().source_commit_epoch, 10);
    assert_eq!(reader.manifest().previous_generation, None);
    assert_eq!(reader.manifest().tables.len(), 1);
    assert_eq!(
        reader.manifest().tables[0].schema,
        crate::relational::row_page::test_row_page_schema("documents", 2)
    );
    assert_eq!(reader.manifest().tables[0].column_count.get(), 2);
    assert_eq!(reader.manifest().tables[0].row_count, 4);
    assert_eq!(reader.manifest().tables[0].next_page_id.get(), 3);
    let descriptors = collect_descriptors(&reader, "documents");
    assert_eq!(page_id_values(&descriptors), vec![1, 2]);
    assert_eq!(physical_generations(&descriptors), vec![1, 1]);
    assert_eq!(descriptors[0].physical_slot, 0);
    assert_eq!(descriptors[1].physical_slot, 1);
    assert_eq!(
        fs::read(directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE)).unwrap(),
        fs::read(directory.join(relational_row_page_manifest_generation_file(1))).unwrap()
    );

    assert_no_temporary_files(&directory);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn publication_rejects_page_and_table_column_count_drift() {
    let directory = unique_test_dir("column-count-drift");
    let config = RelationalRowPagePublicationConfig::default();
    let mut delta = table_delta("documents", vec![page(1, 1, 10, 1, 2)]);
    delta.column_count = NonZeroU32::new(3).unwrap();
    assert!(matches!(
        RelationalRowPagePublisher::new(config).publish(
            &directory,
            1,
            10,
            None,
            vec![delta],
        ),
        Err(RelationalRowPagePublicationError::Admission(message))
            if message.contains("contains 2 columns, expected 3")
    ));
    assert!(!directory.exists());
}

#[test]
fn persisted_row_candidate_does_not_change_latest_selection() {
    let directory = unique_test_dir("candidate");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory
            .join(RELATIONAL_ROW_PAGE_MANIFEST_FILE)
            .with_extension("skein.tmp"),
        b"abandoned latest selector",
    )
    .unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let report = RelationalRowPagePublisher::new(config)
        .persist_generation(
            RelationalRowPageGenerationRequest {
                directory: &directory,
                generation: 1,
                source_commit_epoch: 10,
                base: None,
                expected_previous_generation: None,
                overflow_root: None,
            },
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();

    assert_eq!(report.events, CANDIDATE_PUBLICATION_TRACE);
    assert_eq!(report.generation_artifacts.generation, 1);
    assert!(RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .is_none());
    let candidate = RelationalRowPageRootReader::open_generation(&directory, 1, config).unwrap();
    assert_eq!(candidate.manifest().root_page_count, 1);

    assert_no_temporary_files(&directory);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn bound_row_generation_verifies_the_canonical_manifest_image() {
    let directory = unique_test_dir("bound-generation");
    let config = RelationalRowPagePublicationConfig::default();
    let report = RelationalRowPagePublisher::new(config)
        .persist_generation(
            RelationalRowPageGenerationRequest {
                directory: &directory,
                generation: 1,
                source_commit_epoch: 10,
                base: None,
                expected_previous_generation: None,
                overflow_root: None,
            },
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();

    let reader = RelationalRowPageRootReader::open_bound_generation(
        &directory,
        report.generation_artifacts,
        config,
    )
    .unwrap();
    assert_eq!(reader.manifest().generation, 1);

    let mut wrong_manifest = report.generation_artifacts;
    wrong_manifest.manifest_artifact.encoded_crc32c ^= 1;
    assert!(matches!(
        RelationalRowPageRootReader::open_bound_generation(
            &directory,
            wrong_manifest,
            config,
        ),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("canonical binding")
    ));

    let mut wrong_root = report.generation_artifacts;
    wrong_root.root_set_digest = integrity_digest(b"wrong row root").sha256;
    assert!(matches!(
        RelationalRowPageRootReader::open_bound_generation(&directory, wrong_root, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("identity")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn generation_manifest_replace_failure_leaves_the_canonical_row_root_unselected() {
    let directory = unique_test_dir("generation-manifest-replace-failure");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let base = RelationalRowPageRootReader::open_generation(&directory, 1, config).unwrap();
    let generation_manifest = relational_row_page_manifest_generation_file(2);
    let failure = fail_durable_replace_for_destination(generation_manifest.clone());

    let error = publisher
        .persist_generation(
            RelationalRowPageGenerationRequest {
                directory: &directory,
                generation: 2,
                source_commit_epoch: 11,
                base: Some(&base),
                expected_previous_generation: Some(1),
                overflow_root: None,
            },
            vec![table_delta("documents", vec![page(1, 2, 11, 1, 3)])],
        )
        .unwrap_err();
    drop(failure);

    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Durability(message)
            if message.contains("injected durable replace failure")
    ));
    assert!(!directory.join(generation_manifest).exists());
    assert!(RelationalRowPageRootReader::open_generation(&directory, 2, config).is_err());
    assert_eq!(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn epoch_zero_accepts_only_an_empty_row_root() {
    let directory = unique_test_dir("empty-epoch-zero");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(&directory, 1, 0, None, Vec::new())
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().source_commit_epoch, 0);
    assert_eq!(reader.manifest().root_page_count, 0);

    let rejected_directory = unique_test_dir("non-empty-epoch-zero");
    assert!(matches!(
        RelationalRowPagePublisher::new(config).publish(
            &rejected_directory,
            1,
            0,
            None,
            vec![table_delta("documents", vec![page(1, 1, 0, 1, 2)])],
        ),
        Err(RelationalRowPagePublicationError::Admission(_))
    ));
    assert!(!rejected_directory.exists());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn incremental_publication_reuses_clean_pages_and_keeps_pinned_roots() {
    let directory = unique_test_dir("reuse");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();

    let report = publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![table_delta(
                "documents",
                vec![page(1, 2, 11, 1, 2), page(3, 2, 11, 5, 6)],
            )],
        )
        .unwrap();
    assert_eq!(report.dirty_pages_written, 2);
    assert_eq!(report.root_pages, 3);
    assert_eq!(report.reused_pages, 1);

    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(current.manifest().generation, 2);
    assert_eq!(current.manifest().previous_generation, Some(1));
    assert_eq!(
        current.manifest().tables[0].schema,
        pinned.manifest().tables[0].schema
    );
    assert_eq!(current.manifest().tables[0].row_count, 6);
    assert_eq!(current.manifest().tables[0].next_page_id.get(), 4);
    let current_descriptors = collect_descriptors(&current, "documents");
    assert_eq!(page_id_values(&current_descriptors), vec![1, 2, 3]);
    assert_eq!(physical_generations(&current_descriptors), vec![2, 1, 2]);
    assert_eq!(
        current.manifest().physical_generations,
        vec![
            RelationalRowPagePhysicalGeneration {
                generation: 1,
                allocated_pages: 2,
                live_pages: 1,
            },
            RelationalRowPagePhysicalGeneration {
                generation: 2,
                allocated_pages: 2,
                live_pages: 2,
            },
        ]
    );

    assert_eq!(pinned.manifest().generation, 1);
    let pinned_descriptors = collect_descriptors(&pinned, "documents");
    assert_eq!(page_id_values(&pinned_descriptors), vec![1, 2]);
    assert_eq!(physical_generations(&pinned_descriptors), vec![1, 1]);
    assert_eq!(pinned.manifest().physical_generations[0].live_pages, 2);
    assert!(directory
        .join(relational_row_page_artifact_file(1))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn physical_scrub_rejects_authenticated_occupancy_drift_and_old_file_growth() {
    let directory = unique_test_dir("physical-occupancy-scrub");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![table_delta(
                "documents",
                vec![page(1, 2, 11, 1, 2), page(3, 2, 11, 5, 6)],
            )],
        )
        .unwrap();
    publisher
        .publish(&directory, 3, 11, Some(2), Vec::new())
        .unwrap();
    let reader = RelationalRowPageRootReader::open_generation(&directory, 3, config).unwrap();
    reader.scrub_physical_pages().unwrap();
    let manifest_path = directory.join(relational_row_page_manifest_generation_file(3));
    let original = reader.manifest().clone();
    for (mut changed, expected) in [
        (original.clone(), "live pages"),
        (original.clone(), "bytes, expected"),
    ] {
        if expected == "live pages" {
            // Preserve the total and each allocation bound, but lie about which
            // old generation owns the pages. Recompute a valid manifest digest.
            changed.physical_generations[0].live_pages = 2;
            changed.physical_generations[1].live_pages = 1;
        } else {
            changed.physical_generations[0].allocated_pages += 1;
        }
        fs::write(
            &manifest_path,
            manifest::encode_manifest(&changed, config).unwrap(),
        )
        .unwrap();
        let changed = RelationalRowPageRootReader::open_generation(&directory, 3, config).unwrap();
        assert!(matches!(changed.scrub_physical_pages(),
            Err(RelationalRowPagePublicationError::Corrupt(message)) if message.contains(expected)
        ));
    }
    fs::write(
        manifest_path,
        manifest::encode_manifest(&original, config).unwrap(),
    )
    .unwrap();
    let old_file = OpenOptions::new()
        .write(true)
        .open(directory.join(relational_row_page_artifact_file(1)))
        .unwrap();
    old_file
        .set_len(old_file.metadata().unwrap().len() + 1)
        .unwrap();
    assert!(matches!(reader.scrub_physical_pages(),
        Err(RelationalRowPagePublicationError::Corrupt(message)) if message.contains("bytes, expected")
    ));
    drop(old_file);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn candidate_compaction_rewrites_only_sparse_physical_generations() {
    for rewritten in [2, 3] {
        assert_candidate_compaction_rewrites_sparse_generation(rewritten);
    }
}

fn assert_candidate_compaction_rewrites_sparse_generation(rewritten: u64) {
    let directory = unique_test_dir("candidate-compaction");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                (1..=4)
                    .map(|id| page(id, 1, 10, id as i64, id as i64))
                    .collect(),
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let mut changed = table_delta(
        "documents",
        (1..=rewritten)
            .map(|id| page(id, 2, 11, id as i64, id as i64))
            .collect(),
    );
    changed.next_page_id = NonZeroU64::new(5).unwrap();
    publisher
        .publish(&directory, 2, 11, Some(1), vec![changed])
        .unwrap();
    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let task = skein_core::RuntimeTaskContext::default();
    let report = publisher
        .persist_generation_compacting(
            RelationalRowPageGenerationRequest {
                directory: &directory,
                generation: 3,
                source_commit_epoch: 11,
                base: Some(&base),
                expected_previous_generation: Some(2),
                overflow_root: None,
            },
            Vec::new(),
            RelationalRowPageRewriteConfig::default(),
            &task,
        )
        .unwrap();
    assert_eq!(report.dirty_pages_written, 0);
    assert_eq!(report.relocated_pages_written, 4 - rewritten);
    assert_eq!(report.reused_pages, rewritten);
    assert_eq!(report.events, CANDIDATE_PUBLICATION_TRACE);
    assert_eq!(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        2
    );
    let candidate = RelationalRowPageRootReader::open_generation(&directory, 3, config).unwrap();
    candidate.scrub_physical_pages().unwrap();
    let descriptors = collect_descriptors(&candidate, "documents");
    assert_eq!(
        physical_generations(&descriptors),
        (1..=4)
            .map(|id| if id <= rewritten { 2 } else { 3 })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        candidate
            .manifest()
            .physical_generations
            .iter()
            .map(|entry| entry.allocated_pages)
            .sum::<u64>(),
        4
    );
    for (old, new) in collect_descriptors(&base, "documents")
        .iter()
        .zip(&descriptors)
    {
        let old_page = base.read_page(old).unwrap();
        let new_page = candidate.read_page(new).unwrap();
        assert_eq!(old_page.rows, new_page.rows);
        assert_eq!(old_page.page_id, new_page.page_id);
        assert_eq!(old_page.source_commit_epoch, new_page.source_commit_epoch);
    }
    for descriptor in collect_descriptors(&pinned, "documents") {
        assert_eq!(pinned.read_page(&descriptor).unwrap().generation, 1);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn candidate_compaction_fails_closed_on_limits_cancellation_and_corruption() {
    let directory = unique_test_dir("compaction-failure");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let request = RelationalRowPageGenerationRequest {
        directory: &directory,
        generation: 2,
        source_commit_epoch: 10,
        base: Some(&base),
        expected_previous_generation: Some(1),
        overflow_root: None,
    };
    let rewrite = RelationalRowPageRewriteConfig {
        max_live_ratio_percent: 100,
        ..RelationalRowPageRewriteConfig::default()
    };
    let task = skein_core::RuntimeTaskContext::default();
    for limit in [
        RelationalRowPageRewriteConfig {
            max_live_ratio_percent: 0,
            ..rewrite
        },
        RelationalRowPageRewriteConfig {
            max_live_ratio_percent: 101,
            ..rewrite
        },
        RelationalRowPageRewriteConfig {
            max_scan_pages: NonZeroU64::new(1).unwrap(),
            ..rewrite
        },
        RelationalRowPageRewriteConfig {
            max_rewrite_bytes: NonZeroU64::new(config.page_limits.max_page_bytes.get() as u64)
                .unwrap(),
            ..rewrite
        },
    ] {
        assert!(matches!(
            publisher.persist_generation_compacting(request, Vec::new(), limit, &task),
            Err(RelationalRowPagePublicationError::Admission(_))
        ));
        assert!(!directory
            .join(relational_row_page_manifest_generation_file(2))
            .exists());
        assert!(!directory
            .join(relational_row_page_artifact_file(2))
            .exists());
    }
    task.cancellation().cancel();
    assert!(
        matches!(publisher.persist_generation_compacting(request, Vec::new(), rewrite, &task),
        Err(RelationalRowPagePublicationError::Admission(message)) if message.contains("stopped"))
    );
    let mut artifact = OpenOptions::new()
        .write(true)
        .open(directory.join(relational_row_page_artifact_file(1)))
        .unwrap();
    artifact.seek(SeekFrom::Start(200)).unwrap();
    artifact.write_all(b"corrupt").unwrap();
    artifact.sync_all().unwrap();
    drop(artifact);
    assert!(
        matches!(publisher.persist_generation_compacting(request, Vec::new(), rewrite, &skein_core::RuntimeTaskContext::default()),
        Err(RelationalRowPagePublicationError::Corrupt(message)) if message.contains("checksum"))
    );
    assert!(!directory
        .join(relational_row_page_manifest_generation_file(2))
        .exists());
    assert_eq!(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn occupancy_validation_rejects_authenticated_invalid_counts() {
    let directory = unique_test_dir("invalid-occupancy");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let original = fs::read(&path).unwrap();
    for invalid_count in [false, true] {
        let mut encoded = original.clone();
        let end = encoded.len();
        if invalid_count {
            encoded[end - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
        } else {
            let live_offset = end - manifest::OCCUPANCY_TRAILER_BYTES - 8;
            encoded[live_offset..live_offset + 8].copy_from_slice(&0u64.to_le_bytes());
        }
        let mut hasher = skein_integrity::IntegrityHasher::new();
        hasher.update(&encoded[..280]);
        hasher.update(&encoded[manifest::MANIFEST_HEADER_BYTES..]);
        let digest = hasher.finish();
        encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
        fs::write(&path, encoded).unwrap();
        assert!(matches!(
            RelationalRowPageRootReader::open_latest(&directory, config),
            Err(RelationalRowPagePublicationError::Corrupt(message))
                if message.contains("invalid live/allocated")
                    || message.contains("count exceeds its manifest payload")
        ));
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn relocated_page_inventory_is_independent_of_dirty_vector_limit() {
    let directory = unique_test_dir("relocated-inventory");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let mut relocated = reader.manifest().clone();
    relocated.dirty_page_count = 0;
    relocated.relocated_page_count = 2;
    let config = RelationalRowPagePublicationConfig {
        max_dirty_pages: NonZeroUsize::new(1).unwrap(),
        ..config
    };
    let encoded = manifest::encode_manifest(&relocated, config).unwrap();
    fs::write(directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE), &encoded).unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().relocated_page_count, 2);
    for descriptor in collect_descriptors(&reader, "documents") {
        assert_eq!(reader.read_page(&descriptor).unwrap().rows.len(), 2);
    }
    relocated.physical_generations[0].allocated_pages = 1;
    assert!(matches!(
        manifest::encode_manifest(&relocated, config),
        Err(RelationalRowPagePublicationError::Admission(_))
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn unaccounted_development_manifest_requires_recreation() {
    let directory = unique_test_dir("unaccounted-manifest");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let mut encoded = fs::read(&path).unwrap();
    encoded[10..12].copy_from_slice(&0u16.to_le_bytes());
    fs::write(path, encoded).unwrap();
    assert!(
        matches!(RelationalRowPageRootReader::open_latest(&directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("recreate the development database"))
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn stale_publication_is_rejected_before_creating_a_candidate() {
    let directory = unique_test_dir("stale");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let error = publisher
        .publish(
            &directory,
            2,
            11,
            None,
            vec![table_delta("documents", vec![page(1, 2, 11, 1, 2)])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(1)
        }
    ));
    assert!(!directory
        .join(relational_row_page_artifact_file(2))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn every_pre_manifest_crash_keeps_the_previous_root_selected() {
    for stop_after in [
        RelationalRowPagePublicationPhase::CandidateStarted,
        RelationalRowPagePublicationPhase::CandidatePagesDurable,
        RelationalRowPagePublicationPhase::CandidateRootDurable,
        RelationalRowPagePublicationPhase::CandidateManifestDurable,
        RelationalRowPagePublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir(&format!("crash-{stop_after:?}"));
        let config = RelationalRowPagePublicationConfig::default();
        let publisher = RelationalRowPagePublisher::new(config);
        publisher
            .publish(
                &directory,
                1,
                10,
                None,
                vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
            )
            .unwrap();
        let error = publisher
            .publish_inner(
                &directory,
                2,
                11,
                Some(1),
                vec![table_delta("documents", vec![page(1, 2, 11, 1, 3)])],
                publisher::PublicationControls {
                    overflow_root: None,
                    stop_after: Some(stop_after),
                },
            )
            .unwrap_err();
        assert!(matches!(
            error,
            RelationalRowPagePublicationError::Durability(message)
                if message.contains("injected stop")
        ));
        assert_no_temporary_files(&directory);
        let selected = RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap();
        assert_eq!(selected.manifest().generation, 1);

        publisher
            .publish(
                &directory,
                3,
                12,
                Some(1),
                vec![table_delta("documents", vec![page(1, 3, 12, 1, 3)])],
            )
            .unwrap();
        assert_eq!(
            RelationalRowPageRootReader::open_latest(&directory, config)
                .unwrap()
                .unwrap()
                .manifest()
                .generation,
            3
        );
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn descriptor_corruption_fails_when_the_selected_entry_is_read() {
    let directory = unique_test_dir("descriptor-corruption");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptor_path = directory.join(relational_row_page_root_descriptor_file(1));
    let mut descriptor = OpenOptions::new()
        .read(true)
        .write(true)
        .open(descriptor_path)
        .unwrap();
    descriptor.seek(SeekFrom::Start(16)).unwrap();
    descriptor.write_all(&[1]).unwrap();
    descriptor.sync_all().unwrap();
    assert!(matches!(
        reader.read_table_page_descriptor("documents", 0),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("binding checksum")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn descriptor_binding_rejects_valid_entries_swapped_between_ordinals() {
    let directory = unique_test_dir("descriptor-swap");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptor_path = directory.join(relational_row_page_root_descriptor_file(1));
    let mut encoded = fs::read(&descriptor_path).unwrap();
    let descriptor_bytes = root::ROOT_DESCRIPTOR_BYTES;
    let first = encoded[..descriptor_bytes].to_vec();
    encoded.copy_within(descriptor_bytes..descriptor_bytes * 2, 0);
    encoded[descriptor_bytes..descriptor_bytes * 2].copy_from_slice(&first);
    fs::write(descriptor_path, encoded).unwrap();

    assert!(matches!(
        reader.read_table_page_descriptor("documents", 0),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("binding checksum")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overflow_and_overlap_are_rejected_without_selecting_a_root() {
    let directory = unique_test_dir("admission");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    let mut overflow_page = page(1, 1, 10, 1, 2);
    overflow_page.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Overflow(RelationalOverflowRef {
            digest: integrity_digest(b"overflow").sha256,
            scalar_type: RelationalScalarType::Text,
            compressed_bytes: 8,
            uncompressed_bytes: 16,
        }),
    ]);
    let error = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![overflow_page])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("overflow")
    ));
    assert!(!directory.exists());

    let overlap = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 3), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap_err();
    assert!(matches!(
        overlap,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("overlap")
    ));
    assert!(!directory.exists());
}

#[test]
fn row_root_binds_and_resolves_the_exact_overflow_generation() {
    let directory = unique_test_dir("overflow-root");
    let overflow_config = RelationalOverflowPublicationConfig::default();
    let input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"overflow payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *input.reference();
    RelationalOverflowPublisher::new(overflow_config)
        .publish(&directory, 1, 10, None, vec![input])
        .unwrap();
    let overflow_root = RelationalOverflowRootReader::open_latest(&directory, overflow_config)
        .unwrap()
        .unwrap();

    let mut overflow_page = page(1, 1, 10, 1, 2);
    overflow_page.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Overflow(reference),
    ]);
    let row_config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(row_config)
        .publish_with_overflow_root(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![overflow_page])],
            &overflow_root,
        )
        .unwrap();
    let row_root = RelationalRowPageRootReader::open_latest(&directory, row_config)
        .unwrap()
        .unwrap();
    assert_eq!(
        row_root.overflow_root_binding(),
        Some(overflow_root.manifest().binding())
    );
    row_root.validate_overflow_root(&overflow_root).unwrap();
    let mut budget = RelationalHydrationBudget::default();
    assert_eq!(
        overflow_root
            .hydrate(&reference, &mut budget, None)
            .unwrap(),
        RelationalValue::Text("overflow payload".to_string())
    );

    let row_publisher = RelationalRowPagePublisher::new(row_config);
    let missing_successor = row_publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![table_delta("documents", vec![page(2, 2, 11, 3, 4)])],
        )
        .unwrap_err();
    assert!(matches!(
        missing_successor,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("descended from overflow-bearing pages")
    ));

    RelationalOverflowPublisher::new(overflow_config)
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![RelationalOverflowExtentInput::Reuse(reference)],
        )
        .unwrap();
    let next_overflow_root = RelationalOverflowRootReader::open_latest(&directory, overflow_config)
        .unwrap()
        .unwrap();
    assert!(next_overflow_root.contains(&reference).unwrap());
    row_publisher
        .publish_with_overflow_root(
            &directory,
            2,
            11,
            Some(1),
            vec![table_delta("documents", vec![page(2, 2, 11, 3, 4)])],
            &next_overflow_root,
        )
        .unwrap();
    let next_row_root = RelationalRowPageRootReader::open_latest(&directory, row_config)
        .unwrap()
        .unwrap();
    next_row_root
        .validate_overflow_root(&next_overflow_root)
        .unwrap();

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn row_root_rejects_missing_or_mismatched_overflow_generation() {
    let directory = unique_test_dir("overflow-mismatch");
    let overflow_config = RelationalOverflowPublicationConfig::default();
    RelationalOverflowPublisher::new(overflow_config)
        .publish(&directory, 1, 10, None, Vec::new())
        .unwrap();
    let overflow_root = RelationalOverflowRootReader::open_latest(&directory, overflow_config)
        .unwrap()
        .unwrap();
    let input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"missing payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *input.reference();
    let mut overflow_page = page(1, 1, 10, 1, 2);
    overflow_page.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Overflow(reference),
    ]);
    let publisher = RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default());
    let missing = publisher
        .publish_with_overflow_root(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![overflow_page.clone()])],
            &overflow_root,
        )
        .unwrap_err();
    assert!(matches!(
        missing,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("missing overflow extent")
    ));
    let mismatched = publisher
        .publish_with_overflow_root(
            &directory,
            2,
            11,
            None,
            vec![table_delta("documents", vec![overflow_page])],
            &overflow_root,
        )
        .unwrap_err();
    assert!(matches!(
        mismatched,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("generation/epoch")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn deletion_requires_a_page_in_the_selected_base() {
    let directory = unique_test_dir("delete");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let error = publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema: None,
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(10).unwrap(),
                dirty_pages: Vec::new(),
                deleted_page_ids: vec![page_id(9)],
            }],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("absent from the selected base")
    ));
    assert_eq!(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn deleting_every_page_publishes_an_empty_table_without_breaking_pinned_roots() {
    let directory = unique_test_dir("delete-all");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();

    let mut deletion = table_delta("documents", Vec::new());
    deletion.next_page_id = NonZeroU64::new(3).unwrap();
    deletion.deleted_page_ids = vec![page_id(1), page_id(2)];
    let report = publisher
        .publish(&directory, 2, 11, Some(1), vec![deletion])
        .unwrap();
    assert_eq!(report.dirty_pages_written, 0);
    assert_eq!(report.root_pages, 0);
    assert_eq!(report.reused_pages, 0);
    assert_eq!(report.page_artifact_bytes, 0);

    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(current.manifest().generation, 2);
    assert_eq!(current.manifest().tables.len(), 1);
    assert_eq!(current.manifest().tables[0].table, "documents");
    assert_eq!(current.manifest().tables[0].page_count, 0);
    assert_eq!(current.manifest().tables[0].row_count, 0);
    assert_eq!(
        current.manifest().tables[0].schema,
        pinned.manifest().tables[0].schema
    );
    assert!(collect_descriptors(&current, "documents").is_empty());
    assert_eq!(
        page_id_values(&collect_descriptors(&pinned, "documents")),
        vec![1, 2]
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn new_table_requires_a_digest_bound_schema_before_artifact_creation() {
    let directory = unique_test_dir("missing-schema");
    let config = RelationalRowPagePublicationConfig::default();
    let mut delta = table_delta("documents", vec![page(1, 1, 10, 1, 2)]);
    delta.schema = None;
    let error = RelationalRowPagePublisher::new(config)
        .publish(&directory, 1, 10, None, vec![delta])
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("missing its schema")
    ));
    assert!(!directory
        .join(relational_row_page_manifest_generation_file(1))
        .exists());
    assert!(!directory.exists());
}

#[test]
fn schema_collection_limits_are_enforced_before_artifact_creation() {
    let directory = unique_test_dir("schema-collection-limit");
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_columns = NonZeroUsize::new(2).unwrap();
    let mut delta = table_delta("documents", vec![page(1, 1, 10, 1, 2)]);
    let schema = delta.schema.as_mut().expect("new table has a schema");
    schema.indexes = vec![
        crate::relational::RelationalIndexSchema {
            name: "documents_value_1_idx".to_string(),
            columns: vec!["value_1".to_string()],
            unique: false,
        },
        crate::relational::RelationalIndexSchema {
            name: "documents_id_idx".to_string(),
            columns: vec!["id".to_string()],
            unique: false,
        },
        crate::relational::RelationalIndexSchema {
            name: "documents_value_1_id_idx".to_string(),
            columns: vec!["value_1".to_string(), "id".to_string()],
            unique: false,
        },
    ];
    delta.schema_digest =
        crate::relational::index_shadow::relational_schema_digest(schema).unwrap();
    for page in &mut delta.dirty_pages {
        page.schema_digest = delta.schema_digest;
    }

    let error = RelationalRowPagePublisher::new(config)
        .publish(&directory, 1, 10, None, vec![delta])
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("indexes") && message.contains("exceeding limit 2")
    ));
    assert!(!directory.exists());
}

#[test]
fn resource_admission_finishes_before_generation_artifacts_are_created() {
    let directory = unique_test_dir("resource-admission");
    let config = RelationalRowPagePublicationConfig {
        max_root_key_bytes: NonZeroU64::new(8).unwrap(),
        ..RelationalRowPagePublicationConfig::default()
    };
    let error = RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPagePublicationError::Admission(message)
            if message.contains("root may contain")
    ));
    assert!(!directory
        .join(relational_row_page_artifact_file(1))
        .exists());
    assert!(!directory
        .join(relational_row_page_root_descriptor_file(1))
        .exists());
    assert!(!directory
        .join(relational_row_page_manifest_generation_file(1))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn open_rejects_manifest_corruption_and_wrong_artifact_lengths() {
    let config = RelationalRowPagePublicationConfig::default();
    let manifest_directory = unique_test_dir("manifest-corruption");
    RelationalRowPagePublisher::new(config)
        .publish(
            &manifest_directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    let latest = manifest_directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&latest)
        .unwrap();
    file.seek(SeekFrom::Start(20)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        RelationalRowPageRootReader::open_latest(&manifest_directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("checksum mismatch")
    ));
    fs::remove_dir_all(manifest_directory).unwrap();

    let artifact_directory = unique_test_dir("artifact-length");
    RelationalRowPagePublisher::new(config)
        .publish(
            &artifact_directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 2)])],
        )
        .unwrap();
    OpenOptions::new()
        .write(true)
        .open(artifact_directory.join(relational_row_page_root_descriptor_file(1)))
        .unwrap()
        .set_len(1)
        .unwrap();
    assert!(matches!(
        RelationalRowPageRootReader::open_latest(&artifact_directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(message))
            if message.contains("descriptor artifact contains")
    ));
    fs::remove_dir_all(artifact_directory).unwrap();
}

#[test]
fn cold_open_defers_row_page_payload_io_until_demand_read() {
    let directory = unique_test_dir("cold-page-payload");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();

    let artifact = directory.join(relational_row_page_artifact_file(1));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();

    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .expect("cold open must not read row-page payloads")
        .expect("published row root exists");
    let descriptor = reader
        .read_table_page_descriptor("documents", 0)
        .expect("root metadata remains readable");
    assert!(matches!(
        reader.read_page(&descriptor),
        Err(RelationalRowPagePublicationError::Corrupt(_))
    ));

    fs::remove_dir_all(directory).unwrap();
}

fn collect_descriptors(
    reader: &RelationalRowPageRootReader,
    table: &str,
) -> Vec<RelationalRowPageRootDescriptor> {
    let mut descriptors = Vec::new();
    reader
        .visit_table_pages(table, |descriptor| {
            descriptors.push(descriptor.clone());
            Ok(())
        })
        .unwrap();
    descriptors
}

fn page_id_values(descriptors: &[RelationalRowPageRootDescriptor]) -> Vec<u64> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.logical_page_id.get())
        .collect()
}

fn physical_generations(descriptors: &[RelationalRowPageRootDescriptor]) -> Vec<u64> {
    descriptors
        .iter()
        .map(|descriptor| descriptor.physical_generation)
        .collect()
}

fn table_delta(
    table: &str,
    dirty_pages: Vec<ImmutableRelationalRowPage>,
) -> RelationalRowPageTableDelta {
    let next_page_id = dirty_pages
        .iter()
        .map(|page| page.page_id.get())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .and_then(NonZeroU64::new)
        .expect("test row-page allocator must remain representable");
    RelationalRowPageTableDelta {
        table: table.to_string(),
        schema: Some(crate::relational::row_page::test_row_page_schema(table, 2)),
        schema_digest: schema_digest(),
        column_count: NonZeroU32::new(2).unwrap(),
        next_page_id,
        dirty_pages,
        deleted_page_ids: Vec::new(),
    }
}

fn page(
    page_id_value: u64,
    generation: u64,
    source_commit_epoch: u64,
    lower: i64,
    upper: i64,
) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation,
        source_commit_epoch,
        page_id: page_id(page_id_value),
        schema_digest: schema_digest(),
        column_count: 2,
        rows: (lower..=upper)
            .map(|value| RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::BigInt(value)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(value),
                    RelationalValue::Text(format!("row-{value}")),
                ]),
            })
            .collect(),
    }
}

fn page_id(value: u64) -> RelationalRowPageId {
    RelationalRowPageId::new(NonZeroU64::new(value).unwrap())
}

fn schema_digest() -> Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 2)
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "skein-row-page-publication-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn assert_no_temporary_files(directory: &std::path::Path) {
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        assert_ne!(
            path.extension(),
            Some(std::ffi::OsStr::new("tmp")),
            "{}",
            path.display()
        );
    }
}
