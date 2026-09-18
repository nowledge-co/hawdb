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
use crate::durability::fail_durable_replace_for_destination;
use crate::relational::{
    RelationalHydrationBudget, RelationalOverflowConfig, RelationalOverflowReferenceSetBuilder,
    RelationalOverflowReferenceSortConfig, RelationalScalarType, RelationalValue,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn publish_last_overflow_root_round_trips() {
    let directory = unique_test_dir("round-trip");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (text, text_reference) = encoded_input(RelationalScalarType::Text, &vec![b'x'; 16 * 1024]);
    let (bytes, bytes_reference) = encoded_input(RelationalScalarType::Bytea, b"123456789");
    let report = publisher
        .publish(&directory, 1, 10, None, vec![text, bytes])
        .unwrap();
    assert_eq!(report.extent_count, 2);
    assert_eq!(report.new_extent_count, 2);
    assert_eq!(report.reused_extent_count, 0);
    assert_eq!(report.events, COMPLETE_PUBLICATION_TRACE);

    let reader = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().generation, 1);
    assert_eq!(reader.manifest().source_commit_epoch, 10);
    assert!(reader.contains(&text_reference).unwrap());
    assert!(reader.contains(&bytes_reference).unwrap());
    let mut budget = RelationalHydrationBudget::default();
    assert_eq!(
        reader.hydrate(&text_reference, &mut budget, None).unwrap(),
        RelationalValue::Text("x".repeat(16 * 1024))
    );
    assert_eq!(
        reader.hydrate(&bytes_reference, &mut budget, None).unwrap(),
        RelationalValue::Bytea(b"123456789".to_vec())
    );
    let initial = RelationalHydrationBudget {
        max_decompressed_bytes: 1,
        ..RelationalHydrationBudget::default()
    };
    let mut rejected = initial;
    assert!(matches!(
        reader.hydrate(&bytes_reference, &mut rejected, None),
        Err(RelationalOverflowPublicationError::Admission(_))
    ));
    assert_eq!(rejected, initial);
    assert_eq!(
        fs::read(directory.join(RELATIONAL_OVERFLOW_MANIFEST_FILE)).unwrap(),
        fs::read(directory.join(relational_overflow_manifest_generation_file(1))).unwrap()
    );

    assert_no_temporary_files(&directory);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn persisted_overflow_candidate_does_not_change_latest_selection() {
    let directory = unique_test_dir("candidate");
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory
            .join(RELATIONAL_OVERFLOW_MANIFEST_FILE)
            .with_extension("hawdb.tmp"),
        b"abandoned latest selector",
    )
    .unwrap();
    let config = RelationalOverflowPublicationConfig::default();
    let (input, reference) = encoded_input(RelationalScalarType::Text, b"candidate payload");
    let report = RelationalOverflowPublisher::new(config)
        .persist_generation(&directory, 1, 10, None, None, vec![input])
        .unwrap();

    assert_eq!(report.events, CANDIDATE_PUBLICATION_TRACE);
    assert_eq!(report.generation_artifacts.generation, 1);
    assert!(
        RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .is_none()
    );
    let candidate = RelationalOverflowRootReader::open_generation(&directory, 1, config).unwrap();
    assert!(candidate.contains(&reference).unwrap());

    assert_no_temporary_files(&directory);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn bound_overflow_generation_verifies_the_canonical_manifest_image() {
    let directory = unique_test_dir("bound-generation");
    let config = RelationalOverflowPublicationConfig::default();
    let (input, reference) = encoded_input(RelationalScalarType::Text, b"bound payload");
    let report = RelationalOverflowPublisher::new(config)
        .persist_generation(&directory, 1, 10, None, None, vec![input])
        .unwrap();

    let reader = RelationalOverflowRootReader::open_bound_generation(
        &directory,
        report.generation_artifacts,
        config,
    )
    .unwrap();
    assert!(reader.contains(&reference).unwrap());

    let mut wrong_manifest = report.generation_artifacts;
    wrong_manifest.manifest_artifact.encoded_crc32c ^= 1;
    assert!(matches!(
        RelationalOverflowRootReader::open_bound_generation(
            &directory,
            wrong_manifest,
            config,
        ),
        Err(RelationalOverflowPublicationError::Corrupt(message))
            if message.contains("canonical binding")
    ));

    let mut wrong_root = report.generation_artifacts;
    wrong_root.root_set_digest = hawdb_integrity::integrity_digest(b"wrong overflow root").sha256;
    assert!(matches!(
        RelationalOverflowRootReader::open_bound_generation(&directory, wrong_root, config),
        Err(RelationalOverflowPublicationError::Corrupt(message))
            if message.contains("identity")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn exact_publication_cleans_candidates_on_success_and_cancellation() {
    for cancel_on in [Some(1), Some(2), None] {
        let directory = unique_test_dir("exact-cleanup");
        let config = RelationalOverflowPublicationConfig::default();
        let publisher = RelationalOverflowPublisher::new(config);
        let (base_input, base_reference) =
            encoded_input(RelationalScalarType::Text, b"base payload");
        publisher
            .publish(&directory, 1, 10, None, vec![base_input])
            .unwrap();
        let base = RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap();
        let (introduced_input, introduced_reference) =
            encoded_input(RelationalScalarType::Bytea, b"introduced payload");
        let RelationalOverflowExtentInput::Write {
            encoded: introduced_encoded,
            ..
        } = introduced_input
        else {
            unreachable!("encoded input always writes an envelope");
        };
        let mut references = RelationalOverflowReferenceSetBuilder::new(
            &directory,
            2,
            RelationalOverflowReferenceSortConfig::default(),
        )
        .unwrap();
        references.push(base_reference).unwrap();
        references.push(introduced_reference).unwrap();
        let references = references.finish().unwrap();
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());

        let mut resolver_calls = 0;
        let result = publisher.persist_generation_exact_references(
            RelationalOverflowExactGenerationRequest {
                directory: &directory,
                generation: 2,
                source_commit_epoch: 11,
                base: &base,
                expected_previous_generation: 1,
                references: &references,
                task: &task,
            },
            |reference| {
                if *reference == introduced_reference {
                    resolver_calls += 1;
                    if cancel_on == Some(resolver_calls) {
                        cancellation.cancel();
                    }
                    Ok(Some(Arc::clone(&introduced_encoded)))
                } else {
                    Ok(None)
                }
            },
        );
        if cancel_on.is_some() {
            assert!(matches!(
                result,
                Err(RelationalOverflowPublicationError::Stopped(_))
            ));
            assert!(!directory
                .join(relational_overflow_manifest_generation_file(2))
                .exists());
            assert!(!directory.join(relational_overflow_extent_file(2)).exists());
        } else {
            let report = result.unwrap();
            assert_eq!(report.introduced_extent_count, 1);
            let candidate =
                RelationalOverflowRootReader::open_generation(&directory, 2, config).unwrap();
            assert!(candidate.contains(&base_reference).unwrap());
            assert_eq!(
                candidate
                    .hydrate(
                        &introduced_reference,
                        &mut RelationalHydrationBudget::default(),
                        None
                    )
                    .unwrap(),
                RelationalValue::Bytea(b"introduced payload".to_vec()),
            );
        }
        assert_no_temporary_files(&directory);
        assert_eq!(
            RelationalOverflowRootReader::open_latest(&directory, config)
                .unwrap()
                .unwrap()
                .manifest()
                .generation,
            1
        );

        drop(references);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn generation_manifest_replace_failure_leaves_the_canonical_overflow_root_unselected() {
    let directory = unique_test_dir("generation-manifest-replace-failure");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, _) = encoded_input(RelationalScalarType::Text, b"first payload");
    publisher
        .publish(&directory, 1, 10, None, vec![first])
        .unwrap();
    let base = RelationalOverflowRootReader::open_generation(&directory, 1, config).unwrap();
    let (second, _) = encoded_input(RelationalScalarType::Text, b"second payload");
    let generation_manifest = relational_overflow_manifest_generation_file(2);
    let failure = fail_durable_replace_for_destination(generation_manifest.clone());

    let error = publisher
        .persist_generation(&directory, 2, 11, Some(&base), Some(1), vec![second])
        .unwrap_err();
    drop(failure);

    assert!(matches!(
        error,
        RelationalOverflowPublicationError::Durability(message)
            if message.contains("injected durable replace failure")
    ));
    assert!(!directory.join(generation_manifest).exists());
    assert!(RelationalOverflowRootReader::open_generation(&directory, 2, config).is_err());
    assert_eq!(
        RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn incremental_root_reuses_content_and_keeps_pinned_generation_readable() {
    let directory = unique_test_dir("reuse");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, first_reference) = encoded_input(RelationalScalarType::Text, b"first payload");
    let (removed, removed_reference) =
        encoded_input(RelationalScalarType::Bytea, b"removed payload");
    publisher
        .publish(&directory, 1, 10, None, vec![first, removed])
        .unwrap();
    let pinned = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();

    let (third, third_reference) = encoded_input(RelationalScalarType::Text, b"third payload");
    let report = publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![RelationalOverflowExtentInput::Reuse(first_reference), third],
        )
        .unwrap();
    assert_eq!(report.extent_count, 2);
    assert_eq!(report.new_extent_count, 1);
    assert_eq!(report.reused_extent_count, 1);

    let current = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert!(current.contains(&first_reference).unwrap());
    assert!(current.contains(&third_reference).unwrap());
    assert!(!current.contains(&removed_reference).unwrap());
    assert!(pinned.contains(&removed_reference).unwrap());
    assert_eq!(pinned.manifest().generation, 1);
    assert!(directory.join(relational_overflow_extent_file(1)).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn metadata_only_delta_retains_unmentioned_base_extents() {
    let directory = unique_test_dir("retain-base");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, first_reference) = encoded_input(RelationalScalarType::Text, b"first payload");
    let (second, second_reference) = encoded_input(RelationalScalarType::Bytea, b"second payload");
    publisher
        .publish(&directory, 1, 10, None, vec![first, second])
        .unwrap();
    let base = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let (third, third_reference) = encoded_input(RelationalScalarType::Text, b"third payload");

    let report = publisher
        .persist_generation_retaining_base(&directory, 2, 11, &base, 1, vec![third])
        .unwrap();

    assert_eq!(report.extent_count, 3);
    assert_eq!(report.new_extent_count, 1);
    assert_eq!(report.reused_extent_count, 2);
    let candidate = RelationalOverflowRootReader::open_generation(&directory, 2, config).unwrap();
    assert!(candidate.contains(&first_reference).unwrap());
    assert!(candidate.contains(&second_reference).unwrap());
    assert!(candidate.contains(&third_reference).unwrap());
    assert_eq!(
        RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    let report = publisher
        .persist_generation_retaining_base(&directory, 3, 12, &candidate, 2, Vec::new())
        .unwrap();
    assert_eq!(report.extent_count, 3);
    assert_eq!(report.new_extent_count, 0);
    assert_eq!(report.reused_extent_count, 3);
    let empty_delta_candidate =
        RelationalOverflowRootReader::open_generation(&directory, 3, config).unwrap();
    assert!(empty_delta_candidate.contains(&first_reference).unwrap());
    assert!(empty_delta_candidate.contains(&second_reference).unwrap());
    assert!(empty_delta_candidate.contains(&third_reference).unwrap());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn stale_publication_is_rejected_before_candidate_creation() {
    let directory = unique_test_dir("stale");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, _) = encoded_input(RelationalScalarType::Text, b"first payload");
    publisher
        .publish(&directory, 1, 10, None, vec![first])
        .unwrap();
    let error = publisher
        .publish(&directory, 2, 11, None, Vec::new())
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalOverflowPublicationError::StaleGeneration {
            expected_previous: None,
            actual_previous: Some(1)
        }
    ));
    assert!(!directory.join(relational_overflow_extent_file(2)).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn every_pre_latest_crash_keeps_the_previous_overflow_root_selected() {
    for phase in [
        RelationalOverflowPublicationPhase::CandidateStarted,
        RelationalOverflowPublicationPhase::CandidateExtentsDurable,
        RelationalOverflowPublicationPhase::CandidateRootDurable,
        RelationalOverflowPublicationPhase::CandidateManifestDurable,
        RelationalOverflowPublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir(&format!("crash-{phase:?}"));
        let config = RelationalOverflowPublicationConfig::default();
        let publisher = RelationalOverflowPublisher::new(config);
        let (first, first_reference) = encoded_input(RelationalScalarType::Text, b"first");
        publisher
            .publish(&directory, 1, 10, None, vec![first])
            .unwrap();
        let (second, _) = encoded_input(RelationalScalarType::Text, b"second");
        assert!(matches!(
            publisher.publish_inner(&directory, 2, 11, Some(1), vec![second], Some(phase)),
            Err(RelationalOverflowPublicationError::Durability(_))
        ));
        assert_no_temporary_files(&directory);
        let selected = RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap();
        assert_eq!(selected.manifest().generation, 1);
        assert!(selected.contains(&first_reference).unwrap());
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn reuse_requires_a_matching_base_extent() {
    let directory = unique_test_dir("missing-reuse");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (_, reference) = encoded_input(RelationalScalarType::Text, b"missing");
    let error = publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![RelationalOverflowExtentInput::Reuse(reference)],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalOverflowPublicationError::MissingExtent(digest) if digest == reference.digest
    ));
    assert!(
        RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .is_none()
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn corrupt_descriptor_and_extent_fail_closed() {
    let descriptor_directory = unique_test_dir("descriptor-corrupt");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (input, reference) = encoded_input(RelationalScalarType::Bytea, b"payload");
    publisher
        .publish(&descriptor_directory, 1, 10, None, vec![input])
        .unwrap();
    flip_byte(
        &descriptor_directory.join(relational_overflow_descriptor_file(1)),
        40,
    );
    let reader = RelationalOverflowRootReader::open_latest(&descriptor_directory, config)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reader.contains(&reference),
        Err(RelationalOverflowPublicationError::Corrupt(_))
    ));
    fs::remove_dir_all(descriptor_directory).unwrap();

    let extent_directory = unique_test_dir("extent-corrupt");
    let (input, reference) = encoded_input(RelationalScalarType::Bytea, b"payload");
    publisher
        .publish(&extent_directory, 1, 10, None, vec![input])
        .unwrap();
    flip_byte(
        &extent_directory.join(relational_overflow_extent_file(1)),
        32,
    );
    let reader = RelationalOverflowRootReader::open_latest(&extent_directory, config)
        .unwrap()
        .unwrap();
    let initial = RelationalHydrationBudget::default();
    let mut budget = initial;
    assert!(matches!(
        reader.hydrate(&reference, &mut budget, None),
        Err(RelationalOverflowPublicationError::Corrupt(_))
    ));
    assert_eq!(budget, initial);
    fs::remove_dir_all(extent_directory).unwrap();
}

#[test]
fn hydration_admission_precedes_extent_io_and_allocation() {
    let directory = unique_test_dir("hydrate-admission-first");
    let config = RelationalOverflowPublicationConfig::default();
    let (input, reference) = encoded_input(RelationalScalarType::Bytea, b"payload");
    RelationalOverflowPublisher::new(config)
        .publish(&directory, 1, 10, None, vec![input])
        .unwrap();
    let reader = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    fs::remove_file(directory.join(relational_overflow_extent_file(1))).unwrap();
    let initial = RelationalHydrationBudget {
        max_memory_bytes: usize::try_from(reference.uncompressed_bytes).unwrap(),
        ..RelationalHydrationBudget::default()
    };
    let mut budget = initial;

    assert!(matches!(
        reader.hydrate(&reference, &mut budget, None),
        Err(RelationalOverflowPublicationError::Admission(_))
    ));
    assert_eq!(budget, initial);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn corrupt_latest_manifest_fails_closed() {
    let directory = unique_test_dir("manifest-corrupt");
    let config = RelationalOverflowPublicationConfig::default();
    let (input, _) = encoded_input(RelationalScalarType::Text, b"payload");
    RelationalOverflowPublisher::new(config)
        .publish(&directory, 1, 10, None, vec![input])
        .unwrap();

    flip_byte(&directory.join(RELATIONAL_OVERFLOW_MANIFEST_FILE), 20);
    assert!(matches!(
        RelationalOverflowRootReader::open_latest(&directory, config),
        Err(RelationalOverflowPublicationError::Corrupt(_))
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn descriptor_binding_rejects_valid_entries_swapped_between_ordinals() {
    let directory = unique_test_dir("descriptor-swap");
    let config = RelationalOverflowPublicationConfig::default();
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, first_reference) = encoded_input(RelationalScalarType::Text, b"first");
    let (second, second_reference) = encoded_input(RelationalScalarType::Text, b"second");
    publisher
        .publish(&directory, 1, 10, None, vec![first, second])
        .unwrap();

    let descriptor_path = directory.join(relational_overflow_descriptor_file(1));
    let mut encoded = fs::read(&descriptor_path).unwrap();
    assert_eq!(encoded.len(), 2 * reader::DESCRIPTOR_BYTES);
    let (left, right) = encoded.split_at_mut(reader::DESCRIPTOR_BYTES);
    left.swap_with_slice(right);
    fs::write(&descriptor_path, encoded).unwrap();

    let reader = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let selected_reference = if first_reference.digest < second_reference.digest {
        first_reference
    } else {
        second_reference
    };
    assert!(matches!(
        reader.contains(&selected_reference),
        Err(RelationalOverflowPublicationError::Corrupt(_))
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn root_capacity_is_admitted_before_generation_artifacts_are_created() {
    let directory = unique_test_dir("root-capacity");
    let config = RelationalOverflowPublicationConfig {
        max_extents: NonZeroU64::new(1).unwrap(),
        ..RelationalOverflowPublicationConfig::default()
    };
    let publisher = RelationalOverflowPublisher::new(config);
    let (first, first_reference) = encoded_input(RelationalScalarType::Text, b"first");
    publisher
        .publish(&directory, 1, 10, None, vec![first])
        .unwrap();
    let (second, _) = encoded_input(RelationalScalarType::Text, b"second");

    assert!(matches!(
        publisher.publish(
            &directory,
            2,
            11,
            Some(1),
            vec![
                RelationalOverflowExtentInput::Reuse(first_reference),
                second
            ],
        ),
        Err(RelationalOverflowPublicationError::Admission(_))
    ));
    assert!(!directory.join(relational_overflow_extent_file(2)).exists());
    assert!(!directory
        .join(relational_overflow_descriptor_file(2))
        .exists());
    assert!(!directory
        .join(relational_overflow_manifest_generation_file(2))
        .exists());
    assert_eq!(
        RelationalOverflowRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap()
            .manifest()
            .generation,
        1
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn extent_byte_admission_precedes_generation_artifact_creation() {
    let directory = unique_test_dir("extent-byte-capacity");
    let config = RelationalOverflowPublicationConfig {
        max_new_extent_bytes: NonZeroU64::new(1).unwrap(),
        ..RelationalOverflowPublicationConfig::default()
    };
    let (input, _) = encoded_input(RelationalScalarType::Text, b"payload");

    assert!(matches!(
        RelationalOverflowPublisher::new(config).publish(&directory, 1, 10, None, vec![input]),
        Err(RelationalOverflowPublicationError::Admission(_))
    ));
    assert!(!directory.join(relational_overflow_extent_file(1)).exists());
    assert!(!directory
        .join(relational_overflow_descriptor_file(1))
        .exists());
    assert!(!directory
        .join(relational_overflow_manifest_generation_file(1))
        .exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn empty_overflow_root_is_a_valid_generation() {
    let directory = unique_test_dir("empty");
    let config = RelationalOverflowPublicationConfig::default();
    let report = RelationalOverflowPublisher::new(config)
        .publish(&directory, 1, 0, None, Vec::new())
        .unwrap();
    assert_eq!(report.extent_count, 0);
    assert_eq!(report.extent_artifact_bytes, 0);
    assert_eq!(
        fs::metadata(directory.join(relational_overflow_extent_file(1)))
            .unwrap()
            .len(),
        0
    );
    let reader = RelationalOverflowRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().source_commit_epoch, 0);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn epoch_zero_rejects_a_non_empty_overflow_root_before_artifact_creation() {
    let directory = unique_test_dir("non-empty-epoch-zero");
    let config = RelationalOverflowPublicationConfig::default();
    let (input, _) = encoded_input(RelationalScalarType::Text, b"payload");
    assert!(matches!(
        RelationalOverflowPublisher::new(config).publish(&directory, 1, 0, None, vec![input]),
        Err(RelationalOverflowPublicationError::Admission(_))
    ));
    assert!(!directory.exists());
}

fn encoded_input(
    scalar_type: RelationalScalarType,
    raw: &[u8],
) -> (RelationalOverflowExtentInput, RelationalOverflowRef) {
    let encoded = super::super::encode_overflow_envelope(
        scalar_type,
        raw,
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    (
        RelationalOverflowExtentInput::Write {
            reference: encoded.reference,
            encoded: encoded.bytes,
        },
        encoded.reference,
    )
}

fn flip_byte(path: &std::path::Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    std::io::Read::read_exact(&mut file, &mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn unique_test_dir(name: &str) -> PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hawdb-relational-overflow-{name}-{}-{sequence}",
        std::process::id()
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
