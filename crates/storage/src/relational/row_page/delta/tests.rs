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

use super::super::demand::RelationalRowPageProjectedOverlayValue;
use super::*;
use crate::relational::{
    ImmutableRelationalRowPage, RelationalKey, RelationalOverflowConfig,
    RelationalOverflowExtentInput, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRootReader, RelationalRecoveryFence,
    RelationalRow, RelationalRowChange, RelationalRowChangeCapture, RelationalRowPageEntry,
    RelationalRowPageId, RelationalRowPagePublicationConfig, RelationalRowPagePublisher,
    RelationalRowPageRecoveredValue, RelationalRowPageTableDelta, RelationalScalarType,
    RelationalValue,
};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[test]
fn checkpoint_run_target_is_soft_but_cannot_exceed_the_hard_limit() {
    let config = RelationalRowDeltaConfig::default();
    assert!(!config.checkpoint_recommended(config.checkpoint_runs.get() - 1));
    assert!(config.checkpoint_recommended(config.checkpoint_runs.get()));

    let directory = unique_test_dir("checkpoint-run-policy");
    let base = publish_and_open_base(&directory);
    let invalid = RelationalRowDeltaConfig {
        max_runs: NonZeroUsize::new(1).unwrap(),
        checkpoint_runs: NonZeroUsize::new(2).unwrap(),
        ..config
    };
    assert!(matches!(
        RelationalRowDeltaBuilder::new(
            &directory,
            &base,
            1,
            None,
            table_metadata(),
            invalid,
        ),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("checkpoint target")
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn immutable_runs_round_trip_across_bounded_flushes() {
    let directory = unique_test_dir("round-trip");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder =
        RelationalRowDeltaBuilder::new(&directory, &base, 1, None, table_metadata(), config)
            .unwrap();
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(1, None)).unwrap();
    let report = builder.finish(3, None).unwrap();
    assert_eq!(report.runs, 2);
    assert_eq!(report.entries, 2);
    assert_eq!(report.replayed_batches, 2);
    assert!(report.peak_dirty_entries <= 1);
    assert!(report.peak_dirty_bytes <= config.max_dirty_bytes.get());
    assert_eq!(report.events, COMPLETE_PUBLICATION_TRACE);

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    assert_eq!(reader.manifest().generation(), report.generation);
    let mut rows = Vec::new();
    let read = reader
        .visit_entries(|table, key, value, epoch| {
            rows.push((table.to_string(), key.clone(), value.clone(), epoch));
            true
        })
        .unwrap();
    assert_eq!(read.runs_read, 2);
    assert_eq!(read.entries_visited, 2);
    assert!(!read.stopped_early);
    assert_eq!(present_text(&rows[0].2), Some("two-v2"));
    assert_eq!(rows[0].3, 2);
    assert!(matches!(
        rows[1].2,
        RelationalRowPageRecoveredValue::Deleted
    ));
    assert_eq!(rows[1].3, 3);
    let wrong_source = RelationalRecoverySourceIdentity {
        record_sequence_sha256: hawdb_integrity::Sha256Digest::from_bytes([0x5a; 32]),
        ..RelationalRecoverySourceIdentity::for_test(1, 3)
    };
    assert!(matches!(
        RelationalRowDeltaReader::open_latest_with_recovery_fence(
            &directory,
            &base,
            RelationalRecoveryFence::new(3, wrong_source),
            config,
        ),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn dirty_run_coalesces_repeated_keys_to_the_latest_epoch() {
    let directory = unique_test_dir("coalesce");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(2, Some("two-v3"))).unwrap();
    let report = builder.finish(3, None).unwrap();
    assert_eq!(report.entries, 1);
    assert_eq!(report.peak_dirty_entries, 1);
    assert!(report.peak_dirty_bytes <= config.max_dirty_bytes.get());

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    let mut observed = None;
    reader
        .visit_entries(|_, key, value, epoch| {
            observed = Some((key.clone(), value.clone(), epoch));
            true
        })
        .unwrap();
    let (_, value, epoch) = observed.unwrap();
    assert_eq!(present_text(&value), Some("two-v3"));
    assert_eq!(epoch, 3);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn unpublished_lookup_prefers_dirty_values_then_newest_immutable_run() {
    let directory = unique_test_dir("unpublished-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);

    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    let (dirty, dirty_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert_eq!(dirty.as_ref().and_then(present_text), Some("two-v2"));
    assert_eq!(dirty_report.entries_visited, 1);
    assert_eq!(dirty_report.runs_read, 0);

    builder.record(3, capture(1, Some("one-v3"))).unwrap();
    let (flushed, flushed_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert_eq!(flushed.as_ref().and_then(present_text), Some("two-v2"));
    assert!(flushed_report.runs_read > 0);

    builder.record(4, capture(2, None)).unwrap();
    let (deleted, deleted_report) = builder.lookup_staged("documents", &key(2)).unwrap();
    assert!(matches!(
        deleted,
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(deleted_report.runs_read, 0);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn delta_schema_must_match_the_base_root_column_count() {
    let directory = unique_test_dir("column-count-fence");
    let base = publish_and_open_base(&directory);
    let mut schemas = table_metadata();
    schemas[0].column_count = NonZeroU32::new(3).unwrap();
    assert!(matches!(
        RelationalRowDeltaBuilder::new(
            &directory,
            &base,
            1,
            None,
            schemas,
            RelationalRowDeltaConfig::default(),
        ),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("does not match the selected row root")
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn point_lookup_selects_the_newest_value_across_immutable_runs() {
    let directory = unique_test_dir("point-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.record(3, capture(1, Some("one-v3"))).unwrap();
    builder.record(4, capture(2, Some("two-v4"))).unwrap();
    builder.record(5, capture(2, None)).unwrap();
    builder.finish(5, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 5, config)
        .unwrap()
        .unwrap();
    let (two, two_report) = reader.lookup("documents", &key(2)).unwrap();
    assert!(matches!(
        two,
        Some(RelationalRowPageRecoveredValue::Deleted)
    ));
    assert_eq!(two_report.runs_read, 1);
    assert!(two_report.stopped_early);

    let (one, one_report) = reader.lookup("documents", &key(1)).unwrap();
    assert_eq!(present_text(one.as_ref().unwrap()), Some("one-v3"));
    assert_eq!(one_report.runs_read, 1);
    assert!(one_report.stopped_early);

    let (missing, missing_report) = reader.lookup("documents", &key(9)).unwrap();
    assert!(missing.is_none());
    assert_eq!(missing_report.runs_read, 0);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn range_lookup_prunes_runs_and_honors_exclusive_bounds() {
    let directory = unique_test_dir("range-lookup");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two"))).unwrap();
    builder.record(3, capture(4, Some("four"))).unwrap();
    builder.record(4, capture(6, Some("six"))).unwrap();
    builder.finish(4, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 4, config)
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    let report = reader
        .visit_range_entries(
            "documents",
            Bound::Excluded(&key(2)),
            Bound::Included(&key(4)),
            |key, value, epoch| {
                rows.push((key.clone(), present_text(value).unwrap().to_string(), epoch));
                true
            },
        )
        .unwrap();
    assert_eq!(rows, vec![(key(4), "four".to_string(), 3)]);
    assert_eq!(report.runs_read, 1);
    assert_eq!(report.entries_visited, 1);
    assert!(!report.stopped_early);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn range_lookup_stops_without_reading_irrelevant_suffix_runs() {
    let directory = unique_test_dir("range-early-stop");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two"))).unwrap();
    builder.record(3, capture(4, Some("four"))).unwrap();
    builder.record(4, capture(6, Some("six"))).unwrap();
    builder.finish(4, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 4, config)
        .unwrap()
        .unwrap();
    let mut rows = Vec::new();
    let report = reader
        .visit_range_entries(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            |key, _, _| {
                rows.push(key.clone());
                false
            },
        )
        .unwrap();
    assert_eq!(rows, vec![key(2)]);
    assert_eq!(report.runs_read, 1);
    assert_eq!(report.entries_visited, 1);
    assert!(report.stopped_early);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_range_sources_bound_retained_run_files() {
    let directory = unique_test_dir("range-file-pool");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        max_range_open_files: NonZeroUsize::new(2).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    for (epoch, id) in (2..=5).zip(2..=5) {
        builder.record(epoch, capture(id, Some("value"))).unwrap();
    }
    builder.finish(5, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 5, config)
        .unwrap()
        .unwrap();
    let (mut sources, report) = reader
        .range_sources(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            &[0],
            false,
            8,
        )
        .unwrap();
    assert_eq!(sources.len(), 4);
    assert_eq!(report.runs_read, 4);
    assert_eq!(report.peak_open_files, 0);

    let mut keys = Vec::new();
    for source in &mut sources {
        keys.push(source.next().unwrap().unwrap().0);
        assert!(source.next().unwrap().is_none());
    }
    assert_eq!(keys, vec![key(2), key(3), key(4), key(5)]);
    let mut completed_report = report;
    sources[0]
        .file_pool_snapshot()
        .unwrap()
        .apply_to(&mut completed_report);
    assert_eq!(completed_report.peak_open_files, 2);
    assert_eq!(completed_report.range_file_opens, 4);
    assert_eq!(completed_report.range_file_pool_hits, 0);
    assert_eq!(completed_report.range_file_pool_misses, 4);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_range_sources_decode_only_requested_recovery_fields() {
    let directory = unique_test_dir("range-projected-decode");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let large_body = "x".repeat(60 * 1024);
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some(&large_body))).unwrap();
    builder.finish(2, None).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let (mut sources, _) = reader
        .range_sources(
            "documents",
            Bound::Unbounded,
            Bound::Unbounded,
            &[0],
            false,
            8,
        )
        .unwrap();
    let (_, value, _) = sources[0].next().unwrap().unwrap();
    let RelationalRowPageProjectedOverlayValue::Present { fields, .. } = value else {
        panic!("recovery row must remain present");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].ordinal, 0);
    assert_eq!(fields[0].value, RelationalValue::BigInt(2));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn cumulative_run_byte_admission_precedes_run_creation() {
    let directory = unique_test_dir("artifact-admission");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_run_bytes: NonZeroU64::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    assert!(matches!(
        builder.finish(2, None),
        Err(RelationalRowDeltaError::Admission(_))
    ));
    assert!(!directory
        .join(relational_row_delta_run_file(1, 1, 0))
        .exists());
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn reader_enforces_the_cumulative_run_byte_limit() {
    let directory = unique_test_dir("reader-run-admission");
    let base = publish_and_open_base(&directory);
    let write_config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, write_config);
    builder.record(2, capture(2, Some("two-v2"))).unwrap();
    builder.finish(2, None).unwrap();

    let read_config = RelationalRowDeltaConfig {
        max_run_bytes: NonZeroU64::new(1).unwrap(),
        ..write_config
    };
    assert!(matches!(
        RelationalRowDeltaReader::open_latest(&directory, &base, 2, read_config),
        Err(RelationalRowDeltaError::Admission(message))
            if message.contains("run") && message.contains("exceeding limit")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_is_poisoned_after_a_partial_epoch_error() {
    let directory = unique_test_dir("poison");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig {
        max_dirty_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowDeltaConfig::default()
    };
    let mut builder = builder(&directory, &base, 1, None, config);
    let error = builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![change(1, None), change(2, Some("two"))],
                encoded_bytes: 0,
            },
        )
        .unwrap_err();
    assert!(matches!(error, RelationalRowDeltaError::Admission(_)));
    assert!(matches!(
        builder.advance_empty(2),
        Err(RelationalRowDeltaError::Invalidated(_))
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_rejects_changes_at_the_immutable_base_epoch() {
    let directory = unique_test_dir("base-epoch-change");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);

    assert!(matches!(
        builder.record(1, capture(2, Some("not-newer"))),
        Err(RelationalRowDeltaError::Corrupt(message))
            if message.contains("immutable base epoch")
    ));
    assert!(matches!(
        builder.finish(1, None),
        Err(RelationalRowDeltaError::Invalidated(_))
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn every_pre_latest_stop_keeps_the_previous_delta_selected() {
    for phase in [
        RelationalRowDeltaPublicationPhase::CandidateRunsDurable,
        RelationalRowDeltaPublicationPhase::CandidateManifestDurable,
        RelationalRowDeltaPublicationPhase::BaseRevalidated,
    ] {
        let directory = unique_test_dir(&format!("crash-{phase:?}"));
        let base = publish_and_open_base(&directory);
        let config = RelationalRowDeltaConfig::default();
        let mut first = builder(&directory, &base, 1, None, config);
        first.record(2, capture(2, Some("two-v2"))).unwrap();
        first.finish(2, None).unwrap();
        let previous = RelationalRowDeltaGeneration {
            base_generation: 1,
            delta_generation: 1,
        };
        let mut candidate = builder(&directory, &base, 2, Some(previous), config);
        candidate.record(2, capture(3, Some("three"))).unwrap();
        candidate.record(3, capture(2, Some("two-v3"))).unwrap();
        assert!(matches!(
            candidate.finish_inner(
                3,
                RelationalRecoverySourceIdentity::for_test(1, 3),
                None,
                Some(phase),
            ),
            Err(RelationalRowDeltaError::Durability(_))
        ));
        let selected = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
            .unwrap()
            .unwrap();
        assert_eq!(selected.manifest().generation(), previous);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn stale_builder_cannot_replace_a_newer_delta_root() {
    let directory = unique_test_dir("stale");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut first = builder(&directory, &base, 1, None, config);
    first.record(2, capture(2, Some("two-v2"))).unwrap();
    first.finish(2, None).unwrap();
    let previous = RelationalRowDeltaGeneration {
        base_generation: 1,
        delta_generation: 1,
    };
    let mut stale = builder(&directory, &base, 2, Some(previous), config);
    stale.advance_empty(2).unwrap();
    stale.record(3, capture(2, Some("stale"))).unwrap();
    let mut winner = builder(&directory, &base, 3, Some(previous), config);
    winner.advance_empty(2).unwrap();
    winner.record(3, capture(2, Some("winner"))).unwrap();
    winner.finish(3, None).unwrap();

    assert!(matches!(
        stale.finish(3, None),
        Err(RelationalRowDeltaError::StaleGeneration {
            expected_previous: Some(expected),
            actual_previous: Some(actual),
        }) if expected == previous && actual.delta_generation == 3
    ));
    let selected = RelationalRowDeltaReader::open_latest(&directory, &base, 3, config)
        .unwrap()
        .unwrap();
    assert_eq!(selected.manifest().delta_generation, 3);

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn builder_cannot_publish_after_its_row_root_becomes_stale() {
    let directory = unique_test_dir("stale-base");
    let base = publish_and_open_base(&directory);
    let row_config = RelationalRowPagePublicationConfig::default();
    let delta_config = RelationalRowDeltaConfig::default();
    let mut candidate = builder(&directory, &base, 1, None, delta_config);
    candidate.record(2, capture(2, Some("two-v2"))).unwrap();

    RelationalRowPagePublisher::new(row_config)
        .publish(
            &directory,
            2,
            2,
            Some(1),
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema: None,
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![ImmutableRelationalRowPage {
                    generation: 2,
                    source_commit_epoch: 2,
                    page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                    schema_digest: schema_digest(),
                    column_count: 2,
                    rows: vec![RelationalRowPageEntry {
                        primary_key: key(1),
                        row: row(1, "one-v2"),
                    }],
                }],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();

    assert!(matches!(
        candidate.finish(2, None),
        Err(RelationalRowDeltaError::StaleBase {
            expected,
            actual: Some(actual),
        }) if expected.generation == 1 && actual.generation == 2
    ));
    assert!(!directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE).exists());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn corruption_poisoning_is_demand_driven() {
    let directory = unique_test_dir("run-corrupt");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut corrupted_run = builder(&directory, &base, 1, None, config);
    corrupted_run.record(2, capture(2, Some("two-v2"))).unwrap();
    corrupted_run.finish(2, None).unwrap();
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let run = directory.join(relational_row_delta_run_file(1, 1, 0));
    let end = fs::metadata(&run).unwrap().len() - 1;
    flip_byte(&run, end);
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    assert!(reader.is_poisoned());
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(message)) if message.contains("poisoned")
    ));
    fs::remove_dir_all(directory).unwrap();

    let manifest_directory = unique_test_dir("manifest-corrupt");
    let base = publish_and_open_base(&manifest_directory);
    let mut candidate = builder(&manifest_directory, &base, 1, None, config);
    candidate.record(2, capture(2, Some("two-v2"))).unwrap();
    candidate.finish(2, None).unwrap();
    flip_byte(
        &manifest_directory.join(RELATIONAL_ROW_DELTA_MANIFEST_FILE),
        36,
    );
    assert!(matches!(
        RelationalRowDeltaReader::open_latest(&manifest_directory, &base, 2, config),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    fs::remove_dir_all(manifest_directory).unwrap();
}

#[test]
fn descriptor_binding_rejects_valid_entries_swapped_between_ordinals() {
    let directory = unique_test_dir("descriptor-swap");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder
        .record(
            2,
            RelationalRowChangeCapture::Captured {
                changes: vec![change(2, Some("two-v2")), change(3, Some("three"))],
                encoded_bytes: 0,
            },
        )
        .unwrap();
    builder.finish(2, None).unwrap();
    let run = directory.join(relational_row_delta_run_file(1, 1, 0));
    let mut encoded = fs::read(&run).unwrap();
    let descriptors = &mut encoded
        [codec::RUN_HEADER_BYTES..codec::RUN_HEADER_BYTES + 2 * codec::ENTRY_DESCRIPTOR_BYTES];
    let (left, right) = descriptors.split_at_mut(codec::ENTRY_DESCRIPTOR_BYTES);
    left.swap_with_slice(right);
    fs::write(&run, encoded).unwrap();

    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    assert!(matches!(
        reader.visit_entries(|_, _, _, _| true),
        Err(RelationalRowDeltaError::Corrupt(_))
    ));
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn overflow_references_allow_a_pinned_state_resolver_or_exact_root() {
    let directory = unique_test_dir("overflow");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let overflow_input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"large payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *overflow_input.reference();
    let overflow_capture = RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(2),
            row: Some(RelationalRow::new(vec![
                RelationalValue::BigInt(2),
                RelationalValue::Overflow(reference),
            ])),
        }],
        encoded_bytes: 0,
    };
    let mut unresolved = builder(&directory, &base, 1, None, config);
    unresolved.record(2, overflow_capture.clone()).unwrap();
    unresolved.finish(2, None).unwrap();
    let unresolved_reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    unresolved_reader.validate_overflow_root(None).unwrap();
    let mut observed = None;
    unresolved_reader
        .visit_entries(|_, _, value, _| {
            observed = Some(value.clone());
            true
        })
        .unwrap();
    assert!(matches!(
        observed,
        Some(RelationalRowPageRecoveredValue::Present(row))
            if row.values()[1] == RelationalValue::Overflow(reference)
    ));

    fs::remove_dir_all(&directory).unwrap();
    let directory = unique_test_dir("overflow-root");
    let base = publish_and_open_base(&directory);
    let overflow_input = RelationalOverflowExtentInput::encode(
        RelationalScalarType::Text,
        b"large payload",
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let reference = *overflow_input.reference();
    let overflow_capture = RelationalRowChangeCapture::Captured {
        changes: vec![RelationalRowChange {
            table: "documents".to_string(),
            primary_key: key(2),
            row: Some(RelationalRow::new(vec![
                RelationalValue::BigInt(2),
                RelationalValue::Overflow(reference),
            ])),
        }],
        encoded_bytes: 0,
    };

    RelationalOverflowPublisher::new(RelationalOverflowPublicationConfig::default())
        .publish(&directory, 1, 2, None, vec![overflow_input])
        .unwrap();
    let overflow_root = RelationalOverflowRootReader::open_latest(
        &directory,
        RelationalOverflowPublicationConfig::default(),
    )
    .unwrap()
    .unwrap();
    let mut admitted = builder(&directory, &base, 1, None, config);
    admitted.record(2, overflow_capture).unwrap();
    admitted.finish(2, Some(&overflow_root)).unwrap();
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    reader.validate_overflow_root(Some(&overflow_root)).unwrap();
    let mut observed = None;
    reader
        .visit_entries(|_, _, value, _| {
            observed = Some(value.clone());
            true
        })
        .unwrap();
    assert!(matches!(
        observed,
        Some(RelationalRowPageRecoveredValue::Present(row))
            if row.values()[1] == RelationalValue::Overflow(reference)
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn empty_commit_range_publishes_a_valid_zero_run_generation() {
    let directory = unique_test_dir("empty");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut builder = builder(&directory, &base, 1, None, config);
    builder.advance_empty(2).unwrap();
    let report = builder.finish(2, None).unwrap();
    assert_eq!(report.runs, 0);
    assert_eq!(report.entries, 0);
    let reader = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        reader
            .visit_entries(|_, _, _, _| true)
            .unwrap()
            .entries_visited,
        0
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn pinned_generation_remains_readable_after_new_publication() {
    let directory = unique_test_dir("pinned");
    let base = publish_and_open_base(&directory);
    let config = RelationalRowDeltaConfig::default();
    let mut first = builder(&directory, &base, 1, None, config);
    first.record(2, capture(2, Some("two-v2"))).unwrap();
    first.finish(2, None).unwrap();
    let pinned = RelationalRowDeltaReader::open_latest(&directory, &base, 2, config)
        .unwrap()
        .unwrap();
    let previous = pinned.manifest().generation();
    let mut second = builder(&directory, &base, 2, Some(previous), config);
    second.record(2, capture(2, Some("two-v2"))).unwrap();
    second.record(3, capture(3, Some("three"))).unwrap();
    second.finish(3, None).unwrap();

    let mut pinned_text = None;
    pinned
        .visit_entries(|_, _, value, _| {
            pinned_text = present_text(value).map(str::to_string);
            true
        })
        .unwrap();
    assert_eq!(pinned_text.as_deref(), Some("two-v2"));
    assert_eq!(pinned.manifest().generation(), previous);

    fs::remove_dir_all(directory).unwrap();
}

fn builder(
    directory: &Path,
    base: &RelationalRowPageRootReader,
    delta_generation: u64,
    expected_previous: Option<RelationalRowDeltaGeneration>,
    config: RelationalRowDeltaConfig,
) -> RelationalRowDeltaBuilder {
    RelationalRowDeltaBuilder::new(
        directory,
        base,
        delta_generation,
        expected_previous,
        table_metadata(),
        config,
    )
    .unwrap()
}

fn publish_and_open_base(directory: &Path) -> RelationalRowPageRootReader {
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            1,
            None,
            vec![RelationalRowPageTableDelta {
                table: "documents".to_string(),
                schema: Some(crate::relational::row_page::test_row_page_schema(
                    "documents",
                    2,
                )),
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![ImmutableRelationalRowPage {
                    generation: 1,
                    source_commit_epoch: 1,
                    page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                    schema_digest: schema_digest(),
                    column_count: 2,
                    rows: vec![RelationalRowPageEntry {
                        primary_key: key(1),
                        row: row(1, "one"),
                    }],
                }],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();
    RelationalRowPageRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap()
}

fn table_metadata() -> Vec<RelationalRowDeltaTableMetadata> {
    vec![RelationalRowDeltaTableMetadata {
        table: "documents".to_string(),
        schema_digest: schema_digest(),
        column_count: NonZeroU32::new(2).unwrap(),
        row_count: 1,
    }]
}

fn capture(id: i64, body: Option<&str>) -> RelationalRowChangeCapture {
    RelationalRowChangeCapture::Captured {
        changes: vec![change(id, body)],
        encoded_bytes: 0,
    }
}

fn change(id: i64, body: Option<&str>) -> RelationalRowChange {
    RelationalRowChange {
        table: "documents".to_string(),
        primary_key: key(id),
        row: body.map(|body| row(id, body)),
    }
}

fn row(id: i64, body: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text(body.to_string()),
    ])
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn schema_digest() -> hawdb_integrity::Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 2)
}

fn present_text(value: &RelationalRowPageRecoveredValue) -> Option<&str> {
    match value {
        RelationalRowPageRecoveredValue::Present(row) => match &row.values()[1] {
            RelationalValue::Text(text) => Some(text),
            _ => None,
        },
        RelationalRowPageRecoveredValue::Deleted => None,
    }
}

fn flip_byte(path: &Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-row-delta-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
