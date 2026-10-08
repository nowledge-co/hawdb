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
#[cfg(feature = "full-text-search")]
use crate::{SearchMode, SearchQueryOptions};
use crate::{
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};
use hawdb_core::{
    RuntimeCancellationToken, RuntimeCapabilities, RuntimeCapability, RuntimeTaskContext,
};
use hawdb_qos::{BackgroundWorkHint, LocalQosPolicy, LocalQosScheduler, WorkClass};
#[cfg(feature = "background-maintenance")]
use hawdb_qos::{QosAdmissionCode, WorkRequest};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;

fn appended_row(number: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: format!("compaction {number}"),
        body: "immutable append compaction coverage".to_string(),
        embedding: Some(vec![1.0, number as f32]),
        source_id: None,
        metadata: Default::default(),
    }
}

fn append(root: &Path, number: usize) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![appended_row(number)],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    update.finish().unwrap();
}

fn append_only_root(name: &str, documents: usize) -> PathBuf {
    assert!(documents >= 1);
    let root = test_dir(name);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    writer.finish().unwrap();
    for number in 1..documents {
        append(&root, number);
    }
    root
}

fn policy(bytes: u64) -> SearchOutOfCoreSegmentCompactionPolicy {
    SearchOutOfCoreSegmentCompactionPolicy::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroU64::new(bytes).unwrap(),
    )
    .unwrap()
}

fn partial_mutation_root(name: &str) -> PathBuf {
    let root = append_only_root(name, 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: (2..12).map(appended_row).collect(),
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    for deleted in [vec![0, 2, 3, 4, 5, 6], vec![7, 8, 9, 10, 11]] {
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                deletes: deleted
                    .into_iter()
                    .map(|number| format!("memory:{number:06}"))
                    .collect(),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
    }
    root
}

#[test]
fn initial_content_ownership_allows_bounded_target_reclamation() {
    let root = test_dir("initial_content_target_reclamation");
    let options = SearchOutOfCoreGenerationBuildOptions {
        max_segment_uncompressed_bytes: NonZeroU64::new(4096).unwrap(),
        max_record_bytes: NonZeroU64::new(4096).unwrap(),
        max_content_documents: NonZeroUsize::new(2).unwrap(),
        max_content_artifact_bytes: NonZeroU64::new(64 * 1024).unwrap(),
        ..Default::default()
    };
    let documents: Vec<_> = (0..20)
        .map(|number| {
            let mut source = document(number);
            source.content = "initial content ownership ".repeat(24);
            source
        })
        .collect();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options.clone()).unwrap();
    for source in &documents {
        writer.push(source.clone()).unwrap();
    }
    writer.finish().unwrap();
    let pinned = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(
        pinned.manifest.segments.len() > 1,
        "bounded payload ranges must have independent lexical/vector content ownership"
    );
    let original_target = pinned.manifest.segments[0].segment_id;
    let replacement = SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: "000000".into(),
        title: "replacement of initial document".into(),
        body: "bounded target reclamation changed content".into(),
        embedding: Some(vec![0.0, 1.0]),
        source_id: None,
        metadata: documents[0].metadata.clone(),
    };
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &pinned,
        SearchProjectionDelta {
            upserts: vec![replacement],
            ..Default::default()
        },
        options.clone(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let initial_content_bytes = 64 * 1024;
    let merged = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(initial_content_bytes),
        options,
    )
    .unwrap()
    .expect("initial owners must be eligible within bounded input bytes");
    assert_eq!(merged.source_segment_count(), 2);
    assert!(merged.source_bytes() <= initial_content_bytes);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.document_count(), documents.len());
    assert!(reopened
        .manifest
        .segments
        .iter()
        .all(|segment| segment.segment_id != original_target));
    assert!(reopened
        .visibility
        .retractions()
        .all(|entry| entry.target_segment_id != original_target));
    let changed = reopened
        .hydrate_documents(std::slice::from_ref(&documents[0].id))
        .unwrap();
    assert_eq!(changed.documents.len(), 1);
    assert_eq!(
        changed.documents[0].title,
        "replacement of initial document"
    );
    assert_eq!(
        pinned
            .hydrate_documents(std::slice::from_ref(&documents[0].id))
            .unwrap()
            .documents,
        vec![documents[0].clone()]
    );
    drop(reopened);
    drop(reader);
    drop(pinned);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_admission_counts_the_complete_lexical_artifacts() {
    let root = append_only_root("compaction_complete_lexical_bytes", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let lexical_bytes: u64 = reader
        .segments
        .iter()
        .map(|segment| segment.lexical_projection.artifact_len())
        .sum();
    assert!(lexical_bytes > 0);
    let complete_bytes = reader.projection_payload_bytes() + lexical_bytes;
    let before = super::published_files(&root);
    assert!(
        SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
            &reader,
            policy(complete_bytes - 1),
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .is_none()
    );
    assert!(SearchOutOfCoreGenerationWriter::prepare_segment_compaction(
        &reader,
        policy(complete_bytes - 1),
        Default::default(),
    )
    .unwrap()
    .is_none());
    assert_eq!(super::published_files(&root), before);
    let report = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(complete_bytes),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.source_bytes(), complete_bytes);
    assert_eq!(report.source_segment_count(), 2);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn mutation_compaction_planning_does_not_copy_retained_runs() {
    let _serial = crate::test_allocation::serial();
    let root = partial_mutation_root("mutation_compaction_planning");
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.manifest.mutation_runs.len(), 2);
    let retained_bytes: usize = reader
        .visibility
        .retractions()
        .filter(|entry| entry.target_segment_id == 2)
        .map(|entry| {
            std::mem::size_of_val(entry)
                + entry.document_id.len()
                + entry.retraction.unique_terms.len() * std::mem::size_of::<String>()
                + entry
                    .retraction
                    .unique_terms
                    .to_vec()
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
        })
        .sum();
    let (plan, peak) = crate::test_allocation::measure(|| {
        SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
            &reader,
            policy(256 * 1024 * 1024),
            BackgroundWorkHint::default(),
        )
        .unwrap()
    });
    assert!(plan.is_some());
    assert!(
        peak < retained_bytes,
        "peak={peak}, retained={retained_bytes}"
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_admission_counts_rewritten_mutation_run_artifacts() {
    let root = partial_mutation_root("compaction_complete_mutation_bytes");
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let lexical_bytes: u64 = reader
        .segments
        .iter()
        .map(|segment| segment.lexical_projection.artifact_len())
        .sum();
    let run_bytes: u64 = reader
        .manifest
        .mutation_runs
        .iter()
        .map(|run| run.len)
        .sum();
    assert!(run_bytes > 0);
    let complete_bytes = reader.projection_payload_bytes() + lexical_bytes + run_bytes;
    let selected_policy = |bytes| {
        SearchOutOfCoreSegmentCompactionPolicy::new(
            NonZeroUsize::new(3).unwrap(),
            NonZeroU64::new(bytes).unwrap(),
        )
        .unwrap()
    };
    assert!(
        SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
            &reader,
            selected_policy(complete_bytes - 1),
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .is_none()
    );
    let report = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        selected_policy(complete_bytes),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.source_bytes(), complete_bytes);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.document_count(), 1);
    assert!(reopened.manifest.mutation_runs.is_empty());
    assert_eq!(
        reopened
            .hydrate_documents(&[document(1).id])
            .unwrap()
            .documents
            .len(),
        1
    );
    drop(reopened);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn mutation_compaction_rejects_a_rewrite_that_cannot_reopen_with_its_budget() {
    let root = partial_mutation_root("mutation_compaction_reopen_budget");
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    // Find the bounded-header admission for the existing closure.
    let mut low = 1;
    let mut high = reader.config.max_mutation_working_bytes.get();
    while low < high {
        let middle = low + (high - low) / 2;
        if reader
            .visibility
            .publication_budget(middle, reader.manifest.segments.len(), false)
            .is_ok()
        {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let config = super::super::super::SearchOutOfCoreConfig {
        max_mutation_working_bytes: NonZeroU64::new(low).unwrap(),
        ..Default::default()
    };
    let mut reader = SearchOutOfCoreReader::open_with_config(&root, config.clone()).unwrap();
    reader.config.max_mutation_working_bytes = NonZeroU64::MIN;
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let error = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("mutation-run working set"),
        "{error}"
    );
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    let reopened = SearchOutOfCoreReader::open_with_config(&root, config).unwrap();
    assert_eq!(reopened.document_count(), 1);
    assert_eq!(reopened.manifest.mutation_runs.len(), 2);
    // The original bounded budget also admits the combined streamed run.
    let reader = reopened;
    SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.document_count(), 1);
    assert_eq!(reopened.manifest.mutation_runs.len(), 1);
    assert_eq!(reopened.manifest.mutation_runs[0].entry_count, 10);
    fs::remove_dir_all(root).unwrap();
}

fn ids(documents: usize) -> Vec<String> {
    (0..documents)
        .map(|number| format!("memory:{number:06}"))
        .collect()
}

#[test]
fn compaction_rewrites_a_bounded_append_range_without_changing_reader_results() {
    let root = append_only_root("compaction_equivalence", 4);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.manifest.segments.len(), 4);
    let ids = ids(4);
    let before = reader.hydrate_documents(&ids).unwrap().documents;
    let before_digest = reader.manifest.documents_digest;
    let work = SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
        &reader,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(work.request.class, WorkClass::Projection);
    assert_eq!(work.request.estimated_operations, 2);
    let report = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.source_segment_count(), 2);
    assert!(report.source_bytes() > 0);
    assert_eq!(report.source_read_metrics().hydrated_documents, 0);
    assert_eq!(report.source_read_metrics().streamed_documents, 2);
    assert_eq!(report.build().document_count, 4);
    assert_eq!(report.build().documents_digest, before_digest);

    let compacted = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(compacted.manifest.segments.len(), 3);
    assert_eq!(compacted.manifest.segments[0].level, 1);
    assert_eq!(
        compacted
            .manifest
            .segments
            .iter()
            .skip(1)
            .map(|segment| segment.level)
            .collect::<Vec<_>>(),
        vec![0, 0]
    );
    assert_eq!(compacted.manifest.documents_digest, before_digest);
    assert_eq!(compacted.hydrate_documents(&ids).unwrap().documents, before);
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, before);
    drop(compacted);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn overlapping_compaction_preserves_exact_scores_and_pinned_readers() {
    let root = test_dir("overlapping_compaction");
    let reference_root = test_dir("overlapping_compaction_reference");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in [0, 2, 4, 6] {
        writer.push(appended_row(number).into_document()).unwrap();
    }
    writer.finish().unwrap();
    append(&root, 1);
    append(&root, 3);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let mut replacement = appended_row(2);
    replacement.body = "replacement compaction exact corpus statistics".into();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![replacement.clone()],
            deletes: vec![appended_row(4).into_document().id],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let mut reference =
        SearchOutOfCoreGenerationWriter::create(&reference_root, Default::default()).unwrap();
    for number in [0, 1, 2, 3, 6] {
        reference
            .push(
                if number == 2 {
                    replacement.clone()
                } else {
                    appended_row(number)
                }
                .into_document(),
            )
            .unwrap();
    }
    reference.finish().unwrap();
    let reference = SearchOutOfCoreReader::open(&reference_root).unwrap();
    let compare = |candidate: &SearchOutOfCoreReader| {
        let modes = [
            SearchMode::Text,
            #[cfg(feature = "vector-search")]
            SearchMode::Vector,
            #[cfg(feature = "vector-search")]
            SearchMode::Hybrid,
        ];
        for mode in modes {
            let options = SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: Default::default(),
                policy_epoch: None,
            };
            let expected = reference
                .search_with_options(
                    "compaction replacement",
                    Some(&[1.0, 2.5]),
                    mode,
                    options.clone(),
                )
                .unwrap();
            let actual = candidate
                .search_with_options("compaction replacement", Some(&[1.0, 2.5]), mode, options)
                .unwrap();
            assert_eq!(actual.result.hits, expected.result.hits);
            assert_eq!(actual.result.total_hits, expected.result.total_hits);
        }
    };
    compare(&reader);
    for expected_segments in [3, 2, 1] {
        let current = SearchOutOfCoreReader::open(&root).unwrap();
        let report = SearchOutOfCoreGenerationWriter::compact_segments(
            &current,
            policy(256 * 1024 * 1024),
            Default::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(report.source_read_metrics().hydrated_documents, 0);
        let after = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(after.manifest.segments.len(), expected_segments);
        compare(&after);
        compare(&reader);
    }
    let after = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(after.manifest.mutation_runs.is_empty());
    drop(reader);
    drop(reference);
    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(reference_root).unwrap();
}

#[test]
fn overlapping_compaction_spool_budget_failure_preserves_the_manifest() {
    let root = test_dir("overlapping_compaction_spool_budget");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in [0, 2] {
        writer.push(appended_row(number).into_document()).unwrap();
    }
    writer.finish().unwrap();
    append(&root, 1);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let error = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        SearchOutOfCoreGenerationBuildOptions {
            max_spool_bytes: NonZeroU64::new(8).unwrap(),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("compaction input spool exceeds admission"),
        "{error}"
    );
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(
        SearchOutOfCoreReader::open(&root).unwrap().document_count(),
        3
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn overlapping_compaction_streams_a_body_larger_than_its_memory_reservation() {
    use crate::{SearchDocumentBody, SearchDocumentHeader, SearchLexicalSourcePolicy};
    use hawdb_core::RuntimeMemoryReservation;
    use std::io::Read;

    let root = test_dir("overlapping_compaction_large_body");
    let body_bytes = 32 * 1024 * 1024;
    let options = SearchOutOfCoreGenerationBuildOptions {
        max_record_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
        lexical_max_document_source_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        ..Default::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options.clone()).unwrap();
    writer
        .push_reader(
            SearchDocumentHeader {
                id: "memory:000000".into(),
                title: "large source".into(),
                embedding: None,
                metadata: Default::default(),
            },
            std::io::Cursor::new(b"bounded content ")
                .chain(std::io::repeat(b' ').take(body_bytes - 16)),
            SearchDocumentBody {
                bytes: body_bytes,
                expected_checksum: None,
            },
        )
        .unwrap();
    let mut last = appended_row(2);
    last.embedding = None;
    writer.push(last.into_document()).unwrap();
    writer.finish().unwrap();
    let mut reader = SearchOutOfCoreReader::open(&root).unwrap();
    reader.set_lexical_source_policy(
        SearchLexicalSourcePolicy::new(options.lexical_max_document_source_bytes).unwrap(),
    );
    let mut inserted = appended_row(1);
    inserted.embedding = None;
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![inserted],
            ..Default::default()
        },
        options.clone(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let mut reader = SearchOutOfCoreReader::open(&root).unwrap();
    reader.set_lexical_source_policy(
        SearchLexicalSourcePolicy::new(options.lexical_max_document_source_bytes).unwrap(),
    );
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(16 * 1024 * 1024, 0));
    let report = SearchOutOfCoreGenerationWriter::compact_segments_with_context(
        &reader,
        policy(256 * 1024 * 1024),
        options,
        task,
    )
    .unwrap()
    .unwrap();
    assert_eq!(report.source_read_metrics().hydrated_documents, 0);
    assert_eq!(report.source_read_metrics().streamed_documents, 3);
    assert!(report.source_read_metrics().streamed_body_bytes >= body_bytes);
    assert_eq!(
        SearchOutOfCoreReader::open(&root)
            .unwrap()
            .manifest
            .segments
            .len(),
        1
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_promotes_only_complete_same_level_runs() {
    let root = append_only_root("compaction_levels", 4);
    for expected_levels in [vec![1, 0, 0], vec![1, 1], vec![2]] {
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let report = SearchOutOfCoreGenerationWriter::compact_segments(
            &reader,
            policy(256 * 1024 * 1024),
            Default::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(report.source_segment_count(), 2);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader
                .manifest
                .segments
                .iter()
                .map(|segment| segment.level)
                .collect::<Vec<_>>(),
            expected_levels
        );
    }
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    )
    .unwrap()
    .is_none());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_defers_when_the_selected_artifacts_exceed_its_byte_budget() {
    let root = append_only_root("compaction_budget", 2);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(1),
        Default::default(),
    )
    .unwrap()
    .is_none());
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn tier_configuration_bounds_normal_merges_without_starting_background_work() {
    let root = append_only_root("compaction_tier_limit", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let policy = policy(256 * 1024 * 1024)
        .with_level_zero_target_bytes(NonZeroU64::new(1).unwrap())
        .unwrap()
        .with_level_size_ratio(NonZeroU64::new(4).unwrap())
        .unwrap()
        .with_level_count(NonZeroU32::new(3).unwrap())
        .with_crisis_segment_count(NonZeroUsize::new(8).unwrap());
    assert_eq!(policy.level_count().get(), 3);
    assert_eq!(policy.level_zero_target_bytes().get(), 1);
    assert_eq!(policy.level_size_ratio().get(), 4);
    assert_eq!(policy.crisis_segment_count().get(), 8);
    assert!(
        SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
            &reader,
            policy,
            BackgroundWorkHint::default(),
        )
        .unwrap()
        .is_none()
    );
    assert!(
        SearchOutOfCoreGenerationWriter::compact_segments(&reader, policy, Default::default(),)
            .unwrap()
            .is_none()
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn crisis_merge_uses_a_bounded_pair_and_does_not_exceed_the_top_level() {
    let root = append_only_root("compaction_crisis", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let policy = SearchOutOfCoreSegmentCompactionPolicy::new(
        NonZeroUsize::new(3).unwrap(),
        NonZeroU64::new(256 * 1024 * 1024).unwrap(),
    )
    .unwrap()
    .with_level_count(NonZeroU32::new(1).unwrap())
    .with_crisis_segment_count(NonZeroUsize::new(2).unwrap());
    let report =
        SearchOutOfCoreGenerationWriter::compact_segments(&reader, policy, Default::default())
            .unwrap()
            .unwrap();
    assert_eq!(report.source_segment_count(), 2);
    let compacted = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(compacted.manifest.segments.len(), 1);
    assert_eq!(compacted.manifest.segments[0].level, 0);
    drop(compacted);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_compaction_tracks_and_releases_the_qos_budget() {
    let root = append_only_root("scheduled_compaction", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(2),
        max_total_background_operations: Some(2),
        ..LocalQosPolicy::default()
    });

    let report = SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments(
        &reader,
        &scheduler,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint::default(),
        Default::default(),
    )
    .unwrap();

    assert_eq!(
        report.stop_reason(),
        SearchOutOfCoreSegmentCompactionStopReason::Completed
    );
    assert_eq!(report.compaction().unwrap().source_segment_count(), 2);
    assert_eq!(scheduler.state().running_background_operations, 0);
    let compacted = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(compacted.manifest.segments.len(), 1);
    drop(compacted);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_compaction_defers_without_staging_when_the_qos_budget_is_full() {
    let root = append_only_root("scheduled_compaction_deferred", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(2),
        max_total_background_operations: Some(2),
        ..LocalQosPolicy::default()
    });
    let running = scheduler
        .try_start(WorkRequest::background(WorkClass::Analytics, 1))
        .unwrap();

    let report = SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments(
        &reader,
        &scheduler,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint::default(),
        Default::default(),
    )
    .unwrap();

    assert!(matches!(
        report.stop_reason(),
        SearchOutOfCoreSegmentCompactionStopReason::Deferred(_)
    ));
    assert!(report.compaction().is_none());
    assert_eq!(scheduler.state().running_background_operations, 1);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);

    running.finish();
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_compaction_honors_tenant_budget_before_acquiring_a_qos_permit() {
    let root = append_only_root("scheduled_compaction_tenant_budget", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let report = SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments(
        &reader,
        &scheduler,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint {
            tenant_budget_remaining_operations: Some(1),
            ..BackgroundWorkHint::default()
        },
        Default::default(),
    )
    .unwrap();

    assert_eq!(
        report.stop_reason(),
        SearchOutOfCoreSegmentCompactionStopReason::Deferred(
            QosAdmissionCode::TenantBudgetExceeded
        )
    );
    assert!(report.compaction().is_none());
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_compaction_releases_its_qos_budget_on_execution_error() {
    let root = append_only_root("scheduled_compaction_error", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
    let options = SearchOutOfCoreGenerationBuildOptions {
        max_generation_bytes: NonZeroU64::new(1).unwrap(),
        ..Default::default()
    };

    let error = SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments(
        &reader,
        &scheduler,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint::default(),
        options,
    )
    .unwrap_err();

    assert!(error.to_string().contains("generation"), "{error}");
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_compaction_observes_cancellation_before_qos_admission() {
    let root = append_only_root("scheduled_compaction_cancel", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
    let cancellation = RuntimeCancellationToken::new();
    assert!(cancellation.cancel());

    let error =
        SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments_with_context(
            &reader,
            &scheduler,
            policy(256 * 1024 * 1024),
            BackgroundWorkHint::default(),
            Default::default(),
            RuntimeTaskContext::without_deadline(cancellation),
        )
        .unwrap_err();

    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn scheduled_compaction_requires_background_maintenance_capability() {
    let root = append_only_root("scheduled_compaction_capability", 2);
    let mut reader = SearchOutOfCoreReader::open(&root).unwrap();
    reader.set_runtime_capabilities(
        RuntimeCapabilities::shared_host().with(RuntimeCapability::BackgroundMaintenance, false),
    );
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = SearchOutOfCoreGenerationWriter::compact_scheduled_background_segments(
        &reader,
        &scheduler,
        policy(256 * 1024 * 1024),
        BackgroundWorkHint::default(),
        Default::default(),
    )
    .unwrap_err();

    assert!(
        error.to_string().contains("background_maintenance"),
        "{error}"
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancelled_compaction_preserves_the_active_manifest() {
    let root = append_only_root("compaction_cancel", 2);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let cancellation = RuntimeCancellationToken::new();
    assert!(cancellation.cancel());
    let error = SearchOutOfCoreGenerationWriter::compact_segments_with_context(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
        RuntimeTaskContext::without_deadline(cancellation),
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn staged_compaction_rejects_a_newer_active_generation() {
    let root = append_only_root("compaction_stale", 2);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let staged = SearchOutOfCoreGenerationWriter::prepare_segment_compaction(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    )
    .unwrap()
    .unwrap();

    let mut replacement =
        SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    replacement.push(document(50)).unwrap();
    replacement.finish().unwrap();
    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let error = staged.finish().unwrap_err();
    assert!(matches!(&error, HawDBError::TransactionConflict { .. }));
    assert!(error.to_string().contains("base changed"), "{error}");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_replace_faults_never_publish_a_partial_closure() {
    // The publication order has eight artifact replacements plus the active
    // manifest on vector-enabled builds. Exercise every replacement boundary;
    // a failure at any one of them must leave the old selector authoritative.
    let mut completed_at = None;
    for replace_number in 1..=10 {
        let root = append_only_root(&format!("compaction_replace_fault_{replace_number}"), 2);
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        super::super::io::fail_replace_at(replace_number);
        let result = SearchOutOfCoreGenerationWriter::compact_segments(
            &reader,
            policy(256 * 1024 * 1024),
            Default::default(),
        );
        super::super::io::fail_replace_at(0);
        match result {
            Err(error) => {
                assert!(error
                    .to_string()
                    .contains("injected publication replace failure"));
                assert_eq!(
                    fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                    before
                );
                drop(reader);
                let reopened = SearchOutOfCoreReader::open(&root).unwrap();
                assert_eq!(reopened.document_count(), 2);
                assert_eq!(stage_directories(&root), 0);
                drop(reopened);
            }
            Ok(_) => {
                // The exact count is target-dependent when RaBitQ is disabled.
                completed_at = Some(replace_number);
                drop(reader);
                fs::remove_dir_all(root).unwrap();
                break;
            }
        }
        fs::remove_dir_all(root).unwrap();
    }
    assert!(
        completed_at.is_some(),
        "all injected replace boundaries failed"
    );
}

#[test]
fn compaction_process_abort_never_publishes_a_partial_closure() {
    let root = append_only_root("compaction_process_abort", 2);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let executable = std::env::current_exe().unwrap();
    let child = std::process::Command::new(executable)
        .arg("--exact")
        .arg("out_of_core::generation_writer::tests::compaction::compaction_process_abort_worker")
        .arg("--nocapture")
        .env("HAWDB_TEST_ABORT_REPLACE_AT", "1")
        .env("HAWDB_TEST_COMPACTION_ROOT", &root)
        .env("RUST_TEST_THREADS", "1")
        .status()
        .unwrap();
    assert!(!child.success(), "the worker must be terminated at publish");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.document_count(), 2);
    drop(reopened);
    // A real process crash cannot run Drop; recovery ignores this orphaned
    // stage and later maintenance removes it. The active closure is still
    // complete and authoritative.
    assert_eq!(stage_directories(&root), 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn compaction_process_abort_worker() {
    let Ok(root) = std::env::var("HAWDB_TEST_COMPACTION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let _ = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        policy(256 * 1024 * 1024),
        Default::default(),
    );
}
