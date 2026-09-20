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
    assert_eq!(report.source_read_metrics().hydrated_documents, 2);
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
    assert!(error.to_string().contains("base changed"), "{error}");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(stage_directories(&root), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
