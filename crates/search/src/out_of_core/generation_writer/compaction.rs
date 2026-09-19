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

//! Bounded leveled compaction for immutable append-only search segments.

use super::{
    context_memory::Options, delta::hydration, publication::ActiveManifestUpdate,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationBuildReport,
    SearchOutOfCoreGenerationWriter,
};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::error::{HawDBError, Result};
use crate::{SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use hawdb_core::{RuntimeCapability, RuntimeTaskContext};
use hawdb_qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosScheduler, QosAdmission, QosAdmissionCode,
    WorkClass,
};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

/// Selection limits for one immutable out-of-core segment compaction.
///
/// A compaction selects adjacent segments with the same level, rewrites their
/// complete ordered document range, and publishes one segment at the next
/// level. `max_input_bytes` accounts for every selected immutable artifact,
/// including descriptor, payload, lexical, layout, metadata, and vector files.
/// `level_zero_target_bytes` scales by `level_size_ratio` until the hard input
/// limit; `level_count` caps promotion; and `crisis_segment_count` permits a
/// bounded pair merge when normal tier selection cannot reduce fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutOfCoreSegmentCompactionPolicy {
    level_fan_in: NonZeroUsize,
    max_input_bytes: NonZeroU64,
    level_count: NonZeroU32,
    level_zero_target_bytes: NonZeroU64,
    level_size_ratio: NonZeroU64,
    crisis_segment_count: NonZeroUsize,
}

impl SearchOutOfCoreSegmentCompactionPolicy {
    pub fn new(level_fan_in: NonZeroUsize, max_input_bytes: NonZeroU64) -> Result<Self> {
        if level_fan_in.get() < 2 {
            return Err(HawDBError::Storage(
                "search segment compaction requires a level fan-in of at least two".into(),
            ));
        }
        if max_input_bytes.get() > isize::MAX as u64 {
            return Err(HawDBError::Storage(
                "search segment compaction input byte budget exceeds isize::MAX".into(),
            ));
        }
        Ok(Self {
            level_fan_in,
            max_input_bytes,
            level_count: NonZeroU32::new(8).unwrap(),
            // Preserve the original policy unless the host opts into smaller
            // tier targets.
            level_zero_target_bytes: max_input_bytes,
            level_size_ratio: NonZeroU64::new(2).unwrap(),
            crisis_segment_count: NonZeroUsize::new(16).unwrap(),
        })
    }

    pub const fn level_fan_in(self) -> NonZeroUsize {
        self.level_fan_in
    }

    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }

    pub const fn level_count(self) -> NonZeroU32 {
        self.level_count
    }

    pub const fn level_zero_target_bytes(self) -> NonZeroU64 {
        self.level_zero_target_bytes
    }

    pub const fn level_size_ratio(self) -> NonZeroU64 {
        self.level_size_ratio
    }

    pub const fn crisis_segment_count(self) -> NonZeroUsize {
        self.crisis_segment_count
    }

    pub const fn with_level_count(mut self, level_count: NonZeroU32) -> Self {
        self.level_count = level_count;
        self
    }

    pub fn with_level_zero_target_bytes(mut self, target: NonZeroU64) -> Result<Self> {
        if target.get() > self.max_input_bytes.get() {
            return Err(HawDBError::Storage(
                "search segment compaction level-zero target exceeds its input byte budget".into(),
            ));
        }
        self.level_zero_target_bytes = target;
        Ok(self)
    }

    pub fn with_level_size_ratio(mut self, ratio: NonZeroU64) -> Result<Self> {
        if ratio.get() < 2 {
            return Err(HawDBError::Storage(
                "search segment compaction level size ratio must be at least two".into(),
            ));
        }
        self.level_size_ratio = ratio;
        Ok(self)
    }

    pub const fn with_crisis_segment_count(mut self, threshold: NonZeroUsize) -> Self {
        self.crisis_segment_count = threshold;
        self
    }

    fn level_input_limit(self, level: u32) -> u64 {
        let mut limit = self.level_zero_target_bytes.get();
        for _ in 0..level {
            limit = limit.saturating_mul(self.level_size_ratio.get());
            if limit >= self.max_input_bytes.get() {
                return self.max_input_bytes.get();
            }
        }
        limit
    }
}

impl Default for SearchOutOfCoreSegmentCompactionPolicy {
    fn default() -> Self {
        Self {
            level_fan_in: NonZeroUsize::new(2).unwrap(),
            max_input_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            level_count: NonZeroU32::new(8).unwrap(),
            level_zero_target_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            level_size_ratio: NonZeroU64::new(2).unwrap(),
            crisis_segment_count: NonZeroUsize::new(16).unwrap(),
        }
    }
}

/// Evidence from one committed segment compaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutOfCoreSegmentCompactionReport {
    build: SearchOutOfCoreGenerationBuildReport,
    source_segment_count: usize,
    source_bytes: u64,
    source_read_metrics: SearchOutOfCoreMetrics,
}

/// The terminal outcome of one host-scheduled segment compaction attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchOutOfCoreSegmentCompactionStopReason {
    /// The current manifest has no bounded same-level range eligible for compaction.
    NoEligibleSegments,
    /// The QoS scheduler deferred the work before it began.
    Deferred(QosAdmissionCode),
    /// The QoS scheduler rejected the work before it began.
    Rejected(QosAdmissionCode),
    /// The scheduler admitted the work and the attempt completed.
    Completed,
}

/// Result of one host-scheduled segment compaction attempt.
///
/// The host owns task execution and can use the supplied task context to cancel
/// an admitted compaction. This type records scheduler decisions separately
/// from execution errors returned by the scheduling API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduledSearchOutOfCoreSegmentCompactionReport {
    compaction: Option<SearchOutOfCoreSegmentCompactionReport>,
    stop_reason: SearchOutOfCoreSegmentCompactionStopReason,
}

/// A staged, bounded immutable segment compaction.
///
/// Dropping this value discards its unpublished stage. `finish` rechecks the
/// active generation before replacing the selected manifest entries. Hosts
/// should obtain QoS admission from
/// [`SearchOutOfCoreGenerationWriter::segment_compaction_work_plan`] before
/// staging a compaction.
#[derive(Debug)]
pub struct SearchOutOfCoreSegmentCompaction {
    writer: SearchOutOfCoreGenerationWriter,
    source_segment_count: usize,
    source_bytes: u64,
    source_read_metrics: SearchOutOfCoreMetrics,
}

impl SearchOutOfCoreSegmentCompaction {
    pub fn finish(self) -> Result<SearchOutOfCoreSegmentCompactionReport> {
        let build = self.writer.finish()?;
        Ok(SearchOutOfCoreSegmentCompactionReport {
            build,
            source_segment_count: self.source_segment_count,
            source_bytes: self.source_bytes,
            source_read_metrics: self.source_read_metrics,
        })
    }
}

impl SearchOutOfCoreSegmentCompactionReport {
    pub fn build(&self) -> &SearchOutOfCoreGenerationBuildReport {
        &self.build
    }

    pub const fn source_segment_count(&self) -> usize {
        self.source_segment_count
    }

    pub const fn source_bytes(&self) -> u64 {
        self.source_bytes
    }

    pub fn source_read_metrics(&self) -> &SearchOutOfCoreMetrics {
        &self.source_read_metrics
    }
}

impl ScheduledSearchOutOfCoreSegmentCompactionReport {
    pub const fn compaction(&self) -> Option<&SearchOutOfCoreSegmentCompactionReport> {
        self.compaction.as_ref()
    }

    pub const fn stop_reason(&self) -> SearchOutOfCoreSegmentCompactionStopReason {
        self.stop_reason
    }
}

#[derive(Debug)]
struct Selection {
    start: usize,
    end: usize,
    first_segment_id: u64,
    last_segment_id: u64,
    source_level: u32,
    target_level: u32,
    document_count: usize,
    source_bytes: u64,
}

pub(super) fn segment_background_work_plan(
    reader: &SearchOutOfCoreReader,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    hint: BackgroundWorkHint,
) -> Result<Option<BackgroundWorkPlan>> {
    let task = RuntimeTaskContext::default();
    Ok(select(reader, policy, &task)?.map(|selection| {
        BackgroundWorkPlan::background(WorkClass::Projection, selection.document_count, hint)
    }))
}

pub(super) fn compact(
    reader: &SearchOutOfCoreReader,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    options: SearchOutOfCoreGenerationBuildOptions,
    task: RuntimeTaskContext,
) -> Result<Option<SearchOutOfCoreSegmentCompactionReport>> {
    prepare(reader, policy, options, task)?
        .map_or(Ok(None), |compaction| compaction.finish().map(Some))
}

pub(super) fn scheduled(
    reader: &SearchOutOfCoreReader,
    scheduler: &LocalQosScheduler,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    hint: BackgroundWorkHint,
    options: SearchOutOfCoreGenerationBuildOptions,
    task: RuntimeTaskContext,
) -> Result<ScheduledSearchOutOfCoreSegmentCompactionReport> {
    reader
        .runtime_capabilities
        .require(RuntimeCapability::BackgroundMaintenance)?;
    checkpoint(&task)?;

    let Some(plan) = segment_background_work_plan(reader, policy, hint)? else {
        return Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
            compaction: None,
            stop_reason: SearchOutOfCoreSegmentCompactionStopReason::NoEligibleSegments,
        });
    };
    match scheduler.evaluate_background_work(&plan).admission {
        QosAdmission::Admit => {}
        QosAdmission::Defer { code, .. } => {
            return Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
                compaction: None,
                stop_reason: SearchOutOfCoreSegmentCompactionStopReason::Deferred(code),
            });
        }
        QosAdmission::Reject { code, .. } => {
            return Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
                compaction: None,
                stop_reason: SearchOutOfCoreSegmentCompactionStopReason::Rejected(code),
            });
        }
    }

    let permit = match scheduler.try_start(plan.request) {
        Ok(permit) => permit,
        Err(QosAdmission::Defer { code, .. }) => {
            return Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
                compaction: None,
                stop_reason: SearchOutOfCoreSegmentCompactionStopReason::Deferred(code),
            });
        }
        Err(QosAdmission::Reject { code, .. }) => {
            return Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
                compaction: None,
                stop_reason: SearchOutOfCoreSegmentCompactionStopReason::Rejected(code),
            });
        }
        Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
    };
    let result = compact(reader, policy, options, task);
    permit.finish_with_outcome(result.is_ok());
    Ok(ScheduledSearchOutOfCoreSegmentCompactionReport {
        compaction: result?,
        stop_reason: SearchOutOfCoreSegmentCompactionStopReason::Completed,
    })
}

pub(super) fn prepare(
    reader: &SearchOutOfCoreReader,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    options: SearchOutOfCoreGenerationBuildOptions,
    task: RuntimeTaskContext,
) -> Result<Option<SearchOutOfCoreSegmentCompaction>> {
    checkpoint(&task)?;
    let Some(selection) = select(reader, policy, &task)? else {
        return Ok(None);
    };

    let memory = BuildMemory::new(&task)?;
    let mut options = Options::new(options, &memory, &task)?;
    options.bind_delta_identity(reader, reader.source_graph_commit_epoch())?;
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
        &reader.root,
        options,
        task,
        memory.clone(),
    )?;
    writer.set_lexical_term_policy(reader.lexical_term_policy());
    writer.set_max_lexical_manifest_bytes(reader.config().max_lexical_manifest_bytes)?;
    writer.expected_active_generation = Some(reader.generation());
    writer.active_manifest_update = Some(ActiveManifestUpdate::Compact {
        expected_generation: reader.generation(),
        first_segment_id: selection.first_segment_id,
        last_segment_id: selection.last_segment_id,
        segment_count: selection.end - selection.start,
        target_level: selection.target_level,
    });
    let task_context = writer.task_context.clone();
    let source_read_metrics = hydration::visit_range(
        reader,
        selection.start,
        selection.end,
        &memory,
        &task_context,
        &mut |document| writer.push_inner(document),
    )?;
    Ok(Some(SearchOutOfCoreSegmentCompaction {
        writer,
        source_segment_count: selection.end - selection.start,
        source_bytes: selection.source_bytes,
        source_read_metrics,
    }))
}

fn select(
    reader: &SearchOutOfCoreReader,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    task: &RuntimeTaskContext,
) -> Result<Option<Selection>> {
    let fan_in = policy.level_fan_in.get();
    if let Some(selection) = select_with_fan_in(reader, policy, fan_in, true, task)? {
        return Ok(Some(selection));
    }
    if reader.manifest.segments.len() >= policy.crisis_segment_count.get() {
        return select_with_fan_in(reader, policy, 2, false, task);
    }
    Ok(None)
}

fn select_with_fan_in(
    reader: &SearchOutOfCoreReader,
    policy: SearchOutOfCoreSegmentCompactionPolicy,
    fan_in: usize,
    enforce_tier_limit: bool,
    task: &RuntimeTaskContext,
) -> Result<Option<Selection>> {
    if reader.manifest.segments.len() < fan_in {
        return Ok(None);
    }
    let mut selected = None;
    for start in 0..=reader.manifest.segments.len() - fan_in {
        checkpoint(task)?;
        let candidates = &reader.manifest.segments[start..start + fan_in];
        let source_level = candidates[0].level;
        if candidates
            .iter()
            .any(|segment| segment.level != source_level)
        {
            continue;
        }
        let source_bytes = candidates.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment_bytes(segment)?)
                .ok_or_else(|| HawDBError::Storage("search segment byte count overflows".into()))
        })?;
        if source_bytes > policy.max_input_bytes.get()
            || (enforce_tier_limit && source_bytes > policy.level_input_limit(source_level))
        {
            continue;
        }
        let document_count = candidates.iter().try_fold(0usize, |total, segment| {
            total.checked_add(segment.document_count).ok_or_else(|| {
                HawDBError::Storage("search segment document count overflows".into())
            })
        })?;
        let max_level = (policy.level_count.get() - 1).max(source_level);
        let target_level = source_level.saturating_add(1).min(max_level);
        let candidate = Selection {
            start,
            end: start + fan_in,
            first_segment_id: candidates[0].segment_id,
            last_segment_id: candidates[candidates.len() - 1].segment_id,
            source_level,
            target_level,
            document_count,
            source_bytes,
        };
        if selected
            .as_ref()
            .is_none_or(|current: &Selection| source_level < current.source_level)
        {
            selected = Some(candidate);
        }
    }
    Ok(selected)
}

fn segment_bytes(segment: &crate::out_of_core::SearchOutOfCoreSegmentManifest) -> Result<u64> {
    [
        segment.descriptor_len,
        segment.payload_len,
        segment.metadata_payload_len,
        segment.vector_payload_len,
        segment.layout_len,
        segment.lexical_manifest_len,
        segment.rabitq_artifact_len.unwrap_or_default(),
    ]
    .into_iter()
    .try_fold(0u64, |total, bytes| {
        total
            .checked_add(bytes)
            .ok_or_else(|| HawDBError::Storage("search segment byte count overflows".into()))
    })
}
