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
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{BackgroundWorkHint, BackgroundWorkPlan, WorkClass};
use std::num::{NonZeroU64, NonZeroUsize};

/// Selection limits for one immutable out-of-core segment compaction.
///
/// A compaction selects adjacent segments with the same level, rewrites their
/// complete ordered document range, and publishes one segment at the next
/// level. `max_input_bytes` accounts for every selected immutable artifact,
/// including descriptor, payload, lexical, layout, metadata, and vector files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutOfCoreSegmentCompactionPolicy {
    level_fan_in: NonZeroUsize,
    max_input_bytes: NonZeroU64,
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
        })
    }

    pub const fn level_fan_in(self) -> NonZeroUsize {
        self.level_fan_in
    }

    pub const fn max_input_bytes(self) -> NonZeroU64 {
        self.max_input_bytes
    }
}

impl Default for SearchOutOfCoreSegmentCompactionPolicy {
    fn default() -> Self {
        Self {
            level_fan_in: NonZeroUsize::new(2).unwrap(),
            max_input_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
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

#[derive(Debug)]
struct Selection {
    start: usize,
    end: usize,
    first_segment_id: u64,
    last_segment_id: u64,
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
    if reader.manifest.segments.len() < fan_in {
        return Ok(None);
    }
    for start in 0..=reader.manifest.segments.len() - fan_in {
        checkpoint(task)?;
        let selected = &reader.manifest.segments[start..start + fan_in];
        let source_level = selected[0].level;
        let Some(target_level) = source_level.checked_add(1) else {
            continue;
        };
        if selected.iter().any(|segment| segment.level != source_level) {
            continue;
        }
        let source_bytes = selected.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment_bytes(segment)?)
                .ok_or_else(|| HawDBError::Storage("search segment byte count overflows".into()))
        })?;
        if source_bytes > policy.max_input_bytes.get() {
            continue;
        }
        let document_count = selected.iter().try_fold(0usize, |total, segment| {
            total.checked_add(segment.document_count).ok_or_else(|| {
                HawDBError::Storage("search segment document count overflows".into())
            })
        })?;
        return Ok(Some(Selection {
            start,
            end: start + fan_in,
            first_segment_id: selected[0].segment_id,
            last_segment_id: selected[selected.len() - 1].segment_id,
            target_level,
            document_count,
            source_bytes,
        }));
    }
    Ok(None)
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
