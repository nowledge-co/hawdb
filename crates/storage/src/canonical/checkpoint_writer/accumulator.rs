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

//! Private writer admission for retained segment bytes and lookup-key arrays.
//! Capacity replacements overlap their old admission until the old buffer dies.
//! Returned descriptors, flush output, and filesystem cleanup have separate owners.

use super::*;
use std::ops::Deref;

pub(in crate::canonical) struct Accumulator {
    segment: SegmentAccumulator,
    pending_push: Option<record_push::Progress>,
    pending_properties: Option<node_properties::Progress>,
    memory: CheckpointAllocationOwner,
    work: CheckpointWorkContext,
    admitted: bool,
    pending_flush: Option<(u64, hawdb_integrity::IntegrityDigest)>,
    flush_progress: checkpoint_flush::FlushProgress,
}

impl Deref for Accumulator {
    type Target = SegmentAccumulator;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

impl Accumulator {
    // The enclosing preparation retains the original memory admission and calls
    // this only between attempts, with no work unit or I/O wave held.
    pub(crate) fn bind_checkpoint_attempt(
        &mut self,
        work: &crate::background::CheckpointWorkContext,
    ) {
        self.work.rebind(work);
    }

    pub(in crate::canonical) fn new(
        kind: CanonicalSegmentKind,
        generation: ManifestGeneration,
        segment_id: u64,
        config: CanonicalSegmentConfig,
        work: CheckpointWorkContext,
        admitted: bool,
    ) -> Self {
        Self {
            segment: SegmentAccumulator::new(kind, generation, segment_id, config),
            pending_push: None,
            pending_properties: None,
            memory: CheckpointAllocationOwner::default(),
            work,
            admitted,
            pending_flush: None,
            flush_progress: checkpoint_flush::FlushProgress::default(),
        }
    }

    #[cfg(test)]
    fn with_memory<T>(
        &mut self,
        operation: impl FnOnce(
            &mut SegmentAccumulator,
            &CheckpointDecodeContext,
        ) -> Result<T, CanonicalSegmentError>,
    ) -> Result<T, CanonicalSegmentError> {
        let work = self.work.clone();
        work.classify(|work| {
            let context = CheckpointDecodeContext {
                work: work.clone(),
                memory: RefCell::new(std::mem::take(&mut self.memory)),
            };
            let result = operation(&mut self.segment, &context);
            self.memory = context.memory.into_inner();
            result
        })
        .map_err(|error| match error {
            CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
            CheckpointOperationError::Operation(error) => error,
        })
    }

    pub(in crate::canonical) fn push(
        &mut self,
        record_id: u64,
        payload: &[u8],
        endpoints: Option<(u64, u64)>,
    ) -> Result<(), CanonicalSegmentError> {
        if self.pending_flush.is_some() || self.pending_properties.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical input cannot change during retained property or flush recovery".into(),
            ));
        }
        if !self.admitted {
            let unit = self.work.start_unit()?;
            self.segment.push(record_id, payload, endpoints)?;
            unit.finish();
            return Ok(());
        }
        let work = self.work.clone();
        self.push_retained_with_work_context(record_id, payload, endpoints, &work)
    }

    /// The immutable record payload must remain identical until this push ends.
    pub(in crate::canonical) fn push_retained_with_work_context(
        &mut self,
        record_id: u64,
        payload: &[u8],
        endpoints: Option<(u64, u64)>,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        if self.pending_flush.is_some() || self.pending_properties.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical input cannot change during retained property or flush recovery".into(),
            ));
        }
        record_push::push(self, record_id, payload, endpoints, work)
    }

    pub(in crate::canonical) fn add_node_properties(
        &mut self,
        node: &NodeRecord,
        work: Option<&CheckpointWorkContext>,
    ) -> Result<(), CanonicalSegmentError> {
        if self.pending_flush.is_some() || self.pending_push.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical input cannot change during retained record or flush recovery".into(),
            ));
        }
        if !self.admitted {
            return self.segment.add_node_properties(node, work);
        }
        let owned_work = self.work.clone();
        node_properties::add(self, node, work.unwrap_or(&owned_work))
    }

    pub(in crate::canonical) fn flush_with_work_context(
        mut self,
        file: &mut File,
        artifact_digest: &mut IntegrityHasher,
        offset: u64,
        work: Option<&CheckpointWorkContext>,
    ) -> Result<super::descriptor::Descriptor, CanonicalSegmentError> {
        let owned_work = self.work.clone();
        if self.admitted {
            self.flush_retained_with_work_context(
                file,
                artifact_digest,
                offset,
                work.unwrap_or(&owned_work),
            )
        } else {
            self.segment
                .flush_with_work_context(file, artifact_digest, offset, work)
                .map(|descriptor| {
                    super::descriptor::Descriptor::new(
                        descriptor,
                        CheckpointAllocationOwner::default(),
                    )
                })
        }
    }

    /// Keep input buffers and their admission on safe work denial. A caller
    /// retaining this state must supply the same private file and captured
    /// source on retry. A fresh admitted attempt rolls back its incomplete tail;
    /// cancellation itself performs no additional filesystem operations. The
    /// artifact digest changes only on complete success.
    pub(in crate::canonical) fn flush_retained_with_work_context(
        &mut self,
        file: &mut File,
        artifact_digest: &mut IntegrityHasher,
        offset: u64,
        work: &CheckpointWorkContext,
    ) -> Result<super::descriptor::Descriptor, CanonicalSegmentError> {
        use std::io::{Seek, SeekFrom};
        if self.pending_push.is_some() || self.pending_properties.is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical flush cannot publish an unfinished record".into(),
            ));
        }
        let covered_digest = artifact_digest.clone().finish();
        if let Some((covered_offset, digest)) = self.pending_flush
            && (covered_offset != offset || digest != covered_digest)
        {
            return Err(CanonicalSegmentError::Corrupt(
                "retained canonical flush source boundary changed".into(),
            ));
        }
        if self.pending_flush.is_some() && self.flush_progress.dirty_tail {
            let boundary = offset
                .checked_add(self.flush_progress.written_bytes)
                .ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(
                        "private flush recovery boundary overflow".into(),
                    )
                })?;
            let unit = work.start_unit()?;
            {
                let _wave = work.io_wave()?;
                if file.metadata()?.len() < boundary {
                    return Err(CanonicalSegmentError::Corrupt(
                        "retained canonical flush lost its covered prefix".into(),
                    ));
                }
                file.set_len(boundary)?;
                file.seek(SeekFrom::Start(boundary))?;
                self.flush_progress.dirty_tail = false;
            }
            unit.finish();
            work.checkpoint()?;
        }
        self.pending_flush = Some((offset, covered_digest));
        let result = checkpoint_flush::flush_admitted(
            &self.segment,
            file,
            artifact_digest,
            offset,
            work,
            &mut self.flush_progress,
        );
        if result.is_ok() {
            self.pending_flush = None;
            self.flush_progress = checkpoint_flush::FlushProgress::default();
        }
        result
    }
}

mod node_properties;
mod record_push;

#[cfg(test)]
mod tests;
