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

//! Private checkpoint point reads admit segment bytes and every record visit.
//! Decoded records retain separate ownership; metadata/cache interiors remain gaps.

use super::*;
use crate::scan::CheckpointRangeReadError;

impl CanonicalSegmentReader {
    pub(crate) fn checkpoint_node(
        &self,
        id: NodeId,
        work: &CheckpointWorkContext,
    ) -> Result<Option<CheckpointRecord<NodeRecord>>, CanonicalSegmentError> {
        self.checkpoint_record(CanonicalSegmentKind::Nodes, id.0, work, |id, payload| {
            checkpoint_decode::node(
                id,
                payload,
                self.property_spills.as_ref(),
                self.property_keys(),
                work,
            )
        })
    }

    pub(crate) fn checkpoint_relationship(
        &self,
        id: RelId,
        work: &CheckpointWorkContext,
    ) -> Result<Option<CheckpointRecord<RelRecord>>, CanonicalSegmentError> {
        self.checkpoint_record(
            CanonicalSegmentKind::Relationships,
            id.0,
            work,
            |id, payload| {
                checkpoint_decode::relationship(
                    id,
                    payload,
                    self.property_spills.as_ref(),
                    self.property_keys(),
                    work,
                )
            },
        )
    }

    fn checkpoint_record<T>(
        &self,
        kind: CanonicalSegmentKind,
        id: u64,
        work: &CheckpointWorkContext,
        mut decode: impl FnMut(u64, &[u8]) -> Result<T, CanonicalSegmentError>,
    ) -> Result<Option<T>, CanonicalSegmentError> {
        self.ensure_healthy()?;
        let result = (|| {
            work.checkpoint()?;
            let (descriptor, _) = {
                let unit = work.start_unit()?;
                let wave = work.io_wave()?;
                let result = self.find_descriptor_for_id(kind, id);
                drop(wave);
                let descriptor = result?;
                unit.finish();
                descriptor
            };
            work.checkpoint()?;
            let Some(descriptor) = descriptor else {
                return Ok(None);
            };
            self.validate_selected_descriptor(&descriptor)?;
            work.checkpoint()?;
            let range = SegmentReadRange::new(
                self.manifest.artifact_id,
                descriptor.segment_id,
                descriptor.offset,
                descriptor.length,
            )
            .with_content_digest(descriptor.content_digest);
            // Reuse the captured-handle reader. Its admitted input and scratch
            // survive the framing walk and bypass the serving segment cache.
            let bytes = self
                .range_reader
                .checkpoint_range_with_work_context(&range, work)
                .map_err(|error| match error {
                    CheckpointRangeReadError::Read(error) => CanonicalSegmentError::Read(error),
                    CheckpointRangeReadError::Work(error) => CanonicalSegmentError::Work(error),
                })?;
            let mut found = None;
            decode_segment_records_control(
                &bytes,
                self.manifest.generation,
                &descriptor,
                |record_id, payload| {
                    let unit = work.start_unit()?;
                    let ordering = record_id.cmp(&id);
                    unit.finish();
                    work.checkpoint()?;
                    if ordering.is_lt() {
                        return Ok(CanonicalScanControl::Continue);
                    }
                    if ordering.is_eq() {
                        found = Some(decode(record_id, payload)?);
                    }
                    Ok(CanonicalScanControl::Stop)
                },
            )?;
            work.checkpoint()?;
            Ok(found)
        })();
        // Work denial/cancellation is recoverable and never poisons the shared
        // source. Physical corruption retains the ordinary fail-closed policy.
        self.poison_on_physical_failure(&result);
        result
    }
}
