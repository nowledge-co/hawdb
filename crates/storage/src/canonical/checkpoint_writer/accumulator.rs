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
    memory: CheckpointAllocationOwner,
    work: CheckpointWorkContext,
    admitted: bool,
}

impl Deref for Accumulator {
    type Target = SegmentAccumulator;

    fn deref(&self) -> &Self::Target {
        &self.segment
    }
}

impl Accumulator {
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
            memory: CheckpointAllocationOwner::default(),
            work,
            admitted,
        }
    }

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
        if !self.admitted {
            let unit = self.work.start_unit()?;
            self.segment.push(record_id, payload, endpoints)?;
            unit.finish();
            return Ok(());
        }
        self.with_memory(|segment, context| {
            let unit = context.start_unit()?;
            let record_bytes = 12u64.saturating_add(payload.len() as u64);
            if record_bytes > segment.config.max_record_bytes.get() {
                return Err(CanonicalSegmentError::RecordTooLarge {
                    record_bytes,
                    max_bytes: segment.config.max_record_bytes.get(),
                });
            }
            let payload_bytes = u32_len(payload.len(), "canonical record")?;
            if record_id < segment.max_record_id && segment.record_count > 0 {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical records must be ordered by id".into(),
                ));
            }
            let count = segment.record_count.checked_add(1).ok_or_else(|| {
                CanonicalSegmentError::Corrupt("canonical segment record count overflow".into())
            })?;
            let required = segment
                .records
                .len()
                .checked_add(12)
                .and_then(|bytes| bytes.checked_add(payload.len()))
                .ok_or_else(|| {
                    CanonicalSegmentError::Corrupt("canonical segment byte count overflow".into())
                })?;
            unit.finish();
            capacity(&mut segment.records, required, context)?;
            append(&mut segment.records, &record_id.to_le_bytes(), context)?;
            append(&mut segment.records, &payload_bytes.to_le_bytes(), context)?;
            append(&mut segment.records, payload, context)?;
            if let Some((source_id, target_id)) = endpoints {
                context
                    .push(&mut segment.source_endpoint_keys, source_id)
                    .map_err(source)?;
                context
                    .push(&mut segment.target_endpoint_keys, target_id)
                    .map_err(source)?;
            }
            let unit = context.start_unit()?;
            segment.min_record_id.get_or_insert(record_id);
            segment.max_record_id = record_id;
            segment.record_count = count;
            unit.finish();
            context.checkpoint()?;
            Ok(())
        })
    }

    pub(in crate::canonical) fn add_node_properties(
        &mut self,
        node: &NodeRecord,
        work: Option<&CheckpointWorkContext>,
    ) -> Result<(), CanonicalSegmentError> {
        if !self.admitted {
            return self.segment.add_node_properties(node, work);
        }
        self.with_memory(|segment, context| {
            if node.properties.is_empty() {
                context.checkpoint()?;
                return Ok(());
            }
            for label in &node.labels {
                for (property, value) in &node.properties {
                    let key = checkpoint_bloom::key(*label, property, value, Some(context))?;
                    context
                        .push(&mut segment.node_property_keys, key)
                        .map_err(source)?;
                }
            }
            context.checkpoint()?;
            Ok(())
        })
    }

    pub(in crate::canonical) fn flush_with_work_context(
        self,
        file: &mut File,
        artifact_digest: &mut IntegrityHasher,
        offset: u64,
        work: Option<&CheckpointWorkContext>,
    ) -> Result<super::descriptor::Descriptor, CanonicalSegmentError> {
        let Self {
            segment,
            memory,
            work: owned_work,
            admitted,
        } = self;
        let result = if admitted {
            checkpoint_flush::flush_admitted(
                segment,
                file,
                artifact_digest,
                offset,
                work.unwrap_or(&owned_work),
            )
        } else {
            segment
                .flush_with_work_context(file, artifact_digest, offset, work)
                .map(|descriptor| {
                    super::descriptor::Descriptor::new(
                        descriptor,
                        CheckpointAllocationOwner::default(),
                    )
                })
        };
        // The consuming flush destroys segment data on success and every error.
        drop(memory);
        drop(owned_work);
        result
    }
}

fn capacity(
    bytes: &mut Vec<u8>,
    required: usize,
    context: &CheckpointDecodeContext,
) -> Result<(), CanonicalSegmentError> {
    if required <= bytes.capacity() {
        return Ok(());
    }
    let previous = if bytes.capacity() == 0 {
        None
    } else {
        Some(context.find(bytes.as_ptr() as usize).map_err(source)?)
    };
    let capacity = bytes.capacity().saturating_mul(2).max(required);
    let unit = context.start_unit()?;
    let token = context.reserve(capacity).map_err(source)?;
    let mut replacement = Vec::new();
    replacement.try_reserve_exact(capacity).map_err(|error| {
        source(crate::background::checkpoint_decode_allocation(
            error, capacity, context,
        ))
    })?;
    if replacement.capacity() != capacity {
        return Err(source(crate::background::checkpoint_decode_allocation(
            "canonical segment capacity differs from admitted capacity",
            capacity,
            context,
        )));
    }
    token.address(replacement.as_ptr() as usize);
    unit.finish();
    append(&mut replacement, bytes, context)?;
    drop(std::mem::replace(bytes, replacement));
    if let Some(previous) = previous {
        previous.release_buffer();
    }
    context.checkpoint()?;
    Ok(())
}

fn append(
    bytes: &mut Vec<u8>,
    input: &[u8],
    context: &CheckpointDecodeContext,
) -> Result<(), CanonicalSegmentError> {
    for block in input.chunks(64 * 1024) {
        let unit = context.start_unit()?;
        bytes.extend_from_slice(block);
        unit.finish();
        context.checkpoint()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
