// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Rank access over an admitted descriptor prefix. Only blocks containing
//! selected ordinals are decoded; decoded keys are reused within this probe.

use super::*;
use crate::join_sampling::{
    JoinSamplingContext, JoinSamplingRefusal, SamplingError, SamplingResult,
};
use crate::scan::AdmittedSegmentReadError;
use std::cell::OnceCell;

struct SamplingBlock {
    descriptor: CanonicalAdjacencyBlockDescriptor,
    end: u64,
    entries: OnceCell<Vec<crate::adjacency::OrderedAdjacencyEntry>>,
}

pub(crate) struct CanonicalSamplingPosting<'a> {
    reader: &'a CanonicalAdjacencyReader,
    blocks: Vec<SamplingBlock>,
    count: u64,
}

impl CanonicalSamplingPosting<'_> {
    pub(crate) fn len(&self) -> u64 {
        self.count
    }

    pub(crate) fn select(
        &self,
        ordinal: u64,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<crate::adjacency::OrderedAdjacencyEntry> {
        context.probe()?;
        if ordinal >= self.count {
            return Err(SamplingError::Refused(JoinSamplingRefusal::Arithmetic));
        }
        let index = self.blocks.partition_point(|block| block.end <= ordinal);
        let selected = &self.blocks[index];
        let start = selected.end - u64::from(selected.descriptor.record_count);
        if selected.entries.get().is_none() {
            let block = &selected.descriptor;
            context.prepare(block.record_count as usize)?;
            let mut entries = Vec::new();
            context.reserve(&mut entries, block.record_count as usize)?;
            context.memory(
                usize::try_from(block.length.get())
                    .map_err(|_| SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?,
            )?;
            if block.length.get() > self.reader.max_block_bytes.get() {
                return Err(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget));
            }
            let read = self
                .reader
                .range_reader
                .read_range_admitted(
                    &SegmentReadRange {
                        artifact_id: ARTIFACT_ID,
                        segment_ids: vec![block.block_id],
                        offset: block.offset,
                        length: block.length,
                        content_digest: Some(block.content_digest),
                    },
                    |bytes| context.read(bytes),
                )
                .map_err(|error| match error {
                    AdmittedSegmentReadError::Admission(error) => error,
                    AdmittedSegmentReadError::Read(error) => {
                        self.reader.sampling_error(error.into())
                    }
                })?;
            let mut stopped = None;
            let decoded =
                decode_block_inputs(&read.payload, self.reader.generation, block, |input| {
                    if let Err(error) = context.checkpoint() {
                        stopped = Some(error);
                        return Err(CanonicalAdjacencyError::Source(
                            "join sampling key decode stopped".into(),
                        ));
                    }
                    entries.push(crate::adjacency::OrderedAdjacencyEntry {
                        neighbor_id: input.neighbor_id,
                        relationship_id: input.relationship_id,
                    });
                    Ok(())
                });
            if let Some(error) = stopped {
                return Err(error);
            }
            decoded.map_err(|error| self.reader.sampling_error(error))?;
            selected.entries.set(entries).map_err(|_| {
                SamplingError::Storage(hawdb_core::HawDBError::StorageIntegrity(
                    "sampling posting block was initialized twice".into(),
                ))
            })?;
            context.checkpoint()?;
        }
        selected
            .entries
            .get()
            .and_then(|entries| entries.get((ordinal - start) as usize))
            .copied()
            .ok_or_else(|| {
                self.reader.sampling_error(CanonicalAdjacencyError::Corrupt(
                    "sampling ordinal exceeds the decoded adjacency block".into(),
                ))
            })
    }
}

impl CanonicalAdjacencyReader {
    pub(crate) fn sampling_posting(
        &self,
        endpoint: NodeId,
        direction: AdjacencyDirection,
        rel_type: RelTypeId,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<CanonicalSamplingPosting<'_>> {
        self.ensure_healthy()
            .map_err(|error| self.sampling_error(error))?;
        context.probe()?;
        let mut blocks = Vec::new();
        let mut count = 0u64;
        let mut append = |block: CanonicalAdjacencyBlockDescriptor| -> SamplingResult<()> {
            context.prepare(1)?;
            if block.endpoint != endpoint
                || block.direction != direction
                || block.rel_type != rel_type
            {
                return Err(self.sampling_error(CanonicalAdjacencyError::Corrupt(
                    "sampling descriptor is outside the selected adjacency group".into(),
                )));
            }
            count = count
                .checked_add(u64::from(block.record_count))
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))?;
            context.reserve(&mut blocks, 1)?;
            blocks.push(SamplingBlock {
                descriptor: block,
                end: count,
                entries: OnceCell::new(),
            });
            Ok(())
        };
        match &self.descriptors {
            CanonicalAdjacencyDescriptorBackend::Resident(manifest) => {
                let group = (direction_tag(direction), endpoint.0, rel_type.0);
                let key = |block: &CanonicalAdjacencyBlockDescriptor| {
                    (
                        direction_tag(block.direction),
                        block.endpoint.0,
                        block.rel_type.0,
                    )
                };
                let start = manifest.blocks.partition_point(|block| key(block) < group);
                let end = manifest.blocks.partition_point(|block| key(block) <= group);
                for block in &manifest.blocks[start..end] {
                    append(block.clone())?;
                }
            }
            CanonicalAdjacencyDescriptorBackend::Demand(demand) => {
                let prefix = descriptor_prefix(endpoint, direction, Some(rel_type));
                let admit = |bytes| context.bridge_read(bytes);
                let visit = |bytes| context.bridge_visit(bytes);
                let mut failure = None;
                let result = demand.scan_prefix_admitted(
                    &prefix,
                    GraphDescriptorTreeReadLimits::default(),
                    &admit,
                    &visit,
                    |key, value| {
                        let step = CanonicalAdjacencyBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        )
                        .map_err(|error| self.sampling_error(error))
                        .and_then(&mut append);
                        match step {
                            Ok(()) => Ok(GraphDescriptorTreeScanControl::Continue),
                            Err(error) => {
                                failure = Some(error);
                                Ok(GraphDescriptorTreeScanControl::Stop)
                            }
                        }
                    },
                );
                if let Err(error) = result {
                    return Err(
                        if matches!(&error, GraphDescriptorTreeError::Admission(_)) {
                            context.descriptor_error(error)
                        } else {
                            self.sampling_error(error.into())
                        },
                    );
                }
                if let Some(error) = failure {
                    return Err(error);
                }
            }
        }
        context.checkpoint()?;
        Ok(CanonicalSamplingPosting {
            reader: self,
            blocks,
            count,
        })
    }

    fn sampling_error(&self, error: CanonicalAdjacencyError) -> SamplingError {
        let message = error.to_string();
        self.poison_on_physical_failure::<()>(&Err(error));
        SamplingError::Storage(hawdb_core::HawDBError::StorageIntegrity(message))
    }
}
