// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded equality-domain preparation without hydrating canonical node rows.

use super::*;
use crate::join_sampling::{
    JoinSamplingContext, JoinSamplingRefusal, SamplingError, SamplingResult,
};
use crate::scan::AdmittedSegmentReadError;

impl PersistentPropertyProjectionReader {
    pub(crate) fn sampling_equality_ids(
        &self,
        label: LabelId,
        property: &str,
        expected: &Value,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<Vec<NodeId>> {
        self.ensure_healthy()
            .map_err(|error| self.sampling_error(error))?;
        let kind = PersistentPropertyProjectionKind::Equality;
        if !self.manifest.supports(label, property, kind) {
            return Err(SamplingError::Refused(JoinSamplingRefusal::Unsupported));
        }
        let mut ids = Vec::new();
        let mut failure = None;
        let prefix = property_projection_descriptor_prefix(kind, label, property);
        let admit = |bytes| context.bridge_read(bytes);
        let visit = |bytes| context.bridge_visit(bytes);
        let result = self.descriptor_reader.scan_prefix_admitted(
            &prefix,
            GraphDescriptorTreeReadLimits::default(),
            &admit,
            &visit,
            |key, value| {
                let step = (|| {
                    context.prepare(1)?;
                    let descriptor_bytes = super::ordered::descriptor_memory(value, property, 0)
                        .map_err(|error| self.sampling_error(error))?;
                    context.memory(descriptor_bytes)?;
                    let block =
                        PersistentPropertyProjectionBlockDescriptor::decode_descriptor_tree_entry(
                            key, value,
                        )
                        .map_err(|error| self.sampling_error(error))?;
                    self.validate_selected_descriptor(&block, kind, label, property)
                        .map_err(|error| self.sampling_error(error))?;
                    if expected < &block.min_key || expected > &block.max_key {
                        return Ok(());
                    }
                    context.prepare(block.entry_count as usize)?;
                    // Both the allocation preflight and value decode examine
                    // every entry; neither pass is hidden from admission.
                    context.prepare(block.entry_count as usize)?;
                    let scratch = usize::try_from(block.length.get())
                        .ok()
                        .and_then(|bytes| bytes.checked_mul(2))
                        .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
                    context.memory(scratch)?;
                    if block.length.get() > self.max_block_bytes.get() {
                        return Err(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget));
                    }
                    let read = self
                        .range_reader
                        .read_range_admitted(
                            &SegmentReadRange {
                                artifact_id: self.manifest.artifact_id,
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
                                self.sampling_error(error.into())
                            }
                        })?;
                    let mut largest_value = 0;
                    let mut stopped = None;
                    let inspected = super::ordered::walk_entries(
                        &read.payload,
                        self.manifest.generation,
                        &block,
                        None,
                        |_, encoded| {
                            if let Err(error) = context.checkpoint() {
                                stopped = Some(error);
                                return Err(PersistentPropertyProjectionError::Source(
                                    "join sampling allocation preflight stopped".into(),
                                ));
                            }
                            largest_value = largest_value.max(
                                crate::canonical::standalone_value_allocation_bytes(encoded)?,
                            );
                            Ok(())
                        },
                    );
                    if let Some(error) = stopped {
                        return Err(error);
                    }
                    inspected.map_err(|error| self.sampling_error(error))?;
                    // Reuse the ordered-reader workspace bound: current value,
                    // first/previous keys and their validation copies coexist.
                    let workspace = largest_value
                        .checked_mul(6)
                        .and_then(|bytes| bytes.checked_add(block.property.len().checked_mul(4)?))
                        .ok_or(SamplingError::Refused(JoinSamplingRefusal::MemoryBudget))?;
                    context.memory(workspace)?;
                    let mut admission_error = None;
                    let decoded = decode_projection_block(
                        &read.payload,
                        self.manifest.generation,
                        &block,
                        |value, id| {
                            let admission = context.checkpoint().and_then(|()| {
                                if value == *expected {
                                    context.reserve(&mut ids, 1)?;
                                    ids.push(id);
                                }
                                Ok(())
                            });
                            if let Err(error) = admission {
                                admission_error = Some(error);
                                return Err(PersistentPropertyProjectionError::Source(
                                    "join sampling value decode stopped".into(),
                                ));
                            }
                            Ok(())
                        },
                    );
                    if let Some(error) = admission_error {
                        return Err(error);
                    }
                    decoded.map_err(|error| self.sampling_error(error))?;
                    context.checkpoint()
                })();
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
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(
                self.sampling_error(PersistentPropertyProjectionError::Corrupt(
                    "equality sampling domain contains duplicate node identities".into(),
                )),
            );
        }
        context.checkpoint()?;
        Ok(ids)
    }

    fn sampling_error(&self, error: PersistentPropertyProjectionError) -> SamplingError {
        let message = error.to_string();
        self.poison_on_physical_failure::<()>(&Err(error));
        SamplingError::Storage(hawdb_core::HawDBError::StorageIntegrity(message))
    }
}
