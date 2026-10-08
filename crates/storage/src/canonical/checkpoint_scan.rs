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

//! Checkpoint scans retain admitted decoded records and bypass serving caches.
//! The fixed descriptor cursor does not allocate or clone reader manifests.

use super::*;

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod tests;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointOperationError,
};
use crate::scan::CheckpointRangeReadError;

pub(crate) trait CheckpointCanonicalRecord: Sized {
    const KIND: CanonicalSegmentKind;
    fn decode(
        reader: &CanonicalSegmentReader,
        id: u64,
        payload: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointRecord<Self>, CanonicalSegmentError>;
}

impl CheckpointCanonicalRecord for NodeRecord {
    const KIND: CanonicalSegmentKind = CanonicalSegmentKind::Nodes;
    fn decode(
        reader: &CanonicalSegmentReader,
        id: u64,
        payload: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointRecord<Self>, CanonicalSegmentError> {
        checkpoint_decode::node(
            id,
            payload,
            reader.property_spills.as_ref(),
            reader.property_keys(),
            work,
        )
    }
}

impl CheckpointCanonicalRecord for RelRecord {
    const KIND: CanonicalSegmentKind = CanonicalSegmentKind::Relationships;
    fn decode(
        reader: &CanonicalSegmentReader,
        id: u64,
        payload: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointRecord<Self>, CanonicalSegmentError> {
        checkpoint_decode::relationship(
            id,
            payload,
            reader.property_spills.as_ref(),
            reader.property_keys(),
            work,
        )
    }
}

struct Records<T> {
    records: std::vec::IntoIter<CheckpointRecord<T>>,
    _memory: CheckpointAllocationOwner,
}

impl<T> Default for Records<T> {
    fn default() -> Self {
        Self {
            records: Vec::new().into_iter(),
            _memory: CheckpointAllocationOwner::default(),
        }
    }
}

pub(crate) struct CheckpointCanonicalIterator<'a, T> {
    reader: &'a CanonicalSegmentReader,
    work: CheckpointWorkContext,
    current: Records<T>,
    lower_bound: [u8; 17],
    lower_bound_len: Option<usize>,
    descriptors_seen: u64,
    expected_descriptors: u64,
    finished: bool,
    failed: bool,
}

impl<'a, T: CheckpointCanonicalRecord> CheckpointCanonicalIterator<'a, T> {
    pub(crate) fn new(reader: &'a CanonicalSegmentReader, work: &CheckpointWorkContext) -> Self {
        let mut lower_bound = [0; 17];
        lower_bound[0] = T::KIND.tag();
        Self {
            reader,
            work: work.clone(),
            current: Records::default(),
            lower_bound,
            lower_bound_len: Some(1),
            descriptors_seen: 0,
            expected_descriptors: reader.manifest.segment_count_for_kind(T::KIND),
            finished: false,
            failed: false,
        }
    }

    fn next_descriptor(&mut self) -> Result<Option<AdmittedDescriptor>, CanonicalSegmentError> {
        self.reader.ensure_healthy()?;
        self.work.checkpoint()?;
        if self.finished {
            return Ok(None);
        }
        let Some(length) = self.lower_bound_len else {
            if self.descriptors_seen == self.expected_descriptors {
                self.finished = true;
                return Ok(None);
            }
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor key space ended before its declared count".into(),
            ));
        };
        let mut selected = None;
        let mut error = None;
        let mut memory = CheckpointAllocationOwner::default();
        let read = self.reader.descriptor_reader.checkpoint_scan_from(
            &self.lower_bound[..length],
            GraphDescriptorTreeReadLimits {
                max_descriptors: NonZeroU64::MIN,
                ..Default::default()
            },
            &self.work,
            |key, value| {
                memory.reserve(value.len(), &self.work)?;
                match CanonicalSegmentDescriptor::decode_descriptor_tree_entry(key, value) {
                    Ok(descriptor) => {
                        let mut captured = [0; 17];
                        captured.copy_from_slice(key);
                        selected = Some((captured, descriptor));
                    }
                    Err(e) => error = Some(e),
                }
                Ok(GraphDescriptorTreeScanControl::Stop)
            },
        );
        if let Some(error) = error {
            return Err(error);
        }
        read.map_err(|error| match error {
            GraphDescriptorTreeError::Work(error) => CanonicalSegmentError::Work(error),
            error => CanonicalSegmentError::DescriptorTree(error),
        })?;
        let selected = match selected {
            Some((key, descriptor)) if descriptor.kind == T::KIND => {
                self.reader.validate_selected_descriptor(&descriptor)?;
                Some((key, descriptor))
            }
            Some((_, descriptor)) if descriptor.kind < T::KIND => {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical descriptor seek moved backwards across kinds".into(),
                ));
            }
            Some(_) | None => None,
        };
        if self.descriptors_seen == self.expected_descriptors {
            self.finished = true;
            if selected.is_some() {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "canonical descriptor kind {:?} contains more than the declared {} segments",
                    T::KIND,
                    self.expected_descriptors
                )));
            }
            return Ok(None);
        }
        let (key, descriptor) = selected.ok_or_else(|| {
            CanonicalSegmentError::Corrupt(format!(
                "canonical descriptor kind {:?} ended after {} of {} segments",
                T::KIND,
                self.descriptors_seen,
                self.expected_descriptors
            ))
        })?;
        self.descriptors_seen = self.descriptors_seen.checked_add(1).ok_or_else(|| {
            CanonicalSegmentError::Corrupt("canonical descriptor cursor count overflow".into())
        })?;
        self.lower_bound = key;
        self.lower_bound_len = None;
        for index in (0..key.len()).rev() {
            if key[index] != u8::MAX {
                self.lower_bound[index] += 1;
                self.lower_bound_len = Some(index + 1);
                break;
            }
        }
        if self.lower_bound_len.is_none() && self.descriptors_seen != self.expected_descriptors {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical descriptor key space ended before its declared count".into(),
            ));
        }
        Ok(Some(AdmittedDescriptor {
            descriptor,
            _memory: memory,
        }))
    }

    fn load(&self, descriptor: &AdmittedDescriptor) -> Result<Records<T>, CanonicalSegmentError> {
        self.work
            .classify(|work| {
                let range = SegmentReadRange::new(
                    self.reader.manifest.artifact_id,
                    descriptor.segment_id,
                    descriptor.offset,
                    descriptor.length,
                )
                .with_content_digest(descriptor.content_digest);
                let bytes = self
                    .reader
                    .range_reader
                    .checkpoint_range_with_work_context(&range, work)
                    .map_err(|error| match error {
                        CheckpointRangeReadError::Read(error) => CanonicalSegmentError::Read(error),
                        CheckpointRangeReadError::Work(error) => CanonicalSegmentError::Work(error),
                    })?;
                let allocation = CheckpointDecodeContext {
                    work: work.clone(),
                    memory: std::cell::RefCell::new(CheckpointAllocationOwner::default()),
                };
                let mut records = Vec::new();
                decode_segment_records_control(
                    &bytes,
                    self.reader.manifest.generation,
                    descriptor,
                    |id, payload| {
                        // Framing is fixed-width. Property/value decoding and array
                        // growth own separate bounded units, with no parent permit.
                        let unit = work.start_unit()?;
                        unit.finish();
                        let record = T::decode(self.reader, id, payload, work)?;
                        allocation
                            .push(&mut records, record)
                            .map_err(|error| CanonicalSegmentError::Source(error.to_string()))?;
                        Ok(CanonicalScanControl::Continue)
                    },
                )?;
                work.checkpoint()?;
                Ok(Records {
                    records: records.into_iter(),
                    _memory: allocation.memory.into_inner(),
                })
            })
            .map_err(|error| match error {
                CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
                CheckpointOperationError::Operation(error) => error,
            })
    }

    fn next_record(&mut self) -> Result<Option<CheckpointRecord<T>>, CanonicalSegmentError> {
        loop {
            let unit = self.work.start_unit()?;
            let record = self.current.records.next();
            unit.finish();
            if record.is_some() {
                return Ok(record);
            }
            // Release the exhausted array before admitting the next segment.
            self.current = Records::default();
            let Some(descriptor) = self.next_descriptor()? else {
                return Ok(None);
            };
            self.current = self.load(&descriptor)?;
        }
    }
}

impl<T: CheckpointCanonicalRecord> Iterator for CheckpointCanonicalIterator<'_, T> {
    type Item = Result<CheckpointRecord<T>, CanonicalSegmentError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.next_record() {
            Ok(record) => record.map(Ok),
            Err(error) => {
                self.reader.poison_error(&error);
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

struct AdmittedDescriptor {
    descriptor: CanonicalSegmentDescriptor,
    _memory: CheckpointAllocationOwner,
}
impl std::ops::Deref for AdmittedDescriptor {
    type Target = CanonicalSegmentDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.descriptor
    }
}
