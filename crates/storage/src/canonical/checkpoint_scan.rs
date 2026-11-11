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
    CheckpointAllocationOwner, CheckpointBytes, CheckpointDecodeContext, CheckpointOperationError,
    CheckpointVectorPush,
};
use crate::scan::{CheckpointRangeReadError, CheckpointRangeReadProgress};

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

struct SegmentLoad<T> {
    records: Vec<CheckpointRecord<T>>,
    pending_record: Option<CheckpointVectorPush<CheckpointRecord<T>>>,
    bytes: Option<CheckpointBytes>,
    range_read: Option<CheckpointRangeReadProgress>,
    framing: Option<SegmentRecordCursor>,
    frame: Option<SegmentRecordFrame>,
    validated: bool,
    allocation: Option<CheckpointDecodeContext>,
}

impl<T> Default for SegmentLoad<T> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            pending_record: None,
            bytes: None,
            range_read: None,
            framing: None,
            frame: None,
            validated: false,
            allocation: None,
        }
    }
}

pub(crate) struct CheckpointCanonicalIterator<'a, T> {
    reader: &'a CanonicalSegmentReader,
    work: CheckpointWorkContext,
    cursor: CheckpointCanonicalCursor<T>,
    failed: bool,
}

impl<'a, T: CheckpointCanonicalRecord> CheckpointCanonicalIterator<'a, T> {
    pub(crate) fn new(reader: &'a CanonicalSegmentReader, work: &CheckpointWorkContext) -> Self {
        Self {
            reader,
            work: work.clone(),
            cursor: CheckpointCanonicalCursor::new(reader),
            failed: false,
        }
    }
}

/// Shared task-admitted descriptor traversal for checkpoint and projection scans.
#[derive(Debug)]
pub(super) struct CheckpointDescriptorCursor {
    kind: CanonicalSegmentKind,
    lower_bound: [u8; 17],
    lower_bound_len: Option<usize>,
    descriptors_seen: u64,
    expected_descriptors: u64,
    finished: bool,
}

impl CheckpointDescriptorCursor {
    pub(super) fn new(reader: &CanonicalSegmentReader, kind: CanonicalSegmentKind) -> Self {
        let mut lower_bound = [0; 17];
        lower_bound[0] = kind.tag();
        Self {
            kind,
            lower_bound,
            lower_bound_len: Some(1),
            descriptors_seen: 0,
            expected_descriptors: reader.manifest.segment_count_for_kind(kind),
            finished: false,
        }
    }

    pub(super) fn next(
        &mut self,
        reader: &CanonicalSegmentReader,
        work: &CheckpointWorkContext,
    ) -> Result<Option<AdmittedDescriptor>, CanonicalSegmentError> {
        reader.ensure_healthy()?;
        work.checkpoint()?;
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
        let read = reader.descriptor_reader.checkpoint_scan_from(
            &self.lower_bound[..length],
            GraphDescriptorTreeReadLimits {
                max_descriptors: NonZeroU64::MIN,
                ..Default::default()
            },
            work,
            |key, value| {
                memory.reserve(value.len(), work)?;
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
            Some((key, descriptor)) if descriptor.kind == self.kind => {
                reader.validate_selected_descriptor(&descriptor)?;
                Some((key, descriptor))
            }
            Some((_, descriptor)) if descriptor.kind < self.kind => {
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
                    self.kind, self.expected_descriptors
                )));
            }
            return Ok(None);
        }
        let (key, descriptor) = selected.ok_or_else(|| {
            CanonicalSegmentError::Corrupt(format!(
                "canonical descriptor kind {:?} ended after {} of {} segments",
                self.kind, self.descriptors_seen, self.expected_descriptors
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
}

/// Resume with clones of the same captured reader/manifest. Segment selection
/// and completed record decoding are retained across safe denial. Individual
/// record decoding and the initial range read remain atomic attempts.
pub(crate) struct CheckpointCanonicalCursor<T> {
    manifest: Arc<CanonicalSegmentManifest>,
    pending_descriptor: Option<AdmittedDescriptor>,
    current: Records<T>,
    loading: SegmentLoad<T>,
    descriptors: CheckpointDescriptorCursor,
}

impl<T: CheckpointCanonicalRecord> CheckpointCanonicalCursor<T> {
    pub(crate) fn new(reader: &CanonicalSegmentReader) -> Self {
        Self {
            manifest: reader.manifest.clone(),
            pending_descriptor: None,
            current: Records::default(),
            loading: SegmentLoad::default(),
            descriptors: CheckpointDescriptorCursor::new(reader, T::KIND),
        }
    }

    fn load(
        &mut self,
        reader: &CanonicalSegmentReader,
        work: &CheckpointWorkContext,
    ) -> Result<Records<T>, CanonicalSegmentError> {
        work.classify(|scoped| {
            let descriptor = self.pending_descriptor.as_ref().expect("selected segment");
            self.loading.resume(reader, descriptor, scoped)?;
            Ok(Records {
                records: std::mem::take(&mut self.loading.records).into_iter(),
                _memory: self
                    .loading
                    .allocation
                    .as_ref()
                    .expect("completed allocation inventory")
                    .memory
                    .take(),
            })
        })
        .map_err(|error| match error {
            CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
            CheckpointOperationError::Operation(error) => error,
        })
    }

    pub(crate) fn next_record(
        &mut self,
        reader: &CanonicalSegmentReader,
        work: &CheckpointWorkContext,
    ) -> Result<Option<CheckpointRecord<T>>, CanonicalSegmentError> {
        if !Arc::ptr_eq(&reader.manifest, &self.manifest) {
            return Err(CanonicalSegmentError::Corrupt(
                "retained canonical scan source identity changed".into(),
            ));
        }
        loop {
            let unit = work.start_unit()?;
            let record = self.current.records.next();
            unit.finish();
            if record.is_some() {
                return Ok(record);
            }
            // Release the exhausted array before admitting the next segment.
            self.current = Records::default();
            if self.pending_descriptor.is_none() {
                self.pending_descriptor = self.descriptors.next(reader, work)?;
            }
            if self.pending_descriptor.is_none() {
                return Ok(None);
            }
            self.current = self.load(reader, work)?;
            self.loading = SegmentLoad::default();
            self.pending_descriptor = None;
        }
    }
}

impl<T: CheckpointCanonicalRecord> Iterator for CheckpointCanonicalIterator<'_, T> {
    type Item = Result<CheckpointRecord<T>, CanonicalSegmentError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.cursor.next_record(self.reader, &self.work) {
            Ok(record) => record.map(Ok),
            Err(error) => {
                self.reader.poison_error(&error);
                // This one-shot iterator cannot resume. Release the pending
                // descriptor here; reusable callers retain the cursor directly.
                self.cursor.pending_descriptor = None;
                self.cursor.loading = SegmentLoad::default();
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

impl<T: CheckpointCanonicalRecord> SegmentLoad<T> {
    fn resume(
        &mut self,
        reader: &CanonicalSegmentReader,
        descriptor: &AdmittedDescriptor,
        work: &CheckpointWorkContext,
    ) -> Result<(), CanonicalSegmentError> {
        // Keep the inventory inside the retained state through cancellation and
        // unwinding; cached arrays must not outlive their allocation leases.
        let context = self
            .allocation
            .get_or_insert_with(|| CheckpointDecodeContext {
                work: work.clone(),
                memory: std::cell::RefCell::default(),
            });
        context.work.rebind(work);
        if self.bytes.is_none() {
            let range = SegmentReadRange::new(
                reader.manifest.artifact_id,
                descriptor.segment_id,
                descriptor.offset,
                descriptor.length,
            )
            .with_content_digest(descriptor.content_digest);
            self.bytes = Some(
                reader
                    .range_reader
                    .checkpoint_range_with_preparation(&range, &mut self.range_read, context)
                    .map_err(|error| match error {
                        CheckpointRangeReadError::Read(error) => CanonicalSegmentError::Read(error),
                        CheckpointRangeReadError::Work(error) => CanonicalSegmentError::Work(error),
                    })?,
            );
        }
        let bytes = self.bytes.as_ref().expect("verified segment bytes");
        if self.framing.is_none() {
            let unit = context.start_unit()?;
            self.framing = Some(SegmentRecordCursor::new(
                bytes,
                reader.manifest.generation,
                descriptor,
            )?);
            unit.finish();
        }
        while !self.validated {
            if self.frame.is_none() {
                let unit = context.start_unit()?;
                self.frame = self
                    .framing
                    .as_ref()
                    .expect("segment framing")
                    .next(bytes, descriptor)?;
                self.validated = self.frame.is_none();
                unit.finish();
                if self.validated {
                    break;
                }
            }
            let frame = self.frame.expect("selected record frame");
            if self.pending_record.is_none() {
                let record = T::decode(
                    reader,
                    frame.id,
                    &bytes[frame.payload_start..frame.payload_end],
                    context,
                )?;
                self.pending_record = Some(CheckpointVectorPush::new(record));
            }
            let result = context.resume_push(&mut self.records, &mut self.pending_record);
            if self.pending_record.is_none() {
                // The append may have completed before reporting cancellation.
                self.framing
                    .as_mut()
                    .expect("segment framing")
                    .accept(frame);
                self.frame = None;
            }
            result.map_err(|error| CanonicalSegmentError::Source(error.to_string()))?;
        }
        context.checkpoint()?;
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct AdmittedDescriptor {
    descriptor: CanonicalSegmentDescriptor,
    _memory: CheckpointAllocationOwner,
}
impl std::ops::Deref for AdmittedDescriptor {
    type Target = CanonicalSegmentDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.descriptor
    }
}
