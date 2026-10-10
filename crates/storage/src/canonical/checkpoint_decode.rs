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

//! Private canonical decoding retains allocation admission with its record.
//! Standard BTree comparisons, allocator rounding and destruction latency are
//! still platform assumptions; descriptor/cache ownership is a separate ledger.

use super::*;
use crate::background::CheckpointAllocationOwner;
use std::cell::RefCell;
use std::mem::{align_of, size_of};
use std::ptr::NonNull;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod depth_tests;

#[derive(Debug)]
pub(crate) struct CheckpointRecord<T> {
    // Destroy data before its inventory, including after execution closes.
    record: T,
    _memory: CheckpointAllocationOwner,
}

impl<T> std::ops::Deref for CheckpointRecord<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.record
    }
}

impl<T> std::borrow::Borrow<T> for CheckpointRecord<T> {
    fn borrow(&self) -> &T {
        &self.record
    }
}

impl<T> CheckpointRecord<T> {
    // Compatibility adapters for the original plain-record regression probes.
    // Production estimation retains this wrapper through its final borrow.
    #[cfg(test)]
    pub(crate) fn into_record(self) -> T {
        self.record
    }
}

struct Decoder<'a> {
    work: &'a CheckpointWorkContext,
    memory: RefCell<CheckpointAllocationOwner>,
}

impl<'a> Decoder<'a> {
    fn new(work: &'a CheckpointWorkContext) -> Self {
        Self {
            work,
            memory: RefCell::new(CheckpointAllocationOwner::default()),
        }
    }

    fn reserve(&self, bytes: usize) -> Result<(), CanonicalSegmentError> {
        self.memory.borrow_mut().reserve(bytes, self.work)?;
        Ok(())
    }

    fn allocation(&self, reason: impl Display, bytes: usize) -> CanonicalSegmentError {
        self.work
            .record_failure(CheckpointWorkError::Allocation {
                bytes: bytes as u64,
                reason: reason.to_string(),
            })
            .into()
    }

    fn finish<T>(self, record: T) -> Result<CheckpointRecord<T>, CanonicalSegmentError> {
        self.work.checkpoint()?;
        Ok(CheckpointRecord {
            record,
            _memory: self.memory.into_inner(),
        })
    }

    fn bytes(&self, input: &[u8]) -> Result<Vec<u8>, CanonicalSegmentError> {
        let unit = self.work.start_unit()?;
        self.reserve(input.len())?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(input.len())
            .map_err(|e| self.allocation(e, input.len()))?;
        if output.capacity() != input.len() {
            return Err(self.allocation("canonical byte capacity exceeds admission", input.len()));
        }
        unit.finish();
        for chunk in input.chunks(64 * 1024) {
            let unit = self.work.start_unit()?;
            output.extend_from_slice(chunk);
            unit.finish();
        }
        self.work.checkpoint()?;
        Ok(output)
    }

    fn string(&self, input: &[u8]) -> Result<String, CanonicalSegmentError> {
        let unit = self.work.start_unit()?;
        self.reserve(input.len())?;
        let mut output = String::new();
        output
            .try_reserve_exact(input.len())
            .map_err(|e| self.allocation(e, input.len()))?;
        if output.capacity() != input.len() {
            return Err(self.allocation("canonical string capacity exceeds admission", input.len()));
        }
        unit.finish();
        let mut offset = 0;
        while offset < input.len() {
            let unit = self.work.start_unit()?;
            let end = offset.saturating_add(64 * 1024).min(input.len());
            let chunk = &input[offset..end];
            match std::str::from_utf8(chunk) {
                Ok(text) => {
                    output.push_str(text);
                    offset = end;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if error.error_len().is_some() || end == input.len() {
                        return Err(CanonicalSegmentError::Corrupt(format!(
                            "canonical string is not UTF-8 at byte {}: {error}",
                            offset + valid
                        )));
                    }
                    output.push_str(
                        std::str::from_utf8(&chunk[..valid]).expect("validated UTF-8 prefix"),
                    );
                    offset += valid;
                }
            }
            unit.finish();
        }
        self.work.checkpoint()?;
        Ok(output)
    }

    fn tree_node<K, V>(
        &self,
        len: usize,
        admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError> {
        // Pinned Rust1.97.1 B=6: eleven key/value slots, twelve children,
        // parent metadata and alignment. Cover a root and transient split;
        // other insertion-only nodes hold at least four keys.
        let alignment = align_of::<K>()
            .max(align_of::<V>())
            .max(align_of::<usize>());
        let node_bytes = size_of::<Option<NonNull<u8>>>()
            + 2 * size_of::<u16>()
            + 11 * (size_of::<K>() + size_of::<V>())
            + 12 * size_of::<NonNull<u8>>()
            + 8 * (alignment - 1);
        let nodes = len.saturating_add(1) / 4 + 2;
        if nodes > *admitted {
            let bytes = (nodes - *admitted)
                .checked_mul(node_bytes)
                .ok_or_else(|| self.allocation("canonical tree capacity overflow", usize::MAX))?;
            self.reserve(bytes)?;
            *admitted = nodes;
        }
        Ok(())
    }
}

fn count_fits(
    cursor: &SliceCursor<'_>,
    count: usize,
    minimum: usize,
) -> Result<(), CanonicalSegmentError> {
    if count > (cursor.bytes.len() - cursor.offset) / minimum {
        return Err(CanonicalSegmentError::Corrupt(
            "canonical record is truncated".into(),
        ));
    }
    Ok(())
}

pub(super) fn node(
    id: u64,
    payload: &[u8],
    spills: Option<&PropertySpillReader>,
    keys: &[String],
    work: &CheckpointWorkContext,
) -> Result<CheckpointRecord<NodeRecord>, CanonicalSegmentError> {
    let decoder = Decoder::new(work);
    let record = record_decode::node(id, payload, spills, Some(keys), &decoder)?;
    decoder.finish(record)
}

pub(super) fn relationship(
    id: u64,
    payload: &[u8],
    spills: Option<&PropertySpillReader>,
    keys: &[String],
    work: &CheckpointWorkContext,
) -> Result<CheckpointRecord<RelRecord>, CanonicalSegmentError> {
    let decoder = Decoder::new(work);
    let record = record_decode::relationship(id, payload, spills, Some(keys), &decoder)?;
    decoder.finish(record)
}

impl record_decode::Decoder for Decoder<'_> {
    type Unit = crate::background::CheckpointWorkUnit;

    fn start_unit(&self) -> Result<Self::Unit, CanonicalSegmentError> {
        self.work.start_unit().map_err(Into::into)
    }

    fn finish_unit(&self, unit: Self::Unit) -> Result<(), CanonicalSegmentError> {
        unit.finish();
        self.work.checkpoint().map_err(Into::into)
    }

    fn validate_count(
        &self,
        cursor: &SliceCursor<'_>,
        count: usize,
        minimum: usize,
    ) -> Result<(), CanonicalSegmentError> {
        count_fits(cursor, count, minimum)
    }

    fn string(&self, bytes: &[u8]) -> Result<String, CanonicalSegmentError> {
        self.string(bytes)
    }

    fn key(&self, key: &str) -> Result<String, CanonicalSegmentError> {
        self.string(key.as_bytes())
    }

    fn bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, CanonicalSegmentError> {
        self.bytes(bytes)
    }

    fn list(&self, count: usize) -> Result<Vec<Value>, CanonicalSegmentError> {
        let bytes = count
            .checked_mul(size_of::<Value>())
            .ok_or_else(|| self.allocation("canonical list capacity overflow", usize::MAX))?;
        self.reserve(bytes)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|error| self.allocation(error, bytes))?;
        if values.capacity() != count {
            return Err(self.allocation("canonical list capacity exceeds admission", bytes));
        }
        Ok(values)
    }

    fn insert_label(
        &self,
        labels: &mut BTreeSet<LabelId>,
        label: LabelId,
        admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError> {
        if !labels.contains(&label) {
            self.tree_node::<LabelId, ()>(labels.len(), admitted)?;
            labels.insert(label);
        }
        Ok(())
    }

    fn before_map_insert(
        &self,
        len: usize,
        admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError> {
        self.tree_node::<String, Value>(len, admitted)
    }

    fn visit_spill<T>(
        &self,
        reader: &PropertySpillReader,
        id: u64,
        visitor: impl FnOnce(Option<&[u8]>) -> Result<T, CanonicalSegmentError>,
    ) -> Result<T, CanonicalSegmentError> {
        let encoded = reader
            .checkpoint_value(id, self.work)
            .map_err(|error| match error {
                PropertySpillError::Work(error) => CanonicalSegmentError::Work(error),
                error => CanonicalSegmentError::PropertySpill(error),
            })?;
        visitor(encoded.as_deref())
    }
}
