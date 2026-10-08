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

    fn string_field(&self, cursor: &mut SliceCursor<'_>) -> Result<String, CanonicalSegmentError> {
        let unit = self.work.start_unit()?;
        let length = cursor.read_u32()? as usize;
        let bytes = cursor.read_exact(length)?;
        unit.finish();
        self.work.checkpoint()?;
        self.string(bytes)
    }

    fn properties(
        &self,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
        spills: Option<&PropertySpillReader>,
        keys: Option<&[String]>,
    ) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
        let unit = self.work.start_unit()?;
        let count = cursor.read_u32()? as usize;
        count_fits(cursor, count, 5)?;
        unit.finish();
        self.work.checkpoint()?;
        let mut output = BTreeMap::new();
        let mut admitted_nodes = 0;
        for _ in 0..count {
            let key = if let Some(keys) = keys {
                let unit = self.work.start_unit()?;
                let key_id = cursor.read_u32()?;
                let key = keys.get(key_id as usize).ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(format!(
                        "canonical record references unknown property key id {key_id}"
                    ))
                })?;
                unit.finish();
                self.work.checkpoint()?;
                self.string(key.as_bytes())?
            } else {
                self.string_field(cursor)?
            };
            let value = self.value(cursor, depth, spills)?;
            let unit = self.work.start_unit()?;
            self.tree_node::<String, Value>(output.len(), &mut admitted_nodes)?;
            if output.insert(key, value).is_some() {
                return Err(CanonicalSegmentError::Corrupt(
                    "canonical property map has duplicate keys".into(),
                ));
            }
            unit.finish();
            self.work.checkpoint()?;
        }
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

    fn value(
        &self,
        cursor: &mut SliceCursor<'_>,
        depth: usize,
        spills: Option<&PropertySpillReader>,
    ) -> Result<Value, CanonicalSegmentError> {
        let unit = self.work.start_unit()?;
        ensure_depth(depth)?;
        let tag = cursor.read_u8()?;
        let value = match tag {
            0 => Value::Null,
            1 => match cursor.read_u8()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                n => {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "invalid canonical boolean {n}"
                    )));
                }
            },
            2 => Value::Int(cursor.read_i64()?),
            3 => Value::Float(f64::from_bits(cursor.read_u64()?)),
            4 => {
                unit.finish();
                self.work.checkpoint()?;
                return self.string_field(cursor).map(Value::String);
            }
            5 => {
                let count = cursor.read_u32()? as usize;
                count_fits(cursor, count, 1)?;
                let bytes = count.checked_mul(size_of::<Value>()).ok_or_else(|| {
                    self.allocation("canonical list capacity overflow", usize::MAX)
                })?;
                self.reserve(bytes)?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(count)
                    .map_err(|e| self.allocation(e, bytes))?;
                if values.capacity() != count {
                    return Err(self.allocation("canonical list capacity exceeds admission", bytes));
                }
                unit.finish();
                self.work.checkpoint()?;
                for _ in 0..count {
                    values.push(self.value(cursor, depth.saturating_add(1), spills)?);
                }
                return Ok(Value::List(values));
            }
            6 => {
                unit.finish();
                self.work.checkpoint()?;
                return self
                    .properties(cursor, depth.saturating_add(1), spills, None)
                    .map(Value::Map);
            }
            7 => {
                let id = cursor.read_u64()?;
                let reader = spills.ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(format!(
                        "canonical value references property spill {id} without a published spill artifact"
                    ))
                })?;
                unit.finish();
                self.work.checkpoint()?;
                let encoded = reader
                    .checkpoint_value(id, self.work)
                    .map_err(|e| match e {
                        PropertySpillError::Work(e) => CanonicalSegmentError::Work(e),
                        e => CanonicalSegmentError::PropertySpill(e),
                    })?
                    .ok_or_else(|| {
                        CanonicalSegmentError::Corrupt(format!(
                            "canonical value references missing property spill {id}"
                        ))
                    })?;
                let mut spilled = SliceCursor::new(&encoded);
                let value = self.value(&mut spilled, depth, None)?;
                if !spilled.is_empty() {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "property spill {id} has trailing bytes"
                    )));
                }
                return Ok(value);
            }
            8 => {
                let length = cursor.read_u32()? as usize;
                let bytes = cursor.read_exact(length)?;
                unit.finish();
                self.work.checkpoint()?;
                return self.bytes(bytes).map(Value::Binary);
            }
            9 => Value::Uuid(hawdb_core::Uuid::from_bytes(
                cursor
                    .read_exact(16)?
                    .try_into()
                    .expect("fixed UUID length"),
            )),
            tag => {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "unknown canonical value tag {tag}"
                )));
            }
        };
        unit.finish();
        self.work.checkpoint()?;
        Ok(value)
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
    let mut cursor = SliceCursor::new(payload);
    let unit = work.start_unit()?;
    let count = cursor.read_u32()? as usize;
    count_fits(&cursor, count, 4)?;
    unit.finish();
    work.checkpoint()?;
    let mut labels = BTreeSet::new();
    let mut admitted = 0;
    for _ in 0..count {
        let unit = work.start_unit()?;
        let label = LabelId(cursor.read_u32()?);
        if !labels.contains(&label) {
            decoder.tree_node::<LabelId, ()>(labels.len(), &mut admitted)?;
            labels.insert(label);
        }
        unit.finish();
        work.checkpoint()?;
    }
    let properties = decoder.properties(&mut cursor, 1, spills, Some(keys))?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "node record has trailing bytes".into(),
        ));
    }
    decoder.finish(NodeRecord {
        id: NodeId(id),
        labels,
        properties,
    })
}

pub(super) fn relationship(
    id: u64,
    payload: &[u8],
    spills: Option<&PropertySpillReader>,
    keys: &[String],
    work: &CheckpointWorkContext,
) -> Result<CheckpointRecord<RelRecord>, CanonicalSegmentError> {
    let decoder = Decoder::new(work);
    let mut cursor = SliceCursor::new(payload);
    let unit = work.start_unit()?;
    let source = NodeId(cursor.read_u64()?);
    let target = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    unit.finish();
    work.checkpoint()?;
    let properties = decoder.properties(&mut cursor, 1, spills, Some(keys))?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "relationship record has trailing bytes".into(),
        ));
    }
    decoder.finish(RelRecord {
        id: RelId(id),
        source,
        target,
        rel_type,
        properties,
    })
}
