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

//! Cooperative checkpoint-only WAL decoding. Ordinary decoding stays independent.
//! Field inventory borrows the record instead of allocating duplicate fields.
//! Decoded strings, byte buffers and vectors retain admitted allocation owners.
//! Map nodes/comparisons, replay-created allocations, Arc finalization and
//! cancellation cleanup/destruction still require resource qualification.

use super::*;
use crate::background::{CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError};

mod map_memory;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod memory_tests;

#[cfg(test)]
mod memory_related_tests;

#[cfg(test)]
mod map_memory_tests;

#[cfg(test)]
mod map_related_tests;

#[cfg(test)]
mod replay_work_tests;

#[cfg(test)]
mod replay_related_tests;

#[cfg(test)]
mod replay_scan_tests;

#[cfg(test)]
mod replay_scan_related_tests;

#[cfg(test)]
mod replay_search_scan_tests;

#[cfg(test)]
mod replay_search_related_tests;

pub(crate) fn decode_binary_wal_record_with_work_context(
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<BinaryWalRecordDecode<crate::wal::checkpoint::CheckpointWalEntry>> {
    match work.classify(|work| {
        let context = DecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::new(Default::default()),
        };
        let decoded = decode_binary_wal_record_inner(bytes, &context)?;
        context
            .checkpoint()
            .map_err(HawDBError::from_storage_error)?;
        match decoded {
            BinaryWalRecordDecode::Entry {
                entry,
                commit_epoch,
            } => Ok(BinaryWalRecordDecode::Entry {
                entry: crate::wal::checkpoint::CheckpointWalEntry::new(
                    entry,
                    context.memory.into_inner(),
                ),
                commit_epoch,
            }),
            BinaryWalRecordDecode::Corrupt(reason) => Ok(BinaryWalRecordDecode::Corrupt(reason)),
        }
    }) {
        Ok(decoded) => Ok(decoded),
        Err(CheckpointOperationError::Operation(HawDBError::Storage(reason))) => {
            Ok(BinaryWalRecordDecode::Corrupt(reason))
        }
        Err(CheckpointOperationError::Operation(error)) => Err(error),
        Err(CheckpointOperationError::Work(error)) => Err(HawDBError::Execution(format!(
            "checkpoint WAL decoding stopped: {error}"
        ))),
    }
}

struct DecodeContext {
    work: CheckpointWorkContext,
    memory: std::cell::RefCell<crate::background::CheckpointAllocationOwner>,
}

impl std::ops::Deref for DecodeContext {
    type Target = CheckpointWorkContext;
    fn deref(&self) -> &Self::Target {
        &self.work
    }
}

impl DecodeContext {
    fn reserve(&self, bytes: usize) -> Result<crate::background::CheckpointAllocationToken> {
        self.memory
            .borrow_mut()
            .reserve(bytes, self)
            .map_err(HawDBError::from_storage_error)
    }
    fn find(&self, address: usize) -> Result<crate::background::CheckpointAllocationToken> {
        self.memory
            .borrow()
            .find(address, self)
            .map_err(HawDBError::from_storage_error)
    }
}

fn allocation(error: impl std::fmt::Display, bytes: usize, work: &DecodeContext) -> HawDBError {
    HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
        bytes: bytes as u64,
        reason: error.to_string(),
    }))
}

fn copy_bytes(bytes: &[u8], work: &DecodeContext) -> Result<Vec<u8>> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let token = work.reserve(bytes.len())?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes.len())
        .map_err(|error| allocation(error, bytes.len(), work))?;
    if output.capacity() != bytes.len() {
        return Err(allocation(
            "byte capacity differs from admitted capacity",
            bytes.len(),
            work,
        ));
    }
    token.address(output.as_ptr() as usize);
    unit.finish();
    for chunk in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        output.extend_from_slice(chunk);
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(output)
}

fn visit_utf8(bytes: &[u8], work: &DecodeContext, mut visit: impl FnMut(&str)) -> Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let end = offset.saturating_add(64 * 1024).min(bytes.len());
        let chunk = &bytes[offset..end];
        match std::str::from_utf8(chunk) {
            Ok(text) => {
                visit(text);
                offset = end;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                if error.error_len().is_some() || end == bytes.len() {
                    let index = offset + valid;
                    let reason = match error.error_len() {
                        Some(length) => {
                            format!("invalid utf-8 sequence of {length} bytes from index {index}")
                        }
                        None => format!("incomplete utf-8 byte sequence from index {index}"),
                    };
                    return Err(HawDBError::Storage(format!(
                        "wire string is not valid UTF-8: {reason}"
                    )));
                }
                // Carry at most three incomplete bytes into the next chunk.
                let prefix = std::str::from_utf8(&chunk[..valid]).expect("UTF-8 validated prefix");
                visit(prefix);
                offset += valid;
            }
        }
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

fn copy_string(bytes: &[u8], work: &DecodeContext) -> Result<String> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let token = work.reserve(bytes.len())?;
    let mut output = String::new();
    output
        .try_reserve_exact(bytes.len())
        .map_err(|error| allocation(error, bytes.len(), work))?;
    if output.capacity() != bytes.len() {
        return Err(allocation(
            "string capacity differs from admitted capacity",
            bytes.len(),
            work,
        ));
    }
    token.address(output.as_ptr() as usize);
    unit.finish();
    visit_utf8(bytes, work, |text| output.push_str(text))?;
    Ok(output)
}

fn decode_string_body(bytes: &[u8], pos: &mut usize, work: &DecodeContext) -> Result<String> {
    copy_string(decode_len_body(bytes, pos)?, work)
}

fn copy_arc(bytes: &[u8], work: &DecodeContext) -> Result<Arc<[u8]>> {
    let bytes = copy_bytes(bytes, work)?;
    let previous = work.find(bytes.as_ptr() as usize)?;
    let alignment = std::mem::align_of::<std::sync::atomic::AtomicUsize>();
    let capacity = bytes
        .len()
        .checked_add(2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
        .and_then(|bytes| bytes.checked_add(alignment - 1))
        .map(|bytes| bytes / alignment * alignment)
        .ok_or_else(|| allocation("WAL Arc capacity overflows usize", usize::MAX, work))?;
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let token = work.reserve(capacity)?;
    // Standard Arc finalization remains a CPU/allocation-latency assumption.
    let output: Arc<[u8]> = Arc::from(bytes);
    token.address(output.as_ptr() as usize);
    previous.release_buffer();
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(output)
}

fn push<T>(values: &mut Vec<T>, value: T, work: &DecodeContext) -> Result<()> {
    if values.len() == values.capacity() {
        let previous = if values.capacity() == 0 {
            None
        } else {
            Some(work.find(values.as_ptr() as usize)?)
        };
        let capacity = values
            .capacity()
            .saturating_mul(2)
            .max(values.len().saturating_add(1));
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| allocation("WAL vector capacity overflows usize", usize::MAX, work))?;
        let token = work.reserve(bytes)?;
        let mut replacement = Vec::new();
        replacement.try_reserve_exact(capacity).map_err(|error| {
            allocation(
                error,
                capacity.saturating_mul(std::mem::size_of::<T>()),
                work,
            )
        })?;
        if replacement.capacity() != capacity {
            return Err(allocation(
                "vector capacity differs from admitted capacity",
                bytes,
                work,
            ));
        }
        token.address(replacement.as_ptr() as usize);
        unit.finish();
        let mut old = std::mem::take(values).into_iter();
        let count = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
        while !old.as_slice().is_empty() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            replacement.extend(old.by_ref().take(count));
            unit.finish();
        }
        drop(old);
        if let Some(previous) = previous {
            previous.release_buffer();
        }
        *values = replacement;
    }
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    values.push(value);
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)
}

enum Field<'a> {
    Varint(u32, u64),
    String(u32, &'a [u8]),
    Message(u32, &'a [u8]),
    Unknown,
}
struct Fields<'a, 'w> {
    bytes: &'a [u8],
    pos: usize,
    strings: &'a [u32],
    messages: &'a [u32],
    work: &'w DecodeContext,
}
impl<'a> Fields<'a, '_> {
    fn next(&mut self) -> Result<Option<Field<'a>>> {
        if self.pos == self.bytes.len() {
            return Ok(None);
        }
        let unit = self
            .work
            .start_unit()
            .map_err(HawDBError::from_storage_error)?;
        let (id, wire) = decode_tag(self.bytes, &mut self.pos)?;
        let field = match wire {
            WIRE_TYPE_VARINT => Field::Varint(id, decode_varint_u64(self.bytes, &mut self.pos)?),
            WIRE_TYPE_LEN if self.strings.contains(&id) => {
                Field::String(id, decode_len_body(self.bytes, &mut self.pos)?)
            }
            WIRE_TYPE_LEN if self.messages.contains(&id) => {
                Field::Message(id, decode_len_body(self.bytes, &mut self.pos)?)
            }
            wire => {
                skip_field(self.bytes, &mut self.pos, wire)?;
                Field::Unknown
            }
        };
        unit.finish();
        Ok(Some(field))
    }
}

struct OpFields<'a, 'w> {
    bytes: &'a [u8],
    strings: &'a [u32],
    messages: &'a [u32],
    work: &'w DecodeContext,
}
impl<'a, 'w> OpFields<'a, 'w> {
    fn parse(
        bytes: &'a [u8],
        strings: &'a [u32],
        messages: &'a [u32],
        work: &'w DecodeContext,
    ) -> Result<Self> {
        let out = Self {
            bytes,
            strings,
            messages,
            work,
        };
        let mut fields = out.iter();
        while let Some(field) = fields.next()? {
            if let Field::String(_, bytes) = field {
                visit_utf8(bytes, work, |_| {})?;
            }
        }
        Ok(out)
    }
    fn iter(&self) -> Fields<'a, 'w> {
        Fields {
            bytes: self.bytes,
            pos: 0,
            strings: self.strings,
            messages: self.messages,
            work: self.work,
        }
    }
    fn required_string(&self, id: u32, name: &str) -> Result<String> {
        let mut fields = self.iter();
        while let Some(field) = fields.next()? {
            if let Field::String(found, bytes) = field
                && found == id
            {
                return copy_string(bytes, self.work);
            }
        }
        Err(HawDBError::Storage(format!("WAL op is missing {name}")))
    }
    fn strings_for(&self, id: u32) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut fields = self.iter();
        while let Some(field) = fields.next()? {
            if let Field::String(found, bytes) = field
                && found == id
            {
                push(&mut out, copy_string(bytes, self.work)?, self.work)?;
            }
        }
        Ok(out)
    }
    fn required_varint(&self, id: u32, name: &str) -> Result<u64> {
        let mut fields = self.iter();
        while let Some(field) = fields.next()? {
            if let Field::Varint(found, value) = field
                && found == id
            {
                return Ok(value);
            }
        }
        Err(HawDBError::Storage(format!("WAL op is missing {name}")))
    }
    fn required_message(&self, id: u32, name: &str) -> Result<&'a [u8]> {
        let mut fields = self.iter();
        while let Some(field) = fields.next()? {
            if let Field::Message(found, bytes) = field
                && found == id
            {
                return Ok(bytes);
            }
        }
        Err(HawDBError::Storage(format!("WAL op is missing {name}")))
    }
    fn properties_for(&self, id: u32) -> Result<BTreeMap<String, Value>> {
        let mut properties = BTreeMap::new();
        let mut memory = map_memory::MapMemory::default();
        let mut fields = self.iter();
        while let Some(field) = fields.next()? {
            if let Field::Message(found, bytes) = field
                && found == id
            {
                let (key, value) = decode_map_entry(bytes, 1, self.work)?;
                let unit = self
                    .work
                    .start_unit()
                    .map_err(HawDBError::from_storage_error)?;
                memory.before_insert(properties.len(), self.work)?;
                properties.insert(key, value);
                unit.finish();
            }
        }
        Ok(properties)
    }
}

fn decode_binary_wal_record_inner(
    bytes: &[u8],
    work: &DecodeContext,
) -> Result<BinaryWalRecordDecode> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    if bytes.len() < 21 {
        return Err(HawDBError::Storage(
            "binary WAL record envelope is truncated".to_string(),
        ));
    }
    let lsn = u64::from_le_bytes(bytes[0..8].try_into().expect("8-byte lsn"));
    let record_kind = bytes[8];
    let commit_epoch = u64::from_le_bytes(bytes[9..17].try_into().expect("8-byte commit epoch"));
    let op_count = u32::from_le_bytes(bytes[17..21].try_into().expect("4-byte op count"));
    let mut pos = 21usize;
    unit.finish();
    let op = match record_kind {
        RECORD_KIND_SINGLE => {
            if op_count != 1 {
                return Err(HawDBError::Storage(format!(
                    "single-op WAL record declares op_count {op_count}"
                )));
            }
            let op = decode_op_frame(bytes, &mut pos, work)?;
            if let WalOp::Batch(_) = op {
                return Err(HawDBError::Storage(
                    "single-op WAL record carries a batch envelope".to_string(),
                ));
            }
            op
        }
        RECORD_KIND_BATCH => {
            let mut ops = Vec::new();
            for _ in 0..op_count {
                push(&mut ops, decode_op_frame(bytes, &mut pos, work)?, work)?;
            }
            WalOp::Batch(ops)
        }
        kind => {
            return Err(HawDBError::Storage(format!(
                "unknown WAL record kind {kind}"
            )));
        }
    };
    if pos != bytes.len() {
        return Err(HawDBError::Storage(format!(
            "binary WAL record has {} trailing bytes",
            bytes.len() - pos
        )));
    }
    Ok(BinaryWalRecordDecode::Entry {
        entry: WalEntry { lsn, op },
        commit_epoch,
    })
}

fn decode_value_message(bytes: &[u8], depth: usize, work: &DecodeContext) -> Result<Value> {
    ensure_value_depth(depth)?;
    let mut pos = 0usize;
    let mut value: Option<Value> = None;
    let mut map_nodes = map_memory::MapMemory::default();
    while pos < bytes.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let (field_id, wire_type) = decode_tag(bytes, &mut pos)?;
        unit.finish();
        match (field_id, wire_type) {
            (VALUE_FIELD_NULL, WIRE_TYPE_VARINT) => {
                decode_varint_u64(bytes, &mut pos)?;
                value = Some(Value::Null);
            }
            (VALUE_FIELD_BOOL, WIRE_TYPE_VARINT) => {
                value = Some(Value::Bool(decode_varint_u64(bytes, &mut pos)? != 0));
            }
            (VALUE_FIELD_INT, WIRE_TYPE_VARINT) => {
                value = Some(Value::Int(zigzag_decode_i64(decode_varint_u64(
                    bytes, &mut pos,
                )?)));
            }
            (VALUE_FIELD_FLOAT, WIRE_TYPE_FIXED64) => {
                value = Some(Value::Float(f64::from_bits(decode_fixed64(
                    bytes, &mut pos,
                )?)));
            }
            (VALUE_FIELD_STRING, WIRE_TYPE_LEN) => {
                value = Some(Value::String(decode_string_body(bytes, &mut pos, work)?));
            }
            (VALUE_FIELD_BINARY, WIRE_TYPE_LEN) => {
                value = Some(Value::Binary(copy_bytes(
                    decode_len_body(bytes, &mut pos)?,
                    work,
                )?));
            }
            (VALUE_FIELD_UUID, WIRE_TYPE_LEN) => {
                let encoded = decode_len_body(bytes, &mut pos)?;
                let bytes: [u8; 16] = encoded.try_into().map_err(|_| {
                    HawDBError::Storage("WAL UUID value must contain 16 bytes".to_string())
                })?;
                value = Some(Value::Uuid(hawdb_core::Uuid::from_bytes(bytes)));
            }
            (VALUE_FIELD_ELEMENT, WIRE_TYPE_LEN) => {
                let body = decode_len_body(bytes, &mut pos)?;
                let list = match value.take() {
                    Some(Value::List(mut values)) => {
                        if !body.is_empty() {
                            push(
                                &mut values,
                                decode_value_message(body, depth.saturating_add(1), work)?,
                                work,
                            )?;
                        }
                        values
                    }
                    None | Some(_) => {
                        if body.is_empty() {
                            Vec::new()
                        } else {
                            {
                                let mut values = Vec::new();
                                push(
                                    &mut values,
                                    decode_value_message(body, depth.saturating_add(1), work)?,
                                    work,
                                )?;
                                values
                            }
                        }
                    }
                };
                value = Some(Value::List(list));
            }
            (VALUE_FIELD_MAP_ENTRY, WIRE_TYPE_LEN) => {
                let body = decode_len_body(bytes, &mut pos)?;
                let mut map = match value.take() {
                    Some(Value::Map(values)) => values,
                    None | Some(_) => {
                        map_nodes = map_memory::MapMemory::default();
                        BTreeMap::new()
                    }
                };
                if !body.is_empty() {
                    let (key, entry_value) = decode_map_entry(body, depth.saturating_add(2), work)?;
                    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                    map_nodes.before_insert(map.len(), work)?;
                    map.insert(key, entry_value);
                    unit.finish();
                }
                value = Some(Value::Map(map));
            }
            (_, wire_type) => skip_field(bytes, &mut pos, wire_type)?,
        }
    }
    value.ok_or_else(|| HawDBError::Storage("WAL value message is empty".to_string()))
}

fn decode_map_entry(
    bytes: &[u8],
    value_depth: usize,
    work: &DecodeContext,
) -> Result<(String, Value)> {
    let mut pos = 0usize;
    let mut key = None;
    let mut value = None;
    while pos < bytes.len() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let (field_id, wire_type) = decode_tag(bytes, &mut pos)?;
        unit.finish();
        match (field_id, wire_type) {
            (ENTRY_FIELD_KEY, WIRE_TYPE_LEN) => {
                key = Some(decode_string_body(bytes, &mut pos, work)?)
            }
            (ENTRY_FIELD_VALUE, WIRE_TYPE_LEN) => {
                value = Some(decode_value_message(
                    decode_len_body(bytes, &mut pos)?,
                    value_depth,
                    work,
                )?);
            }
            (_, wire_type) => skip_field(bytes, &mut pos, wire_type)?,
        }
    }
    match (key, value) {
        (Some(key), Some(value)) => Ok((key, value)),
        _ => Err(HawDBError::Storage(
            "WAL map entry is missing its key or value".to_string(),
        )),
    }
}

fn decode_op_frame(bytes: &[u8], pos: &mut usize, work: &DecodeContext) -> Result<WalOp> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let op_code = decode_varint_u64(bytes, pos)?;
    let body = decode_len_body(bytes, pos)?;
    unit.finish();
    decode_op_body(op_code, body, work)
}

fn decode_op_body(op_code: u64, body: &[u8], work: &DecodeContext) -> Result<WalOp> {
    match op_code {
        OP_CREATE_NODE_LABEL => {
            let fields = OpFields::parse(body, &[1], &[], work)?;
            Ok(WalOp::CreateNodeLabel {
                label: fields.required_string(1, "label")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TYPE => {
            let fields = OpFields::parse(body, &[1], &[], work)?;
            Ok(WalOp::CreateRelationshipType {
                rel_type: fields.required_string(1, "relationship type")?,
            })
        }
        OP_CREATE_NODE_TABLE => {
            let fields = OpFields::parse(body, &[1], &[], work)?;
            Ok(WalOp::CreateNodeTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TABLE => {
            let fields = OpFields::parse(body, &[1], &[], work)?;
            Ok(WalOp::CreateRelationshipTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_PROPERTY => {
            let fields = OpFields::parse(body, &[2, 3], &[], work)?;
            Ok(WalOp::CreateProperty {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                value_type: decode_property_type_code(fields.required_varint(4, "value type")?)?,
                nullable: fields.required_varint(5, "nullable flag")? != 0,
            })
        }
        OP_ALTER_TABLE_STATE => {
            let fields = OpFields::parse(body, &[2], &[], work)?;
            Ok(WalOp::AlterTableState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                state: decode_schema_object_state_code(fields.required_varint(3, "state")?)?,
            })
        }
        OP_ALTER_PROPERTY_STATE => {
            let fields = OpFields::parse(body, &[2, 3], &[], work)?;
            Ok(WalOp::AlterPropertyState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                state: decode_schema_object_state_code(fields.required_varint(4, "state")?)?,
            })
        }
        OP_GC_TABLE_DESCRIPTOR => {
            let fields = OpFields::parse(body, &[2], &[], work)?;
            Ok(WalOp::GcTableDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
            })
        }
        OP_GC_PROPERTY_DESCRIPTOR => {
            let fields = OpFields::parse(body, &[2, 3], &[], work)?;
            Ok(WalOp::GcPropertyDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
            })
        }
        OP_CREATE_INDEX => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_COMPOSITE_INDEX => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateCompositeIndex {
                label: fields.required_string(1, "label")?,
                properties: fields.strings_for(2)?,
            })
        }
        OP_CREATE_RANGE_INDEX => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateRangeIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_FULL_TEXT_INDEX => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateFullTextIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_UNIQUE_CONSTRAINT => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateUniqueConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateNodePropertyExistsConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateRelationshipUniqueConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT => {
            let fields = OpFields::parse(body, &[1, 2], &[], work)?;
            Ok(WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE => {
            let fields = OpFields::parse(body, &[2], &[3], work)?;
            Ok(WalOp::CreateNode {
                id: NodeId(fields.required_varint(1, "node id")?),
                label: fields.required_string(2, "label")?,
                properties: fields.properties_for(3)?,
            })
        }
        OP_CREATE_RELATIONSHIP => {
            let fields = OpFields::parse(body, &[4], &[5], work)?;
            Ok(WalOp::CreateRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
                source: NodeId(fields.required_varint(2, "source node id")?),
                target: NodeId(fields.required_varint(3, "target node id")?),
                rel_type: fields.required_string(4, "relationship type")?,
                properties: fields.properties_for(5)?,
            })
        }
        OP_SET_NODE_PROPERTY => {
            let fields = OpFields::parse(body, &[2], &[3], work)?;
            Ok(WalOp::SetNodeProperty {
                id: NodeId(fields.required_varint(1, "node id")?),
                property: fields.required_string(2, "property")?,
                value: decode_value_message(fields.required_message(3, "value")?, 1, work)?,
            })
        }
        OP_SET_RELATIONSHIP_PROPERTY => {
            let fields = OpFields::parse(body, &[2], &[3], work)?;
            Ok(WalOp::SetRelationshipProperty {
                id: RelId(fields.required_varint(1, "relationship id")?),
                property: fields.required_string(2, "property")?,
                value: decode_value_message(fields.required_message(3, "value")?, 1, work)?,
            })
        }
        OP_DELETE_NODE => {
            let fields = OpFields::parse(body, &[], &[], work)?;
            Ok(WalOp::DeleteNode {
                id: NodeId(fields.required_varint(1, "node id")?),
            })
        }
        OP_DELETE_RELATIONSHIP => {
            let fields = OpFields::parse(body, &[], &[], work)?;
            Ok(WalOp::DeleteRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
            })
        }
        OP_PROJECT_GRAPH => {
            let fields = OpFields::parse(body, &[1, 2, 3], &[], work)?;
            Ok(WalOp::ProjectGraph {
                name: fields.required_string(1, "projected graph name")?,
                node_labels: fields.strings_for(2)?,
                rel_types: fields.strings_for(3)?,
            })
        }
        OP_MARK_INITIAL_IMPORT_SOURCE => {
            let fields = OpFields::parse(body, &[1], &[], work)?;
            Ok(WalOp::MarkInitialImportSource {
                source_fingerprint: fields.required_string(1, "source fingerprint")?,
            })
        }
        OP_RELATIONAL => {
            let fields = OpFields::parse(body, &[], &[1], work)?;
            Ok(WalOp::Relational {
                record: copy_arc(fields.required_message(1, "relational record")?, work)?,
            })
        }
        OP_RELATIONAL_SNAPSHOT => {
            let fields = OpFields::parse(body, &[], &[1], work)?;
            Ok(WalOp::RelationalSnapshot {
                record: copy_arc(fields.required_message(1, "relational record")?, work)?,
            })
        }
        OP_APPEND => {
            let fields = OpFields::parse(body, &[], &[1], work)?;
            Ok(WalOp::Append {
                record: copy_arc(fields.required_message(1, "append record")?, work)?,
            })
        }
        op_code => Err(HawDBError::Storage(format!(
            "unknown WAL op code {op_code}"
        ))),
    }
}
