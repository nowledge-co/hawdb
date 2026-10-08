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

//! Count without allocation, admit exact capacity, then write borrowed values
//! directly into the retained payload. The ordinary codec remains the byte and
//! invalid-value diagnostic reference. Decoded cursor ownership is separate.

use super::*;
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};
use crate::canonical::CanonicalSegmentError;

#[cfg(test)]
mod tests;

enum Destination {
    Count(usize),
    Bytes(CheckpointBytes),
}

struct Output<'a> {
    destination: Destination,
    work: &'a CheckpointWorkContext,
}

impl<'a> Output<'a> {
    fn counter(work: &'a CheckpointWorkContext) -> Self {
        Self {
            destination: Destination::Count(0),
            work,
        }
    }

    fn length(&self) -> usize {
        match &self.destination {
            Destination::Count(length) => *length,
            Destination::Bytes(bytes) => bytes.len(),
        }
    }

    fn add_count(&mut self, amount: usize) -> Result<()> {
        let Destination::Count(length) = &mut self.destination else {
            unreachable!("only a counting output accepts an omitted body")
        };
        *length = length.checked_add(amount).ok_or_else(|| {
            HawDBError::from_storage_error(self.work.record_failure(
                CheckpointWorkError::Allocation {
                    bytes: u64::MAX,
                    reason: "binary WAL payload capacity overflows usize".into(),
                },
            ))
        })?;
        Ok(())
    }

    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        match &mut self.destination {
            Destination::Count(_) => {
                let unit = self
                    .work
                    .start_unit()
                    .map_err(HawDBError::from_storage_error)?;
                self.add_count(bytes.len())?;
                unit.finish();
                Ok(())
            }
            Destination::Bytes(output) => output
                .append(bytes, self.work)
                .map_err(HawDBError::from_storage_error),
        }
    }

    fn varint(&mut self, mut value: u64) -> Result<()> {
        let mut bytes = [0; 10];
        let mut length = 0;
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            bytes[length] = if value == 0 { byte } else { byte | 0x80 };
            length += 1;
            if value == 0 {
                break;
            }
        }
        self.raw(&bytes[..length])
    }

    fn tag(&mut self, field: u32, wire: u8) -> Result<()> {
        self.varint((u64::from(field) << 3) | u64::from(wire))
    }

    fn field_varint(&mut self, field: u32, value: u64) -> Result<()> {
        self.tag(field, WIRE_TYPE_VARINT)?;
        self.varint(value)
    }

    fn fixed64(&mut self, field: u32, bits: u64) -> Result<()> {
        self.tag(field, WIRE_TYPE_FIXED64)?;
        self.raw(&bits.to_le_bytes())
    }

    fn bytes(&mut self, field: u32, bytes: &[u8]) -> Result<()> {
        self.tag(field, WIRE_TYPE_LEN)?;
        self.varint(bytes.len() as u64)?;
        self.raw(bytes)
    }

    fn string(&mut self, field: u32, value: &str) -> Result<()> {
        self.bytes(field, value.as_bytes())
    }

    fn predicates(
        &mut self,
        field: u32,
        values: &BTreeMap<String, crate::projection::ProjectedRelationshipPredicate>,
    ) -> Result<()> {
        let work = self.work;
        let length = crate::projection::predicate_checkpoint::encoded_len(values, work)?;
        self.tag(field, WIRE_TYPE_LEN)?;
        self.varint(length as u64)?;
        match self.destination {
            Destination::Count(_) => self.add_count(length),
            Destination::Bytes(_) => {
                crate::projection::predicate_checkpoint::encode_into(values, work, |chunk| {
                    self.raw(chunk)
                })
            }
        }
    }

    fn message(
        &mut self,
        field: u32,
        mut encode: impl FnMut(&mut Output<'_>) -> Result<()>,
    ) -> Result<()> {
        let mut counter = Self::counter(self.work);
        encode(&mut counter)?;
        let length = counter.length();
        self.tag(field, WIRE_TYPE_LEN)?;
        self.varint(length as u64)?;
        match self.destination {
            Destination::Count(_) => self.add_count(length),
            Destination::Bytes(_) => encode(self),
        }
    }

    fn properties(&mut self, field: u32, values: &BTreeMap<String, Value>) -> Result<()> {
        for (key, value) in values {
            self.message(field, |out| encode_entry(key, value, out))?;
        }
        Ok(())
    }
}

fn encode_entry(key: &str, value: &Value, out: &mut Output<'_>) -> Result<()> {
    out.string(ENTRY_FIELD_KEY, key)?;
    out.message(ENTRY_FIELD_VALUE, |out| encode_value(value, out))
}

fn encode_value(value: &Value, out: &mut Output<'_>) -> Result<()> {
    match value {
        Value::Null => out.field_varint(VALUE_FIELD_NULL, 0),
        Value::Bool(value) => out.field_varint(VALUE_FIELD_BOOL, u64::from(*value)),
        Value::Int(value) => out.field_varint(VALUE_FIELD_INT, zigzag_encode_i64(*value)),
        Value::Float(value) => out.fixed64(VALUE_FIELD_FLOAT, value.to_bits()),
        Value::String(value) => out.string(VALUE_FIELD_STRING, value),
        Value::Binary(value) => out.bytes(VALUE_FIELD_BINARY, value),
        Value::Uuid(value) => out.bytes(VALUE_FIELD_UUID, value.as_bytes()),
        Value::List(values) => {
            if values.is_empty() {
                out.bytes(VALUE_FIELD_ELEMENT, &[])?;
            }
            for value in values {
                out.message(VALUE_FIELD_ELEMENT, |out| encode_value(value, out))?;
            }
            Ok(())
        }
        Value::Map(values) => {
            if values.is_empty() {
                out.bytes(VALUE_FIELD_MAP_ENTRY, &[])?;
            }
            for (key, value) in values {
                out.message(VALUE_FIELD_MAP_ENTRY, |out| encode_entry(key, value, out))?;
            }
            Ok(())
        }
    }
}

fn validate_value(value: &Value, work: &CheckpointWorkContext) -> Result<()> {
    crate::canonical::validate_property_value_with_work_context(value, work).map_err(|error| {
        match error {
            CanonicalSegmentError::Work(error) => HawDBError::from_storage_error(error),
            error => HawDBError::Storage(format!(
                "WAL value violates canonical storage limits: {error}"
            )),
        }
    })
}

fn validate_ops(ops: &[WalOp], work: &CheckpointWorkContext) -> Result<()> {
    for op in ops {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        unit.finish();
        match op {
            WalOp::CreateNode { properties, .. } | WalOp::CreateRelationship { properties, .. } => {
                for value in properties.values() {
                    validate_value(value, work)?;
                }
            }
            WalOp::SetNodeProperty { value, .. } | WalOp::SetRelationshipProperty { value, .. } => {
                validate_value(value, work)?
            }
            WalOp::Batch(ops) => validate_ops(ops, work)?,
            _ => {}
        }
    }
    Ok(())
}

fn encode_frame(op: &WalOp, out: &mut Output<'_>) -> Result<()> {
    let mut counter = Output::counter(out.work);
    let code = encode_op_body(op, &mut counter)?;
    let length = counter.length();
    out.varint(code)?;
    out.varint(length as u64)?;
    match out.destination {
        Destination::Count(_) => out.add_count(length),
        Destination::Bytes(_) => {
            encode_op_body(op, out)?;
            Ok(())
        }
    }
}

fn encode_record(entry: &WalEntry, commit_epoch: u64, out: &mut Output<'_>) -> Result<()> {
    out.raw(&entry.lsn.to_le_bytes())?;
    let (kind, count) = match &entry.op {
        WalOp::Batch(ops) => (RECORD_KIND_BATCH, ops.len() as u32),
        _ => (RECORD_KIND_SINGLE, 1),
    };
    out.raw(&[kind])?;
    out.raw(&commit_epoch.to_le_bytes())?;
    out.raw(&count.to_le_bytes())?;
    match &entry.op {
        WalOp::Batch(ops) => {
            for op in ops {
                encode_frame(op, out)?;
            }
        }
        op => encode_frame(op, out)?,
    }
    Ok(())
}

pub(crate) fn encode_binary_wal_record_with_work_context(
    entry: &WalEntry,
    commit_epoch: u64,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes> {
    validate_ops(std::slice::from_ref(&entry.op), work)?;
    let mut counter = Output::counter(work);
    encode_record(entry, commit_epoch, &mut counter)?;
    let length = counter.length();
    let bytes = CheckpointBytes::new(length, work).map_err(HawDBError::from_storage_error)?;
    let mut out = Output {
        destination: Destination::Bytes(bytes),
        work,
    };
    encode_record(entry, commit_epoch, &mut out)?;
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    debug_assert_eq!(out.length(), length);
    match out.destination {
        Destination::Bytes(bytes) => Ok(bytes),
        Destination::Count(_) => unreachable!(),
    }
}

fn encode_op_body(op: &WalOp, out: &mut Output<'_>) -> Result<u64> {
    let op_code = match op {
        WalOp::CreateNodeLabel { label } => {
            out.string(1, label)?;
            OP_CREATE_NODE_LABEL
        }
        WalOp::CreateRelationshipType { rel_type } => {
            out.string(1, rel_type)?;
            OP_CREATE_RELATIONSHIP_TYPE
        }
        WalOp::CreateNodeTable { name } => {
            out.string(1, name)?;
            OP_CREATE_NODE_TABLE
        }
        WalOp::CreateRelationshipTable { name } => {
            out.string(1, name)?;
            OP_CREATE_RELATIONSHIP_TABLE
        }
        WalOp::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => {
            out.field_varint(1, table_kind_code(*table_kind))?;
            out.string(2, table)?;
            out.string(3, property)?;
            out.field_varint(4, property_type_code(*value_type))?;
            out.field_varint(5, u64::from(*nullable))?;
            OP_CREATE_PROPERTY
        }
        WalOp::AlterTableState {
            table_kind,
            table,
            state,
        } => {
            out.field_varint(1, table_kind_code(*table_kind))?;
            out.string(2, table)?;
            out.field_varint(3, schema_object_state_code(*state))?;
            OP_ALTER_TABLE_STATE
        }
        WalOp::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => {
            out.field_varint(1, table_kind_code(*table_kind))?;
            out.string(2, table)?;
            out.string(3, property)?;
            out.field_varint(4, schema_object_state_code(*state))?;
            OP_ALTER_PROPERTY_STATE
        }
        WalOp::GcTableDescriptor { table_kind, table } => {
            out.field_varint(1, table_kind_code(*table_kind))?;
            out.string(2, table)?;
            OP_GC_TABLE_DESCRIPTOR
        }
        WalOp::GcPropertyDescriptor {
            table_kind,
            table,
            property,
        } => {
            out.field_varint(1, table_kind_code(*table_kind))?;
            out.string(2, table)?;
            out.string(3, property)?;
            OP_GC_PROPERTY_DESCRIPTOR
        }
        WalOp::CreateIndex { label, property } => {
            out.string(1, label)?;
            out.string(2, property)?;
            OP_CREATE_INDEX
        }
        WalOp::CreateCompositeIndex { label, properties } => {
            out.string(1, label)?;
            for property in properties {
                out.string(2, property)?;
            }
            OP_CREATE_COMPOSITE_INDEX
        }
        WalOp::CreateRangeIndex { label, property } => {
            out.string(1, label)?;
            out.string(2, property)?;
            OP_CREATE_RANGE_INDEX
        }
        WalOp::CreateFullTextIndex { label, property } => {
            out.string(1, label)?;
            out.string(2, property)?;
            OP_CREATE_FULL_TEXT_INDEX
        }
        WalOp::CreateUniqueConstraint { label, property } => {
            out.string(1, label)?;
            out.string(2, property)?;
            OP_CREATE_UNIQUE_CONSTRAINT
        }
        WalOp::CreateNodePropertyExistsConstraint { label, property } => {
            out.string(1, label)?;
            out.string(2, property)?;
            OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT
        }
        WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
            out.string(1, rel_type)?;
            out.string(2, property)?;
            OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT
        }
        WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            out.string(1, rel_type)?;
            out.string(2, property)?;
            OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT
        }
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => {
            out.field_varint(1, id.0)?;
            out.string(2, label)?;
            out.properties(3, properties)?;
            OP_CREATE_NODE
        }
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => {
            out.field_varint(1, id.0)?;
            out.field_varint(2, source.0)?;
            out.field_varint(3, target.0)?;
            out.string(4, rel_type)?;
            out.properties(5, properties)?;
            OP_CREATE_RELATIONSHIP
        }
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => {
            out.field_varint(1, id.0)?;
            out.string(2, property)?;
            out.message(3, |out| encode_value(value, out))?;
            OP_SET_NODE_PROPERTY
        }
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => {
            out.field_varint(1, id.0)?;
            out.string(2, property)?;
            out.message(3, |out| encode_value(value, out))?;
            OP_SET_RELATIONSHIP_PROPERTY
        }
        WalOp::DeleteNode { id } => {
            out.field_varint(1, id.0)?;
            OP_DELETE_NODE
        }
        WalOp::DeleteRelationship { id } => {
            out.field_varint(1, id.0)?;
            OP_DELETE_RELATIONSHIP
        }
        WalOp::ProjectGraph {
            name,
            node_labels,
            rel_types,
            relationship_predicates,
        } => {
            out.string(1, name)?;
            for label in node_labels {
                out.string(2, label)?;
            }
            for rel_type in rel_types {
                out.string(3, rel_type)?;
            }
            out.predicates(4, relationship_predicates)?;
            OP_PROJECT_GRAPH
        }
        WalOp::MarkInitialImportSource { source_fingerprint } => {
            out.string(1, source_fingerprint)?;
            OP_MARK_INITIAL_IMPORT_SOURCE
        }
        WalOp::Relational { record } => {
            out.bytes(1, record)?;
            OP_RELATIONAL
        }
        WalOp::RelationalSnapshot { record } => {
            out.bytes(1, record)?;
            OP_RELATIONAL_SNAPSHOT
        }
        WalOp::Append { record } => {
            out.bytes(1, record)?;
            OP_APPEND
        }
        WalOp::Batch(_) => {
            return Err(HawDBError::Storage(
                "nested WAL batches cannot be encoded".to_string(),
            ));
        }
    };
    Ok(op_code)
}
