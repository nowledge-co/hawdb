//! Binary record and op codec for the WAL (spec §3.4.3 + §3.5.1).
//!
//! # Record envelope
//!
//! One framed record (the payload of a fragment chain) carries one WAL
//! entry:
//!
//! ```text
//! lsn (8B LE) | record kind (1B) | commit_epoch (8B LE) | op_count (4B LE) | op frames
//! ```
//!
//! The LSN stays per record, as a fixed field ahead of the §3.4.3 batch
//! payload `commit_epoch (8B LE) | op_count (4B LE) | ops`, preserving the
//! existing LSN-contiguity checks. Record kind 0 is a single op
//! (`op_count` must be 1); kind 1 is a `WalOp::Batch`, whose children are
//! the op frames (`op_count` = child count), so `Batch([op])` and a bare
//! `op` stay distinguishable. The commit epoch is advisory for replay —
//! recovery derives commit epochs from LSN order exactly as before — and
//! is recorded for cross-tool debugging per §3.4.3.
//!
//! # Op frames
//!
//! ```text
//! op frame := varint op_code | varint body_len | body
//! body     := field-tagged fields (wire module); unknown field ids are
//!             skipped by wire type, unknown op codes fail closed
//! ```
//!
//! # Op-code and field-id table (append-only forever)
//!
//! Op codes and field ids are stable storage identifiers: never renumber,
//! never reuse. New ops take the next free code; new fields take the next
//! free id within their op. `WalOp::Batch` has no op code — it is the
//! record-kind-1 envelope.
//!
//! | code | op                                        | fields                                                                 |
//! |------|-------------------------------------------|------------------------------------------------------------------------|
//! | 1    | CreateNodeLabel                           | 1 label:str                                                            |
//! | 2    | CreateRelationshipType                    | 1 rel_type:str                                                         |
//! | 3    | CreateNodeTable                           | 1 name:str                                                             |
//! | 4    | CreateRelationshipTable                   | 1 name:str                                                             |
//! | 5    | CreateProperty                            | 1 table_kind:varint, 2 table:str, 3 property:str, 4 value_type:varint, 5 nullable:varint |
//! | 6    | AlterTableState                           | 1 table_kind:varint, 2 table:str, 3 state:varint                       |
//! | 7    | AlterPropertyState                        | 1 table_kind:varint, 2 table:str, 3 property:str, 4 state:varint       |
//! | 8    | GcTableDescriptor                         | 1 table_kind:varint, 2 table:str                                       |
//! | 9    | GcPropertyDescriptor                      | 1 table_kind:varint, 2 table:str, 3 property:str                       |
//! | 10   | CreateIndex                               | 1 label:str, 2 property:str                                            |
//! | 11   | CreateCompositeIndex                      | 1 label:str, 2 property:str (repeated)                                 |
//! | 12   | CreateRangeIndex                          | 1 label:str, 2 property:str                                            |
//! | 13   | CreateFullTextIndex                       | 1 label:str, 2 property:str                                            |
//! | 14   | CreateUniqueConstraint                    | 1 label:str, 2 property:str                                            |
//! | 15   | CreateNodePropertyExistsConstraint        | 1 label:str, 2 property:str                                            |
//! | 16   | CreateRelationshipUniqueConstraint        | 1 rel_type:str, 2 property:str                                         |
//! | 17   | CreateRelationshipPropertyExistsConstraint| 1 rel_type:str, 2 property:str                                         |
//! | 18   | CreateNode                                | 1 id:varint, 2 label:str, 3 property_entry:msg (repeated)              |
//! | 19   | CreateRelationship                        | 1 id:varint, 2 source:varint, 3 target:varint, 4 rel_type:str, 5 property_entry:msg (repeated) |
//! | 20   | SetNodeProperty                           | 1 id:varint, 2 property:str, 3 value:msg                               |
//! | 21   | SetRelationshipProperty                   | 1 id:varint, 2 property:str, 3 value:msg                               |
//! | 22   | DeleteNode                                | 1 id:varint                                                            |
//! | 23   | DeleteRelationship                        | 1 id:varint                                                            |
//! | 24   | ProjectGraph                              | 1 name:str, 2 node_label:str (repeated), 3 rel_type:str (repeated)     |
//! | 25   | MarkInitialImportSource                   | 1 source_fingerprint:str                                               |
//! | 26   | Relational                                | 1 record:bytes                                                         |
//! | 27   | RelationalSnapshot                        | 1 record:bytes                                                         |
//! | 28   | Append                                    | 1 record:bytes                                                         |
//!
//! Nested messages:
//!
//! - property_entry: `1 key:str, 2 value:msg`
//! - value: the encoder writes one kind of value using
//!   `1 null:varint(0), 2 bool:varint, 3 int:varint(zigzag),
//!   4 float:fixed64(bits), 5 string:str, 6 element:msg (repeated, list),
//!   7 map_entry:msg (repeated, property_entry shape), 8 binary:bytes,
//!   9 uuid:bytes (exactly 16 raw UUID bytes, not a string)`.
//!   Empty lists and maps use one zero-length field 6 or 7 respectively.
//!
//! Enum wire values (append-only): TableKind Node=0 Relationship=1;
//! PropertyType Any=0 Bool=1 Int=2 Float=3 String=4 List=5 Text=6;
//! SchemaObjectState DeleteOnly=0 WriteOnly=1 Backfill=2 Validating=3
//! Public=4 Gc=5.

use super::wire::{
    decode_fixed64, decode_len_body, decode_string_body, decode_tag, decode_varint_u64,
    encode_fixed64_field, encode_len_field, encode_string_field, encode_varint_field,
    encode_varint_u64, skip_field, zigzag_decode_i64, zigzag_encode_i64, WIRE_TYPE_FIXED64,
    WIRE_TYPE_LEN, WIRE_TYPE_VARINT,
};
use super::{validate_wal_op_values, WalEntry, WalOp};
use crate::canonical::MAX_VALUE_DEPTH;
use crate::{NodeId, RelId};
use skein_core::Value;
use skein_core::{PropertyType, SchemaObjectState, TableKind};
use skein_core::{Result, SkeinError};
use std::collections::BTreeMap;
use std::sync::Arc;

const RECORD_KIND_SINGLE: u8 = 0;
const RECORD_KIND_BATCH: u8 = 1;

const OP_CREATE_NODE_LABEL: u64 = 1;
const OP_CREATE_RELATIONSHIP_TYPE: u64 = 2;
const OP_CREATE_NODE_TABLE: u64 = 3;
const OP_CREATE_RELATIONSHIP_TABLE: u64 = 4;
const OP_CREATE_PROPERTY: u64 = 5;
const OP_ALTER_TABLE_STATE: u64 = 6;
const OP_ALTER_PROPERTY_STATE: u64 = 7;
const OP_GC_TABLE_DESCRIPTOR: u64 = 8;
const OP_GC_PROPERTY_DESCRIPTOR: u64 = 9;
const OP_CREATE_INDEX: u64 = 10;
const OP_CREATE_COMPOSITE_INDEX: u64 = 11;
const OP_CREATE_RANGE_INDEX: u64 = 12;
const OP_CREATE_FULL_TEXT_INDEX: u64 = 13;
const OP_CREATE_UNIQUE_CONSTRAINT: u64 = 14;
const OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT: u64 = 15;
const OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT: u64 = 16;
const OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT: u64 = 17;
const OP_CREATE_NODE: u64 = 18;
const OP_CREATE_RELATIONSHIP: u64 = 19;
const OP_SET_NODE_PROPERTY: u64 = 20;
const OP_SET_RELATIONSHIP_PROPERTY: u64 = 21;
const OP_DELETE_NODE: u64 = 22;
const OP_DELETE_RELATIONSHIP: u64 = 23;
const OP_PROJECT_GRAPH: u64 = 24;
const OP_MARK_INITIAL_IMPORT_SOURCE: u64 = 25;
const OP_RELATIONAL: u64 = 26;
const OP_RELATIONAL_SNAPSHOT: u64 = 27;
const OP_APPEND: u64 = 28;

const VALUE_FIELD_NULL: u32 = 1;
const VALUE_FIELD_BOOL: u32 = 2;
const VALUE_FIELD_INT: u32 = 3;
const VALUE_FIELD_FLOAT: u32 = 4;
const VALUE_FIELD_STRING: u32 = 5;
const VALUE_FIELD_ELEMENT: u32 = 6;
const VALUE_FIELD_MAP_ENTRY: u32 = 7;
const VALUE_FIELD_BINARY: u32 = 8;
const VALUE_FIELD_UUID: u32 = 9;

const ENTRY_FIELD_KEY: u32 = 1;
const ENTRY_FIELD_VALUE: u32 = 2;

pub enum BinaryWalRecordDecode {
    Entry {
        entry: WalEntry,
        #[allow(dead_code)]
        commit_epoch: u64,
    },
    Corrupt(String),
}

pub fn encode_binary_wal_record(entry: &WalEntry, commit_epoch: u64) -> Result<Vec<u8>> {
    validate_wal_op_values(std::slice::from_ref(&entry.op))?;
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&entry.lsn.to_le_bytes());
    match &entry.op {
        WalOp::Batch(ops) => {
            out.push(RECORD_KIND_BATCH);
            out.extend_from_slice(&commit_epoch.to_le_bytes());
            out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
            for op in ops {
                encode_op_frame(op, &mut out)?;
            }
        }
        op => {
            out.push(RECORD_KIND_SINGLE);
            out.extend_from_slice(&commit_epoch.to_le_bytes());
            out.extend_from_slice(&1u32.to_le_bytes());
            encode_op_frame(op, &mut out)?;
        }
    }
    Ok(out)
}

pub fn decode_binary_wal_record(bytes: &[u8]) -> Result<BinaryWalRecordDecode> {
    match decode_binary_wal_record_inner(bytes) {
        Ok(decoded) => Ok(decoded),
        Err(SkeinError::Storage(reason)) => Ok(BinaryWalRecordDecode::Corrupt(reason)),
        Err(error) => Err(error),
    }
}

fn decode_binary_wal_record_inner(bytes: &[u8]) -> Result<BinaryWalRecordDecode> {
    if bytes.len() < 21 {
        return Err(SkeinError::Storage(
            "binary WAL record envelope is truncated".to_string(),
        ));
    }
    let lsn = u64::from_le_bytes(bytes[0..8].try_into().expect("8-byte lsn"));
    let record_kind = bytes[8];
    let commit_epoch = u64::from_le_bytes(bytes[9..17].try_into().expect("8-byte commit epoch"));
    let op_count = u32::from_le_bytes(bytes[17..21].try_into().expect("4-byte op count"));
    let mut pos = 21usize;
    let op = match record_kind {
        RECORD_KIND_SINGLE => {
            if op_count != 1 {
                return Err(SkeinError::Storage(format!(
                    "single-op WAL record declares op_count {op_count}"
                )));
            }
            let op = decode_op_frame(bytes, &mut pos)?;
            if let WalOp::Batch(_) = op {
                return Err(SkeinError::Storage(
                    "single-op WAL record carries a batch envelope".to_string(),
                ));
            }
            op
        }
        RECORD_KIND_BATCH => {
            let mut ops = Vec::with_capacity(op_count.min(1024) as usize);
            for _ in 0..op_count {
                ops.push(decode_op_frame(bytes, &mut pos)?);
            }
            WalOp::Batch(ops)
        }
        kind => {
            return Err(SkeinError::Storage(format!(
                "unknown WAL record kind {kind}"
            )));
        }
    };
    if pos != bytes.len() {
        return Err(SkeinError::Storage(format!(
            "binary WAL record has {} trailing bytes",
            bytes.len() - pos
        )));
    }
    Ok(BinaryWalRecordDecode::Entry {
        entry: WalEntry { lsn, op },
        commit_epoch,
    })
}

fn encode_op_frame(op: &WalOp, out: &mut Vec<u8>) -> Result<()> {
    let (op_code, body) = encode_op_body(op)?;
    encode_varint_u64(op_code, out);
    encode_varint_u64(body.len() as u64, out);
    out.extend_from_slice(&body);
    Ok(())
}

fn table_kind_code(kind: TableKind) -> u64 {
    match kind {
        TableKind::Node => 0,
        TableKind::Relationship => 1,
    }
}

fn decode_table_kind_code(code: u64) -> Result<TableKind> {
    match code {
        0 => Ok(TableKind::Node),
        1 => Ok(TableKind::Relationship),
        code => Err(SkeinError::Storage(format!("invalid table kind: {code}"))),
    }
}

fn property_type_code(value_type: PropertyType) -> u64 {
    match value_type {
        PropertyType::Any => 0,
        PropertyType::Bool => 1,
        PropertyType::Int => 2,
        PropertyType::Float => 3,
        PropertyType::String => 4,
        PropertyType::List => 5,
        PropertyType::Text => 6,
    }
}

fn decode_property_type_code(code: u64) -> Result<PropertyType> {
    match code {
        0 => Ok(PropertyType::Any),
        1 => Ok(PropertyType::Bool),
        2 => Ok(PropertyType::Int),
        3 => Ok(PropertyType::Float),
        4 => Ok(PropertyType::String),
        5 => Ok(PropertyType::List),
        6 => Ok(PropertyType::Text),
        code => Err(SkeinError::Storage(format!(
            "invalid property type: {code}"
        ))),
    }
}

fn schema_object_state_code(state: SchemaObjectState) -> u64 {
    match state {
        SchemaObjectState::DeleteOnly => 0,
        SchemaObjectState::WriteOnly => 1,
        SchemaObjectState::Backfill => 2,
        SchemaObjectState::Validating => 3,
        SchemaObjectState::Public => 4,
        SchemaObjectState::Gc => 5,
    }
}

fn decode_schema_object_state_code(code: u64) -> Result<SchemaObjectState> {
    match code {
        0 => Ok(SchemaObjectState::DeleteOnly),
        1 => Ok(SchemaObjectState::WriteOnly),
        2 => Ok(SchemaObjectState::Backfill),
        3 => Ok(SchemaObjectState::Validating),
        4 => Ok(SchemaObjectState::Public),
        5 => Ok(SchemaObjectState::Gc),
        code => Err(SkeinError::Storage(format!(
            "invalid schema object state: {code}"
        ))),
    }
}

fn encode_value_message(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => encode_varint_field(VALUE_FIELD_NULL, 0, out),
        Value::Bool(value) => encode_varint_field(VALUE_FIELD_BOOL, u64::from(*value), out),
        Value::Int(value) => encode_varint_field(VALUE_FIELD_INT, zigzag_encode_i64(*value), out),
        Value::Float(value) => encode_fixed64_field(VALUE_FIELD_FLOAT, value.to_bits(), out),
        Value::String(value) => encode_string_field(VALUE_FIELD_STRING, value, out),
        Value::Binary(value) => encode_len_field(VALUE_FIELD_BINARY, value, out),
        Value::Uuid(value) => encode_len_field(VALUE_FIELD_UUID, value.as_bytes(), out),
        Value::List(values) => {
            for value in values {
                let mut body = Vec::new();
                encode_value_message(value, &mut body);
                encode_len_field(VALUE_FIELD_ELEMENT, &body, out);
            }
            // A list is identified by its elements; an empty list needs an
            // explicit marker so it does not decode as Null.
            if values.is_empty() {
                encode_len_field(VALUE_FIELD_ELEMENT, &[], out);
            }
        }
        Value::Map(values) => {
            for (key, value) in values {
                let mut body = Vec::new();
                encode_string_field(ENTRY_FIELD_KEY, key, &mut body);
                let mut value_body = Vec::new();
                encode_value_message(value, &mut value_body);
                encode_len_field(ENTRY_FIELD_VALUE, &value_body, &mut body);
                encode_len_field(VALUE_FIELD_MAP_ENTRY, &body, out);
            }
            if values.is_empty() {
                encode_len_field(VALUE_FIELD_MAP_ENTRY, &[], out);
            }
        }
    }
}

fn decode_value_message(bytes: &[u8], depth: usize) -> Result<Value> {
    ensure_value_depth(depth)?;
    let mut pos = 0usize;
    let mut value: Option<Value> = None;
    while pos < bytes.len() {
        let (field_id, wire_type) = decode_tag(bytes, &mut pos)?;
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
                value = Some(Value::String(decode_string_body(bytes, &mut pos)?));
            }
            (VALUE_FIELD_BINARY, WIRE_TYPE_LEN) => {
                value = Some(Value::Binary(decode_len_body(bytes, &mut pos)?.to_vec()));
            }
            (VALUE_FIELD_UUID, WIRE_TYPE_LEN) => {
                let encoded = decode_len_body(bytes, &mut pos)?;
                let bytes: [u8; 16] = encoded.try_into().map_err(|_| {
                    SkeinError::Storage("WAL UUID value must contain 16 bytes".to_string())
                })?;
                value = Some(Value::Uuid(skein_core::Uuid::from_bytes(bytes)));
            }
            (VALUE_FIELD_ELEMENT, WIRE_TYPE_LEN) => {
                let body = decode_len_body(bytes, &mut pos)?;
                let list = match value.take() {
                    Some(Value::List(mut values)) => {
                        if !body.is_empty() {
                            values.push(decode_value_message(body, depth.saturating_add(1))?);
                        }
                        values
                    }
                    None | Some(_) => {
                        if body.is_empty() {
                            Vec::new()
                        } else {
                            vec![decode_value_message(body, depth.saturating_add(1))?]
                        }
                    }
                };
                value = Some(Value::List(list));
            }
            (VALUE_FIELD_MAP_ENTRY, WIRE_TYPE_LEN) => {
                let body = decode_len_body(bytes, &mut pos)?;
                let mut map = match value.take() {
                    Some(Value::Map(values)) => values,
                    None | Some(_) => BTreeMap::new(),
                };
                if !body.is_empty() {
                    let (key, entry_value) = decode_map_entry(body, depth.saturating_add(2))?;
                    map.insert(key, entry_value);
                }
                value = Some(Value::Map(map));
            }
            (_, wire_type) => skip_field(bytes, &mut pos, wire_type)?,
        }
    }
    value.ok_or_else(|| SkeinError::Storage("WAL value message is empty".to_string()))
}

fn decode_map_entry(bytes: &[u8], value_depth: usize) -> Result<(String, Value)> {
    let mut pos = 0usize;
    let mut key = None;
    let mut value = None;
    while pos < bytes.len() {
        let (field_id, wire_type) = decode_tag(bytes, &mut pos)?;
        match (field_id, wire_type) {
            (ENTRY_FIELD_KEY, WIRE_TYPE_LEN) => key = Some(decode_string_body(bytes, &mut pos)?),
            (ENTRY_FIELD_VALUE, WIRE_TYPE_LEN) => {
                value = Some(decode_value_message(
                    decode_len_body(bytes, &mut pos)?,
                    value_depth,
                )?);
            }
            (_, wire_type) => skip_field(bytes, &mut pos, wire_type)?,
        }
    }
    match (key, value) {
        (Some(key), Some(value)) => Ok((key, value)),
        _ => Err(SkeinError::Storage(
            "WAL map entry is missing its key or value".to_string(),
        )),
    }
}

fn ensure_value_depth(depth: usize) -> Result<()> {
    if depth > MAX_VALUE_DEPTH {
        return Err(SkeinError::Storage(format!(
            "WAL value nesting exceeds {MAX_VALUE_DEPTH}"
        )));
    }
    Ok(())
}

fn encode_properties_fields(
    field_id: u32,
    properties: &BTreeMap<String, Value>,
    out: &mut Vec<u8>,
) {
    for (key, value) in properties {
        let mut body = Vec::new();
        encode_string_field(ENTRY_FIELD_KEY, key, &mut body);
        let mut value_body = Vec::new();
        encode_value_message(value, &mut value_body);
        encode_len_field(ENTRY_FIELD_VALUE, &value_body, &mut body);
        encode_len_field(field_id, &body, out);
    }
}

fn encode_op_body(op: &WalOp) -> Result<(u64, Vec<u8>)> {
    let mut body = Vec::new();
    let op_code = match op {
        WalOp::CreateNodeLabel { label } => {
            encode_string_field(1, label, &mut body);
            OP_CREATE_NODE_LABEL
        }
        WalOp::CreateRelationshipType { rel_type } => {
            encode_string_field(1, rel_type, &mut body);
            OP_CREATE_RELATIONSHIP_TYPE
        }
        WalOp::CreateNodeTable { name } => {
            encode_string_field(1, name, &mut body);
            OP_CREATE_NODE_TABLE
        }
        WalOp::CreateRelationshipTable { name } => {
            encode_string_field(1, name, &mut body);
            OP_CREATE_RELATIONSHIP_TABLE
        }
        WalOp::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => {
            encode_varint_field(1, table_kind_code(*table_kind), &mut body);
            encode_string_field(2, table, &mut body);
            encode_string_field(3, property, &mut body);
            encode_varint_field(4, property_type_code(*value_type), &mut body);
            encode_varint_field(5, u64::from(*nullable), &mut body);
            OP_CREATE_PROPERTY
        }
        WalOp::AlterTableState {
            table_kind,
            table,
            state,
        } => {
            encode_varint_field(1, table_kind_code(*table_kind), &mut body);
            encode_string_field(2, table, &mut body);
            encode_varint_field(3, schema_object_state_code(*state), &mut body);
            OP_ALTER_TABLE_STATE
        }
        WalOp::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => {
            encode_varint_field(1, table_kind_code(*table_kind), &mut body);
            encode_string_field(2, table, &mut body);
            encode_string_field(3, property, &mut body);
            encode_varint_field(4, schema_object_state_code(*state), &mut body);
            OP_ALTER_PROPERTY_STATE
        }
        WalOp::GcTableDescriptor { table_kind, table } => {
            encode_varint_field(1, table_kind_code(*table_kind), &mut body);
            encode_string_field(2, table, &mut body);
            OP_GC_TABLE_DESCRIPTOR
        }
        WalOp::GcPropertyDescriptor {
            table_kind,
            table,
            property,
        } => {
            encode_varint_field(1, table_kind_code(*table_kind), &mut body);
            encode_string_field(2, table, &mut body);
            encode_string_field(3, property, &mut body);
            OP_GC_PROPERTY_DESCRIPTOR
        }
        WalOp::CreateIndex { label, property } => {
            encode_string_field(1, label, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_INDEX
        }
        WalOp::CreateCompositeIndex { label, properties } => {
            encode_string_field(1, label, &mut body);
            for property in properties {
                encode_string_field(2, property, &mut body);
            }
            OP_CREATE_COMPOSITE_INDEX
        }
        WalOp::CreateRangeIndex { label, property } => {
            encode_string_field(1, label, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_RANGE_INDEX
        }
        WalOp::CreateFullTextIndex { label, property } => {
            encode_string_field(1, label, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_FULL_TEXT_INDEX
        }
        WalOp::CreateUniqueConstraint { label, property } => {
            encode_string_field(1, label, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_UNIQUE_CONSTRAINT
        }
        WalOp::CreateNodePropertyExistsConstraint { label, property } => {
            encode_string_field(1, label, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT
        }
        WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
            encode_string_field(1, rel_type, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT
        }
        WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            encode_string_field(1, rel_type, &mut body);
            encode_string_field(2, property, &mut body);
            OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT
        }
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => {
            encode_varint_field(1, id.0, &mut body);
            encode_string_field(2, label, &mut body);
            encode_properties_fields(3, properties, &mut body);
            OP_CREATE_NODE
        }
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => {
            encode_varint_field(1, id.0, &mut body);
            encode_varint_field(2, source.0, &mut body);
            encode_varint_field(3, target.0, &mut body);
            encode_string_field(4, rel_type, &mut body);
            encode_properties_fields(5, properties, &mut body);
            OP_CREATE_RELATIONSHIP
        }
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => {
            encode_varint_field(1, id.0, &mut body);
            encode_string_field(2, property, &mut body);
            let mut value_body = Vec::new();
            encode_value_message(value, &mut value_body);
            encode_len_field(3, &value_body, &mut body);
            OP_SET_NODE_PROPERTY
        }
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => {
            encode_varint_field(1, id.0, &mut body);
            encode_string_field(2, property, &mut body);
            let mut value_body = Vec::new();
            encode_value_message(value, &mut value_body);
            encode_len_field(3, &value_body, &mut body);
            OP_SET_RELATIONSHIP_PROPERTY
        }
        WalOp::DeleteNode { id } => {
            encode_varint_field(1, id.0, &mut body);
            OP_DELETE_NODE
        }
        WalOp::DeleteRelationship { id } => {
            encode_varint_field(1, id.0, &mut body);
            OP_DELETE_RELATIONSHIP
        }
        WalOp::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => {
            encode_string_field(1, name, &mut body);
            for label in node_labels {
                encode_string_field(2, label, &mut body);
            }
            for rel_type in rel_types {
                encode_string_field(3, rel_type, &mut body);
            }
            OP_PROJECT_GRAPH
        }
        WalOp::MarkInitialImportSource { source_fingerprint } => {
            encode_string_field(1, source_fingerprint, &mut body);
            OP_MARK_INITIAL_IMPORT_SOURCE
        }
        WalOp::Relational { record } => {
            encode_len_field(1, record, &mut body);
            OP_RELATIONAL
        }
        WalOp::RelationalSnapshot { record } => {
            encode_len_field(1, record, &mut body);
            OP_RELATIONAL_SNAPSHOT
        }
        WalOp::Append { record } => {
            encode_len_field(1, record, &mut body);
            OP_APPEND
        }
        WalOp::Batch(_) => {
            return Err(SkeinError::Storage(
                "nested WAL batches cannot be encoded".to_string(),
            ));
        }
    };
    Ok((op_code, body))
}

struct OpFields<'a> {
    strings: Vec<(u32, String)>,
    varints: Vec<(u32, u64)>,
    messages: Vec<(u32, &'a [u8])>,
}

impl<'a> OpFields<'a> {
    fn parse(bytes: &'a [u8], string_fields: &[u32], message_fields: &[u32]) -> Result<Self> {
        let mut fields = Self {
            strings: Vec::new(),
            varints: Vec::new(),
            messages: Vec::new(),
        };
        let mut pos = 0usize;
        while pos < bytes.len() {
            let (field_id, wire_type) = decode_tag(bytes, &mut pos)?;
            match wire_type {
                WIRE_TYPE_VARINT => {
                    fields
                        .varints
                        .push((field_id, decode_varint_u64(bytes, &mut pos)?));
                }
                WIRE_TYPE_LEN if string_fields.contains(&field_id) => {
                    fields
                        .strings
                        .push((field_id, decode_string_body(bytes, &mut pos)?));
                }
                WIRE_TYPE_LEN if message_fields.contains(&field_id) => {
                    fields
                        .messages
                        .push((field_id, decode_len_body(bytes, &mut pos)?));
                }
                wire_type => skip_field(bytes, &mut pos, wire_type)?,
            }
        }
        Ok(fields)
    }

    fn required_string(&mut self, field_id: u32, name: &str) -> Result<String> {
        let index = self
            .strings
            .iter()
            .position(|(id, _)| *id == field_id)
            .ok_or_else(|| SkeinError::Storage(format!("WAL op is missing {name}")))?;
        Ok(self.strings.remove(index).1)
    }

    fn strings_for(&mut self, field_id: u32) -> Vec<String> {
        let mut values = Vec::new();
        self.strings.retain_mut(|(id, value)| {
            if *id == field_id {
                values.push(std::mem::take(value));
                false
            } else {
                true
            }
        });
        values
    }

    fn required_varint(&self, field_id: u32, name: &str) -> Result<u64> {
        self.varints
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, value)| *value)
            .ok_or_else(|| SkeinError::Storage(format!("WAL op is missing {name}")))
    }

    fn required_message(&self, field_id: u32, name: &str) -> Result<&'a [u8]> {
        self.messages
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, body)| *body)
            .ok_or_else(|| SkeinError::Storage(format!("WAL op is missing {name}")))
    }

    fn properties_for(&self, field_id: u32) -> Result<BTreeMap<String, Value>> {
        let mut properties = BTreeMap::new();
        for (id, body) in &self.messages {
            if *id == field_id {
                let (key, value) = decode_map_entry(body, 1)?;
                properties.insert(key, value);
            }
        }
        Ok(properties)
    }
}

fn decode_op_frame(bytes: &[u8], pos: &mut usize) -> Result<WalOp> {
    let op_code = decode_varint_u64(bytes, pos)?;
    let body = decode_len_body(bytes, pos)?;
    decode_op_body(op_code, body)
}

fn decode_op_body(op_code: u64, body: &[u8]) -> Result<WalOp> {
    match op_code {
        OP_CREATE_NODE_LABEL => {
            let mut fields = OpFields::parse(body, &[1], &[])?;
            Ok(WalOp::CreateNodeLabel {
                label: fields.required_string(1, "label")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TYPE => {
            let mut fields = OpFields::parse(body, &[1], &[])?;
            Ok(WalOp::CreateRelationshipType {
                rel_type: fields.required_string(1, "relationship type")?,
            })
        }
        OP_CREATE_NODE_TABLE => {
            let mut fields = OpFields::parse(body, &[1], &[])?;
            Ok(WalOp::CreateNodeTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TABLE => {
            let mut fields = OpFields::parse(body, &[1], &[])?;
            Ok(WalOp::CreateRelationshipTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_PROPERTY => {
            let mut fields = OpFields::parse(body, &[2, 3], &[])?;
            Ok(WalOp::CreateProperty {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                value_type: decode_property_type_code(fields.required_varint(4, "value type")?)?,
                nullable: fields.required_varint(5, "nullable flag")? != 0,
            })
        }
        OP_ALTER_TABLE_STATE => {
            let mut fields = OpFields::parse(body, &[2], &[])?;
            Ok(WalOp::AlterTableState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                state: decode_schema_object_state_code(fields.required_varint(3, "state")?)?,
            })
        }
        OP_ALTER_PROPERTY_STATE => {
            let mut fields = OpFields::parse(body, &[2, 3], &[])?;
            Ok(WalOp::AlterPropertyState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                state: decode_schema_object_state_code(fields.required_varint(4, "state")?)?,
            })
        }
        OP_GC_TABLE_DESCRIPTOR => {
            let mut fields = OpFields::parse(body, &[2], &[])?;
            Ok(WalOp::GcTableDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
            })
        }
        OP_GC_PROPERTY_DESCRIPTOR => {
            let mut fields = OpFields::parse(body, &[2, 3], &[])?;
            Ok(WalOp::GcPropertyDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
            })
        }
        OP_CREATE_INDEX => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_COMPOSITE_INDEX => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateCompositeIndex {
                label: fields.required_string(1, "label")?,
                properties: fields.strings_for(2),
            })
        }
        OP_CREATE_RANGE_INDEX => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRangeIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_FULL_TEXT_INDEX => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateFullTextIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_UNIQUE_CONSTRAINT => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateUniqueConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateNodePropertyExistsConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRelationshipUniqueConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT => {
            let mut fields = OpFields::parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE => {
            let mut fields = OpFields::parse(body, &[2], &[3])?;
            Ok(WalOp::CreateNode {
                id: NodeId(fields.required_varint(1, "node id")?),
                label: fields.required_string(2, "label")?,
                properties: fields.properties_for(3)?,
            })
        }
        OP_CREATE_RELATIONSHIP => {
            let mut fields = OpFields::parse(body, &[4], &[5])?;
            Ok(WalOp::CreateRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
                source: NodeId(fields.required_varint(2, "source node id")?),
                target: NodeId(fields.required_varint(3, "target node id")?),
                rel_type: fields.required_string(4, "relationship type")?,
                properties: fields.properties_for(5)?,
            })
        }
        OP_SET_NODE_PROPERTY => {
            let mut fields = OpFields::parse(body, &[2], &[3])?;
            Ok(WalOp::SetNodeProperty {
                id: NodeId(fields.required_varint(1, "node id")?),
                property: fields.required_string(2, "property")?,
                value: decode_value_message(fields.required_message(3, "value")?, 1)?,
            })
        }
        OP_SET_RELATIONSHIP_PROPERTY => {
            let mut fields = OpFields::parse(body, &[2], &[3])?;
            Ok(WalOp::SetRelationshipProperty {
                id: RelId(fields.required_varint(1, "relationship id")?),
                property: fields.required_string(2, "property")?,
                value: decode_value_message(fields.required_message(3, "value")?, 1)?,
            })
        }
        OP_DELETE_NODE => {
            let fields = OpFields::parse(body, &[], &[])?;
            Ok(WalOp::DeleteNode {
                id: NodeId(fields.required_varint(1, "node id")?),
            })
        }
        OP_DELETE_RELATIONSHIP => {
            let fields = OpFields::parse(body, &[], &[])?;
            Ok(WalOp::DeleteRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
            })
        }
        OP_PROJECT_GRAPH => {
            let mut fields = OpFields::parse(body, &[1, 2, 3], &[])?;
            Ok(WalOp::ProjectGraph {
                name: fields.required_string(1, "projected graph name")?,
                node_labels: fields.strings_for(2),
                rel_types: fields.strings_for(3),
            })
        }
        OP_MARK_INITIAL_IMPORT_SOURCE => {
            let mut fields = OpFields::parse(body, &[1], &[])?;
            Ok(WalOp::MarkInitialImportSource {
                source_fingerprint: fields.required_string(1, "source fingerprint")?,
            })
        }
        OP_RELATIONAL => {
            let fields = OpFields::parse(body, &[], &[1])?;
            Ok(WalOp::Relational {
                record: Arc::from(fields.required_message(1, "relational record")?.to_vec()),
            })
        }
        OP_RELATIONAL_SNAPSHOT => {
            let fields = OpFields::parse(body, &[], &[1])?;
            Ok(WalOp::RelationalSnapshot {
                record: Arc::from(fields.required_message(1, "relational record")?.to_vec()),
            })
        }
        OP_APPEND => {
            let fields = OpFields::parse(body, &[], &[1])?;
            Ok(WalOp::Append {
                record: Arc::from(fields.required_message(1, "append record")?.to_vec()),
            })
        }
        op_code => Err(SkeinError::Storage(format!(
            "unknown WAL op code {op_code}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::wire;

    fn round_trip(entry: &WalEntry, commit_epoch: u64) -> (WalEntry, u64) {
        let encoded = encode_binary_wal_record(entry, commit_epoch).unwrap();
        match decode_binary_wal_record(&encoded).unwrap() {
            BinaryWalRecordDecode::Entry {
                entry,
                commit_epoch,
            } => (entry, commit_epoch),
            BinaryWalRecordDecode::Corrupt(reason) => panic!("corrupt round trip: {reason}"),
        }
    }

    fn assert_op_round_trips(op: WalOp) {
        let entry = WalEntry { lsn: 42, op };
        let (decoded, commit_epoch) = round_trip(&entry, 7);
        assert_eq!(commit_epoch, 7);
        assert_eq!(decoded.lsn, 42);
        // The legacy text renderer remains the canonical identity for
        // comparison because WalOp intentionally does not implement PartialEq.
        assert_eq!(decoded.encode(), entry.encode());
    }

    #[test]
    fn value_message_wire_fixtures_cover_every_field() {
        // Literal bytes, independent of the codec constants and helpers, pin
        // the append-only field IDs as well as their wire types and payloads.
        let fixtures: Vec<(Value, Vec<u8>)> = vec![
            (Value::Null, vec![0x08, 0x00]),
            (Value::Bool(true), vec![0x10, 0x01]),
            (Value::Int(-3), vec![0x18, 0x05]),
            (
                Value::Float(-0.5),
                vec![0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe0, 0xbf],
            ),
            (Value::String("a".to_string()), vec![0x2a, 0x01, b'a']),
            (
                Value::List(vec![Value::Null, Value::Bool(true)]),
                vec![0x32, 0x02, 0x08, 0x00, 0x32, 0x02, 0x10, 0x01],
            ),
            (Value::List(Vec::new()), vec![0x32, 0x00]),
            (
                Value::Map(BTreeMap::from([("k".to_string(), Value::Int(-3))])),
                vec![0x3a, 0x07, 0x0a, 0x01, b'k', 0x12, 0x02, 0x18, 0x05],
            ),
            (Value::Map(BTreeMap::new()), vec![0x3a, 0x00]),
            (
                Value::Binary(vec![0x00, 0x01, 0xfe, 0xff]),
                vec![0x42, 0x04, 0x00, 0x01, 0xfe, 0xff],
            ),
            (Value::Binary(Vec::new()), vec![0x42, 0x00]),
            (
                Value::Uuid(
                    skein_core::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                ),
                vec![
                    0x4a, 0x10, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa,
                    0xbb, 0xcc, 0xdd, 0xee, 0xff,
                ],
            ),
        ];
        for (value, expected) in fixtures {
            let mut encoded = Vec::new();
            encode_value_message(&value, &mut encoded);
            assert_eq!(encoded, expected, "encoding {value:?}");
            assert_eq!(decode_value_message(&expected, 1).unwrap(), value);
            let record = encoded_set_node_property_record(&expected);
            let BinaryWalRecordDecode::Entry { entry, .. } =
                decode_binary_wal_record(&record).unwrap()
            else {
                panic!("literal value fixture was rejected: {value:?}");
            };
            assert!(
                matches!(entry.op, WalOp::SetNodeProperty { value: actual, .. } if actual == value)
            );
        }
    }

    #[test]
    fn property_type_wire_assignments_are_append_only() {
        for (value_type, code) in [
            (PropertyType::Any, 0),
            (PropertyType::Bool, 1),
            (PropertyType::Int, 2),
            (PropertyType::Float, 3),
            (PropertyType::String, 4),
            (PropertyType::List, 5),
            (PropertyType::Text, 6),
        ] {
            assert_eq!(property_type_code(value_type), code);
            assert_eq!(decode_property_type_code(code).unwrap(), value_type);
        }
    }

    #[test]
    fn uuid_value_records_require_exactly_sixteen_payload_bytes() {
        for len in 0u8..=32 {
            let mut value = vec![0x4a, len];
            value.extend(std::iter::repeat_n(0xa5, usize::from(len)));
            let record = encoded_set_node_property_record(&value);
            match decode_binary_wal_record(&record).unwrap() {
                BinaryWalRecordDecode::Entry { entry, .. } => {
                    assert_eq!(len, 16);
                    assert!(matches!(
                        entry.op,
                        WalOp::SetNodeProperty { value: Value::Uuid(actual), .. }
                            if actual.as_bytes() == &[0xa5; 16]
                    ));
                }
                BinaryWalRecordDecode::Corrupt(reason) => {
                    assert_ne!(len, 16);
                    assert!(reason.contains("WAL UUID value must contain 16 bytes"));
                }
            }
        }
    }

    #[test]
    fn nested_batches_fail_text_and_binary_encoding() {
        let entry = WalEntry {
            lsn: 42,
            op: WalOp::Batch(vec![WalOp::Batch(vec![WalOp::DeleteNode {
                id: NodeId(7),
            }])]),
        };

        let text_error = entry.encode().unwrap_err();
        assert!(text_error.to_string().contains("nested WAL batches"));
        let binary_error = encode_binary_wal_record(&entry, 9).unwrap_err();
        assert!(binary_error.to_string().contains("nested WAL batches"));
    }

    fn sample_properties() -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("empty_list".to_string(), Value::List(Vec::new())),
            ("empty_map".to_string(), Value::Map(BTreeMap::new())),
            ("flag".to_string(), Value::Bool(true)),
            ("weight".to_string(), Value::Float(-0.5)),
            ("count".to_string(), Value::Int(i64::MIN)),
            ("missing".to_string(), Value::Null),
            ("payload".to_string(), Value::Binary(vec![0, 1, 0xfe, 0xff])),
            (
                "nested".to_string(),
                Value::Map(BTreeMap::from([(
                    "inner".to_string(),
                    Value::List(vec![
                        Value::Int(-3),
                        Value::String("täb\there\nand|pipe,comma;semi=eq:colon".to_string()),
                        Value::List(vec![Value::Null]),
                    ]),
                )])),
            ),
        ])
    }

    fn value_at_depth(depth: usize) -> Value {
        assert!(depth > 0);
        (1..depth).fold(Value::Null, |value, _| Value::List(vec![value]))
    }

    fn encoded_value_at_depth(depth: usize) -> Vec<u8> {
        assert!(depth > 0);
        let mut encoded = Vec::new();
        encode_varint_field(VALUE_FIELD_NULL, 0, &mut encoded);
        for _ in 1..depth {
            let inner = encoded;
            encoded = Vec::new();
            encode_len_field(VALUE_FIELD_ELEMENT, &inner, &mut encoded);
        }
        encoded
    }

    fn encoded_set_node_property_record(value: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        encode_varint_field(1, 7, &mut body);
        encode_string_field(2, "payload", &mut body);
        encode_len_field(3, value, &mut body);

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&1u64.to_le_bytes());
        encoded.push(RECORD_KIND_SINGLE);
        encoded.extend_from_slice(&1u64.to_le_bytes());
        encoded.extend_from_slice(&1u32.to_le_bytes());
        encode_varint_u64(OP_SET_NODE_PROPERTY, &mut encoded);
        encode_varint_u64(body.len() as u64, &mut encoded);
        encoded.extend_from_slice(&body);
        encoded
    }

    /// One WalOp sample per variant. The match is exhaustive on purpose:
    /// adding a WalOp variant without extending the binary codec (and this
    /// list) fails to compile here.
    fn sample_ops() -> Vec<WalOp> {
        let mut samples = Vec::new();
        // Compile-time exhaustiveness pin: every variant must be listed.
        let pin = |op: &WalOp| match op {
            WalOp::CreateNodeLabel { .. }
            | WalOp::CreateRelationshipType { .. }
            | WalOp::CreateNodeTable { .. }
            | WalOp::CreateRelationshipTable { .. }
            | WalOp::CreateProperty { .. }
            | WalOp::AlterTableState { .. }
            | WalOp::AlterPropertyState { .. }
            | WalOp::GcTableDescriptor { .. }
            | WalOp::GcPropertyDescriptor { .. }
            | WalOp::CreateIndex { .. }
            | WalOp::CreateCompositeIndex { .. }
            | WalOp::CreateRangeIndex { .. }
            | WalOp::CreateFullTextIndex { .. }
            | WalOp::CreateUniqueConstraint { .. }
            | WalOp::CreateNodePropertyExistsConstraint { .. }
            | WalOp::CreateRelationshipUniqueConstraint { .. }
            | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
            | WalOp::CreateNode { .. }
            | WalOp::CreateRelationship { .. }
            | WalOp::SetNodeProperty { .. }
            | WalOp::SetRelationshipProperty { .. }
            | WalOp::DeleteNode { .. }
            | WalOp::DeleteRelationship { .. }
            | WalOp::ProjectGraph { .. }
            | WalOp::MarkInitialImportSource { .. }
            | WalOp::Relational { .. }
            | WalOp::RelationalSnapshot { .. }
            | WalOp::Append { .. }
            | WalOp::Batch(_) => {}
        };
        samples.push(WalOp::CreateNodeLabel {
            label: "Memory".to_string(),
        });
        samples.push(WalOp::CreateRelationshipType {
            rel_type: "RELATED_TO".to_string(),
        });
        samples.push(WalOp::CreateNodeTable {
            name: "memories".to_string(),
        });
        samples.push(WalOp::CreateRelationshipTable {
            name: "mentions".to_string(),
        });
        for table_kind in [TableKind::Node, TableKind::Relationship] {
            for value_type in [
                PropertyType::Any,
                PropertyType::Bool,
                PropertyType::Int,
                PropertyType::Float,
                PropertyType::String,
                PropertyType::Text,
                PropertyType::List,
            ] {
                samples.push(WalOp::CreateProperty {
                    table_kind,
                    table: "memories".to_string(),
                    property: "title".to_string(),
                    value_type,
                    nullable: value_type == PropertyType::Any,
                });
            }
            for state in [
                SchemaObjectState::DeleteOnly,
                SchemaObjectState::WriteOnly,
                SchemaObjectState::Backfill,
                SchemaObjectState::Validating,
                SchemaObjectState::Public,
                SchemaObjectState::Gc,
            ] {
                samples.push(WalOp::AlterTableState {
                    table_kind,
                    table: "memories".to_string(),
                    state,
                });
                samples.push(WalOp::AlterPropertyState {
                    table_kind,
                    table: "memories".to_string(),
                    property: "title".to_string(),
                    state,
                });
            }
            samples.push(WalOp::GcTableDescriptor {
                table_kind,
                table: "memories".to_string(),
            });
            samples.push(WalOp::GcPropertyDescriptor {
                table_kind,
                table: "memories".to_string(),
                property: "title".to_string(),
            });
        }
        samples.push(WalOp::CreateIndex {
            label: "Memory".to_string(),
            property: "id".to_string(),
        });
        samples.push(WalOp::CreateCompositeIndex {
            label: "Memory".to_string(),
            properties: vec!["space_id".to_string(), "id".to_string()],
        });
        samples.push(WalOp::CreateCompositeIndex {
            label: "Memory".to_string(),
            properties: Vec::new(),
        });
        samples.push(WalOp::CreateRangeIndex {
            label: "Memory".to_string(),
            property: "importance".to_string(),
        });
        samples.push(WalOp::CreateFullTextIndex {
            label: "Memory".to_string(),
            property: "title".to_string(),
        });
        samples.push(WalOp::CreateUniqueConstraint {
            label: "Memory".to_string(),
            property: "id".to_string(),
        });
        samples.push(WalOp::CreateNodePropertyExistsConstraint {
            label: "Memory".to_string(),
            property: "id".to_string(),
        });
        samples.push(WalOp::CreateRelationshipUniqueConstraint {
            rel_type: "MENTIONS".to_string(),
            property: "id".to_string(),
        });
        samples.push(WalOp::CreateRelationshipPropertyExistsConstraint {
            rel_type: "MENTIONS".to_string(),
            property: "id".to_string(),
        });
        samples.push(WalOp::CreateNode {
            id: NodeId(u64::MAX),
            label: "Memory".to_string(),
            properties: sample_properties(),
        });
        samples.push(WalOp::CreateNode {
            id: NodeId(0),
            label: String::new(),
            properties: BTreeMap::new(),
        });
        samples.push(WalOp::CreateRelationship {
            id: RelId(3),
            source: NodeId(1),
            target: NodeId(2),
            rel_type: "MENTIONS".to_string(),
            properties: sample_properties(),
        });
        samples.push(WalOp::SetNodeProperty {
            id: NodeId(9),
            property: "title".to_string(),
            value: Value::String("Graph foundations".to_string()),
        });
        samples.push(WalOp::SetRelationshipProperty {
            id: RelId(9),
            property: "confidence".to_string(),
            value: Value::Float(f64::NAN),
        });
        samples.push(WalOp::DeleteNode { id: NodeId(11) });
        samples.push(WalOp::DeleteRelationship { id: RelId(12) });
        samples.push(WalOp::ProjectGraph {
            name: "mem".to_string(),
            node_labels: vec!["Memory".to_string(), "Entity".to_string()],
            rel_types: Vec::new(),
        });
        samples.push(WalOp::MarkInitialImportSource {
            source_fingerprint: "sha256:abcdef".to_string(),
        });
        samples.push(WalOp::Relational {
            record: Arc::from(vec![0u8, 1, 2, 254, 255]),
        });
        samples.push(WalOp::RelationalSnapshot {
            record: Arc::from(Vec::new()),
        });
        samples.push(WalOp::Append {
            record: Arc::from(vec![7u8, 8, 9]),
        });
        samples.push(WalOp::Batch(vec![
            WalOp::CreateNodeLabel {
                label: "Memory".to_string(),
            },
            WalOp::CreateNode {
                id: NodeId(1),
                label: "Memory".to_string(),
                properties: sample_properties(),
            },
        ]));
        samples.push(WalOp::Batch(Vec::new()));
        for op in &samples {
            pin(op);
        }
        samples
    }

    #[test]
    fn every_wal_op_variant_round_trips() {
        for op in sample_ops() {
            assert_op_round_trips(op);
        }
    }

    #[test]
    fn wal_encoder_rejects_values_beyond_the_canonical_depth_limit() {
        let entry_at_limit = WalEntry {
            lsn: 1,
            op: WalOp::SetNodeProperty {
                id: NodeId(7),
                property: "payload".to_string(),
                value: value_at_depth(MAX_VALUE_DEPTH),
            },
        };
        encode_binary_wal_record(&entry_at_limit, 1).unwrap();

        let entry_over_limit = WalEntry {
            lsn: 1,
            op: WalOp::SetNodeProperty {
                id: NodeId(7),
                property: "payload".to_string(),
                value: value_at_depth(MAX_VALUE_DEPTH + 1),
            },
        };
        let error = encode_binary_wal_record(&entry_over_limit, 1).unwrap_err();
        assert!(error.to_string().contains("nesting exceeds 32"));
    }

    #[test]
    fn wal_decoder_rejects_nested_headers_beyond_the_depth_limit() {
        let at_limit = encoded_set_node_property_record(&encoded_value_at_depth(MAX_VALUE_DEPTH));
        assert!(matches!(
            decode_binary_wal_record(&at_limit).unwrap(),
            BinaryWalRecordDecode::Entry { .. }
        ));

        let over_limit =
            encoded_set_node_property_record(&encoded_value_at_depth(MAX_VALUE_DEPTH + 1));
        assert!(matches!(
            decode_binary_wal_record(&over_limit).unwrap(),
            BinaryWalRecordDecode::Corrupt(reason) if reason.contains("nesting exceeds 32")
        ));
    }

    #[test]
    fn batch_of_one_stays_distinct_from_a_bare_op() {
        let bare = WalEntry {
            lsn: 1,
            op: WalOp::DeleteNode { id: NodeId(5) },
        };
        let batched = WalEntry {
            lsn: 1,
            op: WalOp::Batch(vec![WalOp::DeleteNode { id: NodeId(5) }]),
        };
        let (decoded_bare, _) = round_trip(&bare, 1);
        let (decoded_batched, _) = round_trip(&batched, 1);
        assert!(!matches!(decoded_bare.op, WalOp::Batch(_)));
        assert!(matches!(&decoded_batched.op, WalOp::Batch(ops) if ops.len() == 1));
    }

    #[test]
    fn nan_float_values_round_trip_bitwise() {
        let entry = WalEntry {
            lsn: 8,
            op: WalOp::SetNodeProperty {
                id: NodeId(1),
                property: "score".to_string(),
                value: Value::Float(f64::from_bits(0x7ff8_0000_dead_beef)),
            },
        };
        let (decoded, _) = round_trip(&entry, 3);
        match decoded.op {
            WalOp::SetNodeProperty {
                value: Value::Float(value),
                ..
            } => assert_eq!(value.to_bits(), 0x7ff8_0000_dead_beef),
            op => panic!("unexpected op {op:?}"),
        }
    }

    #[test]
    fn unknown_op_fields_are_skipped_for_forward_compatibility() {
        // Hand-build a DeleteNode body with an extra unknown field of every
        // wire type; the decoder must ignore them.
        let mut body = Vec::new();
        wire::encode_varint_field(1, 77, &mut body);
        wire::encode_varint_field(63, 12345, &mut body);
        wire::encode_fixed64_field(64, u64::MAX, &mut body);
        wire::encode_len_field(65, b"future-extension", &mut body);
        let mut record = Vec::new();
        record.extend_from_slice(&5u64.to_le_bytes());
        record.push(RECORD_KIND_SINGLE);
        record.extend_from_slice(&9u64.to_le_bytes());
        record.extend_from_slice(&1u32.to_le_bytes());
        wire::encode_varint_u64(OP_DELETE_NODE, &mut record);
        wire::encode_varint_u64(body.len() as u64, &mut record);
        record.extend_from_slice(&body);
        match decode_binary_wal_record(&record).unwrap() {
            BinaryWalRecordDecode::Entry { entry, .. } => match entry.op {
                WalOp::DeleteNode { id } => assert_eq!(id, NodeId(77)),
                op => panic!("unexpected op {op:?}"),
            },
            BinaryWalRecordDecode::Corrupt(reason) => panic!("corrupt: {reason}"),
        }
    }

    #[test]
    fn unknown_op_codes_fail_closed_as_corruption() {
        let mut record = Vec::new();
        record.extend_from_slice(&5u64.to_le_bytes());
        record.push(RECORD_KIND_SINGLE);
        record.extend_from_slice(&9u64.to_le_bytes());
        record.extend_from_slice(&1u32.to_le_bytes());
        wire::encode_varint_u64(9999, &mut record);
        wire::encode_varint_u64(0, &mut record);
        match decode_binary_wal_record(&record).unwrap() {
            BinaryWalRecordDecode::Corrupt(reason) => {
                assert!(reason.contains("unknown WAL op code 9999"));
            }
            BinaryWalRecordDecode::Entry { .. } => panic!("decoded an unknown op code"),
        }
    }

    #[test]
    fn truncated_envelopes_and_trailing_bytes_are_corrupt() {
        let entry = WalEntry {
            lsn: 3,
            op: WalOp::DeleteNode { id: NodeId(4) },
        };
        let encoded = encode_binary_wal_record(&entry, 2).unwrap();
        assert!(matches!(
            decode_binary_wal_record(&encoded[..encoded.len() - 1]).unwrap(),
            BinaryWalRecordDecode::Corrupt(_)
        ));
        let mut padded = encoded;
        padded.push(0);
        assert!(matches!(
            decode_binary_wal_record(&padded).unwrap(),
            BinaryWalRecordDecode::Corrupt(reason) if reason.contains("trailing bytes")
        ));
    }
}
