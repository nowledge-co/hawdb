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

//! Shared WAL value and map-entry wire semantics. Backends preserve their
//! original allocation strategy and cooperative admission boundaries.

use super::*;

pub(super) trait ValueDecoder: record_decode::RecordDecoder {
    type MapNodes: Default;
    fn string(&self, bytes: &[u8], pos: &mut usize) -> Result<String>;
    fn bytes(&self, bytes: &[u8]) -> Result<Vec<u8>>;
    fn push_value(&self, values: &mut Vec<Value>, value: Value) -> Result<()>;
    fn singleton_value(&self, value: Value) -> Result<Vec<Value>>;
    fn before_map_insert(&self, nodes: &mut Self::MapNodes, len: usize) -> Result<()>;
}

pub(super) fn decode_value<D: ValueDecoder>(
    bytes: &[u8],
    depth: usize,
    decoder: &D,
) -> Result<Value> {
    ensure_value_depth(depth)?;
    let mut pos = 0usize;
    let mut value: Option<Value> = None;
    let mut map_nodes = D::MapNodes::default();
    while pos < bytes.len() {
        let (field_id, wire_type) = decoder.with_unit(|| decode_tag(bytes, &mut pos))?;
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
                value = Some(Value::String(decoder.string(bytes, &mut pos)?));
            }
            (VALUE_FIELD_BINARY, WIRE_TYPE_LEN) => {
                value = Some(Value::Binary(
                    decoder.bytes(decode_len_body(bytes, &mut pos)?)?,
                ));
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
                            decoder.push_value(
                                &mut values,
                                decode_value(body, depth.saturating_add(1), decoder)?,
                            )?;
                        }
                        values
                    }
                    None | Some(_) => {
                        if body.is_empty() {
                            Vec::new()
                        } else {
                            decoder.singleton_value(decode_value(
                                body,
                                depth.saturating_add(1),
                                decoder,
                            )?)?
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
                        map_nodes = D::MapNodes::default();
                        BTreeMap::new()
                    }
                };
                if !body.is_empty() {
                    let (key, entry_value) =
                        decode_map_entry(body, depth.saturating_add(2), decoder)?;
                    decoder.with_unit(|| {
                        decoder.before_map_insert(&mut map_nodes, map.len())?;
                        map.insert(key, entry_value);
                        Ok(())
                    })?;
                }
                value = Some(Value::Map(map));
            }
            (_, wire_type) => skip_field(bytes, &mut pos, wire_type)?,
        }
    }
    value.ok_or_else(|| HawDBError::Storage("WAL value message is empty".to_string()))
}

pub(super) fn decode_map_entry<D: ValueDecoder>(
    bytes: &[u8],
    value_depth: usize,
    decoder: &D,
) -> Result<(String, Value)> {
    let mut pos = 0usize;
    let mut key = None;
    let mut value = None;
    while pos < bytes.len() {
        let (field_id, wire_type) = decoder.with_unit(|| decode_tag(bytes, &mut pos))?;
        match (field_id, wire_type) {
            (ENTRY_FIELD_KEY, WIRE_TYPE_LEN) => key = Some(decoder.string(bytes, &mut pos)?),
            (ENTRY_FIELD_VALUE, WIRE_TYPE_LEN) => {
                value = Some(decode_value(
                    decode_len_body(bytes, &mut pos)?,
                    value_depth,
                    decoder,
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
