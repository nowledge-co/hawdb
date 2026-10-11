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

//! Existing graph checkpoint and spill text encodings.
//!
//! These are storage-owned implementation details, not a new host protocol.
//! Keep byte encodings, legacy decoding tolerance, and error messages stable.

#[doc(hidden)]
pub mod envelope;
pub(crate) mod value_decode;

use hawdb_core::{
    HawDBError, IndexKind, PropertyType, Result, SchemaObjectState, TableKind, Value,
};
use std::collections::BTreeMap;

pub fn encode_string_vec(values: &[String]) -> String {
    values
        .iter()
        .map(|value| encode_string(value))
        .collect::<Vec<_>>()
        .join(":")
}

pub fn decode_string_vec(input: &str) -> Result<Vec<String>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split(':').map(decode_string).collect()
}

pub fn encode_u64_vec(values: impl IntoIterator<Item = u64>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub fn decode_u64_vec(input: &str, name: &str) -> Result<Vec<u64>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| parse_u64(value, name))
        .collect()
}

pub fn encode_properties(properties: &BTreeMap<String, Value>) -> String {
    properties
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_value(value)))
        .collect::<Vec<_>>()
        .join(";")
}

pub fn decode_properties(input: &str) -> Result<BTreeMap<String, Value>> {
    let mut properties = BTreeMap::new();
    if input.is_empty() {
        return Ok(properties);
    }
    let mut pairs = input.split(';').peekable();
    let mut offset = 0;
    while let Some(pair) = pairs.next() {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(HawDBError::Storage(format!(
                "invalid property pair: {pair}"
            )));
        };
        let key = decode_string(key)?;
        let value_start = offset + pair.len() - value.len();
        let mut value_end = offset + pair.len();
        offset += pair.len() + 1;
        if value.starts_with('m') {
            // Map leaves contain hex-escaped tagged values; every valid tag
            // starts with ASCII 0x6? or 0x7?. Outer properties use the literal
            // tag instead. Preserve this existing wire grammar without
            // treating a map's internal semicolons as property separators.
            while pairs.peek().is_some_and(|next| {
                next.split_once('=').is_some_and(|(_, encoded)| {
                    matches!(encoded.as_bytes().first(), Some(b'6' | b'7'))
                })
            }) {
                let next = pairs.next().expect("peeked map continuation");
                value_end += next.len() + 1;
                offset += next.len() + 1;
            }
        }
        properties.insert(key, decode_value(&input[value_start..value_end])?);
    }
    Ok(properties)
}

pub fn encode_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(false) => "b0".to_string(),
        Value::Bool(true) => "b1".to_string(),
        Value::Int(value) => format!("i{value}"),
        Value::Float(value) => format!("f{}", value.to_bits()),
        Value::String(value) => format!("s{}", encode_string(value)),
        Value::Uuid(value) => format!("u{value}"),
        Value::Binary(value) => format!(
            "x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::List(values) => format!(
            "l{}",
            values
                .iter()
                .map(|value| encode_string(&encode_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!(
            "m{}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}={}",
                    encode_string(key),
                    encode_string(&encode_value(value))
                ))
                .collect::<Vec<_>>()
                .join(";")
        ),
    }
}

struct OrdinaryValueDecoder;

impl value_decode::Decoder for OrdinaryValueDecoder {
    type Items<'a> = std::str::Split<'a, char>;
    type TemporaryText = String;
    type MapMemory = ();
    fn visit(&self) -> Result<()> {
        Ok(())
    }
    fn integer(&self, input: &str) -> Result<i64> {
        parse_i64(input, "integer value")
    }
    fn unsigned(&self, input: &str) -> Result<u64> {
        parse_u64(input, "float value")
    }
    fn string(&self, input: &str) -> Result<String> {
        decode_string(input)
    }
    fn binary(&self, input: &str) -> Result<Vec<u8>> {
        decode_hex_bytes(input)
    }
    fn temporary(&self, input: &str) -> Result<String> {
        decode_string(input)
    }
    fn items<'a>(&self, input: &'a str, delimiter: u8) -> Self::Items<'a> {
        input.split(char::from(delimiter))
    }
    fn next<'a>(&self, items: &mut Self::Items<'a>) -> Result<Option<&'a str>> {
        Ok(items.next())
    }
    fn equal(&self, input: &str) -> Result<Option<usize>> {
        Ok(input.find('='))
    }
    fn push(&self, values: &mut Vec<Value>, value: Value) -> Result<()> {
        values.push(value);
        Ok(())
    }
    fn insert(
        &self,
        values: &mut BTreeMap<String, Value>,
        _memory: &mut (),
        key: String,
        value: Value,
    ) -> Result<()> {
        values.insert(key, value);
        Ok(())
    }
}

pub fn decode_value(input: &str) -> Result<Value> {
    value_decode::decode(input, &OrdinaryValueDecoder)
}

fn decode_hex_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(HawDBError::Storage(
            "binary value has an odd number of hex digits".to_string(),
        ));
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = decode_hex_digit(digits[0])?;
            let low = decode_hex_digit(digits[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>>>()
}

fn decode_hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(HawDBError::Storage(format!(
            "binary value contains invalid hex digit {:?}",
            char::from(digit)
        ))),
    }
}

pub fn encode_string(input: &str) -> String {
    encode_bytes(input.as_bytes())
}

pub fn encode_bytes(input: &[u8]) -> String {
    input.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn decode_string(input: &str) -> Result<String> {
    let bytes = decode_bytes(input)?;
    String::from_utf8(bytes).map_err(HawDBError::from_storage_error)
}

pub fn decode_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(HawDBError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = input
            .get(offset..offset + 2)
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            .ok_or_else(|| {
                HawDBError::Storage(format!("invalid hex string at byte offset {offset}"))
            })?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

pub fn parse_u32(input: &str, name: &str) -> Result<u32> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

pub fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

pub fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

pub fn encode_value_vec(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| encode_string(&encode_value(value)))
        .collect::<Vec<_>>()
        .join(":")
}

pub fn decode_value_vec(input: &str) -> Result<Vec<Value>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(':')
        .map(|value| decode_string(value).and_then(|value| decode_value(&value)))
        .collect()
}

pub fn encode_table_kind(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Node => "node",
        TableKind::Relationship => "relationship",
    }
}

pub fn decode_table_kind(input: &str) -> Result<TableKind> {
    match input {
        "node" => Ok(TableKind::Node),
        "relationship" => Ok(TableKind::Relationship),
        _ => Err(HawDBError::Storage(format!("invalid table kind: {input}"))),
    }
}

pub fn encode_property_type(value_type: PropertyType) -> &'static str {
    match value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::Text => "text",
        PropertyType::List => "list",
    }
}

pub fn decode_property_type(input: &str) -> Result<PropertyType> {
    match input {
        "any" => Ok(PropertyType::Any),
        "bool" => Ok(PropertyType::Bool),
        "int" => Ok(PropertyType::Int),
        "float" => Ok(PropertyType::Float),
        "string" => Ok(PropertyType::String),
        "text" => Ok(PropertyType::Text),
        "list" => Ok(PropertyType::List),
        _ => Err(HawDBError::Storage(format!(
            "invalid property type: {input}"
        ))),
    }
}

pub fn encode_index_kind(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Equality => "equality",
        IndexKind::Range => "range",
        IndexKind::FullText => "fulltext",
    }
}

pub fn decode_index_kind(input: &str) -> Result<IndexKind> {
    match input {
        "equality" => Ok(IndexKind::Equality),
        "range" => Ok(IndexKind::Range),
        "fulltext" => Ok(IndexKind::FullText),
        _ => Err(HawDBError::Storage(format!("invalid index kind: {input}"))),
    }
}

pub fn encode_nullable(nullable: bool) -> &'static str {
    if nullable {
        "nullable"
    } else {
        "not_null"
    }
}

pub fn decode_nullable(input: &str) -> Result<bool> {
    match input {
        "nullable" => Ok(true),
        "not_null" => Ok(false),
        _ => Err(HawDBError::Storage(format!(
            "invalid nullable flag: {input}"
        ))),
    }
}

pub fn encode_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

pub fn decode_bool(input: &str, name: &str) -> Result<bool> {
    match input {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(HawDBError::Storage(format!("invalid {name}: {input}"))),
    }
}

pub fn encode_schema_object_state(state: SchemaObjectState) -> &'static str {
    match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    }
}

pub fn decode_schema_object_state(input: &str) -> Result<SchemaObjectState> {
    match input {
        "delete_only" => Ok(SchemaObjectState::DeleteOnly),
        "write_only" => Ok(SchemaObjectState::WriteOnly),
        "backfill" => Ok(SchemaObjectState::Backfill),
        "validating" => Ok(SchemaObjectState::Validating),
        "public" => Ok(SchemaObjectState::Public),
        "gc" => Ok(SchemaObjectState::Gc),
        _ => Err(HawDBError::Storage(format!(
            "invalid schema object state: {input}"
        ))),
    }
}

#[cfg(test)]
mod tests;
