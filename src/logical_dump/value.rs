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

//! The dump's value encoding (docs/LOGICAL_DUMP.md, "Values").
//!
//! Every value maps to exactly one JSON form, and every JSON object is a
//! single-key tag, so a reader never has to guess. Null, booleans, strings,
//! lists and `i64` integers use plain JSON. Everything else is tagged:
//! `$float`, `$decimal`, `$binary` (standard padded base64), `$uuid`,
//! `$vector`, the temporal tags shared with the C ABI, and `$map`.

use std::collections::BTreeMap;

use serde_json::{json, Map, Number, Value as Json};

use crate::{HawDBError, Result, Uuid, Value};

pub(super) fn encode(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(item) => Json::Bool(*item),
        Value::Int(item) => Json::Number(Number::from(*item)),
        Value::Float(item) => json!({ "$float": encode_float(*item) }),
        Value::String(item) => Json::String(item.clone()),
        Value::Binary(item) => json!({ "$binary": base64_encode(item) }),
        Value::Uuid(item) => json!({ "$uuid": item.to_string() }),
        Value::Decimal(item) => json!({ "$decimal": item.to_string() }),
        Value::Vector(item) => json!({
            "$vector": item
                .as_ref()
                .coordinates()
                .map(|coordinate| encode_float(f64::from(coordinate)))
                .collect::<Vec<_>>()
        }),
        Value::Date(_)
        | Value::LocalTime(_)
        | Value::LocalDateTime(_)
        | Value::DateTime(_)
        | Value::Duration(_) => {
            let tag = crate::temporal::json_tag(value).expect("temporal value has a JSON tag");
            json!({ tag: value.to_string() })
        }
        Value::List(items) => Json::Array(items.iter().map(encode).collect()),
        Value::Map(items) => json!({ "$map": encode_properties(items) }),
    }
}

/// A property map at the top of a node or relationship record. Its keys are
/// property names, so it needs no `$map` tag.
pub(super) fn encode_properties(items: &BTreeMap<String, Value>) -> Json {
    Json::Object(
        items
            .iter()
            .map(|(key, item)| (key.clone(), encode(item)))
            .collect::<Map<_, _>>(),
    )
}

fn encode_float(value: f64) -> Json {
    if value.is_nan() {
        Json::String("NaN".to_string())
    } else if value == f64::INFINITY {
        Json::String("Infinity".to_string())
    } else if value == f64::NEG_INFINITY {
        Json::String("-Infinity".to_string())
    } else {
        Number::from_f64(value).map_or(Json::Null, Json::Number)
    }
}

pub(super) fn decode(value: &Json) -> Result<Value> {
    Ok(match value {
        Json::Null => Value::Null,
        Json::Bool(item) => Value::Bool(*item),
        Json::Number(item) => Value::Int(
            item.as_i64()
                .ok_or_else(|| malformed(format!("integer {item} is not a signed 64-bit value")))?,
        ),
        Json::String(item) => Value::String(item.clone()),
        Json::Array(items) => Value::List(items.iter().map(decode).collect::<Result<_>>()?),
        Json::Object(items) => {
            let mut entries = items.iter();
            let (Some((tag, payload)), None) = (entries.next(), entries.next()) else {
                return Err(malformed("a value object must have exactly one tag key"));
            };
            decode_tagged(tag, payload)?
        }
    })
}

pub(super) fn decode_properties(value: &Json) -> Result<BTreeMap<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| malformed("properties must be a JSON object"))?
        .iter()
        .map(|(key, item)| Ok((key.clone(), decode(item)?)))
        .collect()
}

fn decode_tagged(tag: &str, payload: &Json) -> Result<Value> {
    let text = || {
        payload
            .as_str()
            .ok_or_else(|| malformed(format!("{tag} payload must be a string")))
    };
    Ok(match tag {
        "$float" => Value::Float(decode_float(payload)?),
        "$decimal" => Value::Decimal(
            text()?
                .parse()
                .map_err(|error| malformed(format!("invalid $decimal: {error}")))?,
        ),
        "$binary" => Value::Binary(base64_decode(text()?)?),
        "$uuid" => Value::Uuid(
            text()?
                .parse::<Uuid>()
                .map_err(|error| malformed(format!("invalid $uuid: {error}")))?,
        ),
        "$vector" => {
            let coordinates = payload
                .as_array()
                .ok_or_else(|| malformed("$vector payload must be an array"))?
                .iter()
                .map(|coordinate| decode_float(coordinate).map(|value| value as f32))
                .collect::<Result<Vec<_>>>()?;
            Value::Vector(
                crate::Vector::new(&coordinates)
                    .map_err(|error| malformed(format!("invalid $vector: {error}")))?,
            )
        }
        "$map" => Value::Map(decode_properties(payload)?),
        _ => match crate::temporal::from_json_tag(tag, text()?) {
            Some(Ok(value)) => value,
            Some(Err(error)) => return Err(malformed(format!("invalid {tag}: {error}"))),
            None => return Err(malformed(format!("unknown value tag {tag}"))),
        },
    })
}

fn decode_float(value: &Json) -> Result<f64> {
    match value {
        Json::Number(number) => number
            .as_f64()
            .ok_or_else(|| malformed("float is out of range")),
        Json::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(malformed(format!("invalid float {text}"))),
        },
        _ => Err(malformed(
            "float must be a number or NaN/Infinity/-Infinity",
        )),
    }
}

pub(super) fn malformed(message: impl std::fmt::Display) -> HawDBError {
    HawDBError::StorageIntegrity(format!("logical dump: {message}"))
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub(super) fn base64_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let word = (u32::from(chunk[0]) << 16)
            | (u32::from(chunk.get(1).copied().unwrap_or(0)) << 8)
            | u32::from(chunk.get(2).copied().unwrap_or(0));
        for position in 0..4 {
            if position <= chunk.len() {
                let index = (word >> (18 - 6 * position)) & 0x3f;
                output.push(char::from(BASE64_ALPHABET[index as usize]));
            } else {
                output.push('=');
            }
        }
    }
    output
}

pub(super) fn base64_decode(text: &str) -> Result<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(malformed("$binary length is not a multiple of 4"));
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    for (chunk_index, chunk) in bytes.chunks(4).enumerate() {
        let last = chunk_index + 1 == bytes.len() / 4;
        let padding = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        if padding > 2 || (padding > 0 && !last) {
            return Err(malformed("$binary has misplaced padding"));
        }
        let mut word = 0u32;
        for byte in &chunk[..4 - padding] {
            let digit = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => return Err(malformed("$binary is not standard base64")),
            };
            word = (word << 6) | u32::from(digit);
        }
        word <<= 6 * padding as u32;
        let decoded = [(word >> 16) as u8, (word >> 8) as u8, word as u8];
        output.extend_from_slice(&decoded[..3 - padding]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_every_padding_length() {
        for length in 0..64usize {
            let bytes = (0..length)
                .map(|index| (index * 37 + 11) as u8)
                .collect::<Vec<_>>();
            let encoded = base64_encode(&bytes);
            assert_eq!(base64_decode(&encoded).unwrap(), bytes, "{encoded}");
        }
        assert_eq!(base64_encode(b"hawdb"), "aGF3ZGI=");
        assert!(base64_decode("aGF3ZGI").is_err());
        assert!(base64_decode("aG=3ZGI=").is_err());
        assert!(base64_decode("aGF3ZG*=").is_err());
    }

    #[test]
    fn values_round_trip_through_one_unambiguous_form() {
        let values = [
            Value::Null,
            Value::Bool(true),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Float(-0.0),
            Value::Float(1.0),
            Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY),
            Value::String("李雷 says {\"$map\": 1}".to_string()),
            Value::Binary(vec![0, 1, 254, 255]),
            Value::Decimal("-12345678901234567890.123456789".parse().unwrap()),
            Value::Vector(crate::Vector::new(&[0.1, -2.5, f32::MIN_POSITIVE]).unwrap()),
            Value::List(vec![Value::Int(1), Value::Map(BTreeMap::new())]),
            Value::Map(BTreeMap::from([(
                "$binary".to_string(),
                Value::String("not binary".to_string()),
            )])),
        ];
        for value in values {
            let encoded = encode(&value);
            let text = serde_json::to_string(&encoded).unwrap();
            let decoded = decode(&serde_json::from_str(&text).unwrap()).unwrap();
            match (&value, &decoded) {
                (Value::Float(left), Value::Float(right)) => {
                    assert_eq!(left.to_bits(), right.to_bits(), "{text}");
                }
                _ => assert_eq!(decoded, value, "{text}"),
            }
        }
        assert!(decode(&json!({ "$float": 1.0, "$uuid": "x" })).is_err());
        assert!(decode(&json!({ "$future": "x" })).is_err());
        assert!(decode(&json!(1.5)).is_err());
    }
}
