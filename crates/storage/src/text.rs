//! Existing graph checkpoint and spill text encodings.
//!
//! These are storage-owned implementation details, not a new host protocol.
//! Keep byte encodings, legacy decoding tolerance, and error messages stable.

use skein_core::{Result, SkeinError, Value};
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
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid property pair: {pair}"
            )));
        };
        properties.insert(decode_string(key)?, decode_value(value)?);
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

pub fn decode_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Err(SkeinError::Storage("empty encoded value".to_string()));
    }
    let (kind, rest) = input
        .split_at_checked(1)
        .ok_or_else(|| SkeinError::Storage("invalid encoded value tag".to_string()))?;
    match kind {
        "n" if rest.is_empty() => Ok(Value::Null),
        "b" => match rest {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(SkeinError::Storage(format!("invalid bool value: {input}"))),
        },
        "i" => parse_i64(rest, "integer value").map(Value::Int),
        "f" => parse_u64(rest, "float value")
            .map(f64::from_bits)
            .map(Value::Float),
        "s" => decode_string(rest).map(Value::String),
        "u" => skein_core::Uuid::parse_str(rest)
            .map(Value::Uuid)
            .map_err(|error| SkeinError::Storage(format!("invalid UUID value: {error}"))),
        "x" => decode_hex_value(rest),
        "l" => decode_list_value(rest),
        "m" => decode_map_value(rest),
        _ => Err(SkeinError::Storage(format!(
            "invalid encoded value tag or payload: {kind:?}"
        ))),
    }
}

fn decode_hex_value(input: &str) -> Result<Value> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(
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
        .map(Value::Binary)
}

fn decode_hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(SkeinError::Storage(format!(
            "binary value contains invalid hex digit {:?}",
            char::from(digit)
        ))),
    }
}

fn decode_list_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Ok(Value::List(Vec::new()));
    }
    input
        .split(',')
        .map(|item| decode_string(item).and_then(|value| decode_value(&value)))
        .collect::<Result<Vec<_>>>()
        .map(Value::List)
}

fn decode_map_value(input: &str) -> Result<Value> {
    let mut values = BTreeMap::new();
    if input.is_empty() {
        return Ok(Value::Map(values));
    }
    for item in input.split(';') {
        let Some((key, value)) = item.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid encoded map item: {item}"
            )));
        };
        values.insert(
            decode_string(key)?,
            decode_string(value).and_then(|value| decode_value(&value))?,
        );
    }
    Ok(Value::Map(values))
}

pub fn encode_string(input: &str) -> String {
    encode_bytes(input.as_bytes())
}

pub fn encode_bytes(input: &[u8]) -> String {
    input.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn decode_string(input: &str) -> Result<String> {
    let bytes = decode_bytes(input)?;
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

pub fn decode_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(format!(
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
                SkeinError::Storage(format!("invalid hex string at byte offset {offset}"))
            })?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

pub fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
mod tests;
