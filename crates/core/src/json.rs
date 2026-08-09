use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::{Result, SkeinError, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsonDocument {
    canonical: Arc<str>,
}

impl JsonDocument {
    pub fn parse(input: &str) -> Result<Self> {
        let value = serde_json::from_str(input)
            .map_err(|error| SkeinError::Semantic(format!("invalid JSON value: {error}")))?;
        validate_json_number_range(&value)?;
        let canonical = serde_json::to_string(&value)
            .map_err(|error| SkeinError::Semantic(format!("failed to encode JSON: {error}")))?;
        Ok(Self {
            canonical: Arc::from(canonical),
        })
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let value = json_from_value(value)?;
        let canonical = serde_json::to_string(&value)
            .map_err(|error| SkeinError::Semantic(format!("failed to encode JSON: {error}")))?;
        Ok(Self {
            canonical: Arc::from(canonical),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn len(&self) -> usize {
        self.canonical.len()
    }

    pub fn is_empty(&self) -> bool {
        self.canonical.is_empty()
    }

    pub fn to_value(&self) -> Result<Value> {
        let value = serde_json::from_str(&self.canonical).map_err(|error| {
            SkeinError::StorageIntegrity(format!("stored JSON failed validation: {error}"))
        })?;
        value_from_json(value)
    }

    pub fn extract(&self, path: &str) -> Result<Option<Value>> {
        let value: serde_json::Value = serde_json::from_str(&self.canonical).map_err(|error| {
            SkeinError::StorageIntegrity(format!("stored JSON failed validation: {error}"))
        })?;
        let tokens = parse_json_path(path)?;
        let mut current = &value;
        for token in tokens {
            let Some(next) = (match token {
                JsonPathToken::Key(key) => current.get(&key),
                JsonPathToken::Index(index) => current.get(index),
            }) else {
                return Ok(None);
            };
            current = next;
        }
        value_from_json(current.clone()).map(Some)
    }
}

impl Display for JsonDocument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.canonical)
    }
}

fn validate_json_number_range(value: &serde_json::Value) -> Result<()> {
    match value {
        serde_json::Value::Number(number)
            if (number.as_u64().is_some() && number.as_i64().is_none())
                || number.as_f64().is_none() =>
        {
            Err(SkeinError::Semantic(
                "JSON number is outside Skein's BIGINT and DOUBLE PRECISION range".to_string(),
            ))
        }
        serde_json::Value::Array(values) => {
            for value in values {
                validate_json_number_range(value)?;
            }
            Ok(())
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                validate_json_number_range(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn json_from_value(value: &Value) -> Result<serde_json::Value> {
    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(value) => Ok(serde_json::Value::Bool(*value)),
        Value::Int(value) => Ok(serde_json::Value::Number((*value).into())),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| SkeinError::Semantic("JSON numbers must be finite".to_string())),
        Value::String(value) => Ok(serde_json::Value::String(value.clone())),
        Value::List(values) => values
            .iter()
            .map(json_from_value)
            .collect::<Result<Vec<_>>>()
            .map(serde_json::Value::Array),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_from_value(value)?)))
            .collect::<Result<serde_json::Map<_, _>>>()
            .map(serde_json::Value::Object),
    }
}

fn value_from_json(value: serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::StorageIntegrity(
                    "stored JSON number is outside Skein's numeric range".to_string(),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| Ok((key, value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathToken {
    Key(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> Result<Vec<JsonPathToken>> {
    let mut chars = path.char_indices().peekable();
    if chars.next().map(|(_, character)| character) != Some('$') {
        return Err(SkeinError::Semantic(
            "JSON path must start with $".to_string(),
        ));
    }
    let mut tokens = Vec::new();
    while let Some((_, character)) = chars.next() {
        match character {
            '.' => {
                let mut key = String::new();
                while let Some((_, next)) = chars.peek() {
                    if matches!(next, '.' | '[') {
                        break;
                    }
                    key.push(*next);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(SkeinError::Semantic(
                        "JSON object path components must be non-empty".to_string(),
                    ));
                }
                tokens.push(JsonPathToken::Key(key));
            }
            '[' => {
                let mut index = String::new();
                let mut closed = false;
                for (_, next) in chars.by_ref() {
                    if next == ']' {
                        closed = true;
                        break;
                    }
                    if !next.is_ascii_digit() {
                        return Err(SkeinError::Semantic(
                            "JSON array indexes must be non-negative integers".to_string(),
                        ));
                    }
                    index.push(next);
                }
                if !closed || index.is_empty() {
                    return Err(SkeinError::Semantic(
                        "JSON array path component is incomplete".to_string(),
                    ));
                }
                let index = index.parse().map_err(|_| {
                    SkeinError::Semantic("JSON array index overflows usize".to_string())
                })?;
                tokens.push(JsonPathToken::Index(index));
            }
            _ => {
                return Err(SkeinError::Semantic(format!(
                    "unsupported JSON path syntax after {character}"
                )));
            }
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_document_is_canonical_and_extracts_structured_values() {
        let document = JsonDocument::parse(r#"{ "z": 1, "a": { "items": [true, "value"] } }"#)
            .expect("valid JSON");
        assert_eq!(document.as_str(), r#"{"a":{"items":[true,"value"]},"z":1}"#);
        assert_eq!(
            document.extract("$.a.items[1]").expect("valid path"),
            Some(Value::String("value".to_string()))
        );
        assert_eq!(
            document.extract("$.missing").expect("valid missing path"),
            None
        );
    }

    #[test]
    fn json_document_rejects_numbers_that_cannot_round_trip() {
        let error = JsonDocument::parse("18446744073709551615")
            .expect_err("u64 values outside BIGINT must not lose precision");
        assert!(error.to_string().contains("BIGINT"));
    }
}
