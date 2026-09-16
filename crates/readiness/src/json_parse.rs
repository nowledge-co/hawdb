//! Shared JSON-to-`Value` parsing helpers for query-probe and route-catalog
//! inventories.

use skein_core::{Result, SkeinError, Value};
use std::collections::BTreeMap;

pub fn optional_query_name(value: &serde_json::Value) -> Option<String> {
    ["name", "query_id", "id"]
        .iter()
        .find_map(|field| value.get(*field).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

pub fn parse_parameters_json(
    value: &serde_json::Value,
    context: &str,
) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic(format!("{context} field 'parameters' must be an object"))
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value, context)?)))
        .collect()
}

pub fn value_from_json(value: &serde_json::Value, context: &str) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(format!(
                    "unsupported JSON number in {context} parameters: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| value_from_json(value, context))
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value, context)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}
