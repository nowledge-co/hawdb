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

//! Shared JSON-to-`Value` parsing helpers for query-probe and route-catalog
//! inventories.

use hawdb_core::{HawDBError, Result, Value};
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
        HawDBError::Semantic(format!("{context} field 'parameters' must be an object"))
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
                Err(HawDBError::Semantic(format!(
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
