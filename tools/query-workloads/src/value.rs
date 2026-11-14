// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Value;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

pub fn typed_value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({"type": "null"}),
        Value::Bool(value) => json!({"type": "bool", "value": value}),
        Value::Int(value) => json!({"type": "int", "value": value}),
        Value::Decimal(value) => json!({"type": "decimal", "value": value.to_string()}),
        Value::Float(value) => json!({
            "type": "float",
            "bits": format!("{:016x}", value.to_bits()),
        }),
        Value::String(value) => json!({"type": "string", "value": value}),
        Value::Uuid(value) => json!({"type": "uuid", "value": value.to_string()}),
        Value::Date(_)
        | Value::LocalTime(_)
        | Value::LocalDateTime(_)
        | Value::DateTime(_)
        | Value::Duration(_) => json!({
            "type": &hawdb_core::temporal::json_tag(value).expect("temporal value")[1..],
            "value": value.to_string(),
        }),
        Value::Vector(value) => json!({
            "type": "vector",
            "hex": value.encoded().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        Value::Binary(value) => json!({
            "type": "binary",
            "hex": value.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        Value::List(values) => json!({
            "type": "list",
            "values": values.iter().map(typed_value_json).collect::<Vec<_>>(),
        }),
        Value::Map(values) => {
            let values = values
                .iter()
                .map(|(key, value)| (key.clone(), typed_value_json(value)))
                .collect::<JsonMap<_, _>>();
            json!({"type": "map", "values": values})
        }
    }
}
