// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Selection {
    Scalar,
    NullableScalar,
    String,
}

pub(super) fn select<'a>(
    input: &Value,
    path: &str,
    selection: Selection,
    account: Option<&QueryMemoryAccount>,
) -> Result<ProjectedValue<'a>> {
    let function = if selection == Selection::String {
        "JSON_STRING"
    } else {
        "JSON_VALUE"
    };
    let json = match input {
        Value::Null => return Ok(ProjectedValue::scalar(Value::Null)),
        Value::String(json) => json,
        value => {
            return Err(HawDBError::Execution(format!(
                "{function} requires a string document, got {:?}",
                description(value, account)
            )));
        }
    };
    const MAX_JSON_INPUT_BYTES: usize = 1024 * 1024;
    if json.len() > MAX_JSON_INPUT_BYTES {
        return Err(HawDBError::Execution(format!(
            "{function} document exceeds its 1 MiB input limit"
        )));
    }
    // Small JSON fields can expand far beyond their source bytes in the DOM.
    let _parse_memory = account
        .map(|account| account.reserve(json.len().saturating_mul(64)))
        .transpose()?;
    let document: serde_json::Value = serde_json::from_str(json).map_err(|error| {
        HawDBError::Execution(format!("{function} document is invalid JSON: {error}"))
    })?;
    let selected = path
        .trim_start_matches("$.")
        .split('.')
        .try_fold(&document, |value, field| value.as_object()?.get(field));
    if selection == Selection::String && !matches!(selected, Some(serde_json::Value::String(_))) {
        return Ok(ProjectedValue::scalar(Value::Null));
    }
    match selected {
        None | Some(serde_json::Value::Null) => Ok(ProjectedValue::scalar(Value::Null)),
        Some(serde_json::Value::Bool(value)) => Ok(ProjectedValue::scalar(Value::Bool(*value))),
        Some(serde_json::Value::Number(value)) => {
            if let Some(integer) = value.as_i64() {
                Ok(ProjectedValue::scalar(Value::Int(integer)))
            } else if value.as_u64().is_some() {
                Err(HawDBError::Execution(format!(
                    "{function} integer exceeds signed 64-bit range"
                )))
            } else if let Some(float) = value.as_f64() {
                Ok(ProjectedValue::scalar(Value::Float(float)))
            } else {
                Err(HawDBError::Execution(format!(
                    "{function} number cannot be represented"
                )))
            }
        }
        Some(serde_json::Value::String(value)) => allocated(
            std::mem::size_of::<Value>().saturating_add(value.len()),
            account,
            || Value::String(value.clone()),
        ),
        Some(_) if selection == Selection::NullableScalar => {
            Ok(ProjectedValue::scalar(Value::Null))
        }
        Some(_) => Err(HawDBError::Execution(format!(
            "{function} selected value is not scalar"
        ))),
    }
}
