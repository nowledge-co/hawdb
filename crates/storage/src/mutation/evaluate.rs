//! Shared SET value semantics for storage mutation and executor preflight.
//!
//! Applying a sequence mutates the supplied staging map in assignment order.
//! Callers retain validation, rollback, and publication responsibility.

use super::{NodeSetAssignment, NodeSetValue};
use skein_core::{Result, SkeinError, Value};
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

pub fn evaluate_node_set_value(
    properties: &BTreeMap<String, Value>,
    assignment: &NodeSetAssignment,
) -> Result<Value> {
    match &assignment.value {
        NodeSetValue::Value(value) => Ok(value.clone()),
        NodeSetValue::Coalesce { default } => Ok(match properties.get(&assignment.property) {
            None | Some(Value::Null) => default.clone(),
            Some(value) => value.clone(),
        }),
        NodeSetValue::AddInt { amount } => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property increment requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(current.checked_add(*amount).ok_or_else(
                || SkeinError::Execution("property increment overflowed i64".to_string()),
            )?))
        }
        NodeSetValue::DecrementFloorZero => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property decrement requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(if current > 0 { current - 1 } else { 0 }))
        }
        NodeSetValue::PreserveNewerExisting { incoming, preserve } => {
            let current = properties
                .get(&assignment.property)
                .cloned()
                .unwrap_or(Value::Null);
            if incoming == &Value::Null {
                return Ok(current);
            }
            if *preserve
                && current != Value::Null
                && value_gt_for_preserve_newer_existing(&current, incoming)
            {
                return Ok(current);
            }
            Ok(incoming.clone())
        }
    }
}

fn value_gt_for_preserve_newer_existing(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left > right,
        (Value::Float(left), Value::Float(right)) => left > right,
        (Value::Int(left), Value::Float(right)) => (*left as f64) > *right,
        (Value::Float(left), Value::Int(right)) => *left > (*right as f64),
        (Value::String(left), Value::String(right)) => left > right,
        _ => false,
    }
}

pub fn apply_node_assignments_to_properties(
    properties: &mut BTreeMap<String, Value>,
    assignments: &[NodeSetAssignment],
) -> Result<()> {
    for assignment in assignments {
        let value = evaluate_node_set_value(properties, assignment)?;
        properties.insert(assignment.property.clone(), value);
    }
    Ok(())
}
