use crate::{LogicalPlan, SetAssignment, SetValue};
use serde_json::Value as Json;
use skein_core::Value;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn parameters(input: &Json) -> BTreeMap<String, Value> {
    fn value(input: &Json) -> Value {
        match input {
            Json::Null => Value::Null,
            Json::Bool(value) => Value::Bool(*value),
            Json::Number(value) if value.is_i64() => Value::Int(value.as_i64().unwrap()),
            Json::Number(value) => Value::Float(value.as_f64().unwrap()),
            Json::String(value) => Value::String(value.clone()),
            Json::Array(values) => Value::List(values.iter().map(value).collect()),
            Json::Object(_) => panic!("unsupported fixture parameter"),
        }
    }
    input
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, input)| (name.clone(), value(input)))
        .collect()
}

pub(super) fn clock_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

// Slots are explicit fixture data. Never erase arbitrary integers or all values
// of a property: a wrong binding outside these exact clock slots must still fail.
pub(super) fn normalize_clock_slots(
    plan: &mut LogicalPlan,
    slots: &Json,
    window: RangeInclusive<i64>,
) {
    for slot in slots.as_array().unwrap() {
        let property = slot["property"].as_str().unwrap();
        let value = match (slot["kind"].as_str().unwrap(), &mut *plan) {
            ("create", LogicalPlan::CreateNode { properties, .. }) => properties
                .get_mut(property)
                .expect("missing clock property"),
            (
                "set",
                LogicalPlan::SetNodeProperty {
                    property: actual,
                    value,
                    ..
                },
            ) => {
                assert_eq!(actual, property);
                literal(value)
            }
            ("set", LogicalPlan::SetNodeProperties { assignments, .. }) => {
                assignment(assignments, property)
            }
            (
                "merge_post",
                LogicalPlan::MergeNode {
                    post_merge_assignments,
                    ..
                },
            ) => assignment(post_merge_assignments, property),
            (
                "merge_create",
                LogicalPlan::MergeNode {
                    on_create_properties,
                    ..
                },
            ) => on_create_properties
                .get_mut(property)
                .expect("missing clock property"),
            (kind, _) => panic!("clock slot does not match plan: {kind}.{property}"),
        };
        let Value::Int(nanos) = value else {
            panic!("clock slot is not an integer")
        };
        assert!(
            window.contains(nanos),
            "clock slot {property} outside planning interval: {nanos}"
        );
        *nanos = 0;
    }
}

fn assignment<'a>(assignments: &'a mut [SetAssignment], property: &str) -> &'a mut Value {
    let mut matches = assignments
        .iter_mut()
        .filter(|item| item.property == property);
    let item = matches.next().expect("missing clock assignment");
    assert!(matches.next().is_none(), "ambiguous clock assignment");
    literal(&mut item.value)
}

fn literal(value: &mut SetValue) -> &mut Value {
    let SetValue::Value(value) = value else {
        panic!("clock assignment is not a literal")
    };
    value
}
