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

use super::*;
use std::cmp::Ordering;

fn properties(value: Option<Value>) -> BTreeMap<String, Value> {
    value
        .into_iter()
        .map(|value| ("counter".into(), value))
        .collect()
}

fn evaluate(current: Option<Value>, value: NodeSetValue) -> Result<Value> {
    evaluate_node_set_value(
        &properties(current),
        &NodeSetAssignment {
            property: "counter".into(),
            value,
        },
    )
}

#[test]
fn ordered_assignments_share_the_updated_staging_value() {
    let mut values = properties(None);
    apply_node_assignments_to_properties(
        &mut values,
        &[
            NodeSetAssignment {
                property: "counter".into(),
                value: NodeSetValue::Coalesce {
                    default: Value::Int(9),
                },
            },
            NodeSetAssignment {
                property: "counter".into(),
                value: NodeSetValue::AddInt { amount: 2 },
            },
            NodeSetAssignment {
                property: "counter".into(),
                value: NodeSetValue::DecrementFloorZero,
            },
        ],
    )
    .unwrap();
    assert_eq!(values, properties(Some(Value::Int(10))));
}

#[test]
fn assignment_failure_does_not_publish_or_rollback_a_callers_staging_map() {
    let original = properties(Some(Value::Int(1)));
    let mut staging = original.clone();
    let error = apply_node_assignments_to_properties(
        &mut staging,
        &[
            NodeSetAssignment {
                property: "counter".into(),
                value: NodeSetValue::Value(Value::Int(i64::MAX)),
            },
            NodeSetAssignment {
                property: "counter".into(),
                value: NodeSetValue::AddInt { amount: 1 },
            },
        ],
    )
    .unwrap_err();
    assert!(
        matches!(error, HawDBError::Execution(message) if message == "property increment overflowed i64")
    );
    assert_eq!(original, properties(Some(Value::Int(1))));
    assert_eq!(staging, properties(Some(Value::Int(i64::MAX))));
}

#[test]
fn preserve_newer_uses_partial_numeric_order_not_query_total_order() {
    for (current, incoming, expected) in [
        (
            Value::Int(i64::MAX),
            Value::Int(i64::MAX - 1),
            Value::Int(i64::MAX),
        ),
        (Value::Float(f64::NAN), Value::Int(1), Value::Int(1)),
        (
            Value::Int(1),
            Value::Float(f64::NAN),
            Value::Float(f64::NAN),
        ),
        (Value::Float(0.0), Value::Float(-0.0), Value::Float(-0.0)),
        (
            Value::Float(f64::INFINITY),
            Value::Int(i64::MAX),
            Value::Float(f64::INFINITY),
        ),
        (
            Value::String("z".into()),
            Value::String("a".into()),
            Value::String("z".into()),
        ),
        (Value::String("z".into()), Value::Int(1), Value::Int(1)),
    ] {
        assert_eq!(
            evaluate(
                Some(current),
                NodeSetValue::PreserveNewerExisting {
                    incoming,
                    preserve: true,
                }
            )
            .unwrap(),
            expected
        );
    }
}

fn oracle(current: Option<&Value>, operation: &NodeSetValue) -> std::result::Result<Value, String> {
    let current = current.unwrap_or(&Value::Null);
    match operation {
        NodeSetValue::Value(value) => Ok(value.clone()),
        NodeSetValue::Coalesce { default } => Ok(if *current == Value::Null {
            default
        } else {
            current
        }
        .clone()),
        NodeSetValue::AddInt { .. } | NodeSetValue::DecrementFloorZero => {
            let number = match current {
                Value::Null => 0,
                Value::Int(value) => i128::from(*value),
                value => {
                    return Err(format!(
                        "property {} requires an integer or null value, got {value:?}",
                        if matches!(operation, NodeSetValue::AddInt { .. }) {
                            "increment"
                        } else {
                            "decrement"
                        },
                    ))
                }
            };
            let wide = match operation {
                NodeSetValue::AddInt { amount } => number + i128::from(*amount),
                _ => (number - 1).max(0),
            };
            i64::try_from(wide)
                .map(Value::Int)
                .map_err(|_| "property increment overflowed i64".into())
        }
        NodeSetValue::PreserveNewerExisting { incoming, preserve }
        | NodeSetValue::PreserveOlderExisting { incoming, preserve } => {
            let preferred = if matches!(operation, NodeSetValue::PreserveOlderExisting { .. }) {
                Ordering::Less
            } else {
                Ordering::Greater
            };
            let order = match (current, incoming) {
                (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
                (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
                (Value::Int(left), Value::Float(right)) => (*left as f64).partial_cmp(right),
                (Value::Float(left), Value::Int(right)) => left.partial_cmp(&(*right as f64)),
                (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
                _ => None,
            };
            Ok(
                if *incoming == Value::Null || (*preserve && order == Some(preferred)) {
                    current
                } else {
                    incoming
                }
                .clone(),
            )
        }
    }
}

fn campaign(seeds: u64) -> usize {
    let mut count = 0;
    for seed in 0..seeds {
        let bits = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(13);
        let inputs = [
            None,
            Some(Value::Null),
            Some(Value::Int(i64::MIN)),
            Some(Value::Int(i64::MAX)),
            Some(Value::Int(bits as i64)),
            Some(Value::Float(f64::from_bits(bits))),
            Some(Value::Float(f64::NAN)),
            Some(Value::Float(f64::INFINITY)),
            Some(Value::String(format!("value-{seed}"))),
            Some(Value::Bool(true)),
            Some(Value::List(vec![Value::Int(seed as i64)])),
            Some(Value::Map(BTreeMap::from([("nested".into(), Value::Null)]))),
        ];
        for current in &inputs {
            for incoming in &inputs {
                let incoming = incoming.clone().unwrap_or(Value::Null);
                let operations = [
                    NodeSetValue::Value(incoming.clone()),
                    NodeSetValue::Coalesce {
                        default: incoming.clone(),
                    },
                    NodeSetValue::AddInt {
                        amount: bits as i64,
                    },
                    NodeSetValue::AddInt { amount: i64::MIN },
                    NodeSetValue::AddInt { amount: i64::MAX },
                    NodeSetValue::DecrementFloorZero,
                    NodeSetValue::PreserveNewerExisting {
                        incoming: incoming.clone(),
                        preserve: false,
                    },
                    NodeSetValue::PreserveNewerExisting {
                        incoming,
                        preserve: true,
                    },
                ];
                for operation in operations {
                    let actual =
                        evaluate(current.clone(), operation.clone()).map_err(|error| match error {
                            HawDBError::Execution(message) => message,
                            error => panic!("unexpected error kind: {error}"),
                        });
                    assert_eq!(
                        actual,
                        oracle(current.as_ref(), &operation),
                        "seed {seed}, current {current:?}, operation {operation:?}"
                    );
                    count += 1;
                }
            }
        }
    }
    count
}

#[test]
fn set_value_generated_smoke() {
    assert_eq!(campaign(4), 4 * 12 * 12 * 8);
}

#[test]
#[ignore = "local differential campaign"]
fn set_value_differential_campaign() {
    let count = campaign(256);
    assert_eq!(count, 256 * 12 * 12 * 8);
    eprintln!("SET value evaluation: 256 seeds, {count} cases");
}

#[test]
fn minimum_preservation_handles_null_replacement_and_disabled_preservation() {
    for (current, incoming, preserve, expected) in [
        (Some(Value::Int(10)), Value::Int(20), true, Value::Int(10)),
        (Some(Value::Int(30)), Value::Int(20), true, Value::Int(20)),
        (None, Value::Int(20), true, Value::Int(20)),
        (Some(Value::Int(10)), Value::Null, false, Value::Int(10)),
        (Some(Value::Int(10)), Value::Int(20), false, Value::Int(20)),
    ] {
        let operation = NodeSetValue::PreserveOlderExisting { incoming, preserve };
        assert_eq!(oracle(current.as_ref(), &operation).unwrap(), expected);
        assert_eq!(evaluate(current, operation).unwrap(), expected);
    }
}

#[test]
fn range_preservation_uses_the_same_zoned_order_as_predicates() {
    let utc = Value::DateTime(
        hawdb_core::temporal::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z").unwrap(),
    );
    let east = Value::DateTime(
        hawdb_core::temporal::DateTime::parse_from_rfc3339("2026-01-01T01:00:00+01:00").unwrap(),
    );
    assert_eq!(
        hawdb_core::temporal::comparable_ordering(&utc, &east),
        Some(Ordering::Less)
    );
    assert_ne!(utc, east);
    assert_eq!(
        evaluate(
            Some(utc.clone()),
            NodeSetValue::PreserveOlderExisting {
                incoming: east.clone(),
                preserve: true
            }
        )
        .unwrap(),
        utc
    );
    assert_eq!(
        evaluate(
            Some(east.clone()),
            NodeSetValue::PreserveNewerExisting {
                incoming: utc.clone(),
                preserve: true
            }
        )
        .unwrap(),
        east
    );
    assert_eq!(
        evaluate(
            Some(east.clone()),
            NodeSetValue::PreserveOlderExisting {
                incoming: utc.clone(),
                preserve: true
            }
        )
        .unwrap(),
        utc
    );
    assert_eq!(
        evaluate(
            Some(utc.clone()),
            NodeSetValue::PreserveNewerExisting {
                incoming: east.clone(),
                preserve: true
            }
        )
        .unwrap(),
        east
    );
}
