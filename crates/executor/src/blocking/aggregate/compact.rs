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
use std::sync::Arc;

const COMPACT_AGGREGATE_ROW: &str = "__hawdb_compact_aggregate";

pub(super) fn encode_compact_group_binding(
    key: Vec<GroupKeyValue>,
    inputs: Vec<AggregateInput>,
) -> Binding {
    let inputs = inputs
        .into_iter()
        .map(encode_aggregate_input)
        .collect::<Vec<_>>();
    let mut binding = Binding::values(BTreeMap::new());
    let key = encode_group_key(key, &mut binding);
    binding.values.insert(
        COMPACT_AGGREGATE_ROW.to_string(),
        Value::List(vec![key, Value::List(inputs)]),
    );
    binding
}

/// Spill form of a group key (#1615). Value elements stay in the key list.
/// An entity element leaves NULL at its position and its record in the graph
/// slot named by that position, which the binding codec writes with its id,
/// labels or endpoints and every property. The merge has no graph access and
/// emits the record it reads back, so the record is written, not re-read.
pub(super) fn encode_group_key(key: Vec<GroupKeyValue>, binding: &mut Binding) -> Value {
    Value::List(
        key.into_iter()
            .enumerate()
            .map(|(position, value)| match value {
                GroupKeyValue::Value(value) => value,
                GroupKeyValue::Node(node) => {
                    binding.nodes.insert_arc(position.to_string(), node);
                    Value::Null
                }
                GroupKeyValue::Relationship(relationship) => {
                    binding
                        .relationships
                        .insert_arc(position.to_string(), relationship);
                    Value::Null
                }
            })
            .collect(),
    )
}

/// Reverses [`encode_group_key`]; `binding` must hold only the key's slots.
pub(super) fn decode_group_key(
    key: Vec<Value>,
    binding: &Binding,
    invalid: fn(&str) -> HawDBError,
) -> Result<Vec<GroupKeyValue>> {
    let mut decoded = key
        .into_iter()
        .map(GroupKeyValue::Value)
        .collect::<Vec<_>>();
    let mut place = |name: &str, entity: GroupKeyValue| {
        let slot = name
            .parse::<usize>()
            .ok()
            .filter(|position| position.to_string() == name)
            .and_then(|position| decoded.get_mut(position))
            .ok_or_else(|| invalid("has a graph key slot outside its key"))?;
        if *slot != GroupKeyValue::Value(Value::Null) {
            return Err(invalid("has a graph key slot over a non-null key value"));
        }
        *slot = entity;
        Ok(())
    };
    for name in binding.nodes.keys() {
        let node = binding.nodes.get_shared(name).expect("listed slot exists");
        place(name, GroupKeyValue::Node(Arc::clone(node)))?;
    }
    for name in binding.relationships.keys() {
        let relationship = binding
            .relationships
            .get_shared(name)
            .expect("listed slot exists");
        place(name, GroupKeyValue::Relationship(Arc::clone(relationship)))?;
    }
    Ok(decoded)
}

pub(super) fn decode_compact_group_binding(
    ordinal: u64,
    mut binding: Binding,
    expected_inputs: usize,
) -> Result<GroupRunRow> {
    if binding.values.len() != 1 {
        return Err(invalid_compact_row("has an invalid binding shape"));
    }
    let Some(Value::List(mut encoded)) = binding.values.remove(COMPACT_AGGREGATE_ROW) else {
        return Err(invalid_compact_row("is missing its payload"));
    };
    if encoded.len() != 2 {
        return Err(invalid_compact_row("has an invalid payload width"));
    }
    let Value::List(inputs) = encoded.pop().expect("compact payload width was checked") else {
        return Err(invalid_compact_row("has an invalid input list"));
    };
    let Value::List(key) = encoded.pop().expect("compact payload width was checked") else {
        return Err(invalid_compact_row("has an invalid group key"));
    };
    if inputs.len() != expected_inputs {
        return Err(invalid_compact_row("has an invalid input width"));
    }
    let inputs = inputs
        .into_iter()
        .map(decode_aggregate_input)
        .collect::<Result<Vec<_>>>()?;
    Ok(GroupRunRow {
        key: decode_group_key(key, &binding, invalid_compact_row)?,
        ordinal,
        inputs,
    })
}

fn encode_aggregate_input(input: AggregateInput) -> Value {
    let fields = match input {
        AggregateInput::Missing => vec![Value::Int(0)],
        AggregateInput::Present => vec![Value::Int(1)],
        AggregateInput::Identity(kind, id) => vec![
            Value::Int(2),
            Value::Int(i64::from(kind)),
            Value::Int((id >> u32::BITS) as i64),
            Value::Int((id & u64::from(u32::MAX)) as i64),
        ],
        AggregateInput::Value(value) => vec![Value::Int(3), value],
    };
    Value::List(fields)
}

fn decode_aggregate_input(value: Value) -> Result<AggregateInput> {
    let Value::List(mut fields) = value else {
        return Err(invalid_compact_row("contains a non-list input"));
    };
    let Some(Value::Int(tag)) = fields.first() else {
        return Err(invalid_compact_row("contains an untagged input"));
    };
    match *tag {
        0 if fields.len() == 1 => Ok(AggregateInput::Missing),
        1 if fields.len() == 1 => Ok(AggregateInput::Present),
        2 if fields.len() == 4 => {
            let (Value::Int(kind), Value::Int(high), Value::Int(low)) =
                (&fields[1], &fields[2], &fields[3])
            else {
                return Err(invalid_compact_row("contains an invalid identity input"));
            };
            let kind = u8::try_from(*kind)
                .map_err(|_| invalid_compact_row("contains an invalid identity kind"))?;
            let high = u32::try_from(*high)
                .map_err(|_| invalid_compact_row("contains invalid identity high bits"))?;
            let low = u32::try_from(*low)
                .map_err(|_| invalid_compact_row("contains invalid identity low bits"))?;
            Ok(AggregateInput::Identity(
                kind,
                (u64::from(high) << u32::BITS) | u64::from(low),
            ))
        }
        3 if fields.len() == 2 => Ok(AggregateInput::Value(fields.pop().expect("value exists"))),
        _ => Err(invalid_compact_row(
            "contains an invalid input tag or width",
        )),
    }
}

fn invalid_compact_row(reason: &str) -> HawDBError {
    HawDBError::Execution(format!("AggregateExec compact spill record {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_group_binding_round_trip_preserves_all_input_kinds() {
        let row = decode_compact_group_binding(
            7,
            encode_compact_group_binding(
                vec![GroupKeyValue::Value(Value::String("group".to_string()))],
                vec![
                    AggregateInput::Missing,
                    AggregateInput::Present,
                    AggregateInput::Identity(2, u64::MAX),
                    AggregateInput::Value(Value::Int(42)),
                ],
            ),
            4,
        )
        .unwrap();

        assert_eq!(row.ordinal, 7);
        assert_eq!(
            row.key,
            vec![GroupKeyValue::Value(Value::String("group".to_string()))]
        );
        assert!(matches!(row.inputs[0], AggregateInput::Missing));
        assert!(matches!(row.inputs[1], AggregateInput::Present));
        assert!(matches!(
            row.inputs[2],
            AggregateInput::Identity(2, u64::MAX)
        ));
        assert!(matches!(
            row.inputs[3],
            AggregateInput::Value(Value::Int(42))
        ));
    }
}
