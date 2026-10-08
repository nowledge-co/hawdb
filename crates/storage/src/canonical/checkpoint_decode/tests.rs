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

fn payload(value: &Value, relationship: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    if relationship {
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&2u64.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
    } else {
        bytes.extend_from_slice(&0u32.to_le_bytes());
    }
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    encode_value(value, &mut bytes, 1).unwrap();
    bytes
}

fn encoded(properties: &BTreeMap<String, Value>) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_properties(properties, &mut bytes, 0).unwrap();
    bytes
}

#[test]
fn checkpoint_units_canonical_decode_matches_independent_all_value_and_float_bit_results() {
    let values = [
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Float(-0.0),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::String(format!("{}🌏last", "界".repeat(21_846))),
        Value::Binary(vec![0xa5; 64 * 1024 + 7]),
        Value::Uuid(hawdb_core::Uuid::from_bytes([0x3f; 16])),
        Value::List(vec![Value::Null, Value::List(Vec::new())]),
        Value::Map(BTreeMap::from([
            ("空".into(), Value::Map(BTreeMap::new())),
            ("list".into(), Value::List(vec![Value::Float(-0.0)])),
        ])),
    ];
    let keys = vec!["p".into()];
    let work = CheckpointWorkContext::new(hawdb_qos::RuntimeTaskContext::default());
    for value in values {
        let bytes = payload(&value, false);
        let ordinary = decode_node_with_property_spills(9, &bytes, None, Some(&keys)).unwrap();
        let controlled = node(9, &bytes, None, &keys, &work).unwrap();
        assert_eq!(controlled.id, ordinary.id);
        assert_eq!(controlled.labels, ordinary.labels);
        assert_eq!(
            encoded(&controlled.properties),
            encoded(&ordinary.properties)
        );
        let bytes = payload(&value, true);
        let ordinary =
            decode_relationship_with_property_spills(17, &bytes, None, Some(&keys)).unwrap();
        let controlled = relationship(17, &bytes, None, &keys, &work).unwrap();
        assert_eq!(controlled.id, ordinary.id);
        assert_eq!(controlled.source, ordinary.source);
        assert_eq!(controlled.target, ordinary.target);
        assert_eq!(controlled.rel_type, ordinary.rel_type);
        assert_eq!(
            encoded(&controlled.properties),
            encoded(&ordinary.properties)
        );
    }
}

#[test]
fn checkpoint_units_canonical_decode_matches_independent_corrupt_record_rejection() {
    let keys = vec!["p".into()];
    let work = CheckpointWorkContext::new(hawdb_qos::RuntimeTaskContext::default());
    for is_relationship in [false, true] {
        let base = payload(
            &Value::List(vec![Value::Int(1), Value::String("界".into())]),
            is_relationship,
        );
        let mut cases = (0..base.len())
            .map(|end| base[..end].to_vec())
            .collect::<Vec<_>>();
        let mut trailing = base.clone();
        trailing.push(0);
        cases.push(trailing);
        let mut unknown_key = base.clone();
        let offset = if is_relationship { 24 } else { 8 };
        unknown_key[offset..offset + 4].copy_from_slice(&99u32.to_le_bytes());
        cases.push(unknown_key);
        let mut invalid_boolean = payload(&Value::Bool(true), is_relationship);
        *invalid_boolean.last_mut().unwrap() = 2;
        cases.push(invalid_boolean);
        let mut invalid_utf8 = payload(&Value::String("x".into()), is_relationship);
        *invalid_utf8.last_mut().unwrap() = 0xff;
        cases.push(invalid_utf8);
        let mut unknown_tag = payload(&Value::Null, is_relationship);
        *unknown_tag.last_mut().unwrap() = 99;
        cases.push(unknown_tag);
        for bytes in cases {
            if is_relationship {
                assert!(
                    decode_relationship_with_property_spills(17, &bytes, None, Some(&keys))
                        .is_err()
                );
                assert!(matches!(
                    relationship(17, &bytes, None, &keys, &work),
                    Err(CanonicalSegmentError::Corrupt(_))
                ));
            } else {
                assert!(decode_node_with_property_spills(9, &bytes, None, Some(&keys)).is_err());
                assert!(matches!(
                    node(9, &bytes, None, &keys, &work),
                    Err(CanonicalSegmentError::Corrupt(_))
                ));
            }
        }
    }
}
