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

// Build raw wire bytes independently: the ordinary encoder correctly refuses
// over-depth values, so using it would hide the decoder acceptance boundary.
fn nested_value(containers: &[bool], leaf: u8) -> Vec<u8> {
    let mut encoded = vec![leaf];
    if matches!(leaf, 5 | 6) {
        encoded.extend_from_slice(&0u32.to_le_bytes());
    }
    for &map in containers.iter().rev() {
        let mut parent = vec![if map { 6 } else { 5 }];
        parent.extend_from_slice(&1u32.to_le_bytes());
        if map {
            parent.extend_from_slice(&1u32.to_le_bytes());
            parent.push(b'k');
        }
        parent.extend_from_slice(&encoded);
        encoded = parent;
    }
    encoded
}

fn record_payload(value: &[u8], relationship: bool) -> Vec<u8> {
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
    bytes.extend_from_slice(value);
    bytes
}

#[test]
fn checkpoint_units_canonical_decode_rejects_twenty_nested_nonempty_maps() {
    let keys = vec!["p".to_string()];
    let work = CheckpointWorkContext::default();
    let value = nested_value(&[true; 20], 0);
    let bytes = record_payload(&value, false);
    assert!(matches!(
        decode_node_with_property_spills(9, &bytes, None, Some(&keys)),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
    assert!(
        matches!(
            node(9, &bytes, None, &keys, &work),
            Err(CanonicalSegmentError::Corrupt(_))
        ),
        "controlled canonical decoding must reject the ordinary map-depth violation"
    );
    let bytes = record_payload(&value, true);
    assert!(matches!(
        decode_relationship_with_property_spills(17, &bytes, None, Some(&keys)),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
    assert!(matches!(
        relationship(17, &bytes, None, &keys, &work),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
}

#[test]
fn checkpoint_units_canonical_decode_generated_depth_boundaries_match_ordinary() {
    let keys = vec!["p".to_string()];
    let work = CheckpointWorkContext::default();
    let mut accepted = 0;
    let mut rejected = 0;
    for depth in 0..=MAX_VALUE_DEPTH + 8 {
        for seed in 0..18u64 {
            let mut state = seed + 1;
            let containers = (0..depth)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    match seed {
                        0 => false,
                        1 => true,
                        _ => state & 1 == 1,
                    }
                })
                .collect::<Vec<_>>();
            for leaf in [0, 5, 6] {
                let value = nested_value(&containers, leaf);
                let bytes = record_payload(&value, false);
                let ordinary = decode_node_with_property_spills(9, &bytes, None, Some(&keys));
                let controlled = node(9, &bytes, None, &keys, &work);
                assert_eq!(
                    ordinary.is_ok(),
                    controlled.is_ok(),
                    "node depth={depth}, seed={seed}, leaf={leaf}"
                );
                match (ordinary, controlled) {
                    (Ok(ordinary), Ok(controlled)) => {
                        assert_eq!(ordinary.properties, controlled.properties);
                        accepted += 1;
                    }
                    (
                        Err(CanonicalSegmentError::Corrupt(a)),
                        Err(CanonicalSegmentError::Corrupt(b)),
                    ) => {
                        assert_eq!(a, b);
                        rejected += 1;
                    }
                    other => panic!("unexpected node result: {other:?}"),
                }
                let bytes = record_payload(&value, true);
                let ordinary =
                    decode_relationship_with_property_spills(17, &bytes, None, Some(&keys));
                let controlled = relationship(17, &bytes, None, &keys, &work);
                assert_eq!(
                    ordinary.is_ok(),
                    controlled.is_ok(),
                    "relationship depth={depth}, seed={seed}, leaf={leaf}"
                );
                match (ordinary, controlled) {
                    (Ok(ordinary), Ok(controlled)) => {
                        assert_eq!(ordinary.properties, controlled.properties)
                    }
                    (
                        Err(CanonicalSegmentError::Corrupt(a)),
                        Err(CanonicalSegmentError::Corrupt(b)),
                    ) => assert_eq!(a, b),
                    other => panic!("unexpected relationship result: {other:?}"),
                }
            }
        }
    }
    assert!(accepted > 0 && rejected > 0);
}
