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

use super::test_support::row;
use super::*;

fn campaign(seeds: u32) {
    for seed in 0..seeds {
        // Integer-only nested values make sorted row equality an independent
        // multiset oracle, without reproducing tolerance-based matching.
        let expected = (0..(seed % 9))
            .map(|index| {
                row([
                    ("id", Value::Int(i64::from(index % 3))),
                    (
                        "nested",
                        Value::List(vec![Value::Null, Value::Int(i64::from(index % 3))]),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        for mutation in 0..5 {
            let mut actual = expected.clone();
            actual.reverse();
            match mutation {
                0 => {}
                1 => {
                    actual.pop();
                }
                2 => actual.push(row([("extra", Value::Bool(true))])),
                3 => {
                    if let Some(first) = actual.first_mut() {
                        first.insert("id".to_string(), Value::Int(-1));
                    }
                }
                4 => {
                    if actual.len() > 1 {
                        actual[0] = actual[1].clone();
                    }
                }
                _ => unreachable!(),
            }
            let mut expected_sorted = expected.clone();
            let mut actual_sorted = actual.clone();
            expected_sorted.sort();
            actual_sorted.sort();
            let output = QueryOutput::from_rows(actual.clone());
            assert_eq!(
                ExpectedRows::Unordered(expected.clone())
                    .assert_matches(
                        "generated",
                        "bag",
                        "rows",
                        &output,
                        CompatibilityTolerance::default()
                    )
                    .is_ok(),
                expected_sorted == actual_sorted,
                "seed={seed} mutation={mutation}"
            );
            assert_eq!(
                ExpectedRows::Exact(expected.clone())
                    .assert_matches(
                        "generated",
                        "ordered",
                        "rows",
                        &output,
                        CompatibilityTolerance::default()
                    )
                    .is_ok(),
                expected == actual
            );
            assert_eq!(
                ExpectedRows::RowCount(expected.len())
                    .assert_matches(
                        "generated",
                        "count",
                        "rows",
                        &output,
                        CompatibilityTolerance::default()
                    )
                    .is_ok(),
                expected.len() == actual.len()
            );
        }

        let values = [
            Value::Null,
            Value::Bool(seed % 2 == 0),
            Value::Int(i64::from(seed)),
            Value::String(format!("value-{seed}")),
            Value::Binary(seed.to_le_bytes().to_vec()),
            Value::List(vec![Value::Int(-1), Value::Null]),
            Value::Map(row([("value", Value::Int(i64::from(seed)))])),
        ];
        for value in values {
            let json = external_shadow_json_from_value(value.clone());
            assert_eq!(external_shadow_value_from_json(&json).unwrap(), value);
        }

        let mask = seed as u8;
        let checks = (0..8)
            .map(|index| CompatibilityShadowCheckReport {
                name: format!("check-{index}"),
                status: if mask & (1 << index) == 0 {
                    CompatibilityShadowStatus::PrimaryOnly
                } else {
                    CompatibilityShadowStatus::Matched
                },
                primary_only_reason: None,
            })
            .collect::<Vec<_>>();
        let report = CompatibilityShadowReport {
            fixture: "generated".to_string(),
            shadow_engine: "oracle".to_string(),
            primary_checks: (0..8)
                .map(|index| CompatibilityCheckReport {
                    name: format!("check-{index}"),
                })
                .collect(),
            shadow_checks: checks,
        };
        let matched = mask.count_ones() as usize;
        for strict in [false, true] {
            for minimum in 0..=9 {
                let result = assess_compatibility_cutover(
                    &report,
                    CompatibilityCutoverPolicy {
                        require_shadow_for_all_checks: strict,
                        min_matched_checks: minimum,
                    },
                );
                assert_eq!(result.matched_checks, matched);
                assert_eq!(result.total_checks, 8);
                assert_eq!(result.primary_only_checks.len(), 8 - matched);
                assert_eq!(
                    result.decision == CompatibilityCutoverDecision::Ready,
                    matched >= minimum && (!strict || matched == 8)
                );
            }
        }
    }
}

#[test]
fn compatibility_contract_differential_smoke() {
    campaign(8);
}

#[test]
#[ignore = "complete local compatibility row, codec, and cutover campaign"]
fn compatibility_contract_differential_campaign() {
    campaign(256);
}
