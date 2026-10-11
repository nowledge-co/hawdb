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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

#[test]
fn checkpoint_units_predicate_evaluation_matches_ordinary_value_and_missing_property_semantics() {
    let wide = format!("{}\0界🦀", "a".repeat(64 * 1024 + 1));
    let values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(0),
        Value::Int(i64::MAX),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Float(f64::INFINITY),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0002)),
        Value::String(String::new()),
        Value::String(wide.clone()),
        Value::String(format!("{wide}z")),
        Value::Binary(vec![0xff; 64 * 1024 + 1]),
        Value::Uuid(hawdb_core::Uuid::from_u128(17)),
        Value::List(vec![Value::Int(0), Value::String(wide.clone())]),
        Value::Map(BTreeMap::from([(
            wide.clone(),
            Value::List(vec![Value::Binary(vec![0x9f; 64 * 1024 + 1])]),
        )])),
    ];
    let local = scheduler();
    for actual in &values {
        let properties = BTreeMap::from([
            ("a-before".into(), Value::Int(1)),
            (wide.clone(), actual.clone()),
            ("z-after".into(), Value::Bool(true)),
        ]);
        for expected in &values {
            for name in [&wide, &format!("{wide}missing")] {
                for predicate in [
                    ProjectedRelationshipPredicate::Eq {
                        property: name.clone(),
                        value: expected.clone(),
                    },
                    ProjectedRelationshipPredicate::Gte {
                        property: name.clone(),
                        value: expected.clone(),
                    },
                ] {
                    let probe = Arc::new(CheckpointWorkProbe::default());
                    assert_eq!(
                        matches(&predicate, &properties, &probe.context(local.clone())).unwrap(),
                        predicate.matches(&properties),
                        "actual type={:?}, expected type={:?}",
                        actual.logical_type(),
                        expected.logical_type()
                    );
                    assert_eq!(probe.peak_units.load(AtomicOrdering::SeqCst), 1);
                    probe.assert_released(&local);
                }
            }
        }
    }
}

#[test]
fn checkpoint_units_predicate_evaluation_splits_wide_comparisons_before_completion() {
    let local = scheduler();
    for value in [
        Value::Binary(vec![0x9f; 32 * 64 * 1024 + 1]),
        Value::String(format!("{}界", "a".repeat(32 * 64 * 1024 + 1))),
    ] {
        let bytes = match &value {
            Value::Binary(bytes) => bytes.len(),
            Value::String(text) => text.len(),
            _ => unreachable!(),
        };
        let properties = BTreeMap::from([("payload".into(), value.clone())]);
        let predicate = ProjectedRelationshipPredicate::Eq {
            property: "payload".into(),
            value,
        };
        let probe = Arc::new(CheckpointWorkProbe::default());
        assert!(matches(&predicate, &properties, &probe.context(local.clone())).unwrap());
        let minimum = bytes.div_ceil(64 * 1024);
        assert!(
            probe.completed.load(AtomicOrdering::SeqCst) >= minimum,
            "wide comparison must yield before processing another 64 KiB"
        );
        probe.assert_released(&local);
        let cancelled = Arc::new(CheckpointWorkProbe::default());
        cancelled
            .cancel_after
            .store(minimum / 2, AtomicOrdering::SeqCst);
        assert_eq!(
            matches(&predicate, &properties, &cancelled.context(local.clone()))
                .unwrap_err()
                .to_string(),
            "storage error: checkpoint build stopped: cancelled"
        );
        cancelled.assert_released(&local);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert!(matches(&predicate, &properties, &retry.context(local.clone())).unwrap());
        retry.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_predicate_evaluation_cancels_each_actual_unit_and_retries_complete_input() {
    let name = format!("{}界", "a".repeat(2 * 64 * 1024 + 1));
    let value = Value::Map(BTreeMap::from([(
        name.clone(),
        Value::List(vec![
            Value::String("\0界🦀".repeat(17000)),
            Value::Binary(vec![0x9f; 3 * 64 * 1024 + 1]),
            Value::Float(-0.0),
        ]),
    )]));
    let properties = BTreeMap::from([(name.clone(), value.clone())]);
    let predicates = [
        ProjectedRelationshipPredicate::And(Vec::new()),
        ProjectedRelationshipPredicate::And(vec![
            ProjectedRelationshipPredicate::Eq {
                property: name.clone(),
                value,
            },
            ProjectedRelationshipPredicate::And(vec![ProjectedRelationshipPredicate::Gte {
                property: name.clone(),
                value: Value::Int(1),
            }]),
        ]),
        ProjectedRelationshipPredicate::Gte {
            property: format!("{name}missing"),
            value: Value::String(name),
        },
    ];
    let local = scheduler();
    for predicate in predicates {
        let expected = predicate.matches(&properties);
        let probe = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            matches(&predicate, &properties, &probe.context(local.clone())).unwrap(),
            expected
        );
        let total = probe.completed.load(AtomicOrdering::SeqCst);
        probe.assert_released(&local);
        for cut in 1..=total {
            let cancelled = Arc::new(CheckpointWorkProbe::default());
            cancelled.cancel_after.store(cut, AtomicOrdering::SeqCst);
            assert_eq!(
                matches(&predicate, &properties, &cancelled.context(local.clone()))
                    .unwrap_err()
                    .to_string(),
                "storage error: checkpoint build stopped: cancelled"
            );
            assert_eq!(cancelled.completed.load(AtomicOrdering::SeqCst), cut);
            cancelled.assert_released(&local);
        }
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            matches(&predicate, &properties, &retry.context(local.clone())).unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
}
