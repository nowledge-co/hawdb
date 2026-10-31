// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::mutation::MutationLimits;
use crate::relational::{
    RelationalColumnSchema, RelationalIndexSchema, RelationalInsertMode, RelationalRow,
    RelationalTableSchema, RelationalTransaction, RelationalWrite,
};
use crate::transaction_locks::LockMode;
use std::ops::RangeBounds;

fn key(values: &[i64]) -> RelationalKey {
    RelationalKey(
        values
            .iter()
            .copied()
            .map(RelationalValue::BigInt)
            .collect(),
    )
}

fn fixture() -> (GraphStore, Catalog) {
    fixture_with_group_type(RelationalScalarType::BigInt)
}

fn fixture_with_group_type(group_type: RelationalScalarType) -> (GraphStore, Catalog) {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    materialized_view: None,
                    name: "rows".into(),
                    columns: ["id", "group", "value"]
                        .into_iter()
                        .map(|name| RelationalColumnSchema {
                            name: name.into(),
                            scalar_type: if name == "group" {
                                group_type
                            } else {
                                RelationalScalarType::BigInt
                            },
                            nullable: false,
                            default: None,
                        })
                        .collect(),
                    primary_key: vec!["id".into()],
                    unique_constraints: vec![],
                    foreign_keys: vec![],
                    indexes: vec![RelationalIndexSchema {
                        name: "by_group".into(),
                        columns: vec!["group".into(), "id".into()],
                        unique: false,
                    }],
                })],
            },
        )
        .unwrap();
    (store, catalog)
}

fn insert(store: &mut GraphStore, catalog: &mut Catalog, id: i64, group: i64) {
    store
        .commit_relational_transaction(
            catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "rows".into(),
                    rows: vec![RelationalRow::new(key(&[id, group, 0]).0)],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();
}

fn range(index: bool, lower: Bound<RelationalKey>, upper: Bound<RelationalKey>) -> LockRequest {
    LockRequest::relational_range(
        LockMode::Exclusive,
        "rows",
        if index {
            vec!["group".into(), "id".into()]
        } else {
            vec!["id".into()]
        },
        lower,
        upper,
    )
}

#[test]
fn bounded_primary_and_secondary_versions_match_an_interval_oracle() {
    let (mut store, mut catalog) = fixture();
    insert(&mut store, &mut catalog, 1, 4);
    let captured = store.capture_read_dependencies(0, 0).unwrap();
    for id in [2, 4, 6] {
        insert(&mut store, &mut catalog, id, 5);
    }
    for index in [false, true] {
        let mut bounds = vec![Bound::Unbounded];
        for id in 0..=8 {
            let value = if index { key(&[5, id]) } else { key(&[id]) };
            bounds.extend([Bound::Included(value.clone()), Bound::Excluded(value)]);
        }
        // A short upper bound must not match the summary of an entire prefix.
        if index {
            bounds.push(Bound::Included(key(&[5])));
        }
        for lower in &bounds {
            for upper in &bounds {
                let changed = [2, 4, 6].iter().any(|&id| {
                    (lower.clone(), upper.clone()).contains(&if index {
                        key(&[5, id])
                    } else {
                        key(&[id])
                    })
                });
                let request = range(index, lower.clone(), upper.clone());
                let result = store.validate_locked_snapshot(&captured, &[request]);
                assert_eq!(
                    result.is_err(),
                    changed,
                    "index={index}, {lower:?}..{upper:?}: {result:?}"
                );
            }
        }
    }
}

#[test]
fn payload_updates_rekeys_and_tombstones_invalidate_index_ranges() {
    use crate::relational::{
        RelationalComparisonOp, RelationalPredicate, RelationalUpdateAssignment,
        RelationalUpdateValue,
    };
    for mode in 0..3 {
        let (mut store, mut catalog) = fixture();
        insert(&mut store, &mut catalog, 1, 4);
        let captured = store.capture_read_dependencies(0, 0).unwrap();
        let predicate = RelationalPredicate::Compare {
            column: "id".into(),
            op: RelationalComparisonOp::Eq,
            value: RelationalValue::BigInt(1),
        };
        let write = if mode == 2 {
            RelationalWrite::DeleteWhere {
                table: "rows".into(),
                predicate,
            }
        } else {
            RelationalWrite::UpdateWhere {
                table: "rows".into(),
                predicate,
                assignments: vec![RelationalUpdateAssignment {
                    column: if mode == 1 { "group" } else { "value" }.into(),
                    value: RelationalUpdateValue::Value(RelationalValue::BigInt(9)),
                }],
            }
        };
        let transaction = store.begin_mutation_transaction(&catalog);
        store
            .commit_mutation_transaction_and_relational(
                &mut catalog,
                transaction,
                RelationalTransaction {
                    writes: vec![write],
                },
                MutationLimits::default(),
            )
            .unwrap();
        let request = range(true, Bound::Included(key(&[4])), Bound::Excluded(key(&[5])));
        assert!(matches!(
            store.validate_locked_snapshot(&captured, &[request]),
            Err(GraphReadDependencyError::Conflict(_))
        ));
        let disjoint = range(true, Bound::Included(key(&[6])), Bound::Excluded(key(&[7])));
        store
            .validate_locked_snapshot(&captured, &[disjoint])
            .unwrap();
        if mode == 1 {
            let destination = range(
                true,
                Bound::Included(key(&[9])),
                Bound::Excluded(key(&[10])),
            );
            assert!(matches!(
                store.validate_locked_snapshot(&captured, &[destination]),
                Err(GraphReadDependencyError::Conflict(_))
            ));
        }
    }
}

#[test]
fn barriers_history_and_source_identity_refuse_uncertain_validation() {
    let (mut store, mut catalog) = fixture();
    let captured = store.capture_read_dependencies(0, 0).unwrap();
    insert(&mut store, &mut catalog, 9, 9);
    let request = range(
        false,
        Bound::Included(key(&[1])),
        Bound::Included(key(&[1])),
    );
    store
        .validate_locked_snapshot(&captured, std::slice::from_ref(&request))
        .unwrap();
    let (other, _) = fixture();
    assert!(matches!(
        other.validate_locked_snapshot(&captured, std::slice::from_ref(&request)),
        Err(GraphReadDependencyError::SourceChanged)
    ));
    // Simulate unavailable historical evidence independently of the barrier
    // that a production baseline replacement also installs.
    store.version_index = crate::version::VersionIndex::default();
    store
        .version_index
        .prune_tombstones_before(store.commit_epoch() + 1);
    assert!(matches!(
        store.validate_locked_snapshot(&captured, &[request]),
        Err(GraphReadDependencyError::CaptureRefused)
    ));
}

#[test]
fn oversized_secondary_keys_publish_an_index_barrier_without_poisoning_primary_points() {
    let (mut store, mut catalog) = fixture_with_group_type(RelationalScalarType::Text);
    let captured = store.capture_read_dependencies(0, 0).unwrap();
    store
        .commit_relational_transaction(
            &mut catalog,
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "rows".into(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::BigInt(2),
                        RelationalValue::Text(
                            "x".repeat(crate::version::MAX_RELATIONAL_INDEX_READ_KEY_BYTES),
                        ),
                        RelationalValue::BigInt(0),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
        )
        .unwrap();
    let secondary = range(true, Bound::Unbounded, Bound::Unbounded);
    assert!(
        matches!(store.validate_locked_snapshot(&captured, &[secondary]),
        Err(GraphReadDependencyError::Conflict(VersionConflict {
            key: VersionKey::ReadRelationalIndexPrefix { prefix, .. }, ..
        })) if prefix.0.is_empty())
    );
    let primary = range(
        false,
        Bound::Included(key(&[1])),
        Bound::Included(key(&[1])),
    );
    store
        .validate_locked_snapshot(&captured, &[primary])
        .unwrap();
}

#[test]
fn narrow_seeks_ignore_unrelated_history_and_broad_validation_has_a_work_limit() {
    let (mut store, _) = fixture();
    store.version_index = crate::version::VersionIndex::from_live_keys_at_epoch(
        (0..=MAX_STAMPS).map(|id| VersionKey::RelationalRow {
            table: "rows".into(),
            primary_key: key(&[id as i64]),
        }),
        store.commit_epoch(),
    );
    let captured = store.capture_read_dependencies(0, 0).unwrap();
    let point = Bound::Included(key(&[MAX_STAMPS as i64]));
    let mut budget = ValidationBudget::default();
    store
        .validate_range(
            &captured,
            VersionRange {
                table: "rows",
                index: None,
                lower: &point,
                upper: &point,
            },
            &mut budget,
        )
        .unwrap();
    assert_eq!(budget.stamps, 1);
    assert!(matches!(
        store.validate_locked_snapshot(
            &captured,
            &[range(false, Bound::Unbounded, Bound::Unbounded)]
        ),
        Err(GraphReadDependencyError::CaptureRefused)
    ));
}

#[test]
fn validation_metadata_is_admitted_before_copying_large_bounds() {
    let (store, _) = fixture();
    let captured = store.capture_read_dependencies(0, 0).unwrap();
    let request = range(
        false,
        Bound::Included(RelationalKey(vec![RelationalValue::Text(
            "x".repeat(MAX_METADATA_BYTES),
        )])),
        Bound::Unbounded,
    );
    assert!(matches!(
        store.validate_locked_snapshot(&captured, &[request]),
        Err(GraphReadDependencyError::CaptureRefused)
    ));
}
