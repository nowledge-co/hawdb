// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::relational::{
    decode_relational_wal_batch, encode_relational_wal_batch_with_replay_access,
    RelationalDecodeLimits, RelationalReplayAccessSet, RelationalSparseRecoveryStage,
};

const TABLE: &str = "order_line";

fn compare(column: &str, op: RelationalComparisonOp, value: i64) -> RelationalPredicate {
    RelationalPredicate::Compare {
        column: column.into(),
        op,
        value: RelationalValue::BigInt(value),
    }
}

fn and(left: RelationalPredicate, right: RelationalPredicate) -> RelationalPredicate {
    RelationalPredicate::And(Box::new(left), Box::new(right))
}

fn prefix(order: i64) -> RelationalPredicate {
    and(
        and(
            compare("w", RelationalComparisonOp::Eq, 1),
            compare("d", RelationalComparisonOp::Eq, 1),
        ),
        compare("o", RelationalComparisonOp::Eq, order),
    )
}

fn line_key(order: i64, number: i64) -> RelationalKey {
    RelationalKey(
        vec![1, 1, order, number]
            .into_iter()
            .map(RelationalValue::BigInt)
            .collect(),
    )
}

fn line(order: i64, number: i64) -> RelationalRow {
    RelationalRow::new(
        vec![1, 1, order, number, 0]
            .into_iter()
            .map(RelationalValue::BigInt)
            .collect(),
    )
}

fn fixture(orders: i64) -> Fixture {
    let table = RelationalTableSchema {
        name: TABLE.into(),
        columns: ["w", "d", "o", "n", "delivered"]
            .into_iter()
            .map(|name| RelationalColumnSchema {
                name: name.into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            })
            .collect(),
        primary_key: ["w", "d", "o", "n"].into_iter().map(String::from).collect(),
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
        materialized_view: None,
    };
    let materialized = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(table),
                    RelationalWrite::Insert {
                        table: TABLE.into(),
                        rows: (1..=orders)
                            .flat_map(|order| (1..=5).map(move |number| line(order, number)))
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    Fixture::from_materialized(materialized)
}

fn update(predicate: RelationalPredicate) -> RelationalWrite {
    RelationalWrite::UpdateWhere {
        table: TABLE.into(),
        predicate,
        assignments: vec![RelationalUpdateAssignment {
            column: "delivered".into(),
            value: RelationalUpdateValue::Value(RelationalValue::BigInt(1)),
        }],
    }
}

fn verify(
    fixture: &Fixture,
    transaction: RelationalTransaction,
    max_entries: usize,
) -> (RelationalSparseLiveHydrationReport, usize) {
    let mut policy = options(false);
    let index = MaterializedIndex(&fixture.materialized);
    // Authoritative execution over the complete resident table is the legacy
    // full-scan oracle, independent of the new candidate selection.
    let (expected, expected_indexes, expected_rows, legacy_access) = fixture
        .materialized
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &index,
        )
        .unwrap();
    let (_, legacy_indexes, legacy_rows) = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &legacy_access,
        )
        .expect("legacy full-table access records remain recoverable");
    assert_eq!(legacy_indexes, expected_indexes);
    assert_eq!(legacy_rows, expected_rows);
    policy.row_capture_limits.max_entries = NonZeroUsize::new(max_entries).unwrap();
    let (_, resident_indexes, resident_rows, resident_access) = fixture
        .materialized
        .stage_transaction_with_index_row_and_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
        )
        .expect("resident candidate selection fits the fixed access budget");
    assert_eq!(resident_indexes, expected_indexes);
    assert_eq!(resident_rows, expected_rows);
    let (_, recovered_indexes, recovered_rows) = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &resident_access,
        )
        .unwrap();
    assert_eq!(recovered_indexes, expected_indexes);
    assert_eq!(recovered_rows, expected_rows);
    let mut surplus = resident_access.entries().to_vec();
    surplus.push(RelationalReplayAccess {
        table: TABLE.into(),
        primary_key: line_key(-1, 1),
    });
    surplus.sort();
    let surplus = RelationalReplayAccessSet::from_decoded_entries(surplus).unwrap();
    let error = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &surplus,
        )
        .unwrap_err();
    assert!(matches!(error, RelationalError::Corruption(_)), "{error:?}");
    let (workspace, report, proven_absent) = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &transaction,
        &index,
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .expect("canonical candidate hydration fits the same fixed workspace budget");
    let workspace_len = workspace.len();
    let constraint = RelationalProvenAbsenceConstraintIndex::new(&index, &proven_absent);
    let (next, indexes, rows, access, _) = fixture
        .metadata
        .stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
            RelationalSparseLiveStage {
                transaction: transaction.clone(),
                hydrated_workspace: workspace.clone(),
                mutation_limits: policy.mutation_limits,
                overflow_config: policy.overflow_config,
                index_capture_limits: policy.index_capture_limits,
                row_capture_limits: policy.row_capture_limits,
                constraint_index: &constraint,
            },
        )
        .unwrap();
    assert_eq!(indexes, expected_indexes);
    assert_eq!(rows, expected_rows);
    assert_eq!(access, resident_access);
    assert_eq!(next.row_count(TABLE), expected.row_count(TABLE));
    let encoded =
        encode_relational_wal_batch_with_replay_access(11, &transaction, Some(&access)).unwrap();
    let decoded = decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).unwrap();
    let (recovered, recovered_indexes, recovered_rows) = fixture
        .metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
            RelationalSparseRecoveryStage {
                transaction: decoded.transaction,
                hydrated_access: workspace,
                mutation_limits: policy.mutation_limits,
                overflow_config: policy.overflow_config,
                index_capture_limits: policy.index_capture_limits,
                row_capture_limits: policy.row_capture_limits,
                expected_replay_access: decoded.replay_access.as_ref().unwrap(),
            },
        )
        .unwrap();
    assert_eq!(recovered_indexes, expected_indexes);
    assert_eq!(recovered_rows, expected_rows);
    assert_eq!(recovered.row_count(TABLE), expected.row_count(TABLE));
    let old =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits);
    let new = old.stage_advance(rows).unwrap();
    for order in [1, 2, 3] {
        for number in 1..=5 {
            let key = line_key(order, number);
            for (view, state) in [
                (old.read_view(), &fixture.materialized),
                (new.read_view(), &expected),
            ] {
                let (row, _) = fixture
                    .reader(Arc::clone(view))
                    .point_projected(
                        TABLE,
                        &key,
                        &[0, 1, 2, 3, 4],
                        RelationalRowPageSnapshotReadLimits::default(),
                        &mut RelationalHydrationBudget::default(),
                        &RuntimeTaskContext::default(),
                    )
                    .unwrap();
                let row = row.map(|row| {
                    complete_sparse_projected_row(TABLE, &[0, 1, 2, 3, 4], row).unwrap()
                });
                assert_eq!(row.as_ref(), state.row(TABLE, &key));
            }
        }
    }
    (report, workspace_len)
}

#[test]
fn composite_mutations_keep_candidate_work_constant_as_unrelated_orders_grow() {
    for orders in [3, 1000] {
        let fixture = fixture(orders);
        let predicate = and(prefix(2), compare("n", RelationalComparisonOp::NotEq, 3));
        for write in [
            update(predicate.clone()),
            RelationalWrite::DeleteWhere {
                table: TABLE.into(),
                predicate,
            },
        ] {
            let (report, workspace_len) = verify(
                &fixture,
                RelationalTransaction {
                    writes: vec![write],
                },
                8,
            );
            assert_eq!(workspace_len, 5);
            assert_eq!(report.rows_decoded, 5);
            assert_eq!(report.range_reads, 1);
            assert_eq!(report.point_reads, 0);
            assert!(report.pages_read <= 2, "{report:?}");
        }
    }
}

#[test]
fn composite_mutation_bounds_and_residuals_match_full_scan() {
    let fixture = fixture(20);
    for (lower, upper) in [
        (RelationalComparisonOp::Gt, RelationalComparisonOp::Lt),
        (RelationalComparisonOp::Gte, RelationalComparisonOp::Lte),
        (RelationalComparisonOp::Gte, RelationalComparisonOp::Lt),
        (RelationalComparisonOp::Gt, RelationalComparisonOp::Lte),
    ] {
        let predicate = and(
            prefix(2),
            and(compare("n", lower, 2), compare("n", upper, 4)),
        );
        verify(
            &fixture,
            RelationalTransaction {
                writes: vec![update(predicate)],
            },
            8,
        );
    }
    for predicate in [
        and(
            prefix(2),
            compare("n", RelationalComparisonOp::Gt, i64::MAX),
        ),
        and(
            prefix(2),
            compare("n", RelationalComparisonOp::Lt, i64::MIN),
        ),
        and(
            prefix(2),
            and(
                compare("n", RelationalComparisonOp::Gt, 4),
                compare("n", RelationalComparisonOp::Lte, 4),
            ),
        ),
        and(prefix(2), compare("o", RelationalComparisonOp::Eq, 3)),
        and(
            prefix(2),
            RelationalPredicate::Compare {
                column: "delivered".into(),
                op: RelationalComparisonOp::Eq,
                value: RelationalValue::Null,
            },
        ),
        and(
            prefix(2),
            RelationalPredicate::Or(
                Box::new(compare("n", RelationalComparisonOp::Eq, 1)),
                Box::new(compare("n", RelationalComparisonOp::Eq, 5)),
            ),
        ),
    ] {
        verify(
            &fixture,
            RelationalTransaction {
                writes: vec![update(predicate)],
            },
            8,
        );
    }
    let district = and(
        compare("w", RelationalComparisonOp::Eq, 1),
        compare("d", RelationalComparisonOp::Eq, 1),
    );
    verify(
        &fixture,
        RelationalTransaction {
            writes: vec![update(and(
                district,
                and(
                    compare("o", RelationalComparisonOp::Gte, 2),
                    compare("o", RelationalComparisonOp::Lt, 3),
                ),
            ))],
        },
        8,
    );
}

#[test]
fn composite_mutations_replay_transient_rows_and_primary_key_changes() {
    let fixture = fixture(20);
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::DeleteByPrimaryKey {
                table: TABLE.into(),
                keys: vec![line_key(2, 1)],
            },
            RelationalWrite::UpdateWhere {
                table: TABLE.into(),
                predicate: and(prefix(2), compare("n", RelationalComparisonOp::Eq, 2)),
                assignments: vec![RelationalUpdateAssignment {
                    column: "o".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::BigInt(1001)),
                }],
            },
            RelationalWrite::Insert {
                table: TABLE.into(),
                rows: vec![line(2, 9)],
                mode: RelationalInsertMode::Error,
            },
            update(prefix(2)),
            RelationalWrite::DeleteWhere {
                table: TABLE.into(),
                predicate: and(prefix(2), compare("n", RelationalComparisonOp::Gte, 4)),
            },
        ],
    };
    verify(&fixture, transaction, 32);
}

#[test]
fn composite_mutation_budget_refusal_publishes_no_partial_changes() {
    let fixture = fixture(20);
    let transaction = RelationalTransaction {
        writes: vec![update(prefix(2))],
    };
    let mut policy = options(false);
    policy.row_capture_limits.max_entries = NonZeroUsize::new(4).unwrap();
    let error = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &transaction,
        &MaterializedIndex(&fixture.materialized),
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .unwrap_err();
    assert!(matches!(error, RelationalError::Admission(_)), "{error:?}");
    verify(&fixture, transaction, 8);
}

#[test]
fn composite_mutation_rekey_collisions_reject_the_complete_batch() {
    let fixture = fixture(20);
    let index = MaterializedIndex(&fixture.materialized);
    let policy = options(false);
    for (column, value) in [("o", 3), ("n", 1)] {
        let transaction = RelationalTransaction {
            writes: vec![
                update(prefix(1)),
                RelationalWrite::UpdateWhere {
                    table: TABLE.into(),
                    predicate: prefix(2),
                    assignments: vec![RelationalUpdateAssignment {
                        column: column.into(),
                        value: RelationalUpdateValue::Value(RelationalValue::BigInt(value)),
                    }],
                },
            ],
        };
        let error = fixture
            .materialized
            .stage_transaction(
                transaction.clone(),
                policy.mutation_limits,
                policy.overflow_config,
            )
            .unwrap_err();
        assert!(matches!(error, RelationalError::Constraint(_)), "{error:?}");
        let error = hydrate_sparse_relational_workspace(
            &fixture.metadata,
            fixture.reader(Arc::clone(&fixture.view)),
            &transaction,
            &index,
            policy,
            Arc::new(RelationalMonotonicAppendMetrics::default()),
        )
        .and_then(|(workspace, _, _)| {
            fixture
                .metadata
                .stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
                    RelationalSparseLiveStage {
                        transaction,
                        hydrated_workspace: workspace,
                        mutation_limits: policy.mutation_limits,
                        overflow_config: policy.overflow_config,
                        index_capture_limits: policy.index_capture_limits,
                        row_capture_limits: policy.row_capture_limits,
                        constraint_index: &index,
                    },
                )
        })
        .unwrap_err();
        assert!(matches!(error, RelationalError::Constraint(_)), "{error:?}");
    }
    verify(
        &fixture,
        RelationalTransaction {
            writes: vec![update(prefix(2))],
        },
        8,
    );
}
