// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::relational::{
    decode_relational_wal_batch, encode_relational_wal_batch_with_replay_access,
    RelationalDecodeLimits, RelationalReplayAccessSet, RelationalSparseRecoveryStage,
};

fn equals(column: &str, value: RelationalValue) -> RelationalPredicate {
    RelationalPredicate::Compare {
        column: column.into(),
        op: RelationalComparisonOp::Eq,
        value,
    }
}

fn update(predicate: RelationalPredicate) -> RelationalWrite {
    RelationalWrite::UpdateWhere {
        table: "nodes".into(),
        assignments: vec![RelationalUpdateAssignment {
            column: "body".into(),
            value: RelationalUpdateValue::Value(RelationalValue::Text("updated".into())),
        }],
        predicate,
    }
}

fn policy() -> RelationalSparseLiveHydrationOptions {
    let mut policy = options(false);
    policy.row_capture_limits.max_entries = NonZeroUsize::new(4).unwrap();
    policy
}

#[test]
fn primary_key_update_validates_non_key_unique_values_and_replay_closure() {
    let mut table = schema();
    table.foreign_keys.clear();
    table.unique_constraints = vec![vec!["body".into()]];
    let fixture = Fixture::with_table_schema(8, table);
    let policy = policy();
    let transaction = |values: &[(i64, &str)]| RelationalTransaction {
        writes: values
            .iter()
            .map(|(id, body)| RelationalWrite::UpdateWhere {
                table: "nodes".into(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "body".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text((*body).into())),
                }],
                predicate: equals("id", RelationalValue::BigInt(*id)),
            })
            .collect(),
    };
    let stage = |transaction| {
        fixture
            .materialized
            .stage_transaction_with_index_row_and_replay_access(
                transaction,
                policy.mutation_limits,
                policy.overflow_config,
                policy.index_capture_limits,
                policy.row_capture_limits,
            )
    };
    for writes in [vec![(3, "row-5-8")], vec![(3, "same"), (5, "same")]] {
        assert!(
            matches!(
                stage(transaction(&writes)),
                Err(RelationalError::Constraint(_))
            ),
            "point writes must enforce UNIQUE"
        );
        fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
    }
    let transaction = transaction(&[(3, "changed-unique")]);
    let (next, indexes, rows, access) = stage(transaction.clone()).unwrap();
    assert!(access.entries().len() <= policy.row_capture_limits.max_entries.get());
    let expected = fixture
        .materialized
        .stage_transaction(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
        )
        .unwrap();
    let (recovered, recovered_indexes, recovered_rows) = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction,
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &access,
        )
        .unwrap();
    assert_eq!(recovered_indexes, indexes);
    assert_eq!(recovered_rows, rows);
    assert_eq!(recovered.row_count("nodes"), next.row_count("nodes"));
    let original =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits);
    let successor = original.stage_advance(rows).unwrap();
    fixture.assert_rows(Arc::clone(successor.read_view()), &expected);
}

#[test]
fn resident_primary_key_mutations_fit_the_same_bounded_replay_budget() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let transaction = RelationalTransaction {
        writes: vec![
            update(equals("id", RelationalValue::BigInt(3))),
            RelationalWrite::DeleteWhere {
                table: "nodes".into(),
                predicate: equals("id", RelationalValue::BigInt(5)),
            },
            update(equals("id", RelationalValue::BigInt(7))),
        ],
    };
    let policy = policy();
    let (next, indexes, rows, access) = fixture
        .materialized
        .stage_transaction_with_index_row_and_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
        )
        .expect("resident point CRUD must not capture unrelated rows");
    assert_eq!(access.entries().len(), 3);
    let (recovered, recovered_indexes, recovered_rows) = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction,
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &access,
        )
        .expect("bounded resident WAL also recovers through authoritative indexes");
    assert_eq!(recovered_indexes, indexes);
    assert_eq!(recovered_rows, rows);
    assert_eq!(recovered.row_count("nodes"), next.row_count("nodes"));
    let original =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits);
    let successor = original.stage_advance(rows).unwrap();
    fixture.assert_rows(Arc::clone(original.read_view()), &fixture.materialized);
    fixture.assert_rows(Arc::clone(successor.read_view()), &next);
}

#[test]
fn resident_recovery_preserves_legacy_and_point_access_with_transient_and_rekeyed_rows() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let mut policy = options(false);
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::DeleteByPrimaryKey {
                table: "nodes".into(),
                keys: vec![RelationalKey(vec![RelationalValue::BigInt(1)])],
            },
            RelationalWrite::UpdateWhere {
                table: "nodes".into(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "id".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::BigInt(23)),
                }],
                predicate: equals("id", RelationalValue::BigInt(3)),
            },
            RelationalWrite::Insert {
                table: "nodes".into(),
                rows: vec![row(31, None, 0)],
                mode: RelationalInsertMode::Error,
            },
            update(equals("id", RelationalValue::BigInt(31))),
            RelationalWrite::DeleteWhere {
                table: "nodes".into(),
                predicate: equals("id", RelationalValue::BigInt(31)),
            },
            update(equals("id", RelationalValue::BigInt(90))),
        ],
    };
    let (_, legacy_indexes, legacy_rows, legacy_access) = fixture
        .materialized
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &MaterializedIndex(&fixture.materialized),
        )
        .unwrap();
    assert_eq!(legacy_access.entries().len(), 18);
    policy.row_capture_limits.max_entries = NonZeroUsize::new(4).unwrap();
    let (_, indexes, rows, access) = fixture
        .materialized
        .stage_transaction_with_index_row_and_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
        )
        .unwrap();
    assert_eq!(access.entries().len(), 4);
    assert_eq!(indexes, legacy_indexes);
    assert_eq!(rows, legacy_rows);
    for expected_access in [&access, &legacy_access] {
        let capture_limits = if expected_access == &legacy_access {
            options(false).row_capture_limits
        } else {
            policy.row_capture_limits
        };
        let (_, recovered_indexes, recovered_rows) = fixture
            .materialized
            .stage_transaction_for_authoritative_recovery_with_replay_access(
                transaction.clone(),
                policy.mutation_limits,
                policy.overflow_config,
                policy.index_capture_limits,
                capture_limits,
                expected_access,
            )
            .unwrap();
        assert_eq!(recovered_indexes, indexes);
        assert_eq!(recovered_rows, rows);
    }
    let mut extra = access.entries().to_vec();
    extra.push(crate::relational::RelationalReplayAccess {
        table: "nodes".into(),
        primary_key: RelationalKey(vec![RelationalValue::BigInt(9)]),
    });
    extra.sort();
    let extra = RelationalReplayAccessSet::from_decoded_entries(extra).unwrap();
    let error = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction,
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            options(false).row_capture_limits,
            &extra,
        )
        .unwrap_err();
    assert!(matches!(error, RelationalError::Corruption(_)), "{error:?}");
}

#[test]
fn resident_primary_key_residuals_match_the_legacy_scan_oracle() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let policy = options(false);
    for predicate in [
        RelationalPredicate::And(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(RelationalPredicate::IsNull {
                column: "parent".into(),
                negated: false,
            }),
        ),
        RelationalPredicate::And(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(equals("id", RelationalValue::BigInt(5))),
        ),
        equals("id", RelationalValue::BigInt(90)),
        equals("id", RelationalValue::Null),
        RelationalPredicate::Or(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(equals("id", RelationalValue::BigInt(5))),
        ),
        RelationalPredicate::Not(Box::new(equals("id", RelationalValue::BigInt(3)))),
    ] {
        for write in [
            update(predicate.clone()),
            RelationalWrite::DeleteWhere {
                table: "nodes".into(),
                predicate,
            },
        ] {
            let transaction = RelationalTransaction {
                writes: vec![write],
            };
            let (_, indexes, rows, _) = fixture
                .materialized
                .stage_transaction_with_index_row_and_replay_access(
                    transaction.clone(),
                    policy.mutation_limits,
                    policy.overflow_config,
                    policy.index_capture_limits,
                    policy.row_capture_limits,
                )
                .unwrap();
            let (_, legacy_indexes, legacy_rows, _) = fixture
                .materialized
                .stage_transaction_with_authoritative_replay_access(
                    transaction,
                    policy.mutation_limits,
                    policy.overflow_config,
                    policy.index_capture_limits,
                    policy.row_capture_limits,
                    &MaterializedIndex(&fixture.materialized),
                )
                .unwrap();
            assert_eq!(indexes, legacy_indexes);
            assert_eq!(rows, legacy_rows);
        }
    }
}

#[test]
fn primary_key_mutations_bound_hydration_and_replay_without_changing_old_views() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let transaction = RelationalTransaction {
        writes: vec![
            update(equals("id", RelationalValue::BigInt(3))),
            RelationalWrite::DeleteWhere {
                table: "nodes".into(),
                predicate: equals("id", RelationalValue::BigInt(5)),
            },
            insert(vec![row(17, None, 8)]),
        ],
    };
    let policy = policy();
    let index = MaterializedIndex(&fixture.materialized);
    let (expected, expected_indexes, expected_rows) = fixture
        .materialized
        .stage_transaction_with_index_and_row_changes(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
        )
        .unwrap();
    let (workspace, report, proven_absent) = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &transaction,
        &index,
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .expect("three point mutations fit the unchanged four-entry workspace budget");
    assert_eq!(workspace.len(), 3);
    assert_eq!(report.point_reads, 3);
    assert_eq!(report.range_reads, 0);
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
    assert_eq!(access.entries().len(), 3);
    let encoded =
        encode_relational_wal_batch_with_replay_access(11, &transaction, Some(&access)).unwrap();
    let decoded = decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).unwrap();
    let (recovered, recovery_indexes, recovery_rows) = fixture
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
    assert_eq!(recovery_indexes, indexes);
    assert_eq!(recovery_rows, rows);
    assert_eq!(recovered.row_count("nodes"), next.row_count("nodes"));
    let original =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits);
    let successor = original.stage_advance(rows).unwrap();
    fixture.assert_rows(Arc::clone(original.read_view()), &fixture.materialized);
    fixture.assert_rows(Arc::clone(successor.read_view()), &expected);
}

#[test]
fn primary_key_conjuncts_preserve_residual_null_and_absence_semantics() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let predicates = [
        RelationalPredicate::And(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(RelationalPredicate::IsNull {
                column: "parent".into(),
                negated: true,
            }),
        ),
        RelationalPredicate::And(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(RelationalPredicate::IsNull {
                column: "parent".into(),
                negated: false,
            }),
        ),
        RelationalPredicate::And(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(equals("id", RelationalValue::BigInt(5))),
        ),
        equals("id", RelationalValue::BigInt(90)),
    ];
    let policy = policy();
    let index = MaterializedIndex(&fixture.materialized);
    for predicate in predicates {
        let transaction = RelationalTransaction {
            writes: vec![update(predicate)],
        };
        let (expected, expected_indexes, expected_rows) = fixture
            .materialized
            .stage_transaction_with_index_and_row_changes(
                transaction.clone(),
                policy.mutation_limits,
                policy.overflow_config,
                policy.index_capture_limits,
                policy.row_capture_limits,
            )
            .unwrap();
        let (workspace, report, proven_absent) = hydrate_sparse_relational_workspace(
            &fixture.metadata,
            fixture.reader(Arc::clone(&fixture.view)),
            &transaction,
            &index,
            policy,
            Arc::new(RelationalMonotonicAppendMetrics::default()),
        )
        .unwrap();
        assert_eq!(report.point_reads, 1);
        assert_eq!(report.range_reads, 0);
        let constraint = RelationalProvenAbsenceConstraintIndex::new(&index, &proven_absent);
        let (_, indexes, rows, _, _) = fixture
            .metadata
            .stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
                RelationalSparseLiveStage {
                    transaction,
                    hydrated_workspace: workspace,
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
        let view =
            RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits)
                .stage_advance(rows)
                .unwrap();
        fixture.assert_rows(Arc::clone(view.read_view()), &expected);
    }
    fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
}

#[test]
fn scan_only_mutations_keep_their_original_resource_refusal() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let index = MaterializedIndex(&fixture.materialized);
    for predicate in [
        RelationalPredicate::Or(
            Box::new(equals("id", RelationalValue::BigInt(3))),
            Box::new(equals("id", RelationalValue::BigInt(5))),
        ),
        RelationalPredicate::Not(Box::new(equals("id", RelationalValue::BigInt(3)))),
        equals("body", RelationalValue::Text("unmatched".into())),
        equals("id", RelationalValue::Null),
    ] {
        let error = hydrate_sparse_relational_workspace(
            &fixture.metadata,
            fixture.reader(Arc::clone(&fixture.view)),
            &RelationalTransaction {
                writes: vec![update(predicate)],
            },
            &index,
            policy(),
            Arc::new(RelationalMonotonicAppendMetrics::default()),
        )
        .unwrap_err();
        assert!(matches!(error, RelationalError::Admission(_)), "{error:?}");
    }
    fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
}

#[test]
fn primary_key_updates_close_destination_reads_before_mutation() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let policy = policy();
    let index = MaterializedIndex(&fixture.materialized);
    for destination in [17, 5] {
        let transaction = RelationalTransaction {
            writes: vec![RelationalWrite::UpdateWhere {
                table: "nodes".into(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "id".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::BigInt(destination)),
                }],
                predicate: equals("id", RelationalValue::BigInt(3)),
            }],
        };
        let result = hydrate_sparse_relational_workspace(
            &fixture.metadata,
            fixture.reader(Arc::clone(&fixture.view)),
            &transaction,
            &index,
            policy,
            Arc::new(RelationalMonotonicAppendMetrics::default()),
        );
        if destination == 5 {
            assert!(
                matches!(result, Err(RelationalError::Constraint(_))),
                "{result:?}"
            );
            continue;
        }
        let (workspace, report, proven_absent) = result.unwrap();
        assert_eq!(report.point_reads, 2);
        assert_eq!(report.range_reads, 0);
        let constraint = RelationalProvenAbsenceConstraintIndex::new(&index, &proven_absent);
        let (_, _, rows, access, _) = fixture
            .metadata
            .stage_sparse_transaction_with_authoritative_replay_access_and_outcomes(
                RelationalSparseLiveStage {
                    transaction: transaction.clone(),
                    hydrated_workspace: workspace,
                    mutation_limits: policy.mutation_limits,
                    overflow_config: policy.overflow_config,
                    index_capture_limits: policy.index_capture_limits,
                    row_capture_limits: policy.row_capture_limits,
                    constraint_index: &constraint,
                },
            )
            .unwrap();
        assert_eq!(access.entries().len(), 2);
        let expected = fixture
            .materialized
            .stage_transaction(transaction, policy.mutation_limits, policy.overflow_config)
            .unwrap();
        let view =
            RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits)
                .stage_advance(rows)
                .unwrap();
        fixture.assert_rows(Arc::clone(view.read_view()), &expected);
    }
    fixture.assert_rows(Arc::clone(&fixture.view), &fixture.materialized);
}

#[test]
fn legacy_full_scan_replay_access_remains_valid_and_exact() {
    let fixture = Fixture::with_foreign_keys(8, false);
    let policy = options(false);
    let transaction = RelationalTransaction {
        writes: vec![update(equals("id", RelationalValue::BigInt(3)))],
    };
    let (expected, expected_indexes, expected_rows, access) = fixture
        .materialized
        .stage_transaction_with_authoritative_replay_access(
            transaction.clone(),
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &MaterializedIndex(&fixture.materialized),
        )
        .unwrap();
    assert_eq!(access.entries().len(), 16);
    let encoded =
        encode_relational_wal_batch_with_replay_access(11, &transaction, Some(&access)).unwrap();
    let decoded = decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).unwrap();
    let hydrated_access = access
        .entries()
        .iter()
        .map(|entry| crate::relational::RelationalSparseRecoveryRow {
            table: entry.table.clone(),
            primary_key: entry.primary_key.clone(),
            row: fixture
                .materialized
                .row(&entry.table, &entry.primary_key)
                .cloned(),
        })
        .collect();
    let (_, indexes, rows) = fixture
        .metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
            RelationalSparseRecoveryStage {
                transaction: decoded.transaction,
                hydrated_access,
                mutation_limits: policy.mutation_limits,
                overflow_config: policy.overflow_config,
                index_capture_limits: policy.index_capture_limits,
                row_capture_limits: policy.row_capture_limits,
                expected_replay_access: decoded.replay_access.as_ref().unwrap(),
            },
        )
        .unwrap();
    assert_eq!(indexes, expected_indexes);
    assert_eq!(rows, expected_rows);
    let view =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits)
            .stage_advance(rows)
            .unwrap();
    fixture.assert_rows(Arc::clone(view.read_view()), &expected);
    let missing =
        RelationalReplayAccessSet::from_decoded_entries(access.entries()[1..].to_vec()).unwrap();
    let error = fixture
        .materialized
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction,
            policy.mutation_limits,
            policy.overflow_config,
            policy.index_capture_limits,
            policy.row_capture_limits,
            &missing,
        )
        .unwrap_err();
    assert!(matches!(error, RelationalError::Corruption(_)), "{error:?}");
}

#[test]
fn composite_primary_keys_use_schema_order_for_points_and_prefix_ranges() {
    let mut table = schema();
    table.foreign_keys.clear();
    table.primary_key = vec!["id".into(), "body".into()];
    let fixture = Fixture::with_table_schema(8, table);
    let (original_key, original_row) = fixture
        .materialized
        .rows("nodes")
        .find(|(key, _)| key.0[0] == RelationalValue::BigInt(3))
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::UpdateWhere {
            table: "nodes".into(),
            assignments: vec![RelationalUpdateAssignment {
                column: "parent".into(),
                value: RelationalUpdateValue::Value(RelationalValue::BigInt(99)),
            }],
            predicate: RelationalPredicate::And(
                Box::new(equals("body", original_row.values()[2].clone())),
                Box::new(equals("id", RelationalValue::BigInt(3))),
            ),
        }],
    };
    let policy = policy();
    let index = MaterializedIndex(&fixture.materialized);
    let (workspace, report, _) = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &transaction,
        &index,
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .unwrap();
    assert_eq!(report.point_reads, 1);
    assert_eq!(report.range_reads, 0);
    assert_eq!(workspace.len(), 1);
    assert_eq!(&workspace[0].primary_key, original_key);
    let (_, _, rows, _, _) = fixture
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
        .unwrap();
    let view =
        RelationalTransactionRowView::new(Arc::clone(&fixture.view), policy.row_capture_limits)
            .stage_advance(rows)
            .unwrap();
    let (row, _) = fixture
        .reader(Arc::clone(view.read_view()))
        .point_projected(
            "nodes",
            original_key,
            &[0, 1, 2],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut RelationalHydrationBudget::default(),
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    let row = complete_sparse_projected_row("nodes", &[0, 1, 2], row.unwrap()).unwrap();
    assert_eq!(row.values()[1], RelationalValue::BigInt(99));
    assert_eq!(original_row.values()[1], RelationalValue::BigInt(2));
    let (workspace, report, _) = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &RelationalTransaction {
            writes: vec![update(equals("id", RelationalValue::BigInt(3)))],
        },
        &index,
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .unwrap();
    assert_eq!(report.range_reads, 1);
    assert_eq!(report.rows_decoded, 1);
    assert_eq!(workspace.len(), 2);
    let error = hydrate_sparse_relational_workspace(
        &fixture.metadata,
        fixture.reader(Arc::clone(&fixture.view)),
        &RelationalTransaction {
            writes: vec![update(equals("body", original_row.values()[2].clone()))],
        },
        &index,
        policy,
        Arc::new(RelationalMonotonicAppendMetrics::default()),
    )
    .unwrap_err();
    assert!(matches!(error, RelationalError::Admission(_)), "{error:?}");
}
