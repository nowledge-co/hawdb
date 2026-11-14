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

fn create() -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
            name: "bag".into(),
            columns: vec![text_column("value", true)],
            primary_key: vec![],
            unique_constraints: vec![],
            foreign_keys: vec![],
            indexes: vec![],
            materialized_view: None,
        })],
    }
}

fn insert(count: usize) -> RelationalTransaction {
    RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "bag".into(),
            rows: vec![RelationalRow::new(vec![RelationalValue::Text("same".into())]); count],
            mode: RelationalInsertMode::Error,
        }],
    }
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

#[test]
fn hidden_handles_survive_updates_deletes_snapshots_and_checkpoint() {
    let store = RelationalStore::default();
    store.commit(create(), |_, _| Ok(())).unwrap();
    let before = store.commit(insert(3), |_, _| Ok(())).unwrap();
    assert_eq!(before.value().last_row_id("bag"), 3);
    assert_eq!(
        before
            .value()
            .rows("bag")
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![key(1), key(2), key(3)]
    );
    let updated = store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::UpdateWhere {
                    table: "bag".into(),
                    assignments: vec![RelationalUpdateAssignment {
                        column: "value".into(),
                        value: RelationalUpdateValue::Value(RelationalValue::Text(
                            "changed".into(),
                        )),
                    }],
                    predicate: RelationalPredicate::Compare {
                        column: "value".into(),
                        op: RelationalComparisonOp::Eq,
                        value: RelationalValue::Text("same".into()),
                    },
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    assert_eq!(updated.value().last_row_id("bag"), 3);
    assert_eq!(
        updated
            .value()
            .rows("bag")
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>(),
        vec![key(1), key(2), key(3)]
    );
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "bag".into(),
                    keys: vec![key(2)],
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let with_hole = decode_relational_checkpoint(
        &store.encode_checkpoint().unwrap(),
        RelationalDecodeLimits::checkpoint(),
    )
    .unwrap();
    assert_eq!(
        with_hole
            .state
            .rows("bag")
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>(),
        vec![key(1), key(3)]
    );
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "bag".into(),
                    keys: vec![key(1), key(2), key(3)],
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    assert_eq!(before.value().row_count("bag"), 3);
    let bytes = store.encode_checkpoint().unwrap();
    let recovered = RelationalStore::from_checkpoint(
        &bytes,
        RelationalDecodeLimits::checkpoint(),
        RelationalMutationLimits::default(),
        RelationalOverflowConfig::default(),
    )
    .unwrap();
    let after = recovered.commit(insert(1), |_, _| Ok(())).unwrap();
    assert_eq!(after.value().last_row_id("bag"), 4);
    assert!(after.value().row("bag", &key(4)).is_some());
    assert!(after
        .value()
        .table_schema("bag")
        .unwrap()
        .required_index_definitions()
        .is_empty());
}

#[test]
fn hidden_handles_replay_wal_and_publish_only_after_durability() {
    let store = RelationalStore::default();
    let mut records = vec![];
    for transaction in [create(), insert(2)] {
        store
            .commit_with_wal(transaction, |_, bytes| {
                records.push(bytes.to_vec());
                Ok(())
            })
            .unwrap();
    }
    assert!(store
        .commit(insert(1), |_, _| Err(RelationalError::Durability(
            "injected failure".into()
        )))
        .is_err());
    assert_eq!(store.snapshot().unwrap().value().last_row_id("bag"), 2);
    let torn = RelationalStore::default();
    torn.replay_wal_record(&records[0], RelationalDecodeLimits::wal())
        .unwrap();
    for length in 0..records[1].len() {
        assert!(torn
            .replay_wal_record(&records[1][..length], RelationalDecodeLimits::wal())
            .is_err());
        assert_eq!(torn.snapshot().unwrap().value().last_row_id("bag"), 0);
        assert_eq!(torn.snapshot().unwrap().value().row_count("bag"), 0);
    }
    let replayed = RelationalStore::default();
    for record in records {
        replayed
            .replay_wal_record(&record, RelationalDecodeLimits::wal())
            .unwrap();
    }
    assert_eq!(replayed.snapshot().unwrap().value().last_row_id("bag"), 2);
    assert!(replayed
        .commit(insert(1), |_, _| Ok(()))
        .unwrap()
        .value()
        .row("bag", &key(3))
        .is_some());
}

#[test]
fn concurrent_hidden_handle_allocation_is_unique() {
    let store = Arc::new(RelationalStore::default());
    store.commit(create(), |_, _| Ok(())).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for _ in 0..16 {
                    store.commit(insert(2), |_, _| Ok(())).unwrap();
                }
            });
        }
    });
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.value().row_count("bag"), 128);
    assert_eq!(snapshot.value().last_row_id("bag"), 128);
}

#[test]
fn hidden_handle_exhaustion_and_invalid_checkpoint_fail_closed() {
    let source = RelationalState::default()
        .stage_transaction(
            create(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let mut exhausted = source.clone();
    Arc::make_mut(exhausted.segments.get_mut("bag").unwrap()).last_row_id = i64::MAX;
    assert!(exhausted
        .stage_transaction(
            insert(1),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default()
        )
        .is_err());
    assert_eq!(exhausted.row_count("bag"), 0);
    let mut corrupt = source
        .stage_transaction(
            insert(1),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    Arc::make_mut(corrupt.segments.get_mut("bag").unwrap()).last_row_id = 0;
    assert!(encode_relational_checkpoint(1, &corrupt).is_err());
}

#[test]
fn sparse_hidden_handle_upserts_use_unique_probes_without_table_scans() {
    let mut transaction = create();
    let RelationalWrite::CreateTable(schema) = &mut transaction.writes[0] else {
        unreachable!()
    };
    schema.unique_constraints.push(vec!["value".into()]);
    let state = RelationalState::default()
        .stage_transaction(
            transaction,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let plan = state
        .plan_sparse_transaction_hydration(&RelationalTransaction {
            writes: vec![
                RelationalWrite::Upsert {
                    table: "bag".into(),
                    rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                        "same".into(),
                    )])],
                    conflict_columns: vec!["value".into()],
                    action: RelationalConflictAction::DoNothing,
                },
                RelationalWrite::Insert {
                    table: "bag".into(),
                    rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                        "next".into(),
                    )])],
                    mode: RelationalInsertMode::Error,
                },
            ],
        })
        .unwrap();
    assert!(plan.scan_tables.is_empty());
    assert_eq!(plan.index_probes.len(), 1);
    assert_eq!(
        plan.point_access
            .iter()
            .map(|access| access.primary_key.clone())
            .collect::<Vec<_>>(),
        vec![key(1), key(2)]
    );
}
