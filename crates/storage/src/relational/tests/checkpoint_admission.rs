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

#[test]
fn controlled_materialized_view_recovery_matches_wal_and_releases_admission() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };

    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let mut previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "old"),
                        upsert_row("id-2", "owner-2", "old"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    previous = previous
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateIndex {
                    table: "documents".into(),
                    index: RelationalIndexSchema {
                        name: "by_payload".into(),
                        columns: vec!["payload".into()],
                        unique: false,
                    },
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let view = MaterializedViewDefinition {
        source: "documents".into(),
        columns: vec![
            MaterializedViewColumn {
                name: "payload".into(),
                expression: MaterializedViewExpression::Column("payload".into()),
            },
            MaterializedViewColumn {
                name: "n".into(),
                expression: MaterializedViewExpression::Count(None),
            },
            MaterializedViewColumn {
                name: "first_owner".into(),
                expression: MaterializedViewExpression::Min("owner".into()),
            },
            MaterializedViewColumn {
                name: "last_owner".into(),
                expression: MaterializedViewExpression::Max("owner".into()),
            },
        ],
        group_by: vec!["payload".into()],
        filter: Some(RelationalPredicate::Not(Box::new(
            RelationalPredicate::IsNull {
                column: "owner".into(),
                negated: false,
            },
        ))),
        search: None,
    }
    .table_schema("stats".into(), previous.table_schema("documents").unwrap())
    .unwrap();
    for write in [
        RelationalWrite::CreateTable(view),
        RelationalWrite::CreateIndex {
            table: "stats".into(),
            index: RelationalIndexSchema {
                name: "stats_payload".into(),
                columns: vec!["payload".into()],
                unique: false,
            },
        },
        RelationalWrite::Insert {
            table: "documents".into(),
            rows: vec![upsert_row("id-1", "owner-1", "new")],
            mode: RelationalInsertMode::Replace,
        },
        RelationalWrite::DeleteByPrimaryKey {
            table: "documents".into(),
            keys: vec![RelationalKey(vec![RelationalValue::Text("id-2".into())])],
        },
        RelationalWrite::DropMaterializedView {
            table: "stats".into(),
        },
    ] {
        let transaction = RelationalTransaction {
            writes: vec![write],
        };
        let expected = apply_transaction_inner(
            &previous,
            transaction.clone(),
            limits,
            overflow,
            TransactionApplyOptions {
                index_capture_limits: Some(RelationalIndexChangeCaptureLimits::default()),
                row_capture_limits: Some(RelationalRowChangeCaptureLimits::default()),
                ..TransactionApplyOptions::materialized()
            },
        )
        .unwrap();
        let encoded = encode_relational_wal_batch(1, &transaction).unwrap();
        for (ceiling, succeeds) in [(1, false), (16 * 1024 * 1024, true)] {
            let governor = RuntimeGovernor::detect(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(ceiling),
                    ..RuntimeGovernorConfig::shared_host()
                },
                IoConcurrencyBudget::new(2, 1),
            );
            governor.pin_resources();
            let permit = governor
                .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
                .unwrap();
            let work =
                CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
            let context = CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            };
            let result = previous.stage_materialized_recovery_with_allocations(
                RelationalMaterializedRecoveryStage {
                    transaction: transaction.clone(),
                    mutation_limits: limits,
                    overflow_config: overflow,
                    index_capture_limits: Some(RelationalIndexChangeCaptureLimits::default()),
                    row_capture_limits: Some(RelationalRowChangeCaptureLimits::default()),
                },
                &context,
            );
            if succeeds {
                let (actual, indexes, rows) = result.unwrap();
                assert_eq!(indexes, expected.index_capture);
                assert_eq!(rows, expected.row_capture);
                for table in ["documents", "stats"] {
                    assert_eq!(
                        actual.table_schema(table),
                        expected.state.table_schema(table)
                    );
                    assert_eq!(
                        actual.rows(table).collect::<Vec<_>>(),
                        expected.state.rows(table).collect::<Vec<_>>()
                    );
                }
                let owned = decode_relational_wal_batch_owned(
                    &encoded,
                    RelationalDecodeLimits::wal(),
                    &context,
                )
                .unwrap();
                assert_eq!(
                    owned,
                    decode_relational_wal_batch(&encoded, RelationalDecodeLimits::wal()).unwrap()
                );
                if let Some(schema) = actual.schemas.get("stats") {
                    let copy = schema
                        .checkpoint_edit(&context.work, |_, _| Ok(()))
                        .unwrap();
                    assert_eq!(copy.schema, schema.schema);
                }
                drop(actual);
            } else {
                assert!(matches!(result, Err(RelationalError::Admission(_))));
            }
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            drop(work);
            drop(permit);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
        previous = expected.state;
    }
}

#[test]
fn owned_relational_wal_decode_matches_shared_codec_and_rejects_corruption() {
    use crate::background::CheckpointDecodeContext;
    for length in [0, 1, 31, 65_535, 65_536, 65_537] {
        let transaction = RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "messages".into(),
                rows: vec![RelationalRow::new(vec![
                    RelationalValue::Text("id-\0-中文".into()),
                    RelationalValue::Text("x".repeat(length)),
                    RelationalValue::Bytea(vec![0xff; length]),
                ])],
                mode: RelationalInsertMode::Error,
            }],
        };
        let bytes = encode_relational_wal_batch(17, &transaction).unwrap();
        let allocations = CheckpointDecodeContext {
            work: Default::default(),
            memory: Default::default(),
        };
        let ordinary = decode_relational_wal_batch(&bytes, RelationalDecodeLimits::wal()).unwrap();
        let owned =
            decode_relational_wal_batch_owned(&bytes, RelationalDecodeLimits::wal(), &allocations)
                .unwrap();
        assert_eq!(owned, ordinary);
        drop(owned);
        for offset in [0, 8, 28, bytes.len() - 1] {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 0xff;
            assert!(decode_relational_wal_batch(&corrupt, RelationalDecodeLimits::wal()).is_err());
            assert!(decode_relational_wal_batch_owned(
                &corrupt,
                RelationalDecodeLimits::wal(),
                &allocations,
            )
            .is_err());
        }
    }
}

#[test]
fn owned_relational_wal_decode_denies_large_field_before_copy_and_releases_memory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "messages".into(),
            rows: vec![RelationalRow::new(vec![RelationalValue::Text(
                "x".repeat(128 * 1024),
            )])],
            mode: RelationalInsertMode::Error,
        }],
    };
    let bytes = encode_relational_wal_batch(1, &transaction).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 * 1024))
        .unwrap();
    let allocations = CheckpointDecodeContext {
        work: CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default())),
        memory: Default::default(),
    };
    let observer = crate::test_allocator::AllocationObservation::start();
    let result =
        decode_relational_wal_batch_owned(&bytes, RelationalDecodeLimits::wal(), &allocations);
    let large_allocations = observer.finish();
    let error = result.unwrap_err();
    assert!(matches!(error, RelationalError::Admission(_)), "{error}");
    assert_eq!(large_allocations, 0, "deny before copying the large field");
    drop(allocations);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn owned_relational_wal_decode_preserves_all_write_variants_and_access_captures() {
    use crate::background::CheckpointDecodeContext;
    let key = RelationalKey(vec![RelationalValue::Text("id-1".into())]);
    let row = RelationalRow::new(vec![RelationalValue::Text("id-1".into())]);
    let predicate = RelationalPredicate::Not(Box::new(RelationalPredicate::IsNull {
        column: "id".into(),
        negated: false,
    }));
    let mut transaction = create_payload_table();
    transaction.writes.extend([
        RelationalWrite::CreateIndex {
            table: "messages".into(),
            index: RelationalIndexSchema {
                name: "by_id".into(),
                columns: vec!["id".into()],
                unique: false,
            },
        },
        RelationalWrite::Insert {
            table: "messages".into(),
            rows: vec![row.clone()],
            mode: RelationalInsertMode::Error,
        },
        RelationalWrite::DeleteByPrimaryKey {
            table: "messages".into(),
            keys: vec![key.clone()],
        },
        RelationalWrite::Upsert {
            table: "messages".into(),
            rows: vec![row],
            conflict_columns: vec!["id".into()],
            action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                column: "id".into(),
                value: RelationalUpsertValue::ExcludedColumn("id".into()),
            }]),
        },
        RelationalWrite::DeleteWhere {
            table: "messages".into(),
            predicate: predicate.clone(),
        },
        RelationalWrite::UpdateWhere {
            table: "messages".into(),
            assignments: vec![RelationalUpdateAssignment {
                column: "id".into(),
                value: RelationalUpdateValue::Value(RelationalValue::Text("id-2".into())),
            }],
            predicate,
        },
        RelationalWrite::AddColumn {
            table: "messages".into(),
            column: RelationalColumnSchema {
                name: "extra".into(),
                scalar_type: RelationalScalarType::Text,
                nullable: true,
                default: None,
            },
        },
    ]);
    let access = RelationalReplayAccessSet::from_decoded_entries(vec![RelationalReplayAccess {
        table: "messages".into(),
        primary_key: key.clone(),
    }])
    .unwrap();
    let capture = RelationalPrimaryKeyChangeCapture::Captured {
        tables: vec![RelationalTablePrimaryKeyChanges {
            table: "messages".into(),
            primary_keys: vec![key],
        }],
        encoded_bytes: 0,
    };
    let encoded =
        encode_relational_wal_batch_with_captures(9, &transaction, Some(&access), &capture)
            .unwrap();
    let allocations = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let ordinary =
        decode_relational_wal_batch(&encoded.record, RelationalDecodeLimits::wal()).unwrap();
    let owned = decode_relational_wal_batch_owned(
        &encoded.record,
        RelationalDecodeLimits::wal(),
        &allocations,
    )
    .unwrap();
    assert_eq!(owned, ordinary);
    assert_eq!(owned.transaction, transaction);
    assert_eq!(owned.replay_access.as_ref(), Some(&access));
}

#[test]
fn planning_preserves_resident_detached_and_file_backed_overflow_terms() {
    let source = RelationalStore::with_overflow_config(
        RelationalMutationLimits::default(),
        RelationalOverflowConfig {
            threshold_bytes: 1,
            ..RelationalOverflowConfig::default()
        },
    );
    source
        .commit(create_payload_table(), |_, _| Ok(()))
        .unwrap();
    source
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "messages".into(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("message-1".into()),
                        RelationalValue::Text("payload".repeat(64)),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let checkpoint = source.encode_checkpoint().unwrap();
    let path = std::env::temp_dir().join(format!(
        "hawdb-admission-size-{}.hawdb",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::write(&path, &checkpoint).unwrap();
    let snapshot = source.snapshot().unwrap();
    let decoded =
        decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint()).unwrap();
    assert_eq!(decoded.state.file_backed_overflow_segment_count(), 1);
    let resident = (std::mem::size_of::<RelationalKey>()
        + std::mem::size_of::<RelationalRow>()
        + std::mem::size_of::<Vec<RelationalValue>>()
        + 2 * "message-1".len()
        + std::mem::size_of::<RelationalOverflowRef>()) as u64;
    for original in [snapshot.value(), &decoded.state] {
        let overflow_bytes = original
            .overflow_segments
            .values()
            .next()
            .unwrap()
            .read()
            .unwrap()
            .as_ref()
            .len() as u64;
        assert_eq!(original.estimated_materialized_row_bytes(), resident);
        assert_eq!(
            original.estimated_checkpoint_bytes(),
            resident + overflow_bytes
        );
        for detached in [false, true] {
            let mut state = original.clone();
            if detached {
                state.omit_materialized_rows();
            }
            let expected = (
                if detached { 0 } else { resident },
                resident + overflow_bytes,
            );
            let mut visits = 0;
            assert_eq!(
                state
                    .estimated_checkpoint_admission_bytes_with_visit(&mut || {
                        visits += 1;
                        Ok::<_, usize>(())
                    })
                    .unwrap(),
                expected
            );
            assert!(visits > 0);
            for denied in 1..=visits {
                let mut reached = 0;
                assert_eq!(
                    state.estimated_checkpoint_admission_bytes_with_visit(&mut || {
                        reached += 1;
                        if reached == denied {
                            Err(denied)
                        } else {
                            Ok(())
                        }
                    }),
                    Err(denied)
                );
                assert_eq!(reached, denied);
            }
            assert_eq!(state.estimated_materialized_row_bytes(), expected.0);
            assert_eq!(state.estimated_checkpoint_bytes(), expected.1);
        }
    }
    drop(decoded);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn controlled_metadata_clone_denies_wide_names_and_keeps_shared_count_ownership() {
    use crate::background::CheckpointWorkContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let name = "t".repeat(128 * 1024);
    let mut transaction = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut transaction.writes[0] else {
        panic!("fixture schema");
    };
    schema.name = name.clone();
    let mut source = RelationalState::default()
        .stage_transaction(
            transaction,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    source.omit_materialized_rows();
    assert!(!source.materialized_rows_resident());
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let observer = crate::test_allocator::AllocationObservation::start();
        let cloned = source.clone_metadata_with_work_context(&work);
        let wide_allocations = observer.finish();
        if succeeds {
            let mut cloned = cloned.unwrap();
            for _ in 0..32 {
                cloned = cloned.clone_metadata_with_work_context(&work).unwrap();
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling),
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes > ceiling - 448 * 1024)
                );
            }
            assert_eq!(cloned.table_schema(&name), source.table_schema(&name));
            assert_eq!(cloned.row_count(&name), 0);
            assert!(!cloned
                .detached_row_counts
                .shares_storage_with(&source.detached_row_counts));
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 3 * 128 * 1024)
            );
            let retained = cloned.detached_row_counts.clone();
            drop(cloned);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 128 * 1024 && available_bytes > ceiling - 160 * 1024)
            );
            assert_eq!(retained.get(&name), Some(&0));
            drop(retained);
        } else {
            assert!(matches!(cloned, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        assert!(source.table_schema(&name).is_some());
        assert_eq!(source.row_count(&name), 0);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_metadata_clone_cancellation_keeps_source_and_refunds_inventory() {
    use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
    use hawdb_qos::{
        IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor,
        RuntimeGovernorConfig, RuntimeTaskContext, RuntimeWorkRequest,
    };
    let mut source = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    source.omit_materialized_rows();
    let ceiling = 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(2, std::sync::atomic::Ordering::SeqCst);
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
        probe.cancellation.clone(),
    ));
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let result = source.clone_metadata_with_work_context(&work);
    assert!(matches!(result, Err(RelationalError::Admission(_))));
    probe.assert_released(&scheduler);
    assert!(source.table_schema("documents").is_some());
    drop(work);
    let retry = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let copied = source.clone_metadata_with_work_context(&retry).unwrap();
    assert_eq!(
        copied.table_schema("documents"),
        source.table_schema("documents")
    );
    drop(copied);
    drop(retry);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_sparse_workspace_denies_wide_key_before_copy_and_retains_only_workspace_nodes() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let source = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let id = "k".repeat(128 * 1024);
    let hydrated = RelationalSparseRecoveryRow {
        table: "documents".into(),
        primary_key: RelationalKey(vec![RelationalValue::Text(id.clone())]),
        row: Some(upsert_row(&id, "owner-1", "body")),
    };
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let input = vec![hydrated.clone()];
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = source.sparse_workspace_with_allocations(
            input,
            RelationalRowChangeCaptureLimits::default(),
            "sparse relational recovery",
            Some(&context),
        );
        let wide_allocations = observer.finish();
        if succeeds {
            let (workspace, supplied) = result.unwrap();
            assert_eq!(workspace.row_count("documents"), 1);
            assert_eq!(supplied[0].primary_key, hydrated.primary_key);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 128 * 1024)
            );
            drop(supplied);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes > ceiling - 32 * 1024 && available_bytes < ceiling)
            );
            drop(workspace);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
            drop(context);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        assert_eq!(source.row_count("documents"), 0);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_sparse_workspace_moves_sorted_rows_into_bounded_pages() {
    use crate::background::CheckpointDecodeContext;
    let source = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let rows = (0..513)
        .map(|id| {
            let id = format!("id-{id:04}");
            RelationalSparseRecoveryRow {
                table: "documents".into(),
                primary_key: RelationalKey(vec![RelationalValue::Text(id.clone())]),
                row: Some(upsert_row(&id, "owner", "body")),
            }
        })
        .collect::<Vec<_>>();
    let (ordinary, expected_access) = source
        .sparse_workspace(
            rows.clone(),
            RelationalRowChangeCaptureLimits::default(),
            "sparse relational recovery",
        )
        .unwrap();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let (controlled, access) = source
        .sparse_workspace_with_allocations(
            rows,
            RelationalRowChangeCaptureLimits::default(),
            "sparse relational recovery",
            Some(&context),
        )
        .unwrap();
    assert_eq!(access, expected_access);
    assert_eq!(controlled.row_count("documents"), 513);
    let pages = controlled.segments["documents"].rows.pages();
    assert_eq!(pages.len(), 3);
    assert!(pages
        .iter()
        .all(|page| page.len() <= RELATIONAL_ROW_PAGE_MAX_ENTRIES));
    for entry in access {
        assert_eq!(
            controlled.row("documents", &entry.primary_key),
            ordinary.row("documents", &entry.primary_key)
        );
    }
}

#[test]
fn controlled_column_positions_share_results_errors_and_deny_capacity_before_allocation() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let source = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let schema = source.table_schema("documents").unwrap();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    for columns in [
        vec![],
        vec!["payload".into(), "id".into(), "id".into()],
        vec!["id".into(), "missing".into()],
    ] {
        let ordinary = column_positions(schema, &columns);
        let controlled = column_positions_with_allocations(schema, &columns, Some(&context));
        match (ordinary, controlled) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
            (Err(expected), Err(actual)) => assert_eq!(actual.to_string(), expected.to_string()),
            result => panic!("column-position drift: {result:?}"),
        }
    }
    let columns = vec!["id".to_string(); 16 * 1024];
    let ceiling = 4096;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let context = CheckpointDecodeContext {
        work: CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default())),
        memory: Default::default(),
    };
    let observer = crate::test_allocator::AllocationObservation::start();
    let denied = column_positions_with_allocations(schema, &columns, Some(&context));
    assert_eq!(observer.finish(), 0);
    assert!(matches!(denied, Err(RelationalError::Admission(_))));
    drop(context);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_row_page_copy_denies_before_wide_keys_and_retains_snapshot_leases() {
    use crate::background::CheckpointWorkContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let key = RelationalKey(vec![RelationalValue::Text(id.clone())]);
    let source = RelationalRowPages::from_map(BTreeMap::from([(
        key.clone(),
        upsert_row(&id, "owner", "body"),
    )]));
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let mut target = source.clone();
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = target.prepare_checkpoint_copy_for_key(&key, &work);
        let wide_allocations = observer.finish();
        if succeeds {
            result.unwrap();
            assert!(!target.pages().shares_storage_with(source.pages()));
            assert_eq!(target.shared_page_count(&source), 0);
            assert_eq!(target.get(&key), source.get(&key));
            let retained = target.clone();
            drop(target);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 128 * 1024)
            );
            assert_eq!(retained.get(&key), source.get(&key));
            drop(retained);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
            assert!(target.pages().shares_storage_with(source.pages()));
            assert_eq!(target.shared_page_count(&source), 1);
            assert_eq!(target.get(&key), source.get(&key));
            drop(target);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_row_page_copy_cancellation_keeps_directory_and_retries() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let key = RelationalKey(vec![RelationalValue::Text("id".into())]);
    let source = RelationalRowPages::from_map(BTreeMap::from([(
        key.clone(),
        upsert_row("id", "owner", "body"),
    )]));
    let mut target = source.clone();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let work = probe.context(scheduler.clone());
    assert!(matches!(
        target.prepare_checkpoint_copy_for_key(&key, &work),
        Err(RelationalError::Admission(_))
    ));
    probe.assert_released(&scheduler);
    assert!(target.pages().shares_storage_with(source.pages()));
    assert_eq!(target.shared_page_count(&source), 1);
    assert_eq!(target.get(&key), source.get(&key));
    target
        .prepare_checkpoint_copy_for_key(&key, &Default::default())
        .unwrap();
    assert!(!target.pages().shares_storage_with(source.pages()));
    assert_eq!(target.get(&key), source.get(&key));
}

#[test]
fn controlled_row_insert_denies_wide_empty_page_and_retains_new_key_ownership() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let key = RelationalKey(vec![RelationalValue::Text(id.clone())]);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let mut target = RelationalRowPages::default();
        let original = target.clone();
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let incoming = key.clone();
        let row = upsert_row(&id, "owner", "body");
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = target.insert_with_allocations(incoming, row, Some(&context));
        let wide_allocations = observer.finish();
        drop(context);
        if succeeds {
            assert!(result.unwrap().is_none());
            assert_eq!(target.len(), 1);
            let retained = target.clone();
            drop(target);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes <= ceiling - 128 * 1024)
            );
            assert!(retained.get(&key).is_some());
            drop(retained);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
            assert_eq!(target.len(), 0);
            assert!(target.pages().shares_storage_with(original.pages()));
            drop(target);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_row_insert_splits_match_ordinary_pages_and_keep_source_snapshot() {
    use crate::background::CheckpointDecodeContext;
    let rows = (0..256)
        .map(|id| {
            let id = format!("id-{id:04}");
            (
                RelationalKey(vec![RelationalValue::Text(id.clone())]),
                upsert_row(&id, "owner", "body"),
            )
        })
        .collect();
    let source = RelationalRowPages::from_map(rows);
    let mut ordinary = source.clone();
    let mut controlled = source.clone();
    let key = RelationalKey(vec![RelationalValue::Text("id-0256".into())]);
    let row = upsert_row("id-0256", "owner", "body");
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    assert_eq!(
        controlled
            .insert_with_allocations(key.clone(), row.clone(), Some(&context))
            .unwrap(),
        ordinary.insert(key.clone(), row)
    );
    assert_eq!(controlled.page_count(), ordinary.page_count());
    assert_eq!(
        controlled
            .pages()
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>(),
        ordinary
            .pages()
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        controlled.iter().collect::<Vec<_>>(),
        ordinary.iter().collect::<Vec<_>>()
    );
    assert_eq!(source.len(), 256);
    assert!(source.get(&key).is_none());
    assert_eq!(controlled.len(), 257);
}

#[test]
fn controlled_row_insert_cancellation_keeps_source_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkProbe};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let old_key = RelationalKey(vec![RelationalValue::Text("id-1".into())]);
    let source = RelationalRowPages::from_map(BTreeMap::from([(
        old_key.clone(),
        upsert_row("id-1", "owner", "body"),
    )]));
    let mut target = source.clone();
    let key = RelationalKey(vec![RelationalValue::Text("id-2".into())]);
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let context = CheckpointDecodeContext {
        work: probe.context(scheduler.clone()),
        memory: Default::default(),
    };
    assert!(matches!(
        target.insert_with_allocations(
            key.clone(),
            upsert_row("id-2", "owner", "body"),
            Some(&context)
        ),
        Err(RelationalError::Admission(_))
    ));
    probe.assert_released(&scheduler);
    assert!(target.pages().shares_storage_with(source.pages()));
    assert_eq!(target.len(), 1);
    assert!(target.get(&key).is_none());
    assert_eq!(target.get(&old_key), source.get(&old_key));
    let retry = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    target
        .insert_with_allocations(
            key.clone(),
            upsert_row("id-2", "owner", "body"),
            Some(&retry),
        )
        .unwrap();
    assert_eq!(target.len(), 2);
    assert!(target.get(&key).is_some());
    assert_eq!(source.len(), 1);
}

#[test]
fn controlled_row_key_projection_denies_wide_values_before_copy() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let row = upsert_row(&"k".repeat(128 * 1024), "owner", "body");
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4096),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4096))
        .unwrap();
    let context = CheckpointDecodeContext {
        work: CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default())),
        memory: Default::default(),
    };
    let observer = crate::test_allocator::AllocationObservation::start();
    let denied = row_key_with_allocations(&row, &[0], Some(&context));
    assert_eq!(observer.finish(), 0);
    assert!(matches!(denied, Err(RelationalError::Admission(_))));
    drop(context);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    assert_eq!(
        row_key_with_allocations(&row, &[2, 0, 0], Some(&context)).unwrap(),
        row_key(&row, &[2, 0, 0])
    );
}

#[test]
fn controlled_sparse_upsert_and_update_match_ordinary_recovery_captures() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let index_limits = RelationalIndexChangeCaptureLimits::default();
    let row_limits = RelationalRowChangeCaptureLimits::default();
    let base = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id-1", "owner-1", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let (directory, metadata) = canonical_metadata_state(&base, "controlled-sparse-mutation");
    let transactions = [
        RelationalTransaction {
            writes: vec![RelationalWrite::Upsert {
                table: "documents".into(),
                rows: vec![upsert_row("id-1", "owner-1", "new")],
                conflict_columns: vec!["id".into()],
                action: RelationalConflictAction::Update(vec![RelationalUpsertAssignment {
                    column: "payload".into(),
                    value: RelationalUpsertValue::ExcludedColumn("payload".into()),
                }]),
            }],
        },
        RelationalTransaction {
            writes: vec![RelationalWrite::UpdateWhere {
                table: "documents".into(),
                assignments: vec![RelationalUpdateAssignment {
                    column: "owner".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::Text("owner-2".into())),
                }],
                predicate: RelationalPredicate::Compare {
                    column: "id".into(),
                    op: RelationalComparisonOp::Eq,
                    value: RelationalValue::Text("id-1".into()),
                },
            }],
        },
    ];
    for transaction in transactions {
        let (ordinary, expected_indexes, expected_rows, access) = base
            .stage_transaction_with_index_row_and_replay_access(
                transaction.clone(),
                limits,
                overflow,
                index_limits,
                row_limits,
            )
            .unwrap();
        let hydrated = access
            .entries()
            .iter()
            .map(|entry| RelationalSparseRecoveryRow {
                table: entry.table.clone(),
                primary_key: entry.primary_key.clone(),
                row: base.row(&entry.table, &entry.primary_key).cloned(),
            })
            .collect();
        let context = CheckpointDecodeContext {
            work: Default::default(),
            memory: Default::default(),
        };
        let (controlled, indexes, rows) = metadata
            .stage_sparse_transaction_for_authoritative_recovery_with_allocations(
                RelationalSparseRecoveryStage {
                    transaction,
                    hydrated_access: hydrated,
                    mutation_limits: limits,
                    overflow_config: overflow,
                    index_capture_limits: index_limits,
                    row_capture_limits: row_limits,
                    expected_replay_access: &access,
                },
                Some(&context),
            )
            .unwrap();
        assert_eq!(indexes, expected_indexes);
        assert_eq!(rows, expected_rows);
        assert_eq!(
            controlled.row_count("documents"),
            ordinary.row_count("documents")
        );
        assert_eq!(metadata.row_count("documents"), 1);
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_row_reconstruction_retains_shared_values_and_skips_overwritten_payload() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let source = upsert_row("id", "owner", &"v".repeat(128 * 1024));
    for (ceiling, replacement, succeeds) in [
        (4096, false, false),
        (4096, true, true),
        (16 * 1024 * 1024, false, true),
    ] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result =
            RelationalRow::reconstruct_with_allocations(&source, Some(&context), |ordinal| {
                Ok(if replacement && ordinal == 2 {
                    RelationalValueRef::Text("short")
                } else {
                    source.values()[ordinal].as_ref()
                })
            });
        let wide_allocations = observer.finish();
        drop(context);
        if succeeds {
            let row = result.unwrap();
            if replacement {
                assert_eq!(wide_allocations, 0);
                assert_eq!(row.values()[2], RelationalValue::Text("short".into()));
            } else {
                assert_eq!(row, source);
            }
            let retained = row.clone();
            drop(row);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling)
            );
            assert_eq!(retained.values()[0], source.values()[0]);
            drop(retained);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    assert_eq!(
        source.values()[2],
        RelationalValue::Text("v".repeat(128 * 1024))
    );
}

#[test]
fn overwritten_update_arithmetic_still_fails_before_recovery_publication() {
    let mut create = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut create.writes[0] else {
        panic!("fixture schema");
    };
    schema.columns.push(RelationalColumnSchema {
        name: "count".into(),
        scalar_type: RelationalScalarType::BigInt,
        nullable: false,
        default: None,
    });
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let base = RelationalState::default()
        .stage_transaction(create, limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![RelationalRow::new(vec![
                        RelationalValue::Text("id".into()),
                        RelationalValue::Text("owner".into()),
                        RelationalValue::Text("body".into()),
                        RelationalValue::BigInt(i64::MAX),
                    ])],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::UpdateWhere {
            table: "documents".into(),
            assignments: vec![
                RelationalUpdateAssignment {
                    column: "count".into(),
                    value: RelationalUpdateValue::BigIntArithmetic {
                        left: RelationalBigIntOperand::Column("count".into()),
                        operator: RelationalBigIntArithmeticOperator::Add,
                        right: RelationalBigIntOperand::Value(RelationalValue::BigInt(1)),
                    },
                },
                RelationalUpdateAssignment {
                    column: "count".into(),
                    value: RelationalUpdateValue::Value(RelationalValue::BigInt(0)),
                },
            ],
            predicate: RelationalPredicate::Compare {
                column: "id".into(),
                op: RelationalComparisonOp::Eq,
                value: RelationalValue::Text("id".into()),
            },
        }],
    };
    let expected = base
        .stage_transaction(transaction.clone(), limits, overflow)
        .unwrap_err()
        .to_string();
    assert!(expected.contains("overflow"));
    let (directory, metadata) = canonical_metadata_state(&base, "overwritten-arithmetic");
    let key = RelationalKey(vec![RelationalValue::Text("id".into())]);
    let access = RelationalReplayAccessSet::from_decoded_entries(vec![RelationalReplayAccess {
        table: "documents".into(),
        primary_key: key.clone(),
    }])
    .unwrap();
    let context = crate::background::CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let error = metadata
        .stage_sparse_transaction_for_authoritative_recovery_with_allocations(
            RelationalSparseRecoveryStage {
                transaction,
                hydrated_access: vec![RelationalSparseRecoveryRow {
                    table: "documents".into(),
                    primary_key: key.clone(),
                    row: base.row("documents", &key).cloned(),
                }],
                mutation_limits: limits,
                overflow_config: overflow,
                index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
                row_capture_limits: RelationalRowChangeCaptureLimits::default(),
                expected_replay_access: &access,
            },
            Some(&context),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), expected);
    assert_eq!(
        base.row("documents", &key).unwrap().values()[3],
        RelationalValue::BigInt(i64::MAX)
    );
    assert_eq!(metadata.row_count("documents"), 1);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn update_without_matches_borrows_wide_literal_during_resolution() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let source = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::UpdateWhere {
            table: "documents".into(),
            assignments: vec![RelationalUpdateAssignment {
                column: "payload".into(),
                value: RelationalUpdateValue::Value(RelationalValue::Text("v".repeat(128 * 1024))),
            }],
            predicate: RelationalPredicate::Compare {
                column: "id".into(),
                op: RelationalComparisonOp::Eq,
                value: RelationalValue::Text("missing".into()),
            },
        }],
    };
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let observer = crate::test_allocator::AllocationObservation::start();
    let applied = apply_transaction_inner(
        &source,
        transaction,
        limits,
        overflow,
        TransactionApplyOptions {
            checkpoint_allocations: Some(&context),
            ..TransactionApplyOptions::materialized()
        },
    )
    .unwrap();
    assert_eq!(observer.finish(), 0);
    assert_eq!(applied.state.row_count("documents"), 0);
    assert_eq!(source.row_count("documents"), 0);
}

#[test]
fn controlled_resolved_collection_rejects_capacity_growth_and_preserves_errors() {
    use crate::background::CheckpointDecodeContext;
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let expected = vec![Ok(3), Ok(1), Ok(3)];
    assert_eq!(
        collect_results_with_allocations(expected.clone(), 3, None).unwrap(),
        collect_results_with_allocations(expected, 3, Some(&context)).unwrap()
    );
    assert!(matches!(
        collect_results_with_allocations(vec![Ok(1), Ok(2)], 1, Some(&context)),
        Err(RelationalError::Admission(_))
    ));
    let failure = || {
        vec![
            Ok(1),
            Err(RelationalError::Schema("missing assignment".into())),
        ]
    };
    assert_eq!(
        collect_results_with_allocations(failure(), 2, None)
            .unwrap_err()
            .to_string(),
        collect_results_with_allocations(failure(), 2, Some(&context))
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn controlled_update_match_denies_wide_key_before_copy_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let base = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row(&id, "owner", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let key = RelationalKey(vec![RelationalValue::Text(id.clone())]);
    let assignments = vec![RelationalUpdateAssignment {
        column: "payload".into(),
        value: RelationalUpdateValue::Value(RelationalValue::Text("new".into())),
    }];
    let predicate = RelationalPredicate::Compare {
        column: "id".into(),
        op: RelationalComparisonOp::Eq,
        value: RelationalValue::Text(id.clone()),
    };
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let mut target = base.clone();
        let mut changed = BTreeSet::new();
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = apply_update(
            &mut target,
            "documents",
            &assignments,
            &predicate,
            UpdateApplyContext {
                limits,
                overflow_config: overflow,
                changed_keys: &mut changed,
                checkpoint_allocations: Some(&context),
                replay_access_tracker: None,
                primary_key_lookup: true,
            },
        );
        let wide_allocations = observer.finish();
        if succeeds {
            assert_eq!(result.unwrap(), 1);
            assert_eq!(
                target.row("documents", &key).unwrap().values()[2],
                RelationalValue::Text("new".into())
            );
            assert!(changed.contains(&key));
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(wide_allocations, 0);
            assert_eq!(target.row("documents", &key), base.row("documents", &key));
            assert!(changed.is_empty());
        }
        assert_eq!(
            base.row("documents", &key).unwrap().values()[2],
            RelationalValue::Text("old".into())
        );
        drop(changed);
        drop(target);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_replay_access_retains_key_inventory_until_output_drop() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut tracker = RelationalReplayAccessTracker::new(
            RelationalRowChangeCaptureLimits::default(),
            Some(&context),
        );
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = tracker.record("documents", &key);
        let copies = observer.finish();
        if succeeds {
            result.unwrap();
            let observer = crate::test_allocator::AllocationObservation::start();
            tracker.record("documents", &key).unwrap();
            assert_eq!(observer.finish(), 0);
            let output = tracker.finish().unwrap();
            drop(context);
            assert_eq!(output.entries()[0].primary_key, key);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes < ceiling - 128 * 1024)
            );
            let copy = output.clone();
            assert_eq!(copy, output);
            drop(output);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes == ceiling)
            );
            assert_eq!(copy.entries()[0].primary_key, key);
            drop(copy);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(tracker.entries.is_empty());
            assert_eq!(tracker.entry_count, 0);
            assert_eq!(tracker.encoded_bytes, 0);
            drop(tracker);
            drop(context);
        }
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_changed_key_tracking_admits_nodes_and_key_copies() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut keys = BTreeSet::new();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = insert_changed_key_with_allocations(&mut keys, &key, Some(&context));
        let copies = observer.finish();
        if succeeds {
            result.unwrap();
            assert!(keys.contains(&key));
            let observer = crate::test_allocator::AllocationObservation::start();
            insert_changed_key_with_allocations(&mut keys, &key, Some(&context)).unwrap();
            assert_eq!(observer.finish(), 0);
            assert_eq!(keys.len(), 1);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes < ceiling - 128 * 1024)
            );
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(keys.is_empty());
        }
        drop(keys);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_conflict_map_denial_preserves_entries_and_replacement_reuses_nodes() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let ceiling = 4096;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let context = CheckpointDecodeContext {
        work: work.clone(),
        memory: Default::default(),
    };
    let mut conflicts = BTreeMap::new();
    let mut denied = false;
    for id in 0..64 {
        let key = RelationalKey(vec![RelationalValue::BigInt(id)]);
        let before = conflicts.clone();
        match insert_staged_conflict_with_allocations(
            &mut conflicts,
            key.clone(),
            None,
            Some(&context),
        ) {
            Ok(previous) => {
                assert_eq!(previous, None);
                let remaining = match work.task_context().reserve_working_memory(ceiling) {
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded {
                        available_bytes,
                        ..
                    }) => available_bytes,
                    other => panic!("unexpected reservation: {other:?}"),
                };
                assert_eq!(
                    insert_staged_conflict_with_allocations(
                        &mut conflicts,
                        key.clone(),
                        Some(key),
                        Some(&context)
                    )
                    .unwrap(),
                    Some(None)
                );
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling),
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == remaining)
                );
            }
            Err(RelationalError::Admission(_)) => {
                assert_eq!(conflicts, before);
                denied = true;
                break;
            }
            Err(error) => panic!("unexpected error: {error}"),
        }
    }
    assert!(denied);
    drop(conflicts);
    drop(context);
    assert!(
        matches!(work.task_context().reserve_working_memory(ceiling),
        Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn conflict_projection_compares_wide_values_without_owned_keys() {
    let row = RelationalRow::new(vec![
        RelationalValue::Text("t".repeat(128 * 1024)),
        RelationalValue::Bytea(vec![0xff; 128 * 1024]),
        RelationalValue::Null,
    ]);
    for positions in [vec![], vec![0], vec![1], vec![2], vec![1, 0, 1], vec![2, 0]] {
        let expected = row_key(&row, &positions);
        let mut wrong = expected.clone();
        wrong.0.push(RelationalValue::Null);
        let observer = crate::test_allocator::AllocationObservation::start();
        assert!(row_matches_key(&row, &positions, &expected));
        assert!(!row_matches_key(&row, &positions, &wrong));
        assert_eq!(observer.finish(), 0);
    }
}

#[test]
fn controlled_primary_conflict_probe_denies_wide_result_before_copy() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let state = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row(&id, "owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let key = RelationalKey(vec![RelationalValue::Text(id)]);
    let schema = state.table_schema("documents").unwrap();
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = conflict_primary_key(
            &state,
            "documents",
            schema,
            &schema.primary_key,
            &key,
            &BTreeSet::new(),
            RelationalConflictProbeContext {
                constraint_index: None,
                allocations: Some(&context),
            },
        );
        let copies = observer.finish();
        if succeeds {
            assert_eq!(result.unwrap(), Some(key.clone()));
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert!(state.row("documents", &key).is_some());
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_transaction_table_names_deny_before_copy_and_deduplicate() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let name = "t".repeat(128 * 1024);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut changed = BTreeMap::new();
        let mut touched = BTreeSet::new();
        let observer = crate::test_allocator::AllocationObservation::start();
        let changed_result =
            changed_table_keys_with_allocations(&mut changed, &name, Some(&context)).map(|_| ());
        let touched_result =
            insert_table_name_with_allocations(&mut touched, &name, Some(&context));
        let copies = observer.finish();
        if succeeds {
            changed_result.unwrap();
            touched_result.unwrap();
            assert_eq!(changed.len(), 1);
            assert!(changed[&name].is_empty());
            assert!(touched.contains(&name));
            let observer = crate::test_allocator::AllocationObservation::start();
            changed_table_keys_with_allocations(&mut changed, &name, Some(&context)).unwrap();
            insert_table_name_with_allocations(&mut touched, &name, Some(&context)).unwrap();
            assert_eq!(observer.finish(), 0);
        } else {
            assert!(matches!(changed_result, Err(RelationalError::Admission(_))));
            assert!(matches!(touched_result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(changed.is_empty());
            assert!(touched.is_empty());
        }
        drop(changed);
        drop(touched);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_delete_match_keys_deny_before_copy_and_release_capacity() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut keys = Vec::new();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = push_cloned_key_with_allocations(&mut keys, &key, Some(&context));
        let copies = observer.finish();
        if succeeds {
            result.unwrap();
            assert_eq!(keys.as_slice(), std::slice::from_ref(&key));
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes < ceiling - 128 * 1024)
            );
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(keys.is_empty());
        }
        drop(keys);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_transitive_cascade_matches_ordinary_captures_and_preserves_source() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let base = RelationalState::default()
        .stage_transaction(create_cascade_tables(), limits, overflow)
        .unwrap()
        .stage_transaction(seed_cascade_rows(), limits, overflow)
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::DeleteByPrimaryKey {
            table: "cascade_parents".into(),
            keys: vec![RelationalKey(vec![RelationalValue::Text(
                "parent-1".into(),
            )])],
        }],
    };
    let options = TransactionApplyOptions {
        index_capture_limits: Some(RelationalIndexChangeCaptureLimits::default()),
        row_capture_limits: Some(RelationalRowChangeCaptureLimits::default()),
        replay_access_limits: Some(RelationalRowChangeCaptureLimits::default()),
        ..TransactionApplyOptions::materialized()
    };
    let expected =
        apply_transaction_inner(&base, transaction.clone(), limits, overflow, options).unwrap();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let actual = apply_transaction_inner(
        &base,
        transaction,
        limits,
        overflow,
        TransactionApplyOptions {
            checkpoint_allocations: Some(&context),
            ..options
        },
    )
    .unwrap();
    assert_eq!(actual.index_capture, expected.index_capture);
    assert_eq!(actual.row_capture, expected.row_capture);
    assert_eq!(actual.replay_access, expected.replay_access);
    for table in [
        "cascade_parents",
        "cascade_children",
        "cascade_grandchildren",
    ] {
        assert_eq!(actual.state.row_count(table), 0);
        assert_eq!(expected.state.row_count(table), 0);
        assert_eq!(base.row_count(table), 1);
    }
}

#[test]
fn controlled_cascade_queue_growth_preserves_wrapped_fifo_order() {
    use crate::background::CheckpointDecodeContext;
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let mut queue = VecDeque::new();
    for id in 0..16 {
        push_cascade_delete_with_allocations(
            &mut queue,
            "table",
            RelationalRow::new(vec![RelationalValue::BigInt(id)]),
            Some(&context),
        )
        .unwrap();
    }
    for id in 0..10 {
        assert_eq!(
            queue.pop_front().unwrap().row.values()[0],
            RelationalValue::BigInt(id)
        );
    }
    for id in 16..32 {
        push_cascade_delete_with_allocations(
            &mut queue,
            "table",
            RelationalRow::new(vec![RelationalValue::BigInt(id)]),
            Some(&context),
        )
        .unwrap();
    }
    for id in 10..32 {
        let entry = queue.pop_front().unwrap();
        assert_eq!(entry.table, "table");
        assert_eq!(entry.row.values()[0], RelationalValue::BigInt(id));
    }
    assert!(queue.is_empty());
}

#[test]
fn controlled_cascade_queue_denies_wide_name_before_copy_and_releases_inventory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let name = "t".repeat(128 * 1024);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut queue = VecDeque::new();
        let row = RelationalRow::new(vec![RelationalValue::BigInt(17)]);
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = push_cascade_delete_with_allocations(&mut queue, &name, row, Some(&context));
        let copies = observer.finish();
        if succeeds {
            result.unwrap();
            assert_eq!(queue.len(), 1);
            assert_eq!(queue[0].table, name);
            assert_eq!(queue[0].row.values()[0], RelationalValue::BigInt(17));
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(queue.is_empty());
        }
        drop(queue);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_overflow_staging_denies_before_copy_and_extent_retains_ownership() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let fixture = RelationalState::default()
        .stage_transaction(
            create_upsert_table(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let mut random = 0x9e37_79b9_7f4a_7c15_u64;
    let bytes = (0..128 * 1024)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random as u8
        })
        .collect::<Vec<_>>();
    for payload in [
        RelationalValue::Text("中".repeat(64 * 1024)),
        RelationalValue::Bytea(bytes),
    ] {
        let mut schema = fixture.table_schema("documents").unwrap().clone();
        schema.columns[2].scalar_type = match &payload {
            RelationalValue::Text(_) => RelationalScalarType::Text,
            _ => RelationalScalarType::Bytea,
        };
        let original = RelationalRow::new(vec![
            RelationalValue::Text("id".into()),
            RelationalValue::Text("owner".into()),
            payload.clone(),
        ]);
        let raw = match &payload {
            RelationalValue::Text(text) => text.as_bytes(),
            RelationalValue::Bytea(bytes) => bytes.as_slice(),
            _ => unreachable!(),
        };
        let ordinary = overflow::encode_overflow_envelope(
            schema.columns[2].scalar_type,
            raw,
            RelationalOverflowConfig::default(),
        )
        .unwrap();
        for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
            let governor = RuntimeGovernor::detect(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(ceiling),
                    ..RuntimeGovernorConfig::shared_host()
                },
                IoConcurrencyBudget::new(2, 1),
            );
            governor.pin_resources();
            let permit = governor
                .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
                .unwrap();
            let work =
                CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
            let context = CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            };
            let mut state = RelationalState::default();
            let mut row = original.clone();
            let observer = crate::test_allocator::AllocationObservation::start();
            let result = overflow::externalize_row_with_allocations(
                &mut state,
                &schema,
                &mut row,
                RelationalOverflowConfig::default(),
                Some(&context),
            );
            let copies = observer.finish();
            if succeeds {
                result.unwrap();
                let RelationalValue::Overflow(reference) = row.values()[2] else {
                    panic!("externalized payload");
                };
                assert_eq!(reference, ordinary.reference);
                let input = state.inline_overflow_extent_input(&reference).unwrap();
                assert!(matches!(
                    &input,
                    RelationalOverflowExtentInput::CheckpointWrite { .. }
                ));
                assert_eq!(input.encoded_bytes().unwrap(), ordinary.bytes.as_ref());
                assert_eq!(
                    overflow::decode_overflow_envelope(
                        &reference,
                        input.encoded_bytes().unwrap(),
                        &mut RelationalHydrationBudget::default(),
                        None
                    )
                    .unwrap(),
                    payload
                );
                let snapshot = state.clone();
                drop(row);
                drop(state);
                drop(context);
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling)
                );
                drop(snapshot);
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling)
                );
                drop(input);
            } else {
                assert!(matches!(result, Err(RelationalError::Admission(_))));
                assert_eq!(copies, 0);
                assert_eq!(row, original);
                assert_eq!(state.overflow_segment_count(), 0);
                drop(row);
                drop(state);
                drop(context);
            }
            assert_eq!(original.values()[2], payload);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            drop(work);
            drop(permit);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }
}

#[test]
fn controlled_row_capture_admits_wide_keys_and_holds_inventory_until_encoding() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap();
    let next = previous
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row(&id, "owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let changed = BTreeMap::from([(
        "documents".into(),
        BTreeSet::from([RelationalKey(vec![RelationalValue::Text(id)])]),
    )]);
    let expected = capture_relational_row_changes(
        &previous,
        &next,
        &changed,
        &BTreeSet::new(),
        RelationalRowChangeCaptureLimits::default(),
    );
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = capture_relational_row_changes_with_allocations(
            &previous,
            &next,
            &changed,
            &BTreeSet::new(),
            RelationalRowChangeCaptureLimits::default(),
            Some(&context),
        );
        let copies = observer.finish();
        if succeeds {
            let capture = result.unwrap();
            assert_eq!(capture, expected);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(capture);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert_eq!(previous.row_count("documents"), 0);
        assert_eq!(next.row_count("documents"), 1);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_index_capture_admits_wide_keys_and_preserves_caller_inventory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap();
    let next = previous
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row(&id, "owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let changed = BTreeMap::from([(
        "documents".into(),
        BTreeSet::from([RelationalKey(vec![RelationalValue::Text(id)])]),
    )]);
    let expected = capture_relational_index_changes(
        &previous,
        &next,
        &changed,
        &BTreeSet::new(),
        RelationalIndexChangeCaptureLimits::default(),
    );
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = capture_relational_index_changes_with_allocations(
            &previous,
            &next,
            &changed,
            &BTreeSet::new(),
            RelationalIndexChangeCaptureLimits::default(),
            Some(&context),
        );
        let copies = observer.finish();
        if succeeds {
            let capture = result.unwrap();
            assert_eq!(capture, expected);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(capture);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert_eq!(previous.row_count("documents"), 0);
        assert_eq!(next.row_count("documents"), 1);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_index_definitions_match_all_roles_and_decimal_names() {
    use crate::background::CheckpointDecodeContext;
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let mut transaction = create_cascade_tables();
    transaction.writes.extend(create_upsert_table().writes);
    for write in transaction.writes {
        let RelationalWrite::CreateTable(mut schema) = write else {
            panic!("fixture schema");
        };
        schema.indexes.push(RelationalIndexSchema {
            name: "secondary".into(),
            columns: schema.primary_key.clone(),
            unique: false,
        });
        schema.indexes.push(RelationalIndexSchema {
            name: "declared_unique".into(),
            columns: schema.primary_key.clone(),
            unique: true,
        });
        assert_eq!(
            required_index_definitions_with_allocations(&schema, Some(&context)).unwrap(),
            schema.required_index_definitions()
        );
    }
    for ordinal in [0, 9, 10, usize::MAX] {
        assert_eq!(
            admitted_ordinal_index_name(RELATIONAL_UNIQUE_INDEX_PREFIX, ordinal, &context).unwrap(),
            relational_unique_index_name(ordinal)
        );
        assert_eq!(
            admitted_ordinal_index_name(RELATIONAL_FOREIGN_KEY_INDEX_PREFIX, ordinal, &context)
                .unwrap(),
            relational_foreign_key_index_name(ordinal)
        );
    }
}

#[test]
fn controlled_secondary_index_capture_admits_projection_keys_before_copy() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "old-owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let next = previous
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", &"k".repeat(128 * 1024), "body")],
                    mode: RelationalInsertMode::Replace,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let changed = BTreeMap::from([(
        "documents".into(),
        BTreeSet::from([RelationalKey(vec![RelationalValue::Text("id".into())])]),
    )]);
    let expected = capture_relational_index_changes(
        &previous,
        &next,
        &changed,
        &BTreeSet::new(),
        RelationalIndexChangeCaptureLimits::default(),
    );
    let RelationalIndexChangeCapture::Captured { changes, .. } = &expected else {
        panic!("fixture capture");
    };
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].kind, RelationalIndexChangeKind::Delete);
    assert_eq!(changes[1].kind, RelationalIndexChangeKind::Insert);
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = capture_relational_index_changes_with_allocations(
            &previous,
            &next,
            &changed,
            &BTreeSet::new(),
            RelationalIndexChangeCaptureLimits::default(),
            Some(&context),
        );
        let copies = observer.finish();
        if succeeds {
            let capture = result.unwrap();
            assert_eq!(capture, expected);
            drop(capture);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert_eq!(previous.row_count("documents"), 1);
        assert_eq!(next.row_count("documents"), 1);
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_resident_recovery_matches_captures_and_rejects_before_wide_cow_copy() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let id = "k".repeat(128 * 1024);
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row(&id, "owner", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let key = RelationalKey(vec![RelationalValue::Text(id.clone())]);
    let access = RelationalReplayAccessSet::from_decoded_entries(vec![RelationalReplayAccess {
        table: "documents".into(),
        primary_key: key.clone(),
    }])
    .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".into(),
            rows: vec![upsert_row(&id, "owner", "new")],
            mode: RelationalInsertMode::Replace,
        }],
    };
    let (expected, expected_indexes, expected_rows) = previous
        .stage_transaction_for_authoritative_recovery_with_replay_access(
            transaction.clone(),
            limits,
            overflow,
            RelationalIndexChangeCaptureLimits::default(),
            RelationalRowChangeCaptureLimits::default(),
            &access,
        )
        .unwrap();
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let input = transaction.clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = previous
            .stage_resident_transaction_for_authoritative_recovery_with_allocations(
                RelationalResidentRecoveryStage {
                    transaction: input,
                    mutation_limits: limits,
                    overflow_config: overflow,
                    index_capture_limits: RelationalIndexChangeCaptureLimits::default(),
                    row_capture_limits: RelationalRowChangeCaptureLimits::default(),
                    expected_replay_access: &access,
                },
                Some(&context),
            );
        let copies = observer.finish();
        if succeeds {
            let (actual, indexes, rows) = result.unwrap();
            assert_eq!(
                actual.row("documents", &key),
                expected.row("documents", &key)
            );
            assert_eq!(indexes, expected_indexes);
            assert_eq!(rows, expected_rows);
            assert!(!actual.materialized_index_postings_resident());
            drop(rows);
            drop(indexes);
            drop(actual);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert_eq!(
            previous.row("documents", &key).unwrap().values()[2],
            RelationalValue::Text("old".into())
        );
        assert!(previous.materialized_index_postings_resident());
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_table_shell_omission_skips_wide_index_names_and_owns_arc_backing() {
    use crate::background::CheckpointWorkContext;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let source = RelationalTableSegment {
        last_row_id: 0,
        rows: Default::default(),
        indexes: BTreeMap::from([("i".repeat(128 * 1024), RelationalIndexPages::default())]),
        _memory: Default::default(),
    };
    let ceiling = 4096;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observer = crate::test_allocator::AllocationObservation::start();
    let copy = source.checkpoint_without_indexes(&work).unwrap();
    assert_eq!(observer.finish(), 0);
    assert!(copy.indexes.is_empty());
    assert_eq!(source.indexes.len(), 1);
    assert!(copy.rows.pages().shares_storage_with(source.rows.pages()));
    let snapshot = copy.clone();
    drop(copy);
    assert!(
        matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling)
    );
    drop(snapshot);
    assert!(
        matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_posting_split_and_removal_match_ordinary_page_boundaries() {
    use crate::background::CheckpointDecodeContext;
    let mut source = RelationalKeySetPages::default();
    for id in 0..256 {
        source.insert(RelationalKey(vec![RelationalValue::BigInt(id)]));
    }
    let mut ordinary = source.clone();
    let mut controlled = source.clone();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let added = RelationalKey(vec![RelationalValue::BigInt(256)]);
    assert_eq!(
        controlled
            .insert_with_allocations(added.clone(), Some(&context))
            .unwrap(),
        ordinary.insert(added.clone())
    );
    assert_eq!(controlled.page_count(), ordinary.page_count());
    assert_eq!(
        controlled
            .pages
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>(),
        ordinary
            .pages
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        controlled.iter().collect::<Vec<_>>(),
        ordinary.iter().collect::<Vec<_>>()
    );
    assert!(!controlled
        .insert_with_allocations(added, Some(&context))
        .unwrap());
    for id in [0, 127, 128, 256] {
        let key = RelationalKey(vec![RelationalValue::BigInt(id)]);
        assert_eq!(
            controlled
                .remove_with_allocations(&key, Some(&context))
                .unwrap(),
            ordinary.remove(&key)
        );
    }
    assert_eq!(
        controlled.iter().collect::<Vec<_>>(),
        ordinary.iter().collect::<Vec<_>>()
    );
    assert_eq!(source.len, 256);
    assert_eq!(source.page_count(), 1);
}

#[test]
fn controlled_posting_cancellation_preserves_shared_snapshot_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkProbe};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let mut source = RelationalKeySetPages::default();
    source.insert(RelationalKey(vec![RelationalValue::BigInt(0)]));
    let mut target = source.clone();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let context = CheckpointDecodeContext {
        work: probe.context(scheduler.clone()),
        memory: Default::default(),
    };
    let key = RelationalKey(vec![RelationalValue::BigInt(1)]);
    assert!(matches!(
        target.insert_with_allocations(key.clone(), Some(&context)),
        Err(RelationalError::Admission(_))
    ));
    probe.assert_released(&scheduler);
    assert!(target.pages.shares_storage_with(&source.pages));
    assert_eq!(
        target.iter().collect::<Vec<_>>(),
        source.iter().collect::<Vec<_>>()
    );
    let retry = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    assert!(target.insert_with_allocations(key, Some(&retry)).unwrap());
    assert_eq!(target.len, 2);
    assert_eq!(source.len, 1);
}

#[test]
fn controlled_posting_removal_denies_wide_copy_and_refunds_destroyed_page() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let key = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    let mut source = RelationalKeySetPages::default();
    source.insert(key.clone());
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut target = source.clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = target.remove_with_allocations(&key, Some(&context));
        let copies = observer.finish();
        if succeeds {
            assert!(result.unwrap());
            assert!(target.is_empty());
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes > ceiling - 32 * 1024 && available_bytes < ceiling)
            );
            let snapshot = target.clone();
            drop(target);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(target.pages.shares_storage_with(&source.pages));
            drop(target);
            drop(context);
        }
        assert_eq!(source.len, 1);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_index_page_split_and_incremental_removal_match_ordinary() {
    use crate::background::CheckpointDecodeContext;
    let mut source = RelationalIndexPages::default();
    for id in 0..256 {
        source.insert_posting(
            RelationalKey(vec![RelationalValue::BigInt(id)]),
            RelationalKey(vec![RelationalValue::BigInt(id)]),
        );
    }
    let mut ordinary = source.clone();
    let mut controlled = source.clone();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let key = RelationalKey(vec![RelationalValue::BigInt(256)]);
    assert_eq!(
        controlled
            .insert_posting_with_allocations(key.clone(), key.clone(), Some(&context))
            .unwrap(),
        ordinary.insert_posting(key.clone(), key.clone())
    );
    assert_eq!(
        controlled
            .pages
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>(),
        ordinary
            .pages
            .iter()
            .map(|page| page.len())
            .collect::<Vec<_>>()
    );
    assert!(!controlled
        .insert_posting_with_allocations(key.clone(), key.clone(), Some(&context))
        .unwrap());
    for id in [0, 127, 128, 256] {
        let key = RelationalKey(vec![RelationalValue::BigInt(id)]);
        controlled
            .remove_posting_with_allocations(&key, &key, Some(&context))
            .unwrap();
        ordinary.remove_posting(&key, &key);
    }
    for id in 0..257 {
        let key = RelationalKey(vec![RelationalValue::BigInt(id)]);
        assert_eq!(
            controlled
                .get(&key)
                .map(|set| set.iter().collect::<Vec<_>>()),
            ordinary.get(&key).map(|set| set.iter().collect::<Vec<_>>())
        );
    }
    assert_eq!(source.pages.len(), 1);
    assert_eq!(source.pages[0].len(), 256);
}

#[test]
fn controlled_index_page_cancellation_keeps_original_directory_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkProbe};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let mut source = RelationalIndexPages::default();
    let original = RelationalKey(vec![RelationalValue::BigInt(0)]);
    source.insert_posting(original.clone(), original.clone());
    let mut target = source.clone();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let context = CheckpointDecodeContext {
        work: probe.context(scheduler.clone()),
        memory: Default::default(),
    };
    let added = RelationalKey(vec![RelationalValue::BigInt(1)]);
    assert!(matches!(
        target.insert_posting_with_allocations(added.clone(), added.clone(), Some(&context)),
        Err(RelationalError::Admission(_))
    ));
    probe.assert_released(&scheduler);
    assert!(target.pages.shares_storage_with(&source.pages));
    assert!(target.get(&added).is_none());
    let retry = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    assert!(target
        .insert_posting_with_allocations(added.clone(), added.clone(), Some(&retry))
        .unwrap());
    assert!(target.get(&added).is_some());
    assert!(source.get(&added).is_none());
}

#[test]
fn controlled_index_page_wide_copy_refusal_preserves_unmodified_postings() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let wide = RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]);
    let existing = RelationalKey(vec![RelationalValue::BigInt(0)]);
    let added = RelationalKey(vec![RelationalValue::BigInt(1)]);
    let mut source = RelationalIndexPages::default();
    source.insert_posting(wide.clone(), existing.clone());
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut target = source.clone();
        let incoming = wide.clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result =
            target.insert_posting_with_allocations(incoming, added.clone(), Some(&context));
        let copies = observer.finish();
        if succeeds {
            assert!(result.unwrap());
            assert_eq!(target.get(&wide).unwrap().len, 2);
            assert_eq!(source.get(&wide).unwrap().len, 1);
            let snapshot = target.clone();
            drop(target);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(target.pages.shares_storage_with(&source.pages));
            drop(target);
            drop(context);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_materialized_dml_matches_all_capture_combinations_and_unique_swap() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "old-1"),
                        upsert_row("id-2", "owner-2", "old-2"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".into(),
            rows: vec![
                upsert_row("id-1", "owner-2", "new-1"),
                upsert_row("id-2", "owner-1", "new-2"),
            ],
            mode: RelationalInsertMode::Replace,
        }],
    };
    for index_capture_limits in [None, Some(RelationalIndexChangeCaptureLimits::default())] {
        for row_capture_limits in [None, Some(RelationalRowChangeCaptureLimits::default())] {
            let expected = apply_transaction_inner(
                &previous,
                transaction.clone(),
                limits,
                overflow,
                TransactionApplyOptions {
                    index_capture_limits,
                    row_capture_limits,
                    ..TransactionApplyOptions::materialized()
                },
            )
            .unwrap();
            let context = CheckpointDecodeContext {
                work: Default::default(),
                memory: Default::default(),
            };
            let (actual, indexes, rows) = previous
                .stage_materialized_recovery_with_allocations(
                    RelationalMaterializedRecoveryStage {
                        transaction: transaction.clone(),
                        mutation_limits: limits,
                        overflow_config: overflow,
                        index_capture_limits,
                        row_capture_limits,
                    },
                    &context,
                )
                .unwrap();
            assert_eq!(indexes, expected.index_capture);
            assert_eq!(rows, expected.row_capture);
            assert_eq!(
                actual.rows("documents").collect::<Vec<_>>(),
                expected.state.rows("documents").collect::<Vec<_>>()
            );
            for owner in ["owner-1", "owner-2"] {
                let key = RelationalKey(vec![RelationalValue::Text(owner.into())]);
                assert_eq!(
                    actual
                        .index_lookup("documents", &relational_unique_index_name(0), &key)
                        .map(|set| set.iter().collect::<Vec<_>>()),
                    expected
                        .state
                        .index_lookup("documents", &relational_unique_index_name(0), &key)
                        .map(|set| set.iter().collect::<Vec<_>>())
                );
            }
            assert_eq!(
                previous
                    .row(
                        "documents",
                        &RelationalKey(vec![RelationalValue::Text("id-1".into())])
                    )
                    .unwrap()
                    .values()[1],
                RelationalValue::Text("owner-1".into())
            );
        }
    }
}

#[test]
fn controlled_materialized_dml_denies_wide_directory_copy_without_source_mutation() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let mut create = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut create.writes[0] else {
        panic!("fixture schema");
    };
    schema.indexes.push(RelationalIndexSchema {
        name: "i".repeat(128 * 1024),
        columns: vec!["owner".into()],
        unique: false,
    });
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create, limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "owner", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::Insert {
            table: "documents".into(),
            rows: vec![upsert_row("id", "owner", "new")],
            mode: RelationalInsertMode::Replace,
        }],
    };
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let input = transaction.clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = previous.stage_materialized_recovery_with_allocations(
            RelationalMaterializedRecoveryStage {
                transaction: input,
                mutation_limits: limits,
                overflow_config: overflow,
                index_capture_limits: None,
                row_capture_limits: None,
            },
            &context,
        );
        let copies = observer.finish();
        if succeeds {
            let (actual, indexes, rows) = result.unwrap();
            assert!(indexes.is_none());
            assert!(rows.is_none());
            assert_eq!(
                actual
                    .row(
                        "documents",
                        &RelationalKey(vec![RelationalValue::Text("id".into())])
                    )
                    .unwrap()
                    .values()[2],
                RelationalValue::Text("new".into())
            );
            drop(actual);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert_eq!(
            previous
                .row(
                    "documents",
                    &RelationalKey(vec![RelationalValue::Text("id".into())])
                )
                .unwrap()
                .values()[2],
            RelationalValue::Text("old".into())
        );
        drop(context);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_schema_copy_preserves_nested_metadata_and_releases_after_last_clone() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let mut transaction = create_upsert_table();
    let RelationalWrite::CreateTable(mut schema) = transaction.writes.remove(0) else {
        panic!("fixture schema");
    };
    schema.columns[2].default = Some(RelationalColumnDefault::Literal(RelationalValue::Text(
        "d".repeat(128 * 1024),
    )));
    schema.foreign_keys.push(RelationalForeignKeySchema {
        columns: vec!["owner".into()],
        referenced_table: "parents".into(),
        referenced_columns: vec!["id".into()],
        on_delete: RelationalReferentialAction::Cascade,
        on_update: RelationalReferentialAction::NoAction,
    });
    schema.indexes.push(RelationalIndexSchema {
        name: "by_owner".into(),
        columns: vec!["owner".into()],
        unique: false,
    });
    let source = RelationalSchemaData::from(schema);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = source.checkpoint_edit(&context.work, |_, _| Ok(()));
        let copies = observer.finish();
        if succeeds {
            let copy = result.unwrap();
            assert_eq!(copy.schema, source.schema);
            let snapshot = copy.clone();
            drop(copy);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            drop(context);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_schema_edit_admits_index_vector_growth_and_keeps_source_unchanged() {
    use crate::background::CheckpointWorkContext;
    let mut transaction = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = transaction.writes.remove(0) else {
        panic!("fixture schema");
    };
    let source = RelationalSchemaData::from(schema);
    let added = RelationalIndexSchema {
        name: "by_payload".into(),
        columns: vec!["payload".into()],
        unique: false,
    };
    let actual = source
        .checkpoint_edit(&CheckpointWorkContext::default(), |schema, context| {
            let owned = clone_index_schema_with_allocations(&added, context)?;
            context
                .push(&mut schema.indexes, owned)
                .map_err(|error| RelationalError::Admission(error.to_string()))
        })
        .unwrap();
    assert!(source.indexes.is_empty());
    assert_eq!(actual.indexes, vec![added]);
    assert_eq!(actual.columns, source.columns);
    assert_eq!(actual.unique_constraints, source.unique_constraints);
}

#[test]
fn controlled_index_rebuild_matches_all_postings_and_preserves_shared_rows() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let source = RelationalState::default()
        .stage_transaction(create_cascade_tables(), limits, overflow)
        .unwrap()
        .stage_transaction(seed_cascade_rows(), limits, overflow)
        .unwrap();
    let mut expected = source.clone();
    let mut actual = source.clone();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    for table in [
        "cascade_parents",
        "cascade_children",
        "cascade_grandchildren",
    ] {
        rebuild_indexes(&mut expected, table).unwrap();
        rebuild_indexes_with_allocations(&mut actual, table, &context).unwrap();
        assert!(actual.segments[table]
            .rows
            .pages()
            .shares_storage_with(source.segments[table].rows.pages()));
        for definition in source
            .table_schema(table)
            .unwrap()
            .required_index_definitions()
        {
            if definition.role == RelationalIndexRole::Primary {
                continue;
            }
            let positions =
                column_positions(source.table_schema(table).unwrap(), &definition.columns).unwrap();
            for (_, row) in source.rows(table) {
                let key = row_key(row, &positions);
                assert_eq!(
                    actual
                        .index_lookup(table, &definition.name, &key)
                        .map(|set| set.iter().collect::<Vec<_>>()),
                    expected
                        .index_lookup(table, &definition.name, &key)
                        .map(|set| set.iter().collect::<Vec<_>>())
                );
            }
        }
    }
    assert_eq!(source.total_row_count(), 3);
}

#[test]
fn controlled_index_rebuild_denies_wide_name_before_copy_and_retains_output_inventory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let mut create = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut create.writes[0] else {
        panic!("fixture schema");
    };
    let name = "i".repeat(128 * 1024);
    schema.indexes.push(RelationalIndexSchema {
        name: name.clone(),
        columns: vec!["owner".into()],
        unique: false,
    });
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let source = RelationalState::default()
        .stage_transaction(create, limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut target = source.clone();
        let original = target.segments["documents"].clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = rebuild_indexes_with_allocations(&mut target, "documents", &context);
        let copies = observer.finish();
        if succeeds {
            result.unwrap();
            assert!(!Arc::ptr_eq(&target.segments["documents"], &original));
            let key = RelationalKey(vec![RelationalValue::Text("owner".into())]);
            assert_eq!(
                target.index_lookup("documents", &name, &key).unwrap().len(),
                1
            );
            let snapshot = target.clone();
            drop(target);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(Arc::ptr_eq(&target.segments["documents"], &original));
            drop(target);
            drop(context);
        }
        drop(original);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_index_rebuild_cancellation_preserves_segment_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkProbe};
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let source = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "owner", "body")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let mut target = source.clone();
    let original = target.segments["documents"].clone();
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe
        .cancel_after
        .store(10, std::sync::atomic::Ordering::SeqCst);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let context = CheckpointDecodeContext {
        work: probe.context(scheduler.clone()),
        memory: Default::default(),
    };
    assert!(matches!(
        rebuild_indexes_with_allocations(&mut target, "documents", &context),
        Err(RelationalError::Admission(_))
    ));
    probe.assert_released(&scheduler);
    assert!(Arc::ptr_eq(&target.segments["documents"], &original));
    let retry = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    rebuild_indexes_with_allocations(&mut target, "documents", &retry).unwrap();
    assert!(!Arc::ptr_eq(&target.segments["documents"], &original));
    let key = RelationalKey(vec![RelationalValue::Text("owner".into())]);
    assert_eq!(
        target
            .index_lookup("documents", &relational_unique_index_name(0), &key)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn controlled_create_table_and_insert_match_ordinary_schema_rows_and_indexes() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let mut transaction = create_upsert_table();
    transaction.writes.push(RelationalWrite::Insert {
        table: "documents".into(),
        rows: vec![upsert_row("id", "owner", "body")],
        mode: RelationalInsertMode::Error,
    });
    let previous = RelationalState::default();
    let expected = apply_transaction_inner(
        &previous,
        transaction.clone(),
        limits,
        overflow,
        TransactionApplyOptions::materialized(),
    )
    .unwrap();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let actual = apply_transaction_inner(
        &previous,
        transaction,
        limits,
        overflow,
        TransactionApplyOptions {
            checkpoint_allocations: Some(&context),
            ..TransactionApplyOptions::materialized()
        },
    )
    .unwrap();
    assert_eq!(
        actual.state.table_schema("documents"),
        expected.state.table_schema("documents")
    );
    assert_eq!(
        actual.state.rows("documents").collect::<Vec<_>>(),
        expected.state.rows("documents").collect::<Vec<_>>()
    );
    let key = RelationalKey(vec![RelationalValue::Text("owner".into())]);
    assert_eq!(
        actual
            .state
            .index_lookup("documents", &relational_unique_index_name(0), &key)
            .map(|set| set.iter().collect::<Vec<_>>()),
        expected
            .state
            .index_lookup("documents", &relational_unique_index_name(0), &key)
            .map(|set| set.iter().collect::<Vec<_>>())
    );
    assert!(previous.table_schema("documents").is_none());
}

#[test]
fn controlled_create_table_denies_wide_default_before_copy_and_snapshot_keeps_schema_inventory() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let mut transaction = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut transaction.writes[0] else {
        panic!("fixture schema");
    };
    schema.columns[2].default = Some(RelationalColumnDefault::Literal(RelationalValue::Text(
        "d".repeat(128 * 1024),
    )));
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let previous = RelationalState::default();
        let input = transaction.clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = apply_transaction_inner(
            &previous,
            input,
            limits,
            overflow,
            TransactionApplyOptions {
                checkpoint_allocations: Some(&context),
                ..TransactionApplyOptions::materialized()
            },
        );
        let copies = observer.finish();
        if succeeds {
            let actual = result.unwrap();
            assert_eq!(actual.state.row_count("documents"), 0);
            let snapshot = actual.state.clone();
            drop(actual);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            drop(context);
        }
        assert!(previous.table_schema("documents").is_none());
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_add_column_matches_ordinary_shared_overflow_and_page_boundaries() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let previous = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![
                        upsert_row("id-1", "owner-1", "old-1"),
                        upsert_row("id-2", "owner-2", "old-2"),
                    ],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![RelationalWrite::AddColumn {
            table: "documents".into(),
            column: RelationalColumnSchema {
                name: "added".into(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                    "d".repeat(128 * 1024),
                ))),
            },
        }],
    };
    let options = TransactionApplyOptions {
        index_capture_limits: Some(RelationalIndexChangeCaptureLimits::default()),
        row_capture_limits: Some(RelationalRowChangeCaptureLimits::default()),
        ..TransactionApplyOptions::materialized()
    };
    let expected =
        apply_transaction_inner(&previous, transaction.clone(), limits, overflow, options).unwrap();
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    let actual = apply_transaction_inner(
        &previous,
        transaction,
        limits,
        overflow,
        TransactionApplyOptions {
            checkpoint_allocations: Some(&context),
            ..options
        },
    )
    .unwrap();
    assert_eq!(
        actual.state.table_schema("documents"),
        expected.state.table_schema("documents")
    );
    assert_eq!(
        actual.state.rows("documents").collect::<Vec<_>>(),
        expected.state.rows("documents").collect::<Vec<_>>()
    );
    assert_eq!(actual.index_capture, expected.index_capture);
    assert_eq!(actual.row_capture, expected.row_capture);
    assert_eq!(actual.state.overflow_segment_count(), 1);
    assert_eq!(
        actual.state.segments["documents"].rows.page_count(),
        previous.segments["documents"].rows.page_count()
    );
    let first = actual.state.rows("documents").next().unwrap().1.values()[3].clone();
    assert!(actual
        .state
        .rows("documents")
        .all(|(_, row)| row.values()[3] == first));
    assert_eq!(previous.table_schema("documents").unwrap().columns.len(), 3);
    assert!(previous
        .rows("documents")
        .all(|(_, row)| row.values().len() == 3));
}

#[test]
fn controlled_add_column_denies_wide_default_without_partial_schema_or_rows() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let source = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "owner", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    let column = RelationalColumnSchema {
        name: "added".into(),
        scalar_type: RelationalScalarType::Text,
        nullable: false,
        default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
            "d".repeat(128 * 1024),
        ))),
    };
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut target = source.clone();
        let original_segment = target.segments["documents"].clone();
        let original_schema = target.schemas["documents"].clone();
        let observer = crate::test_allocator::AllocationObservation::start();
        let result = add_column_with_allocations(
            &mut target,
            "documents",
            &column,
            limits,
            overflow,
            &context,
        );
        let copies = observer.finish();
        if succeeds {
            assert_eq!(result.unwrap(), 1);
            assert_eq!(target.table_schema("documents").unwrap().columns.len(), 4);
            let snapshot = target.clone();
            drop(target);
            drop(context);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes < ceiling - 128 * 1024)
            );
            drop(snapshot);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
            assert!(Arc::ptr_eq(
                &target.segments["documents"],
                &original_segment
            ));
            assert!(Arc::ptr_eq(&target.schemas["documents"], &original_schema));
            drop(target);
            drop(context);
        }
        drop(original_segment);
        drop(original_schema);
        assert_eq!(source.table_schema("documents").unwrap().columns.len(), 3);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling), Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn controlled_materialized_schema_recovery_matches_all_capture_combinations_and_foreign_keys() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let mut transaction = create_content_tables();
    transaction.writes.extend([
        RelationalWrite::Insert {
            table: "content_anchors".into(),
            rows: vec![anchor_row("anchor", "document")],
            mode: RelationalInsertMode::Error,
        },
        RelationalWrite::Insert {
            table: "content_documents".into(),
            rows: vec![document_row("document")],
            mode: RelationalInsertMode::Error,
        },
        RelationalWrite::AddColumn {
            table: "content_anchors".into(),
            column: RelationalColumnSchema {
                name: "label".into(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: Some(RelationalColumnDefault::Literal(RelationalValue::Text(
                    "label".into(),
                ))),
            },
        },
        RelationalWrite::CreateIndex {
            table: "content_anchors".into(),
            index: RelationalIndexSchema {
                name: "label_index".into(),
                columns: vec!["label".into()],
                unique: false,
            },
        },
    ]);
    let previous = RelationalState::default();
    for index_capture_limits in [None, Some(RelationalIndexChangeCaptureLimits::default())] {
        for row_capture_limits in [None, Some(RelationalRowChangeCaptureLimits::default())] {
            let expected = apply_transaction_inner(
                &previous,
                transaction.clone(),
                limits,
                overflow,
                TransactionApplyOptions {
                    index_capture_limits,
                    row_capture_limits,
                    ..TransactionApplyOptions::materialized()
                },
            )
            .unwrap();
            let context = CheckpointDecodeContext {
                work: Default::default(),
                memory: Default::default(),
            };
            let (actual, indexes, rows) = previous
                .stage_materialized_recovery_with_allocations(
                    RelationalMaterializedRecoveryStage {
                        transaction: transaction.clone(),
                        mutation_limits: limits,
                        overflow_config: overflow,
                        index_capture_limits,
                        row_capture_limits,
                    },
                    &context,
                )
                .unwrap();
            assert_eq!(indexes, expected.index_capture);
            assert_eq!(rows, expected.row_capture);
            for table in ["content_documents", "content_anchors"] {
                assert_eq!(
                    actual.table_schema(table),
                    expected.state.table_schema(table)
                );
                assert_eq!(
                    actual.rows(table).collect::<Vec<_>>(),
                    expected.state.rows(table).collect::<Vec<_>>()
                );
            }
            for (name, postings) in &expected.state.segments["content_anchors"].indexes {
                let actual_postings = &actual.segments["content_anchors"].indexes[name];
                assert_eq!(
                    actual_postings
                        .pages
                        .iter()
                        .flat_map(|page| page.iter())
                        .map(|(key, set)| (key, set.iter().collect::<Vec<_>>()))
                        .collect::<Vec<_>>(),
                    postings
                        .pages
                        .iter()
                        .flat_map(|page| page.iter())
                        .map(|(key, set)| (key, set.iter().collect::<Vec<_>>()))
                        .collect::<Vec<_>>()
                );
            }
            let deletion = RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "content_documents".into(),
                    keys: vec![RelationalKey(vec![RelationalValue::Text(
                        "document".into(),
                    )])],
                }],
            };
            let rejected = actual.stage_materialized_recovery_with_allocations(
                RelationalMaterializedRecoveryStage {
                    transaction: deletion,
                    mutation_limits: limits,
                    overflow_config: overflow,
                    index_capture_limits,
                    row_capture_limits,
                },
                &context,
            );
            assert!(matches!(rejected, Err(RelationalError::Constraint(_))));
            assert_eq!(actual.row_count("content_documents"), 1);
        }
    }
    assert!(previous.table_schema("content_documents").is_none());
}

#[test]
fn controlled_foreign_key_validation_denies_wide_key_before_copy_and_retries() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let id = "k".repeat(128 * 1024);
    let source = RelationalState::default()
        .stage_transaction(create_content_tables(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::Insert {
                        table: "content_documents".into(),
                        rows: vec![document_row(&id)],
                        mode: RelationalInsertMode::Error,
                    },
                    RelationalWrite::Insert {
                        table: "content_anchors".into(),
                        rows: vec![anchor_row("anchor", &id)],
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            limits,
            overflow,
        )
        .unwrap();
    let full = BTreeSet::from(["content_anchors".into()]);
    let changed = BTreeMap::new();
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observation = crate::test_allocator::AllocationObservation::start();
        let result =
            validate_foreign_keys_incremental(&source, &source, &changed, &full, Some(&context));
        let copies = observation.finish();
        if succeeds {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(context);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    assert_eq!(source.row_count("content_documents"), 1);
    assert_eq!(source.row_count("content_anchors"), 1);
}

#[test]
fn controlled_legacy_authoritative_recovery_matches_ordinary_and_rejects_schema_changes() {
    use crate::background::CheckpointDecodeContext;
    let limits = RelationalMutationLimits::default();
    let overflow = RelationalOverflowConfig::default();
    let captures = RelationalIndexChangeCaptureLimits::default();
    let source = RelationalState::default()
        .stage_transaction(create_upsert_table(), limits, overflow)
        .unwrap()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::Insert {
                    table: "documents".into(),
                    rows: vec![upsert_row("id", "owner", "old")],
                    mode: RelationalInsertMode::Error,
                }],
            },
            limits,
            overflow,
        )
        .unwrap();
    for writes in [
        vec![RelationalWrite::Insert {
            table: "documents".into(),
            rows: vec![upsert_row("id", "owner", "new")],
            mode: RelationalInsertMode::Replace,
        }],
        vec![RelationalWrite::DeleteByPrimaryKey {
            table: "documents".into(),
            keys: vec![RelationalKey(vec![RelationalValue::Text("id".into())])],
        }],
    ] {
        let transaction = RelationalTransaction { writes };
        let expected = source
            .stage_transaction_for_authoritative_recovery(
                transaction.clone(),
                limits,
                overflow,
                captures,
            )
            .unwrap();
        let context = CheckpointDecodeContext {
            work: Default::default(),
            memory: Default::default(),
        };
        let actual = source
            .stage_legacy_transaction_for_authoritative_recovery_with_allocations(
                transaction,
                limits,
                overflow,
                captures,
                Some(&context),
            )
            .unwrap();
        assert_eq!(actual.1, expected.1);
        assert_eq!(
            actual.0.rows("documents").collect::<Vec<_>>(),
            expected.0.rows("documents").collect::<Vec<_>>()
        );
        assert!(!actual.0.materialized_index_postings_resident);
        assert!(actual.0.segments["documents"].indexes.is_empty());
        assert_eq!(source.row_count("documents"), 1);
    }
    let context = CheckpointDecodeContext {
        work: Default::default(),
        memory: Default::default(),
    };
    assert!(matches!(
        source.stage_legacy_transaction_for_authoritative_recovery_with_allocations(
            create_content_tables(),
            limits,
            overflow,
            captures,
            Some(&context)
        ),
        Err(RelationalError::Admission(_))
    ));
    assert!(source.table_schema("content_documents").is_none());
}

#[test]
fn controlled_conflict_probe_admits_selected_unique_definition_before_wide_copy() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let mut transaction = create_upsert_table();
    let RelationalWrite::CreateTable(schema) = &mut transaction.writes[0] else {
        panic!("fixture schema");
    };
    schema.unique_constraints.clear();
    schema.indexes.push(RelationalIndexSchema {
        name: "i".repeat(128 * 1024),
        columns: vec!["owner".into()],
        unique: true,
    });
    let state = RelationalState::default()
        .stage_transaction(
            transaction,
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let columns = vec!["owner".into()];
    let key = RelationalKey(vec![RelationalValue::Text("owner".into())]);
    let changed = BTreeSet::new();
    for (ceiling, succeeds) in [(16 * 1024, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let context = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = conflict_primary_key(
            &state,
            "documents",
            state.table_schema("documents").unwrap(),
            &columns,
            &key,
            &changed,
            RelationalConflictProbeContext {
                constraint_index: None,
                allocations: Some(&context),
            },
        );
        let copies = observation.finish();
        if succeeds {
            assert_eq!(result.unwrap(), None);
        } else {
            assert!(matches!(result, Err(RelationalError::Admission(_))));
            assert_eq!(copies, 0);
        }
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(context);
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
