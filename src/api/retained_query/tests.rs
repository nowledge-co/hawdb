// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{RuntimeMemorySnapshot, RuntimeResourceBudget};

#[path = "arrow/tests.rs"]
mod arrow;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn governor_with_handles(handles: usize) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            retained_result_handle_limit: nz(handles),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(4), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}
fn fixture() -> Database {
    fixture_with_config(crate::DatabaseConfig::default())
}
fn fixture_with_config(config: crate::DatabaseConfig) -> Database {
    let mut db = Database::new_with_config(config);
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
        .unwrap();
    for n in 0..9 {
        db.query(&format!("CREATE (:Item {{score: {n}}})")).unwrap();
    }
    db
}
const QUERY: &str = "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score, id(n) AS identity, n.score AS again";
fn params() -> BTreeMap<String, Value> {
    BTreeMap::from([("min".into(), Value::Int(2))])
}
fn options() -> RetainedQueryOptions {
    RetainedQueryOptions {
        batch_rows: nz(3),
        ..RetainedQueryOptions::default()
    }
}
fn scores(batch: &RetainedQueryBatch) -> Vec<i64> {
    let RetainedColumnValues::Int64(values) = batch.column(0).unwrap() else {
        panic!("integer column");
    };
    batch
        .selected_rows()
        .iter()
        .map(|row| values[*row as usize])
        .collect()
}

#[test]
fn adapter_delivery_failure_is_sticky_and_drops_source_without_revoking_views() {
    let db = fixture();
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    cursor.abort_delivery();
    assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
    assert_eq!(batch.status(), RetainedQueryStatus::Failed);
    assert_eq!(scores(&batch), vec![2]);
    assert_eq!(cursor.profile().source_pinned_rows, 0);
    for _ in 0..2 {
        assert!(matches!(
            cursor.next_batch(),
            Err(RetainedQueryError::AdapterDelivery)
        ));
    }
    cursor.close();
    cursor.abort_delivery();
    assert_eq!(batch.status(), RetainedQueryStatus::Failed);
}

#[test]
fn two_slots_apply_before_source_work_and_last_view_release_allows_retry() {
    let db = fixture();
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    assert_eq!(cursor.profile().visited_rows, 0);
    let first = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&first), vec![2]);
    assert_eq!(
        first.column_provenance(0).unwrap(),
        first.column_provenance(2).unwrap()
    );
    let view = first.try_retain(0).unwrap();
    let first_ptr = match first.column(0).unwrap() {
        RetainedColumnValues::Int64(v) => v.as_ptr(),
        _ => unreachable!(),
    };
    let view_ptr = match view.column(0).unwrap() {
        RetainedColumnValues::Int64(v) => v.as_ptr(),
        _ => unreachable!(),
    };
    assert_eq!(first_ptr, view_ptr);
    let second = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&second), vec![3, 4, 5]);
    let before = cursor.profile();
    assert!(cursor.next_batch().unwrap_err().is_retryable());
    assert_eq!(cursor.profile().visited_rows, before.visited_rows);
    assert_eq!(cursor.outstanding_batches(), 2);
    drop(first);
    assert!(cursor.next_batch().unwrap_err().is_retryable());
    drop(view);
    let third = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&third), vec![6, 7, 8]);
    drop((second, third));
    assert!(cursor.next_batch().unwrap().is_none());
    assert_eq!(cursor.status(), RetainedQueryStatus::Completed);
    assert_eq!(cursor.profile().visited_rows, 9);
    assert_eq!(cursor.profile().emitted_rows, 7);
    assert_eq!(cursor.profile().handoff_payload_copy_bytes, 0);
}

#[test]
fn offset_and_limit_preserve_physical_values_and_selection_range() {
    let db = fixture();
    let query = format!("{QUERY} SKIP 2 LIMIT 2");
    let mut cursor = db
        .query_with_params_retained(
            &query,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&batch), vec![4, 5]);
    assert_eq!(batch.physical_rows(), 9);
    assert_eq!(batch.selected_rows(), &[4, 5]);
    assert_eq!(batch.selection_provenance().byte_offset, 8);
    assert_eq!(batch.selection_provenance().byte_length, 8);
    assert!(batch.column_provenance(0).unwrap().retained_capacity_bytes >= 72);
    assert_eq!(batch.status(), RetainedQueryStatus::Open);
    assert!(cursor.next_batch().unwrap().is_none());
    assert_eq!(batch.status(), RetainedQueryStatus::Completed);
}

#[test]
fn snapshot_isolation_and_cursor_database_close_do_not_revoke_batches() {
    let mut db = fixture();
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let first = cursor.next_batch().unwrap().unwrap();
    db.query("MATCH (n:Item) SET n.score = 99").unwrap();
    let second = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&second), vec![3, 4, 5]);
    drop(db);
    cursor.close();
    assert!(matches!(
        cursor.next_batch(),
        Err(RetainedQueryError::Closed)
    ));
    assert_eq!(scores(&first), vec![2]);
    assert_eq!(first.status(), RetainedQueryStatus::Closed);
    let retained = first.try_retain(0).unwrap();
    assert_eq!(
        retained.column_provenance(0).unwrap(),
        first.column_provenance(0).unwrap()
    );
    drop((cursor, first));
    assert_eq!(scores(&retained), vec![2]);
}

#[test]
fn cumulative_budget_failure_remains_terminal_and_marks_earlier_batches_failed() {
    let db = fixture();
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                max_result_rows: Some(2),
                ..options()
            },
        )
        .unwrap();
    let first = cursor.next_batch().unwrap().unwrap();
    assert_eq!(first.status(), RetainedQueryStatus::Open);
    let error = cursor.next_batch().unwrap_err();
    assert!(matches!(error, RetainedQueryError::ResultBudget { .. }));
    // Failed delivery still performed source construction and selection work.
    // Only the first batch was emitted; its views remain readable but provisional.
    let profile = cursor.profile();
    assert_eq!(profile.visited_rows, 6);
    assert_eq!(profile.source_constructed_bytes, 6 * 16);
    assert_eq!(profile.predicate_selected_rows, 4);
    assert_eq!(profile.selection_bytes_generated, 4 * 4);
    assert_eq!(profile.emitted_rows, 1);
    assert_eq!(profile.output_payload_bytes, 3 * 8);
    assert_eq!(profile.peak_outstanding_batches, 2);
    assert_eq!(cursor.next_batch().unwrap_err(), error);
    assert_eq!(cursor.profile(), profile);
    assert_eq!(first.status(), RetainedQueryStatus::Failed);
    assert_eq!(scores(&first), vec![2]);
    assert_eq!(cursor.outstanding_batches(), 1);
}

#[test]
fn incompatible_requests_refuse_before_creating_owners() {
    let db = fixture();
    for query in [
        "MATCH (n:Item) RETURN n",
        "MATCH (n:Item) RETURN count(n)",
        "CREATE (:Item {score: 42})",
    ] {
        assert!(matches!(
            db.query_with_params_retained(query, &BTreeMap::new(), options()),
            Err(RetainedQueryError::UnsupportedPlan)
        ));
    }
    assert!(matches!(
        db.query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                require_source_reuse: true,
                ..options()
            }
        ),
        Err(RetainedQueryError::CopyRequired)
    ));
    assert!(db.retained_result_snapshot().is_none());
}

#[test]
fn different_cursors_share_the_configured_handle_limit_and_release_every_charge() {
    let mut db = fixture();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            retained_result_handle_limit: nz(3),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(4), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    db.set_runtime_governor(governor.clone());
    let mut first = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let mut second = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = first.next_batch().unwrap().unwrap();
    assert_eq!(governor.retained_result_snapshot().view_handles, 3);
    assert!(second.next_batch().unwrap_err().is_retryable());
    assert_eq!(second.profile().visited_rows, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    first.close();
    drop(first);
    assert_eq!(governor.retained_result_snapshot().view_handles, 3);
    drop(batch);
    let retry = second.next_batch().unwrap().unwrap();
    assert_eq!(scores(&retry), vec![2]);
    drop((retry, second));
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn bytes_too_small_fail_before_visiting_a_source_row() {
    let db = fixture();
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_bytes: nz(1),
                ..options()
            },
        )
        .unwrap();
    assert_eq!(
        cursor.next_batch().unwrap_err(),
        RetainedQueryError::WorkingUnitTooLarge
    );
    assert_eq!(cursor.profile().visited_rows, 0);
    assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
}

#[test]
fn one_shared_handle_cannot_fit_a_cursor_and_its_first_batch() {
    let mut db = fixture();
    let governor = governor_with_handles(1);
    db.set_runtime_governor(governor.clone());
    assert_eq!(
        db.query_with_params_retained(QUERY, &params(), options())
            .unwrap_err(),
        RetainedQueryError::WorkingUnitTooLarge
    );
    let snapshot = governor.retained_result_snapshot();
    assert_eq!(snapshot.view_handles, 0);
    assert_eq!(snapshot.buffer_owners, 0);
    assert_eq!(snapshot.retained_bytes, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert!(db.retained_result_snapshot().is_none());
    // No owner was published, so correcting this invalid configuration works.
    db.set_runtime_governor(governor_with_handles(2));
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&batch), vec![2]);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn replacing_the_ordinary_governor_cannot_multiply_a_bound_retained_allowance() {
    let mut db = fixture();
    let original = governor_with_handles(3);
    db.set_runtime_governor(original.clone());
    let mut first = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = first.next_batch().unwrap().unwrap();
    let replacement = governor_with_handles(3);
    db.set_runtime_governor(replacement.clone());
    let mut second = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    assert_eq!(original.retained_result_snapshot().view_handles, 3);
    assert_eq!(replacement.retained_result_snapshot().view_handles, 0);
    assert!(second.next_batch().unwrap_err().is_retryable());
    assert_eq!(second.profile().visited_rows, 0);
    drop((first, second, batch));
    assert_eq!(original.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(replacement.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn larger_cursor_options_cannot_bypass_database_row_or_payload_limits() {
    let db = fixture_with_config(crate::DatabaseConfig {
        execution_memory: hawdb_executor::ExecutionMemoryConfig {
            batch_rows: nz(2),
            ..hawdb_executor::ExecutionMemoryConfig::default()
        },
        max_read_result_rows: Some(2),
        ..crate::DatabaseConfig::default()
    });
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(100_000),
                max_result_rows: Some(100_000),
                ..RetainedQueryOptions::default()
            },
        )
        .unwrap();
    let empty = cursor.next_batch().unwrap().unwrap();
    assert_eq!(empty.physical_rows(), 2);
    assert_eq!(cursor.profile().visited_rows, 2);
    drop(empty);
    let first = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&first), vec![2, 3]);
    assert!(matches!(
        cursor.next_batch().unwrap_err(),
        RetainedQueryError::ResultBudget { rows: 4, .. }
    ));

    let db = fixture_with_config(crate::DatabaseConfig {
        max_read_result_payload_bytes: Some(24),
        ..crate::DatabaseConfig::default()
    });
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                max_result_payload_bytes: Some(1 << 30),
                ..options()
            },
        )
        .unwrap();
    let first = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&first), vec![2]);
    assert!(matches!(
        cursor.next_batch().unwrap_err(),
        RetainedQueryError::ResultBudget {
            payload_bytes: 96,
            ..
        }
    ));
}

#[test]
fn more_slots_still_obey_shared_handles_and_never_prefetch() {
    let mut db = fixture();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            retained_result_handle_limit: nz(5),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(4), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(1),
                outstanding_batches: nz(4),
                ..RetainedQueryOptions::default()
            },
        )
        .unwrap();
    let mut batches = (0..4)
        .map(|_| cursor.next_batch().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(cursor.profile().visited_rows, 4);
    assert_eq!(cursor.outstanding_batches(), 4);
    assert_eq!(governor.retained_result_snapshot().view_handles, 5);
    assert!(batches[2].try_retain(0).unwrap_err().is_retryable());
    assert!(cursor.next_batch().unwrap_err().is_retryable());
    assert_eq!(cursor.profile().visited_rows, 4);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    drop(batches.pop());
    let retry = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&retry), vec![4]);
    assert_eq!(cursor.profile().visited_rows, 5);
    drop((batches, retry, cursor));
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn sparse_labels_bound_source_work_and_empty_batch_is_not_eof() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
        .unwrap();
    for _ in 0..7 {
        db.query("CREATE (:Other {body: 'not-read'})").unwrap();
    }
    db.query("CREATE (:Item {score: 3})").unwrap();
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    for inspected in [3, 6] {
        let batch = cursor.next_batch().unwrap().unwrap();
        assert_eq!(batch.physical_rows(), 0);
        assert!(batch.selected_rows().is_empty());
        assert_eq!(cursor.profile().visited_rows, inspected);
        assert_eq!(batch.status(), RetainedQueryStatus::Open);
        drop(batch);
    }
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(scores(&batch), vec![3]);
    assert_eq!(cursor.profile().visited_rows, 8);
    assert!(cursor.next_batch().unwrap().is_none());
    assert_eq!(batch.status(), RetainedQueryStatus::Completed);
}

#[test]
fn cancellation_is_terminal_without_revoking_a_previous_batch_or_source_pin() {
    let db = fixture();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(
        hawdb_core::RuntimeCancellationToken::new(),
    );
    let pins = Arc::clone(&db.runtime.get().unwrap().reader_pins);
    let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
    assert_eq!(pins.lock().unwrap().active_views.len(), 1);
    let mut cursor = snapshot
        .into_retained_query(QUERY, &params(), options())
        .unwrap();
    // Node COW ownership is sufficient: the cursor releases the broader read
    // transaction/file-retirement pin as soon as it detaches its heap source.
    assert_eq!(pins.lock().unwrap().active_views.len(), 0);
    assert_eq!(cursor.profile().source_pinned_rows, 9);
    assert!(cursor.profile().source_pinned_pages > 0);
    let batch = cursor.next_batch().unwrap().unwrap();
    let before = cursor.profile().visited_rows;
    context.cancellation().cancel();
    let error = cursor.next_batch().unwrap_err();
    assert_eq!(
        error,
        RetainedQueryError::Stopped(RuntimeCancellationReason::Cancelled)
    );
    assert_eq!(cursor.next_batch().unwrap_err(), error);
    assert_eq!(cursor.profile().visited_rows, before);
    assert_eq!(pins.lock().unwrap().active_views.len(), 0);
    assert_eq!(cursor.profile().source_pinned_rows, 0);
    assert_eq!(cursor.profile().source_pinned_pages, 0);
    assert_eq!(cursor.profile().source_directory_capacity_bytes, 0);
    assert_eq!(scores(&batch), vec![2]);
    assert_eq!(batch.status(), RetainedQueryStatus::Failed);
}

#[test]
fn heap_source_continues_after_database_close_without_retaining_its_read_pin() {
    let db = fixture();
    let pins = Arc::clone(&db.runtime.get().unwrap().reader_pins);
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let profile = cursor.profile();
    assert_eq!(profile.visited_rows, 0);
    assert_eq!(profile.source_snapshot_rows, 9);
    assert_eq!(profile.source_pinned_rows, 9);
    assert!(profile.source_directory_capacity_bytes > 0);
    assert!(pins.lock().unwrap().active_views.is_empty());
    let governor = cursor.governor.clone();
    drop(db);
    let mut values = Vec::new();
    while let Some(batch) = cursor.next_batch().unwrap() {
        values.extend(scores(&batch));
    }
    assert_eq!(values, vec![2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(cursor.profile().source_snapshot_rows, 9);
    assert_eq!(cursor.profile().source_pinned_rows, 0);
    assert_eq!(cursor.profile().source_directory_capacity_bytes, 0);
    assert!(pins.lock().unwrap().active_views.is_empty());
    drop(cursor);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn empty_typed_source_has_a_schema_and_completes_on_demand() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
        .unwrap();
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    assert_eq!(cursor.schema()[0].data_type, RetainedColumnType::Int64);
    assert!(cursor.schema()[0].nullable);
    assert_eq!(cursor.profile().visited_rows, 0);
    assert!(cursor.next_batch().unwrap().is_none());
    assert_eq!(cursor.status(), RetainedQueryStatus::Completed);
}

#[test]
fn an_impossible_batch_is_terminal_even_while_another_cursor_retains_data() {
    let db = fixture();
    let mut other = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = other.next_batch().unwrap().unwrap();
    let mut impossible = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_bytes: nz(1),
                ..options()
            },
        )
        .unwrap();
    assert_eq!(
        impossible.next_batch().unwrap_err(),
        RetainedQueryError::WorkingUnitTooLarge
    );
    assert_eq!(impossible.profile().visited_rows, 0);
    assert_eq!(scores(&batch), vec![2]);
}

#[test]
fn nullable_float_schema_and_ieee_bits_do_not_depend_on_the_first_row() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE FLOAT")
        .unwrap();
    let bits = [
        None,
        Some((-0.0f64).to_bits()),
        Some(0x7ff8_1234_5678_9abc),
        Some(2.0f64.to_bits()),
    ];
    for bits in bits {
        db.query_with_params(
            "CREATE (:Item {score: $score})",
            &BTreeMap::from([(
                "score".into(),
                bits.map_or(Value::Null, |bits| Value::Float(f64::from_bits(bits))),
            )]),
        )
        .unwrap();
    }
    let parameters = BTreeMap::from([("min".into(), Value::Float(0.0))]);
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &parameters,
            RetainedQueryOptions {
                batch_rows: nz(4),
                ..options()
            },
        )
        .unwrap();
    assert_eq!(cursor.schema()[0].data_type, RetainedColumnType::Float64);
    assert!(cursor.schema()[0].nullable);
    let batch = cursor.next_batch().unwrap().unwrap();
    let RetainedColumnValues::Float64(values) = batch.column(0).unwrap() else {
        panic!("float column");
    };
    assert_eq!(values[1].to_bits(), (-0.0f64).to_bits());
    assert_eq!(values[2].to_bits(), 0x7ff8_1234_5678_9abc);
    assert!(!batch.validity(0).unwrap().is_valid(0));
    assert!(batch.validity_provenance(0).unwrap().is_some());
    assert_eq!(batch.selected_rows(), &[2, 3]);
    assert!(cursor.next_batch().unwrap().is_none());
}

#[test]
fn snapshot_created_before_runtime_configuration_uses_the_shared_host_owner() {
    let mut db = fixture();
    let snapshot = db.begin_read_transaction().unwrap();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(nz(4), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    db.set_runtime_governor(governor.clone());
    let cursor = snapshot
        .into_retained_query(QUERY, &params(), options())
        .unwrap();
    assert!(governor.retained_result_snapshot().retained_bytes > 0);
    assert_eq!(
        db.retained_result_snapshot(),
        Some(governor.retained_result_snapshot())
    );
    drop(cursor);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn adapter_capacity_is_admitted_before_source_work_and_stays_with_the_view() {
    let db = fixture();
    let mut plain = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let mut adapter = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let before = db.retained_result_snapshot().unwrap().retained_bytes;
    let first = adapter.next_batch_with_metadata(4096).unwrap().unwrap();
    let with_adapter = db.retained_result_snapshot().unwrap().retained_bytes - before;
    let before = db.retained_result_snapshot().unwrap().retained_bytes;
    let baseline = plain.next_batch().unwrap().unwrap();
    let without_adapter = db.retained_result_snapshot().unwrap().retained_bytes - before;
    assert_eq!(with_adapter - without_adapter, 4096);
    assert_eq!(scores(&first), scores(&baseline));
    adapter.close();
    plain.close();
    drop((adapter, plain, baseline));
    assert!(db.retained_result_snapshot().unwrap().retained_bytes >= 4096);
    drop(first);
    assert_eq!(db.retained_result_snapshot().unwrap().retained_bytes, 0);

    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    assert_eq!(
        cursor.next_batch_with_metadata(usize::MAX).unwrap_err(),
        RetainedQueryError::SizeOverflow
    );
    assert_eq!(cursor.profile().visited_rows, 0);
    assert_eq!(cursor.profile().source_pinned_rows, 0);
    assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
}

#[test]
fn oversized_adapter_cursor_capacity_fails_without_retaining_control_or_source() {
    let db = fixture();
    assert_eq!(
        db.query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                adapter_metadata_bytes: usize::MAX,
                ..options()
            },
        )
        .unwrap_err(),
        RetainedQueryError::SizeOverflow
    );
    assert_eq!(db.retained_result_snapshot().unwrap().retained_bytes, 0);
}

#[test]
fn adapter_minimum_handles_refuse_before_binding_and_allow_host_correction() {
    let mut db = fixture();
    db.set_runtime_governor(governor_with_handles(2));
    let options = RetainedQueryOptions {
        minimum_shared_handles: nz(3),
        ..options()
    };
    assert_eq!(
        db.query_with_params_retained(QUERY, &params(), options)
            .unwrap_err(),
        RetainedQueryError::WorkingUnitTooLarge
    );
    assert!(db.retained_result_snapshot().is_none());
    db.set_runtime_governor(governor_with_handles(3));
    let cursor = db
        .query_with_params_retained(QUERY, &params(), options)
        .unwrap();
    assert_eq!(cursor.profile().visited_rows, 0);
}
