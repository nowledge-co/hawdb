// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb::{DatabaseConfig, HawDBEmbedded, Value};
use std::collections::BTreeMap;

const QUERY: &str = "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score, id(n) AS identity, n.score AS again";

fn size<T>() -> u32 {
    std::mem::size_of::<T>() as u32
}
fn fixture(config: DatabaseConfig) -> *mut HawdbDatabase {
    let mut embedded = HawDBEmbedded::open_in_memory_with_config(config);
    embedded.query_admitted("CREATE NODE TABLE Item").unwrap();
    embedded
        .query_admitted("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
        .unwrap();
    for score in 0..9 {
        embedded
            .query_admitted(&format!("CREATE (:Item {{score: {score}}})"))
            .unwrap();
    }
    Box::into_raw(Box::new(HawdbDatabase {
        inner: Mutex::new(embedded),
    }))
}
fn request(query: &str, params: &str) -> HawdbRetainedQueryV1 {
    HawdbRetainedQueryV1 {
        abi_version: HAWDB_RETAINED_ABI_V1,
        struct_size: size::<HawdbRetainedQueryV1>(),
        cypher: query.as_ptr().cast(),
        cypher_len: query.len() as u64,
        params_json: params.as_ptr().cast(),
        params_len: params.len() as u64,
        batch_rows: 3,
        outstanding_batches: 0,
        batch_bytes: 0,
        flags: 0,
        reserved: 0,
    }
}
fn cursor(db: *mut HawdbDatabase, query: &str, params: &str) -> HawdbRetainedCursorV1 {
    let request = request(query, params);
    let mut out = HawdbRetainedCursorV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut out,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn batch(cursor: HawdbRetainedCursorV1) -> HawdbRetainedBatchV1 {
    let mut out = HawdbRetainedBatchV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn column(batch: HawdbRetainedBatchV1, index: u64) -> HawdbRetainedColumnV1 {
    let mut out = HawdbRetainedColumnV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_column(
                batch.owner_namespace,
                batch.owner_id,
                index,
                &mut out,
                size::<HawdbRetainedColumnV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn state(namespace: u64, id: u64) -> HawdbRetainedStateV1 {
    let mut out = HawdbRetainedStateV1::default();
    assert_eq!(
        unsafe { hawdb_retained_state(namespace, id, &mut out, size::<HawdbRetainedStateV1>()) },
        HAWDB_RETAINED_OK
    );
    out
}
fn release(namespace: u64, id: u64) {
    assert_eq!(hawdb_retained_release(namespace, id), HAWDB_RETAINED_OK);
}
fn selected_scores(column: HawdbRetainedColumnV1) -> Vec<i64> {
    assert_eq!(column.schema.data_type, HAWDB_RETAINED_INT64);
    let values = unsafe {
        std::slice::from_raw_parts(
            column.values.data.cast::<i64>(),
            column.physical_rows as usize,
        )
    };
    let selection = unsafe {
        std::slice::from_raw_parts(
            column.selection.data.cast::<u32>(),
            column.selected_rows as usize,
        )
    };
    selection.iter().map(|row| values[*row as usize]).collect()
}

#[test]
fn independent_column_lifetime_identity_and_exact_final_release() {
    let db = fixture(DatabaseConfig::default());
    let governor = unsafe { (&*db).inner.lock().unwrap().runtime_governor().clone() };
    let cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let first = batch(cursor);
    let scores = column(first, 0);
    let again = column(first, 2);
    assert_eq!(scores.values.data, again.values.data);
    assert_eq!(scores.values.allocation_id, again.values.allocation_id);
    assert_eq!(
        scores.values.allocation_namespace,
        again.values.allocation_namespace
    );
    assert_eq!(scores.values.generation, again.values.generation);
    assert_eq!(
        scores.values.retained_capacity_bytes,
        again.values.retained_capacity_bytes
    );
    assert_eq!(selected_scores(scores), vec![2]);
    let mut retained = HawdbRetainedColumnV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_column_retain(
                scores.owner_namespace,
                scores.owner_id,
                &mut retained,
                size::<HawdbRetainedColumnV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(retained.values.data, scores.values.data);
    assert_eq!(retained.selection.data, scores.selection.data);
    assert_eq!(retained.values.allocation_id, scores.values.allocation_id);
    release(first.owner_namespace, first.owner_id);
    release(scores.owner_namespace, scores.owner_id);
    release(again.owner_namespace, again.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
    assert_eq!(selected_scores(retained), vec![2]);
    assert_eq!(state(retained.owner_namespace, retained.owner_id).status, 3);
    assert!(governor.retained_result_snapshot().retained_bytes > 0);
    release(retained.owner_namespace, retained.owner_id);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(
        hawdb_retained_release(retained.owner_namespace, retained.owner_id),
        HAWDB_RETAINED_INVALID_HANDLE
    );
    let mut out = retained;
    assert_eq!(
        unsafe {
            hawdb_retained_column_retain(
                retained.owner_namespace,
                retained.owner_id,
                &mut out,
                size::<HawdbRetainedColumnV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_HANDLE
    );
    assert_eq!(out.owner_id, 0);
    assert!(out.values.data.is_null());
}

#[test]
fn slots_backpressure_before_advance_and_retry_after_last_column_release() {
    let db = fixture(DatabaseConfig::default());
    let cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let first = batch(cursor);
    let view = column(first, 0);
    let second = batch(cursor);
    let before = state(cursor.owner_namespace, cursor.owner_id);
    let mut out = first;
    assert_eq!(
        unsafe {
            hawdb_retained_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_BACKPRESSURE
    );
    assert_eq!(out.owner_id, 0);
    assert!(out.selection.data.is_null());
    assert_eq!(
        state(cursor.owner_namespace, cursor.owner_id).visited_rows,
        before.visited_rows
    );
    release(first.owner_namespace, first.owner_id);
    assert_eq!(
        unsafe {
            hawdb_retained_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_BACKPRESSURE
    );
    release(view.owner_namespace, view.owner_id);
    let third = batch(cursor);
    let values = column(third, 0);
    assert_eq!(selected_scores(values), vec![6, 7, 8]);
    release(values.owner_namespace, values.owner_id);
    release(second.owner_namespace, second.owner_id);
    release(third.owner_namespace, third.owner_id);
    assert_eq!(
        unsafe {
            hawdb_retained_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_EOF
    );
    assert_eq!(out.owner_id, 0);
    assert_eq!(state(cursor.owner_namespace, cursor.owner_id).status, 1);
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
}

#[test]
fn late_budget_failure_is_repeated_and_visible_to_earlier_readable_columns() {
    let db = fixture(DatabaseConfig {
        max_read_result_rows: Some(2),
        ..DatabaseConfig::default()
    });
    let cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let first = batch(cursor);
    let view = column(first, 0);
    let mut out = HawdbRetainedBatchV1::default();
    for _ in 0..2 {
        assert_eq!(
            unsafe {
                hawdb_retained_next(
                    cursor.owner_namespace,
                    cursor.owner_id,
                    &mut out,
                    size::<HawdbRetainedBatchV1>(),
                )
            },
            HAWDB_RETAINED_RESULT_BUDGET
        );
        assert_eq!(out.owner_id, 0);
    }
    assert_eq!(state(view.owner_namespace, view.owner_id).status, 2);
    assert_eq!(selected_scores(view), vec![2]);
    assert_eq!(
        state(cursor.owner_namespace, cursor.owner_id).source_pinned_rows,
        0
    );
    release(first.owner_namespace, first.owner_id);
    release(view.owner_namespace, view.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
}

#[test]
fn descriptor_versions_sizes_flags_and_handles_refuse_with_empty_outputs() {
    let db = fixture(DatabaseConfig::default());
    let mut request = request(QUERY, r#"{"min":2}"#);
    let mut out = HawdbRetainedCursorV1 {
        owner_id: 77,
        ..HawdbRetainedCursorV1::default()
    };
    request.abi_version = 99;
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut out,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_ARGUMENT
    );
    assert_eq!(out.owner_id, 0);
    request.abi_version = HAWDB_RETAINED_ABI_V1;
    request.flags = HAWDB_RETAINED_REQUEST_WRITABLE;
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut out,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_COPY_REQUIRED
    );
    request.flags = 0;
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>() - 1,
                &mut out,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_ARGUMENT
    );
    let mut short = [0xa5u8; 24];
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                short.as_mut_ptr().cast(),
                16,
            )
        },
        HAWDB_RETAINED_INVALID_ARGUMENT
    );
    assert_eq!(&short[..16], &[0u8; 16]);
    assert_eq!(&short[16..], &[0xa5u8; 8]);
    let cursor = cursor(db, QUERY, r#"{"min":2}"#);
    assert_eq!(
        hawdb_retained_release(cursor.owner_namespace ^ 1, cursor.owner_id),
        HAWDB_RETAINED_INVALID_HANDLE
    );
    assert_eq!(
        hawdb_retained_release(cursor.owner_namespace, u64::MAX),
        HAWDB_RETAINED_INVALID_HANDLE
    );
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
}

#[test]
fn panic_is_contained_and_output_is_already_empty() {
    let mut out = HawdbRetainedBatchV1 {
        owner_id: 88,
        ..HawdbRetainedBatchV1::default()
    };
    assert_eq!(
        unsafe {
            output(&mut out, size::<HawdbRetainedBatchV1>(), || {
                panic!("injected adapter panic")
            })
        },
        HAWDB_RETAINED_PANIC
    );
    assert_eq!(out.owner_id, 0);
    assert!(out.selection.data.is_null());
}

#[test]
fn selection_limit_shares_the_original_range_and_empty_eof_keeps_fixed_schema() {
    let db = fixture(DatabaseConfig::default());
    let query = format!("{QUERY} SKIP 2 LIMIT 2");
    let mut request = request(&query, r#"{"min":2}"#);
    request.batch_rows = 9;
    let mut cursor = HawdbRetainedCursorV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut cursor,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    let first = batch(cursor);
    let view = column(first, 0);
    assert_eq!(selected_scores(view), vec![4, 5]);
    assert_eq!(view.physical_rows, 9);
    assert_eq!(view.selection.byte_offset, 8);
    assert_eq!(view.selection.byte_length, 8);
    assert!(view.values.retained_capacity_bytes >= 72);
    let mut retained = HawdbRetainedBatchV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_batch_retain(
                first.owner_namespace,
                first.owner_id,
                &mut retained,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(retained.selection.data, first.selection.data);
    assert_eq!(
        retained.selection.allocation_id,
        first.selection.allocation_id
    );
    release(retained.owner_namespace, retained.owner_id);
    release(first.owner_namespace, first.owner_id);
    release(view.owner_namespace, view.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);

    let empty = self::cursor(db, QUERY, r#"{"min":100}"#);
    let mut schema = HawdbRetainedSchemaV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_schema(
                empty.owner_namespace,
                empty.owner_id,
                0,
                &mut schema,
                size::<HawdbRetainedSchemaV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(schema.data_type, HAWDB_RETAINED_INT64);
    let mut out = HawdbRetainedBatchV1::default();
    loop {
        let code = unsafe {
            hawdb_retained_next(
                empty.owner_namespace,
                empty.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        };
        if code == HAWDB_RETAINED_EOF {
            break;
        }
        assert_eq!(code, HAWDB_RETAINED_OK);
        assert_eq!(out.selected_rows, 0);
        assert!(out.selection.data.is_null());
        release(out.owner_namespace, out.owner_id);
    }
    assert_eq!(
        unsafe {
            hawdb_retained_schema(
                empty.owner_namespace,
                empty.owner_id,
                0,
                &mut schema,
                size::<HawdbRetainedSchemaV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(schema.data_type, HAWDB_RETAINED_INT64);
    release(empty.owner_namespace, empty.owner_id);
    unsafe { super::super::hawdb_close(db) };
}

#[test]
fn nullable_float_bits_and_node_identity_are_exposed_without_casting() {
    let mut embedded = HawDBEmbedded::open_in_memory();
    embedded.query_admitted("CREATE NODE TABLE Item").unwrap();
    embedded
        .query_admitted("CREATE PROPERTY ON NODE TABLE Item(score) TYPE FLOAT")
        .unwrap();
    let bits = [
        0x7ff8_0000_0000_0042,
        (-0.0f64).to_bits(),
        0.0f64.to_bits(),
        f64::INFINITY.to_bits(),
    ];
    for value in std::iter::once(Value::Null).chain(
        bits.into_iter()
            .map(|bits| Value::Float(f64::from_bits(bits))),
    ) {
        embedded
            .database_mut()
            .query_with_params(
                "CREATE (:Item {score: $score})",
                &BTreeMap::from([("score".into(), value)]),
            )
            .unwrap();
    }
    let db = Box::into_raw(Box::new(HawdbDatabase {
        inner: Mutex::new(embedded),
    }));
    // Input JSON accepts finite numbers; -1e300 includes all non-null fixture values.
    let mut request = request(QUERY, r#"{"min":-1e300}"#);
    request.batch_rows = 9;
    let mut cursor = HawdbRetainedCursorV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut cursor,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    let first = batch(cursor);
    let view = column(first, 0);
    assert_eq!(view.schema.data_type, HAWDB_RETAINED_FLOAT64);
    assert_eq!(view.schema.nullable, 1);
    assert_eq!(view.validity_kind, HAWDB_RETAINED_VALIDITY_U64_LSB);
    let values = unsafe {
        std::slice::from_raw_parts(view.values.data.cast::<f64>(), view.physical_rows as usize)
    };
    assert_eq!(
        values[1..]
            .iter()
            .map(|value| value.to_bits())
            .collect::<Vec<_>>(),
        bits
    );
    assert_eq!(unsafe { *view.validity.data.cast::<u64>() } & 31, 30);
    let identity = column(first, 1);
    assert_eq!(identity.schema.data_type, HAWDB_RETAINED_UINT64);
    assert_eq!(identity.schema.role, HAWDB_RETAINED_NODE_IDENTITY);
    assert_eq!(identity.schema.nullable, 0);
    assert_eq!(identity.validity_kind, HAWDB_RETAINED_VALIDITY_ALL);
    release(first.owner_namespace, first.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
    assert_eq!(values[1].to_bits(), bits[0]);
    release(view.owner_namespace, view.owner_id);
    release(identity.owner_namespace, identity.owner_id);
}

fn constrained_governor(handles: usize) -> hawdb::RuntimeGovernor {
    hawdb::RuntimeGovernor::new(
        hawdb::RuntimeGovernorConfig {
            retained_result_handle_limit: NonZeroUsize::new(handles).unwrap(),
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..hawdb::RuntimeGovernorConfig::shared_host()
        },
        hawdb::RuntimeResourceSnapshot::from_parts(
            hawdb::RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            hawdb::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb::IoConcurrencyBudget::new(2, 1),
    )
}

#[test]
fn handles_are_shared_across_cursors_and_borrowing_needs_no_additional_owner() {
    let db = fixture(DatabaseConfig::default());
    let governor = constrained_governor(5);
    unsafe {
        (&*db)
            .inner
            .lock()
            .unwrap()
            .database_mut()
            .set_runtime_governor(governor.clone());
    }
    let first_cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let second_cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let first = batch(first_cursor);
    let view = column(first, 0);
    let mut retained = HawdbRetainedColumnV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_column_retain(
                view.owner_namespace,
                view.owner_id,
                &mut retained,
                size::<HawdbRetainedColumnV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(governor.retained_result_snapshot().view_handles, 5);
    let before = governor.retained_result_snapshot();
    let mut borrowed = HawdbRetainedColumnV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_column_borrow(
                first.owner_namespace,
                first.owner_id,
                0,
                &mut borrowed,
                size::<HawdbRetainedColumnV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    assert_eq!(
        borrowed.flags,
        HAWDB_RETAINED_READ_ONLY | HAWDB_RETAINED_BORROWED
    );
    assert_eq!(selected_scores(borrowed), vec![2]);
    assert_eq!(governor.retained_result_snapshot(), before);
    let mut out = HawdbRetainedBatchV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_next(
                second_cursor.owner_namespace,
                second_cursor.owner_id,
                &mut out,
                size::<HawdbRetainedBatchV1>(),
            )
        },
        HAWDB_RETAINED_BACKPRESSURE
    );
    assert_eq!(
        state(second_cursor.owner_namespace, second_cursor.owner_id).visited_rows,
        0
    );
    release(retained.owner_namespace, retained.owner_id);
    let second = batch(second_cursor);
    assert_eq!(second.selected_rows, 1);
    release(first.owner_namespace, first.owner_id);
    release(second.owner_namespace, second.owner_id);
    release(view.owner_namespace, view.owner_id);
    release(first_cursor.owner_namespace, first_cursor.owner_id);
    release(second_cursor.owner_namespace, second_cursor.owner_id);
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    unsafe { super::super::hawdb_close(db) };
}

#[test]
fn inadequate_protocol_handle_limit_refuses_before_binding_and_can_be_corrected() {
    let db = fixture(DatabaseConfig::default());
    unsafe {
        (&*db)
            .inner
            .lock()
            .unwrap()
            .database_mut()
            .set_runtime_governor(constrained_governor(2));
    }
    let request = request(QUERY, r#"{"min":2}"#);
    let mut out = HawdbRetainedCursorV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_query(
                db,
                &request,
                size::<HawdbRetainedQueryV1>(),
                &mut out,
                size::<HawdbRetainedCursorV1>(),
            )
        },
        HAWDB_RETAINED_WORKING_UNIT_TOO_LARGE
    );
    assert_eq!(out.owner_id, 0);
    assert!(unsafe {
        (&*db)
            .inner
            .lock()
            .unwrap()
            .database()
            .retained_result_snapshot()
            .is_none()
    });
    unsafe {
        (&*db)
            .inner
            .lock()
            .unwrap()
            .database_mut()
            .set_runtime_governor(constrained_governor(3));
    }
    let cursor = cursor(db, QUERY, r#"{"min":2}"#);
    let first = batch(cursor);
    let view = column(first, 0);
    assert_eq!(selected_scores(view), vec![2]);
    release(view.owner_namespace, view.owner_id);
    release(first.owner_namespace, first.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe { super::super::hawdb_close(db) };
}
