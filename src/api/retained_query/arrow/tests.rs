// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::CStr;

#[test]
fn contiguous_export_shares_values_with_nonzero_child_offsets_and_survives_close() {
    let mut db = fixture();
    let governor = governor_with_handles(1024);
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(
            &(QUERY.to_owned() + " SKIP 1 LIMIT 2"),
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(batch.selected_rows(), &[3, 4]);
    let RetainedColumnValues::Int64(native) = batch.column(0).unwrap() else {
        panic!("int");
    };
    let original = native.as_ptr();
    let original_provenance = batch.column_provenance(0).unwrap();
    let export = batch.export_arrow(0).unwrap();
    let selected_provenance = export.column_provenance(0).unwrap();
    assert_eq!(selected_provenance.identity, original_provenance.identity);
    assert_eq!(
        selected_provenance.retained_capacity_bytes,
        original_provenance.retained_capacity_bytes
    );
    assert_eq!(
        selected_provenance.byte_offset,
        original_provenance.byte_offset + 3 * 8
    );
    assert_eq!(selected_provenance.byte_length, 2 * 8);
    assert_eq!(selected_provenance, export.column_provenance(2).unwrap());
    unsafe {
        assert_eq!(CStr::from_ptr(export.schema().format), c"+s");
        let fields = std::slice::from_raw_parts(export.schema().children, 3);
        assert_eq!(CStr::from_ptr((*fields[0]).format), c"l");
        assert_eq!(CStr::from_ptr((*fields[1]).format), c"L");
        assert_eq!((*fields[0]).flags, 2);
        assert_eq!((*fields[1]).flags, 0);
        let metadata = std::slice::from_raw_parts((*fields[1]).metadata.cast::<u8>(), 35);
        assert_eq!(i32::from_ne_bytes(metadata[..4].try_into().unwrap()), 1);
        assert_eq!(&metadata[8..18], b"hawdb:role");
        assert_eq!(&metadata[22..], b"node_identity");
        let columns = std::slice::from_raw_parts(export.array().children, 3);
        assert_eq!(export.array().length, 2);
        assert_eq!(export.array().offset, 0);
        assert_eq!((*columns[0]).offset, 3);
        assert_eq!((*columns[0]).length, 2);
        assert_eq!(*(*columns[0]).buffers.add(1), original.cast());
        assert_eq!(*(*columns[0]).buffers.add(1), *(*columns[2]).buffers.add(1));
        assert!((*(*columns[0]).buffers).is_null());
    }
    cursor.close();
    assert_eq!(export.status(), RetainedQueryStatus::Closed);
    drop(batch);
    drop(cursor);
    drop(db);
    unsafe {
        let column = *export.array().children;
        let values = *(*column).buffers.add(1);
        assert_eq!(
            std::slice::from_raw_parts(values.cast::<i64>().add((*column).offset as usize), 2),
            &[3, 4]
        );
    }
    assert!(governor.retained_result_snapshot().retained_bytes > 0);
    drop(export);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
}

#[test]
fn earlier_arrow_export_observes_late_failure_and_keeps_payload_readable() {
    let db = fixture();
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                max_result_rows: Some(1),
                ..options()
            },
        )
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    let export = batch.export_arrow(0).unwrap();
    assert_eq!(export.status(), RetainedQueryStatus::Open);
    assert!(matches!(
        cursor.next_batch(),
        Err(RetainedQueryError::ResultBudget { .. })
    ));
    assert_eq!(export.status(), RetainedQueryStatus::Failed);
    assert_eq!(cursor.profile().source_pinned_rows, 0);
    drop(batch);
    drop(cursor);
    drop(db);
    unsafe {
        let column = *export.array().children;
        assert_eq!(
            *(*(*column).buffers.add(1))
                .cast::<i64>()
                .add((*column).offset as usize),
            2
        );
    }
}

#[test]
fn moved_child_remains_valid_after_record_batch_release() {
    let mut db = fixture();
    let governor = governor_with_handles(1024);
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    let (mut schema, mut array) = batch.export_arrow(0).unwrap().into_raw();
    let mut child = unsafe {
        let source = *array.children;
        let moved = std::ptr::read(source);
        (*source).release = None;
        moved
    };
    drop(batch);
    drop(cursor);
    drop(db);
    unsafe {
        array.release.unwrap()(&mut array);
        schema.release.unwrap()(&mut schema);
        assert!(array.release.is_none() && schema.release.is_none());
        assert_eq!(
            *(*child.buffers.add(1))
                .cast::<i64>()
                .add(child.offset as usize),
            2
        );
        child.release.unwrap()(&mut child);
    }
    assert!(child.release.is_none());
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
}

#[test]
fn schema_ownership_does_not_keep_a_result_slot() {
    let mut db = fixture();
    let governor = governor_with_handles(1024);
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    let schema = batch.export_arrow_schema(0).unwrap();
    drop(batch);
    assert_eq!(cursor.outstanding_batches(), 0);
    cursor.close();
    drop(cursor);
    drop(db);
    assert_eq!(governor.retained_result_snapshot().buffer_owners, 1);
    unsafe {
        assert_eq!(CStr::from_ptr(schema.descriptor().format), c"+s");
    }
    drop(schema);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn sparse_selection_refuses_without_charging_or_copying() {
    let mut db = fixture();
    db.query("MATCH (n:Item) WHERE n.score = 4 SET n.score = 0")
        .unwrap();
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    assert_eq!(batch.selected_rows(), &[2, 3, 5, 6, 7, 8]);
    let before = db.retained_result_snapshot().unwrap();
    assert!(matches!(
        batch.export_arrow(0),
        Err(RetainedQueryError::SelectionRequiresMaterialization)
    ));
    assert_eq!(before, db.retained_result_snapshot().unwrap());
    assert_eq!(scores(&batch), vec![2, 3, 5, 6, 7, 8]);
}

#[test]
fn impossible_protocol_and_wrapper_overflow_refuse_without_partial_ownership() {
    let mut db = fixture();
    let governor = governor_with_handles(10);
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    let before = governor.retained_result_snapshot();
    assert!(matches!(
        batch.export_arrow(0),
        Err(RetainedQueryError::WorkingUnitTooLarge)
    ));
    assert_eq!(before, governor.retained_result_snapshot());
    assert!(matches!(
        batch.export_arrow_schema(usize::MAX),
        Err(RetainedQueryError::SizeOverflow)
    ));
    assert_eq!(before, governor.retained_result_snapshot());
}

#[test]
fn temporary_handle_pressure_rolls_back_and_release_allows_export_retry() {
    let mut db = fixture();
    let governor = governor_with_handles(11);
    db.set_runtime_governor(governor.clone());
    let mut cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    // Exercise failure before construction, after schema construction, and
    // after partial child-array construction; every path must roll back.
    for occupancy in [11, 6, 3] {
        let mut views = Vec::new();
        while governor.retained_result_snapshot().view_handles < occupancy {
            views.push(batch.try_retain(0).unwrap());
        }
        let before = governor.retained_result_snapshot();
        let error = batch.export_arrow(0).unwrap_err();
        assert!(error.is_retryable());
        let after = governor.retained_result_snapshot();
        assert_eq!(before.retained_bytes, after.retained_bytes);
        assert_eq!(before.view_handles, after.view_handles);
        assert_eq!(before.buffer_owners, after.buffer_owners);
        assert!(after.backpressure_events > before.backpressure_events);
        drop(views);
    }
    let export = batch.export_arrow(0).unwrap();
    assert_eq!(governor.retained_result_snapshot().view_handles, 11);
    drop(batch);
    drop(cursor);
    drop(export);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn nullable_float_bits_and_all_null_empty_selection_survive_arrow_layout() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(score) TYPE FLOAT")
        .unwrap();
    for value in [
        Value::Null,
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Float(f64::from_bits(0x7ff800000000002a)),
        Value::Float(f64::INFINITY),
    ] {
        db.query_with_params(
            "CREATE (:Item {score: $value})",
            &BTreeMap::from([("value".into(), value)]),
        )
        .unwrap();
    }
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &BTreeMap::from([("min".into(), Value::Float(f64::NEG_INFINITY))]),
            RetainedQueryOptions {
                batch_rows: nz(5),
                ..options()
            },
        )
        .unwrap();
    let batch = cursor.next_batch().unwrap().unwrap();
    let export = batch.export_arrow(0).unwrap();
    unsafe {
        let field = *export.schema().children;
        assert_eq!(CStr::from_ptr((*field).format), c"g");
        let column = *export.array().children;
        assert_eq!((*column).offset, 1);
        assert_eq!((*column).null_count, -1);
        assert_eq!(*(*(*column).buffers).cast::<u64>(), 30);
        assert_eq!(
            std::slice::from_raw_parts((*(*column).buffers.add(1)).cast::<u64>().add(1), 4),
            &[1 << 63, 0, 0x7ff800000000002a, 0x7ff0000000000000]
        );
    }
    drop(export);
    drop(batch);
    drop(cursor);
    db.query("MATCH (n:Item) SET n.score = NULL").unwrap();
    let mut cursor = db
        .query_with_params_retained(
            QUERY,
            &BTreeMap::from([("min".into(), Value::Float(0.0))]),
            RetainedQueryOptions {
                batch_rows: nz(5),
                ..options()
            },
        )
        .unwrap();
    let schema = cursor.export_arrow_schema(0).unwrap();
    assert_eq!(schema.descriptor().n_children, 3);
    let batch = cursor.next_batch().unwrap().unwrap();
    assert!(batch.selected_rows().is_empty());
    let export = batch.export_arrow(0).unwrap();
    assert_eq!(export.array().length, 0);
    unsafe {
        let column = *export.array().children;
        assert_eq!((*column).offset, 0);
        assert_eq!((*column).null_count, 0);
        assert_eq!(*(*(*column).buffers).cast::<u64>(), 0);
        assert_eq!(CStr::from_ptr((**export.schema().children).format), c"g");
    }
    assert!(cursor.next_batch().unwrap().is_none());
    drop(batch);
    drop(export);
    drop(schema);
    drop(cursor);
    assert_eq!(db.retained_result_snapshot().unwrap().retained_bytes, 0);
}
