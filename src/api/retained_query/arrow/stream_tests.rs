// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::CStr;

fn pointer(stream: &RetainedArrowStream) -> *mut ArrowArrayStream {
    // Schema/next/error callbacks only read the base descriptor; mutation is
    // serialized inside its native owner, separate from this shared reference.
    std::ptr::from_ref(stream.descriptor()).cast_mut()
}
fn pull(stream: &RetainedArrowStream) -> (i32, ArrowArray) {
    let mut array = ArrowArray::default();
    let code = unsafe { stream.descriptor().get_next.unwrap()(pointer(stream), &mut array) };
    (code, array)
}
fn release_array(array: &mut ArrowArray) {
    if let Some(release) = array.release {
        unsafe { release(array) };
    }
}
fn values(array: &ArrowArray) -> Vec<i64> {
    unsafe {
        let column = *array.children;
        std::slice::from_raw_parts(
            (*(*column).buffers.add(1))
                .cast::<i64>()
                .add((*column).offset as usize),
            column.as_ref().unwrap().length as usize,
        )
        .to_vec()
    }
}

#[test]
fn stream_creation_schema_and_stopped_consumer_do_not_pull_or_prefetch() {
    let db = fixture();
    let cursor = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap();
    let stream = cursor.into_arrow_stream(0).unwrap();
    assert_eq!(stream.profile().visited_rows, 0);
    let mut schema = ArrowSchema::default();
    assert_eq!(
        unsafe { stream.descriptor().get_schema.unwrap()(pointer(&stream), &mut schema) },
        0
    );
    assert_eq!(stream.profile().visited_rows, 0);
    let (code, mut first) = pull(&stream);
    assert_eq!(code, 0);
    assert_eq!(values(&first), vec![2]);
    assert_eq!(stream.profile().visited_rows, 3);
    assert_eq!(stream.profile().source_pinned_rows, 9);
    // Metadata observations never drive the next demanded batch.
    for _ in 0..10 {
        assert_eq!(stream.profile().visited_rows, 3);
    }
    drop(stream);
    drop(db);
    assert_eq!(values(&first), vec![2]);
    release_array(&mut first);
    unsafe { schema.release.unwrap()(&mut schema) };
}

#[test]
fn two_slot_backpressure_is_error_before_advancement_and_release_allows_retry() {
    let mut db = fixture();
    let governor = governor_with_handles(1024);
    db.set_runtime_governor(governor.clone());
    let stream = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    let (code, mut first) = pull(&stream);
    assert_eq!(code, 0);
    let (code, mut second) = pull(&stream);
    assert_eq!(code, 0);
    let before = stream.profile();
    let (code, refused) = pull(&stream);
    assert_eq!(code, 12);
    assert!(refused.release.is_none());
    assert_eq!(stream.profile().visited_rows, before.visited_rows);
    let text = unsafe {
        CStr::from_ptr(stream.descriptor().get_last_error.unwrap()(pointer(
            &stream,
        )))
    };
    assert!(text.to_string_lossy().contains("Backpressure"));
    assert_eq!(values(&first), vec![2]);
    release_array(&mut first);
    let (code, mut third) = pull(&stream);
    assert_eq!(code, 0);
    assert_eq!(values(&third), vec![6, 7, 8]);
    assert_eq!(stream.profile().visited_rows, 9);
    let (code, eof) = pull(&stream);
    assert_eq!(code, 0);
    assert!(eof.release.is_none());
    assert_eq!(stream.status(), RetainedQueryStatus::Completed);
    assert_eq!(values(&second), vec![3, 4, 5]);
    assert_eq!(values(&third), vec![6, 7, 8]);
    release_array(&mut second);
    release_array(&mut third);
    drop(stream);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
}

#[test]
fn exhausted_stream_confirms_eof_at_the_exact_handle_cap_with_live_arrays() {
    let mut db = fixture();
    let governor = governor_with_handles(7);
    db.set_runtime_governor(governor.clone());
    let stream = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    let (code, mut array) = pull(&stream);
    assert_eq!(code, 0);
    assert_eq!(values(&array), vec![2, 3, 4, 5, 6, 7, 8]);
    let before = governor.retained_result_snapshot();
    assert_eq!(before.view_handles, before.handle_limit);
    assert_eq!(stream.status(), RetainedQueryStatus::Open);
    for _ in 0..2 {
        let (code, eof) = pull(&stream);
        assert_eq!(code, 0);
        assert!(eof.release.is_none());
        assert_eq!(governor.retained_result_snapshot(), before);
    }
    assert_eq!(stream.status(), RetainedQueryStatus::Completed);
    assert_eq!(stream.profile().visited_rows, 9);
    release_array(&mut array);
    drop(stream);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn child_handle_admission_failure_precedes_source_and_original_cursor_take_is_atomic() {
    let mut db = fixture();
    let governor = governor_with_handles(11);
    db.set_runtime_governor(governor.clone());
    let mut cursor = Some(
        db.query_with_params_retained(QUERY, &params(), options())
            .unwrap(),
    );
    let before = governor.retained_result_snapshot();
    assert!(matches!(
        RetainedArrowStream::try_take_cursor(&mut cursor, usize::MAX, None),
        Err(RetainedQueryError::SizeOverflow)
    ));
    assert!(cursor.is_some());
    assert_eq!(cursor.as_ref().unwrap().profile().visited_rows, 0);
    assert_eq!(governor.retained_result_snapshot(), before);
    let stream = RetainedArrowStream::try_take_cursor(&mut cursor, 0, None).unwrap();
    assert!(cursor.is_none());
    let recovered = stream.into_cursor();
    assert_eq!(recovered.profile().visited_rows, 0);
    let recovered_snapshot = governor.retained_result_snapshot();
    assert_eq!(recovered_snapshot.retained_bytes, before.retained_bytes);
    assert_eq!(recovered_snapshot.view_handles, before.view_handles);
    assert_eq!(recovered_snapshot.buffer_owners, before.buffer_owners);
    assert_eq!(
        recovered_snapshot.backpressure_events,
        before.backpressure_events
    );
    // The temporary stream was admitted and released, so peak observations
    // truthfully include it even though all current charges are restored.
    assert!(recovered_snapshot.peak_retained_bytes > before.peak_retained_bytes);
    assert!(recovered_snapshot.peak_view_handles > before.peak_view_handles);
    cursor = Some(recovered);
    let stream = RetainedArrowStream::try_take_cursor(&mut cursor, 0, None).unwrap();
    let (code, mut first) = pull(&stream);
    assert_eq!(code, 0);
    assert_eq!(governor.retained_result_snapshot().view_handles, 7);
    // Second array descriptors can fit, but its native batch handle cannot.
    let before = stream.profile();
    let (code, refused) = pull(&stream);
    assert_eq!(code, 12);
    assert!(refused.release.is_none());
    assert_eq!(stream.profile().visited_rows, before.visited_rows);
    assert_eq!(governor.retained_result_snapshot().view_handles, 7);
    release_array(&mut first);
    let (code, mut next) = pull(&stream);
    assert_eq!(code, 0);
    assert_eq!(values(&next), vec![3, 4, 5]);
    release_array(&mut next);
    drop(stream);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn unusable_stream_protocol_preserves_cursor_and_concurrent_pulls_serialize() {
    let mut db = fixture();
    let governor = governor_with_handles(6);
    db.set_runtime_governor(governor.clone());
    let mut cursor = Some(
        db.query_with_params_retained(QUERY, &params(), options())
            .unwrap(),
    );
    let before = governor.retained_result_snapshot();
    assert!(matches!(
        RetainedArrowStream::try_take_cursor(&mut cursor, 0, None),
        Err(RetainedQueryError::WorkingUnitTooLarge)
    ));
    assert_eq!(governor.retained_result_snapshot(), before);
    assert_eq!(cursor.as_ref().unwrap().profile().visited_rows, 0);
    drop(cursor);

    let db = fixture();
    let stream = db
        .query_with_params_retained(QUERY, &params(), options())
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    let address = pointer(&stream) as usize;
    let callback = stream.descriptor().get_next.unwrap();
    let mut found = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(move || {
                    let mut array = ArrowArray::default();
                    assert_eq!(
                        unsafe { callback(address as *mut ArrowArrayStream, &mut array) },
                        0
                    );
                    let result = values(&array);
                    release_array(&mut array);
                    result
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    found.sort();
    assert_eq!(found, vec![2, 3, 4, 5]);
    assert_eq!(stream.profile().visited_rows, 6);
}

#[test]
fn sparse_selection_and_late_budget_remain_terminal_after_schema_requests() {
    let mut db = fixture();
    db.query("MATCH (n:Item) WHERE n.score = 4 SET n.score = 0")
        .unwrap();
    let stream = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    let (code, empty) = pull(&stream);
    assert_eq!(code, 22);
    assert!(empty.release.is_none());
    assert_eq!(stream.status(), RetainedQueryStatus::Failed);
    assert_eq!(stream.profile().source_pinned_rows, 0);
    let mut schema = ArrowSchema::default();
    assert_eq!(
        unsafe { stream.descriptor().get_schema.unwrap()(pointer(&stream), &mut schema) },
        0
    );
    unsafe { schema.release.unwrap()(&mut schema) };
    let (code, empty) = pull(&stream);
    assert_eq!(code, 22);
    assert!(empty.release.is_none());
    drop(stream);

    let db = fixture();
    let stream = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                max_result_rows: Some(1),
                ..options()
            },
        )
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    let (code, mut first) = pull(&stream);
    assert_eq!(code, 0);
    for _ in 0..2 {
        let (code, empty) = pull(&stream);
        assert_eq!(code, 12);
        assert!(empty.release.is_none());
        let text = unsafe {
            CStr::from_ptr(stream.descriptor().get_last_error.unwrap()(pointer(
                &stream,
            )))
        };
        assert!(text.to_string_lossy().contains("ResultBudget"));
    }
    assert_eq!(values(&first), vec![2]);
    drop(stream);
    release_array(&mut first);
}

#[test]
fn module_owner_survives_moved_stream_and_independent_array_schema_release() {
    #[derive(Debug)]
    struct Module;
    let module = Arc::new(Module);
    let weak = Arc::downgrade(&module);
    let db = fixture();
    let mut cursor = Some(
        db.query_with_params_retained(QUERY, &params(), options())
            .unwrap(),
    );
    let stream =
        RetainedArrowStream::try_take_cursor(&mut cursor, 0, Some(module.clone())).unwrap();
    drop(module);
    let mut raw = stream.into_raw();
    let mut schema = ArrowSchema::default();
    assert_eq!(unsafe { raw.get_schema.unwrap()(&mut raw, &mut schema) }, 0);
    let mut array = ArrowArray::default();
    assert_eq!(unsafe { raw.get_next.unwrap()(&mut raw, &mut array) }, 0);
    // Move the stream base before release; bookkeeping never uses its address.
    let mut moved = raw;
    unsafe { moved.release.unwrap()(&mut moved) };
    drop(db);
    assert!(weak.upgrade().is_some());
    release_array(&mut array);
    assert!(weak.upgrade().is_some());
    unsafe { schema.release.unwrap()(&mut schema) };
    assert!(weak.upgrade().is_none());
}

#[test]
fn empty_selected_batch_is_not_arrow_eof_and_invalid_outputs_do_not_advance() {
    let mut db = fixture();
    db.query("MATCH (n:Item) SET n.score = NULL").unwrap();
    let stream = db
        .query_with_params_retained(
            QUERY,
            &params(),
            RetainedQueryOptions {
                batch_rows: nz(9),
                ..options()
            },
        )
        .unwrap()
        .into_arrow_stream(0)
        .unwrap();
    assert_eq!(
        unsafe { stream.descriptor().get_next.unwrap()(pointer(&stream), std::ptr::null_mut()) },
        22
    );
    assert_eq!(stream.profile().visited_rows, 0);
    let (code, mut empty) = pull(&stream);
    assert_eq!(code, 0);
    assert!(empty.release.is_some());
    assert_eq!(empty.length, 0);
    release_array(&mut empty);
    let (code, eof) = pull(&stream);
    assert_eq!(code, 0);
    assert!(eof.release.is_none());
    assert_eq!(stream.status(), RetainedQueryStatus::Completed);
}
