// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::super::tests::{constrained_governor, release, request, size, state};
use super::*;
use hawdb::{HawDBEmbedded, RuntimeGovernor};
use std::collections::BTreeMap;

const QUERY: &str = "MATCH (n:Item) WHERE n.id = $id RETURN n";
fn fixture(handles: usize) -> (*mut HawdbDatabase, RuntimeGovernor) {
    let mut db = HawDBEmbedded::open_in_memory();
    db.query_admitted("CREATE NODE TABLE Item").unwrap();
    db.query_admitted("CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT")
        .unwrap();
    db.query_admitted("CREATE INDEX ON :Item(id)").unwrap();
    for id in 1..=128 {
        db.database_mut()
            .query_with_params(
                "CREATE (:Item {id: $id, payload: $payload, nested: $nested})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("payload".into(), Value::String("old\0\u{4e16}".into())),
                    (
                        "nested".into(),
                        Value::List(vec![Value::Map(BTreeMap::from([(
                            "bytes".into(),
                            Value::Binary(vec![0, 255, 7]),
                        )]))]),
                    ),
                ]),
            )
            .unwrap();
    }
    let governor = constrained_governor(handles);
    db.database_mut().set_runtime_governor(governor.clone());
    (
        Box::into_raw(Box::new(HawdbDatabase {
            inner: Mutex::new(db),
        })),
        governor,
    )
}
fn cursor(db: *mut HawdbDatabase, id: u64) -> HawdbRetainedCursorV1 {
    let params = format!("{{\"id\":{id}}}");
    let request = request(QUERY, &params);
    let mut out = HawdbRetainedCursorV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_query(
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
fn next(cursor: HawdbRetainedCursorV1) -> HawdbRetainedNodeV1 {
    let mut out = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn field(value: HawdbRetainedNodeV1, name: &str) -> HawdbRetainedNodeV1 {
    let mut out = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_field(
                value.owner_namespace,
                value.owner_id,
                name.as_ptr().cast(),
                name.len() as u64,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn child(value: HawdbRetainedNodeV1, index: u64) -> HawdbRetainedNodeV1 {
    let mut out = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_child(
                value.owner_namespace,
                value.owner_id,
                index,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_OK
    );
    out
}
fn bytes(value: HawdbRetainedNodeV1) -> Vec<u8> {
    unsafe { std::slice::from_raw_parts(value.data.cast::<u8>(), value.byte_length as usize) }
        .to_vec()
}

#[test]
fn original_node_buffers_survive_write_and_all_parent_releases() {
    let (db, governor) = fixture(32);
    let cursor = cursor(db, 1);
    let node = next(cursor);
    let text = field(node, "payload");
    let duplicate = field(node, "payload");
    assert_eq!(text.data, duplicate.data);
    assert_eq!(text.flags, HAWDB_RETAINED_READ_ONLY);
    let identity = field(node, "_id");
    assert_eq!(identity.byte_length, 8);
    let labels = field(node, "labels");
    let label = child(labels, 0);
    assert_eq!(bytes(label), b"Item");
    let nested = field(node, "nested");
    let map = child(nested, 0);
    let binary = field(map, "bytes");
    assert_eq!(binary.kind, HAWDB_RETAINED_BINARY);
    unsafe {
        (&*db)
            .inner
            .lock()
            .unwrap()
            .query_admitted("MATCH (n:Item) WHERE n.id = 1 SET n.payload = 'new'")
            .unwrap();
    }
    assert_eq!(bytes(text), "old\0\u{4e16}".as_bytes());
    let mut eof = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut eof,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_EOF
    );
    assert_eq!(eof.owner_id, 0);
    for value in [node, duplicate, identity, labels, nested, map] {
        release(value.owner_namespace, value.owner_id);
    }
    release(cursor.owner_namespace, cursor.owner_id);
    unsafe {
        super::super::super::hawdb_close(db);
    }
    assert_eq!(bytes(text), "old\0\u{4e16}".as_bytes());
    assert_eq!(bytes(binary), [0, 255, 7]);
    assert_eq!(bytes(label), b"Item");
    assert_eq!(
        state(text.owner_namespace, text.owner_id).status,
        hawdb::RetainedQueryStatus::Completed as u32
    );
    for value in [text, binary, label] {
        release(value.owner_namespace, value.owner_id);
    }
    assert_eq!(governor.retained_result_snapshot().view_handles, 0);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
}

#[test]
fn failed_child_admission_preserves_parent_and_quota() {
    let (db, governor) = fixture(3);
    let cursor = cursor(db, 1);
    let node = next(cursor);
    let text = field(node, "payload");
    let before = governor.retained_result_snapshot();
    let mut refused = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_retain(
                text.owner_namespace,
                text.owner_id,
                &mut refused,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_BACKPRESSURE
    );
    assert_eq!(refused.owner_id, 0);
    let after = governor.retained_result_snapshot();
    assert_eq!(after.retained_bytes, before.retained_bytes);
    assert_eq!(after.view_handles, before.view_handles);
    assert_eq!(after.buffer_owners, before.buffer_owners);
    assert_eq!(bytes(text), "old\0\u{4e16}".as_bytes());
    release(text.owner_namespace, text.owner_id);
    let duplicate = field(node, "payload");
    release(node.owner_namespace, node.owner_id);
    assert_eq!(
        hawdb_retained_cursor_close(cursor.owner_namespace, cursor.owner_id),
        HAWDB_RETAINED_OK
    );
    assert_eq!(
        state(duplicate.owner_namespace, duplicate.owner_id).status,
        hawdb::RetainedQueryStatus::Closed as u32
    );
    assert_eq!(bytes(duplicate), "old\0\u{4e16}".as_bytes());
    release(duplicate.owner_namespace, duplicate.owner_id);
    release(cursor.owner_namespace, cursor.owner_id);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    unsafe {
        super::super::super::hawdb_close(db);
    }
}

#[test]
fn missing_seek_invalid_field_and_foreign_handle_have_empty_outputs() {
    let (db, governor) = fixture(16);
    let empty = cursor(db, 999);
    let mut out = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_next(
                empty.owner_namespace,
                empty.owner_id,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_EOF
    );
    release(empty.owner_namespace, empty.owner_id);
    let cursor = cursor(db, 1);
    let node = next(cursor);
    assert_eq!(
        unsafe {
            hawdb_retained_node_field(
                node.owner_namespace,
                node.owner_id,
                c"missing".as_ptr(),
                7,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_COLUMN
    );
    assert_eq!(out.owner_id, 0);
    assert_eq!(
        unsafe {
            hawdb_retained_node_child(
                node.owner_namespace ^ 1,
                node.owner_id,
                0,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_HANDLE
    );
    release(node.owner_namespace, node.owner_id);
    assert_eq!(
        unsafe {
            hawdb_retained_node_retain(
                node.owner_namespace,
                node.owner_id,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_INVALID_HANDLE
    );
    release(cursor.owner_namespace, cursor.owner_id);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    unsafe {
        super::super::super::hawdb_close(db);
    }
}

#[test]
fn failed_root_registration_never_becomes_successful_eof() {
    let (db, governor) = fixture(16);
    let cursor = cursor(db, 1);
    PANIC_ON_BATCH_REGISTRATION.with(|flag| flag.set(true));
    for _ in 0..2 {
        let mut out = HawdbRetainedNodeV1::default();
        assert_eq!(
            unsafe {
                hawdb_retained_node_next(
                    cursor.owner_namespace,
                    cursor.owner_id,
                    &mut out,
                    size::<HawdbRetainedNodeV1>(),
                )
            },
            HAWDB_RETAINED_PANIC
        );
        assert_eq!(out.owner_id, 0);
        assert_eq!(
            state(cursor.owner_namespace, cursor.owner_id).status,
            hawdb::RetainedQueryStatus::Failed as u32
        );
    }
    release(cursor.owner_namespace, cursor.owner_id);
    assert_eq!(governor.retained_result_snapshot().retained_bytes, 0);
    unsafe {
        super::super::super::hawdb_close(db);
    }
}

#[test]
fn empty_foreign_cursor_control_is_charged_after_eof_until_release() {
    let (db, governor) = fixture(16);
    let cursor = cursor(db, 999);
    let before = governor.snapshot().admitted_memory_bytes;
    let mut out = HawdbRetainedNodeV1::default();
    assert_eq!(
        unsafe {
            hawdb_retained_node_next(
                cursor.owner_namespace,
                cursor.owner_id,
                &mut out,
                size::<HawdbRetainedNodeV1>(),
            )
        },
        HAWDB_RETAINED_EOF
    );
    let completed = governor.snapshot().admitted_memory_bytes;
    assert!(completed > 0 && completed < before);
    assert_eq!(
        hawdb_retained_cursor_close(cursor.owner_namespace, cursor.owner_id),
        HAWDB_RETAINED_OK
    );
    assert_eq!(governor.snapshot().admitted_memory_bytes, completed);
    release(cursor.owner_namespace, cursor.owner_id);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    unsafe {
        super::super::super::hawdb_close(db);
    }
}
