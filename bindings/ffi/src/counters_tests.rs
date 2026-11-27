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

//! The result envelope's `"counters"` object over the C ABI.

use super::*;
use crate::transaction::{
    hawdb_begin_transaction, hawdb_commit_transaction, hawdb_transaction_query,
};

type QueryFn = unsafe extern "C" fn(
    *mut HawdbDatabase,
    *const c_char,
    usize,
    *const c_char,
    usize,
    *mut HawdbBuffer,
    *mut HawdbBuffer,
) -> bool;

fn empty_buffer() -> HawdbBuffer {
    HawdbBuffer {
        ptr: ptr::null_mut(),
        len: 0,
    }
}

/// Parses and frees an owned result buffer.
unsafe fn take(buffer: &mut HawdbBuffer) -> serde_json::Value {
    assert!(!buffer.ptr.is_null(), "call returned no buffer");
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr.cast::<u8>(), buffer.len) };
    let value = serde_json::from_slice(bytes).unwrap();
    unsafe { hawdb_buffer_free(buffer) };
    value
}

fn run(call: QueryFn, db: *mut HawdbDatabase, text: &str) -> serde_json::Value {
    let mut result = empty_buffer();
    let mut error = empty_buffer();
    let ok = unsafe {
        call(
            db,
            text.as_ptr().cast(),
            text.len(),
            ptr::null(),
            0,
            &mut result,
            &mut error,
        )
    };
    assert!(ok, "{text} failed: {}", unsafe { take(&mut error) });
    unsafe { take(&mut result) }
}

fn in_memory() -> *mut HawdbDatabase {
    let mut error = empty_buffer();
    let db = unsafe { hawdb_open_in_memory(&mut error) };
    assert!(!db.is_null(), "open failed");
    db
}

/// Every counter key with value 0, overridden by `nonzero`.
fn counters(nonzero: serde_json::Value) -> serde_json::Value {
    let mut all = json!({
        "nodes_created": 0,
        "nodes_deleted": 0,
        "relationships_created": 0,
        "relationships_deleted": 0,
        "properties_set": 0,
        "labels_added": 0,
        "labels_removed": 0,
        "indexes_added": 0,
        "indexes_removed": 0,
        "constraints_added": 0,
        "constraints_removed": 0,
    });
    for (key, value) in nonzero.as_object().unwrap() {
        assert!(all.get(key).is_some(), "unknown counter {key}");
        all[key] = value.clone();
    }
    all
}

#[test]
fn cypher_envelope_reports_every_counter_key() {
    let db = in_memory();
    let created = run(hawdb_query, db, "CREATE (:A {k: 1, v: 5})-[:R]->(:B)");
    assert_eq!(
        created["counters"],
        counters(json!({
            "nodes_created": 2,
            "relationships_created": 1,
            "labels_added": 2,
            "properties_set": 2,
        }))
    );
    // Same-value SET still executes, and counts, one property write.
    assert_eq!(
        run(hawdb_query, db, "MATCH (n:A {k: 1}) SET n.v = 5")["counters"],
        counters(json!({"properties_set": 1}))
    );
    // A pattern that matches nothing and a read report all zeros.
    assert_eq!(
        run(hawdb_query, db, "MATCH (n:A {k: 2}) SET n.v = 5")["counters"],
        counters(json!({}))
    );
    let read = run(hawdb_query, db, "MATCH (n:A) RETURN n.v AS v");
    assert_eq!(read["rows"], json!([[5]]));
    assert_eq!(read["counters"], counters(json!({})));
    assert_eq!(
        run(hawdb_query, db, "MATCH (n:A) DETACH DELETE n")["counters"],
        counters(json!({"nodes_deleted": 1, "relationships_deleted": 1}))
    );
    unsafe { hawdb_close(db) };
}

#[test]
fn sql_and_commit_envelopes_report_zero_counters() {
    let db = in_memory();
    assert_eq!(
        run(hawdb_query_sql, db, "SELECT 1 AS one")["counters"],
        counters(json!({}))
    );
    let mut error = empty_buffer();
    assert!(unsafe { hawdb_begin_transaction(db, &mut error) });
    assert_eq!(
        run(hawdb_transaction_query, db, "CREATE (:A {k: 1})")["counters"],
        counters(json!({"nodes_created": 1, "labels_added": 1, "properties_set": 1}))
    );
    assert_eq!(
        run(hawdb_transaction_query, db, "MATCH (n:A) SET n.k = 2")["counters"],
        counters(json!({"properties_set": 1}))
    );
    let mut result = empty_buffer();
    assert!(unsafe { hawdb_commit_transaction(db, &mut result, &mut error) });
    assert_eq!(
        unsafe { take(&mut result) }["counters"],
        counters(json!({}))
    );
    unsafe { hawdb_close(db) };
}
