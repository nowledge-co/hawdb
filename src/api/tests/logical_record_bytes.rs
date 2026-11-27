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

//! Commit admission and storage-pressure signals use the maintained logical
//! record byte estimate instead of walking every record (#2032). The full
//! walk stays available as a reference and counts each invocation.

use super::*;

fn config(mode: crate::StorageResidencyMode) -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: mode,
        // Isolates commit admission from planner statistics refreshes.
        optimizer_statistics_inline_commit_lag: Some(u64::MAX),
        ..DatabaseConfig::default()
    }
}

fn rows(rows: impl Iterator<Item = BTreeMap<String, Value>>) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "rows".to_string(),
        Value::List(rows.map(Value::Map).collect()),
    )])
}

fn walks(db: &Database) -> u64 {
    db.runtime.get().unwrap().store.logical_record_byte_walks()
}

/// Asserts the maintained estimate equals the full walk and returns it.
fn verified_bytes(db: &Database) -> u64 {
    let runtime = db.runtime.get().unwrap();
    let maintained = runtime.store.estimated_logical_record_bytes();
    assert_eq!(
        maintained,
        runtime.store.estimated_logical_record_bytes_reference()
    );
    maintained
}

#[test]
fn autocommit_sets_on_a_large_resident_store_do_not_walk_records() {
    let path = unique_test_dir("logical_record_bytes_no_walk");
    let mut db =
        Database::open_with_config(&path, config(crate::StorageResidencyMode::Materialized))
            .unwrap();
    let body = "b".repeat(256);
    for batch in 0..5_i64 {
        let nodes = (batch * 10_000..(batch + 1) * 10_000).map(|id| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(id)),
                ("body".to_string(), Value::String(body.clone())),
            ])
        });
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:M {id: r.id, body: r.body, v: 0})",
            &rows(nodes),
        )
        .unwrap();
    }
    db.query("CREATE INDEX ON :M(id)").unwrap();
    assert!(!db.storage_residency_report().unwrap().out_of_core);
    let before = walks(&db);
    let bytes_before = db
        .runtime
        .get()
        .unwrap()
        .store
        .estimated_logical_record_bytes();

    for step in 0..256_i64 {
        db.query_with_params(
            "MATCH (m:M {id: $id}) SET m.v = $v",
            &BTreeMap::from([
                ("id".to_string(), Value::Int(step * 191)),
                ("v".to_string(), Value::Int(step + 1)),
            ]),
        )
        .unwrap();
    }
    // Every commit is admitted and reports pressure without a record walk.
    assert_eq!(walks(&db), before);
    assert!(db.storage_pressure_snapshot().unwrap().current_commit_epoch > 0);
    assert_eq!(walks(&db), before);
    // `v` changes from 0 to an equally sized integer.
    assert_eq!(
        db.runtime
            .get()
            .unwrap()
            .store
            .estimated_logical_record_bytes(),
        bytes_before
    );
    assert_eq!(verified_bytes(&db), bytes_before);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn maintained_estimate_matches_full_walk_through_mutations_rollback_and_reopen() {
    for mode in [
        crate::StorageResidencyMode::Materialized,
        crate::StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("logical_record_bytes_matches_walk");
        let mut db = Database::open_with_config(&path, config(mode)).unwrap();
        let large = "雪".repeat(3_000);
        let nodes = (0..300_i64).map(|id| {
            BTreeMap::from([
                ("id".to_string(), Value::Int(id)),
                ("body".to_string(), Value::String(format!("{id}{large}"))),
            ])
        });
        db.query_with_params(
            "UNWIND $rows AS r CREATE (:A {id: r.id, body: r.body, tags: ['x', 'y']})",
            &rows(nodes),
        )
        .unwrap();
        let edges = (1..300_i64).map(|id| {
            BTreeMap::from([
                ("s".to_string(), Value::Int(id - 1)),
                ("t".to_string(), Value::Int(id)),
            ])
        });
        db.query_with_params(
            "UNWIND $rows AS r MATCH (s:A {id: r.s}), (t:A {id: r.t}) \
             CREATE (s)-[:R {w: r.s, note: 'edge'}]->(t)",
            &rows(edges),
        )
        .unwrap();
        let created = verified_bytes(&db);
        if mode == crate::StorageResidencyMode::OutOfCore {
            db.checkpoint().unwrap();
            assert!(db.storage_residency_report().unwrap().out_of_core);
            verified_bytes(&db);
        }

        for statement in [
            // Property additions, growth, shrinkage and removal.
            "MATCH (a:A) WHERE a.id < 40 SET a.extra = {k: [1, 2, 3], s: 'nested'}",
            "MATCH (a:A) WHERE a.id >= 40 AND a.id < 80 SET a.body = 'short'",
            "MATCH (a:A) WHERE a.id >= 80 AND a.id < 90 SET a.tags = NULL",
            "MATCH (a:A {id: 95}) SET a.body = a.body + a.body",
            // Relationship property updates and removal.
            "MATCH (:A)-[r:R]->(:A) WHERE r.w < 50 SET r.note = 'a much longer relationship note'",
            "MATCH (:A)-[r:R]->(:A) WHERE r.w >= 50 AND r.w < 60 SET r.note = NULL",
            // Relationship and node deletes.
            "MATCH (:A)-[r:R]->(:A) WHERE r.w >= 200 AND r.w < 220 DELETE r",
            "MATCH (a:A) WHERE a.id >= 280 DETACH DELETE a",
            "CREATE (:B {payload: 'p'})-[:S {weight: 1.5}]->(:B {payload: 'q'})",
        ] {
            db.query(statement).unwrap();
            verified_bytes(&db);
        }
        let committed = verified_bytes(&db);
        assert_ne!(committed, created);

        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("MATCH (a:A) WHERE a.id < 100 SET a.body = 'rolled back'")
            .unwrap();
        transaction
            .query("MATCH (a:A) WHERE a.id >= 100 AND a.id < 120 DETACH DELETE a")
            .unwrap();
        transaction
            .query("CREATE (:C {body: 'never committed'})")
            .unwrap();
        transaction.rollback();
        assert_eq!(verified_bytes(&db), committed);

        // Crash: drop without a checkpoint, then recover from the WAL.
        drop(db);
        let reopened = Database::open_with_config(&path, config(mode)).unwrap();
        assert_eq!(verified_bytes(&reopened), committed);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}
