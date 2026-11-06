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

//! A property write on a canonical relationship of an out-of-core store
//! copies it into the delta. Canonical reads skip every relationship the delta
//! holds, so the copy must be in the delta adjacency and relationship property
//! index, or expands lose it.

use super::*;
use crate::store::StorageResidencyMode;

/// Node 1 has two outgoing `R` relationships; the write rewrites the first.
fn populate(path: &std::path::Path) {
    let mut db = Database::open(path).unwrap();
    for id in 1..=3 {
        db.query(&format!("CREATE (:N {{id: {id}}})")).unwrap();
    }
    db.query("MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:R {p: 1, q: 'x'}]->(b)")
        .unwrap();
    db.query("MATCH (a:N {id: 1}), (b:N {id: 3}) CREATE (a)-[:R {p: 2}]->(b)")
        .unwrap();
    db.checkpoint().unwrap();
}

const WRITE: &str = "MATCH (:N {id: 1})-[r:R {p: 1}]->(:N {id: 2}) SET r.p = 10";

/// Reads and their rows after the write.
fn reads() -> [(&'static str, Vec<Vec<Value>>); 5] {
    let both = vec![
        vec![Value::Int(2), Value::Int(3)],
        vec![Value::Int(10), Value::Int(2)],
    ];
    [
        (
            "MATCH (s {id: 1})-[r]->(t) RETURN r.p, t.id ORDER BY r.p",
            both.clone(),
        ),
        (
            "MATCH (s:N {id: 1})-[r:R]->(t:N) RETURN r.p, t.id ORDER BY r.p",
            both,
        ),
        (
            "MATCH (t {id: 2})<-[r]-(s) RETURN r.p, s.id",
            vec![vec![Value::Int(10), Value::Int(1)]],
        ),
        (
            "MATCH ()-[r:R]->() RETURN r.p, r.q ORDER BY r.p",
            vec![
                vec![Value::Int(2), Value::Null],
                vec![Value::Int(10), Value::String("x".into())],
            ],
        ),
        (
            "MATCH (s:N)-[r:R {q: 'x'}]->(t:N) RETURN r.p, t.id",
            vec![vec![Value::Int(10), Value::Int(2)]],
        ),
    ]
}

/// Opens `path` without an automatic checkpoint, so a reopen replays the WAL
/// over the same checkpointed base instead of rewriting it.
fn open(path: &std::path::Path, residency: StorageResidencyMode) -> Database {
    Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: residency,
            automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

fn assert_consistent(db: &Database, context: &str) {
    let consistency = db.property_index_consistency_report().unwrap();
    assert_eq!(
        (
            consistency.missing_relationship_key_count,
            consistency.extra_relationship_key_count,
            consistency.mismatched_relationship_key_count,
        ),
        (0, 0, 0),
        "{context}: {:?}",
        consistency.mismatched_relationship_keys
    );
}

fn rows(output: QueryOutput) -> Vec<Vec<Value>> {
    output
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

#[test]
fn relationship_property_writes_keep_canonical_relationships_visible() {
    for residency in [
        StorageResidencyMode::OutOfCore,
        // Control: a materialized store is unaffected.
        StorageResidencyMode::Materialized,
    ] {
        let path = unique_test_dir("out_of_core_relationship_set");
        populate(&path);
        let mut db = open(&path, residency);
        assert_eq!(
            db.storage_residency_report().unwrap().out_of_core,
            residency == StorageResidencyMode::OutOfCore
        );
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query(WRITE).unwrap();
        for (query, expected) in reads() {
            assert_eq!(
                rows(transaction.query(query).unwrap()),
                expected,
                "{residency:?} uncommitted: {query}"
            );
        }
        transaction.commit().unwrap();
        for (query, expected) in reads() {
            assert_eq!(
                rows(db.query(query).unwrap()),
                expected,
                "{residency:?} committed: {query}"
            );
        }
        assert_consistent(&db, &format!("{residency:?} committed"));
        // A reopen before the next checkpoint replays the write from the WAL
        // over the same checkpointed base.
        drop(db);
        let mut db = open(&path, residency);
        assert_eq!(
            db.storage_residency_report().unwrap().out_of_core,
            residency == StorageResidencyMode::OutOfCore
        );
        for (query, expected) in reads() {
            assert_eq!(
                rows(db.query(query).unwrap()),
                expected,
                "{residency:?} replayed: {query}"
            );
        }
        assert_consistent(&db, &format!("{residency:?} replayed"));
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

/// Deleting a canonical relationship, alone or after a property write in the
/// same transaction, leaves only the other relationship, with a clean index.
#[test]
fn deleting_canonical_relationships_keeps_the_delta_indexes_balanced() {
    let remaining = |query: &str| -> Vec<Vec<Value>> {
        match query {
            q if q.starts_with("MATCH (t {id: 2})") => Vec::new(),
            q if q.contains("r.q") => vec![vec![Value::Int(2), Value::Null]],
            q if q.contains("{q: 'x'}") => Vec::new(),
            _ => vec![vec![Value::Int(2), Value::Int(3)]],
        }
    };
    let deletes: [&[&str]; 2] = [
        &["MATCH (:N {id: 1})-[r:R {p: 1}]->(:N {id: 2}) DELETE r"],
        &[
            WRITE,
            "MATCH (:N {id: 1})-[r:R {p: 10}]->(:N {id: 2}) DELETE r",
        ],
    ];
    for residency in [
        StorageResidencyMode::OutOfCore,
        StorageResidencyMode::Materialized,
    ] {
        for statements in deletes {
            let path = unique_test_dir("out_of_core_relationship_delete");
            populate(&path);
            let mut db = open(&path, residency);
            let mut transaction = db.begin_transaction().unwrap();
            for statement in statements {
                transaction.query(statement).unwrap();
            }
            for (query, _) in reads() {
                assert_eq!(
                    rows(transaction.query(query).unwrap()),
                    remaining(query),
                    "{residency:?} {statements:?} uncommitted: {query}"
                );
            }
            transaction.commit().unwrap();
            assert_consistent(&db, &format!("{residency:?} {statements:?} committed"));
            drop(db);
            let mut db = open(&path, residency);
            for (query, _) in reads() {
                assert_eq!(
                    rows(db.query(query).unwrap()),
                    remaining(query),
                    "{residency:?} {statements:?} replayed: {query}"
                );
            }
            assert_consistent(&db, &format!("{residency:?} {statements:?} replayed"));
            drop(db);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
