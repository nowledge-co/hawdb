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

//! Mutations that find their targets by an indexed property seek the index
//! instead of scanning the label, and write exactly what the scan writes.
//!
//! Every scenario runs against an indexed and an unindexed database built
//! from the same statements; the final graphs must be identical.

use super::*;
use crate::store::StorageResidencyMode;

const NODES: usize = 40;

/// Writes by indexed property, including the same-transaction cases the
/// index must see: a node created, re-keyed or deleted earlier in the run.
const WRITES: &[&str] = &[
    "MATCH (m:M {id: 'k1'}) SET m.v = 1",
    // Two nodes share this key; both are written.
    "MATCH (m:M {id: 'dup'}) SET m.v = 2",
    // Extra conjuncts are rechecked on every candidate.
    "MATCH (m:M {id: 'k2'}) WHERE m.state = 'active' SET m.v = 3",
    "MATCH (m:M {id: 'k3'}) WHERE m.state = 'active' SET m.v = 3",
    "CREATE (:M {id: 'new', state: 'active', v: 0})",
    "MATCH (m:M {id: 'new'}) SET m.v = 4",
    "MATCH (m:M {id: 'k5'}) SET m.id = 'k5b'",
    "MATCH (m:M {id: 'k5'}) SET m.v = 5",
    "MATCH (m:M {id: 'k5b'}) SET m.v = 6",
    "MATCH (m:M {id: 'k6'}) DELETE m",
    "MATCH (m:M {id: 'k6'}) SET m.v = 7",
    "MATCH (m:M) WHERE m.id IN ['k7', 'k8', 'missing'] SET m.v = 8",
    "MATCH (m:M {id: 'k9'}) SET m.id = 'k10'",
    // Both the re-keyed node and the original `k10` match.
    "MATCH (m:M {id: 'k10'}) SET m.v = 10",
    "MATCH (m:M {id: 'new'}) DETACH DELETE m",
];

fn populate(path: &std::path::Path, indexed: bool, checkpoint: bool) {
    let mut db = Database::open(path).unwrap();
    if indexed {
        db.query("CREATE INDEX ON :M(id)").unwrap();
    }
    for i in 0..NODES {
        let state = if i % 2 == 0 { "inactive" } else { "active" };
        db.query(&format!(
            "CREATE (:M {{id: 'k{i}', state: '{state}', v: 0, body: '{}'}})",
            "x".repeat(64)
        ))
        .unwrap();
    }
    for _ in 0..2 {
        db.query("CREATE (:M {id: 'dup', state: 'active', v: 0})")
            .unwrap();
    }
    if checkpoint {
        db.checkpoint().unwrap();
    }
}

/// Index seeks and label scans by mutation target lookups, from the public
/// residency report Mem reads.
fn mutation_targets(db: &Database) -> (u64, u64) {
    let reads = db.storage_residency_report().unwrap().graph_index_reads;
    (
        reads.mutation_target_label_scans,
        reads.mutation_target_index_seeks,
    )
}

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

/// Every `:M` node with its internal id and all properties.
fn graph(db: &mut Database) -> Vec<Vec<Value>> {
    db.query(
        "MATCH (m:M) RETURN id(m) AS node, m.id AS id, m.state AS state, m.v AS v, m.body AS body \
         ORDER BY node",
    )
    .unwrap()
    .rows
    .into_rows()
    .into_iter()
    .map(|row| row.values().cloned().collect())
    .collect()
}

/// The report covers resident indexes; an out-of-core store reports itself
/// not ready and is covered by the reopen comparison instead.
fn assert_index_consistent(db: &Database, context: &str) {
    let report = db.property_index_consistency_report().unwrap();
    if !report.ready {
        return;
    }
    assert_eq!(
        (
            report.missing_node_key_count,
            report.extra_node_key_count,
            report.mismatched_node_key_count,
        ),
        (0, 0, 0),
        "{context}"
    );
}

/// Runs `WRITES` in one transaction or one autocommit statement at a time and
/// returns the final graph with the scan and seek counts the writes caused.
fn run(
    residency: StorageResidencyMode,
    indexed: bool,
    checkpoint: bool,
    transactional: bool,
) -> (Vec<Vec<Value>>, u64, u64) {
    let path = unique_test_dir("mutation_index_seek");
    populate(&path, indexed, checkpoint);
    let mut db = open(&path, residency);
    let (scans, seeks) = mutation_targets(&db);
    if transactional {
        let mut transaction = db.begin_transaction().unwrap();
        for write in WRITES {
            transaction.query(write).unwrap();
        }
        transaction.commit().unwrap();
    } else {
        for write in WRITES {
            db.query(write).unwrap();
        }
    }
    let (scans_after, seeks_after) = mutation_targets(&db);
    let counts = (scans_after - scans, seeks_after - seeks);
    let context = format!(
        "{residency:?} indexed={indexed} checkpoint={checkpoint} transactional={transactional}"
    );
    if indexed {
        assert_index_consistent(&db, &context);
    }
    let before_reopen = graph(&mut db);
    drop(db);
    let mut reopened = open(&path, residency);
    assert_eq!(graph(&mut reopened), before_reopen, "{context}: reopen");
    let _ = std::fs::remove_dir_all(&path);
    (before_reopen, counts.0, counts.1)
}

#[test]
fn indexed_property_writes_match_the_label_scan() {
    for residency in [
        StorageResidencyMode::OutOfCore,
        StorageResidencyMode::Materialized,
    ] {
        for checkpoint in [true, false] {
            for transactional in [true, false] {
                let context =
                    format!("{residency:?} checkpoint={checkpoint} transactional={transactional}");
                let (scanned, scan_scans, scan_seeks) =
                    run(residency, false, checkpoint, transactional);
                let (indexed, index_scans, index_seeks) =
                    run(residency, true, checkpoint, transactional);
                assert_eq!(indexed, scanned, "{context}");
                assert!(scan_scans > 0, "{context}: unindexed writes scan");
                assert_eq!(scan_seeks, 0, "{context}: no index to seek");
                // An out-of-core base checkpointed before the index existed
                // has no equality projection for it: writes keep scanning
                // until the next checkpoint publishes one.
                if residency == StorageResidencyMode::Materialized || checkpoint {
                    assert_eq!(index_scans, 0, "{context}: indexed writes never scan");
                    assert!(index_seeks > 0, "{context}: indexed writes seek");
                }
            }
        }
    }
}

#[test]
fn index_created_and_used_without_reopening_seeks() {
    let path = unique_test_dir("mutation_index_seek_fresh");
    let mut db = Database::open(&path).unwrap();
    db.query("CREATE (:M {id: 'a', v: 0})").unwrap();
    db.query("CREATE (:M {id: 'b', v: 0})").unwrap();
    // Backfilled into existing nodes; no checkpoint, no reopen.
    db.query("CREATE INDEX ON :M(id)").unwrap();
    let (scans, seeks) = mutation_targets(&db);
    db.query("MATCH (m:M {id: 'a'}) SET m.v = 1").unwrap();
    assert_eq!(mutation_targets(&db), (scans, seeks + 1));
    let rows = db
        .query("MATCH (m:M) RETURN m.id AS id, m.v AS v ORDER BY id")
        .unwrap();
    assert_eq!(
        rows.rows
            .into_rows()
            .into_iter()
            .map(|row| row.values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::String("a".into()), Value::Int(1)],
            vec![Value::String("b".into()), Value::Int(0)],
        ]
    );
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn index_created_in_the_same_transaction_finds_committed_nodes() {
    for residency in [
        StorageResidencyMode::OutOfCore,
        StorageResidencyMode::Materialized,
    ] {
        let path = unique_test_dir("mutation_index_seek_staged");
        {
            let mut db = Database::open(&path).unwrap();
            db.query("CREATE (:M {id: 'a', v: 0})").unwrap();
            db.query("CREATE (:M {id: 'b', v: 0})").unwrap();
            db.checkpoint().unwrap();
        }
        let mut db = open(&path, residency);
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query("CREATE INDEX ON :M(id)").unwrap();
        // Whether the write seeks the index staged here or scans, it must
        // find the committed node, including one in an out-of-core base.
        transaction
            .query("MATCH (m:M {id: 'a'}) SET m.v = 1")
            .unwrap();
        transaction.commit().unwrap();
        let rows = db
            .query("MATCH (m:M) RETURN m.id AS id, m.v AS v ORDER BY id")
            .unwrap();
        assert_eq!(
            rows.rows
                .into_rows()
                .into_iter()
                .map(|row| row.values().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::String("a".into()), Value::Int(1)],
                vec![Value::String("b".into()), Value::Int(0)],
            ],
            "{residency:?}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}
