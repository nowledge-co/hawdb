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
    // Relationships between matched endpoints: both committed, one created
    // in this run, an existing edge merged, a new edge merged.
    "MATCH (a:M {id: 'k11'}), (b:M {id: 'k12'}) CREATE (a)-[:R {w: 1}]->(b)",
    "CREATE (:M {id: 'fresh', state: 'active', v: 0})",
    "MATCH (a:M {id: 'fresh'}), (b:M {id: 'k13'}) CREATE (a)-[:R {w: 2}]->(b)",
    "MATCH (a:M {id: 'k11'}), (b:M {id: 'k12'}) MERGE (a)-[:R {w: 1}]->(b)",
    "MATCH (a:M {id: 'k15'}), (b:M {id: 'k16'}) MERGE (a)-[:R {w: 3}]->(b)",
    // Re-target: merge an edge from a matched source to a matched target.
    "MATCH (m:M {id: 'k15'})-[:R]->(src:M {id: 'k16'}) MATCH (dst:M {id: 'k19'}) \
     MERGE (m)-[r:R]->(dst) ON CREATE SET r.w = 5",
    // Re-source: copy an existing edge's target to another matched source.
    "MATCH (old:M {id: 'k15'})-[:R]->(t:M {id: 'k16'}), (new:M {id: 'k20'}) \
     MERGE (new)-[r:R]->(t) ON CREATE SET r.w = 6",
    // Node MERGE: an existing key and a new one.
    "MERGE (m:M {id: 'k17'}) SET m.v = 17",
    "MERGE (m:M {id: 'merged'}) SET m.v = 18",
    // Integer keys on a second indexed label.
    "MATCH (n:N {key: 3}) SET n.v = 1",
    "MATCH (n:N) WHERE n.key IN [4, 5, 99] SET n.v = 2",
    "MATCH (a:N {key: 1}), (b:M {id: 'k18'}) CREATE (a)-[:R {w: 4}]->(b)",
    "MATCH (n:N {key: 6}) DETACH DELETE n",
    "MATCH (m:M {id: 'new'}) DETACH DELETE m",
    // Deleting an endpoint removes its edges in both graphs.
    "MATCH (m:M {id: 'k12'}) DETACH DELETE m",
];

fn populate(path: &std::path::Path, indexed: bool, checkpoint: bool) {
    let mut db = Database::open(path).unwrap();
    if indexed {
        db.query("CREATE INDEX ON :M(id)").unwrap();
        db.query("CREATE INDEX ON :N(key)").unwrap();
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
    for key in 0..8 {
        db.query(&format!("CREATE (:N {{key: {key}, v: 0}})"))
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

/// Every `:M` and `:N` node with its internal id and all properties, then
/// every relationship with its id, endpoints and properties.
fn graph(db: &mut Database) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for query in [
        "MATCH (m:M) RETURN id(m) AS node, m.id AS id, m.state AS state, m.v AS v, m.body AS body \
         ORDER BY node",
        "MATCH (n:N) RETURN id(n) AS node, n.key AS key, n.v AS v ORDER BY node",
        "MATCH (a)-[r:R]->(b) RETURN id(r) AS rel, id(a) AS source, id(b) AS target, r.w AS w \
         ORDER BY rel",
    ] {
        rows.extend(
            db.query(query)
                .unwrap()
                .rows
                .into_rows()
                .into_iter()
                .map(|row| row.values().cloned().collect::<Vec<_>>()),
        );
    }
    rows
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
    // k11->k12 (merged once), fresh->k13, k15->k16, k15->k19, k20->k16 and
    // N1->k18, minus k11->k12 removed with k12.
    let edges = db
        .query("MATCH ()-[r:R]->() RETURN count(r) AS n")
        .unwrap()
        .rows
        .get(0, "n")
        .map(|value| value.to_owned_value());
    assert_eq!(edges, Some(Value::Int(5)), "{context}: edges");
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
                // Indexed writes never scan the label. An out-of-core base whose
                // projection does not serve the index yet scans the canonical
                // base by property until a checkpoint publishes it (#2019).
                assert_eq!(index_scans, 0, "{context}: indexed writes never scan");
                if residency == StorageResidencyMode::Materialized || checkpoint {
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

const HOT: usize = 64;

/// A database whose `:M(id)` key `hot` is shared by `HOT` nodes, checkpointed
/// so an out-of-core reopen leaves them in the canonical base.
fn populate_hot(path: &std::path::Path, indexed: bool) {
    let mut db = Database::open(path).unwrap();
    if indexed {
        db.query("CREATE INDEX ON :M(id)").unwrap();
    }
    for _ in 0..HOT {
        db.query("CREATE (:M {id: 'hot', v: 0})").unwrap();
    }
    db.query("CREATE (:M {id: 'cold', v: 0})").unwrap();
    db.checkpoint().unwrap();
}

fn open_limited(
    path: &std::path::Path,
    residency: StorageResidencyMode,
    max_affected_rows: usize,
) -> Database {
    let mut config = DatabaseConfig {
        storage_residency_mode: residency,
        automatic_checkpoint_max_age: std::time::Duration::from_secs(3600),
        ..DatabaseConfig::default()
    };
    config.mutation_limits.max_affected_rows =
        std::num::NonZeroUsize::new(max_affected_rows).unwrap();
    Database::open_with_config(path, config).unwrap()
}

fn index_candidates(db: &Database) -> u64 {
    db.storage_residency_report()
        .unwrap()
        .graph_index_reads
        .mutation_target_index_candidates
}

/// The affected-row cap bounds the seek like it bounds the scan: a
/// high-fanout key under a small limit stops at the first excess match
/// instead of collecting (and, out of core, hydrating) the posting list.
#[test]
fn indexed_seek_stops_at_the_first_match_over_the_row_limit() {
    for residency in [
        StorageResidencyMode::OutOfCore,
        StorageResidencyMode::Materialized,
    ] {
        let indexed_path = unique_test_dir("mutation_index_seek_limit");
        let scanned_path = unique_test_dir("mutation_index_seek_limit_scan");
        populate_hot(&indexed_path, true);
        populate_hot(&scanned_path, false);
        for limit in [1, 2, HOT - 1] {
            let context = format!("{residency:?} limit={limit}");
            let mut indexed = open_limited(&indexed_path, residency, limit);
            let mut scanned = open_limited(&scanned_path, residency, limit);
            for transactional in [false, true] {
                let statement = "MATCH (m:M {id: 'hot'}) SET m.v = 1";
                let (_, seeks) = mutation_targets(&indexed);
                let before = index_candidates(&indexed);
                let error = if transactional {
                    let mut transaction = indexed.begin_transaction().unwrap();
                    transaction.query(statement).unwrap_err()
                } else {
                    indexed.query(statement).unwrap_err()
                };
                let examined = index_candidates(&indexed) - before;
                assert_eq!(mutation_targets(&indexed).1, seeks + 1, "{context}");
                assert!(
                    examined <= limit as u64 + 1,
                    "{context} transactional={transactional}: seek examined \
                     {examined} candidates for limit {limit}"
                );
                let scan_error = scanned.query(statement).unwrap_err();
                assert_eq!(error.to_string(), scan_error.to_string(), "{context}");
                assert!(
                    error
                        .to_string()
                        .contains(&format!("max_mutation_affected_rows {limit}")),
                    "{context}: {error}"
                );
            }
            // The refused writes left every node unchanged.
            let rows = indexed
                .query("MATCH (m:M) WHERE m.v <> 0 RETURN count(m) AS changed")
                .unwrap();
            assert_eq!(
                rows.rows.into_rows()[0]
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![Value::Int(0)],
                "{context}"
            );
        }
        // A limit equal to the number of matches is not exceeded.
        let mut indexed = open_limited(&indexed_path, residency, HOT);
        indexed
            .query("MATCH (m:M {id: 'hot'}) SET m.v = 1")
            .unwrap();
        let rows = indexed
            .query("MATCH (m:M {v: 1}) RETURN count(m) AS changed")
            .unwrap();
        assert_eq!(
            rows.rows.into_rows()[0]
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            vec![Value::Int(HOT as i64)],
            "{residency:?}"
        );
        drop(indexed);
        let _ = std::fs::remove_dir_all(&indexed_path);
        let _ = std::fs::remove_dir_all(&scanned_path);
    }
}
