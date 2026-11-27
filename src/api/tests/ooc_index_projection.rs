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

//! Out-of-core property indexes (#2019): the automatic checkpoint builds the
//! persistent property projection for an index it does not serve yet, the
//! residency report names unserved indexes, and until then mutation targets
//! scan the canonical base by property instead of the label. Indexed results
//! always equal an unindexed twin's.

use super::*;
use crate::store::StorageResidencyMode;
use hawdb_storage::property_projection::PersistentPropertyProjectionKind;
use std::time::{Duration, Instant};

fn config(background: bool) -> DatabaseConfig {
    let mut config = DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        // Only the projection trigger, never the age trigger, checkpoints.
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        ..DatabaseConfig::default()
    };
    if !background {
        config.runtime_capabilities = config
            .runtime_capabilities
            .with(hawdb_core::RuntimeCapability::BackgroundMaintenance, false);
    }
    config
}

fn create_memories(db: &mut Database, ids: std::ops::Range<i64>) {
    let rows = ids
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".to_string(), Value::Int(id)),
                ("k".to_string(), Value::String(format!("k{}", id % 150))),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Memory {id: r.id, k: r.k, v: 0})",
        &BTreeMap::from([("rows".to_string(), Value::List(rows))]),
    )
    .unwrap();
}

/// Label scans, index seeks and canonical property scans by mutation targets.
fn targets(db: &Database) -> (u64, u64, u64) {
    let reads = db.storage_residency_report().unwrap().graph_index_reads;
    (
        reads.mutation_target_label_scans,
        reads.mutation_target_index_seeks,
        reads.mutation_target_property_scans,
    )
}

fn unserved(db: &Database) -> Vec<crate::UnservedGraphPropertyIndex> {
    db.storage_residency_report()
        .unwrap()
        .unserved_graph_property_indexes
}

fn completed(db: &Database) -> u64 {
    db.automatic_checkpoint_report()
        .unwrap()
        .map_or(0, |report| report.completed_checkpoints)
}

/// A checkpointed out-of-core base of 300 `:Memory` nodes, then `CREATE INDEX`.
fn indexed_base(path: &std::path::Path, background: bool) -> Database {
    let mut db = Database::open_with_config(path, config(background)).unwrap();
    create_memories(&mut db, 0..300);
    db.checkpoint().unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    db.query("CREATE INDEX ON :Memory(k)").unwrap();
    db
}

fn set_k(db: &mut Database, key: &str) {
    db.query_with_params(
        "MATCH (m:Memory {k: $k}) SET m.v = m.v + 1",
        &BTreeMap::from([("k".to_string(), Value::String(key.into()))]),
    )
    .unwrap();
}

#[test]
fn automatic_checkpoint_builds_the_projection_for_a_new_index() {
    let path = unique_test_dir("ooc_index_projection_automatic");
    let mut db = indexed_base(&path, true);
    assert_eq!(
        unserved(&db),
        vec![crate::UnservedGraphPropertyIndex {
            label: "Memory".into(),
            properties: vec!["k".into()],
            kind: PersistentPropertyProjectionKind::Equality,
        }]
    );
    // The index commit alone makes the owner checkpoint: no explicit
    // checkpoint, no age trigger (one hour), no reopen.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !unserved(&db).is_empty() {
        assert!(
            Instant::now() < deadline,
            "projection not built: {:?}",
            db.automatic_checkpoint_report()
        );
        std::thread::sleep(Duration::from_millis(20));
        // A write adopts the published generation.
        db.query("CREATE (:Probe)").unwrap();
    }
    assert!(completed(&db) >= 1);
    let report = db.storage_residency_report().unwrap();
    assert!(report.persistent_property_projection_artifact_bytes > 24);
    assert_eq!(report.persistent_property_projection_budget_refusals, 0);
    let before = targets(&db);
    set_k(&mut db, "k7");
    let after = targets(&db);
    assert_eq!(
        (after.0, after.1, after.2),
        (before.0, before.1 + 1, before.2)
    );
    drop(db);

    let mut reopened = Database::open_with_config(&path, config(true)).unwrap();
    assert!(unserved(&reopened).is_empty());
    let before = targets(&reopened);
    set_k(&mut reopened, "k8");
    assert_eq!(targets(&reopened).1, before.1 + 1);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn uncovered_index_mutations_scan_the_canonical_base_by_property() {
    let path = unique_test_dir("ooc_index_projection_uncovered");
    let mut db = indexed_base(&path, false);
    assert_eq!(unserved(&db).len(), 1);
    let before = targets(&db);
    set_k(&mut db, "k7");
    let after = targets(&db);
    // Neither a label scan nor a projection seek: a canonical property scan.
    assert_eq!(
        (after.0, after.1, after.2),
        (before.0, before.1, before.2 + 1)
    );
    let changed = db
        .query("MATCH (m:Memory) WHERE m.v = 1 RETURN m.id AS id ORDER BY id")
        .unwrap()
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    assert_eq!(changed, vec![vec![Value::Int(7)], vec![Value::Int(157)]]);
    // The explicit checkpoint serves it; seeks replace property scans.
    db.checkpoint().unwrap();
    assert!(unserved(&db).is_empty());
    let before = targets(&db);
    set_k(&mut db, "k7");
    assert_eq!(targets(&db), (before.0, before.1 + 1, before.2));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn uncovered_index_property_scan_stops_at_the_first_match_over_the_row_limit() {
    let path = unique_test_dir("ooc_index_projection_limit");
    {
        let mut db = Database::open_with_config(&path, config(false)).unwrap();
        for _ in 0..12 {
            db.query("CREATE (:Memory {k: 'hot', v: 0})").unwrap();
        }
        db.query("CREATE (:Memory {k: 'cold', v: 0})").unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE INDEX ON :Memory(k)").unwrap();
    }
    for limit in [1_usize, 3] {
        let mut limited = config(false);
        limited.mutation_limits.max_affected_rows = std::num::NonZeroUsize::new(limit).unwrap();
        let mut db = Database::open_with_config(&path, limited).unwrap();
        assert_eq!(unserved(&db).len(), 1, "limit={limit}");
        let before = targets(&db);
        let candidates = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .mutation_target_index_candidates;
        let error = db
            .query("MATCH (m:Memory {k: 'hot'}) SET m.v = 1")
            .unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "mutation would exceed max_mutation_affected_rows {limit}"
            )),
            "{error}"
        );
        let examined = db
            .storage_residency_report()
            .unwrap()
            .graph_index_reads
            .mutation_target_index_candidates
            - candidates;
        assert!(examined <= limit as u64 + 1, "limit={limit}: {examined}");
        let after = targets(&db);
        assert_eq!(
            (after.0, after.2),
            (before.0, before.2 + 1),
            "limit={limit}"
        );
        assert_eq!(
            db.query("MATCH (m:Memory) WHERE m.v <> 0 RETURN count(m) AS c")
                .unwrap()
                .rows[0]["c"],
            Value::Int(0)
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// Every `:Memory` node's internal id, `id`, `k` and `v`.
fn memories(db: &mut Database) -> Vec<Vec<Value>> {
    db.query("MATCH (m:Memory) RETURN id(m) AS n, m.id AS id, m.k AS k, m.v AS v ORDER BY n")
        .unwrap()
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

fn lookup(db: &mut Database, key: &str) -> Vec<Vec<Value>> {
    db.query_with_params(
        "MATCH (m:Memory {k: $k}) RETURN id(m) AS n, m.v AS v ORDER BY n",
        &BTreeMap::from([("k".to_string(), Value::String(key.into()))]),
    )
    .unwrap()
    .rows
    .into_rows()
    .into_iter()
    .map(|row| row.values().cloned().collect())
    .collect()
}

#[test]
fn indexed_lookups_and_writes_match_an_unindexed_twin_before_and_after_the_projection() {
    const KEYS: &[&str] = &[
        "k0", "k3", "k5", "k7", "k9", "k10", "k149", "new", "missing",
    ];
    let indexed_path = unique_test_dir("ooc_index_projection_twin_indexed");
    let plain_path = unique_test_dir("ooc_index_projection_twin_plain");
    let mut indexed = indexed_base(&indexed_path, false);
    let mut plain = Database::open_with_config(&plain_path, config(false)).unwrap();
    create_memories(&mut plain, 0..300);
    plain.checkpoint().unwrap();
    for phase in ["uncovered", "covered"] {
        // Delta writes after the last checkpoint: new nodes (one sharing a
        // base key), a re-keyed base node, deleted base nodes and an update.
        for db in [&mut indexed, &mut plain] {
            db.query("CREATE (:Memory {id: 1000, k: 'k10', v: 0})")
                .unwrap();
            db.query("CREATE (:Memory {id: 1001, k: 'new', v: 0})")
                .unwrap();
            db.query("MATCH (m:Memory {id: 5}) SET m.k = 'k7'").unwrap();
            db.query("MATCH (m:Memory {k: 'k9'}) DETACH DELETE m")
                .unwrap();
            db.query("MATCH (m:Memory {id: 3}) SET m.v = 10").unwrap();
        }
        let before = targets(&indexed);
        for key in KEYS {
            assert_eq!(
                lookup(&mut indexed, key),
                lookup(&mut plain, key),
                "{phase} {key}"
            );
            set_k(&mut indexed, key);
            set_k(&mut plain, key);
        }
        assert_eq!(memories(&mut indexed), memories(&mut plain), "{phase}");
        let after = targets(&indexed);
        assert_eq!(
            after.0, before.0,
            "{phase}: indexed writes never scan the label"
        );
        if phase == "uncovered" {
            assert!(after.2 > before.2, "{phase}: canonical property scans");
            indexed.checkpoint().unwrap();
            plain.checkpoint().unwrap();
            assert!(unserved(&indexed).is_empty());
        } else {
            assert_eq!(after.2, before.2, "{phase}: no property scans");
            assert!(after.1 > before.1, "{phase}: projection seeks");
        }
    }
    drop(indexed);
    drop(plain);
    std::fs::remove_dir_all(indexed_path).unwrap();
    std::fs::remove_dir_all(plain_path).unwrap();
}

#[test]
fn resident_stores_report_no_unserved_indexes() {
    let path = unique_test_dir("ooc_index_projection_resident");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::Materialized,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    create_memories(&mut db, 0..10);
    db.query("CREATE INDEX ON :Memory(k)").unwrap();
    assert!(unserved(&db).is_empty());
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

/// A canonical node materialized into the delta by a write to another
/// property stays findable through the index, with a projection serving it.
#[test]
fn materialized_base_node_is_found_by_its_indexed_value() {
    let path = unique_test_dir("ooc_index_projection_materialized");
    let mut db = indexed_base(&path, false);
    db.checkpoint().unwrap();
    assert!(unserved(&db).is_empty());
    // Materializes canonical node id 3 (k3) without touching `k`.
    db.query("MATCH (m:Memory {id: 3}) SET m.v = 10").unwrap();
    let before = targets(&db);
    set_k(&mut db, "k3");
    assert_eq!(targets(&db).1, before.1 + 1);
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.k = 'k3' RETURN m.id AS id, m.v AS v ORDER BY id")
            .unwrap()
            .rows
            .into_rows()
            .into_iter()
            .map(|row| row.values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int(3), Value::Int(11)],
            vec![Value::Int(153), Value::Int(1)],
        ]
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

/// MERGE by an indexed key finds a canonical node materialized into the delta
/// by a write to another property instead of creating a duplicate.
#[test]
fn merge_finds_a_materialized_base_node_by_its_indexed_value() {
    let path = unique_test_dir("ooc_index_projection_merge");
    let mut db = Database::open_with_config(&path, config(false)).unwrap();
    create_memories(&mut db, 0..300);
    db.checkpoint().unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    db.checkpoint().unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    assert!(unserved(&db).is_empty());
    // Materializes canonical node id 7 without touching `id`.
    db.query("MATCH (m:Memory {id: 7}) SET m.v = 10").unwrap();
    db.query("MERGE (m:Memory {id: 7}) SET m.v = m.v + 1")
        .unwrap();
    let rows = |db: &mut Database| {
        db.query("MATCH (m:Memory) WHERE m.id = 7 RETURN m.v AS v")
            .unwrap()
            .rows
            .into_rows()
            .into_iter()
            .map(|row| row.values().cloned().collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(rows(&mut db), vec![vec![Value::Int(11)]]);
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(&path, config(false)).unwrap();
    assert_eq!(rows(&mut db), vec![vec![Value::Int(11)]]);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

/// A projection build refused by its budget is reported, the index stays
/// listed as unserved, and a later automatic checkpoint (here the age
/// trigger) still builds the projection.
#[test]
fn refused_projection_build_is_retried_by_a_later_automatic_checkpoint() {
    let path = unique_test_dir("ooc_index_projection_refused");
    let mut db = Database::open_with_config(&path, config(false)).unwrap();
    create_memories(&mut db, 0..300);
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_secs(2),
            ..config(true)
        },
    )
    .unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    db.runtime
        .get()
        .unwrap()
        .store
        .force_property_projection_budget_refusals(1);
    db.query("CREATE INDEX ON :Memory(k)").unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seen_refusal = false;
    loop {
        let report = db.storage_residency_report().unwrap();
        if report.persistent_property_projection_budget_refusals > 0 {
            seen_refusal = true;
        }
        if seen_refusal && report.unserved_graph_property_indexes.is_empty() {
            assert_eq!(report.persistent_property_projection_budget_refusals, 1);
            assert!(
                report
                    .last_persistent_property_projection_budget_refusal
                    .as_deref()
                    .is_some_and(|reason| reason.contains("definitions")),
                "{report:?}"
            );
            assert!(
                report.persistent_property_projection_artifact_bytes > 24,
                "{report:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "projection not rebuilt after a refusal: {report:?}, {:?}",
            db.automatic_checkpoint_report().unwrap()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let before = targets(&db);
    set_k(&mut db, "k3");
    assert_eq!(targets(&db).1, before.1 + 1);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
