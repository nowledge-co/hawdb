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

//! An out-of-core store seeds the source from the persisted relationship
//! equality projection of its canonical base, merged with the resident delta
//! (#1548). Every case is compared with the node-first expand.

use super::*;
use hawdb_storage::config::StorageResidencyMode;
use hawdb_storage::scan::{ScanPruningReport, ScanPruningStrategy, ScanPruningTargetKind};

fn out_of_core_config() -> DatabaseConfig {
    DatabaseConfig {
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    }
}

/// Populates, checkpoints and reopens `name` out of core.
fn reopened(name: &str, setup: impl FnOnce(&mut Database), config: DatabaseConfig) -> Database {
    let path = unique_test_dir(name);
    {
        let mut db = Database::open(&path).unwrap();
        setup(&mut db);
        db.checkpoint().unwrap();
    }
    let db = Database::open_with_config(&path, config).unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    db
}

/// The node scan-pruning report of a seeded source, if the source seeded.
fn seed_report(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Option<ScanPruningReport> {
    db.explain_analyze_query_with_params(query, parameters)
        .unwrap()
        .execution_profile
        .scan_pruning_reports
        .into_iter()
        .find(|report| {
            report.target_kind == ScanPruningTargetKind::Node && report.rel_type_id.is_some()
        })
}

fn relationship_equality_reads(db: &Database) -> u64 {
    db.storage_residency_report()
        .unwrap()
        .graph_index_reads
        .operation_count(
            hawdb_storage::graph_index_metrics::PersistentGraphIndexClass::RelationshipEquality,
        )
}

fn assert_seeded_matches_node_first(db: &mut Database, offsets: &[i64]) {
    for &offset in offsets {
        let parameters = occurrence(7, offset);
        let seeded = values(db.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(db.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "offset {offset}");
    }
}

/// The canonical base alone: every inline pattern and Mem's parameterized
/// form equal the node-first expand, and the seed is read from the persisted
/// projection and reported.
#[test]
fn canonical_base_seeds_from_the_persisted_projection() {
    let mut db = reopened(
        "relationship_seeds_ooc_base",
        populate,
        out_of_core_config(),
    );
    for pattern in [
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity)",
        "(s:Entity)<-[r:RELATES_TO {table: 7, offset: 4}]-(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]-(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {kind: 1}]->(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 999}]->(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7}]->(t:Entity)",
        "(s:Other)-[r:RELATES_TO {offset: 5}]->(t:Entity)",
    ] {
        let label = if pattern.starts_with("(s:Other)") {
            "Other"
        } else {
            "Entity"
        };
        for tail in ["", " LIMIT 2"] {
            let seeded = format!("MATCH {pattern} RETURN s.id, r.offset, t.id{tail}");
            let reference = format!(
                "MATCH (s:{label}) WITH s MATCH {pattern} RETURN s.id, r.offset, t.id{tail}"
            );
            assert_eq!(
                rows(&mut db, &seeded),
                rows(&mut db, &reference),
                "{seeded}"
            );
        }
    }
    assert_seeded_matches_node_first(&mut db, &[4, 11, 89, 999]);

    let reads = relationship_equality_reads(&db);
    let report = seed_report(&mut db, SEEDED, &occurrence(7, 4)).expect("the source seeds");
    assert!(relationship_equality_reads(&db) > reads);
    // The unique offset posting is probed; the shared table posting exceeds
    // the label. Entity 4 and the decoy Other 4 are the seeds.
    assert_eq!(
        report.strategy,
        ScanPruningStrategy::PropertyEq {
            property: "offset".into()
        }
    );
    assert!(report.pruned);
    assert_eq!(report.candidate_count_before_pruning, 60);
    assert_eq!(report.candidate_count_before_filter, 2);
    // A value no relationship carries seeds nothing.
    let report = seed_report(&mut db, SEEDED, &occurrence(7, 999)).expect("the source seeds");
    assert!(report.exact_empty);
}

const WRITES: [&str; 5] = [
    // A delta create.
    "MATCH (a:Entity {id: 1}), (b:Entity {id: 2}) CREATE (a)-[:RELATES_TO {table: 7, offset: 500}]->(b)",
    // A canonical relationship deleted (tombstoned).
    "MATCH (a:Entity)-[r:RELATES_TO {offset: 4}]->(b:Entity) DELETE r",
    // A canonical relationship moved out of offset 6 and into 600.
    "MATCH (a:Entity)-[r:RELATES_TO {offset: 6}]->(b:Entity) SET r.offset = 600",
    // A canonical relationship rewritten on another property keeps offset 7.
    "MATCH (a:Entity)-[r:RELATES_TO {offset: 7}]->(b:Entity) SET r.kind = 9",
    // A canonical relationship moved into an offset another one carries.
    "MATCH (a:Entity)-[r:RELATES_TO {offset: 8}]->(b:Entity) SET r.offset = 9",
];
const WRITTEN_OFFSETS: [i64; 9] = [500, 4, 6, 600, 7, 8, 9, 5, 999];

fn written_offset_matches(offset: i64) -> usize {
    match offset {
        4 | 6 | 8 | 999 => 0,
        9 => 2,
        _ => 1,
    }
}

/// Committed delta creates, deletes and updates merge with the canonical
/// base, and the delta index covers rewritten canonical relationships.
#[test]
fn committed_delta_writes_merge_with_the_canonical_base() {
    let mut db = reopened(
        "relationship_seeds_ooc_delta",
        populate,
        out_of_core_config(),
    );
    for write in WRITES {
        db.query(write).unwrap();
    }
    let consistency = db.property_index_consistency_report().unwrap();
    assert_eq!(
        consistency.missing_relationship_key_count, 0,
        "{consistency:?}"
    );
    assert_eq!(
        consistency.extra_relationship_key_count, 0,
        "{consistency:?}"
    );
    assert_eq!(
        consistency.mismatched_relationship_key_count, 0,
        "{consistency:?}"
    );
    for offset in WRITTEN_OFFSETS {
        let parameters = occurrence(7, offset);
        let seeded = values(db.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(db.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "offset {offset}");
        assert_eq!(
            seeded.len(),
            written_offset_matches(offset),
            "offset {offset}"
        );
        assert!(
            seed_report(&mut db, SEEDED, &parameters).is_some(),
            "offset {offset}"
        );
    }
    let rewritten = rows(
        &mut db,
        "MATCH (s:Entity)-[r:RELATES_TO {offset: 7, kind: 9}]->(t:Entity) RETURN s.id, t.id",
    );
    assert_eq!(rewritten.len(), 1);
    // An unfiltered expand still sees the rewritten canonical relationships.
    assert_eq!(
        rows(
            &mut db,
            "MATCH (s:Entity {id: 6})-[r:RELATES_TO]->(t:Entity) RETURN r.offset ORDER BY r.offset",
        ),
        vec![vec![Value::Int(66)], vec![Value::Int(600)]]
    );
}

/// Uncommitted writes are visible to the seeded scan inside the writing
/// transaction, and a snapshot pinned before them keeps its view.
#[test]
fn transaction_writes_and_pinned_snapshots_match_the_node_first_expand() {
    let mut db = reopened(
        "relationship_seeds_ooc_transaction",
        populate,
        out_of_core_config(),
    );
    let mut snapshot = db.begin_read_transaction().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for write in WRITES {
        transaction.query(write).unwrap();
    }
    for offset in WRITTEN_OFFSETS {
        let parameters = occurrence(7, offset);
        let seeded = values(transaction.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(
            transaction
                .query_with_params(NODE_FIRST, &parameters)
                .unwrap(),
        );
        assert_eq!(seeded, node_first, "transaction offset {offset}");
        assert_eq!(
            seeded.len(),
            written_offset_matches(offset),
            "offset {offset}"
        );
    }
    transaction.commit().unwrap();
    for offset in WRITTEN_OFFSETS {
        let parameters = occurrence(7, offset);
        let seeded = values(snapshot.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(snapshot.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "snapshot offset {offset}");
        let before = match offset {
            500 | 600 | 999 => 0,
            _ => 1,
        };
        assert_eq!(seeded.len(), before, "snapshot offset {offset}");
    }
    assert_seeded_matches_node_first(&mut db, &WRITTEN_OFFSETS);
}

/// Under tight query and batch budgets the out-of-core seed falls back or
/// emits smaller batches; it never fails where the node-first expand runs.
#[test]
fn tight_memory_budgets_match_the_node_first_expand_out_of_core() {
    for (index, (query_bytes, batch_bytes)) in [(65_536, 4_096), (24_576, 1_024), (16_384, 512)]
        .into_iter()
        .enumerate()
    {
        let mut db = reopened(
            &format!("relationship_seeds_ooc_budget_{index}"),
            populate,
            DatabaseConfig {
                execution_memory: crate::executor::ExecutionMemoryConfig {
                    query_memory_bytes: std::num::NonZeroUsize::new(query_bytes).unwrap(),
                    batch_rows: std::num::NonZeroUsize::new(2).unwrap(),
                    batch_payload_bytes: std::num::NonZeroUsize::new(batch_bytes).unwrap(),
                    ..Default::default()
                },
                ..out_of_core_config()
            },
        );
        db.query(WRITES[0]).unwrap();
        for parameters in [occurrence(7, 4), occurrence(7, 30), occurrence(7, 500)] {
            let seeded = db.query_with_params(SEEDED, &parameters).map(values);
            let node_first = db.query_with_params(NODE_FIRST, &parameters).map(values);
            match (seeded, node_first) {
                (Ok(seeded), Ok(node_first)) => assert_eq!(seeded, node_first),
                // Both refuse: the per-row ceiling applies to either plan.
                (Err(_), Err(_)) => {}
                (seeded, node_first) => panic!(
                    "budget {query_bytes}/{batch_bytes}: seeded {:?}, node-first {:?}",
                    seeded.map(|rows| rows.len()),
                    node_first.map(|rows| rows.len())
                ),
            }
        }
    }
}

/// An oversized key leaves the offset projection incomplete, and a property
/// first written after the checkpoint has no projection: both keep the label
/// scan with equal results.
#[test]
fn incomplete_and_missing_projections_fall_back_to_the_label_scan() {
    let mut incomplete = reopened(
        "relationship_seeds_ooc_incomplete",
        |db| {
            populate(db);
            db.query_with_params(
                "MATCH (a:Entity {id: 1}), (b:Entity {id: 2}) \
                 CREATE (a)-[:RELATES_TO {table: 7, offset: $offset}]->(b)",
                &BTreeMap::from([("offset".into(), Value::String("x".repeat(8 * 1024)))]),
            )
            .unwrap();
        },
        out_of_core_config(),
    );
    let mut missing = reopened(
        "relationship_seeds_ooc_missing",
        populate,
        out_of_core_config(),
    );
    for offset in 0..90_i64 {
        missing
            .query_with_params(
                "MATCH ()-[r:RELATES_TO {offset: $offset}]->() SET r.slot = $offset",
                &BTreeMap::from([("offset".into(), Value::Int(offset))]),
            )
            .unwrap();
    }
    for (db, property) in [(&mut incomplete, "offset"), (&mut missing, "slot")] {
        for value in [4_i64, 30, 999] {
            let seeded = format!(
                "MATCH (s:Entity)-[r:RELATES_TO {{{property}: {value}}}]->(t:Entity) RETURN s.id, t.id"
            );
            let reference = format!(
                "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO {{{property}: {value}}}]->(t:Entity) RETURN s.id, t.id"
            );
            assert_eq!(rows(db, &seeded), rows(db, &reference), "{seeded}");
            assert!(explain(db, &seeded).contains("RelationshipSeededNodeScan"));
            assert!(
                seed_report(db, &seeded, &BTreeMap::new()).is_none(),
                "{seeded}"
            );
        }
    }
    // The shared table posting exceeds the label, so Mem's form falls back
    // when its offset projection is incomplete.
    assert_seeded_matches_node_first(&mut incomplete, &[4, 30, 999]);
    assert!(seed_report(&mut incomplete, SEEDED, &occurrence(7, 4)).is_none());
}

/// An automatic-residency store whose records exceed the materialization
/// threshold becomes out of core at an in-process checkpoint and keeps
/// seeding, before and after later delta writes.
#[test]
fn auto_residency_crossing_the_threshold_in_process_keeps_seeding() {
    let path = unique_test_dir("relationship_seeds_ooc_auto");
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            // Above the empty store, below the populated one.
            auto_materialize_checkpoint_bytes: 4_096,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    populate(&mut db);
    assert!(!db.storage_residency_report().unwrap().out_of_core);
    assert!(seed_report(&mut db, SEEDED, &occurrence(7, 4)).is_some());
    db.checkpoint().unwrap();
    let residency = db.storage_residency_report().unwrap();
    assert!(residency.out_of_core && residency.canonical_artifact_bytes > 0);
    assert_seeded_matches_node_first(&mut db, &[4, 11, 89, 999]);
    let reads = relationship_equality_reads(&db);
    assert!(seed_report(&mut db, SEEDED, &occurrence(7, 4)).is_some());
    assert!(relationship_equality_reads(&db) > reads);
    for write in WRITES {
        db.query(write).unwrap();
    }
    for offset in WRITTEN_OFFSETS {
        let parameters = occurrence(7, offset);
        let seeded = values(db.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(db.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "offset {offset}");
        assert_eq!(
            seeded.len(),
            written_offset_matches(offset),
            "offset {offset}"
        );
        assert!(
            seed_report(&mut db, SEEDED, &parameters).is_some(),
            "offset {offset}"
        );
    }
}
