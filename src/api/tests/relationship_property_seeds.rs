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

//! A single-hop pattern whose source is a plain label scan and whose
//! relationship carries property equalities expands only the endpoints of the
//! matching relationships from the relationship property index (#1548).

use super::*;

fn database() -> Database {
    let mut db = Database::new();
    populate(&mut db);
    db
}

fn populate(db: &mut Database) {
    for id in 0..60_i64 {
        db.query_with_params(
            "CREATE (:Entity {id: $id})",
            &BTreeMap::from([("id".into(), Value::Int(id))]),
        )
        .unwrap();
    }
    for id in 0..20_i64 {
        db.query_with_params(
            "CREATE (:Other {id: $id})",
            &BTreeMap::from([("id".into(), Value::Int(id))]),
        )
        .unwrap();
    }
    let mut create = |source: &str, target: &str, rel: &str, from: i64, to: i64, offset: i64| {
        db.query_with_params(
            &format!(
                "MATCH (a:{source} {{id: $from}}), (b:{target} {{id: $to}}) \
                 CREATE (a)-[:{rel} {{table: 7, offset: $offset, kind: $kind}}]->(b)"
            ),
            &BTreeMap::from([
                ("from".into(), Value::Int(from)),
                ("to".into(), Value::Int(to)),
                ("offset".into(), Value::Int(offset)),
                ("kind".into(), Value::Int(offset % 3)),
            ]),
        )
        .unwrap();
    };
    for offset in 0..90_i64 {
        create(
            "Entity",
            "Entity",
            "RELATES_TO",
            offset % 60,
            (offset * 7 + 1) % 60,
            offset,
        );
    }
    // Same properties on the wrong source label, target label and type.
    for offset in [3_i64, 4, 5] {
        create("Other", "Entity", "RELATES_TO", offset, offset, offset);
        create("Entity", "Other", "RELATES_TO", offset, offset, offset);
        create("Entity", "Entity", "LINK", offset, offset + 1, offset);
    }
}

fn rows(db: &mut Database, query: &str) -> Vec<Vec<Value>> {
    db.query(query)
        .unwrap()
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

#[test]
fn relationship_property_seeds_match_the_node_first_expand_in_order() {
    let mut db = database();
    for pattern in [
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity)",
        "(s:Entity)<-[r:RELATES_TO {table: 7, offset: 4}]-(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]-(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {kind: 1}]->(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7, offset: 999}]->(t:Entity)",
        "(s:Entity)-[r:RELATES_TO {table: 7}]->(t:Entity)",
        "(s:Other)-[r:RELATES_TO {offset: 5}]->(t:Entity)",
    ] {
        for tail in ["", " LIMIT 2"] {
            let seeded = format!("MATCH {pattern} RETURN s.id, r.offset, t.id{tail}");
            // Binding the source first keeps the node-first expand.
            let reference = format!(
                "MATCH (s:{}) WITH s MATCH {pattern} RETURN s.id, r.offset, t.id{tail}",
                if pattern.starts_with("(s:Other)") {
                    "Other"
                } else {
                    "Entity"
                }
            );
            assert_eq!(
                rows(&mut db, &seeded),
                rows(&mut db, &reference),
                "{seeded}"
            );
            let reference_plan = explain(&mut db, &reference);
            assert!(
                !reference_plan.contains("RelationshipSeededNodeScan"),
                "{reference_plan}"
            );
        }
    }
}

fn explain(db: &mut Database, query: &str) -> String {
    let explain = db.query(&format!("EXPLAIN {query}")).unwrap();
    let Value::String(plan) = &explain.rows[0]["plan"] else {
        panic!("missing plan")
    };
    plan.clone()
}

#[test]
fn relationship_property_seeds_expand_only_matching_sources() {
    let mut db = database();
    let plan = explain(
        &mut db,
        "MATCH (s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity) RETURN s.id, t.id",
    );
    assert!(
        plan.contains("RelationshipSeededNodeScan variable=s label=Entity rel=RELATES_TO"),
        "{plan}"
    );
    let output = db
        .explain_analyze_query_with_params(
            "MATCH (s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity) RETURN s.id, t.id",
            &BTreeMap::new(),
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    // The seeds are the sources of the three `offset: 4` relationships of
    // this type: `Entity` 4 (twice) and `Other` 4, which the label check then
    // drops. The scan would have visited all 60 `Entity` nodes.
    let reports = &output.execution_profile.scan_pruning_reports;
    let seeded = reports
        .iter()
        .filter(|report| report.target_kind == hawdb_storage::scan::ScanPruningTargetKind::Node)
        .collect::<Vec<_>>();
    assert_eq!(seeded.len(), 1, "{reports:?}");
    assert_eq!(seeded[0].candidate_count_before_pruning, 60);
    assert_eq!(seeded[0].candidate_count_before_filter, 2);
    // A bound source keeps the node-first expand and records no seeding.
    let output = db
        .explain_analyze_query_with_params(
            "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity) RETURN s.id",
            &BTreeMap::new(),
        )
        .unwrap();
    assert_eq!(output.output.rows.len(), 1);
    assert!(output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .all(|report| report.candidate_count_before_pruning != 60
            || report.candidate_count_before_filter != 1));
}

/// A checkpointed out-of-core store has no resident relationship index to
/// seed from, so the expand keeps the node-first path with equal results.
#[test]
fn relationship_property_seeds_fall_back_without_a_resident_index() {
    let path = unique_test_dir("relationship_property_seeds");
    let expected = {
        let mut db = Database::open(&path).unwrap();
        let mut source = database();
        let source_rows = rows(
            &mut source,
            "MATCH (s:Entity)-[r:RELATES_TO]->(t:Entity) RETURN s.id, r.offset, t.id",
        );
        for row in &source_rows {
            let parameters = BTreeMap::from([
                ("s".into(), row[0].clone()),
                ("o".into(), row[1].clone()),
                ("t".into(), row[2].clone()),
            ]);
            for endpoint in ["s", "t"] {
                db.query_with_params(&format!("MERGE (:Entity {{id: ${endpoint}}})"), &parameters)
                    .unwrap();
            }
            db.query_with_params(
                "MATCH (a:Entity {id: $s}), (b:Entity {id: $t}) \
                 CREATE (a)-[:RELATES_TO {table: 7, offset: $o}]->(b)",
                &parameters,
            )
            .unwrap();
        }
        db.checkpoint().unwrap();
        rows(
            &mut db,
            "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity) \
             RETURN s.id, r.offset, t.id",
        )
    };
    assert!(!expected.is_empty());
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let query = "MATCH (s:Entity)-[r:RELATES_TO {table: 7, offset: 4}]->(t:Entity) RETURN s.id, r.offset, t.id";
    assert_eq!(rows(&mut db, query), expected);
    let output = db
        .explain_analyze_query_with_params(query, &BTreeMap::new())
        .unwrap();
    // The node-first path scans the whole label; nothing was seeded.
    let reports = &output.execution_profile.scan_pruning_reports;
    assert!(
        reports.iter().any(|report| report.target_kind
            == hawdb_storage::scan::ScanPruningTargetKind::Node
            && !report.pruned),
        "{reports:?}"
    );
    assert!(
        reports.iter().all(|report| report.target_kind
            != hawdb_storage::scan::ScanPruningTargetKind::Node
            || report.rel_type_id.is_none()),
        "{reports:?}"
    );
}

const SEEDED: &str =
    "MATCH (s:Entity)-[r:RELATES_TO]->(t:Entity) WHERE r.table = $t AND r.offset = $o \
                      RETURN s.id, r.offset, t.id";
const NODE_FIRST: &str = "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO]->(t:Entity) \
                          WHERE r.table = $t AND r.offset = $o RETURN s.id, r.offset, t.id";

fn occurrence(t: i64, o: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("t".into(), Value::Int(t)), ("o".into(), Value::Int(o))])
}

fn values(output: QueryOutput) -> Vec<Vec<Value>> {
    output
        .rows
        .into_rows()
        .into_iter()
        .map(|row| row.values().cloned().collect())
        .collect()
}

/// Mem's parameterized WHERE form seeds, and a cached plan rebinds the
/// relationship properties for every parameter set.
#[test]
fn parameterized_conjuncts_seed_and_rebind_cached_plans() {
    let mut db = database();
    let explain = db
        .query_with_params(&format!("EXPLAIN {SEEDED}"), &occurrence(7, 4))
        .unwrap();
    let Value::String(plan) = &explain.rows[0]["plan"] else {
        panic!("missing plan")
    };
    assert!(plan.contains("RelationshipSeededNodeScan"), "{plan}");
    for (t, o) in [(7, 4), (7, 11), (7, 999), (8, 4), (7, 89), (7, 4)] {
        let parameters = occurrence(t, o);
        let seeded = values(db.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(db.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "{t}:{o}");
    }
}

/// Uncommitted creates, deletes and updates are visible to the seeded scan
/// inside the writing transaction, and a pinned snapshot keeps its view.
#[test]
fn transaction_writes_and_snapshots_match_the_node_first_expand() {
    let mut db = database();
    let mut snapshot = db.begin_read_transaction().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for write in [
        "MATCH (a:Entity {id: 1}), (b:Entity {id: 2}) CREATE (a)-[:RELATES_TO {table: 7, offset: 500}]->(b)",
        "MATCH (a:Entity)-[r:RELATES_TO {offset: 4}]->(b:Entity) DELETE r",
        "MATCH (a:Entity)-[r:RELATES_TO {offset: 6}]->(b:Entity) SET r.offset = 600",
    ] {
        transaction.query(write).unwrap();
    }
    for offset in [500, 4, 6, 600, 5] {
        let parameters = occurrence(7, offset);
        let seeded = values(transaction.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(
            transaction
                .query_with_params(NODE_FIRST, &parameters)
                .unwrap(),
        );
        assert_eq!(seeded, node_first, "transaction offset {offset}");
        assert_eq!(
            seeded.is_empty(),
            offset == 4 || offset == 6,
            "transaction offset {offset}"
        );
    }
    transaction.commit().unwrap();
    for offset in [500, 4, 6, 600] {
        let parameters = occurrence(7, offset);
        let seeded = values(snapshot.query_with_params(SEEDED, &parameters).unwrap());
        let node_first = values(snapshot.query_with_params(NODE_FIRST, &parameters).unwrap());
        assert_eq!(seeded, node_first, "snapshot offset {offset}");
        assert_eq!(
            seeded.is_empty(),
            offset == 500 || offset == 600,
            "snapshot offset {offset}"
        );
    }
}

/// An undirected self-loop is returned once per traversal direction, as the
/// node-first expand returns it, and a label union keeps the label scan.
#[test]
fn undirected_self_loops_and_label_unions_match_the_node_first_expand() {
    let mut db = database();
    db.query(
        "MATCH (a:Entity {id: 50}), (b:Entity {id: 50}) \
         CREATE (a)-[:RELATES_TO {table: 7, offset: 777}]->(b)",
    )
    .unwrap();
    for (seeded, node_first) in [
        (
            "MATCH (s:Entity)-[r:RELATES_TO {offset: 777}]-(t:Entity) RETURN s.id, t.id",
            "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO {offset: 777}]-(t:Entity) RETURN s.id, t.id",
        ),
        (
            "MATCH (s:Entity:Other)-[r:RELATES_TO {offset: 5}]->(t:Entity) RETURN s.id, t.id",
            "MATCH (s:Entity:Other) WITH s MATCH (s)-[r:RELATES_TO {offset: 5}]->(t:Entity) RETURN s.id, t.id",
        ),
    ] {
        let expected = rows(&mut db, node_first);
        assert!(!expected.is_empty(), "{node_first}");
        assert_eq!(rows(&mut db, seeded), expected, "{seeded}");
    }
}

/// Under a tight query and batch budget the seeded scan falls back or emits
/// smaller batches; it never fails where the node-first expand succeeds.
#[test]
fn tight_memory_budgets_match_the_node_first_expand() {
    for (query_bytes, batch_bytes) in [(65_536, 4_096), (24_576, 1_024), (16_384, 512)] {
        let mut db = Database::new_with_config(DatabaseConfig {
            execution_memory: crate::executor::ExecutionMemoryConfig {
                query_memory_bytes: std::num::NonZeroUsize::new(query_bytes).unwrap(),
                batch_rows: std::num::NonZeroUsize::new(2).unwrap(),
                batch_payload_bytes: std::num::NonZeroUsize::new(batch_bytes).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        });
        populate(&mut db);
        for parameters in [occurrence(7, 4), occurrence(7, 30)] {
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
    // A wide-label scan of many seeds under a small batch budget.
    let mut db = Database::new_with_config(DatabaseConfig {
        execution_memory: crate::executor::ExecutionMemoryConfig {
            batch_rows: std::num::NonZeroUsize::new(64).unwrap(),
            batch_payload_bytes: std::num::NonZeroUsize::new(2_048).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    });
    populate(&mut db);
    let seeded = rows(
        &mut db,
        "MATCH (s:Entity)-[r:RELATES_TO {table: 7}]->(t:Entity) RETURN s.id, r.offset",
    );
    let node_first = rows(
        &mut db,
        "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO {table: 7}]->(t:Entity) RETURN s.id, r.offset",
    );
    assert_eq!(seeded, node_first);
}
