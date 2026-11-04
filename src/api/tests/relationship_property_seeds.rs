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
    db
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
