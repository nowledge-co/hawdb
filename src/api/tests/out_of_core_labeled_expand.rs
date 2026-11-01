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

//! Labeled expansion over an out-of-core canonical base must match the
//! materialized result, including base records shadowed by transaction state.

use super::*;
use crate::store::StorageResidencyMode;

const NODES: i64 = 240;

const QUERIES: [&str; 5] = [
    "MATCH (:Item)-[r:LINK]->(:Item) RETURN count(r) AS n",
    "MATCH (a:Item)-[r:LINK]->(b:Item) RETURN a.id AS a, r.w AS w, b.id AS b, b.name AS name ORDER BY a, w, b",
    "MATCH (a:Item)-[:LINK]->(b:Other) RETURN a.id AS a, b.id AS b ORDER BY a, b",
    "MATCH (a:Item)-[:LINK]->(b:Item) WHERE a.id < 40 RETURN count(b) AS n",
    "MATCH (a:Item {id: 1})-[:LINK*1..2]->(b:Item) RETURN count(DISTINCT b) AS n",
];

fn populate(path: &std::path::Path) {
    let mut db = Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::Materialized,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let nodes = (0..NODES)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("name".into(), Value::String(format!("node-{id}"))),
                ("other".into(), Value::Bool(id % 6 == 0)),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {id: r.id, name: r.name, other: r.other})",
        &BTreeMap::from([("rows".into(), Value::List(nodes))]),
    )
    .unwrap();
    // Relabel by recreating every sixth node as :Other with the same id.
    db.query("MATCH (n:Item) WHERE n.other = true DELETE n")
        .unwrap();
    let others = (0..NODES)
        .filter(|id| id % 6 == 0)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("name".into(), Value::String(format!("other-{id}"))),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Other {id: r.id, name: r.name})",
        &BTreeMap::from([("rows".into(), Value::List(others))]),
    )
    .unwrap();
    for source in 0..NODES {
        for step in 1..=3 {
            let target = (source * 7 + step * 53) % NODES;
            db.query_with_params(
                "MATCH (a {id: $a}), (b {id: $b}) CREATE (a)-[:LINK {w: $w}]->(b)",
                &BTreeMap::from([
                    ("a".into(), Value::Int(source)),
                    ("b".into(), Value::Int(target)),
                    ("w".into(), Value::Int(step)),
                ]),
            )
            .unwrap();
        }
    }
    db.checkpoint().unwrap();
}

fn open(path: &std::path::Path, residency: StorageResidencyMode) -> Database {
    Database::open_with_config(
        path,
        DatabaseConfig {
            storage_residency_mode: residency,
            ..DatabaseConfig::default()
        },
    )
    .unwrap()
}

/// Shadows base records: deletes a base target, updates another, and adds a
/// labeled and an unlabeled target, all in uncommitted transaction state.
const DELTAS: [&str; 6] = [
    "MATCH (n:Item {id: 53}) DETACH DELETE n",
    "MATCH (n:Item {id: 113}) SET n.name = 'updated'",
    "CREATE (:Item {id: 1000, name: 'new'})",
    "CREATE (:Other {id: 1001, name: 'new-other'})",
    "MATCH (a:Item {id: 1}), (b:Item {id: 1000}) CREATE (a)-[:LINK {w: 9}]->(b)",
    "MATCH (a:Item {id: 1}), (b:Other {id: 1001}) CREATE (a)-[:LINK {w: 8}]->(b)",
];

fn results(mut query: impl FnMut(&str) -> QueryOutput) -> Vec<Vec<BTreeMap<String, Value>>> {
    QUERIES
        .iter()
        .map(|cypher| query(cypher).rows.into_rows())
        .collect()
}

#[test]
fn out_of_core_labeled_expand_matches_materialized_with_transaction_deltas() {
    let path = unique_test_dir("out_of_core_labeled_expand");
    populate(&path);
    let mut expected = None;
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let mut db = open(&path, residency);
        let committed = results(|cypher| db.query(cypher).unwrap());
        assert!(!committed[1].is_empty());
        assert!(!committed[2].is_empty());
        let mut tx = db.begin_transaction().unwrap();
        for delta in DELTAS {
            tx.query(delta).unwrap();
        }
        let shadowed = results(|cypher| tx.query(cypher).unwrap());
        tx.rollback();
        let rolled_back = results(|cypher| db.query(cypher).unwrap());
        assert_eq!(rolled_back, committed, "{residency:?} rollback");
        assert_ne!(shadowed, committed, "{residency:?} deltas must be visible");
        let names = shadowed[1]
            .iter()
            .filter_map(|row| match row.get("name") {
                Some(Value::String(name)) => Some(name.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(names.contains(&"updated") && names.contains(&"new"));
        assert!(!names.contains(&"node-53") && !names.contains(&"new-other"));
        match &expected {
            None => expected = Some((committed, shadowed)),
            Some(expected) => assert_eq!(
                (&committed, &shadowed),
                (&expected.0, &expected.1),
                "out-of-core results differ from materialized"
            ),
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
