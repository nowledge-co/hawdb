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

use super::*;
use crate::StorageResidencyMode;

#[test]
fn mixed_transaction_compaction_preserves_one_batch_or_rollback_across_reopen() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        for commit in [false, true] {
            let path = unique_test_dir(&format!("transaction_compaction_{mode:?}_{commit}"));
            {
                let mut db = Database::open(&path).unwrap();
                db.query("CREATE (:Memory {id: 1, value: 10})-[:LINK {weight: 1}]->(:Memory {id: 2, value: 20})")
                    .unwrap();
                db.checkpoint().unwrap();
            }
            let config = DatabaseConfig {
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            };
            let wal_before = read_test_wal(&path).unwrap();
            let epoch;
            {
                let mut db = Database::open_with_config(&path, config.clone()).unwrap();
                assert_eq!(
                    db.storage_residency_report().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
                epoch = db.commit_epoch();
                let mut tx = db.begin_transaction();
                for statement in [
                    "MATCH (m:Memory {id: 1}) SET m.value = 11",
                    "CREATE (:Memory {id: 3, value: 1})",
                    "MATCH (m:Memory {id: 3}) SET m.value = 30",
                    "CREATE (:Memory {id: 4, value: 40})",
                    "MATCH (a:Memory {id: 3}), (b:Memory {id: 2}) CREATE (a)-[:LINK {weight: 2}]->(b)",
                    "MATCH (:Memory {id: 3})-[r:LINK]->(:Memory {id: 2}) SET r.weight = 5",
                    "MATCH (a:Memory {id: 4}), (b:Memory {id: 2}) CREATE (a)-[:LINK {weight: 4}]->(b)",
                    "MATCH (m:Memory {id: 4}) SET m.value = 41",
                    "MATCH (m:Memory {id: 4}) DETACH DELETE m",
                    "MATCH (:Memory {id: 1})-[r:LINK]->(:Memory {id: 2}) DELETE r",
                ] {
                    tx.query(statement).unwrap();
                }
                assert_eq!(
                    tx.query("MATCH (m:Memory {id: 4}) RETURN m.id AS id")
                        .unwrap()
                        .rows
                        .len(),
                    0
                );
                if commit {
                    tx.commit().unwrap();
                } else {
                    tx.rollback();
                }
                assert_eq!(db.commit_epoch(), epoch + u64::from(commit));
                assert_graph(&mut db, commit);
            }
            let wal_after = read_test_wal(&path).unwrap();
            if commit {
                let appended = wal_after.strip_prefix(&wal_before).unwrap();
                assert_eq!(appended.matches("\tbatch\t").count(), 1);
                for (operation, count) in [
                    ("create_node", 1),
                    ("set_node_property", 1),
                    ("create_rel", 1),
                    ("set_rel_property", 0),
                    ("delete_node", 0),
                    ("delete_rel", 1),
                ] {
                    assert_eq!(
                        appended.matches(operation).count(),
                        count,
                        "{mode:?} {operation}"
                    );
                }
            } else {
                assert_eq!(wal_after, wal_before);
            }
            {
                let mut reopened = Database::open_with_config(&path, config).unwrap();
                assert_eq!(
                    reopened.storage_residency_report().out_of_core,
                    mode == StorageResidencyMode::OutOfCore
                );
                assert_eq!(reopened.commit_epoch(), epoch + u64::from(commit));
                assert_graph(&mut reopened, commit);
            }
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}

fn assert_graph(db: &mut Database, committed: bool) {
    let nodes = db
        .query("MATCH (m:Memory) RETURN m.id AS id, m.value AS value ORDER BY m.id")
        .unwrap();
    let mut expected = vec![
        BTreeMap::from([
            ("id".into(), Value::Int(1)),
            ("value".into(), Value::Int(if committed { 11 } else { 10 })),
        ]),
        BTreeMap::from([
            ("id".into(), Value::Int(2)),
            ("value".into(), Value::Int(20)),
        ]),
    ];
    if committed {
        expected.push(BTreeMap::from([
            ("id".into(), Value::Int(3)),
            ("value".into(), Value::Int(30)),
        ]));
    }
    assert_eq!(nodes.rows, expected);
    let relationships = db.query("MATCH (s:Memory)-[r:LINK]->(t:Memory) RETURN s.id AS source, t.id AS target, r.weight AS weight").unwrap();
    assert_eq!(
        relationships.rows,
        vec![BTreeMap::from([
            ("source".into(), Value::Int(if committed { 3 } else { 1 })),
            ("target".into(), Value::Int(2)),
            ("weight".into(), Value::Int(if committed { 5 } else { 1 })),
        ])]
    );
}
