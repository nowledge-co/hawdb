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

//! MERGE by an indexed key on an out-of-core store finds a canonical node
//! that a write to another property moved into the delta (#2019).

use super::*;

fn values_for_id(db: &mut Database, id: i64) -> Vec<Vec<Value>> {
    db.query_with_params(
        "MATCH (m:Memory) WHERE m.id = $id RETURN m.v AS v ORDER BY v",
        &BTreeMap::from([("id".to_string(), Value::Int(id))]),
    )
    .unwrap()
    .rows
    .into_rows()
    .into_iter()
    .map(|row| row.values().cloned().collect::<Vec<_>>())
    .collect()
}

#[test]
fn merge_finds_a_materialized_base_node_by_its_indexed_value() {
    let path = unique_test_dir("materialized_index_merge");
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    let seed = (0..300)
        .map(|id| Value::Map(BTreeMap::from([("id".to_string(), Value::Int(id))])))
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Memory {id: r.id, v: 0})",
        &BTreeMap::from([("rows".to_string(), Value::List(seed))]),
    )
    .unwrap();
    db.checkpoint().unwrap();
    db.query("CREATE INDEX ON :Memory(id)").unwrap();
    // The checkpoint builds the persistent projection that serves the index.
    db.checkpoint().unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    // Materializes canonical node id 7 without touching `id`.
    db.query("MATCH (m:Memory {id: 7}) SET m.v = 10").unwrap();
    db.query("MERGE (m:Memory {id: 7}) SET m.v = m.v + 1")
        .unwrap();
    assert_eq!(values_for_id(&mut db, 7), vec![vec![Value::Int(11)]]);
    // A second materialized node, merged in the same transaction as its write.
    let mut tx = db.begin_transaction().unwrap();
    tx.query("MATCH (m:Memory {id: 8}) SET m.v = 5").unwrap();
    tx.query("MERGE (n:Memory {id: 8}) SET n.v = n.v + 1")
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(values_for_id(&mut db, 8), vec![vec![Value::Int(6)]]);
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(&path, config).unwrap();
    assert_eq!(values_for_id(&mut db, 7), vec![vec![Value::Int(11)]]);
    assert_eq!(values_for_id(&mut db, 8), vec![vec![Value::Int(6)]]);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
