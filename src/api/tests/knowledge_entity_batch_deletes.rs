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

const DELETE: &str = "MATCH (m:Memory) WHERE m.id IN $ids DETACH DELETE m";
const READ: &str = "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id AS id ORDER BY id";

#[test]
fn deletes_knowledge_entity_batch_through_parameterized_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_2'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_3', source_id: 'thread_1'})")
        .unwrap();
    let parameters = batch_id_parameters(&["memory_2", "missing", "memory_1"]).unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let candidates = transaction
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap();
    assert_eq!(candidates.rows.len(), 2);
    assert_eq!(candidates.rows[0]["id"], Value::String("memory_1".into()));
    assert_eq!(candidates.rows[1]["id"], Value::String("memory_2".into()));
    transaction
        .query_request(bounded_batch_query(DELETE, &parameters))
        .unwrap();
    let after = transaction
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap();
    assert!(after.rows.is_empty());
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(before, 3);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    assert!(db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
    let parameters = batch_id_parameters(&["memory_3"]).unwrap();
    assert_eq!(
        db.query_request(bounded_batch_query(READ, &parameters))
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH ()-[r]->() RETURN count(r) AS total",
            &BTreeMap::new(),
        ))
        .unwrap()
        .rows[0]["total"],
        Value::Int(0)
    );
    assert_eq!(
        db.query_request(bounded_batch_query(
            "MATCH (e:Entity) RETURN count(e) AS total",
            &BTreeMap::new(),
        ))
        .unwrap()
        .rows[0]["total"],
        Value::Int(2)
    );
}

#[test]
fn scoped_knowledge_entity_batch_delete_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();
    let mut parameters = batch_id_parameters(&["memory_1", "memory_2"]).unwrap();
    parameters.insert("source_id".into(), Value::String("thread_1".into()));
    let output = db
        .query_request(bounded_batch_query(
            "MATCH (m:Memory) WHERE m.id IN $ids AND m.source_id = $source_id DETACH DELETE m",
            &parameters,
        ))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), 3);
    let remaining = db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(remaining.rows[0]["id"], Value::String("memory_2".into()));
}

#[test]
fn knowledge_entity_batch_delete_deduplicates_writes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let parameters = batch_id_parameters(&["memory_1", "memory_1"]).unwrap();
    let output = db
        .query_request(bounded_batch_query(DELETE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), 2);
    assert!(db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn knowledge_entity_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let parameters = batch_id_parameters(&["0"]).unwrap();
    let output = db
        .query_request(bounded_batch_query(DELETE, &parameters))
        .unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let retained = db
        .query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN id(m) AS node_id, m.id AS id, m.title AS title",
            &BTreeMap::new(),
        ))
        .unwrap();
    assert_eq!(retained.rows.len(), 1);
    assert_eq!(retained.rows[0]["node_id"], Value::Int(0));
    assert_eq!(retained.rows[0]["id"], Value::Null);
    assert_eq!(
        retained.rows[0]["title"],
        Value::String("Idless memory".into())
    );
}

#[test]
fn knowledge_entity_batch_delete_rejects_invalid_identifier() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let parameters = batch_id_parameters(&["memory_1"]).unwrap();
    let error = db
        .query_request(bounded_batch_query(
            "MATCH (m:Bad-Label) WHERE m.id IN $ids DETACH DELETE m",
            &parameters,
        ))
        .unwrap_err();
    assert!(matches!(error, HawDBError::Parse(_)), "{error}");
    assert_eq!(db.commit_epoch().unwrap(), 1);
}

#[test]
fn read_only_database_rejects_knowledge_entity_batch_delete_query() {
    let path = unique_test_dir("read_only_knowledge_entity_batch_delete_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    }
    let wal = read_test_wal(&path).unwrap();
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                recovery_mode: crate::RecoveryMode::Strict,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let parameters = batch_id_parameters(&["memory_1"]).unwrap();
        let error = db
            .query_request(bounded_batch_query(DELETE, &parameters))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(
            db.begin_read_transaction()
                .unwrap()
                .query_request(bounded_batch_query(READ, &parameters))
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    assert_eq!(read_test_wal(&path).unwrap(), wal);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_entity_batch_delete_query_persists_and_replays_from_wal() {
    let path = unique_test_dir("knowledge_entity_batch_delete_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let parameters = batch_id_parameters(&["memory_1", "memory_2"]).unwrap();
        let output = db
            .query_request(bounded_batch_query(DELETE, &parameters))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_node"));
    assert!(wal.contains("delete_rel"));
    {
        let db = Database::open(&path).unwrap();
        let mut read = db.begin_read_transaction().unwrap();
        let parameters = batch_id_parameters(&["memory_1", "memory_2"]).unwrap();
        assert!(read
            .query_request(bounded_batch_query(READ, &parameters))
            .unwrap()
            .rows
            .is_empty());
        let parameters = batch_id_parameters(&["entity_1"]).unwrap();
        assert_eq!(
            read.query_request(bounded_batch_query(
                "MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS id",
                &parameters,
            ))
            .unwrap()
            .rows
            .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
