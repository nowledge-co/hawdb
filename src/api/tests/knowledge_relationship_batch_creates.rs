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

use super::knowledge_relationship_create_upserts::{
    parameters, CREATE, INVALID_CREATE, READ, SCOPED_CREATE,
};
use super::*;

#[test]
fn creates_knowledge_relationship_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_2', name: 'Graph'})")
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let missing = parameters("missing", "entity_1", 0.0);
    assert!(transaction
        .query_request(bounded_batch_query(
            "MATCH (m:Memory {id: $source}) RETURN id(m) AS node_id",
            &missing
        ))
        .unwrap()
        .rows
        .is_empty());
    let target = transaction
        .query_request(bounded_batch_query(
            "MATCH (e:Entity {id: $target}) RETURN id(e) AS node_id",
            &missing,
        ))
        .unwrap();
    assert_eq!(target.rows.len(), 1);
    assert_eq!(target.rows[0]["node_id"], Value::Int(2));
    for (source, target, confidence) in [
        ("memory_1", "entity_1", 0.9),
        ("memory_2", "entity_2", 0.7),
        ("missing", "entity_1", 0.0),
    ] {
        assert!(transaction
            .query_request(bounded_batch_query(
                CREATE,
                &parameters(source, target, confidence)
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    let staged = transaction
        .query_request(bounded_batch_query(READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(staged.rows.len(), 2);
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(epoch, 4);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows, staged.rows);
    for (index, source, target, confidence) in [
        (0, "memory_1", "entity_1", 0.9),
        (1, "memory_2", "entity_2", 0.7),
    ] {
        assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
        assert_eq!(rows.rows[index]["target"], Value::String(target.into()));
        assert_eq!(rows.rows[index]["confidence"], Value::Float(confidence));
    }
}

#[test]
fn scoped_knowledge_relationship_batch_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', space_id: 'default'})")
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for source in ["memory_1", "memory_2"] {
        transaction
            .query_request(bounded_batch_query(
                SCOPED_CREATE,
                &parameters(source, "entity_1", 0.9),
            ))
            .unwrap();
    }
    assert_eq!(transaction.commit().unwrap().rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["source"], Value::String("memory_1".into()));
    assert_eq!(rows.rows[0]["target"], Value::String("entity_1".into()));
    assert_eq!(rows.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(rows.rows[0]["target_node_id"], Value::Int(2));
}

#[test]
fn knowledge_relationship_batch_create_rejects_invalid_identifiers() {
    let path = unique_test_dir("relationship_create_batch_invalid_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        let error = transaction
            .query_request(bounded_batch_query(
                INVALID_CREATE,
                &parameters("memory_1", "entity_1", 0.9),
            ))
            .unwrap_err();
        assert!(matches!(error, HawDBError::Parse(_)));
        transaction.rollback();
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_batch_create_does_not_resolve_an_internal_id_as_an_external_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_request(bounded_batch_query(
            CREATE,
            &parameters("0", "entity_1", 0.9),
        ))
        .unwrap();
    assert!(transaction.commit().unwrap().rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(db
        .query_request(bounded_batch_query(READ, &BTreeMap::new()))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn read_only_database_rejects_knowledge_relationship_batch_create_queries() {
    let path = unique_test_dir("relationship_create_batch_read_only_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
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
        let mut transaction = db.begin_transaction().unwrap();
        let error = transaction
            .query_request(bounded_batch_query(
                CREATE,
                &parameters("memory_1", "entity_1", 0.9),
            ))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        transaction.rollback();
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_batch_create_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("relationship_create_batch_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_2'})").unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (source, target, confidence) in
            [("memory_1", "entity_1", 0.7), ("memory_2", "entity_2", 0.8)]
        {
            transaction
                .query_request(bounded_batch_query(
                    CREATE,
                    &parameters(source, target, confidence),
                ))
                .unwrap();
        }
        assert_eq!(transaction.commit().unwrap().rows.len(), 2);
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("create_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(READ, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for (index, source, target, confidence) in [
            (0, "memory_1", "entity_1", 0.7),
            (1, "memory_2", "entity_2", 0.8),
        ] {
            assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
            assert_eq!(rows.rows[index]["target"], Value::String(target.into()));
            assert_eq!(rows.rows[index]["confidence"], Value::Float(confidence));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
