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

use super::knowledge_relationship_deletes::{
    parameters, INVALID_DELETE, LABEL_DELETE, LABEL_PAGE, LABEL_READ, LABEL_SCOPED_DELETE,
    MENTION_DELETE, MENTION_READ, NODE_COUNT,
};
use super::*;

#[test]
fn deletes_knowledge_relationship_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_2'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let missing = parameters("missing", "label_1");
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
            "MATCH (l:Label {id: $target}) RETURN id(l) AS node_id",
            &missing,
        ))
        .unwrap();
    assert_eq!(target.rows.len(), 1);
    assert_eq!(target.rows[0]["node_id"], Value::Int(1));
    let mut matched = Vec::new();
    for (source, target) in [
        ("memory_1", "label_1"),
        ("memory_2", "label_2"),
        ("missing", "label_1"),
    ] {
        let parameters = parameters(source, target);
        matched.push(
            transaction
                .query_request(bounded_batch_query(LABEL_READ, &parameters))
                .unwrap()
                .rows
                .len(),
        );
        assert!(transaction
            .query_request(bounded_batch_query(LABEL_DELETE, &parameters))
            .unwrap()
            .rows
            .is_empty());
    }
    assert_eq!(matched, [1, 1, 0]);
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(epoch, 2);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    assert!(db
        .query_request(bounded_batch_query(LABEL_PAGE, &BTreeMap::new()))
        .unwrap()
        .rows
        .is_empty());
    let nodes = db
        .query_request(bounded_batch_query(NODE_COUNT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(nodes.rows[0]["total"], Value::Int(4));
}

#[test]
fn knowledge_relationship_batch_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)").unwrap();
    let parameters = parameters("memory_1", "entity_1");
    let mut transaction = db.begin_transaction().unwrap();
    assert!(transaction
        .query_request(bounded_batch_query(MENTION_DELETE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    let staged = transaction
        .query_request(bounded_batch_query(MENTION_READ, &parameters))
        .unwrap();
    assert_eq!(staged.rows.len(), 1);
    assert_eq!(staged.rows[0]["reference"], Value::String("keep".into()));
    assert_eq!(transaction.commit().unwrap().rows.len(), 1);
    let rows = db
        .query_request(bounded_batch_query(MENTION_READ, &parameters))
        .unwrap();
    assert_eq!(rows.rows, staged.rows);
}

#[test]
fn scoped_knowledge_relationship_batch_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_2', name: 'Rust'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (source, target) in [("memory_1", "label_1"), ("memory_2", "label_2")] {
        transaction
            .query_request(bounded_batch_query(
                LABEL_SCOPED_DELETE,
                &parameters(source, target),
            ))
            .unwrap();
    }
    assert!(transaction
        .query_request(bounded_batch_query(
            LABEL_READ,
            &parameters("memory_1", "label_1")
        ))
        .unwrap()
        .rows
        .is_empty());
    let kept = transaction
        .query_request(bounded_batch_query(
            LABEL_READ,
            &parameters("memory_2", "label_2"),
        ))
        .unwrap();
    assert_eq!(kept.rows.len(), 1);
    assert_eq!(kept.rows[0]["source_node_id"], Value::Int(2));
    assert_eq!(kept.rows[0]["target_node_id"], Value::Int(3));
    assert_eq!(transaction.commit().unwrap().rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let remaining = db
        .query_request(bounded_batch_query(LABEL_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);
    assert_eq!(
        remaining.rows[0]["source"],
        Value::String("memory_2".into())
    );
    assert_eq!(remaining.rows[0]["target"], Value::String("label_2".into()));
    assert_eq!(
        remaining.rows[0]["relationship_id"],
        kept.rows[0]["relationship_id"]
    );
}

#[test]
fn knowledge_relationship_batch_delete_rejects_invalid_identifiers() {
    let path = unique_test_dir("relationship_delete_batch_invalid_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        let error = transaction
            .query_request(bounded_batch_query(
                INVALID_DELETE,
                &parameters("memory_1", "label_1"),
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
fn knowledge_relationship_batch_delete_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
        .unwrap();
    let parameters = parameters("0", "label_1");
    let before = db
        .query_request(bounded_batch_query(LABEL_PAGE, &parameters))
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0]["source"], Value::Null);
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_request(bounded_batch_query(LABEL_DELETE, &parameters))
        .unwrap();
    assert!(transaction.commit().unwrap().rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let after = db
        .query_request(bounded_batch_query(LABEL_PAGE, &parameters))
        .unwrap();
    assert_eq!(after.rows, before.rows);
}

#[test]
fn read_only_database_rejects_knowledge_relationship_batch_delete_queries() {
    let path = unique_test_dir("relationship_delete_batch_read_only_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
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
                LABEL_DELETE,
                &parameters("memory_1", "label_1"),
            ))
            .unwrap_err();
        transaction.rollback();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_batch_delete_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("relationship_delete_batch_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})-[:HAS_LABEL]->(:Label {id: 'label_2'})")
            .unwrap();
    }
    let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
    {
        let mut db = Database::open(&path).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        for (source, target) in [("memory_1", "label_1"), ("memory_2", "label_2")] {
            transaction
                .query_request(bounded_batch_query(
                    LABEL_DELETE,
                    &parameters(source, target),
                ))
                .unwrap();
        }
        assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("delete_rel"));
    assert_eq!(wal.matches("\tbatch\t").count(), before + 1);
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .query_request(bounded_batch_query(LABEL_PAGE, &BTreeMap::new()))
            .unwrap()
            .rows
            .is_empty());
        let nodes = db
            .query_request(bounded_batch_query(NODE_COUNT, &BTreeMap::new()))
            .unwrap();
        assert_eq!(nodes.rows[0]["total"], Value::Int(4));
    }
    std::fs::remove_dir_all(path).unwrap();
}
