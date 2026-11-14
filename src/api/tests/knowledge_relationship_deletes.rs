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

pub(super) const LABEL_DELETE: &str =
    "MATCH (m:Memory {id: $source})-[r:HAS_LABEL]->(l:Label {id: $target}) DELETE r";
pub(super) const LABEL_SCOPED_DELETE: &str = "MATCH (m:Memory {id: $source})-[r:HAS_LABEL]->(l:Label {id: $target}) WHERE m.source_id = $scope DELETE r";
pub(super) const LABEL_READ: &str = "MATCH (m:Memory {id: $source})-[r:HAS_LABEL]->(l:Label {id: $target}) RETURN id(m) AS source_node_id, id(l) AS target_node_id, id(r) AS relationship_id";
pub(super) const LABEL_PAGE: &str = "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) RETURN m.id AS source, l.id AS target, id(r) AS relationship_id ORDER BY source, target";
pub(super) const MENTION_DELETE: &str = "MATCH (m:Memory {id: $source})-[r:MENTIONS]->(e:Entity {id: $target}) WHERE r.source_reference = $reference DELETE r";
pub(super) const MENTION_READ: &str = "MATCH (m:Memory {id: $source})-[r:MENTIONS]->(e:Entity {id: $target}) RETURN r.source_reference AS reference ORDER BY reference";
pub(super) const INVALID_DELETE: &str =
    "MATCH (m:Memory {id: $source})-[r:HAS-LABEL]->(l:Label {id: $target}) DELETE r";
pub(super) const NODE_COUNT: &str = "MATCH (n) RETURN count(n) AS total";

pub(super) fn parameters(source: &str, target: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source".into(), Value::String(source.into())),
        ("target".into(), Value::String(target.into())),
        ("scope".into(), Value::String("thread_1".into())),
        ("reference".into(), Value::String("drop".into())),
    ])
}

#[test]
fn deletes_knowledge_relationship_through_bounded_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL {assigned_by: 'system'}]->(:Label {id: 'label_1', name: 'Database'})").unwrap();
    let parameters = parameters("memory_1", "label_1");
    let before = db
        .query_request(bounded_batch_query(LABEL_READ, &parameters))
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(before.rows[0]["target_node_id"], Value::Int(1));
    let epoch = db.commit_epoch().unwrap();
    let output = db
        .query_request(bounded_batch_query(LABEL_DELETE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(epoch, 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    assert!(db
        .query_request(bounded_batch_query(LABEL_READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
    let nodes = db
        .query_request(bounded_batch_query(NODE_COUNT, &parameters))
        .unwrap();
    assert_eq!(nodes.rows[0]["total"], Value::Int(2));
}

#[test]
fn knowledge_relationship_delete_filters_relationship_properties() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {source_reference: 'keep'}]->(:Entity {id: 'entity_1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (e:Entity {id: 'entity_1'}) CREATE (m)-[:MENTIONS {source_reference: 'drop'}]->(e)").unwrap();
    let parameters = parameters("memory_1", "entity_1");
    let output = db
        .query_request(bounded_batch_query(MENTION_DELETE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    let rows = db
        .query_request(bounded_batch_query(MENTION_READ, &parameters))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["reference"], Value::String("keep".into()));
}

#[test]
fn scoped_knowledge_relationship_delete_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL]->(:Label {id: 'label_1', name: 'Database'})").unwrap();
    let mut parameters = parameters("memory_1", "label_1");
    parameters.insert("scope".into(), Value::String("thread_2".into()));
    let before = db
        .query_request(bounded_batch_query(LABEL_READ, &parameters))
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(before.rows[0]["target_node_id"], Value::Int(1));
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(LABEL_SCOPED_DELETE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let after = db
        .query_request(bounded_batch_query(LABEL_READ, &parameters))
        .unwrap();
    assert_eq!(after.rows, before.rows);
}

#[test]
fn knowledge_relationship_delete_rejects_invalid_identifiers() {
    let path = unique_test_dir("relationship_delete_invalid_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let error = db
            .query_request(bounded_batch_query(
                INVALID_DELETE,
                &parameters("memory_1", "label_1"),
            ))
            .unwrap_err();
        assert!(matches!(error, HawDBError::Parse(_)));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_knowledge_relationship_delete_query() {
    let path = unique_test_dir("relationship_delete_read_only_query");
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
        let error = db
            .query_request(bounded_batch_query(
                LABEL_DELETE,
                &parameters("memory_1", "label_1"),
            ))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_delete_query_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_delete_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:HAS_LABEL]->(:Label {id: 'label_1'})")
            .unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let output = db
            .query_request(bounded_batch_query(
                LABEL_DELETE,
                &parameters("memory_1", "label_1"),
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("delete_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let parameters = parameters("memory_1", "label_1");
        assert!(db
            .query_request(bounded_batch_query(LABEL_PAGE, &parameters))
            .unwrap()
            .rows
            .is_empty());
        let nodes = db
            .query_request(bounded_batch_query(NODE_COUNT, &parameters))
            .unwrap();
        assert_eq!(nodes.rows[0]["total"], Value::Int(2));
    }
    std::fs::remove_dir_all(path).unwrap();
}
