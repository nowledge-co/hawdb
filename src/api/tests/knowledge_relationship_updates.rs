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

const UPDATE: &str =
    "MATCH (m:Memory {id: $source})-[r:MENTIONS]->(e:Entity {id: $target}) SET r.weight = $weight";
const FILTERED_UPDATE: &str = "MATCH (m:Memory {id: $source})-[r:MENTIONS {source_reference: $reference}]->(e:Entity {id: $target}) SET r.weight = $weight";
const REVIEW_UPDATE: &str = "MATCH (m:Memory {id: $source})-[r:MENTIONS {source_reference: $reference}]->(e:Entity {id: $target}) SET r.weight = $weight, r.review_status = $status";
const SCOPED_UPDATE: &str = "MATCH (m:Memory {id: $source})-[r:HAS_LABEL]->(l:Label {id: $target}) WHERE m.source_id = $scope SET r.weight = $weight";
const MENTION_PAGE: &str = "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(m) AS source_node_id, m.id AS source, id(e) AS target_node_id, e.id AS target, id(r) AS relationship_id, r.source_reference AS reference, r.weight AS weight, r.review_status AS status ORDER BY source, target, reference";
const LABEL_PAGE: &str = "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) RETURN m.id AS source, l.id AS target, id(r) AS relationship_id, r.weight AS weight ORDER BY source, target";

fn parameters(source: &str, target: &str, reference: &str, weight: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source".into(), Value::String(source.into())),
        ("target".into(), Value::String(target.into())),
        ("reference".into(), Value::String(reference.into())),
        ("weight".into(), Value::Int(weight)),
        ("status".into(), Value::String("approved".into())),
        ("scope".into(), Value::String("thread_1".into())),
    ])
}

#[test]
fn updates_knowledge_relationship_through_bounded_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw', weight: 1}]->(:Entity {id: 'entity_1'})").unwrap();
    let parameters = parameters("memory_1", "entity_1", "raw", 9);
    let epoch = db.commit_epoch().unwrap();
    let output = db
        .query_request(bounded_batch_query(REVIEW_UPDATE, &parameters))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(epoch, 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(MENTION_PAGE, &parameters))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(rows.rows[0]["target_node_id"], Value::Int(1));
    for key in ["source", "target", "reference", "weight", "status"] {
        assert_eq!(rows.rows[0][key], parameters[key]);
    }
}

#[test]
fn scoped_knowledge_relationship_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})").unwrap();
    let mut parameters = parameters("memory_1", "label_1", "", 9);
    parameters.insert("scope".into(), Value::String("thread_2".into()));
    let before = db
        .query_request(bounded_batch_query(LABEL_PAGE, &parameters))
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(SCOPED_UPDATE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let after = db
        .query_request(bounded_batch_query(LABEL_PAGE, &parameters))
        .unwrap();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.rows.len(), 1);
    assert_eq!(after.rows[0]["weight"], Value::Int(1));
}

#[test]
fn updates_knowledge_relationship_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_1', weight: 1}]->(:Entity {id: 'entity_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_1'})-[:MENTIONS {source_reference: 'raw_2', weight: 2}]->(:Entity {id: 'entity_2'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (query, parameters) in [
        (
            FILTERED_UPDATE,
            parameters("memory_1", "entity_1", "raw_1", 9),
        ),
        (
            FILTERED_UPDATE,
            parameters("memory_2", "entity_2", "raw_2", 8),
        ),
        (UPDATE, parameters("missing", "entity_1", "", 7)),
    ] {
        assert!(transaction
            .query_request(bounded_batch_query(query, &parameters))
            .unwrap()
            .rows
            .is_empty());
    }
    let staged = transaction
        .query_request(bounded_batch_query(MENTION_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(staged.rows.len(), 2);
    for (index, source, target, reference, weight) in [
        (0, "memory_1", "entity_1", "raw_1", 9),
        (1, "memory_2", "entity_2", "raw_2", 8),
    ] {
        assert_eq!(staged.rows[index]["source"], Value::String(source.into()));
        assert_eq!(staged.rows[index]["target"], Value::String(target.into()));
        assert_eq!(
            staged.rows[index]["reference"],
            Value::String(reference.into())
        );
        assert_eq!(staged.rows[index]["weight"], Value::Int(weight));
        assert_eq!(staged.rows[index]["status"], Value::Null);
    }
    assert_eq!(transaction.commit().unwrap().rows.len(), 2);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(MENTION_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows, staged.rows);
}

#[test]
fn scoped_knowledge_relationship_batch_update_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})-[:HAS_LABEL {weight: 1}]->(:Label {id: 'label_1', name: 'Database'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2', source_id: 'thread_2', space_id: ''})-[:HAS_LABEL {weight: 2}]->(:Label {id: 'label_2', name: 'Rust'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (source, target, weight) in [("memory_1", "label_1", 9), ("memory_2", "label_2", 8)] {
        transaction
            .query_request(bounded_batch_query(
                SCOPED_UPDATE,
                &parameters(source, target, "", weight),
            ))
            .unwrap();
    }
    assert_eq!(transaction.commit().unwrap().rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(LABEL_PAGE, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    for (index, source, target, weight) in
        [(0, "memory_1", "label_1", 9), (1, "memory_2", "label_2", 2)]
    {
        assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
        assert_eq!(rows.rows[index]["target"], Value::String(target.into()));
        assert_eq!(rows.rows[index]["weight"], Value::Int(weight));
    }
}

#[test]
fn knowledge_relationship_update_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
        .unwrap();
    let epoch = db.commit_epoch().unwrap();
    let error = db.query_request(bounded_batch_query(
        "MATCH (m:Memory {id: $source})-[r:MENTIONS]->(e:Entity {id: $target}) SET r.bad-name = $weight",
        &parameters("memory_1", "entity_1", "", 1),
    )).unwrap_err();
    assert!(matches!(error, HawDBError::Parse(_)));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn knowledge_relationship_update_rejects_empty_assignments() {
    let path = unique_test_dir("relationship_update_empty_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        let epoch = db.commit_epoch().unwrap();
        let wal = read_test_wal(&path).unwrap();
        let error = db
            .query_request(bounded_batch_query(
                "MATCH (m:Memory {id: $source})-[r:MENTIONS]->(e:Entity {id: $target}) SET",
                &parameters("memory_1", "entity_1", "", 1),
            ))
            .unwrap_err();
        assert!(matches!(error, HawDBError::Parse(_)));
        assert_eq!(db.commit_epoch().unwrap(), epoch);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_batch_update_does_not_write_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})-[:MENTIONS {weight: 1}]->(:Entity {id: 'entity_1'})").unwrap();
    let parameters = parameters("0", "entity_1", "", 9);
    let before = db
        .query_request(bounded_batch_query(MENTION_PAGE, &parameters))
        .unwrap();
    assert_eq!(before.rows.len(), 1);
    assert_eq!(before.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(before.rows[0]["source"], Value::Null);
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query_request(bounded_batch_query(UPDATE, &parameters))
        .unwrap();
    assert!(transaction.commit().unwrap().rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    let after = db
        .query_request(bounded_batch_query(MENTION_PAGE, &parameters))
        .unwrap();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.rows[0]["weight"], Value::Int(1));
}

#[test]
fn read_only_database_rejects_knowledge_relationship_update_query() {
    let path = unique_test_dir("relationship_update_read_only_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
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
                UPDATE,
                &parameters("memory_1", "entity_1", "", 9),
            ))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_batch_update_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("relationship_update_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_1'})-[:MENTIONS {weight: 1}]->(:Entity {id: 'entity_1'})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'memory_2'})-[:MENTIONS {weight: 2}]->(:Entity {id: 'entity_2'})",
        )
        .unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for (source, target, weight) in [("memory_1", "entity_1", 9), ("memory_2", "entity_2", 8)] {
            transaction
                .query_request(bounded_batch_query(
                    UPDATE,
                    &parameters(source, target, "", weight),
                ))
                .unwrap();
        }
        assert_eq!(transaction.commit().unwrap().rows.len(), 2);
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("set_rel_property"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(MENTION_PAGE, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for (index, source, target, weight) in [
            (0, "memory_1", "entity_1", 9),
            (1, "memory_2", "entity_2", 8),
        ] {
            assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
            assert_eq!(rows.rows[index]["target"], Value::String(target.into()));
            assert_eq!(rows.rows[index]["weight"], Value::Int(weight));
            assert_eq!(rows.rows[index]["status"], Value::Null);
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
