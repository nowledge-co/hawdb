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

pub(super) const CREATE: &str = "MATCH (m:Memory {id: $source}), (e:Entity {id: $target}) CREATE (m)-[:MENTIONS {confidence: $confidence}]->(e)";
const CREATE_WITH_COUNT: &str = "MATCH (m:Memory {id: $source}), (e:Entity {id: $target}) CREATE (m)-[:MENTIONS {confidence: $confidence, mention_count: $count}]->(e)";
pub(super) const SCOPED_CREATE: &str = "MATCH (m:Memory {id: $source, source_id: $scope}), (e:Entity {id: $target, space_id: $space}) CREATE (m)-[:MENTIONS {confidence: $confidence}]->(e)";
pub(super) const INVALID_CREATE: &str = "MATCH (m:Memory {id: $source}), (e:Entity {id: $target}) CREATE (m)-[:MENTIONS-WITH-DASH]->(e)";
pub(super) const READ: &str = "MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) RETURN id(m) AS source_node_id, m.id AS source, id(e) AS target_node_id, e.id AS target, id(r) AS relationship_id, r.confidence AS confidence, r.mention_count AS count ORDER BY source, target";
const LABEL_MERGE: &str = "MATCH (m:Memory {id: $source}), (l:Label {id: $target}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET r.assigned_by = $assigned_by, r.created_at = $created_at";
const LABEL_READ: &str = "MATCH (m:Memory)-[r:HAS_LABEL]->(l:Label) RETURN id(m) AS source_node_id, m.id AS source, id(l) AS target_node_id, l.id AS target, id(r) AS relationship_id, r.assigned_by AS assigned_by, r.created_at AS created_at ORDER BY source, target";

pub(super) fn parameters(source: &str, target: &str, confidence: f64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source".into(), Value::String(source.into())),
        ("target".into(), Value::String(target.into())),
        ("confidence".into(), Value::Float(confidence)),
        ("count".into(), Value::Int(1)),
        ("scope".into(), Value::String("thread_1".into())),
        ("space".into(), Value::String("default".into())),
    ])
}

fn label_parameters(source: &str, assigned_by: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source".into(), Value::String(source.into())),
        ("target".into(), Value::String("label_1".into())),
        ("assigned_by".into(), Value::String(assigned_by.into())),
        ("created_at".into(), Value::String("2026-07-19".into())),
    ])
}

#[test]
fn creates_knowledge_relationship_through_bounded_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB'})")
        .unwrap();
    let parameters = parameters("memory_1", "entity_1", 0.9);
    let epoch = db.commit_epoch().unwrap();
    let created = db
        .query_request(bounded_batch_query(CREATE_WITH_COUNT, &parameters))
        .unwrap();
    assert_eq!(created.rows.len(), 1);
    assert_eq!(epoch, 2);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap();
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(rows.rows[0]["target_node_id"], Value::Int(1));
    for key in ["source", "target", "confidence", "count"] {
        assert_eq!(rows.rows[0][key], parameters[key]);
    }
}

#[test]
fn scoped_knowledge_relationship_create_does_not_write_filtered_endpoint() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'First', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'HawDB', space_id: 'default'})")
        .unwrap();
    let mut parameters = parameters("memory_1", "entity_1", 0.9);
    parameters.insert("scope".into(), Value::String("thread_2".into()));
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(SCOPED_CREATE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
    let endpoints = db.query_request(bounded_batch_query("MATCH (m:Memory {id: $source}), (e:Entity {id: $target}) RETURN id(m) AS source_node_id, id(e) AS target_node_id, m.source_id AS scope, e.space_id AS space", &parameters)).unwrap();
    assert_eq!(endpoints.rows.len(), 1);
    assert_eq!(endpoints.rows[0]["source_node_id"], Value::Int(0));
    assert_eq!(endpoints.rows[0]["target_node_id"], Value::Int(1));
    assert_eq!(endpoints.rows[0]["scope"], Value::String("thread_1".into()));
    assert_eq!(endpoints.rows[0]["space"], Value::String("default".into()));
}

#[test]
fn knowledge_relationship_create_rejects_invalid_identifiers() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let error = db
        .query_request(bounded_batch_query(
            INVALID_CREATE,
            &parameters("memory_1", "entity_1", 0.9),
        ))
        .unwrap_err();
    assert!(matches!(error, HawDBError::Parse(_)));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
}

#[test]
fn knowledge_relationship_create_does_not_resolve_an_internal_id_as_an_external_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
    let parameters = parameters("0", "entity_1", 0.9);
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(CREATE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(db
        .query_request(bounded_batch_query(READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
    let memory = db
        .query_request(bounded_batch_query(
            "MATCH (m:Memory) RETURN id(m) AS node_id, m.id AS id",
            &parameters,
        ))
        .unwrap();
    assert_eq!(memory.rows.len(), 1);
    assert_eq!(memory.rows[0]["node_id"], Value::Int(0));
    assert_eq!(memory.rows[0]["id"], Value::Null);
}

#[test]
fn upserts_knowledge_relationship_through_bounded_query() {
    let path = unique_test_dir("relationship_upsert_query_retry");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        let parameters = label_parameters("memory_1", "system");
        let epoch = db.commit_epoch().unwrap();
        let created = db
            .query_request(bounded_batch_query(LABEL_MERGE, &parameters))
            .unwrap();
        assert_eq!(created.rows.len(), 1);
        assert_eq!(created.rows[0]["created"], Value::Bool(true));
        assert_eq!(created.rows[0]["rel_id"], Value::Int(0));
        assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
        let before = db
            .query_request(bounded_batch_query(LABEL_READ, &parameters))
            .unwrap();
        assert_eq!(before.rows.len(), 1);
        assert_eq!(before.rows[0]["source_node_id"], Value::Int(0));
        assert_eq!(before.rows[0]["target_node_id"], Value::Int(1));
        assert_eq!(
            before.rows[0]["assigned_by"],
            Value::String("system".into())
        );
        assert_eq!(
            before.rows[0]["created_at"],
            Value::String("2026-07-19".into())
        );
        let wal = read_test_wal(&path).unwrap();
        let mut second = label_parameters("memory_1", "ignored");
        second.insert("created_at".into(), Value::Null);
        let existing = db
            .query_request(bounded_batch_query(LABEL_MERGE, &second))
            .unwrap();
        assert_eq!(existing.rows.len(), 1);
        assert_eq!(existing.rows[0]["created"], Value::Bool(false));
        assert_eq!(existing.rows[0]["rel_id"], created.rows[0]["rel_id"]);
        assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
        assert_eq!(read_test_wal(&path).unwrap(), wal);
        let after = db
            .query_request(bounded_batch_query(LABEL_READ, &parameters))
            .unwrap();
        assert_eq!(after.rows, before.rows);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_upsert_does_not_resolve_an_internal_id_as_an_external_endpoint() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    let parameters = label_parameters("0", "system");
    let epoch = db.commit_epoch().unwrap();
    assert!(db
        .query_request(bounded_batch_query(LABEL_MERGE, &parameters))
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert!(db
        .query_request(bounded_batch_query(LABEL_READ, &parameters))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn upserts_knowledge_relationship_batch_through_bounded_queries() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
    db.query("CREATE (:Label {id: 'label_1'})").unwrap();
    db.query("MATCH (m:Memory {id: 'memory_1'}), (l:Label {id: 'label_1'}) CREATE (m)-[:HAS_LABEL {assigned_by: 'existing'}]->(l)").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (memory, assigned_by) in [
        ("memory_1", "ignored"),
        ("memory_2", "created"),
        ("memory_2", "duplicate"),
    ] {
        assert!(transaction
            .query_request(bounded_batch_query(
                LABEL_MERGE,
                &label_parameters(memory, assigned_by)
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    let output = transaction.commit().unwrap();
    assert_eq!(output.rows.len(), 3);
    for (row, created) in output.rows.iter().zip([false, true, false]) {
        assert_eq!(row["created"], Value::Bool(created));
    }
    assert_eq!(output.rows[0]["rel_id"], Value::Int(0));
    assert_eq!(output.rows[1]["rel_id"], output.rows[2]["rel_id"]);
    assert_eq!(epoch, 4);
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    let rows = db
        .query_request(bounded_batch_query(LABEL_READ, &BTreeMap::new()))
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    for (index, source, assigned_by) in [(0, "memory_1", "existing"), (1, "memory_2", "created")] {
        assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
        assert_eq!(rows.rows[index]["target"], Value::String("label_1".into()));
        assert_eq!(
            rows.rows[index]["assigned_by"],
            Value::String(assigned_by.into())
        );
    }
    assert_eq!(rows.rows[0]["created_at"], Value::Null);
    assert_eq!(
        rows.rows[1]["created_at"],
        Value::String("2026-07-19".into())
    );
}

#[test]
fn knowledge_relationship_batch_upsert_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("relationship_upsert_batch_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Memory {id: 'memory_2'})").unwrap();
        db.query("CREATE (:Label {id: 'label_1'})").unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let mut transaction = db.begin_transaction().unwrap();
        for source in ["memory_1", "memory_2"] {
            transaction
                .query_request(bounded_batch_query(
                    LABEL_MERGE,
                    &label_parameters(source, "system"),
                ))
                .unwrap();
        }
        let output = transaction.commit().unwrap();
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .all(|row| row["created"] == Value::Bool(true)));
        assert_eq!(
            read_test_wal(&path).unwrap().matches("\tbatch\t").count(),
            before + 1
        );
    }
    assert!(read_test_wal(&path).unwrap().contains("create_rel"));
    {
        let mut db = Database::open(&path).unwrap();
        let rows = db
            .query_request(bounded_batch_query(LABEL_READ, &BTreeMap::new()))
            .unwrap();
        assert_eq!(rows.rows.len(), 2);
        for (index, source) in ["memory_1", "memory_2"].into_iter().enumerate() {
            assert_eq!(rows.rows[index]["source"], Value::String(source.into()));
            assert_eq!(rows.rows[index]["target"], Value::String("label_1".into()));
            assert_eq!(
                rows.rows[index]["assigned_by"],
                Value::String("system".into())
            );
            assert_eq!(
                rows.rows[index]["created_at"],
                Value::String("2026-07-19".into())
            );
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn read_only_database_rejects_knowledge_relationship_create_query() {
    let path = unique_test_dir("relationship_create_query_read_only");
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
        let error = db
            .query_request(bounded_batch_query(
                CREATE,
                &parameters("memory_1", "entity_1", 0.9),
            ))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(read_test_wal(&path).unwrap(), wal);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_relationship_create_query_persists_and_replays_from_wal() {
    let path = unique_test_dir("relationship_create_query_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
        db.query("CREATE (:Entity {id: 'entity_1'})").unwrap();
        let before = read_test_wal(&path).unwrap().matches("\tbatch\t").count();
        let output = db
            .query_request(bounded_batch_query(
                CREATE,
                &parameters("memory_1", "entity_1", 0.7),
            ))
            .unwrap();
        assert_eq!(output.rows.len(), 1);
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
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0]["source"], Value::String("memory_1".into()));
        assert_eq!(rows.rows[0]["target"], Value::String("entity_1".into()));
        assert_eq!(rows.rows[0]["confidence"], Value::Float(0.7));
    }
    std::fs::remove_dir_all(path).unwrap();
}
