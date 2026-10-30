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

#[test]
fn scoped_knowledge_property_batch_update_does_not_write_filtered_rows() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'memory_1', title: 'Old 1', source_id: 'thread_1', space_id: ''})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'memory_2', title: 'Old 2', source_id: 'thread_2', space_id: ''})",
    )
    .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    for (id, title, expected) in [("memory_1", "New 1", 1), ("memory_2", "New 2", 0)] {
        let mut parameters = property_title_parameters(id, Some(title)).unwrap();
        parameters.insert("source".into(), Value::String("thread_1".into()));
        let candidate = transaction
            .query_request(bounded_batch_query(
                "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id",
                &parameters,
            ))
            .unwrap();
        assert_eq!(candidate.rows.len(), 1);
        let output = transaction
            .query_request(bounded_batch_query(
                "MATCH (m:Memory {id: $id}) WHERE m.source_id = $source \
             SET m.title = $title RETURN id(m) AS node_id",
                &parameters,
            ))
            .unwrap();
        assert_eq!(output.rows.len(), expected);
        if expected == 1 {
            assert_eq!(output.rows[0]["node_id"], Value::Int(0));
        }
    }
    transaction.commit().unwrap();
    assert_eq!(before, 2);
    assert_eq!(db.commit_epoch().unwrap(), before + 1);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["title"], Value::String("New 1".into()));
    assert_eq!(output.rows[1]["title"], Value::String("Old 2".into()));
}

#[test]
fn knowledge_property_batch_update_rejects_invalid_identifiers() {
    let path = unique_test_dir("invalid_property_batch_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
        let before = db.commit_epoch().unwrap();
        let wal_before = read_test_wal(&path).unwrap();
        let parameters = property_title_parameters("memory_1", Some("New")).unwrap();
        {
            let mut transaction = db.begin_transaction().unwrap();
            let error = transaction
                .query_request(bounded_batch_query(
                    "MATCH (m:Memory {id: $id}) SET m.bad-name = $title RETURN id(m)",
                    &parameters,
                ))
                .unwrap_err();
            assert!(matches!(error, HawDBError::Parse(_)), "{error}");
        }
        assert_eq!(db.commit_epoch().unwrap(), before);
        assert_eq!(read_test_wal(&path).unwrap(), wal_before);
        let output = db
            .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
            .unwrap();
        assert_eq!(output.rows[0]["title"], Value::String("Old".into()));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_property_batch_update_rejects_missing_assignment_parameters() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
        .unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
    let error = db
        .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
        .unwrap_err();
    assert!(error.to_string().contains("missing parameter"));
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows[0]["title"], Value::String("Old".into()));
}

#[test]
fn knowledge_property_batch_update_does_not_resolve_an_internal_id_as_an_external_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let before = db.commit_epoch().unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    let parameters = property_title_parameters("0", Some("New")).unwrap();
    let output = transaction
        .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
        .unwrap();
    assert!(output.rows.is_empty());
    transaction.commit().unwrap();
    assert_eq!(before, 1);
    assert_eq!(db.commit_epoch().unwrap(), before);
    let output = db
        .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["node_id"], Value::Int(0));
    assert_eq!(output.rows[0]["id"], Value::Null);
    assert_eq!(
        output.rows[0]["title"],
        Value::String("Idless memory".into())
    );
}

#[test]
fn read_only_database_rejects_knowledge_property_batch_query_update() {
    let path = unique_test_dir("read_only_property_batch_query_update");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old'})")
            .unwrap();
    }
    let wal_before = read_test_wal(&path).unwrap();
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
        let parameters = property_title_parameters("memory_1", Some("New")).unwrap();
        let error = db
            .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
    }
    assert_eq!(read_test_wal(&path).unwrap(), wal_before);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn knowledge_property_batch_queries_persist_as_one_wal_batch_and_replay() {
    let path = unique_test_dir("property_batch_query_wal_replay");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1', title: 'Old 1'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'memory_2', title: 'Old 2'})")
            .unwrap();
        let before = db.commit_epoch().unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        for (id, title) in [("memory_1", "New 1"), ("memory_2", "New 2")] {
            let parameters = property_title_parameters(id, Some(title)).unwrap();
            let output = transaction
                .query_request(bounded_batch_query(PROPERTY_TITLE_UPDATE, &parameters))
                .unwrap();
            assert_eq!(output.rows.len(), 1);
        }
        transaction.commit().unwrap();
        assert_eq!(db.commit_epoch().unwrap(), before + 1);
    }
    let wal = read_test_wal(&path).unwrap();
    assert!(wal.contains("set_node_property"));
    // Engine bootstrap is checkpointed; the two updates share one user batch.
    assert_eq!(wal.matches("\tbatch\t").count(), 1);
    {
        let mut db = Database::open(&path).unwrap();
        let output = db
            .query_request(bounded_batch_query(PROPERTY_SNAPSHOT, &BTreeMap::new()))
            .unwrap();
        assert_eq!(output.rows.len(), 2);
        assert_eq!(output.rows[0]["node_id"], Value::Int(0));
        assert_eq!(output.rows[0]["title"], Value::String("New 1".into()));
        assert_eq!(output.rows[1]["node_id"], Value::Int(1));
        assert_eq!(output.rows[1]["title"], Value::String("New 2".into()));
    }
    std::fs::remove_dir_all(path).unwrap();
}
