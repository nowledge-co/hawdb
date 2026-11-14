// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::QueryRequest;

fn bounded_query<'a>(query: &'a str, parameters: &'a BTreeMap<String, Value>) -> QueryRequest<'a> {
    QueryRequest::new(query)
        .with_params(parameters)
        .with_output_limits(QueryStreamOptions {
            max_rows: Some(4),
            max_payload_bytes: Some(4096),
        })
}

#[test]
fn deletes_entity_and_incident_edges_through_parameterized_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', title: 'First'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'HawDB'})").unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let output = db
        .query_request(
            QueryRequest::new("MATCH (m:Memory {id: $id}) DETACH DELETE m")
                .with_params(&parameters),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(db.commit_epoch().unwrap(), 2);
    assert!(db
        .query_request(bounded_query(
            "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id",
            &parameters
        ))
        .unwrap()
        .rows
        .is_empty());
    let parameters = BTreeMap::from([("id".into(), Value::String("entity_1".into()))]);
    assert_eq!(
        db.query_request(bounded_query(
            "MATCH (e:Entity {id: $id}) RETURN e.name AS name",
            &parameters
        ))
        .unwrap()
        .rows[0]["name"],
        Value::String("HawDB".into())
    );
    assert!(db
        .query_request(bounded_query(
            "MATCH ()-[r:MENTIONS]->(e:Entity {id: $id}) RETURN id(r) AS relationship_id",
            &parameters
        ))
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn scoped_entity_delete_does_not_write_filtered_seed() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1', source_id: 'thread_1', space_id: ''})")
        .unwrap();
    let parameters = BTreeMap::from([
        ("id".into(), Value::String("memory_1".into())),
        ("source_id".into(), Value::String("thread_2".into())),
    ]);
    let output = db
        .query_request(
            QueryRequest::new(
                "MATCH (m:Memory {id: $id}) WHERE m.source_id = $source_id DETACH DELETE m",
            )
            .with_params(&parameters),
        )
        .unwrap();
    assert!(output.rows.is_empty());
    assert_eq!(db.commit_epoch().unwrap(), 1);
    assert_eq!(
        db.query_request(bounded_query(
            "MATCH (m:Memory {id: $id}) RETURN m.source_id AS source_id",
            &parameters
        ))
        .unwrap()
        .rows[0]["source_id"],
        Value::String("thread_1".into())
    );
}

#[test]
fn external_id_delete_does_not_match_projected_idless_identity() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Idless memory'})")
        .unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("0".into()))]);
    assert!(db
        .query_request(
            QueryRequest::new("MATCH (m:Memory {id: $id}) DETACH DELETE m")
                .with_params(&parameters)
        )
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(db.commit_epoch().unwrap(), 1);
    let rows = db
        .query_request(bounded_query(
            "MATCH (m:Memory) RETURN id(m) AS node_id, m.title AS title",
            &parameters,
        ))
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["node_id"], Value::Int(0));
    assert_eq!(rows[0]["title"], Value::String("Idless memory".into()));
}

#[test]
fn entity_delete_rejects_invalid_query_identifier_without_writing() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
    let error = db
        .query_request(
            QueryRequest::new("MATCH (m:Bad-Label {id: $id}) DETACH DELETE m")
                .with_params(&parameters),
        )
        .unwrap_err();
    assert!(matches!(error, crate::HawDBError::Parse(_)), "{error}");
    assert_eq!(db.commit_epoch().unwrap(), 1);
}

#[test]
fn read_only_database_rejects_entity_delete_query() {
    let path = unique_test_dir("read_only_entity_delete_query");
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})").unwrap();
    }
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
        let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
        let error = db
            .query_request(
                QueryRequest::new("MATCH (m:Memory {id: $id}) DETACH DELETE m")
                    .with_params(&parameters),
            )
            .unwrap_err();
        assert!(error.to_string().contains("read-only"));
        assert_eq!(
            db.query_request(bounded_query(
                "MATCH (m:Memory {id: $id}) RETURN m.id AS id",
                &parameters
            ))
            .unwrap()
            .rows
            .len(),
            1
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn entity_delete_query_persists_and_replays_from_wal() {
    let path = unique_test_dir("entity_delete_query_wal_replay");
    let parameters = BTreeMap::from([("id".into(), Value::String("memory_1".into()))]);
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE (:Memory {id: 'memory_1'})-[:MENTIONS]->(:Entity {id: 'entity_1'})")
            .unwrap();
        db.query_request(
            QueryRequest::new("MATCH (m:Memory {id: $id}) DETACH DELETE m")
                .with_params(&parameters),
        )
        .unwrap();
    }
    assert!(read_test_wal(&path).unwrap().contains("delete_node"));
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db
            .query_request(bounded_query(
                "MATCH (m:Memory {id: $id}) RETURN m.id AS id",
                &parameters
            ))
            .unwrap()
            .rows
            .is_empty());
        let parameters = BTreeMap::from([("id".into(), Value::String("entity_1".into()))]);
        assert_eq!(
            db.query_request(bounded_query(
                "MATCH (e:Entity {id: $id}) RETURN e.id AS id",
                &parameters
            ))
            .unwrap()
            .rows
            .len(),
            1
        );
        assert!(db
            .query_request(bounded_query(
                "MATCH ()-[r:MENTIONS]->(e:Entity {id: $id}) RETURN id(r) AS relationship_id",
                &parameters
            ))
            .unwrap()
            .rows
            .is_empty());
    }
    std::fs::remove_dir_all(path).unwrap();
}
