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
use std::collections::BTreeMap;

fn setup(database: &mut Database) {
    let mut session = database.sql_session();
    session.query(
        "CREATE TABLE memories (id TEXT PRIMARY KEY, space_id TEXT NOT NULL, embedding VECTOR(1024))", &[],
    ).unwrap();
    session
        .query(
            "INSERT INTO memories (id, space_id) VALUES ('a', 'default')",
            &[],
        )
        .unwrap();
    session
        .query(
            "ALTER TABLE memories ADD COLUMN embedding_v2 VECTOR(768) DEFAULT NULL",
            &[],
        )
        .unwrap();
    session
        .query("CREATE INDEX memory_scope ON memories (space_id)", &[])
        .unwrap();
}

fn assert_schema_and_rows(database: &mut Database) {
    let mut session = database.sql_session();
    let columns = session.query(
        "SELECT column_name, dimensions, element_type, nullable FROM system.vector_columns WHERE table_name = 'memories' ORDER BY column_name", &[],
    ).unwrap().output.rows;
    assert_eq!(columns.len(), 2);
    for (column, (name, dimension)) in columns
        .iter()
        .zip([("embedding", 1024), ("embedding_v2", 768)])
    {
        assert_eq!(column["column_name"], Value::String(name.into()));
        assert_eq!(column["dimensions"], Value::Int(dimension));
        assert_eq!(column["element_type"], Value::String("float32".into()));
        assert_eq!(column["nullable"], Value::Bool(true));
    }
    let columns = session.query(
        "SELECT data_type, udt_name, udt_schema FROM information_schema.columns WHERE table_name = 'memories' AND column_name = 'embedding'", &[],
    ).unwrap().output.rows;
    assert_eq!(columns.len(), 1);
    assert_eq!(
        columns[0]["data_type"],
        Value::String("USER-DEFINED".into())
    );
    assert_eq!(columns[0]["udt_name"], Value::String("vector".into()));
    assert_eq!(columns[0]["udt_schema"], Value::String("public".into()));
    let rows = session.query(
        "SELECT id, embedding, embedding_v2 FROM memories WHERE space_id = 'default' AND embedding IS NULL ORDER BY id", &[],
    ).unwrap().output.rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], Value::String("a".into()));
    assert_eq!(rows[0]["embedding"], Value::Null);
    assert_eq!(rows[0]["embedding_v2"], Value::Null);
}

#[test]
fn nullable_vectors_survive_wal_and_checkpoint_reopen() {
    for checkpoint in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "hawdb-vector-schema-{}-{}-{checkpoint}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        {
            let mut database = Database::open(&root).unwrap();
            setup(&mut database);
            assert_schema_and_rows(&mut database);
            if checkpoint {
                database.checkpoint().unwrap();
            }
        }
        {
            let mut database = Database::open(&root).unwrap();
            assert_schema_and_rows(&mut database);
            database
                .sql_session()
                .query("UPDATE memories SET embedding = NULL WHERE id = 'a'", &[])
                .unwrap();
        }
        {
            let mut database = Database::open_with_config(
                &root,
                crate::DatabaseConfig {
                    read_only: true,
                    recovery_mode: crate::RecoveryMode::Strict,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_schema_and_rows(&mut database);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn unsupported_vector_writes_and_constraints_leave_no_partial_state() {
    let mut database = Database::new();
    setup(&mut database);
    for sql in [
        "CREATE TABLE invalid (id VECTOR(3) PRIMARY KEY)",
        "CREATE TABLE invalid (id TEXT PRIMARY KEY, v VECTOR(3) UNIQUE)",
        "CREATE TABLE invalid (id TEXT PRIMARY KEY, v VECTOR(3) DEFAULT '1,2,3')",
        "CREATE TABLE invalid (id TEXT PRIMARY KEY, v VECTOR(3) REFERENCES memories (id))",
        "CREATE INDEX invalid ON memories (embedding)",
        "CREATE UNIQUE INDEX invalid ON memories (embedding)",
        "ALTER TABLE memories ADD COLUMN invalid VECTOR(0)",
        "ALTER TABLE memories ADD COLUMN invalid VECTOR(3) NOT NULL",
        "INSERT INTO memories (id, space_id, embedding) VALUES ('b', 'default', '1,2,3')",
        "UPDATE memories SET embedding = 1 WHERE id = 'a'",
        "CREATE TABLE invalid (stream TEXT NOT NULL, seq BIGINT NOT NULL, v VECTOR(3)) WITH (storage_mode = 'strict_append', partition_key = 'stream', order_key = 'seq')",
    ] {
        let before = database.commit_epoch().unwrap();
        assert!(database.sql_session().transaction(|transaction| {
            transaction.query("INSERT INTO memories (id, space_id) VALUES ('rollback', 'default')", &[])?;
            transaction.query(sql, &[])?;
            Ok(())
        }).is_err(), "{sql}");
        assert_eq!(database.commit_epoch().unwrap(), before, "{sql}");
        assert_schema_and_rows(&mut database);
    }
    for value in [
        Value::List(vec![Value::Float(1.0); 1024]),
        Value::Binary(vec![0; 4096]),
    ] {
        assert!(database
            .sql_session()
            .query(
                "UPDATE memories SET embedding = $1 WHERE id = 'a'",
                &[value],
            )
            .is_err());
    }
    assert_schema_and_rows(&mut database);
    database
        .sql_session()
        .query(
            "CREATE TABLE required (id TEXT PRIMARY KEY, v VECTOR(3) NOT NULL)",
            &[],
        )
        .unwrap();
    assert!(database
        .sql_session()
        .query("INSERT INTO required (id) VALUES ('a')", &[])
        .is_err());
}

fn vector_value(values: &[f32]) -> Value {
    Value::Vector(crate::Vector::new(values).unwrap())
}

fn setup_vector_values(database: &mut Database) {
    database.sql_session().query("CREATE TABLE vector_rows (id BIGINT PRIMARY KEY, eligible BOOLEAN NOT NULL, embedding VECTOR(2))", &[]).unwrap();
    database.query("CREATE NODE TABLE VectorNode").unwrap();
    database
        .query("CREATE PROPERTY ON NODE TABLE VectorNode(embedding) TYPE VECTOR(2)")
        .unwrap();
    for (id, eligible, embedding) in [
        (1, false, vector_value(&[1.0, 0.0])),
        (2, true, vector_value(&[0.8, 0.6])),
        (3, true, vector_value(&[0.6, 0.8])),
        (4, true, vector_value(&[-1.0, 0.0])),
        (5, true, Value::Null),
    ] {
        database
            .sql_session()
            .query(
                "INSERT INTO vector_rows (id, eligible, embedding) VALUES ($1, $2, $3)",
                &[Value::Int(id), Value::Bool(eligible), embedding.clone()],
            )
            .unwrap();
        database
            .query_with_params(
                "CREATE (:VectorNode {id: $id, eligible: $eligible, embedding: $embedding})",
                &BTreeMap::from([
                    ("id".into(), Value::Int(id)),
                    ("eligible".into(), Value::Bool(eligible)),
                    ("embedding".into(), embedding),
                ]),
            )
            .unwrap();
    }
    database.query("CREATE (:VectorRoot {id: 1})").unwrap();
    for id in [3, 4] {
        database.query_with_params("MATCH (r:VectorRoot), (n:VectorNode) WHERE n.id = $id CREATE (r)-[:VECTOR_LINK]->(n)", &BTreeMap::from([("id".into(), Value::Int(id))])).unwrap();
    }
}

fn assert_vector_queries(database: &mut Database) {
    for function in ["cosine_distance", "l2_distance"] {
        let query = vector_value(&[1.0, 0.0]);
        let sql = format!("SELECT id, {function}(embedding, $1) AS distance FROM vector_rows WHERE eligible = true AND embedding IS NOT NULL ORDER BY distance, id LIMIT 2");
        let cypher = format!("MATCH (n:VectorNode) WHERE n.eligible = true AND n.embedding IS NOT NULL RETURN n.id AS id, {function}(n.embedding, $q) AS distance ORDER BY distance, id LIMIT 2");
        let sql_rows = database
            .sql_session()
            .query(&sql, std::slice::from_ref(&query))
            .unwrap()
            .output
            .rows;
        let graph_rows = database
            .query_with_params(&cypher, &BTreeMap::from([("q".into(), query.clone())]))
            .unwrap()
            .rows;
        // Cypher still sorts RETURN columns by name, so compare rows by name.
        assert_eq!(sql_rows, graph_rows.into_rows(), "{function}");
        assert_eq!(
            sql_rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(2), Value::Int(3)]
        );
        let traversal = format!("MATCH (r:VectorRoot)-[:VECTOR_LINK]->(n:VectorNode) RETURN n.id AS id, {function}(n.embedding, $q) AS distance ORDER BY distance, id LIMIT 2");
        let candidates = database
            .query_with_params(&traversal, &BTreeMap::from([("q".into(), query)]))
            .unwrap()
            .rows;
        assert_eq!(
            candidates
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            vec![Value::Int(3), Value::Int(4)]
        );
    }
    let sql = database
        .sql_session()
        .query("SELECT id, embedding FROM vector_rows ORDER BY id", &[])
        .unwrap()
        .output
        .rows;
    let graph = database
        .query("MATCH (n:VectorNode) RETURN n.id AS id, n.embedding AS embedding ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(sql, graph.into_rows());
    assert_eq!(
        database
            .query("MATCH (n:VectorNode) RETURN n.id AS id ORDER BY id")
            .unwrap()
            .rows
            .len(),
        5
    );
    assert_eq!(
        database
            .sql_session()
            .query("SELECT id FROM vector_rows ORDER BY id", &[])
            .unwrap()
            .output
            .rows
            .len(),
        5
    );
}

#[test]
fn shared_vector_values_distances_and_traversal_survive_reopen() {
    for checkpoint in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "hawdb-shared-vector-{}-{checkpoint}-{}",
            std::process::id(),
            hawdb_core::generate_uuidv7().unwrap()
        ));
        {
            let mut database = Database::open(&root).unwrap();
            setup_vector_values(&mut database);
            assert_vector_queries(&mut database);
            if checkpoint {
                database.checkpoint().unwrap();
            }
        }
        {
            let mut database = Database::open_with_config(
                &root,
                crate::DatabaseConfig {
                    read_only: true,
                    recovery_mode: crate::RecoveryMode::Strict,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_vector_queries(&mut database);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn shared_vector_dimension_errors_roll_back_and_nulls_propagate() {
    let mut database = Database::new();
    setup_vector_values(&mut database);
    let wrong = vector_value(&[1.0]);
    let before = database.commit_epoch().unwrap();
    assert!(database
        .sql_session()
        .transaction(|transaction| {
            transaction.query("UPDATE vector_rows SET eligible = false WHERE id = 2", &[])?;
            transaction.query(
                "UPDATE vector_rows SET embedding = $1 WHERE id = 2",
                std::slice::from_ref(&wrong),
            )?;
            Ok(())
        })
        .is_err());
    assert_eq!(database.commit_epoch().unwrap(), before);
    assert!(database
        .query_with_params(
            "MATCH (n:VectorNode) WHERE n.id = 2 SET n.embedding = $q",
            &BTreeMap::from([("q".into(), wrong.clone())])
        )
        .is_err());
    assert_eq!(database.commit_epoch().unwrap(), before);
    for function in ["cosine_distance", "l2_distance"] {
        let sql =
            format!("SELECT {function}(embedding, $1) AS distance FROM vector_rows WHERE id = 5");
        let output = database
            .sql_session()
            .query(&sql, &[vector_value(&[0.0, 0.0])])
            .unwrap();
        assert_eq!(output.output.rows[0]["distance"], Value::Null);
        let sql =
            format!("SELECT {function}(embedding, $1) AS distance FROM vector_rows WHERE id = 2");
        assert!(database
            .sql_session()
            .query(&sql, std::slice::from_ref(&wrong))
            .is_err());
        let cypher = format!(
            "MATCH (n:VectorNode) WHERE n.id = 2 RETURN {function}(n.embedding, $q) AS distance"
        );
        assert!(database
            .query_with_params(&cypher, &BTreeMap::from([("q".into(), wrong.clone())]))
            .is_err());
    }
    assert_vector_queries(&mut database);
}

#[test]
fn shared_vector_metrics_have_independent_ordering_oracles_and_tie_breaks() {
    let mut database = Database::new();
    database
        .sql_session()
        .query(
            "CREATE TABLE metric_rows (id BIGINT PRIMARY KEY, embedding VECTOR(2))",
            &[],
        )
        .unwrap();
    database.query("CREATE (:MetricRoot)").unwrap();
    for (id, coordinates) in [
        (3, [1.0, 0.5]),
        (4, [10.0, 0.0]),
        (2, [1.0, 1.0]),
        (1, [1.0, 0.5]),
    ] {
        let vector = vector_value(&coordinates);
        database
            .sql_session()
            .query(
                "INSERT INTO metric_rows (id, embedding) VALUES ($1, $2)",
                &[Value::Int(id), vector.clone()],
            )
            .unwrap();
        database
            .query_with_params(
                "CREATE (:MetricNode {id: $id, embedding: $v})",
                &BTreeMap::from([("id".into(), Value::Int(id)), ("v".into(), vector)]),
            )
            .unwrap();
        database
            .query_with_params(
                "MATCH (r:MetricRoot), (n:MetricNode) WHERE n.id = $id CREATE (r)-[:METRIC_LINK]->(n)",
                &BTreeMap::from([("id".into(), Value::Int(id))]),
            )
            .unwrap();
    }
    for (function, expected) in [
        (
            "cosine_distance",
            [(4, 0.0), (1, 1.0 - 1.0 / 1.25_f64.sqrt())],
        ),
        ("l2_distance", [(1, 0.5), (3, 0.5)]),
    ] {
        let sql = database.sql_session().query(&format!("SELECT id, {function}(embedding, $1) AS distance FROM metric_rows ORDER BY distance, id LIMIT 2"), &[vector_value(&[1.0, 0.0])]).unwrap().output.rows;
        for pattern in [
            "(n:MetricNode)",
            "(:MetricRoot)-[:METRIC_LINK]->(n:MetricNode)",
        ] {
            let graph = database.query_with_params(&format!("MATCH {pattern} RETURN n.id AS id, {function}(n.embedding, $q) AS distance ORDER BY distance, id LIMIT 2"), &BTreeMap::from([("q".into(), vector_value(&[1.0, 0.0]))])).unwrap().rows;
            // Cypher still sorts RETURN columns by name, so compare rows by name.
            assert_eq!(sql, graph.into_rows());
        }
        assert_eq!(sql.len(), 2);
        for (row, (id, distance)) in sql.iter().zip(expected) {
            assert_eq!(row["id"], Value::Int(id));
            let Value::Float(actual) = row["distance"] else {
                panic!("distance must be Float64");
            };
            assert!(
                (actual - distance).abs() < 1e-12,
                "{function}: {actual} != {distance}"
            );
        }
    }
}

#[test]
fn shared_vector_sql_scalar_projection_dispatch_and_nested_validation() {
    let mut database = Database::new();
    setup_vector_values(&mut database);
    let query = vector_value(&[1.0, 0.0]);
    for projection in [
        "l2_distance(embedding, $1)",
        "coalesce(l2_distance(embedding, $1), 9.0)",
    ] {
        let rows = database
            .sql_session()
            .query(
                &format!("SELECT id, {projection} AS distance FROM vector_rows"),
                std::slice::from_ref(&query),
            )
            .unwrap()
            .output
            .rows;
        assert_eq!(rows.len(), 5);
        let null_replacement = if projection.starts_with("coalesce") {
            Value::Float(9.0)
        } else {
            Value::Null
        };
        assert_eq!(
            rows.iter().find(|row| row["id"] == Value::Int(5)).unwrap()["distance"],
            null_replacement
        );
    }
    database
        .sql_session()
        .query(
            "CREATE TABLE empty_vectors (id BIGINT PRIMARY KEY, embedding VECTOR(2))",
            &[],
        )
        .unwrap();
    for table in ["vector_rows", "empty_vectors"] {
        assert!(database
            .sql_session()
            .query(
                &format!(
                    "SELECT l2_distance(coalesce(embedding, $1), $1) AS distance FROM {table}"
                ),
                std::slice::from_ref(&query)
            )
            .is_err());
    }
    let cypher = "MATCH (n:VectorNode) WHERE n.eligible = true AND n.embedding IS NOT NULL RETURN n.id AS id, cosine_distance(n.embedding, $q) AS distance ORDER BY distance, id LIMIT 1";
    for (coordinates, id) in [([1.0, 0.0], 2), ([-1.0, 0.0], 4), ([1.0, 0.0], 2)] {
        let rows = database
            .query_with_params(
                cypher,
                &BTreeMap::from([("q".into(), vector_value(&coordinates))]),
            )
            .unwrap()
            .rows;
        assert_eq!(rows[0]["id"], Value::Int(id));
    }
}
