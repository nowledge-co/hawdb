// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE lengths (id BIGINT PRIMARY KEY, body TEXT, payload BYTEA)")
        .unwrap();
    for (id, body, payload) in [
        (
            1,
            Value::String("\u{e9}\u{1f980}".into()),
            Value::Binary(vec![0, 255, 1]),
        ),
        (2, Value::String(String::new()), Value::Binary(vec![])),
        (3, Value::Null, Value::Null),
    ] {
        database
            .query_sql_with_params(
                "INSERT INTO lengths (id, body, payload) VALUES ($1, $2, $3)",
                &[Value::Int(id), body, payload],
            )
            .unwrap();
    }
    database
}

#[test]
fn sql_octet_length_projects_bytes_through_streams_sorts_and_joins() {
    let mut database = fixture();
    for sql in [
        "SELECT id, octet_length(body) AS body_bytes, octet_length(payload) AS payload_bytes FROM lengths",
        "SELECT id, octet_length(body) AS body_bytes, octet_length(payload) AS payload_bytes FROM lengths ORDER BY id",
        "SELECT a.id, octet_length(a.body) AS body_bytes, octet_length(b.payload) AS payload_bytes FROM lengths AS a JOIN lengths AS b ON a.id = b.id ORDER BY a.id",
    ] {
        let output = database.query_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(output.rows.len(), 3, "{sql}");
        for row in &output.rows {
            let (body, payload) = match row["id"] {
                Value::Int(1) => (Value::Int(6), Value::Int(3)),
                Value::Int(2) => (Value::Int(0), Value::Int(0)),
                Value::Int(3) => (Value::Null, Value::Null),
                ref other => panic!("unexpected id: {other:?}"),
            };
            assert_eq!(row["body_bytes"], body, "{sql}");
            assert_eq!(row["payload_bytes"], payload, "{sql}");
        }
    }
    let coalesced = database.query_sql(
        "SELECT id, COALESCE(OCTET_LENGTH(body), 0) AS body_bytes, COALESCE(OCTET_LENGTH(payload), 0) AS payload_bytes FROM lengths ORDER BY id",
    ).unwrap();
    for (row, (body_bytes, payload_bytes)) in coalesced.rows.iter().zip([(6, 3), (0, 0), (0, 0)]) {
        assert_eq!(row["body_bytes"], Value::Int(body_bytes));
        assert_eq!(row["payload_bytes"], Value::Int(payload_bytes));
    }
    assert_eq!(coalesced.rows.len(), 3);
    let output = database.query_sql_with_params(
        "SELECT octet_length($1) AS bytes, octet_length('') AS empty, octet_length(NULL) AS absent FROM lengths WHERE id = 1",
        &[Value::String("\u{e9}\u{1f980}".into())],
    ).unwrap();
    assert_eq!(output.rows[0]["bytes"], Value::Int(6));
    assert_eq!(output.rows[0]["empty"], Value::Int(0));
    assert_eq!(output.rows[0]["absent"], Value::Null);
}

#[test]
fn sql_octet_length_predicates_preserve_nulls_parameters_and_aggregate_results() {
    let mut database = fixture();
    for sql in [
        "SELECT id FROM lengths WHERE octet_length(body) = $1 AND octet_length(payload) <= 3",
        "SELECT id FROM lengths WHERE NOT (octet_length(body) < $1) AND octet_length(payload) IN (2, 3)",
        "SELECT id FROM lengths WHERE octet_length(body) + 1 = $1 + 1",
    ] {
        let output = database.query_sql_with_params(sql, &[Value::Int(6)]).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(output.rows.len(), 1, "{sql}");
        assert_eq!(output.rows[0]["id"], Value::Int(1), "{sql}");
    }
    let nulls = database.query_sql("SELECT id FROM lengths WHERE octet_length(payload) IS NULL OR octet_length(body) = 0 ORDER BY id").unwrap();
    assert_eq!(nulls.rows.len(), 2);
    assert_eq!(nulls.rows[0]["id"], Value::Int(2));
    assert_eq!(nulls.rows[1]["id"], Value::Int(3));
    let joined = database.query_sql(
        "SELECT a.id FROM lengths AS a JOIN lengths AS b ON a.id = b.id AND octet_length(a.body) = octet_length(b.body) ORDER BY a.id",
    ).unwrap();
    assert_eq!(joined.rows.len(), 2);
    assert_eq!(joined.rows[0]["id"], Value::Int(1));
    assert_eq!(joined.rows[1]["id"], Value::Int(2));
    let total = database
        .query_sql(
            "SELECT SUM(OCTET_LENGTH(body)) AS bytes FROM lengths WHERE octet_length(payload) <= 3",
        )
        .unwrap();
    assert_eq!(total.rows[0]["bytes"], Value::Int(6));
}

#[test]
fn sql_octet_length_refuses_invalid_inputs_even_without_rows() {
    let mut database = fixture();
    database
        .query_sql("DELETE FROM lengths WHERE id >= 1")
        .unwrap();
    for expression in ["octet_length(id)", "octet_length(1)", "octet_length(TRUE)"] {
        let error = database
            .query_sql(&format!("SELECT {expression} FROM lengths"))
            .expect_err("invalid types must fail before scanning an empty table");
        assert!(
            error
                .to_string()
                .contains("OCTET_LENGTH requires TEXT or BYTEA input"),
            "{expression}: {error}"
        );
    }
    for sql in [
        "SELECT id FROM lengths WHERE octet_length(id) = 1",
        "SELECT id FROM lengths WHERE id = 1 AND octet_length(id) = 1",
        "SELECT a.id FROM lengths AS a JOIN lengths AS b ON a.id = b.id WHERE octet_length(a.id) = 1",
        "SELECT a.id FROM lengths AS a JOIN lengths AS b ON octet_length(a.id) = 1",
    ] {
        let error = database.query_sql(sql).expect_err("predicate types must not depend on rows or access path");
        assert!(error.to_string().contains("OCTET_LENGTH requires TEXT or BYTEA input"), "{sql}: {error}");
    }
    for expression in [
        "octet_length()",
        "octet_length(body, body)",
        "octet_length(*)",
        "octet_length(DISTINCT body)",
    ] {
        assert!(
            database
                .query_sql(&format!("SELECT {expression} FROM lengths"))
                .is_err(),
            "{expression}"
        );
        assert!(
            database
                .query_sql(&format!("SELECT id FROM lengths WHERE {expression} = 1"))
                .is_err(),
            "{expression}"
        );
    }
    assert!(database
        .query_sql("SELECT id FROM lengths WHERE sum(id) = 1")
        .is_err());
}
