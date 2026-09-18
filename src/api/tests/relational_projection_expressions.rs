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

fn projection_fixture() -> Database {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE feeds (\
                id BIGINT PRIMARY KEY, \
                source_title TEXT NOT NULL, \
                title_override TEXT\
            )",
        )
        .expect("create feeds table");
    database
        .query_sql(
            "INSERT INTO feeds (id, source_title, title_override) VALUES \
             (1, 'Source One', 'Local One'), \
             (2, 'Source Two', NULL)",
        )
        .expect("insert feeds");
    database
}

fn projected_title(output: &QueryOutput, id: i64) -> Value {
    output
        .rows
        .iter()
        .find(|row| row.get("id") == Some(&Value::Int(id)))
        .and_then(|row| row.get("title"))
        .cloned()
        .unwrap_or_else(|| panic!("missing title for id {id}"))
}

#[test]
fn relational_projection_coalesce_reads_columns_parameters_and_literals() {
    let mut database = projection_fixture();

    let output = database
        .query_sql(
            "SELECT id, COALESCE(title_override, source_title) AS title \
             FROM feeds",
        )
        .expect("project column coalesce");
    assert_eq!(
        projected_title(&output, 1),
        Value::String("Local One".to_string())
    );
    assert_eq!(
        projected_title(&output, 2),
        Value::String("Source Two".to_string())
    );

    let ordered = database
        .query_sql(
            "SELECT id, COALESCE(title_override, source_title) AS title \
             FROM feeds ORDER BY title DESC",
        )
        .expect("order by coalesce projection alias");
    assert_eq!(
        ordered
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        [Value::Int(2), Value::Int(1)]
    );

    let parameterized = database
        .query_sql_with_params(
            "SELECT id, COALESCE(title_override, $1, 'Fallback') AS title \
             FROM feeds ORDER BY id ASC",
            &[Value::String("Parameter".to_string())],
        )
        .expect("project parameter coalesce");
    assert_eq!(
        projected_title(&parameterized, 2),
        Value::String("Parameter".to_string())
    );

    let nulls = database
        .query_sql_with_params(
            "SELECT id, COALESCE(title_override, $1) AS title FROM feeds",
            &[Value::Null],
        )
        .expect("project null coalesce");
    assert_eq!(projected_title(&nulls, 2), Value::Null);

    let distinct = database
        .query_sql_with_params(
            "SELECT DISTINCT COALESCE(title_override, $1) AS title FROM feeds",
            &[Value::String("Parameter".to_string())],
        )
        .expect("project distinct coalesce");
    assert_eq!(distinct.rows.len(), 2);
    assert!(distinct
        .rows
        .iter()
        .any(|row| row.get("title") == Some(&Value::String("Local One".to_string()))));
    assert!(distinct
        .rows
        .iter()
        .any(|row| row.get("title") == Some(&Value::String("Parameter".to_string()))));
}

#[test]
fn relational_projection_coalesce_reads_bounded_joins() {
    let mut database = projection_fixture();
    database
        .query_sql(
            "CREATE TABLE feed_overrides (\
                feed_id BIGINT PRIMARY KEY, \
                title TEXT NOT NULL\
            )",
        )
        .expect("create feed overrides");
    database
        .query_sql("INSERT INTO feed_overrides (feed_id, title) VALUES (2, 'Joined Two')")
        .expect("insert feed override");

    let output = database
        .query_sql(
            "SELECT f.id, COALESCE(o.title, f.source_title) AS title \
             FROM feeds AS f LEFT JOIN feed_overrides AS o ON o.feed_id = f.id \
             ORDER BY f.id ASC",
        )
        .expect("project joined coalesce");
    assert_eq!(
        projected_title(&output, 1),
        Value::String("Source One".to_string())
    );
    assert_eq!(
        projected_title(&output, 2),
        Value::String("Joined Two".to_string())
    );
}

#[test]
fn relational_projection_coalesce_rejects_invalid_types_before_scanning() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE empty_feeds (id BIGINT PRIMARY KEY, title TEXT)")
        .expect("create empty feeds");

    let error = database
        .query_sql("SELECT COALESCE(title, 1) AS title FROM empty_feeds")
        .expect_err("incompatible coalesce must fail without input rows");
    assert!(
        error
            .to_string()
            .contains("COALESCE arguments have incompatible scalar types"),
        "unexpected error: {error}"
    );

    let parameter_error = database
        .query_sql_with_params(
            "SELECT COALESCE(title, $1) AS title FROM empty_feeds",
            &[Value::Int(1)],
        )
        .expect_err("incompatible parameter must fail without input rows");
    assert!(
        parameter_error
            .to_string()
            .contains("COALESCE arguments have incompatible scalar types"),
        "unexpected error: {parameter_error}"
    );

    let empty = database
        .query_sql("SELECT COALESCE() AS title FROM empty_feeds")
        .expect_err("empty coalesce must fail during binding");
    assert!(
        empty
            .to_string()
            .contains("COALESCE requires at least one argument"),
        "unexpected error: {empty}"
    );
}

#[test]
fn relational_projection_coalesce_keeps_result_limits() {
    let mut database = projection_fixture();

    let row_error = database
        .query_sql_with_params_options(
            "SELECT COALESCE(title_override, source_title) AS title FROM feeds",
            &[],
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: None,
            },
        )
        .expect_err("coalesce projection must honor the row limit");
    assert!(
        row_error
            .to_string()
            .contains("relational SQL output exceeds max_output_rows 1"),
        "unexpected error: {row_error}"
    );

    let error = database
        .query_sql_with_params_options(
            "SELECT COALESCE(title_override, source_title) AS title FROM feeds",
            &[],
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: Some(1),
            },
        )
        .expect_err("coalesce projection must honor the payload limit");
    assert!(
        error
            .to_string()
            .contains("relational SQL output exceeds max_output_payload_bytes 1"),
        "unexpected error: {error}"
    );
}
