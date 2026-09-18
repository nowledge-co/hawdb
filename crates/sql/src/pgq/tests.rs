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

//! Binding cases adapted from PostgreSQL's SQL/PGQ regression suite.
//!
//! Reference revision: PostgreSQL 3d00537feb565c410baf41bb301eee338e4b2317.
//! Reference files: src/test/regress/sql/graph_table.sql and
//! src/backend/parser/parse_graphtable.c.

use hawdb_sql_syntax::parse_postgres_select;

use super::*;

fn catalog() -> PropertyGraphCatalog {
    let mut graph = PropertyGraphSchema::new(["knowledge"]);
    graph.add_vertex_label(
        "document",
        PropertyGraphElementSchema::default()
            .with_property("id", PgqDataType::Int64)
            .with_property("title", PgqDataType::String)
            .with_property("visible", PgqDataType::Boolean),
    );
    graph.add_vertex_label(
        "entity",
        PropertyGraphElementSchema::default()
            .with_property("id", PgqDataType::Int64)
            .with_property("name", PgqDataType::String)
            .with_property("kind", PgqDataType::String),
    );
    graph.add_edge_label(
        "mentions",
        PropertyGraphElementSchema::default().with_property("confidence", PgqDataType::Float64),
    );
    let mut catalog = PropertyGraphCatalog::default();
    catalog.insert(graph);
    catalog
}

#[test]
fn binds_graph_namespace_properties_and_output_schema() {
    let sql = "SELECT matched.name
        FROM source,
        GRAPH_TABLE (
            knowledge
            MATCH (document IS document
                   WHERE document.id = source.id AND document.visible)
                  -[mention IS mentions WHERE mention.confidence >= $1]->
                  (entity IS entity)
            WHERE entity.kind IN ('person', 'company')
            COLUMNS (
                entity.name AS name,
                mention.confidence,
                lower(entity.name) AS normalized
            )
        ) matched";
    let select = parse_postgres_select(sql).expect("owned SQL/PGQ syntax");
    let context =
        PgqBindingContext::default().with_outer_column(["source", "id"], PgqDataType::Int64);
    let bound =
        bind_postgres_graph_tables(sql, &select, &catalog(), &context).expect("bound GRAPH_TABLE");

    assert_eq!(bound.len(), 1);
    assert_eq!(bound[0].variables.len(), 3);
    assert_eq!(bound[0].alias.as_deref(), Some("matched"));
    assert_eq!(
        bound[0]
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.data_type))
            .collect::<Vec<_>>(),
        vec![
            ("name", PgqDataType::String),
            ("confidence", PgqDataType::Float64),
            ("normalized", PgqDataType::String),
        ]
    );
}

#[test]
fn binds_graph_tables_used_as_join_sources() {
    let sql = "SELECT matched.name
        FROM source
        LEFT JOIN GRAPH_TABLE (
            knowledge MATCH (entity IS entity)
            COLUMNS (entity.id AS id, entity.name AS name)
        ) matched ON matched.id = source.id";
    let select = parse_postgres_select(sql).expect("joined GRAPH_TABLE syntax");
    let bound = bind_postgres_graph_tables(sql, &select, &catalog(), &PgqBindingContext::default())
        .expect("joined GRAPH_TABLE binding");
    assert_eq!(bound.len(), 1);
    assert_eq!(bound[0].columns.len(), 2);
}

#[test]
fn applies_graph_table_column_alias_lists() {
    let sql = "SELECT matched.entity_id, matched.entity_name
        FROM GRAPH_TABLE (
            knowledge MATCH (entity IS entity)
            COLUMNS (entity.id, entity.name)
        ) matched(entity_id, entity_name)";
    let select = parse_postgres_select(sql).expect("GRAPH_TABLE column alias syntax");
    let bound = bind_postgres_graph_tables(sql, &select, &catalog(), &PgqBindingContext::default())
        .expect("GRAPH_TABLE column aliases");
    assert_eq!(
        bound[0]
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["entity_id", "entity_name"]
    );
}

#[test]
fn rejects_postgres_raw_graph_shapes_during_binding() {
    for (sql, code) in [
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH ()() COLUMNS (1 AS one))",
            PgqBindErrorCode::InvalidPath,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH -> COLUMNS (1 AS one))",
            PgqBindErrorCode::InvalidPath,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH ()-[]- COLUMNS (1 AS one))",
            PgqBindErrorCode::InvalidPath,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (left_node), (right_node) COLUMNS (1 AS one))",
            PgqBindErrorCode::InvalidPath,
        ),
    ] {
        let select = parse_postgres_select(sql).expect("PostgreSQL raw grammar accepts shape");
        let error = bind_postgres_graph_tables(
            sql,
            &select,
            &catalog(),
            &PgqBindingContext::default(),
        )
        .expect_err("graph transform stage must reject shape");
        assert_eq!(error.code, code);
    }
}

#[test]
fn rejects_unknown_catalog_references_with_source_spans() {
    for (sql, code) in [
        (
            "SELECT * FROM GRAPH_TABLE (missing MATCH (entity) COLUMNS (entity.id))",
            PgqBindErrorCode::UnknownGraph,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS missing) COLUMNS (entity.id))",
            PgqBindErrorCode::UnknownLabel,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS entity) COLUMNS (entity.missing))",
            PgqBindErrorCode::UnknownProperty,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS entity) COLUMNS (unknown.name))",
            PgqBindErrorCode::UnknownReference,
        ),
    ] {
        let select = parse_postgres_select(sql).expect("valid raw syntax");
        let error = bind_postgres_graph_tables(
            sql,
            &select,
            &catalog(),
            &PgqBindingContext::default(),
        )
        .expect_err("catalog mismatch must fail");
        assert_eq!(error.code, code);
        assert!(!error.span.is_empty());
    }
}

#[test]
fn rejects_unqualified_graph_table_output_expressions() {
    for (sql, code) in [
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS entity) COLUMNS (lower(entity.name)))",
            PgqBindErrorCode::MissingColumnName,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS entity) COLUMNS (entity.name, entity.kind AS name))",
            PgqBindErrorCode::DuplicateColumn,
        ),
        (
            "SELECT * FROM GRAPH_TABLE (knowledge MATCH (entity IS entity) COLUMNS (count(*) AS total))",
            PgqBindErrorCode::UnsupportedExpression,
        ),
    ] {
        let select = parse_postgres_select(sql).expect("valid raw syntax");
        let error = bind_postgres_graph_tables(
            sql,
            &select,
            &catalog(),
            &PgqBindingContext::default(),
        )
        .expect_err("GRAPH_TABLE output contract must fail closed");
        assert_eq!(error.code, code);
    }
}

#[test]
fn rejects_non_boolean_graph_predicates() {
    let sql = "SELECT * FROM GRAPH_TABLE (
        knowledge MATCH (entity IS entity WHERE entity.name + 1)
        COLUMNS (entity.name)
    )";
    let select = parse_postgres_select(sql).expect("valid raw syntax");
    let error = bind_postgres_graph_tables(sql, &select, &catalog(), &PgqBindingContext::default())
        .expect_err("typed binding must reject invalid operands");
    assert_eq!(error.code, PgqBindErrorCode::TypeMismatch);
}

#[test]
fn rejects_incompatible_graph_comparisons() {
    let sql = "SELECT * FROM GRAPH_TABLE (
        knowledge MATCH (entity IS entity WHERE entity.name > 1)
        COLUMNS (entity.name)
    )";
    let select = parse_postgres_select(sql).expect("valid raw syntax");
    let error = bind_postgres_graph_tables(sql, &select, &catalog(), &PgqBindingContext::default())
        .expect_err("typed binding must reject incompatible comparisons");
    assert_eq!(error.code, PgqBindErrorCode::TypeMismatch);
}
