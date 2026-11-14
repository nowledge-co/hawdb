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

//! Adapt real relational DDL metadata to the read-only SQL creation contract.

use crate::compile_relational_statement_sql;
use hawdb_sql::{
    bind_postgres_create_property_graph, bind_postgres_graph_tables, syntax, PgqBindingContext,
    PgqCreateBindErrorCode, PgqDataType, PgqSourceCatalog, PgqSourceColumnSchema,
    PgqSourceForeignKeySchema, PgqSourceTableSchema, PropertyGraphCatalog, SqlDataType,
};
use hawdb_storage::relational::{
    RelationalMutationLimits, RelationalOverflowConfig, RelationalScalarType, RelationalState,
};
use std::collections::BTreeMap;

#[derive(Debug)]
struct Snapshot(BTreeMap<Vec<String>, PgqSourceTableSchema>);

impl PgqSourceCatalog for Snapshot {
    fn source_table(&self, name: &[String]) -> Option<&PgqSourceTableSchema> {
        self.0.get(name).or_else(|| match name {
            [table] => self.0.get(&vec!["public".into(), table.clone()]),
            _ => None,
        })
    }
}

fn scalar_type(value: RelationalScalarType) -> SqlDataType {
    match value {
        RelationalScalarType::Boolean => SqlDataType::Boolean,
        RelationalScalarType::BigInt => SqlDataType::BigInt,
        RelationalScalarType::DoublePrecision => SqlDataType::DoublePrecision,
        RelationalScalarType::Decimal(kind) => SqlDataType::Decimal(kind),
        RelationalScalarType::Text => SqlDataType::Text,
        RelationalScalarType::Bytea => SqlDataType::Bytea,
        RelationalScalarType::Uuid => SqlDataType::Uuid,
        RelationalScalarType::Vector(dimension) => SqlDataType::Vector(dimension),
        RelationalScalarType::Timestamp(precision) => {
            SqlDataType::Timestamp(hawdb_core::sql_timestamp::TimestampType {
                zoned: false,
                precision,
            })
        }
        RelationalScalarType::TimestampTz(precision) => {
            SqlDataType::Timestamp(hawdb_core::sql_timestamp::TimestampType {
                zoned: true,
                precision,
            })
        }
    }
}

fn source_snapshot(state: &RelationalState, names: &[&str]) -> Snapshot {
    Snapshot(
        names
            .iter()
            .map(|name| {
                let table = state.table_schema(name).expect("created source table");
                let name = vec!["public".into(), table.name.clone()];
                let source = PgqSourceTableSchema {
                    name: name.clone(),
                    columns: table
                        .columns
                        .iter()
                        .map(|column| PgqSourceColumnSchema {
                            name: column.name.clone(),
                            data_type: scalar_type(column.scalar_type),
                            nullable: column.nullable,
                        })
                        .collect(),
                    primary_key: table.primary_key.clone(),
                    foreign_keys: table
                        .foreign_keys
                        .iter()
                        .map(|foreign_key| PgqSourceForeignKeySchema {
                            columns: foreign_key.columns.clone(),
                            referenced_table: vec![
                                "public".into(),
                                foreign_key.referenced_table.clone(),
                            ],
                            referenced_columns: foreign_key.referenced_columns.clone(),
                        })
                        .collect(),
                };
                (name, source)
            })
            .collect(),
    )
}

#[test]
fn real_ddl_snapshot_binds_creation_and_existing_graph_table_queries() {
    let mut state = RelationalState::default();
    for ddl in [
        "CREATE TABLE vertices (id BIGINT PRIMARY KEY, enabled BOOLEAN, score DOUBLE PRECISION, body TEXT, payload BYTEA, external_id UUID)",
        "CREATE TABLE links (id BIGINT PRIMARY KEY, source_id BIGINT REFERENCES vertices(id), destination_id BIGINT REFERENCES vertices(id))",
    ] {
        state = state.stage_transaction(compile_relational_statement_sql(ddl, &[], &state).unwrap(),
            RelationalMutationLimits::default(), RelationalOverflowConfig::default()).unwrap();
    }
    let snapshot = source_snapshot(&state, &["vertices", "links"]);
    let sql = "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS node)
        EDGE TABLES (links SOURCE KEY (source_id) REFERENCES node(id)
            DESTINATION KEY (destination_id) REFERENCES node(id))";
    let syntax::PostgresStatementSyntax::CreatePropertyGraph(create) =
        syntax::parse_postgres_statement(sql).unwrap()
    else {
        panic!()
    };
    let graph = bind_postgres_create_property_graph(sql, &create, &snapshot).unwrap();
    assert_eq!(
        graph.vertex_labels["node"].properties["payload"],
        PgqDataType::Binary
    );
    assert_eq!(
        graph.vertex_labels["node"].properties["external_id"],
        PgqDataType::Uuid
    );
    let mut catalog = PropertyGraphCatalog::default();
    catalog.insert(graph);
    let query = "SELECT * FROM GRAPH_TABLE(g MATCH (a IS node)-[e IS links]->(b IS node)
        COLUMNS (a.id, a.enabled, a.score, a.body, a.payload, b.external_id))";
    let select = syntax::parse_postgres_select(query).unwrap();
    let bound = bind_postgres_graph_tables(query, &select, &catalog, &PgqBindingContext::default())
        .unwrap();
    assert_eq!(
        bound[0]
            .columns
            .iter()
            .map(|column| column.data_type)
            .collect::<Vec<_>>(),
        [
            PgqDataType::Int64,
            PgqDataType::Boolean,
            PgqDataType::Float64,
            PgqDataType::String,
            PgqDataType::Binary,
            PgqDataType::Uuid
        ]
    );
    let sql = "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices)
        EDGE TABLES (links SOURCE vertices DESTINATION vertices)";
    let syntax::PostgresStatementSyntax::CreatePropertyGraph(create) =
        syntax::parse_postgres_statement(sql).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        bind_postgres_create_property_graph(sql, &create, &snapshot)
            .unwrap_err()
            .code,
        PgqCreateBindErrorCode::AmbiguousForeignKey
    );
    let vertices = snapshot.source_table(&["vertices".into()]).unwrap();
    assert!(!vertices.columns[0].nullable);
    assert!(vertices.columns[1..].iter().all(|column| column.nullable));
}

#[test]
fn timestamp_source_columns_fail_closed_as_graph_properties() {
    let mut state = RelationalState::default();
    state = state
        .stage_transaction(
            compile_relational_statement_sql(
                "CREATE TABLE stamped (id BIGINT PRIMARY KEY, at TIMESTAMP, seen TIMESTAMPTZ(3))",
                &[],
                &state,
            )
            .unwrap(),
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let snapshot = source_snapshot(&state, &["stamped"]);
    for (sql, expected) in [
        (
            "CREATE PROPERTY GRAPH g VERTEX TABLES (stamped)",
            "TIMESTAMP graph properties are not supported",
        ),
        (
            "CREATE PROPERTY GRAPH g VERTEX TABLES (stamped PROPERTIES (seen))",
            "TIMESTAMPTZ graph properties are not supported",
        ),
        (
            "CREATE PROPERTY GRAPH g VERTEX TABLES (stamped PROPERTIES (TIMESTAMP WITH TIME ZONE '2026-01-01' AS p))",
            "TIMESTAMP literals are not supported in PGQ",
        ),
        (
            "CREATE PROPERTY GRAPH g VERTEX TABLES (stamped PROPERTIES (CAST('2026-01-01' AS TIMESTAMP WITH TIME ZONE) AS p))",
            "parameterized scalar casts are not supported in PGQ",
        ),
    ] {
        let syntax::PostgresStatementSyntax::CreatePropertyGraph(create) =
            syntax::parse_postgres_statement(sql).unwrap()
        else {
            panic!("{sql}")
        };
        let error = bind_postgres_create_property_graph(sql, &create, &snapshot).unwrap_err();
        assert_eq!(error.code, PgqCreateBindErrorCode::UnsupportedExpression, "{sql}");
        assert!(error.message.contains(expected), "{sql}: {error:?}");
    }
}
