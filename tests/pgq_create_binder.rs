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

//! This external consumer imports only the embedded facade and the standard library.
use hawdb::sql::{
    self, syntax, PgqCreateBindErrorCode, PgqDataType, PgqSourceCatalog, PgqSourceColumnSchema,
    PgqSourceForeignKeySchema, PgqSourceTableSchema, SqlDataType,
};

struct Snapshot(PgqSourceTableSchema);
impl PgqSourceCatalog for Snapshot {
    fn source_table(&self, name: &[String]) -> Option<&PgqSourceTableSchema> {
        (name == self.0.name).then_some(&self.0)
    }
}

#[test]
fn embedded_catalog_can_install_a_bound_descriptor_without_changing_query_entrypoints() {
    let source = Snapshot(PgqSourceTableSchema {
        name: vec!["source".into()],
        columns: vec![
            PgqSourceColumnSchema {
                name: "id".into(),
                data_type: SqlDataType::BigInt,
                nullable: false,
            },
            PgqSourceColumnSchema {
                name: "payload".into(),
                data_type: SqlDataType::Bytea,
                nullable: true,
            },
            PgqSourceColumnSchema {
                name: "uuid".into(),
                data_type: SqlDataType::Uuid,
                nullable: true,
            },
        ],
        primary_key: vec!["id".into()],
        foreign_keys: Vec::<PgqSourceForeignKeySchema>::new(),
    });
    let sql = "CREATE PROPERTY GRAPH g VERTEX TABLES (source)";
    let syntax::PostgresStatementSyntax::CreatePropertyGraph(create) =
        syntax::parse_postgres_statement(sql).unwrap()
    else {
        panic!()
    };
    let graph = sql::bind_postgres_create_property_graph(sql, &create, &source).unwrap();
    let mut catalog = sql::PropertyGraphCatalog::default();
    catalog.insert(graph);
    let query = "SELECT * FROM GRAPH_TABLE(g MATCH (n IS source) COLUMNS (n.payload, n.uuid))";
    let select = syntax::parse_postgres_select(query).unwrap();
    let bound = sql::bind_postgres_graph_tables(
        query,
        &select,
        &catalog,
        &sql::PgqBindingContext::default(),
    )
    .unwrap();
    assert_eq!(bound[0].columns[0].data_type, PgqDataType::Binary);
    assert_eq!(bound[0].columns[1].data_type, PgqDataType::Uuid);
    let sql = "CREATE PROPERTY GRAPH bad VERTEX TABLES (missing)";
    let syntax::PostgresStatementSyntax::CreatePropertyGraph(create) =
        syntax::parse_postgres_statement(sql).unwrap()
    else {
        panic!()
    };
    let error = sql::bind_postgres_create_property_graph(sql, &create, &source).unwrap_err();
    assert_eq!(error.code, PgqCreateBindErrorCode::UnknownTable);
    let _: &dyn std::error::Error = &error;
    assert!(error.to_string().contains("UnknownTable"));
}
