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

//! Creation semantics adapted from PostgreSQL 3d00537feb565c410baf41bb301eee338e4b2317,
//! src/test/regress/sql/create_property_graph.sql and propgraphcmds.c.
//! These tests exercise the explicitly supported six-scalar profile.

use super::*;
use hawdb_sql_syntax::{parse_postgres_statement, ExpressionKindSyntax, PostgresStatementSyntax};

#[derive(Default, Clone)]
struct Snapshot(BTreeMap<Vec<String>, PgqSourceTableSchema>);

impl PgqSourceCatalog for Snapshot {
    fn source_table(&self, name: &[String]) -> Option<&PgqSourceTableSchema> {
        self.0.get(name).or_else(|| match name {
            [table] => self.0.get(&vec!["public".into(), table.clone()]),
            _ => None,
        })
    }
}

fn table(name: &str, columns: &[(&str, SqlDataType)]) -> PgqSourceTableSchema {
    PgqSourceTableSchema {
        name: vec!["public".into(), name.into()],
        columns: columns
            .iter()
            .map(|(name, data_type)| PgqSourceColumnSchema {
                name: (*name).into(),
                data_type: *data_type,
                nullable: *name != "id",
            })
            .collect(),
        primary_key: vec!["id".into()],
        foreign_keys: vec![],
    }
}

fn snapshot() -> Snapshot {
    use SqlDataType::*;
    let tables = [
        table(
            "vertices",
            &[
                ("id", BigInt),
                ("alternate", BigInt),
                ("score", DoublePrecision),
                ("title", Text),
                ("enabled", Boolean),
                ("payload", Bytea),
                ("external_id", Uuid),
            ],
        ),
        table(
            "links",
            &[
                ("id", BigInt),
                ("source_id", BigInt),
                ("destination_id", BigInt),
                ("score", DoublePrecision),
            ],
        ),
    ];
    Snapshot(
        tables
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect(),
    )
}

fn create(sql: &str) -> CreatePropertyGraph {
    let PostgresStatementSyntax::CreatePropertyGraph(create) =
        parse_postgres_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"))
    else {
        panic!("CREATE expected")
    };
    create
}

fn bind(sql: &str, snapshot: &Snapshot) -> Result<PropertyGraphSchema> {
    bind_postgres_create_property_graph(sql, &create(sql), snapshot)
}

fn code(sql: &str, snapshot: &Snapshot, expected: Code) {
    let error = bind(sql, snapshot).unwrap_err();
    assert_eq!(error.code, expected, "{sql}: {error}");
    assert!(sql.get(error.span.start..error.span.end).is_some());
}

fn explicit_endpoint(edge: &str, vertex: &str) -> String {
    format!(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices NO PROPERTIES)
        EDGE TABLES (links SOURCE KEY ({edge}) REFERENCES vertices({vertex})
        DESTINATION KEY (destination_id) REFERENCES vertices(id) NO PROPERTIES)"
    )
}

#[test]
fn empty_graph_is_read_only_and_all_six_types_reach_graph_table() {
    assert_eq!(
        bind("CREATE TEMP PROPERTY GRAPH g", &Snapshot::default()).unwrap(),
        PropertyGraphSchema::new(["g"])
    );
    let graph = bind(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices)",
        &snapshot(),
    )
    .unwrap();
    assert_eq!(graph.vertex_labels["vertices"].properties.len(), 7);
    let mut catalog = super::super::PropertyGraphCatalog::default();
    catalog.insert(graph);
    let sql = "SELECT * FROM GRAPH_TABLE(g MATCH (v IS vertices)
        COLUMNS (v.id, v.score, v.title, v.enabled, v.payload, v.external_id))";
    let query = hawdb_sql_syntax::parse_postgres_select(sql).unwrap();
    let bound = super::super::bind_postgres_graph_tables(
        sql,
        &query,
        &catalog,
        &super::super::PgqBindingContext::default(),
    )
    .unwrap();
    assert_eq!(
        bound[0]
            .columns
            .iter()
            .map(|column| column.data_type)
            .collect::<Vec<_>>(),
        [
            PgqDataType::Int64,
            PgqDataType::Float64,
            PgqDataType::String,
            PgqDataType::Boolean,
            PgqDataType::Binary,
            PgqDataType::Uuid
        ]
    );
}

#[test]
fn omitted_key_requires_primary_key_but_explicit_key_allows_nullable_unconstrained_columns() {
    let mut snapshot = snapshot();
    let source = snapshot
        .0
        .get_mut(&vec!["public".into(), "vertices".into()])
        .unwrap();
    source.primary_key.clear();
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices)",
        &snapshot,
        Code::MissingKey,
    );
    bind(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices KEY (alternate, title))",
        &snapshot,
    )
    .unwrap();
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices KEY (alternate, alternate))",
        &snapshot,
        Code::InvalidKey,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices KEY (missing))",
        &snapshot,
        Code::UnknownColumn,
    );
}

#[test]
fn endpoints_are_directional_and_can_reference_non_key_columns_without_foreign_keys() {
    bind(&explicit_endpoint("source_id", "alternate"), &snapshot()).unwrap();
    bind(&explicit_endpoint("source_id", "score"), &snapshot()).unwrap();
    code(
        &explicit_endpoint("score", "id"),
        &snapshot(),
        Code::TypeMismatch,
    );
    code(
        &explicit_endpoint("source_id", "id, alternate"),
        &snapshot(),
        Code::InvalidKey,
    );
    code(
        &explicit_endpoint("source_id", "missing"),
        &snapshot(),
        Code::UnknownColumn,
    );
    code(
        &explicit_endpoint("source_id, source_id", "id, alternate"),
        &snapshot(),
        Code::InvalidKey,
    );
}

#[test]
fn foreign_key_inference_preserves_constraint_identity_and_validates_used_target() {
    let sql = "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS v NO PROPERTIES)
        EDGE TABLES (links SOURCE v DESTINATION v NO PROPERTIES)";
    let mut snapshot = snapshot();
    code(sql, &snapshot, Code::MissingForeignKey);
    let fk = PgqSourceForeignKeySchema {
        columns: vec!["source_id".into()],
        referenced_table: vec!["public".into(), "vertices".into()],
        referenced_columns: vec!["id".into()],
    };
    snapshot
        .0
        .get_mut(&vec!["public".into(), "links".into()])
        .unwrap()
        .foreign_keys
        .push(fk.clone());
    bind(sql, &snapshot).unwrap();
    snapshot
        .0
        .get_mut(&vec!["public".into(), "links".into()])
        .unwrap()
        .foreign_keys
        .push(fk);
    code(sql, &snapshot, Code::AmbiguousForeignKey);
    let keys = &mut snapshot
        .0
        .get_mut(&vec!["public".into(), "links".into()])
        .unwrap()
        .foreign_keys;
    keys.pop();
    keys[0].referenced_columns[0] = "score".into();
    code(sql, &snapshot, Code::InvalidSourceSchema);
    snapshot
        .0
        .get_mut(&vec!["public".into(), "links".into()])
        .unwrap()
        .foreign_keys[0]
        .referenced_columns[0] = "missing".into();
    code(sql, &snapshot, Code::InvalidSourceSchema);
    // Explicit endpoints need no traversal or validation of an unused target.
    bind(&explicit_endpoint("source_id", "id"), &snapshot).unwrap();
}

#[test]
fn source_metadata_is_coherent_and_each_declaration_is_resolved_once() {
    use std::cell::Cell;
    struct Counted {
        snapshot: Snapshot,
        lookups: Cell<usize>,
    }
    impl PgqSourceCatalog for Counted {
        fn source_table(&self, name: &[String]) -> Option<&PgqSourceTableSchema> {
            self.lookups.set(self.lookups.get() + 1);
            self.snapshot.source_table(name)
        }
    }
    let source = Counted {
        snapshot: snapshot(),
        lookups: Cell::new(0),
    };
    let sql = explicit_endpoint("source_id", "id");
    bind_postgres_create_property_graph(&sql, &create(&sql), &source).unwrap();
    assert_eq!(source.lookups.get(), 2);
    for mutation in 0..5 {
        let mut snapshot = snapshot();
        let source = snapshot
            .0
            .get_mut(&vec!["public".into(), "vertices".into()])
            .unwrap();
        match mutation {
            0 => source.columns[0].nullable = true,
            1 => source.columns.push(source.columns[0].clone()),
            2 => source.primary_key.push("missing".into()),
            3 => source.name.clear(),
            _ => source.foreign_keys.push(PgqSourceForeignKeySchema {
                columns: vec!["missing".into()],
                referenced_table: vec!["outside".into()],
                referenced_columns: vec!["id".into()],
            }),
        }
        code(
            "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices KEY (alternate))",
            &snapshot,
            Code::InvalidSourceSchema,
        );
    }
}

#[test]
fn aliases_are_unique_and_endpoint_references_only_vertices() {
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices, vertices)",
        &snapshot(),
        Code::DuplicateAlias,
    );
    bind(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS a, vertices AS b)",
        &snapshot(),
    )
    .unwrap();
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS v)
        EDGE TABLES (links AS v SOURCE v DESTINATION v)",
        &snapshot(),
        Code::DuplicateAlias,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices)
        EDGE TABLES (links SOURCE links DESTINATION vertices)",
        &snapshot(),
        Code::UnknownVertex,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (absent)",
        &snapshot(),
        Code::UnknownTable,
    );
}

#[test]
fn shared_labels_require_equal_property_sets_across_vertices_and_edges() {
    let prefix =
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices LABEL shared PROPERTIES (id, score))
        EDGE TABLES (links SOURCE KEY (source_id) REFERENCES vertices(id)
        DESTINATION KEY (destination_id) REFERENCES vertices(id)";
    bind(
        &format!("{prefix} LABEL shared PROPERTIES (score, id))"),
        &snapshot(),
    )
    .unwrap();
    code(
        &format!("{prefix} LABEL shared PROPERTIES (id))"),
        &snapshot(),
        Code::LabelMismatch,
    );
    code("CREATE PROPERTY GRAPH g VERTEX TABLES (vertices LABEL a NO PROPERTIES LABEL a NO PROPERTIES)", &snapshot(), Code::DuplicateLabel);
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices LABEL a PROPERTIES (id AS p)
        LABEL b PROPERTIES (title AS p))",
        &snapshot(),
        Code::PropertyExpressionMismatch,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS a LABEL a PROPERTIES (id AS p),
        vertices AS b LABEL b PROPERTIES (title AS p))",
        &snapshot(),
        Code::PropertyTypeMismatch,
    );
}

#[test]
fn repeated_property_expressions_ignore_spans_parentheses_and_resolved_qualification() {
    let sql = "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS v
        LABEL a PROPERTIES (id + 1 AS p, title)
        LABEL b PROPERTIES (((public.vertices.id) + 01) AS p, vertices.title))";
    bind(sql, &snapshot()).unwrap();
    code(
        &sql.replace("((public.vertices.id) + 01)", "(1 + id)"),
        &snapshot(),
        Code::PropertyExpressionMismatch,
    );
    code(
        &sql.replace("vertices.title", "v.title"),
        &snapshot(),
        Code::UnknownColumn,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES (id, id))",
        &snapshot(),
        Code::DuplicateProperty,
    );
    code(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES (id + 1))",
        &snapshot(),
        Code::MissingPropertyName,
    );
    bind(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices AS a LABEL a PROPERTIES (id AS p),
        vertices AS b LABEL b PROPERTIES (alternate AS p))",
        &snapshot(),
    )
    .unwrap();
}

#[test]
fn implicit_property_names_require_syntactic_columns_even_for_identity_casts() {
    let snapshot = snapshot();
    for (column, cast, data_type) in [
        ("id", "bigint", PgqDataType::Int64),
        ("score", "float8", PgqDataType::Float64),
        ("title", "text", PgqDataType::String),
        ("enabled", "boolean", PgqDataType::Boolean),
        ("payload", "bytea", PgqDataType::Binary),
        ("external_id", "uuid", PgqDataType::Uuid),
    ] {
        let bind_property = |expression: &str| {
            bind(
                &format!(
                    "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES ({expression}))"
                ),
                &snapshot,
            )
        };
        for expression in [column.to_owned(), format!("((public.vertices.{column}))")] {
            let graph = bind_property(&expression).unwrap();
            assert_eq!(
                graph.vertex_labels["vertices"].properties[column],
                data_type
            );
        }
        for expression in [
            format!("{column}::{cast}"),
            format!("((public.vertices.{column})::{cast})"),
            format!("{column}::{cast}::{cast}"),
        ] {
            let error = bind_property(&expression).unwrap_err();
            assert_eq!(
                error.code,
                Code::MissingPropertyName,
                "{expression}: {error}"
            );
            let graph = bind_property(&format!("{expression} AS value")).unwrap();
            assert_eq!(
                graph.vertex_labels["vertices"].properties["value"],
                data_type
            );
        }
    }
    bind(
        "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices
        LABEL a PROPERTIES (id AS p) LABEL b PROPERTIES (id::bigint AS p))",
        &snapshot,
    )
    .unwrap();
}

#[test]
fn quoted_identifiers_preserve_dots_case_and_utf8() {
    let mut source = table(
        "Mixed.Table",
        &[("id", SqlDataType::BigInt), ("a.b", SqlDataType::Text)],
    );
    source.name[0] = "Schema".into();
    let snapshot = Snapshot(BTreeMap::from([(source.name.clone(), source)]));
    let sql =
        "CREATE PROPERTY GRAPH \"Graph.Name\" VERTEX TABLES (\"Schema\".\"Mixed.Table\" AS \"点\"
        PROPERTIES (\"Schema\".\"Mixed.Table\".\"a.b\" AS \"Text.Value\"))";
    let graph = bind(sql, &snapshot).unwrap();
    assert_eq!(graph.name, ["Graph.Name"]);
    assert_eq!(
        graph.vertex_labels["点"].properties["Text.Value"],
        PgqDataType::String
    );
}

#[test]
fn expression_profile_resolves_constants_and_preserves_types() {
    let cases = [
        ("lower(title)", PgqDataType::String),
        ("pg_catalog.upper('a')", PgqDataType::String),
        ("abs(id)", PgqDataType::Int64),
        ("abs(score)", PgqDataType::Float64),
        ("id + '1'", PgqDataType::Int64),
        ("NULL + score", PgqDataType::Float64),
        ("NOT enabled", PgqDataType::Boolean),
        ("id IN (NULL, '2', score)", PgqDataType::Boolean),
        ("id BETWEEN '1' AND '2'", PgqDataType::Boolean),
        ("payload IS NULL", PgqDataType::Boolean),
        ("title || 'a'", PgqDataType::String),
        ("NULL", PgqDataType::String),
        ("'42'::bigint", PgqDataType::Int64),
        ("id::bytea", PgqDataType::Binary),
        ("payload::uuid", PgqDataType::Uuid),
        ("external_id::bytea", PgqDataType::Binary),
        ("'yes'::boolean", PgqDataType::Boolean),
        ("'\\x0102'::bytea", PgqDataType::Binary),
        (
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
            PgqDataType::Uuid,
        ),
        ("-9223372036854775808", PgqDataType::Int64),
    ];
    for (expression, expected) in cases {
        let sql = format!(
            "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES ({expression} AS p))"
        );
        let graph = bind(&sql, &snapshot()).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(
            graph.vertex_labels["vertices"].properties["p"], expected,
            "{expression}"
        );
    }
}

#[test]
fn unsupported_and_ill_typed_expressions_fail_explicitly() {
    for (expression, expected) in [
        ("unknown(id)", Code::UnsupportedExpression),
        ("count(id)", Code::UnsupportedExpression),
        ("lower(DISTINCT title)", Code::UnsupportedExpression),
        ("other.lower(title)", Code::UnsupportedExpression),
        ("$1", Code::UnsupportedExpression),
        ("*", Code::UnsupportedExpression),
        ("title COLLATE c", Code::UnsupportedExpression),
        ("id::integer", Code::UnsupportedExpression),
        ("id::unknown", Code::UnsupportedExpression),
        ("'x'::uuid", Code::InvalidLiteral),
        ("'invalid'::boolean", Code::InvalidLiteral),
        ("'o'::boolean", Code::InvalidLiteral),
        ("'\\x0'::bytea", Code::InvalidLiteral),
        ("'oops' + id", Code::InvalidLiteral),
        ("id::boolean", Code::TypeMismatch),
        ("score::bytea", Code::TypeMismatch),
        ("lower(id)", Code::TypeMismatch),
        ("abs()", Code::TypeMismatch),
        ("id AND enabled", Code::TypeMismatch),
        ("id + title", Code::TypeMismatch),
        ("score % score", Code::TypeMismatch),
        ("9223372036854775808", Code::InvalidLiteral),
        ("1e999", Code::InvalidLiteral),
    ] {
        code(
            &format!(
                "CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES ({expression} AS p))"
            ),
            &snapshot(),
            expected,
        );
    }
}

#[test]
fn malformed_ast_spans_and_deep_expressions_fail_without_panicking() {
    let sql = "CREATE PROPERTY GRAPH \"点\" VERTEX TABLES (vertices PROPERTIES (id AS p))";
    for span in [
        Span::new(usize::MAX, usize::MAX),
        Span::new(5, 1),
        Span::new(23, 24),
    ] {
        let mut ast = create(sql);
        ast.name.parts[0].span = span;
        let error = bind_postgres_create_property_graph(sql, &ast, &snapshot()).unwrap_err();
        assert_eq!(error.code, Code::InvalidSyntaxTree);
        assert!(sql.get(error.span.start..error.span.end).is_some());
    }
    let mut ast = create(sql);
    let PropertyExposure::Expressions(properties) =
        &mut ast.vertex_tables[0].exposure.as_mut().unwrap().labels[0].properties
    else {
        panic!()
    };
    for _ in 0..256 {
        let inner = properties[0].expression.clone();
        properties[0].expression.kind = ExpressionKindSyntax::Parenthesized(Box::new(inner));
    }
    let error = bind_postgres_create_property_graph(sql, &ast, &snapshot()).unwrap_err();
    assert_eq!(error.code, Code::UnsupportedExpression);
    let mut ast = create(sql);
    ast.span = Span::new(usize::MAX, 0);
    let error = bind_postgres_create_property_graph(sql, &ast, &snapshot()).unwrap_err();
    assert_eq!(error.span, Span::default());
}

mod corpus;

#[test]
#[ignore = "bounded local creation binder differential campaign"]
fn create_binding_differential_campaign() {
    // The oracle is a six-by-six compatibility matrix, independent of the binder.
    // Metadata combinations exercise explicit and inferred endpoints separately.
    use SqlDataType::*;
    let types = [Boolean, BigInt, DoublePrecision, Text, Bytea, Uuid];
    let explicit = [
        [true, false, false, false, false, false],
        [false, true, true, false, false, false],
        [false, false, true, false, false, false],
        [false, false, false, true, false, false],
        [false, false, false, false, true, false],
        [false, false, false, false, false, true],
    ];
    let mut checked = 0;
    for seed in 0..64 {
        for (edge_type, &edge_scalar) in types.iter().enumerate() {
            for (vertex_type, &vertex_scalar) in types.iter().enumerate() {
                let mut snapshot = snapshot();
                let vertices = snapshot
                    .0
                    .get_mut(&vec!["public".into(), "vertices".into()])
                    .unwrap();
                vertices.columns[1].data_type = vertex_scalar;
                vertices.columns[1].nullable = seed & 1 != 0;
                let links = snapshot
                    .0
                    .get_mut(&vec!["public".into(), "links".into()])
                    .unwrap();
                links.columns[1].data_type = edge_scalar;
                let target = if seed & 2 == 0 {
                    "vertices"
                } else {
                    "public.vertices"
                };
                let alias = format!("\"node.{seed}\"");
                let sql = format!("CREATE PROPERTY GRAPH g VERTEX TABLES ({target} AS {alias} KEY (alternate) NO PROPERTIES)
                    EDGE TABLES (links SOURCE KEY (source_id) REFERENCES {alias}(alternate)
                    DESTINATION KEY (source_id) REFERENCES {alias}(alternate) NO PROPERTIES)");
                assert_eq!(
                    bind(&sql, &snapshot).is_ok(),
                    explicit[edge_type][vertex_type],
                    "{sql}: {edge_scalar:?} -> {vertex_scalar:?}"
                );
                checked += 1;
                let links = snapshot
                    .0
                    .get_mut(&vec!["public".into(), "links".into()])
                    .unwrap();
                for _ in 0..seed % 3 {
                    links.foreign_keys.push(PgqSourceForeignKeySchema {
                        columns: vec!["source_id".into()],
                        referenced_table: vec!["public".into(), "vertices".into()],
                        referenced_columns: vec!["alternate".into()],
                    });
                }
                let inferred = sql.replace(
                    &format!("KEY (source_id) REFERENCES {alias}(alternate)"),
                    &alias,
                );
                let outcome = bind(&inferred, &snapshot);
                let expected = match seed % 3 {
                    0 => Some(Code::MissingForeignKey),
                    1 if edge_type == vertex_type => None,
                    1 => Some(Code::InvalidSourceSchema),
                    _ => Some(Code::AmbiguousForeignKey),
                };
                assert_eq!(
                    outcome.err().map(|error| error.code),
                    expected,
                    "seed {seed}, {edge_type}, {vertex_type}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 4608);
}

#[test]
fn typed_string_public_ast_and_resolved_literal_identity_use_the_six_type_profile() {
    for (value, target) in [
        ("yes", "boolean"),
        ("42", "bigint"),
        ("1.25", "float8"),
        ("hello", "text"),
        ("\\x01", "bytea"),
        ("a0eebc999c0b4ef8bb6d6bb9bd380a11", "uuid"),
    ] {
        let sql = format!("CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES ('{value}'::{target} AS p))");
        let mut ast = create(&sql);
        let expected = bind_postgres_create_property_graph(&sql, &ast, &snapshot()).unwrap();
        let PropertyExposure::Expressions(properties) =
            &mut ast.vertex_tables[0].exposure.as_mut().unwrap().labels[0].properties
        else {
            panic!()
        };
        let ExpressionKindSyntax::Cast {
            expression,
            data_type,
        } = &properties[0].expression.kind
        else {
            panic!()
        };
        let ExpressionKindSyntax::Literal(hawdb_sql_syntax::LiteralSyntax::String(value)) =
            expression.kind
        else {
            panic!()
        };
        properties[0].expression.kind = ExpressionKindSyntax::TypedString {
            data_type: data_type.parts[0].clone(),
            value,
        };
        assert_eq!(
            bind_postgres_create_property_graph(&sql, &ast, &snapshot()).unwrap(),
            expected
        );
    }
    for (left, right) in [
        ("id + 1", "id + '01'"),
        ("TRUE", "'yes'::boolean"),
        ("'\\x01'::bytea", "'\\001'::bytea"),
        (
            "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
            "'{A0EEBC999C0B4EF8BB6D6BB9BD380A11}'::uuid",
        ),
    ] {
        let sql = format!("CREATE PROPERTY GRAPH g VERTEX TABLES (vertices LABEL a PROPERTIES ({left} AS p) LABEL b PROPERTIES ({right} AS p))");
        bind(&sql, &snapshot()).unwrap();
    }
}

#[test]
fn cast_matrix_does_not_invent_transitive_conversions() {
    let columns = ["enabled", "id", "score", "title", "payload", "external_id"];
    let names = ["boolean", "bigint", "float8", "text", "bytea", "uuid"];
    let allowed = [
        [true, false, false, true, false, false],
        [false, true, true, true, true, false],
        [false, true, true, true, false, false],
        [true, true, true, true, true, true],
        [false, true, false, true, true, true],
        [false, false, false, true, true, true],
    ];
    for (source, column) in columns.iter().enumerate() {
        for (target, name) in names.iter().enumerate() {
            let sql = format!("CREATE PROPERTY GRAPH g VERTEX TABLES (vertices PROPERTIES ({column}::{name} AS p))");
            assert_eq!(
                bind(&sql, &snapshot()).is_ok(),
                allowed[source][target],
                "{column}::{name}"
            );
        }
    }
}
