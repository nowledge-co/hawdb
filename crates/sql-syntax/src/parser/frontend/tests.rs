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

use crate::{
    parse_graph_table, parse_postgres_select, parse_postgres_statement, PostgresFromItemSyntax,
    Span, SyntaxErrorCode,
};

#[test]
fn graph_table_prefix_cannot_backtrack_into_a_relation() {
    for sql in [
        "SELECT * FROM GRAPH_TABLE",
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS ())",
        "SELECT * FROM GRAPH_TABLE (g MATCH (n) COLUMNS (n.id)",
        "SELECT * FROM source JOIN GRAPH_TABLE ON source.id = 1",
    ] {
        assert!(parse_postgres_select(sql).is_err(), "{sql}");
        assert!(parse_postgres_statement(sql).is_err(), "{sql}");
    }
    for name in ["\"GRAPH_TABLE\"", "graph_table_archive"] {
        let sql = format!("SELECT * FROM {name}");
        let select = parse_postgres_select(&sql).unwrap();
        assert!(matches!(
            select.from[0].relation,
            PostgresFromItemSyntax::Relation(_)
        ));
    }
}

#[test]
fn composed_sql_pgq_keeps_utf8_spans_comments_and_independent_parses() {
    let sql = "SELECT g.id FROM /* nested /* comment */ */ GRAPH_TABLE (\"\u{77e5}\u{8b58}\" MATCH (n) COLUMNS (n.id)) AS g";
    let select = parse_postgres_select(sql).unwrap();
    let PostgresFromItemSyntax::GraphTable(table) = &select.from[0].relation else {
        panic!("PGQ alternative")
    };
    assert_eq!(
        &sql[table.graph.span.start..table.graph.span.end],
        "\"\u{77e5}\u{8b58}\""
    );
    assert_eq!(
        &sql[table.span.start..table.span.end],
        "GRAPH_TABLE (\"\u{77e5}\u{8b58}\" MATCH (n) COLUMNS (n.id)) AS g"
    );
    let bad = format!("{sql} WHERE");
    let error = parse_postgres_select(&bad).unwrap_err();
    assert_eq!(error.code, SyntaxErrorCode::UnexpectedEnd);
    assert_eq!(error.span, Span::new(bad.len(), bad.len()));
    assert_eq!(parse_postgres_select(sql).unwrap(), select);
    assert!(parse_graph_table("GRAPH_TABLE (g MATCH (n) COLUMNS (n.id)); SELECT 1").is_err());
}

#[test]
fn graph_and_expression_nesting_have_independent_exact_limits() {
    for (graph_depth, expression_depth, expected) in [
        (128, 128, None),
        (
            129,
            0,
            Some(SyntaxErrorCode::GraphPatternNestingLimitExceeded),
        ),
        (
            0,
            129,
            Some(SyntaxErrorCode::ExpressionNestingLimitExceeded),
        ),
    ] {
        let predicate = format!(
            "{}n.id{}",
            "(".repeat(expression_depth),
            ")".repeat(expression_depth)
        );
        let sql = format!(
            "GRAPH_TABLE (g MATCH {}(n WHERE {predicate}){} COLUMNS (n.id))",
            "(".repeat(graph_depth),
            ")".repeat(graph_depth)
        );
        let result = parse_graph_table(&sql);
        match expected {
            Some(code) => assert_eq!(result.unwrap_err().code, code),
            None => {
                result.expect("independent nesting limits");
            }
        }
    }
}

#[test]
fn aggregate_distinct_does_not_inherit_select_distinct_on_guard() {
    let sql = "SELECT count(DISTINCT on) FROM items";
    let select = parse_postgres_select(sql).unwrap();
    let crate::ExpressionKindSyntax::Function {
        arguments,
        distinct,
        ..
    } = &select.projection[0].expression.kind
    else {
        panic!("function expression");
    };
    assert!(*distinct);
    let crate::ExpressionKindSyntax::Column(name) = &arguments[0].kind else {
        panic!("column argument");
    };
    assert_eq!(&sql[name.span.start..name.span.end], "on");
    assert!(parse_postgres_select("SELECT DISTINCT ON (id) id FROM items").is_err());
}
