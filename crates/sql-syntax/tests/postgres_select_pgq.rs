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

//! Full SELECT envelopes adapted from PostgreSQL's graph_table regression suite.
//!
//! Reference revision: PostgreSQL 3d00537feb565c410baf41bb301eee338e4b2317.
//! Reference file: src/test/regress/sql/graph_table.sql.

// Locally authored alias regressions supplement the adapted PostgreSQL corpus.
#[path = "support/alias_boundaries.rs"]
mod alias_boundaries;

use hawdb_sql_syntax::{
    parse_postgres_select, parse_postgres_statement, BinaryOperatorSyntax, ExpressionKindSyntax,
    PostgresFromItemSyntax, PostgresJoinKind, PostgresStatementSyntax, SyntaxErrorCode,
    UnaryOperatorSyntax,
};

#[test]
fn parses_graph_table_as_a_postgres_from_item() {
    let sql = "SELECT customer_name
        FROM GRAPH_TABLE (
            knowledge
            MATCH (document IS documents WHERE document.id = $1)-[IS mentions]->(entity)
            COLUMNS (entity.name AS customer_name)
        )";
    let select = parse_postgres_select(sql).expect("PostgreSQL SELECT with GRAPH_TABLE");
    assert_eq!(select.projection.len(), 1);
    assert_eq!(select.from.len(), 1);
    let PostgresFromItemSyntax::GraphTable(graph_table) = &select.from[0].relation else {
        panic!("GRAPH_TABLE from item");
    };
    assert_eq!(graph_table.pattern.paths[0].factors.len(), 3);
    assert_eq!(graph_table.columns.len(), 1);
}

#[test]
fn parses_relational_and_graph_from_items_with_outer_clauses() {
    let sql = "SELECT source.id, matched.entity_name
        FROM source,
             GRAPH_TABLE (
                 knowledge
                 MATCH (document IS documents WHERE document.id = source.id)-[IS mentions]->(entity)
                 COLUMNS (entity.name AS entity_name)
             ) AS matched
        WHERE matched.entity_name IS NOT NULL
        ORDER BY source.id, matched.entity_name
        LIMIT $1 OFFSET 0 FOR SHARE;";
    let select = parse_postgres_select(sql).expect("mixed PostgreSQL FROM items");
    assert_eq!(select.from.len(), 2);
    assert!(matches!(
        select.from[0].relation,
        PostgresFromItemSyntax::Relation(_)
    ));
    assert!(matches!(
        select.from[1].relation,
        PostgresFromItemSyntax::GraphTable(_)
    ));
    assert!(select.selection.is_some());
    assert_eq!(select.order_by.len(), 2);
    assert!(select.limit.is_some());
    assert!(select.offset.is_some());
    let locking = select.locking.expect("locking clause");
    assert_eq!(&sql[locking.span.start..locking.span.end], "FOR SHARE");
}

#[test]
fn preserves_grouping_and_having_as_bounded_expression_spans() {
    let sql = "SELECT DISTINCT matched.kind, count(*) AS total
        FROM GRAPH_TABLE (
            knowledge MATCH (node IS entity)
            COLUMNS (node.kind AS kind)
        ) matched
        GROUP BY matched.kind
        HAVING count(*) > 1
        ORDER BY total DESC";
    let select = parse_postgres_select(sql).expect("grouped graph SELECT");
    assert!(select.distinct);
    assert_eq!(select.group_by.len(), 1);
    assert!(select.having.is_some());
    assert_eq!(select.order_by.len(), 1);
}

#[test]
fn routes_owned_postgres_statement_families_without_fallback() {
    let create = parse_postgres_statement("CREATE PROPERTY GRAPH knowledge")
        .expect("CREATE PROPERTY GRAPH syntax");
    assert!(matches!(
        create,
        PostgresStatementSyntax::CreatePropertyGraph(_)
    ));

    let select = parse_postgres_statement(
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH (node) COLUMNS (node.id))",
    )
    .expect("SELECT syntax");
    assert!(matches!(select, PostgresStatementSyntax::Select(_)));
}

#[test]
fn keeps_postgres_graph_transform_errors_out_of_select_parsing() {
    for sql in [
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH ()() COLUMNS (1 AS one))",
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH -> COLUMNS (1 AS one))",
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH ()-[]- COLUMNS (1 AS one))",
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH (left_node), (right_node) COLUMNS (1 AS one))",
    ] {
        parse_postgres_select(sql).expect("raw SELECT syntax accepted before binding");
    }
}

#[test]
fn rejects_select_shapes_outside_the_owned_slice() {
    for sql in [
        "SELECT FROM source",
        "SELECT * FROM",
        "SELECT * FROM source RIGHT JOIN target ON source.id = target.id",
        "SELECT * FROM source FULL JOIN target ON source.id = target.id",
        "SELECT * FROM source JOIN target USING (id)",
        "SELECT * FROM source FETCH FIRST 1 ROW ONLY",
        "SELECT * FROM source LIMIT 1 WHERE source.id = 1",
        "SELECT * FROM source WHERE source.id = 1 WHERE source.id = 2",
        "SELECT * FROM source ORDER BY source.id GROUP BY source.id",
        "SELECT * FROM source WHERE (source.id = 1",
        "SELECT * FROM source WHERE source.id = 1)",
        "SELECT * FROM source WHERE ([source.id)]",
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH (node) COLUMNS (node.id)) WHERE",
        "SELECT * FROM GRAPH_TABLE (knowledge MATCH (node) COLUMNS (node.id)) AS WHERE",
    ] {
        let error = parse_postgres_select(sql).expect_err("unsupported SELECT shape must fail");
        assert!(matches!(
            error.code,
            SyntaxErrorCode::UnexpectedToken | SyntaxErrorCode::UnexpectedEnd
        ));
    }
}

#[test]
fn bounds_expression_nesting_without_heap_growth() {
    let sql = format!(
        "SELECT * FROM source WHERE {}1{}",
        "(".repeat(129),
        ")".repeat(129)
    );
    let error = parse_postgres_select(&sql).expect_err("expression nesting must be bounded");
    assert_eq!(error.code, SyntaxErrorCode::ExpressionNestingLimitExceeded);
}

#[test]
fn builds_pratt_expression_trees_with_postgres_precedence() {
    let sql = "SELECT source.score + 1 * 2 AS rank
        FROM source
        WHERE NOT source.deleted
          AND source.score + 1 * 2 >= $1
          AND source.kind IN ('memory', 'entity')";
    let select = parse_postgres_select(sql).expect("owned PostgreSQL expressions");
    let ExpressionKindSyntax::Binary {
        operator: BinaryOperatorSyntax::And,
        ..
    } = &select.selection.expect("WHERE predicate").kind
    else {
        panic!("AND must be the root predicate operator");
    };
    let ExpressionKindSyntax::Binary {
        operator: BinaryOperatorSyntax::Add,
        right,
        ..
    } = &select.projection[0].expression.kind
    else {
        panic!("addition must be the projection root");
    };
    assert!(matches!(
        right.kind,
        ExpressionKindSyntax::Binary {
            operator: BinaryOperatorSyntax::Multiply,
            ..
        }
    ));

    let comparison = parse_postgres_select("SELECT * FROM source WHERE NOT source.score = 1")
        .expect("NOT comparison expression");
    let ExpressionKindSyntax::Unary {
        operator: UnaryOperatorSyntax::Not,
        expression,
    } = &comparison.selection.expect("WHERE predicate").kind
    else {
        panic!("NOT must be the comparison root");
    };
    assert!(matches!(
        expression.kind,
        ExpressionKindSyntax::Binary {
            operator: BinaryOperatorSyntax::Equal,
            ..
        }
    ));

    let in_list = parse_postgres_select("SELECT * FROM source WHERE NOT source.score IN (1, 2)")
        .expect("NOT IN-list expression");
    let ExpressionKindSyntax::Unary {
        operator: UnaryOperatorSyntax::Not,
        expression,
    } = &in_list.selection.expect("WHERE predicate").kind
    else {
        panic!("NOT must be the IN-list root");
    };
    assert!(matches!(
        expression.kind,
        ExpressionKindSyntax::InList { .. }
    ));

    let conjunction =
        parse_postgres_select("SELECT * FROM source WHERE NOT source.deleted AND source.visible")
            .expect("NOT conjunction expression");
    let ExpressionKindSyntax::Binary {
        left,
        operator: BinaryOperatorSyntax::And,
        ..
    } = &conjunction.selection.expect("WHERE predicate").kind
    else {
        panic!("AND must be the conjunction root");
    };
    assert!(matches!(
        left.kind,
        ExpressionKindSyntax::Unary {
            operator: UnaryOperatorSyntax::Not,
            ..
        }
    ));
}

#[test]
fn parses_supported_joins_around_graph_table_sources() {
    let sql = "SELECT source.id, matched.name, target.rank
        FROM source
        LEFT JOIN GRAPH_TABLE (
            knowledge MATCH (node IS entity)
            COLUMNS (node.id AS id, node.name AS name)
        ) matched ON matched.id = source.id
        INNER JOIN target ON target.id = matched.id
        CROSS JOIN tenant
        WHERE target.rank BETWEEN 1 AND 10";
    let select = parse_postgres_select(sql).expect("supported PostgreSQL joins");
    assert_eq!(select.from.len(), 1);
    let joins = &select.from[0].joins;
    assert_eq!(joins.len(), 3);
    assert_eq!(joins[0].kind, PostgresJoinKind::Left);
    assert!(matches!(
        joins[0].relation,
        PostgresFromItemSyntax::GraphTable(_)
    ));
    assert!(joins[0].condition.is_some());
    assert_eq!(joins[1].kind, PostgresJoinKind::Inner);
    assert!(joins[1].condition.is_some());
    assert_eq!(joins[2].kind, PostgresJoinKind::Cross);
    assert!(joins[2].condition.is_none());
}
