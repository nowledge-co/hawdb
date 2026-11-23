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
    parse_graph_table, parse_postgres_select, parse_postgres_statement, parse_relational_statement,
    ExpressionKindSyntax, PostgresFromItemSyntax, PostgresQueryBodySyntax, PostgresSetOperator,
    RelationalStatementSyntax, Span, SyntaxErrorCode,
};

#[test]
fn values_derived_tables_and_subquery_predicates_keep_query_spans() {
    let sql = "SELECT input.id FROM (VALUES ($1, 'a'), ($2, NULL)) AS input(id, label) \
               WHERE EXISTS (SELECT 1 FROM owners o WHERE o.id = input.id) \
               AND (input.id, input.label) NOT IN \
                   (SELECT owner_id, label FROM blocked)";
    let RelationalStatementSyntax::Select(select) = parse_relational_statement(sql).unwrap() else {
        panic!("query syntax")
    };
    let PostgresFromItemSyntax::Derived(derived) = &select.from[0].relation else {
        panic!("derived table")
    };
    assert_eq!(derived.alias.columns.len(), 2);
    assert!(matches!(
        derived.query.body,
        PostgresQueryBodySyntax::Values { ref rows, .. } if rows.len() == 2 && rows[0].len() == 2
    ));
    let ExpressionKindSyntax::Binary { left, right, .. } =
        &select.selection.as_ref().expect("WHERE").kind
    else {
        panic!("conjunction")
    };
    let ExpressionKindSyntax::Exists { query } = &left.kind else {
        panic!("EXISTS")
    };
    assert!(sql[query.span.start..query.span.end].starts_with("SELECT 1"));
    let ExpressionKindSyntax::InSubquery { query, negated, .. } = &right.kind else {
        panic!("row NOT IN")
    };
    assert!(*negated);
    assert!(sql[query.span.start..query.span.end].starts_with("SELECT owner_id"));
}

#[test]
fn relational_subqueries_remain_confined_to_where_predicates() {
    for sql in [
        "SELECT EXISTS (SELECT 1 FROM owners) FROM owners",
        "SELECT owner FROM owners HAVING EXISTS (SELECT 1 FROM owners)",
        "SELECT owner FROM owners ORDER BY EXISTS (SELECT 1 FROM owners)",
        "SELECT owner FROM owners JOIN blocked ON EXISTS (SELECT 1 FROM blocked)",
        "VALUES (EXISTS (SELECT 1 FROM owners))",
    ] {
        let error = parse_relational_statement(sql).unwrap_err();
        assert_eq!(error.code, SyntaxErrorCode::UnexpectedToken, "{sql}");
    }
}

#[test]
fn relational_ctes_and_set_operations_preserve_precedence_and_branch_tails() {
    let sql = "WITH first(x) AS (SELECT 1), second AS (SELECT 2 UNION SELECT 3) \
               SELECT x FROM first INTERSECT SELECT 2 UNION ALL \
               (SELECT 4 ORDER BY 1 DESC LIMIT 1) EXCEPT SELECT 5 \
               ORDER BY x LIMIT 3 OFFSET 1";
    let RelationalStatementSyntax::Query(query) = parse_relational_statement(sql).unwrap() else {
        panic!("compound query syntax")
    };
    assert_eq!(query.with.len(), 2);
    assert_eq!(
        &sql[query.with[0].name.span.start..query.with[0].name.span.end],
        "first"
    );
    assert_eq!(query.with[0].columns.len(), 1);
    assert_eq!(query.order_by.len(), 1);
    assert!(query.limit.is_some());
    assert!(query.offset.is_some());

    let PostgresQueryBodySyntax::SetOperation {
        left,
        operator: PostgresSetOperator::Except,
        all: false,
        ..
    } = &query.body
    else {
        panic!("outer EXCEPT")
    };
    let PostgresQueryBodySyntax::SetOperation {
        left: union_left,
        operator: PostgresSetOperator::Union,
        all: true,
        right,
        ..
    } = left.as_ref()
    else {
        panic!("left-associative UNION ALL")
    };
    assert!(matches!(right.as_ref(), PostgresQueryBodySyntax::Nested(_)));
    assert!(matches!(
        union_left.as_ref(),
        PostgresQueryBodySyntax::SetOperation {
            operator: PostgresSetOperator::Intersect,
            ..
        }
    ));
}

#[test]
fn recursive_cte_keyword_and_union_shape_are_preserved() {
    let RelationalStatementSyntax::Query(query) = parse_relational_statement(
        "WITH RECURSIVE walk(id) AS (VALUES (1) UNION ALL SELECT id + 1 FROM walk WHERE id < $1) SELECT id FROM walk",
    )
    .unwrap()
    else {
        panic!("recursive query syntax")
    };
    assert!(query.recursive);
    assert_eq!(query.with.len(), 1);
    assert!(matches!(
        query.with[0].query.body,
        PostgresQueryBodySyntax::SetOperation {
            operator: PostgresSetOperator::Union,
            all: true,
            ..
        }
    ));
}

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
fn explicit_and_shorthand_rows_share_the_expression_nesting_limit() {
    let limit = crate::parser::MAX_EXPRESSION_NESTING;
    let explicit =
        |depth: usize| (0..depth).fold("1".to_string(), |nested, _| format!("ROW(1,{nested})"));
    let shorthand =
        |depth: usize| (0..depth).fold("1".to_string(), |nested, _| format!("(1,{nested})"));
    for (label, accepted_expression, rejected_expression) in [
        ("explicit", explicit(limit), explicit(limit + 1)),
        ("shorthand", shorthand(limit), shorthand(limit + 1)),
    ] {
        let accepted = format!("SELECT 1 FROM source WHERE {accepted_expression}");
        parse_postgres_select(&accepted)
            .unwrap_or_else(|error| panic!("{label} row at the nesting limit: {error}"));

        let rejected = format!("SELECT 1 FROM source WHERE {rejected_expression}");
        assert_eq!(
            parse_postgres_select(&rejected).unwrap_err().code,
            SyntaxErrorCode::ExpressionNestingLimitExceeded,
            "{label} row beyond the nesting limit"
        );
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
