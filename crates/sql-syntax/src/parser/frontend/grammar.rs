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

//! Declarative token rules. Ordered alternatives are part of the dialect.

use super::native::{self, Value};
use crate::peg::{Expression, Grammar, NativeMatcher};
use crate::TokenKind;

use Expression::{
    Ahead, Choice, Keyword as K, Not, Optional, Repeat, Rule as R, Sequence as S, Token,
};

type Rule = (&'static str, Expression<Value>);

fn optional(expression: Expression<Value>) -> Expression<Value> {
    Optional(Box::new(expression))
}
fn repeat(expression: Expression<Value>) -> Expression<Value> {
    Repeat(Box::new(expression))
}
fn native(matcher: NativeMatcher<Value>, nullable: bool) -> Expression<Value> {
    Expression::Native { matcher, nullable }
}
fn list(name: &'static str) -> Expression<Value> {
    S(vec![
        R(name),
        repeat(S(vec![Token(TokenKind::Comma, ","), R(name)])),
    ])
}
fn complete(name: &'static str, expected: &'static str) -> Expression<Value> {
    S(vec![
        R(name),
        optional(Token(TokenKind::Semicolon, ";")),
        Token(TokenKind::End, expected),
    ])
}

pub(super) fn build() -> Result<Grammar<Value>, String> {
    let mut rules = vec![
        ("SqlInput", complete("Statement", "end of statement")),
        ("SelectInput", complete("Select", "end of SELECT")),
        (
            "RelationalSelect",
            S(vec![
                K("SELECT"),
                optional(R("Quantifier")),
                R("RelationalProjection"),
                optional(S(vec![K("FROM"), R("RelationalFrom")])),
                optional(R("Where")),
                optional(R("Group")),
                optional(R("Having")),
                optional(R("Order")),
                optional(Choice(vec![
                    S(vec![R("RelationalLimit"), optional(R("RelationalOffset"))]),
                    S(vec![R("RelationalOffset"), optional(R("RelationalLimit"))]),
                ])),
                optional(R("RelationalLocking")),
            ]),
        ),
        (
            "RelationalProjection",
            native(native::relational_projection, true),
        ),
        (
            "RelationalStatementInput",
            complete("RelationalStatement", "end of statement"),
        ),
        (
            "RelationalStatement",
            Choice(vec![R("RelationalSelect"), R("RelationalExplain")]),
        ),
        (
            "RelationalExplain",
            S(vec![
                K("EXPLAIN"),
                optional(R("Analyze")),
                R("RelationalSelect"),
            ]),
        ),
        ("Analyze", K("ANALYZE")),
        ("Statement", Choice(vec![R("Select")])),
        (
            "Select",
            S(vec![
                K("SELECT"),
                optional(R("Quantifier")),
                R("Projection"),
                K("FROM"),
                R("From"),
                optional(R("Where")),
                optional(R("Group")),
                optional(R("Having")),
                optional(R("Order")),
                optional(R("Limit")),
                optional(R("Offset")),
                optional(R("Locking")),
            ]),
        ),
        ("Quantifier", Choice(vec![R("Distinct"), K("ALL")])),
        (
            "Distinct",
            S(vec![
                K("DISTINCT"),
                Not(Box::new(K("ON")), "a projection after DISTINCT"),
            ]),
        ),
        ("Projection", native(native::projection, false)),
        (
            "From",
            S(vec![
                list("FromSource"),
                Ahead(
                    Box::new(Choice(vec![
                        K("WHERE"),
                        K("GROUP"),
                        K("HAVING"),
                        K("ORDER"),
                        K("LIMIT"),
                        K("OFFSET"),
                        K("FOR"),
                        K("FETCH"),
                        Token(TokenKind::Semicolon, ";"),
                        Token(TokenKind::End, "end"),
                    ])),
                    "a join, comma, or supported SELECT clause after FROM item",
                ),
            ]),
        ),
        ("FromSource", S(vec![R("FromItem"), repeat(R("Join"))])),
        ("FromItem", Choice(vec![R("Relation")])),
        (
            "Relation",
            S(vec![
                Not(Box::new(K("GRAPH_TABLE")), "a relation name"),
                native(native::relation, false),
            ]),
        ),
        (
            "Join",
            Choice(vec![R("InnerJoin"), R("LeftJoin"), R("CrossJoin")]),
        ),
        (
            "InnerJoin",
            S(vec![
                optional(K("INNER")),
                K("JOIN"),
                R("FromItem"),
                K("ON"),
                R("JoinExpression"),
            ]),
        ),
        (
            "LeftJoin",
            S(vec![
                K("LEFT"),
                optional(K("OUTER")),
                K("JOIN"),
                R("FromItem"),
                K("ON"),
                R("JoinExpression"),
            ]),
        ),
        ("CrossJoin", S(vec![K("CROSS"), K("JOIN"), R("FromItem")])),
        ("JoinExpression", native(native::join_expression, false)),
        ("Where", S(vec![K("WHERE"), R("ClauseExpression")])),
        ("Group", S(vec![K("GROUP"), K("BY"), R("GroupExpressions")])),
        ("Having", S(vec![K("HAVING"), R("ClauseExpression")])),
        ("Order", S(vec![K("ORDER"), K("BY"), R("OrderItems")])),
        (
            "RelationalLimit",
            S(vec![
                K("LIMIT"),
                Choice(vec![R("LimitAll"), R("ClauseExpression")]),
            ]),
        ),
        ("LimitAll", K("ALL")),
        (
            "RelationalOffset",
            S(vec![
                K("OFFSET"),
                R("OffsetExpression"),
                optional(Choice(vec![K("ROW"), K("ROWS")])),
            ]),
        ),
        ("OffsetExpression", native(native::offset_expression, false)),
        ("Limit", S(vec![K("LIMIT"), R("ClauseExpression")])),
        ("Offset", S(vec![K("OFFSET"), R("ClauseExpression")])),
        (
            "Locking",
            S(vec![K("FOR"), Choice(vec![R("Share"), R("Update")])]),
        ),
        (
            "RelationalLocking",
            S(vec![
                K("FOR"),
                Choice(vec![R("Share"), R("Update")]),
                optional(R("Nonblocking")),
            ]),
        ),
        (
            "Nonblocking",
            Choice(vec![K("NOWAIT"), S(vec![K("SKIP"), K("LOCKED")])]),
        ),
        ("Share", K("SHARE")),
        ("Update", K("UPDATE")),
        ("ClauseExpression", native(native::clause_expression, false)),
        ("GroupExpressions", native(native::group_expressions, false)),
        ("OrderItems", native(native::order_items, false)),
        ("Name", native(native::name, false)),
        ("Alias", native(native::alias, true)),
    ];
    rules.extend([
        ("RelationalFrom", list("RelationalFromSource")),
        (
            "RelationalFromSource",
            S(vec![R("RelationalFromItem"), repeat(R("RelationalJoin"))]),
        ),
        ("RelationalFromItem", R("RelationalRelation")),
        (
            "RelationalRelation",
            S(vec![
                Not(Box::new(K("GRAPH_TABLE")), "a relation name"),
                native(native::relational_relation, false),
            ]),
        ),
        (
            "RelationalJoin",
            Choice(vec![
                R("RelationalInnerJoin"),
                R("RelationalLeftJoin"),
                R("RelationalCrossJoin"),
            ]),
        ),
        (
            "RelationalInnerJoin",
            S(vec![
                optional(K("INNER")),
                K("JOIN"),
                R("RelationalFromItem"),
                K("ON"),
                R("JoinExpression"),
            ]),
        ),
        (
            "RelationalLeftJoin",
            S(vec![
                K("LEFT"),
                optional(K("OUTER")),
                K("JOIN"),
                R("RelationalFromItem"),
                K("ON"),
                R("JoinExpression"),
            ]),
        ),
        (
            "RelationalCrossJoin",
            S(vec![K("CROSS"), K("JOIN"), R("RelationalFromItem")]),
        ),
    ]);
    add_pgq(&mut rules)?;
    Grammar::compile(rules)
}

fn prepend_choice(rules: &mut [Rule], rule: &str, alternative: &'static str) -> Result<(), String> {
    let Some((_, Choice(choices))) = rules.iter_mut().find(|(name, _)| *name == rule) else {
        return Err(format!("PEG extension requires a choice rule: {rule}"));
    };
    choices.insert(0, R(alternative));
    Ok(())
}

fn add_pgq(rules: &mut Vec<Rule>) -> Result<(), String> {
    rules.extend([
        (
            "PgqInput",
            complete("CreatePropertyGraph", "end of statement"),
        ),
        (
            "GraphTableInput",
            complete("GraphTable", "end of GRAPH_TABLE clause"),
        ),
        (
            "GraphTable",
            S(vec![
                K("GRAPH_TABLE"),
                Token(TokenKind::LeftParen, "("),
                R("Name"),
                K("MATCH"),
                R("Pattern"),
                K("COLUMNS"),
                Token(TokenKind::LeftParen, "("),
                R("Columns"),
                Token(TokenKind::RightParen, ")"),
                Token(TokenKind::RightParen, ")"),
                R("Alias"),
            ]),
        ),
        ("Pattern", native(native::pattern, false)),
        ("Columns", native(native::columns, false)),
        (
            "CreatePropertyGraph",
            S(vec![
                K("CREATE"),
                optional(R("Temporary")),
                K("PROPERTY"),
                K("GRAPH"),
                R("Name"),
                optional(R("Vertices")),
                optional(R("Edges")),
            ]),
        ),
        ("Temporary", Choice(vec![K("TEMP"), K("TEMPORARY")])),
        (
            "Vertices",
            S(vec![
                Choice(vec![K("VERTEX"), K("NODE")]),
                K("TABLES"),
                Token(TokenKind::LeftParen, "("),
                R("VertexDefinitions"),
                Token(TokenKind::RightParen, ")"),
            ]),
        ),
        (
            "Edges",
            S(vec![
                Choice(vec![K("EDGE"), K("RELATIONSHIP")]),
                K("TABLES"),
                Token(TokenKind::LeftParen, "("),
                R("EdgeDefinitions"),
                Token(TokenKind::RightParen, ")"),
            ]),
        ),
        ("VertexDefinitions", native(native::vertices, false)),
        ("EdgeDefinitions", native(native::edges, false)),
    ]);
    prepend_choice(rules, "Statement", "CreatePropertyGraph")?;
    prepend_choice(rules, "FromItem", "GraphTable")
}
