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

//! SQL and PGQ token grammars. All syntax decisions precede AST construction.
use super::super::expression;
use crate::peg::{Expression as E, Grammar};
use crate::TokenKind as T;

type Rule = (&'static str, E<()>);
fn s(items: Vec<E<()>>) -> E<()> {
    E::Sequence(items)
}
fn c(items: Vec<E<()>>) -> E<()> {
    E::Choice(items)
}
fn o(item: E<()>) -> E<()> {
    E::Optional(Box::new(item))
}
fn many(item: E<()>) -> E<()> {
    E::Repeat(Box::new(item))
}
fn r(name: &'static str) -> E<()> {
    E::Rule(name)
}
fn k(name: &'static str) -> E<()> {
    E::Keyword(name)
}
fn t(kind: T) -> E<()> {
    E::Token(kind, "a SQL/PGQ token")
}
fn not(item: E<()>, expected: &'static str) -> E<()> {
    E::Not(Box::new(item), expected)
}
fn list(name: &'static str) -> E<()> {
    s(vec![r(name), many(s(vec![t(T::Comma), r(name)]))])
}
fn complete(name: &'static str) -> E<()> {
    s(vec![
        r(name),
        o(t(T::Semicolon)),
        E::Token(
            T::End,
            if name == "GraphTable" {
                "end of GRAPH_TABLE clause"
            } else {
                "end of statement"
            },
        ),
    ])
}
fn keywords(names: &[&'static str]) -> E<()> {
    c(names.iter().copied().map(k).collect())
}

pub(super) fn build() -> Result<Grammar<()>, String> {
    let mut rules: Vec<Rule> = vec![
        ("SqlInput", complete("Statement")),
        ("SelectInput", complete("Select")),
        ("RelationalStatementInput", complete("RelationalStatement")),
        ("PgqInput", complete("CreatePropertyGraph")),
        ("GraphTableInput", complete("GraphTable")),
        ("Statement", c(vec![r("CreatePropertyGraph"), r("Select")])),
        (
            "RelationalStatement",
            c(vec![r("RelationalSelect"), r("RelationalExplain")]),
        ),
        (
            "RelationalExplain",
            s(vec![k("EXPLAIN"), o(r("Analyze")), r("RelationalSelect")]),
        ),
        ("Analyze", k("ANALYZE")),
        (
            "Select",
            s(vec![
                k("SELECT"),
                o(r("Quantifier")),
                r("Projection"),
                k("FROM"),
                r("From"),
                o(r("Where")),
                o(r("Group")),
                o(r("Having")),
                o(r("Order")),
                o(r("Limit")),
                o(r("Offset")),
                o(r("Locking")),
            ]),
        ),
        (
            "RelationalSelect",
            s(vec![
                k("SELECT"),
                o(r("Quantifier")),
                r("RelationalProjection"),
                o(s(vec![k("FROM"), r("RelationalFrom")])),
                o(r("Where")),
                o(r("Group")),
                o(r("Having")),
                o(r("Order")),
                o(c(vec![
                    s(vec![r("RelationalLimit"), o(r("RelationalOffset"))]),
                    s(vec![r("RelationalOffset"), o(r("RelationalLimit"))]),
                ])),
                o(r("RelationalLocking")),
            ]),
        ),
        ("Quantifier", c(vec![r("Distinct"), k("ALL")])),
        (
            "Distinct",
            s(vec![
                k("DISTINCT"),
                not(k("ON"), "a projection after DISTINCT"),
            ]),
        ),
        ("Projection", list("Labeled")),
        ("RelationalProjection", o(list("RelationalLabeled"))),
        ("Labeled", s(vec![r("Or"), o(r("AsAlias"))])),
        (
            "RelationalLabeled",
            s(vec![
                not(k("FROM"), "a projection"),
                r("Or"),
                o(c(vec![r("AsAlias"), r("ProjectionAlias")])),
            ]),
        ),
        ("AsAlias", s(vec![k("AS"), r("Identifier")])),
        (
            "ProjectionAlias",
            s(vec![
                not(r("ProjectionBoundary"), "a projection alias"),
                r("Identifier"),
            ]),
        ),
        (
            "ProjectionBoundary",
            keywords(&[
                "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "FETCH",
            ]),
        ),
        (
            "From",
            s(vec![
                list("FromSource"),
                E::Ahead(
                    Box::new(c(vec![r("ProjectionBoundary"), t(T::Semicolon), t(T::End)])),
                    "a join, comma, or supported SELECT clause after FROM item",
                ),
            ]),
        ),
        ("RelationalFrom", list("RelationalFromSource")),
        ("FromSource", s(vec![r("FromItem"), many(r("Join"))])),
        (
            "RelationalFromSource",
            s(vec![r("RelationalFromItem"), many(r("RelationalJoin"))]),
        ),
        ("FromItem", c(vec![r("GraphTable"), r("Relation")])),
        ("RelationalFromItem", r("RelationalRelation")),
        (
            "Relation",
            s(vec![
                not(
                    c(vec![k("GRAPH_TABLE"), r("ProjectionBoundary")]),
                    "a relation name",
                ),
                r("Name"),
                o(r("Alias")),
            ]),
        ),
        (
            "RelationalRelation",
            s(vec![
                not(
                    c(vec![k("GRAPH_TABLE"), r("ProjectionBoundary")]),
                    "a relation name",
                ),
                r("Name"),
                o(r("TableArguments")),
                o(c(vec![r("RelationalAsAlias"), r("Alias")])),
            ]),
        ),
        (
            "TableArguments",
            s(vec![
                t(T::LeftParen),
                o(list("TableArgument")),
                t(T::RightParen),
            ]),
        ),
        ("TableArgument", s(vec![o(r("ArgumentName")), r("Or")])),
        (
            "ArgumentName",
            s(vec![r("Identifier"), t(T::Equal), t(T::Greater)]),
        ),
        (
            "Alias",
            s(vec![
                o(k("AS")),
                not(r("AliasBoundary"), "a table alias"),
                r("Identifier"),
                o(r("Identifiers")),
            ]),
        ),
        // Relational AS aliases historically admit keywords; bare aliases do not.
        (
            "RelationalAsAlias",
            s(vec![k("AS"), r("Identifier"), o(r("Identifiers"))]),
        ),
        (
            "AliasBoundary",
            keywords(&[
                "WHERE",
                "GROUP",
                "HAVING",
                "ORDER",
                "LIMIT",
                "OFFSET",
                "FETCH",
                "FOR",
                "JOIN",
                "INNER",
                "LEFT",
                "RIGHT",
                "FULL",
                "CROSS",
                "ON",
                "USING",
                "NATURAL",
                "UNION",
                "EXCEPT",
                "INTERSECT",
                "LATERAL",
                "TABLESAMPLE",
                "WINDOW",
            ]),
        ),
        (
            "Join",
            c(vec![r("InnerJoin"), r("LeftJoin"), r("CrossJoin")]),
        ),
        (
            "RelationalJoin",
            c(vec![
                r("RelationalInnerJoin"),
                r("RelationalLeftJoin"),
                r("RelationalCrossJoin"),
            ]),
        ),
        (
            "InnerJoin",
            s(vec![
                o(k("INNER")),
                k("JOIN"),
                r("FromItem"),
                k("ON"),
                r("Or"),
            ]),
        ),
        (
            "LeftJoin",
            s(vec![
                k("LEFT"),
                o(k("OUTER")),
                k("JOIN"),
                r("FromItem"),
                k("ON"),
                r("Or"),
            ]),
        ),
        ("CrossJoin", s(vec![k("CROSS"), k("JOIN"), r("FromItem")])),
        (
            "RelationalInnerJoin",
            s(vec![
                o(k("INNER")),
                k("JOIN"),
                r("RelationalFromItem"),
                k("ON"),
                r("Or"),
            ]),
        ),
        (
            "RelationalLeftJoin",
            s(vec![
                k("LEFT"),
                o(k("OUTER")),
                k("JOIN"),
                r("RelationalFromItem"),
                k("ON"),
                r("Or"),
            ]),
        ),
        (
            "RelationalCrossJoin",
            s(vec![k("CROSS"), k("JOIN"), r("RelationalFromItem")]),
        ),
        ("Where", s(vec![k("WHERE"), r("Or")])),
        ("Having", s(vec![k("HAVING"), r("Or")])),
        ("Group", s(vec![k("GROUP"), k("BY"), list("Or")])),
        ("Order", s(vec![k("ORDER"), k("BY"), list("OrderItem")])),
        (
            "OrderItem",
            s(vec![
                r("Or"),
                o(c(vec![r("Ascending"), r("Descending")])),
                o(r("NullOrder")),
            ]),
        ),
        ("Ascending", k("ASC")),
        ("Descending", k("DESC")),
        (
            "NullOrder",
            s(vec![k("NULLS"), c(vec![r("First"), r("Last")])]),
        ),
        ("First", k("FIRST")),
        ("Last", k("LAST")),
        ("Limit", s(vec![k("LIMIT"), r("Or")])),
        ("Offset", s(vec![k("OFFSET"), r("Or")])),
        (
            "RelationalLimit",
            s(vec![k("LIMIT"), c(vec![r("LimitAll"), r("Or")])]),
        ),
        ("LimitAll", k("ALL")),
        (
            "RelationalOffset",
            s(vec![k("OFFSET"), r("Or"), o(keywords(&["ROW", "ROWS"]))]),
        ),
        (
            "Locking",
            s(vec![k("FOR"), c(vec![r("Share"), r("Update")])]),
        ),
        (
            "RelationalLocking",
            s(vec![r("Locking"), o(r("Nonblocking"))]),
        ),
        ("Share", k("SHARE")),
        ("Update", k("UPDATE")),
        (
            "Nonblocking",
            c(vec![k("NOWAIT"), s(vec![k("SKIP"), k("LOCKED")])]),
        ),
        (
            "GraphTable",
            s(vec![
                k("GRAPH_TABLE"),
                t(T::LeftParen),
                r("Name"),
                k("MATCH"),
                r("Pattern"),
                k("COLUMNS"),
                t(T::LeftParen),
                list("Labeled"),
                t(T::RightParen),
                t(T::RightParen),
                o(r("Alias")),
            ]),
        ),
        ("Pattern", s(vec![list("Path"), o(r("Where"))])),
        ("Path", s(vec![r("Factor"), many(r("Factor"))])),
        ("Factor", s(vec![r("Primary"), o(r("GraphQuantifier"))])),
        ("Primary", c(vec![r("NestedPath"), r("Vertex"), r("Edge")])),
        (
            "NestedPath",
            s(vec![
                t(T::LeftParen),
                E::Ahead(
                    Box::new(c(vec![
                        t(T::LeftParen),
                        t(T::ArrowLeft),
                        t(T::ArrowRight),
                        t(T::Less),
                        t(T::Minus),
                    ])),
                    "a nested graph path",
                ),
                r("NestedContents"),
                t(T::RightParen),
            ]),
        ),
        ("NestedContents", s(vec![r("Path"), o(r("Where"))])),
        (
            "Vertex",
            s(vec![t(T::LeftParen), r("Fields"), t(T::RightParen)]),
        ),
        (
            "Fields",
            s(vec![o(r("Variable")), o(r("Labels")), o(r("Where"))]),
        ),
        (
            "Variable",
            s(vec![
                not(keywords(&["IS", "WHERE"]), "a graph variable"),
                r("Identifier"),
            ]),
        ),
        (
            "Labels",
            s(vec![
                k("IS"),
                r("Identifier"),
                many(s(vec![t(T::Pipe), r("Identifier")])),
            ]),
        ),
        ("Edge", c(vec![r("LeftEdge"), r("RightOrAnyEdge")])),
        (
            "LeftEdge",
            s(vec![
                c(vec![t(T::ArrowLeft), s(vec![t(T::Less), t(T::Minus)])]),
                o(s(vec![r("EdgeFields"), t(T::Minus)])),
            ]),
        ),
        (
            "RightOrAnyEdge",
            c(vec![
                t(T::ArrowRight),
                s(vec![
                    t(T::Minus),
                    c(vec![
                        s(vec![r("EdgeFields"), r("EdgeEnd")]),
                        o(t(T::Greater)),
                    ]),
                ]),
            ]),
        ),
        (
            "EdgeFields",
            s(vec![t(T::LeftBracket), r("Fields"), t(T::RightBracket)]),
        ),
        ("EdgeEnd", c(vec![r("RightEnd"), t(T::Minus)])),
        (
            "RightEnd",
            c(vec![t(T::ArrowRight), s(vec![t(T::Minus), t(T::Greater)])]),
        ),
        (
            "GraphQuantifier",
            s(vec![
                t(T::LeftBrace),
                c(vec![
                    s(vec![t(T::Comma), r("Number")]),
                    s(vec![r("Number"), o(s(vec![t(T::Comma), r("Number")]))]),
                ]),
                t(T::RightBrace),
            ]),
        ),
        (
            "CreatePropertyGraph",
            s(vec![
                k("CREATE"),
                o(r("Temporary")),
                k("PROPERTY"),
                k("GRAPH"),
                r("Name"),
                o(r("Vertices")),
                o(r("Edges")),
            ]),
        ),
        ("Temporary", keywords(&["TEMP", "TEMPORARY"])),
        (
            "Vertices",
            s(vec![
                keywords(&["VERTEX", "NODE"]),
                k("TABLES"),
                t(T::LeftParen),
                list("VertexDefinition"),
                t(T::RightParen),
            ]),
        ),
        (
            "Edges",
            s(vec![
                keywords(&["EDGE", "RELATIONSHIP"]),
                k("TABLES"),
                t(T::LeftParen),
                list("EdgeDefinition"),
                t(T::RightParen),
            ]),
        ),
        (
            "VertexDefinition",
            s(vec![
                r("Name"),
                o(r("AsAlias")),
                o(r("Key")),
                o(r("Exposure")),
            ]),
        ),
        (
            "EdgeDefinition",
            s(vec![
                r("Name"),
                o(r("AsAlias")),
                o(r("Key")),
                k("SOURCE"),
                r("Endpoint"),
                k("DESTINATION"),
                r("Endpoint"),
                o(r("Exposure")),
            ]),
        ),
        ("Key", s(vec![k("KEY"), r("Identifiers")])),
        (
            "Identifiers",
            s(vec![t(T::LeftParen), list("Identifier"), t(T::RightParen)]),
        ),
        (
            "Endpoint",
            c(vec![
                s(vec![
                    r("Key"),
                    k("REFERENCES"),
                    r("Identifier"),
                    r("Identifiers"),
                ]),
                r("Identifier"),
            ]),
        ),
        (
            "Exposure",
            c(vec![
                r("Properties"),
                s(vec![r("LabelExposure"), many(r("LabelExposure"))]),
            ]),
        ),
        (
            "LabelExposure",
            s(vec![
                c(vec![r("NamedLabel"), r("DefaultLabel")]),
                o(r("Properties")),
            ]),
        ),
        ("NamedLabel", s(vec![k("LABEL"), r("Identifier")])),
        ("DefaultLabel", s(vec![k("DEFAULT"), k("LABEL")])),
        (
            "Properties",
            c(vec![
                r("NoProperties"),
                r("AllProperties"),
                r("PropertyExpressions"),
            ]),
        ),
        ("NoProperties", s(vec![k("NO"), k("PROPERTIES")])),
        (
            "AllProperties",
            s(vec![k("PROPERTIES"), k("ALL"), k("COLUMNS")]),
        ),
        (
            "PropertyExpressions",
            s(vec![
                k("PROPERTIES"),
                t(T::LeftParen),
                list("Labeled"),
                t(T::RightParen),
            ]),
        ),
    ];
    rules.extend(
        expression::rules()
            .into_iter()
            .filter(|(name, _)| !matches!(*name, "Input" | "Distinct")),
    );
    Grammar::compile_with_nesting(
        rules,
        &[
            (
                expression::NESTING_RULES,
                super::super::MAX_EXPRESSION_NESTING,
                crate::SyntaxErrorCode::ExpressionNestingLimitExceeded,
            ),
            (
                &["NestedContents"],
                super::super::MAX_GRAPH_PATTERN_NESTING,
                crate::SyntaxErrorCode::GraphPatternNestingLimitExceeded,
            ),
        ],
    )
}
