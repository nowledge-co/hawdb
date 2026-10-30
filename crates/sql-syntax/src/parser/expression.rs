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

//! SQL expression precedence is recognized by token PEG rules. The CST fold
//! constructs syntax nodes without making parsing decisions or rescanning SQL.

use super::identifier_from_token;
use crate::peg::{Expression as E, NodeId, ParseTree};
use crate::{
    BinaryOperatorSyntax as B, ExpressionKindSyntax as Kind, ExpressionSyntax, LiteralSyntax,
    QualifiedName, Span, Token, TokenKind as T, UnaryOperatorSyntax as U,
};

type Rule = (&'static str, E<()>);
fn seq(items: Vec<E<()>>) -> E<()> {
    E::Sequence(items)
}
fn choice(items: Vec<E<()>>) -> E<()> {
    E::Choice(items)
}
fn opt(item: E<()>) -> E<()> {
    E::Optional(Box::new(item))
}
fn rep(item: E<()>) -> E<()> {
    E::Repeat(Box::new(item))
}
fn r(name: &'static str) -> E<()> {
    E::Rule(name)
}
fn k(name: &'static str) -> E<()> {
    E::Keyword(name)
}
fn t(kind: T) -> E<()> {
    E::Token(kind, "an expression token")
}
fn list(name: &'static str) -> E<()> {
    seq(vec![r(name), rep(seq(vec![t(T::Comma), r(name)]))])
}
fn binary(name: &'static str, operand: &'static str, operator: &'static str) -> Rule {
    (
        name,
        seq(vec![r(operand), rep(seq(vec![r(operator), r(operand)]))]),
    )
}
pub(super) const NESTING_RULES: &[&str] = &[
    "NotOperand",
    "SignOperand",
    "ParenthesizedContents",
    "ArgumentContents",
    "InContents",
    "FilterContents",
];
pub(super) fn rules() -> Vec<Rule> {
    vec![
        ("Input", seq(vec![r("Or"), t(T::End)])),
        binary("Or", "And", "OrOp"),
        ("OrOp", k("OR")),
        binary("And", "Not", "AndOp"),
        ("AndOp", k("AND")),
        ("Not", choice(vec![r("NotPrefix"), r("Compare")])),
        ("NotPrefix", seq(vec![k("NOT"), r("NotOperand")])),
        ("NotOperand", r("Not")),
        ("Compare", seq(vec![r("Concat"), rep(r("Comparison"))])),
        (
            "Comparison",
            choice(vec![
                r("BinaryComparison"),
                r("IsNull"),
                r("In"),
                r("Between"),
                r("Like"),
            ]),
        ),
        ("BinaryComparison", seq(vec![r("CompareOp"), r("Concat")])),
        (
            "CompareOp",
            choice(vec![
                t(T::Equal),
                t(T::NotEqual),
                t(T::Less),
                t(T::LessOrEqual),
                t(T::Greater),
                t(T::GreaterOrEqual),
            ]),
        ),
        ("IsNull", seq(vec![k("IS"), opt(r("Negated")), k("NULL")])),
        ("Negated", k("NOT")),
        ("InContents", list("Or")),
        ("In", seq(vec![opt(r("Negated")), k("IN"), r("InValues")])),
        (
            "InValues",
            seq(vec![t(T::LeftParen), r("InContents"), t(T::RightParen)]),
        ),
        (
            "Between",
            seq(vec![
                opt(r("Negated")),
                k("BETWEEN"),
                r("Concat"),
                k("AND"),
                r("Concat"),
            ]),
        ),
        (
            "Like",
            seq(vec![
                opt(r("Negated")),
                r("LikeOp"),
                r("Concat"),
                opt(r("Escape")),
            ]),
        ),
        ("LikeOp", choice(vec![k("LIKE"), k("ILIKE")])),
        ("Escape", seq(vec![k("ESCAPE"), r("Concat")])),
        binary("Concat", "Sum", "ConcatOp"),
        ("ConcatOp", t(T::Concat)),
        binary("Sum", "Product", "SumOp"),
        ("SumOp", choice(vec![t(T::Plus), t(T::Minus)])),
        binary("Product", "Signed", "ProductOp"),
        (
            "ProductOp",
            choice(vec![t(T::Star), t(T::Slash), t(T::Percent)]),
        ),
        (
            "Signed",
            choice(vec![r("SignPrefix"), r("NotPrefix"), r("Postfix")]),
        ),
        ("SignPrefix", seq(vec![r("SumOp"), r("SignOperand")])),
        ("SignOperand", r("Signed")),
        (
            "Postfix",
            seq(vec![r("Atom"), rep(choice(vec![r("Cast"), r("Collate")]))]),
        ),
        ("Cast", seq(vec![t(T::DoubleColon), r("Name")])),
        ("Collate", seq(vec![k("COLLATE"), r("Name")])),
        (
            "Atom",
            choice(vec![
                r("Parenthesized"),
                r("Wildcard"),
                r("Parameter"),
                r("Number"),
                r("String"),
                r("Null"),
                r("True"),
                r("False"),
                r("TypedString"),
                r("Function"),
                r("QualifiedWildcard"),
                r("Column"),
            ]),
        ),
        (
            "Parenthesized",
            seq(vec![
                t(T::LeftParen),
                r("ParenthesizedContents"),
                t(T::RightParen),
            ]),
        ),
        ("ParenthesizedContents", r("Or")),
        ("Wildcard", t(T::Star)),
        ("Parameter", E::Parameter),
        ("Number", t(T::Number)),
        ("String", t(T::String)),
        ("Null", k("NULL")),
        ("True", k("TRUE")),
        ("False", k("FALSE")),
        (
            "TypedString",
            seq(vec![
                choice(vec![k("DATE"), k("TIME"), k("TIMESTAMP"), k("INTERVAL")]),
                t(T::String),
            ]),
        ),
        (
            "Function",
            seq(vec![r("Name"), r("Arguments"), opt(r("Filter"))]),
        ),
        (
            "Arguments",
            seq(vec![
                t(T::LeftParen),
                r("ArgumentContents"),
                t(T::RightParen),
            ]),
        ),
        (
            "ArgumentContents",
            seq(vec![
                opt(choice(vec![r("FunctionDistinct"), k("ALL")])),
                opt(list("Or")),
            ]),
        ),
        ("FunctionDistinct", k("DISTINCT")),
        (
            "Filter",
            seq(vec![
                k("FILTER"),
                t(T::LeftParen),
                r("FilterContents"),
                t(T::RightParen),
            ]),
        ),
        ("FilterContents", seq(vec![k("WHERE"), r("Or")])),
        (
            "QualifiedWildcard",
            seq(vec![r("Name"), t(T::Dot), t(T::Star)]),
        ),
        ("Column", r("Name")),
        (
            "Name",
            seq(vec![
                r("Identifier"),
                rep(seq(vec![t(T::Dot), r("Identifier")])),
            ]),
        ),
        (
            "Identifier",
            choice(vec![t(T::Word), t(T::QuotedIdentifier)]),
        ),
    ]
}

pub(super) struct Fold<'a> {
    pub(super) tree: &'a ParseTree<()>,
    pub(super) tokens: &'a [Token],
}
impl Fold<'_> {
    fn span(&self, node: NodeId) -> Span {
        self.tree.span(node, self.tokens)
    }
    fn first(&self, node: NodeId) -> NodeId {
        self.tree.children(node)[0]
    }
    pub(super) fn name(&self, node: NodeId) -> QualifiedName {
        QualifiedName {
            parts: self
                .tree
                .children(node)
                .iter()
                .map(|id| identifier_from_token(self.tokens[self.tree.nodes[*id].start]))
                .collect(),
            span: self.span(node),
        }
    }
    pub(super) fn expression(&self, node: NodeId) -> ExpressionSyntax {
        let children = self.tree.children(node);
        let span = self.span(node);
        let kind = match self.tree.nodes[node].name.expect("named expression node") {
            "Not" | "Signed" | "Atom" | "NotOperand" | "SignOperand" | "ParenthesizedContents" => {
                return self.expression(self.first(node))
            }
            "Or" | "And" | "Concat" | "Sum" | "Product" => {
                let mut left = self.expression(children[0]);
                for pair in children[1..].chunks_exact(2) {
                    let right = self.expression(pair[1]);
                    let operator = self.operator(pair[0]);
                    left = ExpressionSyntax {
                        span: Span::new(left.span.start, right.span.end),
                        kind: Kind::Binary {
                            left: Box::new(left),
                            operator,
                            right: Box::new(right),
                        },
                    };
                }
                return left;
            }
            "Compare" => {
                let mut left = self.expression(children[0]);
                for suffix in &children[1..] {
                    left = self.comparison(left, self.first(*suffix));
                }
                return left;
            }
            "Postfix" => {
                let mut expression = self.expression(children[0]);
                for suffix in &children[1..] {
                    let name = self.name(self.tree.required(*suffix, "Name"));
                    let end = name.span.end;
                    let kind = if self.tree.nodes[*suffix].name == Some("Cast") {
                        Kind::Cast {
                            expression: Box::new(expression),
                            data_type: name,
                        }
                    } else {
                        Kind::Collate {
                            expression: Box::new(expression),
                            collation: name,
                        }
                    };
                    expression = ExpressionSyntax {
                        kind,
                        span: Span::new(span.start, end),
                    };
                }
                return expression;
            }
            "NotPrefix" => Kind::Unary {
                operator: U::Not,
                expression: Box::new(self.expression(children[0])),
            },
            "SignPrefix" => Kind::Unary {
                operator: if self.tokens[self.tree.nodes[node].start].kind == T::Minus {
                    U::Minus
                } else {
                    U::Plus
                },
                expression: Box::new(self.expression(children[1])),
            },
            "Parenthesized" => Kind::Parenthesized(Box::new(self.expression(children[0]))),
            "Wildcard" => Kind::Wildcard(None),
            "QualifiedWildcard" => Kind::Wildcard(Some(self.name(children[0]))),
            "Column" => Kind::Column(self.name(children[0])),
            "Parameter" => {
                let T::Parameter(position) = self.tokens[self.tree.nodes[node].start].kind else {
                    unreachable!()
                };
                Kind::Parameter(position)
            }
            "Number" => Kind::Literal(LiteralSyntax::Number(span)),
            "String" => Kind::Literal(LiteralSyntax::String(span)),
            "Null" => Kind::Literal(LiteralSyntax::Null),
            "True" => Kind::Literal(LiteralSyntax::Boolean(true)),
            "False" => Kind::Literal(LiteralSyntax::Boolean(false)),
            "TypedString" => Kind::TypedString {
                data_type: identifier_from_token(self.tokens[self.tree.nodes[node].start]),
                value: self.tokens[self.tree.nodes[node].start + 1].span,
            },
            "Function" => {
                let args = self.tree.required(node, "Arguments");
                let contents = self.tree.required(args, "ArgumentContents");
                let arguments = self
                    .tree
                    .children(contents)
                    .iter()
                    .filter(|id| self.tree.nodes[**id].name == Some("Or"))
                    .map(|id| self.expression(*id))
                    .collect();
                let function_end = self.span(args).end;
                let function = Kind::Function {
                    name: self.name(children[0]),
                    arguments,
                    distinct: self.tree.child(contents, "FunctionDistinct").is_some(),
                };
                if let Some(filter) = self.tree.child(node, "Filter") {
                    Kind::Filtered {
                        function: Box::new(ExpressionSyntax {
                            kind: function,
                            span: Span::new(span.start, function_end),
                        }),
                        predicate: Box::new(
                            self.expression(
                                self.tree
                                    .required(self.tree.required(filter, "FilterContents"), "Or"),
                            ),
                        ),
                    }
                } else {
                    function
                }
            }
            other => unreachable!("unexpected expression CST node {other}"),
        };
        ExpressionSyntax { kind, span }
    }
    fn operator(&self, node: NodeId) -> B {
        match self.tokens[self.tree.nodes[node].start].kind {
            T::Equal => B::Equal,
            T::NotEqual => B::NotEqual,
            T::Less => B::Less,
            T::LessOrEqual => B::LessOrEqual,
            T::Greater => B::Greater,
            T::GreaterOrEqual => B::GreaterOrEqual,
            T::Concat => B::Concat,
            T::Plus => B::Add,
            T::Minus => B::Subtract,
            T::Star => B::Multiply,
            T::Slash => B::Divide,
            T::Percent => B::Modulo,
            T::Word if self.tree.nodes[node].name == Some("OrOp") => B::Or,
            T::Word => B::And,
            _ => unreachable!("operator matched by grammar"),
        }
    }
    fn comparison(&self, left: ExpressionSyntax, suffix: NodeId) -> ExpressionSyntax {
        let span = Span::new(left.span.start, self.span(suffix).end);
        let negated = self.tree.child(suffix, "Negated").is_some();
        let children = self.tree.children(suffix);
        let expression = Box::new(left);
        let kind = match self.tree.nodes[suffix].name.unwrap() {
            "BinaryComparison" => Kind::Binary {
                left: expression,
                operator: self.operator(children[0]),
                right: Box::new(self.expression(children[1])),
            },
            "IsNull" => Kind::IsNull {
                expression,
                negated,
            },
            "In" => {
                let values = self
                    .tree
                    .required(self.tree.required(suffix, "InValues"), "InContents");
                Kind::InList {
                    expression,
                    negated,
                    values: self
                        .tree
                        .children(values)
                        .iter()
                        .map(|id| self.expression(*id))
                        .collect(),
                }
            }
            "Between" => {
                let values: Vec<_> = children
                    .iter()
                    .filter(|id| self.tree.nodes[**id].name == Some("Concat"))
                    .collect();
                Kind::Between {
                    expression,
                    negated,
                    low: Box::new(self.expression(*values[0])),
                    high: Box::new(self.expression(*values[1])),
                }
            }
            "Like" => {
                let op = self.tree.required(suffix, "LikeOp");
                Kind::Like {
                    expression,
                    negated,
                    case_insensitive: self.tokens[self.tree.nodes[op].start].span.len() == 5,
                    pattern: Box::new(self.expression(self.tree.required(suffix, "Concat"))),
                    escape: self
                        .tree
                        .child(suffix, "Escape")
                        .map(|id| Box::new(self.expression(self.tree.required(id, "Concat")))),
                }
            }
            _ => unreachable!("comparison matched by grammar"),
        };
        ExpressionSyntax { kind, span }
    }
}
