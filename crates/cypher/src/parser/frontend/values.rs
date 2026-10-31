// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher value syntax is recognized by PEG; this fold only decodes the CST.
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;

use super::Transformer;
use crate::*;

pub(super) fn r(name: &'static str) -> E {
    E::Rule(name)
}
pub(super) fn k(name: &'static str) -> E {
    E::Keyword(name)
}
pub(super) fn t(kind: T) -> E {
    E::Token(
        kind,
        match kind {
            T::Number => "integer digits",
            T::Word => "identifier",
            T::Dot => "'.'",
            _ => "a Cypher value token",
        },
    )
}
pub(super) fn s(items: Vec<E>) -> E {
    E::Sequence(items)
}
pub(super) fn c(items: Vec<E>) -> E {
    E::Choice(items)
}
pub(super) fn o(item: E) -> E {
    E::Optional(Box::new(item))
}
pub(super) fn many(item: E) -> E {
    E::Repeat(Box::new(item))
}
// Ordinary bindings skip the call alternatives. Known function heads still
// enter them without `(` so an incomplete call retains its deeper diagnostic.
pub(super) fn call_start(heads: Vec<E>) -> E {
    E::Ahead(
        Box::new(c(vec![s(vec![t(T::Word), t(T::LeftParen)]), c(heads)])),
        "a Cypher value token",
    )
}
pub(super) fn group(item: E) -> E {
    s(vec![t(T::LeftParen), item, t(T::RightParen)])
}
pub(super) fn list(name: &'static str) -> E {
    s(vec![r(name), many(s(vec![t(T::Comma), r(name)]))])
}
fn tight(item: E) -> E {
    E::Tight(Box::new(item))
}

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        ("v_identifier", t(T::Word)),
        (
            "v_value",
            c(vec![
                r("v_parameter"),
                r("v_list"),
                r("v_string"),
                r("v_number"),
                r("v_boolean"),
                r("v_null"),
                r("v_current_timestamp"),
                r("v_timestamp"),
                r("v_cast"),
                r("v_property"),
            ]),
        ),
        (
            "v_parameter",
            s(vec![t(T::Parameter(0)), r("v_identifier")]),
        ),
        (
            "v_list",
            s(vec![
                t(T::LeftBracket),
                r("v_list_contents"),
                t(T::RightBracket),
            ]),
        ),
        (
            "v_list_contents",
            o(s(vec![list("v_value"), o(t(T::Comma))])),
        ),
        ("v_string", t(T::String)),
        (
            "v_number",
            tight(c(vec![
                s(vec![
                    o(t(T::Minus)),
                    t(T::Number),
                    t(T::Dot),
                    E::Not(Box::new(t(T::Dot)), "a decimal point"),
                    o(t(T::Number)),
                ]),
                s(vec![t(T::Minus), t(T::Dot), t(T::Number)]),
                s(vec![o(t(T::Minus)), t(T::Number)]),
            ])),
        ),
        ("v_boolean", c(vec![k("true"), k("false")])),
        ("v_null", k("null")),
        (
            "v_current_timestamp",
            s(vec![k("CURRENT_TIMESTAMP"), group(s(vec![]))]),
        ),
        (
            "v_timestamp",
            s(vec![k("timestamp"), group(r("v_call_contents"))]),
        ),
        (
            "v_cast",
            s(vec![
                k("CAST"),
                group(s(vec![r("v_call_contents"), k("AS"), k("TIMESTAMP")])),
            ]),
        ),
        ("v_call_contents", r("v_value")),
        (
            "v_property",
            s(vec![
                E::Not(
                    Box::new(c(vec![
                        k("true"),
                        k("false"),
                        k("null"),
                        k("CURRENT_TIMESTAMP"),
                        k("timestamp"),
                        k("CAST"),
                    ])),
                    "a property binding",
                ),
                r("v_identifier"),
                o(s(vec![t(T::Dot), r("v_identifier")])),
            ]),
        ),
        (
            "v_properties",
            s(vec![
                t(T::LeftBrace),
                o(s(vec![list("v_property_entry"), o(t(T::Comma))])),
                t(T::RightBrace),
            ]),
        ),
        (
            "v_property_entry",
            s(vec![r("v_identifier"), t(T::Colon), r("v_value")]),
        ),
    ]
}

impl Transformer<'_> {
    pub(super) fn text(&self, node: NodeId) -> &str {
        let span = self.tree.span(node, self.tokens);
        &self.input[span.start..span.end]
    }
    fn identifiers(&self, node: NodeId) -> impl Iterator<Item = &str> {
        self.tree
            .children(node)
            .iter()
            .filter(|id| self.tree.nodes[**id].name == Some("v_identifier"))
            .map(|id| self.text(*id))
    }
    pub(super) fn ids(&self, node: NodeId) -> Vec<String> {
        self.identifiers(node).map(str::to_owned).collect()
    }
    pub(super) fn identifier_pair(&self, node: NodeId) -> (String, String) {
        let mut names = self.identifiers(node);
        (
            names.next().expect("first identifier").to_owned(),
            names.next().expect("second identifier").to_owned(),
        )
    }
    pub(super) fn joined_ids(&self, node: NodeId, separator: &str) -> String {
        let (count, bytes) = self
            .identifiers(node)
            .fold((0usize, 0usize), |(count, bytes), name| {
                (count + 1, bytes + name.len())
            });
        let mut result = String::with_capacity(bytes + separator.len() * count.saturating_sub(1));
        for (index, name) in self.identifiers(node).enumerate() {
            if index > 0 {
                result.push_str(separator);
            }
            result.push_str(name);
        }
        result
    }
    pub(super) fn value(&mut self, node: NodeId, depth: usize) -> Result<ValueExpression> {
        self.check_depth(node, depth, 1)?;
        let selected = self.tree.children(node)[0];
        let kind = match self.tree.nodes[selected].name.expect("named value rule") {
            "v_parameter" => ValueExpressionKind::Parameter(
                self.text(self.tree.required(selected, "v_identifier"))
                    .to_owned(),
            ),
            "v_property" => {
                let mut names = self.identifiers(selected);
                let variable = names.next().expect("value binding").to_owned();
                if let Some(property) = names.next() {
                    ValueExpressionKind::BindingProperty {
                        variable,
                        property: property.to_owned(),
                    }
                } else {
                    ValueExpressionKind::Binding(variable)
                }
            }
            "v_boolean" => ValueExpressionKind::Literal(Value::Bool(
                self.text(selected).eq_ignore_ascii_case("true"),
            )),
            "v_null" => ValueExpressionKind::Literal(Value::Null),
            "v_string" => {
                ValueExpressionKind::Literal(Value::String(self.decode_string(selected)?))
            }
            "v_number" => {
                let raw = self.text(selected);
                ValueExpressionKind::Literal(if raw.contains('.') {
                    Value::Float(
                        raw.parse()
                            .map_err(|_| self.error(selected, "invalid float"))?,
                    )
                } else {
                    Value::Int(
                        raw.parse()
                            .map_err(|_| self.error(selected, "invalid integer"))?,
                    )
                })
            }
            "v_list" => {
                let children = self
                    .tree
                    .children(self.tree.required(selected, "v_list_contents"))
                    .iter()
                    .copied();
                ValueExpressionKind::List(
                    children
                        .map(|child| self.value(child, depth + 1))
                        .collect::<Result<_>>()?,
                )
            }
            "v_current_timestamp" => ValueExpressionKind::CurrentTimestamp,
            "v_timestamp" | "v_cast" => ValueExpressionKind::Timestamp(Box::new(
                self.value(
                    self.tree
                        .required(self.tree.required(selected, "v_call_contents"), "v_value"),
                    depth + 1,
                )?,
            )),
            _ => unreachable!("known value syntax"),
        };
        let span = self.tree.span(node, self.tokens);
        Ok(AstNode::from_source(
            kind,
            SourceSpan {
                start: span.start,
                end: span.end,
            },
        ))
    }
    pub(super) fn decode_string(&self, node: NodeId) -> Result<String> {
        let raw = self.text(node);
        let mut chars = raw.chars();
        let quote = chars.next().expect("string token");
        let mut out = String::new();
        while let Some(ch) = chars.next() {
            if ch == quote {
                if chars.next().is_none() {
                    return Ok(out);
                }
                return Err(self.error(node, "unexpected string contents"));
            }
            if ch == '\\' {
                out.push(match chars.next() {
                    Some('\\') => '\\',
                    Some('\'') => '\'',
                    Some('"') => '"',
                    Some('n') => '\n',
                    Some('r') => '\r',
                    Some('t') => '\t',
                    Some(ch) => {
                        return Err(self.error(node, &format!("unsupported escape sequence \\{ch}")))
                    }
                    None => return Err(self.error(node, "unterminated escape sequence")),
                });
            } else {
                out.push(ch);
            }
        }
        Err(self.error(node, "unterminated string"))
    }
    pub(super) fn properties(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<std::collections::BTreeMap<String, ValueExpression>> {
        let children = self.tree.children(node).iter().copied();
        let mut properties = std::collections::BTreeMap::new();
        for child in children {
            let name = self
                .text(self.tree.required(child, "v_identifier"))
                .to_owned();
            let value = self.value(self.tree.required(child, "v_value"), depth)?;
            properties.insert(name, value);
        }
        Ok(properties)
    }
}
