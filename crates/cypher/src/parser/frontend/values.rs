// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher value syntax is recognized by PEG; this fold only decodes the CST.
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::NodeId;

use super::Transformer;
use crate::*;

impl Transformer<'_> {
    pub(super) fn property_value(&mut self, node: NodeId, depth: usize) -> Result<ValueExpression> {
        if self.tree.rule_id(node) == rule_id!("v_value") {
            return self.value(node, depth);
        }
        self.check_depth(node, depth, 1)?;
        let variable = self.text(self.tree.required(node, rule_id!("v_identifier")));
        let kind = if let Some(contents) = self.tree.child(node, rule_id!("v_conversion_contents"))
        {
            let function = hawdb_core::ConversionFunction::from_function_name(variable)
                .expect("matched conversion name");
            let argument = self.property_value(self.tree.children(contents)[0], depth + 1)?;
            ValueExpressionKind::Conversion {
                function,
                argument: Box::new(argument),
            }
        } else {
            let index = self.tree.required(node, rule_id!("v_number"));
            ValueExpressionKind::BindingIndex {
                variable: variable.to_owned(),
                index: self.text(index).parse().map_err(|_| {
                    let span = self.tree.span(index, self.tokens);
                    if let Some((whole, _)) = self.text(index).split_once('.') {
                        if whole.parse::<i64>().is_ok() {
                            self.error_at(span.start + whole.len() + 1, "expected ']'")
                        } else {
                            self.error_at(span.start + whole.len(), "invalid integer")
                        }
                    } else {
                        self.error_at(span.end, "invalid integer")
                    }
                })?,
            }
        };
        Ok(self.source(node, kind))
    }
    pub(super) fn text(&self, node: NodeId) -> &str {
        let span = self.tree.span(node, self.tokens);
        &self.input[span.start..span.end]
    }
    fn identifiers(&self, node: NodeId) -> impl Iterator<Item = &str> {
        self.tree
            .children(node)
            .iter()
            .filter(|id| self.tree.rule_id(**id) == rule_id!("v_identifier"))
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
        let kind = match self.tree.node(selected).name.expect("named value rule") {
            "v_parameter" => ValueExpressionKind::Parameter(
                self.text(self.tree.required(selected, rule_id!("v_identifier")))
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
                    Value::Int(raw.parse().map_err(|_| {
                        self.error_at(self.tree.span(selected, self.tokens).end, "invalid integer")
                    })?)
                })
            }
            "v_list" => {
                let children = self
                    .tree
                    .children(self.tree.required(selected, rule_id!("v_list_contents")))
                    .iter()
                    .copied();
                ValueExpressionKind::List(
                    children
                        .map(|child| self.value(child, depth + 1))
                        .collect::<Result<_>>()?,
                )
            }
            "v_temporal" => self.temporal_value(selected, depth)?,
            "v_current_timestamp" => ValueExpressionKind::CurrentTimestamp,
            "v_timestamp" | "v_cast" => ValueExpressionKind::Timestamp(Box::new(self.value(
                self.tree.required(
                    self.tree.required(selected, rule_id!("v_call_contents")),
                    rule_id!("v_value"),
                ),
                depth + 1,
            )?)),
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
                    Some(ch) => super::lexer::decode_escape(ch).ok_or_else(|| {
                        self.error_at(
                            self.tree.span(node, self.tokens).start + raw.len()
                                - chars.as_str().len(),
                            &format!("unsupported escape sequence \\{ch}"),
                        )
                    })?,
                    None => return Err(self.error(node, "unterminated escape sequence")),
                });
            } else {
                out.push(ch);
            }
        }
        Err(self.error_at(self.tree.span(node, self.tokens).end, "unterminated string"))
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
                .text(self.tree.required(child, rule_id!("v_identifier")))
                .to_owned();
            let value_node = self
                .tree
                .child(child, rule_id!("v_property_value"))
                .unwrap_or_else(|| self.tree.required(child, rule_id!("v_value")));
            let value = self.property_value(value_node, depth)?;
            properties.insert(name, value);
        }
        Ok(properties)
    }
}
