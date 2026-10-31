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

//! Temporal function calls, map literals, scalar arithmetic and postfix
//! property access shared by the scalar, predicate and value grammars.

use std::collections::BTreeMap;

use hawdb_core::{Result, TemporalFunction};

use super::super::ast::*;
use super::{Parser, MAX_CYPHER_PARSER_DEPTH};

impl Parser<'_> {
    /// Returns true when the cursor is at `namespace(` or `namespace.member(`
    /// for a temporal function, without consuming input.
    pub(super) fn at_temporal_call(&mut self) -> bool {
        let checkpoint = self.checkpoint();
        let result = self
            .parse_temporal_name()
            .is_some_and(|_| self.consume_char('('));
        self.restore(checkpoint);
        result
    }

    /// Consumes `namespace[.member]` when it names a temporal function and a
    /// call follows; restores the cursor and returns `None` otherwise.
    fn parse_temporal_name(&mut self) -> Option<Option<TemporalFunction>> {
        let checkpoint = self.checkpoint();
        let Ok(namespace) = self.parse_ident() else {
            self.restore(checkpoint);
            return None;
        };
        self.temporal_name_after(namespace, checkpoint)
    }

    /// Continues [`Self::parse_temporal_name`] after its namespace. The inner
    /// `None` reports an unknown member of a temporal namespace.
    fn temporal_name_after(
        &mut self,
        namespace: String,
        checkpoint: super::ParserCheckpoint,
    ) -> Option<Option<TemporalFunction>> {
        if !TemporalFunction::is_namespace(&namespace) {
            self.restore(checkpoint);
            return None;
        }
        let member_start = self.checkpoint();
        if self.peek_char() == Some('.') {
            self.pos += 1;
            if let Ok(member) = self.parse_ident() {
                self.skip_ws();
                if self.peek_char() == Some('(') {
                    return Some(TemporalFunction::from_name(&namespace, Some(&member)));
                }
            }
            self.restore(member_start);
        }
        self.skip_ws();
        if self.peek_char() == Some('(') {
            return Some(TemporalFunction::from_name(&namespace, None));
        }
        self.restore(checkpoint);
        None
    }

    /// Parses a temporal call whose namespace identifier was already
    /// consumed. Returns `None` (cursor restored to `restore`) when the
    /// identifier does not start a temporal call.
    pub(super) fn parse_scalar_temporal_call(
        &mut self,
        namespace: &str,
        restore: super::ParserCheckpoint,
    ) -> Result<Option<ScalarExpressionKind>> {
        let after_namespace = self.checkpoint();
        let Some(function) = self.temporal_name_after(namespace.to_string(), restore) else {
            return Ok(None);
        };
        let function = function.ok_or_else(|| {
            self.restore(after_namespace);
            self.error(&format!("unknown temporal function {namespace}.*"))
        })?;
        self.expect_char('(')?;
        let mut arguments = Vec::new();
        if !self.consume_char(')') {
            loop {
                arguments.push(self.parse_case_scalar()?);
                if self.consume_char(')') {
                    break;
                }
                self.expect_char(',')?;
            }
        }
        self.check_temporal_arity(function, arguments.len())?;
        Ok(Some(ScalarExpressionKind::TemporalFunction {
            function,
            arguments,
        }))
    }

    /// Parses a statically bound temporal call (`date('2026-10-30')`) in a
    /// value position such as a CREATE property map.
    pub(super) fn parse_value_temporal_call(&mut self) -> Result<Option<ValueExpressionKind>> {
        let start = self.checkpoint();
        let Some(function) = self.parse_temporal_name() else {
            return Ok(None);
        };
        let function = function.ok_or_else(|| {
            self.restore(start);
            self.error("unknown temporal function")
        })?;
        self.expect_char('(')?;
        let mut arguments = Vec::new();
        if !self.consume_char(')') {
            loop {
                arguments.push(self.parse_value_argument()?);
                if self.consume_char(')') {
                    break;
                }
                self.expect_char(',')?;
            }
        }
        self.check_temporal_arity(function, arguments.len())?;
        Ok(Some(ValueExpressionKind::Temporal {
            function,
            arguments,
        }))
    }

    fn parse_value_argument(&mut self) -> Result<ValueExpression> {
        self.skip_ws();
        if self.peek_char() != Some('{') {
            return self.parse_value();
        }
        let start = self.pos;
        let entries = self.with_recursion(|parser| {
            parser.parse_map_entries(|parser| parser.parse_value_argument())
        })?;
        Ok(self.source_node(
            ValueExpressionKind::Map(entries.into_iter().collect::<BTreeMap<_, _>>()),
            start,
        ))
    }

    fn check_temporal_arity(&self, function: TemporalFunction, count: usize) -> Result<()> {
        let (min, max) = function.arity();
        if (min..=max).contains(&count) {
            return Ok(());
        }
        let expected = if min == max {
            format!("{min}")
        } else {
            format!("{min} or {max}")
        };
        Err(self.error(&format!(
            "{}() expects {expected} argument(s), got {count}",
            function.function_name()
        )))
    }

    /// Parses `{key: value, ...}` with unique keys, in source order.
    pub(super) fn parse_map_entries<T>(
        &mut self,
        mut parse_value: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<(String, T)>> {
        self.expect_char('{')?;
        let mut entries: Vec<(String, T)> = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let key = self.parse_ident()?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(self.error(&format!("duplicate map key '{key}'")));
            }
            self.expect_char(':')?;
            let value = parse_value(self)?;
            entries.push((key, value));
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok(entries)
    }

    /// Returns true when `.identifier` follows (not `..`).
    pub(super) fn at_property_postfix(&mut self) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        rest.starts_with('.')
            && rest[1..]
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
    }

    /// Returns true when an arithmetic operator follows a complete operand.
    pub(super) fn at_arithmetic_operator(&mut self) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        matches!(rest.chars().next(), Some('+' | '*' | '/' | '%'))
            || (rest.starts_with('-') && !rest.starts_with("->") && !rest.starts_with("-["))
    }

    /// Additive scalar arithmetic over [`Self::parse_case_scalar_operand`].
    /// Operators are left-associative; `*`, `/` and `%` bind tighter than
    /// `+` and `-`. Each operator nests the AST and counts against the
    /// parser recursion budget.
    pub(super) fn parse_case_scalar(&mut self) -> Result<ScalarExpression> {
        let base_depth = self.recursion_depth;
        let result = self.parse_scalar_arithmetic(false);
        self.recursion_depth = base_depth;
        result
    }

    fn parse_scalar_arithmetic(&mut self, product: bool) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let mut expression = if product {
            self.parse_scalar_unary()?
        } else {
            self.parse_scalar_arithmetic(true)?
        };
        loop {
            self.skip_ws();
            let rest = &self.input[self.pos..];
            let op = match rest.chars().next() {
                Some('*') if product => ScalarBinaryOp::Multiply,
                Some('/') if product => ScalarBinaryOp::Divide,
                Some('%') if product => ScalarBinaryOp::Remainder,
                Some('+') if !product => ScalarBinaryOp::Add,
                Some('-') if !product && !rest.starts_with("->") && !rest.starts_with("-[") => {
                    ScalarBinaryOp::Subtract
                }
                _ => break,
            };
            if self.recursion_depth >= MAX_CYPHER_PARSER_DEPTH {
                return Err(self.error(&format!(
                    "Cypher parser nesting exceeds limit of {MAX_CYPHER_PARSER_DEPTH}"
                )));
            }
            self.recursion_depth += 1;
            self.peak_depth = self.peak_depth.max(self.recursion_depth);
            self.pos += 1;
            let right = if product {
                self.parse_scalar_unary()?
            } else {
                self.parse_scalar_arithmetic(true)?
            };
            expression = self.source_node(
                ScalarExpressionKind::Binary {
                    left: Box::new(expression),
                    op,
                    right: Box::new(right),
                },
                start,
            );
        }
        Ok(expression)
    }

    fn parse_scalar_unary(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let rest = &self.input[self.pos..];
        if rest.starts_with('-') && !rest[1..].starts_with(|ch: char| ch.is_ascii_digit()) {
            // `-x` is `-1 * x`, keeping the operand's numeric type and NULL.
            self.pos += 1;
            let operand = self.with_recursion(|parser| parser.parse_scalar_unary())?;
            let minus_one = self.source_node(
                ScalarExpressionKind::Value(AstNode::synthetic(ValueExpressionKind::Literal(
                    hawdb_core::Value::Int(-1),
                ))),
                start,
            );
            return Ok(self.source_node(
                ScalarExpressionKind::Binary {
                    left: Box::new(minus_one),
                    op: ScalarBinaryOp::Multiply,
                    right: Box::new(operand),
                },
                start,
            ));
        }
        self.parse_case_scalar_operand()
    }
}
