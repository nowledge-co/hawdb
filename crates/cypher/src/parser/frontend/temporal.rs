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

//! Temporal calls, map arguments and scalar arithmetic over selected PEG captures.
use std::collections::{BTreeMap, BTreeSet};

use super::{expressions::scalar_binary, Transformer};
use crate::*;
use hawdb_core::{Result, TemporalFunction, Value};
use hawdb_sql_syntax::peg::NodeId;

impl Transformer<'_> {
    fn temporal_function(&self, node: NodeId, scalar: bool) -> Result<TemporalFunction> {
        let name = self.tree.required(node, rule_id!("x_temporal_name"));
        let namespace = self.tokens[self.tree.node(name).start];
        let member = self
            .tree
            .child(name, rule_id!("v_identifier"))
            .map(|id| self.text(id));
        TemporalFunction::from_name(namespace.text(self.input), member).ok_or_else(|| {
            if scalar {
                super::parse_error(
                    self.input,
                    namespace.span().end,
                    &format!("unknown temporal function {}.*", namespace.text(self.input)),
                )
            } else {
                self.error(node, "unknown temporal function")
            }
        })
    }
    fn temporal_arity(&self, node: NodeId, function: TemporalFunction, count: usize) -> Result<()> {
        let (min, max) = function.arity();
        if (min..=max).contains(&count) {
            return Ok(());
        }
        let expected = if min == max {
            min.to_string()
        } else {
            format!("{min} or {max}")
        };
        Err(super::parse_error(
            self.input,
            self.tree.span(node, self.tokens).end,
            &format!(
                "{}() expects {expected} argument(s), got {count}",
                function.function_name()
            ),
        ))
    }
    pub(super) fn temporal_scalar(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpressionKind> {
        let function = self.temporal_function(node, true)?;
        let mut arguments = Vec::new();
        for argument in self.iter_named_children(node, rule_id!("x_case_scalar")) {
            arguments.push(self.case_scalar(argument, depth)?);
        }
        self.temporal_arity(node, function, arguments.len())?;
        Ok(ScalarExpressionKind::TemporalFunction {
            function,
            arguments,
        })
    }
    pub(super) fn temporal_value(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ValueExpressionKind> {
        let function = self.temporal_function(node, false)?;
        let mut arguments = Vec::new();
        for child in self.tree.children(node).iter().copied() {
            match self.tree.node(child).name {
                Some("v_value") => arguments.push(self.value(child, depth + 1)?),
                Some("v_map") => arguments.push(self.value_map(child, depth + 1)?),
                _ => {}
            }
        }
        self.temporal_arity(node, function, arguments.len())?;
        Ok(ValueExpressionKind::Temporal {
            function,
            arguments,
        })
    }
    pub(super) fn scalar_map(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<Vec<(String, ScalarExpression)>> {
        self.expression_budget(node, depth)?;
        let input = self.input;
        let mut keys = BTreeSet::new();
        let mut entries = Vec::new();
        for pair in self.tree.children(node).chunks_exact(2) {
            let span = self.tree.span(pair[0], self.tokens);
            let key = &input[span.start..span.end];
            if !keys.insert(key) {
                return Err(super::parse_error(
                    input,
                    span.end,
                    &format!("duplicate map key '{key}'"),
                ));
            }
            let value = self.case_scalar(pair[1], depth)?;
            entries.push((key.to_owned(), value));
        }
        Ok(entries)
    }
    fn value_map(&mut self, node: NodeId, depth: usize) -> Result<ValueExpression> {
        self.check_depth(node, depth, 1)?;
        let mut entries = BTreeMap::new();
        for pair in self.tree.children(node).chunks_exact(2) {
            let key = self.text(pair[0]).to_owned();
            if entries.contains_key(&key) {
                return Err(super::parse_error(
                    self.input,
                    self.tree.span(pair[0], self.tokens).end,
                    &format!("duplicate map key '{key}'"),
                ));
            }
            let value = if self.tree.rule_id(pair[1]) == rule_id!("v_map") {
                self.value_map(pair[1], depth + 1)?
            } else {
                self.value(pair[1], depth + 1)?
            };
            entries.insert(key, value);
        }
        Ok(self.source(node, ValueExpressionKind::Map(entries)))
    }
    pub(super) fn scalar_arithmetic(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpression> {
        let product = self.tree.rule_id(node) == rule_id!("x_product");
        let children = self.tree.children(node);
        let mut expression = if product {
            self.scalar_operand(children[0], depth)?
        } else {
            self.scalar_arithmetic(children[0], depth)?
        };
        for (index, pair) in children[1..].chunks_exact(2).enumerate() {
            self.expression_budget(pair[0], depth + index + 3)?;
            let op = match self.text(pair[0]) {
                "+" => ScalarBinaryOp::Add,
                "-" => ScalarBinaryOp::Subtract,
                "*" => ScalarBinaryOp::Multiply,
                "/" => ScalarBinaryOp::Divide,
                _ => ScalarBinaryOp::Remainder,
            };
            let right = if product {
                self.scalar_operand(pair[1], depth)?
            } else {
                self.scalar_arithmetic(pair[1], depth)?
            };
            expression = scalar_binary(expression, op, right);
        }
        Ok(expression)
    }
    fn scalar_operand(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let child = self.tree.children(node)[0];
        match self.tree.node(child).name {
            Some("x_scalar") => self.scalar(child, depth),
            Some("x_timestamp_value") => {
                let value = self.value(self.tree.required(child, rule_id!("v_value")), depth)?;
                Ok(self.source(child, ScalarExpressionKind::Value(value)))
            }
            Some("x_or") => {
                let expression = self.case_boolean(child, depth + 1)?;
                Ok(self.source(node, expression.kind))
            }
            Some("x_operand") => {
                let operand = self.scalar_operand(child, depth + 1)?;
                let minus_one = self.source(
                    node,
                    ScalarExpressionKind::Value(AstNode::synthetic(ValueExpressionKind::Literal(
                        Value::Int(-1),
                    ))),
                );
                Ok(self.source(
                    node,
                    ScalarExpressionKind::Binary {
                        left: Box::new(minus_one),
                        op: ScalarBinaryOp::Multiply,
                        right: Box::new(operand),
                    },
                ))
            }
            _ => unreachable!("scalar operand capture"),
        }
    }
}

impl Transformer<'_> {
    pub(super) fn scalar_return_argument(
        &mut self,
        expression: ReturnExpression,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let span = expression.span;
        let mut scalar = match expression.kind {
            ReturnExpressionKind::Value(value) => value,
            ReturnExpressionKind::Path(path) => AstNode {
                kind: ScalarExpressionKind::Path(path),
                span,
            },
            ReturnExpressionKind::Arithmetic { first, rest } => {
                let mut value = self.scalar_return_argument(*first, node, depth)?;
                for (index, (op, right)) in rest.into_iter().enumerate() {
                    self.expression_budget(node, depth + index + 2)?;
                    let right = self.scalar_return_argument(right, node, depth)?;
                    let op = match op {
                        ArithmeticOp::Add => ScalarBinaryOp::Add,
                        ArithmeticOp::Subtract => ScalarBinaryOp::Subtract,
                        ArithmeticOp::Multiply => ScalarBinaryOp::Multiply,
                        ArithmeticOp::Divide => ScalarBinaryOp::Divide,
                        ArithmeticOp::Remainder => ScalarBinaryOp::Remainder,
                    };
                    value = scalar_binary(value, op, right);
                }
                value
            }
            _ => return Err(self.error(node, "aggregate is not a scalar argument")),
        };
        scalar.span = span;
        Ok(scalar)
    }
}

// A recognized specialized CASE can fail its equality/constant checks now that
// temporal syntax admits richer generic CASE operands. Lower those same CST
// captures to the generic AST; never retry recognition or revive an old parser.
impl Transformer<'_> {
    pub(super) fn captured_reference(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let mut identifiers = self.iter_named_children(node, rule_id!("v_identifier"));
        let variable_node = identifiers.next().expect("reference variable");
        let property_node = identifiers.next().expect("reference property");
        let variable = self.text(variable_node);
        let property = self.text(property_node).to_owned();
        let literal = if variable.eq_ignore_ascii_case("true") {
            Some(Value::Bool(true))
        } else if variable.eq_ignore_ascii_case("false") {
            Some(Value::Bool(false))
        } else if variable.eq_ignore_ascii_case("null") {
            Some(Value::Null)
        } else {
            None
        };
        if variable.eq_ignore_ascii_case("CASE") {
            return Err(self.error(node, "expected CASE expression"));
        }
        let kind = if let Some(literal) = literal {
            let value = self.source(variable_node, ValueExpressionKind::Literal(literal));
            let expression = self.source(variable_node, ScalarExpressionKind::Value(value));
            ScalarExpressionKind::PropertyAccess {
                expression: Box::new(expression),
                key: property,
            }
        } else {
            ScalarExpressionKind::Property {
                variable: variable.to_owned(),
                property,
            }
        };
        Ok(self.source(node, kind))
    }
    pub(super) fn captured_value_scalar(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpression> {
        let selected = self.tree.children(node)[0];
        if self.tree.rule_id(selected) == rule_id!("v_temporal") {
            return self.captured_temporal_scalar(selected, depth);
        }
        if self.tree.rule_id(selected) == rule_id!("v_list") {
            let value = self.value(node, depth + 1)?;
            if !value.kind.reads_row_binding() {
                return Ok(self.source(node, ScalarExpressionKind::Value(value)));
            }
            let contents = self.tree.required(selected, rule_id!("v_list_contents"));
            let mut elements = Vec::new();
            for child in self.iter_named_children(contents, rule_id!("v_value")) {
                elements.push(self.captured_value_scalar(child, depth + 2)?);
            }
            return Ok(self.source(node, ScalarExpressionKind::List(elements)));
        }
        if self.tree.rule_id(selected) == rule_id!("v_property")
            && self.tree.children(selected).len() == 2
        {
            return self.captured_reference(selected, depth);
        }
        let value = self.value(node, depth + 1)?;
        super::expressions::value_to_scalar(value)
            .ok_or_else(|| self.error(node, "expected scalar value"))
    }
    fn captured_temporal_scalar(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let function = self.temporal_function(node, true)?;
        let mut arguments = Vec::new();
        for child in self.tree.children(node).iter().copied() {
            match self.tree.node(child).name {
                Some("v_value") => arguments.push(self.captured_value_scalar(child, depth + 1)?),
                Some("v_map") => arguments.push(self.captured_map_scalar(child, depth + 1)?),
                _ => {}
            }
        }
        self.temporal_arity(node, function, arguments.len())?;
        Ok(self.source(
            node,
            ScalarExpressionKind::TemporalFunction {
                function,
                arguments,
            },
        ))
    }
    fn captured_map_scalar(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let mut entries = Vec::new();
        let mut keys = BTreeSet::new();
        let input = self.input;
        for pair in self.tree.children(node).chunks_exact(2) {
            let span = self.tree.span(pair[0], self.tokens);
            let key = &input[span.start..span.end];
            if !keys.insert(key) {
                return Err(super::parse_error(
                    input,
                    span.end,
                    &format!("duplicate map key '{key}'"),
                ));
            }
            let value = if self.tree.rule_id(pair[1]) == rule_id!("v_map") {
                self.captured_map_scalar(pair[1], depth + 1)?
            } else {
                self.captured_value_scalar(pair[1], depth + 1)?
            };
            entries.push((key.to_owned(), value));
        }
        Ok(self.source(node, ScalarExpressionKind::MapLiteral(entries)))
    }
    fn captured_null(
        &mut self,
        reference: NodeId,
        negated: bool,
        depth: usize,
    ) -> Result<ScalarExpression> {
        let expression = self.captured_reference(reference, depth)?;
        let span = SourceSpan {
            start: self.tree.span(reference, self.tokens).start,
            end: self.tokens[self.tree.node(reference).end + usize::from(negated) + 1]
                .span()
                .end,
        };
        Ok(AstNode::from_source(
            ScalarExpressionKind::IsNull {
                expression: Box::new(expression),
                negated,
            },
            span,
        ))
    }
    pub(super) fn reserved_generic_case(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpressionKind> {
        self.expression_budget(node, depth + 1)?;
        let refs = self.named_children(node, rule_id!("d_ref"));
        let values = self.named_children(node, rule_id!("v_value"));
        let (condition, result, otherwise) =
            if let Some(lower) = self.tree.child(node, rule_id!("x_special_lower")) {
                let condition = self.captured_null(refs[0], true, depth + 1)?;
                let expression = self.captured_reference(refs[1], depth + 1)?;
                let span = SourceSpan {
                    start: self.tree.span(lower, self.tokens).start,
                    end: self.tokens[self.tree.node(refs[1]).end].span().end,
                };
                let result =
                    AstNode::from_source(ScalarExpressionKind::Lower(Box::new(expression)), span);
                (
                    condition,
                    result,
                    self.captured_value_scalar(values[0], depth)?,
                )
            } else if refs.len() == 3 && values.len() == 2 {
                let left = self.captured_null(refs[0], false, depth + 1)?;
                let reference = self.captured_reference(refs[1], depth + 1)?;
                let value = self.captured_value_scalar(values[0], depth + 2)?;
                let right = scalar_binary(reference, ScalarBinaryOp::Eq, value);
                (
                    scalar_binary(left, ScalarBinaryOp::Or, right),
                    self.captured_value_scalar(values[1], depth)?,
                    self.captured_reference(refs[2], depth + 1)?,
                )
            } else if refs.len() == 2 && values.len() == 1 {
                (
                    self.captured_null(refs[0], true, depth + 1)?,
                    self.captured_reference(refs[1], depth + 1)?,
                    self.captured_value_scalar(values[0], depth)?,
                )
            } else if refs.len() == 2 && values.len() == 3 {
                let left = self.captured_null(refs[0], true, depth + 1)?;
                let reference = self.captured_reference(refs[1], depth + 1)?;
                let value = self.captured_value_scalar(values[0], depth + 2)?;
                let right = scalar_binary(reference, ScalarBinaryOp::NotEq, value);
                (
                    scalar_binary(left, ScalarBinaryOp::And, right),
                    self.captured_value_scalar(values[1], depth)?,
                    self.captured_value_scalar(values[2], depth)?,
                )
            } else {
                let mut branches = Vec::new();
                for (index, reference) in refs.into_iter().enumerate() {
                    let left = self.captured_reference(reference, depth + 1)?;
                    let right = self.captured_value_scalar(values[index * 2], depth + 1)?;
                    let result = self.captured_value_scalar(values[index * 2 + 1], depth)?;
                    branches.push((scalar_binary(left, ScalarBinaryOp::Eq, right), result));
                }
                let otherwise =
                    self.captured_value_scalar(*values.last().expect("CASE default"), depth)?;
                return Ok(ScalarExpressionKind::Case {
                    operand: None,
                    branches,
                    otherwise: Some(Box::new(otherwise)),
                });
            };
        Ok(ScalarExpressionKind::Case {
            operand: None,
            branches: vec![(condition, result)],
            otherwise: Some(Box::new(otherwise)),
        })
    }
    pub(super) fn floor_generic_case(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpressionKind> {
        self.expression_budget(node, depth + 1)?;
        let differences = self.named_children(node, rule_id!("x_difference"));
        let values = self.named_children(node, rule_id!("v_value"));
        let left = self.floor_generic_difference(differences[0], depth + 1)?;
        let bound = self.captured_value_scalar(values[0], depth + 1)?;
        let condition = scalar_binary(left, ScalarBinaryOp::Lt, bound);
        let result = self.captured_value_scalar(values[1], depth + 1)?;
        let otherwise = self.floor_generic_difference(differences[1], depth + 1)?;
        Ok(ScalarExpressionKind::Case {
            operand: None,
            branches: vec![(condition, result)],
            otherwise: Some(Box::new(otherwise)),
        })
    }
    fn floor_generic_difference(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        let mut expression = None;
        for (index, term) in self
            .iter_named_children(node, rule_id!("x_difference_term"))
            .enumerate()
        {
            self.expression_budget(term, depth + index + 1)?;
            let reference = self.captured_reference(
                self.tree.required(term, rule_id!("d_ref")),
                depth + index + 1,
            )?;
            let default = self.tree.required(term, rule_id!("v_value"));
            let selected = self.tree.children(default)[0];
            if matches!(
                self.tree.node(selected).name,
                Some("v_timestamp" | "v_cast" | "v_current_timestamp")
            ) {
                return Err(self.error(default, "expected scalar function argument"));
            }
            let default = self.captured_value_scalar(default, depth + index + 1)?;
            let value = self.source(
                term,
                ScalarExpressionKind::Coalesce(vec![reference, default]),
            );
            expression = Some(if let Some(left) = expression {
                scalar_binary(left, ScalarBinaryOp::Subtract, value)
            } else {
                value
            });
        }
        Ok(expression.expect("difference term"))
    }
}
