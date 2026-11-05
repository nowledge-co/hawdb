// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! WHERE recognition and index-compatible predicate AST construction.
use super::lexer::TokenKind as T;
use super::Transformer;
use crate::*;
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::NodeId;
impl Transformer<'_> {
    pub(super) fn fold_where(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<Option<PredicateExpression>> {
        if let Some(where_node) = self.tree.child(node, rule_id!("p_where")) {
            let predicate = self.tree.required(where_node, rule_id!("w_or"));
            let kind = self.where_or(predicate, depth, anonymous, true)?;
            Ok(Some(self.source(predicate, kind)))
        } else {
            Ok(None)
        }
    }
    pub(super) fn where_or(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
        columns: bool,
    ) -> Result<PropertyPredicate> {
        let mut predicates = Vec::new();
        let mut single = None;
        for index in 0..self.tree.children(node).len() {
            let child = self.tree.children(node)[index];
            if self.tree.rule_id(child) != rule_id!("w_and") {
                continue;
            }
            let predicate = self.where_and(child, depth, anonymous, columns)?;
            if let Some(first) = single.take() {
                predicates.push(first);
            }
            if predicates.is_empty() {
                single = Some(predicate);
            } else {
                predicates.push(predicate);
            }
        }
        Ok(if predicates.is_empty() {
            single.expect("matched boolean operand")
        } else {
            PropertyPredicate::Or(predicates)
        })
    }
    fn where_and(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
        columns: bool,
    ) -> Result<PropertyPredicate> {
        let mut predicates = Vec::new();
        let mut single = None;
        for index in 0..self.tree.children(node).len() {
            let child = self.tree.children(node)[index];
            if self.tree.rule_id(child) != rule_id!("w_atom") {
                continue;
            }
            let predicate = self.where_atom(child, depth, anonymous, columns)?;
            if let Some(first) = single.take() {
                predicates.push(first);
            }
            if predicates.is_empty() {
                single = Some(predicate);
            } else {
                predicates.push(predicate);
            }
        }
        Ok(if predicates.is_empty() {
            single.expect("matched boolean operand")
        } else {
            PropertyPredicate::And(predicates)
        })
    }
    fn where_atom(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
        columns: bool,
    ) -> Result<PropertyPredicate> {
        self.expression_budget(node, depth + 1)?;
        let selected = self.tree.children(node)[0];
        let depth = depth + 1;
        match self.tree.node(selected).name.expect("predicate rule") {
            "w_not" => Ok(PropertyPredicate::Not(Box::new(self.where_atom(
                self.tree.required(selected, rule_id!("w_atom")),
                depth,
                anonymous,
                columns,
            )?))),
            "w_group" => self.where_or(
                self.tree.required(selected, rule_id!("w_or")),
                depth,
                anonymous,
                columns,
            ),
            "w_graph" | "w_bound" => self.existence(selected, depth, anonymous),
            "w_list_contains" => {
                let (variable, property) =
                    self.identifier_pair(self.tree.required(selected, rule_id!("w_reference")));
                let value = self.value(self.tree.required(selected, rule_id!("v_value")), depth)?;
                if self
                    .text(self.tree.required(selected, rule_id!("v_identifier")))
                    .eq_ignore_ascii_case("LIST_CONTAINS_LOWER")
                {
                    Ok(PropertyPredicate::ListContainsLower {
                        variable,
                        property,
                        value,
                    })
                } else {
                    Ok(PropertyPredicate::ListContains {
                        variable,
                        property,
                        value,
                    })
                }
            }
            "w_contains_fn" => {
                let args = self.named_children(selected, rule_id!("x_scalar"));
                Ok(PropertyPredicate::ExpressionContains {
                    expression: self.scalar(args[0], depth)?,
                    value: self.scalar(args[1], depth)?,
                })
            }
            "w_id" => self.id_predicate(selected, depth),
            "w_property" | "w_parameter" => {
                let left =
                    if let Some(reference) = self.tree.child(selected, rule_id!("w_reference")) {
                        self.predicate_reference(reference)
                    } else {
                        let param = self.tree.required(selected, rule_id!("v_parameter"));
                        let value = self.source(
                            param,
                            ValueExpressionKind::Parameter(
                                self.text(self.tree.required(param, rule_id!("v_identifier")))
                                    .to_owned(),
                            ),
                        );
                        self.source(param, ScalarExpressionKind::Value(value))
                    };
                if let Some(null) = self.tree.child(selected, rule_id!("x_is_null")) {
                    let negated = self.tree.child(null, rule_id!("x_negated")).is_some();
                    return Ok(match left.kind {
                        ScalarExpressionKind::Property { variable, property } => {
                            if negated {
                                PropertyPredicate::IsNotNull { variable, property }
                            } else {
                                PropertyPredicate::IsNull { variable, property }
                            }
                        }
                        ScalarExpressionKind::Value(ValueExpression {
                            kind: ValueExpressionKind::Parameter(parameter),
                            ..
                        }) => {
                            if negated {
                                PropertyPredicate::ParameterIsNotNull { parameter }
                            } else {
                                PropertyPredicate::ParameterIsNull { parameter }
                            }
                        }
                        _ => unreachable!(),
                    });
                }
                if let Some(in_node) = self.tree.child(selected, rule_id!("w_in")) {
                    return self.in_predicate(selected, left, in_node, depth, None);
                }
                if let Some(string) = self.tree.child(selected, rule_id!("w_string")) {
                    let ScalarExpressionKind::Property { variable, property } = left.kind else {
                        unreachable!()
                    };
                    let value =
                        self.value(self.tree.required(string, rule_id!("v_value")), depth)?;
                    let op = self.tree.required(string, rule_id!("w_string_op"));
                    let first = self.tokens[self.tree.node(op).start];
                    return Ok(if first.is_keyword(self.input, "CONTAINS") {
                        PropertyPredicate::Contains {
                            variable,
                            property,
                            value,
                        }
                    } else if first.is_keyword(self.input, "STARTS") {
                        PropertyPredicate::StartsWith {
                            variable,
                            property,
                            value,
                        }
                    } else if first.is_keyword(self.input, "ENDS") {
                        PropertyPredicate::EndsWith {
                            variable,
                            property,
                            value,
                        }
                    } else {
                        PropertyPredicate::RegexMatch {
                            variable,
                            property,
                            pattern: value,
                        }
                    });
                }
                self.where_comparisons(
                    left,
                    self.tree.required(selected, rule_id!("w_comparisons")),
                    depth,
                    columns,
                )
            }
            "w_word" | "w_scalar" | "w_parenthesized_expression" => {
                let left = if let Some(sum) = self.tree.child(selected, rule_id!("r_sum")) {
                    let expression = self.return_expression(sum, depth + 1)?;
                    self.scalar_return_argument(expression, selected, depth + 1)?
                } else if let Some(scalar) = self.tree.child(selected, rule_id!("x_scalar")) {
                    let start = self.tree.node(scalar).start;
                    if !columns
                        && self
                            .tokens
                            .get(start + 1)
                            .is_some_and(|t| t.kind == T::LeftBracket)
                    {
                        return Err(self.error(selected, "expected '.'"));
                    }
                    self.scalar(scalar, depth)?
                } else if let Some(scalar) = self.tree.child(selected, rule_id!("x_case_scalar")) {
                    self.case_scalar(scalar, depth)?
                } else {
                    if !columns {
                        return Err(self.error(selected, "expected '.'"));
                    }
                    self.source(
                        self.tree.required(selected, rule_id!("v_identifier")),
                        ScalarExpressionKind::Variable(
                            self.text(self.tree.required(selected, rule_id!("v_identifier")))
                                .to_owned(),
                        ),
                    )
                };
                let Some(tail) = self.tree.child(selected, rule_id!("w_expression_tail")) else {
                    return if matches!(
                        left.kind,
                        ScalarExpressionKind::Value(AstNode {
                            kind: ValueExpressionKind::Literal(Value::Bool(_) | Value::Null),
                            ..
                        })
                    ) {
                        Ok(PropertyPredicate::ExpressionEq {
                            expression: left,
                            value: true_scalar(),
                        })
                    } else {
                        Err(self.error(selected, "expected '='"))
                    };
                };
                if let Some(null) = self.tree.child(tail, rule_id!("x_is_null")) {
                    let span = left.span;
                    let expression = AstNode {
                        kind: ScalarExpressionKind::IsNull {
                            expression: Box::new(left),
                            negated: self.tree.child(null, rule_id!("x_negated")).is_some(),
                        },
                        span,
                    };
                    return Ok(PropertyPredicate::ExpressionEq {
                        expression,
                        value: true_scalar(),
                    });
                }
                if let Some(in_node) = self.tree.child(tail, rule_id!("w_in")) {
                    return self.in_predicate(selected, left, in_node, depth, None);
                }
                if let Some(pattern) = self.tree.child(tail, rule_id!("v_value")) {
                    return Ok(PropertyPredicate::ExpressionRegexMatch {
                        expression: left,
                        pattern: self.value(pattern, depth)?,
                    });
                }
                if let Some(comparisons) =
                    self.tree.child(tail, rule_id!("w_expression_comparisons"))
                {
                    return self.where_comparisons(left, comparisons, depth, true);
                }
                Ok(PropertyPredicate::ExpressionContains {
                    expression: left,
                    value: self.scalar(self.tree.required(tail, rule_id!("x_scalar")), depth)?,
                })
            }
            _ => unreachable!("predicate atom"),
        }
    }
    fn predicate_reference(&self, node: NodeId) -> ScalarExpression {
        let (variable, property) = self.identifier_pair(node);
        self.source(node, ScalarExpressionKind::Property { variable, property })
    }
    fn where_comparisons(
        &mut self,
        mut left: ScalarExpression,
        node: NodeId,
        depth: usize,
        columns: bool,
    ) -> Result<PropertyPredicate> {
        let op = self.comparison_operator(self.tree.required(node, rule_id!("x_comparison_op")));
        let mut right = if let Some(rhs) = self.tree.child(node, rule_id!("w_right")) {
            if let Some(reference) = self.tree.child(rhs, rule_id!("w_reference")) {
                self.predicate_reference(reference)
            } else {
                let result =
                    self.case_scalar(self.tree.required(rhs, rule_id!("x_case_scalar")), depth)?;
                if !columns && !matches!(result.kind, ScalarExpressionKind::Value(_)) {
                    return Err(self.error(rhs, "expected value"));
                }
                result
            }
        } else {
            self.case_scalar(self.tree.required(node, rule_id!("x_case_scalar")), depth)?
        };
        let mut op = op;
        let following = self.named_children(node, rule_id!("w_following"));
        if following.is_empty() {
            return Ok(comparison_predicate(left, op, right));
        }
        let mut links = Vec::with_capacity(following.len() + 1);
        for following in following {
            links.push(comparison_predicate(left, op, right.clone()));
            let next = self.case_scalar(
                self.tree.required(following, rule_id!("x_case_scalar")),
                depth,
            )?;
            left = right;
            right = next;
            op = self
                .comparison_operator(self.tree.required(following, rule_id!("x_comparison_op")));
        }
        links.push(comparison_predicate(left, op, right));
        Ok(if links.len() == 1 {
            links.remove(0)
        } else {
            PropertyPredicate::And(links)
        })
    }
    fn in_predicate(
        &mut self,
        atom: NodeId,
        left: ScalarExpression,
        node: NodeId,
        depth: usize,
        id: Option<String>,
    ) -> Result<PropertyPredicate> {
        let right = self.case_scalar(self.tree.required(node, rule_id!("x_case_scalar")), depth)?;
        let start = self
            .tree
            .node(self.tree.required(node, rule_id!("x_case_scalar")))
            .start;
        let literal_values = matches!(&right.kind,
            ScalarExpressionKind::Value(values)
                if matches!(values.kind,
                    ValueExpressionKind::Parameter(_) | ValueExpressionKind::List(_))
                    && !values.kind.reads_row_binding());
        if matches!(self.tokens[start].kind, T::Parameter | T::LeftBracket)
            && literal_values
            && let ScalarExpressionKind::Value(values) = right.kind
        {
            return Ok(if let Some(variable) = id {
                PropertyPredicate::IdIn { variable, values }
            } else if let ScalarExpressionKind::Property { variable, property } = left.kind {
                PropertyPredicate::In {
                    variable,
                    property,
                    values,
                }
            } else {
                PropertyPredicate::ExpressionIn {
                    expression: left,
                    values,
                }
            });
        }
        let span = SourceSpan {
            start: left
                .span
                .map_or(self.tree.span(atom, self.tokens).start, |s| s.start),
            end: self.tree.span(atom, self.tokens).end,
        };
        Ok(PropertyPredicate::ExpressionEq {
            expression: AstNode::from_source(
                ScalarExpressionKind::Binary {
                    left: Box::new(left),
                    op: ScalarBinaryOp::In,
                    right: Box::new(right),
                },
                span,
            ),
            value: true_scalar(),
        })
    }
    fn id_predicate(&mut self, node: NodeId, depth: usize) -> Result<PropertyPredicate> {
        let call = self.tree.required(node, rule_id!("w_id_call"));
        let variable = self
            .text(self.tree.required(call, rule_id!("v_identifier")))
            .to_owned();
        if let Some(in_node) = self.tree.child(node, rule_id!("w_in")) {
            let expression = self.source(call, ScalarExpressionKind::Id(variable.clone()));
            return self.in_predicate(node, expression, in_node, depth, Some(variable));
        }
        let value = self.value(self.tree.required(node, rule_id!("v_value")), depth)?;
        let op = self
            .text(self.tree.required(node, rule_id!("w_id_op")))
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();
        Ok(match op.as_str() {
            "=" => PropertyPredicate::IdEq { variable, value },
            "<>" => PropertyPredicate::IdNotEq { variable, value },
            _ => PropertyPredicate::IdCompare {
                variable,
                op: match op.as_str() {
                    "<" => ComparisonOp::Lt,
                    "<=" => ComparisonOp::Lte,
                    ">" => ComparisonOp::Gt,
                    _ => ComparisonOp::Gte,
                },
                value,
            },
        })
    }
    fn existence(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<PropertyPredicate> {
        let wrapper = node;
        let pattern = self.pattern(wrapper, depth, anonymous)?;
        let [step] = pattern.steps.as_slice() else {
            return Err(self.error(
                node,
                "relationship existence predicates support one-hop types",
            ));
        };
        let chain = self.tree.required(wrapper, rule_id!("p_chain"));
        let step_node = self.tree.child(chain, rule_id!("p_step")).expect("step");
        let edge = self.tree.required(step_node, rule_id!("p_relationship"));
        let edge = self.tree.children(edge)[0];
        let body = self.tree.required(edge, rule_id!("p_rel_body"));
        if !step.relationship.rel_type_disjunction.is_empty() {
            return Err(self.error(
                node,
                "relationship type disjunction is not supported in relationship existence predicates",
            ));
        }
        if self.tokens[self.tree.node(body).start + 1].kind == T::Star
            || step.relationship.search != PathSearch::All
            || step.relationship.min_hops != 1
            || step.relationship.max_hops != 1
            || !step.relationship.properties.is_empty()
        {
            return Err(self.error(
                node,
                "relationship existence predicates support one-hop types",
            ));
        }
        if self.tree.rule_id(node) == rule_id!("w_bound") {
            if !pattern.first.label.is_empty() || !pattern.first.properties.is_empty() {
                return Err(self.error(
                    node,
                    "EXISTS relationship subqueries require bound source variables",
                ));
            }
            if !step.target.label.is_empty() || !step.target.properties.is_empty() {
                return Err(self.error(
                    node,
                    "EXISTS relationship subqueries require bound target variables",
                ));
            }
            Ok(PropertyPredicate::BoundRelationshipExists {
                source_variable: pattern.first.variable.clone(),
                rel_type: step.relationship.rel_type.clone(),
                direction: step.relationship.direction,
                target_variable: step.target.variable.clone(),
            })
        } else {
            if !step.target.properties.is_empty() {
                return Err(self.error(
                    node,
                    "relationship existence predicates do not support target properties",
                ));
            }
            Ok(PropertyPredicate::RelationshipExists {
                variable: pattern.first.variable.clone(),
                rel_type: step.relationship.rel_type.clone(),
                direction: step.relationship.direction,
                target_label: step.target.label.clone(),
            })
        }
    }
}
fn true_scalar() -> ScalarExpression {
    AstNode::synthetic(ScalarExpressionKind::Value(AstNode::synthetic(
        ValueExpressionKind::Literal(Value::Bool(true)),
    )))
}
fn comparison_predicate(
    left: ScalarExpression,
    op: ScalarBinaryOp,
    right: ScalarExpression,
) -> PropertyPredicate {
    let (left, op, right) = if matches!(left.kind, ScalarExpressionKind::Value(_))
        && matches!(right.kind, ScalarExpressionKind::Property { .. })
    {
        (
            right,
            match op {
                ScalarBinaryOp::Lt => ScalarBinaryOp::Gt,
                ScalarBinaryOp::Lte => ScalarBinaryOp::Gte,
                ScalarBinaryOp::Gt => ScalarBinaryOp::Lt,
                ScalarBinaryOp::Gte => ScalarBinaryOp::Lte,
                _ => op,
            },
            left,
        )
    } else {
        (left, op, right)
    };
    match (left.kind, right.kind) {
        (
            ScalarExpressionKind::Property { variable, property },
            ScalarExpressionKind::Value(value),
        ) => match op {
            ScalarBinaryOp::Eq => PropertyPredicate::Eq {
                variable,
                property,
                value,
            },
            ScalarBinaryOp::NotEq => PropertyPredicate::NotEq {
                variable,
                property,
                value,
            },
            _ => PropertyPredicate::Compare {
                variable,
                property,
                op: ordering(op),
                value,
            },
        },
        (
            ScalarExpressionKind::Value(ValueExpression {
                kind: ValueExpressionKind::Parameter(parameter),
                ..
            }),
            ScalarExpressionKind::Value(value),
        ) if matches!(op, ScalarBinaryOp::Eq | ScalarBinaryOp::NotEq) => {
            if op == ScalarBinaryOp::Eq {
                PropertyPredicate::ParameterEq {
                    left: parameter,
                    right: value,
                }
            } else {
                PropertyPredicate::ParameterNotEq {
                    left: parameter,
                    right: value,
                }
            }
        }
        (left_kind, right_kind) => {
            let expression = AstNode {
                kind: left_kind,
                span: left.span,
            };
            let value = AstNode {
                kind: right_kind,
                span: right.span,
            };
            match op {
                ScalarBinaryOp::Eq => PropertyPredicate::ExpressionEq { expression, value },
                ScalarBinaryOp::NotEq => PropertyPredicate::ExpressionNotEq { expression, value },
                _ => PropertyPredicate::ExpressionCompare {
                    expression,
                    op: ordering(op),
                    value,
                },
            }
        }
    }
}
fn ordering(op: ScalarBinaryOp) -> ComparisonOp {
    match op {
        ScalarBinaryOp::Lt => ComparisonOp::Lt,
        ScalarBinaryOp::Lte => ComparisonOp::Lte,
        ScalarBinaryOp::Gt => ComparisonOp::Gt,
        ScalarBinaryOp::Gte => ComparisonOp::Gte,
        _ => unreachable!("ordering comparison"),
    }
}
