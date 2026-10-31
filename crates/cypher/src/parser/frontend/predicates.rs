// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! WHERE recognition and index-compatible predicate AST construction.
use super::{values::*, Transformer};
use crate::*;
use hawdb_core::{Result, Value};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;
fn kw(names: &[&'static str]) -> E {
    c(names.iter().copied().map(k).collect())
}
fn tight(items: Vec<E>) -> E {
    E::Tight(Box::new(s(items)))
}
fn not_reserved() -> E {
    E::Not(
        Box::new(kw(&["NOT", "EXISTS", "CASE", "TRUE", "FALSE", "NULL"])),
        "a predicate binding",
    )
}
pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        ("w_input", s(vec![r("w_or"), t(T::End)])),
        ("p_where", s(vec![k("WHERE"), r("w_or")])),
        (
            "w_or",
            s(vec![r("w_and"), many(s(vec![k("OR"), r("w_and")]))]),
        ),
        (
            "w_and",
            s(vec![r("w_atom"), many(s(vec![k("AND"), r("w_atom")]))]),
        ),
        (
            "w_atom",
            c(vec![
                r("w_not"),
                r("w_bound"),
                r("w_graph"),
                r("w_parenthesized_expression"),
                r("w_group"),
                r("w_list_contains"),
                r("w_contains_fn"),
                r("w_parameter"),
                r("w_id"),
                r("w_scalar"),
                r("w_property"),
                r("w_word"),
            ]),
        ),
        ("w_not", s(vec![k("NOT"), r("w_atom")])),
        ("w_group", group(r("w_or"))),
        (
            "w_graph",
            s(vec![
                E::Ahead(
                    Box::new(s(vec![
                        r("p_node"),
                        c(vec![
                            tight(vec![t(T::Minus), t(T::LeftBracket)]),
                            tight(vec![t(T::Less), t(T::Minus), t(T::LeftBracket)]),
                        ]),
                    ])),
                    "a relationship existence pattern",
                ),
                r("w_pattern"),
            ]),
        ),
        ("w_pattern", r("p_chain")),
        (
            "w_bound",
            s(vec![
                k("EXISTS"),
                t(T::LeftBrace),
                k("MATCH"),
                r("w_pattern"),
                t(T::RightBrace),
            ]),
        ),
        (
            "w_list_contains",
            s(vec![
                E::Ahead(
                    Box::new(tight(vec![
                        kw(&["LIST_CONTAINS", "LIST_CONTAINS_LOWER"]),
                        t(T::LeftParen),
                    ])),
                    "a list containment predicate",
                ),
                r("v_identifier"),
                group(s(vec![r("w_reference"), t(T::Comma), r("v_value")])),
            ]),
        ),
        (
            "w_contains_fn",
            s(vec![
                E::Ahead(
                    Box::new(tight(vec![k("CONTAINS"), t(T::LeftParen)])),
                    "a containment predicate",
                ),
                k("CONTAINS"),
                group(s(vec![r("x_scalar"), t(T::Comma), r("x_scalar")])),
            ]),
        ),
        (
            "w_reference",
            s(vec![r("v_identifier"), t(T::Dot), r("v_identifier")]),
        ),
        (
            "w_parameter",
            s(vec![
                r("v_parameter"),
                c(vec![r("x_is_null"), r("w_in"), r("w_comparisons")]),
            ]),
        ),
        ("w_id_call", s(vec![k("ID"), group(r("v_identifier"))])),
        (
            "w_id",
            s(vec![
                r("w_id_call"),
                c(vec![r("w_in"), s(vec![r("w_id_op"), r("v_value")])]),
            ]),
        ),
        (
            "w_id_op",
            c(vec![
                s(vec![t(T::Less), t(T::Greater)]),
                s(vec![t(T::Less), t(T::Equal)]),
                s(vec![t(T::Greater), t(T::Equal)]),
                t(T::Less),
                t(T::Greater),
                t(T::Equal),
            ]),
        ),
        (
            "w_property",
            s(vec![
                not_reserved(),
                r("w_reference"),
                c(vec![
                    r("x_is_null"),
                    r("w_in"),
                    r("w_string"),
                    r("w_comparisons"),
                ]),
            ]),
        ),
        ("w_string", s(vec![r("w_string_op"), r("v_value")])),
        (
            "w_string_op",
            c(vec![
                k("CONTAINS"),
                s(vec![k("STARTS"), k("WITH")]),
                s(vec![k("ENDS"), k("WITH")]),
                tight(vec![t(T::Equal), t(T::Tilde)]),
            ]),
        ),
        (
            "w_word",
            s(vec![
                not_reserved(),
                r("v_identifier"),
                r("w_expression_tail"),
            ]),
        ),
        (
            "w_scalar",
            s(vec![
                c(vec![
                    s(vec![
                        E::Ahead(
                            Box::new(c(vec![
                                t(T::String),
                                t(T::Number),
                                t(T::Minus),
                                kw(&["TRUE", "FALSE", "NULL"]),
                            ])),
                            "a literal operand",
                        ),
                        r("x_case_scalar"),
                    ]),
                    s(vec![
                        E::Ahead(
                            Box::new(c(vec![
                                k("CASE"),
                                tight(vec![
                                    kw(&[
                                        "COALESCE",
                                        "LEFT",
                                        "LOWER",
                                        "JSON_VALUE",
                                        "LENGTH",
                                        "NODES",
                                        "RELATIONSHIPS",
                                        "SIZE",
                                        "HEAD",
                                        "LAST",
                                        "TAIL",
                                        "REVERSE",
                                        "RANGE",
                                    ]),
                                    t(T::LeftParen),
                                ]),
                                s(vec![r("w_reference"), t(T::LeftBracket)]),
                                s(vec![r("v_identifier"), t(T::LeftBracket)]),
                            ])),
                            "a scalar operand",
                        ),
                        r("x_scalar"),
                    ]),
                ]),
                r("w_expression_tail"),
            ]),
        ),
        (
            "w_parenthesized_expression",
            s(vec![
                t(T::LeftParen),
                E::Ahead(
                    Box::new(kw(&[
                        "CASE", "COALESCE", "LEFT", "LOWER", "SIZE", "HEAD", "LAST", "TAIL",
                        "REVERSE", "RANGE",
                    ])),
                    "a parenthesized scalar",
                ),
                r("x_scalar"),
                t(T::RightParen),
                E::Ahead(
                    Box::new(c(vec![
                        t(T::Equal),
                        t(T::Less),
                        t(T::Greater),
                        k("CONTAINS"),
                        k("IN"),
                    ])),
                    "a comparison",
                ),
                r("w_expression_tail"),
            ]),
        ),
        (
            "w_expression_tail",
            c(vec![
                r("x_is_null"),
                r("w_in"),
                s(vec![k("CONTAINS"), r("x_scalar")]),
                r("w_expression_comparisons"),
            ]),
        ),
        (
            "w_comparisons",
            s(vec![
                r("x_comparison_op"),
                r("w_right"),
                many(r("w_following")),
            ]),
        ),
        (
            "w_expression_comparisons",
            s(vec![
                r("x_comparison_op"),
                r("x_scalar"),
                many(r("w_following")),
            ]),
        ),
        (
            "w_following",
            s(vec![r("x_comparison_op"), r("x_case_scalar")]),
        ),
        ("w_right", c(vec![r("w_reference"), r("x_case_scalar")])),
        ("w_in", s(vec![k("IN"), r("x_case_scalar")])),
    ]
}

impl Transformer<'_> {
    pub(super) fn fold_where(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<Option<PredicateExpression>> {
        if let Some(where_node) = self.tree.child(node, "p_where") {
            let predicate = self.tree.required(where_node, "w_or");
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
            if self.tree.nodes[child].name != Some("w_and") {
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
            if self.tree.nodes[child].name != Some("w_atom") {
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
        match self.tree.nodes[selected].name.expect("predicate rule") {
            "w_not" => Ok(PropertyPredicate::Not(Box::new(self.where_atom(
                self.tree.required(selected, "w_atom"),
                depth,
                anonymous,
                columns,
            )?))),
            "w_group" => self.where_or(
                self.tree.required(selected, "w_or"),
                depth,
                anonymous,
                columns,
            ),
            "w_graph" | "w_bound" => self.existence(selected, depth, anonymous),
            "w_list_contains" => {
                let (variable, property) =
                    self.identifier_pair(self.tree.required(selected, "w_reference"));
                let value = self.value(self.tree.required(selected, "v_value"), depth)?;
                if self
                    .text(self.tree.required(selected, "v_identifier"))
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
                let args = self.named_children(selected, "x_scalar");
                Ok(PropertyPredicate::ExpressionContains {
                    expression: self.scalar(args[0], depth)?,
                    value: self.scalar(args[1], depth)?,
                })
            }
            "w_id" => self.id_predicate(selected, depth),
            "w_property" | "w_parameter" => {
                let left = if let Some(reference) = self.tree.child(selected, "w_reference") {
                    self.predicate_reference(reference)
                } else {
                    let param = self.tree.required(selected, "v_parameter");
                    let value = self.source(
                        param,
                        ValueExpressionKind::Parameter(
                            self.text(self.tree.required(param, "v_identifier"))
                                .to_owned(),
                        ),
                    );
                    self.source(param, ScalarExpressionKind::Value(value))
                };
                if let Some(null) = self.tree.child(selected, "x_is_null") {
                    let negated = self.tree.child(null, "x_negated").is_some();
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
                if let Some(in_node) = self.tree.child(selected, "w_in") {
                    return self.in_predicate(selected, left, in_node, depth, None);
                }
                if let Some(string) = self.tree.child(selected, "w_string") {
                    let ScalarExpressionKind::Property { variable, property } = left.kind else {
                        unreachable!()
                    };
                    let value = self.value(self.tree.required(string, "v_value"), depth)?;
                    let op = self.tree.required(string, "w_string_op");
                    let first = self.tokens[self.tree.nodes[op].start];
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
                    self.tree.required(selected, "w_comparisons"),
                    depth,
                    columns,
                )
            }
            "w_word" | "w_scalar" | "w_parenthesized_expression" => {
                let left = if let Some(scalar) = self.tree.child(selected, "x_scalar") {
                    let start = self.tree.nodes[scalar].start;
                    if !columns
                        && self
                            .tokens
                            .get(start + 1)
                            .is_some_and(|t| t.kind == T::LeftBracket)
                    {
                        return Err(self.error(selected, "expected '.'"));
                    }
                    self.scalar(scalar, depth)?
                } else if let Some(scalar) = self.tree.child(selected, "x_case_scalar") {
                    self.case_scalar(scalar, depth)?
                } else {
                    if !columns {
                        return Err(self.error(selected, "expected '.'"));
                    }
                    self.source(
                        self.tree.required(selected, "v_identifier"),
                        ScalarExpressionKind::Variable(
                            self.text(self.tree.required(selected, "v_identifier"))
                                .to_owned(),
                        ),
                    )
                };
                let tail = self.tree.required(selected, "w_expression_tail");
                if let Some(null) = self.tree.child(tail, "x_is_null") {
                    let span = left.span;
                    let expression = AstNode {
                        kind: ScalarExpressionKind::IsNull {
                            expression: Box::new(left),
                            negated: self.tree.child(null, "x_negated").is_some(),
                        },
                        span,
                    };
                    return Ok(PropertyPredicate::ExpressionEq {
                        expression,
                        value: true_scalar(),
                    });
                }
                if let Some(in_node) = self.tree.child(tail, "w_in") {
                    return self.in_predicate(selected, left, in_node, depth, None);
                }
                if let Some(comparisons) = self.tree.child(tail, "w_expression_comparisons") {
                    return self.where_comparisons(left, comparisons, depth, true);
                }
                Ok(PropertyPredicate::ExpressionContains {
                    expression: left,
                    value: self.scalar(self.tree.required(tail, "x_scalar"), depth)?,
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
        let op = self.comparison_operator(self.tree.required(node, "x_comparison_op"));
        let mut right = if let Some(rhs) = self.tree.child(node, "w_right") {
            if let Some(reference) = self.tree.child(rhs, "w_reference") {
                self.predicate_reference(reference)
            } else {
                let result = self.case_scalar(self.tree.required(rhs, "x_case_scalar"), depth)?;
                if !columns && !matches!(result.kind, ScalarExpressionKind::Value(_)) {
                    return Err(self.error(rhs, "expected value"));
                }
                result
            }
        } else {
            self.scalar(self.tree.required(node, "x_scalar"), depth)?
        };
        let mut op = op;
        let following = self.named_children(node, "w_following");
        if following.is_empty() {
            return Ok(comparison_predicate(left, op, right));
        }
        let mut links = Vec::with_capacity(following.len() + 1);
        for following in following {
            links.push(comparison_predicate(left, op, right.clone()));
            let next = self.case_scalar(self.tree.required(following, "x_case_scalar"), depth)?;
            left = right;
            right = next;
            op = self.comparison_operator(self.tree.required(following, "x_comparison_op"));
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
        let right = self.case_scalar(self.tree.required(node, "x_case_scalar"), depth)?;
        let start = self.tree.nodes[self.tree.required(node, "x_case_scalar")].start;
        if matches!(self.tokens[start].kind, T::Parameter(_) | T::LeftBracket)
            && let ScalarExpressionKind::Value(values) = &right.kind
            && matches!(
                values.kind,
                ValueExpressionKind::Parameter(_) | ValueExpressionKind::List(_)
            )
            && !values.kind.reads_row_binding()
        {
            let values = values.clone();
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
        let call = self.tree.required(node, "w_id_call");
        let variable = self
            .text(self.tree.required(call, "v_identifier"))
            .to_owned();
        if let Some(in_node) = self.tree.child(node, "w_in") {
            let expression = self.source(call, ScalarExpressionKind::Id(variable.clone()));
            return self.in_predicate(node, expression, in_node, depth, Some(variable));
        }
        let value = self.value(self.tree.required(node, "v_value"), depth)?;
        let op = self
            .text(self.tree.required(node, "w_id_op"))
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
        let wrapper = self.tree.required(node, "w_pattern");
        let pattern = self.pattern(wrapper, depth, anonymous)?;
        let [step] = pattern.steps.as_slice() else {
            return Err(self.error(
                node,
                "relationship existence predicates support one-hop types",
            ));
        };
        let chain = self.tree.required(wrapper, "p_chain");
        let step_node = self.tree.child(chain, "p_step").expect("step");
        let edge = self.tree.required(step_node, "p_relationship");
        let edge = self.tree.children(edge)[0];
        let body = self.tree.required(edge, "p_rel_body");
        if self.tokens[self.tree.nodes[body].start + 1].kind == T::Star
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
        if self.tree.nodes[node].name == Some("w_bound") {
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
