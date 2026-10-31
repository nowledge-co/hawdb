// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Scalar and CASE token PEG. Recognition is separate from AST specialization.
use super::{values::*, Transformer};
use crate::*;
use hawdb_core::{ListFunction, Result, Value, VectorDistance};
use hawdb_sql_syntax::peg::{Expression as E, NodeId};
use hawdb_sql_syntax::TokenKind as T;

fn kw(names: &[&'static str]) -> E {
    c(names.iter().copied().map(k).collect())
}
fn tight(items: Vec<E>) -> E {
    E::Tight(Box::new(s(items)))
}
fn special_shapes(reference_rule: &'static str, lower_keyword: Option<&'static str>) -> E {
    let reference = || r(reference_rule);
    let value = || r("v_value");
    let is_not_null = || s(vec![reference(), k("IS"), k("NOT"), k("NULL")]);
    let lower = s(vec![
        k("WHEN"),
        is_not_null(),
        k("THEN"),
        lower_keyword.map(r).unwrap_or_else(|| k("LOWER")),
        group(reference()),
        k("ELSE"),
        value(),
        k("END"),
    ]);
    let default = s(vec![
        k("WHEN"),
        reference(),
        k("IS"),
        k("NULL"),
        k("OR"),
        reference(),
        t(T::Equal),
        value(),
        k("THEN"),
        value(),
        k("ELSE"),
        reference(),
        k("END"),
    ]);
    let rank_branch = || {
        s(vec![
            k("WHEN"),
            reference(),
            t(T::Equal),
            value(),
            k("THEN"),
            value(),
        ])
    };
    let rank = s(vec![
        rank_branch(),
        many(rank_branch()),
        k("ELSE"),
        value(),
        k("END"),
    ]);
    let not_null = s(vec![
        k("WHEN"),
        is_not_null(),
        k("THEN"),
        reference(),
        k("ELSE"),
        value(),
        k("END"),
    ]);
    let not_empty = s(vec![
        k("WHEN"),
        is_not_null(),
        k("AND"),
        reference(),
        c(vec![
            tight(vec![t(T::Less), t(T::Greater)]),
            tight(vec![t(T::NotEqual), t(T::Equal)]),
        ]),
        value(),
        k("THEN"),
        value(),
        k("ELSE"),
        value(),
        k("END"),
    ]);
    c(vec![lower, default, rank, not_null, not_empty])
}

pub(super) fn rules() -> Vec<(&'static str, E)> {
    vec![
        ("x_scalar", s(vec![r("x_primary"), many(r("x_postfix"))])),
        (
            "x_primary",
            c(vec![
                r("x_list"),
                r("x_value"),
                r("x_identity"),
                r("x_list_function"),
                r("x_metric"),
                r("x_coalesce"),
                r("x_json"),
                r("x_left"),
                r("x_path_properties"),
                r("x_path"),
                r("x_lower"),
                r("x_date"),
                r("x_case"),
                r("x_ref"),
                r("x_variable"),
            ]),
        ),
        (
            "x_value",
            c(vec![
                r("v_parameter"),
                r("v_string"),
                r("v_number"),
                r("v_boolean"),
                r("v_null"),
            ]),
        ),
        (
            "x_ref",
            s(vec![
                E::Not(Box::new(k("CASE")), "a property binding"),
                r("v_identifier"),
                t(T::Dot),
                r("v_identifier"),
            ]),
        ),
        (
            "x_variable",
            s(vec![
                E::Not(Box::new(k("CASE")), "a variable"),
                r("v_identifier"),
            ]),
        ),
        (
            "x_identity",
            s(vec![r("x_identity_name"), group(r("v_identifier"))]),
        ),
        ("x_identity_name", kw(&["ID", "LABEL", "TYPE"])),
        (
            "x_list_function",
            s(vec![
                r("x_list_function_name"),
                group(r("x_list_arguments")),
            ]),
        ),
        (
            "x_list_function_name",
            kw(&["SIZE", "HEAD", "LAST", "TAIL", "REVERSE", "RANGE"]),
        ),
        ("x_list_arguments", o(list("x_case_scalar"))),
        (
            "x_metric",
            s(vec![
                r("x_metric_name"),
                group(s(vec![r("x_scalar"), t(T::Comma), r("x_scalar")])),
            ]),
        ),
        ("x_metric_name", kw(&["COSINE_DISTANCE", "L2_DISTANCE"])),
        (
            "x_coalesce",
            s(vec![k("COALESCE"), group(r("x_scalar_arguments"))]),
        ),
        ("x_scalar_arguments", list("x_scalar")),
        (
            "x_json",
            s(vec![
                k("JSON_VALUE"),
                group(s(vec![r("x_scalar"), t(T::Comma), r("v_value")])),
            ]),
        ),
        (
            "x_left",
            s(vec![
                k("LEFT"),
                group(s(vec![r("x_scalar"), t(T::Comma), r("v_value")])),
            ]),
        ),
        ("x_lower", s(vec![k("LOWER"), group(r("x_scalar"))])),
        (
            "x_path_properties",
            s(vec![
                k("PROPERTIES"),
                group(s(vec![
                    k("NODES"),
                    group(r("v_identifier")),
                    t(T::Comma),
                    r("v_value"),
                ])),
            ]),
        ),
        (
            "x_path",
            s(vec![r("x_path_name"), group(r("v_identifier"))]),
        ),
        ("x_path_name", kw(&["LENGTH", "NODES", "RELATIONSHIPS"])),
        (
            "x_date",
            s(vec![
                k("DATE_PART"),
                group(s(vec![r("v_value"), t(T::Comma), r("x_ref")])),
            ]),
        ),
        (
            "x_list",
            c(vec![
                r("x_comprehension"),
                r("x_value_list"),
                r("x_expression_list"),
            ]),
        ),
        ("x_value_list", r("v_list")),
        (
            "x_expression_list",
            s(vec![
                t(T::LeftBracket),
                r("x_expression_list_contents"),
                t(T::RightBracket),
            ]),
        ),
        (
            "x_expression_list_contents",
            o(s(vec![list("x_or"), o(t(T::Comma))])),
        ),
        (
            "x_comprehension",
            s(vec![
                t(T::LeftBracket),
                E::Not(
                    Box::new(kw(&["TRUE", "FALSE", "NULL"])),
                    "a comprehension variable",
                ),
                r("v_identifier"),
                k("IN"),
                r("x_comprehension_contents"),
                t(T::RightBracket),
            ]),
        ),
        (
            "x_comprehension_contents",
            s(vec![
                r("x_case_scalar"),
                o(r("x_comprehension_where")),
                o(r("x_comprehension_projection")),
            ]),
        ),
        ("x_comprehension_where", s(vec![k("WHERE"), r("x_or")])),
        (
            "x_comprehension_projection",
            s(vec![t(T::Pipe), r("x_case_scalar")]),
        ),
        (
            "x_postfix",
            s(vec![
                t(T::LeftBracket),
                r("x_postfix_contents"),
                t(T::RightBracket),
            ]),
        ),
        (
            "x_postfix_contents",
            c(vec![
                s(vec![r("x_dots"), o(r("x_slice_end"))]),
                s(vec![r("x_case_scalar"), o(r("x_slice_tail"))]),
            ]),
        ),
        ("x_dots", tight(vec![t(T::Dot), t(T::Dot)])),
        ("x_slice_tail", s(vec![r("x_dots"), o(r("x_slice_end"))])),
        ("x_slice_end", r("x_case_scalar")),
        (
            "x_case_scalar",
            c(vec![r("x_timestamp_value"), r("x_scalar")]),
        ),
        (
            "x_timestamp_value",
            s(vec![
                E::Ahead(
                    Box::new(s(vec![
                        kw(&["CURRENT_TIMESTAMP", "TIMESTAMP", "CAST"]),
                        t(T::LeftParen),
                    ])),
                    "a timestamp value",
                ),
                r("v_value"),
            ]),
        ),
        ("x_or", s(vec![r("x_and"), many(r("x_or_rest"))])),
        ("x_or_rest", s(vec![k("OR"), r("x_and")])),
        ("x_and", s(vec![r("x_condition"), many(r("x_and_rest"))])),
        ("x_and_rest", s(vec![k("AND"), r("x_condition")])),
        (
            "x_condition",
            c(vec![
                r("x_not"),
                s(vec![r("x_condition_base"), o(r("x_condition_tail"))]),
            ]),
        ),
        ("x_not", s(vec![k("NOT"), r("x_not_contents")])),
        ("x_not_contents", r("x_condition")),
        (
            "x_condition_base",
            c(vec![
                r("x_condition_paren"),
                r("x_list_contains"),
                s(vec![
                    E::Not(Box::new(kw(&["NOT", "LIST_CONTAINS"])), "a condition"),
                    r("x_case_scalar"),
                ]),
            ]),
        ),
        ("x_condition_paren", group(r("x_or"))),
        (
            "x_list_contains",
            s(vec![
                k("LIST_CONTAINS"),
                group(s(vec![r("x_case_scalar"), t(T::Comma), r("x_case_scalar")])),
            ]),
        ),
        (
            "x_condition_tail",
            c(vec![r("x_is_null"), r("x_comparisons"), r("x_contains")]),
        ),
        ("x_is_null", s(vec![k("IS"), o(r("x_negated")), k("NULL")])),
        ("x_negated", k("NOT")),
        (
            "x_comparisons",
            s(vec![r("x_comparison"), many(r("x_comparison"))]),
        ),
        (
            "x_comparison",
            s(vec![r("x_comparison_op"), r("x_case_scalar")]),
        ),
        (
            "x_comparison_op",
            c(vec![
                tight(vec![t(T::Less), t(T::Greater)]),
                tight(vec![t(T::NotEqual), t(T::Equal)]),
                tight(vec![t(T::Less), t(T::Equal)]),
                tight(vec![t(T::Greater), t(T::Equal)]),
                t(T::Equal),
                t(T::Less),
                t(T::Greater),
            ]),
        ),
        (
            "x_contains",
            s(vec![r("x_contains_op"), r("x_case_scalar")]),
        ),
        ("x_contains_op", kw(&["CONTAINS", "IN"])),
        (
            "x_case",
            s(vec![
                k("CASE"),
                c(vec![
                    r("x_floor_case"),
                    r("x_reserved_special_case"),
                    r("x_special_case"),
                    r("x_case_body"),
                ]),
            ]),
        ),
        (
            "x_reserved_special_case",
            s(vec![
                E::Ahead(
                    Box::new(s(vec![
                        k("WHEN"),
                        kw(&["true", "false", "null", "CASE"]),
                        t(T::Dot),
                    ])),
                    "a specialized property reference",
                ),
                special_shapes("d_ref", Some("x_special_lower")),
            ]),
        ),
        ("x_special_lower", k("LOWER")),
        (
            "x_special_case",
            s(vec![
                E::Ahead(
                    Box::new(special_shapes("x_ref", None)),
                    "a specialized CASE shape",
                ),
                r("x_case_body"),
            ]),
        ),
        (
            "x_case_body",
            c(vec![r("x_searched_case"), r("x_simple_case")]),
        ),
        (
            "x_searched_case",
            s(vec![
                r("x_searched_branch"),
                many(r("x_searched_branch")),
                o(r("x_else")),
                k("END"),
            ]),
        ),
        (
            "x_searched_branch",
            s(vec![k("WHEN"), r("x_or"), k("THEN"), r("x_case_scalar")]),
        ),
        (
            "x_simple_case",
            s(vec![
                E::Not(Box::new(k("WHEN")), "a CASE operand"),
                r("x_or"),
                r("x_simple_branch"),
                many(r("x_simple_branch")),
                o(r("x_else")),
                k("END"),
            ]),
        ),
        (
            "x_simple_branch",
            s(vec![
                k("WHEN"),
                r("x_case_scalar"),
                k("THEN"),
                r("x_case_scalar"),
            ]),
        ),
        ("x_else", s(vec![k("ELSE"), r("x_case_scalar")])),
        (
            "x_floor_case",
            s(vec![
                k("WHEN"),
                r("x_difference"),
                t(T::Less),
                r("v_value"),
                k("THEN"),
                r("v_value"),
                k("ELSE"),
                r("x_difference"),
                k("END"),
            ]),
        ),
        (
            "x_difference",
            s(vec![
                r("x_difference_term"),
                t(T::Minus),
                r("x_difference_term"),
                many(s(vec![t(T::Minus), r("x_difference_term")])),
            ]),
        ),
        (
            "x_difference_term",
            s(vec![
                k("COALESCE"),
                group(s(vec![r("d_ref"), t(T::Comma), r("v_value")])),
            ]),
        ),
    ]
}

impl Transformer<'_> {
    pub(super) fn expression_budget(&mut self, node: NodeId, depth: usize) -> Result<()> {
        self.check_depth(node, 0, depth)
    }
    pub(super) fn scalar(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let primary = self.tree.required(node, "x_primary");
        let selected = self.tree.children(primary)[0];
        let kind = self.scalar_primary(selected, depth + 1)?;
        let expression = self.source(primary, kind);
        self.scalar_postfixes(node, expression, depth)
    }
    pub(super) fn scalar_postfixes(
        &mut self,
        node: NodeId,
        mut expression: ScalarExpression,
        depth: usize,
    ) -> Result<ScalarExpression> {
        for (index, postfix) in self
            .named_children(node, "x_postfix")
            .into_iter()
            .enumerate()
        {
            self.expression_budget(postfix, depth + index + 1)?;
            let contents = self.tree.required(postfix, "x_postfix_contents");
            let first = self
                .tree
                .child(contents, "x_case_scalar")
                .map(|id| self.case_scalar(id, depth + index + 1).map(Box::new))
                .transpose()?;
            let tail = self.tree.child(contents, "x_slice_tail");
            let slice = tail.is_some() || self.tree.child(contents, "x_dots").is_some();
            let kind = if slice {
                let end = self
                    .tree
                    .child(tail.unwrap_or(contents), "x_slice_end")
                    .map(|id| {
                        self.case_scalar(self.tree.required(id, "x_case_scalar"), depth + index + 1)
                            .map(Box::new)
                    })
                    .transpose()?;
                ScalarExpressionKind::ListSlice {
                    list: Box::new(expression),
                    start: first,
                    end,
                }
            } else {
                ScalarExpressionKind::ListIndex {
                    list: Box::new(expression),
                    index: first.expect("index"),
                }
            };
            let span = SourceSpan {
                start: self.tree.span(node, self.tokens).start,
                end: self.tree.span(postfix, self.tokens).end,
            };
            expression = AstNode::from_source(kind, span);
        }
        Ok(expression)
    }
    pub(super) fn case_scalar(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        if let Some(timestamp) = self.tree.child(node, "x_timestamp_value") {
            let value = self.value(self.tree.required(timestamp, "v_value"), depth)?;
            Ok(self.source(node, ScalarExpressionKind::Value(value)))
        } else {
            self.scalar(self.tree.required(node, "x_scalar"), depth)
        }
    }
    fn scalar_primary(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        Ok(match self.tree.nodes[node].name.expect("scalar atom") {
            "x_value" => ScalarExpressionKind::Value(self.value(node, depth)?),
            "x_variable" => ScalarExpressionKind::Variable(
                self.text(self.tree.required(node, "v_identifier"))
                    .to_owned(),
            ),
            "x_ref" => {
                let (variable, property) = self.identifier_pair(node);
                ScalarExpressionKind::Property { variable, property }
            }
            "x_identity" => {
                let variable = self
                    .text(self.tree.required(node, "v_identifier"))
                    .to_owned();
                if self
                    .text(self.tree.required(node, "x_identity_name"))
                    .eq_ignore_ascii_case("ID")
                {
                    ScalarExpressionKind::Id(variable)
                } else {
                    ScalarExpressionKind::RelationshipType(variable)
                }
            }
            "x_coalesce" => {
                let mut args = Vec::new();
                for child in
                    self.named_children(self.tree.required(node, "x_scalar_arguments"), "x_scalar")
                {
                    args.push(self.scalar(child, depth)?);
                }
                ScalarExpressionKind::Coalesce(args)
            }
            "x_lower" => ScalarExpressionKind::Lower(Box::new(
                self.scalar(self.tree.required(node, "x_scalar"), depth)?,
            )),
            "x_json" | "x_left" => {
                let expression =
                    Box::new(self.scalar(self.tree.required(node, "x_scalar"), depth)?);
                let value = self.value(self.tree.required(node, "v_value"), depth)?;
                if self.tree.nodes[node].name == Some("x_json") {
                    ScalarExpressionKind::JsonValue {
                        expression,
                        path: value,
                    }
                } else {
                    ScalarExpressionKind::Left {
                        expression,
                        length: value,
                    }
                }
            }
            "x_date" => {
                let value = self.value(self.tree.required(node, "v_value"), depth)?;
                let ValueExpressionKind::Literal(Value::String(part)) = value.kind else {
                    return Err(self.error(node, "date_part part requires a string literal"));
                };
                let (variable, property) = self.identifier_pair(self.tree.required(node, "x_ref"));
                ScalarExpressionKind::DatePart {
                    part,
                    variable,
                    property,
                }
            }
            "x_metric" => {
                let args = self.named_children(node, "x_scalar");
                let metric = VectorDistance::from_function_name(
                    self.text(self.tree.required(node, "x_metric_name")),
                )
                .expect("metric rule");
                ScalarExpressionKind::VectorDistance {
                    metric,
                    left: Box::new(self.scalar(args[0], depth)?),
                    right: Box::new(self.scalar(args[1], depth)?),
                }
            }
            "x_path" | "x_path_properties" => {
                ScalarExpressionKind::Path(self.scalar_path(node, depth)?)
            }
            "x_list_function" => {
                let function = ListFunction::from_function_name(
                    self.text(self.tree.required(node, "x_list_function_name")),
                )
                .expect("list function");
                let mut arguments = Vec::new();
                for arg in self
                    .tree
                    .children(self.tree.required(node, "x_list_arguments"))
                    .to_vec()
                {
                    arguments.push(self.case_scalar(arg, depth)?);
                }
                self.list_arity(node, function, arguments.len())?;
                ScalarExpressionKind::ListFunction {
                    function,
                    arguments,
                }
            }
            "x_list" => {
                let list = self.tree.children(node)[0];
                match self.tree.nodes[list].name {
                    Some("x_value_list") => {
                        let value = self.value(list, depth)?;
                        if !value.kind.reads_row_binding() {
                            ScalarExpressionKind::Value(value)
                        } else {
                            let ValueExpressionKind::List(values) = value.kind else {
                                unreachable!()
                            };
                            ScalarExpressionKind::List(
                                values
                                    .into_iter()
                                    .map(value_to_scalar)
                                    .collect::<Option<Vec<_>>>()
                                    .ok_or_else(|| {
                                        self.error(list, "expected a scalar list element")
                                    })?,
                            )
                        }
                    }
                    Some("x_expression_list") => {
                        let mut elements = Vec::new();
                        for child in self
                            .tree
                            .children(self.tree.required(list, "x_expression_list_contents"))
                            .to_vec()
                        {
                            elements.push(self.case_boolean(child, depth)?);
                        }
                        ScalarExpressionKind::List(elements)
                    }
                    Some("x_comprehension") => {
                        let variable = self
                            .text(self.tree.required(list, "v_identifier"))
                            .to_owned();
                        let contents = self.tree.required(list, "x_comprehension_contents");
                        let source = Box::new(
                            self.case_scalar(self.tree.required(contents, "x_case_scalar"), depth)?,
                        );
                        let predicate = self
                            .tree
                            .child(contents, "x_comprehension_where")
                            .map(|id| {
                                self.case_boolean(self.tree.required(id, "x_or"), depth)
                                    .map(Box::new)
                            })
                            .transpose()?;
                        let projection = self
                            .tree
                            .child(contents, "x_comprehension_projection")
                            .map(|id| {
                                self.case_scalar(self.tree.required(id, "x_case_scalar"), depth)
                                    .map(Box::new)
                            })
                            .transpose()?;
                        ScalarExpressionKind::ListComprehension {
                            variable,
                            list: source,
                            predicate,
                            projection,
                        }
                    }
                    _ => unreachable!("list rule"),
                }
            }
            "x_case" => self.case_expression(node, depth)?,
            _ => unreachable!("scalar primary"),
        })
    }
    pub(super) fn scalar_path(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ShortestPathReturnExpression> {
        let variable = self
            .text(self.tree.required(node, "v_identifier"))
            .to_owned();
        if self.tree.nodes[node].name == Some("x_path_properties") {
            let value = self.value(self.tree.required(node, "v_value"), depth)?;
            let ValueExpressionKind::Literal(Value::String(property)) = value.kind else {
                return Err(self.error(node, "node property projection requires a string key"));
            };
            Ok(ShortestPathReturnExpression::NodePropertyList {
                path_variable: variable,
                property,
            })
        } else {
            Ok(
                match self
                    .text(self.tree.required(node, "x_path_name"))
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "length" => ShortestPathReturnExpression::Length {
                        path_variable: variable,
                    },
                    "nodes" => ShortestPathReturnExpression::Nodes {
                        path_variable: variable,
                    },
                    _ => ShortestPathReturnExpression::Relationships {
                        path_variable: variable,
                    },
                },
            )
        }
    }
    pub(super) fn list_arity(
        &self,
        node: NodeId,
        function: ListFunction,
        count: usize,
    ) -> Result<()> {
        let (min, max) = function.arity();
        if (min..=max).contains(&count) {
            return Ok(());
        }
        let expected = if min == max {
            min.to_string()
        } else {
            format!("{min} or {max}")
        };
        Err(self.error(
            node,
            &format!(
                "{}() expects {expected} argument(s), got {count}",
                function.function_name()
            ),
        ))
    }
    pub(super) fn case_boolean(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        let ands = self.named_children(node, "x_and");
        let mut terms = Vec::new();
        for (index, and) in ands.into_iter().enumerate() {
            self.expression_budget(and, depth + index)?;
            terms.push(self.case_and(and, depth + index)?)
        }
        // Subsequent AND operands are captured under the corresponding OR rest.
        for rest in self.named_children(node, "x_or_rest") {
            let index = terms.len();
            self.expression_budget(rest, depth + index)?;
            terms.push(self.case_and(self.tree.required(rest, "x_and"), depth + index)?);
        }
        Ok(join_boolean(terms, ScalarBinaryOp::Or))
    }
    fn case_and(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        let mut terms = vec![self.case_condition(self.tree.required(node, "x_condition"), depth)?];
        for rest in self.named_children(node, "x_and_rest") {
            let index = terms.len();
            self.expression_budget(rest, depth + index)?;
            terms.push(self.case_condition(self.tree.required(rest, "x_condition"), depth + index)?)
        }
        Ok(join_boolean(terms, ScalarBinaryOp::And))
    }
    fn case_condition(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        if let Some(not) = self.tree.child(node, "x_not") {
            let expression = self.case_condition(
                self.tree
                    .required(self.tree.required(not, "x_not_contents"), "x_condition"),
                depth + 1,
            )?;
            return Ok(self.source(node, ScalarExpressionKind::Not(Box::new(expression))));
        }
        let base = self.tree.required(node, "x_condition_base");
        let selected = self.tree.children(base)[0];
        let left = match self.tree.nodes[selected].name {
            Some("x_condition_paren") => {
                let inner = self.case_boolean(self.tree.required(selected, "x_or"), depth + 1)?;
                self.source(selected, inner.kind)
            }
            Some("x_list_contains") => {
                let args = self.named_children(selected, "x_case_scalar");
                let left = self.case_scalar(args[0], depth + 1)?;
                let right = self.case_scalar(args[1], depth + 1)?;
                self.source(
                    selected,
                    ScalarExpressionKind::Binary {
                        left: Box::new(left),
                        op: ScalarBinaryOp::ListContains,
                        right: Box::new(right),
                    },
                )
            }
            Some("x_case_scalar") => self.case_scalar(selected, depth + 1)?,
            _ => unreachable!("condition base"),
        };
        let Some(tail) = self.tree.child(node, "x_condition_tail") else {
            return Ok(left);
        };
        let tail = self.tree.children(tail)[0];
        Ok(match self.tree.nodes[tail].name {
            Some("x_is_null") => self.source(
                node,
                ScalarExpressionKind::IsNull {
                    expression: Box::new(left),
                    negated: self.tree.child(tail, "x_negated").is_some(),
                },
            ),
            Some("x_contains") => {
                let right =
                    self.case_scalar(self.tree.required(tail, "x_case_scalar"), depth + 1)?;
                let op = if self
                    .text(self.tree.required(tail, "x_contains_op"))
                    .eq_ignore_ascii_case("IN")
                {
                    ScalarBinaryOp::In
                } else {
                    ScalarBinaryOp::Contains
                };
                self.source(
                    node,
                    ScalarExpressionKind::Binary {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                )
            }
            Some("x_comparisons") => {
                let comparisons = self.named_children(tail, "x_comparison");
                let chained = comparisons.len() > 1;
                let mut links = Vec::new();
                let mut previous = left;
                for comparison in comparisons {
                    let op =
                        self.comparison_operator(self.tree.required(comparison, "x_comparison_op"));
                    let right = self
                        .case_scalar(self.tree.required(comparison, "x_case_scalar"), depth + 1)?;
                    links.push(scalar_binary(previous, op, right.clone()));
                    previous = right;
                }
                let mut iter = links.into_iter();
                let mut expression = iter.next().expect("comparison");
                for link in iter {
                    expression = scalar_binary(expression, ScalarBinaryOp::And, link)
                }
                if chained {
                    expression
                } else {
                    self.source(node, expression.kind)
                }
            }
            _ => unreachable!("condition suffix"),
        })
    }
    pub(super) fn comparison_operator(&self, node: NodeId) -> ScalarBinaryOp {
        match self.text(node) {
            "=" => ScalarBinaryOp::Eq,
            "<>" | "!=" => ScalarBinaryOp::NotEq,
            "<" => ScalarBinaryOp::Lt,
            "<=" => ScalarBinaryOp::Lte,
            ">" => ScalarBinaryOp::Gt,
            ">=" => ScalarBinaryOp::Gte,
            _ => unreachable!("comparison token"),
        }
    }
    fn case_expression(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        let selected = self.tree.children(node)[0];
        if self.tree.nodes[selected].name == Some("x_floor_case") {
            return self.floor_case(selected, depth);
        }
        if self.tree.nodes[selected].name == Some("x_reserved_special_case") {
            return self.reserved_special_case(selected, depth);
        }
        let special = self.tree.nodes[selected].name == Some("x_special_case");
        let body = if special {
            self.tree.required(selected, "x_case_body")
        } else {
            selected
        };
        if special {
            let generic = self.generic_case(body, 0)?;
            if let Some(kind) = normalize_case(&generic) {
                self.expression_budget(node, depth + special_value_height(&kind))?;
                return Ok(kind);
            }
        }
        self.generic_case(body, depth)
    }
    fn reserved_special_case(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpressionKind> {
        let references = self.named_children(node, "d_ref");
        let names = self.ids(references[0]);
        if references
            .iter()
            .any(|reference| self.ids(*reference) != names)
        {
            return Err(self.error(node, "specialized CASE requires the same property"));
        }
        let mut values = Vec::new();
        for child in self.named_children(node, "v_value") {
            let value = self.value(child, 0)?;
            if value.kind.reads_row_binding() {
                return Err(self.error(child, "expected a constant CASE value"));
            }
            values.push(value);
        }
        let variable = names[0].clone();
        let property = names[1].clone();
        let count = values.len();
        // The matched shapes have distinct direct reference/value counts;
        // LOWER carries a named marker to distinguish its one-value form.
        let kind = if self.tree.child(node, "x_special_lower").is_some() {
            ScalarExpressionKind::CaseLowerPropertyDefault {
                variable,
                property,
                default: values.remove(0),
            }
        } else if references.len() == 3 && count == 2 {
            let default = values.pop().unwrap();
            ScalarExpressionKind::DefaultIfNullOrEq {
                variable,
                property,
                empty: values.remove(0),
                default,
            }
        } else if references.len() == 2 && count == 1 {
            ScalarExpressionKind::DefaultIfNull {
                variable,
                property,
                default: values.remove(0),
            }
        } else if references.len() == 2 && count == 3 {
            let null_or_empty = Box::new(values.pop().unwrap());
            let non_empty = Box::new(values.pop().unwrap());
            ScalarExpressionKind::CasePropertyNotNullOrEq {
                variable,
                property,
                empty: Box::new(values.remove(0)),
                non_empty,
                null_or_empty,
            }
        } else {
            let default = values.pop().unwrap();
            let mut values = values.into_iter();
            let mut branches = Vec::new();
            while let Some(condition) = values.next() {
                branches.push((condition, values.next().expect("matched rank branch")));
            }
            ScalarExpressionKind::CasePropertyEqualsRank {
                variable,
                property,
                branches,
                default,
            }
        };
        self.expression_budget(node, depth + special_value_height(&kind))?;
        Ok(kind)
    }

    fn generic_case(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        let body = self.tree.children(node)[0];
        let simple = self.tree.nodes[body].name == Some("x_simple_case");
        let operand = if simple {
            Some(Box::new(
                self.case_boolean(self.tree.required(body, "x_or"), depth)?,
            ))
        } else {
            None
        };
        let mut branches = Vec::new();
        for branch in self.named_children(
            body,
            if simple {
                "x_simple_branch"
            } else {
                "x_searched_branch"
            },
        ) {
            let (condition, result) = if simple {
                let children = self.named_children(branch, "x_case_scalar");
                (
                    self.case_scalar(children[0], depth)?,
                    self.case_scalar(children[1], depth)?,
                )
            } else {
                (
                    self.case_boolean(self.tree.required(branch, "x_or"), depth)?,
                    self.case_scalar(self.tree.required(branch, "x_case_scalar"), depth)?,
                )
            };
            branches.push((condition, result));
        }
        let otherwise = self
            .tree
            .child(body, "x_else")
            .map(|id| {
                self.case_scalar(self.tree.required(id, "x_case_scalar"), depth)
                    .map(Box::new)
            })
            .transpose()?;
        Ok(ScalarExpressionKind::Case {
            operand,
            branches,
            otherwise,
        })
    }
    fn floor_case(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        let values = self.named_children(node, "v_value");
        for id in values {
            let value = self.value(id, depth)?;
            if value.kind != ValueExpressionKind::Literal(Value::Int(0)) {
                return Err(self.error(node, "CASE floor expression requires zero bounds"));
            }
        }
        let mut differences = Vec::new();
        for difference in self.named_children(node, "x_difference") {
            let mut variable = None;
            let mut terms = Vec::new();
            for term in self.named_children(difference, "x_difference_term") {
                let names = self.ids(self.tree.required(term, "d_ref"));
                if variable.as_ref().is_some_and(|v| v != &names[0]) {
                    return Err(
                        self.error(term, "CASE floor expression supports only one variable")
                    );
                }
                variable = Some(names[0].clone());
                let default = self.value(self.tree.required(term, "v_value"), depth)?;
                if default.kind.reads_row_binding() {
                    return Err(self.error(term, "expected a constant CASE value"));
                }
                terms.push(CoalesceDifferenceTerm {
                    property: names[1].clone(),
                    default,
                })
            }
            differences.push((variable.expect("difference variable"), terms));
        }
        if differences[0] != differences[1] {
            return Err(self.error(
                node,
                "CASE floor expression ELSE must repeat the difference",
            ));
        }
        let (variable, terms) = differences.remove(0);
        Ok(ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, terms })
    }
}
fn value_to_scalar(value: ValueExpression) -> Option<ScalarExpression> {
    let span = value.span;
    let kind = match value.kind {
        ValueExpressionKind::Binding(variable) => {
            if variable.eq_ignore_ascii_case("CASE") {
                return None;
            }
            ScalarExpressionKind::Variable(variable)
        }
        ValueExpressionKind::BindingProperty { variable, property } => {
            if variable.eq_ignore_ascii_case("CASE") {
                return None;
            }
            ScalarExpressionKind::Property { variable, property }
        }
        ValueExpressionKind::List(values) if values.iter().any(|v| v.kind.reads_row_binding()) => {
            ScalarExpressionKind::List(
                values
                    .into_iter()
                    .map(value_to_scalar)
                    .collect::<Option<Vec<_>>>()?,
            )
        }
        kind => ScalarExpressionKind::Value(AstNode { kind, span }),
    };
    Some(AstNode { kind, span })
}
pub(super) fn scalar_binary(
    left: ScalarExpression,
    op: ScalarBinaryOp,
    right: ScalarExpression,
) -> ScalarExpression {
    let span = left.span.zip(right.span).map(|(l, r)| SourceSpan {
        start: l.start,
        end: r.end,
    });
    AstNode {
        kind: ScalarExpressionKind::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
        span,
    }
}
fn join_boolean(mut terms: Vec<ScalarExpression>, op: ScalarBinaryOp) -> ScalarExpression {
    let mut expression = terms.pop().expect("boolean operand");
    while let Some(left) = terms.pop() {
        expression = scalar_binary(left, op, expression)
    }
    expression
}
fn constant(expression: &ScalarExpression) -> Option<ValueExpression> {
    if let ScalarExpressionKind::Value(value) = &expression.kind
        && !value.kind.reads_row_binding()
    {
        return Some(value.clone());
    }
    None
}
fn property(expression: &ScalarExpression) -> Option<(String, String)> {
    if let ScalarExpressionKind::Property { variable, property } = &expression.kind {
        Some((variable.clone(), property.clone()))
    } else {
        None
    }
}
fn is_null_property(expression: &ScalarExpression, negated: bool) -> Option<(String, String)> {
    if let ScalarExpressionKind::IsNull {
        expression,
        negated: n,
    } = &expression.kind
        && *n == negated
    {
        return property(expression);
    }
    None
}
fn property_compare(
    expression: &ScalarExpression,
    op: ScalarBinaryOp,
) -> Option<((String, String), ValueExpression)> {
    if let ScalarExpressionKind::Binary { left, op: o, right } = &expression.kind
        && *o == op
    {
        return Some((property(left)?, constant(right)?));
    }
    None
}
fn normalize_case(kind: &ScalarExpressionKind) -> Option<ScalarExpressionKind> {
    let ScalarExpressionKind::Case {
        operand: None,
        branches,
        otherwise: Some(otherwise),
    } = kind
    else {
        return None;
    };
    if branches.len() == 1 {
        let (condition, result) = &branches[0];
        if let Some((variable, property)) = is_null_property(condition, true) {
            if let ScalarExpressionKind::Lower(inner) = &result.kind
                && self::property(inner) == Some((variable.clone(), property.clone()))
            {
                return Some(ScalarExpressionKind::CaseLowerPropertyDefault {
                    variable,
                    property,
                    default: constant(otherwise)?,
                });
            }
            if self::property(result) == Some((variable.clone(), property.clone())) {
                return Some(ScalarExpressionKind::DefaultIfNull {
                    variable,
                    property,
                    default: constant(otherwise)?,
                });
            }
        }
        if let ScalarExpressionKind::Binary {
            left,
            op: ScalarBinaryOp::Or,
            right,
        } = &condition.kind
        {
            let (variable, property) = is_null_property(left, false)?;
            let (other, empty) = property_compare(right, ScalarBinaryOp::Eq)?;
            if other == (variable.clone(), property.clone())
                && self::property(otherwise) == Some(other)
            {
                return Some(ScalarExpressionKind::DefaultIfNullOrEq {
                    variable,
                    property,
                    empty,
                    default: constant(result)?,
                });
            }
        }
        if let ScalarExpressionKind::Binary {
            left,
            op: ScalarBinaryOp::And,
            right,
        } = &condition.kind
        {
            let (variable, property) = is_null_property(left, true)?;
            let (other, empty) = property_compare(right, ScalarBinaryOp::NotEq)?;
            if other == (variable.clone(), property.clone()) {
                return Some(ScalarExpressionKind::CasePropertyNotNullOrEq {
                    variable,
                    property,
                    empty: Box::new(empty),
                    non_empty: Box::new(constant(result)?),
                    null_or_empty: Box::new(constant(otherwise)?),
                });
            }
        }
    }
    let mut normalized = Vec::new();
    let mut binding = None;
    for (condition, result) in branches {
        let (names, value) = property_compare(condition, ScalarBinaryOp::Eq)?;
        if binding.as_ref().is_some_and(|b| b != &names) {
            return None;
        }
        binding = Some(names);
        normalized.push((value, constant(result)?))
    }
    let (variable, property) = binding?;
    Some(ScalarExpressionKind::CasePropertyEqualsRank {
        variable,
        property,
        branches: normalized,
        default: constant(otherwise)?,
    })
}
fn value_height(value: &ValueExpression) -> usize {
    match &value.kind {
        ValueExpressionKind::List(v) => 1 + v.iter().map(value_height).max().unwrap_or(0),
        ValueExpressionKind::Timestamp(v) => 1 + value_height(v),
        _ => 1,
    }
}
fn special_value_height(kind: &ScalarExpressionKind) -> usize {
    match kind {
        ScalarExpressionKind::CaseLowerPropertyDefault { default, .. }
        | ScalarExpressionKind::DefaultIfNull { default, .. } => value_height(default),
        ScalarExpressionKind::DefaultIfNullOrEq { empty, default, .. } => {
            value_height(empty).max(value_height(default))
        }
        ScalarExpressionKind::CasePropertyNotNullOrEq {
            empty,
            non_empty,
            null_or_empty,
            ..
        } => value_height(empty)
            .max(value_height(non_empty))
            .max(value_height(null_or_empty)),
        ScalarExpressionKind::CasePropertyEqualsRank {
            branches, default, ..
        } => branches
            .iter()
            .map(|(a, b)| value_height(a).max(value_height(b)))
            .max()
            .unwrap_or(0)
            .max(value_height(default)),
        _ => 0,
    }
}
