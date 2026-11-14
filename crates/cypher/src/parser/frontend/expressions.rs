// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Scalar and CASE token PEG. Recognition is separate from AST specialization.
use super::Transformer;
use crate::*;
use hawdb_core::{ListFunction, Result, Value, VectorDistance};
use hawdb_sql_syntax::peg::NodeId;

impl Transformer<'_> {
    pub(super) fn expression_budget(&mut self, node: NodeId, depth: usize) -> Result<()> {
        self.check_depth(node, 0, depth)
    }
    pub(super) fn scalar(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        let primary = self.tree.required(node, rule_id!("x_primary"));
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
            .iter_named_children(node, rule_id!("x_postfix"))
            .enumerate()
        {
            if let Some(key) = self.tree.child(postfix, rule_id!("v_identifier")) {
                self.expression_budget(postfix, depth.max(2) + index)?;
                let kind = ScalarExpressionKind::PropertyAccess {
                    expression: Box::new(expression),
                    key: self.text(key).to_owned(),
                };
                expression = AstNode::from_source(
                    kind,
                    SourceSpan {
                        start: self.tree.span(node, self.tokens).start,
                        end: self.tree.span(postfix, self.tokens).end,
                    },
                );
                continue;
            }
            self.expression_budget(postfix, depth + index + 1)?;
            let contents = self.tree.required(postfix, rule_id!("x_postfix_contents"));
            let first = self
                .tree
                .child(contents, rule_id!("x_case_scalar"))
                .filter(|_| self.tree.child(contents, rule_id!("x_dots")).is_none())
                .map(|id| self.case_scalar(id, depth + index + 1).map(Box::new))
                .transpose()?;
            let tail = self.tree.child(contents, rule_id!("x_slice_tail"));
            let slice = tail.is_some() || self.tree.child(contents, rule_id!("x_dots")).is_some();
            let kind = if slice {
                let end = self
                    .tree
                    .child(tail.unwrap_or(contents), rule_id!("x_case_scalar"))
                    .map(|id| self.case_scalar(id, depth + index + 1).map(Box::new))
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
        self.scalar_arithmetic(self.tree.required(node, rule_id!("x_sum")), depth)
    }

    fn scalar_primary(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        Ok(match self.tree.node(node).name.expect("scalar atom") {
            "x_value" => ScalarExpressionKind::Value(self.value(node, depth)?),
            "x_conversion" => ScalarExpressionKind::Conversion {
                function: hawdb_core::ConversionFunction::from_function_name(
                    self.text(self.tree.required(node, rule_id!("v_identifier"))),
                )
                .expect("matched conversion function"),
                // A single operand keeps its historical capture; arithmetic
                // arguments such as `toFloat(a - b)` use the sum rule.
                argument: Box::new(match self.tree.child(node, rule_id!("x_scalar")) {
                    Some(argument) => self.scalar(argument, depth)?,
                    None => self
                        .case_scalar(self.tree.required(node, rule_id!("x_case_scalar")), depth)?,
                }),
            },
            "x_temporal" => self.temporal_scalar(node, depth)?,
            "x_map" => ScalarExpressionKind::MapLiteral(self.scalar_map(node, depth)?),
            "x_variable" => ScalarExpressionKind::Variable(
                self.text(self.tree.required(node, rule_id!("v_identifier")))
                    .to_owned(),
            ),
            "x_ref" => {
                let (variable, property) = self.identifier_pair(node);
                ScalarExpressionKind::Property { variable, property }
            }
            // `not(condition)` in function-call form, such as a RETURN item;
            // a parenthesized condition is a primary only after `NOT`. A
            // bare `NOT` stays a boolean connective, never an operand.
            "x_condition_paren" => {
                let inner =
                    self.case_boolean(self.tree.required(node, rule_id!("x_or")), depth + 1)?;
                ScalarExpressionKind::Not(Box::new(self.source(node, inner.kind)))
            }
            "x_identity" => {
                let variable = self
                    .text(self.tree.required(node, rule_id!("v_identifier")))
                    .to_owned();
                let name = self.text(self.tree.required(node, rule_id!("x_identity_name")));
                if name.eq_ignore_ascii_case("ID") {
                    ScalarExpressionKind::Id(variable)
                } else if let Some(endpoint) =
                    hawdb_core::RelationshipEndpoint::from_function_name(name)
                {
                    ScalarExpressionKind::RelationshipEndpoint { endpoint, variable }
                } else if let Some(function) = hawdb_core::LabelFunction::from_function_name(name) {
                    ScalarExpressionKind::Label { function, variable }
                } else {
                    ScalarExpressionKind::RelationshipType(variable)
                }
            }
            "x_coalesce" => {
                let mut args = Vec::new();
                for child in self.iter_named_children(node, rule_id!("x_scalar")) {
                    args.push(self.scalar(child, depth)?);
                }
                ScalarExpressionKind::Coalesce(args)
            }
            "x_lower" => ScalarExpressionKind::Lower(Box::new(
                self.scalar(self.tree.required(node, rule_id!("x_scalar")), depth)?,
            )),
            "x_json" | "x_left" => {
                let expression =
                    Box::new(self.scalar(self.tree.required(node, rule_id!("x_scalar")), depth)?);
                let value = self.value(self.tree.required(node, rule_id!("v_value")), depth)?;
                if self.tree.rule_id(node) == rule_id!("x_json") {
                    let null_on_non_scalar = self.tokens
                        [self.tree.node(node).start..self.tree.node(node).end]
                        .iter()
                        .rev()
                        .nth(1)
                        .is_some_and(|token| token.is_keyword(self.input, "NONSCALAR"));
                    ScalarExpressionKind::JsonValue {
                        expression,
                        path: value,
                        null_on_non_scalar,
                    }
                } else {
                    ScalarExpressionKind::Left {
                        expression,
                        length: value,
                    }
                }
            }
            "x_date" => {
                let value = self.value(self.tree.required(node, rule_id!("v_value")), depth)?;
                let ValueExpressionKind::Literal(Value::String(part)) = value.kind else {
                    return Err(self.error(node, "date_part part requires a string literal"));
                };
                let (variable, property) =
                    self.identifier_pair(self.tree.required(node, rule_id!("x_ref")));
                ScalarExpressionKind::DatePart {
                    part,
                    variable,
                    property,
                }
            }
            "x_metric" => {
                let mut args = self.iter_named_children(node, rule_id!("x_scalar"));
                let left = args.next().expect("metric left argument");
                let right = args.next().expect("metric right argument");
                let metric = VectorDistance::from_function_name(
                    self.text(self.tree.required(node, rule_id!("x_metric_name"))),
                )
                .expect("metric rule");
                ScalarExpressionKind::VectorDistance {
                    metric,
                    left: Box::new(self.scalar(left, depth)?),
                    right: Box::new(self.scalar(right, depth)?),
                }
            }
            "x_path" | "x_path_properties" => {
                ScalarExpressionKind::Path(self.scalar_path(node, depth)?)
            }
            "x_list_function" => {
                let name = self.text(self.tree.required(node, rule_id!("x_list_function_name")));
                if name.eq_ignore_ascii_case("reduce") {
                    return self.reduce(node, depth);
                }
                let string_function = hawdb_core::StringFunction::from_function_name(name);
                let numeric_function = hawdb_core::NumericFunction::from_function_name(name);
                let list_function = ListFunction::from_function_name(name);
                let mut arguments = Vec::new();
                for arg in self
                    .tree
                    .children(node)
                    .iter()
                    .copied()
                    .filter(|id| self.tree.rule_id(*id) == rule_id!("x_case_scalar"))
                {
                    arguments.push(self.case_scalar(arg, depth)?);
                }
                if let Some(function) = string_function {
                    self.string_arity(node, function, arguments.len())?;
                    ScalarExpressionKind::StringFunction {
                        function,
                        arguments,
                    }
                } else if let Some(function) = numeric_function {
                    ScalarExpressionKind::NumericFunction {
                        function,
                        argument: Box::new(self.numeric_argument(node, function, arguments)?),
                    }
                } else {
                    let function = list_function.expect("list function");
                    self.list_arity(node, function, arguments.len())?;
                    ScalarExpressionKind::ListFunction {
                        function,
                        arguments,
                    }
                }
            }
            "x_list" => {
                let list = self.tree.children(node)[0];
                match self.tree.node(list).name {
                    Some("x_value_list") => {
                        if let Some(kind) = self.captured_property_list(list, depth)? {
                            return Ok(kind);
                        }
                        let value = self.value(list, depth)?;
                        if !value.kind.reads_row_binding() {
                            ScalarExpressionKind::Value(value)
                        } else {
                            self.captured_value_scalar(list, depth.saturating_sub(1))?
                                .kind
                        }
                    }
                    Some("x_expression_list") => {
                        let mut elements = Vec::new();
                        for child in self
                            .tree
                            .children(
                                self.tree
                                    .required(list, rule_id!("x_expression_list_contents")),
                            )
                            .iter()
                            .copied()
                        {
                            let element = self.case_boolean(child, depth)?;
                            // As in a projection, a pattern is not a value.
                            if matches!(element.kind, ScalarExpressionKind::PatternPredicate(_)) {
                                return Err(self.error(
                                    child,
                                    "a pattern is not a list element; use a pattern comprehension [pattern | expression]",
                                ));
                            }
                            elements.push(element);
                        }
                        ScalarExpressionKind::List(elements)
                    }
                    Some("x_comprehension")
                        if let Some(pattern) = self.tree.child(list, rule_id!("w_graph")) =>
                    {
                        let pattern = Box::new(self.expression_pattern(pattern, depth)?);
                        let predicate = self
                            .tree
                            .child(list, rule_id!("x_comprehension_where"))
                            .map(|id| {
                                self.case_boolean(self.tree.required(id, rule_id!("x_or")), depth)
                                    .map(Box::new)
                            })
                            .transpose()?;
                        let projection = self
                            .tree
                            .required(list, rule_id!("x_comprehension_projection"));
                        let projection = Box::new(self.case_scalar(
                            self.tree.required(projection, rule_id!("x_case_scalar")),
                            depth,
                        )?);
                        ScalarExpressionKind::PatternComprehension {
                            pattern,
                            predicate,
                            projection,
                        }
                    }
                    Some("x_comprehension") => {
                        let variable = self
                            .text(self.tree.required(list, rule_id!("v_identifier")))
                            .to_owned();
                        let contents = self
                            .tree
                            .required(list, rule_id!("x_comprehension_contents"));
                        let source = Box::new(self.case_scalar(
                            self.tree.required(contents, rule_id!("x_case_scalar")),
                            depth,
                        )?);
                        let predicate = self
                            .tree
                            .child(contents, rule_id!("x_comprehension_where"))
                            .map(|id| {
                                self.case_boolean(self.tree.required(id, rule_id!("x_or")), depth)
                                    .map(Box::new)
                            })
                            .transpose()?;
                        let projection = self
                            .tree
                            .child(contents, rule_id!("x_comprehension_projection"))
                            .map(|id| {
                                self.case_scalar(
                                    self.tree.required(id, rule_id!("x_case_scalar")),
                                    depth,
                                )
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
            .text(self.tree.required(node, rule_id!("v_identifier")))
            .to_owned();
        if self.tree.rule_id(node) == rule_id!("x_path_properties") {
            let value = self.value(self.tree.required(node, rule_id!("v_value")), depth)?;
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
                    .text(self.tree.required(node, rule_id!("x_path_name")))
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
    // Property-only lists already have the scalar capture shape. Decode their
    // names once while retaining the value and scalar depth checks in order.
    fn captured_property_list(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<Option<ScalarExpressionKind>> {
        let tree = self.tree;
        let list = tree.children(node)[0];
        let contents = tree.required(list, rule_id!("v_list_contents"));
        let children = tree.children(contents);
        if children.is_empty()
            || !children.iter().all(|child| {
                let selected = tree.children(*child)[0];
                tree.rule_id(selected) == rule_id!("v_property")
                    && tree.children(selected).len() == 2
            })
        {
            return Ok(None);
        }
        self.check_depth(node, depth, 1)?;
        self.check_depth(children[0], depth + 1, 1)?;
        let captured_depth = depth.saturating_sub(1);
        self.check_depth(node, captured_depth + 1, 1)?;
        self.check_depth(children[0], captured_depth + 2, 1)?;
        let mut elements = Vec::with_capacity(children.len());
        for child in children {
            elements.push(self.captured_reference(tree.children(*child)[0], captured_depth + 2)?);
        }
        Ok(Some(ScalarExpressionKind::List(elements)))
    }
    /// `reduce(accumulator = initial, variable IN list | expression)`.
    fn reduce(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpressionKind> {
        let names = self
            .iter_named_children(node, rule_id!("v_identifier"))
            .map(|id| self.text(id).to_owned())
            .collect::<Vec<_>>();
        let operands = self
            .iter_named_children(node, rule_id!("x_case_scalar"))
            .collect::<Vec<_>>();
        let (Ok([accumulator, variable]), [initial, list, expression]) =
            (<[String; 2]>::try_from(names), operands.as_slice())
        else {
            return Err(self.error(
                node,
                "reduce() requires `accumulator = initial, variable IN list | expression`",
            ));
        };
        let (initial, list, expression) = (*initial, *list, *expression);
        Ok(ScalarExpressionKind::Reduce {
            accumulator,
            initial: Box::new(self.case_scalar(initial, depth)?),
            variable,
            list: Box::new(self.case_scalar(list, depth)?),
            expression: Box::new(self.case_scalar(expression, depth)?),
        })
    }
    pub(super) fn numeric_argument<T>(
        &self,
        node: NodeId,
        function: hawdb_core::NumericFunction,
        arguments: Vec<T>,
    ) -> Result<T> {
        let count = arguments.len();
        let mut arguments = arguments.into_iter();
        match (arguments.next(), arguments.next()) {
            (Some(argument), None) => Ok(argument),
            _ => Err(self.error_at(
                self.tree.span(node, self.tokens).end,
                &format!(
                    "{}() expects 1 argument(s), got {count}",
                    function.function_name()
                ),
            )),
        }
    }
    pub(super) fn string_arity(
        &self,
        node: NodeId,
        function: hawdb_core::StringFunction,
        arguments: usize,
    ) -> Result<()> {
        let (min, max) = function.arity();
        if !(min..=max).contains(&arguments) {
            return Err(self.error_at(
                self.tree.span(node, self.tokens).end,
                &format!(
                    "{}() expects {min}..={max} arguments",
                    function.function_name()
                ),
            ));
        }
        Ok(())
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
        Err(self.error_at(
            self.tree.span(node, self.tokens).end,
            &format!(
                "{}() expects {expected} argument(s), got {count}",
                function.function_name()
            ),
        ))
    }
    pub(super) fn case_boolean(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        let ands = self.iter_named_children(node, rule_id!("x_and"));
        let mut terms = BooleanTerms::default();
        for (index, and) in ands.enumerate() {
            self.expression_budget(and, depth + index)?;
            terms.push(self.case_and(and, depth + index)?)
        }
        // Subsequent AND operands are captured under the corresponding OR rest.
        for rest in self.iter_named_children(node, rule_id!("x_or_rest")) {
            let index = terms.len();
            self.expression_budget(rest, depth + index)?;
            terms.push(self.case_and(self.tree.required(rest, rule_id!("x_and")), depth + index)?);
        }
        Ok(join_boolean(terms, ScalarBinaryOp::Or))
    }
    fn case_and(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        let mut terms = BooleanTerms::default();
        terms.push(self.case_condition(self.tree.required(node, rule_id!("x_condition")), depth)?);
        for rest in self.iter_named_children(node, rule_id!("x_and_rest")) {
            let index = terms.len();
            self.expression_budget(rest, depth + index)?;
            terms.push(self.case_condition(
                self.tree.required(rest, rule_id!("x_condition")),
                depth + index,
            )?)
        }
        Ok(join_boolean(terms, ScalarBinaryOp::And))
    }
    fn case_condition(&mut self, node: NodeId, depth: usize) -> Result<ScalarExpression> {
        self.expression_budget(node, depth + 1)?;
        if let Some(not) = self.tree.child(node, rule_id!("x_not")) {
            let expression = self.case_condition(
                self.tree.required(
                    self.tree.required(not, rule_id!("x_not_contents")),
                    rule_id!("x_condition"),
                ),
                depth + 1,
            )?;
            return Ok(self.source(node, ScalarExpressionKind::Not(Box::new(expression))));
        }
        let base = self.tree.required(node, rule_id!("x_condition_base"));
        let selected = self.tree.children(base)[0];
        let left = match self.tree.node(selected).name {
            Some("w_graph") => {
                let pattern = Box::new(self.expression_pattern(selected, depth + 1)?);
                self.source(selected, ScalarExpressionKind::PatternPredicate(pattern))
            }
            Some("x_condition_paren") => {
                let inner =
                    self.case_boolean(self.tree.required(selected, rule_id!("x_or")), depth + 1)?;
                self.source(selected, inner.kind)
            }
            Some("x_list_contains") => {
                let args = self.named_children(selected, rule_id!("x_case_scalar"));
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
        let Some(tail) = self.tree.child(node, rule_id!("x_condition_tail")) else {
            return Ok(left);
        };
        let tail = self.tree.children(tail)[0];
        Ok(match self.tree.node(tail).name {
            Some("x_is_null") => self.source(
                node,
                ScalarExpressionKind::IsNull {
                    expression: Box::new(left),
                    negated: self.tree.child(tail, rule_id!("x_negated")).is_some(),
                },
            ),
            Some("x_contains") => {
                let right = self.case_scalar(
                    self.tree.required(tail, rule_id!("x_case_scalar")),
                    depth + 1,
                )?;
                let op = if self
                    .text(self.tree.required(tail, rule_id!("x_contains_op")))
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
                let comparisons = self.named_children(tail, rule_id!("x_comparison"));
                let chained = comparisons.len() > 1;
                let mut links = Vec::new();
                let mut previous = left;
                for comparison in comparisons {
                    let op = self.comparison_operator(
                        self.tree.required(comparison, rule_id!("x_comparison_op")),
                    );
                    let right = self.case_scalar(
                        self.tree.required(comparison, rule_id!("x_case_scalar")),
                        depth + 1,
                    )?;
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
    /// The pattern of a pattern predicate or comprehension. Its anonymous
    /// variables are scoped to the pattern, so they are numbered on their own.
    pub(super) fn expression_pattern(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<MatchPattern> {
        self.pattern(node, depth, &mut 0)
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
        if self.tree.rule_id(selected) == rule_id!("x_floor_case") {
            return self.floor_case(selected, depth);
        }
        if self.tree.rule_id(selected) == rule_id!("x_reserved_special_case") {
            return self.reserved_special_case(selected, depth);
        }
        let special = self.tree.rule_id(selected) == rule_id!("x_special_case");
        let body = if special {
            self.tree.required(selected, rule_id!("x_case_body"))
        } else {
            selected
        };
        if special {
            if let Some(kind) = self.literal_rank_case(body)? {
                self.expression_budget(node, depth + special_value_height(&kind))?;
                return Ok(kind);
            }
            let generic = self.generic_case(body, 0)?;
            if let Some(kind) = normalize_case(&generic, self.input) {
                self.expression_budget(node, depth + special_value_height(&kind))?;
                return Ok(kind);
            }
        }
        self.generic_case(body, depth)
    }
    // Follow only transparent captures. Parentheses, unary operators and
    // additional operands keep their full lowering and depth checks.
    fn singleton_case_capture(
        &self,
        mut node: NodeId,
        target: super::tree::RuleId,
    ) -> Option<NodeId> {
        loop {
            if self.tree.rule_id(node) == target {
                return Some(node);
            }
            if !matches!(
                self.tree.node(node).name,
                Some(
                    "x_or"
                        | "x_and"
                        | "x_condition_base"
                        | "x_case_scalar"
                        | "x_sum"
                        | "x_product"
                        | "x_operand"
                        | "x_scalar"
                        | "x_primary"
                        | "x_condition_tail"
                        | "x_comparisons"
                )
            ) {
                return None;
            }
            let [child] = self.tree.children(node) else {
                return None;
            };
            let parent_span = self.tree.span(node, self.tokens);
            if parent_span != self.tree.span(*child, self.tokens) {
                return None;
            }
            node = *child;
        }
    }

    fn literal_rank_branch(&self, branch: NodeId) -> Option<(NodeId, NodeId, NodeId)> {
        let condition = self.singleton_case_capture(
            self.tree.child(branch, rule_id!("x_or"))?,
            rule_id!("x_condition"),
        )?;
        let reference = self.singleton_case_capture(
            self.tree.child(condition, rule_id!("x_condition_base"))?,
            rule_id!("x_ref"),
        )?;
        let comparison = self.singleton_case_capture(
            self.tree.child(condition, rule_id!("x_condition_tail"))?,
            rule_id!("x_comparison"),
        )?;
        if self.text(self.tree.child(comparison, rule_id!("x_comparison_op"))?) != "=" {
            return None;
        }
        let value = self.singleton_case_capture(
            self.tree.child(comparison, rule_id!("x_case_scalar"))?,
            rule_id!("x_value"),
        )?;
        let result = self.singleton_case_capture(
            self.tree.child(branch, rule_id!("x_case_scalar"))?,
            rule_id!("x_value"),
        )?;
        Some((reference, value, result))
    }

    fn literal_rank_case(&mut self, node: NodeId) -> Result<Option<ScalarExpressionKind>> {
        let body = self.tree.children(node)[0];
        if self.tree.rule_id(body) != rule_id!("x_searched_case") {
            return Ok(None);
        }
        // Validate the complete shape before allocating or decoding anything.
        // Unsupported shapes use the existing generic AST lowering.
        let Some(default) = self
            .tree
            .child(body, rule_id!("x_else"))
            .and_then(|id| self.tree.child(id, rule_id!("x_case_scalar")))
            .and_then(|id| self.singleton_case_capture(id, rule_id!("x_value")))
        else {
            return Ok(None);
        };
        let mut binding = None;
        let mut first_reference = None;
        for branch in self.iter_named_children(body, rule_id!("x_searched_branch")) {
            let Some((reference, _, _)) = self.literal_rank_branch(branch) else {
                return Ok(None);
            };
            let mut names = self.iter_named_children(reference, rule_id!("v_identifier"));
            let pair = (
                self.text(names.next().expect("matched variable")),
                self.text(names.next().expect("matched property")),
            );
            if binding.is_some_and(|old| old != pair) {
                return Ok(None);
            }
            binding = Some(pair);
            first_reference.get_or_insert(reference);
        }
        let Some(reference) = first_reference else {
            return Ok(None);
        };
        let mut branches = Vec::new();
        for branch in self.iter_named_children(body, rule_id!("x_searched_branch")) {
            let (_, value, result) = self
                .literal_rank_branch(branch)
                .expect("validated rank branch");
            branches.push((self.value(value, 0)?, self.value(result, 0)?));
        }
        let default = self.value(default, 0)?;
        let (variable, property) = self.identifier_pair(reference);
        let kind = ScalarExpressionKind::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        };
        Ok(Some(kind))
    }

    fn reserved_special_case(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ScalarExpressionKind> {
        let references = self.named_children(node, rule_id!("d_ref"));
        let names = self.ids(references[0]);
        if references
            .iter()
            .any(|reference| self.ids(*reference) != names)
        {
            return self.reserved_generic_case(node, depth);
        }
        let mut values = Vec::new();
        for child in self.iter_named_children(node, rule_id!("v_value")) {
            let value = self.value(child, 0)?;
            if value.kind.reads_row_binding() {
                return self.reserved_generic_case(node, depth);
            }
            values.push(value);
        }
        let variable = names[0].clone();
        let property = names[1].clone();
        let count = values.len();
        // The matched shapes have distinct direct reference/value counts;
        // LOWER carries a named marker to distinguish its one-value form.
        let kind = if self.tree.child(node, rule_id!("x_special_lower")).is_some() {
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
        let simple = self.tree.rule_id(body) == rule_id!("x_simple_case");
        let operand = if simple {
            Some(Box::new(self.case_boolean(
                self.tree.required(body, rule_id!("x_or")),
                depth,
            )?))
        } else {
            None
        };
        let mut branches = Vec::new();
        for branch in self.iter_named_children(
            body,
            if simple {
                rule_id!("x_simple_branch")
            } else {
                rule_id!("x_searched_branch")
            },
        ) {
            let (condition, result) = if simple {
                let children = self.named_children(branch, rule_id!("x_case_scalar"));
                (
                    self.case_scalar(children[0], depth)?,
                    self.case_scalar(children[1], depth)?,
                )
            } else {
                (
                    self.case_boolean(self.tree.required(branch, rule_id!("x_or")), depth)?,
                    self.case_scalar(self.tree.required(branch, rule_id!("x_case_scalar")), depth)?,
                )
            };
            branches.push((condition, result));
        }
        let otherwise = self
            .tree
            .child(body, rule_id!("x_else"))
            .map(|id| {
                self.case_scalar(self.tree.required(id, rule_id!("x_case_scalar")), depth)
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
        let values = self.named_children(node, rule_id!("v_value"));
        for id in values {
            let value = self.value(id, depth)?;
            if value.kind != ValueExpressionKind::Literal(Value::Int(0)) {
                return self.floor_generic_case(node, depth);
            }
        }
        let mut differences = Vec::new();
        for difference in self.iter_named_children(node, rule_id!("x_difference")) {
            let mut variable = None;
            let mut terms = Vec::new();
            for term in self.iter_named_children(difference, rule_id!("x_difference_term")) {
                let names = self.ids(self.tree.required(term, rule_id!("d_ref")));
                if variable.as_ref().is_some_and(|v| v != &names[0]) {
                    return self.floor_generic_case(node, depth);
                }
                variable = Some(names[0].clone());
                let default = self.value(self.tree.required(term, rule_id!("v_value")), depth)?;
                if default.kind.reads_row_binding() {
                    return self.floor_generic_case(node, depth);
                }
                terms.push(CoalesceDifferenceTerm {
                    property: names[1].clone(),
                    default,
                })
            }
            differences.push((variable.expect("difference variable"), terms));
        }
        if differences[0] != differences[1] {
            return self.floor_generic_case(node, depth);
        }
        let (variable, terms) = differences.remove(0);
        Ok(ScalarExpressionKind::CaseCoalesceDifferenceFloorZero { variable, terms })
    }
}
pub(super) fn value_to_scalar(value: ValueExpression) -> Option<ScalarExpression> {
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
// Most boolean levels contain one operand. Keep that operand inline while
// preserving left-to-right lowering and the existing right-associated AST.
#[derive(Default)]
struct BooleanTerms {
    first: Option<ScalarExpression>,
    rest: Vec<ScalarExpression>,
}
impl BooleanTerms {
    fn push(&mut self, term: ScalarExpression) {
        if self.first.is_none() {
            self.first = Some(term);
        } else {
            if self.rest.capacity() == 0 {
                self.rest.reserve_exact(1);
            }
            self.rest.push(term);
        }
    }
    fn len(&self) -> usize {
        usize::from(self.first.is_some()) + self.rest.len()
    }
    fn pop(&mut self) -> Option<ScalarExpression> {
        self.rest.pop().or_else(|| self.first.take())
    }
}
fn join_boolean(mut terms: BooleanTerms, op: ScalarBinaryOp) -> ScalarExpression {
    let mut expression = terms.pop().expect("boolean operand");
    while let Some(left) = terms.pop() {
        expression = scalar_binary(left, op, expression)
    }
    expression
}
fn constant(expression: &ScalarExpression) -> Option<ValueExpression> {
    let kind = match &expression.kind {
        ScalarExpressionKind::Value(value) if !value.kind.reads_row_binding() => {
            return Some(value.clone())
        }
        ScalarExpressionKind::TemporalFunction {
            function,
            arguments,
        } => ValueExpressionKind::Temporal {
            function: *function,
            arguments: arguments.iter().map(constant).collect::<Option<Vec<_>>>()?,
        },
        ScalarExpressionKind::MapLiteral(entries) => ValueExpressionKind::Map(
            entries
                .iter()
                .map(|(key, value)| Some((key.clone(), constant(value)?)))
                .collect::<Option<std::collections::BTreeMap<_, _>>>()?,
        ),
        _ => return None,
    };
    Some(AstNode {
        kind,
        span: expression.span,
    })
}

fn property<'a>(expression: &'a ScalarExpression, input: &'a str) -> Option<(&'a str, &'a str)> {
    if let ScalarExpressionKind::Property { variable, property } = &expression.kind {
        Some((variable.as_str(), property.as_str()))
    } else if let ScalarExpressionKind::PropertyAccess { expression, key } = &expression.kind {
        if let ScalarExpressionKind::Value(value) = &expression.kind
            && matches!(
                value.kind,
                ValueExpressionKind::Literal(Value::Bool(_) | Value::Null)
            )
        {
            let span = expression.span?;
            let name = &input[span.start..span.end];
            if ["true", "false", "null"]
                .iter()
                .any(|keyword| name.eq_ignore_ascii_case(keyword))
            {
                return Some((name, key.as_str()));
            }
        }
        None
    } else {
        None
    }
}
fn is_null_property<'a>(
    expression: &'a ScalarExpression,
    negated: bool,
    input: &'a str,
) -> Option<(&'a str, &'a str)> {
    if let ScalarExpressionKind::IsNull {
        expression,
        negated: n,
    } = &expression.kind
        && *n == negated
    {
        return property(expression, input);
    }
    None
}
fn property_compare<'a>(
    expression: &'a ScalarExpression,
    op: ScalarBinaryOp,
    input: &'a str,
) -> Option<((&'a str, &'a str), ValueExpression)> {
    if let ScalarExpressionKind::Binary { left, op: o, right } = &expression.kind
        && *o == op
    {
        return Some((property(left, input)?, constant(right)?));
    }
    None
}
fn normalize_case(kind: &ScalarExpressionKind, input: &str) -> Option<ScalarExpressionKind> {
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
        if let Some((variable, property)) = is_null_property(condition, true, input) {
            if let ScalarExpressionKind::Lower(inner) = &result.kind
                && self::property(inner, input) == Some((variable, property))
            {
                return Some(ScalarExpressionKind::CaseLowerPropertyDefault {
                    variable: variable.to_owned(),
                    property: property.to_owned(),
                    default: constant(otherwise)?,
                });
            }
            if self::property(result, input) == Some((variable, property)) {
                return Some(ScalarExpressionKind::DefaultIfNull {
                    variable: variable.to_owned(),
                    property: property.to_owned(),
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
            let (variable, property) = is_null_property(left, false, input)?;
            let (other, empty) = property_compare(right, ScalarBinaryOp::Eq, input)?;
            if other == (variable, property) && self::property(otherwise, input) == Some(other) {
                return Some(ScalarExpressionKind::DefaultIfNullOrEq {
                    variable: variable.to_owned(),
                    property: property.to_owned(),
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
            let (variable, property) = is_null_property(left, true, input)?;
            let (other, empty) = property_compare(right, ScalarBinaryOp::NotEq, input)?;
            if other == (variable, property) {
                return Some(ScalarExpressionKind::CasePropertyNotNullOrEq {
                    variable: variable.to_owned(),
                    property: property.to_owned(),
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
        let (names, value) = property_compare(condition, ScalarBinaryOp::Eq, input)?;
        if binding.as_ref().is_some_and(|b| b != &names) {
            return None;
        }
        binding = Some(names);
        normalized.push((value, constant(result)?))
    }
    let (variable, property) = binding?;
    Some(ScalarExpressionKind::CasePropertyEqualsRank {
        variable: variable.to_owned(),
        property: property.to_owned(),
        branches: normalized,
        default: constant(otherwise)?,
    })
}
fn value_height(value: &ValueExpression) -> usize {
    match &value.kind {
        ValueExpressionKind::List(v) => 1 + v.iter().map(value_height).max().unwrap_or(0),
        ValueExpressionKind::Timestamp(v) => 1 + value_height(v),
        ValueExpressionKind::Temporal { arguments, .. } => {
            1 + arguments.iter().map(value_height).max().unwrap_or(0)
        }
        ValueExpressionKind::Map(entries) => {
            1 + entries.values().map(value_height).max().unwrap_or(0)
        }
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
