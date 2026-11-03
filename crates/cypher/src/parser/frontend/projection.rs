// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Projection and aggregate grammar with direct AST lowering.
use super::lexer::TokenKind as T;
use super::Transformer;
use crate::*;
use hawdb_core::{ListFunction, Result, Value};
use hawdb_sql_syntax::peg::NodeId;
impl Transformer<'_> {
    pub(super) fn projection_clause(
        &mut self,
        node: NodeId,
        depth: usize,
        anonymous: &mut usize,
    ) -> Result<Clause> {
        let projection = self.tree.required(node, rule_id!("r_projection"));
        let distinct = self
            .tree
            .child(projection, rule_id!("r_distinct"))
            .is_some();
        let items = if let Some(items) = self.tree.take_projection(projection) {
            items?
        } else {
            let mut items = Vec::new();
            for item in self.iter_named_children(projection, rule_id!("r_item")) {
                items.push(self.projection_item(item, depth)?);
            }
            items
        };
        let predicate = self.fold_where(node, depth, anonymous)?;
        let mut order_by = Vec::new();
        if let Some(order) = self.tree.child(node, rule_id!("r_order")) {
            if let Some(items) = self.tree.take_order(order) {
                order_by = items?;
            } else {
                for item in self.iter_named_children(order, rule_id!("r_order_item")) {
                    order_by.push(self.order_item(item, depth)?);
                }
            }
        }
        let offset = self
            .tree
            .child(node, rule_id!("r_offset"))
            .map(|id| self.value(self.tree.required(id, rule_id!("v_value")), depth))
            .transpose()?;
        let limit = self
            .tree
            .child(node, rule_id!("r_limit"))
            .map(|id| self.value(self.tree.required(id, rule_id!("v_value")), depth))
            .transpose()?;
        let projection = ProjectionClause {
            distinct,
            items,
            predicate,
            order_by,
            offset,
            limit,
        };
        Ok(self.source(
            node,
            if self.tree.rule_id(node) == rule_id!("with_clause") {
                ClauseKind::With(projection)
            } else {
                ClauseKind::Return(projection)
            },
        ))
    }
    pub(super) fn projection_item(&mut self, item: NodeId, depth: usize) -> Result<ReturnItem> {
        let expression = self.projection_expression(item, depth)?;
        let alias = self.tree.child(item, rule_id!("r_alias")).map(|id| {
            self.text(self.tree.required(id, rule_id!("v_identifier")))
                .to_owned()
        });
        Ok(self.source(item, ReturnItemKind { expression, alias }))
    }
    fn projection_expression(&mut self, node: NodeId, depth: usize) -> Result<ReturnExpression> {
        let mut expression =
            self.return_expression(self.tree.required(node, rule_id!("r_sum")), depth)?;
        if return_depth(&expression) > super::super::MAX_CYPHER_PARSER_DEPTH {
            return Err(self.error(node, "arithmetic expression exceeds maximum nesting depth"));
        }
        if let Some(null) = self.tree.child(node, rule_id!("x_is_null")) {
            let negated = self.tree.child(null, rule_id!("x_negated")).is_some();
            let operand = match expression.kind {
                ReturnExpressionKind::Value(value) => value,
                ReturnExpressionKind::Path(path) => AstNode {
                    kind: ScalarExpressionKind::Path(path),
                    span: expression.span,
                },
                _ => {
                    return Err(self.error(
                        node,
                        "IS NULL in a projection requires a non-aggregate operand",
                    ))
                }
            };
            let span = SourceSpan {
                start: self.tree.span(node, self.tokens).start,
                end: self.tree.span(null, self.tokens).end,
            };
            let scalar = AstNode::from_source(
                ScalarExpressionKind::IsNull {
                    expression: Box::new(operand),
                    negated,
                },
                span,
            );
            expression = AstNode::from_source(ReturnExpressionKind::Value(scalar), span);
        }
        Ok(expression)
    }
    pub(super) fn return_expression(
        &mut self,
        node: NodeId,
        depth: usize,
    ) -> Result<ReturnExpression> {
        let product = self.tree.rule_id(node) == rule_id!("r_product");
        let first_name = if product {
            rule_id!("r_atom")
        } else {
            rule_id!("r_product")
        };
        let first = self.tree.required(node, first_name);
        let first = if product {
            self.return_atom(first, depth)?
        } else {
            self.return_expression(first, depth)?
        };
        let mut rest = Vec::new();
        for item in self.iter_named_children(
            node,
            if product {
                rule_id!("r_product_rest")
            } else {
                rule_id!("r_sum_rest")
            },
        ) {
            let op = self.tree.required(
                item,
                if product {
                    rule_id!("r_product_op")
                } else {
                    rule_id!("r_sum_op")
                },
            );
            let op = match self.text(op) {
                "+" => ArithmeticOp::Add,
                "-" => ArithmeticOp::Subtract,
                "*" => ArithmeticOp::Multiply,
                "/" => ArithmeticOp::Divide,
                _ => ArithmeticOp::Remainder,
            };
            let rhs = self.tree.required(item, first_name);
            let rhs = if product {
                self.return_atom(rhs, depth)?
            } else {
                self.return_expression(rhs, depth)?
            };
            rest.push((op, rhs));
        }
        Ok(if rest.is_empty() {
            first
        } else {
            self.source(
                node,
                ReturnExpressionKind::Arithmetic {
                    first: Box::new(first),
                    rest,
                },
            )
        })
    }
    fn return_atom(&mut self, node: NodeId, depth: usize) -> Result<ReturnExpression> {
        self.expression_budget(node, depth + 1)?;
        let selected = self.tree.children(node)[0];
        let kind = match self.tree.node(selected).name.expect("return atom") {
            "r_paren" => {
                self.return_expression(
                    self.tree.required(
                        self.tree.required(selected, rule_id!("r_nested_sum")),
                        rule_id!("r_sum"),
                    ),
                    depth + 1,
                )?
                .kind
            }
            "r_unary" => {
                let operand = self.return_atom(
                    self.tree.required(
                        self.tree.required(selected, rule_id!("r_unary_contents")),
                        rule_id!("r_atom"),
                    ),
                    depth + 1,
                )?;
                let minus_one = self.source(
                    node,
                    ReturnExpressionKind::Value(AstNode::synthetic(ScalarExpressionKind::Value(
                        AstNode::synthetic(ValueExpressionKind::Literal(Value::Int(-1))),
                    ))),
                );
                ReturnExpressionKind::Arithmetic {
                    first: Box::new(minus_one),
                    rest: vec![(ArithmeticOp::Multiply, operand)],
                }
            }
            "x_scalar" => ReturnExpressionKind::Value(self.scalar(selected, depth + 1)?),
            "r_path" => {
                let path = if let Some(properties) =
                    self.tree.child(selected, rule_id!("x_path_properties"))
                {
                    self.scalar_path(properties, depth + 1)?
                } else {
                    ShortestPathReturnExpression::Length {
                        path_variable: self
                            .text(self.tree.required(selected, rule_id!("v_identifier")))
                            .to_owned(),
                    }
                };
                ReturnExpressionKind::Path(path)
            }
            "r_collect_top" => {
                let (document_id, score) = self.identifier_pair(selected);
                let limit =
                    self.value(self.tree.required(selected, rule_id!("v_value")), depth + 1)?;
                ReturnExpressionKind::Aggregate(AggregateExpression::CollectTop {
                    document_id,
                    score,
                    limit,
                })
            }
            "r_aggregate" => ReturnExpressionKind::Aggregate(self.aggregate(selected, depth + 1)?),
            "r_list_function" => {
                let call = self.tree.required(selected, rule_id!("r_list_call"));
                let name = self.text(self.tree.required(call, rule_id!("x_list_function_name")));
                let string_function = hawdb_core::StringFunction::from_function_name(name);
                let list_function = ListFunction::from_function_name(name);
                let mut arguments = Vec::new();
                for argument in self
                    .tree
                    .children(call)
                    .iter()
                    .copied()
                    .filter(|id| self.tree.rule_id(*id) == rule_id!("r_sum"))
                {
                    arguments.push(self.return_expression(argument, depth + 1)?)
                }
                if let Some(function) = string_function {
                    self.string_arity(selected, function, arguments.len())?;
                    let scalars = arguments
                        .into_iter()
                        .map(|argument| self.scalar_return_argument(argument, selected, depth + 1))
                        .collect::<Result<Vec<_>>>()?;
                    let scalar = self.source(
                        call,
                        ScalarExpressionKind::StringFunction {
                            function,
                            arguments: scalars,
                        },
                    );
                    let scalar = self.scalar_postfixes(selected, scalar, depth + 1)?;
                    return Ok(self.source(node, ReturnExpressionKind::Value(scalar)));
                }
                let function = list_function.expect("list function");
                self.list_arity(selected, function, arguments.len())?;
                if arguments.iter().any(contains_aggregate) {
                    if self
                        .iter_named_children(selected, rule_id!("x_postfix"))
                        .next()
                        .is_some()
                    {
                        return Err(self.error(
                            selected,
                            "aggregate list functions do not support postfixes",
                        ));
                    }
                    ReturnExpressionKind::ListFunction {
                        function,
                        arguments,
                    }
                } else {
                    let mut scalars = Vec::new();
                    for argument in arguments {
                        scalars.push(self.scalar_return_argument(argument, selected, depth + 1)?);
                    }
                    let scalar = self.source(
                        call,
                        ScalarExpressionKind::ListFunction {
                            function,
                            arguments: scalars,
                        },
                    );
                    ReturnExpressionKind::Value(self.scalar_postfixes(
                        selected,
                        scalar,
                        depth + 1,
                    )?)
                }
            }
            _ => unreachable!("return atom rule"),
        };
        Ok(self.source(node, kind))
    }
    fn aggregate(&mut self, node: NodeId, depth: usize) -> Result<AggregateExpression> {
        let function = match self
            .text(self.tree.required(node, rule_id!("r_aggregate_name")))
            .to_ascii_uppercase()
            .as_str()
        {
            "COUNT" => AggregateFunctionName::Count,
            "SUM" => AggregateFunctionName::Sum,
            "MIN" => AggregateFunctionName::Min,
            "MAX" => AggregateFunctionName::Max,
            "AVG" => AggregateFunctionName::Avg,
            _ => AggregateFunctionName::Collect,
        };
        let contents = self.tree.required(node, rule_id!("r_aggregate_contents"));
        let distinct = self.tree.child(contents, rule_id!("r_distinct")).is_some();
        if self.tree.child(contents, rule_id!("r_star")).is_some() {
            if function != AggregateFunctionName::Count || distinct {
                return Err(self.error(node, "COUNT(DISTINCT *) is not supported"));
            }
            return Ok(AggregateExpression::CountAll);
        }
        let arg = self.tree.required(contents, rule_id!("r_sum"));
        let tokens = &self.tokens[self.tree.node(arg).start..self.tree.node(arg).end];
        let ordinary_word = |token: &super::lexer::Token| {
            token.kind == T::Word
                && !["true", "false", "null"]
                    .iter()
                    .any(|word| token.is_keyword(self.input, word))
        };
        let simple = match tokens {
            [token] => ordinary_word(token),
            [left, dot, right] => {
                ordinary_word(left) && dot.kind == T::Dot && right.kind == T::Word
            }
            _ => false,
        };
        if simple
            && (!distinct
                || matches!(
                    function,
                    AggregateFunctionName::Count | AggregateFunctionName::Collect
                ))
        {
            let variable = tokens[0].text(self.input).to_owned();
            let property = tokens.get(2).map(|token| token.text(self.input).to_owned());
            let fixed = match (function, property) {
                (AggregateFunctionName::Count, Some(property)) => {
                    Some(AggregateExpression::CountProperty {
                        variable,
                        property,
                        distinct,
                    })
                }
                (AggregateFunctionName::Count, None) => {
                    Some(AggregateExpression::CountVariable { variable, distinct })
                }
                (AggregateFunctionName::Collect, Some(property)) => {
                    Some(AggregateExpression::CollectProperty {
                        variable,
                        property,
                        distinct,
                    })
                }
                (AggregateFunctionName::Collect, None) => {
                    Some(AggregateExpression::CollectVariable { variable, distinct })
                }
                (AggregateFunctionName::Min, Some(property)) => {
                    Some(AggregateExpression::MinProperty { variable, property })
                }
                (AggregateFunctionName::Max, Some(property)) => {
                    Some(AggregateExpression::MaxProperty { variable, property })
                }
                (AggregateFunctionName::Max, None) => {
                    Some(AggregateExpression::MaxVariable { variable })
                }
                (AggregateFunctionName::Avg, Some(property)) => {
                    Some(AggregateExpression::AvgProperty { variable, property })
                }
                _ => None,
            };
            if let Some(fixed) = fixed {
                return Ok(fixed);
            }
        }
        let argument = Box::new(self.return_expression(arg, depth)?);
        Ok(AggregateExpression::Expression {
            function,
            distinct,
            argument,
        })
    }
    pub(super) fn order_item(&mut self, node: NodeId, depth: usize) -> Result<OrderItem> {
        let expression = self.tree.required(node, rule_id!("r_order_expression"));
        let expression = order_expression(self.projection_expression(expression, depth)?);
        let direction = if self
            .tree
            .child(node, rule_id!("r_direction"))
            .is_some_and(|id| self.text(id).eq_ignore_ascii_case("DESC"))
        {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        Ok(self.source(
            node,
            OrderItemKind {
                expression,
                direction,
            },
        ))
    }
}
fn return_depth(value: &ReturnExpression) -> usize {
    match &value.kind {
        ReturnExpressionKind::Arithmetic { first, rest } => {
            rest.iter().fold(return_depth(first), |depth, (_, right)| {
                depth.max(return_depth(right)) + 1
            })
        }
        ReturnExpressionKind::ListFunction { arguments, .. } => {
            arguments.iter().map(return_depth).max().unwrap_or(0) + 1
        }
        _ => 1,
    }
}
fn contains_aggregate(value: &ReturnExpression) -> bool {
    match &value.kind {
        ReturnExpressionKind::Aggregate(_) => true,
        ReturnExpressionKind::Arithmetic { first, rest } => {
            contains_aggregate(first) || rest.iter().any(|(_, child)| contains_aggregate(child))
        }
        ReturnExpressionKind::ListFunction { arguments, .. } => {
            arguments.iter().any(contains_aggregate)
        }
        _ => false,
    }
}

fn order_expression(expression: ReturnExpression) -> OrderExpression {
    let span = expression.span;
    match expression.kind {
        ReturnExpressionKind::Value(value) => match value.kind {
            ScalarExpressionKind::Variable(name) => OrderExpression::Column(name),
            ScalarExpressionKind::Property { variable, property } => {
                OrderExpression::Property { variable, property }
            }
            ScalarExpressionKind::Id(variable) => OrderExpression::Id { variable },
            kind => OrderExpression::Value(AstNode {
                kind,
                span: value.span,
            }),
        },
        ReturnExpressionKind::Path(path) => OrderExpression::Value(AstNode {
            kind: ScalarExpressionKind::Path(path),
            span,
        }),
        kind => OrderExpression::Projection(AstNode { kind, span }),
    }
}
