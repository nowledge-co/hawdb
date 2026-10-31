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

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

impl Parser<'_> {
    pub(super) fn parse_return_atom(&mut self) -> Result<ReturnExpressionKind> {
        if self.consume_aggregate_function_name("COLLECT_TOP") {
            self.expect_char('(')?;
            let document_id = self.parse_ident()?;
            self.expect_char(',')?;
            let score = self.parse_ident()?;
            self.expect_char(',')?;
            let limit = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(ReturnExpressionKind::Aggregate(
                AggregateExpression::CollectTop {
                    document_id,
                    score,
                    limit,
                },
            ));
        }
        const FUNCTIONS: [(&str, AggregateFunctionName); 6] = [
            ("COUNT", AggregateFunctionName::Count),
            ("SUM", AggregateFunctionName::Sum),
            ("MIN", AggregateFunctionName::Min),
            ("MAX", AggregateFunctionName::Max),
            ("AVG", AggregateFunctionName::Avg),
            ("COLLECT", AggregateFunctionName::Collect),
        ];
        let Some(function) = FUNCTIONS.into_iter().find_map(|(name, function)| {
            self.consume_aggregate_function_name(name)
                .then_some(function)
        }) else {
            return Ok(ReturnExpressionKind::Value(self.parse_scalar_expression()?));
        };
        // Historical fixed shapes keep their dedicated AST variants (and thus
        // their logical plans); any other argument uses the general form.
        let checkpoint = self.checkpoint();
        match self.parse_fixed_aggregate(function) {
            Ok(Some(aggregate)) => return Ok(ReturnExpressionKind::Aggregate(aggregate)),
            Ok(None) | Err(_) => self.restore(checkpoint),
        }
        self.expect_char('(')?;
        let distinct = self.consume_keyword("DISTINCT");
        if function == AggregateFunctionName::Count && self.consume_char('*') {
            if distinct {
                return Err(self.error("COUNT(DISTINCT *) is not supported"));
            }
            self.expect_char(')')?;
            return Ok(ReturnExpressionKind::Aggregate(
                AggregateExpression::CountAll,
            ));
        }
        let argument = self.parse_pipeline_arithmetic(false)?;
        self.expect_char(')')?;
        Ok(ReturnExpressionKind::Aggregate(
            AggregateExpression::Expression {
                function,
                distinct,
                argument: Box::new(argument),
            },
        ))
    }

    /// Parses the historical `variable[.property]` aggregate shapes, returning
    /// `None` (or an error) when the argument needs the general form.
    fn parse_fixed_aggregate(
        &mut self,
        function: AggregateFunctionName,
    ) -> Result<Option<AggregateExpression>> {
        self.expect_char('(')?;
        let distinct = matches!(
            function,
            AggregateFunctionName::Count | AggregateFunctionName::Collect
        ) && self.consume_keyword("DISTINCT");
        self.skip_ws();
        if ["true", "false", "null"]
            .into_iter()
            .any(|keyword| self.next_keyword_is(keyword))
        {
            return Ok(None);
        }
        let variable = self.parse_ident()?;
        let property = if self.consume_char('.') {
            Some(self.parse_ident()?)
        } else {
            None
        };
        if !self.consume_char(')') {
            return Ok(None);
        }
        Ok(match (function, property) {
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
            (
                AggregateFunctionName::Sum
                | AggregateFunctionName::Min
                | AggregateFunctionName::Avg,
                _,
            ) => None,
        })
    }

    fn consume_aggregate_function_name(&mut self, name: &str) -> bool {
        let checkpoint = self.checkpoint();
        if self.consume_keyword(name) {
            self.skip_ws();
            if self.peek_char() == Some('(') {
                return true;
            }
        }
        self.restore(checkpoint);
        false
    }

    pub(super) fn parse_order_items(&mut self) -> Result<Vec<OrderItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let expression_start = self.checkpoint();
            if self.peek_char() == Some('[') {
                let expression = OrderExpression::Value(self.parse_scalar_expression()?);
                items.push(self.parse_order_direction(expression, expression_start.pos));
                self.skip_ws();
                if !self.consume_char(',') {
                    break;
                }
                continue;
            }
            let first = self.parse_ident()?;
            let expression = if first.eq_ignore_ascii_case("count") && self.consume_char('(') {
                let distinct = self.consume_keyword("DISTINCT");
                let name = if self.consume_char('*') {
                    if distinct {
                        return Err(self.error("COUNT(DISTINCT *) is not supported"));
                    }
                    "count(*)".to_string()
                } else {
                    let variable = self.parse_ident()?;
                    if self.consume_char('.') {
                        let property = self.parse_ident()?;
                        if distinct {
                            format!("count(DISTINCT {variable}.{property})")
                        } else {
                            format!("count({variable}.{property})")
                        }
                    } else if distinct {
                        format!("count(DISTINCT {variable})")
                    } else {
                        format!("count({variable})")
                    }
                };
                self.expect_char(')')?;
                OrderExpression::Column(name)
            } else if first.eq_ignore_ascii_case("id") && self.consume_char('(') {
                let variable = self.parse_ident()?;
                self.expect_char(')')?;
                OrderExpression::Id { variable }
            } else if first.eq_ignore_ascii_case("case")
                || ((matches_ignore_ascii_case(
                    &first,
                    &[
                        "coalesce",
                        "left",
                        "lower",
                        "json_value",
                        "length",
                        "nodes",
                        "relationships",
                    ],
                ) || hawdb_core::ListFunction::from_function_name(&first).is_some())
                    && self.peek_char() == Some('('))
            {
                self.restore(expression_start);
                OrderExpression::Value(self.parse_scalar_expression()?)
            } else if self.consume_char('.') {
                let property = self.parse_ident()?;
                self.skip_ws();
                if self.peek_char() == Some('[') {
                    self.restore(expression_start);
                    OrderExpression::Value(self.parse_scalar_expression()?)
                } else {
                    OrderExpression::Property {
                        variable: first,
                        property,
                    }
                }
            } else if self.peek_char() == Some('[') {
                self.restore(expression_start);
                OrderExpression::Value(self.parse_scalar_expression()?)
            } else {
                OrderExpression::Column(first)
            };
            items.push(self.parse_order_direction(expression, expression_start.pos));
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_order_direction(&mut self, expression: OrderExpression, start: usize) -> OrderItem {
        let direction = if self.consume_keyword("DESC") {
            OrderDirection::Desc
        } else {
            let _ = self.consume_keyword("ASC");
            OrderDirection::Asc
        };
        self.source_node(
            OrderItemKind {
                expression,
                direction,
            },
            start,
        )
    }

    pub(super) fn parse_scalar_expression(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let kind = self.with_recursion(|parser| parser.parse_scalar_expression_inner())?;
        let mut expression = self.source_node(kind, start);
        // Postfix `[index]` and `[start..end]` bind tighter than any operator.
        // Each one nests the AST, so it counts against the recursion budget.
        let base_depth = self.recursion_depth;
        let result = (|| {
            while self.consume_char('[') {
                if self.recursion_depth >= super::MAX_CYPHER_PARSER_DEPTH {
                    return Err(self.error(&format!(
                        "Cypher parser nesting exceeds limit of {}",
                        super::MAX_CYPHER_PARSER_DEPTH
                    )));
                }
                self.recursion_depth += 1;
                let kind = self.parse_list_postfix(expression)?;
                expression = self.source_node(kind, start);
            }
            Ok(expression)
        })();
        self.recursion_depth = base_depth;
        result
    }

    /// Parses the remainder of `list[index]` or `list[start..end]` after `[`.
    fn parse_list_postfix(&mut self, list: ScalarExpression) -> Result<ScalarExpressionKind> {
        let list = Box::new(list);
        if self.consume_token("..") {
            let end = if self.consume_char(']') {
                None
            } else {
                let end = self.parse_case_scalar()?;
                self.expect_char(']')?;
                Some(Box::new(end))
            };
            return Ok(ScalarExpressionKind::ListSlice {
                list,
                start: None,
                end,
            });
        }
        let first = Box::new(self.parse_case_scalar()?);
        if self.consume_token("..") {
            let end = if self.consume_char(']') {
                None
            } else {
                let end = self.parse_case_scalar()?;
                self.expect_char(']')?;
                Some(Box::new(end))
            };
            return Ok(ScalarExpressionKind::ListSlice {
                list,
                start: Some(first),
                end,
            });
        }
        self.expect_char(']')?;
        Ok(ScalarExpressionKind::ListIndex { list, index: first })
    }

    /// Returns true when the cursor is at `[identifier IN`.
    pub(super) fn next_is_list_comprehension(&mut self) -> bool {
        let checkpoint = self.checkpoint();
        let result = self.consume_char('[')
            && self.parse_ident().is_ok_and(|variable| {
                !["true", "false", "null"]
                    .iter()
                    .any(|keyword| variable.eq_ignore_ascii_case(keyword))
            })
            && self.consume_keyword("IN");
        self.restore(checkpoint);
        result
    }

    fn parse_list_comprehension(&mut self) -> Result<ScalarExpressionKind> {
        self.expect_char('[')?;
        let variable = self.parse_ident()?;
        self.expect_keyword("IN")?;
        let list = Box::new(self.parse_case_scalar()?);
        let predicate = if self.consume_keyword("WHERE") {
            Some(Box::new(self.parse_case_or()?))
        } else {
            None
        };
        let projection = if self.consume_char('|') {
            Some(Box::new(self.parse_case_scalar()?))
        } else {
            None
        };
        self.expect_char(']')?;
        Ok(ScalarExpressionKind::ListComprehension {
            variable,
            list,
            predicate,
            projection,
        })
    }

    /// Parses `[element, ...]`. A list of constants and parameters stays a
    /// value so it keeps value-list predicates and pushdown; any row-bound
    /// element makes it a per-row [`ScalarExpressionKind::List`].
    fn parse_list_literal(&mut self) -> Result<ScalarExpressionKind> {
        let checkpoint = self.checkpoint();
        if let Ok(value) = self.parse_value()
            && !value.kind.reads_row_binding()
        {
            return Ok(ScalarExpressionKind::Value(value));
        }
        self.restore(checkpoint);
        self.expect_char('[')?;
        let mut elements = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char(']') {
                break;
            }
            elements.push(self.parse_case_or()?);
            if self.consume_separator_or_end(',', ']')? {
                break;
            }
        }
        Ok(ScalarExpressionKind::List(elements))
    }

    /// Parses the arguments of a list function after its opening parenthesis.
    fn parse_list_function_arguments(
        &mut self,
        function: hawdb_core::ListFunction,
    ) -> Result<ScalarExpressionKind> {
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
        self.check_list_function_arity(function, arguments.len())?;
        Ok(ScalarExpressionKind::ListFunction {
            function,
            arguments,
        })
    }

    pub(super) fn check_list_function_arity(
        &self,
        function: hawdb_core::ListFunction,
        count: usize,
    ) -> Result<()> {
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

    fn parse_scalar_expression_inner(&mut self) -> Result<ScalarExpressionKind> {
        self.skip_ws();
        if self.next_is_list_comprehension() {
            return self.parse_list_comprehension();
        }
        if self.peek_char() == Some('[') {
            return self.parse_list_literal();
        }
        if matches!(
            self.peek_char(),
            Some('$' | '[' | '\'' | '"' | '-' | '0'..='9')
        ) || self.next_keyword_is("true")
            || self.next_keyword_is("false")
            || self.next_keyword_is("null")
        {
            return self.parse_value().map(ScalarExpressionKind::Value);
        }

        let variable = self.parse_ident()?;
        if variable.eq_ignore_ascii_case("id") && self.consume_char('(') {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::Id(variable));
        }
        if let Some(function) = hawdb_core::ListFunction::from_function_name(&variable)
            && self.consume_char('(')
        {
            return self.parse_list_function_arguments(function);
        }
        if (variable.eq_ignore_ascii_case("label") || variable.eq_ignore_ascii_case("type"))
            && self.consume_char('(')
        {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::RelationshipType(variable));
        }
        if let Some(metric) = hawdb_core::VectorDistance::from_function_name(&variable)
            && self.consume_char('(')
        {
            let left = Box::new(self.parse_scalar_expression()?);
            self.expect_char(',')?;
            let right = Box::new(self.parse_scalar_expression()?);
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::VectorDistance {
                metric,
                left,
                right,
            });
        }
        if variable.eq_ignore_ascii_case("coalesce") && self.consume_char('(') {
            let mut expressions = Vec::new();
            loop {
                expressions.push(self.parse_scalar_expression()?);
                self.skip_ws();
                if self.consume_char(')') {
                    break;
                }
                self.expect_char(',')?;
            }
            return Ok(ScalarExpressionKind::Coalesce(expressions));
        }
        if variable.eq_ignore_ascii_case("json_value") && self.consume_char('(') {
            let expression = self.parse_scalar_expression()?;
            self.expect_char(',')?;
            let path = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::JsonValue {
                expression: Box::new(expression),
                path,
            });
        }
        if variable.eq_ignore_ascii_case("left") && self.consume_char('(') {
            let expression = self.parse_scalar_expression()?;
            self.expect_char(',')?;
            let length = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::Left {
                expression: Box::new(expression),
                length,
            });
        }
        if let Some(path) = self.parse_path_function(&variable)? {
            return Ok(ScalarExpressionKind::Path(path));
        }
        if variable.eq_ignore_ascii_case("lower") && self.consume_char('(') {
            let expression = self.parse_scalar_expression()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::Lower(Box::new(expression)));
        }
        if variable.eq_ignore_ascii_case("date_part") && self.consume_char('(') {
            let part = match self.parse_value()?.kind {
                ValueExpressionKind::Literal(hawdb_core::Value::String(part)) => part,
                _ => return Err(self.error("date_part part requires a string literal")),
            };
            self.expect_char(',')?;
            let date_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let date_property = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::DatePart {
                part,
                variable: date_variable,
                property: date_property,
            });
        }
        if variable.eq_ignore_ascii_case("case") {
            let start = self.checkpoint();
            if let Ok(expression) = self.parse_case_lower_property_default_expression() {
                return Ok(expression);
            }
            self.restore(start);
            if let Ok(expression) = self.parse_default_if_null_or_eq_expression() {
                return Ok(expression);
            }
            self.restore(start);
            if let Ok(expression) = self.parse_case_coalesce_difference_floor_zero_expression() {
                return Ok(expression);
            }
            self.restore(start);
            if let Ok(expression) = self.parse_case_property_equals_rank_expression() {
                return Ok(expression);
            }
            self.restore(start);
            if let Ok(expression) = self.parse_case_property_not_null_or_eq_expression() {
                return Ok(expression);
            }
            self.restore(start);
            return self.parse_case_expression();
        }
        if self.consume_char('.') {
            Ok(ScalarExpressionKind::Property {
                variable,
                property: self.parse_ident()?,
            })
        } else {
            Ok(ScalarExpressionKind::Variable(variable))
        }
    }

    /// Parses the remainder of `length(p)`, `nodes(p)`, `relationships(p)` or
    /// `properties(nodes(p), 'key')` after the function name.
    fn parse_path_function(&mut self, name: &str) -> Result<Option<ShortestPathReturnExpression>> {
        let checkpoint = self.checkpoint();
        if !matches_ignore_ascii_case(name, &["length", "nodes", "relationships", "properties"])
            || !self.consume_char('(')
        {
            return Ok(None);
        }
        let path = if name.eq_ignore_ascii_case("properties") {
            if !self.consume_keyword("nodes") || !self.consume_char('(') {
                // `properties(n)` over a node or relationship is not a path function.
                self.restore(checkpoint);
                return Ok(None);
            }
            let path_variable = self.parse_ident()?;
            self.expect_char(')')?;
            self.expect_char(',')?;
            let property = match self.parse_value()?.kind {
                ValueExpressionKind::Literal(hawdb_core::Value::String(property)) => property,
                _ => return Err(self.error("node property projection requires a string key")),
            };
            ShortestPathReturnExpression::NodePropertyList {
                path_variable,
                property,
            }
        } else {
            let path_variable = self.parse_ident()?;
            if name.eq_ignore_ascii_case("length") {
                ShortestPathReturnExpression::Length { path_variable }
            } else if name.eq_ignore_ascii_case("nodes") {
                ShortestPathReturnExpression::Nodes { path_variable }
            } else {
                ShortestPathReturnExpression::Relationships { path_variable }
            }
        };
        self.expect_char(')')?;
        Ok(Some(path))
    }

    /// A value in a specialized CASE shape. Those shapes bind their values
    /// at planning time, so a row-bound list falls back to the generic CASE.
    fn parse_case_constant(&mut self) -> Result<ValueExpression> {
        let value = self.parse_value()?;
        if value.kind.reads_row_binding() {
            return Err(self.error("expected a constant CASE value"));
        }
        Ok(value)
    }

    fn parse_case_lower_property_default_expression(&mut self) -> Result<ScalarExpressionKind> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NOT")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("THEN")?;
        let function = self.parse_ident()?;
        if !function.eq_ignore_ascii_case("lower") {
            return Err(self.error("CASE lower-default expression THEN requires lower"));
        }
        self.expect_char('(')?;
        let lower_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let lower_property = self.parse_ident()?;
        self.expect_char(')')?;
        if lower_variable != variable || lower_property != property {
            return Err(self.error("CASE lower-default expression must lower the checked property"));
        }
        self.expect_keyword("ELSE")?;
        let default = self.parse_case_constant()?;
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        })
    }

    fn parse_default_if_null_or_eq_expression(&mut self) -> Result<ScalarExpressionKind> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("OR")?;
        let eq_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let eq_property = self.parse_ident()?;
        if eq_variable != variable || eq_property != property {
            return Err(self.error("CASE expression supports only one normalized property"));
        }
        self.expect_char('=')?;
        let empty = self.parse_case_constant()?;
        self.expect_keyword("THEN")?;
        let default = self.parse_case_constant()?;
        self.expect_keyword("ELSE")?;
        let else_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let else_property = self.parse_ident()?;
        if else_variable != variable || else_property != property {
            return Err(self.error("CASE expression ELSE must return the normalized property"));
        }
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        })
    }

    fn parse_case_property_not_null_or_eq_expression(&mut self) -> Result<ScalarExpressionKind> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NOT")?;
        self.expect_keyword("NULL")?;
        if !self.consume_keyword("AND") {
            self.expect_keyword("THEN")?;
            let then_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let then_property = self.parse_ident()?;
            if then_variable != variable || then_property != property {
                return Err(self.error("CASE sort expression THEN must return the same property"));
            }
            self.expect_keyword("ELSE")?;
            let default = self.parse_case_constant()?;
            self.expect_keyword("END")?;
            return Ok(ScalarExpressionKind::DefaultIfNull {
                variable,
                property,
                default,
            });
        }
        let neq_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let neq_property = self.parse_ident()?;
        if neq_variable != variable || neq_property != property {
            return Err(self.error("CASE sort expression supports only one property"));
        }
        if !self.consume_token("<>") && !self.consume_token("!=") {
            return Err(self.error("expected CASE sort expression inequality"));
        }
        let empty = self.parse_case_constant()?;
        self.expect_keyword("THEN")?;
        let non_empty = self.parse_case_constant()?;
        self.expect_keyword("ELSE")?;
        let null_or_empty = self.parse_case_constant()?;
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::CasePropertyNotNullOrEq {
            variable,
            property,
            empty: Box::new(empty),
            non_empty: Box::new(non_empty),
            null_or_empty: Box::new(null_or_empty),
        })
    }

    fn parse_case_property_equals_rank_expression(&mut self) -> Result<ScalarExpressionKind> {
        self.expect_keyword("WHEN")?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char('=')?;
        let first_match = self.parse_case_constant()?;
        self.expect_keyword("THEN")?;
        let first_rank = self.parse_case_constant()?;
        let mut branches = vec![(first_match, first_rank)];

        while self.consume_keyword("WHEN") {
            let branch_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let branch_property = self.parse_ident()?;
            if branch_variable != variable || branch_property != property {
                return Err(self.error("CASE rank expression supports only one property"));
            }
            self.expect_char('=')?;
            let branch_match = self.parse_case_constant()?;
            self.expect_keyword("THEN")?;
            let branch_rank = self.parse_case_constant()?;
            branches.push((branch_match, branch_rank));
        }

        self.expect_keyword("ELSE")?;
        let default = self.parse_case_constant()?;
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        })
    }

    fn parse_case_coalesce_difference_floor_zero_expression(
        &mut self,
    ) -> Result<ScalarExpressionKind> {
        self.expect_keyword("WHEN")?;
        let (variable, when_terms) = self.parse_coalesce_difference_terms()?;
        self.skip_ws();
        if !self.consume_char('<') {
            return Err(self.error("expected CASE floor expression '<'"));
        }
        let zero = self.parse_case_constant()?;
        if zero.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(0)) {
            return Err(self.error("CASE floor expression only supports zero lower bound"));
        }
        self.expect_keyword("THEN")?;
        let then_zero = self.parse_case_constant()?;
        if then_zero.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(0)) {
            return Err(self.error("CASE floor expression THEN must be zero"));
        }
        self.expect_keyword("ELSE")?;
        let (else_variable, else_terms) = self.parse_coalesce_difference_terms()?;
        if else_variable != variable || else_terms != when_terms {
            return Err(self.error("CASE floor expression ELSE must repeat the difference"));
        }
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::CaseCoalesceDifferenceFloorZero {
            variable,
            terms: when_terms,
        })
    }

    fn parse_coalesce_difference_terms(&mut self) -> Result<(String, Vec<CoalesceDifferenceTerm>)> {
        let (variable, first) = self.parse_coalesce_difference_term()?;
        let mut terms = vec![first];
        loop {
            self.skip_ws();
            if !self.consume_char('-') {
                break;
            }
            let (next_variable, term) = self.parse_coalesce_difference_term()?;
            if next_variable != variable {
                return Err(self.error("CASE floor expression supports only one variable"));
            }
            terms.push(term);
        }
        if terms.len() < 2 {
            return Err(self.error("CASE floor expression requires a difference"));
        }
        Ok((variable, terms))
    }

    fn parse_coalesce_difference_term(&mut self) -> Result<(String, CoalesceDifferenceTerm)> {
        self.skip_ws();
        self.expect_keyword("COALESCE")?;
        self.expect_char('(')?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char(',')?;
        let default = self.parse_case_constant()?;
        self.expect_char(')')?;
        Ok((variable, CoalesceDifferenceTerm { property, default }))
    }
}

fn matches_ignore_ascii_case(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}
