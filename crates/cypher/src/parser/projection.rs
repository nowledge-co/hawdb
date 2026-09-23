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
    pub(super) fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let item_start = self.pos;
            let expression = self.parse_return_atom()?;
            let expression = self.source_node(expression, item_start);
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(self.source_node(ReturnItemKind { expression, alias }, item_start));
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    pub(super) fn parse_return_atom(&mut self) -> Result<ReturnExpressionKind> {
        Ok(if self.consume_aggregate_function_name("COUNT") {
            self.expect_char('(')?;
            let distinct = self.consume_keyword("DISTINCT");
            let expression = if self.consume_char('*') {
                if distinct {
                    return Err(self.error("COUNT(DISTINCT *) is not supported"));
                }
                ReturnExpressionKind::Aggregate(AggregateExpression::CountAll)
            } else {
                let variable = self.parse_ident()?;
                if self.consume_char('.') {
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountProperty {
                        variable,
                        property: self.parse_ident()?,
                        distinct,
                    })
                } else {
                    ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                        variable,
                        distinct,
                    })
                }
            };
            self.expect_char(')')?;
            expression
        } else if self.consume_aggregate_function_name("MIN") {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(')')?;
            ReturnExpressionKind::Aggregate(AggregateExpression::MinProperty { variable, property })
        } else if self.consume_aggregate_function_name("MAX") {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(')')?;
            ReturnExpressionKind::Aggregate(AggregateExpression::MaxProperty { variable, property })
        } else if self.consume_aggregate_function_name("AVG") {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(')')?;
            ReturnExpressionKind::Aggregate(AggregateExpression::AvgProperty { variable, property })
        } else if self.consume_aggregate_function_name("COLLECT") {
            self.expect_char('(')?;
            let distinct = self.consume_keyword("DISTINCT");
            let variable = self.parse_ident()?;
            if self.consume_char('.') {
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpressionKind::Aggregate(AggregateExpression::CollectProperty {
                    variable,
                    property,
                    distinct,
                })
            } else {
                self.expect_char(')')?;
                ReturnExpressionKind::Aggregate(AggregateExpression::CollectVariable {
                    variable,
                    distinct,
                })
            }
        } else {
            ReturnExpressionKind::Value(self.parse_scalar_expression()?)
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
                || (matches_ignore_ascii_case(&first, &["coalesce", "left", "lower"])
                    && self.peek_char() == Some('('))
            {
                self.restore(expression_start);
                OrderExpression::Value(self.parse_scalar_expression()?)
            } else if self.consume_char('.') {
                OrderExpression::Property {
                    variable: first,
                    property: self.parse_ident()?,
                }
            } else {
                OrderExpression::Column(first)
            };
            let direction = if self.consume_keyword("DESC") {
                OrderDirection::Desc
            } else {
                let _ = self.consume_keyword("ASC");
                OrderDirection::Asc
            };
            items.push(self.source_node(
                OrderItemKind {
                    expression,
                    direction,
                },
                expression_start.pos,
            ));
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    pub(super) fn parse_scalar_expression(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let kind = self.with_recursion(|parser| parser.parse_scalar_expression_inner())?;
        Ok(self.source_node(kind, start))
    }

    fn parse_scalar_expression_inner(&mut self) -> Result<ScalarExpressionKind> {
        self.skip_ws();
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
        if (variable.eq_ignore_ascii_case("label") || variable.eq_ignore_ascii_case("type"))
            && self.consume_char('(')
        {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ScalarExpressionKind::RelationshipType(variable));
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
        let default = self.parse_value()?;
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
        let empty = self.parse_value()?;
        self.expect_keyword("THEN")?;
        let default = self.parse_value()?;
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
            let default = self.parse_value()?;
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
        let empty = self.parse_value()?;
        self.expect_keyword("THEN")?;
        let non_empty = self.parse_value()?;
        self.expect_keyword("ELSE")?;
        let null_or_empty = self.parse_value()?;
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
        let first_match = self.parse_value()?;
        self.expect_keyword("THEN")?;
        let first_rank = self.parse_value()?;
        let mut branches = vec![(first_match, first_rank)];

        while self.consume_keyword("WHEN") {
            let branch_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let branch_property = self.parse_ident()?;
            if branch_variable != variable || branch_property != property {
                return Err(self.error("CASE rank expression supports only one property"));
            }
            self.expect_char('=')?;
            let branch_match = self.parse_value()?;
            self.expect_keyword("THEN")?;
            let branch_rank = self.parse_value()?;
            branches.push((branch_match, branch_rank));
        }

        self.expect_keyword("ELSE")?;
        let default = self.parse_value()?;
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
        let zero = self.parse_value()?;
        if zero.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(0)) {
            return Err(self.error("CASE floor expression only supports zero lower bound"));
        }
        self.expect_keyword("THEN")?;
        let then_zero = self.parse_value()?;
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
        let default = self.parse_value()?;
        self.expect_char(')')?;
        Ok((variable, CoalesceDifferenceTerm { property, default }))
    }
}

fn matches_ignore_ascii_case(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}
