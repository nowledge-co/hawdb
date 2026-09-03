use skein_core::Result;

use super::super::ast::*;
use super::Parser;

impl Parser<'_> {
    pub(super) fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let expression = if self.consume_keyword("COUNT") {
                self.expect_char('(')?;
                let distinct = self.consume_keyword("DISTINCT");
                let expression = if self.consume_char('*') {
                    if distinct {
                        return Err(self.error("COUNT(DISTINCT *) is not supported"));
                    }
                    ReturnExpression::CountAll
                } else {
                    let variable = self.parse_ident()?;
                    if self.consume_char('.') {
                        ReturnExpression::CountProperty {
                            variable,
                            property: self.parse_ident()?,
                            distinct,
                        }
                    } else {
                        ReturnExpression::CountVariable { variable, distinct }
                    }
                };
                self.expect_char(')')?;
                expression
            } else if self.consume_keyword("MIN") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::MinProperty { variable, property }
            } else if self.consume_keyword("MAX") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::MaxProperty { variable, property }
            } else if self.consume_keyword("AVG") {
                self.expect_char('(')?;
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                self.expect_char(')')?;
                ReturnExpression::AvgProperty { variable, property }
            } else if self.consume_keyword("COLLECT") {
                self.expect_char('(')?;
                let distinct = self.consume_keyword("DISTINCT");
                let variable = self.parse_ident()?;
                if self.consume_char('.') {
                    let property = self.parse_ident()?;
                    self.expect_char(')')?;
                    ReturnExpression::CollectProperty {
                        variable,
                        property,
                        distinct,
                    }
                } else {
                    self.expect_char(')')?;
                    ReturnExpression::CollectVariable { variable, distinct }
                }
            } else {
                self.parse_return_projection_expression()?
            };
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(ReturnItem { expression, alias });
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    pub(super) fn parse_order_items(&mut self) -> Result<Vec<OrderItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
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
                self.pos -= first.len();
                OrderExpression::Value(self.parse_return_value_expression()?)
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
            items.push(OrderItem {
                expression,
                direction,
            });
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_return_projection_expression(&mut self) -> Result<ReturnExpression> {
        let expression = self.parse_return_value_expression()?;
        Ok(match expression {
            ReturnValueExpression::Variable(variable) => ReturnExpression::Variable(variable),
            ReturnValueExpression::Property { variable, property } => {
                ReturnExpression::Property { variable, property }
            }
            ReturnValueExpression::Id(variable) => ReturnExpression::Id(variable),
            ReturnValueExpression::RelationshipType(variable) => {
                ReturnExpression::RelationshipType(variable)
            }
            ReturnValueExpression::Coalesce(expressions) => ReturnExpression::Coalesce(expressions),
            ReturnValueExpression::Left { expression, length } => {
                ReturnExpression::Left { expression, length }
            }
            ReturnValueExpression::Lower(expression) => ReturnExpression::Lower(expression),
            ReturnValueExpression::DatePart {
                part,
                variable,
                property,
            } => ReturnExpression::DatePart {
                part,
                variable,
                property,
            },
            ReturnValueExpression::DefaultIfNullOrEq {
                variable,
                property,
                empty,
                default,
            } => ReturnExpression::DefaultIfNullOrEq {
                variable,
                property,
                empty,
                default,
            },
            ReturnValueExpression::DefaultIfNull {
                variable,
                property,
                default,
            } => ReturnExpression::DefaultIfNull {
                variable,
                property,
                default,
            },
            ReturnValueExpression::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            } => ReturnExpression::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            },
            ReturnValueExpression::CasePropertyEqualsRank {
                variable,
                property,
                branches,
                default,
            } => ReturnExpression::CasePropertyEqualsRank {
                variable,
                property,
                branches,
                default,
            },
            ReturnValueExpression::CaseLowerPropertyDefault {
                variable,
                property,
                default,
            } => ReturnExpression::CaseLowerPropertyDefault {
                variable,
                property,
                default,
            },
            ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
                ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, terms }
            }
            ReturnValueExpression::CaseEntitySearchRank(expression) => {
                ReturnExpression::CaseEntitySearchRank(expression)
            }
            ReturnValueExpression::CaseColumnSearchRank(expression) => {
                ReturnExpression::CaseColumnSearchRank(expression)
            }
            ReturnValueExpression::Value(value) => ReturnExpression::Value(value),
        })
    }

    pub(super) fn parse_return_value_expression(&mut self) -> Result<ReturnValueExpression> {
        self.with_recursion(|parser| parser.parse_return_value_expression_inner())
    }

    fn parse_return_value_expression_inner(&mut self) -> Result<ReturnValueExpression> {
        self.skip_ws();
        if matches!(
            self.peek_char(),
            Some('$' | '[' | '\'' | '"' | '-' | '0'..='9')
        ) || self.next_keyword_is("true")
            || self.next_keyword_is("false")
            || self.next_keyword_is("null")
        {
            return self.parse_value().map(ReturnValueExpression::Value);
        }

        let variable = self.parse_ident()?;
        if variable.eq_ignore_ascii_case("id") && self.consume_char('(') {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Id(variable));
        }
        if (variable.eq_ignore_ascii_case("label") || variable.eq_ignore_ascii_case("type"))
            && self.consume_char('(')
        {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::RelationshipType(variable));
        }
        if variable.eq_ignore_ascii_case("coalesce") && self.consume_char('(') {
            let mut expressions = Vec::new();
            loop {
                expressions.push(self.parse_return_value_expression()?);
                self.skip_ws();
                if self.consume_char(')') {
                    break;
                }
                self.expect_char(',')?;
            }
            return Ok(ReturnValueExpression::Coalesce(expressions));
        }
        if variable.eq_ignore_ascii_case("left") && self.consume_char('(') {
            let expression = self.parse_return_value_expression()?;
            self.expect_char(',')?;
            let length = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Left {
                expression: Box::new(expression),
                length,
            });
        }
        if variable.eq_ignore_ascii_case("lower") && self.consume_char('(') {
            let expression = self.parse_return_value_expression()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::Lower(Box::new(expression)));
        }
        if variable.eq_ignore_ascii_case("date_part") && self.consume_char('(') {
            let part = match self.parse_value()? {
                ValueExpression::Literal(skein_core::Value::String(part)) => part,
                _ => return Err(self.error("date_part part requires a string literal")),
            };
            self.expect_char(',')?;
            let date_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let date_property = self.parse_ident()?;
            self.expect_char(')')?;
            return Ok(ReturnValueExpression::DatePart {
                part,
                variable: date_variable,
                property: date_property,
            });
        }
        if variable.eq_ignore_ascii_case("case") {
            let start = self.pos;
            if let Ok(expression) = self.parse_case_entity_search_rank_expression() {
                return Ok(expression);
            }
            self.pos = start;
            if let Ok(expression) = self.parse_case_column_search_rank_expression() {
                return Ok(expression);
            }
            self.pos = start;
            if let Ok(expression) = self.parse_case_lower_property_default_expression() {
                return Ok(expression);
            }
            self.pos = start;
            if let Ok(expression) = self.parse_default_if_null_or_eq_expression() {
                return Ok(expression);
            }
            self.pos = start;
            if let Ok(expression) = self.parse_case_coalesce_difference_floor_zero_expression() {
                return Ok(expression);
            }
            self.pos = start;
            if let Ok(expression) = self.parse_case_property_equals_rank_expression() {
                return Ok(expression);
            }
            self.pos = start;
            return self.parse_case_property_not_null_or_eq_expression();
        }
        if self.consume_char('.') {
            Ok(ReturnValueExpression::Property {
                variable,
                property: self.parse_ident()?,
            })
        } else {
            Ok(ReturnValueExpression::Variable(variable))
        }
    }

    fn parse_case_lower_property_default_expression(&mut self) -> Result<ReturnValueExpression> {
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
        Ok(ReturnValueExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        })
    }

    fn parse_case_column_search_rank_expression(&mut self) -> Result<ReturnValueExpression> {
        self.expect_keyword("WHEN")?;
        let (column, raw_query) = self.parse_column_equals_value()?;
        self.expect_keyword("THEN")?;
        let exact_rank = self.parse_value()?;

        self.expect_keyword("WHEN")?;
        let (normalized_column, normalized_query) = self.parse_column_equals_value()?;
        if normalized_column != column {
            return Err(self.error("CASE column search rank must compare one column"));
        }
        self.expect_keyword("THEN")?;
        let normalized_rank = self.parse_value()?;
        if normalized_rank != exact_rank {
            return Err(self.error("CASE column search rank exact branches must share a rank"));
        }

        self.expect_keyword("WHEN")?;
        let (contains_column, raw_contains) = self.parse_column_contains_value()?;
        if contains_column != column || raw_contains != raw_query {
            return Err(self.error("CASE column search rank raw contains must match raw equality"));
        }
        self.expect_keyword("THEN")?;
        let contains_rank = self.parse_value()?;

        self.expect_keyword("WHEN")?;
        let (normalized_contains_column, normalized_contains) =
            self.parse_column_contains_value()?;
        if normalized_contains_column != column || normalized_contains != normalized_query {
            return Err(self.error(
                "CASE column search rank normalized contains must match normalized equality",
            ));
        }
        self.expect_keyword("THEN")?;
        let normalized_contains_rank = self.parse_value()?;
        if normalized_contains_rank != contains_rank {
            return Err(self.error("CASE column search rank contains branches must share a rank"));
        }
        self.expect_keyword("ELSE")?;
        let fallback_rank = self.parse_value()?;
        self.expect_keyword("END")?;

        Ok(ReturnValueExpression::CaseColumnSearchRank(Box::new(
            CaseColumnSearchRankExpression {
                column,
                raw_query,
                normalized_query,
                exact_rank,
                contains_rank,
                fallback_rank,
            },
        )))
    }

    fn parse_column_equals_value(&mut self) -> Result<(String, ValueExpression)> {
        let column = self.parse_ident()?;
        self.expect_char('=')?;
        let value = self.parse_value()?;
        Ok((column, value))
    }

    fn parse_column_contains_value(&mut self) -> Result<(String, ValueExpression)> {
        let column = self.parse_ident()?;
        self.expect_keyword("CONTAINS")?;
        let value = self.parse_value()?;
        Ok((column, value))
    }

    fn parse_case_entity_search_rank_expression(&mut self) -> Result<ReturnValueExpression> {
        self.expect_keyword("WHEN")?;
        let (variable, name_property, raw_query) = self.parse_lower_property_equals_value()?;
        self.expect_keyword("THEN")?;
        let exact_rank = self.parse_value()?;

        self.expect_keyword("WHEN")?;
        let (normalized_variable, normalized_name_property, normalized_query) =
            self.parse_lower_property_equals_value()?;
        if normalized_variable != variable || normalized_name_property != name_property {
            return Err(self.error("CASE entity search rank must compare one name property"));
        }
        self.expect_keyword("THEN")?;
        let normalized_rank = self.parse_value()?;
        if normalized_rank != exact_rank {
            return Err(self.error("CASE entity search rank exact branches must share a rank"));
        }

        self.expect_keyword("WHEN")?;
        let (aliases_variable, aliases_property, raw_input) =
            self.parse_list_contains_property_value()?;
        if aliases_variable != variable {
            return Err(self.error("CASE entity search rank must compare one variable"));
        }
        self.expect_keyword("THEN")?;
        let alias_rank = self.parse_value()?;
        self.expect_keyword("ELSE")?;
        let fallback_rank = self.parse_value()?;
        self.expect_keyword("END")?;

        Ok(ReturnValueExpression::CaseEntitySearchRank(Box::new(
            CaseEntitySearchRankExpression {
                variable,
                name_property,
                aliases_property,
                raw_query,
                normalized_query,
                raw_input,
                exact_rank,
                alias_rank,
                fallback_rank,
            },
        )))
    }

    fn parse_lower_property_equals_value(&mut self) -> Result<(String, String, ValueExpression)> {
        let function = self.parse_ident()?;
        if !function.eq_ignore_ascii_case("lower") {
            return Err(self.error("expected LOWER in CASE entity search rank"));
        }
        self.expect_char('(')?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_char('=')?;
        let value = self.parse_value()?;
        Ok((variable, property, value))
    }

    fn parse_list_contains_property_value(&mut self) -> Result<(String, String, ValueExpression)> {
        let function = self.parse_ident()?;
        if !function.eq_ignore_ascii_case("list_contains") {
            return Err(self.error("expected list_contains in CASE entity search rank"));
        }
        self.expect_char('(')?;
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char(',')?;
        let value = self.parse_value()?;
        self.expect_char(')')?;
        Ok((variable, property, value))
    }

    fn parse_default_if_null_or_eq_expression(&mut self) -> Result<ReturnValueExpression> {
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
        Ok(ReturnValueExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        })
    }

    fn parse_case_property_not_null_or_eq_expression(&mut self) -> Result<ReturnValueExpression> {
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
            return Ok(ReturnValueExpression::DefaultIfNull {
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
        Ok(ReturnValueExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        })
    }

    fn parse_case_property_equals_rank_expression(&mut self) -> Result<ReturnValueExpression> {
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
        Ok(ReturnValueExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        })
    }

    fn parse_case_coalesce_difference_floor_zero_expression(
        &mut self,
    ) -> Result<ReturnValueExpression> {
        self.expect_keyword("WHEN")?;
        let (variable, when_terms) = self.parse_coalesce_difference_terms()?;
        self.skip_ws();
        if !self.consume_char('<') {
            return Err(self.error("expected CASE floor expression '<'"));
        }
        let zero = self.parse_value()?;
        if zero != ValueExpression::Literal(skein_core::Value::Int(0)) {
            return Err(self.error("CASE floor expression only supports zero lower bound"));
        }
        self.expect_keyword("THEN")?;
        let then_zero = self.parse_value()?;
        if then_zero != ValueExpression::Literal(skein_core::Value::Int(0)) {
            return Err(self.error("CASE floor expression THEN must be zero"));
        }
        self.expect_keyword("ELSE")?;
        let (else_variable, else_terms) = self.parse_coalesce_difference_terms()?;
        if else_variable != variable || else_terms != when_terms {
            return Err(self.error("CASE floor expression ELSE must repeat the difference"));
        }
        self.expect_keyword("END")?;
        Ok(ReturnValueExpression::CaseCoalesceDifferenceFloorZero {
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
