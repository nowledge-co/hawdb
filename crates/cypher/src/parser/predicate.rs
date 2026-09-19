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

use std::collections::BTreeMap;

use hawdb_core::Result;

use super::super::ast::*;
use super::{keyword_matches, Parser};

type MatchRelationshipPattern = (
    Option<String>,
    String,
    BTreeMap<String, ValueExpression>,
    usize,
    usize,
);

enum PropertyPredicateRight {
    Value(ValueExpression),
    Expression(ScalarExpression),
}

impl Parser<'_> {
    pub(super) fn parse_property_predicate(&mut self) -> Result<PropertyPredicate> {
        self.parse_predicate(false)
    }

    pub(super) fn parse_predicate(&mut self, allow_columns: bool) -> Result<PropertyPredicate> {
        let mut predicates = vec![self.parse_property_conjunction(allow_columns)?];
        while self.consume_keyword("OR") {
            predicates.push(self.parse_property_conjunction(allow_columns)?);
        }
        if predicates.len() == 1 {
            Ok(predicates.remove(0))
        } else {
            Ok(PropertyPredicate::Or(predicates))
        }
    }

    fn parse_property_conjunction(&mut self, allow_columns: bool) -> Result<PropertyPredicate> {
        let mut predicates = vec![self.parse_property_predicate_atom(allow_columns)?];
        while self.consume_keyword("AND") {
            predicates.push(self.parse_property_predicate_atom(allow_columns)?);
        }
        if predicates.len() == 1 {
            Ok(predicates.remove(0))
        } else {
            Ok(PropertyPredicate::And(predicates))
        }
    }

    fn parse_property_predicate_atom(&mut self, allow_columns: bool) -> Result<PropertyPredicate> {
        self.with_recursion(|parser| parser.parse_property_predicate_atom_inner(allow_columns))
    }

    fn parse_property_predicate_atom_inner(
        &mut self,
        allow_columns: bool,
    ) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_keyword("NOT") {
            return Ok(PropertyPredicate::Not(Box::new(
                self.parse_property_predicate_atom(allow_columns)?,
            )));
        }
        if self.peek_char() == Some('(') && self.looks_like_relationship_exists_predicate() {
            return self.parse_relationship_exists_predicate();
        }
        if self.consume_keyword("EXISTS") {
            return self.parse_bound_relationship_exists_subquery();
        }
        if self.looks_like_parenthesized_scalar_expression_predicate() {
            self.expect_char('(')?;
            let expression = self.parse_scalar_expression()?;
            self.expect_char(')')?;
            return self.parse_expression_predicate(expression);
        }
        if self.consume_char('(') {
            let predicate = self.parse_predicate(allow_columns)?;
            self.expect_char(')')?;
            return Ok(predicate);
        }
        if self.consume_char('$') {
            let parameter = self.parse_ident()?;
            self.skip_ws();
            if self.consume_char('=') {
                return Ok(PropertyPredicate::ParameterEq {
                    left: parameter,
                    right: self.parse_value()?,
                });
            }
            if self.consume_token("<>") || self.consume_token("!=") {
                return Ok(PropertyPredicate::ParameterNotEq {
                    left: parameter,
                    right: self.parse_value()?,
                });
            }
            self.expect_keyword("IS")?;
            let is_not = self.consume_keyword("NOT");
            self.expect_keyword("NULL")?;
            return if is_not {
                Ok(PropertyPredicate::ParameterIsNotNull { parameter })
            } else {
                Ok(PropertyPredicate::ParameterIsNull { parameter })
            };
        }
        let expression_start = self.checkpoint();
        let variable = self.parse_ident()?;
        if variable.eq_ignore_ascii_case("list_contains") && self.peek_char() == Some('(') {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(',')?;
            let value = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(PropertyPredicate::ListContains {
                variable,
                property,
                value,
            });
        }
        if variable.eq_ignore_ascii_case("list_contains_lower") && self.peek_char() == Some('(') {
            self.expect_char('(')?;
            let variable = self.parse_ident()?;
            self.expect_char('.')?;
            let property = self.parse_ident()?;
            self.expect_char(',')?;
            let value = self.parse_value()?;
            self.expect_char(')')?;
            return Ok(PropertyPredicate::ListContainsLower {
                variable,
                property,
                value,
            });
        }
        if variable.eq_ignore_ascii_case("contains") && self.peek_char() == Some('(') {
            self.expect_char('(')?;
            let expression = self.parse_scalar_expression()?;
            self.expect_char(',')?;
            let value = self.parse_scalar_expression()?;
            self.expect_char(')')?;
            return Ok(PropertyPredicate::ExpressionContains { expression, value });
        }
        if matches_ignore_ascii_case(&variable, &["coalesce", "left", "lower", "case"])
            && self.peek_char() == Some('(')
        {
            self.restore(expression_start);
            let expression = self.parse_scalar_expression()?;
            return self.parse_expression_predicate(expression);
        }
        if variable.eq_ignore_ascii_case("case") {
            self.restore(expression_start);
            let expression = self.parse_scalar_expression()?;
            return self.parse_expression_predicate(expression);
        }
        if variable.eq_ignore_ascii_case("id") && self.consume_char('(') {
            let variable = self.parse_ident()?;
            self.expect_char(')')?;
            return self.parse_id_predicate(variable);
        }
        self.skip_ws();
        if allow_columns && self.peek_char() != Some('.') {
            let expression = self.source_node(
                ScalarExpressionKind::Variable(variable),
                expression_start.pos,
            );
            return self.parse_expression_predicate(expression);
        }
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        let property_span = self.source_span(expression_start.pos);
        self.skip_ws();
        if self.consume_keyword("IS") {
            let is_not = self.consume_keyword("NOT");
            if !self.consume_keyword("NULL") {
                return Err(self.error("expected NULL"));
            }
            if is_not {
                Ok(PropertyPredicate::IsNotNull { variable, property })
            } else {
                Ok(PropertyPredicate::IsNull { variable, property })
            }
        } else if self.consume_keyword("IN") {
            let values = self.parse_value()?;
            Ok(PropertyPredicate::In {
                variable,
                property,
                values,
            })
        } else if self.consume_keyword("CONTAINS") {
            let value = self.parse_value()?;
            Ok(PropertyPredicate::Contains {
                variable,
                property,
                value,
            })
        } else if self.consume_keyword("STARTS") {
            self.expect_keyword("WITH")?;
            let value = self.parse_value()?;
            Ok(PropertyPredicate::StartsWith {
                variable,
                property,
                value,
            })
        } else if self.consume_keyword("ENDS") {
            self.expect_keyword("WITH")?;
            let value = self.parse_value()?;
            Ok(PropertyPredicate::EndsWith {
                variable,
                property,
                value,
            })
        } else if self.consume_token("=~") {
            let pattern = self.parse_value()?;
            Ok(PropertyPredicate::RegexMatch {
                variable,
                property,
                pattern,
            })
        } else if self.consume_char('<') {
            if self.consume_char('>') {
                return match self.parse_property_predicate_right(allow_columns)? {
                    PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::NotEq {
                        variable,
                        property,
                        value,
                    }),
                    PropertyPredicateRight::Expression(value) => {
                        Ok(PropertyPredicate::ExpressionNotEq {
                            expression: AstNode::from_source(
                                ScalarExpressionKind::Property { variable, property },
                                property_span,
                            ),
                            value,
                        })
                    }
                };
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            match self.parse_property_predicate_right(allow_columns)? {
                PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::Compare {
                    variable,
                    property,
                    op,
                    value,
                }),
                PropertyPredicateRight::Expression(value) => {
                    Ok(PropertyPredicate::ExpressionCompare {
                        expression: AstNode::from_source(
                            ScalarExpressionKind::Property { variable, property },
                            property_span,
                        ),
                        op,
                        value,
                    })
                }
            }
        } else if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            match self.parse_property_predicate_right(allow_columns)? {
                PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::Compare {
                    variable,
                    property,
                    op,
                    value,
                }),
                PropertyPredicateRight::Expression(value) => {
                    Ok(PropertyPredicate::ExpressionCompare {
                        expression: AstNode::from_source(
                            ScalarExpressionKind::Property { variable, property },
                            property_span,
                        ),
                        op,
                        value,
                    })
                }
            }
        } else {
            self.expect_char('=')?;
            match self.parse_property_predicate_right(allow_columns)? {
                PropertyPredicateRight::Value(value) => Ok(PropertyPredicate::Eq {
                    variable,
                    property,
                    value,
                }),
                PropertyPredicateRight::Expression(value) => Ok(PropertyPredicate::ExpressionEq {
                    expression: AstNode::from_source(
                        ScalarExpressionKind::Property { variable, property },
                        property_span,
                    ),
                    value,
                }),
            }
        }
    }

    fn parse_property_predicate_right(
        &mut self,
        allow_columns: bool,
    ) -> Result<PropertyPredicateRight> {
        self.skip_ws();
        let value_start = self.checkpoint();
        if matches!(self.peek_char(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_') {
            let variable = self.parse_ident()?;
            if self.consume_char('.') {
                let property = self.parse_ident()?;
                return Ok(PropertyPredicateRight::Expression(self.source_node(
                    ScalarExpressionKind::Property { variable, property },
                    value_start.pos,
                )));
            }
            self.restore(value_start);
        }
        if allow_columns {
            let expression = self.parse_case_scalar()?;
            return Ok(match expression.kind {
                ScalarExpressionKind::Value(value) => PropertyPredicateRight::Value(value),
                _ => PropertyPredicateRight::Expression(expression),
            });
        }
        self.parse_value().map(PropertyPredicateRight::Value)
    }

    fn looks_like_relationship_exists_predicate(&self) -> bool {
        let bytes = self.input.as_bytes();
        let mut depth = 0usize;
        let mut index = self.pos;
        while index < bytes.len() {
            let ch = self.input[index..]
                .chars()
                .next()
                .expect("index stays on a char boundary");
            match ch {
                '(' => depth += 1,
                ')' => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                    if depth == 0 {
                        let after = index + ch.len_utf8();
                        let rest = &self.input[after..];
                        return rest.trim_start().starts_with("-[")
                            || rest.trim_start().starts_with("<-[");
                    }
                }
                _ => {}
            }
            index += ch.len_utf8();
        }
        false
    }

    fn looks_like_parenthesized_scalar_expression_predicate(&self) -> bool {
        let mut index = self.pos;
        while let Some(ch) = self.input[index..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            index += ch.len_utf8();
        }
        if !self.input[index..].starts_with('(') {
            return false;
        }
        index += '('.len_utf8();
        while let Some(ch) = self.input[index..].chars().next() {
            if !ch.is_whitespace() {
                break;
            }
            index += ch.len_utf8();
        }
        ["case", "coalesce", "left", "lower"]
            .iter()
            .any(|keyword| keyword_matches_at(self.input, index, keyword))
            && self.parenthesized_expression_is_followed_by_comparison(index)
    }

    fn parenthesized_expression_is_followed_by_comparison(&self, mut index: usize) -> bool {
        let mut depth = 1usize;
        while let Some(ch) = self.input[index..].chars().next() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let after = index + ch.len_utf8();
                        let rest = self.input[after..].trim_start();
                        return rest.starts_with('=')
                            || rest.starts_with('<')
                            || rest.starts_with('>')
                            || keyword_matches_at(rest, 0, "CONTAINS");
                    }
                }
                _ => {}
            }
            index += ch.len_utf8();
        }
        false
    }

    fn parse_relationship_exists_predicate(&mut self) -> Result<PropertyPredicate> {
        let (variable, _, _) = self.parse_match_node_pattern()?;
        let (direction, rel_type, target_label) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("relationship existence predicates support one-hop types"));
            }
            self.expect_char('-')?;
            let (_, target_label, target_properties) = self.parse_match_node_pattern()?;
            if !target_properties.is_empty() {
                return Err(self
                    .error("relationship existence predicates do not support target properties"));
            }
            (RelationshipDirection::Incoming, rel_type, target_label)
        } else {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("relationship existence predicates support one-hop types"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (_, target_label, target_properties) = self.parse_match_node_pattern()?;
            if !target_properties.is_empty() {
                return Err(self
                    .error("relationship existence predicates do not support target properties"));
            }
            (direction, rel_type, target_label)
        };
        Ok(PropertyPredicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        })
    }

    fn parse_bound_relationship_exists_subquery(&mut self) -> Result<PropertyPredicate> {
        self.expect_char('{')?;
        self.expect_keyword("MATCH")?;
        let (source_variable, source_label, source_properties) = self.parse_match_node_pattern()?;
        if !source_label.is_empty() || !source_properties.is_empty() {
            return Err(self.error("EXISTS relationship subqueries require bound source variables"));
        }
        let (direction, rel_type, target_variable) = if self.consume_char('<') {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("EXISTS relationship subqueries support one-hop types"));
            }
            self.expect_char('-')?;
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            if !target_label.is_empty() || !target_properties.is_empty() {
                return Err(
                    self.error("EXISTS relationship subqueries require bound target variables")
                );
            }
            (RelationshipDirection::Incoming, rel_type, target_variable)
        } else {
            self.expect_char('-')?;
            let (_, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            if !properties.is_empty() || min_hops != 1 || max_hops != 1 {
                return Err(self.error("EXISTS relationship subqueries support one-hop types"));
            }
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            if !target_label.is_empty() || !target_properties.is_empty() {
                return Err(
                    self.error("EXISTS relationship subqueries require bound target variables")
                );
            }
            (direction, rel_type, target_variable)
        };
        self.expect_char('}')?;
        Ok(PropertyPredicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        })
    }

    fn parse_expression_predicate(
        &mut self,
        expression: ScalarExpression,
    ) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_char('<') {
            if self.consume_char('>') {
                return Ok(PropertyPredicate::ExpressionNotEq {
                    expression,
                    value: self.parse_scalar_expression()?,
                });
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            return Ok(PropertyPredicate::ExpressionCompare {
                expression,
                op,
                value: self.parse_scalar_expression()?,
            });
        }
        if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            return Ok(PropertyPredicate::ExpressionCompare {
                expression,
                op,
                value: self.parse_scalar_expression()?,
            });
        }
        if self.consume_keyword("CONTAINS") {
            return Ok(PropertyPredicate::ExpressionContains {
                expression,
                value: self.parse_scalar_expression()?,
            });
        }
        self.expect_char('=')?;
        Ok(PropertyPredicate::ExpressionEq {
            expression,
            value: self.parse_scalar_expression()?,
        })
    }

    fn parse_id_predicate(&mut self, variable: String) -> Result<PropertyPredicate> {
        self.skip_ws();
        if self.consume_keyword("IN") {
            return Ok(PropertyPredicate::IdIn {
                variable,
                values: self.parse_value()?,
            });
        }
        if self.consume_char('<') {
            if self.consume_char('>') {
                return Ok(PropertyPredicate::IdNotEq {
                    variable,
                    value: self.parse_value()?,
                });
            }
            let op = if self.consume_char('=') {
                ComparisonOp::Lte
            } else {
                ComparisonOp::Lt
            };
            return Ok(PropertyPredicate::IdCompare {
                variable,
                op,
                value: self.parse_value()?,
            });
        }
        if self.consume_char('>') {
            let op = if self.consume_char('=') {
                ComparisonOp::Gte
            } else {
                ComparisonOp::Gt
            };
            return Ok(PropertyPredicate::IdCompare {
                variable,
                op,
                value: self.parse_value()?,
            });
        }
        self.expect_char('=')?;
        Ok(PropertyPredicate::IdEq {
            variable,
            value: self.parse_value()?,
        })
    }

    pub(super) fn parse_match_relationship_pattern(&mut self) -> Result<MatchRelationshipPattern> {
        self.parse_match_relationship_pattern_with_search(false)
            .map(|(pattern, _)| pattern)
    }

    pub(super) fn parse_match_relationship_pattern_with_search(
        &mut self,
        allow_shortest: bool,
    ) -> Result<(MatchRelationshipPattern, PathSearch)> {
        self.expect_char('[')?;
        let (variable, rel_type) = if self.consume_char(':') {
            (None, self.parse_ident()?)
        } else if self.peek_char() == Some(']') || (allow_shortest && self.peek_char() == Some('*'))
        {
            (None, String::new())
        } else {
            let variable = self.parse_ident()?;
            let rel_type = if self.consume_char(':') {
                self.parse_ident()?
            } else {
                String::new()
            };
            (Some(variable), rel_type)
        };
        let mut search = PathSearch::All;
        let (min_hops, max_hops) = if self.consume_char('*') {
            if allow_shortest && self.consume_keyword("ALL") {
                self.expect_keyword("SHORTEST")?;
                search = PathSearch::AllShortest;
            }
            self.parse_bounded_hops()?
        } else {
            (1, 1)
        };
        self.skip_ws();
        let properties = if self.peek_char() == Some('{') {
            self.parse_properties()?
        } else {
            BTreeMap::new()
        };
        self.expect_char(']')?;
        Ok(((variable, rel_type, properties, min_hops, max_hops), search))
    }

    pub(super) fn parse_bounded_hops(&mut self) -> Result<(usize, usize)> {
        self.skip_ws();
        let min = if matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
            Some(self.parse_usize()?)
        } else {
            None
        };
        if self.consume_char('.') {
            self.expect_char('.')?;
            let Some(max) = (if matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
                Some(self.parse_usize()?)
            } else {
                None
            }) else {
                return Err(self.error("bounded relationship pattern requires a finite max hop"));
            };
            let min = min.unwrap_or(1);
            if min > max {
                return Err(self.error("relationship min hop must not exceed max hop"));
            }
            return Ok((min, max));
        }
        let Some(exact) = min else {
            return Err(self.error("bounded relationship pattern requires a finite max hop"));
        };
        Ok((exact, exact))
    }

    pub(super) fn parse_set_properties(&mut self) -> Result<Vec<SetProperty>> {
        let mut sets = Vec::new();
        loop {
            sets.push(self.parse_set_property()?);
            self.skip_ws();
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(sets)
    }

    fn parse_set_property(&mut self) -> Result<SetProperty> {
        let variable = self.parse_ident()?;
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.skip_ws();
        self.expect_char('=')?;
        self.skip_ws();
        let value_start = self.checkpoint();
        let starts_case = self.next_keyword_is("CASE");
        let value = if starts_case {
            self.parse_case_set_value()?
        } else if self.consume_keyword("COALESCE") {
            self.expect_char('(')?;
            let expression_variable = self.parse_ident()?;
            self.expect_char('.')?;
            let expression_property = self.parse_ident()?;
            self.expect_char(',')?;
            let default = self.parse_value()?;
            self.expect_char(')')?;
            if self.consume_char('+') {
                SetValueExpression::CoalescePropertyAdd {
                    variable: expression_variable,
                    property: expression_property,
                    default,
                    value: self.parse_value()?,
                }
            } else {
                SetValueExpression::CoalesceProperty {
                    variable: expression_variable,
                    property: expression_property,
                    default,
                }
            }
        } else if self.peek_char().is_some_and(is_identifier_start) {
            let expression_variable = self.parse_ident()?;
            if self.consume_char('.') {
                let expression_property = self.parse_ident()?;
                self.skip_ws();
                if self.consume_char('+') {
                    SetValueExpression::PropertyAdd {
                        variable: expression_variable,
                        property: expression_property,
                        value: self.parse_value()?,
                    }
                } else {
                    SetValueExpression::Property {
                        variable: expression_variable,
                        property: expression_property,
                    }
                }
            } else {
                self.restore(value_start);
                SetValueExpression::Value(self.parse_value()?)
            }
        } else {
            SetValueExpression::Value(self.parse_value()?)
        };
        Ok(SetProperty {
            variable,
            property,
            value,
        })
    }

    fn parse_case_set_value(&mut self) -> Result<SetValueExpression> {
        self.expect_keyword("CASE")?;
        self.expect_keyword("WHEN")?;
        self.skip_ws();
        if self.peek_char() == Some('$') {
            return self.parse_case_preserve_newer_existing_set_value();
        }
        self.parse_case_decrement_floor_zero_set_value_after_when()
    }

    fn parse_case_preserve_newer_existing_set_value(&mut self) -> Result<SetValueExpression> {
        let incoming = self.parse_value()?;
        self.expect_keyword("IS")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("THEN")?;
        let then_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let then_property = self.parse_ident()?;
        self.expect_keyword("WHEN")?;
        let preserve = self.parse_value()?;
        self.expect_char('=')?;
        let preserve_true = self.parse_value()?;
        if preserve_true.kind != ValueExpressionKind::Literal(hawdb_core::Value::Bool(true)) {
            return Err(self.error("CASE preserve SET only supports comparison to true"));
        }
        self.expect_keyword("AND")?;
        let checked_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let checked_property = self.parse_ident()?;
        if checked_variable != then_variable || checked_property != then_property {
            return Err(self.error("CASE preserve SET must check the preserved property"));
        }
        self.expect_keyword("IS")?;
        self.expect_keyword("NOT")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("AND")?;
        let compared_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let compared_property = self.parse_ident()?;
        if compared_variable != then_variable || compared_property != then_property {
            return Err(self.error("CASE preserve SET must compare the preserved property"));
        }
        self.expect_char('>')?;
        let compared_incoming = self.parse_value()?;
        if compared_incoming != incoming {
            return Err(self.error("CASE preserve SET must compare against the incoming value"));
        }
        self.expect_keyword("THEN")?;
        let preserve_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let preserve_property = self.parse_ident()?;
        if preserve_variable != then_variable || preserve_property != then_property {
            return Err(self.error("CASE preserve SET must return the preserved property"));
        }
        self.expect_keyword("ELSE")?;
        let else_value = self.parse_value()?;
        if else_value != incoming {
            return Err(self.error("CASE preserve SET ELSE must return the incoming value"));
        }
        self.expect_keyword("END")?;
        Ok(SetValueExpression::PreserveNewerExisting {
            variable: then_variable,
            property: then_property,
            incoming,
            preserve,
        })
    }

    fn parse_case_decrement_floor_zero_set_value_after_when(
        &mut self,
    ) -> Result<SetValueExpression> {
        let condition_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let condition_property = self.parse_ident()?;
        self.expect_char('>')?;
        let threshold = self.parse_value()?;
        if threshold.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(0)) {
            return Err(self.error("CASE decrement SET only supports a zero threshold"));
        }
        self.expect_keyword("THEN")?;
        let then_variable = self.parse_ident()?;
        self.expect_char('.')?;
        let then_property = self.parse_ident()?;
        if then_variable != condition_variable || then_property != condition_property {
            return Err(self.error("CASE decrement SET must decrement the tested property"));
        }
        self.expect_char('-')?;
        let decrement = self.parse_value()?;
        if decrement.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(1)) {
            return Err(self.error("CASE decrement SET only supports decrement by one"));
        }
        self.expect_keyword("ELSE")?;
        let floor = self.parse_value()?;
        if floor.kind != ValueExpressionKind::Literal(hawdb_core::Value::Int(0)) {
            return Err(self.error("CASE decrement SET only supports a zero floor"));
        }
        self.expect_keyword("END")?;
        Ok(SetValueExpression::DecrementFloorZero {
            variable: condition_variable,
            property: condition_property,
        })
    }
}

fn matches_ignore_ascii_case(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

fn keyword_matches_at(input: &str, index: usize, keyword: &str) -> bool {
    input
        .get(index..)
        .is_some_and(|rest| keyword_matches(rest, keyword))
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}
