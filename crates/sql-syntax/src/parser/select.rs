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

use super::{Depths, Parser};
use crate::{
    ExpressionSyntax, LabeledExpressionSyntax, NullOrderSyntax, OrderBySyntax,
    OrderDirectionSyntax, Span, SyntaxError, TokenKind,
};

pub(super) const SELECT_CLAUSE_KEYWORDS: &[&str] = &[
    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "FETCH",
];
pub(super) const JOIN_BOUNDARY_KEYWORDS: &[&str] = &[
    "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER",
    "LIMIT", "OFFSET", "FOR", "FETCH",
];

impl Parser<'_> {
    pub(super) fn parse_select_projection(
        &mut self,
    ) -> Result<Vec<LabeledExpressionSyntax>, SyntaxError> {
        if self.at_keyword("FROM") {
            return Err(self.unexpected("at least one SELECT projection"));
        }
        let mut projection = Vec::new();
        loop {
            let start_position = self.position;
            let end_position = self.scan_select_expression_end(&["FROM"], true)?;
            projection.push(self.labeled_expression_from_range(start_position, end_position)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(projection)
    }

    pub(super) fn parse_select_expression_list(
        &mut self,
        stop_keywords: &[&str],
        expected: &'static str,
    ) -> Result<Vec<ExpressionSyntax>, SyntaxError> {
        let mut expressions = Vec::new();
        loop {
            let start_position = self.position;
            let end_position = self.scan_select_expression_end(stop_keywords, true)?;
            if start_position == end_position {
                return Err(self.unexpected(expected));
            }
            expressions.push(self.parse_expression_range(start_position, end_position)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(expressions)
    }

    pub(super) fn parse_order_by_items(&mut self) -> Result<Vec<OrderBySyntax>, SyntaxError> {
        let mut items = Vec::new();
        loop {
            let start_position = self.position;
            let end_position = self.scan_select_expression_end(SELECT_CLAUSE_KEYWORDS, true)?;
            items.push(self.order_by_item_from_range(start_position, end_position)?);
            if !self.consume_kind(TokenKind::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn order_by_item_from_range(
        &self,
        start_position: usize,
        end_position: usize,
    ) -> Result<OrderBySyntax, SyntaxError> {
        if start_position == end_position {
            return Err(self.unexpected("an expression after ORDER BY"));
        }
        let start = self.tokens[start_position].span.start;
        let end = self.tokens[end_position - 1].span.end;
        let mut expression_end = end_position;
        let nulls = if expression_end >= start_position + 2
            && self.tokens[expression_end - 2].is_keyword(self.input, "NULLS")
        {
            let nulls = if self.tokens[expression_end - 1].is_keyword(self.input, "FIRST") {
                Some(NullOrderSyntax::First)
            } else if self.tokens[expression_end - 1].is_keyword(self.input, "LAST") {
                Some(NullOrderSyntax::Last)
            } else {
                None
            };
            if nulls.is_some() {
                expression_end -= 2;
            }
            nulls
        } else {
            None
        };
        let direction = if expression_end > start_position
            && self.tokens[expression_end - 1].is_keyword(self.input, "ASC")
        {
            expression_end -= 1;
            Some(OrderDirectionSyntax::Ascending)
        } else if expression_end > start_position
            && self.tokens[expression_end - 1].is_keyword(self.input, "DESC")
        {
            expression_end -= 1;
            Some(OrderDirectionSyntax::Descending)
        } else {
            None
        };
        if start_position == expression_end {
            return Err(self.unexpected("an expression before ORDER BY modifiers"));
        }
        Ok(OrderBySyntax {
            expression: self.parse_expression_range(start_position, expression_end)?,
            direction,
            nulls,
            span: Span::new(start, end),
        })
    }

    pub(super) fn parse_select_clause_expression(
        &mut self,
        stop_keywords: &[&str],
        expected: &'static str,
    ) -> Result<ExpressionSyntax, SyntaxError> {
        let start_position = self.position;
        let end_position = self.scan_select_expression_end(stop_keywords, true)?;
        if start_position == end_position {
            return Err(self.unexpected(expected));
        }
        self.parse_expression_range(start_position, end_position)
    }

    pub(super) fn scan_select_expression_end(
        &mut self,
        stop_keywords: &[&str],
        stop_at_comma: bool,
    ) -> Result<usize, SyntaxError> {
        let mut depths = Depths::default();
        while self.current().kind != TokenKind::End {
            if depths.is_zero()
                && (self.current().kind == TokenKind::Semicolon
                    || (stop_at_comma && self.current().kind == TokenKind::Comma)
                    || stop_keywords.iter().any(|keyword| self.at_keyword(keyword)))
            {
                return Ok(self.position);
            }
            self.update_expression_depths(&mut depths, self.current())?;
            self.advance();
        }
        if !depths.is_zero() {
            return Err(self.unclosed_expression_error(&depths));
        }
        Ok(self.position)
    }

    pub(super) fn at_select_clause_boundary(&self) -> bool {
        matches!(self.current().kind, TokenKind::Semicolon | TokenKind::End)
            || SELECT_CLAUSE_KEYWORDS
                .iter()
                .any(|keyword| self.at_keyword(keyword))
    }
}
