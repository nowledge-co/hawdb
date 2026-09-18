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
    ExpressionSyntax, LabeledExpressionSyntax, LockStrengthSyntax, LockingClauseSyntax,
    NullOrderSyntax, OrderBySyntax, OrderDirectionSyntax, PostgresFromItemSyntax,
    PostgresFromSyntax, PostgresJoinKind, PostgresJoinSyntax, PostgresSelectSyntax,
    RelationTableSyntax, Span, SyntaxError, TokenKind,
};

const SELECT_CLAUSE_KEYWORDS: &[&str] = &[
    "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FOR", "FETCH",
];
const JOIN_BOUNDARY_KEYWORDS: &[&str] = &[
    "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER",
    "LIMIT", "OFFSET", "FOR", "FETCH",
];

impl Parser<'_> {
    pub(super) fn parse_postgres_select(&mut self) -> Result<PostgresSelectSyntax, SyntaxError> {
        let start = self.expect_keyword("SELECT")?.span.start;
        let distinct = self.consume_keyword("DISTINCT");
        if !distinct {
            self.consume_keyword("ALL");
        } else if self.at_keyword("ON") {
            return Err(self.unexpected("a projection after DISTINCT"));
        }

        let projection = self.parse_select_projection()?;
        self.expect_keyword("FROM")?;
        let from = self.parse_select_from_sources()?;

        let selection = if self.consume_keyword("WHERE") {
            Some(self.parse_select_clause_expression(
                SELECT_CLAUSE_KEYWORDS,
                "an expression after WHERE",
            )?)
        } else {
            None
        };
        let group_by = if self.consume_keyword("GROUP") {
            self.expect_keyword("BY")?;
            self.parse_select_expression_list(
                SELECT_CLAUSE_KEYWORDS,
                "an expression list after GROUP BY",
            )?
        } else {
            Vec::new()
        };
        let having = if self.consume_keyword("HAVING") {
            Some(self.parse_select_clause_expression(
                SELECT_CLAUSE_KEYWORDS,
                "an expression after HAVING",
            )?)
        } else {
            None
        };
        let order_by = if self.consume_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.parse_order_by_items()?
        } else {
            Vec::new()
        };
        let limit = if self.consume_keyword("LIMIT") {
            Some(self.parse_select_clause_expression(
                SELECT_CLAUSE_KEYWORDS,
                "an expression after LIMIT",
            )?)
        } else {
            None
        };
        let offset = if self.consume_keyword("OFFSET") {
            Some(self.parse_select_clause_expression(
                SELECT_CLAUSE_KEYWORDS,
                "an expression after OFFSET",
            )?)
        } else {
            None
        };
        if self.at_keyword("FETCH") {
            return Err(self.unexpected("LIMIT or OFFSET in the owned SELECT subset"));
        }
        let locking = if self.consume_keyword("FOR") {
            let start = self.tokens[self.position - 1].span.start;
            let strength = if self.consume_keyword("SHARE") {
                LockStrengthSyntax::Share
            } else if self.consume_keyword("UPDATE") {
                LockStrengthSyntax::Update
            } else {
                return Err(self.unexpected("SHARE or UPDATE after FOR"));
            };
            Some(LockingClauseSyntax {
                strength,
                span: Span::new(start, self.previous_end()),
            })
        } else {
            None
        };
        if !matches!(self.current().kind, TokenKind::Semicolon | TokenKind::End) {
            return Err(self.unexpected("end of SELECT or a supported SELECT clause"));
        }

        Ok(PostgresSelectSyntax {
            distinct,
            projection,
            from,
            selection,
            group_by,
            having,
            order_by,
            limit,
            offset,
            locking,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_select_projection(&mut self) -> Result<Vec<LabeledExpressionSyntax>, SyntaxError> {
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

    fn parse_select_from_sources(&mut self) -> Result<Vec<PostgresFromSyntax>, SyntaxError> {
        let mut from = Vec::new();
        loop {
            if self.at_select_clause_boundary() {
                return Err(self.unexpected("at least one FROM item"));
            }
            let start = self.current().span.start;
            let relation = self.parse_select_from_primary()?;
            let mut joins = Vec::new();
            while self.at_join_start() {
                joins.push(self.parse_select_join()?);
            }
            from.push(PostgresFromSyntax {
                relation,
                joins,
                span: Span::new(start, self.previous_end()),
            });

            if self.consume_kind(TokenKind::Comma) {
                continue;
            }
            if self.at_select_clause_boundary() {
                break;
            }
            return Err(
                self.unexpected("a join, comma, or supported SELECT clause after FROM item")
            );
        }
        Ok(from)
    }

    fn parse_select_from_primary(&mut self) -> Result<PostgresFromItemSyntax, SyntaxError> {
        if self.at_keyword("GRAPH_TABLE") {
            return self
                .parse_graph_table()
                .map(PostgresFromItemSyntax::GraphTable);
        }
        let start = self.current().span.start;
        let name = self.parse_qualified_name()?;
        let alias = self.parse_optional_table_alias()?;
        Ok(PostgresFromItemSyntax::Relation(RelationTableSyntax {
            name,
            alias,
            span: Span::new(start, self.previous_end()),
        }))
    }

    fn parse_select_join(&mut self) -> Result<PostgresJoinSyntax, SyntaxError> {
        let start = self.current().span.start;
        let kind = if self.consume_keyword("JOIN") {
            PostgresJoinKind::Inner
        } else if self.consume_keyword("INNER") {
            self.expect_keyword("JOIN")?;
            PostgresJoinKind::Inner
        } else if self.consume_keyword("LEFT") {
            self.consume_keyword("OUTER");
            self.expect_keyword("JOIN")?;
            PostgresJoinKind::Left
        } else if self.consume_keyword("CROSS") {
            self.expect_keyword("JOIN")?;
            PostgresJoinKind::Cross
        } else {
            return Err(self.unexpected("INNER, LEFT, or CROSS JOIN"));
        };
        let relation = self.parse_select_from_primary()?;
        let condition = if kind == PostgresJoinKind::Cross {
            None
        } else {
            self.expect_keyword("ON")?;
            Some(self.parse_select_clause_expression(
                JOIN_BOUNDARY_KEYWORDS,
                "an expression after JOIN ON",
            )?)
        };
        Ok(PostgresJoinSyntax {
            kind,
            relation,
            condition,
            span: Span::new(start, self.previous_end()),
        })
    }

    fn parse_select_expression_list(
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

    fn parse_order_by_items(&mut self) -> Result<Vec<OrderBySyntax>, SyntaxError> {
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

    fn parse_select_clause_expression(
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

    fn scan_select_expression_end(
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

    fn at_join_start(&self) -> bool {
        ["JOIN", "INNER", "LEFT", "CROSS"]
            .into_iter()
            .any(|keyword| self.at_keyword(keyword))
    }

    fn at_select_clause_boundary(&self) -> bool {
        matches!(self.current().kind, TokenKind::Semicolon | TokenKind::End)
            || SELECT_CLAUSE_KEYWORDS
                .iter()
                .any(|keyword| self.at_keyword(keyword))
    }
}
