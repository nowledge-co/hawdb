use super::{identifier_from_token, is_identifier_kind, Parser, MAX_EXPRESSION_NESTING};
use crate::{
    BinaryOperatorSyntax, ExpressionKindSyntax, ExpressionSyntax, Identifier, LiteralSyntax,
    QualifiedName, Span, SyntaxError, SyntaxErrorCode, Token, TokenKind, UnaryOperatorSyntax,
};

impl Parser<'_> {
    pub(super) fn parse_expression_range(
        &self,
        start_position: usize,
        end_position: usize,
    ) -> Result<ExpressionSyntax, SyntaxError> {
        if start_position == end_position {
            return Err(self.unexpected("an expression"));
        }
        ExpressionParser::new(self.input, self.tokens, start_position, end_position)
            .parse_complete()
    }
}

struct ExpressionParser<'a> {
    input: &'a str,
    tokens: &'a [Token],
    position: usize,
    end_position: usize,
    nesting: usize,
}

impl<'a> ExpressionParser<'a> {
    fn new(
        input: &'a str,
        tokens: &'a [Token],
        start_position: usize,
        end_position: usize,
    ) -> Self {
        Self {
            input,
            tokens,
            position: start_position,
            end_position,
            nesting: 0,
        }
    }

    fn parse_complete(mut self) -> Result<ExpressionSyntax, SyntaxError> {
        let expression = self.parse_expression(0)?;
        if self.position != self.end_position {
            return Err(self.unexpected("an operator or end of expression"));
        }
        Ok(expression)
    }

    fn parse_expression(
        &mut self,
        minimum_binding_power: u8,
    ) -> Result<ExpressionSyntax, SyntaxError> {
        let mut left = self.parse_prefix()?;
        loop {
            if self.at_keyword("COLLATE") {
                const POSTFIX_BINDING_POWER: u8 = 70;
                if POSTFIX_BINDING_POWER < minimum_binding_power {
                    break;
                }
                self.advance();
                let collation = self.parse_qualified_name()?;
                left = ExpressionSyntax {
                    span: Span::new(left.span.start, collation.span.end),
                    kind: ExpressionKindSyntax::Collate {
                        expression: Box::new(left),
                        collation,
                    },
                };
                continue;
            }
            if self.current().kind == TokenKind::DoubleColon {
                const POSTFIX_BINDING_POWER: u8 = 70;
                if POSTFIX_BINDING_POWER < minimum_binding_power {
                    break;
                }
                self.advance();
                let data_type = self.parse_qualified_name()?;
                left = ExpressionSyntax {
                    span: Span::new(left.span.start, data_type.span.end),
                    kind: ExpressionKindSyntax::Cast {
                        expression: Box::new(left),
                        data_type,
                    },
                };
                continue;
            }

            if self.at_keyword("IS") {
                const COMPARISON_BINDING_POWER: u8 = 30;
                if COMPARISON_BINDING_POWER < minimum_binding_power {
                    break;
                }
                self.advance();
                let negated = self.consume_keyword("NOT");
                let null = self.expect_keyword("NULL")?;
                left = ExpressionSyntax {
                    span: Span::new(left.span.start, null.span.end),
                    kind: ExpressionKindSyntax::IsNull {
                        expression: Box::new(left),
                        negated,
                    },
                };
                continue;
            }

            let negated_predicate = self.at_keyword("NOT")
                && (self.peek_keyword(1, "IN") || self.peek_keyword(1, "BETWEEN"));
            if self.at_keyword("IN") || (negated_predicate && self.peek_keyword(1, "IN")) {
                const COMPARISON_BINDING_POWER: u8 = 30;
                if COMPARISON_BINDING_POWER < minimum_binding_power {
                    break;
                }
                let negated = self.consume_keyword("NOT");
                self.expect_keyword("IN")?;
                let values = self.parse_parenthesized_expression_list()?;
                let end = self.previous_end();
                left = ExpressionSyntax {
                    span: Span::new(left.span.start, end),
                    kind: ExpressionKindSyntax::InList {
                        expression: Box::new(left),
                        values,
                        negated,
                    },
                };
                continue;
            }
            if self.at_keyword("BETWEEN") || (negated_predicate && self.peek_keyword(1, "BETWEEN"))
            {
                const COMPARISON_BINDING_POWER: u8 = 30;
                if COMPARISON_BINDING_POWER < minimum_binding_power {
                    break;
                }
                let negated = self.consume_keyword("NOT");
                self.expect_keyword("BETWEEN")?;
                let low = self.parse_expression(COMPARISON_BINDING_POWER + 1)?;
                self.expect_keyword("AND")?;
                let high = self.parse_expression(COMPARISON_BINDING_POWER + 1)?;
                let end = high.span.end;
                left = ExpressionSyntax {
                    span: Span::new(left.span.start, end),
                    kind: ExpressionKindSyntax::Between {
                        expression: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated,
                    },
                };
                continue;
            }

            let Some((left_binding_power, right_binding_power, operator)) =
                self.current_binary_operator()
            else {
                break;
            };
            if left_binding_power < minimum_binding_power {
                break;
            }
            self.advance();
            let right = self.parse_expression(right_binding_power)?;
            let end = right.span.end;
            left = ExpressionSyntax {
                span: Span::new(left.span.start, end),
                kind: ExpressionKindSyntax::Binary {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                },
            };
        }
        Ok(left)
    }

    fn parse_prefix(&mut self) -> Result<ExpressionSyntax, SyntaxError> {
        const NOT_BINDING_POWER: u8 = 25;
        const SIGN_BINDING_POWER: u8 = 65;

        let token = self.current();
        let unary = if self.consume_keyword("NOT") {
            Some((UnaryOperatorSyntax::Not, NOT_BINDING_POWER))
        } else if self.consume_kind(TokenKind::Plus) {
            Some((UnaryOperatorSyntax::Plus, SIGN_BINDING_POWER))
        } else if self.consume_kind(TokenKind::Minus) {
            Some((UnaryOperatorSyntax::Minus, SIGN_BINDING_POWER))
        } else {
            None
        };
        if let Some((operator, binding_power)) = unary {
            self.enter_nesting(token.span)?;
            let expression = self.parse_expression(binding_power);
            self.nesting -= 1;
            let expression = expression?;
            return Ok(ExpressionSyntax {
                span: Span::new(token.span.start, expression.span.end),
                kind: ExpressionKindSyntax::Unary {
                    operator,
                    expression: Box::new(expression),
                },
            });
        }

        match token.kind {
            TokenKind::LeftParen => self.parse_parenthesized_expression(),
            TokenKind::Star => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Wildcard(None),
                    span: token.span,
                })
            }
            TokenKind::Parameter(position) => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Parameter(position),
                    span: token.span,
                })
            }
            TokenKind::Number => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Literal(LiteralSyntax::Number(token.span)),
                    span: token.span,
                })
            }
            TokenKind::String => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Literal(LiteralSyntax::String(token.span)),
                    span: token.span,
                })
            }
            TokenKind::Word if token.is_keyword(self.input, "NULL") => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Literal(LiteralSyntax::Null),
                    span: token.span,
                })
            }
            TokenKind::Word if token.is_keyword(self.input, "TRUE") => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Literal(LiteralSyntax::Boolean(true)),
                    span: token.span,
                })
            }
            TokenKind::Word if token.is_keyword(self.input, "FALSE") => {
                self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Literal(LiteralSyntax::Boolean(false)),
                    span: token.span,
                })
            }
            TokenKind::Word
                if is_typed_string_keyword(token.text(self.input))
                    && self.peek(1).kind == TokenKind::String =>
            {
                self.advance();
                let value = self.advance();
                Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::TypedString {
                        data_type: identifier_from_token(token),
                        value: value.span,
                    },
                    span: Span::new(token.span.start, value.span.end),
                })
            }
            kind if is_identifier_kind(kind) => self.parse_name_expression(),
            _ => Err(self.unexpected("a PostgreSQL expression")),
        }
    }

    fn parse_parenthesized_expression(&mut self) -> Result<ExpressionSyntax, SyntaxError> {
        let start = self.advance().span.start;
        self.enter_nesting(Span::new(start, start + 1))?;
        let result = (|| {
            let expression = self.parse_expression(0)?;
            let end = self.expect_kind(TokenKind::RightParen, ")")?.span.end;
            Ok(ExpressionSyntax {
                kind: ExpressionKindSyntax::Parenthesized(Box::new(expression)),
                span: Span::new(start, end),
            })
        })();
        self.nesting -= 1;
        result
    }

    fn parse_name_expression(&mut self) -> Result<ExpressionSyntax, SyntaxError> {
        let first = self.parse_identifier()?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut parts = vec![first];
        while self.consume_kind(TokenKind::Dot) {
            if self.consume_kind(TokenKind::Star) {
                let qualifier = QualifiedName {
                    parts,
                    span: Span::new(start, end),
                };
                return Ok(ExpressionSyntax {
                    kind: ExpressionKindSyntax::Wildcard(Some(qualifier)),
                    span: Span::new(start, self.previous_end()),
                });
            }
            let part = self.parse_identifier()?;
            end = part.span.end;
            parts.push(part);
        }
        let name = QualifiedName {
            parts,
            span: Span::new(start, end),
        };
        if self.current().kind != TokenKind::LeftParen {
            return Ok(ExpressionSyntax {
                span: name.span,
                kind: ExpressionKindSyntax::Column(name),
            });
        }

        self.advance();
        self.enter_nesting(Span::new(start, self.previous_end()))?;
        let result = (|| {
            let distinct = self.consume_keyword("DISTINCT");
            let mut arguments = Vec::new();
            if self.current().kind != TokenKind::RightParen {
                loop {
                    arguments.push(self.parse_expression(0)?);
                    if !self.consume_kind(TokenKind::Comma) {
                        break;
                    }
                }
            }
            let end = self.expect_kind(TokenKind::RightParen, ")")?.span.end;
            Ok(ExpressionSyntax {
                kind: ExpressionKindSyntax::Function {
                    name,
                    arguments,
                    distinct,
                },
                span: Span::new(start, end),
            })
        })();
        self.nesting -= 1;
        result
    }

    fn parse_parenthesized_expression_list(
        &mut self,
    ) -> Result<Vec<ExpressionSyntax>, SyntaxError> {
        let open = self.expect_kind(TokenKind::LeftParen, "(")?;
        self.enter_nesting(open.span)?;
        let result = (|| {
            if self.current().kind == TokenKind::RightParen {
                return Err(self.unexpected("at least one expression in IN"));
            }
            let mut values = Vec::new();
            loop {
                values.push(self.parse_expression(0)?);
                if !self.consume_kind(TokenKind::Comma) {
                    break;
                }
            }
            self.expect_kind(TokenKind::RightParen, ")")?;
            Ok(values)
        })();
        self.nesting -= 1;
        result
    }

    fn parse_qualified_name(&mut self) -> Result<QualifiedName, SyntaxError> {
        let first = self.parse_identifier()?;
        let start = first.span.start;
        let mut end = first.span.end;
        let mut parts = vec![first];
        while self.consume_kind(TokenKind::Dot) {
            let part = self.parse_identifier()?;
            end = part.span.end;
            parts.push(part);
        }
        Ok(QualifiedName {
            parts,
            span: Span::new(start, end),
        })
    }

    fn parse_identifier(&mut self) -> Result<Identifier, SyntaxError> {
        let token = self.current();
        if !is_identifier_kind(token.kind) {
            return Err(self.unexpected("an identifier"));
        }
        self.advance();
        Ok(identifier_from_token(token))
    }

    fn current_binary_operator(&self) -> Option<(u8, u8, BinaryOperatorSyntax)> {
        let token = self.current();
        let (binding_power, operator) = match token.kind {
            TokenKind::Equal => (30, BinaryOperatorSyntax::Equal),
            TokenKind::NotEqual => (30, BinaryOperatorSyntax::NotEqual),
            TokenKind::Less => (30, BinaryOperatorSyntax::Less),
            TokenKind::LessOrEqual => (30, BinaryOperatorSyntax::LessOrEqual),
            TokenKind::Greater => (30, BinaryOperatorSyntax::Greater),
            TokenKind::GreaterOrEqual => (30, BinaryOperatorSyntax::GreaterOrEqual),
            TokenKind::Concat => (40, BinaryOperatorSyntax::Concat),
            TokenKind::Plus => (50, BinaryOperatorSyntax::Add),
            TokenKind::Minus => (50, BinaryOperatorSyntax::Subtract),
            TokenKind::Star => (60, BinaryOperatorSyntax::Multiply),
            TokenKind::Slash => (60, BinaryOperatorSyntax::Divide),
            TokenKind::Percent => (60, BinaryOperatorSyntax::Modulo),
            TokenKind::Word if token.is_keyword(self.input, "OR") => (10, BinaryOperatorSyntax::Or),
            TokenKind::Word if token.is_keyword(self.input, "AND") => {
                (20, BinaryOperatorSyntax::And)
            }
            _ => return None,
        };
        Some((binding_power, binding_power + 1, operator))
    }

    fn enter_nesting(&mut self, span: Span) -> Result<(), SyntaxError> {
        if self.nesting == MAX_EXPRESSION_NESTING {
            return Err(SyntaxError::new(
                SyntaxErrorCode::ExpressionNestingLimitExceeded,
                span,
            ));
        }
        self.nesting += 1;
        Ok(())
    }

    fn current(&self) -> Token {
        self.peek(0)
    }

    fn peek(&self, offset: usize) -> Token {
        let position = self.position.saturating_add(offset);
        if position >= self.end_position {
            let span = self
                .tokens
                .get(self.end_position)
                .map_or(Span::new(self.input.len(), self.input.len()), |token| {
                    token.span
                });
            return Token::new(TokenKind::End, Span::new(span.start, span.start));
        }
        self.tokens[position]
    }

    fn advance(&mut self) -> Token {
        let token = self.current();
        if self.position < self.end_position {
            self.position += 1;
        }
        token
    }

    fn previous_end(&self) -> usize {
        self.position
            .checked_sub(1)
            .and_then(|position| self.tokens.get(position))
            .map_or(0, |token| token.span.end)
    }

    fn at_keyword(&self, keyword: &str) -> bool {
        self.current().is_keyword(self.input, keyword)
    }

    fn peek_keyword(&self, offset: usize, keyword: &str) -> bool {
        self.peek(offset).is_keyword(self.input, keyword)
    }

    fn consume_keyword(&mut self, keyword: &str) -> bool {
        if self.at_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &'static str) -> Result<Token, SyntaxError> {
        if self.at_keyword(keyword) {
            Ok(self.advance())
        } else {
            Err(self.unexpected(keyword))
        }
    }

    fn consume_kind(&mut self, kind: TokenKind) -> bool {
        if self.current().kind == kind {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_kind(
        &mut self,
        kind: TokenKind,
        expected: &'static str,
    ) -> Result<Token, SyntaxError> {
        if self.current().kind == kind {
            Ok(self.advance())
        } else {
            Err(self.unexpected(expected))
        }
    }

    fn unexpected(&self, expected: &'static str) -> SyntaxError {
        let token = self.current();
        let code = if token.kind == TokenKind::End {
            SyntaxErrorCode::UnexpectedEnd
        } else {
            SyntaxErrorCode::UnexpectedToken
        };
        SyntaxError::new(code, token.span)
            .expected(expected)
            .found(token.text(self.input))
    }
}

fn is_typed_string_keyword(word: &str) -> bool {
    ["DATE", "TIME", "TIMESTAMP", "INTERVAL"]
        .into_iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
}
