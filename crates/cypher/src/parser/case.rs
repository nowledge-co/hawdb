use super::super::ast::*;
use super::Parser;
use skein_core::Result;

impl Parser<'_> {
    pub(super) fn parse_case_expression(&mut self) -> Result<ScalarExpressionKind> {
        let operand = if self.next_keyword_is("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_case_scalar()?))
        };
        let mut branches = Vec::new();
        self.expect_keyword("WHEN")?;
        loop {
            let condition = if operand.is_some() {
                self.parse_case_scalar()?
            } else {
                self.parse_case_or()?
            };
            self.expect_keyword("THEN")?;
            let result = self.parse_case_scalar()?;
            branches.push((condition, result));
            if !self.consume_keyword("WHEN") {
                break;
            }
        }
        let otherwise = if self.consume_keyword("ELSE") {
            Some(Box::new(self.parse_case_scalar()?))
        } else {
            None
        };
        self.expect_keyword("END")?;
        Ok(ScalarExpressionKind::Case {
            operand,
            branches,
            otherwise,
        })
    }

    fn parse_case_scalar(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.checkpoint();
        if self.next_keyword_is("CURRENT_TIMESTAMP")
            || self.next_keyword_is("timestamp")
            || self.next_keyword_is("CAST")
        {
            self.parse_ident()?;
            let is_call = self.consume_char('(');
            self.restore(start);
            if is_call {
                let value = self.parse_value()?;
                return Ok(self.source_node(ScalarExpressionKind::Value(value), start.pos));
            }
        }
        self.parse_scalar_expression()
    }

    fn parse_case_or(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let mut expression = self.parse_case_and()?;
        if self.consume_keyword("OR") {
            let right = self.with_recursion(|parser| parser.parse_case_or())?;
            expression = self.source_node(
                ScalarExpressionKind::Binary {
                    left: Box::new(expression),
                    op: ScalarBinaryOp::Or,
                    right: Box::new(right),
                },
                start,
            );
        }
        Ok(expression)
    }

    fn parse_case_and(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        let mut expression = self.parse_case_condition()?;
        if self.consume_keyword("AND") {
            let right = self.with_recursion(|parser| parser.parse_case_and())?;
            expression = self.source_node(
                ScalarExpressionKind::Binary {
                    left: Box::new(expression),
                    op: ScalarBinaryOp::And,
                    right: Box::new(right),
                },
                start,
            );
        }
        Ok(expression)
    }

    fn parse_case_condition(&mut self) -> Result<ScalarExpression> {
        self.with_recursion(|parser| parser.parse_case_condition_inner())
    }

    fn parse_case_condition_inner(&mut self) -> Result<ScalarExpression> {
        self.skip_ws();
        let start = self.pos;
        if self.consume_keyword("NOT") {
            let expression = self.parse_case_condition()?;
            return Ok(self.source_node(ScalarExpressionKind::Not(Box::new(expression)), start));
        }
        let left = if self.consume_char('(') {
            let expression = self.parse_case_or()?;
            self.expect_char(')')?;
            self.source_node(expression.kind, start)
        } else if self.consume_keyword("list_contains") {
            self.expect_char('(')?;
            let left = self.parse_case_scalar()?;
            self.expect_char(',')?;
            let right = self.parse_case_scalar()?;
            self.expect_char(')')?;
            self.source_node(
                ScalarExpressionKind::Binary {
                    left: Box::new(left),
                    op: ScalarBinaryOp::ListContains,
                    right: Box::new(right),
                },
                start,
            )
        } else {
            self.parse_case_scalar()?
        };
        if self.consume_keyword("IS") {
            let negated = self.consume_keyword("NOT");
            self.expect_keyword("NULL")?;
            return Ok(self.source_node(
                ScalarExpressionKind::IsNull {
                    expression: Box::new(left),
                    negated,
                },
                start,
            ));
        }
        let op = if self.consume_token("<>") || self.consume_token("!=") {
            ScalarBinaryOp::NotEq
        } else if self.consume_token("<=") {
            ScalarBinaryOp::Lte
        } else if self.consume_token(">=") {
            ScalarBinaryOp::Gte
        } else if self.consume_char('=') {
            ScalarBinaryOp::Eq
        } else if self.consume_char('<') {
            ScalarBinaryOp::Lt
        } else if self.consume_char('>') {
            ScalarBinaryOp::Gt
        } else if self.consume_keyword("CONTAINS") {
            ScalarBinaryOp::Contains
        } else {
            return Ok(left);
        };
        let right = self.parse_case_scalar()?;
        Ok(self.source_node(
            ScalarExpressionKind::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            start,
        ))
    }
}
