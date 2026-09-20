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

use hawdb_core::{Result, Value};

use super::super::ast::*;
use super::Parser;

impl Parser<'_> {
    pub(super) fn parse_properties(&mut self) -> Result<BTreeMap<String, ValueExpression>> {
        let mut properties = BTreeMap::new();
        self.expect_char('{')?;
        loop {
            self.skip_ws();
            if self.consume_char('}') {
                break;
            }
            let key = self.parse_ident()?;
            self.skip_ws();
            self.expect_char(':')?;
            let value = self.parse_value()?;
            properties.insert(key, value);
            if self.consume_separator_or_end(',', '}')? {
                break;
            }
        }
        Ok(properties)
    }

    pub(super) fn parse_value(&mut self) -> Result<ValueExpression> {
        self.skip_ws();
        let start = self.pos;
        let kind = self.with_recursion(|parser| parser.parse_value_inner())?;
        Ok(self.source_node(kind, start))
    }

    fn parse_value_inner(&mut self) -> Result<ValueExpressionKind> {
        self.skip_ws();
        match self.peek_char() {
            Some('$') => {
                self.pos += 1;
                self.parse_ident().map(ValueExpressionKind::Parameter)
            }
            Some('[') => self.parse_list().map(ValueExpressionKind::List),
            Some('\'') | Some('"') => self
                .parse_string()
                .map(Value::String)
                .map(ValueExpressionKind::Literal),
            Some(ch) if ch.is_ascii_digit() || ch == '-' => {
                if self.remaining_number_contains_decimal_point() {
                    self.parse_float()
                        .map(Value::Float)
                        .map(ValueExpressionKind::Literal)
                } else {
                    self.parse_int()
                        .map(Value::Int)
                        .map(ValueExpressionKind::Literal)
                }
            }
            _ if self.consume_keyword("true") => {
                Ok(ValueExpressionKind::Literal(Value::Bool(true)))
            }
            _ if self.consume_keyword("false") => {
                Ok(ValueExpressionKind::Literal(Value::Bool(false)))
            }
            _ if self.consume_keyword("null") => Ok(ValueExpressionKind::Literal(Value::Null)),
            _ if self.consume_keyword("CURRENT_TIMESTAMP") => {
                self.expect_char('(')?;
                self.expect_char(')')?;
                Ok(ValueExpressionKind::CurrentTimestamp)
            }
            _ if self.consume_keyword("timestamp") => {
                self.expect_char('(')?;
                let value = self.parse_value()?;
                self.expect_char(')')?;
                Ok(ValueExpressionKind::Timestamp(Box::new(value)))
            }
            _ if self.consume_keyword("CAST") => {
                self.expect_char('(')?;
                let value = self.parse_value()?;
                self.expect_keyword("AS")?;
                self.expect_keyword("TIMESTAMP")?;
                self.expect_char(')')?;
                Ok(ValueExpressionKind::Timestamp(Box::new(value)))
            }
            Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {
                let variable = self.parse_ident()?;
                self.expect_char('.')?;
                let property = self.parse_ident()?;
                Ok(ValueExpressionKind::BindingProperty { variable, property })
            }
            _ => Err(self.error("expected value")),
        }
    }

    pub(super) fn parse_list(&mut self) -> Result<Vec<ValueExpression>> {
        self.expect_char('[')?;
        let mut values = Vec::new();
        loop {
            self.skip_ws();
            if self.consume_char(']') {
                break;
            }
            values.push(self.parse_value()?);
            if self.consume_separator_or_end(',', ']')? {
                break;
            }
        }
        Ok(values)
    }

    pub(super) fn parse_string(&mut self) -> Result<String> {
        let quote = self
            .next_char()
            .ok_or_else(|| self.error("expected string"))?;
        let mut out = String::new();
        while let Some(ch) = self.next_char() {
            if ch == quote {
                return Ok(out);
            }
            if ch == '\\' {
                out.push(self.parse_escape_sequence()?);
                continue;
            }
            out.push(ch);
        }
        Err(self.error("unterminated string"))
    }

    pub(super) fn parse_int(&mut self) -> Result<i64> {
        self.skip_ws();
        let start = self.pos;
        if self.peek_char() == Some('-') {
            self.pos += 1;
        }
        self.consume_required_digits("expected integer digits")?;
        self.input[start..self.pos]
            .parse()
            .map_err(|_| self.error("invalid integer"))
    }

    pub(super) fn parse_float(&mut self) -> Result<f64> {
        self.skip_ws();
        let start = self.pos;
        if self.peek_char() == Some('-') {
            self.pos += 1;
        }
        let leading_digits = self.consume_digits();
        if self.peek_char() == Some('.') {
            self.pos += 1;
            let trailing_digits = self.consume_digits();
            if leading_digits == 0 && trailing_digits == 0 {
                return Err(self.error("expected float digits"));
            }
        } else if leading_digits == 0 {
            return Err(self.error("expected float digits"));
        }
        self.input[start..self.pos]
            .parse()
            .map_err(|_| self.error("invalid float"))
    }

    pub(super) fn remaining_number_contains_decimal_point(&self) -> bool {
        let mut index = self.pos;
        if self.input[index..].starts_with('-') {
            index += 1;
        }
        while let Some(ch) = self.input[index..].chars().next() {
            if ch == '.' {
                return true;
            }
            if !ch.is_ascii_digit() {
                return false;
            }
            index += ch.len_utf8();
        }
        false
    }

    pub(super) fn parse_usize(&mut self) -> Result<usize> {
        self.skip_ws();
        let start = self.pos;
        self.consume_required_digits("expected unsigned integer")?;
        self.input[start..self.pos]
            .parse()
            .map_err(|_| self.error("invalid unsigned integer"))
    }

    pub(super) fn parse_ident(&mut self) -> Result<String> {
        self.skip_ws();
        let start = self.pos;
        let Some(first) = self.peek_char() else {
            return Err(self.error("expected identifier"));
        };
        if !is_ident_start(first) {
            return Err(self.error("expected identifier"));
        }
        self.pos += first.len_utf8();
        while matches!(self.peek_char(), Some(ch) if is_ident_continue(ch)) {
            self.pos += self.peek_char().expect("peeked character").len_utf8();
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn consume_required_digits(&mut self, expected: &str) -> Result<()> {
        if self.consume_digits() == 0 {
            return Err(self.error(expected));
        }
        Ok(())
    }

    fn consume_digits(&mut self) -> usize {
        let start = self.pos;
        while matches!(self.peek_char(), Some(ch) if ch.is_ascii_digit()) {
            self.pos += 1;
        }
        self.pos - start
    }

    fn parse_escape_sequence(&mut self) -> Result<char> {
        match self.next_char() {
            Some('\\') => Ok('\\'),
            Some('\'') => Ok('\''),
            Some('"') => Ok('"'),
            Some('n') => Ok('\n'),
            Some('r') => Ok('\r'),
            Some('t') => Ok('\t'),
            Some(ch) => Err(self.error(&format!("unsupported escape sequence \\{ch}"))),
            None => Err(self.error("unterminated escape sequence")),
        }
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_ident_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}
