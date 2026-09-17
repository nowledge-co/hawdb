use skein_core::{Result, SkeinError};

use super::{keyword_matches, Parser};

impl Parser<'_> {
    pub(super) fn source_node<T>(&self, kind: T, start: usize) -> crate::AstNode<T> {
        crate::AstNode::from_source(kind, self.source_span(start))
    }

    pub(super) fn source_span(&self, start: usize) -> crate::SourceSpan {
        let end = start + self.input[start..self.pos].trim_end().len();
        crate::SourceSpan { start, end }
    }

    pub(super) fn consume_keyword(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        if !keyword_matches(rest, keyword) {
            return false;
        }
        self.pos += keyword.len();
        true
    }

    pub(super) fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error(&format!("expected {keyword}")))
        }
    }

    pub(super) fn parse_keyword_choice<T: Copy>(
        &mut self,
        choices: &[(&str, T)],
        expected: &str,
    ) -> Result<T> {
        for (keyword, value) in choices {
            if self.consume_keyword(keyword) {
                return Ok(*value);
            }
        }
        Err(self.error(expected))
    }

    pub(super) fn next_keyword_is(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        keyword_matches(rest, keyword)
    }

    pub(super) fn consume_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.peek_char() != Some(expected) {
            return false;
        }
        self.pos += expected.len_utf8();
        true
    }

    pub(super) fn consume_separator_or_end(&mut self, separator: char, end: char) -> Result<bool> {
        self.skip_ws();
        if self.consume_char(separator) {
            return Ok(false);
        }
        if self.consume_char(end) {
            return Ok(true);
        }
        Err(self.error(&format!("expected '{separator}' or '{end}'")))
    }

    pub(super) fn expect_char(&mut self, expected: char) -> Result<()> {
        self.skip_ws();
        match self.next_char() {
            Some(ch) if ch == expected => Ok(()),
            _ => Err(self.error(&format!("expected '{expected}'"))),
        }
    }

    pub(super) fn expect_token(&mut self, expected: &str) -> Result<()> {
        self.skip_ws();
        if self.input[self.pos..].starts_with(expected) {
            self.pos += expected.len();
            Ok(())
        } else {
            Err(self.error(&format!("expected {expected}")))
        }
    }

    pub(super) fn consume_token(&mut self, expected: &str) -> bool {
        self.skip_ws();
        if self.input[self.pos..].starts_with(expected) {
            self.pos += expected.len();
            true
        } else {
            false
        }
    }

    pub(super) fn expect_eof(&mut self) -> Result<()> {
        self.skip_ws();
        if self.is_eof() {
            Ok(())
        } else {
            Err(self.error("unexpected trailing input"))
        }
    }

    pub(super) fn skip_ws(&mut self) {
        while let Some(ch) = self.peek_char() {
            if !ch.is_whitespace() {
                break;
            }
            self.pos += ch.len_utf8();
        }
    }

    pub(super) fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    pub(super) fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    pub(super) fn next_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    pub(super) fn error(&self, message: &str) -> SkeinError {
        let (line, column) = self.line_column();
        let near = self.near_fragment();
        SkeinError::Parse(format!(
            "{message} at byte {} line {line} column {column} near `{near}`",
            self.pos
        ))
    }

    fn line_column(&self) -> (usize, usize) {
        let mut line = 1;
        let mut column = 1;
        for ch in self.input[..self.pos].chars() {
            if ch == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        (line, column)
    }

    fn near_fragment(&self) -> String {
        const MAX_CONTEXT_CHARS: usize = 24;
        self.input[self.pos..]
            .chars()
            .take(MAX_CONTEXT_CHARS)
            .flat_map(char::escape_default)
            .collect()
    }
}
