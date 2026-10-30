// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Result;
use hawdb_sql_syntax::{Span, Token, TokenKind};

use super::super::Parser;

/// Preserve Cypher's whitespace, backslash strings and ASCII keyword boundaries.
/// SQL's lexer cannot be used: `--` is a relationship, and `"..."` is a string.
pub(super) fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if tokens.len() == 1_000_000 {
            let mut parser = Parser::new(input);
            parser.pos = start;
            return Err(parser.error("Cypher token count exceeds limit of 1000000"));
        }
        let mut end = start + ch.len_utf8();
        let kind = if ch.is_ascii_alphabetic() || ch == '_' {
            while let Some(&(offset, next)) = chars.peek() {
                if !next.is_ascii_alphanumeric() && next != '_' {
                    break;
                }
                chars.next();
                end = offset + next.len_utf8();
            }
            TokenKind::Word
        } else if ch.is_ascii_digit() {
            while let Some(&(offset, next)) = chars.peek() {
                if !next.is_ascii_digit() {
                    break;
                }
                chars.next();
                end = offset + 1;
            }
            TokenKind::Number
        } else if matches!(ch, '\'' | '"') {
            // Native terminals diagnose escapes and unterminated strings with
            // the original Cypher byte/line/column and context text.
            while let Some((offset, next)) = chars.next() {
                end = offset + next.len_utf8();
                if next == ch {
                    break;
                }
                if next == '\\'
                    && let Some((offset, escaped)) = chars.next()
                {
                    end = offset + escaped.len_utf8();
                }
            }
            TokenKind::String
        } else if ch == ';' {
            TokenKind::Semicolon
        } else {
            // Punctuation is interpreted by the bounded native terminals.
            TokenKind::Question
        };
        tokens.push(Token::new(kind, Span::new(start, end)));
    }
    tokens.push(Token::new(
        TokenKind::End,
        Span::new(input.len(), input.len()),
    ));
    Ok(tokens)
}
