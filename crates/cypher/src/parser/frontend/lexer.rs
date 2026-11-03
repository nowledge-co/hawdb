// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Result;
use hawdb_sql_syntax::Span;

use super::parse_error;

// Cypher admits at most 16 MiB, so byte offsets fit losslessly in u32.
const _: () = assert!(super::MAX_CYPHER_INPUT_BYTES <= u32::MAX as usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum TokenKind {
    Word,
    String,
    Number,
    Parameter,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    LeftBrace,
    RightBrace,
    Comma,
    Dot,
    Semicolon,
    Colon,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Equal,
    Less,
    Greater,
    NotEqual,
    Pipe,
    Caret,
    Question,
    Tilde,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Token {
    pub(super) kind: TokenKind,
    start: u32,
    end: u32,
}

impl Token {
    fn new(kind: TokenKind, span: Span) -> Self {
        Self {
            kind,
            start: u32::try_from(span.start).expect("bounded Cypher token start"),
            end: u32::try_from(span.end).expect("bounded Cypher token end"),
        }
    }

    pub(super) fn span(self) -> Span {
        Span::new(self.start as usize, self.end as usize)
    }

    pub(super) fn text(self, input: &str) -> &str {
        input
            .get(self.start as usize..self.end as usize)
            .unwrap_or_default()
    }

    pub(super) fn is_keyword(self, input: &str, keyword: &str) -> bool {
        self.kind == TokenKind::Word && self.text(input).eq_ignore_ascii_case(keyword)
    }
}

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
            return Err(parse_error(
                input,
                start,
                "Cypher token count exceeds limit of 1000000",
            ));
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
            // AST folding diagnoses escapes and unterminated strings with
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
        } else {
            match ch {
                '(' => TokenKind::LeftParen,
                ')' => TokenKind::RightParen,
                '[' => TokenKind::LeftBracket,
                ']' => TokenKind::RightBracket,
                '{' => TokenKind::LeftBrace,
                '}' => TokenKind::RightBrace,
                ':' => TokenKind::Colon,
                ',' => TokenKind::Comma,
                '.' => TokenKind::Dot,
                ';' => TokenKind::Semicolon,
                '-' => TokenKind::Minus,
                '+' => TokenKind::Plus,
                '*' => TokenKind::Star,
                '/' => TokenKind::Slash,
                '%' => TokenKind::Percent,
                '^' => TokenKind::Caret,
                '=' => TokenKind::Equal,
                '<' => TokenKind::Less,
                '>' => TokenKind::Greater,
                '|' => TokenKind::Pipe,
                '$' => TokenKind::Parameter,
                '!' => TokenKind::NotEqual,
                '~' => TokenKind::Tilde,
                _ => TokenKind::Question,
            }
        };
        tokens.push(Token::new(kind, Span::new(start, end)));
    }
    tokens.push(Token::new(
        TokenKind::End,
        Span::new(input.len(), input.len()),
    ));
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_tokens_preserve_full_admitted_byte_offsets() {
        assert_eq!(std::mem::size_of::<Token>(), 12);
        let end = super::super::MAX_CYPHER_INPUT_BYTES;
        for span in [
            Span::new(0, 0),
            Span::new(end - 1, end),
            Span::new(end, end),
        ] {
            let token = Token::new(TokenKind::String, span);
            assert_eq!(token.span(), span);
        }
    }

    #[test]
    fn compact_token_text_keeps_utf8_ranges_and_named_parameter_boundaries() {
        let input = "mAtCh '你好é' $name";
        let tokens = tokenize(input).unwrap();
        assert!(tokens[0].is_keyword(input, "MATCH"));
        assert_eq!(tokens[1].text(input), "'你好é'");
        assert_eq!(tokens[2].kind, TokenKind::Parameter);
        assert_eq!(tokens[2].text(input), "$");
        assert_eq!(tokens[3].text(input), "name");
        assert_eq!(tokens[4].span(), Span::new(input.len(), input.len()));
        for token in tokens {
            assert!(input.is_char_boundary(token.span().start));
            assert!(input.is_char_boundary(token.span().end));
        }
    }
}
