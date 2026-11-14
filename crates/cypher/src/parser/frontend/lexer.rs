// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::Result;
use hawdb_sql_syntax::Span;

use super::parse_error;

// A nonempty token is at most 16 MiB. Its length minus one fits in 24 bits;
// the End token instead has length zero and retains its full u32 byte offset.
const _: () = assert!(super::MAX_CYPHER_INPUT_BYTES <= 1 << 24);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum TokenKind {
    Word,
    String,
    UnterminatedString,
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
    length_minus_one: [u8; 3],
}

const _: () = assert!(std::mem::size_of::<Token>() == 8);

impl Token {
    fn new(kind: TokenKind, span: Span) -> Self {
        let length = span
            .end
            .checked_sub(span.start)
            .expect("ordered Cypher token span");
        let encoded = if kind == TokenKind::End {
            assert_eq!(length, 0, "end token has no source bytes");
            0
        } else {
            length.checked_sub(1).expect("nonempty Cypher token")
        };
        let bytes = u32::try_from(encoded)
            .expect("bounded Cypher token length")
            .to_le_bytes();
        assert_eq!(bytes[3], 0, "Cypher token length fits in 24 bits");
        Self {
            kind,
            start: u32::try_from(span.start).expect("bounded Cypher token start"),
            length_minus_one: [bytes[0], bytes[1], bytes[2]],
        }
    }

    pub(super) fn span(self) -> Span {
        let start = self.start as usize;
        let length = if self.kind == TokenKind::End {
            0
        } else {
            let [a, b, c] = self.length_minus_one;
            u32::from_le_bytes([a, b, c, 0]) as usize + 1
        };
        Span::new(start, start + length)
    }

    pub(super) fn text(self, input: &str) -> &str {
        let span = self.span();
        input.get(span.start..span.end).unwrap_or_default()
    }

    pub(super) fn is_keyword(self, input: &str, keyword: &str) -> bool {
        self.kind == TokenKind::Word && self.text(input).eq_ignore_ascii_case(keyword)
    }
}

pub(super) fn decode_escape(ch: char) -> Option<char> {
    match ch {
        '\\' | '\'' | '"' => Some(ch),
        'n' => Some('\n'),
        'r' => Some('\r'),
        't' => Some('\t'),
        _ => None,
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
            // Preserve escapes for AST folding. The grammar diagnoses an
            // unterminated token only when a string is admitted at this position.
            let mut terminated = false;
            let mut valid_escapes = true;
            while let Some((offset, next)) = chars.next() {
                end = offset + next.len_utf8();
                if next == ch {
                    terminated = true;
                    break;
                }
                if next == '\\' {
                    if let Some((offset, escaped)) = chars.next() {
                        end = offset + escaped.len_utf8();
                        valid_escapes &= decode_escape(escaped).is_some();
                    } else {
                        valid_escapes = false;
                    }
                }
            }
            // Invalid escapes keep their original lowering-time diagnostic.
            if terminated || !valid_escapes {
                TokenKind::String
            } else {
                TokenKind::UnterminatedString
            }
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
        assert_eq!(std::mem::size_of::<Token>(), 8);
        let end = super::super::MAX_CYPHER_INPUT_BYTES;
        for span in [Span::new(0, 1), Span::new(end - 1, end), Span::new(0, end)] {
            let token = Token::new(TokenKind::String, span);
            assert_eq!(token.span(), span);
        }
        for offset in [0, end - 1, end] {
            let span = Span::new(offset, offset);
            assert_eq!(Token::new(TokenKind::End, span).span(), span);
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
