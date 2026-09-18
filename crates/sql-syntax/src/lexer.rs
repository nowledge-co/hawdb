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

use crate::{Span, SyntaxError, SyntaxErrorCode, Token, TokenKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexerConfig {
    pub max_input_bytes: usize,
    pub max_tokens: usize,
    pub max_block_comment_depth: usize,
}

impl Default for LexerConfig {
    fn default() -> Self {
        Self {
            max_input_bytes: 16 * 1024 * 1024,
            max_tokens: 1_000_000,
            max_block_comment_depth: 64,
        }
    }
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, SyntaxError> {
    tokenize_with_config(input, LexerConfig::default())
}

pub fn tokenize_with_config(input: &str, config: LexerConfig) -> Result<Vec<Token>, SyntaxError> {
    if input.len() > config.max_input_bytes {
        return Err(SyntaxError::new(
            SyntaxErrorCode::InputTooLarge,
            Span::new(0, input.len()),
        ));
    }

    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut position = 0;

    while position < bytes.len() {
        let byte = bytes[position];
        if byte.is_ascii_whitespace() {
            position += 1;
            continue;
        }

        if byte == b'-' && bytes.get(position + 1) == Some(&b'-') {
            position += 2;
            while position < bytes.len() && !matches!(bytes[position], b'\n' | b'\r') {
                position += 1;
            }
            continue;
        }

        if byte == b'/' && bytes.get(position + 1) == Some(&b'*') {
            position = skip_block_comment(input, position, config.max_block_comment_depth)?;
            continue;
        }

        let start = position;
        let kind = match byte {
            b'"' => {
                position = scan_quoted(
                    input,
                    position,
                    b'"',
                    SyntaxErrorCode::UnterminatedQuotedIdentifier,
                )?;
                TokenKind::QuotedIdentifier
            }
            b'\'' => {
                position =
                    scan_quoted(input, position, b'\'', SyntaxErrorCode::UnterminatedString)?;
                TokenKind::String
            }
            b'$' => {
                if bytes.get(position + 1).is_some_and(u8::is_ascii_digit) {
                    let (next, parameter) = scan_parameter(input, position)?;
                    position = next;
                    TokenKind::Parameter(parameter)
                } else if let Some(next) = scan_dollar_quoted_string(input, position)? {
                    position = next;
                    TokenKind::String
                } else {
                    return Err(SyntaxError::new(
                        SyntaxErrorCode::InvalidParameter,
                        Span::new(position, (position + 1).min(input.len())),
                    )
                    .expected("a one-based parameter or dollar-quoted string"));
                }
            }
            b'0'..=b'9' => {
                position = scan_number(input, position);
                TokenKind::Number
            }
            b'(' => single(&mut position, TokenKind::LeftParen),
            b')' => single(&mut position, TokenKind::RightParen),
            b'[' => single(&mut position, TokenKind::LeftBracket),
            b']' => single(&mut position, TokenKind::RightBracket),
            b'{' => single(&mut position, TokenKind::LeftBrace),
            b'}' => single(&mut position, TokenKind::RightBrace),
            b',' => single(&mut position, TokenKind::Comma),
            b'.' => single(&mut position, TokenKind::Dot),
            b';' => single(&mut position, TokenKind::Semicolon),
            b'+' => single(&mut position, TokenKind::Plus),
            b'*' => single(&mut position, TokenKind::Star),
            b'%' => single(&mut position, TokenKind::Percent),
            b'&' => single(&mut position, TokenKind::Ampersand),
            b'^' => single(&mut position, TokenKind::Caret),
            b'?' => single(&mut position, TokenKind::Question),
            b':' if bytes.get(position + 1) == Some(&b':') => {
                position += 2;
                TokenKind::DoubleColon
            }
            b':' => single(&mut position, TokenKind::Colon),
            b'|' if bytes.get(position + 1) == Some(&b'|') => {
                position += 2;
                TokenKind::Concat
            }
            b'|' => single(&mut position, TokenKind::Pipe),
            b'=' => single(&mut position, TokenKind::Equal),
            b'!' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::NotEqual
            }
            b'<' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::LessOrEqual
            }
            b'<' if bytes.get(position + 1) == Some(&b'>') => {
                position += 2;
                TokenKind::NotEqual
            }
            b'<' if bytes.get(position + 1) == Some(&b'-') => {
                position += 2;
                TokenKind::ArrowLeft
            }
            b'<' => single(&mut position, TokenKind::Less),
            b'>' if bytes.get(position + 1) == Some(&b'=') => {
                position += 2;
                TokenKind::GreaterOrEqual
            }
            b'>' => single(&mut position, TokenKind::Greater),
            b'-' if bytes.get(position + 1) == Some(&b'>') => {
                position += 2;
                TokenKind::ArrowRight
            }
            b'-' => single(&mut position, TokenKind::Minus),
            b'/' => single(&mut position, TokenKind::Slash),
            _ => {
                let character = input[position..].chars().next().ok_or_else(|| {
                    SyntaxError::new(
                        SyntaxErrorCode::InvalidCharacter,
                        Span::new(position, position),
                    )
                })?;
                if is_identifier_start(character) {
                    position = scan_identifier(input, position);
                    TokenKind::Word
                } else {
                    let end = position + character.len_utf8();
                    return Err(SyntaxError::new(
                        SyntaxErrorCode::InvalidCharacter,
                        Span::new(position, end),
                    )
                    .found(character.to_string()));
                }
            }
        };

        push_token(
            &mut tokens,
            Token::new(kind, Span::new(start, position)),
            config.max_tokens,
        )?;
    }

    tokens.push(Token::new(
        TokenKind::End,
        Span::new(input.len(), input.len()),
    ));
    Ok(tokens)
}

fn single(position: &mut usize, kind: TokenKind) -> TokenKind {
    *position += 1;
    kind
}

fn push_token(tokens: &mut Vec<Token>, token: Token, max_tokens: usize) -> Result<(), SyntaxError> {
    if tokens.len() >= max_tokens {
        return Err(SyntaxError::new(
            SyntaxErrorCode::TokenLimitExceeded,
            token.span,
        ));
    }
    tokens.push(token);
    Ok(())
}

fn skip_block_comment(input: &str, start: usize, max_depth: usize) -> Result<usize, SyntaxError> {
    if max_depth == 0 {
        return Err(SyntaxError::new(
            SyntaxErrorCode::CommentNestingLimitExceeded,
            Span::new(start, (start + 2).min(input.len())),
        ));
    }
    let bytes = input.as_bytes();
    let mut position = start + 2;
    let mut depth = 1usize;
    while position < bytes.len() {
        match (bytes[position], bytes.get(position + 1).copied()) {
            (b'/', Some(b'*')) => {
                depth = depth.checked_add(1).ok_or_else(|| {
                    SyntaxError::new(
                        SyntaxErrorCode::CommentNestingLimitExceeded,
                        Span::new(start, position + 2),
                    )
                })?;
                if depth > max_depth {
                    return Err(SyntaxError::new(
                        SyntaxErrorCode::CommentNestingLimitExceeded,
                        Span::new(start, position + 2),
                    ));
                }
                position += 2;
            }
            (b'*', Some(b'/')) => {
                depth -= 1;
                position += 2;
                if depth == 0 {
                    return Ok(position);
                }
            }
            _ => position += 1,
        }
    }
    Err(SyntaxError::new(
        SyntaxErrorCode::UnterminatedBlockComment,
        Span::new(start, input.len()),
    ))
}

fn scan_quoted(
    input: &str,
    start: usize,
    quote: u8,
    error_code: SyntaxErrorCode,
) -> Result<usize, SyntaxError> {
    let bytes = input.as_bytes();
    let mut position = start + 1;
    while position < bytes.len() {
        if bytes[position] == quote {
            if bytes.get(position + 1) == Some(&quote) {
                position += 2;
            } else {
                return Ok(position + 1);
            }
        } else {
            position += 1;
        }
    }
    Err(SyntaxError::new(error_code, Span::new(start, input.len())))
}

fn scan_parameter(input: &str, start: usize) -> Result<(usize, u32), SyntaxError> {
    let bytes = input.as_bytes();
    let mut position = start + 1;
    while bytes.get(position).is_some_and(u8::is_ascii_digit) {
        position += 1;
    }
    let value = input[start + 1..position].parse::<u32>().map_err(|_| {
        SyntaxError::new(
            SyntaxErrorCode::InvalidParameter,
            Span::new(start, position),
        )
    })?;
    if value == 0 {
        return Err(SyntaxError::new(
            SyntaxErrorCode::InvalidParameter,
            Span::new(start, position),
        )
        .expected("a one-based PostgreSQL parameter"));
    }
    Ok((position, value))
}

fn scan_dollar_quoted_string(input: &str, start: usize) -> Result<Option<usize>, SyntaxError> {
    let bytes = input.as_bytes();
    let mut delimiter_end = start + 1;
    while let Some(byte) = bytes.get(delimiter_end) {
        if *byte == b'$' {
            let delimiter = &input[start..=delimiter_end];
            let content_start = delimiter_end + 1;
            return input[content_start..]
                .find(delimiter)
                .map(|offset| Some(content_start + offset + delimiter.len()))
                .ok_or_else(|| {
                    SyntaxError::new(
                        SyntaxErrorCode::UnterminatedDollarQuotedString,
                        Span::new(start, input.len()),
                    )
                });
        }
        if !byte.is_ascii_alphanumeric() && *byte != b'_' {
            return Ok(None);
        }
        delimiter_end += 1;
    }
    Ok(None)
}

fn scan_number(input: &str, start: usize) -> usize {
    let bytes = input.as_bytes();
    let mut position = start;
    while bytes.get(position).is_some_and(u8::is_ascii_digit) {
        position += 1;
    }
    if bytes.get(position) == Some(&b'.') && bytes.get(position + 1).is_some_and(u8::is_ascii_digit)
    {
        position += 1;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
    }
    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        let exponent = position;
        position += 1;
        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let digit_start = position;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
        if digit_start == position {
            return exponent;
        }
    }
    position
}

fn scan_identifier(input: &str, start: usize) -> usize {
    let mut end = start;
    for (offset, character) in input[start..].char_indices() {
        if offset == 0 {
            if !is_identifier_start(character) {
                break;
            }
        } else if !is_identifier_continue(character) {
            break;
        }
        end = start + offset + character.len_utf8();
    }
    end
}

fn is_identifier_start(character: char) -> bool {
    character == '_' || character.is_alphabetic()
}

fn is_identifier_continue(character: char) -> bool {
    is_identifier_start(character) || character.is_numeric() || character == '$'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(input: &str) -> Vec<TokenKind> {
        tokenize(input)
            .expect("valid SQL")
            .into_iter()
            .map(|token| token.kind)
            .collect()
    }

    #[test]
    fn tokenizes_postgres_sql_pgq_surface() {
        assert_eq!(
            kinds(
                "SELECT p.name FROM GRAPH_TABLE (g MATCH (p IS person)-[e IS knows]->(q) \
                 COLUMNS (p.name AS name, $1)) AS gt;",
            ),
            vec![
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Dot,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::LeftParen,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::LeftParen,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::RightParen,
                TokenKind::Minus,
                TokenKind::LeftBracket,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::RightBracket,
                TokenKind::ArrowRight,
                TokenKind::LeftParen,
                TokenKind::Word,
                TokenKind::RightParen,
                TokenKind::Word,
                TokenKind::LeftParen,
                TokenKind::Word,
                TokenKind::Dot,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Comma,
                TokenKind::Parameter(1),
                TokenKind::RightParen,
                TokenKind::RightParen,
                TokenKind::Word,
                TokenKind::Word,
                TokenKind::Semicolon,
                TokenKind::End,
            ]
        );
    }

    #[test]
    fn skips_nested_comments_and_preserves_byte_spans() {
        let input = "/* outer /* nested */ done */ SELECT \"na\"\"me\"";
        let tokens = tokenize(input).expect("valid nested comment");
        assert_eq!(tokens[0].text(input), "SELECT");
        assert_eq!(tokens[1].kind, TokenKind::QuotedIdentifier);
        assert_eq!(tokens[1].text(input), "\"na\"\"me\"");
    }

    #[test]
    fn recognizes_dollar_quoted_strings_and_unicode_identifiers() {
        let input = "SELECT 数据, $tag$MATCH (n)$tag$";
        let tokens = tokenize(input).expect("valid PostgreSQL tokens");
        assert_eq!(tokens[1].text(input), "数据");
        assert_eq!(tokens[3].kind, TokenKind::String);
    }

    #[test]
    fn rejects_zero_parameter_and_unterminated_input() {
        let error = tokenize("SELECT $0").expect_err("zero parameter must fail");
        assert_eq!(error.code, SyntaxErrorCode::InvalidParameter);

        let error = tokenize("SELECT /*").expect_err("comment must terminate");
        assert_eq!(error.code, SyntaxErrorCode::UnterminatedBlockComment);
    }

    #[test]
    fn enforces_token_and_comment_depth_limits() {
        let error = tokenize_with_config(
            "SELECT one",
            LexerConfig {
                max_tokens: 1,
                ..LexerConfig::default()
            },
        )
        .expect_err("token budget must fail");
        assert_eq!(error.code, SyntaxErrorCode::TokenLimitExceeded);

        let error = tokenize_with_config(
            "/* /* nested */ */",
            LexerConfig {
                max_block_comment_depth: 1,
                ..LexerConfig::default()
            },
        )
        .expect_err("comment depth must fail");
        assert_eq!(error.code, SyntaxErrorCode::CommentNestingLimitExceeded);

        let error = tokenize_with_config(
            "/**/",
            LexerConfig {
                max_block_comment_depth: 0,
                ..LexerConfig::default()
            },
        )
        .expect_err("zero comment budget must fail");
        assert_eq!(error.code, SyntaxErrorCode::CommentNestingLimitExceeded);
    }
}
