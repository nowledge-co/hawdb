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

//! Conservative admission before recursive upstream AST construction/lowering.

use hawdb_core::{HawDBError, Result};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer};

/// Uses the same dialect/tokenizer as the execution frontend. This is a work
/// gate, not a different semantic parser or a parser fallback. Input bytes bound
/// tokenizer scratch before its allocation; token and conservative recursive
/// counts then bound subsequent parser/lowering/drop work, including cache hits.
/// Keywords are conservatively counted even when they occupy leaf positions.
#[doc(hidden)]
pub fn admit_postgres_sql_syntax(
    input: &str,
    max_input_bytes: usize,
    max_tokens: usize,
    max_recursive_tokens: usize,
) -> Result<()> {
    let refusal = || HawDBError::Execution("recursive SQL syntax exceeding work admission".into());
    if input.len() > max_input_bytes {
        return Err(refusal());
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, input)
        .tokenize_with_location()
        .map_err(|error| {
            HawDBError::Parse(format!("failed to tokenize PostgreSQL SQL: {error}"))
        })?;
    let mut count = 0usize;
    let mut recursive = 0usize;
    for token in tokens {
        if matches!(token.token, Token::Whitespace(_) | Token::EOF) {
            continue;
        }
        count = count.saturating_add(1);
        let grows = match token.token {
            Token::Word(word) => word.quote_style.is_none() && word.keyword != Keyword::NoKeyword,
            Token::Number(_, _)
            | Token::Placeholder(_)
            | Token::Comma
            | Token::Period
            | Token::SemiColon
            | Token::RParen
            | Token::RBracket
            | Token::RBrace
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_)
            | Token::SingleQuotedByteStringLiteral(_) => false,
            _ => true,
        };
        recursive = recursive.saturating_add(usize::from(grows));
        if count > max_tokens || recursive > max_recursive_tokens {
            return Err(refusal());
        }
    }
    Ok(())
}
