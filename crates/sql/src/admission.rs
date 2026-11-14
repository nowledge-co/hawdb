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

//! Conservative admission before PEG matching, AST lowering and cache lookup.

use hawdb_core::{HawDBError, Result};
use hawdb_sql_syntax::{self as syntax, LexerConfig, SyntaxErrorCode, Token, TokenKind as T};
use std::sync::OnceLock;

// Freeze the former sqlparser 0.63 work-admission keyword inventory. These are
// keyword names, not parser code or a grammar: changing a dependency must not
// silently change which identifier leaves consume a host's recursive budget.
fn keywords() -> &'static [&'static str] {
    static KEYWORDS: OnceLock<Vec<&'static str>> = OnceLock::new();
    KEYWORDS.get_or_init(|| include_str!("admission_keywords_v1.txt").lines().collect())
}
fn keyword(word: &str) -> bool {
    if !word.is_ascii() {
        // Preserve the former Unicode-uppercase classification of identifier
        // leaves too (for example dotless-i + N still consumes a keyword unit).
        return keywords()
            .binary_search(&word.to_uppercase().as_str())
            .is_ok();
    }
    if word.len() > 40 {
        return false;
    }
    let mut uppercase = [0_u8; 40];
    for (dst, src) in uppercase.iter_mut().zip(word.bytes()) {
        *dst = src.to_ascii_uppercase();
    }
    let word = &uppercase[..word.len()];
    keywords()
        .binary_search_by(|candidate| candidate.as_bytes().cmp(word))
        .is_ok()
}

/// A work gate over the frontend's lexer, not a second semantic parser.
/// Byte admission precedes scratch allocation; token and conservative recursive
/// counts bound subsequent matching/lowering/drop work, including cache hits.
/// Keyword leaves retain their historical conservative charge.
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
    let tokens = syntax::tokenize_with_config(
        input,
        LexerConfig {
            max_input_bytes,
            // Retain lexical-error priority. Input bytes already bound this arena;
            // work admission below counts tokens after complete lexical validation.
            max_tokens: input.len(),
            ..LexerConfig::default()
        },
    )
    .map_err(|error| {
        if error.code == SyntaxErrorCode::InputTooLarge
            || error.code == SyntaxErrorCode::TokenLimitExceeded
        {
            refusal()
        } else {
            HawDBError::Parse(error.to_string())
        }
    })?;
    let (mut count, mut recursive, mut at) = (0_usize, 0_usize, 0_usize);
    while let Some(token) = tokens.get(at) {
        if token.kind == T::End {
            break;
        }
        count = count.saturating_add(1);
        let grows = match token.kind {
            T::Word => keyword(token.text(input)),
            T::QuotedIdentifier
            | T::Number
            | T::Parameter(_)
            | T::Comma
            | T::Dot
            | T::Semicolon
            | T::RightParen
            | T::RightBracket
            | T::RightBrace
            | T::String
            | T::Question => false,
            _ => true,
        };
        recursive = recursive.saturating_add(usize::from(grows));
        // The expression grammar exposes these as two tokens; the prior
        // admission lexer counted each contiguous operator as one token.
        if tokens
            .get(at + 1)
            .is_some_and(|next| paired_operator(*token, *next))
        {
            at += 1;
        }
        at += 1;
        if count > max_tokens || recursive > max_recursive_tokens {
            return Err(refusal());
        }
    }
    Ok(())
}
fn paired_operator(left: Token, right: Token) -> bool {
    left.span.end == right.span.start
        && matches!(
            (left.kind, right.kind),
            (T::Equal, T::Greater) | (T::Colon, T::Equal)
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keyword_work_inventory_is_sorted_unique_and_case_insensitive() {
        let words: Vec<_> = include_str!("admission_keywords_v1.txt").lines().collect();
        assert_eq!(words.len(), 1102);
        assert!(words.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(words.iter().map(|word| word.len()).max(), Some(40));
        for word in words {
            assert!(keyword(word));
            assert!(keyword(&word.to_ascii_lowercase()));
        }
        assert!(keyword("ıN"));
        assert!(keyword("ſELECT"));
        for word in [
            "row_id",
            "列",
            "select_value",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(!keyword(word));
        }
    }
    #[test]
    fn declared_keyword_and_operator_work_boundaries_are_preserved() {
        // VALUE, ITEMS and ID deliberately consume keyword units even here
        // as leaves: the frozen preflight contract is conservative.
        let input = "SELECT value FROM items WHERE id = $1";
        assert!(admit_postgres_sql_syntax(input, input.len(), 8, 7).is_ok());
        assert!(matches!(
            admit_postgres_sql_syntax(input, input.len(), 7, 7),
            Err(HawDBError::Execution(_))
        ));
        assert!(matches!(
            admit_postgres_sql_syntax(input, input.len(), 8, 6),
            Err(HawDBError::Execution(_))
        ));
        for input in ["top_k => 1", "top_k := 1"] {
            assert!(admit_postgres_sql_syntax(input, input.len(), 3, 1).is_ok());
            assert!(admit_postgres_sql_syntax(input, input.len(), 2, 1).is_err());
        }
        assert!(admit_postgres_sql_syntax("top_k = > 1", 32, 3, 1).is_err());
        assert!(admit_postgres_sql_syntax("\"SELECT\"", 32, 1, 0).is_ok());
    }
}
