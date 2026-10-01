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

use crate::{
    BranchSqlSelector, BranchSqlStatement, BranchSqlValue, ShowBranchStatement,
    ShowBranchesStatement, SqlBound, SqlStatement,
};
use hawdb_core::{HawDBError, Result};
use sqlparser::{
    dialect::PostgreSqlDialect,
    tokenizer::{Token, Tokenizer, Whitespace},
};

/// Parses the branch-only SQL surface before the relational PostgreSQL parser.
/// `sqlparser` intentionally has no dialect extension point for these project
/// commands, so this consumes its token stream and produces the same public
/// HawDB SQL AST used by normal statement dispatch.
pub(super) fn parse_branch_statement(input: &str) -> Result<Option<SqlStatement>> {
    if !is_branch_command_candidate(input) {
        return Ok(None);
    }
    let dialect = PostgreSqlDialect {};
    let tokens = Tokenizer::new(&dialect, input)
        .tokenize()
        .map_err(|error| HawDBError::Parse(format!("failed to tokenize PostgreSQL SQL: {error}")))?
        .into_iter()
        .filter(|token| {
            !matches!(
                token,
                Token::Whitespace(
                    Whitespace::Space
                        | Whitespace::Newline
                        | Whitespace::Tab
                        | Whitespace::SingleLineComment { .. }
                        | Whitespace::MultiLineComment(_)
                )
            )
        })
        .collect::<Vec<_>>();
    let Some(first) = tokens.first() else {
        return Ok(None);
    };
    if !is_keyword(first, "SHOW") {
        return Ok(None);
    }
    let Some(second) = tokens.get(1) else {
        return Ok(None);
    };
    if !matches_keyword(second, &["BRANCHES", "BRANCH"]) {
        return Ok(None);
    }

    let mut parser = BranchParser {
        tokens: &tokens,
        cursor: 0,
    };
    parser.expect_keyword("SHOW")?;
    let statement = if parser.consume_keyword("BRANCHES") {
        let (limit, offset) = parser.parse_page_bounds()?;
        BranchSqlStatement::ShowBranches(ShowBranchesStatement { limit, offset })
    } else if parser.consume_keyword("BRANCH") {
        BranchSqlStatement::ShowBranch(ShowBranchStatement {
            selector: parser.parse_selector()?,
        })
    } else {
        return Err(HawDBError::Parse(
            "SHOW supports only BRANCHES or BRANCH in the current branch SQL surface".to_string(),
        ));
    };
    parser.expect_end()?;
    Ok(Some(SqlStatement::Branch(statement)))
}

struct BranchParser<'a> {
    tokens: &'a [Token],
    cursor: usize,
}

impl BranchParser<'_> {
    fn parse_page_bounds(&mut self) -> Result<(SqlBound, Option<SqlBound>)> {
        let mut limit = None;
        let mut offset = None;
        while !self.at_end() {
            if self.consume_keyword("LIMIT") {
                if limit.replace(self.parse_bound()?).is_some() {
                    return Err(HawDBError::Parse(
                        "SHOW BRANCHES accepts LIMIT only once".to_string(),
                    ));
                }
            } else if self.consume_keyword("OFFSET") {
                if offset.replace(self.parse_bound()?).is_some() {
                    return Err(HawDBError::Parse(
                        "SHOW BRANCHES accepts OFFSET only once".to_string(),
                    ));
                }
            } else {
                return Err(HawDBError::Parse(
                    "SHOW BRANCHES expects LIMIT or OFFSET".to_string(),
                ));
            }
        }
        let limit = limit.ok_or_else(|| {
            HawDBError::Parse("SHOW BRANCHES requires an explicit LIMIT".to_string())
        })?;
        Ok((limit, offset))
    }

    fn parse_bound(&mut self) -> Result<SqlBound> {
        match self.next() {
            Some(Token::Number(value, false)) => value
                .replace('_', "")
                .parse::<u64>()
                .map(SqlBound::Literal)
                .map_err(|_| {
                    HawDBError::Parse(format!(
                        "branch SQL bound {value} is outside the supported range"
                    ))
                }),
            Some(Token::Placeholder(value)) => {
                super::postgres_parameter_position(value).map(SqlBound::Parameter)
            }
            _ => Err(HawDBError::Parse(
                "branch SQL bounds must be an unsigned integer literal or positional parameter"
                    .to_string(),
            )),
        }
    }

    fn parse_selector(&mut self) -> Result<BranchSqlSelector> {
        if self.consume_keyword("NAME") {
            return self.parse_selector_value().map(BranchSqlSelector::Name);
        }
        if self.consume_keyword("ID") {
            return self.parse_selector_value().map(BranchSqlSelector::Id);
        }
        Err(HawDBError::Parse(
            "SHOW BRANCH requires NAME or ID followed by a string literal or positional parameter"
                .to_string(),
        ))
    }

    fn parse_selector_value(&mut self) -> Result<BranchSqlValue> {
        match self.next() {
            Some(Token::SingleQuotedString(value)) => Ok(BranchSqlValue::Literal(value.clone())),
            Some(Token::Placeholder(value)) => {
                super::postgres_parameter_position(value).map(BranchSqlValue::Parameter)
            }
            _ => Err(HawDBError::Parse(
                "branch selector values must be a string literal or positional parameter"
                    .to_string(),
            )),
        }
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| is_keyword(token, expected))
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, expected: &str) -> Result<()> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(HawDBError::Parse(format!(
                "expected branch SQL keyword {expected}"
            )))
        }
    }

    fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.cursor)?;
        self.cursor += 1;
        Some(token)
    }

    fn expect_end(&mut self) -> Result<()> {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| matches!(token, Token::SemiColon))
        {
            self.cursor += 1;
        }
        if self.cursor == self.tokens.len() {
            Ok(())
        } else {
            Err(HawDBError::Parse(
                "unexpected trailing tokens in branch SQL statement".to_string(),
            ))
        }
    }

    fn at_end(&self) -> bool {
        self.tokens
            .get(self.cursor)
            .is_none_or(|token| matches!(token, Token::SemiColon))
    }
}

fn is_branch_command_candidate(mut input: &str) -> bool {
    loop {
        input = input.trim_start();
        if let Some(comment) = input.strip_prefix("--") {
            input = comment.split_once('\n').map_or("", |(_, rest)| rest);
            continue;
        }
        if let Some(comment) = input.strip_prefix("/*") {
            let Some((_, rest)) = comment.split_once("*/") else {
                return false;
            };
            input = rest;
            continue;
        }
        break;
    }
    let Some(keyword) = input.get(..4) else {
        return false;
    };
    if !keyword.eq_ignore_ascii_case("SHOW") {
        return false;
    }
    input
        .get(4..)
        .and_then(|rest| rest.chars().next())
        .is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
}

fn is_keyword(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn matches_keyword(token: &Token, expected: &[&str]) -> bool {
    expected.iter().any(|expected| is_keyword(token, expected))
}
