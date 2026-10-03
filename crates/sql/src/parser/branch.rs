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
    BranchSqlSelector, BranchSqlStatement, BranchSqlValue, CreateBranchStatement,
    DropBranchStatement, ShowBranchStatement, ShowBranchesStatement, SqlBound, SqlStatement,
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
    if !matches_keyword(first, &["SHOW", "USE", "CREATE", "DROP"]) {
        return Ok(None);
    }
    let Some(second) = tokens.get(1) else {
        return Ok(None);
    };
    if !matches_keyword(second, &["BRANCHES", "BRANCH", "CURRENT"]) {
        return Ok(None);
    }

    let mut parser = BranchParser {
        tokens: &tokens,
        cursor: 0,
    };
    let statement = if parser.consume_keyword("USE") {
        parser.expect_keyword("BRANCH")?;
        BranchSqlStatement::UseBranch(parser.parse_selection_selector()?)
    } else if parser.consume_keyword("CREATE") {
        parser.expect_keyword("BRANCH")?;
        let name = if parser.consume_keyword("FROM") {
            None
        } else {
            let name = if parser.consume_keyword("NAME") {
                parser.parse_selector_value()?
            } else {
                parser.parse_name_identifier()?
            };
            parser.expect_keyword("FROM")?;
            Some(name)
        };
        let source = parser.parse_selection_selector()?;
        parser.expect_keyword("AT")?;
        parser.expect_keyword("REVISION")?;
        let expected_source_revision = parser.parse_bound()?;
        parser.expect_keyword("REQUEST")?;
        parser.expect_keyword("KEY")?;
        let request_key = parser.parse_selector_value()?;
        let owner = if parser.consume_keyword("OWNER") {
            Some(parser.parse_selector_value()?)
        } else {
            None
        };
        BranchSqlStatement::CreateBranch(CreateBranchStatement {
            name,
            source,
            expected_source_revision,
            request_key,
            owner,
        })
    } else if parser.consume_keyword("DROP") {
        parser.expect_keyword("BRANCH")?;
        parser.expect_keyword("ID")?;
        let id = parser.parse_selector_value()?;
        parser.expect_keyword("AT")?;
        parser.expect_keyword("REVISION")?;
        BranchSqlStatement::DropBranch(DropBranchStatement {
            id,
            expected_metadata_revision: parser.parse_bound()?,
        })
    } else {
        parser.expect_keyword("SHOW")?;
        if parser.consume_keyword("BRANCHES") {
            let (limit, offset) = parser.parse_page_bounds()?;
            BranchSqlStatement::ShowBranches(ShowBranchesStatement { limit, offset })
        } else if parser.consume_keyword("BRANCH") {
            BranchSqlStatement::ShowBranch(ShowBranchStatement {
                selector: parser.parse_selector()?,
            })
        } else if parser.consume_keyword("CURRENT") {
            parser.expect_keyword("BRANCH")?;
            BranchSqlStatement::ShowCurrentBranch
        } else {
            return Err(HawDBError::Parse(
                "SHOW supports only BRANCHES or BRANCH in the current branch SQL surface"
                    .to_string(),
            ));
        }
    };
    parser.expect_end()?;
    Ok(Some(SqlStatement::Branch(statement)))
}

struct BranchParser<'a> {
    tokens: &'a [Token],
    cursor: usize,
}

impl BranchParser<'_> {
    fn parse_selection_selector(&mut self) -> Result<BranchSqlSelector> {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| matches_keyword(token, &["NAME", "ID"]))
        {
            self.parse_selector()
        } else {
            self.parse_name_identifier().map(BranchSqlSelector::Name)
        }
    }

    fn parse_name_identifier(&mut self) -> Result<BranchSqlValue> {
        match self.next() {
            Some(Token::Word(word)) => Ok(BranchSqlValue::Literal(word.value.clone())),
            _ => Err(HawDBError::Parse(
                "branch name requires an identifier or NAME followed by a string value".into(),
            )),
        }
    }

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
            "branch selector requires NAME or ID followed by a string literal or positional parameter"
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
    let keyword = input
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .next()
        .unwrap_or("");
    ["SHOW", "USE", "CREATE", "DROP"]
        .iter()
        .any(|expected| keyword.eq_ignore_ascii_case(expected))
}

fn is_keyword(token: &Token, expected: &str) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}

fn matches_keyword(token: &Token, expected: &[&str]) -> bool {
    expected.iter().any(|expected| is_keyword(token, expected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_postgres_sql, prepare_postgres_sql};

    #[test]
    fn parses_parameterized_branch_lifecycle_and_selection() {
        assert_eq!(
            parse_postgres_sql("USE BRANCH dev").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::UseBranch(BranchSqlSelector::Name(
                BranchSqlValue::Literal("dev".into())
            )))
        );
        assert_eq!(
            parse_postgres_sql("USE BRANCH \"MixedCase/child\"").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::UseBranch(BranchSqlSelector::Name(
                BranchSqlValue::Literal("MixedCase/child".into())
            )))
        );
        assert!(matches!(
            parse_postgres_sql("CREATE BRANCH dev FROM main AT REVISION 1 REQUEST KEY 'key'")
                .unwrap(),
            SqlStatement::Branch(BranchSqlStatement::CreateBranch(_))
        ));
        assert_eq!(
            parse_postgres_sql("SHOW CURRENT BRANCH").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::ShowCurrentBranch)
        );
        assert_eq!(
            parse_postgres_sql("/* select */ USE BRANCH NAME $1;").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::UseBranch(BranchSqlSelector::Name(
                BranchSqlValue::Parameter(1)
            )))
        );
        assert_eq!(
            parse_postgres_sql("USE BRANCH ID '123e4567-e89b-12d3-a456-426614174000'").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::UseBranch(BranchSqlSelector::Id(
                BranchSqlValue::Literal("123e4567-e89b-12d3-a456-426614174000".into())
            )))
        );
        let prepared = prepare_postgres_sql(
            "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4 OWNER $5",
        )
        .unwrap();
        assert_eq!(
            prepared
                .parameters
                .iter()
                .map(|parameter| parameter.position)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        let SqlStatement::Branch(BranchSqlStatement::CreateBranch(create)) = prepared.statement
        else {
            panic!("expected CREATE BRANCH")
        };
        assert_eq!(create.expected_source_revision, SqlBound::Parameter(3));
        assert_eq!(create.name, Some(BranchSqlValue::Parameter(1)));
        assert!(matches!(
            parse_postgres_sql(
                "CREATE BRANCH FROM NAME 'main' AT REVISION 7 REQUEST KEY 'retry-1'"
            )
            .unwrap(),
            SqlStatement::Branch(BranchSqlStatement::CreateBranch(
                super::CreateBranchStatement { name: None, .. }
            ))
        ));
        assert_eq!(
            prepare_postgres_sql("DROP BRANCH ID $1 AT REVISION $2")
                .unwrap()
                .parameters
                .len(),
            2
        );
        // Ordinary relational DDL still uses its own parser.
        assert!(matches!(
            parse_postgres_sql("CREATE TABLE documents (id BIGINT PRIMARY KEY)").unwrap(),
            SqlStatement::CreateTable(_)
        ));
    }

    #[test]
    fn branch_lifecycle_requires_identity_revision_and_request_key() {
        for sql in [
            "USE BRANCH",
            "USE BRANCH NAME NULL",
            "USE BRANCH NAME $0",
            "USE BRANCH NAME 'dev'; SELECT 1",
            "SHOW CURRENT BRANCH LIMIT 1",
            "CREATE BRANCH NAME 'dev' FROM NAME 'main'",
            "CREATE BRANCH NAME 'dev' FROM NAME 'main' AT REVISION 7",
            "CREATE BRANCH NAME 'dev' FROM NAME 'main' AT REVISION -1 REQUEST KEY 'key'",
            "CREATE BRANCH FROM ID $1 AT REVISION $3 REQUEST KEY $4",
            "DROP BRANCH NAME 'dev' AT REVISION 2",
            "DROP BRANCH ID $1",
            "DROP BRANCH ID $1 AT REVISION 1 AT REVISION 2",
        ] {
            assert!(
                prepare_postgres_sql(sql).is_err(),
                "expected rejection: {sql}"
            );
        }
    }
}
