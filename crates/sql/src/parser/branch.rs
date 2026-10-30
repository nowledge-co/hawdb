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
use hawdb_sql_syntax::{self as syntax, Token, TokenKind as T};
use std::sync::OnceLock;
use syntax::peg::{Expression as E, Grammar, Limits, NodeId, ParseTree};

/// Project branch commands share the bounded SQL lexer and PEG matcher.
pub(super) fn parse_branch_statement(input: &str) -> Result<Option<SqlStatement>> {
    let first = syntax::first_token(input).map_err(parse_error)?;
    if !["SHOW", "USE", "CREATE", "DROP"]
        .iter()
        .any(|word| first.is_keyword(input, word))
    {
        return Ok(None);
    }
    let suffix = &input[first.span.end..];
    let second = syntax::first_token(suffix).map_err(parse_error)?;
    if !["BRANCHES", "BRANCH", "CURRENT"]
        .iter()
        .any(|word| second.is_keyword(suffix, word))
    {
        return Ok(None);
    }
    let tokens = syntax::tokenize(input).map_err(parse_error)?;
    let tree = grammar()
        .parse(input, &tokens, "BranchInput", Limits::default())
        .map_err(parse_error)?;
    let fold = Fold {
        input,
        tokens: &tokens,
        tree: &tree,
    };
    let statement = fold.statement(tree.children(tree.root)[0])?;
    Ok(Some(SqlStatement::Branch(statement)))
}
fn parse_error(error: syntax::SyntaxError) -> HawDBError {
    if error.code == syntax::SyntaxErrorCode::InvalidParameter {
        HawDBError::Semantic("PostgreSQL parameters must use one-based $n syntax".into())
    } else {
        HawDBError::Parse(error.to_string())
    }
}
fn s(items: Vec<E<()>>) -> E<()> {
    E::Sequence(items)
}
fn c(items: Vec<E<()>>) -> E<()> {
    E::Choice(items)
}
fn o(item: E<()>) -> E<()> {
    E::Optional(Box::new(item))
}
fn r(name: &'static str) -> E<()> {
    E::Rule(name)
}
fn k(name: &'static str) -> E<()> {
    E::Keyword(name)
}
fn t(kind: T) -> E<()> {
    E::Token(kind, "a branch SQL token")
}
fn grammar() -> &'static Grammar<()> {
    static GRAMMAR: OnceLock<Grammar<()>> = OnceLock::new();
    GRAMMAR.get_or_init(|| {
        Grammar::compile(vec![
            (
                "BranchInput",
                s(vec![r("Statement"), o(t(T::Semicolon)), t(T::End)]),
            ),
            (
                "Statement",
                c(vec![
                    r("Use"),
                    r("Create"),
                    r("Drop"),
                    r("ShowBranches"),
                    r("ShowBranch"),
                    r("ShowCurrent"),
                ]),
            ),
            ("Use", s(vec![k("USE"), k("BRANCH"), r("Selection")])),
            (
                "Create",
                s(vec![
                    k("CREATE"),
                    k("BRANCH"),
                    o(r("NewName")),
                    k("FROM"),
                    r("Selection"),
                    k("AT"),
                    k("REVISION"),
                    r("Bound"),
                    k("REQUEST"),
                    k("KEY"),
                    r("Value"),
                    o(r("Owner")),
                ]),
            ),
            (
                "NewName",
                c(vec![
                    s(vec![k("NAME"), r("Value")]),
                    s(vec![
                        E::Not(Box::new(k("FROM")), "a new branch name"),
                        r("Identifier"),
                    ]),
                ]),
            ),
            ("Owner", s(vec![k("OWNER"), r("Value")])),
            (
                "Drop",
                s(vec![
                    k("DROP"),
                    k("BRANCH"),
                    k("ID"),
                    r("Value"),
                    k("AT"),
                    k("REVISION"),
                    r("Bound"),
                ]),
            ),
            (
                "ShowBranches",
                s(vec![
                    k("SHOW"),
                    k("BRANCHES"),
                    c(vec![
                        s(vec![r("Limit"), o(r("Offset"))]),
                        s(vec![r("Offset"), r("Limit")]),
                    ]),
                ]),
            ),
            ("Limit", s(vec![k("LIMIT"), r("Bound")])),
            ("Offset", s(vec![k("OFFSET"), r("Bound")])),
            ("ShowBranch", s(vec![k("SHOW"), k("BRANCH"), r("Selector")])),
            ("ShowCurrent", s(vec![k("SHOW"), k("CURRENT"), k("BRANCH")])),
            ("Selection", c(vec![r("Selector"), r("Identifier")])),
            ("Selector", c(vec![r("Name"), r("Id")])),
            ("Name", s(vec![k("NAME"), r("Value")])),
            ("Id", s(vec![k("ID"), r("Value")])),
            ("Identifier", c(vec![t(T::Word), t(T::QuotedIdentifier)])),
            ("Value", c(vec![t(T::String), E::Parameter])),
            ("Bound", c(vec![t(T::Number), E::Parameter])),
        ])
        .expect("valid branch SQL PEG grammar")
    })
}
struct Fold<'a> {
    input: &'a str,
    tokens: &'a [Token],
    tree: &'a ParseTree<()>,
}
impl Fold<'_> {
    fn child(&self, node: NodeId, name: &str) -> NodeId {
        self.tree.required(node, name)
    }
    fn first(&self, node: NodeId) -> NodeId {
        self.tree.children(node)[0]
    }
    fn value(&self, node: NodeId) -> Result<BranchSqlValue> {
        let token = self.tokens[self.tree.nodes[node].start];
        match token.kind {
            T::Parameter(position) => Ok(BranchSqlValue::Parameter(position as usize)),
            T::String => {
                let raw = token.text(self.input);
                if !raw.starts_with('\'') {
                    return Err(HawDBError::Parse(
                        "branch selector values must be a string literal or positional parameter"
                            .into(),
                    ));
                }
                Ok(BranchSqlValue::Literal(
                    raw[1..raw.len() - 1].replace("''", "'"),
                ))
            }
            T::QuotedIdentifier => {
                let raw = token.text(self.input);
                Ok(BranchSqlValue::Literal(
                    raw[1..raw.len() - 1].replace("\"\"", "\""),
                ))
            }
            T::Word => Ok(BranchSqlValue::Literal(token.text(self.input).to_owned())),
            _ => unreachable!("matched branch value"),
        }
    }
    fn bound(&self, node: NodeId) -> Result<SqlBound> {
        let token = self.tokens[self.tree.nodes[node].start];
        if let T::Parameter(position) = token.kind {
            return Ok(SqlBound::Parameter(position as usize));
        }
        let raw = token.text(self.input);
        raw.replace('_', "")
            .parse()
            .map(SqlBound::Literal)
            .map_err(|_| {
                HawDBError::Parse(format!(
                    "branch SQL bound {raw} is outside the supported range"
                ))
            })
    }
    fn selector(&self, node: NodeId) -> Result<BranchSqlSelector> {
        let child = self.first(node);
        match self.tree.nodes[child].name {
            Some("Selector") => self.selector(child),
            Some("Identifier") => self.value(child).map(BranchSqlSelector::Name),
            Some("Name") => self
                .value(self.child(child, "Value"))
                .map(BranchSqlSelector::Name),
            Some("Id") => self
                .value(self.child(child, "Value"))
                .map(BranchSqlSelector::Id),
            _ => unreachable!("matched branch selector"),
        }
    }
    fn statement(&self, node: NodeId) -> Result<BranchSqlStatement> {
        let node = self.first(node);
        Ok(match self.tree.nodes[node].name {
            Some("Use") => {
                BranchSqlStatement::UseBranch(self.selector(self.child(node, "Selection"))?)
            }
            Some("ShowCurrent") => BranchSqlStatement::ShowCurrentBranch,
            Some("ShowBranch") => BranchSqlStatement::ShowBranch(ShowBranchStatement {
                selector: self.selector(self.child(node, "Selector"))?,
            }),
            Some("ShowBranches") => BranchSqlStatement::ShowBranches(ShowBranchesStatement {
                limit: self.bound(self.child(self.child(node, "Limit"), "Bound"))?,
                offset: self
                    .tree
                    .child(node, "Offset")
                    .map(|id| self.bound(self.child(id, "Bound")))
                    .transpose()?,
            }),
            Some("Drop") => BranchSqlStatement::DropBranch(DropBranchStatement {
                id: self.value(self.child(node, "Value"))?,
                expected_metadata_revision: self.bound(self.child(node, "Bound"))?,
            }),
            Some("Create") => BranchSqlStatement::CreateBranch(CreateBranchStatement {
                name: self
                    .tree
                    .child(node, "NewName")
                    .map(|id| self.value(self.first(id)))
                    .transpose()?,
                source: self.selector(self.child(node, "Selection"))?,
                expected_source_revision: self.bound(self.child(node, "Bound"))?,
                request_key: self.value(self.child(node, "Value"))?,
                owner: self
                    .tree
                    .child(node, "Owner")
                    .map(|id| self.value(self.child(id, "Value")))
                    .transpose()?,
            }),
            _ => unreachable!("matched branch statement"),
        })
    }
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
    fn branch_page_bounds_are_explicit_unique_and_order_independent() {
        for input in [
            "SHOW BRANCHES LIMIT 10 OFFSET $1",
            "SHOW BRANCHES OFFSET $1 LIMIT 10",
        ] {
            assert_eq!(
                parse_postgres_sql(input).unwrap(),
                SqlStatement::Branch(BranchSqlStatement::ShowBranches(ShowBranchesStatement {
                    limit: SqlBound::Literal(10),
                    offset: Some(SqlBound::Parameter(1))
                }))
            );
        }
        for input in [
            "SHOW BRANCHES",
            "SHOW BRANCHES OFFSET 1",
            "SHOW BRANCHES LIMIT 1 LIMIT 2",
            "SHOW BRANCHES LIMIT 1 OFFSET 2 OFFSET 3",
            "SHOW BRANCHES LIMIT -1",
            "SHOW BRANCHES LIMIT 18446744073709551616",
            "SHOW BRANCHES LIMIT 1; SHOW CURRENT BRANCH",
        ] {
            assert!(parse_postgres_sql(input).is_err(), "{input}");
        }
        assert_eq!(
            parse_postgres_sql("USE BRANCH \"知识\"\"分支\"").unwrap(),
            SqlStatement::Branch(BranchSqlStatement::UseBranch(BranchSqlSelector::Name(
                BranchSqlValue::Literal("知识\"分支".into())
            )))
        );
        assert!(parse_postgres_sql("USE BRANCH NAME E'line\\n'").is_err());
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
