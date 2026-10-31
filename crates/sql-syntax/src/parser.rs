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
    GraphTable, Identifier, PgqStatement, PostgresSelectSyntax, PostgresStatementSyntax,
    SyntaxError, Token, TokenKind,
};

pub(crate) mod expression;
pub(crate) mod frontend;

pub fn parse_postgres_statement(input: &str) -> Result<PostgresStatementSyntax, SyntaxError> {
    match frontend::parse(input, "SqlInput")? {
        frontend::Output::Select(select) => Ok(PostgresStatementSyntax::Select(select)),
        frontend::Output::Create(create) => {
            Ok(PostgresStatementSyntax::CreatePropertyGraph(*create))
        }
        frontend::Output::Explain { .. } | frontend::Output::GraphTable(_) => {
            unreachable!("SQL entry excludes standalone GRAPH_TABLE")
        }
    }
}

/// Parses the executable relational SELECT and EXPLAIN SELECT families.
pub fn parse_relational_statement(
    input: &str,
) -> Result<crate::RelationalStatementSyntax, SyntaxError> {
    match frontend::parse(input, "RelationalStatementInput")? {
        frontend::Output::Select(select) => Ok(crate::RelationalStatementSyntax::Select(select)),
        frontend::Output::Explain { analyze, select } => {
            Ok(crate::RelationalStatementSyntax::Explain { analyze, select })
        }
        _ => unreachable!("relational entry returns SELECT or EXPLAIN SELECT"),
    }
}

pub fn parse_postgres_select(input: &str) -> Result<PostgresSelectSyntax, SyntaxError> {
    let frontend::Output::Select(select) = frontend::parse(input, "SelectInput")? else {
        unreachable!("SELECT entry produces a SELECT AST")
    };
    Ok(*select)
}

pub fn parse_pgq_statement(input: &str) -> Result<PgqStatement, SyntaxError> {
    let frontend::Output::Create(create) = frontend::parse(input, "PgqInput")? else {
        unreachable!("PGQ entry produces CREATE PROPERTY GRAPH")
    };
    Ok(PgqStatement::CreatePropertyGraph(*create))
}

pub fn parse_graph_table(input: &str) -> Result<GraphTable, SyntaxError> {
    let frontend::Output::GraphTable(table) = frontend::parse(input, "GraphTableInput")? else {
        unreachable!("GRAPH_TABLE entry produces a GRAPH_TABLE AST")
    };
    Ok(*table)
}

const MAX_GRAPH_PATTERN_NESTING: usize = 128;
const MAX_EXPRESSION_NESTING: usize = 128;

fn identifier_from_token(token: Token) -> Identifier {
    Identifier {
        span: token.span,
        quoted: token.kind == TokenKind::QuotedIdentifier,
    }
}
