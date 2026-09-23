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

use super::ast::{CypherQuery, Explain, SetSystemVariable, Statement};
use hawdb_core::time::Instant;
use hawdb_core::{HawDBError, Result};

mod case;
mod cursor;
mod ddl;
mod mutation;
mod pattern;
mod pipeline;
pub use pipeline::parse_pipeline;
mod predicate;
mod procedure;
mod projection;
mod query;
mod scalar;

#[cfg(test)]
mod tests;

pub(crate) const MAX_CYPHER_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CYPHER_PARSER_DEPTH: usize = 32;

/// Parses one Cypher statement.
///
/// Input is limited to 16 MiB and recursive parser nesting is limited to 32
/// entries. Inputs exceeding either limit return a parse error.
pub fn parse(input: &str) -> Result<Statement> {
    parse_inner(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseMetrics {
    pub input_bytes: usize,
    pub elapsed_nanos: u64,
}

#[derive(Debug)]
pub struct ParseMeasurement {
    pub result: Result<Statement>,
    pub metrics: ParseMetrics,
}

pub fn parse_profiled(input: &str) -> ParseMeasurement {
    let started = Instant::now();
    let result = parse_inner(input);
    ParseMeasurement {
        result,
        metrics: ParseMetrics {
            input_bytes: input.len(),
            elapsed_nanos: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        },
    }
}

fn parse_inner(input: &str) -> Result<Statement> {
    if input.len() > MAX_CYPHER_INPUT_BYTES {
        return Err(HawDBError::Parse(format!(
            "Cypher input exceeds maximum length of {MAX_CYPHER_INPUT_BYTES} bytes"
        )));
    }
    let mut parser = Parser::new(input);
    let statement = parser.parse_statement()?;
    parser.consume_char(';');
    parser.expect_eof()?;
    Ok(statement)
}

fn keyword_matches(input: &str, keyword: &str) -> bool {
    let Some(prefix) = input.get(..keyword.len()) else {
        return false;
    };
    if !prefix.eq_ignore_ascii_case(keyword) {
        return false;
    }
    let next = input
        .get(keyword.len()..)
        .and_then(|input| input.chars().next());
    !matches!(next, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementDispatch {
    Begin,
    Create,
    Alter,
    Cypher,
    Explain,
    Unwind,
    Merge,
    Match,
    Set,
    Call,
    Checkpoint,
    Commit,
    Rollback,
}

const TOP_LEVEL_STATEMENTS: &[(&str, StatementDispatch)] = &[
    ("BEGIN", StatementDispatch::Begin),
    ("CREATE", StatementDispatch::Create),
    ("ALTER", StatementDispatch::Alter),
    ("CYPHER", StatementDispatch::Cypher),
    ("EXPLAIN", StatementDispatch::Explain),
    ("UNWIND", StatementDispatch::Unwind),
    ("MERGE", StatementDispatch::Merge),
    ("MATCH", StatementDispatch::Match),
    ("SET", StatementDispatch::Set),
    ("CALL", StatementDispatch::Call),
    ("CHECKPOINT", StatementDispatch::Checkpoint),
    ("COMMIT", StatementDispatch::Commit),
    ("ROLLBACK", StatementDispatch::Rollback),
];

pub(super) struct Parser<'a> {
    input: &'a str,
    pos: usize,
    anonymous_variable_id: usize,
    recursion_depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParserCheckpoint {
    pos: usize,
    anonymous_variable_id: usize,
}

impl<'a> Parser<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            anonymous_variable_id: 0,
            recursion_depth: 0,
        }
    }

    pub(super) fn parse_statement(&mut self) -> Result<Statement> {
        self.with_recursion(|parser| parser.parse_statement_inner())
    }

    fn checkpoint(&self) -> ParserCheckpoint {
        ParserCheckpoint {
            pos: self.pos,
            anonymous_variable_id: self.anonymous_variable_id,
        }
    }

    fn restore(&mut self, checkpoint: ParserCheckpoint) {
        // Speculative branches must not consume names used by the accepted AST.
        // Recursion depth is scoped by with_recursion, not parser backtracking.
        self.pos = checkpoint.pos;
        self.anonymous_variable_id = checkpoint.anonymous_variable_id;
    }

    fn parse_statement_inner(&mut self) -> Result<Statement> {
        let statement_start = self.checkpoint();
        if let Some(statement) = self.parse_multi_stage_pipeline_statement() {
            return Ok(statement);
        }
        match self.parse_statement_dispatch()? {
            StatementDispatch::Begin => {
                self.expect_keyword("TRANSACTION")?;
                Ok(Statement::BeginTransaction)
            }
            StatementDispatch::Create => self.parse_create_statement(),
            StatementDispatch::Alter => self.parse_alter_statement(),
            StatementDispatch::Cypher => self.parse_cypher_query_statement(),
            StatementDispatch::Explain => self.parse_explain_statement(),
            StatementDispatch::Unwind => {
                self.restore(statement_start);
                Ok(Statement::UnwindMutation(Box::new(
                    self.parse_query_pipeline()?,
                )))
            }
            StatementDispatch::Merge => self.parse_merge_statement(),
            StatementDispatch::Match => self.parse_match_statement(),
            StatementDispatch::Set => self.parse_set_system_variable_statement(),
            StatementDispatch::Call => self.parse_call_statement(),
            StatementDispatch::Checkpoint => Ok(Statement::Checkpoint),
            StatementDispatch::Commit => Ok(Statement::Commit),
            StatementDispatch::Rollback => Ok(Statement::Rollback),
        }
    }

    fn parse_multi_stage_pipeline_statement(&mut self) -> Option<Statement> {
        let checkpoint = self.checkpoint();
        let pipeline = self.parse_query_pipeline();
        let is_multi_stage = pipeline.as_ref().is_ok_and(|pipeline| {
            pipeline
                .clauses
                .iter()
                .filter(|clause| matches!(clause.kind, crate::ClauseKind::With(_)))
                .count()
                >= 2
        });
        if is_multi_stage {
            return pipeline
                .ok()
                .map(|pipeline| Statement::Pipeline(Box::new(pipeline)));
        }
        self.restore(checkpoint);
        None
    }

    pub(super) fn with_recursion<T>(
        &mut self,
        parse: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        if self.recursion_depth >= MAX_CYPHER_PARSER_DEPTH {
            return Err(self.error(&format!(
                "Cypher parser nesting exceeds limit of {MAX_CYPHER_PARSER_DEPTH}"
            )));
        }
        self.recursion_depth += 1;
        let result = parse(self);
        self.recursion_depth -= 1;
        result
    }

    fn parse_statement_dispatch(&mut self) -> Result<StatementDispatch> {
        self.parse_keyword_choice(
            TOP_LEVEL_STATEMENTS,
            "expected BEGIN, CREATE, ALTER, CYPHER, EXPLAIN, UNWIND, MERGE, MATCH, SET, CALL, CHECKPOINT, COMMIT, or ROLLBACK",
        )
    }

    pub(super) fn next_anonymous_variable(&mut self) -> String {
        let variable = format!("__anon{}", self.anonymous_variable_id);
        self.anonymous_variable_id += 1;
        variable
    }

    fn parse_set_system_variable_statement(&mut self) -> Result<Statement> {
        self.expect_keyword("SYSTEM")?;
        Ok(Statement::SetSystemVariable(
            self.parse_system_variable_assignment(true)?,
        ))
    }

    fn parse_cypher_query_statement(&mut self) -> Result<Statement> {
        let mut system_variables = Vec::new();
        while self.consume_keyword("SYSTEM") {
            system_variables.push(self.parse_system_variable_assignment(false)?);
        }
        if system_variables.is_empty() {
            return Err(self.error("expected at least one CYPHER system hint"));
        }
        let statement = self.parse_statement()?;
        match statement {
            Statement::BeginTransaction
            | Statement::Checkpoint
            | Statement::Commit
            | Statement::CypherQuery(_)
            | Statement::Explain(_)
            | Statement::Rollback
            | Statement::SetSystemVariable(_) => {
                return Err(self.error("CYPHER system hints require a query or mutation statement"));
            }
            _ => {}
        }
        Ok(Statement::CypherQuery(Box::new(CypherQuery {
            system_variables,
            statement,
        })))
    }

    fn parse_explain_statement(&mut self) -> Result<Statement> {
        let analyze = self.consume_keyword("ANALYZE");
        let statement = self.parse_statement()?;
        let body = explain_statement_body(&statement);
        match body {
            Statement::BeginTransaction
            | Statement::Checkpoint
            | Statement::Commit
            | Statement::CypherQuery(_)
            | Statement::Explain(_)
            | Statement::Rollback
            | Statement::SetSystemVariable(_) => {
                return Err(self.error("EXPLAIN requires a query or mutation statement"));
            }
            _ => {}
        }
        Ok(Statement::Explain(Box::new(Explain { analyze, statement })))
    }

    fn parse_system_variable_assignment(
        &mut self,
        allow_variable_keyword: bool,
    ) -> Result<SetSystemVariable> {
        let name = if allow_variable_keyword && self.consume_keyword("VARIABLE") {
            self.parse_system_variable_name()?
        } else {
            self.expect_char('.')?;
            self.parse_ident()?
        };
        self.expect_char('=')?;
        let value = self.parse_value()?;
        Ok(SetSystemVariable {
            name: name.to_ascii_lowercase(),
            value,
        })
    }

    fn parse_system_variable_name(&mut self) -> Result<String> {
        if self.consume_keyword("SYSTEM") {
            self.expect_char('.')?;
        }
        self.parse_ident()
    }
}

fn explain_statement_body(statement: &Statement) -> &Statement {
    match statement {
        Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}
