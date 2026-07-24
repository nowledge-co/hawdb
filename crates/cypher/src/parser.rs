use super::ast::{CypherQuery, Explain, SetSystemVariable, Statement};
use skein_core::Result;

mod cursor;
mod ddl;
mod mutation;
mod pattern;
mod predicate;
mod procedure;
mod projection;
mod query;
mod scalar;

pub fn parse(input: &str) -> Result<Statement> {
    let mut parser = Parser::new(input);
    let statement = parser.parse_statement()?;
    parser.consume_char(';');
    parser.expect_eof()?;
    Ok(statement)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementDispatch {
    Begin,
    Create,
    Alter,
    Cypher,
    Explain,
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
}

impl<'a> Parser<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        Self {
            input,
            pos: 0,
            anonymous_variable_id: 0,
        }
    }

    pub(super) fn parse_statement(&mut self) -> Result<Statement> {
        match self.parse_statement_dispatch()? {
            StatementDispatch::Begin => {
                self.expect_keyword("TRANSACTION")?;
                Ok(Statement::BeginTransaction)
            }
            StatementDispatch::Create => self.parse_create_statement(),
            StatementDispatch::Alter => self.parse_alter_statement(),
            StatementDispatch::Cypher => self.parse_cypher_query_statement(),
            StatementDispatch::Explain => self.parse_explain_statement(),
            StatementDispatch::Merge => self.parse_merge_statement(),
            StatementDispatch::Match => self.parse_match_statement(),
            StatementDispatch::Set => self.parse_set_system_variable_statement(),
            StatementDispatch::Call => self.parse_call_statement(),
            StatementDispatch::Checkpoint => Ok(Statement::Checkpoint),
            StatementDispatch::Commit => Ok(Statement::Commit),
            StatementDispatch::Rollback => Ok(Statement::Rollback),
        }
    }

    fn parse_statement_dispatch(&mut self) -> Result<StatementDispatch> {
        self.parse_keyword_choice(
            TOP_LEVEL_STATEMENTS,
            "expected BEGIN, CREATE, ALTER, CYPHER, EXPLAIN, MERGE, MATCH, SET, CALL, CHECKPOINT, COMMIT, or ROLLBACK",
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
