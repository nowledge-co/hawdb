// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::{HawDBError, Result};
use hawdb_sql_syntax::peg::NativeMatch;
use hawdb_sql_syntax::{Span, SyntaxError, SyntaxErrorCode, Token};

use super::super::Parser;
use crate::{Clause, ClauseKind, PredicateExpression};

#[cfg(test)]
thread_local! {
    pub(super) static CLAUSE_POSITIONS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub(super) enum Value {
    Clause {
        clause: Box<Clause>,
        anonymous: usize,
        depth: usize,
    },
    Predicate {
        predicate: Box<PredicateExpression>,
        anonymous: usize,
        depth: usize,
    },
}

fn run(
    input: &str,
    tokens: &[Token],
    position: usize,
    parse: impl FnOnce(&mut Parser<'_>) -> Result<Value>,
) -> NativeMatch<Value> {
    let mut parser = Parser::new(input);
    parser.pos = tokens[position].span.start;
    #[cfg(test)]
    {
        parser.peg = true;
    }
    // Retain caller error class and complete contextual diagnostic across the
    // shared syntax engine. Semantic refusals are fatal, not choice failures.
    let value = parse(&mut parser).map_err(|error| {
        let (code, message) = match error {
            HawDBError::Parse(message) => (
                if message.contains("nesting exceeds limit") {
                    SyntaxErrorCode::ParserLimitExceeded
                } else {
                    SyntaxErrorCode::UnexpectedToken
                },
                message,
            ),
            HawDBError::Semantic(message) => (SyntaxErrorCode::NativeSemanticError, message),
            other => (SyntaxErrorCode::NativeSemanticError, other.to_string()),
        };
        SyntaxError::new(code, Span::new(parser.pos, parser.pos))
            .expected("Cypher native diagnostic")
            .found(message)
    });
    NativeMatch {
        end: tokens
            .partition_point(|token| token.span.start < parser.pos)
            .min(tokens.len() - 1),
        value,
    }
}

pub(super) fn clause(input: &str, tokens: &[Token], position: usize) -> NativeMatch<Value> {
    #[cfg(test)]
    CLAUSE_POSITIONS.with(|positions| positions.borrow_mut().push(tokens[position].span.start));
    run(input, tokens, position, |parser| {
        let start = parser.pos;
        parser.deferred_anonymous = true;
        let kind = if parser.consume_keyword("WITH") {
            ClauseKind::With(parser.parse_pipeline_projection(true)?)
        } else if parser.consume_keyword("RETURN") {
            ClauseKind::Return(parser.parse_pipeline_projection(false)?)
        } else {
            unreachable!("native projection prefix")
        };
        Ok(Value::Clause {
            clause: Box::new(parser.source_node(kind, start)),
            anonymous: parser.anonymous_variable_id,
            depth: parser.peak_depth,
        })
    })
}

pub(super) fn predicate(input: &str, tokens: &[Token], position: usize) -> NativeMatch<Value> {
    run(input, tokens, position, |parser| {
        let start = parser.pos;
        parser.deferred_anonymous = true;
        let kind = parser.parse_predicate(true)?;
        Ok(Value::Predicate {
            predicate: Box::new(parser.source_node(kind, start)),
            anonymous: parser.anonymous_variable_id,
            depth: parser.peak_depth,
        })
    })
}
