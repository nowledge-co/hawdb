// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher statement and clause PEG, sharing the SQL/PGQ packrat engine.
//! Recognition produces a concrete syntax tree. AST folding allocates names
//! only for the selected tree; ordered-choice probes have no mutable state.

use std::sync::OnceLock;

use hawdb_core::{HawDBError, Result};
use hawdb_sql_syntax::peg::{Grammar, Limits, NodeId, ParseTree};
use hawdb_sql_syntax::{SyntaxError, SyntaxErrorCode, Token};

use super::{MAX_CYPHER_INPUT_BYTES, MAX_CYPHER_PARSER_DEPTH};
use crate::*;

mod data;
mod expressions;
mod grammar;
mod lexer;
mod patterns;
mod predicates;
mod procedures;
mod projection;
mod schema;
#[cfg(test)]
mod tests;
mod transform;
mod values;

fn grammar(multi_stage: bool) -> &'static Grammar {
    static FULL: OnceLock<Grammar> = OnceLock::new();
    static SIMPLE: OnceLock<Grammar> = OnceLock::new();
    let grammar = if multi_stage { &FULL } else { &SIMPLE };
    grammar.get_or_init(|| grammar::build(multi_stage).expect("valid built-in Cypher PEG grammar"))
}

fn check_input(input: &str) -> Result<()> {
    if input.len() > MAX_CYPHER_INPUT_BYTES {
        return Err(HawDBError::Parse(format!(
            "Cypher input exceeds maximum length of {MAX_CYPHER_INPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(super) fn parse(input: &str) -> Result<Statement> {
    check_input(input)?;
    let tokens = lexer::tokenize(input)?;
    // Two WITH tokens are necessary for the legacy multi-stage admission.
    // Property/alias keywords can only cause a conservative extra probe; quoted
    // strings never contribute. This is syntax dispatch, not parser fallback.
    let multi_stage = tokens
        .iter()
        .filter(|token| token.is_keyword(input, "WITH"))
        .take(2)
        .count()
        == 2;
    let tree = grammar(multi_stage)
        .parse(
            input,
            &tokens,
            "root",
            Limits {
                depth: 8192,
                ..Limits::default()
            },
        )
        .map_err(|error| render(input, error))?;
    let root = tree.required(tree.root, "statement");
    Transformer {
        tree,
        input,
        tokens: &tokens,
        peak_depth: 0,
    }
    .statement(root, 1)
}

pub(super) fn parse_pipeline(input: &str) -> Result<QueryPipeline> {
    check_input(input)?;
    let tokens = lexer::tokenize(input)?;
    let tree = grammar(false)
        .parse(
            input,
            &tokens,
            "pipeline_root",
            Limits {
                depth: 8192,
                ..Limits::default()
            },
        )
        .map_err(|error| render(input, error))?;
    let root = tree.required(tree.root, "pipeline");
    Transformer {
        tree,
        input,
        tokens: &tokens,
        peak_depth: 0,
    }
    .pipeline(root, 0, &mut 0)
}

pub(super) fn parse_predicate(input: &str, columns: bool) -> Result<PropertyPredicate> {
    check_input(input)?;
    let tokens = lexer::tokenize(input)?;
    let tree = grammar(false)
        .parse(
            input,
            &tokens,
            "w_input",
            Limits {
                depth: 8192,
                ..Limits::default()
            },
        )
        .map_err(|error| render(input, error))?;
    let root = tree.required(tree.root, "w_or");
    Transformer {
        tree,
        input,
        tokens: &tokens,
        peak_depth: 0,
    }
    .where_or(root, 0, &mut 0, columns)
}

fn parse_error(input: &str, position: usize, message: &str) -> HawDBError {
    let mut line = 1;
    let mut column = 1;
    for ch in input[..position].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    let near: String = input[position..]
        .chars()
        .take(24)
        .flat_map(char::escape_default)
        .collect();
    HawDBError::Parse(format!(
        "{message} at byte {position} line {line} column {column} near `{near}`"
    ))
}

fn render(input: &str, error: SyntaxError) -> HawDBError {
    let mut position = error.span.start;
    if error.code == SyntaxErrorCode::ParserLimitExceeded {
        return parse_error(
            input,
            position,
            "Cypher parser nesting exceeds limit or PEG resource budget",
        );
    }
    let expected = error.expected.unwrap_or("a Cypher statement");
    // Legacy fixed-character diagnostics point after the unexpected character.
    if matches!(expected, "','" | "'.'")
        && let Some(ch) = input[position..].chars().next()
    {
        position += ch.len_utf8();
    }
    if expected == "unexpected trailing input" {
        parse_error(input, position, expected)
    } else if position == 0 {
        parse_error(input, position, "expected BEGIN, CREATE, ALTER, CYPHER, EXPLAIN, UNWIND, MERGE, MATCH, SET, CALL, CHECKPOINT, COMMIT, or ROLLBACK")
    } else {
        parse_error(input, position, &format!("expected {expected}"))
    }
}

struct Transformer<'a> {
    tree: ParseTree,
    input: &'a str,
    tokens: &'a [Token],
    peak_depth: usize,
}

impl Transformer<'_> {
    fn error_at(&self, position: usize, message: &str) -> HawDBError {
        parse_error(self.input, position, message)
    }

    fn error(&self, node: NodeId, message: &str) -> HawDBError {
        self.error_at(self.tree.span(node, self.tokens).start, message)
    }

    fn check_depth(&mut self, node: NodeId, active: usize, language: usize) -> Result<()> {
        self.peak_depth = self.peak_depth.max(language);
        if active + language > MAX_CYPHER_PARSER_DEPTH {
            return Err(self.error(node, "Cypher parser nesting exceeds limit of 32"));
        }
        Ok(())
    }
}
