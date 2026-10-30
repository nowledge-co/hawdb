// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher statement and clause PEG, sharing the SQL/PGQ packrat engine.
//! Native terminals construct ASTs once; ordered-choice probes have no mutable
//! name-allocation state. Only captures in the selected tree are materialized.

use std::sync::OnceLock;

use hawdb_core::{HawDBError, Result};
use hawdb_sql_syntax::peg::{Grammar, Limits, NodeId, ParseTree};
use hawdb_sql_syntax::{SyntaxError, SyntaxErrorCode, Token};

use super::{Parser, MAX_CYPHER_INPUT_BYTES, MAX_CYPHER_PARSER_DEPTH};
use crate::*;

mod grammar;
mod lexer;
mod native;
#[cfg(test)]
mod tests;
mod transform;

use native::Value;

fn grammar(multi_stage: bool) -> &'static Grammar<Value> {
    static FULL: OnceLock<Grammar<Value>> = OnceLock::new();
    static SIMPLE: OnceLock<Grammar<Value>> = OnceLock::new();
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
        .parse(input, &tokens, "root", Limits::default())
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
    let mut parser = Parser::new(input);
    pipeline_at(&mut parser)
}

pub(super) fn pipeline_at(parser: &mut Parser<'_>) -> Result<QueryPipeline> {
    check_input(parser.input)?;
    let tokens = lexer::tokenize(parser.input)?;
    let start = tokens.partition_point(|token| token.span.start < parser.pos);
    let tokens = &tokens[start..];
    let tree = grammar(false)
        .parse(parser.input, tokens, "pipeline_root", Limits::default())
        .map_err(|error| render(parser.input, error))?;
    let root = tree.required(tree.root, "pipeline");
    let mut transformer = Transformer {
        tree,
        input: parser.input,
        tokens,
        peak_depth: 0,
    };
    let pipeline = transformer.pipeline(
        root,
        parser.recursion_depth,
        &mut parser.anonymous_variable_id,
    )?;
    parser.pos = pipeline.span.expect("source pipeline").end;
    parser.peak_depth = parser
        .peak_depth
        .max(parser.recursion_depth + transformer.peak_depth);
    Ok(pipeline)
}

fn render(input: &str, error: SyntaxError) -> HawDBError {
    if error.expected == Some("Cypher native diagnostic") {
        let message = error.found.expect("native diagnostic text");
        return if error.code == SyntaxErrorCode::NativeSemanticError {
            HawDBError::Semantic(message)
        } else {
            HawDBError::Parse(message)
        };
    }
    let mut parser = Parser::new(input);
    parser.pos = error.span.start;
    if error.code == SyntaxErrorCode::ParserLimitExceeded {
        return parser.error("Cypher parser nesting exceeds limit or PEG resource budget");
    }
    let expected = error.expected.unwrap_or("a Cypher statement");
    if expected == "unexpected trailing input" {
        parser.error(expected)
    } else if parser.pos == 0 {
        parser.error("expected BEGIN, CREATE, ALTER, CYPHER, EXPLAIN, UNWIND, MERGE, MATCH, SET, CALL, CHECKPOINT, COMMIT, or ROLLBACK")
    } else {
        parser.error(&format!("expected {expected}"))
    }
}

struct Transformer<'a> {
    tree: ParseTree<Value>,
    input: &'a str,
    tokens: &'a [Token],
    peak_depth: usize,
}

impl Transformer<'_> {
    fn error(&self, node: NodeId, message: &str) -> HawDBError {
        let mut parser = Parser::new(self.input);
        parser.pos = self.tree.span(node, self.tokens).start;
        parser.error(message)
    }

    fn check_depth(&mut self, node: NodeId, active: usize, native: usize) -> Result<()> {
        self.peak_depth = self.peak_depth.max(native);
        if active + native > MAX_CYPHER_PARSER_DEPTH {
            return Err(self.error(node, "Cypher parser nesting exceeds limit of 32"));
        }
        Ok(())
    }
}
