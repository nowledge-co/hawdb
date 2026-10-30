// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_sql_syntax::peg::{Expression as E, Grammar, NativeMatcher};
use hawdb_sql_syntax::TokenKind;

use super::native::{self, Value};

fn rule(name: &'static str) -> E<Value> {
    E::Rule(name)
}
fn seq(items: Vec<E<Value>>) -> E<Value> {
    E::Sequence(items)
}
fn choice(names: &[&'static str]) -> E<Value> {
    E::Choice(names.iter().map(|name| rule(name)).collect())
}
fn repeat(expression: E<Value>) -> E<Value> {
    E::Repeat(Box::new(expression))
}
fn optional(expression: E<Value>) -> E<Value> {
    E::Optional(Box::new(expression))
}
fn native(matcher: NativeMatcher<Value>) -> E<Value> {
    E::Native {
        matcher,
        nullable: false,
    }
}

pub(super) fn build(multi_stage: bool) -> Result<Grammar<Value>, String> {
    let mut rules = vec![
        ("root", seq(vec![rule("statement"), rule("end")])),
        ("pipeline_root", seq(vec![rule("pipeline"), rule("end")])),
        (
            "end",
            seq(vec![
                optional(E::Token(TokenKind::Semicolon, ";")),
                E::Token(TokenKind::End, "unexpected trailing input"),
            ]),
        ),
        (
            "statement",
            choice(if multi_stage {
                &[
                    "multi_stage",
                    "match",
                    "unwind",
                    "explain",
                    "cypher",
                    "create",
                    "alter",
                    "merge",
                    "set",
                    "call",
                    "begin",
                    "checkpoint",
                    "commit",
                    "rollback",
                ]
            } else {
                &[
                    "match",
                    "unwind",
                    "explain",
                    "cypher",
                    "create",
                    "alter",
                    "merge",
                    "set",
                    "call",
                    "begin",
                    "checkpoint",
                    "commit",
                    "rollback",
                ]
            }),
        ),
        // A PEG lookahead establishes the historical two-WITH admission before
        // selecting the pipeline over CREATE/MERGE/CALL statement templates.
        // Failed probes are memoized, including native AST captures.
        (
            "multi_stage",
            seq(vec![
                E::Not(Box::new(E::Keyword("MATCH")), "a non-MATCH pipeline"),
                E::Ahead(Box::new(rule("two_with")), "a multi-stage pipeline"),
                rule("pipeline"),
            ]),
        ),
        (
            "two_with",
            seq(vec![
                repeat(rule("non_with")),
                rule("with_clause"),
                repeat(rule("non_with")),
                rule("with_clause"),
            ]),
        ),
        (
            "non_with",
            seq(vec![
                E::Not(
                    Box::new(E::Choice(vec![E::Keyword("WITH"), E::Keyword("RETURN")])),
                    "a non-WITH clause",
                ),
                choice(&[
                    "match_clause",
                    "optional_clause",
                    "unwind_clause",
                    "call_clause",
                    "create_clause",
                    "merge_clause",
                    "set_clause",
                    "delete_clause",
                    "detach_clause",
                ]),
            ]),
        ),
        (
            "non_return",
            seq(vec![
                E::Not(Box::new(E::Keyword("RETURN")), "a non-RETURN clause"),
                choice(&["non_with", "with_clause"]),
            ]),
        ),
        (
            "pipeline",
            seq(vec![
                repeat(rule("non_return")),
                optional(rule("return_clause")),
            ]),
        ),
        (
            "match",
            seq(vec![
                E::Ahead(Box::new(E::Keyword("MATCH")), "MATCH"),
                rule("pipeline"),
            ]),
        ),
        (
            "unwind",
            seq(vec![
                E::Ahead(Box::new(E::Keyword("UNWIND")), "UNWIND"),
                rule("pipeline"),
            ]),
        ),
        (
            "explain",
            seq(vec![
                E::Keyword("EXPLAIN"),
                optional(rule("analyze")),
                rule("statement"),
            ]),
        ),
        ("analyze", E::Keyword("ANALYZE")),
        (
            "cypher",
            seq(vec![
                E::Keyword("CYPHER"),
                rule("hint"),
                repeat(rule("hint")),
                rule("statement"),
            ]),
        ),
        (
            "hint",
            seq(vec![E::Keyword("SYSTEM"), native(native::hint)]),
        ),
        (
            "begin",
            seq(vec![E::Keyword("BEGIN"), E::Keyword("TRANSACTION")]),
        ),
        ("checkpoint", E::Keyword("CHECKPOINT")),
        ("commit", E::Keyword("COMMIT")),
        ("rollback", E::Keyword("ROLLBACK")),
    ];
    for (name, keyword) in [
        ("match_clause", "MATCH"),
        ("optional_clause", "OPTIONAL"),
        ("unwind_clause", "UNWIND"),
        ("call_clause", "CALL"),
        ("create_clause", "CREATE"),
        ("merge_clause", "MERGE"),
        ("set_clause", "SET"),
        ("delete_clause", "DELETE"),
        ("detach_clause", "DETACH"),
        ("with_clause", "WITH"),
        ("return_clause", "RETURN"),
    ] {
        rules.push((
            name,
            seq(vec![
                E::Ahead(Box::new(E::Keyword(keyword)), keyword),
                native(native::clause),
            ]),
        ));
    }
    for (name, keyword, matcher) in [
        ("create", "CREATE", native::create as NativeMatcher<Value>),
        ("alter", "ALTER", native::alter),
        ("merge", "MERGE", native::merge),
        ("set", "SET", native::set),
        ("call", "CALL", native::call),
    ] {
        rules.push((name, seq(vec![E::Keyword(keyword), native(matcher)])));
    }
    Grammar::compile(rules)
}
