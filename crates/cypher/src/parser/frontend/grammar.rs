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
            "begin",
            seq(vec![E::Keyword("BEGIN"), E::Keyword("TRANSACTION")]),
        ),
        ("checkpoint", E::Keyword("CHECKPOINT")),
        ("commit", E::Keyword("COMMIT")),
        ("rollback", E::Keyword("ROLLBACK")),
    ];
    for (name, keyword) in [("with_clause", "WITH"), ("return_clause", "RETURN")] {
        rules.push((
            name,
            seq(vec![
                E::Ahead(Box::new(E::Keyword(keyword)), keyword),
                native(native::clause),
            ]),
        ));
    }
    rules.push((
        "call",
        E::Choice(vec![
            rule("u_legacy_call"),
            seq(vec![
                E::Ahead(Box::new(E::Keyword("CALL")), "CALL"),
                rule("pipeline"),
            ]),
        ]),
    ));
    rules.extend(super::procedures::rules());
    rules.push((
        "merge",
        seq(vec![E::Keyword("MERGE"), rule("d_merge_data")]),
    ));
    rules.extend(super::patterns::rules());
    rules.extend(super::values::rules());
    rules.extend(super::data::rules());
    rules.extend(super::schema::rules());
    rules.push((
        "create",
        seq(vec![
            E::Keyword("CREATE"),
            E::Choice(vec![rule("schema_create"), rule("d_create_data")]),
        ]),
    ));
    rules.push((
        "alter",
        seq(vec![E::Keyword("ALTER"), rule("schema_alter")]),
    ));
    Grammar::compile_bounded(
        rules,
        &["v_list_contents", "v_call_contents", "u_map_contents"],
        32,
    )
}
