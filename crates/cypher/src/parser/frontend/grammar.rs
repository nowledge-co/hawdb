// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_sql_syntax::peg::{Expression as E, Grammar};
use hawdb_sql_syntax::TokenKind;

fn rule(name: &'static str) -> E {
    E::Rule(name)
}
fn seq(items: Vec<E>) -> E {
    E::Sequence(items)
}
fn choice(names: &[&'static str]) -> E {
    E::Choice(names.iter().map(|name| rule(name)).collect())
}
fn repeat(expression: E) -> E {
    E::Repeat(Box::new(expression))
}
fn optional(expression: E) -> E {
    E::Optional(Box::new(expression))
}

// These envelope expressions do not produce semantic captures. Keeping them
// inline leaves room for new language rules within the original 256-rule cap.
fn non_with() -> E {
    seq(vec![
        E::Not(
            Box::new(E::Choice(vec![E::Keyword("WITH"), E::Keyword("RETURN")])),
            "a non-WITH clause",
        ),
        choice(&[
            "match_clause",
            "optional_clause",
            "unwind_clause",
            "load_clause",
            "call_clause",
            "create_clause",
            "merge_clause",
            "set_clause",
            "delete_clause",
            "detach_clause",
        ]),
    ])
}
fn two_with() -> E {
    seq(vec![
        repeat(non_with()),
        rule("with_clause"),
        repeat(non_with()),
        rule("with_clause"),
    ])
}
fn non_return() -> E {
    seq(vec![
        E::Not(Box::new(E::Keyword("RETURN")), "a non-RETURN clause"),
        E::Choice(vec![non_with(), rule("with_clause")]),
    ])
}

pub(super) fn build(multi_stage: bool) -> Result<Grammar, String> {
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
        // Failed probes and selected syntax subtrees are memoized.
        (
            "multi_stage",
            seq(vec![
                E::Not(Box::new(E::Keyword("MATCH")), "a non-MATCH pipeline"),
                E::Ahead(Box::new(two_with()), "a multi-stage pipeline"),
                rule("pipeline"),
            ]),
        ),
        (
            "pipeline",
            seq(vec![repeat(non_return()), optional(rule("return_clause"))]),
        ),
        (
            "match",
            seq(vec![
                E::Ahead(
                    Box::new(E::Choice(vec![E::Keyword("MATCH"), E::Keyword("LOAD")])),
                    "MATCH",
                ),
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
                optional(E::Keyword("ANALYZE")),
                rule("statement"),
            ]),
        ),
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
    rules.extend(super::predicates::rules());
    rules.extend(super::expressions::rules());
    rules.extend(super::temporal::rules());
    rules.extend(super::projection::rules());
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
        &[
            "v_temporal",
            "v_map",
            "v_list_contents",
            "v_call_contents",
            "v_conversion_contents",
            "x_conversion",
            "u_map_contents",
            "x_expression_list_contents",
            "x_comprehension_contents",
            "x_not_contents",
            "r_nested_sum",
            "r_unary_contents",
        ],
        32,
    )
}
