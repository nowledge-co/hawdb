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

use super::*;
use crate::tokenize;
use Expression::{
    Choice, Keyword as K, Not, Optional, Repeat, Rule as R, Sequence as S, Token as Tok,
};

fn parse(grammar: &Grammar, sql: &str, limits: Limits) -> Result<ParseTree, SyntaxError> {
    grammar.parse(sql, &tokenize(sql).unwrap(), "Root", limits)
}

#[test]
fn choice_is_ordered_and_does_not_retry_after_a_successful_prefix() {
    let grammar = Grammar::compile(vec![
        ("Root", S(vec![R("Choice"), Tok(TokenKind::End, "end")])),
        (
            "Choice",
            Choice(vec![K("SELECT"), S(vec![K("SELECT"), K("ALL")])]),
        ),
    ])
    .unwrap();
    let error = parse(&grammar, "SELECT ALL", Limits::default())
        .err()
        .unwrap();
    assert_eq!(error.span, Span::new(7, 10));
}

#[test]
fn caches_success_and_failure_at_the_same_token_position() {
    let grammar = Grammar::compile(vec![
        (
            "Root",
            Choice(vec![
                S(vec![R("Prefix"), R("Missing")]),
                S(vec![R("Prefix"), R("Missing")]),
                S(vec![
                    R("Prefix"),
                    Tok(TokenKind::Number, "number"),
                    Tok(TokenKind::End, "end"),
                ]),
            ]),
        ),
        ("Prefix", K("SELECT")),
        ("Missing", K("FROM")),
    ])
    .unwrap();
    let tree = parse(&grammar, "SELECT 1", Limits::default()).unwrap();
    assert_eq!(tree.memo_entries, 3);
    assert_eq!(tree.memo_hits, 3);
    assert_eq!(
        tree.nodes
            .iter()
            .filter(|node| node.name == Some("Prefix"))
            .count(),
        1
    );
}

#[test]
fn invalid_recursive_and_nullable_grammars_fail_compilation() {
    for rules in [
        vec![("Root", R("Missing"))],
        vec![("Root", K("SELECT")), ("Root", K("FROM"))],
        vec![("Root", R("Root"))],
        vec![
            ("Root", R("Other")),
            ("Other", S(vec![Optional(Box::new(K("SELECT"))), R("Root")])),
        ],
        vec![("Root", Repeat(Box::new(Optional(Box::new(K("SELECT"))))))],
        vec![("Root", Repeat(Box::new(Tok(TokenKind::End, "end"))))],
        vec![("Root", Not(Box::new(R("Root")), "not recursive"))],
    ] {
        assert!(Grammar::compile(rules).is_err());
    }
}

#[test]
fn reports_farthest_unicode_token_and_discards_lookahead_errors() {
    let grammar = Grammar::compile(vec![(
        "Root",
        S(vec![
            Not(
                Box::new(S(vec![K("SELECT"), K("FROM")])),
                "a nonempty SELECT",
            ),
            K("SELECT"),
            K("ALL"),
        ]),
    )])
    .unwrap();
    let error = parse(&grammar, "SELECT \u{540d}\u{79f0}", Limits::default())
        .err()
        .unwrap();
    assert_eq!(error.span, Span::new(7, 13));
    assert_eq!(error.expected, Some("ALL"));
}

#[test]
fn parse_results_outlive_the_grammar_input_and_tokens() {
    let (tree, error) = {
        let grammar = Grammar::compile(vec![("Root", K("SELECT"))]).unwrap();
        let valid = String::from("SELECT");
        let invalid = String::from("FROM");
        let tree = parse(&grammar, &valid, Limits::default()).unwrap();
        let error = parse(&grammar, &invalid, Limits::default()).err().unwrap();
        (tree, error)
    };
    assert_eq!(tree.nodes[tree.root].name, Some("Root"));
    assert_eq!(
        (tree.nodes[tree.root].start, tree.nodes[tree.root].end),
        (0, 1)
    );
    assert_eq!(error.expected, Some("SELECT"));
    assert_eq!(error.span, Span::new(0, 4));
}

#[test]
fn work_limit_boundary_and_all_resource_failures_are_fatal() {
    let grammar = Grammar::compile(vec![("Root", K("SELECT"))]).unwrap();
    let tree = parse(&grammar, "SELECT", Limits::default()).unwrap();
    assert!(parse(
        &grammar,
        "SELECT",
        Limits {
            steps: tree.steps,
            ..Limits::default()
        }
    )
    .is_ok());
    let error = parse(
        &grammar,
        "SELECT",
        Limits {
            steps: tree.steps - 1,
            ..Limits::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.code, SyntaxErrorCode::ParserLimitExceeded);

    let grammar = Grammar::compile(vec![
        ("Root", Choice(vec![R("Nested"), K("SELECT")])),
        ("Nested", R("Leaf")),
        ("Leaf", K("SELECT")),
    ])
    .unwrap();
    for limits in [
        Limits {
            nodes: 0,
            ..Limits::default()
        },
        Limits {
            memo_entries: 1,
            ..Limits::default()
        },
        Limits {
            depth: 2,
            ..Limits::default()
        },
        Limits {
            steps: 3,
            ..Limits::default()
        },
    ] {
        let error = parse(&grammar, "SELECT", limits).err().unwrap();
        assert_eq!(error.code, SyntaxErrorCode::ParserLimitExceeded);
    }
}

#[test]
fn nesting_resource_errors_are_not_alternative_mismatches() {
    let grammar = Grammar::compile_with_nesting(
        vec![
            ("Root", Choice(vec![R("Bounded"), K("SELECT")])),
            ("Bounded", K("SELECT")),
        ],
        &[(
            &["Bounded"],
            0,
            SyntaxErrorCode::ExpressionNestingLimitExceeded,
        )],
    )
    .unwrap();
    let error = parse(&grammar, "SELECT", Limits::default()).err().unwrap();
    assert_eq!(error.code, SyntaxErrorCode::ExpressionNestingLimitExceeded);
}

#[test]
fn memoized_recursive_failure_has_linear_rule_evaluations() {
    let grammar = Grammar::compile(vec![
        ("Root", R("Nested")),
        (
            "Nested",
            Choice(vec![
                S(vec![
                    Tok(TokenKind::LeftParen, "("),
                    R("Nested"),
                    Tok(TokenKind::RightParen, ")"),
                ]),
                S(vec![Tok(TokenKind::LeftParen, "("), R("Nested"), K("END")]),
                K("SELECT"),
            ]),
        ),
    ])
    .unwrap();
    // Without failure memoization, each extra unmatched '(' doubles the work.
    let sql = "(".repeat(24);
    let error = parse(
        &grammar,
        &sql,
        Limits {
            steps: 500,
            depth: 128,
            ..Limits::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.code, SyntaxErrorCode::UnexpectedEnd);
    assert_eq!(error.span, Span::new(24, 24));
}

#[test]
fn optional_and_lookahead_preserve_fatal_nesting_errors() {
    for expression in [
        Optional(Box::new(R("Bounded"))),
        Repeat(Box::new(R("Bounded"))),
        Not(Box::new(R("Bounded")), "not bounded"),
        Expression::Ahead(Box::new(R("Bounded")), "bounded"),
    ] {
        let grammar = Grammar::compile_with_nesting(
            vec![("Root", expression), ("Bounded", K("SELECT"))],
            &[(
                &["Bounded"],
                0,
                SyntaxErrorCode::GraphPatternNestingLimitExceeded,
            )],
        )
        .unwrap();
        let error = parse(&grammar, "SELECT", Limits::default()).err().unwrap();
        assert_eq!(
            error.code,
            SyntaxErrorCode::GraphPatternNestingLimitExceeded
        );
    }
}

#[test]
fn cached_success_preserves_diagnostics_after_lookahead() {
    let grammar = Grammar::compile(vec![
        (
            "Root",
            S(vec![
                Not(Box::new(S(vec![R("Prefix"), K("STOP")])), "no STOP"),
                R("Prefix"),
                K("WHERE"),
            ]),
        ),
        (
            "Prefix",
            S(vec![
                K("SELECT"),
                Optional(Box::new(S(vec![K("FROM"), K("TABLE")]))),
            ]),
        ),
    ])
    .unwrap();
    let error = parse(&grammar, "SELECT FROM name", Limits::default())
        .err()
        .unwrap();
    assert_eq!(error.span, Span::new(12, 16));
    assert_eq!(error.expected, Some("TABLE"));
}

#[test]
fn exact_capture_and_memo_budgets_admit_only_complete_matches() {
    let grammar = Grammar::compile(vec![("Root", R("Leaf")), ("Leaf", K("SELECT"))]).unwrap();
    for limits in [
        Limits {
            nodes: 2,
            memo_entries: 2,
            ..Limits::default()
        },
        Limits {
            nodes: 2,
            memo_entries: 3,
            ..Limits::default()
        },
    ] {
        assert!(parse(&grammar, "SELECT", limits).is_ok());
    }
    for limits in [
        Limits {
            nodes: 1,
            ..Limits::default()
        },
        Limits {
            memo_entries: 1,
            ..Limits::default()
        },
    ] {
        let error = parse(&grammar, "SELECT", limits).err().unwrap();
        assert_eq!(error.code, SyntaxErrorCode::ParserLimitExceeded);
    }
}

#[test]
fn nullable_rules_preserve_empty_source_spans() {
    let grammar = Grammar::compile(vec![
        ("Root", R("Empty")),
        ("Empty", Optional(Box::new(K("FROM")))),
    ])
    .unwrap();
    let tokens = tokenize("SELECT").unwrap();
    let tree = grammar
        .parse("SELECT", &tokens, "Root", Limits::default())
        .unwrap();
    let empty = tree.required(tree.root, "Empty");
    assert_eq!(tree.span(empty, &tokens), Span::new(0, 0));
}

#[test]
fn long_invalid_tokens_keep_exact_final_error_text_and_span() {
    let grammar = Grammar::compile(vec![
        ("Root", Choice(vec![R("First"), R("Second"), R("Third")])),
        ("First", K("SELECT")),
        ("Second", K("CREATE")),
        ("Third", K("FROM")),
    ])
    .unwrap();
    let sql = "x".repeat(256 * 1024);
    let error = parse(&grammar, &sql, Limits::default()).err().unwrap();
    assert_eq!(error.code, SyntaxErrorCode::UnexpectedToken);
    assert_eq!(error.span, Span::new(0, sql.len()));
    assert_eq!(error.found.as_deref(), Some(sql.as_str()));
    assert_eq!(error.expected, Some("SELECT"));
}

#[test]
fn backtracking_and_lookahead_discard_captures_but_keep_memoized_nodes() {
    let attempt = || S(vec![R("Prefix"), K("MISSING")]);
    let grammar = Grammar::compile(vec![
        (
            "Root",
            S(vec![
                Expression::Ahead(Box::new(R("Prefix")), "prefix"),
                Not(Box::new(attempt()), "no MISSING"),
                Optional(Box::new(attempt())),
                Repeat(Box::new(attempt())),
                Choice(vec![attempt(), R("Prefix")]),
                R("Tail"),
                Tok(TokenKind::End, "end"),
            ]),
        ),
        ("Prefix", K("SELECT")),
        ("Tail", K("FROM")),
    ])
    .unwrap();
    let tree = parse(&grammar, "SELECT FROM", Limits::default()).unwrap();
    let children = tree.children(tree.root);
    assert_eq!(children.len(), 2);
    assert_eq!(tree.nodes[children[0]].name, Some("Prefix"));
    assert_eq!(tree.nodes[children[1]].name, Some("Tail"));
    assert_eq!(tree.nodes.len(), 3);
    assert_eq!(tree.memo_entries, 3);
    assert_eq!(tree.memo_hits, 5);
}

#[test]
fn repeated_memo_references_each_count_toward_the_edge_budget() {
    let grammar = Grammar::compile(vec![
        (
            "Root",
            S(vec![
                R("Empty"),
                R("Empty"),
                R("Empty"),
                R("Empty"),
                R("Empty"),
                K("SELECT"),
            ]),
        ),
        ("Empty", S(vec![])),
    ])
    .unwrap();
    let tree = parse(
        &grammar,
        "SELECT",
        Limits {
            nodes: 5,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(tree.nodes.len(), 2);
    assert_eq!(tree.children(tree.root).len(), 5);
    assert!(tree.children(tree.root).iter().all(|child| *child == 0));
    assert_eq!(
        parse(
            &grammar,
            "SELECT",
            Limits {
                nodes: 4,
                ..Limits::default()
            }
        )
        .err()
        .unwrap()
        .code,
        SyntaxErrorCode::ParserLimitExceeded,
    );
}

#[test]
fn memoized_subtree_survives_capture_rollback_without_cloning() {
    let grammar = Grammar::compile(vec![
        (
            "Root",
            Choice(vec![S(vec![R("Prefix"), K("MISSING")]), R("Prefix")]),
        ),
        ("Prefix", R("Leaf")),
        ("Leaf", K("SELECT")),
    ])
    .unwrap();
    let tokens = tokenize("SELECT").unwrap();
    let tree = grammar
        .parse("SELECT", &tokens, "Root", Limits::default())
        .unwrap();
    assert_eq!(tree.memo_hits, 1);
    assert_eq!(tree.nodes.len(), 3);
    let prefix = tree.required(tree.root, "Prefix");
    let leaf = tree.required(prefix, "Leaf");
    assert_eq!(tree.span(leaf, &tokens), Span::new(0, 6));
    assert_eq!(tree.children(prefix), &[leaf]);
}

#[test]
fn memo_keeps_successes_and_failures_at_multiple_positions_with_exact_accounting() {
    let prefix = || {
        vec![
            R("Word"),
            Optional(Box::new(R("Missing"))),
            R("Word"),
            Optional(Box::new(R("Missing"))),
        ]
    };
    let mut rejected = prefix();
    rejected.push(K("STOP"));
    let mut accepted = prefix();
    accepted.push(Tok(TokenKind::End, "end"));
    let grammar = Grammar::compile(vec![
        ("Root", Choice(vec![S(rejected), S(accepted)])),
        ("Word", Tok(TokenKind::Word, "word")),
        ("Missing", K("SELECT")),
    ])
    .unwrap();
    let tree = parse(
        &grammar,
        "one two",
        Limits {
            memo_entries: 5,
            ..Limits::default()
        },
    )
    .unwrap();
    assert_eq!(tree.memo_entries, 5);
    assert_eq!(tree.memo_hits, 4);
    assert_eq!(tree.nodes.len(), 3);
    assert_eq!(tree.children(tree.root).len(), 2);
    assert_eq!(
        parse(
            &grammar,
            "one two",
            Limits {
                memo_entries: 4,
                ..Limits::default()
            }
        )
        .err()
        .unwrap()
        .code,
        SyntaxErrorCode::ParserLimitExceeded,
    );
}

#[test]
fn memoized_subtree_cannot_bypass_language_depth_admission() {
    for limit in [1, 2] {
        let grammar = Grammar::compile_bounded(
            vec![
                (
                    "Root",
                    S(vec![
                        Expression::Ahead(Box::new(R("Leaf")), "lookahead"),
                        R("Deep"),
                        Tok(TokenKind::End, "end"),
                    ]),
                ),
                ("Deep", R("Leaf")),
                ("Leaf", K("SELECT")),
            ],
            &["Leaf", "Deep"],
            limit,
        )
        .unwrap();
        let result = parse(&grammar, "SELECT", Limits::default());
        if limit == 1 {
            assert_eq!(
                result.err().unwrap().code,
                SyntaxErrorCode::ParserLimitExceeded
            );
        } else {
            let tree = result.unwrap();
            assert!(tree.memo_hits > 0);
        }
    }
}

#[test]
fn language_depth_refuses_before_internal_peg_stack_budget() {
    let grammar = Grammar::compile_bounded(
        vec![
            ("Root", S(vec![R("Nest"), Tok(TokenKind::End, "end")])),
            (
                "Nest",
                Choice(vec![
                    S(vec![
                        Tok(TokenKind::LeftParen, "("),
                        R("Nest"),
                        Tok(TokenKind::RightParen, ")"),
                    ]),
                    K("SELECT"),
                ]),
            ),
        ],
        &["Nest"],
        4,
    )
    .unwrap();
    assert!(parse(&grammar, "(((SELECT)))", Limits::default()).is_ok());
    let error = parse(&grammar, "((((SELECT))))", Limits::default())
        .err()
        .unwrap();
    assert_eq!(error.code, SyntaxErrorCode::ParserLimitExceeded);
    assert_eq!(
        error.expected,
        Some("language nesting within the parser limit")
    );
}

#[test]
fn independent_language_budgets_survive_memo_reuse() {
    let rules = || {
        vec![
            (
                "Root",
                Expression::Sequence(vec![
                    Expression::Ahead(
                        Box::new(Expression::Sequence(vec![
                            Expression::Keyword("SELECT"),
                            Expression::Rule("Expr"),
                        ])),
                        "cached expression",
                    ),
                    Expression::Rule("Graph"),
                    Expression::Token(TokenKind::End, "end"),
                ]),
            ),
            (
                "Graph",
                Expression::Sequence(vec![
                    Expression::Keyword("SELECT"),
                    Expression::Rule("Expr"),
                ]),
            ),
            ("Expr", Expression::Keyword("SELECT")),
        ]
    };
    let input = "SELECT SELECT";
    let tokens = crate::tokenize(input).unwrap();
    let grammar: Grammar = Grammar::compile_with_nesting(
        rules(),
        &[
            (
                &["Expr"],
                1,
                SyntaxErrorCode::ExpressionNestingLimitExceeded,
            ),
            (
                &["Graph"],
                1,
                SyntaxErrorCode::GraphPatternNestingLimitExceeded,
            ),
        ],
    )
    .unwrap();
    let tree = grammar
        .parse(input, &tokens, "Root", Limits::default())
        .unwrap();
    assert!(tree.memo_hits > 0);
    for (expression_limit, graph_limit, expected) in [
        (0, 1, SyntaxErrorCode::ExpressionNestingLimitExceeded),
        (1, 0, SyntaxErrorCode::GraphPatternNestingLimitExceeded),
    ] {
        let grammar: Grammar = Grammar::compile_with_nesting(
            rules(),
            &[
                (
                    &["Expr"],
                    expression_limit,
                    SyntaxErrorCode::ExpressionNestingLimitExceeded,
                ),
                (
                    &["Graph"],
                    graph_limit,
                    SyntaxErrorCode::GraphPatternNestingLimitExceeded,
                ),
            ],
        )
        .unwrap();
        let error = grammar
            .parse(input, &tokens, "Root", Limits::default())
            .err()
            .unwrap();
        assert_eq!(error.code, expected);
    }
}

#[test]
fn tight_sequences_preserve_capture_rollback_and_packrat_reuse() {
    let grammar = Grammar::compile(vec![
        (
            "Root",
            S(vec![
                Choice(vec![R("Tight"), R("Loose")]),
                Tok(TokenKind::End, "end"),
            ]),
        ),
        ("Tight", Expression::Tight(Box::new(R("Signed")))),
        ("Loose", R("Signed")),
        (
            "Signed",
            S(vec![
                Tok(TokenKind::Minus, "minus"),
                Tok(TokenKind::Number, "number"),
            ]),
        ),
    ])
    .unwrap();
    let adjacent = parse(&grammar, "-1", Limits::default()).unwrap();
    assert!(adjacent.child(adjacent.root, "Tight").is_some());
    for text in ["- 1", "-/*gap*/1"] {
        let loose = parse(&grammar, text, Limits::default()).unwrap();
        assert!(loose.child(loose.root, "Loose").is_some());
        assert!(loose.child(loose.root, "Tight").is_none());
        assert_eq!(loose.memo_entries, 4);
        assert_eq!(loose.memo_hits, 1);
    }
    // Cypher accepts Unicode whitespace; token PEG is independent of the SQL
    // lexer's deliberately narrower whitespace policy.
    let text = "-\u{2003}1";
    let number = text.find('1').unwrap();
    let tokens = [
        crate::Token::new(TokenKind::Minus, Span::new(0, 1)),
        crate::Token::new(TokenKind::Number, Span::new(number, number + 1)),
        crate::Token::new(TokenKind::End, Span::new(text.len(), text.len())),
    ];
    let loose = grammar
        .parse(text, &tokens, "Root", Limits::default())
        .unwrap();
    assert!(loose.child(loose.root, "Loose").is_some());
    assert_eq!(loose.memo_hits, 1);
}

#[test]
fn tight_adjacency_validation_is_bounded_work_and_not_nullable_repetition() {
    let grammar = Grammar::compile(vec![(
        "Root",
        S(vec![
            Expression::Tight(Box::new(S(vec![
                Tok(TokenKind::Minus, "minus"),
                Tok(TokenKind::Number, "number"),
            ]))),
            Tok(TokenKind::End, "end"),
        ]),
    )])
    .unwrap();
    let tree = parse(&grammar, "-1", Limits::default()).unwrap();
    let error = parse(
        &grammar,
        "-1",
        Limits {
            steps: tree.steps - 2,
            ..Limits::default()
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.code, SyntaxErrorCode::ParserLimitExceeded);
    assert_eq!(
        error.span.start, 1,
        "adjacency checks must charge work at the second token"
    );
    let error = parse(&grammar, "- 1", Limits::default()).err().unwrap();
    assert_eq!(error.expected, Some("adjacent tokens"));
    assert!(Grammar::compile(vec![(
        "Root",
        Repeat(Box::new(Expression::Tight(Box::new(Optional(Box::new(
            K("SELECT")
        ))))))
    )])
    .is_err());
    assert!(Grammar::compile(vec![("Root", Expression::Tight(Box::new(R("Root"))))]).is_err());
}
