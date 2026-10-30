// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[test]
fn corpus_matches_reference() {
    for line in include_str!("../../../fixtures/migration_corpus_v1.jsonl").lines() {
        let case: serde_json::Value = serde_json::from_str(line).unwrap();
        let input = case["query"].as_str().unwrap();
        equivalent(input);
    }
}

fn equivalent(input: &str) {
    let expected = super::super::parse_inner(input);
    let actual = crate::parse(input);
    match (&expected, &actual) {
        (Ok(expected), Ok(actual)) => assert_eq!(expected, actual, "{input}"),
        (Err(hawdb_core::HawDBError::Parse(_)), Err(hawdb_core::HawDBError::Parse(_)))
        | (Err(hawdb_core::HawDBError::Semantic(_)), Err(hawdb_core::HawDBError::Semantic(_))) => {}
        _ => panic!("{input}\nreference: {expected:?}\nPEG: {actual:?}"),
    }
}

#[test]
fn generated_clause_and_wrapper_differential() {
    for prefix in [
        "MATCH (:Item)",
        "UNWIND [1,2] AS x WITH x WITH x MATCH (:Item)",
        "CREATE (:Item) WITH 1 AS x WITH x MATCH (:Item)",
        "MERGE (n:Item {id: 'x'}) WITH n WITH n MATCH (:Item)",
        "CALL vector_search($q, topK := 2) YIELD node WITH node WITH node MATCH (:Item)",
    ] {
        for middle in [
            "",
            " MATCH ()",
            " OPTIONAL MATCH ()-[:REL]->()",
            " WHERE NOT ((:Item)-[:REL]->()) MATCH ()",
            " WHERE EXISTS { MATCH ()-[:REL]->() } MATCH ()",
            " WHERE n.id = 'é;\\\"x' WITH n MATCH (__anon0) MATCH ()",
        ] {
            for suffix in [
                " RETURN 1 AS x",
                " RETURN 1 AS x;",
                " SET n.id = 'x'",
                " RETURN",
                " RETURN 1 AS x garbage",
                " WITH n",
            ] {
                let query = format!("{prefix}{middle}{suffix}");
                for wrapper in ["", "EXPLAIN ", "EXPLAIN ANALYZE ", "CYPHER SYSTEM.cpu = 1 "] {
                    equivalent(&format!("{wrapper}{query}"));
                }
            }
        }
    }
}

#[test]
fn every_utf8_truncation_of_representative_inputs_is_bounded() {
    for input in [
        "EXPLAIN CYPHER SYSTEM.cpu = 1 MATCH (:Item {name: '你好é'}) WHERE NOT (n.id = $id) RETURN n.name;",
        "UNWIND [[1], [2]] AS x WITH x WITH x MATCH () RETURN x",
        "CALL page_rank('g', dampingFactor := 0.85) YIELD node RETURN node.id",
        "CREATE PROPERTY ON NODE Item.name STRING",
    ] {
        for (end, _) in input.char_indices().chain(std::iter::once((input.len(), ' '))) {
            equivalent(&input[..end]);
        }
    }
}

#[test]
fn nesting_budget_includes_statement_wrappers() {
    for depth in 25..36 {
        let value = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        for wrapper in [
            "",
            "EXPLAIN ",
            "CYPHER SYSTEM.cpu = 1 ",
            "EXPLAIN CYPHER SYSTEM.cpu = 1 ",
        ] {
            equivalent(&format!("{wrapper}CREATE (:Item {{value: {value}}})"));
        }
        // Standalone pipeline parsing has no enclosing statement frame.
        let input = format!("UNWIND {value} AS x RETURN x");
        let mut parser = super::super::Parser::new(&input);
        let expected = parser.parse_query_pipeline();
        let actual = crate::parse_pipeline(&input);
        assert_eq!(expected.is_ok(), actual.is_ok(), "{input}");
        if let (Ok(expected), Ok(actual)) = (expected, actual) {
            assert_eq!(expected, actual);
        }
    }
}

#[test]
fn lookahead_reuses_native_clause_captures() {
    super::native::CLAUSE_POSITIONS.with(|positions| positions.borrow_mut().clear());
    let query = "UNWIND [1] AS x WITH x WITH x MATCH () WHERE (x)-[:REL]->() MATCH () RETURN x";
    let expected = super::super::parse_inner(query).unwrap();
    assert_eq!(crate::parse_profiled(query).result.unwrap(), expected);
    super::native::CLAUSE_POSITIONS.with(|positions| {
        let positions = positions.borrow();
        assert_eq!(positions.len(), 6, "each clause must be parsed once");
        assert_eq!(
            positions
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            positions.len()
        );
    });
}

#[test]
fn peg_limits_fail_closed_and_keep_utf8_positions() {
    use hawdb_sql_syntax::peg::Limits;
    let input = "MATCH (n {name: 'é'}) RETURN n";
    let tokens = super::lexer::tokenize(input).unwrap();
    for limits in [
        Limits {
            steps: 1,
            ..Limits::default()
        },
        Limits {
            nodes: 1,
            ..Limits::default()
        },
        Limits {
            memo_entries: 1,
            ..Limits::default()
        },
        Limits {
            depth: 1,
            ..Limits::default()
        },
    ] {
        let error = super::grammar(true)
            .parse(input, &tokens, "root", limits)
            .err()
            .expect("limit must refuse the parse");
        assert_eq!(
            error.code,
            hawdb_sql_syntax::SyntaxErrorCode::ParserLimitExceeded
        );
        assert!(input.is_char_boundary(error.span.start));
    }
}

#[test]
fn migration_mutations_match_reference() {
    let cases = include_str!("../../../fixtures/migration_corpus_v1.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mut state = 7_u64;
    for _ in 0..256 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let input = cases[state as usize % cases.len()]["query"]
            .as_str()
            .unwrap();
        let boundaries = input
            .char_indices()
            .map(|(offset, _)| offset)
            .chain(std::iter::once(input.len()))
            .collect::<Vec<_>>();
        let at = boundaries[(state >> 32) as usize % boundaries.len()];
        equivalent(&input[..at]);
        for insertion in ["(", "'", ";", " WITH x "] {
            equivalent(&format!("{}{insertion}{}", &input[..at], &input[at..]));
        }
    }
}

#[test]
fn generated_names_do_not_rewrite_source_identifiers_or_values() {
    equivalent("MATCH () WHERE (n)-[:R]->() MATCH (__anon0) MATCH () RETURN '\0anon0', '__anon0'");
    equivalent("UNWIND [1] AS x WITH x WITH x MATCH () WHERE (n)-[:R]->() WITH x WHERE EXISTS { MATCH ()-[:R]->() } MATCH () RETURN x");
}

#[test]
fn current_scalar_aggregates_match_reference() {
    for expression in [
        "sum(n.score)",
        "sum(DISTINCT n.score)",
        "sum(n.score + 1)",
        "sum(coalesce(n.score, 0))",
        "max(lower(n.title))",
        "count(DISTINCT lower(n.title))",
        "collect(n.score * 2)",
        "-sum(n.score)",
        "sum(n.score) + count(*)",
    ] {
        for prefix in [
            "MATCH (n:Item)",
            "UNWIND [1] AS x WITH x WITH x MATCH (n:Item)",
        ] {
            equivalent(&format!("{prefix} RETURN {expression} AS total"));
        }
    }
}

#[test]
fn invalid_multi_stage_procedure_reports_the_native_semantic_refusal() {
    let input =
        "CREATE (:E) WITH 1 AS x WITH x CALL project_graph('g', ['E'], {'R': 'r.foo IN [1]'})";
    // The old speculative driver discarded the procedure refusal, selected a
    // CREATE-node template, then reported trailing WITH. PEG commits native
    // semantic refusals rather than disguising them as another grammar miss.
    assert!(matches!(
        super::super::parse_inner(input),
        Err(hawdb_core::HawDBError::Parse(_))
    ));
    assert!(matches!(
        crate::parse(input),
        Err(hawdb_core::HawDBError::Semantic(_))
    ));
}

#[test]
fn token_budget_checks_the_exact_boundary_before_ast_construction() {
    let mut input = "?".repeat(1_000_000);
    assert_eq!(super::lexer::tokenize(&input).unwrap().len(), 1_000_001);
    input.push('?');
    let error = super::lexer::tokenize(&input).unwrap_err().to_string();
    assert!(error.contains("Cypher token count exceeds limit of 1000000"));
    assert!(error.contains("at byte 1000000"));
}
