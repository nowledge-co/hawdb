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

#[test]
fn incomplete_function_calls_preserve_the_following_token_diagnostic() {
    for (query, expected) in [
        (
            "MATCH ()-[r:MENTIONS]->() RETURN COUNT WITH x (r)",
            "unexpected trailing input at byte 39 line 1 column 40 near `WITH x (r)`",
        ),
        (
            "MATCH (n) RETURN ID x",
            "unexpected trailing input at byte 20 line 1 column 21 near `x`",
        ),
    ] {
        for result in [
            crate::parse(query).map(|_| ()),
            crate::parse_pipeline(query).map(|_| ()),
        ] {
            let Err(hawdb_core::HawDBError::Parse(message)) = result else {
                panic!("expected a parse diagnostic for {query}: {result:?}");
            };
            assert_eq!(message, expected, "{query}");
        }
    }
}

#[test]
fn corpus_matches_reference() {
    for line in include_str!("../../../fixtures/migration_corpus_v1.jsonl").lines() {
        let case: serde_json::Value = serde_json::from_str(line).unwrap();
        let input = case["query"].as_str().unwrap();
        equivalent(input);
    }
}

fn equivalent(input: &str) {
    frozen_equivalent(input, "statement");
}

fn frozen_equivalent(input: &str, entry: &str) {
    use std::collections::BTreeMap;
    use std::sync::OnceLock;
    static BASELINE: OnceLock<BTreeMap<(String, String), serde_json::Value>> = OnceLock::new();
    let baseline = BASELINE.get_or_init(|| {
        include_str!("../../../fixtures/migration_expressions_predicates_peg_baseline_v1.jsonl")
            .lines()
            .map(|line| {
                let case: serde_json::Value = serde_json::from_str(line).unwrap();
                (
                    (
                        case["query"].as_str().unwrap().to_owned(),
                        case["entry"].as_str().unwrap().to_owned(),
                    ),
                    case,
                )
            })
            .collect()
    });
    assert_frozen(&baseline[&(input.to_owned(), entry.to_owned())]);
}

fn expected_after_main_transitions(expected: &serde_json::Value) -> &serde_json::Value {
    use std::collections::BTreeMap;
    use std::sync::OnceLock;
    type TransitionMap = BTreeMap<(String, String), serde_json::Value>;
    static TRANSITIONS: OnceLock<[TransitionMap; 6]> = OnceLock::new();
    // Keep each earlier oracle immutable. Main #1411 changes row bindings and
    // read-only UNWIND; #1403 expands ORDER BY and changes aggregate sort ASTs.
    // #1382 adds temporal syntax and generic scalar/postfix behavior. Every
    // transition was exported from an independently compiled main.
    // #1428 admits boolean predicates and row-bound UNWIND sources.
    // #1514 adds Mem string functions, computed UNWIND and CASE SET branches.
    // #1550 admits bounded scalar expressions on the left of regex predicates.
    let transitions = TRANSITIONS.get_or_init(|| {
        [
            include_str!("../../../fixtures/migration_main_1411_peg_baseline_v1.jsonl"),
            include_str!(
                "../../../fixtures/migration_main_1403_order_expressions_peg_baseline_v1.jsonl"
            ),
            include_str!("../../../fixtures/migration_main_1382_temporal_peg_baseline_v1.jsonl"),
            include_str!(
                "../../../fixtures/migration_main_1428_boolean_unwind_peg_baseline_v1.jsonl"
            ),
            include_str!("../../../fixtures/migration_main_1514_mem_peg_baseline_v1.jsonl"),
            include_str!(
                "../../../fixtures/migration_main_1550_scalar_regex_peg_baseline_v1.jsonl"
            ),
        ]
        .map(|fixture| {
            fixture
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|case| case.get("previous_kind").is_some())
                .map(|case| {
                    (
                        (
                            case["query"].as_str().unwrap().to_owned(),
                            case["entry"].as_str().unwrap().to_owned(),
                        ),
                        case,
                    )
                })
                .collect()
        })
    });
    let key = (
        expected["query"].as_str().unwrap().to_owned(),
        expected["entry"].as_str().unwrap().to_owned(),
    );
    // A fixture may already describe an intermediate or final main revision.
    // Resume after that fingerprint instead of applying an older transition.
    let start = transitions
        .iter()
        .rposition(|table| {
            table.get(&key).is_some_and(|current| {
                current["kind"] == expected["kind"]
                    && current["ast_sha256"] == expected["ast_sha256"]
            })
        })
        .map_or(0, |index| index + 1);
    let mut expected = expected;
    for table in &transitions[start..] {
        if let Some(current) = table.get(&key) {
            assert_eq!(expected["kind"], current["previous_kind"], "{key:?}");
            assert_eq!(
                expected["ast_sha256"], current["previous_ast_sha256"],
                "{key:?}"
            );
            expected = current;
        }
    }
    expected
}

fn is_main_1550_scalar_regex_transition(query: &str, entry: &str) -> bool {
    use std::collections::BTreeSet;
    use std::sync::OnceLock;
    static TRANSITIONS: OnceLock<BTreeSet<(String, String)>> = OnceLock::new();
    TRANSITIONS
        .get_or_init(|| {
            include_str!("../../../fixtures/migration_main_1550_scalar_regex_peg_baseline_v1.jsonl")
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .map(|case| {
                    (
                        case["query"].as_str().unwrap().to_owned(),
                        case["entry"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        })
        .contains(&(query.to_owned(), entry.to_owned()))
}

fn assert_frozen(expected: &serde_json::Value) {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let expected = expected_after_main_transitions(expected);
    let input = expected["query"].as_str().unwrap();
    let result = if expected["entry"] == "statement" {
        crate::parse(input).map(|ast| format!("{ast:?}"))
    } else {
        crate::parse_pipeline(input).map(|ast| format!("{ast:?}"))
    };
    if let Some(message) = expected.get("error").and_then(serde_json::Value::as_str) {
        let actual = result.as_ref().err().map(ToString::to_string);
        assert_eq!(actual.as_deref(), Some(message), "{input}");
    }
    let (kind, digest) = match result {
        Ok(ast) => {
            let mut digest = String::with_capacity(64);
            for byte in Sha256::digest(ast.as_bytes()) {
                write!(digest, "{byte:02x}").unwrap();
            }
            ("accept", Some(digest))
        }
        Err(hawdb_core::HawDBError::Parse(_)) => ("Parse", None),
        Err(hawdb_core::HawDBError::Semantic(_)) => ("Semantic", None),
        other => panic!("unexpected baseline result: {other:?}"),
    };
    assert_eq!(
        kind,
        expected["kind"].as_str().unwrap(),
        "{}: {input}",
        expected["entry"]
    );
    assert_eq!(
        digest.as_deref(),
        expected["ast_sha256"].as_str(),
        "{}: {input}",
        expected["entry"]
    );
}

#[test]
fn frozen_expressions_and_predicates_preserve_main_ir() {
    // Exported from independently built main 9c355e9a46, in a main-only target.
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_expressions_predicates_peg_baseline_v1.jsonl")
            .lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 8946);
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
        frozen_equivalent(&input, "pipeline");
    }
}

#[test]
fn lookahead_allocates_anonymous_names_only_for_selected_tree() {
    let query = "UNWIND [1] AS x WITH x WITH x MATCH () WHERE (x)-[:REL]->() MATCH () RETURN x";
    equivalent(query);
}

#[test]
fn peg_limits_fail_closed_and_keep_utf8_positions() {
    use super::rust_peg::{recognize_with_limits, TestLimits};
    let input = "MATCH (n {name: 'é'}) RETURN coalesce(n,0)";
    let tokens = super::lexer::tokenize(input).unwrap();
    for limits in [
        TestLimits {
            steps: 1,
            ..TestLimits::default()
        },
        TestLimits {
            nodes: 1,
            ..TestLimits::default()
        },
        TestLimits {
            nesting: 1,
            ..TestLimits::default()
        },
    ] {
        let error = recognize_with_limits(input, &tokens, "root", true, limits)
            .err()
            .expect("limit must refuse the parse");
        assert_eq!(
            error.code,
            hawdb_sql_syntax::SyntaxErrorCode::ParserLimitExceeded
        );
        assert!(input.is_char_boundary(error.span.start));
    }
    let input = format!(
        "MATCH (n) RETURN {}1{}",
        "toInteger(".repeat(33),
        ")".repeat(33)
    );
    let tokens = super::lexer::tokenize(&input).unwrap();
    let error = recognize_with_limits(
        &input,
        &tokens,
        "root",
        false,
        TestLimits {
            steps: usize::MAX,
            nodes: usize::MAX,
            nesting: usize::MAX,
        },
    )
    .err()
    .expect("test overrides cannot raise production limits");
    assert_eq!(
        error.code,
        hawdb_sql_syntax::SyntaxErrorCode::ParserLimitExceeded
    );
    assert!(input.is_char_boundary(error.span.start));
}

fn assert_named_captures(tree: &super::ParseTree, query: &str) {
    let mut pending = (0..tree.nodes.len())
        .chain(std::iter::once(tree.root))
        .collect::<Vec<_>>();
    let mut seen = std::collections::HashSet::new();
    while let Some(node) = pending.pop() {
        if !seen.insert(node) {
            continue;
        }
        assert!(
            tree.node(node).name.is_some(),
            "native AST capture in {query}"
        );
        pending.extend_from_slice(tree.children(node));
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
fn invalid_multi_stage_procedure_preserves_the_semantic_refusal() {
    let input =
        "CREATE (:E) WITH 1 AS x WITH x CALL project_graph('g', ['E'], {'R': 'r.foo IN [1]'})";
    // Semantic refusals remain visible after syntactic recognition.
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

// Captured from main 0f611b6dec before replacing the clause terminals. Debug
// includes source spans, which AstNode's semantic PartialEq intentionally omits.
#[test]
fn frozen_peg_baseline_preserves_ast_spans_and_rejection_classes() {
    let cases: std::collections::BTreeMap<_, _> =
        include_str!("../../../fixtures/migration_corpus_v1.jsonl")
            .lines()
            .map(|line| {
                let case: serde_json::Value = serde_json::from_str(line).unwrap();
                (
                    case["id"].as_str().unwrap().to_owned(),
                    case["query"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
    let mut count = 0;
    for line in include_str!("../../../fixtures/migration_peg_baseline_v1.jsonl").lines() {
        let mut expected: serde_json::Value = serde_json::from_str(line).unwrap();
        expected["query"] = cases[expected["id"].as_str().unwrap()].clone().into();
        assert_frozen(&expected);
        count += 1;
    }
    assert_eq!(count, 2854);
}

#[test]
fn schema_statements_match_without_native_ast_captures() {
    let mut queries: Vec<String> = [
        "CREATE NODE TABLE Node",
        "CREATE NODE LABEL Node",
        "CREATE RELATIONSHIP TABLE Edge",
        "CREATE RELATIONSHIP TYPE Edge",
        "CREATE INDEX ON :Node(id)",
        "CREATE INDEX ON :Node(id, name)",
        "CREATE RANGE INDEX ON :Node(id)",
        "CREATE FULLTEXT INDEX ON :Node(name)",
        "CREATE CONSTRAINT ON :Node(id) ASSERT UNIQUE",
        "CREATE CONSTRAINT ON :Node(id) ASSERT NOT NULL",
        "CREATE CONSTRAINT ON -[:Edge(id)]-> ASSERT EXISTS",
        "CREATE CONSTRAINT ON -[:Edge(id)]-> ASSERT UNIQUE",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    for value_type in [
        "ANY",
        "BOOL",
        "BOOLEAN",
        "INT",
        "INTEGER",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "TEXT",
        "LIST",
        "CHARACTER VARYING",
        "VARCHAR",
        "VECTOR(3)",
    ] {
        for nullable in ["", " NOT NULL"] {
            queries.push(format!(
                "CREATE PROPERTY ON NODE TABLE Node(value) TYPE {value_type}{nullable}"
            ));
        }
    }
    for state in [
        "DELETE_ONLY",
        "WRITE_ONLY",
        "BACKFILL",
        "VALIDATING",
        "PUBLIC",
        "GC",
    ] {
        queries.push(format!("ALTER NODE TABLE Node SET STATE {state}"));
        queries.push(format!(
            "ALTER PROPERTY ON RELATIONSHIP TABLE Edge(value) SET STATE {state}"
        ));
    }
    assert_eq!(queries.len(), 50);
    for query in queries {
        equivalent(&query);
        let tokens = super::lexer::tokenize(&query).unwrap();
        let tree = super::recognize(&query, &tokens, "root", false).unwrap();
        assert_named_captures(&tree, &query);
    }
    for query in [
        "CREATE RANGE INDEX ON :Node(a,b)",
        "CREATE FULLTEXT INDEX ON :Node(a,b)",
        "CREATE PROPERTY ON NODE TABLE Node(v) TYPE VECTOR(0)",
        "CREATE PROPERTY ON NODE TABLE Node(v) TYPE VECTOR(4294967296)",
        "CREATE PROPERTY ON NODE TABLE Node(v) TYPE VECTOR(1.5)",
        "CREATE NODE TYPE Node",
        "CREATE RELATIONSHIP LABEL Edge",
        "ALTER NODE TABLE Node SET STATE UNKNOWN",
    ] {
        equivalent(query);
        assert!(
            crate::parse(query).is_err(),
            "unsupported schema accepted: {query}"
        );
    }
}

#[test]
fn frozen_values_patterns_and_procedures_preserve_main_ir() {
    // Independently compiled main 9c355e9a46 in a main-only Cargo target.
    // Neither AST output nor rejection classes are regenerated from candidate.
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_values_patterns_procedures_peg_baseline_v1.jsonl")
            .lines()
    {
        let expected: serde_json::Value = serde_json::from_str(line).unwrap();
        let query = expected["query"].as_str().unwrap();
        let result = if expected["entry"] == "statement" {
            crate::parse(query).map(|ast| format!("{ast:?}"))
        } else {
            crate::parse_pipeline(query).map(|ast| format!("{ast:?}"))
        };
        let (kind, digest) = match result {
            Ok(ast) => {
                use sha2::Digest;
                (
                    "accept",
                    Some({
                        use std::fmt::Write;
                        let mut digest = String::with_capacity(64);
                        for byte in sha2::Sha256::digest(ast.as_bytes()) {
                            write!(digest, "{byte:02x}").unwrap();
                        }
                        digest
                    }),
                )
            }
            Err(hawdb_core::HawDBError::Parse(_)) => ("Parse", None),
            Err(hawdb_core::HawDBError::Semantic(_)) => ("Semantic", None),
            other => panic!("unexpected baseline result: {other:?}"),
        };
        assert_eq!(
            kind,
            expected["kind"].as_str().unwrap(),
            "{} {}: {query}",
            expected["id"],
            expected["entry"]
        );
        assert_eq!(
            digest.as_deref(),
            expected["ast_sha256"].as_str(),
            "{} {}: {query}",
            expected["id"],
            expected["entry"]
        );
        count += 1;
    }
    assert_eq!(count, 420);
}

#[test]
fn graph_mutations_values_and_procedures_match_without_native_captures() {
    for query in [
        "CREATE (n:I {x:-.5, y:[1, timestamp($time)], z:CAST($time AS TIMESTAMP)})",
        "MERGE (n:I {id:$id}) ON CREATE SET n.x=COALESCE(n.x,0)+1 ON MATCH SET n.x=n.x+1",
        "SET SYSTEM VARIABLE SYSTEM.value = [null, $p]",
        "CYPHER SYSTEM.value=1 CREATE (:I)",
        "MATCH p=shortestPath((:I)-[:R*]->(:I)) DELETE p",
        "UNWIND [1,2,] AS row CREATE (:I {id:row.id})",
        "MATCH (a:A:B), (b) MERGE (a)-[r:R]->(b) ON CREATE SET r.x=true DELETE b",
        "CALL text_search($q, complete:=true)",
        "CALL vector_index_search('ix',$q,candidates:=8,topK:=2)",
        "CALL page_rank('g') RETURN node, rank",
        "CALL page_rank('g') RETURN node",
        "CALL project_graph('g', ['I'], ['R'], {option:['a']})",
    ] {
        equivalent(query);
        let tokens = super::lexer::tokenize(query).unwrap();
        let tree = super::recognize(query, &tokens, "root", false).unwrap();
        assert_named_captures(&tree, query);
    }
}

#[test]
fn main_1411_row_bound_values_preserve_the_independent_main_ir() {
    let mut count = 0;
    for line in include_str!("../../../fixtures/migration_main_1411_peg_baseline_v1.jsonl").lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 140);
}

#[test]
fn contextual_keywords_preserve_main_case_and_mutation_admission() {
    // Independently exported main 2ae7e88614: specialized CASE may reference
    // keyword-named properties; scalar lists and SET retain their own guards.
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_keyword_context_peg_baseline_v1.jsonl").lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 2380);
}

#[test]
fn main_1403_order_expressions_preserve_independent_main_ir() {
    let mut count = 0;
    for line in include_str!(
        "../../../fixtures/migration_main_1403_order_expressions_peg_baseline_v1.jsonl"
    )
    .lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 714);
}

#[test]
fn main_1382_temporal_preserves_independent_main_ir_and_admission() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1382_temporal_peg_baseline_v1.jsonl").lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 5749);
}

#[test]
fn frozen_main_1428_boolean_predicates_and_row_bound_unwind() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1428_boolean_unwind_peg_baseline_v1.jsonl")
            .lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 856);
}

#[test]
fn frozen_main_1377_load_csv_conversions_and_indexes() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1377_load_csv_peg_baseline_v1.jsonl").lines()
    {
        assert_frozen(&serde_json::from_str(line).unwrap());
        count += 1;
    }
    assert_eq!(count, 1626);
}

#[test]
fn csv_value_extensions_preserve_ordinary_property_diagnostics() {
    for (query, expected) in [
        (
            "CREATE (n:Item {value: .5})",
            "expected value at byte 23 line 1 column 24 near `.5})`",
        ),
        (
            "CREATE (:Memory {id: ",
            "expected value at byte 21 line 1 column 22 near ``",
        ),
    ] {
        for error in [
            crate::parse(query).unwrap_err(),
            crate::parse_pipeline(query).unwrap_err(),
        ] {
            let hawdb_core::HawDBError::Parse(actual) = error else {
                panic!("expected a parse diagnostic");
            };
            assert_eq!(actual, expected, "{query}");
        }
    }
}

#[test]
fn frozen_main_1514_mem_contracts_preserve_complete_ir_and_rejection_classes() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1514_mem_peg_baseline_v1.jsonl").lines()
    {
        let expected: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            expected["source_commit"],
            "fd10958dd37b8878151aeb73ff0477d189b5db5e"
        );
        assert_frozen(&expected);
        count += 1;
    }
    assert_eq!(count, 1537);
}

#[test]
fn frozen_main_1550_scalar_regex_transitions_preserve_complete_ir() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1550_scalar_regex_peg_baseline_v1.jsonl")
            .lines()
    {
        let expected: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(expected["previous_kind"], "Parse");
        assert_frozen(&expected);
        count += 1;
    }
    assert_eq!(count, 44);
}

#[test]
fn frozen_main_mem_api_queries_preserve_complete_ir_and_rejections() {
    let mut count = 0;
    for line in
        include_str!("../../../fixtures/migration_main_1514_mem_api_peg_baseline_v1.jsonl").lines()
    {
        let expected: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(
            expected["source_commit"],
            "fd10958dd37b8878151aeb73ff0477d189b5db5e"
        );
        assert_frozen(&expected);
        count += 1;
    }
    assert_eq!(count, 112);
}

#[test]
fn every_core_list_function_is_recognized_by_the_compiled_grammar() {
    for function in hawdb_core::ListFunction::ALL {
        let (arity, _) = function.arity();
        let arguments = (0..arity)
            .map(|index| format!("$argument_{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!(
            "UNWIND [1] AS row RETURN {}({arguments}) AS result",
            function.function_name()
        );
        crate::parse(&query).unwrap();
        crate::parse_pipeline(&query).unwrap();
    }
}

#[test]
fn diagnostic_repairs_preserve_independently_exported_main_messages_and_locations() {
    for input in [
        include_str!("../../../fixtures/migration_main_1514_diagnostic_repairs_v1.json"),
        include_str!("../../../fixtures/migration_main_1514_diagnostic_repairs_v2.json"),
        include_str!("../../../fixtures/migration_main_1514_diagnostic_repairs_v3.json"),
        include_str!("../../../fixtures/migration_main_1514_diagnostic_repairs_v4.json"),
        include_str!("../../../fixtures/migration_main_1514_diagnostic_repairs_v5.json"),
    ] {
        let fixture: serde_json::Value = serde_json::from_str(input).unwrap();
        assert_eq!(
            fixture["reference_commit"],
            "fd10958dd37b8878151aeb73ff0477d189b5db5e"
        );
        for case in fixture["cases"].as_array().unwrap() {
            let query = case["query"].as_str().unwrap();
            let entry = case["entry"].as_str().unwrap();
            let result = match entry {
                "statement" => crate::parse(query).map(|_| ()),
                "pipeline" => crate::parse_pipeline(query).map(|_| ()),
                entry => panic!("unknown diagnostic fixture entry: {entry}"),
            };
            if is_main_1550_scalar_regex_transition(query, entry) {
                assert!(result.is_ok(), "{query}: {result:?}");
                continue;
            }
            assert_eq!(
                format!("{result:?}"),
                case["expected"].as_str().unwrap(),
                "{query}"
            );
        }
    }
}

#[test]
fn rank_case_lowering_preserves_independent_main_boundaries() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/migration_main_1514_rank_boundaries_v1.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["reference_commit"],
        "fd10958dd37b8878151aeb73ff0477d189b5db5e"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 2026);
    for case in cases {
        assert_frozen(case);
    }
}

#[test]
fn diagnostic_buffers_preserve_unicode_message_and_escaped_context() {
    let input = "é\nx";
    let plain = super::parse_error(input, 2, "message🔥");
    assert_eq!(
        plain.to_string(),
        hawdb_core::HawDBError::Parse("message🔥 at byte 2 line 1 column 2 near `\\nx`".to_owned())
            .to_string()
    );
    let expected =
        super::parse_diagnostic(input, 2, super::DiagnosticMessage::Expected("identifier"));
    assert_eq!(
        expected.to_string(),
        hawdb_core::HawDBError::Parse(
            "expected identifier at byte 2 line 1 column 2 near `\\nx`".to_owned()
        )
        .to_string()
    );
}

#[test]
fn project_graph_string_options_reject_non_string_tokens() {
    let query = "CALL project_graph('g', ['I'], ['R'], [1])";
    for result in [
        crate::parse(query).map(|_| ()),
        crate::parse_pipeline(query).map(|_| ()),
    ] {
        let error = result.unwrap_err();
        let hawdb_core::HawDBError::Parse(message) = error else {
            panic!("expected a string-token parse diagnostic");
        };
        assert_eq!(
            message,
            "expected string at byte 38 line 1 column 39 near `[1])`"
        );
    }
}
