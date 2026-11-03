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
fn failed_parse_does_not_consume_names_in_the_next_query() {
    let query = "MATCH (:Source) MATCH (:Memory) RETURN 1";
    let expected = crate::parse(query).unwrap();
    assert!(crate::parse("MATCH (:Source) MATCH (:Memory) WHERE").is_err());
    assert_eq!(crate::parse(query).unwrap(), expected);
    let crate::Statement::Pipeline(pipeline) = expected else {
        panic!("pipeline");
    };
    for (index, clause) in pipeline.clauses.iter().take(2).enumerate() {
        let crate::ClauseKind::Match { patterns, .. } = &clause.kind else {
            panic!("MATCH");
        };
        assert_eq!(patterns[0].first.variable, format!("__anon{index}"));
    }
}

#[test]
fn failed_deep_parse_does_not_change_the_next_query_budget() {
    let query = "MATCH (n) RETURN n";
    let expected = crate::parse(query).unwrap();
    let nested = format!("UNWIND {}0{} AS x RETURN x", "[".repeat(40), "]".repeat(40));
    assert!(crate::parse(&nested).is_err());
    assert_eq!(crate::parse(query).unwrap(), expected);
}

#[test]
fn graph_seed_procedure_accepts_a_parameterized_canonical_node_pipeline() {
    for query in [
        "CALL graph_seed_search($query, label := 'Memory', topK := $window) YIELD node AS seed, score AS original MATCH (seed)-[:LINK*0..2]->(candidate:Memory) RETURN id(seed), candidate.id, original",
        "CALL graph_seed_search($query, label := $label, limit := 4) YIELD node, score RETURN node.id, score",
    ] {
        assert!(crate::parse(query).is_ok(), "graph seed pipeline must parse: {query}");
    }
}

#[test]
fn identity_898_malformed_legacy_clauses_preserve_exact_diagnostics() {
    // Exact diagnostics exported from independently compiled main 9c355e9a46.
    let mut count = 0;
    for line in include_str!("../../fixtures/migration_legacy_diagnostics_v1.jsonl").lines() {
        let case: serde_json::Value = serde_json::from_str(line).unwrap();
        let query = case["query"].as_str().unwrap();
        assert_eq!(
            crate::parse(query).unwrap_err().to_string(),
            case["error"].as_str().unwrap(),
            "{query}"
        );
        count += 1;
    }
    assert_eq!(count, 9);
}

#[test]
fn identity_898_dispatches_extended_syntax_without_reparsing_legacy_errors() {
    for clause in [
        "RETURN node, rank",
        "RETURN node, node_id, node_label, rank",
        "RETURN node, pagerank_score;",
    ] {
        assert!(matches!(
            crate::parse(&format!("CALL page_rank('g') {clause}")).unwrap(),
            crate::Statement::GraphAlgorithm(_)
        ));
    }
    for clause in [
        "YIELD node_labels AS labels RETURN labels",
        "WITH node_id AS id RETURN id",
        "RETURN node, node_id",
        "RETURN node_label",
        "RETURN node, node_id, node_label, node_labels, rank",
        "RETURN rank AS score",
        "RETURN node, rank ORDER BY rank LIMIT 2",
        "RETURN node, rank + 1 AS score",
    ] {
        assert!(
            matches!(
                crate::parse(&format!("CALL page_rank('g') {clause}")).unwrap(),
                crate::Statement::Pipeline(_)
            ),
            "{clause}"
        );
    }
    assert!(crate::parse("CALL page_rank('g') RETURN node AS")
        .unwrap_err()
        .to_string()
        .contains("expected identifier"));
}

#[test]
fn complete_text_search_requires_unique_named_options() {
    crate::parse("CALL text_search($q, complete := true, maxCandidates := $cap) YIELD id, score RETURN id, score").unwrap();
    for options in [
        "complete := true, complete := false",
        "maxCandidates := 1, maxCandidates := 2",
        "topK := 1, limit := 2",
        "maxCandidate := 1",
    ] {
        let query = format!("CALL text_search($q, {options}) YIELD id RETURN id");
        assert!(
            matches!(crate::parse(&query), Err(hawdb_core::HawDBError::Parse(_))),
            "{query}"
        );
    }
}

#[test]
fn vector_index_requires_unique_named_window_options() {
    for (arguments, diagnostic) in [
        ("'docs', $q", "requires candidates"),
        ("'docs', $q, candidates := 8", "requires topK"),
        ("'docs', $q, topK := 1", "requires candidates"),
        (
            "'docs', $q, candidates := 8, candidates := 4, topK := 1",
            "duplicate vector index search option",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1, topK := 2",
            "duplicate vector index search option",
        ),
        (
            "'docs', $q, candidates := 8, topK := 1, limit := 2",
            "unsupported vector index search option",
        ),
    ] {
        let query = format!("CALL vector_index_search({arguments}) YIELD node RETURN node");
        let error = crate::parse(&query).unwrap_err();
        assert!(
            matches!(&error, hawdb_core::HawDBError::Parse(message) if message.contains(diagnostic)),
            "{query}: {error:?}"
        );
    }
    for arguments in ["'docs', $q, 8, 1", "'docs', $q, candidates => 8, topK => 1"] {
        let query = format!("CALL vector_index_search({arguments}) YIELD node RETURN node");
        assert!(
            matches!(crate::parse(&query), Err(hawdb_core::HawDBError::Parse(_))),
            "{query}"
        );
    }
    crate::parse(
        "CALL vector_index_search('docs', $q, topK := 1, candidates := 8) YIELD node RETURN node",
    )
    .unwrap();
}

#[test]
fn vector_index_exact_options_require_static_mode_and_node_domain() {
    for options in [
        "mode := 'exact', topK := 1, maxWork := 10",
        "mode := 'exact', eligible := d, topK := 1",
        "mode := 'exact', eligible := d, topK := 1, maxWork := 10, candidates := 2",
        "mode := $mode, eligible := d, topK := 1, maxWork := 10",
        "mode := 'filtered_ann', eligible := d, topK := 1, maxWork := 10",
        "mode := 'filtered_ann', eligible := d, candidates := 2, topK := 1",
        "mode := 'filtered_ann', candidates := 2, topK := 1, maxWork := 10",
        "mode := 'ann_window', candidates := 2, topK := 1, maxWork := 10",
    ] {
        assert!(
            crate::parse(&format!(
                "MATCH (d) CALL vector_index_search('docs', $q, {options}) YIELD node RETURN node"
            ))
            .is_err(),
            "{options}"
        );
    }
    crate::parse("MATCH (d) CALL vector_index_search('docs', $q, mode := 'exact', eligible := d, topK := 1, maxWork := $work) YIELD node RETURN node").unwrap();
    crate::parse("MATCH (d) CALL vector_index_search('docs', $q, mode := 'filtered_ann', eligible := d, candidates := $candidates, topK := 1, maxWork := $work) YIELD node RETURN node").unwrap();
}
