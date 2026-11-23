// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The one-pass matcher equals the reference whole-document and per-word
//! analyses for aliases, phrases, identifiers, stopwords, CJK and metadata.

use super::*;
use std::collections::BTreeMap;

fn document(title: &str, content: &str) -> SearchDocument {
    SearchDocument {
        id: "doc".into(),
        title: title.into(),
        content: content.into(),
        embedding: None,
        metadata: BTreeMap::from([
            ("kind".into(), "Memory".into()),
            ("external_id".into(), "ext_graphStream-7".into()),
            ("source_id".into(), "source".into()),
            ("space_id".into(), "team".into()),
            ("other".into(), "unindexed graph".into()),
        ]),
    }
}

#[test]
fn one_pass_matches_equal_reference_analysis() {
    let lexicons = [
        SearchAnalyzerLexicon::default(),
        SearchAnalyzerLexicon::empty(),
        SearchAnalyzerLexicon::nowledge_memory(),
        SearchAnalyzerLexicon::default()
            .with_alias_rule(["graph stream"], ["pipeline"])
            .with_alias_rule(["http"], ["web", "protocol"])
            .with_stopwords(["the", "of"]),
    ];
    let documents = [
        document(
            "GraphStream HTTPServerV2",
            "memory-retrieval graph_stream running Runs graph stream graph",
        ),
        document(
            "Graph Graph",
            "graph_graph GRAPH_stream graphGraph x_graph_y",
        ),
        document("中文数据库", "北京海淀区 GraphStream 数据检索 北京大学"),
        document(
            "",
            "The role of the HTTP server, of graph streams: running v2.0",
        ),
        document(
            "Ünïcödé Straße",
            "straße STRASSE café_au_lait ñandú 42abc abc42",
        ),
        document("No body match", ""),
        document("ab", "a b ab a_b aB Ab ab ab ab"),
        document(
            &format!("{} {}", "x".repeat(64), "y".repeat(70)),
            &format!("{}z {} {}", "y".repeat(69), "y".repeat(70), "x".repeat(63)),
        ),
    ];
    let long = format!("{} {} {}", "x".repeat(64), "y".repeat(70), "x".repeat(63));
    let queries = [
        "graph http run memory",
        "graph graph_stream",
        "北京 graph 数据库",
        "memory team source",
        "pipeline web protocol server",
        "straße café ñandú 42abc abc 42",
        "ab a b",
        "ext graphstream 7 unindexed",
        "",
        long.as_str(),
    ];
    let mut compared = 0;
    let mut spans_seen = 0;
    for analyzer in &lexicons {
        for document in &documents {
            for query in queries {
                let terms = crate::tokenize_list(query, analyzer)
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let (actual_terms, actual_spans) = crate::query_matches_bounded(
                    &terms,
                    document,
                    analyzer,
                    usize::MAX,
                    u64::MAX,
                    0,
                )
                .unwrap();
                assert_eq!(
                    actual_terms,
                    crate::matched_query_terms(&terms, document, analyzer),
                    "{query:?} {:?}",
                    document.title
                );
                assert_eq!(
                    actual_spans,
                    crate::matched_query_spans(&terms, document, analyzer),
                    "{query:?} {:?}",
                    document.title
                );
                compared += 1;
                spans_seen += actual_spans.len();
            }
        }
    }
    assert_eq!(compared, lexicons.len() * documents.len() * queries.len());
    assert!(spans_seen > 100, "{spans_seen}");
}

#[test]
fn one_pass_span_limits_match_reference_collector() {
    let analyzer = SearchAnalyzerLexicon::default();
    let document = document("graph graph", "graph stream graph_stream");
    let terms = crate::tokenize_list("graph stream", &analyzer)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let spans = crate::matched_query_spans(&terms, &document, &analyzer);
    assert!(spans.len() > 2);
    for count in 0..spans.len() {
        let actual = crate::query_matches_bounded(&terms, &document, &analyzer, count, u64::MAX, 0)
            .unwrap_err();
        let expected =
            crate::matched_query_spans_bounded(&terms, &document, &analyzer, count, u64::MAX, 0)
                .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
    }
    let total = spans
        .iter()
        .map(|span| {
            (span.field.len() + span.text.len() + span.term.len()) as u64
                + std::mem::size_of::<crate::SearchMatchedSpan>() as u64
        })
        .sum::<u64>();
    for (limit, used) in [(total - 1, 0), (total + 6, 7), (total, 3), (1, 0)] {
        let actual =
            crate::query_matches_bounded(&terms, &document, &analyzer, usize::MAX, limit, used)
                .unwrap_err();
        let expected = crate::matched_query_spans_bounded(
            &terms,
            &document,
            &analyzer,
            usize::MAX,
            limit,
            used,
        )
        .unwrap_err();
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }
}
