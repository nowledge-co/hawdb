// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{bm25_score, tokenize, TextCorpusStats};

fn document(id: usize, title: &str, content: &str, visible: bool) -> SearchDocument {
    SearchDocument {
        id: format!("doc-{id:04}"),
        title: title.into(),
        content: content.into(),
        embedding: None,
        metadata: BTreeMap::from([
            (
                "space_id".into(),
                if visible { "inside" } else { "outside" }.into(),
            ),
            ("kind".into(), "Memory".into()),
            ("external_id".into(), format!("SourceChunk_{id}")),
            ("ignored".into(), "graph graph graph".into()),
        ]),
    }
}

fn assert_reference(
    documents: &[&SearchDocument],
    terms: &BTreeSet<String>,
    analyzer: &SearchAnalyzerLexicon,
) {
    let reference = TextCorpusStats::from_documents(documents.iter().copied(), analyzer);
    let mut scorer = Scorer::from_documents(terms, documents.iter().copied(), analyzer).unwrap();
    assert_eq!(scorer.document_count, reference.document_count);
    assert_eq!(
        scorer.average_document_len.to_bits(),
        reference.average_document_len.to_bits()
    );
    assert_eq!(scorer.document_frequency.len(), terms.len());
    for (term, index) in &scorer.query_terms {
        assert_eq!(
            scorer.document_frequency[*index],
            reference
                .document_frequency
                .get(*term)
                .copied()
                .unwrap_or(0)
        );
    }
    for document in documents {
        assert_eq!(
            scorer.score(document, analyzer).unwrap().to_bits(),
            bm25_score(terms, document, &reference, analyzer).to_bits(),
            "document={} terms={terms:?}",
            document.id
        );
    }
}

fn query_terms(width: usize) -> BTreeSet<String> {
    let mut terms = BTreeSet::from(["graph".into(), "storage".into()]);
    for index in 2..width {
        terms.insert(format!(
            "missing{}{}x",
            char::from(b'a' + (index / 26) as u8),
            char::from(b'a' + (index % 26) as u8)
        ));
    }
    terms
}

#[test]
fn query_statistics_preserve_weighted_alias_cjk_and_filtered_score_bits() {
    let documents = [
        document(0, "graph graph graph", "storage graph graph", true),
        document(1, "graph graph graph", "storage graph graph", true),
        document(
            2,
            "SourceChunk source_chunk",
            "sourceChunk_42 source_chunk source",
            true,
        ),
        document(
            3,
            "全文检索 数据库",
            "中文全文搜索 graph storage 中文全文搜索",
            true,
        ),
        document(4, "", "", true),
        document(5, "graph graph", &"unrelated ".repeat(256), false),
    ];
    let empty = SearchDocument {
        metadata: BTreeMap::new(),
        ..document(6, "", "", true)
    };
    let all = documents.iter().chain([&empty]).collect::<Vec<_>>();
    let filtered = documents
        .iter()
        .filter(|doc| doc.metadata["space_id"] == "inside")
        .collect::<Vec<_>>();
    let aliases = SearchAnalyzerLexicon::empty()
        .with_alias_rule(["graph", "storage"], ["store", "graph"])
        .with_stopwords(["unrelated"]);
    for analyzer in [
        SearchAnalyzerLexicon::empty(),
        SearchAnalyzerLexicon::default(),
        aliases,
    ] {
        for query in [
            "",
            "missing",
            "graph",
            "GRAPH graph storage",
            "SourceChunk_42",
            "中文 全文检索",
            "kind Memory inside",
        ] {
            let terms = tokenize(query, &analyzer);
            for corpus in [&all[..], &filtered[..], &[][..]] {
                assert_reference(corpus, &terms, &analyzer);
            }
        }
        for width in [32, 128] {
            assert_reference(&all, &query_terms(width), &analyzer);
        }
    }
}

#[test]
fn query_statistics_match_the_legacy_scorer_on_seeded_mixed_corpora() {
    let pieces = [
        "graph",
        "storage",
        "epoch",
        "cache",
        "a_b",
        "SourceChunk42",
        "全文检索",
        "中文数据库",
        "GRAPH",
        "graph_storage",
    ];
    let analyzer = SearchAnalyzerLexicon::default();
    for seed in 1..=4u64 {
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) as usize
        };
        let documents = (0..24)
            .map(|id| {
                let title = pieces[next() % pieces.len()];
                let count = next() % 40;
                let content = (0..count)
                    .map(|_| pieces[next() % pieces.len()])
                    .collect::<Vec<_>>()
                    .join(" ");
                document(id, title, &content, !id.is_multiple_of(3))
            })
            .collect::<Vec<_>>();
        for query in [
            "graph storage",
            "全文数据库",
            "SourceChunk a_b",
            "epoch graph storage cache",
            "missing",
        ] {
            let terms = tokenize(query, &analyzer);
            for filtered in [false, true] {
                let corpus = documents
                    .iter()
                    .filter(|doc| !filtered || doc.metadata["space_id"] == "inside")
                    .collect::<Vec<_>>();
                assert_reference(&corpus, &terms, &analyzer);
            }
        }
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn resident_text_query_preserves_filtered_ranks_offsets_counts_and_ties() {
    use crate::{SearchFusionWeights, SearchIndex, SearchMode, SearchQueryOptions};
    let analyzer = SearchAnalyzerLexicon::default();
    let documents = [
        document(0, "graph graph", "storage", true),
        document(1, "graph graph", "storage", true),
        document(2, "storage", "graph graph storage", true),
        document(3, "unrelated", "neither term", true),
        document(4, "graph", &"unrelated ".repeat(512), false),
    ];
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(analyzer.clone());
    for doc in &documents {
        index.upsert(doc.clone()).unwrap();
    }
    for filtered in [false, true] {
        let corpus = documents
            .iter()
            .filter(|doc| !filtered || doc.metadata["space_id"] == "inside")
            .collect::<Vec<_>>();
        let reference = TextCorpusStats::from_documents(corpus.iter().copied(), &analyzer);
        for query in ["graph storage", "missing", ""] {
            let terms = tokenize(query, &analyzer);
            if filtered && query == "graph storage" {
                let unfiltered = TextCorpusStats::from_documents(documents.iter(), &analyzer);
                assert_ne!(
                    bm25_score(&terms, corpus[0], &reference, &analyzer).to_bits(),
                    bm25_score(&terms, corpus[0], &unfiltered, &analyzer).to_bits(),
                    "the fixture must distinguish filtered and full-corpus statistics"
                );
            }
            let mut expected = corpus
                .iter()
                .filter_map(|doc| {
                    let score = bm25_score(&terms, doc, &reference, &analyzer);
                    (score > 0.0).then_some((doc.id.clone(), score))
                })
                .collect::<Vec<_>>();
            expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            for offset in [0, 1, 8] {
                for window in [None, Some(1), Some(4)] {
                    let result = index
                        .search_with_options(
                            query,
                            None,
                            SearchMode::Text,
                            SearchQueryOptions {
                                limit: 2,
                                offset,
                                rank_window: window,
                                fusion_weights: SearchFusionWeights::default(),
                                metadata_filters: if filtered {
                                    BTreeMap::from([("space_id".into(), "inside".into())])
                                } else {
                                    BTreeMap::new()
                                },
                                policy_epoch: None,
                            },
                        )
                        .unwrap();
                    assert_eq!(result.filtered_document_count, corpus.len());
                    assert_eq!(result.total_hits, expected.len());
                    assert_eq!(result.truncated, expected.len() > offset + 2);
                    let page = expected
                        .iter()
                        .enumerate()
                        .skip(offset)
                        .take(2)
                        .collect::<Vec<_>>();
                    assert_eq!(result.hits.len(), page.len());
                    for (hit, (rank, (id, score))) in result.hits.iter().zip(page) {
                        assert_eq!(&hit.id, id);
                        assert_eq!(hit.text_score.to_bits(), score.to_bits());
                        assert_eq!(hit.score.to_bits(), score.to_bits());
                        assert_eq!(hit.text_rank, Some(rank + 1));
                    }
                    let report = result.retrievers.iter().find(|r| r.name == "text").unwrap();
                    assert_eq!(report.backend, "bm25_text");
                    assert!(!report.segmented_lexical_projection_used);
                }
            }
        }
    }
}

#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
#[test]
fn resident_hybrid_query_preserves_legacy_bm25_fusion_windows_and_pages() {
    use crate::{SearchFusionWeights, SearchIndex, SearchMode, SearchQueryOptions};
    let analyzer = SearchAnalyzerLexicon::default();
    let mut documents = [
        document(0, "graph graph", "storage", true),
        document(1, "graph graph", "storage", true),
        document(2, "storage", "graph graph", true),
        document(3, "unrelated", "a vector-only match", true),
        document(4, "graph", &"unrelated ".repeat(128), false),
    ];
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(analyzer.clone());
    for (id, doc) in documents.iter_mut().enumerate() {
        doc.embedding = Some(vec![1.0, id as f32 / 4.0]);
        index.upsert(doc.clone()).unwrap();
    }
    let query = "graph storage";
    let terms = tokenize(query, &analyzer);
    let weights = SearchFusionWeights {
        vector_weight: 0.7,
        text_weight: 1.3,
    };
    for filtered in [false, true] {
        let filters = if filtered {
            BTreeMap::from([("space_id".into(), "inside".into())])
        } else {
            BTreeMap::new()
        };
        let corpus = documents
            .iter()
            .filter(|doc| !filtered || doc.metadata["space_id"] == "inside")
            .collect::<Vec<_>>();
        let statistics = TextCorpusStats::from_documents(corpus.iter().copied(), &analyzer);
        let text_scores = corpus
            .iter()
            .filter_map(|doc| {
                let score = bm25_score(&terms, doc, &statistics, &analyzer);
                (score > 0.0).then_some((doc.id.clone(), score))
            })
            .collect::<BTreeMap<_, _>>();
        let text_ranks = crate::ranked_scores(&text_scores);
        // The unchanged vector-only API supplies its component independently
        // of BM25; every fixture vector has a positive exact cosine score.
        let vectors = index
            .search_with_options_exact(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                SearchQueryOptions {
                    limit: documents.len(),
                    offset: 0,
                    rank_window: None,
                    fusion_weights: weights,
                    metadata_filters: filters.clone(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(vectors.hits.len(), corpus.len());
        for window in [None, Some(1), Some(4)] {
            let mut expected = vectors
                .hits
                .iter()
                .filter_map(|vector| {
                    let rank =
                        |rank: Option<usize>| rank.filter(|r| window.is_none_or(|w| *r <= w));
                    let vr = rank(vector.vector_rank);
                    let tr = rank(text_ranks.get(&vector.id).copied());
                    let score = crate::weighted_rrf_score(
                        crate::rrf_child_score(vr),
                        crate::rrf_child_score(tr),
                        weights,
                    );
                    (score > 0.0).then_some((vector, tr, vr, score))
                })
                .collect::<Vec<_>>();
            expected.sort_by(|a, b| b.3.total_cmp(&a.3).then_with(|| a.0.id.cmp(&b.0.id)));
            for offset in [0, 2, 12] {
                let result = index
                    .search_with_options_exact(
                        query,
                        Some(&[1.0, 0.0]),
                        SearchMode::Hybrid,
                        SearchQueryOptions {
                            limit: 2,
                            offset,
                            rank_window: window,
                            fusion_weights: weights,
                            metadata_filters: filters.clone(),
                            policy_epoch: None,
                        },
                    )
                    .unwrap();
                assert_eq!(result.total_hits, expected.len());
                assert_eq!(result.truncated, expected.len() > offset + 2);
                let page = expected.iter().skip(offset).take(2).collect::<Vec<_>>();
                assert_eq!(result.hits.len(), page.len());
                for (hit, (vector, tr, vr, score)) in result.hits.iter().zip(page) {
                    assert_eq!(hit.id, vector.id);
                    assert_eq!(hit.vector_score.to_bits(), vector.vector_score.to_bits());
                    assert_eq!(
                        hit.text_score.to_bits(),
                        text_scores.get(&hit.id).copied().unwrap_or(0.0).to_bits()
                    );
                    assert_eq!(hit.text_rank, *tr);
                    assert_eq!(hit.vector_rank, *vr);
                    assert_eq!(hit.score.to_bits(), score.to_bits());
                    assert_eq!(hit.rrf_score.to_bits(), score.to_bits());
                }
            }
        }
    }
}

fn vocabulary_document(id: usize, words: usize) -> SearchDocument {
    let mut content = String::from("graph storage ");
    for word in 0..words {
        let value = id * words + word;
        content.push_str(&format!(
            "word{}{}{}x ",
            char::from(b'a' + (value / 676 % 26) as u8),
            char::from(b'a' + (value / 26 % 26) as u8),
            char::from(b'a' + (value % 26) as u8)
        ));
    }
    document(id, "graph", &content, true)
}

fn legacy_run(
    documents: &[SearchDocument],
    terms: &BTreeSet<String>,
    analyzer: &SearchAnalyzerLexicon,
) -> f64 {
    let statistics = TextCorpusStats::from_documents(documents.iter(), analyzer);
    documents
        .iter()
        .map(|doc| bm25_score(terms, doc, &statistics, analyzer))
        .sum()
}

fn query_run(
    documents: &[SearchDocument],
    terms: &BTreeSet<String>,
    analyzer: &SearchAnalyzerLexicon,
) -> f64 {
    let mut scorer = Scorer::from_documents(terms, documents.iter(), analyzer).unwrap();
    documents
        .iter()
        .map(|doc| scorer.score(doc, analyzer).unwrap())
        .sum()
}

#[test]
fn resident_statistics_peak_drops_for_a_large_non_query_vocabulary() {
    let _serial = crate::test_allocation::serial();
    let documents = (0..32)
        .map(|id| vocabulary_document(id, 128))
        .collect::<Vec<_>>();
    let analyzer = SearchAnalyzerLexicon::empty();
    let terms = BTreeSet::from(["graph".into(), "storage".into()]);
    let reference = TextCorpusStats::from_documents(documents.iter(), &analyzer);
    assert!(reference.document_frequency.len() > 4096);
    let baseline = crate::test_allocation::live();
    let (old, old_peak) =
        crate::test_allocation::measure(|| legacy_run(&documents, &terms, &analyzer));
    assert_eq!(crate::test_allocation::live(), baseline);
    let (new, new_peak) =
        crate::test_allocation::measure(|| query_run(&documents, &terms, &analyzer));
    assert_eq!(crate::test_allocation::live(), baseline);
    assert_eq!(old.to_bits(), new.to_bits());
    assert!(
        new_peak - baseline < (old_peak - baseline) / 2,
        "query-only statistics must lower peak allocations: legacy={}, query={}",
        old_peak - baseline,
        new_peak - baseline
    );
    eprintln!(
        "resident peak legacy={} query={}",
        old_peak - baseline,
        new_peak - baseline
    );
}

#[test]
#[ignore = "manual legacy/candidate allocation and warmed scorer timing comparison"]
fn resident_bm25_measurement() {
    let _serial = crate::test_allocation::serial();
    let analyzer = SearchAnalyzerLexicon::empty();
    for (count, words, width) in [(4, 8, 2), (4, 8, 32), (64, 64, 2), (128, 256, 2)] {
        let terms = query_terms(width);
        let documents = (0..count)
            .map(|id| vocabulary_document(id, words))
            .collect::<Vec<_>>();
        let old = || legacy_run(&documents, &terms, &analyzer);
        let new = || query_run(&documents, &terms, &analyzer);
        for _ in 0..2 {
            assert_eq!(old().to_bits(), new().to_bits());
        }
        let mut samples = [Vec::new(), Vec::new()];
        for repeat in 0..21 {
            for variant in if repeat % 2 == 0 { [0, 1] } else { [1, 0] } {
                let start = std::time::Instant::now();
                std::hint::black_box(if variant == 0 { old() } else { new() });
                samples[variant].push(start.elapsed().as_secs_f64() * 1e6);
            }
        }
        let baseline = crate::test_allocation::live();
        let (_, old_peak) = crate::test_allocation::measure(old);
        let (_, new_peak) = crate::test_allocation::measure(new);
        assert_eq!(crate::test_allocation::live(), baseline);
        let mut medians = samples.clone();
        for samples in &mut medians {
            samples.sort_by(f64::total_cmp);
        }
        eprintln!("resident docs={count} words_per_doc={words} query_terms={width} legacy_peak={} query_peak={} legacy_p50_us={:.3} query_p50_us={:.3} legacy_us={:?} query_us={:?}", old_peak-baseline, new_peak-baseline, medians[0][10], medians[1][10], samples[0], samples[1]);
    }
}
