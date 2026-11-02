// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `SearchHitHydration` levels return the same ranking with less I/O.

use super::*;

fn source(number: usize) -> SearchDocument {
    let angle = number as f32 * 0.37;
    SearchDocument {
        id: format!("document:{number:04}"),
        title: format!("graph title {number}"),
        content: if number.is_multiple_of(2) {
            "graph storage body".into()
        } else {
            "unrelated body".into()
        },
        embedding: (number % 7 != 3).then(|| vec![angle.cos(), angle.sin(), 0.25]),
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("external_id".to_string(), format!("external-{number}")),
            ("source_id".to_string(), format!("source-{}", number % 5)),
            (
                "space_id".to_string(),
                if number.is_multiple_of(3) { "a" } else { "b" }.to_string(),
            ),
        ]),
    }
}

fn scores(hit: &SearchHit) -> (String, u64, u64, u64, u64, Option<usize>, Option<usize>) {
    (
        hit.id.clone(),
        hit.score.to_bits(),
        hit.vector_score.to_bits(),
        hit.text_score.to_bits(),
        hit.rrf_score.to_bits(),
        hit.vector_rank,
        hit.text_rank,
    )
}

#[test]
fn hit_hydration_levels() {
    let root = test_dir();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(crate::SearchEmbeddingManifest {
                model: "hydration-levels".into(),
                version: Some("v1".into()),
                dimension: 3,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let sources = (0..40).map(source).collect::<Vec<_>>();
    for document in &sources {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let query = [0.8f32, 0.6, 0.25];
    let scorer = crate::VectorRelevanceQuery::new(&query, hawdb_core::VectorDistance::Cosine);
    let mut filtered = SearchQueryOptions {
        limit: 6,
        offset: 1,
        rank_window: Some(40),
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let unfiltered = filtered.clone();
    filtered
        .metadata_filters
        .insert("space_id".into(), "b".into());
    let mut requests = vec![];
    for options in [unfiltered, filtered] {
        #[cfg(feature = "vector-search")]
        {
            requests.push(("", SearchMode::Vector, options.clone(), true));
            requests.push(("", SearchMode::Vector, options.clone(), false));
        }
        #[cfg(feature = "full-text-search")]
        requests.push(("graph", SearchMode::Text, options.clone(), false));
        #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
        {
            requests.push(("graph", SearchMode::Hybrid, options.clone(), false));
            // Query terms keep body hydration even for vector-only ranking.
            requests.push(("graph", SearchMode::Vector, options.clone(), false));
        }
    }
    for (text, mode, options, exact) in requests {
        let embedding = (mode != SearchMode::Text).then_some(&query[..]);
        let execution = SearchOutOfCoreExecutionContext {
            vector_execution_options: if exact {
                VectorSearchExecutionOptions::default().with_exact_search()
            } else {
                VectorSearchExecutionOptions::default()
            },
            ..Default::default()
        };
        let run = |level| {
            reader
                .search_with_hydration(text, embedding, mode, options.clone(), execution, level)
                .unwrap()
        };
        let documents = run(SearchHitHydration::Documents);
        let scores_only = run(SearchHitHydration::ScoresOnly);
        assert!(!documents.result.hits.is_empty(), "{mode:?} {text:?}");
        assert_eq!(
            documents.result.hits.iter().map(scores).collect::<Vec<_>>(),
            scores_only
                .result
                .hits
                .iter()
                .map(scores)
                .collect::<Vec<_>>(),
            "{mode:?} {text:?} {exact}"
        );
        assert_eq!(documents.result.total_hits, scores_only.result.total_hits);
        assert_eq!(documents.result.retrievers, scores_only.result.retrievers);
        assert_eq!(scores_only.metrics.hydrated_documents, 0);
        assert_eq!(scores_only.metrics.hydrated_bytes, 0);
        assert_eq!(scores_only.metrics.hydration_segment_bytes_read, 0);
        // Only sidecar hydration reads metadata beyond candidate filtering.
        let sidecar = mode == SearchMode::Vector && text.is_empty();
        if sidecar {
            assert!(
                documents.metrics.metadata_segment_bytes_read
                    > scores_only.metrics.metadata_segment_bytes_read
            );
        } else {
            assert_eq!(
                documents.metrics.metadata_segment_bytes_read,
                scores_only.metrics.metadata_segment_bytes_read
            );
        }
        for hit in &scores_only.result.hits {
            assert!(hit.kind.is_none() && hit.external_id.is_none() && hit.source_id.is_none());
            assert!(hit.matched_terms.is_empty() && hit.matched_spans.is_empty());
        }

        // Default hydration matches the stored documents exactly.
        assert_eq!(
            documents.metrics.hydration_segment_bytes_read == 0,
            sidecar,
            "{mode:?} {text:?}"
        );
        assert_eq!(
            documents.metrics.hydrated_documents,
            documents.result.hits.len()
        );
        for hit in &documents.result.hits {
            let stored = &sources[hit.id["document:".len()..].parse::<usize>().unwrap()];
            assert_eq!(hit.kind.as_deref(), Some("memory"));
            assert_eq!(hit.external_id.as_ref(), stored.metadata.get("external_id"));
            assert_eq!(hit.source_id.as_ref(), stored.metadata.get("source_id"));
            if mode != SearchMode::Text {
                let expected = stored
                    .embedding
                    .as_deref()
                    .and_then(|embedding| scorer.score(embedding))
                    .filter(|score| *score > 0.0)
                    .unwrap_or(0.0);
                assert_eq!(hit.vector_score.to_bits(), expected.to_bits(), "{}", hit.id);
            }
            if sidecar {
                assert!(hit.matched_terms.is_empty() && hit.matched_spans.is_empty());
            }
        }
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
