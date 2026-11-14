// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_vector_projection::{ProjectionMetric, RaBitQBitWidth};

#[test]
fn cold_rabitq_queries_validate_before_building_and_keep_l2_zero_queries_valid() {
    let fixture = squared_l2_fixture();
    for scoring in [
        SearchVectorScoring::ProjectionMetric,
        SearchVectorScoring::LinearSquaredL2,
    ] {
        let mut index = SearchIndex::in_memory();
        index.set_vector_scoring(scoring).unwrap();
        index.set_rabitq_projection_build_options(Default::default());
        for row in &fixture.documents {
            index.upsert(fixture.document(row)).unwrap();
        }
        let mut nan = fixture.query.clone();
        nan[0] = f32::NAN;
        let mut infinite = fixture.query.clone();
        infinite[0] = f32::INFINITY;
        for query in [vec![1.0], Vec::new(), nan, infinite] {
            for compressed in [false, true] {
                let result = if compressed {
                    index.try_search_with_options_adaptive_vector_projection_context(
                        "",
                        Some(&query),
                        SearchMode::Vector,
                        options(1, None),
                        AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Required),
                        VectorSearchExecutionOptions::default(),
                    )
                } else {
                    index.search_with_options(
                        "",
                        Some(&query),
                        SearchMode::Vector,
                        options(1, None),
                    )
                };
                if let Ok(result) = result {
                    assert!(result.hits.is_empty());
                }
                assert!(
                    index.rabitq_projection().is_none(),
                    "{scoring:?}, dimension={}, first={:?}",
                    query.len(),
                    query.first()
                );
            }
        }
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let task = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);
        let result = index.try_search_with_options_context(
            "",
            Some(&fixture.query),
            SearchMode::Vector,
            options(1, None),
            VectorSearchExecutionOptions::admitted(64 << 20, &task),
        );
        assert!(result.unwrap_err().to_string().contains("cancel"));
        assert!(index.rabitq_projection().is_none());
        let zero = vec![0.0; fixture.query.len()];
        let empty_domain = index
            .search_with_options(
                "",
                Some(&fixture.query),
                SearchMode::Vector,
                SearchQueryOptions {
                    metadata_filters: BTreeMap::from([("missing".into(), "value".into())]),
                    ..options(1, None)
                },
            )
            .unwrap();
        assert!(empty_domain.hits.is_empty());
        assert!(index.rabitq_projection().is_none());
        let empty_page = index
            .search_with_options(
                "",
                Some(&fixture.query),
                SearchMode::Vector,
                options(0, None),
            )
            .unwrap();
        assert!(empty_page.hits.is_empty());
        assert!(index.rabitq_projection().is_none());
        let result = index
            .search_with_options("", Some(&zero), SearchMode::Vector, options(1, None))
            .unwrap();
        if scoring == SearchVectorScoring::LinearSquaredL2 {
            assert_eq!(result.hits[0].id, "u-zero");
            assert_eq!(result.hits[0].score, 1.0);
            assert!(index.rabitq_projection().is_some());
        } else {
            assert!(result.hits.is_empty());
            assert!(index.rabitq_projection().is_none());
        }
    }
}

#[test]
fn empty_default_requests_reuse_an_existing_rabitq_cache() {
    let fixture = squared_l2_fixture();
    for scoring in [
        SearchVectorScoring::ProjectionMetric,
        SearchVectorScoring::LinearSquaredL2,
    ] {
        let mut index = SearchIndex::in_memory();
        index.set_vector_scoring(scoring).unwrap();
        index.set_rabitq_projection_build_options(Default::default());
        for row in &fixture.documents {
            index.upsert(fixture.document(row)).unwrap();
        }
        index
            .search_with_options(
                "",
                Some(&fixture.query),
                SearchMode::Vector,
                options(1, None),
            )
            .unwrap();
        let cached = index.rabitq_projection().unwrap();
        for options in [
            options(0, None),
            SearchQueryOptions {
                metadata_filters: BTreeMap::from([("missing".into(), "value".into())]),
                ..options(1, None)
            },
        ] {
            let result = index
                .search_with_options("", Some(&fixture.query), SearchMode::Vector, options)
                .unwrap();
            assert!(result.hits.is_empty());
            assert_eq!(
                result.retrievers[0].backend,
                "hawdb_rabitq_candidate_projection"
            );
            assert!(Arc::ptr_eq(&cached, &index.rabitq_projection().unwrap()));
        }
    }
}

#[test]
fn declared_rabitq_budget_failures_refuse_before_cache_build_or_partial_results() {
    let fixture = squared_l2_fixture();
    let mut index = SearchIndex::in_memory();
    index
        .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
        .unwrap();
    index.set_rabitq_projection_build_options(Default::default());
    for row in &fixture.documents {
        index.upsert(fixture.document(row)).unwrap();
    }
    let bounded = VectorSearchExecutionOptions::bounded(NonZeroUsize::MIN, 1, None);
    assert!(index.rabitq_projection().is_none());
    let error = index
        .try_search_with_options_context(
            "",
            Some(&fixture.query),
            SearchMode::Vector,
            options(1, None),
            bounded,
        )
        .unwrap_err();
    assert!(error.to_string().contains("budget"), "{error}");
    assert!(index.rabitq_projection().is_none());
    index
        .search_with_options(
            "",
            Some(&fixture.query),
            SearchMode::Vector,
            options(1, None),
        )
        .unwrap();
    assert!(index.rabitq_projection().is_some());
    let error = index
        .try_search_with_options_context(
            "",
            Some(&fixture.query),
            SearchMode::Vector,
            options(1, None),
            bounded,
        )
        .unwrap_err();
    assert!(error.to_string().contains("budget"), "{error}");
    let exact = index
        .search_with_options_exact(
            "",
            Some(&fixture.query),
            SearchMode::Vector,
            options(1, None),
        )
        .unwrap();
    assert_eq!(exact.hits[0].id, "z-identical");
}

#[test]
fn rabitq_both_widths_preserve_mem_scores_across_checkpoint_and_streamed_reopen() {
    let fixture = squared_l2_fixture();
    let count = fixture.documents.len();
    let query = Some(fixture.query.as_slice());
    for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let root = directory("rabitq-mem-resident");
        let mut index = SearchIndex::open(&root.0).unwrap();
        index
            .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
            .unwrap();
        index.set_rabitq_projection_build_options(RaBitQCandidateProjectionBuildOptions {
            bit_width: width,
            segment_rows: 4,
            ..Default::default()
        });
        for row in &fixture.documents {
            index.upsert(fixture.document(row)).unwrap();
        }
        let result = index
            .search_with_options("", query, SearchMode::Vector, options(count, Some(count)))
            .unwrap();
        assert_eq!(result.retrievers[0].backend, RABITQ_CANDIDATE_BACKEND);
        assert_squared_l2_hits(&fixture, &result.hits);
        assert_eq!(
            index.rabitq_projection().unwrap().manifest().metric,
            ProjectionMetric::SquaredL2
        );
        index.checkpoint().unwrap();
        let pinned = index.read_snapshot();
        drop(index);
        let reopened = SearchIndex::open(&root.0).unwrap();
        let retained = pinned
            .try_search_with_options_adaptive_vector_projection_context(
                "",
                query,
                SearchMode::Vector,
                options(count, Some(count)),
                AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Required),
                VectorSearchExecutionOptions::default(),
            )
            .unwrap();
        let current = reopened
            .try_search_with_options_compressed_vector_projection_mode(
                "",
                query,
                SearchMode::Vector,
                options(count, Some(count)),
                CompressedVectorSearchMode::Required,
            )
            .unwrap();
        for result in [retained, current] {
            assert_squared_l2_hits(&fixture, &result.hits);
            assert_eq!(result.retrievers[0].backend, RABITQ_CANDIDATE_BACKEND);
        }

        let streamed = directory("rabitq-mem-streamed");
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &streamed.0,
            SearchOutOfCoreGenerationBuildOptions {
                vector_scoring: SearchVectorScoring::LinearSquaredL2,
                vector_backend: None,
                rabitq_bit_width: width,
                rabitq_segment_rows: NonZeroUsize::new(4).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        for row in &fixture.documents {
            writer.push(fixture.document(row)).unwrap();
        }
        writer.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(&streamed.0).unwrap();
        for exact in [false, true] {
            let output = if exact {
                reader.search_with_options_exact(
                    "",
                    query,
                    SearchMode::Vector,
                    options(count, None),
                )
            } else {
                reader.search_with_options(
                    "",
                    query,
                    SearchMode::Vector,
                    options(count, Some(count)),
                )
            }
            .unwrap();
            assert_squared_l2_hits(&fixture, &output.result.hits);
            if !exact {
                assert_eq!(
                    output.result.retrievers[0].backend,
                    "hawdb_rabitq_out_of_core_candidate_projection"
                );
            }
        }
        let zero = vec![0.0; fixture.query.len()];
        let nearest_zero = reader
            .search_with_options("", Some(&zero), SearchMode::Vector, options(1, None))
            .unwrap();
        assert_eq!(nearest_zero.result.hits[0].id, "u-zero");
        assert_eq!(nearest_zero.result.hits[0].score, 1.0);

        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts: Vec::new(),
                deletes: vec!["z-identical".into()],
                max_operations: None,
                source_graph_commit_epoch: Some(1),
            },
            SearchOutOfCoreGenerationBuildOptions::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let successor = SearchOutOfCoreReader::open(&streamed.0).unwrap();
        assert_eq!(
            successor.vector_scoring(),
            SearchVectorScoring::LinearSquaredL2
        );
        let current = successor
            .search_with_options("", query, SearchMode::Vector, options(count, Some(count)))
            .unwrap();
        let mut remaining = squared_l2_fixture();
        remaining.documents.retain(|(id, _)| id != "z-identical");
        assert_squared_l2_hits(&remaining, &current.result.hits);
        let previous = reader
            .search_with_options("", query, SearchMode::Vector, options(count, Some(count)))
            .unwrap();
        assert_squared_l2_hits(&fixture, &previous.result.hits);
    }
}

#[test]
fn bound_rabitq_uses_its_selected_metric_for_candidates_and_raw_rerank() {
    let fixture = fixture();
    fixture.assert_valid();
    for width in [1, 4] {
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            let definition = VectorIndexDescriptor::new(
                "vectors.embedding".into(),
                descriptor(metric).target().clone(),
                VectorDimension::new(DIMENSION as u32).unwrap(),
                metric,
                VectorIndexBackend::RaBitQ {
                    bit_width: width,
                    segment_rows: 64,
                    transform_seed: 42,
                },
                [3; 32],
            )
            .unwrap();
            let resident_root = directory("rabitq-bound-resident");
            let mut index = SearchIndex::open(&resident_root.0).unwrap();
            index.bind_vector_index(definition.clone()).unwrap();
            for row in &fixture.documents {
                index.upsert(fixture.document(row)).unwrap();
            }
            let resident = index
                .search_with_options(
                    "",
                    Some(&fixture.query),
                    SearchMode::Vector,
                    options(TOP_K, Some(DOCUMENTS)),
                )
                .unwrap();
            assert_eq!(resident.retrievers[0].backend, RABITQ_CANDIDATE_BACKEND);
            index.checkpoint().unwrap();
            drop(index);
            let reopened = SearchIndex::open(&resident_root.0).unwrap();
            let persisted = reopened
                .search_with_options(
                    "",
                    Some(&fixture.query),
                    SearchMode::Vector,
                    options(TOP_K, Some(DOCUMENTS)),
                )
                .unwrap();
            assert_eq!(ids(&persisted.hits), ids(&resident.hits));
            assert_eq!(persisted.retrievers[0].backend, RABITQ_CANDIDATE_BACKEND);

            let root = directory("rabitq-bound");
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &root.0,
                SearchOutOfCoreGenerationBuildOptions {
                    source_graph_commit_epoch: Some(1),
                    vector_index_descriptor: Some(definition),
                    vector_backend: None,
                    rabitq_bit_width: if width == 1 {
                        RaBitQBitWidth::One
                    } else {
                        RaBitQBitWidth::Four
                    },
                    rabitq_segment_rows: NonZeroUsize::new(64).unwrap(),
                    rabitq_transform_seed: 42,
                    ..Default::default()
                },
            )
            .unwrap();
            for row in &fixture.documents {
                writer.push(fixture.document(row)).unwrap();
            }
            writer.finish().unwrap();
            let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
            let streamed = reader
                .search_with_options(
                    "",
                    Some(&fixture.query),
                    SearchMode::Vector,
                    options(TOP_K, Some(DOCUMENTS)),
                )
                .unwrap();
            for hits in [&resident.hits, &streamed.result.hits] {
                match metric {
                    VectorDistance::Cosine => assert_cosine_hits(&fixture, hits, "rabitq cosine"),
                    VectorDistance::Euclidean => assert_l2_hits(&fixture, hits, "rabitq euclidean"),
                }
            }
        }
    }
}

#[test]
#[ignore = "manual bounded-window quality and resource measurement for issue #1473"]
fn rabitq_metric_measurement() {
    use std::time::Instant;
    const DIM: usize = 1536;
    let mut random = Random(0x1473);
    let query = (0..DIM)
        .map(|_| random.unit() / (DIM as f32).sqrt())
        .collect::<Vec<_>>();
    for count in [32, 2048] {
        let rows = (0..count)
            .map(|id| {
                let norm = 0.25 + 2.0 * (random.unit() + 1.0) / 2.0;
                SearchDocument {
                    id: format!("memory:{id:08}"),
                    title: String::new(),
                    content: String::new(),
                    embedding: Some(
                        (0..DIM)
                            .map(|_| random.unit() * norm / (DIM as f32).sqrt())
                            .collect(),
                    ),
                    metadata: BTreeMap::new(),
                }
            })
            .collect::<Vec<_>>();
        for scoring in [
            SearchVectorScoring::ProjectionMetric,
            SearchVectorScoring::LinearSquaredL2,
        ] {
            for width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
                let mut index = SearchIndex::in_memory();
                index.set_vector_scoring(scoring).unwrap();
                index.set_rabitq_projection_build_options(RaBitQCandidateProjectionBuildOptions {
                    bit_width: width,
                    segment_rows: 256,
                    ..Default::default()
                });
                for row in &rows {
                    index.upsert(row.clone()).unwrap();
                }
                let exact = index
                    .search_with_options_exact(
                        "",
                        Some(&query),
                        SearchMode::Vector,
                        options(10, None),
                    )
                    .unwrap();
                let build_start = Instant::now();
                let projection = index.build_in_memory_rabitq_projection().unwrap().unwrap();
                let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
                for window in [64, 256] {
                    let controls =
                        |kernel| VectorSearchExecutionOptions::default().with_kernel(kernel);
                    let search = |kernel| {
                        index
                            .try_search_with_options_adaptive_vector_projection_context(
                                "",
                                Some(&query),
                                SearchMode::Vector,
                                options(10, Some(window)),
                                AdaptiveVectorSearchOptions::new(
                                    CompressedVectorSearchMode::Required,
                                ),
                                controls(kernel),
                            )
                            .unwrap()
                    };
                    let reference = search(VectorSearchKernelPreference::Scalar);
                    let mut milliseconds = Vec::new();
                    for _ in 0..3 {
                        let start = Instant::now();
                        let result = search(VectorSearchKernelPreference::Auto);
                        milliseconds.push(start.elapsed().as_secs_f64() * 1000.0);
                        assert_eq!(ids(&result.hits), ids(&reference.hits));
                        assert_eq!(
                            result
                                .hits
                                .iter()
                                .map(|hit| hit.score.to_bits())
                                .collect::<Vec<_>>(),
                            reference
                                .hits
                                .iter()
                                .map(|hit| hit.score.to_bits())
                                .collect::<Vec<_>>()
                        );
                    }
                    milliseconds.sort_by(f64::total_cmp);
                    let recall = reference
                        .hits
                        .iter()
                        .filter(|hit| exact.hits.iter().any(|expected| hit.id == expected.id))
                        .count() as f64
                        / exact.hits.len() as f64;
                    let report = &reference.retrievers[0];
                    assert!(report.reranked_candidate_count <= window.min(count));
                    println!(
                        "{}",
                        serde_json::json!({
                            "documents": count, "dimension": DIM, "scoring": format!("{scoring:?}"),
                            "width": width.bits(), "window": window, "build_ms": build_ms,
                            "query_median_ms": milliseconds[1], "recall_at_10": recall,
                            "reranked": report.reranked_candidate_count,
                            "raw_vector_bytes": report.raw_vector_bytes_read,
                            "candidate_payload_bytes": projection.manifest().payload_bytes,
                            "build_admission_bytes": projection.manifest().peak_build_working_bytes,
                            "retained_bytes": projection.retained_capacity_bytes(),
                        })
                    );
                }
            }
        }
    }
}
