// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Binary vector sidecars serve exact, graph, compressed, filtered and hybrid
//! queries with the stored `f32` bits, and old text-sidecar layouts fail closed.

use super::*;
use crate::SearchEmbeddingManifest;

const DIMENSION: usize = 8;

fn embedding(number: usize) -> Vec<f32> {
    let mut state = (number as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 0.6
        })
        .collect()
}

fn corpus(count: usize) -> Vec<SearchDocument> {
    (0..count)
        .map(|number| SearchDocument {
            id: format!("memory:{number:04}"),
            title: format!("graph note {number}"),
            content: if number.is_multiple_of(4) {
                "storage vectors".into()
            } else {
                "other words".into()
            },
            embedding: (number % 17 != 5).then(|| embedding(number)),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), number.to_string()),
                (
                    "space_id".to_string(),
                    if number.is_multiple_of(3) { "a" } else { "b" }.to_string(),
                ),
            ]),
        })
        .collect()
}

fn build(name: &str, documents: &[SearchDocument], backend: Option<bool>) -> PathBuf {
    build_with(name, documents, backend, None)
}

fn build_with(
    name: &str,
    documents: &[SearchDocument],
    backend: Option<bool>,
    distance: Option<hawdb_core::VectorDistance>,
) -> PathBuf {
    let path = test_dir(name);
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "binary-vector-test".into(),
                version: Some("v1".into()),
                dimension: DIMENSION,
            }),
            // Some(true): default graph; Some(false): RaBitQ; None: no projection.
            vector_backend: match backend {
                Some(true) => Some(Default::default()),
                _ => None,
            },
            source_graph_commit_epoch: distance.map(|_| 1),
            vector_index_descriptor: distance.map(|distance| {
                hawdb_core::VectorIndexDescriptor::new(
                    "vectors.embedding".into(),
                    hawdb_core::VectorIndexTarget::Relational {
                        table: "vectors".into(),
                        column: "embedding".into(),
                        column_ordinal: 1,
                    },
                    hawdb_core::VectorDimension::new(DIMENSION as u32).unwrap(),
                    distance,
                    hawdb_core::VectorIndexBackend::default(),
                    [7; 32],
                )
                .unwrap()
            }),
            ..Default::default()
        },
    )
    .unwrap();
    for document in documents {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    path
}

/// Brute-force cosine ranking over the original input vectors.
#[cfg(feature = "vector-search")]
fn reference(
    documents: &[SearchDocument],
    query: &[f32],
    keep: impl Fn(&SearchDocument) -> bool,
    limit: usize,
) -> Vec<(String, u64)> {
    reference_with(
        hawdb_core::VectorDistance::Cosine,
        documents,
        query,
        keep,
        limit,
    )
}

/// Brute-force facade relevance (positive cosine or `1 / (1 + l2)`).
#[cfg(feature = "vector-search")]
fn reference_with(
    metric: hawdb_core::VectorDistance,
    documents: &[SearchDocument],
    query: &[f32],
    keep: impl Fn(&SearchDocument) -> bool,
    limit: usize,
) -> Vec<(String, u64)> {
    let scorer = crate::VectorRelevanceQuery::new(query, metric);
    let mut scores = documents
        .iter()
        .filter(|document| keep(document))
        .filter_map(|document| {
            let score = scorer.score(document.embedding.as_deref()?)?;
            (score > 0.0).then(|| (document.id.clone(), score))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    scores
        .into_iter()
        .take(limit)
        .map(|(id, score)| (id, score.to_bits()))
        .collect()
}

#[cfg(feature = "vector-search")]
fn hits(output: &SearchOutOfCoreOutput) -> Vec<(String, u64)> {
    output
        .result
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.vector_score.to_bits()))
        .collect()
}

#[test]
fn stored_vectors_keep_input_bits_and_layout_v2() {
    let documents = corpus(300);
    let path = build("binary-vector-bits", &documents, None);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let mut stored = Vec::new();
    for artifact in &reader.segments {
        assert_eq!(artifact.layout.format, "HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V2");
        for segment in &artifact.descriptor.segments {
            let mut metrics = SearchOutOfCoreMetrics::default();
            for document in reader
                .read_vector_segment(artifact, segment, &mut metrics)
                .unwrap()
            {
                stored.push((
                    document.id,
                    document
                        .embedding
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>(),
                ));
            }
        }
    }
    let expected = documents
        .iter()
        .filter_map(|document| {
            Some((
                document.id.clone(),
                document
                    .embedding
                    .as_ref()?
                    .iter()
                    .map(|value| value.to_bits())
                    .collect::<Vec<_>>(),
            ))
        })
        .collect::<Vec<_>>();
    assert_eq!(stored, expected);
    fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn exact_and_filtered_scans_match_brute_force_bits() {
    let documents = corpus(300);
    let path = build("binary-vector-exact", &documents, None);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    for number in [1usize, 77, 150, 299] {
        let query = embedding(number + 1000);
        let exact = reader
            .search_with_options_exact("", Some(&query), SearchMode::Vector, options(10, None))
            .unwrap();
        assert_eq!(hits(&exact), reference(&documents, &query, |_| true, 10));
        let mut filtered = options(10, None);
        filtered
            .metadata_filters
            .insert("space_id".into(), "a".into());
        let filtered = reader
            .search_with_options_exact("", Some(&query), SearchMode::Vector, filtered)
            .unwrap();
        assert_eq!(
            hits(&filtered),
            reference(
                &documents,
                &query,
                |document| document.metadata["space_id"] == "a",
                10
            )
        );
    }
    fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn graph_compressed_and_hybrid_serving_match_brute_force_bits() {
    let documents = corpus(300);
    let graph = build("binary-vector-graph", &documents, Some(true));
    let rabitq = build("binary-vector-rabitq", &documents, Some(false));
    let graph_reader = SearchOutOfCoreReader::open(&graph).unwrap();
    let rabitq_reader = SearchOutOfCoreReader::open_with_config(
        &rabitq,
        SearchOutOfCoreConfig {
            max_vector_candidates: NonZeroUsize::new(300).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    for number in [3usize, 64, 200] {
        let query = embedding(number + 2000);
        let expected = reference(&documents, &query, |_| true, 10);
        // A rank window covering the corpus makes graph candidates exhaustive.
        let graph_output = graph_reader
            .search_with_options("", Some(&query), SearchMode::Vector, options(10, Some(300)))
            .unwrap();
        assert_eq!(hits(&graph_output), expected);
        let vector = graph_output
            .result
            .retrievers
            .iter()
            .find(|report| report.name == "vector")
            .unwrap();
        assert_eq!(vector.backend, "hawdb_diskann_out_of_core_candidate_graph");
        let compressed = rabitq_reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&query),
                SearchMode::Vector,
                options(10, None),
                crate::CompressedVectorSearchMode::Required,
            )
            .unwrap();
        assert_eq!(hits(&compressed), expected);

        let mut filtered = options(10, Some(300));
        filtered
            .metadata_filters
            .insert("space_id".into(), "b".into());
        let filtered = graph_reader
            .search_with_options("", Some(&query), SearchMode::Vector, filtered)
            .unwrap();
        assert_eq!(
            hits(&filtered),
            reference(
                &documents,
                &query,
                |document| document.metadata["space_id"] == "b",
                10
            )
        );

        #[cfg(feature = "full-text-search")]
        {
            let hybrid = graph_reader
                .search_with_options(
                    "storage",
                    Some(&query),
                    SearchMode::Hybrid,
                    options(20, Some(300)),
                )
                .unwrap();
            assert!(!hybrid.result.hits.is_empty());
            let scorer =
                crate::VectorRelevanceQuery::new(&query, hawdb_core::VectorDistance::Cosine);
            for hit in &hybrid.result.hits {
                let document = documents
                    .iter()
                    .find(|document| document.id == hit.id)
                    .unwrap();
                let expected = document
                    .embedding
                    .as_deref()
                    .and_then(|embedding| scorer.score(embedding))
                    .filter(|score| *score > 0.0)
                    .unwrap_or(0.0);
                assert_eq!(hit.vector_score.to_bits(), expected.to_bits(), "{}", hit.id);
            }
        }
    }
    drop((graph_reader, rabitq_reader));
    fs::remove_dir_all(graph).unwrap();
    fs::remove_dir_all(rabitq).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn euclidean_graph_and_exact_serving_match_brute_force_l2_bits() {
    let documents = corpus(300);
    let path = build_with(
        "binary-vector-l2",
        &documents,
        Some(true),
        Some(hawdb_core::VectorDistance::Euclidean),
    );
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    for number in [5usize, 120, 260] {
        let query = embedding(number + 3000);
        let expected = reference_with(
            hawdb_core::VectorDistance::Euclidean,
            &documents,
            &query,
            |_| true,
            10,
        );
        let graph = reader
            .search_with_options("", Some(&query), SearchMode::Vector, options(10, Some(300)))
            .unwrap();
        assert_eq!(hits(&graph), expected);
        let exact = reader
            .search_with_options_exact("", Some(&query), SearchMode::Vector, options(10, None))
            .unwrap();
        assert_eq!(hits(&exact), expected);
    }
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn superseded_text_vector_layouts_fail_closed_with_a_rebuild_path() {
    let documents = corpus(20);
    let path = build("binary-vector-old-layout", &documents, None);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let artifact = &reader.segments[0];
    let mut layout = artifact.layout.clone();
    layout
        .validate(
            &reader.manifest.segments[0],
            &artifact.descriptor,
            reader.config.max_compressed_segment_bytes.get(),
        )
        .unwrap();
    layout.format = "HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V1".into();
    let error = layout
        .validate(
            &reader.manifest.segments[0],
            &artifact.descriptor,
            reader.config.max_compressed_segment_bytes.get(),
        )
        .unwrap_err();
    assert!(
        matches!(&error, HawDBError::Storage(message)
            if message.contains("superseded vector sidecar format")
                && message.contains("rebuild the search projection")),
        "{error:?}"
    );
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn required_rerank_preserves_checksum_budget_and_cancellation_guards() {
    let path = test_dir("selected-vector-read-guards");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "selected-vector-test".into(),
                version: Some("v1".into()),
                dimension: 2,
            }),
            vector_backend: None,
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..8 {
        writer
            .push(SearchDocument {
                id: format!("memory:{number:03}"),
                title: "Selected vector test".into(),
                content: "Synthetic content".into(),
                embedding: Some(vec![number as f32 + 1.0, 16.0 - number as f32]),
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    writer.finish().unwrap();
    for candidate_limit in [1, 8] {
        let reader = SearchOutOfCoreReader::open_with_config(
            &path,
            SearchOutOfCoreConfig {
                max_vector_candidates: NonZeroUsize::new(candidate_limit).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let query = |execution| {
            reader.search_with_options_compressed_vector_projection_execution_options(
                "",
                Some(&[1.0, 0.5]),
                SearchMode::Vector,
                options(1, None),
                crate::CompressedVectorSearchMode::Required,
                execution,
            )
        };
        let initial = query(VectorSearchExecutionOptions::default()).unwrap();
        let report = initial
            .result
            .retrievers
            .iter()
            .find(|report| report.name == "vector")
            .unwrap();
        assert_eq!(
            report.backend,
            "hawdb_rabitq_out_of_core_candidate_projection"
        );
        assert_eq!(report.reranked_candidate_count, candidate_limit);
        assert_eq!(initial.result.hits.len(), 1);
        assert_eq!(
            initial.metrics.vector_bytes_read,
            8 * candidate_limit as u64
        );
        assert!(report.fallback_reason_codes.is_empty());

        if candidate_limit == 8 {
            let raw = reader
                .search_with_options_compressed_vector_projection_execution_options(
                    "",
                    Some(&[1.0, 0.5]),
                    SearchMode::Vector,
                    options(1, None),
                    crate::CompressedVectorSearchMode::Disabled,
                    VectorSearchExecutionOptions::default(),
                )
                .unwrap();
            assert_eq!(raw.result.hits[0].id, initial.result.hits[0].id);
            assert_eq!(
                raw.result.hits[0].vector_score.to_bits(),
                initial.result.hits[0].vector_score.to_bits()
            );
        }

        let error = query(VectorSearchExecutionOptions::bounded(
            NonZeroUsize::MIN,
            1,
            None,
        ))
        .unwrap_err();
        assert!(
            matches!(&error, HawDBError::ResourceBudgetExceeded(cause)
                if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                    && cause.scope == hawdb_core::ResourceBudgetScope::Operator
                    && cause.owner == "RaBitQ candidate scan"
                    && cause.used == 0
                    && cause.budget == 1
                    && cause.requested > cause.budget
                    && cause.budget_option
                        == "SearchOutOfCoreConfig.max_vector_search_working_bytes"),
            "{error:?}"
        );

        let cancellation = crate::RuntimeCancellationToken::new();
        let task_context = crate::RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let error = query(VectorSearchExecutionOptions::bounded(
            NonZeroUsize::MIN,
            4 * 1024 * 1024,
            Some(&task_context),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");
        let repeated = query(VectorSearchExecutionOptions::default()).unwrap();
        assert_eq!(repeated.result.hits[0].id, initial.result.hits[0].id);
        assert_eq!(
            repeated.result.hits[0].vector_score.to_bits(),
            initial.result.hits[0].vector_score.to_bits()
        );

        assert_eq!(reader.segments.len(), 1);
        let artifact = &reader.segments[0];
        assert!(artifact.descriptor.segments.len() > 1);
        let hit_id = initial.result.hits[0].id.as_str();
        let (layout, segment) = artifact
            .layout
            .segments
            .iter()
            .zip(&artifact.descriptor.segments)
            .find(|(_, segment)| {
                segment.first_document_id.as_str() <= hit_id
                    && hit_id <= segment.last_document_id.as_str()
            })
            .unwrap();
        let range = layout.vectors;
        assert!(range.offset > 0);
        let payload = path.join(&reader.manifest.segments[0].vector_payload_file);
        let original_payload = fs::read(&payload).unwrap();
        // Rows are fixed-stride 8-byte records at the end of the segment.
        let number = |id: &str| id["memory:".len()..].parse::<u64>().unwrap();
        let index = number(hit_id) - number(&segment.first_document_id);
        let row = range.offset + range.length - (range.entry_count as u64 - index) * 8;
        for (position, expected) in [
            (row, "row checksum mismatch"),
            (range.offset + 30, "vector sidecar"),
        ] {
            let mut file = OpenOptions::new().write(true).open(&payload).unwrap();
            file.seek(SeekFrom::Start(position)).unwrap();
            file.write_all(&[original_payload[usize::try_from(position).unwrap()] ^ 0x40])
                .unwrap();
            file.sync_all().unwrap();
            drop(file);
            let error = query(VectorSearchExecutionOptions::default()).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            fs::write(&payload, &original_payload).unwrap();
        }
        drop(reader);
    }
    fs::remove_dir_all(path).unwrap();
}
