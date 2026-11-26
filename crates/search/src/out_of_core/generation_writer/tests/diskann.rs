// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{SearchOutOfCoreOutput, SearchOutOfCoreReader};
use hawdb_core::{VectorIndexBackend, VectorIndexLsg};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn row(number: usize, embedding: Option<Vec<f32>>) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: String::new(),
        body: String::new(),
        embedding,
        source_id: None,
        metadata: BTreeMap::from([(
            "group".into(),
            if number.is_multiple_of(2) {
                "even"
            } else {
                "odd"
            }
            .into(),
        )]),
    }
}

fn query(reader: &SearchOutOfCoreReader, exact: bool) -> SearchOutOfCoreOutput {
    let options = SearchQueryOptions {
        limit: 8,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::from([("group".into(), "even".into())]),
        policy_epoch: None,
    };
    if exact {
        reader.search_with_options_exact("", Some(&[1.0, 0.0]), SearchMode::Vector, options)
    } else {
        reader.search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options)
    }
    .unwrap()
}

fn ids(output: &SearchOutOfCoreOutput) -> Vec<&str> {
    output
        .result
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect()
}

fn assert_default_graph(reader: &SearchOutOfCoreReader) {
    let actual = query(reader, false);
    let exact = query(reader, true);
    assert_eq!(ids(&actual), ids(&exact));
    let report = actual
        .result
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .unwrap();
    assert_eq!(report.backend, "hawdb_diskann_out_of_core_candidate_graph");
    assert_eq!(report.candidate_score_source, "graph_distance");
    let exact_report = exact
        .result
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .unwrap();
    assert_eq!(exact_report.backend, "scalar_vector_segment_scan");
}

#[test]
fn default_graph_qualification_binds_all_segments_and_visible_vectors() {
    let root = Directory(test_dir("diskann_qualification_closure"));
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root.0,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: std::num::NonZeroUsize::new(2).unwrap(),
            source_graph_commit_epoch: Some(11),
            embedding_manifest: Some(crate::SearchEmbeddingManifest {
                model: "qualification-fixture".into(),
                version: Some("1".into()),
                dimension: 2,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..7 {
        writer
            .push(
                row(
                    number,
                    (number >= 2 && number != 4).then(|| vec![1.0, number as f32 / 10.0]),
                )
                .into_document(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    let mut reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let pinned = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert!(reader.manifest.segments.len() > 1);
    assert!(reader.primary_segment().rabitq_projection.is_none());
    let original = reader.vector_projection_qualification_identity().unwrap();
    assert_eq!(original.document_count, 4);
    assert_eq!(original.projection_generation, reader.generation());
    assert_eq!(original.source_graph_commit_epoch, Some(11));
    let mut previous = original.clone();
    for (epoch, upserts, deletes, visible, stored, reused) in [
        (
            12,
            vec![
                row(2, None),
                row(4, Some(vec![1.0, 0.4])),
                row(7, Some(vec![1.0, 0.7])),
            ],
            vec![
                row(1, None).into_document().id,
                row(3, None).into_document().id,
            ],
            4,
            6,
            0,
        ),
        (
            13,
            vec![row(5, None), row(7, Some(vec![1.0, 0.1]))],
            vec![row(0, None).into_document().id],
            3,
            7,
            4,
        ),
        (14, vec![], vec![row(7, None).into_document().id], 2, 7, 7),
        (15, vec![row(4, None)], vec![], 1, 7, 8),
        (16, vec![], vec![row(6, None).into_document().id], 0, 7, 9),
    ] {
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts,
                deletes,
                source_graph_commit_epoch: Some(epoch),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let refresh = reader.refresh().unwrap();
        assert_eq!(refresh.reused_retractions, reused);
        let identity = reader.vector_projection_qualification_identity().unwrap();
        assert_eq!(identity.algorithm, "diskann");
        assert_eq!(identity.document_count, visible);
        assert_eq!(identity.projection_generation, reader.generation());
        assert_eq!(identity.source_graph_commit_epoch, Some(epoch));
        assert_ne!(identity.source_digest, previous.source_digest);
        assert_ne!(identity.payload_checksum, previous.payload_checksum);
        let resources = reader.vector_projection_resource_evidence().unwrap();
        assert_eq!(resources.raw_vector_bytes, stored * 2 * 4);
        assert_eq!(
            resources.segment_count,
            reader
                .segments
                .iter()
                .filter(|segment| segment.rabitq_projection.is_some())
                .count()
        );
        let bytes = reader
            .manifest
            .segments
            .iter()
            .filter_map(|segment| segment.rabitq_artifact_len)
            .sum::<u64>()
            + reader
                .manifest
                .mutation_runs
                .iter()
                .map(|run| run.len)
                .sum::<u64>();
        assert_eq!(identity.payload_bytes, bytes);
        assert_eq!(resources.projection_payload_bytes, bytes);
        let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_eq!(
            reopened.vector_projection_qualification_identity().unwrap(),
            identity
        );
        assert_eq!(
            reopened.vector_projection_resource_evidence().unwrap(),
            resources
        );
        assert_eq!(
            pinned.vector_projection_qualification_identity().unwrap(),
            original
        );
        if visible != 0 {
            assert_default_graph(&reader);
        } else {
            assert!(query(&reader, false).result.hits.is_empty());
            assert!(query(&reader, true).result.hits.is_empty());
        }
        previous = identity;
    }
}

#[test]
fn default_graph_uses_the_requested_candidate_window_within_its_ceiling() {
    let root = Directory(test_dir("default_diskann_candidate_window"));
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    for number in 0..512 {
        writer
            .push(row(number, Some(vec![1.0, number as f32 / 1024.0])).into_document())
            .unwrap();
    }
    writer.finish().unwrap();
    // Cover the graph to keep navigation independent of the rerank window,
    // regardless of the configured default chosen for ordinary queries.
    let reader = SearchOutOfCoreReader::open_with_config(
        &root.0,
        crate::SearchOutOfCoreConfig {
            default_navigation_beam: NonZeroUsize::new(512).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let options = |rank_window| SearchQueryOptions {
        limit: 10,
        offset: 3,
        rank_window,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    for (window, expected) in [(None, 128), (Some(256), 256)] {
        let output = reader
            .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options(window))
            .unwrap();
        let report = output
            .result
            .retrievers
            .iter()
            .find(|row| row.name == "vector")
            .unwrap();
        assert_eq!(report.backend, "hawdb_diskann_out_of_core_candidate_graph");
        assert_eq!(report.reranked_candidate_count, expected);
        assert_eq!(report.generated_candidate_count, expected);
        // Navigation can cover the graph while raw reranking honors its window.
        assert_eq!(report.candidate_scan_scored_document_count, 512);
        assert_eq!(report.raw_vector_bytes_read, expected as u64 * 8);
        assert_eq!(output.result.hits.len(), 10);
    }
    let exact = reader
        .search_with_options_exact("", Some(&[1.0, 0.0]), SearchMode::Vector, options(None))
        .unwrap();
    let report = exact
        .result
        .retrievers
        .iter()
        .find(|row| row.name == "vector")
        .unwrap();
    assert_eq!(report.backend, "scalar_vector_segment_scan");
    assert_eq!(report.raw_vector_bytes_read, 512 * 8);
    let constrained = SearchOutOfCoreReader::open_with_config(
        &root.0,
        crate::SearchOutOfCoreConfig {
            max_vector_candidates: std::num::NonZeroUsize::new(64).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let output = constrained
        .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options(None))
        .unwrap();
    assert_eq!(output.result.retrievers[0].reranked_candidate_count, 64);
    assert!(constrained
        .search_with_options(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(Some(128))
        )
        .is_err());
}

#[test]
fn default_graph_crud_reuses_content_and_preserves_pinned_readers() {
    for lsg in [false, true] {
        let root = Directory(test_dir("default_diskann_crud"));
        let backend = VectorIndexBackend::DiskAnn {
            max_degree: 8,
            build_beam: 16,
            alpha_milli: 1400,
            lsg: lsg.then_some(VectorIndexLsg {
                sample_size: 16,
                neighbors: 3,
                exponent_milli: 700,
            }),
        };
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &root.0,
            SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(1),
                vector_backend: Some(backend),
                ..Default::default()
            },
        )
        .unwrap();
        for number in 0..32 {
            writer
                .push(row(number, Some(vec![1.0, number as f32 / 10.0])).into_document())
                .unwrap();
        }
        writer.finish().unwrap();
        let pinned = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_default_graph(&pinned);
        let before = query(&pinned, false);
        let old_segment = pinned.manifest.segments[0].clone();
        let graph_path = root
            .0
            .join(old_segment.rabitq_artifact_file.as_ref().unwrap());
        let old_graph = fs::read(&graph_path).unwrap();
        #[cfg(unix)]
        let old_inode = {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(&graph_path).unwrap().ino()
        };
        let changed = SearchProjectionDelta {
            upserts: vec![
                row(0, Some(vec![0.1, 1.0])),
                row(2, None),
                row(32, Some(vec![1.0, 0.01])),
            ],
            deletes: vec![row(1, None).into_document().id],
            source_graph_commit_epoch: Some(2),
            ..Default::default()
        };
        let (_, report, source_reads) =
            SearchOutOfCoreGenerationWriter::prepare_delta(&pinned, changed, Default::default())
                .unwrap()
                .finish()
                .unwrap();
        assert_eq!(report.published_content_segments, 1);
        assert!(source_reads.streamed_documents <= 3);
        let current = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_default_graph(&current);
        assert_eq!(current.source_graph_commit_epoch(), Some(2));
        assert_eq!(current.manifest.segments.len(), 2);
        assert_eq!(current.manifest.segments[0], old_segment);
        assert_eq!(fs::read(&graph_path).unwrap(), old_graph);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(fs::metadata(&graph_path).unwrap().ino(), old_inode);
        }
        assert_eq!(
            current.segments[1]
                .rabitq_projection
                .as_ref()
                .unwrap()
                .manifest()
                .document_count,
            2
        );
        for artifact in &current.segments {
            let inherited = artifact
                .rabitq_projection
                .as_ref()
                .unwrap()
                .backend()
                .unwrap();
            assert_eq!(inherited, backend);
        }
        assert_eq!(ids(&query(&pinned, false)), ids(&before));
        assert_ne!(ids(&query(&current, false)), ids(&before));

        // A fully built but abandoned candidate cannot advance the selector.
        let candidate = SearchOutOfCoreGenerationWriter::prepare_delta(
            &current,
            SearchProjectionDelta {
                upserts: vec![row(34, Some(vec![1.0, 0.0]))],
                source_graph_commit_epoch: Some(3),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .prepare_publication()
        .unwrap();
        drop(candidate);
        let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_eq!(reopened.generation(), current.generation());
        assert_eq!(ids(&query(&reopened, false)), ids(&query(&current, false)));
    }
}

#[test]
fn default_graph_survives_empty_layers_and_deletion_only_successors() {
    let root = Directory(test_dir("default_diskann_empty_crud"));
    let writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer.finish().unwrap();
    for present in [true, false, true] {
        let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
        let document = row(0, Some(vec![1.0, 0.0]));
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts: if present {
                    vec![document.clone()]
                } else {
                    vec![]
                },
                deletes: if present {
                    vec![]
                } else {
                    vec![document.into_document().id]
                },
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let current = SearchOutOfCoreReader::open(&root.0).unwrap();
        let actual = query(&current, false);
        assert_eq!(actual.result.hits.len(), usize::from(present));
        assert_eq!(ids(&actual), ids(&query(&current, true)));
    }
}

#[test]
fn raw_graph_does_not_satisfy_explicit_compression_requirement() {
    let root = Directory(test_dir("default_diskann_compression"));
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer
        .push(row(0, Some(vec![1.0, 0.0])).into_document())
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert!(reader
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 8,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap_err()
        .to_string()
        .contains("required but unavailable"));
}

#[test]
fn graph_delta_refusal_keeps_the_old_complete_generation() {
    let root = Directory(test_dir("default_diskann_refusal"));
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer
        .push(row(0, Some(vec![1.0, 0.0])).into_document())
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = published_files(&root.0);
    for cancel in [false, true] {
        let task = RuntimeTaskContext::default();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            SearchProjectionDelta {
                upserts: vec![row(2, Some(vec![1.0, 0.1]))],
                ..Default::default()
            },
            SearchOutOfCoreGenerationBuildOptions {
                rabitq_build_memory_bytes: if cancel {
                    NonZeroUsize::new(64 << 20).unwrap()
                } else {
                    NonZeroUsize::MIN
                },
                ..Default::default()
            },
            task.clone(),
        )
        .unwrap();
        if cancel {
            task.cancellation().cancel();
        }
        let error = update.finish().unwrap_err();
        if cancel {
            assert!(error.to_string().contains("cancel"), "{error}");
        } else {
            assert!(
                matches!(error, HawDBError::ResourceBudgetExceeded(_)),
                "{error}"
            );
        }
        assert_eq!(published_files(&root.0), before);
        assert_eq!(stage_directories(&root.0), 0);
        let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_eq!(reopened.generation(), reader.generation());
        assert_eq!(ids(&query(&reopened, false)), ids(&query(&reader, false)));
    }
}

#[test]
fn explicit_rabitq_backend_and_nondefault_parameters_survive_default_delta() {
    let root = Directory(test_dir("explicit_rabitq_delta"));
    let backend = VectorIndexBackend::RaBitQ {
        bit_width: 1,
        segment_rows: 3,
        transform_seed: 17,
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root.0,
        SearchOutOfCoreGenerationBuildOptions {
            vector_backend: Some(backend),
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .push(row(0, Some(vec![1.0, 0.0])).into_document())
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row(2, Some(vec![1.0, 0.1]))],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.segments.len(), 2);
    for artifact in &reader.segments {
        assert_eq!(
            artifact
                .rabitq_projection
                .as_ref()
                .unwrap()
                .backend()
                .unwrap(),
            backend
        );
    }
    let actual = reader
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 8,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap();
    assert_eq!(actual.result.hits.len(), 2);
    assert_eq!(
        actual.result.retrievers[0].backend,
        "hawdb_rabitq_out_of_core_candidate_projection"
    );
}

#[test]
fn ordinary_zero_vectors_preserve_absent_cosine_scores_across_delta_and_reopen() {
    let root = Directory(test_dir("default_diskann_zero_cosine"));
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, Default::default()).unwrap();
    writer
        .push(row(0, Some(vec![1.0, 0.0])).into_document())
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let zero_query = reader
        .search_with_options(
            "",
            Some(&[0.0, -0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 8,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    assert!(zero_query.result.hits.is_empty());
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row(2, Some(vec![0.0, -0.0])), row(4, Some(vec![1.0, 0.1]))],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let actual = query(&reader, false);
    let exact = query(&reader, true);
    assert_eq!(ids(&actual), ids(&exact));
    assert_eq!(actual.result.hits.len(), 2);
    assert_eq!(
        actual.result.retrievers[0].backend,
        "scalar_vector_segment_scan"
    );
    assert_eq!(
        reader.segments[0]
            .rabitq_projection
            .as_ref()
            .unwrap()
            .manifest()
            .algorithm,
        "diskann"
    );
    assert!(!reader.segments[1]
        .rabitq_projection
        .as_ref()
        .unwrap()
        .is_diskann());
    assert!(reader.vector_projection_qualification_identity().is_none());
    assert!(reader.vector_projection_resource_evidence().is_none());
}

/// The result bound shared by segment graphs (#1918) must come from visible
/// results only. Here the ten nearest vectors of the base graphs are old
/// versions: the delta supersedes or deletes them, and the visible neighbors
/// lie in the delta graph, which is above the small-domain scan limit.
#[test]
fn graph_result_bound_ignores_superseded_and_deleted_versions() {
    const DIMENSION: usize = 8;
    const ROWS: usize = 2950;
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut noise = move || -> f32 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    };
    let query: Vec<f32> = (0..DIMENSION)
        .map(|position| (position as f32 + 1.0) / 8.0)
        .collect();
    let mut near =
        |scale: f32| -> Vec<f32> { query.iter().map(|value| value + scale * noise()).collect() };
    let random = |near: &mut dyn FnMut(f32) -> Vec<f32>| -> Vec<f32> {
        near(4.0).into_iter().map(|value| value - 0.5).collect()
    };
    let query_options = || SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    for supersede in [true, false] {
        let root = Directory(test_dir("diskann_result_bound_visibility"));
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &root.0,
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: std::num::NonZeroUsize::new(ROWS).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut visible_base = Vec::new();
        for number in 0..2 * ROWS {
            // Old versions almost equal to the query.
            let embedding = if number < 10 {
                near(0.001)
            } else {
                random(&mut near)
            };
            if number >= 10 {
                visible_base.push(cosine(&query, &embedding));
            }
            writer
                .push(row(number, Some(embedding)).into_document())
                .unwrap();
        }
        visible_base.sort_by(f64::total_cmp);
        writer.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
        let mut upserts: Vec<_> = (2 * ROWS..3 * ROWS - 10)
            .map(|number| {
                // The visible neighbors, all farther than the old versions.
                let embedding = if number < 2 * ROWS + 40 {
                    near(0.05)
                } else {
                    random(&mut near)
                };
                row(number, Some(embedding))
            })
            .collect();
        let mut deletes = Vec::new();
        for number in 0..10 {
            if supersede {
                upserts.push(row(number, Some(near(0.08))));
            } else {
                deletes.push(row(number, None).into_document().id);
            }
        }
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts,
                deletes,
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let current = SearchOutOfCoreReader::open(&root.0).unwrap();
        assert_eq!(current.segments.len(), 3);
        crate::out_of_core::vector_serving::RESULT_BOUNDS.with_borrow_mut(Vec::clear);
        let actual = current
            .search_with_options("", Some(&query), SearchMode::Vector, query_options())
            .unwrap();
        let bounds = crate::out_of_core::vector_serving::RESULT_BOUNDS.take();
        assert_eq!(
            bounds.iter().map(|bound| bound.0).collect::<Vec<_>>(),
            [0, 1, 2]
        );
        // The base graphs fill the result, so the delta graph is bounded, by
        // visible base vectors only. The hidden versions are about 1e-6 away.
        assert_eq!(bounds[0].1, None);
        let bound = bounds[2]
            .1
            .expect("the base graphs hold ten visible results");
        assert!(
            bound >= visible_base[9] - 1e-9,
            "bound {bound} is below the tenth visible base distance {}",
            visible_base[9]
        );
        let exact = current
            .search_with_options_exact("", Some(&query), SearchMode::Vector, query_options())
            .unwrap();
        assert_eq!(ids(&actual), ids(&exact), "supersede={supersede}");
    }
}

fn cosine(left: &[f32], right: &[f32]) -> f64 {
    let dot: f64 = left
        .iter()
        .zip(right)
        .map(|(a, b)| f64::from(*a) * f64::from(*b))
        .sum();
    let norm = |values: &[f32]| {
        values
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>()
            .sqrt()
    };
    1.0 - dot / (norm(left) * norm(right))
}
