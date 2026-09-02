use super::super::OUT_OF_CORE_MANIFEST_FILE;
use super::*;
use crate::{
    CompressedVectorSearchMode, SearchMode, SearchProjectionDelta, SearchProjectionKind,
    SearchProjectionRow, SearchQueryOptions, SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
};
use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn streaming_generation_publishes_reopenable_zero_residency_projection() {
    let root = test_dir("streaming_generation");
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(17),
        embedding_manifest: Some(SearchEmbeddingManifest {
            model: "test-model".to_string(),
            version: Some("v1".to_string()),
            dimension: 2,
        }),
        lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
        #[cfg(feature = "vector-search")]
        rabitq_bit_width: skein_vector_projection::RaBitQBitWidth::One,
        ..SearchOutOfCoreGenerationBuildOptions::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
    for number in 0..300 {
        writer.push(document(number)).unwrap();
    }
    let report = writer.finish().unwrap();
    assert_eq!(report.document_count, 300);
    assert_eq!(report.vector_document_count, 300);
    assert!(report.rabitq_artifact_bytes > 0);
    assert!(report.rabitq_source_digest.is_some());
    assert_eq!(report.resident_document_count, 0);
    assert!(report.active_manifest_published_last);
    assert!(!report.cleanup_retry_required);
    assert!(report.peak_segment_document_count <= SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
    assert!(!root.join(crate::SEARCH_SNAPSHOT_FILE).exists());

    let reader = super::super::SearchOutOfCoreReader::open_with_config(
        &root,
        super::super::SearchOutOfCoreConfig {
            max_vector_candidates: NonZeroUsize::new(16).unwrap(),
            ..super::super::SearchOutOfCoreConfig::default()
        },
    )
    .unwrap();
    assert_eq!(reader.document_count(), 300);
    assert_eq!(reader.resident_document_count(), 0);
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(reader.source_graph_commit_epoch(), Some(17));
    #[cfg(feature = "vector-search")]
    assert_eq!(
        reader
            .vector_projection_qualification_identity()
            .expect("vector projection is present")
            .bit_width,
        1
    );
    let output = reader
        .search_with_options(
            "graph storage",
            Some(&[1.0, 0.5]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    assert_eq!(output.result.hits.len(), 5);
    assert!(output.metrics.hydrated_documents <= 5);
    let compressed = reader
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.5]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap();
    assert_eq!(
        compressed.result.retrievers[0].backend,
        "skein_rabitq_out_of_core_candidate_projection"
    );
    assert!(compressed.metrics.rabitq_payload_bytes_read > 0);
    assert!(compressed.result.retrievers[0].reranked_candidate_count <= 16);
    assert_eq!(
        compressed.result.retrievers[0].final_score_source,
        "raw_vector"
    );
    let filtered = reader
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.5]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::from([("group".to_string(), "even".to_string())]),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap();
    assert_eq!(filtered.result.filtered_document_count, 150);
    assert!(filtered.result.hits.iter().all(|hit| {
        hit.id
            .strip_prefix("memory:")
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|number| number % 2 == 0)
    }));
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task_context = RuntimeTaskContext::without_deadline(cancellation);
    let preferred_error = reader
        .search_with_options_compressed_vector_projection_context(
            "",
            Some(&[1.0, 0.5]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Preferred,
            &task_context,
        )
        .unwrap_err();
    assert!(preferred_error.to_string().contains("cancelled"));
    let scalar_error = reader
        .search_with_options_compressed_vector_projection_context(
            "",
            Some(&[1.0, 0.5]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 5,
                offset: 0,
                rank_window: Some(16),
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Disabled,
            &task_context,
        )
        .unwrap_err();
    assert!(scalar_error.to_string().contains("cancelled"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_delta_merge_publishes_without_full_document_residency() {
    let root = test_dir("bounded_delta_merge");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(17),
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "test-model".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            }),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    for number in 0..300 {
        writer.push(document(number)).unwrap();
    }
    writer.finish().unwrap();

    let old_reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &old_reader,
        SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "added".to_string(),
                title: "Added document".to_string(),
                body: "bounded delta generation".to_string(),
                embedding: Some(vec![1.0, 0.5]),
                source_id: None,
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            }],
            deletes: vec!["memory:000001".to_string()],
            max_operations: Some(2),
            source_graph_commit_epoch: Some(18),
        },
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().after_document_count, 300);
    assert!(update.source_read_metrics().peak_segment_document_bytes > 0);
    let (_, build, _) = update.finish().unwrap();
    assert_eq!(build.resident_document_count, 0);
    assert_eq!(build.generation, old_reader.generation() + 1);

    let new_reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(new_reader.source_graph_commit_epoch(), Some(18));
    assert!(new_reader
        .hydrate_documents(&["memory:added".to_string()])
        .is_ok());
    assert!(new_reader
        .hydrate_documents(&["memory:000001".to_string()])
        .is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_delta_admission_rejects_before_staging_and_preserves_generation() {
    let root = test_dir("bounded_delta_admission");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    writer.push(document(0)).unwrap();
    writer.push(document(1)).unwrap();
    let initial = writer.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();

    let error = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "added".to_string(),
                title: "Added document".to_string(),
                body: "bounded delta admission".to_string(),
                embedding: Some(vec![1.0, 0.5]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: vec!["memory:000001".to_string()],
            max_operations: None,
            source_graph_commit_epoch: None,
        },
        SearchOutOfCoreGenerationBuildOptions {
            max_delta_operations: NonZeroUsize::MIN,
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap_err();

    assert!(error.to_string().contains("generation admission"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        initial.generation
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_delta_rejects_stale_base_generation_without_lost_update() {
    let root = test_dir("bounded_delta_stale_base");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    initial.push(document(1)).unwrap();
    initial.finish().unwrap();
    let stale_reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
    let stale_update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &stale_reader,
        SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "stale".to_string(),
                title: "Stale update".to_string(),
                body: "must not overwrite a newer generation".to_string(),
                embedding: Some(vec![1.0, 0.5]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: Vec::new(),
            max_operations: Some(1),
            source_graph_commit_epoch: None,
        },
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    replacement.push(document(10)).unwrap();
    let replacement = replacement.finish().unwrap();
    let manifest_after_replacement = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let error = stale_update.finish().unwrap_err();
    assert!(error.to_string().contains("base changed"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_after_replacement
    );
    let active = super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(active.generation(), replacement.generation);
    assert!(active
        .hydrate_documents(&["memory:000010".to_string()])
        .is_ok());
    assert!(active
        .hydrate_documents(&["memory:stale".to_string()])
        .is_err());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn rabitq_open_rejects_corruption_and_insufficient_serving_memory() {
    let root = test_dir("rabitq_open_admission");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "test-model".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            }),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    for number in 0..32 {
        writer.push(document(number)).unwrap();
    }
    let report = writer.finish().unwrap();

    let admission_error = super::super::SearchOutOfCoreReader::open_with_config(
        &root,
        super::super::SearchOutOfCoreConfig {
            max_vector_search_working_bytes: NonZeroUsize::MIN,
            ..super::super::SearchOutOfCoreConfig::default()
        },
    )
    .unwrap_err();
    assert!(admission_error.to_string().contains("serving admission"));

    let artifact = root.join(format!("search_rabitq.{}.skein", report.generation));
    let mut bytes = fs::read(&artifact).unwrap();
    *bytes.last_mut().unwrap() ^= 0xff;
    fs::write(&artifact, bytes).unwrap();
    let corruption_error = super::super::SearchOutOfCoreReader::open(&root).unwrap_err();
    assert!(corruption_error
        .to_string()
        .contains("does not match its manifest"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_rejects_unordered_input_without_publication() {
    let root = test_dir("unordered_generation");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    writer.push(document(2)).unwrap();
    let error = writer.push(document(1)).unwrap_err();
    assert!(error.to_string().contains("strictly increasing"));
    assert!(writer
        .finish()
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_limit_failure_cleans_stage_and_preserves_active_generation() {
    let root = test_dir("generation_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_documents: NonZeroUsize::new(1).unwrap(),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .push(document(2))
        .unwrap_err()
        .to_string()
        .contains("admitted 1 documents"));
    drop(replacement);

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    let rejected_generation = first.generation + 1;
    assert!(!root
        .join(format!(
            "search_projection_segments.{rejected_generation}.skein"
        ))
        .exists());
    assert!(!root
        .join(format!("search_lexical.{rejected_generation}.skein"))
        .exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn finalize_admission_failure_preserves_active_manifest_and_cleans_stage() {
    let root = test_dir("generation_finalize_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_descriptor_working_bytes: NonZeroU64::MIN,
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .finish()
        .unwrap_err()
        .to_string()
        .contains("descriptor working set"));

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_size_admission_preserves_active_manifest() {
    let root = test_dir("generation_publication_limit");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let manifest_before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_generation_bytes: NonZeroU64::MIN,
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    assert!(replacement
        .finish()
        .unwrap_err()
        .to_string()
        .contains("published bytes"));

    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest_before
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        first.generation
    );
    let rejected_generation = first.generation + 1;
    assert!(!root
        .join(format!(
            "search_projection_segments.{rejected_generation}.skein"
        ))
        .exists());
    assert!(!root
        .join(format!("search_lexical.{rejected_generation}.skein"))
        .exists());
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_replaces_orphaned_next_generation_artifacts() {
    let root = test_dir("generation_orphan_replacement");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();

    let next_generation = first.generation + 1;
    let descriptor_path = root.join(format!(
        "search_projection_segments.{next_generation}.skein"
    ));
    let lexical_path = root.join(format!("search_lexical.{next_generation}.skein"));
    fs::write(&descriptor_path, b"orphaned descriptor").unwrap();
    fs::write(&lexical_path, b"orphaned lexical artifact").unwrap();

    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    replacement.push(document(1)).unwrap();
    let second = replacement.finish().unwrap();

    assert_eq!(second.generation, next_generation);
    assert_ne!(fs::read(descriptor_path).unwrap(), b"orphaned descriptor");
    assert_ne!(
        fs::read(lexical_path).unwrap(),
        b"orphaned lexical artifact"
    );
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        second.generation
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn streaming_generation_recovers_generation_after_active_manifest_corruption() {
    let root = test_dir("generation_manifest_recovery");
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();

    let mut second = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    second.push(document(1)).unwrap();
    let second = second.finish().unwrap();
    assert_eq!(second.generation, first.generation + 1);

    fs::write(root.join(OUT_OF_CORE_MANIFEST_FILE), b"invalid manifest").unwrap();
    let mut replacement = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    replacement.push(document(2)).unwrap();
    let replacement = replacement.finish().unwrap();

    assert_eq!(replacement.generation, second.generation + 1);
    assert_eq!(
        super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        replacement.generation
    );
    fs::remove_dir_all(root).unwrap();
}

fn document(number: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{number:06}"),
        title: format!("Graph storage {number}"),
        content: "Graph storage keeps bounded search generations".repeat(4),
        embedding: Some(vec![1.0, number as f32 / 300.0]),
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
            (
                "group".to_string(),
                if number.is_multiple_of(2) {
                    "even"
                } else {
                    "odd"
                }
                .to_string(),
            ),
        ]),
    }
}

fn stage_directories(root: &Path) -> usize {
    fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".search-generation.")
        })
        .count()
}

fn test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "skein_search_{name}_{}_{}",
        std::process::id(),
        nanos
    ))
}
