use super::super::OUT_OF_CORE_MANIFEST_FILE;
use super::*;
use crate::checksum_bytes;
#[cfg(feature = "vector-search")]
use crate::{CompressedVectorSearchMode, SearchMode, SearchQueryOptions};
use crate::{
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
    SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
};
#[cfg(feature = "vector-search")]
use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

mod manifest_budget;
mod spool_decoding;
mod spool_encoding;
mod term_policy;

#[test]
fn segment_admission_sizes_documents_without_encoding_them() {
    use crate::document_encoding::ENCODING_ATTEMPTS;

    let root = test_dir("segment_preallocation_admission");
    fs::create_dir(&root).unwrap();
    let fields = required_descriptor_fields();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder = SegmentArtifactBuilder::new(&root, 1, &fields, &options).unwrap();
    let attempts = ENCODING_ATTEMPTS.get();
    builder.push(document(0)).unwrap();
    assert_eq!(ENCODING_ATTEMPTS.get(), attempts);
    drop(builder);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn record_admission_counts_reused_metadata_fields_once() {
    let root = test_dir("record_metadata_reuse");
    let mut fields = required_descriptor_fields();
    fields.extend(document(0).metadata.into_keys());
    let field_bytes = fields.iter().map(|field| field.len() as u64).sum();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_metadata_fields: NonZeroUsize::new(fields.len()).unwrap(),
            max_metadata_field_bytes: NonZeroU64::new(field_bytes).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    writer.push(document(0)).unwrap();
    writer.push(document(1)).unwrap();
    assert_eq!(writer.document_count, 2);
    assert_eq!(writer.metadata_fields, fields);
    assert_eq!(writer.metadata_field_bytes, field_bytes);
    drop(writer);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn large_record_is_rejected_without_materializing_its_hex_copy() {
    use crate::document_encoding::ENCODING_ATTEMPTS;

    let root = test_dir("large_record_preallocation");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    let mut source = document(0);
    source.content = " ".repeat(8 * 1024 * 1024);
    let attempts = ENCODING_ATTEMPTS.get();
    assert!(writer
        .push(source)
        .unwrap_err()
        .to_string()
        .contains("encoded bytes"));
    assert_eq!(ENCODING_ATTEMPTS.get(), attempts);
    assert_eq!(writer.spool_bytes, SPOOL_HEADER.len() as u64);
    drop(writer);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn record_admission_checks_all_byte_limits_before_encoding() {
    use crate::document_encoding::{ENCODING_ATTEMPTS, STREAMING_ATTEMPTS};

    let source = document(0);
    let record = crate::encode_search_document_line(&source);
    let bytes = record.len() as u64;
    for (limit, expected_error) in [
        (0, "encoded bytes"),
        (1, "logical bytes"),
        (2, "spool requires"),
        (3, "metadata fields require"),
        (4, "metadata fields require"),
    ] {
        let root = test_dir("record_preallocation_admission");
        let mut options = SearchOutOfCoreGenerationBuildOptions::default();
        match limit {
            0 => options.max_record_bytes = NonZeroU64::new(bytes - 1).unwrap(),
            1 => options.max_logical_document_bytes = NonZeroU64::new(bytes - 1).unwrap(),
            2 => {
                options.max_spool_bytes = NonZeroU64::new(
                    SPOOL_HEADER.len() as u64 + SPOOL_FRAME_HEADER_BYTES + bytes - 1,
                )
                .unwrap()
            }
            3 => {
                options.max_metadata_fields =
                    NonZeroUsize::new(required_descriptor_fields().len()).unwrap()
            }
            4 => {
                options.max_metadata_field_bytes = NonZeroU64::new(
                    required_descriptor_fields()
                        .iter()
                        .map(|field| field.len() as u64)
                        .sum(),
                )
                .unwrap()
            }
            _ => unreachable!(),
        }
        let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
        let attempts = ENCODING_ATTEMPTS.get();
        let streamed = STREAMING_ATTEMPTS.get();
        assert!(writer
            .push(source.clone())
            .unwrap_err()
            .to_string()
            .contains(expected_error));
        assert_eq!(
            ENCODING_ATTEMPTS.get(),
            attempts,
            "limit {limit} encoded a rejected record"
        );
        assert_eq!(STREAMING_ATTEMPTS.get(), streamed);
        assert_eq!(writer.document_count, 0);
        assert_eq!(writer.logical_document_bytes, 0);
        writer.spool.as_mut().unwrap().flush().unwrap();
        assert_eq!(fs::read(&writer.spool_path).unwrap(), SPOOL_HEADER);
        assert!(writer
            .finish()
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn record_admission_accepts_exact_limits_and_rejects_cumulative_overflow() {
    use crate::document_encoding::{ENCODING_ATTEMPTS, STREAMING_ATTEMPTS};

    let source = document(0);
    let record = crate::encode_search_document_line(&source);
    let bytes = record.len() as u64;
    let mut fields = required_descriptor_fields();
    fields.extend(source.metadata.keys().cloned());
    for limited_spool in [false, true] {
        let root = test_dir("record_exact_admission");
        let mut options = SearchOutOfCoreGenerationBuildOptions {
            max_record_bytes: NonZeroU64::new(bytes).unwrap(),
            max_metadata_fields: NonZeroUsize::new(fields.len()).unwrap(),
            max_metadata_field_bytes: NonZeroU64::new(
                fields.iter().map(|field| field.len() as u64).sum(),
            )
            .unwrap(),
            ..Default::default()
        };
        if limited_spool {
            options.max_spool_bytes =
                NonZeroU64::new(SPOOL_HEADER.len() as u64 + SPOOL_FRAME_HEADER_BYTES + bytes)
                    .unwrap();
        } else {
            options.max_logical_document_bytes = NonZeroU64::new(bytes).unwrap();
        }
        let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
        let attempts = ENCODING_ATTEMPTS.get();
        let streamed = STREAMING_ATTEMPTS.get();
        writer.push(source.clone()).unwrap();
        assert_eq!(ENCODING_ATTEMPTS.get(), attempts);
        assert_eq!(STREAMING_ATTEMPTS.get(), streamed + 2);
        assert_eq!(writer.metadata_fields, fields);
        writer.spool.as_mut().unwrap().flush().unwrap();
        let mut expected_spool = SPOOL_HEADER.to_vec();
        expected_spool.extend(bytes.to_le_bytes());
        expected_spool.extend(checksum_bytes(record.as_bytes()).to_le_bytes());
        expected_spool.extend(record.as_bytes());
        assert_eq!(fs::read(&writer.spool_path).unwrap(), expected_spool);
        let mut next = source.clone();
        next.id.push('z');
        // Keep the second record within its per-record limit.
        next.content.truncate(next.content.len() - 1);
        let error = writer.push(next).unwrap_err().to_string();
        assert!(error.contains(if limited_spool {
            "spool requires"
        } else {
            "logical bytes"
        }));
        assert_eq!(ENCODING_ATTEMPTS.get(), attempts);
        assert_eq!(STREAMING_ATTEMPTS.get(), streamed + 2);
        assert_eq!(writer.document_count, 1);
        writer.spool.as_mut().unwrap().flush().unwrap();
        assert_eq!(fs::read(&writer.spool_path).unwrap(), expected_spool);
        drop(writer);
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn fused_generation_reads_source_spool_once() {
    let root = test_dir("fused_generation_read_once");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        },
    )
    .unwrap();
    for number in 0..300 {
        writer.push(document(number)).unwrap();
    }
    spool::read_evidence::take();
    let result = writer.finish();
    let reads = spool::read_evidence::take();
    fs::remove_dir_all(root).unwrap();
    let report = result.unwrap();
    assert_eq!(reads, (1, report.spool_bytes));
}

fn three_pass_artifacts(
    input: &SearchOutOfCoreGenerationWriter,
    source: &SpoolSource,
    generation: u64,
) -> Result<GenerationArtifacts> {
    // Retain the original scheduling as an oracle. The sinks receive separate
    // decoded records, and each artifact completes before the next scan starts.
    let segment = SegmentArtifactBuilder::new(
        &input.stage.path,
        generation,
        &input.metadata_fields,
        &input.options,
    )?
    .build(source)?;
    let config = LexicalProjectionConfig {
        build_memory_bytes: input.options.lexical_build_memory_bytes,
        max_spill_bytes: input.options.lexical_max_spill_bytes,
        max_spill_runs: input.options.lexical_max_spill_runs,
        max_merge_fan_in: input.options.lexical_max_merge_fan_in,
        max_document_source_bytes: input.options.lexical_max_document_source_bytes,
        ..LexicalProjectionConfig::default()
    };
    let lexical = LexicalProjectionWriter::new(config).write_scanned(
        &input.stage.path,
        generation,
        input.options.source_graph_commit_epoch,
        lexical_analyzer_digest(&input.options.analyzer_lexicon),
        input.documents_digest.finish(),
        |consume| source.scan(&mut |document| consume(&document)),
        &input.options.analyzer_lexicon,
    )?;
    drop(lexical);
    let lexical_artifact_name = lexical_artifact_file(generation);
    let (lexical_artifact_bytes, _) =
        file_len_checksum(&input.stage.path.join(&lexical_artifact_name))?;
    let (lexical_manifest_bytes, _) =
        file_len_checksum(&input.stage.path.join(LEXICAL_MANIFEST_FILE))?;
    let rabitq = build_rabitq_artifact(
        source,
        &input.stage.path,
        generation,
        input.vector_document_count,
        input.embedding_dimension,
        input.options.embedding_manifest.as_ref(),
        &input.options,
    )?;
    Ok(GenerationArtifacts {
        segment,
        lexical_artifact_name,
        lexical_artifact_bytes,
        lexical_manifest_bytes,
        rabitq,
    })
}

fn published_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            (
                entry.file_name().into_string().unwrap(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

#[test]
fn fused_generation_is_byte_identical_to_three_pass_builds() {
    let mut random = 219_u64;
    for case in 0..18 {
        let count = [0, 1, 9, 128, 129, 300][case % 6];
        let fused_root = test_dir("fused_generation_equivalence");
        let reference_root = test_dir("three_pass_generation_equivalence");
        let options = SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(17),
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "test-model".to_string(),
                version: Some("v1".to_string()),
                dimension: 2,
            }),
            max_record_bytes: NonZeroU64::new(4096).unwrap(),
            max_segment_uncompressed_bytes: NonZeroU64::new(32 * 1024).unwrap(),
            max_segment_compressed_bytes: NonZeroU64::new(32 * 1024).unwrap(),
            lexical_build_memory_bytes: NonZeroU64::new(4096).unwrap(),
            lexical_max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            rabitq_segment_rows: NonZeroUsize::new(3).unwrap(),
            #[cfg(feature = "vector-search")]
            rabitq_bit_width: if case % 2 == 0 {
                skein_vector_projection::RaBitQBitWidth::One
            } else {
                skein_vector_projection::RaBitQBitWidth::default()
            },
            ..SearchOutOfCoreGenerationBuildOptions::default()
        };
        let mut fused =
            SearchOutOfCoreGenerationWriter::create(&fused_root, options.clone()).unwrap();
        let mut reference =
            SearchOutOfCoreGenerationWriter::create(&reference_root, options).unwrap();
        let mut documents = Vec::new();
        for number in 0..count {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let mut document = document(number);
            document.title = format!("{} \u{1f4da} term{}", document.title, random % 11);
            document.content = format!("{} token{}", document.content, random % 31);
            if case < 6 || (case < 12 && number % 2 != 0) {
                document.embedding = None;
            }
            fused.push(document.clone()).unwrap();
            reference.push(document.clone()).unwrap();
            documents.push(document);
        }
        spool::read_evidence::take();
        let fused_report = fused.finish().unwrap();
        assert_eq!(spool::read_evidence::take(), (1, fused_report.spool_bytes));
        let reference_report = reference
            .finish_with_artifacts(three_pass_artifacts)
            .unwrap();
        let reference_scans = 2 + usize::from(
            cfg!(feature = "vector-search") && reference_report.vector_document_count > 0,
        );
        assert_eq!(
            spool::read_evidence::take(),
            (
                reference_scans,
                reference_scans as u64 * reference_report.spool_bytes
            )
        );
        assert_eq!(fused_report, reference_report, "case {case}");
        assert_eq!(
            published_files(&fused_root),
            published_files(&reference_root),
            "case {case}"
        );
        // Opening verifies the published manifest/artifact digests; hydration
        // additionally validates every segment's document payload checksums.
        let reader = super::super::SearchOutOfCoreReader::open(&fused_root).unwrap();
        assert_eq!(reader.document_count(), count);
        if !documents.is_empty() {
            let hydrated = reader
                .hydrate_documents(
                    &documents
                        .iter()
                        .map(|document| document.id.clone())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            assert_eq!(hydrated.documents, documents);
        }
        drop(reader);
        fs::remove_dir_all(fused_root).unwrap();
        fs::remove_dir_all(reference_root).unwrap();
    }
}

#[test]
fn fused_generation_spool_errors_never_publish_partial_sinks() {
    for fault in ["checksum", "truncated", "trailing", "header"] {
        let root = test_dir("fused_generation_spool_failure");
        let mut initial = SearchOutOfCoreGenerationWriter::create(
            &root,
            SearchOutOfCoreGenerationBuildOptions::default(),
        )
        .unwrap();
        initial.push(document(0)).unwrap();
        let initial = initial.finish().unwrap();
        let before = published_files(&root);
        let mut replacement = SearchOutOfCoreGenerationWriter::create(
            &root,
            SearchOutOfCoreGenerationBuildOptions {
                lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
                ..SearchOutOfCoreGenerationBuildOptions::default()
            },
        )
        .unwrap();
        for number in 0..300 {
            replacement.push(document(number)).unwrap();
        }
        let error = replacement
            .finish_with_artifacts(|input, source, generation| {
                // Mutate after the initial length check so corruption exercises
                // scan validation with the other sinks already partially filled.
                let mut bytes = fs::read(&source.path)?;
                match fault {
                    "checksum" => *bytes.last_mut().unwrap() ^= 1,
                    "truncated" => {
                        bytes.pop();
                    }
                    "trailing" => bytes.push(1),
                    "header" => bytes[0] ^= 1,
                    _ => unreachable!(),
                }
                fs::write(&source.path, bytes)?;
                input.build_artifacts(source, generation)
            })
            .unwrap_err();
        assert!(error.to_string().contains(fault), "{error}");
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), initial.generation);
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents,
            vec![document(0)]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn fused_generation_sink_failures_preserve_active_artifacts() {
    for fault in [
        "lexical",
        "spill",
        "segment",
        "descriptor",
        "publication",
        "vector_memory",
        "vector_count",
    ] {
        if !cfg!(feature = "vector-search") && fault.starts_with("vector") {
            continue;
        }
        let root = test_dir("fused_generation_sink_failure");
        let mut initial = SearchOutOfCoreGenerationWriter::create(
            &root,
            SearchOutOfCoreGenerationBuildOptions::default(),
        )
        .unwrap();
        initial.push(document(0)).unwrap();
        initial.finish().unwrap();
        let before = published_files(&root);
        let mut options = SearchOutOfCoreGenerationBuildOptions {
            lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
            ..SearchOutOfCoreGenerationBuildOptions::default()
        };
        let expected = match fault {
            "lexical" => {
                options.lexical_build_memory_bytes = NonZeroU64::MIN;
                "lexical"
            }
            "spill" => {
                options.lexical_max_spill_bytes = NonZeroU64::MIN;
                "spill bytes"
            }
            "segment" => {
                options.max_segment_compressed_bytes = NonZeroU64::MIN;
                "compressed bytes"
            }
            "descriptor" => {
                options.max_descriptor_working_bytes = NonZeroU64::MIN;
                "descriptor working set"
            }
            "publication" => {
                options.max_generation_bytes = NonZeroU64::MIN;
                "published bytes"
            }
            "vector_memory" => {
                options.rabitq_build_memory_bytes = NonZeroUsize::MIN;
                "resource budget"
            }
            "vector_count" => "expected vector document count",
            _ => unreachable!(),
        };
        let mut replacement = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
        for number in 0..16 {
            replacement.push(document(number)).unwrap();
        }
        if fault == "vector_count" {
            replacement.vector_document_count += 1;
        }
        let error = replacement.finish().unwrap_err();
        assert!(error.to_string().contains(expected), "{fault}: {error}");
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents,
            vec![document(0)]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

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
    assert_eq!(
        report.rabitq_artifact_bytes > 0,
        cfg!(feature = "vector-search")
    );
    assert_eq!(
        report.rabitq_source_digest.is_some(),
        cfg!(feature = "vector-search")
    );
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
    let ids = (0..300)
        .map(|number| document(number).id)
        .collect::<Vec<_>>();
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        (0..300).map(document).collect::<Vec<_>>()
    );
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    {
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
    }
    #[cfg(feature = "vector-search")]
    {
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
    }
    drop(reader);
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
#[cfg(feature = "vector-search")]
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
