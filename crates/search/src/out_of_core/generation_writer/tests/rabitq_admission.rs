use super::super::{rabitq, rabitq_memory};
use super::*;
use skein_core::RuntimeMemoryReservation;
use skein_vector_projection::{
    ProjectionBuildConfig, ProjectionIdentity, ProjectionWriter, RaBitQBitWidth,
};

mod fuzz;

fn options(rows: usize, bits: RaBitQBitWidth) -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        rabitq_segment_rows: NonZeroUsize::new(rows).unwrap(),
        rabitq_bit_width: bits,
        ..Default::default()
    }
}

fn vectors(count: usize, dimension: usize) -> Vec<SearchDocument> {
    (0..count)
        .map(|index| {
            let mut document = document(index);
            document.embedding = Some(
                (0..dimension)
                    .map(|lane| ((index + lane) as f32 * 0.31).sin())
                    .collect(),
            );
            document
        })
        .collect()
}

fn input(
    root: &Path,
    limit: usize,
    options: SearchOutOfCoreGenerationBuildOptions,
    documents: &[SearchDocument],
) -> SearchOutOfCoreGenerationWriter {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0));
    let mut input =
        SearchOutOfCoreGenerationWriter::create_with_context(root, options, task).unwrap();
    for document in documents {
        input.push(document.clone()).unwrap();
    }
    input
}

fn reference(
    path: &Path,
    documents: &[SearchDocument],
    options: &SearchOutOfCoreGenerationBuildOptions,
) -> Vec<u8> {
    let dimension = documents
        .iter()
        .find_map(|document| document.embedding.as_ref().map(Vec::len))
        .unwrap();
    let identity = ProjectionIdentity {
        generation: 1,
        source_epoch: options.source_graph_commit_epoch,
        embedding_model: options
            .embedding_manifest
            .as_ref()
            .map(|identity| identity.model.clone()),
        embedding_version: options
            .embedding_manifest
            .as_ref()
            .and_then(|identity| identity.version.clone()),
    };
    let config = ProjectionBuildConfig::new(dimension, identity)
        .with_segment_rows(options.rabitq_segment_rows.get())
        .with_bit_width(options.rabitq_bit_width)
        .with_max_working_bytes(options.rabitq_build_memory_bytes.get())
        .with_transform_seed(options.rabitq_transform_seed);
    let mut writer = ProjectionWriter::create(path, config).unwrap();
    for (ordinal, vector) in documents
        .iter()
        .filter_map(|document| document.embedding.as_deref())
        .enumerate()
    {
        writer.push(ordinal as u64, vector).unwrap();
    }
    let projection = writer.finish().unwrap();
    let manifest = projection.manifest();
    let identity_bytes = manifest
        .identity
        .embedding_model
        .as_ref()
        .map_or(0, String::len)
        + manifest
            .identity
            .embedding_version
            .as_ref()
            .map_or(0, String::len);
    let json_bound = rabitq_memory::json_bytes(identity_bytes, manifest.segments.len()).unwrap();
    let json = serde_json::to_vec(manifest).unwrap();
    assert!(json.len() <= json_bound);
    assert!(json.capacity() <= 2 * json_bound);
    drop(projection);
    fs::read(path).unwrap()
}

fn trial(
    documents: &[SearchDocument],
    options: &SearchOutOfCoreGenerationBuildOptions,
    limit: usize,
    succeeds: bool,
) -> usize {
    trial_at(
        test_dir("rabitq_admission_trial"),
        documents,
        options,
        limit,
        succeeds,
    )
}

fn trial_at(
    root: PathBuf,
    documents: &[SearchDocument],
    options: &SearchOutOfCoreGenerationBuildOptions,
    limit: usize,
    succeeds: bool,
) -> usize {
    let input = input(&root, limit, options.clone(), documents);
    let ledger = input.memory.ledger.clone();
    let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
    // Startup and builder paths vary with stage sequence and sandbox root.
    // Retain the real owners and pad their overlap before testing subsequent
    // backend phases. Constructor boundaries are checked independently.
    let startup_bytes = 256 * 1024;
    assert!(ledger.snapshot().peak_bytes < startup_bytes);
    let startup_padding = input
        .memory
        .retained
        .reserve(startup_bytes - ledger.snapshot().used_bytes)
        .unwrap();
    let result = (|| -> Result<Option<RaBitQGenerationArtifact>> {
        for document in documents {
            let admitted = input.memory.admit_document(document.clone())?;
            builder.push(&admitted)?;
        }
        builder.finish()
    })();
    assert_eq!(result.is_ok(), succeeds, "limit {limit}: {result:?}");
    if let Ok(Some(artifact)) = &result {
        let actual = fs::read(input.stage.path.join(&artifact.file_name)).unwrap();
        let expected = reference(&root.join("reference.skein"), documents, options);
        assert_eq!(actual, expected);
        assert_eq!(
            artifact.document_count,
            documents
                .iter()
                .filter(|document| document.embedding.is_some())
                .count()
        );
    }
    drop(startup_padding);
    drop(input);
    if let Ok(Some(artifact)) = &result {
        assert_eq!(ledger.snapshot().used_bytes, artifact.file_name.capacity());
    } else {
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    drop(result);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().account_count, 3);
    assert_eq!(stage_directories(&root), 0);
    let peak = ledger.snapshot().peak_bytes;
    assert!(peak <= limit);
    fs::remove_dir_all(root).unwrap();
    peak
}

#[test]
fn rabitq_bytes_and_filename_lifetime_match_reference_at_exact_and_short_budgets() {
    for bits in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        for rows in [1, 3, 1024] {
            let documents = vectors(13, 9);
            let mut options = options(rows, bits);
            options.embedding_manifest = Some(SearchEmbeddingManifest {
                model: "model\0\n\u{0130}\u{1f680}".repeat(12),
                version: Some("version\t\\\"".repeat(7)),
                dimension: 9,
            });
            let peak = trial(&documents, &options, 16 * 1024 * 1024, true);
            assert_eq!(trial(&documents, &options, peak, true), peak);
            trial(&documents, &options, peak - 1, false);
        }
    }
}

#[test]
fn rabitq_exact_budgets_keep_different_startup_paths_charged() {
    let documents = vectors(3, 9);
    let options = options(1, RaBitQBitWidth::One);
    let peak = trial_at(
        test_dir("rabitq_short_path"),
        &documents,
        &options,
        16 * 1024 * 1024,
        true,
    );
    assert_eq!(
        trial_at(
            test_dir("rabitq_longer_path_for_independent_startup_capacity"),
            &documents,
            &options,
            peak,
            true,
        ),
        peak
    );
    trial_at(
        test_dir("rabitq_third_path_length"),
        &documents,
        &options,
        peak - 1,
        false,
    );
}

#[test]
fn constructor_admission_fails_before_backend_allocation() {
    let root = test_dir("rabitq_constructor_admission");
    let documents = vectors(1, 64);
    let limit = 1024 * 1024;
    let input = input(&root, limit, options(8, RaBitQBitWidth::Four), &documents);
    let config = ProjectionBuildConfig::new(64, ProjectionIdentity::new(1))
        .with_segment_rows(8)
        .with_bit_width(RaBitQBitWidth::Four);
    let state = config.resource_admission().unwrap().peak_working_bytes;
    let available = crate::rabitq_artifact_file(1).capacity() + state - 1;
    let occupied = input
        .memory
        .retained
        .reserve(limit - input.memory.ledger.snapshot().used_bytes - available)
        .unwrap();
    rabitq::evidence::take();
    assert!(RaBitQArtifactBuilder::new(&input, 1).is_err());
    assert_eq!(rabitq::evidence::take(), (0, 0, 0));
    assert_eq!(fs::read_dir(&input.stage.path).unwrap().count(), 1);
    let ledger = input.memory.ledger.clone();
    drop(occupied);
    drop(input);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn quantization_and_directory_denials_poison_before_backend_push() {
    for directory in [false, true] {
        let root = test_dir("rabitq_push_admission");
        let documents = vectors(1, 64);
        let limit = 1024 * 1024;
        let input = input(
            &root,
            limit,
            options(
                if directory { 1 } else { 2 },
                if directory {
                    RaBitQBitWidth::One
                } else {
                    RaBitQBitWidth::Four
                },
            ),
            &documents,
        );
        let ledger = input.memory.ledger.clone();
        let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
        let document = input.memory.admit_document(documents[0].clone()).unwrap();
        let required = if directory {
            rabitq_memory::directory_bytes(1).unwrap()
        } else {
            64 * (8 + std::mem::size_of::<usize>() + 3 * 16)
        };
        let occupied = input
            .memory
            .retained
            .reserve(limit - ledger.snapshot().used_bytes - (required - 1))
            .unwrap();
        rabitq::evidence::take();
        assert!(builder
            .push(&document)
            .unwrap_err()
            .to_string()
            .contains("query memory"));
        assert_eq!(rabitq::evidence::take(), (0, 0, 0));
        drop(occupied);
        assert!(builder
            .push(&document)
            .unwrap_err()
            .to_string()
            .contains("already failed"));
        assert!(builder
            .finish()
            .unwrap_err()
            .to_string()
            .contains("already failed"));
        assert_eq!(rabitq::evidence::take(), (0, 0, 0));
        drop(document);
        drop(input);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn finalization_denial_precedes_serialization_and_private_reopen() {
    let root = test_dir("rabitq_finalize_admission");
    let documents = vectors(1, 9);
    let limit = 1024 * 1024;
    let input = input(&root, limit, options(1, RaBitQBitWidth::One), &documents);
    let ledger = input.memory.ledger.clone();
    let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
    builder.push(&documents[0]).unwrap();
    let required = rabitq_memory::finalize_bytes(0, 1).unwrap();
    let occupied = input
        .memory
        .retained
        .reserve(limit - ledger.snapshot().used_bytes - (required - 1))
        .unwrap();
    rabitq::evidence::take();
    assert!(builder
        .finish()
        .unwrap_err()
        .to_string()
        .contains("query memory"));
    assert_eq!(rabitq::evidence::take(), (0, 0, 0));
    assert!(!input
        .stage
        .path
        .join(crate::rabitq_artifact_file(1))
        .exists());
    drop(occupied);
    drop(input);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_vectors_and_unexpected_counts_cannot_publish_a_prefix() {
    for count in [false, true] {
        let root = test_dir("rabitq_failed_input");
        let documents = vectors(1, 8);
        let input = input(
            &root,
            1024 * 1024,
            options(1, RaBitQBitWidth::Four),
            &documents,
        );
        let ledger = input.memory.ledger.clone();
        let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
        if count {
            builder.push(&documents[0]).unwrap();
        }
        let mut bad = documents[0].clone();
        bad.embedding.as_mut().unwrap()[0] = f32::NAN;
        assert!(builder.push(&bad).is_err());
        assert!(builder
            .push(&documents[0])
            .unwrap_err()
            .to_string()
            .contains("already failed"));
        assert!(builder.finish().is_err());
        assert!(!input
            .stage
            .path
            .join(crate::rabitq_artifact_file(1))
            .exists());
        drop(input);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn fused_rabitq_root_denial_preserves_old_artifacts_and_reopen() {
    let root = test_dir("rabitq_fused_root_denial");
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(document(0)).unwrap();
    initial.finish().unwrap();
    let before = published_files(&root);
    let replacement = input(
        &root,
        256 * 1024,
        options(1024, RaBitQBitWidth::Four),
        &vectors(1, 512),
    );
    let ledger = replacement.memory.ledger.clone();
    rabitq::evidence::take();
    assert!(replacement
        .finish()
        .unwrap_err()
        .to_string()
        .contains("query memory"));
    assert_eq!(rabitq::evidence::take(), (0, 0, 0));
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(published_files(&root), before);
    let reader = crate::out_of_core::SearchOutOfCoreReader::open(&root).unwrap();
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

#[test]
fn finalization_bound_arithmetic_rejects_overflow() {
    assert!(rabitq_memory::finalize_bytes(usize::MAX, 1).is_err());
    assert!(rabitq_memory::finalize_bytes(0, usize::MAX).is_err());
    assert!(rabitq_memory::directory_bytes(usize::MAX).is_err());
}

#[test]
fn state_directory_and_finalize_leases_cover_every_live_backend_phase() {
    let root = test_dir("rabitq_live_owners");
    let documents = vectors(5, 8);
    let options = options(1, RaBitQBitWidth::Four);
    let input = input(&root, 1024 * 1024, options, &documents);
    let ledger = input.memory.ledger.clone();
    let before = ledger.snapshot().used_bytes;
    let state = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1))
        .with_segment_rows(1)
        .with_bit_width(RaBitQBitWidth::Four)
        .resource_admission()
        .unwrap()
        .peak_working_bytes;
    let name = crate::rabitq_artifact_file(1).capacity();
    let path = input
        .stage
        .path
        .join(crate::rabitq_artifact_file(1))
        .capacity();
    let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, before + state + name + path);
    for (index, document) in documents.iter().enumerate() {
        builder.push(document).unwrap();
        assert_eq!(
            ledger.snapshot().used_bytes,
            before + state + name + path + rabitq_memory::directory_bytes(index + 1).unwrap()
        );
    }
    let prior_finish = ledger.snapshot().used_bytes;
    rabitq::evidence::take_reopened_bytes();
    let artifact = builder.finish().unwrap().unwrap();
    assert_eq!(
        rabitq::evidence::take_reopened_bytes(),
        prior_finish + rabitq_memory::finalize_bytes(0, documents.len()).unwrap()
    );
    assert_eq!(ledger.snapshot().used_bytes, before + name);
    drop(input);
    assert_eq!(ledger.snapshot().used_bytes, name);
    drop(artifact);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn missing_vectors_do_not_create_a_backend_or_retain_admission() {
    let root = test_dir("rabitq_no_vectors");
    let mut documents = vectors(3, 8);
    for document in &mut documents {
        document.embedding = None;
    }
    let input = input(&root, 1024 * 1024, Default::default(), &documents);
    let ledger = input.memory.ledger.clone();
    rabitq::evidence::take();
    let mut builder = RaBitQArtifactBuilder::new(&input, 1).unwrap();
    for document in &documents {
        builder.push(document).unwrap();
    }
    assert!(builder.finish().unwrap().is_none());
    assert_eq!(rabitq::evidence::take(), (0, 0, 0));
    drop(input);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}
