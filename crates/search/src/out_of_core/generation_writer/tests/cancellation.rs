use super::*;
use crate::{RuntimeCancellationToken, RuntimeTaskContext};

fn assert_cancelled(error: SkeinError) {
    assert!(matches!(error, SkeinError::Execution(_)), "{error}");
    assert!(error.to_string().contains("cancelled"), "{error}");
}

fn initial_generation(root: &Path) -> u64 {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    writer.finish().unwrap().generation
}

#[test]
fn cancelled_create_does_not_allocate_a_stage_or_create_the_root() {
    let root = test_dir("cancelled_create");
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    assert_cancelled(
        SearchOutOfCoreGenerationWriter::create_with_context(&root, Default::default(), task)
            .unwrap_err(),
    );
    assert!(!root.exists());
}

#[test]
fn parent_cancellation_poisoning_cannot_publish_partial_spool_input() {
    let root = test_dir("cancelled_push");
    let parent = RuntimeTaskContext::default();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        parent.child(),
    )
    .unwrap();
    writer.push(document(0)).unwrap();
    parent.cancellation().cancel();
    assert_cancelled(writer.push(document(1)).unwrap_err());
    assert!(writer.poisoned);
    assert_eq!(writer.document_count, 1);
    assert_cancelled(writer.finish().unwrap_err());
    assert_eq!(stage_directories(&root), 0);
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn expired_finish_rejects_before_source_scan_and_preserves_active_generation() {
    let root = test_dir("expired_finish");
    let generation = initial_generation(&root);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(1)).unwrap();
    writer.task_context = RuntimeTaskContext::new(
        RuntimeCancellationToken::new(),
        Some(std::time::Instant::now()),
    );
    let error = writer
        .finish_with_artifacts(|_, _, _| panic!("expired build scanned its input"))
        .unwrap_err();
    assert!(matches!(error, SkeinError::Execution(_)), "{error}");
    assert!(error.to_string().contains("deadline"), "{error}");
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(
        super::super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        generation
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn spool_scan_stops_after_the_consumer_cancels_without_another_record() {
    let root = test_dir("cancelled_spool_scan");
    let task = RuntimeTaskContext::default();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        task.clone(),
    )
    .unwrap();
    for index in 0..8 {
        writer.push(document(index)).unwrap();
    }
    let mut observed = Vec::new();
    let error = writer
        .finish_with_artifacts(|input, source, _| {
            source.scan_with_context(&input.task_context, &mut |ordinal, document| {
                observed.push((ordinal, document.id));
                if ordinal == 2 {
                    task.cancellation().cancel();
                }
                Ok(())
            })?;
            panic!("cancelled scan succeeded");
        })
        .unwrap_err();
    assert_cancelled(error);
    assert_eq!(observed.len(), 3);
    assert_eq!(stage_directories(&root), 0);
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_after_all_sinks_finish_still_prevents_publication_and_allows_retry() {
    let root = test_dir("cancelled_before_publish");
    let generation = initial_generation(&root);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let task = RuntimeTaskContext::default();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        task.clone(),
    )
    .unwrap();
    for index in 1..4 {
        writer.push(document(index)).unwrap();
    }
    let error = writer
        .finish_with_artifacts(|input, source, next| {
            let artifacts = input.build_artifacts(source, next)?;
            assert!(artifacts.lexical_artifact_bytes > 0);
            assert!(artifacts.segment.document_payload_bytes > 0);
            task.cancellation().cancel();
            Ok(artifacts)
        })
        .unwrap_err();
    assert_cancelled(error);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert!(!root.join(lexical_artifact_file(generation + 1)).exists());
    assert_eq!(stage_directories(&root), 0);
    let mut retry = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    retry.push(document(1)).unwrap();
    let report = retry.finish().unwrap();
    assert_eq!(report.generation, generation + 1);
    assert_eq!(
        super::super::super::SearchOutOfCoreReader::open(&root)
            .unwrap()
            .generation(),
        report.generation
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn delta_build_context_is_preserved_between_prepare_and_finish() {
    let root = test_dir("cancelled_delta_finish");
    initial_generation(&root);
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let task = RuntimeTaskContext::default();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        SearchProjectionDelta::default(),
        Default::default(),
        task.clone(),
    )
    .unwrap();
    task.cancellation().cancel();
    assert_cancelled(update.finish().unwrap_err());
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn real_fused_scan_cancellation_removes_all_partially_built_sinks() {
    let root = test_dir("cancelled_real_scan");
    initial_generation(&root);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let task = RuntimeTaskContext::default().with_memory_reservation(
        skein_core::RuntimeMemoryReservation::new(16 * 1024 * 1024, 0),
    );
    let options = SearchOutOfCoreGenerationBuildOptions {
        lexical_build_memory_bytes: NonZeroU64::new(1024).unwrap(),
        ..Default::default()
    };
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&root, options, task.clone()).unwrap();
    for index in 0..300 {
        writer.push(document(index)).unwrap();
    }
    let spool_bytes = writer.spool_bytes;
    let ledger = writer.memory.ledger.clone();
    spool::read_evidence::take();
    let _cancel = spool::read_evidence::cancel_after_bytes(16 * 1024, task.cancellation().clone());
    assert_cancelled(writer.finish().unwrap_err());
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes > SPOOL_BUFFER_BYTES);
    let (opens, bytes) = spool::read_evidence::take();
    assert_eq!(opens, 1);
    assert!(
        bytes >= 16 * 1024 && bytes < spool_bytes,
        "read {bytes} of {spool_bytes}"
    );
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_after_the_commit_gate_returns_the_successfully_published_generation() {
    let root = test_dir("cancelled_after_commit_gate");
    let first = initial_generation(&root);
    let task = RuntimeTaskContext::default();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        task.clone(),
    )
    .unwrap();
    writer.push(document(1)).unwrap();
    let _cancel = publication::late_cancellation::arm(task.cancellation().clone());
    let report = writer.finish().unwrap();
    assert!(task.cancellation().is_cancelled());
    assert_eq!(report.generation, first + 1);
    assert!(report.active_manifest_published_last);
    assert_eq!(stage_directories(&root), 0);
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(
        reader
            .hydrate_documents(&[document(1).id])
            .unwrap()
            .documents,
        vec![document(1)]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
