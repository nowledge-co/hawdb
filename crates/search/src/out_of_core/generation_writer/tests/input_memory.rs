use super::*;
use crate::build_memory::{decode_evidence, document_bytes};
use crate::document_codec::allocation_evidence;
use skein_core::RuntimeMemoryReservation;

fn context(bytes: u64) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(bytes, 0))
}

#[test]
fn exhausted_initial_memory_is_rejected_before_root_creation() {
    let root = test_dir("input_memory_empty");
    for bytes in [0, 1, SPOOL_BUFFER_BYTES as u64] {
        assert!(SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            context(bytes)
        )
        .is_err());
        assert!(!root.exists());
    }
}

#[test]
fn input_spare_capacity_is_rejected_before_encoding_and_released_on_drop() {
    let root = test_dir("input_memory_capacity");
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(128 * 1024),
    )
    .unwrap();
    let ledger = writer.memory.ledger.clone();
    let initial = ledger.snapshot().used_bytes;
    let mut input = document(0);
    input.content.reserve_exact(128 * 1024);
    allocation_evidence::take();
    assert!(writer.push(input).is_err());
    assert_eq!(allocation_evidence::take(), 0);
    assert_eq!(ledger.snapshot().used_bytes, initial);
    drop(writer);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn decoded_record_is_admitted_alongside_its_raw_record_and_reader_buffer() {
    let root = test_dir("input_memory_decode_overlap");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    let mut ledger = None;
    let error = writer
        .finish_with_artifacts(|_, source, _| {
            let line = encode_search_document_line(&document(0));
            let available = SPOOL_BUFFER_BYTES + line.len();
            let memory = BuildMemory::new(&context(available as u64)).unwrap();
            ledger = Some(memory.ledger.clone());
            let source = SpoolSource {
                path: source.path.clone(),
                document_count: source.document_count,
                max_record_bytes: source.max_record_bytes,
                max_metadata_fields: source.max_metadata_fields,
                memory,
            };
            decode_evidence::take();
            source.scan_admitted(&RuntimeTaskContext::default(), &mut |_, _| {
                panic!("unadmitted decoded record reached its sink")
            })?;
            panic!("unadmitted decoded record was accepted");
        })
        .unwrap_err();
    assert!(error.to_string().contains("query memory ledger"), "{error}");
    assert_eq!(decode_evidence::take(), 0);
    let snapshot = ledger.unwrap().snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes >= SPOOL_BUFFER_BYTES);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn transferred_documents_keep_their_charge_after_source_and_writer_drop() {
    let root = test_dir("input_memory_sink_transfer");
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(1024 * 1024),
    )
    .unwrap();
    for number in 0..3 {
        writer.push(document(number)).unwrap();
    }
    let ledger = writer.memory.ledger.clone();
    let mut retained = Vec::new();
    let error = writer
        .finish_with_artifacts(|input, source, _| {
            source.scan_admitted(&input.task_context, &mut |_, document| {
                retained.push(document);
                if retained.len() == 2 {
                    return Err(SkeinError::Execution(
                        "injected downstream failure".to_string(),
                    ));
                }
                Ok(())
            })?;
            panic!("downstream failure was lost");
        })
        .unwrap_err();
    assert!(error.to_string().contains("injected downstream failure"));
    let expected = retained
        .iter()
        .map(|document| document_bytes(document).unwrap())
        .sum::<usize>();
    assert!(expected > 0);
    assert_eq!(ledger.snapshot().used_bytes, expected);
    assert_eq!(ledger.snapshot().account_count, 3);
    drop(retained);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn actual_segment_keeps_decoded_input_admission_until_flush_or_drop() {
    let root = test_dir("input_memory_segment_owner");
    fs::create_dir(&root).unwrap();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let fields = required_descriptor_fields();
    let memory = BuildMemory::new(&context(1024 * 1024)).unwrap();
    let ledger = memory.ledger.clone();
    let mut segments =
        SegmentArtifactBuilder::new_with_memory(&root, 1, &fields, &options, memory.clone())
            .unwrap();
    let slots = ledger.snapshot().used_bytes;
    let input = document(0);
    let retained = document_bytes(&input).unwrap();
    assert!(retained > 0);
    let document = memory.admit_document(input).unwrap();
    assert_eq!(document.retained_bytes(), retained);
    segments.push_admitted(0, document).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, slots + retained);
    segments.finish(1).unwrap();
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_complete_fused_build_releases_all_tracked_input_charges() {
    let root = test_dir("input_memory_fused_finish");
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(1024 * 1024),
    )
    .unwrap();
    let ledger = writer.memory.ledger.clone();
    for number in 0..8 {
        writer.push(document(number)).unwrap();
    }
    let report = writer.finish().unwrap();
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert!(snapshot.peak_bytes > SPOOL_BUFFER_BYTES);
    assert!(snapshot.peak_bytes <= 1024 * 1024);
    assert_eq!(snapshot.account_count, 3);
    assert!(report.active_manifest_published_last);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fused_lexical_frequencies_share_capacity_with_the_input_owner() {
    let root = test_dir("input_memory_lexical_shared");
    let task = context(1024 * 1024);
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&root, Default::default(), task)
            .unwrap();
    let mut input = document(0);
    input.content = (0..60)
        .map(|index| format!("unique{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    writer.push(input).unwrap();
    let ledger = writer.memory.ledger.clone();
    let occupied = 1024 * 1024 - ledger.snapshot().used_bytes - 100_000;
    let sibling = writer.memory.retained.reserve(occupied).unwrap();
    let error = writer.finish().unwrap_err();
    assert!(error.to_string().contains("query memory ledger"), "{error}");
    assert_eq!(ledger.snapshot().used_bytes, occupied);
    assert_eq!(stage_directories(&root), 0);
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    drop(sibling);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn shared_memory_exhaustion_during_the_fused_scan_preserves_the_old_generation() {
    let root = test_dir("input_memory_fused_failure");
    let mut first = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    first.push(document(0)).unwrap();
    let first_generation = first.finish().unwrap().generation;
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let initial = required_descriptor_field_names()
        .map(|field| SET_ENTRY_BYTES + field.len())
        .sum::<usize>()
        + SPOOL_BUFFER_BYTES;
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context((initial + 175_000) as u64),
    )
    .unwrap();
    for number in 0..3 {
        let mut input = document(number);
        input.content = "memory ".repeat(7_000);
        writer.push(input).unwrap();
    }
    let ledger = writer.memory.ledger.clone();
    let error = writer.finish().unwrap_err();
    assert!(error.to_string().contains("query memory ledger"), "{error}");
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_eq!(stage_directories(&root), 0);
    let mut retry = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    retry.push(document(1)).unwrap();
    assert_eq!(retry.finish().unwrap().generation, first_generation + 1);
    fs::remove_dir_all(root).unwrap();
}
