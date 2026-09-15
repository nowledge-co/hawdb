use super::*;
use skein_core::{RuntimeCancellationToken, RuntimeMemoryReservation};

fn context(bytes: u64) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(bytes, 0))
}

#[test]
fn owned_paths_admit_before_copy_and_release_after_cancellation() {
    use crate::build_memory::path::{evidence, OwnedPath};
    let task = RuntimeTaskContext::default();
    let source = Path::new("generation/root");
    let bytes = source.as_os_str().as_encoded_bytes().len();
    let limited = BuildMemory::new(&context(bytes as u64 - 1)).unwrap();
    evidence::take();
    assert!(OwnedPath::copy(source, &limited, &task).is_err());
    assert_eq!(evidence::take(), 0);
    assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    let memory = BuildMemory::new(&context(bytes as u64)).unwrap();
    let owned = OwnedPath::copy(source, &memory, &task).unwrap();
    assert_eq!(owned.as_ref(), source);
    assert_eq!(owned.capacity(), memory.ledger.snapshot().used_bytes);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    evidence::cancel_next(task.cancellation().clone());
    assert!(OwnedPath::copy(source, &memory, &task).is_err());
    assert!(task.cancellation().is_cancelled());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn empty_option_collections_still_charge_their_spare_capacity() {
    let memory = BuildMemory::new(&context(8192)).unwrap();
    let task = RuntimeTaskContext::default();
    let mut options = SearchOutOfCoreGenerationBuildOptions {
        analyzer_lexicon: SearchAnalyzerLexicon::empty(),
        ..Default::default()
    };
    let ordinary = context_memory::Options::new(options.clone(), &memory, &task).unwrap();
    drop(ordinary);
    options.analyzer_lexicon.alias_rules.reserve_exact(4096);
    assert!(options.analyzer_lexicon.alias_rules.is_empty());
    assert!(context_memory::Options::new(options, &memory, &task).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn cancelled_or_unadmitted_startup_creates_no_directory() {
    let root = test_dir("context_startup");
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    for task in [
        RuntimeTaskContext::without_deadline(cancellation),
        context(0),
        context(1),
    ] {
        assert!(SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task,
        )
        .is_err());
        assert!(!root.exists());
    }
}

#[test]
fn input_exhaustion_precedes_spool_append_and_releases_writer_state() {
    let root = test_dir("context_input_admission");
    let budget = 1024 * 1024;
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(budget),
    )
    .unwrap();
    let memory = writer.memory.clone();
    let before = writer.documents_digest.finish();
    let held = memory
        .retained
        .reserve(budget as usize - memory.ledger.snapshot().used_bytes)
        .unwrap();
    assert!(writer.push(document(0)).is_err());
    assert_eq!(writer.document_count, 0);
    assert_eq!(writer.spool_bytes, SPOOL_HEADER.len() as u64);
    assert_eq!(writer.documents_digest.finish(), before);
    writer.spool.as_mut().unwrap().flush().unwrap();
    assert_eq!(fs::read(&writer.spool_path).unwrap(), SPOOL_HEADER);
    drop(writer);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, held.bytes());
    drop(held);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn segment_document_slots_are_admitted_before_artifact_creation() {
    let root = test_dir("context_segment_slots");
    fs::create_dir(&root).unwrap();
    let bytes = SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS * std::mem::size_of::<AdmittedDocument>();
    let memory = BuildMemory::new(&context(bytes as u64 - 1)).unwrap();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let fields = required_descriptor_fields();
    let result =
        SegmentArtifactBuilder::new_with_memory(&root, 1, &fields, &options, memory.clone());
    assert!(result.is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_before_finish_preserves_the_complete_active_generation() {
    let root = test_dir("context_cancel_before_finish");
    let previous = document(0);
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(previous.clone()).unwrap();
    let generation = initial.finish().unwrap().generation;
    let before = published_files(&root);
    let task = context(8 * 1024 * 1024);
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        task.clone(),
    )
    .unwrap();
    let memory = writer.memory.clone();
    writer.push(document(1)).unwrap();
    task.cancellation().cancel();
    let error = writer.finish().unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(published_files(&root), before);
    let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), generation);
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&previous.id))
            .unwrap()
            .documents,
        vec![previous],
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_during_spool_read_drops_all_staged_outputs_and_charges() {
    for unit in ["token ", "token \u{9f98}\u{9750} "] {
        let root = test_dir("context_cancel_spool");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(document(0)).unwrap();
        initial.finish().unwrap();
        let before = published_files(&root);
        let task = context(8 * 1024 * 1024);
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task.clone(),
        )
        .unwrap();
        let memory = writer.memory.clone();
        let mut source = document(1);
        source.content = unit.repeat(16 * 1024);
        writer.push(source).unwrap();
        spool::read_evidence::take();
        let _cancel = spool::read_evidence::cancel_after_bytes(0, task.cancellation().clone());
        let error = writer.finish().unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
        let (opens, read_bytes) = spool::read_evidence::take();
        assert_eq!(opens, 1);
        assert!(read_bytes <= SPOOL_BUFFER_BYTES as u64);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn artifact_admission_and_compression_cancellation_preserve_the_active_generation() {
    use super::super::artifacts::encoding::evidence;
    for cancel in [false, true] {
        let root = test_dir("context_compression_failure");
        let previous = document(0);
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(previous.clone()).unwrap();
        let generation = initial.finish().unwrap().generation;
        let before = published_files(&root);
        let task = context(if cancel {
            32 * 1024 * 1024
        } else {
            4 * 1024 * 1024
        });
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task.clone(),
        )
        .unwrap();
        let memory = writer.memory.clone();
        writer.push(document(1)).unwrap();
        evidence::take_starts();
        if cancel {
            evidence::cancel_on_output(task.cancellation().clone());
        }
        let error = writer.finish().unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancel" } else { "budget" }),
            "{error}"
        );
        assert_eq!(evidence::take_starts(), usize::from(cancel));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation);
        assert_eq!(
            reader
                .hydrate_documents(std::slice::from_ref(&previous.id))
                .unwrap()
                .documents,
            vec![previous]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(feature = "vector-search")]
#[test]
fn cancellation_after_vector_core_calls_preserves_the_active_generation() {
    use super::super::rabitq::evidence;
    for call in 0..3 {
        let root = test_dir("context_vector_cancellation");
        let previous = document(0);
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(previous.clone()).unwrap();
        let generation = initial.finish().unwrap().generation;
        let before = published_files(&root);
        let task = context(32 * 1024 * 1024);
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task.clone(),
        )
        .unwrap();
        let memory = writer.memory.clone();
        writer.push(document(1)).unwrap();
        evidence::take();
        evidence::cancel_after(call, task.cancellation().clone());
        let error = writer.finish().unwrap_err();
        assert!(error.to_string().contains("cancel"), "{error}");
        assert_eq!(
            evidence::take(),
            (1, usize::from(call >= 1), usize::from(call >= 2))
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation);
        assert_eq!(
            reader
                .hydrate_documents(std::slice::from_ref(&previous.id))
                .unwrap()
                .documents,
            vec![previous]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn chinese_analyzer_denial_preserves_the_active_generation_and_cleans_the_stage() {
    let root = test_dir("context_chinese_analyzer_denial");
    let previous = document(0);
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(previous.clone()).unwrap();
    let generation = initial.finish().unwrap().generation;
    let before = published_files(&root);
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(4 * 1024 * 1024),
    )
    .unwrap();
    let memory = writer.memory.clone();
    let mut next = document(1);
    next.content = "\u{9f98}\u{9750}\u{9f49}".into();
    writer.push(next).unwrap();
    assert!(writer.needs_chinese_analyzer);
    let error = writer.finish().unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(published_files(&root), before);
    let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), generation);
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&previous.id))
            .unwrap()
            .documents,
        vec![previous]
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn resident_frequency_denial_stops_analysis_and_preserves_the_active_generation() {
    let root = test_dir("context_frequency_denial");
    let previous = document(0);
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(previous.clone()).unwrap();
    let generation = initial.finish().unwrap().generation;
    let before = published_files(&root);
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        Default::default(),
        context(4 * 1024 * 1024),
    )
    .unwrap();
    let memory = writer.memory.clone();
    let mut next = document(1);
    next.embedding = None;
    next.content = (0..4096).map(|index| format!("token{index:05} ")).collect();
    writer.push(next).unwrap();
    assert!(!writer.needs_chinese_analyzer);
    crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.set(0));
    let error = writer.finish().unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"), "{error}");
    let visited = crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.get());
    assert!(visited > 0 && visited < 4096, "analysis visits={visited}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(published_files(&root), before);
    let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), generation);
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&previous.id))
            .unwrap()
            .documents,
        vec![previous]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn governed_chinese_build_preserves_the_complete_artifact_bytes_and_reopen() {
    let original_root = test_dir("context_chinese_original");
    let governed_root = test_dir("context_chinese_governed");
    let mut documents = vec![document(0), document(1)];
    documents[0].content =
        "GraphStorage \u{4e2d}\u{534e}\u{4eba}\u{6c11}\u{5171}\u{548c}\u{56fd}".into();
    documents[1].content = "\u{9f98}\u{9750}\u{9f49} write_ahead_log \u{20000}\u{20001}".into();
    for input in &mut documents {
        input.embedding = None;
    }
    let mut original =
        SearchOutOfCoreGenerationWriter::create(&original_root, Default::default()).unwrap();
    let mut governed = SearchOutOfCoreGenerationWriter::create_with_context(
        &governed_root,
        Default::default(),
        context(32 * 1024 * 1024),
    )
    .unwrap();
    let memory = governed.memory.clone();
    for input in &documents {
        original.push(input.clone()).unwrap();
        governed.push(input.clone()).unwrap();
    }
    original
        .finish_with_artifacts(SearchOutOfCoreGenerationWriter::build_artifacts)
        .unwrap();
    governed.finish().unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(
        published_files(&governed_root),
        published_files(&original_root)
    );
    let reader = crate::SearchOutOfCoreReader::open(&governed_root).unwrap();
    let ids = documents
        .iter()
        .map(|document| document.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, documents);
    drop(reader);
    fs::remove_dir_all(original_root).unwrap();
    fs::remove_dir_all(governed_root).unwrap();
}
