use super::super::{
    artifacts::SegmentArtifactBuilder, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use super::*;
use crate::build_memory::AdmittedDocument;
use crate::out_of_core::{OUT_OF_CORE_LAYOUT_FORMAT, OUT_OF_CORE_MANIFEST_FILE};
use crate::{SearchDocument, SearchOutOfCoreReader, SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

thread_local! { static NAMES: Cell<usize> = const { Cell::new(0) }; }
thread_local! { static CANCEL_NAME: RefCell<Option<skein_core::RuntimeCancellationToken>> = const { RefCell::new(None) }; }

pub(super) fn record_name() {
    NAMES.set(NAMES.get() + 1);
    CANCEL_NAME.with_borrow_mut(|token| {
        if let Some(token) = token.take() {
            token.cancel();
        }
    });
}

fn task(bytes: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(bytes as u64, 0))
}
fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(&task(bytes)).unwrap()
}
fn root() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-artifact-paths-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
fn native_bytes(path: &Path) -> usize {
    path.as_os_str().as_encoded_bytes().len()
}
fn join_peak(stage: &Path, name: &str) -> usize {
    assert!(
        !matches!(stage.components().next(), Some(std::path::Component::Prefix(prefix)) if prefix.kind().is_verbatim())
    );
    3 * (native_bytes(stage) + name.len() + 1).max(8)
}

// Derive expected paths and capacities with direct standard-library operations,
// without asking the production owner for its reserve or observed peak.
fn segment_expected(stage: &Path) -> (usize, usize, usize) {
    let mut peak = 0;
    let mut retained = 0;
    for name in [
        SEARCH_SEGMENT_PAYLOAD_FILE,
        STAGE_METADATA_FILE,
        STAGE_VECTOR_FILE,
        SEARCH_SEGMENT_DESCRIPTOR_FILE,
    ] {
        peak = peak.max(retained + join_peak(stage, name));
        retained += stage.join(name).capacity();
    }
    let descriptor = stage.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let temporary = descriptor.with_extension("skein.tmp");
    peak = peak.max(retained + native_bytes(&descriptor) + "skein.tmp".len() + 1);
    retained += temporary.capacity();
    (peak, retained, descriptor.capacity() + temporary.capacity())
}
fn segment_trial(stage: &Path, short: bool) {
    let (peak, retained, _) = segment_expected(stage);
    let memory = memory(137 + peak - usize::from(short));
    let ledger = memory.ledger.clone();
    let sibling = memory.input.reserve(137).unwrap();
    let result = Segment::new(stage, &memory, &RuntimeTaskContext::default());
    assert_eq!(result.is_ok(), !short);
    if let Ok(paths) = result {
        for (actual, name) in [
            (&paths.document, SEARCH_SEGMENT_PAYLOAD_FILE),
            (&paths.metadata, STAGE_METADATA_FILE),
            (&paths.vector, STAGE_VECTOR_FILE),
            (&paths.descriptor, SEARCH_SEGMENT_DESCRIPTOR_FILE),
        ] {
            assert_eq!(actual.as_ref(), stage.join(name));
        }
        assert_eq!(
            paths.temporary.as_ref(),
            stage
                .join(SEARCH_SEGMENT_DESCRIPTOR_FILE)
                .with_extension("skein.tmp")
        );
        assert_eq!(ledger.snapshot().peak_bytes, 137 + peak);
        drop(sibling);
        drop(memory);
        assert_eq!(ledger.snapshot().used_bytes, retained);
        drop(paths);
    } else {
        assert_eq!(ledger.snapshot().used_bytes, 137);
        drop(sibling);
    }
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().account_count, 3);
}

#[test]
fn segment_paths_share_independent_exact_and_one_short_budgets() {
    for stage in ["", ".", "stage/\u{130}", "stage/a/../b"] {
        segment_trial(Path::new(stage), false);
        segment_trial(Path::new(stage), true);
    }
}

fn name_trial(generation: u64, vector: bool, short: bool) {
    let memory = memory(137 + 384 - usize::from(short));
    let ledger = memory.ledger.clone();
    let sibling = memory.input.reserve(137).unwrap();
    NAMES.set(0);
    let result = if vector {
        Name::rabitq(generation, &memory, &RuntimeTaskContext::default())
    } else {
        Name::lexical(generation, &memory, &RuntimeTaskContext::default())
    };
    assert_eq!(result.is_ok(), !short);
    assert_eq!(NAMES.get(), usize::from(!short));
    if let Ok(name) = result {
        let expected = if vector {
            format!("search_rabitq.{generation}.skein")
        } else {
            format!("search_lexical.{generation}.skein")
        };
        assert_eq!(name.as_str(), expected);
        let retained = name.capacity();
        assert!(retained >= expected.len());
        assert!(retained <= 128);
        assert_eq!(ledger.snapshot().peak_bytes, 137 + 384);
        drop(sibling);
        drop(memory);
        assert_eq!(ledger.snapshot().used_bytes, retained);
        drop(name);
    } else {
        assert_eq!(ledger.snapshot().used_bytes, 137);
        drop(sibling);
    }
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn artifact_names_are_preadmitted_and_retain_full_capacities() {
    for generation in [0, 9, 10, u64::MAX] {
        for vector in [false, true] {
            for short in [false, true] {
                name_trial(generation, vector, short);
            }
        }
    }
}

#[test]
fn cancelled_name_construction_releases_the_allocated_payload() {
    let context = task(1024);
    let memory = BuildMemory::new(&context).unwrap();
    CANCEL_NAME.with_borrow_mut(|token| *token = Some(context.cancellation().clone()));
    NAMES.set(0);
    let error = Name::lexical(u64::MAX, &memory, &context).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(NAMES.get(), 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

fn lexical_trial(stage: &Path, generation: u64, short: bool) {
    let name = format!("search_lexical.{generation}.skein");
    let artifact = stage.join(&name);
    let manifest = stage.join(LEXICAL_MANIFEST_FILE);
    let peak = 384
        .max(name.capacity() + join_peak(stage, &name))
        .max(name.capacity() + artifact.capacity() + join_peak(stage, LEXICAL_MANIFEST_FILE));
    let memory = memory(137 + peak - usize::from(short));
    let ledger = memory.ledger.clone();
    let sibling = memory.input.reserve(137).unwrap();
    let result = Lexical::new(stage, generation, &memory, &RuntimeTaskContext::default());
    assert_eq!(result.is_ok(), !short);
    if let Ok(paths) = result {
        assert_eq!(paths.name.as_str(), name);
        assert_eq!(paths.artifact.as_ref(), artifact);
        assert_eq!(paths.manifest.as_ref(), manifest);
        assert_eq!(
            ledger.snapshot().used_bytes,
            137 + name.capacity() + artifact.capacity() + manifest.capacity()
        );
        assert_eq!(ledger.snapshot().peak_bytes, 137 + peak);
        drop(sibling);
        drop(memory);
        let Lexical {
            name: retained,
            artifact,
            manifest,
        } = paths;
        drop(artifact);
        drop(manifest);
        assert_eq!(ledger.snapshot().used_bytes, name.capacity());
        drop(retained);
    } else {
        drop(sibling);
    }
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn lexical_checksum_paths_release_before_the_retained_name() {
    for generation in [0, 10, u64::MAX] {
        for short in [false, true] {
            lexical_trial(Path::new("stage/\u{130}"), generation, short);
        }
    }
}

fn builder_base() -> usize {
    SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS * std::mem::size_of::<AdmittedDocument>()
        + OUT_OF_CORE_LAYOUT_FORMAT.len()
}

#[test]
fn segment_constructor_admits_every_path_before_creating_files() {
    for short in [true, false] {
        let root = root();
        fs::create_dir(&root).unwrap();
        let names = [
            SEARCH_SEGMENT_PAYLOAD_FILE,
            STAGE_METADATA_FILE,
            STAGE_VECTOR_FILE,
        ];
        for name in names {
            fs::write(root.join(name), b"untouched").unwrap();
        }
        let (peak, _, retained) = segment_expected(&root);
        let memory = memory(137 + builder_base() + peak - usize::from(short));
        let ledger = memory.ledger.clone();
        let sibling = memory.input.reserve(137).unwrap();
        let fields = BTreeSet::new();
        let options = SearchOutOfCoreGenerationBuildOptions::default();
        let result = SegmentArtifactBuilder::new_with_context(
            &root,
            1,
            &fields,
            &options,
            memory.clone(),
            RuntimeTaskContext::default(),
        );
        assert_eq!(result.is_ok(), !short);
        for name in names {
            assert_eq!(
                fs::read(root.join(name)).unwrap(),
                if short {
                    b"untouched".as_slice()
                } else {
                    b"".as_slice()
                }
            );
        }
        if let Ok(builder) = result {
            assert_eq!(
                ledger.snapshot().used_bytes,
                137 + builder_base() + retained
            );
            drop(sibling);
            drop(memory);
            assert_eq!(ledger.snapshot().used_bytes, builder_base() + retained);
            drop(builder);
        } else {
            drop(sibling);
        }
        assert_eq!(ledger.snapshot().used_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn cancelled_segment_constructor_and_names_do_not_enter_allocation_or_io() {
    let root = root();
    let context = task(1024 * 1024);
    let memory = BuildMemory::new(&context).unwrap();
    context.cancellation().cancel();
    NAMES.set(0);
    assert!(Name::lexical(1, &memory, &context).is_err());
    assert!(Name::rabitq(1, &memory, &context).is_err());
    assert_eq!(NAMES.get(), 0);
    assert!(Lexical::new(&root, 1, &memory, &context).is_err());
    let error = SegmentArtifactBuilder::new_with_context(
        &root,
        1,
        &BTreeSet::new(),
        &Default::default(),
        memory.clone(),
        context,
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert!(!root.exists());
    assert_eq!(memory.ledger.snapshot().peak_bytes, 0);
}

#[test]
fn descriptor_finish_reuses_paths_with_only_encoding_capacity_available() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let memory = memory(1024 * 1024);
    let fields = BTreeSet::new();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let builder =
        SegmentArtifactBuilder::new_with_memory(&root, 1, &fields, &options, memory.clone())
            .unwrap();
    let body = crate::encode_search_segment_descriptor_body(&crate::SearchSegmentDescriptor {
        target_documents: SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
        document_count: 0,
        segments: Vec::new(),
    });
    let expected = format!(
        "{body}checksum\t{}\n",
        crate::checksum_bytes(body.as_bytes())
    );
    let held = memory
        .retained
        .reserve(1024 * 1024 - memory.ledger.snapshot().used_bytes - expected.len())
        .unwrap();
    let output = builder.finish(0).unwrap();
    assert_eq!(
        fs::read(root.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap(),
        expected.as_bytes()
    );
    assert!(!root
        .join(SEARCH_SEGMENT_DESCRIPTOR_FILE)
        .with_extension("skein.tmp")
        .exists());
    drop(held);
    drop(output);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

fn document(index: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{index}"),
        title: "graph".into(),
        content: format!("artifact \u{130} {index}"),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}
fn files(root: &Path) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            (entry.file_name(), fs::read(entry.path()).unwrap())
        })
        .collect()
}

fn update_trial(case: usize) {
    let root = root();
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let before = files(&root);
    let context = task(16 * 1024 * 1024);
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(case as u64 + 1),
            ..Default::default()
        },
        context,
    )
    .unwrap();
    let mut expected = document(case + 1);
    #[cfg(feature = "vector-search")]
    {
        expected.embedding = Some(vec![1.0, case as f32]);
    }
    expected.metadata.insert("case".into(), case.to_string());
    writer.push(expected.clone()).unwrap();
    let ledger = writer.memory.ledger.clone();
    let mut held = None;
    let result =
        writer.finish_with_artifacts(|input, source, generation| {
            match case % 4 {
                0 => {
                    let available = builder_base() + segment_expected(&input.stage.path).0 - 1;
                    held =
                        Some(input.memory.retained.reserve(
                            16 * 1024 * 1024 - ledger.snapshot().used_bytes - available,
                        )?);
                }
                1 => fs::create_dir(input.stage.path.join(STAGE_METADATA_FILE))?,
                3 => {
                    input.task_context.cancellation().cancel();
                }
                _ => {}
            }
            input.build_artifacts(source, generation)
        });
    assert_eq!(
        result.is_ok(),
        case % 4 == 2,
        "case={case} result={result:?}"
    );
    if let Err(error) = &result {
        match case % 4 {
            0 => assert!(
                matches!(error, SkeinError::Execution(message) if message.contains("query memory"))
            ),
            1 => assert!(matches!(error, SkeinError::Storage(_))),
            3 => assert!(
                matches!(error, SkeinError::Execution(message) if message.contains("cancelled"))
            ),
            _ => unreachable!(),
        }
    }
    assert_eq!(
        ledger.snapshot().used_bytes,
        held.as_ref().map_or(0, QueryMemoryLease::bytes)
    );
    drop(held);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    if let Ok(report) = result {
        assert_eq!(report.generation, first.generation + 1);
        assert_eq!(reader.source_graph_commit_epoch(), Some(case as u64 + 1));
        assert_eq!(
            reader
                .hydrate_documents(&[expected.id.clone()])
                .unwrap()
                .documents,
            vec![expected]
        );
    } else {
        assert_eq!(files(&root), before);
        assert_eq!(reader.generation(), first.generation);
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents,
            vec![document(0)]
        );
    }
    assert!(root.join(OUT_OF_CORE_MANIFEST_FILE).is_file());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_builder_failures_preserve_the_old_generation_and_release_paths() {
    for case in 0..4 {
        update_trial(case);
    }
}

#[test]
fn real_artifact_summaries_retain_names_on_the_writer_root() {
    let root = root();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    let mut expected = document(1);
    expected.metadata.insert("case".into(), "summary".into());
    #[cfg(feature = "vector-search")]
    {
        expected.embedding = Some(vec![1.0, 2.0]);
    }
    writer.push(expected.clone()).unwrap();
    let ledger = writer.memory.ledger.clone();
    let report = writer
        .finish_with_artifacts(|input, source, generation| {
            let before = ledger.snapshot().used_bytes;
            let artifacts = input.build_artifacts(source, generation)?;
            let layout = &artifacts.segment.layout;
            let retained = layout.format.capacity()
                + layout.segments.capacity()
                    * std::mem::size_of::<crate::out_of_core::SearchOutOfCoreSegmentLayout>()
                + artifacts.lexical_artifact_name.capacity()
                + artifacts
                    .rabitq
                    .as_ref()
                    .map_or(0, |artifact| artifact.file_name.capacity());
            assert_eq!(ledger.snapshot().used_bytes, before + retained);
            Ok(artifacts)
        })
        .unwrap();
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(
        report.lexical_artifact_bytes,
        fs::metadata(root.join(format!("search_lexical.{}.skein", report.generation)))
            .unwrap()
            .len()
    );
    assert_eq!(
        report.lexical_manifest_bytes,
        fs::metadata(root.join(format!(
            "search_lexical.manifest.{}.skein",
            report.generation
        )))
        .unwrap()
        .len()
    );
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(&[expected.id.clone()])
            .unwrap()
            .documents,
        vec![expected]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn rabitq_wrapper_path_admission_precedes_backend_creation() {
    use super::super::{rabitq, RaBitQArtifactBuilder};
    for short in [false, true] {
        let root = root();
        let input = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task(1024 * 1024),
        )
        .unwrap();
        let ledger = input.memory.ledger.clone();
        let name = crate::rabitq_artifact_file(u64::MAX);
        let path = input.stage.path.join(&name);
        let peak = 384.max(name.capacity() + join_peak(&input.stage.path, &name));
        let held = input
            .memory
            .retained
            .reserve(1024 * 1024 - ledger.snapshot().used_bytes - peak + usize::from(short))
            .unwrap();
        let before = ledger.snapshot().used_bytes;
        rabitq::evidence::take();
        let result = RaBitQArtifactBuilder::new(&input, u64::MAX);
        assert_eq!(result.is_ok(), !short);
        assert_eq!(rabitq::evidence::take(), (0, 0, 0));
        assert!(!path.exists());
        if let Ok(builder) = result {
            assert_eq!(
                ledger.snapshot().used_bytes,
                before + name.capacity() + path.capacity()
            );
            assert!(builder.finish().unwrap().is_none());
        }
        assert_eq!(ledger.snapshot().used_bytes, before);
        drop(input);
        assert_eq!(ledger.snapshot().used_bytes, held.bytes());
        drop(held);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }
}
