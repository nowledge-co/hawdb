use super::*;
use crate::out_of_core::generation_writer::{
    SearchOutOfCoreGenerationWriter, STAGE_METADATA_FILE, STAGE_VECTOR_FILE,
};
use crate::out_of_core::{OUT_OF_CORE_LAYOUT_FORMAT, OUT_OF_CORE_MANIFEST_FILE};
use crate::{
    SearchDocument, SearchOutOfCoreReader, SEARCH_SEGMENT_DESCRIPTOR_FILE,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use skein_executor::QueryMemoryLease;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

type Hook = Box<dyn FnOnce(&BuildMemory, &RuntimeTaskContext)>;
mod fuzz;
thread_local! {
    pub(super) static BEFORE: RefCell<Option<Hook>> = const { RefCell::new(None) };
    pub(super) static PREPARED: RefCell<Option<Hook>> = const { RefCell::new(None) };
    pub(super) static COMMITTED: RefCell<Option<Hook>> = const { RefCell::new(None) };
    static HELD: RefCell<Option<QueryMemoryLease>> = const { RefCell::new(None) };
    static NAMES: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn run(
    slot: &'static std::thread::LocalKey<RefCell<Option<Hook>>>,
    input: &PublishGenerationInput<'_>,
) {
    if let Some(hook) = slot.with_borrow_mut(Option::take) {
        hook(input.memory, input.task_context);
    }
}
pub(super) fn record_names() {
    NAMES.set(NAMES.get() + 1);
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
        "skein-publication-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
fn document(number: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{number}"),
        title: "graph".into(),
        content: "storage".into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}
fn fixture() -> (PathBuf, u64) {
    let root = root();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    let generation = writer.finish().unwrap().generation;
    (root, generation)
}
fn entries(root: &Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}

#[test]
fn original_names_are_preadmitted_and_retain_actual_capacities() {
    for generation in [0, 9, 10, u64::MAX] {
        for limit in [137 + 6 * 3 * 128 - 1, 137 + 6 * 3 * 128] {
            let memory = memory(limit);
            let other = memory.input.reserve(137).unwrap();
            NAMES.set(0);
            let result = paths::Names::new(generation, &memory, &RuntimeTaskContext::default());
            assert_eq!(result.is_ok(), limit == 137 + 6 * 3 * 128);
            assert_eq!(NAMES.get(), usize::from(result.is_ok()));
            if let Ok(names) = result {
                let expected = [
                    format!("search_projection_segments.{generation}.skein"),
                    format!("search_projection_segment_payloads.{generation}.skein"),
                    format!("search_projection_metadata_payloads.{generation}.skein"),
                    format!("search_projection_vector_payloads.{generation}.skein"),
                    format!("search_projection_out_of_core_layout.{generation}.skein"),
                    format!("search_lexical.manifest.{generation}.skein"),
                ];
                assert_eq!(
                    names.all().map(String::as_str),
                    expected.each_ref().map(String::as_str)
                );
                let bytes = expected.iter().map(String::capacity).sum::<usize>();
                let ledger = memory.ledger.clone();
                drop(other);
                drop(memory);
                assert_eq!(ledger.snapshot().used_bytes, bytes);
                drop(names);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            } else {
                assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            }
        }
    }
}

fn input<'a>(
    root: &'a Path,
    stage: &'a Path,
    memory: &'a BuildMemory,
    context: &'a RuntimeTaskContext,
    layout: &'a SearchOutOfCoreLayoutBody,
    lexical: &'a str,
    rabitq: Option<&'a RaBitQGenerationArtifact>,
) -> PublishGenerationInput<'a> {
    PublishGenerationInput {
        task_context: context,
        memory,
        root,
        stage,
        generation: layout.generation,
        document_count: 0,
        documents_digest: 0,
        source_graph_commit_epoch: None,
        import_source_graph_commit_epoch: None,
        embedding_manifest: None,
        embedding_dimension: None,
        layout,
        lexical_artifact_name: lexical,
        rabitq,
        payload_bytes: 0,
        metadata_payload_bytes: 0,
        vector_payload_bytes: 0,
        max_generation_bytes: u64::MAX,
    }
}

// Expected allocations use direct standard-library operations, independently of
// the production path owner and its admission formulas.
fn check_paths(
    input: &PublishGenerationInput<'_>,
    names: &paths::Names,
    paths: &paths::Paths,
    sequence: u64,
) -> usize {
    let transfers = [
        (
            &paths.descriptor,
            SEARCH_SEGMENT_DESCRIPTOR_FILE,
            names.descriptor.as_str(),
        ),
        (
            &paths.payload,
            SEARCH_SEGMENT_PAYLOAD_FILE,
            names.payload.as_str(),
        ),
        (
            &paths.metadata,
            STAGE_METADATA_FILE,
            names.metadata.as_str(),
        ),
        (&paths.vector, STAGE_VECTOR_FILE, names.vector.as_str()),
        (
            &paths.lexical,
            input.lexical_artifact_name,
            input.lexical_artifact_name,
        ),
        (
            &paths.lexical_manifest,
            LEXICAL_MANIFEST_FILE,
            names.lexical_manifest.as_str(),
        ),
    ];
    let mut bytes = 0;
    let mut next = sequence;
    for (transfer, source, destination) in
        transfers
            .into_iter()
            .chain(paths.rabitq.as_ref().map(|transfer| {
                let artifact = input.rabitq.unwrap();
                (
                    transfer,
                    artifact.file_name.as_str(),
                    artifact.file_name.as_str(),
                )
            }))
    {
        let source = input.stage.join(source);
        assert_eq!(transfer.source.as_ref(), source);
        bytes += source.capacity() + check_target(&transfer.target, input.root, destination, next);
        next = next.wrapping_add(1);
    }
    bytes += check_target(&paths.layout, input.root, &names.layout, next);
    bytes += check_target(
        &paths.manifest,
        input.root,
        OUT_OF_CORE_MANIFEST_FILE,
        next.wrapping_add(1),
    );
    bytes
}
fn check_target(target: &paths::Target, root: &Path, name: &str, sequence: u64) -> usize {
    let expected = root.join(name);
    let temporary = expected.with_extension(format!("tmp.{}.{sequence}", std::process::id()));
    assert_eq!(target.path.as_ref(), expected);
    assert_eq!(target.temporary.as_ref(), temporary);
    expected.capacity() + temporary.capacity()
}

#[test]
fn target_extension_formatting_and_path_overlap_are_admitted_before_commit() {
    let root = Path::new("parent/\u{130}");
    let name = "artifact.skein";
    let sequence = u64::MAX;
    let target = root.join(name);
    let extension = format!("tmp.{}.{sequence}", std::process::id());
    let temporary = target.with_extension(&extension);
    let join_peak = 3 * (root.as_os_str().as_encoded_bytes().len() + name.len() + 1).max(8);
    let temporary_peak =
        target.capacity() + 384 + target.as_os_str().as_encoded_bytes().len() + extension.len() + 1;
    let peak = 137 + join_peak.max(temporary_peak);
    for limit in [peak - 1, peak] {
        let memory = memory(limit);
        let other = memory.input.reserve(137).unwrap();
        let result = paths::Target::new(
            root,
            name,
            sequence,
            &memory,
            &RuntimeTaskContext::default(),
        );
        assert_eq!(result.is_ok(), limit == peak);
        if let Ok(owned) = result {
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                137 + target.capacity() + temporary.capacity()
            );
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            let ledger = memory.ledger.clone();
            drop(other);
            drop(memory);
            assert_eq!(
                ledger.snapshot().used_bytes,
                target.capacity() + temporary.capacity()
            );
            drop(owned);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        }
    }
}

fn path_trial(
    root: &Path,
    generation: u64,
    sequence: u64,
    include_vector: bool,
    limit: usize,
    succeeds: bool,
) -> usize {
    let stage = root.join("private-stage");
    let context = RuntimeTaskContext::default();
    let memory = memory(limit);
    let other = memory.input.reserve(137).unwrap();
    let names = paths::Names::new(generation, &memory, &context).unwrap();
    let names_bytes = memory.ledger.snapshot().used_bytes - 137;
    let layout = SearchOutOfCoreLayoutBody {
        format: OUT_OF_CORE_LAYOUT_FORMAT.into(),
        generation,
        document_count: 0,
        segments: Vec::new(),
    };
    let lexical = format!("search_lexical.{generation}.skein");
    let artifact = include_vector.then(|| RaBitQGenerationArtifact {
        file_name: crate::out_of_core::generation_writer::artifact_paths::Name::rabitq(
            generation, &memory, &context,
        )
        .unwrap(),
        artifact_bytes: 0,
        artifact_checksum: 0,
        source_digest: 0,
        document_count: 0,
        payload_checksum: 0,
        peak_build_working_bytes: 0,
    });
    let input = input(
        root,
        &stage,
        &memory,
        &context,
        &layout,
        &lexical,
        artifact.as_ref(),
    );
    let mut next = sequence;
    let paths = paths::Paths::prepare(&input, &names, &mut || {
        let current = next;
        next = next.wrapping_add(1);
        current
    });
    assert_eq!(
        paths.is_ok(),
        succeeds,
        "generation={generation} sequence={sequence} limit={limit}"
    );
    if let Ok(paths) = paths {
        let path_bytes = check_paths(&input, &names, &paths, sequence);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            137 + names_bytes
                + path_bytes
                + artifact
                    .as_ref()
                    .map_or(0, |artifact| artifact.file_name.capacity())
        );
        drop(paths);
    }
    let peak = memory.ledger.snapshot().peak_bytes;
    drop(names);
    drop(artifact);
    drop(other);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    peak
}

#[test]
fn complete_paths_share_exact_and_one_short_roots_without_filesystem_writes() {
    let root = root();
    for generation in [0, u64::MAX] {
        for sequence in [9, u64::MAX - 3] {
            for vector in [false, true] {
                let peak = path_trial(&root, generation, sequence, vector, 1024 * 1024, true);
                assert_eq!(
                    path_trial(&root, generation, sequence, vector, peak, true),
                    peak
                );
                path_trial(&root, generation, sequence, vector, peak - 1, false);
                assert!(!root.exists());
            }
        }
    }
}

fn occupy(memory: &BuildMemory, available: usize) {
    let snapshot = memory.ledger.snapshot();
    let lease = memory
        .retained
        .reserve(snapshot.budget_bytes - snapshot.used_bytes - available)
        .unwrap();
    HELD.with_borrow_mut(|held| *held = Some(lease));
}

#[test]
fn publication_denial_and_precommit_cancellation_preserve_the_active_generation() {
    for cancel in [false, true] {
        let (root, generation) = fixture();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let files = entries(&root);
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            task(16 * 1024 * 1024),
        )
        .unwrap();
        writer.push(document(1)).unwrap();
        let ledger = writer.memory.ledger.clone();
        if cancel {
            PREPARED.with_borrow_mut(|slot| {
                *slot = Some(Box::new(|_, task| {
                    task.cancellation().cancel();
                }))
            });
        } else {
            NAMES.set(0);
            BEFORE.with_borrow_mut(|slot| {
                *slot = Some(Box::new(|memory, _| occupy(memory, 6 * 3 * 128 - 1)))
            });
        }
        let error = writer.finish().unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancel { "cancelled" } else { "query memory" }),
            "{error}"
        );
        if !cancel {
            assert_eq!(NAMES.get(), 0);
        }
        HELD.with_borrow_mut(|slot| slot.take());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        assert_eq!(entries(&root), files);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation);
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
fn committed_publication_needs_no_new_path_admission_even_when_cancelled() {
    for cancel in [false, true] {
        let (root, generation) = fixture();
        let context = task(16 * 1024 * 1024);
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            Default::default(),
            context.clone(),
        )
        .unwrap();
        writer.push(document(1)).unwrap();
        let ledger = writer.memory.ledger.clone();
        COMMITTED.with_borrow_mut(|slot| {
            *slot = Some(Box::new(move |memory, task| {
                occupy(memory, 0);
                if cancel {
                    task.cancellation().cancel();
                }
            }))
        });
        let result = writer.finish();
        assert!(result.is_ok(), "{result:?}");
        let report = result.unwrap();
        assert_eq!(context.cancellation().is_cancelled(), cancel);
        assert_eq!(report.generation, generation + 1);
        HELD.with_borrow_mut(|slot| slot.take());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation + 1);
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
}

#[test]
fn failed_manifest_last_publication_keeps_the_old_generation_and_cleans_temporaries() {
    let (root, generation) = fixture();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let obstruction = root.join(format!(
        "search_projection_out_of_core_layout.{}.skein",
        generation + 1
    ));
    let blocked = obstruction.clone();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(1)).unwrap();
    let ledger = writer.memory.ledger.clone();
    COMMITTED.with_borrow_mut(|slot| {
        *slot = Some(Box::new(move |_, _| fs::create_dir(blocked).unwrap()))
    });
    assert!(writer.finish().is_err());
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert!(entries(&root).iter().all(|name| {
        let name = name.to_string_lossy();
        !name.contains(".tmp.") && !name.starts_with(".search-generation.")
    }));
    fs::remove_dir(obstruction).unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), generation);
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
fn borrowed_temporary_guards_clean_up_failed_link_and_write_replacements() {
    let root = root();
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source.skein");
    let target = root.join("target.skein");
    let temporary = root.join("temporary.skein");
    fs::write(&source, b"old contents").unwrap();
    fs::create_dir(&target).unwrap();
    assert!(crate::out_of_core::publish_generation_link_at(&source, &target, &temporary).is_err());
    assert!(!temporary.exists());
    assert!(
        crate::out_of_core::write_generation_artifact_at(&target, &temporary, b"new contents")
            .is_err()
    );
    assert!(!temporary.exists());
    assert_eq!(fs::read(&source).unwrap(), b"old contents");
    fs::remove_dir(&target).unwrap();
    crate::out_of_core::publish_generation_link_at(&source, &target, &temporary).unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"old contents");
    crate::out_of_core::write_generation_artifact_at(&target, &temporary, b"new contents").unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"new contents");
    assert_eq!(fs::read(&source).unwrap(), b"old contents");
    assert!(!temporary.exists());
    fs::remove_dir_all(root).unwrap();
}
