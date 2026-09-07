use super::*;
use crate::build_memory::path::evidence as path_evidence;
use crate::build_memory::SPOOL_BUFFER_BYTES;
use crate::lexical_projection::{
    self as lexical, ArtifactBuilder, LexicalProjectionWriter, SpillRuns,
};
use crate::SearchAnalyzerLexicon;
use std::fs;
use std::path::{Component, PathBuf};

mod fuzz;

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(&task(bytes)).unwrap()
}

fn task(bytes: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(bytes as u64, 0))
}

fn root(name: &str) -> PathBuf {
    lexical::tests::projection_root(name)
}

// Existing phase-denial tests include these retained allocations without moving
// their failure to the earlier, independently tested path-admission boundary.
pub(in crate::lexical_projection) fn publication_bytes(root: &Path, generation: u64) -> usize {
    let name = artifact_file(generation);
    let artifact = root.join(&name);
    let manifest = root.join(MANIFEST_FILE);
    name.capacity()
        + FORMAT.len()
        + LAYOUT.len()
        + artifact.capacity()
        + artifact.with_extension("skein.tmp").capacity()
        + manifest.capacity()
        + manifest.with_extension("skein.tmp").capacity()
}

fn join_peak(parent: &Path, name: &Path) -> usize {
    let length =
        parent.as_os_str().as_encoded_bytes().len() + name.as_os_str().as_encoded_bytes().len() + 1;
    if matches!(parent.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim())
    {
        4 * length.max(8) + 4 * length.max(4) * size_of::<Component<'_>>()
    } else {
        3 * length.max(8)
    }
}

fn publication_case(parent: &Path, generation: u64) {
    let name = format!("search_lexical.{generation}.skein");
    let artifact = parent.join(&name);
    let manifest = parent.join("search_lexical.manifest.skein");
    let temporary = artifact.with_extension("skein.tmp");
    let manifest_temporary = manifest.with_extension("skein.tmp");
    let expected = [&artifact, &temporary, &manifest, &manifest_temporary];
    let bounds = [
        join_peak(parent, Path::new(&name)),
        artifact.as_os_str().as_encoded_bytes().len() + "skein.tmp".len() + 1,
        join_peak(parent, Path::new("search_lexical.manifest.skein")),
        manifest.as_os_str().as_encoded_bytes().len() + "skein.tmp".len() + 1,
    ];
    let capacities = [
        artifact.capacity(),
        artifact.with_extension("skein.tmp").capacity(),
        manifest.capacity(),
        manifest.with_extension("skein.tmp").capacity(),
    ];
    let mut retained = 137;
    let mut peak = 0;
    for (capacity, bound) in capacities.into_iter().zip(bounds) {
        peak = peak.max(retained + bound);
        retained += capacity;
    }
    for limit in [peak - 1, peak] {
        let memory = memory(limit);
        let other = memory.input.reserve(137).unwrap();
        path_evidence::take();
        let result = Publication::new(parent, &name, &memory, &RuntimeTaskContext::default());
        assert_eq!(result.is_ok(), limit == peak);
        if let Ok(paths) = result {
            assert_eq!(path_evidence::take(), 4);
            for (actual, expected) in [
                &paths.artifact,
                &paths.temporary,
                &paths.manifest,
                &paths.manifest_temporary,
            ]
            .into_iter()
            .zip(expected)
            {
                assert_eq!(actual.as_ref(), expected);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, retained);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            let ledger = memory.ledger.clone();
            drop(other);
            drop(memory);
            assert_eq!(ledger.snapshot().used_bytes, retained - 137);
            drop(paths);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert!(path_evidence::take() < 4);
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        }
    }
}

#[test]
fn publication_paths_preadmit_each_native_allocation_and_retain_its_capacity() {
    for parent in [
        Path::new(""),
        Path::new("parent/\u{130}/../data"),
        Path::new("base.with.extension"),
    ] {
        for generation in [0, 9, 10, u64::MAX] {
            publication_case(parent, generation);
        }
    }
}

#[test]
fn manifest_names_transfer_their_charge_with_the_serialized_body() {
    let peak = 137 + 384 + "SKEIN_LEXICAL_MANIFEST_V1".len() + "SKEIN_LEXICAL_COMPACT_V1".len();
    for generation in [0, u64::MAX] {
        for limit in [peak - 1, peak] {
            let memory = memory(limit);
            let other = memory.spool.reserve(137).unwrap();
            evidence::take();
            let result = Names::new(generation, &memory, &RuntimeTaskContext::default());
            assert_eq!(result.is_ok(), limit == peak);
            assert_eq!(evidence::take(), (usize::from(limit == peak), 0));
            if let Ok(names) = result {
                assert_eq!(names.artifact, format!("search_lexical.{generation}.skein"));
                let bytes =
                    names.format.capacity() + names.layout.capacity() + names.artifact.capacity();
                let manifest = names.into_manifest(|format, layout, artifact_file| ManifestBody {
                    format,
                    layout,
                    artifact_file,
                    generation,
                    source_graph_commit_epoch: None,
                    analyzer_digest: 0,
                    documents_digest: 0,
                    artifact_len: 24,
                    artifact_checksum: 0,
                    byte_counters: lexical::SearchLexicalArtifactBytes {
                        header_bytes: 24,
                        ..Default::default()
                    },
                    document_count: 0,
                    total_document_len: 0,
                    posting_count: 0,
                    posting_offset: 24,
                    posting_bytes: 0,
                    dictionaries: Vec::new(),
                    blocks: Vec::new(),
                });
                drop(other);
                let ledger = memory.ledger.clone();
                drop(memory);
                assert_eq!(ledger.snapshot().used_bytes, bytes);
                assert!(manifest.body.encode_bounded(u64::MAX).is_ok());
                drop(manifest);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }
}

#[test]
fn cancellation_after_name_allocation_releases_every_name() {
    let memory = memory(4096);
    let task = task(4096);
    evidence::take();
    evidence::cancel_next(&task);
    assert!(Names::new(7, &memory, &task).is_err());
    assert_eq!(evidence::take(), (1, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn run_registry_growth_admits_old_and_new_slots_before_moving_cleanup() {
    let root = root("run-registry-growth");
    fs::create_dir(&root).unwrap();
    let paths: Vec<_> = (0..5).map(|i| root.join(format!("run{i}.tmp"))).collect();
    let bytes: usize = paths
        .iter()
        .map(|p| p.as_os_str().as_encoded_bytes().len())
        .sum();
    let peak = 12 * size_of::<OwnedPath>() + bytes;
    for (limit, cancelled) in [(peak - 1, false), (peak, false), (peak, true)] {
        let memory = memory(limit);
        let task = task(limit);
        let mut runs = Runs::new(&memory).unwrap();
        for path in &paths[..4] {
            runs.reserve_one(&memory, &task).unwrap();
            let owned = OwnedPath::copy(path, &memory, &task).unwrap();
            fs::write(path, b"owned").unwrap();
            runs.push_reserved(owned);
        }
        let fifth = OwnedPath::copy(&paths[4], &memory, &task).unwrap();
        evidence::take();
        if cancelled {
            evidence::cancel_next(&task);
        }
        let result = runs.reserve_one(&memory, &task);
        let succeeds = limit == peak && !cancelled;
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(evidence::take(), (0, usize::from(limit == peak)));
        if succeeds {
            fs::write(&paths[4], b"owned").unwrap();
            runs.push_reserved(fifth);
        } else {
            assert_eq!(runs.len(), 4);
            assert_eq!(fs::read_dir(&root).unwrap().count(), 4);
            drop(fifth);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, runs.retained_bytes());
        drop(runs);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn run_path_denial_does_not_advance_sequence_or_create_a_file() {
    let root = root("run-path-denial");
    fs::create_dir(&root).unwrap();
    let memory = memory(4096);
    let mut runs = SpillRuns::new(&root, u64::MAX, Default::default(), memory.clone()).unwrap();
    let competing = memory.input.reserve(4096 - 383).unwrap();
    evidence::take();
    assert!(runs.next_path().is_err());
    assert_eq!(evidence::take(), (0, 0));
    assert_eq!(runs.sequence, 0);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    drop(competing);
    let path = runs.next_path().unwrap();
    assert_eq!(
        path.as_ref(),
        root.join(".search-lexical.18446744073709551615.0.tmp")
    );
    assert_eq!(runs.sequence, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, path.capacity());
    drop(path);
    drop(runs);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}

fn sidecar(
    path: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    dictionary: bool,
) -> Result<()> {
    let path = OwnedPath::copy(path, memory, task)?;
    sidecar_owned(path, memory, task, dictionary)
}

fn sidecar_owned(
    path: OwnedPath,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    dictionary: bool,
) -> Result<()> {
    let spill = lexical::dictionary_store::SpillBudget::new(0, u64::MAX);
    if dictionary {
        let writer = lexical::dictionary_store::Writer::new_with_context(
            path,
            Default::default(),
            spill,
            lexical::dictionary_store::DirectoryBudget::new(u64::MAX),
            memory.clone(),
            task.clone(),
        )?;
        drop(writer);
    } else {
        let writer = lexical::doclist::Writer::new_with_context(
            path,
            spill,
            65536,
            memory.clone(),
            task.clone(),
        )?;
        drop(writer);
    }
    Ok(())
}

#[test]
fn sidecar_admission_cancellation_and_collision_never_remove_unowned_files() {
    let root = root("sidecar-path-ownership");
    fs::create_dir(&root).unwrap();
    let path = root.join("sidecar.tmp");
    let bytes = path.as_os_str().as_encoded_bytes().len();
    for dictionary in [false, true] {
        for limit in [bytes - 1, bytes] {
            let memory = memory(limit);
            assert_eq!(
                sidecar(&path, &memory, &task(limit), dictionary).is_ok(),
                limit == bytes
            );
            assert!(!path.exists());
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
        let memory = memory(4096);
        let task = task(4096);
        path_evidence::cancel_next(task.cancellation().clone());
        assert!(sidecar(&path, &memory, &task, dictionary).is_err());
        assert!(!path.exists());
        let owned = OwnedPath::copy(&path, &memory, &RuntimeTaskContext::default()).unwrap();
        assert!(sidecar_owned(owned, &memory, &task, dictionary).is_err());
        assert!(!path.exists());
        fs::write(&path, b"existing sidecar").unwrap();
        assert!(sidecar(&path, &memory, &RuntimeTaskContext::default(), dictionary).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing sidecar");
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        fs::remove_file(&path).unwrap();
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn artifact_constructor_borrows_the_admitted_path_and_checks_context_before_io() {
    let root = root("lexical-borrowed-artifact");
    fs::create_dir(&root).unwrap();
    let path = root.join("artifact.tmp");
    fs::write(&path, b"old payload").unwrap();
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let memory = memory(path_bytes + SPOOL_BUFFER_BYTES);
    let task = task(path_bytes + SPOOL_BUFFER_BYTES);
    let owned = OwnedPath::copy(&path, &memory, &task).unwrap();
    task.cancellation().cancel();
    assert!(
        ArtifactBuilder::new_with_context(&owned, 1, Default::default(), memory.clone(), task)
            .is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), b"old payload");
    let writer = ArtifactBuilder::new_with_context(
        &owned,
        1,
        Default::default(),
        memory.clone(),
        RuntimeTaskContext::default(),
    )
    .unwrap();
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        path_bytes + SPOOL_BUFFER_BYTES
    );
    drop(writer);
    assert_eq!(memory.ledger.snapshot().used_bytes, path_bytes);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_file(path).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn committed_manifest_publication_needs_no_new_path_budget_or_checkpoint() {
    let root = root("lexical-path-publication-gate");
    fs::create_dir(&root).unwrap();
    let document = crate::SearchDocument {
        id: "document".into(),
        title: "".into(),
        content: "graph".into(),
        embedding: None,
        metadata: Default::default(),
    };
    let old = LexicalProjectionWriter::new(Default::default())
        .write(
            &root,
            6,
            None,
            11,
            13,
            std::iter::once(&document),
            &SearchAnalyzerLexicon::empty(),
        )
        .unwrap();
    let before = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let task = task(4 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    evidence::pressure_at_gate();
    let reader = LexicalProjectionWriter::new(Default::default())
        .with_context(task.clone())
        .with_memory(memory.clone())
        .write(
            &root,
            7,
            None,
            11,
            13,
            std::iter::once(&document),
            &SearchAnalyzerLexicon::empty(),
        )
        .unwrap();
    assert!(task.cancellation().is_cancelled());
    assert_eq!(reader.manifest.generation, 7);
    assert_eq!(reader.manifest.document_count, 1);
    assert_eq!(old.manifest.generation, 6);
    assert_ne!(fs::read(root.join(MANIFEST_FILE)).unwrap(), before);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 3);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    drop(old);
    let reader = lexical::LexicalProjectionReader::load_named_with_cache(
        &root,
        MANIFEST_FILE,
        None,
        11,
        13,
        Default::default(),
        std::sync::Arc::new(skein_storage::SegmentCache::new(1024 * 1024)),
    )
    .unwrap()
    .unwrap();
    assert_eq!(reader.manifest.generation, 7);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
