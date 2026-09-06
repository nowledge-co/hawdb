use super::*;
use crate::{
    RuntimeCancellationToken, SearchAnalyzerAliasRule, SearchAnalyzerLexicon, SearchDocument,
};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

thread_local! {
    static IDENTITIES: Cell<usize> = const { Cell::new(0) };
    static PATHS: Cell<usize> = const { Cell::new(0) };
    static CANCEL_IDENTITY: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
    static CANCEL_PATH: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
}

pub(super) fn record_identity() {
    IDENTITIES.set(IDENTITIES.get() + 1);
    CANCEL_IDENTITY.with_borrow_mut(|value| {
        if let Some(token) = value.take() {
            token.cancel();
        }
    });
}
pub(super) fn record_path() {
    PATHS.set(PATHS.get() + 1);
    CANCEL_PATH.with_borrow_mut(|value| {
        if let Some(token) = value.take() {
            token.cancel();
        }
    });
}
fn take() -> (usize, usize) {
    (IDENTITIES.replace(0), PATHS.replace(0))
}

fn task(bytes: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(bytes as u64, 0))
}
fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(&task(bytes)).unwrap()
}
fn spare(value: &str, capacity: usize) -> String {
    let mut result = String::with_capacity(capacity);
    result.push_str(value);
    result
}
fn root() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-build-context-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn options() -> SearchOutOfCoreGenerationBuildOptions {
    let mut inputs = Vec::with_capacity(3);
    inputs.push(spare("graph", 1024));
    let mut aliases = Vec::with_capacity(7);
    aliases.push(spare("storage", 2048));
    let mut rules = Vec::with_capacity(5);
    rules.push(SearchAnalyzerAliasRule { inputs, aliases });
    SearchOutOfCoreGenerationBuildOptions {
        analyzer_lexicon: SearchAnalyzerLexicon {
            alias_rules: rules,
            stopwords: BTreeSet::from([spare("skip", 4096)]),
        },
        embedding_manifest: Some(SearchEmbeddingManifest {
            model: spare("model", 512),
            version: Some(spare("v1", 2048)),
            dimension: 2,
        }),
        ..Default::default()
    }
}

fn fixture() -> (PathBuf, SearchOutOfCoreReader) {
    let path = root();
    let mut writer = super::super::SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "model-\u{130}".into(),
                version: Some("v1".into()),
                dimension: 2,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    writer
        .push(SearchDocument {
            id: "memory:000".into(),
            title: "graph".into(),
            content: "storage".into(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    (path, reader)
}

#[test]
fn option_capacities_move_under_an_independent_exact_and_one_short_root() {
    let bytes = 5 * size_of::<SearchAnalyzerAliasRule>()
        + (3 + 7) * size_of::<String>()
        + 1024
        + 2048
        + 1024
        + 4096
        + 512
        + 2048;
    for limit in [137 + bytes - 1, 137 + bytes] {
        let memory = memory(limit);
        let other = memory.input.reserve(137).unwrap();
        let value = options();
        let pointers = (
            value.analyzer_lexicon.alias_rules.as_ptr(),
            value.embedding_manifest.as_ref().unwrap().model.as_ptr(),
        );
        let result = Options::new(value, &memory, &RuntimeTaskContext::default());
        assert_eq!(result.is_ok(), limit == 137 + bytes);
        if let Ok(options) = result {
            assert_eq!(
                (
                    options.analyzer_lexicon.alias_rules.as_ptr(),
                    options.embedding_manifest.as_ref().unwrap().model.as_ptr()
                ),
                pointers
            );
            assert_eq!(memory.ledger.snapshot().used_bytes, 137 + bytes);
            let ledger = memory.ledger.clone();
            drop(other);
            drop(memory);
            assert_eq!(ledger.snapshot().used_bytes, bytes);
            drop(options);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        }
    }
}

#[test]
fn empty_lexicon_retains_spare_rule_slots_and_a_possible_tree_leaf() {
    let mut lexicon = SearchAnalyzerLexicon {
        alias_rules: Vec::with_capacity(17),
        stopwords: BTreeSet::from(["removed".into()]),
    };
    lexicon.stopwords.remove("removed");
    let value = SearchOutOfCoreGenerationBuildOptions {
        analyzer_lexicon: lexicon,
        ..Default::default()
    };
    let bytes = 17 * size_of::<SearchAnalyzerAliasRule>() + 1024;
    let memory = memory(bytes);
    let owned = Options::new(value, &memory, &RuntimeTaskContext::default()).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn inherited_identity_is_admitted_before_one_copy_and_retained_after_binding() {
    let (path, reader) = fixture();
    let copy_bytes = "model-\u{130}".len() + "v1".len();
    for extra in [copy_bytes - 1, copy_bytes] {
        let value = SearchOutOfCoreGenerationBuildOptions::default();
        let raw = options_bytes(&value, &RuntimeTaskContext::default()).unwrap();
        let memory = memory(raw + extra + 137);
        let other = memory.input.reserve(137).unwrap();
        let mut owned = Options::new(value, &memory, &RuntimeTaskContext::default()).unwrap();
        take();
        let result = owned.bind_identity(&reader, Some(9), &memory, &RuntimeTaskContext::default());
        assert_eq!(result.is_ok(), extra == copy_bytes);
        assert_eq!(take(), (usize::from(extra == copy_bytes), 0));
        if result.is_ok() {
            assert_eq!(owned.embedding_manifest, reader.embedding_manifest());
            assert_eq!(owned.source_graph_commit_epoch, Some(9));
            assert_eq!(memory.ledger.snapshot().used_bytes, raw + copy_bytes + 137);
        } else {
            assert!(owned.embedding_manifest.is_none());
            assert!(owned.source_graph_commit_epoch.is_none());
            assert_eq!(memory.ledger.snapshot().used_bytes, raw + 137);
        }
        drop(owned);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
    }
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn matching_supplied_identity_keeps_its_allocation_without_comparison_copies() {
    let (path, reader) = fixture();
    let mut identity = reader.embedding_manifest().unwrap();
    identity.model.reserve_exact(4096);
    let pointer = identity.model.as_ptr();
    let value = SearchOutOfCoreGenerationBuildOptions {
        embedding_manifest: Some(identity),
        ..Default::default()
    };
    let raw = options_bytes(&value, &RuntimeTaskContext::default()).unwrap();
    let memory = memory(raw);
    let mut owned = Options::new(value, &memory, &RuntimeTaskContext::default()).unwrap();
    take();
    owned
        .bind_identity(&reader, Some(9), &memory, &RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(take(), (0, 0));
    assert_eq!(
        owned.embedding_manifest.as_ref().unwrap().model.as_ptr(),
        pointer
    );
    assert_eq!(memory.ledger.snapshot().peak_bytes, raw);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn identity_mismatches_do_not_copy_or_mutate_options() {
    let (path, reader) = fixture();
    for (change, expected) in [
        (0, "source graph epoch"),
        (1, "import provenance"),
        (2, "embedding identity"),
        (3, "analyzer"),
    ] {
        let mut value = SearchOutOfCoreGenerationBuildOptions::default();
        match change {
            0 => value.source_graph_commit_epoch = Some(8),
            1 => value.import_source_graph_commit_epoch = Some(8),
            2 => {
                value.embedding_manifest = Some(SearchEmbeddingManifest {
                    model: "other".into(),
                    version: None,
                    dimension: 2,
                })
            }
            _ => value.analyzer_lexicon = SearchAnalyzerLexicon::empty(),
        }
        let before = value.clone();
        let memory = memory(1024 * 1024);
        let mut owned = Options::new(value, &memory, &RuntimeTaskContext::default()).unwrap();
        let raw = memory.ledger.snapshot().used_bytes;
        take();
        let error = owned
            .bind_identity(&reader, Some(9), &memory, &RuntimeTaskContext::default())
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(*owned, before);
        assert_eq!(take(), (0, 0));
        assert_eq!(memory.ledger.snapshot().used_bytes, raw);
        drop(owned);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn cancellation_after_identity_copy_is_transactional_and_releases_temporary_charge() {
    let (path, reader) = fixture();
    let memory = memory(1024 * 1024);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let mut owned = Options::new(Default::default(), &memory, &task).unwrap();
    let raw = memory.ledger.snapshot().used_bytes;
    CANCEL_IDENTITY.with_borrow_mut(|value| *value = Some(token));
    take();
    assert!(owned
        .bind_identity(&reader, Some(9), &memory, &task)
        .is_err());
    assert_eq!(take(), (1, 0));
    assert!(owned.embedding_manifest.is_none());
    assert!(owned.source_graph_commit_epoch.is_none());
    assert_eq!(memory.ledger.snapshot().used_bytes, raw);
    assert!(Options::new(Default::default(), &memory, &task).is_err());
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn path_copy_and_join_reserve_before_allocation_and_retain_actual_capacity() {
    let parent = Path::new("parent/\u{130}/../child");
    let name = Path::new("file.skein");
    let bytes = parent.as_os_str().as_encoded_bytes().len();
    let join_peak = 3 * (bytes + 1 + name.as_os_str().as_encoded_bytes().len()).max(8);
    for (copy, peak) in [(true, bytes), (false, join_peak)] {
        for limit in [137 + peak - 1, 137 + peak] {
            let memory = memory(limit);
            let other = memory.input.reserve(137).unwrap();
            take();
            let result = if copy {
                OwnedPath::copy(parent, &memory, &RuntimeTaskContext::default())
            } else {
                OwnedPath::join(parent, name, &memory, &RuntimeTaskContext::default())
            };
            assert_eq!(result.is_ok(), limit == 137 + peak);
            assert_eq!(take(), (0, usize::from(limit == 137 + peak)));
            if let Ok(owned) = result {
                assert_eq!(
                    owned.as_ref(),
                    if copy {
                        parent.to_path_buf()
                    } else {
                        parent.join(name)
                    }
                );
                let retained = owned.value.capacity();
                assert_eq!(memory.ledger.snapshot().peak_bytes, 137 + peak);
                assert_eq!(memory.ledger.snapshot().used_bytes, 137 + retained);
                let ledger = memory.ledger.clone();
                drop(other);
                drop(memory);
                assert_eq!(ledger.snapshot().used_bytes, retained);
                drop(owned);
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }
    assert_eq!(
        join_bytes(10, 20, true).unwrap(),
        4 * 31 + 4 * 31 * size_of::<Component<'_>>()
    );
    assert!(join_bytes(usize::MAX, 1, false).is_err());
    assert!(join_bytes(usize::MAX / 8, 1, true).is_err());
}

#[test]
fn cancelled_path_copy_releases_its_allocation_and_charge() {
    let memory = memory(4096);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    CANCEL_PATH.with_borrow_mut(|value| *value = Some(token));
    take();
    assert!(OwnedPath::copy(Path::new("some/path"), &memory, &task).is_err());
    assert_eq!(take(), (0, 1));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[cfg(unix)]
#[test]
fn opaque_unix_path_bytes_are_not_lossily_converted() {
    use std::os::unix::ffi::OsStringExt;
    let parent = PathBuf::from(std::ffi::OsString::from_vec(b"base/\xff".to_vec()));
    let name = Path::new("file");
    let memory = memory(4096);
    let path = OwnedPath::join(&parent, name, &memory, &RuntimeTaskContext::default()).unwrap();
    assert_eq!(path.as_os_str().as_encoded_bytes(), b"base/\xff/file");
    drop(path);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[cfg(windows)]
#[test]
fn windows_verbatim_and_drive_paths_keep_standard_join_semantics() {
    use std::os::windows::ffi::OsStringExt;
    for (parent, verbatim) in [
        (PathBuf::from(r"\\?\C:\alpha\.\beta\.."), true),
        (PathBuf::from("C:"), false),
        (
            PathBuf::from(std::ffi::OsString::from_wide(&[0x61, 0xd800])),
            false,
        ),
    ] {
        let name = Path::new("child.skein");
        let length = parent.as_os_str().as_encoded_bytes().len()
            + name.as_os_str().as_encoded_bytes().len()
            + 1;
        let bytes = if verbatim {
            4 * length.max(8) + 4 * length.max(4) * size_of::<Component<'_>>()
        } else {
            3 * length.max(8)
        };
        for limit in [bytes - 1, bytes] {
            let memory = memory(limit);
            take();
            let result = OwnedPath::join(&parent, name, &memory, &RuntimeTaskContext::default());
            assert_eq!(result.is_ok(), limit == bytes);
            assert_eq!(take(), (0, usize::from(limit == bytes)));
            if let Ok(path) = result {
                assert_eq!(path.as_ref(), parent.join(name));
                assert_eq!(memory.ledger.snapshot().peak_bytes, bytes);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn exhausted_stage_name_admission_does_not_create_a_stage() {
    let path = root();
    let memory = memory(1024 * 1024);
    let value = Options::new(Default::default(), &memory, &RuntimeTaskContext::default()).unwrap();
    let initial = super::super::required_descriptor_field_names()
        .map(|field| 1024 + field.len())
        .sum::<usize>()
        + super::super::SPOOL_BUFFER_BYTES
        + path.as_os_str().as_encoded_bytes().len();
    let other = memory
        .input
        .reserve(1024 * 1024 - memory.ledger.snapshot().used_bytes - initial - 383)
        .unwrap();
    take();
    assert!(
        super::super::SearchOutOfCoreGenerationWriter::create_with_memory(
            &path,
            value,
            RuntimeTaskContext::default(),
            memory.clone()
        )
        .is_err()
    );
    assert_eq!(take(), (0, 1));
    assert!(path.is_dir());
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, other.bytes());
    drop(other);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn writer_admits_option_capacities_before_creating_the_root() {
    let path = root();
    let mut value = SearchOutOfCoreGenerationBuildOptions::default();
    value.analyzer_lexicon.alias_rules.reserve_exact(4096);
    take();
    assert!(
        super::super::SearchOutOfCoreGenerationWriter::create_with_context(
            &path,
            value,
            task(16 * 1024)
        )
        .is_err()
    );
    assert_eq!(take(), (0, 0));
    assert!(!path.exists());
}

#[test]
fn abandoned_delta_retains_context_until_drop_without_publishing() {
    let (path, reader) = fixture();
    let before = std::fs::read(path.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let mut identity = reader.embedding_manifest().unwrap();
    identity.model.reserve_exact(4096);
    let pointer = identity.model.as_ptr();
    let value = SearchOutOfCoreGenerationBuildOptions {
        embedding_manifest: Some(identity),
        ..Default::default()
    };
    let raw = options_bytes(&value, &RuntimeTaskContext::default()).unwrap();
    let update = super::super::SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        crate::SearchProjectionDelta {
            source_graph_commit_epoch: Some(9),
            ..Default::default()
        },
        value,
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let writer = update.writer_for_test();
    let ledger = writer.memory.ledger.clone();
    let stage = writer.stage.path.to_path_buf();
    assert!(stage.is_dir());
    assert_eq!(
        writer
            .options
            .embedding_manifest
            .as_ref()
            .unwrap()
            .model
            .as_ptr(),
        pointer
    );
    assert_eq!(
        ledger.snapshot().used_bytes,
        raw + writer.root.value.capacity()
            + writer.stage.path.value.capacity()
            + writer.spool_path.value.capacity()
            + writer.metadata_memory.bytes()
            + writer.spool_memory.as_ref().unwrap().bytes()
            + writer.last_id_memory.as_ref().unwrap().bytes()
    );
    drop(update);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(!stage.exists());
    assert_eq!(
        std::fs::read(path.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reopened.document_count(), 1);
    assert_eq!(reopened.source_graph_commit_epoch(), None);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn writer_retains_owned_context_and_spool_scan_borrows_its_path() {
    let path = root();
    let value = options();
    let raw = options_bytes(&value, &RuntimeTaskContext::default()).unwrap();
    let pointer = value.embedding_manifest.as_ref().unwrap().model.as_ptr();
    let writer = super::super::SearchOutOfCoreGenerationWriter::create(&path, value).unwrap();
    let ledger = writer.memory.ledger.clone();
    let path_bytes = writer.root.value.capacity()
        + writer.stage.path.value.capacity()
        + writer.spool_path.value.capacity();
    assert_eq!(
        writer
            .options
            .embedding_manifest
            .as_ref()
            .unwrap()
            .model
            .as_ptr(),
        pointer
    );
    assert_eq!(
        ledger.snapshot().used_bytes,
        raw + path_bytes
            + writer.metadata_memory.bytes()
            + writer.spool_memory.as_ref().unwrap().bytes()
    );
    let error = writer
        .finish_with_artifacts(|writer, source, _| {
            assert!(std::ptr::eq(source.path, writer.spool_path.as_ref()));
            Err(SkeinError::Execution("injected build failure".into()))
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("injected build failure"),
        "{error}"
    );
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(std::fs::read_dir(&path).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".search-generation.")));
    std::fs::remove_dir_all(path).unwrap();
}
