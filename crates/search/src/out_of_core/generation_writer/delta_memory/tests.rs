use super::*;
use crate::build_memory::SET_ENTRY_BYTES;
use crate::out_of_core::generation_writer::{
    required_descriptor_field_names, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use crate::{RuntimeCancellationToken, SearchIndex, SearchOutOfCoreReader, SearchProjectionKind};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

thread_local! {
    static CONVERSIONS: Cell<usize> = const { Cell::new(0) };
    static ROWS: Cell<usize> = const { Cell::new(0) };
    static CANCEL_ROW: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
}
pub(super) fn record_conversion() {
    CONVERSIONS.set(CONVERSIONS.get() + 1);
}
pub(super) fn record_row() {
    ROWS.set(ROWS.get() + 1);
    CANCEL_ROW.with_borrow_mut(|slot| {
        if let Some(token) = slot.take() {
            token.cancel();
        }
    });
}
fn take() -> (usize, usize) {
    (CONVERSIONS.replace(0), ROWS.replace(0))
}
fn task(bytes: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(bytes as u64, 0))
}
fn row(id: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.to_owned(),
        title: "graph".to_owned(),
        body: "delta \u{130}\0".to_owned(),
        embedding: None,
        source_id: Some("source".to_owned()),
        metadata: BTreeMap::from([("space_id".to_owned(), "team".to_owned())]),
    }
}
// Independent semantic reference: retained old keys, replaced values, optional
// source override, and the public kind-prefixed ID contract.
fn reference(row: &SearchProjectionRow) -> SearchDocument {
    let mut metadata = row.metadata.clone();
    metadata.insert("kind".to_owned(), row.kind.as_str().to_owned());
    metadata.insert("external_id".to_owned(), row.external_id.clone());
    if let Some(source) = &row.source_id {
        metadata.insert("source_id".to_owned(), source.clone());
    }
    SearchDocument {
        id: [row.kind.as_str(), ":", row.external_id.as_str()].concat(),
        title: row.title.clone(),
        content: row.body.clone(),
        embedding: row.embedding.clone(),
        metadata,
    }
}
fn simple_delta() -> SearchProjectionDelta {
    let mut upserts = Vec::with_capacity(7);
    let mut row = row("001");
    row.external_id.reserve_exact(91);
    row.title.reserve_exact(73);
    row.body.reserve_exact(55);
    row.source_id.as_mut().unwrap().reserve_exact(39);
    let mut embedding = Vec::with_capacity(11);
    embedding.extend([1.0, 0.5]);
    row.embedding = Some(embedding);
    upserts.push(row);
    let mut deletes = Vec::with_capacity(5);
    let mut id = "memory:002".to_owned();
    id.reserve_exact(61);
    deletes.push(id);
    SearchProjectionDelta {
        upserts,
        deletes,
        ..Default::default()
    }
}
fn independent_simple_peak(delta: &SearchProjectionDelta) -> usize {
    let row = &delta.upserts[0];
    delta.upserts.capacity() * size_of::<SearchProjectionRow>()
        + delta.deletes.capacity() * size_of::<String>()
        + size_of::<SearchDocument>()
        + row.external_id.capacity()
        + row.title.capacity()
        + row.body.capacity()
        + row.source_id.as_ref().unwrap().capacity()
        + row.embedding.as_ref().unwrap().capacity() * 4
        + 2048
        + "space_id".len()
        + "team".len()
        + "memory:001".len()
        + "kind".len()
        + "memory".len()
        + "external_id".len()
        + "source_id".len()
        + 3 * 2048
        + delta.deletes[0].capacity()
}
fn temp(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-delta-memory-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn fixture(name: &str) -> (PathBuf, SearchOutOfCoreReader) {
    let root = temp(name);
    let mut index = SearchIndex::open(&root).unwrap();
    for id in ["000", "002", "004", "008"] {
        index.upsert(reference(&row(id))).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    (root, reader)
}
fn entries(root: &Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}
fn manifest(root: &Path) -> Vec<u8> {
    fs::read(root.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap()
}

#[test]
fn delta_conversion_admits_spare_capacity_and_growth_before_allocating() {
    for exact in [false, true] {
        let delta = simple_delta();
        let peak = independent_simple_peak(&delta);
        let external = delta.upserts[0].external_id.as_ptr();
        let body = delta.upserts[0].body.as_ptr();
        let embedding = delta.upserts[0].embedding.as_ref().unwrap().as_ptr();
        let expected = reference(&delta.upserts[0]);
        let task = task(peak + 137 - usize::from(!exact));
        let memory = BuildMemory::new(&task).unwrap();
        let other = memory.spool.reserve(137).unwrap();
        take();
        let result = Input::new(delta, &memory, u64::MAX, &task);
        assert_eq!(result.is_ok(), exact);
        assert_eq!(take(), if exact { (1, 1) } else { (0, 0) });
        if let Ok(input) = result {
            assert_eq!(input.upserts, [expected]);
            assert_eq!(input.upserts.capacity(), 1);
            assert_eq!(input.deletes.capacity(), 5);
            assert_eq!(input.upserts[0].metadata["external_id"].as_ptr(), external);
            assert_eq!(input.upserts[0].content.as_ptr(), body);
            assert_eq!(
                input.upserts[0].embedding.as_ref().unwrap().as_ptr(),
                embedding
            );
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak + 137);
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                input.retained_bytes(&task).unwrap() + 137
            );
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    assert!(slots::<SearchDocument>(usize::MAX).is_err());
    assert!(slots::<u8>(isize::MAX as usize + 1).is_err());
}

#[test]
fn delta_component_limit_covers_the_conversion_peak_independently_of_root() {
    for exact in [false, true] {
        let delta = simple_delta();
        let peak = independent_simple_peak(&delta);
        let task = task(4 * 1024 * 1024);
        let memory = BuildMemory::new(&task).unwrap();
        take();
        let result = Input::new(delta, &memory, (peak - usize::from(!exact)) as u64, &task);
        assert_eq!(result.is_ok(), exact);
        assert_eq!(take(), if exact { (1, 1) } else { (0, 0) });
        drop(result);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn delta_conversion_preserves_kinds_replacement_metadata_and_ascending_consumption() {
    let mut upserts = Vec::new();
    for kind in [
        SearchProjectionKind::SourceChunk,
        SearchProjectionKind::Entity,
        SearchProjectionKind::Memory,
        SearchProjectionKind::Message,
        SearchProjectionKind::Community,
        SearchProjectionKind::Source,
    ] {
        for with_source in [false, true] {
            let mut row = row(if with_source { "\u{130}\0" } else { "" });
            row.kind = kind;
            row.metadata.extend([
                ("kind".to_owned(), "old".to_owned()),
                ("external_id".to_owned(), "old-external".to_owned()),
                ("source_id".to_owned(), "old-source".to_owned()),
            ]);
            if !with_source {
                row.source_id = None;
            }
            upserts.push(row);
        }
    }
    let mut expected = upserts.iter().map(reference).collect::<Vec<_>>();
    expected.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    let task = task(4 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let mut input = Input::new(
        SearchProjectionDelta {
            upserts,
            deletes: vec!["z".to_owned(), "a".to_owned()],
            ..Default::default()
        },
        &memory,
        u64::MAX,
        &task,
    )
    .unwrap();
    for expected in expected {
        assert_eq!(input.upsert_id(), Some(expected.id.as_str()));
        input
            .consume_upsert(|actual| {
                assert_eq!(actual, expected);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(input.upsert_id(), None);
    assert_eq!(input.delete_id(), Some("a"));
    input.discard_delete();
    assert_eq!(input.delete_id(), Some("z"));
    input.discard_delete();
    drop(input);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn delta_consumption_retains_source_during_callback_and_vector_slots_until_drop() {
    let task = task(4 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let ledger = memory.ledger.clone();
    let mut input = Input::new(simple_delta(), &memory, u64::MAX, &task).unwrap();
    let before = ledger.snapshot().used_bytes;
    let payload = document_bytes(&input.upserts[0]).unwrap() - size_of::<SearchDocument>();
    input
        .consume_upsert(|document| {
            assert_eq!(ledger.snapshot().used_bytes, before);
            let bytes = document_bytes(&document)?;
            let owned = memory.admit_document(document)?;
            assert_eq!(ledger.snapshot().used_bytes, before + bytes);
            drop(owned);
            Ok(())
        })
        .unwrap();
    assert_eq!(ledger.snapshot().used_bytes, before - payload);
    drop(memory);
    assert_eq!(ledger.snapshot().used_bytes, before - payload);
    input.discard_delete();
    assert_eq!(
        ledger.snapshot().used_bytes,
        size_of::<SearchDocument>() + 5 * size_of::<String>()
    );
    drop(input);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().account_count, 3);
}

#[test]
fn delta_cancellation_and_invalid_ids_release_every_input_owner() {
    let token = RuntimeCancellationToken::new();
    let task = task(4 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let cancelled = RuntimeTaskContext::without_deadline(token.clone());
    CANCEL_ROW.with_borrow_mut(|slot| *slot = Some(token));
    take();
    assert!(Input::new(
        SearchProjectionDelta {
            upserts: vec![row("a"), row("b")],
            ..Default::default()
        },
        &memory,
        u64::MAX,
        &cancelled
    )
    .is_err());
    assert_eq!(take(), (1, 1));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    take();
    assert!(Input::new(simple_delta(), &memory, u64::MAX, &cancelled).is_err());
    assert_eq!(take(), (0, 0));
    for (upserts, deletes) in [
        (vec![row("a"), row("a")], vec![]),
        (vec![], vec!["x".to_owned(), "x".to_owned()]),
        (vec![row("a")], vec!["memory:a".to_owned()]),
        (vec![], vec![String::new()]),
    ] {
        assert!(Input::new(
            SearchProjectionDelta {
                upserts,
                deletes,
                ..Default::default()
            },
            &memory,
            u64::MAX,
            &task
        )
        .is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let mut input = Input::new(simple_delta(), &memory, u64::MAX, &task).unwrap();
    assert!(input
        .consume_upsert(|_| Err(SkeinError::Execution("consumer failed".to_owned())))
        .is_err());
    drop(input);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn delta_spare_input_denial_precedes_staging_and_source_io() {
    let (root, reader) = fixture("spare");
    let before = manifest(&root);
    let before_entries = entries(&root);
    let delta = SearchProjectionDelta {
        upserts: Vec::with_capacity(8192),
        ..Default::default()
    };
    take();
    crate::out_of_core::query_io::evidence::take();
    let result = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta,
        Default::default(),
        task(64 * 1024),
    );
    assert!(result.is_err());
    assert_eq!(take(), (0, 0));
    assert_eq!(crate::out_of_core::query_io::evidence::take(), (0, 0));
    assert_eq!(manifest(&root), before);
    assert_eq!(entries(&root), before_entries);
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.document_count(), 4);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn delta_and_writer_startup_share_the_root_before_staging() {
    let (root, reader) = fixture("startup");
    let before = manifest(&root);
    let before_entries = entries(&root);
    let delta = simple_delta();
    let expected = reference(&delta.upserts[0]);
    let retained = document_bytes(&expected).unwrap() + delta.upserts[0].external_id.capacity()
        - expected.metadata["external_id"].capacity()
        + delta.upserts[0].title.capacity()
        - expected.title.capacity()
        + delta.upserts[0].body.capacity()
        - expected.content.capacity()
        + delta.upserts[0].source_id.as_ref().unwrap().capacity()
        - expected.metadata["source_id"].capacity()
        + (delta.upserts[0].embedding.as_ref().unwrap().capacity()
            - expected.embedding.as_ref().unwrap().capacity())
            * 4
        + delta.deletes.capacity() * size_of::<String>()
        + delta.deletes[0].capacity();
    let writer = 8192
        + required_descriptor_field_names()
            .map(|field| SET_ENTRY_BYTES + field.len())
            .sum::<usize>();
    assert!(retained + writer - 1 > independent_simple_peak(&delta));
    take();
    crate::out_of_core::query_io::evidence::take();
    let result = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta,
        Default::default(),
        task(retained + writer - 1),
    );
    assert!(result.is_err());
    assert_eq!(take(), (1, 1));
    assert_eq!(crate::out_of_core::query_io::evidence::take(), (0, 0));
    assert_eq!(manifest(&root), before);
    assert_eq!(entries(&root), before_entries);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn delta_merge_preserves_publication_until_finish_and_matches_document_oracle() {
    let (root, reader) = fixture("merge");
    let before = manifest(&root);
    let delta = SearchProjectionDelta {
        upserts: vec![row("999"), row("004"), row("001")],
        deletes: vec![
            "memory:zzz".to_owned(),
            "memory:003".to_owned(),
            "memory:000".to_owned(),
        ],
        source_graph_commit_epoch: Some(19),
        ..Default::default()
    };
    let mut expected = ["000", "002", "004", "008"]
        .into_iter()
        .map(|id| {
            let doc = reference(&row(id));
            (doc.id.clone(), doc)
        })
        .collect::<BTreeMap<_, _>>();
    for id in &delta.deletes {
        expected.remove(id);
    }
    for row in &delta.upserts {
        let doc = reference(row);
        expected.insert(doc.id.clone(), doc);
    }
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta,
        Default::default(),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    assert_eq!(manifest(&root), before);
    assert_eq!(update.delta_report().deleted_documents, 1);
    assert_eq!(update.delta_report().after_document_count, expected.len());
    let (_, report, _) = update.finish().unwrap();
    assert_eq!(report.document_count, expected.len());
    assert_ne!(manifest(&root), before);
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    let ids = expected.keys().cloned().collect::<Vec<_>>();
    assert_eq!(
        reopened.hydrate_documents(&ids).unwrap().documents,
        expected.into_values().collect::<Vec<_>>()
    );
    assert_eq!(reopened.source_graph_commit_epoch(), Some(19));
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn delta_failures_before_and_during_source_scan_preserve_publication_and_staging() {
    let (root, reader) = fixture("rejected");
    let before = manifest(&root);
    let before_entries = entries(&root);
    for failure in ["duplicate", "cancel", "writer"] {
        let mut delta = SearchProjectionDelta {
            upserts: vec![row("001")],
            ..Default::default()
        };
        let mut options = SearchOutOfCoreGenerationBuildOptions::default();
        let mut context = task(4 * 1024 * 1024);
        match failure {
            "duplicate" => delta.upserts.push(row("001")),
            "cancel" => {
                let token = RuntimeCancellationToken::new();
                context = RuntimeTaskContext::without_deadline(token.clone());
                CANCEL_ROW.with_borrow_mut(|slot| *slot = Some(token));
            }
            "writer" => options.max_documents = std::num::NonZeroUsize::MIN,
            _ => unreachable!(),
        }
        crate::out_of_core::query_io::evidence::take();
        assert!(
            SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                &reader, delta, options, context
            )
            .is_err(),
            "{failure}"
        );
        assert_eq!(
            crate::out_of_core::query_io::evidence::take(),
            if failure == "writer" { (1, 1) } else { (0, 0) },
            "{failure}"
        );
        assert_eq!(manifest(&root), before);
        assert_eq!(entries(&root), before_entries);
    }
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reopened
            .hydrate_documents(&["memory:000".to_owned()])
            .unwrap()
            .documents,
        [reference(&row("000"))]
    );
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}
