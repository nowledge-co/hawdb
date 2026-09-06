use super::*;
use crate::query_memory::QueryMemory;
use crate::{encode_search_document_line, RuntimeCancellationToken, SearchIndex};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::out_of_core) struct Evidence {
    pub requests: usize,
    pub selections: usize,
    pub decodes: usize,
    pub selected: usize,
}
thread_local! {
    static EVIDENCE: Cell<Evidence> = Cell::default();
    static LAST_SELECTED: Cell<usize> = const { Cell::new(0) };
    static CANCEL_SELECTED: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
}
pub(super) fn record_requests() {
    let mut value = EVIDENCE.get();
    value.requests += 1;
    EVIDENCE.set(value);
}
pub(super) fn record_selection() {
    let mut value = EVIDENCE.get();
    value.selections += 1;
    EVIDENCE.set(value);
}
pub(super) fn record_decode() {
    let mut value = EVIDENCE.get();
    value.decodes += 1;
    EVIDENCE.set(value);
}
pub(super) fn record_selected(document: &SearchDocument) {
    let mut value = EVIDENCE.get();
    value.selected += 1;
    EVIDENCE.set(value);
    LAST_SELECTED.set(document.id.as_ptr() as usize);
    CANCEL_SELECTED.with_borrow_mut(|slot| {
        if let Some(token) = slot.take() {
            token.cancel();
        }
    });
}
pub(in crate::out_of_core) fn take() -> Evidence {
    EVIDENCE.replace(Evidence::default())
}

fn memory(bytes: usize) -> QueryMemory {
    QueryMemory::new(NonZeroU64::new(bytes as u64).unwrap(), None).unwrap()
}
fn document(id: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{id:03}"),
        title: "graph".to_owned(),
        content: format!("hydrate graph {id} \u{130}\0"),
        embedding: Some(vec![1.0, 0.5]),
        metadata: BTreeMap::from([("space_id".to_owned(), "team".to_owned())]),
    }
}
fn descriptor(documents: &[SearchDocument]) -> SearchSegmentDescriptorEntry {
    let fields = documents
        .iter()
        .flat_map(|document| document.metadata.keys().cloned())
        .collect::<BTreeSet<_>>();
    SearchSegmentDescriptorEntry::from_documents(0, &documents.iter().collect::<Vec<_>>(), &fields)
}
fn text(documents: &[SearchDocument]) -> String {
    let mut text = "SKEIN_SEARCH_SEGMENT_V1\n".to_owned();
    for document in documents {
        text.push_str(&encode_search_document_line(document));
    }
    text
}
fn fixture(name: &str, count: usize) -> (PathBuf, SearchOutOfCoreReader) {
    fixture_documents(name, (0..count).map(document))
}
fn fixture_documents(
    name: &str,
    documents: impl IntoIterator<Item = SearchDocument>,
) -> (PathBuf, SearchOutOfCoreReader) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "skein-hydration-{name}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let mut index = SearchIndex::open(&root).unwrap();
    for document in documents {
        index.upsert(document).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    (root, reader)
}
fn read(reader: &SearchOutOfCoreReader, ids: &[String], memory: &QueryMemory) -> Result<Documents> {
    load(
        reader,
        ids.iter().map(String::as_str),
        &memory.working,
        &RuntimeTaskContext::default(),
        &mut SearchOutOfCoreMetrics::default(),
    )
}
fn entries(root: &Path) -> BTreeSet<std::ffi::OsString> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect()
}

#[test]
fn decoded_documents_are_admitted_before_allocation_and_keep_exact_capacity() {
    let input = vec![document(0), document(1)];
    let descriptor = descriptor(&input);
    // Independent one-metadata-entry record envelope, including actual slots.
    let required: usize = input
        .iter()
        .map(|doc| {
            size_of::<SearchDocument>()
                + doc.id.len()
                + doc.title.len()
                + doc.content.len()
                + 8
                + 2048
                + "space_id".len()
                + "team".len()
        })
        .sum();
    for bound in [required - 1, required] {
        let memory = memory(bound + 137);
        let other = memory.scores.reserve(137).unwrap();
        take();
        let result = Segment::decode(
            &text(&input),
            &descriptor,
            &memory.working,
            &RuntimeTaskContext::default(),
        );
        assert_eq!(result.is_ok(), bound == required);
        assert_eq!(take().decodes, usize::from(bound == required));
        if let Ok(segment) = result {
            assert_eq!(segment.documents, input);
            assert_eq!(segment.documents.capacity(), input.len());
            assert_eq!(memory.ledger.snapshot().used_bytes, required + 137);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 2);
    }
    assert!(slots::<SearchDocument>(usize::MAX).is_err());
    assert!(slots::<u8>(isize::MAX as usize + 1).is_err());
}

#[test]
fn request_and_decode_phases_obey_independent_exact_and_short_overlap() {
    let (root, reader) = fixture("overlap", 2);
    let ids = [document(1).id, document(0).id];
    let request_bytes = 2 * size_of::<Request<'_>>();
    let output_bytes = 2 * size_of::<SearchDocument>() + size_of::<QueryMemoryLease>();
    let payload = reader.descriptor.segments[0].payload_range.unwrap();
    let text_bytes = text(&[document(0), document(1)]).len();
    let peak = 137
        + request_bytes
        + output_bytes
        + payload.length as usize
        + text_bytes
        + query_io::DECODE_WORKSPACE_BYTES;
    for bound in [peak - 1, peak] {
        let memory = memory(bound);
        let other = memory.scores.reserve(137).unwrap();
        query_io::evidence::take();
        take();
        let result = read(&reader, &ids, &memory);
        assert_eq!(result.is_ok(), bound == peak);
        assert_eq!(take().decodes, usize::from(bound == peak));
        assert_eq!(query_io::evidence::take(), (1, usize::from(bound == peak)));
        if let Ok(documents) = result {
            assert_eq!(&*documents, &[document(1), document(0)]);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(other);
    }
    take();
    query_io::evidence::take();
    assert!(read(&reader, &ids, &memory(request_bytes - 1)).is_err());
    assert_eq!(take(), Evidence::default());
    assert_eq!(query_io::evidence::take(), (0, 0));
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn selected_payloads_move_and_retain_owners_after_reader_and_query_drop() {
    let (root, reader) = fixture("owners", 9);
    let ids = [document(8).id, document(0).id, document(3).id];
    let memory = memory(4 * 1024 * 1024);
    let ledger = memory.ledger.clone();
    let documents = read(&reader, &ids, &memory).unwrap();
    assert_eq!(
        documents.documents[0].id.as_ptr() as usize,
        LAST_SELECTED.get()
    );
    assert_eq!(documents.documents.capacity(), 3);
    assert_eq!(documents.payloads.len(), 3);
    assert_eq!(documents.payloads.capacity(), 3);
    let retained = documents
        .iter()
        .map(|doc| document_bytes(doc).unwrap())
        .sum::<usize>()
        + 3 * size_of::<QueryMemoryLease>();
    assert_eq!(ledger.snapshot().used_bytes, retained);
    drop(reader);
    drop(memory);
    assert_eq!(&*documents, &[document(8), document(0), document(3)]);
    assert_eq!(ledger.snapshot().used_bytes, retained);
    drop(documents);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().account_count, 2);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bad_headers_counts_and_records_never_return_partial_documents() {
    let input = vec![document(0), document(1)];
    let descriptor = descriptor(&input);
    let source = text(&input);
    let memory = memory(4 * 1024 * 1024);
    for damaged in [
        source.replacen("SEGMENT_V1", "SEGMENT_V9", 1),
        text(&input[..1]),
        format!("{source}{}", encode_search_document_line(&document(2))),
        source.replacen("doc\t", "bad\t", 1),
    ] {
        take();
        assert!(Segment::decode(
            &damaged,
            &descriptor,
            &memory.working,
            &RuntimeTaskContext::default()
        )
        .is_err());
        assert_eq!(take().decodes, 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    for damaged in [
        source.replacen("doc\t6", "doc\tg", 1),
        text(&[document(1), document(0)]),
        text(&[document(0), document(0)]),
    ] {
        assert!(Segment::decode(
            &damaged,
            &descriptor,
            &memory.working,
            &RuntimeTaskContext::default()
        )
        .is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn empty_duplicate_missing_and_logical_limits_fail_without_publication_changes() {
    let (root, mut reader) = fixture("requests", 5);
    let manifest = fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let memory = memory(4 * 1024 * 1024);
    query_io::evidence::take();
    assert!(read(&reader, &[], &memory).unwrap().is_empty());
    for ids in [
        vec![document(0).id, document(0).id],
        vec!["missing".to_owned()],
    ] {
        assert!(read(&reader, &ids, &memory).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    assert_eq!(query_io::evidence::take(), (0, 0));
    reader.config.max_hydrated_bytes = NonZeroU64::MIN;
    assert!(read(&reader, &[document(0).id], &memory).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    reader.config.max_hydrated_documents = std::num::NonZeroUsize::MIN;
    take();
    query_io::evidence::take();
    assert!(read(&reader, &[document(0).id, document(1).id], &memory).is_err());
    assert_eq!(take(), Evidence::default());
    assert_eq!(query_io::evidence::take(), (0, 0));
    assert_eq!(
        fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        manifest
    );
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reopened
            .hydrate_documents(&[document(0).id])
            .unwrap()
            .documents,
        [document(0)]
    );
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_after_payload_move_releases_all_source_and_destination_owners() {
    let (root, reader) = fixture("cancel", 5);
    let ids = [document(0).id, document(4).id];
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let memory = memory(4 * 1024 * 1024);
    CANCEL_SELECTED.with_borrow_mut(|slot| *slot = Some(token));
    take();
    assert!(load(
        &reader,
        ids.iter().map(String::as_str),
        &memory.working,
        &task,
        &mut SearchOutOfCoreMetrics::default()
    )
    .is_err());
    assert_eq!(take().selected, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    take();
    query_io::evidence::take();
    assert!(load(
        &reader,
        ids.iter().map(String::as_str),
        &memory.working,
        &task,
        &mut SearchOutOfCoreMetrics::default()
    )
    .is_err());
    assert_eq!(take(), Evidence::default());
    assert_eq!(query_io::evidence::take(), (0, 0));
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn later_corrupt_segment_discards_earlier_selected_documents() {
    let (root, mut reader) = fixture("corrupt", 5);
    let memory = memory(4 * 1024 * 1024);
    let before = fs::read(root.join(crate::SEARCH_SEGMENT_PAYLOAD_FILE)).unwrap();
    reader.descriptor.segments[2]
        .payload_range
        .as_mut()
        .unwrap()
        .checksum ^= 1;
    take();
    assert!(read(&reader, &[document(0).id, document(4).id], &memory).is_err());
    assert_eq!(take().selected, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(
        fs::read(root.join(crate::SEARCH_SEGMENT_PAYLOAD_FILE)).unwrap(),
        before
    );
    drop(reader);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(&[document(4).id])
            .unwrap()
            .documents,
        [document(4)]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn direct_hydration_preserves_request_order_and_uses_admitted_segment_decoding() {
    let (root, reader) = fixture("direct", 5);
    take();
    let output = reader
        .hydrate_documents(&[document(4).id, document(0).id])
        .unwrap();
    assert_eq!(output.documents, [document(4), document(0)]);
    assert_eq!(
        take(),
        Evidence {
            requests: 1,
            selections: 1,
            decodes: 2,
            selected: 2
        }
    );
    assert_eq!(output.metrics.segment_range_reads, 2);
    assert_eq!(output.metrics.hydrated_documents, 2);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn delta_source_read_uses_build_admission_and_failure_preserves_publication() {
    let (root, reader) = fixture("delta", 2);
    let before = fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let before_entries = entries(&root);
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(64 * 1024, 0));
    take();
    query_io::evidence::take();
    let result = crate::SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        crate::SearchProjectionDelta::default(),
        Default::default(),
        task,
    );
    assert!(result.is_err());
    assert_eq!(take().decodes, 0);
    assert_eq!(query_io::evidence::take(), (1, 0));
    assert_eq!(entries(&root), before_entries);
    assert_eq!(
        fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reopened
            .hydrate_documents(&[document(0).id])
            .unwrap()
            .documents,
        [document(0)]
    );
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn build_consumer_shares_the_source_root_while_taking_document_ownership() {
    let (root, reader) = fixture("build-owner", 2);
    let task = RuntimeTaskContext::default().with_memory_reservation(
        skein_core::RuntimeMemoryReservation::new(4 * 1024 * 1024, 0),
    );
    let memory = crate::build_memory::BuildMemory::new(&task).unwrap();
    let other = memory.spool.reserve(137).unwrap();
    let source = reader
        .read_hydration_segment(
            &reader.descriptor.segments[0],
            &mut SearchOutOfCoreMetrics::default(),
            &memory.retained,
            &task,
        )
        .unwrap();
    let mut expected = memory.ledger.snapshot().used_bytes;
    let mut seen = 0;
    source
        .visit(&task, &mut |document| {
            assert_eq!(memory.ledger.snapshot().used_bytes, expected);
            let bytes = document_bytes(&document)?;
            let payload = payload_bytes(&document)?;
            let owned = memory.admit_document(document)?;
            assert_eq!(memory.ledger.snapshot().used_bytes, expected + bytes);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
            drop(owned);
            expected -= payload;
            seen += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 2);
    assert_eq!(memory.ledger.snapshot().used_bytes, 137);
    drop(other);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn query_hydration_respects_the_existing_task_root_after_vector_scoring() {
    let mut large = document(0);
    large.content = "payload ".repeat(8192);
    let (root, reader) = fixture_documents("query-root", [large, document(1)]);
    let before = fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let limit = query_io::DECODE_WORKSPACE_BYTES as u64 + 64 * 1024;
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(limit, limit));
    take();
    query_io::evidence::take();
    let result = reader.search_with_options_compressed_vector_projection_context(
        "",
        Some(&[1.0, 0.5]),
        crate::SearchMode::Vector,
        crate::SearchQueryOptions {
            limit: 1,
            offset: 0,
            rank_window: None,
            fusion_weights: Default::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        },
        crate::CompressedVectorSearchMode::Disabled,
        &task,
    );
    let error = result.unwrap_err();
    assert!(error.to_string().contains("query memory"), "{error}");
    assert_eq!(
        take(),
        Evidence {
            requests: 1,
            selections: 1,
            decodes: 0,
            selected: 0
        }
    );
    assert_eq!(query_io::evidence::take(), (2, 1));
    assert_eq!(
        fs::read(root.join(super::super::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
