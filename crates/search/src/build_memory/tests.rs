use super::*;
use skein_core::{RuntimeCancellationToken, RuntimeMemoryReservation};
use std::collections::BTreeMap;

fn document() -> SearchDocument {
    SearchDocument {
        id: "doc:1".into(),
        title: "title".into(),
        content: "content".into(),
        embedding: Some(vec![1.0, 0.0, 0.5]),
        metadata: BTreeMap::from([("key".into(), "value".into())]),
    }
}

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

#[test]
fn document_spool_and_retained_state_share_exact_root_admission() {
    let input = document();
    let size = document_bytes(&input).unwrap();
    let memory = memory(size + 8);
    let owned = memory.admit_document(input).unwrap();
    assert_eq!(owned.retained_bytes(), size);
    assert_eq!(owned.id, "doc:1");
    let spool = memory.spool.reserve(8).unwrap();
    assert!(memory.retained.reserve(1).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, size + 8);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 8);
    let retained = memory.retained.reserve(size).unwrap();
    drop((spool, retained));
    let snapshot = memory.ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert_eq!(snapshot.peak_bytes, size + 8);
}

#[test]
fn spare_string_vector_and_empty_tree_capacity_remain_charged() {
    let mut input = document();
    input.content.reserve_exact(4096);
    input.embedding.as_mut().unwrap().reserve_exact(1024);
    input.metadata.pop_first().unwrap();
    let expected = size_of::<SearchDocument>()
        + input.id.capacity()
        + input.title.capacity()
        + input.content.capacity()
        + input.embedding.as_ref().unwrap().capacity() * size_of::<f32>()
        + MAP_ENTRY_BYTES;
    assert_eq!(document_bytes(&input).unwrap(), expected);
    let memory = memory(expected - 1);
    assert!(memory.admit_document(input).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn repeated_records_keep_account_metadata_bounded() {
    let memory = memory(64 * 1024);
    for _ in 0..4096 {
        let owned = memory.clone().admit_document(document()).unwrap();
        assert_eq!(owned.retained_bytes(), memory.ledger.snapshot().used_bytes);
        drop(owned);
    }
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn a_worker_owner_keeps_capacity_unavailable_until_it_releases_the_document() {
    let input = document();
    let size = document_bytes(&input).unwrap();
    let memory = memory(size);
    let worker = memory.clone();
    let (held, ready) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let owned = worker.admit_document(input).unwrap();
        held.send(()).unwrap();
        released.recv().unwrap();
        drop(owned);
    });
    ready.recv().unwrap();
    let while_held = memory.spool.reserve(1);
    release.send(()).unwrap();
    handle.join().unwrap();
    assert!(while_held.is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(memory.spool.reserve(size).is_ok());
}

#[test]
fn stopped_context_zero_reservation_and_overflow_fail_closed() {
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let cancelled = RuntimeTaskContext::without_deadline(token);
    assert!(BuildMemory::new(&cancelled)
        .unwrap_err()
        .to_string()
        .contains("cancel"));
    let expired = RuntimeTaskContext::with_timeout(std::time::Duration::ZERO);
    assert!(BuildMemory::new(&expired).is_err());
    let zero = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(0, 4096));
    assert!(BuildMemory::new(&zero).is_err());
    assert!(checked_add(usize::MAX, 1).is_err());
    assert!(checked_mul(usize::MAX, 2).is_err());
    let ungoverned = BuildMemory::new(&RuntimeTaskContext::default()).unwrap();
    assert_eq!(ungoverned.ledger.snapshot().budget_bytes, usize::MAX);
}

#[test]
fn delta_source_identity_capacity_is_part_of_the_owned_input() {
    let row = SearchProjectionRow {
        kind: crate::SearchProjectionKind::Memory,
        external_id: "doc:1".into(),
        title: "title".into(),
        body: "body".into(),
        embedding: None,
        metadata: BTreeMap::new(),
        source_id: Some(String::with_capacity(4096)),
    };
    let expected = size_of::<SearchProjectionRow>()
        + row.external_id.capacity()
        + row.title.capacity()
        + row.body.capacity()
        + row.source_id.as_ref().unwrap().capacity()
        + MAP_ENTRY_BYTES;
    assert_eq!(projection_row_bytes(&row).unwrap(), expected);
}

#[test]
fn replacement_capacity_is_admitted_with_the_old_allocation_still_owned() {
    for limit in [23, 24] {
        let memory = memory(limit);
        let mut lease = memory.retained.reserve(0).unwrap();
        let mut values = Vec::<u8>::new();
        reserve_capacity(&mut values, 8, &mut lease).unwrap();
        values.extend_from_slice(b"original");
        let result = reserve_capacity(&mut values, 16, &mut lease);
        assert_eq!(result.is_ok(), limit == 24);
        assert_eq!(values, b"original");
        assert_eq!(values.capacity(), if limit == 24 { 16 } else { 8 });
        assert_eq!(lease.bytes(), values.capacity());
        assert_eq!(memory.ledger.snapshot().used_bytes, lease.bytes());
        drop(values);
        drop(lease);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
