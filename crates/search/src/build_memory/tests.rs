use super::*;
use skein_core::RuntimeMemoryReservation;
use std::collections::BTreeMap;

fn document() -> SearchDocument {
    SearchDocument {
        id: "doc:1".to_string(),
        title: "title".to_string(),
        content: "content".to_string(),
        embedding: Some(vec![1.0, 0.0, 0.5]),
        metadata: BTreeMap::from([("key".to_string(), "value".to_string())]),
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
fn document_and_spool_leases_share_the_same_root_and_release_independently() {
    let document = document();
    let size = document_bytes(&document).unwrap();
    let memory = memory(size + 8);
    let owned = memory.admit_document(document).unwrap();
    assert_eq!(owned.retained_bytes(), size);
    let spool = memory.spool.reserve(8).unwrap();
    assert!(memory.retained.reserve(1).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, size + 8);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 8);
    let next = memory.retained.reserve(size).unwrap();
    drop((spool, next));
    let snapshot = memory.ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert_eq!(snapshot.peak_bytes, size + 8);
}

#[test]
fn owned_string_and_vector_spare_capacity_is_charged_not_only_length() {
    let mut input = document();
    input.content.reserve_exact(4096);
    input.embedding.as_mut().unwrap().reserve_exact(1024);
    let logical = document_bytes(&document()).unwrap();
    let actual = document_bytes(&input).unwrap();
    assert!(actual >= logical + 8192);
    let memory = memory(actual - 1);
    assert!(memory.admit_document(input).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn decoding_reserves_container_growth_before_the_decoder_allocates() {
    let input = document();
    let line = crate::encode_search_document_line(&input);
    let bound = decoded_document_bytes(&line, 1).unwrap();
    assert!(bound > line.len());
    let limited = memory(bound - 1);
    decode_evidence::take();
    assert!(limited.decode_document(&line, 1).is_err());
    assert_eq!(decode_evidence::take(), 0);
    assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    let admitted = memory(bound);
    let decoded = admitted.decode_document(&line, 1).unwrap();
    assert_eq!(decode_evidence::take(), 1);
    assert_eq!(decoded.document, input);
    assert_eq!(decoded.retained_bytes(), bound);
    drop(decoded);
    assert_eq!(admitted.ledger.snapshot().used_bytes, 0);
}

#[test]
fn empty_metadata_retains_a_root_node_allowance() {
    let mut input = document();
    input.metadata.pop_first().unwrap();
    assert!(input.metadata.is_empty());
    let expected = size_of::<SearchDocument>()
        + input.id.capacity()
        + input.title.capacity()
        + input.content.capacity()
        + input.embedding.as_ref().unwrap().capacity() * size_of::<f32>()
        + MAP_ENTRY_BYTES;
    assert_eq!(document_bytes(&input).unwrap(), expected);
    let line = crate::encode_search_document_line(&input);
    assert_eq!(decoded_document_bytes(&line, 0).unwrap(), expected);
    let limited = memory(expected - 1);
    assert!(limited.admit_document(input).is_err());
    decode_evidence::take();
    assert!(limited.decode_document(&line, 0).is_err());
    assert_eq!(decode_evidence::take(), 0);
    assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    let admitted = memory(expected);
    let owned = admitted.decode_document(&line, 0).unwrap();
    assert_eq!(owned.retained_bytes(), expected);
    drop(owned);
    assert_eq!(admitted.ledger.snapshot().used_bytes, 0);
}

#[test]
fn malformed_records_and_metadata_count_fail_without_a_leaked_charge() {
    let memory = memory(64 * 1024);
    let count_overflow = "doc\t61\t\t\t\t61=62;63=64\n";
    decode_evidence::take();
    assert!(memory.decode_document(count_overflow, 1).is_err());
    assert_eq!(decode_evidence::take(), 0);
    for corrupt in [
        "doc\t0\u{00e9}a\t\t\t\t\n",
        "doc\t61\t\t\t1,bad\t\n",
        "doc\t61\t\t\t\tgg=61\n",
    ] {
        assert!(memory.decode_document(corrupt, 2).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let duplicate = "doc\t61\t\t\t\t61=62;61=6364\n";
    let admitted = decoded_document_bytes(duplicate, 2).unwrap();
    let decoded = memory.decode_document(duplicate, 2).unwrap();
    assert!(decoded.retained_bytes() < admitted);
    assert_eq!(decoded.metadata["a"], "cd");
    drop(decoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn repeated_records_do_not_register_per_record_accounts() {
    let memory = memory(64 * 1024);
    for _ in 0..10_000 {
        let owned = memory.admit_document(document()).unwrap();
        drop(owned);
    }
    let snapshot = memory.ledger.snapshot();
    assert_eq!(snapshot.account_count, 3);
    assert_eq!(snapshot.used_bytes, 0);
}

#[test]
fn admission_zero_and_arithmetic_overflow_fail_closed() {
    assert!(BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(0, 1),)
    )
    .is_err());
    assert!(checked_add(usize::MAX, 1).is_err());
    assert!(checked_mul(usize::MAX, 2).is_err());
}

#[test]
fn a_cross_thread_document_owner_retains_capacity_until_it_drops() {
    let input = document();
    let size = document_bytes(&input).unwrap();
    let memory = memory(size);
    let document = memory.admit_document(input).unwrap();
    let (release, wait) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        wait.recv().unwrap();
        drop(document);
    });
    assert!(memory.input.reserve(1).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, size);
    release.send(()).unwrap();
    thread.join().unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
