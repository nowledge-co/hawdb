use super::*;
use crate::RuntimeTaskContext;
use skein_core::RuntimeMemoryReservation;
use std::collections::{BTreeMap, HashMap};

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

#[test]
#[expect(
    clippy::mutable_key_type,
    reason = "Only immutable term bytes participate in Eq, Ord and Hash."
)]
fn shared_term_ownership_survives_dedup_and_frequency_consumers() {
    let text = "retained term";
    let bytes = text.len() + size_of::<Payload>() + 2 * size_of::<usize>();
    let memory = memory(bytes);
    let term = Term::copy(text, Some(&memory)).unwrap();
    let address = term.as_ptr();
    let mut dedup = HashMap::new();
    dedup.insert(term.clone(), ());
    let mut frequencies = BTreeMap::new();
    frequencies.insert(term.clone(), 3u32);
    assert_eq!(term.clone_bytes(), 0);
    assert_eq!(term.capacity(), text.len());
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    drop(term);
    drop(dedup);
    let (term, frequency) = frequencies.pop_first().unwrap();
    assert_eq!(frequency, 3);
    assert_eq!(term.as_ptr(), address);
    let consumer = term.clone();
    drop(term);
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    assert_eq!(consumer.as_str(), text);
    drop(consumer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn one_short_admission_does_not_call_the_string_allocator() {
    let text = "rejected term";
    let bytes = text.len() + size_of::<Payload>() + 2 * size_of::<usize>();
    let memory = memory(bytes - 1);
    let mut entered = false;
    assert!(Term::build(text.len(), Some(&memory), || {
        entered = true;
        text.to_owned()
    })
    .is_err());
    assert!(!entered);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn consumer_unwind_and_rejected_untracked_handoff_release_the_payload() {
    let memory = memory(1024);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let term = Term::copy("unwind term", Some(&memory)).unwrap();
        let _consumer = term.clone();
        panic!("consumer failed");
    }));
    assert!(result.is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let term = Term::copy("admitted term", Some(&memory)).unwrap();
    assert!(term.into_untracked().is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let term = Term::copy("legacy term", None).unwrap();
    assert_eq!(term.clone_bytes(), term.len());
    assert_eq!(term.into_untracked().unwrap(), "legacy term");
}
