// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::RuntimeTaskContext;
use hawdb_core::RuntimeMemoryReservation;
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

#[test]
fn reserved_term_decodes_with_a_full_root_and_outlives_the_reservation_handle() {
    use crate::build_memory::reserved::{Grant, ReservedMemory};
    use std::io::Read;
    let source = b"decoded spill term";
    let capacity = source.len() + size_of::<Payload<Grant>>() + 2 * size_of::<usize>();
    let memory = memory(8192);
    let reservation = ReservedMemory::new(&memory.spool, capacity).unwrap();
    let before = memory.ledger.snapshot();
    let competing = memory
        .input
        .reserve(before.budget_bytes - before.used_bytes)
        .unwrap();
    let mut entered = false;
    assert!(Term::build_reserved(source.len() + 1, &reservation, || {
        entered = true;
        unreachable!("one-short reservation cannot enter the allocator");
    })
    .is_err());
    assert!(!entered);
    let term = Term::build_reserved(source.len(), &reservation, || {
        let mut bytes = vec![0; source.len()];
        source.as_slice().read_exact(&mut bytes)?;
        String::from_utf8(bytes).map_err(|error| HawDBError::Storage(error.to_string()))
    })
    .unwrap();
    assert_eq!(term.as_str(), "decoded spill term");
    let consumer = term.clone();
    assert_eq!(consumer.as_ptr(), term.as_ptr());
    drop((term, reservation, competing));
    assert_eq!(memory.ledger.snapshot().used_bytes, before.used_bytes);
    assert_eq!(consumer.clone_bytes(), 0);
    drop(consumer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn failed_reserved_decode_returns_its_grant_for_retry() {
    use crate::build_memory::reserved::ReservedMemory;
    let memory = memory(8192);
    let reservation = ReservedMemory::new(&memory.spool, 1024).unwrap();
    assert!(Term::build_reserved(256, &reservation, || {
        Err(HawDBError::Storage("short spill read".into()))
    })
    .is_err());
    let grant = reservation.reserve(1024).unwrap();
    drop(grant);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = Term::build_reserved(256, &reservation, || {
            panic!("spill decoder unwound");
        });
    }))
    .is_err());
    let grant = reservation.reserve(1024).unwrap();
    drop((grant, reservation));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
