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
use hawdb_core::RuntimeMemoryReservation;

fn memory() -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(8 * 1024 * 1024, 0)),
    )
    .unwrap()
}

#[test]
fn dedup_growth_admits_replacement_beside_the_old_table() {
    let memory = memory();
    let control = Control {
        memory: Some(&memory),
        ..Default::default()
    };
    let mut dedup = Dedup::new();
    for index in 0..1024 {
        let term = Term::copy(&format!("key{index}"), Some(&memory)).unwrap();
        let text = Text::Owned(term);
        if dedup.terms.len() == dedup.terms.capacity() {
            let old_capacity = dedup.terms.capacity();
            let old_bytes = table_bytes::<(Text<'_>, ())>(old_capacity).unwrap();
            let next_bytes = table_bytes::<(Text<'_>, ())>(dedup.terms.len() + 1).unwrap();
            let before = memory.ledger.snapshot();
            let mut competing = memory
                .input
                .reserve(before.budget_bytes - before.used_bytes - next_bytes + 1)
                .unwrap();
            let error = dedup.insert(text.clone(), control).unwrap_err();
            assert!(error.to_string().contains("query_memory_bytes"));
            assert_eq!(dedup.terms.capacity(), old_capacity);
            assert!(!dedup.terms.contains_key(text.as_str()));
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                before.used_bytes + competing.bytes()
            );
            competing.shrink(1);
            drop(dedup.insert(text, control).unwrap().unwrap());
            assert_eq!(dedup._memory.as_ref().unwrap().bytes(), next_bytes);
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                before.used_bytes + competing.bytes() + next_bytes - old_bytes
            );
        } else {
            drop(dedup.insert(text, control).unwrap().unwrap());
        }
    }
    assert_eq!(dedup.terms.len(), 1024);
    drop(dedup);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn dedup_shares_owned_text_and_keeps_consumers_alive_after_scope_reset() {
    let memory = memory();
    let control = Control {
        memory: Some(&memory),
        ..Default::default()
    };
    let term = Term::copy("owned text", Some(&memory)).unwrap();
    let payload_bytes = memory.ledger.snapshot().used_bytes;
    let address = term.as_ptr();
    let mut dedup = Dedup::new();
    let emitted = dedup.insert(Text::Owned(term), control).unwrap().unwrap();
    let before = memory.ledger.snapshot().used_bytes;
    assert!(dedup
        .insert(Text::Borrowed("owned text"), control)
        .unwrap()
        .is_none());
    assert_eq!(memory.ledger.snapshot().used_bytes, before);
    assert_eq!(emitted.as_ptr(), address);
    drop(dedup);
    assert_eq!(memory.ledger.snapshot().used_bytes, payload_bytes);
    assert_eq!(emitted.as_str(), "owned text");
    drop(emitted);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn dedup_reset_reuses_its_admitted_table_capacity() {
    let memory = memory();
    let control = Control {
        memory: Some(&memory),
        ..Default::default()
    };
    let mut dedup = Dedup::new();
    for key in ["alpha", "beta", "gamma"] {
        drop(dedup.insert(Text::Borrowed(key), control).unwrap().unwrap());
    }
    let capacity = dedup.terms.capacity();
    let retained = dedup._memory.as_ref().unwrap().bytes();
    let before = memory.ledger.snapshot().used_bytes;
    dedup.reset();
    assert_eq!(dedup.terms.len(), 0);
    assert_eq!(dedup.terms.capacity(), capacity);
    assert_eq!(dedup._memory.as_ref().unwrap().bytes(), retained);
    assert_eq!(memory.ledger.snapshot().used_bytes, before);
    drop(
        dedup
            .insert(Text::Borrowed("delta"), control)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(dedup.terms.capacity(), capacity);
    assert_eq!(dedup._memory.as_ref().unwrap().bytes(), retained);
    drop(dedup);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn lowercase_bounds_cover_every_unicode_scalar_and_contextual_sigma() {
    for scalar in 0..=0x10ffff {
        let Some(ch) = char::from_u32(scalar) else {
            continue;
        };
        let output_bytes: usize = ch.to_lowercase().map(char::len_utf8).sum();
        assert!(output_bytes <= 3 * ch.len_utf8(), "scalar={scalar:x}");
    }
    let memory = memory();
    let control = Control {
        memory: Some(&memory),
        ..Default::default()
    };
    let raw = Text::lowercase("\u{39f}\u{3a3}", false, control).unwrap();
    let part = Text::lowercase("\u{39f}\u{3a3}", true, control).unwrap();
    assert_eq!(raw.as_str(), "\u{3bf}\u{3c2}");
    assert_eq!(part.as_str(), "\u{3bf}\u{3c3}");
    drop((raw, part));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn rejected_term_materialization_keeps_the_replacement_admitted_for_retry() {
    let memory = memory();
    let control = Control {
        memory: Some(&memory),
        ..Default::default()
    };
    let next = "new-key".repeat(512);
    let mut dedup = Dedup::new();
    for key in ["alpha", "beta", "gamma"] {
        drop(dedup.insert(Text::Borrowed(key), control).unwrap());
    }
    let capacity = dedup.terms.capacity();
    assert_eq!(dedup.terms.len(), capacity);
    let before = memory.ledger.snapshot();
    let replacement = table_bytes::<(Text<'_>, ())>(capacity + 1).unwrap();
    let competing = memory
        .input
        .reserve(before.budget_bytes - before.used_bytes - replacement)
        .unwrap();
    let error = dedup.insert(Text::Borrowed(&next), control).unwrap_err();
    assert!(error.to_string().contains("query_memory_bytes"));
    assert!(dedup.terms.capacity() > capacity);
    let retained = table_bytes::<(Text<'_>, ())>(dedup.terms.capacity()).unwrap();
    assert_eq!(dedup._memory.as_ref().unwrap().bytes(), retained);
    assert_eq!(dedup.terms.len(), 3);
    for key in ["alpha", "beta", "gamma"] {
        assert!(dedup.terms.contains_key(key));
    }
    assert!(!dedup.terms.contains_key(next.as_str()));
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        retained + competing.bytes()
    );
    drop(competing);
    let term = dedup
        .insert(Text::Borrowed(&next), control)
        .unwrap()
        .unwrap();
    assert_eq!(term.as_str(), next);
    drop(term);
    drop(dedup);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn duplicates_need_no_new_admission_with_full_or_spare_capacity() {
    for count in [3, 4] {
        let memory = memory();
        let control = Control {
            memory: Some(&memory),
            ..Default::default()
        };
        let mut dedup = Dedup::new();
        for key in ["alpha", "beta", "gamma", "delta"].into_iter().take(count) {
            drop(dedup.insert(Text::Borrowed(key), control).unwrap());
        }
        assert_eq!(dedup.terms.len() == dedup.terms.capacity(), count == 3);
        let before = memory.ledger.snapshot();
        let competing = memory
            .input
            .reserve(before.budget_bytes - before.used_bytes)
            .unwrap();
        assert!(dedup
            .insert(Text::Borrowed("alpha"), control)
            .unwrap()
            .is_none());
        assert_eq!(memory.ledger.snapshot().used_bytes, before.budget_bytes);
        drop(competing);
        drop(dedup);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
