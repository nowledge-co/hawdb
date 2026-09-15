use super::*;
use skein_core::RuntimeMemoryReservation;

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
            assert_eq!(dedup.memory.as_ref().unwrap().bytes(), next_bytes);
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
