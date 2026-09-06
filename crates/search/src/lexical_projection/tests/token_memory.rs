use super::*;
use crate::build_memory::SET_ENTRY_BYTES;
use analyzer::tokens::{self, Text};
use skein_core::RuntimeMemoryReservation;

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

#[test]
fn streamed_identifier_order_matches_the_frozen_allocating_analyzer() {
    let lexicon = SearchAnalyzerLexicon::nowledge_memory()
        .with_alias_rule(["foo", "foo_bar"], ["bar", "foo_bar", "extra"])
        .with_alias_rule(["foo_bar"], ["extra", "another"])
        .with_stopwords(["bar", "running"]);
    for raw in [
        "",
        "___",
        "foo__bar",
        "HTTPServer42foo7BAR",
        "running",
        "studies",
        "received",
        "racing",
        "buzzing",
        "class",
        "foo_bar_foo_bar",
        "\u{039f}\u{03a3}",
        "\u{039f}\u{03a3}\u{0301}",
        "\u{0130}Case",
        "\u{01c5}Foo",
        "\u{4e2d}\u{56fd}\u{79d1}\u{5b66}\u{9662}",
        "\u{20000}\u{20001}\u{20002}",
        "\u{3042}\u{3044}\u{3046}_abc_\u{ac00}\u{ac01}",
    ] {
        let memory = memory(4 * 1024 * 1024);
        let mut actual = Vec::new();
        tokens::identifier(raw, &lexicon, None, Some(&memory), |token| {
            actual.push(token.to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            actual,
            super::token_reference::identifier_tokens(raw, &lexicon),
            "{raw:?}"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
    }
}

#[test]
fn insufficient_scratch_rejects_before_constructing_a_token() {
    let memory = memory(4);
    tokens::evidence::take();
    assert!(tokens::identifier(
        "alpha",
        &SearchAnalyzerLexicon::empty(),
        None,
        Some(&memory),
        |_| panic!("unadmitted token must not be emitted")
    )
    .is_err());
    assert_eq!(tokens::evidence::take(), (0, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn token_dedup_admission_includes_the_live_source_string() {
    let required = SET_ENTRY_BYTES + 2 * "alpha".len();
    for (limit, succeeds) in [(required - 1, false), (required, true)] {
        let memory = memory(limit);
        let mut emitted = 0;
        tokens::evidence::take();
        let result = tokens::identifier(
            "alpha",
            &SearchAnalyzerLexicon::empty(),
            None,
            Some(&memory),
            |_| {
                emitted += 1;
                Ok(())
            },
        );
        assert_eq!(result.is_ok(), succeeds, "{result:?}");
        let (allocations, insertions) = tokens::evidence::take();
        assert_eq!(emitted, usize::from(succeeds));
        assert_eq!(insertions, usize::from(succeeds));
        assert_eq!(allocations, if succeeds { 2 } else { 1 });
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert!(memory.ledger.snapshot().peak_bytes <= limit);
    }
}

#[test]
fn boundary_lowering_admits_expansion_before_allocation() {
    let expected = "i\u{0307}_http42";
    let memory = memory(expected.len() - 1);
    tokens::evidence::take();
    assert!(Text::boundary("\u{0130}", "HTTP42", Some(&memory)).is_err());
    assert_eq!(tokens::evidence::take(), (0, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let memory = self::memory(expected.len());
    let boundary = Text::boundary("\u{0130}", "HTTP42", Some(&memory)).unwrap();
    assert_eq!(boundary.as_str(), expected);
    assert_eq!(memory.ledger.snapshot().used_bytes, expected.len());
    drop(boundary);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn alias_fanout_stops_before_cloning_unadmitted_aliases() {
    let lexicon = SearchAnalyzerLexicon::empty()
        .with_alias_rule(["alpha"], (0..10_000).map(|index| format!("alias_{index}")));
    let memory = memory(SET_ENTRY_BYTES + 2 * "alpha".len());
    let mut emitted = 0;
    tokens::evidence::take();
    assert!(
        tokens::identifier("alpha", &lexicon, None, Some(&memory), |_| {
            emitted += 1;
            Ok(())
        })
        .is_err()
    );
    assert_eq!(emitted, 1);
    assert_eq!(tokens::evidence::take(), (2, 1));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn failed_consumer_releases_scratch_and_dedup_without_inserting() {
    let memory = memory(64 * 1024);
    tokens::evidence::take();
    let error = tokens::identifier(
        "alpha",
        &SearchAnalyzerLexicon::empty(),
        None,
        Some(&memory),
        |_| {
            assert!(memory.ledger.snapshot().used_bytes >= SET_ENTRY_BYTES + 10);
            Err(SkeinError::Execution(
                "injected consumer failure".to_string(),
            ))
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("injected consumer failure"));
    assert_eq!(tokens::evidence::take(), (1, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn cancellation_during_one_identifier_stops_further_emission() {
    let task = RuntimeTaskContext::default();
    let memory = memory(64 * 1024);
    let mut emitted = 0;
    assert!(tokens::identifier(
        "HTTPServer42",
        &SearchAnalyzerLexicon::empty(),
        Some(&task),
        Some(&memory),
        |_| {
            emitted += 1;
            task.cancellation().cancel();
            Ok(())
        }
    )
    .is_err());
    assert_eq!(emitted, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn contextual_lowercase_keeps_its_capacity_charged_until_drop() {
    let memory = memory(64 * 1024);
    for raw in [
        "\u{039f}\u{03a3}",
        "\u{039f}\u{03a3}\u{0301}",
        "\u{0130}",
        "ASCII\u{0130}TAIL",
        "\u{0130}\u{0130}\u{0130}\u{0130}",
    ] {
        let value = Text::lower(raw, Some(&memory)).unwrap();
        assert_eq!(value.as_str(), raw.to_lowercase());
        assert!(memory.ledger.snapshot().used_bytes >= value.as_str().len());
        drop(value);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    assert_eq!(
        Text::lower("\u{039f}\u{03a3}", None).unwrap().as_str(),
        "\u{03bf}\u{03c2}"
    );
    assert_eq!(
        Text::boundary("\u{039f}\u{03a3}", "X", None)
            .unwrap()
            .as_str(),
        "\u{03bf}\u{03c3}_x"
    );
}
