use super::*;
use skein_core::RuntimeMemoryReservation;

const FRAGMENTS: [&str; 16] = [
    "",
    "_",
    "foo_bar",
    "foo bar foo",
    "HTTPServer42",
    "running studied studies",
    "RAG",
    "graph retrieval",
    "checkpoint",
    "space_id",
    "\u{4e2d}\u{56fd}\u{79d1}\u{5b66}\u{9662}",
    "\u{20000}\u{20001}\u{20002}",
    "\u{3042}\u{3044}\u{3046}",
    "\u{039f}\u{03a3}",
    "\u{0130}Case",
    "\0.../-",
];

fn text(random: &mut Random) -> String {
    let mut text = String::new();
    for _ in 0..random.index(16) {
        text.push_str(FRAGMENTS[random.index(FRAGMENTS.len())]);
        text.push_str([" ", "_", "/", "..", "\n"][random.index(5)]);
    }
    text
}

fn identifier_campaign_case(case: usize, random: &mut Random) -> usize {
    use analyzer::tokens;
    let lexicon = SearchAnalyzerLexicon::nowledge_memory()
        .with_alias_rule(["foo", "foo_bar"], ["bar", "foo_bar", "extra"]);
    let mut raw = String::new();
    let alphabet = [
        'a',
        'B',
        'C',
        'd',
        '1',
        '2',
        '_',
        '\u{039f}',
        '\u{03a3}',
        '\u{0130}',
        '\u{4e2d}',
        '\u{56fd}',
        '\u{20000}',
        '\u{3042}',
        '\u{ac00}',
    ];
    for _ in 0..random.index(48) {
        raw.push(alphabet[random.index(alphabet.len())]);
    }
    if case.is_multiple_of(4) {
        raw.push_str("foo_bar");
    }
    let expected = super::super::token_reference::identifier_tokens(&raw, &lexicon);
    assert_eq!(
        crate::identifier_tokens(&raw, &lexicon),
        expected,
        "serving identifier case {case}"
    );
    let context = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&context).unwrap();
    let mut actual = Vec::new();
    tokens::identifier(&raw, &lexicon, Some(&context), Some(&memory), |token| {
        actual.push(token.to_string());
        Ok(())
    })
    .unwrap();
    assert_eq!(actual, expected, "identifier case {case}: {raw:?}");
    let peak = memory.ledger.snapshot().peak_bytes;
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    if peak > 1 {
        for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
            let context = RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0));
            let memory = BuildMemory::new(&context).unwrap();
            let mut prefix = Vec::new();
            let result =
                tokens::identifier(&raw, &lexicon, Some(&context), Some(&memory), |token| {
                    prefix.push(token.to_string());
                    Ok(())
                });
            assert_eq!(
                result.is_ok(),
                succeeds,
                "identifier case {case}: limit={limit}, {result:?}"
            );
            assert_eq!(&expected[..prefix.len()], prefix, "identifier case {case}");
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert!(memory.ledger.snapshot().peak_bytes <= limit);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
        }
    }
    usize::from(peak > 1)
}

fn unicode_lowercase_campaign() {
    use analyzer::tokens::Text;
    let context = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(1024, 0));
    let memory = BuildMemory::new(&context).unwrap();
    let mut scalars = 0;
    // Every Unicode scalar exercises the pinned std allocation envelope. The
    // identifier/document campaigns separately cover contextual combinations.
    for ch in (0..=0x10ffff).filter_map(char::from_u32) {
        let mut bytes = [0; 4];
        let raw = ch.encode_utf8(&mut bytes);
        let value = Text::lower(raw, Some(&memory)).unwrap();
        assert_eq!(value.as_str(), raw.to_lowercase());
        drop(value);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        scalars += 1;
    }
    assert_eq!(scalars, 1_112_064);
    eprintln!("analyzer lowercase: unicode_scalars={scalars}");
}

#[test]
#[ignore = "local incremental-analyzer campaign; run the explicit Bazel fuzz suite"]
fn frequency_parity_campaign() {
    let lexicons = [
        SearchAnalyzerLexicon::empty(),
        SearchAnalyzerLexicon::default(),
        SearchAnalyzerLexicon::nowledge_memory(),
        SearchAnalyzerLexicon::default()
            .with_alias_rule(["foo", "foo_bar"], ["bar", "RAG", "foo_bar"])
            .with_alias_rule(["foo_bar"], ["bar", "extra_alias"])
            .with_stopwords(["bar", "http", "graph"]),
    ];
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(8 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut random = Random(0x206a11);
    let mut identifier_random = Random(0x206b11);
    let mut limited = 0;
    let mut cancelled = 0;
    let mut identifier_budget_pairs = 0;
    for case in 0..12_000 {
        identifier_budget_pairs += identifier_campaign_case(case, &mut identifier_random);
        let lexicon = &lexicons[case % lexicons.len()];
        let mut input = document("a", &text(&mut random), &text(&mut random));
        for key in ["kind", "external_id", "source_id", "space_id", "ignored"] {
            if random.index(3) == 0 {
                input.metadata.insert(key.to_string(), text(&mut random));
            }
        }
        // Freeze the preceding algorithms, including allocating part/ngram
        // generation, so a shared production recipe cannot mask a regression.
        let tokens = super::super::token_reference::document_tokens(&input, lexicon);
        let length = tokens.len();
        let mut expected = BTreeMap::<String, u32>::new();
        for token in tokens {
            *expected.entry(token).or_default() += 1;
        }
        let config = LexicalProjectionConfig {
            max_document_tokens: NonZeroUsize::new(length.max(1)).unwrap(),
            ..Default::default()
        };
        let result =
            analyzer::analyze(&input, lexicon, config, Some(&task), Some(&memory)).unwrap();
        assert_eq!(result.document.document_len as usize, length, "case {case}");
        assert_eq!(result.document.frequencies, expected, "case {case}");
        drop(result);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        if length > 1 {
            let config = LexicalProjectionConfig {
                max_document_tokens: NonZeroUsize::new(length - 1).unwrap(),
                ..config
            };
            assert!(
                analyzer::analyze(&input, lexicon, config, Some(&task), Some(&memory)).is_err(),
                "case {case}"
            );
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            limited += 1;
        }
        if case % 16 == 0 {
            let cancelled_task = task.child();
            cancelled_task.cancellation().cancel();
            assert!(analyzer::analyze(
                &input,
                lexicon,
                config,
                Some(&cancelled_task),
                Some(&memory)
            )
            .is_err());
            assert!(!task.cancellation().is_cancelled());
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            cancelled += 1;
        }
    }
    assert!(limited > 10_000);
    assert_eq!(cancelled, 750);
    assert!(identifier_budget_pairs > 11_000);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    unicode_lowercase_campaign();
    eprintln!(
        "analyzer seed=0x206a11: cases=12000 parity=12000 limited={limited} cancelled={cancelled}; identifier_seed=0x206b11 identifier_parity=12000 identifier_budget_pairs={identifier_budget_pairs}"
    );
}
