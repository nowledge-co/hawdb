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
    let mut limited = 0;
    let mut cancelled = 0;
    for case in 0..12_000 {
        let lexicon = &lexicons[case % lexicons.len()];
        let mut input = document("a", &text(&mut random), &text(&mut random));
        for key in ["kind", "external_id", "source_id", "space_id", "ignored"] {
            if random.index(3) == 0 {
                input.metadata.insert(key.to_string(), text(&mut random));
            }
        }
        // The reference still materializes the preceding token sequence. It
        // does not share field visitation or frequency accumulation with the
        // incremental production analyzer.
        let tokens = document_tokens(&input, lexicon);
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
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    eprintln!(
        "analyzer seed=0x206a11: cases=12000 parity=12000 limited={limited} cancelled={cancelled}"
    );
}
