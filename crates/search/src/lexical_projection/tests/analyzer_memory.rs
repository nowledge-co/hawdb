use super::*;
use crate::build_memory::SET_ENTRY_BYTES;
use skein_core::RuntimeMemoryReservation;

fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

fn check_parity(document: &SearchDocument, lexicon: &SearchAnalyzerLexicon) {
    let tokens = super::token_reference::document_tokens(document, lexicon);
    let length = tokens.len() as u32;
    let mut expected = BTreeMap::<String, u32>::new();
    for token in tokens {
        *expected.entry(token).or_default() += 1;
    }
    let memory = memory(8 * 1024 * 1024);
    let result =
        analyzer::analyze(document, lexicon, Default::default(), None, Some(&memory)).unwrap();
    assert_eq!(result.document.document_len, length);
    assert_eq!(result.document.frequencies, expected);
    drop(result);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn streamed_frequencies_preserve_weighting_dedup_aliases_and_field_boundaries() {
    let texts = [
        "",
        "___ ...",
        "foo_bar foo bar foo_bar foo bar",
        "foo _ bar ___ baz",
        "HTTPServer42 http_server42 running studied studies",
        "RAG graph retrieval checkpoint",
        "\u{4e2d}\u{56fd}\u{79d1}\u{5b66}\u{9662} \u{20000}\u{20001}\u{20002}",
        "\u{039f}\u{03a3} \u{0130}Case \u{3042}\u{3044}\u{3046} \0 ending",
    ];
    let lexicons = [
        SearchAnalyzerLexicon::empty(),
        SearchAnalyzerLexicon::default(),
        SearchAnalyzerLexicon::nowledge_memory()
            .with_alias_rule(["foo"], ["bar", "foo_bar"])
            .with_stopwords(["running", "bar"]),
    ];
    for lexicon in &lexicons {
        for (index, text) in texts.iter().enumerate() {
            let mut input = document("a", text, texts[(index + 1) % texts.len()]);
            input.metadata = BTreeMap::from([
                ("kind".to_string(), text.to_string()),
                ("external_id".to_string(), "HTTPServer42".to_string()),
                ("source_id".to_string(), "foo bar".to_string()),
                ("space_id".to_string(), "foo bar".to_string()),
                ("ignored".to_string(), "never_index_this_field".to_string()),
            ]);
            check_parity(&input, lexicon);
        }
    }
}

#[test]
fn token_limit_stops_before_visiting_the_remaining_document() {
    let input = document("a", "", &"alpha ".repeat(20_000));
    let config = LexicalProjectionConfig {
        max_document_tokens: NonZeroUsize::new(8).unwrap(),
        ..Default::default()
    };
    let memory = memory(64 * 1024);
    analyzer::evidence::take();
    let error = analyzer::analyze(
        &input,
        &SearchAnalyzerLexicon::empty(),
        config,
        None,
        Some(&memory),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("tokens"), "{error}");
    let (visited, _) = analyzer::evidence::take();
    assert!(visited > 0 && visited <= 9, "visited {visited} identifiers");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn frequency_key_admission_precedes_insertion_and_releases_on_failure() {
    let input = document("a", "", "alpha");
    let memory = memory(SET_ENTRY_BYTES + "alpha".len() - 1);
    analyzer::evidence::take();
    assert!(analyzer::analyze(
        &input,
        &SearchAnalyzerLexicon::empty(),
        Default::default(),
        None,
        Some(&memory)
    )
    .is_err());
    assert_eq!(analyzer::evidence::take(), (1, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn repeated_tokens_do_not_retain_a_document_sized_token_list() {
    let lexicon = SearchAnalyzerLexicon::empty();
    let mut peaks = Vec::new();
    for repetitions in [2, 10_000] {
        let input = document("a", "", &"alpha ".repeat(repetitions));
        let memory = memory(8 * 1024);
        let result =
            analyzer::analyze(&input, &lexicon, Default::default(), None, Some(&memory)).unwrap();
        assert_eq!(result.document.frequencies.len(), 2);
        assert_eq!(result.document.frequencies["alpha"], repetitions as u32);
        assert_eq!(result.document.frequencies["alpha_alpha"], 1);
        peaks.push(memory.ledger.snapshot().peak_bytes);
        assert!(memory.ledger.snapshot().used_bytes > 0);
        drop(result);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    assert_eq!(peaks[0], peaks[1]);
}

#[test]
fn source_and_cancellation_reject_before_token_generation() {
    let input = document("a", "alpha", "beta");
    let memory = memory(64 * 1024);
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    analyzer::evidence::take();
    assert!(analyzer::analyze(
        &input,
        &SearchAnalyzerLexicon::empty(),
        Default::default(),
        Some(&task),
        Some(&memory)
    )
    .is_err());
    let config = LexicalProjectionConfig {
        max_document_source_bytes: NonZeroU64::MIN,
        ..Default::default()
    };
    assert!(analyzer::analyze(
        &input,
        &SearchAnalyzerLexicon::empty(),
        config,
        None,
        Some(&memory)
    )
    .is_err());
    assert_eq!(analyzer::evidence::take(), (0, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn posting_chunk_retains_transferred_terms_after_analyzer_drop_and_until_spill() {
    let root = projection_root("analyzer-term-transfer");
    fs::create_dir_all(&root).unwrap();
    let memory = memory(64 * 1024);
    let analyzed = analyzer::analyze(
        &document("a", "", "alpha beta"),
        &SearchAnalyzerLexicon::empty(),
        Default::default(),
        None,
        Some(&memory),
    )
    .unwrap();
    let mut chunk = posting_chunk::PostingChunk::new(memory.clone()).unwrap();
    for (term, term_frequency) in analyzed.document.frequencies {
        chunk
            .push(Posting {
                term,
                ordinal: 0,
                term_frequency,
            })
            .unwrap();
    }
    drop(analyzed._memory);
    assert!(chunk.retained_bytes() > 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, chunk.retained_bytes());
    let before = chunk.retained_bytes();
    let mut runs = SpillRuns::new(&root, 1, Default::default());
    chunk.spill(&mut runs).unwrap();
    assert!(chunk.is_empty());
    assert!(chunk.retained_bytes() < before);
    assert_eq!(memory.ledger.snapshot().used_bytes, chunk.retained_bytes());
    drop(chunk);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(runs);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn growing_a_posting_chunk_admits_old_and_replacement_slots_together() {
    let memory = memory(64 * 1024);
    let mut chunk = posting_chunk::PostingChunk::new(memory.clone()).unwrap();
    for ordinal in 0..5 {
        chunk
            .push(Posting {
                term: "alpha".to_string(),
                ordinal,
                term_frequency: 1,
            })
            .unwrap();
    }
    let snapshot = memory.ledger.snapshot();
    let slot_size = std::mem::size_of::<Posting>();
    assert_eq!(snapshot.used_bytes, 8 * slot_size + 5 * "alpha".len());
    assert!(snapshot.peak_bytes >= 12 * slot_size + 4 * "alpha".len());
    drop(chunk);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn failed_lexical_memory_admission_preserves_publication_and_releases_leases() {
    let root = projection_root("analyzer-memory-publication");
    fs::create_dir_all(&root).unwrap();
    let input = document("a", "alpha", "beta");
    let lexicon = SearchAnalyzerLexicon::empty();
    let reader = LexicalProjectionWriter::new(Default::default())
        .write(&root, 1, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap();
    let before = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let memory = memory(1);
    let result = LexicalProjectionWriter::new(Default::default())
        .with_memory(memory.clone())
        .write(&root, 2, None, 11, 13, std::iter::once(&input), &lexicon);
    let error = result.unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), before);
    assert!(!root.join(artifact_file(2)).exists());
    assert!(fs::read_dir(&root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".tmp")));
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
