use super::*;
use crate::{identifier_parts, identifier_tokens, push_analyzed_token, TokenSequence};

// Frozen pre-streaming traversal: keep this independent of the production
// visitor and accumulator. The unchanged identifier analyzer is shared.
fn reference_tokens(text: &str, analyzer: &SearchAnalyzerLexicon) -> Vec<String> {
    let mut tokens = TokenSequence::default();
    let mut previous_part = None::<String>;
    for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
        let parts = identifier_parts(raw);
        if let (Some(previous), Some(first)) = (previous_part.as_ref(), parts.first()) {
            push_analyzed_token(&mut tokens, format!("{previous}_{first}"), analyzer);
        }
        tokens.extend(identifier_tokens(raw, analyzer));
        if let Some(last) = parts.last() {
            previous_part = Some(last.clone());
        }
    }
    tokens.into_vec()
}

fn reference_document_tokens(
    document: &SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
) -> Vec<String> {
    let title = reference_tokens(&document.title, analyzer);
    let mut tokens = title.clone();
    tokens.extend(title);
    tokens.extend(reference_tokens(&document.content, analyzer));
    for key in ["kind", "external_id", "source_id", "space_id"] {
        if let Some(value) = document.metadata.get(key) {
            tokens.extend(reference_tokens(value, analyzer));
        }
    }
    tokens
}

fn assert_reference(document: &SearchDocument, analyzer: &SearchAnalyzerLexicon) {
    let expected_tokens = reference_document_tokens(document, analyzer);
    assert_eq!(crate::document_tokens(document, analyzer), expected_tokens);
    let mut expected_frequencies = BTreeMap::<String, u32>::new();
    for token in &expected_tokens {
        *expected_frequencies.entry(token.clone()).or_default() += 1;
    }
    let actual = analyze_delta_document(document, analyzer, LexicalProjectionConfig::default())
        .expect("reference fixture must be admitted");
    assert_eq!(actual.document_len as usize, expected_tokens.len());
    assert_eq!(actual.frequencies, expected_frequencies, "{}", document.id);
    let resident_bytes = document.id.len() as u64
        + 64
        + expected_frequencies
            .keys()
            .map(|term| term.len() as u64 + 32)
            .sum::<u64>();
    assert_eq!(actual.resident_bytes, resident_bytes);
}

fn document(title: &str, content: &str) -> SearchDocument {
    SearchDocument {
        id: "analysis-reference".to_string(),
        title: title.to_string(),
        content: content.to_string(),
        embedding: None,
        metadata: BTreeMap::from([
            ("kind".to_string(), "Graph Graph".to_string()),
            ("external_id".to_string(), "RawEvidenceV2".to_string()),
            (
                "source_id".to_string(),
                "source_chunk/sourceChunk".to_string(),
            ),
            ("space_id".to_string(), "Team Workspace".to_string()),
            (
                "private_note".to_string(),
                "must_not_be_analyzed".to_string(),
            ),
        ]),
    }
}

fn analyzers() -> [SearchAnalyzerLexicon; 3] {
    [
        SearchAnalyzerLexicon::empty(),
        SearchAnalyzerLexicon::default(),
        SearchAnalyzerLexicon::default()
            .with_normalized_alias_rule(["raw evidence", "RawEvidence"], ["episodic provenance"])
            .with_normalized_alias_rule(["graph graph"], ["graph", "graph pair"])
            .with_stopwords(["thread", "memory lifecycle"]),
    ]
}

#[test]
fn field_weighting_and_local_uniqueness_match_the_legacy_sequence() {
    let mut fixture = document("Graph Graph", "Graph Graph Graph");
    fixture.metadata.clear();
    fixture.metadata.insert("kind".into(), "Graph Graph".into());
    fixture
        .metadata
        .insert("space_id".into(), "Graph Graph".into());
    let analyzer = SearchAnalyzerLexicon::empty();
    let actual = analyze_delta_document(&fixture, &analyzer, Default::default()).unwrap();
    assert_eq!(actual.document_len, 16);
    assert_eq!(actual.frequencies["graph"], 11);
    assert_eq!(actual.frequencies["graph_graph"], 5);
    assert_reference(&fixture, &analyzer);
}

#[test]
fn identifier_cjk_alias_and_field_boundaries_match_the_legacy_sequence() {
    for analyzer in analyzers() {
        for (title, content) in [
            ("", ""),
            ("Graph Graph", "Graph-Graph/Graph...Graph"),
            ("raw evidence", "RawEvidence raw__evidence RAW_EVIDENCE"),
            ("HTTPServerV2", "running runners studies indexed IDs2SQL"),
            ("GraphGraph", "graph_graph Graph Graph"),
            ("RAG", "MVCC WAL LSM checkpoint/GraphStream"),
            (
                "\u{77e5}\u{8bc6}",
                "\u{77e5}\u{8bc6}\u{56fe}\u{8c31}Graph2\u{641c}\u{7d22}",
            ),
            (
                "\u{3042}\u{30a2}",
                "\u{ac00}\u{ac01}\u{ac02} \u{20000}\u{20001}\u{20002}",
            ),
            (
                "\u{130}Index",
                "Stra\u{df}e CAF\u{c9} e\u{301} \u{1f600} Graph",
            ),
            ("thread", "memory lifecycle---thread...raw evidence"),
        ] {
            assert_reference(&document(title, content), &analyzer);
        }
    }
}

#[test]
fn lexical_source_token_and_term_admission_boundaries_remain_explicit() {
    let analyzer = SearchAnalyzerLexicon::empty();
    let mut fixture = document("", "alpha beta gamma");
    fixture.metadata.clear();
    let count = reference_document_tokens(&fixture, &analyzer).len();
    let mut config = LexicalProjectionConfig {
        max_document_tokens: NonZeroUsize::new(count).unwrap(),
        ..Default::default()
    };
    analyze_delta_document(&fixture, &analyzer, config).unwrap();
    config.max_document_tokens = NonZeroUsize::new(count - 1).unwrap();
    assert!(analyze_delta_document(&fixture, &analyzer, config)
        .unwrap_err()
        .to_string()
        .contains("tokens"));

    for width in [4095, 4096, 4097, 5202] {
        fixture.content = "x".repeat(width);
        let result = analyze_delta_document(&fixture, &analyzer, Default::default());
        assert_eq!(result.is_ok(), width <= 4096, "term width {width}");
        if let Err(error) = result {
            assert!(error.to_string().contains("lexical term"));
        }
    }
    for width in [4 * 1024 * 1024, 4 * 1024 * 1024 + 1] {
        fixture.content = " ".repeat(width);
        let result = analyze_delta_document(&fixture, &analyzer, Default::default());
        assert_eq!(result.is_ok(), width == 4 * 1024 * 1024);
        if let Err(error) = result {
            assert!(error.to_string().contains("source bytes"));
        }
    }
}

#[test]
fn token_admission_stops_before_analyzing_the_document_tail() {
    let mut fixture = document("", &"Graph ".repeat(32_768));
    fixture.metadata.clear();
    let config = LexicalProjectionConfig {
        max_document_tokens: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    };
    crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.set(0));
    let error =
        analyze_delta_document(&fixture, &SearchAnalyzerLexicon::empty(), config).unwrap_err();
    assert!(error.to_string().contains("tokens"));
    let visited = crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.get());
    assert_eq!(
        visited, 2,
        "admission must not traverse the remaining input"
    );
}

#[test]
fn visitor_propagates_callback_failure_without_visiting_later_identifiers() {
    crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.set(0));
    let mut callbacks = 0;
    let error = visit_token_list(
        "first second third",
        &SearchAnalyzerLexicon::empty(),
        |_, _| {
            callbacks += 1;
            Err(SkeinError::Execution("stop analysis".to_string()))
        },
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        SkeinError::Execution("stop analysis".to_string()).to_string()
    );
    assert_eq!(callbacks, 1);
    assert_eq!(
        crate::analyzer_stream::IDENTIFIER_VISITS.with(|visits| visits.get()),
        1
    );
}

#[test]
fn frequency_map_admission_includes_field_markers_before_insertion() {
    let id = "admission";
    let base = id.len() as u64 + 64;
    let marker = (std::mem::size_of::<AnalyzedTerm>() - std::mem::size_of::<u32>()) as u64;
    let required = base + "alpha".len() as u64 + 32 + marker;
    for (limit, admitted) in [(required, true), (required - 1, false)] {
        let config = LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(limit).unwrap(),
            ..Default::default()
        };
        let mut accumulator = DocumentAnalysis::new(id, config).unwrap();
        let result = accumulator.push("alpha".into(), TokenOccurrence::Repeated, 0, 1);
        assert_eq!(result.is_ok(), admitted);
        if admitted {
            assert_eq!(accumulator.frequencies.len(), 1);
            assert_eq!(accumulator.document_len, 1);
            assert_eq!(accumulator.resident_bytes, required - marker);
        } else {
            assert!(result.unwrap_err().to_string().contains("analyzer bytes"));
            assert!(accumulator.frequencies.is_empty());
            assert_eq!(accumulator.document_len, 0);
            assert_eq!(accumulator.resident_bytes, base);
        }
    }
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(base - 1).unwrap(),
        ..Default::default()
    };
    assert!(DocumentAnalysis::new(id, config).is_err());
}

#[test]
fn repeated_occurrences_keep_exact_frequencies_with_a_small_map_budget() {
    let mut fixture = document("", &"Graph ".repeat(32_768));
    fixture.metadata.clear();
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(256).unwrap(),
        ..Default::default()
    };
    let actual = analyze_delta_document(&fixture, &SearchAnalyzerLexicon::empty(), config)
        .expect("repeated terms fit the small frequency accumulator");
    assert_eq!(actual.document_len, 32_769);
    assert_eq!(actual.frequencies.len(), 2);
    assert_eq!(actual.frequencies["graph"], 32_768);
    assert_eq!(actual.frequencies["graph_graph"], 1);
    assert!(actual.resident_bytes < 256);
}

fn assert_projection_scores(
    reader: &LexicalProjectionReader,
    delta: &LexicalMiniDelta,
    documents: &BTreeMap<String, SearchDocument>,
    analyzer: &SearchAnalyzerLexicon,
    query_terms: &BTreeSet<String>,
) {
    let bags = documents
        .values()
        .map(|document| {
            let tokens = reference_document_tokens(document, analyzer);
            let len = tokens.len();
            let mut frequencies = BTreeMap::<String, usize>::new();
            for token in tokens {
                *frequencies.entry(token).or_default() += 1;
            }
            (&document.id, len, frequencies)
        })
        .collect::<Vec<_>>();
    let average_len =
        bags.iter().map(|(_, len, _)| len).sum::<usize>() as f64 / documents.len() as f64;
    for term in query_terms {
        let frequency = bags
            .iter()
            .filter(|(_, _, bag)| bag.contains_key(term))
            .count();
        let inverse_frequency = (1.0
            + (documents.len() as f64 - frequency as f64 + 0.5) / (frequency as f64 + 0.5))
            .ln();
        let mut expected = BTreeMap::new();
        for (id, len, bag) in &bags {
            if let Some(frequency) = bag.get(term) {
                let frequency = *frequency as f64;
                let denominator = frequency
                    + BM25_K1 * (1.0 - BM25_B + BM25_B * *len as f64 / average_len.max(1.0));
                expected.insert(
                    (*id).clone(),
                    inverse_frequency * (frequency * (BM25_K1 + 1.0)) / denominator,
                );
            }
        }
        let actual = reader
            .score(&BTreeSet::from([term.clone()]), delta, None, |_| Ok(true))
            .unwrap();
        assert_eq!(
            actual.matching_document_count,
            expected.len(),
            "count for {term}"
        );
        assert_eq!(actual.scores, expected, "scores for {term}");
    }
}

struct AnalysisFixtureCleanup(PathBuf);

impl Drop for AnalysisFixtureCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn streamed_frequencies_preserve_persisted_scores_delta_reopen_and_failed_publication() {
    let root = std::env::temp_dir().join(format!(
        "skein-document-analysis-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    fs::create_dir(&root).unwrap();
    let _cleanup = AnalysisFixtureCleanup(root.clone());
    let analyzer = analyzers().into_iter().last().unwrap();
    let mut documents = [
        document("Graph Graph", "RawEvidence graph_graph Graph Graph"),
        document(
            "GraphGraph",
            "\u{77e5}\u{8bc6}\u{56fe}\u{8c31} raw evidence",
        ),
        document("HTTPServerV2", "Graph Graph Graph GraphStream running"),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, mut document)| {
        document.id = format!("document-{index}");
        (document.id.clone(), document)
    })
    .collect::<BTreeMap<_, _>>();
    let mut terms = documents
        .values()
        .flat_map(|document| reference_document_tokens(document, &analyzer))
        .collect::<BTreeSet<_>>();
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(4_096).unwrap(),
        target_block_bytes: NonZeroU64::new(256).unwrap(),
        max_block_bytes: NonZeroU64::new(1_024).unwrap(),
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let reader = LexicalProjectionWriter::new(config)
        .write(&root, 1, Some(7), 11, 13, documents.values(), &analyzer)
        .unwrap();
    let expected_len = documents
        .values()
        .map(|document| reference_document_tokens(document, &analyzer).len() as u64)
        .sum::<u64>();
    assert_eq!(reader.manifest.total_document_len, expected_len);
    assert_eq!(reader.manifest.document_count, documents.len() as u64);
    for term in &terms {
        let expected_df = documents
            .values()
            .filter(|document| reference_document_tokens(document, &analyzer).contains(term))
            .count();
        assert_eq!(reader.manifest.document_frequency(term), expected_df as u64);
    }
    let mut delta = LexicalMiniDelta::default();
    assert_projection_scores(&reader, &delta, &documents, &analyzer, &terms);

    let mut updated = documents["document-0"].clone();
    updated.title = "Graph raw evidence Graph".to_string();
    updated.content = "\u{77e5}\u{8bc6}\u{56fe}\u{8c31} Graph Graph".to_string();
    delta
        .upsert(&updated, documents.get(&updated.id), &analyzer, config)
        .unwrap();
    documents.insert(updated.id.clone(), updated.clone());
    assert!(delta
        .delete("document-1", documents.get("document-1"), &analyzer, config)
        .unwrap());
    documents.remove("document-1");
    terms.extend(
        documents
            .values()
            .flat_map(|document| reference_document_tokens(document, &analyzer)),
    );
    assert_projection_scores(&reader, &delta, &documents, &analyzer, &terms);

    let retained_bytes = delta.resident_bytes;
    updated.content = "x".repeat(4097);
    assert!(delta.upsert(&updated, None, &analyzer, config).is_err());
    assert_eq!(delta.resident_bytes, retained_bytes);
    assert_projection_scores(&reader, &delta, &documents, &analyzer, &terms);

    let next = LexicalProjectionWriter::new(config)
        .write(&root, 2, Some(8), 11, 17, documents.values(), &analyzer)
        .unwrap();
    drop(next);
    let reopened = LexicalProjectionReader::load(&root, Some(8), 11, 17, config)
        .unwrap()
        .unwrap();
    assert_projection_scores(
        &reopened,
        &LexicalMiniDelta::default(),
        &documents,
        &analyzer,
        &terms,
    );
    let manifest = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let limited = LexicalProjectionConfig {
        max_document_tokens: NonZeroUsize::new(1).unwrap(),
        ..config
    };
    assert!(LexicalProjectionWriter::new(limited)
        .write(&root, 3, Some(9), 11, 19, documents.values(), &analyzer)
        .is_err());
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), manifest);
    assert!(!root.join(artifact_file(3)).exists());
    assert_projection_scores(
        &reopened,
        &LexicalMiniDelta::default(),
        &documents,
        &analyzer,
        &terms,
    );
    drop(reopened);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "manual local analyzer differential campaign"]
fn document_analysis_differential_campaign() {
    let pieces = [
        "Graph",
        "graph_graph",
        "GraphGraph",
        "RawEvidence",
        "raw evidence",
        "WAL",
        "MVCC",
        "HTTPServerV2",
        "source_chunk",
        "thread",
        "studies",
        "running",
        "memory lifecycle",
        "\u{77e5}\u{8bc6}\u{56fe}\u{8c31}",
        "\u{3042}\u{30a2}\u{3044}",
        "\u{ac00}\u{ac01}\u{ac02}",
        "\u{20000}\u{20001}\u{20002}",
        "CAF\u{c9}",
        "\u{130}Index",
        "ID42",
    ];
    let separators = [" ", "---", "/", "\n", "__", ".", "\u{1f600}", ""];
    let analyzers = analyzers();
    let mut random = 0x3920_616e_616c_797a_u64;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random as usize
    };
    for case in 0..768 {
        let mut text = String::new();
        for _ in 0..1 + next() % 48 {
            text.push_str(pieces[next() % pieces.len()]);
            text.push_str(separators[next() % separators.len()]);
        }
        let mut fixture = document(&text, &format!("{text} {text}"));
        fixture.id = format!("seed-392-case-{case}");
        for key in ["kind", "external_id", "source_id", "space_id"] {
            fixture.metadata.insert(key.to_string(), text.clone());
        }
        assert_reference(&fixture, &analyzers[case % analyzers.len()]);
    }
}
