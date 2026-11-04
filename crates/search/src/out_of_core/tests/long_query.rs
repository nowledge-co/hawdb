// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Long natural-language queries must fit the bounded scored-term cap by
//! keeping their most selective terms instead of refusing the whole query.

use super::*;

const SENTENCE: &str = "researchers reported that running computers tested several \
     nations' learning systems while engineers measured growing networks, \
     detecting failures and improving routers";

fn corpus() -> Vec<SearchDocument> {
    (0..48)
        .map(|number| SearchDocument {
            id: format!("memory:{number:03}"),
            title: format!("note {number}"),
            // Term `lexeme{k}` occurs in the first k + 1 documents, so the
            // projection's document frequencies strictly order those terms.
            content: (number..48)
                .map(|k| format!("lexeme{k}"))
                .chain(["researchers running computers networks".to_string()])
                .collect::<Vec<_>>()
                .join(" "),
            embedding: Some(vec![number as f32 + 1.0, (48 - number) as f32]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), number.to_string()),
                ("space_id".to_string(), "team".to_string()),
            ]),
        })
        .collect()
}

fn long_query() -> String {
    // Far more analyzed terms than the scored cap once phrase pairs are added.
    (0..48)
        .map(|k| format!("lexeme{k}"))
        .chain((0..40).map(|k| format!("absent{k}")))
        .collect::<Vec<_>>()
        .join(" ")
}

fn checkpointed(name: &str) -> PathBuf {
    let path = test_dir(name);
    let mut index = SearchIndex::open(&path).unwrap();
    for document in corpus() {
        index.upsert(document).unwrap();
    }
    index.checkpoint().unwrap();
    path
}

#[test]
fn long_and_suffix_heavy_queries_are_scored_in_every_mode() {
    let path = checkpointed("long-query-modes");
    let reader = reduced_reader(&path);
    let embedding = [1.0, 0.5];
    for query in [long_query().as_str(), SENTENCE] {
        let modes: &[(SearchMode, Option<usize>)] = &[
            (SearchMode::Text, None),
            // Hybrid needs the vector capability; text-only lanes score text.
            #[cfg(feature = "vector-search")]
            (SearchMode::Hybrid, Some(16)),
        ];
        for &(mode, window) in modes {
            let output = reader
                .search_with_options(query, Some(&embedding), mode, options(10, window))
                .unwrap_or_else(|error| panic!("{mode:?} {query:?}: {error}"));
            assert_eq!(output.result.hits.len(), 10, "{mode:?}");
            assert!(
                output.result.hits.iter().any(|hit| hit.text_rank.is_some()),
                "{mode:?} must keep a lexical contribution"
            );
        }
    }
    fs::remove_dir_all(path).unwrap();
}

/// Independent oracle: rank analyzed terms by live-corpus document frequency
/// (absent terms last), break ties by term bytes, and keep the first `limit`.
fn most_selective(
    reader: &SearchOutOfCoreReader,
    terms: &BTreeSet<String>,
    limit: usize,
) -> BTreeSet<String> {
    let statistics = crate::lexical_projection::LexicalCorpusStatistics::aggregate(
        reader
            .segments
            .iter()
            .map(|segment| &segment.lexical_projection),
        terms,
        reader.lexical_term_policy.max_term_bytes(),
    )
    .unwrap();
    let mut ranked = terms
        .iter()
        .map(|term| match statistics.document_frequency(term) {
            0 => (u64::MAX, term.clone()),
            frequency => (frequency, term.clone()),
        })
        .collect::<Vec<_>>();
    ranked.sort();
    ranked
        .into_iter()
        .take(limit)
        .map(|(_, term)| term)
        .collect()
}

fn ranking(scores: &BTreeMap<String, f64>) -> Vec<(String, f64)> {
    let mut ranked = scores
        .iter()
        .map(|(id, score)| (id.clone(), *score))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(|(a, av), (b, bv)| bv.total_cmp(av).then_with(|| a.cmp(b)));
    ranked
}

#[test]
fn long_query_scores_exactly_its_most_selective_terms() {
    let path = checkpointed("long-query-selection");
    let reader = reduced_reader(&path);
    let query = long_query();
    let analyzed = reader.query_terms(&query, None).unwrap().values;
    let limit = reader.config().max_lexical_query_terms.get();
    assert_eq!(limit, crate::lexical_projection::DEFAULT_MAX_QUERY_TERMS);
    assert!(analyzed.len() > limit, "{} analyzed terms", analyzed.len());
    let expected_terms = most_selective(&reader, &analyzed, limit);
    assert!(expected_terms.iter().all(|term| !term.contains("absent")));
    assert!(expected_terms.contains("lexeme0") && !expected_terms.contains("lexeme47"));

    let budget = NonZeroUsize::new(16 << 20).unwrap();
    let ledger = hawdb_executor::QueryMemoryLedger::new(budget);
    let memory = ledger.account(
        hawdb_executor::QueryMemoryClass::BlockingState,
        "long query selection",
        budget,
    );
    let task = crate::RuntimeTaskContext::default();
    let context = crate::lexical_projection::QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    };
    for context in [None, Some(context)] {
        let mut selected = analyzed.clone();
        let selection = reader.select_query_terms(&mut selected, context).unwrap();
        assert_eq!(selected, expected_terms);
        assert_eq!(selection.analyzed, analyzed.len());
        assert_eq!(selection.kept, limit);
        assert_eq!(selection.dropped(), analyzed.len() - limit);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    let expected = reader
        .query_text_scores(
            &expected_terms,
            None,
            false,
            &CandidateSet::All(48),
            &mut Default::default(),
            None,
        )
        .unwrap();
    let expected = ranking(&expected.values);
    assert!(expected.len() > 10);
    let output = reader
        .search_with_options(&query, None, SearchMode::Text, options(48, None))
        .unwrap();
    let actual = output
        .result
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.text_score))
        .collect::<Vec<_>>();
    assert_eq!(actual.len(), expected.len());
    for ((actual_id, actual_score), (expected_id, expected_score)) in actual.iter().zip(&expected) {
        assert_eq!(actual_id, expected_id);
        assert!((actual_score - expected_score).abs() < 1e-12);
    }
    assert!(output
        .result
        .hits
        .iter()
        .flat_map(|hit| &hit.matched_terms)
        .all(|term| expected_terms.contains(term)));
    assert!(output
        .result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryTermsReduced));
    let reason = format!(
        "scored the {limit} most selective of {} analyzed terms; dropped {}",
        analyzed.len(),
        analyzed.len() - limit
    );
    assert!(
        output
            .result
            .fallback_reasons
            .iter()
            .any(|text| text.contains(&reason)),
        "{:?}",
        output.result.fallback_reasons
    );
    let text = output
        .result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert!(text
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryTermsReduced));

    // Producer seeds apply the same selection to the same statistics.
    let seeds = reader
        .text_seed_scores_with_context(&query, 48, &memory, &task, |_| Ok(true))
        .unwrap();
    assert_eq!(seeds.report.normalized_term_count, analyzed.len());
    assert_eq!(seeds.report.dropped_term_count, analyzed.len() - limit);
    assert_eq!(ranking(seeds.scores()).len(), expected.len());
    for ((id, score), (expected_id, expected_score)) in
        ranking(seeds.scores()).iter().zip(&expected)
    {
        assert_eq!(id, expected_id);
        assert!((score - expected_score).abs() < 1e-12);
    }
    drop(seeds);
    assert_eq!(ledger.snapshot().used_bytes, 0);

    // A query within the cap is scored unchanged and reports no reduction.
    let short = reader
        .search_with_options(
            "lexeme3 lexeme40",
            None,
            SearchMode::Text,
            options(48, None),
        )
        .unwrap();
    assert!(!short
        .result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryTermsReduced));
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn lexical_query_term_caps_are_configurable_and_analyzed_overflow_fails_closed() {
    let path = checkpointed("long-query-config");
    let query = long_query();
    let config = SearchOutOfCoreConfig {
        max_lexical_query_terms: NonZeroUsize::new(4).unwrap(),
        lexical_query_term_policy: crate::LexicalQueryTermPolicy::MostSelective,
        ..SearchOutOfCoreConfig::default()
    };
    let reader =
        SearchOutOfCoreReader::open_with_config_and_analyzer(&path, config, Default::default())
            .unwrap();
    let analyzed = reader.query_terms(&query, None).unwrap().values;
    let expected_terms = most_selective(&reader, &analyzed, 4);
    let output = reader
        .search_with_options(&query, None, SearchMode::Text, options(48, None))
        .unwrap();
    assert!(output
        .result
        .fallback_reasons
        .iter()
        .any(|text| text.contains("scored the 4 most selective")
            && text.contains("max_query_terms 4")));
    assert!(!output.result.hits.is_empty());
    assert!(output
        .result
        .hits
        .iter()
        .flat_map(|hit| &hit.matched_terms)
        .all(|term| expected_terms.contains(term)));

    // Genuine normalization overflow still surfaces instead of truncating.
    let config = SearchOutOfCoreConfig {
        max_lexical_query_terms: NonZeroUsize::new(4).unwrap(),
        max_lexical_query_analyzed_terms: NonZeroUsize::new(8).unwrap(),
        ..SearchOutOfCoreConfig::default()
    };
    let reader =
        SearchOutOfCoreReader::open_with_config_and_analyzer(&path, config, Default::default())
            .unwrap();
    let modes: &[SearchMode] = &[
        SearchMode::Text,
        #[cfg(feature = "vector-search")]
        SearchMode::Hybrid,
    ];
    for &mode in modes {
        let error = reader
            .search_with_options(&query, Some(&[1.0, 0.5]), mode, options(10, Some(16)))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("more than 8 analyzed terms (max_query_analyzed_terms)"),
            "{error}"
        );
    }
    // The analyzed cap is never below the scored cap.
    let config = SearchOutOfCoreConfig {
        max_lexical_query_terms: NonZeroUsize::new(4096).unwrap(),
        lexical_query_term_policy: crate::LexicalQueryTermPolicy::MostSelective,
        max_lexical_query_analyzed_terms: NonZeroUsize::new(1).unwrap(),
        ..SearchOutOfCoreConfig::default()
    };
    let reader =
        SearchOutOfCoreReader::open_with_config_and_analyzer(&path, config, Default::default())
            .unwrap();
    let output = reader
        .search_with_options(&query, None, SearchMode::Text, options(10, None))
        .unwrap();
    assert!(!output
        .result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryTermsReduced));
    fs::remove_dir_all(path).unwrap();
}

/// The long-query cases score the most selective terms (opt-in).
fn reduced_reader(path: &std::path::Path) -> SearchOutOfCoreReader {
    SearchOutOfCoreReader::open_with_config_and_analyzer(
        path,
        SearchOutOfCoreConfig {
            lexical_query_term_policy: crate::LexicalQueryTermPolicy::MostSelective,
            ..SearchOutOfCoreConfig::default()
        },
        Default::default(),
    )
    .unwrap()
}

/// By default a query above the scored-term cap is refused.
#[test]
fn default_policy_refuses_queries_above_the_scored_term_cap() {
    let path = checkpointed("long-query-default-refuses");
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let query = (0..reader.config().max_lexical_query_terms.get() + 8)
        .map(|k| format!("term{k}"))
        .collect::<Vec<_>>()
        .join(" ");
    let error = reader
        .search_with_options(&query, None, SearchMode::Text, options(5, None))
        .map(|_| ())
        .unwrap_err();
    assert!(error.to_string().contains("max_query_terms"), "{error}");
}

/// `Exact` refuses before any corpus statistics are read. The seed's query
/// tokenization is accounted, so its budget is the peak an `Exact` refusal
/// needs; `MostSelective` reads statistics past that peak and hits the budget.
#[test]
fn exact_refusal_reads_no_corpus_statistics() {
    let path = checkpointed("long-query-exact-no-statistics");
    let query = long_query();
    let task = crate::RuntimeTaskContext::default();
    let exact = SearchOutOfCoreReader::open(&path).unwrap();
    let reduced = reduced_reader(&path);

    // Term selection reads statistics only through the account.
    let tiny = NonZeroUsize::new(1).unwrap();
    let ledger = hawdb_executor::QueryMemoryLedger::new(tiny);
    let memory = ledger.account(
        hawdb_executor::QueryMemoryClass::BlockingState,
        "exact refusal",
        tiny,
    );
    let context = crate::lexical_projection::QueryContext {
        memory: &memory,
        task: &task,
        execution: None,
        io_scope: None,
    };
    for (reader, refused) in [(&exact, true), (&reduced, false)] {
        let mut terms = reader.query_terms(&query, None).unwrap().values;
        let error = reader
            .select_query_terms(&mut terms, Some(context))
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert_eq!(error.contains("max_query_terms"), refused, "{error}");
    }
    drop(memory);

    // Producer seeds: the budget is exactly the Exact refusal's peak.
    let seed = |reader: &SearchOutOfCoreReader, budget: NonZeroUsize| {
        let ledger = hawdb_executor::QueryMemoryLedger::new(budget);
        let memory = ledger.account(
            hawdb_executor::QueryMemoryClass::BlockingState,
            "exact seed refusal",
            budget,
        );
        let error = reader
            .text_seed_scores_with_context(&query, 48, &memory, &task, |_| Ok(true))
            .map(|_| ())
            .unwrap_err()
            .to_string();
        drop(memory);
        (error, ledger.snapshot().peak_bytes)
    };
    let (error, peak) = seed(&exact, NonZeroUsize::new(16 << 20).unwrap());
    assert!(error.contains("max_query_terms"), "{error}");
    let budget = NonZeroUsize::new(peak.max(1)).unwrap();
    let (error, _) = seed(&exact, budget);
    assert!(error.contains("max_query_terms"), "{error}");
    let (error, _) = seed(&reduced, budget);
    assert!(
        !error.contains("max_query_terms") && error.contains("budget"),
        "{error}"
    );
    fs::remove_dir_all(path).unwrap();
}
