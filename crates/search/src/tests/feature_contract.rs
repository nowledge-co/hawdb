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

//! Feature-independent algorithms and compiled/runtime admission contracts.

use super::*;

#[test]
fn search_modes_obey_compiled_and_requested_capabilities() {
    let path = unique_test_dir("search_feature_contract");
    let document = doc("a", "Graph storage", "Graph evidence", [1.0, 0.0]);
    let mut index = SearchIndex::open(&path).unwrap();
    index.upsert(document.clone()).unwrap();
    index.checkpoint().unwrap();
    let mut reader = SearchOutOfCoreReader::open(&path).unwrap();

    for text in [false, true] {
        for vector in [false, true] {
            let requested = RuntimeCapabilities::default()
                .with(RuntimeCapability::FullTextSearch, text)
                .with(RuntimeCapability::VectorSearch, vector);
            let effective = requested.intersection(compiled_runtime_capabilities());
            index.set_runtime_capabilities(requested);
            reader.set_runtime_capabilities(requested);
            assert_eq!(index.runtime_capabilities(), effective);
            assert_eq!(reader.runtime_capabilities(), effective);

            for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
                for query in ["graph", ""] {
                    for limit in [0, 1] {
                        let options = SearchQueryOptions {
                            limit,
                            offset: 0,
                            rank_window: None,
                            fusion_weights: SearchFusionWeights::default(),
                            metadata_filters: BTreeMap::new(),
                            policy_epoch: None,
                        };
                        let resident = index.try_search_with_options(
                            query,
                            Some(&[1.0, 0.0]),
                            mode,
                            options.clone(),
                        );
                        let out_of_core = reader
                            .search_with_options(query, Some(&[1.0, 0.0]), mode, options)
                            .map(|output| output.result);
                        for result in [resident, out_of_core] {
                            assert_mode_admission(result, mode, effective, query, limit);
                        }
                    }
                }
            }
        }
    }

    // Canonical artifact hydration does not require either serving capability.
    assert_eq!(
        reader
            .hydrate_documents(&["a".to_string()])
            .unwrap()
            .documents,
        vec![document]
    );
    drop(reader);
    drop(index);
    fs::remove_dir_all(path).unwrap();
}

fn assert_mode_admission(
    result: Result<SearchResultSet>,
    mode: SearchMode,
    available: RuntimeCapabilities,
    query: &str,
    limit: usize,
) {
    let required: &[RuntimeCapability] = match mode {
        SearchMode::Text => &[RuntimeCapability::FullTextSearch],
        SearchMode::Vector => &[RuntimeCapability::VectorSearch],
        SearchMode::Hybrid => &[
            RuntimeCapability::FullTextSearch,
            RuntimeCapability::VectorSearch,
        ],
    };
    if required
        .iter()
        .all(|capability| available.is_enabled(*capability))
    {
        let output = result.unwrap();
        assert_eq!(output.document_count, 1);
        if !query.is_empty() && limit != 0 {
            assert_eq!(output.hits.len(), 1);
            assert_eq!(output.hits[0].id, "a");
        }
    } else {
        let error = result.unwrap_err();
        assert!(
            matches!(error, HawDBError::CapabilityUnavailable { capability }
                if required.contains(&capability) && !available.is_enabled(capability)),
            "{mode:?}: {error}"
        );
    }
}

#[test]
fn background_delta_obeys_compiled_and_requested_capabilities() {
    for requested in [false, true] {
        for scheduled in [false, true] {
            let mut index = SearchIndex::in_memory();
            let original = doc("old", "Old", "Unchanged on rejection", [1.0, 0.0]);
            index.upsert(original.clone()).unwrap();
            index.set_runtime_capabilities(
                RuntimeCapabilities::default()
                    .with(RuntimeCapability::BackgroundMaintenance, requested),
            );
            let delta = SearchProjectionDelta {
                upserts: Vec::new(),
                deletes: vec!["old".to_string()],
                max_operations: Some(1),
                source_graph_commit_epoch: Some(42),
            };
            let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());
            let result = if scheduled {
                index.apply_scheduled_background_projection_delta(&scheduler, delta)
            } else {
                index.apply_background_projection_delta(
                    &LocalQosPolicy::default(),
                    &LocalQosState::default(),
                    delta,
                )
            };
            if requested && cfg!(feature = "background-maintenance") {
                assert_eq!(result.unwrap().operation_count, 1);
                assert_eq!(index.document_count(), 0);
            } else {
                assert_eq!(
                    result.unwrap_err(),
                    HawDBError::CapabilityUnavailable {
                        capability: RuntimeCapability::BackgroundMaintenance
                    }
                );
                assert_eq!(index.document("old"), Some(&original));
                assert_eq!(index.source_graph_commit_epoch, None);
            }
            assert_eq!(scheduler.state().running_background_operations, 0);
        }
    }
}

#[test]
fn analyzer_identifiers_and_stems_are_feature_independent() {
    let lexicon = SearchAnalyzerLexicon::default();
    for (input, expected) in [
        (
            "Nowledge_Source",
            vec!["nowledge", "source", "nowledge_source"],
        ),
        (
            "SourceChunk parserV2",
            vec!["source", "chunk", "parser", "v", "2", "v_2"],
        ),
        (
            "LSMTree HTTPServer MVCCSnapshot",
            vec!["lsm", "tree", "http", "server", "mvcc", "snapshot"],
        ),
        (
            "Memories sources threading archived chunks",
            vec!["memory", "source", "thread", "archive", "chunk"],
        ),
    ] {
        let tokens = tokenize(input, &lexicon);
        for term in expected {
            assert!(
                tokens.contains(term),
                "{input}: missing {term} in {tokens:?}"
            );
        }
    }
    assert_eq!(
        tokenize("GRAPH storage", &lexicon),
        tokenize("graph storage", &lexicon)
    );
}

#[test]
fn analyzer_dictionary_terms_are_feature_independent() {
    let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
    let source = format!("\u{73b0}\u{4ee3}{term}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}");
    let tokens = tokenize(&source, &SearchAnalyzerLexicon::default());
    assert!(tokens.contains(term));
    assert!(tokens.contains("\u{6570}\u{636e}\u{5e93}"));
}

#[test]
fn analyzer_aliases_and_stopwords_are_feature_independent() {
    let default = SearchAnalyzerLexicon::default();
    assert!(
        tokenize("crystallization", &default).is_disjoint(&tokenize("Crystal memory", &default))
    );
    let application = SearchAnalyzerLexicon::nowledge_memory();
    assert!(!tokenize("crystallization", &application)
        .is_disjoint(&tokenize("Crystal memory", &application)));

    let normalized = default
        .with_normalized_alias_rule(["raw evidence", "RawEvidence"], ["episodic provenance"])
        .with_stopwords(["memory lifecycle", "thread"]);
    let tokens = tokenize("RawEvidence MemoryLifecycle Thread checkpoint", &normalized);
    assert!(tokens.contains("episodic_provenance"));
    assert!(tokens.contains("checkpoint"));
    for ignored in ["memory", "lifecycle", "memory_lifecycle", "thread"] {
        assert!(!tokens.contains(ignored), "{ignored}: {tokens:?}");
    }
}
