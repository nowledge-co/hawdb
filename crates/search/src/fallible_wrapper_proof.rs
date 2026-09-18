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

//! Public in-memory error propagation and payload-policy regressions.

use super::*;

#[derive(Clone, Copy, Debug)]
enum Wrapper {
    Scalar,
    Preferred,
    Compressed,
    Adaptive,
}

const WRAPPERS: [Wrapper; 4] = [
    Wrapper::Scalar,
    Wrapper::Preferred,
    Wrapper::Compressed,
    Wrapper::Adaptive,
];

fn options() -> SearchQueryOptions {
    SearchQueryOptions {
        limit: 5,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::from([("space_id".into(), "workspace".into())]),
        policy_epoch: None,
    }
}

fn public_wrapper(
    index: &SearchIndex,
    mode: SearchMode,
    wrapper: Wrapper,
) -> Result<SearchResultSet> {
    match wrapper {
        Wrapper::Scalar => index.search_with_options("graph", Some(&[1.0, 0.0]), mode, options()),
        Wrapper::Preferred => index.search_with_options_prefer_compressed_vector_projection(
            "graph",
            Some(&[1.0, 0.0]),
            mode,
            options(),
        ),
        Wrapper::Compressed => index.search_with_options_compressed_vector_projection_mode(
            "graph",
            Some(&[1.0, 0.0]),
            mode,
            options(),
            CompressedVectorSearchMode::Disabled,
        ),
        Wrapper::Adaptive => index.search_with_options_adaptive_vector_projection(
            "graph",
            Some(&[1.0, 0.0]),
            mode,
            options(),
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred),
        ),
    }
}

#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn checked(index: &SearchIndex, mode: SearchMode, wrapper: Wrapper) -> Result<SearchResultSet> {
    match wrapper {
        Wrapper::Scalar => index.try_search_with_options_using_vector_backend(
            "graph",
            Some(&[1.0, 0.0]),
            mode,
            options(),
            SearchExecutionStrategy::fixed(VectorSearchBackend::Scalar, None, false),
            None,
        ),
        _ => index.try_search_with_options_compressed_vector_projection_mode_internal(
            "graph",
            Some(&[1.0, 0.0]),
            mode,
            options(),
            AdaptiveVectorSearchOptions::new(if matches!(wrapper, Wrapper::Compressed) {
                CompressedVectorSearchMode::Disabled
            } else {
                CompressedVectorSearchMode::Preferred
            }),
            AdaptiveVectorExecutionControls::IN_MEMORY,
        ),
    }
}

#[test]
fn public_wrappers_return_the_first_unavailable_capability() {
    for text in [false, true] {
        for vector in [false, true] {
            let mut index = SearchIndex::in_memory();
            index.set_runtime_capabilities(
                RuntimeCapabilities::default()
                    .with(RuntimeCapability::FullTextSearch, text)
                    .with(RuntimeCapability::VectorSearch, vector),
            );
            for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
                let required: &[RuntimeCapability] = match mode {
                    SearchMode::Text => &[RuntimeCapability::FullTextSearch],
                    SearchMode::Vector => &[RuntimeCapability::VectorSearch],
                    SearchMode::Hybrid => &[
                        RuntimeCapability::FullTextSearch,
                        RuntimeCapability::VectorSearch,
                    ],
                };
                let Some(&capability) = required
                    .iter()
                    .find(|&&capability| !index.runtime_capabilities().is_enabled(capability))
                else {
                    continue;
                };
                let expected = HawDBError::CapabilityUnavailable { capability };
                for wrapper in WRAPPERS {
                    assert_eq!(public_wrapper(&index, mode, wrapper).unwrap_err(), expected);
                }
                for (query, limit) in [("graph", 5), ("", 0)] {
                    assert_eq!(
                        index
                            .search(query, Some(&[1.0, 0.0]), mode, limit)
                            .unwrap_err(),
                        expected
                    );
                    assert_eq!(
                        index
                            .search_with_report(query, Some(&[1.0, 0.0]), mode, limit)
                            .unwrap_err(),
                        expected
                    );
                }
            }
        }
    }
}

#[cfg(feature = "full-text-search")]
fn populate(index: &mut SearchIndex) {
    for (id, text, vector) in [
        ("first", "graph graph storage", [1.0, 0.0]),
        ("second", "graph retrieval", [0.5, 0.5]),
    ] {
        index
            .upsert(SearchDocument {
                id: id.into(),
                title: String::new(),
                content: text.into(),
                embedding: Some(vector.to_vec()),
                metadata: BTreeMap::from([("space_id".into(), "workspace".into())]),
            })
            .unwrap();
    }
}

#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
#[test]
fn checked_in_memory_search_keeps_complete_enabled_results() {
    let mut index = SearchIndex::in_memory();
    populate(&mut index);
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        for wrapper in WRAPPERS {
            assert_eq!(
                checked(&index, mode, wrapper).unwrap(),
                public_wrapper(&index, mode, wrapper).unwrap()
            );
        }
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn existing_try_api_is_not_a_drop_in_replacement_for_in_memory_access() {
    struct Directory(PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let root = Directory(std::env::temp_dir().join(format!(
        "hawdb-fallible-contract-{}-{}", std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    )));
    let mut index = SearchIndex::open(&root.0).unwrap();
    populate(&mut index);
    for ordinal in 0..SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
        index
            .upsert(SearchDocument {
                id: format!("zzz-{ordinal:04}"),
                title: String::new(),
                content: "outside the requested workspace".into(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".into(), "other".into())]),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let index = SearchIndex::open(&root.0).unwrap();
    let before = public_wrapper(&index, SearchMode::Text, Wrapper::Scalar).unwrap();
    assert_eq!(before.total_hits, 2);
    let before_hits = index.search("graph", None, SearchMode::Text, 5).unwrap();
    fs::write(root.0.join(SEARCH_SEGMENT_PAYLOAD_FILE), b"torn payload").unwrap();
    for wrapper in WRAPPERS {
        assert_eq!(
            public_wrapper(&index, SearchMode::Text, wrapper).unwrap(),
            before
        );
    }
    assert_eq!(
        index.search("graph", None, SearchMode::Text, 5).unwrap(),
        before_hits
    );

    assert!(index
        .try_search_with_options("graph", None, SearchMode::Text, options())
        .is_err());
}
