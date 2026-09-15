//! Private proof for the return-type proposal; no public signature changes.

use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

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

fn legacy(index: &SearchIndex, mode: SearchMode, wrapper: Wrapper) -> SearchResultSet {
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
fn disabled_capabilities_can_return_typed_errors_without_a_different_read_policy() {
    for (mode, capability) in [
        (SearchMode::Text, RuntimeCapability::FullTextSearch),
        (SearchMode::Vector, RuntimeCapability::VectorSearch),
    ] {
        let mut index = SearchIndex::in_memory();
        index.set_runtime_capabilities(RuntimeCapabilities::default().with(capability, false));
        for wrapper in WRAPPERS {
            assert!(matches!(checked(&index, mode, wrapper),
                Err(SkeinError::CapabilityUnavailable { capability: actual }) if actual == capability));
            assert!(catch_unwind(AssertUnwindSafe(|| legacy(&index, mode, wrapper))).is_err());
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
                legacy(&index, mode, wrapper)
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
        "skein-fallible-contract-{}-{}", std::process::id(),
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
    let before = legacy(&index, SearchMode::Text, Wrapper::Scalar);
    assert_eq!(before.total_hits, 2);
    fs::write(root.0.join(SEARCH_SEGMENT_PAYLOAD_FILE), b"torn payload").unwrap();
    assert_eq!(
        checked(&index, SearchMode::Text, Wrapper::Scalar).unwrap(),
        before
    );
    assert!(index
        .try_search_with_options("graph", None, SearchMode::Text, options())
        .is_err());
}
