use super::*;
use crate::out_of_core::{SearchOutOfCoreConfig, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use crate::{SearchDocument, SearchIndex};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) fn fixture() -> (PathBuf, SearchOutOfCoreReader) {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "skein-filter-admission-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut index = SearchIndex::open(&path).unwrap();
    for number in 0..9 {
        index
            .upsert(SearchDocument {
                id: format!("memory:{number:03}"),
                title: "graph".into(),
                content: "memory storage".into(),
                embedding: None,
                metadata: BTreeMap::from([
                    (
                        "space_id".into(),
                        ["team", "shared", "private"][number % 3].into(),
                    ),
                    ("kind".into(), "memory".into()),
                    ("is_latest".into(), (number % 2 == 0).to_string()),
                    ("created_at".into(), number.to_string()),
                    ("event_start".into(), number.to_string()),
                    (
                        "temporal_context".into(),
                        if number % 2 == 0 { "past" } else { "future" }.into(),
                    ),
                ]),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let reader = SearchOutOfCoreReader::open_with_config(
        &path,
        SearchOutOfCoreConfig {
            spill_directory: path.join("spill"),
            ..SearchOutOfCoreConfig::default()
        },
    )
    .unwrap();
    (path, reader)
}

#[test]
fn query_filter_admission_uses_task_root_before_candidate_io() {
    use crate::out_of_core::{query_io, SearchOutOfCoreExecutionContext};
    use crate::{SearchMode, SearchQueryOptions, VectorSearchExecutionOptions};
    let (path, reader) = fixture();
    let mut value = String::with_capacity(4096);
    value.push_str("team");
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(2048, 2048));
    let options = SearchQueryOptions {
        limit: 3,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::from([("space".into(), value)]),
        policy_epoch: None,
    };
    take();
    query_io::evidence::take();
    let result = reader.search_with_options_internal(
        "graph",
        None,
        SearchMode::Text,
        options,
        SearchOutOfCoreExecutionContext {
            vector_execution_options: VectorSearchExecutionOptions {
                task_context: Some(&task),
                ..Default::default()
            },
            ..SearchOutOfCoreExecutionContext::scalar()
        },
    );
    assert!(result.is_err(), "query must reject before candidate I/O");
    let error = result.unwrap_err().to_string();
    #[cfg(feature = "full-text-search")]
    assert!(error.contains("query memory"), "{error}");
    #[cfg(not(feature = "full-text-search"))]
    assert!(
        error.contains("capability unavailable: full_text_search"),
        "{error}"
    );
    assert_eq!(take(), Evidence::default());
    assert_eq!(query_io::evidence::take(), (0, 0));
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(all(feature = "full-text-search", feature = "acl"))]
#[test]
fn successful_query_keeps_requested_report_filters_and_applies_acl() {
    use crate::out_of_core::SearchOutOfCoreExecutionContext;
    use crate::{SearchMode, SearchQueryOptions};
    let (path, reader) = fixture();
    let access = SearchAccessControlContext::visibility_scopes(7, "space_id", ["team", "shared"]);
    let requested = filters("history", "true");
    let options = SearchQueryOptions {
        limit: 9,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: requested.clone(),
        policy_epoch: Some(7),
    };
    let output = reader
        .search_with_options_internal(
            "graph",
            None,
            SearchMode::Text,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: Some(&access),
                ..SearchOutOfCoreExecutionContext::scalar()
            },
        )
        .unwrap();
    let mut ids = output
        .result
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, ["memory:001", "memory:003", "memory:007"]);
    assert_eq!(output.result.candidate_set.metadata_filters, requested);
    assert_eq!(output.result.candidate_set.policy_epoch, Some(7));
    drop(reader);
    std::fs::remove_dir_all(path).unwrap();
}

pub(super) fn assert_candidates(
    reader: &SearchOutOfCoreReader,
    input: &Input,
    report: &mut SearchPredicatePushdownReport,
    memory: &QueryMemory,
    expected: &[bool; 9],
) -> usize {
    let mut metrics = SearchOutOfCoreMetrics::default();
    let set = reader
        .build_candidate_set(
            &input.predicates,
            report,
            &mut metrics,
            memory,
            Some(&RuntimeTaskContext::default()),
        )
        .unwrap();
    assert_eq!(
        set.cardinality(),
        expected.iter().filter(|value| **value).count()
    );
    for (number, expected) in expected.iter().enumerate() {
        assert_eq!(
            set.contains(&format!("memory:{number:03}"), &mut metrics)
                .unwrap(),
            *expected
        );
    }
    set.cardinality()
}
