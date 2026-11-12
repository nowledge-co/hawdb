// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Embedded out-of-core candidate reads charge every eligible-set-scaled
//! allocation to the caller's admitted working memory and refuse, with a typed
//! budget error, a request that does not fit (#1722). A hybrid request without
//! a rank window ranks the complete eligible set in both legs.

use super::exact_candidate_tests::{
    embedding, handle, memory_filter, read_config, snapshot_search, Root,
};
use super::*;
use hawdb_core::ResourceBudgetKind;
use std::num::NonZeroUsize;

const LIMIT: usize = 10;
const LARGE_BUDGET: usize = 256 << 20;

fn requests() -> Vec<(&'static str, SearchCandidateRequest)> {
    let query = embedding(5_005);
    let text = SearchCandidateRequest::text("storage", LIMIT);
    let vector = SearchCandidateRequest::vector(query.clone(), LIMIT);
    let hybrid = SearchCandidateRequest::hybrid("storage", query, LIMIT);
    [
        ("text", text),
        ("vector", vector.clone()),
        ("vector exact", vector.with_exact_vector_search()),
        ("hybrid", hybrid.clone()),
        ("hybrid exact", hybrid.with_exact_vector_search()),
    ]
    .into_iter()
    .map(|(name, request)| (name, request.with_metadata_filters(memory_filter())))
    .collect()
}

fn store(name: &str, budget: usize) -> (Root, EmbeddedStoreHandle) {
    let root = Root::new(name);
    let mut config = read_config();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(budget).unwrap();
    let handle = handle(&root, true, config);
    (root, handle)
}

fn charged_bytes(output: &SearchCandidateOutput) -> u64 {
    output
        .out_of_core_metrics
        .as_ref()
        .expect("out-of-core metrics")
        .query_memory_peak_bytes
}

#[test]
fn snapshot_candidate_reads_charge_admitted_working_memory() {
    let (_root, handle) = store("ooc-working-large", LARGE_BUDGET);
    let mut hybrid_exact = 0;
    for (name, request) in requests() {
        let output = snapshot_search(&handle, &request).unwrap();
        assert_eq!(output.result.hits.len(), LIMIT, "{name}");
        let charged = charged_bytes(&output);
        assert!(
            charged > 0 && charged <= LARGE_BUDGET as u64,
            "{name}: {charged}"
        );
        if name == "hybrid exact" {
            hybrid_exact = charged;
        }
        // The admitted handle read charges the same account kind.
        let output = handle.search_candidates_with_report(&request).unwrap();
        assert!(charged_bytes(&output) > 0, "{name}");
    }
    // The unwindowed hybrid working set exceeds every page-bounded read's.
    assert!(
        hybrid_exact > page_bounded_admission(&handle) as u64,
        "{hybrid_exact}"
    );
}

/// The smallest admission serving text, exact vector-only and windowed exact
/// hybrid reads. Default vector stages size themselves to the remaining bytes.
fn page_bounded_admission(handle: &EmbeddedStoreHandle) -> usize {
    requests()
        .into_iter()
        .filter(|(name, _)| matches!(*name, "text" | "vector exact" | "hybrid exact"))
        .map(|(name, request)| {
            let request = if name == "hybrid exact" {
                request.with_rank_window(Some(LIMIT))
            } else {
                request
            };
            charged_bytes(&snapshot_search(handle, &request).unwrap()) as usize
        })
        .max()
        .unwrap()
}

#[test]
fn snapshot_hybrid_without_rank_window_is_refused_above_admitted_working_memory() {
    let small = {
        let (_root, handle) = store("ooc-working-measure", LARGE_BUDGET);
        page_bounded_admission(&handle)
    };
    let (_root, handle) = store("ooc-working-small", small);
    for (name, request) in requests() {
        let result = snapshot_search(&handle, &request);
        if name == "hybrid exact" {
            match result {
                Err(HawDBError::ResourceBudgetExceeded(error)) => {
                    assert_eq!(error.resource, ResourceBudgetKind::MemoryBytes, "{error}");
                    assert_eq!(error.budget, small as u64, "{error}");
                }
                other => panic!("hybrid exact served above its admission: {other:?}"),
            }
            // A rank window bounds both rankings and fits the same admission.
            let windowed =
                snapshot_search(&handle, &request.with_rank_window(Some(LIMIT))).unwrap();
            assert_eq!(windowed.result.hits.len(), LIMIT);
            assert!(charged_bytes(&windowed) <= small as u64);
        } else if name != "hybrid" {
            // Text and vector-only reads are unaffected at this admission.
            let output = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(output.result.hits.len(), LIMIT, "{name}");
            assert!(charged_bytes(&output) <= small as u64, "{name}");
        } else if let Ok(output) = result {
            // Default hybrid retains only the bounded graph window in its vector leg.
            assert!(charged_bytes(&output) <= small as u64);
        }
    }
}
