// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Ordinary out-of-core reads under a caller's runtime admission charge every
//! eligible-set-scaled allocation to one working account (#1722). Live
//! allocations, not RSS, show the charge covers the retained working set and
//! that a refused query never allocates beyond its admission.

use super::*;
use crate::test_allocation;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};

const DIMENSION: usize = 8;
const DOCUMENTS: usize = 4_000;
const LIMIT: usize = 10;
const LARGE_BUDGET: usize = 256 << 20;

fn embedding(number: usize) -> Vec<f32> {
    let mut state = (number as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 0.6
        })
        .collect()
}

fn reader() -> SearchOutOfCoreReader {
    reader_with(DOCUMENTS)
}

fn reader_with(documents: usize) -> SearchOutOfCoreReader {
    let path = test_dir("admitted-working");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "admitted-working".into(),
                version: Some("1".into()),
                dimension: DIMENSION,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..documents {
        writer
            .push(SearchDocument {
                id: format!("memory:{number:06}"),
                title: format!("note {number}"),
                content: "storage vectors".into(),
                embedding: Some(embedding(number)),
                metadata: BTreeMap::from([(
                    "kind".to_string(),
                    if number % 4 == 0 { "source" } else { "memory" }.to_string(),
                )]),
            })
            .unwrap();
    }
    writer.finish().unwrap();
    SearchOutOfCoreReader::open(&path).unwrap()
}

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(4 << 30), Some(4 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

struct Measured {
    result: Result<SearchOutOfCoreOutput>,
    // Peak live bytes above the baseline while the query ran.
    peak: usize,
    // Live bytes the returned, caller-owned result still holds.
    retained_result: usize,
}

fn search(
    reader: &SearchOutOfCoreReader,
    mode: SearchMode,
    exact: bool,
    rank_window: Option<usize>,
    budget: usize,
) -> Measured {
    let governor = governor();
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::foreground_query(budget as u64, 1 << 20)
                .with_io_slots(1)
                .with_blocking(true),
        )
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let mut vector = VectorSearchExecutionOptions::admitted(budget, &task);
    if exact {
        vector = vector.with_exact_search();
    }
    let mut options = options(LIMIT, rank_window);
    options
        .metadata_filters
        .insert("kind".into(), "memory".into());
    let query = embedding(5_005);
    let baseline = test_allocation::live();
    let (result, peak) = test_allocation::measure(|| {
        reader.search_with_options_admitted(
            "storage",
            (mode != SearchMode::Text).then_some(query.as_slice()),
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: None,
                compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
                vector_execution_options: vector,
            },
            &permit,
        )
    });
    let retained_result = test_allocation::live().saturating_sub(baseline);
    Measured {
        result,
        peak: peak.saturating_sub(baseline),
        retained_result,
    }
}

fn assert_refused(measured: &Measured, budget: usize) {
    match &measured.result {
        Err(HawDBError::ResourceBudgetExceeded(error)) => {
            assert_eq!(error.resource, ResourceBudgetKind::MemoryBytes, "{error}");
            assert_eq!(error.budget, budget as u64, "{error}");
            assert_eq!(
                error.budget_option, "VectorSearchExecutionOptions.max_working_bytes",
                "{error}"
            );
        }
        other => panic!("expected a typed working-memory refusal, got {other:?}"),
    }
    // Refusal happens before the query allocates beyond its admission.
    assert!(
        measured.peak <= budget,
        "refused query allocated {} live bytes over its {budget}-byte admission",
        measured.peak
    );
}

#[test]
fn hybrid_without_rank_window_is_charged_and_refused_below_its_working_set() {
    let _serial = test_allocation::serial();
    let reader = reader();
    let charged = |mode, exact, rank_window| {
        let output = search(&reader, mode, exact, rank_window, LARGE_BUDGET)
            .result
            .unwrap_or_else(|error| panic!("{mode:?} exact={exact}: {error}"));
        assert_eq!(output.result.hits.len(), LIMIT, "{mode:?} exact={exact}");
        output.metrics.query_memory_peak_bytes as usize
    };
    // The smallest admission that serves every page-bounded exact read: text,
    // vector-only, and hybrid with a rank window. Default vector stages size
    // their working set to the account's remaining bytes, so they fit any
    // admission that leaves room for the rest of the query.
    let small = [
        charged(SearchMode::Text, false, None),
        charged(SearchMode::Vector, true, None),
        charged(SearchMode::Hybrid, true, Some(LIMIT)),
    ]
    .into_iter()
    .max()
    .unwrap();
    for exact in [false, true] {
        // Text and vector-only reads fit the small admission.
        for mode in [SearchMode::Text, SearchMode::Vector] {
            let measured = search(&reader, mode, exact, None, small);
            let output = measured
                .result
                .unwrap_or_else(|error| panic!("{mode:?} exact={exact}: {error}"));
            assert_eq!(output.result.hits.len(), LIMIT, "{mode:?} exact={exact}");
            let charged = output.metrics.query_memory_peak_bytes as usize;
            assert!(charged > 0 && charged <= small, "{mode:?}: {charged}");
        }

        // Exact hybrid without a rank window ranks every eligible document in
        // both legs. (Default hybrid's vector leg is bounded by its graph
        // window; only its text ranking scales with the eligible set.)
        if exact {
            let refused = search(&reader, SearchMode::Hybrid, exact, None, small);
            assert_refused(&refused, small);
        }
        // A rank window bounds both rankings, so the same admission serves it.
        let windowed = search(&reader, SearchMode::Hybrid, exact, Some(LIMIT), small)
            .result
            .unwrap_or_else(|error| panic!("windowed hybrid exact={exact}: {error}"));
        assert_eq!(windowed.result.hits.len(), LIMIT);

        let served = search(&reader, SearchMode::Hybrid, exact, None, LARGE_BUDGET);
        let working = served.peak.saturating_sub(served.retained_result);
        let output = served.result.unwrap();
        assert_eq!(output.result.hits.len(), LIMIT);
        // Every eligible memory scores in both legs: nothing is truncated.
        assert_eq!(output.result.total_hits, DOCUMENTS - DOCUMENTS / 4);
        let charged = output.metrics.query_memory_peak_bytes as usize;
        assert!(
            charged >= working,
            "exact={exact}: charged {charged} bytes below {working} live working bytes"
        );
        assert!(charged > small && charged <= LARGE_BUDGET, "{charged}");

        // Exact scoring has no adaptive stage: one byte below the charged
        // peak is refused, again within admission. (The default RaBitQ stage
        // sizes its scan to whatever the account still has available.)
        if exact {
            assert_refused(
                &search(&reader, SearchMode::Hybrid, exact, None, charged - 1),
                charged - 1,
            );
        }
    }
}

#[test]
fn exact_vector_and_unwindowed_hybrid_charges_track_live_allocations() {
    // Charges derive from the std B-tree layout at minimum occupancy, so they
    // must cover live working bytes without exceeding them severalfold.
    const ELIGIBLE_DOCUMENTS: usize = 20_000;
    let _serial = test_allocation::serial();
    let reader = reader_with(ELIGIBLE_DOCUMENTS * 4 / 3);
    for mode in [SearchMode::Vector, SearchMode::Hybrid] {
        let measured = search(&reader, mode, true, None, LARGE_BUDGET);
        let working = measured.peak.saturating_sub(measured.retained_result);
        let output = measured.result.unwrap();
        assert_eq!(output.result.hits.len(), LIMIT);
        let charged = output.metrics.query_memory_peak_bytes as usize;
        assert!(
            charged >= working,
            "{mode:?}: charged {charged} < live {working}"
        );
        assert!(
            charged <= 2 * working,
            "{mode:?}: charged {charged} > 2x live {working}"
        );
    }
}
