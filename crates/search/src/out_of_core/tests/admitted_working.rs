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
const DOCUMENTS: usize = 600;
const LIMIT: usize = 10;
// Text and vector-only reads retain only their page; hybrid without a rank
// window charges both complete rankings over every eligible document.
const SMALL_BUDGET: usize = 4 << 20;
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
    for number in 0..DOCUMENTS {
        writer
            .push(SearchDocument {
                id: format!("memory:{number:04}"),
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
    for exact in [false, true] {
        // Text and vector-only reads fit the small admission.
        for mode in [SearchMode::Text, SearchMode::Vector] {
            let measured = search(&reader, mode, exact, None, SMALL_BUDGET);
            let output = measured
                .result
                .unwrap_or_else(|error| panic!("{mode:?} exact={exact}: {error}"));
            assert_eq!(output.result.hits.len(), LIMIT, "{mode:?} exact={exact}");
            let charged = output.metrics.query_memory_peak_bytes as usize;
            assert!(
                charged > 0 && charged <= SMALL_BUDGET,
                "{mode:?}: {charged}"
            );
        }

        // Hybrid without a rank window ranks every eligible document in both legs.
        let refused = search(&reader, SearchMode::Hybrid, exact, None, SMALL_BUDGET);
        assert_refused(&refused, SMALL_BUDGET);
        // A rank window bounds both rankings, so the same admission serves it.
        let windowed = search(
            &reader,
            SearchMode::Hybrid,
            exact,
            Some(LIMIT),
            SMALL_BUDGET,
        )
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
        assert!(
            charged > SMALL_BUDGET && charged <= LARGE_BUDGET,
            "{charged}"
        );

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
