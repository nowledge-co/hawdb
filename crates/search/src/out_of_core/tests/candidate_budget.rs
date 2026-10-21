// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn candidate_block_budget_refuses_one_byte_below_exact_fit() {
    check_candidate_budget(ResourceBudgetKind::MemoryBytes);
}

#[test]
fn candidate_spill_budget_counts_prior_blocks_and_excludes_header() {
    check_candidate_budget(ResourceBudgetKind::SpillBytes);
}

fn check_candidate_budget(resource: ResourceBudgetKind) {
    let path = test_dir("candidate-byte-budget");
    let spill = path.join("spill");
    fs::create_dir_all(&spill).unwrap();
    let unrelated = spill.join("unrelated");
    fs::write(&unrelated, b"preserve unrelated spill ownership").unwrap();
    let assert_clean = || {
        let files = fs::read_dir(&spill)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(files, vec![unrelated.clone()]);
        assert_eq!(
            fs::read(&unrelated).unwrap(),
            b"preserve unrelated spill ownership"
        );
    };

    let mut index = SearchIndex::open(&path).unwrap();
    for number in 0..6 {
        let space = if number < 5 { "team" } else { "private" };
        index.upsert(document(number, space)).unwrap();
    }
    index.checkpoint().unwrap();
    let mut filtered = options(10, None);
    filtered
        .metadata_filters
        .insert("space_id".to_string(), "team".to_string());
    let expected = index
        .try_search_with_options("graph", None, SearchMode::Text, filtered.clone())
        .unwrap();
    assert_eq!(expected.total_hits, 5);
    assert_eq!(expected.hits.len(), 5);
    let unfiltered = index
        .try_search_with_options("graph", None, SearchMode::Text, options(10, None))
        .unwrap();
    drop(index);

    // Each test descriptor covers two documents. Filtering the last document
    // leaves blocks of 44, 44 and 22 bytes (u32 length + ten-byte ID + u64 ordinal).
    assert_eq!(crate::SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS, 2);
    let exact = SearchOutOfCoreConfig {
        spill_directory: spill.clone(),
        max_candidate_block_bytes: NonZeroU64::new(44).unwrap(),
        max_candidate_spill_bytes: NonZeroU64::new(110).unwrap(),
        ..SearchOutOfCoreConfig::default()
    };
    let mut constrained = exact.clone();
    match resource {
        ResourceBudgetKind::MemoryBytes => {
            constrained.max_candidate_block_bytes = NonZeroU64::new(43).unwrap();
        }
        ResourceBudgetKind::SpillBytes => {
            constrained.max_candidate_spill_bytes = NonZeroU64::new(109).unwrap();
        }
        ResourceBudgetKind::SpillRuns | ResourceBudgetKind::MetadataEntries => unreachable!(),
    }
    let reader = SearchOutOfCoreReader::open_with_config(&path, constrained).unwrap();
    assert_eq!(reader.segments.len(), 1);
    assert_eq!(reader.segments[0].descriptor.segments.len(), 3);
    let segment_id = reader.segments[0].descriptor.segments[0].segment_id;
    let expected_error = match resource {
        ResourceBudgetKind::MemoryBytes => ResourceBudgetError {
            resource,
            scope: ResourceBudgetScope::Operator,
            owner: "search candidate block".to_string(),
            memory_class: None,
            requested: 44,
            used: 0,
            budget: 43,
            budget_option: "SearchOutOfCoreConfig.max_candidate_block_bytes",
            message: format!(
                "search candidate block for layer 0 segment {segment_id} requires 44 bytes, exceeding 43"
            ),
        },
        ResourceBudgetKind::SpillBytes => ResourceBudgetError {
            resource,
            scope: ResourceBudgetScope::Operator,
            owner: "search candidate spill".to_string(),
            memory_class: None,
            requested: 22,
            used: 88,
            budget: 109,
            budget_option: "SearchOutOfCoreConfig.max_candidate_spill_bytes",
            message: "search candidate spill requires 110 bytes, exceeding 109".to_string(),
        },
        ResourceBudgetKind::SpillRuns | ResourceBudgetKind::MetadataEntries => unreachable!(),
    };
    for _ in 0..2 {
        let error = reader
            .search_with_options("graph", None, SearchMode::Text, filtered.clone())
            .unwrap_err();
        assert_eq!(error, HawDBError::from(expected_error.clone()));
        assert_clean();
    }

    // A refusal does not invalidate the reader: an unfiltered query needs no spill.
    let output = reader
        .search_with_options("graph", None, SearchMode::Text, options(10, None))
        .unwrap();
    assert_search_parity(&unfiltered, &output.result);
    assert_eq!(output.metrics.candidate_spill_bytes, 0);
    assert_clean();
    drop(reader);

    let reader = SearchOutOfCoreReader::open_with_config(&path, exact).unwrap();
    let output = reader
        .search_with_options("graph", None, SearchMode::Text, filtered)
        .unwrap();
    assert_search_parity(&expected, &output.result);
    assert_eq!(
        output.result.candidate_set.representation,
        "spilled_sorted_document_ids"
    );
    assert_eq!(output.metrics.candidate_spill_bytes, 110);
    assert_clean();
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}
