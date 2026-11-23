// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Typed bounded catch-up refusals (#1924): a prefix behind the source head
//! and a batch over its per-batch budget are neither untyped `Execution`
//! nor `Storage`, publish nothing, and leave the handle usable.

use super::*;
use crate::{SearchProjectionChangeBatch, SearchProjectionRelationalDelta};

#[derive(Debug, Clone, Copy)]
enum EntryPoint {
    Graph,
    Relational,
    BatchHydrator,
    Scheduled,
}

const ENTRY_POINTS: [EntryPoint; 4] = [
    EntryPoint::Graph,
    EntryPoint::Relational,
    EntryPoint::BatchHydrator,
    EntryPoint::Scheduled,
];

fn no_relational(
    _: &mut DatabaseReadTransaction,
    _: &SearchProjectionChangeBatch,
) -> Result<SearchProjectionRelationalDelta> {
    Ok(SearchProjectionRelationalDelta::default())
}

/// Runs `entry` with one change budget and its own projection budget.
fn catch_up(
    db: &Database,
    index: &mut SearchIndex,
    entry: EntryPoint,
    max_changes: usize,
    max_projection: usize,
    max_batches: usize,
) -> Result<bool> {
    match entry {
        EntryPoint::Graph => db
            .catch_up_search_projection(index, max_changes, max_batches)
            .map(|report| report.complete),
        EntryPoint::Relational => db
            .catch_up_search_projection_with_relational(
                index,
                max_changes,
                max_batches,
                no_relational,
            )
            .map(|report| report.complete),
        EntryPoint::BatchHydrator => db
            .catch_up_search_projection_with_batch_hydrator(
                index,
                max_changes,
                max_projection,
                max_batches,
                no_relational,
            )
            .map(|report| report.complete),
        EntryPoint::Scheduled => db
            .catch_up_search_projection_scheduled(index, max_changes, max_batches)
            .map(|report| report.catch_up.complete),
    }
}

/// A persistent projection caught up at `base`, then `commits` single-node
/// commits pending.
fn pending_commits(name: &str, commits: usize) -> (Database, SearchIndex, std::path::PathBuf, u64) {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm0', title: 'Base'})")
        .unwrap();
    let path = unique_test_dir(name);
    let mut index = SearchIndex::open(&path).unwrap();
    db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    index.checkpoint().unwrap();
    let base = db.commit_epoch().unwrap();
    for commit in 1..=commits {
        db.query(&format!(
            "CREATE (:Memory {{id: 'm{commit}', title: 'Pending {commit}'}})"
        ))
        .unwrap();
    }
    (db, index, path, base)
}

fn durable_epoch(index: &SearchIndex) -> Option<u64> {
    index
        .projection_freshness()
        .durable_source_graph_commit_epoch
}

#[test]
fn bounded_catch_up_refuses_a_prefix_with_a_typed_non_poisoning_error() {
    for entry in ENTRY_POINTS {
        let (db, mut index, path, base) = pending_commits("catch_up_prefix_1924", 3);
        let head = db.commit_epoch().unwrap();
        assert_eq!(head, base + 3);
        // No concurrent writes: retrying the same budgets, or more batches,
        // refuses again because the interval exceeds one batch.
        for max_batches in [1, 4, 64] {
            let error = catch_up(&db, &mut index, entry, 2, 64, max_batches).unwrap_err();
            assert_eq!(
                error,
                HawDBError::SearchProjectionPrefixBehindSource {
                    complete_through_commit_epoch: base + 2,
                    source_commit_epoch: head,
                },
                "{entry:?}"
            );
            assert_eq!(
                error.to_string(),
                format!(
                    "execution error: search projection complete-through epoch {} differs from source snapshot epoch {head}; prepare the complete source interval before publishing its watermark",
                    base + 2
                )
            );
            assert_eq!(durable_epoch(&index), Some(base), "{entry:?}");
            assert!(index.document("memory:m1").is_none(), "{entry:?}");
        }
        assert!(!db.storage_handle_poisoned().unwrap(), "{entry:?}");

        // The probe names the budget that covers the whole interval.
        let probe = db
            .build_search_projection_change_batch_from_freshness(&index, Some(64))
            .unwrap()
            .unwrap();
        assert_eq!(probe.complete_through_commit_epoch(), Some(head));
        let cap = probe.operation_count();
        assert!(
            catch_up(&db, &mut index, entry, cap, 64, 1).unwrap(),
            "{entry:?}"
        );
        assert_eq!(durable_epoch(&index), Some(head), "{entry:?}");
        assert!(index.document("memory:m3").is_some(), "{entry:?}");
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn single_commit_over_the_change_budget_is_a_typed_budget_refusal() {
    for entry in ENTRY_POINTS {
        let mut db = Database::new();
        let path = unique_test_dir("catch_up_commit_budget_1924");
        let mut index = SearchIndex::open(&path).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        for id in ["m1", "m2", "m3"] {
            transaction
                .query(&format!(
                    "CREATE (:Memory {{id: '{id}', title: 'Same commit'}})"
                ))
                .unwrap();
        }
        transaction.commit().unwrap();
        let commit = db.commit_epoch().unwrap();

        let error = catch_up(&db, &mut index, entry, 2, 64, 4).unwrap_err();
        assert_eq!(
            error,
            HawDBError::SearchProjectionBatchBudgetExceeded {
                budget: crate::SearchProjectionBatchBudget::ChangeBatch,
                commit_epoch: Some(commit),
                required_operations: 3,
                limit: 2,
            },
            "{entry:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "storage error: search projection change at commit epoch {commit} requires 3 operations, exceeding configured per-batch limit 2"
            )
        );
        assert_eq!(durable_epoch(&index), None, "{entry:?}");
        assert_eq!(index.document_count(), 0, "{entry:?}");
        assert!(!db.storage_handle_poisoned().unwrap(), "{entry:?}");

        assert!(
            catch_up(&db, &mut index, entry, 3, 64, 1).unwrap(),
            "{entry:?}"
        );
        assert_eq!(durable_epoch(&index), Some(commit), "{entry:?}");
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn graph_delta_over_its_operation_limit_is_a_typed_budget_refusal() {
    let mut db = Database::new();
    let mut transaction = db.begin_transaction().unwrap();
    transaction
        .query("CREATE (:Memory {id: 'm1', title: 'First'})")
        .unwrap();
    transaction
        .query("CREATE (:Memory {id: 'm2', title: 'Second'})")
        .unwrap();
    transaction.commit().unwrap();
    let commit = db.commit_epoch().unwrap();
    let expected = HawDBError::SearchProjectionBatchBudgetExceeded {
        budget: crate::SearchProjectionBatchBudget::GraphDelta,
        commit_epoch: None,
        required_operations: 2,
        limit: 1,
    };
    let text = "storage error: search projection graph delta operation count 2 exceeded configured limit 1";

    let mut request = db
        .build_search_projection_graph_delta_request_after(0, Some(2))
        .unwrap()
        .unwrap();
    request.max_operations = Some(1);
    let error = db
        .build_search_projection_graph_delta(&request)
        .unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(error.to_string(), text);
    assert!(!db.storage_handle_poisoned().unwrap());

    // The projection budget of the batch hydrator path refuses the graph
    // delta after selection; nothing is published.
    let path = unique_test_dir("catch_up_graph_delta_budget_1924");
    let mut index = SearchIndex::open(&path).unwrap();
    let error = catch_up(&db, &mut index, EntryPoint::BatchHydrator, 2, 1, 1).unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(durable_epoch(&index), None);
    assert_eq!(index.document_count(), 0);
    assert!(!db.storage_handle_poisoned().unwrap());

    assert!(catch_up(&db, &mut index, EntryPoint::BatchHydrator, 2, 2, 1).unwrap());
    assert_eq!(durable_epoch(&index), Some(commit));
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

/// One host-derived row on top of the batch's own graph operation.
fn derive_one_row(
    _: &mut DatabaseReadTransaction,
    _: &SearchProjectionChangeBatch,
) -> Result<SearchProjectionRelationalDelta> {
    Ok(SearchProjectionRelationalDelta {
        delta: SearchProjectionDelta {
            upserts: vec![search_projection_row("derived", "Derived", "Host row")],
            ..SearchProjectionDelta::default()
        },
        processed_primary_key_count: 0,
    })
}

#[test]
fn host_derived_rows_over_the_projection_budget_are_a_typed_budget_refusal() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'm1', title: 'Source'})")
        .unwrap();
    let path = unique_test_dir("catch_up_projection_budget_1924");
    let mut index = SearchIndex::open(&path).unwrap();

    let error = db
        .catch_up_search_projection_with_batch_hydrator(&mut index, 1, 1, 4, derive_one_row)
        .unwrap_err();
    assert_eq!(
        error,
        HawDBError::SearchProjectionBatchBudgetExceeded {
            budget: crate::SearchProjectionBatchBudget::ProjectionDelta,
            commit_epoch: None,
            required_operations: 2,
            limit: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "storage error: incremental projection update operation count 2 exceeded configured limit 1"
    );
    assert_eq!(durable_epoch(&index), None);
    assert_eq!(index.document_count(), 0);
    assert!(!db.storage_handle_poisoned().unwrap());

    assert!(
        db.catch_up_search_projection_with_batch_hydrator(&mut index, 1, 2, 1, derive_one_row)
            .unwrap()
            .complete
    );
    assert_eq!(durable_epoch(&index), Some(db.commit_epoch().unwrap()));
    assert!(index.document("memory:derived").is_some());
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_epoch_ahead_of_the_graph_stays_a_storage_error() {
    let db = Database::new();
    let request = SearchProjectionGraphDeltaRequest {
        upsert_node_ids: Vec::new(),
        delete_document_ids: Vec::new(),
        max_operations: None,
        complete_through_graph_commit_epoch: Some(db.commit_epoch().unwrap() + 1),
    };
    assert!(matches!(
        db.build_search_projection_graph_delta(&request),
        Err(HawDBError::Storage(message)) if message.contains("is ahead of graph commit epoch")
    ));
}
