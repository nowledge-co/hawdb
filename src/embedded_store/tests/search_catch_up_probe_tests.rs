// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The read-only change batch probe and the typed bounded catch-up refusals
//! through the embedded handle (#1924).

use super::*;
use crate::{
    DatabaseReadTransaction, IncrementalSearchMaintenanceOptions, SearchProjectionChangeBatchProbe,
    SearchProjectionRelationalDelta,
};

fn resident_handle(name: &str) -> (std::path::PathBuf, EmbeddedStoreHandle) {
    resident_handle_over(name, Database::new())
}

fn resident_handle_over(name: &str, db: Database) -> (std::path::PathBuf, EmbeddedStoreHandle) {
    let root = unique_embedded_store_test_dir(name);
    let graph = EmbeddedGraph::from_database(db, GraphMode::WritableCutover);
    let projection = SearchProjection::from_index(SearchIndex::open(root.join("search")).unwrap());
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, Some(projection))).unwrap();
    (root, handle)
}

fn create(handle: &EmbeddedStoreHandle, ids: &[&str]) {
    for id in ids {
        handle
            .query_with_report(&format!("CREATE (:Memory {{id: '{id}', title: '{id}'}})"))
            .unwrap();
    }
}

fn graph_epoch(handle: &EmbeddedStoreHandle) -> u64 {
    handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap()
}

fn durable_epoch(handle: &EmbeddedStoreHandle) -> Option<u64> {
    handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap()
        .durable_source_graph_commit_epoch
}

fn hydrate_nothing(
    _: &mut DatabaseReadTransaction,
    _: &crate::SearchProjectionChangeBatch,
) -> crate::Result<SearchProjectionRelationalDelta> {
    Ok(SearchProjectionRelationalDelta::default())
}

#[test]
fn probe_reports_no_backlog_a_covering_batch_and_a_prefix() {
    let (root, handle) = resident_handle("catch_up_probe_1924");
    assert_eq!(
        handle.search_projection_change_batch_probe(8).unwrap(),
        None
    );
    assert!(matches!(
        handle.search_projection_change_batch_probe(0),
        Err(HawDBError::Semantic(_))
    ));

    create(&handle, &["m1", "m2", "m3"]);
    let head = graph_epoch(&handle);
    let prefix = handle.search_projection_change_batch_probe(2).unwrap();
    assert_eq!(
        prefix,
        Some(SearchProjectionChangeBatchProbe {
            start_commit_epoch: 0,
            operation_count: 2,
            complete_through_commit_epoch: Some(head - 1),
            source_commit_epoch: head,
            covers_source: false,
        })
    );
    // The probe neither hydrates nor publishes.
    assert_eq!(durable_epoch(&handle), None);

    // A prefix probe predicts the typed catch-up refusal for that budget.
    for max_batches in [1, 4] {
        assert_eq!(
            handle
                .catch_up_search_projection(2, max_batches)
                .unwrap_err(),
            HawDBError::SearchProjectionPrefixBehindSource {
                complete_through_commit_epoch: head - 1,
                source_commit_epoch: head,
            }
        );
    }
    assert_eq!(durable_epoch(&handle), None);
    assert!(!handle.storage_handle_poisoned().unwrap());

    let covering = handle
        .search_projection_change_batch_probe(64)
        .unwrap()
        .unwrap();
    assert!(covering.covers_source);
    assert_eq!(covering.operation_count, 3);
    assert_eq!(covering.complete_through_commit_epoch, Some(head));
    assert!(
        handle
            .catch_up_search_projection(covering.operation_count, 1)
            .unwrap()
            .complete
    );
    assert_eq!(durable_epoch(&handle), Some(head));
    assert_eq!(
        handle.search_projection_change_batch_probe(8).unwrap(),
        None
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn probe_refuses_a_single_commit_over_its_cap_with_the_typed_budget_error() {
    let mut db = Database::new();
    let mut transaction = db.begin_transaction().unwrap();
    for id in ["m1", "m2"] {
        transaction
            .query(&format!("CREATE (:Memory {{id: '{id}', title: '{id}'}})"))
            .unwrap();
    }
    transaction.commit().unwrap();
    let (root, handle) = resident_handle_over("catch_up_probe_commit_budget_1924", db);
    let commit = graph_epoch(&handle);
    let expected = HawDBError::SearchProjectionBatchBudgetExceeded {
        budget: crate::SearchProjectionBatchBudget::ChangeBatch,
        commit_epoch: Some(commit),
        required_operations: 2,
        limit: 1,
    };
    assert_eq!(
        handle.search_projection_change_batch_probe(1).unwrap_err(),
        expected
    );
    assert_eq!(
        handle
            .catch_up_search_projection_with_batch_hydrator(1, 8, 1, hydrate_nothing)
            .unwrap_err(),
        expected
    );
    assert!(!handle.storage_handle_poisoned().unwrap());
    assert_eq!(durable_epoch(&handle), None);
    let probe = handle
        .search_projection_change_batch_probe(2)
        .unwrap()
        .unwrap();
    assert!(probe.covers_source);
    assert!(
        handle
            .catch_up_search_projection_with_batch_hydrator(2, 8, 1, hydrate_nothing)
            .unwrap()
            .complete
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn handle_entry_points_refuse_a_prefix_typed_and_a_fresh_probe_recovers_after_a_write() {
    let (root, handle) = resident_handle("catch_up_handle_prefix_1924");
    create(&handle, &["m1", "m2"]);
    let probe = handle
        .search_projection_change_batch_probe(64)
        .unwrap()
        .unwrap();
    assert!(probe.covers_source);
    // A write lands between the probe and the catch-up: the probed budget is
    // now a prefix.
    create(&handle, &["m3"]);
    let head = graph_epoch(&handle);
    let expected = HawDBError::SearchProjectionPrefixBehindSource {
        complete_through_commit_epoch: head - 1,
        source_commit_epoch: head,
    };
    let cap = probe.operation_count;
    assert_eq!(
        handle.catch_up_search_projection(cap, 4).unwrap_err(),
        expected
    );
    assert_eq!(
        handle
            .catch_up_search_projection_with_relational(cap, 4, hydrate_nothing)
            .unwrap_err(),
        expected
    );
    assert_eq!(
        handle
            .catch_up_search_projection_with_batch_hydrator(cap, cap, 4, hydrate_nothing)
            .unwrap_err(),
        expected
    );
    assert_eq!(
        handle
            .catch_up_search_projection_scheduled(cap, 4)
            .unwrap_err(),
        expected
    );
    assert_eq!(durable_epoch(&handle), None);
    assert!(!handle.storage_handle_poisoned().unwrap());

    let fresh = handle
        .search_projection_change_batch_probe(64)
        .unwrap()
        .unwrap();
    assert!(fresh.covers_source);
    assert!(
        handle
            .catch_up_search_projection_with_batch_hydrator(
                fresh.operation_count,
                fresh.operation_count,
                1,
                hydrate_nothing,
            )
            .unwrap()
            .complete
    );
    assert_eq!(durable_epoch(&handle), Some(head));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_probe_and_catch_up_refusal_use_the_durable_watermark() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "catch_up_probe_incremental_1924",
        IncrementalSearchMaintenanceOptions::default(),
    );
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 4, hydrate_nothing)
        .unwrap();
    let base = durable_epoch(&handle).unwrap();
    assert_eq!(
        handle.search_projection_change_batch_probe(8).unwrap(),
        None
    );
    create(&handle, &["m1", "m2", "m3"]);
    let head = graph_epoch(&handle);
    let prefix = handle
        .search_projection_change_batch_probe(1)
        .unwrap()
        .unwrap();
    assert_eq!(prefix.start_commit_epoch, base);
    assert!(!prefix.covers_source);
    assert_eq!(
        handle
            .catch_up_search_projection_with_batch_hydrator(1, 64, 4, hydrate_nothing)
            .unwrap_err(),
        HawDBError::SearchProjectionPrefixBehindSource {
            complete_through_commit_epoch: prefix.complete_through_commit_epoch.unwrap(),
            source_commit_epoch: head,
        }
    );
    assert_eq!(durable_epoch(&handle), Some(base));
    assert!(!handle.storage_handle_poisoned().unwrap());
    let covering = handle
        .search_projection_change_batch_probe(64)
        .unwrap()
        .unwrap();
    assert!(covering.covers_source);
    handle
        .catch_up_search_projection_with_batch_hydrator(
            covering.operation_count,
            64,
            1,
            hydrate_nothing,
        )
        .unwrap();
    assert_eq!(durable_epoch(&handle), Some(head));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_host_derived_rows_over_the_projection_budget_are_typed() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "catch_up_projection_budget_incremental_1924",
        IncrementalSearchMaintenanceOptions::default(),
    );
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 4, hydrate_nothing)
        .unwrap();
    let base = durable_epoch(&handle);
    create(&handle, &["m1"]);
    let derive_one_row = |_: &mut DatabaseReadTransaction,
                          _: &crate::SearchProjectionChangeBatch|
     -> crate::Result<SearchProjectionRelationalDelta> {
        Ok(SearchProjectionRelationalDelta {
            delta: crate::search::SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "derived".into(),
                    title: "Derived".into(),
                    body: "Host row".into(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                ..Default::default()
            },
            processed_primary_key_count: 0,
        })
    };
    let error = handle
        .catch_up_search_projection_with_batch_hydrator(1, 1, 4, derive_one_row)
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
    assert_eq!(durable_epoch(&handle), base);
    assert!(!handle.storage_handle_poisoned().unwrap());

    handle
        .catch_up_search_projection_with_batch_hydrator(1, 2, 1, derive_one_row)
        .unwrap();
    assert_eq!(durable_epoch(&handle), Some(graph_epoch(&handle)));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

fn assert_not_attached(error: HawDBError) {
    assert!(
        matches!(&error, HawDBError::Semantic(message) if message.starts_with("search projection is not attached")),
        "{error:?}"
    );
}

#[test]
fn probe_and_readiness_without_a_projection_are_semantic_not_storage() {
    let graph = EmbeddedGraph::from_database(Database::new(), GraphMode::WritableCutover);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'm1', title: 'm1'})")
        .unwrap();
    assert_not_attached(handle.search_projection_change_batch_probe(8).unwrap_err());
    assert_not_attached(
        handle
            .search_projection_changefeed_readiness(false, Some(8))
            .unwrap_err(),
    );
    assert!(!handle.storage_handle_poisoned().unwrap());
}

#[test]
fn probe_and_readiness_after_a_failed_incremental_reattach_are_semantic() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "catch_up_probe_detached_1924",
        IncrementalSearchMaintenanceOptions::default(),
    );
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 4, hydrate_nothing)
        .unwrap();
    create(&handle, &["m1"]);
    // Remove the durable selector so the next refresh cannot attach the head;
    // the store detaches its incremental reader rather than serve a stale one.
    let selector = root
        .join("search")
        .join("search_projection.out_of_core.manifest.hawdb");
    std::fs::remove_file(&selector).unwrap();
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 4, hydrate_nothing)
        .unwrap_err();
    assert_not_attached(handle.search_projection_change_batch_probe(8).unwrap_err());
    assert_not_attached(
        handle
            .search_projection_changefeed_readiness(false, Some(8))
            .unwrap_err(),
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// No public constructor attaches both projections, and qualified read-only
/// out-of-core search is opened elsewhere; set the private fields directly to
/// cover the helper's remaining states.
#[test]
fn probe_and_readiness_report_ambiguous_and_unmaintained_projections_as_semantic() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "catch_up_probe_states_1924",
        IncrementalSearchMaintenanceOptions::default(),
    );
    let semantic = |error: HawDBError, prefix: &str| {
        assert!(
            matches!(&error, HawDBError::Semantic(message) if message.starts_with(prefix)),
            "{error:?}"
        );
    };
    handle.write_store().unwrap().search_projection =
        Some(SearchProjection::from_index(SearchIndex::in_memory()));
    for error in [
        handle.search_projection_change_batch_probe(8).unwrap_err(),
        handle
            .search_projection_changefeed_readiness(false, Some(8))
            .unwrap_err(),
    ] {
        semantic(error, "search projection ownership is ambiguous");
    }
    {
        let mut store = handle.write_store().unwrap();
        store.search_projection = None;
        store
            .out_of_core_search_projection
            .as_mut()
            .unwrap()
            .maintenance = None;
    }
    for error in [
        handle.search_projection_change_batch_probe(8).unwrap_err(),
        handle
            .search_projection_changefeed_readiness(false, Some(8))
            .unwrap_err(),
    ] {
        semantic(error, "search projection has no catch-up maintenance");
    }
    assert!(!handle.storage_handle_poisoned().unwrap());
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
