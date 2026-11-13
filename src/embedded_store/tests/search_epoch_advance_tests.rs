// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Verified metadata-only search projection epoch advance (#1756).
//!
//! Each path advances across graph commits that only touch non-projection
//! labels (import bookkeeping) and an additive schema statement, writes only
//! the selector, refuses a range with one Memory write or a change-log gap
//! with the typed refusal, reopens at the advanced epoch, and still agrees
//! with a full rebuild from the graph after incremental catch-up.

use super::*;
use crate::search::{
    SearchDocumentSetDigest, SearchProjectionEpochAdvanceReport, SearchProjectionScanBudget,
    SearchRebuildOptions,
};
use crate::{
    DatabaseConfig, GraphOnlySearchProjectionRebuild, IncrementalSearchMaintenanceOptions,
    RuntimeTaskContext, SearchProjectionCatchUpReport, SearchProjectionEpochAdvanceRefusal,
    SearchProjectionRebuildOptions,
};
use std::path::Path;

const SELECTOR: &str = "search_projection.out_of_core.manifest.hawdb";
const DECLARE: &str = "CREATE PROPERTY ON NODE TABLE Entity(pagerank_score) TYPE ANY DEFAULT 0.0";

fn graph_epoch(handle: &EmbeddedStoreHandle) -> u64 {
    handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap()
}

/// (source, durable) epochs the runtime reports for the projection.
fn projection_epochs(handle: &EmbeddedStoreHandle) -> (Option<u64>, Option<u64>) {
    let freshness = handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    (
        freshness.source_graph_commit_epoch,
        freshness.durable_source_graph_commit_epoch,
    )
}

/// Graph commits a host makes while importing that the projection does not
/// index: bookkeeping on a non-projection label and an additive schema
/// statement (an empty capture since #1749).
fn import_state_commits(handle: &EmbeddedStoreHandle, page: u32) {
    handle
        .query_with_report(&format!(
            "MERGE (s:ImportState {{id: 'cursor'}}) SET s.page = {page}"
        ))
        .unwrap();
    handle
        .query_with_report(&format!(
            "CREATE (:ImportCheckpoint {{id: 'page-{page}', rows: {page}}})"
        ))
        .unwrap();
}

type Files = BTreeMap<String, (u64, u64)>;

/// Every file with its length and (on Unix) inode.
fn files(root: &Path) -> Files {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            #[cfg(unix)]
            let inode = std::os::unix::fs::MetadataExt::ino(&metadata);
            #[cfg(not(unix))]
            let inode = 0;
            (
                entry.file_name().to_string_lossy().into_owned(),
                (metadata.len(), inode),
            )
        })
        .collect()
}

/// The advance wrote the selector and nothing else: the generation file set
/// is unchanged, every content file keeps its inode and length, and the
/// reported bytes are the selector's.
fn assert_metadata_only(
    before: &Files,
    after: &Files,
    report: &SearchProjectionEpochAdvanceReport,
) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "the advance created or removed generation files"
    );
    let mut content_bytes = 0u64;
    for (name, entry) in before {
        if name != SELECTOR {
            assert_eq!(&after[name], entry, "{name} was rewritten");
            content_bytes += entry.0;
        }
    }
    assert!(report.advanced);
    assert_eq!(report.metadata_bytes_written, after[SELECTOR].0);
    assert!(
        report.metadata_bytes_written < 16 * 1024,
        "selector is {} bytes",
        report.metadata_bytes_written
    );
    assert!(
        content_bytes > 50 * report.metadata_bytes_written,
        "content {content_bytes} bytes, selector {}",
        report.metadata_bytes_written
    );
}

fn refusal(error: &HawDBError) -> SearchProjectionEpochAdvanceRefusal {
    assert!(
        !matches!(error, HawDBError::Storage(_)),
        "a refusal must not be a storage failure: {error:?}"
    );
    match error {
        HawDBError::SearchProjectionEpochAdvanceRefused { reason, .. } => *reason,
        other => panic!("expected a typed epoch advance refusal, got {other:?}"),
    }
}

/// The full closure verification of a cleanup pass accepts the advanced
/// selector and keeps every file it selects.
fn assert_cleanup_verifies(handle: &EmbeddedStoreHandle, search: &Path) {
    let report = handle
        .retry_search_projection_cleanup(crate::SearchProjectionCleanupOptions::default())
        .unwrap();
    assert_eq!(report.generation_discovery_failures, 0, "{report:?}");
    assert_eq!(report.scan_failures, 0, "{report:?}");
    assert_eq!(report.delete_failures, 0, "{report:?}");
    assert!(!report.retry_required, "{report:?}");
    assert!(report.verification_rehashed_bytes + report.verification_reused_bytes > 0);
    assert!(search.join(SELECTOR).is_file());
}

fn catch_up(handle: &EmbeddedStoreHandle) -> crate::Result<SearchProjectionCatchUpReport> {
    handle.catch_up_search_projection_with_batch_hydrator(64, 64, 64, |_, _| {
        Ok(SearchProjectionRelationalDelta::default())
    })
}

fn digest(path: &Path) -> (Option<u64>, SearchDocumentSetDigest) {
    let reader = crate::SearchOutOfCoreReader::open(path).unwrap();
    let mut digest = SearchDocumentSetDigest::default();
    reader
        .visit_projection_documents(
            SearchProjectionScanBudget {
                max_rows: 1 << 20,
                max_payload_bytes: 1 << 30,
                max_encoded_record_bytes: 1 << 24,
            },
            &RuntimeTaskContext::default(),
            &mut |document| {
                digest.add(document);
                Ok(())
            },
        )
        .unwrap();
    (reader.source_graph_commit_epoch(), digest)
}

/// Rebuilds the projection from the graph into `target` and asserts that it
/// agrees, epoch and documents, with the maintained projection at `current`.
fn assert_agrees_with_rebuild(handle: &EmbeddedStoreHandle, current: &Path, target: &Path) {
    let report = handle
        .rebuild_out_of_core_search_projection(
            target,
            SearchProjectionRebuildOptions::default(),
            &mut GraphOnlySearchProjectionRebuild,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    let (epoch, maintained) = digest(current);
    let (rebuilt_epoch, rebuilt) = digest(target);
    assert_eq!(epoch, Some(report.source_graph_commit_epoch));
    assert_eq!(rebuilt_epoch, epoch);
    assert_eq!(maintained, rebuilt);
    assert!(maintained.document_count() >= 3);
}

/// A corpus large enough that rewriting it would dwarf the selector.
fn create_corpus(handle: &EmbeddedStoreHandle) {
    let body = "imported memory text that a full checkpoint would rewrite ".repeat(8);
    handle
        .with_transaction(|transaction| {
            for number in 0..300 {
                transaction.query(&format!(
                    "CREATE (:Memory {{id: 'bulk{number:03}', title: 'bulk {number}', content: '{body}{number}', space_id: 'default'}})"
                ))?;
            }
            Ok(())
        })
        .unwrap();
}

fn create_memories(handle: &EmbeddedStoreHandle, ids: &[&str]) {
    for id in ids {
        handle
            .query_with_report(&format!(
                "CREATE (:Memory {{id: '{id}', title: 'memory {id}', content: 'advance body {id}', space_id: 'default'}})"
            ))
            .unwrap();
    }
}

/// The initial-import path: a resident projection checkpointed through
/// `apply_initial_import_projection_delta_and_checkpoint`.
#[test]
fn resident_advance_is_metadata_only_verified_and_durable() {
    let root = unique_embedded_store_test_dir("epoch_advance_resident");
    let search = root.join("search");
    let resident = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        search.clone(),
        GraphMode::WritableCutover,
    );
    let (handle, _) = EmbeddedStoreHandle::open_with_options(resident.clone()).unwrap();
    create_corpus(&handle);
    create_memories(&handle, &["m1", "m2", "m3"]);
    {
        let mut store = handle.write_store().unwrap();
        let store = &mut *store;
        store
            .graph
            .database()
            .rebuild_search_projection(
                store.search_projection.as_mut().unwrap().index_mut(),
                SearchRebuildOptions::default(),
            )
            .unwrap();
    }
    let empty = SearchProjectionDelta {
        upserts: Vec::new(),
        deletes: Vec::new(),
        max_operations: None,
        source_graph_commit_epoch: None,
    };
    handle
        .apply_initial_import_projection_delta_and_checkpoint(empty, 41)
        .unwrap();
    let imported = graph_epoch(&handle);
    assert_eq!(projection_epochs(&handle), (Some(imported), Some(imported)));

    import_state_commits(&handle, 1);
    handle.query_with_report(DECLARE).unwrap();
    import_state_commits(&handle, 2);
    let target = graph_epoch(&handle);
    assert!(target >= imported + 5);
    let before = files(&search);
    let report = handle
        .advance_search_projection_source_epoch(target)
        .unwrap();
    assert_metadata_only(&before, &files(&search), &report);
    assert_eq!(report.source_graph_commit_epoch_before, imported);
    assert_eq!(report.source_graph_commit_epoch_after, target);
    assert_eq!(report.content_source_graph_commit_epoch, imported);
    assert_eq!(report.checked_change_log_entries, 4);
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    assert!(handle
        .initial_import_projection_pending()
        .unwrap()
        .is_some_and(|pending| pending.uncheckpointed_deltas == 0));

    // One Memory write in the range is refused, and nothing is published.
    create_memories(&handle, &["m4"]);
    let written = graph_epoch(&handle);
    let unchanged = files(&search);
    let error = handle
        .advance_search_projection_source_epoch(written)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );
    assert!(
        error
            .to_string()
            .contains(&format!("graph commit {written}")),
        "{error}"
    );
    let error = handle
        .advance_search_projection_source_epoch(written + 10)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::RangeUnavailable
    );
    assert_eq!(files(&search), unchanged);
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    drop(handle);

    // The resident reopen keeps the advanced epoch and its documents.
    let (handle, _) = EmbeddedStoreHandle::open_with_options(resident.clone()).unwrap();
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    assert_eq!(
        handle
            .runtime_status()
            .unwrap()
            .projection_freshness
            .unwrap()
            .document_count,
        303
    );
    assert_cleanup_verifies(&handle, &search);
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    drop(handle);

    // The host's runtime reopens the same directory for incremental
    // maintenance; catch-up replays the Memory write after the advanced epoch
    // and the result agrees with a full rebuild from the graph.
    let incremental = resident
        .clone()
        .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default());
    let (handle, _) = EmbeddedStoreHandle::open_with_options(incremental).unwrap();
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    catch_up(&handle).unwrap();
    assert_eq!(projection_epochs(&handle).1, Some(written));
    assert_agrees_with_rebuild(&handle, &search, &root.join("rebuilt"));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// The runtime incremental out-of-core path.
#[test]
fn incremental_advance_is_metadata_only_verified_and_durable() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "epoch_advance_incremental",
        IncrementalSearchMaintenanceOptions::default(),
    );
    create_corpus(&handle);
    create_memories(&handle, &["m1", "m2", "m3"]);
    let search = root.join("projection");
    handle
        .rebuild_out_of_core_search_projection(
            &search,
            SearchProjectionRebuildOptions::default(),
            &mut GraphOnlySearchProjectionRebuild,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    drop(handle);
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        search.clone(),
        GraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default());
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
    let built = graph_epoch(&handle);
    assert_eq!(projection_epochs(&handle), (Some(built), Some(built)));

    import_state_commits(&handle, 1);
    handle.query_with_report(DECLARE).unwrap();
    let target = graph_epoch(&handle);
    let before = files(&search);
    let report = handle
        .advance_search_projection_source_epoch(target)
        .unwrap();
    assert_metadata_only(&before, &files(&search), &report);
    assert_eq!(report.source_graph_commit_epoch_before, built);
    assert_eq!(report.checked_change_log_entries, 2);
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    let noop = handle
        .advance_search_projection_source_epoch(target)
        .unwrap();
    assert!(!noop.advanced);
    assert_eq!(noop.metadata_bytes_written, 0);
    drop(handle);

    // Reopen keeps the advanced epoch; catch-up has nothing to replay.
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    assert_eq!(catch_up(&handle).unwrap().applied_batch_count, 0);
    assert_cleanup_verifies(&handle, &search);

    // One Memory write in the range is refused with the typed reason.
    import_state_commits(&handle, 2);
    create_memories(&handle, &["m4"]);
    let written = graph_epoch(&handle);
    let unchanged = files(&search);
    let error = handle
        .advance_search_projection_source_epoch(written)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );
    assert_eq!(files(&search), unchanged);
    assert_eq!(projection_epochs(&handle), (Some(target), Some(target)));
    // A Memory deletion is a projection change too.
    catch_up(&handle).unwrap();
    handle
        .query_with_report("MATCH (m:Memory {id: 'm1'}) DETACH DELETE m")
        .unwrap();
    let error = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );

    // The host falls back to catch-up, and later bookkeeping advances again
    // from the new content epoch.
    catch_up(&handle).unwrap();
    let caught_up = graph_epoch(&handle);
    import_state_commits(&handle, 3);
    let report = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap();
    assert_eq!(report.content_source_graph_commit_epoch, caught_up);
    assert_agrees_with_rebuild(&handle, &search, &root.join("rebuilt"));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// A range the change log no longer covers is refused, never assumed empty.
#[test]
fn advance_across_a_change_log_gap_is_refused() {
    let root = unique_embedded_store_test_dir("epoch_advance_gap");
    let search = root.join("search");
    let mut index = SearchIndex::open(&search).unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "m1".into(),
            title: "memory m1".into(),
            body: "bootstrap".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        search.clone(),
        GraphMode::WritableCutover,
    )
    .with_database_config(DatabaseConfig {
        max_search_projection_change_log_entries: Some(2),
        ..DatabaseConfig::default()
    })
    .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default());
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    // No source epoch yet: the range has no start.
    let error = handle
        .advance_search_projection_source_epoch(0)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::RangeUnavailable
    );
    create_memories(&handle, &["m1"]);
    catch_up(&handle).unwrap();
    let source = graph_epoch(&handle);
    assert_eq!(projection_epochs(&handle).1, Some(source));
    // Three bookkeeping captures overflow the two retained entries, so the
    // log no longer starts at the projection's epoch, although none of the
    // trimmed commits touched the projection.
    import_state_commits(&handle, 1);
    import_state_commits(&handle, 2);
    let target = graph_epoch(&handle);
    let unchanged = files(&search);
    let error = handle
        .advance_search_projection_source_epoch(target)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ChangeLogGap
    );
    match &error {
        HawDBError::SearchProjectionEpochAdvanceRefused {
            source_graph_commit_epoch,
            target_graph_commit_epoch,
            ..
        } => {
            assert_eq!(*source_graph_commit_epoch, Some(source));
            assert_eq!(*target_graph_commit_epoch, target);
        }
        _ => unreachable!(),
    }
    assert_eq!(files(&search), unchanged);
    assert_eq!(projection_epochs(&handle), (Some(source), Some(source)));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
