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
    for target in [written - 1, written + 10] {
        let error = handle
            .advance_search_projection_source_epoch(target)
            .unwrap_err();
        assert_eq!(
            refusal(&error),
            SearchProjectionEpochAdvanceRefusal::RangeUnavailable
        );
    }
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

/// Labels are fixed at node creation: no Cypher statement relabels a node out
/// of a projection kind, so a range cannot drop a projected document without
/// recording its deletion. The relabel attempts below must fail without
/// committing anything, after which the range still advances.
#[test]
fn relabel_out_of_a_projection_kind_cannot_commit() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "epoch_advance_relabel",
        IncrementalSearchMaintenanceOptions::default(),
    );
    create_memories(&handle, &["m1"]);
    catch_up(&handle).unwrap();
    let source = graph_epoch(&handle);
    for statement in [
        "MATCH (n:Memory {id: 'm1'}) REMOVE n:Memory SET n:Other",
        "MATCH (n:Memory {id: 'm1'}) REMOVE n:Memory",
        "MATCH (n:Memory {id: 'm1'}) SET n:Other",
        "MATCH (n:Memory {id: 'm1'}) SET n:Other:Memory",
    ] {
        assert!(
            handle.query_with_report(statement).is_err(),
            "{statement} unexpectedly succeeded"
        );
        assert_eq!(graph_epoch(&handle), source, "{statement} committed");
    }
    let labels = handle
        .query_with_report("MATCH (n:Memory {id: 'm1'}) RETURN count(n) AS c")
        .unwrap();
    assert_eq!(labels.output.rows[0].get("c"), Some(&Value::Int(1)));
    import_state_commits(&handle, 1);
    let report = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap();
    assert!(report.advanced);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

fn sql(handle: &EmbeddedStoreHandle, statement: &str) {
    handle
        .with_transaction(|transaction| transaction.query_sql(statement).map(|_| ()))
        .unwrap();
}

/// Mem's bookkeeping upsert, as one relational transaction.
fn upsert_migration_state(handle: &EmbeddedStoreHandle, value: &str) {
    sql(
        handle,
        &format!(
            "INSERT INTO content_migration_state (key, value, updated_at) \
             VALUES ('cursor', '{value}', 1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
        ),
    );
}

fn relational_fixture(
    name: &str,
    declared: &[&str],
) -> (std::path::PathBuf, EmbeddedStoreHandle, std::path::PathBuf) {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        &format!("{name}_bootstrap"),
        IncrementalSearchMaintenanceOptions::default(),
    );
    create_corpus(&handle);
    create_memories(&handle, &["m1"]);
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
    .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default())
    .with_non_projection_relational_tables(declared.iter().copied());
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    sql(
        &handle,
        "CREATE TABLE content_migration_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at BIGINT NOT NULL)",
    );
    sql(
        &handle,
        "CREATE TABLE memory_sources (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
    );
    // The schema statements are outside the ranges below.
    catch_up(&handle).unwrap();
    (root, handle, search)
}

/// Mem keeps import state in a relational table. A declared non-projection
/// table lets the advance through and is counted; any other table, and the
/// default (nothing declared), still refuse.
#[test]
fn declared_non_projection_relational_tables_are_allowed_and_counted() {
    let (root, handle, search) =
        relational_fixture("epoch_advance_relational", &["content_migration_state"]);
    let source = projection_epochs(&handle).1.unwrap();
    upsert_migration_state(&handle, "page-1");
    import_state_commits(&handle, 1);
    upsert_migration_state(&handle, "page-2");
    let target = graph_epoch(&handle);
    let before = files(&search);
    let report = handle
        .advance_search_projection_source_epoch(target)
        .unwrap();
    assert_metadata_only(&before, &files(&search), &report);
    assert_eq!(report.source_graph_commit_epoch_before, source);
    assert_eq!(report.allowed_relational_change_count, 2);
    assert_eq!(report.checked_change_log_entries, 4);

    // An undeclared table is a projection change for all HawDB knows.
    upsert_migration_state(&handle, "page-3");
    sql(
        &handle,
        "INSERT INTO memory_sources (id, body) VALUES ('s1', 'host mapped')",
    );
    let unchanged = files(&search);
    let error = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );
    assert!(error.to_string().contains("memory_sources"), "{error}");
    assert_eq!(files(&search), unchanged);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();

    // Default-deny: without a declaration the bookkeeping table refuses.
    let (root, handle, _) = relational_fixture("epoch_advance_relational_default", &[]);
    upsert_migration_state(&handle, "page-1");
    let error = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );
    assert!(
        error.to_string().contains("content_migration_state"),
        "{error}"
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_non_projection_relational_table_declarations_refuse_open() {
    let root = unique_embedded_store_test_dir("epoch_advance_declaration");
    for tables in [
        vec![""],
        vec!["content_migration_state", "content_migration_state"],
        vec!["hawdb_schema_migrations"],
    ] {
        let options = EmbeddedStoreOpenOptions::with_search_projection(
            root.join("graph"),
            root.join("search"),
            GraphMode::WritableCutover,
        )
        .with_non_projection_relational_tables(tables.clone());
        assert!(
            matches!(
                EmbeddedStoreHandle::open_with_options(options),
                Err(HawDBError::Semantic(_))
            ),
            "{tables:?}"
        );
    }
    let _ = std::fs::remove_dir_all(root);
}

/// The node-state check reads the current graph, so it only proves a range
/// that ends at the current epoch. A projected node changed inside (S, T] and
/// deleted after T would otherwise be skipped as absent.
#[test]
fn advance_to_a_past_epoch_is_refused() {
    let (root, handle, _) = relational_fixture("epoch_advance_past", &[]);
    handle
        .query_with_report("MATCH (m:Memory {id: 'm1'}) SET m.title = 'edited'")
        .unwrap();
    let edited = graph_epoch(&handle);
    handle
        .query_with_report("MATCH (m:Memory {id: 'm1'}) DETACH DELETE m")
        .unwrap();
    let error = handle
        .advance_search_projection_source_epoch(edited)
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::RangeUnavailable
    );
    // The current epoch sees the recorded deletion.
    let error = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap_err();
    assert_eq!(
        refusal(&error),
        SearchProjectionEpochAdvanceRefusal::ProjectionChanged
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// Graph writers racing the advance: every commit needs the store write lock
/// the advance holds, so each advance either proves the exact epoch it was
/// asked for or refuses with a typed reason, never a storage error, and the
/// projection still agrees with a rebuild afterwards.
#[test]
fn concurrent_writers_never_slip_past_an_advance() {
    let (root, handle, search) = relational_fixture("epoch_advance_race", &[]);
    let handle = std::sync::Arc::new(handle);
    let writer = {
        let handle = std::sync::Arc::clone(&handle);
        std::thread::spawn(move || {
            // The writer's statement locks and the advance's exclusive
            // database lease can meet in the deadlock detector; a retry
            // refusal commits nothing and is retried, as hosts do.
            let write = |statement: String| loop {
                match handle.query_with_report(&statement) {
                    Ok(_) => break,
                    Err(HawDBError::TransactionRetryRequired(_)) => continue,
                    Err(HawDBError::Execution(message))
                        if message.contains("runtime admission") =>
                    {
                        continue
                    }
                    Err(error) => panic!("{statement}: {error}"),
                }
            };
            for number in 0..40 {
                if number % 4 == 0 {
                    write(format!(
                        "CREATE (:Memory {{id: 'race{number}', title: 'race', content: 'race body', space_id: 'default'}})"
                    ));
                } else {
                    write(format!(
                        "MERGE (s:ImportState {{id: 'cursor'}}) SET s.page = {number}"
                    ));
                }
            }
        })
    };
    // Runtime admission may turn either side away while the other holds
    // its I/O slot; that refusal does no work and is simply retried.
    let admission = |error: &HawDBError| matches!(error, HawDBError::Execution(message) if message.contains("runtime admission"));
    let mut advanced = 0usize;
    let mut refused = 0usize;
    while !writer.is_finished() {
        std::thread::sleep(std::time::Duration::from_millis(2));
        match catch_up(&handle) {
            Ok(_) => {}
            Err(error) if admission(&error) => continue,
            Err(error) => panic!("catch-up: {error}"),
        }
        let target = graph_epoch(&handle);
        match handle.advance_search_projection_source_epoch(target) {
            Err(error) if admission(&error) => {}
            Ok(report) => {
                assert_eq!(report.source_graph_commit_epoch_after, target);
                assert_eq!(
                    projection_epochs(&handle).1.map(|e| e >= target),
                    Some(true)
                );
                advanced += 1;
            }
            Err(error) => {
                assert!(
                    matches!(
                        refusal(&error),
                        SearchProjectionEpochAdvanceRefusal::RangeUnavailable
                            | SearchProjectionEpochAdvanceRefusal::ProjectionChanged
                    ),
                    "{error}"
                );
                refused += 1;
            }
        }
    }
    writer.join().unwrap();
    assert!(advanced + refused > 0);
    catch_up(&handle).unwrap();
    let report = handle
        .advance_search_projection_source_epoch(graph_epoch(&handle))
        .unwrap();
    assert_eq!(
        Some(report.source_graph_commit_epoch_after),
        projection_epochs(&handle).1
    );
    assert_agrees_with_rebuild(&handle, &search, &root.join("rebuilt"));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
