// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Storage pressure through the admitted embedded handle
//! (`nowledge-co/hawdb#1725`).
//!
//! Hosts that disable background maintenance and checkpoint explicitly need
//! to prove that no generation reclamation debt remains. These tests pin
//! `EmbeddedStoreHandle::storage_pressure_snapshot` to the `Database`
//! contract: the same snapshot, failed deletions reported as pending debt,
//! and a held reader pin retaining older generations until it is released.

use super::*;
use crate::StoragePressureSnapshot;

fn open_explicit_checkpoint_handle(graph_path: &std::path::Path) -> EmbeddedStoreHandle {
    let mut options = EmbeddedStoreOpenOptions::graph_only(graph_path, GraphMode::WritableCutover);
    // Only explicit checkpoints may publish generations during these tests.
    options.database_config = Some(crate::DatabaseConfig {
        automatic_checkpoint_max_age: Duration::from_secs(24 * 60 * 60),
        ..crate::DatabaseConfig::default()
    });
    EmbeddedStoreHandle::open_with_options(options).unwrap().0
}

fn create_memory(handle: &EmbeddedStoreHandle, id: usize) {
    handle
        .query_with_report(&format!("CREATE (:Memory {{id: 'memory-{id}'}})"))
        .unwrap();
}

fn database_snapshot(handle: &EmbeddedStoreHandle) -> StoragePressureSnapshot {
    handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .storage_pressure_snapshot()
        .unwrap()
}

/// Clears the two signals sampled from the wall clock and filesystem, which
/// may legitimately move between two otherwise identical observations.
fn without_ambient_samples(mut snapshot: StoragePressureSnapshot) -> StoragePressureSnapshot {
    snapshot.wal_age_millis = 0;
    snapshot.available_free_space_bytes = None;
    snapshot
        .reason_codes
        .retain(|reason| *reason != crate::StoragePressureReasonCode::FreeSpaceReserve);
    snapshot
}

/// The selected main branch's graph data directory, which owns the WAL and
/// checkpoint generations that storage pressure accounts for.
fn graph_data_dir(graph_path: &std::path::Path) -> std::path::PathBuf {
    let hawdb_storage::branch_project::ProjectManifest::Branch(selector) =
        hawdb_storage::branch_project::inspect_project_manifest(graph_path).unwrap()
    else {
        panic!("expected a branch project")
    };
    graph_path
        .join("branches")
        .join(selector.main_branch_id().as_uuid().to_string())
        .join("data")
}

/// Generation-numbered `{prefix}.{generation}.hawdb` files in the graph data
/// directory, as a host would list them.
fn generation_files(graph_path: &std::path::Path, prefix: &str) -> Vec<(u64, std::path::PathBuf)> {
    let mut found = std::fs::read_dir(graph_data_dir(graph_path))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter_map(|path| {
            let generation = path
                .file_name()?
                .to_str()?
                .strip_prefix(&format!("{prefix}."))?
                .strip_suffix(".hawdb")?
                .parse::<u64>()
                .ok()?;
            Some((generation, path))
        })
        .collect::<Vec<_>>();
    found.sort();
    found
}

#[test]
fn handle_storage_pressure_matches_the_database_snapshot() {
    let root = unique_embedded_store_test_dir("handle_storage_pressure_parity");
    let graph_path = root.join("graph");
    let handle = open_explicit_checkpoint_handle(&graph_path);

    // Given WAL debt from an uncheckpointed write.
    create_memory(&handle, 1);
    let pending = handle.storage_pressure_snapshot().unwrap();
    assert!(pending.wal_bytes > 0);
    assert!(pending.current_commit_epoch > pending.checkpoint_commit_epoch);
    assert_eq!(
        without_ambient_samples(pending),
        without_ambient_samples(database_snapshot(&handle))
    );

    // When an explicit checkpoint publishes it, both views report no debt.
    handle.checkpoint().unwrap();
    let clean = handle.storage_pressure_snapshot().unwrap();
    assert_eq!(clean.current_commit_epoch, clean.checkpoint_commit_epoch);
    assert_eq!(clean.wal_age_millis, 0);
    assert_eq!(clean.generation_reclamation_pending_files, 0);
    assert_eq!(clean.generation_reclamation_pending_bytes, 0);
    assert!(!clean.generation_reclamation_retry_required);
    // The handle's own standing read publication is a reader at the current
    // epoch, exactly as the Database pin registry reports it.
    assert_eq!(
        clean.oldest_reader_commit_epoch,
        Some(clean.current_commit_epoch)
    );
    assert_eq!(clean.oldest_reader_lag, 0);
    assert_eq!(
        without_ambient_samples(clean),
        without_ambient_samples(database_snapshot(&handle))
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "test-support")]
fn handle_storage_pressure_reports_failed_generation_reclamation_until_retried() {
    let root = unique_embedded_store_test_dir("handle_storage_pressure_reclamation_debt");
    let graph_path = root.join("graph");
    let handle = open_explicit_checkpoint_handle(&graph_path);
    for id in 1..=2 {
        create_memory(&handle, id);
        handle.checkpoint().unwrap();
    }
    // Given an obsolete checkpoint generation whose deletion fails once.
    let (oldest, oldest_path) = generation_files(&graph_path, "checkpoint")
        .into_iter()
        .next()
        .unwrap();
    let name = oldest_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    create_memory(&handle, 3);
    hawdb_storage::store::set_generation_reclamation_remove_failpoint(Some(name));
    let checkpoint = handle.checkpoint();
    hawdb_storage::store::set_generation_reclamation_remove_failpoint(None);
    checkpoint.unwrap();

    // Then the handle reports the retained file as reclamation debt.
    let debt = handle.storage_pressure_snapshot().unwrap();
    assert!(oldest_path.exists(), "generation {oldest} must be retained");
    assert!(debt.generation_reclamation_retry_required);
    assert_eq!(debt.generation_reclamation_pending_files, 1);
    assert_eq!(
        debt.generation_reclamation_pending_bytes,
        std::fs::metadata(&oldest_path).unwrap().len()
    );
    assert!(debt
        .reason_codes
        .contains(&crate::StoragePressureReasonCode::GenerationReclamationDebt));
    assert!(debt.recommends_checkpoint());
    assert_eq!(
        without_ambient_samples(debt),
        without_ambient_samples(database_snapshot(&handle))
    );

    // When the next explicit checkpoint retries reclamation, no debt remains.
    handle.checkpoint().unwrap();
    let cleared = handle.storage_pressure_snapshot().unwrap();
    assert!(!oldest_path.exists());
    assert!(!cleared.generation_reclamation_retry_required);
    assert_eq!(cleared.generation_reclamation_pending_files, 0);
    assert_eq!(cleared.generation_reclamation_pending_bytes, 0);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn handle_storage_pressure_shows_reader_pinned_generations_until_release() {
    let root = unique_embedded_store_test_dir("handle_storage_pressure_reader_pin");
    let graph_path = root.join("graph");
    let handle = open_explicit_checkpoint_handle(&graph_path);
    create_memory(&handle, 0);
    handle.checkpoint().unwrap();

    // Given a reader pinned to an older committed generation.
    let (permit, reader) = handle.canonical_read_transaction(4096, None).unwrap();
    let pinned = reader.published_read_view();
    let pinned_generation = pinned.physical_generation().unwrap().0;
    // When later writes are checkpointed while that reader is still open.
    for id in 1..=3 {
        create_memory(&handle, id);
        handle.checkpoint().unwrap();
    }

    // Then the pinned WAL generation stays on disk, and the handle reports it
    // as reader-retained rather than as a failed delete.
    let retained = handle.storage_pressure_snapshot().unwrap();
    assert!(generation_files(&graph_path, "wal")
        .iter()
        .any(|(generation, _)| *generation == pinned_generation));
    assert!(generation_files(&graph_path, "wal").len() > 2);
    assert_eq!(
        retained.oldest_reader_commit_epoch,
        Some(pinned.visible_commit_epoch())
    );
    assert_eq!(
        retained.oldest_reader_lag,
        retained.current_commit_epoch - pinned.visible_commit_epoch()
    );
    assert!(retained.oldest_reader_lag > 0);
    assert!(retained.obsolete_generation_bytes > 0);
    assert!(retained
        .reason_codes
        .contains(&crate::StoragePressureReasonCode::ReaderPinnedObsoleteGenerations));
    assert_eq!(retained.generation_reclamation_pending_files, 0);
    assert_eq!(
        without_ambient_samples(retained),
        without_ambient_samples(database_snapshot(&handle))
    );

    // When the reader is released and another explicit checkpoint runs.
    drop(reader);
    drop(permit);
    handle.checkpoint().unwrap();

    // Then no reader lag or reclamation debt remains, and at most the
    // current and previous WAL generations remain on disk.
    let released = handle.storage_pressure_snapshot().unwrap();
    assert_eq!(released.oldest_reader_lag, 0);
    assert!(!released.generation_reclamation_retry_required);
    assert_eq!(released.generation_reclamation_pending_files, 0);
    assert_eq!(released.generation_reclamation_pending_bytes, 0);
    let wal_files = generation_files(&graph_path, "wal");
    assert!(wal_files.len() <= 2, "{wal_files:?}");
    assert!(wal_files
        .iter()
        .all(|(generation, _)| *generation != pinned_generation));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
