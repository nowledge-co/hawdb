// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Search-projection generation cleanup above the verification ceiling
//! through the embedded facade (#1755).
//!
//! Cleanup re-verifies the active closure before it deletes anything, bounded
//! by `max_verification_bytes`. Once a generation exceeded that ceiling every
//! pass refused, nothing was reclaimed and callers were not told. The ceiling
//! is lowered here so a small corpus exercises the same over-ceiling path.

use super::*;
use crate::{SearchOutOfCoreGenerationBuildOptions, SearchProjectionCleanupOptions};
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

const CEILING: u64 = 64 * 1024;
const DOCUMENTS: usize = 120;

fn cleanup_options(max_verification_bytes: u64) -> SearchProjectionCleanupOptions {
    SearchProjectionCleanupOptions {
        max_verification_bytes: NonZeroU64::new(max_verification_bytes).unwrap(),
        ..SearchProjectionCleanupOptions::default()
    }
}

fn body(id: usize, revision: usize) -> String {
    let mut state = (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ revision as u64;
    (0..160)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            format!("w{:x}", state & 0xffff_ffff)
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn row(id: usize, revision: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("m{id}"),
        title: format!("memory {id} revision {revision}"),
        body: body(id, revision),
        embedding: Some((0..16).map(|axis| ((id + axis) % 7) as f32 + 0.5).collect()),
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn delta(id: usize, revision: usize) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![row(id, revision)],
        max_operations: Some(1),
        ..Default::default()
    }
}

/// Generation numbers of every generation-named projection file.
fn file_generations(root: &Path) -> BTreeSet<u64> {
    std::fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let stem = name.strip_prefix("search_")?.strip_suffix(".hawdb")?;
            stem.rsplit('.').next()?.parse().ok()
        })
        .collect()
}

fn projection_bytes(root: &Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("search_"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

/// What a new process knows: no file of the closure was hashed by it yet.
fn forget_verifications(root: &Path) {
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            hawdb_storage::immutable_object::forget_published_object(&path).unwrap();
        }
    }
}

fn plant_obsolete(root: &Path) -> PathBuf {
    let path = root.join("search_projection_segment_payloads.0.hawdb");
    std::fs::write(&path, b"superseded by an earlier process").unwrap();
    path
}

fn bootstrap(search_path: &Path) -> u64 {
    let mut index = SearchIndex::open(search_path).unwrap();
    index.set_projection_cleanup_options(cleanup_options(CEILING));
    for id in 0..DOCUMENTS {
        index.upsert_projection_row(row(id, 0)).unwrap();
    }
    let report = index.checkpoint_with_report().unwrap();
    assert!(projection_bytes(search_path) > 4 * CEILING);
    report.projection_generation
}

#[test]
fn resident_checkpoints_above_the_ceiling_reclaim_and_report_cleanup() {
    let root = unique_embedded_store_test_dir("search_cleanup_ceiling_resident");
    let search_path = root.join("search");
    bootstrap(&search_path);
    let mut index = SearchIndex::open(&search_path).unwrap();
    index.set_projection_cleanup_options(cleanup_options(CEILING));
    let graph = EmbeddedGraph::from_database(Database::new(), GraphMode::WritableCutover);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(
        graph,
        Some(SearchProjection::from_index(index)),
    ))
    .unwrap();

    for round in 1..=5 {
        let report = handle
            .apply_search_projection_delta_and_checkpoint(delta(round, round))
            .unwrap();
        let cleanup = report.projection_cleanup.expect("checkpoint ran cleanup");
        assert!(!cleanup.retry_required, "round {round}: {cleanup:?}");
        assert!(!cleanup.closure_unverified, "round {round}: {cleanup:?}");
        assert!(cleanup.verification_reused_bytes > CEILING, "{cleanup:?}");
        let generations = file_generations(&search_path);
        assert!(generations.len() <= 2, "round {round}: {generations:?}");
    }
    let report = handle.checkpoint_initial_import_projection().unwrap();
    let cleanup = report.projection_cleanup.expect("checkpoint ran cleanup");
    assert!(!cleanup.retry_required, "{cleanup:?}");
    assert!(cleanup.deleted_files > 0, "{cleanup:?}");
    assert!(file_generations(&search_path).len() <= 2);

    // A restarted process has verified nothing; the explicit call with
    // limits that cover the generation reclaims, a smaller one refuses.
    let obsolete = plant_obsolete(&search_path);
    forget_verifications(&search_path);
    let refused = handle
        .retry_search_projection_cleanup(cleanup_options(CEILING))
        .unwrap();
    assert!(refused.retry_required, "{refused:?}");
    assert!(refused
        .failure_kinds
        .contains_key("captured_closure_unverified"));
    assert!(obsolete.exists());
    let reclaimed = handle
        .retry_search_projection_cleanup(cleanup_options(64 * CEILING))
        .unwrap();
    assert!(!reclaimed.retry_required, "{reclaimed:?}");
    assert_eq!(reclaimed.deleted_files, 1, "{reclaimed:?}");
    assert!(!obsolete.exists());
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, snapshot.completions);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_publications_above_the_ceiling_reclaim_and_report_cleanup() {
    let root = unique_embedded_store_test_dir("search_cleanup_ceiling_incremental");
    let search_path = root.join("search");
    let bootstrap_generation = bootstrap(&search_path);
    let maintenance = super::super::IncrementalSearchMaintenanceOptions {
        build_options: SearchOutOfCoreGenerationBuildOptions {
            cleanup_options: cleanup_options(CEILING),
            ..Default::default()
        },
        ..Default::default()
    };
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        search_path.clone(),
        GraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(maintenance);
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();

    let mut current = bootstrap_generation;
    for round in 1..=5 {
        // Appended segments stay referenced by the manifest; a superseded
        // generation file is what a publication must reclaim.
        let obsolete = plant_obsolete(&search_path);
        let report = handle
            .apply_search_projection_delta_and_checkpoint(delta(DOCUMENTS + round, 0))
            .unwrap();
        let cleanup = report.projection_cleanup.expect("publication ran cleanup");
        assert!(!cleanup.retry_required, "round {round}: {cleanup:?}");
        assert!(!cleanup.closure_unverified, "round {round}: {cleanup:?}");
        assert_eq!(cleanup.deleted_files, 1, "round {round}: {cleanup:?}");
        assert!(!obsolete.exists());
        let reader = crate::SearchOutOfCoreReader::open(&search_path).unwrap();
        assert!(reader.generation() > current);
        current = reader.generation();
        drop(reader);
        // Nothing an unbounded pass could still reclaim was left behind.
        let unbounded = handle
            .retry_search_projection_cleanup(cleanup_options(u64::MAX))
            .unwrap();
        assert!(!unbounded.retry_required, "round {round}: {unbounded:?}");
        assert_eq!(unbounded.deleted_files, 0, "round {round}: {unbounded:?}");
    }
    assert!(projection_bytes(&search_path) > 4 * CEILING);

    let obsolete = plant_obsolete(&search_path);
    forget_verifications(&search_path);
    let refused = handle
        .retry_search_projection_cleanup(cleanup_options(CEILING))
        .unwrap();
    assert!(refused.retry_required, "{refused:?}");
    assert_eq!(refused.delete_attempts, 0, "{refused:?}");
    assert!(obsolete.exists());
    let reclaimed = handle
        .retry_search_projection_cleanup(cleanup_options(64 * CEILING))
        .unwrap();
    assert!(!reclaimed.retry_required, "{reclaimed:?}");
    assert!(!obsolete.exists());
    let snapshot = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(snapshot.admissions, snapshot.completions);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn explicit_search_cleanup_requires_an_open_projection() {
    let graph = EmbeddedGraph::from_database(Database::new(), GraphMode::WritableCutover);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap();
    assert!(handle
        .retry_search_projection_cleanup(SearchProjectionCleanupOptions::default())
        .is_err());
}
