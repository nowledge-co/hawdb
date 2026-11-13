// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Repeated `EmbeddedStoreHandle::checkpoint()` calls must reclaim superseded
//! checkpoint objects from the project immutable-object store (#1736).
//!
//! Every checkpoint publishes its closure (relational checkpoint, manifests,
//! canonical/adjacency/property artifacts, row pages, ...) into
//! `branches/objects/objects/checkpoint-artifact/`. Once a newer head no
//! longer references an older closure and no reader pins it, the post-
//! publication sweep must remove it, with background maintenance on or off,
//! without debt and without losing data across a reopen.

use super::*;
use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind};
use hawdb_storage::sealed_root::SealedRoot;
use std::collections::BTreeSet;

const ROUNDS: usize = 10;
const ROWS_PER_ROUND: usize = 400;
const NODES_PER_ROUND: usize = 20;

fn open_options(graph: &std::path::Path, background_maintenance: bool) -> EmbeddedStoreOpenOptions {
    let mut config = DatabaseConfig {
        // Only explicit checkpoints publish in the foreground variant.
        automatic_checkpoint_max_age: Duration::from_secs(24 * 60 * 60),
        ..DatabaseConfig::default()
    };
    config.runtime_capabilities.background_maintenance = background_maintenance;
    EmbeddedStoreOpenOptions::graph_only(graph, GraphMode::WritableCutover)
        .with_database_config(config)
}

fn objects_root(graph: &std::path::Path) -> std::path::PathBuf {
    graph.join("branches").join("objects")
}

fn artifact_directory(graph: &std::path::Path) -> std::path::PathBuf {
    objects_root(graph)
        .join("objects")
        .join("checkpoint-artifact")
}

/// Bytes and files in `checkpoint-artifact/`, as a host would measure them.
fn artifact_usage(graph: &std::path::Path) -> (u64, usize) {
    std::fs::read_dir(artifact_directory(graph))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .fold((0, 0), |(bytes, files), len| (bytes + len, files + 1))
}

fn artifact_names(graph: &std::path::Path) -> BTreeSet<String> {
    std::fs::read_dir(artifact_directory(graph))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

fn main_head_path(graph: &std::path::Path) -> std::path::PathBuf {
    let hawdb_storage::branch_project::ProjectManifest::Branch(selector) =
        hawdb_storage::branch_project::inspect_project_manifest(graph).unwrap()
    else {
        panic!("expected a branch project")
    };
    graph
        .join("branches")
        .join(selector.main_branch_id().as_uuid().to_string())
        .join("branch.head")
}

/// Checkpoint artifacts (by digest name) and their bytes referenced by the
/// sealed root named `digest`.
fn root_closure(graph: &std::path::Path, digest: &str) -> BTreeMap<String, u64> {
    let bytes = std::fs::read(
        objects_root(graph)
            .join("objects")
            .join("sealed-root")
            .join(digest),
    )
    .unwrap();
    SealedRoot::decode(&bytes)
        .unwrap()
        .checkpoint_references
        .iter()
        .filter(|reference| reference.kind == ObjectKind::CheckpointArtifact)
        .map(|reference| (reference.sha256.to_string(), reference.byte_length))
        .collect()
}

fn head_root_digest(graph: &std::path::Path) -> String {
    hawdb_storage::branch_head::read_branch_head(&main_head_path(graph))
        .unwrap()
        .sealed_root
        .sha256
        .to_string()
}

/// Creation baselines recorded in the catalog; reclamation retains them.
fn base_root_digests(graph: &std::path::Path) -> Vec<String> {
    hawdb_storage::branch_catalog::read_catalog(&hawdb_storage::branch_project::catalog_path(graph))
        .unwrap()
        .branches
        .iter()
        .filter_map(|record| record.base_root_digest)
        .map(|digest| hawdb_integrity::Sha256Digest::from_bytes(digest).to_string())
        .collect()
}

fn write_round(handle: &EmbeddedStoreHandle, round: usize) {
    let body = "x".repeat(240);
    let start = round * ROWS_PER_ROUND;
    let rows = (start..start + ROWS_PER_ROUND).collect::<Vec<_>>();
    handle
        .with_transaction(|transaction| {
            for chunk in rows.chunks(200) {
                let values = chunk
                    .iter()
                    .map(|id| format!("({id}, '{body}{id}')"))
                    .collect::<Vec<_>>()
                    .join(", ");
                transaction
                    .query_sql(&format!("INSERT INTO content (id, body) VALUES {values}"))?;
            }
            for item in 0..NODES_PER_ROUND {
                transaction.query(&format!(
                    "CREATE (:Memory {{round: {round}, item: {item}}})"
                ))?;
            }
            Ok(())
        })
        .unwrap();
}

fn assert_contents(handle: &EmbeddedStoreHandle, rounds: usize, context: &str) {
    let rows = handle
        .with_transaction(|transaction| {
            transaction.query_sql("SELECT COUNT(*) AS c, SUM(id) AS s FROM content")
        })
        .unwrap()
        .rows;
    let total = rounds * ROWS_PER_ROUND;
    assert_eq!(
        rows[0]["c"],
        Value::Int(total as i64),
        "{context}: relational rows"
    );
    assert_eq!(
        rows[0]["s"],
        Value::Int((total * (total - 1) / 2) as i64),
        "{context}: relational row identities"
    );
    let nodes = handle
        .query_with_report("MATCH (m:Memory) RETURN count(m) AS c")
        .unwrap()
        .output
        .rows;
    assert_eq!(
        nodes[0]["c"],
        Value::Int((rounds * NODES_PER_ROUND) as i64),
        "{context}: graph nodes"
    );
}

fn assert_checkpoint_artifacts_bounded(background_maintenance: bool) {
    let root = unique_embedded_store_test_dir(&format!("object_reclaim_{background_maintenance}"));
    let graph = root.join("graph");
    let (handle, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&graph, background_maintenance))
            .unwrap();
    handle
        .with_transaction(|transaction| {
            transaction.query_sql("CREATE TABLE content (id BIGINT PRIMARY KEY, body TEXT)")?;
            Ok(())
        })
        .unwrap();
    let mut history = Vec::new();
    let mut previous_head = head_root_digest(&graph);
    for round in 0..ROUNDS {
        write_round(&handle, round);
        handle.checkpoint().unwrap();
        let head = head_root_digest(&graph);
        let closure = root_closure(&graph, &head);
        // Retained: the selected head, the head it superseded (still pinned
        // by the captured checkpoint source and the handle's read
        // publication while the post-publication sweep runs) and the
        // catalog creation baseline. Nothing else may survive.
        let mut live = closure.clone();
        live.extend(root_closure(&graph, &previous_head));
        for base in base_root_digests(&graph) {
            live.extend(root_closure(&graph, &base));
        }
        let present = artifact_names(&graph);
        let usage = artifact_usage(&graph);
        history.push((round, usage, closure.len(), closure.values().sum::<u64>()));
        assert!(
            present.iter().all(|name| live.contains_key(name)),
            "bg={background_maintenance} round {round}: unreachable checkpoint artifacts \
             survived: {:?}; history {history:?}",
            present
                .iter()
                .filter(|name| !live.contains_key(*name))
                .collect::<Vec<_>>()
        );
        assert!(closure.keys().all(|name| present.contains(name)));
        previous_head = head;
    }
    let pressure = handle.storage_pressure_snapshot().unwrap();
    assert!(
        !pressure.object_reclamation_retry_required
            && pressure.object_reclamation_pending_files == 0
            && pressure.object_reclamation_pending_bytes == 0,
        "bg={background_maintenance} object reclamation debt: {pressure:?}"
    );
    assert!(pressure.object_store_bytes >= artifact_usage(&graph).0);
    assert!(pressure.object_store_files as usize >= artifact_usage(&graph).1);
    assert!(!pressure
        .reason_codes
        .contains(&crate::StoragePressureReasonCode::ObjectReclamationDebt));

    // The explicit host entry point is admitted like `checkpoint()` and has
    // nothing left to do beyond the pinned predecessor.
    let report = handle
        .reclaim_branch_storage(crate::BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases, "{report:?}");
    assert_eq!(report.pending_objects, 0, "{report:?}");
    assert!(!report.retry_required, "{report:?}");
    assert_contents(&handle, ROUNDS, "before reopen");
    drop(handle);

    let (reopened, _) =
        EmbeddedStoreHandle::open_with_options(open_options(&graph, background_maintenance))
            .unwrap();
    assert_contents(&reopened, ROUNDS, "after reopen");
    write_round(&reopened, ROUNDS);
    reopened.checkpoint().unwrap();
    assert_contents(&reopened, ROUNDS + 1, "after post-reopen checkpoint");
    let mut live = root_closure(&graph, &head_root_digest(&graph));
    live.extend(root_closure(&graph, &previous_head));
    for base in base_root_digests(&graph) {
        live.extend(root_closure(&graph, &base));
    }
    assert!(artifact_names(&graph)
        .iter()
        .all(|name| live.contains_key(name)));
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn checkpoint_reclaims_superseded_objects() {
    assert_checkpoint_artifacts_bounded(false);
}

#[test]
fn checkpoint_reclaims_superseded_objects_background() {
    assert_checkpoint_artifacts_bounded(true);
}

/// A held reader keeps the closure it reads; the sweep removes it only once
/// that reader is released.
#[test]
fn reader_pins_its_checkpoint_closure_until_released() {
    let root = unique_embedded_store_test_dir("object_reclaim_reader_pin");
    let graph = root.join("graph");
    let (handle, _) = EmbeddedStoreHandle::open_with_options(open_options(&graph, false)).unwrap();
    handle
        .with_transaction(|transaction| {
            transaction.query_sql("CREATE TABLE content (id BIGINT PRIMARY KEY, body TEXT)")?;
            Ok(())
        })
        .unwrap();
    write_round(&handle, 0);
    handle.checkpoint().unwrap();
    let pinned_head =
        hawdb_storage::branch_head::read_branch_head(&main_head_path(&graph)).unwrap();
    let objects = ImmutableObjectStore::open(objects_root(&graph)).unwrap();
    let pinned_root = SealedRoot::decode(&objects.read(pinned_head.sealed_root).unwrap()).unwrap();
    let mut reader = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .begin_read_transaction()
        .unwrap();
    for round in 1..4 {
        write_round(&handle, round);
        handle.checkpoint().unwrap();
    }
    // Every object of the pinned closure is still present.
    objects.read(pinned_head.sealed_root).unwrap();
    for reference in &pinned_root.checkpoint_references {
        objects.verify_stored(*reference).unwrap();
    }
    let rows = reader
        .query("MATCH (m:Memory) RETURN count(m) AS c")
        .unwrap()
        .rows;
    assert_eq!(rows[0]["c"], Value::Int(NODES_PER_ROUND as i64));
    drop(reader);
    write_round(&handle, 4);
    handle.checkpoint().unwrap();
    assert!(objects.read(pinned_head.sealed_root).is_err());
    assert_contents(&handle, 5, "after the reader released its pin");
    drop(handle);
    let _ = std::fs::remove_dir_all(root);
}
