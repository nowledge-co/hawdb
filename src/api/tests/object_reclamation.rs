// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `Database::checkpoint` reclaims superseded immutable objects while every
//! branch head, creation base root and pinned reader keeps its closure
//! (#1736).

use crate::{BranchCreateRequest, BranchSelector, Database, Value};
use hawdb_storage::immutable_object::ImmutableObjectStore;
use hawdb_storage::sealed_root::SealedRoot;
use std::collections::BTreeSet;
use std::path::Path;

fn sealed_root_digests(path: &Path) -> BTreeSet<String> {
    std::fs::read_dir(path.join("branches/objects/objects/sealed-root"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect()
}

fn head_root(
    path: &Path,
    branch: hawdb_core::Uuid,
) -> hawdb_storage::immutable_object::ObjectReference {
    hawdb_storage::branch_head::read_branch_head(
        &path
            .join("branches")
            .join(branch.to_string())
            .join("branch.head"),
    )
    .unwrap()
    .sealed_root
}

fn assert_closure_present(
    path: &Path,
    root: hawdb_storage::immutable_object::ObjectReference,
    context: &str,
) {
    let objects = ImmutableObjectStore::open(path.join("branches/objects")).unwrap();
    let sealed = SealedRoot::decode(&objects.read(root).unwrap()).unwrap();
    for reference in std::iter::once(sealed.durable_manifest)
        .chain(sealed.checkpoint_references.iter().copied())
        .chain(sealed.sealed_wals.iter().map(|wal| wal.object))
    {
        objects
            .verify_stored(reference)
            .unwrap_or_else(|error| panic!("{context}: {reference:?}: {error}"));
    }
}

fn memory_ids(rows: Vec<std::collections::BTreeMap<String, Value>>) -> Vec<Value> {
    let mut ids = rows
        .into_iter()
        .map(|row| row["m.id"].clone())
        .collect::<Vec<_>>();
    ids.sort_by_key(|value| format!("{value:?}"));
    ids
}

fn string_ids(ids: &[&str]) -> Vec<Value> {
    let mut ids = ids
        .iter()
        .map(|id| Value::String((*id).to_string()))
        .collect::<Vec<_>>();
    ids.sort_by_key(|value| format!("{value:?}"));
    ids
}

/// A child created from an older parent root, and a reader pinned on that
/// root, survive repeated parent checkpoints and their automatic sweeps; the
/// child then checkpoints (and sweeps) itself, and both branches reopen.
#[test]
fn parent_checkpoints_keep_child_from_older_root_and_pinned_reader() {
    let path = super::unique_test_dir("object_reclamation_child");
    let mut database = Database::open(&path).unwrap();
    database.query("CREATE (:Memory {id: 'root'})").unwrap();
    database.checkpoint().unwrap();
    let main = database
        .describe_branch(BranchSelector::Name("main".into()))
        .unwrap();
    let old_root = head_root(&path, main.id);
    let mut reader = database.begin_read_transaction().unwrap();
    let current = database.current_branch().unwrap().unwrap();
    let child = database
        .create_branch(BranchCreateRequest {
            name: Some("child".into()),
            parent: BranchSelector::Id(main.id),
            expected_source_commit_epoch: current.commit_epoch,
            owner: None,
            idempotency_key: "create-child".into(),
        })
        .unwrap();
    let child_head = head_root(&path, child.id);

    let mut superseded = BTreeSet::new();
    for round in 0..5 {
        superseded.insert(head_root(&path, main.id).sha256.to_string());
        database
            .query(&format!("CREATE (:Memory {{id: 'main-{round}'}})"))
            .unwrap();
        database.checkpoint().unwrap();
        let context = format!("round {round}");
        assert_closure_present(&path, old_root, &context);
        assert_closure_present(&path, child_head, &context);
        assert_closure_present(&path, head_root(&path, main.id), &context);
        let pressure = database.storage_pressure_snapshot().unwrap();
        assert!(!pressure.object_reclamation_retry_required, "{pressure:?}");
        assert!(pressure.object_store_bytes > 0);
    }
    // Superseded parent heads are gone except the one the reader pins and
    // the most recent predecessor.
    let roots = sealed_root_digests(&path);
    let retained_superseded = superseded
        .iter()
        .filter(|digest| roots.contains(*digest))
        .count();
    assert!(
        retained_superseded <= 2,
        "{retained_superseded} of {superseded:?}"
    );
    assert_eq!(
        memory_ids(
            reader
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .into_rows()
        ),
        string_ids(&["root"])
    );

    database.query_sql("USE BRANCH child").unwrap();
    assert_eq!(
        memory_ids(
            database
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .into_rows()
        ),
        string_ids(&["root"])
    );
    database
        .query("CREATE (:Memory {id: 'child-only'})")
        .unwrap();
    database.checkpoint().unwrap();
    assert!(
        !database
            .storage_pressure_snapshot()
            .unwrap()
            .object_reclamation_retry_required
    );
    assert_closure_present(&path, old_root, "after child sweep");
    assert_closure_present(&path, head_root(&path, main.id), "after child sweep");
    assert_eq!(
        memory_ids(
            reader
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .into_rows()
        ),
        string_ids(&["root"])
    );
    drop(reader);
    database.query_sql("USE BRANCH main").unwrap();
    database
        .query("CREATE (:Memory {id: 'main-last'})")
        .unwrap();
    database.checkpoint().unwrap();
    drop(database);

    let mut database = Database::open(&path).unwrap();
    assert_eq!(
        memory_ids(
            database
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .into_rows()
        ),
        string_ids(&[
            "root",
            "main-0",
            "main-1",
            "main-2",
            "main-3",
            "main-4",
            "main-last"
        ])
    );
    database.query_sql("USE BRANCH child").unwrap();
    assert_eq!(
        memory_ids(
            database
                .query("MATCH (m:Memory) RETURN m.id")
                .unwrap()
                .rows
                .into_rows()
        ),
        string_ids(&["root", "child-only"])
    );
    drop(database);
    let _ = std::fs::remove_dir_all(path);
}
