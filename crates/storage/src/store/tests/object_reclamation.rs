// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Post-publication reclamation of superseded immutable objects (#1736).

use super::*;
use crate::branch_reclamation::{
    set_object_reclamation_failpoint, BranchReclamationLimits, ObjectReclamationFailpoint,
};

fn checkpoint(branch: &mut AdmittedBranchStore) -> Result<()> {
    let (store, catalog) = branch.store_and_catalog_mut();
    store.checkpoint(catalog)
}

fn message_rows(store: &GraphStore) -> usize {
    store.relational_state().row_count("messages")
}

/// Every object name in the project immutable namespace (staging excluded).
fn object_names(fixture: &BranchFixture) -> BTreeSet<(String, String)> {
    let mut names = BTreeSet::new();
    for kind in [
        "checkpoint",
        "sealed-wal",
        "sealed-root",
        "checkpoint-artifact",
        "durable-manifest",
    ] {
        let directory = fixture.objects.join("objects").join(kind);
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            names.insert((
                kind.to_string(),
                entry.unwrap().file_name().into_string().unwrap(),
            ));
        }
    }
    names
}

fn kind_directory(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Checkpoint => "checkpoint",
        ObjectKind::SealedWal => "sealed-wal",
        ObjectKind::SealedRoot => "sealed-root",
        ObjectKind::CheckpointArtifact => "checkpoint-artifact",
        ObjectKind::DurableManifest => "durable-manifest",
    }
}

/// The complete closure (root included) of a sealed root, by object name.
fn closure(fixture: &BranchFixture, root: ObjectReference) -> BTreeSet<(String, String)> {
    let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
    let sealed = SealedRoot::decode(&objects.read(root).unwrap()).unwrap();
    std::iter::once(root)
        .chain(std::iter::once(sealed.durable_manifest))
        .chain(sealed.checkpoint_references.iter().copied())
        .chain(sealed.sealed_wals.iter().map(|wal| wal.object))
        .map(|reference| {
            (
                kind_directory(reference.kind).to_string(),
                reference.sha256.to_string(),
            )
        })
        .collect()
}

fn assert_closure_present(fixture: &BranchFixture, root: ObjectReference, context: &str) {
    let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
    let sealed = SealedRoot::decode(&objects.read(root).unwrap()).unwrap();
    objects.verify_stored(sealed.durable_manifest).unwrap();
    for reference in sealed
        .checkpoint_references
        .iter()
        .copied()
        .chain(sealed.sealed_wals.iter().map(|wal| wal.object))
    {
        objects
            .verify_stored(reference)
            .unwrap_or_else(|error| panic!("{context}: {reference:?}: {error}"));
    }
}

fn base_root(fixture: &BranchFixture, id: branch_catalog::BranchId) -> ObjectReference {
    let catalog = branch_catalog::read_catalog(&fixture.catalog_path).unwrap();
    let digest = catalog
        .branches
        .iter()
        .find(|record| record.id == id)
        .unwrap()
        .base_root_digest
        .unwrap();
    let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
    objects
        .sealed_root_by_digest(hawdb_integrity::Sha256Digest::from_bytes(digest))
        .unwrap()
        .unwrap()
}

fn object_debt(store: &GraphStore) -> (bool, u64, u64) {
    let pressure = store.storage_pressure_snapshot(None);
    (
        pressure.object_reclamation_retry_required,
        pressure.object_reclamation_pending_files,
        pressure.object_reclamation_pending_bytes,
    )
}

fn assert_no_object_debt(store: &GraphStore, context: &str) {
    assert_eq!(object_debt(store), (false, 0, 0), "{context}");
    assert!(
        !store
            .storage_pressure_snapshot(None)
            .reason_codes
            .contains(&crate::pressure::StoragePressureReasonCode::ObjectReclamationDebt),
        "{context}"
    );
}

/// Liveness spans every branch: a child created from an older parent root,
/// that child's own head, and a reader pinned on the old parent root all
/// survive repeated parent checkpoints, each followed by an automatic sweep
/// that still removes the parent's own superseded closures.
#[test]
fn parent_sweeps_keep_child_base_root_and_pinned_reader_closures() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let fixture = BranchFixture::new();
        let mut main = fixture.admit(fixture.main, durability);
        write_schema_and_graph(&mut main);
        checkpoint(&mut main).unwrap();
        let old_root = main.head().sealed_root;
        // A reader on the old parent root and a child created from it.
        let reader = main.store().snapshot_for_read();
        fixture.fork(&main, branch_id(2), "child");
        let child_base = base_root(&fixture, branch_id(2));
        assert_eq!(child_base, old_root);
        let child_head = branch_head::read_branch_head(&fixture.head_path(branch_id(2)))
            .unwrap()
            .sealed_root;

        let mut superseded = Vec::new();
        for round in 0..4 {
            superseded.push(main.head().sealed_root);
            write_row(&mut main, &format!("main-{round}-{}", "x".repeat(8192)));
            checkpoint(&mut main).unwrap();
            assert_no_object_debt(main.store(), &format!("{durability:?} round {round}"));
            for (context, root) in [
                ("old parent root", old_root),
                ("child base root", child_base),
                ("child head", child_head),
                ("parent head", main.head().sealed_root),
            ] {
                assert_closure_present(
                    &fixture,
                    root,
                    &format!("{durability:?} round {round}: {context}"),
                );
            }
        }
        // Parent heads that nothing else references were reclaimed, so the
        // namespace holds only the live closures.
        let mut live = closure(&fixture, main.head().sealed_root);
        for root in [old_root, child_head, base_root(&fixture, fixture.main)] {
            live.extend(closure(&fixture, root));
        }
        assert_eq!(object_names(&fixture), live, "{durability:?}");
        for root in &superseded[1..] {
            assert!(!object_names(&fixture)
                .contains(&("sealed-root".to_string(), root.sha256.to_string())));
        }
        assert_eq!(message_rows(&reader), 1);
        assert_eq!(reader.node_count_for_label(None), 3);

        // The child admits from its head and base after every sweep, keeps
        // reading while the parent continues, and checkpoints itself.
        let mut child = fixture.admit(branch_id(2), durability);
        assert_eq!(message_rows(child.store()), 1);
        assert_eq!(child.store().node_count_for_label(None), 3);
        for round in 0..2 {
            write_row(&mut main, &format!("main-late-{round}"));
            checkpoint(&mut main).unwrap();
            assert_no_object_debt(main.store(), "parent sweep with an admitted child");
            assert_eq!(message_rows(child.store()), 1);
        }
        write_row(&mut child, "child-only");
        checkpoint(&mut child).unwrap();
        assert_no_object_debt(child.store(), "child sweep");
        assert_closure_present(&fixture, child_base, "child base after child sweep");
        assert_closure_present(&fixture, old_root, "reader root after child sweep");
        assert_closure_present(&fixture, main.head().sealed_root, "parent head");
        assert_eq!(message_rows(&reader), 1);
        drop(reader);
        drop(child);
        let child = fixture.admit(branch_id(2), durability);
        assert_eq!(message_rows(child.store()), 2);
        drop(child);
        drop(main);
        let main = fixture.admit(fixture.main, durability);
        assert_eq!(message_rows(main.store()), 7);
        assert_eq!(main.store().node_count_for_label(None), 3);
    }
}

/// A fault in any phase keeps the checkpoint successful, records retryable
/// object debt, and the next checkpoint finishes the sweep.
#[test]
fn object_sweep_faults_are_retryable_debt() {
    for failpoint in [
        ObjectReclamationFailpoint::Listing,
        ObjectReclamationFailpoint::DirectorySyncBeforeRemoval,
        ObjectReclamationFailpoint::Remove,
        ObjectReclamationFailpoint::DirectorySyncAfterRemoval,
    ] {
        let fixture = BranchFixture::new();
        let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
        write_schema_and_graph(&mut main);
        checkpoint(&mut main).unwrap();
        write_row(&mut main, &"first".repeat(4096));
        checkpoint(&mut main).unwrap();
        // The faulty checkpoint below supersedes this head.
        let superseded = main.head().sealed_root;
        let superseded_name = ("sealed-root".to_string(), superseded.sha256.to_string());
        write_row(&mut main, &"second".repeat(4096));

        set_object_reclamation_failpoint(Some(failpoint));
        let result = checkpoint(&mut main);
        set_object_reclamation_failpoint(None);
        result.unwrap();
        let (retry, pending_files, pending_bytes) = object_debt(main.store());
        assert!(retry, "{failpoint:?}");
        let names = object_names(&fixture);
        match failpoint {
            ObjectReclamationFailpoint::Listing => {
                assert!(names.contains(&superseded_name), "{failpoint:?}");
            }
            ObjectReclamationFailpoint::DirectorySyncBeforeRemoval => {
                // Nothing is removed before every head is durable.
                assert!(names.contains(&superseded_name), "{failpoint:?}");
                assert!(pending_files > 0 && pending_bytes > 0, "{failpoint:?}");
            }
            ObjectReclamationFailpoint::Remove => {
                assert_eq!(pending_files, 1, "{failpoint:?}");
                assert!(pending_bytes > 0, "{failpoint:?}");
            }
            ObjectReclamationFailpoint::DirectorySyncAfterRemoval => {
                assert!(!names.contains(&superseded_name), "{failpoint:?}");
                assert_eq!((pending_files, pending_bytes), (0, 0), "{failpoint:?}");
            }
        }
        assert!(main
            .store()
            .storage_pressure_snapshot(None)
            .reason_codes
            .contains(&crate::pressure::StoragePressureReasonCode::ObjectReclamationDebt));

        write_row(&mut main, "retry");
        checkpoint(&mut main).unwrap();
        assert_no_object_debt(main.store(), &format!("{failpoint:?} retry"));
        assert!(!object_names(&fixture).contains(&superseded_name));
        assert_eq!(message_rows(main.store()), 4);
        drop(main);
        let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
        assert_eq!(message_rows(main.store()), 4);
    }
}

/// A checkpoint with nothing new to publish still retries object debt, and an
/// automatic pass never waits behind a held catalog metadata lock.
#[test]
fn pending_object_debt_retries_without_waiting_for_the_metadata_lock() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    for round in 0..3 {
        write_row(&mut main, &format!("row-{round}"));
        checkpoint(&mut main).unwrap();
    }
    write_row(&mut main, "listed");
    set_object_reclamation_failpoint(Some(ObjectReclamationFailpoint::Listing));
    let result = checkpoint(&mut main);
    set_object_reclamation_failpoint(None);
    result.unwrap();
    assert!(object_debt(main.store()).0);
    let before = object_names(&fixture);

    let metadata =
        branch_catalog::CatalogMetadataLease::acquire(fixture.catalog_path.parent().unwrap())
            .unwrap();
    main.store().retry_pending_object_reclamation();
    assert!(object_debt(main.store()).0);
    assert_eq!(object_names(&fixture), before);
    drop(metadata);

    main.store().retry_pending_object_reclamation();
    assert_no_object_debt(main.store(), "retry after the lock is released");
    assert!(object_names(&fixture).len() < before.len());
    assert_eq!(message_rows(main.store()), 5);
}

/// Explicit reclamation never refuses a namespace above its limits: each pass
/// removes at most its budget (at least one object) and reports the rest.
#[test]
fn explicit_reclamation_makes_bounded_progress_above_its_limits() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    // A held reader keeps every superseded closure reachable for now.
    let reader = main.store().snapshot_for_read();
    let readers = (0..4)
        .map(|round| {
            write_row(&mut main, &format!("garbage-{round}-{}", "y".repeat(8192)));
            checkpoint(&mut main).unwrap();
            main.store().snapshot_for_read()
        })
        .collect::<Vec<_>>();
    drop(reader);
    drop(readers);
    let garbage_before = object_names(&fixture).len();
    // Objects published by an earlier process have no publication record and
    // must be re-hashed; only those bytes count against `max_bytes`.
    for (kind, name) in object_names(&fixture) {
        let _ = crate::immutable_object::forget_published_object(
            &fixture.objects.join("objects").join(kind).join(name),
        );
    }
    let tiny = BranchReclamationLimits {
        max_objects: 100,
        max_bytes: 1,
        ..Default::default()
    };
    let first = main.store_mut().reclaim_branch_storage(tiny).unwrap();
    assert_eq!(first.reclaimed_objects, 1, "{first:?}");
    assert!(first.pending_objects > 0, "{first:?}");
    assert!(first.pending_bytes > 0, "{first:?}");
    assert!(!first.retry_required, "{first:?}");
    assert_eq!(
        first.object_store_objects as usize,
        garbage_before - 1,
        "{first:?}"
    );
    // The explicit receipt is visible as pending debt.
    let (retry, files, bytes) = object_debt(main.store());
    assert!(retry);
    assert_eq!((files, bytes), (first.pending_objects, first.pending_bytes));

    let budget = BranchReclamationLimits {
        max_objects: 3,
        ..Default::default()
    };
    let mut passes = 0;
    loop {
        let report = main.store_mut().reclaim_branch_storage(budget).unwrap();
        assert!(report.reclaimed_objects <= 3, "{report:?}");
        passes += 1;
        if report.pending_objects == 0 {
            break;
        }
        assert!(passes < 100, "{report:?}");
    }
    assert!(passes > 1);
    assert_no_object_debt(main.store(), "drained");
    let mut live = closure(&fixture, main.head().sealed_root);
    live.extend(closure(&fixture, base_root(&fixture, fixture.main)));
    assert_eq!(object_names(&fixture), live);
    drop(main);
    let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(message_rows(main.store()), 5);
}

/// One corrupt unreachable object without quarantine evidence is retained and
/// reported by name; automatic passes keep sweeping everything else and do
/// not turn it into permanent retry debt. Explicit reclamation still refuses.
#[test]
fn corrupt_unreachable_object_is_retained_reported_and_does_not_block_sweeps() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    let digest = "c".repeat(64);
    let corrupt_path = fixture
        .objects
        .join("objects")
        .join("checkpoint-artifact")
        .join(&digest);
    fs::write(&corrupt_path, b"bytes that do not hash to their name").unwrap();
    for round in 0..3 {
        let superseded = main.head().sealed_root;
        write_row(&mut main, &format!("row-{round}"));
        checkpoint(&mut main).unwrap();
        let names = object_names(&fixture);
        assert!(
            !names.contains(&("sealed-root".to_string(), superseded.sha256.to_string())),
            "round {round}: the sweep stopped at the corrupt object"
        );
        assert!(corrupt_path.exists());
        let pressure = main.store().storage_pressure_snapshot(None);
        assert!(!pressure.object_reclamation_retry_required, "{pressure:?}");
        assert_eq!(pressure.object_reclamation_skipped_files, 1);
        assert_eq!(
            pressure.object_reclamation_skipped_bytes,
            fs::metadata(&corrupt_path).unwrap().len()
        );
        let corrupt = pressure.object_reclamation_corrupt_object.unwrap();
        assert_eq!(corrupt.kind, ObjectKind::CheckpointArtifact);
        assert_eq!(corrupt.sha256.to_string(), digest);
        assert!(pressure
            .reason_codes
            .contains(&crate::pressure::StoragePressureReasonCode::ObjectReclamationCorruptObject));
    }
    assert!(main
        .store_mut()
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .is_err());
    assert!(corrupt_path.exists());
    // Removing the damaged file clears the report at the next pass.
    fs::remove_file(&corrupt_path).unwrap();
    write_row(&mut main, "clean");
    checkpoint(&mut main).unwrap();
    let pressure = main.store().storage_pressure_snapshot(None);
    assert_eq!(pressure.object_reclamation_skipped_files, 0);
    assert!(pressure.object_reclamation_corrupt_object.is_none());
    assert_no_object_debt(main.store(), "after the corrupt object is gone");
    assert_eq!(message_rows(main.store()), 5);
}

/// A second admitted owner (another branch) holds a reader pinned on one of
/// its own superseded roots. Its head no longer references that root, so
/// only its registered pin keeps it; repeated checkpoints and sweeps by the
/// other owner must keep the closure intact and readable.
#[test]
fn another_owners_registered_reader_pin_survives_foreign_sweeps() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    fixture.fork(&main, branch_id(2), "child");
    let mut child = fixture.admit(branch_id(2), DurabilityPolicy::SyncOnEveryWrite);
    write_row(&mut child, &"child-pinned".repeat(1024));
    checkpoint(&mut child).unwrap();
    let pinned_root = child.head().sealed_root;
    let reader = child.store().snapshot_for_read();
    write_row(&mut child, "child-later");
    checkpoint(&mut child).unwrap();
    assert_ne!(child.head().sealed_root, pinned_root);
    assert_closure_present(&fixture, pinned_root, "after the child's own sweep");

    for round in 0..4 {
        write_row(&mut main, &format!("main-{round}-{}", "z".repeat(4096)));
        checkpoint(&mut main).unwrap();
        assert_no_object_debt(main.store(), &format!("main round {round}"));
        assert_closure_present(&fixture, pinned_root, &format!("main round {round}"));
        assert_eq!(message_rows(&reader), 2);
    }
    let report = main
        .store_mut()
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    assert!(!report.deferred_for_active_leases, "{report:?}");
    assert_closure_present(&fixture, pinned_root, "explicit foreign sweep");
    assert_eq!(message_rows(&reader), 2);

    // Released: the next sweep by the other owner reclaims it.
    drop(reader);
    write_row(&mut main, "main-after-release");
    checkpoint(&mut main).unwrap();
    assert!(!object_names(&fixture)
        .contains(&("sealed-root".to_string(), pinned_root.sha256.to_string())));
    assert_eq!(message_rows(child.store()), 3);
    drop(child);
    let child = fixture.admit(branch_id(2), DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(message_rows(child.store()), 3);
}

/// Two owners publish and sweep concurrently: each sweep must either defer
/// on the other's publication lock or see its fully pinned root. No head,
/// base or closure is ever lost, and both branches reopen intact.
#[test]
fn concurrent_owner_publications_and_sweeps_never_lose_a_live_closure() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    fixture.fork(&main, branch_id(2), "child");
    let child = fixture.admit(branch_id(2), DurabilityPolicy::SyncOnEveryWrite);
    const ROUNDS: usize = 8;
    let run = |mut branch: AdmittedBranchStore, name: &'static str| {
        std::thread::spawn(move || {
            for round in 0..ROUNDS {
                write_row(&mut branch, &format!("{name}-{round}-{}", "w".repeat(2048)));
                checkpoint(&mut branch).unwrap();
            }
            branch
        })
    };
    let main_thread = run(main, "main");
    let child_thread = run(child, "child");
    let main = main_thread.join().unwrap();
    let child = child_thread.join().unwrap();
    for (context, root) in [
        ("main head", main.head().sealed_root),
        ("child head", child.head().sealed_root),
        ("child base", base_root(&fixture, branch_id(2))),
        ("main base", base_root(&fixture, fixture.main)),
    ] {
        assert_closure_present(&fixture, root, context);
    }
    assert_eq!(message_rows(main.store()), ROUNDS + 1);
    assert_eq!(message_rows(child.store()), ROUNDS + 1);
    // Whatever a deferred pass left is collected once both are quiet.
    let mut main = main;
    main.store_mut()
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    let mut live = closure(&fixture, main.head().sealed_root);
    for root in [
        child.head().sealed_root,
        base_root(&fixture, branch_id(2)),
        base_root(&fixture, fixture.main),
    ] {
        live.extend(closure(&fixture, root));
    }
    assert_eq!(object_names(&fixture), live);
    drop(main);
    drop(child);
    let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    let child = fixture.admit(branch_id(2), DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(message_rows(main.store()), ROUNDS + 1);
    assert_eq!(message_rows(child.store()), ROUNDS + 1);
}

/// A damaged object reachable from a retained root is live-data damage, not
/// backlog: the automatic pass removes nothing and names the root under its
/// own reason code until the object is repaired.
#[test]
fn damaged_live_object_is_reported_as_integrity_not_backlog() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    let base = base_root(&fixture, fixture.main);
    assert_ne!(base, main.head().sealed_root);
    let objects = ImmutableObjectStore::open(&fixture.objects).unwrap();
    let base_path = objects.object_path(base);
    let original = fs::read(&base_path).unwrap();
    let mut damaged = original.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 0xff;
    fs::write(&base_path, &damaged).unwrap();

    let superseded = main.head().sealed_root;
    write_row(&mut main, "after-damage");
    checkpoint(&mut main).unwrap();
    assert!(object_names(&fixture)
        .contains(&("sealed-root".to_string(), superseded.sha256.to_string())));
    let pressure = main.store().storage_pressure_snapshot(None);
    assert_eq!(pressure.object_reclamation_damaged_live_root, Some(base));
    assert!(pressure.object_reclamation_retry_required);
    assert!(pressure
        .reason_codes
        .contains(&crate::pressure::StoragePressureReasonCode::ObjectReclamationLiveObjectDamaged));

    fs::write(&base_path, &original).unwrap();
    write_row(&mut main, "after-repair");
    checkpoint(&mut main).unwrap();
    assert_no_object_debt(main.store(), "after repair");
    assert!(main
        .store()
        .storage_pressure_snapshot(None)
        .object_reclamation_damaged_live_root
        .is_none());
    assert!(!object_names(&fixture)
        .contains(&("sealed-root".to_string(), superseded.sha256.to_string())));
    assert_eq!(message_rows(main.store()), 3);
}

/// Garbage this process just published is removed without re-hashing, so a
/// tiny byte budget still removes it; only re-hashed bytes are budgeted.
#[test]
fn same_process_garbage_is_not_rehashed_against_the_byte_budget() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    let reader = main.store().snapshot_for_read();
    let readers = (0..3)
        .map(|round| {
            write_row(&mut main, &format!("fresh-{round}-{}", "q".repeat(8192)));
            checkpoint(&mut main).unwrap();
            main.store().snapshot_for_read()
        })
        .collect::<Vec<_>>();
    drop(reader);
    drop(readers);
    let report = main
        .store_mut()
        .reclaim_branch_storage(BranchReclamationLimits {
            max_bytes: 1,
            ..Default::default()
        })
        .unwrap();
    // Streamed checkpoint artifacts carry publication records; only small
    // buffered objects (roots, manifests) without one wait for a later pass.
    assert!(report.reclaimed_objects > 1, "{report:?}");
    assert!(
        report.pending_bytes * 4 < report.reclaimed_bytes,
        "{report:?}"
    );
    main.store_mut()
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .unwrap();
    let mut live = closure(&fixture, main.head().sealed_root);
    live.extend(closure(&fixture, base_root(&fixture, fixture.main)));
    assert_eq!(object_names(&fixture), live);
}
