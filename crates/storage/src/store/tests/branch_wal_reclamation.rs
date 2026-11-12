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

//! Reclamation of superseded branch-private WAL files (#1727).

use super::*;
use crate::store::{
    set_branch_wal_sweep_failpoint, set_checkpoint_failpoint,
    set_generation_reclamation_remove_failpoint, BranchWalSweepFailpoint, CheckpointPublishStage,
};

/// Generations of every `wal.<generation>.hawdb` directly beside a head.
fn branch_wal_generations(directory: &Path) -> Vec<u64> {
    let mut generations = fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name();
            crate::artifact_files::parse_generation_file(name.to_str()?, "wal.")
        })
        .collect::<Vec<_>>();
    generations.sort_unstable();
    generations
}

fn branch_directory(fixture: &BranchFixture, id: branch_catalog::BranchId) -> PathBuf {
    fixture.head_path(id).parent().unwrap().to_path_buf()
}

fn checkpoint(branch: &mut AdmittedBranchStore) -> Result<()> {
    let (store, catalog) = branch.store_and_catalog_mut();
    store.checkpoint(catalog)
}

fn assert_no_reclamation_debt(branch: &AdmittedBranchStore) {
    let pressure = branch.store().storage_pressure_snapshot(None);
    assert!(!pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 0);
    assert_eq!(pressure.generation_reclamation_pending_bytes, 0);
}

fn message_rows(branch: &AdmittedBranchStore) -> usize {
    branch.store().relational_state().row_count("messages")
}

#[test]
fn branch_checkpoints_reclaim_superseded_private_wals_while_forks_keep_reading() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let fixture = BranchFixture::new();
        let mut main = fixture.admit(fixture.main, durability);
        write_schema_and_graph(&mut main);
        let epoch = main.store().commit_epoch();
        main.store_mut().seal_admitted_branch(epoch).unwrap();
        fixture.fork(&main, branch_id(2), "child");
        let child_directory = branch_directory(&fixture, branch_id(2));
        let child_wals = branch_wal_generations(&child_directory);
        assert_eq!(child_wals.len(), 1);

        let main_directory = branch_directory(&fixture, fixture.main);
        for round in 0..6 {
            let superseded = main.head().active_wal.generation;
            write_row(&mut main, &format!("main-{round}-{}", "x".repeat(4096)));
            checkpoint(&mut main).unwrap();
            let selected = main.head().active_wal.generation;
            assert!(selected > superseded);
            // Only the WAL selected by the durable head survives; the sealed
            // predecessor (including the pre-fork seal) is reclaimed.
            assert_eq!(
                branch_wal_generations(&main_directory),
                vec![selected],
                "{durability:?} round {round}"
            );
            assert_no_reclamation_debt(&main);
            // A parent checkpoint never touches another branch directory.
            assert_eq!(branch_wal_generations(&child_directory), child_wals);
        }
        assert_eq!(message_rows(&main), 7);

        // The child was created from the sealed parent root. It recovers from
        // immutable objects and its own WAL only, after every parent WAL that
        // existed at fork time is gone.
        let mut child = fixture.admit(branch_id(2), durability);
        assert_eq!(message_rows(&child), 1);
        assert_eq!(child.store().node_count_for_label(None), 3);
        write_row(&mut child, "child-only");
        checkpoint(&mut child).unwrap();
        write_row(&mut child, "child-after-checkpoint");
        assert_eq!(
            branch_wal_generations(&child_directory),
            vec![child.head().active_wal.generation]
        );
        assert_no_reclamation_debt(&child);
        drop(child);
        let child = fixture.admit(branch_id(2), durability);
        assert_eq!(message_rows(&child), 3);
        drop(child);

        drop(main);
        let main = fixture.admit(fixture.main, durability);
        assert_eq!(message_rows(&main), 7);
        assert_eq!(main.store().node_count_for_label(None), 3);
    }
}

#[test]
fn failed_branch_wal_removal_is_retryable_reclamation_debt() {
    let fixture = BranchFixture::new();
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();
    write_row(&mut main, &"debt".repeat(4096));
    let directory = branch_directory(&fixture, fixture.main);
    let superseded = main.head().active_wal.generation;
    let superseded_path = directory.join(crate::artifact_files::wal_generation_file(superseded));
    let superseded_bytes = fs::metadata(&superseded_path).unwrap().len();

    set_generation_reclamation_remove_failpoint(Some(crate::artifact_files::wal_generation_file(
        superseded,
    )));
    let result = checkpoint(&mut main);
    set_generation_reclamation_remove_failpoint(None);
    // Reclamation is post-commit maintenance: the checkpoint itself succeeded.
    result.unwrap();
    assert!(superseded_path.exists());
    let pressure = main.store().storage_pressure_snapshot(None);
    assert!(pressure.generation_reclamation_retry_required);
    assert_eq!(pressure.generation_reclamation_pending_files, 1);
    assert_eq!(
        pressure.generation_reclamation_pending_bytes,
        superseded_bytes
    );

    write_row(&mut main, "retry");
    checkpoint(&mut main).unwrap();
    assert!(!superseded_path.exists());
    assert_eq!(
        branch_wal_generations(&directory),
        vec![main.head().active_wal.generation]
    );
    assert_no_reclamation_debt(&main);
    drop(main);
    let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(message_rows(&main), 3);
}

#[test]
fn failed_head_reread_or_directory_sync_deletes_nothing_and_is_retryable_debt() {
    for failpoint in [
        BranchWalSweepFailpoint::HeadRead,
        BranchWalSweepFailpoint::DirectorySync,
    ] {
        let fixture = BranchFixture::new();
        let directory = branch_directory(&fixture, fixture.main);
        let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
        write_schema_and_graph(&mut main);
        checkpoint(&mut main).unwrap();
        write_row(&mut main, &"barrier".repeat(4096));
        let superseded = main.head().active_wal.generation;
        let superseded_path =
            directory.join(crate::artifact_files::wal_generation_file(superseded));
        let superseded_bytes = fs::metadata(&superseded_path).unwrap().len();

        // The sweep cannot confirm or make durable the selecting head, so it
        // must not remove anything that head superseded.
        set_branch_wal_sweep_failpoint(Some(failpoint));
        let result = checkpoint(&mut main);
        set_branch_wal_sweep_failpoint(None);
        result.unwrap();
        assert!(superseded_path.exists(), "{failpoint:?}");
        assert_eq!(
            branch_wal_generations(&directory),
            vec![superseded, main.head().active_wal.generation],
            "{failpoint:?}"
        );
        let pressure = main.store().storage_pressure_snapshot(None);
        assert!(pressure.generation_reclamation_retry_required);
        assert_eq!(pressure.generation_reclamation_pending_files, 1);
        assert_eq!(
            pressure.generation_reclamation_pending_bytes,
            superseded_bytes
        );

        write_row(&mut main, "retry");
        checkpoint(&mut main).unwrap();
        assert!(!superseded_path.exists(), "{failpoint:?}");
        assert_eq!(
            branch_wal_generations(&directory),
            vec![main.head().active_wal.generation]
        );
        assert_no_reclamation_debt(&main);
        drop(main);
        let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
        assert_eq!(message_rows(&main), 3);
    }
}

#[test]
fn interrupted_checkpoint_keeps_selected_wal_and_a_later_checkpoint_reclaims_leftovers() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        let fixture = BranchFixture::new();
        let directory = branch_directory(&fixture, fixture.main);
        let mut main = fixture.admit(fixture.main, durability);
        write_schema_and_graph(&mut main);
        checkpoint(&mut main).unwrap();

        // Interrupted before the head selects a successor: the old WAL is
        // still the selected recovery input and must survive.
        write_row(&mut main, "before-head");
        let selected = main.head().active_wal.generation;
        set_checkpoint_failpoint(Some(CheckpointPublishStage::ManifestPublished));
        let result = checkpoint(&mut main);
        set_checkpoint_failpoint(None);
        assert!(result.is_err());
        drop(main);
        assert!(directory
            .join(crate::artifact_files::wal_generation_file(selected))
            .exists());
        let mut main = fixture.admit(fixture.main, durability);
        assert_eq!(main.head().active_wal.generation, selected);
        assert_eq!(message_rows(&main), 2);

        // Interrupted after the head selected a successor but before the
        // sweep: the superseded WAL is left behind, recovery uses only the
        // new head, and the next successful checkpoint reclaims the leftover.
        write_row(&mut main, "after-head");
        let superseded = main.head().active_wal.generation;
        set_checkpoint_failpoint(Some(CheckpointPublishStage::BranchHeadPublished));
        let result = checkpoint(&mut main);
        set_checkpoint_failpoint(None);
        assert!(result.is_err());
        drop(main);
        let leftover = directory.join(crate::artifact_files::wal_generation_file(superseded));
        assert!(leftover.exists());
        let mut main = fixture.admit(fixture.main, durability);
        assert!(main.head().active_wal.generation > superseded);
        assert_eq!(message_rows(&main), 3);
        write_row(&mut main, "reclaim");
        checkpoint(&mut main).unwrap();
        assert!(!leftover.exists());
        assert_eq!(
            branch_wal_generations(&directory),
            vec![main.head().active_wal.generation]
        );
        assert_no_reclamation_debt(&main);
        drop(main);
        let main = fixture.admit(fixture.main, durability);
        assert_eq!(message_rows(&main), 4);
    }
}

#[test]
fn branch_wal_reclamation_keeps_newer_names_and_reader_pinned_generations() {
    let fixture = BranchFixture::new();
    let directory = branch_directory(&fixture, fixture.main);
    let mut main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    write_schema_and_graph(&mut main);
    checkpoint(&mut main).unwrap();

    // A name above the selected generation may be a prepared successor or
    // interrupted-publication evidence; no sweep may remove it.
    let evidence_generation = main.head().active_wal.generation + 16;
    let evidence = directory.join(crate::artifact_files::wal_generation_file(
        evidence_generation,
    ));
    fs::write(&evidence, b"interrupted publication evidence").unwrap();

    // A reader-pinned generation is retained until the pin is released.
    write_row(&mut main, "pinned");
    let pinned = main.head().active_wal.generation;
    let (store, catalog) = main.store_and_catalog_mut();
    let prepared = store.prepare_checkpoint(catalog).unwrap().unwrap();
    store
        .publish_prepared_checkpoint_with_reader_generations(
            prepared,
            Some(store.commit_epoch()),
            &BTreeSet::from([pinned]),
            None,
        )
        .unwrap();
    let selected = main.head().active_wal.generation;
    assert!(selected < evidence_generation);
    assert_eq!(
        branch_wal_generations(&directory),
        vec![pinned, selected, evidence_generation]
    );

    write_row(&mut main, "released");
    checkpoint(&mut main).unwrap();
    let selected = main.head().active_wal.generation;
    assert!(selected < evidence_generation);
    assert_eq!(
        branch_wal_generations(&directory),
        vec![selected, evidence_generation]
    );
    assert_eq!(
        fs::read(&evidence).unwrap(),
        b"interrupted publication evidence"
    );
    assert_no_reclamation_debt(&main);
    drop(main);
    let main = fixture.admit(fixture.main, DurabilityPolicy::SyncOnEveryWrite);
    assert_eq!(message_rows(&main), 3);
}
