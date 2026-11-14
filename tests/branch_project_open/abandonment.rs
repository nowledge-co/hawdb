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

use super::*;
use hawdb::{BranchAbandonmentLimits, BranchReclamationLimits};
use hawdb_storage::branch_catalog::{self, BranchState, CreateOutcome};
use hawdb_storage::branch_head::read_branch_head;
use hawdb_storage::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
use std::path::Path;

fn damaged_pending(project: &Project, durability: DurabilityPolicy) -> (Database, hawdb::Uuid) {
    let mut writer = Database::open_with_durability(&project.0, durability).unwrap();
    writer.query("CREATE (:Memory {id: 'parent'})").unwrap();
    let child = pending_children(&project.0, &mut writer, 1)[0];
    writer.query("CREATE (:Memory {id: 'later'})").unwrap();
    writer.checkpoint().unwrap();
    (writer, child)
}

fn child_head(project: &Project, child: hawdb::Uuid) -> PathBuf {
    project
        .0
        .join("branches")
        .join(child.to_string())
        .join("branch.head")
}

fn orphan(project: &Project) -> PathBuf {
    let mut objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let reference =
        ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, b"forensic orphan");
    objects.publish(reference, b"forensic orphan").unwrap();
    objects.object_path(reference)
}

fn archived_file(report: &hawdb::BranchAbandonmentReport, relative: &str) -> PathBuf {
    let envelope: serde_json::Value = serde_json::from_slice(
        &std::fs::read(report.quarantine_directory.join("intent.json")).unwrap(),
    )
    .unwrap();
    let index = envelope["manifest"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .position(|file| file["path"] == relative)
        .unwrap();
    report.quarantine_directory.join(format!("file-{index}"))
}

#[test]
fn quarantine_evidence_uses_an_independent_retryable_budget() {
    let project = Project::new();
    let (writer, child) = damaged_pending(&project, DurabilityPolicy::default());
    std::fs::write(child_head(&project, child), b"damaged pending head").unwrap();
    let orphan_path = orphan(&project);
    drop(writer);
    let mut database = Database::open(&project.0).unwrap();
    let plan = database
        .inspect_pending_branch_abandonment(child, Default::default())
        .unwrap();
    database
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap();
    fn bytes(directory: &Path) -> u64 {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }
    let object_bytes = bytes(&project.0.join("branches/objects/objects"));
    assert!(plan.evidence_bytes() > object_bytes);
    let mut limits = BranchReclamationLimits {
        max_bytes: object_bytes,
        evidence_limits: BranchAbandonmentLimits {
            max_bytes: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let error = database.reclaim_branch_storage(limits).unwrap_err();
    assert!(matches!(
        error,
        HawDBError::BranchEvidenceBudgetExceeded {
            resource: "bytes",
            limit: 1,
            ..
        }
    ));
    assert!(orphan_path.exists());
    assert!(child_head(&project, child).exists());
    limits.evidence_limits = Default::default();
    let report = database.reclaim_branch_storage(limits).unwrap();
    assert!(!report.deferred_for_active_leases);
    assert!(!orphan_path.exists());
    assert!(!child_head(&project, child).exists());
    assert_eq!(values(&mut database).len(), 2);
}

#[test]
fn abandonment_preserves_evidence_and_replays_after_reclamation() {
    for durability in [
        DurabilityPolicy::SyncOnEveryWrite,
        DurabilityPolicy::SyncOnCheckpoint,
    ] {
        for damage in ["head", "root", "missing-root", "orphan"] {
            let project = Project::new();
            let (writer, child) = damaged_pending(&project, durability);
            let head_path = child_head(&project, child);
            let head = read_branch_head(&head_path).unwrap();
            let store = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
            let orphan_path = orphan(&project);
            let damaged_path = match damage {
                "head" => head_path.clone(),
                "orphan" => orphan_path.clone(),
                _ => store.object_path(head.sealed_root),
            };
            if damage == "missing-root" {
                std::fs::remove_file(&damaged_path).unwrap();
            } else {
                std::fs::write(&damaged_path, b"damaged evidence").unwrap();
            }
            if damage == "orphan" {
                // An unrelated orphan does not prevent normal create recovery.
                // Keep this fixture's child pending through its own bad head.
                std::fs::write(&head_path, b"damaged pending head").unwrap();
            }
            drop(writer);
            let mut metadata = Database::open_with_durability(&project.0, durability).unwrap();
            assert_no_runtime(&metadata);
            let plan = metadata
                .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
                .unwrap();
            assert_eq!(plan.branch_id(), child);
            assert!(plan.evidence_files() > 2);
            assert!(plan.evidence_bytes() > 16);
            let acknowledgement = plan.acknowledge_potential_data_loss();
            let report = metadata
                .abandon_pending_branch(acknowledgement.clone())
                .unwrap();
            assert!(!report.replayed);
            assert_eq!(report.metadata_revision, plan.metadata_revision() + 1);
            assert_no_runtime(&metadata);
            if damage != "missing-root" {
                let relative = damaged_path
                    .strip_prefix(project.0.join("branches"))
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/");
                assert_eq!(
                    std::fs::read(archived_file(&report, &relative)).unwrap(),
                    b"damaged evidence"
                );
            }
            let catalog =
                branch_catalog::read_catalog(&project.0.join("branches/catalog.hawdb")).unwrap();
            let record = catalog
                .branches
                .iter()
                .find(|record| record.id.as_uuid() == child)
                .unwrap();
            assert_eq!(record.state, BranchState::Deleted);
            assert_eq!(record.create_outcome, CreateOutcome::Abandoned);
            let gc = metadata
                .reclaim_branch_storage(BranchReclamationLimits::default())
                .unwrap();
            assert_eq!(gc.reclaimed_branch_directories, 1);
            assert!(!orphan_path.exists());
            assert!(!head_path.exists());
            assert_eq!(values(&mut metadata).len(), 2);
            drop(metadata);
            let mut reopened = Database::open_with_durability(&project.0, durability).unwrap();
            let replay = reopened.abandon_pending_branch(acknowledgement).unwrap();
            assert!(replay.replayed);
            assert_eq!(replay.catalog_revision, report.catalog_revision);
            assert_eq!(replay.quarantine_directory, report.quarantine_directory);
            let fresh = reopened
                .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
                .unwrap();
            assert!(
                reopened
                    .abandon_pending_branch(fresh.acknowledge_potential_data_loss())
                    .unwrap()
                    .replayed
            );
        }
    }
}

#[test]
fn abandonment_rejects_stale_ownership_and_changed_sources_before_writing_intent() {
    let project = Project::new();
    let (writer, child) = damaged_pending(&project, DurabilityPolicy::SyncOnEveryWrite);
    std::fs::write(child_head(&project, child), b"damaged head").unwrap();
    let metadata = Database::open(&project.0).unwrap();
    assert!(metadata
        .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
        .is_err());
    drop(writer);
    let mut metadata = metadata;
    let limits = BranchAbandonmentLimits::default();
    assert!(metadata
        .inspect_pending_branch_abandonment(
            child,
            BranchAbandonmentLimits {
                max_files: 1,
                ..limits
            }
        )
        .is_err());
    assert!(metadata
        .inspect_pending_branch_abandonment(
            child,
            BranchAbandonmentLimits {
                max_bytes: 1,
                ..limits
            }
        )
        .is_err());
    let plan = metadata
        .inspect_pending_branch_abandonment(child, limits)
        .unwrap();
    let before = std::fs::read(project.0.join("branches/catalog.hawdb")).unwrap();
    let lease = hawdb_storage::ownership::DatabaseDirectoryLease::acquire(
        child_head(&project, child).parent().unwrap(),
    )
    .unwrap();
    assert!(metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .is_err());
    drop(lease);
    std::fs::write(child_head(&project, child), b"changed head").unwrap();
    assert!(metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .is_err());
    assert_eq!(
        std::fs::read(project.0.join("branches/catalog.hawdb")).unwrap(),
        before
    );
    assert!(!project.0.join("branches/quarantine").exists());
    std::fs::write(child_head(&project, child), b"damaged head").unwrap();
    let mut catalog = branch_catalog::Catalog::decode(&before).unwrap();
    catalog.revision += 1;
    branch_catalog::write_catalog(&project.0.join("branches/catalog.hawdb"), &catalog).unwrap();
    assert!(metadata
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .is_err());
    assert!(!project.0.join("branches/quarantine").exists());
    let fresh = metadata
        .inspect_pending_branch_abandonment(child, limits)
        .unwrap();
    metadata
        .abandon_pending_branch(fresh.acknowledge_potential_data_loss())
        .unwrap();
}

#[test]
fn abandonment_damaged_quarantine_blocks_complete_sweep() {
    for damage in [
        "intent", "receipt", "copy", "source", "private", "extra", "archive",
    ] {
        let project = Project::new();
        let (writer, child) = damaged_pending(&project, DurabilityPolicy::SyncOnEveryWrite);
        std::fs::write(child_head(&project, child), b"damaged head").unwrap();
        let orphan_path = orphan(&project);
        std::fs::write(&orphan_path, b"broken orphan").unwrap();
        drop(writer);
        let mut database = Database::open(&project.0).unwrap();
        let plan = database
            .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
            .unwrap();
        let report = database
            .abandon_pending_branch(plan.acknowledge_potential_data_loss())
            .unwrap();
        if damage == "archive" {
            let saved = project.0.join("saved-quarantine");
            std::fs::rename(&report.quarantine_directory, &saved).unwrap();
            assert!(database
                .reclaim_branch_storage(BranchReclamationLimits::default())
                .is_err());
            assert!(orphan_path.exists());
            assert!(child_head(&project, child).exists());
            std::fs::rename(saved, &report.quarantine_directory).unwrap();
        }
        let affected = match damage {
            "intent" => report.quarantine_directory.join("intent.json"),
            "receipt" => report.quarantine_directory.join("committed"),
            "copy" => archived_file(&report, &format!("{child}/branch.head")),
            "private" => child_head(&project, child),
            "extra" => {
                let path = child_head(&project, child).with_file_name("unarchived-evidence");
                std::fs::write(&path, b"new evidence").unwrap();
                path
            }
            _ => orphan_path.clone(),
        };
        let original = std::fs::read(&affected).unwrap();
        if damage == "intent" || damage == "receipt" {
            std::fs::remove_file(&affected).unwrap();
        } else {
            std::fs::write(&affected, b"evidence mismatch").unwrap();
        }
        assert!(
            database
                .reclaim_branch_storage(BranchReclamationLimits::default())
                .is_err(),
            "{damage}"
        );
        assert!(orphan_path.exists());
        assert!(child_head(&project, child).exists());
        if damage == "extra" {
            std::fs::remove_file(&affected).unwrap();
        } else {
            std::fs::write(&affected, original).unwrap();
        }
        assert_eq!(
            database
                .reclaim_branch_storage(BranchReclamationLimits::default())
                .unwrap()
                .reclaimed_branch_directories,
            1
        );
    }
}

#[test]
fn abandonment_never_authorizes_reclaiming_corrupt_live_dependencies() {
    let project = Project::new();
    let (writer, child) = damaged_pending(&project, DurabilityPolicy::SyncOnEveryWrite);
    std::fs::write(child_head(&project, child), b"damaged head").unwrap();
    let main = main_id(&writer);
    let head = read_branch_head(&child_head(&project, main)).unwrap();
    let objects = ImmutableObjectStore::open(project.0.join("branches/objects")).unwrap();
    let live = objects.object_path(head.sealed_root);
    let original = std::fs::read(&live).unwrap();
    std::fs::write(&live, b"broken live root").unwrap();
    let orphan_path = orphan(&project);
    drop(writer);
    let mut database = Database::open(&project.0).unwrap();
    let plan = database
        .inspect_pending_branch_abandonment(child, BranchAbandonmentLimits::default())
        .unwrap();
    database
        .abandon_pending_branch(plan.acknowledge_potential_data_loss())
        .unwrap();
    // Admit the healthy owner, then restore the archived corruption. This
    // exercises the complete live-root GC mark rather than admission failure.
    std::fs::write(&live, &original).unwrap();
    assert_eq!(values(&mut database).len(), 2);
    std::fs::write(&live, b"broken live root").unwrap();
    assert!(database
        .reclaim_branch_storage(BranchReclamationLimits::default())
        .is_err());
    assert!(orphan_path.exists());
    assert!(child_head(&project, child).exists());
    std::fs::write(live, original).unwrap();
    drop(database);
    let mut database = Database::open(&project.0).unwrap();
    assert_eq!(
        database
            .reclaim_branch_storage(BranchReclamationLimits::default())
            .unwrap()
            .reclaimed_branch_directories,
        1
    );
}
