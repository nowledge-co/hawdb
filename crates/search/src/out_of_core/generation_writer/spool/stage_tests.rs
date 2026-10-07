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
use crate::build_memory::directory;
use crate::test_allocation as allocation;
use hawdb_core::RuntimeMemoryReservation;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = crate::out_of_core::generation_writer::tests::test_dir("stage_cleanup");
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn foreign_root_retries_do_not_reset_bounded_cleanup_progress() {
    const CHILD: &str = "HAWDB_ROOT_CLEANUP_PROGRESS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::foreign_root_retries_do_not_reset_bounded_cleanup_progress"
                )
                .strip_prefix("hawdb_search::")
                .unwrap(),
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }

    fn retain(root: &Path, count: usize) -> Vec<PathBuf> {
        let mut writers = Vec::new();
        let mut paths = Vec::new();
        for index in 0..count {
            let mut writer = crate::SearchOutOfCoreGenerationWriter::create_with_context(
                root,
                Default::default(),
                RuntimeTaskContext::default()
                    .with_memory_reservation(RuntimeMemoryReservation::new(16 * 1024 * 1024, 0)),
            )
            .unwrap();
            writer
                .push(crate::SearchDocument {
                    id: index.to_string(),
                    title: String::new(),
                    content: "retained cleanup evidence".into(),
                    embedding: None,
                    metadata: Default::default(),
                })
                .unwrap();
            let path = fs::read_dir(root)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| {
                    !paths.contains(path)
                        && path
                            .extension()
                            .is_some_and(|extension| extension == "stage")
                })
                .unwrap();
            let unexpected = path.join("unexpected");
            fs::create_dir(&unexpected).unwrap();
            fs::write(unexpected.join("evidence"), b"retained").unwrap();
            paths.push(path);
            // Keep every stage active until the root's inventory is complete,
            // so automatic retries cannot advance its initial ordering.
            writers.push(writer);
        }
        drop(writers);
        paths
    }

    let fixture = Fixture::new();
    let root = fixture.0.join("root");
    let foreign = fixture.0.join("foreign");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&foreign).unwrap();
    let paths = retain(&root, 8);
    let foreign_paths = retain(&foreign, 1);
    let first = crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 4).unwrap();
    assert_eq!(first.attempted_stages, 4);
    assert_eq!(first.pending_stages, 8);
    assert_eq!(first.blocked_stages, 8);
    assert_eq!(first.removed_stages, 0);

    // Only the fixture owns these injections. The first four remain blocked.
    for path in &paths[4..] {
        fs::remove_file(path.join("unexpected/evidence")).unwrap();
        fs::remove_dir(path.join("unexpected")).unwrap();
    }
    for _ in 0..3 {
        let report =
            crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&foreign, 1).unwrap();
        assert_eq!(report.attempted_stages, 1);
        assert_eq!(report.pending_stages, 1);
        assert_eq!(report.blocked_stages, 1);
        assert_eq!(report.removed_stages, 0);
    }
    let resumed = crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 4).unwrap();
    assert_eq!(resumed.attempted_stages, 4);
    assert_eq!(resumed.removed_stages, 4);
    assert_eq!(resumed.pending_stages, 4);
    for path in &paths[4..] {
        assert!(!path.exists());
    }
    for path in paths[..4].iter().chain(&foreign_paths) {
        assert_eq!(
            fs::read(path.join("unexpected/evidence")).unwrap(),
            b"retained"
        );
        fs::remove_file(path.join("unexpected/evidence")).unwrap();
        fs::remove_dir(path.join("unexpected")).unwrap();
    }
    for (root, count) in [(&root, 4), (&foreign, 1)] {
        let report =
            crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(root, count).unwrap();
        assert_eq!(report.removed_stages, count);
        assert_eq!(report.pending_stages, 0);
        assert_eq!(report.reserved_disk_bytes, 0);
        assert_eq!(report.retained_memory_bytes, 0);
    }
}

#[test]
fn retained_stages_do_not_exhaust_an_unrelated_root() {
    const CHILD: &str = "HAWDB_ROOT_CLEANUP_ISOLATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::retained_stages_do_not_exhaust_an_unrelated_root"
                )
                .strip_prefix("hawdb_search::")
                .unwrap(),
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
        return;
    }
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let blocked = fixture.0.join("blocked");
    let independent = fixture.0.join("independent");
    fs::create_dir(&blocked).unwrap();
    fs::create_dir(&independent).unwrap();
    let project = ProjectFileDescriptors::acquire_existing(&blocked, 4).unwrap();
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 * 1024 * 1024),
            background_task_limit: std::num::NonZeroUsize::new(1),
            ..Default::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    let request = hawdb_qos::RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024);
    let mut evidence = Vec::new();
    for index in 0_u32..256 {
        let before = fs::read_dir(&blocked)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<std::collections::BTreeSet<_>>();
        let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
        let mut writer = admission
            .create_writer(&blocked, Default::default())
            .unwrap();
        writer
            .writer_mut()
            .push(crate::SearchDocument {
                id: index.to_string(),
                title: String::new(),
                content: "unpublished evidence".into(),
                embedding: None,
                metadata: Default::default(),
            })
            .unwrap();
        let path = fs::read_dir(&blocked)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                !before.contains(path)
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "stage")
            })
            .unwrap();
        let unexpected = path.join("unexpected");
        fs::create_dir(&unexpected).unwrap();
        fs::write(unexpected.join("evidence"), index.to_le_bytes()).unwrap();
        drop(writer);
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        evidence.push(unexpected);
    }
    drop(project);
    let debt = crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&blocked, 0).unwrap();
    assert_eq!(debt.pending_stages, 256);
    assert_eq!(debt.blocked_stages, 256);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        debt.retained_memory_bytes as u64
    );
    let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let error = admission
        .create_writer(&blocked, Default::default())
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("cleanup owner capacity exhausted"));
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        debt.retained_memory_bytes as u64
    );
    #[cfg(unix)]
    {
        let alias = fixture.0.join("alias");
        std::os::unix::fs::symlink(&blocked, &alias).unwrap();
        let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
        let error = admission
            .create_writer(&alias, Default::default())
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("cleanup owner capacity exhausted"));
    }
    assert_eq!(
        fs::read_dir(&blocked)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "stage")
            })
            .count(),
        256
    );
    let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let mut writer = admission
        .create_writer(&independent, Default::default())
        .expect("retained stages in another root must not prevent generation admission");
    writer
        .writer_mut()
        .push(crate::SearchDocument {
            id: "published".into(),
            title: String::new(),
            content: "independent generation".into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    writer.finish().unwrap();
    assert_eq!(
        crate::SearchOutOfCoreReader::open(&independent)
            .unwrap()
            .document_count(),
        1
    );
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        debt.retained_memory_bytes as u64
    );
    for (index, path) in evidence.iter().enumerate() {
        assert_eq!(
            fs::read(path.join("evidence")).unwrap(),
            u32::try_from(index).unwrap().to_le_bytes()
        );
    }
    let reopened = ProjectFileDescriptors::acquire_existing(&blocked, 8).unwrap();
    // Only the test owns these injected directories; production cleanup retains them.
    for path in evidence {
        fs::remove_file(path.join("evidence")).unwrap();
        fs::remove_dir(path).unwrap();
    }
    let removed =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&blocked, 256).unwrap();
    assert_eq!(removed.removed_stages, 256);
    assert_eq!(removed.pending_stages, 0);
    assert_eq!(removed.retained_memory_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(reopened.metrics().open, 0);
}

#[test]
fn retained_host_memory_denial_leaves_no_private_stage() {
    let fixture = Fixture::new();
    let budget = 16 * 1024 * 1024;
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(budget),
            ..Default::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    let admission = crate::SearchGenerationAdmission::acquire(
        &governor,
        hawdb_qos::RuntimeWorkRequest::background_maintenance(budget),
    )
    .unwrap();
    let error = admission
        .create_writer(&fixture.0, Default::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory_saturated"), "{error}");
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
}

#[test]
fn pending_cleanup_releases_work_admission_and_retries_on_the_next_create() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 1).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let request = RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024);
    let permit = std::sync::Arc::new(governor.try_admit(request).unwrap());
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let mut memory = BuildMemory::new(&task).unwrap();
    memory.host_admission = Some(permit.clone());
    let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
    let path = stage.path.to_path_buf();
    let held = File::create(fixture.0.join("held")).unwrap();
    drop(stage);
    drop(memory);
    drop(permit);
    drop(held);
    let pending =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 0).unwrap();
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        pending.retained_memory_bytes as u64
    );
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(pending.retained_memory_bytes > 0);
    assert!(path.exists());

    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let next = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
    assert!(!path.exists());
    let report =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 0).unwrap();
    assert_eq!(report.pending_stages, 0);
    drop(next);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn pending_cleanup_does_not_pin_the_previous_project_descriptor_limit() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 1).unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
    let path = stage.path.to_path_buf();
    let held = File::create(fixture.0.join("held")).unwrap();
    drop(stage);
    drop(memory);
    drop(held);
    drop(project);
    let reopened = ProjectFileDescriptors::acquire_existing(&fixture.0, 3).unwrap();
    let report =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1).unwrap();
    assert_eq!(report.removed_stages, 1);
    assert!(!path.exists());
    assert_eq!(reopened.metrics().open, 0);
}

#[test]
fn stage_cleanup_admission_precedes_creation_and_keeps_the_parent() {
    let fixture = Fixture::new();
    let bytes = directory::stage_removal_bytes(&fixture.0).unwrap();
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64 - 1, 0));
    let memory = BuildMemory::new(&task).unwrap();
    assert!(StageDirectory::create(&fixture.0, &memory, &task).is_err());
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn stage_cleanup_retains_workspace_through_full_root_cancellation_and_unwind() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    for unwind in [false, true] {
        let fixture = Fixture::new();
        let budget = 8 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
        let path = stage.path.to_path_buf();
        for index in 0..129 {
            fs::write(path.join(format!("generated-{index}.hawdb")), b"partial").unwrap();
        }
        let reserved = memory.ledger.snapshot().used_bytes;
        let held = memory.input.reserve(budget as usize - reserved).unwrap();
        task.cancellation().cancel();
        if unwind {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _stage = stage;
                panic!("injected writer unwind");
            }));
            assert!(result.is_err());
        } else {
            let ((), peak) = allocation::measure(|| drop(stage));
            assert!(peak <= reserved, "requested {peak}, reserved {reserved}");
            assert_eq!(allocation::live(), 0);
        }
        assert!(!path.exists());
        assert!(fixture.0.exists());
        assert_eq!(memory.ledger.snapshot().used_bytes, held.bytes());
        drop(held);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[cfg(unix)]
#[test]
fn native_stage_cleanup_unlinks_symlinks_without_following_their_targets() {
    let fixture = Fixture::new();
    let outside = fixture.0.join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("retained"), b"outside").unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let stage = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
    let path = stage.path.to_path_buf();
    std::os::unix::fs::symlink(&outside, path.join("link")).unwrap();
    drop(stage);
    assert!(!path.exists());
    assert_eq!(fs::read(outside.join("retained")).unwrap(), b"outside");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn descriptor_denial_retains_only_cleanup_metadata_until_retry() {
    use crate::SearchGenerationAdmission;
    use hawdb_qos::IoConcurrencyBudget;
    use hawdb_qos::{RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest};
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 4).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let request = RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024);
    let admission = SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let mut writer = admission
        .create_writer(&fixture.0, Default::default())
        .unwrap();
    writer
        .writer_mut()
        .push(crate::SearchDocument {
            id: "a".into(),
            title: String::new(),
            content: "retained bytes".into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    stage::evidence::install(fixture.0.clone(), start_tx, ready_rx);
    let root = fixture.0.clone();
    let competitor = std::thread::spawn(move || {
        start_rx.recv().unwrap();
        let held = (0..4)
            .map(|index| File::create(root.join(format!("competitor-{index}"))).unwrap())
            .collect::<Vec<_>>();
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        drop(held);
    });
    // Exhaust the real project after the scan closes, immediately before unlink.
    drop(writer);
    let report =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 0).unwrap();
    assert_eq!(report.pending_stages, 1);
    assert!(report.reserved_disk_bytes > 0);
    assert!(report.retained_memory_bytes > 0);
    assert!(report.retained_memory_bytes < 8192);
    assert!(matches!(
        report.descriptor_error,
        Some(hawdb_core::error::FileDescriptorError::BudgetExceeded { .. })
    ));
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        report.retained_memory_bytes as u64
    );
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    let denied =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1).unwrap();
    assert_eq!(denied.attempted_stages, 1);
    assert_eq!(denied.pending_stages, 1);
    assert_eq!(denied.reserved_disk_bytes, report.reserved_disk_bytes);
    release_tx.send(()).unwrap();
    competitor.join().unwrap();
    let released =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1).unwrap();
    assert_eq!(released.removed_stages, 1);
    assert_eq!(released.pending_stages, 0);
    assert_eq!(released.reserved_disk_bytes, 0);
    assert_eq!(released.retained_memory_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn persistent_cleanup_failure_keeps_evidence_without_pinning_the_fd_domain() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 4).unwrap();
    let governor = hawdb_qos::RuntimeGovernor::detect(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 * 1024 * 1024),
            background_task_limit: std::num::NonZeroUsize::new(1),
            ..Default::default()
        },
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    let request = hawdb_qos::RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024);
    let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let mut writer = admission
        .create_writer(&fixture.0, Default::default())
        .unwrap();
    writer
        .writer_mut()
        .push(crate::SearchDocument {
            id: "private".into(),
            title: String::new(),
            content: "unpublished".into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    let path = fs::read_dir(&fixture.0)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "stage")
        })
        .unwrap();
    let unexpected = path.join("unexpected");
    fs::create_dir(&unexpected).unwrap();
    fs::write(unexpected.join("evidence"), b"retained").unwrap();
    drop(writer);
    drop(project);
    assert_eq!(governor.snapshot().active_background_tasks, 0);

    let reopened = ProjectFileDescriptors::acquire_existing(&fixture.0, 8).unwrap();
    for _ in 0..2 {
        let report =
            crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1).unwrap();
        assert_eq!(report.attempted_stages, 1);
        assert_eq!(report.pending_stages, 1);
        assert_eq!(report.blocked_stages, 1);
        assert_eq!(
            governor.snapshot().admitted_memory_bytes,
            report.retained_memory_bytes as u64
        );
        assert!(report.retained_memory_bytes < 8192);
        assert_eq!(fs::read(unexpected.join("evidence")).unwrap(), b"retained");
        assert_eq!(reopened.metrics().open, 0);
    }
    let next = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let mut writer = next.create_writer(&fixture.0, Default::default()).unwrap();
    writer
        .writer_mut()
        .push(crate::SearchDocument {
            id: "published".into(),
            title: String::new(),
            content: "survives cleanup debt".into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    writer.finish().unwrap();
    assert_eq!(
        crate::SearchOutOfCoreReader::open(&fixture.0)
            .unwrap()
            .document_count(),
        1
    );
    assert_eq!(fs::read(unexpected.join("evidence")).unwrap(), b"retained");
    fs::remove_file(unexpected.join("evidence")).unwrap();
    fs::remove_dir(unexpected).unwrap();
    assert_eq!(
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1)
            .unwrap()
            .removed_stages,
        1
    );
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[cfg(unix)]
#[test]
fn cleanup_retry_resolves_root_aliases_and_preserves_active_stages() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let fixture = Fixture::new();
    let root = fixture.0.join("root");
    let alias = fixture.0.join("alias");
    fs::create_dir(&root).unwrap();
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    let project = ProjectFileDescriptors::acquire_existing(&root, 1).unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let pending = StageDirectory::create(&root, &memory, &task).unwrap();
    let pending_path = pending.path.to_path_buf();
    let active = StageDirectory::create(&root, &memory, &task).unwrap();
    let held = File::create(root.join("held")).unwrap();
    drop(pending);
    drop(held);
    let report =
        crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(alias.join("."), 1).unwrap();
    assert_eq!(report.removed_stages, 1);
    assert!(!pending_path.exists());
    assert!(active.path.exists());
    drop(active);
    assert_eq!(project.metrics().open, 0);
}
