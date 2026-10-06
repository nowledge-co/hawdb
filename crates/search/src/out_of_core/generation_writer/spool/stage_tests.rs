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
fn descriptor_denial_retains_accounting_until_explicit_or_automatic_retry() {
    use crate::SearchGenerationAdmission;
    use hawdb_qos::IoConcurrencyBudget;
    use hawdb_qos::{RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest};
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    for retry in 0..3 {
        let fixture = Fixture::new();
        let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 4).unwrap();
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(64 * 1024 * 1024),
                background_task_limit: std::num::NonZeroUsize::new(1),
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
        assert!(matches!(
            report.descriptor_error,
            Some(hawdb_core::error::FileDescriptorError::BudgetExceeded { .. })
        ));
        assert_eq!(
            governor.snapshot().admitted_memory_bytes,
            request.memory_bytes
        );
        assert!(governor.try_admit(request).is_err());
        let denied =
            crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1).unwrap();
        assert_eq!(denied.attempted_stages, 1);
        assert_eq!(denied.pending_stages, 1);
        assert_eq!(denied.reserved_disk_bytes, report.reserved_disk_bytes);
        release_tx.send(()).unwrap();
        competitor.join().unwrap();
        match retry {
            0 => {
                let released =
                    crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 1)
                        .unwrap();
                assert!(released.removed_stages <= 1);
            }
            1 => {
                let task = RuntimeTaskContext::default();
                let memory = BuildMemory::new(&task).unwrap();
                let next = StageDirectory::create(&fixture.0, &memory, &task).unwrap();
                assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
                drop(next);
            }
            _ => {
                // The old permit fills the sole background slot. Retry must run
                // before admission, rather than only inside create_writer.
                let next = SearchGenerationAdmission::acquire(&governor, request).unwrap();
                let writer = next.create_writer(&fixture.0, Default::default()).unwrap();
                drop(writer);
            }
        }
        let released =
            crate::SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&fixture.0, 0).unwrap();
        assert_eq!(released.pending_stages, 0);
        assert_eq!(released.reserved_disk_bytes, 0);
        assert_eq!(released.retained_memory_bytes, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(project.metrics().open, 0);
    }
}
