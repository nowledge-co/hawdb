// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::build_memory::{read::tests::governor, BuildMemory};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{RuntimeGovernor, RuntimePermit, RuntimeWorkRequest};
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};

type Hook = RefCell<Option<Box<dyn FnOnce()>>>;
type ProcessOpenHook = RefCell<Option<Box<dyn FnOnce(&File)>>>;
thread_local! {
    pub(super) static BEFORE_PROCESS_UNLOCK: Hook = const { RefCell::new(None) };
    pub(super) static AFTER_PROCESS_OPEN: ProcessOpenHook = RefCell::new(None);
    pub(super) static AFTER_INITIALIZING: Hook = const { RefCell::new(None) };
    pub(super) static AFTER_RETIRING: Hook = const { RefCell::new(None) };
    pub(super) static AFTER_NATIVE_CLOSE: Hook = const { RefCell::new(None) };
}
pub(super) fn hook(slot: &'static std::thread::LocalKey<Hook>) {
    let action = slot.with(|slot| slot.borrow_mut().take());
    if let Some(action) = action {
        action();
    }
}

pub(super) fn after_process_open(file: &File) {
    let callback = AFTER_PROCESS_OPEN.with(|slot| slot.borrow_mut().take());
    if let Some(callback) = callback {
        callback(file);
    }
}

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
    governor: RuntimeGovernor,
    permit: RuntimePermit,
    task: RuntimeTaskContext,
    memory: BuildMemory,
}
impl Fixture {
    fn new(limit: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-root-registry-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        Self {
            root,
            project,
            governor,
            permit,
            task,
            memory,
        }
    }
    fn admission(&self) -> ReadAdmission<'_> {
        ReadAdmission {
            memory: &self.memory,
            task: &self.task,
            permit: crate::build_memory::read::ReadPermit::Governed(&self.permit),
        }
    }
    fn open(&self) -> Result<RootRef> {
        RootRef::acquire(&self.root, &self.project.io_context(), &self.admission())
    }
    fn is_registered(&self) -> bool {
        let directory = SourceDirectory::capture(&self.root, &self.project.io_context()).unwrap();
        registry()
            .iter()
            .any(|entry| entry.state.identity == directory.identity())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        assert_eq!(self.project.metrics().open, 0);
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn shared_root_clones_reuse_native_handles_and_refund_after_last_drop() {
    let fixture = Fixture::new(8);
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    let root = fixture.open().unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    let charged = fixture.memory.ledger.snapshot().used_bytes;
    assert!(charged > baseline);
    let copy = root.clone();
    let reopen = fixture.open().unwrap();
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, charged);
    assert_eq!(fixture.project.metrics().open, 2);
    assert_eq!(fixture.governor.snapshot().active_background_io_slots, 0);
    drop(root);
    drop(reopen);
    assert!(fixture.is_registered());
    assert_eq!(fixture.project.metrics().open, 2);
    drop(copy);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    assert!(!fixture.is_registered());
}

#[test]
fn head_read_follows_captured_directory_after_path_replacement_with_one_io_slot() {
    let fixture = Fixture::new(8);
    std::fs::write(fixture.root.join("head"), b"captured head").unwrap();
    let moved = fixture.root.with_extension("moved");
    #[cfg(windows)]
    {
        // Windows can refuse ancestor rename once the process-lock child is
        // open. Replace the path after directory capture and registration, but
        // before that lock opens; both lock and head must follow the capture.
        let old_path = fixture.root.clone();
        let new_path = moved.clone();
        AFTER_INITIALIZING.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                std::fs::rename(&old_path, new_path).unwrap();
                std::fs::create_dir(&old_path).unwrap();
                std::fs::write(old_path.join("head"), b"foreign head").unwrap();
            }));
        });
    }
    let root = fixture.open().unwrap();
    #[cfg(not(windows))]
    {
        // Keep the original stronger post-initialization replacement on Unix.
        std::fs::rename(&fixture.root, &moved).unwrap();
        std::fs::create_dir(&fixture.root).unwrap();
        std::fs::write(fixture.root.join("head"), b"foreign head").unwrap();
    }
    assert!(moved.join(PROCESS_LOCK).is_file());
    assert!(!fixture.root.join(PROCESS_LOCK).exists());
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    let head = root
        .read_head(
            &fixture.project.io_context(),
            "head",
            &fixture.admission(),
            64,
        )
        .unwrap();
    assert_eq!(&*head, b"captured head");
    assert_eq!(
        std::fs::read(fixture.root.join("head")).unwrap(),
        b"foreign head"
    );
    assert_eq!(fixture.project.metrics().open, 2);
    assert_eq!(fixture.governor.snapshot().active_background_io_slots, 0);
    drop(head);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    drop(root);
    std::fs::remove_dir_all(moved).unwrap();
}

#[test]
fn held_head_denial_returns_descriptor_memory_and_io_capacity() {
    let fixture = Fixture::new(8);
    std::fs::write(fixture.root.join("head"), [7_u8; 1024]).unwrap();
    let root = fixture.open().unwrap();
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    assert!(root
        .read_head(
            &fixture.project.io_context(),
            "head",
            &fixture.admission(),
            8
        )
        .is_err());
    assert_eq!(fixture.project.metrics().open, 2);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(fixture.governor.snapshot().active_background_io_slots, 0);
    // Capture a context without an inventory before reserving all free slots.
    let context = fixture.project.io_context();
    let reserved = fixture.project.reserve(6).unwrap();
    assert!(root
        .read_head(&context, "head", &fixture.admission(), 2048)
        .is_err());
    assert_eq!(fixture.project.metrics().open, 2);
    assert_eq!(fixture.project.metrics().reserved, 6);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(fixture.governor.snapshot().active_background_io_slots, 0);
    drop(reserved);
    drop(root);
}

#[test]
fn two_concurrent_final_references_retire_instead_of_leaving_registry_owner() {
    let fixture = Fixture::new(8);
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    for _ in 0..32 {
        let root = fixture.open().unwrap();
        let copy = root.clone();
        let start = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let other = start.clone();
            scope.spawn(move || {
                other.wait();
                drop(root);
            });
            scope.spawn(move || {
                start.wait();
                drop(copy);
            });
        });
        assert_eq!(fixture.project.metrics().open, 0);
        assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
        assert!(!fixture.is_registered());
    }
}

#[test]
fn retirement_excludes_reacquisition_before_and_after_actual_native_close() {
    use std::sync::mpsc;
    use std::time::Duration;
    let fixture = Fixture::new(8);
    let root = fixture.open().unwrap();
    let (events, observed) = mpsc::channel();
    let (release_before, continue_before) = mpsc::channel();
    let (release_after, continue_after) = mpsc::channel();
    std::thread::scope(|scope| {
        let worker = scope.spawn(move || {
            let first = events.clone();
            AFTER_RETIRING.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    first.send("retiring").unwrap();
                    continue_before
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }))
            });
            AFTER_NATIVE_CLOSE.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    events.send("closed").unwrap();
                    continue_after.recv_timeout(Duration::from_secs(5)).unwrap();
                }))
            });
            drop(root);
        });
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap(),
            "retiring"
        );
        let before_count = fixture.project.metrics().open;
        let before_denied = fixture.open().is_err();
        release_before.send(()).unwrap();
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)).unwrap(),
            "closed"
        );
        let after_count = fixture.project.metrics().open;
        let after_denied = fixture.open().is_err();
        release_after.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(before_count, 2);
        assert!(before_denied);
        assert_eq!(after_count, 0);
        assert!(after_denied);
    });
    assert!(!fixture.is_registered());
    drop(fixture.open().unwrap());
}

#[test]
fn lock_fd_denial_cleans_initializing_entry_and_never_creates_lock() {
    let fixture = Fixture::new(1);
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    assert!(fixture.open().is_err());
    assert_eq!(fixture.project.metrics().open, 0);
    assert!(!fixture.root.join(PROCESS_LOCK).exists());
    assert!(!fixture.is_registered());
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
}

#[test]
fn a_different_project_domain_cannot_borrow_a_ready_roots_descriptors() {
    let owner = Fixture::new(8);
    let foreign = Fixture::new(8);
    let root = owner.open().unwrap();
    let result = RootRef::acquire(
        &owner.root,
        &foreign.project.io_context(),
        &foreign.admission(),
    );
    assert!(
        matches!(result, Err(HawDBError::Storage(ref message)) if message == "source root belongs to another project descriptor domain")
    );
    assert_eq!(foreign.project.metrics().open, 0);
    assert_eq!(owner.project.metrics().open, 2);
    drop(root);
}

#[test]
fn cancelled_initialization_closes_before_unregistering_and_allows_retry() {
    let fixture = Fixture::new(8);
    let cancel = fixture.task.cancellation().clone();
    AFTER_INITIALIZING.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            cancel.cancel();
        }))
    });
    assert!(fixture.open().is_err());
    assert_eq!(fixture.project.metrics().open, 0);
    assert!(!fixture.is_registered());
    let task = fixture.permit.bind_task_context(Default::default());
    let admission = ReadAdmission {
        memory: &fixture.memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&fixture.permit),
    };
    drop(RootRef::acquire(&fixture.root, &fixture.project.io_context(), &admission).unwrap());
}

#[test]
fn retained_root_releases_execution_but_keeps_peer_exclusion_until_final_close() {
    const PROBE: &str = "HAWDB_ROOT_REGISTRY_PROBE";
    const BUSY: &str = "HAWDB_ROOT_REGISTRY_PROBE_BUSY";
    if let Some(root) = std::env::var_os(PROBE) {
        let project = ProjectFileDescriptors::acquire_existing(Path::new(&root), 8).unwrap();
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        };
        let result = RootRef::acquire(Path::new(&root), &project.io_context(), &admission);
        if std::env::var_os(BUSY).is_some() {
            assert!(
                matches!(result, Err(HawDBError::Storage(ref message)) if message == "source root is owned by another process")
            );
        } else {
            drop(result.unwrap());
        }
        assert_eq!(project.metrics().open, 0);
        return;
    }
    // The shared test_dir helper retains a default-1024 project. This case
    // owns an explicit eight-FD domain, including in the peer process.
    let root = std::env::temp_dir().join(format!(
        "hawdb-root-registry-retained-peer-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let project = ProjectFileDescriptors::acquire(&root, 8).unwrap();
    let governor = governor();
    let retained = {
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        RootRef::acquire(
            &root,
            &project.io_context(),
            &ReadAdmission {
                memory: &memory,
                task: &task,
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
            },
        )
        .unwrap()
    };
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert!(snapshot.admitted_memory_bytes > 0);
    assert_eq!(project.metrics().open, 2);
    let probe = |busy| peer_root_probe(&root, busy);
    probe(true);
    // A new operation can be admitted and reuse custody without opening a
    // second process-lock descriptor or retaining the old execution permit.
    {
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let successor = RootRef::acquire(
            &root,
            &project.io_context(),
            &ReadAdmission {
                memory: &memory,
                task: &task,
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
            },
        )
        .unwrap();
        assert_eq!(project.metrics().open, 2);
        drop(successor);
    }
    probe(true);
    // A failed publisher must close only its own captured directory. Closing a
    // duplicate record-lock handle here could release this root's peer lock on
    // Unix implementations whose locks belong to the process.
    // Same-thread reservations are deliberately borrowed by nested operations;
    // occupy actual descriptors to exercise a genuine final-slot denial.
    let occupied: Vec<_> = (0..5)
        .map(|_| SourceDirectory::capture(&root, &project.io_context()).unwrap())
        .collect();
    assert_eq!(project.metrics().open, 7);
    assert!(
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(&root)
            .is_err()
    );
    assert_eq!(project.metrics().open, 7);
    assert_eq!(project.metrics().reserved, 0);
    drop(occupied);
    assert_eq!(project.metrics().open, 2);
    probe(true);
    let publisher =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(&root)
            .unwrap();
    // Shared root directory + process lock + publication lock.
    // Neither directory nor process lock is duplicated.
    assert_eq!(project.metrics().open, 3);
    probe(true);
    drop(publisher);
    assert_eq!(project.metrics().open, 2);
    probe(true);
    let publisher =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(&root)
            .unwrap();
    assert_eq!(project.metrics().open, 3);
    drop(retained);
    assert_eq!(project.metrics().open, 3);
    probe(true);
    drop(publisher);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    probe(false);
    assert!(root.exists(), "runtime custody never deletes durable roots");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cold_registry_gate_has_no_query_lifetime_heap() {
    use crate::test_allocation as allocation;
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let (_, peak) = allocation::measure(|| drop(registry()));
    assert_eq!(peak, 0);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn publisher_only_custody_excludes_new_root_initialization_then_allows_retry() {
    let fixture = Fixture::new(8);
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    let publisher =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
            &fixture.root,
        )
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 3);
    assert!(
        matches!(fixture.open(), Err(HawDBError::Storage(message)) if message.contains("publication owns source custody"))
    );
    assert_eq!(fixture.project.metrics().open, 3);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    drop(publisher);
    assert_eq!(fixture.project.metrics().open, 0);
    drop(fixture.open().unwrap());
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
}

#[test]
fn initializing_root_rejects_publisher_without_removing_initialization() {
    let fixture = Fixture::new(8);
    let path = fixture.root.clone();
    AFTER_INITIALIZING.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            let error =
                super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
                    &path,
                )
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("source root is initializing or retiring"));
        }));
    });
    let root = fixture.open().unwrap();
    assert!(AFTER_INITIALIZING.with(|slot| slot.borrow().is_none()));
    assert_eq!(fixture.project.metrics().open, 2);
    drop(root);
    drop(
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
            &fixture.root,
        )
        .unwrap(),
    );
}

#[test]
fn a_shared_descriptor_domain_does_not_allow_foreign_governor_borrowing() {
    let fixture = Fixture::new(8);
    let root = fixture.open().unwrap();
    let original_charge = fixture.governor.snapshot().admitted_memory_bytes;
    let foreign = governor();
    let permit = foreign
        .try_admit(RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let foreign_charge = foreign.snapshot().admitted_memory_bytes;
    let result = RootRef::acquire(
        &fixture.root,
        &fixture.project.io_context(),
        &ReadAdmission {
            memory: &memory,
            task: &task,
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        },
    );
    assert!(
        matches!(result, Err(HawDBError::Storage(message)) if message == "source root belongs to another runtime governor")
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(foreign.snapshot().admitted_memory_bytes, foreign_charge);
    assert_eq!(
        fixture.governor.snapshot().admitted_memory_bytes,
        original_charge
    );
    assert_eq!(fixture.project.metrics().open, 2);
    // The legitimate successor still shares the root and its retained charge.
    drop(fixture.open().unwrap());
    assert_eq!(fixture.project.metrics().open, 2);
    drop(root);
}

#[test]
fn native_publisher_promotion_moves_custody_without_a_new_descriptor() {
    let fixture = Fixture::new(4);
    std::fs::write(fixture.root.join("head"), b"promoted head").unwrap();
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    let mut publisher =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
            &fixture.root,
        )
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 3);
    peer_root_probe(&fixture.root, true);
    // Occupy the final slot: successful promotion proves it does not open a
    // replacement directory or process-lock file, even temporarily.
    let occupied = SourceDirectory::capture(&fixture.root, &fixture.project.io_context()).unwrap();
    assert_eq!(fixture.project.metrics().open, 4);
    let root = publisher
        .admit_source_root(&fixture.project.io_context(), &fixture.admission())
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 4);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(fixture.governor.snapshot().active_background_io_slots, 0);
    drop(occupied);
    let successor = fixture.open().unwrap();
    assert_eq!(fixture.project.metrics().open, 3);
    peer_root_probe(&fixture.root, true);
    drop(publisher);
    assert_eq!(fixture.project.metrics().open, 2);
    peer_root_probe(&fixture.root, true);
    let head = root
        .read_head(
            &fixture.project.io_context(),
            "head",
            &fixture.admission(),
            64,
        )
        .unwrap();
    assert_eq!(&*head, b"promoted head");
    drop(head);
    drop(root);
    assert_eq!(fixture.project.metrics().open, 2);
    drop(successor);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    assert!(!fixture.is_registered());
    peer_root_probe(&fixture.root, false);
}

#[test]
fn publisher_promotion_admission_failure_preserves_original_custody_and_retry() {
    use hawdb_core::RuntimeMemoryReservation;
    let fixture = Fixture::new(4);
    let mut publisher =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
            &fixture.root,
        )
        .unwrap();
    for limit in [1, state_bytes()] {
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let baseline = fixture.governor.snapshot().admitted_memory_bytes;
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: crate::build_memory::read::ReadPermit::Governed(&fixture.permit),
        };
        assert!(publisher
            .admit_source_root(&fixture.project.io_context(), &admission)
            .is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fixture.governor.snapshot().admitted_memory_bytes, baseline);
        assert_eq!(fixture.project.metrics().open, 3);
        assert!(!fixture.is_registered());
        assert!(fixture.open().is_err());
    }
    let cancelled = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&cancelled).unwrap();
    cancelled.cancellation().cancel();
    let admission = ReadAdmission {
        memory: &memory,
        task: &cancelled,
        permit: crate::build_memory::read::ReadPermit::Governed(&fixture.permit),
    };
    assert!(publisher
        .admit_source_root(&fixture.project.io_context(), &admission)
        .is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fixture.project.metrics().open, 3);
    assert!(!fixture.is_registered());
    let root = publisher
        .admit_source_root(&fixture.project.io_context(), &fixture.admission())
        .unwrap();
    drop(publisher);
    assert_eq!(fixture.project.metrics().open, 2);
    drop(root);
    assert_eq!(fixture.project.metrics().open, 0);
}

fn peer_root_probe(root: &Path, busy: bool) {
    const PROBE: &str = "HAWDB_ROOT_REGISTRY_PROBE";
    const BUSY: &str = "HAWDB_ROOT_REGISTRY_PROBE_BUSY";
    const CASE: &str = "out_of_core::root_registry::tests::retained_root_releases_execution_but_keeps_peer_exclusion_until_final_close";
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CASE, "--nocapture"])
        .env(PROBE, root);
    if busy {
        command.env(BUSY, "1");
    } else {
        command.env_remove(BUSY);
    }
    let result = command.output().unwrap();
    let output = String::from_utf8_lossy(&result.stdout);
    assert!(
        result.status.success(),
        "{output}\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        output.contains("1 passed; 0 failed"),
        "probe selected no case: {output}"
    );
}

#[test]
fn exact_pins_protect_each_cleanup_path_until_the_last_independent_owner_closes() {
    use crate::generation_cleanup::{
        once::PreparedCleanup, SearchProjectionCleanupState, SearchProjectionGenerations,
    };
    use crate::out_of_core::artifact_key::ArtifactKey;
    for retry in [false, true] {
        let fixture = Fixture::new(8);
        let retained = [
            "search_projection_segments.17.hawdb",
            "search_projection_segment_payloads.17.hawdb",
        ];
        let unrelated = [
            "search_projection_mutation_run.17.hawdb",
            "search_projection_segments.18.hawdb",
        ];
        for name in retained.iter().chain(unrelated.iter()) {
            std::fs::write(fixture.root.join(name), name.as_bytes()).unwrap();
        }
        let mut publication =
            super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
                &fixture.root,
            )
            .unwrap();
        let admission = fixture.admission();
        let capture = publication
            .capture_source_root(&fixture.project.io_context(), &admission)
            .unwrap();
        let pin = capture.pin(ArtifactKey::Content(17)).unwrap();
        let child = pin.clone();
        drop(capture);
        assert!(publication.protects(ArtifactKey::Content(17)));
        assert!(!publication.protects(ArtifactKey::MutationRun(17)));
        let mut state = SearchProjectionCleanupState::default();
        let mut sweep = || {
            let generations = SearchProjectionGenerations {
                out_of_core: Some(30),
                ..Default::default()
            };
            if retry {
                state.run_with_bound_test_generations(
                    &fixture.root,
                    generations,
                    Default::default(),
                    &publication,
                );
            } else {
                PreparedCleanup::prepare(&fixture.root, &fixture.memory, &fixture.task)
                    .unwrap()
                    .run_with_bound_test_generations(
                        &fixture.root,
                        generations,
                        Default::default(),
                        &fixture.task,
                        &publication,
                    );
            }
        };
        sweep();
        for name in retained {
            assert_eq!(
                std::fs::read(fixture.root.join(name)).unwrap(),
                name.as_bytes()
            );
        }
        for name in unrelated {
            assert!(!fixture.root.join(name).exists());
        }
        drop(pin);
        sweep();
        for name in retained {
            assert!(fixture.root.join(name).exists());
        }
        drop(child);
        sweep();
        for name in retained {
            assert!(!fixture.root.join(name).exists());
        }
        drop(publication);
        assert_eq!(fixture.memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fixture.project.metrics().open, 0);
    }
}

#[test]
fn pending_capture_defers_both_cleanup_paths_before_exact_keys_are_known() {
    use crate::generation_cleanup::{
        once::PreparedCleanup, SearchProjectionCleanupState, SearchProjectionGenerations,
    };
    for retry in [false, true] {
        let fixture = Fixture::new(8);
        let name = "search_projection_segments.17.hawdb";
        std::fs::write(fixture.root.join(name), b"pending source").unwrap();
        let mut publication =
            super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
                &fixture.root,
            )
            .unwrap();
        let admission = fixture.admission();
        let capture = publication
            .capture_source_root(&fixture.project.io_context(), &admission)
            .unwrap();
        let mut state = SearchProjectionCleanupState::default();
        let mut sweep = || {
            let generations = SearchProjectionGenerations {
                out_of_core: Some(30),
                ..Default::default()
            };
            if retry {
                state.run_with_bound_test_generations(
                    &fixture.root,
                    generations,
                    Default::default(),
                    &publication,
                );
            } else {
                PreparedCleanup::prepare(&fixture.root, &fixture.memory, &fixture.task)
                    .unwrap()
                    .run_with_bound_test_generations(
                        &fixture.root,
                        generations,
                        Default::default(),
                        &fixture.task,
                        &publication,
                    );
            }
        };
        sweep();
        assert_eq!(
            std::fs::read(fixture.root.join(name)).unwrap(),
            b"pending source"
        );
        drop(capture);
        sweep();
        assert!(!fixture.root.join(name).exists());
        drop(publication);
        assert_eq!(fixture.memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fixture.project.metrics().open, 0);
    }
}

// A dup shares the inherited open description, making the fork-to-exec window
// deterministic without depending on process scheduling or extending deadlines.
fn assert_inherited_description_released(publisher: bool) {
    let fixture = Fixture::new(8);
    let inherited = Arc::new(std::sync::Mutex::new(None));
    let saved = inherited.clone();
    AFTER_PROCESS_OPEN.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |file| {
            *saved.lock().unwrap() = Some(file.try_clone().unwrap());
        }));
    });
    let close: Box<dyn FnOnce()> = if publisher {
        let owner =
            super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
                &fixture.root,
            )
            .unwrap();
        Box::new(move || drop(owner))
    } else {
        let owner = fixture.open().unwrap();
        Box::new(move || drop(owner))
    };
    let inherited = inherited
        .lock()
        .unwrap()
        .take()
        .expect("duplicated native description");
    assert!(AFTER_PROCESS_OPEN.with(|slot| slot.borrow().is_none()));
    peer_root_probe(&fixture.root, true);
    close();
    assert_eq!(fixture.project.metrics().open, 1);
    assert!(!fixture.is_registered());
    peer_root_probe(&fixture.root, false);
    drop(inherited);
    let next = fixture.open().unwrap();
    peer_root_probe(&fixture.root, true);
    drop(next);
    peer_root_probe(&fixture.root, false);
}

#[test]
fn final_root_unlocks_before_an_inherited_description_closes() {
    assert_inherited_description_released(false);
}

#[test]
fn native_publisher_unlocks_before_an_inherited_description_closes() {
    assert_inherited_description_released(true);
}

#[test]
fn captured_native_file_keeps_exact_pin_and_retires_cache_before_process_unlock() {
    use crate::out_of_core::artifact_key::ArtifactKey;
    use hawdb_core::RuntimeMemoryPermit;
    #[derive(Debug)]
    struct Custody {
        _pin: ArtifactPin,
        memory: ReaderMemory,
    }
    impl RuntimeMemoryPermit for Custody {
        fn bytes(&self) -> u64 {
            self.memory.bytes()
        }
    }
    struct Control;
    impl hawdb_storage::file_io::ReadAdmissionControl for Control {
        fn checkpoint(&mut self) -> std::io::Result<()> {
            Ok(())
        }
        fn admit_validation(&mut self, _: u64) -> std::io::Result<()> {
            panic!("native identity acquisition must not read payload")
        }
    }
    let fixture = Fixture::new(8);
    let baseline = fixture.memory.ledger.snapshot().used_bytes;
    let name = "search_projection_segment_payloads.17.hawdb";
    std::fs::write(fixture.root.join(name), b"captured payload").unwrap();
    let mut publication =
        super::super::publish_lease::SearchProjectionPublishLease::acquire_for_consumer(
            &fixture.root,
        )
        .unwrap();
    let admission = fixture.admission();
    let capture = publication
        .capture_source_root(&fixture.project.io_context(), &admission)
        .unwrap();
    let pin = capture.pin(ArtifactKey::Content(17)).unwrap();
    let directory = capture
        .root()
        .state()
        .native
        .read()
        .unwrap()
        .as_ref()
        .unwrap()
        .directory
        .clone();
    let key_memory = admission
        .retained(File::captured_immutable_key_bytes::<ReaderMemory>(name).unwrap())
        .unwrap();
    let custody = Custody {
        _pin: pin.clone(),
        memory: admission
            .retained(File::captured_immutable_binding_bytes::<Custody>())
            .unwrap(),
    };
    let file = {
        let _io = admission.io().unwrap();
        File::open_captured_native_immutable(
            directory,
            &fixture.project.io_context(),
            name,
            key_memory,
            custody,
            &mut Control,
        )
        .unwrap()
    };
    drop(pin);
    drop(capture);
    drop(publication);
    assert_eq!(fixture.project.metrics().open, 2);
    peer_root_probe(&fixture.root, true);
    let bytes = admission.read_unbound_file(&file, 1024).unwrap();
    assert_eq!(&*bytes, b"captured payload");
    drop(bytes);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let pressure = fixture.project.reserve(6).unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(pressure);
    let bytes = admission.read_unbound_file(&file, 1024).unwrap();
    assert_eq!(&*bytes, b"captured payload");
    drop(bytes);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let project = fixture.project.clone();
    let root = fixture.root.clone();
    BEFORE_PROCESS_UNLOCK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(
                project.metrics().cached_handles,
                0,
                "idle children must close before unlock"
            );
            assert_eq!(project.metrics().open, 2);
            peer_root_probe(&root, true);
        }));
    });
    let project = fixture.project.clone();
    AFTER_NATIVE_CLOSE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(project.metrics().cached_handles, 0);
            assert_eq!(project.metrics().open, 0);
        }))
    });
    drop(file);
    assert!(!fixture.is_registered());
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.memory.ledger.snapshot().used_bytes, baseline);
    peer_root_probe(&fixture.root, false);
}
