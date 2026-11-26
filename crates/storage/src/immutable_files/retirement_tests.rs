// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Ordinary reads beside a retirement paused while it holds every lock it
//! ever holds (#1959).

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::file_io::ReadAdmissionControl;
use crate::immutable_object::{ImmutableObjectStore, ObjectKind};
use hawdb_core::RuntimeCancellationReason;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const LIMIT: Duration = Duration::from_secs(30);

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
    objects: ImmutableObjectStore,
    live: ImmutableFileBinding,
    doomed: ImmutableFileBinding,
    bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-retirement-read-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let project = ProjectFileDescriptors::acquire_existing(&root, 8).unwrap();
        let mut objects = ImmutableObjectStore::open(root.join("immutable")).unwrap();
        let bytes: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();
        let mut publish = |bytes: &[u8]| {
            let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, bytes);
            objects.publish(reference, bytes).unwrap();
            ImmutableFileBinding {
                reference,
                object_path: objects.object_path(reference),
            }
        };
        let live = publish(&bytes);
        let doomed = publish(&[7; 4096]);
        Self {
            root,
            project,
            objects,
            live,
            doomed,
            bytes,
        }
    }

    fn handles(&self) -> &ImmutableFileHandles {
        &self.project.immutable_handles
    }

    fn get(&self, binding: &ImmutableFileBinding) -> io::Result<Arc<File>> {
        self.handles().get(binding, &self.project.io_context())
    }

    /// Mounts the live object and opens it through the ordinary path API.
    fn live_file(&self) -> File {
        let path = self.root.join("live.page");
        self.handles()
            .mount(&path, self.live.clone(), &self.project.io_context())
            .unwrap();
        File::open(path).unwrap()
    }

    /// Runs `body` while a retirement of `doomed` is paused, then resumes it.
    fn while_retiring<R>(&self, body: impl FnOnce() -> R) -> (R, io::Result<()>) {
        std::thread::scope(|scope| {
            // Owned in here: a failing `body` resumes before the scope joins.
            let (pause, arm) = RetirementPause::new();
            let retiring = scope.spawn(move || {
                arm.arm();
                self.handles().retire_unreachable(self.doomed.reference)
            });
            assert_eq!(pause.wait_paused(LIMIT), Some(self.doomed.reference));
            let result = body();
            assert!(!retiring.is_finished(), "retirement must still be paused");
            drop(pause);
            (result, retiring.join().unwrap())
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn warm_reads_of_other_objects_do_not_wait_for_retirement() {
    let fixture = Fixture::new();
    let file = fixture.live_file();
    drop(fixture.get(&fixture.live).unwrap());
    let (read, retired) = fixture.while_retiring(|| {
        let mut bytes = vec![0; fixture.bytes.len()];
        crate::io::read_exact_at(&file, &mut bytes, 0).map(|()| bytes)
    });
    assert_eq!(read.unwrap(), fixture.bytes);
    retired.unwrap();
}

#[test]
fn cold_reads_wait_for_retirement_and_then_succeed() {
    let fixture = Fixture::new();
    let file = fixture.live_file();
    let (sender, receiver) = mpsc::channel();
    std::thread::scope(|scope| {
        let ((), retired) = fixture.while_retiring(|| {
            let (file, length) = (&file, fixture.bytes.len());
            scope.spawn(move || {
                let mut bytes = vec![0; length];
                let read = crate::io::read_exact_at(file, &mut bytes, 0).map(|()| bytes);
                sender.send(read).unwrap();
            });
            // Nothing can be reported before the retirement resumes: the cold
            // open must neither fail nor overtake it.
            assert_eq!(
                receiver
                    .recv_timeout(Duration::from_millis(200))
                    .unwrap_err(),
                mpsc::RecvTimeoutError::Timeout
            );
        });
        retired.unwrap();
        assert_eq!(
            receiver.recv_timeout(LIMIT).unwrap().unwrap(),
            fixture.bytes
        );
    });
}

struct StoppingControl {
    reason: RuntimeCancellationReason,
    after: Instant,
}

impl ReadAdmissionControl for StoppingControl {
    fn admit_validation(&mut self, _bytes: u64) -> io::Result<()> {
        panic!("a stopped read must not admit validation");
    }

    fn checkpoint(&mut self) -> io::Result<()> {
        if Instant::now() < self.after {
            return Ok(());
        }
        Err(io::Error::other(self.reason))
    }
}

#[test]
fn a_cold_read_waiting_for_retirement_observes_cancellation_and_deadline() {
    for reason in [
        RuntimeCancellationReason::Cancelled,
        RuntimeCancellationReason::DeadlineExceeded,
    ] {
        let fixture = Fixture::new();
        let file = fixture.live_file();
        let (stopped, retired) = fixture.while_retiring(|| {
            let mut control = StoppingControl {
                reason,
                after: Instant::now() + Duration::from_millis(50),
            };
            file.with_admitted_read(&mut control, |_| Ok(()))
        });
        let error = stopped.unwrap_err();
        assert!(crate::reader_failure::is_admission_or_cancellation(&error));
        assert_eq!(
            hawdb_core::error::HawDBError::from_storage_error(error),
            hawdb_core::error::HawDBError::RuntimeTaskStopped(reason)
        );
        retired.unwrap();
        // Neither the gate nor any descriptor stays held.
        assert_eq!(fixture.project.metrics().open, 0);
        drop(fixture.get(&fixture.live).unwrap());
    }
}

#[test]
fn a_lease_taken_while_retirement_runs_keeps_the_object() {
    let fixture = Fixture::new();
    drop(fixture.get(&fixture.doomed).unwrap());
    let (lease, retired) = fixture.while_retiring(|| fixture.get(&fixture.doomed).unwrap());
    let error = retired.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    let mut bytes = vec![0; 4096];
    crate::io::read_exact_at(&lease, &mut bytes, 0).unwrap();
    assert_eq!(bytes, [7; 4096]);
    drop(lease);
    fixture
        .handles()
        .retire_unreachable(fixture.doomed.reference)
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn an_object_leased_while_another_retires_stays_pending_and_the_sweep_succeeds() {
    let mut fixture = Fixture::new();
    let other = {
        let bytes = [9; 4096];
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
        fixture.objects.publish(reference, &bytes).unwrap();
        ImmutableFileBinding {
            reference,
            object_path: fixture.objects.object_path(reference),
        }
    };
    let candidates = [fixture.doomed.clone(), other, fixture.live.clone()];
    for binding in &candidates {
        drop(fixture.get(binding).unwrap());
    }
    let root = fixture.root.clone();
    let (report, leases) = std::thread::scope(|scope| {
        let (pause, arm) = RetirementPause::new();
        let sweep = scope.spawn(move || {
            arm.arm();
            ImmutableObjectStore::open(root.join("immutable"))
                .unwrap()
                .sweep_unreachable_bounded(
                    &[],
                    &Default::default(),
                    crate::immutable_object::ObjectSweepBudget {
                        max_objects: 16,
                        max_bytes: 1 << 20,
                        max_verified_reachable_bytes: 0,
                        skip_unverifiable: true,
                    },
                    &mut || Ok(()),
                )
        });
        let first = pause.wait_paused(LIMIT).unwrap();
        // Each object is decided on its own: lease every other candidate after
        // the sweep selected it and before the sweep retires it.
        let leases: Vec<_> = candidates
            .iter()
            .filter(|binding| binding.reference != first)
            .map(|binding| (binding.clone(), fixture.get(binding).unwrap()))
            .collect();
        assert_eq!(leases.len(), 2);
        drop(pause);
        (sweep.join().unwrap().unwrap(), leases)
    });
    assert_eq!((report.reclaimed_objects, report.pending_objects), (1, 2));
    assert!(report.retry_required);
    for (binding, lease) in &leases {
        assert!(binding.object_path.exists());
        assert_eq!(
            lease.metadata().unwrap().len(),
            binding.reference.byte_length
        );
    }
    drop(leases);
    for binding in &candidates {
        fixture
            .handles()
            .retire_unreachable(binding.reference)
            .unwrap();
    }
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn a_retired_and_removed_object_is_not_found_and_a_reused_name_is_verified() {
    let mut fixture = Fixture::new();
    drop(fixture.get(&fixture.doomed).unwrap());
    let reference = fixture.doomed.reference;
    let path = fixture.doomed.object_path.clone();
    fixture.handles().retire_unreachable(reference).unwrap();
    crate::immutable_object::forget_published_object(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    let missing = || fixture.get(&fixture.doomed).unwrap_err().kind();
    assert_eq!(missing(), io::ErrorKind::NotFound);
    // Nothing may serve another inode that takes the object's name.
    std::fs::write(&path, [8; 4096]).unwrap();
    assert_eq!(missing(), io::ErrorKind::InvalidData);
    std::fs::remove_file(&path).unwrap();
    // A republication has the same content by construction of its name.
    fixture.objects.publish(reference, &[7; 4096]).unwrap();
    let mut bytes = vec![0; 4096];
    crate::io::read_exact_at(&fixture.get(&fixture.doomed).unwrap(), &mut bytes, 0).unwrap();
    assert_eq!(bytes, [7; 4096]);
}

/// Between a successful retirement and its unlink nothing excludes an open;
/// the reachability barrier does. If one happens it has the right inode, which
/// outlives its name. (Windows refuses the unlink, which the sweep defers.)
#[cfg(unix)]
#[test]
fn a_lease_taken_between_retirement_and_unlink_reads_its_own_inode() {
    let fixture = Fixture::new();
    fixture
        .handles()
        .retire_unreachable(fixture.doomed.reference)
        .unwrap();
    let lease = fixture.get(&fixture.doomed).unwrap();
    std::fs::remove_file(&fixture.doomed.object_path).unwrap();
    std::fs::write(&fixture.doomed.object_path, [8; 4096]).unwrap();
    let mut bytes = vec![0; 4096];
    crate::io::read_exact_at(&lease, &mut bytes, 0).unwrap();
    assert_eq!(bytes, [7; 4096]);
}

#[test]
fn an_alias_being_read_keeps_the_object_its_retired_sibling_shares() {
    let fixture = Fixture::new();
    let aliases = ["a.page", "b.page"].map(|name| fixture.root.join(name));
    for alias in &aliases {
        fixture
            .handles()
            .mount(alias, fixture.doomed.clone(), &fixture.project.io_context())
            .unwrap();
    }
    let retire = || {
        fixture
            .handles()
            .retire_unreachable(fixture.doomed.reference)
    };
    struct Open;
    impl ReadAdmissionControl for Open {
        fn admit_validation(&mut self, _bytes: u64) -> io::Result<()> {
            Ok(())
        }
        fn checkpoint(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let reader = File::open(&aliases[1]).unwrap();
    let pinned = reader.try_clone().unwrap().pin_admitted(&mut Open).unwrap();
    std::fs::remove_file(&aliases[0]).unwrap();
    assert_eq!(retire().unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert!(fixture.handles().binding(&aliases[1]).unwrap().is_some());
    let mut bytes = vec![0; 4096];
    crate::io::read_exact_at(&pinned, &mut bytes, 0).unwrap();
    assert_eq!(bytes, [7; 4096]);
    drop(pinned);
    retire().unwrap();
    std::fs::remove_file(&fixture.doomed.object_path).unwrap();
    // The surviving name is the same inode, but the retired object is not
    // reachable through the old logical reference any more.
    assert_eq!(
        crate::io::read_exact_at(&reader, &mut bytes, 0)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert_eq!(std::fs::read(&aliases[1]).unwrap(), [7; 4096]);
}

#[test]
fn cold_corruption_beside_retirement_stays_an_integrity_error() {
    let fixture = Fixture::new();
    let file = fixture.live_file();
    // A same-length replacement; `forget` drops this process's publication
    // record so that the identity is really verified.
    crate::immutable_object::forget_published_object(&fixture.live.object_path).unwrap();
    let mut damaged = fixture.bytes.clone();
    damaged[1000] ^= 1;
    std::fs::remove_file(&fixture.live.object_path).unwrap();
    std::fs::write(&fixture.live.object_path, damaged).unwrap();
    std::thread::scope(|scope| {
        let (reading, retired) = fixture.while_retiring(|| {
            let file = &file;
            let reading = scope.spawn(move || {
                let mut byte = [0];
                crate::io::read_exact_at(file, &mut byte, 0)
            });
            std::thread::sleep(Duration::from_millis(50));
            reading
        });
        retired.unwrap();
        let error = reading.join().unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!crate::reader_failure::is_admission_or_cancellation(&error));
    });
}

/// Readers and a retirer race freely; no read may see anything but its bytes.
#[test]
fn concurrent_reads_and_retirements_never_conflict_or_deadlock() {
    let fixture = Fixture::new();
    let file = fixture.live_file();
    let deadline = Instant::now() + Duration::from_millis(500);
    std::thread::scope(|scope| {
        for _ in 0..3 {
            scope.spawn(|| {
                while Instant::now() < deadline {
                    let mut byte = [0];
                    crate::io::read_exact_at(&file, &mut byte, 250).unwrap();
                    assert_eq!(byte, [250]);
                }
            });
        }
        while Instant::now() < deadline {
            fixture
                .handles()
                .retire_unreachable(fixture.doomed.reference)
                .unwrap();
            fixture.handles().evict_idle(usize::MAX);
        }
    });
}

mod checkpoint_reader {
    use super::*;
    use crate::background::{CheckpointWorkContext, CheckpointWorkError, CheckpointWorkProbe};
    use hawdb_core::RuntimeTaskContext;

    #[test]
    fn a_reader_inside_an_active_checkpoint_never_waits_for_retirement() {
        let fixture = Fixture::new();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = CheckpointWorkContext::new(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let context = fixture.project.io_context();
        let get = || {
            fixture
                .handles()
                .checkpoint_get(&fixture.live, &context, &work)
        };
        // Cold: defers instead of waiting, as for any contended cold open.
        let (cold, retired) = fixture.while_retiring(get);
        assert!(matches!(
            cold,
            Err(CheckpointImmutableFileError::Work(
                CheckpointWorkError::Contended(_)
            ))
        ));
        retired.unwrap();
        // Warm: unaffected.
        let work = CheckpointWorkContext::new(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        drop(fixture.get(&fixture.live).unwrap());
        let (warm, retired) = fixture.while_retiring(|| {
            fixture
                .handles()
                .checkpoint_get(&fixture.live, &context, &work)
        });
        warm.unwrap();
        retired.unwrap();
    }
}

/// Exhaustive interleavings of the protocol's atomic steps: two readers (each
/// a warm lookup, else a cold open) of two cache keys that name one object,
/// beside one retirement of it. Every `handles` critical section is one step.
mod protocol_model {
    use std::collections::BTreeSet;

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    enum Pc {
        Start,
        WaitOpening,
        Opened,
        Leased,
        Done,
        Checked,
        Retired,
        Refused,
    }

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct State {
        /// The holder of `opening`; 2 is the retirement.
        opening: Option<usize>,
        cached: [bool; 2],
        pc: [Pc; 3],
    }

    impl State {
        fn leased(&self) -> bool {
            self.pc[..2].contains(&Pc::Leased)
        }

        fn step(&self, actor: usize, atomic: bool) -> Option<State> {
            let mut next = self.clone();
            let pc = self.pc[actor];
            next.pc[actor] = match (actor, pc) {
                (0 | 1, Pc::Start) if self.cached[actor] => Pc::Leased,
                (0 | 1, Pc::Start) => Pc::WaitOpening,
                (0 | 1, Pc::WaitOpening) if self.opening.is_none() => {
                    next.opening = Some(actor);
                    Pc::Opened
                }
                (0 | 1, Pc::Opened) => {
                    next.cached[actor] = true;
                    next.opening = None;
                    Pc::Leased
                }
                (0 | 1, Pc::Leased) => Pc::Done,
                (2, Pc::Start) if self.opening.is_none() => {
                    next.opening = Some(2);
                    Pc::WaitOpening
                }
                // One pass: see every entry idle and, unless split by the
                // mutation, remove one entry under the same lock.
                (2, Pc::WaitOpening | Pc::Checked) => {
                    if pc == Pc::WaitOpening && self.leased() {
                        next.opening = None;
                        Pc::Refused
                    } else if !atomic && pc == Pc::WaitOpening {
                        Pc::Checked
                    } else if let Some(key) = self.cached.iter().position(|cached| *cached) {
                        // The split variant removes only idle entries, as the
                        // old code did, and finishes regardless.
                        if !atomic && self.pc[key] == Pc::Leased {
                            next.opening = None;
                            Pc::Retired
                        } else {
                            next.cached[key] = false;
                            pc
                        }
                    } else {
                        next.opening = None;
                        Pc::Retired
                    }
                }
                _ => return None,
            };
            Some(next)
        }
    }

    /// Returns the number of states, or the first violating transition.
    fn explore(atomic: bool) -> Result<usize, String> {
        let mut seen = BTreeSet::new();
        let mut todo = Vec::new();
        for cached in [[false, false], [true, false], [false, true], [true, true]] {
            todo.push(State {
                opening: None,
                cached,
                pc: [Pc::Start; 3],
            });
        }
        while let Some(state) = todo.pop() {
            if !seen.insert(state.clone()) {
                continue;
            }
            let successors: Vec<_> = (0..3)
                .filter_map(|actor| state.step(actor, atomic))
                .collect();
            let finished =
                state.pc[..2] == [Pc::Done; 2] && matches!(state.pc[2], Pc::Retired | Pc::Refused);
            if successors.is_empty() && !finished {
                return Err(format!("deadlock: {state:?}"));
            }
            for next in successors {
                // The retirement may succeed only while nothing is leased or
                // cached, which is what lets its caller unlink the object.
                if next.pc[2] == Pc::Retired
                    && state.pc[2] != Pc::Retired
                    && (next.leased() || next.cached != [false; 2])
                {
                    return Err(format!("retired in use: {state:?} -> {next:?}"));
                }
                todo.push(next);
            }
        }
        Ok(seen.len())
    }

    #[test]
    fn every_interleaving_retires_only_an_unused_object_without_deadlock() {
        assert!(explore(true).unwrap() > 50);
        // The model notices the race that one `handles` lock rules out.
        assert!(explore(false).unwrap_err().starts_with("retired in use"));
    }
}
