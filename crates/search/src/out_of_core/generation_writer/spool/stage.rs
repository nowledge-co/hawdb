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

//! Retain a pre-admitted cleanup owner when counted removal is denied.

use super::*;
use crate::build_memory::{directory, path::OwnedPath};
use hawdb_executor::QueryMemoryLease;
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// Bound each canonical root independently. Every map entry is pre-admitted;
// retention needs no allocation during Drop or descriptor exhaustion.
const MAX_ROOT_OWNERS: usize = 256;
static OWNERS: Mutex<BTreeMap<u64, Owner>> = Mutex::new(BTreeMap::new());
static NEXT_OWNER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static NEXT_RETRY_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
const AUTOMATIC_RETRY_STAGES: usize = 4;
const AUTOMATIC_CLEANUP_BATCHES: usize = 4;
// Bound the three shared ledger accounts retained by path and metadata leases.
const CLEANUP_ACCOUNT_METADATA_BYTES: usize = 4096;

enum Slot {
    Active,
    Pending(Ticket),
}

struct Owner {
    root: OwnedPath,
    slot: Slot,
    last_retry: u64,
    _memory: QueryMemoryLease,
    _host_memory: Option<hawdb_qos::RuntimeRetainedMemory>,
}

struct Registration {
    index: u64,
}
impl Registration {
    fn acquire(owner: Owner) -> Result<Self> {
        let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
        if owners
            .values()
            .filter(|entry| *entry.root == *owner.root)
            .count()
            >= MAX_ROOT_OWNERS
        {
            return Err(HawDBError::Execution(
                "search private-stage cleanup owner capacity exhausted".into(),
            ));
        }
        let index = NEXT_OWNER
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |index| {
                index.checked_add(1)
            })
            .map_err(|_| HawDBError::Execution("search cleanup owner identity exhausted".into()))?;
        owners.insert(index, owner);
        Ok(Self { index })
    }
    fn retain(self, ticket: Ticket) {
        OWNERS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(&self.index)
            .expect("registered cleanup owner")
            .slot = Slot::Pending(ticket);
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let removed = {
            let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
            if owners
                .get(&self.index)
                .is_some_and(|owner| matches!(owner.slot, Slot::Active))
            {
                let removed = owners.remove(&self.index);
                // BTreeMap may retain an empty root allocation after removal.
                // Release it before the final owner's admission disappears.
                if owners.is_empty() {
                    *owners = BTreeMap::new();
                }
                removed
            } else {
                None
            }
        };
        drop(removed);
    }
}

struct Ticket {
    value: Box<TicketFields>,
}
impl std::ops::Deref for Ticket {
    type Target = TicketFields;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl std::ops::DerefMut for Ticket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
struct TicketFields {
    path: OwnedPath,
    source_root: OwnedPath,
    disk_reservation: u64,
    error: Option<CleanupFailure>,
}

enum CleanupFailure {
    Descriptor(hawdb_core::error::FileDescriptorError),
    Other,
}
impl From<HawDBError> for CleanupFailure {
    fn from(error: HawDBError) -> Self {
        match error {
            HawDBError::FileDescriptors(error) => Self::Descriptor(error),
            _ => Self::Other,
        }
    }
}

// An interrupted attempt still owns a private stage. Return its ticket to the
// pre-admitted registry before its registration can release retained memory.
struct CleanupAttempt {
    registration: Option<Registration>,
    ticket: Option<Ticket>,
}

impl CleanupAttempt {
    fn new(registration: Registration, ticket: Ticket) -> Self {
        Self {
            registration: Some(registration),
            ticket: Some(ticket),
        }
    }

    fn ticket(&mut self) -> &mut Ticket {
        self.ticket.as_mut().expect("live cleanup attempt")
    }

    fn complete(mut self) {
        // Native removal has completed. Free ticket allocations before their
        // registration and its retained admission disappear.
        drop(self.ticket.take());
        drop(self.registration.take());
    }
}

impl Drop for CleanupAttempt {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.registration
                .take()
                .expect("registered cleanup attempt")
                .retain(ticket);
        }
    }
}

#[derive(Debug, Default)]
pub struct SearchStagingCleanupReport {
    pub attempted_stages: usize,
    pub removed_stages: usize,
    pub pending_stages: usize,
    /// Conservative stage limits retained until deletion, not cumulative writes.
    pub reserved_disk_bytes: u64,
    pub retained_memory_bytes: usize,
    pub descriptor_denials: usize,
    /// Tickets that used their bounded retry work without finishing.
    pub progress_limited_stages: usize,
    /// Non-descriptor failures; retained paths remain evidence for inspection.
    pub blocked_stages: usize,
    pub descriptor_error: Option<hawdb_core::error::FileDescriptorError>,
}

pub(in crate::out_of_core) fn retry_staging_cleanup(
    root: &Path,
    max_attempts: usize,
) -> Result<SearchStagingCleanupReport> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task)?;
    let root = OwnedPath::absolute(root, &memory, &task)?;
    // The original absolute spelling also needs no descriptor. In particular,
    // Windows may add a verbatim prefix when canonicalizing the same directory.
    let (pending, exact) = {
        let owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
        (
            owners
                .values()
                .any(|owner| matches!(owner.slot, Slot::Pending(_))),
            owners.values().any(
                |owner| matches!(&owner.slot, Slot::Pending(ticket) if ticket.matches_root(&root)),
            ),
        )
    };
    if !pending {
        return Ok(SearchStagingCleanupReport::default());
    }
    if exact {
        retry_registered(&root, max_attempts, usize::MAX, &memory, &task)
    } else {
        let root = OwnedPath::canonicalize(&root, &memory, &task)?;
        retry_registered(&root, max_attempts, usize::MAX, &memory, &task)
    }
}

fn retry_registered(
    root: &Path,
    max_attempts: usize,
    max_batches: usize,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<SearchStagingCleanupReport> {
    let mut report = SearchStagingCleanupReport::default();
    checkpoint(task)?;
    let _selection_memory = memory
        .spool
        .reserve(std::mem::size_of::<[(u64, u64); MAX_ROOT_OWNERS]>())?;
    let mut selected = [(0, 0); MAX_ROOT_OWNERS];
    let mut count = 0;
    {
        let owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
        for (&index, owner) in owners.iter() {
            if matches!(&owner.slot, Slot::Pending(ticket) if ticket.matches_root(root)) {
                if count == MAX_ROOT_OWNERS {
                    return Err(HawDBError::Execution(
                        "search cleanup root inventory exceeds owner capacity".into(),
                    ));
                }
                selected[count] = (owner.last_retry, index);
                count += 1;
            }
        }
    }
    // Foreign roots cannot reset this root's progress. Sort only admitted
    // identities, with no allocation or permanently retained root index.
    selected[..count].sort_unstable();
    for &(last_retry, index) in &selected[..count] {
        if report.attempted_stages == max_attempts {
            break;
        }
        let ticket = {
            let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
            match owners.get_mut(&index) {
                Some(owner)
                    if owner.last_retry == last_retry
                        && matches!(&owner.slot, Slot::Pending(ticket) if ticket.matches_root(root)) =>
                {
                    owner.last_retry = NEXT_RETRY_SEQUENCE
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                            sequence.checked_add(1)
                        })
                        .map_err(|_| {
                            HawDBError::Execution("search cleanup retry identity exhausted".into())
                        })?;
                    match std::mem::replace(&mut owner.slot, Slot::Active) {
                        Slot::Pending(ticket) => Some(ticket),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            }
        };
        if let Some(ticket) = ticket {
            let mut attempt = CleanupAttempt::new(Registration { index }, ticket);
            let workspace = (|| {
                checkpoint(task)?;
                memory
                    .spool
                    .reserve(directory::stage_removal_bytes(&attempt.ticket().path)?)
            })();
            let _workspace = match workspace {
                Ok(workspace) => workspace,
                Err(error) => {
                    // Caller admission failed before touching the stage. Keep
                    // its previous cleanup disposition and ownership intact.
                    return Err(error);
                }
            };
            report.attempted_stages += 1;
            // A panic after this point is an unknown cleanup failure. Normal
            // errors below replace it with their concrete disposition.
            attempt.ticket().error = Some(CleanupFailure::Other);
            let result = (|| {
                let _project = ProjectFileDescriptors::acquire_component(
                    attempt
                        .ticket()
                        .path
                        .parent()
                        .expect("registered stage parent"),
                    false,
                )?;
                attempt.ticket().remove_batches(max_batches)
            })();
            match result {
                Ok(true) => {
                    report.removed_stages += 1;
                    attempt.complete();
                }
                result => {
                    attempt.ticket().error = result.err().map(CleanupFailure::from);
                }
            }
        }
    }
    let owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
    for owner in owners.values() {
        if let Slot::Pending(ticket) = &owner.slot
            && ticket.matches_root(root)
        {
            report.pending_stages += 1;
            report.reserved_disk_bytes = report
                .reserved_disk_bytes
                .saturating_add(ticket.disk_reservation);
            report.retained_memory_bytes = report
                .retained_memory_bytes
                .saturating_add(owner._memory.bytes())
                .saturating_add(owner.root.retained_bytes())
                .saturating_add(ticket.path.retained_bytes())
                .saturating_add(ticket.source_root.retained_bytes());
            match &ticket.error {
                Some(CleanupFailure::Descriptor(error)) => {
                    report.descriptor_denials += 1;
                    report.descriptor_error = Some(error.clone());
                }
                Some(CleanupFailure::Other) => report.blocked_stages += 1,
                None => report.progress_limited_stages += 1,
            }
        }
    }
    Ok(report)
}

impl Ticket {
    fn matches_root(&self, root: &Path) -> bool {
        self.path.parent() == Some(root) || &*self.source_root == root
    }

    fn remove_batches(&self, max_batches: usize) -> Result<bool> {
        // Cleanup is resumable: each counted step can defer without losing the
        // owner. Close the iterator before unlinking its bounded batch.
        // No operation-wide descriptor reservation or busy retry is needed.
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            result => {
                if !result?.is_dir() {
                    return Err(HawDBError::Storage(
                        "search private stage is no longer a directory".into(),
                    ));
                }
            }
        }
        for _ in 0..max_batches {
            let mut paths: [Option<(std::path::PathBuf, bool)>;
                directory::STAGE_REMOVAL_BATCH_ENTRIES] = Default::default();
            let mut count = 0;
            {
                let entries = fs::read_dir(&self.path)?;
                for (slot, entry) in paths.iter_mut().zip(entries) {
                    let entry = entry?;
                    let kind = entry.file_type()?;
                    *slot = Some((entry.path(), kind.is_dir() && !kind.is_symlink()));
                    count += 1;
                }
            }
            if count == 0 {
                fs::remove_dir(&self.path)?;
                return Ok(true);
            }
            for (path, directory) in paths.into_iter().flatten() {
                if directory {
                    return Err(HawDBError::Storage(
                        "search private stage contains an unexpected directory".into(),
                    ));
                }
                #[cfg(test)]
                evidence::before_unlink(&path)?;
                fs::remove_file(path)?;
            }
        }
        Ok(false)
    }
}

pub(in crate::out_of_core) struct StageDirectory {
    pub(in crate::out_of_core) path: super::super::context_memory::OwnedPath,
    registration: Option<Registration>,
    ticket: Option<Ticket>,
    cleanup_memory: Option<QueryMemoryLease>,
    project: Option<ProjectFileDescriptors>,
    admission: Option<Arc<hawdb_qos::RuntimePermit>>,
}

impl StageDirectory {
    pub(in crate::out_of_core) fn create(
        root: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let source_root = OwnedPath::absolute(root, memory, task)?;
        let project = ProjectFileDescriptors::acquire_component(&source_root, false)?;
        let root = OwnedPath::canonicalize(&source_root, memory, task)?;
        retry_registered(
            &root,
            AUTOMATIC_RETRY_STAGES,
            AUTOMATIC_CLEANUP_BATCHES,
            memory,
            task,
        )?;
        for _ in 0..64 {
            checkpoint(task)?;
            let _name_memory = memory.retained.reserve(3 * 128)?;
            let sequence = GENERATION_WRITER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let name = format!(
                ".search-generation.{}.{}.stage",
                std::process::id(),
                sequence
            );
            if name.capacity() > 128 {
                return Err(HawDBError::Execution(
                    "search stage name exceeds preflight capacity".into(),
                ));
            }
            let path = super::super::context_memory::OwnedPath::join(
                &root,
                Path::new(&name),
                memory,
                task,
            )?;
            let cleanup = memory
                .spool
                .reserve(directory::stage_removal_bytes(&path)?)?;
            let metadata = memory.retained.reserve(
                std::mem::size_of::<TicketFields>()
                    + std::mem::size_of::<Owner>()
                    + crate::build_memory::MAP_ENTRY_BYTES
                    + CLEANUP_ACCOUNT_METADATA_BYTES,
            )?;
            let ticket_path = OwnedPath::copy(&path, memory, task)?;
            let owner_root = OwnedPath::copy(&root, memory, task)?;
            let retained_bytes = crate::build_memory::checked_add(
                metadata.bytes(),
                crate::build_memory::checked_add(
                    crate::build_memory::checked_add(
                        ticket_path.retained_bytes(),
                        owner_root.retained_bytes(),
                    )?,
                    source_root.retained_bytes(),
                )?,
            )?;
            let host_memory = memory
                .host_admission
                .as_ref()
                .map(|permit| {
                    permit
                        .reserve_retained_memory(retained_bytes as u64)
                        .map_err(|error| HawDBError::Execution(error.to_string()))
                })
                .transpose()?;
            let registration = Registration::acquire(Owner {
                root: owner_root,
                slot: Slot::Active,
                last_retry: 0,
                _memory: metadata,
                _host_memory: host_memory,
            })?;
            let created = super::super::io::GenerationIo::new(memory, task)
                .native(&[&path], || fs::create_dir(&path))?;
            match created {
                Ok(()) => {
                    let stage = Self {
                        path,
                        registration: Some(registration),
                        ticket: Some(Ticket {
                            value: Box::new(TicketFields {
                                path: ticket_path,
                                source_root,
                                disk_reservation: 0,
                                error: None,
                            }),
                        }),
                        cleanup_memory: Some(cleanup),
                        project: Some(project),
                        admission: memory.host_admission.clone(),
                    };
                    checkpoint(task)?;
                    return Ok(stage);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HawDBError::Storage(
            "failed to allocate a unique search generation stage directory".into(),
        ))
    }

    pub(in crate::out_of_core) fn reserve_disk(&mut self, bytes: u64) {
        self.ticket.as_mut().expect("live stage").disk_reservation = bytes;
    }

    pub(in crate::out_of_core) fn reserve_additional_disk(&mut self, bytes: u64) -> Result<()> {
        let ticket = self.ticket.as_mut().expect("live stage");
        ticket.disk_reservation = ticket
            .disk_reservation
            .checked_add(bytes)
            .ok_or_else(|| HawDBError::Storage("search stage disk reservation overflow".into()))?;
        Ok(())
    }
}

impl StageDirectory {
    pub(in crate::out_of_core) fn cleanup(&mut self) -> bool {
        let Some(ticket) = self.ticket.take() else {
            return false;
        };
        let registration = self.registration.take().expect("registered private stage");
        let mut attempt = CleanupAttempt::new(registration, ticket);
        attempt.ticket().error = Some(CleanupFailure::Other);
        let pending = match attempt.ticket().remove_batches(usize::MAX) {
            Ok(true) => {
                attempt.complete();
                false
            }
            result => {
                attempt.ticket().error = result.err().map(CleanupFailure::from);
                drop(attempt);
                true
            }
        };
        // Idle debt owns only its accounted metadata and disk reservation.
        // Retries acquire fresh workspace and the current project FD domain.
        self.cleanup_memory.take();
        self.project.take();
        self.admission.take();
        pending
    }
}

impl Drop for StageDirectory {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_admission_failure_preserves_cleanup_disposition() {
        let root = crate::out_of_core::generation_writer::tests::test_dir("retry_admission");
        fs::create_dir_all(&root).unwrap();
        let project = ProjectFileDescriptors::acquire_existing(&root, 1).unwrap();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut stage = StageDirectory::create(&root, &memory, &task).unwrap();
        stage.reserve_disk(64);
        let path = stage.path.to_path_buf();
        fs::write(path.join("retained"), b"evidence").unwrap();
        let held = File::create(root.join("held")).unwrap();
        drop(stage);
        drop(held);

        for descriptor_denied in [true, false] {
            if !descriptor_denied {
                retry_registered(&root, 1, 0, &memory, &task).unwrap();
            }
            let before = retry_staging_cleanup(&root, 0).unwrap();
            assert_eq!(before.descriptor_denials, usize::from(descriptor_denied));
            assert_eq!(
                before.progress_limited_stages,
                usize::from(!descriptor_denied)
            );
            assert_eq!(before.blocked_stages, 0);
            for cancelled in [true, false] {
                let bytes = if cancelled { 8 * 1024 * 1024 } else { 1 };
                let retry_task = RuntimeTaskContext::default()
                    .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(bytes, 0));
                let retry_memory = BuildMemory::new(&retry_task).unwrap();
                if cancelled {
                    retry_task.cancellation().cancel();
                }
                assert!(retry_registered(&root, 1, 4, &retry_memory, &retry_task).is_err());
                let after = retry_staging_cleanup(&root, 0).unwrap();
                assert_eq!(after.pending_stages, 1);
                assert_eq!(after.blocked_stages, 0);
                assert_eq!(after.descriptor_denials, before.descriptor_denials);
                assert_eq!(after.descriptor_error, before.descriptor_error);
                assert_eq!(
                    after.progress_limited_stages,
                    before.progress_limited_stages
                );
                assert_eq!(after.retained_memory_bytes, before.retained_memory_bytes);
                assert_eq!(after.reserved_disk_bytes, 64);
                assert_eq!(fs::read(path.join("retained")).unwrap(), b"evidence");
                assert_eq!(retry_memory.ledger.snapshot().used_bytes, 0);
            }
        }
        assert_eq!(retry_staging_cleanup(&root, 1).unwrap().removed_stages, 1);
        assert!(!path.exists());
        assert_eq!(project.metrics().open, 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn full_registry_does_not_retry_a_closed_foreign_project() {
        const CHILD: &str = "HAWDB_STAGE_REGISTRY_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // Saturate the process-wide registry without affecting concurrent tests.
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    concat!(
                        module_path!(),
                        "::full_registry_does_not_retry_a_closed_foreign_project"
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
        let root = crate::out_of_core::generation_writer::tests::test_dir("full_stage_registry");
        let foreign = root.join("foreign");
        let target = root.join("target");
        fs::create_dir_all(&foreign).unwrap();
        fs::create_dir(&target).unwrap();
        let project = ProjectFileDescriptors::acquire_existing(&foreign, 1).unwrap();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let stage = StageDirectory::create(&foreign, &memory, &task).unwrap();
        let path = stage.path.to_path_buf();
        fs::write(path.join("retained"), b"evidence").unwrap();
        let held = File::create(foreign.join("held")).unwrap();
        drop(stage);
        drop(held);
        drop(project);
        let registrations = (0..MAX_ROOT_OWNERS)
            .map(|_| StageDirectory::create(&target, &memory, &task).unwrap())
            .collect::<Vec<_>>();
        let target_paths = fs::read_dir(&target)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(target_paths.len(), MAX_ROOT_OWNERS);

        let error = StageDirectory::create(&target, &memory, &task)
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("cleanup owner capacity exhausted"));
        assert_eq!(
            fs::read_dir(&target)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<std::collections::BTreeSet<_>>(),
            target_paths
        );
        assert_eq!(fs::read(path.join("retained")).unwrap(), b"evidence");
        let reopened = ProjectFileDescriptors::acquire_existing(&foreign, 8).unwrap();
        assert_eq!(
            retry_staging_cleanup(&foreign, 1).unwrap().removed_stages,
            1
        );
        assert_eq!(reopened.metrics().open, 0);
        assert!(!path.exists());
        drop(registrations);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn automatic_cleanup_bounds_each_attempt_and_preserves_remaining_ownership() {
        let root = crate::out_of_core::generation_writer::tests::test_dir("bounded_stage_cleanup");
        fs::create_dir_all(&root).unwrap();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let stage = StageDirectory::create(&root, &memory, &task).unwrap();
        for index in 0..33 {
            fs::write(stage.path.join(format!("partial-{index}")), b"partial").unwrap();
        }
        // Keep this ticket active so unrelated concurrent admissions cannot
        // accelerate cleanup and invalidate the per-attempt observation.
        let ticket = stage.ticket.as_ref().unwrap();
        for remaining in [17, 1, 0] {
            assert_eq!(
                ticket.remove_batches(AUTOMATIC_CLEANUP_BATCHES).unwrap(),
                remaining == 0
            );
            if remaining == 0 {
                assert!(!stage.path.exists());
            } else {
                assert_eq!(fs::read_dir(&stage.path).unwrap().count(), remaining);
                assert!(memory.ledger.snapshot().used_bytes > 0);
            }
        }
        drop(stage);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
pub(super) mod evidence {
    use std::path::{Path, PathBuf};
    use std::sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    };
    struct Hook {
        root: PathBuf,
        started: Sender<()>,
        resume: Receiver<()>,
    }
    static HOOK: Mutex<Option<Hook>> = Mutex::new(None);
    struct UnlinkFailure {
        path: PathBuf,
        attempts: usize,
    }
    static UNLINK_FAILURE: Mutex<Option<UnlinkFailure>> = Mutex::new(None);

    pub(in crate::out_of_core) struct UnlinkFailureGuard {
        path: PathBuf,
    }
    impl UnlinkFailureGuard {
        pub(in crate::out_of_core) fn attempts(&self) -> usize {
            let failure = UNLINK_FAILURE.lock().unwrap();
            let failure = failure.as_ref().expect("installed unlink failure");
            assert_eq!(failure.path, self.path);
            failure.attempts
        }
    }
    impl Drop for UnlinkFailureGuard {
        fn drop(&mut self) {
            let mut failure = UNLINK_FAILURE.lock().unwrap();
            if failure
                .as_ref()
                .is_some_and(|failure| failure.path == self.path)
            {
                failure.take();
            }
        }
    }
    pub(in crate::out_of_core) fn fail_unlink(path: &Path) -> UnlinkFailureGuard {
        let path = std::fs::canonicalize(path).unwrap();
        let mut failure = UNLINK_FAILURE.lock().unwrap();
        assert!(failure.is_none());
        *failure = Some(UnlinkFailure {
            path: path.clone(),
            attempts: 0,
        });
        UnlinkFailureGuard { path }
    }
    pub(in crate::out_of_core) fn install(
        root: PathBuf,
        started: Sender<()>,
        resume: Receiver<()>,
    ) {
        *HOOK.lock().unwrap() = Some(Hook {
            root: std::fs::canonicalize(root).unwrap(),
            started,
            resume,
        });
    }
    pub(super) fn before_unlink(path: &Path) -> std::io::Result<()> {
        let hook = {
            let mut hook = HOOK.lock().unwrap();
            if hook
                .as_ref()
                .is_some_and(|hook| path.starts_with(&hook.root))
            {
                hook.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            hook.started.send(()).unwrap();
            hook.resume.recv().unwrap();
        }
        let mut failure = UNLINK_FAILURE.lock().unwrap();
        if let Some(failure) = failure.as_mut()
            && failure.path == path
        {
            failure.attempts += 1;
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        Ok(())
    }
}
