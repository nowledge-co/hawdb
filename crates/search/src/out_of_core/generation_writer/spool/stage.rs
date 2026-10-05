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
use crate::build_memory::path::OwnedPath;
use hawdb_executor::QueryMemoryLease;
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use std::sync::{Arc, Mutex};

// Admission happens before any private directory exists. Retention needs no
// allocation during Drop, cancellation, unwind, or descriptor exhaustion.
const MAX_OWNERS: usize = 256;
static OWNERS: Mutex<[Slot; MAX_OWNERS]> = Mutex::new([const { Slot::Vacant }; MAX_OWNERS]);

enum Slot {
    Vacant,
    Active,
    Pending(Ticket),
}

struct Registration {
    index: usize,
}
impl Registration {
    fn acquire() -> Result<Self> {
        let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
        let index = owners
            .iter()
            .position(|slot| matches!(slot, Slot::Vacant))
            .ok_or_else(|| {
                HawDBError::Execution(
                    "search private-stage cleanup owner capacity exhausted".into(),
                )
            })?;
        owners[index] = Slot::Active;
        Ok(Self { index })
    }
    fn retain(self, ticket: Ticket) {
        OWNERS.lock().unwrap_or_else(|error| error.into_inner())[self.index] =
            Slot::Pending(ticket);
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(owners[self.index], Slot::Active) {
            owners[self.index] = Slot::Vacant;
        }
    }
}

struct Ticket {
    value: Box<TicketFields>,
    _memory: QueryMemoryLease,
    _admission: Option<Arc<hawdb_qos::RuntimePermit>>,
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
    _project: ProjectFileDescriptors,
    disk_reservation: u64,
    error: Option<HawDBError>,
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
    pub descriptor_error: Option<hawdb_core::error::FileDescriptorError>,
}

pub(in crate::out_of_core) fn retry_staging_cleanup(
    root: &Path,
    max_attempts: usize,
) -> Result<SearchStagingCleanupReport> {
    let mut report = SearchStagingCleanupReport::default();
    for index in 0..MAX_OWNERS {
        let ticket = {
            let mut owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
            match &owners[index] {
                Slot::Pending(ticket)
                    if ticket.path.parent() == Some(root)
                        && report.attempted_stages < max_attempts =>
                {
                    match std::mem::replace(&mut owners[index], Slot::Active) {
                        Slot::Pending(ticket) => Some(ticket),
                        _ => unreachable!(),
                    }
                }
                _ => None,
            }
        };
        if let Some(mut ticket) = ticket {
            report.attempted_stages += 1;
            match ticket.remove() {
                Ok(()) => {
                    report.removed_stages += 1;
                    drop(ticket);
                    OWNERS.lock().unwrap_or_else(|error| error.into_inner())[index] = Slot::Vacant;
                }
                Err(error) => {
                    ticket.error = Some(error);
                    OWNERS.lock().unwrap_or_else(|error| error.into_inner())[index] =
                        Slot::Pending(ticket);
                }
            }
        }
    }
    let owners = OWNERS.lock().unwrap_or_else(|error| error.into_inner());
    for slot in owners.iter() {
        if let Slot::Pending(ticket) = slot
            && ticket.path.parent() == Some(root)
        {
            report.pending_stages += 1;
            report.reserved_disk_bytes = report
                .reserved_disk_bytes
                .saturating_add(ticket.disk_reservation);
            report.retained_memory_bytes = report
                .retained_memory_bytes
                .saturating_add(ticket._memory.bytes());
            if let Some(HawDBError::FileDescriptors(error)) = &ticket.error {
                report.descriptor_denials += 1;
                report.descriptor_error = Some(error.clone());
            }
        }
    }
    Ok(report)
}

impl Ticket {
    fn remove(&self) -> Result<()> {
        // Cleanup is resumable: each counted step can defer without losing the
        // owner. Close the iterator before unlinking its bounded batch.
        // No operation-wide descriptor reservation or busy retry is needed.
        match fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            result => {
                result?;
            }
        }
        loop {
            let mut paths: [Option<(std::path::PathBuf, bool)>; 4] = Default::default();
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
                return Ok(());
            }
            for (path, directory) in paths.into_iter().flatten() {
                if directory {
                    return Err(HawDBError::Storage(
                        "search private stage contains an unexpected directory".into(),
                    ));
                }
                #[cfg(test)]
                evidence::before_unlink(&path);
                fs::remove_file(path)?;
            }
        }
    }
}

pub(in crate::out_of_core) struct StageDirectory {
    pub(in crate::out_of_core) path: super::super::context_memory::OwnedPath,
    registration: Option<Registration>,
    ticket: Option<Ticket>,
}

impl StageDirectory {
    pub(in crate::out_of_core) fn create(
        root: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let registration = Registration::acquire()?;
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
                root,
                Path::new(&name),
                memory,
                task,
            )?;
            let cleanup = memory.spool.reserve(crate::build_memory::checked_add(
                crate::build_memory::directory::stage_removal_bytes(&path)?,
                std::mem::size_of::<TicketFields>(),
            )?)?;
            let ticket_path = OwnedPath::copy(&path, memory, task)?;
            let project = ProjectFileDescriptors::acquire_component(root, false)?;
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
                                _project: project,
                                disk_reservation: 0,
                                error: None,
                            }),
                            _memory: cleanup,
                            _admission: memory.host_admission.clone(),
                        }),
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
        let Some(mut ticket) = self.ticket.take() else {
            return false;
        };
        let registration = self.registration.take().expect("registered private stage");
        if let Err(error) = ticket.remove() {
            ticket.error = Some(error);
            registration.retain(ticket);
            true
        } else {
            false
        }
    }
}

impl Drop for StageDirectory {
    fn drop(&mut self) {
        self.cleanup();
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
    pub(in crate::out_of_core) fn install(
        root: PathBuf,
        started: Sender<()>,
        resume: Receiver<()>,
    ) {
        *HOOK.lock().unwrap() = Some(Hook {
            root,
            started,
            resume,
        });
    }
    pub(super) fn before_unlink(path: &Path) {
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
    }
}
