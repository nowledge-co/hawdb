// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::build_memory::read::ReadAdmission;
use crate::out_of_core::scratch_directory::{io_scope, Funding};
use crate::out_of_core::ScratchDirectory;
use hawdb_storage::file_io::{CapturedDirectoryCandidate, SourceDirectory};

#[cfg(test)]
type AfterCreateHook = std::cell::RefCell<Option<Box<dyn FnOnce() -> Result<()>>>>;
#[cfg(test)]
thread_local! {
    static AFTER_CREATE: AfterCreateHook =
        const { std::cell::RefCell::new(None) };
}

pub(super) struct Custody {
    parent: SourceDirectory,
    pub(super) candidate: CapturedDirectoryCandidate,
    namespace: Option<Arc<ScratchDirectory>>,
    prepared: Option<(OwnedPath, Funding)>,
    pub(super) governed: bool,
}
impl Custody {
    fn namespace(&mut self) -> Result<Arc<ScratchDirectory>> {
        if self.namespace.is_none() {
            let child = self
                .parent
                .open_created_directory(&self.parent.operation_context(), &self.candidate)?;
            let (label, funding) = self.prepared.take().expect("admitted namespace metadata");
            self.namespace = Some(Arc::new(ScratchDirectory::new(child, label, funding)));
        }
        Ok(self.namespace.as_ref().unwrap().clone())
    }
    pub(super) fn remove_batches(&mut self, max_batches: usize) -> Result<bool> {
        if self.candidate.removal_completed() || !self.candidate.may_own_entry() {
            return Ok(true);
        }
        if !self.candidate.creation_completed() {
            return Err(HawDBError::Storage(
                "uncertain private directory creation remains quarantined".into(),
            ));
        }
        for _ in 0..max_batches {
            let namespace = self.namespace()?;
            let mut names = [[0u8; 256]; directory::STAGE_REMOVAL_BATCH_ENTRIES];
            let mut lengths = [0usize; directory::STAGE_REMOVAL_BATCH_ENTRIES];
            let mut count = 0;
            let mut full = false;
            let scan = namespace.native().visit_children(
                &namespace.native().operation_context(),
                usize::MAX,
                |name| {
                    if count == names.len() {
                        full = true;
                        return Err(io::Error::from(io::ErrorKind::Interrupted));
                    }
                    names[count][..name.len()].copy_from_slice(name.as_bytes());
                    lengths[count] = name.len();
                    count += 1;
                    Ok(())
                },
            );
            if !(full
                && scan
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::Interrupted))
            {
                scan?;
            }
            if count == 0 {
                drop(namespace);
                self.namespace.take();
                self.parent.remove_created_directory(
                    &self.parent.operation_context(),
                    &mut self.candidate,
                )?;
                return Ok(true);
            }
            // Prove this batch contains only regular private files before unlink.
            for i in 0..count {
                let name =
                    std::str::from_utf8(&names[i][..lengths[i]]).expect("generated ASCII name");
                drop(
                    namespace
                        .native()
                        .open_child(&namespace.native().operation_context(), name)?,
                );
            }
            for i in 0..count {
                let name =
                    std::str::from_utf8(&names[i][..lengths[i]]).expect("generated ASCII name");
                namespace
                    .native()
                    .remove_child(&namespace.native().operation_context(), name)?;
            }
        }
        Ok(false)
    }
}

impl StageDirectory {
    pub(in crate::out_of_core) fn create_captured(
        label: &Path,
        parent: &SourceDirectory,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        admission: Option<&ReadAdmission<'_>>,
    ) -> Result<Self> {
        let governed = admission.and_then(ReadAdmission::runtime_permit).is_some();
        if let Some(admission) = admission {
            admission.validate_mode()?;
        } else if task.has_resource_controllers() || memory.host_admission.is_some() {
            return Err(HawDBError::Execution(
                "captured validation scratch requires originating admission".into(),
            ));
        }
        // No pathname canonicalization, project acquisition or path-backed retry.
        let root = OwnedPath::absolute(label, memory, task)?;
        for _ in 0..64 {
            checkpoint(task)?;
            let _name = memory.spool.reserve(128)?;
            let sequence = GENERATION_WRITER_SEQUENCE
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| HawDBError::Execution("private stage name exhausted".into()))?;
            let name = format!(
                ".search-generation.{}.{}.stage",
                std::process::id(),
                sequence
            );
            let path = OwnedPath::join(&root, Path::new(&name), memory, task)?;
            let ticket_path = OwnedPath::copy(&path, memory, task)?;
            let source_root = OwnedPath::copy(&root, memory, task)?;
            let owner_root = OwnedPath::copy(&root, memory, task)?;
            let namespace_label = OwnedPath::copy(&path, memory, task)?;
            let metadata_bytes = std::mem::size_of::<TicketFields>()
                + std::mem::size_of::<Custody>()
                + std::mem::size_of::<Owner>()
                + crate::build_memory::MAP_ENTRY_BYTES
                + CLEANUP_ACCOUNT_METADATA_BYTES
                + parent.metadata_bound(0)?;
            let other_bytes = ticket_path.retained_bytes()
                + source_root.retained_bytes()
                + owner_root.retained_bytes();
            let funding = Funding::new(memory, task, admission, metadata_bytes, other_bytes)?;
            let namespace_bytes = std::mem::size_of::<ScratchDirectory>()
                + 2 * std::mem::size_of::<usize>()
                + parent.metadata_bound(name.len())?;
            let namespace_funding = Funding::new(
                memory,
                task,
                admission,
                namespace_bytes,
                namespace_label.retained_bytes(),
            )?;
            let cleanup = memory
                .spool
                .reserve(directory::stage_removal_bytes(&path)?)?;
            let _io = io_scope(task, governed)?;
            let retained_parent = parent.duplicate(&parent.operation_context())?;
            let candidate = retained_parent.prepare_directory_candidate(&name)?;
            let registration = Registration::acquire(Owner {
                root: owner_root,
                slot: Slot::Active,
                last_retry: 0,
                _memory: memory.retained.reserve(0)?,
                _host_memory: None,
                captured_funding: Some(funding),
            })?;
            let mut attempt = CleanupAttempt::new(
                registration,
                Ticket {
                    value: Box::new(TicketFields {
                        path: ticket_path,
                        source_root,
                        disk_reservation: 0,
                        error: Some(CleanupFailure::Other),
                        captured: Some(Box::new(Custody {
                            parent: retained_parent,
                            candidate,
                            namespace: None,
                            prepared: Some((namespace_label, namespace_funding)),
                            governed,
                        })),
                    }),
                },
            );
            let custody = attempt.ticket().captured.as_mut().unwrap();
            match custody.parent.create_directory_candidate(
                &custody.parent.operation_context(),
                &mut custody.candidate,
            ) {
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    attempt.complete();
                    continue;
                }
                result => result?,
            }
            #[cfg(test)]
            AFTER_CREATE
                .with(|hook| hook.borrow_mut().take())
                .map_or(Ok(()), |hook| hook())?;
            let namespace = custody.namespace()?;
            let stage = Self {
                path,
                registration: attempt.registration.take(),
                ticket: attempt.ticket.take(),
                cleanup_memory: Some(cleanup),
                project: None,
                admission: None,
                native: Some(namespace),
                captured_task: Some(task.clone()),
            };
            drop(attempt);
            drop(_io);
            checkpoint(task)?;
            return Ok(stage);
        }
        Err(HawDBError::Storage(
            "private captured stage name collisions exhausted".into(),
        ))
    }

    pub(in crate::out_of_core) fn scratch_directory(&self) -> Option<Arc<ScratchDirectory>> {
        self.native.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_memory::read::ReadPermit;
    use hawdb_core::RuntimeMemoryReservation;
    use hawdb_qos::RuntimeWorkRequest;

    #[test]
    fn lost_response_after_creation_retains_original_custody_and_retries_off_replacement_root() {
        for governed in [false, true] {
            let base = std::env::temp_dir().join(format!(
                "hawdb-captured-stage-debt-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            let project = ProjectFileDescriptors::acquire(&base, 16).unwrap();
            let root = base.join("selected");
            let moved = base.join("moved");
            std::fs::create_dir(&root).unwrap();
            let parent = SourceDirectory::capture(&root, &project.io_context()).unwrap();
            let governor = crate::build_memory::read::tests::governor();
            let memory = {
                let permit = governed.then(|| {
                    governor
                        .try_admit(
                            RuntimeWorkRequest::background_maintenance(8 << 20)
                                .with_io_wave_slots(1),
                        )
                        .unwrap()
                });
                let task = match &permit {
                    Some(permit) => permit.bind_task_context(Default::default()),
                    None => RuntimeTaskContext::default()
                        .with_memory_reservation(RuntimeMemoryReservation::new(8 << 20, 0)),
                };
                let memory = BuildMemory::new(&task).unwrap();
                let admission = permit.as_ref().map(|permit| ReadAdmission {
                    memory: &memory,
                    task: &task,
                    permit: ReadPermit::Governed(permit),
                });
                AFTER_CREATE.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(|| {
                        Err(HawDBError::Execution(
                            "injected lost response after mkdir".into(),
                        ))
                    }))
                });
                let error = StageDirectory::create_captured(
                    &root,
                    &parent,
                    &memory,
                    &task,
                    admission.as_ref(),
                )
                .err()
                .expect("injected refusal");
                assert!(error.to_string().contains("lost response after mkdir"));
                assert!(memory.ledger.snapshot().used_bytes > 0);
                memory
            };
            assert_eq!(governor.snapshot().active_background_tasks, 0);
            assert_eq!(governor.snapshot().active_background_io_slots, 0);
            let stage_name = {
                let owners = OWNERS.lock().unwrap();
                owners
                    .values()
                    .find_map(|owner| match &owner.slot {
                        Slot::Pending(ticket) if ticket.matches_root(&root) => {
                            Some(ticket.path.file_name().unwrap().to_owned())
                        }
                        _ => None,
                    })
                    .expect("retained native cleanup owner")
            };
            // No child was opened: this ordering reaches retry on Windows too.
            std::fs::rename(&root, &moved).unwrap();
            std::fs::create_dir_all(root.join(&stage_name)).unwrap();
            std::fs::write(root.join(&stage_name).join("foreign"), b"preserve B").unwrap();
            if governed {
                let report = retry_staging_cleanup(&root, 1).unwrap();
                assert_eq!(report.pending_stages, 1);
                assert_eq!(report.blocked_stages, 1);
                assert!(moved.join(&stage_name).is_dir());
                let permit = governor
                    .try_admit(
                        RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1),
                    )
                    .unwrap();
                let task = permit.bind_task_context(Default::default());
                let report = retry_staging_cleanup_with_context(&root, 1, &task).unwrap();
                assert_eq!(report.removed_stages, 1);
                assert_eq!(report.pending_stages, 0);
            } else {
                assert_eq!(retry_staging_cleanup(&root, 1).unwrap().removed_stages, 1);
            }
            assert!(!moved.join(&stage_name).exists());
            assert_eq!(
                std::fs::read(root.join(&stage_name).join("foreign")).unwrap(),
                b"preserve B"
            );
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            drop(parent);
            assert_eq!(project.metrics().open, 0);
            std::fs::remove_dir_all(base).unwrap();
        }
    }
}
