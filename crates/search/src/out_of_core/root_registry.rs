// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Private native custody, separate from publication exclusion and artifact pins.
//! A cooperating producer/GC protocol is still required before serving activation.

use crate::build_memory::{
    read::{ReadAdmission, ReaderMemory},
    shared::Shared,
    BuildMemory,
};
use crate::error::{HawDBError, Result};
use hawdb_executor::QueryMemoryLease;
use hawdb_storage::file_descriptors::FileOpenContext;
use hawdb_storage::file_io::{
    File, SourceDirectory, SourceDirectoryIdentity, SourceDirectoryLockMode,
};
use std::collections::LinkedList;
use std::mem::size_of;
use std::path::Path;
use std::sync::{Arc, RwLock};

mod gate;
mod pins;
pub(super) use pins::{ArtifactPin, PendingCapture};

const PROCESS_LOCK: &str = ".search-source-process.lock";
static ROOTS: gate::Gate<Registry> = gate::Gate::new(Registry {
    roots: LinkedList::new(),
    publishers: LinkedList::new(),
});

struct Registry {
    roots: LinkedList<Entry>,
    publishers: LinkedList<PublisherEntry>,
}
impl std::ops::Deref for Registry {
    type Target = LinkedList<Entry>;
    fn deref(&self) -> &Self::Target {
        &self.roots
    }
}
impl std::ops::DerefMut for Registry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.roots
    }
}

struct PublisherEntry {
    identity: SourceDirectoryIdentity,
    _memory: QueryMemoryLease,
}

/// Publication and root initialization share one structural gate. A publisher
/// borrows an existing root's process lock or owns the sole temporary handle;
/// it never opens a second same-process handle to a retained root's lock inode.
pub(super) struct PublicationRegistration {
    // Native process custody closes before its directory; the marker is last.
    custody: PublicationCustody,
    _marker: PublisherMarker,
}

enum PublicationCustody {
    Borrowed(RootRef),
    Native(NativeResources),
}

struct PublisherMarker {
    identity: SourceDirectoryIdentity,
}

impl std::fmt::Debug for PublicationRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationRegistration")
            .field("identity", &self._marker.identity)
            .finish_non_exhaustive()
    }
}

impl PublicationRegistration {
    pub(super) fn acquire(
        directory: SourceDirectory,
        memory: &BuildMemory,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<Self> {
        crate::build_control::checkpoint(task)?;
        let charge = memory
            .retained
            .reserve(size_of::<PublisherEntry>() + 2 * size_of::<usize>() + directory_bytes())?;
        let identity = directory.identity();
        let borrowed = {
            let mut registry = registry();
            if registry
                .publishers
                .iter()
                .any(|entry| entry.identity == identity)
            {
                return Err(HawDBError::ProjectionBusy);
            }
            let borrowed = match registry
                .iter()
                .find(|entry| entry.state.identity == identity)
            {
                Some(entry) if entry.phase == Phase::Ready => {
                    let native = entry.state.native.read().unwrap_or_else(|p| p.into_inner());
                    if !native
                        .as_ref()
                        .expect("ready root owns native resources")
                        .directory
                        .shares_project_context(&directory.operation_context())
                    {
                        return Err(HawDBError::Storage(
                            "publication root belongs to another project descriptor domain".into(),
                        ));
                    }
                    Some(RootRef(Some(entry.state.clone())))
                }
                Some(_) => {
                    return Err(HawDBError::Storage(
                        "source root is initializing or retiring".into(),
                    ))
                }
                None => None,
            };
            registry.publishers.push_back(PublisherEntry {
                identity,
                _memory: charge,
            });
            borrowed
        };
        let marker = PublisherMarker { identity };
        let custody = match borrowed {
            Some(root) => {
                // The borrowed root already owns this exact captured directory.
                // Return the redundant capture outside the structural gate.
                drop(directory);
                PublicationCustody::Borrowed(root)
            }
            None => {
                let file = directory.open_lock_file(
                    &directory.operation_context(),
                    PROCESS_LOCK,
                    SourceDirectoryLockMode::ReadWriteCreate,
                )?;
                #[cfg(test)]
                tests::after_process_open(&file);
                let file = file.try_lock_owned().map_err(|error| match error {
                    std::fs::TryLockError::WouldBlock => {
                        HawDBError::Storage("source root is owned by another process".into())
                    }
                    std::fs::TryLockError::Error(error) => error.into(),
                })?;
                PublicationCustody::Native(NativeResources {
                    process_lock: Some(file),
                    directory: Arc::new(directory),
                })
            }
        };
        Ok(Self {
            custody,
            _marker: marker,
        })
    }

    pub(super) fn identity(&self) -> SourceDirectoryIdentity {
        self._marker.identity
    }

    pub(super) fn protects(&self, key: super::artifact_key::ArtifactKey) -> bool {
        let roots = registry();
        roots
            .iter()
            .find(|entry| entry.state.identity == self._marker.identity)
            .is_some_and(|entry| entry.phase != Phase::Ready || entry.pins.protects(key))
    }

    pub(super) fn with_directory<T>(&self, action: impl FnOnce(&SourceDirectory) -> T) -> T {
        match &self.custody {
            PublicationCustody::Native(native) => action(&native.directory),
            PublicationCustody::Borrowed(root) => {
                let native = root
                    .state()
                    .native
                    .read()
                    .unwrap_or_else(|p| p.into_inner());
                action(
                    &native
                        .as_ref()
                        .expect("live root owns its directory")
                        .directory,
                )
            }
        }
    }

    pub(super) fn borrow_admitted_root(&self, admission: &ReadAdmission<'_>) -> Result<RootRef> {
        match &self.custody {
            PublicationCustody::Borrowed(root)
                if root
                    .state()
                    ._memory
                    .shares_governor_with(admission.require_governor()?) =>
            {
                Ok(root.clone())
            }
            PublicationCustody::Borrowed(_) => Err(HawDBError::Execution(
                "cleanup root belongs to another governor".into(),
            )),
            PublicationCustody::Native(_) => Err(HawDBError::Execution(
                "cleanup needs resource-admitted native custody".into(),
            )),
        }
    }

    /// Promote an exclusively borrowed publisher's existing native resources.
    /// Admission and cancellation happen before mutation; failure leaves the
    /// original process lock and directory in the publisher. No close/reopen,
    /// new FD, retained execution permit or new governor is involved.
    pub(super) fn admit_root(
        &mut self,
        context: &FileOpenContext,
        admission: &ReadAdmission<'_>,
    ) -> Result<RootRef> {
        admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        if !self.with_directory(|directory| directory.shares_project_context(context)) {
            return Err(HawDBError::Storage(
                "publication root belongs to another project descriptor domain".into(),
            ));
        }
        if let PublicationCustody::Borrowed(root) = &self.custody {
            if !root
                .state()
                ._memory
                .shares_governor_with(admission.require_governor()?)
            {
                return Err(HawDBError::Storage(
                    "source root belongs to another runtime governor".into(),
                ));
            }
            return Ok(root.clone());
        }
        admission.require_governor()?;
        let state_memory = admission.retained(state_bytes())?;
        let entry_memory =
            admission.retained(size_of::<Entry>() + 2 * size_of::<usize>() + pins::BASE_BYTES)?;
        let state = Shared::new(State {
            identity: self._marker.identity,
            native: RwLock::new(None),
            _memory: state_memory,
        });
        admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        // Only this exclusive publisher can promote its marker. A concurrent
        // RootRef acquisition refuses while its native-only marker is present.
        // Prepare the empty candidate's native guard before entering ROOTS.
        let mut native = state.native.write().unwrap_or_else(|p| p.into_inner());
        {
            let mut roots = registry();
            assert!(roots
                .iter()
                .all(|entry| entry.state.identity != state.identity));
            assert!(roots
                .publishers
                .iter()
                .any(|entry| entry.identity == state.identity));
            roots.push_back(Entry {
                state: state.clone(),
                phase: Phase::Ready,
                pins: pins::PinTable::default(),
                _memory: entry_memory,
            });
            let previous = std::mem::replace(
                &mut self.custody,
                PublicationCustody::Borrowed(RootRef(Some(state.clone()))),
            );
            let PublicationCustody::Native(resources) = previous else {
                unreachable!("exclusive native-only publisher promotes once");
            };
            *native = Some(resources);
            // Ready lookup takes a read guard under ROOTS. Release this new
            // state's writer before any such lookup can observe Ready.
            drop(native);
        }
        Ok(RootRef(Some(state)))
    }
}

impl Drop for PublisherMarker {
    fn drop(&mut self) {
        let removed = {
            let mut registry = registry();
            let position = registry
                .publishers
                .iter()
                .position(|entry| entry.identity == self.identity)
                .expect("live publisher remains registered");
            let mut tail = registry.publishers.split_off(position);
            let removed = tail.pop_front();
            registry.publishers.append(&mut tail);
            removed
        };
        drop(removed);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Initializing,
    Ready,
    Retiring,
}

struct Entry {
    state: Shared<State>,
    phase: Phase,
    pins: pins::PinTable,
    // pop_front frees the node before this receipt is released outside the gate.
    _memory: ReaderMemory,
}

struct NativeResources {
    // Closing the process lock must precede dropping its captured directory.
    process_lock: Option<hawdb_storage::file_io::PublicationLockGuard>,
    // Shared only with logical files whose exact artifact pin retains State.
    directory: Arc<SourceDirectory>,
}
impl Drop for NativeResources {
    fn drop(&mut self) {
        // Keys retain memory only. Close idle native children before releasing
        // process custody; all in-flight/logical users must retain a root pin.
        self.directory.retire_cached_children();
        #[cfg(test)]
        if self.process_lock.is_some() {
            tests::hook(&tests::BEFORE_PROCESS_UNLOCK);
        }
    }
}

const fn directory_bytes() -> usize {
    size_of::<SourceDirectory>() + 2 * size_of::<usize>()
}
const fn state_bytes() -> usize {
    size_of::<State>() + 2 * size_of::<usize>() + directory_bytes()
}

struct State {
    identity: SourceDirectoryIdentity,
    native: RwLock<Option<NativeResources>>,
    // SharedAllocation frees its control block before this payload receipt.
    _memory: ReaderMemory,
}

pub(super) struct RootRef(Option<Shared<State>>);

impl RootRef {
    pub(super) fn validation_stage(
        &self,
        label: &Path,
        admission: &ReadAdmission<'_>,
    ) -> Result<super::generation_writer::StageDirectory> {
        if !self
            .state()
            ._memory
            .shares_governor_with(admission.require_governor()?)
        {
            return Err(HawDBError::Execution(
                "private scratch belongs to another governor".into(),
            ));
        }
        let parent = {
            let native = self
                .state()
                .native
                .read()
                .unwrap_or_else(|p| p.into_inner());
            Arc::clone(
                &native
                    .as_ref()
                    .expect("live root owns its directory")
                    .directory,
            )
        };
        super::generation_writer::StageDirectory::create_captured(
            label,
            &parent,
            admission.memory,
            admission.task,
            Some(admission),
        )
    }

    /// A born immutable generation cannot carry mutable repair/reindex state.
    /// Preserve native lookup errors; only NotFound proves absence.
    pub(super) fn require_absent_control_child(
        &self,
        context: &FileOpenContext,
        name: &str,
        admission: &ReadAdmission<'_>,
    ) -> Result<()> {
        let _io = admission
            .io()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let file = {
            let native = self
                .state()
                .native
                .read()
                .unwrap_or_else(|p| p.into_inner());
            native
                .as_ref()
                .expect("live root owns its directory")
                .directory
                .open_child(context, name)
        };
        match file {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
            Ok(_) => Err(HawDBError::StorageIntegrity(format!(
                "immutable Consumer candidate contains operational marker {name}"
            ))),
        }
    }

    /// Open relative to the retained directory, then return the opening wave
    /// before admitting the held-file read. A pathname replacement cannot
    /// redirect the selected head, and a one-slot operation cannot self-wait.
    #[cfg(test)]
    pub(super) fn read_head<'execution>(
        &self,
        context: &FileOpenContext,
        name: &str,
        admission: &ReadAdmission<'execution>,
        maximum: u64,
    ) -> Result<crate::build_memory::read::ReadBytes<'execution>> {
        let file = self.open_head(context, name, admission)?;
        admission.read_unbound_file(&file, maximum)
    }

    pub(super) fn open_head(
        &self,
        context: &FileOpenContext,
        name: &str,
        admission: &ReadAdmission<'_>,
    ) -> Result<File> {
        let file = {
            let _io = admission
                .io()
                .map_err(|error| HawDBError::Execution(error.to_string()))?;
            let native = self
                .state()
                .native
                .read()
                .unwrap_or_else(|p| p.into_inner());
            native
                .as_ref()
                .expect("live root owns its directory")
                .directory
                .open_child(context, name)?
        };
        Ok(file)
    }

    pub(super) fn acquire(
        root: &Path,
        context: &FileOpenContext,
        admission: &ReadAdmission<'_>,
    ) -> Result<Self> {
        admission.require_governor()?;
        let state_memory = admission.retained(state_bytes())?;
        let entry_memory =
            admission.retained(size_of::<Entry>() + 2 * size_of::<usize>() + pins::BASE_BYTES)?;
        let directory = {
            let _io = admission
                .io()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            SourceDirectory::capture(root, context)?
        };
        let identity = directory.identity();
        let state = Shared::new(State {
            identity,
            native: RwLock::new(Some(NativeResources {
                process_lock: None,
                directory: Arc::new(directory),
            })),
            _memory: state_memory,
        });
        let existing = {
            let mut roots = registry();
            match roots.iter().find(|entry| entry.state.identity == identity) {
                Some(entry) if entry.phase == Phase::Ready => {
                    // Ready resources have no writer: retirement first changes
                    // phase under this same registry gate. This read performs
                    // only a pointer comparison, never native I/O.
                    let native = entry.state.native.read().unwrap_or_else(|p| p.into_inner());
                    if !native
                        .as_ref()
                        .expect("ready root owns resources")
                        .directory
                        .shares_project_context(context)
                    {
                        return Err(HawDBError::Storage(
                            "source root belongs to another project descriptor domain".into(),
                        ));
                    }
                    if !entry
                        .state
                        ._memory
                        .shares_governor_with(admission.require_governor()?)
                    {
                        return Err(HawDBError::Storage(
                            "source root belongs to another runtime governor".into(),
                        ));
                    }
                    Some(entry.state.clone())
                }
                Some(_) => {
                    return Err(HawDBError::Storage(
                        "source root is initializing or retiring".into(),
                    ))
                }
                None => {
                    if roots
                        .publishers
                        .iter()
                        .any(|entry| entry.identity == identity)
                    {
                        return Err(HawDBError::Storage("publication owns source custody; borrow its existing root or retry after publication".into()));
                    }
                    roots.push_back(Entry {
                        state: state.clone(),
                        phase: Phase::Initializing,
                        pins: pins::PinTable::default(),
                        _memory: entry_memory,
                    });
                    None
                }
            }
        };
        if let Some(existing) = existing {
            // Candidate directory and unused receipts close outside the gate.
            return Ok(Self(Some(existing)));
        }
        // This guard also retires a failed/cancelled/unwinding initialization.
        let owner = Self(Some(state));
        #[cfg(test)]
        tests::hook(&tests::AFTER_INITIALIZING);
        {
            let _io = admission
                .io()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            let mut native = owner
                .state()
                .native
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let native = native
                .as_mut()
                .expect("initializing root owns native resources");
            let lock = native.directory.open_lock_file(
                context,
                PROCESS_LOCK,
                SourceDirectoryLockMode::ReadWriteCreate,
            )?;
            #[cfg(test)]
            tests::after_process_open(&lock);
            native.process_lock = Some(lock.try_lock_owned().map_err(|error| match error {
                std::fs::TryLockError::WouldBlock => {
                    HawDBError::Storage("source root is owned by another process".into())
                }
                std::fs::TryLockError::Error(error) => error.into(),
            })?);
        }
        admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        {
            let mut roots = registry();
            let entry = roots
                .iter_mut()
                .find(|entry| entry.state.identity == identity)
                .expect("initializing entry remains registered");
            entry.phase = Phase::Ready;
        }
        Ok(owner)
    }

    fn state(&self) -> &Shared<State> {
        self.0.as_ref().expect("live root reference")
    }
}

impl Clone for RootRef {
    fn clone(&self) -> Self {
        // A live external reference prevents retirement; the gate coordinates
        // this increment with nonfinal decrements and the final count check.
        let _roots = registry();
        Self(Some(self.state().clone()))
    }
}

impl Drop for RootRef {
    fn drop(&mut self) {
        let state = self.0.take().expect("root reference dropped once");
        {
            let mut roots = registry();
            if state.strong_count() > 2 {
                // Registry + another external owner guarantee no payload dies
                // here. Moving this decrement off-gate leaks on concurrent drop.
                drop(state);
                return;
            }
            assert_eq!(state.strong_count(), 2);
            let entry = roots
                .iter_mut()
                .find(|entry| entry.state.identity == state.identity)
                .expect("live root remains registered");
            assert!(
                entry.pins.is_empty(),
                "live pins retain external root references"
            );
            entry.phase = Phase::Retiring;
        }
        #[cfg(test)]
        tests::hook(&tests::AFTER_RETIRING);
        let native = state
            .native
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        drop(native);
        #[cfg(test)]
        tests::hook(&tests::AFTER_NATIVE_CLOSE);
        let removed = {
            let mut roots = registry();
            let position = roots
                .iter()
                .position(|entry| entry.state.identity == state.identity)
                .expect("retiring root remains registered");
            let mut tail = roots.split_off(position);
            let removed = tail.pop_front();
            roots.append(&mut tail);
            removed
        };
        // No recursive destruction, native close or final Shared drop under gate.
        drop(removed);
        drop(state);
    }
}

fn registry() -> gate::Guard<'static, Registry> {
    gate::lock(&ROOTS)
}

#[cfg(test)]
mod tests;
