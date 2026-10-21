// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Admitted registrations, independent of producer and execution lifetimes.
use super::*;
use crate::out_of_core::{artifact_key::ArtifactKey, SearchProjectionPublishLease};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};

// Cover sparse/empty retained map capacity separately from removed values.
// Conservative node/split credits, not allocator-overhead or RSS qualification.
pub(super) const BASE_BYTES: usize = 2 * crate::build_memory::MAP_ENTRY_BYTES;
const NODE_BYTES: usize = 2 * crate::build_memory::MAP_ENTRY_BYTES;

#[derive(Default)]
pub(super) struct PinTable {
    pending: usize,
    artifacts: BTreeMap<ArtifactKey, PinEntry>,
}
struct PinEntry {
    owners: usize,
    _memory: ReaderMemory,
}
impl PinTable {
    pub(super) fn protects(&self, key: ArtifactKey) -> bool {
        self.pending != 0 || self.artifacts.contains_key(&key)
    }
    pub(super) fn is_empty(&self) -> bool {
        self.pending == 0 && self.artifacts.is_empty()
    }
}

/// Construction cannot escape its borrowed execution admission. GC protects
/// the entire captured root until exact owners have taken their registrations.
pub(in crate::out_of_core) struct PendingCapture<'a, 'execution> {
    root: RootRef,
    admission: &'a ReadAdmission<'execution>,
}

impl RootRef {
    /// A mutable successful publication lease is the authority to begin capture.
    /// The returned guard retains neither this lease nor its operation slot.
    pub(in crate::out_of_core) fn begin_capture<'a, 'execution>(
        self,
        publication: &mut SearchProjectionPublishLease,
        admission: &'a ReadAdmission<'execution>,
    ) -> Result<PendingCapture<'a, 'execution>> {
        self.begin_cleanup_capture(publication, admission)
    }

    /// Synchronous cleanup holds the publication borrow through validation and
    /// sweep; no read/serving capability is returned to its caller.
    pub(in crate::out_of_core) fn begin_cleanup_capture<'a, 'execution>(
        self,
        publication: &SearchProjectionPublishLease,
        admission: &'a ReadAdmission<'execution>,
    ) -> Result<PendingCapture<'a, 'execution>> {
        admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        if self.state().identity != publication.source_identity()
            || !self
                .state()
                ._memory
                .shares_governor_with(admission.require_governor()?)
        {
            return Err(HawDBError::StorageIntegrity(
                "capture source authority does not match its root".into(),
            ));
        }
        {
            let mut roots = registry();
            let entry = roots
                .iter_mut()
                .find(|entry| entry.state.identity == self.state().identity)
                .expect("live captured root remains registered");
            assert!(entry.phase == Phase::Ready);
            entry.pins.pending =
                entry.pins.pending.checked_add(1).ok_or_else(|| {
                    HawDBError::Execution("source capture count exhausted".into())
                })?;
        }
        Ok(PendingCapture {
            root: self,
            admission,
        })
    }
}

#[derive(Debug)]
struct FileCustody {
    _pin: ArtifactPin,
    memory: ReaderMemory,
}
impl hawdb_core::RuntimeMemoryPermit for FileCustody {
    fn bytes(&self) -> u64 {
        hawdb_core::RuntimeMemoryPermit::bytes(&self.memory)
    }
}

impl PendingCapture<'_, '_> {
    /// Validation scratch belongs to the original captured directory. Labels
    /// are diagnostic and cannot select a parent or create a new FD domain.
    pub(in crate::out_of_core) fn validation_stage(
        &self,
        label: &Path,
    ) -> Result<crate::out_of_core::generation_writer::StageDirectory> {
        self.root.validation_stage(label, self.admission)
    }

    /// Every lazy child owns an exact pin and retained memory, while cache keys
    /// own memory only. No execution/publication permit escapes this borrow.
    pub(in crate::out_of_core) fn open_artifact(
        &self,
        name: &str,
        reference: hawdb_storage::immutable_object::ObjectReference,
    ) -> Result<File> {
        let key = ArtifactKey::from_file_name(name)
            .ok_or_else(|| HawDBError::StorageIntegrity("invalid captured artifact name".into()))?;
        let pin = self.pin(key)?;
        let directory = {
            let native = self
                .root
                .state()
                .native
                .read()
                .unwrap_or_else(|p| p.into_inner());
            Arc::clone(
                &native
                    .as_ref()
                    .expect("live root owns native resources")
                    .directory,
            )
        };
        let key_memory = self
            .admission
            .retained(File::captured_immutable_key_bytes::<ReaderMemory>(name)?)?;
        let memory = self
            .admission
            .retained(File::captured_immutable_binding_bytes::<FileCustody>())?;
        let context = directory.operation_context();
        Ok(File::open_captured_immutable(
            directory,
            &context,
            name,
            reference,
            key_memory,
            FileCustody { _pin: pin, memory },
        )?)
    }

    pub(in crate::out_of_core) fn source_identity(
        &self,
    ) -> hawdb_storage::file_io::SourceDirectoryIdentity {
        self.root.state().identity
    }

    pub(in crate::out_of_core) fn root(&self) -> &RootRef {
        &self.root
    }

    pub(in crate::out_of_core) fn admission(&self) -> &ReadAdmission<'_> {
        self.admission
    }

    pub(in crate::out_of_core) fn pin_segment(
        &self,
        manifest: &crate::out_of_core::SearchOutOfCoreSegmentManifest,
    ) -> Result<[Option<ArtifactPin>; 3]> {
        let content = ArtifactKey::from_file_name(&manifest.descriptor_file)
            .filter(|key| matches!(key, ArtifactKey::Content(_)))
            .ok_or_else(|| HawDBError::StorageIntegrity("invalid content artifact key".into()))?;
        for name in [
            &manifest.payload_file,
            &manifest.metadata_payload_file,
            &manifest.vector_payload_file,
            &manifest.layout_file,
        ] {
            if ArtifactKey::from_file_name(name) != Some(content) {
                return Err(HawDBError::StorageIntegrity(
                    "content artifacts disagree on source identity".into(),
                ));
            }
        }
        let lexical = ArtifactKey::from_file_name(&manifest.lexical_manifest_file)
            .filter(|key| matches!(key, ArtifactKey::Lexical(_)))
            .ok_or_else(|| HawDBError::StorageIntegrity("invalid lexical artifact key".into()))?;
        let vector = manifest
            .rabitq_artifact_file
            .as_ref()
            .map(|name| {
                ArtifactKey::from_file_name(name)
                    .filter(|key| matches!(key, ArtifactKey::RaBitQ(_)))
                    .ok_or_else(|| {
                        HawDBError::StorageIntegrity("invalid vector artifact key".into())
                    })
            })
            .transpose()?;
        Ok([
            Some(self.pin(content)?),
            Some(self.pin(lexical)?),
            vector.map(|key| self.pin(key)).transpose()?,
        ])
    }

    pub(in crate::out_of_core) fn pin(&self, key: ArtifactKey) -> Result<ArtifactPin> {
        self.admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        let entry_memory = self.admission.retained(NODE_BYTES)?;
        let memory = self
            .admission
            .retained(size_of::<PinRecord>() + 2 * size_of::<usize>())?;
        // Clone outside ROOTS: RootRef cloning and final destruction use ROOTS.
        let record = Shared::new(PinRecord {
            root: self.root.clone(),
            key,
            registered: AtomicBool::new(false),
            _memory: memory,
        });
        self.admission
            .checkpoint()
            .map_err(|e| HawDBError::Execution(e.to_string()))?;
        let mut unused_memory = Some(entry_memory);
        {
            let mut roots = registry();
            let entry = roots
                .iter_mut()
                .find(|entry| entry.state.identity == self.root.state().identity)
                .expect("pending capture keeps its root registered");
            assert!(entry.pins.pending > 0);
            match entry.pins.artifacts.entry(key) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let owners = entry.get().owners.checked_add(1).ok_or_else(|| {
                        HawDBError::Execution("source pin count exhausted".into())
                    })?;
                    entry.get_mut().owners = owners;
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(PinEntry {
                        owners: 1,
                        _memory: unused_memory.take().unwrap(),
                    });
                }
            }
            record.registered.store(true, Ordering::Release);
        }
        drop(unused_memory);
        Ok(ArtifactPin(record))
    }
}

impl Drop for PendingCapture<'_, '_> {
    fn drop(&mut self) {
        let mut roots = registry();
        let entry = roots
            .iter_mut()
            .find(|entry| entry.state.identity == self.root.state().identity)
            .expect("pending capture keeps its root registered");
        assert!(entry.pins.pending > 0);
        entry.pins.pending -= 1;
        // RootRef field destruction happens after this guard is released.
    }
}

#[derive(Clone)]
pub(in crate::out_of_core) struct ArtifactPin(Shared<PinRecord>);
impl std::fmt::Debug for ArtifactPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArtifactPin").field(&self.0.key).finish()
    }
}
struct PinRecord {
    root: RootRef,
    key: ArtifactKey,
    registered: AtomicBool,
    _memory: ReaderMemory,
}
impl Drop for PinRecord {
    fn drop(&mut self) {
        if !self.registered.load(Ordering::Acquire) {
            return;
        }
        let removed = {
            let mut roots = registry();
            let root = roots
                .iter_mut()
                .find(|entry| entry.state.identity == self.root.state().identity)
                .expect("pin keeps its source root registered");
            let entry = root
                .pins
                .artifacts
                .get_mut(&self.key)
                .expect("live artifact pin remains registered");
            assert!(entry.owners > 0);
            entry.owners -= 1;
            if entry.owners == 0 {
                root.pins.artifacts.remove(&self.key)
            } else {
                None
            }
        };
        // Node credits and RootRef are released outside structural exclusion.
        drop(removed);
    }
}
