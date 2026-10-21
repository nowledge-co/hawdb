// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Synchronous closure validation borrowed from actual publication custody.
//! Returned file readers stay inside discovery; no serving capability escapes.
use super::*;
use hawdb_storage::immutable_object::ObjectReferenceWriter;
use std::io::{Read, Seek, SeekFrom, Write};

mod resident;

/// Physical verification ceilings, not a new memory or descriptor allowance.
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) artifact_bytes: u64,
    pub(super) total_bytes: u64,
    pub(super) files: usize,
}

pub(super) struct Source<'a> {
    publication: &'a SearchProjectionPublishLease,
    task: &'a hawdb_core::RuntimeTaskContext,
    limits: Limits,
    admitted_bytes: std::cell::Cell<u64>,
    admitted_files: std::cell::Cell<usize>,
}
impl<'a> Source<'a> {
    pub(super) fn new(
        publication: &'a SearchProjectionPublishLease,
        task: &'a hawdb_core::RuntimeTaskContext,
        limits: Limits,
    ) -> Self {
        Self {
            publication,
            task,
            limits,
            admitted_bytes: std::cell::Cell::new(0),
            admitted_files: std::cell::Cell::new(0),
        }
    }
    pub(super) fn admit(&self, length: u64) -> Result<()> {
        crate::build_control::checkpoint(self.task)?;
        let bytes = self
            .admitted_bytes
            .get()
            .checked_add(length)
            .filter(|bytes| *bytes <= self.limits.total_bytes);
        let files = self
            .admitted_files
            .get()
            .checked_add(1)
            .filter(|files| *files <= self.limits.files);
        if length > self.limits.artifact_bytes || bytes.is_none() || files.is_none() {
            return Err(HawDBError::Execution(
                "captured cleanup verification exceeds operation ceiling".into(),
            ));
        }
        self.admitted_bytes.set(bytes.unwrap());
        self.admitted_files.set(files.unwrap());
        Ok(())
    }
    fn require_admission(
        &self,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<()> {
        if let Some(admission) = admission {
            admission.validate_mode()?;
        }
        if admission.is_some_and(|admission| !admission.matches_publication(self.publication)) {
            return Err(HawDBError::Execution(
                "cleanup admission belongs to another publication".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn validate_capture(
        &self,
        capture: &super::root_registry::PendingCapture<'_, '_>,
    ) -> Result<()> {
        if capture.source_identity() != self.publication.source_identity() {
            return Err(HawDBError::StorageIntegrity(
                "validation scratch and artifact capture have different sources".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn validation_stage(
        &self,
        label: &Path,
        memory: &crate::build_memory::BuildMemory,
        task: &hawdb_core::RuntimeTaskContext,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<super::generation_writer::StageDirectory> {
        self.require_admission(admission)?;
        self.publication.with_captured_directory(|parent| {
            super::generation_writer::StageDirectory::create_captured(
                label, parent, memory, task, admission,
            )
        })
    }
    pub(super) fn open_admitted(
        &self,
        name: &str,
        object: ArtifactObject,
        admission: &crate::build_memory::read::ReadAdmission<'_>,
    ) -> Result<File> {
        self.require_admission(Some(admission))?;
        self.admit(object.byte_length)?;
        let _scratch = admission.scratch(8192)?;
        let mut file = {
            let _io = admission
                .io()
                .map_err(|error| HawDBError::Execution(error.to_string()))?;
            self.publication.open_captured_child(name)?
        };
        admission.with_read_scope(
            &file,
            object.byte_length,
            self.limits.artifact_bytes,
            |reader| {
                let reference = object.reference();
                let mut digest = ObjectReferenceWriter::new(
                    reference.kind,
                    reference.format_version,
                    reference.byte_length,
                )
                .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
                let mut scratch = [0u8; 8192];
                loop {
                    crate::build_control::checkpoint(self.task)?;
                    let count = reader.read(&mut scratch)?;
                    if count == 0 {
                        break;
                    }
                    digest.write_all(&scratch[..count])?;
                }
                if digest
                    .finish()
                    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
                    != reference
                {
                    return Err(HawDBError::StorageIntegrity(
                        "fresh cleanup name differs from selected object".into(),
                    ));
                }
                Ok(())
            },
        )?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
    pub(super) fn read_admitted<'execution>(
        &self,
        name: &str,
        object: ArtifactObject,
        checksum: u64,
        maximum: u64,
        admission: &crate::build_memory::read::ReadAdmission<'execution>,
    ) -> Result<crate::build_memory::read::ReadBytes<'execution>> {
        admission.require_read_limit(object.byte_length, maximum)?;
        let file = self.open_admitted(name, object, admission)?;
        admission.read_file_range(
            &file,
            object.byte_length,
            maximum,
            crate::build_memory::read::FileRange {
                offset: 0,
                length: object.byte_length,
                checksum,
            },
        )
    }

    pub(super) fn head_file(
        &self,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<Option<File>> {
        self.require_admission(admission)?;
        let _io = admission
            .map(|admission| admission.io())
            .transpose()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let file = match self
            .publication
            .open_captured_child(OUT_OF_CORE_MANIFEST_FILE)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        self.admit(file.metadata()?.len())?;
        Ok(Some(file))
    }
    pub(super) fn head(&self) -> Result<Option<Vec<u8>>> {
        self.head_file(None)?
            .map(|file| read_file(file, MAX_OUT_OF_CORE_MANIFEST_BYTES, self.task))
            .transpose()
    }
    pub(super) fn prove_no_projection_files(
        &self,
        root_label: &Path,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<()> {
        self.require_admission(admission)?;
        let _memory = admission
            .map(|admission| {
                admission.scratch(crate::build_memory::directory::scan_bytes(root_label)?)
            })
            .transpose()?;
        let _io = admission
            .map(|admission| admission.io())
            .transpose()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        self.publication.with_captured_directory(|directory| {
            directory.visit_children(&directory.operation_context(), self.limits.files, |name| {
                self.task
                    .checkpoint()
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                if name.starts_with("search_") {
                    return Err(std::io::Error::other(
                        "projection files exist without a validated head",
                    ));
                }
                Ok(())
            })
        })?;
        Ok(())
    }

    pub(super) fn open(&self, name: &str, object: ArtifactObject) -> Result<File> {
        self.admit(object.byte_length)?;
        let mut file = self.publication.open_captured_child(name)?;
        if file.metadata()?.len() != object.byte_length {
            return Err(HawDBError::StorageIntegrity(
                "captured cleanup artifact length mismatch".into(),
            ));
        }
        let reference = object.reference();
        let mut hasher = ObjectReferenceWriter::new(
            reference.kind,
            reference.format_version,
            reference.byte_length,
        )
        .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        let mut scratch = [0u8; 8192];
        loop {
            crate::build_control::checkpoint(self.task)?;
            let count = match file.read(&mut scratch) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if count == 0 {
                break;
            }
            hasher.write_all(&scratch[..count])?;
        }
        if hasher
            .finish()
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
            != reference
        {
            return Err(HawDBError::StorageIntegrity(
                "captured cleanup artifact identity mismatch".into(),
            ));
        }
        crate::build_control::checkpoint(self.task)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
    pub(super) fn verify_resident_vector_absence(
        &self,
        receipt: Option<ResidentSnapshot>,
        absence_proven: bool,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<bool> {
        self.require_admission(admission)?;
        // NotFound from this actual directory is a source fact, not a pathname
        // existence probe. All other failures retain the unknown dependency.
        let _io = admission
            .map(|admission| admission.io())
            .transpose()
            .map_err(|error| HawDBError::Execution(error.to_string()))?;
        let file = match self
            .publication
            .open_captured_child(crate::SEARCH_SNAPSHOT_FILE)
        {
            Ok(file) => file,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && receipt.is_none()
                    && absence_proven =>
            {
                return Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && receipt.is_none() => {
                return Ok(false)
            }
            Err(error) => return Err(error.into()),
        };
        drop(_io);
        let Some(receipt) = receipt else {
            return Ok(false);
        };
        self.admit(receipt.object.byte_length)?;
        let bytes = match admission {
            Some(admission) => admission.read_unbound_file(&file, receipt.object.byte_length)?,
            None => crate::build_memory::read::ReadBytes::unaccounted(read_file(
                file,
                receipt.object.byte_length,
                self.task,
            )?),
        };
        let expected = receipt.object.reference();
        let mut digest = ObjectReferenceWriter::new(
            expected.kind,
            expected.format_version,
            expected.byte_length,
        )
        .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
        for chunk in bytes.chunks(8192) {
            crate::build_control::checkpoint(self.task)?;
            digest.write_all(chunk)?;
        }
        if digest
            .finish()
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?
            != expected
        {
            return Err(HawDBError::StorageIntegrity(
                "resident snapshot differs from its producer receipt".into(),
            ));
        }
        let actual =
            resident::vector_count(&bytes, self.limits.artifact_bytes, self.task, admission)?;
        if actual != receipt.vector_document_count {
            return Err(HawDBError::StorageIntegrity(
                "resident snapshot vector count differs from producer receipt".into(),
            ));
        }
        Ok(actual == 0)
    }

    pub(super) fn verify_manifest(
        &self,
        manifest: &SearchOutOfCoreManifestBody,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<()> {
        let _name_memory = admission
            .map(|admission| admission.scratch(128))
            .transpose()?;
        for segment in &manifest.segments {
            let objects = segment.objects;
            for (name, object) in [
                (segment.descriptor_file.as_str(), objects.descriptor),
                (segment.payload_file.as_str(), objects.payload),
                (segment.metadata_payload_file.as_str(), objects.metadata),
                (segment.vector_payload_file.as_str(), objects.vector),
                (segment.layout_file.as_str(), objects.layout),
                (
                    segment.lexical_manifest_file.as_str(),
                    objects.lexical_manifest,
                ),
            ] {
                self.verify_admitted(name, object, admission)?;
            }
            let name = crate::lexical_projection::artifact_file(objects.lexical_generation);
            self.verify_admitted(&name, objects.lexical_payload, admission)?;
            if let Some(name) = &segment.rabitq_artifact_file {
                self.verify_admitted(
                    name,
                    objects.rabitq.expect("validated vector reference"),
                    admission,
                )?;
            }
        }
        for run in &manifest.mutation_runs {
            self.verify_admitted(&run.file, run.object, admission)?;
        }
        Ok(())
    }
    fn verify_admitted(
        &self,
        name: &str,
        object: ArtifactObject,
        admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
    ) -> Result<()> {
        let Some(admission) = admission else {
            drop(self.open(name, object)?);
            return Ok(());
        };
        drop(self.open_admitted(name, object, admission)?);
        Ok(())
    }

    pub(super) fn read(
        &self,
        name: &str,
        object: ArtifactObject,
        checksum: u64,
        maximum: u64,
    ) -> Result<Vec<u8>> {
        if object.byte_length > maximum {
            return Err(HawDBError::Execution(
                "captured cleanup metadata exceeds operation ceiling".into(),
            ));
        }
        let bytes = read_file(self.open(name, object)?, maximum, self.task)?;
        if checksum_bytes(&bytes) != checksum {
            return Err(HawDBError::StorageIntegrity(
                "captured cleanup artifact checksum mismatch".into(),
            ));
        }
        Ok(bytes)
    }
}
fn read_file(
    mut file: File,
    maximum: u64,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<Vec<u8>> {
    let length = file.metadata()?.len();
    if length > maximum {
        return Err(HawDBError::Storage(
            "captured cleanup metadata exceeds read ceiling".into(),
        ));
    }
    let length = usize::try_from(length)
        .map_err(|_| HawDBError::Storage("captured cleanup length exceeds address space".into()))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|error| HawDBError::Execution(error.to_string()))?;
    let mut scratch = [0u8; 8192];
    loop {
        crate::build_control::checkpoint(task)?;
        let count = match file.read(&mut scratch) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            break;
        }
        if count > length - bytes.len() {
            return Err(HawDBError::StorageIntegrity(
                "captured cleanup metadata grew".into(),
            ));
        }
        bytes.extend_from_slice(&scratch[..count]);
    }
    if bytes.len() != length {
        return Err(HawDBError::StorageIntegrity(
            "captured cleanup metadata shrank".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::build_memory::{
        read::{ReadAdmission, ReadPermit},
        BuildMemory,
    };
    use crate::generation_cleanup::{once::PreparedCleanup, SearchProjectionCleanupState};
    use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    struct Fixture {
        root: PathBuf,
        _project: ProjectFileDescriptors,
    }
    impl Fixture {
        fn new() -> Self {
            let root = crate::test_temp_dir().join(format!(
                "hawdb-sync-cleanup-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            let project = ProjectFileDescriptors::acquire(&root, 128).unwrap();
            for _ in 0..3 {
                let mut writer =
                    SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
                writer
                    .push(crate::SearchDocument {
                        id: "memory:a".into(),
                        title: "source".into(),
                        content: "actual source bytes".into(),
                        embedding: None,
                        metadata: Default::default(),
                    })
                    .unwrap();
                writer.finish().unwrap();
            }
            Self {
                root,
                _project: project,
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[test]
    fn synchronous_cleanup_uses_remaining_original_accounts_and_reclaims_after_refund() {
        for retry in [false, true] {
            let fixture = Fixture::new();
            let task = RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(8 << 20, 0));
            let memory = BuildMemory::new(&task).unwrap();
            let publication =
                SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
            let admission = ReadAdmission {
                memory: &memory,
                task: &task,
                permit: ReadPermit::Synchronous(&publication),
            };
            let obsolete = fixture
                .root
                .join("search_projection_segment_payloads.0.hawdb");
            std::fs::write(&obsolete, b"obsolete evidence").unwrap();
            let held = memory.input.reserve((8 << 20) - 1024).unwrap();
            let before = memory.ledger.snapshot().used_bytes;
            if retry {
                let report = SearchProjectionCleanupState::default().run_under_publication(
                    &fixture.root,
                    Default::default(),
                    &publication,
                    &Default::default(),
                    Some(&admission),
                );
                assert!(report.retry_required);
                assert_eq!(report.delete_attempts, 0);
            } else {
                let report = PreparedCleanup::prepare(&fixture.root, &memory, &task)
                    .unwrap()
                    .run_under_publication(
                        &fixture.root,
                        Default::default(),
                        &task,
                        &publication,
                        &Default::default(),
                        None,
                    );
                assert!(report.retry_required);
                assert_eq!(report.deleted_files, 0);
            }
            assert!(obsolete.exists());
            assert_eq!(memory.ledger.snapshot().used_bytes, before);
            drop(held);
            let report = SearchProjectionCleanupState::default().run_under_publication(
                &fixture.root,
                Default::default(),
                &publication,
                &Default::default(),
                Some(&admission),
            );
            assert!(!report.retry_required, "{report:?}");
            assert!(!obsolete.exists());
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(task.memory_reservation().unwrap().memory_bytes(), 8 << 20);
            assert!(admission.runtime_permit().is_none());
        }
    }

    #[test]
    fn synchronous_admission_cannot_construct_persistent_root_or_borrow_foreign_publication() {
        let first = Fixture::new();
        let second = Fixture::new();
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(8 << 20, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let mut publication =
            SearchProjectionPublishLease::acquire_for_cleanup(&first.root).unwrap();
        let foreign = SearchProjectionPublishLease::acquire_for_cleanup(&second.root).unwrap();
        {
            let admission = ReadAdmission {
                memory: &memory,
                task: &task,
                permit: ReadPermit::Synchronous(&publication),
            };
            assert!(publication.cleanup_capture(&admission).is_err());
            let source = Source::new(
                &foreign,
                &task,
                Limits {
                    artifact_bytes: 1 << 20,
                    total_bytes: 8 << 20,
                    files: 100,
                },
            );
            assert!(source.head_file(Some(&admission)).is_err());
        }
        // Runtime rejection also precedes promotion of native custody.
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: ReadPermit::Synchronous(&foreign),
        };
        assert!(publication.admit_cleanup_resources(&admission).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn governed_task_cannot_be_relabelled_as_local_synchronous_budget() {
        let fixture = Fixture::new();
        let governor = crate::build_memory::read::tests::governor();
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(8 << 20)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let publication = SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: ReadPermit::Synchronous(&publication),
        };
        assert!(task.has_resource_controllers());
        let source = Source::new(
            &publication,
            &task,
            Limits {
                artifact_bytes: 1 << 20,
                total_bytes: 8 << 20,
                files: 100,
            },
        );
        assert!(source.head_file(Some(&admission)).is_err());
        assert!(admission.retained(1).is_err());
        assert!(admission.scratch(1).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
    }
}
