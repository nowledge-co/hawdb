// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Check the producer's vector count against its complete captured snapshot.
use super::*;

pub(super) fn vector_count(
    bytes: &[u8],
    maximum: u64,
    task: &hawdb_core::RuntimeTaskContext,
    admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
) -> Result<usize> {
    let _header_memory = admission
        .map(|admission| admission.scratch(64 * 1024))
        .transpose()?;
    let end = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| HawDBError::StorageIntegrity("resident snapshot header missing".into()))?;
    if end > 8192 {
        return Err(HawDBError::StorageIntegrity(
            "resident snapshot header too large".into(),
        ));
    }
    let header = std::str::from_utf8(&bytes[..end])
        .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
    let length = crate::parse_snapshot_header(header)?
        .uncompressed_len
        .ok_or_else(|| HawDBError::StorageIntegrity("resident snapshot length missing".into()))?;
    if length as u64 > maximum {
        return Err(HawDBError::Execution(
            "resident snapshot exceeds verification ceiling".into(),
        ));
    }
    // Existing Vec decoding can transiently grow past the final length. Keep
    // both capacities admitted until the decoded text and validation die.
    let _decode_memory = admission
        .map(|admission| admission.scratch(crate::build_memory::checked_mul(length, 2)?))
        .transpose()?;
    let text = match admission {
        Some(admission) => crate::decode_search_snapshot_text_admitted(
            bytes,
            maximum,
            &admission.memory.spool,
            task,
        )?,
        None => crate::decode_search_snapshot_text_bounded(bytes, maximum)?,
    };
    let (body, checksum) = crate::split_checksum(&text)?;
    if checksum_bytes(body.as_bytes()) != checksum
        || body.lines().next() != Some("HAWDB_SEARCH_PROJECTION_V1")
    {
        return Err(HawDBError::StorageIntegrity(
            "resident snapshot body checksum/header mismatch".into(),
        ));
    }
    let mut count = 0usize;
    for line in body.lines() {
        crate::build_control::checkpoint(task)?;
        let mut fields = line.split('\t');
        if fields.next() == Some("doc") {
            let _id = fields.next();
            let _title = fields.next();
            let _content = fields.next();
            let embedding = fields.next().ok_or_else(|| {
                HawDBError::StorageIntegrity("resident document embedding missing".into())
            })?;
            if fields.next().is_none() || fields.next().is_some() {
                return Err(HawDBError::StorageIntegrity(
                    "invalid resident document field count".into(),
                ));
            }
            if !embedding.is_empty() {
                count = count.checked_add(1).ok_or_else(|| {
                    HawDBError::StorageIntegrity("resident vector count overflow".into())
                })?;
            }
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_memory::read::{tests::governor, ReadAdmission};
    use crate::build_memory::BuildMemory;
    use crate::generation_cleanup::{once::PreparedCleanup, SearchProjectionCleanupState};
    use crate::{SearchDocument, SearchIndex};
    use hawdb_qos::RuntimeWorkRequest;
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    #[derive(Clone, Copy)]
    enum Scenario {
        ZeroVectors,
        MissingSnapshot,
        ReplacedValidSnapshot,
        ForgedZeroCount,
        MissingReceipt,
        LostSnapshotAndReceipt,
    }

    fn run(scenario: Scenario) {
        for governed in [false, true] {
            for retry in [false, true] {
                let root = crate::test_temp_dir().join(format!(
                    "hawdb-resident-retention-{}",
                    hawdb_core::generate_uuidv7().unwrap()
                ));
                let project = ProjectFileDescriptors::acquire(&root, 128).unwrap();
                let mut index = SearchIndex::open(&root).unwrap();
                let vector = matches!(
                    scenario,
                    Scenario::ForgedZeroCount | Scenario::LostSnapshotAndReceipt
                );
                let document = SearchDocument {
                    id: "memory:resident".into(),
                    title: "owned output".into(),
                    content: "original source".into(),
                    embedding: vector.then(|| vec![1.0, 0.0]),
                    metadata: Default::default(),
                };
                index.upsert(document.clone()).unwrap();
                index.checkpoint().unwrap();
                let vector_artifact = root.join("search_rabitq.1.hawdb");
                let vector_before = if vector && cfg!(feature = "vector-search") {
                    Some(std::fs::read(&vector_artifact).unwrap())
                } else {
                    assert!(!vector_artifact.exists());
                    None
                };
                let head = root.join(OUT_OF_CORE_MANIFEST_FILE);
                let mut manifest =
                    SearchOutOfCoreManifestBody::decode(&std::fs::read(&head).unwrap()).unwrap();
                let receipt = manifest.resident_snapshot.unwrap();
                assert_eq!(receipt.vector_document_count, usize::from(vector));
                assert_eq!(
                    receipt.object,
                    ArtifactObject::for_bytes(
                        &std::fs::read(root.join(crate::SEARCH_SNAPSHOT_FILE)).unwrap()
                    )
                );
                let orphan = root.join("search_rabitq.777.hawdb");
                std::fs::write(&orphan, b"unreferenced evidence").unwrap();
                match scenario {
                    Scenario::ZeroVectors => {}
                    Scenario::MissingSnapshot => {
                        std::fs::remove_file(root.join(crate::SEARCH_SNAPSHOT_FILE)).unwrap()
                    }
                    Scenario::ReplacedValidSnapshot => {
                        let replacement = root.join("replacement-snapshot.hawdb");
                        let mut foreign = document.clone();
                        let mut replacement_bytes = None;
                        for attempt in 0..256 {
                            foreign.content = format!("foreign{attempt:08x}");
                            let report = crate::snapshot_writer::write_search_snapshot(
                                &replacement,
                                None,
                                None,
                                None,
                                None,
                                None,
                                std::iter::once(&foreign),
                            )
                            .unwrap();
                            if report.encoded_len == receipt.object.byte_length {
                                replacement_bytes = Some(std::fs::read(&replacement).unwrap());
                                break;
                            }
                        }
                        let bytes = replacement_bytes
                            .expect("same-length independent codec-valid snapshot");
                        assert_eq!(bytes.len() as u64, receipt.object.byte_length);
                        assert_ne!(ArtifactObject::for_bytes(&bytes), receipt.object);
                        // Both codec and body checksums are valid. Only the
                        // originating snapshot reference distinguishes it.
                        crate::decode_search_snapshot_text(&bytes).unwrap();
                        std::fs::write(root.join(crate::SEARCH_SNAPSHOT_FILE), bytes).unwrap();
                    }
                    Scenario::ForgedZeroCount => {
                        manifest
                            .resident_snapshot
                            .as_mut()
                            .unwrap()
                            .vector_document_count = 0;
                        std::fs::write(&head, manifest.encode().unwrap()).unwrap();
                    }
                    Scenario::MissingReceipt => {
                        manifest.resident_snapshot = None;
                        std::fs::write(&head, manifest.encode().unwrap()).unwrap();
                    }
                    Scenario::LostSnapshotAndReceipt => {
                        manifest.resident_snapshot = None;
                        assert!(!manifest.resident_snapshot_absent);
                        std::fs::write(&head, manifest.encode().unwrap()).unwrap();
                        std::fs::remove_file(root.join(crate::SEARCH_SNAPSHOT_FILE)).unwrap();
                    }
                }
                let governor = governor();
                let permit = governor
                    .try_admit(
                        RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1),
                    )
                    .unwrap();
                let task = if governed {
                    permit.bind_task_context(Default::default())
                } else {
                    Default::default()
                };
                let memory = BuildMemory::new(&task).unwrap();
                let admission = ReadAdmission {
                    memory: &memory,
                    task: &task,
                    permit: crate::build_memory::read::ReadPermit::Governed(&permit),
                };
                let mut publication =
                    SearchProjectionPublishLease::acquire_for_cleanup(&root).unwrap();
                if governed {
                    publication.admit_cleanup_resources(&admission).unwrap();
                }
                let admission = governed.then_some(&admission);
                let success = matches!(scenario, Scenario::ZeroVectors);
                if retry {
                    let report = SearchProjectionCleanupState::default().run_under_publication(
                        &root,
                        Default::default(),
                        &publication,
                        &Default::default(),
                        admission,
                    );
                    assert_eq!(report.retry_required, !success, "{report:?}");
                    if !success {
                        assert_eq!(report.delete_attempts, 0);
                    }
                } else {
                    let report = PreparedCleanup::prepare(&root, &memory, &task)
                        .unwrap()
                        .run_under_publication(
                            &root,
                            Default::default(),
                            &task,
                            &publication,
                            &Default::default(),
                            admission,
                        );
                    assert_eq!(report.retry_required, !success, "{report:?}");
                    if !success {
                        assert_eq!(report.deleted_files, 0);
                    }
                }
                assert_eq!(orphan.exists(), !success);
                if let Some(expected) = vector_before {
                    assert_eq!(std::fs::read(&vector_artifact).unwrap(), expected);
                } else {
                    assert!(!vector_artifact.exists());
                }
                drop(publication);
                assert_eq!(memory.ledger.snapshot().used_bytes, 0);
                drop(index);
                drop(project);
                std::fs::remove_dir_all(&root).unwrap();
            }
        }
    }

    #[test]
    fn zero_vector_snapshot_proof_allows_actual_reclamation() {
        run(Scenario::ZeroVectors);
    }
    #[test]
    fn missing_expected_snapshot_cannot_authorize_reclamation() {
        run(Scenario::MissingSnapshot);
    }
    #[test]
    fn valid_foreign_snapshot_cannot_supply_its_own_expected_reference() {
        run(Scenario::ReplacedValidSnapshot);
    }
    #[test]
    fn metadata_zero_claim_cannot_hide_actual_resident_vectors() {
        run(Scenario::ForgedZeroCount);
    }
    #[test]
    fn missing_producer_receipt_cannot_select_a_native_identity_fallback() {
        run(Scenario::MissingReceipt);
    }
    #[test]
    fn losing_snapshot_and_receipt_does_not_mint_an_absence_witness() {
        run(Scenario::LostSnapshotAndReceipt);
    }
}
