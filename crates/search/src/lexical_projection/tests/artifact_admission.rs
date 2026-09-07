use super::*;
use crate::build_memory::SPOOL_BUFFER_BYTES;
use artifact_memory::Documents;
use skein_core::RuntimeMemoryReservation;
use std::mem::size_of;

pub(super) fn memory(bytes: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0)),
    )
    .unwrap()
}

pub(super) fn legacy_document_block(
    generation: u64,
    block: u64,
    documents: &[(String, u32)],
) -> Vec<u8> {
    let mut bytes = b"SKNLEX01".to_vec();
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes.extend_from_slice(&block.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&(documents.len() as u32).to_le_bytes());
    for (id, length) in documents {
        bytes.extend_from_slice(&(id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(id.as_bytes());
        bytes.extend_from_slice(&length.to_le_bytes());
    }
    bytes
}

#[test]
fn artifact_buffer_rejects_before_creating_a_file() {
    let root = projection_root("artifact-buffer-preflight");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("new.tmp");
    let memory = memory(SPOOL_BUFFER_BYTES - 1);
    assert!(ArtifactBuilder::new(&path, 1, Default::default(), memory.clone()).is_err());
    assert!(!path.exists());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn document_id_is_admitted_before_cloning() {
    let slots = 4 * size_of::<(String, u32)>();
    let memory = memory(slots + 2);
    let mut documents = Documents::new(memory.clone()).unwrap();
    artifact_memory::evidence::take();
    assert!(documents.push("abc", 7).is_err());
    assert_eq!(artifact_memory::evidence::take(), (0, 0));
    assert!(documents.is_empty());
    assert_eq!(memory.ledger.snapshot().used_bytes, slots);
    drop(documents);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn document_encoding_admits_input_payload_and_directory_keys_together() {
    let retained = 4 * size_of::<(String, u32)>() + 3;
    let encoded_bytes = 29 + 11 + 6;
    for (limit, succeeds) in [
        (retained + encoded_bytes - 1, false),
        (retained + encoded_bytes, true),
    ] {
        let memory = memory(limit);
        let mut documents = Documents::new(memory.clone()).unwrap();
        documents.push("abc", u32::MAX).unwrap();
        artifact_memory::evidence::take();
        let encoded = documents.encode(11, 13, Default::default(), &RuntimeTaskContext::default());
        assert_eq!(encoded.is_ok(), succeeds);
        assert_eq!(
            artifact_memory::evidence::take(),
            (0, usize::from(succeeds))
        );
        if let Ok(encoded) = encoded {
            assert_eq!(
                encoded.payload,
                legacy_document_block(11, 13, &[("abc".to_string(), u32::MAX)])
            );
            assert_eq!(encoded.min_key, "abc");
            assert_eq!(encoded.max_key, "abc");
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                retained + encoded_bytes
            );
            documents.clear();
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                retained - 3 + encoded_bytes
            );
            drop(encoded);
        }
        documents.release();
        assert_eq!(documents.retained_bytes(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn document_slot_growth_keeps_the_old_allocation_admitted() {
    let memory = memory(64 * 1024);
    let mut documents = Documents::new(memory.clone()).unwrap();
    for _ in 0..5 {
        documents.push("a", 1).unwrap();
    }
    let slot = size_of::<(String, u32)>();
    assert_eq!(memory.ledger.snapshot().used_bytes, 8 * slot + 5);
    assert!(memory.ledger.snapshot().peak_bytes >= 12 * slot + 4);
    drop(documents);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn document_block_limits_and_cancellation_reject_before_encoding() {
    let config = LexicalProjectionConfig {
        max_block_bytes: NonZeroU64::new(39).unwrap(),
        ..Default::default()
    };
    assert!(Documents::record_bytes("abc", config).is_err());
    let memory = memory(64 * 1024);
    let mut documents = Documents::new(memory.clone()).unwrap();
    documents.push("abc", 1).unwrap();
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    artifact_memory::evidence::take();
    assert!(documents.encode(1, 0, Default::default(), &task).is_err());
    assert!(documents
        .encode(1, 0, config, &RuntimeTaskContext::default())
        .is_err());
    assert_eq!(artifact_memory::evidence::take(), (0, 0));
    drop(documents);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn document_and_dictionary_directories_share_the_operation_root() {
    let memory = memory(31);
    let directory = dictionary_store::DirectoryBudget::new(1024)
        .with_memory(&memory)
        .unwrap();
    let mut documents = Vec::<u64>::new();
    let mut dictionaries = Vec::<u64>::new();
    directory.admit(&mut documents, 8).unwrap();
    documents.push(1);
    assert!(directory.clone().admit(&mut dictionaries, 8).is_err());
    assert_eq!(dictionaries.capacity(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 16);
    assert!(directory.clone().with_memory(&memory).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 16);
    drop(documents);
    drop(dictionaries);
    drop(directory);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn directory_growth_admits_old_and_replacement_slots() {
    for (limit, succeeds) in [(39, false), (40, true)] {
        let memory = memory(limit);
        let directory = dictionary_store::DirectoryBudget::new(32)
            .with_memory(&memory)
            .unwrap();
        let mut entries = Vec::<u64>::new();
        directory.admit(&mut entries, 8).unwrap();
        entries.push(1);
        assert_eq!(directory.admit(&mut entries, 8).is_ok(), succeeds);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            if succeeds { 32 } else { 16 }
        );
        assert_eq!(entries.capacity(), if succeeds { 2 } else { 1 });
        drop(entries);
        drop(directory);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn completed_artifact_keeps_directory_ownership_until_manifest_handoff_finishes() {
    let root = projection_root("artifact-directory-handoff");
    fs::create_dir_all(&root).unwrap();
    let memory = memory(64 * 1024);
    let path = root.join("artifact.tmp");
    let mut artifact = ArtifactBuilder::new(&path, 1, Default::default(), memory.clone()).unwrap();
    artifact.push_document("abc", 7).unwrap();
    artifact.finish_documents().unwrap();
    assert_eq!(artifact.document_pending.retained_bytes(), 0);
    let summary = artifact.finish().unwrap();
    let directories = size_of::<BlockDescriptor>() + 6;
    assert_eq!(memory.ledger.snapshot().used_bytes, directories);
    let blocks = summary.blocks;
    let dictionaries = summary.dictionaries;
    assert_eq!(blocks[0].min_key, "abc");
    assert_eq!(memory.ledger.snapshot().used_bytes, directories);
    drop(blocks);
    drop(dictionaries);
    drop(summary._directory);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_document_encoding_preserves_the_published_generation() {
    let root = projection_root("artifact-encoding-atomicity");
    fs::create_dir_all(&root).unwrap();
    let input = document("abc", "", "");
    let lexicon = SearchAnalyzerLexicon::empty();
    let reader = LexicalProjectionWriter::new(Default::default())
        .write(&root, 1, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap();
    let before = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let memory = memory(
        SPOOL_BUFFER_BYTES
            + 3
            + 4 * size_of::<(String, u32)>()
            + 3
            + 45
            + paths::tests::publication_bytes(&root, 2),
    );
    artifact_memory::evidence::take();
    let error = LexicalProjectionWriter::new(Default::default())
        .with_memory(memory.clone())
        .write(&root, 2, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(artifact_memory::evidence::take(), (1, 0));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), before);
    assert!(!root.join(artifact_file(2)).exists());
    assert!(fs::read_dir(&root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".tmp")));
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn manifest_encoding_retains_admitted_capacity_and_rejects_a_short_root() {
    let root = projection_root("artifact-manifest-memory");
    fs::create_dir_all(&root).unwrap();
    let config = LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(32).unwrap(),
        ..Default::default()
    };
    let inputs = (0..128)
        .map(|id| document(&format!("memory:{id:08}"), "", "graph"))
        .collect::<Vec<_>>();
    let reader = LexicalProjectionWriter::new(config)
        .write(
            &root,
            1,
            None,
            11,
            13,
            inputs.iter(),
            &SearchAnalyzerLexicon::empty(),
        )
        .unwrap();
    let expected = reader
        .manifest
        .encode_bounded(config.max_directory_bytes.get())
        .unwrap();
    let memory = memory(1024 * 1024);
    let bytes = reader
        .manifest
        .encode_admitted(config.max_directory_bytes.get(), &memory)
        .unwrap();
    assert_eq!(bytes.as_ref(), expected);
    let snapshot = memory.ledger.snapshot();
    assert!(snapshot.used_bytes >= expected.len());
    assert!(snapshot.peak_bytes > snapshot.used_bytes);
    drop(bytes);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    for (limit, succeeds) in [
        (snapshot.peak_bytes, true),
        (snapshot.peak_bytes - 1, false),
    ] {
        let memory = self::memory(limit);
        let result = reader
            .manifest
            .encode_admitted(config.max_directory_bytes.get(), &memory);
        assert_eq!(result.is_ok(), succeeds);
        drop(result);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn artifact_checksum_scratch_uses_the_shared_budget() {
    let root = projection_root("artifact-checksum-memory");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("data");
    fs::write(&path, b"checksum fixture").unwrap();
    let file = File::open(path).unwrap();
    for (limit, succeeds) in [(15, false), (16, true)] {
        let memory = memory(limit);
        let result = file_digest_with_memory(&file, &RuntimeTaskContext::default(), Some(&memory));
        assert_eq!(result.is_ok(), succeeds);
        if let Ok(result) = result {
            assert_eq!(result, (16, checksum(b"checksum fixture")));
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    drop(file);
    fs::remove_dir_all(root).unwrap();
}
