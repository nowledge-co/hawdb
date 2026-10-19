// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::document_encoding::legacy_encode;

fn assert_record(
    record: &spool::SpoolRecord,
    expected: &SearchDocument,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) {
    let mut encoded = Vec::new();
    record.write_to(&mut encoded, memory, task).unwrap();
    assert_eq!(encoded, legacy_encode(expected).as_bytes());
}

fn replacement_input_keeps_original_documents(partitioned: bool) {
    let root = test_dir("held_generation_spool");
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let documents = (0..3).map(document).collect::<Vec<_>>();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(if partitioned { 1 } else { 100 }).unwrap(),
            source_graph_commit_epoch: Some(7),
            ..Default::default()
        },
    )
    .unwrap();
    let mut replacement = SPOOL_HEADER.to_vec();
    for original in &documents {
        writer.push(original.clone()).unwrap();
        let mut foreign = original.clone();
        foreign.content = "z".repeat(original.content.len());
        let encoded = legacy_encode(&foreign);
        replacement.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        replacement.extend_from_slice(&checksum_bytes(encoded.as_bytes()).to_le_bytes());
        replacement.extend_from_slice(encoded.as_bytes());
    }
    writer.spool.as_mut().unwrap().flush().unwrap();
    assert_eq!(replacement.len() as u64, writer.spool_bytes);
    let original_path = root.join("held-original-spool");
    std::fs::rename(&writer.spool_path, &original_path).unwrap();
    std::fs::write(&writer.spool_path, &replacement).unwrap();
    let report = writer
        .finish()
        .expect("build from original spool, not its replacement label");
    assert_eq!(
        report.published_content_segments,
        if partitioned { 3 } else { 1 }
    );
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.source_graph_commit_epoch(), Some(7));
    let ids = documents.iter().map(|d| d.id.clone()).collect::<Vec<_>>();
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, documents);
}

#[test]
fn held_generation_spool_single_ignores_same_length_valid_foreign_input() {
    replacement_input_keeps_original_documents(false);
}

#[test]
fn held_generation_spool_partitioned_ignores_same_length_valid_foreign_input() {
    replacement_input_keeps_original_documents(true);
}

#[test]
fn held_generation_spool_cursors_share_one_file_and_keep_independent_positions() {
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    let root = test_dir("held_generation_spool_cursors");
    let project = ProjectFileDescriptors::acquire(&root, 1).unwrap();
    let context = project.io_context();
    let mut file = context
        .open(
            OpenOptions::new().create_new(true).read(true).write(true),
            &root.join("input.hawdb"),
        )
        .unwrap();
    let mut bytes = SPOOL_HEADER.to_vec();
    let documents = [document(0), document(1)];
    for doc in &documents {
        let encoded = legacy_encode(doc);
        bytes.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&checksum_bytes(encoded.as_bytes()).to_le_bytes());
        bytes.extend_from_slice(encoded.as_bytes());
    }
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let held = spool::HeldSpool::capture(file, bytes.len() as u64, &memory).unwrap();
    let source = SpoolSource {
        held: Some(held),
        path: &root.join("never-open-this-label"),
        document_count: 2,
        max_record_bytes: 1024 * 1024,
        max_metadata_fields: 256,
        memory: memory.clone(),
    };
    let mut left = source.cursor(&task).unwrap();
    let mut right = source.cursor(&task).unwrap();
    assert_eq!(project.metrics().open, 1);
    let retained_record = left.next().unwrap().unwrap();
    assert_record(&retained_record, &documents[0], &memory, &task);
    assert_record(
        &left.next().unwrap().unwrap(),
        &documents[1],
        &memory,
        &task,
    );
    assert_record(
        &right.next().unwrap().unwrap(),
        &documents[0],
        &memory,
        &task,
    );
    drop(source);
    assert_eq!(project.metrics().open, 1);
    assert_record(
        &right.next().unwrap().unwrap(),
        &documents[1],
        &memory,
        &task,
    );
    assert!(left.next().unwrap().is_none());
    assert!(right.next().unwrap().is_none());
    drop((left, right));
    // A record/body reference is also a real native owner after its cursor ends.
    assert_eq!(project.metrics().open, 1);
    assert_record(&retained_record, &documents[0], &memory, &task);
    drop(retained_record);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn held_generation_spool_requires_its_live_owner_instead_of_falling_back_to_a_path() {
    let root = test_dir("held_generation_spool_owner");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    assert!(writer.input_source().is_err());
    writer.seal_input_spool().unwrap();
    assert!(writer.spool.is_none());
    assert!(writer.input_source().is_ok());
    writer.held_spool.take();
    assert!(writer.input_source().is_err());
    assert!(super::super::initial::finish(writer, 1).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn held_generation_spool_changed_original_refuses_before_replacing_the_old_selector() {
    for append_bytes in [false, true] {
        let root = test_dir("held_generation_spool_changed_original");
        let mut previous =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        previous.push(document(99)).unwrap();
        previous.finish().unwrap();
        let before = std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(document(0)).unwrap();
        writer.seal_input_spool().unwrap();
        let mut bytes = std::fs::read(&writer.spool_path).unwrap();
        if append_bytes {
            bytes.push(b'x');
        } else {
            let end = bytes.len() - 2;
            bytes[end] ^= 1;
        }
        // Rewrite the same inode, rather than changing only a namespace label.
        std::fs::write(&writer.spool_path, &bytes).unwrap();
        let source = writer.input_source().unwrap();
        let result = source
            .cursor(&writer.task_context)
            .and_then(|mut c| c.next());
        assert!(result.is_err());
        assert_eq!(
            std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        drop(source);
        drop(writer);
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader
                .hydrate_documents(&[document(99).id])
                .unwrap()
                .documents,
            vec![document(99)]
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
