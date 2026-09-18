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

use super::*;
use crate::document_encoding::{legacy_encode, ENCODING_ATTEMPTS, STREAMING_ATTEMPTS};

#[test]
fn public_generation_spools_without_record_materialization_and_reopens() {
    let root = test_dir("bounded_spool_public_generation");
    let documents = (0..3)
        .map(|number| {
            let mut document = document(number);
            document.content = "graph \u{4e2d}\u{6587} ".repeat(4097);
            document
                .metadata
                .insert("spool_test".into(), "\u{1f980};=\t\n".repeat(3001));
            document
        })
        .collect::<Vec<_>>();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    let mut expected = SPOOL_HEADER.to_vec();
    let mut digest = Crc32cHasher::new();
    let mut largest_record = 0;
    for document in &documents {
        let record = legacy_encode(document);
        largest_record = largest_record.max(record.len() as u64);
        expected.extend_from_slice(&(record.len() as u64).to_le_bytes());
        expected.extend_from_slice(&checksum_bytes(record.as_bytes()).to_le_bytes());
        expected.extend_from_slice(record.as_bytes());
        digest.update(record.as_bytes());
        let allocated = ENCODING_ATTEMPTS.get();
        let streamed = STREAMING_ATTEMPTS.get();
        writer.push(document.clone()).unwrap();
        assert_eq!(ENCODING_ATTEMPTS.get(), allocated);
        assert_eq!(STREAMING_ATTEMPTS.get(), streamed + 2);
        assert_eq!(writer.documents_digest.finish(), digest.finish());
        writer.spool.as_mut().unwrap().flush().unwrap();
        assert_eq!(fs::read(&writer.spool_path).unwrap(), expected);
    }
    spool::read_evidence::take_max_request();
    let report = writer.finish().unwrap();
    let largest_read = spool::read_evidence::take_max_request();
    assert!(largest_read > 0);
    assert!(largest_read <= 8192, "unbounded spool read: {largest_read}");
    assert_eq!(report.documents_digest, digest.finish());
    assert_eq!(report.spool_bytes, expected.len() as u64);
    assert_eq!(report.document_count, documents.len());
    assert_eq!(report.peak_record_bytes, largest_record);
    assert!(report.active_manifest_published_last);
    let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
    let ids = documents
        .iter()
        .map(|document| document.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(reader.hydrate_documents(&ids).unwrap().documents, documents);
    drop(reader);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn spool_write_and_deferred_flush_failures_preserve_the_active_generation() {
    let root = test_dir("bounded_spool_public_io_failure");
    let original = document(0);
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(original.clone()).unwrap();
    let generation = initial.finish().unwrap().generation;
    let before = published_files(&root);
    for deferred in [false, true] {
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        // A real read-only file produces a portable I/O failure, either during
        // append or later when the accepted buffered frame is flushed by finish.
        writer.spool = Some(BufWriter::with_capacity(
            if deferred { 16 * 1024 } else { 1 },
            File::open(&writer.spool_path).unwrap(),
        ));
        let result = writer.push(document(1));
        if deferred {
            result.unwrap();
            assert_eq!(writer.document_count, 1);
        } else {
            assert!(result.is_err());
            assert!(writer.poisoned);
            assert_eq!(writer.document_count, 0);
            assert_eq!(
                writer.documents_digest.finish(),
                Crc32cHasher::new().finish()
            );
            assert!(writer
                .push(document(2))
                .unwrap_err()
                .to_string()
                .contains("poisoned"));
        }
        assert!(writer.finish().is_err());
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = crate::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), generation);
        assert_eq!(
            reader
                .hydrate_documents(std::slice::from_ref(&original.id))
                .unwrap()
                .documents,
            vec![original.clone()]
        );
    }
    fs::remove_dir_all(root).unwrap();
}
