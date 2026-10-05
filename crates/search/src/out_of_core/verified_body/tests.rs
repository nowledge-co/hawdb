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
use crate::{
    SearchDocumentBody, SearchDocumentHeader, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use hawdb_core::RuntimeMemoryReservation;
use hawdb_storage::file_io as fs;

struct Generated {
    remaining: u64,
    position: usize,
}

impl Read for Generated {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        // Many complete repeated terms, separated by padding. The producer and
        // consumer own only bounded buffers even in the 128-MiB case.
        const WORDS: &[u8] = b"streamed graph document ";
        let count = self.remaining.min(output.len() as u64) as usize;
        for byte in &mut output[..count] {
            *byte = WORDS.get(self.position % 128).copied().unwrap_or(b' ');
            self.position += 1;
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}

fn header() -> SearchDocumentHeader {
    SearchDocumentHeader {
        id: "a".into(),
        title: "title".into(),
        embedding: None,
        metadata: BTreeMap::from([("kind".into(), "memo".into())]),
    }
}

fn task(bytes: u64) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(bytes, 0))
}

fn options(bytes: u64) -> SearchOutOfCoreGenerationBuildOptions {
    SearchOutOfCoreGenerationBuildOptions {
        max_record_bytes: NonZeroU64::new(bytes * 2 + 1024).unwrap(),
        max_segment_uncompressed_bytes: NonZeroU64::new(bytes * 2 + 4096).unwrap(),
        lexical_max_document_source_bytes: NonZeroU64::new(bytes + 1024).unwrap(),
        lexical_max_document_tokens: NonZeroUsize::new(8_000_000).unwrap(),
        lexical_build_memory_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
        ..Default::default()
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn public_build_candidates_and_verified_transfer_exceed_their_memory_reservations() {
    for bytes in [8, 32, 128].map(|mib| mib * 1024 * 1024) {
        let root = super::super::tests::test_dir("public_streamed_document");
        let build_task = task(16 * 1024 * 1024);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create_with_context(&root, options(bytes), build_task)
                .unwrap();
        let memory = writer.memory_for_test();
        writer
            .push_reader(
                header(),
                Generated {
                    remaining: bytes,
                    position: 0,
                },
                SearchDocumentBody {
                    bytes,
                    expected_checksum: None,
                },
            )
            .unwrap();
        let report = writer.finish().unwrap();
        assert_eq!(report.document_count, 1);
        assert_eq!(report.resident_document_count, 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        let config = SearchOutOfCoreConfig {
            max_uncompressed_segment_bytes: NonZeroU64::new(bytes * 2 + 4096).unwrap(),
            max_hydrated_bytes: NonZeroU64::MIN,
            ..Default::default()
        };
        let reader = SearchOutOfCoreReader::open_with_config(&root, config).unwrap();
        let query = super::super::tests::options(1, None);
        let candidates = reader
            .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
            .unwrap();
        assert_eq!(candidates.result.hits.len(), 1);
        assert!(reader
            .search_with_options("graph", None, SearchMode::Text, query)
            .is_err());
        let mut body = reader
            .open_verified_body(
                &candidates.result.hits[0],
                SearchBodyReadOptions {
                    max_body_bytes: NonZeroU64::new(bytes).unwrap(),
                    ..Default::default()
                },
                task(4 * 1024 * 1024),
            )
            .unwrap();
        assert_eq!(body.header(), &header());
        assert_eq!(body.body_bytes(), bytes);
        assert!(!body.is_complete());
        let mut expected = Generated {
            remaining: bytes,
            position: 0,
        };
        let mut actual_buffer = [0; 8192];
        let mut expected_buffer = [0; 8192];
        let mut digest = Crc32cHasher::new();
        loop {
            let count = body.read(&mut actual_buffer).unwrap();
            let expected_count = expected.read(&mut expected_buffer).unwrap();
            assert_eq!(count, expected_count);
            assert_eq!(&actual_buffer[..count], &expected_buffer[..count]);
            digest.update(&actual_buffer[..count]);
            if count == 0 {
                break;
            }
        }
        assert!(body.is_complete());
        assert_eq!(body.body_checksum(), digest.finish());
        drop(body);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn reader_input_failures_poison_the_writer_without_publishing() {
    for (body, declared, checksum) in [
        (&b"short"[..], 6, None),
        (&b"excess"[..], 5, None),
        (&b"\xff"[..], 1, None),
        (&b"valid"[..], 5, Some(u64::MAX)),
    ] {
        let root = super::super::tests::test_dir("streamed_input_failure");
        let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options(8192)).unwrap();
        assert!(writer
            .push_reader(
                header(),
                body,
                SearchDocumentBody {
                    bytes: declared,
                    expected_checksum: checksum
                }
            )
            .is_err());
        assert!(writer
            .push_reader(
                header(),
                &b""[..],
                SearchDocumentBody {
                    bytes: 0,
                    expected_checksum: None
                }
            )
            .is_err());
        assert!(writer.finish().is_err());
        assert!(SearchOutOfCoreReader::open(&root).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn streamed_mutable_lifecycle_keeps_large_bodies_out_of_operation_memory() {
    let bytes = 32 * 1024 * 1024;
    let root = super::super::tests::test_dir("streamed_mutable_lifecycle");
    let config = SearchOutOfCoreConfig {
        max_uncompressed_segment_bytes: NonZeroU64::new(bytes * 2 + 4096).unwrap(),
        max_hydrated_bytes: NonZeroU64::MIN,
        max_reanalysis_working_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        max_reanalysis_document_tokens: NonZeroUsize::new(8_000_000).unwrap(),
        ..Default::default()
    };
    let source_policy =
        crate::SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes + 1024).unwrap()).unwrap();
    let open = || {
        SearchOutOfCoreReader::open_with_source_policy(
            &root,
            config.clone(),
            Default::default(),
            source_policy,
        )
        .unwrap()
    };
    let source = || Generated {
        remaining: bytes,
        position: 0,
    };
    let declared = SearchDocumentBody {
        bytes,
        expected_checksum: None,
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    writer.push_reader(header(), source(), declared).unwrap();
    writer.finish().unwrap();
    let original = open();
    let query = super::super::tests::options(8, None);
    let original_candidate = original
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .remove(0);
    let mut appended = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &original,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut second = header();
    second.id = "b".into();
    appended.upsert_reader(second, source(), declared).unwrap();
    let (_, build, metrics) = appended.finish().unwrap();
    assert_eq!(build.document_count, 2);
    assert_eq!(metrics.streamed_documents, 0);
    let before_compaction = open();
    let before = before_compaction
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap();
    let compacted = SearchOutOfCoreGenerationWriter::prepare_segment_compaction_with_context(
        &before_compaction,
        crate::SearchOutOfCoreSegmentCompactionPolicy::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap()
    .unwrap()
    .finish()
    .unwrap();
    assert_eq!(compacted.source_read_metrics().streamed_documents, 2);
    assert_eq!(compacted.source_read_metrics().hydrated_documents, 0);
    let base = open();
    let after = base
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap();
    assert_eq!(
        before
            .result
            .hits
            .iter()
            .map(|hit| &hit.scores)
            .collect::<Vec<_>>(),
        after
            .result
            .hits
            .iter()
            .map(|hit| &hit.scores)
            .collect::<Vec<_>>()
    );
    let mut replacement = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &base,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut changed = header();
    changed.title = "replacement".into();
    replacement
        .upsert_reader(changed, source(), declared)
        .unwrap();
    let (_, build, metrics) = replacement.finish().unwrap();
    assert_eq!(build.document_count, 2);
    assert_eq!(metrics.streamed_body_bytes, bytes);
    assert_eq!(metrics.hydrated_documents, 0);
    let replaced = open();
    let visible = replaced
        .search_candidates_with_options("replacement", None, SearchMode::Text, query.clone())
        .unwrap();
    assert_eq!(visible.result.hits.len(), 1);
    assert_eq!(visible.result.hits[0].scores.id, "a");
    let mut repeated = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &replaced,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut changed_again = header();
    changed_again.title = "secondreplacement".into();
    repeated
        .upsert_reader(changed_again, source(), declared)
        .unwrap();
    repeated.finish().unwrap();
    let repeated = open();
    assert!(repeated
        .search_candidates_with_options("replacement", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .is_empty());
    assert_eq!(
        repeated
            .search_candidates_with_options(
                "secondreplacement",
                None,
                SearchMode::Text,
                query.clone()
            )
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    let mut deleted = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &repeated,
        options(bytes),
        task(8 * 1024 * 1024),
    )
    .unwrap();
    deleted.delete("a").unwrap();
    let (_, build, metrics) = deleted.finish().unwrap();
    assert_eq!(build.document_count, 1);
    assert_eq!(metrics.streamed_body_bytes, bytes);
    let current = open();
    let visible = current
        .search_candidates_with_options("graph", None, SearchMode::Text, query)
        .unwrap();
    assert_eq!(visible.result.hits.len(), 1);
    assert_eq!(visible.result.hits[0].scores.id, "b");
    assert!(current
        .open_verified_body(
            &original_candidate,
            SearchBodyReadOptions::default(),
            task(4 * 1024 * 1024)
        )
        .is_err());
    let mut old_body = original
        .open_verified_body(
            &original_candidate,
            SearchBodyReadOptions {
                max_body_bytes: NonZeroU64::new(bytes).unwrap(),
                ..Default::default()
            },
            task(4 * 1024 * 1024),
        )
        .unwrap();
    assert_eq!(
        std::io::copy(&mut old_body, &mut std::io::sink()).unwrap(),
        bytes
    );
    assert!(old_body.is_complete());
    drop(old_body);
    let mut restored = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &current,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    restored
        .upsert_reader(header(), source(), declared)
        .unwrap();
    restored.finish().unwrap();
    let restored = open();
    let visible = restored
        .search_candidates_with_options(
            "graph",
            None,
            SearchMode::Text,
            super::super::tests::options(8, None),
        )
        .unwrap();
    assert_eq!(visible.result.hits.len(), 2);
    assert_eq!(visible.result.hits[0].scores.id, "a");
    assert_eq!(visible.result.hits[1].scores.id, "b");
    drop(restored);
    drop(current);
    drop(repeated);
    drop(replaced);
    drop(base);
    drop(before_compaction);
    drop(original);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "full-text-search")]
#[test]
fn distinct_term_retractions_spill_and_reopen_below_their_encoded_size() {
    struct Distinct {
        position: usize,
        bytes: usize,
    }
    impl Read for Distinct {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            let count = output.len().min(self.bytes - self.position);
            for byte in &mut output[..count] {
                let column = self.position % 512;
                let token = self.position / 512;
                *byte = match column {
                    0 => b't',
                    1..=8 => b'0' + ((token / 10usize.pow((8 - column) as u32)) % 10) as u8,
                    _ => b' ',
                };
                self.position += 1;
            }
            Ok(count)
        }
    }
    let bytes = 32 * 1024 * 1024;
    let root = super::super::tests::test_dir("streamed_distinct_retractions");
    let mut build_options = options(bytes);
    build_options.lexical_build_memory_bytes = NonZeroU64::new(256 * 1024).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        build_options.clone(),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    writer
        .push_reader(
            header(),
            Distinct {
                position: 0,
                bytes: bytes as usize,
            },
            SearchDocumentBody {
                bytes,
                expected_checksum: None,
            },
        )
        .unwrap();
    writer.finish().unwrap();
    let config = SearchOutOfCoreConfig {
        max_hydrated_bytes: NonZeroU64::MIN,
        max_mutation_working_bytes: NonZeroU64::new(64 * 1024).unwrap(),
        max_reanalysis_working_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        ..Default::default()
    };
    let source_policy =
        crate::SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes + 1024).unwrap()).unwrap();
    let reader = SearchOutOfCoreReader::open_with_source_policy(
        &root,
        config.clone(),
        Default::default(),
        source_policy,
    )
    .unwrap();
    let mut update = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &reader,
        build_options,
        task(8 * 1024 * 1024),
    )
    .unwrap();
    update.delete("a").unwrap();
    update.finish().unwrap();
    let reopened = SearchOutOfCoreReader::open_with_source_policy(
        &root,
        config,
        Default::default(),
        source_policy,
    )
    .unwrap();
    assert_eq!(reopened.document_count(), 0);
    assert!(reopened.manifest.mutation_runs[0].len > 1024 * 1024);
    let query = super::super::tests::options(1, None);
    assert!(reopened
        .search_candidates_with_options("t00000100", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .is_empty());
    assert_eq!(
        reader
            .search_candidates_with_options("t00000100", None, SearchMode::Text, query)
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    drop(reopened);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
