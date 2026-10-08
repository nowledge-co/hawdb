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
use crate::{SearchDocumentBody, SearchOutOfCoreGenerationWriter};
use hawdb_storage::file_io as fs;

#[path = "fixtures.rs"]
mod fixtures;
use fixtures::{header, options, task, Generated};

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
