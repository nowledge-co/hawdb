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

use super::super::{checksum, ArtifactBuilder, LexicalProjectionConfig, ARTIFACT_HEADER};
use super::*;
use crate::{SearchDocument, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader};
use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-block-encoding-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ObservedWriter {
    bytes: Vec<u8>,
    max_request: usize,
    short_write: usize,
    stop_after: usize,
    interrupt: bool,
    zero: bool,
}

impl Default for ObservedWriter {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            max_request: 0,
            short_write: usize::MAX,
            stop_after: usize::MAX,
            interrupt: false,
            zero: false,
        }
    }
}

impl Write for ObservedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.max_request = self.max_request.max(bytes.len());
        if std::mem::take(&mut self.interrupt) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.bytes.len() >= self.stop_after {
            return if self.zero {
                Ok(0)
            } else {
                Err(io::Error::other("injected block write failure"))
            };
        }
        let length = bytes
            .len()
            .min(self.short_write)
            .min(self.stop_after - self.bytes.len());
        self.bytes.extend_from_slice(&bytes[..length]);
        Ok(length)
    }

    fn flush(&mut self) -> io::Result<()> {
        panic!("block encoding must not add a per-block flush");
    }
}

// Independent legacy wire oracle; do not use the production encoding helpers.
fn reference(entries: Entries<'_>, generation: u64, block_id: u64) -> Vec<u8> {
    let mut bytes = b"SKNLEX01".to_vec();
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes.extend_from_slice(&block_id.to_le_bytes());
    let (tag, count) = match entries {
        Entries::Documents(values) => (1, values.len()),
        Entries::Postings(values) => (2, values.len()),
    };
    bytes.push(tag);
    bytes.extend_from_slice(&(count as u32).to_le_bytes());
    let text = |bytes: &mut Vec<u8>, value: &str| {
        bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    };
    match entries {
        Entries::Documents(values) => {
            for (id, length) in values {
                text(&mut bytes, id);
                bytes.extend_from_slice(&length.to_le_bytes());
            }
        }
        Entries::Postings(values) => {
            for posting in values {
                text(&mut bytes, &posting.term);
                text(&mut bytes, &posting.document_id);
                bytes.extend_from_slice(&posting.term_frequency.to_le_bytes());
                bytes.extend_from_slice(&posting.document_len.to_le_bytes());
            }
        }
    }
    bytes
}

fn assert_wire(entries: Entries<'_>, generation: u64, block_id: u64, short_write: usize) {
    let expected = reference(entries, generation, block_id);
    let mut writer = ObservedWriter {
        short_write,
        interrupt: true,
        ..Default::default()
    };
    let descriptor = write_block(
        &mut writer,
        generation,
        block_id,
        24,
        expected.len() as u64,
        entries,
    )
    .unwrap();
    assert_eq!(writer.bytes, expected);
    assert!(writer.max_request <= 8192);
    assert_eq!(descriptor.length, expected.len() as u64);
    assert_eq!(descriptor.checksum, checksum(&expected));
    assert_eq!(descriptor.block_id, block_id);
    assert_eq!(descriptor.offset, 24);
    let (count, kind, first, last) = match entries {
        Entries::Documents(values) => (
            values.len(),
            BlockKind::Documents,
            values.first().unwrap().0.as_str(),
            values.last().unwrap().0.as_str(),
        ),
        Entries::Postings(values) => (
            values.len(),
            BlockKind::Postings,
            values.first().unwrap().term.as_str(),
            values.last().unwrap().term.as_str(),
        ),
    };
    assert_eq!(descriptor.entry_count as usize, count);
    assert_eq!(descriptor.kind, kind);
    assert_eq!(&descriptor.min_key, first);
    assert_eq!(&descriptor.max_key, last);

    let mut rejected = ObservedWriter::default();
    let error = write_block(
        &mut rejected,
        generation,
        block_id,
        24,
        expected.len() as u64 - 1,
        entries,
    )
    .unwrap_err();
    assert!(error.to_string().contains("byte block, exceeding"));
    assert!(rejected.bytes.is_empty());
    assert_eq!(rejected.max_request, 0);
}

fn postings() -> Vec<Posting> {
    vec![
        Posting {
            term: "alpha".into(),
            document_id: "a".into(),
            term_frequency: 3,
            document_len: 9,
        },
        Posting {
            term: "alpha".into(),
            document_id: "b".into(),
            term_frequency: 2,
            document_len: 7,
        },
        Posting {
            term: "\u{4e2d}\u{6587}".into(),
            document_id: "z\u{e9}".into(),
            term_frequency: u32::MAX,
            document_len: u32::MAX,
        },
    ]
}

#[test]
fn blocks_preserve_wire_descriptors_and_exact_admission() {
    let documents = vec![("a".into(), 0), ("z\u{e9}".into(), u32::MAX)];
    let postings = postings();
    for entries in [Entries::Documents(&documents), Entries::Postings(&postings)] {
        for short_write in [1, 2, 7, usize::MAX] {
            assert_wire(entries, u64::MAX, 19, short_write);
        }
    }
}

#[test]
fn long_fields_are_written_in_bounded_chunks() {
    let id = format!("a{}z", "\u{e9}".repeat(20_000));
    let documents = vec![(id.clone(), 1)];
    let postings = vec![Posting {
        term: id.clone().into(),
        document_id: id,
        term_frequency: 1,
        document_len: 1,
    }];
    assert_wire(Entries::Documents(&documents), 5, 0, usize::MAX);
    assert_wire(Entries::Postings(&postings), 5, 1, 127);
}

#[test]
fn cancellation_during_short_writes_or_after_the_last_write_rejects_the_block() {
    use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};

    struct CancelWriter {
        output: ObservedWriter,
        cancel_at: usize,
        cancellation: RuntimeCancellationToken,
    }

    impl Write for CancelWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = self.output.write(bytes)?;
            if self.output.bytes.len() >= self.cancel_at {
                self.cancellation.cancel();
            }
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.output.flush()
        }
    }

    let documents = vec![("x".repeat(20_000), 1)];
    let entries = Entries::Documents(&documents);
    let expected = reference(entries, 7, 2);
    for cancel_at in [1, 8192, expected.len()] {
        let cancellation = RuntimeCancellationToken::new();
        let task = RuntimeTaskContext::without_deadline(cancellation.clone());
        let mut writer = CancelWriter {
            output: ObservedWriter {
                short_write: 127,
                ..Default::default()
            },
            cancel_at,
            cancellation,
        };
        let error = write_block_with_context(
            &mut writer,
            7,
            2,
            24,
            expected.len() as u64,
            entries,
            Some(&task),
        )
        .unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert_eq!(writer.output.bytes, expected[..writer.output.bytes.len()]);
        assert!(writer.output.max_request <= SPILL_IO_BUFFER_BYTES);
        assert!(writer.output.bytes.len() <= cancel_at + 126);
    }
}

#[test]
fn every_write_fault_preserves_only_the_written_prefix() {
    let documents = vec![("doc-\u{4e2d}".into(), 3), ("doc-z".into(), 9)];
    let postings = postings();
    for entries in [Entries::Documents(&documents), Entries::Postings(&postings)] {
        let expected = reference(entries, 7, 2);
        for cut in 0..expected.len() {
            for zero in [false, true] {
                let mut writer = ObservedWriter {
                    short_write: 3,
                    stop_after: cut,
                    zero,
                    interrupt: true,
                    ..Default::default()
                };
                assert!(
                    write_block(&mut writer, 7, 2, 24, expected.len() as u64, entries).is_err()
                );
                assert_eq!(writer.bytes, expected[..cut]);
            }
        }
    }
}

#[test]
fn representation_failures_happen_before_writing() {
    let documents = vec![("id".into(), 1)];
    for (offset, block_id) in [(u64::MAX, 0), (0, u64::MAX)] {
        let mut writer = ObservedWriter::default();
        assert!(write_block(
            &mut writer,
            1,
            block_id,
            offset,
            u64::MAX,
            Entries::Documents(&documents)
        )
        .is_err());
        assert_eq!(writer.max_request, 0);
    }
    for entries in [Entries::Documents(&[]), Entries::Postings(&[])] {
        let mut writer = ObservedWriter::default();
        assert!(write_block(&mut writer, 1, 0, 24, u64::MAX, entries).is_err());
        assert_eq!(writer.max_request, 0);
    }
    let mut counter = CountingWriter(u64::MAX);
    assert!(counter.write(b"x").is_err());
    assert_eq!(counter.0, u64::MAX);
    if let Some(count) = (u32::MAX as usize).checked_add(1) {
        let mut writer = ObservedWriter::default();
        assert!(encode_block_header(&mut writer, 1, 0, BlockKind::Documents, count).is_err());
        assert_eq!(writer.max_request, 0);
    }
}

#[test]
fn artifact_builder_preserves_block_boundaries_and_statistics() {
    let fixture = Fixture::new();
    let path = fixture.0.join("artifact.hawdb");
    let config = LexicalProjectionConfig {
        target_block_bytes: NonZeroU64::new(24).unwrap(),
        ..Default::default()
    };
    let mut builder = ArtifactBuilder::new(&path, 11, config).unwrap();
    let documents: Vec<(String, u32)> = vec![
        ("a".into(), 9),
        ("b".into(), 7),
        ("z\u{e9}".into(), u32::MAX),
    ];
    for (id, length) in &documents {
        builder.push_document(id, *length).unwrap();
    }
    builder.finish_documents().unwrap();
    let postings = postings();
    for posting in &postings {
        builder.push_posting(posting).unwrap();
    }
    builder.merge_postings(&[], config).unwrap();
    let summary = builder.finish().unwrap();
    let actual = fs::read(&path).unwrap();
    assert_eq!(
        summary
            .blocks
            .iter()
            .map(|block| (block.kind, block.entry_count))
            .collect::<Vec<_>>(),
        vec![
            (BlockKind::Documents, 2),
            (BlockKind::Documents, 1),
            (BlockKind::Postings, 1),
            (BlockKind::Postings, 1),
            (BlockKind::Postings, 1),
        ]
    );
    let mut expected = ARTIFACT_HEADER.to_vec();
    expected.extend_from_slice(&11u64.to_le_bytes());
    let mut document_index = 0;
    let mut posting_index = 0;
    for (block_id, block) in summary.blocks.iter().enumerate() {
        assert_eq!(block.block_id, block_id as u64);
        let count = block.entry_count as usize;
        let entries = match block.kind {
            BlockKind::Documents => {
                let entries =
                    Entries::Documents(&documents[document_index..document_index + count]);
                document_index += count;
                entries
            }
            BlockKind::Postings => {
                let entries = Entries::Postings(&postings[posting_index..posting_index + count]);
                posting_index += count;
                entries
            }
        };
        let encoded = reference(entries, 11, block.block_id);
        assert_eq!(block.offset as usize, expected.len());
        assert_eq!(block.length as usize, encoded.len());
        assert_eq!(block.checksum, checksum(&encoded));
        expected.extend_from_slice(&encoded);
    }
    assert_eq!(actual, expected);
    assert_eq!(summary.len, actual.len() as u64);
    assert_eq!(summary.checksum, checksum(&actual));
    assert_eq!(document_index, documents.len());
    assert_eq!(posting_index, postings.len());
    assert_eq!(summary.posting_count, 3);
    assert_eq!(
        summary
            .term_statistics
            .iter()
            .map(|value| (value.term.as_str(), value.document_frequency))
            .collect::<Vec<_>>(),
        vec![("alpha", 2), ("\u{4e2d}\u{6587}", 1)]
    );
}

#[test]
fn rejected_public_generation_preserves_published_artifacts() {
    let fixture = Fixture::new();
    let document = |id: String| SearchDocument {
        id,
        title: String::new(),
        content: "alpha".into(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let mut writer =
        SearchOutOfCoreGenerationWriter::create(&fixture.0, Default::default()).unwrap();
    writer.push(document("kept".into())).unwrap();
    writer.finish().unwrap();
    let files = || {
        fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                assert!(path.is_file(), "unexpected staging directory: {path:?}");
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(&path).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let before = files();
    let mut writer =
        SearchOutOfCoreGenerationWriter::create(&fixture.0, Default::default()).unwrap();
    writer.push(document("x".repeat(2 * 1024 * 1024))).unwrap();
    let error = writer.finish().unwrap_err();
    assert!(
        error.to_string().contains("byte block, exceeding"),
        "{error}"
    );
    assert_eq!(files(), before);
    let reader = SearchOutOfCoreReader::open(&fixture.0).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(&["kept".into()])
            .unwrap()
            .documents,
        vec![document("kept".into())],
    );
    let mut writer =
        SearchOutOfCoreGenerationWriter::create(&fixture.0, Default::default()).unwrap();
    writer.push(document("recovered".into())).unwrap();
    writer.finish().unwrap();
    let current = SearchOutOfCoreReader::open(&fixture.0).unwrap();
    assert_eq!(
        current
            .hydrate_documents(&["recovered".into()])
            .unwrap()
            .documents,
        vec![document("recovered".into())],
    );
    assert_eq!(
        reader
            .hydrate_documents(&["kept".into()])
            .unwrap()
            .documents,
        vec![document("kept".into())],
    );
}

fn campaign(cases: usize) {
    let mut seed = 0x392b10c_u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for case in 0..cases {
        let count = (next() % 12 + 1) as usize;
        let documents = (0..count)
            .map(|index| {
                (
                    format!(
                        "{index:04}-{}-\u{4e2d}\u{e9}",
                        "x".repeat((next() % 80) as usize)
                    ),
                    next() as u32,
                )
            })
            .collect::<Vec<_>>();
        let postings = documents
            .iter()
            .map(|(id, length)| Posting {
                term: format!("term-{}-\u{1f980}", next() % 7).into(),
                document_id: id.clone(),
                term_frequency: next() as u32,
                document_len: *length,
            })
            .collect::<Vec<_>>();
        for entries in [Entries::Documents(&documents), Entries::Postings(&postings)] {
            let generation = next();
            let block_id = case as u64;
            assert_wire(entries, generation, block_id, (next() % 23 + 1) as usize);
            let expected = reference(entries, generation, block_id);
            let cut = next() as usize % expected.len();
            let mut writer = ObservedWriter {
                stop_after: cut,
                short_write: 3,
                ..Default::default()
            };
            assert!(write_block(
                &mut writer,
                generation,
                block_id,
                24,
                expected.len() as u64,
                entries
            )
            .is_err());
            assert_eq!(writer.bytes, expected[..cut]);
        }
    }
}

#[test]
fn block_encoding_differential_smoke() {
    campaign(24);
}

#[test]
#[ignore = "explicit local block encoding differential campaign"]
fn block_encoding_differential_campaign() {
    campaign(512);
}
