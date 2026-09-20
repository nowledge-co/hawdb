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
use std::collections::BTreeMap;

fn legacy_record(document: &SearchDocument) -> Vec<u8> {
    crate::document_encoding::legacy_encode(document).into_bytes()
}

fn legacy_frame(record: &[u8]) -> Vec<u8> {
    let mut frame = (record.len() as u64).to_le_bytes().to_vec();
    frame.extend_from_slice(&checksum_bytes(record).to_le_bytes());
    frame.extend_from_slice(record);
    frame
}

struct FaultWriter {
    bytes: Vec<u8>,
    remaining: usize,
    max_write: usize,
    interrupted: bool,
}

impl Write for FaultWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if std::mem::take(&mut self.interrupted) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "injected frame write failure",
            ));
        }
        let written = bytes.len().min(self.remaining).min(self.max_write);
        self.bytes.extend_from_slice(&bytes[..written]);
        self.remaining -= written;
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        panic!("frame append must not introduce per-document flushes");
    }
}

fn source(seed: u64, content: String) -> SearchDocument {
    SearchDocument {
        id: format!("doc-{seed:06}"),
        title: "\u{4e2d}\u{6587};=\n\u{1f980}".into(),
        content,
        embedding: Some(vec![0.0, -0.0, f32::MIN_POSITIVE, 1.25]),
        metadata: BTreeMap::from([("key;=\t".into(), format!("value-{seed}\0"))]),
    }
}

#[test]
fn controlled_frame_admits_scratch_and_stops_after_a_bounded_write() {
    struct CancellingWriter<'a> {
        bytes: Vec<u8>,
        task: &'a RuntimeTaskContext,
    }
    impl Write for CancellingWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            if self.bytes.len() >= crate::document_encoding::HEX_BUFFER_BYTES {
                self.task.cancellation().cancel();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let document = source(0, "body ".repeat(32 * 1024));
    let encoding = DocumentEncoding::new(&document).unwrap();
    let mut digest = DocumentsDigest::default();
    digest.add_bytes(b"earlier records");
    let before = digest.finish();
    let mut output = CancellingWriter {
        bytes: Vec::new(),
        task: &task,
    };
    let error =
        write_frame_with_context(&mut output, &encoding, &mut digest, &memory, &task).unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert!(output.bytes.len() <= 2 * crate::document_encoding::HEX_BUFFER_BYTES);
    assert!(output.bytes.len() < encoding.len());
    assert_eq!(digest.finish(), before);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);

    let task = RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(
            crate::document_encoding::HEX_BUFFER_BYTES as u64 - 1,
            0,
        ),
    );
    let memory = BuildMemory::new(&task).unwrap();
    let mut output = Vec::new();
    assert!(write_frame_with_context(&mut output, &encoding, &mut digest, &memory, &task).is_err());
    assert!(output.is_empty());
    assert_eq!(digest.finish(), before);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn spool_frame_preserves_wire_and_digest_at_every_failed_write_boundary() {
    let document = source(0, "\u{4e2d}abc\n".into());
    let record = legacy_record(&document);
    let expected = legacy_frame(&record);
    let encoding = DocumentEncoding::new(&document).unwrap();
    let mut initial = DocumentsDigest::default();
    initial.add_bytes(b"earlier records");
    let mut complete = initial;
    complete.add_bytes(&record);
    for boundary in 0..=expected.len() {
        let mut output = FaultWriter {
            bytes: Vec::new(),
            remaining: boundary,
            max_write: 3,
            interrupted: true,
        };
        let mut digest = initial;
        let result = write_frame(&mut output, &encoding, &mut digest);
        assert_eq!(output.bytes, expected[..boundary]);
        if boundary == expected.len() {
            result.unwrap();
            assert_eq!(digest.finish(), complete.finish());
        } else {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("injected frame write failure"));
            assert_eq!(digest.finish(), initial.finish(), "boundary={boundary}");
        }
    }
}

#[test]
fn spool_frame_unwind_does_not_commit_document_digest() {
    struct PanicWriter;
    impl Write for PanicWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            panic!("injected frame panic");
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let document = source(0, "body".into());
    let mut digest = DocumentsDigest::default();
    digest.add_bytes(b"prefix");
    let before = digest.finish();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        write_frame(
            &mut PanicWriter,
            &DocumentEncoding::new(&document).unwrap(),
            &mut digest,
        )
    }));
    assert!(result.is_err());
    assert_eq!(digest.finish(), before);
}

#[test]
#[ignore = "explicit local Bazel spool-encoding differential campaign"]
fn spool_encoding_differential_campaign() {
    let mut random = 0x392_5a001_u64;
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    let mut digest = DocumentsDigest::default();
    let mut reference = DocumentsDigest::default();
    for seed in 0..256 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let length = [0, 1, 4095, 4096, 4097, 8191, 8192, 8193][seed as usize % 8];
        let unit = ["a", "\u{4e2d}", "\u{1f980}", ";=\t\0\n"][random as usize % 4];
        let mut document = source(seed, unit.repeat(length));
        document.embedding = (seed % 3 != 0).then(|| vec![f32::from_bits(random as u32), -0.0]);
        if seed % 5 == 0 {
            document
                .metadata
                .insert(String::new(), document.title.clone());
        }
        let record = legacy_record(&document);
        let frame = legacy_frame(&record);
        let encoding = DocumentEncoding::new(&document).unwrap();
        let mut output = FaultWriter {
            bytes: Vec::new(),
            remaining: frame.len(),
            max_write: 1 + random as usize % 127,
            interrupted: seed % 2 == 0,
        };
        write_frame(&mut output, &encoding, &mut digest).unwrap();
        reference.add_bytes(&record);
        assert_eq!(digest.finish(), reference.finish(), "seed={seed}");
        assert_eq!(output.bytes, frame, "seed={seed}");
        actual.extend(output.bytes);
        expected.extend_from_slice(&frame);
        let boundary = random as usize % frame.len();
        let mut failing = FaultWriter {
            bytes: Vec::new(),
            remaining: boundary,
            max_write: 7,
            interrupted: false,
        };
        let before = digest.finish();
        assert!(write_frame(&mut failing, &encoding, &mut digest).is_err());
        assert_eq!(failing.bytes, frame[..boundary], "seed={seed}");
        assert_eq!(digest.finish(), before);
    }
    assert_eq!(actual, expected);
}
