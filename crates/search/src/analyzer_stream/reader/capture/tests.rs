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
use crate::build_memory::BuildMemory;
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use hawdb_storage::file_io::{self as fs, File};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn checksum(bytes: &[u8]) -> u64 {
    let mut digest = Crc32cHasher::new();
    digest.update(bytes);
    digest.finish()
}

#[test]
fn capture_is_single_pass_and_preserves_utf8_across_reads() {
    let text = "header \u{4e2d}\u{6587} \u{1f642} tail";
    for width in 1..=text.len() {
        let mut input = super::super::tests::ShortReads {
            remaining: text.as_bytes(),
            first: 0,
            width,
        };
        let mut output = Vec::new();
        let receipt = capture(
            &mut input,
            &mut output,
            text.len() as u64,
            Some(checksum(text.as_bytes())),
            Control::default(),
        )
        .unwrap();
        assert_eq!(output, text.as_bytes());
        assert_eq!(receipt.bytes, text.len() as u64);
        assert_eq!(receipt.checksum, checksum(text.as_bytes()));
        assert!(receipt.needs_chinese);
        assert!(input.remaining.is_empty());
    }
}

#[test]
fn capture_never_seals_mismatched_or_invalid_input() {
    for (bytes, expected, hash) in [
        (&b"short"[..], 6, None),
        (&b"excess"[..], 5, None),
        (&b"valid"[..], 5, Some(checksum(b"other"))),
        (&b"\xe4\xb8"[..], 2, None),
        (&b"\xff"[..], 1, None),
    ] {
        let mut output = Vec::new();
        assert!(capture(
            &mut &bytes[..],
            &mut output,
            expected,
            hash,
            Control::default()
        )
        .is_err());
    }
}

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
}

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = crate::test_temp_dir().join(format!(
            "hawdb-source-capture-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
        Self { root, project }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn captured_body_exceeds_memory_and_cleanup_denial_keeps_bytes_for_retry() {
    let fixture = Fixture::new();
    let path = fixture.root.join("source.stage");
    let bytes = 128 * 1024 * 1024;
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(64 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut output = File::create(&path).unwrap();
    let receipt = capture(
        &mut io::repeat(b'x').take(bytes),
        &mut output,
        bytes,
        None,
        Control {
            memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
            task: Some(&task),
            ..Control::default()
        },
    )
    .unwrap();
    output.sync_all().unwrap();
    drop(output);
    assert_eq!(receipt.bytes, bytes);
    assert!(!receipt.needs_chinese);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);

    let mut input = File::open(&path).unwrap();
    let mut read_bytes = 0;
    let mut digest = Crc32cHasher::new();
    let mut buffer = [0; BUFFER_BYTES];
    loop {
        let count = input.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        assert!(buffer[..count].iter().all(|byte| *byte == b'x'));
        read_bytes += count as u64;
        digest.update(&buffer[..count]);
    }
    drop(input);
    assert_eq!(read_bytes, bytes);
    assert_eq!(digest.finish(), receipt.checksum);

    let mut competitors: Vec<_> = (0..4).map(|_| File::open(&path).unwrap()).collect();
    assert_eq!(fixture.project.metrics().open, 4);
    let denied = HawDBError::from(fs::remove_file(&path).unwrap_err());
    assert!(matches!(denied, HawDBError::FileDescriptors(_)), "{denied}");
    assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes);
    drop(competitors.pop());
    fs::remove_file(&path).unwrap();
    assert!(!path.exists());
    drop(competitors);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn capture_admits_scratch_before_reading_and_refunds_after_failure() {
    struct UnexpectedRead;
    impl Read for UnexpectedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("read before admission")
        }
    }
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new((BUFFER_BYTES + 3) as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let result = capture(
        &mut UnexpectedRead,
        &mut io::sink(),
        0,
        None,
        Control {
            memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
            task: Some(&task),
            ..Control::default()
        },
    );
    assert!(result.is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);

    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new((BUFFER_BYTES + 4) as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let control = Control {
        memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
        task: Some(&task),
        ..Control::default()
    };
    assert!(capture(&mut io::empty(), &mut io::sink(), 0, None, control).is_ok());
    assert!(capture(&mut &b"wrong"[..], &mut io::sink(), 5, Some(0), control).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn capture_requires_successful_eof_and_propagates_sink_failures() {
    struct LateFailure {
        sent: bool,
    }
    impl Read for LateFailure {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if self.sent {
                return Err(io::Error::other("source failed at EOF"));
            }
            output[..4].copy_from_slice(b"body");
            self.sent = true;
            Ok(4)
        }
    }
    let mut private = Vec::new();
    let error = capture(
        &mut LateFailure { sent: false },
        &mut private,
        4,
        None,
        Control::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("source failed at EOF"));
    assert_eq!(private, b"body");

    struct BrokenSink;
    impl Write for BrokenSink {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("stage write failed"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let error = capture(
        &mut &b"body"[..],
        &mut BrokenSink,
        4,
        None,
        Control::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("stage write failed"));
}
