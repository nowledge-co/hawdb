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

use super::super::tests::{compressed, envelope, fixture};
use super::*;
use crate::build_memory::BuildMemory;
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use std::io::{Cursor, Seek, SeekFrom};

fn verify(
    bytes: &[u8],
    segment: &SearchSegmentDescriptorEntry,
    output: &mut impl Write,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Receipt> {
    let admission = ReadAdmission {
        memory,
        task,
        max_header_bytes: 4096,
    };
    read_validated(
        bytes,
        bytes.len() as u64,
        checksum_bytes(bytes),
        u64::MAX,
        Some(admission),
        |text| select(text, segment, "a", output, u64::MAX, admission),
    )
}

fn source(id: &str, body: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: "title".into(),
        content: body.into(),
        embedding: Some(vec![1.0, -2.5]),
        metadata: BTreeMap::from([("key".into(), "value".into())]),
    }
}

#[test]
fn selected_body_preserves_wire_grammar_and_validates_unselected_suffix() {
    let sources = [
        source("a", "text\t\n\0\u{e9}\u{4e2d}\u{1f980}"),
        source("b", "later"),
    ];
    let (text, segment) = fixture(&sources);
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(8 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    for text in [
        text.clone(),
        text.replace('\n', "\r\n"),
        text.trim_end_matches('\n').into(),
        format!("\n{text}\nHAWDB_SEARCH_SEGMENT_V1\n"),
    ] {
        let bytes = envelope(
            &compressed(text.as_bytes()),
            text.len(),
            checksum_bytes(text.as_bytes()),
        );
        let mut output = Vec::new();
        let receipt = verify(&bytes, &segment, &mut output, &memory, &task).unwrap();
        assert_eq!(output, sources[0].content.as_bytes());
        assert_eq!(receipt.header.id, sources[0].id);
        assert_eq!(receipt.header.title, sources[0].title);
        assert_eq!(receipt.header.embedding, sources[0].embedding);
        assert_eq!(receipt.header.metadata, sources[0].metadata);
        assert_eq!(receipt.body_bytes, output.len() as u64);
        assert_eq!(receipt.body_checksum, checksum_bytes(&output));
        drop(receipt);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    for damaged in [
        format!("{text}bad document\n"),
        text.replace("6c61746572", "ff"),
        text.replace("6c61746572", "0"),
    ] {
        let bytes = envelope(
            &compressed(damaged.as_bytes()),
            damaged.len(),
            checksum_bytes(damaged.as_bytes()),
        );
        let mut private_output = Vec::new();
        assert!(verify(&bytes, &segment, &mut private_output, &memory, &task).is_err());
        assert_eq!(
            private_output,
            sources[0].content.as_bytes(),
            "private prefixes never constitute a successful receipt"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn verified_body_exceeds_its_entire_operation_memory() {
    const BODY_BYTES: usize = 128 * 1024 * 1024;
    let (_, segment) = fixture(&[source("a", "")]);
    // The producer and compressed fixture are separate owners. Neither stores
    // a full raw/hex body, and the result consumer uses a counted private file.
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1).unwrap();
    let mut digest = Crc32cHasher::new();
    let mut inflated_bytes = 0usize;
    let mut write = |bytes: &[u8]| {
        encoder.write_all(bytes).unwrap();
        digest.update(bytes);
        inflated_bytes += bytes.len();
    };
    write(b"HAWDB_SEARCH_SEGMENT_V1\ndoc\t61\t7469746c65\t");
    let chunk = b"61".repeat(4096);
    for _ in 0..BODY_BYTES / 4096 {
        write(&chunk);
    }
    write(b"\t1,-2.5\t6b6579=76616c7565\n");
    let encoded = encoder.finish().unwrap();
    let bytes = envelope(&encoded, inflated_bytes, digest.finish());
    let root = crate::test_temp_dir().join(format!(
        "hawdb-selected-body-{}-{}",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let descriptors =
        hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let path = root.join("private-body");
    let mut output = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(4 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let receipt = verify(&bytes, &segment, &mut output, &memory, &task).unwrap();
    assert_eq!(receipt.body_bytes, BODY_BYTES as u64);
    assert!(memory.ledger.snapshot().peak_bytes < BODY_BYTES);
    output.seek(SeekFrom::Start(0)).unwrap();
    let mut reread = CheckedReader::new(output);
    assert_eq!(
        io::copy(&mut reread, &mut io::sink()).unwrap(),
        BODY_BYTES as u64
    );
    assert_eq!(reread.digest.finish(), receipt.body_checksum);
    drop(reread);
    drop(receipt);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_file(path).unwrap();
    assert_eq!(descriptors.metrics().open, 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn verified_body_admission_and_late_checksum_fail_without_receipt() {
    let (text, segment) = fixture(&[source("a", "small")]);
    let bytes = envelope(
        &compressed(text.as_bytes()),
        text.len(),
        checksum_bytes(text.as_bytes()),
    );
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        max_header_bytes: 4096,
    };
    let mut output = Vec::new();
    assert!(read_validated(
        &bytes[..],
        bytes.len() as u64,
        checksum_bytes(&bytes) ^ 1,
        u64::MAX,
        Some(admission),
        |text| select(text, &segment, "a", &mut output, 100, admission)
    )
    .is_err());
    assert_eq!(output, b"small");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let short = RuntimeTaskContext::default().with_memory_reservation(
        RuntimeMemoryReservation::new((4 * INPUT_BYTES - 1) as u64, 0),
    );
    let memory = BuildMemory::new(&short).unwrap();
    let mut input = Cursor::new(&bytes);
    let admission = ReadAdmission {
        memory: &memory,
        task: &short,
        max_header_bytes: 4096,
    };
    assert!(read_validated(
        &mut input,
        bytes.len() as u64,
        checksum_bytes(&bytes),
        u64::MAX,
        Some(admission),
        |_| Ok(())
    )
    .is_err());
    assert_eq!(input.position(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
#[cfg(unix)]
fn counted_publication_scan_reopen_and_cleanup_preserve_owned_bytes_on_denial() {
    use hawdb_storage::durability::durable_replace_file;
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;
    let root = crate::test_temp_dir().join(format!(
        "hawdb-body-descriptors-{}-{}",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let project = ProjectFileDescriptors::acquire(&root, 6).unwrap();
    let active = root.join("active");
    let private = root.join("private");
    fs::write(&active, b"old").unwrap();
    fs::write(&private, b"complete candidate").unwrap();
    File::open(&private).unwrap().sync_all().unwrap();
    let mut old_pin = File::open(&active).unwrap();
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(65536, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let owned =
        crate::build_memory::path::OwnedPath::join(&root, Path::new("private"), &memory, &task)
            .unwrap();
    let mut live_bytes = 18u64;
    let mut cumulative_bytes = live_bytes;
    let held: Vec<_> = (0..5).map(|_| File::open(&active).unwrap()).collect();
    for error in [
        fs::read_dir(&root).unwrap_err(),
        File::open(&owned).unwrap_err(),
        fs::remove_file(&owned).unwrap_err(),
        durable_replace_file(&owned, &active).unwrap_err(),
    ] {
        assert!(matches!(
            HawDBError::from(error),
            HawDBError::FileDescriptors(_)
        ));
        assert_eq!(live_bytes, 18);
        assert_eq!(std::fs::read(&private).unwrap(), b"complete candidate");
        assert_eq!(std::fs::read(&active).unwrap(), b"old");
    }
    assert!(memory.ledger.snapshot().used_bytes > 0);
    drop(held);
    let quota = project.reserve(2).unwrap();
    // Another thread cannot borrow this operation's reserved publication wave.
    let competing_root = root.clone();
    let competitors = std::thread::spawn(move || {
        (0..3)
            .map(|_| File::open(competing_root.join("active")).unwrap())
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    assert_eq!(project.metrics().open + project.metrics().reserved, 6);
    // rename has two nested path admissions and directory sync uses one. All
    // borrow the reserved wave instead of requiring unreserved headroom.
    durable_replace_file(&owned, &active).unwrap();
    assert_eq!(project.metrics().reserved, 2);
    assert_eq!(std::fs::read(&active).unwrap(), b"complete candidate");
    let mut pinned = String::new();
    old_pin.read_to_string(&mut pinned).unwrap();
    assert_eq!(pinned, "old");
    let mut reopened = File::open(&active).unwrap();
    let mut selected = String::new();
    reopened.read_to_string(&mut selected).unwrap();
    assert_eq!(selected, "complete candidate");
    drop(reopened);
    drop(quota);
    drop(competitors);
    // The successful publication transfers private-byte ownership to the
    // active generation. A separate abandoned stage exercises retained debt.
    live_bytes = 0;
    fs::write(&owned, b"complete candidate").unwrap();
    live_bytes += 18;
    cumulative_bytes += 18;
    let held: Vec<_> = (0..5).map(|_| File::open(&active).unwrap()).collect();
    let error = HawDBError::from(fs::remove_file(&owned).unwrap_err());
    assert!(matches!(error, HawDBError::FileDescriptors(_)));
    assert_eq!(live_bytes, 18);
    drop(held);
    fs::remove_file(&owned).unwrap();
    live_bytes -= 18;
    assert_eq!(live_bytes, 0);
    assert_eq!(cumulative_bytes, 36);
    drop(owned);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(old_pin);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
    assert!(project.metrics().high_water <= 6);
    fs::remove_file(active).unwrap();
    fs::remove_dir(root).unwrap();
}

#[test]
fn body_hex_decoder_preserves_every_small_buffer_boundary() {
    let original = "a\u{e9}\u{4e2d}\u{1f980}\n";
    let bytes = format!("{}\tsuffix", crate::encode_string(original));
    for capacity in 1..=bytes.len() {
        let mut text = BufReader::with_capacity(capacity, bytes.as_bytes());
        let mut body = HexBody {
            text: &mut text,
            done: false,
        };
        let mut decoded = Vec::new();
        body.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, original.as_bytes());
        let mut suffix = String::new();
        text.read_to_string(&mut suffix).unwrap();
        assert_eq!(suffix, "suffix");
    }
}

#[test]
fn selected_body_cancellation_and_sink_failure_release_admission() {
    struct CancelSink(crate::RuntimeCancellationToken);
    impl Write for CancelSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.cancel();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    struct BrokenSink;
    impl Write for BrokenSink {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (text, segment) = fixture(&[source("a", "small")]);
    let bytes = envelope(
        &compressed(text.as_bytes()),
        text.len(),
        checksum_bytes(text.as_bytes()),
    );
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let token = task.cancellation().clone();
    let error = verify(&bytes, &segment, &mut CancelSink(token), &memory, &task)
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let task = RuntimeTaskContext::default();
    let error = verify(&bytes, &segment, &mut BrokenSink, &memory, &task)
        .err()
        .unwrap();
    assert!(error.to_string().contains("broken pipe"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn decoder_admission_failure_is_not_masked_by_a_second_frame_parse() {
    let text = "x".repeat(1024 * 1024);
    let bytes = envelope(
        &compressed(text.as_bytes()),
        text.len(),
        checksum_bytes(text.as_bytes()),
    );
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let error = read_validated(
        &bytes[..],
        bytes.len() as u64,
        checksum_bytes(&bytes),
        u64::MAX,
        Some(ReadAdmission {
            memory: &memory,
            task: &task,
            max_header_bytes: 4096,
        }),
        |input| Ok(io::copy(input, &mut io::sink())?),
    )
    .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert!(!error.to_string().contains("frame magic"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
