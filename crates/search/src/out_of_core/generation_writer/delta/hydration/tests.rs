use super::*;
use crate::{
    checksum_bytes, encode_search_document_line, encode_search_snapshot_text, SearchDocument,
};
use skein_core::RuntimeMemoryReservation;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

fn documents(count: usize, bytes: usize) -> Vec<SearchDocument> {
    (0..count)
        .map(|index| SearchDocument {
            id: format!("memory:{index:04}"),
            title: "title".into(),
            content: "x".repeat(bytes),
            embedding: Some(vec![1.5, -2.25]),
            metadata: BTreeMap::from([("field".into(), "value".into())]),
        })
        .collect()
}

fn fixture(documents: &[SearchDocument]) -> (String, SearchSegmentDescriptorEntry) {
    let mut text = String::from("SKEIN_SEARCH_SEGMENT_V1\n");
    for document in documents {
        text.push_str(&encode_search_document_line(document));
    }
    let descriptor = SearchSegmentDescriptorEntry::from_documents(
        0,
        &documents.iter().collect::<Vec<_>>(),
        &BTreeSet::new(),
    );
    (text, descriptor)
}

#[test]
fn delta_hydration_matches_legacy_grammar_and_retains_callback_documents() {
    let expected = documents(9, 101);
    let (text, segment) = fixture(&expected);
    for text in [
        text.clone(),
        text.replace('\n', "\r\n"),
        text.trim_end_matches('\n').to_owned(),
    ] {
        let bytes = encode_search_snapshot_text(&text).unwrap();
        assert_eq!(
            crate::decode_search_segment_documents_bounded(&bytes, u64::MAX).unwrap(),
            expected
        );
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut output = Vec::new();
        let peak = read_segment(
            Cursor::new(&bytes),
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            u64::MAX,
            &memory,
            &task,
            &mut |document| {
                output.push(document);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            output.iter().map(|doc| &doc.document).collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            peak,
            expected.iter().map(search_document_bytes).max().unwrap()
        );
        assert!(
            memory.ledger.snapshot().used_bytes
                >= output.iter().map(|doc| doc.retained_bytes()).sum()
        );
        drop(output);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn delta_hydration_streams_a_segment_larger_than_the_operation_budget() {
    let expected = documents(128, 64 * 1024);
    let (text, segment) = fixture(&expected);
    let bytes = encode_search_snapshot_text(&text).unwrap();
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(6 * 1024 * 1024, 0));
    assert!(text.len() > 2 * 6 * 1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    // Fixed ledger class metadata belongs to the operation, not this measured pass.
    drop(memory.input.reserve(1).unwrap());
    drop(memory.spool.reserve(1).unwrap());
    drop(memory.retained.reserve(1).unwrap());
    let _serial = crate::test_allocation::serial();
    let mut count = 0;
    let (result, allocated) = crate::test_allocation::measure(|| {
        read_segment(
            Cursor::new(&bytes),
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            text.len() as u64,
            &memory,
            &task,
            &mut |document| {
                assert_eq!(document.document, expected[count]);
                count += 1;
                Ok(())
            },
        )
    });
    result.unwrap();
    assert_eq!(count, expected.len());
    let peak = memory.ledger.snapshot().peak_bytes;
    assert!(peak < 4 * 1024 * 1024, "peak={peak}");
    assert!(
        allocated <= peak,
        "requested Rust peak={allocated}, admitted peak={peak}"
    );
    assert_eq!(crate::test_allocation::live(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn delta_hydration_rejects_corrupt_tail_before_callback_error() {
    let expected = documents(4, 17);
    let (text, segment) = fixture(&expected);
    let mut bytes = encode_search_snapshot_text(&text).unwrap();
    let checksum = checksum_bytes(&bytes);
    *bytes.last_mut().unwrap() ^= 1;
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let error = read_segment(
        Cursor::new(&bytes),
        bytes.len() as u64,
        checksum,
        &segment,
        u64::MAX,
        &memory,
        &task,
        &mut |_| Err(invalid("injected consumer failure")),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("payload checksum mismatch"),
        "{error}"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

struct Fussy<'a> {
    bytes: &'a [u8],
    position: usize,
    chunk: usize,
    calls: usize,
    fail_at: Option<usize>,
}

impl Read for Fussy<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        assert!(output.len() <= INPUT_BYTES);
        self.calls += 1;
        if self.calls.is_multiple_of(5) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at == Some(self.position) {
            return Err(io::Error::other("late source read failure"));
        }
        let count = output
            .len()
            .min(self.chunk)
            .min(self.bytes.len() - self.position)
            .min(self.fail_at.unwrap_or(usize::MAX) - self.position);
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

#[test]
fn delta_hydration_keeps_short_read_integrity_and_noncanonical_headers() {
    let expected = documents(4, 7);
    let (text, segment) = fixture(&expected);
    let ordinary = encode_search_snapshot_text(&text).unwrap();
    let split = ordinary
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .unwrap();
    let header = std::str::from_utf8(&ordinary[..split])
        .unwrap()
        .replace("_len\t", "_len\t+000000000000000");
    let mut bytes = header.into_bytes();
    bytes.extend_from_slice(&ordinary[split..]);
    assert_eq!(
        crate::decode_search_segment_documents_bounded(&bytes, u64::MAX).unwrap(),
        expected
    );
    for chunk in [1, 17, 8192] {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut count = 0;
        let input = Fussy {
            bytes: &bytes,
            position: 0,
            chunk,
            calls: 0,
            fail_at: None,
        };
        read_segment(
            input,
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            u64::MAX,
            &memory,
            &task,
            &mut |document| {
                assert_eq!(document.document, expected[count]);
                count += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, expected.len());
        let input = Fussy {
            bytes: &bytes,
            position: 0,
            chunk,
            calls: 0,
            fail_at: Some(bytes.len() - 1),
        };
        let error = read_segment(
            input,
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            u64::MAX,
            &memory,
            &task,
            &mut |_| Err(invalid("consumer rejected")),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("late source read failure"),
            "{error}"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn delta_hydration_stops_for_cancellation_and_retained_callback_pressure() {
    let expected = documents(8, 64 * 1024);
    let (text, segment) = fixture(&expected);
    let bytes = encode_search_snapshot_text(&text).unwrap();
    for cancel in [false, true] {
        let budget = 6 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let mut retained = None;
        let mut held = None;
        let mut count = 0;
        let result = read_segment(
            Cursor::new(&bytes),
            bytes.len() as u64,
            checksum_bytes(&bytes),
            &segment,
            u64::MAX,
            &memory,
            &task,
            &mut |document| {
                count += 1;
                assert_eq!(count, 1);
                retained = Some(document);
                if cancel {
                    task.cancellation().cancel();
                } else {
                    held = Some(
                        memory
                            .retained
                            .reserve(budget as usize - memory.ledger.snapshot().used_bytes)
                            .unwrap(),
                    );
                }
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(count, 1);
        assert!(memory.ledger.snapshot().used_bytes > 0);
        drop(retained);
        drop(held);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn delta_hydration_requires_outer_integrity_for_semantically_equivalent_headers() {
    let expected = documents(2, 7);
    let (text, segment) = fixture(&expected);
    let original = encode_search_snapshot_text(&text).unwrap();
    let split = original
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .unwrap();
    let mut lines = std::str::from_utf8(&original[..split])
        .unwrap()
        .lines()
        .collect::<Vec<_>>();
    lines.swap(2, 3);
    let mut bytes = lines.join("\n").into_bytes();
    bytes.extend_from_slice(&original[split..]);
    assert_eq!(bytes.len(), original.len());
    assert_eq!(
        crate::decode_search_segment_documents_bounded(&bytes, u64::MAX).unwrap(),
        expected
    );
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let error = read_segment(
        Cursor::new(&bytes),
        bytes.len() as u64,
        checksum_bytes(&original),
        &segment,
        u64::MAX,
        &memory,
        &task,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("payload checksum mismatch"),
        "{error}"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
