use super::*;
use crate::{decode_search_document_line, encode_embedding, encode_metadata, encode_string};
use std::collections::BTreeMap;

fn document() -> SearchDocument {
    SearchDocument {
        id: "message:0042".to_string(),
        title: "\u{56fe}\u{6570}\u{636e}\u{5e93}\t\n".to_string(),
        content: "bytes\0 UTF-8 \u{1f680} ; =".repeat(80),
        embedding: Some(vec![
            0.0,
            -0.0,
            f32::MIN,
            f32::MAX,
            f32::MIN_POSITIVE,
            f32::from_bits(1),
        ]),
        metadata: BTreeMap::from([
            (String::new(), String::new()),
            ("key;=\t".to_string(), "value\n\0\u{00e9}".to_string()),
        ]),
    }
}

// Preserve the preceding wire construction as an independent sizing/format
// oracle. The production writer no longer builds these intermediate strings.
fn legacy_line(document: &SearchDocument) -> String {
    format!(
        "doc\t{}\t{}\t{}\t{}\t{}\n",
        encode_string(&document.id),
        encode_string(&document.title),
        encode_string(&document.content),
        encode_embedding(document.embedding.as_deref()),
        encode_metadata(&document.metadata),
    )
}

#[test]
fn sizing_and_single_buffer_encoding_preserve_v1_wire_bytes() {
    for embedding in [None, Some(Vec::new()), document().embedding] {
        let input = SearchDocument {
            embedding,
            ..document()
        };
        let expected = legacy_line(&input);
        assert_eq!(encoded_len(&input, u64::MAX, None).unwrap(), expected.len());
        let bytes = encode_bounded(&input, expected.len() as u64, None).unwrap();
        assert_eq!(bytes, expected);
        assert_eq!(crate::encode_search_document_line(&input), expected);
        let decoded = decode_search_document_line(&bytes).unwrap();
        assert_eq!(legacy_line(&decoded), expected);
    }
}

#[test]
fn oversized_or_cancelled_records_fail_before_output_allocation() {
    let input = document();
    let exact = legacy_line(&input).len() as u64;
    allocation_evidence::take();
    for limit in [0, 8, 9, 16, exact - 1] {
        assert!(encode_bounded(&input, limit, None).is_err());
        assert_eq!(
            allocation_evidence::take(),
            0,
            "allocated under limit {limit}"
        );
    }
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    assert!(matches!(
        encode_bounded(&input, exact, Some(&task)),
        Err(SkeinError::Execution(_))
    ));
    assert_eq!(allocation_evidence::take(), 0);
    encode_bounded(&input, exact, None).unwrap();
    assert_eq!(allocation_evidence::take(), 1);
}

#[test]
fn corrupt_non_ascii_hex_and_excess_fields_fail_without_panicking_or_echoing_input() {
    for hex in ["0\u{00e9}a", "a\u{4e2d}", "\u{1f680}", "gg", "0"] {
        for field in 0..4 {
            let mut fields = ["61", "62", "63", "", "6b=76"].map(str::to_string);
            match field {
                0..=2 => fields[field] = hex.to_string(),
                _ => fields[4] = format!("6b={hex}"),
            }
            let line = format!("doc\t{}\n", fields.join("\t"));
            assert!(decode_search_document_line(&line).is_err(), "{line:?}");
        }
    }
    let extra_fields = format!("doc\t{}", "\t".repeat(100_000));
    assert!(
        decode_search_document_line(&extra_fields)
            .unwrap_err()
            .to_string()
            .len()
            < 128
    );
}

#[test]
fn record_size_arithmetic_fails_closed_on_overflow() {
    let mut size = Size {
        bytes: u64::MAX,
        limit: u64::MAX,
    };
    assert!(size.add(1).is_err());
    assert_eq!(size.bytes, u64::MAX);
    assert!(size.write_str("x").is_err());
}

#[test]
#[ignore = "dedicated local production record codec campaign"]
fn bounded_record_bytes_campaign() {
    let mut seed = 0x206c_0dec_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        seed
    };
    let mut accepted = 0;
    let mut rejected = 0;
    for case in 0..25_000 {
        let mut input = document();
        input.content = format!(
            "record-{case:05}:{}",
            "content".repeat(next() as usize % 40)
        );
        input.embedding = Some(
            (0..next() as usize % 16)
                .map(|_| f32::from_bits(next() as u32))
                .collect(),
        );
        let expected = legacy_line(&input);
        let length = expected.len() as u64;
        assert_eq!(
            encode_bounded(&input, length, None).unwrap(),
            expected,
            "case {case}"
        );
        allocation_evidence::take();
        assert!(
            encode_bounded(&input, length - 1, None).is_err(),
            "case {case}"
        );
        assert_eq!(allocation_evidence::take(), 0, "case {case}");
        let mut bytes = expected.into_bytes();
        let offset = next() as usize % bytes.len();
        match case % 6 {
            0 => {}
            1 => bytes[offset] ^= next() as u8 | 1,
            2 => {
                bytes.remove(offset);
            }
            3 => bytes.splice(4..6, "0\u{00e9}a".bytes()).for_each(drop),
            4 => bytes.truncate(offset),
            _ => bytes.insert(offset, b'\t'),
        }
        let result = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|line| decode_search_document_line(line).ok());
        if let Some(decoded) = result {
            accepted += 1;
            let canonical = legacy_line(&decoded);
            assert_eq!(
                encode_bounded(&decoded, canonical.len() as u64, None).unwrap(),
                canonical
            );
            let again = decode_search_document_line(&canonical).unwrap();
            assert_eq!(legacy_line(&again), canonical);
            let required = crate::build_memory::document_bytes(&again).unwrap();
            let task = RuntimeTaskContext::default().with_memory_reservation(
                skein_core::RuntimeMemoryReservation::new(required as u64, 0),
            );
            let memory = crate::build_memory::BuildMemory::new(&task).unwrap();
            let owned = memory
                .decode_document(&canonical, again.metadata.len())
                .unwrap();
            assert_eq!(owned.retained_bytes(), required);
            assert_eq!(legacy_line(&owned), canonical);
            drop(owned);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            let task = task.with_memory_reservation(skein_core::RuntimeMemoryReservation::new(
                required as u64 - 1,
                0,
            ));
            let memory = crate::build_memory::BuildMemory::new(&task).unwrap();
            crate::build_memory::decode_evidence::take();
            assert!(memory
                .decode_document(&canonical, again.metadata.len())
                .is_err());
            assert_eq!(crate::build_memory::decode_evidence::take(), 0);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        } else {
            rejected += 1;
        }
    }
    assert!(accepted > 4_000 && rejected > 4_000);
    eprintln!("record seed=0x206c0dec: cases=25000 accepted={accepted} rejected={rejected}");
}
