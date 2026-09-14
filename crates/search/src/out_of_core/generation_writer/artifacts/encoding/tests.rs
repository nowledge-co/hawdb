use super::*;
use crate::document_encoding::SegmentKind;
use crate::{decode_search_snapshot_text_bounded, encode_search_snapshot_text, SearchDocument};
use std::collections::BTreeMap;
use std::fmt::Write as _;

// Preserve the previous materializing grammar independently of the shared
// counting/streaming implementation, including optional vector ordinals.
fn legacy_text(documents: &[SearchDocument], kind: SegmentKind) -> String {
    let (mut text, mut ordinal) = match kind {
        SegmentKind::Documents => (String::from("SKEIN_SEARCH_SEGMENT_V1\n"), 0),
        SegmentKind::Metadata {
            vector_ordinal_base,
        } => (
            String::from("SKEIN_SEARCH_METADATA_SEGMENT_V1\n"),
            vector_ordinal_base,
        ),
        SegmentKind::Vectors {
            vector_ordinal_base,
        } => (
            String::from("SKEIN_SEARCH_VECTOR_SEGMENT_V1\n"),
            vector_ordinal_base,
        ),
    };
    for document in documents {
        match kind {
            SegmentKind::Documents => {
                text.push_str(&crate::document_encoding::legacy_encode(document))
            }
            SegmentKind::Metadata { .. } => {
                writeln!(
                    text,
                    "meta\t{}\t{}\t{}",
                    crate::encode_string(&document.id),
                    document
                        .embedding
                        .as_ref()
                        .map(|_| ordinal.to_string())
                        .unwrap_or_else(|| "-".into()),
                    crate::encode_metadata(&document.metadata)
                )
                .unwrap();
            }
            SegmentKind::Vectors { .. } => {
                if let Some(embedding) = document.embedding.as_deref() {
                    writeln!(
                        text,
                        "vector\t{ordinal}\t{}\t{}",
                        crate::encode_string(&document.id),
                        crate::encode_embedding(Some(embedding))
                    )
                    .unwrap();
                }
            }
        }
        if document.embedding.is_some() {
            ordinal = ordinal.saturating_add(1);
        }
    }
    text
}

fn check(documents: &[SearchDocument], base: u64) {
    for kind in [
        SegmentKind::Documents,
        SegmentKind::Metadata {
            vector_ordinal_base: base,
        },
        SegmentKind::Vectors {
            vector_ordinal_base: base,
        },
    ] {
        let text = legacy_text(documents, kind);
        let compressed = zstd::stream::encode_all(text.as_bytes(), 3).unwrap();
        let mut expected = format!(
            "SKEIN_COMPRESSED_V1\ncodec\tzstd\nuncompressed_checksum\t{}\ncompressed_checksum\t{}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
            crate::checksum_bytes(text.as_bytes()), crate::checksum_bytes(&compressed), text.len(), compressed.len(),
        ).into_bytes();
        expected.extend_from_slice(&compressed);
        assert_eq!(encode_search_snapshot_text(&text).unwrap(), expected);
        let encoding = SegmentEncoding::new(documents, kind).unwrap();
        assert_eq!(encoding.len(), text.len());
        let actual = encode_segment_payload(
            &encoding,
            9,
            kind.name(),
            text.len() as u64,
            expected.len() as u64,
        )
        .unwrap();
        assert_eq!(actual, expected, "{} compressed bytes differ", kind.name());
        assert_eq!(
            decode_search_snapshot_text_bounded(&actual, text.len() as u64).unwrap(),
            text
        );
        for (raw_limit, compressed_limit, expected_error) in [
            (text.len() - 1, expected.len(), "bytes, exceeding"),
            (text.len(), expected.len() - 1, "compressed bytes"),
            (text.len(), 1, "compressed bytes"),
        ] {
            let error = encode_segment_payload(
                &encoding,
                9,
                kind.name(),
                raw_limit as u64,
                compressed_limit as u64,
            )
            .unwrap_err();
            assert!(error.to_string().contains(expected_error), "{error}");
        }
    }
}

#[test]
fn segment_envelopes_match_legacy_at_exact_and_short_budgets() {
    check(&[], 0);
    let mut documents = Vec::new();
    for embedding in [
        None,
        Some(vec![]),
        Some(vec![
            0.0,
            -0.0,
            f32::MIN,
            f32::MAX,
            f32::from_bits(1),
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
        ]),
    ] {
        documents.push(SearchDocument {
            id: "\0\t\n;=\u{4e2d}\u{1f980}".into(),
            title: "cafe\u{301}".into(),
            content: "\r\n\u{fffd}\u{10ffff}".into(),
            embedding,
            metadata: BTreeMap::from([
                (String::new(), String::new()),
                (";=\t\0".into(), "value".into()),
            ]),
        });
    }
    for base in [0, 9, 99, u64::MAX - 1] {
        check(&documents, base);
    }
}

#[test]
fn streamed_segments_cross_hex_and_compression_buffer_boundaries() {
    let mut state = 0x392_c0de_u64;
    for length in [4095, 4096, 4097, 131071, 131072, 131073, 1024 * 1024] {
        let content: String = (0..length)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                char::from(b' ' + ((state >> 32) % 95) as u8)
            })
            .collect();
        check(
            &[SearchDocument {
                id: "large".into(),
                title: content.clone(),
                content,
                embedding: Some((0..1000).map(|i| i as f32 / 7.0).collect()),
                metadata: BTreeMap::from([("large".into(), "value".repeat(length / 5))]),
            }],
            1000,
        );
    }
}

#[test]
fn seeded_segment_records_preserve_vector_ordinals_and_metadata() {
    let mut state = 0x392_5eed_u64;
    for case in 0..128 {
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) as u32
        };
        let documents = (0..case % 17)
            .map(|ordinal| SearchDocument {
                id: format!("document-{ordinal}"),
                title: format!("title-{}", next()),
                content: "mixed\u{4e2d}\u{1f980}\0\t\n".repeat(next() as usize % 80),
                embedding: (next() % 3 != 0)
                    .then(|| (0..next() % 8).map(|_| f32::from_bits(next())).collect()),
                metadata: (0..next() % 8)
                    .map(|_| (format!("key-{}", next() % 4), format!("value-{}", next())))
                    .collect(),
            })
            .collect::<Vec<_>>();
        check(&documents, u64::from(next()));
    }
}

#[test]
fn compressed_buffer_rejects_growth_before_allocation_or_mutation() {
    let mut buffer = CompressedBuffer {
        bytes: Vec::new(),
        limit: 4,
        digest: Crc32cHasher::new(),
    };
    buffer.write_all(b"1234").unwrap();
    let capacity = buffer.bytes.capacity();
    let digest = buffer.digest.finish();
    assert!(buffer.write_all(b"5").is_err());
    assert!(buffer.reserve(usize::MAX).is_err());
    assert_eq!(buffer.bytes, b"1234");
    assert_eq!(buffer.bytes.capacity(), capacity);
    assert_eq!(buffer.digest.finish(), digest);
}

#[test]
fn small_compressed_writes_reuse_capacity_without_reserving_the_whole_budget() {
    let mut buffer = CompressedBuffer {
        bytes: Vec::new(),
        limit: 256 * 1024 * 1024,
        digest: Crc32cHasher::new(),
    };
    for _ in 0..1024 {
        buffer.write_all(&[0; 1024]).unwrap();
        assert!(buffer.bytes.capacity() <= buffer.bytes.len() * 2);
    }
    assert_eq!(buffer.bytes.len(), 1024 * 1024);
}

#[test]
fn digest_writer_accounts_only_accepted_bytes_and_propagates_failure() {
    struct ShortWriter(Vec<u8>);
    impl Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.0.len() == 3 {
                return Err(io::Error::other("injected failure"));
            }
            self.0.push(bytes[0]);
            Ok(1)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = ShortWriter(Vec::new());
    let mut digest = Crc32cHasher::new();
    let error = DigestWriter {
        writer: &mut writer,
        digest: &mut digest,
    }
    .write_all(b"1234")
    .unwrap_err();
    assert_eq!(error.to_string(), "injected failure");
    assert_eq!(writer.0, b"123");
    assert_eq!(digest.finish(), crate::checksum_bytes(b"123"));
}
