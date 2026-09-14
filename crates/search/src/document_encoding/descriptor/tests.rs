use super::*;
use crate::{
    SearchNumericRange, SearchSegmentDescriptorEntry, SearchSegmentFieldSummary,
    SearchTimestampRange,
};
use std::collections::{BTreeMap, BTreeSet};

struct TestDirectory(std::path::PathBuf);

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn sample() -> SearchSegmentDescriptor {
    SearchSegmentDescriptor {
        target_documents: 256,
        document_count: 3,
        segments: vec![SearchSegmentDescriptorEntry {
            segment_id: 0,
            first_document_id: "a\t\0".into(),
            last_document_id: "z\u{1f980}".into(),
            document_count: 3,
            payload_range: Some(SearchSegmentPayloadRange {
                artifact_id: crate::SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
                offset: 0,
                length: 123,
                checksum: u64::MAX,
            }),
            metadata: BTreeMap::from([(
                "field\t\n;=\u{4e2d}".into(),
                SearchSegmentFieldSummary {
                    present_count: 3,
                    values: BTreeSet::from(["0".into(), "value\0\n\u{1f980}".into()]),
                    numeric_range: Some(SearchNumericRange {
                        min: -0.0,
                        max: f64::MAX,
                    }),
                    timestamp_range: Some(SearchTimestampRange {
                        min_epoch_millis: i64::MIN,
                        max_epoch_millis: i64::MAX,
                    }),
                },
            )]),
        }],
    }
}

fn legacy(descriptor: &SearchSegmentDescriptor) -> Vec<u8> {
    let body = crate::encode_search_segment_descriptor_body(descriptor);
    format!(
        "{body}checksum\t{}\n",
        crate::checksum_bytes(body.as_bytes())
    )
    .into_bytes()
}

fn check(descriptor: &SearchSegmentDescriptor) {
    let expected = legacy(descriptor);
    let encoding = DescriptorEncoding::new(descriptor, expected.len() as u64).unwrap();
    assert_eq!(encoding.len(), expected.len());
    let mut output = Vec::new();
    encoding.write_to(&mut output).unwrap();
    assert_eq!(output, expected);
    assert_eq!(
        crate::decode_search_segment_descriptor_text(std::str::from_utf8(&output).unwrap())
            .unwrap(),
        *descriptor
    );
    let error = DescriptorEncoding::new(descriptor, expected.len() as u64 - 1)
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains(&format!("requires {} bytes", expected.len())),
        "{error}"
    );
}

#[test]
fn descriptor_bytes_match_legacy_at_exact_and_short_limits() {
    check(&SearchSegmentDescriptor {
        target_documents: 256,
        document_count: 0,
        segments: Vec::new(),
    });
    let mut descriptor = sample();
    check(&descriptor);
    descriptor.segments[0].payload_range = None;
    let field = descriptor.segments[0].metadata.values_mut().next().unwrap();
    field.numeric_range = None;
    field.timestamp_range = None;
    field.values.clear();
    check(&descriptor);
}

#[test]
fn empty_dictionary_values_retain_the_legacy_wire_representation() {
    for values in [
        BTreeSet::new(),
        BTreeSet::from([String::new()]),
        BTreeSet::from([String::new(), "a".into()]),
    ] {
        let mut descriptor = sample();
        descriptor.segments[0]
            .metadata
            .values_mut()
            .next()
            .unwrap()
            .values = values;
        let expected = legacy(&descriptor);
        let mut actual = Vec::new();
        DescriptorEncoding::new(&descriptor, expected.len() as u64)
            .unwrap()
            .write_to(&mut actual)
            .unwrap();
        assert_eq!(actual, expected);
        // V3 represents both an empty dictionary and a single empty value as
        // an empty field. Preserve its decoding instead of changing the format.
        assert_eq!(
            crate::decode_search_segment_descriptor_text(std::str::from_utf8(&actual).unwrap())
                .unwrap(),
            crate::decode_search_segment_descriptor_text(std::str::from_utf8(&expected).unwrap())
                .unwrap(),
        );
    }
}

#[test]
fn seeded_descriptors_preserve_ranges_order_and_optional_fields() {
    let mut state = 0x392_d35c_u64;
    for case in 0..256 {
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 32) as u32
        };
        let mut descriptor = sample();
        descriptor.segments[0].metadata.clear();
        for index in 0..case % 17 {
            descriptor.segments[0].metadata.insert(
                format!("key-{index}"),
                SearchSegmentFieldSummary {
                    present_count: next() as usize % 4,
                    values: (0..next() % 8)
                        .map(|_| format!("value-{}-\u{4e2d}\u{1f980}", next()))
                        .collect(),
                    numeric_range: (next() % 2 == 0).then(|| SearchNumericRange {
                        min: -(next() as f64),
                        max: next() as f64 / 3.0,
                    }),
                    timestamp_range: (next() % 2 == 0).then(|| SearchTimestampRange {
                        min_epoch_millis: -(i64::from(next())),
                        max_epoch_millis: i64::from(next()),
                    }),
                },
            );
        }
        check(&descriptor);
    }
}

#[test]
fn multiple_segments_preserve_document_bounds_and_complete_field_summaries() {
    let documents = (0..crate::SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS * 3 + 1)
        .map(|index| {
            let id = format!("document-{index:05}");
            (
                id.clone(),
                SearchDocument {
                    id,
                    title: String::new(),
                    content: String::new(),
                    embedding: None,
                    metadata: BTreeMap::from([
                        ("kind".into(), "Memory".into()),
                        ("rating".into(), index.to_string()),
                        ("created_at".into(), "2026-09-14T12:30:00Z".into()),
                        (
                            "labels".into(),
                            format!("[\"tag-{}\",\"shared\"]", index % 5),
                        ),
                    ]),
                },
            )
        })
        .collect();
    let mut descriptor = SearchSegmentDescriptor::build(&documents);
    assert_eq!(descriptor.segments.len(), 4);
    check(&descriptor);
    let mut offset = 0;
    for segment in &mut descriptor.segments {
        let length = segment.document_count as u64 * 7;
        segment.payload_range = Some(SearchSegmentPayloadRange {
            artifact_id: crate::SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            offset,
            length,
            checksum: segment.segment_id * 13,
        });
        offset += length;
    }
    check(&descriptor);
}

#[test]
fn large_descriptor_writes_use_bounded_chunks() {
    struct BoundedWriter {
        bytes: usize,
        digest: Crc32cHasher,
    }
    impl io::Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(bytes.len() <= HEX_BUFFER_BYTES);
            self.bytes += bytes.len();
            self.digest.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut descriptor = sample();
    descriptor.segments[0]
        .metadata
        .values_mut()
        .next()
        .unwrap()
        .values = BTreeSet::from(["\u{4e2d}\u{1f980}".repeat(150_000)]);
    let expected = legacy(&descriptor);
    let mut writer = BoundedWriter {
        bytes: 0,
        digest: Crc32cHasher::new(),
    };
    DescriptorEncoding::new(&descriptor, expected.len() as u64)
        .unwrap()
        .write_to(&mut writer)
        .unwrap();
    assert_eq!(writer.bytes, expected.len());
    assert_eq!(writer.digest.finish(), crate::checksum_bytes(&expected));
}

#[test]
fn descriptor_short_writes_preserve_prefix_and_error_through_footer() {
    struct ShortWriter {
        bytes: Vec<u8>,
        remaining: usize,
        interrupt: bool,
    }
    impl io::Write for ShortWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if std::mem::take(&mut self.interrupt) {
                return Err(io::ErrorKind::Interrupted.into());
            }
            if self.remaining == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "injected descriptor write failure",
                ));
            }
            let count = self.remaining.min(bytes.len()).min(3);
            self.bytes.extend_from_slice(&bytes[..count]);
            self.remaining -= count;
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let descriptor = sample();
    let expected = legacy(&descriptor);
    let encoding = DescriptorEncoding::new(&descriptor, expected.len() as u64).unwrap();
    for boundary in 0..=expected.len() {
        let mut writer = ShortWriter {
            bytes: Vec::new(),
            remaining: boundary,
            interrupt: true,
        };
        let result = encoding.write_to(&mut writer);
        assert_eq!(writer.bytes, expected[..boundary]);
        if boundary == expected.len() {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::StorageFull);
            assert_eq!(error.to_string(), "injected descriptor write failure");
        }
    }
}

#[test]
fn descriptor_admission_precedes_opening_or_replacing_files() {
    let root = std::env::temp_dir().join(format!(
        "skein-descriptor-admission-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let _cleanup = TestDirectory(root.clone());
    let path = root.join(crate::SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let tmp = path.with_extension("skein.tmp");
    std::fs::write(&path, b"previous descriptor").unwrap();
    std::fs::write(&tmp, b"previous temporary file").unwrap();
    let descriptor = sample();
    let expected = legacy(&descriptor);
    let result = crate::write_search_segment_descriptor_bounded(
        &root,
        &descriptor,
        expected.len() as u64 - 1,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("descriptor requires"));
    assert_eq!(std::fs::read(&path).unwrap(), b"previous descriptor");
    assert_eq!(std::fs::read(&tmp).unwrap(), b"previous temporary file");
    assert_eq!(
        crate::write_search_segment_descriptor_bounded(&root, &descriptor, expected.len() as u64)
            .unwrap(),
        expected.len() as u64
    );
    assert_eq!(std::fs::read(path).unwrap(), expected);
    assert!(!tmp.exists());
    std::fs::remove_dir_all(root).unwrap();
}
