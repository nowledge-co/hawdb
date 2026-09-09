use super::*;
use std::collections::BTreeMap;
use std::io::Write as _;

fn document() -> SearchDocument {
    SearchDocument {
        id: "id".into(),
        title: "\u{4e2d}\u{6587}".into(),
        content: "a;=\t\n\0\u{1f980}".into(),
        embedding: Some(vec![0.0, -0.0, 1.25, f32::MAX]),
        metadata: BTreeMap::from([("k;=".into(), "v\n".into())]),
    }
}

struct ShortWriter {
    output: Vec<u8>,
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
                "injected disk full",
            ));
        }
        let count = bytes.len().min(self.remaining).min(3);
        self.output.extend_from_slice(&bytes[..count]);
        self.remaining -= count;
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn bounded_encoding_preserves_short_writes_and_original_errors() {
    let document = document();
    let expected = super::tests::legacy_encode(&document);
    let encoding = DocumentEncoding::new(&document).unwrap();
    for boundary in 0..=expected.len() {
        let mut writer = ShortWriter {
            output: Vec::new(),
            remaining: boundary,
            interrupt: true,
        };
        let result = encoding.write_to(&mut writer);
        assert_eq!(writer.output, expected.as_bytes()[..boundary]);
        if boundary == expected.len() {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::StorageFull);
            assert_eq!(error.to_string(), "injected disk full");
        }
    }
    struct ZeroWriter;
    impl io::Write for ZeroWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Ok(0)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert_eq!(
        encoding.write_to(&mut ZeroWriter).unwrap_err().kind(),
        io::ErrorKind::WriteZero
    );
}

#[test]
fn bounded_encoding_does_not_materialize_large_hex_fields() {
    struct BoundedWriter {
        bytes: usize,
        largest: usize,
        digest: skein_integrity::Crc32cHasher,
    }
    impl io::Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            assert!(bytes.len() <= HEX_BUFFER_BYTES);
            self.bytes += bytes.len();
            self.largest = self.largest.max(bytes.len());
            self.digest.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut document = document();
    document.content = "\u{4e2d}\u{1f980}".repeat(300_000);
    let expected = super::tests::legacy_encode(&document);
    let attempts = ENCODING_ATTEMPTS.get();
    let mut writer = BoundedWriter {
        bytes: 0,
        largest: 0,
        digest: skein_integrity::Crc32cHasher::new(),
    };
    DocumentEncoding::new(&document)
        .unwrap()
        .write_to(&mut writer)
        .unwrap();
    assert_eq!(ENCODING_ATTEMPTS.get(), attempts);
    assert_eq!(writer.bytes, expected.len());
    assert_eq!(writer.largest, HEX_BUFFER_BYTES);
    assert_eq!(
        writer.digest.finish(),
        crate::checksum_bytes(expected.as_bytes())
    );
    writer.flush().unwrap();
}

#[test]
fn bounded_encoding_rejects_internal_length_drift_without_overwriting() {
    let document = document();
    let length = DocumentEncoding::new(&document).unwrap().len();
    for bytes in [length - 1, length + 1] {
        let encoding = DocumentEncoding {
            document: &document,
            bytes,
        };
        let mut output = Vec::new();
        assert_eq!(
            encoding.write_to(&mut output).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(output.len() <= bytes);
    }
}
