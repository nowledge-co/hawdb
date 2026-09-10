use super::*;
use std::cell::Cell;

thread_local! {
    static DECODED_BYTES: Cell<Option<usize>> = const { Cell::new(None) };
}

pub(super) fn record_decoded_bytes(bytes: usize) {
    DECODED_BYTES.set(Some(bytes));
}

fn envelope(payload: &[u8], declared_len: usize, checksum: u64) -> Vec<u8> {
    let mut bytes = format!(
        "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{checksum}\ncompressed_checksum\t{}\nuncompressed_len\t{declared_len}\ncompressed_len\t{}\n\n",
        checksum_bytes(payload), payload.len(),
    ).into_bytes();
    bytes.extend_from_slice(payload);
    bytes
}

fn compressed(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, SEARCH_COMPRESSION_LEVEL).unwrap()
}

fn checked_decode(bytes: &[u8], declared: usize, limit: u64) -> Result<String> {
    DECODED_BYTES.set(None);
    let result = decode_search_snapshot_text_bounded(bytes, limit);
    if let Some(actual) = DECODED_BYTES.get() {
        assert!(
            actual <= declared.saturating_add(1),
            "decoded {actual} bytes for a {declared}-byte declaration"
        );
    }
    result
}

#[test]
fn false_length_stops_inflation_at_the_declared_boundary() {
    let source = vec![b'x'; 2 * 1024 * 1024];
    let payload = compressed(&source);
    for declared in [0, 1, 31, 8192] {
        let bytes = envelope(&payload, declared, checksum_bytes(&source));
        for limit in [4 * 1024 * 1024, u64::MAX] {
            let error = checked_decode(&bytes, declared, limit).unwrap_err();
            assert!(error.to_string().contains("uncompressed length mismatch"));
            assert_eq!(DECODED_BYTES.get(), Some(declared + 1));
        }
    }
}

#[test]
fn valid_envelopes_preserve_exact_and_one_short_reader_admission() {
    for source in [
        String::new(),
        "\0\r\n\t".to_string(),
        "ASCII \u{e9}\u{4e2d}\u{1f980}".to_string(),
        "larger than the decoder scratch ".repeat(8192),
    ] {
        let bytes = encode_search_snapshot_text(&source).unwrap();
        for limit in [source.len() as u64, u64::MAX] {
            assert_eq!(checked_decode(&bytes, source.len(), limit).unwrap(), source);
            assert_eq!(DECODED_BYTES.get(), Some(source.len()));
        }
        if !source.is_empty() {
            let error = checked_decode(&bytes, source.len(), source.len() as u64 - 1).unwrap_err();
            assert!(error.to_string().contains("uncompressed payload requires"));
            assert_eq!(DECODED_BYTES.get(), None);
        }
    }
}

#[test]
fn bounded_inflation_preserves_checksums_lengths_and_utf8_validation() {
    let source = b"small source";
    let payload = compressed(source);
    let checksum = checksum_bytes(source);
    let cases = [
        (
            envelope(&payload, source.len() + 1, checksum),
            "uncompressed length mismatch",
        ),
        (
            envelope(&payload, source.len(), checksum ^ 1),
            "uncompressed checksum mismatch",
        ),
        (
            envelope(&compressed(&[0xff]), 1, checksum_bytes(&[0xff])),
            "not valid UTF-8",
        ),
    ];
    for (bytes, expected_error) in cases {
        let error = decode_search_snapshot_text_bounded(&bytes, 1024).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
    }

    let mut bad_checksum = envelope(&payload, source.len(), checksum);
    *bad_checksum.last_mut().unwrap() ^= 1;
    let error = checked_decode(&bad_checksum, source.len(), 1024).unwrap_err();
    assert!(error.to_string().contains("compressed checksum mismatch"));
    assert_eq!(DECODED_BYTES.get(), None);

    let too_long = envelope(&payload, 1, checksum);
    let error = checked_decode(&too_long, 1, 1).unwrap_err();
    assert!(error
        .to_string()
        .contains("decompressed payload exceeded 1 bytes"));
    assert_eq!(DECODED_BYTES.get(), Some(2));
}

#[test]
fn concatenated_frames_and_trailing_damage_are_not_hidden_by_length_admission() {
    let mut payload = compressed(b"first");
    payload.extend_from_slice(&compressed(b"second"));
    let source = b"firstsecond";
    let bytes = envelope(&payload, source.len(), checksum_bytes(source));
    assert_eq!(
        checked_decode(&bytes, source.len(), u64::MAX).unwrap(),
        "firstsecond"
    );

    let shorter = envelope(&payload, 5, checksum_bytes(b"first"));
    assert!(checked_decode(&shorter, 5, u64::MAX).is_err());
    assert_eq!(DECODED_BYTES.get(), Some(6));

    for end in 0..payload.len() {
        let truncated = envelope(&payload[..end], source.len(), checksum_bytes(source));
        assert!(checked_decode(&truncated, source.len(), u64::MAX).is_err());
    }
    let mut corrupt_tail = compressed(b"first");
    corrupt_tail.extend_from_slice(b"not a zstd frame");
    let bytes = envelope(&corrupt_tail, 5, checksum_bytes(b"first"));
    let error = checked_decode(&bytes, 5, u64::MAX).unwrap_err();
    assert!(error.to_string().contains("zstd decompression failed"));
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-compression-admission-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn public_reopen_rejects_a_false_length_without_inflating_the_whole_snapshot() {
    let root = TestRoot::new();
    let path = root.0.join(SEARCH_SNAPSHOT_FILE);
    let document = SearchDocument {
        id: "document".to_string(),
        title: String::new(),
        content: " ".repeat(128 * 1024),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let body = format!(
        "SKEIN_SEARCH_PROJECTION_V1\n{}",
        encode_search_document_line(&document)
    );
    let text = format!("{body}checksum\t{}\n", checksum_bytes(body.as_bytes()));
    let payload = compressed(text.as_bytes());
    let valid = envelope(&payload, text.len(), checksum_bytes(text.as_bytes()));
    fs::write(&path, &valid).unwrap();
    {
        let index = SearchIndex::open(&root.0).unwrap();
        assert_eq!(index.document("document").unwrap(), &document);
    }

    let invalid = envelope(&payload, 1, checksum_bytes(text.as_bytes()));
    fs::write(&path, &invalid).unwrap();
    DECODED_BYTES.set(None);
    let error = match SearchIndex::open(&root.0) {
        Err(error) => error,
        Ok(_) => panic!("false-length snapshot was accepted"),
    };
    assert!(error.to_string().contains("uncompressed length mismatch"));
    assert_eq!(DECODED_BYTES.get(), Some(2));
    assert_eq!(fs::read(&path).unwrap(), invalid);

    fs::write(&path, &valid).unwrap();
    let restored = SearchIndex::open(&root.0).unwrap();
    assert_eq!(restored.document("document").unwrap(), &document);
}

#[test]
#[ignore = "explicit local compressed-envelope differential campaign"]
fn compressed_envelope_differential_campaign() {
    let mut state = 0x0392_dec0_de01_u64;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        state
    };
    for case in 0..512 {
        let mut source = Vec::new();
        for _ in 0..(next() as usize % 4096) {
            source.extend_from_slice(match next() % 5 {
                0 => b"\0\r\n",
                1 => "\u{4e2d}".as_bytes(),
                2 => "\u{1f980}".as_bytes(),
                3 => b"repeat repeat ",
                _ => b"az09",
            });
        }
        if case % 10 == 8 {
            source.push(0xff);
        }
        let mut payload = compressed(&source);
        let mut declared = source.len();
        let mut checksum = checksum_bytes(&source);
        let mut limit = u64::MAX;
        match case % 10 {
            1 => declared = source.len().saturating_sub(1),
            2 => declared += 1,
            3 => limit = (source.len() as u64).saturating_sub(1),
            4 => checksum ^= 1,
            5 => {
                payload.pop();
            }
            6 => {
                payload.extend_from_slice(&compressed(b"tail"));
                source.extend_from_slice(b"tail");
                declared = source.len();
                checksum = checksum_bytes(&source);
            }
            7 => {
                declared = 0;
                checksum = checksum_bytes(b"");
            }
            9 => {
                let position = next() as usize % payload.len();
                payload[position] ^= 1;
            }
            _ => {}
        }
        // The oracle fully inflates these small fixtures independently of the
        // production reader's declared-length admission and bounded Read path.
        let expected = zstd::stream::decode_all(payload.as_slice())
            .ok()
            .filter(|bytes| bytes.len() == declared && bytes.len() as u64 <= limit)
            .filter(|bytes| checksum_bytes(bytes) == checksum)
            .and_then(|bytes| String::from_utf8(bytes).ok());
        let bytes = envelope(&payload, declared, checksum);
        let actual = checked_decode(&bytes, declared, limit);
        assert_eq!(actual.ok(), expected, "case {case}");
    }
}
