use super::*;

// Raw zstd framing and bitwise CRC are independent of the production encoder.
fn crc(bytes: &[u8]) -> u64 {
    let mut state = u32::MAX;
    for &byte in bytes {
        state ^= u32::from(byte);
        for _ in 0..8 {
            state = (state >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(state & 1));
        }
    }
    u64::from(!state)
}

fn raw_frame(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() <= 65_791);
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd];
    if bytes.len() < 256 {
        frame.extend_from_slice(&[0x20, bytes.len() as u8]);
    } else {
        frame.push(0x60);
        frame.extend_from_slice(&((bytes.len() - 256) as u16).to_le_bytes());
    }
    frame.extend_from_slice(&(((bytes.len() as u32) << 3) | 1).to_le_bytes()[..3]);
    frame.extend_from_slice(bytes);
    frame
}

#[derive(Clone)]
struct Envelope {
    fields: Vec<(&'static str, String)>,
    payload: Vec<u8>,
}

impl Envelope {
    fn new(bytes: &[u8]) -> Self {
        let payload = raw_frame(bytes);
        Self {
            fields: vec![
                ("codec", "zstd".into()),
                ("uncompressed_checksum", crc(bytes).to_string()),
                ("compressed_checksum", crc(&payload).to_string()),
                ("uncompressed_len", bytes.len().to_string()),
                ("compressed_len", payload.len().to_string()),
            ],
            payload,
        }
    }

    fn set(&mut self, key: &str, value: impl ToString) {
        self.fields
            .iter_mut()
            .find(|(name, _)| *name == key)
            .unwrap()
            .1 = value.to_string();
    }

    fn remove(&mut self, key: &str) {
        self.fields.retain(|(name, _)| *name != key);
    }

    fn bytes(&self) -> Vec<u8> {
        let mut bytes = b"SKEIN_COMPRESSED_V1\n".to_vec();
        for (key, value) in &self.fields {
            bytes.extend_from_slice(format!("{key}\t{value}\n").as_bytes());
        }
        bytes.push(b'\n');
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn repair_payload_binding(&mut self) {
        self.set("compressed_checksum", crc(&self.payload));
        self.set("compressed_len", self.payload.len());
    }

    fn assert_error(&self, limit: Option<u64>, expected: &str) {
        assert_eq!(error(&self.bytes(), limit), format!("fixture {expected}"));
    }
}

fn error(bytes: &[u8], limit: Option<u64>) -> String {
    match read_durable_text_bytes_with_limit(bytes, "fixture", limit) {
        Err(SkeinError::Storage(message)) => message,
        Err(other) => panic!("wrong error class: {other}"),
        Ok(_) => panic!("invalid envelope accepted"),
    }
}

#[test]
fn frozen_encoder_and_independent_decoder_fixtures() {
    assert_eq!(crc(b"123456789"), 0xe306_9283);
    let mut encoded = b"SKEIN_COMPRESSED_V1\ncodec\tzstd\nuncompressed_checksum\t910901175\ncompressed_checksum\t3205880307\nuncompressed_len\t3\ncompressed_len\t12\n\n".to_vec();
    encoded.extend_from_slice(&[
        0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x19, 0, 0, b'a', b'b', b'c',
    ]);
    assert_eq!(
        encode_durable_text("abc", DurableCompression::Zstd).unwrap(),
        encoded
    );
    assert_eq!(read_durable_text_bytes(&encoded, "fixture").unwrap(), "abc");
    for text in ["", "abc", "\0\t\n\r", "\u{e9}\u{4e2d}\u{1f680}"] {
        let raw = Envelope::new(text.as_bytes()).bytes();
        assert_eq!(read_durable_text_bytes(&raw, "fixture").unwrap(), text);
        assert_eq!(
            read_durable_text_bytes_with_limit(&raw, "fixture", Some(text.len() as u64)).unwrap(),
            text
        );
    }
}

#[test]
fn header_field_matrix_preserves_errors_and_precedence() {
    let valid = Envelope::new(b"abc");
    let missing = [
        ("codec", "compressed envelope uses unsupported codec"),
        (
            "compressed_len",
            "compressed envelope missing compressed_len",
        ),
        (
            "compressed_checksum",
            "compressed envelope missing compressed_checksum",
        ),
        (
            "uncompressed_len",
            "compressed envelope missing uncompressed_len",
        ),
        (
            "uncompressed_checksum",
            "compressed envelope missing uncompressed_checksum",
        ),
    ];
    let mut cases = 0;
    for (key, expected) in missing {
        let mut fixture = valid.clone();
        fixture.remove(key);
        fixture.assert_error(None, expected);
        cases += 1;
        let mut fixture = valid.clone();
        let field = fixture
            .fields
            .iter()
            .find(|(name, _)| *name == key)
            .unwrap()
            .clone();
        fixture.fields.push(field);
        fixture.set("compressed_len", 0);
        fixture.assert_error(
            None,
            &format!("compressed envelope has duplicate field: {key}"),
        );
        cases += 1;
    }
    for (key, context) in [
        ("compressed_checksum", "compressed checksum"),
        ("uncompressed_checksum", "uncompressed checksum"),
        ("compressed_len", "compressed length"),
        ("uncompressed_len", "uncompressed length"),
    ] {
        for value in ["", "-1", "1.0", "18446744073709551616", "\u{e9}", " 3"] {
            let mut fixture = valid.clone();
            fixture.set(key, value);
            assert_eq!(
                error(&fixture.bytes(), None),
                format!("invalid {context}: {value}")
            );
            cases += 1;
        }
    }
    let mut fixture = valid.clone();
    fixture.fields.push(("unknown", "1".into()));
    fixture.assert_error(
        None,
        "compressed envelope has invalid header line: unknown\t1",
    );
    cases += 1;
    let mut fixture = valid.clone();
    fixture.set("codec", "none");
    fixture.set("compressed_len", 0);
    fixture.assert_error(None, "compressed envelope uses unsupported codec");
    cases += 1;
    let mut fixture = valid.clone();
    fixture.remove("compressed_checksum");
    fixture.set("compressed_len", 0);
    fixture.assert_error(None, "compressed length mismatch: expected 0, got 12");
    cases += 1;
    let mut fixture = valid.clone();
    fixture.set("compressed_checksum", 0);
    fixture.remove("uncompressed_len");
    fixture.assert_error(
        Some(0),
        &format!(
            "compressed checksum mismatch: expected 0, got {}",
            crc(&fixture.payload)
        ),
    );
    cases += 1;
    let mut fixture = valid.clone();
    fixture.remove("uncompressed_checksum");
    fixture.assert_error(Some(0), "decoded byte limit exceeded: max_decoded_bytes=0");
    cases += 1;
    let mut fixture = valid;
    fixture.set("uncompressed_len", 4);
    fixture.remove("uncompressed_checksum");
    fixture.assert_error(None, "uncompressed length mismatch: expected 4, got 3");
    cases += 1;
    assert_eq!(cases, 40);
}

#[test]
fn header_order_and_legacy_tolerance_remain_unchanged() {
    fn permutations(fixture: &mut Envelope, at: usize, count: &mut usize) {
        if at == fixture.fields.len() {
            assert_eq!(
                read_durable_text_bytes(&fixture.bytes(), "fixture").unwrap(),
                "abc"
            );
            *count += 1;
            return;
        }
        for index in at..fixture.fields.len() {
            fixture.fields.swap(at, index);
            permutations(fixture, at + 1, count);
            fixture.fields.swap(at, index);
        }
    }
    let mut fixture = Envelope::new(b"abc");
    let mut count = 0;
    permutations(&mut fixture, 0, &mut count);
    assert_eq!(count, 120);
    fixture.set("uncompressed_len", "+003");
    let bytes = fixture.bytes();
    let mut repeated = b"SKEIN_COMPRESSED_V1\n".to_vec();
    repeated.extend_from_slice(&bytes);
    assert_eq!(
        read_durable_text_bytes(&repeated, "fixture").unwrap(),
        "abc"
    );
    assert_eq!(
        error(b"abc", None),
        "fixture is missing the V1 compressed envelope"
    );
    assert_eq!(
        error(b"SKEIN_COMPRESSED_V1", None),
        "fixture compressed envelope missing header terminator"
    );
    assert!(error(b"SKEIN_COMPRESSED_V1\n\xff\n\n", None)
        .starts_with("fixture compressed envelope header is invalid: "));
    assert!(error(b"SKEIN_COMPRESSED_V1x\n\n", None).contains("invalid header line"));
}

#[test]
fn decoded_limits_bound_declared_and_actual_output() {
    let mut fixture = Envelope::new(b"abc");
    fixture.assert_error(Some(2), "decoded byte limit exceeded: max_decoded_bytes=2");
    fixture.set("uncompressed_len", 1);
    fixture.assert_error(None, "decoded byte limit exceeded: max_decoded_bytes=1");
    fixture.assert_error(Some(2), "decoded byte limit exceeded: max_decoded_bytes=2");
    fixture.assert_error(Some(3), "uncompressed length mismatch: expected 1, got 3");
    fixture.set("uncompressed_len", u64::MAX);
    if usize::BITS == 64 {
        fixture.assert_error(Some(3), "decoded byte limit exceeded: max_decoded_bytes=3");
    } else {
        assert!(error(&fixture.bytes(), Some(3)).starts_with("invalid uncompressed length:"));
    }
    let text = "x".repeat(8 * 1024 * 1024 + 1);
    let bytes = encode_durable_text(&text, DurableCompression::Zstd).unwrap();
    assert_eq!(
        read_durable_text_bytes_with_limit(&bytes, "fixture", Some(text.len() as u64)).unwrap(),
        text
    );
    assert_eq!(
        error(&bytes, Some((text.len() - 1) as u64)),
        format!(
            "fixture decoded byte limit exceeded: max_decoded_bytes={}",
            text.len() - 1
        )
    );
}

#[test]
fn frames_corruption_and_utf8_are_checked_independently() {
    for length in [255, 256, 257, 65_791] {
        let text = "x".repeat(length);
        let fixture = Envelope::new(text.as_bytes());
        assert_eq!(
            read_durable_text_bytes(&fixture.bytes(), "fixture").unwrap(),
            text
        );
    }
    let mut fixture = Envelope::new(b"abc");
    for length in 0..fixture.payload.len() {
        let mut truncated = fixture.clone();
        truncated.payload.truncate(length);
        truncated.repair_payload_binding();
        assert!(error(&truncated.bytes(), None).starts_with("fixture zstd decompression failed: "));
    }
    fixture.payload.extend_from_slice(&raw_frame(b"def"));
    fixture.repair_payload_binding();
    fixture.set("uncompressed_len", 6);
    fixture.set("uncompressed_checksum", crc(b"abcdef"));
    assert_eq!(
        read_durable_text_bytes(&fixture.bytes(), "fixture").unwrap(),
        "abcdef"
    );
    fixture.assert_error(Some(5), "decoded byte limit exceeded: max_decoded_bytes=5");
    for bytes in [&b"\xff"[..], &b"\xc0\xaf"[..], &b"\xed\xa0\x80"[..]] {
        assert!(error(&Envelope::new(bytes).bytes(), None)
            .starts_with("fixture decompressed payload is not valid UTF-8: "));
    }
}

fn campaign(seeds: u64, steps: usize) -> usize {
    let mut count = 0;
    for seed in 0..seeds {
        let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        for step in 0..steps {
            let mut text = String::new();
            for _ in 0..(step % 96 + 1) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                text.push(match state % 8 {
                    0 => '\0',
                    1 => '\n',
                    2 => '\t',
                    3 => '\u{e9}',
                    4 => '\u{4e2d}',
                    5 => '\u{1f680}',
                    _ => char::from(b'a' + (state % 26) as u8),
                });
            }
            let valid = Envelope::new(text.as_bytes());
            let bytes = valid.bytes();
            assert_eq!(read_durable_text_bytes(&bytes, "fixture").unwrap(), text);
            count += 1;
            let encoded = encode_durable_text(&text, DurableCompression::Zstd).unwrap();
            assert_eq!(
                read_durable_text_bytes_with_limit(&encoded, "fixture", Some(text.len() as u64))
                    .unwrap(),
                text
            );
            count += 1;
            assert_eq!(
                read_durable_text_bytes_with_limit(&bytes, "fixture", Some(u64::MAX)).unwrap(),
                text
            );
            count += 1;
            valid.assert_error(
                Some(text.len() as u64 - 1),
                &format!(
                    "decoded byte limit exceeded: max_decoded_bytes={}",
                    text.len() - 1
                ),
            );
            count += 1;
            let mut fixture = valid.clone();
            fixture.set("uncompressed_len", 0);
            fixture.assert_error(None, "decoded byte limit exceeded: max_decoded_bytes=0");
            count += 1;
            fixture.assert_error(
                Some(text.len() as u64),
                &format!(
                    "uncompressed length mismatch: expected 0, got {}",
                    text.len()
                ),
            );
            count += 1;
            fixture.set("uncompressed_len", text.len() + 1);
            fixture.assert_error(
                None,
                &format!(
                    "uncompressed length mismatch: expected {}, got {}",
                    text.len() + 1,
                    text.len()
                ),
            );
            count += 1;
            fixture.assert_error(
                Some(text.len() as u64),
                &format!(
                    "decoded byte limit exceeded: max_decoded_bytes={}",
                    text.len()
                ),
            );
            count += 1;
            let mut fixture = valid.clone();
            fixture.set("compressed_checksum", crc(&valid.payload) ^ 1);
            fixture.assert_error(
                None,
                &format!(
                    "compressed checksum mismatch: expected {}, got {}",
                    crc(&valid.payload) ^ 1,
                    crc(&valid.payload)
                ),
            );
            count += 1;
            let mut fixture = valid.clone();
            fixture.set("uncompressed_checksum", crc(text.as_bytes()) ^ 1);
            fixture.assert_error(
                None,
                &format!(
                    "uncompressed checksum mismatch: expected {}, got {}",
                    crc(text.as_bytes()) ^ 1,
                    crc(text.as_bytes())
                ),
            );
            count += 1;
            let mut fixture = valid.clone();
            fixture.set("compressed_len", valid.payload.len() + 1);
            fixture.assert_error(
                None,
                &format!(
                    "compressed length mismatch: expected {}, got {}",
                    valid.payload.len() + 1,
                    valid.payload.len()
                ),
            );
            count += 1;
            let mut fixture = valid.clone();
            fixture.remove("uncompressed_checksum");
            fixture.assert_error(None, "compressed envelope missing uncompressed_checksum");
            count += 1;
            let mut fixture = valid.clone();
            fixture.payload.pop();
            fixture.repair_payload_binding();
            assert!(
                error(&fixture.bytes(), None).starts_with("fixture zstd decompression failed: ")
            );
            count += 1;
            let mut invalid_utf8 = text.as_bytes().to_vec();
            invalid_utf8.push(0xff);
            assert!(error(&Envelope::new(&invalid_utf8).bytes(), None)
                .starts_with("fixture decompressed payload is not valid UTF-8: "));
            count += 1;
            let mut fixture = valid.clone();
            let last = fixture.payload.len() - 1;
            fixture.payload[last] ^= 1;
            fixture.assert_error(
                None,
                &format!(
                    "compressed checksum mismatch: expected {}, got {}",
                    crc(&valid.payload),
                    crc(&fixture.payload)
                ),
            );
            count += 1;
            let mut fixture = valid;
            fixture.payload.push(0);
            fixture.assert_error(
                None,
                &format!(
                    "compressed length mismatch: expected {}, got {}",
                    fixture.payload.len() - 1,
                    fixture.payload.len()
                ),
            );
            count += 1;
        }
    }
    count
}

#[test]
fn durable_envelope_differential_smoke() {
    assert_eq!(campaign(4, 16), 1_024);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn durable_envelope_differential_campaign() {
    let cases = campaign(128, 64);
    assert_eq!(cases, 131_072);
    eprintln!("durable envelope campaign: seeds=128 steps=64 cases={cases}");
}
