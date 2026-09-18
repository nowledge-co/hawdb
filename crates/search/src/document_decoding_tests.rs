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

#[test]
fn malformed_hex_returns_storage_errors_without_panicking() {
    for input in [
        "a\u{e9}a",
        "\u{4e2d}a",
        "a\u{4e2d}",
        "\u{1f980}",
        "ff",
        "c0af",
        "eda080",
    ] {
        let outcome = std::panic::catch_unwind(|| decode_string(input));
        assert!(outcome.is_ok(), "decoder panicked for {input:?}");
        assert!(matches!(outcome.unwrap(), Err(HawDBError::Storage(_))));
    }
}

#[test]
fn invalid_hex_diagnostic_does_not_copy_the_complete_source() {
    let input = format!("{}zz", "61".repeat(128 * 1024));
    let error = decode_string(&input).unwrap_err().to_string();
    assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
    assert!(error.contains("invalid hex"));
}

fn reference_decode(input: &str) -> Option<String> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let digit = |byte| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };
    let mut bytes = Vec::new();
    for pair in input.as_bytes().chunks_exact(2) {
        // The existing unsigned radix parser accepts a leading plus per pair.
        let high = if pair[0] == b'+' { 0 } else { digit(pair[0])? };
        bytes.push(high * 16 + digit(pair[1])?);
    }
    String::from_utf8(bytes).ok()
}

fn assert_decode(input: &str) {
    let actual = std::panic::catch_unwind(|| decode_string(input));
    assert!(actual.is_ok(), "decoder panicked for {input:?}");
    let actual = actual.unwrap();
    match reference_decode(input) {
        Some(expected) => assert_eq!(actual.unwrap(), expected),
        None => assert!(matches!(actual, Err(HawDBError::Storage(_)))),
    }
}

#[test]
fn hex_decoder_preserves_every_ascii_pair_and_valid_unicode() {
    for first in 0..=127 {
        for second in 0..=127 {
            assert_decode(std::str::from_utf8(&[first, second]).unwrap());
        }
    }
    for source in ["", "plain ASCII", "\0\t\r\n", "\u{e9}\u{4e2d}\u{1f980}"] {
        let encoded = encode_string(source);
        assert_eq!(decode_string(&encoded).unwrap(), source);
        assert_eq!(decode_string(&encoded.to_uppercase()).unwrap(), source);
    }
    assert_eq!(decode_string("+0+F4142").unwrap(), "\0\u{f}AB");
}

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hawdb-hex-decoding-{}-{}-{sequence}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
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

fn snapshot_bytes(record: &str) -> Vec<u8> {
    let body = format!("HAWDB_SEARCH_PROJECTION_V1\ndoc\t676f6f64\t\t\t\t\n{record}\n");
    let checksum = checksum_bytes(body.as_bytes());
    encode_search_snapshot_text(&format!("{body}checksum\t{checksum}\n")).unwrap()
}

#[test]
fn reopen_rejects_malformed_hex_even_with_valid_snapshot_checksums() {
    let root = TestRoot::new();
    let path = root.0.join(SEARCH_SNAPSHOT_FILE);
    let valid = snapshot_bytes("doc\t6964\t7469746c65\t626f6479\t\t6b=76");
    fs::write(&path, &valid).unwrap();
    {
        let index = SearchIndex::open(&root.0).unwrap();
        assert_eq!(index.document("id").unwrap().content, "body");
    }

    let malformed = "a\u{e9}a";
    for record in [
        format!("doc\t{malformed}\t\t\t\t"),
        format!("doc\t6964\t{malformed}\t\t\t"),
        format!("doc\t6964\t\t{malformed}\t\t"),
        format!("doc\t6964\t\t\t\t{malformed}=76"),
        format!("doc\t6964\t\t\t\t6b={malformed}"),
        format!("embedding_manifest\t{malformed}\t76\t2"),
        format!("embedding_manifest\t6d\t{malformed}\t2"),
    ] {
        let corrupt = snapshot_bytes(&record);
        fs::write(&path, &corrupt).unwrap();
        let text = read_search_snapshot_text(&path).unwrap();
        let (body, checksum) = split_checksum(&text).unwrap();
        assert_eq!(checksum_bytes(body.as_bytes()), checksum);

        let outcome = std::panic::catch_unwind(|| SearchIndex::open(&root.0));
        assert!(outcome.is_ok(), "public reopen panicked for {record:?}");
        let Err(error) = outcome.unwrap() else {
            panic!("public reopen accepted a malformed persisted field");
        };
        assert!(matches!(error, HawDBError::Storage(_)));
        assert!(error.to_string().contains("invalid hex"));
        assert_eq!(fs::read(&path).unwrap(), corrupt);
    }

    fs::write(&path, valid).unwrap();
    let restored = SearchIndex::open(&root.0).unwrap();
    assert!(restored.document("good").is_some());
    assert!(restored.document("id").is_some());
}

#[test]
fn malformed_payload_fields_return_errors_without_partial_rows() {
    for malformed in ["a\u{e9}a", "\u{4e2d}a", "ff"] {
        for field in [1, 2, 3, 5] {
            let mut fields = ["doc", "6964", "", "", "", ""];
            let metadata = format!("6b={malformed}");
            fields[field] = if field == 5 { &metadata } else { malformed };
            let text = format!(
                "HAWDB_SEARCH_SEGMENT_V1\ndoc\t676f6f64\t\t\t\t\n{}\n",
                fields.join("\t")
            );
            let payload = encode_search_snapshot_text(&text).unwrap();
            assert_eq!(decode_search_snapshot_text(&payload).unwrap(), text);
            let outcome = std::panic::catch_unwind(|| decode_search_segment_documents(&payload));
            assert!(outcome.is_ok());
            let error = outcome.unwrap().unwrap_err();
            assert!(matches!(error, HawDBError::Storage(_)));
            assert!(error.to_string().contains(if malformed == "ff" {
                "utf-8"
            } else {
                "invalid hex"
            }));
        }
    }
    let payload = encode_search_snapshot_text(
        "HAWDB_SEARCH_SEGMENT_V1\ndoc\t676f6f64\t\t\t\t\ndoc\t6964\t\t\t\t6b=76\n",
    )
    .unwrap();
    assert_eq!(decode_search_segment_documents(&payload).unwrap().len(), 2);
}

#[test]
#[ignore = "explicit local differential campaign"]
fn hex_decoding_differential_campaign() {
    fn next(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }
    let alphabet = [
        "a",
        "F",
        "+",
        "-",
        "g",
        "0",
        "7",
        "\0",
        "\u{e9}",
        "\u{4e2d}",
        "\u{1f980}",
    ];
    let mut state = 0x395e_71c0_64ab_921du64;
    for case in 0..1024 {
        let mut source = String::new();
        for _ in 0..next(&mut state) % 64 {
            source.push_str(alphabet[next(&mut state) as usize % alphabet.len()]);
        }
        if case % 32 == 0 {
            source = source.repeat(128);
        }
        let mut encoded = encode_string(&source);
        assert_eq!(decode_string(&encoded).unwrap(), source);
        assert_decode(&source);
        match case % 5 {
            0 => encoded.make_ascii_uppercase(),
            1 => {
                let offset = next(&mut state) as usize % (encoded.len() + 1);
                encoded.insert(offset, '\u{4e2d}');
            }
            2 => encoded.truncate(next(&mut state) as usize % (encoded.len() + 1)),
            3 => encoded.push_str("ff"),
            _ => {
                let offset = next(&mut state) as usize % (encoded.len() + 1);
                encoded.insert_str(offset, "+F");
            }
        }
        assert_decode(&encoded);
    }
}
