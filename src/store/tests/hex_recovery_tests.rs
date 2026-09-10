use super::super::{
    decode_bytes, decode_properties, decode_string, decode_value, encode_bytes, encode_string,
    encode_value,
};
use super::{active_checkpoint_path, read_durable_text, rewrite_checksummed_file, unique_test_dir};
use crate::error::SkeinError;
use crate::value::Value;
use crate::{Database, DatabaseConfig};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

// Independent byte oracle, including the legacy radix parser's leading plus.
fn reference(input: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if !input.len().is_multiple_of(2) {
        return None;
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = if pair[0] == b'+' { 0 } else { nibble(pair[0])? };
            Some(high * 16 + nibble(pair[1])?)
        })
        .collect()
}

#[test]
fn hex_preserves_all_ascii_pairs_and_valid_byte_round_trips() {
    for first in 0..128 {
        for second in 0..128 {
            let input = String::from_utf8(vec![first, second]).unwrap();
            assert_eq!(decode_bytes(&input).ok(), reference(&input));
        }
    }
    let bytes = (0..=255).collect::<Vec<u8>>();
    assert_eq!(decode_bytes(&encode_bytes(&bytes)).unwrap(), bytes);
    for text in ["", "\0\t\n", "ASCII \u{e9}\u{4e2d}\u{1f980}"] {
        assert_eq!(decode_string(&encode_string(text)).unwrap(), text);
    }
}

#[test]
fn malformed_hex_returns_storage_errors_without_panicking() {
    for input in ["a\u{e9}a", "\u{1f980}", "00a\u{e9}a", "f", "gg"] {
        let result = std::panic::catch_unwind(|| decode_bytes(input));
        assert!(result.is_ok(), "hex decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(SkeinError::Storage(_))));
    }
}

#[test]
fn bad_hex_diagnostics_do_not_copy_the_field() {
    for input in [
        format!("gg{}", "00".repeat(128 * 1024)),
        format!("{}gg", "00".repeat(128 * 1024)),
    ] {
        let error = decode_bytes(&input).unwrap_err().to_string();
        assert!(error.contains("invalid hex"), "{error}");
        assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
    }
}

#[test]
fn malformed_value_tags_are_rejected_without_unbounded_diagnostics() {
    for input in ["\u{e9}", "\u{4e2d}", "\u{1f980}", "", "?", "nextra"] {
        let result = std::panic::catch_unwind(|| decode_value(input));
        assert!(result.is_ok(), "value decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(SkeinError::Storage(_))));
    }
    let input = format!("?{}", "x".repeat(256 * 1024));
    let error = decode_value(&input).unwrap_err().to_string();
    assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
}

#[test]
fn nested_value_paths_propagate_corruption_and_preserve_valid_values() {
    let bad_hex = "a\u{e9}a";
    let bad_value = encode_string("\u{e9}");
    for input in [
        format!("s{bad_hex}"),
        format!("l{bad_hex}"),
        format!("m{bad_hex}=6931"),
        format!("m6964={bad_hex}"),
        format!("l{bad_value}"),
        format!("m6964={bad_value}"),
    ] {
        let result = std::panic::catch_unwind(|| decode_value(&input));
        assert!(result.is_ok(), "nested decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(SkeinError::Storage(_))));
    }
    assert!(decode_properties("6964=sa\u{e9}a").is_err());
    for value in [
        Value::Null,
        Value::Bool(true),
        Value::Int(-7),
        Value::Float(1.25),
        Value::Uuid(skein_core::Uuid::parse_str("01890f3e-0e3c-7f9e-9b23-1bcdef012345").unwrap()),
        Value::String("\u{4e2d}".into()),
        Value::Binary(vec![0, 255]),
        Value::List(vec![Value::String("\u{e9}".into()), Value::Null]),
        Value::Map(BTreeMap::from([(
            "\u{1f980}".into(),
            Value::List(vec![Value::Int(8)]),
        )])),
    ] {
        assert_eq!(decode_value(&encode_value(&value)).unwrap(), value);
    }
}

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let fixture = Self(unique_test_dir("hex_recovery"));
        let mut database = Database::open(&fixture.0).unwrap();
        database.query("CREATE (:HexRecovery {id: 1})").unwrap();
        database.checkpoint().unwrap();
        fixture
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn assert_corrupt_checkpoint_rejected(fixture: &Fixture, from: &str, to: &str, expected: &str) {
    let checkpoint = active_checkpoint_path(&fixture.0);
    let manifest = fixture.0.join("manifest.skein");
    let valid_checkpoint = fs::read(&checkpoint).unwrap();
    let valid_manifest = fs::read(&manifest).unwrap();
    assert!(read_durable_text(&checkpoint, "checkpoint")
        .unwrap()
        .contains(from));
    // Update the inner checksum and outer length/CRC/SHA so corruption reaches
    // the semantic decoder instead of failing an integrity precheck.
    rewrite_checksummed_file(&checkpoint, from, to, "checkpoint");
    let before = snapshot(&fixture.0);
    for read_only in [true, false] {
        let result = std::panic::catch_unwind(|| {
            Database::open_with_config(
                &fixture.0,
                DatabaseConfig {
                    read_only,
                    ..Default::default()
                },
            )
        });
        assert!(
            result.is_ok(),
            "public open panicked, read_only={read_only}"
        );
        let error = match result.unwrap() {
            Ok(_) => panic!("corrupted checkpoint opened"),
            Err(error) => error,
        };
        assert!(matches!(error, SkeinError::Storage(_)));
        assert!(error.to_string().contains(expected), "{error}");
        assert!(error.to_string().len() < 256);
        assert_eq!(snapshot(&fixture.0), before);
    }
    fs::write(checkpoint, valid_checkpoint).unwrap();
    fs::write(manifest, valid_manifest).unwrap();
    let mut database = Database::open(&fixture.0).unwrap();
    assert_eq!(
        database
            .query("MATCH (n:HexRecovery) RETURN n.id AS id")
            .unwrap()
            .rows,
        vec![BTreeMap::from([("id".to_string(), Value::Int(1))])]
    );
}

#[test]
fn public_checkpoint_reopen_rejects_bad_hex_without_writes() {
    let fixture = Fixture::new();
    let label = encode_string("HexRecovery");
    for input in ["a\u{e9}a", "\u{1f980}", "gg", "f", "ff"] {
        let expected = if input == "ff" {
            "invalid utf-8"
        } else {
            "invalid hex"
        };
        assert_corrupt_checkpoint_rejected(
            &fixture,
            &format!("label\t0\t{label}\n"),
            &format!("label\t0\t{input}\n"),
            expected,
        );
    }
}

#[test]
fn public_checkpoint_histogram_rejects_bad_value_tags_without_writes() {
    let fixture = Fixture::new();
    let checkpoint = active_checkpoint_path(&fixture.0);
    let label = format!("label\t0\t{}\n", encode_string("HexRecovery"));
    let valid_histogram = "stat_property_histogram\t0\t6964\t6931\n";
    rewrite_checksummed_file(
        &checkpoint,
        &label,
        &format!("{label}{valid_histogram}"),
        "checkpoint",
    );
    drop(Database::open(&fixture.0).unwrap());
    for input in ["\u{e9}", "\u{4e2d}", "\u{1f980}"] {
        assert_corrupt_checkpoint_rejected(
            &fixture,
            valid_histogram,
            &format!(
                "stat_property_histogram\t0\t6964\t{}\n",
                encode_string(input)
            ),
            "invalid encoded value",
        );
    }
}

fn campaign(cases: usize) {
    let mut seed = 0x0404_5afe_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed
    };
    for case in 0..cases {
        let mut input = (0..next() % 128)
            .map(|_| format!("{:02x}", (next() >> 32) as u8))
            .collect::<String>();
        match case % 8 {
            1 => input.make_ascii_uppercase(),
            2 => input.push_str("gg"),
            3 => input.push('f'),
            4 => input.insert_str(0, "a\u{e9}a"),
            5 => input.push_str("+0+f"),
            6 => input.truncate(input.len() / 2),
            7 => input.push('\u{1f980}'),
            _ => {}
        }
        let expected = reference(&input);
        let decoded = decode_bytes(&input);
        if let Err(error) = &decoded {
            assert!(matches!(error, SkeinError::Storage(_)));
            assert!(error.to_string().len() < 128, "case {case}");
        }
        assert_eq!(decoded.ok(), expected, "case {case}");
        let expected = expected.and_then(|bytes| String::from_utf8(bytes).ok());
        assert_eq!(decode_string(&input).ok(), expected, "case {case}");
        assert_eq!(
            decode_value(&format!("s{input}")).ok(),
            expected.map(Value::String),
            "case {case}"
        );
        let bad_tag = ["\u{e9}", "\u{4e2d}", "\u{1f980}"][case % 3];
        assert!(decode_value(&format!("{bad_tag}{input}")).is_err());
        let error = decode_value(&format!("?{input}")).unwrap_err();
        assert!(matches!(error, SkeinError::Storage(_)));
        assert!(error.to_string().len() < 128, "case {case}");
    }
}

#[test]
fn recovery_text_differential_smoke() {
    campaign(32);
}

#[test]
#[ignore = "explicit local recovery text differential campaign"]
fn recovery_text_differential_campaign() {
    campaign(1024);
}
