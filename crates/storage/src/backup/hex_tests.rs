use super::*;
use crate::hex_test_support::{campaign_inputs, reference_decode};
use std::path::PathBuf;

fn reference_string(input: &str) -> Option<String> {
    String::from_utf8(reference_decode(input)?).ok()
}

#[test]
fn persisted_hex_preserves_ascii_pairs_and_unicode_round_trips() {
    for first in 0..128 {
        for second in 0..128 {
            let input = String::from_utf8(vec![first, second]).unwrap();
            assert_eq!(decode_string(&input).ok(), reference_string(&input));
        }
    }
    for value in ["", "\0\t\n", "ASCII \u{e9}\u{4e2d}\u{1f980}"] {
        assert_eq!(decode_string(&encode_string(value)).unwrap(), value);
    }
}

#[test]
fn persisted_hex_rejects_non_ascii_without_panicking() {
    for input in ["a\u{e9}a", "\u{1f980}", "00a\u{e9}a", "f", "gg", "ff"] {
        let result = std::panic::catch_unwind(|| decode_string(input));
        assert!(result.is_ok(), "decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(SkeinError::Storage(_))));
    }
}

#[test]
fn persisted_hex_error_does_not_copy_the_bad_field() {
    let input = format!("{}gg", "00".repeat(128 * 1024));
    let error = decode_string(&input).unwrap_err().to_string();
    assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("skein-backup-hex-{}-{nonce}", std::process::id()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn persisted_hex_public_load_rejects_checksum_valid_corruption_without_writes() {
    let directory = TestDirectory::new();
    let path = directory.0.join(BACKUP_MANIFEST_FILE);
    let manifest = BackupManifest::write(
        &path,
        7,
        11,
        vec![BackupFileEntry {
            name: "checkpoint.7.skein".to_string(),
            encoded_len: 42,
            encoded_checksum: 9,
            sha256: Sha256Digest::from_bytes([3; 32]),
        }],
    )
    .unwrap();
    let valid = fs::read_to_string(&path).unwrap();
    assert_eq!(BackupManifest::load(&path).unwrap(), manifest);
    let (body, _) = split_backup_manifest_checksum(&valid).unwrap();
    for invalid in ["a\u{e9}a", "\u{1f980}", "gg", "ff"] {
        let body = body.replace(&encode_string("checkpoint.7.skein"), invalid);
        let corrupt = format!("{body}checksum\t{}\n", checksum_u64(body.as_bytes()));
        fs::write(&path, &corrupt).unwrap();
        let result = std::panic::catch_unwind(|| BackupManifest::load(&path));
        assert!(result.is_ok(), "public load panicked for {invalid:?}");
        let error = result.unwrap().unwrap_err();
        assert!(matches!(error, SkeinError::Storage(_)));
        let expected = if invalid == "ff" {
            "invalid utf-8"
        } else {
            "invalid hex"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(fs::read_to_string(&path).unwrap(), corrupt);
    }
    fs::write(&path, &valid).unwrap();
    assert_eq!(BackupManifest::load(&path).unwrap(), manifest);
}

#[test]
#[ignore = "explicit local backup hex differential campaign"]
fn persisted_hex_differential_campaign() {
    for (case, input) in campaign_inputs().iter().enumerate() {
        assert_eq!(
            decode_string(input).ok(),
            reference_string(input),
            "case {case}"
        );
    }
}
