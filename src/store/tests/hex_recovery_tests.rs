use super::super::encode_string;
use super::{active_checkpoint_path, read_durable_text, rewrite_checksummed_file, unique_test_dir};
use crate::error::SkeinError;
use crate::value::Value;
use crate::{Database, DatabaseConfig};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

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
