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

use crate::store_facade_tests::{
    active_checkpoint_path, refresh_manifest_checkpoint_metadata, unique_test_dir,
};
use crate::{Database, DatabaseConfig, HawDBError, Value};
use hawdb_storage::text::envelope::{encode_durable_text, read_durable_text_bytes};
use hawdb_storage::DurableCompression;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut pending = vec![root.to_owned()];
    let mut result = BTreeMap::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(&path).unwrap(),
                );
            }
        }
    }
    result
}

fn assert_rejected_without_writes(root: &Path, limit: Option<u64>, expected: &str) {
    let before = files(root);
    for read_only in [true, false] {
        let result = Database::open_with_config(
            root,
            DatabaseConfig {
                read_only,
                max_checkpoint_decoded_bytes: limit,
                ..Default::default()
            },
        );
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("invalid checkpoint admitted"),
        };
        assert!(matches!(error, HawDBError::Storage(_)));
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(
            files(root),
            before,
            "failed open modified storage, read_only={read_only}"
        );
    }
}

#[test]
fn storage_owned_envelope_preserves_checkpoint_reopen_and_caller_limits() {
    let fixture = Fixture(unique_test_dir("storage_owned_envelope"));
    {
        let mut database = Database::open(&fixture.0).unwrap();
        database
            .query("CREATE (:EnvelopeRecovery {id: 7})")
            .unwrap();
        database.checkpoint().unwrap();
    }
    let checkpoint_path = active_checkpoint_path(&fixture.0);
    let checkpoint = fs::read(&checkpoint_path).unwrap();
    let manifest = fs::read(fixture.0.join("manifest.hawdb")).unwrap();
    let text = read_durable_text_bytes(&checkpoint, "checkpoint").unwrap();
    assert_eq!(
        encode_durable_text(&text, DurableCompression::Zstd).unwrap(),
        checkpoint
    );
    let length = text.len() as u64;
    assert!(length > 0);
    assert_rejected_without_writes(&fixture.0, Some(length - 1), "decoded byte limit exceeded");
    for read_only in [true, false] {
        let mut database = Database::open_with_config(
            &fixture.0,
            DatabaseConfig {
                read_only,
                max_checkpoint_decoded_bytes: Some(length),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            database
                .query("MATCH (n:EnvelopeRecovery) RETURN n.id AS id")
                .unwrap()
                .rows,
            vec![BTreeMap::from([("id".to_owned(), Value::Int(7))])]
        );
    }

    let header_end = checkpoint
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .unwrap();
    let header = std::str::from_utf8(&checkpoint[..header_end]).unwrap();
    for (from, to, limit, expected) in [
        (
            format!("uncompressed_len\t{length}"),
            "uncompressed_len\t0".to_owned(),
            None,
            "decoded byte limit exceeded",
        ),
        (
            format!("uncompressed_len\t{length}"),
            "uncompressed_len\t0".to_owned(),
            Some(length),
            "uncompressed length mismatch",
        ),
        (
            "codec\tzstd".to_owned(),
            "codec\tzstd\ncodec\tzstd".to_owned(),
            Some(length),
            "compressed envelope has duplicate field: codec",
        ),
    ] {
        let rewritten = header.replace(&from, &to);
        assert_ne!(rewritten, header);
        let mut corrupt = format!("{rewritten}\n\n").into_bytes();
        corrupt.extend_from_slice(&checkpoint[header_end + 2..]);
        fs::write(&checkpoint_path, corrupt).unwrap();
        // Keep the selected manifest length/CRC/SHA valid so open reaches the
        // envelope decoder rather than stopping at the outer binding check.
        refresh_manifest_checkpoint_metadata(&checkpoint_path);
        assert_rejected_without_writes(&fixture.0, limit, expected);
        fs::write(&checkpoint_path, &checkpoint).unwrap();
        fs::write(fixture.0.join("manifest.hawdb"), &manifest).unwrap();
    }
    let mut database = Database::open(&fixture.0).unwrap();
    assert_eq!(
        database
            .query("MATCH (n:EnvelopeRecovery) RETURN n.id AS id")
            .unwrap()
            .rows,
        vec![BTreeMap::from([("id".to_owned(), Value::Int(7))])]
    );
}
