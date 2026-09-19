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

//! Facade-level storage tests that drive the on-disk layout of the graph store
//! kernel through the root crate's public surface.
//!
//! These tests need the facade (`Database`, `HawDBEmbedded`) as well as the
//! storage layout internals, so they live in the root crate. The helpers below
//! delegate to storage's public, doc-hidden support API.

mod checkpoint_parse_order_tests;
mod derived_repair_tests;
mod doctor_tests;
mod envelope_recovery_tests;
mod hex_recovery_tests;
mod row_page_compaction;

pub use hawdb_storage::store::read_durable_text;

use hawdb_integrity::integrity_digest;
use hawdb_storage::store::checksum_bytes;
use hawdb_storage::text::envelope::{encode_durable_text, DURABLE_COMPRESSION_HEADER};
use hawdb_storage::DurableCompression;

pub fn unique_test_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hawdb_store_{name}_{nanos}"))
}

pub fn active_checkpoint_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    active_generation_path(path.as_ref(), "checkpoint_generation", "checkpoint")
}

pub fn active_generation_path(
    root: &std::path::Path,
    manifest_field: &str,
    prefix: &str,
) -> std::path::PathBuf {
    let manifest = std::fs::read_to_string(root.join("manifest.hawdb")).unwrap();
    assert!(manifest.contains("HAWDB_MANIFEST_V1\n"));
    let generation = manifest.lines().find_map(|line| {
        let (field, value) = line.split_once('\t')?;
        (field == manifest_field && value != "none").then_some(value)
    });
    root.join(format!(
        "{prefix}.{}.hawdb",
        generation.expect("active generation must exist")
    ))
}

pub fn rewrite_checksummed_file(path: &std::path::Path, from: &str, to: &str, kind: &str) {
    let was_compressed = std::fs::read(path)
        .unwrap()
        .starts_with(DURABLE_COMPRESSION_HEADER.as_bytes());
    let text = if kind == "manifest" {
        std::fs::read_to_string(path).unwrap()
    } else {
        read_durable_text(path, kind).unwrap()
    };
    let (body, _) = text.rsplit_once("checksum\t").unwrap();
    let body = body.replace(from, to);
    let checksum = checksum_bytes(body.as_bytes());
    let rewritten = format!("{body}checksum\t{checksum}\n");
    if was_compressed {
        std::fs::write(
            path,
            encode_durable_text(&rewritten, DurableCompression::default()).unwrap(),
        )
        .unwrap();
    } else {
        std::fs::write(path, rewritten.as_bytes()).unwrap();
    }
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("checkpoint."))
    {
        refresh_manifest_checkpoint_metadata(path);
    }
    let rewritten = if kind == "manifest" {
        std::fs::read_to_string(path).unwrap()
    } else {
        read_durable_text(path, kind).unwrap()
    };
    assert!(
        rewritten.contains(to),
        "{kind} rewrite did not update storage version"
    );
}

pub fn refresh_manifest_checkpoint_metadata(checkpoint_path: &std::path::Path) {
    let root = checkpoint_path.parent().unwrap();
    let manifest_path = root.join("manifest.hawdb");
    let manifest = std::fs::read_to_string(&manifest_path).unwrap();
    let (body, _) = manifest.rsplit_once("checksum\t").unwrap();
    let checkpoint = std::fs::read(checkpoint_path).unwrap();
    let encoded_len = checkpoint.len() as u64;
    let integrity = integrity_digest(&checkpoint);
    let encoded_checksum = integrity.crc32c.as_u64();
    let encoded_sha256 = integrity.sha256;
    let mut rewritten_body = String::new();
    for line in body.lines() {
        if line.starts_with("checkpoint_encoded_len\t") {
            rewritten_body.push_str(&format!("checkpoint_encoded_len\t{encoded_len}\n"));
        } else if line.starts_with("checkpoint_encoded_checksum\t") {
            rewritten_body.push_str(&format!(
                "checkpoint_encoded_checksum\t{encoded_checksum}\n"
            ));
        } else if line.starts_with("checkpoint_encoded_sha256\t") {
            rewritten_body.push_str(&format!("checkpoint_encoded_sha256\t{encoded_sha256}\n"));
        } else {
            rewritten_body.push_str(line);
            rewritten_body.push('\n');
        }
    }
    let checksum = checksum_bytes(rewritten_body.as_bytes());
    std::fs::write(
        manifest_path,
        format!("{rewritten_body}checksum\t{checksum}\n"),
    )
    .unwrap();
}
