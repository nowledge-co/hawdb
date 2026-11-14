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

//! Fixtures for tests/logical_dump.rs.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use hawdb::Value;

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = root.join(format!(
            "hawdb-logical-dump-{name}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Rewrites the storage version line of every storage manifest under `root`
/// and re-seals its checksum footer, returning how many files changed. This
/// models a database written by a release with another storage version.
pub fn rewrite_storage_version(root: &Path, from: &str, to: &str) -> usize {
    let mut rewritten = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.file_name().and_then(|name| name.to_str()) != Some("manifest.hawdb") {
                continue;
            }
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let needle = format!("version\t{from}\n");
            let Some((body, _)) = text.rsplit_once("checksum\t") else {
                continue;
            };
            if !body.contains(&needle) {
                continue;
            }
            let body = body.replace(&needle, &format!("version\t{to}\n"));
            let checksum = hawdb_integrity::checksum_u64(body.as_bytes());
            fs::write(&path, format!("{body}checksum\t{checksum}\n")).unwrap();
            rewritten += 1;
        }
    }
    rewritten
}

/// The checked-in format 1 dump: `(path, bytes)` for every file.
pub fn golden_v1_files() -> Vec<(&'static str, &'static [u8])> {
    vec![
        ("manifest.json", GOLDEN_MANIFEST.as_bytes()),
        ("schema.json", GOLDEN_SCHEMA.as_bytes()),
        ("data/table-00000-00000.jsonl", GOLDEN_DOCS.as_bytes()),
        ("data/table-00001-00000.jsonl", GOLDEN_REGISTRY.as_bytes()),
        ("data/nodes-00000-00000.jsonl", GOLDEN_NODES.as_bytes()),
        (
            "data/relationships-00000-00000.jsonl",
            GOLDEN_RELATIONSHIPS.as_bytes(),
        ),
    ]
}

/// `SELECT id, title, body, amount, score, payload, embedding, at, made FROM
/// docs ORDER BY id` after importing the golden dump.
pub fn golden_v1_rows() -> Vec<Vec<Value>> {
    let datetime = |text: &str| {
        hawdb::temporal::from_json_tag("$datetime", text)
            .unwrap()
            .unwrap()
    };
    let local = |text: &str| {
        hawdb::temporal::from_json_tag("$localdatetime", text)
            .unwrap()
            .unwrap()
    };
    vec![
        vec![
            Value::Int(i64::MIN),
            Value::String("最小".to_string()),
            Value::Null,
            Value::Decimal("-9999999999999999.9999".parse().unwrap()),
            Value::Float(f64::NEG_INFINITY),
            Value::Binary(Vec::new()),
            Value::Null,
            datetime("1970-01-01T00:00:00+00:00"),
            local("2024-01-02T03:04:05.123"),
        ],
        vec![
            Value::Int(1),
            Value::String("日志 🚀".to_string()),
            Value::String("line\nbreak \"quoted\" \\ tab\t".to_string()),
            Value::Decimal("12345.6789".parse().unwrap()),
            Value::Float(-0.0),
            Value::Binary(vec![0, 255, 10]),
            Value::Vector(hawdb::Vector::new(&[0.5, -1.25]).unwrap()),
            datetime("2026-11-11T08:30:00.123456+00:00"),
            local("2024-01-02T03:04:05.123"),
        ],
        vec![
            Value::Int(2),
            Value::String("Ünïcødé".to_string()),
            Value::String("ok".to_string()),
            Value::Null,
            Value::Float(f64::NAN),
            Value::Null,
            Value::Vector(hawdb::Vector::new(&[3.0, 4.0]).unwrap()),
            Value::Null,
            local("1999-12-31T23:59:59.999"),
        ],
        vec![
            Value::Int(i64::MAX),
            Value::String("最大".to_string()),
            Value::Null,
            Value::Decimal("0.0001".parse().unwrap()),
            Value::Float(1e300),
            Value::Binary(b"hawdb".to_vec()),
            Value::Null,
            Value::Null,
            local("2024-01-02T03:04:05.123"),
        ],
    ]
}

const GOLDEN_MANIFEST: &str = include_str!("golden_v1/manifest.json");
const GOLDEN_SCHEMA: &str = include_str!("golden_v1/schema.json");
const GOLDEN_DOCS: &str = include_str!("golden_v1/data/table-00000-00000.jsonl");
const GOLDEN_REGISTRY: &str = include_str!("golden_v1/data/table-00001-00000.jsonl");
const GOLDEN_NODES: &str = include_str!("golden_v1/data/nodes-00000-00000.jsonl");
const GOLDEN_RELATIONSHIPS: &str = include_str!("golden_v1/data/relationships-00000-00000.jsonl");
