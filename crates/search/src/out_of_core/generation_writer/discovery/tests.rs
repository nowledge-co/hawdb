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
use crate::out_of_core::generation_writer::{
    tests::document, tests::test_dir, SearchOutOfCoreGenerationWriter,
};
use crate::test_allocation as allocation;
use hawdb_core::RuntimeMemoryReservation;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = test_dir("discovery_ownership");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(document(0)).unwrap();
        writer.finish().unwrap();
        Self(root)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn context(bytes: usize) -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    // Initialize fixed ledger class metadata outside payload measurements.
    drop(memory.spool.reserve(1).unwrap());
    (memory, task)
}

#[test]
fn discovery_validates_complete_manifests_and_preserves_recovery_rules() {
    let fixture = Fixture::new();
    let (memory, task) = context(8 * 1024 * 1024);
    let manifest = fixture.0.join(OUT_OF_CORE_MANIFEST_FILE);
    let original = fs::read(&manifest).unwrap();
    assert_eq!(active(&fixture.0, &memory, &task).unwrap(), Some(1));
    assert_eq!(next(&fixture.0, u64::MAX, &memory, &task).unwrap(), 2);
    fs::write(&manifest, b"corrupt").unwrap();
    // A higher filename with a valid but mismatching body cannot advance recovery.
    fs::copy(
        fixture.0.join("search_lexical.manifest.1.hawdb"),
        fixture.0.join("search_lexical.manifest.99.hawdb"),
    )
    .unwrap();
    assert_eq!(next(&fixture.0, u64::MAX, &memory, &task).unwrap(), 2);
    fs::write(
        fixture.0.join("search_lexical.manifest.1.hawdb"),
        b"corrupt",
    )
    .unwrap();
    assert_eq!(next(&fixture.0, u64::MAX, &memory, &task).unwrap(), 1);
    fs::write(&manifest, &original).unwrap();
    fs::remove_file(&manifest).unwrap();
    assert_eq!(next(&fixture.0, u64::MAX, &memory, &task).unwrap(), 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn discovery_resource_failure_and_cancellation_never_reuse_a_generation() {
    let fixture = Fixture::new();
    for corrupt in [false, true] {
        if corrupt {
            fs::write(fixture.0.join(OUT_OF_CORE_MANIFEST_FILE), b"corrupt").unwrap();
        }
        for cancel in [false, true] {
            let (memory, task) = context(if cancel { 8 * 1024 * 1024 } else { 8192 });
            if cancel {
                task.cancellation().cancel();
            }
            assert!(matches!(
                next(&fixture.0, u64::MAX, &memory, &task),
                Err(HawDBError::Execution(_))
            ));
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn discovery_decode_admission_covers_valid_escaped_sequence_and_invalid_json() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let bytes = fs::read(fixture.0.join("search_lexical.manifest.1.hawdb")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut cases = vec![bytes];
    for count in [1, 5, 17, 257] {
        let mut changed = value.clone();
        let template = changed["body"]["blocks"][0].clone();
        changed["body"]["blocks"] = serde_json::Value::Array(
            (0..count)
                .map(|index| {
                    let mut block = template.clone();
                    block["min_key"] =
                        serde_json::json!(format!("{index:04}{}", "\\\"\n".repeat(65)));
                    block["max_key"] = block["min_key"].clone();
                    block
                })
                .collect(),
        );
        cases.push(serde_json::to_vec(&changed).unwrap());
        // Derived struct visitors also accept sequence representations.
        let blocks = changed["body"]["blocks"].as_array_mut().unwrap();
        for block in blocks {
            *block = serde_json::json!([
                block["block_id"],
                block["kind"],
                block["min_key"],
                block["max_key"],
                block["offset"],
                block["length"],
                block["checksum"],
                block["entry_count"],
                block["ordinal_start"],
            ]);
        }
        cases.push(serde_json::to_vec(&changed).unwrap());
    }
    cases.push(br#"{"body":{"unknown":0},"checksum":0}"#.to_vec());
    cases.push(format!("{{\"body\":{{\"{}\":0}}}}", "\\u0061".repeat(8193)).into_bytes());
    for bytes in cases {
        let (memory, task) = context(64 * 1024 * 1024);
        let (_, peak) = allocation::measure(|| {
            drop(admitted_manifest_generation(&bytes, &memory, &task));
        });
        assert!(
            peak <= memory.ledger.snapshot().peak_bytes,
            "requested {peak}, admitted {}",
            memory.ledger.snapshot().peak_bytes
        );
        assert_eq!(allocation::live(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        let exact = memory.ledger.snapshot().peak_bytes;
        let (limited, task) = context(exact - 1);
        assert!(matches!(
            admitted_manifest_generation(&bytes, &limited, &task),
            Err(HawDBError::Execution(_))
        ));
    }
    // Active outer decoding and native recovery traversal use the same ledger.
    for corrupt in [false, true] {
        if corrupt {
            fs::write(fixture.0.join(OUT_OF_CORE_MANIFEST_FILE), b"corrupt").unwrap();
        }
        let (memory, task) = context(8 * 1024 * 1024);
        let (result, peak) = allocation::measure(|| next(&fixture.0, u64::MAX, &memory, &task));
        assert_eq!(result.unwrap(), 2);
        assert!(peak <= memory.ledger.snapshot().peak_bytes);
        assert_eq!(allocation::live(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
