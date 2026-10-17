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

#[test]
fn active_manifest_admission_covers_compact_untrusted_segment_sequences() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let path = fixture.0.join(OUT_OF_CORE_MANIFEST_FILE);
    let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    // Struct visitors accept sequence encodings too. Compact malformed names
    // still allocate complete records before checksum/name validation refuses.
    let fields = [
        "segment_id",
        "level",
        "generation",
        "descriptor_file",
        "descriptor_len",
        "descriptor_checksum",
        "payload_file",
        "payload_len",
        "metadata_payload_file",
        "metadata_payload_len",
        "vector_payload_file",
        "vector_payload_len",
        "layout_file",
        "layout_len",
        "layout_checksum",
        "lexical_manifest_file",
        "lexical_manifest_len",
        "lexical_manifest_checksum",
        "rabitq_artifact_file",
        "rabitq_artifact_len",
        "rabitq_artifact_checksum",
        "rabitq_source_digest",
        "rabitq_vector_document_count",
        "rabitq_payload_checksum",
        "rabitq_peak_build_working_bytes",
        "document_count",
        "documents_digest",
        "source_graph_commit_epoch",
    ];
    for count in [65, 513] {
        let mut value = original.clone();
        let template = &original["body"]["segments"][0];
        let record = serde_json::Value::Array(
            fields
                .iter()
                .map(|field| {
                    let value = template[*field].clone();
                    if value.is_string() {
                        serde_json::json!("")
                    } else {
                        value
                    }
                })
                .collect(),
        );
        value["body"]["segments"] = serde_json::Value::Array(vec![record; count]);
        let bytes = serde_json::to_vec(&value).unwrap();
        let decoded: SearchOutOfCoreManifestEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.body.segments.len(), count);
        drop(decoded);
        fs::write(&path, &bytes).unwrap();
        let (memory, task) = context(64 * 1024 * 1024);
        let (_, peak) = allocation::measure(|| {
            assert!(active_identity(&fixture.0, &memory, &task).is_err());
        });
        assert!(
            peak <= memory.ledger.snapshot().peak_bytes,
            "{count} compact records requested {peak}, admitted {}",
            memory.ledger.snapshot().peak_bytes
        );
        assert_eq!(allocation::live(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        let (limited, task) = context(memory.ledger.snapshot().peak_bytes - 1);
        assert!(matches!(
            active_identity(&fixture.0, &limited, &task),
            Err(HawDBError::Execution(_))
        ));
        assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn active_manifest_admission_covers_compact_untrusted_mutation_sequences() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let path = fixture.0.join(OUT_OF_CORE_MANIFEST_FILE);
    let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for count in [65, 513] {
        let mut value = original.clone();
        value["body"]["mutation_runs"] =
            serde_json::Value::Array(vec![serde_json::json!([1, "", 1, 0, 1, 0]); count]);
        let bytes = serde_json::to_vec(&value).unwrap();
        let decoded: SearchOutOfCoreManifestEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.body.mutation_runs.len(), count);
        drop(decoded);
        fs::write(&path, &bytes).unwrap();
        let (memory, task) = context(64 * 1024 * 1024);
        let (_, peak) = allocation::measure(|| {
            assert!(active_identity(&fixture.0, &memory, &task).is_err());
        });
        assert!(
            peak <= memory.ledger.snapshot().peak_bytes,
            "{count} mutation records requested {peak}, admitted {}",
            memory.ledger.snapshot().peak_bytes
        );
        assert_eq!(allocation::live(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        let (limited, task) = context(memory.ledger.snapshot().peak_bytes - 1);
        assert!(matches!(
            active_identity(&fixture.0, &limited, &task),
            Err(HawDBError::Execution(_))
        ));
        assert_eq!(limited.ledger.snapshot().used_bytes, 0);
    }
}
