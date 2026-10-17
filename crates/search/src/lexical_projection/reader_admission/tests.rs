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
use crate::build_memory::{read::tests::governor, BuildMemory};
use crate::test_allocation as allocation;
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::RuntimeWorkRequest;

#[test]
fn admitted_lexical_manifest_refuses_compact_directory_before_record_allocation() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let body = super::super::manifest_encoding::tests::manifest(Vec::new());
    let records = std::iter::repeat_n("[0,\"Documents\",\"x\",\"x\",24,1,0,1,0]", 16385)
        .collect::<Vec<_>>()
        .join(",");
    let mut raw = serde_json::to_value(&body).unwrap();
    raw["blocks"] = serde_json::from_str(&format!("[{records}]")).unwrap();
    let decoded: ManifestBody = serde_json::from_value(raw.clone()).unwrap();
    let checksum = crate::checksum_bytes(&serde_json::to_vec(&decoded).unwrap());
    let bytes = serde_json::to_vec(&serde_json::json!({"body":raw,"checksum":checksum})).unwrap();
    assert!(bytes.len() < 2 << 20);
    let (error, peak) = allocation::measure(|| ManifestBody::decode(&bytes).unwrap_err());
    assert!(matches!(&error, HawDBError::Storage(message) if message.contains("invalid bounds")));
    assert!(
        peak > 2 << 20,
        "fixture must expand beyond native working budget: {peak}"
    );
    drop(error);
    assert_eq!(allocation::live(), 0);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
    let baseline = memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let (_, peak) = allocation::measure(|| {
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        };
        let error = LexicalProjectionReader::load_admitted_manifest_bytes(
            OpenContext {
                root: Path::new("unused-no-artifact-open"),
                identity: Identity {
                    source_epoch: body.source_graph_commit_epoch,
                    analyzer_digest: body.analyzer_digest,
                    documents_digest: body.documents_digest,
                },
                config: LexicalProjectionConfig::default(),
                admission: &admission,
                max_artifact_read_bytes: 32 << 20,
            },
            &bytes,
        )
        .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)));
    });
    assert!(
        peak < 16 << 10,
        "directory allocated before admission: {peak}"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(allocation::live(), 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn lexical_identity_mismatch_and_read_limit_release_retained_admission_without_opening() {
    let body = super::super::manifest_encoding::tests::manifest(Vec::new());
    let bytes = body.encode(1 << 20).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
    let baseline = memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let root =
        crate::test_temp_dir().join(format!("missing-lexical-admission-{}", std::process::id()));
    assert!(!root.exists());
    for mismatch in [true, false] {
        let result = LexicalProjectionReader::load_admitted_manifest_bytes(
            OpenContext {
                root: &root,
                identity: Identity {
                    source_epoch: body.source_graph_commit_epoch,
                    analyzer_digest: if mismatch {
                        body.analyzer_digest.wrapping_add(1)
                    } else {
                        body.analyzer_digest
                    },
                    documents_digest: body.documents_digest,
                },
                config: LexicalProjectionConfig::default(),
                admission: &admission,
                max_artifact_read_bytes: body.artifact_len - 1,
            },
            &bytes,
        );
        if mismatch {
            assert!(result.unwrap().is_none());
        } else {
            assert!(
                matches!(result, Err(HawDBError::Execution(message)) if message.contains("operation read limit"))
            );
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
        assert_eq!(governor.snapshot().admitted_memory_bytes, host);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
    }
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
