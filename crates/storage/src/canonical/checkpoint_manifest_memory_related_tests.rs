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
use crate::background::{CheckpointOperationError, CheckpointWorkProbe};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::fmt::Write;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;

fn governor(ceiling: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        Ok(permit) => {
            drop(permit);
            0
        }
        Err(error) => panic!("unexpected memory observation: {error}"),
    }
}

struct Fixture {
    directory: PathBuf,
    manifest: CanonicalSegmentManifest,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-manifest-related-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([LabelId(5)]),
            properties: BTreeMap::from([
                (String::new(), Value::Null),
                (format!("{}🦀\0\t\n", "界".repeat(87_723)), Value::Int(19)),
                ("last".into(), Value::Bool(true)),
            ]),
        };
        let path = directory.join("canonical.hawdb");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(23),
                [&node],
                std::iter::empty::<&RelRecord>(),
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(23),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        Self {
            directory,
            manifest,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

// Independent copy of the established wire layout, with no controlled encoder
// calls. This intentionally uses the old unbounded formatting only as an oracle.
fn legacy_bytes(manifest: &CanonicalSegmentManifest) -> String {
    let mut body = format!(
        "{MANIFEST_HEADER_V1}\nrecord_layout\tproperty_key_ids\ngeneration\t{}\nsource_commit_epoch\t{}\nartifact_id\t{}\nartifact_len\t{}\nartifact_digest\t{}\nartifact_sha256\t{}\nnode_count\t{}\nrelationship_count\t{}\nsegment_count\t{}\nnode_segment_count\t{}\nrelationship_segment_count\t{}\ndescriptor_root_len\t{}\ndescriptor_root_crc32c\t{}\ndescriptor_root_sha256\t{}\n",
        manifest.generation.0,
        manifest.source_commit_epoch,
        manifest.artifact_id,
        manifest.artifact_len,
        manifest.artifact_digest.0,
        manifest.artifact_sha256,
        manifest.node_count,
        manifest.relationship_count,
        manifest.segment_count,
        manifest.node_segment_count,
        manifest.relationship_segment_count,
        manifest.descriptor_root_artifact.encoded_len,
        manifest.descriptor_root_artifact.encoded_crc32c,
        manifest.descriptor_root_artifact.encoded_sha256,
    );
    for (id, key) in manifest.property_keys.iter().enumerate() {
        write!(body, "property_key\t{id}\t").unwrap();
        for byte in key.bytes() {
            write!(body, "{byte:02x}").unwrap();
        }
        body.push('\n');
    }
    format!("{body}checksum\t{}\n", content_digest(body.as_bytes()).0)
}

#[test]
fn checkpoint_units_canonical_manifest_memory_matches_legacy_bytes_at_scalar_boundaries() {
    let fixture = Fixture::new();
    for maximum in [false, true] {
        for populated in [false, true] {
            let mut manifest = fixture.manifest.clone();
            if maximum {
                manifest.generation = ManifestGeneration(u64::MAX);
                manifest.source_commit_epoch = u64::MAX;
                manifest.artifact_len = u64::MAX;
                manifest.artifact_digest = ContentDigest(u64::MAX);
                manifest.node_count = u64::MAX;
                manifest.relationship_count = u64::MAX;
                manifest.node_segment_count = u64::MAX - 1;
                manifest.relationship_segment_count = 1;
                manifest.segment_count = u64::MAX;
                manifest.descriptor_root_artifact.encoded_len = u64::MAX;
                manifest.descriptor_root_artifact.encoded_crc32c = u32::MAX;
            }
            if !populated {
                manifest.property_keys.clear();
            }
            manifest.validate().unwrap();
            let expected = legacy_bytes(&manifest);
            assert_eq!(manifest.encode().unwrap(), expected);
            assert_eq!(
                CanonicalSegmentManifest::decode(&expected).unwrap(),
                manifest
            );
        }
    }
}

#[test]
fn checkpoint_units_canonical_manifest_memory_capacity_has_no_growth_debt() {
    let fixture = Fixture::new();
    let expected = legacy_bytes(&fixture.manifest);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let encoded = fixture.manifest.encode_with_work_context(&work).unwrap();
    assert_eq!(encoded, expected);
    let capacity = encoded.text.capacity() as u64;
    assert!(capacity > 512 * 1024);
    assert!(used(&task, ceiling) >= capacity);
    assert!(used(&task, ceiling) < capacity + 256);
    drop(encoded);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_manifest_memory_every_actual_unit_cancels_and_retries() {
    let fixture = Fixture::new();
    let expected = legacy_bytes(&fixture.manifest);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    local.set_telemetry_sink(Some(baseline.clone()));
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let output = fixture.manifest.encode_with_work_context(&work).unwrap();
    assert_eq!(output, expected);
    drop(output);
    drop(work);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 64);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    assert_eq!(used(&task, ceiling), 0);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            fixture.manifest.encode_with_work_context(&work),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        probe.assert_released(&local);
        assert_eq!(used(&task, ceiling), 0);
        assert_eq!(fixture.manifest.encode().unwrap(), expected);
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let output = fixture.manifest.encode_with_work_context(&work).unwrap();
    assert_eq!(output, expected);
    assert_eq!(
        CanonicalSegmentManifest::decode(&output).unwrap(),
        fixture.manifest
    );
    drop(output);
    drop(work);
    assert_eq!(used(&task, ceiling), 0);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_manifest_memory_invalid_source_precedes_text_admission() {
    let mut fixture = Fixture::new();
    fixture
        .manifest
        .property_keys
        .push(fixture.manifest.property_keys[1].clone());
    let expected = fixture.manifest.encode().unwrap_err().to_string();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let result = work.classify(|work| fixture.manifest.encode_with_work_context(work));
    match result {
        Err(CheckpointOperationError::Operation(error @ CanonicalSegmentError::Corrupt(_))) => {
            assert_eq!(error.to_string(), expected);
        }
        _ => panic!("invalid manifest must retain its ordinary error"),
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
