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
use crate::background::CheckpointOperationError;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

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
struct Fixture {
    directory: PathBuf,
    manifest: CanonicalSegmentManifest,
    expected: String,
}
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-manifest-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let name = format!("{}🦀\0\t\n", "界".repeat(87_723));
        let node = NodeRecord {
            id: NodeId(5),
            labels: BTreeSet::from([LabelId(7)]),
            properties: BTreeMap::from([(name.clone(), Value::Int(17))]),
        };
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(19),
                [&node],
                std::iter::empty::<&RelRecord>(),
            )
            .unwrap();
        assert_eq!(manifest.property_keys, vec![name]);
        let expected = manifest.encode().unwrap();
        assert!(expected.len() > 512 * 1024);
        assert_eq!(
            CanonicalSegmentManifest::decode(&expected).unwrap(),
            manifest
        );
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(19),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.get_node(node.id).unwrap().unwrap(), node);
        Self {
            directory,
            manifest,
            expected,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn checkpoint_units_canonical_manifest_memory_denies_before_large_property_name_expansion() {
    let fixture = Fixture::new();
    let ceiling = 1;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| fixture.manifest.encode_with_work_context(work));
    let allocations = observation.finish();
    assert!(matches!(result, Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { .. })))), "canonical manifest capacity must be admitted before expansion; observed large allocations={allocations}");
    assert_eq!(allocations, 0);
    assert_eq!(fixture.manifest.encode().unwrap(), fixture.expected);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_manifest_memory_encoded_output_retains_capacity_after_execution_closes(
) {
    let fixture = Fixture::new();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let output = fixture.manifest.encode_with_work_context(&work).unwrap();
    assert_eq!(output, fixture.expected);
    assert_eq!(
        CanonicalSegmentManifest::decode(&output).unwrap(),
        fixture.manifest
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        ceiling,
        "manifest text retains its actual allocation after the worker closes"
    );
    assert_eq!(output, fixture.expected);
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
