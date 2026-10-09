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
use crate::background::CheckpointWorkProbe;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
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
        Ok(lease) => {
            drop(lease);
            0
        }
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        Err(error) => panic!("unexpected memory observation: {error}"),
    }
}

struct Fixture {
    directory: PathBuf,
    manifest: CanonicalSegmentManifest,
}

impl Fixture {
    fn new(keys: impl IntoIterator<Item = String>) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-validation-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([LabelId(5)]),
            properties: BTreeMap::from([("base".into(), Value::Int(19))]),
        };
        let path = directory.join("canonical.hawdb");
        let mut manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(23),
                [&node],
                std::iter::empty::<&RelRecord>(),
            )
            .unwrap();
        assert_eq!(manifest.property_keys, vec!["base"]);
        manifest.property_keys.extend(keys);
        manifest.validate().unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(23),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
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

#[test]
fn checkpoint_units_canonical_validation_memory_admits_large_uniqueness_scratch_before_allocation()
{
    let fixture = Fixture::new((0..8192).map(|index| format!("key-{index:05}")));
    assert_eq!(fixture.manifest.property_keys.len(), 8193);
    let expected = fixture.manifest.encode().unwrap();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start_all();
    let result = fixture.manifest.validate_with_work_context(&work);
    let allocations = observation.finish();
    assert!(
        matches!(
            result,
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ),
        "validation scratch must be admitted before allocation; allocations={allocations}"
    );
    assert_eq!(allocations, 0);
    assert_eq!(used(&task, 1), 0);
    assert_eq!(fixture.manifest.encode().unwrap(), expected);
    assert_eq!(
        CanonicalSegmentManifest::decode(&expected).unwrap(),
        fixture.manifest
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_validation_memory_wide_keys_use_bounded_units_and_release_scratch() {
    let prefix = "界".repeat(87_723);
    let fixture = Fixture::new((0..128).map(|index| format!("{prefix}🦀\0\t\n-{index:03}")));
    let expected = fixture.manifest.encode().unwrap();
    let ceiling = 64 * 1024 * 1024;
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
    fixture.manifest.validate_with_work_context(&work).unwrap();
    drop(work);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(
        total > fixture.manifest.property_keys.len() * 4,
        "wide uniqueness validation cannot charge one operation per unbounded key: {total}"
    );
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    assert_eq!(used(&task, ceiling), 0);
    for cut in [1, total / 2, total - 1, total] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            fixture.manifest.validate_with_work_context(&work),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        probe.assert_released(&local);
        assert_eq!(used(&task, ceiling), 0);
        assert_eq!(fixture.manifest.encode().unwrap(), expected);
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    fixture.manifest.validate_with_work_context(&work).unwrap();
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(
        CanonicalSegmentManifest::decode(&expected).unwrap(),
        fixture.manifest
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
