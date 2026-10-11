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
use crate::background::{CheckpointWorkError, CheckpointWorkProbe};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

fn oracle(keys: &[String]) -> bool {
    let mut seen = BTreeSet::new();
    keys.iter().all(|key| seen.insert(key.as_str()))
}

#[test]
fn checkpoint_units_canonical_validation_memory_related_matches_tree_oracle_across_merge_boundaries(
) {
    let ceiling = 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    for count in [0, 1, 31, 32, 33, 1023, 1024, 1025, 4097, 8193] {
        let mut keys: Vec<_> = (0..count).map(|index| format!("key-{index:05}")).collect();
        assert!(oracle(&keys));
        validate(&keys, &work).unwrap();
        assert_eq!(used(&task, ceiling), 0);
        if count > 0 {
            keys.push(keys[count / 2].clone());
            assert!(!oracle(&keys));
            assert!(matches!(
                validate(&keys, &work),
                Err(CanonicalSegmentError::Corrupt(_))
            ));
            assert_eq!(used(&task, ceiling), 0);
            keys.pop();
            validate(&keys, &work).unwrap();
            assert_eq!(used(&task, ceiling), 0);
        }
    }
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_validation_memory_related_digest_collisions_require_exact_nonadjacent_checks(
) {
    let first = format!("{}a", "界".repeat(87_723));
    let different = format!("{}b", "界".repeat(87_723));
    let mut keys = vec![first.clone(), different, String::new(), first];
    let colliding: Vec<_> = (0..keys.len())
        .map(|index| Key { index, ..EMPTY })
        .collect();
    assert!(!oracle(&keys));
    let work = CheckpointWorkContext::default();
    assert!(matches!(
        validate_sorted(&keys, &colliding, &work),
        Err(CanonicalSegmentError::Corrupt(_))
    ));
    keys.pop();
    assert!(oracle(&keys));
    validate_sorted(&keys, &colliding[..keys.len()], &work).unwrap();
}

#[test]
fn checkpoint_units_canonical_validation_memory_related_every_collision_comparison_unit_cancels() {
    let keys = vec![
        format!("{}a", "界".repeat(87_723)),
        format!("{}b", "界".repeat(87_723)),
        String::new(),
        "other".into(),
    ];
    let colliding: Vec<_> = (0..keys.len())
        .map(|index| Key { index, ..EMPTY })
        .collect();
    assert!(oracle(&keys));
    let ceiling = 1024 * 1024;
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
    validate_sorted(&keys, &colliding, &work).unwrap();
    drop(work);
    let total = baseline.completed.load(Ordering::SeqCst);
    assert!(total > 10);
    assert_eq!(baseline.peak_units.load(Ordering::SeqCst), 1);
    baseline.assert_released(&local);
    for cut in 1..=total {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        assert!(matches!(
            validate_sorted(&keys, &colliding, &work),
            Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_)))
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), cut);
        probe.assert_released(&local);
        assert_eq!(used(&task, ceiling), 0);
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    validate_sorted(&keys, &colliding, &work).unwrap();
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_canonical_validation_memory_related_merge_overlap_denial_releases_input_and_retries(
) {
    let keys: Vec<_> = (0..8193).map(|index| format!("key-{index:05}")).collect();
    assert!(oracle(&keys));
    let ceiling = 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let held = task.reserve_working_memory(600 * 1024).unwrap();
    let before = used(&task, ceiling);
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = validate(&keys, &work);
    let allocations = observation.finish();
    assert!(
        matches!(result, Err(CanonicalSegmentError::Work(CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }))) if available_bytes > 0)
    );
    assert_eq!(
        allocations, 1,
        "only the admitted initial array may allocate; merge overlap is denied first"
    );
    assert_eq!(used(&task, ceiling), before);
    assert!(oracle(&keys));
    drop(held);
    validate(&keys, &work).unwrap();
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
