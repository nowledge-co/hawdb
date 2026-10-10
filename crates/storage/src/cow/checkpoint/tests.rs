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
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, QosTelemetryEvent, QosTelemetryPhase,
    QosTelemetrySink, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMaintenanceWork,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn admitted(
    ceiling: u64,
    task: RuntimeTaskContext,
    scheduler: LocalQosScheduler,
) -> (
    RuntimeGovernor,
    RuntimeMaintenanceWork,
    CheckpointWorkContext,
) {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(std::num::NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(4096, ceiling, 1, task)
        .unwrap();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(scheduler);
    (governor, admission, work)
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn record(id: u64, body: &str) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::from([LabelId(7)]),
        properties: BTreeMap::from([
            ("body".into(), Value::String(body.into())),
            ("rank".into(), Value::Int(0)),
        ]),
    }
}

#[test]
fn graph_page_copy_charge_follows_pins_after_pause_and_foreground_detachment() {
    let body = "索引".repeat(16 * 1024);
    let mut map = CowSegmentedMap::from(BTreeMap::from([(NodeId(0), record(0, &body))]));
    let old = map.clone();
    let (governor, mut admission, work) =
        admitted(2 * 1024 * 1024, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_copy_for_key(&NodeId(0), &work)
        .unwrap();
    assert!(!map.shares_storage_with(&old));
    assert!(
        admission.memory_report().live_accounted_bytes >= body.len() as u64,
        "page and directory allocation owners must follow live data"
    );
    assert_eq!(map.get(&NodeId(0)), old.get(&NodeId(0)));
    // This pin owns the actual admitted page and directory, including after
    // the task/admission handle closes. A normal foreground copy has its own
    // data while the pinned original retains the original allocation leases.
    let pin = map.clone();
    map.get_mut(&NodeId(0))
        .unwrap()
        .properties
        .insert("rank".into(), Value::Int(1));
    assert_eq!(
        pin.get(&NodeId(0)).unwrap().properties["rank"],
        Value::Int(0)
    );
    assert_eq!(
        map.get(&NodeId(0)).unwrap().properties["rank"],
        Value::Int(1)
    );
    admission.pause();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes >= body.len() as u64);
    drop(pin);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(
        old.get(&NodeId(0)).unwrap().properties["body"],
        Value::String(body)
    );
    drop(old);
    drop(map);
}

#[test]
fn missing_key_update_keeps_shared_storage_without_a_copy_charge() {
    let mut map = CowSegmentedMap::from(BTreeMap::from([(NodeId(0), record(0, "value"))]));
    let pin = map.clone();
    let (governor, admission, work) = admitted(4096, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_copy_for_key(&NodeId(1), &work)
        .unwrap();
    assert!(map.shares_storage_with(&pin));
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn absent_key_insertion_copies_the_target_page_with_retained_ownership() {
    let body = "copy".repeat(1024);
    let mut map = CowSegmentedMap::from(BTreeMap::from([(NodeId(0), record(0, &body))]));
    let pin = map.clone();
    let (governor, admission, work) =
        admitted(2 * 1024 * 1024, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &work)
        .unwrap();
    assert!(!map.shares_storage_with(&pin));
    assert!(admission.memory_report().live_accounted_bytes >= body.len() as u64);
    map.insert(NodeId(1), record(1, "new"));
    assert_eq!(map.len(), 2);
    assert_eq!(pin.len(), 1);
    assert!(pin.get(&NodeId(1)).is_none());
    assert_eq!(map.get(&NodeId(0)), pin.get(&NodeId(0)));
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes >= body.len() as u64);
    drop(map);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn large_directory_denies_before_allocating_and_keeps_every_shared_page() {
    let len = 10_000;
    // A valid sorted directory of small one-record pages isolates directory
    // copying from payload copying without constructing oversized records.
    let mut map = CowSegmentedMap {
        segments: CowSegment::from(
            (0..len)
                .map(|id| {
                    CowSegment::from(BTreeMap::from([(
                        NodeId(id),
                        NodeRecord {
                            id: NodeId(id),
                            labels: BTreeSet::new(),
                            properties: BTreeMap::new(),
                        },
                    )]))
                })
                .collect::<Vec<_>>(),
        ),
        len: len as usize,
        delta_pressure_bytes: u128::from(len) * 32,
    };
    let pin = map.clone();
    let (governor, admission, work) = admitted(4096, RuntimeTaskContext::default(), scheduler());
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = map.prepare_checkpoint_copy_for_key(&NodeId(0), &work);
    let large_allocations = observer.finish();
    assert!(
        matches!(result, Err(CheckpointWorkError::Memory(_))),
        "COW directory capacity must be admitted before allocation"
    );
    assert_eq!(large_allocations, 0);
    assert!(map.shares_storage_with(&pin));
    assert_eq!(map.shared_segment_count_with(&pin), len as usize);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(map.len(), len as usize);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[derive(Debug)]
struct CancelAt {
    calls: AtomicUsize,
    at: usize,
    token: RuntimeCancellationToken,
}
impl QosTelemetrySink for CancelAt {
    fn record_qos(&self, event: QosTelemetryEvent) {
        if event.phase == QosTelemetryPhase::Admission
            && self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.at
        {
            self.token.cancel();
        }
    }
}

#[test]
fn cancellation_at_every_copy_unit_preserves_original_directory_and_refunds_buffers() {
    let original = CowSegmentedMap::from(
        (0..128)
            .map(|id| (NodeId(id), record(id, "value")))
            .collect::<BTreeMap<_, _>>(),
    );
    let observer = Arc::new(CancelAt {
        calls: AtomicUsize::new(0),
        at: usize::MAX,
        token: RuntimeCancellationToken::new(),
    });
    let qos = scheduler();
    qos.set_telemetry_sink(Some(observer.clone()));
    let (governor, admission, work) = admitted(2 * 1024 * 1024, RuntimeTaskContext::default(), qos);
    let mut successful = original.clone();
    successful
        .prepare_checkpoint_copy_for_key(&NodeId(0), &work)
        .unwrap();
    let units = observer.calls.load(Ordering::SeqCst);
    assert!(units > original.len());
    drop(successful);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    for at in 1..=units {
        let token = RuntimeCancellationToken::new();
        let observer = Arc::new(CancelAt {
            calls: AtomicUsize::new(0),
            at,
            token: token.clone(),
        });
        let qos = scheduler();
        qos.set_telemetry_sink(Some(observer));
        let (governor, admission, work) = admitted(
            2 * 1024 * 1024,
            RuntimeTaskContext::without_deadline(token),
            qos.clone(),
        );
        let mut map = original.clone();
        let error = map
            .prepare_checkpoint_copy_for_key(&NodeId(0), &work)
            .unwrap_err();
        assert!(
            matches!(error, CheckpointWorkError::Stopped(_)),
            "unit {at}: {error}"
        );
        assert!(map.shares_storage_with(&original), "unit {at}");
        assert_eq!(
            map.iter().collect::<Vec<_>>(),
            original.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            admission.memory_report().live_accounted_bytes,
            0,
            "unit {at}"
        );
        assert_eq!(qos.state().running_background_operations, 0);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
