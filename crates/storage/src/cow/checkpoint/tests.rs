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

#[test]
fn empty_map_insert_denies_initial_capacity_before_changing_shared_storage() {
    let mut map = CowSegmentedMap::<NodeId, NodeId>::default();
    let pin = map.clone();
    let (governor, admission, work) = admitted(64, RuntimeTaskContext::default(), scheduler());
    let result = map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work);
    assert!(
        matches!(result, Err(CheckpointWorkError::Memory(_))),
        "initial page and directory capacity must be admitted before allocation"
    );
    assert!(map.shares_storage_with(&pin));
    assert!(map.is_empty());
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn empty_map_first_page_charge_survives_insertion_task_close_and_snapshot_pins() {
    let mut map = CowSegmentedMap::<NodeId, NodeId>::default();
    let original = map.clone();
    let (governor, admission, work) = admitted(4096, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
        .unwrap();
    assert!(map.is_empty());
    assert!(map.get(&NodeId(0)).is_none());
    let first_page_bound = ownership_bytes::<BTreeMap<NodeId, NodeId>>()
        + tree_bytes::<NodeId, NodeId>(1, &work).unwrap();
    assert!(
        admission.memory_report().live_accounted_bytes >= first_page_bound as u64,
        "first-page capacity ownership must precede insertion"
    );
    assert!(!map.shares_storage_with(&original));
    map.insert(NodeId(0), NodeId(9));
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&NodeId(0)), Some(&NodeId(9)));
    assert!(original.is_empty());
    let pin = map.clone();
    drop(work);
    drop(admission);
    drop(map);
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    assert_eq!(pin.get(&NodeId(0)), Some(&NodeId(9)));
    drop(pin);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn empty_map_cancellation_at_every_initial_unit_preserves_the_original_root() {
    let original = CowSegmentedMap::<NodeId, NodeId>::default();
    let observer = Arc::new(CancelAt {
        calls: AtomicUsize::new(0),
        at: usize::MAX,
        token: RuntimeCancellationToken::new(),
    });
    let qos = scheduler();
    qos.set_telemetry_sink(Some(observer.clone()));
    let (governor, admission, work) = admitted(4096, RuntimeTaskContext::default(), qos);
    let mut map = original.clone();
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
        .unwrap();
    let units = observer.calls.load(Ordering::SeqCst);
    assert!(units > 0);
    drop(map);
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
            4096,
            RuntimeTaskContext::without_deadline(token),
            qos.clone(),
        );
        let mut map = original.clone();
        let error = map
            .prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
            .unwrap_err();
        assert!(
            matches!(error, CheckpointWorkError::Stopped(_)),
            "unit {at}: {error}"
        );
        assert!(map.shares_storage_with(&original), "unit {at}");
        assert!(map.is_empty());
        assert_eq!(
            admission.memory_report().live_accounted_bytes,
            0,
            "unit {at}"
        );
        assert_eq!(qos.state().running_background_operations, 0);
        drop(map);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn empty_map_copy_after_a_snapshot_keeps_its_first_insertion_capacity_admitted() {
    let mut map = CowSegmentedMap::<NodeId, NodeId>::default();
    let (governor, admission, work) = admitted(4096, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
        .unwrap();
    let empty_pin = map.clone();
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
        .unwrap();
    assert!(map.is_empty());
    assert!(!map.shares_storage_with(&empty_pin));
    drop(empty_pin);
    let bound = ownership_bytes::<BTreeMap<NodeId, NodeId>>()
        + tree_bytes::<NodeId, NodeId>(1, &work).unwrap();
    assert!(
        admission.memory_report().live_accounted_bytes >= bound as u64,
        "a copied empty page must retain first-insertion capacity after the old pin closes"
    );
    map.insert(NodeId(0), NodeId(9));
    assert_eq!(map.get(&NodeId(0)), Some(&NodeId(9)));
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    drop(map);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn unique_page_insertion_denies_growth_before_mutating_or_losing_ownership() {
    let mut map = CowSegmentedMap::from(BTreeMap::from([(NodeId(0), NodeId(9))]));
    let (governor, admission, work) = admitted(64, RuntimeTaskContext::default(), scheduler());
    let result = map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &work);
    assert!(
        matches!(result, Err(CheckpointWorkError::Memory(_))),
        "a unique page still needs insertion/split capacity admission"
    );
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&NodeId(0)), Some(&NodeId(9)));
    assert!(map.get(&NodeId(1)).is_none());
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn preflighted_insertions_keep_growth_and_split_charges_with_every_live_page() {
    let count = 1025usize;
    let mut map = CowSegmentedMap::<NodeId, NodeId>::default();
    let old = map.clone();
    let (governor, admission, work) =
        admitted(4 * 1024 * 1024, RuntimeTaskContext::default(), scheduler());
    let mut first_charge = None;
    for id in 0..count {
        map.prepare_checkpoint_insertions_for_key(&NodeId(id as u64), count, &work)
            .unwrap();
        let charge = admission.memory_report().live_accounted_bytes;
        assert_eq!(
            *first_charge.get_or_insert(charge),
            charge,
            "preflight must share one growth allowance across the complete batch"
        );
    }
    let directory_capacity = map.segments.capacity();
    assert!(directory_capacity >= count);
    let charge = admission.memory_report().live_accounted_bytes;
    let page_charge = CowSegmentedMap::<NodeId, NodeId>::checkpoint_growth_bytes(
        0,
        count,
        size_of::<NodeId>(),
        &work,
    )
    .unwrap() as u64;
    assert!(
        charge >= page_charge,
        "the complete growth bound must remain admitted"
    );
    for id in 0..count {
        map.insert(NodeId(id as u64), NodeId(id as u64 + 9));
    }
    assert_eq!(map.segments.capacity(), directory_capacity);
    assert!(map.segment_count() >= 3);
    assert_eq!(map.len(), count);
    assert!(old.is_empty());
    for id in 0..count {
        assert_eq!(map.get(&NodeId(id as u64)), Some(&NodeId(id as u64 + 9)));
    }
    let pin = map.clone();
    let last_page = map.segments.last().unwrap().clone();
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes >= charge);
    drop(map);
    assert!(governor.snapshot().admitted_memory_bytes >= charge);
    drop(pin);
    assert!(
        governor.snapshot().admitted_memory_bytes >= page_charge,
        "the split right page must retain the original growth inventory"
    );
    assert_eq!(
        last_page.last_key_value(),
        Some((&NodeId(1024), &NodeId(1033)))
    );
    drop(last_page);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(old);
}

#[test]
fn existing_growth_allowance_does_not_bypass_a_lower_resumed_memory_ceiling() {
    let mut map = CowSegmentedMap::from(BTreeMap::from([(NodeId(0), NodeId(9))]));
    let (governor, admission, work) =
        admitted(16 * 1024, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insertions_for_key(&NodeId(1), 4, &work)
        .unwrap();
    let charge = admission.memory_report().live_accounted_bytes;
    let (lower_governor, lower_admission, lower_work) =
        admitted(64, RuntimeTaskContext::default(), scheduler());
    let result = map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &lower_work);
    assert!(matches!(result, Err(CheckpointWorkError::Memory(_))));
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(&NodeId(0)), Some(&NodeId(9)));
    assert!(map.get(&NodeId(1)).is_none());
    assert_eq!(admission.memory_report().live_accounted_bytes, charge);
    assert_eq!(lower_admission.memory_report().live_accounted_bytes, 0);
    drop(lower_work);
    drop(lower_admission);
    assert_eq!(lower_governor.snapshot().admitted_memory_bytes, 0);
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes >= charge);
    drop(map);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn consumed_growth_allowance_requires_new_admission_for_the_next_batch() {
    let mut map = CowSegmentedMap::<NodeId, NodeId>::default();
    let (governor, admission, work) =
        admitted(16 * 1024, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insertions_for_key(&NodeId(0), 4, &work)
        .unwrap();
    for id in 0..4 {
        map.insert(NodeId(id), NodeId(id + 9));
    }
    let before = admission.memory_report().live_accounted_bytes;
    map.prepare_checkpoint_insertions_for_key(&NodeId(4), 4, &work)
        .unwrap();
    assert!(
        admission.memory_report().live_accounted_bytes > before,
        "consumed credits cannot admit another batch for free"
    );
    for id in 4..8 {
        map.insert(NodeId(id), NodeId(id + 9));
    }
    assert_eq!(map.len(), 8);
    for id in 0..8 {
        assert_eq!(map.get(&NodeId(id)), Some(&NodeId(id + 9)));
    }
    let pin = map.clone();
    drop(work);
    drop(admission);
    drop(map);
    assert!(governor.snapshot().admitted_memory_bytes > before);
    drop(pin);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn insertion_cancellation_at_every_growth_unit_keeps_the_original_root() {
    let make_map = || CowSegmentedMap::from(BTreeMap::from([(NodeId(0), NodeId(9))]));
    let mut map = make_map();
    let original = map.clone();
    let observer = Arc::new(CancelAt {
        calls: AtomicUsize::new(0),
        at: usize::MAX,
        token: RuntimeCancellationToken::default(),
    });
    let observed_scheduler = scheduler();
    observed_scheduler.set_telemetry_sink(Some(observer.clone()));
    let (governor, admission, work) =
        admitted(4096, RuntimeTaskContext::default(), observed_scheduler);
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &work)
        .unwrap();
    let units = observer.calls.load(Ordering::SeqCst);
    assert!(units > 0);
    drop(work);
    drop(admission);
    drop(map);
    drop(original);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    for at in 1..=units {
        let mut map = make_map();
        let original = map.clone();
        let token = RuntimeCancellationToken::default();
        let observer = Arc::new(CancelAt {
            calls: AtomicUsize::new(0),
            at,
            token: token.clone(),
        });
        let observed_scheduler = scheduler();
        observed_scheduler.set_telemetry_sink(Some(observer));
        let task = RuntimeTaskContext::without_deadline(token);
        let (governor, admission, work) = admitted(4096, task, observed_scheduler.clone());
        assert!(matches!(
            map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &work),
            Err(CheckpointWorkError::Stopped(_))
        ));
        assert!(map.shares_storage_with(&original), "unit {at}");
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(&NodeId(0)), Some(&NodeId(9)));
        assert_eq!(admission.memory_report().live_accounted_bytes, 0);
        assert_eq!(observed_scheduler.state().running_background_operations, 0);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn replacing_a_record_consumes_growth_before_another_split_can_be_admitted() {
    let mut map = CowSegmentedMap::from(
        (0..10)
            .map(|id| (NodeId(id), record(id, "")))
            .collect::<BTreeMap<_, _>>(),
    );
    assert_eq!(map.segment_count(), 1);
    let (governor, admission, work) =
        admitted(8 * 1024, RuntimeTaskContext::default(), scheduler());
    map.prepare_checkpoint_insert_copy_for_key(&NodeId(0), &work)
        .unwrap();
    let previous = map
        .insert(NodeId(0), record(0, &"p".repeat(128 * 1024)))
        .unwrap();
    assert_eq!(previous, record(0, ""));
    assert_eq!(
        map.segment_count(),
        2,
        "a replacement can split a primary page"
    );
    let charge = admission.memory_report().live_accounted_bytes;
    let result = map.prepare_checkpoint_insert_copy_for_key(&NodeId(1), &work);
    assert!(
        matches!(result, Err(CheckpointWorkError::Memory(_))),
        "the first replacement consumed its allowance; the second split needs fresh admission"
    );
    assert_eq!(map.len(), 10);
    assert_eq!(
        map.get(&NodeId(0)).unwrap().properties.get("body"),
        Some(&Value::String("p".repeat(128 * 1024)))
    );
    for id in 1..10 {
        assert_eq!(map.get(&NodeId(id)), Some(&record(id, "")));
    }
    assert_eq!(admission.memory_report().live_accounted_bytes, charge);
    drop(work);
    drop(admission);
    assert!(governor.snapshot().admitted_memory_bytes >= charge);
    drop(map);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
