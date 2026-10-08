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
use crate::background::{CheckpointAllocationOwner, CheckpointWorkProbe};
use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTaskContext,
    RuntimeWorkRequest, WorkClass, WorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::{atomic::Ordering, Mutex};

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

fn available(task: &RuntimeTaskContext) -> u64 {
    // A larger request stops at the static task ceiling and never queries the
    // governor ledger. An exact-ceiling request reaches the controller, where
    // the lease's own allocation makes it fail even when no buffers are live.
    let ceiling = task.memory_reservation().unwrap().memory_bytes();
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("the oversized probe must expose the real working-memory remainder"),
    }
}

fn permit_overhead() -> u64 {
    let governor = governor(1024);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1024))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let before = available(&task);
    let sample = task.reserve_working_memory(1).unwrap().unwrap();
    let overhead = before - available(&task) - sample.bytes();
    drop(sample);
    assert_eq!(available(&task), before);
    overhead
}

#[test]
fn checkpoint_units_wal_decode_memory_one_byte_overlap_denies_and_retries_same_reservation() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::SetNodeProperty {
            id: NodeId(3),
            property: String::new(),
            value: Value::Binary(vec![0x9a; 64 * 1024 + 7]),
        },
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let capacity = 64 * 1024 + 7;
    let overhead = permit_overhead();
    let required = capacity + CheckpointAllocationOwner::METADATA_BYTES as u64 + 2 * overhead;
    let ceiling = required + overhead;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let held = task.reserve_working_memory(1).unwrap().unwrap();
    assert_eq!(available(&task), required - 1);
    let work = CheckpointWorkContext::new(task.clone());
    let error = match decode_binary_wal_record_with_work_context(&bytes, &work) {
        Err(HawDBError::Execution(error)) => error,
        _ => {
            panic!("exactly one byte of unavailable decoded capacity must defer before allocation")
        }
    };
    assert!(error.contains(&format!("requested {} bytes", capacity + overhead)));
    assert!(error.contains(&format!("{}-byte remaining", capacity + overhead - 1)));
    assert_eq!(available(&task), required - 1);
    drop(held);
    let decoded = decode_binary_wal_record_with_work_context(&bytes, &work).unwrap();
    assert!(
        super::tests::outcome(Ok(decoded))
            == super::tests::outcome(super::super::decode_binary_wal_record(&bytes))
    );
    assert_eq!(available(&task), ceiling);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_runtime_and_cow_snapshot_retain_admission() {
    let name = "graph".repeat(200);
    let labels = vec!["界🦀".repeat(1000), "second".into()];
    let types = vec!["EDGE".repeat(400)];
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::ProjectGraph {
            name: name.clone(),
            node_labels: labels.clone(),
            rel_types: types.clone(),
        },
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(_) => panic!("complete projection record must decode"),
    };
    let mut store = crate::store::GraphStore::in_memory();
    let mut catalog = hawdb_core::Catalog::default();
    decoded
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap();
    let snapshot = store.snapshot();
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(
        store.projected_graph_definition(&name).unwrap().node_labels,
        labels
    );
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    let definition = snapshot.projected_graph_definition(&name).unwrap();
    assert_eq!(definition.node_labels, labels);
    assert_eq!(definition.rel_types, types);
    drop(snapshot);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[derive(Debug)]
struct ObservedMemory {
    task: RuntimeTaskContext,
    active: Arc<Mutex<Vec<u64>>>,
    success: Arc<Mutex<Vec<u64>>>,
}

#[derive(Debug)]
struct ObservedPermit {
    permit: Box<dyn RuntimeMemoryPermit>,
    active: Arc<Mutex<Vec<u64>>>,
}

impl RuntimeMemoryPermit for ObservedPermit {
    fn bytes(&self) -> u64 {
        self.permit.bytes()
    }
}

impl Drop for ObservedPermit {
    fn drop(&mut self) {
        let mut active = self.active.lock().unwrap();
        let index = active
            .iter()
            .position(|bytes| *bytes == self.permit.bytes())
            .unwrap();
        active.remove(index);
    }
}

impl RuntimeMemoryController for ObservedMemory {
    fn reserve(
        &self,
        bytes: u64,
        _ceiling: u64,
    ) -> std::result::Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
        let permit = self
            .task
            .reserve_working_memory(bytes)?
            .expect("observer delegates a real governor lease");
        self.active.lock().unwrap().push(bytes);
        self.success.lock().unwrap().push(bytes);
        Ok(Box::new(ObservedPermit {
            permit,
            active: self.active.clone(),
        }))
    }
}

#[test]
fn checkpoint_units_wal_decode_memory_growth_releases_old_capacity_keeps_inventory_and_output() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Batch(
            (0..1057)
                .map(|id| WalOp::DeleteNode { id: NodeId(id) })
                .collect(),
        ),
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let observed = Arc::new(ObservedMemory {
        task: task.clone(),
        active: Default::default(),
        success: Default::default(),
    });
    let work = CheckpointWorkContext::new(task.clone().with_memory_controller(observed.clone()));
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(_) => panic!("complete 1057-operation batch must decode"),
    };
    let WalOp::Batch(ops) = &decoded.op else {
        panic!("the batch envelope must remain");
    };
    let capacity_bytes = (ops.capacity() * std::mem::size_of::<WalOp>()) as u64;
    let metadata = CheckpointAllocationOwner::METADATA_BYTES as u64;
    let active = observed.active.lock().unwrap().clone();
    let success = observed.success.lock().unwrap().clone();
    assert!(success.iter().filter(|bytes| **bytes != metadata).count() > 10);
    assert_eq!(
        active
            .iter()
            .filter(|bytes| **bytes != metadata)
            .copied()
            .collect::<Vec<_>>(),
        vec![capacity_bytes]
    );
    assert_eq!(
        ceiling - available(&task),
        active.iter().sum::<u64>() + active.len() as u64 * permit_overhead()
    );
    assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
    drop(decoded);
    assert!(observed.active.lock().unwrap().is_empty());
    assert_eq!(available(&task), ceiling);
    drop(work);
    drop(observed);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_cancel_every_unit_fully_retries_same_governor() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::ProjectGraph {
            name: "graph".into(),
            node_labels: vec!["first".into(), "second".into()],
            rel_types: vec!["EDGE".into()],
        },
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(baseline.clone()));
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(scheduler.clone());
    super::tests::parity(&bytes, &work);
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 30);
    baseline.assert_released(&scheduler);
    drop(work);
    for stop in 0..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        scheduler.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(scheduler.clone());
        let held = (stop == 0).then(|| {
            scheduler
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap()
        });
        assert!(matches!(
            decode_binary_wal_record_with_work_context(&bytes, &work),
            Err(HawDBError::Execution(_))
        ));
        drop(held);
        probe.assert_released(&scheduler);
        drop(work);
        scheduler.set_telemetry_sink(None);
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        assert_eq!(available(&task), ceiling);
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(scheduler.clone());
        super::tests::parity(&bytes, &work);
        assert_eq!(available(&task), ceiling);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_partial_replay_retains_moved_data_and_snapshot() {
    struct ResetApplyFailure;
    impl Drop for ResetApplyFailure {
        fn drop(&mut self) {
            crate::store::set_wal_apply_failpoint(None);
        }
    }
    let labels = vec!["界🦀".repeat(1000)];
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Batch(vec![
            WalOp::ProjectGraph {
                name: "first".into(),
                node_labels: labels.clone(),
                rel_types: Vec::new(),
            },
            WalOp::ProjectGraph {
                name: "unapplied".into(),
                node_labels: vec!["second".repeat(1000)],
                rel_types: Vec::new(),
            },
        ]),
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(_) => panic!("complete batch must decode"),
    };
    let mut store = crate::store::GraphStore::in_memory();
    let mut catalog = hawdb_core::Catalog::default();
    let reset = ResetApplyFailure;
    crate::store::set_wal_apply_failpoint(Some(1));
    let error = decoded
        .replay_into(&mut store, &mut catalog, &work)
        .unwrap_err();
    drop(reset);
    assert!(error.to_string().contains("injected failure"));
    assert_eq!(store.commit_epoch(), 0);
    assert!(store.projected_graph_definition("unapplied").is_none());
    assert_eq!(
        store
            .projected_graph_definition("first")
            .unwrap()
            .node_labels,
        labels
    );
    let snapshot = store.snapshot();
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(
        snapshot
            .projected_graph_definition("first")
            .unwrap()
            .node_labels,
        labels
    );
    drop(snapshot);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_arc_conversion_releases_temporary_capacity() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Relational {
            record: Arc::from(vec![0xa7; 3 * 64 * 1024 + 7]),
        },
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let observed = Arc::new(ObservedMemory {
        task: task.clone(),
        active: Default::default(),
        success: Default::default(),
    });
    let work = CheckpointWorkContext::new(task.clone().with_memory_controller(observed.clone()));
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(_) => panic!("complete binary record must decode"),
    };
    let WalOp::Relational { record } = &decoded.op else {
        panic!("the relational envelope must remain");
    };
    let align = std::mem::align_of::<std::sync::atomic::AtomicUsize>();
    let arc_bytes = (record.len() + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
        .div_ceil(align)
        * align;
    let metadata = CheckpointAllocationOwner::METADATA_BYTES as u64;
    let active = observed.active.lock().unwrap().clone();
    let success = observed.success.lock().unwrap().clone();
    assert_eq!(
        success
            .iter()
            .filter(|bytes| **bytes != metadata)
            .copied()
            .collect::<Vec<_>>(),
        vec![record.len() as u64, arc_bytes as u64]
    );
    assert_eq!(
        active
            .iter()
            .filter(|bytes| **bytes != metadata)
            .copied()
            .collect::<Vec<_>>(),
        vec![arc_bytes as u64]
    );
    assert_eq!(
        ceiling - available(&task),
        active.iter().sum::<u64>() + active.len() as u64 * permit_overhead()
    );
    assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(decoded);
    assert!(observed.active.lock().unwrap().is_empty());
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_late_corruption_releases_earlier_allocations() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Batch(vec![
            WalOp::ProjectGraph {
                name: "first".into(),
                node_labels: vec!["wide".repeat(40_000)],
                rel_types: Vec::new(),
            },
            WalOp::ProjectGraph {
                name: "second".into(),
                node_labels: vec!["truncated".into()],
                rel_types: Vec::new(),
            },
        ]),
    };
    let mut bytes = encode_binary_wal_record(&entry, 19).unwrap();
    bytes.pop();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let observed = Arc::new(ObservedMemory {
        task: task.clone(),
        active: Default::default(),
        success: Default::default(),
    });
    let work = CheckpointWorkContext::new(task.clone().with_memory_controller(observed.clone()));
    let ordinary = super::super::decode_binary_wal_record(&bytes);
    assert!(matches!(&ordinary, Ok(BinaryWalRecordDecode::Corrupt(_))));
    assert!(
        super::tests::outcome(decode_binary_wal_record_with_work_context(&bytes, &work))
            == super::tests::outcome(ordinary)
    );
    assert!(
        !observed.success.lock().unwrap().is_empty(),
        "the later error must follow actual earlier allocations"
    );
    assert!(observed.active.lock().unwrap().is_empty());
    assert_eq!(available(&task), ceiling);
    drop(work);
    drop(observed);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_decode_memory_pressure_defers_and_recovers_same_admission() {
    let bytes = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: WalOp::ProjectGraph {
                name: "graph".into(),
                node_labels: vec!["first".into()],
                rel_types: Vec::new(),
            },
        },
        19,
    )
    .unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let normal = RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
        RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
    );
    let mut critical = normal;
    critical.memory.pressure = hawdb_qos::RuntimeMemoryPressure::Critical;
    assert!(governor.update_resources(critical));
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::Pressure)
    ));
    assert!(matches!(
        decode_binary_wal_record_with_work_context(&bytes, &work),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert!(governor.update_resources(normal));
    super::tests::parity(&bytes, &work);
    assert_eq!(available(&task), ceiling);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
