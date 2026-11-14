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
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimePermit, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

fn fixture() -> (RuntimeGovernor, RuntimePermit, CheckpointWorkContext) {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(10_000),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(10_000))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    (governor, permit, work)
}

fn available(work: &CheckpointWorkContext) -> u64 {
    match work.task_context().reserve_working_memory(10_000) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("probe must expose the charged controller remainder"),
    }
}

fn buffer(
    owner: &mut CheckpointAllocationOwner,
    bytes: usize,
    work: &CheckpointWorkContext,
) -> Vec<u8> {
    let token = owner.reserve(bytes, work).unwrap();
    let data = vec![0; bytes];
    token.address(data.as_ptr() as usize);
    data
}

#[test]
fn retained_replay_consolidates_nodes_without_refunding_live_backing() {
    let (governor, permit, work) = fixture();
    let mut target = CheckpointAllocationOwner::default();
    let old = buffer(&mut target, 8, &work);
    let mut incoming = CheckpointAllocationOwner::default();
    let first = buffer(&mut incoming, 16, &work);
    let second = buffer(&mut incoming, 32, &work);
    let before = available(&work);
    target.append_retained(&mut incoming, &work).unwrap();
    assert!(incoming.is_empty());
    let head = target.head.as_ref().unwrap();
    assert!(head.next.is_none());
    assert_eq!(head.buffer.lock().unwrap().as_ref().unwrap().bytes(), 56);
    assert!(available(&work) > before);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(permit);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 10_000);
    assert_eq!(first.len() + second.len() + old.len(), 56);
    drop((first, second, old));
    drop(target);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_transfer_never_mutates_a_shared_snapshot_suffix() {
    let (governor, permit, work) = fixture();
    let mut target = CheckpointAllocationOwner::default();
    let old = buffer(&mut target, 8, &work);
    let snapshot = target.clone();
    let snapshot_head = snapshot.head.as_ref().unwrap().clone();
    let mut incoming = CheckpointAllocationOwner::default();
    let first = buffer(&mut incoming, 16, &work);
    let second = buffer(&mut incoming, 32, &work);
    target.append_retained(&mut incoming, &work).unwrap();
    assert!(incoming.is_empty());
    assert!(Arc::ptr_eq(
        target.head.as_ref().unwrap().next.as_ref().unwrap(),
        &snapshot_head
    ));
    assert_eq!(
        snapshot_head
            .buffer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .bytes(),
        8
    );
    assert_eq!(
        target
            .head
            .as_ref()
            .unwrap()
            .buffer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .bytes(),
        48
    );
    drop((first, second));
    drop(target);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 10_000);
    assert_eq!(old.len(), 8);
    drop(old);
    drop((snapshot, snapshot_head, permit));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn cancellation_before_retained_transfer_preserves_both_owners_and_charges() {
    let (governor, permit, work) = fixture();
    let mut target = CheckpointAllocationOwner::default();
    let old = buffer(&mut target, 8, &work);
    let mut incoming = CheckpointAllocationOwner::default();
    let first = buffer(&mut incoming, 16, &work);
    let second = buffer(&mut incoming, 32, &work);
    let before = available(&work);
    let target_head = Arc::as_ptr(target.head.as_ref().unwrap());
    let incoming_head = Arc::as_ptr(incoming.head.as_ref().unwrap());
    let cancelled = CheckpointWorkContext::new(work.task_context().child());
    cancelled.task_context().cancellation().cancel();
    assert!(target.append_retained(&mut incoming, &cancelled).is_err());
    assert_eq!(Arc::as_ptr(target.head.as_ref().unwrap()), target_head);
    assert_eq!(Arc::as_ptr(incoming.head.as_ref().unwrap()), incoming_head);
    assert_eq!(available(&work), before);
    drop((old, first, second, target, incoming, permit));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn unsupported_consolidation_keeps_inventory_and_does_not_renew_stall_progress() {
    #[derive(Debug)]
    struct Controller;
    #[derive(Debug)]
    struct Lease(u64);
    impl RuntimeMemoryPermit for Lease {
        fn bytes(&self) -> u64 {
            self.0
        }
    }
    impl hawdb_core::RuntimeMemoryController for Controller {
        fn reserve(
            &self,
            bytes: u64,
            _ceiling: u64,
        ) -> Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
            Ok(Box::new(Lease(bytes)))
        }
    }
    let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let work = CheckpointWorkContext::new(
        RuntimeTaskContext::default().with_memory_controller(Arc::new(Controller)),
    )
    .with_progress_counter(progress.clone());
    let mut incoming = CheckpointAllocationOwner::default();
    let first = buffer(&mut incoming, 16, &work);
    let second = buffer(&mut incoming, 32, &work);
    let mut target = CheckpointAllocationOwner::default();
    target.append_retained(&mut incoming, &work).unwrap();
    assert!(incoming.is_empty());
    // Only transferring the two retained nodes advances useful work. The
    // unsupported merge probe must not extend the five-second stall deadline.
    assert_eq!(progress.load(Ordering::Relaxed), 2);
    assert!(target
        .find(first.as_ptr() as usize, &work)
        .unwrap()
        .0
        .is_some());
    assert!(target
        .find(second.as_ptr() as usize, &work)
        .unwrap()
        .0
        .is_some());
    drop((first, second, target));
}

#[test]
fn incremental_replay_consolidates_real_governor_charges_and_preserves_backing() {
    let (governor, unused_permit, unused_work) = fixture();
    drop((unused_permit, unused_work));
    let admission = governor
        .try_admit_incremental_maintenance(128, 10_000, 1, RuntimeTaskContext::default())
        .unwrap();
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let mut target = CheckpointAllocationOwner::default();
    let old = buffer(&mut target, 8, &work);
    let mut incoming = CheckpointAllocationOwner::default();
    let first = buffer(&mut incoming, 16, &work);
    let second = buffer(&mut incoming, 32, &work);
    let before = governor.snapshot().admitted_memory_bytes;
    target.append_retained(&mut incoming, &work).unwrap();
    assert!(incoming.is_empty());
    let head = target.head.as_ref().unwrap();
    assert!(head.next.is_none());
    assert_eq!(head.buffer.lock().unwrap().as_ref().unwrap().bytes(), 56);
    assert!(governor.snapshot().admitted_memory_bytes < before);
    assert!(governor.snapshot().admitted_memory_bytes > 128 + 56);
    drop(admission);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(old.len() + first.len() + second.len(), 56);
    drop((old, first, second, target));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 128);
    drop(work);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_head_compaction_is_one_step_and_keeps_all_backing_charged() {
    let (governor, permit, work) = fixture();
    let mut owner = CheckpointAllocationOwner::default();
    let first = buffer(&mut owner, 8, &work);
    let second = buffer(&mut owner, 16, &work);
    let third = buffer(&mut owner, 32, &work);
    let before = available(&work);
    assert!(owner.coalesce_retained_head());
    let head = owner.head.as_ref().unwrap();
    assert_eq!(head.buffer.lock().unwrap().as_ref().unwrap().bytes(), 48);
    assert!(head.next.is_some());
    assert!(available(&work) > before);
    assert!(owner.coalesce_retained_head());
    let head = owner.head.as_ref().unwrap();
    assert_eq!(head.buffer.lock().unwrap().as_ref().unwrap().bytes(), 56);
    assert!(head.next.is_none());
    assert!(!owner.coalesce_retained_head());
    assert_eq!(first.len() + second.len() + third.len(), 56);
    drop((first, second, third, owner, permit));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn retained_head_compaction_leaves_shared_head_and_suffix_unchanged() {
    let (governor, permit, work) = fixture();
    let mut owner = CheckpointAllocationOwner::default();
    let first = buffer(&mut owner, 8, &work);
    let second = buffer(&mut owner, 16, &work);
    let snapshot = owner.clone();
    let before = available(&work);
    assert!(!owner.coalesce_retained_head());
    assert_eq!(available(&work), before);
    drop(snapshot);
    let suffix = owner.head.as_ref().unwrap().next.as_ref().unwrap().clone();
    assert!(!owner.coalesce_retained_head());
    assert_eq!(available(&work), before);
    assert_eq!(
        owner
            .head
            .as_ref()
            .unwrap()
            .buffer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .bytes(),
        16
    );
    drop(suffix);
    assert!(owner.coalesce_retained_head());
    assert_eq!(
        owner
            .head
            .as_ref()
            .unwrap()
            .buffer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .bytes(),
        24
    );
    drop((first, second, owner, permit));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
