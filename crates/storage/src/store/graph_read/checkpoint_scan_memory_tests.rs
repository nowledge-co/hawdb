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
use crate::background::{CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
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
fn fill(store: &mut GraphStore, catalog: &mut Catalog) {
    for i in 0..3 {
        store
            .create_node(
                catalog,
                "Wide",
                BTreeMap::from([
                    ("id".into(), Value::Int(i)),
                    ("binary".into(), Value::Binary(vec![0xb1; 257 * 1024 + 3])),
                    (
                        "text".into(),
                        Value::String(format!("{}🦀", "界".repeat(87_723))),
                    ),
                ]),
            )
            .unwrap();
    }
}
#[test]
fn checkpoint_units_scan_memory_materialized_capture_does_not_copy_wide_source_records() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    fill(&mut store, &mut catalog);
    let expected = store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(expected.len(), 3);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let scan = store.checkpoint_node_records_owned(&work).unwrap();
    let allocations = observation.finish();
    assert_eq!(
        allocations, 0,
        "capturing borrowed immutable overlays must not clone wide record payloads"
    );
    let mut steps = scan.checkpoint_steps();
    for node in &expected {
        let actual = steps.next().unwrap().unwrap().unwrap();
        assert_eq!(actual.id, node.id);
        assert_eq!(actual.labels, node.labels);
        assert_eq!(actual.properties, node.properties);
    }
    assert!(steps.next().is_none());
    drop(steps);
    store.ensure_usable().unwrap();
    assert_eq!(
        store
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_scan_memory_canonical_denies_before_unadmitted_segment_or_record_buffers() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-scan-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &root,
        &mut catalog,
        DurabilityPolicy::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(8 * 1024 * 1024),
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    fill(&mut store, &mut catalog);
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    assert!(store.nodes.is_empty());
    let expected = store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(expected.len(), 3);
    let identity = store.checkpoint_source_identity();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        let mut steps = store
            .checkpoint_node_records_owned(work)?
            .checkpoint_steps();
        steps.next().transpose()
    });
    let allocations = observation.finish();
    assert!(matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(
        allocations, 0,
        "canonical descriptor/input/record denial must precede unadmitted large allocation"
    );
    assert_eq!(store.checkpoint_source_identity(), identity);
    store.ensure_usable().unwrap();
    assert_eq!(
        store
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
