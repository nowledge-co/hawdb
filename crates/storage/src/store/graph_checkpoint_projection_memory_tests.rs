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

fn governor(bytes: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
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
fn fixture() -> (
    Catalog,
    GraphStore,
    ProjectedGraphDefinition,
    ProjectedGraphArtifactData,
) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let nodes = (0..33)
        .map(|_| {
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for i in 0..nodes.len() {
        for _ in 0..2 {
            store
                .create_relationship(
                    &mut catalog,
                    nodes[i],
                    nodes[(i + 1) % nodes.len()],
                    "LINK",
                    BTreeMap::new(),
                )
                .unwrap();
        }
        store
            .create_relationship(&mut catalog, nodes[i], nodes[i], "BACK", BTreeMap::new())
            .unwrap();
    }
    let n = nodes.len();
    let expected = ProjectedGraphArtifactData::new(
        nodes,
        (0..=n).collect(),
        (0..n).map(|i| (i + 1) % n).collect(),
        (0..=n).collect(),
        (0..n).map(|i| (i + n - 1) % n).collect(),
    )
    .unwrap();
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".into()],
        rel_types: vec!["LINK".into()],
        relationship_predicates: BTreeMap::new(),
    };
    (catalog, store, definition, expected)
}
#[test]
fn checkpoint_units_projection_producer_memory_denies_before_large_private_allocation() {
    let (catalog, store, definition, expected) = fixture();
    let nodes = store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let relationships = store
        .relationship_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, work)
    });
    let allocations = observer.finish();
    assert!(matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(
        allocations, 0,
        "one-byte denial must precede large private producer allocation"
    );
    assert_eq!(
        store
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        nodes
    );
    assert_eq!(
        store
            .relationship_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        relationships
    );
    assert_eq!(
        checkpoint_projected_graph_from_definition(
            &catalog,
            &store,
            &definition,
            &CheckpointWorkContext::default()
        )
        .unwrap(),
        expected
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_projection_producer_memory_retains_all_five_array_capacities() {
    let (catalog, store, definition, expected) = fixture();
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let data =
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, &work).unwrap();
    assert_eq!(data, expected);
    let capacity = (data.nodes.capacity() * std::mem::size_of::<NodeId>()
        + (data.csr_offsets.capacity()
            + data.csr_targets.capacity()
            + data.csc_offsets.capacity()
            + data.csc_sources.capacity())
            * std::mem::size_of::<usize>()) as u64;
    assert!(capacity > 1000);
    let used = match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        Ok(lease) => {
            drop(lease);
            0
        }
        Err(error) => panic!("unexpected memory error {error}"),
    };
    assert!(used>=capacity,"every actual final adjacency capacity must be admitted; charged={used}, capacity={capacity}");
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(data, expected);
    drop(data);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
