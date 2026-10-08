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
use crate::background::{
    CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError, CheckpointWorkProbe,
};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::sync::{atomic::Ordering, Arc};

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
        Ok(lease) => {
            drop(lease);
            0
        }
        Err(error) => panic!("unexpected governor failure {error}"),
    }
}
fn fixture(
    n: usize,
) -> (
    Catalog,
    GraphStore,
    ProjectedGraphDefinition,
    ProjectedGraphArtifactData,
) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    let nodes = (0..n)
        .map(|_| {
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for i in 0..n {
        store
            .create_relationship(
                &mut catalog,
                nodes[i],
                nodes[(i + 1) % n],
                "LINK",
                BTreeMap::new(),
            )
            .unwrap();
    }
    let data = ProjectedGraphArtifactData::new(
        nodes,
        (0..=n).collect(),
        (0..n).map(|i| (i + 1) % n).collect(),
        (0..=n).collect(),
        (0..n).map(|i| (i + n - 1) % n).collect(),
    )
    .unwrap();
    (
        catalog,
        store,
        ProjectedGraphDefinition {
            node_labels: vec!["Memory".into()],
            rel_types: vec!["LINK".into()],
            relationship_predicates: BTreeMap::new(),
        },
        data,
    )
}
fn capacity(data: &ProjectedGraphArtifactData) -> u64 {
    (data.nodes.capacity() * std::mem::size_of::<NodeId>()
        + (data.csr_offsets.capacity()
            + data.csr_targets.capacity()
            + data.csc_offsets.capacity()
            + data.csc_sources.capacity())
            * std::mem::size_of::<usize>()) as u64
}
#[test]
fn checkpoint_units_projection_producer_related_wide_capacity_lower_bound_releases_temporary_containers(
) {
    let (catalog, store, definition, expected) = fixture(8193);
    let ceiling = 128 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let data =
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, &work).unwrap();
    assert_eq!(data, expected);
    let bytes = capacity(&data);
    assert!(bytes > 512 * 1024);
    assert!(data.nodes.capacity() * std::mem::size_of::<NodeId>() > 96 * 1024);
    let charged = used(&task, ceiling);
    assert!(
        charged >= bytes,
        "all five wide final capacities must be admitted; charged={charged}, capacity={bytes}"
    );
    assert!(charged<bytes+96*1024,"selector/adjacency scratch must already be destroyed and released; charged={charged}, capacity={bytes}");
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(data, expected);
    drop(data);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_projection_producer_related_denial_retries_on_the_same_admitted_task() {
    let (catalog, store, definition, expected) = fixture(5);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let held = task.reserve_working_memory(ceiling - 64).unwrap();
    let before = used(&task, ceiling);
    let error = work.classify(|work| {
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, work)
    });
    assert!(matches!(
        error,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(used(&task, ceiling), before);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(held);
    assert_eq!(used(&task, ceiling), 0);
    let retry =
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, &work).unwrap();
    assert_eq!(retry, expected);
    assert!(used(&task, ceiling) >= capacity(&retry));
    drop(retry);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_projection_producer_related_every_actual_cpu_unit_cancels_and_retries_complete_arrays(
) {
    let (catalog, store, definition, expected) = fixture(5);
    let nodes = store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let relationships = store
        .relationship_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    let baseline = checkpoint_projected_graph_from_definition(
        &catalog,
        &store,
        &definition,
        &probe.context(local.clone()),
    )
    .unwrap();
    assert_eq!(baseline, expected);
    let total = probe.completed.load(Ordering::SeqCst);
    assert!(total > 100);
    probe.assert_released(&local);
    drop(baseline);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    for cut in 1..=total {
        let cancelled = Arc::new(CheckpointWorkProbe::default());
        cancelled.cancel_after.store(cut, Ordering::SeqCst);
        local.set_telemetry_sink(Some(cancelled.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(cancelled.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        let error =
            checkpoint_projected_graph_from_definition(&catalog, &store, &definition, &work)
                .unwrap_err();
        assert_eq!(
            error.to_string(),
            "storage error: checkpoint build stopped: cancelled",
            "cut={cut}"
        );
        assert_eq!(cancelled.completed.load(Ordering::SeqCst), cut);
        assert_eq!(cancelled.peak_units.load(Ordering::SeqCst), 1);
        cancelled.assert_released(&local);
        assert_eq!(used(&task, ceiling), 0, "cut={cut}");
        assert_eq!(
            governor.snapshot().active_background_io_slots,
            0,
            "cut={cut}"
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
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let retry =
        checkpoint_projected_graph_from_definition(&catalog, &store, &definition, &work).unwrap();
    assert_eq!(retry, expected);
    drop(retry);
    assert_eq!(used(&task, ceiling), 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
