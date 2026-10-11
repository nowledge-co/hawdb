use super::*;
use crate::background::{CheckpointOperationError, CheckpointWorkError, CheckpointWorkProbe};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext, Value};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    WorkClass, WorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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

fn local() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
    match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => ceiling - available_bytes,
        _ => panic!("the exact-ceiling probe must read the real governor ledger"),
    }
}

fn definition(bytes: usize) -> ProjectedGraphDefinition {
    ProjectedGraphDefinition {
        node_labels: vec!["Memory".into(), "中文".into()],
        rel_types: vec!["LINKS".into()],
        relationship_predicates: BTreeMap::from([(
            "LINKS".into(),
            crate::projection::ProjectedRelationshipPredicate::Eq {
                property: "body".into(),
                value: Value::Binary((0..bytes).map(|i| (i % 256) as u8).collect()),
            },
        )]),
    }
}

fn cycle(nodes: usize) -> ProjectedGraphArtifactData {
    ProjectedGraphArtifactData::new(
        (0..nodes).map(|id| NodeId(id as u64 + 1)).collect(),
        (0..=nodes).collect(),
        (0..nodes).map(|id| (id + 1) % nodes).collect(),
        (0..=nodes).collect(),
        (0..nodes).map(|id| (id + nodes - 1) % nodes).collect(),
    )
    .unwrap()
}

fn array_bytes(data: &ProjectedGraphArtifactData) -> usize {
    data.nodes.capacity() * std::mem::size_of::<NodeId>()
        + (data.csr_offsets.capacity()
            + data.csr_targets.capacity()
            + data.csc_offsets.capacity()
            + data.csc_sources.capacity())
            * std::mem::size_of::<usize>()
}

#[test]
fn checkpoint_units_projection_artifact_related_empty_roots_allocate_nothing() {
    let source = encode_projected_graph_artifacts(19, 23, []);
    let (epoch, artifacts) = decode(&source, &CheckpointWorkContext::default()).unwrap();
    assert_eq!(epoch, 23);
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start_all();
    let root = artifacts.into_root(&work).unwrap();
    let empty = CheckpointProjectedGraphRoot::empty(&work).unwrap();
    let snapshot = root.clone();
    assert!(root.get("missing").is_none());
    assert_eq!(root.values().count(), 0);
    assert!(empty.get("missing").is_none());
    assert_eq!(snapshot.values().count(), 0);
    assert_eq!(
        observation.finish(),
        0,
        "empty root preparation and cloning must not create any heap allocation"
    );
    assert_eq!(used(&task, 1), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(root);
    drop(empty);
    drop(snapshot);
}

#[test]
fn checkpoint_units_projection_artifact_related_arrays_and_root_charge_actual_capacities() {
    let expected = cycle(8193);
    let definition = definition(0);
    let source = encode_projected_graph_artifacts(19, 23, [("g", &definition, expected.clone())]);
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let (epoch, artifacts) = decode(&source, &work).unwrap();
    assert_eq!(epoch, 23);
    let data = &artifacts.get("g").unwrap().data;
    assert_eq!(*data, expected);
    let actual_bytes = array_bytes(data);
    assert!(actual_bytes > 5 * 64 * 1024);
    assert!(
        used(&task, ceiling) >= actual_bytes as u64,
        "every retained array capacity needs its own admission"
    );
    let before_root = used(&task, ceiling);
    let root = artifacts.into_root(&work).unwrap();
    let root_bytes = std::mem::size_of::<BTreeMap<String, ProjectedGraphArtifact>>()
        + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
    assert!(
        used(&task, ceiling) >= before_root + root_bytes as u64,
        "root cell is admitted off gate"
    );
    let snapshot = root.clone();
    assert!(root.data.shares_storage_with(&snapshot.data));
    assert_eq!(root.get("g").unwrap().data, expected);
    drop(root);
    assert!(used(&task, ceiling) >= actual_bytes as u64);
    drop(snapshot);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_artifact_related_removed_result_retains_inventory() {
    let definition = definition(257 * 1024 + 3);
    let data = cycle(17);
    let source = encode_projected_graph_artifacts(19, 23, [("g", &definition, data.clone())]);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let (_, mut artifacts) = decode(&source, &work).unwrap();
    let removed = artifacts.remove("g").unwrap();
    assert!(artifacts.is_empty());
    drop(artifacts);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(removed.definition, definition);
    assert_eq!(removed.data, data);
    drop(removed);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_artifact_related_root_and_invalidation_deny_one_byte_and_fully_retry(
) {
    let definition = definition(513);
    let data = cycle(17);
    let source = encode_projected_graph_artifacts(19, 23, [("g", &definition, data.clone())]);
    let denied = governor(1);
    let permit = denied
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let (_, artifacts) = decode(&source, &CheckpointWorkContext::default()).unwrap();
    assert!(matches!(
        work.classify(|work| artifacts.into_root(work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(used(&task, 1), 0);
    let (_, artifacts) = decode(&source, &CheckpointWorkContext::default()).unwrap();
    let mut root = artifacts
        .into_root(&CheckpointWorkContext::default())
        .unwrap();
    let snapshot = root.clone();
    assert!(matches!(
        work.classify(|work| root.invalidate("g", work)),
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(root.get("g").unwrap().definition, definition);
    assert_eq!(root.get("g").unwrap().data, data);
    assert!(root.data.shares_storage_with(&snapshot.data));
    assert_eq!(used(&task, 1), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(denied.snapshot().admitted_memory_bytes, 0);
    let ceiling = 64 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    root.invalidate("g", &work).unwrap();
    assert!(root.get("g").is_none());
    assert_eq!(snapshot.get("g").unwrap().data, data);
    assert!(used(&task, ceiling) > 0);
    drop(root);
    assert_eq!(used(&task, ceiling), 0);
    let (_, artifacts) = decode(&source, &CheckpointWorkContext::default()).unwrap();
    let root = artifacts.into_root(&work).unwrap();
    assert_eq!(root.get("g").unwrap().definition, definition);
    assert_eq!(root.get("g").unwrap().data, data);
    assert!(used(&task, ceiling) > 0);
    drop(root);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_artifact_related_invalidation_keeps_snapshot_and_payload_admission()
{
    let definition = definition(257 * 1024 + 3);
    let data = cycle(17);
    let source = encode_projected_graph_artifacts(
        19,
        23,
        [
            ("g", &definition, data.clone()),
            ("other", &definition, data.clone()),
        ],
    );
    let source_ceiling = 32 * 1024 * 1024;
    let source_governor = governor(source_ceiling);
    let source_permit = source_governor
        .try_admit(RuntimeWorkRequest::background_maintenance(source_ceiling))
        .unwrap();
    let source_work =
        CheckpointWorkContext::new(source_permit.bind_task_context(RuntimeTaskContext::default()));
    let (_, artifacts) = decode(&source, &source_work).unwrap();
    let mut root = artifacts.into_root(&source_work).unwrap();
    let snapshot = root.clone();
    let other = std::ptr::from_ref(root.get("other").unwrap());
    drop(source_work);
    drop(source_permit);
    let mutation_ceiling = 64 * 1024;
    let mutation_governor = governor(mutation_ceiling);
    let mutation_permit = mutation_governor
        .try_admit(RuntimeWorkRequest::background_maintenance(mutation_ceiling))
        .unwrap();
    let task = mutation_permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let observation = crate::test_allocator::AllocationObservation::start();
    root.invalidate("g", &work).unwrap();
    assert_eq!(
        observation.finish(),
        0,
        "invalidation must share the wide payloads"
    );
    assert!(root.data.shares_storage_with(&snapshot.data));
    assert!(root.get("g").is_none());
    assert_eq!(root.values().count(), 1);
    assert_eq!(std::ptr::from_ref(root.get("other").unwrap()), other);
    assert_eq!(snapshot.get("g").unwrap().definition, definition);
    assert_eq!(snapshot.values().count(), 2);
    let mutation_usage = used(&task, mutation_ceiling);
    assert!(mutation_usage > 0, "invalidation metadata must be admitted");
    root.invalidate("g", &work).unwrap();
    root.invalidate("missing", &work).unwrap();
    assert_eq!(used(&task, mutation_ceiling), mutation_usage);
    drop(work);
    drop(task);
    drop(mutation_permit);
    assert_eq!(
        mutation_governor.snapshot().admitted_memory_bytes,
        mutation_ceiling
    );
    assert_eq!(
        source_governor.snapshot().admitted_memory_bytes,
        source_ceiling
    );
    drop(root);
    assert_eq!(mutation_governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(
        source_governor.snapshot().admitted_memory_bytes,
        source_ceiling
    );
    assert_eq!(snapshot.get("g").unwrap().data, data);
    drop(snapshot);
    assert_eq!(source_governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_artifact_related_invalidation_cancels_every_unit_without_mutating_source(
) {
    let definition = definition(0);
    let data = cycle(1);
    let source = encode_projected_graph_artifacts(
        19,
        23,
        (0..129)
            .map(|_| data.clone())
            .enumerate()
            .map(|(id, data)| (format!("g{id:03}"), data))
            .collect::<Vec<_>>()
            .iter()
            .map(|(name, data)| (name.as_str(), &definition, data.clone())),
    );
    let (_, artifacts) = decode(&source, &CheckpointWorkContext::default()).unwrap();
    let mut original = artifacts
        .into_root(&CheckpointWorkContext::default())
        .unwrap();
    for id in (0..128).step_by(2) {
        original
            .invalidate(&format!("g{id:03}"), &CheckpointWorkContext::default())
            .unwrap();
    }
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let baseline = probe.context(local.clone());
    let mut complete = original.clone();
    complete.invalidate("g128", &baseline).unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units > 3);
    assert_eq!(complete.values().count(), 64);
    drop(baseline);
    probe.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 64 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        let held = (stop == 0).then(|| {
            local
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap()
        });
        let mut root = original.clone();
        let result = work.classify(|work| root.invalidate("g128", work));
        assert!(
            matches!(result, Err(CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop == 0)
                || matches!(result, Err(CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop > 0)
        );
        assert_eq!(root.values().count(), 65);
        assert_eq!(root.get("g128").unwrap().data, data);
        assert!(root.data.shares_storage_with(&original.data));
        assert_eq!(used(&task, ceiling), 0);
        drop(held);
        drop(work);
        probe.assert_released(&local);
        local.set_telemetry_sink(None);
        let retry = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        root.invalidate("g128", &retry).unwrap();
        assert!(root.get("g128").is_none());
        assert_eq!(root.values().count(), 64);
        assert!(original.get("g128").is_some());
        drop(root);
        assert_eq!(used(&task, ceiling), 0);
        drop(retry);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn checkpoint_units_projection_artifact_related_decode_cancels_every_unit_and_fully_retries() {
    let definition = definition(513);
    let data = cycle(17);
    let source = encode_projected_graph_artifacts(19, 23, [("g", &definition, data.clone())]);
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let baseline = probe.context(local.clone());
    let (_, artifacts) = decode(&source, &baseline).unwrap();
    let root = artifacts.into_root(&baseline).unwrap();
    assert_eq!(root.get("g").unwrap().data, data);
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units > 100);
    drop(root);
    drop(baseline);
    probe.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 4 * 1024 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let work = CheckpointWorkContext::new(permit.bind_task_context(
            RuntimeTaskContext::without_deadline(probe.cancellation.clone()),
        ))
        .with_scheduler(local.clone());
        let held = (stop == 0).then(|| {
            local
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap()
        });
        let result = work.classify(|work| {
            decode(&source, work).and_then(|(_, artifacts)| artifacts.into_root(work))
        });
        assert!(
            matches!(result, Err(CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop == 0)
                || matches!(result, Err(CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop > 0)
        );
        assert_eq!(used(&task, ceiling), 0);
        drop(held);
        drop(work);
        probe.assert_released(&local);
        local.set_telemetry_sink(None);
        let retry = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let (epoch, artifacts) = decode(&source, &retry).unwrap();
        assert_eq!(epoch, 23);
        let root = artifacts.into_root(&retry).unwrap();
        assert_eq!(root.get("g").unwrap().definition, definition);
        assert_eq!(root.get("g").unwrap().data, data);
        drop(root);
        assert_eq!(used(&task, ceiling), 0);
        drop(retry);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
