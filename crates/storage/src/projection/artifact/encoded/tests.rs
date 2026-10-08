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
use crate::background::{CheckpointOperationError, CheckpointWorkProbe};
use crate::projection::artifact::{
    decode_projected_graph_artifacts, encode_projected_graph_artifacts,
    encode_projected_graph_artifacts_with_work_context,
};
use crate::projection::{
    ProjectedGraphArtifactData, ProjectedGraphDefinition, ProjectedRelationshipPredicate,
};
use crate::NodeId;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext, Value};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::collections::BTreeMap;
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
        _ => panic!("read actual governor working-memory admission"),
    }
}

fn source() -> (String, ProjectedGraphDefinition, ProjectedGraphArtifactData) {
    let nodes = 33usize;
    (
        format!("{}\0中文🦀", "n".repeat(257 * 1024 + 3)),
        ProjectedGraphDefinition {
            node_labels: vec!["Memory-界".into(), String::new()],
            rel_types: vec!["LINKS".into()],
            relationship_predicates: BTreeMap::from([(
                "LINKS".into(),
                ProjectedRelationshipPredicate::Eq {
                    property: "payload".into(),
                    value: Value::Binary(vec![0x9f; 4097]),
                },
            )]),
        },
        ProjectedGraphArtifactData::new(
            (1..=nodes as u64).map(NodeId).collect(),
            (0..=nodes).collect(),
            (0..nodes).map(|i| (i + 1) % nodes).collect(),
            (0..=nodes).collect(),
            (0..nodes).map(|i| (i + nodes - 1) % nodes).collect(),
        )
        .unwrap(),
    )
}

#[test]
fn checkpoint_units_projection_encoded_capacity_remains_charged_without_growth_debt() {
    let (name, definition, data) = source();
    let expected = encode_projected_graph_artifacts(19, 23, [(&*name, &definition, data.clone())]);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let encoded = encode_projected_graph_artifacts_with_work_context(
        19,
        23,
        [Ok((&*name, &definition, data.clone()))],
        &work,
    )
    .unwrap();
    assert_eq!(encoded, expected);
    let capacity = encoded.text.capacity() as u64;
    assert!(capacity > 512 * 1024);
    let charged = used(&task, ceiling);
    assert!(
        charged >= capacity,
        "actual complete text capacity must be admitted"
    );
    assert!(
        charged < capacity + 256,
        "old growth buffers and predicate scratch must already be released"
    );
    let (epoch, decoded) = decode_projected_graph_artifacts(&encoded).unwrap();
    assert_eq!(epoch, 23);
    assert_eq!(decoded[&name].definition, definition);
    assert_eq!(decoded[&name].data, data);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(encoded, expected);
    drop(encoded);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_encoded_growth_admits_overlap_before_allocating_and_retries() {
    let ceiling = 600 * 1024;
    let prefix = "p".repeat(257 * 1024);
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut body = CheckpointProjectedGraphText::new();
    body.append(&prefix, &work).unwrap();
    let capacity = body.text.capacity();
    assert_eq!(capacity, prefix.len());
    let before = used(&task, ceiling);
    let observation = crate::test_allocator::AllocationObservation::start();
    let denied = work.classify(|work| body.append("x", work));
    let allocations = observation.finish();
    assert!(matches!(
        denied,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(
        allocations, 0,
        "overlap denial must precede allocating a replacement body"
    );
    assert_eq!(&*body, prefix);
    assert_eq!(body.text.capacity(), capacity);
    assert_eq!(used(&task, ceiling), before);
    drop(body);
    assert_eq!(used(&task, ceiling), 0);
    let expected = format!("{prefix}x");
    let mut retry = CheckpointProjectedGraphText::new();
    retry.append(&expected, &work).unwrap();
    assert_eq!(&*retry, expected);
    assert!(used(&task, ceiling) >= retry.text.capacity() as u64);
    drop(retry);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_encoded_every_actual_unit_cancels_and_retries_identical_bytes() {
    let (name, definition, data) = source();
    let expected = encode_projected_graph_artifacts(19, 23, [(&*name, &definition, data.clone())]);
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let encoded = encode_projected_graph_artifacts_with_work_context(
        19,
        23,
        [Ok((&*name, &definition, data.clone()))],
        &probe.context(local.clone()),
    )
    .unwrap();
    assert_eq!(encoded, expected);
    let total = probe.completed.load(Ordering::SeqCst);
    assert!(total > 32);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&local);
    drop(encoded);
    let ceiling = 8 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
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
        let error = encode_projected_graph_artifacts_with_work_context(
            19,
            23,
            [Ok((&*name, &definition, data.clone()))],
            &work,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "storage error: checkpoint build stopped: cancelled"
        );
        assert_eq!(cancelled.completed.load(Ordering::SeqCst), cut);
        assert_eq!(used(&task, ceiling), 0);
        cancelled.assert_released(&local);
    }
    local.set_telemetry_sink(None);
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let retry = encode_projected_graph_artifacts_with_work_context(
        19,
        23,
        [Ok((&*name, &definition, data.clone()))],
        &work,
    )
    .unwrap();
    assert_eq!(retry, expected);
    let (epoch, decoded) = decode_projected_graph_artifacts(&retry).unwrap();
    assert_eq!(epoch, 23);
    assert_eq!(decoded[&name].definition, definition);
    assert_eq!(decoded[&name].data, data);
    drop(retry);
    assert_eq!(used(&task, ceiling), 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
