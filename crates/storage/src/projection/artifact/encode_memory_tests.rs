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
use crate::background::{CheckpointOperationError, CheckpointWorkError};
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

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
fn source() -> (String, ProjectedGraphDefinition, ProjectedGraphArtifactData) {
    (
        format!("{}\0中文🦀", "n".repeat(257 * 1024 + 3)),
        ProjectedGraphDefinition {
            node_labels: vec!["Memory".into()],
            rel_types: vec!["LINKS".into()],
            relationship_predicates: BTreeMap::new(),
        },
        ProjectedGraphArtifactData::new(Vec::new(), vec![0], Vec::new(), vec![0], Vec::new())
            .unwrap(),
    )
}
#[test]
fn checkpoint_units_projection_artifact_encode_memory_denial_precedes_large_name_output() {
    let (name, definition, data) = source();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        encode_projected_graph_artifacts_with_work_context(
            19,
            23,
            [Ok((name.as_str(), &definition, data))],
            work,
        )
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
        "complete body admission must precede any large name-output allocation"
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
#[test]
fn checkpoint_units_projection_artifact_encode_memory_retains_full_body_after_execution() {
    let (name, definition, data) = source();
    let expected =
        encode_projected_graph_artifacts(19, 23, [(name.as_str(), &definition, data.clone())]);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let encoded = encode_projected_graph_artifacts_with_work_context(
        19,
        23,
        [Ok((name.as_str(), &definition, data))],
        &work,
    )
    .unwrap();
    assert_eq!(&*encoded, expected.as_str());
    assert!(encoded.len() > 512 * 1024);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(&*encoded, expected.as_str());
    let (epoch, decoded) = decode_projected_graph_artifacts(&encoded).unwrap();
    assert_eq!(epoch, 23);
    assert_eq!(decoded.len(), 1);
    let artifact = decoded.get(&name).unwrap();
    assert_eq!(artifact.definition, definition);
    assert_eq!(artifact.projection_epoch, 19);
    assert_eq!(artifact.commit_epoch, 23);
    assert!(artifact.data.nodes.is_empty());
    assert_eq!(artifact.data.csr_offsets, [0]);
    assert_eq!(artifact.data.csc_offsets, [0]);
    assert!(artifact.data.csr_targets.is_empty());
    assert!(artifact.data.csc_sources.is_empty());
    drop(encoded);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
