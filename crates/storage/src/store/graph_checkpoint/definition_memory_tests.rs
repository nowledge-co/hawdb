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
use crate::background::{CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkError};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
};

const OWNER: u64 = 4096;

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

#[test]
fn relationship_definition_denies_before_cloning_large_property() {
    let property = "索引".repeat(16 * 1024);
    let governor = governor();
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, 4096, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        let memory = CheckpointDecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::default(),
        };
        let mut definitions = Vec::new();
        let mut limits = PersistentPropertyProjectionDefinitionAdmission::new(Default::default());
        push_relationship_property_projection_definitions(
            &mut definitions,
            &mut limits,
            RelTypeId(4),
            &property,
            &memory,
        )
    });
    let allocations = observer.finish();
    assert!(
        matches!(
            result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                _
            )))
        ),
        "property string must be admitted before cloning"
    );
    assert_eq!(allocations, 0, "denial precedes large backing allocations");
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn composite_definition_denies_before_identity_or_hex_scratch_allocation() {
    let properties = vec!["索引".repeat(16 * 1024), "rank".into()];
    let governor = governor();
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, 4096, 1, RuntimeTaskContext::default())
        .unwrap();
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone());
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        let memory = CheckpointDecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::default(),
        };
        let result =
            crate::property_projection::persistent_composite_property_identity_with_work_context(
                &properties,
                &memory,
            );
        assert!(
            matches!(
                result,
                Err(
                    crate::property_projection::PersistentPropertyProjectionError::Work(
                        CheckpointWorkError::Memory(_)
                    )
                )
            ),
            "composite identity must be admitted before encoding"
        );
        result.map(drop)
    });
    let allocations = observer.finish();
    assert!(
        matches!(
            result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                _
            )))
        ),
        "composite identity must be admitted before encoding"
    );
    assert_eq!(allocations, 0, "denial precedes identity and hex scratch");
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn definition_strings_vector_and_shared_composite_grammar_keep_their_leases() {
    let properties = vec!["索引".repeat(12 * 1024), "rank:with:separators".into()];
    let expected = format!(
        "hawdb-composite-property-v1:{}:{}",
        properties[0]
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        properties[1]
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
    );
    assert_eq!(
        crate::property_projection::persistent_composite_property_identity(&properties).unwrap(),
        expected,
    );
    let governor = governor();
    let mut admission = governor
        .try_admit_incremental_maintenance(OWNER, 2 * 1024 * 1024, 1, RuntimeTaskContext::default())
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        ..Default::default()
    });
    let work =
        crate::background::CheckpointWorkContext::new(admission.task_context().unwrap().clone())
            .with_scheduler(scheduler.clone());
    let memory = CheckpointDecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::default(),
    };
    let mut definitions = Vec::new();
    let identity =
        crate::property_projection::persistent_composite_property_identity_with_work_context(
            &properties,
            &memory,
        )
        .unwrap();
    assert_eq!(identity, expected);
    let mut limits = PersistentPropertyProjectionDefinitionAdmission::new(Default::default());
    push_property_projection_definition(
        &mut definitions,
        &mut limits,
        PersistentPropertyProjectionDefinition {
            label_id: LabelId(3),
            property: identity,
            kind: PersistentPropertyProjectionKind::CompositeEquality,
            complete: false,
        },
        &memory,
    )
    .unwrap();
    push_relationship_property_projection_definitions(
        &mut definitions,
        &mut limits,
        RelTypeId(4),
        &properties[0],
        &memory,
    )
    .unwrap();
    assert_eq!(definitions.len(), 3);
    assert_eq!(definitions[1].property, properties[0]);
    assert_eq!(definitions[2].property, properties[0]);
    let capacity = definitions.capacity()
        * std::mem::size_of::<PersistentPropertyProjectionDefinition>()
        + definitions
            .iter()
            .map(|definition| definition.property.capacity())
            .sum::<usize>();
    assert!(admission.memory_report().live_accounted_bytes >= capacity as u64);
    admission.pause();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(governor.snapshot().admitted_memory_bytes >= OWNER + capacity as u64);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(definitions[0].property, expected);
    drop(definitions);
    drop(memory);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
