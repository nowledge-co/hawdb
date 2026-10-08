use super::*;
use crate::background::{CheckpointOperationError, CheckpointWorkError};
use crate::projection::ProjectedRelationshipPredicate;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext, Value};
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

fn source() -> (String, ProjectedGraphDefinition) {
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".into()],
        rel_types: vec!["LINKS".into()],
        relationship_predicates: BTreeMap::from([(
            "LINKS".into(),
            ProjectedRelationshipPredicate::Eq {
                property: "body".into(),
                value: Value::Binary((0..257 * 1024 + 3).map(|i| (i % 256) as u8).collect()),
            },
        )]),
    };
    let data =
        ProjectedGraphArtifactData::new(Vec::new(), vec![0], Vec::new(), vec![0], Vec::new())
            .unwrap();
    (
        encode_projected_graph_artifacts(19, 23, [("wide-predicate", &definition, data)]),
        definition,
    )
}

#[test]
fn checkpoint_units_projection_artifact_memory_admits_before_allocating_payload() {
    let (source, _) = source();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let result =
        work.classify(|work| decode_projected_graph_artifacts_with_work_context(&source, work));
    let allocations = observation.finish();
    assert!(matches!(
        result,
        Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
            RuntimeMemoryError::ReservationExceeded { .. }
        )))
    ));
    assert_eq!(
        allocations, 0,
        "artifact denial must precede any unadmitted payload allocation over 64 KiB"
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_projection_artifact_memory_retains_complete_predicate_and_arrays_after_execution(
) {
    let (source, definition) = source();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let (epoch, artifacts) =
        decode_projected_graph_artifacts_with_work_context(&source, &work).unwrap();
    assert_eq!(epoch, 23);
    assert_eq!(artifacts.len(), 1);
    let artifact = artifacts.get("wide-predicate").unwrap();
    assert_eq!(artifact.definition, definition);
    assert_eq!(artifact.projection_epoch, 19);
    assert_eq!(artifact.commit_epoch, 23);
    assert!(artifact.data.nodes.is_empty());
    assert_eq!(artifact.data.csr_offsets, [0]);
    assert_eq!(artifact.data.csc_offsets, [0]);
    assert!(artifact.data.csr_targets.is_empty());
    assert!(artifact.data.csc_sources.is_empty());
    drop(source);
    drop(definition);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    let artifact = artifacts.get("wide-predicate").unwrap();
    let ProjectedRelationshipPredicate::Eq { property, value } = artifact
        .definition
        .relationship_predicates
        .get("LINKS")
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(property, "body");
    let Value::Binary(bytes) = value else {
        unreachable!()
    };
    assert_eq!(bytes.len(), 257 * 1024 + 3);
    assert!(bytes
        .iter()
        .enumerate()
        .all(|(i, byte)| *byte == (i % 256) as u8));
    assert_eq!(artifact.data.csr_offsets, [0]);
    assert_eq!(artifact.data.csc_offsets, [0]);
    drop(artifacts);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
