use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointOperationError};
use crate::projection::ProjectedRelationshipPredicate;
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

fn source() -> BTreeMap<String, ProjectedRelationshipPredicate> {
    BTreeMap::from([(
        "LINKS".into(),
        ProjectedRelationshipPredicate::Eq {
            property: "body".into(),
            value: Value::Binary((0..257 * 1024 + 3).map(|i| (i % 256) as u8).collect()),
        },
    )])
}

#[test]
fn checkpoint_units_predicate_decode_memory_admits_before_allocating_owned_payload() {
    let source = source();
    let encoded = crate::projection::encode_projected_relationship_predicates(&source);
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| {
        let context = DecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::new(CheckpointAllocationOwner::default()),
        };
        predicate::decode(&encoded, &context)
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
        "denied decoded ownership must precede any payload allocation over 64 KiB"
    );
    assert_eq!(
        crate::projection::decode_projected_relationship_predicates(&encoded).unwrap(),
        source
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_predicate_decode_memory_retains_complete_values_after_execution() {
    let source = source();
    let encoded = crate::projection::encode_projected_relationship_predicates(&source);
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let context = DecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::new(CheckpointAllocationOwner::default()),
    };
    let decoded = predicate::decode(&encoded, &context).unwrap();
    assert_eq!(decoded, source);
    let output = crate::wal::checkpoint::CheckpointWalEntry::new(
        WalEntry {
            lsn: 17,
            op: WalOp::ProjectGraph {
                name: "wide-predicate".into(),
                node_labels: vec!["Memory".into()],
                rel_types: vec!["LINKS".into()],
                relationship_predicates: decoded,
            },
        },
        context.memory.into_inner(),
    );
    drop(encoded);
    drop(source);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    let WalOp::ProjectGraph {
        relationship_predicates,
        ..
    } = &output.op
    else {
        unreachable!()
    };
    let ProjectedRelationshipPredicate::Eq { property, value } =
        relationship_predicates.get("LINKS").unwrap()
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
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
