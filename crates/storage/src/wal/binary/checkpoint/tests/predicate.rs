use super::*;
use crate::projection::ProjectedRelationshipPredicate;

fn entry() -> WalEntry {
    WalEntry {
        lsn: 17,
        op: WalOp::ProjectGraph {
            name: "wide-predicate".into(),
            node_labels: vec!["Memory".into()],
            rel_types: vec!["LINKS".into()],
            relationship_predicates: BTreeMap::from([(
                "LINKS".into(),
                ProjectedRelationshipPredicate::Eq {
                    property: "body".into(),
                    value: Value::Binary((0..257 * 1024 + 3).map(|i| (i % 256) as u8).collect()),
                },
            )]),
        },
    }
}

#[test]
fn checkpoint_units_predicate_admission_counts_before_any_payload_sized_allocation() {
    let entry = entry();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work));
    let large_allocations = observation.finish();
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { .. })
        ))
    ));
    assert_eq!(
        large_allocations, 0,
        "denied payload admission must precede any allocation over the 64 KiB work bound"
    );
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_predicate_admission_charges_emission_scratch_with_retained_output() {
    let entry = entry();
    let expected = encode_binary_wal_record(&entry, 23).unwrap();
    let requested = expected.len() as u64 + 24;
    let ceiling = requested + 64 * 1024 - 1;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result = work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work));
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded {
                requested_bytes: 65560,
                available_bytes: 65535
            })
        ))
    ));
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
