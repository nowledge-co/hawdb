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
fn checkpoint_units_predicate_all_value_bytes_match_independent_ordinary_codec() {
    let mut values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::String(String::new()),
        Value::String("\0中文🦀".repeat(8193)),
        Value::Binary(Vec::new()),
        Value::Binary((0..65536 + 7).map(|i| (i % 256) as u8).collect()),
        Value::Uuid(hawdb_core::Uuid::from_bytes([0; 16])),
        Value::Uuid(hawdb_core::Uuid::from_bytes([255; 16])),
        Value::List(Vec::new()),
        Value::Map(BTreeMap::new()),
        Value::List(vec![Value::Null, Value::String("nested".into())]),
        Value::Map(BTreeMap::from([(
            "\0:;=中文".into(),
            Value::List(vec![Value::Int(-1)]),
        )])),
    ];
    values.extend([i64::MIN, -1, 0, 1, i64::MAX].map(Value::Int));
    values.extend(
        [
            0,
            0x8000000000000000,
            0x7ff0000000000000,
            0xfff0000000000000,
            0x7ff8000000000123,
        ]
        .map(|bits| Value::Float(f64::from_bits(bits))),
    );
    let local = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    for value in values {
        let mut entry = entry();
        let WalOp::ProjectGraph {
            relationship_predicates,
            ..
        } = &mut entry.op
        else {
            unreachable!()
        };
        *relationship_predicates = BTreeMap::from([
            (
                "\0Unicode中文".into(),
                ProjectedRelationshipPredicate::And(vec![
                    ProjectedRelationshipPredicate::Eq {
                        property: "eq\0property".into(),
                        value: value.clone(),
                    },
                    ProjectedRelationshipPredicate::And(vec![
                        ProjectedRelationshipPredicate::Gte {
                            property: "gte:;=".into(),
                            value,
                        },
                    ]),
                ]),
            ),
            (
                "second".into(),
                ProjectedRelationshipPredicate::Eq {
                    property: String::new(),
                    value: Value::Null,
                },
            ),
        ]);
        for epoch in [0, 23, u64::MAX] {
            let expected = encode_binary_wal_record(&entry, epoch).unwrap();
            let actual = encode_binary_wal_record_with_work_context(&entry, epoch, &work).unwrap();
            assert_eq!(&*actual, expected);
            assert_eq!(local.snapshot().running_background_operations, 0);
        }
    }
}

#[test]
fn checkpoint_units_predicate_emission_retains_output_and_releases_scratch() {
    let entry = entry();
    let expected = encode_binary_wal_record(&entry, 23).unwrap();
    let output_bytes = expected.len() as u64 + 24;
    let scratch_bytes = 65536 + 24;
    let ceiling = output_bytes + scratch_bytes;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let local = scheduler();
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let output = encode_binary_wal_record_with_work_context(&entry, 23, &work).unwrap();
    assert_eq!(&*output, expected);
    // The full scratch allocation and its lease are free after emission,
    // while every byte in the retained output is still charged.
    let scratch = task.reserve_working_memory(65536).unwrap().unwrap();
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    drop(scratch);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(local.snapshot().running_background_operations, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(&*output, expected);
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_predicate_cancels_every_actual_unit_and_retries_full_same_reservation() {
    let entry = entry();
    let expected = encode_binary_wal_record(&entry, 23).unwrap();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let work = baseline.context(local.clone());
    let output = work
        .classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work))
        .unwrap_or_else(|_| panic!("baseline predicate encoding must complete"));
    assert_eq!(&*output, expected);
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 50);
    drop(output);
    drop(work);
    baseline.assert_released(&local);
    for stop in 0..=units {
        let ceiling = expected.len() as u64 + 24 + 65536 + 24;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
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
        let result =
            work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work));
        assert!(
            matches!(result,Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop==0)
                || matches!(result,Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop>0)
        );
        drop(held);
        drop(work);
        probe.assert_released(&local);
        local.set_telemetry_sink(None);
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        assert!(
            matches!(task.reserve_working_memory(ceiling),Err(RuntimeMemoryError::ReservationExceeded {available_bytes,..}) if available_bytes==ceiling)
        );
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let output = encode_binary_wal_record_with_work_context(&entry, 23, &work).unwrap();
        assert_eq!(&*output, expected);
        assert_eq!(encode_binary_wal_record(&entry, 23).unwrap(), expected);
        drop(output);
        assert!(
            matches!(task.reserve_working_memory(ceiling),Err(RuntimeMemoryError::ReservationExceeded {available_bytes,..}) if available_bytes==ceiling)
        );
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(local.snapshot().running_background_operations, 0);
    }
}

#[test]
fn checkpoint_units_predicate_count_rejects_unrepresentable_text_without_allocating_payload() {
    let mut item = ProjectedRelationshipPredicate::Eq {
        property: "body".into(),
        value: Value::Null,
    };
    for _ in 0..80 {
        item = ProjectedRelationshipPredicate::And(vec![item]);
    }
    let mut entry = entry();
    let WalOp::ProjectGraph {
        relationship_predicates,
        ..
    } = &mut entry.op
    else {
        unreachable!()
    };
    *relationship_predicates = BTreeMap::from([("LINKS".into(), item)]);
    let work = CheckpointWorkContext::default().with_scheduler(scheduler());
    let observation = crate::test_allocator::AllocationObservation::start();
    let result = work.classify(|work| encode_binary_wal_record_with_work_context(&entry, 23, work));
    let allocations = observation.finish();
    assert!(matches!(
        result,
        Err(crate::background::CheckpointOperationError::Work(
            CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                ..
            }
        ))
    ));
    assert_eq!(allocations, 0);
}
