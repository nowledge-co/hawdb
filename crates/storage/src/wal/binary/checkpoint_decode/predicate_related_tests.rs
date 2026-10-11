use super::*;
use crate::background::{CheckpointOperationError, CheckpointWorkProbe};
use crate::projection::ProjectedRelationshipPredicate;
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    WorkClass, WorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;

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
fn source() -> BTreeMap<String, ProjectedRelationshipPredicate> {
    BTreeMap::from([(
        "LINKS".into(),
        ProjectedRelationshipPredicate::Eq {
            property: "body".into(),
            value: Value::Binary((0..257 * 1024 + 3).map(|i| (i % 256) as u8).collect()),
        },
    )])
}
fn entry(predicates: BTreeMap<String, ProjectedRelationshipPredicate>) -> WalEntry {
    WalEntry {
        lsn: 17,
        op: WalOp::ProjectGraph {
            name: "wide-predicate".into(),
            node_labels: vec!["Memory".into()],
            rel_types: vec!["LINKS".into()],
            relationship_predicates: predicates,
        },
    }
}
fn decode_owned(
    encoded: &str,
    work: &CheckpointWorkContext,
) -> Result<crate::wal::checkpoint::CheckpointWalEntry> {
    let context = DecodeContext {
        work: work.clone(),
        memory: std::cell::RefCell::new(Default::default()),
    };
    let result = predicate::decode(encoded, &context)?;
    Ok(crate::wal::checkpoint::CheckpointWalEntry::new(
        entry(result),
        context.memory.into_inner(),
    ))
}

#[test]
fn checkpoint_units_predicate_decode_related_all_values_match_ordinary_and_actual_record() {
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
    let local = local();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    for value in values {
        let source = BTreeMap::from([
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
        let encoded = crate::projection::encode_projected_relationship_predicates(&source);
        let output = decode_owned(&encoded, &work).unwrap();
        let WalOp::ProjectGraph {
            relationship_predicates,
            ..
        } = &output.op
        else {
            unreachable!()
        };
        assert_eq!(
            *relationship_predicates,
            crate::projection::decode_projected_relationship_predicates(&encoded).unwrap()
        );
        let bytes = encode_binary_wal_record(&entry(source), 23).unwrap();
        let BinaryWalRecordDecode::Entry {
            entry: decoded,
            commit_epoch,
        } = decode_binary_wal_record_with_work_context(&bytes, &work).unwrap()
        else {
            panic!("complete ordinary record must decode")
        };
        assert_eq!(commit_epoch, 23);
        assert_eq!(encode_binary_wal_record(&decoded, 23).unwrap(), bytes);
        assert_eq!(local.snapshot().running_background_operations, 0);
    }
}

fn leaf_text(leaf: &str) -> String {
    let value = format!(
        "l{},{},{}",
        crate::text::encode_string("s6571"),
        crate::text::encode_string("s626f6479"),
        crate::text::encode_string(leaf)
    );
    format!("m4c494e4b53={}", crate::text::encode_string(&value))
}

#[test]
fn checkpoint_units_predicate_decode_related_preserves_legacy_tolerance_and_diagnostics() {
    let mut cases = [
        "",
        "m",
        "n",
        "l",
        "b0",
        "s",
        "中",
        "m1=00",
        "m00=1",
        "m00=ff",
        "m00=e4b8",
        "m00=",
        "m00=6e",
        "m00=6c",
        "m+1=6c",
        "m00=6c36323330",
        "m00=6c3733",
        "m00=6c3733363136653634",
    ]
    .map(str::to_owned)
    .to_vec();
    let values = vec![
        Value::Null,
        Value::List(Vec::new()),
        Value::List(vec![Value::Int(0)]),
        Value::List(vec![Value::String("eq".into())]),
        Value::List(vec![Value::String("eq".into()), Value::Int(1), Value::Null]),
        Value::List(vec![Value::String("and".into()), Value::List(Vec::new())]),
        Value::List(vec![
            Value::String("and".into()),
            Value::List(vec![Value::Null]),
        ]),
        Value::List(vec![
            Value::String("unknown".into()),
            Value::String("body".into()),
            Value::Null,
        ]),
    ];
    for value in values {
        cases.push(crate::text::encode_value(&Value::Map(BTreeMap::from([(
            "LINKS".into(),
            value,
        )]))));
    }
    for leaf in [
        "i+000123",
        "i-0",
        "i-9223372036854775808",
        "i9223372036854775807",
        "i9223372036854775808",
        "i-9223372036854775809",
        "i+",
        "i--1",
        "i",
        "f+0",
        "f18446744073709551615",
        "f18446744073709551616",
        "f-1",
        "f",
        "x",
        "x00ff",
        "x0",
        "x+1",
        "xgg",
        "s",
        "s+1",
        "sff",
        "se4b8",
        "l",
        "m",
        "m00",
        "u00000000-0000-0000-0000-000000000000",
        "uinvalid",
        "nextra",
        "b2",
        "中",
    ] {
        cases.push(leaf_text(leaf));
    }
    cases.push(leaf_text(&format!("i{}123", "0".repeat(65537))));
    cases.push(leaf_text(&format!("f+{}0", "0".repeat(65537))));
    let local = local();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    for encoded in cases {
        let context = DecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::new(Default::default()),
        };
        let expected = crate::projection::decode_projected_relationship_predicates(&encoded);
        let actual = predicate::decode(&encoded, &context);
        assert_eq!(
            actual, expected,
            "complete predicate diagnostic/tolerance parity"
        );
        assert_eq!(local.snapshot().running_background_operations, 0);
    }
}

#[test]
fn checkpoint_units_predicate_decode_related_cancels_every_actual_unit_and_fully_retries() {
    let source = source();
    let encoded = crate::projection::encode_projected_relationship_predicates(&source);
    let expected = encode_binary_wal_record(&entry(source), 23).unwrap();
    let local = local();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let baseline = probe.context(local.clone());
    let output = baseline
        .classify(|work| decode_owned(&encoded, work))
        .unwrap_or_else(|_| panic!("baseline predicate decoding must complete"));
    assert_eq!(encode_binary_wal_record(&output, 23).unwrap(), expected);
    let units = probe.completed.load(Ordering::SeqCst);
    assert!(units > 50);
    drop(output);
    drop(baseline);
    probe.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 32 * 1024 * 1024;
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
        let result = work.classify(|work| decode_owned(&encoded, work));
        assert!(
            matches!(result,Err(CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop==0)
                || matches!(result,Err(CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop>0)
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
        let output = decode_owned(&encoded, &work).unwrap();
        assert_eq!(encode_binary_wal_record(&output, 23).unwrap(), expected);
        assert_eq!(
            crate::projection::encode_projected_relationship_predicates(
                &crate::projection::decode_projected_relationship_predicates(&encoded).unwrap()
            ),
            encoded
        );
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

fn frame(fields: &[(u8, Vec<u8>)]) -> Vec<u8> {
    fn varint(mut value: u64, bytes: &mut Vec<u8>) {
        loop {
            let byte = (value & 127) as u8;
            value >>= 7;
            bytes.push(if value == 0 { byte } else { byte | 128 });
            if value == 0 {
                break;
            }
        }
    }
    let mut body = Vec::new();
    for (field, value) in fields {
        body.push((field << 3) | 2);
        varint(value.len() as u64, &mut body);
        body.extend_from_slice(value);
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&17u64.to_le_bytes());
    bytes.push(RECORD_KIND_SINGLE);
    bytes.extend_from_slice(&23u64.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    varint(OP_PROJECT_GRAPH, &mut bytes);
    varint(body.len() as u64, &mut bytes);
    bytes.extend_from_slice(&body);
    bytes
}

#[test]
fn checkpoint_units_predicate_decode_related_borrowed_field_preserves_record_error_priority() {
    let local = local();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    let malformed = [
        vec![],
        b"m".to_vec(),
        b"broken".to_vec(),
        b"m00=ff".to_vec(),
        vec![255],
    ];
    for name in [None, Some(Vec::new()), Some(b"wide".to_vec())] {
        for first in &malformed {
            for second in [None, Some(first.clone()), Some(vec![255])] {
                let mut fields = Vec::new();
                if let Some(name) = &name {
                    fields.push((1, name.clone()));
                }
                fields.push((4, first.clone()));
                if let Some(second) = second {
                    fields.push((4, second));
                }
                let bytes = frame(&fields);
                let expected = decode_binary_wal_record(&bytes).unwrap();
                let actual = decode_binary_wal_record_with_work_context(&bytes, &work).unwrap();
                match (expected, actual) {
                    (
                        BinaryWalRecordDecode::Corrupt(expected),
                        BinaryWalRecordDecode::Corrupt(actual),
                    ) => assert_eq!(actual, expected),
                    (
                        BinaryWalRecordDecode::Entry {
                            entry: expected,
                            commit_epoch: expected_epoch,
                        },
                        BinaryWalRecordDecode::Entry {
                            entry: actual,
                            commit_epoch: actual_epoch,
                        },
                    ) => {
                        assert_eq!(actual_epoch, expected_epoch);
                        assert_eq!(
                            encode_binary_wal_record(&actual, actual_epoch).unwrap(),
                            encode_binary_wal_record(&expected, expected_epoch).unwrap()
                        );
                    }
                    _ => panic!("ordinary/controlled record outcomes disagree"),
                }
                assert_eq!(local.snapshot().running_background_operations, 0);
            }
        }
    }
}

#[test]
fn checkpoint_units_predicate_decode_related_actual_record_retains_values_after_source_closure() {
    let reference = encode_binary_wal_record(&entry(source()), 23).unwrap();
    let bytes = reference.clone();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()))
        .with_scheduler(local());
    let BinaryWalRecordDecode::Entry {
        entry: output,
        commit_epoch,
    } = decode_binary_wal_record_with_work_context(&bytes, &work).unwrap()
    else {
        panic!("admitted complete record must decode")
    };
    drop(bytes);
    drop(work);
    drop(permit);
    assert_eq!(commit_epoch, 23);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    assert_eq!(
        encode_binary_wal_record(&output, commit_epoch).unwrap(),
        reference
    );
    drop(output);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
