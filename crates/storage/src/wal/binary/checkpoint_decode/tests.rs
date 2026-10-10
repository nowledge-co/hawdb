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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

#[derive(PartialEq, Eq)]
pub(super) enum Outcome {
    Entry(Vec<u8>, u64),
    Corrupt(String),
    Error(String),
}

pub(super) fn outcome<E: std::borrow::Borrow<WalEntry>>(
    decoded: Result<BinaryWalRecordDecode<E>>,
) -> Outcome {
    match decoded {
        Ok(BinaryWalRecordDecode::Entry {
            entry,
            commit_epoch,
        }) => Outcome::Entry(
            encode_binary_wal_record(entry.borrow(), commit_epoch).unwrap(),
            commit_epoch,
        ),
        Ok(BinaryWalRecordDecode::Corrupt(reason)) => Outcome::Corrupt(reason),
        Err(error) => Outcome::Error(error.to_string()),
    }
}

pub(super) fn parity(bytes: &[u8], work: &CheckpointWorkContext) {
    assert!(
        outcome(decode_binary_wal_record_with_work_context(bytes, work))
            == outcome(super::super::decode_binary_wal_record(bytes)),
        "controlled and ordinary allocation backends must agree for {} bytes",
        bytes.len(),
    );
}

#[test]
fn checkpoint_units_wal_decode_matches_all_ops_values_and_wide_utf8_boundaries() {
    let scheduler = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(scheduler.clone());
    let mut ops = super::super::tests::sample_ops();
    let mut nested = Value::String("nested".into());
    for _ in 1..MAX_VALUE_DEPTH {
        nested = Value::List(vec![nested]);
    }
    for value in [
        Value::Binary(vec![0xff; 3 * 64 * 1024 + 3]),
        Value::String(format!("{}🦀界", "a".repeat(64 * 1024 - 1))),
        Value::String("🦀界é".repeat(40_000)),
        Value::Map(BTreeMap::from([
            ("empty".into(), Value::List(Vec::new())),
            ("map".into(), Value::Map(BTreeMap::new())),
        ])),
        nested,
    ] {
        ops.push(WalOp::SetNodeProperty {
            id: NodeId(3),
            property: "value".into(),
            value,
        });
    }
    for op in &ops {
        let bytes = encode_binary_wal_record(
            &WalEntry {
                lsn: 17,
                op: op.clone(),
            },
            19,
        )
        .unwrap();
        parity(&bytes, &work);
    }
    let bytes = encode_binary_wal_record(
        &WalEntry {
            lsn: 17,
            op: WalOp::Batch(
                ops.into_iter()
                    .flat_map(|op| match op {
                        WalOp::Batch(children) => children,
                        op => vec![op],
                    })
                    .collect(),
            ),
        },
        19,
    )
    .unwrap();
    parity(&bytes, &work);
    assert_eq!(scheduler.state().running_background_operations, 0);
}

fn raw_record(code: u64, body: &[u8]) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend(17u64.to_le_bytes());
    record.push(RECORD_KIND_SINGLE);
    record.extend(19u64.to_le_bytes());
    record.extend(1u32.to_le_bytes());
    encode_varint_u64(code, &mut record);
    encode_varint_u64(body.len() as u64, &mut record);
    record.extend(body);
    record
}

#[test]
fn checkpoint_units_wal_decode_preserves_invalid_unknown_duplicate_and_utf8_diagnostics() {
    let scheduler = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(scheduler.clone());
    for op in super::super::tests::sample_ops() {
        let bytes = encode_binary_wal_record(&WalEntry { lsn: 17, op }, 19).unwrap();
        for end in 0..bytes.len() {
            parity(&bytes[..end], &work);
        }
        let mut trailing = bytes.clone();
        trailing.push(0xff);
        parity(&trailing, &work);
        for kind in [2, 255] {
            let mut invalid = bytes.clone();
            invalid[8] = kind;
            parity(&invalid, &work);
        }
    }
    for code in 0..=OP_APPEND + 1 {
        parity(&raw_record(code, &[]), &work);
    }
    for offset in [0, 1, 64 * 1024 - 3, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 2] {
        for tail in [&[0xff][..], &[0xe2, 0x82][..], &[0xe2, 0x28, 0xa1][..]] {
            let mut string = vec![b'a'; offset];
            string.extend(tail);
            let mut body = Vec::new();
            encode_len_field(1, &string, &mut body);
            parity(&raw_record(OP_CREATE_NODE_LABEL, &body), &work);
            let mut duplicate = Vec::new();
            encode_string_field(1, "first", &mut duplicate);
            duplicate.extend(body);
            parity(&raw_record(OP_CREATE_NODE_LABEL, &duplicate), &work);
        }
    }
    let mut body = Vec::new();
    encode_string_field(1, "first", &mut body);
    encode_string_field(1, "second", &mut body);
    encode_len_field(100, &[0xff], &mut body);
    encode_varint_field(101, u64::MAX, &mut body);
    parity(&raw_record(OP_CREATE_NODE_LABEL, &body), &work);
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
fn checkpoint_units_wal_decode_cancels_every_classified_unit_and_fully_retries() {
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Batch(vec![
            WalOp::CreateNode {
                id: NodeId(1),
                label: "Item".into(),
                properties: BTreeMap::from([
                    ("name".into(), Value::String("界🦀".repeat(12))),
                    (
                        "nested".into(),
                        Value::List(vec![
                            Value::Int(3),
                            Value::Map(BTreeMap::from([("key".into(), Value::Bool(true))])),
                        ]),
                    ),
                ]),
            },
            WalOp::DeleteNode { id: NodeId(1) },
        ]),
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let scheduler = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let work = baseline.context(scheduler.clone());
    parity(&bytes, &work);
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 30);
    baseline.assert_released(&scheduler);
    for stop in 0..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let held = (stop == 0).then(|| {
            scheduler
                .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                .unwrap()
        });
        assert!(matches!(
            decode_binary_wal_record_with_work_context(&bytes, &work),
            Err(HawDBError::Execution(_))
        ));
        drop(held);
        probe.assert_released(&scheduler);
        let retry = Arc::new(CheckpointWorkProbe::default());
        let work = retry.context(scheduler.clone());
        parity(&bytes, &work);
        retry.assert_released(&scheduler);
    }
}

#[test]
fn checkpoint_units_wal_decode_generated_field_orders_and_mutations_match() {
    let scheduler = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(scheduler.clone());
    let mut checked = 0usize;
    let mut codes = std::collections::BTreeSet::new();
    for op in super::super::tests::sample_ops() {
        let children = match op {
            WalOp::Batch(children) => children,
            op => vec![op],
        };
        for op in children {
            let (code, body) = encode_op_body(&op).unwrap();
            codes.insert(code);
            let mut groups = BTreeMap::<u32, Vec<Vec<u8>>>::new();
            let mut pos = 0;
            while pos < body.len() {
                let start = pos;
                let (id, wire) = decode_tag(&body, &mut pos).unwrap();
                skip_field(&body, &mut pos, wire).unwrap();
                groups
                    .entry(id)
                    .or_default()
                    .push(body[start..pos].to_vec());
            }
            let mut groups: Vec<_> = groups.into_values().collect();
            let original = outcome(super::super::decode_binary_wal_record(&raw_record(
                code, &body,
            )));
            // Move field groups while keeping repeated-field order, which is part
            // of composite-index and projected-graph semantics, unchanged.
            for reverse in [false, true] {
                if reverse {
                    groups.reverse();
                }
                for rotation in 0..groups.len() {
                    let mut reordered = Vec::new();
                    for offset in 0..groups.len() {
                        for field in &groups[(rotation + offset) % groups.len()] {
                            reordered.extend(field);
                        }
                    }
                    // Unknown fields exercise all supported wire types. Invalid
                    // UTF-8 in an unknown byte field must remain uninterpreted.
                    encode_varint_field(100, u64::MAX, &mut reordered);
                    encode_fixed64_field(101, u64::MAX, &mut reordered);
                    encode_len_field(102, &[0xff, 0x80], &mut reordered);
                    let record = raw_record(code, &reordered);
                    assert!(outcome(super::super::decode_binary_wal_record(&record)) == original);
                    assert!(
                        outcome(decode_binary_wal_record_with_work_context(&record, &work))
                            == original
                    );
                    checked += 1;
                }
            }
            for group in &groups {
                for field in group {
                    let mut duplicate = body.clone();
                    duplicate.extend(field);
                    parity(&raw_record(code, &duplicate), &work);
                    checked += 1;
                }
            }
            // Fixed deterministic mutations combine valid field encodings with
            // malformed tags/lengths, rather than testing only valid prefixes.
            for index in 0..body.len() {
                for byte in [0, 0x7f, 0x80, 0xff] {
                    let mut mutated = body.clone();
                    mutated[index] = byte;
                    parity(&raw_record(code, &mutated), &work);
                    checked += 1;
                }
            }
            assert_eq!(scheduler.state().running_background_operations, 0);
        }
    }
    assert_eq!(codes, (OP_CREATE_NODE_LABEL..=OP_APPEND).collect());
    assert!(checked > 1000);
    println!(
        "WAL field-order and mutation parity: {checked} cases, {} operation codes",
        codes.len()
    );
}
