// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual encoding-only diagnostics; synchronized query timing is separate.
use hawdb_storage::wal::binary::{
    decode_binary_wal_record, encode_binary_wal_record, BinaryWalRecordDecode,
};
use hawdb_storage::wal::{WalEntry, WalOp};
use hawdb_storage::{NodeId, RelId, Value};
use serde_json::json;
use std::hint::black_box;
use std::io::{BufRead, Write};
use std::time::Instant;

fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        3,
        "usage: wal_batch_encode <node|relationship|large-small> <operations> <rounds>"
    );
    let count: usize = args[1].parse().unwrap();
    let rounds: usize = args[2].parse().unwrap();
    assert!(count > 0 && rounds > 0);
    let ops = (0..count)
        .map(|id| match args[0].as_str() {
            "node" => WalOp::SetNodeProperty {
                id: NodeId(id as u64),
                property: "value".into(),
                value: Value::Int(id as i64),
            },
            "relationship" => WalOp::SetRelationshipProperty {
                id: RelId(id as u64),
                property: "weight".into(),
                value: Value::Int(id as i64),
            },
            "large-small" => WalOp::SetNodeProperty {
                id: NodeId(id as u64),
                property: "body".into(),
                value: if id == 0 {
                    Value::Binary(vec![7; 1024 * 1024])
                } else {
                    Value::Int(id as i64)
                },
            },
            _ => panic!("unknown workload"),
        })
        .collect();
    let entry = WalEntry {
        lsn: 7,
        op: WalOp::Batch(ops),
    };
    let expected = encode_binary_wal_record(&entry, 11).unwrap();
    let BinaryWalRecordDecode::Entry {
        entry: decoded,
        commit_epoch,
    } = decode_binary_wal_record(&expected).unwrap()
    else {
        panic!("encoded batch rejected")
    };
    assert_eq!(decoded.encode(), entry.encode());
    assert_eq!(commit_epoch, 11);
    let checksum = expected.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    println!(
        "{}",
        json!({"ready":args[0],"operations":count,"encoded_bytes":expected.len(),"fnv1a64":checksum,"scope":"encoding-only; no WAL I/O"})
    );
    std::io::stdout().flush().unwrap();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    for round in 0..rounds + 2 {
        let mut command = String::new();
        assert!(input.read_line(&mut command).unwrap() > 0);
        assert_eq!(command.trim(), "step");
        let started = Instant::now();
        let bytes = encode_binary_wal_record(black_box(&entry), black_box(11)).unwrap();
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(bytes, expected);
        black_box(&bytes);
        println!(
            "{}",
            json!({"round":round,"warmup":round<2,"elapsed_ns":elapsed})
        );
        std::io::stdout().flush().unwrap();
    }
}
