// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb::{ConcurrentDatabase, QueryOutput, Value};
use serde_json::{json, Value as JsonValue};
use std::sync::Barrier;
use std::time::Instant;

use super::{latency, COMMITS, SMOKE};

pub const READ: &str =
    "MATCH (n:MvccCounter) RETURN n.id AS id, n.value AS value, n.mirror AS mirror ORDER BY id";
pub const UPDATE: &str = "MATCH (n:MvccCounter) WHERE n.id = $id SET n.value = 1, n.mirror = 1";
const READS: usize = if SMOKE { 8 } else { 64 };

pub struct Measurement {
    worker: usize,
    samples: Vec<u64>,
    intervals: Vec<(u64, u64)>,
    epochs: Vec<u64>,
}

pub fn verify(output: &QueryOutput, committed: u64) {
    assert_eq!(output.rows.len(), COMMITS);
    let mut updated = 0;
    for (id, row) in output.rows.iter().enumerate() {
        assert_eq!(row["id"], Value::Int(id.try_into().unwrap()));
        assert!(matches!(row["value"], Value::Int(0 | 1)));
        assert_eq!(row["value"], row["mirror"], "torn property update");
        updated += u64::from(row["value"] == Value::Int(1));
    }
    assert_eq!(updated, committed, "rows must match their snapshot epoch");
}

pub fn measure(
    database: &ConcurrentDatabase,
    worker: usize,
    ready: &Barrier,
    go: &Barrier,
    origin: Instant,
    initial_epoch: u64,
) -> Measurement {
    let mut samples = Vec::with_capacity(READS);
    let mut intervals = Vec::with_capacity(READS);
    let mut epochs = Vec::with_capacity(READS);
    let mut previous = initial_epoch;
    let mut seen = [false; COMMITS];
    ready.wait();
    go.wait();
    for _ in 0..READS {
        let started = Instant::now();
        let mut snapshot = database.begin_read_transaction().unwrap();
        let epoch = snapshot.commit_epoch();
        let output = snapshot.query(READ).unwrap();
        drop(snapshot);
        let finished = Instant::now();
        samples.push(
            finished
                .duration_since(started)
                .as_nanos()
                .try_into()
                .unwrap(),
        );
        intervals.push((
            started
                .duration_since(origin)
                .as_nanos()
                .try_into()
                .unwrap(),
            finished
                .duration_since(origin)
                .as_nanos()
                .try_into()
                .unwrap(),
        ));
        assert!(epoch >= previous, "fresh reads must not move backwards");
        verify(&output, epoch - initial_epoch);
        for (id, row) in output.rows.iter().enumerate() {
            let updated = row["value"] == Value::Int(1);
            assert!(!seen[id] || updated, "committed row disappeared");
            seen[id] = updated;
        }
        previous = epoch;
        epochs.push(epoch);
    }
    Measurement {
        worker,
        samples,
        intervals,
        epochs,
    }
}

pub fn report(
    measurements: &[Measurement],
    writes: &[(u64, u64)],
    elapsed: u64,
    initial_epoch: u64,
) -> JsonValue {
    if measurements.is_empty() {
        return JsonValue::Null;
    }
    let write_start = writes.iter().map(|s| s.0).min().unwrap();
    let write_end = writes.iter().map(|s| s.1).max().unwrap();
    let read_start = measurements
        .iter()
        .flat_map(|m| &m.intervals)
        .map(|s| s.0)
        .min()
        .unwrap();
    let read_end = measurements
        .iter()
        .flat_map(|m| &m.intervals)
        .map(|s| s.1)
        .max()
        .unwrap();
    let writes_during_reads = writes
        .iter()
        .filter(|s| s.0 >= read_start && s.1 <= read_end)
        .count();
    let reads_during_writes = measurements
        .iter()
        .flat_map(|m| &m.intervals)
        .filter(|s| s.0 >= write_start && s.1 <= write_end)
        .count();
    let intermediate_reads = measurements
        .iter()
        .flat_map(|m| &m.epochs)
        .filter(|&&epoch| epoch > initial_epoch && epoch < initial_epoch + COMMITS as u64)
        .count();
    // A successful finite workload alone would allow all reads to run before
    // or after the writes. Reject that as missing mixed-workload coverage.
    assert!(
        writes_during_reads > 0 && reads_during_writes > 0 && intermediate_reads > 0,
        "mixed workload lacked coverage: writes_during_reads={writes_during_reads}, \
         reads_during_writes={reads_during_writes}, intermediate_reads={intermediate_reads}, \
         writer_phase_ns={write_start}..{write_end}, reader_phase_ns={read_start}..{read_end}"
    );
    let samples = measurements
        .iter()
        .flat_map(|m| m.samples.iter().copied())
        .collect::<Vec<_>>();
    json!({
        "requests": samples.len(), "requests_per_reader": READS,
        "requests_per_second": samples.len() as f64 * 1e9 / elapsed as f64,
        "latency": latency(&samples),
        "reads_during_writer_phase": reads_during_writes,
        "writes_during_reader_phase": writes_during_reads,
        "intermediate_snapshot_reads": intermediate_reads,
        "pinned_snapshot_verified": true,
        "workers": measurements.iter().map(|m| json!({
            "worker": m.worker, "requests": m.samples.len(),
            "intervals_ns": m.intervals, "epochs": m.epochs,
            "latency": latency(&m.samples),
        })).collect::<Vec<_>>(),
    })
}
