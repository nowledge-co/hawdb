// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual same-process comparison of serialized and MVCC admitted entrypoints.

use hawdb::{
    EmbeddedQueryError, HawDBConcurrentEmbedded, HawDBEmbedded, QueryOutput, QueryRequest,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, Value,
};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

enum Engine {
    Serialized(Box<Mutex<HawDBEmbedded>>),
    Mvcc(HawDBConcurrentEmbedded),
}

impl Engine {
    fn query(
        &self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput, EmbeddedQueryError> {
        let request = QueryRequest::new(text).with_params(parameters);
        match self {
            Self::Serialized(engine) => engine.lock().unwrap().query_request_admitted(request),
            Self::Mvcc(engine) => engine.query_request_admitted(request),
        }
    }
}

fn fixture(rows: usize, mvcc: bool) -> Engine {
    let mut embedded = HawDBEmbedded::open_in_memory();
    embedded.update_runtime_resources(RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
        RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(4 << 30), None, None, None),
    ));
    embedded
        .query_with_params_admitted(
            "UNWIND $ids AS row CREATE (:Item {id: row.id, value: row.value})",
            &BTreeMap::from([(
                "ids".into(),
                Value::List(
                    (0..rows)
                        .map(|id| {
                            Value::Map(BTreeMap::from([
                                ("id".into(), Value::Int(id as i64)),
                                ("value".into(), Value::Int(0)),
                            ]))
                        })
                        .collect(),
                ),
            )]),
        )
        .unwrap();
    embedded
        .query_admitted("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")
        .unwrap();
    embedded
        .query_admitted("CREATE INDEX ON :Item(id)")
        .unwrap();
    if mvcc {
        Engine::Mvcc(HawDBConcurrentEmbedded::new(embedded))
    } else {
        Engine::Serialized(Box::new(Mutex::new(embedded)))
    }
}

fn run(rows: usize, workers: usize, operations: usize, mixed: bool, mvcc: bool, round: usize) {
    let setup_started = Instant::now();
    let engine = Arc::new(fixture(rows, mvcc));
    let read = "MATCH (n:Item) WHERE n.value >= 0 RETURN count(n) AS total";
    for _ in 0..3 {
        engine.query(read, &BTreeMap::new()).unwrap();
    }
    let setup = setup_started.elapsed();
    let start = Arc::new(Barrier::new(workers + 1));
    let mut handles = Vec::new();
    for worker in 0..workers {
        let engine = Arc::clone(&engine);
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            let params = BTreeMap::from([("id".into(), Value::Int(worker as i64))]);
            let mut reads = Vec::new();
            let mut writes = Vec::new();
            start.wait();
            for operation in 0..operations {
                let writing = mixed && operation % 10 == 0;
                let before = Instant::now();
                if writing {
                    engine
                        .query(
                            "MATCH (n:Item) WHERE n.id = $id SET n.value = n.value + 1",
                            &params,
                        )
                        .unwrap();
                    writes.push(before.elapsed().as_nanos() as u64);
                } else {
                    let output = engine.query(read, &BTreeMap::new()).unwrap();
                    assert_eq!(output.rows[0]["total"], Value::Int(rows as i64));
                    reads.push(before.elapsed().as_nanos() as u64);
                }
            }
            (reads, writes)
        }));
    }
    let before = Instant::now();
    start.wait();
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    for handle in handles {
        let (mut r, mut w) = handle.join().unwrap();
        reads.append(&mut r);
        writes.append(&mut w);
    }
    let elapsed = before.elapsed();
    let output = engine
        .query(
            "MATCH (n:Item) WHERE n.id < 4 RETURN n.value AS value",
            &BTreeMap::new(),
        )
        .unwrap();
    let actual: i64 = output
        .rows
        .iter()
        .map(|row| match row["value"] {
            Value::Int(value) => value,
            _ => panic!("expected integer counter"),
        })
        .sum();
    assert_eq!(actual, writes.len() as i64);
    let governor = match engine.as_ref() {
        Engine::Serialized(engine) => engine.lock().unwrap().runtime_governor().snapshot(),
        Engine::Mvcc(engine) => engine.runtime_governor().snapshot(),
    };
    assert_eq!(governor.active_cpu_slots, 0);
    assert_eq!(governor.admitted_memory_bytes, 0);
    reads.sort_unstable();
    writes.sort_unstable();
    let quantile = |values: &[u64], percentile: usize| {
        values
            .get((values.len().saturating_sub(1) * percentile) / 100)
            .copied()
            .unwrap_or(0) as f64
            / 1_000_000.0
    };
    println!(
        "{}",
        serde_json::json!({
            "mode": if mvcc { "mvcc" } else { "serialized_admitted" },
            "round": round, "rows": rows, "workers": workers, "operations_per_worker": operations,
            "setup_and_warmup_ms": setup.as_secs_f64()*1000.0,
            "effective_cpu_slots": governor.limits.effective_cpu_slots.get(),
            "memory_budget_bytes": governor.limits.memory_budget_bytes,
            "admission_rejections": governor.admission_rejections,
            "write_percent": if mixed { 10 } else { 0 }, "elapsed_ms": elapsed.as_secs_f64()*1000.0,
            "operations_per_second": (workers*operations) as f64 / elapsed.as_secs_f64(),
            "read_p50_ms": quantile(&reads, 50), "read_p95_ms": quantile(&reads, 95), "read_p99_ms": quantile(&reads, 99),
            "write_p50_ms": quantile(&writes, 50), "write_p95_ms": quantile(&writes, 95), "write_p99_ms": quantile(&writes, 99),
            "verified_writes": writes.len(),
        })
    );
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let number = |index, default| {
        args.get(index)
            .map(|s: &String| s.parse::<usize>().unwrap())
            .unwrap_or(default)
    };
    let rounds = number(1, 3);
    let operations = number(2, 100);
    let rows = number(3, 4096);
    for round in 0..rounds {
        for workers in [1, 4] {
            for mixed in [false, true] {
                for mvcc in if round % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    run(rows, workers, operations, mixed, mvcc, round);
                }
            }
        }
    }
}
