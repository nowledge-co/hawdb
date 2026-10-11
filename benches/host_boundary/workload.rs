// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Engine-only half of the cross-language boundary benchmark. Inputs, statements,
//! consumer work and checksums are shared with Python/Go by the matrix driver.
//! This binary is a local qualification tool, not a production control plane.

use hawdb::{HawDBEmbedded, Value};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Instant;

#[path = "checksum.rs"]
mod checksum;

#[derive(Deserialize)]
struct Job {
    case: String,
    backend: String,
    path: String,
    rows: Vec<BTreeMap<String, serde_json::Value>>,
    insert_single: String,
    insert_bulk: String,
    scan: String,
    point: String,
    columns: Vec<String>,
}

fn value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(Value::Int)
            .unwrap_or_else(|| Value::Float(value.as_f64().expect("finite fixture float"))),
        serde_json::Value::String(value) => Value::String(value),
        serde_json::Value::Array(items) => {
            Value::List(items.into_iter().map(self::value).collect())
        }
        serde_json::Value::Object(items) => Value::Map(
            items
                .into_iter()
                .map(|(key, item)| (key, self::value(item)))
                .collect(),
        ),
    }
}

pub struct Observer {
    pub begin: fn(),
    pub finish: fn() -> serde_json::Value,
}

fn run(job: Job, observer: Option<&Observer>) -> Result<serde_json::Value, String> {
    let input_rows: Vec<_> = job
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(key, item)| (key, value(item)))
                .collect::<BTreeMap<_, _>>()
        })
        .collect();
    let mut database = match job.backend.as_str() {
        "memory" => HawDBEmbedded::open_in_memory(),
        "file" => HawDBEmbedded::open(&job.path).map_err(|error| error.to_string())?,
        _ => return Err("unknown backend".into()),
    };
    let setup = Instant::now();
    database
        .query_admitted("CREATE INDEX ON :Boundary(id)")
        .map_err(|error| format!("setup: {error}"))?;
    if matches!(job.case.as_str(), "select" | "point" | "wide") {
        for rows in input_rows.chunks(512) {
            let params = BTreeMap::from([(
                "rows".into(),
                Value::List(rows.iter().cloned().map(Value::Map).collect()),
            )]);
            database
                .query_with_params_admitted(&job.insert_bulk, &params)
                .map_err(|error| format!("setup: {error}"))?;
        }
    }
    let setup_ns = setup.elapsed().as_nanos();
    // Warm the same read shape in this process/database, rather than relying
    // on the driver's discarded process to warm this handle's plan cache.
    if matches!(job.case.as_str(), "select" | "point" | "wide") {
        let calls = if job.case == "point" {
            input_rows.len()
        } else {
            1
        };
        for row in input_rows.iter().take(calls) {
            let params = if job.case == "point" {
                BTreeMap::from([("id".into(), row["id"].clone())])
            } else {
                BTreeMap::new()
            };
            let output = database
                .query_with_params_admitted(
                    if job.case == "point" {
                        &job.point
                    } else {
                        &job.scan
                    },
                    &params,
                )
                .map_err(|error| format!("warmup: {error}"))?;
            std::hint::black_box(output);
        }
    }
    let mut query_ns = 0u128;
    let mut consume_ns = 0u128;
    let mut rows_seen = 0usize;
    let mut payload_bytes = 0usize;
    let mut hash = checksum::Checksum::new(&job.columns);
    if let Some(observer) = observer {
        (observer.begin)();
    }
    let started = Instant::now();
    if job.case == "fill" {
        for row in &input_rows {
            let called = Instant::now();
            database
                .query_with_params_admitted(&job.insert_single, row)
                .map_err(|error| format!("execute: {error}"))?;
            query_ns += called.elapsed().as_nanos();
        }
    } else if job.case == "fill_bulk" {
        let params = BTreeMap::from([(
            "rows".into(),
            Value::List(input_rows.iter().cloned().map(Value::Map).collect()),
        )]);
        let called = Instant::now();
        database
            .query_with_params_admitted(&job.insert_bulk, &params)
            .map_err(|error| format!("execute: {error}"))?;
        query_ns += called.elapsed().as_nanos();
    }
    let write_ns = query_ns;
    let calls = if job.case == "point" {
        input_rows.len()
    } else {
        1
    };
    for row in input_rows.iter().take(calls) {
        let params = if job.case == "point" {
            BTreeMap::from([("id".into(), row["id"].clone())])
        } else {
            BTreeMap::new()
        };
        let called = Instant::now();
        let output = database
            .query_with_params_admitted(
                if job.case == "point" {
                    &job.point
                } else {
                    &job.scan
                },
                &params,
            )
            .map_err(|error| format!("execute: {error}"))?;
        query_ns += called.elapsed().as_nanos();
        if output.schema().columns() != job.columns {
            return Err("consumer: result schema mismatch".into());
        }
        payload_bytes += output.payload_bytes();
        let consumed = Instant::now();
        for values in output.value_rows() {
            hash.row(values);
            rows_seen += 1;
        }
        consume_ns += consumed.elapsed().as_nanos();
    }
    let elapsed_ns = started.elapsed().as_nanos();
    let native_profile = observer.map(|observer| (observer.finish)());
    Ok(json!({
        "status": "ok", "layer": "rust", "case": job.case, "backend": job.backend,
        "input_rows": input_rows.len(), "output_rows": rows_seen,
        "values": rows_seen * job.columns.len(), "checksum": hash.hex(),
        "setup_ns": setup_ns, "query_boundary_ns": query_ns,
        "write_boundary_ns": write_ns, "read_boundary_ns": query_ns - write_ns,
        "consumer_ns": consume_ns, "elapsed_ns": elapsed_ns,
        "native_profile": native_profile,
        "engine_payload_bytes": payload_bytes,
        "durability": "SyncOnEveryWrite", "prefetch": 0,
    }))
}

pub fn main_with_observer(observer: Option<&Observer>) {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("Usage: host_boundary JOB.json (see bindings/benchmarks/run.py)");
        return;
    };
    let result = std::fs::read(&path)
        .map_err(|error| error.to_string())
        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
        .and_then(|job| run(job, observer));
    println!(
        "{}",
        result.unwrap_or_else(|error| json!({"status": "error", "layer": "rust", "error": error}))
    );
}
