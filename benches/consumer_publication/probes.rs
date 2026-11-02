// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{latency, ns};
use hawdb::{Database, DatabasePublicationOwner, Value};
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Barrier;
use std::time::Instant;

pub struct Probe {
    pub enabled: bool,
    pub clock: Instant,
}

pub struct Phase {
    name: &'static str,
    started_ns: u64,
    before: Json,
}

impl Probe {
    pub fn begin(&self, name: &'static str) -> Option<Phase> {
        self.enabled.then(|| {
            let before = resources();
            Phase {
                name,
                started_ns: ns(self.clock),
                before,
            }
        })
    }

    pub fn finish(&self, phase: Option<Phase>) -> Option<Json> {
        phase.map(|phase| {
            let finished_ns = ns(self.clock);
            json!({"phase": phase.name, "started_ns": phase.started_ns,
                "finished_ns": finished_ns, "resources_before": phase.before,
                "resources_after": resources()})
        })
    }
}

fn resources() -> Json {
    let mut result = json!({
        "scope": "process observations at phase boundaries; not phase heap ownership or a phase peak",
        "status_available": false,
        "rss_unit": "KiB", "cpu_unit": "Linux process clock ticks, not converted",
    });
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        result["status_available"] = json!(true);
        for line in status.lines() {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if matches!(name, "VmRSS" | "VmHWM" | "RssAnon" | "RssFile" | "Threads") {
                result[name] = json!(value
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap());
            }
        }
    }
    if let Ok(stat) = std::fs::read_to_string("/proc/self/stat")
        && let Some((_, fields)) = stat.rsplit_once(") ")
    {
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        result["cpu_user_ticks"] = json!(fields[11].parse::<u64>().unwrap());
        result["cpu_system_ticks"] = json!(fields[12].parse::<u64>().unwrap());
    }
    if let Ok(io) = std::fs::read_to_string("/proc/self/io") {
        for line in io.lines() {
            if let Some((name, value)) = line.split_once(':') {
                result[format!("io_{name}")] = json!(value.trim().parse::<u64>().unwrap());
            }
        }
    }
    result
}

pub struct Reader<'a> {
    pub owner: &'a DatabasePublicationOwner<Database>,
    pub barrier: &'a Barrier,
    pub active_writers: &'a AtomicUsize,
    pub committed_writes: &'a AtomicUsize,
    pub probe: &'a Probe,
    pub worker: usize,
    pub requests: usize,
    pub documents: usize,
    pub payload: usize,
}

impl Reader<'_> {
    pub fn run(self) -> Vec<Json> {
        let expected_content = Value::String("x".repeat(self.payload));
        let mut samples = Vec::with_capacity(self.requests);
        self.barrier.wait();
        for sequence in 0..self.requests {
            let id = Value::String(format!("m{}", (self.worker + sequence) % self.documents));
            let parameters = BTreeMap::from([("id".into(), id.clone())]);
            let started_ns = ns(self.probe.clock);
            let active_before = self.active_writers.load(Ordering::Acquire);
            let writes_before = self.committed_writes.load(Ordering::Acquire);
            let requested = Instant::now();
            let (mut snapshot, wait, held) = self
                .owner
                .with_read(|database| {
                    let entered = Instant::now();
                    let wait = ns(requested);
                    let snapshot = database.begin_read_transaction()?;
                    Ok((snapshot, wait, ns(entered)))
                })
                .unwrap();
            let query_started_ns = ns(self.probe.clock);
            let rows = snapshot
                .query_with_params(
                    "MATCH (n:Memory) WHERE n.id=$id RETURN n.id AS id,n.content AS content",
                    &parameters,
                )
                .unwrap()
                .rows;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["id"], id);
            assert_eq!(rows[0]["content"], expected_content);
            let elapsed = ns(requested);
            let finished_ns = ns(self.probe.clock);
            let active_after = self.active_writers.load(Ordering::Acquire);
            let writes_after = self.committed_writes.load(Ordering::Acquire);
            drop(snapshot);
            samples.push(json!({
                "worker": self.worker, "sequence": sequence,
                "started_ns": started_ns, "query_started_ns": query_started_ns,
                "finished_ns": finished_ns, "elapsed_ns": elapsed,
                "owner_wait_plus_entry_ns": wait, "owner_read_callback_ns": held,
                "active_writers_before": active_before, "active_writers_after": active_after,
                "committed_writes_before": writes_before, "committed_writes_after": writes_after,
            }));
        }
        samples
    }
}

pub fn summarize(workers: Vec<Vec<Json>>, attempts: &[Json]) -> Json {
    let samples = workers.into_iter().flatten().collect::<Vec<_>>();
    let elapsed = samples
        .iter()
        .map(|s| s["elapsed_ns"].as_u64().unwrap())
        .collect::<Vec<_>>();
    let waits = samples
        .iter()
        .map(|s| s["owner_wait_plus_entry_ns"].as_u64().unwrap())
        .collect::<Vec<_>>();
    let completed_during_preparation = samples
        .iter()
        .filter(|sample| {
            attempts.iter().any(|attempt| {
                attempt["phase_observations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|phase| {
                        matches!(
                            phase["phase"].as_str(),
                            Some("coverage_preparation" | "native_construction")
                        ) && sample["query_started_ns"].as_u64().unwrap()
                            >= phase["started_ns"].as_u64().unwrap()
                            && sample["finished_ns"].as_u64().unwrap()
                                <= phase["finished_ns"].as_u64().unwrap()
                    })
            })
        })
        .count();
    let completed_with_active_writers = samples
        .iter()
        .filter(|s| s["active_writers_after"].as_u64().unwrap() > 0)
        .count();
    json!({"requests": samples.len(), "latency": latency(&elapsed),
        "owner_wait_plus_entry": latency(&waits), "samples": samples,
        "completed_during_preparation": completed_during_preparation,
        "completed_with_active_writers": completed_with_active_writers,
        "query": "canonical Memory identity and complete content via a fresh pinned transaction per request",
        "scope": "read-owner capture followed by query outside the outer owner; validation included, snapshot drop excluded"})
}
