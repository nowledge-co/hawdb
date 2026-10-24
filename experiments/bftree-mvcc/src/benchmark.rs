// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::adapter::{Error, Result, Stage, Store, MAX_SCAN_RECORDS, MAX_SCAN_ROWS, MAX_WRITES};
use crate::index::Layout;
use hawdb_qos::ProcessMemorySnapshot;
use serde::Serialize;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

#[derive(Serialize)]
struct Latency {
    operations: usize,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    total_ns: u64,
}
impl Latency {
    fn from(mut samples: Vec<u64>) -> Self {
        let total_ns = samples.iter().sum();
        samples.sort_unstable();
        let percentile = |p: usize| samples[(samples.len() * p).div_ceil(100).saturating_sub(1)];
        Self {
            operations: samples.len(),
            p50_ns: percentile(50),
            p95_ns: percentile(95),
            p99_ns: percentile(99),
            total_ns,
        }
    }
}
#[derive(Serialize)]
struct Report {
    layout: Layout,
    keys: usize,
    key_shape: &'static str,
    update_rounds: usize,
    value_bytes: usize,
    profile: &'static str,
    working_index: &'static str,
    durability: &'static str,
    commit_batches: Latency,
    prepare_and_sync: Latency,
    index_install: Latency,
    current_get: Latency,
    old_get: Latency,
    current_full_scan: Latency,
    old_full_scan: Latency,
    reopen_ns: u64,
    journal_bytes: u64,
    resident_bytes: Option<u64>,
    process_peak_bytes: Option<u64>,
}
fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}
#[derive(Clone, Copy)]
pub enum KeyShape {
    Numeric8,
    Prefix64,
    Spread64,
}
impl KeyShape {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "numeric8" => Ok(Self::Numeric8),
            "prefix64" => Ok(Self::Prefix64),
            "spread64" => Ok(Self::Spread64),
            _ => Err(Error::Invalid("unknown benchmark key shape")),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Numeric8 => "numeric8",
            Self::Prefix64 => "prefix64",
            Self::Spread64 => "spread64",
        }
    }
    fn key(self, n: usize) -> Vec<u8> {
        let number = (n as u64).to_be_bytes();
        match self {
            Self::Numeric8 => number.to_vec(),
            Self::Prefix64 => {
                let mut key = vec![b'p'; 56];
                key.extend_from_slice(&number);
                key
            }
            Self::Spread64 => {
                let mut key = number.to_vec();
                let mut state = (n as u64).wrapping_add(0xd1b5_4a32_d192_ed03);
                for _ in 0..56 {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    key.push(state as u8);
                }
                key
            }
        }
    }
}
fn value(round: usize, n: usize) -> Vec<u8> {
    let mut value = vec![b'x'; 100];
    value[..8].copy_from_slice(&(round as u64).to_le_bytes());
    value[8..16].copy_from_slice(&(n as u64).to_le_bytes());
    value
}

pub fn one(path: &Path, layout: Layout, keys: usize, rounds: usize) -> Result<()> {
    one_shaped(path, layout, keys, rounds, KeyShape::Numeric8)
}

pub fn one_shaped(
    path: &Path,
    layout: Layout,
    keys: usize,
    rounds: usize,
    shape: KeyShape,
) -> Result<()> {
    if !(16..=MAX_SCAN_ROWS).contains(&keys)
        || !(1..=64).contains(&rounds)
        || keys * (rounds + 1) > MAX_SCAN_RECORDS
        || path.exists()
    {
        return Err(Error::Invalid(
            "benchmark needs a new path, 16..4096 keys and 1..64 rounds",
        ));
    }
    let key = |n| shape.key(n);
    let store = Store::open_layout(path, layout)?;
    let mut operation = 1;
    for start in (0..keys).step_by(MAX_WRITES) {
        let mut tx = store.begin_write()?;
        for n in start..(start + MAX_WRITES).min(keys) {
            tx.put(&key(n), &value(0, n))?;
        }
        tx.commit(operation)?;
        operation += 1;
    }
    let old = store.begin_read()?;
    let mut commits = Vec::new();
    let mut prepare_and_sync = Vec::new();
    let mut index_install = Vec::new();
    for round in 1..=rounds {
        for start in (0..keys).step_by(64) {
            let began = Instant::now();
            let mut tx = store.begin_write()?;
            for n in start..(start + 64).min(keys) {
                tx.put(&key(n), &value(round, n))?;
            }
            let commit_start = Instant::now();
            let mut durable = None;
            tx.commit_with_hook(operation, |stage| {
                match stage {
                    Stage::Durable => {
                        prepare_and_sync.push(nanos(commit_start));
                        durable = Some(Instant::now());
                    }
                    Stage::Published => {
                        index_install.push(nanos(durable.expect("durable boundary")))
                    }
                    Stage::Installed(_) => {}
                }
                Ok(())
            })?;
            operation += 1;
            commits.push(nanos(began));
        }
    }
    let current = store.begin_read()?;
    // Warm both paths once before the recorded, deterministic read sequence.
    assert_eq!(current.get(&key(0))?, Some(value(rounds, 0)));
    assert_eq!(old.get(&key(0))?, Some(value(0, 0)));
    let mut current_get = Vec::new();
    let mut old_get = Vec::new();
    for i in 0..4096 {
        let n = (i * 73) % keys;
        let began = Instant::now();
        let got = current.get(&key(n))?;
        current_get.push(nanos(began));
        assert_eq!(got, Some(value(rounds, n)));
        let began = Instant::now();
        let got = old.get(&key(n))?;
        old_get.push(nanos(began));
        assert_eq!(got, Some(value(0, n)));
    }
    let mut current_scan = Vec::new();
    let mut old_scan = Vec::new();
    for _ in 0..32 {
        let began = Instant::now();
        let rows = current.scan(&[], &[255; 64])?;
        current_scan.push(nanos(began));
        assert_eq!(rows.len(), keys);
        assert!(rows
            .iter()
            .enumerate()
            .all(|(n, (k, v))| *k == key(n) && *v == value(rounds, n)));
        let began = Instant::now();
        let rows = old.scan(&[], &[255; 64])?;
        old_scan.push(nanos(began));
        assert_eq!(rows.len(), keys);
        assert!(rows
            .iter()
            .enumerate()
            .all(|(n, (k, v))| *k == key(n) && *v == value(0, n)));
    }
    let memory = ProcessMemorySnapshot::capture().ok();
    let journal_bytes = store.journal_bytes()?;
    drop(old);
    drop(current);
    drop(store);
    let began = Instant::now();
    let reopened = Store::open_layout(path, layout)?;
    let reopen_ns = nanos(began);
    assert_eq!(
        reopened.begin_read()?.get(&key(keys - 1))?,
        Some(value(rounds, keys - 1))
    );
    let report = Report {
        layout,
        keys,
        key_shape: shape.name(),
        update_rounds: rounds,
        value_bytes: 100,
        profile: if cfg!(debug_assertions) {
            "checked-dev"
        } else {
            "release"
        },
        working_index: "memory; journal on native filesystem",
        durability: "sync_all per complete transaction batch",
        commit_batches: Latency::from(commits),
        prepare_and_sync: Latency::from(prepare_and_sync),
        index_install: Latency::from(index_install),
        current_get: Latency::from(current_get),
        old_get: Latency::from(old_get),
        current_full_scan: Latency::from(current_scan),
        old_full_scan: Latency::from(old_scan),
        reopen_ns,
        journal_bytes,
        resident_bytes: memory
            .filter(|m| m.capabilities.resident_memory)
            .map(|m| m.resident_bytes),
        process_peak_bytes: memory
            .filter(|m| m.capabilities.resident_memory)
            .map(|m| m.peak_resident_bytes),
    };
    println!(
        "{}",
        serde_json::to_string(&report).map_err(|e| Error::Index(e.to_string()))?
    );
    Ok(())
}

pub fn compare(directory: &Path, keys: usize, rounds: usize) -> Result<()> {
    // create_dir deliberately refuses an existing experiment directory.
    std::fs::create_dir(directory)?;
    let mut reports = Vec::new();
    let layouts = [Layout::Versioned, Layout::Hybrid, Layout::Cow];
    for repetition in 0..3 {
        for offset in 0..3 {
            let layout = layouts[(repetition + offset) % 3];
            let path = directory.join(format!("{}-{repetition}.journal", layout.name()));
            let output = Command::new(std::env::current_exe()?)
                .arg("bench-one")
                .arg(&path)
                .arg(layout.name())
                .arg(keys.to_string())
                .arg(rounds.to_string())
                .output()?;
            if !output.status.success() {
                return Err(Error::Index(format!(
                    "benchmark child failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )));
            }
            reports.push(
                serde_json::from_slice::<serde_json::Value>(&output.stdout)
                    .map_err(|e| Error::Index(e.to_string()))?,
            );
        }
    }
    let output = serde_json::json!({"repetitions":3,"order":"rotated; separate process per layout/run","reports":reports});
    let encoded = serde_json::to_vec_pretty(&output).map_err(|e| Error::Index(e.to_string()))?;
    std::fs::write(directory.join("report.json"), &encoded)?;
    println!("{}", String::from_utf8_lossy(&encoded));
    Ok(())
}
