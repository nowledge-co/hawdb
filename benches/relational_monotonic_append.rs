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

use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, RelationalIndexMode, StorageResidencyMode,
};
use serde_json::json;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const SEED_ROWS_PER_PARTITION: usize = if SMOKE { 512 } else { 8_192 };
const SAMPLES: usize = if SMOKE { 3 } else { 31 };
const WARMUP_SAMPLES: usize = if SMOKE { 1 } else { 3 };
const BATCH_SIZES: &[usize] = if SMOKE { &[8] } else { &[1, 8, 32] };

fn main() {
    let results = BATCH_SIZES
        .iter()
        .copied()
        .map(benchmark_batch)
        .collect::<Vec<_>>();
    println!(
        "relational_monotonic_append {}",
        json!({
            "seed_rows_per_partition": SEED_ROWS_PER_PARTITION,
            "samples": SAMPLES,
            "durability": "sync_on_checkpoint",
            "results": results,
        })
    );
}

fn benchmark_batch(batch_size: usize) -> serde_json::Value {
    let candidate_path = unique_path(&format!("candidate-{batch_size}"));
    let baseline_path = unique_path(&format!("baseline-{batch_size}"));
    let mut candidate = seeded_database(&candidate_path, true);
    let mut baseline = seeded_database(&baseline_path, false);
    let mut candidate_samples = Vec::with_capacity(SAMPLES);
    let mut baseline_samples = Vec::with_capacity(SAMPLES);
    let total_samples = WARMUP_SAMPLES + SAMPLES;

    for sample in 0..total_samples {
        let base = sample * batch_size;
        let sql = insert_batch(base, batch_size);
        if sample % 2 == 0 {
            measure(&mut candidate, &sql, sample, &mut candidate_samples)
                .unwrap_or_else(|error| panic!("candidate batch {batch_size} failed: {error}"));
            measure(&mut baseline, &sql, sample, &mut baseline_samples)
                .unwrap_or_else(|error| panic!("baseline batch {batch_size} failed: {error}"));
        } else {
            measure(&mut baseline, &sql, sample, &mut baseline_samples)
                .unwrap_or_else(|error| panic!("baseline batch {batch_size} failed: {error}"));
            measure(&mut candidate, &sql, sample, &mut candidate_samples)
                .unwrap_or_else(|error| panic!("candidate batch {batch_size} failed: {error}"));
        }
    }

    candidate_samples.sort_unstable();
    baseline_samples.sort_unstable();
    let candidate_p50 = percentile(&candidate_samples, 50);
    let candidate_p95 = percentile(&candidate_samples, 95);
    let candidate_p99 = percentile(&candidate_samples, 99);
    let baseline_p50 = percentile(&baseline_samples, 50);
    let baseline_p95 = percentile(&baseline_samples, 95);
    let baseline_p99 = percentile(&baseline_samples, 99);
    let candidate_metrics = candidate.storage_residency_report().relational_rows;
    let baseline_metrics = baseline.storage_residency_report().relational_rows;

    if batch_size > 1 {
        assert_eq!(
            candidate_metrics.monotonic_append_hits,
            total_samples as u64
        );
        assert_eq!(candidate_metrics.monotonic_append_fallbacks, 0);
        assert_eq!(baseline_metrics.monotonic_append_attempts, 0);
        if !SMOKE {
            assert!(
                candidate_p50 < baseline_p50,
                "enabled monotonic append p50 must beat the disabled baseline for batch {batch_size}: candidate={candidate_p50}ns baseline={baseline_p50}ns"
            );
        }
    } else {
        assert_eq!(candidate_metrics.monotonic_append_attempts, 0);
        assert_eq!(baseline_metrics.monotonic_append_attempts, 0);
        if !SMOKE {
            assert!(
                candidate_p95.saturating_mul(100) <= baseline_p95.saturating_mul(105),
                "enabled single-row p95 regressed by more than 5%: candidate={candidate_p95}ns baseline={baseline_p95}ns"
            );
        }
    }

    let report = json!({
        "batch_size": batch_size,
        "candidate_ns_p50": candidate_p50,
        "candidate_ns_p95": candidate_p95,
        "candidate_ns_p99": candidate_p99,
        "baseline_ns_p50": baseline_p50,
        "baseline_ns_p95": baseline_p95,
        "baseline_ns_p99": baseline_p99,
        "candidate_rows_per_second_p50": rows_per_second(batch_size, candidate_p50),
        "baseline_rows_per_second_p50": rows_per_second(batch_size, baseline_p50),
        "p50_speedup": baseline_p50 as f64 / candidate_p50.max(1) as f64,
        "single_row_p95_regression": (batch_size == 1).then(|| {
            candidate_p95 as f64 / baseline_p95.max(1) as f64 - 1.0
        }),
        "candidate_monotonic_append_attempts": candidate_metrics.monotonic_append_attempts,
        "candidate_monotonic_append_hits": candidate_metrics.monotonic_append_hits,
        "candidate_monotonic_append_fallbacks": candidate_metrics.monotonic_append_fallbacks,
        "candidate_proven_absent_primary_keys": candidate_metrics.monotonic_append_proven_absent_primary_keys,
        "baseline_monotonic_append_attempts": baseline_metrics.monotonic_append_attempts,
    });
    drop(candidate);
    drop(baseline);
    std::fs::remove_dir_all(candidate_path).expect("remove candidate benchmark database");
    std::fs::remove_dir_all(baseline_path).expect("remove baseline benchmark database");
    report
}

fn measure(
    db: &mut Database,
    sql: &str,
    sample: usize,
    samples: &mut Vec<u64>,
) -> Result<(), String> {
    let started = Instant::now();
    let output = db.query_sql(sql).map_err(|error| error.to_string())?;
    let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    black_box(output);
    if sample >= WARMUP_SAMPLES {
        samples.push(elapsed);
    }
    Ok(())
}

fn seeded_database(path: &Path, fast_path_enabled: bool) -> Database {
    let seed_config = DatabaseConfig {
        relational_index_mode: RelationalIndexMode::Shadow,
        storage_residency_mode: StorageResidencyMode::Materialized,
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_durability_and_config(
        path,
        DurabilityPolicy::SyncOnCheckpoint,
        seed_config,
    )
    .expect("open monotonic append benchmark seed database");
    db.query_sql(
        "CREATE TABLE events (stream TEXT NOT NULL, sequence BIGINT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (stream, sequence))",
    )
    .expect("create monotonic append benchmark table");
    for start in (0..SEED_ROWS_PER_PARTITION).step_by(256) {
        let count = (SEED_ROWS_PER_PARTITION - start).min(256);
        db.query_sql(&seed_batch("a", start, count))
            .expect("seed lower partition");
        db.query_sql(&seed_batch("c", start, count))
            .expect("seed upper partition");
    }
    db.checkpoint().expect("checkpoint benchmark seed");
    drop(db);

    let config = DatabaseConfig {
        relational_index_mode: RelationalIndexMode::Authoritative,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_monotonic_append_fast_path: fast_path_enabled,
        ..DatabaseConfig::default()
    };
    Database::open_with_durability_and_config(path, DurabilityPolicy::SyncOnCheckpoint, config)
        .expect("reopen monotonic append benchmark database")
}

fn seed_batch(stream: &str, start: usize, count: usize) -> String {
    let values = (start..start + count)
        .map(|sequence| format!("('{stream}', {sequence}, 'seed-{sequence:08}')"))
        .collect::<Vec<_>>()
        .join(",");
    format!("INSERT INTO events (stream, sequence, payload) VALUES {values}")
}

fn insert_batch(base: usize, count: usize) -> String {
    let values = (base..base + count)
        .map(|sequence| format!("('b', {sequence}, 'event-{sequence:08}')"))
        .collect::<Vec<_>>()
        .join(",");
    format!("INSERT INTO events (stream, sequence, payload) VALUES {values}")
}

fn percentile(samples: &[u64], percentile: usize) -> u64 {
    let index = (samples.len() - 1) * percentile / 100;
    samples[index]
}

fn rows_per_second(batch_size: usize, elapsed_ns: u64) -> f64 {
    batch_size as f64 * 1_000_000_000.0 / elapsed_ns.max(1) as f64
}

fn unique_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-relational-monotonic-append-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}
