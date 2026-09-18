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
    ConcurrentDatabase, ConcurrentTransactionOptions, Database,
    WalGroupCommitAdaptiveColdStartEvidence, WalGroupCommitAdaptivePolicyEvidence,
    WalGroupCommitAdaptiveSteadyStateEvidence, WalGroupCommitConfig, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
};
use serde_json::json;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "wal_group_commit/recovery.rs"]
mod recovery;
use recovery::{verify_recovery, ExpectedRecovery};

const WORKERS: usize = 8;
const COMMITS_PER_WORKER: usize = 32;
const CONCURRENT_COMMIT_COUNT: usize = WORKERS * COMMITS_PER_WORKER;
const SINGLE_WRITER_COMMIT_COUNT: usize = 128;
const MEASUREMENT_ROUNDS: usize = 11;
const MIN_CONCURRENT_P95_REGRESSION_BUDGET_MICROS: u64 = 100;
const MIN_SINGLE_WRITER_P95_REGRESSION_BUDGET_MICROS: u64 = 50;
const P95_REGRESSION_BUDGET_PER_MILLION: u64 = 50_000;
const MIN_POLICY_ELAPSED_REGRESSION_BUDGET_MICROS: u64 = 1_000;
// Gross-regression guard only. A derived window that lands near the fixed
// default is indistinguishable from it, so a tight budget here samples noise
// instead of measuring the policy. Correctness of the derivation itself is
// proven deterministically over the fsync-baseline matrix.
const POLICY_ELAPSED_REGRESSION_BUDGET_PER_MILLION: u64 = 250_000;
const POLICY_SAFETY_NET_P95_BUDGET_PER_MILLION: u64 = 250_000;
const STAGGER_STEP_MICROS: u64 = 100;
const ADAPTIVE_WARMUP_COMMIT_COUNT: usize = 8;
const ADAPTIVE_WARMUP_SETTLE_DELAY: Duration = Duration::from_millis(110);

fn main() {
    let concurrent_pairs = measure_pairs("concurrent", measure_concurrent);
    let single_writer_pairs = measure_pairs("single-writer", measure_single_writer);
    let cold_policy_pairs = measure_policy_pairs("staggered-cold", measure_staggered);
    let steady_state_policy_pairs =
        measure_policy_pairs("staggered-steady-state", measure_warm_staggered);
    let concurrent = PairedSummary::from_pairs(&concurrent_pairs);
    let single_writer = PairedSummary::from_pairs(&single_writer_pairs);
    let cold_policy = PairedSummary::from_pairs(&cold_policy_pairs);
    let steady_state_policy = PairedSummary::from_pairs(&steady_state_policy_pairs);
    let concurrent_p95_budget = hybrid_p95_budget(
        concurrent.baseline_p95_commit_micros,
        MIN_CONCURRENT_P95_REGRESSION_BUDGET_MICROS,
    );
    let single_writer_p95_budget = hybrid_p95_budget(
        single_writer.baseline_p95_commit_micros,
        MIN_SINGLE_WRITER_P95_REGRESSION_BUDGET_MICROS,
    );
    let steady_state_policy_p95_budget = safety_net_p95_budget(
        steady_state_policy.baseline_p95_commit_micros,
        MIN_CONCURRENT_P95_REGRESSION_BUDGET_MICROS,
    );
    let steady_state_policy_elapsed_budget =
        hybrid_elapsed_budget(steady_state_policy.baseline_elapsed_micros);
    let evidence = WalGroupCommitEvidence {
        measurement_rounds: MEASUREMENT_ROUNDS,
        commit_count: CONCURRENT_COMMIT_COUNT,
        baseline_elapsed_micros: concurrent.baseline_elapsed_micros,
        baseline_fsync_count: CONCURRENT_COMMIT_COUNT as u64,
        grouped_elapsed_micros: concurrent.candidate_elapsed_micros,
        grouped_fsync_count: concurrent.candidate_shared_sync_count,
        concurrent_tail_latency: WalGroupCommitTailLatencyEvidence {
            commit_count: CONCURRENT_COMMIT_COUNT,
            paired_p95_regression_micros: concurrent.paired_p95_regression_micros,
            paired_p95_mad_micros: concurrent.paired_p95_mad_micros,
            max_accepted_p95_regression_micros: concurrent_p95_budget,
        },
        single_writer_tail_latency: WalGroupCommitTailLatencyEvidence {
            commit_count: SINGLE_WRITER_COMMIT_COUNT,
            paired_p95_regression_micros: single_writer.paired_p95_regression_micros,
            paired_p95_mad_micros: single_writer.paired_p95_mad_micros,
            max_accepted_p95_regression_micros: single_writer_p95_budget,
        },
        single_writer_max_coalescing_wait_count: single_writer.candidate_max_coalescing_wait_count,
        single_writer_max_observed_group_entries: single_writer
            .candidate_max_observed_group_entries,
        adaptive_cold_start_behavior: Some(WalGroupCommitAdaptiveColdStartEvidence {
            commit_count: CONCURRENT_COMMIT_COUNT,
            min_fallback_delay_count: cold_policy.candidate_min_adaptive_fallback_count,
            min_coalescing_wait_count: cold_policy.candidate_min_coalescing_wait_count,
            min_observed_group_entries: cold_policy.candidate_min_observed_group_entries,
        }),
        adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
            commit_count: CONCURRENT_COMMIT_COUNT,
            max_fallback_delay_count: steady_state_policy.candidate_max_adaptive_fallback_count,
            min_fsync_baseline_sample_count: steady_state_policy
                .candidate_min_fsync_baseline_sample_count,
            min_coalescing_wait_count: steady_state_policy.candidate_min_coalescing_wait_count,
            min_observed_group_entries: steady_state_policy.candidate_min_observed_group_entries,
            safety_net: policy_evidence(
                &steady_state_policy,
                steady_state_policy_elapsed_budget,
                steady_state_policy_p95_budget,
            ),
        }),
        strict_recovery_verified: concurrent.strict_recovery_verified
            && single_writer.strict_recovery_verified
            && cold_policy.strict_recovery_verified
            && steady_state_policy.strict_recovery_verified,
        wal_order_verified: concurrent.wal_order_verified
            && single_writer.wal_order_verified
            && cold_policy.wal_order_verified
            && steady_state_policy.wal_order_verified,
    };
    let admission = WalGroupCommitConfig::adaptive_enabled_after_evidence(
        evidence,
        NonZeroUsize::new(WORKERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_micros(500),
    );

    println!(
        "wal_group_commit {}",
        json!({
            "measurement_rounds": MEASUREMENT_ROUNDS,
            "concurrent": {
                "workers": WORKERS,
                "commit_count": CONCURRENT_COMMIT_COUNT,
                "summary": concurrent.to_json(),
                "paired_rounds": paired_measurements_json(&concurrent_pairs),
                "max_p95_regression_micros": concurrent_p95_budget,
            },
            "single_writer": {
                "commit_count": SINGLE_WRITER_COMMIT_COUNT,
                "summary": single_writer.to_json(),
                "paired_rounds": paired_measurements_json(&single_writer_pairs),
                "max_p95_regression_micros": single_writer_p95_budget,
            },
            "staggered_cold_start_fixed_vs_adaptive": {
                "workers": WORKERS,
                "commit_count": CONCURRENT_COMMIT_COUNT,
                "stagger_step_micros": STAGGER_STEP_MICROS,
                "baseline_policy": "fixed",
                "candidate_policy": "adaptive_fsync",
                "evidence_kind": "behavioral",
                "summary": cold_policy.to_json(),
                "paired_rounds": paired_measurements_json(&cold_policy_pairs),
                "min_fallback_delay_count": cold_policy.candidate_min_adaptive_fallback_count,
                "min_coalescing_wait_count": cold_policy.candidate_min_coalescing_wait_count,
                "min_observed_group_entries": cold_policy.candidate_min_observed_group_entries,
            },
            "staggered_steady_state_fixed_vs_adaptive": {
                "workers": WORKERS,
                "commit_count": CONCURRENT_COMMIT_COUNT,
                "warmup_commit_count": ADAPTIVE_WARMUP_COMMIT_COUNT,
                "stagger_step_micros": STAGGER_STEP_MICROS,
                "baseline_policy": "fixed",
                "candidate_policy": "adaptive_fsync",
                "summary": steady_state_policy.to_json(),
                "paired_rounds": paired_measurements_json(&steady_state_policy_pairs),
                "evidence_kind": "behavioral_with_safety_net",
                "safety_net_max_elapsed_regression_micros": steady_state_policy_elapsed_budget,
                "safety_net_max_p95_regression_micros": steady_state_policy_p95_budget,
                "max_fallback_delay_count": steady_state_policy.candidate_max_adaptive_fallback_count,
                "min_fsync_baseline_sample_count": steady_state_policy
                    .candidate_min_fsync_baseline_sample_count,
                "min_coalescing_wait_count": steady_state_policy.candidate_min_coalescing_wait_count,
                "min_observed_group_entries": steady_state_policy.candidate_min_observed_group_entries,
            },
            "throughput_improvement_ratio": concurrent.baseline_elapsed_micros as f64
                / concurrent.candidate_elapsed_micros.max(1) as f64,
            "evidence_admitted": admission.is_ok(),
            "evidence_rejection": admission.err().map(|error| error.to_string()),
        })
    );
}

type MeasureFn = fn(&str, WalGroupCommitConfig) -> Measurement;

fn measure_pairs(label: &str, measure: MeasureFn) -> Vec<MeasurementPair> {
    (0..MEASUREMENT_ROUNDS)
        .map(|round| {
            let baseline_label = format!("{label}-round-{round}-baseline");
            let candidate_label = format!("{label}-round-{round}-candidate");
            let candidate_config = adaptive_candidate_config();
            let (baseline, candidate) = if round % 2 == 0 {
                (
                    measure(&baseline_label, WalGroupCommitConfig::disabled()),
                    measure(&candidate_label, candidate_config),
                )
            } else {
                let candidate = measure(&candidate_label, candidate_config);
                let baseline = measure(&baseline_label, WalGroupCommitConfig::disabled());
                (baseline, candidate)
            };
            MeasurementPair {
                round,
                baseline,
                candidate,
            }
        })
        .collect()
}

fn measure_policy_pairs(label: &str, measure: MeasureFn) -> Vec<MeasurementPair> {
    (0..MEASUREMENT_ROUNDS)
        .map(|round| {
            let fixed_label = format!("{label}-round-{round}-fixed");
            let adaptive_label = format!("{label}-round-{round}-adaptive");
            let fixed_config = fixed_candidate_config();
            let adaptive_config = adaptive_candidate_config();
            let (baseline, candidate) = if round % 2 == 0 {
                (
                    measure(&fixed_label, fixed_config),
                    measure(&adaptive_label, adaptive_config),
                )
            } else {
                let candidate = measure(&adaptive_label, adaptive_config);
                let baseline = measure(&fixed_label, fixed_config);
                (baseline, candidate)
            };
            MeasurementPair {
                round,
                baseline,
                candidate,
            }
        })
        .collect()
}

fn fixed_candidate_config() -> WalGroupCommitConfig {
    WalGroupCommitConfig::benchmark_candidate(
        NonZeroUsize::new(WORKERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    )
    .expect("fixed benchmark candidate bounds must be valid")
}

fn adaptive_candidate_config() -> WalGroupCommitConfig {
    WalGroupCommitConfig::benchmark_adaptive_candidate(
        NonZeroUsize::new(WORKERS).unwrap(),
        NonZeroU64::new(1024 * 1024).unwrap(),
        Duration::from_micros(500),
    )
    .expect("adaptive benchmark candidate bounds must be valid")
}

fn policy_evidence(
    summary: &PairedSummary,
    elapsed_budget_micros: u64,
    p95_budget_micros: u64,
) -> WalGroupCommitAdaptivePolicyEvidence {
    WalGroupCommitAdaptivePolicyEvidence {
        paired_elapsed_regression_micros: summary.paired_elapsed_regression_micros,
        paired_elapsed_mad_micros: summary.paired_elapsed_mad_micros,
        max_accepted_elapsed_regression_micros: elapsed_budget_micros,
        tail_latency: WalGroupCommitTailLatencyEvidence {
            commit_count: CONCURRENT_COMMIT_COUNT,
            paired_p95_regression_micros: summary.paired_p95_regression_micros,
            paired_p95_mad_micros: summary.paired_p95_mad_micros,
            max_accepted_p95_regression_micros: p95_budget_micros,
        },
    }
}

fn measure_concurrent(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure_multi_writer(label, group_commit, 0, false)
}

fn measure_staggered(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure_multi_writer(label, group_commit, STAGGER_STEP_MICROS, false)
}

fn measure_warm_staggered(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure_multi_writer(label, group_commit, STAGGER_STEP_MICROS, true)
}

fn measure_multi_writer(
    label: &str,
    group_commit: WalGroupCommitConfig,
    stagger_step_micros: u64,
    warm_fsync_baseline: bool,
) -> Measurement {
    measure(
        label,
        group_commit,
        CONCURRENT_COMMIT_COUNT,
        warm_fsync_baseline,
        |database| {
            let barrier = Arc::new(Barrier::new(WORKERS));
            let writers = (0..WORKERS)
            .map(|worker| {
                let database = database.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut latencies = Vec::with_capacity(COMMITS_PER_WORKER);
                    for round in 0..COMMITS_PER_WORKER {
                        let id = worker * COMMITS_PER_WORKER + round;
                        let mut transaction = database
                            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                                Duration::from_secs(5),
                            ))
                            .expect("benchmark transaction must begin");
                        transaction
                            .query_sql(&format!(
                                "INSERT INTO public.messages (id, body) VALUES ({id}, 'payload-{id}')"
                            ))
                            .expect("benchmark insert must stage");
                        barrier.wait();
                        let stagger_steps = worker.saturating_sub(1) as u64;
                        if stagger_step_micros > 0 && stagger_steps > 0 {
                            std::thread::sleep(Duration::from_micros(
                                stagger_steps.saturating_mul(stagger_step_micros),
                            ));
                        }
                        let commit_started = Instant::now();
                        transaction.commit().expect("benchmark commit must succeed");
                        latencies.push(elapsed_micros(commit_started));
                    }
                    latencies
                })
            })
            .collect::<Vec<_>>();
            writers
                .into_iter()
                .flat_map(|writer| writer.join().expect("benchmark writer must join"))
                .collect()
        },
    )
}

fn measure_single_writer(label: &str, group_commit: WalGroupCommitConfig) -> Measurement {
    measure(
        label,
        group_commit,
        SINGLE_WRITER_COMMIT_COUNT,
        false,
        |database| {
            let mut latencies = Vec::with_capacity(SINGLE_WRITER_COMMIT_COUNT);
            for id in 0..SINGLE_WRITER_COMMIT_COUNT {
                let mut transaction = database
                    .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                        Duration::from_secs(5),
                    ))
                    .expect("benchmark transaction must begin");
                transaction
                    .query_sql(&format!(
                        "INSERT INTO public.messages (id, body) VALUES ({id}, 'payload-{id}')"
                    ))
                    .expect("benchmark insert must stage");
                let commit_started = Instant::now();
                transaction.commit().expect("benchmark commit must succeed");
                latencies.push(elapsed_micros(commit_started));
            }
            latencies
        },
    )
}

fn measure(
    label: &str,
    group_commit: WalGroupCommitConfig,
    commit_count: usize,
    warm_fsync_baseline: bool,
    execute: impl FnOnce(&ConcurrentDatabase) -> Vec<u64>,
) -> Measurement {
    let mut progress = MeasurementProgress::new(label);
    let path = benchmark_path(label);
    let mut database = Database::open(&path).expect("benchmark database must open");
    progress.start("setup-schema");
    database
        .query_sql("CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
        .expect("benchmark schema must be created");
    let database = ConcurrentDatabase::new_with_wal_group_commit(database, group_commit);
    progress.start("warmup");
    let warmup_commit_count = if warm_fsync_baseline {
        warm_group_commit_baseline(&database);
        ADAPTIVE_WARMUP_COMMIT_COUNT
    } else {
        0
    };
    let group_commit_before = database
        .wal_group_commit_snapshot()
        .expect("group commit metrics must be readable before measurement");
    progress.start("measurement");
    let started = Instant::now();
    let mut commit_latencies = execute(&database);
    let elapsed_micros = elapsed_micros(started);
    progress.start("observation");
    commit_latencies.sort_unstable();
    let commit_latency = CommitLatencySummary::from_sorted(&commit_latencies);
    let group_commit_after = database
        .wal_group_commit_snapshot()
        .expect("group commit metrics must be readable");
    let group_commit = group_commit_delta(group_commit_before, group_commit_after);
    let final_epoch = database
        .commit_epoch()
        .expect("benchmark commit epoch must be readable");
    eprintln!(
        "wal_group_commit counters measurement={label} elapsed_micros={elapsed_micros} submitted={} completed={} syncs={} fsync_micros={}",
        group_commit.submitted_commits,
        group_commit.completed_commits,
        group_commit.shared_sync_count,
        group_commit.total_fsync_micros,
    );
    eprintln!(
        "wal_group_commit latency measurement={label} commits={} total_micros={} min_micros={} p50_micros={} p95_micros={} p99_micros={} max_micros={}",
        commit_latency.count,
        commit_latency.total_micros,
        commit_latency.min_micros,
        commit_latency.p50_micros,
        commit_latency.p95_micros,
        commit_latency.p99_micros,
        commit_latency.max_micros,
    );
    progress.start("database-close");
    drop(database);

    let verification = verify_recovery(
        &path,
        ExpectedRecovery {
            final_epoch,
            commit_count,
            warmup_start_id: CONCURRENT_COMMIT_COUNT,
            warmup_commit_count,
        },
        |phase| progress.start(phase),
    );
    progress.start("cleanup");
    std::fs::remove_dir_all(&path).expect("benchmark database must be removable");
    progress.finish();
    Measurement {
        commit_count,
        elapsed_micros,
        p50_commit_micros: commit_latency.p50_micros,
        p95_commit_micros: commit_latency.p95_micros,
        p99_commit_micros: commit_latency.p99_micros,
        group_commit,
        strict_recovery_verified: verification.strict_recovery_verified,
        wal_order_verified: verification.wal_order_verified,
    }
}

fn warm_group_commit_baseline(database: &ConcurrentDatabase) {
    for offset in 0..ADAPTIVE_WARMUP_COMMIT_COUNT {
        let id = CONCURRENT_COMMIT_COUNT + offset;
        let mut transaction = database
            .begin_transaction(ConcurrentTransactionOptions::pessimistic(
                Duration::from_secs(5),
            ))
            .expect("benchmark warmup transaction must begin");
        transaction
            .query_sql(&format!(
                "INSERT INTO public.messages (id, body) VALUES ({id}, 'warmup-{id}')"
            ))
            .expect("benchmark warmup insert must stage");
        transaction
            .commit()
            .expect("benchmark warmup commit must succeed");
    }
    std::thread::sleep(ADAPTIVE_WARMUP_SETTLE_DELAY);
}

fn group_commit_delta(
    before: WalGroupCommitSnapshot,
    after: WalGroupCommitSnapshot,
) -> WalGroupCommitSnapshot {
    WalGroupCommitSnapshot {
        adaptive_fallback_count: after
            .adaptive_fallback_count
            .saturating_sub(before.adaptive_fallback_count),
        adaptive_delay_clamp_count: after
            .adaptive_delay_clamp_count
            .saturating_sub(before.adaptive_delay_clamp_count),
        submitted_commits: after
            .submitted_commits
            .saturating_sub(before.submitted_commits),
        completed_commits: after
            .completed_commits
            .saturating_sub(before.completed_commits),
        group_count: after.group_count.saturating_sub(before.group_count),
        coalescing_wait_count: after
            .coalescing_wait_count
            .saturating_sub(before.coalescing_wait_count),
        shared_sync_count: after
            .shared_sync_count
            .saturating_sub(before.shared_sync_count),
        grouped_wal_entries: after
            .grouped_wal_entries
            .saturating_sub(before.grouped_wal_entries),
        grouped_wal_bytes: after
            .grouped_wal_bytes
            .saturating_sub(before.grouped_wal_bytes),
        total_fsync_micros: after
            .total_fsync_micros
            .saturating_sub(before.total_fsync_micros),
        ..after
    }
}

struct MeasurementProgress<'a> {
    label: &'a str,
    phase: &'static str,
    started: Instant,
}

impl<'a> MeasurementProgress<'a> {
    fn new(label: &'a str) -> Self {
        eprintln!("wal_group_commit progress measurement={label} phase=setup-open state=started");
        Self {
            label,
            phase: "setup-open",
            started: Instant::now(),
        }
    }

    fn start(&mut self, phase: &'static str) {
        self.finish();
        eprintln!(
            "wal_group_commit progress measurement={} phase={phase} state=started",
            self.label,
        );
        self.phase = phase;
        self.started = Instant::now();
    }

    fn finish(&self) {
        eprintln!(
            "wal_group_commit progress measurement={} phase={} state=completed elapsed_micros={}",
            self.label,
            self.phase,
            elapsed_micros(self.started),
        );
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    let index = values
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(values.len().saturating_sub(1));
    values.get(index).copied().unwrap_or_default()
}

struct CommitLatencySummary {
    count: usize,
    total_micros: u64,
    min_micros: u64,
    p50_micros: u64,
    p95_micros: u64,
    p99_micros: u64,
    max_micros: u64,
}

impl CommitLatencySummary {
    fn from_sorted(values: &[u64]) -> Self {
        Self {
            count: values.len(),
            total_micros: values.iter().copied().fold(0_u64, u64::saturating_add),
            min_micros: values.first().copied().unwrap_or_default(),
            p50_micros: percentile(values, 50),
            p95_micros: percentile(values, 95),
            p99_micros: percentile(values, 99),
            max_micros: values.last().copied().unwrap_or_default(),
        }
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn benchmark_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-wal-group-commit-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

struct MeasurementPair {
    round: usize,
    baseline: Measurement,
    candidate: Measurement,
}

struct PairedSummary {
    baseline_elapsed_micros: u64,
    candidate_elapsed_micros: u64,
    baseline_p95_commit_micros: u64,
    candidate_p95_commit_micros: u64,
    paired_p95_regression_micros: i64,
    paired_p95_mad_micros: u64,
    paired_elapsed_regression_micros: i64,
    paired_elapsed_mad_micros: u64,
    baseline_shared_sync_count: u64,
    candidate_shared_sync_count: u64,
    candidate_max_coalescing_wait_count: u64,
    candidate_max_observed_group_entries: usize,
    // Minima across rounds: cold-start evidence must hold in every round, not
    // in the best one.
    candidate_min_coalescing_wait_count: u64,
    candidate_min_observed_group_entries: usize,
    candidate_min_adaptive_fallback_count: u64,
    candidate_max_adaptive_fallback_count: u64,
    candidate_min_fsync_baseline_sample_count: u64,
    strict_recovery_verified: bool,
    wal_order_verified: bool,
}

impl PairedSummary {
    fn from_pairs(pairs: &[MeasurementPair]) -> Self {
        let paired_p95_regressions = pairs
            .iter()
            .map(|pair| {
                signed_delta(
                    pair.candidate.p95_commit_micros,
                    pair.baseline.p95_commit_micros,
                )
            })
            .collect::<Vec<_>>();
        let paired_elapsed_regressions = pairs
            .iter()
            .map(|pair| signed_delta(pair.candidate.elapsed_micros, pair.baseline.elapsed_micros))
            .collect::<Vec<_>>();
        Self {
            baseline_elapsed_micros: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.baseline.elapsed_micros)
                    .collect(),
            ),
            candidate_elapsed_micros: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.candidate.elapsed_micros)
                    .collect(),
            ),
            baseline_p95_commit_micros: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.baseline.p95_commit_micros)
                    .collect(),
            ),
            candidate_p95_commit_micros: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.candidate.p95_commit_micros)
                    .collect(),
            ),
            paired_p95_regression_micros: median_i64(paired_p95_regressions.clone()),
            paired_p95_mad_micros: median_absolute_deviation(&paired_p95_regressions),
            paired_elapsed_regression_micros: median_i64(paired_elapsed_regressions.clone()),
            paired_elapsed_mad_micros: median_absolute_deviation(&paired_elapsed_regressions),
            baseline_shared_sync_count: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.baseline.group_commit.shared_sync_count)
                    .collect(),
            ),
            candidate_shared_sync_count: median_u64(
                pairs
                    .iter()
                    .map(|pair| pair.candidate.group_commit.shared_sync_count)
                    .collect(),
            ),
            candidate_min_coalescing_wait_count: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.coalescing_wait_count)
                .min()
                .unwrap_or_default(),
            candidate_min_observed_group_entries: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.max_observed_group_entries)
                .min()
                .unwrap_or_default(),
            candidate_min_adaptive_fallback_count: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.adaptive_fallback_count)
                .min()
                .unwrap_or_default(),
            candidate_max_adaptive_fallback_count: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.adaptive_fallback_count)
                .max()
                .unwrap_or_default(),
            candidate_min_fsync_baseline_sample_count: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.fsync_baseline_sample_count)
                .min()
                .unwrap_or_default(),
            candidate_max_coalescing_wait_count: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.coalescing_wait_count)
                .max()
                .unwrap_or_default(),
            candidate_max_observed_group_entries: pairs
                .iter()
                .map(|pair| pair.candidate.group_commit.max_observed_group_entries)
                .max()
                .unwrap_or_default(),
            strict_recovery_verified: pairs.iter().all(|pair| {
                pair.baseline.strict_recovery_verified && pair.candidate.strict_recovery_verified
            }),
            wal_order_verified: pairs
                .iter()
                .all(|pair| pair.baseline.wal_order_verified && pair.candidate.wal_order_verified),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        json!({
            "baseline_elapsed_micros": self.baseline_elapsed_micros,
            "candidate_elapsed_micros": self.candidate_elapsed_micros,
            "baseline_p95_commit_micros": self.baseline_p95_commit_micros,
            "candidate_p95_commit_micros": self.candidate_p95_commit_micros,
            "paired_p95_regression_micros": self.paired_p95_regression_micros,
            "paired_p95_mad_micros": self.paired_p95_mad_micros,
            "paired_elapsed_regression_micros": self.paired_elapsed_regression_micros,
            "paired_elapsed_mad_micros": self.paired_elapsed_mad_micros,
            "baseline_shared_sync_count": self.baseline_shared_sync_count,
            "candidate_shared_sync_count": self.candidate_shared_sync_count,
            "candidate_max_coalescing_wait_count": self.candidate_max_coalescing_wait_count,
            "candidate_max_observed_group_entries": self.candidate_max_observed_group_entries,
            "strict_recovery_verified": self.strict_recovery_verified,
            "wal_order_verified": self.wal_order_verified,
        })
    }
}

fn paired_measurements_json(pairs: &[MeasurementPair]) -> serde_json::Value {
    pairs
        .iter()
        .map(|pair| {
            json!({
                "round": pair.round,
                "baseline": pair.baseline.to_json(),
                "candidate": pair.candidate.to_json(),
                "paired_p95_regression_micros": signed_delta(
                    pair.candidate.p95_commit_micros,
                    pair.baseline.p95_commit_micros,
                ),
                "paired_elapsed_regression_micros": signed_delta(
                    pair.candidate.elapsed_micros,
                    pair.baseline.elapsed_micros,
                ),
            })
        })
        .collect()
}

fn hybrid_p95_budget(baseline_p95_micros: u64, minimum_micros: u64) -> u64 {
    let relative = u64::try_from(
        u128::from(baseline_p95_micros) * u128::from(P95_REGRESSION_BUDGET_PER_MILLION) / 1_000_000,
    )
    .unwrap_or(u64::MAX);
    minimum_micros.max(relative)
}

fn safety_net_p95_budget(baseline_p95_micros: u64, minimum_micros: u64) -> u64 {
    let relative = u64::try_from(
        u128::from(baseline_p95_micros) * u128::from(POLICY_SAFETY_NET_P95_BUDGET_PER_MILLION)
            / 1_000_000,
    )
    .unwrap_or(u64::MAX);
    minimum_micros.max(relative)
}

fn hybrid_elapsed_budget(baseline_elapsed_micros: u64) -> u64 {
    let relative = u64::try_from(
        u128::from(baseline_elapsed_micros)
            * u128::from(POLICY_ELAPSED_REGRESSION_BUDGET_PER_MILLION)
            / 1_000_000,
    )
    .unwrap_or(u64::MAX);
    MIN_POLICY_ELAPSED_REGRESSION_BUDGET_MICROS.max(relative)
}

fn signed_delta(candidate: u64, baseline: u64) -> i64 {
    let delta = i128::from(candidate) - i128::from(baseline);
    delta.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn median_i64(mut values: Vec<i64>) -> i64 {
    values.sort_unstable();
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn median_absolute_deviation(values: &[i64]) -> u64 {
    let median = median_i64(values.to_vec());
    median_u64(values.iter().map(|value| value.abs_diff(median)).collect())
}

struct Measurement {
    commit_count: usize,
    elapsed_micros: u64,
    p50_commit_micros: u64,
    p95_commit_micros: u64,
    p99_commit_micros: u64,
    group_commit: WalGroupCommitSnapshot,
    strict_recovery_verified: bool,
    wal_order_verified: bool,
}

impl Measurement {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "elapsed_micros": self.elapsed_micros,
            "commits_per_second": self.commit_count as f64 * 1_000_000.0
                / self.elapsed_micros.max(1) as f64,
            "p50_commit_micros": self.p50_commit_micros,
            "p95_commit_micros": self.p95_commit_micros,
            "p99_commit_micros": self.p99_commit_micros,
            "shared_sync_count": self.group_commit.shared_sync_count,
            "group_count": self.group_commit.group_count,
            "coalescing_wait_count": self.group_commit.coalescing_wait_count,
            "delay_policy": self.group_commit.delay_policy.as_str(),
            "last_wait_decision": self.group_commit.last_wait_decision.as_str(),
            "effective_delay_micros": self.group_commit.effective_delay_micros,
            "max_observed_effective_delay_micros": self
                .group_commit
                .max_observed_effective_delay_micros,
            "fsync_baseline_micros": self.group_commit.fsync_baseline_micros,
            "fsync_baseline_sample_count": self.group_commit.fsync_baseline_sample_count,
            "adaptive_fallback_count": self.group_commit.adaptive_fallback_count,
            "adaptive_delay_clamp_count": self.group_commit.adaptive_delay_clamp_count,
            "max_observed_group_entries": self.group_commit.max_observed_group_entries,
            "max_observed_group_bytes": self.group_commit.max_observed_group_bytes,
            "strict_recovery_verified": self.strict_recovery_verified,
            "wal_order_verified": self.wal_order_verified,
        })
    }
}
