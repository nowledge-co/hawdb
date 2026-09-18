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

use hawdb::executor::{
    execute_with_row_consumer_profile_and_external_and_context_and_memory, ExecutionMemoryConfig,
    ExternalReadOperator, VectorSeedExecutionOutput, VectorSeedExecutionRequest,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::schema::Catalog;
use hawdb::store::GraphStore;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::{
    execute_morsels_ordered, MorselAdmission, MorselAdmissionRequest, PipelineId,
    SharedExecutorPool, SharedPoolMorselScheduler,
};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const MORSEL_ROWS: usize = 1_048_576;
const MORSEL_TARGET_ROWS: usize = 16_384;
const MORSEL_ITERATIONS: usize = 8;
const SCHEDULER_SAMPLES: usize = 11;
const PRODUCTION_BATCHES_PER_MORSEL: usize = 16;
const PRODUCTION_MIN_MORSELS_PER_WORKER: usize = 4;
const DEFAULT_BENCH_MORSELS_PER_WORKER: usize = 16;
const MORSEL_WORKERS_ENV: &str = "HAWDB_MORSEL_BENCH_WORKERS";
const MORSEL_ROWS_ENV: &str = "HAWDB_MORSEL_BENCH_ROWS";

pub(super) fn scheduler_benchmark(requested_workers: NonZeroUsize) -> serde_json::Value {
    let bytes_per_worker = NonZeroUsize::new(64 * 1024).unwrap();
    let admission = MorselAdmission::try_new(MorselAdmissionRequest {
        pipeline_id: PipelineId(1),
        input_rows: MORSEL_ROWS,
        target_rows: NonZeroUsize::new(MORSEL_TARGET_ROWS).unwrap(),
        requested_parallelism: requested_workers,
        bytes_per_worker,
        memory_budget_bytes: NonZeroUsize::new(
            bytes_per_worker
                .get()
                .saturating_mul(requested_workers.get()),
        )
        .unwrap(),
    })
    .unwrap();
    let input = (0..MORSEL_ROWS)
        .map(|row| (row as u64).wrapping_mul(0x9e37_79b9))
        .collect::<Vec<_>>();
    let pool = SharedExecutorPool::new(requested_workers).unwrap();
    let scheduler = SharedPoolMorselScheduler::new(pool);
    let mut sequential_checksum = 0u64;
    let mut parallel_checksum = 0u64;
    let mut sequential_samples = Vec::with_capacity(SCHEDULER_SAMPLES);
    let mut parallel_samples = Vec::with_capacity(SCHEDULER_SAMPLES);
    for sample in 0..SCHEDULER_SAMPLES {
        let parallel_first = sample % 2 == 1;
        for parallel in [parallel_first, !parallel_first] {
            let started = Instant::now();
            for _ in 0..MORSEL_ITERATIONS {
                let outputs = if parallel {
                    scheduler
                        .execute(&admission, |morsel| {
                            Ok(morsel_checksum(
                                &input[morsel.start_row..morsel.start_row + morsel.row_count],
                            ))
                        })
                        .unwrap()
                } else {
                    execute_morsels_ordered(&admission, |morsel| {
                        Ok(morsel_checksum(
                            &input[morsel.start_row..morsel.start_row + morsel.row_count],
                        ))
                    })
                    .unwrap()
                };
                let checksum = outputs
                    .into_iter()
                    .fold(0u64, |total, value| total.wrapping_add(value));
                if parallel {
                    parallel_checksum = black_box(checksum);
                } else {
                    sequential_checksum = black_box(checksum);
                }
            }
            if parallel {
                parallel_samples.push(started.elapsed().as_nanos());
            } else {
                sequential_samples.push(started.elapsed().as_nanos());
            }
        }
    }
    assert_eq!(parallel_checksum, sequential_checksum);
    sequential_samples.sort_unstable();
    parallel_samples.sort_unstable();
    let sequential_ns = sequential_samples[SCHEDULER_SAMPLES / 2];
    let parallel_ns = parallel_samples[SCHEDULER_SAMPLES / 2];
    serde_json::json!({
        "rows": MORSEL_ROWS,
        "target_rows": MORSEL_TARGET_ROWS,
        "morsel_count": admission.morsel_count(),
        "admitted_workers": admission.max_workers(),
        "iterations_per_sample": MORSEL_ITERATIONS,
        "samples": SCHEDULER_SAMPLES,
        "sequential_median_ns": sequential_ns,
        "shared_pool_median_ns": parallel_ns,
        "speedup": sequential_ns as f64 / parallel_ns.max(1) as f64,
        "checksum": parallel_checksum,
    })
}

fn morsel_checksum(input: &[u64]) -> u64 {
    input.iter().fold(0u64, |total, value| {
        total.wrapping_add(value.rotate_left(17).wrapping_mul(0xbf58_476d_1ce4_e5b9))
    })
}

pub(super) fn benchmark_workers() -> NonZeroUsize {
    let available = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
    let Some(raw) = std::env::var(MORSEL_WORKERS_ENV).ok() else {
        return available.min(NonZeroUsize::new(4).unwrap());
    };
    let requested = raw
        .parse::<usize>()
        .ok()
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| panic!("{MORSEL_WORKERS_ENV} must be 4, 8, or 16"));
    assert!(
        matches!(requested.get(), 4 | 8 | 16),
        "{MORSEL_WORKERS_ENV} must be 4, 8, or 16"
    );
    assert!(
        requested <= available,
        "{MORSEL_WORKERS_ENV}={} exceeds available parallelism {}",
        requested,
        available
    );
    requested
}

pub(super) fn benchmark_production_rows(requested_workers: NonZeroUsize) -> usize {
    let default_rows = hawdb_executor::memory::DEFAULT_EXECUTION_BATCH_ROWS
        .saturating_mul(PRODUCTION_BATCHES_PER_MORSEL)
        .saturating_mul(DEFAULT_BENCH_MORSELS_PER_WORKER)
        .saturating_mul(requested_workers.get());
    let rows = match std::env::var(MORSEL_ROWS_ENV) {
        Ok(raw) => raw
            .parse::<usize>()
            .ok()
            .filter(|rows| *rows > 0)
            .unwrap_or_else(|| panic!("{MORSEL_ROWS_ENV} must be a positive integer")),
        Err(_) => default_rows,
    };
    let minimum_rows = hawdb_executor::memory::DEFAULT_EXECUTION_BATCH_ROWS
        .saturating_mul(PRODUCTION_BATCHES_PER_MORSEL)
        .saturating_mul(PRODUCTION_MIN_MORSELS_PER_WORKER)
        .saturating_mul(requested_workers.get());
    assert!(
        rows >= minimum_rows,
        "{MORSEL_ROWS_ENV}={rows} cannot activate {} workers with default morsel admission; use at least {minimum_rows}",
        requested_workers
    );
    rows
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Probe {
    pub(super) checksum: u64,
    pub(super) output_rows: usize,
    pub(super) fully_streamed: bool,
    pub(super) morsel_count: usize,
    pub(super) max_admitted_workers: usize,
    pub(super) peak_active_workers: usize,
    pub(super) peak_buffered_outputs: usize,
    pub(super) peak_buffered_output_bytes: usize,
    pub(super) peak_reorder_entries: usize,
    pub(super) query_memory_peak_bytes: usize,
    pub(super) query_memory_completion_bytes: usize,
    pub(super) spilled_bytes: u64,
    pub(super) spill_run_count: usize,
    pub(super) steady_resident_bytes: Option<u64>,
    pub(super) peak_resident_bytes: Option<u64>,
    pub(super) minor_page_faults: Option<u64>,
    pub(super) major_page_faults: Option<u64>,
}

pub(super) fn stream_probe(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &RuntimeTaskContext,
    memory: &ExecutionMemoryConfig,
) -> Probe {
    let mut checksum = 0u64;
    let mut output_rows = 0usize;
    let mut external = BenchmarkExternalRead;
    let report = execute_with_row_consumer_profile_and_external_and_context_and_memory(
        plan,
        catalog,
        store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &mut |row| {
            output_rows = output_rows.saturating_add(1);
            checksum = checksum.wrapping_add(super::output_row_score(&row));
            Ok(())
        },
        context,
        memory,
    )
    .expect("morsel production benchmark execution must succeed");
    let spilled_bytes = report
        .profile
        .blocking_operator_memory_reports
        .iter()
        .fold(0u64, |total, operator| {
            total.saturating_add(operator.spilled_bytes)
        });
    let spill_run_count = report
        .profile
        .blocking_operator_memory_reports
        .iter()
        .fold(0usize, |total, operator| {
            total.saturating_add(operator.spill_run_count)
        });
    let pipeline = report.profile.pipeline_memory_report;
    Probe {
        checksum,
        output_rows,
        fully_streamed: report.fully_streamed,
        morsel_count: pipeline.morsel_count,
        max_admitted_workers: pipeline.morsel_max_admitted_workers,
        peak_active_workers: pipeline.morsel_peak_active_workers,
        peak_buffered_outputs: pipeline.morsel_peak_buffered_outputs,
        peak_buffered_output_bytes: pipeline.morsel_peak_buffered_output_bytes,
        peak_reorder_entries: pipeline.morsel_peak_reorder_entries,
        query_memory_peak_bytes: pipeline.query_memory_peak_bytes,
        query_memory_completion_bytes: pipeline.query_memory_completion_bytes,
        spilled_bytes,
        spill_run_count,
        steady_resident_bytes: pipeline.steady_resident_bytes,
        peak_resident_bytes: pipeline.peak_resident_bytes,
        minor_page_faults: pipeline.minor_page_faults,
        major_page_faults: pipeline.major_page_faults,
    }
}

struct BenchmarkExternalRead;

impl ExternalReadOperator for BenchmarkExternalRead {
    fn execute_vector_seed(
        &mut self,
        _request: VectorSeedExecutionRequest<'_>,
    ) -> hawdb::Result<VectorSeedExecutionOutput> {
        Err(hawdb::HawDBError::Execution(
            "vector reads are outside the executor vectorization benchmark".to_string(),
        ))
    }
}

pub(super) fn percentile(samples: &[u128], percentile: usize) -> u128 {
    let index = samples
        .len()
        .saturating_sub(1)
        .saturating_mul(percentile)
        .div_ceil(100);
    samples[index.min(samples.len().saturating_sub(1))]
}
