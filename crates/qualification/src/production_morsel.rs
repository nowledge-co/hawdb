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

use super::{
    latency_percentiles, parameter_digest, runtime_report,
    validate_production_identity_for_current_target, LatencyPercentiles, MixedSoakRuntimeReport,
    ProductionGraphQualificationError,
};
use hawdb::{
    IoConcurrencyBudget, NowledgeGraphStatement, NowledgeMemEmbeddedStoreHandle,
    NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemReadOptions,
    ProductionEvidenceBinding, ProductionQualificationIdentity, QueryStreamReport,
    RuntimeCancellationToken, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
    StorageDeviceProfile, StorageResidencyMode,
};
use hawdb_query::QueryIdentity;
use serde::Serialize;
use std::collections::BTreeSet;
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub const PRODUCTION_MORSEL_PROFILE_PROTOCOL: &str = "hawdb-production-morsel-profile-v1";
pub const PRODUCTION_MORSEL_MATRIX_PROTOCOL: &str = "hawdb-production-morsel-matrix-v1";
pub const REQUIRED_PRODUCTION_MORSEL_WORKERS: [usize; 3] = [4, 8, 16];
const MIN_PRODUCTION_LATENCY_SAMPLES: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionMorselProfileConfig {
    pub open_options: NowledgeMemOpenOptions,
    pub runtime_governor_config: RuntimeGovernorConfig,
    pub statement: NowledgeGraphStatement,
    pub read_options: NowledgeMemReadOptions,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub expected_workers: usize,
    pub warmup_runs: usize,
    pub measurement_runs: usize,
    pub cancellation_start_timeout: Duration,
    pub max_cancellation_latency: Duration,
}

impl ProductionMorselProfileConfig {
    fn validate(&self) -> Result<(), ProductionGraphQualificationError> {
        if self.open_options.mode != NowledgeMemGraphMode::ShadowReadOnly {
            return Err(ProductionGraphQualificationError::new(
                "production morsel qualification requires shadow read-only mode",
            ));
        }
        let database_config = self.open_options.database_config.as_ref().ok_or_else(|| {
            ProductionGraphQualificationError::new(
                "production morsel qualification requires an explicit database config",
            )
        })?;
        if database_config.storage_residency_mode != StorageResidencyMode::Materialized {
            return Err(ProductionGraphQualificationError::new(
                "production morsel qualification requires materialized storage",
            ));
        }
        if !REQUIRED_PRODUCTION_MORSEL_WORKERS.contains(&self.expected_workers) {
            return Err(ProductionGraphQualificationError::new(
                "production morsel expected_workers must be 4, 8, or 16",
            ));
        }
        if self.measurement_runs == 0 {
            return Err(ProductionGraphQualificationError::new(
                "production morsel measurement_runs must be greater than zero",
            ));
        }
        if self.read_options.max_rows.is_none()
            || self.read_options.max_estimated_payload_bytes.is_none()
        {
            return Err(ProductionGraphQualificationError::new(
                "production morsel qualification requires explicit row and payload budgets",
            ));
        }
        if self.cancellation_start_timeout.is_zero() || self.max_cancellation_latency.is_zero() {
            return Err(ProductionGraphQualificationError::new(
                "production morsel cancellation timeouts must be greater than zero",
            ));
        }
        validate_production_identity_for_current_target(
            &self.evidence_binding,
            &self.expected_identity,
        )
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProductionMorselRuntimeShape {
    pub configured_cpu_slots: usize,
    pub effective_cpu_slots: usize,
    pub memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
    pub query_memory_budget_bytes: u64,
    pub batch_payload_budget_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProductionMorselExecutionReport {
    pub warmup_runs: usize,
    pub measurement_runs: usize,
    pub fully_streamed_runs: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub columnar_batches: usize,
    pub morsel_count: usize,
    pub morsel_max_admitted_workers: usize,
    pub morsel_peak_active_workers: usize,
    pub morsel_peak_buffered_outputs: usize,
    pub morsel_peak_buffered_output_bytes: usize,
    pub morsel_peak_reorder_entries: usize,
    pub query_memory_peak_bytes: usize,
    pub query_memory_completion_bytes: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
    pub rows_per_second: u64,
    pub latency: LatencyPercentiles,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionMorselCancellationReport {
    pub cancellation_observed: bool,
    pub latency_micros: u64,
    pub max_latency_micros: u64,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionMorselProfileReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub process_id: u32,
    pub expected_workers: usize,
    pub query_digest: String,
    pub parameter_digest: String,
    pub open_report: serde_json::Value,
    pub evidence_binding: ProductionEvidenceBinding,
    pub runtime_shape: ProductionMorselRuntimeShape,
    pub execution: ProductionMorselExecutionReport,
    pub cancellation: ProductionMorselCancellationReport,
    pub runtime: MixedSoakRuntimeReport,
}

impl ProductionMorselProfileReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_MORSEL_PROFILE_PROTOCOL,
            "evidence_kind": "representative_production_morsel_profile",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "process_id": self.process_id,
            "expected_workers": self.expected_workers,
            "query_identity": {
                "query_digest": self.query_digest,
                "parameter_digest": self.parameter_digest,
            },
            "open_report": self.open_report,
            "evidence_binding": self.evidence_binding.json(),
            "runtime_shape": self.runtime_shape,
            "execution": self.execution,
            "cancellation": self.cancellation,
            "runtime": self.runtime,
        })
    }
}

pub fn run_production_morsel_profile(
    config: ProductionMorselProfileConfig,
) -> Result<ProductionMorselProfileReport, ProductionGraphQualificationError> {
    config.validate()?;
    let execution_memory = &config
        .open_options
        .database_config
        .as_ref()
        .expect("validated production profile has a database config")
        .execution_memory;
    let query_memory_budget_bytes =
        u64::try_from(execution_memory.query_memory_bytes.get()).unwrap_or(u64::MAX);
    let batch_payload_budget_bytes =
        u64::try_from(execution_memory.batch_payload_bytes.get()).unwrap_or(u64::MAX);
    let query_identity = QueryIdentity::new("cypher", &config.statement.cypher);
    let parameter_digest = parameter_digest(&config.statement.parameters);
    let storage_io = IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::detect(
        &config.open_options.graph_path,
    ));
    let governor = RuntimeGovernor::detect(config.runtime_governor_config, storage_io);
    let (store, open_report) =
        NowledgeMemEmbeddedStoreHandle::open_with_options_and_runtime_governor(
            config.open_options.clone(),
            governor,
        )
        .map_err(ProductionGraphQualificationError::from_error)?;
    let graph_epoch = store
        .runtime_status()
        .map_err(ProductionGraphQualificationError::from_error)?
        .graph_commit_epoch;
    if graph_epoch != config.expected_identity.canonical_graph_commit_epoch {
        return Err(ProductionGraphQualificationError::new(format!(
            "qualification graph epoch {graph_epoch} does not match expected epoch {}",
            config.expected_identity.canonical_graph_commit_epoch
        )));
    }
    for _ in 0..config.warmup_runs {
        store
            .read_query_with_params_streaming_context(
                &config.statement.cypher,
                &config.statement.parameters,
                &config.read_options,
                &RuntimeTaskContext::default(),
                |_| Ok(()),
            )
            .map_err(ProductionGraphQualificationError::from_error)?;
    }
    let runtime_before = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let mut durations = Vec::with_capacity(config.measurement_runs);
    let mut queries = Vec::with_capacity(config.measurement_runs);
    for _ in 0..config.measurement_runs {
        let started = Instant::now();
        let query = store
            .read_query_with_params_streaming_context(
                &config.statement.cypher,
                &config.statement.parameters,
                &config.read_options,
                &RuntimeTaskContext::default(),
                |_| Ok(()),
            )
            .map_err(ProductionGraphQualificationError::from_error)?;
        durations.push(elapsed_micros(started));
        queries.push(query);
    }
    let cancellation = run_cancellation_probe(&store, &config)?;
    let runtime_after = store
        .runtime_governor_snapshot()
        .map_err(ProductionGraphQualificationError::from_error)?;
    let runtime_shape = ProductionMorselRuntimeShape {
        configured_cpu_slots: runtime_before.limits.configured_cpu_slots.get(),
        effective_cpu_slots: runtime_before.limits.effective_cpu_slots.get(),
        memory_budget_bytes: runtime_before.limits.memory_budget_bytes,
        result_budget_bytes: runtime_before.limits.result_budget_bytes,
        query_memory_budget_bytes,
        batch_payload_budget_bytes,
    };
    let execution = summarize_queries(config.warmup_runs, &queries, &durations);
    let runtime = runtime_report(runtime_before, runtime_after);
    let mut blocker_codes = Vec::new();
    if execution.measurement_runs < MIN_PRODUCTION_LATENCY_SAMPLES {
        blocker_codes.push("latency_sample_count_below_100".to_string());
    }
    if execution.warmup_runs < 3 {
        blocker_codes.push("warmup_sample_count_below_3".to_string());
    }
    if execution.fully_streamed_runs != execution.measurement_runs {
        blocker_codes.push("query_not_fully_streamed".to_string());
    }
    if execution.output_rows == 0 {
        blocker_codes.push("query_returned_no_rows".to_string());
    }
    if queries
        .windows(2)
        .any(|pair| pair[0].output_rows != pair[1].output_rows)
    {
        blocker_codes.push("query_output_cardinality_changed".to_string());
    }
    append_execution_shape_blockers(
        &mut blocker_codes,
        &execution,
        runtime_shape,
        config.expected_workers,
    );
    if !cancellation.cancellation_observed {
        blocker_codes.push("cancellation_not_observed".to_string());
    }
    if cancellation.latency_micros > cancellation.max_latency_micros {
        blocker_codes.push("cancellation_latency_exceeded".to_string());
    }
    if runtime.admissions_delta < config.measurement_runs.saturating_add(1) as u64 {
        blocker_codes.push("runtime_admission_not_observed_for_every_run".to_string());
    }
    if runtime.completions_delta < config.measurement_runs.saturating_add(1) as u64 {
        blocker_codes.push("runtime_completion_not_observed_for_every_run".to_string());
    }
    if runtime.cancellations_delta == 0 {
        blocker_codes.push("runtime_cancellation_not_recorded".to_string());
    }
    if runtime.admission_waits_delta != 0 || runtime.admission_rejections_delta != 0 {
        blocker_codes.push("foreground_admission_regression".to_string());
    }
    if runtime.final_active_foreground_tasks != 0
        || runtime.final_active_background_tasks != 0
        || runtime.final_active_blocking_tasks != 0
        || runtime.final_admitted_memory_bytes != 0
    {
        blocker_codes.push("runtime_permit_leak".to_string());
    }
    if runtime.final_overcommitted {
        blocker_codes.push("runtime_overcommitted".to_string());
    }
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(ProductionMorselProfileReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        process_id: std::process::id(),
        expected_workers: config.expected_workers,
        query_digest: query_identity.query_digest().to_string(),
        parameter_digest,
        open_report: open_report.json(),
        evidence_binding: config.evidence_binding,
        runtime_shape,
        execution,
        cancellation,
        runtime,
    })
}

fn run_cancellation_probe(
    store: &NowledgeMemEmbeddedStoreHandle,
    config: &ProductionMorselProfileConfig,
) -> Result<ProductionMorselCancellationReport, ProductionGraphQualificationError> {
    let token = RuntimeCancellationToken::new();
    let task_context = RuntimeTaskContext::without_deadline(token.clone());
    let worker_store = store.clone();
    let statement = config.statement.clone();
    let read_options = config.read_options.clone();
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (resume_sender, resume_receiver) = mpsc::sync_channel(1);
    let (outcome_sender, outcome_receiver) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let mut first_row = true;
        let result = worker_store.read_query_with_params_streaming_context(
            &statement.cypher,
            &statement.parameters,
            &read_options,
            &task_context,
            |_| {
                if first_row {
                    first_row = false;
                    started_sender
                        .send(())
                        .map_err(|error| hawdb::HawDBError::Execution(error.to_string()))?;
                    resume_receiver
                        .recv()
                        .map_err(|error| hawdb::HawDBError::Execution(error.to_string()))?;
                }
                Ok(())
            },
        );
        let outcome = match result {
            Err(error) if error.to_string().contains("cancelled") => Ok(()),
            Err(error) => Err(format!("unexpected_cancellation_error:{error}")),
            Ok(_) => Err("query_completed_without_cancellation".to_string()),
        };
        let _ = outcome_sender.send(outcome);
    });
    if started_receiver
        .recv_timeout(config.cancellation_start_timeout)
        .is_err()
    {
        token.cancel();
        let _ = resume_sender.send(());
        if outcome_receiver
            .recv_timeout(config.max_cancellation_latency)
            .is_ok()
        {
            let _ = worker.join();
        }
        return Ok(ProductionMorselCancellationReport {
            cancellation_observed: false,
            latency_micros: 0,
            max_latency_micros: duration_micros(config.max_cancellation_latency),
            error_code: Some("cancellation_probe_did_not_stream_a_row".to_string()),
        });
    }
    let cancelled_at = Instant::now();
    token.cancel();
    let _ = resume_sender.send(());
    let outcome = outcome_receiver
        .recv_timeout(config.max_cancellation_latency)
        .unwrap_or_else(|_| Err("cancellation_probe_timeout".to_string()));
    let latency_micros = elapsed_micros(cancelled_at);
    if !matches!(outcome, Err(ref code) if code == "cancellation_probe_timeout") {
        worker.join().map_err(|_| {
            ProductionGraphQualificationError::new("production morsel cancellation worker panicked")
        })?;
    }
    Ok(ProductionMorselCancellationReport {
        cancellation_observed: outcome.is_ok(),
        latency_micros,
        max_latency_micros: duration_micros(config.max_cancellation_latency),
        error_code: outcome.err(),
    })
}

fn summarize_queries(
    warmup_runs: usize,
    queries: &[QueryStreamReport],
    durations: &[u64],
) -> ProductionMorselExecutionReport {
    let mut report = ProductionMorselExecutionReport {
        warmup_runs,
        measurement_runs: queries.len(),
        latency: latency_percentiles(durations),
        ..ProductionMorselExecutionReport::default()
    };
    for query in queries {
        let pipeline = &query.execution_profile.pipeline_memory_report;
        report.fully_streamed_runs = report
            .fully_streamed_runs
            .saturating_add(usize::from(query.fully_streamed));
        report.output_rows = report.output_rows.saturating_add(query.output_rows);
        report.output_payload_bytes = report
            .output_payload_bytes
            .saturating_add(query.output_payload_bytes);
        report.intermediate_rows = report
            .intermediate_rows
            .saturating_add(pipeline.intermediate_rows);
        report.intermediate_payload_bytes = report
            .intermediate_payload_bytes
            .saturating_add(pipeline.intermediate_payload_bytes);
        report.columnar_batches = report
            .columnar_batches
            .saturating_add(pipeline.columnar_batches);
        report.morsel_count = report.morsel_count.saturating_add(pipeline.morsel_count);
        report.morsel_max_admitted_workers = report
            .morsel_max_admitted_workers
            .max(pipeline.morsel_max_admitted_workers);
        report.morsel_peak_active_workers = report
            .morsel_peak_active_workers
            .max(pipeline.morsel_peak_active_workers);
        report.morsel_peak_buffered_outputs = report
            .morsel_peak_buffered_outputs
            .max(pipeline.morsel_peak_buffered_outputs);
        report.morsel_peak_buffered_output_bytes = report
            .morsel_peak_buffered_output_bytes
            .max(pipeline.morsel_peak_buffered_output_bytes);
        report.morsel_peak_reorder_entries = report
            .morsel_peak_reorder_entries
            .max(pipeline.morsel_peak_reorder_entries);
        report.query_memory_peak_bytes = report
            .query_memory_peak_bytes
            .max(pipeline.query_memory_peak_bytes);
        report.query_memory_completion_bytes = report
            .query_memory_completion_bytes
            .max(pipeline.query_memory_completion_bytes);
        for operator in &query.execution_profile.blocking_operator_memory_reports {
            report.spilled_bytes = report.spilled_bytes.saturating_add(operator.spilled_bytes);
            report.spill_run_count = report
                .spill_run_count
                .saturating_add(operator.spill_run_count);
        }
        report.steady_resident_bytes =
            max_option(report.steady_resident_bytes, pipeline.steady_resident_bytes);
        report.peak_resident_bytes =
            max_option(report.peak_resident_bytes, pipeline.peak_resident_bytes);
        report.total_page_faults = sum_option(report.total_page_faults, pipeline.total_page_faults);
        report.minor_page_faults = sum_option(report.minor_page_faults, pipeline.minor_page_faults);
        report.major_page_faults = sum_option(report.major_page_faults, pipeline.major_page_faults);
    }
    let elapsed = durations
        .iter()
        .copied()
        .fold(0u64, u64::saturating_add)
        .max(1);
    report.rows_per_second = u64::try_from(
        (report.output_rows as u128)
            .saturating_mul(1_000_000)
            .checked_div(u128::from(elapsed))
            .unwrap_or_default(),
    )
    .unwrap_or(u64::MAX);
    report
}

fn append_execution_shape_blockers(
    blocker_codes: &mut Vec<String>,
    execution: &ProductionMorselExecutionReport,
    runtime_shape: ProductionMorselRuntimeShape,
    expected_workers: usize,
) {
    if execution.columnar_batches == 0 || execution.morsel_count == 0 {
        blocker_codes.push("columnar_morsel_fragment_not_observed".to_string());
    }
    if execution.morsel_max_admitted_workers != expected_workers {
        blocker_codes.push("morsel_admitted_worker_count_mismatch".to_string());
    }
    if execution.morsel_peak_active_workers != expected_workers {
        blocker_codes.push("morsel_active_worker_count_mismatch".to_string());
    }
    if execution.morsel_peak_buffered_outputs > expected_workers {
        blocker_codes.push("morsel_output_window_exceeded".to_string());
    }
    if execution.morsel_peak_reorder_entries > expected_workers {
        blocker_codes.push("morsel_reorder_window_exceeded".to_string());
    }
    let output_window_budget = runtime_shape
        .batch_payload_budget_bytes
        .saturating_mul(u64::try_from(expected_workers).unwrap_or(u64::MAX));
    if u64::try_from(execution.morsel_peak_buffered_output_bytes).unwrap_or(u64::MAX)
        > output_window_budget
    {
        blocker_codes.push("morsel_output_bytes_exceeded".to_string());
    }
    if u64::try_from(execution.query_memory_peak_bytes).unwrap_or(u64::MAX)
        > runtime_shape.query_memory_budget_bytes
    {
        blocker_codes.push("query_memory_budget_exceeded".to_string());
    }
    if execution.query_memory_completion_bytes != 0 {
        blocker_codes.push("query_memory_not_released".to_string());
    }
    if execution.spilled_bytes != 0 || execution.spill_run_count != 0 {
        blocker_codes.push("morsel_pipeline_spilled".to_string());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionMorselMatrixPolicy {
    pub min_throughput_gain_per_million: u32,
    pub max_p99_regression_per_million: u32,
    pub max_peak_rss_regression_per_million: u32,
    pub max_cancellation_latency_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionMorselMatrixSample {
    pub workers: usize,
    pub rows_per_second: u64,
    pub p99_micros: u64,
    pub peak_resident_bytes: Option<u64>,
    pub cancellation_latency_micros: u64,
    pub morsel_peak_buffered_outputs: usize,
    pub morsel_peak_buffered_output_bytes: usize,
    pub morsel_peak_reorder_entries: usize,
    pub query_memory_peak_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionMorselMatrixReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: Option<ProductionEvidenceBinding>,
    pub query_digest: Option<String>,
    pub parameter_digest: Option<String>,
    pub policy: ProductionMorselMatrixPolicy,
    pub samples: Vec<ProductionMorselMatrixSample>,
}

impl ProductionMorselMatrixReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_MORSEL_MATRIX_PROTOCOL,
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "evidence_binding": self.evidence_binding.as_ref().map(ProductionEvidenceBinding::json),
            "query_identity": {
                "query_digest": self.query_digest,
                "parameter_digest": self.parameter_digest,
            },
            "policy": self.policy,
            "samples": self.samples,
        })
    }
}

pub fn evaluate_production_morsel_matrix(
    reports: &[ProductionMorselProfileReport],
    policy: ProductionMorselMatrixPolicy,
) -> ProductionMorselMatrixReport {
    let mut blocker_codes = Vec::new();
    let mut ordered = reports.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|report| report.expected_workers);
    let workers = ordered
        .iter()
        .map(|report| report.expected_workers)
        .collect::<Vec<_>>();
    if workers != REQUIRED_PRODUCTION_MORSEL_WORKERS {
        blocker_codes.push("required_worker_matrix_missing".to_string());
    }
    let evidence_binding = ordered
        .first()
        .map(|report| report.evidence_binding.clone());
    let query_digest = ordered.first().map(|report| report.query_digest.clone());
    let parameter_digest = ordered
        .first()
        .map(|report| report.parameter_digest.clone());
    if ordered
        .iter()
        .map(|report| report.process_id)
        .collect::<BTreeSet<_>>()
        .len()
        != ordered.len()
    {
        blocker_codes.push("profiles_not_process_isolated".to_string());
    }
    for report in &ordered {
        if !report.ready {
            blocker_codes.push(format!(
                "worker_{}_profile_not_ready",
                report.expected_workers
            ));
        }
        if evidence_binding.as_ref().map(|binding| &binding.identity)
            != Some(&report.evidence_binding.identity)
        {
            blocker_codes.push("evidence_identity_mismatch".to_string());
        }
        if query_digest.as_ref() != Some(&report.query_digest)
            || parameter_digest.as_ref() != Some(&report.parameter_digest)
        {
            blocker_codes.push("query_identity_mismatch".to_string());
        }
        if report.cancellation.latency_micros > policy.max_cancellation_latency_micros {
            blocker_codes.push(format!(
                "worker_{}_cancellation_latency_regression",
                report.expected_workers
            ));
        }
    }
    for pair in ordered.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if !ratio_at_least(
            current.execution.rows_per_second,
            previous.execution.rows_per_second,
            policy.min_throughput_gain_per_million,
        ) {
            blocker_codes.push(format!(
                "worker_{}_throughput_did_not_improve",
                current.expected_workers
            ));
        }
        if !regression_within(
            current.execution.latency.p99_micros,
            previous.execution.latency.p99_micros,
            policy.max_p99_regression_per_million,
        ) {
            blocker_codes.push(format!(
                "worker_{}_p99_regression",
                current.expected_workers
            ));
        }
        match (
            previous.execution.peak_resident_bytes,
            current.execution.peak_resident_bytes,
        ) {
            (Some(previous), Some(current))
                if !regression_within(
                    current,
                    previous,
                    policy.max_peak_rss_regression_per_million,
                ) =>
            {
                blocker_codes.push(format!(
                    "worker_{}_peak_rss_regression",
                    pair[1].expected_workers
                ));
            }
            (None, _) | (_, None) => blocker_codes.push("peak_rss_metric_unavailable".to_string()),
            _ => {}
        }
    }
    blocker_codes.sort();
    blocker_codes.dedup();
    let samples = ordered
        .into_iter()
        .map(|report| ProductionMorselMatrixSample {
            workers: report.expected_workers,
            rows_per_second: report.execution.rows_per_second,
            p99_micros: report.execution.latency.p99_micros,
            peak_resident_bytes: report.execution.peak_resident_bytes,
            cancellation_latency_micros: report.cancellation.latency_micros,
            morsel_peak_buffered_outputs: report.execution.morsel_peak_buffered_outputs,
            morsel_peak_buffered_output_bytes: report.execution.morsel_peak_buffered_output_bytes,
            morsel_peak_reorder_entries: report.execution.morsel_peak_reorder_entries,
            query_memory_peak_bytes: report.execution.query_memory_peak_bytes,
        })
        .collect();

    ProductionMorselMatrixReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        evidence_binding,
        query_digest,
        parameter_digest,
        policy,
        samples,
    }
}

fn ratio_at_least(current: u64, previous: u64, gain_per_million: u32) -> bool {
    (u128::from(current)).saturating_mul(1_000_000)
        >= (u128::from(previous))
            .saturating_mul(1_000_000u128.saturating_add(u128::from(gain_per_million)))
}

fn regression_within(current: u64, previous: u64, tolerance_per_million: u32) -> bool {
    (u128::from(current)).saturating_mul(1_000_000)
        <= (u128::from(previous))
            .saturating_mul(1_000_000u128.saturating_add(u128::from(tolerance_per_million)))
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn sum_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value),
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::{
        Database, DatabaseConfig, ProductionQualificationIdentity, Value,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn identity() -> ProductionQualificationIdentity {
        ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "production".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn report(
        workers: usize,
        throughput: u64,
        p99: u64,
        rss: u64,
    ) -> ProductionMorselProfileReport {
        let identity = identity();
        ProductionMorselProfileReport {
            ready: true,
            blocker_codes: Vec::new(),
            process_id: workers as u32,
            expected_workers: workers,
            query_digest: "q1:query".to_string(),
            parameter_digest: "sha256:parameters".to_string(),
            open_report: serde_json::json!({}),
            evidence_binding: ProductionEvidenceBinding {
                identity,
                generated_at_unix_seconds: 1,
            },
            runtime_shape: ProductionMorselRuntimeShape::default(),
            execution: ProductionMorselExecutionReport {
                measurement_runs: 100,
                fully_streamed_runs: 100,
                output_rows: 1000,
                morsel_max_admitted_workers: workers,
                morsel_peak_active_workers: workers,
                peak_resident_bytes: Some(rss),
                rows_per_second: throughput,
                latency: LatencyPercentiles {
                    sample_count: 100,
                    p99_micros: p99,
                    ..LatencyPercentiles::default()
                },
                ..ProductionMorselExecutionReport::default()
            },
            cancellation: ProductionMorselCancellationReport {
                cancellation_observed: true,
                latency_micros: 100,
                max_latency_micros: 1_000,
                error_code: None,
            },
            runtime: MixedSoakRuntimeReport {
                admissions_delta: 101,
                admission_waits_delta: 0,
                admission_rejections_delta: 0,
                completions_delta: 101,
                cancellations_delta: 1,
                deadline_exceeded_delta: 0,
                final_active_foreground_tasks: 0,
                final_active_background_tasks: 0,
                final_active_blocking_tasks: 0,
                final_admitted_memory_bytes: 0,
                final_overcommitted: false,
            },
        }
    }

    #[test]
    fn matrix_accepts_bound_scaling_evidence() {
        let reports = [
            report(4, 100, 1000, 1000),
            report(8, 130, 900, 1050),
            report(16, 170, 850, 1100),
        ];
        let matrix = evaluate_production_morsel_matrix(
            &reports,
            ProductionMorselMatrixPolicy {
                min_throughput_gain_per_million: 100_000,
                max_p99_regression_per_million: 50_000,
                max_peak_rss_regression_per_million: 100_000,
                max_cancellation_latency_micros: 1_000,
            },
        );

        assert!(
            matrix.ready,
            "unexpected blockers: {:?}",
            matrix.blocker_codes
        );
        assert_eq!(matrix.samples.len(), 3);
    }

    #[test]
    fn matrix_rejects_missing_workers_and_regressions() {
        let reports = [report(4, 100, 1000, 1000), report(16, 90, 1200, 1400)];
        let matrix = evaluate_production_morsel_matrix(
            &reports,
            ProductionMorselMatrixPolicy {
                min_throughput_gain_per_million: 0,
                max_p99_regression_per_million: 0,
                max_peak_rss_regression_per_million: 0,
                max_cancellation_latency_micros: 1_000,
            },
        );

        assert!(!matrix.ready);
        assert!(matrix
            .blocker_codes
            .iter()
            .any(|code| code == "required_worker_matrix_missing"));
        assert!(matrix
            .blocker_codes
            .iter()
            .any(|code| code == "worker_16_throughput_did_not_improve"));
    }

    #[test]
    fn execution_shape_rejects_unbounded_or_replayed_morsel_evidence() {
        let runtime_shape = ProductionMorselRuntimeShape {
            query_memory_budget_bytes: 1_024,
            batch_payload_budget_bytes: 100,
            ..ProductionMorselRuntimeShape::default()
        };
        let execution = ProductionMorselExecutionReport {
            columnar_batches: 1,
            morsel_count: 1,
            morsel_max_admitted_workers: 4,
            morsel_peak_active_workers: 4,
            morsel_peak_buffered_outputs: 5,
            morsel_peak_buffered_output_bytes: 401,
            morsel_peak_reorder_entries: 5,
            query_memory_peak_bytes: 1_025,
            query_memory_completion_bytes: 1,
            spilled_bytes: 1,
            spill_run_count: 1,
            ..ProductionMorselExecutionReport::default()
        };
        let mut blockers = Vec::new();

        append_execution_shape_blockers(&mut blockers, &execution, runtime_shape, 4);

        assert_eq!(
            blockers,
            [
                "morsel_output_window_exceeded",
                "morsel_reorder_window_exceeded",
                "morsel_output_bytes_exceeded",
                "query_memory_budget_exceeded",
                "query_memory_not_released",
                "morsel_pipeline_spilled",
            ]
        );
    }

    #[test]
    fn profile_observes_in_flight_cancellation_through_the_mem_handle() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "hawdb-production-morsel-{}-{id}",
            std::process::id()
        ));
        let graph_path = root.join("database");
        let database_config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::Materialized,
            max_read_result_rows: Some(2048),
            max_read_result_payload_bytes: Some(4 * 1024 * 1024),
            ..DatabaseConfig::default()
        };
        let graph_commit_epoch = {
            let mut database = Database::open_with_config(&graph_path, database_config.clone())
                .expect("fixture database should open");
            let mut transaction = database.begin_transaction();
            for row in 0..1024 {
                transaction
                    .query_with_params(
                        "CREATE (:Memory {id: $id})",
                        &BTreeMap::from([("id".to_string(), Value::Int(row))]),
                    )
                    .expect("fixture row should be inserted");
            }
            transaction.commit().expect("fixture commit should succeed");
            database
                .checkpoint()
                .expect("fixture checkpoint should succeed");
            database.commit_epoch()
        };
        let mut qualification_identity = identity();
        qualification_identity.canonical_graph_commit_epoch = graph_commit_epoch;
        let report = run_production_morsel_profile(ProductionMorselProfileConfig {
            open_options: NowledgeMemOpenOptions::graph_only(
                &graph_path,
                NowledgeMemGraphMode::ShadowReadOnly,
            )
            .with_database_config(database_config),
            runtime_governor_config: RuntimeGovernorConfig {
                cpu_slot_limit: NonZeroUsize::new(4),
                foreground_task_limit: NonZeroUsize::new(4),
                background_task_limit: NonZeroUsize::new(1),
                blocking_task_limit: NonZeroUsize::new(1),
                memory_budget_bytes: Some(64 * 1024 * 1024),
                result_budget_bytes: 4 * 1024 * 1024,
                ..RuntimeGovernorConfig::shared_host()
            },
            statement: NowledgeGraphStatement {
                cypher: "MATCH (m:Memory) RETURN m.id AS memory_id".to_string(),
                parameters: BTreeMap::new(),
            },
            read_options: NowledgeMemReadOptions {
                max_rows: Some(2048),
                max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            },
            evidence_binding: ProductionEvidenceBinding {
                identity: qualification_identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity: qualification_identity,
            expected_workers: 4,
            warmup_runs: 0,
            measurement_runs: 1,
            cancellation_start_timeout: Duration::from_secs(5),
            max_cancellation_latency: Duration::from_secs(5),
        })
        .expect("morsel profile should complete");

        assert!(report.cancellation.cancellation_observed);
        assert_eq!(report.runtime.cancellations_delta, 1);
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "latency_sample_count_below_100"));

        drop(report);
        std::fs::remove_dir_all(root).expect("fixture should be removable");
    }
}
