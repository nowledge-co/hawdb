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

//! Typed revision-bound qualification workloads for CI and production replicas.

#![forbid(unsafe_code)]

mod content_store_memory_profile;
mod content_store_row_pages;
mod content_store_sql_corpus;
mod evidence_digest;
mod production_blocking;
mod production_graph;
mod production_graph_matrix;
mod production_morsel;
mod production_search;
mod production_vector;
mod release_bundle;

pub use content_store_memory_profile::*;
pub use content_store_row_pages::*;
pub use content_store_sql_corpus::*;
pub use production_blocking::*;
pub use production_graph::*;
pub use production_graph_matrix::*;
pub use production_morsel::*;
pub use production_search::*;
pub use production_vector::*;
pub use release_bundle::*;

use hawdb::executor::ExecutionMemoryConfig;
use hawdb::store::MutationLimits;
use hawdb::{
    DatabaseConfig, DurabilityPolicy, HawDBEmbedded, HawDBEmbeddedOpenOptions, HawDBTokioEmbedded,
    ProcessMemoryProfile, ProcessMemorySnapshot, QueryStreamReport, RuntimeGovernorConfig,
    RuntimeTaskContext, RuntimeWorkRequest, StorageResidencyMode, StorageResidencyReport, Value,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

pub const MIXED_SOAK_PROTOCOL: &str = "hawdb-mixed-runtime-soak-v1";
pub const MIXED_SOAK_WORKLOAD: &str =
    "foreground-point-read+background-distinct-cartesian+background-checkpoint-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedSoakConfig {
    pub database_path: PathBuf,
    pub source_revision: String,
    pub dataset_id: String,
    pub node_count: usize,
    pub payload_bytes: usize,
    pub foreground_workers: usize,
    pub foreground_rounds_per_worker: usize,
    pub background_rounds: usize,
    pub segment_cache_bytes: u64,
    pub runtime_memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
    pub blocking_operator_bytes: usize,
    pub task_timeout: Duration,
}

impl MixedSoakConfig {
    pub fn scheduled(
        database_path: impl Into<PathBuf>,
        source_revision: impl Into<String>,
    ) -> Self {
        Self {
            database_path: database_path.into(),
            source_revision: source_revision.into(),
            dataset_id: "scheduled-synthetic-v1".to_string(),
            node_count: 4_096,
            payload_bytes: 4_096,
            foreground_workers: 4,
            foreground_rounds_per_worker: 8,
            background_rounds: 2,
            segment_cache_bytes: 1024 * 1024,
            runtime_memory_budget_bytes: 4 * 1024 * 1024,
            result_budget_bytes: 512 * 1024,
            blocking_operator_bytes: 64 * 1024,
            task_timeout: Duration::from_secs(300),
        }
    }

    fn validate(&self) -> Result<(), MixedSoakError> {
        let nonzero = [
            ("node_count", self.node_count as u64),
            ("payload_bytes", self.payload_bytes as u64),
            ("foreground_workers", self.foreground_workers as u64),
            (
                "foreground_rounds_per_worker",
                self.foreground_rounds_per_worker as u64,
            ),
            ("background_rounds", self.background_rounds as u64),
            ("segment_cache_bytes", self.segment_cache_bytes),
            (
                "runtime_memory_budget_bytes",
                self.runtime_memory_budget_bytes,
            ),
            ("result_budget_bytes", self.result_budget_bytes),
            (
                "blocking_operator_bytes",
                self.blocking_operator_bytes as u64,
            ),
            ("task_timeout_millis", duration_millis(self.task_timeout)),
        ];
        if let Some((name, _)) = nonzero.into_iter().find(|(_, value)| *value == 0) {
            return Err(MixedSoakError::new(format!(
                "mixed soak {name} must be greater than zero"
            )));
        }
        if self.source_revision.trim().is_empty() {
            return Err(MixedSoakError::new(
                "mixed soak source_revision must not be empty",
            ));
        }
        if self.dataset_id.trim().is_empty() {
            return Err(MixedSoakError::new(
                "mixed soak dataset_id must not be empty",
            ));
        }
        if self.database_path.exists() {
            return Err(MixedSoakError::new(format!(
                "mixed soak database path already exists: {}",
                self.database_path.display()
            )));
        }
        let raw_bytes = raw_dataset_bytes(self);
        if raw_bytes <= self.runtime_memory_budget_bytes {
            return Err(MixedSoakError::new(format!(
                "mixed soak raw dataset bytes {raw_bytes} must exceed runtime memory budget {}",
                self.runtime_memory_budget_bytes
            )));
        }
        if self.result_budget_bytes >= self.runtime_memory_budget_bytes {
            return Err(MixedSoakError::new(
                "mixed soak result budget must be smaller than the runtime memory budget",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedSoakError {
    message: String,
}

impl MixedSoakError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for MixedSoakError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for MixedSoakError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MixedSoakIdentity {
    pub source_revision: String,
    pub target_os: String,
    pub target_arch: String,
    pub dataset_id: String,
    pub dataset_fingerprint: String,
    pub configuration_digest: String,
    pub graph_commit_epoch: u64,
    pub workload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MixedSoakConfigurationReport {
    pub node_count: usize,
    pub payload_bytes: usize,
    pub raw_dataset_bytes: u64,
    pub foreground_workers: usize,
    pub foreground_rounds_per_worker: usize,
    pub background_rounds: usize,
    pub segment_cache_bytes: u64,
    pub runtime_memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
    pub blocking_operator_bytes: usize,
    pub task_timeout_millis: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LatencyPercentiles {
    pub sample_count: usize,
    pub min_micros: u64,
    pub p50_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MixedSoakExecutionReport {
    pub query_count: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub blocking_operator_count: usize,
    pub peak_tracked_operator_bytes: usize,
    pub max_spill_bytes_per_query: u64,
    pub max_spill_runs_per_query: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
    pub latency: LatencyPercentiles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MixedSoakProcessReport {
    pub resident_memory_supported: bool,
    pub total_page_faults_supported: bool,
    pub split_page_faults_supported: bool,
    pub start_resident_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub steady_resident_growth_bytes: u64,
    pub lifetime_peak_resident_growth_bytes: u64,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MixedSoakRuntimeReport {
    pub admissions_delta: u64,
    pub admission_waits_delta: u64,
    pub admission_rejections_delta: u64,
    pub completions_delta: u64,
    pub cancellations_delta: u64,
    pub deadline_exceeded_delta: u64,
    pub final_active_foreground_tasks: usize,
    pub final_active_background_tasks: usize,
    pub final_active_blocking_tasks: usize,
    pub final_admitted_memory_bytes: u64,
    pub final_overcommitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MixedSoakStorageReport {
    pub out_of_core: bool,
    pub canonical_generation_before: Option<u64>,
    pub canonical_generation_after: Option<u64>,
    pub canonical_artifact_bytes_before: u64,
    pub canonical_artifact_bytes_after: u64,
    pub canonical_node_count_after: u64,
    pub canonical_exceeds_cache: bool,
    pub raw_dataset_exceeds_runtime_memory: bool,
    pub segment_cache_capacity_bytes: u64,
    pub segment_cache_resident_bytes: u64,
    pub segment_cache_hits_delta: u64,
    pub segment_cache_misses_delta: u64,
    pub segment_cache_evictions_delta: u64,
    pub segment_cache_admission_rejections_delta: u64,
    pub segment_cache_digest_mismatches_delta: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MixedSoakCheckpointReport {
    pub completed: bool,
    pub latency_micros: u64,
    pub graph_commit_epoch_before: u64,
    pub graph_commit_epoch_after: u64,
    pub canonical_generation_advanced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MixedSoakReport {
    pub protocol: &'static str,
    pub evidence_kind: &'static str,
    pub fixture_setup_path: &'static str,
    pub production_eligible: bool,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub errors: Vec<String>,
    pub identity: MixedSoakIdentity,
    pub configuration: MixedSoakConfigurationReport,
    pub foreground: MixedSoakExecutionReport,
    pub background: MixedSoakExecutionReport,
    pub checkpoint: MixedSoakCheckpointReport,
    pub process: MixedSoakProcessReport,
    pub runtime: MixedSoakRuntimeReport,
    pub storage: MixedSoakStorageReport,
}

impl MixedSoakReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("mixed soak report serialization should succeed")
    }
}

pub fn run_mixed_soak(config: &MixedSoakConfig) -> Result<MixedSoakReport, MixedSoakError> {
    config.validate()?;
    let configuration = configuration_report(config);
    let configuration_digest = stable_digest(&serde_json::to_vec(&configuration).map_err(error)?);
    let (prepared_storage, prepared_epoch, dataset_fingerprint) = prepare_fixture(config)?;
    let options = embedded_options(config);
    let database = HawDBTokioEmbedded::open_owned(options).map_err(error)?;
    let start_process = ProcessMemorySnapshot::capture().map_err(error)?;
    let runtime_before = database.runtime_snapshot();
    let storage_before =
        database.with_embedded(|embedded| embedded.database().storage_residency_report());

    let runtime = database.runtime().clone();
    let workload_database = database.clone();
    let workload_config = config.clone();
    let outcomes = runtime
        .block_on(async move { run_concurrent_workload(workload_database, workload_config).await })
        .map_err(error)?;

    let end_process = ProcessMemorySnapshot::capture().map_err(error)?;
    let process = process_report(ProcessMemoryProfile::between(start_process, end_process));
    let runtime_after = database.runtime_snapshot();
    let (storage_after, final_epoch) = database.with_embedded(|embedded| {
        (
            embedded.database().storage_residency_report(),
            embedded.database().commit_epoch(),
        )
    });
    let foreground = aggregate_execution(&outcomes.foreground_durations, &outcomes.foreground);
    let background = aggregate_execution(&outcomes.background_durations, &outcomes.background);
    let runtime = runtime_report(runtime_before, runtime_after);
    let storage = storage_report(config, &storage_before, &storage_after, &prepared_storage);
    let checkpoint = MixedSoakCheckpointReport {
        completed: outcomes.checkpoint_completed,
        latency_micros: outcomes.checkpoint_latency_micros,
        graph_commit_epoch_before: prepared_epoch,
        graph_commit_epoch_after: final_epoch,
        canonical_generation_advanced: storage_after.canonical_generation
            > prepared_storage.canonical_generation,
    };
    let mut blocker_codes = Vec::new();
    if !storage.out_of_core {
        blocker_codes.push("storage_not_out_of_core".to_string());
    }
    if !storage.canonical_exceeds_cache {
        blocker_codes.push("canonical_artifact_does_not_exceed_cache".to_string());
    }
    if !storage.raw_dataset_exceeds_runtime_memory {
        blocker_codes.push("raw_dataset_does_not_exceed_runtime_memory".to_string());
    }
    let expected_foreground = config
        .foreground_workers
        .saturating_mul(config.foreground_rounds_per_worker);
    if foreground.query_count != expected_foreground {
        blocker_codes.push("foreground_query_count_mismatch".to_string());
    }
    if background.query_count != config.background_rounds {
        blocker_codes.push("background_query_count_mismatch".to_string());
    }
    if background.blocking_operator_count == 0 {
        blocker_codes.push("background_blocking_operator_not_observed".to_string());
    }
    if background.spilled_bytes == 0 || background.spill_run_count == 0 {
        blocker_codes.push("background_spill_not_observed".to_string());
    }
    if !checkpoint.completed {
        blocker_codes.push("checkpoint_not_completed".to_string());
    }
    if !checkpoint.canonical_generation_advanced {
        blocker_codes.push("checkpoint_generation_not_advanced".to_string());
    }
    if !process.resident_memory_supported {
        blocker_codes.push("resident_memory_metric_unavailable".to_string());
    }
    if !process.total_page_faults_supported {
        blocker_codes.push("total_page_fault_metric_unavailable".to_string());
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
    if !outcomes.errors.is_empty() {
        blocker_codes.push("workload_error".to_string());
    }
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(MixedSoakReport {
        protocol: MIXED_SOAK_PROTOCOL,
        evidence_kind: "synthetic_scheduled_soak",
        fixture_setup_path: "controlled_raw_fixture",
        production_eligible: false,
        ready: blocker_codes.is_empty(),
        blocker_codes,
        errors: outcomes.errors,
        identity: MixedSoakIdentity {
            source_revision: config.source_revision.clone(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            dataset_id: config.dataset_id.clone(),
            dataset_fingerprint,
            configuration_digest,
            graph_commit_epoch: final_epoch,
            workload: MIXED_SOAK_WORKLOAD.to_string(),
        },
        configuration,
        foreground,
        background,
        checkpoint,
        process,
        runtime,
        storage,
    })
}

#[derive(Default)]
struct WorkloadOutcomes {
    foreground: Vec<QueryStreamReport>,
    foreground_durations: Vec<u64>,
    background: Vec<QueryStreamReport>,
    background_durations: Vec<u64>,
    checkpoint_completed: bool,
    checkpoint_latency_micros: u64,
    errors: Vec<String>,
}

#[derive(Default)]
struct QueryTaskOutcome {
    reports: Vec<QueryStreamReport>,
    durations: Vec<u64>,
    error: Option<String>,
}

fn merge_query_task_outcome(
    reports: &mut Vec<QueryStreamReport>,
    durations: &mut Vec<u64>,
    errors: &mut Vec<String>,
    outcome: QueryTaskOutcome,
) {
    reports.extend(outcome.reports);
    durations.extend(outcome.durations);
    if let Some(error) = outcome.error {
        errors.push(error);
    }
}

async fn run_concurrent_workload(
    database: HawDBTokioEmbedded,
    config: MixedSoakConfig,
) -> WorkloadOutcomes {
    let participant_count = config.foreground_workers.saturating_add(2);
    let barrier = Arc::new(Barrier::new(participant_count));
    let mut foreground_handles = Vec::with_capacity(config.foreground_workers);
    for worker in 0..config.foreground_workers {
        let database = database.clone();
        let barrier = Arc::clone(&barrier);
        let config = config.clone();
        foreground_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut outcome = QueryTaskOutcome {
                reports: Vec::with_capacity(config.foreground_rounds_per_worker),
                durations: Vec::with_capacity(config.foreground_rounds_per_worker),
                ..QueryTaskOutcome::default()
            };
            for round in 0..config.foreground_rounds_per_worker {
                let id = worker
                    .saturating_mul(config.foreground_rounds_per_worker)
                    .saturating_add(round)
                    % config.node_count;
                let mut parameters = BTreeMap::new();
                parameters.insert("id".to_string(), Value::Int(saturating_i64(id)));
                let started = Instant::now();
                let report = match stream_query(
                    &database,
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.body AS body",
                    parameters,
                    config.task_timeout,
                )
                .await
                {
                    Ok(report) => report,
                    Err(error) => {
                        outcome.error = Some(format!(
                            "foreground worker {worker} round {round} node {id}: {error}"
                        ));
                        return outcome;
                    }
                };
                outcome.durations.push(elapsed_micros(started));
                outcome.reports.push(report);
            }
            outcome
        }));
    }

    let background_database = database.clone();
    let background_barrier = Arc::clone(&barrier);
    let background_config = config.clone();
    let background_handle = tokio::spawn(async move {
        background_barrier.wait().await;
        let mut outcome = QueryTaskOutcome {
            reports: Vec::with_capacity(background_config.background_rounds),
            durations: Vec::with_capacity(background_config.background_rounds),
            ..QueryTaskOutcome::default()
        };
        for round in 0..background_config.background_rounds {
            let started = Instant::now();
            let cypher = if round % 2 == 0 {
                "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
                 MATCH (m:Memory) RETURN DISTINCT m.bucket AS bucket"
            } else {
                "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
                 MATCH (a:Memory), (b:Memory) \
                 WHERE a.id < 32 AND b.id < 32 \
                 RETURN a.id AS left_id, b.id AS right_id"
            };
            let report = match stream_query(
                &background_database,
                cypher,
                BTreeMap::new(),
                background_config.task_timeout,
            )
            .await
            {
                Ok(report) => report,
                Err(error) => {
                    outcome.error = Some(format!("background round {round}: {error}"));
                    return outcome;
                }
            };
            outcome.durations.push(elapsed_micros(started));
            outcome.reports.push(report);
        }
        outcome
    });

    let checkpoint_database = database.clone();
    let checkpoint_barrier = Arc::clone(&barrier);
    let checkpoint_config = config.clone();
    let checkpoint_handle = tokio::spawn(async move {
        checkpoint_barrier.wait().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut parameters = BTreeMap::new();
        parameters.insert(
            "id".to_string(),
            Value::String("scheduled-soak-marker".to_string()),
        );
        parameters.insert(
            "body".to_string(),
            Value::String("checkpoint mutation".to_string()),
        );
        checkpoint_database
            .query_with_request(
                "CREATE (:SoakMarker {id: $id, body: $body})",
                parameters,
                RuntimeWorkRequest::background_maintenance(1024 * 1024),
                RuntimeTaskContext::with_timeout(checkpoint_config.task_timeout),
            )
            .await
            .map_err(error)?;
        let started = Instant::now();
        checkpoint_database
            .query_with_request(
                "CHECKPOINT",
                BTreeMap::new(),
                RuntimeWorkRequest::background_maintenance(1024 * 1024),
                RuntimeTaskContext::with_timeout(checkpoint_config.task_timeout),
            )
            .await
            .map_err(error)?;
        Ok::<_, MixedSoakError>(elapsed_micros(started))
    });

    let mut outcomes = WorkloadOutcomes::default();
    for handle in foreground_handles {
        match handle.await {
            Ok(outcome) => merge_query_task_outcome(
                &mut outcomes.foreground,
                &mut outcomes.foreground_durations,
                &mut outcomes.errors,
                outcome,
            ),
            Err(error) => outcomes.errors.push(format!("foreground join: {error}")),
        }
    }
    match background_handle.await {
        Ok(outcome) => merge_query_task_outcome(
            &mut outcomes.background,
            &mut outcomes.background_durations,
            &mut outcomes.errors,
            outcome,
        ),
        Err(error) => outcomes.errors.push(format!("background join: {error}")),
    }
    match checkpoint_handle.await {
        Ok(Ok(duration)) => {
            outcomes.checkpoint_completed = true;
            outcomes.checkpoint_latency_micros = duration;
        }
        Ok(Err(error)) => outcomes.errors.push(format!("checkpoint: {error}")),
        Err(error) => outcomes.errors.push(format!("checkpoint join: {error}")),
    }
    outcomes
}

async fn stream_query(
    database: &HawDBTokioEmbedded,
    cypher: &str,
    parameters: BTreeMap<String, Value>,
    timeout: Duration,
) -> Result<QueryStreamReport, MixedSoakError> {
    let mut stream = database
        .query_stream_with_params(
            cypher.to_string(),
            parameters,
            RuntimeTaskContext::with_timeout(timeout),
        )
        .await
        .map_err(error)?;
    while stream.next_batch().await.map_err(error)?.is_some() {}
    stream
        .report()
        .cloned()
        .ok_or_else(|| MixedSoakError::new("query stream completed without a terminal report"))
}

fn prepare_fixture(
    config: &MixedSoakConfig,
) -> Result<(StorageResidencyReport, u64, String), MixedSoakError> {
    let mut embedded = HawDBEmbedded::open_with_options(embedded_options(config)).map_err(error)?;
    let mut fingerprint = StableHasher::new();
    fingerprint.update(config.dataset_id.as_bytes());
    for id in 0..config.node_count {
        let stable_id = u64::try_from(id).unwrap_or(u64::MAX);
        let payload = deterministic_payload(stable_id, config.payload_bytes);
        let mut parameters = BTreeMap::new();
        parameters.insert("id".to_string(), Value::Int(saturating_i64(id)));
        parameters.insert("bucket".to_string(), Value::Int(saturating_i64(id)));
        parameters.insert("body".to_string(), Value::String(payload.clone()));
        embedded
            .database_mut()
            .query_with_params(
                "CREATE (:Memory {id: $id, bucket: $bucket, body: $body})",
                &parameters,
            )
            .map_err(error)?;
        fingerprint.update(&stable_id.to_le_bytes());
        fingerprint.update(payload.as_bytes());
    }
    embedded.database_mut().checkpoint().map_err(error)?;
    let report = embedded.database().storage_residency_report();
    let epoch = embedded.database().commit_epoch();
    Ok((report, epoch, fingerprint.finish()))
}

fn embedded_options(config: &MixedSoakConfig) -> HawDBEmbeddedOpenOptions {
    let raw_bytes = raw_dataset_bytes(config);
    let batch_payload_bytes =
        usize::try_from((config.runtime_memory_budget_bytes / 16).clamp(8 * 1024, 256 * 1024))
            .unwrap_or(256 * 1024);
    let max_spill_runs = config.node_count.saturating_mul(2).max(2);
    let merge_levels = usize::BITS.saturating_sub(config.node_count.leading_zeros());
    let spill_amplification = u64::from(merge_levels).saturating_add(2);
    let database = DatabaseConfig {
        max_read_result_rows: Some(config.node_count.saturating_add(16)),
        max_read_result_payload_bytes: Some(
            usize::try_from(config.result_budget_bytes).unwrap_or(usize::MAX),
        ),
        execution_memory: ExecutionMemoryConfig {
            batch_rows: NonZeroUsize::new(256).unwrap(),
            batch_payload_bytes: NonZeroUsize::new(batch_payload_bytes).unwrap(),
            blocking_operator_bytes: NonZeroUsize::new(config.blocking_operator_bytes).unwrap(),
            max_spill_bytes: NonZeroU64::new(
                raw_bytes
                    .saturating_mul(spill_amplification)
                    .max(1024 * 1024),
            )
            .unwrap(),
            max_spill_runs: NonZeroUsize::new(max_spill_runs).unwrap(),
            spill_directory: config.database_path.with_extension("spill"),
            ..ExecutionMemoryConfig::default()
        },
        mutation_limits: MutationLimits {
            max_affected_rows: NonZeroUsize::new(1_024).unwrap(),
            max_operations: NonZeroUsize::new(1_024).unwrap(),
            max_result_rows: NonZeroUsize::new(16).unwrap(),
            max_result_payload_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        },
        max_wal_record_bytes: Some(128 * 1024),
        max_wal_batch_operations: Some(1_024),
        segment_cache_capacity_bytes: config.segment_cache_bytes,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: Some(raw_bytes.saturating_mul(2)),
        max_wal_replay_entries: Some(config.node_count.saturating_add(1024)),
        max_wal_replay_bytes: Some(raw_bytes.saturating_mul(4)),
        ..DatabaseConfig::default()
    };
    let concurrency = config.foreground_workers.saturating_add(2).max(1);
    let governor = RuntimeGovernorConfig {
        cpu_slot_limit: NonZeroUsize::new(concurrency),
        foreground_task_limit: NonZeroUsize::new(config.foreground_workers),
        background_task_limit: NonZeroUsize::new(1),
        blocking_task_limit: NonZeroUsize::new(concurrency),
        memory_budget_bytes: Some(config.runtime_memory_budget_bytes),
        result_budget_bytes: config.result_budget_bytes,
        ..RuntimeGovernorConfig::shared_host()
    };
    HawDBEmbeddedOpenOptions::new(&config.database_path)
        .with_config(database)
        .with_durability(DurabilityPolicy::SyncOnCheckpoint)
        .with_runtime_governor_config(governor)
}

fn configuration_report(config: &MixedSoakConfig) -> MixedSoakConfigurationReport {
    MixedSoakConfigurationReport {
        node_count: config.node_count,
        payload_bytes: config.payload_bytes,
        raw_dataset_bytes: raw_dataset_bytes(config),
        foreground_workers: config.foreground_workers,
        foreground_rounds_per_worker: config.foreground_rounds_per_worker,
        background_rounds: config.background_rounds,
        segment_cache_bytes: config.segment_cache_bytes,
        runtime_memory_budget_bytes: config.runtime_memory_budget_bytes,
        result_budget_bytes: config.result_budget_bytes,
        blocking_operator_bytes: config.blocking_operator_bytes,
        task_timeout_millis: duration_millis(config.task_timeout),
    }
}

fn aggregate_execution(
    durations: &[u64],
    reports: &[QueryStreamReport],
) -> MixedSoakExecutionReport {
    let mut aggregate = MixedSoakExecutionReport {
        query_count: reports.len(),
        latency: latency_percentiles(durations),
        ..MixedSoakExecutionReport::default()
    };
    for report in reports {
        aggregate.output_rows = aggregate.output_rows.saturating_add(report.output_rows);
        aggregate.output_payload_bytes = aggregate
            .output_payload_bytes
            .saturating_add(report.output_payload_bytes);
        let profile = &report.execution_profile;
        aggregate.intermediate_rows = aggregate
            .intermediate_rows
            .saturating_add(profile.pipeline_memory_report.intermediate_rows);
        aggregate.intermediate_payload_bytes = aggregate
            .intermediate_payload_bytes
            .saturating_add(profile.pipeline_memory_report.intermediate_payload_bytes);
        aggregate.blocking_operator_count = aggregate
            .blocking_operator_count
            .saturating_add(profile.blocking_operator_memory_reports.len());
        for operator in &profile.blocking_operator_memory_reports {
            aggregate.peak_tracked_operator_bytes = aggregate
                .peak_tracked_operator_bytes
                .max(operator.peak_tracked_bytes);
            aggregate.max_spill_bytes_per_query = aggregate
                .max_spill_bytes_per_query
                .max(operator.max_spill_bytes);
            aggregate.max_spill_runs_per_query = aggregate
                .max_spill_runs_per_query
                .max(operator.max_spill_runs);
            aggregate.spilled_bytes = aggregate
                .spilled_bytes
                .saturating_add(operator.spilled_bytes);
            aggregate.spill_run_count = aggregate
                .spill_run_count
                .saturating_add(operator.spill_run_count);
            aggregate.spilled_rows = aggregate.spilled_rows.saturating_add(operator.spilled_rows);
        }
    }
    aggregate
}

pub(crate) fn latency_percentiles(samples: &[u64]) -> LatencyPercentiles {
    if samples.is_empty() {
        return LatencyPercentiles::default();
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    LatencyPercentiles {
        sample_count: sorted.len(),
        min_micros: sorted[0],
        p50_micros: percentile(&sorted, 50),
        p95_micros: percentile(&sorted, 95),
        p99_micros: percentile(&sorted, 99),
        max_micros: *sorted.last().unwrap_or(&0),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len().saturating_sub(1))]
}

fn process_report(profile: ProcessMemoryProfile) -> MixedSoakProcessReport {
    MixedSoakProcessReport {
        resident_memory_supported: profile.capabilities.resident_memory,
        total_page_faults_supported: profile.capabilities.total_page_faults,
        split_page_faults_supported: profile.capabilities.split_page_faults,
        start_resident_bytes: profile.start_resident_bytes,
        steady_resident_bytes: profile.steady_resident_bytes,
        peak_resident_bytes: profile.peak_resident_bytes,
        steady_resident_growth_bytes: profile.steady_resident_growth_bytes,
        lifetime_peak_resident_growth_bytes: profile.lifetime_peak_resident_growth_bytes,
        total_page_faults: profile.total_page_faults,
        minor_page_faults: profile.minor_page_faults,
        major_page_faults: profile.major_page_faults,
    }
}

pub(crate) fn runtime_report(
    before: hawdb::RuntimeGovernorSnapshot,
    after: hawdb::RuntimeGovernorSnapshot,
) -> MixedSoakRuntimeReport {
    MixedSoakRuntimeReport {
        admissions_delta: after.admissions.saturating_sub(before.admissions),
        admission_waits_delta: after.admission_waits.saturating_sub(before.admission_waits),
        admission_rejections_delta: after
            .admission_rejections
            .saturating_sub(before.admission_rejections),
        completions_delta: after.completions.saturating_sub(before.completions),
        cancellations_delta: after.cancellations.saturating_sub(before.cancellations),
        deadline_exceeded_delta: after
            .deadline_exceeded
            .saturating_sub(before.deadline_exceeded),
        final_active_foreground_tasks: after.active_foreground_tasks,
        final_active_background_tasks: after.active_background_tasks,
        final_active_blocking_tasks: after.active_blocking_tasks,
        final_admitted_memory_bytes: after.admitted_memory_bytes,
        final_overcommitted: after.overcommitted,
    }
}

fn storage_report(
    config: &MixedSoakConfig,
    before: &StorageResidencyReport,
    after: &StorageResidencyReport,
    prepared: &StorageResidencyReport,
) -> MixedSoakStorageReport {
    MixedSoakStorageReport {
        out_of_core: after.out_of_core,
        canonical_generation_before: prepared.canonical_generation,
        canonical_generation_after: after.canonical_generation,
        canonical_artifact_bytes_before: prepared.canonical_artifact_bytes,
        canonical_artifact_bytes_after: after.canonical_artifact_bytes,
        canonical_node_count_after: after.canonical_node_count,
        canonical_exceeds_cache: after.canonical_artifact_bytes
            > after.segment_cache_capacity_bytes,
        raw_dataset_exceeds_runtime_memory: raw_dataset_bytes(config)
            > config.runtime_memory_budget_bytes,
        segment_cache_capacity_bytes: after.segment_cache_capacity_bytes,
        segment_cache_resident_bytes: after.segment_cache_resident_bytes,
        segment_cache_hits_delta: after
            .segment_cache_hit_count
            .saturating_sub(before.segment_cache_hit_count),
        segment_cache_misses_delta: after
            .segment_cache_miss_count
            .saturating_sub(before.segment_cache_miss_count),
        segment_cache_evictions_delta: after
            .segment_cache_eviction_count
            .saturating_sub(before.segment_cache_eviction_count),
        segment_cache_admission_rejections_delta: after
            .segment_cache_admission_rejection_count
            .saturating_sub(before.segment_cache_admission_rejection_count),
        segment_cache_digest_mismatches_delta: after
            .segment_cache_digest_mismatch_count
            .saturating_sub(before.segment_cache_digest_mismatch_count),
    }
}

fn deterministic_payload(seed: u64, bytes: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut output = String::with_capacity(bytes);
    for _ in 0..bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        output.push(char::from(
            ALPHABET[(state as usize) & (ALPHABET.len() - 1)],
        ));
    }
    output
}

fn raw_dataset_bytes(config: &MixedSoakConfig) -> u64 {
    u64::try_from(config.node_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(config.payload_bytes).unwrap_or(u64::MAX))
}

fn saturating_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn error(error: impl Display) -> MixedSoakError {
    MixedSoakError::new(error.to_string())
}

fn stable_digest(bytes: &[u8]) -> String {
    let mut hasher = StableHasher::new();
    hasher.update(bytes);
    hasher.finish()
}

struct StableHasher(Sha256);

impl StableHasher {
    fn new() -> Self {
        Self(Sha256::new())
    }

    fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(self) -> String {
        format!("sha256:{:x}", self.0.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn latency_percentiles_use_nearest_rank() {
        let latency = latency_percentiles(&[50, 10, 40, 20, 30]);

        assert_eq!(latency.sample_count, 5);
        assert_eq!(latency.min_micros, 10);
        assert_eq!(latency.p50_micros, 30);
        assert_eq!(latency.p95_micros, 50);
        assert_eq!(latency.p99_micros, 50);
        assert_eq!(latency.max_micros, 50);
    }

    #[test]
    fn deterministic_payload_and_digest_are_stable() {
        assert_eq!(deterministic_payload(7, 32), deterministic_payload(7, 32));
        assert_ne!(deterministic_payload(7, 32), deterministic_payload(8, 32));
        assert_eq!(
            stable_digest(b"hawdb"),
            "sha256:0008f032b344fab74c72624fc8b9fb01130ed1c777a91e2c49e440909026a25a"
        );
    }

    #[test]
    fn small_mixed_soak_emits_non_production_typed_evidence() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("hawdb-qualification-{}-{id}", std::process::id()));
        let mut config = MixedSoakConfig::scheduled(root.join("database"), "test-revision");
        config.dataset_id = "test-dataset".to_string();
        config.node_count = 512;
        config.payload_bytes = 8 * 1024;
        config.foreground_workers = 2;
        config.foreground_rounds_per_worker = 2;
        config.background_rounds = 1;
        config.segment_cache_bytes = 64 * 1024;
        // Admit the planning phase and bounded stream buffers while keeping the
        // raw dataset twice the governor budget. Operator/cache limits stay small.
        config.runtime_memory_budget_bytes = 2 * 1024 * 1024;
        config.result_budget_bytes = 64 * 1024;
        config.blocking_operator_bytes = 32 * 1024;
        // Avoid treating shared-runner scheduling delays as workload failures.
        config.task_timeout = Duration::from_secs(120);
        assert_eq!(
            raw_dataset_bytes(&config),
            2 * config.runtime_memory_budget_bytes
        );

        let report = run_mixed_soak(&config).unwrap();

        assert_eq!(report.protocol, MIXED_SOAK_PROTOCOL);
        assert_eq!(report.evidence_kind, "synthetic_scheduled_soak");
        assert!(!report.production_eligible);
        assert_eq!(report.identity.source_revision, "test-revision");
        assert_eq!(report.identity.dataset_id, "test-dataset");
        let expected_foreground = config
            .foreground_workers
            .saturating_mul(config.foreground_rounds_per_worker);
        assert_eq!(
            report.foreground.query_count, expected_foreground,
            "foreground errors: {:?}",
            report.errors
        );
        assert_eq!(
            report.background.query_count, 1,
            "background errors: {:?}",
            report.errors
        );
        assert!(report.storage.raw_dataset_exceeds_runtime_memory);
        assert!(
            report.checkpoint.completed,
            "checkpoint errors: {:?}",
            report.errors
        );
        assert!(report.checkpoint.canonical_generation_advanced);
        assert!(
            report.errors.is_empty(),
            "workload errors: {:?}",
            report.errors
        );
        assert_eq!(report.runtime.final_active_foreground_tasks, 0);
        assert_eq!(report.runtime.final_active_background_tasks, 0);
        assert_eq!(report.runtime.final_active_blocking_tasks, 0);
        assert_eq!(report.runtime.final_admitted_memory_bytes, 0);
        assert_eq!(
            report.runtime.admissions_delta,
            report.runtime.completions_delta
        );
        assert!(!report.runtime.final_overcommitted);
        assert!(report.json().is_object());

        let _ = std::fs::remove_dir_all(root);
        let _ = std::fs::remove_dir_all(config.database_path.with_extension("spill"));
    }
}
