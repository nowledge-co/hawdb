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

use super::evidence::execute_qualified_read;
use super::resource::{process_evidence, runtime_memory_evidence};
use super::{
    ContentStoreOpenTimingEvidence, ContentStoreProcessResourceEvidence,
    ContentStoreResourceProfileKind, ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport,
    ContentStoreRuntimeMemoryEvidence, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
};
use crate::evidence_digest::{hash_bytes, hash_value};
use crate::production_graph::validate_production_identity_for_current_target;
use crate::{
    nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSchemaIdentity, ContentStoreSqlCorpus, ContentStoreSqlCorpusIdentity,
    ContentStoreSqlStatementClassification, ContentStoreSqlStatementKind,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
};
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, IoConcurrencyBudget,
    ProcessMemoryProfile, ProcessMemorySnapshot, ProductionEvidenceBinding,
    ProductionQualificationIdentity, RelationalIndexMode, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeGovernorSnapshot, RuntimeMemorySnapshot, RuntimeWorkRequest, StorageDeviceProfile,
    StorageResidencyMode, StorageResidencyReport, Value,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Instant;

pub const PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL: &str =
    "hawdb-production-content-store-storage-qualification-v1";

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreReadCase {
    pub case_name: String,
    pub statement_name: String,
    pub parameters: Vec<Value>,
    pub expected_output_rows: usize,
    pub expected_output_sha256: String,
    pub max_intermediate_rows: u64,
    pub max_physical_pages_per_run: u64,
    pub max_physical_bytes_per_run: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreResourceLimits {
    pub max_steady_resident_bytes: u64,
    pub max_peak_resident_bytes: u64,
    pub max_total_page_faults_per_run: Option<u64>,
    pub max_minor_page_faults_per_run: Option<u64>,
    pub max_major_page_faults_per_run: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreReadContractEvidence {
    pub case_name: String,
    pub statement_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub expected_output_rows: usize,
    pub expected_output_sha256: String,
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub max_intermediate_rows: u64,
    pub max_physical_pages_per_run: u64,
    pub max_physical_bytes_per_run: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreStorageQualificationConfig {
    pub database_path: PathBuf,
    pub database_config: DatabaseConfig,
    pub runtime_governor_config: RuntimeGovernorConfig,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub measurement_runs: usize,
    pub open_payload_cache_limits: ProductionContentStoreOpenCacheLimits,
    pub resource_limits: ProductionContentStoreResourceLimits,
    pub read_cases: Vec<ProductionContentStoreReadCase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOpenCacheLimits {
    pub max_requests: u64,
    pub max_resident_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreResidencyEvidence {
    pub database_commit_epoch: u64,
    pub row_serving: bool,
    pub row_materialized_rows_resident: bool,
    pub row_checkpoint_state_metadata_only: bool,
    pub row_base_generation: Option<u64>,
    pub row_recovery_delta_generation: Option<u64>,
    pub row_base_commit_epoch: Option<u64>,
    pub row_visible_commit_epoch: Option<u64>,
    pub row_page_artifact_bytes: u64,
    pub row_root_descriptor_artifact_bytes: u64,
    pub row_root_key_artifact_bytes: u64,
    pub row_overflow_extent_artifact_bytes: u64,
    pub row_overflow_descriptor_artifact_bytes: u64,
    pub row_overflow_extent_count: u64,
    pub row_canonical_artifact_bytes: u64,
    pub row_recovery_delta_artifact_bytes: u64,
    pub row_live_entries: usize,
    pub row_live_encoded_bytes: usize,
    pub row_live_resident_bytes: usize,
    pub index_serving: bool,
    pub index_base_generation: Option<u64>,
    pub index_recovery_delta_generation: Option<u64>,
    pub index_base_commit_epoch: Option<u64>,
    pub index_visible_commit_epoch: Option<u64>,
    pub index_base_page_count: u64,
    pub index_canonical_artifact_bytes: u64,
    pub index_recovery_delta_artifact_bytes: u64,
    pub index_live_entries: usize,
    pub index_live_encoded_bytes: usize,
    pub segment_cache_capacity_bytes: u64,
    pub segment_cache_resident_bytes: u64,
    pub segment_cache_pinned_bytes: u64,
    pub row_index_epoch_aligned: bool,
    pub row_artifact_exceeds_cache: bool,
    pub index_artifact_exceeds_cache: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOpenEvidence {
    pub case_name: String,
    pub latency_micros: u64,
    pub open_timings: ContentStoreOpenTimingEvidence,
    pub payload_cache: ProductionContentStoreOpenCacheEvidence,
    pub recovered_commit_epoch: u64,
    pub replayed_wal_entries: usize,
    pub replayed_wal_bytes: u64,
    pub process: ContentStoreProcessResourceEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOpenCacheEvidence {
    pub capacity_bytes: u64,
    pub resident_bytes: u64,
    pub pinned_bytes: u64,
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
    pub admission_rejection_count: u64,
    pub digest_mismatch_count: u64,
    pub within_limits: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionContentStoreReadPhase {
    Cold,
    Warm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreRunEvidence {
    pub case_name: String,
    pub statement_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub expected_output_sha256: String,
    pub run: usize,
    pub phase: ProductionContentStoreReadPhase,
    pub latency_micros: u64,
    pub read: ContentStoreRowPageReadReport,
    pub process: ContentStoreProcessResourceEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreRuntimeGovernorEvidence {
    pub configured_memory_ceiling_bytes: Option<u64>,
    pub memory_fraction_per_million: u32,
    pub effective_memory_limit_bytes: Option<u64>,
    pub effective_available_memory_bytes: Option<u64>,
    pub memory_capacity_bytes: u64,
    pub memory_budget_bytes: u64,
    pub result_budget_bytes: u64,
    pub effective_cpu_slots: usize,
    pub foreground_io_depth: usize,
    pub admissions_delta: u64,
    pub admission_waits_delta: u64,
    pub admission_rejections_delta: u64,
    pub completions_delta: u64,
    pub final_active_foreground_tasks: usize,
    pub final_active_background_tasks: usize,
    pub final_active_blocking_tasks: usize,
    pub final_active_cpu_slots: usize,
    pub final_active_foreground_io_slots: usize,
    pub final_active_background_io_slots: usize,
    pub final_admitted_memory_bytes: u64,
    pub final_overcommitted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionContentStoreStorageQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: ProductionEvidenceBinding,
    pub corpus: ContentStoreSqlCorpusIdentity,
    pub schema: ContentStoreSchemaIdentity,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub max_relational_hydration_bytes: u64,
    pub measurement_runs: usize,
    pub open_payload_cache_limits: ProductionContentStoreOpenCacheLimits,
    pub resource_limits: ProductionContentStoreResourceLimits,
    pub read_contracts: Vec<ProductionContentStoreReadContractEvidence>,
    pub runtime_memory: ContentStoreRuntimeMemoryEvidence,
    pub runtime_governor: ProductionContentStoreRuntimeGovernorEvidence,
    pub lifecycle_process: ContentStoreProcessResourceEvidence,
    pub initial_residency: ProductionContentStoreResidencyEvidence,
    pub final_residency: ProductionContentStoreResidencyEvidence,
    pub opens: Vec<ProductionContentStoreOpenEvidence>,
    pub runs: Vec<ProductionContentStoreRunEvidence>,
}

impl ProductionContentStoreStorageQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_CONTENT_STORE_STORAGE_QUALIFICATION_PROTOCOL,
            "evidence_kind": "representative_production_relational_replica",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "evidence_binding": self.evidence_binding.json(),
            "corpus": self.corpus,
            "schema": self.schema,
            "resource_profile_kind": self.resource_profile_kind,
            "configured_available_memory_bytes": self.configured_available_memory_bytes,
            "max_relational_hydration_bytes": self.max_relational_hydration_bytes,
            "measurement_runs": self.measurement_runs,
            "open_payload_cache_limits": self.open_payload_cache_limits,
            "resource_limits": self.resource_limits,
            "read_contracts": self.read_contracts,
            "runtime_memory": self.runtime_memory,
            "runtime_governor": self.runtime_governor,
            "lifecycle_process": self.lifecycle_process,
            "initial_residency": self.initial_residency,
            "final_residency": self.final_residency,
            "opens": self.opens,
            "runs": self.runs,
        })
    }
}

pub fn run_production_content_store_storage_qualification(
    config: ProductionContentStoreStorageQualificationConfig,
) -> Result<ProductionContentStoreStorageQualificationReport, HawDBError> {
    let corpus = nowledge_content_store_sql_corpus()?;
    validate_config(&config, &corpus)?;

    let lifecycle_start = ProcessMemorySnapshot::capture()?;
    let runtime_memory = runtime_memory_evidence(RuntimeMemorySnapshot::detect());
    let storage_io = IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::detect(
        &config.database_path,
    ));
    let governor = RuntimeGovernor::detect(config.runtime_governor_config, storage_io);
    let governor_before = governor.snapshot();
    let read_contracts = config
        .read_cases
        .iter()
        .map(|read_case| {
            let statement = corpus
                .statement(&read_case.statement_name)
                .expect("validated production read statement");
            ProductionContentStoreReadContractEvidence {
                case_name: read_case.case_name.clone(),
                statement_name: read_case.statement_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: ordered_parameter_digest(&read_case.parameters),
                expected_output_rows: read_case.expected_output_rows,
                expected_output_sha256: read_case.expected_output_sha256.clone(),
                max_output_rows: statement.max_rows,
                max_output_payload_bytes: statement.max_payload_bytes,
                max_intermediate_rows: read_case.max_intermediate_rows,
                max_physical_pages_per_run: read_case.max_physical_pages_per_run,
                max_physical_bytes_per_run: read_case.max_physical_bytes_per_run,
            }
        })
        .collect();
    let mut blocker_codes = Vec::new();
    let mut opens = Vec::with_capacity(config.read_cases.len());
    let mut runs = Vec::with_capacity(
        config
            .read_cases
            .len()
            .saturating_mul(config.measurement_runs),
    );
    let mut initial_residency = None;
    let mut final_residency = None;

    for read_case in &config.read_cases {
        let process_before_open = ProcessMemorySnapshot::capture()?;
        let open_started = Instant::now();
        let mut database = Database::open_with_durability_and_config(
            &config.database_path,
            DurabilityPolicy::SyncOnEveryWrite,
            config.database_config.clone(),
        )?;
        database.set_runtime_governor(governor.clone());
        let open_latency_micros = elapsed_micros(open_started);
        let process_after_open = ProcessMemorySnapshot::capture()?;
        let recovery = database.storage_recovery_report();
        let open_timings = ContentStoreOpenTimingEvidence::from(recovery.open_timings);
        if !open_timings.consistent || open_timings.total_open_micros > open_latency_micros {
            blocker_codes.push("content_store_open_timing_invalid".to_string());
        }
        let open_residency = database.storage_residency_report();
        let observed = residency_evidence(database.commit_epoch(), &open_residency);
        let payload_cache = open_cache_evidence(&open_residency, config.open_payload_cache_limits);
        if !payload_cache.within_limits {
            blocker_codes.push("content_store_open_payload_cache_unbounded".to_string());
        }
        collect_residency_blockers(&observed, &config.expected_identity, &mut blocker_codes);
        if let Some(first) = initial_residency.as_ref() {
            if !same_storage_identity(first, &observed) {
                blocker_codes
                    .push("content_store_storage_identity_changed_between_cases".to_string());
            }
        } else {
            initial_residency = Some(observed.clone());
        }
        opens.push(ProductionContentStoreOpenEvidence {
            case_name: read_case.case_name.clone(),
            latency_micros: open_latency_micros,
            open_timings,
            payload_cache,
            recovered_commit_epoch: recovery.recovered_commit_epoch,
            replayed_wal_entries: recovery.replayed_wal_entries,
            replayed_wal_bytes: recovery.replayed_wal_bytes,
            process: process_evidence(ProcessMemoryProfile::between(
                process_before_open,
                process_after_open,
            )),
        });

        let statement = corpus.statement(&read_case.statement_name).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "production Content Store case {} references missing statement {}",
                read_case.case_name, read_case.statement_name
            ))
        })?;
        let first_case_run = runs.len();
        for run in 0..config.measurement_runs {
            let phase = if run == 0 {
                ProductionContentStoreReadPhase::Cold
            } else {
                ProductionContentStoreReadPhase::Warm
            };
            let read_phase = if run == 0 {
                ContentStoreRowPageReadPhase::ProductionCold
            } else {
                ContentStoreRowPageReadPhase::ProductionWarm
            };
            let process_before = ProcessMemorySnapshot::capture()?;
            let started = Instant::now();
            let result_bytes = u64::try_from(statement.max_payload_bytes).unwrap_or(u64::MAX);
            let working_memory_bytes = u64::try_from(
                config
                    .database_config
                    .execution_memory
                    .blocking_operator_bytes
                    .get(),
            )
            .unwrap_or(u64::MAX);
            let _permit = governor
                .try_admit(
                    RuntimeWorkRequest::foreground_query(
                        working_memory_bytes.saturating_add(result_bytes),
                        result_bytes,
                    )
                    .with_io_slots(1)
                    .with_blocking(true),
                )
                .map_err(|error| {
                    HawDBError::Execution(format!(
                        "production Content Store read admission failed: {error}"
                    ))
                })?;
            let read = execute_qualified_read(
                &mut database,
                statement,
                read_case.parameters.clone(),
                read_phase,
                read_case.expected_output_rows,
            )?;
            let latency_micros = elapsed_micros(started);
            let process_after = ProcessMemorySnapshot::capture()?;
            let process =
                process_evidence(ProcessMemoryProfile::between(process_before, process_after));
            collect_run_blockers(
                read_case,
                &read,
                &process,
                &observed,
                &config,
                &mut blocker_codes,
            );
            runs.push(ProductionContentStoreRunEvidence {
                case_name: read_case.case_name.clone(),
                statement_name: read_case.statement_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: ordered_parameter_digest(&read_case.parameters),
                expected_output_sha256: read_case.expected_output_sha256.clone(),
                run,
                phase,
                latency_micros,
                read,
                process,
            });
        }
        let case_runs = &runs[first_case_run..];
        if case_runs.first().is_none_or(|run| {
            run.phase != ProductionContentStoreReadPhase::Cold || run.read.cache.misses == 0
        }) {
            blocker_codes.push("content_store_cold_cache_miss_not_observed".to_string());
        }
        if !case_runs.iter().any(|run| {
            run.phase == ProductionContentStoreReadPhase::Warm && run.read.cache.hits > 0
        }) {
            blocker_codes.push("content_store_warm_cache_hit_not_observed".to_string());
        }
        let after = residency_evidence(
            database.commit_epoch(),
            &database.storage_residency_report(),
        );
        if !same_storage_identity(&observed, &after) {
            blocker_codes.push("content_store_storage_identity_changed_during_reads".to_string());
        }
        if after.segment_cache_pinned_bytes != 0 {
            blocker_codes.push("content_store_segment_cache_pin_leak".to_string());
        }
        final_residency = Some(after);
    }

    if !runs
        .iter()
        .any(|run| run.read.execution.index_runtime_path == "authoritative")
    {
        blocker_codes.push("content_store_authoritative_index_read_not_observed".to_string());
    }

    let lifecycle_end = ProcessMemorySnapshot::capture()?;
    let lifecycle_process = process_evidence(ProcessMemoryProfile::between(
        lifecycle_start,
        lifecycle_end,
    ));
    collect_resident_blockers(
        &lifecycle_process,
        config.resource_limits,
        "content_store_lifecycle",
        &mut blocker_codes,
    );
    let runtime_governor = runtime_governor_evidence(
        config.runtime_governor_config,
        governor_before,
        governor.snapshot(),
    );
    collect_runtime_governor_blockers(&runtime_governor, runs.len(), &config, &mut blocker_codes);
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(ProductionContentStoreStorageQualificationReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        evidence_binding: config.evidence_binding,
        corpus: corpus.identity(),
        schema: nowledge_content_store_schema_identity(),
        resource_profile_kind: config.resource_profile_kind,
        configured_available_memory_bytes: config.configured_available_memory_bytes,
        max_relational_hydration_bytes: u64::try_from(
            config.database_config.max_relational_hydration_bytes.get(),
        )
        .unwrap_or(u64::MAX),
        measurement_runs: config.measurement_runs,
        open_payload_cache_limits: config.open_payload_cache_limits,
        resource_limits: config.resource_limits,
        read_contracts,
        runtime_memory,
        runtime_governor,
        lifecycle_process,
        initial_residency: initial_residency.expect("validated non-empty read cases"),
        final_residency: final_residency.expect("validated non-empty read cases"),
        opens,
        runs,
    })
}

fn open_cache_evidence(
    report: &StorageResidencyReport,
    limits: ProductionContentStoreOpenCacheLimits,
) -> ProductionContentStoreOpenCacheEvidence {
    let requests = report
        .segment_cache_hit_count
        .saturating_add(report.segment_cache_miss_count);
    let within_limits = report.segment_cache_resident_bytes <= limits.max_resident_bytes
        && requests <= limits.max_requests
        && report.segment_cache_pinned_bytes == 0
        && report.segment_cache_eviction_count == 0
        && report.segment_cache_admission_rejection_count == 0
        && report.segment_cache_digest_mismatch_count == 0;
    ProductionContentStoreOpenCacheEvidence {
        capacity_bytes: report.segment_cache_capacity_bytes,
        resident_bytes: report.segment_cache_resident_bytes,
        pinned_bytes: report.segment_cache_pinned_bytes,
        hit_count: report.segment_cache_hit_count,
        miss_count: report.segment_cache_miss_count,
        eviction_count: report.segment_cache_eviction_count,
        admission_rejection_count: report.segment_cache_admission_rejection_count,
        digest_mismatch_count: report.segment_cache_digest_mismatch_count,
        within_limits,
    }
}

fn validate_config(
    config: &ProductionContentStoreStorageQualificationConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(), HawDBError> {
    if !config.database_path.is_dir() {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires an existing HawDB database directory"
                .to_string(),
        ));
    }
    if !config.database_config.read_only {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires read_only database config".to_string(),
        ));
    }
    if config.database_config.storage_residency_mode != StorageResidencyMode::OutOfCore {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires out-of-core storage".to_string(),
        ));
    }
    if config.database_config.relational_index_mode != RelationalIndexMode::Authoritative {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires authoritative relational indexes"
                .to_string(),
        ));
    }
    if config.database_config.segment_cache_capacity_bytes == 0 {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires a non-zero segment cache".to_string(),
        ));
    }
    if config.open_payload_cache_limits.max_requests == 0
        || config.open_payload_cache_limits.max_resident_bytes == 0
        || config.open_payload_cache_limits.max_resident_bytes
            > config.database_config.segment_cache_capacity_bytes
    {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires non-zero open payload-cache limits within the segment-cache capacity"
                .to_string(),
        ));
    }
    if config.measurement_runs < 2 || config.measurement_runs > 1024 {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires between 2 and 1024 measurement runs"
                .to_string(),
        ));
    }
    validate_resource_profile(
        config.resource_profile_kind,
        config.configured_available_memory_bytes,
        config.runtime_governor_config,
        config.resource_limits,
    )?;
    validate_production_identity_for_current_target(
        &config.evidence_binding,
        &config.expected_identity,
    )
    .map_err(|error| HawDBError::Semantic(error.to_string()))?;

    validate_read_cases(
        &config.read_cases,
        corpus,
        config.runtime_governor_config.result_budget_bytes,
    )
}

pub(super) fn validate_read_cases(
    read_cases: &[ProductionContentStoreReadCase],
    corpus: &ContentStoreSqlCorpus,
    result_budget_bytes: u64,
) -> Result<(), HawDBError> {
    if read_cases.is_empty() {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires at least one read case".to_string(),
        ));
    }
    let mut case_names = BTreeSet::new();
    for read_case in read_cases {
        if read_case.case_name.trim().is_empty() || !case_names.insert(&read_case.case_name) {
            return Err(HawDBError::Semantic(
                "production Content Store case names must be non-empty and unique".to_string(),
            ));
        }
        let statement = corpus.statement(&read_case.statement_name).ok_or_else(|| {
            HawDBError::Semantic(format!(
                "production Content Store case {} references unknown statement {}",
                read_case.case_name, read_case.statement_name
            ))
        })?;
        if statement.kind != ContentStoreSqlStatementKind::Read
            || statement.classification == ContentStoreSqlStatementClassification::RetainedOnSqlite
        {
            return Err(HawDBError::Semantic(format!(
                "production Content Store case {} must reference a HawDB-owned read statement",
                read_case.case_name
            )));
        }
        if statement.parameters.len() != read_case.parameters.len() {
            return Err(HawDBError::Semantic(format!(
                "production Content Store case {} supplies {} parameters for a {}-parameter statement",
                read_case.case_name,
                read_case.parameters.len(),
                statement.parameters.len()
            )));
        }
        if u64::try_from(statement.max_payload_bytes).unwrap_or(u64::MAX) > result_budget_bytes {
            return Err(HawDBError::Semantic(format!(
                "production Content Store case {} payload budget exceeds the runtime governor result budget",
                read_case.case_name
            )));
        }
        if read_case.expected_output_rows > statement.max_rows
            || read_case.max_intermediate_rows == 0
            || read_case.max_physical_pages_per_run == 0
            || read_case.max_physical_bytes_per_run == 0
            || !valid_sha256(&read_case.expected_output_sha256)
        {
            return Err(HawDBError::Semantic(format!(
                "production Content Store case {} has invalid output, intermediate, I/O, or digest limits",
                read_case.case_name
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_resource_profile(
    kind: ContentStoreResourceProfileKind,
    configured_available_memory_bytes: u64,
    governor: RuntimeGovernorConfig,
    limits: ProductionContentStoreResourceLimits,
) -> Result<(), HawDBError> {
    if configured_available_memory_bytes == 0 {
        return Err(HawDBError::Semantic(
            "production Content Store qualification requires a non-zero configured memory profile"
                .to_string(),
        ));
    }
    match kind {
        ContentStoreResourceProfileKind::Capability512Mib
            if configured_available_memory_bytes != CONTENT_STORE_512_MIB_CAPABILITY_BYTES =>
        {
            return Err(HawDBError::Semantic(format!(
                "production Content Store 512 MiB capability must declare {CONTENT_STORE_512_MIB_CAPABILITY_BYTES} bytes"
            )));
        }
        ContentStoreResourceProfileKind::SharedHost8Gib
            if configured_available_memory_bytes != CONTENT_STORE_SHARED_HOST_8_GIB_BYTES =>
        {
            return Err(HawDBError::Semantic(format!(
                "production Content Store shared-host profile must declare {CONTENT_STORE_SHARED_HOST_8_GIB_BYTES} bytes"
            )));
        }
        _ => {}
    }
    let shared_host_governor = RuntimeGovernorConfig::shared_host();
    match kind {
        ContentStoreResourceProfileKind::Capability512Mib
            if governor.memory_budget_bytes != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES) =>
        {
            return Err(HawDBError::Semantic(format!(
                "production Content Store 512 MiB capability requires an explicit {CONTENT_STORE_512_MIB_CAPABILITY_BYTES}-byte governor ceiling"
            )));
        }
        ContentStoreResourceProfileKind::SharedHost8Gib
            if governor.memory_budget_bytes.is_some()
                || governor.memory_fraction_per_million
                    != shared_host_governor.memory_fraction_per_million
                || governor.fallback_memory_budget_bytes
                    != shared_host_governor.fallback_memory_budget_bytes =>
        {
            return Err(HawDBError::Semantic(
                "production Content Store shared-host profile requires the dynamic shared-host governor memory policy"
                    .to_string(),
            ));
        }
        _ => {}
    }
    if limits.max_steady_resident_bytes == 0
        || limits.max_peak_resident_bytes == 0
        || limits.max_steady_resident_bytes > limits.max_peak_resident_bytes
        || limits.max_peak_resident_bytes > configured_available_memory_bytes
    {
        return Err(HawDBError::Semantic(
            "production Content Store resident-memory limits must be non-zero, ordered, and within the configured profile"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) fn residency_evidence(
    database_commit_epoch: u64,
    report: &StorageResidencyReport,
) -> ProductionContentStoreResidencyEvidence {
    let rows = &report.relational_rows;
    let indexes = &report.relational_indexes;
    ProductionContentStoreResidencyEvidence {
        database_commit_epoch,
        row_serving: rows.serving,
        row_materialized_rows_resident: rows.materialized_rows_resident,
        row_checkpoint_state_metadata_only: rows.checkpoint_state_metadata_only,
        row_base_generation: rows.base_generation,
        row_recovery_delta_generation: rows.recovery_delta_generation,
        row_base_commit_epoch: rows.base_commit_epoch,
        row_visible_commit_epoch: rows.visible_commit_epoch,
        row_page_artifact_bytes: rows.page_artifact_bytes,
        row_root_descriptor_artifact_bytes: rows.root_descriptor_artifact_bytes,
        row_root_key_artifact_bytes: rows.root_key_artifact_bytes,
        row_overflow_extent_artifact_bytes: rows.overflow_extent_artifact_bytes,
        row_overflow_descriptor_artifact_bytes: rows.overflow_descriptor_artifact_bytes,
        row_overflow_extent_count: rows.overflow_extent_count,
        row_canonical_artifact_bytes: rows.canonical_artifact_bytes(),
        row_recovery_delta_artifact_bytes: rows.recovery_delta_artifact_bytes,
        row_live_entries: rows.live_entries,
        row_live_encoded_bytes: rows.live_encoded_bytes,
        row_live_resident_bytes: rows.live_resident_bytes,
        index_serving: indexes.serving,
        index_base_generation: indexes.base_generation,
        index_recovery_delta_generation: indexes.recovery_delta_generation,
        index_base_commit_epoch: indexes.base_commit_epoch,
        index_visible_commit_epoch: indexes.visible_commit_epoch,
        index_base_page_count: indexes.base_page_count,
        index_canonical_artifact_bytes: indexes.canonical_artifact_bytes(),
        index_recovery_delta_artifact_bytes: indexes.recovery_delta_artifact_bytes,
        index_live_entries: indexes.live_entries,
        index_live_encoded_bytes: indexes.live_encoded_bytes,
        segment_cache_capacity_bytes: report.segment_cache_capacity_bytes,
        segment_cache_resident_bytes: report.segment_cache_resident_bytes,
        segment_cache_pinned_bytes: report.segment_cache_pinned_bytes,
        row_index_epoch_aligned: rows.base_generation == indexes.base_generation
            && rows.base_commit_epoch == indexes.base_commit_epoch
            && rows.visible_commit_epoch == indexes.visible_commit_epoch,
        row_artifact_exceeds_cache: rows.canonical_artifact_bytes()
            > report.segment_cache_capacity_bytes,
        index_artifact_exceeds_cache: indexes.canonical_artifact_bytes()
            > report.segment_cache_capacity_bytes,
    }
}

fn collect_residency_blockers(
    residency: &ProductionContentStoreResidencyEvidence,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    if !residency.row_serving {
        blockers.push("content_store_relational_rows_not_serving".to_string());
    }
    if !residency.index_serving {
        blockers.push("content_store_relational_indexes_not_serving".to_string());
    }
    if !residency.row_index_epoch_aligned {
        blockers.push("content_store_relational_row_index_epoch_mismatch".to_string());
    }
    if residency.row_visible_commit_epoch != Some(residency.database_commit_epoch)
        || residency.index_visible_commit_epoch != Some(residency.database_commit_epoch)
    {
        blockers.push("content_store_relational_view_not_current".to_string());
    }
    if residency.database_commit_epoch != expected.canonical_graph_commit_epoch {
        blockers.push("content_store_replica_epoch_identity_mismatch".to_string());
    }
    if !residency.row_artifact_exceeds_cache {
        blockers.push("content_store_row_artifact_does_not_exceed_cache".to_string());
    }
    if !residency.index_artifact_exceeds_cache {
        blockers.push("content_store_index_artifact_does_not_exceed_cache".to_string());
    }
    if residency.segment_cache_pinned_bytes != 0 {
        blockers.push("content_store_segment_cache_pin_leak".to_string());
    }
}

fn collect_run_blockers(
    read_case: &ProductionContentStoreReadCase,
    read: &ContentStoreRowPageReadReport,
    process: &ContentStoreProcessResourceEvidence,
    residency: &ProductionContentStoreResidencyEvidence,
    config: &ProductionContentStoreStorageQualificationConfig,
    blockers: &mut Vec<String>,
) {
    if read.output_sha256 != read_case.expected_output_sha256 {
        blockers.push("content_store_output_digest_mismatch".to_string());
    }
    if read.execution.base_generation != residency.row_base_generation.unwrap_or_default()
        || read.execution.base_commit_epoch != residency.row_base_commit_epoch.unwrap_or_default()
        || read.execution.visible_commit_epoch
            != residency.row_visible_commit_epoch.unwrap_or_default()
    {
        blockers.push("content_store_read_generation_identity_mismatch".to_string());
    }
    if read.execution.intermediate_rows > read_case.max_intermediate_rows {
        blockers.push("content_store_intermediate_row_budget_exceeded".to_string());
    }
    if read
        .execution
        .physical_pages
        .saturating_add(read.execution.index_physical_pages)
        > read_case.max_physical_pages_per_run
    {
        blockers.push("content_store_physical_page_budget_exceeded".to_string());
    }
    if read
        .execution
        .physical_bytes
        .saturating_add(read.execution.index_physical_bytes)
        > read_case.max_physical_bytes_per_run
    {
        blockers.push("content_store_physical_byte_budget_exceeded".to_string());
    }
    if read.execution.cache_admission_rejections != 0
        || read.execution.index_cache_admission_rejections != 0
        || read.cache.admission_rejections != 0
    {
        blockers.push("content_store_cache_admission_rejection_observed".to_string());
    }
    if read.cache.pinned_bytes_after != 0 {
        blockers.push("content_store_segment_cache_pin_leak".to_string());
    }
    if read.cache.resident_bytes_after > residency.segment_cache_capacity_bytes {
        blockers.push("content_store_segment_cache_capacity_exceeded".to_string());
    }
    let hydration_limit = config.database_config.max_relational_hydration_bytes.get() as u64;
    if read.execution.hydrated_compressed_bytes > hydration_limit
        || read.execution.hydrated_decompressed_bytes > hydration_limit
    {
        blockers.push("content_store_hydration_budget_exceeded".to_string());
    }
    collect_run_process_blockers(
        process,
        config.resource_limits,
        "content_store_read",
        blockers,
    );
}

fn collect_resident_blockers(
    process: &ContentStoreProcessResourceEvidence,
    limits: ProductionContentStoreResourceLimits,
    prefix: &str,
    blockers: &mut Vec<String>,
) {
    if !process.resident_memory_supported {
        blockers.push(format!("{prefix}_resident_memory_unavailable"));
    } else {
        if process.steady_resident_bytes > limits.max_steady_resident_bytes {
            blockers.push(format!("{prefix}_steady_resident_budget_exceeded"));
        }
        if process.peak_resident_bytes > limits.max_peak_resident_bytes {
            blockers.push(format!("{prefix}_peak_resident_budget_exceeded"));
        }
    }
}

pub(super) fn collect_run_process_blockers(
    process: &ContentStoreProcessResourceEvidence,
    limits: ProductionContentStoreResourceLimits,
    prefix: &str,
    blockers: &mut Vec<String>,
) {
    collect_resident_blockers(process, limits, prefix, blockers);
    collect_fault_blocker(
        prefix,
        "total",
        process.total_page_faults_supported,
        process.total_page_faults,
        limits.max_total_page_faults_per_run,
        blockers,
    );
    collect_fault_blocker(
        prefix,
        "minor",
        process.split_page_faults_supported,
        process.minor_page_faults,
        limits.max_minor_page_faults_per_run,
        blockers,
    );
    collect_fault_blocker(
        prefix,
        "major",
        process.split_page_faults_supported,
        process.major_page_faults,
        limits.max_major_page_faults_per_run,
        blockers,
    );
}

fn collect_fault_blocker(
    prefix: &str,
    kind: &str,
    supported: bool,
    observed: Option<u64>,
    maximum: Option<u64>,
    blockers: &mut Vec<String>,
) {
    let Some(maximum) = maximum else {
        return;
    };
    if !supported || observed.is_none() {
        blockers.push(format!("{prefix}_{kind}_page_faults_unavailable"));
    } else if observed.is_some_and(|observed| observed > maximum) {
        blockers.push(format!("{prefix}_{kind}_page_fault_budget_exceeded"));
    }
}

pub(super) fn runtime_governor_evidence(
    config: RuntimeGovernorConfig,
    before: RuntimeGovernorSnapshot,
    after: RuntimeGovernorSnapshot,
) -> ProductionContentStoreRuntimeGovernorEvidence {
    ProductionContentStoreRuntimeGovernorEvidence {
        configured_memory_ceiling_bytes: config.memory_budget_bytes,
        memory_fraction_per_million: config.memory_fraction_per_million,
        effective_memory_limit_bytes: after.resources.memory.effective_limit_bytes,
        effective_available_memory_bytes: after.resources.memory.effective_available_bytes,
        memory_capacity_bytes: after.limits.memory_capacity_bytes,
        memory_budget_bytes: after.limits.memory_budget_bytes,
        result_budget_bytes: after.limits.result_budget_bytes,
        effective_cpu_slots: after.limits.effective_cpu_slots.get(),
        foreground_io_depth: after.limits.foreground_io_depth.get(),
        admissions_delta: after.admissions.saturating_sub(before.admissions),
        admission_waits_delta: after.admission_waits.saturating_sub(before.admission_waits),
        admission_rejections_delta: after
            .admission_rejections
            .saturating_sub(before.admission_rejections),
        completions_delta: after.completions.saturating_sub(before.completions),
        final_active_foreground_tasks: after.active_foreground_tasks,
        final_active_background_tasks: after.active_background_tasks,
        final_active_blocking_tasks: after.active_blocking_tasks,
        final_active_cpu_slots: after.active_cpu_slots,
        final_active_foreground_io_slots: after.active_foreground_io_slots,
        final_active_background_io_slots: after.active_background_io_slots,
        final_admitted_memory_bytes: after.admitted_memory_bytes,
        final_overcommitted: after.overcommitted,
    }
}

fn collect_runtime_governor_blockers(
    governor: &ProductionContentStoreRuntimeGovernorEvidence,
    expected_runs: usize,
    config: &ProductionContentStoreStorageQualificationConfig,
    blockers: &mut Vec<String>,
) {
    let expected_runs = u64::try_from(expected_runs).unwrap_or(u64::MAX);
    if governor.admissions_delta != expected_runs || governor.completions_delta != expected_runs {
        blockers.push("content_store_runtime_admission_not_observed_for_every_run".to_string());
    }
    if governor.admission_waits_delta != 0 || governor.admission_rejections_delta != 0 {
        blockers.push("content_store_runtime_admission_regression".to_string());
    }
    if governor.final_active_foreground_tasks != 0
        || governor.final_active_background_tasks != 0
        || governor.final_active_blocking_tasks != 0
        || governor.final_active_cpu_slots != 0
        || governor.final_active_foreground_io_slots != 0
        || governor.final_active_background_io_slots != 0
        || governor.final_admitted_memory_bytes != 0
        || governor.final_overcommitted
    {
        blockers.push("content_store_runtime_permit_leak".to_string());
    }
    collect_runtime_memory_policy_blockers(governor, config.resource_profile_kind, blockers);
}

pub(super) fn collect_runtime_memory_policy_blockers(
    governor: &ProductionContentStoreRuntimeGovernorEvidence,
    resource_profile_kind: ContentStoreResourceProfileKind,
    blockers: &mut Vec<String>,
) {
    if governor.memory_capacity_bytes == 0
        || governor.memory_budget_bytes == 0
        || governor.memory_budget_bytes > governor.memory_capacity_bytes
    {
        blockers.push("content_store_runtime_memory_policy_invalid".to_string());
    }
    match resource_profile_kind {
        ContentStoreResourceProfileKind::Capability512Mib => {
            if governor.configured_memory_ceiling_bytes
                != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
                || governor.memory_capacity_bytes > CONTENT_STORE_512_MIB_CAPABILITY_BYTES
            {
                blockers.push("content_store_512_mib_governor_ceiling_not_effective".to_string());
            }
        }
        ContentStoreResourceProfileKind::SharedHost8Gib => {
            if governor.effective_memory_limit_bytes != Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES)
            {
                blockers.push("content_store_shared_host_8_gib_limit_not_observed".to_string());
            }
            if governor.configured_memory_ceiling_bytes.is_some()
                || governor.memory_capacity_bytes > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
                || governor.memory_budget_bytes > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES
            {
                blockers
                    .push("content_store_shared_host_dynamic_memory_policy_mismatch".to_string());
            }
        }
        ContentStoreResourceProfileKind::ConfiguredWorkload => {}
    }
}

fn same_storage_identity(
    left: &ProductionContentStoreResidencyEvidence,
    right: &ProductionContentStoreResidencyEvidence,
) -> bool {
    left.database_commit_epoch == right.database_commit_epoch
        && left.row_base_generation == right.row_base_generation
        && left.row_recovery_delta_generation == right.row_recovery_delta_generation
        && left.row_base_commit_epoch == right.row_base_commit_epoch
        && left.row_visible_commit_epoch == right.row_visible_commit_epoch
        && left.index_base_generation == right.index_base_generation
        && left.index_recovery_delta_generation == right.index_recovery_delta_generation
        && left.index_base_commit_epoch == right.index_base_commit_epoch
        && left.index_visible_commit_epoch == right.index_visible_commit_epoch
}

pub(super) fn statement_digest(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, b"hawdb-production-content-store-statement-v1");
    hash_bytes(&mut hasher, sql.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

pub(super) fn ordered_parameter_digest(parameters: &[Value]) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, b"hawdb-production-content-store-parameters-v1");
    hasher.update((parameters.len() as u64).to_le_bytes());
    for value in parameters {
        hash_value(&mut hasher, value);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence_digest::rows_sha256;
    use crate::nowledge_content_store_sql_corpus;
    use hawdb::PRODUCTION_QUALIFICATION_POLICY_VERSION;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn production_content_store_runner_binds_cold_warm_relational_residency() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "hawdb-production-content-store-{}-{id}",
            std::process::id()
        ));
        let mut seed = super::super::ContentStoreInitialRowPageQualificationConfig::synthetic(
            &path,
            "production-content-store-test",
        );
        seed.base_message_count = 48;
        seed.message_payload_bytes = 8 * 1024;
        seed.base_chunk_count = 48;
        seed.chunk_payload_bytes = 8 * 1024;
        seed.segment_cache_capacity_bytes = 2 * 1024 * 1024;
        super::super::fixture::bootstrap_checkpoint(
            &seed,
            &nowledge_content_store_sql_corpus().unwrap(),
        )
        .unwrap();

        let mut database_config =
            super::super::fixture::database_config(&seed, RelationalIndexMode::Authoritative);
        database_config.read_only = true;
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let mut database = Database::open_with_durability_and_config(
            &path,
            DurabilityPolicy::SyncOnEveryWrite,
            database_config.clone(),
        )
        .unwrap();
        // The residual OR makes the anchor lookup a broad access on this
        // single-thread fixture. The ordered page exercises the index path.
        let cases = [
            (
                "thread-message-point",
                "thread_message_anchor_lookup",
                vec![
                    Value::String("thread-storage-1".to_string()),
                    Value::String("content-message-00000000".to_string()),
                    Value::Int(1),
                ],
            ),
            (
                "thread-message-ordered-page",
                "thread_activity_timestamps",
                vec![
                    Value::String("thread-storage-1".to_string()),
                    Value::Int(1),
                    Value::Int(0),
                ],
            ),
        ];
        let read_cases = cases
            .into_iter()
            .map(|(case_name, statement_name, parameters)| {
                let statement = corpus.statement(statement_name).unwrap();
                let output = database
                    .query_sql_with_params_options(
                        &statement.sql,
                        &parameters,
                        hawdb::QueryStreamOptions {
                            max_rows: Some(statement.max_rows),
                            max_payload_bytes: Some(statement.max_payload_bytes),
                        },
                    )
                    .unwrap();
                assert_eq!(output.rows.len(), 1);
                ProductionContentStoreReadCase {
                    case_name: case_name.to_string(),
                    statement_name: statement.name.clone(),
                    parameters,
                    expected_output_rows: 1,
                    expected_output_sha256: rows_sha256(&output.rows),
                    max_intermediate_rows: 256,
                    max_physical_pages_per_run: 1024,
                    max_physical_bytes_per_run: 128 * 1024 * 1024,
                }
            })
            .collect();
        let commit_epoch = database.commit_epoch();
        drop(database);

        let identity = ProductionQualificationIdentity {
            source_revision: "production-content-store-test".to_string(),
            rust_toolchain: "rustc-test".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "production-content-store-config".to_string(),
            deployment_profile: "representative-production-replica".to_string(),
            dataset_fingerprint: "production-content-store-dataset".to_string(),
            canonical_graph_commit_epoch: commit_epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let mut runtime_governor_config = RuntimeGovernorConfig::shared_host();
        runtime_governor_config.memory_budget_bytes = Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES);
        let report = run_production_content_store_storage_qualification(
            ProductionContentStoreStorageQualificationConfig {
                database_path: path.clone(),
                database_config,
                runtime_governor_config,
                resource_profile_kind: ContentStoreResourceProfileKind::Capability512Mib,
                configured_available_memory_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                evidence_binding: ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                expected_identity: identity.clone(),
                measurement_runs: 2,
                open_payload_cache_limits: ProductionContentStoreOpenCacheLimits {
                    max_requests: 16,
                    max_resident_bytes: 2 * 1024 * 1024,
                },
                resource_limits: ProductionContentStoreResourceLimits {
                    max_steady_resident_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_peak_resident_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_total_page_faults_per_run: None,
                    max_minor_page_faults_per_run: None,
                    max_major_page_faults_per_run: None,
                },
                read_cases,
            },
        )
        .unwrap();

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        let release = crate::evaluate_production_release_qualification_bundle(
            crate::ProductionReleaseQualificationArtifacts {
                content_store_512_mib_read: Some(report.json()),
                ..crate::ProductionReleaseQualificationArtifacts::default()
            },
            identity,
            crate::ProductionReleaseQualificationPolicy::default(),
        );
        assert!(
            release.content_store_512_mib_read.ready,
            "unexpected release blockers: {:?}",
            release.content_store_512_mib_read.blocker_codes
        );
        assert_eq!(report.opens.len(), 2);
        for open in &report.opens {
            assert!(open.open_timings.consistent);
            assert!(open.open_timings.total_open_micros <= open.latency_micros);
            assert!(open.payload_cache.within_limits);
            assert!(open.payload_cache.miss_count > 0);
            assert!(open.payload_cache.resident_bytes > 0);
        }
        assert_eq!(report.runs.len(), 4);
        assert_eq!(report.runtime_governor.admissions_delta, 4);
        assert_eq!(report.runtime_governor.completions_delta, 4);
        assert_eq!(report.runtime_governor.final_admitted_memory_bytes, 0);
        assert!(
            report.runtime_governor.memory_capacity_bytes <= CONTENT_STORE_512_MIB_CAPABILITY_BYTES
        );
        assert!(report.initial_residency.row_artifact_exceeds_cache);
        assert!(report.initial_residency.index_artifact_exceeds_cache);
        assert!(report.initial_residency.row_index_epoch_aligned);
        assert_eq!(report.initial_residency.segment_cache_pinned_bytes, 0);
        for runs in report.runs.chunks_exact(2) {
            assert!(runs[0].read.cache.misses > 0);
            assert!(runs[0].read.execution.physical_pages > 0);
            assert!(runs[1].read.cache.hits > 0);
        }
        assert_eq!(report.runs[0].read.execution.index_runtime_path, "none");
        assert_eq!(
            report.runs[2].read.execution.index_runtime_path,
            "authoritative"
        );
        assert!(report.runs[2].read.execution.index_physical_pages > 0);
        assert!(report
            .runs
            .iter()
            .all(|run| run.read.execution.intermediate_rows > 0));
        let json = report.json().to_string();
        assert!(!json.contains(path.to_string_lossy().as_ref()));
        assert!(!json.contains("thread-storage-1"));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn lifecycle_faults_are_not_compared_with_per_run_limits() {
        let limits = ProductionContentStoreResourceLimits {
            max_steady_resident_bytes: 1024,
            max_peak_resident_bytes: 1024,
            max_total_page_faults_per_run: Some(1),
            max_minor_page_faults_per_run: Some(1),
            max_major_page_faults_per_run: Some(1),
        };
        let process = ContentStoreProcessResourceEvidence {
            resident_memory_supported: true,
            total_page_faults_supported: true,
            split_page_faults_supported: true,
            start_resident_bytes: 512,
            steady_resident_bytes: 512,
            peak_resident_bytes: 512,
            steady_resident_growth_bytes: 0,
            lifetime_peak_resident_growth_bytes: 0,
            total_page_faults: Some(100),
            minor_page_faults: Some(90),
            major_page_faults: Some(10),
        };
        let mut lifecycle_blockers = Vec::new();
        collect_resident_blockers(
            &process,
            limits,
            "content_store_lifecycle",
            &mut lifecycle_blockers,
        );
        assert!(lifecycle_blockers.is_empty());

        let mut run_blockers = Vec::new();
        collect_run_process_blockers(&process, limits, "content_store_read", &mut run_blockers);
        assert_eq!(run_blockers.len(), 3);
    }

    #[test]
    fn production_content_store_runner_requires_read_only_authoritative_open() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-production-content-store-invalid-{}-{}",
            std::process::id(),
            TEST_ID.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let identity = ProductionQualificationIdentity {
            source_revision: "revision".to_string(),
            rust_toolchain: "rustc-test".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "config".to_string(),
            deployment_profile: "representative-production-replica".to_string(),
            dataset_fingerprint: "dataset".to_string(),
            canonical_graph_commit_epoch: 1,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let error = run_production_content_store_storage_qualification(
            ProductionContentStoreStorageQualificationConfig {
                database_path: path.clone(),
                database_config: DatabaseConfig::default(),
                runtime_governor_config: RuntimeGovernorConfig::shared_host(),
                resource_profile_kind: ContentStoreResourceProfileKind::ConfiguredWorkload,
                configured_available_memory_bytes: 1024,
                evidence_binding: ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                expected_identity: identity,
                measurement_runs: 2,
                open_payload_cache_limits: ProductionContentStoreOpenCacheLimits {
                    max_requests: 1,
                    max_resident_bytes: 1,
                },
                resource_limits: ProductionContentStoreResourceLimits {
                    max_steady_resident_bytes: 1024,
                    max_peak_resident_bytes: 1024,
                    max_total_page_faults_per_run: None,
                    max_minor_page_faults_per_run: None,
                    max_major_page_faults_per_run: None,
                },
                read_cases: vec![ProductionContentStoreReadCase {
                    case_name: "case".to_string(),
                    statement_name: "thread_messages_page".to_string(),
                    parameters: vec![Value::String("thread".to_string()), Value::Int(1)],
                    expected_output_rows: 0,
                    expected_output_sha256: "0".repeat(64),
                    max_intermediate_rows: 1,
                    max_physical_pages_per_run: 1,
                    max_physical_bytes_per_run: 1,
                }],
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("read_only"));
        std::fs::remove_dir_all(path).unwrap();
    }
}
