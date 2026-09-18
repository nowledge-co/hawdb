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
use super::production::{
    collect_run_process_blockers, collect_runtime_memory_policy_blockers, ordered_parameter_digest,
    residency_evidence, runtime_governor_evidence, statement_digest, validate_read_cases,
    validate_resource_profile,
};
use super::resource::{process_evidence, regular_file_bytes_by_name, runtime_memory_evidence};
use super::{
    ContentStoreProcessResourceEvidence, ContentStoreResourceProfileKind,
    ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport, ContentStoreRuntimeMemoryEvidence,
    ProductionContentStoreReadCase, ProductionContentStoreReadContractEvidence,
    ProductionContentStoreResidencyEvidence, ProductionContentStoreResourceLimits,
    ProductionContentStoreRuntimeGovernorEvidence,
};
use crate::production_graph::validate_production_identity_for_current_target;
use crate::{
    nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSchemaIdentity, ContentStoreSqlCorpus, ContentStoreSqlCorpusIdentity,
};
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, IoConcurrencyBudget,
    ProcessMemoryProfile, ProcessMemorySnapshot, ProductionEvidenceBinding,
    ProductionQualificationIdentity, RelationalIndexMode, RelationalOverflowCompactionConfig,
    RelationalOverflowCompactionReport, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeWorkRequest, StorageDeviceProfile, StorageReclamationWatermark,
    StorageResidencyMode, StorageScrubReport, Value,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

pub const PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL: &str =
    "hawdb-production-content-store-overflow-compaction-qualification-v1";

const CLEANUP_MARKER_QUERY: &str =
    "CREATE (:HawDBQualificationMarker {id: $id, purpose: $purpose})";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOverflowCompactionLimits {
    pub process: ProductionContentStoreResourceLimits,
    pub max_compaction_elapsed_micros: u64,
    pub max_cleanup_elapsed_micros: u64,
    pub max_new_generation_artifact_bytes: u64,
    pub max_new_artifact_write_amplification_per_million: u64,
    pub min_reclaimable_base_extent_count: u64,
    pub min_physically_removed_extent_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionRelationalOverflowCompactionPolicyEvidence {
    pub max_scan_rows: usize,
    pub max_scan_pages: usize,
    pub max_scan_bytes: usize,
    pub max_overlay_entries: usize,
    pub max_overlay_bytes: usize,
    pub max_rewrite_bytes: u64,
    pub max_sort_memory_bytes: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub max_reference_occurrences: u64,
    pub admission_bytes: u64,
}

impl ProductionRelationalOverflowCompactionPolicyEvidence {
    fn from_config(config: RelationalOverflowCompactionConfig) -> Result<Self, HawDBError> {
        Ok(Self {
            max_scan_rows: config.max_scan_rows.get(),
            max_scan_pages: config.max_scan_pages.get(),
            max_scan_bytes: config.max_scan_bytes.get(),
            max_overlay_entries: config.max_overlay_entries.get(),
            max_overlay_bytes: config.max_overlay_bytes.get(),
            max_rewrite_bytes: config.max_rewrite_bytes.get(),
            max_sort_memory_bytes: config.reference_sort.max_memory_bytes.get(),
            max_spill_bytes: config.reference_sort.max_spill_bytes.get(),
            max_spill_runs: config.reference_sort.max_runs.get(),
            max_reference_occurrences: config.reference_sort.max_reference_occurrences.get(),
            admission_bytes: config.admission_bytes()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreOverflowCompactionQualificationConfig {
    /// Existing caller-owned disposable writable replica. The runner never
    /// copies or mutates the source production database.
    pub replica_path: PathBuf,
    pub database_config: DatabaseConfig,
    pub runtime_governor_config: RuntimeGovernorConfig,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub compaction: RelationalOverflowCompactionConfig,
    pub limits: ProductionContentStoreOverflowCompactionLimits,
    pub verification_cases: Vec<ProductionContentStoreReadCase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionRelationalOverflowCompactionEvidence {
    pub source_commit_epoch: u64,
    pub published_generation: u64,
    pub tables_scanned: usize,
    pub rows_scanned: usize,
    pub pages_read: usize,
    pub row_bytes_read: usize,
    pub hydrated_values: usize,
    pub overlay_entries: usize,
    pub overlay_bytes: usize,
    pub reference_occurrences: u64,
    pub unique_references: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_sort_memory_bytes: usize,
    pub previous_extent_count: u64,
    pub published_extent_count: u64,
    pub reclaimable_base_extent_count: u64,
    pub new_extent_count: u64,
    pub reused_extent_count: u64,
    pub copied_base_extent_count: u64,
    pub introduced_extent_count: u64,
    pub admitted_memory_bytes: u64,
}

impl From<RelationalOverflowCompactionReport> for ProductionRelationalOverflowCompactionEvidence {
    fn from(report: RelationalOverflowCompactionReport) -> Self {
        Self {
            source_commit_epoch: report.source_commit_epoch,
            published_generation: report.published_generation,
            tables_scanned: report.tables_scanned,
            rows_scanned: report.rows_scanned,
            pages_read: report.pages_read,
            row_bytes_read: report.row_bytes_read,
            hydrated_values: report.hydrated_values,
            overlay_entries: report.overlay_entries,
            overlay_bytes: report.overlay_bytes,
            reference_occurrences: report.reference_occurrences,
            unique_references: report.unique_references,
            spill_run_count: report.spill_run_count,
            spill_bytes: report.spill_bytes,
            peak_sort_memory_bytes: report.peak_sort_memory_bytes,
            previous_extent_count: report.previous_extent_count,
            published_extent_count: report.published_extent_count,
            reclaimable_base_extent_count: report.reclaimable_base_extent_count,
            new_extent_count: report.new_extent_count,
            reused_extent_count: report.reused_extent_count,
            copied_base_extent_count: report.copied_base_extent_count,
            introduced_extent_count: report.introduced_extent_count,
            admitted_memory_bytes: report.admitted_memory_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOverflowVerificationEvidence {
    pub case_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub expected_output_sha256: String,
    pub read: ContentStoreRowPageReadReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionStorageReclamationWatermarkEvidence {
    pub current_commit_epoch: u64,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub oldest_reader_commit_epoch: Option<u64>,
    pub safe_reclaim_commit_epoch: u64,
    pub durable: bool,
}

impl From<StorageReclamationWatermark> for ProductionStorageReclamationWatermarkEvidence {
    fn from(watermark: StorageReclamationWatermark) -> Self {
        Self {
            current_commit_epoch: watermark.current_commit_epoch,
            checkpoint_epoch: watermark.checkpoint_epoch,
            checkpoint_commit_epoch: watermark.checkpoint_commit_epoch,
            oldest_reader_commit_epoch: watermark.oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch: watermark.safe_reclaim_commit_epoch,
            durable: watermark.durable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionStorageScrubEvidence {
    pub generation: u64,
    pub checked_file_count: usize,
    pub checked_bytes: u64,
    pub sha256_verified_file_count: usize,
    pub wal_record_count: usize,
    pub wal_bytes: u64,
}

impl From<StorageScrubReport> for ProductionStorageScrubEvidence {
    fn from(report: StorageScrubReport) -> Self {
        Self {
            generation: report.generation,
            checked_file_count: report.checked_file_count,
            checked_bytes: report.checked_bytes,
            sha256_verified_file_count: report.sha256_verified_file_count,
            wal_record_count: report.wal_record_count,
            wal_bytes: report.wal_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreOverflowArtifactEvidence {
    pub files_before: usize,
    pub files_after_compaction: usize,
    pub files_after_cleanup: usize,
    pub new_generation_artifact_bytes: u64,
    pub published_live_overflow_extent_bytes: u64,
    pub new_artifact_write_amplification_per_million: u64,
    pub physically_removed_extent_files: usize,
    pub physically_removed_extent_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionContentStoreOverflowCompactionQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: ProductionEvidenceBinding,
    pub corpus: ContentStoreSqlCorpusIdentity,
    pub schema: ContentStoreSchemaIdentity,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub limits: ProductionContentStoreOverflowCompactionLimits,
    pub read_contracts: Vec<ProductionContentStoreReadContractEvidence>,
    pub runtime_memory: ContentStoreRuntimeMemoryEvidence,
    pub runtime_governor: ProductionContentStoreRuntimeGovernorEvidence,
    pub compaction_elapsed_micros: u64,
    pub cleanup_elapsed_micros: u64,
    pub compaction_process: ContentStoreProcessResourceEvidence,
    pub compaction_policy: ProductionRelationalOverflowCompactionPolicyEvidence,
    pub compaction: ProductionRelationalOverflowCompactionEvidence,
    pub artifacts: ProductionContentStoreOverflowArtifactEvidence,
    pub initial_residency: ProductionContentStoreResidencyEvidence,
    pub compacted_residency: ProductionContentStoreResidencyEvidence,
    pub final_residency: ProductionContentStoreResidencyEvidence,
    pub reopened_residency: ProductionContentStoreResidencyEvidence,
    pub reclamation: ProductionStorageReclamationWatermarkEvidence,
    pub scrub: ProductionStorageScrubEvidence,
    pub before_reads: Vec<ProductionContentStoreOverflowVerificationEvidence>,
    pub compacted_reads: Vec<ProductionContentStoreOverflowVerificationEvidence>,
    pub reopened_reads: Vec<ProductionContentStoreOverflowVerificationEvidence>,
}

impl ProductionContentStoreOverflowCompactionQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_CONTENT_STORE_OVERFLOW_COMPACTION_QUALIFICATION_PROTOCOL,
            "evidence_kind": "representative_production_relational_overflow_compaction",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "evidence_binding": self.evidence_binding.json(),
            "corpus": self.corpus,
            "schema": self.schema,
            "resource_profile_kind": self.resource_profile_kind,
            "configured_available_memory_bytes": self.configured_available_memory_bytes,
            "limits": self.limits,
            "read_contracts": self.read_contracts,
            "runtime_memory": self.runtime_memory,
            "runtime_governor": self.runtime_governor,
            "compaction_elapsed_micros": self.compaction_elapsed_micros,
            "cleanup_elapsed_micros": self.cleanup_elapsed_micros,
            "compaction_process": self.compaction_process,
            "compaction_policy": self.compaction_policy,
            "compaction": self.compaction,
            "artifacts": self.artifacts,
            "initial_residency": self.initial_residency,
            "compacted_residency": self.compacted_residency,
            "final_residency": self.final_residency,
            "reopened_residency": self.reopened_residency,
            "reclamation": self.reclamation,
            "scrub": self.scrub,
            "before_reads": self.before_reads,
            "compacted_reads": self.compacted_reads,
            "reopened_reads": self.reopened_reads,
        })
    }
}

/// Qualifies exact overflow compaction against an existing caller-owned
/// disposable replica. The runner deliberately mutates that replica and never
/// copies or opens the source production database.
pub fn run_production_content_store_overflow_compaction_qualification(
    config: ProductionContentStoreOverflowCompactionQualificationConfig,
) -> Result<ProductionContentStoreOverflowCompactionQualificationReport, HawDBError> {
    let corpus = nowledge_content_store_sql_corpus()?;
    validate_config(&config, &corpus)?;

    let runtime_memory = runtime_memory_evidence(RuntimeMemorySnapshot::detect());
    let compaction_policy =
        ProductionRelationalOverflowCompactionPolicyEvidence::from_config(config.compaction)?;
    let storage_io = IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::detect(
        &config.replica_path,
    ));
    let governor = RuntimeGovernor::detect(config.runtime_governor_config, storage_io);
    if config.compaction.admission_bytes()? > governor.snapshot().limits.memory_budget_bytes {
        return Err(HawDBError::Semantic(
            "production overflow compaction admission exceeds the effective runtime memory budget"
                .to_string(),
        ));
    }
    let governor_before = governor.snapshot();
    let mut database = Database::open_with_durability_and_config(
        &config.replica_path,
        DurabilityPolicy::SyncOnEveryWrite,
        config.database_config.clone(),
    )?;
    database.set_runtime_governor(governor.clone());

    let initial_residency = residency_evidence(
        database.commit_epoch(),
        &database.storage_residency_report(),
    );
    let mut blocker_codes = Vec::new();
    collect_initial_blockers(&config, &initial_residency, &mut blocker_codes);
    let read_contracts = read_contracts(&config.verification_cases, &corpus);
    let before_reads = verify_cases(
        &mut database,
        &config.verification_cases,
        &corpus,
        &governor,
        &config.database_config,
        ContentStoreRowPageReadPhase::ProductionCold,
        &mut blocker_codes,
    )?;

    let files_before = regular_file_bytes_by_name(&config.replica_path)?;
    let process_before = ProcessMemorySnapshot::capture()?;
    let compaction_started = Instant::now();
    let compaction = database.compact_relational_overflow(config.compaction)?;
    let compaction_elapsed_micros = elapsed_micros(compaction_started);
    let process_after = ProcessMemorySnapshot::capture()?;
    let compaction_process =
        process_evidence(ProcessMemoryProfile::between(process_before, process_after));
    let compaction = ProductionRelationalOverflowCompactionEvidence::from(compaction);
    let files_after_compaction = regular_file_bytes_by_name(&config.replica_path)?;
    let compacted_residency = residency_evidence(
        database.commit_epoch(),
        &database.storage_residency_report(),
    );
    let compacted_reads = verify_cases(
        &mut database,
        &config.verification_cases,
        &corpus,
        &governor,
        &config.database_config,
        ContentStoreRowPageReadPhase::ProductionWarm,
        &mut blocker_codes,
    )?;

    let cleanup_started = Instant::now();
    let marker_epoch = publish_cleanup_marker(&mut database, compaction.published_generation)?;
    database.checkpoint()?;
    let cleanup_elapsed_micros = elapsed_micros(cleanup_started);
    let final_residency = residency_evidence(
        database.commit_epoch(),
        &database.storage_residency_report(),
    );
    let reclamation = database.storage_reclamation_watermark().into();
    let scrub = database.scrub_storage()?.into();
    let files_after_cleanup = regular_file_bytes_by_name(&config.replica_path)?;
    drop(database);

    let mut reopened = Database::open_with_durability_and_config(
        &config.replica_path,
        DurabilityPolicy::SyncOnEveryWrite,
        config.database_config.clone(),
    )?;
    reopened.set_runtime_governor(governor.clone());
    let reopened_residency = residency_evidence(
        reopened.commit_epoch(),
        &reopened.storage_residency_report(),
    );
    let reopened_reads = verify_cases(
        &mut reopened,
        &config.verification_cases,
        &corpus,
        &governor,
        &config.database_config,
        ContentStoreRowPageReadPhase::WalRecovery,
        &mut blocker_codes,
    )?;
    drop(reopened);

    let new_generation_artifact_bytes =
        created_or_grown_bytes(&files_before, &files_after_compaction);
    let (physically_removed_extent_files, physically_removed_extent_bytes) =
        removed_overflow_extents(&files_before, &files_after_cleanup);
    let artifacts = ProductionContentStoreOverflowArtifactEvidence {
        files_before: files_before.len(),
        files_after_compaction: files_after_compaction.len(),
        files_after_cleanup: files_after_cleanup.len(),
        new_generation_artifact_bytes,
        published_live_overflow_extent_bytes: compacted_residency
            .row_overflow_extent_artifact_bytes,
        new_artifact_write_amplification_per_million: ratio_per_million(
            new_generation_artifact_bytes,
            compacted_residency.row_overflow_extent_artifact_bytes,
        ),
        physically_removed_extent_files,
        physically_removed_extent_bytes,
    };
    let runtime_governor = runtime_governor_evidence(
        config.runtime_governor_config,
        governor_before,
        governor.snapshot(),
    );
    collect_result_blockers(
        ResultBlockerInputs {
            config: &config,
            initial: &initial_residency,
            compacted: &compacted_residency,
            final_residency: &final_residency,
            reopened: &reopened_residency,
            marker_epoch,
            compaction_elapsed_micros,
            cleanup_elapsed_micros,
            process: &compaction_process,
            compaction: &compaction,
            artifacts: &artifacts,
            governor: &runtime_governor,
            reclamation: &reclamation,
            scrub: &scrub,
        },
        &mut blocker_codes,
    );
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(
        ProductionContentStoreOverflowCompactionQualificationReport {
            ready: blocker_codes.is_empty(),
            blocker_codes,
            evidence_binding: config.evidence_binding,
            corpus: corpus.identity(),
            schema: nowledge_content_store_schema_identity(),
            resource_profile_kind: config.resource_profile_kind,
            configured_available_memory_bytes: config.configured_available_memory_bytes,
            limits: config.limits,
            read_contracts,
            runtime_memory,
            runtime_governor,
            compaction_elapsed_micros,
            cleanup_elapsed_micros,
            compaction_process,
            compaction_policy,
            compaction,
            artifacts,
            initial_residency,
            compacted_residency,
            final_residency,
            reopened_residency,
            reclamation,
            scrub,
            before_reads,
            compacted_reads,
            reopened_reads,
        },
    )
}

fn validate_config(
    config: &ProductionContentStoreOverflowCompactionQualificationConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(), HawDBError> {
    if !config.replica_path.is_dir() {
        return Err(HawDBError::Semantic(
            "production overflow compaction qualification requires an existing replica directory"
                .to_string(),
        ));
    }
    if config.database_config.read_only {
        return Err(HawDBError::Semantic(
            "production overflow compaction qualification requires a writable disposable replica"
                .to_string(),
        ));
    }
    if config.database_config.storage_residency_mode != StorageResidencyMode::OutOfCore
        || config.database_config.relational_index_mode != RelationalIndexMode::Authoritative
        || config.database_config.segment_cache_capacity_bytes == 0
    {
        return Err(HawDBError::Semantic(
            "production overflow compaction qualification requires out-of-core authoritative storage with a non-zero segment cache"
                .to_string(),
        ));
    }
    validate_resource_profile(
        config.resource_profile_kind,
        config.configured_available_memory_bytes,
        config.runtime_governor_config,
        config.limits.process,
    )?;
    validate_production_identity_for_current_target(
        &config.evidence_binding,
        &config.expected_identity,
    )
    .map_err(|error| HawDBError::Semantic(error.to_string()))?;
    validate_read_cases(
        &config.verification_cases,
        corpus,
        config.runtime_governor_config.result_budget_bytes,
    )?;
    let limits = config.limits;
    if limits.max_compaction_elapsed_micros == 0
        || limits.max_cleanup_elapsed_micros == 0
        || limits.max_new_generation_artifact_bytes == 0
        || limits.max_new_artifact_write_amplification_per_million == 0
        || limits.min_reclaimable_base_extent_count == 0
        || limits.min_physically_removed_extent_bytes == 0
    {
        return Err(HawDBError::Semantic(
            "production overflow compaction qualification limits must all be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn read_contracts(
    cases: &[ProductionContentStoreReadCase],
    corpus: &ContentStoreSqlCorpus,
) -> Vec<ProductionContentStoreReadContractEvidence> {
    cases
        .iter()
        .map(|case| {
            let statement = corpus
                .statement(&case.statement_name)
                .expect("validated overflow compaction verification statement");
            ProductionContentStoreReadContractEvidence {
                case_name: case.case_name.clone(),
                statement_name: case.statement_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: ordered_parameter_digest(&case.parameters),
                expected_output_rows: case.expected_output_rows,
                expected_output_sha256: case.expected_output_sha256.clone(),
                max_output_rows: statement.max_rows,
                max_output_payload_bytes: statement.max_payload_bytes,
                max_intermediate_rows: case.max_intermediate_rows,
                max_physical_pages_per_run: case.max_physical_pages_per_run,
                max_physical_bytes_per_run: case.max_physical_bytes_per_run,
            }
        })
        .collect()
}

fn verify_cases(
    database: &mut Database,
    cases: &[ProductionContentStoreReadCase],
    corpus: &ContentStoreSqlCorpus,
    governor: &RuntimeGovernor,
    database_config: &DatabaseConfig,
    phase: ContentStoreRowPageReadPhase,
    blockers: &mut Vec<String>,
) -> Result<Vec<ProductionContentStoreOverflowVerificationEvidence>, HawDBError> {
    cases
        .iter()
        .map(|case| {
            let statement = corpus
                .statement(&case.statement_name)
                .expect("validated overflow compaction verification statement");
            let result_bytes = u64::try_from(statement.max_payload_bytes).unwrap_or(u64::MAX);
            let working_memory_bytes = u64::try_from(
                database_config
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
                        "production overflow compaction verification admission failed: {error}"
                    ))
                })?;
            let read = execute_qualified_read(
                database,
                statement,
                case.parameters.clone(),
                phase,
                case.expected_output_rows,
            )?;
            if read.output_sha256 != case.expected_output_sha256 {
                blockers.push("overflow_compaction_output_digest_mismatch".to_string());
            }
            if read.execution.intermediate_rows > case.max_intermediate_rows {
                blockers.push("overflow_compaction_intermediate_row_budget_exceeded".to_string());
            }
            if read
                .execution
                .physical_pages
                .saturating_add(read.execution.index_physical_pages)
                > case.max_physical_pages_per_run
            {
                blockers.push("overflow_compaction_verification_page_budget_exceeded".to_string());
            }
            if read
                .execution
                .physical_bytes
                .saturating_add(read.execution.index_physical_bytes)
                > case.max_physical_bytes_per_run
            {
                blockers.push("overflow_compaction_verification_byte_budget_exceeded".to_string());
            }
            if read.cache.pinned_bytes_after != 0
                || read.execution.cache_admission_rejections != 0
                || read.execution.index_cache_admission_rejections != 0
            {
                blockers.push("overflow_compaction_verification_cache_regression".to_string());
            }
            Ok(ProductionContentStoreOverflowVerificationEvidence {
                case_name: case.case_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: ordered_parameter_digest(&case.parameters),
                expected_output_sha256: case.expected_output_sha256.clone(),
                read,
            })
        })
        .collect()
}

fn collect_initial_blockers(
    config: &ProductionContentStoreOverflowCompactionQualificationConfig,
    residency: &ProductionContentStoreResidencyEvidence,
    blockers: &mut Vec<String>,
) {
    if residency.database_commit_epoch != config.expected_identity.canonical_graph_commit_epoch {
        blockers.push("overflow_compaction_replica_epoch_identity_mismatch".to_string());
    }
    if !residency.row_serving
        || !residency.index_serving
        || !residency.row_index_epoch_aligned
        || residency.row_visible_commit_epoch != Some(residency.database_commit_epoch)
        || residency.index_visible_commit_epoch != Some(residency.database_commit_epoch)
    {
        blockers.push("overflow_compaction_initial_storage_identity_invalid".to_string());
    }
    if residency.row_materialized_rows_resident || !residency.row_checkpoint_state_metadata_only {
        blockers.push("overflow_compaction_initial_rows_not_metadata_only".to_string());
    }
    if residency.row_overflow_extent_count == 0 || residency.row_overflow_extent_artifact_bytes == 0
    {
        blockers.push("overflow_compaction_initial_overflow_missing".to_string());
    }
    if !residency.row_artifact_exceeds_cache {
        blockers.push("overflow_compaction_artifact_does_not_exceed_cache".to_string());
    }
    if residency.segment_cache_pinned_bytes != 0 {
        blockers.push("overflow_compaction_initial_cache_pin_leak".to_string());
    }
}

fn publish_cleanup_marker(
    database: &mut Database,
    published_generation: u64,
) -> Result<u64, HawDBError> {
    let mut parameters = BTreeMap::new();
    parameters.insert(
        "id".to_string(),
        Value::String(format!("overflow-compaction-{published_generation}")),
    );
    parameters.insert(
        "purpose".to_string(),
        Value::String("physical-reclamation-evidence".to_string()),
    );
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(CLEANUP_MARKER_QUERY, &parameters)?;
    transaction.commit()?;
    Ok(database.commit_epoch())
}

struct ResultBlockerInputs<'a> {
    config: &'a ProductionContentStoreOverflowCompactionQualificationConfig,
    initial: &'a ProductionContentStoreResidencyEvidence,
    compacted: &'a ProductionContentStoreResidencyEvidence,
    final_residency: &'a ProductionContentStoreResidencyEvidence,
    reopened: &'a ProductionContentStoreResidencyEvidence,
    marker_epoch: u64,
    compaction_elapsed_micros: u64,
    cleanup_elapsed_micros: u64,
    process: &'a ContentStoreProcessResourceEvidence,
    compaction: &'a ProductionRelationalOverflowCompactionEvidence,
    artifacts: &'a ProductionContentStoreOverflowArtifactEvidence,
    governor: &'a ProductionContentStoreRuntimeGovernorEvidence,
    reclamation: &'a ProductionStorageReclamationWatermarkEvidence,
    scrub: &'a ProductionStorageScrubEvidence,
}

fn collect_result_blockers(inputs: ResultBlockerInputs<'_>, blockers: &mut Vec<String>) {
    let limits = inputs.config.limits;
    collect_run_process_blockers(
        inputs.process,
        limits.process,
        "overflow_compaction",
        blockers,
    );
    if inputs.compaction_elapsed_micros > limits.max_compaction_elapsed_micros {
        blockers.push("overflow_compaction_elapsed_budget_exceeded".to_string());
    }
    if inputs.cleanup_elapsed_micros > limits.max_cleanup_elapsed_micros {
        blockers.push("overflow_compaction_cleanup_elapsed_budget_exceeded".to_string());
    }
    if inputs.compaction.source_commit_epoch != inputs.initial.database_commit_epoch
        || inputs.compaction.published_generation
            != inputs.compacted.row_base_generation.unwrap_or(0)
        || inputs.compacted.row_base_generation != inputs.compacted.index_base_generation
        || inputs.compacted.row_visible_commit_epoch != Some(inputs.initial.database_commit_epoch)
        || inputs.compacted.index_visible_commit_epoch != Some(inputs.initial.database_commit_epoch)
    {
        blockers.push("overflow_compaction_published_identity_mismatch".to_string());
    }
    if inputs.compaction.hydrated_values != 0 {
        blockers.push("overflow_compaction_hydrated_payload".to_string());
    }
    if inputs.compaction.reclaimable_base_extent_count < limits.min_reclaimable_base_extent_count {
        blockers.push("overflow_compaction_reclaimable_extent_evidence_missing".to_string());
    }
    if inputs.compaction.reused_extent_count != 0
        || inputs.compaction.new_extent_count != inputs.compaction.published_extent_count
        || inputs
            .compaction
            .copied_base_extent_count
            .saturating_add(inputs.compaction.introduced_extent_count)
            != inputs.compaction.new_extent_count
    {
        blockers.push("overflow_compaction_exact_rewrite_shape_invalid".to_string());
    }
    if inputs.compaction.admitted_memory_bytes
        != inputs
            .config
            .compaction
            .admission_bytes()
            .unwrap_or(u64::MAX)
        || inputs.compaction.peak_sort_memory_bytes
            > inputs
                .config
                .compaction
                .reference_sort
                .max_memory_bytes
                .get()
        || inputs.compaction.spill_bytes
            > inputs
                .config
                .compaction
                .reference_sort
                .max_spill_bytes
                .get()
        || inputs.compaction.spill_run_count
            > inputs.config.compaction.reference_sort.max_runs.get()
    {
        blockers.push("overflow_compaction_internal_budget_evidence_invalid".to_string());
    }
    if inputs.artifacts.new_generation_artifact_bytes > limits.max_new_generation_artifact_bytes {
        blockers.push("overflow_compaction_new_artifact_budget_exceeded".to_string());
    }
    if inputs
        .artifacts
        .new_artifact_write_amplification_per_million
        > limits.max_new_artifact_write_amplification_per_million
    {
        blockers.push("overflow_compaction_write_amplification_budget_exceeded".to_string());
    }
    if inputs.artifacts.physically_removed_extent_bytes < limits.min_physically_removed_extent_bytes
        || inputs.artifacts.physically_removed_extent_files == 0
    {
        blockers.push("overflow_compaction_physical_reclamation_missing".to_string());
    }
    if inputs.marker_epoch != inputs.initial.database_commit_epoch.saturating_add(1)
        || inputs.final_residency.database_commit_epoch != inputs.marker_epoch
        || inputs.reopened.database_commit_epoch != inputs.marker_epoch
        || inputs.final_residency.row_visible_commit_epoch != Some(inputs.marker_epoch)
        || inputs.final_residency.index_visible_commit_epoch != Some(inputs.marker_epoch)
        || inputs.reopened.row_visible_commit_epoch != Some(inputs.marker_epoch)
        || inputs.reopened.index_visible_commit_epoch != Some(inputs.marker_epoch)
        || !inputs.final_residency.row_index_epoch_aligned
        || !inputs.reopened.row_index_epoch_aligned
    {
        blockers.push("overflow_compaction_cleanup_reopen_identity_mismatch".to_string());
    }
    if !inputs.reclamation.durable
        || inputs.reclamation.oldest_reader_commit_epoch.is_some()
        || inputs.reclamation.safe_reclaim_commit_epoch < inputs.initial.database_commit_epoch
    {
        blockers.push("overflow_compaction_reclamation_watermark_invalid".to_string());
    }
    if inputs.scrub.generation != inputs.final_residency.row_base_generation.unwrap_or(0)
        || inputs.scrub.checked_file_count == 0
        || inputs.scrub.checked_bytes == 0
    {
        blockers.push("overflow_compaction_scrub_evidence_invalid".to_string());
    }
    if inputs.governor.admissions_delta == 0
        || inputs.governor.admissions_delta != inputs.governor.completions_delta
        || inputs.governor.admission_rejections_delta != 0
        || inputs.governor.final_active_foreground_tasks != 0
        || inputs.governor.final_active_background_tasks != 0
        || inputs.governor.final_active_blocking_tasks != 0
        || inputs.governor.final_admitted_memory_bytes != 0
        || inputs.governor.final_overcommitted
    {
        blockers.push("overflow_compaction_runtime_admission_invalid".to_string());
    }
    collect_runtime_memory_policy_blockers(
        inputs.governor,
        inputs.config.resource_profile_kind,
        blockers,
    );
}

fn created_or_grown_bytes(before: &BTreeMap<String, u64>, after: &BTreeMap<String, u64>) -> u64 {
    after.iter().fold(0u64, |total, (name, bytes)| {
        total.saturating_add(bytes.saturating_sub(before.get(name).copied().unwrap_or(0)))
    })
}

fn removed_overflow_extents(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
) -> (usize, u64) {
    before
        .iter()
        .filter(|(name, _)| is_overflow_extent(name) && !after.contains_key(*name))
        .fold((0usize, 0u64), |(count, bytes), (_, file_bytes)| {
            (count.saturating_add(1), bytes.saturating_add(*file_bytes))
        })
}

fn is_overflow_extent(name: &str) -> bool {
    name.starts_with("relational-overflow-") && name.ends_with(".extents.hawdb")
}

fn ratio_per_million(numerator: u64, denominator: u64) -> u64 {
    u64::try_from(u128::from(numerator).saturating_mul(1_000_000) / u128::from(denominator.max(1)))
        .unwrap_or(u64::MAX)
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
