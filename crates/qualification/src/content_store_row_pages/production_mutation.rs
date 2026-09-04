use super::resource::process_evidence;
use super::{
    ContentStoreOpenTimingEvidence, ContentStoreProcessResourceEvidence,
    ContentStoreResourceProfileKind, CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
};
use crate::evidence_digest::{hash_bytes, hash_value, rows_sha256};
use crate::production_graph::validate_production_identity_for_current_target;
use crate::{
    latency_percentiles, nowledge_content_store_schema_identity, nowledge_content_store_sql_corpus,
    ContentStoreSchemaIdentity, ContentStoreSqlCorpus, ContentStoreSqlCorpusIdentity,
    ContentStoreSqlStatementClassification, ContentStoreSqlStatementKind, LatencyPercentiles,
    CONTENT_STORE_SHARED_HOST_8_GIB_BYTES, CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use skein::{
    ConcurrentDatabase, ConcurrentTransactionOptions, Database, DatabaseConfig, DurabilityPolicy,
    ProcessMemoryProfile, ProcessMemorySnapshot, ProductionEvidenceBinding,
    ProductionQualificationIdentity, QueryStreamOptions, RelationalIndexMode, SkeinError,
    StoragePressureSnapshot, StorageRecoveryReport, StorageResidencyMode, StorageResidencyReport,
    Value, WalGroupCommitActivation, WalGroupCommitConfig, WalGroupCommitSnapshot,
};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

pub const PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL: &str =
    "skein-production-content-store-mutation-qualification-v1";
pub const PRODUCTION_CONTENT_STORE_WRITER_MATRIX: [usize; 4] = [1, 4, 8, 10];
pub const MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION: u32 = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionContentStoreMutationKind {
    Insert,
    Update,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreMutationOperation {
    pub statement_name: String,
    pub parameters: Vec<Value>,
    pub kind: ProductionContentStoreMutationKind,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreMutationWorker {
    /// Caller-owned disjoint logical key domain. The value is never retained.
    pub conflict_domain: String,
    pub operations: Vec<ProductionContentStoreMutationOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreMutationVerificationCase {
    pub case_name: String,
    pub statement_name: String,
    pub parameters: Vec<Value>,
    pub expected_output_rows: usize,
    pub expected_output_sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreMutationMatrixCase {
    /// Existing disposable writable replica. It must not be the source replica.
    pub replica_path: PathBuf,
    pub writer_count: usize,
    pub workers: Vec<ProductionContentStoreMutationWorker>,
    pub verification_cases: Vec<ProductionContentStoreMutationVerificationCase>,
    /// Same-shape reference from the accepted previous revision.
    pub reference_commit_p95_micros: u64,
    pub max_commit_p95_micros: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionContentStoreMutationQualificationConfig {
    /// Representative source database. It is opened read-only only to bind
    /// identity and is never used for mutation measurements.
    pub source_database_path: PathBuf,
    pub database_config: DatabaseConfig,
    pub wal_group_commit: WalGroupCommitConfig,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub resource_limits: ProductionContentStoreMutationResourceLimits,
    pub max_commit_p95_regression_per_million: u32,
    pub latency_reference: ProductionContentStoreMutationLatencyReference,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub cases: Vec<ProductionContentStoreMutationMatrixCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationLatencyReference {
    pub source_revision: String,
    pub configuration_digest: String,
    pub dataset_fingerprint: String,
    pub generated_at_unix_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationResourceLimits {
    pub max_steady_resident_bytes: u64,
    pub max_peak_resident_bytes: u64,
    pub max_total_page_faults_per_case: Option<u64>,
    pub max_minor_page_faults_per_case: Option<u64>,
    pub max_major_page_faults_per_case: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationVerificationContractEvidence {
    pub case_name: String,
    pub statement_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub expected_output_rows: usize,
    pub expected_output_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationRunEvidence {
    pub writer_index: usize,
    pub operation_index: usize,
    pub kind: ProductionContentStoreMutationKind,
    pub statement_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub statement_latency_micros: u64,
    pub commit_latency_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationVerificationEvidence {
    pub case_name: String,
    pub statement_name: String,
    pub statement_sha256: String,
    pub parameter_sha256: String,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub output_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationStorageEvidence {
    pub pressure_state: String,
    pub commit_epoch: u64,
    pub checkpoint_commit_epoch: u64,
    pub wal_bytes: u64,
    pub delta_bytes: u64,
    pub cache_capacity_bytes: u64,
    pub cache_resident_bytes: u64,
    pub cache_pinned_bytes: u64,
    pub row_canonical_artifact_bytes: u64,
    pub row_recovery_delta_entries: u64,
    pub row_live_entries: usize,
    pub row_live_encoded_bytes: usize,
    pub row_live_resident_bytes: usize,
    pub index_canonical_artifact_bytes: u64,
    pub index_recovery_delta_entries: usize,
    pub index_live_entries: usize,
    pub index_live_encoded_bytes: usize,
    pub row_visible_commit_epoch: Option<u64>,
    pub index_visible_commit_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationRecoveryEvidence {
    pub total_open_latency_micros: u64,
    pub open_timings: ContentStoreOpenTimingEvidence,
    pub checkpoint_commit_epoch: Option<u64>,
    pub recovered_commit_epoch: u64,
    pub replayed_wal_entries: usize,
    pub replayed_wal_bytes: u64,
    pub torn_tail_ignored: bool,
    pub torn_tail_repaired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreWalGroupEvidence {
    pub activation: String,
    pub delay_policy: String,
    pub submitted_commits: u64,
    pub completed_commits: u64,
    pub group_count: u64,
    pub coalescing_wait_count: u64,
    pub shared_sync_count: u64,
    pub grouped_wal_entries: u64,
    pub grouped_wal_bytes: u64,
    pub max_observed_group_entries: usize,
    pub max_observed_group_bytes: u64,
    pub total_fsync_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionContentStoreMutationCaseReport {
    pub writer_count: usize,
    pub operation_count: usize,
    pub initial_commit_epoch: u64,
    pub committed_epoch: u64,
    pub insert_statement_latency: LatencyPercentiles,
    pub update_statement_latency: LatencyPercentiles,
    pub insert_commit_latency: LatencyPercentiles,
    pub update_commit_latency: LatencyPercentiles,
    pub overall_commit_latency: LatencyPercentiles,
    pub reference_commit_p95_micros: u64,
    pub commit_p95_regression_per_million: u32,
    pub max_commit_p95_micros: u64,
    pub mutation_elapsed_micros: u64,
    pub checkpoint_latency_micros: u64,
    pub process: ContentStoreProcessResourceEvidence,
    pub initial_storage: ProductionContentStoreMutationStorageEvidence,
    pub dirty_storage: ProductionContentStoreMutationStorageEvidence,
    pub recovered_storage: ProductionContentStoreMutationStorageEvidence,
    pub final_storage: ProductionContentStoreMutationStorageEvidence,
    pub wal_group: ProductionContentStoreWalGroupEvidence,
    pub wal_replay_open: ProductionContentStoreMutationRecoveryEvidence,
    pub manifest_only_open: ProductionContentStoreMutationRecoveryEvidence,
    pub runs: Vec<ProductionContentStoreMutationRunEvidence>,
    pub verification_contracts: Vec<ProductionContentStoreMutationVerificationContractEvidence>,
    pub replay_verification: Vec<ProductionContentStoreMutationVerificationEvidence>,
    pub checkpoint_verification: Vec<ProductionContentStoreMutationVerificationEvidence>,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionContentStoreMutationQualificationReport {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: ProductionEvidenceBinding,
    pub corpus: ContentStoreSqlCorpusIdentity,
    pub schema: ContentStoreSchemaIdentity,
    pub resource_profile_kind: ContentStoreResourceProfileKind,
    pub configured_available_memory_bytes: u64,
    pub resource_limits: ProductionContentStoreMutationResourceLimits,
    pub max_commit_p95_regression_per_million: u32,
    pub latency_reference: ProductionContentStoreMutationLatencyReference,
    pub cases: Vec<ProductionContentStoreMutationCaseReport>,
}

impl ProductionContentStoreMutationQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": PRODUCTION_CONTENT_STORE_MUTATION_QUALIFICATION_PROTOCOL,
            "evidence_kind": "representative_production_relational_mutation_replicas",
            "production_eligible": true,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
            "evidence_binding": self.evidence_binding.json(),
            "corpus": self.corpus,
            "schema": self.schema,
            "resource_profile_kind": self.resource_profile_kind,
            "configured_available_memory_bytes": self.configured_available_memory_bytes,
            "resource_limits": self.resource_limits,
            "max_commit_p95_regression_per_million": self.max_commit_p95_regression_per_million,
            "latency_reference": self.latency_reference,
            "cases": self.cases,
        })
    }
}

pub fn run_production_content_store_mutation_qualification(
    config: ProductionContentStoreMutationQualificationConfig,
) -> Result<ProductionContentStoreMutationQualificationReport, SkeinError> {
    let corpus = nowledge_content_store_sql_corpus()?;
    validate_config(&config, &corpus)?;
    validate_source_database(&config)?;
    let mut reports = Vec::with_capacity(config.cases.len());
    for case in &config.cases {
        reports.push(run_case(&config, case, &corpus)?);
    }
    let mut blocker_codes = reports
        .iter()
        .flat_map(|report| report.blocker_codes.iter().cloned())
        .collect::<Vec<_>>();
    blocker_codes.sort();
    blocker_codes.dedup();
    Ok(ProductionContentStoreMutationQualificationReport {
        ready: blocker_codes.is_empty(),
        blocker_codes,
        evidence_binding: config.evidence_binding,
        corpus: corpus.identity(),
        schema: nowledge_content_store_schema_identity(),
        resource_profile_kind: config.resource_profile_kind,
        configured_available_memory_bytes: config.configured_available_memory_bytes,
        resource_limits: config.resource_limits,
        max_commit_p95_regression_per_million: config.max_commit_p95_regression_per_million,
        latency_reference: config.latency_reference,
        cases: reports,
    })
}

fn run_case(
    config: &ProductionContentStoreMutationQualificationConfig,
    case: &ProductionContentStoreMutationMatrixCase,
    corpus: &ContentStoreSqlCorpus,
) -> Result<ProductionContentStoreMutationCaseReport, SkeinError> {
    let process_start = ProcessMemorySnapshot::capture()?;
    let database = Database::open_with_durability_and_config(
        &case.replica_path,
        DurabilityPolicy::SyncOnEveryWrite,
        config.database_config.clone(),
    )?;
    let concurrent =
        ConcurrentDatabase::new_with_wal_group_commit(database, config.wal_group_commit);
    let initial_commit_epoch = concurrent.commit_epoch()?;
    let initial_pressure = concurrent.storage_pressure_snapshot()?;
    let initial_storage =
        storage_evidence(&initial_pressure, &concurrent.storage_residency_report()?);
    validate_initial_replica(
        initial_commit_epoch,
        &initial_storage,
        &initial_pressure,
        &config.expected_identity,
    )?;
    let wal_before = concurrent.wal_group_commit_snapshot()?;
    let barrier = Arc::new(Barrier::new(case.writer_count));
    let mutation_started = Instant::now();
    let mut runs = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(case.writer_count);
        for (writer_index, worker) in case.workers.iter().enumerate() {
            let database = concurrent.clone();
            let barrier = Arc::clone(&barrier);
            handles
                .push(scope.spawn(move || {
                    execute_worker(database, barrier, writer_index, worker, corpus)
                }));
        }
        let mut runs = Vec::new();
        for handle in handles {
            let writer_runs = handle.join().map_err(|_| {
                SkeinError::Execution(
                    "production Content Store mutation worker panicked".to_string(),
                )
            })??;
            runs.extend(writer_runs);
        }
        Ok::<_, SkeinError>(runs)
    })?;
    let mutation_elapsed_micros = elapsed_micros(mutation_started);
    runs.sort_by_key(|run| (run.writer_index, run.operation_index));
    let committed_epoch = concurrent.commit_epoch()?;
    let dirty_storage = storage_evidence(
        &concurrent.storage_pressure_snapshot()?,
        &concurrent.storage_residency_report()?,
    );
    let wal_after = concurrent.wal_group_commit_snapshot()?;
    let wal_group = wal_group_delta(wal_before, wal_after)?;
    drop(concurrent);

    let wal_open_started = Instant::now();
    let recovered = ConcurrentDatabase::open_with_durability_and_config(
        &case.replica_path,
        DurabilityPolicy::SyncOnEveryWrite,
        config.database_config.clone(),
    )?;
    let wal_replay_open = recovery_evidence(
        elapsed_micros(wal_open_started),
        recovered.storage_recovery_report()?,
    );
    let replay_verification = verify_cases(&recovered, case, corpus)?;
    let recovered_storage = storage_evidence(
        &recovered.storage_pressure_snapshot()?,
        &recovered.storage_residency_report()?,
    );
    let checkpoint_started = Instant::now();
    recovered.checkpoint()?;
    let checkpoint_latency_micros = elapsed_micros(checkpoint_started);
    drop(recovered);

    let manifest_open_started = Instant::now();
    let final_database = ConcurrentDatabase::open_with_durability_and_config(
        &case.replica_path,
        DurabilityPolicy::SyncOnEveryWrite,
        config.database_config.clone(),
    )?;
    let manifest_only_open = recovery_evidence(
        elapsed_micros(manifest_open_started),
        final_database.storage_recovery_report()?,
    );
    let checkpoint_verification = verify_cases(&final_database, case, corpus)?;
    let final_storage = storage_evidence(
        &final_database.storage_pressure_snapshot()?,
        &final_database.storage_residency_report()?,
    );
    drop(final_database);
    let process = process_evidence(ProcessMemoryProfile::between(
        process_start,
        ProcessMemorySnapshot::capture()?,
    ));

    let insert_statement = latency_for(&runs, ProductionContentStoreMutationKind::Insert, |run| {
        run.statement_latency_micros
    });
    let update_statement = latency_for(&runs, ProductionContentStoreMutationKind::Update, |run| {
        run.statement_latency_micros
    });
    let insert_commit = latency_for(&runs, ProductionContentStoreMutationKind::Insert, |run| {
        run.commit_latency_micros
    });
    let update_commit = latency_for(&runs, ProductionContentStoreMutationKind::Update, |run| {
        run.commit_latency_micros
    });
    let overall_commit = latency_percentiles(
        &runs
            .iter()
            .map(|run| run.commit_latency_micros)
            .collect::<Vec<_>>(),
    );
    let commit_p95_regression_per_million =
        regression_per_million(overall_commit.p95_micros, case.reference_commit_p95_micros);
    let verification_contracts = case
        .verification_cases
        .iter()
        .map(|verification| {
            let statement = corpus
                .statement(&verification.statement_name)
                .expect("validated production verification statement");
            ProductionContentStoreMutationVerificationContractEvidence {
                case_name: verification.case_name.clone(),
                statement_name: verification.statement_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: parameter_digest(&verification.parameters),
                expected_output_rows: verification.expected_output_rows,
                expected_output_sha256: verification.expected_output_sha256.clone(),
            }
        })
        .collect();
    let mut blocker_codes = Vec::new();
    collect_case_blockers(
        CaseBlockerInputs {
            config,
            case,
            initial_commit_epoch,
            committed_epoch,
            initial: &initial_storage,
            dirty: &dirty_storage,
            recovered: &recovered_storage,
            final_storage: &final_storage,
            wal: &wal_group,
            wal_replay: &wal_replay_open,
            manifest_only: &manifest_only_open,
            replay_verification: &replay_verification,
            checkpoint_verification: &checkpoint_verification,
            process: &process,
            commit_latency: overall_commit,
            regression_per_million: commit_p95_regression_per_million,
        },
        &mut blocker_codes,
    );
    blocker_codes.sort();
    blocker_codes.dedup();

    Ok(ProductionContentStoreMutationCaseReport {
        writer_count: case.writer_count,
        operation_count: runs.len(),
        initial_commit_epoch,
        committed_epoch,
        insert_statement_latency: insert_statement,
        update_statement_latency: update_statement,
        insert_commit_latency: insert_commit,
        update_commit_latency: update_commit,
        overall_commit_latency: overall_commit,
        reference_commit_p95_micros: case.reference_commit_p95_micros,
        commit_p95_regression_per_million,
        max_commit_p95_micros: case.max_commit_p95_micros,
        mutation_elapsed_micros,
        checkpoint_latency_micros,
        process,
        initial_storage,
        dirty_storage,
        recovered_storage,
        final_storage,
        wal_group,
        wal_replay_open,
        manifest_only_open,
        runs,
        verification_contracts,
        replay_verification,
        checkpoint_verification,
        blocker_codes,
    })
}

fn execute_worker(
    database: ConcurrentDatabase,
    barrier: Arc<Barrier>,
    writer_index: usize,
    worker: &ProductionContentStoreMutationWorker,
    corpus: &ContentStoreSqlCorpus,
) -> Result<Vec<ProductionContentStoreMutationRunEvidence>, SkeinError> {
    let mut runs = Vec::with_capacity(worker.operations.len());
    barrier.wait();
    for (operation_index, operation) in worker.operations.iter().enumerate() {
        let statement = corpus.statement(&operation.statement_name).ok_or_else(|| {
            SkeinError::Semantic(format!(
                "production Content Store mutation references unknown statement {}",
                operation.statement_name
            ))
        })?;
        let mut transaction = database.begin_transaction(ConcurrentTransactionOptions::default())?;
        let statement_started = Instant::now();
        transaction.query_sql_with_params(&statement.sql, &operation.parameters)?;
        let statement_latency_micros = elapsed_micros(statement_started);
        let commit_started = Instant::now();
        transaction.commit()?;
        runs.push(ProductionContentStoreMutationRunEvidence {
            writer_index,
            operation_index,
            kind: operation.kind,
            statement_name: operation.statement_name.clone(),
            statement_sha256: statement_digest(&statement.sql),
            parameter_sha256: parameter_digest(&operation.parameters),
            statement_latency_micros,
            commit_latency_micros: elapsed_micros(commit_started),
        });
    }
    Ok(runs)
}

fn verify_cases(
    database: &ConcurrentDatabase,
    case: &ProductionContentStoreMutationMatrixCase,
    corpus: &ContentStoreSqlCorpus,
) -> Result<Vec<ProductionContentStoreMutationVerificationEvidence>, SkeinError> {
    let transaction = database.begin_read_transaction()?;
    case.verification_cases
        .iter()
        .map(|verification| {
            let statement = corpus
                .statement(&verification.statement_name)
                .ok_or_else(|| {
                    SkeinError::Semantic(format!(
                        "production Content Store verification references unknown statement {}",
                        verification.statement_name
                    ))
                })?;
            let output = transaction.query_sql_with_params_options(
                &statement.sql,
                &verification.parameters,
                QueryStreamOptions {
                    max_rows: Some(statement.max_rows),
                    max_payload_bytes: Some(statement.max_payload_bytes),
                },
            )?;
            Ok(ProductionContentStoreMutationVerificationEvidence {
                case_name: verification.case_name.clone(),
                statement_name: verification.statement_name.clone(),
                statement_sha256: statement_digest(&statement.sql),
                parameter_sha256: parameter_digest(&verification.parameters),
                output_rows: output.rows.len(),
                output_payload_bytes: output.payload_bytes(),
                output_sha256: rows_sha256(&output.rows),
            })
        })
        .collect()
}

fn validate_config(
    config: &ProductionContentStoreMutationQualificationConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(), SkeinError> {
    validate_production_identity_for_current_target(
        &config.evidence_binding,
        &config.expected_identity,
    )
    .map_err(|error| SkeinError::Semantic(error.to_string()))?;
    if config.database_config.read_only
        || config.database_config.storage_residency_mode != StorageResidencyMode::OutOfCore
        || config.database_config.relational_index_mode != RelationalIndexMode::Authoritative
        || config.database_config.segment_cache_capacity_bytes == 0
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation replicas require writable out-of-core authoritative storage with a bounded cache"
                .to_string(),
        ));
    }
    if config.wal_group_commit.activation() != WalGroupCommitActivation::EvidenceValidated {
        return Err(SkeinError::Semantic(
            "production Content Store mutation qualification requires evidence-validated WAL group commit"
                .to_string(),
        ));
    }
    if config.max_commit_p95_regression_per_million
        > MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION
    {
        return Err(SkeinError::Semantic(format!(
            "production Content Store commit p95 regression budget must not exceed {MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION} per million"
        )));
    }
    if config.latency_reference.source_revision.trim().is_empty()
        || config.latency_reference.configuration_digest
            != config.expected_identity.configuration_digest
        || config.latency_reference.dataset_fingerprint
            != config.expected_identity.dataset_fingerprint
        || config.latency_reference.generated_at_unix_seconds == 0
    {
        return Err(SkeinError::Semantic(
            "production Content Store commit latency reference must bind a revision and the current configuration and dataset shape"
                .to_string(),
        ));
    }
    validate_resource_profile(config)?;
    if config.cases.len() != PRODUCTION_CONTENT_STORE_WRITER_MATRIX.len() {
        return Err(SkeinError::Semantic(
            "production Content Store mutation qualification requires exactly four writer cases"
                .to_string(),
        ));
    }
    let mut writer_counts = BTreeSet::new();
    let mut replica_paths = BTreeSet::new();
    let source_path = std::fs::canonicalize(&config.source_database_path)?;
    if !source_path.is_dir() {
        return Err(SkeinError::Semantic(
            "production Content Store mutation source must be an existing database directory"
                .to_string(),
        ));
    }
    for case in &config.cases {
        validate_case(case, corpus)?;
        if !writer_counts.insert(case.writer_count) {
            return Err(SkeinError::Semantic(
                "production Content Store writer counts must be unique".to_string(),
            ));
        }
        let canonical = std::fs::canonicalize(&case.replica_path)?;
        if !canonical.is_dir() || canonical == source_path || !replica_paths.insert(canonical) {
            return Err(SkeinError::Semantic(
                "production Content Store mutation replicas must be distinct existing directories separate from the read-only source"
                    .to_string(),
            ));
        }
    }
    if writer_counts
        != PRODUCTION_CONTENT_STORE_WRITER_MATRIX
            .into_iter()
            .collect::<BTreeSet<_>>()
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation writer matrix must be exactly 1, 4, 8, and 10"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_source_database(
    config: &ProductionContentStoreMutationQualificationConfig,
) -> Result<(), SkeinError> {
    let mut source_config = config.database_config.clone();
    source_config.read_only = true;
    let source = Database::open_with_durability_and_config(
        &config.source_database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        source_config,
    )?;
    let commit_epoch = source.commit_epoch();
    let pressure = source.storage_pressure_snapshot();
    let residency = source.storage_residency_report();
    let evidence = storage_evidence(&pressure, &residency);
    if commit_epoch != config.expected_identity.canonical_graph_commit_epoch
        || !storage_view_current(&evidence)
        || evidence.row_canonical_artifact_bytes <= evidence.cache_capacity_bytes
        || evidence.index_canonical_artifact_bytes <= evidence.cache_capacity_bytes
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation source identity or larger-than-cache storage view is invalid"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_initial_replica(
    commit_epoch: u64,
    storage: &ProductionContentStoreMutationStorageEvidence,
    pressure: &StoragePressureSnapshot,
    expected_identity: &ProductionQualificationIdentity,
) -> Result<(), SkeinError> {
    if commit_epoch != expected_identity.canonical_graph_commit_epoch
        || !storage_view_current(storage)
        || storage.row_canonical_artifact_bytes <= storage.cache_capacity_bytes
        || storage.index_canonical_artifact_bytes <= storage.cache_capacity_bytes
        || storage.cache_pinned_bytes != 0
        || !pressure.admits_mutation()
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation replica failed identity, larger-than-cache, pin, or storage-pressure preflight"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_resource_profile(
    config: &ProductionContentStoreMutationQualificationConfig,
) -> Result<(), SkeinError> {
    if config.configured_available_memory_bytes == 0
        || config.resource_limits.max_steady_resident_bytes == 0
        || config.resource_limits.max_peak_resident_bytes == 0
        || config.resource_limits.max_steady_resident_bytes
            > config.resource_limits.max_peak_resident_bytes
        || config.resource_limits.max_peak_resident_bytes > config.configured_available_memory_bytes
        || config.database_config.segment_cache_capacity_bytes
            > config.configured_available_memory_bytes
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation memory limits must be non-zero, ordered, and within the declared profile"
                .to_string(),
        ));
    }
    match config.resource_profile_kind {
        ContentStoreResourceProfileKind::Capability512Mib
            if config.configured_available_memory_bytes
                != CONTENT_STORE_512_MIB_CAPABILITY_BYTES =>
        {
            Err(SkeinError::Semantic(format!(
                "production Content Store mutation 512 MiB capability must declare {CONTENT_STORE_512_MIB_CAPABILITY_BYTES} bytes"
            )))
        }
        ContentStoreResourceProfileKind::SharedHost8Gib
            if config.configured_available_memory_bytes != CONTENT_STORE_SHARED_HOST_8_GIB_BYTES =>
        {
            Err(SkeinError::Semantic(format!(
                "production Content Store mutation shared-host profile must declare {CONTENT_STORE_SHARED_HOST_8_GIB_BYTES} bytes"
            )))
        }
        ContentStoreResourceProfileKind::SharedHost8Gib
            if config.resource_limits.max_peak_resident_bytes
                > CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES =>
        {
            Err(SkeinError::Semantic(format!(
                "production Content Store mutation shared-host peak RSS budget must not exceed {CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES} bytes"
            )))
        }
        _ => Ok(()),
    }
}

fn validate_case(
    case: &ProductionContentStoreMutationMatrixCase,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(), SkeinError> {
    if !case.replica_path.is_dir()
        || case.writer_count == 0
        || case.workers.len() != case.writer_count
        || case.verification_cases.is_empty()
        || case.reference_commit_p95_micros == 0
        || case.max_commit_p95_micros == 0
    {
        return Err(SkeinError::Semantic(
            "production Content Store mutation case has an invalid replica, writer, verification, or latency contract"
                .to_string(),
        ));
    }
    let mut conflict_domains = BTreeSet::new();
    let mut operation_count = None;
    for worker in &case.workers {
        if worker.conflict_domain.trim().is_empty()
            || !conflict_domains.insert(worker.conflict_domain.as_str())
            || worker.operations.is_empty()
        {
            return Err(SkeinError::Semantic(
                "production Content Store mutation workers require unique non-empty conflict domains and operations"
                    .to_string(),
            ));
        }
        if operation_count
            .replace(worker.operations.len())
            .is_some_and(|expected| expected != worker.operations.len())
        {
            return Err(SkeinError::Semantic(
                "production Content Store mutation workers must execute the same operation count"
                    .to_string(),
            ));
        }
        let kinds = worker
            .operations
            .iter()
            .map(|operation| operation.kind)
            .collect::<BTreeSet<_>>();
        if kinds
            != [
                ProductionContentStoreMutationKind::Insert,
                ProductionContentStoreMutationKind::Update,
            ]
            .into_iter()
            .collect()
        {
            return Err(SkeinError::Semantic(
                "every production Content Store mutation worker must execute insert and update operations"
                    .to_string(),
            ));
        }
        for operation in &worker.operations {
            let statement = corpus.statement(&operation.statement_name).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "production Content Store mutation references unknown statement {}",
                    operation.statement_name
                ))
            })?;
            validate_mutation_operation(operation, statement)?;
        }
    }
    let mut verification_names = BTreeSet::new();
    for verification in &case.verification_cases {
        let statement = corpus
            .statement(&verification.statement_name)
            .ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "production Content Store verification references unknown statement {}",
                    verification.statement_name
                ))
            })?;
        if verification.case_name.trim().is_empty()
            || !verification_names.insert(verification.case_name.as_str())
            || statement.kind != ContentStoreSqlStatementKind::Read
            || statement.classification == ContentStoreSqlStatementClassification::RetainedOnSqlite
            || statement.parameters.len() != verification.parameters.len()
            || verification.expected_output_rows > statement.max_rows
            || !valid_sha256(&verification.expected_output_sha256)
        {
            return Err(SkeinError::Semantic(
                "production Content Store mutation verification contract is invalid".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_mutation_operation(
    operation: &ProductionContentStoreMutationOperation,
    statement: &crate::ContentStoreSqlStatementSpec,
) -> Result<(), SkeinError> {
    if statement.kind != ContentStoreSqlStatementKind::Mutation
        || statement.classification == ContentStoreSqlStatementClassification::RetainedOnSqlite
        || statement.parameters.len() != operation.parameters.len()
    {
        return Err(SkeinError::Semantic(format!(
            "production Content Store operation {} is not a parameter-complete Skein-owned mutation",
            operation.statement_name
        )));
    }
    let lowered = skein::sql::parse_postgres_sql(&statement.sql)?;
    let kind_matches = matches!(
        (operation.kind, lowered),
        (
            ProductionContentStoreMutationKind::Insert,
            skein::sql::SqlStatement::Insert(_)
        ) | (
            ProductionContentStoreMutationKind::Update,
            skein::sql::SqlStatement::Update(_)
        )
    );
    if !kind_matches {
        return Err(SkeinError::Semantic(format!(
            "production Content Store operation {} kind does not match its lowered SQL statement",
            operation.statement_name
        )));
    }
    Ok(())
}

struct CaseBlockerInputs<'a> {
    config: &'a ProductionContentStoreMutationQualificationConfig,
    case: &'a ProductionContentStoreMutationMatrixCase,
    initial_commit_epoch: u64,
    committed_epoch: u64,
    initial: &'a ProductionContentStoreMutationStorageEvidence,
    dirty: &'a ProductionContentStoreMutationStorageEvidence,
    recovered: &'a ProductionContentStoreMutationStorageEvidence,
    final_storage: &'a ProductionContentStoreMutationStorageEvidence,
    wal: &'a ProductionContentStoreWalGroupEvidence,
    wal_replay: &'a ProductionContentStoreMutationRecoveryEvidence,
    manifest_only: &'a ProductionContentStoreMutationRecoveryEvidence,
    replay_verification: &'a [ProductionContentStoreMutationVerificationEvidence],
    checkpoint_verification: &'a [ProductionContentStoreMutationVerificationEvidence],
    process: &'a ContentStoreProcessResourceEvidence,
    commit_latency: LatencyPercentiles,
    regression_per_million: u32,
}

fn collect_case_blockers(inputs: CaseBlockerInputs<'_>, blockers: &mut Vec<String>) {
    let CaseBlockerInputs {
        config,
        case,
        initial_commit_epoch,
        committed_epoch,
        initial,
        dirty,
        recovered,
        final_storage,
        wal,
        wal_replay,
        manifest_only,
        replay_verification,
        checkpoint_verification,
        process,
        commit_latency,
        regression_per_million,
    } = inputs;
    let operation_count = case
        .workers
        .iter()
        .map(|worker| worker.operations.len() as u64)
        .sum::<u64>();
    if initial_commit_epoch != config.expected_identity.canonical_graph_commit_epoch {
        blockers.push("content_store_mutation_initial_epoch_identity_mismatch".to_string());
    }
    if committed_epoch != initial_commit_epoch.saturating_add(operation_count) {
        blockers.push("content_store_mutation_commit_epoch_mismatch".to_string());
    }
    if !storage_view_current(initial)
        || !storage_view_current(dirty)
        || !storage_view_current(recovered)
        || !storage_view_current(final_storage)
    {
        blockers.push("content_store_mutation_row_index_view_not_current".to_string());
    }
    if initial.row_canonical_artifact_bytes <= initial.cache_capacity_bytes
        || initial.index_canonical_artifact_bytes <= initial.cache_capacity_bytes
    {
        blockers.push("content_store_mutation_artifact_does_not_exceed_cache".to_string());
    }
    if initial.cache_pinned_bytes != 0
        || dirty.cache_pinned_bytes != 0
        || recovered.cache_pinned_bytes != 0
        || final_storage.cache_pinned_bytes != 0
    {
        blockers.push("content_store_mutation_cache_pin_leak".to_string());
    }
    if dirty.wal_bytes <= initial.wal_bytes
        || dirty.row_live_entries == 0
        || dirty.index_live_entries == 0
    {
        blockers.push("content_store_mutation_live_wal_evidence_missing".to_string());
    }
    if wal.submitted_commits != operation_count || wal.completed_commits != operation_count {
        blockers.push("content_store_mutation_group_commit_accounting_mismatch".to_string());
    }
    if wal_replay.replayed_wal_entries == 0
        || wal_replay.replayed_wal_bytes == 0
        || wal_replay.recovered_commit_epoch != committed_epoch
        || wal_replay.torn_tail_ignored
        || wal_replay.torn_tail_repaired
    {
        blockers.push("content_store_mutation_wal_replay_evidence_invalid".to_string());
    }
    if !wal_replay.open_timings.consistent
        || wal_replay.open_timings.total_open_micros > wal_replay.total_open_latency_micros
    {
        blockers.push("content_store_mutation_wal_open_timing_invalid".to_string());
    }
    if recovered.row_recovery_delta_entries == 0 || recovered.index_recovery_delta_entries == 0 {
        blockers.push("content_store_mutation_recovery_delta_evidence_missing".to_string());
    }
    if manifest_only.replayed_wal_entries != 0
        || manifest_only.replayed_wal_bytes != 0
        || manifest_only.recovered_commit_epoch != committed_epoch
        || manifest_only.checkpoint_commit_epoch != Some(committed_epoch)
    {
        blockers.push("content_store_mutation_manifest_open_evidence_invalid".to_string());
    }
    if !manifest_only.open_timings.consistent
        || manifest_only.open_timings.total_open_micros > manifest_only.total_open_latency_micros
    {
        blockers.push("content_store_mutation_manifest_open_timing_invalid".to_string());
    }
    if final_storage.row_recovery_delta_entries != 0
        || final_storage.index_recovery_delta_entries != 0
        || final_storage.row_live_entries != 0
        || final_storage.index_live_entries != 0
    {
        blockers.push("content_store_mutation_checkpoint_did_not_fold_deltas".to_string());
    }
    collect_verification_blockers(case, replay_verification, blockers);
    collect_verification_blockers(case, checkpoint_verification, blockers);
    if replay_verification != checkpoint_verification {
        blockers.push("content_store_mutation_replay_checkpoint_result_mismatch".to_string());
    }
    if commit_latency.p95_micros > case.max_commit_p95_micros {
        blockers.push("content_store_mutation_commit_p95_budget_exceeded".to_string());
    }
    if regression_per_million > config.max_commit_p95_regression_per_million {
        blockers.push("content_store_mutation_commit_p95_regression_exceeded".to_string());
    }
    collect_process_blockers(process, config.resource_limits, blockers);
}

fn collect_verification_blockers(
    case: &ProductionContentStoreMutationMatrixCase,
    observed: &[ProductionContentStoreMutationVerificationEvidence],
    blockers: &mut Vec<String>,
) {
    if observed.len() != case.verification_cases.len() {
        blockers.push("content_store_mutation_verification_count_mismatch".to_string());
        return;
    }
    for (expected, actual) in case.verification_cases.iter().zip(observed) {
        if actual.case_name != expected.case_name
            || actual.statement_name != expected.statement_name
            || actual.output_rows != expected.expected_output_rows
            || actual.output_sha256 != expected.expected_output_sha256
        {
            blockers.push("content_store_mutation_verification_mismatch".to_string());
        }
    }
}

fn collect_process_blockers(
    process: &ContentStoreProcessResourceEvidence,
    limits: ProductionContentStoreMutationResourceLimits,
    blockers: &mut Vec<String>,
) {
    if !process.resident_memory_supported {
        blockers.push("content_store_mutation_resident_memory_unavailable".to_string());
    } else if process.steady_resident_bytes > limits.max_steady_resident_bytes {
        blockers.push("content_store_mutation_steady_resident_budget_exceeded".to_string());
    } else if process.peak_resident_bytes > limits.max_peak_resident_bytes {
        blockers.push("content_store_mutation_peak_resident_budget_exceeded".to_string());
    }
    for (kind, supported, observed, maximum) in [
        (
            "total",
            process.total_page_faults_supported,
            process.total_page_faults,
            limits.max_total_page_faults_per_case,
        ),
        (
            "minor",
            process.split_page_faults_supported,
            process.minor_page_faults,
            limits.max_minor_page_faults_per_case,
        ),
        (
            "major",
            process.split_page_faults_supported,
            process.major_page_faults,
            limits.max_major_page_faults_per_case,
        ),
    ] {
        if let Some(maximum) = maximum {
            if !supported || observed.is_none() {
                blockers.push(format!(
                    "content_store_mutation_{kind}_page_faults_unavailable"
                ));
            } else if observed.is_some_and(|observed| observed > maximum) {
                blockers.push(format!(
                    "content_store_mutation_{kind}_page_fault_budget_exceeded"
                ));
            }
        }
    }
}

fn storage_view_current(storage: &ProductionContentStoreMutationStorageEvidence) -> bool {
    storage.row_visible_commit_epoch == Some(storage.commit_epoch)
        && storage.index_visible_commit_epoch == Some(storage.commit_epoch)
        && storage.cache_resident_bytes <= storage.cache_capacity_bytes
}

fn storage_evidence(
    pressure: &StoragePressureSnapshot,
    residency: &StorageResidencyReport,
) -> ProductionContentStoreMutationStorageEvidence {
    ProductionContentStoreMutationStorageEvidence {
        pressure_state: pressure.state.as_str().to_string(),
        commit_epoch: pressure.current_commit_epoch,
        checkpoint_commit_epoch: pressure.checkpoint_commit_epoch,
        wal_bytes: pressure.wal_bytes,
        delta_bytes: pressure.delta_bytes,
        cache_capacity_bytes: pressure.cache_capacity_bytes,
        cache_resident_bytes: pressure.cache_resident_bytes,
        cache_pinned_bytes: pressure.cache_pinned_bytes,
        row_canonical_artifact_bytes: residency.relational_rows.canonical_artifact_bytes(),
        row_recovery_delta_entries: residency.relational_rows.recovery_delta_entries,
        row_live_entries: residency.relational_rows.live_entries,
        row_live_encoded_bytes: residency.relational_rows.live_encoded_bytes,
        row_live_resident_bytes: residency.relational_rows.live_resident_bytes,
        index_canonical_artifact_bytes: residency.relational_indexes.canonical_artifact_bytes(),
        index_recovery_delta_entries: residency.relational_indexes.recovery_delta_entries,
        index_live_entries: residency.relational_indexes.live_entries,
        index_live_encoded_bytes: residency.relational_indexes.live_encoded_bytes,
        row_visible_commit_epoch: residency.relational_rows.visible_commit_epoch,
        index_visible_commit_epoch: residency.relational_indexes.visible_commit_epoch,
    }
}

fn recovery_evidence(
    total_open_latency_micros: u64,
    report: StorageRecoveryReport,
) -> ProductionContentStoreMutationRecoveryEvidence {
    ProductionContentStoreMutationRecoveryEvidence {
        total_open_latency_micros,
        open_timings: report.open_timings.into(),
        checkpoint_commit_epoch: report.checkpoint_commit_epoch,
        recovered_commit_epoch: report.recovered_commit_epoch,
        replayed_wal_entries: report.replayed_wal_entries,
        replayed_wal_bytes: report.replayed_wal_bytes,
        torn_tail_ignored: report.torn_tail_ignored,
        torn_tail_repaired: report.torn_tail_repaired,
    }
}

fn wal_group_delta(
    before: WalGroupCommitSnapshot,
    after: WalGroupCommitSnapshot,
) -> Result<ProductionContentStoreWalGroupEvidence, SkeinError> {
    Ok(ProductionContentStoreWalGroupEvidence {
        activation: match after.activation {
            WalGroupCommitActivation::Disabled => "disabled",
            WalGroupCommitActivation::BenchmarkCandidate => "benchmark_candidate",
            WalGroupCommitActivation::EvidenceValidated => "evidence_validated",
        }
        .to_string(),
        delay_policy: after.delay_policy.as_str().to_string(),
        submitted_commits: monotonic_delta(
            "submitted commits",
            before.submitted_commits,
            after.submitted_commits,
        )?,
        completed_commits: monotonic_delta(
            "completed commits",
            before.completed_commits,
            after.completed_commits,
        )?,
        group_count: monotonic_delta("group count", before.group_count, after.group_count)?,
        coalescing_wait_count: monotonic_delta(
            "coalescing wait count",
            before.coalescing_wait_count,
            after.coalescing_wait_count,
        )?,
        shared_sync_count: monotonic_delta(
            "shared sync count",
            before.shared_sync_count,
            after.shared_sync_count,
        )?,
        grouped_wal_entries: monotonic_delta(
            "grouped WAL entries",
            before.grouped_wal_entries,
            after.grouped_wal_entries,
        )?,
        grouped_wal_bytes: monotonic_delta(
            "grouped WAL bytes",
            before.grouped_wal_bytes,
            after.grouped_wal_bytes,
        )?,
        max_observed_group_entries: after.max_observed_group_entries,
        max_observed_group_bytes: after.max_observed_group_bytes,
        total_fsync_micros: monotonic_delta(
            "total fsync micros",
            before.total_fsync_micros,
            after.total_fsync_micros,
        )?,
    })
}

fn monotonic_delta(name: &str, before: u64, after: u64) -> Result<u64, SkeinError> {
    after.checked_sub(before).ok_or_else(|| {
        SkeinError::Execution(format!(
            "production Content Store mutation {name} decreased from {before} to {after}"
        ))
    })
}

fn latency_for(
    runs: &[ProductionContentStoreMutationRunEvidence],
    kind: ProductionContentStoreMutationKind,
    field: impl Fn(&ProductionContentStoreMutationRunEvidence) -> u64,
) -> LatencyPercentiles {
    latency_percentiles(
        &runs
            .iter()
            .filter(|run| run.kind == kind)
            .map(field)
            .collect::<Vec<_>>(),
    )
}

fn regression_per_million(observed: u64, reference: u64) -> u32 {
    if observed <= reference || reference == 0 {
        return 0;
    }
    u32::try_from(
        observed
            .saturating_sub(reference)
            .saturating_mul(1_000_000)
            .checked_div(reference)
            .unwrap_or(u64::MAX),
    )
    .unwrap_or(u32::MAX)
}

fn statement_digest(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(
        &mut hasher,
        b"skein-production-content-store-mutation-statement-v1",
    );
    hash_bytes(&mut hasher, sql.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn parameter_digest(parameters: &[Value]) -> String {
    let mut hasher = Sha256::new();
    hash_bytes(
        &mut hasher,
        b"skein-production-content-store-mutation-parameters-v1",
    );
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
    use crate::content_store_row_pages::fixture::{
        bootstrap_checkpoint, database_config, THREAD_DOCUMENT_ID, THREAD_STORAGE_ID,
    };
    use crate::ContentStoreInitialRowPageQualificationConfig;
    use skein::{
        WalGroupCommitAdaptiveColdStartEvidence, WalGroupCommitAdaptivePolicyEvidence,
        WalGroupCommitAdaptiveSteadyStateEvidence, WalGroupCommitEvidence,
        WalGroupCommitTailLatencyEvidence, PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    use std::collections::BTreeMap;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn production_mutation_runner_qualifies_isolated_writer_matrix() {
        let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
        let root = std::env::temp_dir().join(format!(
            "skein-production-content-mutation-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let source_path = root.join("source");
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let seed = seed_config(&source_path);
        let source_checkpoint = bootstrap_checkpoint(&seed, &corpus).unwrap();
        let mut cases = Vec::new();
        for writer_count in PRODUCTION_CONTENT_STORE_WRITER_MATRIX {
            let replica_path = root.join(format!("writers-{writer_count}"));
            let replica_seed = seed_config(&replica_path);
            let checkpoint = bootstrap_checkpoint(&replica_seed, &corpus).unwrap();
            assert_eq!(checkpoint.commit_epoch, source_checkpoint.commit_epoch);
            cases.push(mutation_case(replica_path, writer_count));
        }

        let identity = ProductionQualificationIdentity {
            source_revision: "production-content-mutation-test".to_string(),
            rust_toolchain: "rustc-test".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "production-content-mutation-config".to_string(),
            deployment_profile: "representative-production-replica".to_string(),
            dataset_fingerprint: "production-content-mutation-dataset".to_string(),
            canonical_graph_commit_epoch: source_checkpoint.commit_epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let report = run_production_content_store_mutation_qualification(
            ProductionContentStoreMutationQualificationConfig {
                source_database_path: source_path.clone(),
                database_config: database_config(&seed, RelationalIndexMode::Authoritative),
                wal_group_commit: qualified_group_commit(),
                resource_profile_kind: ContentStoreResourceProfileKind::Capability512Mib,
                configured_available_memory_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                resource_limits: ProductionContentStoreMutationResourceLimits {
                    max_steady_resident_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_peak_resident_bytes: CONTENT_STORE_512_MIB_CAPABILITY_BYTES,
                    max_total_page_faults_per_case: None,
                    max_minor_page_faults_per_case: None,
                    max_major_page_faults_per_case: None,
                },
                max_commit_p95_regression_per_million:
                    MAX_PRODUCTION_COMMIT_P95_REGRESSION_PER_MILLION,
                latency_reference: ProductionContentStoreMutationLatencyReference {
                    source_revision: "accepted-production-content-mutation-test".to_string(),
                    configuration_digest: identity.configuration_digest.clone(),
                    dataset_fingerprint: identity.dataset_fingerprint.clone(),
                    generated_at_unix_seconds: 1,
                },
                evidence_binding: ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                expected_identity: identity.clone(),
                cases,
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
                content_store_mutation_matrix: Some(report.json()),
                ..crate::ProductionReleaseQualificationArtifacts::default()
            },
            identity,
            crate::ProductionReleaseQualificationPolicy::default(),
        );
        assert!(
            release.content_store_mutation_matrix.ready,
            "unexpected release blockers: {:?}",
            release.content_store_mutation_matrix.blocker_codes
        );
        assert_eq!(report.cases.len(), 4);
        for case in &report.cases {
            assert_eq!(case.operation_count, case.writer_count * 2);
            assert_eq!(
                case.wal_group.submitted_commits,
                case.operation_count as u64
            );
            assert!(case.wal_replay_open.replayed_wal_entries > 0);
            assert!(case.wal_replay_open.open_timings.consistent);
            assert!(
                case.wal_replay_open.open_timings.total_open_micros
                    <= case.wal_replay_open.total_open_latency_micros
            );
            assert_eq!(case.manifest_only_open.replayed_wal_entries, 0);
            assert!(case.manifest_only_open.open_timings.consistent);
            assert_eq!(case.replay_verification, case.checkpoint_verification);
            assert_eq!(case.commit_p95_regression_per_million, 0);
        }
        let json = report.json().to_string();
        assert!(!json.contains(root.to_string_lossy().as_ref()));
        assert!(!json.contains("mutation-owner"));

        let mut source_config = database_config(&seed, RelationalIndexMode::Authoritative);
        source_config.read_only = true;
        let source = Database::open_with_config(&source_path, source_config).unwrap();
        assert_eq!(source.commit_epoch(), source_checkpoint.commit_epoch);
        drop(source);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn seed_config(path: &Path) -> ContentStoreInitialRowPageQualificationConfig {
        let mut config = ContentStoreInitialRowPageQualificationConfig::synthetic(
            path,
            "production-content-mutation-test",
        );
        config.base_message_count = 10;
        config.message_payload_bytes = 1024;
        config.base_chunk_count = 10;
        config.chunk_payload_bytes = 1024;
        config.segment_cache_capacity_bytes = 4 * 1024;
        config
    }

    fn mutation_case(
        replica_path: PathBuf,
        writer_count: usize,
    ) -> ProductionContentStoreMutationMatrixCase {
        let workers = (0..writer_count)
            .map(|writer| {
                let document_id = format!("mutation-document-{writer_count}-{writer}");
                let owner_id = format!("mutation-owner-{writer_count}-{writer}");
                let message_id = format!("content-message-{writer:08}");
                ProductionContentStoreMutationWorker {
                    conflict_domain: format!("writer-{writer}"),
                    operations: vec![
                        ProductionContentStoreMutationOperation {
                            statement_name: "upsert_content_document".to_string(),
                            parameters: vec![
                                Value::String(document_id),
                                Value::String("thread".to_string()),
                                Value::String(owner_id),
                                Value::String("default".to_string()),
                                Value::String("application/x-nowledge-thread".to_string()),
                                Value::Int(1),
                                Value::String("2026-08-16T00:00:00Z".to_string()),
                                Value::String("2026-08-16T00:00:00Z".to_string()),
                            ],
                            kind: ProductionContentStoreMutationKind::Insert,
                        },
                        ProductionContentStoreMutationOperation {
                            statement_name: "update_thread_message_order".to_string(),
                            parameters: vec![
                                Value::Int(1_000 + writer as i64),
                                Value::String("2026-08-16T00:00:01Z".to_string()),
                                Value::String(THREAD_STORAGE_ID.to_string()),
                                Value::String(message_id),
                            ],
                            kind: ProductionContentStoreMutationKind::Update,
                        },
                    ],
                }
            })
            .collect::<Vec<_>>();
        let mut verification_cases = Vec::with_capacity(writer_count * 2);
        for writer in 0..writer_count {
            let document_id = format!("mutation-document-{writer_count}-{writer}");
            let owner_id = format!("mutation-owner-{writer_count}-{writer}");
            verification_cases.push(ProductionContentStoreMutationVerificationCase {
                case_name: format!("insert-{writer}"),
                statement_name: "thread_owned_document_ids".to_string(),
                parameters: vec![Value::String(owner_id)],
                expected_output_rows: 1,
                expected_output_sha256: rows_sha256(&[BTreeMap::from([(
                    "content_doc_id".to_string(),
                    Value::String(document_id),
                )])]),
            });
            let content_message_id = format!("content-message-{writer:08}");
            verification_cases.push(ProductionContentStoreMutationVerificationCase {
                case_name: format!("update-{writer}"),
                statement_name: "thread_message_anchor_lookup".to_string(),
                parameters: vec![
                    Value::String(THREAD_STORAGE_ID.to_string()),
                    Value::String(content_message_id.clone()),
                    Value::Int(1),
                ],
                expected_output_rows: 1,
                expected_output_sha256: rows_sha256(&[BTreeMap::from([
                    (
                        "content_doc_id".to_string(),
                        Value::String(THREAD_DOCUMENT_ID.to_string()),
                    ),
                    (
                        "content_hash".to_string(),
                        Value::String(format!("hash-base-{writer:08}")),
                    ),
                    (
                        "content_message_id".to_string(),
                        Value::String(content_message_id),
                    ),
                    (
                        "message_id".to_string(),
                        Value::String(format!("message-{writer:08}")),
                    ),
                    ("order_index".to_string(), Value::Int(1_000 + writer as i64)),
                    (
                        "thread_storage_id".to_string(),
                        Value::String(THREAD_STORAGE_ID.to_string()),
                    ),
                ])]),
            });
        }
        ProductionContentStoreMutationMatrixCase {
            replica_path,
            writer_count,
            workers,
            verification_cases,
            reference_commit_p95_micros: u64::MAX / 2,
            max_commit_p95_micros: u64::MAX,
        }
    }

    fn qualified_group_commit() -> WalGroupCommitConfig {
        let tail = WalGroupCommitTailLatencyEvidence {
            commit_count: 16,
            paired_p95_regression_micros: 1,
            paired_p95_mad_micros: 1,
            max_accepted_p95_regression_micros: 10,
        };
        let evidence = WalGroupCommitEvidence {
            measurement_rounds: 9,
            commit_count: 16,
            baseline_elapsed_micros: 200,
            baseline_fsync_count: 16,
            grouped_elapsed_micros: 100,
            grouped_fsync_count: 2,
            concurrent_tail_latency: tail,
            single_writer_tail_latency: tail,
            single_writer_max_coalescing_wait_count: 0,
            single_writer_max_observed_group_entries: 1,
            adaptive_cold_start_behavior: Some(WalGroupCommitAdaptiveColdStartEvidence {
                commit_count: 16,
                min_fallback_delay_count: 1,
                min_coalescing_wait_count: 1,
                min_observed_group_entries: 2,
            }),
            adaptive_steady_state_behavior: Some(WalGroupCommitAdaptiveSteadyStateEvidence {
                commit_count: 16,
                max_fallback_delay_count: 0,
                min_fsync_baseline_sample_count: 8,
                min_coalescing_wait_count: 1,
                min_observed_group_entries: 2,
                safety_net: WalGroupCommitAdaptivePolicyEvidence {
                    paired_elapsed_regression_micros: -1,
                    paired_elapsed_mad_micros: 1,
                    max_accepted_elapsed_regression_micros: 10,
                    tail_latency: tail,
                },
            }),
            strict_recovery_verified: true,
            wal_order_verified: true,
        };
        WalGroupCommitConfig::enabled_after_evidence(
            evidence,
            NonZeroUsize::new(16).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
            Duration::from_millis(10),
        )
        .unwrap()
    }
}
