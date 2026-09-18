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

use super::{latency_percentiles, LatencyPercentiles};
use crate::production_graph::validate_production_identity_for_current_target;
use hawdb::{
    ProcessMemoryProfile, ProcessMemorySnapshot, ProductionEvidenceBinding,
    ProductionQualificationIdentity, RuntimeCancellationToken, RuntimeTaskContext,
    SearchAccessControlContext, SearchFallbackReasonCode, SearchIndex, SearchOutOfCoreConfig,
    SearchOutOfCoreReader, SearchProjectionQualificationIdentity, SearchResultSet,
    VectorProjectionQualificationIdentity, VectorProjectionResourceEvidence,
    VectorSearchExecutionOptions, VectorSearchKernelPreference,
    MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT, MAX_VECTOR_RECALL_VALIDATION_SAMPLES,
    MAX_VECTOR_RECALL_VALIDATION_TOP_K, MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::Instant;

mod lifecycle;
mod matrix;
mod oracle;
mod query;

pub use matrix::{
    ProductionVectorMatrixExpectation, ProductionVectorQualificationMatrixReport,
    PRODUCTION_VECTOR_QUALIFICATION_MATRIX_PROTOCOL,
};
pub use oracle::{ProductionVectorRaBitQReferenceCase, ProductionVectorRaBitQReferenceEvidence};
use query::{
    collect_recall_evidence, execute_query, execute_serving_query, measure_query,
    measure_serving_query, request_digest, VectorExecutionProfile,
};
pub use query::{
    ProductionVectorExecutionMetrics, ProductionVectorQueryEvidence, ProductionVectorRecallEvidence,
};

pub const PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL: &str =
    "hawdb-production-vector-qualification-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProductionVectorCaseKind {
    Unfiltered,
    MetadataFiltered,
    AclFiltered,
}

impl ProductionVectorCaseKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unfiltered => "unfiltered",
            Self::MetadataFiltered => "metadata_filtered",
            Self::AclFiltered => "acl_filtered",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionVectorQueryCase {
    pub name: String,
    pub kind: ProductionVectorCaseKind,
    pub query_embedding: Vec<f32>,
    pub metadata_filters: BTreeMap<String, String>,
    pub access_control: Option<SearchAccessControlContext>,
}

impl ProductionVectorQueryCase {
    fn effective_metadata_filters(
        &self,
    ) -> Result<BTreeMap<String, String>, ProductionVectorQualificationError> {
        match &self.access_control {
            Some(access_control) => access_control
                .effective_metadata_filters(&self.metadata_filters)
                .map_err(ProductionVectorQualificationError::from_error),
            None => Ok(self.metadata_filters.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionVectorLifecycleConfig {
    /// Every path must be a disposable writable copy of `projection_path`.
    pub replica_paths: Vec<PathBuf>,
    /// This disposable copy is intentionally corrupted and cannot be reused.
    pub corruption_replica_path: PathBuf,
    pub delta: hawdb::SearchProjectionDelta,
    pub verification_case: ProductionVectorQueryCase,
    pub expected_upsert_document_id: String,
    pub expected_deleted_document_id: String,
    pub mixed_load_probe_runs: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProductionVectorQualificationConfig {
    /// Full-residency algorithm oracle and lifecycle fixture.
    pub projection_path: PathBuf,
    /// Production out-of-core generation whose document identity is released.
    pub search_projection_path: PathBuf,
    pub query_cases: Vec<ProductionVectorQueryCase>,
    pub lifecycle: ProductionVectorLifecycleConfig,
    pub warmup_runs: usize,
    pub measurement_runs: usize,
    pub recall_samples: usize,
    pub top_k: usize,
    pub candidate_limit: usize,
    pub minimum_recall_per_million: u32,
    pub require_rabitq_reference_verification: bool,
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
}

impl ProductionVectorQualificationConfig {
    fn validate(&self) -> Result<(), ProductionVectorQualificationError> {
        if !self.projection_path.is_dir() {
            return Err(ProductionVectorQualificationError::new(
                "production vector qualification requires an existing projection directory",
            ));
        }
        if !self.search_projection_path.is_dir() {
            return Err(ProductionVectorQualificationError::new(
                "production vector qualification requires an existing out-of-core search projection directory",
            ));
        }
        if self.measurement_runs == 0
            || self.recall_samples == 0
            || self.top_k == 0
            || self.candidate_limit < self.top_k
            || self.recall_samples > MAX_VECTOR_RECALL_VALIDATION_SAMPLES
            || self.top_k > MAX_VECTOR_RECALL_VALIDATION_TOP_K
            || self.candidate_limit > MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT
            || self.max_working_bytes == 0
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector qualification requires bounded non-zero measurements, recall samples, TopK, working memory, and a candidate limit at least as large as TopK",
            ));
        }
        validate_production_identity_for_current_target(
            &self.evidence_binding,
            &self.expected_identity,
        )
        .map_err(ProductionVectorQualificationError::from_error)?;
        validate_query_cases(&self.query_cases, &self.expected_identity)?;
        self.lifecycle
            .validate(&self.projection_path, &self.expected_identity)?;
        Ok(())
    }

    fn execution_options<'a>(
        &self,
        task_context: &'a RuntimeTaskContext,
        kernel: VectorSearchKernelPreference,
    ) -> VectorSearchExecutionOptions<'a> {
        VectorSearchExecutionOptions::admitted(self.max_working_bytes, task_context)
            .with_kernel(kernel)
            .capture_candidates_for_validation()
    }
}

impl ProductionVectorLifecycleConfig {
    fn validate(
        &self,
        source_path: &Path,
        expected_identity: &ProductionQualificationIdentity,
    ) -> Result<(), ProductionVectorQualificationError> {
        if self.replica_paths.len() < 3 || self.mixed_load_probe_runs == 0 {
            return Err(ProductionVectorQualificationError::new(
                "production vector lifecycle requires at least three replicas and a non-zero mixed-load run count",
            ));
        }
        if self.delta.upserts.is_empty() || self.delta.deletes.is_empty() {
            return Err(ProductionVectorQualificationError::new(
                "production vector lifecycle delta requires both upsert and delete operations",
            ));
        }
        if self.delta.source_graph_commit_epoch
            != Some(expected_identity.canonical_graph_commit_epoch)
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector lifecycle delta must retain the expected canonical graph epoch",
            ));
        }
        if self.expected_upsert_document_id.trim().is_empty()
            || self.expected_deleted_document_id.trim().is_empty()
            || self.expected_upsert_document_id == self.expected_deleted_document_id
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector lifecycle document identities are invalid",
            ));
        }
        let source_path = std::fs::canonicalize(source_path).map_err(|_| {
            ProductionVectorQualificationError::new(
                "production vector source projection path cannot be canonicalized",
            )
        })?;
        let mut paths = BTreeSet::new();
        for path in self
            .replica_paths
            .iter()
            .chain(std::iter::once(&self.corruption_replica_path))
        {
            let canonical_path = std::fs::canonicalize(path).map_err(|_| {
                ProductionVectorQualificationError::new(
                    "production vector lifecycle replica path cannot be canonicalized",
                )
            })?;
            if !canonical_path.is_dir()
                || canonical_path == source_path
                || !paths.insert(canonical_path)
            {
                return Err(ProductionVectorQualificationError::new(
                    "production vector lifecycle replicas must be distinct existing directories separate from the source projection",
                ));
            }
        }
        validate_query_case(&self.verification_case)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorQualificationError {
    message: String,
}

impl ProductionVectorQualificationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn from_error(error: impl Display) -> Self {
        Self::new(error.to_string())
    }
}

impl Display for ProductionVectorQualificationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProductionVectorQualificationError {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ProductionVectorLifecycleReport {
    pub incremental_fallback_safe: bool,
    pub checkpoint_reopen_restores_projection: bool,
    pub stale_generation_isolated: bool,
    pub corrupt_projection_rejected: bool,
    pub cancellation_propagated: bool,
    pub serving_cancellation_propagated: bool,
    pub mixed_foreground_background: bool,
    pub update_latency: LatencyPercentiles,
    pub checkpoint_latency: LatencyPercentiles,
    pub reopen_latency: LatencyPercentiles,
    pub cancellation_latency: LatencyPercentiles,
    pub serving_cancellation_latency: LatencyPercentiles,
    pub checkpoint_write_amplification_per_million: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorQualificationReport {
    pub protocol: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: ProductionEvidenceBinding,
    pub expected_identity: ProductionQualificationIdentity,
    pub projection_identity: VectorProjectionQualificationIdentity,
    pub oracle_projection_identity: VectorProjectionQualificationIdentity,
    pub search_projection_identity: SearchProjectionQualificationIdentity,
    pub projection_resources: VectorProjectionResourceEvidence,
    pub recall_evidence: Vec<ProductionVectorRecallEvidence>,
    pub query_evidence: Vec<ProductionVectorQueryEvidence>,
    pub rabitq_reference_verification: ProductionVectorRaBitQReferenceEvidence,
    pub lifecycle: ProductionVectorLifecycleReport,
    pub process_memory: ProcessMemoryProfile,
}

impl ProductionVectorQualificationReport {
    pub fn json(&self) -> serde_json::Value {
        let blockers = self.recompute_blocker_codes();
        serde_json::json!({
            "protocol": self.protocol,
            "evidence_kind": "representative_production_vector_replica",
            "production_eligible": true,
            "ready": blockers.is_empty(),
            "blocker_codes": blockers,
            "evidence_binding": self.evidence_binding.json(),
            "expected_identity": self.expected_identity.json(),
            "projection_identity": self.projection_identity.json(),
            "oracle_projection_identity": self.oracle_projection_identity.json(),
            "search_projection_identity": self.search_projection_identity.json(),
            "projection_resources": self.projection_resources.json(),
            "recall_evidence": self.recall_evidence.iter().map(ProductionVectorRecallEvidence::json).collect::<Vec<_>>(),
            "query_evidence": self.query_evidence.iter().map(ProductionVectorQueryEvidence::json).collect::<Vec<_>>(),
            "rabitq_reference_verification": self.rabitq_reference_verification.json(),
            "lifecycle": self.lifecycle,
            "process_memory": process_memory_json(self.process_memory),
        })
    }

    fn recompute_blocker_codes(&self) -> Vec<String> {
        let mut blockers = Vec::new();
        if self.protocol != PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL {
            blockers.push("protocol_mismatch".to_string());
        }
        if self
            .evidence_binding
            .validate_for(&self.expected_identity)
            .is_err()
        {
            blockers.push("production_evidence_binding_invalid".to_string());
        }
        if self.projection_identity.document_count < MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT {
            blockers.push("dataset_too_small".to_string());
        }
        if !self.projection_identity.file_backed
            || self.projection_identity.projection_generation == 0
            || self.projection_identity.source_graph_commit_epoch
                != Some(self.expected_identity.canonical_graph_commit_epoch)
        {
            blockers.push("projection_identity_not_production_bound".to_string());
        }
        if self.projection_identity.projection_generation
            != self.search_projection_identity.projection_generation
            || self.projection_identity.source_graph_commit_epoch
                != self.search_projection_identity.source_graph_commit_epoch
            || self.projection_identity.embedding_model
                != self.search_projection_identity.embedding_model
            || self.projection_identity.embedding_version
                != self.search_projection_identity.embedding_version
            || Some(self.projection_identity.dimension)
                != self.search_projection_identity.embedding_dimension
        {
            blockers.push("search_vector_generation_identity_mismatch".to_string());
        }
        if !same_vector_algorithm_identity(
            &self.projection_identity,
            &self.oracle_projection_identity,
        ) {
            blockers.push("serving_vector_oracle_identity_mismatch".to_string());
        }
        if self.projection_resources.segment_count == 0
            || self.projection_resources.raw_vector_bytes == 0
            || self.projection_resources.projection_payload_bytes == 0
            || self.projection_resources.configured_build_working_bytes == 0
            || self.projection_resources.peak_build_working_bytes == 0
        {
            blockers.push("projection_resource_evidence_incomplete".to_string());
        }
        if self.recall_evidence.is_empty()
            || self
                .recall_evidence
                .iter()
                .any(|evidence| !evidence.report.validates_required_approximate_backend())
        {
            blockers.push("recall_evidence_failed".to_string());
        }
        if self.query_evidence.is_empty()
            || self.query_evidence.iter().any(|evidence| {
                !evidence.auto_scalar_candidate_parity
                    || !evidence.auto_scalar_final_parity
                    || !evidence.serving_auto_final_parity
                    || evidence.auto_metrics.kernel.is_empty()
                    || evidence.auto_metrics.max_admitted_workers == 0
                    || evidence.scalar_candidate_metrics.kernel != "scalar"
                    || evidence.serving_metrics.backend
                        != "hawdb_rabitq_out_of_core_candidate_projection"
                    || evidence.serving_metrics.candidate_score_source != "quantized_projection"
                    || evidence.serving_metrics.final_score_source != "raw_vector"
                    || evidence.serving_metrics.kernel.is_empty()
                    || evidence.serving_metrics.max_admitted_workers == 0
                    || evidence.serving_metrics.projection_payload_bytes_read == 0
                    || evidence.auto_metrics.fallback_count > 0
                    || evidence.scalar_candidate_metrics.fallback_count > 0
                    || evidence.serving_metrics.fallback_count > 0
            })
        {
            blockers.push("query_execution_evidence_failed".to_string());
        }
        if self.rabitq_reference_verification.required && !self.rabitq_reference_verification.ready
        {
            blockers.push("rabitq_reference_verification_unavailable".to_string());
        }
        if !self.process_memory.capabilities.resident_memory {
            blockers.push("resident_memory_metric_unavailable".to_string());
        }
        if !self.process_memory.capabilities.total_page_faults {
            blockers.push("total_page_fault_metric_unavailable".to_string());
        }
        let lifecycle = &self.lifecycle;
        if !lifecycle.incremental_fallback_safe
            || !lifecycle.checkpoint_reopen_restores_projection
            || !lifecycle.stale_generation_isolated
            || !lifecycle.corrupt_projection_rejected
            || !lifecycle.cancellation_propagated
            || !lifecycle.serving_cancellation_propagated
            || !lifecycle.mixed_foreground_background
        {
            blockers.push("projection_lifecycle_evidence_failed".to_string());
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }
}

pub fn run_production_vector_qualification(
    config: ProductionVectorQualificationConfig,
) -> Result<ProductionVectorQualificationReport, ProductionVectorQualificationError> {
    config.validate()?;
    let reference_search_identity = SearchOutOfCoreReader::open(&config.projection_path)
        .map_err(ProductionVectorQualificationError::from_error)?
        .production_qualification_identity();
    let serving_config = SearchOutOfCoreConfig {
        max_vector_candidates: NonZeroUsize::new(config.candidate_limit)
            .expect("validated candidate limit is non-zero"),
        max_vector_search_working_bytes: NonZeroUsize::new(config.max_working_bytes)
            .expect("validated vector working memory is non-zero"),
        max_vector_search_parallelism: config.max_parallelism,
        ..SearchOutOfCoreConfig::default()
    };
    let serving_reader =
        SearchOutOfCoreReader::open_with_config(&config.search_projection_path, serving_config)
            .map_err(ProductionVectorQualificationError::from_error)?;
    let search_projection_identity = serving_reader.production_qualification_identity();
    if !same_search_document_identity(&reference_search_identity, &search_projection_identity) {
        return Err(ProductionVectorQualificationError::new(
            "production vector oracle document identity does not match the released out-of-core search generation",
        ));
    }
    let projection_identity = serving_reader
        .vector_projection_qualification_identity()
        .ok_or_else(|| {
            ProductionVectorQualificationError::new(
                "production vector qualification requires RaBitQ on the released out-of-core search generation",
            )
        })?;
    let projection_resources = serving_reader
        .vector_projection_resource_evidence()
        .ok_or_else(|| {
            ProductionVectorQualificationError::new(
                "released out-of-core RaBitQ resource evidence is unavailable",
            )
        })?;
    if projection_identity.source_graph_commit_epoch
        != Some(config.expected_identity.canonical_graph_commit_epoch)
    {
        return Err(ProductionVectorQualificationError::new(
            "production vector projection epoch does not match the expected release identity",
        ));
    }
    let task_context =
        RuntimeTaskContext::default().with_admitted_parallelism(config.max_parallelism);
    for _ in 0..config.warmup_runs {
        for query_case in &config.query_cases {
            execute_serving_query(&serving_reader, query_case, &config, &task_context)?;
        }
    }
    let process_start =
        ProcessMemorySnapshot::capture().map_err(ProductionVectorQualificationError::from_error)?;
    let serving_runs = config
        .query_cases
        .iter()
        .map(|query_case| {
            measure_serving_query(&serving_reader, query_case, &config, &task_context)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (serving_cancellation_propagated, serving_cancellation_micros) =
        run_serving_cancellation_probe(
            &serving_reader,
            &config.lifecycle.verification_case,
            &config,
        )?;
    let process_end =
        ProcessMemorySnapshot::capture().map_err(ProductionVectorQualificationError::from_error)?;
    let process_memory = ProcessMemoryProfile::between(process_start, process_end);
    drop(serving_reader);

    let index = SearchIndex::open(&config.projection_path)
        .map_err(ProductionVectorQualificationError::from_error)?;
    let oracle_projection_identity = index
        .vector_projection_qualification_identity()
        .ok_or_else(|| {
            ProductionVectorQualificationError::new(
                "production vector qualification requires a valid file-backed RaBitQ projection",
            )
        })?;
    if !same_vector_algorithm_identity(&projection_identity, &oracle_projection_identity) {
        return Err(ProductionVectorQualificationError::new(
            "production vector oracle algorithm identity does not match the released RaBitQ projection",
        ));
    }

    for _ in 0..config.warmup_runs {
        for query_case in &config.query_cases {
            execute_query(
                &index,
                query_case,
                &config,
                &task_context,
                VectorExecutionProfile::AutoCandidate,
            )?;
        }
    }

    let auto_runs = config
        .query_cases
        .iter()
        .map(|query_case| {
            measure_query(
                &index,
                query_case,
                &config,
                &task_context,
                VectorExecutionProfile::AutoCandidate,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let recall_evidence = config
        .query_cases
        .iter()
        .map(|query_case| collect_recall_evidence(&index, query_case, &config))
        .collect::<Result<Vec<_>, _>>()?;
    let scalar_candidate_runs = config
        .query_cases
        .iter()
        .map(|query_case| {
            measure_query(
                &index,
                query_case,
                &config,
                &task_context,
                VectorExecutionProfile::ScalarCandidate,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let exact_runs = config
        .query_cases
        .iter()
        .map(|query_case| {
            measure_query(
                &index,
                query_case,
                &config,
                &task_context,
                VectorExecutionProfile::ExactRaw,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let query_evidence = config
        .query_cases
        .iter()
        .zip(auto_runs)
        .zip(scalar_candidate_runs)
        .zip(exact_runs)
        .zip(serving_runs)
        .map(
            |((((query_case, auto), scalar_candidate), exact), serving)| {
                ProductionVectorQueryEvidence {
                    name: query_case.name.clone(),
                    kind: query_case.kind,
                    request_digest: request_digest(query_case, &config),
                    exact_result_digest: exact.result_digest.clone(),
                    auto_result_digest: auto.result_digest.clone(),
                    scalar_candidate_result_digest: scalar_candidate.result_digest.clone(),
                    serving_result_digest: serving.result_digest.clone(),
                    auto_candidate_digest: auto.candidate_digest.clone(),
                    scalar_candidate_digest: scalar_candidate.candidate_digest.clone(),
                    auto_scalar_candidate_parity: auto.candidate_digest
                        == scalar_candidate.candidate_digest,
                    auto_scalar_final_parity: auto.result_digest == scalar_candidate.result_digest,
                    serving_auto_final_parity: serving.result_digest == auto.result_digest,
                    auto_final_matches_exact: auto.result_digest == exact.result_digest,
                    scalar_candidate_final_matches_exact: scalar_candidate.result_digest
                        == exact.result_digest,
                    exact_latency: latency_percentiles(&exact.latencies),
                    auto_latency: latency_percentiles(&auto.latencies),
                    scalar_candidate_latency: latency_percentiles(&scalar_candidate.latencies),
                    serving_latency: latency_percentiles(&serving.latencies),
                    auto_metrics: auto.metrics,
                    scalar_candidate_metrics: scalar_candidate.metrics,
                    serving_metrics: serving.metrics,
                }
            },
        )
        .collect::<Vec<_>>();
    let rabitq_reference_verification = oracle::collect_reference_verification(
        &config,
        &query_evidence,
        usize::from(projection_identity.bit_width),
    )?;
    drop(index);

    let mut lifecycle = lifecycle::run_lifecycle(&config)?;
    lifecycle.serving_cancellation_propagated = serving_cancellation_propagated;
    lifecycle.serving_cancellation_latency = latency_percentiles(&[serving_cancellation_micros]);
    let mut report = ProductionVectorQualificationReport {
        protocol: PRODUCTION_VECTOR_QUALIFICATION_PROTOCOL.to_string(),
        ready: false,
        blocker_codes: Vec::new(),
        evidence_binding: config.evidence_binding,
        expected_identity: config.expected_identity,
        projection_identity,
        oracle_projection_identity,
        search_projection_identity,
        projection_resources,
        recall_evidence,
        query_evidence,
        rabitq_reference_verification,
        lifecycle,
        process_memory,
    };
    report.blocker_codes = report.recompute_blocker_codes();
    report.ready = report.blocker_codes.is_empty();
    Ok(report)
}

fn run_serving_cancellation_probe(
    reader: &SearchOutOfCoreReader,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
) -> Result<(bool, u64), ProductionVectorQualificationError> {
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task_context = RuntimeTaskContext::without_deadline(token)
        .with_admitted_parallelism(config.max_parallelism);
    let started = Instant::now();
    let result = execute_serving_query(reader, query_case, config, &task_context);
    let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    Ok((
        matches!(result, Err(ref error) if error.to_string().contains("cancelled")),
        elapsed,
    ))
}

fn same_vector_algorithm_identity(
    serving: &VectorProjectionQualificationIdentity,
    oracle: &VectorProjectionQualificationIdentity,
) -> bool {
    serving.source_graph_commit_epoch == oracle.source_graph_commit_epoch
        && serving.document_count == oracle.document_count
        && serving.format_version == oracle.format_version
        && serving.algorithm == oracle.algorithm
        && serving.bit_width == oracle.bit_width
        && serving.dimension == oracle.dimension
        && serving.transform_seed == oracle.transform_seed
        && serving.embedding_model == oracle.embedding_model
        && serving.embedding_version == oracle.embedding_version
        && serving.file_backed
        && oracle.file_backed
}

fn same_search_document_identity(
    left: &SearchProjectionQualificationIdentity,
    right: &SearchProjectionQualificationIdentity,
) -> bool {
    left.source_graph_commit_epoch == right.source_graph_commit_epoch
        && left.document_count == right.document_count
        && left.documents_digest == right.documents_digest
        && left.analyzer_digest == right.analyzer_digest
        && left.embedding_model == right.embedding_model
        && left.embedding_version == right.embedding_version
        && left.embedding_dimension == right.embedding_dimension
}

fn validate_query_cases(
    query_cases: &[ProductionVectorQueryCase],
    expected_identity: &ProductionQualificationIdentity,
) -> Result<(), ProductionVectorQualificationError> {
    if query_cases.is_empty() {
        return Err(ProductionVectorQualificationError::new(
            "production vector qualification requires query cases",
        ));
    }
    let mut names = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    for query_case in query_cases {
        validate_query_case(query_case)?;
        if !names.insert(query_case.name.clone()) {
            return Err(ProductionVectorQualificationError::new(
                "production vector qualification query case names must be unique",
            ));
        }
        kinds.insert(query_case.kind);
    }
    for required in [
        ProductionVectorCaseKind::Unfiltered,
        ProductionVectorCaseKind::MetadataFiltered,
    ] {
        if !kinds.contains(&required) {
            return Err(ProductionVectorQualificationError::new(format!(
                "production vector qualification requires a {} case",
                required.as_str()
            )));
        }
    }
    let acl_enabled = expected_identity
        .enabled_features
        .iter()
        .any(|feature| feature == "acl");
    if acl_enabled && !kinds.contains(&ProductionVectorCaseKind::AclFiltered) {
        return Err(ProductionVectorQualificationError::new(
            "production vector qualification requires an ACL case when acl is enabled",
        ));
    }
    Ok(())
}

fn validate_query_case(
    query_case: &ProductionVectorQueryCase,
) -> Result<(), ProductionVectorQualificationError> {
    if query_case.name.trim().is_empty()
        || query_case.query_embedding.is_empty()
        || !query_case
            .query_embedding
            .iter()
            .all(|value| value.is_finite())
    {
        return Err(ProductionVectorQualificationError::new(
            "production vector query cases require a name and a finite non-empty embedding",
        ));
    }
    match query_case.kind {
        ProductionVectorCaseKind::Unfiltered
            if !query_case.metadata_filters.is_empty() || query_case.access_control.is_some() =>
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector unfiltered case must not contain filters",
            ));
        }
        ProductionVectorCaseKind::MetadataFiltered
            if query_case.metadata_filters.is_empty() || query_case.access_control.is_some() =>
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector metadata case requires metadata filters without ACL context",
            ));
        }
        ProductionVectorCaseKind::AclFiltered if query_case.access_control.is_none() => {
            return Err(ProductionVectorQualificationError::new(
                "production vector ACL case requires access-control context",
            ));
        }
        _ => {}
    }
    query_case.effective_metadata_filters()?;
    Ok(())
}

pub(super) fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub(super) fn fallback_is_projection_unavailable(result: &SearchResultSet) -> bool {
    result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .is_some_and(|retriever| {
            retriever
                .fallback_reason_codes
                .contains(&SearchFallbackReasonCode::CompressedVectorProjectionUnavailable)
        })
}

fn process_memory_json(profile: ProcessMemoryProfile) -> serde_json::Value {
    serde_json::json!({
        "capabilities": {
            "resident_memory": profile.capabilities.resident_memory,
            "total_page_faults": profile.capabilities.total_page_faults,
            "split_page_faults": profile.capabilities.split_page_faults,
        },
        "start_resident_bytes": profile.start_resident_bytes,
        "start_peak_resident_bytes": profile.start_peak_resident_bytes,
        "steady_resident_bytes": profile.steady_resident_bytes,
        "peak_resident_bytes": profile.peak_resident_bytes,
        "steady_resident_growth_bytes": profile.steady_resident_growth_bytes,
        "lifetime_peak_resident_growth_bytes": profile.lifetime_peak_resident_growth_bytes,
        "total_page_faults": profile.total_page_faults,
        "minor_page_faults": profile.minor_page_faults,
        "major_page_faults": profile.major_page_faults,
    })
}

#[cfg(test)]
#[path = "production_vector/tests.rs"]
mod tests;
