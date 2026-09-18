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
    elapsed_micros, ProductionVectorCaseKind, ProductionVectorQualificationConfig,
    ProductionVectorQualificationError, ProductionVectorQueryCase,
};
use crate::LatencyPercentiles;
use hawdb::{
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, RuntimeTaskContext, SearchIndex,
    SearchMode, SearchOutOfCoreReader, SearchQueryOptions, SearchResultSet,
    VectorRecallValidationOptions, VectorRecallValidationReport, VectorSearchKernelPreference,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorRecallEvidence {
    pub name: String,
    pub kind: ProductionVectorCaseKind,
    pub request_digest: String,
    pub report: VectorRecallValidationReport,
}

impl ProductionVectorRecallEvidence {
    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "kind": self.kind.as_str(),
            "request_digest": self.request_digest,
            "report": self.report.json(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ProductionVectorExecutionMetrics {
    pub backend: String,
    pub candidate_score_source: String,
    pub final_score_source: String,
    pub kernel: String,
    pub max_admitted_workers: usize,
    pub segment_count: usize,
    pub scanned_segment_count: usize,
    pub scored_document_count: usize,
    pub filtered_document_count: usize,
    pub scanned_block_count: usize,
    pub skipped_block_count: usize,
    pub projection_payload_bytes_read: u64,
    pub raw_vector_bytes_read: u64,
    pub peak_admitted_working_bytes: usize,
    pub fallback_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorQueryEvidence {
    pub name: String,
    pub kind: ProductionVectorCaseKind,
    pub request_digest: String,
    pub exact_result_digest: String,
    pub auto_result_digest: String,
    pub scalar_candidate_result_digest: String,
    pub serving_result_digest: String,
    pub auto_candidate_digest: String,
    pub scalar_candidate_digest: String,
    pub auto_scalar_candidate_parity: bool,
    pub auto_scalar_final_parity: bool,
    pub serving_auto_final_parity: bool,
    pub auto_final_matches_exact: bool,
    pub scalar_candidate_final_matches_exact: bool,
    pub exact_latency: LatencyPercentiles,
    pub auto_latency: LatencyPercentiles,
    pub scalar_candidate_latency: LatencyPercentiles,
    pub serving_latency: LatencyPercentiles,
    pub auto_metrics: ProductionVectorExecutionMetrics,
    pub scalar_candidate_metrics: ProductionVectorExecutionMetrics,
    pub serving_metrics: ProductionVectorExecutionMetrics,
}

impl ProductionVectorQueryEvidence {
    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "kind": self.kind.as_str(),
            "request_digest": self.request_digest,
            "exact_result_digest": self.exact_result_digest,
            "auto_result_digest": self.auto_result_digest,
            "scalar_candidate_result_digest": self.scalar_candidate_result_digest,
            "serving_result_digest": self.serving_result_digest,
            "auto_candidate_digest": self.auto_candidate_digest,
            "scalar_candidate_digest": self.scalar_candidate_digest,
            "auto_scalar_candidate_parity": self.auto_scalar_candidate_parity,
            "auto_scalar_final_parity": self.auto_scalar_final_parity,
            "serving_auto_final_parity": self.serving_auto_final_parity,
            "auto_final_matches_exact": self.auto_final_matches_exact,
            "scalar_candidate_final_matches_exact": self.scalar_candidate_final_matches_exact,
            "exact_latency": self.exact_latency,
            "auto_latency": self.auto_latency,
            "scalar_candidate_latency": self.scalar_candidate_latency,
            "serving_latency": self.serving_latency,
            "auto_metrics": self.auto_metrics,
            "scalar_candidate_metrics": self.scalar_candidate_metrics,
            "serving_metrics": self.serving_metrics,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum VectorExecutionProfile {
    AutoCandidate,
    ScalarCandidate,
    ExactRaw,
}

#[derive(Default)]
pub(super) struct QueryMeasurement {
    pub(super) result_digest: String,
    pub(super) candidate_digest: String,
    pub(super) latencies: Vec<u64>,
    pub(super) metrics: ProductionVectorExecutionMetrics,
}

pub(super) fn measure_query(
    index: &SearchIndex,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
    task_context: &RuntimeTaskContext,
    profile: VectorExecutionProfile,
) -> Result<QueryMeasurement, ProductionVectorQualificationError> {
    let mut measurement = QueryMeasurement {
        latencies: Vec::with_capacity(config.measurement_runs),
        ..QueryMeasurement::default()
    };
    for _ in 0..config.measurement_runs {
        let started = Instant::now();
        let result = execute_query(index, query_case, config, task_context, profile)?;
        measurement.latencies.push(elapsed_micros(started));
        let result_digest = result_digest(&result);
        let candidate_digest = candidate_digest(&result);
        if !measurement.result_digest.is_empty() && measurement.result_digest != result_digest {
            return Err(ProductionVectorQualificationError::new(
                "production vector query returned non-deterministic final results",
            ));
        }
        if !measurement.candidate_digest.is_empty()
            && measurement.candidate_digest != candidate_digest
        {
            return Err(ProductionVectorQualificationError::new(
                "production vector query returned non-deterministic candidate results",
            ));
        }
        measurement.result_digest = result_digest;
        measurement.candidate_digest = candidate_digest;
        add_execution_metrics(&mut measurement.metrics, &result);
    }
    Ok(measurement)
}

pub(super) fn measure_serving_query(
    reader: &SearchOutOfCoreReader,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
    task_context: &RuntimeTaskContext,
) -> Result<QueryMeasurement, ProductionVectorQualificationError> {
    let mut measurement = QueryMeasurement {
        latencies: Vec::with_capacity(config.measurement_runs),
        ..QueryMeasurement::default()
    };
    for _ in 0..config.measurement_runs {
        let started = Instant::now();
        let result = execute_serving_query(reader, query_case, config, task_context)?;
        measurement.latencies.push(elapsed_micros(started));
        let result_digest = result_digest(&result);
        let candidate_digest = candidate_digest(&result);
        if !measurement.result_digest.is_empty() && measurement.result_digest != result_digest {
            return Err(ProductionVectorQualificationError::new(
                "production out-of-core vector query returned non-deterministic final results",
            ));
        }
        if !measurement.candidate_digest.is_empty()
            && measurement.candidate_digest != candidate_digest
        {
            return Err(ProductionVectorQualificationError::new(
                "production out-of-core vector query returned non-deterministic candidates",
            ));
        }
        measurement.result_digest = result_digest;
        measurement.candidate_digest = candidate_digest;
        add_execution_metrics(&mut measurement.metrics, &result);
    }
    Ok(measurement)
}

pub(super) fn execute_serving_query(
    reader: &SearchOutOfCoreReader,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
    task_context: &RuntimeTaskContext,
) -> Result<SearchResultSet, ProductionVectorQualificationError> {
    reader
        .search_with_options_compressed_vector_projection_context(
            "",
            Some(&query_case.query_embedding),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: config.top_k,
                offset: 0,
                rank_window: Some(config.candidate_limit),
                fusion_weights: hawdb::SearchFusionWeights::default(),
                metadata_filters: query_case.effective_metadata_filters()?,
                policy_epoch: query_case
                    .access_control
                    .as_ref()
                    .map(|access_control| access_control.policy_epoch),
            },
            CompressedVectorSearchMode::Required,
            task_context,
        )
        .map(|output| output.result)
        .map_err(ProductionVectorQualificationError::from_error)
}

pub(super) fn execute_query(
    index: &SearchIndex,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
    task_context: &RuntimeTaskContext,
    profile: VectorExecutionProfile,
) -> Result<SearchResultSet, ProductionVectorQualificationError> {
    let metadata_filters = query_case.effective_metadata_filters()?;
    let (compression_mode, kernel) = match profile {
        VectorExecutionProfile::AutoCandidate => (
            CompressedVectorSearchMode::Required,
            VectorSearchKernelPreference::Auto,
        ),
        VectorExecutionProfile::ScalarCandidate => (
            CompressedVectorSearchMode::Required,
            VectorSearchKernelPreference::Scalar,
        ),
        VectorExecutionProfile::ExactRaw => (
            CompressedVectorSearchMode::Disabled,
            VectorSearchKernelPreference::Scalar,
        ),
    };
    index
        .try_search_with_options_adaptive_vector_projection_context(
            "",
            Some(&query_case.query_embedding),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: config.top_k,
                offset: 0,
                rank_window: Some(config.candidate_limit),
                fusion_weights: hawdb::SearchFusionWeights::default(),
                metadata_filters,
                policy_epoch: query_case
                    .access_control
                    .as_ref()
                    .map(|access_control| access_control.policy_epoch),
            },
            AdaptiveVectorSearchOptions::new(compression_mode),
            config.execution_options(task_context, kernel),
        )
        .map_err(ProductionVectorQualificationError::from_error)
}

pub(super) fn collect_recall_evidence(
    index: &SearchIndex,
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
) -> Result<ProductionVectorRecallEvidence, ProductionVectorQualificationError> {
    let options = VectorRecallValidationOptions {
        max_samples: config.recall_samples,
        top_k: config.top_k,
        candidate_limit: config.candidate_limit,
        minimum_recall_per_million: config.minimum_recall_per_million,
        metadata_filters: query_case.metadata_filters.clone(),
    };
    let report = match &query_case.access_control {
        Some(access_control) => {
            index.validate_sampled_vector_recall_access_control(options, access_control)
        }
        None => index.validate_sampled_vector_recall(options),
    };
    Ok(ProductionVectorRecallEvidence {
        name: query_case.name.clone(),
        kind: query_case.kind,
        request_digest: request_digest(query_case, config),
        report,
    })
}

fn add_execution_metrics(
    aggregate: &mut ProductionVectorExecutionMetrics,
    result: &SearchResultSet,
) {
    let Some(vector) = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
    else {
        return;
    };
    merge_stable_label(&mut aggregate.backend, &vector.backend);
    merge_stable_label(
        &mut aggregate.candidate_score_source,
        &vector.candidate_score_source,
    );
    merge_stable_label(
        &mut aggregate.final_score_source,
        &vector.final_score_source,
    );
    if let Some(kernel) = &vector.candidate_scan_kernel {
        if aggregate.kernel.is_empty() {
            aggregate.kernel = kernel.clone();
        } else if aggregate.kernel != *kernel {
            aggregate.kernel = "mixed".to_string();
        }
    }
    aggregate.max_admitted_workers = aggregate
        .max_admitted_workers
        .max(vector.candidate_scan_worker_count);
    aggregate.segment_count = aggregate
        .segment_count
        .max(vector.candidate_scan_segment_count);
    aggregate.scanned_segment_count = aggregate
        .scanned_segment_count
        .saturating_add(vector.candidate_scan_scanned_segment_count);
    aggregate.scored_document_count = aggregate
        .scored_document_count
        .saturating_add(vector.candidate_scan_scored_document_count);
    aggregate.filtered_document_count = aggregate
        .filtered_document_count
        .saturating_add(vector.candidate_scan_filtered_document_count);
    aggregate.scanned_block_count = aggregate
        .scanned_block_count
        .saturating_add(vector.candidate_scan_scanned_block_count);
    aggregate.skipped_block_count = aggregate
        .skipped_block_count
        .saturating_add(vector.candidate_scan_skipped_block_count);
    aggregate.projection_payload_bytes_read = aggregate
        .projection_payload_bytes_read
        .saturating_add(vector.candidate_scan_payload_bytes_read);
    aggregate.raw_vector_bytes_read = aggregate
        .raw_vector_bytes_read
        .saturating_add(vector.raw_vector_bytes_read);
    aggregate.peak_admitted_working_bytes = aggregate
        .peak_admitted_working_bytes
        .max(vector.candidate_scan_admitted_working_bytes);
    if !vector.fallback_reason_codes.is_empty() {
        aggregate.fallback_count = aggregate.fallback_count.saturating_add(1);
    }
}

fn merge_stable_label(aggregate: &mut String, observed: &str) {
    if aggregate.is_empty() {
        *aggregate = observed.to_string();
    } else if aggregate != observed {
        *aggregate = "mixed".to_string();
    }
}

pub(super) fn result_contains(result: &SearchResultSet, document_id: &str) -> bool {
    result.hits.iter().any(|hit| hit.id == document_id)
}

pub(super) fn query_options(
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
) -> Result<SearchQueryOptions, ProductionVectorQualificationError> {
    Ok(SearchQueryOptions {
        limit: config.top_k,
        offset: 0,
        rank_window: Some(config.candidate_limit),
        fusion_weights: hawdb::SearchFusionWeights::default(),
        metadata_filters: query_case.effective_metadata_filters()?,
        policy_epoch: query_case
            .access_control
            .as_ref()
            .map(|access_control| access_control.policy_epoch),
    })
}

pub(super) fn request_digest(
    query_case: &ProductionVectorQueryCase,
    config: &ProductionVectorQualificationConfig,
) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"hawdb-production-vector-request-v1");
    hash_field(&mut hasher, query_case.kind.as_str().as_bytes());
    for value in &query_case.query_embedding {
        hasher.update(value.to_bits().to_le_bytes());
    }
    for (name, value) in &query_case.metadata_filters {
        hash_field(&mut hasher, name.as_bytes());
        hash_field(&mut hasher, value.as_bytes());
    }
    if let Some(access_control) = &query_case.access_control {
        hasher.update([1]);
        hasher.update(access_control.policy_epoch.to_le_bytes());
        hash_field(
            &mut hasher,
            access_control.visibility_metadata_field.as_bytes(),
        );
        for value in &access_control.allowed_visibility_values {
            hash_field(&mut hasher, value.as_bytes());
        }
    } else {
        hasher.update([0]);
    }
    hasher.update((config.top_k as u64).to_le_bytes());
    hasher.update((config.candidate_limit as u64).to_le_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

pub(super) fn result_digest(result: &SearchResultSet) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"hawdb-production-vector-result-v1");
    hasher.update((result.total_hits as u64).to_le_bytes());
    for hit in &result.hits {
        hash_field(&mut hasher, hit.id.as_bytes());
        hasher.update(hit.score.to_bits().to_le_bytes());
        hasher.update(hit.vector_score.to_bits().to_le_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

pub(super) fn candidate_digest(result: &SearchResultSet) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"hawdb-production-vector-candidates-v1");
    if let Some(vector) = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
    {
        for id in &vector.candidate_top_ids {
            hash_field(&mut hasher, id.as_bytes());
        }
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}
