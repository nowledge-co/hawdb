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

use crate::error::{HawDBError, Result};
use hawdb_evidence::production_evidence_blocker_codes;

pub const SEARCH_LEXICAL_QUALIFICATION_PROTOCOL: &str =
    "hawdb-search-lexical-production-qualification";
pub const SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION: u64 = 2;
const MINIMUM_DOCUMENT_COUNT: usize = 100_000;
const MAX_RSS_BUDGET_PER_MILLION: u64 = 1_100_000;
const MAX_WRITE_REGRESSION_PER_MILLION: u64 = 1_100_000;
const MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION: u64 = 500_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchLexicalFeasibilityCoverage {
    pub selective_identifier: bool,
    pub cjk_text: bool,
    pub common_term: bool,
    pub no_hit: bool,
    pub metadata_filter: bool,
    pub acl_filter: bool,
    pub hybrid_rrf: bool,
    pub bounded_generation_update: bool,
    pub bounded_rabitq_serving: bool,
    pub incremental_upsert_delete: bool,
    pub checkpoint_reopen: bool,
    pub corrupt_artifact: bool,
    pub stale_manifest: bool,
    pub mixed_foreground_background: bool,
    pub larger_than_memory: bool,
}

impl SearchLexicalFeasibilityCoverage {
    fn complete(&self, acl_required: bool) -> bool {
        self.selective_identifier
            && self.cjk_text
            && self.common_term
            && self.no_hit
            && self.metadata_filter
            && (!acl_required || self.acl_filter)
            && self.hybrid_rrf
            && self.bounded_generation_update
            && self.bounded_rabitq_serving
            && self.incremental_upsert_delete
            && self.checkpoint_reopen
            && self.corrupt_artifact
            && self.stale_manifest
            && self.mixed_foreground_background
            && self.larger_than_memory
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "selective_identifier": self.selective_identifier,
            "cjk_text": self.cjk_text,
            "common_term": self.common_term,
            "no_hit": self.no_hit,
            "metadata_filter": self.metadata_filter,
            "acl_filter": self.acl_filter,
            "hybrid_rrf": self.hybrid_rrf,
            "bounded_generation_update": self.bounded_generation_update,
            "bounded_rabitq_serving": self.bounded_rabitq_serving,
            "incremental_upsert_delete": self.incremental_upsert_delete,
            "checkpoint_reopen": self.checkpoint_reopen,
            "corrupt_artifact": self.corrupt_artifact,
            "stale_manifest": self.stale_manifest,
            "mixed_foreground_background": self.mixed_foreground_background,
            "larger_than_memory": self.larger_than_memory,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchLexicalFeasibilityMetrics {
    pub canonical_dataset_bytes: u64,
    pub storage_memory_budget_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub baseline_selective_text_p50_micros: u64,
    pub baseline_selective_text_p95_micros: u64,
    pub baseline_selective_text_p99_micros: u64,
    pub segmented_selective_text_p50_micros: u64,
    pub segmented_selective_text_p95_micros: u64,
    pub segmented_selective_text_p99_micros: u64,
    pub baseline_throughput_per_second: u64,
    pub segmented_throughput_per_second: u64,
    pub selective_posting_bytes_read: u64,
    pub selective_candidate_postings_visited: u64,
    pub selective_matching_document_count: usize,
    pub process_memory_capabilities: hawdb_qos::ProcessMemoryCapabilities,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
    pub metadata_sidecar_bytes_read: u64,
    pub vector_sidecar_bytes_read: u64,
    pub hydration_bytes: u64,
    pub segmented_vector_p50_micros: u64,
    pub segmented_vector_p95_micros: u64,
    pub segmented_vector_p99_micros: u64,
    pub segmented_hybrid_p50_micros: u64,
    pub segmented_hybrid_p95_micros: u64,
    pub segmented_hybrid_p99_micros: u64,
    pub baseline_update_p95_micros: u64,
    pub segmented_update_p95_micros: u64,
    pub baseline_checkpoint_p95_micros: u64,
    pub segmented_checkpoint_p95_micros: u64,
    pub consolidation_write_amplification_per_million: u64,
    pub recovery_p95_micros: u64,
}

impl SearchLexicalFeasibilityMetrics {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "canonical_dataset_bytes": self.canonical_dataset_bytes,
            "storage_memory_budget_bytes": self.storage_memory_budget_bytes,
            "steady_resident_bytes": self.steady_resident_bytes,
            "peak_resident_bytes": self.peak_resident_bytes,
            "baseline_selective_text_p50_micros": self.baseline_selective_text_p50_micros,
            "baseline_selective_text_p95_micros": self.baseline_selective_text_p95_micros,
            "baseline_selective_text_p99_micros": self.baseline_selective_text_p99_micros,
            "segmented_selective_text_p50_micros": self.segmented_selective_text_p50_micros,
            "segmented_selective_text_p95_micros": self.segmented_selective_text_p95_micros,
            "segmented_selective_text_p99_micros": self.segmented_selective_text_p99_micros,
            "baseline_throughput_per_second": self.baseline_throughput_per_second,
            "segmented_throughput_per_second": self.segmented_throughput_per_second,
            "selective_posting_bytes_read": self.selective_posting_bytes_read,
            "selective_candidate_postings_visited": self.selective_candidate_postings_visited,
            "selective_matching_document_count": self.selective_matching_document_count,
            "process_memory_capabilities": {
                "resident_memory": self.process_memory_capabilities.resident_memory,
                "total_page_faults": self.process_memory_capabilities.total_page_faults,
                "split_page_faults": self.process_memory_capabilities.split_page_faults,
            },
            "total_page_faults": self.total_page_faults,
            "minor_page_faults": self.minor_page_faults,
            "major_page_faults": self.major_page_faults,
            "metadata_sidecar_bytes_read": self.metadata_sidecar_bytes_read,
            "vector_sidecar_bytes_read": self.vector_sidecar_bytes_read,
            "hydration_bytes": self.hydration_bytes,
            "segmented_vector_p50_micros": self.segmented_vector_p50_micros,
            "segmented_vector_p95_micros": self.segmented_vector_p95_micros,
            "segmented_vector_p99_micros": self.segmented_vector_p99_micros,
            "segmented_hybrid_p50_micros": self.segmented_hybrid_p50_micros,
            "segmented_hybrid_p95_micros": self.segmented_hybrid_p95_micros,
            "segmented_hybrid_p99_micros": self.segmented_hybrid_p99_micros,
            "baseline_update_p95_micros": self.baseline_update_p95_micros,
            "segmented_update_p95_micros": self.segmented_update_p95_micros,
            "baseline_checkpoint_p95_micros": self.baseline_checkpoint_p95_micros,
            "segmented_checkpoint_p95_micros": self.segmented_checkpoint_p95_micros,
            "consolidation_write_amplification_per_million": self.consolidation_write_amplification_per_million,
            "recovery_p95_micros": self.recovery_p95_micros,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchProjectionQualificationIdentity {
    pub projection_generation: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub document_count: usize,
    pub documents_digest: u64,
    pub analyzer_digest: u64,
    pub embedding_model: Option<String>,
    pub embedding_version: Option<String>,
    pub embedding_dimension: Option<usize>,
}

impl SearchProjectionQualificationIdentity {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "projection_generation": self.projection_generation,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "document_count": self.document_count,
            "documents_digest": self.documents_digest,
            "analyzer_digest": self.analyzer_digest,
            "embedding_model": self.embedding_model,
            "embedding_version": self.embedding_version,
            "embedding_dimension": self.embedding_dimension,
        })
    }

    fn complete(&self) -> bool {
        self.projection_generation > 0
            && self.source_graph_commit_epoch.is_some()
            && self.document_count > 0
            && self.documents_digest > 0
            && self.analyzer_digest > 0
            && self
                .embedding_model
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .embedding_version
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .embedding_dimension
                .is_some_and(|dimension| dimension > 0)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchTopKScoreParity {
    pub text: bool,
    pub vector: bool,
    pub hybrid: bool,
}

impl SearchTopKScoreParity {
    fn complete(self) -> bool {
        self.text && self.vector && self.hybrid
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "text": self.text,
            "vector": self.vector,
            "hybrid": self.hybrid,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchLexicalProductionQualificationReport {
    pub protocol: String,
    pub protocol_version: u64,
    pub projection_generation: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub document_count: usize,
    pub projection_identity: SearchProjectionQualificationIdentity,
    pub evidence_binding: Option<crate::ProductionEvidenceBinding>,
    pub expected_identity: Option<crate::ProductionQualificationIdentity>,
    pub topk_score_parity: SearchTopKScoreParity,
    pub exact_topk_score_parity: bool,
    pub coverage: SearchLexicalFeasibilityCoverage,
    pub metrics: SearchLexicalFeasibilityMetrics,
    pub blocker_codes: Vec<String>,
    pub ready: bool,
}

impl SearchLexicalProductionQualificationReport {
    pub fn evaluate(
        projection_generation: u64,
        source_graph_commit_epoch: Option<u64>,
        document_count: usize,
        exact_topk_score_parity: bool,
        coverage: SearchLexicalFeasibilityCoverage,
        metrics: SearchLexicalFeasibilityMetrics,
    ) -> Self {
        let projection_identity = SearchProjectionQualificationIdentity {
            projection_generation,
            source_graph_commit_epoch,
            document_count,
            ..SearchProjectionQualificationIdentity::default()
        };
        Self::evaluate_internal(
            projection_identity,
            None,
            None,
            SearchTopKScoreParity {
                text: exact_topk_score_parity,
                vector: exact_topk_score_parity,
                hybrid: exact_topk_score_parity,
            },
            coverage,
            metrics,
        )
    }

    pub fn evaluate_for_production(
        projection_identity: SearchProjectionQualificationIdentity,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
        topk_score_parity: SearchTopKScoreParity,
        coverage: SearchLexicalFeasibilityCoverage,
        metrics: SearchLexicalFeasibilityMetrics,
    ) -> Self {
        Self::evaluate_internal(
            projection_identity,
            Some(evidence_binding),
            Some(expected_identity),
            topk_score_parity,
            coverage,
            metrics,
        )
    }

    fn evaluate_internal(
        projection_identity: SearchProjectionQualificationIdentity,
        evidence_binding: Option<crate::ProductionEvidenceBinding>,
        expected_identity: Option<crate::ProductionQualificationIdentity>,
        topk_score_parity: SearchTopKScoreParity,
        coverage: SearchLexicalFeasibilityCoverage,
        metrics: SearchLexicalFeasibilityMetrics,
    ) -> Self {
        let projection_generation = projection_identity.projection_generation;
        let source_graph_commit_epoch = projection_identity.source_graph_commit_epoch;
        let document_count = projection_identity.document_count;
        let mut report = Self {
            protocol: SEARCH_LEXICAL_QUALIFICATION_PROTOCOL.to_string(),
            protocol_version: SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION,
            projection_generation,
            source_graph_commit_epoch,
            document_count,
            projection_identity: projection_identity.clone(),
            evidence_binding,
            expected_identity,
            topk_score_parity,
            exact_topk_score_parity: topk_score_parity.complete(),
            coverage,
            metrics,
            blocker_codes: Vec::new(),
            ready: false,
        };
        report.blocker_codes = report.recompute_blocker_codes(&projection_identity);
        report.ready = report.blocker_codes.is_empty();
        report
    }

    pub fn validate_for_projection(
        &self,
        projection_identity: &SearchProjectionQualificationIdentity,
    ) -> Result<()> {
        let blockers = self.recompute_blocker_codes(projection_identity);
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(HawDBError::Storage(format!(
                "segmented lexical projection is not qualified for production: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn validate_for_projection_and_release(
        &self,
        projection_identity: &SearchProjectionQualificationIdentity,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Result<()> {
        let blockers =
            self.recompute_blocker_codes_for_release(projection_identity, expected_identity);
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(HawDBError::Storage(format!(
                "segmented lexical projection is not qualified for the current production release: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn json(&self) -> serde_json::Value {
        let recomputed_blockers = self.recompute_blocker_codes(&self.projection_identity);
        serde_json::json!({
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "projection_generation": self.projection_generation,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "document_count": self.document_count,
            "projection_identity": self.projection_identity.json(),
            "evidence_binding": self.evidence_binding.as_ref().map(crate::ProductionEvidenceBinding::json),
            "expected_identity": self.expected_identity.as_ref().map(crate::ProductionQualificationIdentity::json),
            "topk_score_parity": self.topk_score_parity.json(),
            "exact_topk_score_parity": self.exact_topk_score_parity,
            "coverage": self.coverage.json(),
            "metrics": self.metrics.json(),
            "thresholds": {
                "minimum_document_count": MINIMUM_DOCUMENT_COUNT,
                "minimum_selective_p95_improvement_per_million": MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION,
                "maximum_rss_budget_per_million": MAX_RSS_BUDGET_PER_MILLION,
                "maximum_write_regression_per_million": MAX_WRITE_REGRESSION_PER_MILLION,
            },
            "blocker_codes": recomputed_blockers,
            "ready": recomputed_blockers.is_empty(),
        })
    }

    fn recompute_blocker_codes(
        &self,
        projection_identity: &SearchProjectionQualificationIdentity,
    ) -> Vec<String> {
        let mut blockers = Vec::new();
        let document_count = projection_identity.document_count;
        if self.protocol != SEARCH_LEXICAL_QUALIFICATION_PROTOCOL
            || self.protocol_version != SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION
        {
            blockers.push("protocol_mismatch".to_string());
        }
        if !self.projection_identity.complete()
            || self.projection_identity != *projection_identity
            || self.projection_generation != projection_identity.projection_generation
            || self.source_graph_commit_epoch != projection_identity.source_graph_commit_epoch
            || self.document_count != projection_identity.document_count
        {
            blockers.push("projection_identity_mismatch".to_string());
        }
        match (&self.evidence_binding, &self.expected_identity) {
            (Some(binding), Some(expected)) => {
                blockers.extend(production_evidence_blocker_codes(binding, expected));
                if self.projection_identity.source_graph_commit_epoch
                    != Some(expected.canonical_graph_commit_epoch)
                {
                    blockers.push("source_graph_epoch_release_identity_mismatch".to_string());
                }
            }
            (None, _) => blockers.push("production_evidence_binding_missing".to_string()),
            (_, None) => blockers.push("production_expected_identity_missing".to_string()),
        }
        if document_count < MINIMUM_DOCUMENT_COUNT {
            blockers.push("dataset_too_small".to_string());
        }
        if !self.exact_topk_score_parity || !self.topk_score_parity.complete() {
            blockers.push("topk_score_parity_failed".to_string());
        }
        let acl_required = self.expected_identity.as_ref().is_some_and(|identity| {
            identity
                .enabled_features
                .iter()
                .any(|feature| feature == "acl")
        });
        if !self.coverage.complete(acl_required) {
            blockers.push("workload_coverage_incomplete".to_string());
        }
        if self.metrics.storage_memory_budget_bytes == 0
            || self.metrics.canonical_dataset_bytes <= self.metrics.storage_memory_budget_bytes
        {
            blockers.push("larger_than_memory_not_proven".to_string());
        }
        if !ratio_within(
            self.metrics.steady_resident_bytes,
            self.metrics.storage_memory_budget_bytes,
            MAX_RSS_BUDGET_PER_MILLION,
        ) || !ratio_within(
            self.metrics.peak_resident_bytes,
            self.metrics.storage_memory_budget_bytes,
            MAX_RSS_BUDGET_PER_MILLION,
        ) {
            blockers.push("resident_memory_budget_exceeded".to_string());
        }
        if self.metrics.baseline_selective_text_p95_micros == 0
            || self.metrics.segmented_selective_text_p95_micros == 0
            || !ratio_within(
                self.metrics.segmented_selective_text_p95_micros,
                self.metrics.baseline_selective_text_p95_micros,
                MIN_SELECTIVE_P95_IMPROVEMENT_PER_MILLION,
            )
        {
            blockers.push("selective_text_p95_improvement_insufficient".to_string());
        }
        if self.metrics.selective_posting_bytes_read == 0
            || self.metrics.selective_candidate_postings_visited == 0
            || self.metrics.selective_candidate_postings_visited >= document_count as u64
            || self.metrics.selective_matching_document_count >= document_count
        {
            blockers.push("selective_work_not_posting_proportional".to_string());
        }
        if !self.metrics.process_memory_capabilities.resident_memory
            || !self.metrics.process_memory_capabilities.total_page_faults
            || self.metrics.total_page_faults.is_none()
            || (self.metrics.process_memory_capabilities.split_page_faults
                && (self.metrics.minor_page_faults.is_none()
                    || self.metrics.major_page_faults.is_none()))
            || (!self.metrics.process_memory_capabilities.split_page_faults
                && (self.metrics.minor_page_faults.is_some()
                    || self.metrics.major_page_faults.is_some()))
        {
            blockers.push("process_memory_metrics_invalid".to_string());
        }
        if self.metrics.metadata_sidecar_bytes_read == 0
            || self.metrics.vector_sidecar_bytes_read == 0
            || self.metrics.hydration_bytes == 0
        {
            blockers.push("out_of_core_io_metrics_missing".to_string());
        }
        if [
            self.metrics.segmented_vector_p50_micros,
            self.metrics.segmented_vector_p95_micros,
            self.metrics.segmented_vector_p99_micros,
            self.metrics.segmented_hybrid_p50_micros,
            self.metrics.segmented_hybrid_p95_micros,
            self.metrics.segmented_hybrid_p99_micros,
        ]
        .contains(&0)
        {
            blockers.push("vector_hybrid_latency_metrics_missing".to_string());
        }
        if !regression_within(
            self.metrics.segmented_update_p95_micros,
            self.metrics.baseline_update_p95_micros,
        ) {
            blockers.push("update_p95_regression_exceeded".to_string());
        }
        if !regression_within(
            self.metrics.segmented_checkpoint_p95_micros,
            self.metrics.baseline_checkpoint_p95_micros,
        ) {
            blockers.push("checkpoint_p95_regression_exceeded".to_string());
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }

    fn recompute_blocker_codes_for_release(
        &self,
        projection_identity: &SearchProjectionQualificationIdentity,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Vec<String> {
        let mut blockers = self.recompute_blocker_codes(projection_identity);
        if self.expected_identity.as_ref() != Some(expected_identity) {
            blockers.push("current_release_identity_mismatch".to_string());
        }
        match &self.evidence_binding {
            Some(binding) => blockers.extend(production_evidence_blocker_codes(
                binding,
                expected_identity,
            )),
            None => blockers.push("production_evidence_binding_missing".to_string()),
        }
        if projection_identity.source_graph_commit_epoch
            != Some(expected_identity.canonical_graph_commit_epoch)
        {
            blockers.push("source_graph_epoch_current_release_mismatch".to_string());
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }
}

fn regression_within(measured: u64, baseline: u64) -> bool {
    baseline > 0
        && measured > 0
        && ratio_within(measured, baseline, MAX_WRITE_REGRESSION_PER_MILLION)
}

fn ratio_within(measured: u64, baseline: u64, limit_per_million: u64) -> bool {
    baseline > 0
        && u128::from(measured).saturating_mul(1_000_000)
            <= u128::from(baseline).saturating_mul(u128::from(limit_per_million))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_coverage() -> SearchLexicalFeasibilityCoverage {
        SearchLexicalFeasibilityCoverage {
            selective_identifier: true,
            cjk_text: true,
            common_term: true,
            no_hit: true,
            metadata_filter: true,
            acl_filter: true,
            hybrid_rrf: true,
            bounded_generation_update: true,
            bounded_rabitq_serving: true,
            incremental_upsert_delete: true,
            checkpoint_reopen: true,
            corrupt_artifact: true,
            stale_manifest: true,
            mixed_foreground_background: true,
            larger_than_memory: true,
        }
    }

    fn passing_metrics() -> SearchLexicalFeasibilityMetrics {
        SearchLexicalFeasibilityMetrics {
            canonical_dataset_bytes: 2_000_000_000,
            storage_memory_budget_bytes: 1_000_000_000,
            steady_resident_bytes: 900_000_000,
            peak_resident_bytes: 1_050_000_000,
            baseline_selective_text_p50_micros: 100,
            baseline_selective_text_p95_micros: 200,
            baseline_selective_text_p99_micros: 300,
            segmented_selective_text_p50_micros: 40,
            segmented_selective_text_p95_micros: 100,
            segmented_selective_text_p99_micros: 140,
            baseline_throughput_per_second: 1_000,
            segmented_throughput_per_second: 2_000,
            selective_posting_bytes_read: 4096,
            selective_candidate_postings_visited: 32,
            selective_matching_document_count: 16,
            process_memory_capabilities: hawdb_qos::ProcessMemoryCapabilities {
                resident_memory: true,
                total_page_faults: true,
                split_page_faults: true,
            },
            total_page_faults: Some(20),
            minor_page_faults: Some(20),
            major_page_faults: Some(0),
            metadata_sidecar_bytes_read: 2048,
            vector_sidecar_bytes_read: 4096,
            hydration_bytes: 1024,
            segmented_vector_p50_micros: 50,
            segmented_vector_p95_micros: 110,
            segmented_vector_p99_micros: 160,
            segmented_hybrid_p50_micros: 60,
            segmented_hybrid_p95_micros: 120,
            segmented_hybrid_p99_micros: 180,
            baseline_update_p95_micros: 100,
            segmented_update_p95_micros: 110,
            baseline_checkpoint_p95_micros: 1_000,
            segmented_checkpoint_p95_micros: 1_100,
            consolidation_write_amplification_per_million: 1_100_000,
            recovery_p95_micros: 20_000,
        }
    }

    fn projection_identity(
        projection_generation: u64,
        source_graph_commit_epoch: Option<u64>,
        document_count: usize,
    ) -> SearchProjectionQualificationIdentity {
        SearchProjectionQualificationIdentity {
            projection_generation,
            source_graph_commit_epoch,
            document_count,
            documents_digest: 11,
            analyzer_digest: 12,
            embedding_model: Some("test-embedding".to_string()),
            embedding_version: Some("1".to_string()),
            embedding_dimension: Some(3),
        }
    }

    fn production_identity() -> crate::ProductionQualificationIdentity {
        crate::ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: vec!["full-text-search".to_string(), "vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: 42,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn complete_parity() -> SearchTopKScoreParity {
        SearchTopKScoreParity {
            text: true,
            vector: true,
            hybrid: true,
        }
    }

    #[test]
    fn accepts_complete_generation_bound_production_evidence() {
        let identity = production_identity();
        let projection_identity = projection_identity(7, Some(42), 100_000);
        let report = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity.clone(),
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            complete_parity(),
            complete_coverage(),
            passing_metrics(),
        );

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        report
            .validate_for_projection(&projection_identity)
            .unwrap();
        report
            .validate_for_projection_and_release(&projection_identity, &production_identity())
            .unwrap();
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn requires_acl_coverage_only_when_acl_is_in_the_release_feature_set() {
        let mut coverage = complete_coverage();
        coverage.acl_filter = false;
        let identity = production_identity();
        let without_acl = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity(7, Some(42), 100_000),
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            complete_parity(),
            coverage.clone(),
            passing_metrics(),
        );
        assert!(without_acl.ready);

        let mut identity = production_identity();
        identity.enabled_features.push("acl".to_string());
        let with_acl = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity(7, Some(42), 100_000),
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            complete_parity(),
            coverage,
            passing_metrics(),
        );
        assert!(with_acl
            .blocker_codes
            .contains(&"workload_coverage_incomplete".to_string()));
    }

    #[test]
    fn rejects_evidence_bound_to_a_different_current_dataset() {
        let identity = production_identity();
        let projection_identity = projection_identity(7, Some(42), 100_000);
        let report = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity.clone(),
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            complete_parity(),
            complete_coverage(),
            passing_metrics(),
        );
        let mut current_identity = production_identity();
        current_identity.dataset_fingerprint = "different-dataset".to_string();

        let error = report
            .validate_for_projection_and_release(&projection_identity, &current_identity)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("current_release_identity_mismatch"));
        assert!(error.to_string().contains("evidence_identity_mismatch"));
    }

    #[test]
    fn rejects_forged_ready_flag_and_stale_projection_identity() {
        let mut metrics = passing_metrics();
        metrics.segmented_selective_text_p95_micros = 101;
        let identity = production_identity();
        let mut report = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity(7, Some(42), 100_000),
            crate::ProductionEvidenceBinding {
                identity: identity.clone(),
                generated_at_unix_seconds: 1,
            },
            identity,
            complete_parity(),
            complete_coverage(),
            metrics,
        );
        report.ready = true;
        report.blocker_codes.clear();

        let error = report
            .validate_for_projection(&projection_identity(8, Some(43), 100_000))
            .unwrap_err();
        assert!(error.to_string().contains("projection_identity_mismatch"));
        assert!(error
            .to_string()
            .contains("selective_text_p95_improvement_insufficient"));
        assert_eq!(report.json()["ready"], false);
    }

    #[test]
    fn rejects_small_or_memory_resident_benchmark_fixtures() {
        let mut metrics = passing_metrics();
        metrics.canonical_dataset_bytes = metrics.storage_memory_budget_bytes;
        let report = SearchLexicalProductionQualificationReport::evaluate(
            1,
            None,
            99_999,
            true,
            complete_coverage(),
            metrics,
        );

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"dataset_too_small".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"larger_than_memory_not_proven".to_string()));
    }
}
