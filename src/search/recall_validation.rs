use super::{SearchRetrieverReport, TURBOQUANT_CANDIDATE_BACKEND};
use crate::production_evidence::production_evidence_blocker_codes;
use crate::{Result, SkeinError};
use std::collections::BTreeSet;

pub const VECTOR_RECALL_VALIDATION_PROTOCOL: &str = "skein-vector-recall-validation-v1";
pub const VECTOR_RECALL_PRODUCTION_QUALIFICATION_PROTOCOL: &str =
    "skein-vector-recall-production-qualification-v1";
pub const MAX_VECTOR_RECALL_VALIDATION_SAMPLES: usize = 128;
pub const MAX_VECTOR_RECALL_VALIDATION_TOP_K: usize = 100;
pub const MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT: usize = 1_000;
pub const MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT: usize = 100_000;
const PER_MILLION: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecallValidationOptions {
    pub max_samples: usize,
    pub top_k: usize,
    pub candidate_limit: usize,
    pub minimum_recall_per_million: u32,
    pub metadata_filters: std::collections::BTreeMap<String, String>,
}

impl Default for VectorRecallValidationOptions {
    fn default() -> Self {
        Self {
            max_samples: 32,
            top_k: 10,
            candidate_limit: 40,
            minimum_recall_per_million: 950_000,
            metadata_filters: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum VectorRecallValidationBlocker {
    NoSamplesRequested,
    TopKZero,
    CandidateLimitBelowTopK,
    MetadataFilterInvalid,
    NoEligibleVectors,
    GroundTruthEmpty,
    ApproximateBackendUnavailable,
    ApproximateFallbackObserved,
    IndexCoverageIncomplete,
    CandidateRecallBelowThreshold,
    RecallBelowThreshold,
}

impl VectorRecallValidationBlocker {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoSamplesRequested => "no_samples_requested",
            Self::TopKZero => "top_k_zero",
            Self::CandidateLimitBelowTopK => "candidate_limit_below_top_k",
            Self::MetadataFilterInvalid => "metadata_filter_invalid",
            Self::NoEligibleVectors => "no_eligible_vectors",
            Self::GroundTruthEmpty => "ground_truth_empty",
            Self::ApproximateBackendUnavailable => "approximate_backend_unavailable",
            Self::ApproximateFallbackObserved => "approximate_fallback_observed",
            Self::IndexCoverageIncomplete => "index_coverage_incomplete",
            Self::CandidateRecallBelowThreshold => "candidate_recall_below_threshold",
            Self::RecallBelowThreshold => "recall_below_threshold",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecallValidationReport {
    pub protocol: String,
    pub ready: bool,
    pub approximate_backend: String,
    pub sample_candidate_count: usize,
    pub requested_sample_count: usize,
    pub executed_sample_count: usize,
    pub top_k: usize,
    pub candidate_limit: usize,
    pub minimum_recall_per_million: u32,
    pub exact_hit_count: usize,
    pub candidate_hit_count: usize,
    pub candidate_overlap_count: usize,
    pub candidate_recall_at_k_per_million: u32,
    pub approximate_hit_count: usize,
    pub overlap_count: usize,
    pub recall_at_k_per_million: u32,
    pub overlap_at_k_per_million: u32,
    pub fallback_count: usize,
    pub index_coverage_incomplete_count: usize,
    pub average_filter_selectivity_per_million: u32,
    pub max_filter_selectivity_per_million: u32,
    pub blocker_codes: Vec<VectorRecallValidationBlocker>,
}

impl VectorRecallValidationReport {
    pub fn validates_required_approximate_backend(&self) -> bool {
        self.protocol == VECTOR_RECALL_VALIDATION_PROTOCOL
            && self.ready
            && self.approximate_backend == TURBOQUANT_CANDIDATE_BACKEND
            && self.requested_sample_count > 0
            && self.executed_sample_count == self.requested_sample_count
            && self.exact_hit_count > 0
            && self.candidate_recall_at_k_per_million >= self.minimum_recall_per_million
            && self.fallback_count == 0
            && self.index_coverage_incomplete_count == 0
            && self.recall_at_k_per_million >= self.minimum_recall_per_million
            && self.blocker_codes.is_empty()
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "approximate_backend": self.approximate_backend,
            "sample_candidate_count": self.sample_candidate_count,
            "requested_sample_count": self.requested_sample_count,
            "executed_sample_count": self.executed_sample_count,
            "top_k": self.top_k,
            "candidate_limit": self.candidate_limit,
            "minimum_recall_per_million": self.minimum_recall_per_million,
            "exact_hit_count": self.exact_hit_count,
            "candidate_hit_count": self.candidate_hit_count,
            "candidate_overlap_count": self.candidate_overlap_count,
            "candidate_recall_at_k_per_million": self.candidate_recall_at_k_per_million,
            "approximate_hit_count": self.approximate_hit_count,
            "overlap_count": self.overlap_count,
            "recall_at_k_per_million": self.recall_at_k_per_million,
            "overlap_at_k_per_million": self.overlap_at_k_per_million,
            "fallback_count": self.fallback_count,
            "index_coverage_incomplete_count": self.index_coverage_incomplete_count,
            "average_filter_selectivity_per_million": self.average_filter_selectivity_per_million,
            "max_filter_selectivity_per_million": self.max_filter_selectivity_per_million,
            "blocker_codes": self.blocker_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorProjectionQualificationIdentity {
    pub projection_generation: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub document_count: usize,
    pub source_digest: u64,
    pub payload_bytes: u64,
    pub payload_checksum: u32,
    pub format_version: u32,
    pub algorithm: String,
    pub bit_width: u8,
    pub dimension: usize,
    pub transform_seed: u64,
    pub embedding_model: Option<String>,
    pub embedding_version: Option<String>,
    pub file_backed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorProjectionResourceEvidence {
    pub segment_count: usize,
    pub requested_segment_rows: usize,
    pub admitted_segment_rows: usize,
    pub configured_build_working_bytes: usize,
    pub peak_build_working_bytes: usize,
    pub raw_vector_bytes: u64,
    pub projection_payload_bytes: u64,
    pub build_write_amplification_per_million: u64,
}

impl VectorProjectionResourceEvidence {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "segment_count": self.segment_count,
            "requested_segment_rows": self.requested_segment_rows,
            "admitted_segment_rows": self.admitted_segment_rows,
            "configured_build_working_bytes": self.configured_build_working_bytes,
            "peak_build_working_bytes": self.peak_build_working_bytes,
            "raw_vector_bytes": self.raw_vector_bytes,
            "projection_payload_bytes": self.projection_payload_bytes,
            "build_write_amplification_per_million": self.build_write_amplification_per_million,
        })
    }
}

impl VectorProjectionQualificationIdentity {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "projection_generation": self.projection_generation,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "document_count": self.document_count,
            "source_digest": self.source_digest,
            "payload_bytes": self.payload_bytes,
            "payload_checksum": self.payload_checksum,
            "format_version": self.format_version,
            "algorithm": self.algorithm,
            "bit_width": self.bit_width,
            "dimension": self.dimension,
            "transform_seed": self.transform_seed,
            "embedding_model": self.embedding_model,
            "embedding_version": self.embedding_version,
            "file_backed": self.file_backed,
        })
    }

    fn complete(&self) -> bool {
        self.projection_generation > 0
            && self.source_graph_commit_epoch.is_some()
            && self.document_count > 0
            && self.source_digest > 0
            && self.payload_bytes > 0
            && self.format_version > 0
            && self.algorithm == "turboquant"
            && self.bit_width == 4
            && self.dimension > 0
            && self
                .embedding_model
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .embedding_version
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self.file_backed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecallProductionQualificationReport {
    pub protocol: String,
    pub recall: VectorRecallValidationReport,
    pub projection_identity: VectorProjectionQualificationIdentity,
    pub evidence_binding: crate::ProductionEvidenceBinding,
    pub expected_identity: crate::ProductionQualificationIdentity,
    pub blocker_codes: Vec<String>,
    pub ready: bool,
}

impl VectorRecallProductionQualificationReport {
    pub fn evaluate(
        recall: VectorRecallValidationReport,
        projection_identity: VectorProjectionQualificationIdentity,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Self {
        let mut report = Self {
            protocol: VECTOR_RECALL_PRODUCTION_QUALIFICATION_PROTOCOL.to_string(),
            recall,
            projection_identity,
            evidence_binding,
            expected_identity,
            blocker_codes: Vec::new(),
            ready: false,
        };
        report.blocker_codes =
            report.recompute_blocker_codes(&report.projection_identity, &report.expected_identity);
        report.ready = report.blocker_codes.is_empty();
        report
    }

    pub fn validate_for(
        &self,
        projection_identity: &VectorProjectionQualificationIdentity,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Result<()> {
        let blockers = self.recompute_blocker_codes(projection_identity, expected_identity);
        if blockers.is_empty() {
            Ok(())
        } else {
            Err(SkeinError::Storage(format!(
                "TurboQuant projection is not qualified for the current production release: {}",
                blockers.join(",")
            )))
        }
    }

    pub fn json(&self) -> serde_json::Value {
        let blockers =
            self.recompute_blocker_codes(&self.projection_identity, &self.expected_identity);
        serde_json::json!({
            "protocol": self.protocol,
            "recall": self.recall.json(),
            "projection_identity": self.projection_identity.json(),
            "evidence_binding": self.evidence_binding.json(),
            "expected_identity": self.expected_identity.json(),
            "minimum_document_count": MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT,
            "blocker_codes": blockers,
            "ready": blockers.is_empty(),
        })
    }

    fn recompute_blocker_codes(
        &self,
        projection_identity: &VectorProjectionQualificationIdentity,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Vec<String> {
        let mut blockers = Vec::new();
        if self.protocol != VECTOR_RECALL_PRODUCTION_QUALIFICATION_PROTOCOL {
            blockers.push("protocol_mismatch".to_string());
        }
        if !self.recall.validates_required_approximate_backend() {
            blockers.push("recall_validation_failed".to_string());
        }
        if !self.projection_identity.complete() || self.projection_identity != *projection_identity
        {
            blockers.push("projection_identity_mismatch".to_string());
        }
        if projection_identity.document_count < MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT {
            blockers.push("dataset_too_small".to_string());
        }
        if self.expected_identity != *expected_identity {
            blockers.push("current_release_identity_mismatch".to_string());
        }
        blockers.extend(production_evidence_blocker_codes(
            &self.evidence_binding,
            expected_identity,
        ));
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

pub(super) struct VectorRecallValidationAccumulator {
    sample_candidate_count: usize,
    requested_sample_count: usize,
    top_k: usize,
    candidate_limit: usize,
    minimum_recall_per_million: u32,
    executed_sample_count: usize,
    exact_hit_count: usize,
    candidate_hit_count: usize,
    candidate_overlap_count: usize,
    approximate_hit_count: usize,
    overlap_count: usize,
    fallback_count: usize,
    approximate_backend_count: usize,
    index_coverage_incomplete_count: usize,
    filter_selectivity_sum: u64,
    max_filter_selectivity_per_million: u32,
    metadata_filter_valid: bool,
}

impl VectorRecallValidationAccumulator {
    pub(super) fn new(
        sample_candidate_count: usize,
        options: &VectorRecallValidationOptions,
    ) -> Self {
        Self {
            sample_candidate_count,
            requested_sample_count: options
                .max_samples
                .min(MAX_VECTOR_RECALL_VALIDATION_SAMPLES)
                .min(sample_candidate_count),
            top_k: options.top_k.min(MAX_VECTOR_RECALL_VALIDATION_TOP_K),
            candidate_limit: options
                .candidate_limit
                .min(MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT),
            minimum_recall_per_million: options.minimum_recall_per_million.min(PER_MILLION as u32),
            executed_sample_count: 0,
            exact_hit_count: 0,
            candidate_hit_count: 0,
            candidate_overlap_count: 0,
            approximate_hit_count: 0,
            overlap_count: 0,
            fallback_count: 0,
            approximate_backend_count: 0,
            index_coverage_incomplete_count: 0,
            filter_selectivity_sum: 0,
            max_filter_selectivity_per_million: 0,
            metadata_filter_valid: true,
        }
    }

    pub(super) fn requested_sample_count(&self) -> usize {
        self.requested_sample_count
    }

    pub(super) fn top_k(&self) -> usize {
        self.top_k
    }

    pub(super) fn candidate_limit(&self) -> usize {
        self.candidate_limit.max(self.top_k)
    }

    pub(super) fn mark_metadata_filter_invalid(&mut self) {
        self.metadata_filter_valid = false;
    }

    pub(super) fn record(
        &mut self,
        exact_ids: &[String],
        candidate_ids: &[String],
        approximate_ids: &[String],
        approximate_retriever: &SearchRetrieverReport,
    ) {
        self.executed_sample_count = self.executed_sample_count.saturating_add(1);
        self.exact_hit_count = self.exact_hit_count.saturating_add(exact_ids.len());
        self.candidate_hit_count = self.candidate_hit_count.saturating_add(candidate_ids.len());
        self.approximate_hit_count = self
            .approximate_hit_count
            .saturating_add(approximate_ids.len());
        let exact_ids = exact_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        self.candidate_overlap_count = self.candidate_overlap_count.saturating_add(
            candidate_ids
                .iter()
                .filter(|id| exact_ids.contains(id.as_str()))
                .count(),
        );
        self.overlap_count = self.overlap_count.saturating_add(
            approximate_ids
                .iter()
                .filter(|id| exact_ids.contains(id.as_str()))
                .count(),
        );
        if approximate_retriever.backend == TURBOQUANT_CANDIDATE_BACKEND {
            self.approximate_backend_count = self.approximate_backend_count.saturating_add(1);
        }
        if !approximate_retriever.fallback_reason_codes.is_empty() {
            self.fallback_count = self.fallback_count.saturating_add(1);
        }
        if !approximate_retriever.index_coverage_complete {
            self.index_coverage_incomplete_count =
                self.index_coverage_incomplete_count.saturating_add(1);
        }
        let selectivity = approximate_retriever
            .filter_selectivity_per_million
            .unwrap_or(0);
        self.filter_selectivity_sum = self
            .filter_selectivity_sum
            .saturating_add(u64::from(selectivity));
        self.max_filter_selectivity_per_million =
            self.max_filter_selectivity_per_million.max(selectivity);
    }

    pub(super) fn finish(self) -> VectorRecallValidationReport {
        let recall_at_k_per_million = ratio_per_million(self.overlap_count, self.exact_hit_count);
        let candidate_recall_at_k_per_million =
            ratio_per_million(self.candidate_overlap_count, self.exact_hit_count);
        let overlap_denominator = self.executed_sample_count.saturating_mul(self.top_k);
        let overlap_at_k_per_million = ratio_per_million(self.overlap_count, overlap_denominator);
        let average_filter_selectivity_per_million =
            average_per_million(self.filter_selectivity_sum, self.executed_sample_count);
        let mut blocker_codes = BTreeSet::new();
        if self.requested_sample_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::NoSamplesRequested);
        }
        if self.top_k == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::TopKZero);
        }
        if self.candidate_limit < self.top_k {
            blocker_codes.insert(VectorRecallValidationBlocker::CandidateLimitBelowTopK);
        }
        if !self.metadata_filter_valid {
            blocker_codes.insert(VectorRecallValidationBlocker::MetadataFilterInvalid);
        }
        if self.sample_candidate_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::NoEligibleVectors);
        }
        if self.exact_hit_count == 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::GroundTruthEmpty);
        }
        if self.approximate_backend_count != self.executed_sample_count {
            blocker_codes.insert(VectorRecallValidationBlocker::ApproximateBackendUnavailable);
        }
        if self.fallback_count > 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::ApproximateFallbackObserved);
        }
        if self.index_coverage_incomplete_count > 0 {
            blocker_codes.insert(VectorRecallValidationBlocker::IndexCoverageIncomplete);
        }
        if self.exact_hit_count > 0
            && candidate_recall_at_k_per_million < self.minimum_recall_per_million
        {
            blocker_codes.insert(VectorRecallValidationBlocker::CandidateRecallBelowThreshold);
        }
        if self.exact_hit_count > 0 && recall_at_k_per_million < self.minimum_recall_per_million {
            blocker_codes.insert(VectorRecallValidationBlocker::RecallBelowThreshold);
        }
        let blocker_codes = blocker_codes.into_iter().collect::<Vec<_>>();
        VectorRecallValidationReport {
            protocol: VECTOR_RECALL_VALIDATION_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            approximate_backend: TURBOQUANT_CANDIDATE_BACKEND.to_string(),
            sample_candidate_count: self.sample_candidate_count,
            requested_sample_count: self.requested_sample_count,
            executed_sample_count: self.executed_sample_count,
            top_k: self.top_k,
            candidate_limit: self.candidate_limit,
            minimum_recall_per_million: self.minimum_recall_per_million,
            exact_hit_count: self.exact_hit_count,
            candidate_hit_count: self.candidate_hit_count,
            candidate_overlap_count: self.candidate_overlap_count,
            candidate_recall_at_k_per_million,
            approximate_hit_count: self.approximate_hit_count,
            overlap_count: self.overlap_count,
            recall_at_k_per_million,
            overlap_at_k_per_million,
            fallback_count: self.fallback_count,
            index_coverage_incomplete_count: self.index_coverage_incomplete_count,
            average_filter_selectivity_per_million,
            max_filter_selectivity_per_million: self.max_filter_selectivity_per_million,
            blocker_codes,
        }
    }
}

pub(super) fn sample_positions(candidate_count: usize, sample_count: usize) -> Vec<usize> {
    if candidate_count == 0 || sample_count == 0 {
        return Vec::new();
    }
    let sample_count = sample_count.min(candidate_count);
    if sample_count == 1 {
        return vec![0];
    }
    (0..sample_count)
        .map(|sample| {
            sample.saturating_mul(candidate_count.saturating_sub(1))
                / sample_count.saturating_sub(1)
        })
        .collect()
}

fn ratio_per_million(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    u32::try_from(
        u64::try_from(numerator)
            .unwrap_or(u64::MAX)
            .saturating_mul(PER_MILLION)
            / u64::try_from(denominator).unwrap_or(u64::MAX),
    )
    .unwrap_or(u32::MAX)
}

fn average_per_million(sum: u64, count: usize) -> u32 {
    if count == 0 {
        return 0;
    }
    u32::try_from(sum / u64::try_from(count).unwrap_or(u64::MAX)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::{
        SearchCandidateSetReport, SearchPredicatePushdownReport, SearchRetrieverCandidateSetReport,
    };

    fn retriever(backend: &str) -> SearchRetrieverReport {
        SearchRetrieverReport {
            name: "vector".to_string(),
            backend: backend.to_string(),
            backend_selection_reason: None,
            estimated_raw_vector_bytes: None,
            filter_selectivity_per_million: Some(500_000),
            available: true,
            input_candidate_set: SearchCandidateSetReport {
                id_space: String::new(),
                representation: String::new(),
                cardinality: 0,
                exact: true,
                snapshot_source_graph_commit_epoch: None,
                policy_epoch: None,
                filtered_out_count: 0,
                metadata_filters: Default::default(),
                metadata_predicate_pushdown: SearchPredicatePushdownReport::default(),
            },
            candidate_score_source: String::new(),
            final_score_source: String::new(),
            generated_candidate_count: 0,
            candidate_scan_rounds: 0,
            descriptor_pruned_count: 0,
            scalar_filtered_count: 0,
            residual_filtered_count: 0,
            reranked_candidate_count: 0,
            raw_vector_bytes_read: 0,
            candidate_scan_kernel: None,
            candidate_scan_worker_count: 0,
            candidate_scan_segment_count: 0,
            candidate_scan_scanned_segment_count: 0,
            candidate_scan_scored_document_count: 0,
            candidate_scan_filtered_document_count: 0,
            candidate_scan_scanned_block_count: 0,
            candidate_scan_skipped_block_count: 0,
            candidate_scan_payload_bytes_read: 0,
            candidate_scan_admitted_working_bytes: 0,
            posting_bytes_read: 0,
            candidate_postings_visited: 0,
            segmented_lexical_projection_used: false,
            index_covered_document_count: 1,
            index_candidate_document_count: 1,
            index_coverage_complete: true,
            candidate_count: 0,
            candidate_set: SearchRetrieverCandidateSetReport {
                id_space: String::new(),
                representation: String::new(),
                cardinality: 0,
                exact: true,
                snapshot_source_graph_commit_epoch: None,
                policy_epoch: None,
            },
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
            candidate_top_ids: Vec::new(),
            top_hit_ids: Vec::new(),
            top_candidates: Vec::new(),
        }
    }

    fn ready_recall_report() -> VectorRecallValidationReport {
        let options = VectorRecallValidationOptions {
            max_samples: 1,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: Default::default(),
        };
        let mut accumulator = VectorRecallValidationAccumulator::new(1, &options);
        accumulator.record(
            &["a".to_string()],
            &["a".to_string()],
            &["a".to_string()],
            &retriever(TURBOQUANT_CANDIDATE_BACKEND),
        );
        accumulator.finish()
    }

    fn projection_identity() -> VectorProjectionQualificationIdentity {
        VectorProjectionQualificationIdentity {
            projection_generation: 7,
            source_graph_commit_epoch: Some(42),
            document_count: MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT,
            source_digest: 11,
            payload_bytes: 4096,
            payload_checksum: 12,
            format_version: 1,
            algorithm: "turboquant".to_string(),
            bit_width: 4,
            dimension: 3,
            transform_seed: 17,
            embedding_model: Some("test-embedding".to_string()),
            embedding_version: Some("1".to_string()),
            file_backed: true,
        }
    }

    fn production_identity() -> crate::ProductionQualificationIdentity {
        crate::ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: vec!["vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: 42,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    #[test]
    fn accumulator_reports_recall_and_overlap_without_copying_ids() {
        let options = VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 2,
            candidate_limit: 2,
            minimum_recall_per_million: 500_000,
            metadata_filters: Default::default(),
        };
        let mut accumulator = VectorRecallValidationAccumulator::new(2, &options);
        accumulator.record(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "x".to_string()],
            &["a".to_string(), "x".to_string()],
            &retriever(TURBOQUANT_CANDIDATE_BACKEND),
        );
        accumulator.record(
            &["c".to_string(), "d".to_string()],
            &["c".to_string(), "d".to_string()],
            &["c".to_string(), "d".to_string()],
            &retriever(TURBOQUANT_CANDIDATE_BACKEND),
        );

        let report = accumulator.finish();

        assert!(report.ready);
        assert!(report.validates_required_approximate_backend());
        assert_eq!(report.candidate_hit_count, 4);
        assert_eq!(report.candidate_overlap_count, 3);
        assert_eq!(report.candidate_recall_at_k_per_million, 750_000);
        assert_eq!(report.recall_at_k_per_million, 750_000);
        assert_eq!(report.overlap_at_k_per_million, 750_000);
        assert_eq!(report.average_filter_selectivity_per_million, 500_000);
        let json = report.json().to_string();
        assert!(!json.contains("\"a\""));
        assert!(!json.contains("\"d\""));
    }

    #[test]
    fn missing_approximate_backend_fails_closed() {
        let options = VectorRecallValidationOptions {
            max_samples: 1,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: Default::default(),
        };
        let mut accumulator = VectorRecallValidationAccumulator::new(1, &options);
        accumulator.record(
            &["a".to_string()],
            &[],
            &[],
            &retriever("compressed_vector_projection_required"),
        );

        let report = accumulator.finish();

        assert!(!report.ready);
        assert!(!report.validates_required_approximate_backend());
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::ApproximateBackendUnavailable));
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::RecallBelowThreshold));
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::CandidateRecallBelowThreshold));
    }

    #[test]
    fn candidate_limit_below_top_k_fails_closed() {
        let options = VectorRecallValidationOptions {
            max_samples: 1,
            top_k: 2,
            candidate_limit: 1,
            minimum_recall_per_million: 0,
            metadata_filters: Default::default(),
        };
        let report = VectorRecallValidationAccumulator::new(1, &options).finish();

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::CandidateLimitBelowTopK));
    }

    #[test]
    fn production_qualification_binds_current_release_and_projection_identity() {
        let expected = production_identity();
        let projection = projection_identity();
        let report = VectorRecallProductionQualificationReport::evaluate(
            ready_recall_report(),
            projection.clone(),
            crate::ProductionEvidenceBinding {
                identity: expected.clone(),
                generated_at_unix_seconds: 1,
            },
            expected.clone(),
        );

        assert!(
            report.ready,
            "unexpected blockers: {:?}",
            report.blocker_codes
        );
        report.validate_for(&projection, &expected).unwrap();
        assert_eq!(report.json()["ready"], true);

        let mut stale_projection = projection;
        stale_projection.projection_generation += 1;
        let error = report
            .validate_for(&stale_projection, &expected)
            .unwrap_err();
        assert!(error.to_string().contains("projection_identity_mismatch"));

        let mut other_dataset = expected;
        other_dataset.dataset_fingerprint = "other-dataset".to_string();
        let error = report
            .validate_for(&stale_projection, &other_dataset)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("current_release_identity_mismatch"));
        assert!(error.to_string().contains("evidence_identity_mismatch"));
    }

    #[test]
    fn sample_positions_are_bounded_and_spread() {
        assert_eq!(sample_positions(10, 3), vec![0, 4, 9]);
        assert_eq!(sample_positions(2, 10), vec![0, 1]);
        assert!(sample_positions(0, 3).is_empty());
    }
}
