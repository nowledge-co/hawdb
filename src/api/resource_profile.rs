use super::*;
use crate::production_evidence::production_evidence_blocker_codes;

pub const STORAGE_RESOURCE_PROFILE_PROTOCOL: &str = "skein-storage-resource-profile-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageResourceProfileLimits {
    pub min_canonical_artifact_bytes: u64,
    pub max_steady_resident_bytes: u64,
    pub max_peak_resident_bytes: u64,
    pub max_total_page_faults: Option<u64>,
    pub max_minor_page_faults: Option<u64>,
    pub max_major_page_faults: Option<u64>,
    pub max_intermediate_rows: usize,
    pub max_intermediate_payload_bytes: usize,
    pub max_output_rows: usize,
    pub max_output_payload_bytes: usize,
    pub require_fully_streamed: bool,
}

impl StorageResourceProfileLimits {
    fn validate(&self) -> Result<()> {
        let positive = [
            (
                "min_canonical_artifact_bytes",
                self.min_canonical_artifact_bytes,
            ),
            ("max_steady_resident_bytes", self.max_steady_resident_bytes),
            ("max_peak_resident_bytes", self.max_peak_resident_bytes),
            ("max_intermediate_rows", self.max_intermediate_rows as u64),
            (
                "max_intermediate_payload_bytes",
                self.max_intermediate_payload_bytes as u64,
            ),
            ("max_output_rows", self.max_output_rows as u64),
            (
                "max_output_payload_bytes",
                self.max_output_payload_bytes as u64,
            ),
        ];
        if let Some((name, _)) = positive.into_iter().find(|(_, value)| *value == 0) {
            return Err(SkeinError::Semantic(format!(
                "storage resource profile {name} must be greater than zero"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageResourceProfileReport {
    pub resource_ready: bool,
    pub blocker_codes: Vec<String>,
    pub evidence_binding: Option<crate::ProductionEvidenceBinding>,
    pub expected_identity: Option<crate::ProductionQualificationIdentity>,
    pub canonical_graph_commit_epoch: u64,
    pub limits: StorageResourceProfileLimits,
    pub durable: bool,
    pub before: crate::store::StorageResidencyReport,
    pub after: crate::store::StorageResidencyReport,
    pub query: QueryStreamReport,
}

impl StorageResourceProfileReport {
    pub fn json(&self) -> serde_json::Value {
        let production_blocker_codes = self.production_blocker_codes();
        let pipeline = &self.query.execution_profile.pipeline_memory_report;
        let evidence_binding = self
            .evidence_binding
            .as_ref()
            .map(crate::ProductionEvidenceBinding::json);
        let expected_identity = self
            .expected_identity
            .as_ref()
            .map(crate::ProductionQualificationIdentity::json);
        let identity_matches_expected = self
            .evidence_binding
            .as_ref()
            .zip(self.expected_identity.as_ref())
            .is_some_and(|(binding, expected)| {
                production_evidence_blocker_codes(binding, expected).is_empty()
            });
        let blocking_operator_memory_reports = self
            .query
            .execution_profile
            .blocking_operator_memory_reports
            .iter()
            .map(|report| {
                serde_json::json!({
                    "operator": report.operator,
                    "budget_bytes": report.budget_bytes,
                    "peak_tracked_bytes": report.peak_tracked_bytes,
                    "input_rows": report.input_rows,
                    "max_spill_bytes": report.max_spill_bytes,
                    "max_spill_runs": report.max_spill_runs,
                    "spilled_bytes": report.spilled_bytes,
                    "spill_run_count": report.spill_run_count,
                    "spilled_rows": report.spilled_rows,
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": STORAGE_RESOURCE_PROFILE_PROTOCOL,
            "protocol_version": 2,
            "present": true,
            "resource_ready": self.resource_ready,
            "ready": production_blocker_codes.is_empty(),
            "blocker_codes": production_blocker_codes,
            "evidence_binding": evidence_binding,
            "expected_identity": expected_identity,
            "canonical_graph_commit_epoch": self.canonical_graph_commit_epoch,
            "identity_matches_expected": identity_matches_expected,
            "limits": {
                "min_canonical_artifact_bytes": self.limits.min_canonical_artifact_bytes,
                "max_steady_resident_bytes": self.limits.max_steady_resident_bytes,
                "max_peak_resident_bytes": self.limits.max_peak_resident_bytes,
                "max_total_page_faults": self.limits.max_total_page_faults,
                "max_minor_page_faults": self.limits.max_minor_page_faults,
                "max_major_page_faults": self.limits.max_major_page_faults,
                "max_intermediate_rows": self.limits.max_intermediate_rows,
                "max_intermediate_payload_bytes": self.limits.max_intermediate_payload_bytes,
                "max_output_rows": self.limits.max_output_rows,
                "max_output_payload_bytes": self.limits.max_output_payload_bytes,
                "require_fully_streamed": self.limits.require_fully_streamed,
            },
            "storage": {
                "durable": self.durable,
                "out_of_core": self.after.out_of_core,
                "canonical_generation": self.after.canonical_generation,
                "canonical_artifact_bytes": self.after.canonical_artifact_bytes,
                "canonical_node_count": self.after.canonical_node_count,
                "canonical_relationship_count": self.after.canonical_relationship_count,
                "canonical_exceeds_cache": self.after.canonical_artifact_bytes
                    > self.after.segment_cache_capacity_bytes,
                "segment_cache_capacity_bytes": self.after.segment_cache_capacity_bytes,
                "segment_cache_resident_bytes_before": self.before.segment_cache_resident_bytes,
                "segment_cache_resident_bytes_after": self.after.segment_cache_resident_bytes,
                "segment_cache_miss_count_delta": self.after.segment_cache_miss_count
                    .saturating_sub(self.before.segment_cache_miss_count),
                "segment_cache_eviction_count_delta": self.after.segment_cache_eviction_count
                    .saturating_sub(self.before.segment_cache_eviction_count),
                "segment_cache_admission_rejection_count_delta": self
                    .after
                    .segment_cache_admission_rejection_count
                    .saturating_sub(self.before.segment_cache_admission_rejection_count),
                "segment_cache_digest_mismatch_count_delta": self
                    .after
                    .segment_cache_digest_mismatch_count
                    .saturating_sub(self.before.segment_cache_digest_mismatch_count),
                "estimated_delta_resident_bytes": self.after.estimated_delta_resident_bytes,
                "max_out_of_core_delta_bytes": self.after.max_out_of_core_delta_bytes,
                "delta_within_budget": self.after.delta_within_budget,
            },
            "execution": {
                "fully_streamed": self.query.fully_streamed,
                "output_rows": self.query.output_rows,
                "output_payload_bytes": self.query.output_payload_bytes,
                "intermediate_rows": pipeline.intermediate_rows,
                "columnar_batches": pipeline.columnar_batches,
                "columnar_input_rows": pipeline.columnar_input_rows,
                "columnar_selected_rows": pipeline.columnar_selected_rows,
                "morsel_count": pipeline.morsel_count,
                "morsel_max_admitted_workers": pipeline.morsel_max_admitted_workers,
                "morsel_peak_active_workers": pipeline.morsel_peak_active_workers,
                "intermediate_payload_bytes": pipeline.intermediate_payload_bytes,
                "peak_batch_rows": pipeline.peak_batch_rows,
                "peak_batch_payload_bytes": pipeline.peak_batch_payload_bytes,
                "start_resident_bytes": pipeline.start_resident_bytes,
                "start_peak_resident_bytes": pipeline.start_peak_resident_bytes,
                "steady_resident_bytes": pipeline.steady_resident_bytes,
                "peak_resident_bytes": pipeline.peak_resident_bytes,
                "steady_resident_growth_bytes": pipeline.steady_resident_growth_bytes,
                "lifetime_peak_resident_growth_bytes": pipeline.lifetime_peak_resident_growth_bytes,
                "total_page_faults": pipeline.total_page_faults,
                "minor_page_faults": pipeline.minor_page_faults,
                "major_page_faults": pipeline.major_page_faults,
                "metric_capabilities": {
                    "resident_memory": pipeline.steady_resident_bytes.is_some()
                        && pipeline.peak_resident_bytes.is_some(),
                    "total_page_faults": pipeline.total_page_faults.is_some(),
                    "split_page_faults": pipeline.minor_page_faults.is_some()
                        && pipeline.major_page_faults.is_some(),
                },
                "blocking_operator_kinds": self.query.execution_profile.blocking_operator_kinds,
                "blocking_operator_memory_reports": blocking_operator_memory_reports,
            },
        })
    }

    pub fn production_ready(&self) -> bool {
        self.production_blocker_codes().is_empty()
    }

    pub fn production_blocker_codes(&self) -> Vec<String> {
        let mut blockers = self.blocker_codes.clone();
        match (&self.evidence_binding, &self.expected_identity) {
            (Some(binding), Some(expected)) => {
                blockers.extend(production_evidence_blocker_codes(binding, expected));
                if binding.identity.canonical_graph_commit_epoch
                    != self.canonical_graph_commit_epoch
                {
                    blockers.push("evidence_canonical_graph_commit_epoch_mismatch".to_string());
                }
            }
            (None, _) => blockers.push("production_evidence_binding_missing".to_string()),
            (_, None) => blockers.push("production_expected_identity_missing".to_string()),
        }
        blockers.sort();
        blockers.dedup();
        blockers
    }
}

impl Database {
    pub fn storage_resource_profile(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
    ) -> Result<StorageResourceProfileReport> {
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            None,
            None,
            None,
        )
    }

    pub fn storage_resource_profile_for_production(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<StorageResourceProfileReport> {
        evidence_binding.validate_for(&expected_identity)?;
        let commit_epoch = self.commit_epoch();
        if evidence_binding.identity.canonical_graph_commit_epoch != commit_epoch {
            return Err(SkeinError::Semantic(format!(
                "production evidence canonical graph commit epoch {} does not match database epoch {commit_epoch}",
                evidence_binding.identity.canonical_graph_commit_epoch
            )));
        }
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            Some(evidence_binding),
            Some(expected_identity),
            None,
        )
    }

    pub(crate) fn storage_resource_profile_for_production_with_context(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<StorageResourceProfileReport> {
        evidence_binding.validate_for(&expected_identity)?;
        let commit_epoch = self.commit_epoch();
        if evidence_binding.identity.canonical_graph_commit_epoch != commit_epoch {
            return Err(SkeinError::Semantic(format!(
                "production evidence canonical graph commit epoch {} does not match database epoch {commit_epoch}",
                evidence_binding.identity.canonical_graph_commit_epoch
            )));
        }
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            Some(evidence_binding),
            Some(expected_identity),
            Some(task_context),
        )
    }

    fn storage_resource_profile_with_binding(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: Option<crate::ProductionEvidenceBinding>,
        expected_identity: Option<crate::ProductionQualificationIdentity>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<StorageResourceProfileReport> {
        limits.validate()?;
        let canonical_graph_commit_epoch = self.commit_epoch();
        let durable = self.storage_recovery_report().durable;
        let before = self.storage_residency_report();
        let mut read = self.begin_read_transaction();
        let stream_options = QueryStreamOptions {
            max_rows: Some(limits.max_output_rows),
            max_payload_bytes: Some(limits.max_output_payload_bytes),
        };
        let query = match task_context {
            Some(task_context) => read.query_with_params_streaming_context(
                cypher_text,
                parameters,
                stream_options,
                task_context,
                |_| Ok(()),
            ),
            None => read.query_with_params_streaming(
                cypher_text,
                parameters,
                stream_options,
                |_| Ok(()),
            ),
        }?;
        drop(read);
        let after = self.storage_residency_report();
        let pipeline = &query.execution_profile.pipeline_memory_report;
        let mut blocker_codes = Vec::new();

        if !durable {
            blocker_codes.push("database_not_durable".to_string());
        }
        if !after.out_of_core {
            blocker_codes.push("storage_not_out_of_core".to_string());
        }
        if after.canonical_artifact_bytes < limits.min_canonical_artifact_bytes {
            blocker_codes.push("canonical_artifact_below_required_size".to_string());
        }
        if after.canonical_artifact_bytes <= after.segment_cache_capacity_bytes {
            blocker_codes.push("canonical_artifact_does_not_exceed_cache".to_string());
        }
        if after.segment_cache_resident_bytes > after.segment_cache_capacity_bytes {
            blocker_codes.push("segment_cache_capacity_exceeded".to_string());
        }
        if after.segment_cache_miss_count == before.segment_cache_miss_count {
            blocker_codes.push("canonical_segment_read_not_observed".to_string());
        }
        if after.segment_cache_digest_mismatch_count != before.segment_cache_digest_mismatch_count {
            blocker_codes.push("canonical_segment_digest_mismatch".to_string());
        }
        if !after.delta_within_budget {
            blocker_codes.push("mutation_delta_budget_exceeded".to_string());
        }
        if limits.require_fully_streamed && !query.fully_streamed {
            blocker_codes.push("query_not_fully_streamed".to_string());
        }
        check_optional_metric(
            &mut blocker_codes,
            pipeline.steady_resident_bytes,
            Some(limits.max_steady_resident_bytes),
            "steady_rss",
            true,
        );
        check_optional_metric(
            &mut blocker_codes,
            pipeline.peak_resident_bytes,
            Some(limits.max_peak_resident_bytes),
            "peak_rss",
            true,
        );
        check_optional_metric(
            &mut blocker_codes,
            pipeline.total_page_faults,
            limits.max_total_page_faults,
            "total_page_faults",
            true,
        );
        if let Some(max_page_faults) = limits.max_minor_page_faults {
            check_optional_metric(
                &mut blocker_codes,
                pipeline.minor_page_faults,
                Some(max_page_faults),
                "minor_page_faults",
                true,
            );
        }
        if let Some(max_page_faults) = limits.max_major_page_faults {
            check_optional_metric(
                &mut blocker_codes,
                pipeline.major_page_faults,
                Some(max_page_faults),
                "major_page_faults",
                true,
            );
        }
        if pipeline.intermediate_rows > limits.max_intermediate_rows {
            blocker_codes.push("intermediate_rows_exceeded".to_string());
        }
        if pipeline.intermediate_payload_bytes > limits.max_intermediate_payload_bytes {
            blocker_codes.push("intermediate_payload_bytes_exceeded".to_string());
        }
        if query.output_rows > limits.max_output_rows {
            blocker_codes.push("output_rows_exceeded".to_string());
        }
        if query.output_payload_bytes > limits.max_output_payload_bytes {
            blocker_codes.push("output_payload_bytes_exceeded".to_string());
        }

        Ok(StorageResourceProfileReport {
            resource_ready: blocker_codes.is_empty(),
            blocker_codes,
            evidence_binding,
            expected_identity,
            canonical_graph_commit_epoch,
            limits,
            durable,
            before,
            after,
            query,
        })
    }
}

fn check_optional_metric(
    blocker_codes: &mut Vec<String>,
    measured: Option<u64>,
    limit: Option<u64>,
    name: &str,
    required: bool,
) {
    match measured {
        Some(measured) if limit.is_some_and(|limit| measured > limit) => {
            blocker_codes.push(format!("{name}_exceeded"));
        }
        Some(_) => {}
        None if required => blocker_codes.push(format!("{name}_unavailable")),
        None => {}
    }
}
