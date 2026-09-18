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

//! Typed storage resource-profile evidence for embedded HawDB hosts.

use hawdb_core::{HawDBError, Result};
use hawdb_evidence::{
    production_evidence_blocker_codes, ProductionEvidenceBinding, ProductionQualificationIdentity,
};
use hawdb_executor::QueryStreamReport;
use hawdb_storage::{
    GraphIndexReadMetricsSnapshot, PersistentGraphIndexClass,
    RelationalIndexStorageResidencyReport, RelationalRowStorageResidencyReport,
    StorageResidencyReport,
};

pub use hawdb_evidence::resource_profile::STORAGE_RESOURCE_PROFILE_PROTOCOL;

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
    #[doc(hidden)]
    pub fn validate(&self) -> Result<()> {
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
            return Err(HawDBError::Semantic(format!(
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
    pub evidence_binding: Option<ProductionEvidenceBinding>,
    pub expected_identity: Option<ProductionQualificationIdentity>,
    pub canonical_graph_commit_epoch: u64,
    pub limits: StorageResourceProfileLimits,
    pub durable: bool,
    pub before: StorageResidencyReport,
    pub after: StorageResidencyReport,
    pub query: QueryStreamReport,
}

#[doc(hidden)]
pub struct StorageResourceProfileObservation {
    pub canonical_graph_commit_epoch: u64,
    pub limits: StorageResourceProfileLimits,
    pub durable: bool,
    pub before: StorageResidencyReport,
    pub after: StorageResidencyReport,
    pub query: QueryStreamReport,
    pub evidence_binding: Option<ProductionEvidenceBinding>,
    pub expected_identity: Option<ProductionQualificationIdentity>,
}

impl StorageResourceProfileReport {
    #[doc(hidden)]
    pub fn from_observation(observation: StorageResourceProfileObservation) -> Self {
        let StorageResourceProfileObservation {
            canonical_graph_commit_epoch,
            limits,
            durable,
            before,
            after,
            query,
            evidence_binding,
            expected_identity,
        } = observation;
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
        if after.segment_cache_hit_count == before.segment_cache_hit_count
            && after.segment_cache_miss_count == before.segment_cache_miss_count
        {
            blocker_codes.push("storage_segment_access_not_observed".to_string());
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

        Self {
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
        }
    }

    pub fn json(&self) -> serde_json::Value {
        let production_blocker_codes = self.production_blocker_codes();
        let pipeline = &self.query.execution_profile.pipeline_memory_report;
        let evidence_binding = self
            .evidence_binding
            .as_ref()
            .map(ProductionEvidenceBinding::json);
        let expected_identity = self
            .expected_identity
            .as_ref()
            .map(ProductionQualificationIdentity::json);
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
        let graph_index_reads = self
            .after
            .graph_index_reads
            .delta_since(self.before.graph_index_reads);
        let graph_index_reads_json = graph_index_reads_json(graph_index_reads);
        let mut json = serde_json::json!({
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
                "canonical_adjacency_artifact_bytes": self.after
                    .canonical_adjacency_artifact_bytes,
                "persistent_property_projection_artifact_bytes": self.after
                    .persistent_property_projection_artifact_bytes,
                "canonical_node_count": self.after.canonical_node_count,
                "canonical_relationship_count": self.after.canonical_relationship_count,
                "canonical_exceeds_cache": self.after.canonical_artifact_bytes
                    > self.after.segment_cache_capacity_bytes,
                "segment_cache_capacity_bytes": self.after.segment_cache_capacity_bytes,
                "segment_cache_resident_bytes_before": self.before.segment_cache_resident_bytes,
                "segment_cache_resident_bytes_after": self.after.segment_cache_resident_bytes,
                "segment_cache_hit_count_delta": self.after.segment_cache_hit_count
                    .saturating_sub(self.before.segment_cache_hit_count),
                "segment_cache_miss_count_delta": self.after.segment_cache_miss_count
                    .saturating_sub(self.before.segment_cache_miss_count),
                "segment_cache_eviction_count_delta": self.after.segment_cache_eviction_count
                    .saturating_sub(self.before.segment_cache_eviction_count),
                "segment_cache_admission_rejection_count_delta": self.after
                    .segment_cache_admission_rejection_count
                    .saturating_sub(self.before.segment_cache_admission_rejection_count),
                "segment_cache_digest_mismatch_count_delta": self.after
                    .segment_cache_digest_mismatch_count
                    .saturating_sub(self.before.segment_cache_digest_mismatch_count),
                "persistent_graph_index_reads": graph_index_reads_json,
                "relational_rows": relational_row_residency_json(&self.after.relational_rows),
                "relational_indexes": relational_index_residency_json(&self.after.relational_indexes),
                "estimated_delta_resident_bytes": self.after.estimated_delta_resident_bytes,
                "max_out_of_core_delta_bytes": self.after.max_out_of_core_delta_bytes,
                "graph_manifest_open_budget_bytes": self.after.graph_manifest_open_budget_bytes,
                "graph_manifest_encoded_bytes": self.after.graph_manifest_encoded_bytes,
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
        });
        let execution = json
            .get_mut("execution")
            .and_then(serde_json::Value::as_object_mut)
            .expect("resource profile execution is an object");
        execution.insert(
            "query_memory_budget_bytes".to_string(),
            pipeline.query_memory_budget_bytes.into(),
        );
        execution.insert(
            "query_memory_peak_bytes".to_string(),
            pipeline.query_memory_peak_bytes.into(),
        );
        execution.insert(
            "query_memory_completion_bytes".to_string(),
            pipeline.query_memory_completion_bytes.into(),
        );
        execution.insert(
            "query_memory_account_count".to_string(),
            pipeline.query_memory_account_count.into(),
        );
        execution.insert(
            "morsel_peak_buffered_outputs".to_string(),
            pipeline.morsel_peak_buffered_outputs.into(),
        );
        execution.insert(
            "morsel_peak_buffered_output_bytes".to_string(),
            pipeline.morsel_peak_buffered_output_bytes.into(),
        );
        execution.insert(
            "morsel_peak_reorder_entries".to_string(),
            pipeline.morsel_peak_reorder_entries.into(),
        );
        json
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

fn relational_row_residency_json(
    report: &RelationalRowStorageResidencyReport,
) -> serde_json::Value {
    serde_json::json!({
        "serving": report.serving,
        "materialized_rows_resident": report.materialized_rows_resident,
        "checkpoint_state_metadata_only": report.checkpoint_state_metadata_only,
        "materialized_row_count": report.materialized_row_count,
        "materialized_row_bytes": report.materialized_row_bytes,
        "logical_row_count": report.logical_row_count,
        "base_generation": report.base_generation,
        "recovery_delta_generation": report.recovery_delta_generation,
        "base_commit_epoch": report.base_commit_epoch,
        "visible_commit_epoch": report.visible_commit_epoch,
        "root_page_count": report.root_page_count,
        "physical_generation_count": report.physical_generation_count,
        "allocated_page_count": report.allocated_page_count,
        "live_page_bytes": report.live_page_bytes,
        "allocated_page_bytes": report.allocated_page_bytes,
        "page_artifact_bytes": report.page_artifact_bytes,
        "root_descriptor_artifact_bytes": report.root_descriptor_artifact_bytes,
        "root_key_artifact_bytes": report.root_key_artifact_bytes,
        "overflow_extent_count": report.overflow_extent_count,
        "overflow_extent_artifact_bytes": report.overflow_extent_artifact_bytes,
        "overflow_descriptor_artifact_bytes": report.overflow_descriptor_artifact_bytes,
        "canonical_artifact_bytes": report.canonical_artifact_bytes(),
        "recovery_delta_runs": report.recovery_delta_runs,
        "recovery_delta_checkpoint_runs": report.recovery_delta_checkpoint_runs,
        "recovery_delta_checkpoint_recommended": report.recovery_delta_checkpoint_recommended,
        "recovery_delta_entries": report.recovery_delta_entries,
        "recovery_delta_artifact_bytes": report.recovery_delta_artifact_bytes,
        "live_batches": report.live_batches,
        "live_entries": report.live_entries,
        "live_encoded_bytes": report.live_encoded_bytes,
        "live_resident_bytes": report.live_resident_bytes,
        "monotonic_append_attempts": report.monotonic_append_attempts,
        "monotonic_append_hits": report.monotonic_append_hits,
        "monotonic_append_fallbacks": report.monotonic_append_fallbacks,
        "monotonic_append_proven_absent_primary_keys": report.monotonic_append_proven_absent_primary_keys,
    })
}

fn relational_index_residency_json(
    report: &RelationalIndexStorageResidencyReport,
) -> serde_json::Value {
    serde_json::json!({
        "serving": report.serving,
        "base_generation": report.base_generation,
        "recovery_delta_generation": report.recovery_delta_generation,
        "base_commit_epoch": report.base_commit_epoch,
        "visible_commit_epoch": report.visible_commit_epoch,
        "root_count": report.root_count,
        "base_page_count": report.base_page_count,
        "base_artifact_bytes": report.base_artifact_bytes,
        "recovery_delta_pages": report.recovery_delta_pages,
        "recovery_delta_entries": report.recovery_delta_entries,
        "recovery_delta_artifact_bytes": report.recovery_delta_artifact_bytes,
        "canonical_artifact_bytes": report.canonical_artifact_bytes(),
        "live_batches": report.live_batches,
        "live_entries": report.live_entries,
        "live_encoded_bytes": report.live_encoded_bytes,
    })
}

fn graph_index_reads_json(reads: GraphIndexReadMetricsSnapshot) -> serde_json::Value {
    use PersistentGraphIndexClass as Class;

    serde_json::json!({
        "total_operation_count": reads.total_operation_count(),
        "operation_counts": {
            "node_equality": reads.operation_count(Class::NodeEquality),
            "node_range": reads.operation_count(Class::NodeRange),
            "node_full_text": reads.operation_count(Class::NodeFullText),
            "node_composite_equality": reads.operation_count(Class::NodeCompositeEquality),
            "relationship_equality": reads.operation_count(Class::RelationshipEquality),
            "relationship_range": reads.operation_count(Class::RelationshipRange),
            "forward_adjacency": reads.operation_count(Class::ForwardAdjacency),
            "reverse_adjacency": reads.operation_count(Class::ReverseAdjacency),
        },
        "class_reads": {
            "node_equality": graph_index_class_read_json(reads, Class::NodeEquality),
            "node_range": graph_index_class_read_json(reads, Class::NodeRange),
            "node_full_text": graph_index_class_read_json(reads, Class::NodeFullText),
            "node_composite_equality": graph_index_class_read_json(reads, Class::NodeCompositeEquality),
            "relationship_equality": graph_index_class_read_json(reads, Class::RelationshipEquality),
            "relationship_range": graph_index_class_read_json(reads, Class::RelationshipRange),
            "forward_adjacency": graph_index_class_read_json(reads, Class::ForwardAdjacency),
            "reverse_adjacency": graph_index_class_read_json(reads, Class::ReverseAdjacency),
        },
        "property": {
            "blocks_considered": reads.property_blocks_considered,
            "blocks_pruned": reads.property_blocks_pruned,
            "blocks_read": reads.property_blocks_read,
            "bytes_read": reads.property_bytes_read,
            "entries_decoded": reads.property_entries_decoded,
            "candidates_returned": reads.property_candidates_returned,
        },
        "adjacency": {
            "blocks_considered": reads.adjacency_blocks_considered,
            "blocks_read": reads.adjacency_blocks_read,
            "bytes_read": reads.adjacency_bytes_read,
            "records_decoded": reads.adjacency_records_decoded,
            "sparse_blocks_read": reads.adjacency_sparse_blocks_read,
            "dense_blocks_read": reads.adjacency_dense_blocks_read,
        },
    })
}

fn graph_index_class_read_json(
    reads: GraphIndexReadMetricsSnapshot,
    class: PersistentGraphIndexClass,
) -> serde_json::Value {
    serde_json::json!({
        "operation_count": reads.operation_count(class),
        "blocks_read": reads.blocks_read(class),
        "bytes_read": reads.bytes_read(class),
        "cache_hits": reads.cache_hits(class),
        "cache_misses": reads.cache_misses(class),
    })
}

#[cfg(test)]
mod tests {
    use super::{relational_row_residency_json, StorageResourceProfileLimits};
    use hawdb_storage::RelationalRowStorageResidencyReport;

    #[test]
    fn limits_reject_zero_budgets() {
        let limits = StorageResourceProfileLimits {
            min_canonical_artifact_bytes: 0,
            max_steady_resident_bytes: 1,
            max_peak_resident_bytes: 1,
            max_total_page_faults: None,
            max_minor_page_faults: None,
            max_major_page_faults: None,
            max_intermediate_rows: 1,
            max_intermediate_payload_bytes: 1,
            max_output_rows: 1,
            max_output_payload_bytes: 1,
            require_fully_streamed: true,
        };

        assert!(limits.validate().is_err());
    }

    #[test]
    fn row_page_occupancy_is_visible_in_resource_profile_json() {
        let report = RelationalRowStorageResidencyReport {
            physical_generation_count: 2,
            allocated_page_count: 7,
            root_page_count: 3,
            live_page_bytes: 3 * 128 * 1024,
            allocated_page_bytes: 7 * 128 * 1024,
            page_artifact_bytes: 2 * 128 * 1024,
            ..Default::default()
        };
        let json = relational_row_residency_json(&report);

        assert_eq!(json["physical_generation_count"], 2);
        assert_eq!(json["allocated_page_count"], 7);
        assert_eq!(json["live_page_bytes"], 3 * 128 * 1024);
        assert_eq!(json["allocated_page_bytes"], 7 * 128 * 1024);
        assert_eq!(json["canonical_artifact_bytes"], 7 * 128 * 1024);
    }
}
