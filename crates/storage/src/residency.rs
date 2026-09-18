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

//! Storage-owned residency snapshots for host resource accounting.

use crate::{GraphIndexReadMetricsSnapshot, RelationalIndexStorageResidencyReport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageResidencyReport {
    pub out_of_core: bool,
    pub canonical_generation: Option<u64>,
    pub canonical_artifact_bytes: u64,
    pub canonical_adjacency_artifact_bytes: u64,
    pub persistent_property_projection_artifact_bytes: u64,
    pub canonical_node_count: u64,
    pub canonical_relationship_count: u64,
    pub delta_node_count: usize,
    pub delta_relationship_count: usize,
    pub node_tombstone_count: usize,
    pub relationship_tombstone_count: usize,
    pub estimated_delta_resident_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    pub delta_within_budget: bool,
    pub checkpoint_statistics_commit_epoch: u64,
    pub checkpoint_statistics_complete: bool,
    pub checkpoint_statistics_stale: bool,
    pub graph_manifest_open_budget_bytes: u64,
    pub graph_manifest_encoded_bytes: u64,
    pub segment_cache_capacity_bytes: u64,
    pub segment_cache_resident_bytes: u64,
    pub segment_cache_pinned_bytes: u64,
    pub segment_cache_hit_count: u64,
    pub segment_cache_miss_count: u64,
    pub segment_cache_eviction_count: u64,
    pub segment_cache_admission_rejection_count: u64,
    pub segment_cache_digest_mismatch_count: u64,
    pub graph_index_reads: GraphIndexReadMetricsSnapshot,
    pub relational_rows: RelationalRowStorageResidencyReport,
    pub relational_indexes: RelationalIndexStorageResidencyReport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalRowStorageResidencyReport {
    pub serving: bool,
    pub materialized_rows_resident: bool,
    pub checkpoint_state_metadata_only: bool,
    pub materialized_row_count: usize,
    pub materialized_row_bytes: u64,
    pub logical_row_count: usize,
    pub base_generation: Option<u64>,
    pub recovery_delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_page_count: u64,
    pub physical_generation_count: usize,
    pub allocated_page_count: u64,
    pub live_page_bytes: u64,
    /// Physical allocation referenced by the active root. Historical files held
    /// only by old reader pins or delayed cleanup are not included.
    pub allocated_page_bytes: u64,
    pub page_artifact_bytes: u64,
    pub root_descriptor_artifact_bytes: u64,
    pub root_key_artifact_bytes: u64,
    pub overflow_extent_count: u64,
    pub overflow_extent_artifact_bytes: u64,
    pub overflow_descriptor_artifact_bytes: u64,
    pub recovery_delta_runs: usize,
    pub recovery_delta_checkpoint_runs: usize,
    pub recovery_delta_checkpoint_recommended: bool,
    pub recovery_delta_entries: u64,
    pub recovery_delta_artifact_bytes: u64,
    pub live_batches: usize,
    pub live_entries: usize,
    pub live_encoded_bytes: usize,
    pub live_resident_bytes: usize,
    pub monotonic_append_attempts: u64,
    pub monotonic_append_hits: u64,
    pub monotonic_append_fallbacks: u64,
    pub monotonic_append_proven_absent_primary_keys: u64,
}

impl RelationalRowStorageResidencyReport {
    pub fn canonical_artifact_bytes(&self) -> u64 {
        self.allocated_page_bytes
            .saturating_add(self.root_descriptor_artifact_bytes)
            .saturating_add(self.root_key_artifact_bytes)
            .saturating_add(self.overflow_extent_artifact_bytes)
            .saturating_add(self.overflow_descriptor_artifact_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::RelationalRowStorageResidencyReport;

    #[test]
    fn row_residency_counts_active_artifacts_only() {
        let report = RelationalRowStorageResidencyReport {
            allocated_page_bytes: 100,
            root_descriptor_artifact_bytes: 20,
            root_key_artifact_bytes: 10,
            overflow_extent_artifact_bytes: 7,
            overflow_descriptor_artifact_bytes: 3,
            ..Default::default()
        };

        assert_eq!(report.canonical_artifact_bytes(), 140);
    }
}
