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

//! Inward read contract for the graph engine.
//!
//! The embedded facade may depend on this observability surface instead of the
//! concrete `GraphStore`. Execution-time store contracts already live in
//! `hawdb_executor::store` (`GraphExecutionRead` / `GraphExecutionWrite`), so
//! this trait deliberately covers only the read/observability surface the
//! facade itself consumes.

use crate::{
    AppendSegmentReadOutput, AppendTableSchema, PublishedReadView, RelationalKey,
    SegmentCacheSnapshot, StorageRecoveryReport, StorageResidencyReport,
};
use hawdb_core::{BasicGraphStatistics, Catalog, GraphStatistics, Result};

/// Read-only observability surface consumed by the embedded facade.
pub trait GraphReadEngine {
    fn storage_version(&self) -> &'static str;

    fn commit_epoch(&self) -> u64;

    fn storage_handle_poisoned(&self) -> bool;

    fn published_read_view(&self) -> PublishedReadView;

    fn basic_statistics(&self) -> BasicGraphStatistics;

    fn statistics(&self, catalog: &Catalog) -> GraphStatistics;

    fn storage_recovery_report(&self) -> StorageRecoveryReport;

    fn segment_cache_snapshot(&self) -> Option<SegmentCacheSnapshot>;

    fn storage_residency_report(&self) -> StorageResidencyReport;

    fn adjacency_consistency_report(&self) -> crate::consistency::AdjacencyConsistencyReport;

    fn adjacency_consolidation_plan(&self) -> crate::consistency::AdjacencyConsolidationPlan;

    fn degree_statistics_consistency_report(
        &self,
    ) -> crate::consistency::DegreeStatisticsConsistencyReport;

    fn property_index_consistency_report(
        &self,
        catalog: &Catalog,
    ) -> crate::consistency::PropertyIndexConsistencyReport;

    fn columnar_shadow_checkpoint_report(&self) -> Option<crate::ColumnarShadowCheckpointReport>;

    fn columnar_shadow_recovery_status(&self) -> crate::ColumnarShadowRecoveryStatus;

    fn projected_graph_statuses(&self) -> Vec<crate::ProjectedGraphStatus>;

    fn storage_pressure_snapshot(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> crate::StoragePressureSnapshot;

    fn append_storage_residency_report(&self) -> crate::AppendStorageResidencyReport;

    fn columnar_shadow_admission_bytes(&self) -> u64;

    fn relational_index_recovery_report(&self) -> Option<&crate::RelationalIndexRecoveryReport>;

    fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Option<&crate::relational::RelationalIndexShadowCheckpointReport>;

    fn relational_index_shadow_recovery_status(
        &self,
    ) -> &crate::relational::RelationalIndexShadowRecoveryStatus;

    fn search_projection_changefeed_status(&self) -> crate::SearchProjectionChangefeedStatus;

    fn append_table_schema(&self, table: &str) -> Option<&AppendTableSchema>;

    fn initial_import_source_fingerprint(&self) -> Option<&str>;

    fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &RelationalKey,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput>;
}

/// Maintenance/commit surface the embedded facade drives itself.
///
/// This deliberately does not restate the execution write contract: statement
/// execution goes through `hawdb_executor::store::GraphExecutionWrite`
/// (`commit_mutation_with_limits` and friends). These are the storage-lifecycle
/// operations the facade schedules directly.
pub trait GraphMutationEngine {
    fn plan_schema_maintenance(&self, catalog: &Catalog) -> Vec<crate::SchemaMaintenancePlanItem>;

    fn run_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
    ) -> Result<Vec<crate::SchemaMaintenanceAction>>;

    fn rebuild_projected_graph_artifacts(&mut self, catalog: &Catalog) -> Result<()>;

    fn rebuild_bounded_property_index_projections(
        &mut self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> Vec<crate::PropertyIndexProjectionRebuildAction>;

    fn scrub_storage(&mut self) -> Result<crate::StorageScrubReport>;

    fn backup_to(
        &mut self,
        catalog: &Catalog,
        destination: impl AsRef<std::path::Path>,
    ) -> Result<crate::StorageBackupReport>;
}
