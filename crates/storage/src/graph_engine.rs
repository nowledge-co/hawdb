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
//! The embedded store still owns the concrete `GraphStore`; this module names
//! the read-only observability surface the facade is allowed to depend on so it
//! can be generic over the engine instead of the concrete store.

use crate::{
    AppendSegmentReadOutput, AppendState, AppendTableSchema, PublishedReadView, RelationalKey,
    RelationalState, SegmentCacheSnapshot, StorageRecoveryReport, StorageResidencyReport,
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

    fn append_table_schema(&self, table: &str) -> Option<&AppendTableSchema>;

    fn initial_import_source_fingerprint(&self) -> Option<&str>;

    fn append_state(&self) -> &AppendState;

    fn relational_state(&self) -> &RelationalState;

    fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &RelationalKey,
        after: Option<&RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput>;

    /// Fail-closed poison decision taken while serving a read result.
    fn poison_on_storage_error<T>(&self, result: &Result<T>);

    fn ensure_usable(&self) -> Result<()>;

    fn is_out_of_core(&self) -> bool;

    fn relationship_owned(&self, id: crate::RelId) -> Result<Option<crate::RelRecord>>;
}

/// Graph write surface the executor drives.
pub trait GraphMutationEngine {
    fn checkpoint(&mut self, catalog: &Catalog) -> Result<()>;

    fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: std::collections::BTreeMap<String, hawdb_core::Value>,
    ) -> Result<crate::NodeId>;

    fn create_node_label(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
    ) -> Result<hawdb_core::LabelId>;

    fn create_node_table(
        &mut self,
        catalog: &mut Catalog,
        name: &str,
    ) -> Result<hawdb_core::TableId>;

    fn create_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<hawdb_core::IndexId>;

    fn create_composite_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: &[String],
    ) -> Result<hawdb_core::IndexId>;

    fn create_full_text_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<hawdb_core::IndexId>;

    fn create_node_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<hawdb_core::ConstraintId>;

    fn create_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: crate::ConnectedNodesCreate,
    ) -> Result<(crate::NodeId, crate::RelId, crate::NodeId)>;

    fn alter_table_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: hawdb_core::TableKind,
        table: &str,
        state: hawdb_core::SchemaObjectState,
    ) -> Result<(hawdb_core::TableId, bool)>;

    fn add_int_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&crate::PropertyFilter>,
        property: &str,
        amount: i64,
    ) -> Result<Vec<crate::NodeId>>;

    fn alter_property_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: hawdb_core::TableKind,
        table: &str,
        property: &str,
        state: hawdb_core::SchemaObjectState,
    ) -> Result<(hawdb_core::PropertyId, bool)>;

    fn create_property_descriptor(
        &mut self,
        catalog: &mut Catalog,
        table_kind: hawdb_core::TableKind,
        table: &str,
        property: &str,
        value_type: hawdb_core::PropertyType,
        nullable: bool,
    ) -> Result<hawdb_core::PropertyId>;

    fn create_range_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<hawdb_core::IndexId>;

    fn create_relationship_table(
        &mut self,
        catalog: &mut Catalog,
        name: &str,
    ) -> Result<hawdb_core::TableId>;

    fn create_relationship_type(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
    ) -> Result<hawdb_core::RelTypeId>;

    fn create_relationship_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<hawdb_core::ConstraintId>;

    fn create_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: crate::MatchedRelationshipCreate,
    ) -> Result<Vec<(crate::NodeId, crate::RelId, crate::NodeId)>>;

    fn create_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<hawdb_core::ConstraintId>;

    fn set_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&crate::PropertyFilter>,
        property: &str,
        value: hawdb_core::Value,
    ) -> Result<Vec<crate::NodeId>>;

    fn set_node_properties(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&crate::PropertyFilter>,
        assignments: &[crate::NodeSetAssignment],
    ) -> Result<Vec<crate::NodeId>>;

    fn set_node_properties_by_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[crate::NodeId],
        assignments: &[crate::NodeSetAssignment],
    ) -> Result<Vec<crate::NodeId>>;

    fn set_relationship_property(
        &mut self,
        catalog: &mut Catalog,
        update: crate::RelationshipPropertyUpdate,
    ) -> Result<Vec<crate::RelId>>;

    fn set_relationship_properties(
        &mut self,
        catalog: &mut Catalog,
        update: crate::RelationshipPropertiesUpdate,
    ) -> Result<Vec<crate::RelId>>;

    fn delete_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: crate::RelationshipDeleteRequest,
    ) -> Result<Vec<crate::RelId>>;

    fn delete_relationship_target_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: crate::RelationshipTargetNodeDelete,
    ) -> Result<Vec<crate::NodeId>>;

    fn merge_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        match_properties: std::collections::BTreeMap<String, hawdb_core::Value>,
        on_create_properties: std::collections::BTreeMap<String, hawdb_core::Value>,
        on_match_assignments: &[crate::NodeSetAssignment],
        post_merge_assignments: &[crate::NodeSetAssignment],
    ) -> Result<(crate::NodeId, bool)>;

    fn merge_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: crate::ConnectedNodesCreate,
    ) -> Result<(crate::NodeId, crate::RelId, crate::NodeId, bool)>;

    fn merge_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: crate::MatchedRelationshipMerge,
    ) -> Result<Vec<(crate::NodeId, crate::RelId, crate::NodeId, bool)>>;

    fn merge_relationships_from_matched_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: crate::MatchedRelationshipCopyMerge,
    ) -> Result<Vec<(crate::NodeId, crate::RelId, crate::NodeId, bool)>>;

    fn merge_relationships_from_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: crate::MatchedRelationshipSourceRetargetMerge,
    ) -> Result<Vec<(crate::NodeId, crate::RelId, crate::NodeId, bool)>>;

    fn merge_relationships_to_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: crate::MatchedRelationshipRetargetMerge,
    ) -> Result<Vec<(crate::NodeId, crate::RelId, crate::NodeId, bool)>>;

    fn register_projected_graph(
        &mut self,
        name: &str,
        definition: crate::ProjectedGraphDefinition,
    ) -> Result<()>;

    fn delete_node_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[crate::NodeId],
        detach: bool,
    ) -> Result<Vec<crate::NodeId>>;
}
