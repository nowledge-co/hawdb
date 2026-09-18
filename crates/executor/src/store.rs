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

//! Storage-neutral graph read contract used by execution operators.

use hawdb_core::{Catalog, LabelId, RelTypeId, Result};
use hawdb_plan::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{
    AdjacencyDirection, GraphMutation, MutationLimits, MutationSummary, NodeId, NodeRecord,
    NodeSetAssignment, ProjectedGraphDefinition, ProjectedNodeRecord, PropertyFilter, RelRecord,
    ScanPredicate, ScanPruningReport, ScanSegmentFallback, SegmentReadExecutionReport,
};
use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};

use crate::QueryMemoryAccount;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanControl {
    Continue,
    Stop,
}

pub struct PrunedRelationshipScan<'a> {
    pub relationships: Box<dyn Iterator<Item = RelRecord> + 'a>,
    pub report: ScanPruningReport,
}

pub struct PrunedNodeScan<'a> {
    pub nodes: Box<dyn Iterator<Item = NodeRecord> + 'a>,
    pub report: ScanPruningReport,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceScanCandidateRow {
    pub node_id: u64,
    pub properties: std::collections::BTreeMap<String, hawdb_core::Value>,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceScanReadLimits {
    pub io_depth: NonZeroUsize,
    pub max_coalesced_bytes: NonZeroU64,
    pub max_wave_bytes: NonZeroU64,
    pub max_live_candidate_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SourceScanCandidateVisit {
    Rows {
        graph_epoch: u64,
        skipped_segment_count: usize,
        report: SegmentReadExecutionReport,
        candidate_count: usize,
    },
    Fallback(ScanSegmentFallback),
}

/// Per-query memory boundary for ordered adjacency reads.
///
/// Storage implementations may need compact transient keys to preserve order
/// across relationship types. Such keys must satisfy both this local byte
/// limit and the optional query-root account before allocation.
#[derive(Clone, Copy)]
pub struct AdjacencyReadMemory<'a> {
    pub budget_bytes: usize,
    pub account: Option<&'a QueryMemoryAccount>,
}

/// The graph reads required by storage-independent execution operators.
///
/// Implementations own storage layout, residency, and pruning details. Owned
/// records keep executor state independent from storage guard lifetimes and
/// make memory accounting deterministic at the operator boundary.
pub trait GraphExecutionRead {
    fn is_out_of_core(&self) -> bool;

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>>;

    fn scan_nodes_borrowed<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a>;

    fn node_count_for_label(&self, label_id: Option<LabelId>) -> usize;

    fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize;

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    /// Visits every live relationship, including persisted base records and
    /// uncheckpointed changes, in either residency mode. Stop and callback
    /// errors terminate the scan without further consumer calls.
    fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_projected_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.visit_nodes_owned(label_id, &mut |node| {
            let properties = required_properties
                .iter()
                .filter_map(|property| {
                    node.properties
                        .get(property)
                        .cloned()
                        .map(|value| (property.clone(), value))
                })
                .collect();
            consumer(ProjectedNodeRecord {
                id: node.id,
                labels: node.labels,
                properties,
            })
        })
    }

    fn visit_projected_nodes_by_access_owned(
        &self,
        label_id: LabelId,
        access: &NodeProjectionAccess,
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_projected_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[hawdb_core::Value],
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.visit_projected_nodes_by_access_owned(
            label_id,
            &NodeProjectionAccess::PropertyValues {
                property: property.to_string(),
                values: values.to_vec(),
            },
            required_properties,
            consumer,
        )
    }

    fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[hawdb_core::Value],
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_nodes_by_composite_property_owned(
        &self,
        label_id: LabelId,
        predicates: &[(String, hawdb_core::Value)],
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_nodes_by_composite_range_owned(
        &self,
        label_id: LabelId,
        seek: &CompositeRangeSeek,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_nodes_by_property_range_owned(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(hawdb_core::Value, bool)>,
        upper: Option<&(hawdb_core::Value, bool)>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_nodes_by_full_text_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        query: &str,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn projected_graph_definition(&self, name: &str) -> Option<ProjectedGraphDefinition>;

    fn visit_source_scan_candidates(
        &self,
        predicate: &ScanPredicate,
        limits: SourceScanReadLimits,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit>;

    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_ordered_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        _memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.visit_adjacent_relationships_owned(node_id, rel_type, direction, consumer)
    }

    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)>;

    fn visit_ordered_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        _memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        self.visit_adjacent_relationships_with_filter_owned(
            node_id, rel_type, direction, filter, consumer,
        )
    }

    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>>;

    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>>;
}

/// The mutation operations required by storage-independent executor preflight.
///
/// Transaction ownership, WAL publication, and recovery remain implementation
/// concerns. The executor supplies only a validated mutation or a bounded set
/// of node IDs selected through [`GraphExecutionRead`].
pub trait GraphExecutionWrite: GraphExecutionRead {
    fn commit_mutation_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary>;

    fn set_node_properties_by_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>>;

    fn delete_node_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>>;
}
