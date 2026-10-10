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

//! Root storage adapter for executor-owned graph read contracts.

use crate::store::{
    AdjacencyReadMemory, GraphExecutionRead, GraphExecutionWrite, PrunedNodeScan,
    PrunedRelationshipScan, ScanControl, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use crate::QueryMemoryLease;
use hawdb_core::error::Result;
use hawdb_core::schema::Catalog;
use hawdb_core::{LabelId, RelTypeId};
use hawdb_plan_cypher::NodeProjectionAccess;
use hawdb_storage::store::{GraphScanControl, GraphStore};
use hawdb_storage::{
    adjacency::AdjacencyDirection,
    mutation::{GraphMutation, MutationLimits, MutationSummary, NodeSetAssignment, PropertyFilter},
    NodeId, NodeRecord, ProjectedNodeRecord, RelId, RelRecord,
};
use std::collections::BTreeSet;

fn to_store_control(control: ScanControl) -> GraphScanControl {
    match control {
        ScanControl::Continue => GraphScanControl::Continue,
        ScanControl::Stop => GraphScanControl::Stop,
    }
}

fn to_execution_control(control: GraphScanControl) -> ScanControl {
    match control {
        GraphScanControl::Continue => ScanControl::Continue,
        GraphScanControl::Stop => ScanControl::Stop,
    }
}

fn adapt_node_consumer(
    consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    visit: impl FnOnce(&mut dyn FnMut(NodeRecord) -> GraphScanControl) -> Result<GraphScanControl>,
) -> Result<ScanControl> {
    let mut consumer_error = None;
    let control = visit(&mut |node| match consumer(node) {
        Ok(control) => to_store_control(control),
        Err(error) => {
            consumer_error = Some(error);
            GraphScanControl::Stop
        }
    })?;
    match consumer_error {
        Some(error) => Err(error),
        None => Ok(to_execution_control(control)),
    }
}

impl hawdb_storage::read_view::GraphReadAllocation for QueryMemoryLease {
    fn bytes(&self) -> usize {
        self.bytes()
    }
    fn grow(&mut self, bytes: usize) -> hawdb_core::Result<()> {
        QueryMemoryLease::grow(self, bytes)
    }
}

impl GraphExecutionRead for GraphStore {
    fn file_descriptor_context(&self) -> Option<hawdb_storage::file_descriptors::FileOpenContext> {
        GraphStore::file_descriptor_context(self)
    }

    fn is_out_of_core(&self) -> bool {
        GraphStore::is_out_of_core(self)
    }

    fn projected_node_owned_admitted(
        &self,
        id: NodeId,
        required_properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<ProjectedNodeRecord>> {
        GraphStore::projected_node_owned_admitted(self, id, required_properties, admit)
    }

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        GraphStore::node_owned(self, id)
    }

    fn node_with_allocation(
        &self,
        id: NodeId,
        label_ids: Option<&[LabelId]>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
    ) -> Result<hawdb_storage::read_view::AdmittedNodeRead> {
        GraphStore::node_with_allocation(self, id, label_ids, admit)
    }

    fn relationship_with_allocation(
        &self,
        id: RelId,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
    ) -> Result<hawdb_storage::read_view::AdmittedRelationshipRead> {
        GraphStore::relationship_with_allocation(self, id, admit)
    }

    fn scan_nodes_borrowed<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        Box::new(GraphStore::scan_nodes(self, label_id))
    }

    fn scan_indexed_nodes_borrowed<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        value: &hawdb_core::Value,
    ) -> Option<Box<dyn Iterator<Item = &'a NodeRecord> + 'a>> {
        Some(Box::new(GraphStore::seek_nodes_by_property(
            self, label_id, property, value,
        )))
    }

    fn node_count_for_label(&self, label_id: Option<LabelId>) -> usize {
        GraphStore::node_count_for_label(self, label_id)
    }

    fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize {
        GraphStore::relationship_count_for_type(self, rel_type)
    }

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::try_visit_nodes_owned(self, label_id, |node| {
            consumer(node).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn visit_relationships_with_allocation(
        &self,
        rel_type: Option<RelTypeId>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_relationships_with_allocation(self, rel_type, admit, &mut |record| {
            consumer(record).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn visit_nodes_with_allocation(
        &self,
        label_id: Option<LabelId>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedNodeRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_nodes_with_allocation(self, label_id, admit, &mut |node| {
            consumer(node).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn visit_nodes_by_access_with_allocation(
        &self,
        label_id: LabelId,
        access: &NodeProjectionAccess,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedNodeRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_nodes_by_access_with_allocation(
            self,
            label_id,
            access,
            admit,
            &mut |node| consumer(node).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_projected_nodes_admitted(
        &self,
        label_id: Option<LabelId>,
        properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_projected_nodes_admitted(self, label_id, properties, admit, &mut |node| {
            consumer(node).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn visit_projected_nodes_with_allocation(
        &self,
        label_id: Option<LabelId>,
        properties: &BTreeSet<String>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedProjectedNode,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_projected_nodes_with_allocation(
            self,
            label_id,
            properties,
            admit,
            &mut |node| consumer(node).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_projected_nodes_by_access_admitted(
        &self,
        label_id: LabelId,
        access: &NodeProjectionAccess,
        properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_projected_nodes_by_access_admitted(
            self,
            label_id,
            access,
            properties,
            admit,
            &mut |node| consumer(node).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_projected_nodes_by_property_admitted(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[hawdb_core::Value],
        properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_projected_nodes_by_property_admitted(
            self,
            label_id,
            property,
            values,
            properties,
            admit,
            &mut |node| consumer(node).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_projected_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let mut consumer_error = None;
        let control =
            GraphStore::visit_projected_nodes_owned(self, label_id, required_properties, |node| {
                match consumer(node) {
                    Ok(control) => to_store_control(control),
                    Err(error) => {
                        consumer_error = Some(error);
                        GraphScanControl::Stop
                    }
                }
            })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(to_execution_control(control)),
        }
    }

    fn visit_projected_nodes_by_access_owned(
        &self,
        label_id: LabelId,
        access: &NodeProjectionAccess,
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let mut consumer_error = None;
        let control = GraphStore::visit_projected_nodes_by_access_owned(
            self,
            label_id,
            access,
            required_properties,
            |node| match consumer(node) {
                Ok(control) => to_store_control(control),
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(to_execution_control(control)),
        }
    }

    fn visit_projected_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[hawdb_core::Value],
        required_properties: &BTreeSet<String>,
        consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let mut consumer_error = None;
        let control = GraphStore::visit_projected_nodes_by_property_owned(
            self,
            label_id,
            property,
            values,
            required_properties,
            |node| match consumer(node) {
                Ok(control) => to_store_control(control),
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(to_execution_control(control)),
        }
    }

    fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[hawdb_core::Value],
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let mut consumer_error = None;
        let control =
            GraphStore::visit_nodes_by_property_owned(self, label_id, property, values, |node| {
                match consumer(node) {
                    Ok(control) => to_store_control(control),
                    Err(error) => {
                        consumer_error = Some(error);
                        GraphScanControl::Stop
                    }
                }
            })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(to_execution_control(control)),
        }
    }

    fn visit_nodes_by_composite_property_owned(
        &self,
        label_id: LabelId,
        predicates: &[(String, hawdb_core::Value)],
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        adapt_node_consumer(consumer, |consumer| {
            GraphStore::visit_nodes_by_composite_property_owned(
                self, label_id, predicates, consumer,
            )
        })
    }

    fn visit_nodes_by_composite_range_owned(
        &self,
        label_id: LabelId,
        seek: &hawdb_plan_cypher::CompositeRangeSeek,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        adapt_node_consumer(consumer, |consumer| {
            GraphStore::visit_nodes_by_composite_range_owned(self, label_id, seek, consumer)
        })
    }

    fn visit_nodes_by_property_range_owned(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(hawdb_core::Value, bool)>,
        upper: Option<&(hawdb_core::Value, bool)>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        adapt_node_consumer(consumer, |consumer| {
            GraphStore::visit_nodes_by_property_range_owned(
                self, label_id, property, lower, upper, consumer,
            )
        })
    }

    fn visit_nodes_by_full_text_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        query: &str,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        adapt_node_consumer(consumer, |consumer| {
            GraphStore::visit_nodes_by_full_text_property_owned(
                self, label_id, property, query, consumer,
            )
        })
    }

    fn projected_graph_definition(
        &self,
        name: &str,
    ) -> Option<hawdb_storage::projection::ProjectedGraphDefinition> {
        GraphStore::projected_graph_definition(self, name).cloned()
    }

    fn visit_source_scan_candidates(
        &self,
        predicate: &hawdb_storage::scan::ScanPredicate,
        limits: SourceScanReadLimits,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        let mut consumer_error = None;
        let visit = GraphStore::visit_published_source_scan_candidates_bounded(
            self,
            predicate,
            hawdb_storage::store::SourceScanCandidateLimits::bounded(
                limits.io_depth,
                limits.max_coalesced_bytes,
                limits.max_wave_bytes,
                limits.max_live_candidate_bytes,
            ),
            task_context,
            &mut |row| match consumer(SourceScanCandidateRow {
                node_id: row.node_id,
                properties: row.properties,
            }) {
                Ok(control) => Ok(to_store_control(control)),
                Err(error) => {
                    consumer_error = Some(error);
                    Ok(hawdb_storage::store::GraphScanControl::Stop)
                }
            },
        )?;
        if let Some(error) = consumer_error {
            return Err(error);
        }
        Ok(match visit {
            hawdb_storage::store::SourceScanCandidateVisit::Rows {
                graph_epoch,
                skipped_segment_count,
                report,
                candidate_count,
            } => SourceScanCandidateVisit::Rows {
                graph_epoch,
                skipped_segment_count,
                report,
                candidate_count,
            },
            hawdb_storage::store::SourceScanCandidateVisit::Fallback(reason) => {
                SourceScanCandidateVisit::Fallback(reason)
            }
        })
    }

    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::try_visit_adjacent_relationships_owned(
            self,
            node_id,
            rel_type,
            direction,
            |relationship| consumer(relationship).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_ordered_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphExecutionRead::visit_ordered_adjacent_relationships_with_allocation(
            self,
            node_id,
            rel_type,
            direction,
            memory,
            &mut |bytes| memory.admit_node(bytes, 0, None).map(Some),
            &mut |relationship| {
                let (relationship, _allocation) = relationship.into_parts();
                consumer(relationship)
            },
        )
    }

    fn visit_ordered_adjacent_relationships_with_allocation(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        memory: AdjacencyReadMemory<'_>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::visit_ordered_adjacent_relationships_with_allocation(
            self,
            node_id,
            rel_type,
            direction,
            memory.budget_bytes,
            admit,
            |relationship| consumer(relationship).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_filtered_ordered_relationships_with_allocation(
        &self,
        adjacency: (NodeId, Option<RelTypeId>, AdjacencyDirection),
        filter: &PropertyFilter,
        memory: AdjacencyReadMemory<'_>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<hawdb_storage::scan::ScanPruningReport>)> {
        GraphStore::visit_filtered_ordered_relationships_with_allocation(
            self,
            adjacency,
            filter,
            memory.budget_bytes,
            admit,
            |row| consumer(row).map(to_store_control),
        )
        .map(|(control, report)| (to_execution_control(control), report))
    }

    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<hawdb_storage::scan::ScanPruningReport>)> {
        let mut consumer_error = None;
        let (control, report) = GraphStore::visit_adjacent_relationships_with_filter_owned(
            self,
            node_id,
            rel_type,
            direction,
            filter,
            |relationship| match consumer(relationship) {
                Ok(control) => to_store_control(control),
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok((to_execution_control(control), report)),
        }
    }

    fn visit_ordered_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<hawdb_storage::scan::ScanPruningReport>)> {
        GraphStore::visit_filtered_ordered_relationships_with_allocation(
            self,
            (node_id, rel_type, direction),
            filter,
            memory.budget_bytes,
            &mut |bytes| memory.admit_node(bytes, 0, None).map(Some),
            |row| {
                let (relationship, _allocation) = row.into_parts();
                consumer(relationship).map(to_store_control)
            },
        )
        .map(|(control, report)| (to_execution_control(control), report))
    }

    fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        GraphStore::try_visit_relationships_owned(self, rel_type, |relationship| {
            consumer(relationship).map(to_store_control)
        })
        .map(to_execution_control)
    }

    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        let scan = GraphStore::scan_relationships_with_filter_pruning(self, rel_type, filter);
        Ok(PrunedRelationshipScan {
            relationships: Box::new(scan.relationships.into_iter().cloned()),
            report: scan.report,
        })
    }

    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>> {
        let scan = GraphStore::scan_nodes_with_filter_pruning(self, catalog, label_id, filter);
        Ok(PrunedNodeScan {
            nodes: Box::new(scan.nodes.into_iter().map(std::borrow::Cow::Borrowed)),
            report: scan.report,
        })
    }

    fn scan_nodes_with_filter_pruning_admitted<'a>(
        &'a self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        admit: &mut hawdb_storage::read_view::GraphReadAllocator<'_>,
    ) -> Result<PrunedNodeScan<'a>> {
        let (scan, allocation) = GraphStore::scan_nodes_with_filter_pruning_admitted(
            self, catalog, label_id, filter, admit,
        )?;
        Ok(PrunedNodeScan {
            nodes: Box::new(scan.nodes.into_iter().map(move |node| {
                let _retained_allocation = &allocation;
                std::borrow::Cow::Borrowed(node)
            })),
            report: scan.report,
        })
    }
}

impl GraphExecutionWrite for GraphStore {
    fn commit_mutation_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        GraphStore::commit_mutation_with_limits(self, catalog, mutation, limits)
    }

    fn commit_mutations_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        GraphStore::commit_mutations_with_limits(self, catalog, mutations, limits)
    }

    fn set_node_properties_by_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        GraphStore::set_node_properties_by_ids_with_limits(self, catalog, ids, assignments, limits)
    }

    fn delete_node_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        GraphStore::delete_node_ids_with_limits(self, catalog, ids, detach, limits)
    }
}
