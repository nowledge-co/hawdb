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

use crate::error::Result;
use crate::schema::Catalog;
use crate::store::{GraphScanControl, GraphStore};
use hawdb_core::{LabelId, RelTypeId};
use hawdb_executor::store::{
    AdjacencyReadMemory, GraphExecutionRead, GraphExecutionWrite, PrunedNodeScan,
    PrunedRelationshipScan, ScanControl, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use hawdb_executor::QueryMemoryLease;
use hawdb_plan::NodeProjectionAccess;
use hawdb_storage::{
    AdjacencyDirection, GraphMutation, MutationLimits, MutationSummary, NodeId, NodeRecord,
    NodeSetAssignment, ProjectedNodeRecord, PropertyFilter, RelId, RelRecord,
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

impl GraphExecutionRead for GraphStore {
    fn is_out_of_core(&self) -> bool {
        GraphStore::is_out_of_core(self)
    }

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        GraphStore::node_owned(self, id)
    }

    fn scan_nodes_borrowed<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        Box::new(GraphStore::scan_nodes(self, label_id))
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
        seek: &hawdb_plan::CompositeRangeSeek,
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
    ) -> Option<hawdb_storage::ProjectedGraphDefinition> {
        GraphStore::projected_graph_definition(self, name).cloned()
    }

    fn visit_source_scan_candidates(
        &self,
        predicate: &hawdb_storage::ScanPredicate,
        limits: SourceScanReadLimits,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        let mut consumer_error = None;
        let visit = GraphStore::visit_published_source_scan_candidates_bounded(
            self,
            predicate,
            crate::store::SourceScanCandidateLimits::bounded(
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
                    Ok(crate::store::GraphScanControl::Stop)
                }
            },
        )?;
        if let Some(error) = consumer_error {
            return Err(error);
        }
        Ok(match visit {
            crate::store::SourceScanCandidateVisit::Rows {
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
            crate::store::SourceScanCandidateVisit::Fallback(reason) => {
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
        let mut key_lease = memory
            .account
            .map(|account| account.reserve(0))
            .transpose()?;
        GraphStore::try_visit_ordered_adjacent_relationships_accounted(
            self,
            node_id,
            rel_type,
            direction,
            memory.budget_bytes,
            |bytes| grow_optional_lease(&mut key_lease, bytes),
            |relationship| consumer(relationship).map(to_store_control),
        )
        .map(to_execution_control)
    }

    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        filter: &PropertyFilter,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<hawdb_storage::ScanPruningReport>)> {
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
    ) -> Result<(ScanControl, Option<hawdb_storage::ScanPruningReport>)> {
        let mut entries = Vec::new();
        let mut key_lease = memory
            .account
            .map(|account| account.reserve(0))
            .transpose()?;
        let mut collection_error = None;
        let (control, report) = GraphStore::visit_adjacent_relationships_with_filter_owned(
            self,
            node_id,
            rel_type,
            direction,
            filter,
            |relationship| match push_ordered_adjacency_entry(
                &mut entries,
                ordered_adjacency_key(&relationship, direction),
                memory.budget_bytes,
                &mut key_lease,
            ) {
                Ok(()) => GraphScanControl::Continue,
                Err(error) => {
                    collection_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        if let Some(error) = collection_error {
            return Err(error);
        }
        if control == GraphScanControl::Stop {
            return Ok((ScanControl::Stop, report));
        }
        emit_ordered_adjacency_entries(self, entries, consumer).map(|control| (control, report))
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
            nodes: Box::new(scan.nodes.into_iter().cloned()),
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

fn ordered_adjacency_key(
    relationship: &RelRecord,
    direction: AdjacencyDirection,
) -> (NodeId, RelId) {
    let neighbor = match direction {
        AdjacencyDirection::Outgoing => relationship.target,
        AdjacencyDirection::Incoming => relationship.source,
    };
    (neighbor, relationship.id)
}

fn push_ordered_adjacency_entry(
    entries: &mut Vec<(NodeId, RelId)>,
    entry: (NodeId, RelId),
    memory_budget_bytes: usize,
    key_lease: &mut Option<QueryMemoryLease>,
) -> Result<()> {
    let entry_bytes = std::mem::size_of::<(NodeId, RelId)>();
    let required_bytes = entries.len().saturating_add(1).saturating_mul(entry_bytes);
    if required_bytes > memory_budget_bytes {
        return Err(crate::error::HawDBError::Execution(format!(
            "ordered adjacency keys use {required_bytes} bytes, exceeding blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    grow_optional_lease(key_lease, entry_bytes)?;
    entries.push(entry);
    Ok(())
}

fn grow_optional_lease(lease: &mut Option<QueryMemoryLease>, bytes: usize) -> Result<()> {
    if let Some(lease) = lease {
        lease.grow(bytes)?;
    }
    Ok(())
}

fn emit_ordered_adjacency_entries(
    store: &GraphStore,
    mut entries: Vec<(NodeId, RelId)>,
    consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
) -> Result<ScanControl> {
    entries.sort_unstable();
    for (_, relationship_id) in entries {
        let Some(relationship) = store.relationship_owned(relationship_id)? else {
            return Err(crate::error::HawDBError::StorageIntegrity(format!(
                "ordered adjacency references missing relationship {}",
                relationship_id.0
            )));
        };
        if consumer(relationship)? == ScanControl::Stop {
            return Ok(ScanControl::Stop);
        }
    }
    Ok(ScanControl::Continue)
}
