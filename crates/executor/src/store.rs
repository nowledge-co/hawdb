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
use hawdb_plan_cypher::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{
    adjacency::AdjacencyDirection,
    mutation::{GraphMutation, MutationLimits, MutationSummary, NodeSetAssignment, PropertyFilter},
    projection::ProjectedGraphDefinition,
    scan::{ScanPredicate, ScanPruningReport, ScanSegmentFallback, SegmentReadExecutionReport},
    NodeId, NodeRecord, ProjectedNodeRecord, RelRecord,
};
use std::borrow::Cow;
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
    /// Resident storage lends records so consumers can admit the selected
    /// owned input before cloning. Hydrating sources retain their own read
    /// boundary and may supply owned records.
    pub nodes: Box<dyn Iterator<Item = Cow<'a, NodeRecord>> + 'a>,
    pub report: ScanPruningReport,
}

struct QueryGraphReadAllocation {
    allocation: crate::QueryMemoryLease,
    task_context: Option<hawdb_core::RuntimeTaskContext>,
    retained_state: Option<QueryMemoryAccount>,
}

impl hawdb_storage::read_view::GraphReadAllocation for QueryGraphReadAllocation {
    fn bytes(&self) -> usize {
        self.allocation.bytes()
    }

    fn grow(&mut self, bytes: usize) -> Result<()> {
        crate::pipeline::runtime_checkpoint(self.task_context.as_ref())?;
        if bytes == 0 {
            Ok(())
        } else {
            self.allocation.grow(bytes)
        }
    }

    fn retain_state(&mut self) -> Result<()> {
        crate::pipeline::runtime_checkpoint(self.task_context.as_ref())?;
        let Some(account) = self.retained_state.as_ref() else {
            return Ok(());
        };
        let bytes = self.allocation.bytes();
        let mut retained = account.reserve(0)?;
        self.allocation.transfer_to(bytes, &mut retained, bytes)?;
        self.allocation = retained;
        self.retained_state = None;
        Ok(())
    }
}

/// Transfer a query-ledger permit with owned graph input.
#[doc(hidden)]
pub fn admit_graph_read(
    account: &QueryMemoryAccount,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    bytes: usize,
) -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>> {
    crate::pipeline::runtime_checkpoint(task_context)?;
    Ok(Box::new(QueryGraphReadAllocation {
        allocation: account.reserve(bytes)?,
        task_context: task_context.cloned(),
        retained_state: account.retained_state().cloned(),
    }))
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

struct LocalGraphReadAllocation {
    bytes: usize,
    limit: usize,
    task_context: Option<hawdb_core::RuntimeTaskContext>,
}

impl hawdb_storage::read_view::GraphReadAllocation for LocalGraphReadAllocation {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn grow(&mut self, bytes: usize) -> Result<()> {
        crate::pipeline::runtime_checkpoint(self.task_context.as_ref())?;
        let next = self
            .bytes
            .checked_add(bytes)
            .filter(|&next| next <= self.limit)
            .ok_or_else(|| {
                hawdb_core::HawDBError::Execution(
                    "graph read exceeds blocking_operator_bytes".into(),
                )
            })?;
        self.bytes = next;
        Ok(())
    }
}

impl AdjacencyReadMemory<'_> {
    /// Standalone traversal keeps its explicit local bound even without a
    /// query ledger. Query callers additionally own the admitted source lease.
    pub(crate) fn admit_node(
        self,
        bytes: usize,
        related_bytes: usize,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>> {
        crate::pipeline::runtime_checkpoint(task_context)?;
        let limit = self
            .budget_bytes
            .checked_sub(related_bytes)
            .ok_or_else(|| {
                hawdb_core::HawDBError::Execution(
                    "graph read exceeds blocking_operator_bytes".into(),
                )
            })?;
        if bytes > limit {
            return Err(hawdb_core::HawDBError::Execution(format!(
                "graph read uses {} bytes, exceeding blocking_operator_bytes {}",
                bytes.saturating_add(related_bytes),
                self.budget_bytes,
            )));
        }
        if let Some(account) = self.account {
            admit_graph_read(account, task_context, bytes)
        } else {
            Ok(Box::new(LocalGraphReadAllocation {
                bytes,
                limit,
                task_context: task_context.cloned(),
            }))
        }
    }
}

/// The graph reads required by storage-independent execution operators.
///
/// Implementations own storage layout, residency, and pruning details. Owned
/// records keep executor state independent from storage guard lifetimes and
/// make memory accounting deterministic at the operator boundary.
pub trait GraphExecutionRead {
    /// Query-owned files outside the store directory use the same project cap.
    #[doc(hidden)]
    fn file_descriptor_context(&self) -> Option<hawdb_storage::file_descriptors::FileOpenContext> {
        None
    }

    fn is_out_of_core(&self) -> bool;

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>>;

    /// Full point ownership with optional label pruning and a transferable
    /// before-copy permit. Unsupported readers fail closed.
    fn node_with_allocation(
        &self,
        _id: NodeId,
        _label_ids: Option<&[LabelId]>,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
    ) -> Result<hawdb_storage::read_view::AdmittedNodeRead> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted full node point reads".into(),
        ))
    }

    /// Full relationship point ownership with a transferable before-copy permit.
    fn relationship_with_allocation(
        &self,
        _id: hawdb_storage::RelId,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
    ) -> Result<hawdb_storage::read_view::AdmittedRelationshipRead> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted relationship point reads".into(),
        ))
    }

    /// Selected-column point read. Implementations must call admission before
    /// allocating the owned result, and propagate rejection without decoding.
    fn projected_node_owned_admitted(
        &self,
        _id: NodeId,
        _required_properties: &BTreeSet<String>,
        _admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<ProjectedNodeRecord>> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted projected node reads".into(),
        ))
    }

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

    /// Full-record admission before cloning/decoding with transferable
    /// ownership and pre-decode normal Stop. Unsupported readers fail closed.
    fn visit_nodes_with_allocation(
        &self,
        _label_id: Option<LabelId>,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        _consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedNodeRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support transferable full node scan allocations".into(),
        ))
    }

    /// Indexed full-record ownership must be admitted before payload copying.
    fn visit_nodes_by_access_with_allocation(
        &self,
        _label_id: LabelId,
        _access: &NodeProjectionAccess,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        _consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedNodeRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted full node index reads".into(),
        ))
    }

    /// Visits every live relationship, including persisted base records and
    /// uncheckpointed changes, in either residency mode. Stop and callback
    /// errors terminate the scan without further consumer calls.
    fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    /// Full relationship admission is transferable and precedes payload ownership.
    fn visit_relationships_with_allocation(
        &self,
        _rel_type: Option<RelTypeId>,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        _consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted relationship scans".into(),
        ))
    }

    /// Admit owned input before cloning/decoding; retain its allocation through
    /// the consumer call. An unsupported reader must refuse this contract.
    fn visit_projected_nodes_admitted(
        &self,
        _label_id: Option<LabelId>,
        _properties: &BTreeSet<String>,
        _admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        _consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted projected scans".into(),
        ))
    }

    /// Transfer the before-ownership permit to buffered consumers. Admission
    /// may request normal Stop before the next selected payload is owned.
    fn visit_projected_nodes_with_allocation(
        &self,
        _label_id: Option<LabelId>,
        _properties: &BTreeSet<String>,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        _consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedProjectedNode,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support transferable projected scan allocations".into(),
        ))
    }

    /// Admit owned input before cloning/decoding; retain its allocation through
    /// the consumer call. An unsupported reader must refuse this contract.
    fn visit_projected_nodes_by_access_admitted(
        &self,
        _label_id: LabelId,
        _access: &NodeProjectionAccess,
        _properties: &BTreeSet<String>,
        _admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        _consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted projected scans".into(),
        ))
    }

    /// Admit owned input before cloning/decoding; retain its allocation through
    /// the consumer call. An unsupported reader must refuse this contract.
    fn visit_projected_nodes_by_property_admitted(
        &self,
        _label_id: LabelId,
        _property: &str,
        _values: &[hawdb_core::Value],
        _properties: &BTreeSet<String>,
        _admit: &mut dyn FnMut(
            usize,
        )
            -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
        _consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted projected scans".into(),
        ))
    }

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
        memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        // The transferable source fails closed for unsupported readers. Never
        // delegate a budgeted read to a legacy visitor that owns values first.
        self.visit_ordered_adjacent_relationships_with_allocation(
            node_id,
            rel_type,
            direction,
            memory,
            &mut |bytes| memory.admit_node(bytes, 0, None).map(Some),
            &mut |input| {
                let (relationship, _allocation) = input.into_parts();
                consumer(relationship)
            },
        )
    }

    /// Ordered full relationship sources retain the permit with buffered rows.
    /// Unsupported readers must not fall back to copying unadmitted properties.
    fn visit_ordered_adjacent_relationships_with_allocation(
        &self,
        _node_id: NodeId,
        _rel_type: Option<RelTypeId>,
        _direction: AdjacencyDirection,
        _memory: AdjacencyReadMemory<'_>,
        _admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        _consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        Err(hawdb_core::HawDBError::Execution(
            "storage reader does not support admitted ordered relationship reads".into(),
        ))
    }

    /// Filtered ordered sources preserve the allocation and live task at the
    /// source boundary, including records rejected by the predicate.
    fn visit_filtered_ordered_relationships_with_allocation(
        &self,
        adjacency: (NodeId, Option<RelTypeId>, AdjacencyDirection),
        filter: &PropertyFilter,
        memory: AdjacencyReadMemory<'_>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        self.visit_ordered_adjacent_relationships_with_allocation(
            adjacency.0,
            adjacency.1,
            adjacency.2,
            memory,
            admit,
            &mut |input| {
                let relationship = input.relationship();
                if crate::predicate::property_filter_matches_values(
                    filter,
                    relationship.id.0,
                    &relationship.properties,
                ) {
                    consumer(input)
                } else {
                    Ok(ScanControl::Continue)
                }
            },
        )
        .map(|control| (control, None))
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
        memory: AdjacencyReadMemory<'_>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        self.visit_ordered_adjacent_relationships_owned(
            node_id,
            rel_type,
            direction,
            memory,
            &mut |relationship| {
                if crate::predicate::property_filter_matches_values(
                    filter,
                    relationship.id.0,
                    &relationship.properties,
                ) {
                    consumer(relationship)
                } else {
                    Ok(ScanControl::Continue)
                }
            },
        )
        .map(|control| (control, None))
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

    /// Every candidate collection and the retained reference vector must be
    /// admitted before allocation. Unsupported stores fail closed.
    fn scan_nodes_with_filter_pruning_admitted<'a>(
        &'a self,
        _catalog: &Catalog,
        _label_id: Option<LabelId>,
        _filter: Option<&PropertyFilter>,
        _admit: &mut hawdb_storage::read_view::GraphReadAllocator<'_>,
    ) -> Result<PrunedNodeScan<'a>> {
        Err(hawdb_core::HawDBError::Execution(
            "graph store does not support admitted pruning reads".into(),
        ))
    }
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

    fn commit_mutations_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
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
