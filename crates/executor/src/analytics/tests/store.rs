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

//! Only the reads used by projection and result-identity execution are implemented.
use super::*;
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use hawdb_plan_cypher::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{
    adjacency::AdjacencyDirection,
    scan::{ScanPredicate, ScanPruningReport},
    ProjectedNodeRecord,
};

impl GraphExecutionRead for Fixture {
    fn visit_nodes_with_allocation(
        &self,
        label: Option<LabelId>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedNodeRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.node_scans.set(self.node_scans.get() + 1);
        for (index, node) in self.nodes.iter().enumerate() {
            if self.fail_node_at == Some(index) {
                return Err(HawDBError::StorageIntegrity("node scan sentinel".into()));
            }
            if label.is_some_and(|selected| !node.labels.contains(&selected)) {
                continue;
            }
            if let Some((at, token)) = &self.cancel_node_at
                && *at == index
            {
                token.cancel();
            }
            let Some(allocation) = admit(hawdb_core::ids::node_allocation_bytes(node))? else {
                return Ok(ScanControl::Stop);
            };
            self.node_visits.set(self.node_visits.get() + 1);
            if consumer(
                hawdb_storage::read_view::AdmittedNodeRecord::clone_admitted(node, allocation)?,
            )? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        if let Some(token) = &self.cancel_after_nodes {
            token.cancel();
        }
        Ok(ScanControl::Continue)
    }
    fn visit_projected_nodes_with_allocation(
        &self,
        label: Option<LabelId>,
        properties: &BTreeSet<String>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedProjectedNode,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.node_scans.set(self.node_scans.get() + 1);
        for (index, node) in self.nodes.iter().enumerate() {
            if self.fail_node_at == Some(index) {
                return Err(HawDBError::StorageIntegrity("node scan sentinel".into()));
            }
            if label.is_some_and(|selected| !node.labels.contains(&selected)) {
                continue;
            }
            if let Some((at, token)) = &self.cancel_node_at
                && *at == index
            {
                token.cancel();
            }
            let Some(allocation) = admit(hawdb_core::ids::projected_node_allocation_bytes(
                node, properties,
            ))?
            else {
                return Ok(ScanControl::Stop);
            };
            self.node_visits.set(self.node_visits.get() + 1);
            if consumer(
                hawdb_storage::read_view::AdmittedProjectedNode::clone_admitted(
                    node, properties, allocation,
                )?,
            )? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        if let Some(token) = &self.cancel_after_nodes {
            token.cancel();
        }
        Ok(ScanControl::Continue)
    }
    fn visit_relationships_with_allocation(
        &self,
        rel_type: Option<RelTypeId>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        self.rel_scans.set(self.rel_scans.get() + 1);
        if self.fail_rel_scan == Some(self.rel_scans.get()) {
            return Err(HawDBError::StorageIntegrity(
                "relationship scan sentinel".into(),
            ));
        }
        for record in &self.relationships {
            if rel_type.is_some_and(|selected| record.rel_type != selected) {
                continue;
            }
            let Some(allocation) = admit(hawdb_core::ids::relationship_allocation_bytes(record))?
            else {
                return Ok(ScanControl::Stop);
            };
            self.rel_visits.set(self.rel_visits.get() + 1);
            if consumer(
                hawdb_storage::read_view::AdmittedRelationshipRecord::clone_admitted(
                    record, allocation,
                )?,
            )? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn visit_ordered_adjacent_relationships_with_allocation(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        _: crate::store::AdjacencyReadMemory<'_>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
        consumer: &mut dyn FnMut(
            hawdb_storage::read_view::AdmittedRelationshipRecord,
        ) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for record in &self.relationships {
            let endpoint = match direction {
                AdjacencyDirection::Outgoing => record.source,
                AdjacencyDirection::Incoming => record.target,
            };
            if endpoint != node_id || rel_type.is_some_and(|selected| record.rel_type != selected) {
                continue;
            }
            let ordinal = self.adjacency_visits.get();
            if self.fail_adjacency_at == Some(ordinal) {
                return Err(HawDBError::StorageIntegrity(
                    "adjacency scan sentinel".into(),
                ));
            }
            if let Some((at, token)) = &self.cancel_adjacency_at
                && *at == ordinal
            {
                token.cancel();
            }
            let Some(allocation) = admit(hawdb_core::ids::relationship_allocation_bytes(record))?
            else {
                return Ok(ScanControl::Stop);
            };
            self.adjacency_visits.set(ordinal + 1);
            if consumer(
                hawdb_storage::read_view::AdmittedRelationshipRecord::clone_admitted(
                    record, allocation,
                )?,
            )? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn is_out_of_core(&self) -> bool {
        panic!("unexpected graph execution read: is_out_of_core")
    }
    fn node_count_for_label(&self, _: Option<LabelId>) -> usize {
        panic!("unexpected graph execution read: node_count_for_label")
    }
    fn relationship_count_for_type(&self, _: Option<RelTypeId>) -> usize {
        panic!("unexpected graph execution read: relationship_count_for_type")
    }
    fn projected_node_owned_admitted(
        &self,
        id: NodeId,
        required_properties: &BTreeSet<String>,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<ProjectedNodeRecord>> {
        if self.fail_identity_node == Some(id) {
            return Err(HawDBError::StorageIntegrity(
                "identity lookup sentinel".into(),
            ));
        }
        let Some(node) = self.nodes.iter().find(|node| node.id == id) else {
            return Ok(None);
        };
        admit(hawdb_core::ids::projected_node_allocation_bytes(
            node,
            required_properties,
        ))?;
        Ok(Some(hawdb_core::ids::project_node_record_ref(
            node,
            required_properties,
        )))
    }

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        if self.fail_identity_node == Some(id) {
            return Err(HawDBError::StorageIntegrity(
                "identity lookup sentinel".into(),
            ));
        }
        Ok(self.nodes.iter().find(|node| node.id == id).cloned())
    }
    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for relationship in &self.relationships {
            let endpoint = match direction {
                AdjacencyDirection::Outgoing => relationship.source,
                AdjacencyDirection::Incoming => relationship.target,
            };
            if endpoint != node_id
                || rel_type.is_some_and(|selected| relationship.rel_type != selected)
            {
                continue;
            }
            let ordinal = self.adjacency_visits.get();
            if self.fail_adjacency_at == Some(ordinal) {
                return Err(HawDBError::StorageIntegrity(
                    "adjacency scan sentinel".into(),
                ));
            }
            self.adjacency_visits.set(ordinal + 1);
            if let Some((at, token)) = &self.cancel_adjacency_at
                && *at == ordinal
            {
                token.cancel();
            }
            if consumer(relationship.clone())? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn scan_nodes_borrowed<'a>(
        &'a self,
        _: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        panic!("unexpected graph execution read: scan_nodes_borrowed")
    }
    fn visit_nodes_owned(
        &self,
        label: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        assert!(label.is_none());
        self.node_scans.set(self.node_scans.get() + 1);
        for (index, node) in self.nodes.iter().enumerate() {
            if self.fail_node_at == Some(index) {
                return Err(HawDBError::StorageIntegrity("node scan sentinel".into()));
            }
            if let Some((at, token)) = &self.cancel_node_at
                && *at == index
            {
                token.cancel();
            }
            self.node_visits.set(self.node_visits.get() + 1);
            if consumer(node.clone())? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        if let Some(token) = &self.cancel_after_nodes {
            token.cancel();
        }
        Ok(ScanControl::Continue)
    }
    fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        assert!(rel_type.is_none());
        self.rel_scans.set(self.rel_scans.get() + 1);
        if self.fail_rel_scan == Some(self.rel_scans.get()) {
            return Err(HawDBError::StorageIntegrity(
                "relationship scan sentinel".into(),
            ));
        }
        for relationship in &self.relationships {
            self.rel_visits.set(self.rel_visits.get() + 1);
            if consumer(relationship.clone())? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn visit_projected_nodes_by_access_owned(
        &self,
        _: LabelId,
        _: &NodeProjectionAccess,
        _: &BTreeSet<String>,
        _: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_projected_nodes_by_access_owned")
    }
    fn visit_nodes_by_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &[Value],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_nodes_by_property_owned")
    }
    fn visit_nodes_by_composite_property_owned(
        &self,
        _: LabelId,
        _: &[(String, Value)],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_nodes_by_composite_property_owned")
    }
    fn visit_nodes_by_composite_range_owned(
        &self,
        _: LabelId,
        _: &CompositeRangeSeek,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_nodes_by_composite_range_owned")
    }
    fn visit_nodes_by_property_range_owned(
        &self,
        _: LabelId,
        _: &str,
        _: Option<&(Value, bool)>,
        _: Option<&(Value, bool)>,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_nodes_by_property_range_owned")
    }
    fn visit_nodes_by_full_text_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &str,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_nodes_by_full_text_property_owned")
    }
    fn projected_graph_definition(&self, name: &str) -> Option<ProjectedGraphDefinition> {
        assert_eq!(name, "graph");
        self.definition.clone()
    }
    fn visit_source_scan_candidates(
        &self,
        _: &ScanPredicate,
        _: SourceScanReadLimits,
        _: Option<&RuntimeTaskContext>,
        _: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        panic!("unexpected graph execution read: visit_source_scan_candidates")
    }
    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        _: NodeId,
        _: Option<RelTypeId>,
        _: AdjacencyDirection,
        _: &PropertyFilter,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        panic!("unexpected graph execution read: visit_adjacent_relationships_with_filter_owned")
    }
    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        _: Option<RelTypeId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        panic!("projection must use the residency-neutral owned relationship visitor")
    }
    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        _: &Catalog,
        _: Option<LabelId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>> {
        panic!("unexpected graph execution read: scan_nodes_with_filter_pruning")
    }
}
