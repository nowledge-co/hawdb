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

//! Only the three reads used by projection execution are implemented.
use super::*;
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use hawdb_plan::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{AdjacencyDirection, ProjectedNodeRecord, ScanPredicate, ScanPruningReport};

impl GraphExecutionRead for Fixture {
    fn is_out_of_core(&self) -> bool {
        panic!("unexpected graph execution read: is_out_of_core")
    }
    fn node_count_for_label(&self, _: Option<LabelId>) -> usize {
        panic!("unexpected graph execution read: node_count_for_label")
    }
    fn relationship_count_for_type(&self, _: Option<RelTypeId>) -> usize {
        panic!("unexpected graph execution read: relationship_count_for_type")
    }
    fn node_owned(&self, _id: NodeId) -> Result<Option<NodeRecord>> {
        panic!("unexpected graph execution read: node_owned")
    }
    fn visit_adjacent_relationships_owned(
        &self,
        _node_id: NodeId,
        _: Option<RelTypeId>,
        _direction: AdjacencyDirection,
        _consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected graph execution read: visit_adjacent_relationships_owned")
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
