//! Only the three reads used by projection execution are implemented.
use super::*;
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use skein_plan::{CompositeRangeSeek, NodeProjectionAccess};
use skein_storage::{AdjacencyDirection, ProjectedNodeRecord, ScanPredicate, ScanPruningReport};

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
                return Err(SkeinError::StorageIntegrity("node scan sentinel".into()));
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
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        assert!(rel_type.is_none() && filter.is_none());
        self.rel_scans.set(self.rel_scans.get() + 1);
        if self.fail_rel_scan == Some(self.rel_scans.get()) {
            return Err(SkeinError::StorageIntegrity(
                "relationship scan sentinel".into(),
            ));
        }
        Ok(PrunedRelationshipScan {
            relationships: Box::new(self.relationships.iter().cloned()),
            report: ScanPruningReport {
                target_kind: skein_storage::ScanPruningTargetKind::Relationship,
                label_id: None,
                rel_type_id: None,
                strategy: skein_storage::ScanPruningStrategy::FullLabelScan,
                pruned: false,
                exact_empty: self.relationships.is_empty(),
                candidate_count_before_pruning: self.relationships.len(),
                pruned_candidate_count: 0,
                candidate_count_before_filter: self.relationships.len(),
                output_count: self.relationships.len(),
                filtered_out_count: 0,
            },
        })
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
