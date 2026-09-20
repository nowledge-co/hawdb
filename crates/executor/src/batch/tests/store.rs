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

//! Fail-closed read fixture: a test must explicitly implement every storage read it needs.

use super::*;
use crate::predicate::node_matches_property_filter;
use crate::store::{
    GraphExecutionRead, PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow,
    SourceScanCandidateVisit, SourceScanReadLimits,
};
use hawdb_core::{LabelId, RelTypeId};
use hawdb_plan::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{
    AdjacencyDirection, ProjectedGraphDefinition, ProjectedNodeRecord, RelRecord, ScanPredicate,
};
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct ReadFixture {
    pub(super) nodes: Vec<NodeRecord>,
    pub(super) relationships: Vec<RelRecord>,
    pub(super) definition: Option<ProjectedGraphDefinition>,
    pub(super) out_of_core: bool,
    pub(super) source_candidates: Option<Vec<SourceScanCandidateRow>>,
    pub(super) source_reads: Cell<usize>,
    pub(super) adjacency_cancellation: Option<hawdb_core::RuntimeCancellationToken>,
}

impl GraphExecutionRead for ReadFixture {
    fn is_out_of_core(&self) -> bool {
        self.out_of_core
    }
    fn node_count_for_label(&self, label: Option<LabelId>) -> usize {
        self.nodes
            .iter()
            .filter(|node| label.is_none_or(|label| node.labels.contains(&label)))
            .count()
    }
    fn relationship_count_for_type(&self, _: Option<RelTypeId>) -> usize {
        panic!("unexpected batch test store read: relationship_count_for_type")
    }
    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
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
            if let Some(token) = &self.adjacency_cancellation {
                token.cancel();
            }
            let adjacent = match direction {
                AdjacencyDirection::Outgoing => relationship.source == node_id,
                AdjacencyDirection::Incoming => relationship.target == node_id,
            };
            if adjacent
                && rel_type.is_none_or(|id| relationship.rel_type == id)
                && consumer(relationship.clone())? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn scan_nodes_borrowed<'a>(
        &'a self,
        _: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        panic!("unexpected batch test store read: scan_nodes_borrowed")
    }
    fn visit_nodes_owned(
        &self,
        label: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for node in &self.nodes {
            if label.is_none_or(|label| node.labels.contains(&label))
                && consumer(node.clone())? == ScanControl::Stop
            {
                return Ok(ScanControl::Stop);
            }
        }
        Ok(ScanControl::Continue)
    }
    fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        for relationship in &self.relationships {
            if rel_type.is_none_or(|rel_type| relationship.rel_type == rel_type)
                && consumer(relationship.clone())? == ScanControl::Stop
            {
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
        panic!("unexpected batch test store read: visit_projected_nodes_by_access_owned")
    }
    fn visit_nodes_by_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &[Value],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected batch test store read: visit_nodes_by_property_owned")
    }
    fn visit_nodes_by_composite_property_owned(
        &self,
        _: LabelId,
        _: &[(String, Value)],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected batch test store read: visit_nodes_by_composite_property_owned")
    }
    fn visit_nodes_by_composite_range_owned(
        &self,
        _: LabelId,
        _: &CompositeRangeSeek,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected batch test store read: visit_nodes_by_composite_range_owned")
    }
    fn visit_nodes_by_property_range_owned(
        &self,
        _: LabelId,
        _: &str,
        _: Option<&(Value, bool)>,
        _: Option<&(Value, bool)>,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected batch test store read: visit_nodes_by_property_range_owned")
    }
    fn visit_nodes_by_full_text_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &str,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        panic!("unexpected batch test store read: visit_nodes_by_full_text_property_owned")
    }
    fn projected_graph_definition(&self, name: &str) -> Option<ProjectedGraphDefinition> {
        (name == "MemoryGraph")
            .then(|| self.definition.clone())
            .flatten()
    }
    fn visit_source_scan_candidates(
        &self,
        _: &ScanPredicate,
        limits: SourceScanReadLimits,
        _: Option<&RuntimeTaskContext>,
        consumer: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        let candidates = self
            .source_candidates
            .as_ref()
            .expect("unexpected source scan");
        assert_eq!(limits.io_depth.get(), SOURCE_SEGMENT_SCAN_IO_DEPTH);
        assert_eq!(
            limits.max_coalesced_bytes.get(),
            SOURCE_SEGMENT_SCAN_MAX_COALESCED_BYTES
        );
        assert_eq!(
            limits.max_wave_bytes.get(),
            SOURCE_SEGMENT_SCAN_MAX_WAVE_BYTES
        );
        self.source_reads.set(self.source_reads.get() + 1);
        for row in candidates {
            if consumer(row.clone())? == ScanControl::Stop {
                break;
            }
        }
        Ok(SourceScanCandidateVisit::Rows {
            graph_epoch: 1,
            skipped_segment_count: 0,
            candidate_count: candidates.len(),
            report: hawdb_storage::SegmentReadExecutionReport {
                wave_count: 1,
                range_count: 1,
                bytes_read: 1,
                max_wave_bytes_read: 1,
            },
        })
    }
    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        _: NodeId,
        _: Option<RelTypeId>,
        _: AdjacencyDirection,
        _: &PropertyFilter,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        panic!("unexpected batch test store read: visit_adjacent_relationships_with_filter_owned")
    }
    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        _: Option<RelTypeId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        panic!("unexpected batch test store read: scan_relationships_with_filter_pruning")
    }
    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        _: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>> {
        let candidate_count = self.nodes.len();
        let nodes: Vec<NodeRecord> = self
            .nodes
            .iter()
            .filter(|node| label_id.is_none_or(|label| node.labels.contains(&label)))
            .filter(|node| filter.is_none_or(|filter| node_matches_property_filter(node, filter)))
            .cloned()
            .collect();
        let output_count = nodes.len();
        Ok(PrunedNodeScan {
            nodes: Box::new(nodes.into_iter()),
            report: ScanPruningReport {
                target_kind: ScanPruningTargetKind::Node,
                label_id,
                rel_type_id: None,
                strategy: ScanPruningStrategy::FullLabelScan,
                pruned: false,
                exact_empty: output_count == 0,
                candidate_count_before_pruning: candidate_count,
                pruned_candidate_count: 0,
                candidate_count_before_filter: candidate_count,
                output_count,
                filtered_out_count: candidate_count.saturating_sub(output_count),
            },
        })
    }
}
