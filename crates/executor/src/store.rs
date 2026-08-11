//! Storage-neutral graph read contract used by execution operators.

use skein_core::{Catalog, LabelId, RelTypeId, Result};
use skein_storage::{
    AdjacencyDirection, NodeId, NodeRecord, PropertyFilter, RelRecord, ScanPruningReport,
};

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

/// The graph reads required by storage-independent execution operators.
///
/// Implementations own storage layout, residency, and pruning details. Owned
/// records keep executor state independent from storage guard lifetimes and
/// make memory accounting deterministic at the operator boundary.
pub trait GraphExecutionRead {
    fn is_out_of_core(&self) -> bool;

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>>;

    fn node_count_for_label(&self, label_id: Option<LabelId>) -> usize;

    fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize;

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[skein_core::Value],
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl>;

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
