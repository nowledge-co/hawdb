use super::*;
use crate::observer::NoopExecutionObserver;
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use skein_plan::{CompositeRangeSeek, NodeProjectionAccess};
use skein_storage::{
    ProjectedGraphDefinition, ProjectedNodeRecord, RelId, ScanPredicate, ScanPruningReport,
};
use std::cell::Cell;
use std::collections::BTreeSet;

struct ChainStore {
    context: RuntimeTaskContext,
    visits: Cell<usize>,
    stop_after: usize,
    failure: bool,
    panic: bool,
}

impl GraphExecutionRead for ChainStore {
    fn is_out_of_core(&self) -> bool {
        false
    }
    fn node_count_for_label(&self, _: Option<LabelId>) -> usize {
        6
    }
    fn relationship_count_for_type(&self, _: Option<RelTypeId>) -> usize {
        5
    }
    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        Ok(Some(NodeRecord {
            id,
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        }))
    }
    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        _: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        assert_eq!(direction, AdjacencyDirection::Outgoing);
        self.visits.set(self.visits.get() + 1);
        let control = consumer(RelRecord {
            id: RelId(node_id.0),
            source: node_id,
            target: NodeId(node_id.0 + 1),
            rel_type: RelTypeId(0),
            properties: BTreeMap::new(),
        })?;
        // Cancel after the last callback in a level, not from inside it. The
        // next level/materialization must notice before another storage read.
        if self.visits.get() == self.stop_after {
            assert!(!self.panic, "injected adjacency panic");
            if self.failure {
                return Err(SkeinError::Execution(
                    "injected adjacency failure".to_string(),
                ));
            }
            self.context.cancellation().cancel();
        }
        Ok(control)
    }
    fn scan_nodes_borrowed<'a>(
        &'a self,
        _: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        unreachable!()
    }
    fn visit_nodes_owned(
        &self,
        _: Option<LabelId>,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_projected_nodes_by_access_owned(
        &self,
        _: LabelId,
        _: &NodeProjectionAccess,
        _: &BTreeSet<String>,
        _: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_nodes_by_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &[Value],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_nodes_by_composite_property_owned(
        &self,
        _: LabelId,
        _: &[(String, Value)],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_nodes_by_composite_range_owned(
        &self,
        _: LabelId,
        _: &CompositeRangeSeek,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_nodes_by_property_range_owned(
        &self,
        _: LabelId,
        _: &str,
        _: Option<&(Value, bool)>,
        _: Option<&(Value, bool)>,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn visit_nodes_by_full_text_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &str,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!()
    }
    fn projected_graph_definition(&self, _: &str) -> Option<ProjectedGraphDefinition> {
        unreachable!()
    }
    fn visit_source_scan_candidates(
        &self,
        _: &ScanPredicate,
        _: SourceScanReadLimits,
        _: Option<&RuntimeTaskContext>,
        _: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        unreachable!()
    }
    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        _: NodeId,
        _: Option<RelTypeId>,
        _: AdjacencyDirection,
        _: &PropertyFilter,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        unreachable!()
    }
    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        _: Option<RelTypeId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        unreachable!()
    }
    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        _: &Catalog,
        _: Option<LabelId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>> {
        unreachable!()
    }
}

fn search() -> ShortestPathSearch<'static> {
    ShortestPathSearch {
        source: NodeId(0),
        target: NodeId(5),
        rel_type_id: None,
        direction: RelationshipDirection::Outgoing,
        min_hops: 1,
        max_hops: 5,
        path_node_visibility_filter: None,
    }
}

#[test]
fn level_boundary_cancellation_and_storage_failures_release_all_state() {
    for stop_after in 1..=5 {
        for mode in 0..3 {
            let budget = NonZeroUsize::new(64 * 1024).unwrap();
            let ledger = QueryMemoryLedger::new(budget);
            let account = ledger.account(
                QueryMemoryClass::BlockingState,
                "shortest path test",
                budget,
            );
            let store = ChainStore {
                context: RuntimeTaskContext::default(),
                visits: Cell::new(0),
                stop_after,
                failure: mode == 1,
                panic: mode == 2,
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                search_shortest_paths(
                    &store,
                    search(),
                    budget,
                    10,
                    account,
                    Some(&store.context),
                    &NoopExecutionObserver,
                )
            }));
            if mode == 2 {
                assert!(result.is_err());
            } else {
                let Err(error) = result.unwrap() else {
                    panic!("expected an interrupted search")
                };
                assert!(error.to_string().contains(if mode == 1 {
                    "injected adjacency failure"
                } else {
                    "cancel"
                }));
            }
            assert_eq!(store.visits.get(), stop_after);
            assert_eq!(ledger.snapshot().used_bytes, 0);
            assert!(ledger.snapshot().peak_bytes > 0);
        }
    }
}

#[test]
fn backtracking_is_cancellable_after_output_has_started() {
    let budget = NonZeroUsize::new(64 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "shortest path test",
        budget,
    );
    let context = RuntimeTaskContext::default();
    let mut checkpoints = 0;
    {
        let mut tracker = OperatorMemoryTracker::with_account(budget, account);
        let mut dag = SearchDag::default();
        dag.node(NodeId(0), 0, false, &mut tracker).unwrap();
        dag.node(NodeId(5), 1, false, &mut tracker).unwrap();
        for _ in 0..3 {
            grow_vec(&mut dag.edges, &mut tracker).unwrap();
            dag.edges.push(1);
        }
        dag.nodes[0].successors = 0..3;
        let before = ledger.snapshot().used_bytes;
        // Marking uses five checkpoints. The sixth emits one path; the
        // seventh cancels with scratch and partially materialized output live.
        let error = dag
            .materialize(&search(), 1, 3, &mut tracker, || {
                checkpoints += 1;
                if checkpoints == 7 {
                    assert!(ledger.snapshot().used_bytes > before);
                    context.cancellation().cancel();
                }
                runtime_checkpoint(Some(&context))
            })
            .unwrap_err();
        assert!(error.to_string().contains("cancel"));
    }
    assert_eq!(checkpoints, 7);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn exhausted_output_budget_returns_no_partial_paths() {
    let budget = NonZeroUsize::new(4096).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    {
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "shortest path test",
            budget,
        );
        let mut tracker = OperatorMemoryTracker::with_account(budget, account);
        let mut dag = SearchDag::default();
        dag.node(NodeId(0), 0, false, &mut tracker).unwrap();
        dag.node(NodeId(5), 1, false, &mut tracker).unwrap();
        for _ in 0..64 {
            grow_vec(&mut dag.edges, &mut tracker).unwrap();
            dag.edges.push(1);
        }
        dag.nodes[0].successors = 0..64;
        let error = dag
            .materialize(&search(), 1, usize::MAX, &mut tracker, || Ok(()))
            .unwrap_err();
        assert!(error.to_string().contains("blocking_operator_bytes"));
    }
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(ledger.snapshot().peak_bytes <= budget.get());
}

#[test]
fn vector_growth_reserves_old_and_new_allocations_together() {
    let budget = NonZeroUsize::new(size_of::<[usize; 8]>()).unwrap();
    let mut tracker = OperatorMemoryTracker::new(budget);
    let mut values = Vec::new();
    grow_vec(&mut values, &mut tracker).unwrap();
    values.extend([0usize, 1, 2, 3]);
    // The retained eight-element allocation fits, but overlapping old/new
    // allocations do not. Rejection must leave the original vector intact.
    assert!(grow_vec(&mut values, &mut tracker).is_err());
    assert_eq!(values, [0, 1, 2, 3]);
    assert_eq!(tracker.used_bytes, 4 * size_of::<usize>());
}
