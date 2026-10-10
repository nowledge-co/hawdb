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

use super::*;
use crate::observer::NoopExecutionObserver;
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use hawdb_plan_cypher::{CompositeRangeSeek, NodeProjectionAccess};
use hawdb_storage::{
    projection::ProjectedGraphDefinition,
    scan::{ScanPredicate, ScanPruningReport},
    ProjectedNodeRecord, RelId,
};
use std::cell::Cell;
use std::collections::BTreeSet;

struct ChainStore {
    context: RuntimeTaskContext,
    visits: Cell<usize>,
    stop_after: usize,
    failure: bool,
    panic: bool,
    cancel_before_admission: bool,
    output_node: Option<NodeRecord>,
    node_copies: Cell<usize>,
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
        self.node_copies.set(self.node_copies.get() + 1);
        Ok(Some(self.output_node.clone().unwrap_or(NodeRecord {
            id,
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        })))
    }
    fn node_with_allocation(
        &self,
        id: NodeId,
        label_ids: Option<&[hawdb_core::LabelId]>,
        admit: &mut hawdb_storage::read_view::ControlledGraphReadAllocator<'_>,
    ) -> Result<hawdb_storage::read_view::AdmittedNodeRead> {
        use hawdb_storage::read_view::{AdmittedNodeRead, AdmittedNodeRecord};
        let empty = NodeRecord {
            id,
            labels: BTreeSet::new(),
            properties: BTreeMap::new(),
        };
        let node = self.output_node.as_ref().unwrap_or(&empty);
        if !crate::predicate::node_matches_label_pattern(node, label_ids) {
            return Ok(AdmittedNodeRead::Missing);
        }
        let Some(allocation) = admit(hawdb_core::ids::node_allocation_bytes(node))? else {
            return Ok(AdmittedNodeRead::Stopped);
        };
        self.node_copies.set(self.node_copies.get() + 1);
        AdmittedNodeRecord::clone_admitted(node, allocation).map(AdmittedNodeRead::Node)
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
                return Err(HawDBError::Execution(
                    "injected adjacency failure".to_string(),
                ));
            }
            self.context.cancellation().cancel();
        }
        Ok(control)
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
        let _ = memory;
        if self.cancel_before_admission {
            self.context.cancellation().cancel();
        }
        let Some(_source) = admit(2048)? else {
            return Ok(ScanControl::Stop);
        };
        self.visit_adjacent_relationships_owned(node_id, rel_type, direction, &mut |relationship| {
            let Some(allocation) = admit(hawdb_core::ids::relationship_allocation_bytes(
                &relationship,
            ))?
            else {
                return Ok(ScanControl::Stop);
            };
            consumer(
                hawdb_storage::read_view::AdmittedRelationshipRecord::clone_admitted(
                    &relationship,
                    allocation,
                )?,
            )
        })
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
    fn visit_relationships_owned(
        &self,
        _: Option<RelTypeId>,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
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
                cancel_before_admission: false,
                output_node: None,
                node_copies: Cell::new(0),
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

#[test]
fn shortest_path_length_result_accounts_shared_binding_footprint() {
    let budget = NonZeroUsize::new(4096).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "length guard", budget);
    let mut tracker = OperatorMemoryTracker::with_account(budget, account.clone());
    tracker.try_charge(std::mem::size_of::<Binding>()).unwrap();
    let binding = shortest_path_binding(
        &hawdb_storage::store::GraphStore::default(),
        &[NodeId(1), NodeId(2)],
        &[ShortestPathProjection {
            name: "length".into(),
            expression: ShortestPathProjectionExpression::Length,
        }],
        &mut tracker,
        &account,
        None,
    )
    .unwrap();
    assert_eq!(tracker.used_bytes, binding_memory_bytes(&binding));
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn shortest_path_length_result_refuses_before_retained_budget_overflow() {
    let expected = Binding {
        values: BTreeMap::from([("length".into(), Value::Int(1))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    };
    let budget = NonZeroUsize::new(binding_memory_bytes(&expected) - 1).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(QueryMemoryClass::BlockingState, "length guard", budget);
    let mut tracker = OperatorMemoryTracker::with_account(budget, account.clone());
    tracker.try_charge(std::mem::size_of::<Binding>()).unwrap();
    let result = shortest_path_binding(
        &hawdb_storage::store::GraphStore::default(),
        &[NodeId(1), NodeId(2)],
        &[ShortestPathProjection {
            name: "length".into(),
            expression: ShortestPathProjectionExpression::Length,
        }],
        &mut tracker,
        &account,
        None,
    );
    assert!(result.is_err());
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn shortest_path_output_refuses_native_point_copy_before_allocation() {
    let budget = NonZeroUsize::new(4096).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "output source guard",
        budget,
    );
    let mut tracker = OperatorMemoryTracker::with_account(budget, account.clone());
    let store = ChainStore {
        context: RuntimeTaskContext::default(),
        visits: Cell::new(0),
        stop_after: usize::MAX,
        failure: false,
        panic: false,
        cancel_before_admission: false,
        output_node: Some(NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([(
                "body".into(),
                Value::String("x".repeat(1024 * 1024 + 137)),
            )]),
        }),
        node_copies: Cell::new(0),
    };
    let result = shortest_path_binding(
        &store,
        &[NodeId(1)],
        &[ShortestPathProjection {
            name: "body".into(),
            expression: ShortestPathProjectionExpression::NodePropertyList {
                property: "body".into(),
            },
        }],
        &mut tracker,
        &account,
        None,
    );
    assert!(result.is_err());
    assert_eq!(
        store.node_copies.get(),
        0,
        "output cloned native point before source admission"
    );
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn shortest_path_forwards_cancellation_to_source_admission() {
    let budget = NonZeroUsize::new(64 * 1024).unwrap();
    let ledger = QueryMemoryLedger::new(budget);
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "cancellation source guard",
        budget,
    );
    let store = ChainStore {
        context: RuntimeTaskContext::default(),
        visits: Cell::new(0),
        stop_after: usize::MAX,
        failure: false,
        panic: false,
        cancel_before_admission: true,
        output_node: None,
        node_copies: Cell::new(0),
    };
    let result = search_shortest_paths(
        &store,
        search(),
        budget,
        10,
        account,
        Some(&store.context),
        &NoopExecutionObserver,
    );
    assert!(result.is_err());
    assert_eq!(
        store.visits.get(),
        0,
        "cancelled source copied an adjacency record"
    );
    assert_eq!(store.node_copies.get(), 0);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}
