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
use crate::store::{
    PrunedNodeScan, PrunedRelationshipScan, SourceScanCandidateRow, SourceScanCandidateVisit,
    SourceScanReadLimits,
};
use hawdb_core::{LabelId, RelTypeId, RuntimeCancellationToken};
use hawdb_plan::{
    BatchMutationOperation, BatchMutationValue, CompositeRangeSeek, NodeProjectionAccess,
    PhysicalPlan, Projection, ProjectionExpression, SetAssignment, SetValue,
};
use hawdb_storage::{
    AdjacencyDirection, GraphMutation, MutationSummary, NodeRecord, ProjectedGraphDefinition,
    ProjectedNodeRecord, PropertyFilter, RelRecord, ScanPredicate, ScanPruningReport,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

#[derive(Debug, PartialEq, Eq)]
enum Write {
    Commit(Box<GraphMutation>),
    CommitBatch(Vec<GraphMutation>),
    Set(Vec<NodeId>, Vec<NodeSetAssignment>),
    Delete(Vec<NodeId>, bool),
}

#[derive(Default)]
struct RecordingStore {
    nodes: Vec<NodeRecord>,
    writes: Vec<(Write, MutationLimits)>,
    scan_error_at: Option<usize>,
    cancel_at: Option<(usize, RuntimeCancellationToken)>,
    write_error: bool,
}

fn sentinel() -> HawDBError {
    HawDBError::Execution("injected storage error".into())
}

#[test]
fn unwind_mutations_preflight_every_row_and_publish_one_batch() {
    let operation = BatchMutationOperation::MergeNode {
        label: "Entity".into(),
        match_properties: BTreeMap::from([(
            "id".into(),
            BatchMutationValue::RowProperty("id".into()),
        )]),
        on_create_properties: BTreeMap::from([(
            "name".into(),
            BatchMutationValue::RowProperty("name".into()),
        )]),
    };
    let plan = PhysicalPlan::UnwindMutation {
        rows: vec![
            Value::Map(BTreeMap::from([
                ("id".into(), Value::String("one".into())),
                ("name".into(), Value::String("First".into())),
            ])),
            Value::Map(BTreeMap::from([
                ("id".into(), Value::String("two".into())),
                ("name".into(), Value::String("Second".into())),
            ])),
        ],
        variable: "row".into(),
        operation: operation.clone(),
    };
    let mut catalog = Catalog::default();
    let mut store = RecordingStore::default();
    let rows = execute_mutation_with_store(
        &plan,
        &mut catalog,
        &mut store,
        MutationLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        rows,
        vec![BTreeMap::from([("committed".into(), Value::Bool(true))])]
    );
    let [(write, _)] = store.writes.as_slice() else {
        panic!("expected exactly one storage write")
    };
    let Write::CommitBatch(mutations) = write else {
        panic!("expected one batch commit")
    };
    assert_eq!(mutations.len(), 2);
    assert!(matches!(
        &mutations[0],
        GraphMutation::MergeNode {
            label,
            match_properties,
            on_create_properties,
            ..
        } if label == "Entity"
            && match_properties.get("id") == Some(&Value::String("one".into()))
            && on_create_properties.get("name") == Some(&Value::String("First".into()))
    ));

    let invalid = PhysicalPlan::UnwindMutation {
        rows: vec![
            Value::Map(BTreeMap::from([("id".into(), Value::String("one".into()))])),
            Value::Int(2),
        ],
        variable: "row".into(),
        operation: operation.clone(),
    };
    let mut store = RecordingStore::default();
    let error = execute_mutation_with_store(
        &invalid,
        &mut Catalog::default(),
        &mut store,
        MutationLimits::default(),
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be a map"), "{error}");
    assert!(store.writes.is_empty());

    let input_over_budget = PhysicalPlan::UnwindMutation {
        rows: vec![Value::Map(BTreeMap::from([(
            "id".into(),
            Value::String("one".into()),
        )]))],
        variable: "row".into(),
        operation,
    };
    let mut store = RecordingStore::default();
    let error = execute_mutation_with_store(
        &input_over_budget,
        &mut Catalog::default(),
        &mut store,
        MutationLimits {
            max_result_payload_bytes: NonZeroUsize::new(1).unwrap(),
            ..MutationLimits::default()
        },
        None,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("UNWIND batch input would exceed"),
        "{error}"
    );
    assert!(store.writes.is_empty());
}

impl GraphExecutionRead for RecordingStore {
    fn is_out_of_core(&self) -> bool {
        false
    }

    fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        Ok(self.nodes.iter().find(|node| node.id == id).cloned())
    }

    fn visit_nodes_owned(
        &self,
        label: Option<LabelId>,
        consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        assert_eq!(label, None, "preflight owns label-pattern matching");
        for (index, node) in self.nodes.iter().enumerate() {
            if self.scan_error_at == Some(index) {
                return Err(sentinel());
            }
            if let Some((at, token)) = &self.cancel_at
                && *at == index
            {
                token.cancel();
            }
            if consumer(node.clone())? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        if self.scan_error_at == Some(self.nodes.len()) {
            return Err(sentinel());
        }
        if let Some((at, token)) = &self.cancel_at
            && *at == self.nodes.len()
        {
            token.cancel();
        }
        Ok(ScanControl::Continue)
    }

    fn node_count_for_label(&self, _: Option<LabelId>) -> usize {
        unreachable!("unexpected read in mutation preflight")
    }

    fn relationship_count_for_type(&self, _: Option<RelTypeId>) -> usize {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_relationships_owned(
        &self,
        _: Option<RelTypeId>,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_adjacent_relationships_owned(
        &self,
        _: NodeId,
        _: Option<RelTypeId>,
        _: AdjacencyDirection,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn scan_nodes_borrowed<'a>(
        &'a self,
        _: Option<LabelId>,
    ) -> Box<dyn Iterator<Item = &'a NodeRecord> + 'a> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_projected_nodes_by_access_owned(
        &self,
        _: LabelId,
        _: &NodeProjectionAccess,
        _: &BTreeSet<String>,
        _: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_nodes_by_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &[Value],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_nodes_by_composite_property_owned(
        &self,
        _: LabelId,
        _: &[(String, Value)],
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_nodes_by_composite_range_owned(
        &self,
        _: LabelId,
        _: &CompositeRangeSeek,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_nodes_by_property_range_owned(
        &self,
        _: LabelId,
        _: &str,
        _: Option<&(Value, bool)>,
        _: Option<&(Value, bool)>,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_nodes_by_full_text_property_owned(
        &self,
        _: LabelId,
        _: &str,
        _: &str,
        _: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn projected_graph_definition(&self, _: &str) -> Option<ProjectedGraphDefinition> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_source_scan_candidates(
        &self,
        _: &ScanPredicate,
        _: SourceScanReadLimits,
        _: Option<&RuntimeTaskContext>,
        _: &mut dyn FnMut(SourceScanCandidateRow) -> Result<ScanControl>,
    ) -> Result<SourceScanCandidateVisit> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn visit_adjacent_relationships_with_filter_owned(
        &self,
        _: NodeId,
        _: Option<RelTypeId>,
        _: AdjacencyDirection,
        _: &PropertyFilter,
        _: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
    ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        _: Option<RelTypeId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedRelationshipScan<'a>> {
        unreachable!("unexpected read in mutation preflight")
    }

    fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        _: &Catalog,
        _: Option<LabelId>,
        _: Option<&PropertyFilter>,
    ) -> Result<PrunedNodeScan<'a>> {
        unreachable!("unexpected read in mutation preflight")
    }
}

impl GraphExecutionWrite for RecordingStore {
    fn commit_mutation_with_limits(
        &mut self,
        _: &mut Catalog,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.writes
            .push((Write::Commit(Box::new(mutation)), limits));
        if self.write_error {
            return Err(sentinel());
        }
        Ok(MutationSummary {
            rows: vec![BTreeMap::from([("committed".into(), Value::Bool(true))])],
            relational_mutation_outcomes: vec![],
            append_mutation_outcomes: vec![],
        })
    }

    fn commit_mutations_with_limits(
        &mut self,
        _: &mut Catalog,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.writes.push((Write::CommitBatch(mutations), limits));
        if self.write_error {
            return Err(sentinel());
        }
        Ok(MutationSummary {
            rows: vec![BTreeMap::from([("committed".into(), Value::Bool(true))])],
            relational_mutation_outcomes: vec![],
            append_mutation_outcomes: vec![],
        })
    }

    fn set_node_properties_by_ids_with_limits(
        &mut self,
        _: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        self.writes
            .push((Write::Set(ids.to_vec(), assignments.to_vec()), limits));
        if self.write_error {
            return Err(sentinel());
        }
        Ok(ids.to_vec())
    }

    fn delete_node_ids_with_limits(
        &mut self,
        _: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        self.writes
            .push((Write::Delete(ids.to_vec(), detach), limits));
        if self.write_error {
            return Err(sentinel());
        }
        Ok(ids.to_vec())
    }
}

fn fixture(count: usize) -> (Catalog, RecordingStore) {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Item");
    let other = catalog.get_or_create_label("Other");
    let nodes = (0..count)
        .map(|index| NodeRecord {
            id: NodeId(index as u64),
            labels: BTreeSet::from([if index % 3 == 1 { other } else { label }]),
            properties: BTreeMap::from([("score".into(), Value::Int(index as i64))]),
        })
        .collect();
    (
        catalog,
        RecordingStore {
            nodes,
            ..RecordingStore::default()
        },
    )
}

fn assignments() -> Vec<SetAssignment> {
    vec![
        SetAssignment {
            property: "score".into(),
            value: SetValue::AddInt {
                property: "score".into(),
                amount: 3,
            },
        },
        SetAssignment {
            property: "score".into(),
            value: SetValue::AddInt {
                property: "score".into(),
                amount: 7,
            },
        },
    ]
}

fn return_mode(count: bool) -> SetNodePropertiesReturnMode {
    if count {
        SetNodePropertiesReturnMode::Count {
            name: "count".into(),
        }
    } else {
        SetNodePropertiesReturnMode::Project(vec![Projection {
            name: "score".into(),
            expression: ProjectionExpression::Property {
                variable: "n".into(),
                property: "score".into(),
            },
        }])
    }
}

fn plan(kind: usize) -> PhysicalPlan {
    match kind {
        0 => PhysicalPlan::SetNodeProperties {
            variable: "n".into(),
            label: "Item".into(),
            predicate: Some(Predicate::ConstantBool(true)),
            assignments: assignments(),
        },
        1 => PhysicalPlan::DeleteNode {
            variable: "n".into(),
            label: "Item".into(),
            predicate: Some(Predicate::ConstantBool(true)),
            detach: true,
        },
        2 | 3 => PhysicalPlan::SetNodePropertiesReturn {
            variable: "n".into(),
            label: "Item".into(),
            predicate: Some(Predicate::ConstantBool(true)),
            assignments: assignments(),
            returns: return_mode(kind == 3),
        },
        _ => unreachable!(),
    }
}

fn expected_rows(kind: usize, ids: &[NodeId]) -> Vec<Row> {
    if kind == 3 {
        vec![BTreeMap::from([(
            "count".into(),
            Value::Int(ids.len() as i64),
        )])]
    } else {
        ids.iter()
            .map(|id| {
                BTreeMap::from([(
                    if kind == 2 { "score" } else { "node_id" }.into(),
                    Value::Int(id.0 as i64 + if kind == 2 { 10 } else { 0 }),
                )])
            })
            .collect()
    }
}

fn expected_write(kind: usize, ids: Vec<NodeId>) -> Write {
    if kind == 1 {
        Write::Delete(ids, true)
    } else {
        Write::Set(
            ids,
            vec![
                NodeSetAssignment {
                    property: "score".into(),
                    value: hawdb_storage::NodeSetValue::AddInt { amount: 3 },
                },
                NodeSetAssignment {
                    property: "score".into(),
                    value: hawdb_storage::NodeSetValue::AddInt { amount: 7 },
                },
            ],
        )
    }
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value.max(1)).unwrap()
}

fn campaign(seeds: usize) -> usize {
    let mut cases = 0;
    for seed in 0..seeds {
        let count = seed.wrapping_mul(17) % 13;
        let ids = (0..count)
            .filter(|id| id % 3 != 1)
            .map(|id| NodeId(id as u64))
            .collect::<Vec<_>>();
        for kind in 0..4 {
            let rows = expected_rows(kind, &ids);
            // The generated output schema is fixed: an i64 plus its field name.
            // Do not use production payload estimators in the rejection oracle.
            let payload = rows.len() * if kind >= 2 { 5 + 8 } else { 7 + 8 };
            for boundary in 0..7 {
                let mut limits = MutationLimits {
                    max_affected_rows: nz(100),
                    max_operations: nz(100),
                    max_result_rows: nz(100),
                    max_result_payload_bytes: nz(10_000),
                };
                match boundary {
                    1 => limits.max_affected_rows = nz(1),
                    2 => limits.max_result_rows = nz(1),
                    3 => limits.max_result_payload_bytes = nz(1),
                    4 => limits.max_operations = nz(1),
                    5 => {
                        limits = MutationLimits {
                            max_affected_rows: nz(ids.len()),
                            max_operations: nz(ids.len() * 2),
                            max_result_rows: nz(rows.len()),
                            max_result_payload_bytes: nz(payload),
                        }
                    }
                    6 => limits.max_result_payload_bytes = nz(payload.saturating_sub(1)),
                    _ => {}
                }
                let budget_error = ids.len() > limits.max_affected_rows.get()
                    || (kind != 3 && ids.len() > limits.max_result_rows.get())
                    || payload > limits.max_result_payload_bytes.get()
                    // Plain SET/DELETE delegate operation admission to storage.
                    || (kind >= 2 && ids.len() * 2 > limits.max_operations.get());
                for injection in 0..5 {
                    let (mut catalog, mut store) = fixture(count);
                    let original = store.nodes.clone();
                    let token = RuntimeCancellationToken::new();
                    match injection {
                        1 => store.scan_error_at = Some(count / 2),
                        2 => store.scan_error_at = Some(count),
                        3 => store.write_error = true,
                        4 => {
                            token.cancel();
                        }
                        _ => {}
                    }
                    let context = RuntimeTaskContext::without_deadline(token);
                    let result = execute_mutation_with_store(
                        &plan(kind),
                        &mut catalog,
                        &mut store,
                        limits,
                        Some(&context),
                    );
                    let rejected_before_write = budget_error || matches!(injection, 1 | 2 | 4);
                    let label = format!(
                        "seed={seed}, kind={kind}, boundary={boundary}, injection={injection}"
                    );
                    assert_eq!(
                        store.nodes, original,
                        "{label}: preflight changed source nodes"
                    );
                    if rejected_before_write {
                        assert!(result.is_err(), "{label}: expected rejection");
                        assert!(store.writes.is_empty(), "{label}: premature storage call");
                    } else {
                        assert_eq!(
                            store.writes,
                            vec![(expected_write(kind, ids.clone()), limits)],
                            "{label}"
                        );
                        if injection == 3 {
                            assert!(
                                matches!(result, Err(HawDBError::Execution(message)) if message == "injected storage error"),
                                "{label}"
                            );
                        } else {
                            assert_eq!(result.unwrap(), rows, "{label}");
                        }
                    }
                    cases += 1;
                }
            }
        }
    }
    cases
}

#[test]
fn preflight_generated_smoke() {
    assert_eq!(campaign(16), 16 * 4 * 7 * 5);
}

#[test]
#[ignore = "local differential campaign"]
fn mutation_preflight_differential_campaign() {
    let cases = campaign(256);
    assert_eq!(cases, 256 * 4 * 7 * 5);
    eprintln!("mutation preflight: 256 seeds, {cases} budget/error cases");
}

#[test]
fn direct_command_delegates_limits_and_preserves_store_result() {
    let (mut catalog, mut store) = fixture(0);
    let limits = MutationLimits {
        max_operations: nz(7),
        ..MutationLimits::default()
    };
    let command = PhysicalPlan::CreateNode {
        label: "Item".into(),
        properties: BTreeMap::new(),
    };
    let rows =
        execute_mutation_with_store(&command, &mut catalog, &mut store, limits, None).unwrap();
    assert_eq!(
        rows,
        vec![BTreeMap::from([("committed".into(), Value::Bool(true))])]
    );
    assert_eq!(
        store.writes,
        vec![(
            Write::Commit(Box::new(GraphMutation::CreateNode {
                label: "Item".into(),
                properties: BTreeMap::new(),
            })),
            limits
        )]
    );
    store.writes.clear();
    let error = execute_mutation_with_store(
        &PhysicalPlan::EmptyExec,
        &mut catalog,
        &mut store,
        limits,
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, HawDBError::Execution(message) if message == "physical plan is not an executable mutation")
    );
    assert!(store.writes.is_empty());
    store.write_error = true;
    assert!(
        matches!(execute_mutation_with_store(&command, &mut catalog, &mut store, limits, None),
        Err(HawDBError::Execution(message)) if message == "injected storage error")
    );
    assert_eq!(store.writes.len(), 1);
}

#[test]
fn periodic_and_return_final_checkpoints_precede_storage_writes() {
    for kind in 0..4 {
        let (mut catalog, mut store) = fixture(DEFAULT_EXECUTION_BATCH_ROWS + 1);
        let token = RuntimeCancellationToken::new();
        store.cancel_at = Some((DEFAULT_EXECUTION_BATCH_ROWS - 1, token.clone()));
        let context = RuntimeTaskContext::without_deadline(token);
        assert!(execute_mutation_with_store(
            &plan(kind),
            &mut catalog,
            &mut store,
            MutationLimits::default(),
            Some(&context)
        )
        .is_err());
        assert!(store.writes.is_empty());
    }
    for kind in [2, 3] {
        let (mut catalog, mut store) = fixture(3);
        let token = RuntimeCancellationToken::new();
        store.cancel_at = Some((3, token.clone()));
        let context = RuntimeTaskContext::without_deadline(token);
        assert!(execute_mutation_with_store(
            &plan(kind),
            &mut catalog,
            &mut store,
            MutationLimits::default(),
            Some(&context)
        )
        .is_err());
        assert!(store.writes.is_empty());
    }
}

#[test]
fn late_set_return_assignment_error_leaves_source_and_writer_untouched() {
    let (mut catalog, mut store) = fixture(3);
    store.nodes[2]
        .properties
        .insert("score".into(), Value::Int(i64::MAX));
    let original = store.nodes.clone();
    let error = execute_mutation_with_store(
        &plan(2),
        &mut catalog,
        &mut store,
        MutationLimits::default(),
        None,
    )
    .unwrap_err();
    assert!(
        matches!(error, HawDBError::Execution(message) if message == "property increment overflowed i64")
    );
    assert_eq!(store.nodes, original);
    assert!(store.writes.is_empty());
}

#[test]
fn staged_return_reads_updated_nodes_without_applying_assignments_again() {
    let (catalog, mut store) = fixture(3);
    let ids = vec![NodeId(0), NodeId(2)];
    for node in &mut store.nodes {
        node.properties
            .insert("score".into(), Value::Int(node.id.0 as i64 + 10));
    }
    let mutation_rows = expected_rows(0, &ids);
    for kind in [2, 3] {
        assert_eq!(
            project_staged_mutation_return_rows(
                &plan(kind),
                &catalog,
                &store,
                &mutation_rows,
                MutationLimits::default(),
            )
            .unwrap(),
            Some(expected_rows(kind, &ids))
        );
        assert!(project_staged_mutation_return_rows(
            &plan(kind),
            &catalog,
            &store,
            &mutation_rows,
            MutationLimits {
                max_result_payload_bytes: nz(1),
                ..MutationLimits::default()
            },
        )
        .is_err());
    }
    assert_eq!(
        project_staged_mutation_return_rows(
            &plan(0),
            &catalog,
            &store,
            &mutation_rows,
            MutationLimits::default(),
        )
        .unwrap(),
        None
    );
    assert!(store.writes.is_empty());
}

#[test]
fn staged_return_rejects_invalid_or_missing_node_ids() {
    let (catalog, store) = fixture(1);
    for (row, expected) in [
        (
            BTreeMap::new(),
            "staged SET RETURN did not produce a node id",
        ),
        (
            BTreeMap::from([("node_id".into(), Value::String("0".into()))]),
            "staged SET RETURN did not produce a node id",
        ),
        (
            BTreeMap::from([("node_id".into(), Value::Int(-1))]),
            "staged SET RETURN produced a negative node id",
        ),
        (
            BTreeMap::from([("node_id".into(), Value::Int(99))]),
            "updated node 99 is missing during staged SET RETURN projection",
        ),
    ] {
        let error = project_staged_mutation_return_rows(
            &plan(2),
            &catalog,
            &store,
            &[row],
            MutationLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(message) if message == expected));
    }
    assert!(store.writes.is_empty());
}
