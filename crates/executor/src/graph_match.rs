// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Correlated MATCH execution with one optional boundary around the complete clause.

use crate::binding::{
    binding_memory_bytes, node_memory_bytes, relationship_memory_bytes, value_memory_bytes, Binding,
};
use crate::expression::{
    evaluate_conjunction_with_context, evaluate_predicate_with_context, node_value, path_value,
    relationship_value,
};
use crate::kernel::OperatorMemoryTracker;
use crate::pipeline::{
    runtime_checkpoint, AccountedBindingBatch, BatchControl, BatchExecutionContext, BindingBatch,
    BindingBatchSource,
};
use crate::predicate::{label_ids_for_pattern, node_matches_label_pattern};
use crate::store::{admit_graph_read, AdjacencyReadMemory, GraphExecutionRead, ScanControl};
use crate::traversal::shortest_path_bfs::{
    ShortestPathBfs, ShortestPathBfsOutcome, ShortestPathBfsSpec,
};
use crate::traversal::{
    projected_node_with_allocation, visit_bounded_expand_trails, visit_one_hop_adjacency_entries,
    visit_one_hop_relationship_records, visit_zero_hop_expand_target, BoundedTrailSpec,
    OneHopRecordSpec,
};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryClass};
use hawdb_core::{HawDBError, RelTypeId, Result, Value};
use hawdb_plan_cypher::{
    GraphEntityKind, GraphMatchNode, GraphMatchProgram, GraphMatchShortestPath, GraphMatchStep,
    PhysicalPlan, Predicate, ShortestPathSelection,
};
use hawdb_storage::read_view::{
    AdmittedNodeRead, AdmittedRelationshipRead, AdmittedVec, GraphReadAdmission,
    GraphReadAllocation,
};
use hawdb_storage::{adjacency::AdjacencyDirection, NodeId, NodeRecord, RelId};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

mod indexed_node;

const MAX_MATCH_STEPS: usize = 32;

/// WHERE conjuncts grouped by the number of steps completed before they run.
struct PlacedPredicate<'a> {
    early: Vec<Vec<&'a Predicate>>,
    last: Vec<&'a Predicate>,
    /// Conjuncts on the relationship that step `i` binds, evaluated on each
    /// relationship it reads before the target node is read. They belong to
    /// the group after step `i` and are not repeated there.
    edge: Vec<Vec<&'a Predicate>>,
}

impl<'a> PlacedPredicate<'a> {
    fn new(program: &'a GraphMatchProgram) -> Result<Self> {
        let end = program.steps.len();
        let mut early = vec![Vec::new(); end];
        let mut last = Vec::new();
        let mut edge = vec![Vec::new(); end];
        let invalid = || {
            HawDBError::Execution(
                "MATCH predicate placement does not match its conjuncts and steps".to_string(),
            )
        };
        if program.predicate_steps.is_empty() {
            if !program.edge_predicates.is_empty() {
                return Err(invalid());
            }
            last.extend(program.predicate.as_ref());
            return Ok(Self { early, last, edge });
        }
        let conjuncts = program.predicate_conjuncts();
        if conjuncts.len() != program.predicate_steps.len()
            || program.predicate_steps.iter().any(|step| *step > end)
            || program
                .edge_predicates
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid());
        }
        let mut edge_conjuncts = program.edge_predicates.iter().copied().peekable();
        for (position, (conjunct, step)) in
            conjuncts.iter().zip(&program.predicate_steps).enumerate()
        {
            if edge_conjuncts.next_if_eq(&position).is_some() {
                // Only a one-hop step that binds a relationship reads one.
                let expand = step.checked_sub(1).filter(|index| {
                    matches!(
                        program.steps.get(*index),
                        Some(GraphMatchStep::Expand {
                            relationship: Some(_),
                            min_hops: 1,
                            max_hops: 1,
                            ..
                        })
                    )
                });
                let Some(expand) = expand else {
                    return Err(invalid());
                };
                edge[expand].push(conjunct);
                continue;
            }
            match early.get_mut(*step) {
                Some(group) => group.push(conjunct),
                None => last.push(conjunct),
            }
        }
        if edge_conjuncts.next().is_some() {
            return Err(invalid());
        }
        Ok(Self { early, last, edge })
    }
}

/// Runs `visit` once per relationship type a one-hop step reads: with `None`
/// for an untyped step, else with each of `rel_type_ids` until one stops.
/// Types are disjoint, so no relationship is visited twice.
fn visit_step_types(
    rel_type_ids: Option<&[RelTypeId]>,
    visit: &mut dyn FnMut(Option<RelTypeId>) -> Result<ScanControl>,
) -> Result<ScanControl> {
    let Some(rel_type_ids) = rel_type_ids else {
        return visit(None);
    };
    for &rel_type_id in rel_type_ids {
        if visit(Some(rel_type_id))? == ScanControl::Stop {
            return Ok(ScanControl::Stop);
        }
    }
    Ok(ScanControl::Continue)
}

/// Retained bytes of one relationship id in the trail-uniqueness set.
const USED_RELATIONSHIP_BYTES: usize = 48;

/// Bytes one more graph slot adds to a bound row: the slot pair, both `Arc`
/// headers, the name `String` and slot-vector growth, as charged by
/// `binding_memory_bytes`.
fn graph_slot_entry_bytes(name: &str) -> usize {
    name.len()
        .saturating_add(16 * std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of::<NodeRecord>())
}

pub(crate) fn stream_graph_match(
    program: &GraphMatchProgram,
    input: Option<&PhysicalPlan>,
    source: &mut dyn BindingBatchSource,
    store: &dyn GraphExecutionRead,
    context: BatchExecutionContext<'_>,
    limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if program.steps.len() > MAX_MATCH_STEPS {
        return Err(HawDBError::Execution(
            "MATCH exceeds maximum pattern depth".to_string(),
        ));
    }
    // Validate before reading input so empty streams cannot bypass shape checks.
    for step in &program.steps {
        if let GraphMatchStep::Expand {
            relationship,
            rel_type,
            properties,
            min_hops,
            max_hops,
            ..
        } = step
            && (*min_hops != 1 || *max_hops != 1)
            && (relationship.is_some() || !properties.is_empty() || rel_type.is_empty())
        {
            return Err(HawDBError::Execution(
                "bounded MATCH expansion requires a typed pattern without relationship bindings"
                    .to_string(),
            ));
        }
    }
    let placed = PlacedPredicate::new(program)?;
    if limit.is_reached(0) {
        return Ok(BatchControl::Stop);
    }
    let account = context.operator_account("GraphMatchExec state");
    let source_account = context.source_account_retaining("GraphMatchExec source", account.clone());
    let output_account = context.memory_ledger.account(
        QueryMemoryClass::PipelineBatch,
        "GraphMatchExec output",
        context.memory.batch_payload_bytes,
    );
    let mut output = AccountedBindingBatch::with_account(
        "GraphMatchExec",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        output_account,
    );
    let runtime = MatchRuntime {
        program,
        placed,
        deferred: Cell::new(false),
        store,
        context,
        account: &account,
        source_account: &source_account,
        indexed: indexed_node::IndexedNodeStats::default(),
        // Seed scoring reads the candidate's properties and annotates each
        // hop, so it keeps whole records and per-step row copies.
        scoring: context.observer.seed_graph_scoring_input().is_some(),
    };
    let mut emitted = 0;
    let mut append = |binding: &Binding| {
        if output.push_cloned(binding, emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        emitted += 1;
        if output.is_full() && output.emit(emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        Ok(if limit.is_reached(emitted) {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    };
    let mut visit_input = |input: &Binding| -> Result<BatchControl> {
        runtime_checkpoint(context.task_context)?;
        let (mut binding, _leases) = runtime.prepare(input)?;
        runtime.deferred.set(false);
        let mut matched = false;
        // Steps extend this one row in place and restore it on return.
        let control = runtime.visit(0, &mut binding, &mut BTreeSet::new(), &mut |row| {
            matched = true;
            append(row)
        })?;
        if control == ScanControl::Stop {
            return Ok(BatchControl::Stop);
        }
        if !matched && program.optional {
            let _lease = account.reserve(
                binding_memory_bytes(&binding).saturating_add(
                    program
                        .introduced
                        .iter()
                        .map(|name| name.len() + 64)
                        .sum::<usize>(),
                ),
            )?;
            let mut row = binding.clone();
            for name in &program.introduced {
                row.nodes.remove(name);
                row.relationships.remove(name);
                row.values.insert(name.clone(), Value::Null);
            }
            if context.observer.seed_graph_scoring_input().is_some() {
                crate::scoring::advance_seed_hop(&mut row, 0, false)?;
            }
            if append(&row)? == ScanControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        Ok(BatchControl::Continue)
    };
    let control = if let Some(input) = input {
        source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
            for row in &batch {
                if visit_input(row)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            Ok(BatchControl::Continue)
        })?
    } else {
        visit_input(&Binding::values(BTreeMap::new()))?
    };
    runtime.indexed.report(context.observer);
    if output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}

struct MatchRuntime<'a> {
    indexed: indexed_node::IndexedNodeStats<'a>,
    program: &'a GraphMatchProgram,
    placed: PlacedPredicate<'a>,
    /// Set while the current partial candidate carries an early conjunct
    /// whose evaluation failed. Its descendants then skip early filtering
    /// and evaluate the whole predicate after the last step, so a candidate
    /// that never completes, or that a text-earlier conjunct rejects, raises
    /// no error the end-of-match evaluation would not have raised.
    deferred: Cell<bool>,
    store: &'a dyn GraphExecutionRead,
    context: BatchExecutionContext<'a>,
    account: &'a QueryMemoryAccount,
    source_account: &'a QueryMemoryAccount,
    scoring: bool,
}

/// The rest of a one-hop step after its relationship is bound.
struct EdgeTarget<'p, 'l> {
    /// Conjuncts on the relationship, run before the target is read.
    conjuncts: &'p [&'p Predicate],
    pattern: &'p GraphMatchNode,
    labels: Option<&'l [hawdb_core::LabelId]>,
    /// The target to read, or `None` when the step's target is bound.
    neighbor: Option<NodeId>,
    /// The bound relationship's size, which shares the target's read budget.
    edge_bytes: usize,
}

/// One candidate target of an expansion step.
enum Target {
    /// The step's target variable is already bound to this node id.
    Bound,
    /// A record read for an unbound target variable.
    Read(NodeRecord),
}

impl MatchRuntime<'_> {
    /// The properties a node variable's reads are limited to, if any.
    fn projection(&self, variable: &str) -> Option<&BTreeSet<String>> {
        if self.scoring {
            return None;
        }
        self.program.projected_nodes.get(variable)
    }

    /// The properties a relationship variable's reads are limited to, if any.
    fn relationship_projection(&self, variable: &str) -> Option<&BTreeSet<String>> {
        if self.scoring {
            return None;
        }
        self.program.projected_relationships.get(variable)
    }

    /// Reads an unbound target, limited to its projection, with an admitted
    /// allocation retained by the caller while the record is bound.
    /// `related_bytes` is the size of the relationship read with it, which
    /// shares the read budget.
    fn read_target(
        &self,
        variable: &str,
        id: NodeId,
        label_ids: Option<&[hawdb_core::LabelId]>,
        related_bytes: usize,
    ) -> Result<Option<(NodeRecord, Box<dyn GraphReadAllocation>)>> {
        let memory = self.adjacency_memory();
        let task_context = self.context.task_context;
        if let Some(properties) = self.projection(variable) {
            return projected_node_with_allocation(
                self.store,
                id,
                label_ids,
                properties,
                &mut |bytes| memory.admit_node(bytes, related_bytes, task_context),
                task_context,
            );
        }
        let mut admit = |bytes| {
            memory
                .admit_node(bytes, related_bytes, task_context)
                .map(Some)
        };
        match self.store.node_with_allocation(
            id,
            label_ids,
            &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                &mut admit,
                task_context,
            ),
        )? {
            AdmittedNodeRead::Node(node) => Ok(Some(node.into_parts())),
            // This admission never requests Stop; it fails instead.
            AdmittedNodeRead::Missing | AdmittedNodeRead::Stopped => Ok(None),
        }
    }

    fn adjacency_memory(&self) -> AdjacencyReadMemory<'_> {
        AdjacencyReadMemory {
            budget_bytes: self.context.memory.query_memory_bytes.get(),
            account: Some(self.source_account),
        }
    }

    fn prepare(
        &self,
        input: &Binding,
    ) -> Result<(Binding, AdmittedVec<Box<dyn GraphReadAllocation>>)> {
        let mut admit =
            |bytes| admit_graph_read(self.source_account, self.context.task_context, bytes);
        let admission = GraphReadAdmission::new(&mut admit);
        let mut leases = AdmittedVec::new(&admission)?;
        leases.try_push(admit_graph_read(
            self.source_account,
            self.context.task_context,
            binding_memory_bytes(input),
        )?)?;
        let mut row = input.clone();
        for name in &self.program.introduced {
            row.nodes.remove(name);
            row.relationships.remove(name);
            row.values.remove(name);
        }
        for import in &self.program.imports {
            row.nodes.remove(&import.variable);
            row.relationships.remove(&import.variable);
            let value = input.values.get(&import.column).ok_or_else(|| {
                HawDBError::Execution(format!(
                    "missing graph column '{}' during MATCH",
                    import.column
                ))
            })?;
            if value == &Value::Null {
                row.values.insert(import.variable.clone(), Value::Null);
                continue;
            }
            let Value::Map(values) = value else {
                return Err(HawDBError::Execution(
                    "typed graph column requires an entity value".to_string(),
                ));
            };
            let id = entity_id(values, "_id")?;
            match import.kind {
                GraphEntityKind::Node
                    if let Some(properties) = self.projection(&import.variable) =>
                {
                    let node = projected_node_with_allocation(
                        self.store,
                        NodeId(id),
                        None,
                        properties,
                        &mut |bytes| {
                            admit_graph_read(
                                self.source_account,
                                self.context.task_context,
                                bytes
                                    .saturating_add(import.variable.len())
                                    .saturating_add(1024),
                            )
                        },
                        self.context.task_context,
                    )?;
                    let Some((node, allocation)) = node else {
                        return Err(HawDBError::Execution(
                            "bound node disappeared during MATCH".to_string(),
                        ));
                    };
                    leases.try_push(allocation)?;
                    row.nodes.insert(import.variable.clone(), node);
                }
                GraphEntityKind::Node => {
                    let mut admit_node = |bytes: usize| {
                        admit_graph_read(
                            self.source_account,
                            self.context.task_context,
                            bytes
                                .saturating_add(import.variable.len())
                                .saturating_add(1024),
                        )
                        .map(Some)
                    };
                    let AdmittedNodeRead::Node(node) = self.store.node_with_allocation(
                        NodeId(id),
                        None,
                        &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                            &mut admit_node,
                            self.context.task_context,
                        ),
                    )?
                    else {
                        return Err(HawDBError::Execution(
                            "bound node disappeared during MATCH".to_string(),
                        ));
                    };
                    let (node, allocation) = node.into_parts();
                    leases.try_push(allocation)?;
                    row.nodes.insert(import.variable.clone(), node);
                }
                GraphEntityKind::Relationship => {
                    let source = NodeId(entity_id(values, "source_id")?);
                    let mut found = None;
                    self.store
                        .visit_ordered_adjacent_relationships_with_allocation(
                            source,
                            None,
                            AdjacencyDirection::Outgoing,
                            self.adjacency_memory(),
                            &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                                &mut |bytes| {
                                    admit_graph_read(
                                        self.source_account,
                                        self.context.task_context,
                                        bytes
                                            .saturating_add(import.variable.len())
                                            .saturating_add(1024),
                                    )
                                    .map(Some)
                                },
                                self.context.task_context,
                            ),
                            &mut |input| {
                                let (relationship, allocation) = input.into_parts();
                                runtime_checkpoint(self.context.task_context)?;
                                if relationship.id == RelId(id) {
                                    found = Some((relationship, allocation));
                                    Ok(ScanControl::Stop)
                                } else {
                                    Ok(ScanControl::Continue)
                                }
                            },
                        )?;
                    let (relationship, allocation) = found.ok_or_else(|| {
                        HawDBError::Execution(
                            "bound relationship disappeared during MATCH".to_string(),
                        )
                    })?;
                    leases.try_push(allocation)?;
                    row.relationships
                        .insert(import.variable.clone(), relationship);
                }
            }
        }
        Ok((row, leases))
    }

    /// Runs the conjuncts placed before step `index`, then the step. A
    /// conjunct that is false or NULL drops the partial candidate: the final
    /// conjunction could not be true for any completion of it.
    fn visit(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(self.context.task_context)?;
        let conjuncts = self
            .placed
            .early
            .get(index)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if self.deferred.get() || conjuncts.is_empty() {
            return self.visit_step(index, row, used, emit);
        }
        for conjunct in conjuncts {
            match self.conjunct_holds(conjunct, row) {
                Ok(true) => {}
                Ok(false) => return Ok(ScanControl::Continue),
                Err(HawDBError::Execution(_)) => {
                    // Cancellation is sticky; report it now rather than defer.
                    runtime_checkpoint(self.context.task_context)?;
                    self.deferred.set(true);
                    let control = self.visit_step(index, row, used, emit);
                    self.deferred.set(false);
                    return control;
                }
                Err(error) => return Err(error),
            }
        }
        self.visit_step(index, row, used, emit)
    }

    fn conjunct_holds(&self, conjunct: &Predicate, row: &Binding) -> Result<bool> {
        evaluate_predicate_with_context(
            conjunct,
            self.context.catalog,
            self.store,
            row,
            self.context.observer,
            self.adjacency_memory(),
            self.context.task_context,
        )
    }

    /// Accepts a complete match. Conjuncts that ran early were true, so the
    /// remaining ones decide, in their original order.
    fn complete_match_holds(&self, row: &Binding) -> Result<bool> {
        let full;
        let conjuncts: &[&Predicate] = match &self.program.predicate {
            Some(predicate) if self.deferred.get() => {
                full = [predicate];
                &full
            }
            _ => &self.placed.last,
        };
        if conjuncts.is_empty() {
            return Ok(true);
        }
        evaluate_conjunction_with_context(
            conjuncts,
            self.context.catalog,
            self.store,
            row,
            self.context.observer,
            self.adjacency_memory(),
            self.context.task_context,
        )
    }

    fn visit_step(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let Some(step) = self.program.steps.get(index) else {
            if !self.complete_match_holds(row)? {
                return Ok(ScanControl::Continue);
            }
            return emit(row);
        };
        match step {
            GraphMatchStep::Node(pattern) => {
                if let Some(node) = row.nodes.get(&pattern.variable) {
                    return if self.node_matches(pattern, node) {
                        self.visit(index + 1, row, used, emit)
                    } else {
                        Ok(ScanControl::Continue)
                    };
                }
                if row.values.get(&pattern.variable) == Some(&Value::Null) {
                    return Ok(ScanControl::Continue);
                }
                let labels = label_ids_for_pattern(self.context.catalog, &pattern.label);
                let label = labels.as_deref().and_then(|labels| {
                    if let [label] = labels {
                        Some(*label)
                    } else {
                        None
                    }
                });
                let lookup = self.indexed_node_access(pattern, label, row)?;
                // The node joins the row this visit already owns; admission
                // covers its record and slot, not another copy of the row.
                let mut admit = |bytes: usize| {
                    crate::store::admit_graph_read(
                        self.source_account,
                        self.context.task_context,
                        bytes.saturating_add(1024 + pattern.variable.len()),
                    )
                    .map(Some)
                };
                let mut allocator = hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                    &mut admit,
                    self.context.task_context,
                );
                let visited = Cell::new(0usize);
                let matched = Cell::new(0usize);
                let mut bind = |node: NodeRecord| {
                    runtime_checkpoint(self.context.task_context)?;
                    visited.set(visited.get().saturating_add(1));
                    if !self.node_matches(pattern, &node) {
                        return Ok(ScanControl::Continue);
                    }
                    self.visit_bound(
                        index,
                        row,
                        used,
                        &pattern.variable,
                        node,
                        None,
                        &mut |row| {
                            matched.set(matched.get().saturating_add(1));
                            emit(row)
                        },
                    )
                };
                let control = match (lookup.as_ref(), self.projection(&pattern.variable)) {
                    (Some(lookup), Some(properties)) => {
                        let mut admit = |bytes: usize| {
                            crate::store::admit_graph_read(
                                self.source_account,
                                self.context.task_context,
                                bytes.saturating_add(1024 + pattern.variable.len()),
                            )
                        };
                        let mut allocator = hawdb_storage::read_view::GraphReadAllocator::new(
                            &mut admit,
                            self.context.task_context,
                        );
                        let mut visit = |node: hawdb_storage::ProjectedNodeRecord| {
                            bind(NodeRecord {
                                id: node.id,
                                labels: node.labels,
                                properties: node.properties,
                            })
                        };
                        let hawdb_plan_cypher::NodeProjectionAccess::PropertyValues {
                            property,
                            values,
                        } = &lookup.access
                        else {
                            unreachable!()
                        };
                        if !self.store.is_out_of_core()
                            && let Some(control) = self
                                .store
                                .visit_indexed_projected_nodes_by_property_admitted(
                                    lookup.label,
                                    property,
                                    values,
                                    properties,
                                    &mut allocator,
                                    &mut visit,
                                )?
                        {
                            Ok(control)
                        } else {
                            self.store.visit_projected_nodes_by_access_admitted(
                                lookup.label,
                                &lookup.access,
                                properties,
                                &mut allocator,
                                &mut visit,
                            )
                        }
                    }
                    (Some(lookup), None) => self.store.visit_nodes_by_access_with_allocation(
                        lookup.label,
                        &lookup.access,
                        &mut allocator,
                        &mut |input| {
                            let (node, _allocation) = input.into_parts();
                            bind(node)
                        },
                    ),
                    (None, Some(properties)) => self.store.visit_projected_nodes_with_allocation(
                        label,
                        properties,
                        &mut allocator,
                        &mut |input| {
                            let (node, _allocation) = input.into_parts();
                            bind(NodeRecord {
                                id: node.id,
                                labels: node.labels,
                                properties: node.properties,
                            })
                        },
                    ),
                    (None, None) => self.store.visit_nodes_with_allocation(
                        label,
                        &mut allocator,
                        &mut |input| {
                            let (node, _allocation) = input.into_parts();
                            bind(node)
                        },
                    ),
                }?;
                if let Some(lookup) = &lookup {
                    let total = self.store.node_count_for_label(Some(lookup.label));
                    self.indexed
                        .record(lookup, total, visited.get(), matched.get());
                }
                Ok(control)
            }
            GraphMatchStep::Expand {
                source,
                relationship,
                rel_type,
                rel_types,
                properties,
                direction,
                min_hops,
                max_hops,
                target,
            } => {
                let bounded = *min_hops != 1 || *max_hops != 1;
                let Some(source_id) = row.nodes.get(source).map(|node| node.id) else {
                    if row.values.get(source) == Some(&Value::Null) {
                        return Ok(ScanControl::Continue);
                    }
                    return Err(HawDBError::Execution(format!(
                        "missing MATCH source '{source}'"
                    )));
                };
                // `None` reads every type. Otherwise the step reads these
                // types: its one type, or the disjunction's types that exist.
                let single;
                let disjunction: Vec<RelTypeId>;
                let rel_type_ids = if !rel_types.is_empty() {
                    disjunction = rel_types
                        .iter()
                        .filter_map(|name| self.context.catalog.rel_type_id(name))
                        .collect();
                    Some(disjunction.as_slice())
                } else if rel_type.is_empty() {
                    None
                } else {
                    single = self.context.catalog.rel_type_id(rel_type);
                    Some(single.as_slice())
                };
                let rel_type_ids = match rel_type_ids {
                    Some([]) => {
                        // No named type exists, so only a zero-hop trail matches.
                        if *min_hops == 0 {
                            let labels = label_ids_for_pattern(self.context.catalog, &target.label);
                            return visit_zero_hop_expand_target(
                                self.store,
                                source_id,
                                labels.as_deref(),
                                *max_hops,
                                self.adjacency_memory(),
                                self.context.task_context,
                                &mut |node, hop| {
                                    self.visit_target(
                                        index,
                                        row,
                                        used,
                                        target,
                                        (Target::Read(node), hop),
                                        emit,
                                    )
                                },
                            );
                        }
                        return Ok(ScanControl::Continue);
                    }
                    rel_type_ids => rel_type_ids,
                };
                let labels = label_ids_for_pattern(self.context.catalog, &target.label);
                // A bound target is compared by id and matched against its
                // bound record; reading it again would return the same node.
                let bound_target = row.nodes.get(&target.variable).map(|node| node.id);
                let no_properties = BTreeSet::new();
                let target_properties = if bound_target.is_some() {
                    Some(&no_properties)
                } else {
                    self.projection(&target.variable)
                };
                let observed = |node: NodeRecord| match bound_target {
                    Some(_) => Target::Bound,
                    None => Target::Read(node),
                };
                if bounded {
                    // Trails exclude relationships bound earlier in the
                    // pattern, and later steps exclude the trail's own.
                    let excluded = used.clone();
                    let _excluded = self
                        .account
                        .reserve(excluded.len().saturating_mul(USED_RELATIONSHIP_BYTES))?;
                    // Validation rejects untyped bounded steps before any row.
                    let Some(rel_type_ids) = rel_type_ids else {
                        return Err(HawDBError::Execution(
                            "bounded MATCH expansion requires a typed pattern".to_string(),
                        ));
                    };
                    return visit_bounded_expand_trails(
                        self.store,
                        BoundedTrailSpec {
                            source: source_id,
                            rel_type_ids,
                            direction: *direction,
                            target_label_ids: labels.as_deref(),
                            min_hops: *min_hops,
                            max_hops: *max_hops,
                        },
                        &excluded,
                        target_properties,
                        self.adjacency_memory(),
                        self.context.task_context,
                        &mut |node, hop, trail| {
                            if bound_target.is_some_and(|id| id != node.id) {
                                return Ok(ScanControl::Continue);
                            }
                            let _used = self
                                .account
                                .reserve(trail.len().saturating_mul(USED_RELATIONSHIP_BYTES))?;
                            used.extend(trail.iter().copied());
                            let control = self.visit_target(
                                index,
                                row,
                                used,
                                target,
                                (observed(node), hop),
                                emit,
                            );
                            for id in trail {
                                used.remove(id);
                            }
                            control
                        },
                    );
                }
                let bound_relationship = relationship
                    .as_ref()
                    .and_then(|name| row.relationships.get(name))
                    .map(|record| record.id);
                if relationship
                    .as_ref()
                    .is_some_and(|name| row.values.get(name) == Some(&Value::Null))
                {
                    return Ok(ScanControl::Continue);
                }
                if relationship.is_none() && properties.is_empty() {
                    // Nothing reads an anonymous relationship without property
                    // constraints beyond its id and endpoint.
                    return visit_step_types(rel_type_ids, &mut |rel_type_id| {
                        visit_one_hop_adjacency_entries(
                            self.store,
                            source_id,
                            rel_type_id,
                            *direction,
                            self.adjacency_memory(),
                            self.context.task_context,
                            &mut |relationship_id, neighbor| {
                                if used.contains(&relationship_id)
                                    || bound_target.is_some_and(|id| id != neighbor)
                                {
                                    return Ok(ScanControl::Continue);
                                }
                                let (observed, _allocation) = match bound_target {
                                    Some(_) => (Target::Bound, None),
                                    None => match self.read_target(
                                        &target.variable,
                                        neighbor,
                                        labels.as_deref(),
                                        0,
                                    )? {
                                        Some((node, allocation)) => {
                                            (Target::Read(node), Some(allocation))
                                        }
                                        None => return Ok(ScanControl::Continue),
                                    },
                                };
                                let _used = self.account.reserve(USED_RELATIONSHIP_BYTES)?;
                                used.insert(relationship_id);
                                let control = self.visit_target(
                                    index,
                                    row,
                                    used,
                                    target,
                                    (observed, 1),
                                    emit,
                                );
                                used.remove(&relationship_id);
                                control
                            },
                        )
                    });
                }
                // Conjuncts on this relationship run before its target is read.
                let edge_conjuncts = self
                    .placed
                    .edge
                    .get(index)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let projection = match relationship {
                    Some(variable) if bound_relationship.is_none() => {
                        self.relationship_projection(variable)
                    }
                    _ => None,
                };
                visit_step_types(rel_type_ids, &mut |rel_type_id| {
                    visit_one_hop_relationship_records(
                        self.store,
                        OneHopRecordSpec {
                            source: source_id,
                            rel_type_id,
                            direction: *direction,
                            rel_properties: properties,
                            projection,
                        },
                        self.adjacency_memory(),
                        self.context.observer,
                        self.context.task_context,
                        &mut |edge, neighbor| {
                            if used.contains(&edge.id)
                                || bound_relationship.is_some_and(|id| id != edge.id)
                                || bound_target.is_some_and(|id| id != neighbor)
                                || !properties_match(properties, &edge.properties)
                            {
                                return Ok(ScanControl::Continue);
                            }
                            let edge_bytes = relationship_memory_bytes(&edge);
                            let _used = self.account.reserve(USED_RELATIONSHIP_BYTES)?;
                            let _edge = self.source_account.reserve(
                                edge_bytes.saturating_add(
                                    relationship
                                        .as_ref()
                                        .map_or(0, |name| graph_slot_entry_bytes(name)),
                                ),
                            )?;
                            let edge_id = edge.id;
                            used.insert(edge_id);
                            // A bound relationship keeps its record: the ids match.
                            let inserted = match relationship {
                                Some(variable) if bound_relationship.is_none() => {
                                    row.relationships.insert(variable.clone(), edge);
                                    Some(variable)
                                }
                                _ => None,
                            };
                            let control = self.visit_edge_target(
                                index,
                                row,
                                used,
                                EdgeTarget {
                                    conjuncts: edge_conjuncts,
                                    pattern: target,
                                    labels: labels.as_deref(),
                                    neighbor: bound_target.is_none().then_some(neighbor),
                                    edge_bytes,
                                },
                                emit,
                            );
                            if let Some(variable) = inserted {
                                row.relationships.remove(variable);
                            }
                            used.remove(&edge_id);
                            control
                        },
                    )
                })
            }
            GraphMatchStep::ShortestPath(step) => {
                self.visit_shortest_paths(index, row, used, step, emit)
            }
        }
    }

    /// Runs `conjuncts` on the relationship step `index` has just bound in
    /// `row`, then reads the step's target, `neighbor` when it is unbound, and
    /// continues with it. A false or NULL conjunct drops the relationship
    /// before its target is read, as it would drop the candidate after the
    /// step. A failing one defers the whole predicate to the end of the match
    /// for this candidate, as [`Self::visit`] does.
    fn visit_edge_target(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        edge: EdgeTarget<'_, '_>,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let EdgeTarget {
            conjuncts,
            pattern,
            labels,
            neighbor,
            edge_bytes,
        } = edge;
        let mut defer = false;
        if !self.deferred.get() {
            for conjunct in conjuncts {
                match self.conjunct_holds(conjunct, row) {
                    Ok(true) => {}
                    Ok(false) => return Ok(ScanControl::Continue),
                    Err(HawDBError::Execution(_)) => {
                        // Cancellation is sticky; report it now rather than defer.
                        runtime_checkpoint(self.context.task_context)?;
                        defer = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let (observed, _allocation) = match neighbor {
            None => (Target::Bound, None),
            Some(id) => match self.read_target(&pattern.variable, id, labels, edge_bytes)? {
                Some((node, allocation)) => (Target::Read(node), Some(allocation)),
                None => return Ok(ScanControl::Continue),
            },
        };
        if !defer {
            return self.visit_target(index, row, used, pattern, (observed, 1), emit);
        }
        self.deferred.set(true);
        let control = self.visit_target(index, row, used, pattern, (observed, 1), emit);
        self.deferred.set(false);
        control
    }

    /// Binds `variable` to `node` in the row for the rest of the pattern,
    /// then restores the row. With seed scoring the extended row is a copy
    /// carrying the hop annotation `hop`.
    #[allow(clippy::too_many_arguments)]
    fn visit_bound(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        variable: &str,
        node: NodeRecord,
        hop: Option<usize>,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        if self.scoring {
            let _lease = self.source_account.reserve(
                binding_memory_bytes(row)
                    .saturating_add(node_memory_bytes(&node))
                    .saturating_add(variable.len()),
            )?;
            let mut next = row.clone();
            next.nodes.insert(variable.to_string(), node);
            if let Some(hop) = hop {
                crate::scoring::advance_seed_hop(&mut next, hop, true)?;
            }
            return self.visit(index + 1, &mut next, used, emit);
        }
        let _lease = self
            .source_account
            .reserve(node_memory_bytes(&node).saturating_add(graph_slot_entry_bytes(variable)))?;
        row.nodes.insert(variable.to_string(), node);
        // Evaluate placed conjuncts while this candidate's slot is bound.
        let control = self.visit(index + 1, row, used, emit);
        row.nodes.remove(variable);
        control
    }

    /// Binds the path variable to each selected shortest path between the two
    /// bound endpoints. Relationships bound earlier in the clause are not
    /// traversed, and the path's relationships are unavailable to later steps.
    fn visit_shortest_paths(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        step: &GraphMatchShortestPath,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let endpoint = |name: &str| match row.nodes.get(name) {
            Some(node) => Ok(Some(node.id)),
            None if row.values.get(name) == Some(&Value::Null) => Ok(None),
            None => Err(HawDBError::Execution(format!(
                "shortestPath endpoint '{name}' is not bound"
            ))),
        };
        let (Some(source), Some(target)) = (endpoint(&step.source)?, endpoint(&step.target)?)
        else {
            return Ok(ScanControl::Continue);
        };
        let rel_type_id = if step.rel_type.is_empty() {
            None
        } else {
            match self.context.catalog.rel_type_id(&step.rel_type) {
                Some(id) => Some(id),
                // An unknown type has no relationships; only a zero-length
                // path can match.
                None if source != target => return Ok(ScanControl::Continue),
                None => None,
            }
        };
        // The search excludes the relationships bound so far while each path
        // extends `used` in place.
        let excluded = used.clone();
        let _excluded = self
            .account
            .reserve(excluded.len().saturating_mul(USED_RELATIONSHIP_BYTES))?;
        let spec = ShortestPathBfsSpec {
            source,
            target,
            direction: step.direction,
            min_hops: step.min_hops,
            max_hops: step.max_hops,
            bounded: step.bounded,
            all: step.selection == ShortestPathSelection::All,
            excluded: &excluded,
        };
        let task_context = self.context.task_context;
        let memory = self.adjacency_memory();
        let mut tracker = OperatorMemoryTracker::with_account(
            self.context.memory.blocking_operator_bytes,
            self.account.clone(),
        );
        let mut checkpoint = || runtime_checkpoint(task_context);
        let mut adjacency =
            |node: NodeId,
             direction: AdjacencyDirection,
             visit: &mut dyn FnMut(RelId, NodeId) -> Result<ScanControl>| {
                // The search needs only relationship ids and neighbors.
                self.store.visit_ordered_adjacency_entries_with_allocation(
                    (node, rel_type_id, direction),
                    memory,
                    &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                        &mut |bytes| memory.admit_node(bytes, 0, task_context).map(Some),
                        task_context,
                    ),
                    &mut |entry| {
                        // An undirected self-loop appears in both adjacency
                        // directions; traverse it once, as other patterns do.
                        if direction == AdjacencyDirection::Incoming
                            && step.direction == hawdb_core::RelationshipDirection::Undirected
                            && entry.neighbor_id == node
                        {
                            return Ok(ScanControl::Continue);
                        }
                        visit(entry.relationship_id, entry.neighbor_id)
                    },
                )
            };
        let search =
            match ShortestPathBfs::search(&spec, &mut adjacency, &mut tracker, &mut checkpoint)? {
                ShortestPathBfsOutcome::NotFound => return Ok(ScanControl::Continue),
                ShortestPathBfsOutcome::ZeroLength => {
                    return self.visit_path(index, row, used, step, (&[source], &[]), emit)
                }
                ShortestPathBfsOutcome::Found(search) => search,
            };
        let control = search.visit_paths(
            &mut tracker,
            &mut checkpoint,
            &mut |nodes, relationships| {
                self.visit_path(index, row, used, step, (nodes, relationships), emit)
            },
        );
        (*search).release(&mut tracker);
        control
    }

    fn visit_path(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        step: &GraphMatchShortestPath,
        path: (&[NodeId], &[RelId]),
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(self.context.task_context)?;
        let (node_ids, relationship_ids) = path;
        let catalog = self.context.catalog;
        let task_context = self.context.task_context;
        let mut admit =
            |bytes: usize| admit_graph_read(self.source_account, task_context, bytes).map(Some);
        // Owns the copied path payload until the bound row is released.
        let mut payload = self.source_account.reserve(
            node_ids
                .len()
                .saturating_add(relationship_ids.len())
                .saturating_mul(std::mem::size_of::<Value>())
                .saturating_add(256),
        )?;
        let mut nodes = Vec::with_capacity(node_ids.len());
        for &id in node_ids {
            let AdmittedNodeRead::Node(node) = self.store.node_with_allocation(
                id,
                None,
                &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                    &mut admit,
                    task_context,
                ),
            )?
            else {
                return Err(HawDBError::Execution(
                    "shortestPath node disappeared during MATCH".to_string(),
                ));
            };
            let (node, _allocation) = node.into_parts();
            let value = node_value(&node, catalog);
            payload.grow(value_memory_bytes(&value))?;
            nodes.push(value);
        }
        let mut relationships = Vec::with_capacity(relationship_ids.len());
        for &id in relationship_ids {
            let AdmittedRelationshipRead::Relationship(relationship) =
                self.store.relationship_with_allocation(
                    id,
                    &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                        &mut admit,
                        task_context,
                    ),
                )?
            else {
                return Err(HawDBError::Execution(
                    "shortestPath relationship disappeared during MATCH".to_string(),
                ));
            };
            let (relationship, _allocation) = relationship.into_parts();
            let value = relationship_value(&relationship, catalog);
            payload.grow(value_memory_bytes(&value))?;
            relationships.push(value);
        }
        let value = path_value(nodes, relationships);
        let _lease = self.source_account.reserve(
            graph_slot_entry_bytes(&step.path).saturating_add(
                relationship_ids
                    .len()
                    .saturating_mul(USED_RELATIONSHIP_BYTES),
            ),
        )?;
        // The path variable is introduced by this clause, so the row has no
        // value under its name to restore.
        row.values.insert(step.path.clone(), value);
        used.extend(relationship_ids.iter().copied());
        let control = self.visit(index + 1, row, used, emit);
        for id in relationship_ids {
            used.remove(id);
        }
        row.values.remove(&step.path);
        control
    }

    fn visit_target(
        &self,
        index: usize,
        row: &mut Binding,
        used: &mut BTreeSet<RelId>,
        pattern: &GraphMatchNode,
        observed_target: (Target, usize),
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        let (target, hop) = observed_target;
        if row.values.get(&pattern.variable) == Some(&Value::Null) {
            return Ok(ScanControl::Continue);
        }
        let node = match target {
            Target::Bound => {
                let Some(bound) = row.nodes.get(&pattern.variable) else {
                    return Err(HawDBError::Execution(format!(
                        "missing MATCH target '{}'",
                        pattern.variable
                    )));
                };
                if !self.node_matches(pattern, bound) {
                    return Ok(ScanControl::Continue);
                }
                if !self.scoring {
                    return self.visit(index + 1, row, used, emit);
                }
                bound.clone()
            }
            Target::Read(node) => {
                let bound = row.nodes.get(&pattern.variable);
                if !self.node_matches(pattern, &node)
                    || bound.is_some_and(|bound| bound.id != node.id)
                {
                    return Ok(ScanControl::Continue);
                }
                // The same node is already bound; keep its record.
                if bound.is_some() && !self.scoring {
                    return self.visit(index + 1, row, used, emit);
                }
                node
            }
        };
        self.visit_bound(index, row, used, &pattern.variable, node, Some(hop), emit)
    }

    fn node_matches(&self, pattern: &GraphMatchNode, node: &NodeRecord) -> bool {
        node_matches_label_pattern(
            node,
            label_ids_for_pattern(self.context.catalog, &pattern.label).as_deref(),
        ) && properties_match(&pattern.properties, &node.properties)
    }
}

fn entity_id(values: &BTreeMap<String, Value>, field: &str) -> Result<u64> {
    match values.get(field) {
        Some(Value::Int(id)) => Ok(*id as u64),
        _ => Err(HawDBError::Execution(format!(
            "typed graph value lacks identity field '{field}'"
        ))),
    }
}

fn properties_match(expected: &BTreeMap<String, Value>, actual: &BTreeMap<String, Value>) -> bool {
    expected
        .iter()
        .all(|(name, value)| value != &Value::Null && actual.get(name) == Some(value))
}
