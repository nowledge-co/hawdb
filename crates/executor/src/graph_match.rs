//! Correlated MATCH execution with one optional boundary around the complete clause.

use crate::binding::{binding_memory_bytes, node_memory_bytes, relationship_memory_bytes, Binding};
use crate::expression::evaluate_predicate_with_memory;
use crate::pipeline::{
    runtime_checkpoint, AccountedBindingBatch, BatchControl, BatchExecutionContext, BindingBatch,
    BindingBatchSource,
};
use crate::predicate::{label_ids_for_pattern, node_matches_label_pattern};
use crate::store::{AdjacencyReadMemory, GraphExecutionRead, ScanControl};
use crate::traversal::{
    visit_bounded_expand_targets, visit_one_hop_relationships_with_context, BoundedExpandSpec,
    OneHopRelationshipSpec,
};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease};
use hawdb_core::{HawDBError, Result, Value};
use hawdb_plan::{
    GraphEntityKind, GraphMatchNode, GraphMatchProgram, GraphMatchStep, PhysicalPlan,
};
use hawdb_storage::{AdjacencyDirection, NodeId, NodeRecord, RelId};
use std::collections::{BTreeMap, BTreeSet};

const MAX_MATCH_STEPS: usize = 32;

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
    if limit.is_reached(0) {
        return Ok(BatchControl::Stop);
    }
    let account = context.operator_account("GraphMatchExec state");
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
        store,
        context,
        account: &account,
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
        let (binding, _leases) = runtime.prepare(input)?;
        let mut matched = false;
        let control = runtime.visit(0, &binding, &BTreeSet::new(), &mut |row| {
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
    if output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}

struct MatchRuntime<'a> {
    program: &'a GraphMatchProgram,
    store: &'a dyn GraphExecutionRead,
    context: BatchExecutionContext<'a>,
    account: &'a QueryMemoryAccount,
}

impl MatchRuntime<'_> {
    fn adjacency_memory(&self) -> AdjacencyReadMemory<'_> {
        AdjacencyReadMemory {
            budget_bytes: self.context.memory.blocking_operator_bytes.get(),
            account: Some(self.account),
        }
    }

    fn prepare(&self, input: &Binding) -> Result<(Binding, Vec<QueryMemoryLease>)> {
        let mut leases = vec![self.account.reserve(binding_memory_bytes(input))?];
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
                GraphEntityKind::Node => {
                    let node = self.store.node_owned(NodeId(id))?.ok_or_else(|| {
                        HawDBError::Execution("bound node disappeared during MATCH".to_string())
                    })?;
                    leases.push(
                        self.account.reserve(
                            import
                                .variable
                                .len()
                                .saturating_add(node_memory_bytes(&node)),
                        )?,
                    );
                    row.nodes.insert(import.variable.clone(), node);
                }
                GraphEntityKind::Relationship => {
                    let source = NodeId(entity_id(values, "source_id")?);
                    let mut found = None;
                    self.store.visit_ordered_adjacent_relationships_owned(
                        source,
                        None,
                        AdjacencyDirection::Outgoing,
                        self.adjacency_memory(),
                        &mut |relationship| {
                            runtime_checkpoint(self.context.task_context)?;
                            if relationship.id == RelId(id) {
                                found = Some(relationship);
                                Ok(ScanControl::Stop)
                            } else {
                                Ok(ScanControl::Continue)
                            }
                        },
                    )?;
                    let relationship = found.ok_or_else(|| {
                        HawDBError::Execution(
                            "bound relationship disappeared during MATCH".to_string(),
                        )
                    })?;
                    leases.push(
                        self.account.reserve(
                            import
                                .variable
                                .len()
                                .saturating_add(relationship_memory_bytes(&relationship)),
                        )?,
                    );
                    row.relationships
                        .insert(import.variable.clone(), relationship);
                }
            }
        }
        Ok((row, leases))
    }

    fn visit(
        &self,
        index: usize,
        row: &Binding,
        used: &BTreeSet<RelId>,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(self.context.task_context)?;
        let Some(step) = self.program.steps.get(index) else {
            if let Some(predicate) = &self.program.predicate
                && !evaluate_predicate_with_memory(
                    predicate,
                    self.context.catalog,
                    self.store,
                    row,
                    self.context.observer,
                    self.adjacency_memory(),
                )?
            {
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
                self.store.visit_nodes_owned(label, &mut |node| {
                    runtime_checkpoint(self.context.task_context)?;
                    if !self.node_matches(pattern, &node) {
                        return Ok(ScanControl::Continue);
                    }
                    let _lease = self.account.reserve(
                        binding_memory_bytes(row)
                            .saturating_add(node_memory_bytes(&node))
                            .saturating_add(pattern.variable.len()),
                    )?;
                    let mut next = row.clone();
                    next.nodes.insert(pattern.variable.clone(), node);
                    self.visit(index + 1, &next, used, emit)
                })
            }
            GraphMatchStep::Expand {
                source,
                relationship,
                rel_type,
                properties,
                direction,
                min_hops,
                max_hops,
                target,
            } => {
                let Some(source_node) = row.nodes.get(source) else {
                    if row.values.get(source) == Some(&Value::Null) {
                        return Ok(ScanControl::Continue);
                    }
                    return Err(HawDBError::Execution(format!(
                        "missing MATCH source '{source}'"
                    )));
                };
                let rel_type_id = if rel_type.is_empty() {
                    None
                } else {
                    let Some(id) = self.context.catalog.rel_type_id(rel_type) else {
                        return Ok(ScanControl::Continue);
                    };
                    Some(id)
                };
                let labels = label_ids_for_pattern(self.context.catalog, &target.label);
                if *min_hops != 1 || *max_hops != 1 {
                    if relationship.is_some()
                        || !properties.is_empty()
                        || *direction != hawdb_core::RelationshipDirection::Outgoing
                        || rel_type_id.is_none()
                    {
                        return Err(HawDBError::Execution("bounded MATCH expansion requires an outgoing typed pattern without relationship bindings".to_string()));
                    }
                    return visit_bounded_expand_targets(
                        self.store,
                        BoundedExpandSpec {
                            source: source_node.id,
                            rel_type_id: rel_type_id.unwrap(),
                            target_label_ids: labels.as_deref(),
                            min_hops: *min_hops,
                            max_hops: *max_hops,
                        },
                        self.adjacency_memory(),
                        self.context.task_context,
                        &mut |node, _| self.visit_target(index, row, used, target, node, emit),
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
                visit_one_hop_relationships_with_context(
                    self.store,
                    OneHopRelationshipSpec {
                        source: source_node.id,
                        rel_type_id,
                        target_label_ids: labels.as_deref(),
                        rel_properties: properties,
                        relationship_scan_filter: None,
                        direction: *direction,
                    },
                    self.adjacency_memory(),
                    self.context.observer,
                    self.context.task_context,
                    &mut |edge, node| {
                        if used.contains(&edge.id)
                            || bound_relationship.is_some_and(|id| id != edge.id)
                            || !properties_match(properties, &edge.properties)
                        {
                            return Ok(ScanControl::Continue);
                        }
                        let _lease = self.account.reserve(
                            binding_memory_bytes(row)
                                .saturating_add(relationship_memory_bytes(&edge))
                                .saturating_add(used.len().saturating_add(1).saturating_mul(48)),
                        )?;
                        let mut next = row.clone();
                        let mut used = used.clone();
                        used.insert(edge.id);
                        if let Some(variable) = relationship {
                            next.relationships.insert(variable.clone(), edge);
                        }
                        self.visit_target(index, &next, &used, target, node, emit)
                    },
                )
            }
        }
    }

    fn visit_target(
        &self,
        index: usize,
        row: &Binding,
        used: &BTreeSet<RelId>,
        pattern: &GraphMatchNode,
        node: NodeRecord,
        emit: &mut dyn FnMut(&Binding) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        if !self.node_matches(pattern, &node)
            || row
                .nodes
                .get(&pattern.variable)
                .is_some_and(|bound| bound.id != node.id)
            || row.values.get(&pattern.variable) == Some(&Value::Null)
        {
            return Ok(ScanControl::Continue);
        }
        let _lease = self.account.reserve(
            binding_memory_bytes(row)
                .saturating_add(node_memory_bytes(&node))
                .saturating_add(pattern.variable.len()),
        )?;
        let mut next = row.clone();
        next.nodes.insert(pattern.variable.clone(), node);
        self.visit(index + 1, &next, used, emit)
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
