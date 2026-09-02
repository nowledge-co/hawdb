//! Storage-implementation-neutral node scan and lookup operators.

use crate::binding::{binding_memory_bytes, Binding};
use crate::expression::{
    evaluate_predicate_with_memory, insert_projected_value, project_value,
    property_filter_from_predicate,
};
use crate::kernel::{push_bounded_operator_binding, OperatorMemoryTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{runtime_checkpoint, AccountedBindingBatch, BatchControl, BindingBatch};
use crate::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
};
use crate::store::{AdjacencyReadMemory, GraphExecutionRead, ScanControl};
use crate::traversal::{
    visit_bounded_expand_targets, visit_one_hop_relationships_with_budget, BoundedExpandSpec,
    OneHopRelationshipSpec,
};
use crate::{ExecutionLimit, QueryMemoryAccount};
use skein_core::{
    Catalog, LabelId, RelTypeId, RelationshipDirection, Result, RuntimeTaskContext, SkeinError,
    Value,
};
use skein_plan::{
    ComparisonOp, ExactPropertySeekBranch, NodeProjectionAccess, Predicate, Projection,
};
use skein_storage::{
    AdjacencyDirection, NodeId, NodeRecord, ProjectedNodeRecord, PropertyFilter, RangeBound,
    ScanPredicate, ScanPruningReport, ScanPruningStrategy, ScanPruningTargetKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

const UNION_DEDUP_ENTRY_BYTES: usize = 64;

#[derive(Clone, Copy)]
pub struct NodeScanSpec<'a> {
    pub variable: &'a str,
    pub label: &'a str,
    pub property_filter: Option<&'a PropertyFilter>,
}

#[derive(Clone, Copy)]
pub struct NodeProjectionScanSpec<'a> {
    pub variable: &'a str,
    pub label: &'a str,
    pub access: &'a NodeProjectionAccess,
    pub required_properties: &'a [String],
    pub predicate: Option<&'a Predicate>,
    pub items: &'a [Projection],
}

#[derive(Clone, Copy)]
pub struct NodeScanContext<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn GraphExecutionRead,
    pub execution_limit: ExecutionLimit,
    pub memory_budget: NonZeroUsize,
    pub memory_account: &'a QueryMemoryAccount,
    pub batch_memory_budget: NonZeroUsize,
    pub batch_memory_account: &'a QueryMemoryAccount,
    pub batch_rows: usize,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl NodeScanContext<'_> {
    fn memory_tracker(self) -> OperatorMemoryTracker {
        OperatorMemoryTracker::with_account(self.memory_budget, self.memory_account.clone())
    }

    fn output_batch(self, operator: &'static str) -> AccountedBindingBatch {
        AccountedBindingBatch::with_account(
            operator,
            self.batch_rows,
            self.batch_memory_budget,
            self.batch_memory_account.clone(),
        )
    }
}

#[derive(Default)]
pub struct AdjacencyExpandFilters<'a> {
    pub relationship_scan_filter: Option<&'a PropertyFilter>,
    pub target_scan_filter: Option<&'a PropertyFilter>,
}

pub struct ExpandedBinding {
    pub binding: Binding,
    pub target_id: Option<NodeId>,
    pub hop: usize,
}

#[derive(Clone, Copy)]
pub struct AdjacencyExpandSpec<'a> {
    pub source_variable: &'a str,
    pub rel_variable: Option<&'a str>,
    pub rel_properties: &'a BTreeMap<String, Value>,
    pub direction: RelationshipDirection,
    pub target_variable: &'a str,
    pub min_hops: usize,
    pub max_hops: usize,
    pub optional: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn stream_expand_binding(
    binding: &Binding,
    spec: AdjacencyExpandSpec<'_>,
    rel_type_id: Option<RelTypeId>,
    target_label_ids: Option<&[LabelId]>,
    filters: &AdjacencyExpandFilters<'_>,
    store: &dyn GraphExecutionRead,
    memory: AdjacencyReadMemory<'_>,
    task_context: Option<&RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
    consumer: &mut dyn FnMut(ExpandedBinding) -> Result<ScanControl>,
) -> Result<ScanControl> {
    runtime_checkpoint(task_context)?;
    let source = binding.nodes.get(spec.source_variable).ok_or_else(|| {
        SkeinError::Execution(format!(
            "missing variable '{}' during expand",
            spec.source_variable
        ))
    })?;
    let bound_target_id = binding.nodes.get(spec.target_variable).map(|node| node.id);
    let mut matched = false;
    let control = if spec.rel_variable.is_some()
        || !spec.rel_properties.is_empty()
        || filters.relationship_scan_filter.is_some()
        || spec.direction != RelationshipDirection::Outgoing
    {
        visit_one_hop_relationships_with_budget(
            store,
            OneHopRelationshipSpec {
                source: source.id,
                rel_type_id,
                target_label_ids,
                rel_properties: spec.rel_properties,
                relationship_scan_filter: filters.relationship_scan_filter,
                direction: spec.direction,
            },
            memory,
            observer,
            &mut |relationship, target| {
                runtime_checkpoint(task_context)?;
                if bound_target_id.is_some_and(|node_id| node_id != target.id)
                    || filters
                        .target_scan_filter
                        .is_some_and(|filter| !node_matches_property_filter(&target, filter))
                {
                    return Ok(ScanControl::Continue);
                }
                let mut nodes = binding.nodes.clone();
                nodes.insert(spec.target_variable.to_string(), target.clone());
                let mut relationships = binding.relationships.clone();
                if let Some(rel_variable) = spec.rel_variable {
                    relationships.insert(rel_variable.to_string(), relationship);
                }
                let expanded = ExpandedBinding {
                    binding: Binding {
                        values: binding.values.clone(),
                        nodes,
                        relationships,
                    },
                    target_id: Some(target.id),
                    hop: 1,
                };
                ensure_expanded_binding_fits(&expanded, memory.budget_bytes)?;
                matched = true;
                consumer(expanded)
            },
        )?
    } else {
        visit_bounded_expand_targets(
            store,
            BoundedExpandSpec {
                source: source.id,
                rel_type_id: rel_type_id.expect("typed bounded expand checked by planner"),
                target_label_ids,
                min_hops: spec.min_hops,
                max_hops: spec.max_hops,
            },
            memory,
            task_context,
            &mut |target, hop| {
                if bound_target_id.is_some_and(|node_id| node_id != target.id)
                    || filters
                        .target_scan_filter
                        .is_some_and(|filter| !node_matches_property_filter(&target, filter))
                {
                    return Ok(ScanControl::Continue);
                }
                let mut nodes = binding.nodes.clone();
                nodes.insert(spec.target_variable.to_string(), target.clone());
                let expanded = ExpandedBinding {
                    binding: Binding {
                        values: binding.values.clone(),
                        nodes,
                        relationships: binding.relationships.clone(),
                    },
                    target_id: Some(target.id),
                    hop,
                };
                ensure_expanded_binding_fits(&expanded, memory.budget_bytes)?;
                matched = true;
                consumer(expanded)
            },
        )?
    };
    if control == ScanControl::Stop {
        return Ok(ScanControl::Stop);
    }
    if spec.optional && !matched {
        let mut nodes = binding.nodes.clone();
        nodes.insert(spec.target_variable.to_string(), null_lookup_node());
        let expanded = ExpandedBinding {
            binding: Binding {
                values: binding.values.clone(),
                nodes,
                relationships: binding.relationships.clone(),
            },
            target_id: None,
            hop: 0,
        };
        ensure_expanded_binding_fits(&expanded, memory.budget_bytes)?;
        return consumer(expanded);
    }
    Ok(ScanControl::Continue)
}

pub fn adjacency_exists(
    store: &dyn GraphExecutionRead,
    source: NodeId,
    target: NodeId,
    rel_type_id: RelTypeId,
    direction: RelationshipDirection,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<bool> {
    runtime_checkpoint(task_context)?;
    let mut found = false;
    let mut visit_direction = |adjacency_direction: AdjacencyDirection| {
        let mut visit = |relationship: skein_storage::RelRecord| {
            runtime_checkpoint(task_context)?;
            let matches = match adjacency_direction {
                AdjacencyDirection::Outgoing => {
                    relationship.source == source && relationship.target == target
                }
                AdjacencyDirection::Incoming => {
                    relationship.target == source && relationship.source == target
                }
            };
            if matches {
                found = true;
                Ok(ScanControl::Stop)
            } else {
                Ok(ScanControl::Continue)
            }
        };
        store.visit_adjacent_relationships_owned(
            source,
            Some(rel_type_id),
            adjacency_direction,
            &mut visit,
        )
    };
    match direction {
        RelationshipDirection::Outgoing => {
            visit_direction(AdjacencyDirection::Outgoing)?;
        }
        RelationshipDirection::Incoming => {
            visit_direction(AdjacencyDirection::Incoming)?;
        }
        RelationshipDirection::Undirected => {
            if visit_direction(AdjacencyDirection::Outgoing)? == ScanControl::Continue {
                visit_direction(AdjacencyDirection::Incoming)?;
            }
        }
    }
    Ok(found)
}

fn ensure_expanded_binding_fits(
    expanded: &ExpandedBinding,
    memory_budget_bytes: usize,
) -> Result<()> {
    let bytes = binding_memory_bytes(&expanded.binding);
    if bytes > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "AdjacencyExpandExec result uses {bytes} bytes, exceeding blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    Ok(())
}

pub fn stream_node_scan_batches(
    spec: NodeScanSpec<'_>,
    context: NodeScanContext<'_>,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let exact_label = exact_scan_label_id(context.catalog, spec.label);
    let exact_label_id = exact_label.flatten();
    if !context.store.is_out_of_core()
        && let Some(label_id) = exact_label
        && context
            .store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= context.memory_budget.get()
    {
        let scan = context.store.scan_nodes_with_filter_pruning(
            context.catalog,
            label_id,
            spec.property_filter,
        )?;
        observer.record_scan_pruning_report(scan.report.clone());
        let mut batch = context.output_batch("NodeScanExec");
        let mut emitted = 0usize;
        for node in scan.nodes {
            runtime_checkpoint(context.task_context)?;
            let binding = node_binding(spec.variable, node);
            if !predicate(&binding)? {
                continue;
            }
            if batch.push(binding, emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            emitted = emitted.saturating_add(1);
            if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            if context.execution_limit.is_reached(emitted) {
                break;
            }
        }
        if !batch.is_empty() && batch.emit(emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return Ok(if context.execution_limit.is_reached(emitted) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        });
    }

    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    let mut batch = context.output_batch("NodeScanExec");
    let mut emitted = 0usize;
    let mut visit = |node: NodeRecord| {
        runtime_checkpoint(context.task_context)?;
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        if spec
            .property_filter
            .map(|filter| node_matches_property_filter(&node, filter))
            .is_some_and(|matches| !matches)
        {
            return Ok(ScanControl::Continue);
        }
        let binding = node_binding(spec.variable, node);
        if !predicate(&binding)? {
            return Ok(ScanControl::Continue);
        }
        if batch.push(binding, emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        Ok(if context.execution_limit.is_reached(emitted) {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    };
    let control = context
        .store
        .visit_nodes_owned(exact_label_id, &mut visit)?;
    let candidate_count = context.store.node_count_for_label(exact_label_id);
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: emitted,
        filtered_out_count: candidate_count.saturating_sub(emitted),
    });
    let final_emit_control = batch.emit(emit)?;
    if final_emit_control == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == ScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub fn stream_node_projection_scan_batches(
    spec: NodeProjectionScanSpec<'_>,
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let exact_label = exact_scan_label_id(context.catalog, spec.label);
    let exact_label_id = exact_label.flatten();
    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    let required_properties = spec
        .required_properties
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut batch = context.output_batch("NodeProjectionScanExec");
    let mut emitted = 0usize;
    let mut visited = 0usize;
    let mut visit = |node: ProjectedNodeRecord| {
        runtime_checkpoint(context.task_context)?;
        visited = visited.saturating_add(1);
        let node = NodeRecord {
            id: node.id,
            labels: node.labels,
            properties: node.properties,
        };
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        let binding = node_binding(spec.variable, node);
        if let Some(predicate) = spec.predicate
            && !evaluate_predicate_with_memory(
                predicate,
                context.catalog,
                context.store,
                &binding,
                observer,
                AdjacencyReadMemory {
                    budget_bytes: context.memory_budget.get(),
                    account: Some(context.memory_account),
                },
            )?
        {
            return Ok(ScanControl::Continue);
        }
        let output = if spec.items.is_empty() {
            binding
        } else {
            let mut values = BTreeMap::new();
            for item in spec.items {
                insert_projected_value(
                    &mut values,
                    &item.name,
                    project_value(item, context.catalog, &binding)?,
                );
            }
            Binding::values(values)
        };
        if batch.push(output, emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        Ok(if context.execution_limit.is_reached(emitted) {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    };
    let property_filter = spec
        .predicate
        .and_then(|predicate| property_filter_from_predicate(predicate).ok());
    if spec.access.is_label_scan()
        && !context.store.is_out_of_core()
        && let Some(label_id) = exact_label
        && context
            .store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= context.memory_budget.get()
    {
        let scan = context.store.scan_nodes_with_filter_pruning(
            context.catalog,
            label_id,
            property_filter.as_ref(),
        )?;
        observer.record_scan_pruning_report(scan.report.clone());
        let mut control = ScanControl::Continue;
        for node in scan.nodes {
            let properties = required_properties
                .iter()
                .filter_map(|property| {
                    node.properties
                        .get(property)
                        .cloned()
                        .map(|value| (property.clone(), value))
                })
                .collect();
            control = visit(ProjectedNodeRecord {
                id: node.id,
                labels: node.labels,
                properties,
            })?;
            if control == ScanControl::Stop {
                break;
            }
        }
        if batch.emit(emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return Ok(if control == ScanControl::Stop {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        });
    }
    let control = match (exact_label_id, spec.access) {
        (_, NodeProjectionAccess::LabelScan) => context.store.visit_projected_nodes_owned(
            exact_label_id,
            &required_properties,
            &mut visit,
        )?,
        (Some(label_id), NodeProjectionAccess::PropertyUnion { branches }) => {
            let mut seen = BTreeSet::new();
            let mut dedup_memory = context.memory_tracker();
            let mut control = ScanControl::Continue;
            for branch in branches {
                control = context.store.visit_projected_nodes_by_property_owned(
                    label_id,
                    &branch.property,
                    &branch.values,
                    &required_properties,
                    &mut |node| {
                        if !seen.insert(node.id) {
                            return Ok(ScanControl::Continue);
                        }
                        dedup_memory.try_charge(UNION_DEDUP_ENTRY_BYTES)?;
                        visit(node)
                    },
                )?;
                if control == ScanControl::Stop {
                    break;
                }
            }
            control
        }
        (Some(label_id), access) => context.store.visit_projected_nodes_by_access_owned(
            label_id,
            access,
            &required_properties,
            &mut visit,
        )?,
        (None, _) => ScanControl::Continue,
    };
    let candidate_count = context.store.node_count_for_label(exact_label_id);
    let pruned = !spec.access.is_label_scan();
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: node_projection_access_strategy(spec.access),
        pruned,
        exact_empty: visited == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: candidate_count.saturating_sub(visited),
        candidate_count_before_filter: visited,
        output_count: emitted,
        filtered_out_count: visited.saturating_sub(emitted),
    });
    if batch.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == ScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

fn node_projection_access_strategy(access: &NodeProjectionAccess) -> ScanPruningStrategy {
    match access {
        NodeProjectionAccess::LabelScan => ScanPruningStrategy::FullLabelScan,
        NodeProjectionAccess::PropertyValues { property, values } if values.len() == 1 => {
            ScanPruningStrategy::PropertyEq {
                property: property.clone(),
            }
        }
        NodeProjectionAccess::PropertyValues { property, .. } => ScanPruningStrategy::PropertyIn {
            property: property.clone(),
        },
        NodeProjectionAccess::PropertyUnion { .. } => ScanPruningStrategy::OrUnion,
        NodeProjectionAccess::CompositeEquality { predicates } => {
            ScanPruningStrategy::CompositePropertyEq {
                properties: predicates
                    .iter()
                    .map(|(property, _)| property.clone())
                    .collect(),
            }
        }
        NodeProjectionAccess::CompositeRange { seek } => {
            ScanPruningStrategy::CompositePropertyRange {
                properties: seek.index_properties.clone(),
            }
        }
        NodeProjectionAccess::PropertyRange { property, .. } => {
            ScanPruningStrategy::PropertyRange {
                property: property.clone(),
            }
        }
        NodeProjectionAccess::FullText { property, .. } => ScanPruningStrategy::FullText {
            property: property.clone(),
        },
    }
}

pub fn execute_node_scan(
    spec: NodeScanSpec<'_>,
    context: NodeScanContext<'_>,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    observer: &dyn ExecutionObserver,
) -> Result<Vec<Binding>> {
    let exact_label = exact_scan_label_id(context.catalog, spec.label);
    let exact_label_id = exact_label.flatten();
    if !context.store.is_out_of_core()
        && let Some(label_id) = exact_label
        && context
            .store
            .node_count_for_label(label_id)
            .saturating_mul(std::mem::size_of::<&NodeRecord>())
            <= context.memory_budget.get()
    {
        let scan = context.store.scan_nodes_with_filter_pruning(
            context.catalog,
            label_id,
            spec.property_filter,
        )?;
        observer.record_scan_pruning_report(scan.report.clone());
        let mut output = Vec::new();
        let mut tracker = context.memory_tracker();
        for node in scan.nodes {
            let binding = node_binding(spec.variable, node);
            if !predicate(&binding)? {
                continue;
            }
            push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)?;
            if context.execution_limit.is_reached(output.len()) {
                break;
            }
        }
        return Ok(output);
    }

    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    let mut output = Vec::new();
    let mut tracker = context.memory_tracker();
    let mut visit = |node: NodeRecord| {
        if exact_label.is_none() && !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        if spec
            .property_filter
            .map(|filter| node_matches_property_filter(&node, filter))
            .is_some_and(|matches| !matches)
        {
            return Ok(ScanControl::Continue);
        }
        let binding = node_binding(spec.variable, node);
        if !predicate(&binding)? {
            return Ok(ScanControl::Continue);
        }
        push_bounded_operator_binding("NodeScanExec", &mut output, binding, &mut tracker)?;
        Ok(if context.execution_limit.is_reached(output.len()) {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    };
    context
        .store
        .visit_nodes_owned(exact_label_id, &mut visit)?;
    let candidate_count = context.store.node_count_for_label(exact_label_id);
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: exact_label_id,
        rel_type_id: None,
        strategy: ScanPruningStrategy::FullLabelScan,
        pruned: false,
        exact_empty: candidate_count == 0,
        candidate_count_before_pruning: candidate_count,
        pruned_candidate_count: 0,
        candidate_count_before_filter: candidate_count,
        output_count: output.len(),
        filtered_out_count: candidate_count.saturating_sub(output.len()),
    });
    Ok(output)
}

pub fn stream_index_node_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    values: &[Value],
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    let mut batch = context.output_batch("IndexNodeSeekExec");
    let mut emitted = 0usize;
    let mut matched = 0usize;
    let mut visit = |node| {
        matched = matched.saturating_add(1);
        if batch.push(node_binding(variable, node), emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        emitted = emitted.saturating_add(1);
        if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
            return Ok(ScanControl::Stop);
        }
        Ok(if context.execution_limit.is_reached(emitted) {
            ScanControl::Stop
        } else {
            ScanControl::Continue
        })
    };
    let control = context
        .store
        .visit_nodes_by_property_owned(label_id, property, values, &mut visit)?;
    let final_emit_control = batch.emit(emit)?;
    let candidate_count_before_pruning = context.store.node_count_for_label(Some(label_id));
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if values.len() == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: matched == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning.saturating_sub(matched),
        candidate_count_before_filter: matched,
        output_count: matched.min(context.execution_limit.output_rows.unwrap_or(usize::MAX)),
        filtered_out_count: 0,
    });
    if final_emit_control == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == ScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
}

pub fn stream_index_node_union_seek_batches(
    variable: &str,
    label: &str,
    branches: &[ExactPropertySeekBranch],
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    let mut batch = context.output_batch("IndexNodeUnionSeekExec");
    let mut seen = BTreeSet::new();
    let mut dedup_memory = context.memory_tracker();
    let mut emitted = 0usize;
    let mut control = ScanControl::Continue;
    for branch in branches {
        control = context.store.visit_nodes_by_property_owned(
            label_id,
            &branch.property,
            &branch.values,
            &mut |node| {
                if !seen.insert(node.id) {
                    return Ok(ScanControl::Continue);
                }
                dedup_memory.try_charge(UNION_DEDUP_ENTRY_BYTES)?;
                if batch.push(node_binding(variable, node), emit)? == BatchControl::Stop {
                    return Ok(ScanControl::Stop);
                }
                emitted = emitted.saturating_add(1);
                if batch.is_full() && batch.emit(emit)? == BatchControl::Stop {
                    return Ok(ScanControl::Stop);
                }
                Ok(if context.execution_limit.is_reached(emitted) {
                    ScanControl::Stop
                } else {
                    ScanControl::Continue
                })
            },
        )?;
        if control == ScanControl::Stop {
            break;
        }
    }
    let final_emit_control = batch.emit(emit)?;
    let candidate_count_before_pruning = context.store.node_count_for_label(Some(label_id));
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: ScanPruningStrategy::OrUnion,
        pruned: true,
        exact_empty: seen.is_empty(),
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning.saturating_sub(seen.len()),
        candidate_count_before_filter: seen.len(),
        output_count: emitted,
        filtered_out_count: 0,
    });
    if final_emit_control == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(match control {
        ScanControl::Continue => BatchControl::Continue,
        ScanControl::Stop => BatchControl::Stop,
    })
}

#[derive(Clone, Copy)]
pub struct NodeColumnLookupSpec<'a> {
    pub variable: &'a str,
    pub label: &'a str,
    pub property: &'a str,
    pub column: &'a str,
    pub optional: bool,
}

pub fn execute_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
) -> Result<Vec<Binding>> {
    if let Some(Some(label_id)) = exact_scan_label_id(context.catalog, spec.label) {
        return execute_indexed_node_column_lookup(spec, input, label_id, context, observer);
    }

    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    let mut output = Vec::new();
    let mut tracker = context.memory_tracker();
    for binding in input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        let mut matched = false;
        let mut visit = |node: NodeRecord| {
            if node_matches_label_pattern(&node, label_ids.as_deref())
                && node.properties.get(spec.property) == Some(expected)
            {
                let mut next = binding.clone();
                next.nodes.insert(spec.variable.to_string(), node);
                push_bounded_operator_binding(
                    "NodeColumnLookupExec",
                    &mut output,
                    next,
                    &mut tracker,
                )?;
                matched = true;
                if context.execution_limit.is_reached(output.len()) {
                    return Ok(ScanControl::Stop);
                }
            }
            Ok(ScanControl::Continue)
        };
        context.store.visit_nodes_owned(None, &mut visit)?;
        if context.execution_limit.is_reached(output.len()) {
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if context.execution_limit.is_reached(output.len()) {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

fn execute_indexed_node_column_lookup(
    spec: NodeColumnLookupSpec<'_>,
    input: Vec<Binding>,
    label_id: LabelId,
    context: NodeScanContext<'_>,
    observer: &dyn ExecutionObserver,
) -> Result<Vec<Binding>> {
    let mut lookup_values = BTreeSet::new();
    for binding in &input {
        let expected = binding.values.get(spec.column).ok_or_else(|| {
            SkeinError::Execution(format!(
                "missing column '{}' during node column lookup",
                spec.column
            ))
        })?;
        lookup_values.insert(expected.clone());
    }

    let mut unique_candidate_ids = BTreeSet::new();
    let mut output = Vec::new();
    let mut tracker = context.memory_tracker();
    for binding in input {
        let expected = binding
            .values
            .get(spec.column)
            .expect("lookup column was validated before index lookup")
            .clone();
        let mut matched = false;
        let mut visit = |node: NodeRecord| {
            unique_candidate_ids.insert(node.id);
            let mut next = binding.clone();
            next.nodes.insert(spec.variable.to_string(), node);
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            matched = true;
            Ok(if context.execution_limit.is_reached(output.len()) {
                ScanControl::Stop
            } else {
                ScanControl::Continue
            })
        };
        context.store.visit_nodes_by_property_owned(
            label_id,
            spec.property,
            std::slice::from_ref(&expected),
            &mut visit,
        )?;
        if context.execution_limit.is_reached(output.len()) {
            record_node_column_lookup_report(
                label_id,
                spec.property,
                lookup_values.len(),
                unique_candidate_ids.len(),
                output.len(),
                context.store,
                observer,
            );
            return Ok(output);
        }
        if spec.optional && !matched {
            let mut next = binding;
            next.nodes
                .insert(spec.variable.to_string(), null_lookup_node());
            push_bounded_operator_binding("NodeColumnLookupExec", &mut output, next, &mut tracker)?;
            if context.execution_limit.is_reached(output.len()) {
                record_node_column_lookup_report(
                    label_id,
                    spec.property,
                    lookup_values.len(),
                    unique_candidate_ids.len(),
                    output.len(),
                    context.store,
                    observer,
                );
                return Ok(output);
            }
        }
    }

    record_node_column_lookup_report(
        label_id,
        spec.property,
        lookup_values.len(),
        unique_candidate_ids.len(),
        output.len(),
        context.store,
        observer,
    );
    Ok(output)
}

fn record_node_column_lookup_report(
    label_id: LabelId,
    property: &str,
    lookup_value_count: usize,
    candidate_count_before_filter: usize,
    output_count: usize,
    store: &dyn GraphExecutionRead,
    observer: &dyn ExecutionObserver,
) {
    let candidate_count_before_pruning = store.node_count_for_label(Some(label_id));
    observer.record_scan_pruning_report(ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: Some(label_id),
        rel_type_id: None,
        strategy: if lookup_value_count == 0 {
            ScanPruningStrategy::Empty
        } else if lookup_value_count == 1 {
            ScanPruningStrategy::PropertyEq {
                property: property.to_string(),
            }
        } else {
            ScanPruningStrategy::PropertyIn {
                property: property.to_string(),
            }
        },
        pruned: true,
        exact_empty: candidate_count_before_filter == 0,
        candidate_count_before_pruning,
        pruned_candidate_count: candidate_count_before_pruning
            .saturating_sub(candidate_count_before_filter),
        candidate_count_before_filter,
        output_count,
        filtered_out_count: 0,
    });
}

pub fn single_node_binding(variable: &str, node: NodeRecord) -> Binding {
    node_binding(variable, node)
}

pub fn null_lookup_node() -> NodeRecord {
    NodeRecord {
        id: NodeId(0),
        labels: BTreeSet::new(),
        properties: BTreeMap::new(),
    }
}

fn node_binding(variable: &str, node: NodeRecord) -> Binding {
    Binding {
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(variable.to_string(), node)]),
        relationships: BTreeMap::new(),
    }
}

fn exact_scan_label_id(catalog: &Catalog, label: &str) -> Option<Option<LabelId>> {
    if label.is_empty() {
        return Some(None);
    }
    if label.contains(':') {
        return None;
    }
    catalog.label_id(label).map(Some)
}

pub fn source_scan_pruning_strategy(predicate: &ScanPredicate) -> ScanPruningStrategy {
    match predicate {
        ScanPredicate::False => ScanPruningStrategy::Empty,
        ScanPredicate::Eq { property, .. } => ScanPruningStrategy::PropertyEq {
            property: property.clone(),
        },
        ScanPredicate::In { property, .. } => ScanPruningStrategy::PropertyIn {
            property: property.clone(),
        },
        ScanPredicate::Range { property, .. } => ScanPruningStrategy::PropertyRange {
            property: property.clone(),
        },
        ScanPredicate::IsNull { property } | ScanPredicate::IsMissing { property } => {
            ScanPruningStrategy::PropertyMissingOrNull {
                property: property.clone(),
            }
        }
        ScanPredicate::Exists { property } => ScanPruningStrategy::PropertyExists {
            property: property.clone(),
        },
        ScanPredicate::Or(_) => ScanPruningStrategy::OrUnion,
        ScanPredicate::And(predicates) => predicates
            .iter()
            .map(source_scan_pruning_strategy)
            .find(|strategy| !matches!(strategy, ScanPruningStrategy::FullLabelScan))
            .unwrap_or(ScanPruningStrategy::FullLabelScan),
        ScanPredicate::True => ScanPruningStrategy::FullLabelScan,
    }
}

pub fn source_storage_scan_predicate(
    predicate: &Predicate,
    variable: &str,
) -> Option<ScanPredicate> {
    match predicate {
        Predicate::And(predicates) => {
            let predicates = predicates
                .iter()
                .filter_map(|predicate| source_storage_scan_predicate(predicate, variable))
                .collect::<Vec<_>>();
            match predicates.len() {
                0 => None,
                1 => predicates.into_iter().next(),
                _ => Some(ScanPredicate::And(predicates)),
            }
        }
        Predicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| source_storage_scan_predicate(predicate, variable))
            .collect::<Option<Vec<_>>>()
            .and_then(|predicates| {
                (!predicates.is_empty()).then_some(ScanPredicate::Or(predicates))
            }),
        Predicate::PropertyEq {
            variable: candidate,
            property,
            value,
        } if candidate == variable => Some(ScanPredicate::Eq {
            property: property.clone(),
            value: value.clone(),
        }),
        Predicate::PropertyIn {
            variable: candidate,
            property,
            values,
        } if candidate == variable => Some(ScanPredicate::In {
            property: property.clone(),
            values: values.clone(),
        }),
        Predicate::PropertyCompare {
            variable: candidate,
            property,
            op,
            value,
        } if candidate == variable => {
            let bound = RangeBound {
                value: value.clone(),
                inclusive: matches!(op, ComparisonOp::Gte | ComparisonOp::Lte),
            };
            let (lower, upper) = match op {
                ComparisonOp::Gt | ComparisonOp::Gte => (Some(bound), None),
                ComparisonOp::Lt | ComparisonOp::Lte => (None, Some(bound)),
            };
            Some(ScanPredicate::Range {
                property: property.clone(),
                lower,
                upper,
            })
        }
        Predicate::PropertyIsNull {
            variable: candidate,
            property,
        } if candidate == variable => Some(ScanPredicate::Or(vec![
            ScanPredicate::IsNull {
                property: property.clone(),
            },
            ScanPredicate::IsMissing {
                property: property.clone(),
            },
        ])),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observer::NoopExecutionObserver;
    use crate::store::{PrunedNodeScan, PrunedRelationshipScan};
    use skein_storage::{AdjacencyDirection, RelId, RelRecord};
    use std::cell::Cell;

    struct HighDegreeStore {
        degree: usize,
        relationship_visits: Cell<usize>,
    }

    impl GraphExecutionRead for HighDegreeStore {
        fn is_out_of_core(&self) -> bool {
            false
        }

        fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
            Ok(Some(NodeRecord {
                id,
                labels: BTreeSet::new(),
                properties: BTreeMap::new(),
            }))
        }

        fn node_count_for_label(&self, _label_id: Option<LabelId>) -> usize {
            self.degree.saturating_add(1)
        }

        fn relationship_count_for_type(&self, _rel_type: Option<RelTypeId>) -> usize {
            self.degree
        }

        fn visit_nodes_owned(
            &self,
            _label_id: Option<LabelId>,
            _consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
        ) -> Result<ScanControl> {
            panic!("node scans are not used by adjacency expansion tests")
        }

        fn visit_nodes_by_property_owned(
            &self,
            _label_id: LabelId,
            _property: &str,
            _values: &[Value],
            _consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
        ) -> Result<ScanControl> {
            panic!("property scans are not used by adjacency expansion tests")
        }

        fn visit_projected_nodes_by_access_owned(
            &self,
            _label_id: LabelId,
            _access: &NodeProjectionAccess,
            _required_properties: &BTreeSet<String>,
            _consumer: &mut dyn FnMut(ProjectedNodeRecord) -> Result<ScanControl>,
        ) -> Result<ScanControl> {
            panic!("projected scans are not used by adjacency expansion tests")
        }

        fn visit_adjacent_relationships_owned(
            &self,
            node_id: NodeId,
            rel_type: Option<RelTypeId>,
            direction: AdjacencyDirection,
            consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
        ) -> Result<ScanControl> {
            assert_eq!(node_id, NodeId(0));
            assert_eq!(rel_type, Some(RelTypeId(0)));
            assert_eq!(direction, AdjacencyDirection::Outgoing);
            for offset in 0..self.degree {
                self.relationship_visits
                    .set(self.relationship_visits.get().saturating_add(1));
                if consumer(RelRecord {
                    id: RelId(offset as u64),
                    source: NodeId(0),
                    target: NodeId(offset as u64 + 1),
                    rel_type: RelTypeId(0),
                    properties: BTreeMap::from([("keep".to_string(), Value::Bool(true))]),
                })? == ScanControl::Stop
                {
                    return Ok(ScanControl::Stop);
                }
            }
            Ok(ScanControl::Continue)
        }

        fn visit_adjacent_relationships_with_filter_owned(
            &self,
            node_id: NodeId,
            rel_type: Option<RelTypeId>,
            direction: AdjacencyDirection,
            _filter: &PropertyFilter,
            consumer: &mut dyn FnMut(RelRecord) -> Result<ScanControl>,
        ) -> Result<(ScanControl, Option<ScanPruningReport>)> {
            self.visit_adjacent_relationships_owned(node_id, rel_type, direction, consumer)
                .map(|control| (control, None))
        }

        fn scan_relationships_with_filter_pruning<'a>(
            &'a self,
            _rel_type: Option<RelTypeId>,
            _filter: Option<&PropertyFilter>,
        ) -> Result<PrunedRelationshipScan<'a>> {
            panic!("relationship scans are not used by adjacency expansion tests")
        }

        fn scan_nodes_with_filter_pruning<'a>(
            &'a self,
            _catalog: &Catalog,
            _label_id: Option<LabelId>,
            _filter: Option<&PropertyFilter>,
        ) -> Result<PrunedNodeScan<'a>> {
            panic!("node scans are not used by adjacency expansion tests")
        }
    }

    fn high_degree_expand_visits(
        rel_variable: Option<&str>,
        rel_properties: BTreeMap<String, Value>,
    ) -> (usize, usize) {
        let store = HighDegreeStore {
            degree: 100_000,
            relationship_visits: Cell::new(0),
        };
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(
                "source".to_string(),
                NodeRecord {
                    id: NodeId(0),
                    labels: BTreeSet::new(),
                    properties: BTreeMap::new(),
                },
            )]),
            relationships: BTreeMap::new(),
        };
        let mut emitted = 0usize;
        let control = stream_expand_binding(
            &binding,
            AdjacencyExpandSpec {
                source_variable: "source",
                rel_variable,
                rel_properties: &rel_properties,
                direction: RelationshipDirection::Outgoing,
                target_variable: "target",
                min_hops: 1,
                max_hops: 1,
                optional: false,
            },
            Some(RelTypeId(0)),
            None,
            &AdjacencyExpandFilters::default(),
            &store,
            AdjacencyReadMemory {
                budget_bytes: 1024 * 1024,
                account: None,
            },
            None,
            &NoopExecutionObserver,
            &mut |_| {
                emitted = emitted.saturating_add(1);
                Ok(if emitted == 50 {
                    ScanControl::Stop
                } else {
                    ScanControl::Continue
                })
            },
        )
        .unwrap();
        assert_eq!(control, ScanControl::Stop);
        (emitted, store.relationship_visits.get())
    }

    #[test]
    fn adjacency_exists_stops_after_the_first_matching_target() {
        let store = HighDegreeStore {
            degree: 100_000,
            relationship_visits: Cell::new(0),
        };

        assert!(adjacency_exists(
            &store,
            NodeId(0),
            NodeId(17),
            RelTypeId(0),
            RelationshipDirection::Outgoing,
            None,
        )
        .unwrap());
        assert_eq!(store.relationship_visits.get(), 17);
    }

    #[test]
    fn exact_label_detection_rejects_union_patterns() {
        let mut catalog = Catalog::default();
        let memory = catalog.get_or_create_label("Memory");

        assert_eq!(exact_scan_label_id(&catalog, "Memory"), Some(Some(memory)));
        assert_eq!(exact_scan_label_id(&catalog, ""), Some(None));
        assert_eq!(exact_scan_label_id(&catalog, "Memory:Source"), None);
        assert_eq!(exact_scan_label_id(&catalog, "Missing"), None);
    }

    #[test]
    fn source_predicate_pushdown_requires_matching_variable() {
        let predicate = Predicate::PropertyCompare {
            variable: "source".to_string(),
            property: "created_at".to_string(),
            op: ComparisonOp::Gte,
            value: Value::Int(10),
        };

        assert!(matches!(
            source_storage_scan_predicate(&predicate, "source"),
            Some(ScanPredicate::Range {
                lower: Some(RangeBound {
                    inclusive: true,
                    ..
                }),
                upper: None,
                ..
            })
        ));
        assert_eq!(source_storage_scan_predicate(&predicate, "other"), None);
    }

    #[test]
    fn bounded_one_hop_expand_stops_storage_visit_at_limit() {
        assert_eq!(high_degree_expand_visits(None, BTreeMap::new()), (50, 50));
    }

    #[test]
    fn relationship_binding_expand_stops_storage_visit_at_limit() {
        assert_eq!(
            high_degree_expand_visits(Some("relationship"), BTreeMap::new()),
            (50, 50)
        );
    }

    #[test]
    fn filtered_expand_uses_only_the_bounded_adjacency_visit() {
        assert_eq!(
            high_degree_expand_visits(
                None,
                BTreeMap::from([("keep".to_string(), Value::Bool(true))]),
            ),
            (50, 50)
        );
    }
}
