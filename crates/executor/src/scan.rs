//! Storage-implementation-neutral node scan and lookup operators.

use crate::binding::{binding_memory_bytes, Binding};
use crate::kernel::{push_bounded_operator_binding, OperatorMemoryTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{runtime_checkpoint, BatchControl, BindingBatch};
use crate::predicate::{
    label_ids_for_pattern, node_matches_label_pattern, node_matches_property_filter,
};
use crate::store::{GraphExecutionRead, ScanControl};
use crate::traversal::{bounded_expand_targets, one_hop_relationships_with_budget};
use crate::ExecutionLimit;
use skein_core::{
    Catalog, LabelId, RelTypeId, RelationshipDirection, Result, RuntimeTaskContext, SkeinError,
    Value,
};
use skein_plan::{ComparisonOp, Predicate};
use skein_storage::{
    NodeId, NodeRecord, PropertyFilter, RangeBound, ScanPredicate, ScanPruningReport,
    ScanPruningStrategy, ScanPruningTargetKind,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

#[derive(Clone, Copy)]
pub struct NodeScanSpec<'a> {
    pub variable: &'a str,
    pub label: &'a str,
    pub property_filter: Option<&'a PropertyFilter>,
}

#[derive(Clone, Copy)]
pub struct NodeScanContext<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn GraphExecutionRead,
    pub execution_limit: ExecutionLimit,
    pub memory_budget: NonZeroUsize,
    pub batch_rows: usize,
    pub task_context: Option<&'a RuntimeTaskContext>,
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
pub fn expand_binding(
    binding: Binding,
    spec: AdjacencyExpandSpec<'_>,
    rel_type_id: Option<RelTypeId>,
    target_label_ids: Option<&[LabelId]>,
    filters: &AdjacencyExpandFilters<'_>,
    store: &dyn GraphExecutionRead,
    memory_budget_bytes: usize,
    task_context: Option<&RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
) -> Result<Vec<ExpandedBinding>> {
    runtime_checkpoint(task_context)?;
    let source = binding.nodes.get(spec.source_variable).ok_or_else(|| {
        SkeinError::Execution(format!(
            "missing variable '{}' during expand",
            spec.source_variable
        ))
    })?;
    let bound_target_id = binding.nodes.get(spec.target_variable).map(|node| node.id);
    let mut output = Vec::new();
    let mut output_bytes = 0usize;
    if spec.rel_variable.is_some()
        || !spec.rel_properties.is_empty()
        || filters.relationship_scan_filter.is_some()
        || spec.direction != RelationshipDirection::Outgoing
    {
        for (relationship, target) in one_hop_relationships_with_budget(
            store,
            source.id,
            rel_type_id,
            target_label_ids,
            spec.rel_properties,
            filters.relationship_scan_filter,
            spec.direction,
            memory_budget_bytes,
            observer,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
            }
            let mut nodes = binding.nodes.clone();
            nodes.insert(spec.target_variable.to_string(), target.clone());
            let mut relationships = binding.relationships.clone();
            if let Some(rel_variable) = spec.rel_variable {
                relationships.insert(rel_variable.to_string(), relationship.clone());
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
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    } else {
        for (target, hop) in bounded_expand_targets(
            store,
            source.id,
            rel_type_id.expect("typed bounded expand checked by planner"),
            target_label_ids,
            spec.min_hops,
            spec.max_hops,
            memory_budget_bytes,
        )? {
            runtime_checkpoint(task_context)?;
            if bound_target_id.is_some_and(|node_id| node_id != target.id)
                || filters
                    .target_scan_filter
                    .is_some_and(|filter| !node_matches_property_filter(&target, filter))
            {
                continue;
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
            admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
            output.push(expanded);
        }
    }
    if spec.optional && output.is_empty() {
        let mut nodes = binding.nodes;
        nodes.insert(spec.target_variable.to_string(), null_lookup_node());
        let expanded = ExpandedBinding {
            binding: Binding {
                values: binding.values,
                nodes,
                relationships: binding.relationships,
            },
            target_id: None,
            hop: 0,
        };
        admit_expanded_binding(&expanded, &mut output_bytes, memory_budget_bytes)?;
        output.push(expanded);
    }
    Ok(output)
}

fn admit_expanded_binding(
    expanded: &ExpandedBinding,
    used_bytes: &mut usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    let bytes = binding_memory_bytes(&expanded.binding);
    if bytes > memory_budget_bytes || used_bytes.saturating_add(bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "AdjacencyExpandExec seed state exceeds blocking_operator_bytes {memory_budget_bytes}"
        )));
    }
    *used_bytes = used_bytes.saturating_add(bytes);
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
        let mut batch = Vec::with_capacity(context.batch_rows);
        let mut emitted = 0usize;
        for node in scan.nodes {
            runtime_checkpoint(context.task_context)?;
            let binding = node_binding(spec.variable, node);
            if !predicate(&binding)? {
                continue;
            }
            batch.push(binding);
            emitted = emitted.saturating_add(1);
            if batch.len() == context.batch_rows
                && emit(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(context.batch_rows),
                ))? == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            if context.execution_limit.is_reached(emitted) {
                break;
            }
        }
        if !batch.is_empty() && emit(batch)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        return Ok(if context.execution_limit.is_reached(emitted) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        });
    }

    let label_ids = label_ids_for_pattern(context.catalog, spec.label);
    let mut batch = Vec::with_capacity(context.batch_rows);
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
        batch.push(binding);
        emitted = emitted.saturating_add(1);
        if batch.len() == context.batch_rows
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(context.batch_rows),
            ))? == BatchControl::Stop
        {
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
    let final_emit_control = if batch.is_empty() {
        BatchControl::Continue
    } else {
        emit(batch)?
    };
    if final_emit_control == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(if control == ScanControl::Stop {
        BatchControl::Stop
    } else {
        BatchControl::Continue
    })
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
        let mut tracker = OperatorMemoryTracker::new(context.memory_budget);
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
    let mut tracker = OperatorMemoryTracker::new(context.memory_budget);
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
    let mut batch = Vec::with_capacity(context.batch_rows);
    let mut emitted = 0usize;
    let mut matched = 0usize;
    let mut visit = |node| {
        matched = matched.saturating_add(1);
        batch.push(node_binding(variable, node));
        emitted = emitted.saturating_add(1);
        if batch.len() == context.batch_rows
            && emit(std::mem::replace(
                &mut batch,
                Vec::with_capacity(context.batch_rows),
            ))? == BatchControl::Stop
        {
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
    let final_emit_control = if batch.is_empty() {
        BatchControl::Continue
    } else {
        emit(batch)?
    };
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
    let mut tracker = OperatorMemoryTracker::new(context.memory_budget);
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
    let mut tracker = OperatorMemoryTracker::new(context.memory_budget);
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
}
