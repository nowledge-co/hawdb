//! Private batch scan and count handlers.

use super::*;

pub(super) fn stream_composite_node_seek_batches(
    variable: &str,
    label: &str,
    predicates: &[(String, Value)],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
        context
            .store
            .visit_nodes_by_composite_property_owned(label_id, predicates, consumer)
    })
}

pub(super) fn stream_composite_node_range_seek_batches(
    variable: &str,
    label: &str,
    seek: &crate::planner::CompositeRangeSeek,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
        context
            .store
            .visit_nodes_by_composite_range_owned(label_id, seek, consumer)
    })
}

pub(super) struct NodeRangeSeekSpec<'a> {
    pub(super) variable: &'a str,
    pub(super) label: &'a str,
    pub(super) property: &'a str,
    pub(super) lower: &'a Option<(Value, bool)>,
    pub(super) upper: &'a Option<(Value, bool)>,
}

impl NodeRangeSeekSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            variable,
            label,
            property,
            lower,
            upper,
        } = self;
        let Some(label_id) = context.catalog.label_id(label) else {
            return Ok(BatchControl::Continue);
        };
        stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
            context.store.visit_nodes_by_property_range_owned(
                label_id,
                property,
                lower.as_ref(),
                upper.as_ref(),
                consumer,
            )
        })
    }
}

pub(super) fn stream_node_text_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    query: &str,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let Some(label_id) = context.catalog.label_id(label) else {
        return Ok(BatchControl::Continue);
    };
    stream_visited_node_batches(variable, context, execution_limit, emit, |consumer| {
        context
            .store
            .visit_nodes_by_full_text_property_owned(label_id, property, query, consumer)
    })
}

pub(super) fn stream_optional_relationship_count_sum_batches(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let label_ids = label_ids_for_pattern(context.catalog, label);
    let count_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "OptionalRelationshipCountSumExec",
        context.memory.blocking_operator_bytes,
    );
    let mut total = 0usize;
    let mut nodes_since_checkpoint = 0usize;
    context.store.visit_nodes_owned(None, &mut |node| {
        nodes_since_checkpoint += 1;
        if nodes_since_checkpoint == context.memory.batch_rows.get() {
            nodes_since_checkpoint = 0;
            runtime_checkpoint(context.task_context)?;
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref())
            || !node_properties_match(&node, properties)
        {
            return Ok(ScanControl::Continue);
        }
        for leg in legs {
            let count = relationship_count_sum_leg(
                context.catalog,
                context.store,
                node.id,
                leg,
                skein_executor::store::AdjacencyReadMemory {
                    budget_bytes: context.memory.blocking_operator_bytes.get(),
                    account: Some(&count_account),
                },
                context.observer,
                context.task_context,
            )?;
            total = total.saturating_add(count);
        }
        Ok(ScanControl::Continue)
    })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(total as i64))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_node_count_batches(
    label: &str,
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let label_id = (!label.is_empty())
        .then(|| context.catalog.label_id(label))
        .flatten();
    let count = if label.is_empty() {
        context.store.node_count_for_label(None)
    } else if let Some(label_id) = label_id {
        context.store.node_count_for_label(Some(label_id))
    } else {
        0
    };
    context
        .observer
        .record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Node,
            label_id,
            rel_type_id: None,
            strategy: ScanPruningStrategy::ExactCount,
            pruned: true,
            exact_empty: count == 0,
            candidate_count_before_pruning: count,
            pruned_candidate_count: count,
            candidate_count_before_filter: 0,
            output_count: 1,
            filtered_out_count: 0,
        });
    let count = i64::try_from(count).map_err(|_| {
        SkeinError::Execution(format!(
            "node count for label '{label}' exceeds the supported i64 result range"
        ))
    })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(count))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_relationship_count_batches(
    rel_type: &str,
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let rel_type_id = (!rel_type.is_empty())
        .then(|| context.catalog.rel_type_id(rel_type))
        .flatten();
    let count = if rel_type.is_empty() {
        context.store.relationship_count_for_type(None)
    } else if let Some(rel_type_id) = rel_type_id {
        context.store.relationship_count_for_type(Some(rel_type_id))
    } else {
        0
    };
    context
        .observer
        .record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Relationship,
            label_id: None,
            rel_type_id,
            strategy: ScanPruningStrategy::ExactCount,
            pruned: true,
            exact_empty: count == 0,
            candidate_count_before_pruning: count,
            pruned_candidate_count: count,
            candidate_count_before_filter: 0,
            output_count: 1,
            filtered_out_count: 0,
        });
    let count = i64::try_from(count).map_err(|_| {
        SkeinError::Execution(format!(
            "relationship count for type '{rel_type}' exceeds the supported i64 result range"
        ))
    })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(count))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_node_projection_batches(
    spec: NodeProjectionScanSpec<'_>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if !spec.items.is_empty()
        && spec.access.is_label_scan()
        && let Some(result) = try_stream_columnar_node_projection_batches(
            spec.variable,
            spec.label,
            spec.predicate,
            spec.items,
            context,
            execution_limit,
            emit,
        )
    {
        return result;
    }
    stream_node_projection_scan_batches(spec, context, execution_limit, emit)
}

pub(super) fn stream_empty_batches(
    _context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    _emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    Ok(BatchControl::Continue)
}
