//! Private streaming filter, projection, and limit handlers.

use super::*;

pub(super) fn stream_filter_batches(
    predicate: &Predicate,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if let PhysicalPlan::SeqNodeScan { variable, label } = input
        && let Ok(filter) = property_filter_from_predicate(predicate)
    {
        return stream_node_scan_batches(
            variable,
            label,
            Some((predicate, &filter)),
            context,
            execution_limit,
            emit,
        );
    }
    if let PhysicalPlan::AdjacencyExpandExec {
        rel_variable: Some(rel_variable),
        input: expand_input,
        ..
    } = input
        && let Some(filter) = exact_relationship_scan_filter_from_predicate(predicate, rel_variable)
    {
        return stream_filtered_adjacency_expand_batches(
            input,
            expand_input,
            predicate,
            context,
            execution_limit,
            AdjacencyExpandFilters {
                relationship_scan_filter: Some(&filter),
                target_scan_filter: None,
            },
            emit,
        );
    }
    if let PhysicalPlan::AdjacencyExpandExec {
        target_variable,
        input: expand_input,
        ..
    } = input
        && predicate_references_only_variable(predicate, target_variable)
        && let Ok(filter) = property_filter_from_predicate(predicate)
    {
        return stream_filtered_adjacency_expand_batches(
            input,
            expand_input,
            predicate,
            context,
            execution_limit,
            AdjacencyExpandFilters {
                relationship_scan_filter: None,
                target_scan_filter: Some(&filter),
            },
            emit,
        );
    }
    let predicate_account = context.memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "FilterExec relationship predicate",
        context.memory.blocking_operator_bytes,
    );
    let emitted = Cell::new(0usize);
    execute_prepared_binding_batches(
        BatchPlanRef::descendant(input),
        context,
        ExecutionLimit::unlimited(),
        &mut |batch| {
            let remaining = execution_limit
                .output_rows
                .unwrap_or(usize::MAX)
                .saturating_sub(emitted.get());
            if remaining == 0 {
                return Ok(BatchControl::Stop);
            }
            let mut filtered = TransformBatchBuilder::new(
                "FilterExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?;
            let mut emit_filtered = |output: BindingBatch| {
                emitted.set(emitted.get().saturating_add(output.len()));
                emit(output)
            };
            for binding in batch {
                if evaluate_predicate_observed(
                    predicate,
                    context.catalog,
                    context.store,
                    &binding,
                    context.observer,
                    skein_executor::store::AdjacencyReadMemory {
                        budget_bytes: context.memory.blocking_operator_bytes.get(),
                        account: Some(&predicate_account),
                    },
                )? {
                    filtered.reserve_before_allocation()?;
                    filtered.push(binding);
                    if filtered.is_full()
                        && filtered.emit(&mut emit_filtered)? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
                    if execution_limit.is_reached(emitted.get().saturating_add(filtered.len())) {
                        break;
                    }
                }
            }
            if !filtered.is_empty() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if execution_limit.is_reached(emitted.get()) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}

pub(super) fn stream_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if let Some(result) =
        try_stream_columnar_projection_batches(items, input, context, execution_limit, emit)
    {
        return result;
    }
    let emitted = Cell::new(0usize);
    execute_prepared_binding_batches(
        BatchPlanRef::descendant(input),
        context,
        execution_limit,
        &mut |batch| {
            let mut projected = TransformBatchBuilder::new(
                "ProjectExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?;
            let mut emit_projected = |output: BindingBatch| {
                emitted.set(emitted.get().saturating_add(output.len()));
                emit(output)
            };
            for binding in batch {
                projected.reserve_before_allocation()?;
                let mut values = BTreeMap::new();
                for item in items {
                    let value = project_value(item, context.catalog, &binding)?;
                    insert_projected_value(&mut values, &item.name, value);
                }
                projected.push(Binding {
                    values,
                    nodes: binding.nodes,
                    relationships: binding.relationships,
                });
                if projected.is_full() && projected.emit(&mut emit_projected)? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
            }
            if !projected.is_empty() && projected.emit(&mut emit_projected)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if execution_limit.is_reached(emitted.get()) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}

pub(super) fn stream_limit_batches(
    offset: usize,
    limit: Option<usize>,
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let skipped = Cell::new(0usize);
    let emitted = Cell::new(0usize);
    let output_cap = match (limit, execution_limit.output_rows) {
        (Some(limit), Some(parent)) => (limit).min(parent),
        (Some(limit), None) => limit,
        (None, Some(parent)) => parent,
        (None, None) => usize::MAX,
    };
    execute_prepared_binding_batches(
        BatchPlanRef::descendant(input),
        context,
        ExecutionLimit {
            output_rows: Some(offset.saturating_add(output_cap)),
        },
        &mut |batch| {
            let mut output = TransformBatchBuilder::new(
                "LimitExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?;
            let mut emit_output = |batch: BindingBatch| {
                emitted.set(emitted.get().saturating_add(batch.len()));
                emit(batch)
            };
            for binding in batch {
                if skipped.get() < offset {
                    skipped.set(skipped.get().saturating_add(1));
                    continue;
                }
                if emitted.get() == output_cap {
                    break;
                }
                output.reserve_before_allocation()?;
                output.push(binding);
                if output.is_full() && output.emit(&mut emit_output)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            if !output.is_empty() && output.emit(&mut emit_output)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if emitted.get() == output_cap {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}
