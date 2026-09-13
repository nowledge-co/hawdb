//! Fast paths and recursive-source wiring for streaming transforms.

use super::*;
use crate::pipeline::BindingBatchSource;
use crate::transform as executor_transform;

struct PreparedTransformSource<'a> {
    context: BatchReadContext<'a>,
}

impl BindingBatchSource for PreparedTransformSource<'_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        execute_prepared_binding_batches(
            BatchPlanRef::descendant(input),
            self.context,
            execution_limit,
            emit,
        )
    }
}

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
    let mut source = PreparedTransformSource { context };
    executor_transform::stream_filter_batches(
        input,
        &mut source,
        context.kernel_context(),
        execution_limit,
        &mut |binding| {
            evaluate_predicate_observed(
                predicate,
                context.catalog,
                context.store,
                binding,
                context.observer,
                crate::store::AdjacencyReadMemory {
                    budget_bytes: context.memory.blocking_operator_bytes.get(),
                    account: Some(&predicate_account),
                },
            )
        },
        emit,
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
    let mut source = PreparedTransformSource { context };
    executor_transform::stream_projection_batches(
        items,
        input,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
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
    let mut source = PreparedTransformSource { context };
    executor_transform::stream_limit_batches(
        offset,
        limit,
        input,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}
