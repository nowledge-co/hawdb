//! Recursive batch wiring for storage-independent blocking operators.

use super::*;
use crate::blocking::{self as executor_blocking, BindingBatchSource};

struct RecursiveBindingBatchSource<'a> {
    context: BatchReadContext<'a>,
}

pub(super) fn stream_hash_join_batches(
    plan: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_hash_join_batches(
        plan,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}

impl BindingBatchSource for RecursiveBindingBatchSource<'_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        execute_binding_batches(input, self.context, execution_limit, emit)
    }
}

pub(super) fn stream_distinct_batches(
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_distinct_batches(
        input,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn stream_sort_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_sort_batches(
        input,
        items,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stream_top_n_batches(
    input: &PhysicalPlan,
    items: &[SortItem],
    offset: usize,
    limit: usize,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_top_n_batches(
        input,
        items,
        offset,
        limit,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn stream_aggregate_batches(
    input: &PhysicalPlan,
    group_keys: &[Projection],
    items: &[Aggregation],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_aggregate_batches(
        input,
        group_keys,
        items,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn stream_cartesian_product_batches(
    left: &PhysicalPlan,
    right: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let mut source = RecursiveBindingBatchSource { context };
    executor_blocking::stream_cartesian_product_batches(
        left,
        right,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}
