//! Root facade wiring for storage-independent blocking operators.

use super::*;
use skein_executor::blocking::{self as executor_blocking, BindingBatchSource};

struct RootBindingBatchSource<'a> {
    context: BatchReadContext<'a>,
}

impl BindingBatchSource for RootBindingBatchSource<'_> {
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
    let mut source = RootBindingBatchSource { context };
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
    let mut source = RootBindingBatchSource { context };
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
    let mut source = RootBindingBatchSource { context };
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
    let mut source = RootBindingBatchSource { context };
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
    let mut source = RootBindingBatchSource { context };
    executor_blocking::stream_cartesian_product_batches(
        left,
        right,
        &mut source,
        context.kernel_context(),
        execution_limit,
        emit,
    )
}
