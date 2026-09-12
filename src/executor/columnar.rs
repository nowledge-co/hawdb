//! Root context adapter for executor-owned numeric read fragments.

use super::*;
use skein_executor::numeric::{self, NumericExecutionContext};

pub(super) use numeric::{default_morsel_parallelism, supports_parallel_morsel_execution};

#[cfg(test)]
mod scan_error_tests;

impl<'a> BatchReadContext<'a> {
    fn numeric_context(self) -> NumericExecutionContext<'a> {
        NumericExecutionContext {
            catalog: self.catalog,
            store: self.store,
            memory: self.memory,
            memory_ledger: self.memory_ledger,
            task_context: self.task_context,
            observer: self.observer,
        }
    }
}

pub(super) fn try_stream_columnar_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    numeric::try_stream_columnar_projection_batches(
        items,
        input,
        context.numeric_context(),
        execution_limit,
        emit,
    )
}

pub(super) fn try_stream_columnar_node_projection_batches(
    variable: &str,
    label: &str,
    predicate: Option<&Predicate>,
    items: &[Projection],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Option<Result<BatchControl>> {
    numeric::try_stream_columnar_node_projection_batches(
        variable,
        label,
        predicate,
        items,
        context.numeric_context(),
        execution_limit,
        emit,
    )
}
