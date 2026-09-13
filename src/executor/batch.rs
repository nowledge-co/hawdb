//! Root facade for the executor-owned read-only batch engine.

pub(super) use skein_executor::batch::{
    collect_batch_pipeline, execute_prepared_binding_batches, BatchReadContext, ExecutionContext,
    PreparedPhysicalPlan, PreparedStorageCapability,
};
#[cfg(test)]
pub(super) use skein_executor::batch::{
    execute_binding_batches, BatchPlanRef, PreparedExecutionMode,
};

#[cfg(test)]
use super::*;

#[cfg(test)]
mod dispatch_tests;
