use super::*;

/// Capability probing and execution consume the same exhaustive handler match.
/// The probe drops borrowed closures without invoking them; neither path boxes
/// a handler or builds another copy of the physical plan.
pub(super) trait BatchDispatch {
    type Output;

    fn supported(
        self,
        execute: impl FnOnce(
            BatchReadContext<'_>,
            ExecutionLimit,
            &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl>,
    ) -> Self::Output;

    fn unsupported(self, plan: &PhysicalPlan) -> Self::Output;
}

pub(super) struct BatchSupport;

impl BatchDispatch for BatchSupport {
    type Output = bool;

    fn supported(
        self,
        _execute: impl FnOnce(
            BatchReadContext<'_>,
            ExecutionLimit,
            &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl>,
    ) -> bool {
        true
    }

    fn unsupported(self, _plan: &PhysicalPlan) -> bool {
        false
    }
}

pub(super) struct BatchExecution<'context, 'emit> {
    pub(super) context: BatchReadContext<'context>,
    pub(super) execution_limit: ExecutionLimit,
    pub(super) emit: &'emit mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
}

impl BatchDispatch for BatchExecution<'_, '_> {
    type Output = Result<BatchControl>;

    fn supported(
        self,
        execute: impl FnOnce(
            BatchReadContext<'_>,
            ExecutionLimit,
            &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
        ) -> Result<BatchControl>,
    ) -> Self::Output {
        execute(self.context, self.execution_limit, self.emit)
    }

    fn unsupported(self, plan: &PhysicalPlan) -> Self::Output {
        Err(unsupported_batch_operator(plan))
    }
}

pub(super) fn unsupported_batch_operator(plan: &PhysicalPlan) -> SkeinError {
    SkeinError::Execution(format!(
        "physical operator '{}' does not support batch execution",
        plan.kind().as_str(),
    ))
}
