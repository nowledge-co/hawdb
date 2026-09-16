use super::{
    AdmittedRelationalExecution, QueryMemoryLedger, RelationalQueryReadModes,
    RelationalQueryResourceContext, RelationalQueryStoreReader, RelationalState, Result,
    SkeinError,
};

pub(super) use crate::physical_plan::*;

pub(super) trait RelationalExecutionAdmission {
    fn admit<'state, 'runtime, R: RelationalQueryStoreReader>(
        self,
        state: &'state RelationalState,
        read_modes: RelationalQueryReadModes<'state, R>,
        resources: RelationalQueryResourceContext<'runtime>,
    ) -> Result<AdmittedRelationalExecution<'state, 'runtime, R>>;
}

impl RelationalExecutionAdmission for PreparedRelationalExecutionDescriptor {
    fn admit<'state, 'runtime, R: RelationalQueryStoreReader>(
        self,
        state: &'state RelationalState,
        read_modes: RelationalQueryReadModes<'state, R>,
        resources: RelationalQueryResourceContext<'runtime>,
    ) -> Result<AdmittedRelationalExecution<'state, 'runtime, R>> {
        skein_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let query_memory_budget = skein_executor::memory::enforced_query_memory_budget(
            resources.execution_memory,
            resources.task_context,
        )?;
        let estimated_bytes = self
            .memory_shape
            .estimated_bytes(resources.execution_memory);
        if estimated_bytes > query_memory_budget.get() {
            return Err(SkeinError::Execution(format!(
                "prepared relational query requires {estimated_bytes} estimated bytes, exceeding query_memory_bytes {query_memory_budget}"
            )));
        }
        Ok(AdmittedRelationalExecution {
            state,
            index_read_mode: read_modes.index,
            row_read_mode: read_modes.row,
            limits: resources.limits,
            execution_memory: resources.execution_memory,
            memory_ledger: QueryMemoryLedger::new(query_memory_budget),
            task_context: resources.task_context,
        })
    }
}
