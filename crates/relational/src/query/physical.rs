// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{
    AdmittedRelationalExecution, HawDBError, QueryMemoryLedger, RelationalQueryReadModes,
    RelationalQueryResourceContext, RelationalQueryStoreReader, RelationalState, Result,
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
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let query_memory_budget = hawdb_executor::memory::enforced_query_memory_budget(
            resources.execution_memory,
            resources.task_context,
        )?;
        let estimated_bytes = self
            .memory_shape
            .estimated_bytes(resources.execution_memory);
        if estimated_bytes > query_memory_budget.get() {
            return Err(HawDBError::Execution(format!(
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
