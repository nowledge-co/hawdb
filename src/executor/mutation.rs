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

//! Concrete-store entrypoint for storage-neutral mutation execution.

use super::*;

use hawdb_executor::mutation::execute_mutation_with_store;
pub use hawdb_executor::mutation::project_staged_mutation_return_rows;
pub use hawdb_executor::mutation::{is_mutation_plan, mutation_command};

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn hawdb_executor::store::GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store(plan, catalog, store, limits, task_context)
}
