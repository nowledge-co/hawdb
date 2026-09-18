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

//! Facade integration helper for executor-owned shortest paths.

use super::*;
pub(super) use executor_traversal::ShortestPathSearch;
use hawdb_executor::store::GraphExecutionRead;
use hawdb_executor::traversal as executor_traversal;

#[cfg(test)]
pub(super) fn all_shortest_paths(
    store: &dyn GraphExecutionRead,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<(Vec<Vec<NodeId>>, usize, usize)> {
    let memory_ledger = QueryMemoryLedger::new(memory_budget);
    let memory_account = memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "ShortestPathExec test",
        memory_budget,
    );
    executor_traversal::all_shortest_paths(
        store,
        search,
        memory_budget,
        result_limit,
        memory_account,
        task_context,
        &hawdb_executor::observer::NoopExecutionObserver,
    )
}
