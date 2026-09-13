//! Facade integration helper for executor-owned shortest paths.

use super::*;
pub(super) use executor_traversal::ShortestPathSearch;
use skein_executor::store::GraphExecutionRead;
use skein_executor::traversal as executor_traversal;

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
        &skein_executor::observer::NoopExecutionObserver,
    )
}
