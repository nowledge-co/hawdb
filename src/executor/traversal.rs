//! Root facade wiring for storage-independent traversal operators.

use super::*;
use skein_executor::traversal as executor_traversal;

#[cfg(test)]
pub(super) use executor_traversal::ShortestPathSearch;
pub(super) use executor_traversal::{ShortestPathExecInput, TraversalExecutionContext};

pub(super) fn execute_shortest_path(
    catalog: &Catalog,
    store: &GraphStore,
    input: ShortestPathExecInput<'_>,
    execution_limit: ExecutionLimit,
    context: TraversalExecutionContext<'_>,
) -> Result<skein_executor::pipeline::AccountedBindingSet> {
    executor_traversal::execute_shortest_path(catalog, store, input, execution_limit, context)
}

#[cfg(test)]
pub(super) fn all_shortest_paths(
    store: &GraphStore,
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

pub(super) fn relationship_count_sum_leg(
    catalog: &Catalog,
    store: &GraphStore,
    source: NodeId,
    leg: &RelationshipCountLeg,
    memory: skein_executor::store::AdjacencyReadMemory<'_>,
    observer: &dyn skein_executor::observer::ExecutionObserver,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<usize> {
    executor_traversal::relationship_count_sum_leg(
        catalog,
        store,
        source,
        leg,
        memory,
        observer,
        task_context,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn thread_repair_stats_rows(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    identity_label: &str,
    identity_ref_property: &str,
    thread_id_property: &str,
    message_rel_type: &str,
    message_label: &str,
    memory_rel_type: &str,
    memory_label: &str,
    memory_budget: NonZeroUsize,
    memory_ledger: &QueryMemoryLedger,
    observer: &dyn skein_executor::observer::ExecutionObserver,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<skein_executor::pipeline::AccountedBindingSet> {
    executor_traversal::thread_repair_stats_rows(
        catalog,
        store,
        label,
        identity_label,
        identity_ref_property,
        thread_id_property,
        message_rel_type,
        message_label,
        memory_rel_type,
        memory_label,
        memory_budget,
        memory_ledger,
        observer,
        task_context,
    )
}
