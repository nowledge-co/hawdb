//! Root facade wiring for storage-independent expression evaluation.

use super::*;
use skein_executor::store::GraphExecutionRead;

pub(super) use skein_executor::expression::{
    node_scan_filter_from_predicate, project_value, property_filter_from_predicate,
    relationship_filter_from_properties_and_predicate,
};

pub(super) fn evaluate_predicate(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    binding: &Binding,
) -> Result<bool> {
    evaluate_predicate_observed(
        predicate,
        catalog,
        store,
        binding,
        &skein_executor::observer::NoopExecutionObserver,
        skein_executor::store::AdjacencyReadMemory {
            budget_bytes: skein_executor::memory::DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES,
            account: None,
        },
    )
}

pub(super) fn evaluate_predicate_observed(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    binding: &Binding,
    observer: &dyn skein_executor::observer::ExecutionObserver,
    adjacency_memory: skein_executor::store::AdjacencyReadMemory<'_>,
) -> Result<bool> {
    skein_executor::expression::evaluate_predicate_with_memory(
        predicate,
        catalog,
        store,
        binding,
        observer,
        adjacency_memory,
    )
}
