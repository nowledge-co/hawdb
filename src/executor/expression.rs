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

//! Root facade wiring for storage-independent expression evaluation.

#[cfg(test)]
use super::*;
#[cfg(test)]
use hawdb_executor::store::GraphExecutionRead;

#[cfg(test)]
pub(super) use hawdb_executor::expression::property_filter_from_predicate;

#[cfg(test)]
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
        &hawdb_executor::observer::NoopExecutionObserver,
        hawdb_executor::store::AdjacencyReadMemory {
            budget_bytes: hawdb_executor::memory::DEFAULT_BLOCKING_OPERATOR_MEMORY_BYTES,
            account: None,
        },
    )
}

#[cfg(test)]
pub(super) fn evaluate_predicate_observed(
    predicate: &Predicate,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    binding: &Binding,
    observer: &dyn hawdb_executor::observer::ExecutionObserver,
    adjacency_memory: hawdb_executor::store::AdjacencyReadMemory<'_>,
) -> Result<bool> {
    hawdb_executor::expression::evaluate_predicate_with_memory(
        predicate,
        catalog,
        store,
        binding,
        observer,
        adjacency_memory,
    )
}
