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
pub use hawdb_executor::mutation::{
    is_mutation_plan, materialize_unwind_mutations, mutation_command,
};

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn hawdb_executor::store::GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store(plan, catalog, store, limits, task_context)
}

/// Runs the read input of a row-evaluated SET against `store` and evaluates
/// its SET items and RETURN. The caller commits or stages the mutation, so
/// the input, the evaluation and the write observe one snapshot.
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_match_set<S: ExecutionStore>(
    set: &hawdb_plan_cypher::MatchSet<PhysicalPlan>,
    parameters: &BTreeMap<String, Value>,
    catalog: &Catalog,
    store: &S,
    limits: MutationLimits,
    output_limits: hawdb_executor::result_delivery::OutputLimits,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<hawdb_executor::mutation::MatchSetOutcome> {
    use hawdb_executor::mutation::{
        affected_rows_exceeded, match_set_account, match_set_row_bytes, MatchSetRecord,
    };
    use hawdb_storage::mutation::MutationEntity;

    // Every input row may write, so the row budget is the affected-row limit.
    // The rows, entity copies and computed values are charged to one account
    // of the statement's query memory.
    let max_rows = limits.max_affected_rows.get();
    let ledger = QueryMemoryLedger::for_query(memory, task_context)?;
    let account = match_set_account(&ledger);
    let mut charge = account.reserve(0)?;
    let mut rows = Vec::new();
    let mut external = NoExternalReadOperator;
    execute_with_request_consumer(
        ExecutionRequest::new(&set.input, parameters, memory)
            .with_optional_task_context(task_context),
        ExecutionResources::read_only(catalog, store, &mut external),
        &mut |row| {
            if rows.len() == max_rows {
                return Err(affected_rows_exceeded(limits));
            }
            charge.grow(match_set_row_bytes(&row))?;
            rows.push(row);
            Ok(())
        },
    )?;
    hawdb_executor::mutation::resolve_match_set(
        set,
        rows,
        catalog,
        &mut |entity| match entity {
            MutationEntity::Node(id) => {
                hawdb_executor::store::GraphExecutionRead::node_owned(store, id)
                    .map(|node| node.map(MatchSetRecord::Node))
            }
            MutationEntity::Relationship(id) => {
                crate::store::InternalGraphEngine::relationship_owned(store, id)
                    .map(|relationship| relationship.map(MatchSetRecord::Relationship))
            }
        },
        &mut charge,
        limits,
        output_limits,
        task_context,
    )
}

/// Executes a row-evaluated SET and commits it atomically. Returns its
/// RETURN rows, or the committed entity ids without RETURN.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_match_set<S: ExecutionStore>(
    set: &hawdb_plan_cypher::MatchSet<PhysicalPlan>,
    parameters: &BTreeMap<String, Value>,
    catalog: &mut Catalog,
    store: &mut S,
    limits: MutationLimits,
    output_limits: hawdb_executor::result_delivery::OutputLimits,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let outcome = resolve_match_set(
        set,
        parameters,
        catalog,
        store,
        limits,
        output_limits,
        memory,
        task_context,
    )?;
    runtime_checkpoint(task_context)?;
    let committed = if outcome.mutations.is_empty() {
        Vec::new()
    } else {
        hawdb_executor::store::GraphExecutionWrite::commit_mutations_with_limits(
            store,
            catalog,
            outcome.mutations,
            limits,
        )?
        .rows
    };
    Ok(if set.returns.is_some() {
        outcome.rows
    } else {
        committed
    })
}
