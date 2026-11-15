// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `RETURN` rows of a mutation statement, projected from the entities its
//! storage summary reports. Callers project against the private working state
//! before publishing, so the rows and the mutation commit or fail together.

use super::{materialize_node_mutation, mutation_command};
use crate::binding::{map_payload_bytes, Binding};
use crate::expression::project_value;
use crate::pipeline::runtime_checkpoint;
use crate::result_delivery::OutputLimits;
use crate::store::{GraphExecutionRead, MutationReturnStores};
use crate::{ExecutionMemoryConfig, Row};
use hawdb_core::{
    Catalog, HawDBError, ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, Result,
    RuntimeTaskContext, Value,
};
use hawdb_plan_cypher::{
    MutationReturn, MutationReturnEntity, MutationReturnState, PhysicalPlan,
    SetNodePropertiesReturnMode,
};
use hawdb_storage::mutation::{GraphMutation, MutationLimits};
use hawdb_storage::{NodeId, RelId};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

/// The storage command of the mutation under a `RETURN`. Node `SET`/`DELETE`
/// predicates that storage filters cannot express are resolved to node ids
/// first, with the ordinary executor semantics.
pub fn mutation_return_command(
    mutation: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<GraphMutation> {
    match mutation_command(mutation) {
        Ok(Some(command)) => Ok(command),
        Ok(None) => Err(HawDBError::Execution(
            "mutation RETURN requires an executable mutation".to_string(),
        )),
        Err(error) => {
            materialize_node_mutation(mutation, catalog, store, limits, memory, task_context)?
                .ok_or(error)
        }
    }
}

/// Limits for staging the mutation itself. Its storage summary rows are
/// internal ids, bounded by the affected-row limit; the result-row and payload
/// limits apply to the projected `RETURN` rows instead (see
/// [`project_mutation_return_rows`]), so a `count(...)` reports one row however
/// many entities change and every refusal is a typed budget error.
pub fn mutation_return_staging_limits(limits: MutationLimits) -> MutationLimits {
    MutationLimits {
        max_result_rows: limits.max_affected_rows,
        max_result_payload_bytes: NonZeroUsize::MAX,
        ..limits
    }
}

/// Projects `returns` over each mutation summary row. Written entities are
/// read from the state the mutation produced, deleted entities from the state
/// the statement started from. Returned rows obey the mutation result limits
/// and the query output limits.
pub fn project_mutation_return_rows(
    returns: &MutationReturn,
    catalog: &Catalog,
    stores: MutationReturnStores<'_>,
    mutation_rows: &[Row],
    limits: MutationLimits,
    output_limits: OutputLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let items = match &returns.mode {
        SetNodePropertiesReturnMode::Count { name } => {
            let row = BTreeMap::from([(
                name.clone(),
                Value::Int(i64::try_from(mutation_rows.len()).unwrap_or(i64::MAX)),
            )]);
            // SKIP/LIMIT apply to the single count row.
            if returns.offset > 0 || returns.limit == Some(0) {
                return Ok(Vec::new());
            }
            admit_row(0, 0, &row, limits, output_limits)?;
            return Ok(vec![row]);
        }
        SetNodePropertiesReturnMode::Project(items) => items,
    };
    let store = match returns.state {
        MutationReturnState::Written => stores.after,
        MutationReturnState::Deleted => stores.before,
    };
    // Only the rows SKIP/LIMIT select are projected and charged.
    let selected = mutation_rows
        .iter()
        .skip(returns.offset)
        .take(returns.limit.unwrap_or(usize::MAX));
    let mut output = Vec::with_capacity(
        mutation_rows
            .len()
            .saturating_sub(returns.offset)
            .min(returns.limit.unwrap_or(usize::MAX))
            .min(limits.max_result_rows.get()),
    );
    let mut payload_bytes = 0usize;
    for row in selected {
        runtime_checkpoint(task_context)?;
        let binding = bind_row(returns, store, row)?;
        let projected = items
            .iter()
            .map(|item| {
                project_value(item, catalog, &binding).map(|value| (item.name.clone(), value))
            })
            .collect::<Result<Row>>()?;
        payload_bytes = admit_row(
            output.len(),
            payload_bytes,
            &projected,
            limits,
            output_limits,
        )?;
        output.push(projected);
    }
    Ok(output)
}

fn bind_row(
    returns: &MutationReturn,
    store: &dyn GraphExecutionRead,
    row: &Row,
) -> Result<Binding> {
    let mut nodes = BTreeMap::new();
    let mut relationships = BTreeMap::new();
    for binding in &returns.bindings {
        let id = match row.get(&binding.column) {
            Some(Value::Int(id)) => u64::try_from(*id).map_err(|_| {
                HawDBError::Execution(format!(
                    "mutation RETURN received a negative id for '{}'",
                    binding.variable
                ))
            })?,
            _ => {
                return Err(HawDBError::Execution(format!(
                    "mutation RETURN has no '{}' id for '{}'",
                    binding.column, binding.variable
                )))
            }
        };
        let missing = || {
            HawDBError::Execution(format!(
                "mutation RETURN variable '{}' is missing from the statement state",
                binding.variable
            ))
        };
        match binding.entity {
            MutationReturnEntity::Node => {
                let node = store.node_owned(NodeId(id))?.ok_or_else(missing)?;
                nodes.insert(binding.variable.clone(), node);
            }
            MutationReturnEntity::Relationship => {
                let relationship = store.relationship_owned(RelId(id))?.ok_or_else(missing)?;
                relationships.insert(binding.variable.clone(), relationship);
            }
            MutationReturnEntity::RelationshipSource | MutationReturnEntity::RelationshipTarget => {
                let relationship = store.relationship_owned(RelId(id))?.ok_or_else(missing)?;
                let endpoint = if binding.entity == MutationReturnEntity::RelationshipSource {
                    relationship.source
                } else {
                    relationship.target
                };
                let node = store.node_owned(endpoint)?.ok_or_else(missing)?;
                nodes.insert(binding.variable.clone(), node);
            }
        }
    }
    Ok(Binding {
        values: BTreeMap::new(),
        nodes: nodes.into(),
        relationships: relationships.into(),
    })
}

/// Admits one more returned row after `admitted` rows of `payload_bytes`, and
/// returns the new payload total.
fn admit_row(
    admitted: usize,
    payload_bytes: usize,
    row: &Row,
    limits: MutationLimits,
    output_limits: OutputLimits,
) -> Result<usize> {
    if admitted >= limits.max_result_rows.get() {
        return Err(result_rows_exceeded(admitted, 1, limits));
    }
    let row_bytes = map_payload_bytes(row);
    let next = payload_bytes.saturating_add(row_bytes);
    if next > limits.max_result_payload_bytes.get() {
        return Err(result_payload_exceeded(payload_bytes, row_bytes, limits));
    }
    output_limits.validate(admitted + 1, next)?;
    Ok(next)
}

/// The typed refusal of mutation `RETURN` rows over
/// `MutationLimits.max_result_rows`: `requested` more rows on top of `used`.
/// Every mutation `RETURN` form (`CREATE`, `MERGE`, `SET`, `DELETE`, and
/// relationship `MERGE` after `UNWIND`) reports this error.
pub(crate) fn result_rows_exceeded(
    used: usize,
    requested: usize,
    limits: MutationLimits,
) -> HawDBError {
    ResourceBudgetError {
        resource: ResourceBudgetKind::MetadataEntries,
        scope: ResourceBudgetScope::Query,
        owner: "mutation RETURN rows".to_string(),
        memory_class: None,
        requested: u64::try_from(requested).unwrap_or(u64::MAX),
        used: u64::try_from(used).unwrap_or(u64::MAX),
        budget: u64::try_from(limits.max_result_rows.get()).unwrap_or(u64::MAX),
        budget_option: "MutationLimits.max_result_rows",
        message: format!(
            "mutation would exceed max_mutation_result_rows {}",
            limits.max_result_rows
        ),
    }
    .into()
}

/// The typed refusal of mutation `RETURN` rows over
/// `MutationLimits.max_result_payload_bytes`: `requested` more bytes on top of
/// `used`.
pub(crate) fn result_payload_exceeded(
    used: usize,
    requested: usize,
    limits: MutationLimits,
) -> HawDBError {
    ResourceBudgetError {
        resource: ResourceBudgetKind::MemoryBytes,
        scope: ResourceBudgetScope::Query,
        owner: "mutation RETURN rows".to_string(),
        memory_class: None,
        requested: u64::try_from(requested).unwrap_or(u64::MAX),
        used: u64::try_from(used).unwrap_or(u64::MAX),
        budget: u64::try_from(limits.max_result_payload_bytes.get()).unwrap_or(u64::MAX),
        budget_option: "MutationLimits.max_result_payload_bytes",
        message: format!(
            "mutation result payload would exceed max_mutation_result_payload_bytes {}",
            limits.max_result_payload_bytes
        ),
    }
    .into()
}
