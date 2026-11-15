// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Row evaluation of [`MatchSet`] SET items (#1791).
//!
//! The caller executes the read input and passes its rows here. Each SET item
//! is evaluated over every row against the values the earlier items left,
//! then applied to an in-memory copy of the entities it writes; a later item
//! and the RETURN projection read the result. Nothing reaches the store until
//! the caller commits the returned mutation, so a failing row leaves no
//! partial effect.

use super::node_set_value;
use crate::binding::{map_memory_bytes, map_payload_bytes, value_memory_bytes, Binding};
use crate::expression::evaluate_projection_expression;
use crate::memory::DEFAULT_EXECUTION_BATCH_ROWS;
use crate::pipeline::runtime_checkpoint;
use crate::result_delivery::OutputLimits;
use crate::Row;
use crate::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan_cypher::{
    GraphEntityKind, MatchSet, MatchSetReturn, MatchSetValue, PhysicalPlan, ProjectionExpression,
    SortDirection, SortKey,
};
use hawdb_storage::mutation::evaluate::evaluate_node_set_value;
use hawdb_storage::mutation::{
    EntityPropertyUpdate, GraphMutation, MutationEntity, MutationLimits, NodeSetAssignment,
};
use hawdb_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// A record a SET input row reads, loaded by the caller from its store.
#[derive(Debug, Clone)]
pub enum MatchSetRecord {
    Node(NodeRecord),
    Relationship(RelRecord),
}

/// The mutation to commit and the RETURN rows of a [`MatchSet`].
#[derive(Debug)]
pub struct MatchSetOutcome {
    /// `None` when no row targets an entity.
    pub mutation: Option<GraphMutation>,
    /// The projected rows; empty without RETURN.
    pub rows: Vec<Row>,
}

#[derive(Clone)]
enum Loaded {
    Node(Arc<NodeRecord>),
    Relationship(Arc<RelRecord>),
}

impl Loaded {
    fn properties(&self) -> &BTreeMap<String, Value> {
        match self {
            Self::Node(node) => &node.properties,
            Self::Relationship(relationship) => &relationship.properties,
        }
    }

    fn properties_mut(&mut self) -> &mut BTreeMap<String, Value> {
        match self {
            Self::Node(node) => &mut Arc::make_mut(node).properties,
            Self::Relationship(relationship) => &mut Arc::make_mut(relationship).properties,
        }
    }
}

/// One input row: its scalar values and, per graph binding, the entity it
/// holds (`None` for an unmatched OPTIONAL variable).
struct InputRow {
    binding: Binding,
    entities: Vec<(usize, Option<MutationEntity>)>,
}

/// The account charged for a row-evaluated SET: its input rows, the copies of
/// the entities it reads, the values it computes and its RETURN rows.
pub fn match_set_account(ledger: &QueryMemoryLedger) -> QueryMemoryAccount {
    ledger.account(
        QueryMemoryClass::BlockingState,
        "row-evaluated SET",
        ledger.budget_bytes(),
    )
}

/// The bytes a SET input row holds, charged while it is collected.
pub fn match_set_row_bytes(row: &Row) -> usize {
    map_memory_bytes(row)
}

/// Evaluates the SET items of `plan` over `input` and projects its RETURN.
///
/// `charge` already holds the input rows; the entity copies, computed values,
/// RETURN rows and the returned mutation's values grow it. Exceeding its
/// account refuses the statement with `ResourceBudgetExceeded` before any
/// write.
#[allow(clippy::too_many_arguments)]
pub fn resolve_match_set(
    plan: &MatchSet<PhysicalPlan>,
    input: Vec<Row>,
    catalog: &Catalog,
    load: &mut dyn FnMut(MutationEntity) -> Result<Option<MatchSetRecord>>,
    charge: &mut QueryMemoryLease,
    limits: MutationLimits,
    output_limits: OutputLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<MatchSetOutcome> {
    if input.len() > limits.max_affected_rows.get() {
        return Err(affected_rows_exceeded(limits));
    }
    let mut records = BTreeMap::<MutationEntity, Loaded>::new();
    let mut rows = Vec::with_capacity(input.len());
    for (index, mut row) in input.into_iter().enumerate() {
        checkpoint(index, task_context)?;
        let mut binding = Binding::values(BTreeMap::new());
        let mut entities = Vec::new();
        for (position, variable) in plan.bindings.iter().enumerate() {
            let value = row.remove(&variable.column).unwrap_or(Value::Null);
            let Some(kind) = variable.entity else {
                binding.values.insert(variable.variable.clone(), value);
                continue;
            };
            let entity = match value {
                Value::Null => None,
                Value::Int(id) => {
                    let id = u64::try_from(id).map_err(|_| {
                        HawDBError::Execution(format!(
                            "SET input produced a negative id for '{}'",
                            variable.variable
                        ))
                    })?;
                    Some(match kind {
                        GraphEntityKind::Node => MutationEntity::Node(NodeId(id)),
                        GraphEntityKind::Relationship => MutationEntity::Relationship(RelId(id)),
                    })
                }
                value => {
                    return Err(HawDBError::Execution(format!(
                        "SET input produced {value:?} for graph variable '{}'",
                        variable.variable
                    )))
                }
            };
            if let Some(entity) = entity
                && !records.contains_key(&entity)
            {
                let record = load(entity)?.ok_or_else(|| missing(entity))?;
                charge.grow(match &record {
                    MatchSetRecord::Node(node) => std::mem::size_of::<NodeRecord>()
                        .saturating_add(map_memory_bytes(&node.properties)),
                    MatchSetRecord::Relationship(relationship) => std::mem::size_of::<RelRecord>()
                        .saturating_add(map_memory_bytes(&relationship.properties)),
                })?;
                records.insert(
                    entity,
                    match (entity, record) {
                        (MutationEntity::Node(_), MatchSetRecord::Node(node)) => {
                            Loaded::Node(Arc::new(node))
                        }
                        (MutationEntity::Relationship(_), MatchSetRecord::Relationship(rel)) => {
                            Loaded::Relationship(Arc::new(rel))
                        }
                        _ => return Err(missing(entity)),
                    },
                );
            }
            entities.push((position, entity));
        }
        rows.push(InputRow { binding, entities });
    }

    let target = |row: &InputRow, variable: &str| {
        row.entities
            .iter()
            .find(|(position, _)| plan.bindings[*position].variable == variable)
            .and_then(|(_, entity)| *entity)
    };
    // Entities in first-write order, with their written properties.
    let mut written = Vec::<(MutationEntity, Vec<String>)>::new();
    let mut positions = BTreeMap::<MutationEntity, usize>::new();
    for assignment in &plan.assignments {
        // Every row reads the state the earlier items left, so the values of
        // one item are computed before any of them is applied.
        let mut values = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter_mut().enumerate() {
            checkpoint(index, task_context)?;
            let Some(entity) = target(row, &assignment.variable) else {
                // SET on an unmatched OPTIONAL variable is a no-op.
                continue;
            };
            let value = match &assignment.value {
                MatchSetValue::Expression(expression) => {
                    attach(&mut row.binding, &row.entities, plan, &records);
                    let value = evaluate_projection_expression(expression, catalog, &row.binding);
                    row.binding.nodes.clear();
                    row.binding.relationships.clear();
                    value?
                }
                MatchSetValue::Local(value) => evaluate_node_set_value(
                    records[&entity].properties(),
                    &NodeSetAssignment {
                        property: assignment.property.clone(),
                        value: node_set_value(value),
                    },
                )?,
            };
            charge.grow(value_memory_bytes(&value))?;
            values.push((entity, value));
        }
        for (entity, value) in values {
            // The value's charge moves into the entity copy, which releases
            // the value it replaces.
            if let Some(replaced) = records
                .get_mut(&entity)
                .expect("a target entity is loaded")
                .properties_mut()
                .insert(assignment.property.clone(), value)
            {
                charge.shrink(value_memory_bytes(&replaced));
            }
            let position = *positions.entry(entity).or_insert_with(|| {
                written.push((entity, Vec::new()));
                written.len() - 1
            });
            let properties = &mut written[position].1;
            if !properties.contains(&assignment.property) {
                properties.push(assignment.property.clone());
            }
        }
    }

    let mut output = Vec::new();
    if let Some(returns) = &plan.returns {
        output = project_returns(
            returns,
            &mut rows,
            plan,
            &records,
            catalog,
            charge,
            task_context,
        )?;
        if output.len() > limits.max_result_rows.get() {
            return Err(HawDBError::Execution(format!(
                "mutation would exceed max_mutation_result_rows {}",
                limits.max_result_rows
            )));
        }
        let payload_bytes = output.iter().fold(0usize, |total, row| {
            total.saturating_add(map_payload_bytes(row))
        });
        output_limits.validate(output.len(), payload_bytes)?;
        if payload_bytes > limits.max_result_payload_bytes.get() {
            return Err(HawDBError::Execution(format!(
                "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                limits.max_result_payload_bytes
            )));
        }
    }

    let mut updates = Vec::with_capacity(written.len());
    for (entity, properties) in written {
        let current = records[&entity].properties();
        let mut assignments = Vec::with_capacity(properties.len());
        for property in properties {
            let value = current.get(&property).cloned().unwrap_or(Value::Null);
            charge.grow(value_memory_bytes(&value))?;
            assignments.push((property, value));
        }
        updates.push(EntityPropertyUpdate {
            entity,
            assignments,
        });
    }
    Ok(MatchSetOutcome {
        mutation: (!updates.is_empty()).then_some(GraphMutation::SetEntityProperties(updates)),
        rows: output,
    })
}

/// Projects every updated row, then applies DISTINCT, ORDER BY, SKIP and
/// LIMIT to the returned rows. The SET already applied to every row.
fn project_returns(
    returns: &MatchSetReturn,
    rows: &mut [InputRow],
    plan: &MatchSet<PhysicalPlan>,
    records: &BTreeMap<MutationEntity, Loaded>,
    catalog: &Catalog,
    charge: &mut QueryMemoryLease,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let keys = returns
        .order_by
        .iter()
        .map(|item| {
            let expression = match &item.key {
                SortKey::Column(name) => ProjectionExpression::Column(name.clone()),
                SortKey::Property { variable, property } => ProjectionExpression::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                SortKey::Id { variable } => ProjectionExpression::Id {
                    variable: variable.clone(),
                },
                SortKey::Expression(expression) => expression.clone(),
            };
            (expression, item.direction)
        })
        .collect::<Vec<_>>();
    // Without ordering or DISTINCT, rows past OFFSET + LIMIT are never returned.
    let needed = (keys.is_empty() && !returns.distinct)
        .then(|| {
            returns
                .limit
                .map(|limit| returns.offset.saturating_add(limit))
        })
        .flatten();
    let mut projected = Vec::<(Row, Vec<Value>)>::new();
    let mut distinct = BTreeSet::<Vec<Value>>::new();
    for (index, row) in rows.iter_mut().enumerate() {
        if needed.is_some_and(|needed| projected.len() >= needed) {
            break;
        }
        checkpoint(index, task_context)?;
        attach(&mut row.binding, &row.entities, plan, records);
        let result = project_row(returns, &keys, &row.binding, catalog);
        row.binding.nodes.clear();
        row.binding.relationships.clear();
        let (values, sort_values) = result?;
        charge.grow(map_memory_bytes(&values))?;
        charge.grow(sort_values.iter().map(value_memory_bytes).sum())?;
        if returns.distinct {
            let key = values.values().cloned().collect::<Vec<_>>();
            charge.grow(key.iter().map(value_memory_bytes).sum())?;
            if !distinct.insert(key) {
                continue;
            }
        }
        projected.push((values, sort_values));
    }
    if !keys.is_empty() {
        // Stable: equal keys keep the input row order.
        projected.sort_by(|(_, left), (_, right)| {
            left.iter()
                .zip(right)
                .zip(&keys)
                .map(|((left, right), (_, direction))| direction.compare(left, right))
                .find(|ordering| ordering.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }
    Ok(projected
        .into_iter()
        .skip(returns.offset)
        .take(returns.limit.unwrap_or(usize::MAX))
        .map(|(values, _)| values)
        .collect())
}

/// One returned row and its sort values. Sort keys read the row's bindings
/// with every returned column also bound as a value under its name.
fn project_row(
    returns: &MatchSetReturn,
    keys: &[(ProjectionExpression, SortDirection)],
    binding: &Binding,
    catalog: &Catalog,
) -> Result<(Row, Vec<Value>)> {
    let values = returns
        .items
        .iter()
        .map(|item| {
            evaluate_projection_expression(&item.expression, catalog, binding)
                .map(|value| (item.name.clone(), value))
        })
        .collect::<Result<Row>>()?;
    if keys.is_empty() {
        return Ok((values, Vec::new()));
    }
    let mut keyed = binding.clone();
    keyed.values.extend(
        values
            .iter()
            .map(|(name, value)| (name.clone(), value.clone())),
    );
    let sort_values = keys
        .iter()
        .map(|(expression, _)| evaluate_projection_expression(expression, catalog, &keyed))
        .collect::<Result<Vec<_>>>()?;
    Ok((values, sort_values))
}

/// Binds the row's entities in their current state. A NULL entity declares
/// its variable as a NULL value, as an unmatched OPTIONAL MATCH does.
fn attach(
    binding: &mut Binding,
    entities: &[(usize, Option<MutationEntity>)],
    plan: &MatchSet<PhysicalPlan>,
    records: &BTreeMap<MutationEntity, Loaded>,
) {
    for (position, entity) in entities {
        let variable = plan.bindings[*position].variable.clone();
        match entity.map(|entity| &records[&entity]) {
            None => {
                binding.values.insert(variable, Value::Null);
            }
            Some(Loaded::Node(node)) => binding.nodes.insert_arc(variable, Arc::clone(node)),
            Some(Loaded::Relationship(relationship)) => binding
                .relationships
                .insert_arc(variable, Arc::clone(relationship)),
        }
    }
}

fn checkpoint(index: usize, task_context: Option<&RuntimeTaskContext>) -> Result<()> {
    if index.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
        runtime_checkpoint(task_context)?;
    }
    Ok(())
}

fn missing(entity: MutationEntity) -> HawDBError {
    HawDBError::Execution(match entity {
        MutationEntity::Node(id) => format!("matched node {} is missing during SET", id.0),
        MutationEntity::Relationship(id) => {
            format!("matched relationship {} is missing during SET", id.0)
        }
    })
}

/// The refusal of a SET input with more rows than the mutation may affect.
pub fn affected_rows_exceeded(limits: MutationLimits) -> HawDBError {
    HawDBError::Execution(format!(
        "mutation would exceed max_mutation_affected_rows {}",
        limits.max_affected_rows
    ))
}

#[cfg(test)]
mod tests;
