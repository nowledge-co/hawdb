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
use crate::binding::{map_payload_bytes, Binding};
use crate::expression::evaluate_projection_expression;
use crate::memory::DEFAULT_EXECUTION_BATCH_ROWS;
use crate::pipeline::runtime_checkpoint;
use crate::result_delivery::OutputLimits;
use crate::Row;
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan_cypher::{GraphEntityKind, MatchSet, MatchSetValue, PhysicalPlan};
use hawdb_storage::mutation::evaluate::evaluate_node_set_value;
use hawdb_storage::mutation::{
    EntityPropertyUpdate, GraphMutation, MutationEntity, MutationLimits, NodeSetAssignment,
};
use hawdb_storage::{NodeId, NodeRecord, RelId, RelRecord};
use std::collections::BTreeMap;
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

/// Evaluates the SET items of `plan` over `input` and projects its RETURN.
pub fn resolve_match_set(
    plan: &MatchSet<PhysicalPlan>,
    input: Vec<Row>,
    catalog: &Catalog,
    load: &mut dyn FnMut(MutationEntity) -> Result<Option<MatchSetRecord>>,
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
            values.push((entity, value));
        }
        for (entity, value) in values {
            records
                .get_mut(&entity)
                .expect("a target entity is loaded")
                .properties_mut()
                .insert(assignment.property.clone(), value);
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
    if let Some(items) = &plan.returns {
        let mut payload_bytes = 0usize;
        for (index, row) in rows.iter_mut().enumerate() {
            checkpoint(index, task_context)?;
            if output.len() == limits.max_result_rows.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation would exceed max_mutation_result_rows {}",
                    limits.max_result_rows
                )));
            }
            attach(&mut row.binding, &row.entities, plan, &records);
            let projected = items
                .iter()
                .map(|item| {
                    evaluate_projection_expression(&item.expression, catalog, &row.binding)
                        .map(|value| (item.name.clone(), value))
                })
                .collect::<Result<Row>>();
            row.binding.nodes.clear();
            row.binding.relationships.clear();
            let projected = projected?;
            payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&projected));
            output_limits.validate(output.len() + 1, payload_bytes)?;
            if payload_bytes > limits.max_result_payload_bytes.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
            }
            output.push(projected);
        }
    }

    let updates = written
        .into_iter()
        .map(|(entity, properties)| {
            let current = records[&entity].properties();
            EntityPropertyUpdate {
                entity,
                assignments: properties
                    .into_iter()
                    .map(|property| {
                        let value = current.get(&property).cloned().unwrap_or(Value::Null);
                        (property, value)
                    })
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    Ok(MatchSetOutcome {
        mutation: (!updates.is_empty()).then_some(GraphMutation::SetEntityProperties(updates)),
        rows: output,
    })
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
