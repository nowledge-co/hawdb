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

//! Bounded mutation preflight over the storage-neutral execution contracts.
//!
//! The store retains transaction admission, atomic commit, and WAL ownership.

use super::{mutation_command, node_set_assignment, node_set_value};
use crate::binding::{map_payload_bytes, Binding};
use crate::expression::{evaluate_predicate, project_value};
use crate::memory::DEFAULT_EXECUTION_BATCH_ROWS;
use crate::observer::NoopExecutionObserver;
use crate::pipeline::runtime_checkpoint;
use crate::predicate::{label_ids_for_pattern, node_matches_label_pattern};
use crate::store::{GraphExecutionRead, GraphExecutionWrite, ScanControl};
use crate::Row;
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan::{PhysicalPlan, Predicate, SetNodePropertiesReturnMode};
use hawdb_storage::mutation::evaluate::evaluate_node_set_value;
use hawdb_storage::{MutationLimits, NodeId, NodeSetAssignment};
use std::collections::BTreeMap;

pub fn execute_mutation_with_store(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    runtime_checkpoint(task_context)?;
    if let PhysicalPlan::SetNodePropertiesReturn {
        variable,
        label,
        predicate,
        assignments,
        returns,
    } = plan
    {
        return execute_set_node_properties_return_with_limits(
            variable,
            label,
            predicate.as_ref(),
            assignments,
            returns,
            catalog,
            store,
            limits,
            task_context,
        );
    }
    match mutation_command(plan) {
        Ok(Some(mutation)) => store
            .commit_mutation_with_limits(catalog, mutation, limits)
            .map(|summary| summary.rows),
        Ok(None) => Err(HawDBError::Execution(
            "physical plan is not an executable mutation".to_string(),
        )),
        Err(error) => {
            if let Some(rows) =
                execute_node_mutation_with_limits(plan, catalog, store, limits, task_context)?
            {
                Ok(rows)
            } else {
                Err(error)
            }
        }
    }
}

pub fn project_staged_mutation_return_rows(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    mutation_rows: &[Row],
    limits: MutationLimits,
) -> Result<Option<Vec<Row>>> {
    let PhysicalPlan::SetNodePropertiesReturn {
        variable, returns, ..
    } = plan
    else {
        return Ok(None);
    };

    let output = match returns {
        SetNodePropertiesReturnMode::Project(returns) => {
            let mut output = Vec::with_capacity(mutation_rows.len());
            let mut payload_bytes = 0usize;
            for row in mutation_rows {
                let id = match row.get("node_id") {
                    Some(Value::Int(id)) => NodeId(u64::try_from(*id).map_err(|_| {
                        HawDBError::Execution(
                            "staged SET RETURN produced a negative node id".to_string(),
                        )
                    })?),
                    _ => {
                        return Err(HawDBError::Execution(
                            "staged SET RETURN did not produce a node id".to_string(),
                        ));
                    }
                };
                let node = store.node_owned(id)?.ok_or_else(|| {
                    HawDBError::Execution(format!(
                        "updated node {} is missing during staged SET RETURN projection",
                        id.0
                    ))
                })?;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                let projected = returns
                    .iter()
                    .map(|item| {
                        project_value(item, catalog, &binding)
                            .map(|value| (item.name.clone(), value))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                payload_bytes = payload_bytes.saturating_add(map_payload_bytes(&projected));
                if payload_bytes > limits.max_result_payload_bytes.get() {
                    return Err(HawDBError::Execution(format!(
                        "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                        limits.max_result_payload_bytes
                    )));
                }
                output.push(projected);
            }
            output
        }
        SetNodePropertiesReturnMode::Count { name } => {
            let row = BTreeMap::from([(
                name.clone(),
                Value::Int(i64::try_from(mutation_rows.len()).unwrap_or(i64::MAX)),
            )]);
            if map_payload_bytes(&row) > limits.max_result_payload_bytes.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
            }
            vec![row]
        }
    };
    Ok(Some(output))
}

fn execute_node_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<Vec<Row>>> {
    let (variable, label, predicate, action) = match plan {
        PhysicalPlan::SetNodeProperty {
            variable,
            label,
            predicate,
            property,
            value,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Set(vec![NodeSetAssignment {
                property: property.clone(),
                value: node_set_value(value),
            }]),
        ),
        PhysicalPlan::SetNodeProperties {
            variable,
            label,
            predicate,
            assignments,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Set(assignments.iter().map(node_set_assignment).collect()),
        ),
        PhysicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => (
            variable,
            label,
            predicate.as_ref(),
            NodeMutationPreflightAction::Delete { detach: *detach },
        ),
        _ => return Ok(None),
    };
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut ids = Vec::with_capacity(limits.max_affected_rows.get().min(1024));
    let mut visited = 0usize;
    store.visit_nodes_owned(None, &mut |node| {
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
            runtime_checkpoint(task_context)?;
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate
            && !evaluate_predicate(predicate, catalog, store, &binding, &NoopExecutionObserver)?
        {
            return Ok(ScanControl::Continue);
        }
        if ids.len() == limits.max_affected_rows.get() {
            return Err(HawDBError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
        }
        ids.push(binding.nodes[variable].id);
        Ok(ScanControl::Continue)
    })?;
    if ids.len() > limits.max_result_rows.get() {
        return Err(HawDBError::Execution(format!(
            "mutation would exceed max_mutation_result_rows {}",
            limits.max_result_rows
        )));
    }
    let output = ids
        .iter()
        .map(|id| BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))]))
        .collect::<Vec<_>>();
    let payload_bytes = output.iter().fold(0usize, |total, row| {
        total.saturating_add(map_payload_bytes(row))
    });
    if payload_bytes > limits.max_result_payload_bytes.get() {
        return Err(HawDBError::Execution(format!(
            "mutation result payload would exceed max_mutation_result_payload_bytes {}",
            limits.max_result_payload_bytes
        )));
    }
    match action {
        NodeMutationPreflightAction::Set(assignments) => {
            store.set_node_properties_by_ids_with_limits(catalog, &ids, &assignments, limits)?;
        }
        NodeMutationPreflightAction::Delete { detach } => {
            store.delete_node_ids_with_limits(catalog, &ids, detach, limits)?;
        }
    }
    Ok(Some(output))
}

enum NodeMutationPreflightAction {
    Set(Vec<NodeSetAssignment>),
    Delete { detach: bool },
}

#[allow(clippy::too_many_arguments)]
fn execute_set_node_properties_return_with_limits(
    variable: &str,
    label: &str,
    predicate: Option<&Predicate>,
    assignments: &[hawdb_plan::SetAssignment],
    returns: &SetNodePropertiesReturnMode,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let assignments = assignments
        .iter()
        .map(node_set_assignment)
        .collect::<Vec<_>>();
    let label_ids = label_ids_for_pattern(catalog, label);
    let mut ids = Vec::with_capacity(limits.max_affected_rows.get().min(1024));
    let mut projected_rows = Vec::new();
    let mut projected_payload_bytes = 0usize;
    let mut visited = 0usize;
    store.visit_nodes_owned(None, &mut |node| {
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
            runtime_checkpoint(task_context)?;
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        let original_binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node.clone())]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate
            && !evaluate_predicate(
                predicate,
                catalog,
                store,
                &original_binding,
                &NoopExecutionObserver,
            )?
        {
            return Ok(ScanControl::Continue);
        }
        if ids.len() == limits.max_affected_rows.get() {
            return Err(HawDBError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
        }
        let id = node.id;
        ids.push(id);
        if let SetNodePropertiesReturnMode::Project(returns) = returns {
            if projected_rows.len() == limits.max_result_rows.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation would exceed max_mutation_result_rows {}",
                    limits.max_result_rows
                )));
            }
            let mut projected_node = node;
            for assignment in &assignments {
                let value = evaluate_node_set_value(&projected_node.properties, assignment)?;
                projected_node
                    .properties
                    .insert(assignment.property.clone(), value);
            }
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), projected_node)]),
                relationships: BTreeMap::new(),
            };
            let values = returns
                .iter()
                .map(|item| {
                    project_value(item, catalog, &binding).map(|value| (item.name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            let next_payload = projected_payload_bytes.saturating_add(map_payload_bytes(&values));
            if next_payload > limits.max_result_payload_bytes.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
            }
            projected_payload_bytes = next_payload;
            projected_rows.push(values);
        }
        Ok(ScanControl::Continue)
    })?;

    let output = match returns {
        SetNodePropertiesReturnMode::Project(_) => projected_rows,
        SetNodePropertiesReturnMode::Count { name } => {
            let row = BTreeMap::from([(name.clone(), Value::Int(ids.len() as i64))]);
            if map_payload_bytes(&row) > limits.max_result_payload_bytes.get() {
                return Err(HawDBError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
            }
            vec![row]
        }
    };
    let operation_count = ids
        .len()
        .checked_mul(assignments.len())
        .ok_or_else(|| HawDBError::Execution("mutation operation count overflow".to_string()))?;
    if operation_count > limits.max_operations.get() {
        return Err(HawDBError::Execution(format!(
            "mutation would exceed max_mutation_operations {}",
            limits.max_operations
        )));
    }
    runtime_checkpoint(task_context)?;
    store.set_node_properties_by_ids_with_limits(catalog, &ids, &assignments, limits)?;
    Ok(output)
}

#[cfg(test)]
mod tests;
