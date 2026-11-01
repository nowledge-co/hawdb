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

use super::staged::StagedMutations;
use super::{mutation_command, node_set_assignment, node_set_value};
use crate::binding::{map_payload_bytes, value_payload_bytes, Binding};
use crate::expression::{evaluate_predicate, project_value};
use crate::load_csv::{open_load_csv, CsvImportPolicy, CsvRows};
use crate::memory::DEFAULT_EXECUTION_BATCH_ROWS;
use crate::observer::NoopExecutionObserver;
use crate::pipeline::runtime_checkpoint;
use crate::predicate::property_filter_from_properties;
use crate::predicate::{label_ids_for_pattern, node_matches_label_pattern};
use crate::result_delivery::OutputLimits;
use crate::store::{GraphExecutionRead, GraphExecutionWrite, ScanControl};
use crate::Row;
use crate::{ExecutionMemoryConfig, QueryMemoryLedger};
use hawdb_core::{Catalog, HawDBError, Result, RuntimeTaskContext, Value};
use hawdb_plan_cypher::{
    BatchMutationOperation, BatchMutationValue, PhysicalPlan, Predicate,
    SetNodePropertiesReturnMode,
};
use hawdb_storage::mutation::evaluate::evaluate_node_set_value;
use hawdb_storage::{
    mutation::{GraphMutation, MatchedRelationshipCreate, MutationLimits, NodeSetAssignment},
    NodeId,
};
use std::collections::BTreeMap;

mod endpoints;
mod relationship;
pub use endpoints::resolve_relationship_merge_endpoints;

/// Executes a mutation plan with the default execution memory configuration.
pub fn execute_mutation_with_store(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store_output_limits(
        plan,
        catalog,
        store,
        limits,
        OutputLimits::default(),
        &ExecutionMemoryConfig::default(),
        task_context,
    )
}

/// Applies query output limits to explicit RETURN rows before committing.
/// Storage mutation summaries remain governed by `MutationLimits`. A staged
/// `UNWIND` batch charges its endpoint resolution to a statement ledger built
/// from `memory`, the host's configured execution memory.
pub fn execute_mutation_with_store_output_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    output_limits: OutputLimits,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    runtime_checkpoint(task_context)?;
    if let PhysicalPlan::UnwindMutation {
        rows,
        variable,
        operation,
    } = plan
    {
        // Stage the rows, then resolve both matched-endpoint forms against
        // the snapshot the commit uses: relationship CREATE and MERGE.
        let ledger = QueryMemoryLedger::for_query(memory, task_context)?;
        let mut staged =
            stage_unwind_mutations(rows, variable, operation, limits, &ledger, task_context)?;
        staged.resolve_endpoints(catalog, &*store, limits, memory, task_context)?;
        return commit_staged_mutations_with_output(
            plan,
            staged,
            catalog,
            store,
            limits,
            output_limits,
            task_context,
        );
    }
    if matches!(plan, PhysicalPlan::LoadCsvMutation { .. }) {
        return Err(HawDBError::Semantic(
            "LOAD CSV requires the database query runtime, which supplies the CSV import policy"
                .to_string(),
        ));
    }

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
            output_limits,
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

/// Stages a row-sourced mutation plan (`LOAD CSV` or `UNWIND`) before any
/// graph state is changed, charged to a new statement ledger built from
/// `memory`. Returns `None` for every other plan. Callers then resolve its
/// matched-relationship endpoints against the snapshot the commit will use.
pub fn stage_row_mutations(
    plan: &PhysicalPlan,
    parameters: &BTreeMap<String, Value>,
    csv_import: Option<&CsvImportPolicy>,
    limits: MutationLimits,
    memory: &ExecutionMemoryConfig,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<StagedMutations>> {
    let ledger = || QueryMemoryLedger::for_query(memory, task_context);
    match plan {
        PhysicalPlan::LoadCsvMutation { source, operation } => {
            let mut rows = open_load_csv(csv_import, source, parameters)?;
            materialize_load_csv_mutations(
                &mut rows,
                &source.variable,
                operation,
                limits,
                &ledger()?,
                task_context,
            )
            .map(Some)
        }
        PhysicalPlan::UnwindMutation {
            rows,
            variable,
            operation,
        } => stage_unwind_mutations(rows, variable, operation, limits, &ledger()?, task_context)
            .map(Some),
        _ => Ok(None),
    }
}

/// Commits a batch resolved by [`materialize_load_csv_mutations`] or
/// [`stage_unwind_mutations`] as one atomic storage batch. The batch stays
/// charged to its statement ledger until storage returns.
pub fn commit_staged_mutations(
    staged: StagedMutations,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
) -> Result<Vec<Row>> {
    if staged.is_empty() {
        return Ok(Vec::new());
    }
    let (mutations, _charge) = staged.into_parts();
    store
        .commit_mutations_with_limits(catalog, mutations, limits)
        .map(|summary| summary.rows)
}

/// [`commit_staged_mutations`] for a host that also applies query output
/// limits: an `UNWIND` relationship MERGE with RETURN projects its rows
/// against the resolved endpoints before the atomic commit, as before staging.
pub fn commit_staged_mutations_with_output(
    plan: &PhysicalPlan,
    staged: StagedMutations,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    output_limits: OutputLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let output = match plan {
        PhysicalPlan::UnwindMutation {
            operation: BatchMutationOperation::MergeRelationship(merge),
            ..
        } if merge.returns.is_some() => Some(relationship::project_before_commit(
            merge,
            staged.mutations(),
            catalog,
            &*store,
            limits,
            output_limits,
            task_context,
        )?),
        _ => None,
    };
    runtime_checkpoint(task_context)?;
    let rows = commit_staged_mutations(staged, catalog, store, limits)?;
    Ok(output.unwrap_or(rows))
}

/// Resolves every bounded `UNWIND` row before any graph state is changed.
///
/// Callers that own a graph transaction use this to retain the statement
/// savepoint contract while staging the entire batch as one storage mutation.
pub fn materialize_unwind_mutations(
    rows: &[Value],
    variable: &str,
    operation: &BatchMutationOperation,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<GraphMutation>> {
    let mut mutations = Vec::with_capacity(rows.len());
    resolve_unwind_rows(
        rows,
        variable,
        operation,
        limits,
        task_context,
        |mutation| {
            mutations.push(mutation);
            Ok(())
        },
    )?;
    Ok(mutations)
}

/// [`materialize_unwind_mutations`] as a staged batch, ready for
/// [`StagedMutations::resolve_matched_endpoints`]. The mutation list keeps the
/// `UNWIND` contract and is bounded by `limits` only; endpoint resolution
/// charges its scans, id maps and the id lists it copies into the endpoint
/// filters to `ledger`.
pub fn stage_unwind_mutations(
    rows: &[Value],
    variable: &str,
    operation: &BatchMutationOperation,
    limits: MutationLimits,
    ledger: &QueryMemoryLedger,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<StagedMutations> {
    let mut staged = StagedMutations::uncharged_list(ledger)?;
    resolve_unwind_rows(
        rows,
        variable,
        operation,
        limits,
        task_context,
        |mutation| staged.push(mutation),
    )?;
    Ok(staged)
}

fn resolve_unwind_rows(
    rows: &[Value],
    variable: &str,
    operation: &BatchMutationOperation,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
    mut push: impl FnMut(GraphMutation) -> Result<()>,
) -> Result<()> {
    if rows.len() > limits.max_operations.get() {
        return Err(HawDBError::Execution(format!(
            "UNWIND batch would exceed max_mutation_operations {}",
            limits.max_operations
        )));
    }
    let mut input_payload_bytes = 0usize;
    for (index, row) in rows.iter().enumerate() {
        if index.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
            runtime_checkpoint(task_context)?;
        }
        input_payload_bytes = input_payload_bytes.saturating_add(value_payload_bytes(row));
        if input_payload_bytes > limits.max_result_payload_bytes.get() {
            return Err(HawDBError::Execution(format!(
                "UNWIND batch input would exceed max_mutation_result_payload_bytes {}",
                limits.max_result_payload_bytes
            )));
        }
        if let Some(mutation) =
            resolve_row_mutation(operation, variable, row, NullProperties::Keep)?
        {
            push(mutation)?;
        }
    }
    Ok(())
}

/// Streams every `LOAD CSV` record and resolves it before any graph state is
/// changed, so the whole file commits or fails as one statement.
///
/// Records count against `max_mutation_operations` and their payload against
/// `max_mutation_result_payload_bytes`, and every staged mutation is charged
/// to `ledger` before it is retained; a file beyond any of these limits fails
/// before anything is written. Batched commits are not supported yet.
pub fn materialize_load_csv_mutations<R: std::io::Read>(
    rows: &mut CsvRows<R>,
    variable: &str,
    operation: &BatchMutationOperation,
    limits: MutationLimits,
    ledger: &QueryMemoryLedger,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<StagedMutations> {
    let mut input_payload_bytes = 0usize;
    let mut row_count = 0usize;
    let mut staged = StagedMutations::charged(ledger)?;
    while let Some(row) = rows.next_row()? {
        if row_count.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
            runtime_checkpoint(task_context)?;
        }
        row_count += 1;
        if row_count > limits.max_operations.get() {
            return Err(HawDBError::Execution(format!(
                "LOAD CSV would exceed max_mutation_operations {} in one transaction; \
                 split the file (batched commits are not supported yet)",
                limits.max_operations
            )));
        }
        input_payload_bytes = input_payload_bytes.saturating_add(value_payload_bytes(&row));
        if input_payload_bytes > limits.max_result_payload_bytes.get() {
            return Err(HawDBError::Execution(format!(
                "LOAD CSV input would exceed max_mutation_result_payload_bytes {} in one \
                 transaction; split the file (batched commits are not supported yet)",
                limits.max_result_payload_bytes
            )));
        }
        if let Some(mutation) =
            resolve_row_mutation(operation, variable, &row, NullProperties::Omit)?
        {
            staged.push(mutation)?;
        }
    }
    Ok(staged)
}

/// How a row-sourced mutation treats `NULL` property values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NullProperties {
    /// Store `NULL` values as given (historical `UNWIND` behaviour).
    Keep,
    /// openCypher semantics: CREATE omits `NULL` properties and MERGE refuses
    /// a `NULL` match property.
    Omit,
}

/// Resolves one row to its storage mutation. `None` means the row matched
/// nothing: a relationship endpoint filter contained `NULL`.
fn resolve_row_mutation(
    operation: &BatchMutationOperation,
    variable: &str,
    row: &Value,
    nulls: NullProperties,
) -> Result<Option<GraphMutation>> {
    let properties = |properties| resolve_row_properties(properties, variable, row);
    let created = |values| {
        let mut values: BTreeMap<String, Value> = properties(values)?;
        if nulls == NullProperties::Omit {
            values.retain(|_, value| *value != Value::Null);
        }
        Ok::<_, HawDBError>(values)
    };
    match operation {
        BatchMutationOperation::MergeRelationship(merge) => {
            let source = properties(&merge.source_properties)?;
            let target = properties(&merge.target_properties)?;
            let endpoint_filter = |properties: &BTreeMap<String, Value>| {
                if properties
                    .values()
                    .any(|value| matches!(value, Value::Null))
                {
                    // Cypher equality with NULL never binds an endpoint.
                    Some(hawdb_storage::mutation::PropertyFilter::Or(Vec::new()))
                } else {
                    property_filter_from_properties(properties)
                }
            };
            let rel_match_properties = properties(&merge.rel_match_properties)?;
            if let Some((property, _)) = rel_match_properties
                .iter()
                .find(|(_, value)| matches!(value, Value::Null))
            {
                return Err(HawDBError::Semantic(format!(
                    "relationship MERGE cannot match NULL property '{property}'"
                )));
            }
            Ok(Some(GraphMutation::MergeRelationshipsBetweenMatches(
                hawdb_storage::mutation::MatchedRelationshipMerge {
                    source_label: merge.source_label.clone(),
                    source_filter: endpoint_filter(&source),
                    target_label: merge.target_label.clone(),
                    target_filter: endpoint_filter(&target),
                    rel_type: merge.rel_type.clone(),
                    rel_match_properties,
                    on_create_properties: created(&merge.on_create_properties)?,
                },
            )))
        }
        BatchMutationOperation::CreateNode { label, properties } => {
            Ok(Some(GraphMutation::CreateNode {
                label: label.clone(),
                properties: created(properties)?,
            }))
        }
        BatchMutationOperation::MergeNode {
            label,
            match_properties,
            on_create_properties,
        } => {
            let match_properties = properties(match_properties)?;
            if nulls == NullProperties::Omit
                && let Some((property, _)) = match_properties
                    .iter()
                    .find(|(_, value)| **value == Value::Null)
            {
                return Err(HawDBError::Execution(format!(
                    "cannot MERGE node using NULL value for property '{property}'"
                )));
            }
            Ok(Some(GraphMutation::MergeNode {
                label: label.clone(),
                match_properties,
                on_create_properties: created(on_create_properties)?,
                on_match_assignments: Vec::new(),
                post_merge_assignments: Vec::new(),
            }))
        }
        BatchMutationOperation::CreateMatchedRelationship(relationship) => {
            let source = properties(&relationship.source_properties)?;
            let target = properties(&relationship.target_properties)?;
            // `n.p = NULL` is never true, so the MATCH yields no row.
            if source
                .values()
                .chain(target.values())
                .any(|value| *value == Value::Null)
            {
                return Ok(None);
            }
            let mut rel_properties = properties(&relationship.rel_properties)?;
            rel_properties.retain(|_, value| *value != Value::Null);
            Ok(Some(GraphMutation::CreateRelationshipsBetweenMatches(
                MatchedRelationshipCreate {
                    source_label: relationship.source_label.clone(),
                    source_filter: property_filter_from_properties(&source),
                    rel_type: relationship.rel_type.clone(),
                    rel_properties,
                    target_label: relationship.target_label.clone(),
                    target_filter: property_filter_from_properties(&target),
                },
            )))
        }
    }
}

fn resolve_row_properties(
    properties: &BTreeMap<String, BatchMutationValue>,
    variable: &str,
    row: &Value,
) -> Result<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|(property, value)| Ok((property.clone(), resolve_row_value(value, variable, row)?)))
        .collect()
}

fn resolve_row_value(value: &BatchMutationValue, variable: &str, row: &Value) -> Result<Value> {
    match value {
        BatchMutationValue::Static(value) => Ok(value.clone()),
        BatchMutationValue::Row => Ok(row.clone()),
        BatchMutationValue::RowProperty(row_property) => {
            let Value::Map(values) = row else {
                return Err(HawDBError::Semantic(format!(
                    "row for '{variable}.{row_property}' must be a map"
                )));
            };
            Ok(values.get(row_property).cloned().unwrap_or(Value::Null))
        }
        BatchMutationValue::RowIndex(index) => {
            let Value::List(values) = row else {
                return Err(HawDBError::Semantic(format!(
                    "row for '{variable}[{index}]' must be a list"
                )));
            };
            let position = if *index < 0 {
                usize::try_from(index.unsigned_abs())
                    .ok()
                    .and_then(|offset| values.len().checked_sub(offset))
            } else {
                usize::try_from(*index).ok()
            };
            Ok(position
                .and_then(|position| values.get(position))
                .cloned()
                .unwrap_or(Value::Null))
        }
        BatchMutationValue::Converted { function, input } => {
            function.apply(&resolve_row_value(input, variable, row)?)
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
    if let PhysicalPlan::UnwindMutation {
        operation: BatchMutationOperation::MergeRelationship(merge),
        ..
    } = plan
        && merge.returns.is_some()
    {
        return relationship::project_staged(merge, catalog, store, mutation_rows, limits)
            .map(Some);
    }
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
                    nodes: BTreeMap::from([(variable.clone(), node)]).into(),
                    relationships: BTreeMap::new().into(),
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
    let Some((ids, action)) = node_mutation_targets(plan, catalog, store, limits, task_context)?
    else {
        return Ok(None);
    };
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

/// Resolve node predicates before staging, preserving the ordinary executor's
/// NULL/expression/label semantics. No storage mutation occurs in this helper.
pub fn materialize_node_mutation(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<GraphMutation>> {
    let Some((ids, action)) = node_mutation_targets(plan, catalog, store, limits, task_context)?
    else {
        return Ok(None);
    };
    let filter = Some(hawdb_storage::mutation::PropertyFilter::IdIn {
        values: ids.into_iter().map(|id| Value::Int(id.0 as i64)).collect(),
    });
    Ok(Some(match action {
        NodeMutationPreflightAction::Set(assignments) => GraphMutation::SetNodeProperties {
            label: String::new(),
            filter,
            assignments,
        },
        NodeMutationPreflightAction::Delete { detach } => GraphMutation::DeleteNode {
            label: String::new(),
            filter,
            detach,
        },
    }))
}

fn node_mutation_targets(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Option<(Vec<NodeId>, NodeMutationPreflightAction)>> {
    runtime_checkpoint(task_context)?;
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
        }
        | PhysicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            ..
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
            nodes: BTreeMap::from([(variable.to_string(), node)]).into(),
            relationships: BTreeMap::new().into(),
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
    Ok(Some((ids, action)))
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
    assignments: &[hawdb_plan_cypher::SetAssignment],
    returns: &SetNodePropertiesReturnMode,
    catalog: &mut Catalog,
    store: &mut dyn GraphExecutionWrite,
    limits: MutationLimits,
    output_limits: OutputLimits,
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
            nodes: BTreeMap::from([(variable.to_string(), node.clone())]).into(),
            relationships: BTreeMap::new().into(),
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
                nodes: BTreeMap::from([(variable.to_string(), projected_node)]).into(),
                relationships: BTreeMap::new().into(),
            };
            let values = returns
                .iter()
                .map(|item| {
                    project_value(item, catalog, &binding).map(|value| (item.name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            let next_payload = projected_payload_bytes.saturating_add(map_payload_bytes(&values));
            output_limits.validate(projected_rows.len() + 1, next_payload)?;
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
            output_limits.validate(1, map_payload_bytes(&row))?;
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
