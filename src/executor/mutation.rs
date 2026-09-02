//! Mutation preflight, translation, and execution.

use super::*;
use skein_executor::store::{GraphExecutionRead, GraphExecutionWrite, ScanControl};
use skein_storage::RelRecord;

pub fn execute_mutation_with_limits(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    limits: MutationLimits,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    execute_mutation_with_store(plan, catalog, store, limits, task_context)
}

fn execute_mutation_with_store(
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
        Ok(None) => Err(SkeinError::Execution(
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
                        SkeinError::Execution(
                            "staged SET RETURN produced a negative node id".to_string(),
                        )
                    })?),
                    _ => {
                        return Err(SkeinError::Execution(
                            "staged SET RETURN did not produce a node id".to_string(),
                        ));
                    }
                };
                let node = store.node_owned(id)?.ok_or_else(|| {
                    SkeinError::Execution(format!(
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
                    return Err(SkeinError::Execution(format!(
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
                return Err(SkeinError::Execution(format!(
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
    let mut callback_error = None;
    store.visit_nodes_owned(None, &mut |node| {
        if callback_error.is_some() {
            return Ok(ScanControl::Stop);
        }
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS)
            && let Err(error) = runtime_checkpoint(task_context)
        {
            callback_error = Some(error);
            return Ok(ScanControl::Stop);
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        let binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node)]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate {
            match evaluate_predicate(predicate, catalog, store, &binding) {
                Ok(true) => {}
                Ok(false) => return Ok(ScanControl::Continue),
                Err(error) => {
                    callback_error = Some(error);
                    return Ok(ScanControl::Stop);
                }
            }
        }
        if ids.len() == limits.max_affected_rows.get() {
            callback_error = Some(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
            return Ok(ScanControl::Stop);
        }
        ids.push(binding.nodes[variable].id);
        Ok(ScanControl::Continue)
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }
    if ids.len() > limits.max_result_rows.get() {
        return Err(SkeinError::Execution(format!(
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
        return Err(SkeinError::Execution(format!(
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
    assignments: &[crate::planner::SetAssignment],
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
    let mut callback_error = None;
    store.visit_nodes_owned(None, &mut |node| {
        if callback_error.is_some() {
            return Ok(ScanControl::Stop);
        }
        visited = visited.saturating_add(1);
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS)
            && let Err(error) = runtime_checkpoint(task_context)
        {
            callback_error = Some(error);
            return Ok(ScanControl::Stop);
        }
        if !node_matches_label_pattern(&node, label_ids.as_deref()) {
            return Ok(ScanControl::Continue);
        }
        let original_binding = Binding {
            values: BTreeMap::new(),
            nodes: BTreeMap::from([(variable.to_string(), node.clone())]),
            relationships: BTreeMap::new(),
        };
        if let Some(predicate) = predicate {
            match evaluate_predicate(predicate, catalog, store, &original_binding) {
                Ok(true) => {}
                Ok(false) => return Ok(ScanControl::Continue),
                Err(error) => {
                    callback_error = Some(error);
                    return Ok(ScanControl::Stop);
                }
            }
        }
        if ids.len() == limits.max_affected_rows.get() {
            callback_error = Some(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            )));
            return Ok(ScanControl::Stop);
        }
        let id = node.id;
        ids.push(id);
        if let SetNodePropertiesReturnMode::Project(returns) = returns {
            if projected_rows.len() == limits.max_result_rows.get() {
                callback_error = Some(SkeinError::Execution(format!(
                    "mutation would exceed max_mutation_result_rows {}",
                    limits.max_result_rows
                )));
                return Ok(ScanControl::Stop);
            }
            let mut projected_node = node;
            for assignment in &assignments {
                let value = match crate::store::evaluate_node_set_value(
                    &projected_node.properties,
                    assignment,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        callback_error = Some(error);
                        return Ok(ScanControl::Stop);
                    }
                };
                projected_node
                    .properties
                    .insert(assignment.property.clone(), value);
            }
            let binding = Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::from([(variable.to_string(), projected_node)]),
                relationships: BTreeMap::new(),
            };
            let values = match returns
                .iter()
                .map(|item| {
                    project_value(item, catalog, &binding).map(|value| (item.name.clone(), value))
                })
                .collect::<Result<BTreeMap<_, _>>>()
            {
                Ok(values) => values,
                Err(error) => {
                    callback_error = Some(error);
                    return Ok(ScanControl::Stop);
                }
            };
            let next_payload = projected_payload_bytes.saturating_add(map_payload_bytes(&values));
            if next_payload > limits.max_result_payload_bytes.get() {
                callback_error = Some(SkeinError::Execution(format!(
                    "mutation result payload would exceed max_mutation_result_payload_bytes {}",
                    limits.max_result_payload_bytes
                )));
                return Ok(ScanControl::Stop);
            }
            projected_payload_bytes = next_payload;
            projected_rows.push(values);
        }
        Ok(ScanControl::Continue)
    })?;
    if let Some(error) = callback_error {
        return Err(error);
    }

    let output = match returns {
        SetNodePropertiesReturnMode::Project(_) => projected_rows,
        SetNodePropertiesReturnMode::Count { name } => {
            let row = BTreeMap::from([(name.clone(), Value::Int(ids.len() as i64))]);
            if map_payload_bytes(&row) > limits.max_result_payload_bytes.get() {
                return Err(SkeinError::Execution(format!(
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
        .ok_or_else(|| SkeinError::Execution("mutation operation count overflow".to_string()))?;
    if operation_count > limits.max_operations.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_operations {}",
            limits.max_operations
        )));
    }
    runtime_checkpoint(task_context)?;
    store.set_node_properties_by_ids_with_limits(catalog, &ids, &assignments, limits)?;
    Ok(output)
}

pub fn mutation_command(plan: &PhysicalPlan) -> Result<Option<GraphMutation>> {
    match plan {
        PhysicalPlan::CreateNodeLabel { label } => Ok(Some(GraphMutation::CreateNodeLabel {
            label: label.clone(),
        })),
        PhysicalPlan::CreateRelationshipType { rel_type } => {
            Ok(Some(GraphMutation::CreateRelationshipType {
                rel_type: rel_type.clone(),
            }))
        }
        PhysicalPlan::CreateNodeTable { name } => {
            Ok(Some(GraphMutation::CreateNodeTable { name: name.clone() }))
        }
        PhysicalPlan::CreateRelationshipTable { name } => {
            Ok(Some(GraphMutation::CreateRelationshipTable {
                name: name.clone(),
            }))
        }
        PhysicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => Ok(Some(GraphMutation::CreateProperty {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            property: property.clone(),
            value_type: property_type_to_core(*value_type),
            nullable: *nullable,
        })),
        PhysicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => Ok(Some(GraphMutation::AlterTableState {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            state: object_state_to_core(*state),
        })),
        PhysicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => Ok(Some(GraphMutation::AlterPropertyState {
            table_kind: table_kind_to_core(*table_kind),
            table: table.clone(),
            property: property.clone(),
            state: object_state_to_core(*state),
        })),
        PhysicalPlan::CreateIndex { label, property } => Ok(Some(GraphMutation::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        })),
        PhysicalPlan::CreateCompositeIndex { label, properties } => {
            Ok(Some(GraphMutation::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            }))
        }
        PhysicalPlan::CreateRangeIndex { label, property } => {
            Ok(Some(GraphMutation::CreateRangeIndex {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateFullTextIndex { label, property } => {
            Ok(Some(GraphMutation::CreateFullTextIndex {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateUniqueConstraint { label, property } => {
            Ok(Some(GraphMutation::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            Ok(Some(GraphMutation::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            Ok(Some(GraphMutation::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }))
        }
        PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => Ok(
            Some(GraphMutation::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }),
        ),
        PhysicalPlan::CreateNode { label, properties } => Ok(Some(GraphMutation::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        })),
        PhysicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => Ok(Some(GraphMutation::MergeNode {
            label: label.clone(),
            match_properties: match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
            on_match_assignments: on_match_assignments
                .iter()
                .map(node_set_assignment)
                .collect(),
            post_merge_assignments: post_merge_assignments
                .iter()
                .map(node_set_assignment)
                .collect(),
        })),
        PhysicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Ok(Some(GraphMutation::MergeConnectedNodes(
            ConnectedNodesCreate {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
        ))),
        PhysicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsBetweenMatches(
            MatchedRelationshipMerge {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                rel_type: rel_type.clone(),
                rel_match_properties: rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
            },
        ))),
        PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(
            GraphMutation::MergeRelationshipsFromMatchedRelationships(
                MatchedRelationshipCopyMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    old_rel_type: old_rel_type.clone(),
                    old_rel_filter: old_rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    new_rel_type: new_rel_type.clone(),
                    new_rel_match_properties: new_rel_match_properties.clone(),
                    on_create_properties: on_create_properties
                        .iter()
                        .map(|(property, value)| {
                            (
                                property.clone(),
                                relationship_on_create_property_value(value),
                            )
                        })
                        .collect(),
                },
            ),
        )),
        PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_target_label,
            new_target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsToMatchedTarget(
            MatchedRelationshipRetargetMerge {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                old_rel_type: old_rel_type.clone(),
                old_rel_filter: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_filter: property_filter_from_properties(old_target_properties),
                new_target_label: new_target_label.clone(),
                new_target_filter: property_filter_from_properties(new_target_properties),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
        ))),
        PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label,
            old_source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_source_label,
            new_source_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => Ok(Some(GraphMutation::MergeRelationshipsFromMatchedTarget(
            MatchedRelationshipSourceRetargetMerge {
                old_source_label: old_source_label.clone(),
                old_source_filter: property_filter_from_properties(old_source_properties),
                old_rel_type: old_rel_type.clone(),
                old_rel_filter: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_filter: property_filter_from_properties(old_target_properties),
                new_source_label: new_source_label.clone(),
                new_source_filter: property_filter_from_properties(new_source_properties),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
        ))),
        PhysicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => Ok(Some(GraphMutation::CreateRelationshipsBetweenMatches(
            MatchedRelationshipCreate {
                source_label: source_label.clone(),
                source_filter: property_filter_from_properties(source_properties),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
            },
        ))),
        PhysicalPlan::SetNodeProperty {
            label,
            predicate,
            property,
            value,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            match value {
                SetValue::Value(value) => Ok(Some(GraphMutation::SetNodeProperty {
                    label: label.clone(),
                    filter,
                    property: property.clone(),
                    value: value.clone(),
                })),
                SetValue::Coalesce { .. } => Err(SkeinError::Semantic(
                    "COALESCE node SET is not supported in transactional MATCH SET".to_string(),
                )),
                SetValue::AddInt { amount, .. } => Ok(Some(GraphMutation::SetNodePropertyAddInt {
                    label: label.clone(),
                    filter,
                    property: property.clone(),
                    amount: *amount,
                })),
                SetValue::DecrementFloorZero { .. } => Ok(Some(GraphMutation::SetNodeProperties {
                    label: label.clone(),
                    filter,
                    assignments: vec![NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::DecrementFloorZero,
                    }],
                })),
                SetValue::PreserveNewerExisting {
                    incoming, preserve, ..
                } => Ok(Some(GraphMutation::SetNodeProperties {
                    label: label.clone(),
                    filter,
                    assignments: vec![NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::PreserveNewerExisting {
                            incoming: incoming.clone(),
                            preserve: *preserve,
                        },
                    }],
                })),
            }
        }
        PhysicalPlan::SetNodeProperties {
            label,
            predicate,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            Ok(Some(GraphMutation::SetNodeProperties {
                label: label.clone(),
                filter,
                assignments: assignments.iter().map(node_set_assignment).collect(),
            }))
        }
        PhysicalPlan::SetNodePropertiesReturn {
            label,
            predicate,
            assignments,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            Ok(Some(GraphMutation::SetNodeProperties {
                label: label.clone(),
                filter,
                assignments: assignments.iter().map(node_set_assignment).collect(),
            }))
        }
        PhysicalPlan::SetRelationshipProperty {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            property,
            value,
            ..
        } => Ok(Some(GraphMutation::SetRelationshipProperty {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
            target_filter: property_filter_from_properties(target_properties),
            property: property.clone(),
            value: value.clone(),
        })),
        PhysicalPlan::SetRelationshipProperties {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            assignments,
            ..
        } => Ok(Some(GraphMutation::SetRelationshipProperties {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
            target_filter: property_filter_from_properties(target_properties),
            assignments: assignments
                .iter()
                .map(|assignment| RelationshipSetAssignment {
                    property: assignment.property.clone(),
                    value: assignment.value.clone(),
                })
                .collect(),
        })),
        PhysicalPlan::DeleteNode {
            label,
            predicate,
            detach,
            ..
        } => Ok(Some(GraphMutation::DeleteNode {
            label: label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            detach: *detach,
        })),
        PhysicalPlan::DeleteRelationship {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            ..
        } => Ok(Some(GraphMutation::DeleteRelationship {
            source_label: source_label.clone(),
            filter: predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?,
            rel_type: rel_type.clone(),
            target_label: target_label.clone(),
            target_filter: property_filter_from_properties(target_properties),
            rel_filter: relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?,
        })),
        PhysicalPlan::DeleteRelationshipTargetNodes {
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
            detach,
            ..
        } => Ok(Some(GraphMutation::DeleteRelationshipTargetNodes(
            RelationshipTargetNodeDelete {
                source_label: source_label.clone(),
                source_filter: source_predicate
                    .as_ref()
                    .map(property_filter_from_predicate)
                    .transpose()?,
                rel_type: rel_type.clone(),
                rel_filter: property_filter_from_properties(rel_properties),
                target_label: target_label.clone(),
                target_filter: property_filter_from_properties(target_properties),
                detach: *detach,
            },
        ))),
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => Ok(Some(GraphMutation::CreateConnectedNodes(
            ConnectedNodesCreate {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
        ))),
        PhysicalPlan::EmptyExec
        | PhysicalPlan::NodeCountExec { .. }
        | PhysicalPlan::RelationshipCountExec { .. }
        | PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::NodeProjectionScanExec { .. }
        | PhysicalPlan::SourceSegmentScan { .. }
        | PhysicalPlan::NodeCartesianProductExec { .. }
        | PhysicalPlan::NodeColumnLookupExec { .. }
        | PhysicalPlan::IndexNodeSeek { .. }
        | PhysicalPlan::IndexNodeMultiSeek { .. }
        | PhysicalPlan::IndexNodeUnionSeek { .. }
        | PhysicalPlan::IndexNodeCompositeSeek { .. }
        | PhysicalPlan::IndexNodeCompositeRangeSeek { .. }
        | PhysicalPlan::IndexNodeRangeSeek { .. }
        | PhysicalPlan::IndexNodeTextSeek { .. }
        | PhysicalPlan::AdjacencyExpandExec { .. }
        | PhysicalPlan::AdjacencyExistsExec { .. }
        | PhysicalPlan::OptionalDegreeExec { .. }
        | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. }
        | PhysicalPlan::ShortestPathExec { .. }
        | PhysicalPlan::FilterExec { .. }
        | PhysicalPlan::ProjectExec { .. }
        | PhysicalPlan::AggregateExec { .. }
        | PhysicalPlan::DistinctExec { .. }
        | PhysicalPlan::SortExec { .. }
        | PhysicalPlan::TopNExec { .. }
        | PhysicalPlan::LimitExec { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::VectorSeedScan { .. } => Ok(None),
    }
}

pub fn is_mutation_plan(plan: &PhysicalPlan) -> Result<bool> {
    Ok(matches!(
        plan.class(),
        skein_plan::PhysicalPlanClass::Schema | skein_plan::PhysicalPlanClass::Mutation
    ))
}

pub(super) fn node_set_assignment(assignment: &crate::planner::SetAssignment) -> NodeSetAssignment {
    NodeSetAssignment {
        property: assignment.property.clone(),
        value: node_set_value(&assignment.value),
    }
}

fn node_set_value(value: &SetValue) -> NodeSetValue {
    match value {
        SetValue::Value(value) => NodeSetValue::Value(value.clone()),
        SetValue::Coalesce { default, .. } => NodeSetValue::Coalesce {
            default: default.clone(),
        },
        SetValue::AddInt { amount, .. } => NodeSetValue::AddInt { amount: *amount },
        SetValue::DecrementFloorZero { .. } => NodeSetValue::DecrementFloorZero,
        SetValue::PreserveNewerExisting {
            incoming, preserve, ..
        } => NodeSetValue::PreserveNewerExisting {
            incoming: incoming.clone(),
            preserve: *preserve,
        },
    }
}

pub(super) fn relationship_on_create_property_value(
    value: &RelationshipOnCreateValue,
) -> RelationshipOnCreatePropertyValue {
    match value {
        RelationshipOnCreateValue::Value(value) => {
            RelationshipOnCreatePropertyValue::Value(value.clone())
        }
        RelationshipOnCreateValue::MatchedRelationshipProperty { property } => {
            RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                property: property.clone(),
            }
        }
    }
}

pub(super) fn try_projected_graph_with_node_filter(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    node_labels: &[String],
    rel_types: &[String],
    include_node: impl Fn(&NodeRecord) -> bool,
    layout: ProjectionLayout,
    budget: ProjectionMemoryBudget,
) -> Result<ProjectedGraph> {
    let source = GraphExecutionProjectionSource(store);
    if node_labels.is_empty() && rel_types.is_empty() {
        return ProjectedGraph::try_from_store_with_node_filter_and_layout(
            &source,
            None,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let label_ids = node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &[],
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    let rel_type_ids = rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::try_from_store_without_edges_with_node_filter_and_layout(
                &source,
                include_node,
                layout,
                budget,
            )
            .map_err(|error| SkeinError::Execution(error.to_string()));
        }
        return ProjectedGraph::try_from_store_labels_without_edges_with_node_filter_and_layout(
            &source,
            &label_ids,
            include_node,
            layout,
            budget,
        )
        .map_err(|error| SkeinError::Execution(error.to_string()));
    }
    ProjectedGraph::try_from_store_labels_and_rel_types_with_node_filter_and_layout(
        &source,
        &label_ids,
        &rel_type_ids,
        include_node,
        layout,
        budget,
    )
    .map_err(|error| SkeinError::Execution(error.to_string()))
}

struct GraphExecutionProjectionSource<'a>(&'a dyn GraphExecutionRead);

impl skein_analytics::ProjectionSource for GraphExecutionProjectionSource<'_> {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        self.0
            .visit_nodes_owned(None, &mut |node| {
                Ok(match visitor(node) {
                    skein_analytics::ProjectionScanControl::Continue => ScanControl::Continue,
                    skein_analytics::ProjectionScanControl::Stop => ScanControl::Stop,
                })
            })
            .map(|control| match control {
                ScanControl::Continue => skein_analytics::ProjectionScanControl::Continue,
                ScanControl::Stop => skein_analytics::ProjectionScanControl::Stop,
            })
            .map_err(|error| error.to_string())
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> skein_analytics::ProjectionScanControl,
    ) -> std::result::Result<skein_analytics::ProjectionScanControl, String> {
        let scan = self
            .0
            .scan_relationships_with_filter_pruning(None, None)
            .map_err(|error| error.to_string())?;
        for relationship in scan.relationships {
            if visitor(relationship) == skein_analytics::ProjectionScanControl::Stop {
                return Ok(skein_analytics::ProjectionScanControl::Stop);
            }
        }
        Ok(skein_analytics::ProjectionScanControl::Continue)
    }
}
