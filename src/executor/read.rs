//! Physical read-plan dispatch and materialized fallback execution.

use super::*;

pub(super) fn execute_bindings_with_limit(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    context: &mut ExecutionContext<'_>,
    execution_limit: ExecutionLimit,
) -> Result<Vec<Binding>> {
    runtime_checkpoint(context.task_context)?;
    if let Some(plan) = PreparedPhysicalPlan::prepare(plan, store, context.memory).batch() {
        return collect_batch_pipeline(plan, catalog, store, context, execution_limit);
    }
    match plan {
        PhysicalPlan::CreateNodeLabel { label } => {
            let existed = catalog.label_id(label);
            let id = store.create_node_label(catalog, label)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("label_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(existed.is_none())),
            ]))])
        }
        PhysicalPlan::CreateRelationshipType { rel_type } => {
            let existed = catalog.rel_type_id(rel_type);
            let id = store.create_relationship_type(catalog, rel_type)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(existed.is_none())),
            ]))])
        }
        PhysicalPlan::CreateNodeTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Node, name);
            let id = store.create_node_table(catalog, name)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("table_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(existed.is_none())),
            ]))])
        }
        PhysicalPlan::CreateRelationshipTable { name } => {
            let existed = catalog.table_id(crate::schema::TableKind::Relationship, name);
            let id = store.create_relationship_table(catalog, name)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("table_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(existed.is_none())),
            ]))])
        }
        PhysicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => {
            let table_kind = table_kind_to_core(*table_kind);
            let existed = catalog
                .table_id(table_kind, table)
                .and_then(|table_id| catalog.property_descriptor_id(table_id, property))
                .is_some();
            let id = store.create_property_descriptor(
                catalog,
                table_kind,
                table,
                property,
                property_type_to_core(*value_type),
                *nullable,
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("property_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) =
                store.alter_table_state(catalog, table_kind_to_core(*table_kind), table, state)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("table_id".to_string(), Value::Int(id.0 as i64)),
                ("changed".to_string(), Value::Bool(changed)),
            ]))])
        }
        PhysicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => {
            let state = object_state_to_core(*state);
            let (id, changed) = store.alter_property_state(
                catalog,
                table_kind_to_core(*table_kind),
                table,
                property,
                state,
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("property_id".to_string(), Value::Int(id.0 as i64)),
                ("changed".to_string(), Value::Bool(changed)),
            ]))])
        }
        PhysicalPlan::CreateIndex { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.property_index_id(label_id, property).is_some());
            let id = store.create_property_index(catalog, label, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("index_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateCompositeIndex { label, properties } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .composite_property_index_id(label_id, properties)
                    .is_some()
            });
            let id = store.create_composite_property_index(catalog, label, properties)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("index_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateRangeIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::Range,
                    )
                    .is_some()
            });
            let id = store.create_range_property_index(catalog, label, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("index_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateFullTextIndex { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .property_index_id_with_kind(
                        label_id,
                        property,
                        crate::schema::IndexKind::FullText,
                    )
                    .is_some()
            });
            let id = store.create_full_text_property_index(catalog, label, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("index_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateUniqueConstraint { label, property } => {
            let existed = catalog
                .label_id(label)
                .is_some_and(|label_id| catalog.unique_constraint_id(label_id, property).is_some());
            let id = store.create_unique_constraint(catalog, label, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            let existed = catalog.label_id(label).is_some_and(|label_id| {
                catalog
                    .node_property_exists_constraint_id(label_id, property)
                    .is_some()
            });
            let id = store.create_node_property_exists_constraint(catalog, label, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_unique_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store.create_relationship_unique_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            let existed = catalog.rel_type_id(rel_type).is_some_and(|rel_type_id| {
                catalog
                    .relationship_property_exists_constraint_id(rel_type_id, property)
                    .is_some()
            });
            let id = store
                .create_relationship_property_exists_constraint(catalog, rel_type, property)?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(!existed)),
            ]))])
        }
        PhysicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => {
            let graph = try_projected_graph_with_node_filter(
                catalog,
                store,
                node_labels,
                rel_types,
                |_| true,
                ProjectionLayout::Outgoing,
                ProjectionMemoryBudget::new(context.memory.blocking_operator_bytes),
            )?;
            store.register_projected_graph(
                name,
                ProjectedGraphDefinition {
                    node_labels: node_labels.clone(),
                    rel_types: rel_types.clone(),
                },
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("graph_name".to_string(), Value::String(name.clone())),
                (
                    "node_count".to_string(),
                    Value::Int(graph.node_count() as i64),
                ),
                (
                    "edge_count".to_string(),
                    Value::Int(graph.edge_count() as i64),
                ),
            ]))])
        }
        PhysicalPlan::CreateNode { label, properties } => {
            let id = store.create_node(catalog, label, properties.clone())?;
            Ok(vec![Binding::scalar("node_id", Value::Int(id.0 as i64))])
        }
        PhysicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => {
            let on_match_assignments = on_match_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let post_merge_assignments = post_merge_assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let (id, created) = store.merge_node(
                catalog,
                label,
                match_properties.clone(),
                on_create_properties.clone(),
                &on_match_assignments,
                &post_merge_assignments,
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("node_id".to_string(), Value::Int(id.0 as i64)),
                ("created".to_string(), Value::Bool(created)),
            ]))])
        }
        PhysicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target, created) = store.merge_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                ("created".to_string(), Value::Bool(created)),
            ]))])
        }
        PhysicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => {
            let rows = store.merge_relationships_between_matches(
                catalog,
                MatchedRelationshipMerge {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_match_properties: rel_match_properties.clone(),
                    on_create_properties: on_create_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| {
                    Binding::values(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]))
                })
                .collect())
        }
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
        } => {
            let rows = store.merge_relationships_from_matched_relationships(
                catalog,
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
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| {
                    Binding::values(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]))
                })
                .collect())
        }
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
        } => {
            let rows = store.merge_relationships_to_matched_target(
                catalog,
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
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| {
                    Binding::values(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]))
                })
                .collect())
        }
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
        } => {
            let rows = store.merge_relationships_from_matched_target(
                catalog,
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
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target, created)| {
                    Binding::values(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                        ("created".to_string(), Value::Bool(created)),
                    ]))
                })
                .collect())
        }
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
            let ids = match value {
                SetValue::Value(value) => store.set_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    value.clone(),
                )?,
                SetValue::Coalesce { default, .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::Coalesce {
                            default: default.clone(),
                        },
                    }],
                )?,
                SetValue::AddInt { amount, .. } => store.add_int_node_property(
                    catalog,
                    label,
                    filter.as_ref(),
                    property,
                    *amount,
                )?,
                SetValue::DecrementFloorZero { .. } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::DecrementFloorZero,
                    }],
                )?,
                SetValue::PreserveNewerExisting {
                    incoming, preserve, ..
                } => store.set_node_properties(
                    catalog,
                    label,
                    filter.as_ref(),
                    &[NodeSetAssignment {
                        property: property.clone(),
                        value: NodeSetValue::PreserveNewerExisting {
                            incoming: incoming.clone(),
                            preserve: *preserve,
                        },
                    }],
                )?,
            };
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("node_id", Value::Int(id.0 as i64)))
                .collect())
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
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let ids = store.set_node_properties(catalog, label, filter.as_ref(), &assignments)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("node_id", Value::Int(id.0 as i64)))
                .collect())
        }
        PhysicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => {
            let assignments = assignments
                .iter()
                .map(node_set_assignment)
                .collect::<Vec<_>>();
            let label_ids = label_ids_for_pattern(catalog, label);
            let mut ids = Vec::new();
            store.try_visit_nodes_owned(None, |node| {
                if !node_matches_label_pattern(&node, label_ids.as_deref()) {
                    return Ok(GraphScanControl::Continue);
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate
                    && !evaluate_predicate(predicate, catalog, store, &binding)?
                {
                    return Ok(GraphScanControl::Continue);
                }
                ids.push(id);
                Ok(GraphScanControl::Continue)
            })?;
            let ids = store.set_node_properties_by_ids(catalog, &ids, &assignments)?;
            match returns {
                SetNodePropertiesReturnMode::Project(returns) => ids
                    .into_iter()
                    .map(|id| {
                        let node = store.node_owned(id)?.ok_or_else(|| {
                            SkeinError::Execution(format!(
                                "updated node {} is missing during SET RETURN projection",
                                id.0
                            ))
                        })?;
                        let binding = Binding {
                            values: BTreeMap::new(),
                            nodes: BTreeMap::from([(variable.clone(), node)]),
                            relationships: BTreeMap::new(),
                        };
                        let values = returns
                            .iter()
                            .map(|item| {
                                project_value(item, catalog, &binding)
                                    .map(|value| (item.name.clone(), value))
                            })
                            .collect::<Result<BTreeMap<_, _>>>()?;
                        Ok(Binding::values(values))
                    })
                    .collect(),
                SetNodePropertiesReturnMode::Count { name } => Ok(vec![Binding::scalar(
                    name.clone(),
                    Value::Int(ids.len() as i64),
                )]),
            }
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
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.set_relationship_property(
                catalog,
                RelationshipPropertyUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    property: property.clone(),
                    value: value.clone(),
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("rel_id", Value::Int(id.0 as i64)))
                .collect())
        }
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
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let assignments = assignments
                .iter()
                .map(|assignment| RelationshipSetAssignment {
                    property: assignment.property.clone(),
                    value: assignment.value.clone(),
                })
                .collect::<Vec<_>>();
            let ids = store.set_relationship_properties(
                catalog,
                RelationshipPropertiesUpdate {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter: relationship_filter_from_properties_and_predicate(
                        rel_properties,
                        rel_predicate.as_ref(),
                    )?,
                    assignments,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("rel_id", Value::Int(id.0 as i64)))
                .collect())
        }
        PhysicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => {
            let label_id = if label.is_empty() {
                None
            } else {
                let Some(label_id) = catalog.label_id(label) else {
                    return Ok(Vec::new());
                };
                Some(label_id)
            };
            let candidate_filter = predicate
                .as_ref()
                .and_then(|predicate| node_scan_filter_from_predicate(predicate, variable));
            let mut ids = Vec::new();
            store.try_visit_nodes_owned(label_id, |node| {
                if candidate_filter
                    .as_ref()
                    .is_some_and(|filter| !node_matches_property_filter(&node, filter))
                {
                    return Ok(GraphScanControl::Continue);
                }
                let id = node.id;
                let binding = Binding {
                    values: BTreeMap::new(),
                    nodes: BTreeMap::from([(variable.clone(), node)]),
                    relationships: BTreeMap::new(),
                };
                if let Some(predicate) = predicate
                    && !evaluate_predicate(predicate, catalog, store, &binding)?
                {
                    return Ok(GraphScanControl::Continue);
                }
                ids.push(id);
                Ok(GraphScanControl::Continue)
            })?;
            let ids = store.delete_node_ids(catalog, &ids, *detach)?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("node_id", Value::Int(id.0 as i64)))
                .collect())
        }
        PhysicalPlan::DeleteRelationship {
            source_label,
            predicate,
            rel_type,
            rel_properties,
            rel_predicate,
            target_label,
            target_properties,
            ..
        } => {
            let filter = predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let rel_filter = relationship_filter_from_properties_and_predicate(
                rel_properties,
                rel_predicate.as_ref(),
            )?;
            let ids = store.delete_relationships(
                catalog,
                RelationshipDeleteRequest {
                    source_label: source_label.clone(),
                    filter,
                    rel_type: rel_type.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    rel_filter,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("rel_id", Value::Int(id.0 as i64)))
                .collect())
        }
        PhysicalPlan::DeleteRelationshipTargetNodes {
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
            detach,
            ..
        } => {
            let source_filter = source_predicate
                .as_ref()
                .map(property_filter_from_predicate)
                .transpose()?;
            let ids = store.delete_relationship_target_nodes(
                catalog,
                RelationshipTargetNodeDelete {
                    source_label: source_label.clone(),
                    source_filter,
                    rel_type: rel_type.clone(),
                    rel_filter: property_filter_from_properties(rel_properties),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                    detach: *detach,
                },
            )?;
            Ok(ids
                .into_iter()
                .map(|id| Binding::scalar("node_id", Value::Int(id.0 as i64)))
                .collect())
        }
        PhysicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => {
            let (source, rel, target) = store.create_connected_nodes(
                catalog,
                ConnectedNodesCreate {
                    source_label: source_label.clone(),
                    source_properties: source_properties.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_properties: target_properties.clone(),
                },
            )?;
            Ok(vec![Binding::values(BTreeMap::from([
                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                ("rel_id".to_string(), Value::Int(rel.0 as i64)),
            ]))])
        }
        PhysicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => {
            let rows = store.create_relationships_between_matches(
                catalog,
                MatchedRelationshipCreate {
                    source_label: source_label.clone(),
                    source_filter: property_filter_from_properties(source_properties),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    target_label: target_label.clone(),
                    target_filter: property_filter_from_properties(target_properties),
                },
            )?;
            Ok(rows
                .into_iter()
                .map(|(source, rel, target)| {
                    Binding::values(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(rel.0 as i64)),
                    ]))
                })
                .collect())
        }
        PhysicalPlan::EmptyExec
        | PhysicalPlan::NodeCountExec { .. }
        | PhysicalPlan::RelationshipCountExec { .. }
        | PhysicalPlan::SeqNodeScan { .. }
        | PhysicalPlan::NodeProjectionScanExec { .. }
        | PhysicalPlan::SourceSegmentScan { .. }
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
        | PhysicalPlan::ShortestPathExec { .. }
        | PhysicalPlan::NodeCartesianProductExec { .. }
        | PhysicalPlan::HashJoinExec { .. }
        | PhysicalPlan::FilterExec { .. }
        | PhysicalPlan::ProjectExec { .. }
        | PhysicalPlan::AggregateExec { .. }
        | PhysicalPlan::DistinctExec { .. }
        | PhysicalPlan::SortExec { .. }
        | PhysicalPlan::TopNExec { .. }
        | PhysicalPlan::LimitExec { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::VectorSeedScan { .. }
        | PhysicalPlan::ThreadRepairStatsExec { .. } => {
            unreachable!("batch-capable plan bypassed pipeline dispatch")
        }
    }
}
