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

use super::*;

pub fn plan(statement: &Statement) -> Result<LogicalPlan> {
    plan_with_params(statement, &BTreeMap::new())
}

pub fn plan_with_params(
    statement: &Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    match statement {
        Statement::BeginTransaction | Statement::Commit | Statement::Rollback => {
            Err(HawDBError::Semantic(
                "transaction control is executed by a database session".to_string(),
            ))
        }
        Statement::Checkpoint => Err(HawDBError::Semantic(
            "CHECKPOINT is executed by the database session".to_string(),
        )),
        Statement::CypherQuery(_) => Err(HawDBError::Semantic(
            "CYPHER system hints are applied before planning".to_string(),
        )),
        Statement::Explain(_) => Err(HawDBError::Semantic(
            "EXPLAIN is executed by the database query runtime".to_string(),
        )),
        Statement::SetSystemVariable(_) => Err(HawDBError::Semantic(
            "SET system variable is executed by the database session".to_string(),
        )),
        Statement::CreateNodeLabel(label) => Ok(LogicalPlan::CreateNodeLabel {
            label: label.clone(),
        }),
        Statement::CreateRelationshipType(rel_type) => Ok(LogicalPlan::CreateRelationshipType {
            rel_type: rel_type.clone(),
        }),
        Statement::CreateNodeTable(name) => Ok(LogicalPlan::CreateNodeTable { name: name.clone() }),
        Statement::CreateRelationshipTable(name) => {
            Ok(LogicalPlan::CreateRelationshipTable { name: name.clone() })
        }
        Statement::CreateProperty(property) => Ok(LogicalPlan::CreateProperty {
            table_kind: property.table_kind,
            table: property.table.clone(),
            property: property.property.clone(),
            value_type: property.value_type,
            nullable: property.nullable,
        }),
        Statement::AlterTableState(alter) => Ok(LogicalPlan::AlterTableState {
            table_kind: alter.table_kind,
            table: alter.table.clone(),
            state: alter.state,
        }),
        Statement::AlterPropertyState(alter) => Ok(LogicalPlan::AlterPropertyState {
            table_kind: alter.table_kind,
            table: alter.table.clone(),
            property: alter.property.clone(),
            state: alter.state,
        }),
        Statement::CreateIndex(index) => Ok(LogicalPlan::CreateIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateCompositeIndex(index) => Ok(LogicalPlan::CreateCompositeIndex {
            label: index.label.clone(),
            properties: index.properties.clone(),
        }),
        Statement::CreateRangeIndex(index) => Ok(LogicalPlan::CreateRangeIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateFullTextIndex(index) => Ok(LogicalPlan::CreateFullTextIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateUniqueConstraint(index) => Ok(LogicalPlan::CreateUniqueConstraint {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateNodePropertyExistsConstraint(index) => {
            Ok(LogicalPlan::CreateNodePropertyExistsConstraint {
                label: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::CreateRelationshipUniqueConstraint(index) => {
            Ok(LogicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::CreateRelationshipPropertyExistsConstraint(index) => {
            Ok(LogicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::ProjectGraph(project) => Ok(LogicalPlan::ProjectGraph {
            name: project.name.clone(),
            node_labels: project.node_labels.clone(),
            rel_types: project.rel_types.clone(),
        }),
        Statement::GraphAlgorithm(algorithm) => Ok(LogicalPlan::GraphAlgorithm {
            algorithm: plan_graph_algorithm_kind(algorithm.algorithm),
            graph_name: algorithm.graph_name.clone(),
            options: bind_graph_algorithm_options(&algorithm.options, parameters)?,
            score_column: algorithm.score_column.clone(),
            node_visibility_predicate: None,
        }),
        Statement::VectorSearch(search) => bind_vector_seed(search, parameters, false),
        Statement::CreateNode(node) => Ok(LogicalPlan::CreateNode {
            label: node.label.clone(),
            properties: bind_properties(&node.properties, parameters)?,
        }),
        Statement::MergeNode(node) => {
            let on_create_properties = bind_on_create_set_properties(
                node.variable.as_deref(),
                &node.on_create_sets,
                parameters,
            )?;
            let on_match_assignments = bind_on_match_set_assignments(
                node.variable.as_deref(),
                &node.on_match_sets,
                parameters,
            )?;
            let post_merge_assignments = bind_post_merge_set_assignments(
                node.variable.as_deref(),
                &node.post_merge_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeNode {
                label: node.label.clone(),
                match_properties: bind_properties(&node.properties, parameters)?,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            })
        }
        Statement::MergeRelationship(relationship) => Ok(LogicalPlan::MergeRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
        Statement::MatchCreateRelationship(create) => {
            if create.create_source_variable != create.source_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship CREATE source variable '{}' does not match bound variable '{}'",
                    create.create_source_variable, create.source_variable
                )));
            }
            if create.create_target_variable != create.target_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship CREATE target variable '{}' does not match bound variable '{}'",
                    create.create_target_variable, create.target_variable
                )));
            }
            let (source_properties, target_properties) = bind_two_node_relationship_create_filters(
                &create.source_variable,
                &create.source_properties,
                &create.target_variable,
                &create.target_properties,
                create.predicate.as_ref(),
                parameters,
            )?;
            Ok(LogicalPlan::CreateMatchedRelationship {
                source_label: create.source_label.clone(),
                source_properties,
                target_label: create.target_label.clone(),
                target_properties,
                rel_type: create.rel_type.clone(),
                rel_properties: bind_properties(&create.rel_properties, parameters)?,
            })
        }
        Statement::MatchMergeRelationship(merge) => {
            if merge.merge_source_variable != merge.source_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship MERGE source variable '{}' does not match bound variable '{}'",
                    merge.merge_source_variable, merge.source_variable
                )));
            }
            if merge.merge_target_variable != merge.target_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship MERGE target variable '{}' does not match bound variable '{}'",
                    merge.merge_target_variable, merge.target_variable
                )));
            }
            let (source_properties, target_properties) = bind_two_node_relationship_create_filters(
                &merge.source_variable,
                &merge.source_properties,
                &merge.target_variable,
                &merge.target_properties,
                merge.predicate.as_ref(),
                parameters,
            )?;
            let on_create_properties = bind_relationship_on_create_set_properties(
                merge.rel_variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeMatchedRelationship {
                source_label: merge.source_label.clone(),
                source_properties,
                target_label: merge.target_label.clone(),
                target_properties,
                rel_type: merge.rel_type.clone(),
                rel_match_properties: bind_properties(&merge.rel_properties, parameters)?,
                on_create_properties,
            })
        }
        Statement::MatchExpandMergeRelationship(merge) => {
            if merge.expand.direction != RelationshipDirection::Outgoing
                || merge.expand.min_hops != 1
                || merge.expand.max_hops != 1
            {
                return Err(HawDBError::Semantic(
                    "relationship-copy MERGE supports only one-hop outgoing MATCH patterns"
                        .to_string(),
                ));
            }
            if merge.merge_source_variable != merge.source_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship-copy MERGE source variable '{}' does not match bound variable '{}'",
                    merge.merge_source_variable, merge.source_variable
                )));
            }
            if merge.merge_target_variable != merge.expand.target_variable {
                return Err(HawDBError::Semantic(format!(
                    "relationship-copy MERGE target variable '{}' does not match bound variable '{}'",
                    merge.merge_target_variable, merge.expand.target_variable
                )));
            }
            if merge.predicate.is_some() {
                return Err(HawDBError::Semantic(
                    "relationship-copy MERGE does not support WHERE predicates".to_string(),
                ));
            }
            let on_create_properties = bind_relationship_copy_on_create_set_properties(
                merge.rel_variable.as_deref(),
                merge.expand.variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label: merge.source_label.clone(),
                source_properties: bind_properties(&merge.source_properties, parameters)?,
                old_rel_variable: merge.expand.variable.clone(),
                old_rel_type: merge.expand.rel_type.clone(),
                old_rel_properties: bind_properties(&merge.expand.properties, parameters)?,
                target_label: merge.expand.target_label.clone(),
                target_properties: bind_properties(&merge.expand.target_properties, parameters)?,
                new_rel_type: merge.rel_type.clone(),
                new_rel_match_properties: bind_properties(&merge.rel_properties, parameters)?,
                on_create_properties,
            })
        }
        Statement::MatchExpandMatchMergeRelationship(merge) => {
            if merge.expand.direction != RelationshipDirection::Outgoing
                || merge.expand.min_hops != 1
                || merge.expand.max_hops != 1
            {
                return Err(HawDBError::Semantic(
                    "relationship retarget MERGE supports only one-hop outgoing MATCH patterns"
                        .to_string(),
                ));
            }
            let (source_properties, new_target_properties) =
                bind_two_node_relationship_create_filters(
                    &merge.source_variable,
                    &merge.source_properties,
                    &merge.matched_target_variable,
                    &merge.matched_target_properties,
                    merge.predicate.as_ref(),
                    parameters,
                )?;
            let on_create_properties = bind_relationship_on_create_set_properties(
                merge.rel_variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            let old_rel_properties = bind_properties(&merge.expand.properties, parameters)?;
            let old_target_properties =
                bind_properties(&merge.expand.target_properties, parameters)?;
            let new_rel_match_properties = bind_properties(&merge.rel_properties, parameters)?;
            if merge.merge_source_variable == merge.source_variable
                && merge.merge_target_variable == merge.matched_target_variable
            {
                return Ok(LogicalPlan::MergeRelationshipToMatchedTarget {
                    source_label: merge.source_label.clone(),
                    source_properties,
                    old_rel_type: merge.expand.rel_type.clone(),
                    old_rel_properties,
                    old_target_label: merge.expand.target_label.clone(),
                    old_target_properties,
                    new_target_label: merge.matched_target_label.clone(),
                    new_target_properties,
                    new_rel_type: merge.rel_type.clone(),
                    new_rel_match_properties,
                    on_create_properties,
                });
            }
            if merge.merge_source_variable == merge.matched_target_variable
                && merge.merge_target_variable == merge.expand.target_variable
            {
                return Ok(LogicalPlan::MergeRelationshipFromMatchedTarget {
                    old_source_label: merge.source_label.clone(),
                    old_source_properties: source_properties,
                    old_rel_type: merge.expand.rel_type.clone(),
                    old_rel_properties,
                    old_target_label: merge.expand.target_label.clone(),
                    old_target_properties,
                    new_source_label: merge.matched_target_label.clone(),
                    new_source_properties: new_target_properties,
                    new_rel_type: merge.rel_type.clone(),
                    new_rel_match_properties,
                    on_create_properties,
                });
            }
            Err(HawDBError::Semantic(format!(
                "relationship retarget MERGE variables '{}'-'{}' do not match supported bound pairs '{}'-'{}' or '{}'-'{}'",
                merge.merge_source_variable,
                merge.merge_target_variable,
                merge.source_variable,
                merge.matched_target_variable,
                merge.matched_target_variable,
                merge.expand.target_variable
            )))
        }
        Statement::MatchSet(update) => {
            if update.sets.is_empty() {
                return Err(HawDBError::Semantic(
                    "SET requires at least one assignment".to_string(),
                ));
            }
            if let Some(expand) = &update.expand {
                if expand.min_hops != 1 || expand.max_hops != 1 {
                    return Err(HawDBError::Semantic(
                        "relationship SET supports only one-hop relationship patterns".to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing {
                    return Err(HawDBError::Semantic(
                        "relationship SET supports only outgoing relationship patterns".to_string(),
                    ));
                }
                if expand.rel_type.is_empty() {
                    return Err(HawDBError::Semantic(
                        "relationship SET requires a relationship type".to_string(),
                    ));
                }
                let Some(rel_variable) = &expand.variable else {
                    return Err(HawDBError::Semantic(
                        "relationship SET requires a relationship variable".to_string(),
                    ));
                };
                for set in &update.sets {
                    if set.variable != *rel_variable {
                        return Err(HawDBError::Semantic(format!(
                            "relationship SET can only update relationship variable '{}', got '{}'",
                            rel_variable, set.variable
                        )));
                    }
                }
                let mutation_predicate = plan_relationship_mutation_predicate(
                    update.predicate.as_ref(),
                    &update.variable,
                    rel_variable,
                    &expand.target_variable,
                    &expand.target_properties,
                    parameters,
                )?;
                let predicate = combine_pattern_and_optional_predicate(
                    &update.variable,
                    &update.properties,
                    mutation_predicate.source_predicate,
                    parameters,
                )?;
                let assignments = update
                    .sets
                    .iter()
                    .map(|set| {
                        Ok(RelationshipSetAssignment {
                            property: set.property.clone(),
                            value: bind_relationship_set_value(&set.value, parameters)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if assignments.len() == 1 {
                    let assignment = assignments
                        .into_iter()
                        .next()
                        .expect("relationship SET assignment is present");
                    return Ok(LogicalPlan::SetRelationshipProperty {
                        source_variable: update.variable.clone(),
                        source_label: update.label.clone(),
                        predicate,
                        rel_variable: rel_variable.clone(),
                        rel_type: expand.rel_type.clone(),
                        rel_properties: bind_properties(&expand.properties, parameters)?,
                        rel_predicate: mutation_predicate.rel_predicate,
                        target_variable: expand.target_variable.clone(),
                        target_label: expand.target_label.clone(),
                        target_properties: mutation_predicate.target_properties,
                        property: assignment.property,
                        value: assignment.value,
                    });
                }
                return Ok(LogicalPlan::SetRelationshipProperties {
                    source_variable: update.variable.clone(),
                    source_label: update.label.clone(),
                    predicate,
                    rel_variable: rel_variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    rel_predicate: mutation_predicate.rel_predicate,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    target_properties: mutation_predicate.target_properties,
                    assignments,
                });
            }
            let scope = BTreeSet::from([update.variable.clone()]);
            if let Some(predicate) = &update.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut assignments = Vec::with_capacity(update.sets.len());
            for set in &update.sets {
                if set.variable != update.variable {
                    return Err(HawDBError::Semantic(format!(
                        "unknown variable '{}' in set item",
                        set.variable
                    )));
                }
                assignments.push(SetAssignment {
                    property: set.property.clone(),
                    value: plan_set_value(set, parameters)?,
                });
            }
            Ok(LogicalPlan::SetNodeProperties {
                variable: update.variable.clone(),
                label: update.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &update.variable,
                    &update.properties,
                    update.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                assignments,
            })
        }
        Statement::MatchSetReturn(update_return) => {
            let update = &update_return.update;
            if update.expand.is_some() {
                return Err(HawDBError::Semantic(
                    "SET RETURN supports only single-node MATCH updates".to_string(),
                ));
            }
            if update.sets.is_empty() {
                return Err(HawDBError::Semantic(
                    "SET requires at least one assignment".to_string(),
                ));
            }
            let scope = BTreeSet::from([update.variable.clone()]);
            if let Some(predicate) = &update.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut assignments = Vec::with_capacity(update.sets.len());
            for set in &update.sets {
                if set.variable != update.variable {
                    return Err(HawDBError::Semantic(format!(
                        "unknown variable '{}' in set item",
                        set.variable
                    )));
                }
                assignments.push(SetAssignment {
                    property: set.property.clone(),
                    value: plan_set_value(set, parameters)?,
                });
            }
            let returns =
                plan_set_node_properties_return_mode(update, &update_return.returns, parameters)?;
            Ok(LogicalPlan::SetNodePropertiesReturn {
                variable: update.variable.clone(),
                label: update.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &update.variable,
                    &update.properties,
                    update.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                assignments,
                returns,
            })
        }
        Statement::MatchOptionalRelationshipCountSum(query) => {
            if query.legs.is_empty() {
                return Err(HawDBError::Semantic(
                    "optional relationship count sum requires at least one count leg".to_string(),
                ));
            }
            Ok(LogicalPlan::OptionalRelationshipCountSum {
                variable: query.variable.clone(),
                label: query.label.clone(),
                properties: bind_properties(&query.properties, parameters)?,
                legs: bind_relationship_count_legs(&query.legs, parameters)?,
                output: query.output.clone(),
            })
        }
        Statement::MatchThreadRepairStats(query) => Ok(LogicalPlan::ThreadRepairStats {
            label: query.label.clone(),
            identity_label: query.identity_label.clone(),
            identity_ref_property: query.identity_ref_property.clone(),
            thread_id_property: query.thread_id_property.clone(),
            message_rel_type: query.message_rel_type.clone(),
            message_label: query.message_label.clone(),
            memory_rel_type: query.memory_rel_type.clone(),
            memory_label: query.memory_label.clone(),
        }),
        Statement::MatchDelete(delete) => {
            if let Some(expand) = &delete.expand {
                if expand.min_hops != 1 || expand.max_hops != 1 {
                    return Err(HawDBError::Semantic(
                        "relationship DELETE supports only one-hop relationship patterns"
                            .to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing {
                    return Err(HawDBError::Semantic(
                        "relationship DELETE supports only outgoing relationship patterns"
                            .to_string(),
                    ));
                }
                if expand.rel_type.is_empty() {
                    return Err(HawDBError::Semantic(
                        "relationship DELETE requires a relationship type".to_string(),
                    ));
                }
                if delete.detach {
                    if delete.delete_variable != expand.target_variable {
                        return Err(HawDBError::Semantic(format!(
                            "DETACH DELETE after relationship MATCH can only delete target variable '{}', got '{}'",
                            expand.target_variable, delete.delete_variable
                        )));
                    }
                    if expand.variable.is_some() || !expand.properties.is_empty() {
                        return Err(HawDBError::Semantic(
                            "DETACH DELETE after relationship MATCH does not support relationship variables or properties yet".to_string(),
                        ));
                    }
                    let scope = BTreeSet::from([delete.variable.clone()]);
                    let source_predicate = combine_pattern_and_optional_cypher_predicate(
                        &delete.variable,
                        &delete.properties,
                        delete.predicate.as_ref(),
                        &scope,
                        parameters,
                    )?;
                    return Ok(LogicalPlan::DeleteRelationshipTargetNodes {
                        source_variable: delete.variable.clone(),
                        source_label: delete.label.clone(),
                        source_predicate,
                        rel_type: expand.rel_type.clone(),
                        rel_properties: BTreeMap::new(),
                        target_variable: expand.target_variable.clone(),
                        target_label: expand.target_label.clone(),
                        target_properties: bind_properties(&expand.target_properties, parameters)?,
                        detach: true,
                    });
                }
                let Some(rel_variable) = &expand.variable else {
                    return Err(HawDBError::Semantic(
                        "relationship DELETE requires a relationship variable".to_string(),
                    ));
                };
                if delete.delete_variable != *rel_variable {
                    return Err(HawDBError::Semantic(format!(
                        "relationship DELETE can only delete relationship variable '{}', got '{}'",
                        rel_variable, delete.delete_variable
                    )));
                }
                let mutation_predicate = plan_relationship_mutation_predicate(
                    delete.predicate.as_ref(),
                    &delete.variable,
                    rel_variable,
                    &expand.target_variable,
                    &expand.target_properties,
                    parameters,
                )?;
                let predicate = combine_pattern_and_optional_predicate(
                    &delete.variable,
                    &delete.properties,
                    mutation_predicate.source_predicate,
                    parameters,
                )?;
                return Ok(LogicalPlan::DeleteRelationship {
                    source_variable: delete.variable.clone(),
                    source_label: delete.label.clone(),
                    predicate,
                    rel_variable: rel_variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    rel_predicate: mutation_predicate.rel_predicate,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    target_properties: mutation_predicate.target_properties,
                });
            }
            let scope = BTreeSet::from([delete.variable.clone()]);
            if let Some(predicate) = &delete.predicate {
                validate_predicate(&scope, predicate)?;
            }
            if delete.delete_variable != delete.variable {
                return Err(HawDBError::Semantic(format!(
                    "unknown variable '{}' in delete item",
                    delete.delete_variable
                )));
            }
            Ok(LogicalPlan::DeleteNode {
                variable: delete.variable.clone(),
                label: delete.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &delete.variable,
                    &delete.properties,
                    delete.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                detach: delete.detach,
            })
        }
        Statement::CreateRelationship(relationship) => Ok(LogicalPlan::CreateRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
        Statement::MatchNodesReturn(query) => {
            if query.left_variable == query.right_variable {
                return Err(HawDBError::Semantic(format!(
                    "duplicate node variable '{}' in match pattern",
                    query.left_variable
                )));
            }
            let scope = BTreeSet::from([query.left_variable.clone(), query.right_variable.clone()]);
            let mut left = LogicalPlan::NodeScan {
                variable: query.left_variable.clone(),
                label: query.left_label.clone(),
            };
            if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                &query.left_variable,
                &query.left_properties,
                parameters,
            )?) {
                left = LogicalPlan::Filter {
                    predicate,
                    input: Box::new(left),
                };
            }
            let mut right = LogicalPlan::NodeScan {
                variable: query.right_variable.clone(),
                label: query.right_label.clone(),
            };
            if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                &query.right_variable,
                &query.right_properties,
                parameters,
            )?) {
                right = LogicalPlan::Filter {
                    predicate,
                    input: Box::new(right),
                };
            }
            let input = LogicalPlan::NodeCartesianProduct {
                left: Box::new(left),
                right: Box::new(right),
            };
            let mut input = if let Some(predicate) = query
                .predicate
                .as_ref()
                .map(|predicate| plan_predicate(predicate, &scope, parameters))
                .transpose()?
            {
                LogicalPlan::Filter {
                    predicate,
                    input: Box::new(input),
                }
            } else {
                input
            };
            let planned_returns = plan_return_items(&scope, &query.returns, parameters)?;
            input = planned_returns.into_logical(input);
            let limit = query
                .limit
                .as_ref()
                .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                .transpose()?;
            if let Some(limit) = limit {
                input = LogicalPlan::Limit {
                    offset: 0,
                    limit: Some(limit),
                    input: Box::new(input),
                };
            }
            Ok(input)
        }
        Statement::ShortestPathReturn(query) => plan_shortest_path_return(query, parameters),
        Statement::MatchReturn(query) => {
            let mut scope = BTreeSet::from([query.variable.clone()]);
            let mut input_columns = BTreeSet::new();
            if query.vector_seed.is_some() {
                input_columns.extend(["id".to_string(), "score".to_string()]);
            }
            if let Some(expand) = &query.expand {
                scope.insert(expand.target_variable.clone());
                if !expand.properties.is_empty() && (expand.min_hops != 1 || expand.max_hops != 1) {
                    return Err(HawDBError::Semantic(
                        "relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if expand.rel_type.is_empty() && (expand.min_hops != 1 || expand.max_hops != 1) {
                    return Err(HawDBError::Semantic(
                        "untyped relationship patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing
                    && (expand.min_hops != 1 || expand.max_hops != 1)
                {
                    return Err(HawDBError::Semantic(
                        "non-outgoing relationship patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if !expand.target_properties.is_empty()
                    && (expand.min_hops != 1 || expand.max_hops != 1)
                {
                    return Err(HawDBError::Semantic(
                        "target node property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if let Some(rel_variable) = &expand.variable {
                    if expand.min_hops != 1 || expand.max_hops != 1 {
                        return Err(HawDBError::Semantic(
                            "relationship variables are supported only for one-hop patterns"
                                .to_string(),
                        ));
                    }
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(post_expand) = &query.post_match_expand {
                if !scope.contains(&post_expand.source_variable) {
                    return Err(HawDBError::Semantic(format!(
                        "post-MATCH source variable '{}' is not bound",
                        post_expand.source_variable
                    )));
                }
                if !post_expand.source_properties.is_empty() {
                    return Err(HawDBError::Semantic(
                        "post-MATCH relationship reads do not support source property patterns"
                            .to_string(),
                    ));
                }
                if !post_expand.expand.properties.is_empty()
                    && (post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1)
                {
                    return Err(HawDBError::Semantic(
                        "post-MATCH relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if !post_expand.expand.target_properties.is_empty()
                    && (post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1)
                {
                    return Err(HawDBError::Semantic(
                        "post-MATCH target node property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                scope.insert(post_expand.expand.target_variable.clone());
                if let Some(rel_variable) = &post_expand.expand.variable {
                    if post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1 {
                        return Err(HawDBError::Semantic(
                            "post-MATCH relationship variables are supported only for one-hop patterns"
                                .to_string(),
                        ));
                    }
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(optional) = &query.optional_expand {
                if !scope.contains(&optional.source_variable) {
                    return Err(HawDBError::Semantic(format!(
                        "OPTIONAL MATCH source variable '{}' is not bound",
                        optional.source_variable
                    )));
                }
                if query.optional_with.is_none()
                    && !returns_are_count_only(&query.returns)
                    && optional_direct_count_alias(query, optional)?.is_none()
                    && optional_direct_collect_alias(query, optional)?.is_none()
                    && !optional_direct_row_projection(query, optional)
                {
                    return Err(HawDBError::Semantic(
                        "OPTIONAL MATCH is currently supported only for COUNT returns, source projections plus one COUNT, source projections plus one COLLECT, or non-aggregate row projections".to_string(),
                    ));
                }
                if !optional.expand.properties.is_empty()
                    && (optional.expand.min_hops != 1 || optional.expand.max_hops != 1)
                {
                    return Err(HawDBError::Semantic(
                        "OPTIONAL MATCH relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                scope.insert(optional.expand.target_variable.clone());
                if let Some(rel_variable) = &optional.expand.variable {
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(optional_with) = &query.optional_with {
                if let Some(optional) = &query.optional_expand {
                    if optional_with.group_variable != optional.source_variable {
                        return Err(HawDBError::Semantic(
                            "OPTIONAL MATCH WITH must group by the optional source variable"
                                .to_string(),
                        ));
                    }
                    let count_variable = optional_with.count_variable.as_str();
                    let count_matches_relationship =
                        optional.expand.variable.as_deref() == Some(count_variable);
                    let count_matches_target = optional.expand.target_variable == count_variable;
                    if !count_matches_relationship && !count_matches_target {
                        return Err(HawDBError::Semantic(
                            "OPTIONAL MATCH WITH COUNT must reference the optional relationship or target variable"
                                .to_string(),
                        ));
                    }
                    if query.returns.iter().any(|item| {
                        matches!(
                            item.expression,
                            AstNode {
                                kind: ReturnExpressionKind::Aggregate(_),
                                ..
                            }
                        )
                    }) {
                        return Err(HawDBError::Semantic(
                            "OPTIONAL MATCH WITH supports only projection returns".to_string(),
                        ));
                    }
                    validate_with_alias_filter(
                        query.aggregate_with_filter.as_ref(),
                        &scope,
                        &BTreeSet::from([optional_with.alias.clone()]),
                    )?;
                } else {
                    let aggregate_with = optional_with_as_aggregate(optional_with);
                    validate_aggregate_with_match_return(query, &aggregate_with)?;
                }
            }
            if let Some(collect_with) = &query.collect_with {
                validate_collect_with_match_return(query, collect_with)?;
            }
            if let Some(distinct_with) = &query.distinct_with {
                validate_distinct_with_match_return(query, distinct_with)?;
            }
            if let Some(with_projection) = &query.with_projection {
                validate_with_projection_match_return(query, &scope, with_projection)?;
            }
            if let Some(aggregate_with) = &query.aggregate_with {
                validate_aggregate_with_match_return(query, aggregate_with)?;
            }
            if let Some(predicate) = &query.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let vector_seeded_max_hops = query
                .expand
                .as_ref()
                .map(|expand| expand.max_hops)
                .unwrap_or_default()
                .saturating_add(
                    query
                        .post_match_expand
                        .as_ref()
                        .map(|expand| expand.expand.max_hops)
                        .unwrap_or_default(),
                );
            if query.vector_seed.is_some() && vector_seeded_max_hops > MAX_VECTOR_SEEDED_GRAPH_HOPS
            {
                return Err(HawDBError::Semantic(format!(
                    "vector-seeded graph expansion supports at most {MAX_VECTOR_SEEDED_GRAPH_HOPS} hops"
                )));
            }
            let mut input = if let Some(search) = &query.vector_seed {
                LogicalPlan::NodeColumnLookup {
                    variable: query.variable.clone(),
                    label: query.label.clone(),
                    property: "id".to_string(),
                    column: "external_id".to_string(),
                    optional: false,
                    input: Box::new(bind_vector_seed(search, parameters, true)?),
                }
            } else {
                LogicalPlan::NodeScan {
                    variable: query.variable.clone(),
                    label: query.label.clone(),
                }
            };
            if let Some(expand) = &query.expand {
                input = LogicalPlan::Expand {
                    source_variable: query.variable.clone(),
                    source_label: query.label.clone(),
                    rel_variable: expand.variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    direction: expand.direction,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    min_hops: expand.min_hops,
                    max_hops: expand.max_hops,
                    optional: false,
                    input: Box::new(input),
                };
            }
            if let Some(post_expand) = &query.post_match_expand {
                input = LogicalPlan::Expand {
                    source_variable: post_expand.source_variable.clone(),
                    source_label: post_expand.source_label.clone(),
                    rel_variable: post_expand.expand.variable.clone(),
                    rel_type: post_expand.expand.rel_type.clone(),
                    rel_properties: bind_properties(&post_expand.expand.properties, parameters)?,
                    direction: post_expand.expand.direction,
                    target_variable: post_expand.expand.target_variable.clone(),
                    target_label: post_expand.expand.target_label.clone(),
                    min_hops: post_expand.expand.min_hops,
                    max_hops: post_expand.expand.max_hops,
                    optional: false,
                    input: Box::new(input),
                };
            }
            let pattern_predicate = plan_match_pattern_predicate(
                &query.variable,
                &query.properties,
                query.expand.as_ref(),
                query.post_match_expand.as_ref(),
                parameters,
            )?;
            let predicate = combine_pattern_and_optional_cypher_predicate_parts(
                pattern_predicate,
                query.predicate.as_ref(),
                &scope,
                parameters,
            )?;
            if let Some(predicate) = predicate
                && let Some(predicate) =
                    pushdown_relationship_property_eq_predicates(&mut input, predicate)
            {
                input = LogicalPlan::Filter {
                    predicate,
                    input: Box::new(input),
                };
            }
            if let Some(collect_with) = &query.collect_with {
                return plan_collect_with_match_return(input, query, collect_with);
            }
            if let Some(distinct_with) = &query.distinct_with {
                return plan_distinct_with_match_return(
                    input,
                    &scope,
                    query,
                    distinct_with,
                    parameters,
                );
            }
            if let Some(aggregate_with) = &query.aggregate_with {
                return plan_aggregate_with_match_return(
                    input,
                    &scope,
                    query,
                    aggregate_with,
                    parameters,
                );
            }
            if let Some(with_projection) = &query.with_projection {
                input = plan_with_projection(input, &scope, with_projection, parameters)?;
                let column_names = with_projection_column_names(with_projection);
                if let Some(filter) = &query.aggregate_with_filter {
                    input = LogicalPlan::Filter {
                        predicate: plan_with_alias_filter(filter, parameters)?,
                        input: Box::new(input),
                    };
                }
                if !query.with_order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &column_names,
                            &query.with_order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let with_offset = query
                    .with_offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let with_limit = query
                    .with_limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if with_offset > 0 || with_limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset: with_offset,
                        limit: with_limit,
                        input: Box::new(input),
                    };
                }
                let projections = query
                    .returns
                    .iter()
                    .map(|item| {
                        plan_projection_with_columns(&scope, &column_names, item, parameters)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let projection_names = projections
                    .iter()
                    .map(|projection| projection.name.clone())
                    .collect::<BTreeSet<_>>();
                input = LogicalPlan::Project {
                    items: projections,
                    input: Box::new(input),
                };
                if query.distinct {
                    input = LogicalPlan::Distinct {
                        input: Box::new(input),
                    };
                }
                if !query.order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &projection_names,
                            &query.order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let offset = query
                    .offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let limit = query
                    .limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if offset > 0 || limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset,
                        limit,
                        input: Box::new(input),
                    };
                }
                return Ok(input);
            }
            if let Some(optional_with) = &query.optional_with {
                if query.optional_expand.is_none() {
                    let aggregate_with = optional_with_as_aggregate(optional_with);
                    return plan_aggregate_with_match_return(
                        input,
                        &scope,
                        query,
                        &aggregate_with,
                        parameters,
                    );
                }
                let optional = query
                    .optional_expand
                    .as_ref()
                    .expect("optional WITH validated above");
                input = LogicalPlan::OptionalDegree {
                    source_variable: optional.source_variable.clone(),
                    rel_type: optional.expand.rel_type.clone(),
                    rel_properties: bind_properties(&optional.expand.properties, parameters)?,
                    direction: optional.expand.direction,
                    target_label: optional.expand.target_label.clone(),
                    target_properties: bind_properties(
                        &optional.expand.target_properties,
                        parameters,
                    )?,
                    alias: optional_with.alias.clone(),
                    input: Box::new(input),
                };
                if let Some(filter) = &query.aggregate_with_filter {
                    input = LogicalPlan::Filter {
                        predicate: plan_with_alias_filter(filter, parameters)?,
                        input: Box::new(input),
                    };
                }
                let projections = query
                    .returns
                    .iter()
                    .map(|item| {
                        plan_projection_with_columns(
                            &scope,
                            &BTreeSet::from([optional_with.alias.clone()]),
                            item,
                            parameters,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let projection_names = projections
                    .iter()
                    .map(|projection| projection.name.clone())
                    .collect::<BTreeSet<_>>();
                input = LogicalPlan::Project {
                    items: projections,
                    input: Box::new(input),
                };
                if query.distinct {
                    input = LogicalPlan::Distinct {
                        input: Box::new(input),
                    };
                }
                if !query.order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &projection_names,
                            &query.order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let offset = query
                    .offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let limit = query
                    .limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if offset > 0 || limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset,
                        limit,
                        input: Box::new(input),
                    };
                }
                return Ok(input);
            }
            if let Some(optional) = &query.optional_expand
                && let Some(count_alias) = optional_direct_count_alias(query, optional)?
            {
                return plan_optional_direct_count_return(
                    input,
                    &scope,
                    query,
                    optional,
                    count_alias,
                    parameters,
                );
            }
            if let Some(optional) = &query.optional_expand {
                let optional_row_projection = optional_direct_row_projection(query, optional);
                input = LogicalPlan::Expand {
                    source_variable: optional.source_variable.clone(),
                    source_label: optional.source_label.clone(),
                    rel_variable: optional.expand.variable.clone(),
                    rel_type: optional.expand.rel_type.clone(),
                    rel_properties: bind_properties(&optional.expand.properties, parameters)?,
                    direction: optional.expand.direction,
                    target_variable: optional.expand.target_variable.clone(),
                    target_label: optional.expand.target_label.clone(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: optional_row_projection
                        || optional_direct_collect_alias(query, optional)?.is_some(),
                    input: Box::new(input),
                };
                if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                    &optional.expand.target_variable,
                    &optional.expand.target_properties,
                    parameters,
                )?) {
                    input = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(input),
                    };
                }
            }
            let planned_returns =
                plan_return_items_with_columns(&scope, &input_columns, &query.returns, parameters)?;
            let projection_names = planned_returns
                .names()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut input = planned_returns.into_logical(input);
            if query.distinct {
                input = LogicalPlan::Distinct {
                    input: Box::new(input),
                };
            }
            if !query.order_by.is_empty() {
                input = LogicalPlan::Sort {
                    items: plan_sort_items(
                        planned_sort_scope(&input, &scope),
                        &projection_names,
                        &query.order_by,
                        parameters,
                    )?,
                    input: Box::new(input),
                };
            }
            let offset = match &query.offset {
                Some(offset) => bind_pagination_value(offset, parameters, "offset")?,
                None => 0,
            };
            let limit = query
                .limit
                .as_ref()
                .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                .transpose()?;
            if offset != 0 || limit.is_some() {
                input = LogicalPlan::Limit {
                    offset,
                    limit,
                    input: Box::new(input),
                };
            }
            Ok(input)
        }
    }
}
