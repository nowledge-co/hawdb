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
        Statement::UnwindMutation(query) => plan_parsed_pipeline_query(query, parameters),
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
            relationship_predicates: bind_projected_relationship_predicates(
                &project.relationship_predicates,
            )?,
        }),
        Statement::GraphAlgorithm(algorithm) => Ok(LogicalPlan::GraphAlgorithm {
            algorithm: plan_graph_algorithm_kind(algorithm.algorithm),
            graph_name: algorithm.graph_name.clone(),
            options: bind_graph_algorithm_options(&algorithm.options, parameters)?,
            score_column: algorithm.score_column.clone(),
            return_node_identity: algorithm.return_node_identity,
            node_visibility_predicate: None,
        }),
        Statement::VectorSearch(search) => bind_vector_seed(search, parameters, false),
        Statement::Pipeline(query) => plan_parsed_pipeline_query(query, parameters),
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
        Statement::CreateRelationship(relationship) => Ok(LogicalPlan::CreateRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
    }
}
