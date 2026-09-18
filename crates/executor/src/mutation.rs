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

//! Storage-neutral translation from physical plans to graph mutation commands.
//!
//! Preflight uses the existing storage-neutral execution contracts; the embedding
//! layer retains transaction admission, atomic commit, and recovery ownership.

use hawdb_core::{HawDBError, Result};
use hawdb_ddl::{object_state_to_core, property_type_to_core, table_kind_to_core};
use hawdb_plan::{PhysicalPlan, RelationshipOnCreateValue, SetValue};
use hawdb_storage::{
    ConnectedNodesCreate, GraphMutation, MatchedRelationshipCopyMerge, MatchedRelationshipCreate,
    MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
    MatchedRelationshipSourceRetargetMerge, NodeSetAssignment, NodeSetValue,
    RelationshipOnCreatePropertyValue, RelationshipSetAssignment, RelationshipTargetNodeDelete,
};

use crate::expression::{
    property_filter_from_predicate, relationship_filter_from_properties_and_predicate,
};
use crate::predicate::property_filter_from_properties;

mod preflight;
pub use preflight::{execute_mutation_with_store, project_staged_mutation_return_rows};

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
                SetValue::Coalesce { .. } => Err(HawDBError::Semantic(
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
        | PhysicalPlan::HashJoinExec { .. }
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
        hawdb_plan::PhysicalPlanClass::Schema | hawdb_plan::PhysicalPlanClass::Mutation
    ))
}

pub fn node_set_assignment(assignment: &hawdb_plan::SetAssignment) -> NodeSetAssignment {
    NodeSetAssignment {
        property: assignment.property.clone(),
        value: node_set_value(&assignment.value),
    }
}

pub fn node_set_value(value: &SetValue) -> NodeSetValue {
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

pub fn relationship_on_create_property_value(
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

#[cfg(test)]
mod tests;
