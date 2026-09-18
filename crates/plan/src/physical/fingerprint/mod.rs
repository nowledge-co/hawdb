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

use super::{NodeProjectionAccess, PhysicalPlan, PlanChildren};
use crate::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, GraphAlgorithmKind, Predicate,
    Projection, ProjectionExpression, RelationshipCountFilter, RelationshipCountLeg,
    RelationshipOnCreateValue, SetAssignment, SetNodePropertiesReturnMode, SetValue, SortDirection,
    SortItem, SortKey,
};
use hawdb_core::Value;
use hawdb_cypher::RelationshipDirection;
use std::collections::BTreeMap;

mod common;
mod predicate;
mod projection;

use common::*;
use predicate::*;
pub use projection::write_projection_expression;
use projection::*;

impl PhysicalPlan {
    /// Returns the physical operator-tree shape without identifiers, literals,
    /// runtime parameters, estimates, or other per-instance payloads.
    pub fn fingerprint(&self) -> String {
        let mut output = String::new();
        self.write_shape_fingerprint(&mut output);
        output
    }

    /// Returns the deterministic full-plan serialization used for internal
    /// tie-breaking and diagnostics that need bound plan details.
    pub fn instance_fingerprint(&self) -> String {
        let mut output = String::new();
        self.write_instance_fingerprint(&mut output);
        output
    }

    fn write_shape_fingerprint(&self, output: &mut String) {
        output.push_str(self.kind().as_str());
        if let PhysicalPlan::NodeProjectionScanExec { access, .. } = self {
            output.push('[');
            output.push_str(access.physical_operator_name());
            output.push(']');
        }
        match self.children() {
            PlanChildren::None => {}
            PlanChildren::Unary(input) => {
                output.push('(');
                input.write_shape_fingerprint(output);
                output.push(')');
            }
            PlanChildren::Binary(left, right) => {
                output.push('(');
                left.write_shape_fingerprint(output);
                output.push(',');
                right.write_shape_fingerprint(output);
                output.push(')');
            }
        }
    }

    fn write_instance_fingerprint(&self, output: &mut String) {
        match self {
            PhysicalPlan::CreateNodeLabel { label } => {
                output.push_str("CreateNodeLabel(");
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipType { rel_type } => {
                output.push_str("CreateRelationshipType(");
                write_identifier(output, rel_type);
                output.push(')');
            }
            PhysicalPlan::CreateNodeTable { name } => {
                output.push_str("CreateNodeTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipTable { name } => {
                output.push_str("CreateRelationshipTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                output.push_str("CreateProperty(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push_str(value_type.fingerprint_suffix());
                output.push_str(if *nullable { ":nullable" } else { ":not_null" });
                output.push(')');
            }
            PhysicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                output.push_str("AlterTableState(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push(':');
                output.push_str(state.as_str());
                output.push(')');
            }
            PhysicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                output.push_str("AlterPropertyState(");
                output.push_str(table_kind.as_str());
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push(':');
                output.push_str(state.as_str());
                output.push(')');
            }
            PhysicalPlan::CreateIndex { label, property } => {
                output.push_str("CreateIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateCompositeIndex { label, properties } => {
                output.push_str("CreateCompositeIndex(");
                write_identifier(output, label);
                output.push('(');
                write_identifier_list(output, properties);
                output.push(')');
            }
            PhysicalPlan::CreateRangeIndex { label, property } => {
                output.push_str("CreateRangeIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateFullTextIndex { label, property } => {
                output.push_str("CreateFullTextIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateUniqueConstraint { label, property } => {
                output.push_str("CreateUniqueConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                output.push_str("CreateNodePropertyExistsConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipUniqueConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipPropertyExistsConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                output.push_str("ProjectGraph(");
                write_identifier(output, name);
                output.push_str(":labels=");
                write_identifier_list(output, node_labels);
                output.push_str(":rels=");
                write_identifier_list(output, rel_types);
                output.push(')');
            }
            PhysicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
                node_visibility_predicate,
            } => {
                output.push_str("GraphAlgorithm(");
                output.push_str(match algorithm {
                    GraphAlgorithmKind::PageRank => "page_rank",
                    GraphAlgorithmKind::Louvain => "louvain",
                });
                output.push(':');
                write_identifier(output, graph_name);
                output.push_str(":damping=");
                if let Some(damping) = options.damping {
                    output.push_str(&damping.to_bits().to_string());
                }
                output.push_str(":iterations=");
                if let Some(iterations) = options.max_iterations {
                    output.push_str(&iterations.to_string());
                }
                output.push_str(":score=");
                write_identifier(output, score_column);
                output.push_str(":node_visibility=");
                write_optional_predicate(output, node_visibility_predicate.as_ref());
                output.push(')');
            }
            PhysicalPlan::VectorSeedScan {
                embedding_parameter,
                output_external_id,
                metadata_filters,
                vector_plan,
                resource_profile,
            } => {
                output.push_str("VectorSeedScan(");
                write_identifier(output, embedding_parameter);
                output.push(':');
                output.push_str(if *output_external_id {
                    "external"
                } else {
                    "public"
                });
                output.push(':');
                write_properties(
                    output,
                    &metadata_filters
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                        .collect(),
                );
                output.push(':');
                output.push_str(&vector_plan.fingerprint());
                output.push(':');
                output.push_str(&resource_profile.priority.to_string());
                output.push(':');
                output.push_str(&resource_profile.max_parallelism.to_string());
                output.push(':');
                match resource_profile.max_working_memory_bytes {
                    Some(bytes) => output.push_str(&bytes.to_string()),
                    None => output.push_str("unbounded"),
                }
                output.push(')');
            }
            PhysicalPlan::CreateNode { label, properties } => {
                output.push_str("CreateNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push(')');
            }
            PhysicalPlan::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => {
                output.push_str("MergeNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(',');
                write_set_assignments(output, on_match_assignments);
                output.push(',');
                write_set_assignments(output, post_merge_assignments);
                output.push(')');
            }
            PhysicalPlan::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("MergeRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
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
                output.push_str("MergeMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
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
                output.push_str("MergeRelationshipFromMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(")=>[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_relationship_on_create_properties(output, on_create_properties);
                output.push(']');
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
                output.push_str("MergeRelationshipToMatchedTarget(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_target=(");
                write_identifier(output, new_target_label);
                output.push(',');
                write_properties(output, new_target_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
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
                output.push_str("MergeRelationshipFromMatchedTarget(old_source=");
                write_identifier(output, old_source_label);
                output.push(',');
                write_properties(output, old_source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_source=(");
                write_identifier(output, new_source_label);
                output.push(',');
                write_properties(output, new_source_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
            }
            PhysicalPlan::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => {
                output.push_str("SetNodeProperty(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_set_value(output, value);
                output.push(')');
            }
            PhysicalPlan::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => {
                output.push_str("CreateMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => {
                output.push_str("SetNodeProperties(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push(')');
            }
            PhysicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => {
                output.push_str("SetNodePropertiesReturn(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push_str(",returns=");
                write_set_return_mode(output, returns);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperty {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                property,
                value,
            } => {
                output.push_str("SetRelationshipProperty(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperties {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                assignments,
            } => {
                output.push_str("SetRelationshipProperties(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push_str(",assignments=");
                for assignment in assignments {
                    write_identifier(output, &assignment.property);
                    output.push('=');
                    write_value(output, &assignment.value);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => {
                output.push_str("DeleteNode(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::DeleteRelationship {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
            } => {
                output.push_str("DeleteRelationship(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(')');
            }
            PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => {
                output.push_str("DeleteRelationshipTargetNodes(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push(',');
                write_optional_predicate(output, source_predicate.as_ref());
                output.push_str("-[:");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("CreateRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::EmptyExec => output.push_str("EmptyExec"),
            PhysicalPlan::SeqNodeScan { variable, label } => {
                output.push_str("SeqNodeScan(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::NodeProjectionScanExec {
                variable,
                label,
                access,
                required_properties,
                predicate,
                items,
            } => {
                output.push_str("NodeProjectionScanExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push_str(",access=");
                write_node_projection_access(output, access);
                output.push_str(",properties=");
                for property in required_properties {
                    write_identifier(output, property);
                    output.push(',');
                }
                output.push_str("predicate=");
                write_optional_predicate(output, predicate.as_ref());
                output.push_str(",columns=");
                write_projection_list(output, items);
                output.push(')');
            }
            PhysicalPlan::SourceSegmentScan {
                variable,
                predicate,
            } => {
                output.push_str("SourceSegmentScan(");
                write_identifier(output, variable);
                output.push_str(",predicate=");
                write_predicate(output, predicate);
                output.push(')');
            }
            PhysicalPlan::HashJoinExec {
                left_key,
                right_key,
                left,
                right,
            } => {
                output.push_str("HashJoinExec(");
                for key in [left_key, right_key] {
                    write_identifier(output, &key.variable);
                    write_identifier(output, &key.property);
                    output.push(',');
                }
                left.write_instance_fingerprint(output);
                output.push(',');
                right.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::NodeCartesianProductExec { left, right } => {
                output.push_str("NodeCartesianProductExec(");
                left.write_instance_fingerprint(output);
                output.push(',');
                right.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                input,
            } => {
                output.push_str("NodeColumnLookupExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_identifier(output, column);
                output.push(',');
                output.push_str(if *optional { "optional" } else { "required" });
                output.push(',');
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => {
                output.push_str("IndexNodeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::IndexNodeMultiSeek {
                variable,
                label,
                property,
                values,
            } => {
                output.push_str("IndexNodeMultiSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" IN [");
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_value(output, value);
                }
                output.push_str("])");
            }
            PhysicalPlan::IndexNodeUnionSeek {
                variable,
                label,
                branches,
            } => {
                output.push_str("IndexNodeUnionSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('(');
                for (branch_index, branch) in branches.iter().enumerate() {
                    if branch_index > 0 {
                        output.push('|');
                    }
                    write_identifier(output, &branch.property);
                    output.push_str(" IN [");
                    for (value_index, value) in branch.values.iter().enumerate() {
                        if value_index > 0 {
                            output.push(',');
                        }
                        write_value(output, value);
                    }
                    output.push(']');
                }
                output.push_str("))");
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => {
                output.push_str("IndexNodeCompositeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('(');
                for (index, (property, value)) in predicates.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_identifier(output, property);
                    output.push('=');
                    write_value(output, value);
                }
                output.push(')');
            }
            PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable,
                label,
                seek,
            } => {
                output.push_str("IndexNodeCompositeRangeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push_str("(index=");
                write_identifier_list(output, &seek.index_properties);
                output.push_str(",prefix=");
                for (index, (property, value)) in seek.equality_prefix.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_identifier(output, property);
                    output.push('=');
                    write_value(output, value);
                }
                output.push_str(",range=");
                write_identifier(output, &seek.range_property);
                output.push_str(",lower=");
                write_optional_range_bound(output, seek.lower.as_ref());
                output.push_str(",upper=");
                write_optional_range_bound(output, seek.upper.as_ref());
                output.push_str("))");
            }
            PhysicalPlan::IndexNodeRangeSeek {
                variable,
                label,
                property,
                lower,
                upper,
            } => {
                output.push_str("IndexNodeRangeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(",lower=");
                write_optional_range_bound(output, lower.as_ref());
                output.push_str(",upper=");
                write_optional_range_bound(output, upper.as_ref());
                output.push(')');
            }
            PhysicalPlan::IndexNodeTextSeek {
                variable,
                label,
                property,
                query,
            } => {
                output.push_str("IndexNodeTextSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" contains ");
                write_identifier(output, query);
                output.push(')');
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                optional,
                graph_budget,
                input,
            } => {
                output.push_str("AdjacencyExpandExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                if let Some(rel_variable) = rel_variable {
                    write_identifier(output, rel_variable);
                    output.push(':');
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",optional=");
                output.push_str(if *optional { "true" } else { "false" });
                if let Some(graph_budget) = graph_budget {
                    output.push_str(",graph_budget=");
                    output.push_str(&graph_budget.candidate_limit.to_string());
                    output.push(':');
                    output.push_str(&graph_budget.payload_byte_limit.to_string());
                }
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::AdjacencyExistsExec {
                source_variable,
                rel_type,
                direction,
                target_variable,
                input,
            } => {
                output.push_str("AdjacencyExistsExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(':');
                output.push_str(match direction {
                    RelationshipDirection::Outgoing => "out",
                    RelationshipDirection::Incoming => "in",
                    RelationshipDirection::Undirected => "both",
                });
                output.push(':');
                write_identifier(output, target_variable);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::OptionalDegreeExec {
                source_variable,
                rel_type,
                rel_properties,
                direction,
                target_label,
                target_properties,
                alias,
                input,
            } => {
                output.push_str("OptionalDegreeExec(");
                write_identifier(output, source_variable);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",alias=");
                write_identifier(output, alias);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable,
                label,
                properties,
                legs,
                output: projection,
            } => {
                output.push_str("OptionalRelationshipCountSumExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push_str(",legs=[");
                for (index, leg) in legs.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_relationship_count_leg(output, leg);
                }
                output.push_str("],output=");
                write_identifier(output, projection);
                output.push(')');
            }
            PhysicalPlan::NodeCountExec {
                label,
                output: projection,
            } => {
                output.push_str("NodeCountExec(");
                write_identifier(output, label);
                output.push_str(",output=");
                write_identifier(output, projection);
                output.push(')');
            }
            PhysicalPlan::RelationshipCountExec {
                rel_type,
                output: projection,
            } => {
                output.push_str("RelationshipCountExec(");
                write_identifier(output, rel_type);
                output.push_str(",output=");
                write_identifier(output, projection);
                output.push(')');
            }
            PhysicalPlan::ThreadRepairStatsExec {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            } => {
                output.push_str("ThreadRepairStatsExec(");
                write_identifier(output, label);
                output.push_str(",identity=");
                write_identifier(output, identity_label);
                output.push('.');
                write_identifier(output, identity_ref_property);
                output.push('=');
                write_identifier(output, thread_id_property);
                output.push_str(",messages=");
                write_identifier(output, message_rel_type);
                output.push(':');
                write_identifier(output, message_label);
                output.push_str(",memories=");
                write_identifier(output, memory_rel_type);
                output.push(':');
                write_identifier(output, memory_label);
                output.push(')');
            }
            PhysicalPlan::ShortestPathExec {
                source_variable,
                source_label,
                source_id,
                source_visibility_predicate,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_id,
                target_visibility_predicate,
                min_hops,
                max_hops,
                returns,
            } => {
                output.push_str("ShortestPathExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str(",source_id=");
                write_value(output, source_id);
                output.push_str(",source_visibility=");
                write_optional_predicate(output, source_visibility_predicate.as_ref());
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",target_id=");
                write_value(output, target_id);
                output.push_str(",target_visibility=");
                write_optional_predicate(output, target_visibility_predicate.as_ref());
                output.push_str(",returns=");
                for item in returns {
                    write_identifier(output, &item.name);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::FilterExec { predicate, input } => {
                output.push_str("FilterExec(");
                write_predicate(output, predicate);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::ProjectExec { items, input } => {
                output.push_str("ProjectExec(");
                write_projection_list(output, items);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                input,
            } => {
                output.push_str("AggregateExec(groups=");
                write_projection_list(output, group_keys);
                output.push_str(",aggs=");
                write_aggregation_list(output, items);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::DistinctExec { input } => {
                output.push_str("DistinctExec(input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::SortExec { items, input } => {
                output.push_str("SortExec(");
                write_sort_list(output, items);
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::TopNExec {
                items,
                offset,
                limit,
                input,
            } => {
                output.push_str("TopNExec(");
                write_sort_list(output, items);
                output.push_str(",offset=");
                output.push_str(&offset.to_string());
                output.push_str(",limit=");
                output.push_str(&limit.to_string());
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::LimitExec {
                offset,
                limit,
                input,
            } => {
                output.push_str("LimitExec(offset=");
                output.push_str(&offset.to_string());
                output.push_str(",limit=");
                match limit {
                    Some(limit) => output.push_str(&limit.to_string()),
                    None => output.push_str("none"),
                }
                output.push_str(",input=");
                input.write_instance_fingerprint(output);
                output.push(')');
            }
        }
    }
}

fn write_node_projection_access(output: &mut String, access: &NodeProjectionAccess) {
    match access {
        NodeProjectionAccess::LabelScan => output.push_str("label_scan"),
        NodeProjectionAccess::PropertyValues { property, values } => {
            output.push_str("property_values(");
            write_identifier(output, property);
            output.push('=');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push(')');
        }
        NodeProjectionAccess::PropertyUnion { branches } => {
            output.push_str("property_union(");
            for (branch_index, branch) in branches.iter().enumerate() {
                if branch_index > 0 {
                    output.push('|');
                }
                write_identifier(output, &branch.property);
                output.push('=');
                for (value_index, value) in branch.values.iter().enumerate() {
                    if value_index > 0 {
                        output.push(',');
                    }
                    write_value(output, value);
                }
            }
            output.push(')');
        }
        NodeProjectionAccess::CompositeEquality { predicates } => {
            output.push_str("composite_equality(");
            for (index, (property, value)) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
            }
            output.push(')');
        }
        NodeProjectionAccess::CompositeRange { seek } => {
            output.push_str("composite_range(index=");
            write_identifier_list(output, &seek.index_properties);
            output.push_str(",prefix=");
            for (index, (property, value)) in seek.equality_prefix.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
            }
            output.push_str(",range=");
            write_identifier(output, &seek.range_property);
            output.push(',');
            write_optional_range_bound(output, seek.lower.as_ref());
            output.push(',');
            write_optional_range_bound(output, seek.upper.as_ref());
            output.push(')');
        }
        NodeProjectionAccess::PropertyRange {
            property,
            lower,
            upper,
        } => {
            output.push_str("property_range(");
            write_identifier(output, property);
            output.push(',');
            write_optional_range_bound(output, lower.as_ref());
            output.push(',');
            write_optional_range_bound(output, upper.as_ref());
            output.push(')');
        }
        NodeProjectionAccess::FullText { property, query } => {
            output.push_str("full_text(");
            write_identifier(output, property);
            output.push('=');
            write_identifier(output, query);
            output.push(')');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_seek(value: i64) -> PhysicalPlan {
        PhysicalPlan::IndexNodeSeek {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "id".to_string(),
            value: Value::Int(value),
        }
    }

    #[test]
    fn plan_fingerprint_represents_operator_shape_not_bound_values() {
        let first = index_seek(1);
        let second = index_seek(2);

        assert_eq!(first.fingerprint(), "IndexNodeSeek");
        assert_eq!(first.fingerprint(), second.fingerprint());
        assert_ne!(first.instance_fingerprint(), second.instance_fingerprint());
    }

    #[test]
    fn plan_fingerprint_preserves_tree_topology() {
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(index_seek(1)),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "e".to_string(),
                label: "Entity".to_string(),
            }),
        };

        assert_eq!(
            plan.fingerprint(),
            "NodeCartesianProductExec(IndexNodeSeek,SeqNodeScan)"
        );
    }

    #[test]
    fn projected_access_is_part_of_the_instance_fingerprint() {
        let projected = |access| PhysicalPlan::NodeProjectionScanExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            access,
            required_properties: vec!["title".to_string()],
            predicate: None,
            items: Vec::new(),
        };
        let first = projected(NodeProjectionAccess::PropertyValues {
            property: "stable_id".to_string(),
            values: vec![Value::String("memory:1".to_string())],
        });
        let second = projected(NodeProjectionAccess::PropertyValues {
            property: "stable_id".to_string(),
            values: vec![Value::String("memory:2".to_string())],
        });
        let label_scan = projected(NodeProjectionAccess::LabelScan);

        assert_eq!(first.fingerprint(), "NodeProjectionScanExec[IndexNodeSeek]");
        assert_eq!(first.fingerprint(), second.fingerprint());
        assert_eq!(
            label_scan.fingerprint(),
            "NodeProjectionScanExec[SeqNodeScan]"
        );
        assert_ne!(first.fingerprint(), label_scan.fingerprint());
        assert_ne!(first.instance_fingerprint(), second.instance_fingerprint());
        assert_ne!(
            first.instance_fingerprint(),
            label_scan.instance_fingerprint()
        );
    }

    #[test]
    fn composite_range_instance_fingerprint_identifies_the_selected_index() {
        let projected = |index_properties| PhysicalPlan::NodeProjectionScanExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            access: NodeProjectionAccess::CompositeRange {
                seek: super::super::CompositeRangeSeek {
                    index_properties,
                    equality_prefix: vec![(
                        "space_id".to_string(),
                        Value::String("space:1".to_string()),
                    )],
                    range_property: "created_at".to_string(),
                    lower: Some((Value::Int(10), true)),
                    upper: None,
                },
            },
            required_properties: vec!["created_at".to_string()],
            predicate: None,
            items: Vec::new(),
        };
        let narrow = projected(vec!["space_id".to_string(), "created_at".to_string()]);
        let covering = projected(vec![
            "space_id".to_string(),
            "created_at".to_string(),
            "stable_id".to_string(),
        ]);

        assert_eq!(narrow.fingerprint(), covering.fingerprint());
        assert_ne!(
            narrow.instance_fingerprint(),
            covering.instance_fingerprint()
        );
    }
}
