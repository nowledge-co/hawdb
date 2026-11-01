// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Requested-heap accounting for the physical IR. Every variant and field is
//! matched explicitly so changes to retained IR require an accounting update.

use super::*;
use hawdb_core::{checked_retained_heap_sum, RetainedHeapBytes};

// plan-core stays independent of the value/core crate. Its public vector IR
// can be accounted here without reversing that dependency boundary.
fn resource_profile_heap_bytes(profile: &crate::VectorExecutionResourceProfile) -> Option<usize> {
    fn require_copy<T: Copy>(_: &T) {}
    require_copy(profile);
    let crate::VectorExecutionResourceProfile {
        priority: _,
        max_parallelism: _,
        max_working_memory_bytes: _,
    } = profile;
    Some(0)
}

fn vector_plan_heap_bytes(plan: &crate::VectorPhysicalPlan) -> Option<usize> {
    fn leaf<T: Copy>(_: &T) {}
    fn input_heap(input: &crate::VectorPhysicalPlan) -> Option<usize> {
        std::mem::size_of::<crate::VectorPhysicalPlan>().checked_add(vector_plan_heap_bytes(input)?)
    }
    use crate::VectorPhysicalPlan as V;
    match plan {
        V::Filter { fields } => fields.retained_heap_bytes(),
        V::VectorCandidateScan {
            source,
            embedding_dimension,
            candidate_limit,
            input,
        } => {
            leaf(source);
            leaf(embedding_dimension);
            leaf(candidate_limit);
            input_heap(input)
        }
        V::ResidualFilter {
            fields,
            initial_candidate_limit,
            input,
        } => {
            leaf(initial_candidate_limit);
            fields
                .retained_heap_bytes()?
                .checked_add(input_heap(input)?)
        }
        V::RawVectorRerank {
            embedding_dimension,
            input,
        } => {
            leaf(embedding_dimension);
            input_heap(input)
        }
        V::TopK { limit, input } => {
            leaf(limit);
            input_heap(input)
        }
    }
}

impl RetainedHeapBytes for PhysicalPlan {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::CreateNodeLabel { label } => {
                checked_retained_heap_sum([label.retained_heap_bytes()])
            }
            Self::CreateRelationshipType { rel_type } => {
                checked_retained_heap_sum([rel_type.retained_heap_bytes()])
            }
            Self::CreateNodeTable { name } => {
                checked_retained_heap_sum([name.retained_heap_bytes()])
            }
            Self::CreateRelationshipTable { name } => {
                checked_retained_heap_sum([name.retained_heap_bytes()])
            }
            Self::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => checked_retained_heap_sum([
                table_kind.retained_heap_bytes(),
                table.retained_heap_bytes(),
                property.retained_heap_bytes(),
                value_type.retained_heap_bytes(),
                nullable.retained_heap_bytes(),
            ]),
            Self::AlterTableState {
                table_kind,
                table,
                state,
            } => checked_retained_heap_sum([
                table_kind.retained_heap_bytes(),
                table.retained_heap_bytes(),
                state.retained_heap_bytes(),
            ]),
            Self::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => checked_retained_heap_sum([
                table_kind.retained_heap_bytes(),
                table.retained_heap_bytes(),
                property.retained_heap_bytes(),
                state.retained_heap_bytes(),
            ]),
            Self::CreateIndex { label, property } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
            ]),
            Self::CreateCompositeIndex { label, properties } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                properties.retained_heap_bytes(),
            ]),
            Self::CreateRangeIndex { label, property } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
            ]),
            Self::CreateFullTextIndex { label, property } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
            ]),
            Self::CreateUniqueConstraint { label, property } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
            ]),
            Self::CreateNodePropertyExistsConstraint { label, property } => {
                checked_retained_heap_sum([
                    label.retained_heap_bytes(),
                    property.retained_heap_bytes(),
                ])
            }
            Self::CreateRelationshipUniqueConstraint { rel_type, property } => {
                checked_retained_heap_sum([
                    rel_type.retained_heap_bytes(),
                    property.retained_heap_bytes(),
                ])
            }
            Self::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                checked_retained_heap_sum([
                    rel_type.retained_heap_bytes(),
                    property.retained_heap_bytes(),
                ])
            }
            Self::ProjectGraph {
                name,
                node_labels,
                rel_types,
                relationship_predicates,
            } => checked_retained_heap_sum([
                name.retained_heap_bytes(),
                node_labels.retained_heap_bytes(),
                rel_types.retained_heap_bytes(),
                relationship_predicates.retained_heap_bytes(),
            ]),
            Self::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
                return_node_identity,
                node_visibility_predicate,
            } => checked_retained_heap_sum([
                algorithm.retained_heap_bytes(),
                graph_name.retained_heap_bytes(),
                options.retained_heap_bytes(),
                score_column.retained_heap_bytes(),
                return_node_identity.retained_heap_bytes(),
                node_visibility_predicate.retained_heap_bytes(),
            ]),
            Self::GraphAlgorithmOutput(value0) => {
                checked_retained_heap_sum([value0.retained_heap_bytes()])
            }
            Self::GraphSeedScan {
                query_parameter,
                label,
                variable,
                score_column,
                top_k,
                node_visibility_predicate,
            } => checked_retained_heap_sum([
                query_parameter.retained_heap_bytes(),
                label.retained_heap_bytes(),
                variable.retained_heap_bytes(),
                score_column.retained_heap_bytes(),
                top_k.retained_heap_bytes(),
                node_visibility_predicate.retained_heap_bytes(),
            ]),
            Self::TextSeedScan {
                query_parameter,
                top_k,
                complete,
                output_external_id,
                metadata_filters,
                resource_profile,
            } => checked_retained_heap_sum([
                query_parameter.retained_heap_bytes(),
                top_k.retained_heap_bytes(),
                complete.retained_heap_bytes(),
                output_external_id.retained_heap_bytes(),
                metadata_filters.retained_heap_bytes(),
                resource_profile_heap_bytes(resource_profile),
            ]),
            Self::VectorSeedScan {
                embedding_parameter,
                output_external_id,
                metadata_filters,
                vector_plan,
                resource_profile,
            } => checked_retained_heap_sum([
                embedding_parameter.retained_heap_bytes(),
                output_external_id.retained_heap_bytes(),
                metadata_filters.retained_heap_bytes(),
                vector_plan_heap_bytes(vector_plan),
                resource_profile_heap_bytes(resource_profile),
            ]),
            Self::VectorIndexScan(value0) => {
                checked_retained_heap_sum([value0.retained_heap_bytes()])
            }
            Self::CreateNode { label, properties } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                properties.retained_heap_bytes(),
            ]),
            Self::UnwindMutation {
                rows,
                variable,
                operation,
            } => checked_retained_heap_sum([
                rows.retained_heap_bytes(),
                variable.retained_heap_bytes(),
                operation.retained_heap_bytes(),
            ]),
            Self::LoadCsvMutation { source, operation } => checked_retained_heap_sum([
                source.retained_heap_bytes(),
                operation.retained_heap_bytes(),
            ]),
            Self::LoadCsvExec { source } => source.retained_heap_bytes(),
            Self::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                match_properties.retained_heap_bytes(),
                on_create_properties.retained_heap_bytes(),
                on_match_assignments.retained_heap_bytes(),
                post_merge_assignments.retained_heap_bytes(),
            ]),
            Self::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
            ]),
            Self::MergeMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_match_properties,
                on_create_properties,
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_match_properties.retained_heap_bytes(),
                on_create_properties.retained_heap_bytes(),
            ]),
            Self::MergeRelationshipFromMatchedRelationship {
                source_label,
                source_properties,
                old_rel_type,
                old_rel_properties,
                target_label,
                target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                old_rel_type.retained_heap_bytes(),
                old_rel_properties.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                new_rel_type.retained_heap_bytes(),
                new_rel_match_properties.retained_heap_bytes(),
                on_create_properties.retained_heap_bytes(),
            ]),
            Self::MergeRelationshipToMatchedTarget {
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
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                old_rel_type.retained_heap_bytes(),
                old_rel_properties.retained_heap_bytes(),
                old_target_label.retained_heap_bytes(),
                old_target_properties.retained_heap_bytes(),
                new_target_label.retained_heap_bytes(),
                new_target_properties.retained_heap_bytes(),
                new_rel_type.retained_heap_bytes(),
                new_rel_match_properties.retained_heap_bytes(),
                on_create_properties.retained_heap_bytes(),
            ]),
            Self::MergeRelationshipFromMatchedTarget {
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
            } => checked_retained_heap_sum([
                old_source_label.retained_heap_bytes(),
                old_source_properties.retained_heap_bytes(),
                old_rel_type.retained_heap_bytes(),
                old_rel_properties.retained_heap_bytes(),
                old_target_label.retained_heap_bytes(),
                old_target_properties.retained_heap_bytes(),
                new_source_label.retained_heap_bytes(),
                new_source_properties.retained_heap_bytes(),
                new_rel_type.retained_heap_bytes(),
                new_rel_match_properties.retained_heap_bytes(),
                on_create_properties.retained_heap_bytes(),
            ]),
            Self::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
            ]),
            Self::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                property.retained_heap_bytes(),
                value.retained_heap_bytes(),
            ]),
            Self::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                assignments.retained_heap_bytes(),
            ]),
            Self::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                assignments.retained_heap_bytes(),
                returns.retained_heap_bytes(),
            ]),
            Self::SetRelationshipProperty {
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
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                rel_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                rel_predicate.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                property.retained_heap_bytes(),
                value.retained_heap_bytes(),
            ]),
            Self::SetRelationshipProperties {
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
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                rel_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                rel_predicate.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                assignments.retained_heap_bytes(),
            ]),
            Self::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                detach.retained_heap_bytes(),
            ]),
            Self::DeleteRelationship {
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
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                rel_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                rel_predicate.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
            ]),
            Self::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                source_predicate.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                detach.retained_heap_bytes(),
            ]),
            Self::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => checked_retained_heap_sum([
                source_label.retained_heap_bytes(),
                source_properties.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
            ]),
            Self::GraphMatchExec { program, input } => checked_retained_heap_sum([
                program.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::EmptyExec => checked_retained_heap_sum([]),
            Self::UnwindExec { rows, variable } => checked_retained_heap_sum([
                rows.retained_heap_bytes(),
                variable.retained_heap_bytes(),
            ]),
            Self::UnwindRowsExec {
                expression,
                variable,
                input,
            } => checked_retained_heap_sum([
                expression.retained_heap_bytes(),
                variable.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::SeqNodeScan { variable, label } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
            ]),
            Self::NodeProjectionScanExec {
                variable,
                label,
                access,
                required_properties,
                predicate,
                items,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                access.retained_heap_bytes(),
                required_properties.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
                items.retained_heap_bytes(),
            ]),
            Self::SourceSegmentScan {
                variable,
                predicate,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                predicate.retained_heap_bytes(),
            ]),
            Self::NodeCartesianProductExec { left, right } => {
                checked_retained_heap_sum([left.retained_heap_bytes(), right.retained_heap_bytes()])
            }
            Self::HashJoinExec {
                left_key,
                right_key,
                left,
                right,
            } => checked_retained_heap_sum([
                left_key.retained_heap_bytes(),
                right_key.retained_heap_bytes(),
                left.retained_heap_bytes(),
                right.retained_heap_bytes(),
            ]),
            Self::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                node_visibility_predicate,
                input,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
                column.retained_heap_bytes(),
                optional.retained_heap_bytes(),
                node_visibility_predicate.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
                value.retained_heap_bytes(),
            ]),
            Self::IndexNodeMultiSeek {
                variable,
                label,
                property,
                values,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
                values.retained_heap_bytes(),
            ]),
            Self::IndexNodeUnionSeek {
                variable,
                label,
                branches,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                branches.retained_heap_bytes(),
            ]),
            Self::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                predicates.retained_heap_bytes(),
            ]),
            Self::IndexNodeCompositeRangeSeek {
                variable,
                label,
                seek,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                seek.retained_heap_bytes(),
            ]),
            Self::IndexNodeRangeSeek {
                variable,
                label,
                property,
                lower,
                upper,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
                lower.retained_heap_bytes(),
                upper.retained_heap_bytes(),
            ]),
            Self::IndexNodeTextSeek {
                variable,
                label,
                property,
                query,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                property.retained_heap_bytes(),
                query.retained_heap_bytes(),
            ]),
            Self::AdjacencyExpandExec {
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
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                rel_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                direction.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                min_hops.retained_heap_bytes(),
                max_hops.retained_heap_bytes(),
                optional.retained_heap_bytes(),
                graph_budget.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::AdjacencyExistsExec {
                source_variable,
                rel_type,
                direction,
                target_variable,
                input,
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                direction.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::OptionalDegreeExec {
                source_variable,
                rel_type,
                rel_properties,
                direction,
                target_label,
                target_properties,
                alias,
                input,
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                rel_properties.retained_heap_bytes(),
                direction.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_properties.retained_heap_bytes(),
                alias.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::OptionalRelationshipCountSumExec {
                variable,
                label,
                properties,
                legs,
                output,
            } => checked_retained_heap_sum([
                variable.retained_heap_bytes(),
                label.retained_heap_bytes(),
                properties.retained_heap_bytes(),
                legs.retained_heap_bytes(),
                output.retained_heap_bytes(),
            ]),
            Self::NodeCountExec { label, output } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                output.retained_heap_bytes(),
            ]),
            Self::RelationshipCountExec { rel_type, output } => checked_retained_heap_sum([
                rel_type.retained_heap_bytes(),
                output.retained_heap_bytes(),
            ]),
            Self::ThreadRepairStatsExec {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            } => checked_retained_heap_sum([
                label.retained_heap_bytes(),
                identity_label.retained_heap_bytes(),
                identity_ref_property.retained_heap_bytes(),
                thread_id_property.retained_heap_bytes(),
                message_rel_type.retained_heap_bytes(),
                message_label.retained_heap_bytes(),
                memory_rel_type.retained_heap_bytes(),
                memory_label.retained_heap_bytes(),
            ]),
            Self::ShortestPathExec {
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
            } => checked_retained_heap_sum([
                source_variable.retained_heap_bytes(),
                source_label.retained_heap_bytes(),
                source_id.retained_heap_bytes(),
                source_visibility_predicate.retained_heap_bytes(),
                rel_type.retained_heap_bytes(),
                direction.retained_heap_bytes(),
                target_variable.retained_heap_bytes(),
                target_label.retained_heap_bytes(),
                target_id.retained_heap_bytes(),
                target_visibility_predicate.retained_heap_bytes(),
                min_hops.retained_heap_bytes(),
                max_hops.retained_heap_bytes(),
                returns.retained_heap_bytes(),
            ]),
            Self::FilterExec { predicate, input } => checked_retained_heap_sum([
                predicate.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::ProjectExec { items, input } => checked_retained_heap_sum([
                items.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::AggregateExec {
                group_keys,
                items,
                input,
            } => checked_retained_heap_sum([
                group_keys.retained_heap_bytes(),
                items.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::DistinctExec { input } => {
                checked_retained_heap_sum([input.retained_heap_bytes()])
            }
            Self::SortExec { items, input } => checked_retained_heap_sum([
                items.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::TopNExec {
                items,
                offset,
                limit,
                input,
            } => checked_retained_heap_sum([
                items.retained_heap_bytes(),
                offset.retained_heap_bytes(),
                limit.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::LimitExec {
                offset,
                limit,
                input,
            } => checked_retained_heap_sum([
                offset.retained_heap_bytes(),
                limit.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::ScoringRerankExec {
                score_column,
                spec,
                limit,
                input,
            } => checked_retained_heap_sum([
                score_column.retained_heap_bytes(),
                spec.retained_heap_bytes(),
                limit.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::ScoringProgramExec {
                score_column,
                seed_graph_input,
                program,
                reference_time_millis,
                limit,
                input,
            } => checked_retained_heap_sum([
                score_column.retained_heap_bytes(),
                seed_graph_input.retained_heap_bytes(),
                program.retained_heap_bytes(),
                reference_time_millis.retained_heap_bytes(),
                limit.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
            Self::HostScoringExec {
                scoring,
                reference_time_millis,
                input,
            } => checked_retained_heap_sum([
                scoring.retained_heap_bytes(),
                reference_time_millis.retained_heap_bytes(),
                input.retained_heap_bytes(),
            ]),
        }
    }
}
