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

use super::super::PhysicalPlan;
use crate::{plan_vector_search, OptimizerContext};
use hawdb_plan::{LogicalPlan, VectorCandidateSource, VectorSearchLogicalPlan};

pub(super) fn lower(
    logical: &LogicalPlan,
    optimizer_context: &OptimizerContext,
) -> Option<PhysicalPlan> {
    match logical {
        LogicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => Some(PhysicalPlan::ProjectGraph {
            name: name.clone(),
            node_labels: node_labels.clone(),
            rel_types: rel_types.clone(),
        }),
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
            node_visibility_predicate,
        } => Some(PhysicalPlan::GraphAlgorithm {
            algorithm: *algorithm,
            graph_name: graph_name.clone(),
            options: *options,
            score_column: score_column.clone(),
            node_visibility_predicate: node_visibility_predicate.clone(),
        }),
        LogicalPlan::VectorSeed {
            embedding_parameter,
            embedding_dimension,
            top_k,
            output_external_id,
        } => {
            let logical = VectorSearchLogicalPlan {
                embedding_dimension: *embedding_dimension,
                filter_fields: Vec::new(),
                residual_filter_fields: Vec::new(),
                initial_candidate_limit: *top_k,
                candidate_source: VectorCandidateSource::Scalar,
                candidate_limit: *top_k,
                top_k: *top_k,
            };
            let planned = plan_vector_search(&logical, optimizer_context).ok()?;
            Some(PhysicalPlan::VectorSeedScan {
                embedding_parameter: embedding_parameter.clone(),
                output_external_id: *output_external_id,
                metadata_filters: Default::default(),
                resource_profile: planned.properties.execution_resource_profile(),
                vector_plan: planned.plan,
            })
        }
        LogicalPlan::ThreadRepairStats {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => Some(PhysicalPlan::ThreadRepairStatsExec {
            label: label.clone(),
            identity_label: identity_label.clone(),
            identity_ref_property: identity_ref_property.clone(),
            thread_id_property: thread_id_property.clone(),
            message_rel_type: message_rel_type.clone(),
            message_label: message_label.clone(),
            memory_rel_type: memory_rel_type.clone(),
            memory_label: memory_label.clone(),
        }),
        _ => None,
    }
}
