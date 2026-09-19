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
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => {
            let rows = super::mutation::execute_mutation_with_limits(
                plan,
                catalog,
                store,
                MutationLimits::default(),
                context.task_context,
            )?;
            Ok(rows.into_iter().map(Binding::values).collect())
        }
    }
}
