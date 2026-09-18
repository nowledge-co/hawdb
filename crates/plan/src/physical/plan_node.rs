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

use super::{PhysicalPlan, PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren};

pub type PhysicalPlanChildren<'a> = PlanChildren<'a, PhysicalPlan>;

impl PhysicalPlan {
    pub fn kind(&self) -> PhysicalPlanKind {
        match self {
            PhysicalPlan::CreateNodeLabel { .. } => PhysicalPlanKind::CreateNodeLabel,
            PhysicalPlan::CreateRelationshipType { .. } => PhysicalPlanKind::CreateRelationshipType,
            PhysicalPlan::CreateNodeTable { .. } => PhysicalPlanKind::CreateNodeTable,
            PhysicalPlan::CreateRelationshipTable { .. } => {
                PhysicalPlanKind::CreateRelationshipTable
            }
            PhysicalPlan::CreateProperty { .. } => PhysicalPlanKind::CreateProperty,
            PhysicalPlan::AlterTableState { .. } => PhysicalPlanKind::AlterTableState,
            PhysicalPlan::AlterPropertyState { .. } => PhysicalPlanKind::AlterPropertyState,
            PhysicalPlan::CreateIndex { .. } => PhysicalPlanKind::CreateIndex,
            PhysicalPlan::CreateCompositeIndex { .. } => PhysicalPlanKind::CreateCompositeIndex,
            PhysicalPlan::CreateRangeIndex { .. } => PhysicalPlanKind::CreateRangeIndex,
            PhysicalPlan::CreateFullTextIndex { .. } => PhysicalPlanKind::CreateFullTextIndex,
            PhysicalPlan::CreateUniqueConstraint { .. } => PhysicalPlanKind::CreateUniqueConstraint,
            PhysicalPlan::CreateNodePropertyExistsConstraint { .. } => {
                PhysicalPlanKind::CreateNodePropertyExistsConstraint
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { .. } => {
                PhysicalPlanKind::CreateRelationshipUniqueConstraint
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. } => {
                PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint
            }
            PhysicalPlan::ProjectGraph { .. } => PhysicalPlanKind::ProjectGraph,
            PhysicalPlan::GraphAlgorithm { .. } => PhysicalPlanKind::GraphAlgorithm,
            PhysicalPlan::VectorSeedScan { .. } => PhysicalPlanKind::VectorSeedScan,
            PhysicalPlan::CreateNode { .. } => PhysicalPlanKind::CreateNode,
            PhysicalPlan::MergeNode { .. } => PhysicalPlanKind::MergeNode,
            PhysicalPlan::MergeRelationship { .. } => PhysicalPlanKind::MergeRelationship,
            PhysicalPlan::MergeMatchedRelationship { .. } => {
                PhysicalPlanKind::MergeMatchedRelationship
            }
            PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. } => {
                PhysicalPlanKind::MergeRelationshipFromMatchedRelationship
            }
            PhysicalPlan::MergeRelationshipToMatchedTarget { .. } => {
                PhysicalPlanKind::MergeRelationshipToMatchedTarget
            }
            PhysicalPlan::MergeRelationshipFromMatchedTarget { .. } => {
                PhysicalPlanKind::MergeRelationshipFromMatchedTarget
            }
            PhysicalPlan::CreateMatchedRelationship { .. } => {
                PhysicalPlanKind::CreateMatchedRelationship
            }
            PhysicalPlan::SetNodeProperty { .. } => PhysicalPlanKind::SetNodeProperty,
            PhysicalPlan::SetNodeProperties { .. } => PhysicalPlanKind::SetNodeProperties,
            PhysicalPlan::SetNodePropertiesReturn { .. } => {
                PhysicalPlanKind::SetNodePropertiesReturn
            }
            PhysicalPlan::SetRelationshipProperty { .. } => {
                PhysicalPlanKind::SetRelationshipProperty
            }
            PhysicalPlan::SetRelationshipProperties { .. } => {
                PhysicalPlanKind::SetRelationshipProperties
            }
            PhysicalPlan::DeleteNode { .. } => PhysicalPlanKind::DeleteNode,
            PhysicalPlan::DeleteRelationship { .. } => PhysicalPlanKind::DeleteRelationship,
            PhysicalPlan::DeleteRelationshipTargetNodes { .. } => {
                PhysicalPlanKind::DeleteRelationshipTargetNodes
            }
            PhysicalPlan::CreateRelationship { .. } => PhysicalPlanKind::CreateRelationship,
            PhysicalPlan::EmptyExec => PhysicalPlanKind::EmptyExec,
            PhysicalPlan::SeqNodeScan { .. } => PhysicalPlanKind::SeqNodeScan,
            PhysicalPlan::NodeProjectionScanExec { .. } => PhysicalPlanKind::NodeProjectionScanExec,
            PhysicalPlan::SourceSegmentScan { .. } => PhysicalPlanKind::SourceSegmentScan,
            PhysicalPlan::HashJoinExec { .. } => PhysicalPlanKind::HashJoinExec,
            PhysicalPlan::NodeCartesianProductExec { .. } => {
                PhysicalPlanKind::NodeCartesianProductExec
            }
            PhysicalPlan::NodeColumnLookupExec { .. } => PhysicalPlanKind::NodeColumnLookupExec,
            PhysicalPlan::IndexNodeSeek { .. } => PhysicalPlanKind::IndexNodeSeek,
            PhysicalPlan::IndexNodeMultiSeek { .. } => PhysicalPlanKind::IndexNodeMultiSeek,
            PhysicalPlan::IndexNodeUnionSeek { .. } => PhysicalPlanKind::IndexNodeUnionSeek,
            PhysicalPlan::IndexNodeCompositeSeek { .. } => PhysicalPlanKind::IndexNodeCompositeSeek,
            PhysicalPlan::IndexNodeCompositeRangeSeek { .. } => {
                PhysicalPlanKind::IndexNodeCompositeRangeSeek
            }
            PhysicalPlan::IndexNodeRangeSeek { .. } => PhysicalPlanKind::IndexNodeRangeSeek,
            PhysicalPlan::IndexNodeTextSeek { .. } => PhysicalPlanKind::IndexNodeTextSeek,
            PhysicalPlan::AdjacencyExpandExec { .. } => PhysicalPlanKind::AdjacencyExpandExec,
            PhysicalPlan::AdjacencyExistsExec { .. } => PhysicalPlanKind::AdjacencyExistsExec,
            PhysicalPlan::OptionalDegreeExec { .. } => PhysicalPlanKind::OptionalDegreeExec,
            PhysicalPlan::OptionalRelationshipCountSumExec { .. } => {
                PhysicalPlanKind::OptionalRelationshipCountSumExec
            }
            PhysicalPlan::NodeCountExec { .. } => PhysicalPlanKind::NodeCountExec,
            PhysicalPlan::RelationshipCountExec { .. } => PhysicalPlanKind::RelationshipCountExec,
            PhysicalPlan::ThreadRepairStatsExec { .. } => PhysicalPlanKind::ThreadRepairStatsExec,
            PhysicalPlan::ShortestPathExec { .. } => PhysicalPlanKind::ShortestPathExec,
            PhysicalPlan::FilterExec { .. } => PhysicalPlanKind::FilterExec,
            PhysicalPlan::ProjectExec { .. } => PhysicalPlanKind::ProjectExec,
            PhysicalPlan::AggregateExec { .. } => PhysicalPlanKind::AggregateExec,
            PhysicalPlan::DistinctExec { .. } => PhysicalPlanKind::DistinctExec,
            PhysicalPlan::SortExec { .. } => PhysicalPlanKind::SortExec,
            PhysicalPlan::TopNExec { .. } => PhysicalPlanKind::TopNExec,
            PhysicalPlan::LimitExec { .. } => PhysicalPlanKind::LimitExec,
        }
    }

    pub fn class(&self) -> PhysicalPlanClass {
        self.kind().class()
    }

    pub fn children(&self) -> PhysicalPlanChildren<'_> {
        match self {
            PhysicalPlan::NodeCartesianProductExec { left, right }
            | PhysicalPlan::HashJoinExec { left, right, .. } => PlanChildren::Binary(left, right),
            PhysicalPlan::NodeColumnLookupExec { input, .. }
            | PhysicalPlan::AdjacencyExpandExec { input, .. }
            | PhysicalPlan::AdjacencyExistsExec { input, .. }
            | PhysicalPlan::OptionalDegreeExec { input, .. }
            | PhysicalPlan::FilterExec { input, .. }
            | PhysicalPlan::ProjectExec { input, .. }
            | PhysicalPlan::AggregateExec { input, .. }
            | PhysicalPlan::DistinctExec { input }
            | PhysicalPlan::SortExec { input, .. }
            | PhysicalPlan::TopNExec { input, .. }
            | PhysicalPlan::LimitExec { input, .. } => PlanChildren::Unary(input),
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
            | PhysicalPlan::ProjectGraph { .. }
            | PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::VectorSeedScan { .. }
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
            | PhysicalPlan::CreateRelationship { .. }
            | PhysicalPlan::EmptyExec
            | PhysicalPlan::SeqNodeScan { .. }
            | PhysicalPlan::NodeProjectionScanExec { .. }
            | PhysicalPlan::SourceSegmentScan { .. }
            | PhysicalPlan::IndexNodeSeek { .. }
            | PhysicalPlan::IndexNodeMultiSeek { .. }
            | PhysicalPlan::IndexNodeUnionSeek { .. }
            | PhysicalPlan::IndexNodeCompositeSeek { .. }
            | PhysicalPlan::IndexNodeCompositeRangeSeek { .. }
            | PhysicalPlan::IndexNodeRangeSeek { .. }
            | PhysicalPlan::IndexNodeTextSeek { .. }
            | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
            | PhysicalPlan::NodeCountExec { .. }
            | PhysicalPlan::RelationshipCountExec { .. }
            | PhysicalPlan::ThreadRepairStatsExec { .. }
            | PhysicalPlan::ShortestPathExec { .. } => PlanChildren::None,
        }
    }
}

impl PhysicalPlanNode for PhysicalPlan {
    fn kind(&self) -> PhysicalPlanKind {
        PhysicalPlan::kind(self)
    }

    fn children(&self) -> PhysicalPlanChildren<'_> {
        PhysicalPlan::children(self)
    }
}
