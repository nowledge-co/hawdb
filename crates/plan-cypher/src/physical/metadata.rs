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

//! Physical optimizer operator metadata.

use std::collections::BTreeMap;
use std::str::FromStr;

/// A plan-local physical operator identity assigned in pre-order.
///
/// The identity is stable for one physical plan shape and lets planning and
/// execution diagnostics correlate repeated operators without relying on
/// operator names or process-local addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PhysicalOperatorId(usize);

impl PhysicalOperatorId {
    /// Creates an identity from its pre-order position in the physical plan.
    pub const fn from_ordinal(ordinal: usize) -> Self {
        Self(ordinal)
    }

    /// Returns the operator's zero-based pre-order position.
    pub const fn ordinal(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PhysicalPlanKind {
    CreateNodeLabel,
    CreateRelationshipType,
    CreateNodeTable,
    CreateRelationshipTable,
    CreateProperty,
    AlterTableState,
    AlterPropertyState,
    CreateIndex,
    CreateCompositeIndex,
    CreateRangeIndex,
    CreateFullTextIndex,
    CreateUniqueConstraint,
    CreateNodePropertyExistsConstraint,
    CreateRelationshipUniqueConstraint,
    CreateRelationshipPropertyExistsConstraint,
    ProjectGraph,
    GraphAlgorithm,
    VectorSeedScan,
    CreateNode,
    UnwindMutationExec,
    MergeNode,
    MergeRelationship,
    MergeMatchedRelationship,
    MergeRelationshipFromMatchedRelationship,
    MergeRelationshipToMatchedTarget,
    MergeRelationshipFromMatchedTarget,
    CreateMatchedRelationship,
    SetNodeProperty,
    SetNodeProperties,
    SetNodePropertiesReturn,
    SetRelationshipProperty,
    SetRelationshipProperties,
    DeleteNode,
    DeleteRelationship,
    DeleteRelationshipTargetNodes,
    CreateRelationship,
    GraphMatchExec,
    EmptyExec,
    SeqNodeScan,
    NodeProjectionScanExec,
    SourceSegmentScan,
    NodeCartesianProductExec,
    HashJoinExec,
    NodeColumnLookupExec,
    IndexNodeSeek,
    IndexNodeMultiSeek,
    IndexNodeUnionSeek,
    IndexNodeCompositeSeek,
    IndexNodeCompositeRangeSeek,
    IndexNodeRangeSeek,
    IndexNodeTextSeek,
    AdjacencyExpandExec,
    AdjacencyExistsExec,
    OptionalDegreeExec,
    OptionalRelationshipCountSumExec,
    NodeCountExec,
    RelationshipCountExec,
    ThreadRepairStatsExec,
    ShortestPathExec,
    FilterExec,
    ProjectExec,
    AggregateExec,
    DistinctExec,
    SortExec,
    TopNExec,
    LimitExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PhysicalPlanClass {
    Schema,
    Mutation,
    Access,
    Traversal,
    Relational,
    Procedure,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlanChildren<'a, P> {
    None,
    Unary(&'a P),
    Binary(&'a P, &'a P),
}

pub trait PhysicalPlanNode {
    fn kind(&self) -> PhysicalPlanKind;

    fn children(&self) -> PlanChildren<'_, Self>
    where
        Self: Sized;

    fn class(&self) -> PhysicalPlanClass {
        self.kind().class()
    }
}

impl PhysicalPlanKind {
    pub fn all() -> &'static [Self] {
        const ALL: &[PhysicalPlanKind] = &[
            PhysicalPlanKind::CreateNodeLabel,
            PhysicalPlanKind::CreateRelationshipType,
            PhysicalPlanKind::CreateNodeTable,
            PhysicalPlanKind::CreateRelationshipTable,
            PhysicalPlanKind::CreateProperty,
            PhysicalPlanKind::AlterTableState,
            PhysicalPlanKind::AlterPropertyState,
            PhysicalPlanKind::CreateIndex,
            PhysicalPlanKind::CreateCompositeIndex,
            PhysicalPlanKind::CreateRangeIndex,
            PhysicalPlanKind::CreateFullTextIndex,
            PhysicalPlanKind::CreateUniqueConstraint,
            PhysicalPlanKind::CreateNodePropertyExistsConstraint,
            PhysicalPlanKind::CreateRelationshipUniqueConstraint,
            PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint,
            PhysicalPlanKind::ProjectGraph,
            PhysicalPlanKind::GraphAlgorithm,
            PhysicalPlanKind::VectorSeedScan,
            PhysicalPlanKind::CreateNode,
            PhysicalPlanKind::UnwindMutationExec,
            PhysicalPlanKind::MergeNode,
            PhysicalPlanKind::MergeRelationship,
            PhysicalPlanKind::MergeMatchedRelationship,
            PhysicalPlanKind::MergeRelationshipFromMatchedRelationship,
            PhysicalPlanKind::MergeRelationshipToMatchedTarget,
            PhysicalPlanKind::MergeRelationshipFromMatchedTarget,
            PhysicalPlanKind::CreateMatchedRelationship,
            PhysicalPlanKind::SetNodeProperty,
            PhysicalPlanKind::SetNodeProperties,
            PhysicalPlanKind::SetNodePropertiesReturn,
            PhysicalPlanKind::SetRelationshipProperty,
            PhysicalPlanKind::SetRelationshipProperties,
            PhysicalPlanKind::DeleteNode,
            PhysicalPlanKind::DeleteRelationship,
            PhysicalPlanKind::DeleteRelationshipTargetNodes,
            PhysicalPlanKind::CreateRelationship,
            PhysicalPlanKind::GraphMatchExec,
            PhysicalPlanKind::EmptyExec,
            PhysicalPlanKind::SeqNodeScan,
            PhysicalPlanKind::NodeProjectionScanExec,
            PhysicalPlanKind::SourceSegmentScan,
            PhysicalPlanKind::NodeCartesianProductExec,
            PhysicalPlanKind::HashJoinExec,
            PhysicalPlanKind::NodeColumnLookupExec,
            PhysicalPlanKind::IndexNodeSeek,
            PhysicalPlanKind::IndexNodeMultiSeek,
            PhysicalPlanKind::IndexNodeUnionSeek,
            PhysicalPlanKind::IndexNodeCompositeSeek,
            PhysicalPlanKind::IndexNodeCompositeRangeSeek,
            PhysicalPlanKind::IndexNodeRangeSeek,
            PhysicalPlanKind::IndexNodeTextSeek,
            PhysicalPlanKind::AdjacencyExpandExec,
            PhysicalPlanKind::AdjacencyExistsExec,
            PhysicalPlanKind::OptionalDegreeExec,
            PhysicalPlanKind::OptionalRelationshipCountSumExec,
            PhysicalPlanKind::NodeCountExec,
            PhysicalPlanKind::RelationshipCountExec,
            PhysicalPlanKind::ThreadRepairStatsExec,
            PhysicalPlanKind::ShortestPathExec,
            PhysicalPlanKind::FilterExec,
            PhysicalPlanKind::ProjectExec,
            PhysicalPlanKind::AggregateExec,
            PhysicalPlanKind::DistinctExec,
            PhysicalPlanKind::SortExec,
            PhysicalPlanKind::TopNExec,
            PhysicalPlanKind::LimitExec,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PhysicalPlanKind::CreateNodeLabel => "CreateNodeLabel",
            PhysicalPlanKind::CreateRelationshipType => "CreateRelationshipType",
            PhysicalPlanKind::CreateNodeTable => "CreateNodeTable",
            PhysicalPlanKind::CreateRelationshipTable => "CreateRelationshipTable",
            PhysicalPlanKind::CreateProperty => "CreateProperty",
            PhysicalPlanKind::AlterTableState => "AlterTableState",
            PhysicalPlanKind::AlterPropertyState => "AlterPropertyState",
            PhysicalPlanKind::CreateIndex => "CreateIndex",
            PhysicalPlanKind::CreateCompositeIndex => "CreateCompositeIndex",
            PhysicalPlanKind::CreateRangeIndex => "CreateRangeIndex",
            PhysicalPlanKind::CreateFullTextIndex => "CreateFullTextIndex",
            PhysicalPlanKind::CreateUniqueConstraint => "CreateUniqueConstraint",
            PhysicalPlanKind::CreateNodePropertyExistsConstraint => {
                "CreateNodePropertyExistsConstraint"
            }
            PhysicalPlanKind::CreateRelationshipUniqueConstraint => {
                "CreateRelationshipUniqueConstraint"
            }
            PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint => {
                "CreateRelationshipPropertyExistsConstraint"
            }
            PhysicalPlanKind::ProjectGraph => "ProjectGraph",
            PhysicalPlanKind::GraphAlgorithm => "GraphAlgorithm",
            PhysicalPlanKind::VectorSeedScan => "VectorSeedScan",
            PhysicalPlanKind::CreateNode => "CreateNode",
            PhysicalPlanKind::UnwindMutationExec => "UnwindMutationExec",
            PhysicalPlanKind::MergeNode => "MergeNode",
            PhysicalPlanKind::MergeRelationship => "MergeRelationship",
            PhysicalPlanKind::MergeMatchedRelationship => "MergeMatchedRelationship",
            PhysicalPlanKind::MergeRelationshipFromMatchedRelationship => {
                "MergeRelationshipFromMatchedRelationship"
            }
            PhysicalPlanKind::MergeRelationshipToMatchedTarget => {
                "MergeRelationshipToMatchedTarget"
            }
            PhysicalPlanKind::MergeRelationshipFromMatchedTarget => {
                "MergeRelationshipFromMatchedTarget"
            }
            PhysicalPlanKind::CreateMatchedRelationship => "CreateMatchedRelationship",
            PhysicalPlanKind::SetNodeProperty => "SetNodeProperty",
            PhysicalPlanKind::SetNodeProperties => "SetNodeProperties",
            PhysicalPlanKind::SetNodePropertiesReturn => "SetNodePropertiesReturn",
            PhysicalPlanKind::SetRelationshipProperty => "SetRelationshipProperty",
            PhysicalPlanKind::SetRelationshipProperties => "SetRelationshipProperties",
            PhysicalPlanKind::DeleteNode => "DeleteNode",
            PhysicalPlanKind::DeleteRelationship => "DeleteRelationship",
            PhysicalPlanKind::DeleteRelationshipTargetNodes => "DeleteRelationshipTargetNodes",
            PhysicalPlanKind::CreateRelationship => "CreateRelationship",
            PhysicalPlanKind::GraphMatchExec => "GraphMatchExec",
            PhysicalPlanKind::EmptyExec => "EmptyExec",
            PhysicalPlanKind::SeqNodeScan => "SeqNodeScan",
            PhysicalPlanKind::NodeProjectionScanExec => "NodeProjectionScanExec",
            PhysicalPlanKind::SourceSegmentScan => "SourceSegmentScan",
            PhysicalPlanKind::HashJoinExec => "HashJoinExec",
            PhysicalPlanKind::NodeCartesianProductExec => "NodeCartesianProductExec",
            PhysicalPlanKind::NodeColumnLookupExec => "NodeColumnLookupExec",
            PhysicalPlanKind::IndexNodeSeek => "IndexNodeSeek",
            PhysicalPlanKind::IndexNodeMultiSeek => "IndexNodeMultiSeek",
            PhysicalPlanKind::IndexNodeUnionSeek => "IndexNodeUnionSeek",
            PhysicalPlanKind::IndexNodeCompositeSeek => "IndexNodeCompositeSeek",
            PhysicalPlanKind::IndexNodeCompositeRangeSeek => "IndexNodeCompositeRangeSeek",
            PhysicalPlanKind::IndexNodeRangeSeek => "IndexNodeRangeSeek",
            PhysicalPlanKind::IndexNodeTextSeek => "IndexNodeTextSeek",
            PhysicalPlanKind::AdjacencyExpandExec => "AdjacencyExpandExec",
            PhysicalPlanKind::AdjacencyExistsExec => "AdjacencyExistsExec",
            PhysicalPlanKind::OptionalDegreeExec => "OptionalDegreeExec",
            PhysicalPlanKind::OptionalRelationshipCountSumExec => {
                "OptionalRelationshipCountSumExec"
            }
            PhysicalPlanKind::NodeCountExec => "NodeCountExec",
            PhysicalPlanKind::RelationshipCountExec => "RelationshipCountExec",
            PhysicalPlanKind::ThreadRepairStatsExec => "ThreadRepairStatsExec",
            PhysicalPlanKind::ShortestPathExec => "ShortestPathExec",
            PhysicalPlanKind::FilterExec => "FilterExec",
            PhysicalPlanKind::ProjectExec => "ProjectExec",
            PhysicalPlanKind::AggregateExec => "AggregateExec",
            PhysicalPlanKind::DistinctExec => "DistinctExec",
            PhysicalPlanKind::SortExec => "SortExec",
            PhysicalPlanKind::TopNExec => "TopNExec",
            PhysicalPlanKind::LimitExec => "LimitExec",
        }
    }

    pub fn class(self) -> PhysicalPlanClass {
        match self {
            PhysicalPlanKind::CreateNodeLabel
            | PhysicalPlanKind::CreateRelationshipType
            | PhysicalPlanKind::CreateNodeTable
            | PhysicalPlanKind::CreateRelationshipTable
            | PhysicalPlanKind::CreateProperty
            | PhysicalPlanKind::AlterTableState
            | PhysicalPlanKind::AlterPropertyState
            | PhysicalPlanKind::CreateIndex
            | PhysicalPlanKind::CreateCompositeIndex
            | PhysicalPlanKind::CreateRangeIndex
            | PhysicalPlanKind::CreateFullTextIndex
            | PhysicalPlanKind::CreateUniqueConstraint
            | PhysicalPlanKind::CreateNodePropertyExistsConstraint
            | PhysicalPlanKind::CreateRelationshipUniqueConstraint
            | PhysicalPlanKind::CreateRelationshipPropertyExistsConstraint => {
                PhysicalPlanClass::Schema
            }
            PhysicalPlanKind::CreateNode
            | PhysicalPlanKind::UnwindMutationExec
            | PhysicalPlanKind::MergeNode
            | PhysicalPlanKind::MergeRelationship
            | PhysicalPlanKind::MergeMatchedRelationship
            | PhysicalPlanKind::MergeRelationshipFromMatchedRelationship
            | PhysicalPlanKind::MergeRelationshipToMatchedTarget
            | PhysicalPlanKind::MergeRelationshipFromMatchedTarget
            | PhysicalPlanKind::CreateMatchedRelationship
            | PhysicalPlanKind::SetNodeProperty
            | PhysicalPlanKind::SetNodeProperties
            | PhysicalPlanKind::SetNodePropertiesReturn
            | PhysicalPlanKind::SetRelationshipProperty
            | PhysicalPlanKind::SetRelationshipProperties
            | PhysicalPlanKind::DeleteNode
            | PhysicalPlanKind::DeleteRelationship
            | PhysicalPlanKind::DeleteRelationshipTargetNodes
            | PhysicalPlanKind::CreateRelationship => PhysicalPlanClass::Mutation,
            PhysicalPlanKind::SeqNodeScan
            | PhysicalPlanKind::NodeProjectionScanExec
            | PhysicalPlanKind::SourceSegmentScan
            | PhysicalPlanKind::NodeColumnLookupExec
            | PhysicalPlanKind::IndexNodeSeek
            | PhysicalPlanKind::IndexNodeMultiSeek
            | PhysicalPlanKind::IndexNodeUnionSeek
            | PhysicalPlanKind::IndexNodeCompositeSeek
            | PhysicalPlanKind::IndexNodeCompositeRangeSeek
            | PhysicalPlanKind::IndexNodeRangeSeek
            | PhysicalPlanKind::IndexNodeTextSeek => PhysicalPlanClass::Access,
            PhysicalPlanKind::GraphMatchExec
            | PhysicalPlanKind::AdjacencyExpandExec
            | PhysicalPlanKind::AdjacencyExistsExec
            | PhysicalPlanKind::OptionalDegreeExec
            | PhysicalPlanKind::OptionalRelationshipCountSumExec
            | PhysicalPlanKind::ShortestPathExec => PhysicalPlanClass::Traversal,
            PhysicalPlanKind::NodeCountExec
            | PhysicalPlanKind::RelationshipCountExec
            | PhysicalPlanKind::EmptyExec
            | PhysicalPlanKind::NodeCartesianProductExec
            | PhysicalPlanKind::HashJoinExec
            | PhysicalPlanKind::FilterExec
            | PhysicalPlanKind::ProjectExec
            | PhysicalPlanKind::AggregateExec
            | PhysicalPlanKind::DistinctExec
            | PhysicalPlanKind::SortExec
            | PhysicalPlanKind::TopNExec
            | PhysicalPlanKind::LimitExec => PhysicalPlanClass::Relational,
            PhysicalPlanKind::ProjectGraph
            | PhysicalPlanKind::GraphAlgorithm
            | PhysicalPlanKind::VectorSeedScan
            | PhysicalPlanKind::ThreadRepairStatsExec => PhysicalPlanClass::Procedure,
        }
    }
}

impl PhysicalPlanClass {
    pub fn all() -> &'static [Self] {
        const ALL: &[PhysicalPlanClass] = &[
            PhysicalPlanClass::Schema,
            PhysicalPlanClass::Mutation,
            PhysicalPlanClass::Access,
            PhysicalPlanClass::Traversal,
            PhysicalPlanClass::Relational,
            PhysicalPlanClass::Procedure,
        ];
        ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            PhysicalPlanClass::Schema => "schema",
            PhysicalPlanClass::Mutation => "mutation",
            PhysicalPlanClass::Access => "access",
            PhysicalPlanClass::Traversal => "traversal",
            PhysicalPlanClass::Relational => "relational",
            PhysicalPlanClass::Procedure => "procedure",
        }
    }
}

impl FromStr for PhysicalPlanKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_str() == value)
            .ok_or("unknown physical plan kind")
    }
}

impl FromStr for PhysicalPlanClass {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::all()
            .iter()
            .copied()
            .find(|class| class.as_str() == value)
            .ok_or("unknown physical plan class")
    }
}

impl<'a, P> PlanChildren<'a, P> {
    pub fn len(self) -> usize {
        match self {
            PlanChildren::None => 0,
            PlanChildren::Unary(_) => 1,
            PlanChildren::Binary(_, _) => 2,
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

pub fn plan_operator_counts<P>(plan: &P) -> BTreeMap<String, usize>
where
    P: PhysicalPlanNode,
{
    let mut counts = BTreeMap::new();
    visit_plan(plan, &mut |node| {
        *counts.entry(node.kind().as_str().to_string()).or_default() += 1;
    });
    counts
}

pub fn plan_class_counts<P>(plan: &P) -> BTreeMap<String, usize>
where
    P: PhysicalPlanNode,
{
    let mut counts = BTreeMap::new();
    visit_plan(plan, &mut |node| {
        *counts.entry(node.class().as_str().to_string()).or_default() += 1;
    });
    counts
}

pub fn visit_plan<P>(plan: &P, visitor: &mut impl FnMut(&P))
where
    P: PhysicalPlanNode,
{
    visitor(plan);
    match plan.children() {
        PlanChildren::None => {}
        PlanChildren::Unary(input) => visit_plan(input, visitor),
        PlanChildren::Binary(left, right) => {
            visit_plan(left, visitor);
            visit_plan(right, visitor);
        }
    }
}

/// Visits a physical plan in pre-order and assigns each operator its plan-local identity.
pub fn visit_plan_with_ids<P>(plan: &P, visitor: &mut impl FnMut(PhysicalOperatorId, &P))
where
    P: PhysicalPlanNode,
{
    fn visit<P>(
        plan: &P,
        next_ordinal: &mut usize,
        visitor: &mut impl FnMut(PhysicalOperatorId, &P),
    ) where
        P: PhysicalPlanNode,
    {
        let operator_id = PhysicalOperatorId::from_ordinal(*next_ordinal);
        *next_ordinal = next_ordinal
            .checked_add(1)
            .expect("physical plan operator count exceeds usize");
        visitor(operator_id, plan);
        match plan.children() {
            PlanChildren::None => {}
            PlanChildren::Unary(input) => visit(input, next_ordinal, visitor),
            PlanChildren::Binary(left, right) => {
                visit(left, next_ordinal, visitor);
                visit(right, next_ordinal, visitor);
            }
        }
    }

    let mut next_ordinal = 0;
    visit(plan, &mut next_ordinal, visitor);
}

#[cfg(test)]
mod tests {
    use super::{
        plan_class_counts, plan_operator_counts, visit_plan, visit_plan_with_ids,
        PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren,
    };

    #[test]
    fn physical_plan_kind_exposes_stable_strings_and_classes() {
        assert_eq!(PhysicalPlanKind::IndexNodeSeek.as_str(), "IndexNodeSeek");
        assert_eq!(
            PhysicalPlanKind::IndexNodeSeek.class(),
            PhysicalPlanClass::Access
        );
        assert_eq!(
            PhysicalPlanKind::AdjacencyExpandExec.class(),
            PhysicalPlanClass::Traversal
        );
        assert_eq!(
            PhysicalPlanKind::ProjectExec.class(),
            PhysicalPlanClass::Relational
        );
    }

    #[test]
    fn physical_plan_kind_strings_round_trip_for_diagnostics() {
        for kind in PhysicalPlanKind::all() {
            assert_eq!(kind.as_str().parse::<PhysicalPlanKind>(), Ok(*kind));
        }
        assert!("UnknownExec".parse::<PhysicalPlanKind>().is_err());
    }

    #[test]
    fn physical_plan_class_exposes_stable_strings() {
        assert_eq!(PhysicalPlanClass::Schema.as_str(), "schema");
        assert_eq!(PhysicalPlanClass::Mutation.as_str(), "mutation");
        assert_eq!(PhysicalPlanClass::Access.as_str(), "access");
        assert_eq!(PhysicalPlanClass::Traversal.as_str(), "traversal");
        assert_eq!(PhysicalPlanClass::Relational.as_str(), "relational");
        assert_eq!(PhysicalPlanClass::Procedure.as_str(), "procedure");
    }

    #[test]
    fn physical_plan_class_strings_round_trip_for_diagnostics() {
        for class in PhysicalPlanClass::all() {
            assert_eq!(class.as_str().parse::<PhysicalPlanClass>(), Ok(*class));
        }
        assert!("unknown".parse::<PhysicalPlanClass>().is_err());
    }

    #[test]
    fn plan_children_reports_arity_without_knowing_plan_type() {
        assert_eq!(PlanChildren::<&str>::None.len(), 0);
        assert!(PlanChildren::<&str>::None.is_empty());

        let child = "scan";
        assert_eq!(PlanChildren::Unary(&child).len(), 1);

        let left = "left";
        let right = "right";
        assert_eq!(PlanChildren::Binary(&left, &right).len(), 2);
    }

    #[derive(Debug)]
    enum TestPlan {
        Seek,
        Project(Box<TestPlan>),
        Join(Box<TestPlan>, Box<TestPlan>),
    }

    impl PhysicalPlanNode for TestPlan {
        fn kind(&self) -> PhysicalPlanKind {
            match self {
                TestPlan::Seek => PhysicalPlanKind::IndexNodeSeek,
                TestPlan::Project(_) => PhysicalPlanKind::ProjectExec,
                TestPlan::Join(_, _) => PhysicalPlanKind::NodeCartesianProductExec,
            }
        }

        fn children(&self) -> PlanChildren<'_, Self> {
            match self {
                TestPlan::Seek => PlanChildren::None,
                TestPlan::Project(input) => PlanChildren::Unary(input),
                TestPlan::Join(left, right) => PlanChildren::Binary(left, right),
            }
        }
    }

    #[test]
    fn plan_node_helpers_traverse_without_knowing_plan_payloads() {
        let plan = TestPlan::Project(Box::new(TestPlan::Join(
            Box::new(TestPlan::Seek),
            Box::new(TestPlan::Seek),
        )));

        let mut visit_order = Vec::new();
        visit_plan(&plan, &mut |node| {
            visit_order.push(node.kind().as_str());
        });

        assert_eq!(
            visit_order,
            vec![
                "ProjectExec",
                "NodeCartesianProductExec",
                "IndexNodeSeek",
                "IndexNodeSeek"
            ]
        );
        assert_eq!(plan_operator_counts(&plan)["IndexNodeSeek"], 2);
        assert_eq!(plan_operator_counts(&plan)["ProjectExec"], 1);
        assert_eq!(plan_class_counts(&plan)["access"], 2);
        assert_eq!(plan_class_counts(&plan)["relational"], 2);

        let mut identified = Vec::new();
        visit_plan_with_ids(&plan, &mut |operator_id, node| {
            identified.push((operator_id.ordinal(), node.kind().as_str()));
        });
        assert_eq!(
            identified,
            vec![
                (0, "ProjectExec"),
                (1, "NodeCartesianProductExec"),
                (2, "IndexNodeSeek"),
                (3, "IndexNodeSeek"),
            ]
        );
    }
}
