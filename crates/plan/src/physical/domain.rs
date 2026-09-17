use super::PhysicalPlan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalOperatorDomain {
    Schema,
    Mutation,
    Access,
    Traversal,
    Relational,
    Procedure,
}

macro_rules! define_domain_plan_ref {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name<'a>(&'a PhysicalPlan);

        impl<'a> $name<'a> {
            fn new(plan: &'a PhysicalPlan) -> Self {
                Self(plan)
            }

            pub fn plan(self) -> &'a PhysicalPlan {
                self.0
            }
        }
    };
}

define_domain_plan_ref!(SchemaPhysicalPlanRef);
define_domain_plan_ref!(MutationPhysicalPlanRef);
define_domain_plan_ref!(AccessPhysicalPlanRef);
define_domain_plan_ref!(TraversalPhysicalPlanRef);
define_domain_plan_ref!(RelationalPhysicalPlanRef);
define_domain_plan_ref!(ProcedurePhysicalPlanRef);

/// Zero-copy diagnostic classification of the compatibility plan facade.
///
/// These references do not provide a decomposed, type-isolated plan model.
#[derive(Debug, Clone, Copy)]
pub enum PhysicalPlanDomainRef<'a> {
    Schema(SchemaPhysicalPlanRef<'a>),
    Mutation(MutationPhysicalPlanRef<'a>),
    Access(AccessPhysicalPlanRef<'a>),
    Traversal(TraversalPhysicalPlanRef<'a>),
    Relational(RelationalPhysicalPlanRef<'a>),
    Procedure(ProcedurePhysicalPlanRef<'a>),
}

impl<'a> PhysicalPlanDomainRef<'a> {
    pub fn domain(self) -> PhysicalOperatorDomain {
        match self {
            Self::Schema(_) => PhysicalOperatorDomain::Schema,
            Self::Mutation(_) => PhysicalOperatorDomain::Mutation,
            Self::Access(_) => PhysicalOperatorDomain::Access,
            Self::Traversal(_) => PhysicalOperatorDomain::Traversal,
            Self::Relational(_) => PhysicalOperatorDomain::Relational,
            Self::Procedure(_) => PhysicalOperatorDomain::Procedure,
        }
    }

    pub fn plan(self) -> &'a PhysicalPlan {
        match self {
            Self::Schema(plan) => plan.plan(),
            Self::Mutation(plan) => plan.plan(),
            Self::Access(plan) => plan.plan(),
            Self::Traversal(plan) => plan.plan(),
            Self::Relational(plan) => plan.plan(),
            Self::Procedure(plan) => plan.plan(),
        }
    }
}

impl PhysicalPlan {
    pub fn as_domain(&self) -> PhysicalPlanDomainRef<'_> {
        match self {
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
            | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. } => {
                PhysicalPlanDomainRef::Schema(SchemaPhysicalPlanRef::new(self))
            }
            PhysicalPlan::CreateNode { .. }
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
                PhysicalPlanDomainRef::Mutation(MutationPhysicalPlanRef::new(self))
            }
            PhysicalPlan::SeqNodeScan { .. }
            | PhysicalPlan::NodeProjectionScanExec { .. }
            | PhysicalPlan::SourceSegmentScan { .. }
            | PhysicalPlan::NodeColumnLookupExec { .. }
            | PhysicalPlan::IndexNodeSeek { .. }
            | PhysicalPlan::IndexNodeMultiSeek { .. }
            | PhysicalPlan::IndexNodeUnionSeek { .. }
            | PhysicalPlan::IndexNodeCompositeSeek { .. }
            | PhysicalPlan::IndexNodeCompositeRangeSeek { .. }
            | PhysicalPlan::IndexNodeRangeSeek { .. }
            | PhysicalPlan::IndexNodeTextSeek { .. } => {
                PhysicalPlanDomainRef::Access(AccessPhysicalPlanRef::new(self))
            }
            PhysicalPlan::AdjacencyExpandExec { .. }
            | PhysicalPlan::AdjacencyExistsExec { .. }
            | PhysicalPlan::OptionalDegreeExec { .. }
            | PhysicalPlan::OptionalRelationshipCountSumExec { .. }
            | PhysicalPlan::ShortestPathExec { .. } => {
                PhysicalPlanDomainRef::Traversal(TraversalPhysicalPlanRef::new(self))
            }
            PhysicalPlan::NodeCountExec { .. }
            | PhysicalPlan::RelationshipCountExec { .. }
            | PhysicalPlan::EmptyExec
            | PhysicalPlan::NodeCartesianProductExec { .. }
            | PhysicalPlan::HashJoinExec { .. }
            | PhysicalPlan::FilterExec { .. }
            | PhysicalPlan::ProjectExec { .. }
            | PhysicalPlan::AggregateExec { .. }
            | PhysicalPlan::DistinctExec { .. }
            | PhysicalPlan::SortExec { .. }
            | PhysicalPlan::TopNExec { .. }
            | PhysicalPlan::LimitExec { .. } => {
                PhysicalPlanDomainRef::Relational(RelationalPhysicalPlanRef::new(self))
            }
            PhysicalPlan::ProjectGraph { .. }
            | PhysicalPlan::GraphAlgorithm { .. }
            | PhysicalPlan::VectorSeedScan { .. }
            | PhysicalPlan::ThreadRepairStatsExec { .. } => {
                PhysicalPlanDomainRef::Procedure(ProcedurePhysicalPlanRef::new(self))
            }
        }
    }

    pub fn domain(&self) -> PhysicalOperatorDomain {
        self.as_domain().domain()
    }
}
