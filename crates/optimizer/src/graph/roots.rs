use super::OptimizerTrace;

pub use hawdb_plan::{
    FastPathPhysicalPhase, LogicalPhase, LogicalPlanRoot, LoweringReadyLogicalPlanRoot,
    LoweringReadyPhase, PhysicalPhase, PlanPhase, PlanPhaseKind,
};

pub type PhysicalPlanRoot<P = PhysicalPhase> = hawdb_plan::PhysicalPlanRoot<OptimizerTrace, P>;
pub type FastPathPhysicalPlanRoot = hawdb_plan::FastPathPhysicalPlanRoot<OptimizerTrace>;
