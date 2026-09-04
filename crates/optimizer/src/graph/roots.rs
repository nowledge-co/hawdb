use super::OptimizerTrace;

pub use skein_plan::{
    FastPathPhysicalPhase, LogicalPhase, LogicalPlanRoot, LoweringReadyLogicalPlanRoot,
    LoweringReadyPhase, PhysicalPhase, PlanPhase, PlanPhaseKind,
};

pub type PhysicalPlanRoot<P = PhysicalPhase> = skein_plan::PhysicalPlanRoot<OptimizerTrace, P>;
pub type FastPathPhysicalPlanRoot = skein_plan::FastPathPhysicalPlanRoot<OptimizerTrace>;
