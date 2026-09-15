use crate::{
    Distribution, MemoryBudgetClass, OptimizationSearchReport, OptimizerConfig, OptimizerTrace,
    PhysicalProperties, PlanCost, PlanCostBreakdown, ScanPruningSupport, StageStats,
    VectorPrecision,
};
use skein_plan::PhysicalPlan;

mod access_path;
mod cardinality;
mod catalog;
mod costing;
mod logical_rewrite;
mod lowering;
mod properties;
mod roots;
mod selected_trace;
mod stages;
mod value_range;

pub use catalog::{
    optimizer_catalog_from_graph_statistics, OptimizerCatalog, OptimizerCatalogIndexes,
    OptimizerCatalogStatistics, OptimizerIndexStatistics,
};
pub use lowering::CascadesOptimizer;
pub use roots::{
    FastPathPhysicalPhase, FastPathPhysicalPlanRoot, LogicalPhase, LogicalPlanRoot,
    LoweringReadyLogicalPlanRoot, LoweringReadyPhase, PhysicalPhase, PhysicalPlanRoot, PlanPhase,
    PlanPhaseKind,
};

#[cfg(test)]
mod tests;
