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

use crate::{
    Distribution, MemoryBudgetClass, OptimizationSearchReport, OptimizerConfig, OptimizerTrace,
    PhysicalProperties, PlanCost, PlanCostBreakdown, ScanPruningSupport, StageStats,
    VectorPrecision,
};
use hawdb_plan::PhysicalPlan;

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
