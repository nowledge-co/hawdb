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

//! Domain-neutral Cascades memo, rule, stage, cost, and diagnostics contracts.

pub mod cost;
pub mod memo;
pub mod properties;
pub mod rule;
pub mod search;
pub mod stage;

pub use cost::{PlanCost, PlanCostBreakdown};
pub use memo::{GroupId, Memo, MemoGroup};
pub use properties::{
    Distribution, MemoryBudgetClass, PhysicalProperties, RequiredProperties, ScanPruningSupport,
    VectorPrecision,
};
pub use rule::{
    apply_rule_batch, AppliedRule, OptimizerRule, RuleApplication, RuleBatch, RuleId, RuleKind,
    RulePromise,
};
pub use search::{
    OptimizerSearchDirective, OptimizerSearchDirectiveError, RuleEvent, RuleOutcome, SearchMode,
};
pub use stage::{
    ApplyOrder, OptimizationPipeline, OptimizationStage, PipelineExecution, RuleStage,
    StageRuleBatch, StageStats, StageTrace,
};
