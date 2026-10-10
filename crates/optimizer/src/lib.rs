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

//! Compatibility facade over the framework and domain-owned optimizer families.

pub use hawdb_cascades::{context, cost, memo, properties, rule, stage};
pub use hawdb_optimizer_graph::{graph, logical, operator, search, trace};
pub use hawdb_optimizer_predicate::predicate;
pub use hawdb_optimizer_relational::{
    relational, relational_join, relational_join_hypergraph, relational_join_rewrite,
    relational_planning, relational_profile, relational_sargability,
};
pub use hawdb_optimizer_vector::vector;

pub use context::{
    ExplainMode, OptimizerContext, QueryFamily, ResourceHints, StatementClass, TraceSink,
};
pub use cost::{PlanCost, PlanCostBreakdown};
pub use graph::{
    CascadesOptimizer, FastPathPhysicalPhase, FastPathPhysicalPlanRoot, LogicalPhase,
    LogicalPlanRoot, LoweringReadyLogicalPlanRoot, LoweringReadyPhase, OptimizerCatalog,
    OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerIndexStatistics, PhysicalPhase,
    PhysicalPlanRoot, PlanPhase, PlanPhaseKind,
};
pub use hawdb_optimizer_graph::PhysicalPlanNode as PlanNode;
pub use hawdb_optimizer_graph::{
    plan_class_counts, plan_operator_counts, visit_plan, visit_plan_with_ids, PhysicalOperatorId,
    PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren,
};
pub use hawdb_optimizer_relational::{
    estimate_relational_access_cost, estimate_relational_access_path_cost,
    estimate_relational_access_path_cost_with_context, estimate_relational_join_cost,
    estimate_relational_join_cost_with_contexts, estimate_relational_probe_join_cost,
    RelationalAccessCostContext, RelationalJoinCardinality, RelationalJoinCostContexts,
    RelationalJoinRightInput, RelationalJoinSelectivity,
};
pub use logical::{LogicalPlanClass, LogicalPlanKind, LogicalPlanNode};
pub use memo::{GroupId, Memo, MemoGroup};
pub use predicate::{
    normalize_search_enum_value, push_search_predicates, search_field_is_enum_like, SearchFieldRef,
    SearchPredicate, SearchPredicateOp, SearchPredicateParseError, SearchPredicatePushdown,
    SearchPredicateSet, SearchScalarValue, SearchScanPredicateSupport,
};
pub use properties::{
    Distribution, MemoryBudgetClass, PhysicalProperties, RequiredProperties, ScanPruningSupport,
    VectorPrecision,
};
pub use relational::{
    estimate_relational_nested_loop_join_cost, select_relational_access_path,
    select_relational_access_path_with_context, skyline_prune_relational_access_paths,
    skyline_prune_relational_access_paths_with_context, RelationalAccessPathDescriptor,
    RelationalAccessPathKind, RelationalNestedLoopJoinCost,
};
pub use relational_join::{
    enumerate_relational_inner_joins, enumerate_relational_inner_joins_with_cost_contexts,
    RelationalInnerJoinEnumeration, RelationalJoinAccessApplicability, RelationalJoinAccessPath,
    RelationalJoinEnumerationConfig, RelationalJoinEnumerationError, RelationalJoinGraph,
    RelationalJoinPlan, RelationalJoinPredicate, RelationalJoinPredicateId, RelationalJoinRelation,
    RelationalJoinStep,
};
pub use relational_join_hypergraph::{
    enumerate_relational_csg_cmp_joins, enumerate_relational_csg_cmp_joins_with_cost_contexts,
    enumerate_relational_csg_cmp_joins_with_implementations,
    enumerate_relational_csg_cmp_joins_with_right_input_policy, RelationalCsgCmpAlternative,
    RelationalCsgCmpEnumeration, RelationalCsgCmpJoinImplementation, RelationalCsgCmpPlan,
    RelationalCsgCmpPlanNode, RelationalCsgCmpRightInputPolicy, RelationalEquiJoinAlgorithm,
};
pub use relational_join_rewrite::{
    analyze_relational_join_conflicts, enumerate_relational_join_rewrites,
    enumerate_relational_join_rewrites_with_cost_contexts, RelationalJoinConflictAnalysis,
    RelationalJoinConflictDescriptor, RelationalJoinConflictRule, RelationalJoinOperator,
    RelationalJoinOperatorId, RelationalJoinOperatorKind, RelationalJoinRewriteEnumeration,
    RelationalJoinRewriteError, RelationalJoinRewritePlan, RelationalJoinRewriteProblem,
    RelationalJoinRewriteStep, RelationalJoinTree,
};
pub use relational_planning::{
    RelationalJoinPlanningAttempt, RelationalJoinPlanningBudget, RelationalJoinPlanningCost,
    RelationalJoinPlanningDirective, RelationalJoinPlanningFallbackClass,
    RelationalJoinPlanningOutcome, RelationalJoinPlanningReason, RelationalJoinPlanningStatus,
    RelationalJoinPlanningStrategy,
};
pub use relational_profile::{
    RelationalOperatorCardinalityProfile, RelationalOperatorId, RelationalOperatorKind,
};
pub use rule::{
    apply_rule_batch, AppliedRule, OptimizerRule, RuleApplication, RuleBatch, RuleId, RuleKind,
    RulePromise,
};
pub use search::{
    OptimizationSearchReport, OptimizerSearchDirective, OptimizerSearchDirectiveError, RuleEvent,
    RuleOutcome, SearchMode, SelectedPlanTrace,
};
pub use stage::{
    ApplyOrder, OptimizationPipeline, OptimizationStage, PipelineExecution, RuleStage,
    StageRuleBatch, StageStats, StageTrace,
};
pub use trace::{OperatorCardinalityEstimate, OptimizerConfig, OptimizerTrace};
pub use vector::{
    plan_vector_search, select_adaptive_vector_backend, validate_vector_pipeline,
    AdaptiveVectorBackend, AdaptiveVectorBackendDecision, AdaptiveVectorBackendInput,
    AdaptiveVectorBackendPolicy, PlannedVectorSearch, VectorCompressionPreference, VectorPlanError,
    VectorPlanProperties,
};
