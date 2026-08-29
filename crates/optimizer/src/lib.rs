pub mod context;
pub mod cost;
pub mod graph;
pub mod logical;
pub mod memo;
pub mod operator;
pub mod predicate;
pub mod properties;
pub mod relational;
pub mod rule;
pub mod search;
pub mod stage;
pub mod trace;
pub mod vector;

pub use context::{
    ExplainMode, OptimizerContext, QueryFamily, ResourceHints, StatementClass, TraceSink,
};
pub use cost::{PlanCost, PlanCostBreakdown};
pub use graph::{
    CascadesOptimizer, FastPathPhysicalPhase, FastPathPhysicalPlanRoot, LogicalPhase,
    LogicalPlanRoot, OptimizedLogicalPhase, OptimizedLogicalPlanRoot, OptimizerCatalog,
    OptimizerCatalogIndexes, OptimizerCatalogStatistics, OptimizerIndexStatistics, PhysicalPhase,
    PhysicalPlanRoot, PlanPhase, PlanPhaseKind,
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
    select_relational_access_path, skyline_prune_relational_access_paths,
    RelationalAccessPathDescriptor, RelationalAccessPathKind,
};
pub use rule::{
    apply_rule_batch, AppliedRule, OptimizerRule, RuleApplication, RuleBatch, RuleId, RuleKind,
    RulePromise,
};
pub use search::{
    OptimizationSearchReport, OptimizerSearchDirective, OptimizerSearchDirectiveError, RuleEvent,
    RuleOutcome, SearchMode, SelectedPlanTrace,
};
pub use skein_plan::PhysicalPlanNode as PlanNode;
pub use skein_plan::{
    plan_class_counts, plan_operator_counts, visit_plan, visit_plan_with_ids, PhysicalOperatorId,
    PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren,
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
