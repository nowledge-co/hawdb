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

//! Relational access costing, join enumeration, and sargability.

mod cardinality_defaults;
pub mod relational;
pub mod relational_join;
mod relational_join_cost;
pub mod relational_join_hypergraph;
pub mod relational_join_rewrite;
pub mod relational_planning;
pub mod relational_profile;
pub mod relational_sargability;

pub use hawdb_cascades::*;
pub use relational::{
    estimate_relational_nested_loop_join_cost, select_relational_access_path,
    skyline_prune_relational_access_paths, RelationalAccessPathDescriptor,
    RelationalAccessPathKind, RelationalNestedLoopJoinCost,
};
pub use relational_join::{
    enumerate_relational_inner_joins, RelationalInnerJoinEnumeration,
    RelationalJoinAccessApplicability, RelationalJoinAccessPath, RelationalJoinEnumerationConfig,
    RelationalJoinEnumerationError, RelationalJoinGraph, RelationalJoinPlan,
    RelationalJoinPredicate, RelationalJoinPredicateId, RelationalJoinRelation, RelationalJoinStep,
};
pub use relational_join_cost::{
    estimate_relational_access_cost, estimate_relational_access_path_cost,
    estimate_relational_join_cost, estimate_relational_probe_join_cost, RelationalJoinCardinality,
    RelationalJoinRightInput, RelationalJoinSelectivity,
};
pub use relational_join_hypergraph::{
    enumerate_relational_csg_cmp_joins, enumerate_relational_csg_cmp_joins_with_implementations,
    enumerate_relational_csg_cmp_joins_with_right_input_policy, RelationalCsgCmpAlternative,
    RelationalCsgCmpEnumeration, RelationalCsgCmpJoinImplementation, RelationalCsgCmpPlan,
    RelationalCsgCmpPlanNode, RelationalCsgCmpRightInputPolicy, RelationalEquiJoinAlgorithm,
};
pub use relational_join_rewrite::{
    analyze_relational_join_conflicts, enumerate_relational_join_rewrites,
    RelationalJoinConflictAnalysis, RelationalJoinConflictDescriptor, RelationalJoinConflictRule,
    RelationalJoinOperator, RelationalJoinOperatorId, RelationalJoinOperatorKind,
    RelationalJoinRewriteEnumeration, RelationalJoinRewriteError, RelationalJoinRewritePlan,
    RelationalJoinRewriteProblem, RelationalJoinRewriteStep, RelationalJoinTree,
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
