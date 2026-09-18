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

//! Compatibility re-exports for optimizer operator metadata.
//!
//! New code should import logical operators from `logical` and physical
//! operators from `physical` so implementation rules stay distinct from logical
//! rewrites.

pub use crate::logical::{LogicalPlanClass, LogicalPlanKind, LogicalPlanNode};
pub use hawdb_plan::PhysicalPlanNode as PlanNode;
pub use hawdb_plan::{
    plan_class_counts, plan_operator_counts, visit_plan, PhysicalPlanClass, PhysicalPlanKind,
    PhysicalPlanNode, PlanChildren,
};
