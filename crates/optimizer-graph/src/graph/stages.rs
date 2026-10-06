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

use crate::{ApplyOrder, OptimizationStage};

pub(super) const LOGICAL_REWRITE_STAGE: OptimizationStage =
    OptimizationStage::new("logical_rewrite", ApplyOrder::FixedPoint);
pub(super) const LOGICAL_GROUPING_STAGE: OptimizationStage =
    OptimizationStage::new("logical_grouping", ApplyOrder::Once);
pub(super) const PHYSICAL_SEARCH_STAGE: OptimizationStage =
    OptimizationStage::new("physical_search", ApplyOrder::BottomUp);
pub(super) const ACCESS_PATH_SELECTION_STAGE: OptimizationStage =
    OptimizationStage::new("access_path_selection", ApplyOrder::BottomUp);
pub(super) const PLAN_FINALIZATION_STAGE: OptimizationStage =
    OptimizationStage::new("plan_finalization", ApplyOrder::TopDown);
pub(super) const DIRECT_PHYSICAL_FALLBACK_STAGE: OptimizationStage =
    OptimizationStage::new("direct_physical_fallback", ApplyOrder::Once);
pub(super) const SELECTED_PLAN_COSTING_STAGE: OptimizationStage =
    OptimizationStage::new("selected_plan_costing", ApplyOrder::Once);
