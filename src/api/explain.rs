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

use super::plan_cache::OptimizedQueryPlan;
use crate::executor::{self, Row};
use crate::qos::WorkRequest;
use hawdb_explain::json::{
    empty_read_execution_profile as owner_empty_read_execution_profile,
    explain_analyze_output_row as owner_explain_analyze_output_row,
    explain_output_row as owner_explain_output_row, ExplainJsonInput,
};

fn explain_json_input<'a>(
    optimized: &'a OptimizedQueryPlan,
    work_request: &'a WorkRequest,
    statement_kind: &'static str,
) -> ExplainJsonInput<'a> {
    ExplainJsonInput {
        physical_plan: &optimized.physical_plan,
        trace: &optimized.trace,
        plan_cache_lookup: optimized.plan_cache_lookup,
        configured_max_optimizer_groups: optimized.configured_max_optimizer_groups,
        effective_max_optimizer_groups: optimized.effective_max_optimizer_groups,
        work_request,
        statement_kind,
    }
}

pub(super) fn explain_output_row(
    optimized: &OptimizedQueryPlan,
    work_request: WorkRequest,
    statement_kind: &'static str,
) -> Row {
    owner_explain_output_row(&explain_json_input(
        optimized,
        &work_request,
        statement_kind,
    ))
}

pub(super) fn explain_analyze_output_row(
    optimized: &OptimizedQueryPlan,
    work_request: WorkRequest,
    statement_kind: &'static str,
    row_count: usize,
    profile: &executor::ReadExecutionProfile,
) -> Row {
    owner_explain_analyze_output_row(
        explain_json_input(optimized, &work_request, statement_kind),
        row_count,
        profile,
    )
}

pub(super) fn empty_read_execution_profile() -> executor::ReadExecutionProfile {
    owner_empty_read_execution_profile()
}
