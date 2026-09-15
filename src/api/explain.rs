use super::plan_cache::OptimizedQueryPlan;
use crate::executor::{self, Row};
use crate::qos::WorkRequest;
use skein_explain::json::{
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
