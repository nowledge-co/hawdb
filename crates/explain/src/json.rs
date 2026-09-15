use skein_core::Value;
use skein_executor::{
    observer::{
        blocking_operator_memory_report_value, graph_expansion_report_value,
        pipeline_memory_report_value, scan_pruning_report_value, vector_execution_report_value,
    },
    PipelineMemoryReport, ReadExecutionProfile, Row,
};
use skein_optimizer::{
    Distribution, OptimizerTrace, PhysicalProperties, PlanCost, PlanCostBreakdown, StageTrace,
};
use skein_plan::{visit_plan, NodeProjectionAccess, PhysicalPlan};
use skein_plan_cache::PlanCacheLookup;
use skein_qos::WorkRequest;
use skein_storage::ScanPruningReport;
use std::collections::BTreeMap;

/// Inputs for structured embedded `EXPLAIN` output.
///
/// This is intentionally independent of the root database facade so every
/// field is derived from the owned plan, optimizer, executor, and QoS crates.
#[doc(hidden)]
pub struct ExplainJsonInput<'a> {
    pub physical_plan: &'a PhysicalPlan,
    pub trace: &'a OptimizerTrace,
    pub plan_cache_lookup: PlanCacheLookup,
    pub configured_max_optimizer_groups: Option<usize>,
    pub effective_max_optimizer_groups: usize,
    pub work_request: &'a WorkRequest,
    pub statement_kind: &'static str,
}

/// Builds the structured row returned for `EXPLAIN`.
#[doc(hidden)]
pub fn explain_output_row(input: &ExplainJsonInput<'_>) -> Row {
    let mut row = Row::new();
    row.insert("mode".to_string(), Value::String("explain".to_string()));
    row.insert(
        "statement_kind".to_string(),
        Value::String(input.statement_kind.to_string()),
    );
    row.insert(
        "plan".to_string(),
        Value::String(input.physical_plan.explain(0)),
    );
    row.insert(
        "selected_plan".to_string(),
        Value::String(input.trace.selected_plan.clone()),
    );
    row.insert(
        "selected_plan_fingerprint".to_string(),
        Value::String(input.trace.selected_plan_fingerprint.clone()),
    );
    row.insert(
        "query_digest".to_string(),
        input
            .trace
            .query_digest
            .as_ref()
            .map(|digest| Value::String(digest.clone()))
            .unwrap_or(Value::Null),
    );
    row.insert(
        "selected_plan_cost".to_string(),
        explain_plan_cost_value(input.trace.selected_plan_cost),
    );
    row.insert(
        "selected_plan_cost_breakdown".to_string(),
        explain_plan_cost_breakdown_value(input.trace.selected_plan_cost_breakdown),
    );
    row.insert(
        "selected_plan_properties".to_string(),
        explain_physical_properties_value(&input.trace.selected_plan_properties),
    );
    row.insert(
        "operator_cardinalities".to_string(),
        operator_cardinalities_value(input.trace, None),
    );
    row.insert(
        "optimizer_stages".to_string(),
        explain_optimizer_stages_value(&input.trace.stage_events),
    );
    row.insert(
        "semantic_checks".to_string(),
        explain_semantic_checks_value(),
    );
    row.insert("fast_path".to_string(), explain_fast_path_value(input));
    row.insert(
        "optimizer_budget".to_string(),
        explain_optimizer_budget_value(input),
    );
    row.insert(
        "chosen_indexes".to_string(),
        explain_chosen_indexes_value(input),
    );
    row.insert(
        "plan_cache_lookup".to_string(),
        Value::String(input.plan_cache_lookup.as_str().to_string()),
    );
    if let Some(reason) = input.plan_cache_lookup.bypass_reason() {
        row.insert(
            "plan_cache_bypass_reason".to_string(),
            Value::String(reason.as_str().to_string()),
        );
    } else {
        row.insert("plan_cache_bypass_reason".to_string(), Value::Null);
    }
    row.insert(
        "work_request".to_string(),
        explain_work_request_value(input.work_request),
    );
    row.insert(
        "resource_class".to_string(),
        Value::String(input.work_request.class.as_str().to_string()),
    );
    row
}

/// Builds the structured row returned for `EXPLAIN ANALYZE`.
#[doc(hidden)]
pub fn explain_analyze_output_row(
    input: ExplainJsonInput<'_>,
    row_count: usize,
    profile: &ReadExecutionProfile<ScanPruningReport>,
) -> Row {
    let mut row = explain_output_row(&input);
    row.insert(
        "mode".to_string(),
        Value::String("explain_analyze".to_string()),
    );
    row.insert("row_count".to_string(), usize_value(row_count));
    row.insert(
        "operator_cardinalities".to_string(),
        operator_cardinalities_value(input.trace, Some(profile)),
    );
    row.insert(
        "scan_pruning_report_count".to_string(),
        usize_value(profile.scan_pruning_reports.len()),
    );
    row.insert(
        "scan_pruning_reports".to_string(),
        Value::List(
            profile
                .scan_pruning_reports
                .iter()
                .map(scan_pruning_report_value)
                .collect(),
        ),
    );
    row.insert(
        "vector_execution_report_count".to_string(),
        usize_value(profile.vector_execution_reports.len()),
    );
    row.insert(
        "vector_execution_reports".to_string(),
        Value::List(
            profile
                .vector_execution_reports
                .iter()
                .map(vector_execution_report_value)
                .collect(),
        ),
    );
    row.insert(
        "graph_expansion_report_count".to_string(),
        usize_value(profile.graph_expansion_reports.len()),
    );
    row.insert(
        "graph_expansion_reports".to_string(),
        Value::List(
            profile
                .graph_expansion_reports
                .iter()
                .map(graph_expansion_report_value)
                .collect(),
        ),
    );
    row.insert(
        "blocking_operator_memory_reports".to_string(),
        Value::List(
            profile
                .blocking_operator_memory_reports
                .iter()
                .map(blocking_operator_memory_report_value)
                .collect(),
        ),
    );
    row.insert(
        "pipeline_memory_report".to_string(),
        pipeline_memory_report_value(&profile.pipeline_memory_report),
    );
    row.insert(
        "row_limit_enforced_before_output".to_string(),
        Value::Bool(profile.row_limit_enforced_before_output),
    );
    row.insert(
        "operator_row_cap_enabled".to_string(),
        Value::Bool(profile.operator_row_cap_enabled),
    );
    row
}

/// Returns an empty execution profile for `EXPLAIN` paths without execution.
#[doc(hidden)]
pub fn empty_read_execution_profile() -> ReadExecutionProfile<ScanPruningReport> {
    ReadExecutionProfile {
        max_rows: None,
        detection_row_cap: None,
        row_limit_enforced_before_output: false,
        operator_row_cap_enabled: false,
        operator_cardinality_profiles: Vec::new(),
        blocking_operator_kinds: Vec::new(),
        scan_pruning_reports: Vec::new(),
        vector_execution_reports: Vec::new(),
        graph_expansion_reports: Vec::new(),
        blocking_operator_memory_reports: Vec::new(),
        pipeline_memory_report: PipelineMemoryReport::default(),
    }
}

fn operator_cardinalities_value(
    trace: &OptimizerTrace,
    profile: Option<&ReadExecutionProfile<ScanPruningReport>>,
) -> Value {
    Value::List(
        trace
            .selected_plan_cardinality_estimates
            .iter()
            .map(|estimate| {
                let actual_rows = profile
                    .and_then(|profile| {
                        profile.operator_cardinality_profiles.iter().find(|actual| {
                            actual.operator_id == estimate.operator_id
                                && actual.operator == estimate.operator
                        })
                    })
                    .and_then(|actual| actual.actual_rows)
                    .map(usize_value)
                    .unwrap_or(Value::Null);
                Value::Map(BTreeMap::from([
                    (
                        "operator_id".to_string(),
                        usize_value(estimate.operator_id.ordinal()),
                    ),
                    (
                        "operator".to_string(),
                        Value::String(estimate.operator.as_str().to_string()),
                    ),
                    (
                        "estimated_rows".to_string(),
                        u64_value(estimate.estimated_rows),
                    ),
                    ("actual_rows".to_string(), actual_rows),
                ]))
            })
            .collect(),
    )
}

fn explain_plan_cost_value(cost: PlanCost) -> Value {
    Value::Map(BTreeMap::from([
        ("estimated_rows".to_string(), u64_value(cost.estimated_rows)),
        ("cost".to_string(), u64_value(cost.cost)),
    ]))
}

fn explain_plan_cost_breakdown_value(cost: PlanCostBreakdown) -> Value {
    Value::Map(BTreeMap::from([
        ("estimated_rows".to_string(), u64_value(cost.estimated_rows)),
        ("cost".to_string(), u64_value(cost.cost)),
        ("cpu".to_string(), u64_value(cost.cpu)),
        ("random_io".to_string(), u64_value(cost.random_io)),
        ("sequential_io".to_string(), u64_value(cost.sequential_io)),
        ("output_rows".to_string(), u64_value(cost.output_rows)),
    ]))
}

fn explain_optimizer_stages_value(stages: &[StageTrace]) -> Value {
    Value::List(stages.iter().map(explain_optimizer_stage_value).collect())
}

fn explain_optimizer_stage_value(stage: &StageTrace) -> Value {
    let stats = stage.stats();
    Value::Map(BTreeMap::from([
        ("name".to_string(), Value::String(stage.name().to_string())),
        (
            "apply_order".to_string(),
            Value::String(stage.apply_order().as_str().to_string()),
        ),
        ("input_count".to_string(), usize_value(stats.input_count)),
        ("output_count".to_string(), usize_value(stats.output_count)),
        (
            "applied_rules".to_string(),
            usize_value(stats.applied_rules),
        ),
        (
            "skipped_rules".to_string(),
            usize_value(stats.skipped_rules),
        ),
    ]))
}

fn explain_physical_properties_value(properties: &PhysicalProperties) -> Value {
    Value::Map(BTreeMap::from([
        (
            "distribution".to_string(),
            explain_distribution_value(&properties.distribution),
        ),
        (
            "ordering".to_string(),
            Value::List(
                properties
                    .ordering
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        (
            "covering_fields".to_string(),
            Value::List(
                properties
                    .covering_fields
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        (
            "scan_pruning".to_string(),
            Value::String(properties.scan_pruning.as_str().to_string()),
        ),
        (
            "vector_precision".to_string(),
            Value::String(properties.vector_precision.as_str().to_string()),
        ),
        (
            "memory_budget".to_string(),
            Value::String(properties.memory_budget.as_str().to_string()),
        ),
    ]))
}

fn explain_distribution_value(distribution: &Distribution) -> Value {
    let keys = match distribution {
        Distribution::Hash(keys) => keys.clone(),
        Distribution::Any | Distribution::Single => Vec::new(),
    };
    Value::Map(BTreeMap::from([
        (
            "kind".to_string(),
            Value::String(distribution.as_str().to_string()),
        ),
        (
            "keys".to_string(),
            Value::List(keys.into_iter().map(Value::String).collect()),
        ),
    ]))
}

fn explain_semantic_checks_value() -> Value {
    Value::Map(BTreeMap::from([
        ("parse".to_string(), Value::String("passed".to_string())),
        (
            "parameter_binding".to_string(),
            Value::String("passed".to_string()),
        ),
        (
            "semantic_validation".to_string(),
            Value::String("passed".to_string()),
        ),
    ]))
}

fn explain_fast_path_value(input: &ExplainJsonInput<'_>) -> Value {
    let reason = explain_fast_path_reason(input);
    Value::Map(BTreeMap::from([
        ("selected".to_string(), Value::Bool(reason.is_some())),
        (
            "reason".to_string(),
            reason
                .map(|reason| Value::String(reason.to_string()))
                .unwrap_or(Value::Null),
        ),
    ]))
}

fn explain_fast_path_reason(input: &ExplainJsonInput<'_>) -> Option<&'static str> {
    let mut fused_reason = None;
    visit_plan(input.physical_plan, &mut |plan| {
        let PhysicalPlan::NodeProjectionScanExec {
            access,
            predicate: None,
            ..
        } = plan
        else {
            return;
        };
        fused_reason = match access {
            NodeProjectionAccess::PropertyValues { values, .. } if values.len() == 1 => {
                Some("index_node_seek_without_residual_filter")
            }
            NodeProjectionAccess::PropertyValues { .. } => {
                Some("index_node_multi_seek_without_residual_filter")
            }
            _ => fused_reason,
        };
    });
    if fused_reason.is_some() {
        return fused_reason;
    }
    if input
        .trace
        .selected_plan_operator_counts
        .contains_key("IndexNodeSeek")
        && !input
            .trace
            .selected_plan_operator_counts
            .contains_key("FilterExec")
    {
        return Some("index_node_seek_without_residual_filter");
    }
    if input
        .trace
        .selected_plan_operator_counts
        .contains_key("IndexNodeMultiSeek")
        && !input
            .trace
            .selected_plan_operator_counts
            .contains_key("FilterExec")
    {
        return Some("index_node_multi_seek_without_residual_filter");
    }
    None
}

fn explain_optimizer_budget_value(input: &ExplainJsonInput<'_>) -> Value {
    Value::Map(BTreeMap::from([
        (
            "max_groups".to_string(),
            usize_value(input.effective_max_optimizer_groups),
        ),
        (
            "configured_max_groups".to_string(),
            option_usize_value(input.configured_max_optimizer_groups),
        ),
        ("groups".to_string(), usize_value(input.trace.groups)),
        (
            "search_mode".to_string(),
            Value::String(input.trace.search_mode.as_str().to_string()),
        ),
        (
            "budget_exceeded".to_string(),
            Value::Bool(input.trace.groups > input.effective_max_optimizer_groups),
        ),
    ]))
}

fn explain_chosen_indexes_value(input: &ExplainJsonInput<'_>) -> Value {
    let counts = &input.trace.selected_plan_operator_counts;
    let index_operators = [
        ("IndexNodeSeek", "node_seek"),
        ("IndexNodeMultiSeek", "node_multi_seek"),
        ("IndexNodeUnionSeek", "node_union_seek"),
        ("IndexNodeCompositeSeek", "node_composite_seek"),
        ("IndexNodeCompositeRangeSeek", "node_composite_range_seek"),
        ("IndexNodeRangeSeek", "node_range_seek"),
        ("IndexNodeTextSeek", "node_text_seek"),
    ];
    let mut selected = BTreeMap::<&'static str, usize>::new();
    for (operator, _) in index_operators {
        if let Some(count) = counts.get(operator) {
            selected.insert(operator, *count);
        }
    }
    visit_plan(input.physical_plan, &mut |plan| {
        let PhysicalPlan::NodeProjectionScanExec { access, .. } = plan else {
            return;
        };
        if !access.is_label_scan() {
            *selected.entry(access.physical_operator_name()).or_default() += 1;
        }
    });
    Value::List(
        index_operators
            .into_iter()
            .filter_map(|(operator, kind)| {
                selected.get(operator).map(|count| {
                    Value::Map(BTreeMap::from([
                        ("operator".to_string(), Value::String(operator.to_string())),
                        ("kind".to_string(), Value::String(kind.to_string())),
                        ("count".to_string(), usize_value(*count)),
                    ]))
                })
            })
            .collect(),
    )
}

fn explain_work_request_value(work_request: &WorkRequest) -> Value {
    Value::Map(BTreeMap::from([
        (
            "priority".to_string(),
            Value::String(work_request.priority.as_str().to_string()),
        ),
        (
            "class".to_string(),
            Value::String(work_request.class.as_str().to_string()),
        ),
        (
            "estimated_operations".to_string(),
            usize_value(work_request.estimated_operations),
        ),
    ]))
}

fn usize_value(value: usize) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn u64_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn option_usize_value(value: Option<usize>) -> Value {
    value.map(usize_value).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::empty_read_execution_profile;

    #[test]
    fn empty_profile_has_no_execution_observations() {
        let profile = empty_read_execution_profile();
        assert!(profile.operator_cardinality_profiles.is_empty());
        assert!(profile.scan_pruning_reports.is_empty());
        assert_eq!(profile.pipeline_memory_report.output_rows, 0);
        assert!(!profile.row_limit_enforced_before_output);
    }
}
