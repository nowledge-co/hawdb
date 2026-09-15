#![deny(unsafe_code)]

//! Explain output contracts and terminal formatting for embedded query clients.

#[doc(hidden)]
pub mod json;

use skein_executor::{QueryOutput, ReadExecutionProfile};
use skein_optimizer::{OptimizerTrace, PhysicalOperatorId, PhysicalPlanKind};
use skein_plan::{PhysicalPlan, PhysicalPlanChildren};
use skein_plan_cache::PlanCacheLookup;
use skein_qos::WorkRequest;
use skein_storage::ScanPruningReport;
use std::fmt::{Display, Formatter, Write};
use unicode_width::UnicodeWidthStr;

const NOT_AVAILABLE: &str = "N/A";

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainOutput {
    pub physical_plan: PhysicalPlan,
    pub trace: OptimizerTrace,
    pub work_request: WorkRequest,
    pub plan_cache_lookup: PlanCacheLookup,
    pub statement_kind: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainAnalyzeOutput {
    pub output: QueryOutput,
    pub execution_profile: ReadExecutionProfile<ScanPruningReport>,
    pub physical_plan: PhysicalPlan,
    pub trace: OptimizerTrace,
    pub work_request: WorkRequest,
    pub plan_cache_lookup: PlanCacheLookup,
    pub statement_kind: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeGraphExplainOutput {
    pub plan: String,
    pub trace: OptimizerTrace,
    pub work_request: WorkRequest,
    pub plan_cache_lookup: PlanCacheLookup,
    pub statement_kind: &'static str,
}

impl Display for ExplainOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let rows = plan_rows(&self.physical_plan, |node, operator_id, _is_root| {
            ExplainRow {
                id: String::new(),
                estimated_rows: estimated_rows(&self.trace, operator_id, node.kind()),
                actual_rows: None,
                task: "root".to_string(),
                access_object: access_object(node),
                execution_info: None,
                operator_info: operator_info(node),
                memory: None,
                disk: None,
            }
        });
        write_table(
            formatter,
            &["id", "estRows", "task", "access object", "operator info"],
            &rows
                .iter()
                .map(|row| {
                    vec![
                        row.id.as_str(),
                        optional_text(&row.estimated_rows),
                        row.task.as_str(),
                        row.access_object.as_str(),
                        row.operator_info.as_str(),
                    ]
                })
                .collect::<Vec<_>>(),
        )?;
        write!(
            formatter,
            "\noptimizer: mode={}, groups={}, cost={}, cache={}\nquery digest: {}\nplan shape: {}",
            self.trace.search_mode.as_str(),
            self.trace.groups,
            self.trace.selected_plan_cost.cost,
            self.plan_cache_lookup.as_str(),
            self.trace.query_digest.as_deref().unwrap_or(NOT_AVAILABLE),
            self.trace.selected_plan_fingerprint,
        )
    }
}

impl Display for ExplainAnalyzeOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let rows = plan_rows(&self.physical_plan, |node, operator_id, is_root| {
            let blocking = self
                .execution_profile
                .blocking_operator_memory_reports
                .iter()
                .find(|report| report.operator == node.kind().as_str());
            ExplainRow {
                id: String::new(),
                estimated_rows: estimated_rows(&self.trace, operator_id, node.kind()),
                actual_rows: actual_rows(&self.execution_profile, operator_id, node.kind()),
                task: "root".to_string(),
                access_object: access_object(node),
                execution_info: if is_root {
                    Some(root_execution_info(&self.execution_profile))
                } else {
                    blocking.map(|report| format!("input_rows={}", report.input_rows))
                },
                operator_info: operator_info(node),
                memory: blocking
                    .map(|report| {
                        format!(
                            "peak={}/budget={}",
                            format_bytes(report.peak_tracked_bytes as u64),
                            format_bytes(report.budget_bytes as u64),
                        )
                    })
                    .or_else(|| {
                        is_root.then(|| {
                            let pipeline = &self.execution_profile.pipeline_memory_report;
                            format!(
                                "peak={}/budget={}",
                                format_bytes(pipeline.query_memory_peak_bytes as u64),
                                format_bytes(pipeline.query_memory_budget_bytes as u64),
                            )
                        })
                    }),
                disk: blocking.and_then(|report| {
                    (report.spill_run_count > 0).then(|| {
                        format!(
                            "runs={}/{}, rows={}, bytes={}/{}",
                            report.spill_run_count,
                            report.max_spill_runs,
                            report.spilled_rows,
                            format_bytes(report.spilled_bytes),
                            format_bytes(report.max_spill_bytes),
                        )
                    })
                }),
            }
        });
        write_table(
            formatter,
            &[
                "id",
                "estRows",
                "actRows",
                "task",
                "access object",
                "execution info",
                "operator info",
                "memory",
                "disk",
            ],
            &rows
                .iter()
                .map(|row| {
                    vec![
                        row.id.as_str(),
                        optional_text(&row.estimated_rows),
                        optional_text(&row.actual_rows),
                        row.task.as_str(),
                        row.access_object.as_str(),
                        optional_text(&row.execution_info),
                        row.operator_info.as_str(),
                        optional_text(&row.memory),
                        optional_text(&row.disk),
                    ]
                })
                .collect::<Vec<_>>(),
        )?;
        write!(
            formatter,
            "\noptimizer: mode={}, groups={}, cost={}, cache={}\nquery digest: {}\nplan shape: {}",
            self.trace.search_mode.as_str(),
            self.trace.groups,
            self.trace.selected_plan_cost.cost,
            self.plan_cache_lookup.as_str(),
            self.trace.query_digest.as_deref().unwrap_or(NOT_AVAILABLE),
            self.trace.selected_plan_fingerprint,
        )
    }
}

#[derive(Debug)]
struct ExplainRow {
    id: String,
    estimated_rows: Option<String>,
    actual_rows: Option<String>,
    task: String,
    access_object: String,
    execution_info: Option<String>,
    operator_info: String,
    memory: Option<String>,
    disk: Option<String>,
}

fn plan_rows(
    plan: &PhysicalPlan,
    mut make_row: impl FnMut(&PhysicalPlan, PhysicalOperatorId, bool) -> ExplainRow,
) -> Vec<ExplainRow> {
    fn visit(
        plan: &PhysicalPlan,
        is_root: bool,
        next_operator_ordinal: &mut usize,
        ancestors_have_sibling: &mut Vec<bool>,
        is_last: bool,
        make_row: &mut dyn FnMut(&PhysicalPlan, PhysicalOperatorId, bool) -> ExplainRow,
        rows: &mut Vec<ExplainRow>,
    ) {
        let operator_id = PhysicalOperatorId::from_ordinal(*next_operator_ordinal);
        *next_operator_ordinal = next_operator_ordinal
            .checked_add(1)
            .expect("physical plan operator count exceeds usize");
        let mut row = make_row(plan, operator_id, is_root);
        row.id = tree_identifier(
            plan.kind().as_str(),
            is_root,
            ancestors_have_sibling,
            is_last,
        );
        rows.push(row);

        match plan.children() {
            PhysicalPlanChildren::None => {}
            PhysicalPlanChildren::Unary(child) => {
                ancestors_have_sibling.push(false);
                visit(
                    child,
                    false,
                    next_operator_ordinal,
                    ancestors_have_sibling,
                    true,
                    make_row,
                    rows,
                );
                ancestors_have_sibling.pop();
            }
            PhysicalPlanChildren::Binary(left, right) => {
                ancestors_have_sibling.push(true);
                visit(
                    left,
                    false,
                    next_operator_ordinal,
                    ancestors_have_sibling,
                    false,
                    make_row,
                    rows,
                );
                ancestors_have_sibling.pop();
                ancestors_have_sibling.push(false);
                visit(
                    right,
                    false,
                    next_operator_ordinal,
                    ancestors_have_sibling,
                    true,
                    make_row,
                    rows,
                );
                ancestors_have_sibling.pop();
            }
        }
    }

    let mut rows = Vec::new();
    let mut next_operator_ordinal = 0;
    visit(
        plan,
        true,
        &mut next_operator_ordinal,
        &mut Vec::new(),
        true,
        &mut make_row,
        &mut rows,
    );
    rows
}

fn estimated_rows(
    trace: &OptimizerTrace,
    operator_id: PhysicalOperatorId,
    operator: PhysicalPlanKind,
) -> Option<String> {
    trace
        .selected_plan_cardinality_estimates
        .iter()
        .find(|estimate| estimate.operator_id == operator_id && estimate.operator == operator)
        .map(|estimate| format_estimated_rows(estimate.estimated_rows))
}

fn actual_rows(
    profile: &ReadExecutionProfile<ScanPruningReport>,
    operator_id: PhysicalOperatorId,
    operator: PhysicalPlanKind,
) -> Option<String> {
    profile
        .operator_cardinality_profiles
        .iter()
        .find(|cardinality| {
            cardinality.operator_id == operator_id && cardinality.operator == operator
        })
        .and_then(|cardinality| cardinality.actual_rows)
        .map(|rows| rows.to_string())
}

fn tree_identifier(
    kind: &str,
    is_root: bool,
    ancestors_have_sibling: &[bool],
    is_last: bool,
) -> String {
    if is_root {
        return kind.to_string();
    }
    let mut id = String::new();
    for has_sibling in ancestors_have_sibling
        .iter()
        .take(ancestors_have_sibling.len().saturating_sub(1))
    {
        id.push_str(if *has_sibling { "│ " } else { "  " });
    }
    id.push_str(if is_last { "└─" } else { "├─" });
    id.push_str(kind);
    id
}

fn access_object(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. }
        | PhysicalPlan::NodeProjectionScanExec { label, .. } => format!("label:{label}"),
        PhysicalPlan::SourceSegmentScan { .. } => "source-segments".to_string(),
        PhysicalPlan::NodeColumnLookupExec {
            label,
            property,
            column,
            ..
        } => format!("label:{label}, property:{property}, column:{column}"),
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            label, property, ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            label, property, ..
        } => format!("label:{label}, index:{property}"),
        PhysicalPlan::IndexNodeUnionSeek {
            label, branches, ..
        } => format!(
            "label:{label}, indexes:{}",
            branches
                .iter()
                .map(|branch| branch.property.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => format!(
            "label:{label}, index:{}",
            predicates
                .iter()
                .map(|(property, _)| property.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ),
        PhysicalPlan::IndexNodeCompositeRangeSeek { label, seek, .. } => {
            format!("label:{label}, index:{}", seek.index_properties.join(","))
        }
        PhysicalPlan::AdjacencyExpandExec { rel_type, .. }
        | PhysicalPlan::OptionalDegreeExec { rel_type, .. }
        | PhysicalPlan::ShortestPathExec { rel_type, .. } => format!("rel:{rel_type}"),
        PhysicalPlan::GraphAlgorithm { graph_name, .. } => format!("graph:{graph_name}"),
        _ => String::new(),
    }
}

fn operator_info(plan: &PhysicalPlan) -> String {
    let kind = plan.kind().as_str();
    plan.explain(0)
        .lines()
        .next()
        .unwrap_or(kind)
        .strip_prefix(kind)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn root_execution_info(profile: &ReadExecutionProfile<ScanPruningReport>) -> String {
    let pipeline = &profile.pipeline_memory_report;
    let mut fields = vec![
        format!("output_rows={}", pipeline.output_rows),
        format!("output_bytes={}", pipeline.output_payload_bytes),
        format!("intermediate_rows={}", pipeline.intermediate_rows),
        format!(
            "query_memory={}/{}, completion={}",
            format_bytes(pipeline.query_memory_peak_bytes as u64),
            format_bytes(pipeline.query_memory_budget_bytes as u64),
            format_bytes(pipeline.query_memory_completion_bytes as u64),
        ),
    ];
    if pipeline.columnar_batches > 0 {
        fields.push(format!(
            "columnar_batches={}, columnar_rows={}/{}, morsels={}, workers={}/{}, buffered_morsels={}/{}, reorder={}",
            pipeline.columnar_batches,
            pipeline.columnar_selected_rows,
            pipeline.columnar_input_rows,
            pipeline.morsel_count,
            pipeline.morsel_peak_active_workers,
            pipeline.morsel_max_admitted_workers,
            pipeline.morsel_peak_buffered_outputs,
            format_bytes(pipeline.morsel_peak_buffered_output_bytes as u64),
            pipeline.morsel_peak_reorder_entries,
        ));
    }
    if let Some(peak_resident_bytes) = pipeline.peak_resident_bytes {
        fields.push(format!("peak_rss={}", format_bytes(peak_resident_bytes)));
    }
    if let Some(resident_growth_bytes) = pipeline.steady_resident_growth_bytes {
        fields.push(format!(
            "steady_rss_growth={}",
            format_bytes(resident_growth_bytes)
        ));
    }
    if let Some(peak_growth_bytes) = pipeline.lifetime_peak_resident_growth_bytes {
        fields.push(format!(
            "lifetime_peak_rss_growth={}",
            format_bytes(peak_growth_bytes)
        ));
    }
    if let Some(total_page_faults) = pipeline.total_page_faults {
        fields.push(format!("total_faults={total_page_faults}"));
    }
    if let Some(minor_page_faults) = pipeline.minor_page_faults {
        fields.push(format!("minor_faults={minor_page_faults}"));
    }
    if let Some(major_page_faults) = pipeline.major_page_faults {
        fields.push(format!("major_faults={major_page_faults}"));
    }
    fields.join(", ")
}

fn format_estimated_rows(rows: u64) -> String {
    format!("{rows}.00")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn optional_text(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or(NOT_AVAILABLE)
}

fn write_table(
    formatter: &mut Formatter<'_>,
    headers: &[&str],
    rows: &[Vec<&str>],
) -> std::fmt::Result {
    let widths = headers
        .iter()
        .enumerate()
        .map(|(column, header)| {
            rows.iter()
                .map(|row| display_width(row[column]))
                .max()
                .unwrap_or_default()
                .max(display_width(header))
        })
        .collect::<Vec<_>>();

    write_border(formatter, &widths)?;
    write_cells(formatter, headers, &widths)?;
    write_border(formatter, &widths)?;
    for row in rows {
        write_cells(formatter, row, &widths)?;
    }
    write_border(formatter, &widths)
}

fn write_border(formatter: &mut Formatter<'_>, widths: &[usize]) -> std::fmt::Result {
    formatter.write_char('+')?;
    for width in widths {
        for _ in 0..width.saturating_add(2) {
            formatter.write_char('-')?;
        }
        formatter.write_char('+')?;
    }
    formatter.write_char('\n')
}

fn write_cells(
    formatter: &mut Formatter<'_>,
    cells: &[impl AsRef<str>],
    widths: &[usize],
) -> std::fmt::Result {
    formatter.write_char('|')?;
    for (cell, width) in cells.iter().zip(widths) {
        let cell = cell.as_ref();
        write!(
            formatter,
            " {cell}{} |",
            " ".repeat(width.saturating_sub(display_width(cell)))
        )?;
    }
    formatter.write_char('\n')
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

#[cfg(test)]
mod tests {
    use super::{display_width, format_bytes};

    #[test]
    fn formats_binary_byte_units_without_platform_dependencies() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn aligns_wide_unicode_by_terminal_column_width() {
        assert_eq!(display_width("Memory"), 6);
        assert_eq!(display_width("记忆"), 4);
    }
}
