//! Value encoding for execution reports exposed through the embedded facade.

use crate::{
    BlockingOperatorMemoryReport, GraphExpansionExecutionReport, PipelineMemoryReport,
    VectorExecutionReport,
};
use skein_core::Value;
use skein_storage::{ScanPruningReport, ScanPruningStrategy};
use std::collections::BTreeMap;

pub fn blocking_operator_memory_report_value(report: &BlockingOperatorMemoryReport) -> Value {
    Value::Map(BTreeMap::from([
        (
            "operator".to_string(),
            Value::String(report.operator.clone()),
        ),
        ("budget_bytes".to_string(), usize_value(report.budget_bytes)),
        (
            "peak_tracked_bytes".to_string(),
            usize_value(report.peak_tracked_bytes),
        ),
        ("input_rows".to_string(), usize_value(report.input_rows)),
        (
            "max_spill_bytes".to_string(),
            u64_value(report.max_spill_bytes),
        ),
        (
            "max_spill_runs".to_string(),
            usize_value(report.max_spill_runs),
        ),
        ("spilled_bytes".to_string(), u64_value(report.spilled_bytes)),
        (
            "spill_run_count".to_string(),
            usize_value(report.spill_run_count),
        ),
        ("spilled_rows".to_string(), usize_value(report.spilled_rows)),
    ]))
}

pub fn pipeline_memory_report_value(report: &PipelineMemoryReport) -> Value {
    Value::Map(BTreeMap::from([
        (
            "query_memory_budget_bytes".to_string(),
            usize_value(report.query_memory_budget_bytes),
        ),
        (
            "query_memory_peak_bytes".to_string(),
            usize_value(report.query_memory_peak_bytes),
        ),
        (
            "query_memory_completion_bytes".to_string(),
            usize_value(report.query_memory_completion_bytes),
        ),
        (
            "query_memory_account_count".to_string(),
            usize_value(report.query_memory_account_count),
        ),
        (
            "intermediate_rows".to_string(),
            usize_value(report.intermediate_rows),
        ),
        (
            "intermediate_payload_bytes".to_string(),
            usize_value(report.intermediate_payload_bytes),
        ),
        (
            "peak_batch_rows".to_string(),
            usize_value(report.peak_batch_rows),
        ),
        (
            "peak_batch_payload_bytes".to_string(),
            usize_value(report.peak_batch_payload_bytes),
        ),
        (
            "columnar_batches".to_string(),
            usize_value(report.columnar_batches),
        ),
        (
            "columnar_input_rows".to_string(),
            usize_value(report.columnar_input_rows),
        ),
        (
            "columnar_selected_rows".to_string(),
            usize_value(report.columnar_selected_rows),
        ),
        ("morsel_count".to_string(), usize_value(report.morsel_count)),
        (
            "morsel_max_admitted_workers".to_string(),
            usize_value(report.morsel_max_admitted_workers),
        ),
        (
            "morsel_peak_active_workers".to_string(),
            usize_value(report.morsel_peak_active_workers),
        ),
        (
            "morsel_peak_buffered_outputs".to_string(),
            usize_value(report.morsel_peak_buffered_outputs),
        ),
        (
            "morsel_peak_buffered_output_bytes".to_string(),
            usize_value(report.morsel_peak_buffered_output_bytes),
        ),
        (
            "morsel_peak_reorder_entries".to_string(),
            usize_value(report.morsel_peak_reorder_entries),
        ),
        ("output_rows".to_string(), usize_value(report.output_rows)),
        (
            "output_payload_bytes".to_string(),
            usize_value(report.output_payload_bytes),
        ),
        (
            "start_resident_bytes".to_string(),
            optional_u64_value(report.start_resident_bytes),
        ),
        (
            "start_peak_resident_bytes".to_string(),
            optional_u64_value(report.start_peak_resident_bytes),
        ),
        (
            "steady_resident_bytes".to_string(),
            optional_u64_value(report.steady_resident_bytes),
        ),
        (
            "peak_resident_bytes".to_string(),
            optional_u64_value(report.peak_resident_bytes),
        ),
        (
            "steady_resident_growth_bytes".to_string(),
            optional_u64_value(report.steady_resident_growth_bytes),
        ),
        (
            "lifetime_peak_resident_growth_bytes".to_string(),
            optional_u64_value(report.lifetime_peak_resident_growth_bytes),
        ),
        (
            "total_page_faults".to_string(),
            optional_u64_value(report.total_page_faults),
        ),
        (
            "minor_page_faults".to_string(),
            optional_u64_value(report.minor_page_faults),
        ),
        (
            "major_page_faults".to_string(),
            optional_u64_value(report.major_page_faults),
        ),
    ]))
}

pub fn graph_expansion_report_value(report: &GraphExpansionExecutionReport) -> Value {
    Value::Map(BTreeMap::from([
        ("seed_count".to_string(), usize_value(report.seed_count)),
        (
            "expanded_node_count".to_string(),
            usize_value(report.expanded_node_count),
        ),
        (
            "expanded_edge_count".to_string(),
            usize_value(report.expanded_edge_count),
        ),
        (
            "relation_types".to_string(),
            Value::List(
                report
                    .relation_types
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        ("min_hops".to_string(), usize_value(report.min_hops)),
        ("max_hops".to_string(), usize_value(report.max_hops)),
        (
            "reranked_seed_count".to_string(),
            usize_value(report.reranked_seed_count),
        ),
        (
            "candidate_limit".to_string(),
            usize_value(report.candidate_limit),
        ),
        (
            "payload_byte_limit".to_string(),
            usize_value(report.payload_byte_limit),
        ),
        (
            "payload_bytes_used".to_string(),
            usize_value(report.payload_bytes_used),
        ),
        (
            "returned_count".to_string(),
            usize_value(report.returned_count),
        ),
        ("truncated".to_string(), Value::Bool(report.truncated())),
        (
            "truncation_reason".to_string(),
            report
                .truncation_reason
                .map(|reason| Value::String(reason.as_str().to_string()))
                .unwrap_or(Value::Null),
        ),
    ]))
}

pub fn vector_execution_report_value(report: &VectorExecutionReport) -> Value {
    Value::Map(BTreeMap::from([
        (
            "backend".to_string(),
            Value::String(report.backend.as_str().to_string()),
        ),
        (
            "compression_mode".to_string(),
            Value::String(report.compression_mode.as_str().to_string()),
        ),
        (
            "candidate_source".to_string(),
            Value::String(report.candidate_source.as_str().to_string()),
        ),
        (
            "backend_selection_reason".to_string(),
            report
                .backend_selection_reason
                .map(|reason| Value::String(reason.as_str().to_string()))
                .unwrap_or(Value::Null),
        ),
        (
            "estimated_raw_vector_bytes".to_string(),
            report
                .estimated_raw_vector_bytes
                .map(u64_value)
                .unwrap_or(Value::Null),
        ),
        (
            "filter_selectivity_per_million".to_string(),
            report
                .filter_selectivity_per_million
                .map(|value| u64_value(u64::from(value)))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_score_source".to_string(),
            Value::String(report.candidate_score_source.as_str().to_string()),
        ),
        (
            "final_score_source".to_string(),
            Value::String(report.final_score_source.as_str().to_string()),
        ),
        (
            "generated_candidate_count".to_string(),
            usize_value(report.generated_candidate_count),
        ),
        (
            "descriptor_pruned_count".to_string(),
            usize_value(report.descriptor_pruned_count),
        ),
        (
            "scalar_filtered_count".to_string(),
            usize_value(report.scalar_filtered_count),
        ),
        (
            "residual_filtered_count".to_string(),
            usize_value(report.residual_filtered_count),
        ),
        (
            "candidate_scan_rounds".to_string(),
            usize_value(report.candidate_scan_rounds),
        ),
        (
            "reranked_candidate_count".to_string(),
            usize_value(report.reranked_candidate_count),
        ),
        (
            "returned_count".to_string(),
            usize_value(report.returned_count),
        ),
        (
            "raw_vector_bytes_read".to_string(),
            u64_value(report.raw_vector_bytes_read),
        ),
        (
            "candidate_scan_kernel".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| Value::String(metrics.kernel.clone()))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_worker_count".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| usize_value(metrics.worker_count))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_segment_count".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| usize_value(metrics.segment_count))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_scanned_block_count".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| usize_value(metrics.scanned_block_count))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_skipped_block_count".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| usize_value(metrics.skipped_block_count))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_payload_bytes_read".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| u64_value(metrics.payload_bytes_read))
                .unwrap_or(Value::Null),
        ),
        (
            "candidate_scan_admitted_working_bytes".to_string(),
            report
                .candidate_scan_metrics
                .as_ref()
                .map(|metrics| usize_value(metrics.admitted_working_bytes))
                .unwrap_or(Value::Null),
        ),
        (
            "index_covered_document_count".to_string(),
            report
                .index_covered_document_count
                .map(usize_value)
                .unwrap_or(Value::Null),
        ),
        (
            "index_candidate_document_count".to_string(),
            report
                .index_candidate_document_count
                .map(usize_value)
                .unwrap_or(Value::Null),
        ),
        (
            "index_coverage_complete".to_string(),
            report
                .index_coverage_complete
                .map(Value::Bool)
                .unwrap_or(Value::Null),
        ),
        (
            "fallback_reason_codes".to_string(),
            Value::List(
                report
                    .fallback_reason_codes
                    .iter()
                    .map(|code| Value::String(code.as_str().to_string()))
                    .collect(),
            ),
        ),
    ]))
}

pub fn scan_pruning_report_value(report: &ScanPruningReport) -> Value {
    Value::Map(BTreeMap::from([
        (
            "target_kind".to_string(),
            Value::String(report.target_kind.as_str().to_string()),
        ),
        (
            "label_id".to_string(),
            report
                .label_id
                .map(|label_id| Value::Int(i64::from(label_id.0)))
                .unwrap_or(Value::Null),
        ),
        (
            "rel_type_id".to_string(),
            report
                .rel_type_id
                .map(|rel_type_id| Value::Int(i64::from(rel_type_id.0)))
                .unwrap_or(Value::Null),
        ),
        (
            "strategy".to_string(),
            scan_pruning_strategy_value(&report.strategy),
        ),
        ("pruned".to_string(), Value::Bool(report.pruned)),
        ("exact_empty".to_string(), Value::Bool(report.exact_empty)),
        (
            "candidate_count_before_pruning".to_string(),
            usize_value(report.candidate_count_before_pruning),
        ),
        (
            "pruned_candidate_count".to_string(),
            usize_value(report.pruned_candidate_count),
        ),
        (
            "candidate_count_before_filter".to_string(),
            usize_value(report.candidate_count_before_filter),
        ),
        ("output_count".to_string(), usize_value(report.output_count)),
        (
            "filtered_out_count".to_string(),
            usize_value(report.filtered_out_count),
        ),
    ]))
}

fn scan_pruning_strategy_value(strategy: &ScanPruningStrategy) -> Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => {
            Value::Map(BTreeMap::from([kind_value_pair("full_label_scan")]))
        }
        ScanPruningStrategy::ExactCount => {
            Value::Map(BTreeMap::from([kind_value_pair("exact_count")]))
        }
        ScanPruningStrategy::Empty => Value::Map(BTreeMap::from([kind_value_pair("empty")])),
        ScanPruningStrategy::IdEq => Value::Map(BTreeMap::from([kind_value_pair("id_eq")])),
        ScanPruningStrategy::IdIn => Value::Map(BTreeMap::from([kind_value_pair("id_in")])),
        ScanPruningStrategy::IdRange => Value::Map(BTreeMap::from([kind_value_pair("id_range")])),
        ScanPruningStrategy::PropertyEq { property } => {
            scan_pruning_property_strategy_value("property_eq", property)
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            scan_pruning_property_strategy_value("property_not_eq", property)
        }
        ScanPruningStrategy::PropertyMissingOrNull { property } => {
            scan_pruning_property_strategy_value("property_missing_or_null", property)
        }
        ScanPruningStrategy::PropertyExists { property } => {
            scan_pruning_property_strategy_value("property_exists", property)
        }
        ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            scan_pruning_property_strategy_value("property_default_if_null_eq", property)
        }
        ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            scan_pruning_property_strategy_value("property_default_if_null_not_eq", property)
        }
        ScanPruningStrategy::PropertyIn { property } => {
            scan_pruning_property_strategy_value("property_in", property)
        }
        ScanPruningStrategy::CompositePropertyEq { properties } => Value::Map(BTreeMap::from([
            kind_value_pair("composite_property_eq"),
            (
                "properties".to_string(),
                Value::List(properties.iter().cloned().map(Value::String).collect()),
            ),
        ])),
        ScanPruningStrategy::CompositePropertyRange { properties } => Value::Map(BTreeMap::from([
            (
                "kind".to_string(),
                Value::String("composite_property_range".to_string()),
            ),
            (
                "properties".to_string(),
                Value::List(properties.iter().cloned().map(Value::String).collect()),
            ),
        ])),
        ScanPruningStrategy::PropertyRange { property } => {
            scan_pruning_property_strategy_value("property_range", property)
        }
        ScanPruningStrategy::FullText { property } => {
            scan_pruning_property_strategy_value("full_text", property)
        }
        ScanPruningStrategy::OrUnion => Value::Map(BTreeMap::from([kind_value_pair("or_union")])),
    }
}

fn scan_pruning_property_strategy_value(kind: &str, property: &str) -> Value {
    Value::Map(BTreeMap::from([
        kind_value_pair(kind),
        ("property".to_string(), Value::String(property.to_string())),
    ]))
}

fn kind_value_pair(kind: &str) -> (String, Value) {
    ("kind".to_string(), Value::String(kind.to_string()))
}

fn usize_value(value: usize) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn u64_value(value: u64) -> Value {
    Value::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

fn optional_u64_value(value: Option<u64>) -> Value {
    value.map(u64_value).unwrap_or(Value::Null)
}

#[cfg(test)]
use crate as report_types;
#[cfg(test)]
mod tests;
