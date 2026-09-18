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

//! Stable query-execution report contract for the embedded Nowledge Mem facade.
//!
//! The facade owns statement execution and live instrumentation. This module owns
//! the portable report shape and JSON encoding used by readiness tooling.

use crate::bounded_read_evidence::NowledgeMemGraphMode;
use hawdb_cypher::Statement;
use hawdb_executor::{
    GraphExpansionExecutionReport, PipelineMemoryReport, QueryOutput, ReadExecutionProfile,
    VectorExecutionReport,
};
use hawdb_plan_cache::PlanCacheLookup;
use hawdb_storage::{ScanPruningReport, ScanPruningStrategy};
use std::collections::{BTreeMap, BTreeSet};

pub use hawdb_evidence::inventory::NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL;
pub use hawdb_nowledge_contracts::NowledgeMemQueryExecutionPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemFastPathClassification {
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<&'static str>,
}

pub fn nowledge_mem_fast_path_classification(
    statement: &Statement,
) -> NowledgeMemFastPathClassification {
    let shape = hawdb_cypher::read_route::classify_read_route_shape(statement);
    NowledgeMemFastPathClassification {
        execution_path: if shape.is_fast_path() {
            NowledgeMemQueryExecutionPath::FastPath
        } else {
            NowledgeMemQueryExecutionPath::OptimizedPath
        },
        fast_path_reason: shape.fast_path_reason,
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemPlanCacheReport {
    pub cacheable: bool,
    pub hit: bool,
    pub miss: bool,
    pub bypassed: bool,
}

#[doc(hidden)]
pub const fn nowledge_mem_plan_cache_report(
    lookup: Option<PlanCacheLookup>,
) -> NowledgeMemPlanCacheReport {
    match lookup {
        Some(PlanCacheLookup::Hit) => NowledgeMemPlanCacheReport {
            cacheable: true,
            hit: true,
            miss: false,
            bypassed: false,
        },
        Some(PlanCacheLookup::Miss) => NowledgeMemPlanCacheReport {
            cacheable: true,
            hit: false,
            miss: true,
            bypassed: false,
        },
        Some(PlanCacheLookup::Bypass(_)) => NowledgeMemPlanCacheReport {
            cacheable: false,
            hit: false,
            miss: false,
            bypassed: true,
        },
        None => NowledgeMemPlanCacheReport {
            cacheable: false,
            hit: false,
            miss: false,
            bypassed: false,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub statement_kind: String,
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<String>,
    pub elapsed_micros: u128,
    pub slow_log_threshold_micros: Option<u128>,
    pub slow_log_candidate: bool,
    pub physical_plan_captured: bool,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub vector_execution_reports: Vec<VectorExecutionReport>,
    pub graph_expansion_reports: Vec<GraphExpansionExecutionReport>,
    pub pipeline_memory_report: Option<PipelineMemoryReport>,
    pub output_row_shape: NowledgeMemQueryOutputRowShape,
    pub api_behavior: NowledgeMemQueryApiBehavior,
}

impl NowledgeMemQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "statement_kind": self.statement_kind,
            "execution_path": self.execution_path.as_str(),
            "fast_path_reason": self.fast_path_reason,
            "fast_path_selected": self.execution_path == NowledgeMemQueryExecutionPath::FastPath,
            "elapsed_micros": self.elapsed_micros,
            "slow_log_threshold_micros": self.slow_log_threshold_micros,
            "slow_log_candidate": self.slow_log_candidate,
            "physical_plan_captured": self.physical_plan_captured,
            "plan_cache_lookup": self.plan_cache_lookup,
            "plan_cache_bypass_reason": self.plan_cache_bypass_reason,
            "plan_cache_cacheable": self.plan_cache_cacheable,
            "plan_cache_hit": self.plan_cache_hit,
            "plan_cache_miss": self.plan_cache_miss,
            "plan_cache_bypassed": self.plan_cache_bypassed,
            "plan_cache": {
                "lookup": self.plan_cache_lookup,
                "bypass_reason": self.plan_cache_bypass_reason,
                "cacheable": self.plan_cache_cacheable,
                "hit": self.plan_cache_hit,
                "miss": self.plan_cache_miss,
                "bypassed": self.plan_cache_bypassed,
            },
            "physical_operator_counts": self.physical_operator_counts,
            "optimizer_decision_count": self.optimizer_decision_count,
            "optimizer_rule_event_count": self.optimizer_rule_event_count,
            "scan_pruning_report_count": self.scan_pruning_reports.len(),
            "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
            "vector_execution_report_count": self.vector_execution_reports.len(),
            "vector_execution_reports": self.vector_execution_reports.iter().map(vector_execution_report_json).collect::<Vec<_>>(),
            "graph_expansion_report_count": self.graph_expansion_reports.len(),
            "graph_expansion_reports": self.graph_expansion_reports.iter().map(graph_expansion_report_json).collect::<Vec<_>>(),
            "pipeline_memory_report": self.pipeline_memory_report.as_ref().map(pipeline_memory_report_json),
            "output_row_shape": self.output_row_shape.json(),
            "api_behavior": self.api_behavior.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutputRowShape {
    pub row_count: usize,
    pub column_count: usize,
    pub columns: Vec<String>,
}

impl NowledgeMemQueryOutputRowShape {
    fn from_output(output: &QueryOutput) -> Self {
        let columns: Vec<_> = output
            .schema()
            .columns()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            row_count: output.rows.len(),
            column_count: columns.len(),
            columns,
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryApiBehavior {
    pub include_metadata_false_strips_metadata: bool,
    pub ordering_contract_recorded: bool,
    pub pagination_contract_recorded: bool,
    pub error_class_stable: bool,
    pub statement_has_ordering: bool,
    pub statement_has_pagination: bool,
}

impl NowledgeMemQueryApiBehavior {
    fn from_statement(statement: &Statement) -> Self {
        let shape = hawdb_cypher::read_route::classify_read_route_shape(statement);
        Self {
            include_metadata_false_strips_metadata: true,
            ordering_contract_recorded: true,
            pagination_contract_recorded: true,
            error_class_stable: true,
            statement_has_ordering: shape.has_ordering,
            statement_has_pagination: shape.has_pagination,
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "include_metadata_false_strips_metadata": self.include_metadata_false_strips_metadata,
            "ordering_contract_recorded": self.ordering_contract_recorded,
            "pagination_contract_recorded": self.pagination_contract_recorded,
            "error_class_stable": self.error_class_stable,
            "statement_has_ordering": self.statement_has_ordering,
            "statement_has_pagination": self.statement_has_pagination,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemQueryReport,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemQueryReportOptions {
    pub capture_physical_plan: bool,
    pub slow_log_threshold_micros: Option<u128>,
}

#[doc(hidden)]
pub struct NowledgeMemQueryReportInput<'a> {
    pub mode: NowledgeMemGraphMode,
    pub statement_kind: &'a str,
    pub statement: &'a Statement,
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<&'a str>,
    pub physical_plan_captured: bool,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub plan_cache_lookup: Option<PlanCacheLookup>,
    pub execution_profile: Option<&'a ReadExecutionProfile<ScanPruningReport>>,
    pub output: &'a QueryOutput,
    pub options: NowledgeMemQueryReportOptions,
    pub elapsed_micros: u128,
}

#[doc(hidden)]
pub fn nowledge_mem_query_report(input: NowledgeMemQueryReportInput<'_>) -> NowledgeMemQueryReport {
    let slow_log_candidate = input
        .options
        .slow_log_threshold_micros
        .is_some_and(|threshold| input.elapsed_micros >= threshold);
    let plan_cache = nowledge_mem_plan_cache_report(input.plan_cache_lookup);
    NowledgeMemQueryReport {
        protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
        mode: input.mode,
        statement_kind: input.statement_kind.to_string(),
        execution_path: input.execution_path,
        fast_path_reason: input.fast_path_reason.map(str::to_string),
        elapsed_micros: input.elapsed_micros,
        slow_log_threshold_micros: input.options.slow_log_threshold_micros,
        slow_log_candidate,
        physical_plan_captured: input.physical_plan_captured,
        plan_cache_lookup: input
            .plan_cache_lookup
            .map(|lookup| lookup.as_str().to_string()),
        plan_cache_bypass_reason: input
            .plan_cache_lookup
            .and_then(|lookup| lookup.bypass_reason())
            .map(|reason| reason.as_str().to_string()),
        plan_cache_cacheable: plan_cache.cacheable,
        plan_cache_hit: plan_cache.hit,
        plan_cache_miss: plan_cache.miss,
        plan_cache_bypassed: plan_cache.bypassed,
        physical_operator_counts: input.physical_operator_counts,
        optimizer_decision_count: input.optimizer_decision_count,
        optimizer_rule_event_count: input.optimizer_rule_event_count,
        scan_pruning_reports: input
            .execution_profile
            .map(|profile| profile.scan_pruning_reports.clone())
            .unwrap_or_default(),
        vector_execution_reports: input
            .execution_profile
            .map(|profile| profile.vector_execution_reports.clone())
            .unwrap_or_default(),
        graph_expansion_reports: input
            .execution_profile
            .map(|profile| profile.graph_expansion_reports.clone())
            .unwrap_or_default(),
        pipeline_memory_report: input
            .execution_profile
            .map(|profile| profile.pipeline_memory_report.clone()),
        output_row_shape: NowledgeMemQueryOutputRowShape::from_output(input.output),
        api_behavior: NowledgeMemQueryApiBehavior::from_statement(input.statement),
    }
}

fn vector_execution_report_json(report: &VectorExecutionReport) -> serde_json::Value {
    serde_json::json!({
        "backend": report.backend.as_str(),
        "compression_mode": report.compression_mode.as_str(),
        "candidate_source": report.candidate_source.as_str(),
        "backend_selection_reason": report.backend_selection_reason.map(|reason| reason.as_str()),
        "estimated_raw_vector_bytes": report.estimated_raw_vector_bytes,
        "filter_selectivity_per_million": report.filter_selectivity_per_million,
        "candidate_score_source": report.candidate_score_source.as_str(),
        "final_score_source": report.final_score_source.as_str(),
        "generated_candidate_count": report.generated_candidate_count,
        "descriptor_pruned_count": report.descriptor_pruned_count,
        "scalar_filtered_count": report.scalar_filtered_count,
        "residual_filtered_count": report.residual_filtered_count,
        "candidate_scan_rounds": report.candidate_scan_rounds,
        "reranked_candidate_count": report.reranked_candidate_count,
        "returned_count": report.returned_count,
        "raw_vector_bytes_read": report.raw_vector_bytes_read,
        "candidate_scan": report.candidate_scan_metrics.as_ref().map(|metrics| serde_json::json!({
            "kernel": metrics.kernel,
            "worker_count": metrics.worker_count,
            "segment_count": metrics.segment_count,
            "scanned_segment_count": metrics.scanned_segment_count,
            "scored_document_count": metrics.scored_document_count,
            "filtered_document_count": metrics.filtered_document_count,
            "scanned_block_count": metrics.scanned_block_count,
            "skipped_block_count": metrics.skipped_block_count,
            "payload_bytes_read": metrics.payload_bytes_read,
            "admitted_working_bytes": metrics.admitted_working_bytes,
        })),
        "index_covered_document_count": report.index_covered_document_count,
        "index_candidate_document_count": report.index_candidate_document_count,
        "index_coverage_complete": report.index_coverage_complete,
        "fallback_reason_codes": report.fallback_reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
    })
}

fn pipeline_memory_report_json(report: &PipelineMemoryReport) -> serde_json::Value {
    serde_json::json!({
        "intermediate_rows": report.intermediate_rows,
        "intermediate_payload_bytes": report.intermediate_payload_bytes,
        "peak_batch_rows": report.peak_batch_rows,
        "peak_batch_payload_bytes": report.peak_batch_payload_bytes,
        "output_rows": report.output_rows,
        "output_payload_bytes": report.output_payload_bytes,
        "start_resident_bytes": report.start_resident_bytes,
        "start_peak_resident_bytes": report.start_peak_resident_bytes,
        "steady_resident_bytes": report.steady_resident_bytes,
        "peak_resident_bytes": report.peak_resident_bytes,
        "steady_resident_growth_bytes": report.steady_resident_growth_bytes,
        "lifetime_peak_resident_growth_bytes": report.lifetime_peak_resident_growth_bytes,
        "total_page_faults": report.total_page_faults,
        "minor_page_faults": report.minor_page_faults,
        "major_page_faults": report.major_page_faults,
    })
}

fn graph_expansion_report_json(report: &GraphExpansionExecutionReport) -> serde_json::Value {
    serde_json::json!({
        "seed_count": report.seed_count,
        "expanded_node_count": report.expanded_node_count,
        "expanded_edge_count": report.expanded_edge_count,
        "relation_types": report.relation_types,
        "min_hops": report.min_hops,
        "max_hops": report.max_hops,
        "reranked_seed_count": report.reranked_seed_count,
        "candidate_limit": report.candidate_limit,
        "payload_byte_limit": report.payload_byte_limit,
        "payload_bytes_used": report.payload_bytes_used,
        "returned_count": report.returned_count,
        "truncated": report.truncated(),
        "truncation_reason": report.truncation_reason.map(|reason| reason.as_str()),
    })
}

#[doc(hidden)]
pub fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "target_kind": report.target_kind.as_str(),
        "label_id": report.label_id.map(|label_id| label_id.0),
        "rel_type_id": report.rel_type_id.map(|rel_type_id| rel_type_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_pruning": report.candidate_count_before_pruning,
        "pruned_candidate_count": report.pruned_candidate_count,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => serde_json::json!({"kind": "full_label_scan"}),
        ScanPruningStrategy::ExactCount => serde_json::json!({"kind": "exact_count"}),
        ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyMissingOrNull { property } => {
            serde_json::json!({"kind": "property_missing_or_null", "property": property})
        }
        ScanPruningStrategy::PropertyExists { property } => {
            serde_json::json!({"kind": "property_exists", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_eq", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::CompositePropertyEq { properties } => {
            serde_json::json!({"kind": "composite_property_eq", "properties": properties})
        }
        ScanPruningStrategy::CompositePropertyRange { properties } => {
            serde_json::json!({"kind": "composite_property_range", "properties": properties})
        }
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::FullText { property } => {
            serde_json::json!({"kind": "full_text", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn query_execution_path_reexports_the_shared_contract_type() {
        assert_eq!(
            TypeId::of::<NowledgeMemQueryExecutionPath>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemQueryExecutionPath>()
        );
    }

    #[test]
    fn query_report_preserves_empty_profile_contract() {
        let report = NowledgeMemQueryReport {
            protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::ShadowReadOnly,
            statement_kind: "match_return".to_string(),
            execution_path: NowledgeMemQueryExecutionPath::FastPath,
            fast_path_reason: Some("simple_read".to_string()),
            elapsed_micros: 7,
            slow_log_threshold_micros: Some(10),
            slow_log_candidate: false,
            physical_plan_captured: false,
            plan_cache_lookup: None,
            plan_cache_bypass_reason: None,
            plan_cache_cacheable: false,
            plan_cache_hit: false,
            plan_cache_miss: false,
            plan_cache_bypassed: false,
            physical_operator_counts: BTreeMap::new(),
            optimizer_decision_count: 0,
            optimizer_rule_event_count: 0,
            scan_pruning_reports: Vec::new(),
            vector_execution_reports: Vec::new(),
            graph_expansion_reports: Vec::new(),
            pipeline_memory_report: None,
            output_row_shape: NowledgeMemQueryOutputRowShape {
                row_count: 1,
                column_count: 1,
                columns: vec!["memory".to_string()],
            },
            api_behavior: NowledgeMemQueryApiBehavior {
                include_metadata_false_strips_metadata: true,
                ordering_contract_recorded: true,
                pagination_contract_recorded: true,
                error_class_stable: true,
                statement_has_ordering: true,
                statement_has_pagination: false,
            },
        };

        assert_eq!(
            report.json(),
            serde_json::json!({
                "protocol": NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
                "mode": "shadow_read_only",
                "statement_kind": "match_return",
                "execution_path": "fast_path",
                "fast_path_reason": "simple_read",
                "fast_path_selected": true,
                "elapsed_micros": 7,
                "slow_log_threshold_micros": 10,
                "slow_log_candidate": false,
                "physical_plan_captured": false,
                "plan_cache_lookup": null,
                "plan_cache_bypass_reason": null,
                "plan_cache_cacheable": false,
                "plan_cache_hit": false,
                "plan_cache_miss": false,
                "plan_cache_bypassed": false,
                "plan_cache": {"lookup": null, "bypass_reason": null, "cacheable": false, "hit": false, "miss": false, "bypassed": false},
                "physical_operator_counts": {},
                "optimizer_decision_count": 0,
                "optimizer_rule_event_count": 0,
                "scan_pruning_report_count": 0,
                "scan_pruning_reports": [],
                "vector_execution_report_count": 0,
                "vector_execution_reports": [],
                "graph_expansion_report_count": 0,
                "graph_expansion_reports": [],
                "pipeline_memory_report": null,
                "output_row_shape": {"row_count": 1, "column_count": 1, "columns": ["memory"]},
                "api_behavior": {
                    "include_metadata_false_strips_metadata": true,
                    "ordering_contract_recorded": true,
                    "pagination_contract_recorded": true,
                    "error_class_stable": true,
                    "statement_has_ordering": true,
                    "statement_has_pagination": false,
                },
            })
        );
    }

    #[test]
    fn plan_cache_report_preserves_lookup_semantics() {
        assert_eq!(
            nowledge_mem_plan_cache_report(Some(PlanCacheLookup::Hit)),
            NowledgeMemPlanCacheReport {
                cacheable: true,
                hit: true,
                miss: false,
                bypassed: false,
            }
        );
        assert_eq!(
            nowledge_mem_plan_cache_report(Some(PlanCacheLookup::Miss)),
            NowledgeMemPlanCacheReport {
                cacheable: true,
                hit: false,
                miss: true,
                bypassed: false,
            }
        );
        assert_eq!(
            nowledge_mem_plan_cache_report(Some(PlanCacheLookup::Bypass(
                hawdb_plan_cache::PlanCacheBypassReason::MutationPlanning,
            ))),
            NowledgeMemPlanCacheReport {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: true,
            }
        );
        assert_eq!(
            nowledge_mem_plan_cache_report(None),
            NowledgeMemPlanCacheReport {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: false,
            }
        );
    }
}
