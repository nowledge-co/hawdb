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

//! Host-neutral workload evidence models for Nowledge graph routes.

use crate::KnowledgeFanoutReasonCode;
use std::collections::BTreeMap;

/// The execution class selected for a qualified Nowledge graph query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemQueryExecutionPath {
    FastPath,
    OptimizedPath,
}

impl NowledgeMemQueryExecutionPath {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FastPath => "fast_path",
            Self::OptimizedPath => "optimized_path",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureOptions {
    pub limit: usize,
    pub max_entities: usize,
    pub max_edges: usize,
    pub changed_since_epoch_nanos: Option<i64>,
    pub capture_physical_plan: bool,
    pub include_bounded_expansion_probes: bool,
}

impl Default for NowledgeGraphRouteWorkloadFixtureOptions {
    fn default() -> Self {
        Self {
            limit: 8,
            max_entities: 8,
            max_edges: 16,
            changed_since_epoch_nanos: Some(100),
            capture_physical_plan: true,
            include_bounded_expansion_probes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadFixtureReport {
    pub protocol: &'static str,
    pub ready: bool,
    pub route_count: usize,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub bounded_expansion_probe_count: usize,
    pub failed_bounded_expansion_probe_count: usize,
    pub bounded_expansion_reports: Vec<NowledgeGraphRouteWorkloadBoundedExpansionReport>,
    pub search_metadata_probe_count: usize,
    pub failed_search_metadata_probe_count: usize,
    pub search_metadata_reports: Vec<NowledgeSearchMetadataWorkloadReport>,
    pub graph_rag_probe_count: usize,
    pub failed_graph_rag_probe_count: usize,
    pub graph_rag_reports: Vec<NowledgeGraphRagWorkloadReport>,
    pub source_projection_probe_count: usize,
    pub failed_source_projection_probe_count: usize,
    pub source_projection_reports: Vec<NowledgeSourceProjectionWorkloadReport>,
    pub routes: Vec<NowledgeGraphRouteWorkloadRouteReport>,
}

impl NowledgeGraphRouteWorkloadFixtureReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "route_count": self.route_count,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "bounded_expansion_probe_count": self.bounded_expansion_probe_count,
            "failed_bounded_expansion_probe_count": self.failed_bounded_expansion_probe_count,
            "bounded_expansion_reports": self.bounded_expansion_reports.iter().map(NowledgeGraphRouteWorkloadBoundedExpansionReport::json).collect::<Vec<_>>(),
            "search_metadata_probe_count": self.search_metadata_probe_count,
            "failed_search_metadata_probe_count": self.failed_search_metadata_probe_count,
            "search_metadata_reports": self.search_metadata_reports.iter().map(NowledgeSearchMetadataWorkloadReport::json).collect::<Vec<_>>(),
            "graph_rag_probe_count": self.graph_rag_probe_count,
            "failed_graph_rag_probe_count": self.failed_graph_rag_probe_count,
            "graph_rag_reports": self.graph_rag_reports.iter().map(NowledgeGraphRagWorkloadReport::json).collect::<Vec<_>>(),
            "source_projection_probe_count": self.source_projection_probe_count,
            "failed_source_projection_probe_count": self.failed_source_projection_probe_count,
            "source_projection_reports": self.source_projection_reports.iter().map(NowledgeSourceProjectionWorkloadReport::json).collect::<Vec<_>>(),
            "routes": self.routes.iter().map(NowledgeGraphRouteWorkloadRouteReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeSourceProjectionWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub source_graph_commit_epoch: Option<u64>,
    pub complete_through_graph_commit_epoch: Option<u64>,
    pub too_small_batch_failed_closed: bool,
    pub operation_count: usize,
    pub upserted_documents: usize,
    pub deleted_documents: usize,
    pub source_document_count: usize,
    pub indexed_source_document_ready: bool,
    pub error_class: Option<String>,
}

impl NowledgeSourceProjectionWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "source_graph_commit_epoch": self.source_graph_commit_epoch,
            "complete_through_graph_commit_epoch": self.complete_through_graph_commit_epoch,
            "too_small_batch_failed_closed": self.too_small_batch_failed_closed,
            "operation_count": self.operation_count,
            "upserted_documents": self.upserted_documents,
            "deleted_documents": self.deleted_documents,
            "source_document_count": self.source_document_count,
            "indexed_source_document_ready": self.indexed_source_document_ready,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRagWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub schema_protocol: Option<String>,
    pub context_epoch: Option<u64>,
    pub schema_fingerprint: Option<u64>,
    pub label_count: usize,
    pub relationship_type_count: usize,
    pub property_count: usize,
    pub route_count: usize,
    pub common_path_count: usize,
    pub parameter_requirement_count: usize,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub blocking_operator_count: usize,
    pub streaming: bool,
    pub error_class: Option<String>,
}

impl NowledgeGraphRagWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "schema_protocol": self.schema_protocol,
            "context_epoch": self.context_epoch,
            "schema_fingerprint": self.schema_fingerprint,
            "label_count": self.label_count,
            "relationship_type_count": self.relationship_type_count,
            "property_count": self.property_count,
            "route_count": self.route_count,
            "common_path_count": self.common_path_count,
            "parameter_requirement_count": self.parameter_requirement_count,
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "blocking_operator_count": self.blocking_operator_count,
            "streaming": self.streaming,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeSearchMetadataWorkloadReport {
    pub name: String,
    pub ready: bool,
    pub total_hits: usize,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub metadata_filters: BTreeMap<String, String>,
    pub input_predicate_count: usize,
    pub pushed_predicate_count: usize,
    pub residual_predicate_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub field_summary_count: usize,
    pub fields: Vec<String>,
    pub error_class: Option<String>,
}

impl NowledgeSearchMetadataWorkloadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "total_hits": self.total_hits,
            "document_count": self.document_count,
            "filtered_document_count": self.filtered_document_count,
            "metadata_filters": self.metadata_filters,
            "input_predicate_count": self.input_predicate_count,
            "pushed_predicate_count": self.pushed_predicate_count,
            "residual_predicate_count": self.residual_predicate_count,
            "pruned_segment_count": self.pruned_segment_count,
            "scanned_segment_count": self.scanned_segment_count,
            "field_summary_count": self.field_summary_count,
            "fields": self.fields,
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadBoundedExpansionReport {
    pub name: String,
    pub ready: bool,
    pub graph_context_limit: usize,
    pub graph_context_max_hops: usize,
    pub path_count: usize,
    pub node_count: usize,
    pub relationship_count: usize,
    pub truncated: bool,
    pub fanout_reason_codes: Vec<KnowledgeFanoutReasonCode>,
    pub error_class: Option<String>,
}

impl NowledgeGraphRouteWorkloadBoundedExpansionReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "graph_context_limit": self.graph_context_limit,
            "graph_context_max_hops": self.graph_context_max_hops,
            "path_count": self.path_count,
            "node_count": self.node_count,
            "relationship_count": self.relationship_count,
            "truncated": self.truncated,
            "fanout_reason_codes": self.fanout_reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
            "error_class": self.error_class,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadRouteReport {
    pub route: String,
    pub ready: bool,
    pub query_count: usize,
    pub failed_query_count: usize,
    pub total_rows: usize,
    pub total_elapsed_micros: u128,
    pub queries: Vec<NowledgeGraphRouteWorkloadQueryReport>,
}

impl NowledgeGraphRouteWorkloadRouteReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "route": self.route,
            "ready": self.ready,
            "query_count": self.query_count,
            "failed_query_count": self.failed_query_count,
            "total_rows": self.total_rows,
            "total_elapsed_micros": self.total_elapsed_micros,
            "queries": self.queries.iter().map(NowledgeGraphRouteWorkloadQueryReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphRouteWorkloadQueryReport {
    pub name: String,
    pub query_family: Option<String>,
    pub ready: bool,
    pub row_count: usize,
    pub elapsed_micros: u128,
    pub execution_path: Option<NowledgeMemQueryExecutionPath>,
    pub physical_plan_captured: bool,
    pub scan_pruning_report_count: usize,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub error_class: Option<String>,
}

impl NowledgeGraphRouteWorkloadQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "query_family": self.query_family,
            "ready": self.ready,
            "row_count": self.row_count,
            "elapsed_micros": self.elapsed_micros,
            "execution_path": self.execution_path.map(NowledgeMemQueryExecutionPath::as_str),
            "physical_plan_captured": self.physical_plan_captured,
            "scan_pruning_report_count": self.scan_pruning_report_count,
            "physical_operator_counts": self.physical_operator_counts,
            "error_class": self.error_class,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_workload_options_preserve_qualified_bounds() {
        assert_eq!(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
            NowledgeGraphRouteWorkloadFixtureOptions {
                limit: 8,
                max_entities: 8,
                max_edges: 16,
                changed_since_epoch_nanos: Some(100),
                capture_physical_plan: true,
                include_bounded_expansion_probes: true,
            }
        );
    }

    #[test]
    fn workload_report_json_preserves_nested_evidence() {
        let report = NowledgeGraphRouteWorkloadFixtureReport {
            protocol: "hawdb-nowledge-graph-route-workload-fixture-v1",
            ready: true,
            route_count: 1,
            query_count: 1,
            failed_query_count: 0,
            total_rows: 2,
            total_elapsed_micros: 3,
            bounded_expansion_probe_count: 1,
            failed_bounded_expansion_probe_count: 0,
            bounded_expansion_reports: vec![NowledgeGraphRouteWorkloadBoundedExpansionReport {
                name: "bounded".to_string(),
                ready: true,
                graph_context_limit: 4,
                graph_context_max_hops: 2,
                path_count: 1,
                node_count: 2,
                relationship_count: 1,
                truncated: false,
                fanout_reason_codes: vec![KnowledgeFanoutReasonCode::DenseAdjacency],
                error_class: None,
            }],
            search_metadata_probe_count: 1,
            failed_search_metadata_probe_count: 0,
            search_metadata_reports: vec![],
            graph_rag_probe_count: 0,
            failed_graph_rag_probe_count: 0,
            graph_rag_reports: vec![],
            source_projection_probe_count: 0,
            failed_source_projection_probe_count: 0,
            source_projection_reports: vec![],
            routes: vec![NowledgeGraphRouteWorkloadRouteReport {
                route: "/graph/overview".to_string(),
                ready: true,
                query_count: 1,
                failed_query_count: 0,
                total_rows: 2,
                total_elapsed_micros: 3,
                queries: vec![NowledgeGraphRouteWorkloadQueryReport {
                    name: "overview".to_string(),
                    query_family: Some("graph_overview".to_string()),
                    ready: true,
                    row_count: 2,
                    elapsed_micros: 3,
                    execution_path: Some(NowledgeMemQueryExecutionPath::FastPath),
                    physical_plan_captured: true,
                    scan_pruning_report_count: 1,
                    physical_operator_counts: BTreeMap::from([("NodeScan".to_string(), 1)]),
                    error_class: None,
                }],
            }],
        };

        let json = report.json();
        assert_eq!(
            json["bounded_expansion_reports"][0]["fanout_reason_codes"],
            serde_json::json!(["dense_adjacency"])
        );
        assert_eq!(
            json["routes"][0]["queries"][0]["execution_path"],
            "fast_path"
        );
        assert_eq!(
            json["routes"][0]["queries"][0]["physical_operator_counts"]["NodeScan"],
            1
        );
    }
}
