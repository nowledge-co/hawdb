//! Stable result contract for query-runtime preflight execution.
//!
//! Hosts open databases and execute probes. This module owns the redacted result
//! model and JSON encoding so readiness consumers do not depend on the embedded facade.

use crate::nowledge_mem_query_report::scan_pruning_report_json;
use skein_storage::ScanPruningReport;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightReport {
    pub protocol: String,
    pub ready: bool,
    pub database_opened: bool,
    pub redaction: NowledgeQueryRuntimePreflightRedactionSummary,
    pub probe_count: usize,
    pub passed_probe_count: usize,
    pub failed_probe_count: usize,
    pub required_route_count: usize,
    pub covered_route_count: usize,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub required_routes_covered: bool,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub route_coverage_ready: bool,
    pub route_coverage_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
    pub probes: Vec<NowledgeQueryRuntimePreflightProbeReport>,
}

impl NowledgeQueryRuntimePreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "database_opened": self.database_opened,
            "redaction": self.redaction.json(),
            "probe_count": self.probe_count,
            "passed_probe_count": self.passed_probe_count,
            "failed_probe_count": self.failed_probe_count,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "missing_required_routes": self.missing_required_routes,
            "required_routes_covered": self.required_routes_covered,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "route_coverage_ready": self.route_coverage_ready,
            "route_coverage_blocker_codes": self.route_coverage_blocker_codes,
            "blocker_codes": self.blocker_codes,
            "probes": self.probes.iter().map(NowledgeQueryRuntimePreflightProbeReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeQueryRuntimePreflightRedactionSummary {
    pub rows_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
    pub raw_errors_copied: bool,
}

impl NowledgeQueryRuntimePreflightRedactionSummary {
    pub const fn ready(&self) -> bool {
        !self.rows_copied
            && !self.parameters_copied
            && !self.local_paths_copied
            && !self.raw_errors_copied
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "rows_copied": self.rows_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
            "raw_errors_copied": self.raw_errors_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbeReport {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub ready: bool,
    pub success: bool,
    pub output_row_count: usize,
    pub selected_plan_fingerprint: Option<String>,
    pub search_mode: Option<String>,
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub work_priority: Option<String>,
    pub work_class: Option<String>,
    pub estimated_operations: Option<usize>,
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub pruned_scan_count: usize,
    pub error_class: Option<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeQueryRuntimePreflightProbeReport {
    pub fn json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "name": self.name,
            "route": self.route,
            "query_family": self.query_family,
            "ready": self.ready,
            "success": self.success,
            "blocker_codes": self.blocker_codes,
        });
        let object = value
            .as_object_mut()
            .expect("query runtime preflight probe report is an object");
        if self.success {
            object.insert(
                "output_row_count".to_string(),
                serde_json::json!(self.output_row_count),
            );
            object.insert(
                "selected_plan_fingerprint".to_string(),
                serde_json::json!(self.selected_plan_fingerprint),
            );
            object.insert(
                "search_mode".to_string(),
                serde_json::json!(self.search_mode),
            );
            object.insert(
                "selected_plan_operator_counts".to_string(),
                serde_json::json!(self.selected_plan_operator_counts),
            );
            object.insert(
                "selected_plan_class_counts".to_string(),
                serde_json::json!(self.selected_plan_class_counts),
            );
            object.insert(
                "optimizer_decision_count".to_string(),
                serde_json::json!(self.optimizer_decision_count),
            );
            object.insert(
                "optimizer_rule_event_count".to_string(),
                serde_json::json!(self.optimizer_rule_event_count),
            );
            object.insert(
                "plan_cache_lookup".to_string(),
                serde_json::json!(self.plan_cache_lookup),
            );
            object.insert(
                "plan_cache".to_string(),
                serde_json::json!({
                    "lookup": self.plan_cache_lookup,
                    "bypass_reason": self.plan_cache_bypass_reason,
                    "cacheable": self.plan_cache_cacheable,
                    "hit": self.plan_cache_hit,
                    "miss": self.plan_cache_miss,
                    "bypassed": self.plan_cache_bypassed,
                }),
            );
            object.insert(
                "work_request".to_string(),
                serde_json::json!({
                    "priority": self.work_priority,
                    "class": self.work_class,
                    "estimated_operations": self.estimated_operations,
                }),
            );
            object.insert(
                "execution_profile".to_string(),
                serde_json::json!({
                    "max_rows": self.max_rows,
                    "detection_row_cap": self.detection_row_cap,
                    "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": self.operator_row_cap_enabled,
                    "blocking_operator_kinds": self.blocking_operator_kinds,
                    "scan_pruning_report_count": self.scan_pruning_reports.len(),
                    "pruned_scan_count": self.pruned_scan_count,
                    "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
                }),
            );
        } else {
            object.insert(
                "error_class".to_string(),
                serde_json::json!(self.error_class),
            );
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_probe_never_serializes_execution_details() {
        let report = NowledgeQueryRuntimePreflightProbeReport {
            name: "malformed".to_string(),
            route: Some("graph-overview".to_string()),
            query_family: None,
            ready: false,
            success: false,
            output_row_count: 1,
            selected_plan_fingerprint: Some("plan".to_string()),
            search_mode: Some("disabled".to_string()),
            selected_plan_operator_counts: BTreeMap::new(),
            selected_plan_class_counts: BTreeMap::new(),
            optimizer_decision_count: 1,
            optimizer_rule_event_count: 1,
            plan_cache_lookup: Some("hit".to_string()),
            plan_cache_bypass_reason: None,
            plan_cache_cacheable: true,
            plan_cache_hit: true,
            plan_cache_miss: false,
            plan_cache_bypassed: false,
            work_priority: Some("interactive".to_string()),
            work_class: Some("query".to_string()),
            estimated_operations: Some(1),
            max_rows: Some(1),
            detection_row_cap: Some(2),
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_kinds: Vec::new(),
            scan_pruning_reports: Vec::new(),
            pruned_scan_count: 0,
            error_class: Some("parse".to_string()),
            blocker_codes: vec!["query_runtime_probe_failed".to_string()],
        };

        assert_eq!(
            report.json(),
            serde_json::json!({
                "name": "malformed",
                "route": "graph-overview",
                "query_family": null,
                "ready": false,
                "success": false,
                "blocker_codes": ["query_runtime_probe_failed"],
                "error_class": "parse",
            })
        );
    }

    #[test]
    fn redaction_is_ready_only_without_raw_execution_data() {
        assert!(NowledgeQueryRuntimePreflightRedactionSummary::default().ready());
        assert!(NowledgeQueryRuntimePreflightRedactionSummary {
            rows_copied: true,
            ..NowledgeQueryRuntimePreflightRedactionSummary::default()
        }
        .json()["ready"]
            .as_bool()
            .is_some_and(|ready| !ready));
    }
}
