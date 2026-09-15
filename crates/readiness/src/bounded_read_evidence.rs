//! Bounded-read evidence contract and fail-closed readiness reduction.
//!
//! The embedded facade produces [`NowledgeMemReadReport`] from live query execution.
//! This module owns the portable evidence representation and its reduction so developer
//! adapters can evaluate serialized reports without depending on the facade crate.

use skein_executor::BlockingOperatorMemoryReport;
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::BTreeSet;

pub const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
pub const NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphMode {
    ShadowReadOnly,
    WritableCutover,
}

impl NowledgeMemGraphMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowReadOnly => "shadow_read_only",
            Self::WritableCutover => "writable_cutover",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub max_estimated_payload_bytes: Option<usize>,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_count: usize,
    pub blocking_operator_kinds: Vec<String>,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub output_payload_bytes: usize,
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
    pub streaming: bool,
}

impl NowledgeMemReadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "max_estimated_payload_bytes": self.max_estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
            "operator_row_cap_enabled": self.operator_row_cap_enabled,
            "blocking_operator_count": self.blocking_operator_count,
            "blocking_operator_kinds": self.blocking_operator_kinds,
            "blocking_operator_memory_reports": self.blocking_operator_memory_reports.iter().map(blocking_operator_memory_report_json).collect::<Vec<_>>(),
            "intermediate_rows": self.intermediate_rows,
            "intermediate_payload_bytes": self.intermediate_payload_bytes,
            "output_payload_bytes": self.output_payload_bytes,
            "steady_resident_bytes": self.steady_resident_bytes,
            "peak_resident_bytes": self.peak_resident_bytes,
            "total_page_faults": self.total_page_faults,
            "minor_page_faults": self.minor_page_faults,
            "major_page_faults": self.major_page_faults,
            "streaming": self.streaming,
        })
    }

    pub fn bounded_read_evidence_json(&self) -> serde_json::Value {
        nowledge_mem_bounded_read_evidence_json(self)
    }
}

pub fn nowledge_mem_bounded_read_evidence_json(
    report: &NowledgeMemReadReport,
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_routes(report, &[])
}

pub fn nowledge_mem_bounded_read_evidence_json_with_routes(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_route_readiness(report, covered_routes, None)
}

pub use skein_route_ownership::graph::NowledgeMemRouteReadinessSummary;

pub fn nowledge_mem_bounded_read_evidence_json_with_route_readiness(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_bounded_read_blocker_codes(report);
    let missing_covered_routes = missing_nowledge_mem_bounded_read_routes(covered_routes);
    let route_readiness_blocker = route_readiness.and_then(|summary| {
        (!summary.route_primary_ready
            || !summary.route_query_plan_evidence_ready
            || !summary.route_query_profile_evidence_ready
            || !summary.route_query_api_behavior_evidence_ready
            || !summary.route_relationship_property_pruning_evidence_ready
            || summary.relationship_property_pruning_required_count
                != summary.relationship_property_pruning_report_count)
            .then_some("graph_route_readiness_not_ready")
    });
    let blocker_codes = blocker_codes
        .into_iter()
        .chain((!missing_covered_routes.is_empty()).then_some("missing_covered_routes"))
        .chain((route_readiness.is_none()).then_some("graph_route_readiness_missing"))
        .chain(route_readiness_blocker)
        .collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();
    let route_primary_ready = route_readiness.map(|summary| summary.route_primary_ready);
    let primary_ready_routes = route_readiness
        .map(|summary| summary.primary_ready_routes.clone())
        .unwrap_or_default();
    let route_query_plan_evidence_ready =
        route_readiness.map(|summary| summary.route_query_plan_evidence_ready);
    let route_query_profile_evidence_ready =
        route_readiness.map(|summary| summary.route_query_profile_evidence_ready);
    let route_query_api_behavior_evidence_ready =
        route_readiness.map(|summary| summary.route_query_api_behavior_evidence_ready);
    let relationship_property_pruning_required_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_required_count);
    let relationship_property_pruning_report_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_report_count);
    let route_relationship_property_pruning_evidence_ready =
        route_readiness.map(|summary| summary.route_relationship_property_pruning_evidence_ready);
    let blocking_operator_memory_reports_complete =
        blocking_operator_memory_reports_complete(report);
    let blocking_operator_memory_within_budget = blocking_operator_memory_within_budget(report);
    let spill_within_budget = blocking_operator_spill_within_budget(report);

    serde_json::json!({
        "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        "present": true,
        "ready": ready,
        "mode": report.mode.as_str(),
        "max_rows": report.max_rows,
        "execution_row_cap": report.execution_row_cap,
        "estimated_payload_bytes": report.estimated_payload_bytes,
        "max_estimated_payload_bytes": report.max_estimated_payload_bytes,
        "row_limit_enforced_before_output": report.row_limit_enforced_before_output,
        "operator_row_cap_enabled": report.operator_row_cap_enabled,
        "streaming": report.streaming,
        "blocking_operator_count": report.blocking_operator_count,
        "blocking_operator_kinds": report.blocking_operator_kinds,
        "blocking_operator_memory_reports": report.blocking_operator_memory_reports.iter().map(blocking_operator_memory_report_json).collect::<Vec<_>>(),
        "blocking_operator_memory_reports_complete": blocking_operator_memory_reports_complete,
        "blocking_operator_memory_within_budget": blocking_operator_memory_within_budget,
        "spill_within_budget": spill_within_budget,
        "row_budget_exceeded": report.row_budget_exceeded,
        "payload_budget_exceeded": report.payload_budget_exceeded,
        "covered_routes": covered_routes,
        "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        "missing_covered_routes": missing_covered_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_primary_ready": route_primary_ready,
        "primary_ready_routes": primary_ready_routes,
        "route_query_plan_evidence_ready": route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": route_query_profile_evidence_ready,
        "route_query_api_behavior_evidence_ready": route_query_api_behavior_evidence_ready,
        "relationship_property_pruning_required_count": relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": relationship_property_pruning_report_count,
        "route_relationship_property_pruning_evidence_ready": route_relationship_property_pruning_evidence_ready,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_bounded_read_blocker_codes(report: &NowledgeMemReadReport) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    let expected_row_cap = match report.max_rows {
        Some(0) => {
            blockers.push("invalid_max_rows");
            None
        }
        Some(max_rows) => max_rows.checked_add(1),
        None => {
            blockers.push("missing_max_rows");
            None
        }
    };
    if report.mode != NowledgeMemGraphMode::ShadowReadOnly {
        blockers.push("not_shadow_read_only");
    }

    match (report.execution_row_cap, expected_row_cap) {
        (Some(execution_row_cap), Some(expected_row_cap))
            if execution_row_cap == expected_row_cap => {}
        (Some(_), _) => blockers.push("execution_row_cap_mismatch"),
        (None, _) => blockers.push("missing_execution_row_cap"),
    }
    if !report.row_limit_enforced_before_output {
        blockers.push("row_limit_not_enforced_before_output");
    }
    if !report.operator_row_cap_enabled {
        blockers.push("operator_row_cap_disabled");
    }
    if report.row_budget_exceeded {
        blockers.push("row_budget_exceeded");
    }
    if report.payload_budget_exceeded {
        blockers.push("payload_budget_exceeded");
    }
    if !blocking_operator_memory_reports_complete(report) {
        blockers.push("blocking_operator_memory_report_incomplete");
    }
    if !blocking_operator_memory_within_budget(report) {
        blockers.push("blocking_operator_memory_budget_exceeded");
    }
    if !blocking_operator_spill_within_budget(report) {
        blockers.push("blocking_operator_spill_budget_exceeded");
    }
    blockers
}

fn blocking_operator_memory_reports_complete(report: &NowledgeMemReadReport) -> bool {
    let expected = report
        .blocking_operator_kinds
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = report
        .blocking_operator_memory_reports
        .iter()
        .map(|report| report.operator.as_str())
        .collect::<BTreeSet<_>>();
    report.blocking_operator_count == expected.len() && actual == expected
}

fn blocking_operator_memory_within_budget(report: &NowledgeMemReadReport) -> bool {
    report
        .blocking_operator_memory_reports
        .iter()
        .all(|report| report.budget_bytes > 0 && report.peak_tracked_bytes <= report.budget_bytes)
}

fn blocking_operator_spill_within_budget(report: &NowledgeMemReadReport) -> bool {
    report
        .blocking_operator_memory_reports
        .iter()
        .all(|report| {
            report.max_spill_bytes > 0
                && report.max_spill_runs > 0
                && report.spilled_bytes <= report.max_spill_bytes
                && report.spill_run_count <= report.max_spill_runs
                && ((report.spill_run_count == 0 && report.spilled_bytes == 0)
                    || (report.spill_run_count > 0 && report.spilled_bytes > 0))
        })
}

fn blocking_operator_memory_report_json(
    report: &BlockingOperatorMemoryReport,
) -> serde_json::Value {
    serde_json::json!({
        "operator": report.operator,
        "budget_bytes": report.budget_bytes,
        "peak_tracked_bytes": report.peak_tracked_bytes,
        "input_rows": report.input_rows,
        "max_spill_bytes": report.max_spill_bytes,
        "max_spill_runs": report.max_spill_runs,
        "spilled_bytes": report.spilled_bytes,
        "spill_run_count": report.spill_run_count,
        "spilled_rows": report.spilled_rows,
    })
}

#[doc(hidden)]
pub fn missing_nowledge_mem_bounded_read_routes(covered_routes: &[String]) -> Vec<&'static str> {
    let covered_routes = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_routes.contains(route))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_bounded_read_evidence_json,
        nowledge_mem_bounded_read_evidence_json_with_route_readiness, NowledgeMemGraphMode,
        NowledgeMemReadReport, NowledgeMemRouteReadinessSummary,
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
    };
    use skein_executor::BlockingOperatorMemoryReport;
    use skein_route_ownership::graph::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;

    #[test]
    fn bounded_read_evidence_fails_closed_for_missing_row_cap() {
        let mut report = report(
            NowledgeMemGraphMode::ShadowReadOnly,
            None,
            false,
            false,
            Vec::new(),
        );
        report.blocking_operator_count = 1;
        report.blocking_operator_kinds = vec!["Sort".to_string()];

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(evidence["present"], true);
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["max_rows"], 512);
        assert_eq!(evidence["execution_row_cap"], serde_json::Value::Null);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "missing_execution_row_cap",
                "row_limit_not_enforced_before_output",
                "operator_row_cap_disabled",
                "blocking_operator_memory_report_incomplete",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_requires_shadow_read_only_mode() {
        let evidence = nowledge_mem_bounded_read_evidence_json(&report(
            NowledgeMemGraphMode::WritableCutover,
            Some(513),
            true,
            true,
            Vec::new(),
        ));

        assert_eq!(evidence["mode"], "writable_cutover");
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "not_shadow_read_only",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_accepts_streaming_with_budgeted_blocking_operator() {
        let report = report(
            NowledgeMemGraphMode::ShadowReadOnly,
            Some(513),
            true,
            true,
            vec![BlockingOperatorMemoryReport {
                operator: "TopNExec".to_string(),
                budget_bytes: 4096,
                peak_tracked_bytes: 2048,
                input_rows: 100,
                max_spill_bytes: 8192,
                max_spill_runs: 4,
                spilled_bytes: 4096,
                spill_run_count: 2,
                spilled_rows: 64,
            }],
        );
        let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect::<Vec<_>>();
        let route_readiness = NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: covered_routes.clone(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json_with_route_readiness(
            &report,
            &covered_routes,
            Some(&route_readiness),
        );

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["streaming"], true);
        assert_eq!(evidence["blocking_operator_memory_reports_complete"], true);
        assert_eq!(evidence["blocking_operator_memory_within_budget"], true);
        assert_eq!(evidence["spill_within_budget"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    fn report(
        mode: NowledgeMemGraphMode,
        execution_row_cap: Option<usize>,
        row_limit_enforced_before_output: bool,
        operator_row_cap_enabled: bool,
        blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
    ) -> NowledgeMemReadReport {
        NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap,
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output,
            operator_row_cap_enabled,
            blocking_operator_count: usize::from(!blocking_operator_memory_reports.is_empty()),
            blocking_operator_kinds: blocking_operator_memory_reports
                .iter()
                .map(|report| report.operator.clone())
                .collect(),
            blocking_operator_memory_reports,
            intermediate_rows: 100,
            intermediate_payload_bytes: 2048,
            output_payload_bytes: 128,
            steady_resident_bytes: Some(1024),
            peak_resident_bytes: Some(2048),
            total_page_faults: Some(1),
            minor_page_faults: Some(1),
            major_page_faults: Some(0),
            streaming: true,
        }
    }
}
