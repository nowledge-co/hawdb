use super::nowledge_graph_route_readiness_json;
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_graph_read_route_specs_json,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};

#[test]
fn route_readiness_reports_ready_for_all_required_routes() {
    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(ready_routes())).unwrap();

    assert_eq!(readiness["protocol"], "nmem-graph-route-readiness-v1");
    assert_eq!(
        readiness["evidence_protocol"],
        "nmem-graph-route-evidence-v1"
    );
    assert_eq!(readiness["evidence_ready"], true);
    assert_eq!(
        readiness["route_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(readiness["route_primary_ready"], true);
    assert_eq!(readiness["route_query_runtime_ready"], true);
    assert_eq!(
        readiness["query_runtime_route_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(
        readiness["query_runtime_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(
        readiness["query_runtime_plan_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(
        readiness["query_runtime_profile_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(
        readiness["query_runtime_api_behavior_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(readiness["query_runtime_failed_query_count"], 0);
    assert_eq!(readiness["query_runtime_missing_plan_evidence_count"], 0);
    assert_eq!(readiness["query_runtime_missing_profile_evidence_count"], 0);
    assert_eq!(readiness["relationship_property_pruning_required_count"], 0);
    assert_eq!(readiness["relationship_property_pruning_report_count"], 0);
    assert_eq!(readiness["route_query_plan_evidence_ready"], true);
    assert_eq!(readiness["route_query_profile_evidence_ready"], true);
    assert_eq!(readiness["route_query_api_behavior_evidence_ready"], true);
    assert_eq!(
        readiness["route_relationship_property_pruning_evidence_ready"],
        true
    );
    assert_eq!(
        readiness["route_primary_blocker_codes"],
        serde_json::json!([])
    );
    assert_eq!(
        readiness["covered_route_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
    );
    assert_eq!(
        readiness["covered_routes"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES)
    );
    assert_eq!(
        readiness["route_catalog"],
        nowledge_mem_graph_read_route_specs_json()
    );
    let search_route = readiness["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["route"] == "/graph/search")
        .unwrap();
    assert_eq!(search_route["owner"], "search_runtime");
    assert_eq!(
        search_route["required_evidence_kind"],
        "search_candidate_shadow"
    );
    assert_eq!(search_route["stale_on_catalog_change"], true);
    assert_eq!(readiness["missing_required_routes"], serde_json::json!([]));
    assert_eq!(readiness["required_routes_covered"], true);
    assert_eq!(readiness["unknown_routes"], serde_json::json!([]));
    assert_eq!(readiness["duplicate_routes"], serde_json::json!([]));
    assert_eq!(readiness["route_coverage_ready"], true);
    assert_eq!(
        readiness["route_coverage_blocker_codes"],
        serde_json::json!([])
    );
    assert_eq!(readiness["evidence_route_coverage_present"], true);
    assert_eq!(readiness["evidence_route_coverage_matches"], true);
    assert_eq!(
        readiness["evidence_route_coverage_blocker_codes"],
        serde_json::json!([])
    );
}

#[test]
fn route_readiness_fails_closed_for_missing_required_route() {
    let mut routes = ready_routes();
    routes.pop();

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(
        readiness["route_primary_blocker_codes"],
        serde_json::json!(["missing_required_routes"])
    );
    assert_eq!(readiness["required_routes_covered"], false);
    assert_eq!(readiness["route_coverage_ready"], false);
    assert_eq!(
        readiness["route_coverage_blocker_codes"],
        serde_json::json!(["missing_required_routes"])
    );
    assert_eq!(
        readiness["missing_required_routes"],
        serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.last().unwrap()])
    );
}

#[test]
fn route_readiness_fails_closed_for_unknown_route() {
    let mut routes = ready_routes();
    routes.push(ready_route("/graph/manual-extra-route"));

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["required_routes_covered"], true);
    assert_eq!(readiness["route_coverage_ready"], false);
    assert_eq!(
        readiness["unknown_routes"],
        serde_json::json!(["/graph/manual-extra-route"])
    );
    assert_eq!(
        readiness["route_coverage_blocker_codes"],
        serde_json::json!(["unknown_routes"])
    );
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "unknown_routes"));
}

#[test]
fn route_readiness_fails_closed_for_duplicate_route() {
    let mut routes = ready_routes();
    routes.push(ready_route("/graph/overview"));

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["required_routes_covered"], true);
    assert_eq!(readiness["route_coverage_ready"], false);
    assert_eq!(
        readiness["duplicate_routes"],
        serde_json::json!(["/graph/overview"])
    );
    assert_eq!(
        readiness["route_coverage_blocker_codes"],
        serde_json::json!(["duplicate_routes"])
    );
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "duplicate_routes"));
}

#[test]
fn route_readiness_fails_closed_when_evidence_route_coverage_is_stale() {
    let mut evidence = ready_evidence(ready_routes());
    evidence["covered_routes"] = serde_json::json!(["/graph/overview"]);
    evidence["covered_route_count"] = serde_json::json!(1);

    let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_coverage_ready"], true);
    assert_eq!(readiness["evidence_route_coverage_present"], true);
    assert_eq!(readiness["evidence_route_coverage_matches"], false);
    assert_eq!(
        readiness["evidence_route_coverage_blocker_codes"],
        serde_json::json!(["route_coverage_evidence_mismatch"])
    );
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_coverage_evidence_mismatch"));
}

#[test]
fn route_readiness_fails_closed_for_unready_route() {
    let mut routes = ready_routes();
    routes[0]["primary_ready"] = serde_json::json!(false);
    routes[0]["blocker_codes"] = serde_json::json!(["primary_route_disabled"]);

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(
        readiness["route_primary_blocker_codes"],
        serde_json::json!(["primary_route_disabled", "route_primary_not_ready"])
    );
}

#[test]
fn route_readiness_fails_closed_without_route_parity_evidence_source() {
    let mut routes = ready_routes();
    routes[0]
        .as_object_mut()
        .unwrap()
        .remove("shadow_compare_evidence_source");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_shadow_compare_evidence_missing"));
}

#[test]
fn route_readiness_recomputes_shadow_compare_detail_identity() {
    let mut routes = ready_routes();
    routes[0]["shadow_compare"] = serde_json::json!({
        "source": "route_parity_evidence",
        "ready": true,
        "matched_per_million": 999999,
        "primary_engine": "skein",
        "shadow_engine": "kuzu",
        "blocker_codes": []
    });

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_parity_matched_per_million_not_full"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_parity_primary_engine_mismatch"));
    assert_eq!(
        readiness["routes"][0]["shadow_compare"]["computed_blocker_codes"],
        serde_json::json!([
            "route_parity_matched_per_million_not_full",
            "route_parity_primary_engine_mismatch",
            "route_parity_shadow_engine_mismatch"
        ])
    );
}

#[test]
fn route_readiness_fails_closed_without_query_runtime_reports() {
    let mut routes = ready_routes();
    routes[0]["query_reports"] = serde_json::json!([]);

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "missing_query_runtime_reports"));
}

#[test]
fn route_readiness_fails_closed_without_required_query_families() {
    let mut routes = ready_routes();
    routes[0]
        .as_object_mut()
        .unwrap()
        .remove("required_query_families");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert_eq!(
        readiness["routes"][0]["query_family_blocker_codes"],
        serde_json::json!(["route_required_query_families_mismatch"])
    );
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_required_query_families_mismatch"));
}

#[test]
fn route_readiness_fails_closed_when_route_query_family_does_not_match() {
    let mut routes = ready_routes();
    let shortest_path = routes
        .iter_mut()
        .find(|route| route["route"] == "/graph/shortest-path")
        .unwrap();
    shortest_path["query_reports"][0]["query_family"] = serde_json::json!("memory_lookup");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    let route = readiness["routes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|route| route["route"] == "/graph/shortest-path")
        .unwrap();
    assert_eq!(
        route["computed_required_query_families"],
        serde_json::json!(["graph_traversal"])
    );
    assert_eq!(
        route["query_family_blocker_codes"],
        serde_json::json!(["route_required_query_family_missing"])
    );
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_required_query_family_missing"));
}

#[test]
fn route_readiness_fails_closed_without_query_report_identity() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("query_name");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_identity_missing"));
}

#[test]
fn route_readiness_fails_closed_for_non_graph_query_report() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["statement_kind"] = serde_json::json!("set_system_variable");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_not_graph_read"));
}

#[test]
fn route_readiness_fails_closed_without_query_report_profile() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("scan_pruning_report_count");
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("scan_pruning_reports");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_scan_pruning_profile_missing"));
}

#[test]
fn route_readiness_fails_closed_without_query_report_plan_evidence() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("physical_operator_counts");
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("optimizer_decision_count");
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("optimizer_rule_event_count");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert_eq!(readiness["route_query_plan_evidence_ready"], false);
    assert_eq!(
        readiness["query_runtime_plan_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1)
    );
    assert_eq!(readiness["query_runtime_missing_plan_evidence_count"], 1);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_query_plan_evidence_not_ready"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_physical_operator_counts_missing"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_optimizer_decision_count_missing"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_optimizer_rule_event_count_missing"));
}

#[test]
fn route_readiness_fails_closed_without_api_behavior_metadata_stripping() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("api_behavior");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_api_behavior_metadata_stripping_missing"));
    assert_eq!(
        readiness["routes"][0]["query_reports"][0]["api_behavior"]
            ["include_metadata_false_strips_metadata"],
        serde_json::Value::Null
    );
}

#[test]
fn route_readiness_fails_closed_when_api_behavior_metadata_stripping_is_false() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["api_behavior"]["include_metadata_false_strips_metadata"] =
        serde_json::json!(false);

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_api_behavior_metadata_stripping_missing"));
}

#[test]
fn route_readiness_fails_closed_without_api_behavior_ordering_contract() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["api_behavior"]
        .as_object_mut()
        .unwrap()
        .remove("ordering_contract_recorded");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert_eq!(
        readiness["query_runtime_api_behavior_report_count"],
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1)
    );
    assert_eq!(readiness["route_query_api_behavior_evidence_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_query_api_behavior_evidence_not_ready"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_api_behavior_ordering_missing"));
    assert_eq!(
        readiness["routes"][0]["query_runtime_api_behavior_report_count"],
        0
    );
    assert_eq!(
        readiness["routes"][0]["query_api_behavior_evidence_ready"],
        false
    );
    assert_eq!(
        readiness["routes"][0]["query_reports"][0]["api_behavior"]["ready"],
        false
    );
}

#[test]
fn route_readiness_fails_closed_without_api_behavior_error_class_contract() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["api_behavior"]
        .as_object_mut()
        .unwrap()
        .remove("error_class_stable");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_api_behavior_error_class_missing"));
}

#[test]
fn route_readiness_fails_closed_without_api_behavior_pagination_contract() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["api_behavior"]
        .as_object_mut()
        .unwrap()
        .remove("pagination_contract_recorded");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_api_behavior_pagination_missing"));
}

#[test]
fn route_readiness_fails_closed_without_output_row_shape() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("output_row_shape");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_output_row_shape_missing"));
    assert_eq!(
        readiness["routes"][0]["query_reports"][0]["output_row_shape"]["ready"],
        false
    );
}

#[test]
fn route_readiness_fails_closed_when_output_row_shape_column_count_is_stale() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["output_row_shape"]["column_count"] = serde_json::json!(3);

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_output_row_shape_missing"));
    assert_eq!(
        readiness["routes"][0]["query_reports"][0]["output_row_shape"]["ready"],
        false
    );
}

#[test]
fn route_readiness_accepts_relationship_property_pruning_evidence() {
    let mut routes = ready_routes();
    routes[0]["relationship_property_pruning_required_count"] = serde_json::json!(1);
    routes[0]["relationship_property_pruning_report_count"] = serde_json::json!(1);
    routes[0]["query_reports"][0]["scan_pruning_reports"][0]["target_kind"] =
        serde_json::json!("relationship");
    routes[0]["query_reports"][0]["scan_pruning_reports"][0]["strategy"] = serde_json::json!({
        "kind": "relationship_property",
        "property": "type"
    });

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], true);
    assert_eq!(readiness["relationship_property_pruning_required_count"], 1);
    assert_eq!(readiness["relationship_property_pruning_report_count"], 1);
    assert_eq!(
        readiness["route_relationship_property_pruning_evidence_ready"],
        true
    );
    assert_eq!(
        readiness["routes"][0]["relationship_property_pruning_report_count"],
        1
    );
}

#[test]
fn route_readiness_fails_closed_without_relationship_property_pruning_report() {
    let mut routes = ready_routes();
    routes[0]["relationship_property_pruning_required_count"] = serde_json::json!(1);
    routes[0]["relationship_property_pruning_report_count"] = serde_json::json!(0);
    routes[0]["relationship_property_pruning_evidence_ready"] = serde_json::json!(false);

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(
        readiness["route_relationship_property_pruning_evidence_ready"],
        false
    );
    assert_eq!(readiness["relationship_property_pruning_required_count"], 1);
    assert_eq!(readiness["relationship_property_pruning_report_count"], 0);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_relationship_property_pruning_evidence_not_ready"));
}

#[test]
fn route_readiness_fails_closed_without_scan_pruning_report_details() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["scan_pruning_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("candidate_count_before_pruning");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_scan_pruning_profile_missing"));
}

#[test]
fn route_readiness_fails_closed_without_scan_pruning_target_kind() {
    let mut routes = ready_routes();
    routes[0]["query_reports"][0]["scan_pruning_reports"][0]
        .as_object_mut()
        .unwrap()
        .remove("target_kind");

    let readiness = nowledge_graph_route_readiness_json(&ready_evidence(routes)).unwrap();

    assert_eq!(readiness["route_primary_ready"], false);
    assert_eq!(readiness["route_query_runtime_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "query_report_scan_pruning_profile_missing"));
}

#[test]
fn route_readiness_fails_closed_when_evidence_envelope_is_not_ready() {
    let mut evidence = ready_evidence(ready_routes());
    evidence["ready"] = serde_json::json!(false);

    let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();

    assert_eq!(readiness["evidence_ready"], false);
    assert_eq!(readiness["route_primary_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "graph_route_evidence_not_ready"));
}

#[test]
fn route_readiness_fails_closed_when_evidence_protocol_is_missing() {
    let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
        "routes": ready_routes()
    }))
    .unwrap();

    assert_eq!(readiness["evidence_protocol"], serde_json::Value::Null);
    assert_eq!(readiness["route_primary_ready"], false);
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "graph_route_evidence_protocol_mismatch"));
    assert!(readiness["route_primary_blocker_codes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|code| code == "route_coverage_evidence_missing"));
}

pub(super) fn ready_evidence(routes: Vec<serde_json::Value>) -> serde_json::Value {
    let route_coverage = test_route_coverage(&routes);
    let mut evidence = serde_json::json!({
        "protocol": "nmem-graph-route-evidence-v1",
        "ready": true,
        "routes": routes
    });
    let object = evidence.as_object_mut().unwrap();
    for (key, value) in route_coverage {
        object.insert(key, value);
    }
    evidence
}

fn test_route_coverage(routes: &[serde_json::Value]) -> Vec<(String, serde_json::Value)> {
    let mut route_counts = std::collections::BTreeMap::<String, usize>::new();
    for route in routes
        .iter()
        .filter_map(|route| route.get("route").and_then(serde_json::Value::as_str))
    {
        *route_counts.entry(route.to_string()).or_default() += 1;
    }
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let route_names = route_counts
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let unknown_routes = route_names
        .iter()
        .filter(|route| !required_routes.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| route.as_str())
        .collect::<Vec<_>>();
    let required_routes_covered = missing_required_routes.is_empty();
    let mut blocker_codes = Vec::new();
    if !required_routes_covered {
        blocker_codes.push("missing_required_routes");
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("unknown_routes");
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("duplicate_routes");
    }
    vec![
        (
            "required_route_count".to_string(),
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()),
        ),
        (
            "covered_route_count".to_string(),
            serde_json::json!(covered_routes.len()),
        ),
        (
            "covered_routes".to_string(),
            serde_json::json!(covered_routes),
        ),
        (
            "missing_required_routes".to_string(),
            serde_json::json!(missing_required_routes),
        ),
        (
            "required_routes_covered".to_string(),
            serde_json::json!(required_routes_covered),
        ),
        (
            "unknown_routes".to_string(),
            serde_json::json!(unknown_routes),
        ),
        (
            "duplicate_routes".to_string(),
            serde_json::json!(duplicate_routes),
        ),
        (
            "route_catalog_version".to_string(),
            serde_json::json!(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION),
        ),
        (
            "route_catalog_digest".to_string(),
            serde_json::json!(nowledge_mem_graph_read_route_catalog_digest()),
        ),
        (
            "route_coverage_ready".to_string(),
            serde_json::json!(blocker_codes.is_empty()),
        ),
        (
            "route_coverage_blocker_codes".to_string(),
            serde_json::json!(blocker_codes),
        ),
    ]
}

pub(super) fn ready_routes() -> Vec<serde_json::Value> {
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| ready_route(route))
        .collect()
}

pub(super) fn ready_route(route: &str) -> serde_json::Value {
    let required_query_families =
        skein_route_ownership::graph::nowledge_mem_required_query_families_for_route(route);
    let query_family = required_query_families
        .first()
        .copied()
        .unwrap_or("memory_lookup");
    serde_json::json!({
        "route": route,
        "shadow_compare_ready": true,
        "shadow_compare_evidence_source": "route_parity_evidence",
        "shadow_compare": {
            "source": "route_parity_evidence",
            "ready": true,
            "matched_per_million": 1000000,
            "primary_engine": "kuzu",
            "shadow_engine": "skein",
            "blocker_codes": []
        },
        "primary_ready": true,
        "required_query_families": required_query_families,
        "relationship_property_pruning_required_count": 0,
        "relationship_property_pruning_report_count": 0,
        "relationship_property_pruning_evidence_ready": true,
        "query_reports": [ready_query_report(query_family)],
        "blocker_codes": []
    })
}

fn ready_query_report(query_family: &str) -> serde_json::Value {
    serde_json::json!({
        "query_name": "overview-memory-lookup",
        "query_index": 0,
        "query_family": query_family,
        "protocol": "skein-nowledge-mem-query-report-v1",
        "statement_kind": "match_return",
        "execution_path": "fast_path",
        "fast_path_selected": true,
        "slow_log_candidate": false,
        "physical_plan_captured": false,
        "elapsed_micros": 12,
        "physical_operator_counts": {
            "IndexNodeSeek": 1,
            "ProjectExec": 1
        },
        "optimizer_decision_count": 2,
        "optimizer_rule_event_count": 1,
        "output_row_shape": {
            "row_count": 1,
            "column_count": 2,
            "columns": ["m.id", "m.title"]
        },
        "scan_pruning_report_count": 1,
        "scan_pruning_reports": [
            {
                "target_kind": "node",
                "label_id": 1,
                "strategy": {
                    "kind": "property_eq",
                    "property": "id"
                },
                "pruned": true,
                "exact_empty": false,
                "candidate_count_before_pruning": 2,
                "pruned_candidate_count": 1,
                "candidate_count_before_filter": 1,
                "output_count": 1,
                "filtered_out_count": 0
            }
        ],
        "plan_cache": {
            "lookup": "miss",
            "cacheable": true,
            "hit": false,
            "miss": true,
            "bypassed": false
        },
        "api_behavior": {
            "include_metadata_false_strips_metadata": true,
            "ordering_contract_recorded": true,
            "pagination_contract_recorded": true,
            "error_class_stable": true,
            "statement_has_ordering": false,
            "statement_has_pagination": false
        }
    })
}
