use crate::{Result, SkeinError};
use std::path::Path;

pub use skein_readiness::integration_bundle::{
    nowledge_mem_integration_bundle_json, IntegrationBundleInputs,
};

#[cfg(test)]
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-library";
pub fn nowledge_mem_integration_bundle_usage() -> String {
    "nowledge-mem-integration-bundle requires [--require-ready] --submodule-path <path> --submodule-commit <commit> --legacy-data-retained --coexistence-mode shadow|side_by_side --content-store-present --content-store-engine sqlite --content-store-messages-available --content-store-source-chunks-available --previous-wrapper-preflight-json <path> --replacement-summary-json <path> --bounded-read-evidence-json <path> --graph-route-readiness-json <path> --route-ownership-json <path> --search-route-ownership-json <path> --active-search-route-ownership-json <path> --active-search-route-readiness-json <path> --query-runtime-preflight-json <path> --search-candidate-shadow-evidence-json <path> --library-readiness-json <path> --cutover-controls-json <path> --operations-readiness-json <path> --blackbox-manifest-json <path>"
        .to_string()
}

pub fn run_nowledge_mem_integration_bundle(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut inputs = IntegrationBundleInputs::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                inputs.require_ready = true;
            }
            "--submodule-path" => {
                inputs.submodule_path = Some(next_arg(&mut args)?);
            }
            "--submodule-commit" => {
                inputs.submodule_commit = Some(next_arg(&mut args)?);
            }
            "--legacy-data-retained" => {
                inputs.legacy_data_retained = true;
            }
            "--legacy-data-deleted" => {
                inputs.legacy_data_deleted = true;
            }
            "--coexistence-mode" => {
                inputs.coexistence_mode = Some(next_arg(&mut args)?);
            }
            "--content-store-present" => {
                inputs.content_store_present = true;
            }
            "--content-store-engine" => {
                inputs.content_store_engine = Some(next_arg(&mut args)?);
            }
            "--content-store-messages-available" => {
                inputs.content_store_messages_available = true;
            }
            "--content-store-source-chunks-available" => {
                inputs.content_store_source_chunks_available = true;
            }
            "--previous-wrapper-preflight-json" => {
                inputs.previous_wrapper_preflight = Some(read_json_arg(&mut args)?);
            }
            "--replacement-summary-json" => {
                inputs.replacement_summary = Some(read_json_arg(&mut args)?);
            }
            "--bounded-read-evidence-json" => {
                inputs.bounded_read_evidence = Some(read_json_arg(&mut args)?);
            }
            "--graph-route-readiness-json" => {
                inputs.graph_route_readiness = Some(read_json_arg(&mut args)?);
            }
            "--route-ownership-json" => {
                inputs.route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--search-route-ownership-json" => {
                inputs.search_route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--active-search-route-ownership-json" => {
                inputs.active_search_route_ownership = Some(read_json_arg(&mut args)?);
            }
            "--active-search-route-readiness-json" => {
                inputs.active_search_route_readiness = Some(read_json_arg(&mut args)?);
            }
            "--query-runtime-preflight-json" => {
                inputs.query_runtime_preflight = Some(read_json_arg(&mut args)?);
            }
            "--search-candidate-shadow-evidence-json" => {
                inputs.search_candidate_shadow_evidence = Some(read_json_arg(&mut args)?);
            }
            "--library-readiness-json" => {
                inputs.library_readiness = Some(read_json_arg(&mut args)?);
            }
            "--cutover-controls-json" => {
                inputs.cutover_controls = Some(read_json_arg(&mut args)?);
            }
            "--operations-readiness-json" => {
                inputs.operations_readiness = Some(read_json_arg(&mut args)?);
            }
            "--blackbox-manifest-json" => {
                inputs.blackbox_manifest = Some(read_json_arg(&mut args)?);
            }
            _ => {
                return Err(SkeinError::Semantic(nowledge_mem_integration_bundle_usage()));
            }
        }
    }

    let require_ready = inputs.require_ready;
    Ok((nowledge_mem_integration_bundle_json(inputs)?, require_ready))
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let path = next_arg(args)?;
    read_json_file(Path::new(&path))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|_| {
        SkeinError::Execution(
            "failed to read Nowledge Mem integration bundle input: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse Nowledge Mem integration bundle input: invalid_json".to_string(),
        )
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_mem_integration_bundle_usage()))
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_integration_bundle_json, IntegrationBundleInputs,
        SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
    };
    use crate::{
        nowledge_mem_active_search_route_ownership_all_skein,
        nowledge_mem_active_search_route_ownership_readiness,
        nowledge_mem_active_search_route_read_evidence_all_skein_ready,
        nowledge_mem_active_search_route_readiness, nowledge_mem_graph_read_route_catalog_digest,
        nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_specs_json,
        nowledge_mem_integration_readiness_json, nowledge_mem_route_ownership_all_skein,
        nowledge_mem_route_ownership_readiness, nowledge_mem_search_candidate_shadow_evidence_json,
        nowledge_mem_search_route_ownership_all_skein,
        nowledge_mem_search_route_ownership_readiness, NowledgeMemRouteOwnershipPolicy,
        NowledgeMemRouteReadinessSummary, NowledgeMemSearchCandidateShadowAccumulator,
        NowledgeMemSearchRouteOwnershipPolicy, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
        NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES, REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn facade_preserves_owner_type_and_function_identity() {
        use skein_readiness::{graph_summary, integration_bundle};
        use std::any::TypeId;

        assert_eq!(
            TypeId::of::<crate::IntegrationBundleInputs>(),
            TypeId::of::<integration_bundle::IntegrationBundleInputs>()
        );
        assert_eq!(
            TypeId::of::<crate::GraphRouteReadinessSummary>(),
            TypeId::of::<graph_summary::GraphRouteReadinessSummary>()
        );
        let owner: fn(
            integration_bundle::IntegrationBundleInputs,
        ) -> crate::Result<serde_json::Value> = crate::nowledge_mem_integration_bundle_json;
        assert!(std::ptr::fn_addr_eq(
            owner,
            integration_bundle::nowledge_mem_integration_bundle_json as fn(_) -> _
        ));
        let summary: fn(&serde_json::Value) -> graph_summary::GraphRouteReadinessSummary =
            crate::nowledge_graph_route_readiness_summary;
        assert!(std::ptr::fn_addr_eq(
            summary,
            graph_summary::nowledge_graph_route_readiness_summary as fn(_) -> _
        ));
        let from_bundle: fn(&serde_json::Value) -> graph_summary::GraphRouteReadinessSummary =
            crate::nowledge_graph_route_readiness_summary_from_bundle;
        assert!(std::ptr::fn_addr_eq(
            from_bundle,
            graph_summary::nowledge_graph_route_readiness_summary_from_bundle as fn(_) -> _
        ));
    }

    #[test]
    fn generated_bundle_feeds_integration_readiness_gate() {
        let bundle = nowledge_mem_integration_bundle_json(ready_inputs()).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(bundle["protocol"], "nowledge-mem-skein-integration-bundle");
        assert_eq!(bundle["submodule"]["path"], "skein");
        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_query_runtime_alignment"]["ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["evidence_protocol_matches"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["evidence_ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]
                ["evidence_route_catalog_metadata_ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]
                ["summary_route_catalog_metadata_ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]
                ["route_catalog_metadata_ready_matches"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_search_route_ownership_alignment"]["ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_active_search_route_ownership_alignment"]["ready"],
            true
        );
        assert_eq!(
            bundle["replacement_summary_active_search_route_readiness_alignment"]["ready"],
            true
        );
        assert_eq!(
            bundle["blackbox_manifest"]["protocol"],
            "skein-blackbox-report-v1"
        );
        assert_eq!(
            bundle["cutover_controls"]["protocol"],
            NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL
        );
        assert_eq!(bundle["cutover_controls"]["ready"], true);
        assert_eq!(
            bundle["operations_readiness"]["protocol"],
            NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL
        );
        assert_eq!(bundle["operations_readiness"]["ready"], true);
        assert_eq!(
            bundle["blackbox_manifest"]["redaction"]["raw_query_text_copied"],
            false
        );
        assert!(bundle["blackbox_manifest"]["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| artifact["name"] == "slow-query-log.jsonl"));
        assert!(bundle["blackbox_manifest"]["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| artifact["background_qos"]["protocol"]
                == "skein-background-maintenance-report"));
        assert!(bundle["blackbox_manifest"]["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|artifact| artifact["background_qos"]["memory_pressure_ready"] == true));
        assert_eq!(readiness["ready"], true);
        assert_eq!(readiness["failed_checks"], serde_json::json!([]));
    }

    #[test]
    fn bundle_input_read_errors_are_redacted_by_default() {
        let secret_path =
            unique_test_path("bundle-input-secret-path-do-not-emit").join("missing-secret.json");

        let error = super::read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read Nowledge Mem integration bundle input: io_error"
        );
        assert!(!error.contains("bundle-input-secret-path-do-not-emit"));
        assert!(!error.contains("missing-secret"));
    }

    #[test]
    fn bundle_input_parse_errors_are_redacted_by_default() {
        let root = unique_test_path("bundle-input-parse-redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-bundle-input-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"secret\": \"bundle-input-parse-secret-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse Nowledge Mem integration bundle input: invalid_json"
        );
        assert!(!error.contains("secret-bundle-input-path-do-not-emit"));
        assert!(!error.contains("bundle-input-parse-secret-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn generated_bundle_fails_closed_when_legacy_data_is_not_retained() {
        let mut inputs = ready_inputs();
        inputs.legacy_data_retained = false;

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["coexistence"]["blocker_codes"],
            serde_json::json!(["legacy_data_not_retained"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["legacy_coexistence"])
        );
    }

    #[test]
    fn generated_bundle_detects_bounded_read_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.bounded_read_evidence.as_mut().unwrap()["max_rows"] = serde_json::json!(128);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"],
            serde_json::json!(["bounded_read_max_rows_mismatch"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_bounded_read_payload_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.bounded_read_evidence.as_mut().unwrap()["payload_budget_exceeded"] =
            serde_json::json!(true);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["payload_budget_exceeded_matches"],
            false
        );
        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"],
            serde_json::json!(["bounded_read_payload_budget_exceeded_mismatch"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_stale_bounded_read_route_catalog() {
        let mut inputs = ready_inputs();
        inputs.bounded_read_evidence.as_mut().unwrap()["route_catalog_digest"] =
            serde_json::json!("fnv1a64:stale");

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["route_catalog_digest_matches"],
            false
        );
        assert_eq!(
            bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"],
            serde_json::json!([
                "bounded_read_route_catalog_digest_not_ready",
                "bounded_read_route_catalog_digest_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_query_runtime_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.query_runtime_preflight.as_mut().unwrap()["probes"]
            .as_array_mut()
            .unwrap()
            .pop();

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_query_runtime_alignment"]["blocker_codes"],
            serde_json::json!([
                "query_runtime_preflight_covered_routes_mismatch",
                "query_runtime_preflight_route_coverage_ready_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!([
                "query_runtime_preflight",
                "query_runtime_preflight_alignment"
            ])
        );
    }

    #[test]
    fn generated_bundle_detects_stale_query_runtime_route_catalog() {
        let mut inputs = ready_inputs();
        inputs.query_runtime_preflight.as_mut().unwrap()["route_catalog_digest"] =
            serde_json::json!("fnv1a64:stale");

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_query_runtime_alignment"]["blocker_codes"],
            serde_json::json!([
                "query_runtime_preflight_route_coverage_ready_mismatch",
                "query_runtime_preflight_route_catalog_digest_not_ready",
                "query_runtime_preflight_route_catalog_digest_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!([
                "query_runtime_preflight",
                "query_runtime_preflight_alignment"
            ])
        );
    }

    #[test]
    fn generated_bundle_detects_unready_graph_route_evidence_envelope() {
        let mut inputs = ready_inputs();
        inputs.graph_route_readiness.as_mut().unwrap()["evidence_ready"] = serde_json::json!(false);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["blocker_codes"],
            serde_json::json!(["graph_route_evidence_not_ready"])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["graph_route_readiness", "graph_route_readiness_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_stale_graph_route_catalog() {
        let mut inputs = ready_inputs();
        inputs.graph_route_readiness.as_mut().unwrap()["route_catalog_digest"] =
            serde_json::json!("fnv1a64:stale");

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["blocker_codes"],
            serde_json::json!([
                "graph_route_catalog_digest_not_ready",
                "graph_route_catalog_digest_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_search_route_ownership_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.replacement_summary.as_mut().unwrap()["search_route_ownership"]
            ["lancedb_route_count"] = serde_json::json!(1);
        inputs.replacement_summary.as_mut().unwrap()["search_route_ownership"]["lancedb_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES[0]]);
        inputs.replacement_summary.as_mut().unwrap()["search_route_ownership"]["ready"] =
            serde_json::json!(false);
        inputs.replacement_summary.as_mut().unwrap()["search_route_ownership"]
            ["production_cutover_ready"] = serde_json::json!(false);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_search_route_ownership_alignment"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            bundle["replacement_summary_search_route_ownership_alignment"]["blocker_codes"],
            serde_json::json!([
                "search_route_ownership_ready_mismatch",
                "search_route_ownership_cutover_ready_mismatch",
                "search_route_ownership_lancedb_count_mismatch",
                "search_route_ownership_lancedb_routes_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["search_route_ownership_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_active_search_route_ownership_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.active_search_route_ownership.as_mut().unwrap()["lancedb_route_count"] =
            serde_json::json!(1);
        inputs.active_search_route_ownership.as_mut().unwrap()["lancedb_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES[0]]);
        inputs.active_search_route_ownership.as_mut().unwrap()["ready"] = serde_json::json!(false);
        inputs.active_search_route_ownership.as_mut().unwrap()["production_cutover_ready"] =
            serde_json::json!(false);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_active_search_route_ownership_alignment"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            bundle["replacement_summary_active_search_route_ownership_alignment"]["blocker_codes"],
            serde_json::json!([
                "search_route_ownership_ready_mismatch",
                "search_route_ownership_cutover_ready_mismatch",
                "search_route_ownership_lancedb_count_mismatch",
                "search_route_ownership_lancedb_routes_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["active_search_route_ownership_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_active_search_route_readiness_alignment_mismatch() {
        let mut inputs = ready_inputs();
        inputs.active_search_route_readiness.as_mut().unwrap()
            ["lancedb_handle_required_route_count"] = serde_json::json!(1);
        inputs.active_search_route_readiness.as_mut().unwrap()["lancedb_handle_required_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES[0]]);
        inputs.active_search_route_readiness.as_mut().unwrap()["ready"] = serde_json::json!(false);
        inputs.active_search_route_readiness.as_mut().unwrap()["production_cutover_ready"] =
            serde_json::json!(false);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_active_search_route_readiness_alignment"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            bundle["replacement_summary_active_search_route_readiness_alignment"]["blocker_codes"],
            serde_json::json!([
                "active_search_route_readiness_ready_mismatch",
                "active_search_route_readiness_cutover_ready_mismatch",
                "active_search_route_readiness_lancedb_handle_count_mismatch",
                "active_search_route_readiness_lancedb_handle_routes_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["active_search_route_readiness_alignment"])
        );
    }

    #[test]
    fn generated_bundle_detects_graph_route_pruning_summary_mismatch() {
        let mut inputs = ready_inputs();
        inputs.replacement_summary.as_mut().unwrap()["graph_route_readiness"]
            ["relationship_property_pruning_report_count"] = serde_json::json!(1);
        inputs.replacement_summary.as_mut().unwrap()["graph_route_readiness"]
            ["route_relationship_property_pruning_evidence_ready"] = serde_json::json!(false);

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            bundle["replacement_summary_graph_route_alignment"]["blocker_codes"],
            serde_json::json!([
                "replacement_summary_relationship_property_pruning_evidence_not_ready",
                "graph_route_relationship_property_pruning_evidence_mismatch",
                "graph_route_relationship_property_pruning_report_count_mismatch"
            ])
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(
            readiness["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
    }

    #[test]
    fn generated_bundle_recomputes_graph_route_parity_identity() {
        let mut inputs = ready_inputs();
        inputs.graph_route_readiness.as_mut().unwrap()["routes"][0]["shadow_compare"]
            ["matched_per_million"] = serde_json::json!(999999);
        inputs.graph_route_readiness.as_mut().unwrap()["routes"][0]["shadow_compare"]
            ["primary_engine"] = serde_json::json!("skein");

        let bundle = nowledge_mem_integration_bundle_json(inputs).unwrap();
        let readiness = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(
            bundle["graph_route_parity_alignment"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            bundle["graph_route_parity_alignment"]["not_ready_routes"],
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]])
        );
        assert_eq!(readiness["ready"], false);
        assert!(readiness["failed_checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check == "graph_route_parity_alignment"));
    }

    fn ready_inputs() -> IntegrationBundleInputs {
        IntegrationBundleInputs {
            require_ready: false,
            submodule_path: Some("/redacted/vendor/skein".to_string()),
            submodule_commit: Some("abc1234".to_string()),
            legacy_data_retained: true,
            legacy_data_deleted: false,
            coexistence_mode: Some("shadow".to_string()),
            content_store_present: true,
            content_store_engine: Some("sqlite".to_string()),
            content_store_messages_available: true,
            content_store_source_chunks_available: true,
            previous_wrapper_preflight: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": [],
                "failed_checks": []
            })),
            replacement_summary: Some(ready_replacement_summary()),
            bounded_read_evidence: Some(ready_bounded_read_evidence()),
            graph_route_readiness: Some(ready_graph_route_readiness()),
            route_ownership: Some(ready_route_ownership()),
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            active_search_route_readiness: Some(ready_active_search_route_readiness()),
            query_runtime_preflight: Some(ready_query_runtime_preflight()),
            search_candidate_shadow_evidence: Some(ready_search_candidate_shadow_evidence()),
            library_readiness: Some(ready_library_readiness()),
            cutover_controls: Some(ready_cutover_controls()),
            operations_readiness: Some(ready_operations_readiness()),
            blackbox_manifest: Some(ready_blackbox_manifest()),
        }
    }

    fn ready_cutover_controls() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
            "ready": true,
            "controls": {
                "graph_reads": "skein",
                "search_reads": "skein",
                "dual_writes": "enabled",
                "initial_import": "disabled",
                "projection_catch_up": "enabled"
            },
            "graph": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "search": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "work": {
                "dual_writes_enabled": true,
                "initial_import_enabled": false,
                "initial_import_inactive_for_cutover": true,
                "initial_import_cutover_catch_up_ready": false,
                "initial_import_safe_for_read_cutover": true,
                "projection_catch_up_enabled": true
            },
            "blocker_codes": [],
            "production_status": {
                "graph": {
                    "skein_cutover_effective": true
                },
                "search": {
                    "skein_cutover_effective": true
                }
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            }
        })
    }

    fn ready_operations_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
            "present": true,
            "ready": true,
            "graph": {
                "open": true,
                "read_only": false,
                "commit_epoch": 7
            },
            "search_projection": {
                "open": true,
                "commit_lag": 0,
                "stale": false
            },
            "storage_lifecycle": {
                "ready": true,
                "action": "ready"
            },
            "readiness": {
                "storage_lifecycle_ready": true,
                "storage_recovery_ready": true,
                "slow_query_ready": true,
                "background_maintenance_ready": true
            },
            "blocker_codes": [],
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            }
        })
    }

    fn ready_replacement_summary() -> serde_json::Value {
        let mut summary = serde_json::json!({
            "protocol": "skein-nowledge-replacement-summary",
            "production_cutover_ready": true,
            "blocking_categories": [],
            "missing_evidence": [],
            "shadow_evidence": {
                "ready": true
            },
            "dual_engine_evidence": {
                "present": true,
                "ready": true,
                "consistent": true
            },
            "source_mutation_dual_write_readiness": {
                "protocol": NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
                "ready": true,
                "required_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
                "evidence_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
                "ready_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
                "missing_required_families": [],
                "blocker_codes": []
            },
            "replacement_readiness_family_summary": {
                "min_replacement_readiness_per_million": 1_000_000,
                "blocked_query_families": [],
                "required_query_families": [
                    "memory_lookup",
                    "graph_traversal",
                    "projected_graph",
                    "label_stats_read",
                    "search_projection"
                ],
                "missing_required_query_families": []
            },
            "search_projection_evidence": {
                "protocol": "skein-nowledge-search-projection-evidence",
                "ready": true,
                "fts_ready": true,
                "vector_ready": true,
                "document_identity_ready": true,
                "incremental_update_ready": true,
                "predicate_pushdown_ready": true,
                "production_filter_pruning_ready": true,
                "compressed_vector_projection_required": true,
                "compressed_vector_projection_ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "protocol": "skein-nowledge-search-projection-shadow-evidence",
                "evidence_source": SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
                "present": true,
                "ready": true,
                "document_count_parity": true,
                "document_identity_parity": true,
                "table_parity_ready": true,
                "embedding_identity_parity": true,
                "incremental_watermark_parity": true,
                "pushdown_evidence": {
                    "ready": true,
                    "shadow_segment_descriptor_scan_filter_fields_ready": true,
                    "shadow_segment_document_pruning_ready": true,
                    "shadow_segment_pruning_candidate_document_count": 4,
                    "shadow_segment_pruned_document_count": 2,
                    "shadow_segment_scanned_document_count": 2,
                    "primary_scan_filter_fields": scan_filter_fields_json(),
                    "shadow_scan_filter_fields": scan_filter_fields_json(),
                    "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
                },
                "blocker_codes": []
            },
            "bounded_read_evidence": ready_bounded_read_evidence(),
            "graph_route_readiness": ready_graph_route_readiness(),
            "query_runtime_preflight": ready_replacement_summary_query_runtime_preflight(),
            "cutover_evidence": {
                "storage_recovery_required": true,
                "storage_recovery_ready": true,
                "storage_recovery_protocol_matches": true,
                "storage_recovery_durable": true,
                "storage_recovery_checkpoint_boundary_present": true,
                "storage_recovery_wal_replay_bounded": true,
                "storage_recovery_replay_boundary_consistent": true,
                "storage_recovery_torn_tail_clean": true,
                "storage_recovery_blocker_codes": [],
                "storage_recovery_blockers": [],
                "background_maintenance_required": true,
                "background_maintenance_ready": true,
                "background_maintenance_protocol_matches": true,
                "background_maintenance_executable_search_projection_graph_delta_count": 1,
                "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                "background_maintenance_deferred_search_projection_graph_delta_count": 0,
                "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                "background_maintenance_executable_search_projection_graph_delta_operations": 2,
                "background_maintenance_admitted_search_projection_graph_delta_operations": 2,
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                "background_maintenance_foreground_admission_probe_ready": true,
                "background_maintenance_foreground_admission_probe_admission": "admit",
                "background_maintenance_memory_pressure_ready": true,
                "background_maintenance_memory_budget_bytes": 4096,
                "background_maintenance_estimated_memory_bytes": 1024,
                "background_maintenance_blocker_codes": [],
                "background_maintenance_blockers": []
            }
        });
        summary["replacement_boundaries"] = serde_json::json!({
            "graph_layer": {
                "scope": "kuzu_ladybug_graph_layer",
                "replacement_role": "primary_replacement",
                "storage_owner": "skein"
            },
            "search_projection": {
                "scope": "lancedb_search_projection",
                "replacement_role": "rebuildable_projection",
                "storage_owner": "skein"
            },
            "content_store": {
                "scope": "sqlite_content_store",
                "replacement_role": "external_out_of_scope",
                "storage_owner": "nowledge_mem"
            },
            "large_blob_store": {
                "scope": "large_blob_value_store",
                "replacement_role": "external_out_of_scope",
                "storage_owner": "nowledge_mem"
            }
        });
        summary["search_route_ownership"] = ready_search_route_ownership();
        summary["active_search_route_ownership"] = ready_active_search_route_ownership();
        summary["active_search_route_readiness"] = ready_active_search_route_readiness();
        summary
    }

    fn scan_filter_fields_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
    }

    fn scan_filter_field_summaries_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied()
            .chain(std::iter::once("document_id"))
            .map(|field| {
                serde_json::json!({
                    "field": field,
                    "segment_count": 1,
                    "present_document_count": 1,
                    "value_summary_used": true,
                    "value_summary_segment_count": 1,
                    "numeric_range_summary_used": matches!(field, "importance" | "confidence"),
                    "numeric_range_segment_count": usize::from(matches!(
                        field,
                        "importance" | "confidence"
                    )),
                    "timestamp_range_summary_used": matches!(
                        field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    ),
                    "timestamp_range_segment_count": usize::from(matches!(
                        field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    )),
                    "unique_key_summary_used": field == "document_id",
                    "unique_key_summary_segment_count": usize::from(field == "document_id"),
                })
            })
            .collect::<Vec<_>>())
    }

    fn ready_replacement_summary_query_runtime_preflight() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "present": true,
            "ready": true,
            "database_opened": true,
            "probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "passed_probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "unknown_routes": [],
            "duplicate_routes": [],
            "required_routes_covered": true,
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "probe_details_ready": true,
            "blocker_codes": []
        })
    }

    fn ready_bounded_read_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-bounded-read-evidence-v2",
            "present": true,
            "ready": true,
            "route_primary_ready": true,
            "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_report_count": 0,
            "route_relationship_property_pruning_evidence_ready": true,
            "mode": "shadow_read_only",
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 128,
            "max_estimated_payload_bytes": 4194304,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "blocking_operator_count": 0,
            "blocking_operator_memory_reports_complete": true,
            "blocking_operator_memory_within_budget": true,
            "spill_within_budget": true,
            "streaming": false,
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "blocker_codes": []
        })
    }

    fn ready_route_ownership() -> serde_json::Value {
        nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_search_route_ownership() -> serde_json::Value {
        nowledge_mem_search_route_ownership_readiness(
            &nowledge_mem_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_active_search_route_ownership() -> serde_json::Value {
        nowledge_mem_active_search_route_ownership_readiness(
            &nowledge_mem_active_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_active_search_route_readiness() -> serde_json::Value {
        nowledge_mem_active_search_route_readiness(
            &nowledge_mem_active_search_route_read_evidence_all_skein_ready(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_route_readiness_summary() -> NowledgeMemRouteReadinessSummary {
        NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| (*route).to_string())
                .collect(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        }
    }

    fn ready_graph_route_readiness() -> serde_json::Value {
        let routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                let required_query_families =
                    crate::nowledge_mem_required_query_families_for_route(route);
                let query_family = required_query_families
                    .first()
                    .copied()
                    .unwrap_or("memory_lookup");
                let mut object = serde_json::Map::new();
                object.insert("route".to_string(), serde_json::json!(route));
                let spec = nowledge_mem_graph_read_route_spec(route).unwrap();
                object.insert("owner".to_string(), serde_json::json!(spec.owner.as_str()));
                object.insert(
                    "required_evidence_kind".to_string(),
                    serde_json::json!(spec.required_evidence_kind.as_str()),
                );
                object.insert(
                    "stale_on_catalog_change".to_string(),
                    serde_json::json!(spec.stale_on_catalog_change),
                );
                object.insert("shadow_compare_ready".to_string(), serde_json::json!(true));
                object.insert(
                    "shadow_compare_evidence_source".to_string(),
                    serde_json::json!("route_parity_evidence"),
                );
                object.insert(
                    "shadow_compare".to_string(),
                    serde_json::json!({
                        "source": "route_parity_evidence",
                        "ready": true,
                        "matched_per_million": 1000000,
                        "primary_engine": "kuzu",
                        "shadow_engine": "skein",
                        "blocker_codes": [],
                        "computed_blocker_codes": []
                    }),
                );
                object.insert("primary_ready".to_string(), serde_json::json!(true));
                object.insert(
                    "required_query_families".to_string(),
                    serde_json::json!(required_query_families),
                );
                object.insert(
                    "computed_required_query_families".to_string(),
                    serde_json::json!(required_query_families),
                );
                object.insert(
                    "query_family_blocker_codes".to_string(),
                    serde_json::json!([]),
                );
                object.insert("query_runtime_ready".to_string(), serde_json::json!(true));
                object.insert("query_report_count".to_string(), serde_json::json!(1));
                object.insert(
                    "query_runtime_report_count".to_string(),
                    serde_json::json!(1),
                );
                object.insert(
                    "query_runtime_plan_report_count".to_string(),
                    serde_json::json!(1),
                );
                object.insert(
                    "query_runtime_profile_report_count".to_string(),
                    serde_json::json!(1),
                );
                object.insert(
                    "query_runtime_failed_query_count".to_string(),
                    serde_json::json!(0),
                );
                object.insert(
                    "query_runtime_missing_plan_evidence_count".to_string(),
                    serde_json::json!(0),
                );
                object.insert(
                    "query_runtime_missing_profile_evidence_count".to_string(),
                    serde_json::json!(0),
                );
                object.insert(
                    "relationship_property_pruning_required_count".to_string(),
                    serde_json::json!(0),
                );
                object.insert(
                    "relationship_property_pruning_report_count".to_string(),
                    serde_json::json!(0),
                );
                object.insert(
                    "relationship_property_pruning_evidence_ready".to_string(),
                    serde_json::json!(true),
                );
                object.insert(
                    "query_plan_evidence_ready".to_string(),
                    serde_json::json!(true),
                );
                object.insert(
                    "query_profile_evidence_ready".to_string(),
                    serde_json::json!(true),
                );
                object.insert(
                    "query_reports".to_string(),
                    serde_json::json!([ready_graph_route_query_report(query_family)]),
                );
                object.insert("blocker_codes".to_string(), serde_json::json!([]));
                serde_json::Value::Object(object)
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "protocol": "nmem-graph-route-readiness-v1",
            "evidence_protocol": "nmem-graph-route-evidence-v1",
            "evidence_ready": true,
            "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "evidence_route_coverage_present": true,
            "evidence_route_coverage_matches": true,
            "evidence_route_coverage_blocker_codes": [],
            "primary_ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_plan_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_profile_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_api_behavior_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "query_runtime_failed_query_count": 0,
            "query_runtime_missing_plan_evidence_count": 0,
            "query_runtime_missing_profile_evidence_count": 0,
            "relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_report_count": 0,
            "missing_query_runtime_routes": [],
            "route_query_runtime_ready": true,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_query_api_behavior_evidence_ready": true,
            "route_relationship_property_pruning_evidence_ready": true,
            "route_primary_ready": true,
            "route_primary_blocker_codes": [],
            "route_catalog": nowledge_mem_graph_read_route_specs_json(),
            "routes": routes
        })
    }

    fn ready_graph_route_query_report(query_family: &str) -> serde_json::Value {
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
            "physical_operator_counts_present": true,
            "optimizer_decision_count": 2,
            "optimizer_rule_event_count": 1,
            "scan_pruning_report_count": 1,
            "scan_pruning_reports_present": true,
            "scan_pruning_reports": [
                {
                    "target_kind": "node",
                    "label_id": 1,
                    "rel_type_id": null,
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
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "ready": true,
            "blocker_codes": []
        })
    }

    fn ready_search_candidate_shadow_evidence() -> serde_json::Value {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(
            &["mem_1", "mem_2", "mem_3"],
            &["mem_1", "mem_2", "mem_3"],
        );
        accumulator.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        let mut evidence =
            nowledge_mem_search_candidate_shadow_evidence_json(&accumulator.evidence());
        evidence["text_retriever_ready"] = serde_json::json!(true);
        evidence["vector_retriever_ready"] = serde_json::json!(true);
        evidence["retriever_leg_candidate_counts"] = serde_json::json!({
            "text": 3,
            "vector": 3,
        });
        evidence["fts_top_k_overlap_ready"] = serde_json::json!(true);
        evidence["vector_top_k_overlap_ready"] = serde_json::json!(true);
        evidence["top_k_overlap_observed"] = serde_json::json!({
            "fts": true,
            "vector": true,
        });
        evidence["candidate_readiness"] = serde_json::json!({
            "source_chunk_identity_ready": true,
            "fail_soft_observed": true,
            "projection_marker_status_visible": true,
            "projection_watermark_ready": true,
            "embedding_identity_ready": true,
        });
        evidence
    }

    fn ready_query_runtime_preflight() -> serde_json::Value {
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_query_runtime_preflight_probe(route))
            .collect::<Vec<_>>();
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "redaction": {
                "ready": true,
                "rows_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
                "raw_errors_copied": false
            },
            "probe_count": probes.len(),
            "passed_probe_count": probes.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "unknown_routes": [],
            "duplicate_routes": [],
            "required_routes_covered": true,
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "blocker_codes": [],
            "probes": probes
        })
    }

    fn ready_query_runtime_preflight_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "name": format!("probe:{route}"),
            "route": route,
            "query_family": "memory_lookup",
            "ready": true,
            "success": true,
            "output_row_count": 1,
            "selected_plan_fingerprint": "IndexNodeSeek(1:m:6:Memory)",
            "selected_plan_operator_counts": {
                "IndexNodeSeek": 1,
                "ProjectExec": 1
            },
            "selected_plan_class_counts": {
                "access": 1,
                "relational": 1
            },
            "optimizer_decision_count": 2,
            "optimizer_rule_event_count": 1,
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "bypass_reason": null,
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "execution_profile": {
                "scan_pruning_report_count": 1,
                "pruned_scan_count": 1,
                "scan_pruning_reports": [
                    {
                        "target_kind": "node",
                        "label_id": 1,
                        "rel_type_id": null,
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
                ]
            },
            "blocker_codes": []
        })
    }

    fn ready_library_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-library-readiness-v1",
            "present": true,
            "ready": true,
            "ready_area_count": 11,
            "blocked_area_count": 0,
            "blocker_codes": [],
            "redaction": {
                "ready": true,
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "production_path": {
                "ready": true,
                "in_process": true,
                "cli_required": false,
                "env_control_plane_required": false,
                "spawned_helper_required": false
            },
            "open_report": {
                "graph_opened": true,
                "search_projection_opened": true
            },
            "production_resource_profile": ready_production_resource_profile(),
            "readiness_by_area": {
                "graph": {"ready": true, "blocker_codes": []},
                "query": {"ready": true, "blocker_codes": []},
                "storage": {"ready": true, "blocker_codes": []},
                "background": {"ready": true, "blocker_codes": []},
                "query_family": {"ready": true, "blocker_codes": []},
                "graph_route": {"ready": true, "blocker_codes": []},
                "search_route_ownership": {"ready": true, "blocker_codes": []},
                "search_projection": {"ready": true, "blocker_codes": []},
                "search_projection_shadow": {"ready": true, "blocker_codes": []},
                "search_candidate_shadow": {"ready": true, "blocker_codes": []},
                "workload_fixture": {"ready": true, "blocker_codes": []}
            }
        })
    }

    fn ready_production_resource_profile() -> serde_json::Value {
        serde_json::json!({
            "protocol": crate::STORAGE_RESOURCE_PROFILE_PROTOCOL,
            "protocol_version": 2,
            "present": true,
            "resource_ready": true,
            "ready": true,
            "blocker_codes": [],
            "evidence_binding": {
                "identity": production_identity(),
                "generated_at_unix_seconds": 1
            },
            "expected_identity": production_identity(),
            "canonical_graph_commit_epoch": 42,
            "identity_matches_expected": true,
            "limits": {
                "min_canonical_artifact_bytes": 536870912u64,
                "max_steady_resident_bytes": 536870912u64,
                "max_peak_resident_bytes": 805306368u64,
                "max_total_page_faults": 1010,
                "max_minor_page_faults": 1000,
                "max_major_page_faults": 10,
                "max_intermediate_rows": 1000,
                "max_intermediate_payload_bytes": 1048576,
                "max_output_rows": 100,
                "max_output_payload_bytes": 1048576,
                "require_fully_streamed": true
            },
            "storage": {
                "durable": true,
                "out_of_core": true,
                "canonical_artifact_bytes": 1073741824u64,
                "canonical_exceeds_cache": true,
                "segment_cache_capacity_bytes": 67108864,
                "segment_cache_resident_bytes_after": 33554432,
                "delta_within_budget": true
            },
            "execution": {
                "fully_streamed": true,
                "start_resident_bytes": 251658240,
                "start_peak_resident_bytes": 377487360,
                "steady_resident_bytes": 268435456,
                "peak_resident_bytes": 402653184,
                "steady_resident_growth_bytes": 16777216,
                "lifetime_peak_resident_growth_bytes": 25165824,
                "total_page_faults": 100,
                "minor_page_faults": 100,
                "major_page_faults": 0,
                "metric_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": true
                },
                "intermediate_rows": 200,
                "intermediate_payload_bytes": 524288,
                "output_rows": 100,
                "output_payload_bytes": 262144
            }
        })
    }

    fn production_identity() -> serde_json::Value {
        serde_json::json!({
            "source_revision": "test-revision",
            "rust_toolchain": "test-toolchain",
            "target_os": "linux",
            "target_arch": "x86_64",
            "enabled_features": ["full-text-search", "vector-search"],
            "durable_format_version": 1,
            "schema_version": 1,
            "configuration_digest": "test-config",
            "deployment_profile": "production-replica",
            "dataset_fingerprint": "test-dataset",
            "canonical_graph_commit_epoch": 42,
            "policy_version": crate::PRODUCTION_QUALIFICATION_POLICY_VERSION
        })
    }

    fn ready_blackbox_manifest() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-blackbox-report-v1",
            "artifact_dir_present": true,
            "artifact_count": 2,
            "events_path": "events.jsonl",
            "artifacts": [
                {
                    "name": "slow-query-log.jsonl",
                    "format": "jsonl",
                    "byte_len": 0,
                    "checksum": 0,
                    "jsonl": {
                        "line_count": 0,
                        "nonempty_line_count": 0
                    }
                },
                {
                    "name": "background-maintenance.json",
                    "format": "json",
                    "byte_len": 256,
                    "checksum": 2,
                    "json": {
                        "parse_ready": true,
                        "protocol": "skein-background-maintenance-report",
                        "ready": true,
                        "blocker_codes": []
                    },
                    "background_qos": {
                        "protocol": "skein-background-maintenance-report",
                        "ready": true,
                        "total_candidates": 1,
                        "admitted_count": 1,
                        "deferred_count": 0,
                        "rejected_count": 0,
                        "executable_search_projection_graph_delta_count": 1,
                        "admitted_search_projection_graph_delta_count": 1,
                        "deferred_search_projection_graph_delta_count": 0,
                        "rejected_search_projection_graph_delta_count": 0,
                        "executable_search_projection_graph_delta_operations": 2,
                        "admitted_search_projection_graph_delta_operations": 2,
                        "max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                        "memory_pressure_ready": true,
                        "memory_budget_bytes": 4096,
                        "estimated_memory_bytes": 1024,
                        "blocker_codes": []
                    }
                }
            ],
            "redaction": {
                "raw_query_text_copied": false,
                "raw_parameters_copied": false,
                "raw_artifact_payloads_copied": false,
                "artifact_paths_are_relative": true
            }
        })
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }
}
