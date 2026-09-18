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

use super::nowledge_graph_route_readiness_json;
use super::tests::{ready_evidence, ready_route, ready_routes};
use hawdb_core::HawDBError;
use hawdb_route_ownership::graph::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;
use serde_json::{json, Value};

#[test]
fn graph_route_readiness_differential_smoke() {
    campaign(4);
}

#[test]
#[ignore = "explicit local generated readiness campaign"]
fn graph_route_readiness_differential_campaign() {
    campaign(128);
}

fn campaign(seeds: u64) {
    let mutations = rejected_evidence_mutations();
    let mut coverage_cases = 0;
    let mut contradiction_cases = 0;
    let mut accepted_cases = 0;
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        for _ in 0..16 {
            let mut names = Vec::new();
            for &route in REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES {
                for _ in 0..rng.below(3) {
                    names.push(route.to_string());
                }
            }
            if rng.below(2) == 0 {
                names.push(format!("/unknown/{seed}"));
            }
            shuffle(&mut names, &mut rng);
            check_coverage(&names, seed);
            coverage_cases += 1;
        }
        // Include admitted, empty, duplicate-only and unknown-only inventories,
        // instead of relying on random subsets to reach each boundary.
        let complete = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect::<Vec<_>>();
        for names in [
            complete,
            Vec::new(),
            vec![REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0].to_string(); 2],
            vec!["/unknown/only".to_string()],
        ] {
            check_coverage(&names, seed);
            coverage_cases += 1;
        }

        let mut routes = ready_routes();
        shuffle(&mut routes, &mut rng);
        for route in &mut routes {
            route["shadow_compare"]["primary_engine"] =
                json!(["kuzu", "ladybug", "kuzu/ladybug"][rng.below(3)]);
            let query = &mut route["query_reports"][0];
            query["statement_kind"] = json!(
                [
                    "match_return",
                    "match_nodes_return",
                    "shortest_path_return",
                    "match_optional_relationship_count_sum",
                    "match_thread_repair_stats",
                    "graph_algorithm",
                    "project_graph",
                ][rng.below(7)]
            );
            query["execution_path"] = json!(["fast_path", "optimized_path"][rng.below(2)]);
            query["elapsed_micros"] = json!(rng.below(1000));
            if rng.below(2) == 0 {
                let cache = query.as_object_mut().unwrap().remove("plan_cache").unwrap();
                for (name, value) in cache.as_object().unwrap() {
                    query[format!("plan_cache_{name}")] = value.clone();
                }
                query["include_metadata_false_strips_metadata"] = query["api_behavior"]
                    .as_object_mut()
                    .unwrap()
                    .remove("include_metadata_false_strips_metadata")
                    .unwrap();
            }
        }
        let evidence = ready_evidence(routes);
        let report = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(report["route_primary_ready"], true, "seed={seed}");
        assert_eq!(report["route_primary_blocker_codes"], json!([]));
        let roundtrip = serde_json::from_slice(&serde_json::to_vec(&evidence).unwrap()).unwrap();
        assert_eq!(
            report,
            nowledge_graph_route_readiness_json(&roundtrip).unwrap()
        );
        accepted_cases += 1;

        let baseline = ready_evidence(ready_routes());
        for (case, (pointer, bad_value, blocker)) in mutations.iter().enumerate() {
            let mut evidence = baseline.clone();
            let route = seed as usize % REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len();
            let pointer = pointer.replace("/routes/0/", &format!("/routes/{route}/"));
            *evidence
                .pointer_mut(&pointer)
                .expect("mutation must reach an existing field") = bad_value.clone();
            assert_rejected(&evidence, blocker, seed, case);
            contradiction_cases += 1;
        }
        for case in 0..4 {
            let mut evidence = baseline.clone();
            let mut expected = Vec::new();
            // Independent mutations may share an ancestor. Only use top-level
            // envelope contradictions here so none can erase another witness.
            for (index, (pointer, bad_value, blocker)) in mutations.iter().take(5).enumerate() {
                if rng.below(2) == 0 || index == case {
                    *evidence.pointer_mut(pointer).unwrap() = bad_value.clone();
                    expected.push(*blocker);
                }
            }
            for blocker in expected {
                assert_rejected(&evidence, blocker, seed, case);
            }
            contradiction_cases += 1;
        }
    }
    eprintln!(
        "graph-route-readiness-differential-v1 seeds={seeds} coverage_cases={coverage_cases} contradiction_cases={contradiction_cases} accepted_cases={accepted_cases}"
    );
}

fn check_coverage(names: &[String], seed: u64) {
    let evidence = ready_evidence(names.iter().map(|name| ready_route(name)).collect());
    let report = nowledge_graph_route_readiness_json(&evidence).unwrap();
    // Deliberately count against the raw list, independently of the reducer's
    // map/set implementation and the fixture's claimed coverage fields.
    let mut covered = Vec::new();
    let mut missing = Vec::new();
    for &route in REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES {
        if names.iter().any(|name| name == route) {
            covered.push(route);
        } else {
            missing.push(route);
        }
    }
    let mut unknown = names
        .iter()
        .filter(|name| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    unknown.sort();
    unknown.dedup();
    let mut duplicate = names
        .iter()
        .filter(|name| names.iter().filter(|other| *other == *name).count() > 1)
        .cloned()
        .collect::<Vec<_>>();
    duplicate.sort();
    duplicate.dedup();
    let mut blockers = Vec::new();
    if !missing.is_empty() {
        blockers.push("missing_required_routes");
    }
    if !unknown.is_empty() {
        blockers.push("unknown_routes");
    }
    if !duplicate.is_empty() {
        blockers.push("duplicate_routes");
    }
    assert_eq!(report["route_count"], json!(names.len()), "seed={seed}");
    assert_eq!(report["covered_routes"], json!(covered), "seed={seed}");
    assert_eq!(report["covered_route_count"], json!(covered.len()));
    assert_eq!(report["missing_required_routes"], json!(missing));
    assert_eq!(report["required_routes_covered"], json!(missing.is_empty()));
    assert_eq!(report["unknown_routes"], json!(unknown));
    assert_eq!(report["duplicate_routes"], json!(duplicate));
    assert_eq!(report["route_coverage_blocker_codes"], json!(blockers));
    assert_eq!(report["evidence_route_coverage_matches"], true);
    assert_eq!(report["route_primary_ready"], json!(blockers.is_empty()));
    blockers.sort();
    assert_eq!(report["route_primary_blocker_codes"], json!(blockers));
}

fn assert_rejected(evidence: &Value, blocker: &str, seed: u64, case: usize) {
    let report = nowledge_graph_route_readiness_json(evidence).unwrap();
    assert_eq!(
        report["route_primary_ready"], false,
        "seed={seed} case={case} blocker={blocker}"
    );
    let codes = report["route_primary_blocker_codes"].as_array().unwrap();
    assert!(
        codes.contains(&json!(blocker)),
        "seed={seed} case={case} blocker={blocker} actual={codes:?}"
    );
    let strings = codes
        .iter()
        .map(|code| code.as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(strings.windows(2).all(|pair| pair[0] < pair[1]));
}

fn rejected_evidence_mutations() -> Vec<(&'static str, Value, &'static str)> {
    vec![
        (
            "/protocol",
            json!("wrong-protocol"),
            "graph_route_evidence_protocol_mismatch",
        ),
        ("/ready", json!(false), "graph_route_evidence_not_ready"),
        (
            "/route_catalog_digest",
            json!("stale"),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/covered_route_count",
            json!(0),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/route_catalog_version",
            json!("stale"),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/required_route_count",
            Value::Null,
            "route_coverage_evidence_mismatch",
        ),
        (
            "/covered_routes",
            json!([]),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/required_routes_covered",
            json!(false),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/route_coverage_ready",
            json!(false),
            "route_coverage_evidence_mismatch",
        ),
        (
            "/routes/0/primary_ready",
            Value::Null,
            "route_primary_not_ready",
        ),
        ("/routes/0/blocker_codes", json!(["host_gate"]), "host_gate"),
        (
            "/routes/0/shadow_compare_ready",
            json!(false),
            "route_shadow_compare_not_ready",
        ),
        (
            "/routes/0/shadow_compare_evidence_source",
            Value::Null,
            "route_shadow_compare_evidence_missing",
        ),
        (
            "/routes/0/shadow_compare/source",
            json!("untrusted"),
            "route_shadow_compare_detail_source_mismatch",
        ),
        (
            "/routes/0/shadow_compare/ready",
            json!(false),
            "route_shadow_compare_detail_not_ready",
        ),
        (
            "/routes/0/shadow_compare/matched_per_million",
            json!(999_999),
            "route_parity_matched_per_million_not_full",
        ),
        (
            "/routes/0/shadow_compare/primary_engine",
            json!("hawdb"),
            "route_parity_primary_engine_mismatch",
        ),
        (
            "/routes/0/shadow_compare/shadow_engine",
            json!("kuzu"),
            "route_parity_shadow_engine_mismatch",
        ),
        (
            "/routes/0/shadow_compare/blocker_codes",
            json!(["parity_gate"]),
            "parity_gate",
        ),
        (
            "/routes/0/required_query_families",
            json!(["unregistered"]),
            "route_required_query_families_mismatch",
        ),
        (
            "/routes/0/query_reports",
            json!([]),
            "missing_query_runtime_reports",
        ),
        (
            "/routes/0/relationship_property_pruning_required_count",
            Value::Null,
            "route_relationship_property_pruning_evidence_not_ready",
        ),
        (
            "/routes/0/relationship_property_pruning_report_count",
            json!(1),
            "route_relationship_property_pruning_evidence_not_ready",
        ),
        (
            "/routes/0/relationship_property_pruning_evidence_ready",
            json!(false),
            "route_relationship_property_pruning_evidence_not_ready",
        ),
        (
            "/routes/0/query_reports/0/query_name",
            json!(" "),
            "query_report_identity_missing",
        ),
        (
            "/routes/0/query_reports/0/query_index",
            json!(-1),
            "query_report_identity_missing",
        ),
        (
            "/routes/0/query_reports/0/query_family",
            Value::Null,
            "query_report_query_family_missing",
        ),
        (
            "/routes/0/query_reports/0/query_family",
            json!("unknown"),
            "query_report_unknown_query_family",
        ),
        (
            "/routes/0/query_reports/0/protocol",
            json!("wrong"),
            "query_report_protocol_mismatch",
        ),
        (
            "/routes/0/query_reports/0/statement_kind",
            json!("delete"),
            "query_report_not_graph_read",
        ),
        (
            "/routes/0/query_reports/0/execution_path",
            json!("host_scan"),
            "query_report_execution_path_missing",
        ),
        (
            "/routes/0/query_reports/0/fast_path_selected",
            Value::Null,
            "query_report_fast_path_selected_missing",
        ),
        (
            "/routes/0/query_reports/0/slow_log_candidate",
            json!("false"),
            "query_report_slow_log_candidate_missing",
        ),
        (
            "/routes/0/query_reports/0/physical_plan_captured",
            Value::Null,
            "query_report_physical_plan_flag_missing",
        ),
        (
            "/routes/0/query_reports/0/elapsed_micros",
            Value::Null,
            "query_report_elapsed_micros_missing",
        ),
        (
            "/routes/0/query_reports/0/physical_operator_counts",
            json!([]),
            "query_report_physical_operator_counts_missing",
        ),
        (
            "/routes/0/query_reports/0/optimizer_decision_count",
            Value::Null,
            "query_report_optimizer_decision_count_missing",
        ),
        (
            "/routes/0/query_reports/0/optimizer_rule_event_count",
            Value::Null,
            "query_report_optimizer_rule_event_count_missing",
        ),
        (
            "/routes/0/query_reports/0/output_row_shape/row_count",
            Value::Null,
            "query_report_output_row_shape_missing",
        ),
        (
            "/routes/0/query_reports/0/output_row_shape/column_count",
            json!(3),
            "query_report_output_row_shape_missing",
        ),
        (
            "/routes/0/query_reports/0/output_row_shape/columns",
            json!([]),
            "query_report_output_row_shape_missing",
        ),
        (
            "/routes/0/query_reports/0/plan_cache/lookup",
            json!("bypass"),
            "query_report_plan_cache_bypassed",
        ),
        (
            "/routes/0/query_reports/0/plan_cache/cacheable",
            Value::Null,
            "query_report_plan_cache_state_missing",
        ),
        (
            "/routes/0/query_reports/0/plan_cache/hit",
            Value::Null,
            "query_report_plan_cache_state_missing",
        ),
        (
            "/routes/0/query_reports/0/plan_cache/miss",
            Value::Null,
            "query_report_plan_cache_state_missing",
        ),
        (
            "/routes/0/query_reports/0/plan_cache/bypassed",
            json!(true),
            "query_report_plan_cache_bypassed",
        ),
        (
            "/routes/0/query_reports/0/api_behavior/include_metadata_false_strips_metadata",
            json!(false),
            "query_report_api_behavior_metadata_stripping_missing",
        ),
        (
            "/routes/0/query_reports/0/api_behavior/ordering_contract_recorded",
            Value::Null,
            "query_report_api_behavior_ordering_missing",
        ),
        (
            "/routes/0/query_reports/0/api_behavior/pagination_contract_recorded",
            json!(false),
            "query_report_api_behavior_pagination_missing",
        ),
        (
            "/routes/0/query_reports/0/api_behavior/error_class_stable",
            json!(false),
            "query_report_api_behavior_error_class_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_report_count",
            json!(2),
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/target_kind",
            json!("unknown"),
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/strategy",
            json!({}),
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/pruned",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/exact_empty",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/candidate_count_before_pruning",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/pruned_candidate_count",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/candidate_count_before_filter",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/output_count",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
        (
            "/routes/0/query_reports/0/scan_pruning_reports/0/filtered_out_count",
            Value::Null,
            "query_report_scan_pruning_profile_missing",
        ),
    ]
}

#[test]
fn malformed_route_evidence_returns_stable_errors() {
    for evidence in [
        Value::Null,
        json!(true),
        json!(7),
        json!("routes"),
        json!({}),
        json!({"routes": {}}),
    ] {
        assert!(
            matches!(nowledge_graph_route_readiness_json(&evidence), Err(HawDBError::Semantic(message))
            if message == "graph route evidence JSON must contain a routes array")
        );
    }
    for route in [
        Value::Null,
        json!({}),
        json!({"route": false}),
        json!({"route": " \t"}),
    ] {
        assert!(
            matches!(nowledge_graph_route_readiness_json(&json!([route])), Err(HawDBError::Semantic(message))
            if message == "graph route evidence route is required")
        );
    }
}

struct Generator(u64);

impl Generator {
    fn below(&mut self, limit: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % limit as u64) as usize
    }
}

fn shuffle<T>(items: &mut [T], rng: &mut Generator) {
    for index in (1..items.len()).rev() {
        items.swap(index, rng.below(index + 1));
    }
}
