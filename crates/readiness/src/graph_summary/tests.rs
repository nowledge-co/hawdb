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

use super::{
    nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
};
use serde_json::{json, Value};

fn graph() -> Value {
    let bundle: Value =
        serde_json::from_str(include_str!("../integration_bundle/ready.json")).unwrap();
    bundle["graph_route_readiness"].clone()
}

#[test]
fn top_level_presence_precedes_nested_fallback_even_when_invalid() {
    let valid = graph();
    let expected = nowledge_graph_route_readiness_summary(&valid);
    assert!(expected.ready);
    for top in [
        Value::Null,
        json!(false),
        json!(0),
        json!("wrong"),
        json!([]),
        json!({}),
    ] {
        let bundle = json!({"graph_route_readiness": top, "cutover_evidence": {"graph_route_readiness": valid}});
        let actual = nowledge_graph_route_readiness_summary_from_bundle(&bundle);
        assert_eq!(actual, nowledge_graph_route_readiness_summary(&top));
        assert!(!actual.ready);
    }
    assert_eq!(
        nowledge_graph_route_readiness_summary_from_bundle(
            &json!({"cutover_evidence": {"graph_route_readiness": valid}})
        ),
        expected
    );
}

#[test]
fn summary_retains_ordered_blockers_and_filters_only_nonstring_coverage_entries() {
    let mut value = graph();
    value["route_primary_blocker_codes"] = json!(["second", "first", "second", 42, null]);
    let routes = value["covered_routes"].as_array_mut().unwrap();
    routes.push(json!("unknown"));
    routes.push(json!("unknown"));
    routes.push(json!(42));
    let summary = nowledge_graph_route_readiness_summary(&value);
    assert!(!summary.ready);
    assert_eq!(summary.unknown_routes, vec!["unknown", "unknown"]);
    assert_eq!(summary.duplicate_routes, vec!["unknown"]);
    assert_eq!(
        summary.blocker_codes,
        json!(["second", "first", "second", 42, null])
    );
    assert_eq!(
        summary.covered_routes.len(),
        value["covered_routes"].as_array().unwrap().len() - 1
    );
}

#[test]
fn catalog_metadata_duplicate_entries_keep_last_observation() {
    let mut value = graph();
    let original = value["routes"][0].clone();
    let mut stale = original.clone();
    stale["owner"] = json!("stale");
    value["routes"].as_array_mut().unwrap().push(stale);
    let summary = nowledge_graph_route_readiness_summary(&value);
    assert!(!summary.route_catalog_metadata_ready);
    assert_eq!(
        summary.route_catalog_metadata_mismatch_routes,
        vec![original["route"].as_str().unwrap()]
    );
    value["routes"].as_array_mut().unwrap().push(original);
    assert!(nowledge_graph_route_readiness_summary(&value).route_catalog_metadata_ready);
}
