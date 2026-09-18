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

use super::test_support::*;
use super::*;
use crate::{Database, QueryOutput, Result, Value};
use std::fs;
use std::time::Duration;

#[test]
fn runs_nowledge_shaped_fixture() {
    let mut db = Database::new();
    let fixture = nowledge_memory_core_fixture();

    let report = run_compatibility_fixture(&mut db, &fixture).unwrap();

    assert_eq!(report.fixture, "nowledge-memory-core");
    assert_eq!(report.checks.len(), 644);
}

#[test]
fn public_nowledge_core_fixture_and_inventory_are_gate_ready() {
    let fixture = nowledge_memory_core_fixture();
    let inventory = nowledge_memory_core_inventory();
    let mut primary = Database::new();
    let mut shadow = DatabaseShadowEngine::default();

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
    let bundle = assess_compatibility_migration_gate_bundle(
        &fixture,
        &inventory,
        &report,
        CompatibilityInventoryCoveragePolicy::default(),
        CompatibilityCutoverPolicy::default(),
    );
    let bundle_json = super::compatibility_migration_gate_bundle_to_json(&bundle);

    assert_eq!(fixture.name, "nowledge-memory-core");
    assert_eq!(inventory.required_checks.len(), fixture.checks.len());
    assert_eq!(
        bundle.migration_gate.decision,
        CompatibilityCutoverDecision::Ready
    );
    assert!(bundle.migration_gate.blockers.is_empty());
    assert_eq!(bundle_json["coverage"]["covered_checks"], 644);
    assert_eq!(bundle_json["coverage"]["coverage_per_million"], 1_000_000);
    assert!(bundle_json["coverage"]["coverage_by_query_family"]
        .as_array()
        .unwrap()
        .iter()
        .all(|family| family["coverage_per_million"] == 1_000_000));
    assert_eq!(bundle_json["inventory_gate"]["decision"], "ready");
    assert_eq!(
        bundle_json["inventory_gate"]["coverage_per_million"],
        1_000_000
    );
    assert_eq!(
        bundle_json["inventory_gate"]["coverage_by_query_family"],
        bundle_json["coverage"]["coverage_by_query_family"]
    );
    assert_eq!(bundle_json["cutover"]["decision"], "ready");
    assert_eq!(bundle_json["cutover"]["matched_checks"], 644);
    assert_eq!(bundle_json["cutover"]["matched_per_million"], 1_000_000);
    assert_eq!(bundle_json["migration_gate"]["decision"], "ready");
    assert_eq!(bundle_json["migration_gate"]["inventory_decision"], "ready");
    assert_eq!(bundle_json["migration_gate"]["shadow_decision"], "ready");
    assert_eq!(bundle_json["migration_gate"]["shadow_total_checks"], 644);
    assert_eq!(bundle_json["migration_gate"]["shadow_matched_checks"], 644);
    assert_eq!(
        bundle_json["migration_gate"]["shadow_matched_per_million"],
        1_000_000
    );
    assert_eq!(
        bundle_json["migration_gate"]["shadow_primary_only_checks"],
        0
    );
    assert_eq!(
        bundle_json["migration_gate"]["shadow_evidence_present"],
        true
    );
    assert_eq!(bundle_json["replacement_readiness_per_million"], 1_000_000);
}

#[test]
fn runs_nowledge_shaped_fixture_against_shadow_engine() {
    let mut primary = Database::new();
    let mut shadow = DatabaseShadowEngine::default();
    let fixture = nowledge_memory_core_fixture();

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(report.fixture, "nowledge-memory-core");
    assert_eq!(report.shadow_engine, "hawdb-shadow");
    assert_eq!(report.primary_checks.len(), 644);
    assert_eq!(report.shadow_checks.len(), 644);
    assert_eq!(
        report
            .shadow_checks
            .iter()
            .filter(|check| check.status == CompatibilityShadowStatus::Matched)
            .count(),
        644
    );
    assert_eq!(
        report.shadow_checks.last().map(|check| check.status),
        Some(CompatibilityShadowStatus::Matched)
    );

    let cutover = assess_compatibility_cutover(&report, CompatibilityCutoverPolicy::default());
    assert_eq!(cutover.decision, CompatibilityCutoverDecision::Ready);
    assert_eq!(cutover.matched_checks, 644);
    assert!(cutover.primary_only_checks.is_empty());
    assert!(cutover.blockers.is_empty());

    let inventory_gate = assess_query_inventory_gate(
        &assess_query_inventory_coverage(&fixture, &nowledge_memory_core_inventory()),
        CompatibilityInventoryCoveragePolicy::default(),
    );
    let migration_gate = assess_compatibility_migration_gate(&inventory_gate, &cutover);
    assert_eq!(migration_gate.decision, CompatibilityCutoverDecision::Ready);
    assert_eq!(
        migration_gate.inventory_decision,
        CompatibilityCutoverDecision::Ready
    );
    assert_eq!(
        migration_gate.shadow_decision,
        CompatibilityCutoverDecision::Ready
    );
    assert!(migration_gate.blockers.is_empty());
}

#[test]
fn runs_fixture_against_external_shadow_command() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *project_graph*) echo '{"primary_only":true}' ;;
    *MATCH*) echo '{"ok":{"rows":[{"title":"Graph foundations"}]}}' ;;
    *) echo '{"ok":{"rows":[]}}' ;;
  esac
done
"#,
    );
    let mut shadow = ExternalShadowCommand::spawn("external-shadow", "sh", [script]).unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-fixture".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Graph foundations".to_string()),
            )])]),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(report.shadow_engine, "external-shadow");
    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn reports_external_shadow_project_graph_primary_only_reason() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-projection-primary-only-reason",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *project_graph*) echo '{"primary_only":true,"reason":"projection metadata is not exposed"}' ;;
    *) echo '{"ok":{"rows":[]}}' ;;
  esac
done
"#,
    );
    let mut shadow = ExternalShadowCommand::spawn(
        "external-shadow-projection-primary-only-reason",
        "sh",
        [script],
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-projection-primary-only-reason-fixture".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
        )],
        checks: vec![CompatibilityCheck::ProjectedGraph(
            ProjectedGraphFixtureCheck {
                name: "mentions projection".to_string(),
                rel_type: Some("MENTIONS".to_string()),
                expected_node_count: 2,
                expected_edge_count: 1,
                expected_incoming: vec![(1, vec![0])],
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: CompatibilityTolerance::default(),
            },
        )],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
    let cutover = assess_compatibility_cutover(&report, CompatibilityCutoverPolicy::default());
    let cutover_json = super::compatibility_cutover_report_to_json(&cutover);

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::PrimaryOnly
    );
    assert_eq!(
        report.shadow_checks[0].primary_only_reason.as_deref(),
        Some("projection metadata is not exposed")
    );
    assert_eq!(
        cutover
            .primary_only_reasons
            .get("mentions projection")
            .map(String::as_str),
        Some("projection metadata is not exposed")
    );
    assert!(cutover
        .blockers
        .iter()
        .any(|blocker| blocker.contains("projection metadata is not exposed")));
    assert_eq!(
        cutover_json["primary_only_reasons"]["mentions projection"],
        "projection metadata is not exposed"
    );
}

#[test]
fn sends_external_shadow_protocol_version() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-protocol-version",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"protocol_version":1'*) echo '{"ok":{"rows":[{"title":"Graph foundations"}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"missing protocol version"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-protocol-version", "sh", [script]).unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-protocol-version-fixture".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Graph foundations".to_string()),
            )])]),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn sends_external_shadow_request_id() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-request-id",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"request_id":1'*) echo '{"ok":{"rows":[]}}' ;;
    *) echo '{"error":{"class":"execution","message":"missing request id"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-request-id", "sh", [script]).unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-request-id-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn accepts_external_shadow_response_request_id_echo() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-response-request-id-echo",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"request_id":1'*) echo '{"request_id":1,"ok":{"rows":[]}}' ;;
    *) echo '{"error":{"class":"execution","message":"missing request id"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-response-request-id-echo", "sh", [script])
            .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-response-request-id-echo-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn rejects_external_shadow_response_request_id_mismatch() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-response-request-id-mismatch",
        r#"#!/bin/sh
while IFS= read -r line; do
  echo '{"request_id":99,"ok":{"rows":[]}}'
done
"#,
    );
    let mut shadow = ExternalShadowCommand::spawn(
        "external-shadow-response-request-id-mismatch",
        "sh",
        [script],
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-response-request-id-mismatch-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("response request_id 99 did not match request_id 1"));
}

#[test]
fn runs_session_fixture_against_external_shadow_command() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-session",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *execute_session*) echo '{"ok":{"outputs":[{"rows":[]},{"rows":[{}]},{"rows":[{"title":"New"}]}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected execute_session"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-session", "sh", [script]).unwrap();
    let fixture = session_effect_fixture("external-shadow-session-fixture");

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(report.shadow_engine, "external-shadow-session");
    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn rejects_external_shadow_session_output_count_mismatch() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-session-count-mismatch",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *execute_session*) echo '{"ok":{"outputs":[{"rows":[]},{"rows":[{"title":"New"}]}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected execute_session"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-session-count-mismatch", "sh", [script])
            .unwrap();
    let fixture = session_effect_fixture("external-shadow-session-count-mismatch-fixture");

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("session returned 2 outputs for 3 statements"));
}

#[test]
fn rejects_external_shadow_session_extra_outputs() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-session-extra-outputs",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *execute_session*) echo '{"ok":{"outputs":[{"rows":[]},{"rows":[{}]},{"rows":[{"title":"New"}]},{"rows":[]}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected execute_session"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-session-extra-outputs", "sh", [script])
            .unwrap();
    let fixture = session_effect_fixture("external-shadow-session-extra-outputs-fixture");

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("session returned 4 outputs for 3 statements"));
}

#[test]
fn reports_external_shadow_session_output_index_on_decode_error() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-session-output-index",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *execute_session*) echo '{"ok":{"outputs":[{"rows":[]},{"oops":[]},{"rows":[{"title":"New"}]}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected execute_session"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-session-output-index", "sh", [script])
            .unwrap();
    let fixture = session_effect_fixture("external-shadow-session-output-index-fixture");

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error.to_string().contains("session output 1 missing rows"));
}

#[test]
fn sends_external_shadow_session_access() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-session-access",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"op":"execute_session"'*'"access":"mutation"'*'"statement_index":0'*'"statement_index":1'*'"statement_index":2'*) echo '{"ok":{"outputs":[{"rows":[]},{"rows":[{}]},{"rows":[{"title":"New"}]}]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected mutation session access"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-session-access", "sh", [script]).unwrap();
    let fixture = session_effect_fixture("external-shadow-session-access-fixture");

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn includes_external_shadow_stderr_tail_on_stdout_close() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-stderr-tail",
        r#"#!/bin/sh
while IFS= read -r line; do
  echo "wrapper boot failed" >&2
  exit 7
done
"#,
    );
    let trace_path = unique_test_path("external-shadow-error-trace.jsonl");
    let mut shadow = ExternalShadowCommand::spawn_with_trace_path(
        "external-shadow-stderr-tail",
        "sh",
        [script],
        &trace_path,
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-stderr-tail-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("shadow engine 'external-shadow-stderr-tail' closed stdout"));
    assert!(error.to_string().contains("child status: exit status: 7"));
    assert!(error
        .to_string()
        .contains("stderr tail: wrapper boot failed"));
    drop(shadow);
    let trace = fs::read_to_string(&trace_path).unwrap();
    assert!(trace.contains("\"event\":\"request\""));
    assert!(trace.contains("\"event\":\"error\""));
    assert!(trace.contains("child status: exit status: 7"));
    assert!(trace.contains("wrapper boot failed"));
    let _ = fs::remove_file(trace_path);
}

#[test]
fn includes_external_shadow_stdout_tail_on_invalid_json() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-stdout-tail",
        r#"#!/bin/sh
while IFS= read -r line; do
  echo "wrapper log accidentally written to stdout"
done
"#,
    );
    let trace_path = unique_test_path("external-shadow-stdout-error-trace.jsonl");
    let mut shadow = ExternalShadowCommand::spawn_with_trace_path(
        "external-shadow-stdout-tail",
        "sh",
        [script],
        &trace_path,
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-stdout-tail-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("shadow engine 'external-shadow-stdout-tail' returned invalid JSON"));
    assert!(error
        .to_string()
        .contains("stdout line tail: wrapper log accidentally written to stdout"));
    drop(shadow);
    let trace = fs::read_to_string(&trace_path).unwrap();
    assert!(trace.contains("\"event\":\"error\""));
    assert!(trace.contains("stdout line tail"));
    let _ = fs::remove_file(trace_path);
}

#[test]
fn times_out_external_shadow_without_response() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-timeout",
        r#"#!/bin/sh
while IFS= read -r line; do
  :
done
"#,
    );
    let trace_path = unique_test_path("external-shadow-timeout-trace.jsonl");
    let mut shadow = ExternalShadowCommand::spawn_with_trace_path_and_request_timeout(
        "external-shadow-timeout",
        "sh",
        [script],
        &trace_path,
        Duration::from_millis(20),
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-timeout-fixture".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::RowCount(0),
        ))],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error
        .to_string()
        .contains("shadow engine 'external-shadow-timeout' did not return a response within"));
    assert!(error.to_string().contains("child status:"));
    drop(shadow);
    let trace = fs::read_to_string(&trace_path).unwrap();
    assert!(trace.contains("\"event\":\"error\""));
    assert!(trace.contains("did not return a response within"));
    assert!(trace.contains("child status:"));
    let _ = fs::remove_file(trace_path);
}

#[test]
fn writes_external_shadow_trace_jsonl() {
    let mut primary = Database::new();
    let script = write_external_shadow_script(
        "external-shadow-trace",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *MATCH*) echo '{"ok":{"rows":[{"title":"Graph foundations"}]}}' ;;
    *) echo '{"ok":{"rows":[]}}' ;;
  esac
done
"#,
    );
    let trace_path = unique_test_path("external-shadow-trace.jsonl");
    let mut shadow = ExternalShadowCommand::spawn_with_trace_path(
        "external-shadow-trace",
        "sh",
        [script],
        &trace_path,
    )
    .unwrap();
    let fixture = CompatibilityFixture {
        name: "external-shadow-trace-fixture".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "read title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Graph foundations".to_string()),
            )])]),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
    drop(shadow);

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
    let trace = fs::read_to_string(&trace_path).unwrap();
    assert!(trace.contains("\"event\":\"request\""));
    assert!(trace.contains("\"event\":\"response\""));
    assert!(trace.contains("\"protocol_version\":1"));
    assert!(trace.contains("\"request_id\":1"));
    assert!(trace.contains("\"fixture\":\"external-shadow-trace-fixture\""));
    assert!(trace.contains("\"check\":\"read title\""));
    assert!(trace.contains("\"phase\":\"fixture_setup\""));
    assert!(trace.contains("\"phase\":\"statement\""));
    assert!(trace.contains("\"access\":\"mutation\""));
    assert!(trace.contains("\"access\":\"read\""));
    let _ = fs::remove_file(trace_path);
}

#[test]
fn reports_row_mismatch_with_fixture_context() {
    let mut db = Database::new();
    let fixture = CompatibilityFixture {
        name: "mismatch".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "wrong title",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Runtime strategy".to_string()),
            )])]),
        ))],
    };

    let error = run_compatibility_fixture(&mut db, &fixture).unwrap_err();

    assert!(error.to_string().contains("fixture 'mismatch'"));
    assert!(error.to_string().contains("wrong title"));
    assert!(error.to_string().contains("row mismatch"));
}

#[test]
fn reports_shadow_row_mismatch_with_engine_context() {
    let mut primary = Database::new();
    let mut shadow = MismatchingShadowEngine;
    let fixture = CompatibilityFixture {
        name: "shadow-mismatch".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "title lookup",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Graph foundations".to_string()),
            )])]),
        ))],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error.to_string().contains("shadow-mismatch"));
    assert!(error.to_string().contains("title lookup"));
    assert!(error
        .to_string()
        .contains("shadow engine 'mismatching-shadow'"));
    assert!(error.to_string().contains("row mismatch"));
}

#[test]
fn compares_shadow_error_classes() {
    let mut primary = Database::new();
    let mut shadow = DatabaseShadowEngine::default();
    let fixture = CompatibilityFixture {
        name: "error-class".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(
            CypherFixtureCheck::expect_error(
                "missing parameter",
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.id = $missing RETURN m.title AS title",
                ),
                super::ExpectedErrorClass::Semantic,
            ),
        )],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn compares_shadow_mutation_effects() {
    let mut primary = Database::new();
    let mut shadow = DatabaseShadowEngine::default();
    let fixture = CompatibilityFixture {
        name: "mutation-effect".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Old'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(
            CypherFixtureCheck::expect_rows(
                "set title",
                CypherFixtureStatement::new("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'"),
                ExpectedRows::RowCount(1),
            )
            .with_effect_query(
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title",
                ),
                ExpectedRows::Exact(vec![row([("title", Value::String("New".to_string()))])]),
            ),
        )],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn compares_shadow_float_rows_with_tolerance() {
    let mut primary = Database::new();
    let mut shadow = SlightlyDifferentFloatShadowEngine;
    let fixture = CompatibilityFixture {
        name: "float-tolerance".to_string(),
        setup: vec![
            CypherFixtureStatement::new("CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})"),
            CypherFixtureStatement::new(
                "CALL project_graph('FloatGraph', ['Memory', 'Entity'], ['MENTIONS'])",
            ),
        ],
        checks: vec![CompatibilityCheck::Cypher(
            CypherFixtureCheck::expect_rows(
                "pagerank score",
                CypherFixtureStatement::new(
                    "CALL page_rank('FloatGraph') RETURN node, pagerank_score",
                ),
                ExpectedRows::RowCount(2),
            )
            .with_tolerance(CompatibilityTolerance { float_abs: 1.0e-6 }),
        )],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::Matched
    );
}

#[test]
fn reports_shadow_projected_graph_mismatch_with_engine_context() {
    let mut primary = Database::new();
    let mut shadow = MismatchingProjectedGraphShadowEngine::default();
    let fixture = CompatibilityFixture {
        name: "projected-graph-mismatch".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
        )],
        checks: vec![CompatibilityCheck::ProjectedGraph(
            ProjectedGraphFixtureCheck {
                name: "mentions projection".to_string(),
                rel_type: Some("MENTIONS".to_string()),
                expected_node_count: 2,
                expected_edge_count: 1,
                expected_incoming: vec![(1, vec![0])],
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: CompatibilityTolerance::default(),
            },
        )],
    };

    let error =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap_err();

    assert!(error.to_string().contains("projected-graph-mismatch"));
    assert!(error.to_string().contains("mentions projection"));
    assert!(error
        .to_string()
        .contains("shadow engine 'bad-projection-shadow'"));
    assert!(error
        .to_string()
        .contains("projected graph validation failed"));
}

#[test]
fn keeps_projected_graph_primary_only_when_shadow_has_no_projection_hook() {
    let mut primary = Database::new();
    let mut shadow = NoProjectionShadowEngine::default();
    let fixture = CompatibilityFixture {
        name: "primary-only-projection".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 2})",
        )],
        checks: vec![CompatibilityCheck::ProjectedGraph(
            ProjectedGraphFixtureCheck {
                name: "mentions projection".to_string(),
                rel_type: Some("MENTIONS".to_string()),
                expected_node_count: 2,
                expected_edge_count: 1,
                expected_incoming: vec![(1, vec![0])],
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: CompatibilityTolerance::default(),
            },
        )],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();

    assert_eq!(
        report.shadow_checks[0].status,
        CompatibilityShadowStatus::PrimaryOnly
    );

    let cutover = assess_compatibility_cutover(&report, CompatibilityCutoverPolicy::default());
    assert_eq!(cutover.decision, CompatibilityCutoverDecision::Blocked);
    assert_eq!(
        cutover.primary_only_checks,
        vec!["mentions projection".to_string()]
    );
    assert!(cutover
        .blockers
        .iter()
        .any(|blocker| blocker.contains("did not cover checks")));

    let relaxed = assess_compatibility_cutover(
        &report,
        CompatibilityCutoverPolicy {
            require_shadow_for_all_checks: false,
            min_matched_checks: 0,
        },
    );
    assert_eq!(relaxed.decision, CompatibilityCutoverDecision::Ready);
}

#[test]
fn cutover_gate_blocks_when_minimum_match_count_is_not_met() {
    let mut primary = Database::new();
    let mut shadow = DatabaseShadowEngine::default();
    let fixture = CompatibilityFixture {
        name: "single-check".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, title: 'Graph foundations'})",
        )],
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "title lookup",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.title AS title"),
            ExpectedRows::Exact(vec![row([(
                "title",
                Value::String("Graph foundations".to_string()),
            )])]),
        ))],
    };

    let report =
        run_compatibility_fixture_with_shadow(&mut primary, &fixture, &mut shadow).unwrap();
    let cutover = assess_compatibility_cutover(
        &report,
        CompatibilityCutoverPolicy {
            require_shadow_for_all_checks: true,
            min_matched_checks: 2,
        },
    );

    assert_eq!(cutover.decision, CompatibilityCutoverDecision::Blocked);
    assert!(cutover.blockers[0].contains("below required minimum"));
}
fn session_effect_fixture(name: &str) -> CompatibilityFixture {
    CompatibilityFixture {
        name: name.to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(
            CypherFixtureCheck::expect_rows(
                "session set title",
                CypherFixtureStatement::new("MATCH (m:Memory) WHERE m.id = 1 SET m.title = 'New'"),
                ExpectedRows::RowCount(1),
            )
            .with_setup_query(CypherFixtureStatement::new(
                "CREATE (:Memory {id: 1, title: 'Old'})",
            ))
            .with_effect_query(
                CypherFixtureStatement::new(
                    "MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title",
                ),
                ExpectedRows::Exact(vec![row([("title", Value::String("New".to_string()))])]),
            )
            .with_session_execution(),
        )],
    }
}

#[derive(Debug, Default)]
struct DatabaseShadowEngine {
    db: Database,
}

impl CompatibilityShadowEngine for DatabaseShadowEngine {
    fn name(&self) -> &str {
        "hawdb-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }

    fn execute_session(
        &mut self,
        statements: &[CypherFixtureStatement],
    ) -> Result<Vec<QueryOutput>> {
        let mut session = self.db.session();
        statements
            .iter()
            .map(|statement| session.query_with_params(&statement.cypher, &statement.parameters))
            .collect()
    }

    fn project_graph(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        let graph = self.db.project_graph(check.rel_type.as_deref());
        Ok(Some(super::projected_graph_shadow_output(&graph, check)))
    }
}

#[derive(Debug)]
struct SlightlyDifferentFloatShadowEngine;

impl CompatibilityShadowEngine for SlightlyDifferentFloatShadowEngine {
    fn name(&self) -> &str {
        "float-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        if !statement.cypher.contains("page_rank") {
            return Ok(QueryOutput {
                rows: Vec::new().into(),
            });
        }
        Ok(QueryOutput {
            rows: vec![
                row([
                    ("node", Value::Int(1)),
                    ("pagerank_score", Value::Float(0.6491233)),
                ]),
                row([
                    ("node", Value::Int(0)),
                    ("pagerank_score", Value::Float(0.3508773)),
                ]),
            ]
            .into(),
        })
    }
}

#[derive(Debug)]
struct MismatchingShadowEngine;

impl CompatibilityShadowEngine for MismatchingShadowEngine {
    fn name(&self) -> &str {
        "mismatching-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        if statement.cypher.starts_with("MATCH") {
            Ok(QueryOutput {
                rows: vec![row([(
                    "title",
                    Value::String("Runtime strategy".to_string()),
                )])]
                .into(),
            })
        } else {
            Ok(QueryOutput {
                rows: Vec::new().into(),
            })
        }
    }
}

#[derive(Debug, Default)]
struct MismatchingProjectedGraphShadowEngine {
    db: Database,
}

impl CompatibilityShadowEngine for MismatchingProjectedGraphShadowEngine {
    fn name(&self) -> &str {
        "bad-projection-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }

    fn project_graph(
        &mut self,
        _check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        Ok(Some(ProjectedGraphShadowOutput {
            node_count: 2,
            edge_count: 0,
            incoming: vec![(1, Vec::new())],
            communities: Vec::new(),
            hierarchical_communities: Vec::new(),
            page_rank_scores: Vec::new(),
            page_rank_top_node: None,
        }))
    }
}

#[derive(Debug, Default)]
struct NoProjectionShadowEngine {
    db: Database,
}

impl CompatibilityShadowEngine for NoProjectionShadowEngine {
    fn name(&self) -> &str {
        "no-projection-shadow"
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }
}
