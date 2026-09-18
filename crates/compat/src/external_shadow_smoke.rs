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

use crate::{
    external_shadow_ready_missing_capabilities, external_shadow_trace_report_json,
    CompatibilityCheck, CompatibilityFixture, CompatibilityShadowReport, CompatibilityShadowStatus,
    CypherFixtureCheck, CypherFixtureStatement, ExpectedRows, ExternalShadowReady,
    ProjectedGraphFixtureCheck,
};
#[cfg(test)]
use crate::{CompatibilityCheckReport, CompatibilityShadowCheckReport};
use hawdb_core::{HawDBError, Result, Value};
use std::collections::BTreeMap;

#[doc(hidden)]
pub fn external_shadow_adapter_smoke_fixture() -> CompatibilityFixture {
    CompatibilityFixture {
        name: "external-shadow-adapter-smoke".to_string(),
        setup: vec![CypherFixtureStatement::new(
            "CREATE (:Memory {id: 1, stable_id: 'smoke-memory', title: 'Adapter Smoke'})",
        )],
        checks: vec![
            CompatibilityCheck::Cypher(
                CypherFixtureCheck::expect_rows(
                    "session query returns seeded memory",
                    CypherFixtureStatement::with_parameters(
                        "MATCH (m:Memory) WHERE m.stable_id = $stable_id RETURN m.title AS title",
                        BTreeMap::from([(
                            "stable_id".to_string(),
                            Value::String("smoke-memory".to_string()),
                        )]),
                    ),
                    ExpectedRows::Exact(vec![BTreeMap::from([(
                        "title".to_string(),
                        Value::String("Adapter Smoke".to_string()),
                    )])]),
                )
                .with_session_execution(),
            ),
            CompatibilityCheck::ProjectedGraph(ProjectedGraphFixtureCheck {
                name: "single memory projection".to_string(),
                rel_type: None,
                expected_node_count: 1,
                expected_edge_count: 0,
                expected_incoming: Vec::new(),
                expected_communities: Vec::new(),
                expected_hierarchical_communities: Vec::new(),
                expected_page_rank_scores: Vec::new(),
                page_rank_top_node: None,
                tolerance: Default::default(),
            }),
        ],
    }
}

#[doc(hidden)]
pub fn external_shadow_adapter_smoke_report_json(
    ready: &ExternalShadowReady,
    report: &CompatibilityShadowReport,
    request_count: u64,
    trace_path: Option<&str>,
) -> serde_json::Value {
    let missing_capabilities = external_shadow_ready_missing_capabilities(Some(ready));
    let matched_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::Matched)
        .count();
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .count();
    let primary_only_reasons = report
        .shadow_checks
        .iter()
        .filter_map(|check| {
            check
                .primary_only_reason
                .as_ref()
                .map(|reason| (check.name.clone(), reason.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let primary_check_count = report.primary_checks.len();
    let shadow_check_count = report.shadow_checks.len();
    let dual_engine_ready = primary_check_count == shadow_check_count
        && shadow_check_count > 0
        && matched_checks == shadow_check_count
        && primary_only_checks == 0;
    let mut json = serde_json::json!({
        "protocol": "hawdb-external-shadow-adapter-smoke",
        "ready": {
            "protocol_version": ready.protocol_version,
            "engine_kind": ready.engine_kind,
            "wrapper_identity": ready.wrapper_identity,
            "capabilities": ready.capabilities,
            "missing_capabilities": missing_capabilities,
        },
        "fixture": report.fixture,
        "shadow_engine": report.shadow_engine,
        "total_checks": report.shadow_checks.len(),
        "matched_checks": matched_checks,
        "primary_only_checks": primary_only_checks,
        "primary_only_reasons": primary_only_reasons,
        "dual_engine_evidence": {
            "ready": dual_engine_ready,
            "primary_engine": "hawdb",
            "shadow_engine": report.shadow_engine,
            "primary_check_count": primary_check_count,
            "shadow_check_count": shadow_check_count,
            "matched_check_count": matched_checks,
            "primary_only_check_count": primary_only_checks,
        },
        "request_count": request_count,
        "operation_expectations": {
            "ready": true,
            "execute_session": true,
            "project_graph": true,
        },
        "adapter_smoke_ready": missing_capabilities.is_empty() && dual_engine_ready,
    });
    if let Some(trace_path) = trace_path
        && let Some(object) = json.as_object_mut()
    {
        object.insert(
            "shadow_trace".to_string(),
            external_shadow_trace_report_json(trace_path, request_count),
        );
    }
    json
}

#[doc(hidden)]
pub fn enforce_external_shadow_adapter_smoke_requirements(
    ready: &ExternalShadowReady,
    report: &CompatibilityShadowReport,
    require_previous_wrapper: bool,
) -> Result<()> {
    let missing_capabilities = external_shadow_ready_missing_capabilities(Some(ready));
    if !missing_capabilities.is_empty() {
        return Err(HawDBError::Execution(format!(
            "external shadow adapter smoke missing required capabilities: {}",
            missing_capabilities.join(", ")
        )));
    }
    if require_previous_wrapper && ready.engine_kind.as_deref() != Some("previous_wrapper") {
        return Err(HawDBError::Execution(
            "external shadow adapter smoke requires engine_kind 'previous_wrapper'".to_string(),
        ));
    }
    if !report
        .shadow_checks
        .iter()
        .any(|check| check.status == CompatibilityShadowStatus::Matched)
    {
        return Err(HawDBError::Execution(
            "external shadow adapter smoke did not match any shadow checks".to_string(),
        ));
    }
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .map(|check| check.name.as_str())
        .collect::<Vec<_>>();
    if require_previous_wrapper && !primary_only_checks.is_empty() {
        return Err(HawDBError::Execution(format!(
            "external shadow adapter smoke requires all checks to run on previous-wrapper; primary-only checks: {}",
            primary_only_checks.join(", ")
        )));
    }
    if !report
        .shadow_checks
        .iter()
        .any(|check| check.name == "single memory projection")
    {
        return Err(HawDBError::Execution(
            "external shadow adapter smoke did not exercise project_graph".to_string(),
        ));
    }
    Ok(())
}

#[doc(hidden)]
pub fn should_run_shadow_ready(
    require_ready: bool,
    require_cutover_evidence: bool,
    shadow_ready: bool,
) -> bool {
    require_ready || require_cutover_evidence || shadow_ready
}

#[doc(hidden)]
pub fn is_self_shadow_command(shadow_name: &str, program: &str, program_args: &[String]) -> bool {
    shadow_name == "self"
        || program.ends_with("hawdb-shadow-self")
        || program_args.iter().any(|arg| arg == "hawdb-shadow-self")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CypherExecutionMode;

    #[test]
    fn detects_self_shadow_commands() {
        assert!(is_self_shadow_command(
            "oracle",
            "target/debug/hawdb-shadow-self",
            &[]
        ));
        assert!(is_self_shadow_command(
            "oracle",
            "cargo",
            &[
                "run".to_string(),
                "--quiet".to_string(),
                "--bin".to_string(),
                "hawdb-shadow-self".to_string(),
                "--".to_string(),
            ],
        ));
        assert!(is_self_shadow_command(
            "self",
            "/usr/bin/legacy-wrapper",
            &[]
        ));
        assert!(!is_self_shadow_command(
            "legacy-wrapper",
            "/usr/bin/nmem-graph-shadow",
            &[]
        ));
    }

    #[test]
    fn shadow_ready_preflight_is_enabled_by_each_required_gate() {
        assert!(should_run_shadow_ready(true, false, false));
        assert!(should_run_shadow_ready(false, true, false));
        assert!(should_run_shadow_ready(false, false, true));
        assert!(!should_run_shadow_ready(false, false, false));
    }

    #[test]
    fn adapter_smoke_fixture_exercises_session_and_project_graph() {
        let fixture = external_shadow_adapter_smoke_fixture();

        assert_eq!(fixture.name, "external-shadow-adapter-smoke");
        assert_eq!(fixture.setup.len(), 1);
        assert!(fixture.checks.iter().any(|check| matches!(
            check,
            CompatibilityCheck::Cypher(cypher)
                if cypher.name == "session query returns seeded memory"
                    && cypher.execution_mode == CypherExecutionMode::Session
        )));
        assert!(fixture.checks.iter().any(|check| matches!(
            check,
            CompatibilityCheck::ProjectedGraph(projected)
                if projected.name == "single memory projection"
                    && projected.expected_node_count == 1
                    && projected.expected_edge_count == 0
        )));
    }

    #[test]
    fn adapter_smoke_requires_previous_wrapper_when_requested() {
        let ready = ready("protocol_smoke", None);
        let report = report(vec![
            shadow_check(
                "session query returns seeded memory",
                CompatibilityShadowStatus::Matched,
                None,
            ),
            shadow_check(
                "single memory projection",
                CompatibilityShadowStatus::PrimaryOnly,
                Some("projection metadata is not exposed"),
            ),
        ]);

        let error =
            enforce_external_shadow_adapter_smoke_requirements(&ready, &report, true).unwrap_err();

        assert!(error
            .to_string()
            .contains("requires engine_kind 'previous_wrapper'"));
    }

    #[test]
    fn adapter_smoke_blocks_primary_only_projection_for_previous_wrapper() {
        let ready = ready("previous_wrapper", Some("nowledge-previous-wrapper:test"));
        let report = report(vec![
            shadow_check(
                "session query returns seeded memory",
                CompatibilityShadowStatus::Matched,
                None,
            ),
            shadow_check(
                "single memory projection",
                CompatibilityShadowStatus::PrimaryOnly,
                Some("projection metadata is not exposed"),
            ),
        ]);

        let error =
            enforce_external_shadow_adapter_smoke_requirements(&ready, &report, true).unwrap_err();
        assert!(error.to_string().contains(
            "requires all checks to run on previous-wrapper; primary-only checks: single memory projection"
        ));

        let json = external_shadow_adapter_smoke_report_json(&ready, &report, 3, None);
        assert_eq!(json["adapter_smoke_ready"], false);
        assert_eq!(json["dual_engine_evidence"]["ready"], false);
        assert_eq!(json["dual_engine_evidence"]["primary_engine"], "hawdb");
        assert_eq!(
            json["dual_engine_evidence"]["shadow_engine"],
            "legacy-wrapper"
        );
        assert_eq!(json["dual_engine_evidence"]["primary_check_count"], 2);
        assert_eq!(json["dual_engine_evidence"]["shadow_check_count"], 2);
        assert_eq!(json["dual_engine_evidence"]["matched_check_count"], 1);
        assert_eq!(json["dual_engine_evidence"]["primary_only_check_count"], 1);
        assert_eq!(json["matched_checks"], 1);
        assert_eq!(json["primary_only_checks"], 1);
        assert_eq!(
            json["primary_only_reasons"]["single memory projection"],
            "projection metadata is not exposed"
        );
    }

    fn ready(engine_kind: &str, wrapper_identity: Option<&str>) -> ExternalShadowReady {
        ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some(engine_kind.to_string()),
            wrapper_identity: wrapper_identity.map(str::to_string),
        }
    }

    fn report(shadow_checks: Vec<CompatibilityShadowCheckReport>) -> CompatibilityShadowReport {
        let primary_checks = shadow_checks
            .iter()
            .map(|check| CompatibilityCheckReport {
                name: check.name.clone(),
            })
            .collect();
        CompatibilityShadowReport {
            fixture: "external-shadow-adapter-smoke".to_string(),
            shadow_engine: "legacy-wrapper".to_string(),
            primary_checks,
            shadow_checks,
        }
    }

    fn shadow_check(
        name: &str,
        status: CompatibilityShadowStatus,
        primary_only_reason: Option<&str>,
    ) -> CompatibilityShadowCheckReport {
        CompatibilityShadowCheckReport {
            name: name.to_string(),
            status,
            primary_only_reason: primary_only_reason.map(str::to_string),
        }
    }
}
