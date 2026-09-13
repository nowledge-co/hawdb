use super::test_support::*;
use super::*;
use std::collections::BTreeMap;

#[test]
fn query_inventory_reports_fixture_coverage() {
    let fixture = nowledge_memory_core_fixture();
    let inventory = nowledge_memory_core_inventory();

    let coverage = assess_query_inventory_coverage(&fixture, &inventory);
    let gate =
        assess_query_inventory_gate(&coverage, CompatibilityInventoryCoveragePolicy::default());
    let coverage_json = super::compatibility_inventory_coverage_report_to_json(&coverage);
    let gate_json = super::compatibility_inventory_gate_report_to_json(&gate);

    assert_eq!(coverage.inventory, "nowledge-memory-core-inventory");
    assert_eq!(coverage.fixture, "nowledge-memory-core");
    assert_eq!(coverage.required_checks, 644);
    assert_eq!(coverage.covered_checks, 644);
    assert!(coverage.missing_checks.is_empty());
    assert!(coverage.extra_fixture_checks.is_empty());
    assert_eq!(gate.decision, CompatibilityCutoverDecision::Ready);
    assert!(gate.blockers.is_empty());
    assert_eq!(coverage_json["covered_checks"], 644);
    assert_eq!(gate_json["decision"], "ready");
    assert_eq!(gate_json["blockers"].as_array().unwrap().len(), 0);
}

#[test]
fn query_inventory_reports_missing_and_extra_checks() {
    let fixture = CompatibilityFixture {
        name: "partial".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "extra check",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
            ExpectedRows::RowCount(0),
        ))],
    };
    let inventory = CompatibilityQueryInventory {
        name: "required".to_string(),
        required_checks: vec![CompatibilityQueryInventoryItem::new(
            "required check",
            "read",
        )],
    };

    let coverage = assess_query_inventory_coverage(&fixture, &inventory);
    let gate = assess_query_inventory_gate(
        &coverage,
        CompatibilityInventoryCoveragePolicy {
            require_all_required_checks: true,
            allow_extra_fixture_checks: false,
        },
    );

    assert_eq!(coverage.required_checks, 1);
    assert_eq!(coverage.covered_checks, 0);
    assert_eq!(coverage.coverage_by_query_family.len(), 1);
    assert_eq!(coverage.coverage_by_query_family[0].query_family, "read");
    assert_eq!(coverage.coverage_by_query_family[0].required_checks, 1);
    assert_eq!(coverage.coverage_by_query_family[0].covered_checks, 0);
    assert_eq!(
        coverage.coverage_by_query_family[0].missing_checks,
        vec!["required check".to_string()]
    );
    assert_eq!(coverage.missing_checks, vec!["required check".to_string()]);
    assert_eq!(
        coverage.extra_fixture_checks,
        vec!["extra check".to_string()]
    );
    assert_eq!(gate.decision, CompatibilityCutoverDecision::Blocked);
    assert_eq!(gate.blockers.len(), 2);
    assert!(gate.blockers[0].contains("missing required query checks"));
    assert!(gate.blockers[1].contains("not declared by inventory"));
    let gate_json = super::compatibility_inventory_gate_report_to_json(&gate);
    assert_eq!(gate_json["decision"], "blocked");
    assert_eq!(
        gate_json["coverage_by_query_family"][0]["query_family"],
        "read"
    );
    assert_eq!(
        gate_json["coverage_by_query_family"][0]["coverage_per_million"],
        0
    );
    assert_eq!(gate_json["missing_checks"][0], "required check");
    assert_eq!(gate_json["extra_fixture_checks"][0], "extra check");
}

#[test]
fn query_inventory_can_audit_scanner_names_by_cypher() {
    let cypher = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";
    let fixture = CompatibilityFixture {
        name: "partial".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "semantic memory lookup",
            CypherFixtureStatement::new(cypher),
            ExpectedRows::RowCount(0),
        ))],
    };
    let inventory = CompatibilityQueryInventory {
        name: "scanned".to_string(),
        required_checks: vec![CompatibilityQueryInventoryItem::new(
            "crates/nmem-graph/src/store.rs:42:abcd",
            "read",
        )
        .with_source("crates/nmem-graph/src/store.rs:42")
        .with_cypher("MATCH (m:Memory) WHERE m.id = $id\nRETURN m.title AS title")],
    };

    let name_coverage = assess_query_inventory_coverage(&fixture, &inventory);
    let cypher_coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);

    assert_eq!(name_coverage.covered_checks, 0);
    assert_eq!(cypher_coverage.covered_checks, 1);
    assert!(cypher_coverage.missing_checks.is_empty());
    assert!(cypher_coverage.extra_fixture_checks.is_empty());

    let shadow = CompatibilityShadowReport {
        fixture: "partial".to_string(),
        shadow_engine: "scanner-shadow".to_string(),
        primary_checks: vec![CompatibilityCheckReport {
            name: "semantic memory lookup".to_string(),
        }],
        shadow_checks: vec![CompatibilityShadowCheckReport {
            name: "semantic memory lookup".to_string(),
            status: CompatibilityShadowStatus::Matched,
            primary_only_reason: None,
        }],
    };
    let bundle = assess_compatibility_cypher_migration_gate_bundle(
        &fixture,
        &inventory,
        &shadow,
        CompatibilityInventoryCoveragePolicy {
            require_all_required_checks: true,
            allow_extra_fixture_checks: false,
        },
        CompatibilityCutoverPolicy::default(),
    );

    assert_eq!(
        bundle.migration_gate.decision,
        CompatibilityCutoverDecision::Ready
    );
    assert!(bundle.migration_gate.blockers.is_empty());
    let json = super::compatibility_migration_gate_bundle_to_json(&bundle);
    assert_eq!(json["dual_engine_evidence"]["ready"], true);
    assert_eq!(json["dual_engine_evidence"]["primary_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["shadow_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["matched_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["primary_only_check_count"], 0);
    assert_eq!(json["cutover"]["dual_engine_evidence"]["ready"], true);
}

#[test]
fn cypher_migration_gate_can_require_rollback_evidence() {
    let cypher = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title";
    let fixture = CompatibilityFixture {
        name: "partial".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "semantic memory lookup",
            CypherFixtureStatement::new(cypher),
            ExpectedRows::RowCount(0),
        ))],
    };
    let inventory = CompatibilityQueryInventory {
        name: "scanned".to_string(),
        required_checks: vec![CompatibilityQueryInventoryItem::new(
            "crates/nmem-graph/src/store.rs:42:abcd",
            "read",
        )
        .with_source("crates/nmem-graph/src/store.rs:42")
        .with_cypher(cypher)],
    };
    let shadow = CompatibilityShadowReport {
        fixture: "partial".to_string(),
        shadow_engine: "previous-wrapper".to_string(),
        primary_checks: vec![CompatibilityCheckReport {
            name: "semantic memory lookup".to_string(),
        }],
        shadow_checks: vec![CompatibilityShadowCheckReport {
            name: "semantic memory lookup".to_string(),
            status: CompatibilityShadowStatus::Matched,
            primary_only_reason: None,
        }],
    };

    let blocked = assess_compatibility_cypher_migration_gate_bundle_with_rollback(
        &fixture,
        &inventory,
        &shadow,
        CompatibilityInventoryCoveragePolicy::default(),
        CompatibilityCutoverPolicy::default(),
        CompatibilityRollbackEvidence {
            required: true,
            ready: false,
            evidence: None,
            blockers: Vec::new(),
        },
    );
    assert_eq!(
        blocked.migration_gate.decision,
        CompatibilityCutoverDecision::Blocked
    );
    assert_eq!(blocked.migration_gate.rollback_blockers, 1);

    let ready = assess_compatibility_cypher_migration_gate_bundle_with_rollback(
        &fixture,
        &inventory,
        &shadow,
        CompatibilityInventoryCoveragePolicy::default(),
        CompatibilityCutoverPolicy::default(),
        CompatibilityRollbackEvidence {
            required: true,
            ready: true,
            evidence: Some("previous wrapper reopen smoke passed".to_string()),
            blockers: Vec::new(),
        },
    );
    assert_eq!(
        ready.migration_gate.decision,
        CompatibilityCutoverDecision::Ready
    );
    assert_eq!(
        ready.migration_gate.rollback_evidence.as_deref(),
        Some("previous wrapper reopen smoke passed")
    );
}

#[test]
fn migration_gate_combines_inventory_and_shadow_blockers() {
    let fixture = CompatibilityFixture {
        name: "partial".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "extra check",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
            ExpectedRows::RowCount(0),
        ))],
    };
    let inventory = CompatibilityQueryInventory {
        name: "required".to_string(),
        required_checks: vec![CompatibilityQueryInventoryItem::new(
            "required check",
            "read",
        )],
    };
    let inventory_gate = assess_query_inventory_gate(
        &assess_query_inventory_coverage(&fixture, &inventory),
        CompatibilityInventoryCoveragePolicy {
            require_all_required_checks: true,
            allow_extra_fixture_checks: false,
        },
    );
    let shadow = CompatibilityCutoverReport {
        fixture: "other-fixture".to_string(),
        shadow_engine: "shadow".to_string(),
        decision: CompatibilityCutoverDecision::Blocked,
        primary_check_count: 1,
        total_checks: 1,
        matched_checks: 0,
        primary_only_checks: vec!["extra check".to_string()],
        primary_only_reasons: BTreeMap::new(),
        blockers: vec!["shadow failed".to_string()],
    };

    let gate = assess_compatibility_migration_gate(&inventory_gate, &shadow);

    assert_eq!(gate.decision, CompatibilityCutoverDecision::Blocked);
    assert_eq!(
        gate.inventory_decision,
        CompatibilityCutoverDecision::Blocked
    );
    assert_eq!(gate.shadow_decision, CompatibilityCutoverDecision::Blocked);
    assert_eq!(gate.shadow_total_checks, 1);
    assert_eq!(gate.shadow_matched_checks, 0);
    assert_eq!(gate.shadow_primary_only_checks, 1);
    assert!(!gate.shadow_evidence_present);
    assert_eq!(gate.fixture_mismatch_blockers, 1);
    assert_eq!(gate.inventory_blockers, 2);
    assert_eq!(gate.shadow_blockers, 1);
    assert_eq!(gate.fixture_mismatch_blocker_messages.len(), 1);
    assert_eq!(gate.inventory_blocker_messages.len(), 2);
    assert_eq!(gate.shadow_blocker_messages, vec!["shadow failed"]);
    assert!(!gate.rollback_required);
    assert!(!gate.rollback_ready);
    assert_eq!(gate.rollback_blockers, 0);
    assert!(gate.rollback_blocker_messages.is_empty());
    assert_eq!(gate.blockers.len(), 4);
    assert!(gate.blockers[0].contains("does not match"));
    assert!(gate.blockers[1].starts_with("inventory:"));
    assert!(gate.blockers[3].starts_with("shadow:"));
    let gate_json = super::compatibility_migration_gate_report_to_json(&gate);
    assert_eq!(gate_json["decision"], "blocked");
    assert_eq!(gate_json["inventory_decision"], "blocked");
    assert_eq!(gate_json["shadow_decision"], "blocked");
    assert_eq!(gate_json["shadow_total_checks"], 1);
    assert_eq!(gate_json["shadow_matched_checks"], 0);
    assert_eq!(gate_json["shadow_primary_only_checks"], 1);
    assert_eq!(gate_json["shadow_evidence_present"], false);
    assert_eq!(gate_json["fixture_mismatch_blockers"], 1);
    assert_eq!(gate_json["inventory_blockers"], 2);
    assert_eq!(gate_json["shadow_blockers"], 1);
    assert_eq!(gate_json["rollback_required"], false);
    assert_eq!(gate_json["rollback_ready"], false);
    assert_eq!(gate_json["rollback_evidence"], serde_json::Value::Null);
    assert_eq!(gate_json["rollback_blockers"], 0);
    assert_eq!(
        gate_json["fixture_mismatch_blocker_messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        gate_json["inventory_blocker_messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(gate_json["shadow_blocker_messages"][0], "shadow failed");
    assert!(gate_json["rollback_blocker_messages"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(gate_json["blockers"].as_array().unwrap().len(), 4);
}

#[test]
fn migration_gate_blocks_when_required_rollback_evidence_is_missing() {
    let inventory_gate = CompatibilityInventoryGateReport {
        inventory: "required".to_string(),
        fixture: "fixture".to_string(),
        decision: CompatibilityCutoverDecision::Ready,
        required_checks: 1,
        covered_checks: 1,
        coverage_by_query_family: Vec::new(),
        missing_checks: Vec::new(),
        extra_fixture_checks: Vec::new(),
        blockers: Vec::new(),
    };
    let shadow = CompatibilityCutoverReport {
        fixture: "fixture".to_string(),
        shadow_engine: "previous-wrapper".to_string(),
        decision: CompatibilityCutoverDecision::Ready,
        primary_check_count: 1,
        total_checks: 1,
        matched_checks: 1,
        primary_only_checks: Vec::new(),
        primary_only_reasons: BTreeMap::new(),
        blockers: Vec::new(),
    };

    let gate = super::assess_compatibility_migration_gate_with_rollback(
        &inventory_gate,
        &shadow,
        CompatibilityRollbackEvidence {
            required: true,
            ready: false,
            evidence: None,
            blockers: Vec::new(),
        },
    );

    assert_eq!(gate.decision, CompatibilityCutoverDecision::Blocked);
    assert!(gate.rollback_required);
    assert!(!gate.rollback_ready);
    assert_eq!(gate.rollback_blockers, 1);
    assert_eq!(
        gate.rollback_blocker_messages[0],
        "previous database reopen evidence is required before cutover"
    );
    assert_eq!(
        gate.blockers[0],
        "rollback: previous database reopen evidence is required before cutover"
    );
}

#[test]
fn migration_gate_accepts_caller_owned_rollback_evidence() {
    let inventory_gate = CompatibilityInventoryGateReport {
        inventory: "required".to_string(),
        fixture: "fixture".to_string(),
        decision: CompatibilityCutoverDecision::Ready,
        required_checks: 1,
        covered_checks: 1,
        coverage_by_query_family: Vec::new(),
        missing_checks: Vec::new(),
        extra_fixture_checks: Vec::new(),
        blockers: Vec::new(),
    };
    let shadow = CompatibilityCutoverReport {
        fixture: "fixture".to_string(),
        shadow_engine: "previous-wrapper".to_string(),
        decision: CompatibilityCutoverDecision::Ready,
        primary_check_count: 1,
        total_checks: 1,
        matched_checks: 1,
        primary_only_checks: Vec::new(),
        primary_only_reasons: BTreeMap::new(),
        blockers: Vec::new(),
    };

    let gate = super::assess_compatibility_migration_gate_with_rollback(
        &inventory_gate,
        &shadow,
        CompatibilityRollbackEvidence {
            required: true,
            ready: true,
            evidence: Some("ladybug reopen smoke passed".to_string()),
            blockers: Vec::new(),
        },
    );

    assert_eq!(gate.decision, CompatibilityCutoverDecision::Ready);
    assert!(gate.rollback_required);
    assert!(gate.rollback_ready);
    assert_eq!(
        gate.rollback_evidence.as_deref(),
        Some("ladybug reopen smoke passed")
    );
    assert_eq!(gate.rollback_blockers, 0);
    assert!(gate.blockers.is_empty());

    let gate_json = super::compatibility_migration_gate_report_to_json(&gate);
    assert_eq!(gate_json["rollback_required"], true);
    assert_eq!(gate_json["rollback_ready"], true);
    assert_eq!(
        gate_json["rollback_evidence"],
        "ladybug reopen smoke passed"
    );
}

#[test]
fn migration_gate_bundle_reports_blocked_json() {
    let fixture = CompatibilityFixture {
        name: "partial".to_string(),
        setup: Vec::new(),
        checks: vec![CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
            "extra check",
            CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
            ExpectedRows::RowCount(0),
        ))],
    };
    let inventory = CompatibilityQueryInventory {
        name: "required".to_string(),
        required_checks: vec![CompatibilityQueryInventoryItem::new(
            "required check",
            "read",
        )],
    };
    let shadow = CompatibilityShadowReport {
        fixture: "partial".to_string(),
        shadow_engine: "shadow".to_string(),
        primary_checks: vec![CompatibilityCheckReport {
            name: "extra check".to_string(),
        }],
        shadow_checks: vec![CompatibilityShadowCheckReport {
            name: "extra check".to_string(),
            status: CompatibilityShadowStatus::PrimaryOnly,
            primary_only_reason: None,
        }],
    };

    let bundle = assess_compatibility_migration_gate_bundle(
        &fixture,
        &inventory,
        &shadow,
        CompatibilityInventoryCoveragePolicy {
            require_all_required_checks: true,
            allow_extra_fixture_checks: false,
        },
        CompatibilityCutoverPolicy::default(),
    );
    let json = super::compatibility_migration_gate_bundle_to_json(&bundle);

    assert_eq!(
        bundle.migration_gate.decision,
        CompatibilityCutoverDecision::Blocked
    );
    assert_eq!(json["coverage"]["missing_checks"][0], "required check");
    assert_eq!(json["coverage"]["coverage_per_million"], 0);
    assert_eq!(json["inventory_gate"]["decision"], "blocked");
    assert_eq!(json["inventory_gate"]["coverage_per_million"], 0);
    assert_eq!(json["cutover"]["decision"], "blocked");
    assert_eq!(json["cutover"]["primary_check_count"], 1);
    assert_eq!(json["cutover"]["matched_per_million"], 0);
    assert_eq!(json["cutover"]["dual_engine_evidence"]["ready"], false);
    assert_eq!(json["dual_engine_evidence"]["ready"], false);
    assert_eq!(
        json["dual_engine_evidence"]["primary_engine"],
        serde_json::json!("skein")
    );
    assert_eq!(
        json["dual_engine_evidence"]["shadow_engine"],
        serde_json::json!("shadow")
    );
    assert_eq!(json["dual_engine_evidence"]["primary_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["shadow_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["matched_check_count"], 0);
    assert_eq!(json["dual_engine_evidence"]["primary_only_check_count"], 1);
    assert_eq!(json["migration_gate"]["decision"], "blocked");
    assert_eq!(json["migration_gate"]["shadow_total_checks"], 1);
    assert_eq!(json["migration_gate"]["shadow_matched_checks"], 0);
    assert_eq!(json["migration_gate"]["shadow_matched_per_million"], 0);
    assert_eq!(json["migration_gate"]["shadow_primary_only_checks"], 1);
    assert_eq!(json["migration_gate"]["shadow_evidence_present"], false);
    assert_eq!(json["migration_gate"]["fixture_mismatch_blockers"], 0);
    assert_eq!(json["migration_gate"]["inventory_blockers"], 2);
    assert_eq!(json["migration_gate"]["shadow_blockers"], 2);
    assert_eq!(
        json["migration_gate"]["inventory_blocker_messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        json["migration_gate"]["shadow_blocker_messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        json["migration_gate"]["blockers"].as_array().unwrap().len(),
        4
    );
    assert_eq!(json["replacement_readiness_per_million"], 0);
}

#[test]
fn migration_gate_bundle_reports_replacement_readiness_by_query_family() {
    let fixture = CompatibilityFixture {
        name: "family-shadow".to_string(),
        setup: Vec::new(),
        checks: vec![
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "read check",
                CypherFixtureStatement::new("MATCH (m:Memory) RETURN m.id AS id"),
                ExpectedRows::RowCount(0),
            )),
            CompatibilityCheck::Cypher(CypherFixtureCheck::expect_rows(
                "write check",
                CypherFixtureStatement::new("MATCH (m:Memory) SET m.seen = true"),
                ExpectedRows::RowCount(0),
            )),
        ],
    };
    let inventory = CompatibilityQueryInventory {
        name: "family-inventory".to_string(),
        required_checks: vec![
            CompatibilityQueryInventoryItem::new("read check", "read"),
            CompatibilityQueryInventoryItem::new("write check", "mutation"),
        ],
    };
    let shadow = CompatibilityShadowReport {
        fixture: "family-shadow".to_string(),
        shadow_engine: "shadow".to_string(),
        primary_checks: vec![
            CompatibilityCheckReport {
                name: "read check".to_string(),
            },
            CompatibilityCheckReport {
                name: "write check".to_string(),
            },
        ],
        shadow_checks: vec![
            CompatibilityShadowCheckReport {
                name: "read check".to_string(),
                status: CompatibilityShadowStatus::Matched,
                primary_only_reason: None,
            },
            CompatibilityShadowCheckReport {
                name: "write check".to_string(),
                status: CompatibilityShadowStatus::PrimaryOnly,
                primary_only_reason: Some("shadow write disabled".to_string()),
            },
        ],
    };

    let bundle = assess_compatibility_migration_gate_bundle(
        &fixture,
        &inventory,
        &shadow,
        CompatibilityInventoryCoveragePolicy {
            require_all_required_checks: true,
            allow_extra_fixture_checks: false,
        },
        CompatibilityCutoverPolicy::default(),
    );
    let json = super::compatibility_migration_gate_bundle_to_json(&bundle);
    let families = json["replacement_readiness_by_query_family"]
        .as_array()
        .unwrap();

    assert_eq!(json["dual_engine_evidence"]["ready"], false);
    assert_eq!(json["dual_engine_evidence"]["primary_check_count"], 2);
    assert_eq!(json["dual_engine_evidence"]["shadow_check_count"], 2);
    assert_eq!(json["dual_engine_evidence"]["matched_check_count"], 1);
    assert_eq!(json["dual_engine_evidence"]["primary_only_check_count"], 1);
    assert_eq!(json["replacement_readiness_per_million"], 500_000);
    assert_eq!(families.len(), 2);
    assert_eq!(families[0]["query_family"], "mutation");
    assert_eq!(families[0]["covered_checks"], 1);
    assert_eq!(families[0]["shadow_matched_checks"], 0);
    assert_eq!(families[0]["shadow_primary_only_checks"], 1);
    assert_eq!(
        families[0]["shadow_primary_only_check_names"][0],
        "write check"
    );
    assert_eq!(families[0]["replacement_readiness_per_million"], 0);
    assert_eq!(families[1]["query_family"], "read");
    assert_eq!(families[1]["covered_checks"], 1);
    assert_eq!(families[1]["shadow_matched_checks"], 1);
    assert_eq!(families[1]["replacement_readiness_per_million"], 1_000_000);
}

#[test]
fn accepts_external_shadow_ready_preflight() {
    let script = write_external_shadow_script(
        "external-shadow-ready",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"op":"ready"'*) echo '{"ok":{"protocol_version":1,"capabilities":["execute","execute_session","project_graph"]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected ready"}}' ;;
  esac
done
"#,
    );
    let mut shadow = ExternalShadowCommand::spawn("external-shadow-ready", "sh", [script]).unwrap();

    let ready = shadow.require_ready().unwrap();

    assert_eq!(ready.protocol_version, EXTERNAL_SHADOW_PROTOCOL_VERSION);
    assert_eq!(shadow.request_count(), 1);
    assert_eq!(
        ready.capabilities,
        vec![
            "execute".to_string(),
            "execute_session".to_string(),
            "project_graph".to_string()
        ]
    );
    assert_eq!(ready.engine_kind, None);
}

#[test]
fn decodes_external_shadow_ready_engine_kind() {
    let script = write_external_shadow_script(
        "external-shadow-ready-engine-kind",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"op":"ready"'*) echo '{"ok":{"protocol_version":1,"engine_kind":"previous_wrapper","capabilities":["execute","execute_session","project_graph"]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected ready"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-ready-engine-kind", "sh", [script]).unwrap();

    let ready = shadow.require_ready().unwrap();

    assert_eq!(ready.engine_kind.as_deref(), Some("previous_wrapper"));
}

#[test]
fn rejects_external_shadow_ready_missing_required_capability() {
    let script = write_external_shadow_script(
        "external-shadow-ready-missing-capability",
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"op":"ready"'*) echo '{"ok":{"protocol_version":1,"capabilities":["execute","execute_session"]}}' ;;
    *) echo '{"error":{"class":"execution","message":"expected ready"}}' ;;
  esac
done
"#,
    );
    let mut shadow =
        ExternalShadowCommand::spawn("external-shadow-ready-missing-capability", "sh", [script])
            .unwrap();

    let error = shadow.require_ready().unwrap_err();

    assert!(error.to_string().contains("missing required capability"));
    assert!(error.to_string().contains("project_graph"));
}

#[test]
fn rejects_ambiguous_external_projected_graph_response_shape() {
    let error = super::decode_external_projected_graph_response(
        "ambiguous-projection-shadow",
        serde_json::json!({
            "primary_only": true,
            "error": {
                "class": "execution",
                "message": "projection hook failed"
            }
        }),
    )
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous-projection-shadow"));
    assert!(error.to_string().contains("only one of"));
}

#[test]
fn rejects_malformed_external_projected_graph_primary_only_flag() {
    let error = super::decode_external_projected_graph_response(
        "malformed-projection-shadow",
        serde_json::json!({
            "primary_only": "true"
        }),
    )
    .unwrap_err();

    assert!(error.to_string().contains("malformed-projection-shadow"));
    assert!(error
        .to_string()
        .contains("field 'primary_only' must be a boolean"));
}

#[test]
fn decodes_external_projected_graph_primary_only_reason() {
    let result = super::decode_external_projected_graph_response(
        "primary-only-projection-shadow",
        serde_json::json!({
            "primary_only": true,
            "reason": "projection metadata is not exposed"
        }),
    )
    .unwrap();

    assert_eq!(
        result,
        ProjectedGraphShadowResult::PrimaryOnly {
            reason: Some("projection metadata is not exposed".to_string())
        }
    );
}

#[test]
fn rejects_ambiguous_external_query_response_shape() {
    let error = super::decode_external_query_response(
        "ambiguous-query-shadow",
        serde_json::json!({
            "ok": {
                "rows": []
            },
            "error": {
                "class": "execution",
                "message": "query failed"
            }
        }),
    )
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous-query-shadow"));
    assert!(error.to_string().contains("only one of 'ok' or 'error'"));
}

#[test]
fn rejects_ambiguous_external_session_response_shape() {
    let error = super::decode_external_session_response(
        "ambiguous-session-shadow",
        serde_json::json!({
            "ok": {
                "outputs": []
            },
            "error": {
                "class": "execution",
                "message": "session failed"
            }
        }),
    )
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous-session-shadow"));
    assert!(error.to_string().contains("only one of 'ok' or 'error'"));
}

#[test]
fn rejects_ambiguous_external_ready_response_shape() {
    let error = super::decode_external_ready_response(
        "ambiguous-ready-shadow",
        serde_json::json!({
            "ok": {
                "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
                "capabilities": ["execute", "execute_session", "project_graph"]
            },
            "error": {
                "class": "execution",
                "message": "not ready"
            }
        }),
    )
    .unwrap_err();

    assert!(error.to_string().contains("ambiguous-ready-shadow"));
    assert!(error.to_string().contains("only one of 'ok' or 'error'"));
}
