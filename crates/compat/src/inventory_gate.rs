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
    assess_compatibility_cutover, CompatibilityCheck, CompatibilityCutoverPolicy,
    CompatibilityCutoverReport, CompatibilityFixture, CompatibilityShadowReport,
};
pub use hawdb_evidence::query_inventory::{
    build_compatibility_query_inventory, build_compatibility_query_inventory_from_json,
    build_compatibility_query_inventory_from_json_str, compatibility_query_inventory_to_json,
    CompatibilityQueryCallSite, CompatibilityQueryInventory, CompatibilityQueryInventoryItem,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityInventoryCoverageReport {
    pub inventory: String,
    pub fixture: String,
    pub required_checks: usize,
    pub covered_checks: usize,
    pub coverage_by_query_family: Vec<CompatibilityQueryFamilyCoverage>,
    pub missing_checks: Vec<String>,
    pub extra_fixture_checks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityQueryFamilyCoverage {
    pub query_family: String,
    pub required_checks: usize,
    pub covered_checks: usize,
    pub covered_check_names: Vec<String>,
    pub missing_checks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibilityInventoryCoveragePolicy {
    pub require_all_required_checks: bool,
    pub allow_extra_fixture_checks: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityInventoryGateReport {
    pub inventory: String,
    pub fixture: String,
    pub decision: CompatibilityCutoverDecision,
    pub required_checks: usize,
    pub covered_checks: usize,
    pub coverage_by_query_family: Vec<CompatibilityQueryFamilyCoverage>,
    pub missing_checks: Vec<String>,
    pub extra_fixture_checks: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityMigrationGateReport {
    pub fixture: String,
    pub inventory: String,
    pub shadow_engine: String,
    pub decision: CompatibilityCutoverDecision,
    pub inventory_decision: CompatibilityCutoverDecision,
    pub shadow_decision: CompatibilityCutoverDecision,
    pub shadow_total_checks: usize,
    pub shadow_matched_checks: usize,
    pub shadow_primary_only_checks: usize,
    pub shadow_evidence_present: bool,
    pub fixture_mismatch_blockers: usize,
    pub inventory_blockers: usize,
    pub shadow_blockers: usize,
    pub rollback_required: bool,
    pub rollback_ready: bool,
    pub rollback_evidence: Option<String>,
    pub rollback_blockers: usize,
    pub fixture_mismatch_blocker_messages: Vec<String>,
    pub inventory_blocker_messages: Vec<String>,
    pub shadow_blocker_messages: Vec<String>,
    pub rollback_blocker_messages: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityMigrationGateBundle {
    pub coverage: CompatibilityInventoryCoverageReport,
    pub inventory_gate: CompatibilityInventoryGateReport,
    pub cutover: CompatibilityCutoverReport,
    pub migration_gate: CompatibilityMigrationGateReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CompatibilityRollbackEvidence {
    pub required: bool,
    pub ready: bool,
    pub evidence: Option<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityCutoverDecision {
    Ready,
    Blocked,
}

impl Default for CompatibilityInventoryCoveragePolicy {
    fn default() -> Self {
        Self {
            require_all_required_checks: true,
            allow_extra_fixture_checks: true,
        }
    }
}

pub fn compatibility_inventory_coverage_report_to_json(
    report: &CompatibilityInventoryCoverageReport,
) -> serde_json::Value {
    serde_json::json!({
        "inventory": report.inventory,
        "fixture": report.fixture,
        "required_checks": report.required_checks,
        "covered_checks": report.covered_checks,
        "coverage_per_million": ratio_per_million(report.covered_checks, report.required_checks),
        "coverage_by_query_family": report
            .coverage_by_query_family
            .iter()
            .map(query_family_coverage_to_json)
            .collect::<Vec<_>>(),
        "missing_checks": report.missing_checks,
        "extra_fixture_checks": report.extra_fixture_checks,
    })
}

pub fn compatibility_inventory_gate_report_to_json(
    report: &CompatibilityInventoryGateReport,
) -> serde_json::Value {
    serde_json::json!({
        "inventory": report.inventory,
        "fixture": report.fixture,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "required_checks": report.required_checks,
        "covered_checks": report.covered_checks,
        "coverage_per_million": ratio_per_million(report.covered_checks, report.required_checks),
        "coverage_by_query_family": report
            .coverage_by_query_family
            .iter()
            .map(query_family_coverage_to_json)
            .collect::<Vec<_>>(),
        "missing_checks": report.missing_checks,
        "extra_fixture_checks": report.extra_fixture_checks,
        "blockers": report.blockers,
    })
}

pub fn compatibility_cutover_report_to_json(
    report: &CompatibilityCutoverReport,
) -> serde_json::Value {
    serde_json::json!({
        "fixture": report.fixture,
        "shadow_engine": report.shadow_engine,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "primary_check_count": report.primary_check_count,
        "total_checks": report.total_checks,
        "matched_checks": report.matched_checks,
        "matched_per_million": ratio_per_million(report.matched_checks, report.total_checks),
        "primary_only_checks": report.primary_only_checks,
        "primary_only_reasons": report.primary_only_reasons,
        "dual_engine_evidence": cutover_dual_engine_evidence_to_json(report),
        "blockers": report.blockers,
    })
}

pub fn compatibility_migration_gate_report_to_json(
    report: &CompatibilityMigrationGateReport,
) -> serde_json::Value {
    serde_json::json!({
        "fixture": report.fixture,
        "inventory": report.inventory,
        "shadow_engine": report.shadow_engine,
        "decision": compatibility_cutover_decision_as_str(report.decision),
        "inventory_decision": compatibility_cutover_decision_as_str(report.inventory_decision),
        "shadow_decision": compatibility_cutover_decision_as_str(report.shadow_decision),
        "shadow_total_checks": report.shadow_total_checks,
        "shadow_matched_checks": report.shadow_matched_checks,
        "shadow_primary_only_checks": report.shadow_primary_only_checks,
        "shadow_matched_per_million": ratio_per_million(
            report.shadow_matched_checks,
            report.shadow_total_checks
        ),
        "shadow_evidence_present": report.shadow_evidence_present,
        "fixture_mismatch_blockers": report.fixture_mismatch_blockers,
        "inventory_blockers": report.inventory_blockers,
        "shadow_blockers": report.shadow_blockers,
        "rollback_required": report.rollback_required,
        "rollback_ready": report.rollback_ready,
        "rollback_evidence": report.rollback_evidence,
        "rollback_blockers": report.rollback_blockers,
        "fixture_mismatch_blocker_messages": report.fixture_mismatch_blocker_messages,
        "inventory_blocker_messages": report.inventory_blocker_messages,
        "shadow_blocker_messages": report.shadow_blocker_messages,
        "rollback_blocker_messages": report.rollback_blocker_messages,
        "blockers": report.blockers,
    })
}

pub fn compatibility_migration_gate_bundle_to_json(
    bundle: &CompatibilityMigrationGateBundle,
) -> serde_json::Value {
    serde_json::json!({
        "coverage": compatibility_inventory_coverage_report_to_json(&bundle.coverage),
        "inventory_gate": compatibility_inventory_gate_report_to_json(&bundle.inventory_gate),
        "cutover": compatibility_cutover_report_to_json(&bundle.cutover),
        "migration_gate": compatibility_migration_gate_report_to_json(&bundle.migration_gate),
        "dual_engine_evidence": cutover_dual_engine_evidence_to_json(&bundle.cutover),
        "replacement_readiness_by_query_family": replacement_readiness_by_query_family_to_json(bundle),
        "replacement_readiness_per_million": ratio_per_million(
            bundle.coverage.covered_checks.min(bundle.cutover.matched_checks),
            bundle.coverage.required_checks.max(bundle.cutover.total_checks),
        ),
    })
}

fn compatibility_cutover_decision_as_str(decision: CompatibilityCutoverDecision) -> &'static str {
    match decision {
        CompatibilityCutoverDecision::Ready => "ready",
        CompatibilityCutoverDecision::Blocked => "blocked",
    }
}

fn ratio_per_million(numerator: usize, denominator: usize) -> u64 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128).saturating_mul(1_000_000) / denominator as u128) as u64
}

fn cutover_dual_engine_evidence_to_json(report: &CompatibilityCutoverReport) -> serde_json::Value {
    let primary_only_check_count = report.primary_only_checks.len();
    serde_json::json!({
        "ready": report.decision == CompatibilityCutoverDecision::Ready
            && report.primary_check_count == report.total_checks
            && report.total_checks > 0
            && report.matched_checks == report.total_checks
            && primary_only_check_count == 0,
        "primary_engine": "hawdb",
        "shadow_engine": report.shadow_engine,
        "primary_check_count": report.primary_check_count,
        "shadow_check_count": report.total_checks,
        "matched_check_count": report.matched_checks,
        "primary_only_check_count": primary_only_check_count,
        "matched_per_million": ratio_per_million(report.matched_checks, report.total_checks),
    })
}

fn query_family_coverage_to_json(report: &CompatibilityQueryFamilyCoverage) -> serde_json::Value {
    serde_json::json!({
        "query_family": report.query_family,
        "required_checks": report.required_checks,
        "covered_checks": report.covered_checks,
        "coverage_per_million": ratio_per_million(report.covered_checks, report.required_checks),
        "missing_checks": report.missing_checks,
    })
}

fn replacement_readiness_by_query_family_to_json(
    bundle: &CompatibilityMigrationGateBundle,
) -> Vec<serde_json::Value> {
    let primary_only_checks = bundle
        .cutover
        .primary_only_checks
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    bundle
        .coverage
        .coverage_by_query_family
        .iter()
        .map(|family| {
            let shadow_primary_only_check_names = family
                .covered_check_names
                .iter()
                .filter(|check| primary_only_checks.contains(check.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            let shadow_primary_only_checks = shadow_primary_only_check_names.len();
            let shadow_matched_checks = family.covered_checks - shadow_primary_only_checks;
            serde_json::json!({
                "query_family": family.query_family,
                "required_checks": family.required_checks,
                "covered_checks": family.covered_checks,
                "inventory_missing_checks": family.missing_checks,
                "shadow_matched_checks": shadow_matched_checks,
                "shadow_primary_only_checks": shadow_primary_only_checks,
                "shadow_primary_only_check_names": shadow_primary_only_check_names,
                "coverage_per_million": ratio_per_million(family.covered_checks, family.required_checks),
                "shadow_matched_per_million": ratio_per_million(shadow_matched_checks, family.required_checks),
                "replacement_readiness_per_million": ratio_per_million(
                    family.covered_checks.min(shadow_matched_checks),
                    family.required_checks,
                ),
            })
        })
        .collect()
}

pub fn assess_query_inventory_coverage(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
) -> CompatibilityInventoryCoverageReport {
    let fixture_checks = fixture
        .checks
        .iter()
        .map(compatibility_check_name)
        .collect::<BTreeSet<_>>();
    let required_checks = inventory
        .required_checks
        .iter()
        .map(|check| check.name.as_str())
        .collect::<BTreeSet<_>>();
    let missing_checks = required_checks
        .difference(&fixture_checks)
        .map(|check| (*check).to_string())
        .collect::<Vec<_>>();
    let extra_fixture_checks = fixture_checks
        .difference(&required_checks)
        .map(|check| (*check).to_string())
        .collect::<Vec<_>>();
    let coverage_by_query_family =
        coverage_by_query_family(inventory.required_checks.iter(), |item| {
            fixture_checks.contains(item.name.as_str())
        });

    CompatibilityInventoryCoverageReport {
        inventory: inventory.name.clone(),
        fixture: fixture.name.clone(),
        required_checks: required_checks.len(),
        covered_checks: required_checks.len() - missing_checks.len(),
        coverage_by_query_family,
        missing_checks,
        extra_fixture_checks,
    }
}

pub fn assess_query_inventory_cypher_coverage(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
) -> CompatibilityInventoryCoverageReport {
    let fixture_keys = fixture
        .checks
        .iter()
        .map(compatibility_check_coverage_key)
        .collect::<BTreeSet<_>>();
    let required_keys = inventory
        .required_checks
        .iter()
        .map(inventory_item_coverage_key)
        .collect::<BTreeSet<_>>();
    let missing_checks = inventory
        .required_checks
        .iter()
        .filter(|item| !fixture_keys.contains(&inventory_item_coverage_key(item)))
        .map(|item| item.name.clone())
        .collect::<Vec<_>>();
    let extra_fixture_checks = fixture
        .checks
        .iter()
        .filter(|check| !required_keys.contains(&compatibility_check_coverage_key(check)))
        .map(compatibility_check_name)
        .map(str::to_string)
        .collect::<Vec<_>>();
    let coverage_by_query_family =
        coverage_by_query_family(inventory.required_checks.iter(), |item| {
            fixture_keys.contains(&inventory_item_coverage_key(item))
        });

    CompatibilityInventoryCoverageReport {
        inventory: inventory.name.clone(),
        fixture: fixture.name.clone(),
        required_checks: inventory.required_checks.len(),
        covered_checks: inventory.required_checks.len() - missing_checks.len(),
        coverage_by_query_family,
        missing_checks,
        extra_fixture_checks,
    }
}

fn coverage_by_query_family<'a>(
    items: impl Iterator<Item = &'a CompatibilityQueryInventoryItem>,
    is_covered: impl Fn(&CompatibilityQueryInventoryItem) -> bool,
) -> Vec<CompatibilityQueryFamilyCoverage> {
    #[derive(Default)]
    struct FamilyAccumulator {
        required_checks: usize,
        covered_checks: usize,
        covered_check_names: Vec<String>,
        missing_checks: Vec<String>,
    }

    let mut families = BTreeMap::<String, FamilyAccumulator>::new();
    for item in items {
        let family = families.entry(item.query_family.clone()).or_default();
        family.required_checks += 1;
        if is_covered(item) {
            family.covered_checks += 1;
            family.covered_check_names.push(item.name.clone());
        } else {
            family.missing_checks.push(item.name.clone());
        }
    }

    families
        .into_iter()
        .map(|(query_family, family)| CompatibilityQueryFamilyCoverage {
            query_family,
            required_checks: family.required_checks,
            covered_checks: family.covered_checks,
            covered_check_names: family.covered_check_names,
            missing_checks: family.missing_checks,
        })
        .collect()
}

pub fn assess_query_inventory_gate(
    coverage: &CompatibilityInventoryCoverageReport,
    policy: CompatibilityInventoryCoveragePolicy,
) -> CompatibilityInventoryGateReport {
    let mut blockers = Vec::new();
    if coverage.required_checks == 0 {
        blockers.push("query inventory has no required checks".to_string());
    }
    if policy.require_all_required_checks && !coverage.missing_checks.is_empty() {
        blockers.push(format!(
            "fixture '{}' is missing required query checks: {}",
            coverage.fixture,
            coverage.missing_checks.join(", ")
        ));
    }
    if !policy.allow_extra_fixture_checks && !coverage.extra_fixture_checks.is_empty() {
        blockers.push(format!(
            "fixture '{}' contains checks not declared by inventory '{}': {}",
            coverage.fixture,
            coverage.inventory,
            coverage.extra_fixture_checks.join(", ")
        ));
    }

    CompatibilityInventoryGateReport {
        inventory: coverage.inventory.clone(),
        fixture: coverage.fixture.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        required_checks: coverage.required_checks,
        covered_checks: coverage.covered_checks,
        coverage_by_query_family: coverage.coverage_by_query_family.clone(),
        missing_checks: coverage.missing_checks.clone(),
        extra_fixture_checks: coverage.extra_fixture_checks.clone(),
        blockers,
    }
}

pub fn assess_compatibility_migration_gate(
    inventory: &CompatibilityInventoryGateReport,
    shadow: &CompatibilityCutoverReport,
) -> CompatibilityMigrationGateReport {
    assess_compatibility_migration_gate_with_rollback(
        inventory,
        shadow,
        CompatibilityRollbackEvidence::default(),
    )
}

pub fn assess_compatibility_migration_gate_with_rollback(
    inventory: &CompatibilityInventoryGateReport,
    shadow: &CompatibilityCutoverReport,
    rollback: CompatibilityRollbackEvidence,
) -> CompatibilityMigrationGateReport {
    let mut blockers = Vec::new();
    let mut fixture_mismatch_blocker_messages = Vec::new();
    if inventory.fixture != shadow.fixture {
        fixture_mismatch_blocker_messages.push(format!(
            "inventory fixture '{}' does not match shadow fixture '{}'",
            inventory.fixture, shadow.fixture
        ));
    }
    blockers.extend(fixture_mismatch_blocker_messages.iter().cloned());
    let inventory_blocker_messages = inventory.blockers.clone();
    let inventory_blockers = inventory.blockers.len();
    blockers.extend(
        inventory_blocker_messages
            .iter()
            .map(|blocker| format!("inventory: {blocker}")),
    );
    let shadow_blocker_messages = shadow.blockers.clone();
    let shadow_blockers = shadow.blockers.len();
    blockers.extend(
        shadow_blocker_messages
            .iter()
            .map(|blocker| format!("shadow: {blocker}")),
    );
    let rollback_blocker_messages = rollback_blockers(&rollback);
    let rollback_blockers = rollback_blocker_messages.len();
    blockers.extend(
        rollback_blocker_messages
            .iter()
            .map(|blocker| format!("rollback: {blocker}")),
    );

    CompatibilityMigrationGateReport {
        fixture: inventory.fixture.clone(),
        inventory: inventory.inventory.clone(),
        shadow_engine: shadow.shadow_engine.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        inventory_decision: inventory.decision,
        shadow_decision: shadow.decision,
        shadow_total_checks: shadow.total_checks,
        shadow_matched_checks: shadow.matched_checks,
        shadow_primary_only_checks: shadow.primary_only_checks.len(),
        shadow_evidence_present: shadow.matched_checks > 0,
        fixture_mismatch_blockers: fixture_mismatch_blocker_messages.len(),
        inventory_blockers,
        shadow_blockers,
        rollback_required: rollback.required,
        rollback_ready: rollback.ready,
        rollback_evidence: rollback.evidence,
        rollback_blockers,
        fixture_mismatch_blocker_messages,
        inventory_blocker_messages,
        shadow_blocker_messages,
        rollback_blocker_messages,
        blockers,
    }
}

fn rollback_blockers(rollback: &CompatibilityRollbackEvidence) -> Vec<String> {
    let mut blockers = rollback.blockers.clone();
    if rollback.required && !rollback.ready {
        blockers.push("previous database reopen evidence is required before cutover".to_string());
    }
    blockers
}

pub fn assess_compatibility_migration_gate_bundle(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
    shadow: &CompatibilityShadowReport,
    inventory_policy: CompatibilityInventoryCoveragePolicy,
    cutover_policy: CompatibilityCutoverPolicy,
) -> CompatibilityMigrationGateBundle {
    let coverage = assess_query_inventory_coverage(fixture, inventory);
    let inventory_gate = assess_query_inventory_gate(&coverage, inventory_policy);
    let cutover = assess_compatibility_cutover(shadow, cutover_policy);
    let migration_gate = assess_compatibility_migration_gate(&inventory_gate, &cutover);
    CompatibilityMigrationGateBundle {
        coverage,
        inventory_gate,
        cutover,
        migration_gate,
    }
}

pub fn assess_compatibility_cypher_migration_gate_bundle(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
    shadow: &CompatibilityShadowReport,
    inventory_policy: CompatibilityInventoryCoveragePolicy,
    cutover_policy: CompatibilityCutoverPolicy,
) -> CompatibilityMigrationGateBundle {
    assess_compatibility_cypher_migration_gate_bundle_with_rollback(
        fixture,
        inventory,
        shadow,
        inventory_policy,
        cutover_policy,
        CompatibilityRollbackEvidence::default(),
    )
}

pub fn assess_compatibility_cypher_migration_gate_bundle_with_rollback(
    fixture: &CompatibilityFixture,
    inventory: &CompatibilityQueryInventory,
    shadow: &CompatibilityShadowReport,
    inventory_policy: CompatibilityInventoryCoveragePolicy,
    cutover_policy: CompatibilityCutoverPolicy,
    rollback: CompatibilityRollbackEvidence,
) -> CompatibilityMigrationGateBundle {
    let coverage = assess_query_inventory_cypher_coverage(fixture, inventory);
    let inventory_gate = assess_query_inventory_gate(&coverage, inventory_policy);
    let cutover = assess_compatibility_cutover(shadow, cutover_policy);
    let migration_gate =
        assess_compatibility_migration_gate_with_rollback(&inventory_gate, &cutover, rollback);
    CompatibilityMigrationGateBundle {
        coverage,
        inventory_gate,
        cutover,
        migration_gate,
    }
}

fn compatibility_check_name(check: &CompatibilityCheck) -> &str {
    match check {
        CompatibilityCheck::Cypher(check) => &check.name,
        CompatibilityCheck::ProjectedGraph(check) => &check.name,
    }
}

fn compatibility_check_coverage_key(check: &CompatibilityCheck) -> String {
    match check {
        CompatibilityCheck::Cypher(check) => cypher_coverage_key(&check.statement.cypher),
        CompatibilityCheck::ProjectedGraph(check) => check.name.clone(),
    }
}

fn inventory_item_coverage_key(item: &CompatibilityQueryInventoryItem) -> String {
    item.cypher
        .as_deref()
        .map(cypher_coverage_key)
        .unwrap_or_else(|| item.name.clone())
}

fn cypher_coverage_key(cypher: &str) -> String {
    cypher.split_whitespace().collect::<Vec<_>>().join(" ")
}
