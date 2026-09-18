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

//! Source-derived Nowledge query-inventory coverage over compatibility fixtures.
//! Database execution and host-specific migration-gate assembly stay in the facade.

use crate::{
    assess_query_inventory_cypher_coverage, compatibility_inventory_coverage_report_to_json,
    external_shadow_ready_missing_capabilities, external_shadow_trace_health_from_bundle,
    external_shadow_trace_report_json, nowledge_memory_core_fixture, CompatibilityCheck,
    CompatibilityQueryInventoryItem, CompatibilityRollbackEvidence, ExternalShadowReady,
    REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};
use hawdb_core::{HawDBError, Result};
use hawdb_evidence::{
    inventory::{
        background_maintenance_evidence_health_from_bundle,
        replacement_readiness_family_evidence_health_from_bundle,
        storage_recovery_evidence_health_from_bundle,
    },
    query_inventory::scan_nowledge_query_inventory,
};
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeCypherMigrationGateJsonOptions {
    pub shadow_name: Option<String>,
    pub self_shadow: bool,
    pub shadow_ready: Option<ExternalShadowReady>,
    pub ready_preflight: bool,
    pub shadow_trace_path: Option<String>,
    pub shadow_request_count: Option<u64>,
    pub include_cutover_evidence: bool,
    pub storage_recovery_required: bool,
    pub storage_recovery: Option<serde_json::Value>,
    pub background_maintenance_required: bool,
    pub background_maintenance: Option<serde_json::Value>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub previous_wrapper_contract_evidence: Option<serde_json::Value>,
    pub rollback: CompatibilityRollbackEvidence,
}

pub fn scan_nowledge_query_inventory_cypher_coverage_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    Ok(compatibility_inventory_coverage_report_to_json(&coverage))
}

pub fn scan_nowledge_query_inventory_cypher_coverage_detail_to_json(
    root: impl AsRef<Path>,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let coverage = assess_query_inventory_cypher_coverage(&fixture, &inventory);
    let fixture_cypher_keys = fixture
        .checks
        .iter()
        .filter_map(fixture_check_cypher)
        .map(cypher_coverage_key)
        .collect::<BTreeSet<_>>();
    let missing_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_none_or(|key| !fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();
    let covered_items = inventory
        .required_checks
        .iter()
        .filter(|item| {
            item.cypher
                .as_deref()
                .map(cypher_coverage_key)
                .is_some_and(|key| fixture_cypher_keys.contains(&key))
        })
        .map(inventory_item_detail_to_json)
        .collect::<Vec<_>>();

    Ok(serde_json::json!({
        "coverage": compatibility_inventory_coverage_report_to_json(&coverage),
        "covered_items": covered_items,
        "missing_items": missing_items,
    }))
}

fn fixture_check_cypher(check: &CompatibilityCheck) -> Option<&str> {
    match check {
        CompatibilityCheck::Cypher(check) => Some(check.statement.cypher.as_str()),
        CompatibilityCheck::ProjectedGraph(_) => None,
    }
}

fn inventory_item_detail_to_json(item: &CompatibilityQueryInventoryItem) -> serde_json::Value {
    serde_json::json!({
        "name": item.name,
        "query_family": item.query_family,
        "source": item.source,
        "cypher": item.cypher,
    })
}

fn cypher_coverage_key(cypher: &str) -> String {
    cypher.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[doc(hidden)]
pub fn augment_nowledge_cypher_migration_gate_json(
    bundle: &mut serde_json::Value,
    fallback_shadow_name: &str,
    options: NowledgeCypherMigrationGateJsonOptions,
) -> Result<()> {
    let shadow_name = options
        .shadow_name
        .as_deref()
        .unwrap_or(fallback_shadow_name);
    let ready_preflight = options.ready_preflight || options.shadow_ready.is_some();

    if options.shadow_name.is_some() || options.include_cutover_evidence {
        insert_shadow_run_json(bundle, shadow_name, options.self_shadow)?;
    }
    if let Some(ready) = options.shadow_ready.as_ref() {
        insert_shadow_ready_json(bundle, ready)?;
    }
    if let Some(trace_path) = options.shadow_trace_path.as_ref() {
        insert_shadow_trace_json(
            bundle,
            trace_path,
            options.shadow_request_count.unwrap_or_default(),
        )?;
    }
    if let Some(storage_recovery) = options.storage_recovery.as_ref() {
        migration_gate_json_object(bundle)?
            .insert("storage_recovery".to_string(), storage_recovery.clone());
    }
    if let Some(contract_evidence) = options.previous_wrapper_contract_evidence.as_ref() {
        migration_gate_json_object(bundle)?.insert(
            "previous_wrapper_contract_evidence".to_string(),
            contract_evidence.clone(),
        );
    }
    if options.include_cutover_evidence {
        insert_cutover_evidence_json(
            bundle,
            options.self_shadow,
            ready_preflight,
            options.shadow_ready.as_ref(),
            options.storage_recovery_required,
            options.background_maintenance_required,
        )?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn migration_gate_json_object(
    bundle: &mut serde_json::Value,
) -> Result<&mut serde_json::Map<String, serde_json::Value>> {
    bundle.as_object_mut().ok_or_else(|| {
        HawDBError::Execution("migration gate bundle must be a JSON object".to_string())
    })
}

fn insert_json<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(
        key.to_string(),
        serde_json::to_value(value).expect("cutover evidence values must serialize"),
    );
}

fn insert_shadow_run_json(
    bundle: &mut serde_json::Value,
    shadow_name: &str,
    self_shadow: bool,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_run".to_string(),
        serde_json::json!({
            "shadow_name": shadow_name,
            "self_shadow": self_shadow,
            "evidence_kind": if self_shadow {
                "protocol_smoke"
            } else {
                "previous_wrapper"
            },
        }),
    );
    Ok(())
}

fn insert_shadow_ready_json(
    bundle: &mut serde_json::Value,
    ready: &ExternalShadowReady,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_ready".to_string(),
        serde_json::json!({
            "protocol_version": ready.protocol_version,
            "capabilities": &ready.capabilities,
            "engine_kind": &ready.engine_kind,
            "wrapper_identity": &ready.wrapper_identity,
        }),
    );
    Ok(())
}

fn insert_shadow_trace_json(
    bundle: &mut serde_json::Value,
    trace_path: &str,
    request_count: u64,
) -> Result<()> {
    migration_gate_json_object(bundle)?.insert(
        "shadow_trace".to_string(),
        external_shadow_trace_report_json(trace_path, request_count),
    );
    Ok(())
}

fn insert_cutover_evidence_json(
    bundle: &mut serde_json::Value,
    self_shadow: bool,
    ready_preflight: bool,
    shadow_ready: Option<&ExternalShadowReady>,
    storage_recovery_required: bool,
    background_maintenance_required: bool,
) -> Result<()> {
    let migration_gate = bundle
        .get("migration_gate")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            HawDBError::Execution("migration gate bundle missing migration_gate".to_string())
        })?;
    let migration_gate_ready = migration_gate
        .get("decision")
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let ready_engine_kind = shadow_ready.and_then(|ready| ready.engine_kind.as_deref());
    let ready_wrapper_identity = shadow_ready.and_then(|ready| ready.wrapper_identity.as_deref());
    let ready_missing_capabilities = external_shadow_ready_missing_capabilities(shadow_ready);
    let shadow_evidence_present = migration_gate
        .get("shadow_evidence_present")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let shadow_trace_health = external_shadow_trace_health_from_bundle(bundle);
    let storage_recovery_health =
        storage_recovery_evidence_health_from_bundle(bundle, storage_recovery_required);
    let background_maintenance_health =
        background_maintenance_evidence_health_from_bundle(bundle, background_maintenance_required);
    let replacement_family_health =
        replacement_readiness_family_evidence_health_from_bundle(bundle);
    let mut blockers = Vec::new();
    if self_shadow {
        blockers.push("shadow run is protocol smoke, not previous-wrapper evidence".to_string());
    }
    if !ready_preflight {
        blockers.push("shadow ready preflight was not executed".to_string());
    }
    if ready_preflight && ready_engine_kind.is_none() {
        blockers.push("shadow ready response missing engine_kind".to_string());
    }
    if let Some(engine_kind) = ready_engine_kind
        && engine_kind != "previous_wrapper"
    {
        blockers.push("shadow ready engine_kind is not previous_wrapper".to_string());
    }
    if ready_preflight
        && ready_engine_kind == Some("previous_wrapper")
        && ready_wrapper_identity.is_none()
    {
        blockers.push("shadow ready response missing wrapper_identity".to_string());
    }
    if ready_preflight && !ready_missing_capabilities.is_empty() {
        blockers.push("shadow ready response missing required capabilities".to_string());
    }
    if !shadow_evidence_present {
        blockers.push("no matched shadow checks are present".to_string());
    }
    if shadow_trace_health.present && !shadow_trace_health.complete {
        blockers.push("shadow trace is incomplete or unavailable".to_string());
    }
    if !storage_recovery_health.ready {
        blockers.extend(storage_recovery_health.blockers.iter().cloned());
    }
    if !background_maintenance_health.ready {
        blockers.extend(background_maintenance_health.blockers.iter().cloned());
    }
    if !replacement_family_health.ready {
        blockers.extend(replacement_family_health.blockers.iter().cloned());
    }
    if !migration_gate_ready {
        blockers.push("migration gate decision is not ready".to_string());
    }

    let mut evidence = serde_json::Map::new();
    insert_json(&mut evidence, "eligible", blockers.is_empty());
    insert_json(
        &mut evidence,
        "evidence_kind",
        if self_shadow {
            "protocol_smoke"
        } else {
            "previous_wrapper"
        },
    );
    insert_json(&mut evidence, "requires_previous_wrapper", true);
    insert_json(&mut evidence, "requires_ready_preflight", true);
    insert_json(
        &mut evidence,
        "requires_ready_engine_kind",
        "previous_wrapper",
    );
    insert_json(&mut evidence, "requires_ready_wrapper_identity", true);
    insert_json(
        &mut evidence,
        "requires_ready_capabilities",
        REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
    );
    insert_json(&mut evidence, "requires_shadow_evidence", true);
    insert_json(&mut evidence, "ready_preflight", ready_preflight);
    insert_json(&mut evidence, "ready_engine_kind", ready_engine_kind);
    insert_json(
        &mut evidence,
        "ready_wrapper_identity",
        ready_wrapper_identity,
    );
    insert_json(
        &mut evidence,
        "ready_missing_capabilities",
        ready_missing_capabilities,
    );
    insert_json(
        &mut evidence,
        "shadow_evidence_present",
        shadow_evidence_present,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_present",
        shadow_trace_health.present,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_complete",
        shadow_trace_health.complete,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_summary_available",
        shadow_trace_health.summary_available,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_request_count_matches",
        shadow_trace_health.request_count_matches,
    );
    insert_json(
        &mut evidence,
        "shadow_trace_pending_request_count",
        shadow_trace_health.pending_request_count,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_required",
        storage_recovery_health.required,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_present",
        storage_recovery_health.present,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_ready",
        storage_recovery_health.ready,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_protocol_matches",
        storage_recovery_health.protocol_matches,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_durable",
        storage_recovery_health.durable_recovery_observed,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_checkpoint_boundary_present",
        storage_recovery_health.checkpoint_boundary_present,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_wal_replay_bounded",
        storage_recovery_health.wal_replay_bounded,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_replay_boundary_consistent",
        storage_recovery_health.replay_boundary_consistent,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_torn_tail_clean",
        storage_recovery_health.torn_tail_clean,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_blocker_codes",
        storage_recovery_health.blocker_codes,
    );
    insert_json(
        &mut evidence,
        "storage_recovery_blockers",
        storage_recovery_health.blockers,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_required",
        background_maintenance_health.required,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_present",
        background_maintenance_health.present,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_ready",
        background_maintenance_health.ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_protocol_matches",
        background_maintenance_health.protocol_matches,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_total_candidates",
        background_maintenance_health.total_candidates,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_ranked_count",
        background_maintenance_health.ranked_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_executable_search_projection_graph_delta_count",
        background_maintenance_health.executable_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_admitted_search_projection_graph_delta_count",
        background_maintenance_health.admitted_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_deferred_search_projection_graph_delta_count",
        background_maintenance_health.deferred_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_rejected_search_projection_graph_delta_count",
        background_maintenance_health.rejected_search_projection_graph_delta_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_executable_search_projection_graph_delta_operations",
        background_maintenance_health.executable_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        background_maintenance_health.admitted_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        background_maintenance_health
            .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_admission_probe_ready",
        background_maintenance_health.foreground_admission_probe_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_admission_probe_admission",
        background_maintenance_health
            .foreground_admission_probe_admission_name
            .as_deref(),
    );
    insert_json(
        &mut evidence,
        "background_maintenance_memory_pressure_ready",
        background_maintenance_health.memory_pressure_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_memory_budget_bytes",
        background_maintenance_health.memory_budget_bytes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_estimated_memory_bytes",
        background_maintenance_health.estimated_memory_bytes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_ready",
        background_maintenance_health.qos_snapshot_ready,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_foreground_admitted",
        background_maintenance_health.qos_snapshot_foreground_admitted,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_background_bounded",
        background_maintenance_health.qos_snapshot_background_bounded,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_total_background_over_budget",
        background_maintenance_health.qos_snapshot_total_background_over_budget,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_qos_snapshot_blocker_codes",
        background_maintenance_health.qos_snapshot_blocker_codes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_foreground_ranked_count",
        background_maintenance_health.foreground_ranked_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_unknown_admission_count",
        background_maintenance_health.unknown_admission_count,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_blocker_codes",
        background_maintenance_health.blocker_codes,
    );
    insert_json(
        &mut evidence,
        "background_maintenance_blockers",
        background_maintenance_health.blockers,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_family_report_present",
        replacement_family_health.present,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_min_per_million",
        replacement_family_health.min_replacement_readiness_per_million,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_invalid_family_count",
        replacement_family_health.invalid_family_count,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_blocked_query_families",
        replacement_family_health.blocked_query_families,
    );
    insert_json(
        &mut evidence,
        "replacement_readiness_blockers",
        replacement_family_health.blockers,
    );
    insert_json(&mut evidence, "migration_gate_ready", migration_gate_ready);
    insert_json(&mut evidence, "blockers", blockers);
    migration_gate_json_object(bundle)?.insert(
        "cutover_evidence".to_string(),
        serde_json::Value::Object(evidence),
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{
        augment_nowledge_cypher_migration_gate_json,
        scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
        scan_nowledge_query_inventory_cypher_coverage_to_json,
        NowledgeCypherMigrationGateJsonOptions,
    };
    use crate::{ExternalShadowReady, REQUIRED_EXTERNAL_SHADOW_CAPABILITIES};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scanned_cypher_coverage_reports_fixture_matches() {
        let root = unique_test_path("coverage");
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn query() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }
            "#,
        )
        .unwrap();

        let coverage = scan_nowledge_query_inventory_cypher_coverage_to_json(&root).unwrap();

        assert_eq!(coverage["fixture"], "nowledge-memory-core");
        assert_eq!(coverage["required_checks"], 1);
        assert_eq!(coverage["covered_checks"], 1);
        assert_eq!(coverage["missing_checks"].as_array().unwrap().len(), 0);
        assert_eq!(
            coverage["extra_fixture_checks"].as_array().unwrap().len(),
            643
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn coverage_detail_reports_missing_item_metadata() {
        let root = unique_test_path("detail");
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("repo.rs"),
            r#"
                pub fn covered() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title"
                }

                pub fn missing() -> &'static str {
                    "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
                }
            "#,
        )
        .unwrap();

        let detail = scan_nowledge_query_inventory_cypher_coverage_detail_to_json(&root).unwrap();
        let missing_items = detail["missing_items"].as_array().unwrap();
        let covered_items = detail["covered_items"].as_array().unwrap();

        assert_eq!(detail["coverage"]["required_checks"], 2);
        assert_eq!(detail["coverage"]["covered_checks"], 1);
        assert_eq!(covered_items.len(), 1);
        assert_eq!(missing_items.len(), 1);
        assert_eq!(
            missing_items[0]["cypher"],
            "MATCH (m:Memory) WHERE m.id = $id RETURN m.uncovered_property"
        );
        assert_eq!(missing_items[0]["query_family"], "read");
        assert_eq!(
            missing_items[0]["source"],
            "crates/nmem-graph/src/repo.rs:7"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn migration_gate_metadata_uses_compat_owned_cutover_protocol() {
        let mut bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true,
            },
        });
        let options = NowledgeCypherMigrationGateJsonOptions {
            shadow_name: Some("previous-wrapper".to_string()),
            shadow_ready: Some(ExternalShadowReady {
                protocol_version: 1,
                capabilities: REQUIRED_EXTERNAL_SHADOW_CAPABILITIES
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
                engine_kind: Some("previous_wrapper".to_string()),
                wrapper_identity: Some("kuzu-v1".to_string()),
            }),
            ready_preflight: true,
            include_cutover_evidence: true,
            ..NowledgeCypherMigrationGateJsonOptions::default()
        };

        augment_nowledge_cypher_migration_gate_json(&mut bundle, "fallback", options).unwrap();

        assert_eq!(bundle["shadow_run"]["shadow_name"], "previous-wrapper");
        assert_eq!(bundle["shadow_ready"]["engine_kind"], "previous_wrapper");
        assert_eq!(
            bundle["cutover_evidence"]["ready_engine_kind"],
            "previous_wrapper"
        );
        assert_eq!(
            bundle["cutover_evidence"]["requires_previous_wrapper"],
            true
        );
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb-compat-nowledge-inventory-{name}-{nanos}"))
    }
}
