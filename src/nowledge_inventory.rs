#[cfg(test)]
use crate::api::BackgroundMaintenanceSummary;
use crate::api::Database;
use crate::compat::{
    assess_compatibility_cypher_migration_gate_bundle_with_rollback,
    compatibility_migration_gate_bundle_to_json, nowledge_memory_core_fixture,
    run_compatibility_fixture_with_shadow, CompatibilityCutoverPolicy,
    CompatibilityInventoryCoveragePolicy, CompatibilityShadowEngine,
};
use crate::error::Result;
#[cfg(test)]
use crate::qos::{LocalQosClassSnapshot, LocalQosSnapshot};
use crate::qos::{LocalQosPolicy, LocalQosState};
use crate::search::SearchIndex;
use std::path::Path;

pub use skein_evidence::query_inventory::{
    scan_nowledge_query_inventory, scan_nowledge_query_inventory_to_json,
    scan_nowledge_query_inventory_with_options, NowledgeInventoryScanOptions,
};
#[cfg(test)]
const BACKGROUND_MAINTENANCE_ESTIMATED_BYTES_PER_OPERATION: u64 = 1024;

pub use skein_nowledge_contracts::background_maintenance_summary_to_json;

pub use skein_compat::NowledgeCypherMigrationGateJsonOptions;

pub use skein_evidence::inventory::{
    background_maintenance_evidence_health, background_maintenance_evidence_health_from_bundle,
    replacement_readiness_family_evidence_health,
    replacement_readiness_family_evidence_health_from_bundle, storage_recovery_evidence_health,
    storage_recovery_evidence_health_from_bundle, BackgroundMaintenanceEvidenceHealth,
    ReplacementReadinessFamilyEvidenceHealth, StorageRecoveryEvidenceHealth,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};

#[cfg(test)]
mod facade_tests;

pub use skein_compat::{
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
};

pub fn scan_nowledge_query_inventory_cypher_migration_gate_to_json(
    root: impl AsRef<Path>,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<serde_json::Value> {
    scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
        root,
        shadow,
        NowledgeCypherMigrationGateJsonOptions::default(),
    )
}

pub fn scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
    root: impl AsRef<Path>,
    shadow: &mut impl CompatibilityShadowEngine,
    options: NowledgeCypherMigrationGateJsonOptions,
) -> Result<serde_json::Value> {
    let inventory = scan_nowledge_query_inventory(root)?;
    let fixture = nowledge_memory_core_fixture();
    let shadow_engine_name = shadow.name().to_string();
    let mut primary = Database::new();
    let shadow_report = run_compatibility_fixture_with_shadow(&mut primary, &fixture, shadow)?;
    let bundle = assess_compatibility_cypher_migration_gate_bundle_with_rollback(
        &fixture,
        &inventory,
        &shadow_report,
        CompatibilityInventoryCoveragePolicy::default(),
        CompatibilityCutoverPolicy::default(),
        options.rollback.clone(),
    );
    let mut json = compatibility_migration_gate_bundle_to_json(&bundle);
    if let Some(background_maintenance) = options.background_maintenance.as_ref() {
        skein_compat::nowledge_inventory::migration_gate_json_object(&mut json)?.insert(
            "background_maintenance".to_string(),
            background_maintenance.clone(),
        );
    } else {
        insert_background_maintenance_summary_json(&mut json, &primary)?;
    }
    if let Some(replacement_readiness) = options.replacement_readiness_by_query_family.as_ref() {
        skein_compat::nowledge_inventory::migration_gate_json_object(&mut json)?.insert(
            "replacement_readiness_by_query_family".to_string(),
            replacement_readiness.clone(),
        );
    }
    skein_compat::nowledge_inventory::augment_nowledge_cypher_migration_gate_json(
        &mut json,
        &shadow_engine_name,
        options,
    )?;
    Ok(json)
}

#[cfg(test)]
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

fn insert_background_maintenance_summary_json(
    bundle: &mut serde_json::Value,
    database: &Database,
) -> Result<()> {
    let search_index = SearchIndex::in_memory();
    let summary = database.background_maintenance_summary(
        Some(&search_index),
        &LocalQosPolicy::default(),
        &LocalQosState::default(),
        Default::default(),
    );
    skein_compat::nowledge_inventory::migration_gate_json_object(bundle)?.insert(
        "background_maintenance".to_string(),
        background_maintenance_summary_to_json(&summary),
    );
    Ok(())
}

#[cfg(test)]
mod summary_json_oracle {
    use super::*;

    pub fn background_maintenance_summary_to_json(
        summary: &BackgroundMaintenanceSummary,
    ) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        insert_json(&mut object, "total_candidates", summary.total_candidates);
        insert_json(&mut object, "admitted_count", summary.admitted_count);
        insert_json(&mut object, "deferred_count", summary.deferred_count);
        insert_json(&mut object, "rejected_count", summary.rejected_count);
        insert_json(
            &mut object,
            "total_estimated_operations",
            summary.total_estimated_operations,
        );
        insert_json(
            &mut object,
            "admitted_estimated_operations",
            summary.admitted_estimated_operations,
        );
        insert_json(
            &mut object,
            "deferred_estimated_operations",
            summary.deferred_estimated_operations,
        );
        insert_json(
            &mut object,
            "rejected_estimated_operations",
            summary.rejected_estimated_operations,
        );
        insert_json(
            &mut object,
            "executable_search_projection_graph_delta_count",
            summary.executable_search_projection_graph_delta_count,
        );
        insert_json(
            &mut object,
            "admitted_search_projection_graph_delta_count",
            summary.admitted_search_projection_graph_delta_count,
        );
        insert_json(
            &mut object,
            "deferred_search_projection_graph_delta_count",
            summary.deferred_search_projection_graph_delta_count,
        );
        insert_json(
            &mut object,
            "rejected_search_projection_graph_delta_count",
            summary.rejected_search_projection_graph_delta_count,
        );
        insert_json(
            &mut object,
            "executable_search_projection_graph_delta_operations",
            summary.executable_search_projection_graph_delta_operations,
        );
        insert_json(
            &mut object,
            "admitted_search_projection_graph_delta_operations",
            summary.admitted_search_projection_graph_delta_operations,
        );
        insert_json(
            &mut object,
            "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        );
        insert_json(
            &mut object,
            "foreground_admission_probe_ready",
            summary.foreground_admission_probe_ready,
        );
        insert_json(
            &mut object,
            "foreground_admission_probe_admission",
            summary.foreground_admission_probe_admission_name.as_deref(),
        );
        if let Some(qos_snapshot) = summary.qos_snapshot.as_ref() {
            insert_json(
                &mut object,
                "qos_snapshot",
                background_maintenance_qos_snapshot_to_json(qos_snapshot),
            );
            insert_json(&mut object, "qos_snapshot_ready", qos_snapshot.ready);
            insert_json(
                &mut object,
                "qos_snapshot_foreground_admitted",
                qos_snapshot.foreground_admitted,
            );
            insert_json(
                &mut object,
                "qos_snapshot_background_enabled",
                qos_snapshot.background_enabled,
            );
            insert_json(
                &mut object,
                "qos_snapshot_background_bounded",
                qos_snapshot.background_bounded,
            );
            insert_json(
                &mut object,
                "qos_snapshot_running_background_operations",
                qos_snapshot.running_background_operations,
            );
            insert_json(
                &mut object,
                "qos_snapshot_max_total_background_operations",
                qos_snapshot.max_total_background_operations,
            );
            insert_json(
                &mut object,
                "qos_snapshot_remaining_total_background_operations",
                qos_snapshot.remaining_total_background_operations,
            );
            insert_json(
                &mut object,
                "qos_snapshot_total_background_over_budget",
                qos_snapshot.total_background_over_budget,
            );
            insert_json(
                &mut object,
                "qos_snapshot_blocker_codes",
                qos_snapshot
                    .blocker_codes
                    .iter()
                    .map(|code| code.as_str())
                    .collect::<Vec<_>>(),
            );
            insert_json(
                &mut object,
                "memory_pressure",
                background_maintenance_memory_pressure_to_json(summary, qos_snapshot),
            );
        }
        insert_json(
            &mut object,
            "top_admitted_kind",
            summary.top_admitted_kind.map(|kind| kind.as_str()),
        );
        insert_json(
            &mut object,
            "top_admitted_name",
            summary.top_admitted_name.as_deref(),
        );
        insert_json(
            &mut object,
            "ranked",
            summary
                .ranked
                .iter()
                .map(background_maintenance_summary_item_to_json)
                .collect::<Vec<_>>(),
        );
        serde_json::Value::Object(object)
    }

    fn background_maintenance_memory_pressure_to_json(
        summary: &BackgroundMaintenanceSummary,
        qos_snapshot: &LocalQosSnapshot,
    ) -> serde_json::Value {
        let estimated_memory_bytes =
            estimate_background_maintenance_memory_bytes(summary.total_estimated_operations);
        let memory_budget_bytes = qos_snapshot
            .remaining_total_background_operations
            .or(qos_snapshot.max_total_background_operations)
            .map(estimate_background_maintenance_memory_bytes)
            .unwrap_or(0);
        serde_json::json!({
            "ready": !qos_snapshot.total_background_over_budget
                && estimated_memory_bytes <= memory_budget_bytes,
            "budget_bytes": memory_budget_bytes,
            "estimated_bytes": estimated_memory_bytes,
        })
    }

    fn estimate_background_maintenance_memory_bytes(operations: usize) -> u64 {
        u64::try_from(operations)
            .unwrap_or(u64::MAX)
            .saturating_mul(BACKGROUND_MAINTENANCE_ESTIMATED_BYTES_PER_OPERATION)
    }

    fn background_maintenance_qos_snapshot_to_json(
        snapshot: &LocalQosSnapshot,
    ) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        insert_json(&mut object, "ready", snapshot.ready);
        insert_json(
            &mut object,
            "foreground_admitted",
            snapshot.foreground_admitted,
        );
        insert_json(
            &mut object,
            "background_enabled",
            snapshot.background_enabled,
        );
        insert_json(
            &mut object,
            "background_bounded",
            snapshot.background_bounded,
        );
        insert_json(
            &mut object,
            "running_background_operations",
            snapshot.running_background_operations,
        );
        insert_json(
            &mut object,
            "max_total_background_operations",
            snapshot.max_total_background_operations,
        );
        insert_json(
            &mut object,
            "remaining_total_background_operations",
            snapshot.remaining_total_background_operations,
        );
        insert_json(
            &mut object,
            "total_background_over_budget",
            snapshot.total_background_over_budget,
        );
        insert_json(
            &mut object,
            "blocker_codes",
            snapshot
                .blocker_codes
                .iter()
                .map(|code| code.as_str())
                .collect::<Vec<_>>(),
        );
        insert_json(
            &mut object,
            "classes",
            snapshot
                .class_snapshots
                .iter()
                .map(background_maintenance_qos_class_snapshot_to_json)
                .collect::<Vec<_>>(),
        );
        serde_json::Value::Object(object)
    }

    fn background_maintenance_qos_class_snapshot_to_json(
        snapshot: &LocalQosClassSnapshot,
    ) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        insert_json(&mut object, "class", snapshot.class.as_str());
        insert_json(
            &mut object,
            "running_background_operations",
            snapshot.running_background_operations,
        );
        insert_json(
            &mut object,
            "max_background_operations",
            snapshot.max_background_operations,
        );
        insert_json(
            &mut object,
            "remaining_background_operations",
            snapshot.remaining_background_operations,
        );
        insert_json(&mut object, "over_budget", snapshot.over_budget);
        serde_json::Value::Object(object)
    }

    fn background_maintenance_summary_item_to_json(
        item: &crate::api::BackgroundMaintenanceSummaryItem,
    ) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        insert_json(&mut object, "kind", item.kind.as_str());
        insert_json(&mut object, "name", &item.name);
        insert_json(&mut object, "work_class", &item.work_class_name);
        insert_json(&mut object, "priority", &item.priority_name);
        insert_json(
            &mut object,
            "estimated_operations",
            item.estimated_operations,
        );
        insert_json(&mut object, "hint_active_topic", item.hint_active_topic);
        insert_json(
            &mut object,
            "hint_recent_delta_operations",
            item.hint_recent_delta_operations,
        );
        insert_json(
            &mut object,
            "hint_source_graph_commit_lag",
            item.hint_source_graph_commit_lag,
        );
        insert_json(
            &mut object,
            "hint_query_probability_per_million",
            item.hint_query_probability_per_million,
        );
        insert_json(
            &mut object,
            "hint_staleness_millis",
            item.hint_staleness_millis,
        );
        insert_json(
            &mut object,
            "hint_staleness_ttl_millis",
            item.hint_staleness_ttl_millis,
        );
        insert_json(
            &mut object,
            "hint_freshness_slo_millis",
            item.hint_freshness_slo_millis,
        );
        insert_json(
            &mut object,
            "hint_tenant_budget_remaining_operations",
            item.hint_tenant_budget_remaining_operations,
        );
        insert_json(&mut object, "admission", &item.admission_name);
        insert_json(&mut object, "admission_code", &item.admission_code_name);
        insert_json(&mut object, "score", item.score);
        insert_json(&mut object, "reason_codes", &item.reason_code_names);
        insert_json(&mut object, "reasons", &item.reasons);
        insert_json(
            &mut object,
            "has_executable_search_projection_graph_delta",
            item.has_executable_search_projection_graph_delta,
        );
        insert_json(
            &mut object,
            "search_projection_graph_delta_operation_count",
            item.search_projection_graph_delta_operation_count,
        );
        insert_json(
            &mut object,
            "search_projection_graph_delta_upsert_node_count",
            item.search_projection_graph_delta_upsert_node_count,
        );
        insert_json(
            &mut object,
            "search_projection_graph_delta_delete_document_count",
            item.search_projection_graph_delta_delete_document_count,
        );
        insert_json(
            &mut object,
            "search_projection_graph_delta_complete_through_graph_commit_epoch",
            item.search_projection_graph_delta_complete_through_graph_commit_epoch,
        );
        insert_json(
            &mut object,
            "search_projection_graph_delta_max_operations",
            item.search_projection_graph_delta_max_operations,
        );
        serde_json::Value::Object(object)
    }
}

#[cfg(test)]
mod summary_json_facade_tests {
    use super::{
        background_maintenance_summary_to_json, summary_json_oracle, BackgroundMaintenanceSummary,
    };

    #[test]
    fn facade_matches_the_pre_migration_summary_json_oracle() {
        let summary = BackgroundMaintenanceSummary {
            total_candidates: 3,
            total_estimated_operations: 7,
            ..Default::default()
        };

        assert_eq!(
            background_maintenance_summary_to_json(&summary),
            summary_json_oracle::background_maintenance_summary_to_json(&summary),
        );
    }

    #[test]
    fn facade_matches_the_pre_migration_sampled_summary_json_oracle() {
        let database = crate::Database::new();
        let search_index = crate::SearchIndex::in_memory();
        let summary = database.background_maintenance_summary(
            Some(&search_index),
            &crate::LocalQosPolicy::default(),
            &crate::LocalQosState::default(),
            Default::default(),
        );

        assert!(summary.qos_snapshot.is_some());
        assert_eq!(
            background_maintenance_summary_to_json(&summary),
            summary_json_oracle::background_maintenance_summary_to_json(&summary),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        scan_nowledge_query_inventory, scan_nowledge_query_inventory_cypher_migration_gate_to_json,
        scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json,
        NowledgeCypherMigrationGateJsonOptions,
    };
    use crate::compat::{
        CompatibilityRollbackEvidence, CompatibilityShadowEngine, ExternalShadowReady,
        ProjectedGraphFixtureCheck, ProjectedGraphShadowOutput,
    };
    use crate::{Database, QueryOutput, Result};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scanned_cypher_migration_gate_reports_ready_bundle() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle =
            scan_nowledge_query_inventory_cypher_migration_gate_to_json(&root, &mut shadow)
                .unwrap();

        assert_eq!(bundle["coverage"]["required_checks"], 1);
        assert_eq!(bundle["coverage"]["covered_checks"], 1);
        assert_eq!(bundle["inventory_gate"]["decision"], "ready");
        assert_eq!(bundle["cutover"]["decision"], "ready");
        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["migration_gate"]["shadow_decision"], "ready");
        assert_eq!(
            bundle["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap(),
            bundle["background_maintenance"]["ranked"]
                .as_array()
                .unwrap()
                .len() as u64
        );
        assert!(
            bundle["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(bundle["background_maintenance"]["ranked"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["work_class"] == "projection"
                && item["priority"] == "background"
                && item["admission"] == "admit"));
        let executable_delta = bundle["background_maintenance"]["ranked"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["has_executable_search_projection_graph_delta"] == true)
            .unwrap();
        assert!(
            executable_delta["search_projection_graph_delta_operation_count"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            executable_delta["hint_recent_delta_operations"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            executable_delta["hint_source_graph_commit_lag"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(executable_delta["hint_active_topic"], false);
        assert_eq!(
            executable_delta["hint_query_probability_per_million"]
                .as_u64()
                .unwrap(),
            0
        );
        assert_eq!(
            executable_delta["hint_staleness_millis"].as_u64().unwrap(),
            0
        );
        assert!(executable_delta["hint_staleness_ttl_millis"].is_null());
        assert!(executable_delta["hint_freshness_slo_millis"].is_null());
        assert!(executable_delta["hint_tenant_budget_remaining_operations"].is_null());
        assert!(
            executable_delta["search_projection_graph_delta_upsert_node_count"]
                .as_u64()
                .is_some()
        );
        assert!(
            executable_delta["search_projection_graph_delta_delete_document_count"]
                .as_u64()
                .is_some()
        );
        assert!(executable_delta
            ["search_projection_graph_delta_complete_through_graph_commit_epoch"]
            .as_u64()
            .is_some());
        assert!(
            bundle["background_maintenance"]["executable_search_projection_graph_delta_count"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            bundle["background_maintenance"]["admitted_search_projection_graph_delta_count"]
                .as_u64()
                .is_some()
        );
        assert!(
            bundle["background_maintenance"]["deferred_search_projection_graph_delta_count"]
                .as_u64()
                .is_some()
        );
        assert!(
            bundle["background_maintenance"]["rejected_search_projection_graph_delta_count"]
                .as_u64()
                .is_some()
        );
        assert!(
            bundle["background_maintenance"]["executable_search_projection_graph_delta_operations"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(bundle["background_maintenance"]
            ["admitted_search_projection_graph_delta_operations"]
            .as_u64()
            .is_some());
        assert!(bundle["background_maintenance"]
            ["max_search_projection_graph_delta_complete_through_graph_commit_epoch"]
            .as_u64()
            .is_some());
        assert_eq!(
            bundle["background_maintenance"]["qos_snapshot"]["ready"],
            true
        );
        assert_eq!(
            bundle["background_maintenance"]["qos_snapshot"]["background_bounded"],
            true
        );
        assert_eq!(
            bundle["background_maintenance"]["qos_snapshot"]["blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(
            bundle["migration_gate"]["blockers"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(bundle.get("shadow_run").is_none());
        assert!(bundle.get("cutover_evidence").is_none());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_can_include_shadow_wiring_metadata() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-metadata-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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
        let trace_path = root.join("shadow.jsonl");
        fs::write(
            &trace_path,
            r#"{"sequence":1,"event":"request","payload":{"op":"ready"}}"#.to_string()
                + "\n"
                + r#"{"sequence":1,"event":"response","payload":{"ready":true}}"#
                + "\n"
                + r#"{"sequence":2,"event":"request","payload":{"op":"execute_session"}}"#
                + "\n"
                + r#"{"sequence":2,"event":"response","payload":{"ok":{"rows":[]}}}"#
                + "\n",
        )
        .unwrap();

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_name: Some("previous-wrapper".to_string()),
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec![
                        "execute".to_string(),
                        "execute_session".to_string(),
                        "project_graph".to_string(),
                    ],
                    engine_kind: Some("previous_wrapper".to_string()),
                    wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
                }),
                shadow_trace_path: Some(trace_path.to_string_lossy().into_owned()),
                shadow_request_count: Some(2),
                include_cutover_evidence: true,
                storage_recovery_required: true,
                storage_recovery: Some(serde_json::json!({
                    "protocol": "skein-storage-recovery-report",
                    "storage_version": "skein-storage-v1",
                    "durable": true,
                    "checkpoint_epoch": 7,
                    "checkpoint_commit_epoch": 7,
                    "wal_replay_start_lsn": 8,
                    "next_lsn_after_replay": 9,
                    "replayed_wal_entries": 1,
                    "max_wal_replay_entries": 1024,
                    "recovered_commit_epoch": 8,
                    "torn_tail_ignored": false,
                    "torn_tail_reason": null,
                    "readiness": {
                        "durable_recovery_observed": true,
                        "checkpoint_boundary_present": true,
                        "wal_replay_bounded": true,
                        "torn_tail_clean": true
                    }
                })),
                background_maintenance_required: true,
                background_maintenance: Some(serde_json::json!({
                    "protocol": "skein-background-maintenance-report",
                    "total_candidates": 1,
                    "admitted_count": 1,
                    "deferred_count": 0,
                    "rejected_count": 0,
                    "foreground_admission_probe_ready": true,
                    "foreground_admission_probe_admission": "admit",
                    "qos_snapshot": ready_qos_snapshot(),
                    "slow_query": ready_slow_query(),
                    "memory_pressure": ready_memory_pressure(),
                    "ranked": [
                        {
                            "kind": "search_projection_graph_delta",
                            "work_class": "projection",
                            "priority": "background",
                            "admission": "admit",
                            "has_executable_search_projection_graph_delta": true,
                            "search_projection_graph_delta_operation_count": 1,
                            "search_projection_graph_delta_complete_through_graph_commit_epoch": 1
                        }
                    ]
                })),
                replacement_readiness_by_query_family: Some(
                    ready_replacement_readiness_by_query_family(),
                ),
                previous_wrapper_contract_evidence: Some(serde_json::json!({
                    "ready": true,
                    "evidence_kind": "previous_wrapper_contract",
                    "wrapper_identity": "nowledge-previous-wrapper:test",
                    "requires_full_contract_ready": true,
                    "requires_wrapper_identity": true,
                    "blocker_codes": [],
                    "blockers": []
                })),
                rollback: CompatibilityRollbackEvidence {
                    required: true,
                    ready: true,
                    evidence: Some("previous wrapper reopen smoke passed".to_string()),
                    blockers: Vec::new(),
                },
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["shadow_run"]["shadow_name"], "previous-wrapper");
        assert_eq!(bundle["shadow_run"]["self_shadow"], false);
        assert_eq!(bundle["shadow_run"]["evidence_kind"], "previous_wrapper");
        assert_eq!(
            bundle["shadow_ready"]["protocol_version"],
            crate::EXTERNAL_SHADOW_PROTOCOL_VERSION
        );
        assert_eq!(
            bundle["cutover_evidence"]["requires_ready_engine_kind"],
            "previous_wrapper"
        );
        assert_eq!(
            bundle["cutover_evidence"]["ready_engine_kind"],
            "previous_wrapper"
        );
        assert_eq!(bundle["shadow_trace"]["request_count"], 2);
        assert_eq!(bundle["shadow_trace"]["path"], "<redacted>");
        assert_eq!(bundle["shadow_trace"]["path_redacted"], true);
        assert_eq!(bundle["shadow_trace"]["summary_available"], true);
        assert_eq!(bundle["shadow_trace"]["request_op_counts"]["ready"], 1);
        assert_eq!(
            bundle["shadow_trace"]["request_op_counts"]["execute_session"],
            1
        );
        assert_eq!(bundle["shadow_trace"]["response_op_counts"]["ready"], 1);
        assert_eq!(
            bundle["shadow_trace"]["response_op_counts"]["execute_session"],
            1
        );
        assert_eq!(bundle["previous_wrapper_contract_evidence"]["ready"], true);
        assert_eq!(
            bundle["previous_wrapper_contract_evidence"]["wrapper_identity"],
            "nowledge-previous-wrapper:test"
        );
        assert_eq!(bundle["cutover_evidence"]["eligible"], true);
        assert_eq!(
            bundle["cutover_evidence"]["ready_missing_capabilities"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_present"], true);
        assert_eq!(bundle["cutover_evidence"]["shadow_trace_complete"], true);
        assert_eq!(
            bundle["cutover_evidence"]["shadow_trace_summary_available"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["shadow_trace_request_count_matches"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["shadow_trace_pending_request_count"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_required"],
            true
        );
        assert_eq!(bundle["cutover_evidence"]["storage_recovery_present"], true);
        assert_eq!(bundle["cutover_evidence"]["storage_recovery_ready"], true);
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_wal_replay_bounded"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_required"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_present"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_ready"],
            true
        );
        assert!(
            bundle["cutover_evidence"]["background_maintenance_total_candidates"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            bundle["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_count"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(bundle["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_count"]
            .as_u64()
            .is_some());
        assert!(bundle["cutover_evidence"]
            ["background_maintenance_deferred_search_projection_graph_delta_count"]
            .as_u64()
            .is_some());
        assert!(bundle["cutover_evidence"]
            ["background_maintenance_rejected_search_projection_graph_delta_count"]
            .as_u64()
            .is_some());
        assert!(
            bundle["cutover_evidence"]
                ["background_maintenance_executable_search_projection_graph_delta_operations"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(bundle["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_operations"]
            .as_u64()
            .is_some());
        assert!(bundle["cutover_evidence"]
            ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"]
            .as_u64()
            .is_some());
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_foreground_ranked_count"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_unknown_admission_count"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_family_report_present"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_min_per_million"],
            1_000_000
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_invalid_family_count"],
            0
        );
        assert_eq!(
            bundle["cutover_evidence"]["replacement_readiness_blocked_query_families"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(bundle["cutover_evidence"]["ready_preflight"], true);
        assert_eq!(bundle["cutover_evidence"]["shadow_evidence_present"], true);
        assert_eq!(bundle["migration_gate"]["rollback_required"], true);
        assert_eq!(bundle["migration_gate"]["rollback_ready"], true);
        assert_eq!(
            bundle["migration_gate"]["rollback_evidence"],
            "previous wrapper reopen smoke passed"
        );
        assert!(
            bundle["background_maintenance"]["admitted_count"]
                .as_u64()
                .unwrap()
                > 0
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_requires_previous_wrapper_ready_engine_kind() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-engine-kind-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_name: Some("shadow-without-engine-kind".to_string()),
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec![
                        "execute".to_string(),
                        "execute_session".to_string(),
                        "project_graph".to_string(),
                    ],
                    engine_kind: None,
                    wrapper_identity: None,
                }),
                include_cutover_evidence: true,
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert_eq!(
            bundle["cutover_evidence"]["requires_ready_engine_kind"],
            "previous_wrapper"
        );
        assert!(bundle["cutover_evidence"]["ready_engine_kind"].is_null());
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready response missing engine_kind"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_requires_previous_wrapper_identity() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-wrapper-identity-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_name: Some("shadow-without-wrapper-identity".to_string()),
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec![
                        "execute".to_string(),
                        "execute_session".to_string(),
                        "project_graph".to_string(),
                    ],
                    engine_kind: Some("previous_wrapper".to_string()),
                    wrapper_identity: None,
                }),
                include_cutover_evidence: true,
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["migration_gate"]["decision"], "ready");
        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert_eq!(
            bundle["cutover_evidence"]["requires_ready_wrapper_identity"],
            true
        );
        assert!(bundle["cutover_evidence"]["ready_wrapper_identity"].is_null());
        assert_eq!(
            bundle["cutover_evidence"]["blockers"][0],
            "shadow ready response missing wrapper_identity"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_can_use_caller_background_maintenance_report() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-background-report-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_name: Some("previous-wrapper".to_string()),
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec![
                        "execute".to_string(),
                        "execute_session".to_string(),
                        "project_graph".to_string(),
                    ],
                    engine_kind: Some("previous_wrapper".to_string()),
                    wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
                }),
                include_cutover_evidence: true,
                background_maintenance_required: true,
                background_maintenance: Some(serde_json::json!({
                    "protocol": "skein-background-maintenance-report",
                    "total_candidates": 0,
                    "foreground_admission_probe_ready": true,
                    "foreground_admission_probe_admission": "admit",
                    "qos_snapshot": ready_qos_snapshot(),
                    "slow_query": ready_slow_query(),
                    "memory_pressure": ready_memory_pressure(),
                    "ranked": []
                })),
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(
            bundle["background_maintenance"]["protocol"],
            "skein-background-maintenance-report"
        );
        assert_eq!(bundle["background_maintenance"]["total_candidates"], 0);
        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_present"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_protocol_matches"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["background_maintenance_blocker_codes"],
            serde_json::json!(["no_candidates", "no_ranked_work"])
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn ready_replacement_readiness_by_query_family() -> serde_json::Value {
        serde_json::json!([
            {
                "query_family": "memory_lookup",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "graph_traversal",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "projected_graph",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "label_stats_read",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "search_projection",
                "replacement_readiness_per_million": 1_000_000
            }
        ])
    }

    fn ready_qos_snapshot() -> serde_json::Value {
        serde_json::json!({
            "ready": true,
            "foreground_admitted": true,
            "background_enabled": true,
            "background_bounded": true,
            "running_background_operations": 0,
            "max_total_background_operations": 4096,
            "remaining_total_background_operations": 4096,
            "total_background_over_budget": false,
            "blocker_codes": []
        })
    }

    fn ready_slow_query() -> serde_json::Value {
        serde_json::json!({
            "ready": true,
            "capacity": 8,
            "record_count": 1,
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            }
        })
    }

    fn ready_memory_pressure() -> serde_json::Value {
        serde_json::json!({
            "ready": true,
            "budget_bytes": 4096,
            "estimated_bytes": 1024
        })
    }

    #[test]
    fn scanned_cypher_migration_gate_blocks_when_required_rollback_evidence_is_missing() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-rollback-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                rollback: CompatibilityRollbackEvidence {
                    required: true,
                    ready: false,
                    evidence: None,
                    blockers: Vec::new(),
                },
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["migration_gate"]["decision"], "blocked");
        assert_eq!(bundle["migration_gate"]["rollback_required"], true);
        assert_eq!(bundle["migration_gate"]["rollback_ready"], false);
        assert_eq!(bundle["migration_gate"]["rollback_blockers"], 1);
        assert_eq!(
            bundle["migration_gate"]["rollback_blocker_messages"][0],
            "previous database reopen evidence is required before cutover"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scanned_cypher_migration_gate_blocks_when_required_storage_recovery_is_missing() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-migration-gate-storage-recovery-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
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

        let mut shadow = TestShadowEngine::default();
        let bundle = scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json(
            &root,
            &mut shadow,
            NowledgeCypherMigrationGateJsonOptions {
                shadow_ready: Some(ExternalShadowReady {
                    protocol_version: crate::EXTERNAL_SHADOW_PROTOCOL_VERSION,
                    capabilities: vec![
                        "execute".to_string(),
                        "execute_session".to_string(),
                        "project_graph".to_string(),
                    ],
                    engine_kind: Some("previous_wrapper".to_string()),
                    wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
                }),
                include_cutover_evidence: true,
                storage_recovery_required: true,
                ..NowledgeCypherMigrationGateJsonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(bundle["cutover_evidence"]["eligible"], false);
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_required"],
            true
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_present"],
            false
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_blockers"][0],
            "storage recovery evidence is required before cutover"
        );
        assert_eq!(
            bundle["cutover_evidence"]["storage_recovery_blocker_codes"][0],
            "missing_evidence"
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[derive(Default)]
    struct TestShadowEngine {
        db: Database,
    }

    impl CompatibilityShadowEngine for TestShadowEngine {
        fn name(&self) -> &str {
            "test-shadow"
        }

        fn execute(
            &mut self,
            statement: &crate::compat::CypherFixtureStatement,
        ) -> Result<QueryOutput> {
            self.db
                .query_with_params(&statement.cypher, &statement.parameters)
        }

        fn execute_session(
            &mut self,
            statements: &[crate::compat::CypherFixtureStatement],
        ) -> Result<Vec<QueryOutput>> {
            let mut session = self.db.session();
            statements
                .iter()
                .map(|statement| {
                    session.query_with_params(&statement.cypher, &statement.parameters)
                })
                .collect()
        }

        fn project_graph(
            &mut self,
            check: &ProjectedGraphFixtureCheck,
        ) -> Result<Option<ProjectedGraphShadowOutput>> {
            let graph = self.db.project_graph(check.rel_type.as_deref());
            let page_rank_scores = graph
                .page_rank(Default::default())
                .into_iter()
                .map(|score| (score.node.0, score.score))
                .collect::<Vec<_>>();
            Ok(Some(ProjectedGraphShadowOutput {
                node_count: graph.node_count(),
                edge_count: graph.edge_count(),
                incoming: check
                    .expected_incoming
                    .iter()
                    .map(|(node, _)| {
                        let sources = graph
                            .incoming_sources(crate::store::NodeId(*node))
                            .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                            .unwrap_or_default();
                        (*node, sources)
                    })
                    .collect(),
                communities: if check.expected_communities.is_empty() {
                    Vec::new()
                } else {
                    graph
                        .louvain_communities(Default::default())
                        .into_iter()
                        .map(|assignment| (assignment.node.0, assignment.community.0))
                        .collect()
                },
                hierarchical_communities: if check.expected_hierarchical_communities.is_empty() {
                    Vec::new()
                } else {
                    graph
                        .hierarchical_louvain_communities(Default::default())
                        .into_iter()
                        .map(|assignment| {
                            (assignment.level, assignment.node.0, assignment.community.0)
                        })
                        .collect()
                },
                page_rank_top_node: page_rank_scores.first().map(|(node, _)| *node),
                page_rank_scores,
            }))
        }
    }

    #[test]
    fn scanned_inventory_skips_cfg_test_module_literals() {
        let root = std::env::temp_dir().join(format!(
            "skein-nowledge-inventory-cfg-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let source_dir = root.join("crates/nmem-graph/src");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(
            source_dir.join("client.rs"),
            r#"
                pub fn production() -> &'static str {
                    "MATCH (m:Memory) RETURN m.id"
                }

                #[cfg(test)]
                mod tests {
                    #[test]
                    fn ignored() {
                        let query = "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id));";
                    }
                }
            "#,
        )
        .unwrap();

        let inventory = scan_nowledge_query_inventory(&root).unwrap();

        assert_eq!(inventory.required_checks.len(), 1);
        assert_eq!(
            inventory.required_checks[0].cypher.as_deref(),
            Some("MATCH (m:Memory) RETURN m.id")
        );

        fs::remove_dir_all(root).unwrap();
    }
}
