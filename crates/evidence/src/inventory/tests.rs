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

#[test]
fn background_maintenance_evidence_health_rejects_protocol_mismatch() {
    let summary = serde_json::json!({
        "protocol": "unexpected-background-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "qos_snapshot": ready_qos_snapshot(),
        "slow_query": ready_slow_query(),
        "memory_pressure": ready_memory_pressure(),
        "ranked": [
            {
                "kind": "schema_maintenance",
                "work_class": "mutation",
                "priority": "background",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.protocol_matches, Some(false));
    assert_eq!(health.blocker_codes, vec!["protocol_mismatch".to_string()]);
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence protocol mismatch".to_string()]
    );
}

#[test]
fn replacement_readiness_family_evidence_health_blocks_incomplete_families() {
    let families = serde_json::json!([
        {
            "query_family": "mutation",
            "replacement_readiness_per_million": 500_000
        },
        {
            "query_family": "read",
            "replacement_readiness_per_million": 1_000_000
        }
    ]);

    let health = super::replacement_readiness_family_evidence_health(Some(&families));

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.min_replacement_readiness_per_million, Some(500_000));
    assert_eq!(health.invalid_family_count, 0);
    assert_eq!(health.blocked_query_families, vec!["mutation".to_string()]);
    assert_eq!(
        health.missing_required_query_families,
        vec![
            "memory_lookup".to_string(),
            "graph_traversal".to_string(),
            "projected_graph".to_string(),
            "label_stats_read".to_string(),
            "search_projection".to_string()
        ]
    );
    assert_eq!(
        health.blockers,
        vec![
            "replacement readiness is incomplete for query families: mutation".to_string(),
            "replacement readiness is missing required query families: memory_lookup, graph_traversal, projected_graph, label_stats_read, search_projection".to_string()
        ]
    );
}

#[test]
fn replacement_readiness_family_evidence_health_is_optional_for_legacy_bundles() {
    let health = super::replacement_readiness_family_evidence_health(None);

    assert!(!health.present);
    assert!(health.ready);
    assert_eq!(health.min_replacement_readiness_per_million, None);
    assert_eq!(health.invalid_family_count, 0);
    assert!(health.blocked_query_families.is_empty());
    assert!(health.missing_required_query_families.is_empty());
    assert!(health.blockers.is_empty());
}

#[test]
fn replacement_readiness_family_evidence_health_rejects_invalid_entries() {
    let families = serde_json::json!([
        {
            "query_family": "read"
        }
    ]);

    let health = super::replacement_readiness_family_evidence_health(Some(&families));

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.invalid_family_count, 1);
    assert_eq!(
        health.missing_required_query_families,
        vec![
            "memory_lookup".to_string(),
            "graph_traversal".to_string(),
            "projected_graph".to_string(),
            "label_stats_read".to_string(),
            "search_projection".to_string()
        ]
    );
    assert_eq!(
        health.blockers,
        vec![
            "replacement readiness family report has invalid entries".to_string(),
            "replacement readiness is missing required query families: memory_lookup, graph_traversal, projected_graph, label_stats_read, search_projection".to_string()
        ]
    );
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
fn replacement_readiness_family_evidence_health_requires_nowledge_families() {
    let families = ready_replacement_readiness_by_query_family();

    let health = super::replacement_readiness_family_evidence_health(Some(&families));

    assert!(health.present);
    assert!(health.ready);
    assert!(health.blocked_query_families.is_empty());
    assert!(health.missing_required_query_families.is_empty());
    assert!(health.blockers.is_empty());
}

#[test]
fn background_maintenance_evidence_health_requires_present_summary() {
    let health = super::background_maintenance_evidence_health(None, true);

    assert!(health.required);
    assert!(!health.present);
    assert!(!health.ready);
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence is required before cutover".to_string()]
    );
    assert_eq!(health.blocker_codes, vec!["missing_evidence".to_string()]);
}

#[test]
fn background_maintenance_evidence_health_rejects_foreground_ranked_work() {
    let summary = serde_json::json!({
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "qos_snapshot": ready_qos_snapshot(),
        "slow_query": ready_slow_query(),
        "memory_pressure": ready_memory_pressure(),
        "ranked": [
            {
                "kind": "schema_maintenance",
                "work_class": "mutation",
                "priority": "foreground",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.total_candidates, Some(1));
    assert_eq!(health.ranked_count, Some(1));
    assert_eq!(health.foreground_ranked_count, 1);
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence ranked foreground work".to_string()]
    );
    assert_eq!(
        health.blocker_codes,
        vec!["foreground_ranked_work".to_string()]
    );
}

#[test]
fn background_maintenance_evidence_health_rejects_memory_pressure() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "qos_snapshot": ready_qos_snapshot(),
        "slow_query": ready_slow_query(),
        "memory_pressure": {
            "ready": true,
            "budget_bytes": 4096,
            "estimated_bytes": 8192
        },
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "defer"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.memory_pressure_ready, Some(false));
    assert_eq!(health.memory_budget_bytes, Some(4096));
    assert_eq!(health.estimated_memory_bytes, Some(8192));
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence exceeds configured memory budget".to_string()]
    );
    assert_eq!(
        health.blocker_codes,
        vec!["memory_budget_exceeded".to_string()]
    );
}

#[test]
fn background_maintenance_evidence_health_requires_memory_pressure() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "qos_snapshot": ready_qos_snapshot(),
        "slow_query": ready_slow_query(),
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "defer"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.memory_pressure_ready, None);
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence lacks memory-pressure budget".to_string()]
    );
    assert_eq!(
        health.blocker_codes,
        vec!["memory_pressure_missing".to_string()]
    );
}

#[test]
fn background_maintenance_evidence_health_requires_qos_snapshot() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "memory_pressure": ready_memory_pressure(),
        "slow_query": ready_slow_query(),
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.qos_snapshot_ready, None);
    assert_eq!(
        health.blocker_codes,
        vec!["qos_snapshot_missing".to_string()]
    );
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence lacks a local QoS snapshot".to_string()]
    );
}

#[test]
fn background_maintenance_evidence_health_rejects_unbounded_qos_snapshot() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "memory_pressure": ready_memory_pressure(),
        "qos_snapshot": {
            "ready": false,
            "foreground_admitted": true,
            "background_enabled": true,
            "background_bounded": false,
            "running_background_operations": 0,
            "max_total_background_operations": null,
            "remaining_total_background_operations": null,
            "total_background_over_budget": false,
            "blocker_codes": ["background_unbounded"]
        },
        "slow_query": ready_slow_query(),
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.qos_snapshot_ready, Some(false));
    assert_eq!(health.qos_snapshot_background_bounded, Some(false));
    assert_eq!(
        health.qos_snapshot_blocker_codes,
        vec!["background_unbounded".to_string()]
    );
    assert_eq!(
        health.blocker_codes,
        vec![
            "qos_snapshot_not_ready".to_string(),
            "qos_snapshot_background_unbounded".to_string(),
            "background_unbounded".to_string()
        ]
    );
}

#[test]
fn background_maintenance_evidence_health_requires_foreground_admission_probe() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "memory_pressure": ready_memory_pressure(),
        "qos_snapshot": ready_qos_snapshot(),
        "slow_query": ready_slow_query(),
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.foreground_admission_probe_ready, None);
    assert_eq!(
        health.blockers,
        vec![
            "background maintenance evidence lacks a passing foreground admission probe"
                .to_string()
        ]
    );
    assert_eq!(
        health.blocker_codes,
        vec!["foreground_admission_probe_missing".to_string()]
    );
}

#[test]
fn background_maintenance_evidence_health_requires_slow_query_summary() {
    let summary = serde_json::json!({
        "protocol": "hawdb-background-maintenance-report",
        "total_candidates": 1,
        "foreground_admission_probe_ready": true,
        "foreground_admission_probe_admission": "admit",
        "memory_pressure": ready_memory_pressure(),
        "qos_snapshot": ready_qos_snapshot(),
        "ranked": [
            {
                "kind": "search_projection_graph_delta",
                "work_class": "projection",
                "priority": "background",
                "admission": "admit"
            }
        ]
    });

    let health = super::background_maintenance_evidence_health(Some(&summary), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.slow_query_ready, None);
    assert_eq!(health.blocker_codes, vec!["slow_query_missing".to_string()]);
    assert_eq!(
        health.blockers,
        vec!["background maintenance evidence lacks slow-query summary".to_string()]
    );
}

#[test]
fn storage_recovery_evidence_health_reports_stable_blocker_codes() {
    let report = serde_json::json!({
        "protocol": "unexpected-report",
        "readiness": {
            "durable_recovery_observed": false,
            "checkpoint_boundary_present": false,
            "wal_replay_bounded": false,
            "torn_tail_clean": false
        }
    });

    let health = super::storage_recovery_evidence_health(Some(&report), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(
        health.blocker_codes,
        vec![
            "protocol_mismatch".to_string(),
            "durable_recovery_not_observed".to_string(),
            "checkpoint_boundary_missing".to_string(),
            "wal_replay_unbounded".to_string(),
            "replay_boundary_inconsistent".to_string(),
            "torn_tail_observed".to_string()
        ]
    );
}

#[test]
fn storage_recovery_evidence_health_recomputes_raw_recovery_fields() {
    let report = serde_json::json!({
        "protocol": "hawdb-storage-recovery-report",
        "durable": true,
        "checkpoint_epoch": 7,
        "checkpoint_commit_epoch": null,
        "wal_replay_start_lsn": 8,
        "next_lsn_after_replay": 42,
        "replayed_wal_entries": 3,
        "max_wal_replay_entries": 2,
        "recovered_commit_epoch": 700,
        "torn_tail_ignored": true,
        "torn_tail_reason": "partial wal entry",
        "readiness": {
            "durable_recovery_observed": true,
            "checkpoint_boundary_present": true,
            "wal_replay_bounded": true,
            "torn_tail_clean": true
        }
    });

    let health = super::storage_recovery_evidence_health(Some(&report), true);

    assert!(health.present);
    assert!(!health.ready);
    assert_eq!(health.protocol_matches, Some(true));
    assert_eq!(health.durable_recovery_observed, Some(true));
    assert_eq!(health.checkpoint_boundary_present, Some(false));
    assert_eq!(health.wal_replay_bounded, Some(false));
    assert_eq!(health.replay_boundary_consistent, Some(false));
    assert_eq!(health.torn_tail_clean, Some(false));
    assert_eq!(
        health.blocker_codes,
        vec![
            "checkpoint_boundary_missing".to_string(),
            "wal_replay_unbounded".to_string(),
            "replay_boundary_inconsistent".to_string(),
            "torn_tail_observed".to_string()
        ]
    );
}
