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

//! Host-neutral library and operations readiness reports.
//!
//! The facade samples live handles; this module reduces those owned snapshots
//! without opening databases, retaining handles, or changing runtime authority.

use crate::bounded_read_evidence::GraphMode;
use crate::previous_wrapper_preflight::OPERATIONS_READINESS_PROTOCOL;
use crate::runtime_status::EmbeddedRuntimeStatus;
use crate::slow_query::SlowQueryReport;
use crate::{ReadinessAreaMap, ReadinessAreaSummary};
use hawdb_evidence::inventory::background_maintenance_evidence_health;
use hawdb_nowledge_contracts::{
    background_maintenance_summary_to_json, BackgroundMaintenanceKind,
    BackgroundMaintenanceSummary, EmbeddedStorageRecoveryReport, StorageLifecycleActionKind,
    StorageLifecycleDecision,
};

pub const READINESS_DASHBOARD_PROTOCOL: &str = "hawdb-nowledge-mem-readiness-dashboard-v1";

#[derive(Debug, Clone, PartialEq)]
pub struct LibraryReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: GraphMode,
    pub redaction: ReadinessRedactionSummary,
    pub production_path: LibraryProductionPathSummary,
    pub blocker_codes: Vec<String>,
    pub readiness_by_area: ReadinessAreaMap,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub graph_route_readiness: serde_json::Value,
    pub search_route_ownership: serde_json::Value,
    pub active_search_route_ownership: serde_json::Value,
    pub active_search_route_readiness: serde_json::Value,
    pub bounded_read_evidence: serde_json::Value,
    pub storage_recovery: serde_json::Value,
    pub background_maintenance: serde_json::Value,
    pub query_family_evidence: serde_json::Value,
    pub search_projection_evidence: serde_json::Value,
    pub search_projection_shadow_evidence: serde_json::Value,
    pub search_candidate_shadow_evidence: serde_json::Value,
    pub workload_fixture_evidence: serde_json::Value,
    pub production_resource_profile: serde_json::Value,
}

impl LibraryReadinessReport {
    pub fn areas(&self) -> Vec<ReadinessAreaSummary> {
        self.readiness_by_area.areas()
    }

    pub fn json(&self) -> serde_json::Value {
        let areas = self.areas();
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "redaction": self.redaction.json(),
            "production_path": self.production_path.json(),
            "blocker_codes": self.blocker_codes,
            "readiness_by_area": self.readiness_by_area.json(),
            "areas": areas.iter().map(ReadinessAreaSummary::json).collect::<Vec<_>>(),
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "graph": {
                "open": self.graph_open,
                "mode": self.mode.as_str(),
                "read_only": self.graph_read_only,
            },
            "graph_route_readiness": self.graph_route_readiness,
            "search_route_ownership": self.search_route_ownership,
            "active_search_route_ownership": self.active_search_route_ownership,
            "active_search_route_readiness": self.active_search_route_readiness,
            "bounded_read_evidence": self.bounded_read_evidence,
            "storage_recovery": self.storage_recovery,
            "background_maintenance": self.background_maintenance,
            "query_family_evidence": self.query_family_evidence,
            "search_projection_evidence": self.search_projection_evidence,
            "search_projection_shadow_evidence": self.search_projection_shadow_evidence,
            "search_candidate_shadow_evidence": self.search_candidate_shadow_evidence,
            "workload_fixture_evidence": self.workload_fixture_evidence,
            "production_resource_profile": self.production_resource_profile,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LibraryProductionPathSummary {
    pub in_process: bool,
    pub cli_required: bool,
    pub env_control_plane_required: bool,
    pub spawned_helper_required: bool,
}

impl Default for LibraryProductionPathSummary {
    fn default() -> Self {
        Self {
            in_process: true,
            cli_required: false,
            env_control_plane_required: false,
            spawned_helper_required: false,
        }
    }
}

impl LibraryProductionPathSummary {
    pub fn ready(&self) -> bool {
        self.in_process
            && !self.cli_required
            && !self.env_control_plane_required
            && !self.spawned_helper_required
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "in_process": self.in_process,
            "cli_required": self.cli_required,
            "env_control_plane_required": self.env_control_plane_required,
            "spawned_helper_required": self.spawned_helper_required,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadinessRedactionSummary {
    pub query_text_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
}

impl ReadinessRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.query_text_copied && !self.parameters_copied && !self.local_paths_copied
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "query_text_copied": self.query_text_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessDashboard {
    pub protocol: String,
    pub ready: bool,
    pub mode: GraphMode,
    pub area_count: usize,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub blocker_codes: Vec<String>,
    pub areas: Vec<ReadinessAreaSummary>,
    pub storage_lifecycle_action: StorageLifecycleActionKind,
    pub storage_lifecycle_ready: bool,
    pub slow_query_ready: bool,
    pub slow_query_record_count: usize,
}

impl ReadinessDashboard {
    #[doc(hidden)]
    pub fn from_reports(
        library: &LibraryReadinessReport,
        storage_lifecycle: &StorageLifecycleDecision,
        slow_query: &SlowQueryReport,
    ) -> Self {
        let areas = readiness_dashboard_areas(library, slow_query);
        let blocked_area_count = areas.iter().filter(|area| !area.ready).count();
        let ready_area_count = areas.len().saturating_sub(blocked_area_count);

        Self {
            protocol: READINESS_DASHBOARD_PROTOCOL.to_string(),
            ready: library.ready && slow_query.ready,
            mode: library.mode,
            area_count: areas.len(),
            ready_area_count,
            blocked_area_count,
            blocker_codes: library.blocker_codes.clone(),
            areas,
            storage_lifecycle_action: storage_lifecycle.action,
            storage_lifecycle_ready: storage_lifecycle.ready_for_mem_lifecycle,
            slow_query_ready: slow_query.ready,
            slow_query_record_count: slow_query.record_count,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "area_count": self.area_count,
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "blocker_codes": self.blocker_codes,
            "areas": self.areas.iter().map(ReadinessAreaSummary::json).collect::<Vec<_>>(),
            "storage_lifecycle": {
                "ready": self.storage_lifecycle_ready,
                "action": self.storage_lifecycle_action.as_str(),
            },
            "slow_query": {
                "ready": self.slow_query_ready,
                "record_count": self.slow_query_record_count,
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperationsReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: GraphMode,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub search_projection_open: bool,
    pub graph_commit_epoch: u64,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub storage_lifecycle_action: StorageLifecycleActionKind,
    pub storage_lifecycle_ready: bool,
    pub storage_recovery_ready: bool,
    pub slow_query_ready: bool,
    pub background_maintenance_ready: bool,
    pub blocker_codes: Vec<String>,
    pub runtime_status: EmbeddedRuntimeStatus,
    pub storage_lifecycle_decision: StorageLifecycleDecision,
    pub storage_recovery: EmbeddedStorageRecoveryReport,
    pub slow_query: SlowQueryReport,
    pub background_maintenance: BackgroundMaintenanceReport,
}

impl OperationsReadinessReport {
    #[doc(hidden)]
    pub fn from_reports(
        mode: GraphMode,
        graph_read_only: bool,
        runtime_status: EmbeddedRuntimeStatus,
        storage_recovery: EmbeddedStorageRecoveryReport,
        slow_query: SlowQueryReport,
        background_maintenance: BackgroundMaintenanceReport,
    ) -> Self {
        let search_projection_open = runtime_status.projection_freshness.is_some();
        let projection_commit_lag = runtime_status.projection_commit_lag();
        let projection_stale = search_projection_open && runtime_status.projection_stale();
        let storage_lifecycle_decision =
            StorageLifecycleDecision::from_storage_recovery(storage_recovery.clone());
        let mut blocker_codes = Vec::new();
        if !storage_lifecycle_decision.ready_for_mem_lifecycle {
            blocker_codes.push("storage_recovery_not_ready".to_string());
            blocker_codes.extend(
                storage_lifecycle_decision
                    .blocker_codes
                    .iter()
                    .map(|code| format!("storage_lifecycle.{code}")),
            );
        }
        if !slow_query.ready {
            blocker_codes.push("slow_query_report_not_ready".to_string());
        }
        if !background_maintenance.ready {
            blocker_codes.push("background_maintenance_not_ready".to_string());
        }
        if search_projection_open && projection_stale {
            blocker_codes.push("search_projection_stale".to_string());
        }

        Self {
            protocol: OPERATIONS_READINESS_PROTOCOL.to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            mode,
            graph_open: true,
            graph_read_only,
            search_projection_open,
            graph_commit_epoch: runtime_status.graph_commit_epoch,
            projection_commit_lag,
            projection_stale,
            storage_lifecycle_action: storage_lifecycle_decision.action,
            storage_lifecycle_ready: storage_lifecycle_decision.ready_for_mem_lifecycle,
            storage_recovery_ready: storage_recovery.ready,
            slow_query_ready: slow_query.ready,
            background_maintenance_ready: background_maintenance.ready,
            blocker_codes,
            runtime_status,
            storage_lifecycle_decision,
            storage_recovery,
            slow_query,
            background_maintenance,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "graph": {
                "open": self.graph_open,
                "read_only": self.graph_read_only,
                "commit_epoch": self.graph_commit_epoch,
            },
            "search_projection": {
                "open": self.search_projection_open,
                "commit_lag": self.projection_commit_lag,
                "stale": self.projection_stale,
            },
            "storage_lifecycle": {
                "ready": self.storage_lifecycle_ready,
                "action": self.storage_lifecycle_action.as_str(),
            },
            "readiness": {
                "storage_lifecycle_ready": self.storage_lifecycle_ready,
                "storage_recovery_ready": self.storage_recovery_ready,
                "slow_query_ready": self.slow_query_ready,
                "background_maintenance_ready": self.background_maintenance_ready,
            },
            "blocker_codes": self.blocker_codes,
            "runtime_status": self.runtime_status.json(),
            "storage_lifecycle_decision": self.storage_lifecycle_decision.json(),
            "storage_recovery": self.storage_recovery.json(),
            "slow_query": self.slow_query.json(),
            "background_maintenance": self.background_maintenance.json(),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackgroundMaintenanceReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub total_candidates: usize,
    pub admitted_count: usize,
    pub deferred_count: usize,
    pub rejected_count: usize,
    pub total_estimated_operations: usize,
    pub admitted_estimated_operations: usize,
    pub deferred_estimated_operations: usize,
    pub rejected_estimated_operations: usize,
    pub executable_search_projection_graph_delta_count: usize,
    pub admitted_search_projection_graph_delta_count: usize,
    pub deferred_search_projection_graph_delta_count: usize,
    pub rejected_search_projection_graph_delta_count: usize,
    pub executable_search_projection_graph_delta_operations: usize,
    pub admitted_search_projection_graph_delta_operations: usize,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub foreground_admission_probe_ready: Option<bool>,
    pub foreground_admission_probe_admission_name: Option<String>,
    pub memory_pressure_ready: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub estimated_memory_bytes: Option<u64>,
    pub slow_query_ready: Option<bool>,
    pub slow_query_record_count: Option<u64>,
    pub slow_query_capacity: Option<u64>,
    pub slow_query_redaction_ready: Option<bool>,
    pub top_admitted_kind: Option<BackgroundMaintenanceKind>,
    pub top_admitted_name: Option<String>,
    pub ranked_count: u64,
    pub foreground_ranked_count: u64,
    pub unknown_admission_count: u64,
    pub blocker_codes: Vec<String>,
    summary: serde_json::Value,
}

impl BackgroundMaintenanceReport {
    pub fn from_summary(summary: &BackgroundMaintenanceSummary) -> Self {
        Self::from_summary_with_slow_query(summary, None)
    }

    #[doc(hidden)]
    pub fn from_summary_with_slow_query(
        summary: &BackgroundMaintenanceSummary,
        slow_query: Option<&SlowQueryReport>,
    ) -> Self {
        let mut json = background_maintenance_summary_to_json(summary);
        if let Some(object) = json.as_object_mut() {
            object.insert(
                "protocol".to_string(),
                serde_json::Value::String("hawdb-background-maintenance-report".to_string()),
            );
            if let Some(slow_query) = slow_query {
                object.insert(
                    "slow_query".to_string(),
                    background_maintenance_slow_query_json(slow_query),
                );
                object.insert(
                    "slow_query_ready".to_string(),
                    serde_json::Value::Bool(slow_query.ready),
                );
                object.insert(
                    "slow_query_record_count".to_string(),
                    serde_json::json!(slow_query.record_count),
                );
                object.insert(
                    "slow_query_capacity".to_string(),
                    serde_json::json!(slow_query.capacity),
                );
            }
        }
        let health = background_maintenance_evidence_health(Some(&json), true);

        Self {
            protocol: "hawdb-background-maintenance-report".to_string(),
            present: true,
            ready: health.ready,
            total_candidates: summary.total_candidates,
            admitted_count: summary.admitted_count,
            deferred_count: summary.deferred_count,
            rejected_count: summary.rejected_count,
            total_estimated_operations: summary.total_estimated_operations,
            admitted_estimated_operations: summary.admitted_estimated_operations,
            deferred_estimated_operations: summary.deferred_estimated_operations,
            rejected_estimated_operations: summary.rejected_estimated_operations,
            executable_search_projection_graph_delta_count: summary
                .executable_search_projection_graph_delta_count,
            admitted_search_projection_graph_delta_count: summary
                .admitted_search_projection_graph_delta_count,
            deferred_search_projection_graph_delta_count: summary
                .deferred_search_projection_graph_delta_count,
            rejected_search_projection_graph_delta_count: summary
                .rejected_search_projection_graph_delta_count,
            executable_search_projection_graph_delta_operations: summary
                .executable_search_projection_graph_delta_operations,
            admitted_search_projection_graph_delta_operations: summary
                .admitted_search_projection_graph_delta_operations,
            max_search_projection_graph_delta_complete_through_graph_commit_epoch: summary
                .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            foreground_admission_probe_ready: health.foreground_admission_probe_ready,
            foreground_admission_probe_admission_name: health
                .foreground_admission_probe_admission_name,
            memory_pressure_ready: health.memory_pressure_ready,
            memory_budget_bytes: health.memory_budget_bytes,
            estimated_memory_bytes: health.estimated_memory_bytes,
            slow_query_ready: health.slow_query_ready,
            slow_query_record_count: health.slow_query_record_count,
            slow_query_capacity: health.slow_query_capacity,
            slow_query_redaction_ready: health.slow_query_redaction_ready,
            top_admitted_kind: summary.top_admitted_kind,
            top_admitted_name: summary.top_admitted_name.clone(),
            ranked_count: health.ranked_count.unwrap_or_default(),
            foreground_ranked_count: health.foreground_ranked_count,
            unknown_admission_count: health.unknown_admission_count,
            blocker_codes: health.blocker_codes,
            summary: json,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.summary.clone()
    }
}

fn background_maintenance_slow_query_json(slow_query: &SlowQueryReport) -> serde_json::Value {
    serde_json::json!({
        "protocol": slow_query.protocol,
        "ready": slow_query.ready,
        "capacity": slow_query.capacity,
        "record_count": slow_query.record_count,
        "latest_sequence": slow_query.latest_sequence,
        "max_elapsed_micros": slow_query.max_elapsed_micros,
        "redaction": {
            "query_text_copied": false,
            "parameters_copied": false,
            "local_paths_copied": false,
        },
    })
}

fn readiness_dashboard_areas(
    library: &LibraryReadinessReport,
    slow_query: &SlowQueryReport,
) -> Vec<ReadinessAreaSummary> {
    let mut areas = library.areas();
    areas.push(ReadinessAreaSummary {
        name: "slow_query".to_string(),
        ready: slow_query.ready,
        blocker_codes: if slow_query.ready {
            Vec::new()
        } else {
            vec!["slow_query_report_not_ready".to_string()]
        },
    });
    areas
}
