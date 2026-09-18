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

//! Host-neutral JSON representation of background-maintenance summaries.

use crate::{BackgroundMaintenanceSummary, BackgroundMaintenanceSummaryItem};
use hawdb_qos::{LocalQosClassSnapshot, LocalQosSnapshot};

const BACKGROUND_MAINTENANCE_ESTIMATED_BYTES_PER_OPERATION: u64 = 1024;

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

fn background_maintenance_qos_snapshot_to_json(snapshot: &LocalQosSnapshot) -> serde_json::Value {
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
    item: &BackgroundMaintenanceSummaryItem,
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

fn insert_json<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(
        key.to_string(),
        serde_json::to_value(value).expect("background maintenance values must serialize"),
    );
}

#[cfg(test)]
mod tests {
    use super::background_maintenance_summary_to_json;
    use crate::BackgroundMaintenanceSummary;

    #[test]
    fn summary_json_preserves_empty_contract_shape() {
        let json = background_maintenance_summary_to_json(&BackgroundMaintenanceSummary::default());

        assert_eq!(json["total_candidates"], 0);
        assert_eq!(json["ranked"], serde_json::json!([]));
        assert!(json["qos_snapshot"].is_null());
        assert!(json["memory_pressure"].is_null());
    }
}
