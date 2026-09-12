//! Evidence-only health evaluation; database probes and report assembly stay in the facade.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRecoveryEvidenceHealth {
    pub required: bool,
    pub present: bool,
    pub ready: bool,
    pub protocol_matches: Option<bool>,
    pub durable_recovery_observed: Option<bool>,
    pub checkpoint_boundary_present: Option<bool>,
    pub wal_replay_bounded: Option<bool>,
    pub replay_boundary_consistent: Option<bool>,
    pub torn_tail_clean: Option<bool>,
    pub blocker_codes: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceEvidenceHealth {
    pub required: bool,
    pub present: bool,
    pub ready: bool,
    pub protocol_matches: Option<bool>,
    pub total_candidates: Option<u64>,
    pub ranked_count: Option<u64>,
    pub executable_search_projection_graph_delta_count: Option<u64>,
    pub admitted_search_projection_graph_delta_count: Option<u64>,
    pub deferred_search_projection_graph_delta_count: Option<u64>,
    pub rejected_search_projection_graph_delta_count: Option<u64>,
    pub executable_search_projection_graph_delta_operations: Option<u64>,
    pub admitted_search_projection_graph_delta_operations: Option<u64>,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch: Option<u64>,
    pub foreground_admission_probe_ready: Option<bool>,
    pub foreground_admission_probe_admission_name: Option<String>,
    pub memory_pressure_ready: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub estimated_memory_bytes: Option<u64>,
    pub qos_snapshot_ready: Option<bool>,
    pub qos_snapshot_foreground_admitted: Option<bool>,
    pub qos_snapshot_background_bounded: Option<bool>,
    pub qos_snapshot_total_background_over_budget: Option<bool>,
    pub qos_snapshot_blocker_codes: Vec<String>,
    pub slow_query_ready: Option<bool>,
    pub slow_query_record_count: Option<u64>,
    pub slow_query_capacity: Option<u64>,
    pub slow_query_redaction_ready: Option<bool>,
    pub foreground_ranked_count: u64,
    pub unknown_admission_count: u64,
    pub blocker_codes: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacementReadinessFamilyEvidenceHealth {
    pub present: bool,
    pub ready: bool,
    pub min_replacement_readiness_per_million: Option<u64>,
    pub invalid_family_count: u64,
    pub blocked_query_families: Vec<String>,
    pub missing_required_query_families: Vec<String>,
    pub blockers: Vec<String>,
}

pub const NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";

pub const REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES: &[&str] = &[
    "memory_lookup",
    "graph_traversal",
    "projected_graph",
    "label_stats_read",
    "search_projection",
];

pub fn storage_recovery_evidence_health_from_bundle(
    bundle: &serde_json::Value,
    required: bool,
) -> StorageRecoveryEvidenceHealth {
    storage_recovery_evidence_health(bundle.get("storage_recovery"), required)
}

pub fn background_maintenance_evidence_health_from_bundle(
    bundle: &serde_json::Value,
    required: bool,
) -> BackgroundMaintenanceEvidenceHealth {
    background_maintenance_evidence_health(bundle.get("background_maintenance"), required)
}

pub fn replacement_readiness_family_evidence_health_from_bundle(
    bundle: &serde_json::Value,
) -> ReplacementReadinessFamilyEvidenceHealth {
    replacement_readiness_family_evidence_health(
        bundle.get("replacement_readiness_by_query_family"),
    )
}

pub fn replacement_readiness_family_evidence_health(
    replacement_readiness_by_query_family: Option<&serde_json::Value>,
) -> ReplacementReadinessFamilyEvidenceHealth {
    let Some(families) =
        replacement_readiness_by_query_family.and_then(serde_json::Value::as_array)
    else {
        return ReplacementReadinessFamilyEvidenceHealth {
            present: false,
            ready: true,
            min_replacement_readiness_per_million: None,
            invalid_family_count: 0,
            blocked_query_families: Vec::new(),
            missing_required_query_families: Vec::new(),
            blockers: Vec::new(),
        };
    };

    let invalid_family_count = families
        .iter()
        .filter(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
                .is_none()
                || family
                    .get("replacement_readiness_per_million")
                    .and_then(serde_json::Value::as_u64)
                    .is_none()
        })
        .count() as u64;
    let min_replacement_readiness_per_million = families
        .iter()
        .filter_map(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
        })
        .min();
    let blocked_query_families = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|readiness| readiness < 1_000_000)
        })
        .filter_map(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let present_query_families = families
        .iter()
        .filter_map(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
        })
        .collect::<std::collections::BTreeSet<_>>();
    let missing_required_query_families = REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
        .iter()
        .filter(|family| !present_query_families.contains(**family))
        .map(|family| (*family).to_string())
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if invalid_family_count > 0 {
        blockers.push("replacement readiness family report has invalid entries".to_string());
    }
    if !blocked_query_families.is_empty() {
        blockers.push(format!(
            "replacement readiness is incomplete for query families: {}",
            blocked_query_families.join(", ")
        ));
    }
    if !missing_required_query_families.is_empty() {
        blockers.push(format!(
            "replacement readiness is missing required query families: {}",
            missing_required_query_families.join(", ")
        ));
    }

    ReplacementReadinessFamilyEvidenceHealth {
        present: true,
        ready: blockers.is_empty(),
        min_replacement_readiness_per_million,
        invalid_family_count,
        blocked_query_families,
        missing_required_query_families,
        blockers,
    }
}

pub fn background_maintenance_evidence_health(
    background_maintenance: Option<&serde_json::Value>,
    required: bool,
) -> BackgroundMaintenanceEvidenceHealth {
    let Some(background_maintenance) = background_maintenance else {
        let blockers = if required {
            vec!["background maintenance evidence is required before cutover".to_string()]
        } else {
            Vec::new()
        };
        return BackgroundMaintenanceEvidenceHealth {
            required,
            present: false,
            ready: !required,
            protocol_matches: None,
            total_candidates: None,
            ranked_count: None,
            executable_search_projection_graph_delta_count: None,
            admitted_search_projection_graph_delta_count: None,
            deferred_search_projection_graph_delta_count: None,
            rejected_search_projection_graph_delta_count: None,
            executable_search_projection_graph_delta_operations: None,
            admitted_search_projection_graph_delta_operations: None,
            max_search_projection_graph_delta_complete_through_graph_commit_epoch: None,
            foreground_admission_probe_ready: None,
            foreground_admission_probe_admission_name: None,
            memory_pressure_ready: None,
            memory_budget_bytes: None,
            estimated_memory_bytes: None,
            qos_snapshot_ready: None,
            qos_snapshot_foreground_admitted: None,
            qos_snapshot_background_bounded: None,
            qos_snapshot_total_background_over_budget: None,
            qos_snapshot_blocker_codes: Vec::new(),
            slow_query_ready: None,
            slow_query_record_count: None,
            slow_query_capacity: None,
            slow_query_redaction_ready: None,
            foreground_ranked_count: 0,
            unknown_admission_count: 0,
            blocker_codes: if required {
                vec!["missing_evidence".to_string()]
            } else {
                Vec::new()
            },
            blockers,
        };
    };
    let protocol_matches = background_maintenance
        .get("protocol")
        .and_then(serde_json::Value::as_str)
        .map(|protocol| protocol == "skein-background-maintenance-report");
    let total_candidates = background_maintenance
        .get("total_candidates")
        .and_then(serde_json::Value::as_u64);
    let ranked = background_maintenance
        .get("ranked")
        .and_then(serde_json::Value::as_array);
    let ranked_count = ranked.map(|items| items.len() as u64);
    let executable_search_projection_graph_delta_count = optional_u64_field(
        background_maintenance,
        "executable_search_projection_graph_delta_count",
    )
    .or_else(|| {
        Some(derived_executable_search_projection_graph_delta_count(
            background_maintenance,
        ))
    });
    let admitted_search_projection_graph_delta_count = optional_u64_field(
        background_maintenance,
        "admitted_search_projection_graph_delta_count",
    )
    .or_else(|| {
        Some(derived_search_projection_graph_delta_admission_count(
            background_maintenance,
            "admit",
        ))
    });
    let deferred_search_projection_graph_delta_count = optional_u64_field(
        background_maintenance,
        "deferred_search_projection_graph_delta_count",
    )
    .or_else(|| {
        Some(derived_search_projection_graph_delta_admission_count(
            background_maintenance,
            "defer",
        ))
    });
    let rejected_search_projection_graph_delta_count = optional_u64_field(
        background_maintenance,
        "rejected_search_projection_graph_delta_count",
    )
    .or_else(|| {
        Some(derived_search_projection_graph_delta_admission_count(
            background_maintenance,
            "reject",
        ))
    });
    let executable_search_projection_graph_delta_operations = optional_u64_field(
        background_maintenance,
        "executable_search_projection_graph_delta_operations",
    )
    .or_else(|| {
        Some(derived_search_projection_graph_delta_operations(
            background_maintenance,
            None,
        ))
    });
    let admitted_search_projection_graph_delta_operations = optional_u64_field(
        background_maintenance,
        "admitted_search_projection_graph_delta_operations",
    )
    .or_else(|| {
        Some(derived_search_projection_graph_delta_operations(
            background_maintenance,
            Some("admit"),
        ))
    });
    let max_search_projection_graph_delta_complete_through_graph_commit_epoch = optional_u64_field(
        background_maintenance,
        "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
    )
    .or_else(|| {
        derived_max_search_projection_graph_delta_complete_through_graph_commit_epoch(
            background_maintenance,
        )
    });
    let memory_pressure = background_maintenance.get("memory_pressure");
    let foreground_admission_probe_ready = background_maintenance
        .get("foreground_admission_probe_ready")
        .and_then(serde_json::Value::as_bool);
    let foreground_admission_probe_admission_name = background_maintenance
        .get("foreground_admission_probe_admission")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let raw_memory_pressure_ready = memory_pressure
        .and_then(|memory_pressure| memory_pressure.get("ready"))
        .or_else(|| background_maintenance.get("memory_pressure_ready"))
        .and_then(serde_json::Value::as_bool);
    let memory_budget_bytes = memory_pressure
        .and_then(|memory_pressure| memory_pressure.get("budget_bytes"))
        .or_else(|| background_maintenance.get("memory_budget_bytes"))
        .and_then(serde_json::Value::as_u64);
    let estimated_memory_bytes = memory_pressure
        .and_then(|memory_pressure| memory_pressure.get("estimated_bytes"))
        .or_else(|| background_maintenance.get("estimated_memory_bytes"))
        .and_then(serde_json::Value::as_u64);
    let memory_pressure_ready = if raw_memory_pressure_ready.is_some()
        || memory_budget_bytes.is_some()
        || estimated_memory_bytes.is_some()
    {
        Some(
            raw_memory_pressure_ready == Some(true)
                && estimated_memory_bytes
                    .zip(memory_budget_bytes)
                    .is_some_and(|(estimated, budget)| estimated <= budget),
        )
    } else {
        None
    };
    let qos_snapshot = background_maintenance.get("qos_snapshot");
    let qos_snapshot_ready = qos_snapshot
        .and_then(|snapshot| snapshot.get("ready"))
        .or_else(|| background_maintenance.get("qos_snapshot_ready"))
        .and_then(serde_json::Value::as_bool);
    let qos_snapshot_foreground_admitted = qos_snapshot
        .and_then(|snapshot| snapshot.get("foreground_admitted"))
        .or_else(|| background_maintenance.get("qos_snapshot_foreground_admitted"))
        .and_then(serde_json::Value::as_bool);
    let qos_snapshot_background_bounded = qos_snapshot
        .and_then(|snapshot| snapshot.get("background_bounded"))
        .or_else(|| background_maintenance.get("qos_snapshot_background_bounded"))
        .and_then(serde_json::Value::as_bool);
    let qos_snapshot_total_background_over_budget = qos_snapshot
        .and_then(|snapshot| snapshot.get("total_background_over_budget"))
        .or_else(|| background_maintenance.get("qos_snapshot_total_background_over_budget"))
        .and_then(serde_json::Value::as_bool);
    let qos_snapshot_blocker_codes = qos_snapshot
        .and_then(|snapshot| snapshot.get("blocker_codes"))
        .or_else(|| background_maintenance.get("qos_snapshot_blocker_codes"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    let slow_query = background_maintenance.get("slow_query");
    let slow_query_ready = slow_query
        .and_then(|slow_query| slow_query.get("ready"))
        .or_else(|| background_maintenance.get("slow_query_ready"))
        .and_then(serde_json::Value::as_bool);
    let slow_query_record_count = slow_query
        .and_then(|slow_query| slow_query.get("record_count"))
        .or_else(|| background_maintenance.get("slow_query_record_count"))
        .and_then(serde_json::Value::as_u64);
    let slow_query_capacity = slow_query
        .and_then(|slow_query| slow_query.get("capacity"))
        .or_else(|| background_maintenance.get("slow_query_capacity"))
        .and_then(serde_json::Value::as_u64);
    let slow_query_redaction = slow_query.and_then(|slow_query| slow_query.get("redaction"));
    let slow_query_redaction_ready = slow_query.map(|_| {
        slow_query_redaction
            .and_then(|redaction| redaction.get("query_text_copied"))
            .and_then(serde_json::Value::as_bool)
            == Some(false)
            && slow_query_redaction
                .and_then(|redaction| redaction.get("parameters_copied"))
                .and_then(serde_json::Value::as_bool)
                == Some(false)
            && slow_query_redaction
                .and_then(|redaction| redaction.get("local_paths_copied"))
                .and_then(serde_json::Value::as_bool)
                == Some(false)
    });
    let foreground_ranked_count = ranked
        .into_iter()
        .flatten()
        .filter(|item| {
            item.get("priority").and_then(serde_json::Value::as_str) == Some("foreground")
        })
        .count() as u64;
    let unknown_admission_count = background_maintenance
        .get("ranked")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            !matches!(
                item.get("admission").and_then(serde_json::Value::as_str),
                Some("admit" | "defer" | "reject")
            )
        })
        .count() as u64;
    let mut blocker_codes = Vec::new();
    let mut blockers = Vec::new();
    if protocol_matches == Some(false) {
        blocker_codes.push("protocol_mismatch".to_string());
        blockers.push("background maintenance evidence protocol mismatch".to_string());
    }
    if required && total_candidates.unwrap_or_default() == 0 {
        blocker_codes.push("no_candidates".to_string());
        blockers.push("background maintenance evidence has no candidates".to_string());
    }
    if required && ranked_count.unwrap_or_default() == 0 {
        blocker_codes.push("no_ranked_work".to_string());
        blockers.push("background maintenance evidence has no ranked work".to_string());
    }
    if required && foreground_admission_probe_ready != Some(true) {
        blocker_codes.push("foreground_admission_probe_missing".to_string());
        blockers.push(
            "background maintenance evidence lacks a passing foreground admission probe"
                .to_string(),
        );
    }
    if foreground_ranked_count > 0 {
        blocker_codes.push("foreground_ranked_work".to_string());
        blockers.push("background maintenance evidence ranked foreground work".to_string());
    }
    if unknown_admission_count > 0 {
        blocker_codes.push("unknown_admission".to_string());
        blockers.push("background maintenance evidence has unknown admission values".to_string());
    }
    if required && memory_pressure_ready.is_none() {
        blocker_codes.push("memory_pressure_missing".to_string());
        blockers.push("background maintenance evidence lacks memory-pressure budget".to_string());
    }
    if memory_pressure_ready == Some(false) {
        blocker_codes.push("memory_budget_exceeded".to_string());
        blockers
            .push("background maintenance evidence exceeds configured memory budget".to_string());
    }
    if required && qos_snapshot_ready.is_none() {
        blocker_codes.push("qos_snapshot_missing".to_string());
        blockers.push("background maintenance evidence lacks a local QoS snapshot".to_string());
    }
    if qos_snapshot_ready == Some(false) {
        blocker_codes.push("qos_snapshot_not_ready".to_string());
        blockers.push("background maintenance local QoS snapshot is not ready".to_string());
    }
    if qos_snapshot_background_bounded == Some(false) {
        blocker_codes.push("qos_snapshot_background_unbounded".to_string());
        blockers.push("background maintenance local QoS snapshot is unbounded".to_string());
    }
    if qos_snapshot_total_background_over_budget == Some(true) {
        blocker_codes.push("qos_snapshot_background_over_budget".to_string());
        blockers.push("background maintenance local QoS snapshot is over budget".to_string());
    }
    blocker_codes.extend(qos_snapshot_blocker_codes.iter().cloned());
    if required && slow_query_ready.is_none() {
        blocker_codes.push("slow_query_missing".to_string());
        blockers.push("background maintenance evidence lacks slow-query summary".to_string());
    }
    if slow_query_ready == Some(false) {
        blocker_codes.push("slow_query_not_ready".to_string());
        blockers.push("background maintenance slow-query summary is not ready".to_string());
    }
    if slow_query_record_count
        .zip(slow_query_capacity)
        .is_some_and(|(record_count, capacity)| record_count > capacity)
    {
        blocker_codes.push("slow_query_unbounded".to_string());
        blockers.push("background maintenance slow-query summary exceeds capacity".to_string());
    }
    if slow_query_redaction_ready == Some(false) {
        blocker_codes.push("slow_query_redaction_not_ready".to_string());
        blockers.push("background maintenance slow-query summary is not redacted".to_string());
    }
    BackgroundMaintenanceEvidenceHealth {
        required,
        present: true,
        ready: blockers.is_empty(),
        protocol_matches,
        total_candidates,
        ranked_count,
        executable_search_projection_graph_delta_count,
        admitted_search_projection_graph_delta_count,
        deferred_search_projection_graph_delta_count,
        rejected_search_projection_graph_delta_count,
        executable_search_projection_graph_delta_operations,
        admitted_search_projection_graph_delta_operations,
        max_search_projection_graph_delta_complete_through_graph_commit_epoch,
        foreground_admission_probe_ready,
        foreground_admission_probe_admission_name,
        memory_pressure_ready,
        memory_budget_bytes,
        estimated_memory_bytes,
        qos_snapshot_ready,
        qos_snapshot_foreground_admitted,
        qos_snapshot_background_bounded,
        qos_snapshot_total_background_over_budget,
        qos_snapshot_blocker_codes,
        slow_query_ready,
        slow_query_record_count,
        slow_query_capacity,
        slow_query_redaction_ready,
        foreground_ranked_count,
        unknown_admission_count,
        blocker_codes,
        blockers,
    }
}

fn optional_u64_field(object: &serde_json::Value, field: &str) -> Option<u64> {
    object.get(field).and_then(serde_json::Value::as_u64)
}

fn ranked_background_maintenance_items(
    background_maintenance: &serde_json::Value,
) -> impl Iterator<Item = &serde_json::Value> {
    background_maintenance
        .get("ranked")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
}

fn is_executable_search_projection_graph_delta(item: &serde_json::Value) -> bool {
    item.get("has_executable_search_projection_graph_delta")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}

fn derived_executable_search_projection_graph_delta_count(
    background_maintenance: &serde_json::Value,
) -> u64 {
    ranked_background_maintenance_items(background_maintenance)
        .filter(|item| is_executable_search_projection_graph_delta(item))
        .count() as u64
}

fn derived_search_projection_graph_delta_admission_count(
    background_maintenance: &serde_json::Value,
    admission: &str,
) -> u64 {
    ranked_background_maintenance_items(background_maintenance)
        .filter(|item| {
            is_executable_search_projection_graph_delta(item)
                && item.get("admission").and_then(serde_json::Value::as_str) == Some(admission)
        })
        .count() as u64
}

fn derived_search_projection_graph_delta_operations(
    background_maintenance: &serde_json::Value,
    admission: Option<&str>,
) -> u64 {
    ranked_background_maintenance_items(background_maintenance)
        .filter(|item| {
            is_executable_search_projection_graph_delta(item)
                && admission.is_none_or(|admission| {
                    item.get("admission").and_then(serde_json::Value::as_str) == Some(admission)
                })
        })
        .filter_map(|item| {
            item.get("search_projection_graph_delta_operation_count")
                .and_then(serde_json::Value::as_u64)
        })
        .sum()
}

fn derived_max_search_projection_graph_delta_complete_through_graph_commit_epoch(
    background_maintenance: &serde_json::Value,
) -> Option<u64> {
    ranked_background_maintenance_items(background_maintenance)
        .filter(|item| is_executable_search_projection_graph_delta(item))
        .filter_map(|item| {
            item.get("search_projection_graph_delta_complete_through_graph_commit_epoch")
                .and_then(serde_json::Value::as_u64)
        })
        .max()
}

pub fn storage_recovery_evidence_health(
    storage_recovery: Option<&serde_json::Value>,
    required: bool,
) -> StorageRecoveryEvidenceHealth {
    let Some(storage_recovery) = storage_recovery else {
        let blockers = if required {
            vec!["storage recovery evidence is required before cutover".to_string()]
        } else {
            Vec::new()
        };
        return StorageRecoveryEvidenceHealth {
            required,
            present: false,
            ready: !required,
            protocol_matches: None,
            durable_recovery_observed: None,
            checkpoint_boundary_present: None,
            wal_replay_bounded: None,
            replay_boundary_consistent: None,
            torn_tail_clean: None,
            blocker_codes: if required {
                vec!["missing_evidence".to_string()]
            } else {
                Vec::new()
            },
            blockers,
        };
    };
    let protocol_matches = storage_recovery
        .get("protocol")
        .and_then(serde_json::Value::as_str)
        .map(|protocol| protocol == "skein-storage-recovery-report");
    let readiness = storage_recovery.get("readiness");
    let readiness_durable_recovery_observed = readiness
        .and_then(|readiness| readiness.get("durable_recovery_observed"))
        .and_then(serde_json::Value::as_bool);
    let readiness_checkpoint_boundary_present = readiness
        .and_then(|readiness| readiness.get("checkpoint_boundary_present"))
        .and_then(serde_json::Value::as_bool);
    let readiness_wal_replay_bounded = readiness
        .and_then(|readiness| readiness.get("wal_replay_bounded"))
        .and_then(serde_json::Value::as_bool);
    let readiness_torn_tail_clean = readiness
        .and_then(|readiness| readiness.get("torn_tail_clean"))
        .and_then(serde_json::Value::as_bool);
    let durable_recovery_observed = Some(
        readiness_durable_recovery_observed == Some(true)
            && storage_recovery
                .get("durable")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
    );
    let checkpoint_boundary_present = Some(
        readiness_checkpoint_boundary_present == Some(true)
            && storage_recovery
                .get("checkpoint_epoch")
                .and_then(serde_json::Value::as_u64)
                .is_some()
            && storage_recovery
                .get("checkpoint_commit_epoch")
                .and_then(serde_json::Value::as_u64)
                .is_some(),
    );
    let replayed_wal_entries = storage_recovery
        .get("replayed_wal_entries")
        .and_then(serde_json::Value::as_u64);
    let max_wal_replay_entries = storage_recovery
        .get("max_wal_replay_entries")
        .and_then(serde_json::Value::as_u64);
    let wal_replay_bounded = Some(
        readiness_wal_replay_bounded == Some(true)
            && replayed_wal_entries
                .zip(max_wal_replay_entries)
                .is_some_and(|(replayed, max)| replayed <= max),
    );
    let replay_boundary_consistent = Some(storage_recovery_replay_boundary_consistent(
        storage_recovery,
        replayed_wal_entries,
    ));
    let torn_tail_clean = Some(
        readiness_torn_tail_clean == Some(true)
            && storage_recovery
                .get("torn_tail_ignored")
                .and_then(serde_json::Value::as_bool)
                == Some(false)
            && storage_recovery
                .get("torn_tail_reason")
                .is_none_or(serde_json::Value::is_null),
    );
    let mut blocker_codes = Vec::new();
    let mut blockers = Vec::new();
    if protocol_matches != Some(true) {
        blocker_codes.push("protocol_mismatch".to_string());
        blockers.push("storage recovery evidence protocol mismatch".to_string());
    }
    if durable_recovery_observed != Some(true) {
        blocker_codes.push("durable_recovery_not_observed".to_string());
        blockers.push("storage recovery evidence does not prove durable recovery".to_string());
    }
    if checkpoint_boundary_present != Some(true) {
        blocker_codes.push("checkpoint_boundary_missing".to_string());
        blockers.push("storage recovery evidence lacks checkpoint boundary".to_string());
    }
    if wal_replay_bounded != Some(true) {
        blocker_codes.push("wal_replay_unbounded".to_string());
        blockers.push("storage recovery evidence lacks bounded WAL replay".to_string());
    }
    if replay_boundary_consistent != Some(true) {
        blocker_codes.push("replay_boundary_inconsistent".to_string());
        blockers
            .push("storage recovery evidence has inconsistent WAL replay boundaries".to_string());
    }
    if torn_tail_clean != Some(true) {
        blocker_codes.push("torn_tail_observed".to_string());
        blockers.push("storage recovery evidence observed torn WAL tail".to_string());
    }
    StorageRecoveryEvidenceHealth {
        required,
        present: true,
        ready: blockers.is_empty(),
        protocol_matches,
        durable_recovery_observed,
        checkpoint_boundary_present,
        wal_replay_bounded,
        replay_boundary_consistent,
        torn_tail_clean,
        blocker_codes,
        blockers,
    }
}

fn storage_recovery_replay_boundary_consistent(
    storage_recovery: &serde_json::Value,
    replayed_wal_entries: Option<u64>,
) -> bool {
    let checkpoint_commit_epoch = storage_recovery
        .get("checkpoint_commit_epoch")
        .and_then(serde_json::Value::as_u64);
    let wal_replay_start_lsn = storage_recovery
        .get("wal_replay_start_lsn")
        .and_then(serde_json::Value::as_u64);
    let next_lsn_after_replay = storage_recovery
        .get("next_lsn_after_replay")
        .and_then(serde_json::Value::as_u64);
    let recovered_commit_epoch = storage_recovery
        .get("recovered_commit_epoch")
        .and_then(serde_json::Value::as_u64);

    matches!(
        (
            checkpoint_commit_epoch,
            wal_replay_start_lsn,
            next_lsn_after_replay,
            replayed_wal_entries,
            recovered_commit_epoch,
        ),
        (
            Some(checkpoint_commit_epoch),
            Some(wal_replay_start_lsn),
            Some(next_lsn_after_replay),
            Some(replayed_wal_entries),
            Some(recovered_commit_epoch),
        ) if checkpoint_commit_epoch <= recovered_commit_epoch
            && wal_replay_start_lsn.checked_add(replayed_wal_entries)
                == Some(next_lsn_after_replay)
            && checkpoint_commit_epoch.checked_add(replayed_wal_entries)
                == Some(recovered_commit_epoch)
    )
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod differential;
