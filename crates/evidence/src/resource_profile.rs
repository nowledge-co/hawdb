//! Validation of production storage resource-profile evidence.

use std::collections::BTreeSet;

use crate::json_access::{
    evidence_bool, evidence_string, evidence_u64, nested_bool, nested_u64, nested_value,
    string_array_at,
};

pub const STORAGE_RESOURCE_PROFILE_PROTOCOL: &str = "skein-storage-resource-profile-v2";

/// Recomputes readiness from the evidence instead of trusting its ready flags.
pub fn production_resource_profile_ready(evidence: &serde_json::Value) -> bool {
    production_resource_profile_blocker_codes(evidence).is_empty()
}

/// Returns sorted, deduplicated failures of the resource-profile evidence contract.
pub fn production_resource_profile_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if evidence_string(evidence, "protocol") != Some(STORAGE_RESOURCE_PROFILE_PROTOCOL)
        || evidence_u64(evidence, "protocol_version") != Some(2)
    {
        blockers.insert("production_resource_profile_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "present") != Some(true) {
        blockers.insert("production_resource_profile_missing".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("production_resource_profile_not_ready".to_string());
    }
    if evidence_bool(evidence, "resource_ready") != Some(true) {
        blockers.insert("production_resource_profile_resource_not_ready".to_string());
    }
    if !string_array_at(evidence, &["blocker_codes"]).is_some_and(|codes| codes.is_empty()) {
        blockers.insert("production_resource_profile_has_blockers".to_string());
    }

    let binding_identity = nested_value(evidence, &["evidence_binding", "identity"]);
    let expected_identity = evidence.get("expected_identity");
    let canonical_graph_commit_epoch = evidence_u64(evidence, "canonical_graph_commit_epoch");
    let identity_valid = evidence_bool(evidence, "identity_matches_expected") == Some(true)
        && nested_u64(evidence, &["evidence_binding", "generated_at_unix_seconds"])
            .is_some_and(|generated_at| generated_at > 0)
        && binding_identity == expected_identity
        && binding_identity.is_some_and(|identity| {
            [
                "source_revision",
                "rust_toolchain",
                "target_os",
                "target_arch",
                "configuration_digest",
                "deployment_profile",
                "dataset_fingerprint",
            ]
            .into_iter()
            .all(|field| {
                identity
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
            }) && identity
                .get("enabled_features")
                .is_some_and(serde_json::Value::is_array)
                && identity
                    .get("durable_format_version")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|version| version > 0)
                && identity
                    .get("schema_version")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|version| version > 0)
                && identity
                    .get("policy_version")
                    .and_then(serde_json::Value::as_u64)
                    == Some(crate::PRODUCTION_QUALIFICATION_POLICY_VERSION)
                && identity
                    .get("canonical_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == canonical_graph_commit_epoch
        });
    if !identity_valid {
        blockers.insert("production_resource_profile_identity_invalid".to_string());
    }

    let canonical_bytes = nested_u64(evidence, &["storage", "canonical_artifact_bytes"]);
    let cache_capacity = nested_u64(evidence, &["storage", "segment_cache_capacity_bytes"]);
    let cache_resident = nested_u64(evidence, &["storage", "segment_cache_resident_bytes_after"]);
    let minimum_canonical_bytes = nested_u64(evidence, &["limits", "min_canonical_artifact_bytes"]);
    let storage_bytes_within_budget = matches!(
        (
            canonical_bytes,
            cache_capacity,
            cache_resident,
            minimum_canonical_bytes,
        ),
        (Some(canonical), Some(capacity), Some(resident), Some(minimum))
            if canonical >= minimum && canonical > capacity && resident <= capacity
    );
    if nested_bool(evidence, &["storage", "durable"]) != Some(true)
        || nested_bool(evidence, &["storage", "out_of_core"]) != Some(true)
        || nested_bool(evidence, &["storage", "canonical_exceeds_cache"]) != Some(true)
        || nested_bool(evidence, &["storage", "delta_within_budget"]) != Some(true)
        || !storage_bytes_within_budget
    {
        blockers.insert("production_resource_profile_storage_budget_invalid".to_string());
    }

    let require_fully_streamed = nested_bool(evidence, &["limits", "require_fully_streamed"]);
    let fully_streamed = nested_bool(evidence, &["execution", "fully_streamed"]);
    if require_fully_streamed.is_none()
        || fully_streamed.is_none()
        || (require_fully_streamed == Some(true) && fully_streamed != Some(true))
    {
        blockers.insert("production_resource_profile_streaming_invalid".to_string());
    }
    if nested_u64(evidence, &["execution", "start_resident_bytes"]).is_none()
        || nested_u64(evidence, &["execution", "start_peak_resident_bytes"]).is_none()
        || nested_u64(evidence, &["execution", "steady_resident_growth_bytes"]).is_none()
        || nested_u64(
            evidence,
            &["execution", "lifetime_peak_resident_growth_bytes"],
        )
        .is_none()
    {
        blockers.insert("production_resource_profile_resident_growth_missing".to_string());
    }

    for (metric, limit) in [
        ("steady_resident_bytes", "max_steady_resident_bytes"),
        ("peak_resident_bytes", "max_peak_resident_bytes"),
        ("intermediate_rows", "max_intermediate_rows"),
        (
            "intermediate_payload_bytes",
            "max_intermediate_payload_bytes",
        ),
        ("output_rows", "max_output_rows"),
        ("output_payload_bytes", "max_output_payload_bytes"),
    ] {
        let measured = nested_u64(evidence, &["execution", metric]);
        let admitted = nested_u64(evidence, &["limits", limit]);
        if measured.is_none() || admitted.is_none() || measured > admitted {
            blockers.insert(format!("production_resource_profile_{metric}_invalid"));
        }
    }

    let resident_memory_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "resident_memory"],
    );
    let total_page_faults_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "total_page_faults"],
    );
    let split_page_faults_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "split_page_faults"],
    );
    if resident_memory_supported != Some(true)
        || total_page_faults_supported != Some(true)
        || split_page_faults_supported.is_none()
    {
        blockers.insert("production_resource_profile_metric_capabilities_invalid".to_string());
    }

    let total_page_faults = nested_u64(evidence, &["execution", "total_page_faults"]);
    let max_total_page_faults = nested_u64(evidence, &["limits", "max_total_page_faults"]);
    if total_page_faults.is_none()
        || max_total_page_faults.is_none()
        || total_page_faults > max_total_page_faults
    {
        blockers.insert("production_resource_profile_total_page_faults_invalid".to_string());
    }

    for (metric, limit) in [
        ("minor_page_faults", "max_minor_page_faults"),
        ("major_page_faults", "max_major_page_faults"),
    ] {
        let measured = nested_u64(evidence, &["execution", metric]);
        let admitted = nested_u64(evidence, &["limits", limit]);
        if admitted.is_some()
            && (split_page_faults_supported != Some(true)
                || measured.is_none()
                || measured > admitted)
        {
            blockers.insert(format!("production_resource_profile_{metric}_invalid"));
        }
    }
    blockers.into_iter().collect()
}

#[cfg(test)]
mod tests;
