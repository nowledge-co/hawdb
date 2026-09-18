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
    require_bool, require_empty_array, require_string, validate_common_artifact,
    validate_exact_binding,
};
use crate::{
    CONTENT_STORE_512_MIB_CAPABILITY_BYTES, CONTENT_STORE_SHARED_HOST_8_GIB_BYTES,
    CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES,
    CONTENT_STORE_SHARED_HOST_NOMINAL_AVAILABLE_BYTES,
    CONTENT_STORE_SHARED_HOST_NOMINAL_MIN_BUDGET_BYTES,
    PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
};
use hawdb::{ProductionQualificationIdentity, RuntimeGovernorConfig};
use serde_json::Value;

pub(super) fn validate_memory_profiles(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
        "production_content_store_memory_profiles",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    let shared_host_profile = artifact.pointer("/shared_host_8_gib");
    let capability = artifact.pointer("/capability_512_mib");
    let Some(shared_host) = shared_host_profile else {
        blockers.push("content_store_shared_host_memory_profile_missing".to_string());
        return deduplicate(blockers);
    };
    let Some(capability) = capability else {
        blockers.push("content_store_512_mib_memory_profile_missing".to_string());
        return deduplicate(blockers);
    };
    validate_shared_host_profile(shared_host, &mut blockers);
    validate_capability_profile(capability, &mut blockers);
    for pointer in [
        "/observed_effective_limit_bytes",
        "/observed_effective_available_bytes",
    ] {
        if shared_host.pointer(pointer) != capability.pointer(pointer) {
            blockers.push("content_store_memory_profile_snapshot_mismatch".to_string());
        }
    }
    deduplicate(blockers)
}

fn validate_shared_host_profile(profile: &Value, blockers: &mut Vec<String>) {
    validate_profile_header(
        profile,
        "shared_host8_gib",
        "content_store_shared_host",
        blockers,
    );
    let fraction = u64::from(RuntimeGovernorConfig::shared_host().memory_fraction_per_million);
    let available = unsigned(profile, "/observed_effective_available_bytes");
    let expected_budget = available.map(|available| {
        scale_memory(available, fraction).min(CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES)
    });
    if unsigned(profile, "/required_effective_limit_bytes")
        != Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES)
        || unsigned(profile, "/observed_effective_limit_bytes")
            != Some(CONTENT_STORE_SHARED_HOST_8_GIB_BYTES)
        || !is_null(profile, "/configured_memory_ceiling_bytes")
        || unsigned(profile, "/nominal_available_threshold_bytes")
            != Some(CONTENT_STORE_SHARED_HOST_NOMINAL_AVAILABLE_BYTES)
        || unsigned(profile, "/memory_fraction_per_million") != Some(fraction)
        || unsigned(profile, "/memory_capacity_bytes")
            != Some(CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES)
        || unsigned(profile, "/expected_capacity_bytes")
            != Some(CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES)
        || unsigned(profile, "/memory_budget_bytes") != expected_budget
        || unsigned(profile, "/expected_dynamic_budget_bytes") != expected_budget
    {
        blockers.push("content_store_shared_host_memory_policy_invalid".to_string());
    }
    let nominal = expected_budget.is_some_and(|budget| {
        (CONTENT_STORE_SHARED_HOST_NOMINAL_MIN_BUDGET_BYTES
            ..=CONTENT_STORE_SHARED_HOST_MAX_CAPACITY_BYTES)
            .contains(&budget)
    });
    if boolean(profile, "/nominal_budget_range_observed") != Some(nominal) {
        blockers.push("content_store_shared_host_nominal_range_invalid".to_string());
    }
}

fn validate_capability_profile(profile: &Value, blockers: &mut Vec<String>) {
    validate_profile_header(
        profile,
        "capability512_mib",
        "content_store_512_mib",
        blockers,
    );
    let fraction = u64::from(RuntimeGovernorConfig::shared_host().memory_fraction_per_million);
    let available = unsigned(profile, "/observed_effective_available_bytes");
    let expected_budget = available.map(|available| {
        scale_memory(available, fraction).min(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
    });
    if !is_null(profile, "/required_effective_limit_bytes")
        || unsigned(profile, "/configured_memory_ceiling_bytes")
            != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
        || !is_null(profile, "/nominal_available_threshold_bytes")
        || boolean(profile, "/nominal_budget_range_observed") != Some(false)
        || unsigned(profile, "/memory_fraction_per_million") != Some(fraction)
        || unsigned(profile, "/memory_capacity_bytes")
            != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
        || unsigned(profile, "/expected_capacity_bytes")
            != Some(CONTENT_STORE_512_MIB_CAPABILITY_BYTES)
        || unsigned(profile, "/memory_budget_bytes") != expected_budget
        || unsigned(profile, "/expected_dynamic_budget_bytes") != expected_budget
    {
        blockers.push("content_store_512_mib_memory_policy_invalid".to_string());
    }
}

fn validate_profile_header(profile: &Value, kind: &str, prefix: &str, blockers: &mut Vec<String>) {
    require_string(
        profile,
        "/profile_kind",
        kind,
        &format!("{prefix}_memory_profile_kind_invalid"),
        blockers,
    );
    require_bool(
        profile,
        "/ready",
        true,
        &format!("{prefix}_memory_profile_not_ready"),
        blockers,
    );
    require_empty_array(
        profile,
        "/blocker_codes",
        &format!("{prefix}_memory_profile_blockers_present"),
        blockers,
    );
}

fn scale_memory(bytes: u64, fraction_per_million: u64) -> u64 {
    (u128::from(bytes) * u128::from(fraction_per_million) / 1_000_000).min(u128::from(u64::MAX))
        as u64
}

fn unsigned(value: &Value, pointer: &str) -> Option<u64> {
    value.pointer(pointer).and_then(Value::as_u64)
}

fn boolean(value: &Value, pointer: &str) -> Option<bool> {
    value.pointer(pointer).and_then(Value::as_bool)
}

fn is_null(value: &Value, pointer: &str) -> bool {
    value.pointer(pointer).is_some_and(Value::is_null)
}

fn deduplicate(mut blockers: Vec<String>) -> Vec<String> {
    blockers.sort();
    blockers.dedup();
    blockers
}
