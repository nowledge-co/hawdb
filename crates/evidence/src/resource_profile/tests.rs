use super::{
    production_resource_profile_blocker_codes, production_resource_profile_ready,
    STORAGE_RESOURCE_PROFILE_PROTOCOL,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

const METRICS: &[(&str, &str)] = &[
    ("steady_resident_bytes", "max_steady_resident_bytes"),
    ("peak_resident_bytes", "max_peak_resident_bytes"),
    ("intermediate_rows", "max_intermediate_rows"),
    (
        "intermediate_payload_bytes",
        "max_intermediate_payload_bytes",
    ),
    ("output_rows", "max_output_rows"),
    ("output_payload_bytes", "max_output_payload_bytes"),
    ("total_page_faults", "max_total_page_faults"),
];

fn ready_profile() -> Value {
    let identity = json!({
        "source_revision": "revision", "rust_toolchain": "toolchain",
        "target_os": "test-os", "target_arch": "test-arch",
        "configuration_digest": "configuration", "deployment_profile": "profile",
        "dataset_fingerprint": "dataset", "enabled_features": ["feature"],
        "durable_format_version": 1, "schema_version": 1,
        "policy_version": crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        "canonical_graph_commit_epoch": 7
    });
    json!({
        "protocol": "skein-storage-resource-profile-v2", "protocol_version": 2,
        "present": true, "ready": true, "resource_ready": true, "blocker_codes": [],
        "identity_matches_expected": true, "canonical_graph_commit_epoch": 7,
        "evidence_binding": {"identity": identity, "generated_at_unix_seconds": 1},
        "expected_identity": identity,
        "storage": {
            "durable": true, "out_of_core": true, "canonical_exceeds_cache": true,
            "delta_within_budget": true, "canonical_artifact_bytes": 4096,
            "segment_cache_capacity_bytes": 1024, "segment_cache_resident_bytes_after": 256
        },
        "limits": {
            "min_canonical_artifact_bytes": 2048,
            "max_steady_resident_bytes": 64, "max_peak_resident_bytes": 64,
            "max_intermediate_rows": 64, "max_intermediate_payload_bytes": 64,
            "max_output_rows": 64, "max_output_payload_bytes": 64,
            "max_total_page_faults": 64, "max_minor_page_faults": 64,
            "max_major_page_faults": 64, "require_fully_streamed": true
        },
        "execution": {
            "fully_streamed": true, "start_resident_bytes": 16,
            "start_peak_resident_bytes": 16, "steady_resident_growth_bytes": 0,
            "lifetime_peak_resident_growth_bytes": 0, "steady_resident_bytes": 16,
            "peak_resident_bytes": 16, "intermediate_rows": 16,
            "intermediate_payload_bytes": 16, "output_rows": 16, "output_payload_bytes": 16,
            "total_page_faults": 16, "minor_page_faults": 8, "major_page_faults": 8,
            "metric_capabilities": {"resident_memory": true, "total_page_faults": true, "split_page_faults": true}
        }
    })
}

fn assert_codes(profile: &Value, suffixes: impl IntoIterator<Item = String>) {
    let expected: Vec<_> = suffixes
        .into_iter()
        .map(|suffix| format!("production_resource_profile_{suffix}"))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(production_resource_profile_blocker_codes(profile), expected);
    assert_eq!(
        production_resource_profile_ready(profile),
        expected.is_empty()
    );
}

// Each edit changes one independent requirement of the known-ready fixture.
fn required_fields() -> Vec<(String, Vec<String>)> {
    let mut fields = Vec::new();
    let mut add = |path: &str, codes: &[&str]| {
        fields.push((
            path.to_string(),
            codes.iter().map(|code| code.to_string()).collect(),
        ));
    };
    for (path, code) in [
        ("/protocol", "protocol_mismatch"),
        ("/protocol_version", "protocol_mismatch"),
        ("/present", "missing"),
        ("/ready", "not_ready"),
        ("/resource_ready", "resource_not_ready"),
        ("/blocker_codes", "has_blockers"),
        ("/identity_matches_expected", "identity_invalid"),
        ("/canonical_graph_commit_epoch", "identity_invalid"),
        (
            "/evidence_binding/generated_at_unix_seconds",
            "identity_invalid",
        ),
        ("/expected_identity", "identity_invalid"),
        (
            "/limits/min_canonical_artifact_bytes",
            "storage_budget_invalid",
        ),
        ("/limits/require_fully_streamed", "streaming_invalid"),
        ("/execution/fully_streamed", "streaming_invalid"),
    ] {
        add(path, &[code]);
    }
    for field in [
        "source_revision",
        "rust_toolchain",
        "target_os",
        "target_arch",
        "configuration_digest",
        "deployment_profile",
        "dataset_fingerprint",
        "enabled_features",
        "durable_format_version",
        "schema_version",
        "policy_version",
        "canonical_graph_commit_epoch",
    ] {
        add(
            &format!("/evidence_binding/identity/{field}"),
            &["identity_invalid"],
        );
    }
    for field in [
        "durable",
        "out_of_core",
        "canonical_exceeds_cache",
        "delta_within_budget",
        "canonical_artifact_bytes",
        "segment_cache_capacity_bytes",
        "segment_cache_resident_bytes_after",
    ] {
        add(&format!("/storage/{field}"), &["storage_budget_invalid"]);
    }
    for field in [
        "start_resident_bytes",
        "start_peak_resident_bytes",
        "steady_resident_growth_bytes",
        "lifetime_peak_resident_growth_bytes",
    ] {
        add(&format!("/execution/{field}"), &["resident_growth_missing"]);
    }
    for (metric, limit) in METRICS {
        add(
            &format!("/execution/{metric}"),
            &[&format!("{metric}_invalid")],
        );
        add(&format!("/limits/{limit}"), &[&format!("{metric}_invalid")]);
    }
    for metric in ["minor_page_faults", "major_page_faults"] {
        add(
            &format!("/execution/{metric}"),
            &[&format!("{metric}_invalid")],
        );
    }
    for capability in ["resident_memory", "total_page_faults"] {
        add(
            &format!("/execution/metric_capabilities/{capability}"),
            &["metric_capabilities_invalid"],
        );
    }
    add(
        "/execution/metric_capabilities/split_page_faults",
        &[
            "metric_capabilities_invalid",
            "minor_page_faults_invalid",
            "major_page_faults_invalid",
        ],
    );
    fields
}

fn remove(profile: &mut Value, pointer: &str) {
    let (parent, key) = pointer.rsplit_once('/').unwrap();
    profile
        .pointer_mut(parent)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove(key);
}

#[test]
fn resource_profile_required_fields_fail_closed_without_coercion() {
    assert_eq!(
        STORAGE_RESOURCE_PROFILE_PROTOCOL,
        "skein-storage-resource-profile-v2"
    );
    assert_codes(&ready_profile(), []);
    for (path, codes) in required_fields() {
        let mut missing = ready_profile();
        remove(&mut missing, &path);
        assert_codes(&missing, codes.clone());
        for invalid in [
            Value::Null,
            json!(-1),
            json!("invalid"),
            json!({}),
            json!([]),
        ] {
            let mut profile = ready_profile();
            let unchanged = profile.pointer(&path) == Some(&invalid);
            *profile.pointer_mut(&path).unwrap() = invalid;
            assert_codes(&profile, if unchanged { Vec::new() } else { codes.clone() });
        }
    }
}

#[test]
fn resource_profile_metric_limits_are_inclusive_and_keep_u64_precision() {
    for (metric, limit) in METRICS {
        for maximum in [0, 1, 64, u64::MAX - 1, u64::MAX] {
            for measured in [0, maximum, maximum.saturating_add(1)] {
                let mut profile = ready_profile();
                profile["execution"][metric] = measured.into();
                profile["limits"][limit] = maximum.into();
                assert_codes(
                    &profile,
                    (measured > maximum).then(|| format!("{metric}_invalid")),
                );
            }
        }
    }
}

#[test]
fn resource_profile_identity_is_recomputed_even_when_ready_flags_agree() {
    for field in [
        "source_revision",
        "rust_toolchain",
        "target_os",
        "target_arch",
        "configuration_digest",
        "deployment_profile",
        "dataset_fingerprint",
    ] {
        let mut profile = ready_profile();
        profile["expected_identity"][field] = json!(" \t ");
        profile["evidence_binding"]["identity"][field] = json!(" \t ");
        assert_codes(&profile, ["identity_invalid".into()]);
    }
    for field in ["durable_format_version", "schema_version", "policy_version"] {
        let mut profile = ready_profile();
        profile["expected_identity"][field] = json!(0);
        profile["evidence_binding"]["identity"][field] = json!(0);
        assert_codes(&profile, ["identity_invalid".into()]);
    }
    let mut profile = ready_profile();
    profile["evidence_binding"]["generated_at_unix_seconds"] = json!(0);
    assert_codes(&profile, ["identity_invalid".into()]);
    for identity in ["expected_identity", "evidence_binding"] {
        let mut profile = ready_profile();
        if identity == "expected_identity" {
            profile[identity]["canonical_graph_commit_epoch"] = json!(8);
        } else {
            profile[identity]["identity"]["canonical_graph_commit_epoch"] = json!(8);
        }
        assert_codes(&profile, ["identity_invalid".into()]);
    }
}

#[test]
fn resource_profile_storage_bounds_are_recomputed_independently_of_flags() {
    for (canonical, minimum, capacity, resident, ready) in [
        (4096, 4096, 1024, 1024, true),
        (4096, 4097, 1024, 256, false),
        (1024, 1024, 1024, 256, false),
        (4096, 2048, 1024, 1025, false),
        (u64::MAX, u64::MAX, u64::MAX - 1, u64::MAX - 1, true),
    ] {
        let mut profile = ready_profile();
        profile["storage"]["canonical_artifact_bytes"] = canonical.into();
        profile["storage"]["segment_cache_capacity_bytes"] = capacity.into();
        profile["storage"]["segment_cache_resident_bytes_after"] = resident.into();
        profile["limits"]["min_canonical_artifact_bytes"] = minimum.into();
        assert_codes(&profile, (!ready).then(|| "storage_budget_invalid".into()));
    }
}

#[test]
fn resource_profile_platform_fault_limits_and_streaming_remain_explicit() {
    let mut profile = ready_profile();
    profile["execution"]["metric_capabilities"]["split_page_faults"] = json!(false);
    for metric in ["minor_page_faults", "major_page_faults"] {
        profile["execution"][metric] = Value::Null;
        profile["limits"][format!("max_{metric}")] = Value::Null;
    }
    assert_codes(&profile, []);
    profile["limits"]["max_minor_page_faults"] = json!(0);
    assert_codes(&profile, ["minor_page_faults_invalid".into()]);
    profile["limits"]["max_minor_page_faults"] = Value::Null;
    profile["execution"]["metric_capabilities"]["total_page_faults"] = json!(false);
    assert_codes(&profile, ["metric_capabilities_invalid".into()]);
    for required in [false, true] {
        let mut profile = ready_profile();
        profile["limits"]["require_fully_streamed"] = required.into();
        profile["execution"]["fully_streamed"] = json!(false);
        assert_codes(&profile, required.then(|| "streaming_invalid".into()));
    }
    for invalid in [
        json!([null]),
        json!([1]),
        json!(["blocker"]),
        json!(["blocker", null]),
    ] {
        let mut profile = ready_profile();
        profile["blocker_codes"] = invalid;
        assert_codes(&profile, ["has_blockers".into()]);
    }
}

fn campaign(seeds: u64, steps: usize) {
    let fields = required_fields();
    for seed in 1..=seeds {
        let mut state = seed;
        for step in 0..steps {
            let mut profile = ready_profile();
            let mut codes = Vec::new();
            for _ in 0..=step % 7 {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let (path, expected) = &fields[(state % fields.len() as u64) as usize];
                if state & 1 == 0 {
                    remove(&mut profile, path);
                } else if let Some(value) = profile.pointer_mut(path) {
                    *value = Value::Null;
                }
                codes.extend(expected.iter().cloned());
            }
            assert_codes(&profile, codes);
        }
    }
    eprintln!(
        "resource profile: {seeds} seeds, {} complete blocker/readiness comparisons",
        seeds as usize * steps
    );
}

#[test]
fn resource_profile_differential_smoke() {
    campaign(4, 16);
}

#[test]
#[ignore = "deterministic local resource-profile evidence campaign"]
fn resource_profile_differential_campaign() {
    campaign(128, 64);
}
