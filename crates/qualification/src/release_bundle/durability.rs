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
    require_bool, require_empty_array, require_nonzero, require_string, validate_common_artifact,
    validate_exact_binding, PRODUCTION_RELEASE_CONTROL_EVIDENCE_PROTOCOL,
    REQUIRED_PRODUCTION_RELEASE_CONTROLS,
};
use hawdb::{ProductionQualificationIdentity, STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL};
use serde_json::Value;
use std::collections::BTreeSet;

const CRASH_POINTS: [&str; 5] = [
    "before_wal_append",
    "after_wal_append",
    "after_wal_sync",
    "during_checkpoint_publication",
    "after_manifest_publication",
];

pub(super) fn validate_crash_recovery(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    require_string(
        artifact,
        "/protocol",
        STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
        "crash_recovery_protocol_mismatch",
        &mut blockers,
    );
    require_bool(
        artifact,
        "/ready",
        true,
        "crash_recovery_reported_not_ready",
        &mut blockers,
    );
    require_empty_array(
        artifact,
        "/blocker_codes",
        "crash_recovery_blockers_present",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    if artifact
        .pointer("/expected_identity")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .as_ref()
        != Some(expected)
    {
        blockers.push("crash_recovery_expected_identity_mismatch".to_string());
    }
    let repetitions = artifact
        .pointer("/required_repetitions")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    if repetitions == 0 {
        blockers.push("crash_recovery_repetitions_missing".to_string());
    }
    let Some(cases) = artifact.pointer("/cases").and_then(Value::as_array) else {
        blockers.push("crash_recovery_cases_missing".to_string());
        return deduplicate(blockers);
    };
    if artifact.pointer("/case_count").and_then(Value::as_u64) != Some(cases.len() as u64) {
        blockers.push("crash_recovery_case_count_mismatch".to_string());
    }
    let mut observed = BTreeSet::new();
    for case in cases {
        let point = case.pointer("/point").and_then(Value::as_str);
        let repetition = case.pointer("/repetition").and_then(Value::as_u64);
        let Some((point, repetition)) = point.zip(repetition) else {
            blockers.push("crash_recovery_case_identity_invalid".to_string());
            continue;
        };
        if !CRASH_POINTS.contains(&point) || repetition >= repetitions {
            blockers.push("crash_recovery_case_identity_invalid".to_string());
        }
        if !observed.insert((point.to_string(), repetition)) {
            blockers.push("crash_recovery_case_duplicate".to_string());
        }
        for (pointer, code) in [
            ("/ready", "crash_recovery_case_not_ready"),
            ("/process_terminated", "crash_process_not_terminated"),
            ("/whole_batch_recovered", "crash_batch_not_atomic"),
            ("/replay_lsn_present", "crash_replay_lsn_missing"),
            (
                "/relationship_endpoints_valid",
                "crash_relationship_integrity_failed",
            ),
            (
                "/projection_watermark_valid",
                "crash_projection_watermark_failed",
            ),
            (
                "/artifact_generation_valid",
                "crash_artifact_generation_failed",
            ),
        ] {
            require_bool(case, pointer, true, code, &mut blockers);
        }
        if case.pointer("/commit_epoch").and_then(Value::as_u64)
            != case
                .pointer("/recovered_commit_epoch")
                .and_then(Value::as_u64)
        {
            blockers.push("crash_commit_epoch_mismatch".to_string());
        }
        let recovered = case
            .pointer("/recovered_batch_present")
            .and_then(Value::as_bool);
        if point == "before_wal_append" && recovered != Some(false) {
            blockers.push("crash_unwritten_batch_recovered".to_string());
        }
        if matches!(
            point,
            "after_wal_sync" | "during_checkpoint_publication" | "after_manifest_publication"
        ) && recovered != Some(true)
        {
            blockers.push("crash_committed_batch_missing".to_string());
        }
    }
    for point in CRASH_POINTS {
        for repetition in 0..repetitions {
            if !observed.contains(&(point.to_string(), repetition)) {
                blockers.push("crash_recovery_matrix_incomplete".to_string());
                break;
            }
        }
    }
    deduplicate(blockers)
}

pub(super) fn validate_release_controls(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_RELEASE_CONTROL_EVIDENCE_PROTOCOL,
        "exact_revision_release_controls",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    require_string(
        artifact,
        "/source_revision",
        &expected.source_revision,
        "release_control_revision_mismatch",
        &mut blockers,
    );
    if !valid_full_source_revision(&expected.source_revision) {
        blockers.push("release_control_revision_not_full".to_string());
    }
    let Some(checks) = artifact.pointer("/checks").and_then(Value::as_array) else {
        blockers.push("release_control_checks_missing".to_string());
        return deduplicate(blockers);
    };
    let mut observed = BTreeSet::new();
    for check in checks {
        let Some(name) = check.pointer("/name").and_then(Value::as_str) else {
            blockers.push("release_control_name_missing".to_string());
            continue;
        };
        if !observed.insert(name.to_string()) {
            blockers.push("release_control_duplicate".to_string());
        }
        require_string(
            check,
            "/source_revision",
            &expected.source_revision,
            "release_control_check_revision_mismatch",
            &mut blockers,
        );
        require_string(
            check,
            "/conclusion",
            "success",
            "release_control_check_failed",
            &mut blockers,
        );
        if !check
            .pointer("/artifact_sha256")
            .and_then(Value::as_str)
            .is_some_and(valid_sha256)
        {
            blockers.push("release_control_artifact_digest_invalid".to_string());
        }
    }
    for required in REQUIRED_PRODUCTION_RELEASE_CONTROLS {
        if !observed.contains(required) {
            blockers.push(format!("release_control_{required}_missing"));
        }
    }
    require_nonzero(
        artifact,
        "/evidence_binding/generated_at_unix_seconds",
        "release_control_timestamp_missing",
        &mut blockers,
    );
    deduplicate(blockers)
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

pub(super) fn valid_full_source_revision(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn deduplicate(mut blockers: Vec<String>) -> Vec<String> {
    blockers.sort();
    blockers.dedup();
    blockers
}
