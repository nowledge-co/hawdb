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

use super::graph_search::{deduplicate, validate_runtime};
use super::{
    require_bool, require_empty_array, require_nonzero, validate_common_artifact,
    validate_exact_binding, ProductionArtifactAssessment, ProductionMorselMatrixArtifactAssessment,
    ProductionReleaseQualificationPolicy,
};
use crate::{
    ProductionMorselMatrixPolicy, PRODUCTION_BLOCKING_QUALIFICATION_PROTOCOL,
    PRODUCTION_MORSEL_PROFILE_PROTOCOL, REQUIRED_PRODUCTION_MORSEL_WORKERS,
};
use hawdb::ProductionQualificationIdentity;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn evaluate_morsel_matrix(
    artifacts: &[Value],
    expected: &ProductionQualificationIdentity,
    policy: ProductionReleaseQualificationPolicy,
) -> ProductionMorselMatrixArtifactAssessment {
    let mut matrix_blockers = Vec::new();
    let mut profiles = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    let mut query_identity = None;
    let mut profile_reports = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let mut blockers = validate_morsel_profile(artifact, expected, policy.morsel);
        let workers = artifact
            .pointer("/expected_workers")
            .and_then(Value::as_u64)
            .and_then(|workers| usize::try_from(workers).ok());
        match workers {
            Some(workers)
                if REQUIRED_PRODUCTION_MORSEL_WORKERS.contains(&workers)
                    && !profiles.contains_key(&workers) =>
            {
                profiles.insert(workers, artifact);
            }
            Some(_) => blockers.push("morsel_worker_profile_duplicate_or_invalid".to_string()),
            None => blockers.push("morsel_worker_profile_missing".to_string()),
        }
        match artifact.pointer("/process_id").and_then(Value::as_u64) {
            Some(process_id) if process_id > 0 && process_ids.insert(process_id) => {}
            _ => blockers.push("morsel_profiles_not_process_isolated".to_string()),
        }
        let current_query_identity = artifact.pointer("/query_identity");
        match (&query_identity, current_query_identity) {
            (Some(expected), Some(current)) if expected != current => {
                blockers.push("morsel_query_identity_mismatch".to_string());
            }
            (None, Some(current)) => query_identity = Some(current.clone()),
            (_, None) => blockers.push("morsel_query_identity_missing".to_string()),
            _ => {}
        }
        let assessment = ProductionArtifactAssessment::evaluated(
            "representative_production_morsel_profile",
            artifact,
            blockers,
        );
        if !assessment.ready {
            matrix_blockers.push("morsel_profile_not_ready".to_string());
        }
        profile_reports.push(assessment);
    }
    if profiles.keys().copied().collect::<Vec<_>>() != REQUIRED_PRODUCTION_MORSEL_WORKERS {
        matrix_blockers.push("required_worker_matrix_missing".to_string());
    }
    for pair in REQUIRED_PRODUCTION_MORSEL_WORKERS.windows(2) {
        if let (Some(previous), Some(current)) = (profiles.get(&pair[0]), profiles.get(&pair[1])) {
            validate_morsel_scaling(
                previous,
                current,
                pair[1],
                policy.morsel,
                &mut matrix_blockers,
            );
        }
    }
    matrix_blockers.sort();
    matrix_blockers.dedup();
    ProductionMorselMatrixArtifactAssessment {
        ready: matrix_blockers.is_empty(),
        blocker_codes: matrix_blockers,
        profile_reports,
    }
}

pub(super) fn validate_blocking(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_BLOCKING_QUALIFICATION_PROTOCOL,
        "active_route_blocking_operators",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    validate_runtime(artifact, "/runtime", &mut blockers);
    for field in [
        "active_bytes",
        "pending_write_bytes",
        "active_runs",
        "orphan_cleanup_failures",
        "run_delete_failures",
    ] {
        if artifact
            .pointer(&format!("/spill_pool/{field}"))
            .and_then(Value::as_u64)
            != Some(0)
        {
            blockers.push(format!("blocking_spill_pool_{field}_nonzero"));
        }
    }
    let Some(cases) = artifact.pointer("/cases").and_then(Value::as_array) else {
        blockers.push("blocking_route_cases_missing".to_string());
        return deduplicate(blockers);
    };
    let mut kinds = BTreeSet::new();
    let mut routes = BTreeSet::new();
    for case in cases {
        if let Some(kind) = case.pointer("/operator_kind").and_then(Value::as_str) {
            kinds.insert(kind);
        }
        if !case
            .pointer("/route_name")
            .and_then(Value::as_str)
            .is_some_and(|route| !route.is_empty() && routes.insert(route))
        {
            blockers.push("blocking_route_name_duplicate_or_missing".to_string());
        }
        require_bool(
            case,
            "/ready",
            true,
            "blocking_route_not_ready",
            &mut blockers,
        );
        require_bool(
            case,
            "/fully_streamed",
            true,
            "blocking_route_not_streamed",
            &mut blockers,
        );
        require_empty_array(
            case,
            "/blocker_codes",
            "blocking_route_blockers_present",
            &mut blockers,
        );
        let input = case.pointer("/input_rows").and_then(Value::as_u64);
        let minimum = case.pointer("/minimum_input_rows").and_then(Value::as_u64);
        if !input
            .zip(minimum)
            .is_some_and(|(input, minimum)| minimum > 0 && input >= minimum)
        {
            blockers.push("blocking_route_cardinality_below_minimum".to_string());
        }
        let peak = case.pointer("/peak_tracked_bytes").and_then(Value::as_u64);
        let budget = case.pointer("/budget_bytes").and_then(Value::as_u64);
        if !peak
            .zip(budget)
            .is_some_and(|(peak, budget)| budget > 0 && peak <= budget)
        {
            blockers.push("blocking_route_memory_limit_exceeded".to_string());
        }
        for (used_pointer, limit_pointer, code) in [
            (
                "/spilled_bytes",
                "/max_spill_bytes",
                "blocking_route_spill_byte_limit_exceeded",
            ),
            (
                "/spill_run_count",
                "/max_spill_runs",
                "blocking_route_spill_run_limit_exceeded",
            ),
        ] {
            let used = case.pointer(used_pointer).and_then(Value::as_u64);
            let limit = case.pointer(limit_pointer).and_then(Value::as_u64);
            if !used.zip(limit).is_some_and(|(used, limit)| used <= limit) {
                blockers.push(code.to_string());
            }
        }
        if !case
            .pointer("/disposition")
            .and_then(Value::as_str)
            .is_some_and(|disposition| {
                matches!(
                    disposition,
                    "external_spill_observed" | "in_memory_within_admission"
                )
            })
        {
            blockers.push("blocking_route_disposition_missing".to_string());
        }
    }
    for kind in ["distinct", "cartesian_build"] {
        if !kinds.contains(kind) {
            blockers.push(format!("blocking_{kind}_route_missing"));
        }
    }
    deduplicate(blockers)
}

fn validate_morsel_profile(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    policy: ProductionMorselMatrixPolicy,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        PRODUCTION_MORSEL_PROFILE_PROTOCOL,
        "representative_production_morsel_profile",
        &mut blockers,
    );
    validate_exact_binding(artifact, "/evidence_binding", expected, &mut blockers);
    let workers = artifact
        .pointer("/expected_workers")
        .and_then(Value::as_u64);
    let warmups = artifact
        .pointer("/execution/warmup_runs")
        .and_then(Value::as_u64);
    let measurements = artifact
        .pointer("/execution/measurement_runs")
        .and_then(Value::as_u64);
    let streamed = artifact
        .pointer("/execution/fully_streamed_runs")
        .and_then(Value::as_u64);
    if !warmups.is_some_and(|warmups| warmups >= 3) {
        blockers.push("morsel_warmup_samples_insufficient".to_string());
    }
    if !measurements.is_some_and(|measurements| measurements >= 100) {
        blockers.push("morsel_measurement_samples_insufficient".to_string());
    }
    if measurements != streamed {
        blockers.push("morsel_not_fully_streamed".to_string());
    }
    for (pointer, code) in [
        (
            "/execution/morsel_max_admitted_workers",
            "morsel_admitted_workers_mismatch",
        ),
        (
            "/execution/morsel_peak_active_workers",
            "morsel_active_workers_mismatch",
        ),
    ] {
        if artifact.pointer(pointer).and_then(Value::as_u64) != workers {
            blockers.push(code.to_string());
        }
    }
    if artifact
        .pointer("/runtime_shape/effective_cpu_slots")
        .and_then(Value::as_u64)
        .zip(workers)
        .is_none_or(|(slots, workers)| slots < workers)
    {
        blockers.push("morsel_effective_cpu_slots_insufficient".to_string());
    }
    for pointer in [
        "/execution/rows_per_second",
        "/execution/latency/sample_count",
        "/execution/peak_resident_bytes",
    ] {
        require_nonzero(
            artifact,
            pointer,
            "morsel_measurement_missing",
            &mut blockers,
        );
    }
    require_bool(
        artifact,
        "/cancellation/cancellation_observed",
        true,
        "morsel_cancellation_not_observed",
        &mut blockers,
    );
    let cancellation = artifact
        .pointer("/cancellation/latency_micros")
        .and_then(Value::as_u64);
    if !cancellation.is_some_and(|latency| latency <= policy.max_cancellation_latency_micros) {
        blockers.push("morsel_cancellation_latency_regression".to_string());
    }
    validate_runtime(artifact, "/runtime", &mut blockers);
    deduplicate(blockers)
}

fn validate_morsel_scaling(
    previous: &Value,
    current: &Value,
    workers: usize,
    policy: ProductionMorselMatrixPolicy,
    blockers: &mut Vec<String>,
) {
    let previous_throughput = previous
        .pointer("/execution/rows_per_second")
        .and_then(Value::as_u64);
    let current_throughput = current
        .pointer("/execution/rows_per_second")
        .and_then(Value::as_u64);
    if !previous_throughput
        .zip(current_throughput)
        .is_some_and(|(previous, current)| {
            ratio_at_least(current, previous, policy.min_throughput_gain_per_million)
        })
    {
        blockers.push(format!("worker_{workers}_throughput_did_not_improve"));
    }
    for (pointer, limit, code) in [
        (
            "/execution/latency/p99_micros",
            policy.max_p99_regression_per_million,
            "p99_regression",
        ),
        (
            "/execution/peak_resident_bytes",
            policy.max_peak_rss_regression_per_million,
            "peak_rss_regression",
        ),
    ] {
        let previous_value = previous.pointer(pointer).and_then(Value::as_u64);
        let current_value = current.pointer(pointer).and_then(Value::as_u64);
        if !previous_value
            .zip(current_value)
            .is_some_and(|(previous, current)| regression_within(current, previous, limit))
        {
            blockers.push(format!("worker_{workers}_{code}"));
        }
    }
}

fn ratio_at_least(current: u64, previous: u64, minimum_per_million: u32) -> bool {
    previous > 0
        && u128::from(current).saturating_mul(1_000_000)
            >= u128::from(previous).saturating_mul(u128::from(minimum_per_million))
}

fn regression_within(current: u64, previous: u64, maximum_per_million: u32) -> bool {
    if previous == 0 {
        return current == 0;
    }
    u128::from(current).saturating_mul(1_000_000)
        <= u128::from(previous).saturating_mul(u128::from(maximum_per_million))
}
