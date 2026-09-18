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

use super::{require_bool, require_nonzero, require_unsigned};
use crate::{
    ProductionGraphResourcePhase, ProductionGraphResourceRunEvidence,
    ProductionGraphResourceSummary,
};
use hawdb::ProductionQualificationIdentity;
use serde_json::Value;

pub(super) fn validate(
    artifact: &Value,
    profile: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    validate_lifecycle_memory(artifact, expected, blockers);

    let Some(run_values) = artifact.pointer("/resource_runs").and_then(Value::as_array) else {
        blockers.push("graph_resource_runs_missing".to_string());
        return;
    };
    let expected_run_count = artifact
        .pointer("/execution/measurement_runs")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok());
    if expected_run_count != Some(run_values.len()) {
        blockers.push("graph_resource_run_count_mismatch".to_string());
    }
    let runs = match serde_json::from_value::<Vec<ProductionGraphResourceRunEvidence>>(
        Value::Array(run_values.clone()),
    ) {
        Ok(runs) => runs,
        Err(_) => {
            blockers.push("graph_resource_run_decode_failed".to_string());
            return;
        }
    };

    let limits = ResourceLimits::from_profile(profile);
    if limits.require_fully_streamed != Some(true) {
        blockers.push("graph_resource_streaming_policy_not_strict".to_string());
    }

    for (index, run) in runs.iter().enumerate() {
        validate_run(index, run, limits, expected, blockers);
    }
    if let Some(last) = runs.last() {
        validate_tail_profile(profile, last, blockers);
    }

    let Some(summary_value) = artifact.pointer("/resource_summary") else {
        blockers.push("graph_resource_summary_missing".to_string());
        return;
    };
    let summary =
        match serde_json::from_value::<ProductionGraphResourceSummary>(summary_value.clone()) {
            Ok(summary) => summary,
            Err(_) => {
                blockers.push("graph_resource_summary_decode_failed".to_string());
                return;
            }
        };
    if summary != crate::production_graph::resource_summary(&runs) {
        blockers.push("graph_resource_summary_mismatch".to_string());
    }
    validate_execution_totals(artifact, &runs, blockers);
}

#[derive(Debug, Clone, Copy)]
struct ResourceLimits {
    max_steady_resident_bytes: Option<u64>,
    max_peak_resident_bytes: Option<u64>,
    max_total_page_faults: Option<u64>,
    max_minor_page_faults: Option<u64>,
    max_major_page_faults: Option<u64>,
    max_intermediate_rows: Option<u64>,
    max_intermediate_payload_bytes: Option<u64>,
    max_output_rows: Option<u64>,
    max_output_payload_bytes: Option<u64>,
    require_fully_streamed: Option<bool>,
    segment_cache_capacity_bytes: Option<u64>,
}

impl ResourceLimits {
    fn from_profile(profile: &Value) -> Self {
        let unsigned = |pointer| profile.pointer(pointer).and_then(Value::as_u64);
        Self {
            max_steady_resident_bytes: unsigned("/limits/max_steady_resident_bytes"),
            max_peak_resident_bytes: unsigned("/limits/max_peak_resident_bytes"),
            max_total_page_faults: unsigned("/limits/max_total_page_faults"),
            max_minor_page_faults: unsigned("/limits/max_minor_page_faults"),
            max_major_page_faults: unsigned("/limits/max_major_page_faults"),
            max_intermediate_rows: unsigned("/limits/max_intermediate_rows"),
            max_intermediate_payload_bytes: unsigned("/limits/max_intermediate_payload_bytes"),
            max_output_rows: unsigned("/limits/max_output_rows"),
            max_output_payload_bytes: unsigned("/limits/max_output_payload_bytes"),
            require_fully_streamed: profile
                .pointer("/limits/require_fully_streamed")
                .and_then(Value::as_bool),
            segment_cache_capacity_bytes: unsigned("/storage/segment_cache_capacity_bytes"),
        }
    }
}

fn validate_run(
    index: usize,
    run: &ProductionGraphResourceRunEvidence,
    limits: ResourceLimits,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    if run.run != index {
        blockers.push("graph_resource_run_sequence_invalid".to_string());
    }
    let expected_phase = if index == 0 {
        ProductionGraphResourcePhase::Cold
    } else {
        ProductionGraphResourcePhase::Warm
    };
    if run.phase != expected_phase {
        blockers.push("graph_resource_run_phase_invalid".to_string());
    }
    if !run.fully_streamed {
        blockers.push("graph_resource_run_not_fully_streamed".to_string());
    }
    if !within_usize_limit(run.output_rows, limits.max_output_rows) {
        blockers.push("graph_resource_run_output_row_limit_exceeded".to_string());
    }
    if !within_usize_limit(run.output_payload_bytes, limits.max_output_payload_bytes) {
        blockers.push("graph_resource_run_output_payload_limit_exceeded".to_string());
    }
    if !within_usize_limit(run.intermediate_rows, limits.max_intermediate_rows) {
        blockers.push("graph_resource_run_intermediate_row_limit_exceeded".to_string());
    }
    if !within_usize_limit(
        run.intermediate_payload_bytes,
        limits.max_intermediate_payload_bytes,
    ) {
        blockers.push("graph_resource_run_intermediate_payload_limit_exceeded".to_string());
    }
    if run.steady_resident_bytes.is_none() || run.peak_resident_bytes.is_none() {
        blockers.push("graph_resource_run_rss_missing".to_string());
    }
    if run.total_page_faults.is_none() {
        blockers.push("graph_resource_run_page_faults_missing".to_string());
    }
    if expected.target_os != "windows"
        && (run.minor_page_faults.is_none() || run.major_page_faults.is_none())
    {
        blockers.push("graph_resource_run_split_page_faults_missing".to_string());
    }
    for (within_limit, blocker) in [
        (
            within_optional_limit(run.steady_resident_bytes, limits.max_steady_resident_bytes),
            "graph_resource_run_steady_rss_limit_exceeded",
        ),
        (
            within_optional_limit(run.peak_resident_bytes, limits.max_peak_resident_bytes),
            "graph_resource_run_peak_rss_limit_exceeded",
        ),
        (
            within_optional_limit(run.total_page_faults, limits.max_total_page_faults),
            "graph_resource_run_total_page_fault_limit_exceeded",
        ),
        (
            within_optional_limit(run.minor_page_faults, limits.max_minor_page_faults),
            "graph_resource_run_minor_page_fault_limit_exceeded",
        ),
        (
            within_optional_limit(run.major_page_faults, limits.max_major_page_faults),
            "graph_resource_run_major_page_fault_limit_exceeded",
        ),
    ] {
        if !within_limit {
            blockers.push(blocker.to_string());
        }
    }
    if !limits.segment_cache_capacity_bytes.is_some_and(|capacity| {
        run.segment_cache_resident_bytes_before <= capacity
            && run.segment_cache_resident_bytes_after <= capacity
    }) {
        blockers.push("graph_resource_run_cache_capacity_exceeded".to_string());
    }
}

fn validate_tail_profile(
    profile: &Value,
    run: &ProductionGraphResourceRunEvidence,
    blockers: &mut Vec<String>,
) {
    let usize_metrics_match = [
        ("/execution/output_rows", run.output_rows),
        ("/execution/output_payload_bytes", run.output_payload_bytes),
        ("/execution/intermediate_rows", run.intermediate_rows),
        (
            "/execution/intermediate_payload_bytes",
            run.intermediate_payload_bytes,
        ),
    ]
    .into_iter()
    .all(|(pointer, expected)| {
        profile.pointer(pointer).and_then(Value::as_u64) == u64::try_from(expected).ok()
    });
    let optional_metrics_match = [
        (
            "/execution/steady_resident_bytes",
            run.steady_resident_bytes,
        ),
        ("/execution/peak_resident_bytes", run.peak_resident_bytes),
        ("/execution/total_page_faults", run.total_page_faults),
        ("/execution/minor_page_faults", run.minor_page_faults),
        ("/execution/major_page_faults", run.major_page_faults),
    ]
    .into_iter()
    .all(|(pointer, expected)| profile.pointer(pointer).and_then(Value::as_u64) == expected);
    let cache_metrics_match = [
        (
            "/storage/segment_cache_resident_bytes_before",
            run.segment_cache_resident_bytes_before,
        ),
        (
            "/storage/segment_cache_resident_bytes_after",
            run.segment_cache_resident_bytes_after,
        ),
        (
            "/storage/segment_cache_hit_count_delta",
            run.segment_cache_hit_count,
        ),
        (
            "/storage/segment_cache_miss_count_delta",
            run.segment_cache_miss_count,
        ),
        (
            "/storage/segment_cache_eviction_count_delta",
            run.segment_cache_eviction_count,
        ),
        (
            "/storage/segment_cache_admission_rejection_count_delta",
            run.segment_cache_admission_rejection_count,
        ),
    ]
    .into_iter()
    .all(|(pointer, expected)| profile.pointer(pointer).and_then(Value::as_u64) == Some(expected));
    let fully_streamed_matches = profile
        .pointer("/execution/fully_streamed")
        .and_then(Value::as_bool)
        == Some(run.fully_streamed);
    if !usize_metrics_match
        || !optional_metrics_match
        || !cache_metrics_match
        || !fully_streamed_matches
    {
        blockers.push("graph_resource_tail_profile_mismatch".to_string());
    }
}

fn validate_execution_totals(
    artifact: &Value,
    runs: &[ProductionGraphResourceRunEvidence],
    blockers: &mut Vec<String>,
) {
    let totals = runs.iter().fold([0u64; 4], |mut totals, run| {
        for (total, value) in totals.iter_mut().zip([
            run.output_rows,
            run.output_payload_bytes,
            run.intermediate_rows,
            run.intermediate_payload_bytes,
        ]) {
            *total = total.saturating_add(u64::try_from(value).unwrap_or(u64::MAX));
        }
        totals
    });
    for ((field, blocker), expected_total) in [
        ("output_rows", "graph_execution_output_rows_mismatch"),
        (
            "output_payload_bytes",
            "graph_execution_output_payload_mismatch",
        ),
        (
            "intermediate_rows",
            "graph_execution_intermediate_rows_mismatch",
        ),
        (
            "intermediate_payload_bytes",
            "graph_execution_intermediate_payload_mismatch",
        ),
    ]
    .into_iter()
    .zip(totals)
    {
        if artifact
            .pointer(&format!("/execution/{field}"))
            .and_then(Value::as_u64)
            != Some(expected_total)
        {
            blockers.push(blocker.to_string());
        }
    }
}

fn validate_lifecycle_memory(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
    blockers: &mut Vec<String>,
) {
    let Some(memory) = artifact.pointer("/lifecycle_process_memory") else {
        blockers.push("graph_lifecycle_memory_missing".to_string());
        return;
    };
    require_bool(
        memory,
        "/resident_memory_available",
        true,
        "graph_lifecycle_rss_metric_unavailable",
        blockers,
    );
    require_bool(
        memory,
        "/total_page_faults_available",
        true,
        "graph_lifecycle_page_fault_metric_unavailable",
        blockers,
    );
    let split_expected = expected.target_os != "windows";
    require_bool(
        memory,
        "/split_page_faults_available",
        split_expected,
        "graph_lifecycle_split_page_fault_capability_mismatch",
        blockers,
    );
    require_nonzero(
        memory,
        "/steady_resident_bytes",
        "graph_lifecycle_steady_rss_missing",
        blockers,
    );
    require_nonzero(
        memory,
        "/peak_resident_bytes",
        "graph_lifecycle_peak_rss_missing",
        blockers,
    );
    require_unsigned(
        memory,
        "/total_page_faults",
        "graph_lifecycle_page_faults_missing",
        blockers,
    );
    if split_expected {
        require_unsigned(
            memory,
            "/minor_page_faults",
            "graph_lifecycle_minor_page_faults_missing",
            blockers,
        );
        require_unsigned(
            memory,
            "/major_page_faults",
            "graph_lifecycle_major_page_faults_missing",
            blockers,
        );
    }
}

fn within_optional_limit(actual: Option<u64>, limit: Option<u64>) -> bool {
    match (actual, limit) {
        (Some(actual), Some(limit)) => actual <= limit,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => true,
    }
}

fn within_usize_limit(actual: usize, limit: Option<u64>) -> bool {
    limit.is_some_and(|limit| u64::try_from(actual).is_ok_and(|actual| actual <= limit))
}
