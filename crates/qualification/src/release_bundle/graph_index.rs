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
    graph_search, require_bool, require_empty_array, require_nonzero, require_string,
    validate_common_artifact,
};
use hawdb::{PersistentGraphIndexClass, ProductionQualificationIdentity};
use serde_json::Value;
use std::collections::BTreeSet;

const MATRIX_PROTOCOL: &str = "hawdb-production-graph-index-qualification-matrix-v1";

pub(super) fn validate_matrix(
    artifact: &Value,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    let mut blockers = Vec::new();
    validate_common_artifact(
        artifact,
        MATRIX_PROTOCOL,
        "representative_production_replica",
        &mut blockers,
    );
    require_empty_array(
        artifact,
        "/blocker_codes",
        "graph_index_matrix_blockers_present",
        &mut blockers,
    );
    let required_count =
        u64::try_from(PersistentGraphIndexClass::ALL.len()).expect("class count fits in u64");
    for (pointer, blocker) in [
        (
            "/required_class_count",
            "graph_index_matrix_required_class_count_mismatch",
        ),
        (
            "/qualified_class_count",
            "graph_index_matrix_qualified_class_count_mismatch",
        ),
    ] {
        if artifact.pointer(pointer).and_then(Value::as_u64) != Some(required_count) {
            blockers.push(blocker.to_string());
        }
    }

    let Some(cases) = artifact.pointer("/cases").and_then(Value::as_array) else {
        blockers.push("graph_index_matrix_cases_missing".to_string());
        return deduplicate(blockers);
    };
    if cases.len() != PersistentGraphIndexClass::ALL.len() {
        blockers.push("graph_index_matrix_case_count_mismatch".to_string());
    }

    let mut observed_classes = BTreeSet::new();
    for (index, case) in cases.iter().enumerate() {
        let class = case
            .pointer("/persistent_index_evidence/class")
            .and_then(Value::as_str);
        let class_name = class.unwrap_or("unknown");
        for blocker in graph_search::validate_graph(case, expected) {
            blockers.push(format!("graph_index_case_{class_name}_{blocker}"));
        }
        let Some(class) = class else {
            blockers.push("graph_index_case_class_missing".to_string());
            continue;
        };
        if !observed_classes.insert(class.to_string()) {
            blockers.push(format!("graph_index_matrix_duplicate_class_{class}"));
        }
        if PersistentGraphIndexClass::ALL
            .get(index)
            .map(|required| required.as_str())
            != Some(class)
        {
            blockers.push("graph_index_matrix_class_order_mismatch".to_string());
        }
        validate_index_case(case, class, &mut blockers);
    }
    for required in PersistentGraphIndexClass::ALL {
        if !observed_classes.contains(required.as_str()) {
            blockers.push(format!(
                "graph_index_matrix_class_{}_missing",
                required.as_str()
            ));
        }
    }
    deduplicate(blockers)
}

fn validate_index_case(case: &Value, class: &str, blockers: &mut Vec<String>) {
    let Some(evidence) = case.pointer("/persistent_index_evidence") else {
        blockers.push(format!("graph_index_case_{class}_evidence_missing"));
        return;
    };
    if !PersistentGraphIndexClass::ALL
        .into_iter()
        .any(|required| required.as_str() == class)
    {
        blockers.push(format!("graph_index_case_{class}_class_unknown"));
    }
    for (pointer, blocker) in [
        ("/exact_result_parity", "result_parity_failed"),
        ("/artifact_exceeds_cache", "artifact_not_larger_than_cache"),
        ("/cold_read_observed", "cold_read_missing"),
        ("/warm_read_observed", "warm_read_missing"),
        (
            "/cancellation/cancellation_observed",
            "cancellation_missing",
        ),
        (
            "/cancellation/handle_poisoned_after",
            "cancellation_poisoned_handle",
        ),
        (
            "/cancellation/subsequent_read_succeeded",
            "post_cancellation_read_failed",
        ),
    ] {
        let expected = pointer != "/cancellation/handle_poisoned_after";
        require_bool(
            evidence,
            pointer,
            expected,
            &format!("graph_index_case_{class}_{blocker}"),
            blockers,
        );
    }
    for (pointer, blocker) in [
        ("/digest_runs", "digest_runs_missing"),
        ("/operation_count", "operations_missing"),
        ("/max_blocks_read", "blocks_read_missing"),
        ("/max_bytes_read", "bytes_read_missing"),
        ("/max_blocks_read_per_run", "block_budget_missing"),
        ("/max_bytes_read_per_run", "byte_budget_missing"),
        ("/required_artifact_bytes", "artifact_bytes_missing"),
        (
            "/segment_cache_capacity_bytes",
            "segment_cache_capacity_missing",
        ),
    ] {
        require_nonzero(
            evidence,
            pointer,
            &format!("graph_index_case_{class}_{blocker}"),
            blockers,
        );
    }
    validate_result_parity(evidence, class, blockers);
    validate_artifact_residency(evidence, class, blockers);
    validate_cancellation(evidence, class, blockers);
    validate_runs(case, evidence, class, blockers);
}

fn validate_result_parity(evidence: &Value, class: &str, blockers: &mut Vec<String>) {
    let reference_digest = evidence
        .pointer("/reference_output_digest")
        .and_then(Value::as_str);
    let observed_digest = evidence
        .pointer("/observed_output_digest")
        .and_then(Value::as_str);
    let digest_matches = reference_digest
        .zip(observed_digest)
        .is_some_and(|(reference, observed)| valid_sha256(reference) && reference == observed);
    let row_count_matches = evidence
        .pointer("/reference_output_rows")
        .and_then(Value::as_u64)
        .zip(
            evidence
                .pointer("/observed_output_rows")
                .and_then(Value::as_u64),
        )
        .is_some_and(|(reference, observed)| reference == observed);
    if !digest_matches || !row_count_matches {
        blockers.push(format!("graph_index_case_{class}_raw_result_mismatch"));
    }
}

fn validate_artifact_residency(evidence: &Value, class: &str, blockers: &mut Vec<String>) {
    let artifact_bytes = evidence
        .pointer("/required_artifact_bytes")
        .and_then(Value::as_u64);
    let cache_bytes = evidence
        .pointer("/segment_cache_capacity_bytes")
        .and_then(Value::as_u64);
    if !artifact_bytes
        .zip(cache_bytes)
        .is_some_and(|(artifact, cache)| cache > 0 && artifact > cache)
    {
        blockers.push(format!(
            "graph_index_case_{class}_raw_artifact_not_larger_than_cache"
        ));
    }
}

fn validate_cancellation(evidence: &Value, class: &str, blockers: &mut Vec<String>) {
    let Some(cancellation) = evidence.pointer("/cancellation") else {
        blockers.push(format!("graph_index_case_{class}_cancellation_missing"));
        return;
    };
    let latency = cancellation
        .pointer("/latency_micros")
        .and_then(Value::as_u64);
    let limit = cancellation
        .pointer("/max_latency_micros")
        .and_then(Value::as_u64);
    if !latency
        .zip(limit)
        .is_some_and(|(latency, limit)| limit > 0 && latency <= limit)
    {
        blockers.push(format!(
            "graph_index_case_{class}_cancellation_latency_exceeded"
        ));
    }
    for (pointer, blocker) in [
        ("/pinned_bytes_before", "pre_cancellation_pins_nonzero"),
        ("/pinned_bytes_after", "post_cancellation_pins_nonzero"),
    ] {
        if cancellation.pointer(pointer).and_then(Value::as_u64) != Some(0) {
            blockers.push(format!("graph_index_case_{class}_{blocker}"));
        }
    }
}

fn validate_runs(case: &Value, evidence: &Value, class: &str, blockers: &mut Vec<String>) {
    let measurement_runs = evidence
        .pointer("/measurement_runs")
        .and_then(Value::as_u64);
    if !measurement_runs.is_some_and(|runs| runs >= 2)
        || measurement_runs
            != case
                .pointer("/execution/measurement_runs")
                .and_then(Value::as_u64)
        || evidence
            .pointer("/runs_using_required_class")
            .and_then(Value::as_u64)
            != measurement_runs
    {
        blockers.push(format!(
            "graph_index_case_{class}_measurement_count_mismatch"
        ));
    }
    let Some(runs) = evidence.pointer("/runs").and_then(Value::as_array) else {
        blockers.push(format!("graph_index_case_{class}_runs_missing"));
        return;
    };
    if measurement_runs != u64::try_from(runs.len()).ok() {
        blockers.push(format!("graph_index_case_{class}_run_count_mismatch"));
    }

    let mut operation_count = 0u64;
    let mut max_blocks_read = 0u64;
    let mut max_bytes_read = 0u64;
    let max_blocks_read_per_run = evidence
        .pointer("/max_blocks_read_per_run")
        .and_then(Value::as_u64);
    let max_bytes_read_per_run = evidence
        .pointer("/max_bytes_read_per_run")
        .and_then(Value::as_u64);
    for (index, run) in runs.iter().enumerate() {
        if run.pointer("/run").and_then(Value::as_u64) != u64::try_from(index).ok() {
            blockers.push(format!("graph_index_case_{class}_run_sequence_invalid"));
        }
        let phase = if index == 0 { "cold" } else { "warm" };
        require_string(
            run,
            "/phase",
            phase,
            &format!("graph_index_case_{class}_run_phase_invalid"),
            blockers,
        );
        for (pointer, blocker) in [
            ("/operation_count", "run_operation_missing"),
            ("/blocks_read", "run_blocks_missing"),
            ("/bytes_read", "run_bytes_missing"),
        ] {
            require_nonzero(
                run,
                pointer,
                &format!("graph_index_case_{class}_{blocker}"),
                blockers,
            );
        }
        operation_count = operation_count.saturating_add(
            run.pointer("/operation_count")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        );
        max_blocks_read = max_blocks_read.max(
            run.pointer("/blocks_read")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        );
        max_bytes_read = max_bytes_read.max(
            run.pointer("/bytes_read")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        );
        if !run
            .pointer("/blocks_read")
            .and_then(Value::as_u64)
            .zip(max_blocks_read_per_run)
            .is_some_and(|(actual, limit)| actual <= limit)
        {
            blockers.push(format!(
                "graph_index_case_{class}_run_block_budget_exceeded"
            ));
        }
        if !run
            .pointer("/bytes_read")
            .and_then(Value::as_u64)
            .zip(max_bytes_read_per_run)
            .is_some_and(|(actual, limit)| actual <= limit)
        {
            blockers.push(format!("graph_index_case_{class}_run_byte_budget_exceeded"));
        }
    }
    for (pointer, expected, blocker) in [
        (
            "/operation_count",
            operation_count,
            "operation_count_mismatch",
        ),
        (
            "/max_blocks_read",
            max_blocks_read,
            "max_blocks_read_mismatch",
        ),
        ("/max_bytes_read", max_bytes_read, "max_bytes_read_mismatch"),
    ] {
        if evidence.pointer(pointer).and_then(Value::as_u64) != Some(expected) {
            blockers.push(format!("graph_index_case_{class}_{blocker}"));
        }
    }
    if !runs
        .first()
        .and_then(|run| run.pointer("/index_cache_misses"))
        .and_then(Value::as_u64)
        .is_some_and(|misses| misses > 0)
    {
        blockers.push(format!("graph_index_case_{class}_raw_cold_read_missing"));
    }
    if !runs.iter().skip(1).any(|run| {
        run.pointer("/index_cache_hits")
            .and_then(Value::as_u64)
            .is_some_and(|hits| hits > 0)
    }) {
        blockers.push(format!("graph_index_case_{class}_raw_warm_read_missing"));
    }
}

fn valid_sha256(value: &str) -> bool {
    value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn deduplicate(mut blockers: Vec<String>) -> Vec<String> {
    blockers.sort();
    blockers.dedup();
    blockers
}
