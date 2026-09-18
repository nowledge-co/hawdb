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

use super::*;
use serde_json::{json, Value};

const FAMILIES: [&str; 5] = [
    "memory_lookup",
    "graph_traversal",
    "projected_graph",
    "label_stats_read",
    "search_projection",
];

fn set(value: &mut Value, path: &[&str], replacement: Option<Value>) {
    let (last, parents) = path.split_last().unwrap();
    let mut object = value;
    for key in parents {
        object = object
            .as_object_mut()
            .unwrap()
            .entry(*key)
            .or_insert(json!({}));
    }
    let object = object.as_object_mut().unwrap();
    if let Some(replacement) = replacement {
        object.insert((*last).to_string(), replacement);
    } else {
        object.remove(*last);
    }
}

fn next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state ^ (*state >> 29)
}

fn recovery_oracle(report: Option<&Value>, required: bool) -> StorageRecoveryEvidenceHealth {
    let mut result = StorageRecoveryEvidenceHealth {
        required,
        present: report.is_some(),
        ready: !required,
        protocol_matches: None,
        durable_recovery_observed: None,
        checkpoint_boundary_present: None,
        wal_replay_bounded: None,
        replay_boundary_consistent: None,
        torn_tail_clean: None,
        blocker_codes: Vec::new(),
        blockers: Vec::new(),
    };
    let Some(report) = report else {
        if required {
            result.blocker_codes.push("missing_evidence".into());
            result
                .blockers
                .push("storage recovery evidence is required before cutover".into());
        }
        return result;
    };
    let number = |key| report.get(key).and_then(Value::as_u64).map(u128::from);
    let flag = |key| report["readiness"][key] == json!(true);
    result.protocol_matches = report["protocol"]
        .as_str()
        .map(|v| v == "hawdb-storage-recovery-report");
    result.durable_recovery_observed =
        Some(flag("durable_recovery_observed") && report["durable"] == json!(true));
    result.checkpoint_boundary_present = Some(
        flag("checkpoint_boundary_present")
            && number("checkpoint_epoch").is_some()
            && number("checkpoint_commit_epoch").is_some(),
    );
    result.wal_replay_bounded = Some(
        flag("wal_replay_bounded")
            && matches!((number("replayed_wal_entries"), number("max_wal_replay_entries")), (Some(n), Some(limit)) if n <= limit),
    );
    // Widened arithmetic independently rejects overflow and inconsistent replay frontiers.
    result.replay_boundary_consistent = Some(
        match (
            number("checkpoint_commit_epoch"),
            number("wal_replay_start_lsn"),
            number("next_lsn_after_replay"),
            number("replayed_wal_entries"),
            number("recovered_commit_epoch"),
        ) {
            (Some(checkpoint), Some(start), Some(end), Some(count), Some(recovered)) => {
                checkpoint + count == recovered && start + count == end
            }
            _ => false,
        },
    );
    result.torn_tail_clean = Some(
        flag("torn_tail_clean")
            && report["torn_tail_ignored"] == json!(false)
            && report["torn_tail_reason"].is_null(),
    );
    for (valid, code, message) in [
        (
            result.protocol_matches,
            "protocol_mismatch",
            "storage recovery evidence protocol mismatch",
        ),
        (
            result.durable_recovery_observed,
            "durable_recovery_not_observed",
            "storage recovery evidence does not prove durable recovery",
        ),
        (
            result.checkpoint_boundary_present,
            "checkpoint_boundary_missing",
            "storage recovery evidence lacks checkpoint boundary",
        ),
        (
            result.wal_replay_bounded,
            "wal_replay_unbounded",
            "storage recovery evidence lacks bounded WAL replay",
        ),
        (
            result.replay_boundary_consistent,
            "replay_boundary_inconsistent",
            "storage recovery evidence has inconsistent WAL replay boundaries",
        ),
        (
            result.torn_tail_clean,
            "torn_tail_observed",
            "storage recovery evidence observed torn WAL tail",
        ),
    ] {
        if valid != Some(true) {
            result.blocker_codes.push(code.into());
            result.blockers.push(message.into());
        }
    }
    result.ready = result.blockers.is_empty();
    result
}

fn recovery_cases(seed: u64) -> Vec<Option<Value>> {
    let (checkpoint, start, count) = match seed % 4 {
        0 => (0, 0, 0),
        1 => (u64::MAX, u64::MAX, 0),
        2 => (u64::MAX - 1, u64::MAX - 1, 1),
        _ => (seed, seed + 1, seed % 13),
    };
    let baseline = json!({
        "protocol": "hawdb-storage-recovery-report", "durable": true,
        "checkpoint_epoch": checkpoint, "checkpoint_commit_epoch": checkpoint,
        "wal_replay_start_lsn": start, "next_lsn_after_replay": start + count,
        "replayed_wal_entries": count, "max_wal_replay_entries": count,
        "recovered_commit_epoch": checkpoint + count, "torn_tail_ignored": false,
        "torn_tail_reason": null, "ready": true,
        "readiness": {
            "durable_recovery_observed": true, "checkpoint_boundary_present": true,
            "wal_replay_bounded": true, "torn_tail_clean": true
        }
    });
    let mut cases = vec![
        None,
        Some(Value::Null),
        Some(json!([])),
        Some(baseline.clone()),
    ];
    let paths: &[&[&str]] = &[
        &["protocol"],
        &["durable"],
        &["checkpoint_epoch"],
        &["checkpoint_commit_epoch"],
        &["wal_replay_start_lsn"],
        &["next_lsn_after_replay"],
        &["replayed_wal_entries"],
        &["max_wal_replay_entries"],
        &["recovered_commit_epoch"],
        &["torn_tail_ignored"],
        &["torn_tail_reason"],
        &["readiness"],
        &["readiness", "durable_recovery_observed"],
        &["readiness", "checkpoint_boundary_present"],
        &["readiness", "wal_replay_bounded"],
        &["readiness", "torn_tail_clean"],
    ];
    let values = [
        None,
        Some(Value::Null),
        Some(json!(false)),
        Some(json!(true)),
        Some(json!(-1)),
        Some(json!(0)),
        Some(json!(1)),
        Some(json!(u64::MAX)),
        Some(json!(1.0)),
        Some(json!("invalid")),
        Some(json!({})),
        Some(json!([])),
    ];
    for path in paths {
        for replacement in &values {
            let mut report = baseline.clone();
            set(&mut report, path, replacement.clone());
            cases.push(Some(report));
        }
    }
    // Contradictory summary booleans must not admit raw overflow.
    let mut overflow = baseline;
    for key in ["checkpoint_commit_epoch", "wal_replay_start_lsn"] {
        overflow[key] = json!(u64::MAX);
    }
    for key in ["next_lsn_after_replay", "recovered_commit_epoch"] {
        overflow[key] = json!(0);
    }
    overflow["replayed_wal_entries"] = json!(1);
    overflow["max_wal_replay_entries"] = json!(1);
    cases.push(Some(overflow));
    cases
}

fn family_oracle(input: Option<&Value>) -> ReplacementReadinessFamilyEvidenceHealth {
    let mut result = ReplacementReadinessFamilyEvidenceHealth {
        present: false,
        ready: true,
        min_replacement_readiness_per_million: None,
        invalid_family_count: 0,
        blocked_query_families: Vec::new(),
        missing_required_query_families: Vec::new(),
        blockers: Vec::new(),
    };
    let Some(Value::Array(rows)) = input else {
        return result;
    };
    result.present = true;
    let mut names = Vec::new();
    let mut scores = Vec::new();
    for row in rows {
        let name = row["query_family"].as_str();
        let score = row["replacement_readiness_per_million"].as_u64();
        if name.is_none() || score.is_none() {
            result.invalid_family_count += 1;
        }
        if let Some(name) = name {
            names.push(name);
        }
        if let Some(score) = score {
            scores.push(score);
            if score < 1_000_000
                && let Some(name) = name
            {
                result.blocked_query_families.push(name.to_string());
            }
        }
    }
    scores.sort_unstable();
    result.min_replacement_readiness_per_million = scores.first().copied();
    for name in FAMILIES {
        if !names.contains(&name) {
            result.missing_required_query_families.push(name.into());
        }
    }
    if result.invalid_family_count != 0 {
        result
            .blockers
            .push("replacement readiness family report has invalid entries".into());
    }
    if !result.blocked_query_families.is_empty() {
        result.blockers.push(format!(
            "replacement readiness is incomplete for query families: {}",
            result.blocked_query_families.join(", ")
        ));
    }
    if !result.missing_required_query_families.is_empty() {
        result.blockers.push(format!(
            "replacement readiness is missing required query families: {}",
            result.missing_required_query_families.join(", ")
        ));
    }
    result.ready = result.blockers.is_empty();
    result
}

fn family_cases(seed: u64) -> Vec<Option<Value>> {
    let ready: Vec<_> = FAMILIES
        .iter()
        .map(|name| {
            json!({
                "query_family": name, "replacement_readiness_per_million": 1_000_000
            })
        })
        .collect();
    let mut cases = vec![
        None,
        Some(Value::Null),
        Some(json!({})),
        Some(json!([])),
        Some(json!(ready)),
    ];
    let mut state = seed;
    for round in 0..32 {
        let mut rows = if round % 2 == 0 {
            ready.clone()
        } else {
            Vec::new()
        };
        for _ in 0..(next(&mut state) % 12 + 1) {
            let name = match next(&mut state) % 8 {
                0 => json!(null),
                1 => json!(7),
                2 => json!(""),
                3 => json!("\u{65e5}\u{672c}-unknown"),
                n => json!(FAMILIES[n as usize % 5]),
            };
            let score = match next(&mut state) % 8 {
                0 => json!(null),
                1 => json!(-1),
                2 => json!(999_999),
                3 => json!(0),
                4 => json!(1_000_000),
                5 => json!(u64::MAX),
                6 => json!(1.0),
                _ => json!("1000000"),
            };
            rows.push(json!({"query_family": name, "replacement_readiness_per_million": score}));
        }
        let rotation = next(&mut state) as usize % rows.len();
        rows.rotate_left(rotation);
        cases.push(Some(json!(rows)));
    }
    cases
}

fn background_baseline(seed: u64, required: bool) -> (Value, BackgroundMaintenanceEvidenceHealth) {
    let operations = seed % 100;
    let report = json!({
        "protocol": "hawdb-background-maintenance-report", "total_candidates": 3,
        "foreground_admission_probe_ready": true, "foreground_admission_probe_admission": "admit",
        "memory_pressure": {"ready": true, "budget_bytes": u64::MAX, "estimated_bytes": u64::MAX},
        "qos_snapshot": {"ready": true, "foreground_admitted": true, "background_bounded": true,
            "total_background_over_budget": false, "blocker_codes": []},
        "slow_query": {"ready": true, "record_count": 8, "capacity": 8,
            "redaction": {"query_text_copied": false, "parameters_copied": false, "local_paths_copied": false}},
        "ranked": [
            {"priority": "background", "admission": "admit", "has_executable_search_projection_graph_delta": true,
                "search_projection_graph_delta_operation_count": operations,
                "search_projection_graph_delta_complete_through_graph_commit_epoch": seed},
            {"priority": "background", "admission": "defer", "has_executable_search_projection_graph_delta": true,
                "search_projection_graph_delta_operation_count": 2,
                "search_projection_graph_delta_complete_through_graph_commit_epoch": seed + 1},
            {"priority": "background", "admission": "reject", "has_executable_search_projection_graph_delta": false,
                "search_projection_graph_delta_operation_count": 1000}
        ]
    });
    let expected = BackgroundMaintenanceEvidenceHealth {
        required,
        present: true,
        ready: true,
        protocol_matches: Some(true),
        total_candidates: Some(3),
        ranked_count: Some(3),
        executable_search_projection_graph_delta_count: Some(2),
        admitted_search_projection_graph_delta_count: Some(1),
        deferred_search_projection_graph_delta_count: Some(1),
        rejected_search_projection_graph_delta_count: Some(0),
        executable_search_projection_graph_delta_operations: Some(operations + 2),
        admitted_search_projection_graph_delta_operations: Some(operations),
        max_search_projection_graph_delta_complete_through_graph_commit_epoch: Some(seed + 1),
        foreground_admission_probe_ready: Some(true),
        foreground_admission_probe_admission_name: Some("admit".into()),
        memory_pressure_ready: Some(true),
        memory_budget_bytes: Some(u64::MAX),
        estimated_memory_bytes: Some(u64::MAX),
        qos_snapshot_ready: Some(true),
        qos_snapshot_foreground_admitted: Some(true),
        qos_snapshot_background_bounded: Some(true),
        qos_snapshot_total_background_over_budget: Some(false),
        qos_snapshot_blocker_codes: Vec::new(),
        slow_query_ready: Some(true),
        slow_query_record_count: Some(8),
        slow_query_capacity: Some(8),
        slow_query_redaction_ready: Some(true),
        foreground_ranked_count: 0,
        unknown_admission_count: 0,
        blocker_codes: Vec::new(),
        blockers: Vec::new(),
    };
    (report, expected)
}

fn background_blockers(expected: &mut BackgroundMaintenanceEvidenceHealth) {
    // The oracle evaluates generated facts, never parses the report or calls production helpers.
    let rules = [
        (
            expected.protocol_matches == Some(false),
            "protocol_mismatch",
            "background maintenance evidence protocol mismatch",
        ),
        (
            expected.required && expected.total_candidates.unwrap_or(0) == 0,
            "no_candidates",
            "background maintenance evidence has no candidates",
        ),
        (
            expected.required && expected.ranked_count.unwrap_or(0) == 0,
            "no_ranked_work",
            "background maintenance evidence has no ranked work",
        ),
        (
            expected.required && expected.foreground_admission_probe_ready != Some(true),
            "foreground_admission_probe_missing",
            "background maintenance evidence lacks a passing foreground admission probe",
        ),
        (
            expected.foreground_ranked_count != 0,
            "foreground_ranked_work",
            "background maintenance evidence ranked foreground work",
        ),
        (
            expected.unknown_admission_count != 0,
            "unknown_admission",
            "background maintenance evidence has unknown admission values",
        ),
        (
            expected.required && expected.memory_pressure_ready.is_none(),
            "memory_pressure_missing",
            "background maintenance evidence lacks memory-pressure budget",
        ),
        (
            expected.memory_pressure_ready == Some(false),
            "memory_budget_exceeded",
            "background maintenance evidence exceeds configured memory budget",
        ),
        (
            expected.required && expected.qos_snapshot_ready.is_none(),
            "qos_snapshot_missing",
            "background maintenance evidence lacks a local QoS snapshot",
        ),
        (
            expected.qos_snapshot_ready == Some(false),
            "qos_snapshot_not_ready",
            "background maintenance local QoS snapshot is not ready",
        ),
        (
            expected.qos_snapshot_background_bounded == Some(false),
            "qos_snapshot_background_unbounded",
            "background maintenance local QoS snapshot is unbounded",
        ),
        (
            expected.qos_snapshot_total_background_over_budget == Some(true),
            "qos_snapshot_background_over_budget",
            "background maintenance local QoS snapshot is over budget",
        ),
        (
            expected.required && expected.slow_query_ready.is_none(),
            "slow_query_missing",
            "background maintenance evidence lacks slow-query summary",
        ),
        (
            expected.slow_query_ready == Some(false),
            "slow_query_not_ready",
            "background maintenance slow-query summary is not ready",
        ),
        (
            matches!((expected.slow_query_record_count, expected.slow_query_capacity), (Some(n), Some(cap)) if n > cap),
            "slow_query_unbounded",
            "background maintenance slow-query summary exceeds capacity",
        ),
        (
            expected.slow_query_redaction_ready == Some(false),
            "slow_query_redaction_not_ready",
            "background maintenance slow-query summary is not redacted",
        ),
    ];
    for (index, (blocked, code, message)) in rules.into_iter().enumerate() {
        if index == 12 {
            expected
                .blocker_codes
                .extend(expected.qos_snapshot_blocker_codes.clone());
        }
        if blocked {
            expected.blocker_codes.push(code.into());
            expected.blockers.push(message.into());
        }
    }
    expected.ready = expected.blockers.is_empty();
}

fn background_case(
    seed: u64,
    case: usize,
    layout: usize,
    required: bool,
) -> (Value, BackgroundMaintenanceEvidenceHealth) {
    let (mut report, mut expected) = background_baseline(seed, required);
    match case {
        0 => {}
        1 => {
            report["protocol"] = json!("wrong");
            expected.protocol_matches = Some(false);
        }
        2 => {
            report.as_object_mut().unwrap().remove("protocol");
            expected.protocol_matches = None;
        }
        3 => {
            report["protocol"] = json!(7);
            expected.protocol_matches = None;
        }
        4 => {
            report["total_candidates"] = json!(0);
            expected.total_candidates = Some(0);
        }
        5 => {
            report["total_candidates"] = json!(-1);
            expected.total_candidates = None;
        }
        6 => {
            report["foreground_admission_probe_ready"] = json!(false);
            expected.foreground_admission_probe_ready = Some(false);
        }
        7 => {
            report["foreground_admission_probe_ready"] = Value::Null;
            expected.foreground_admission_probe_ready = None;
        }
        8 => {
            report["foreground_admission_probe_admission"] = json!(7);
            expected.foreground_admission_probe_admission_name = None;
        }
        9 => {
            report["memory_pressure"]["ready"] = json!(false);
            expected.memory_pressure_ready = Some(false);
        }
        10 => {
            report["memory_pressure"]["budget_bytes"] = json!(u64::MAX - 1);
            expected.memory_budget_bytes = Some(u64::MAX - 1);
            expected.memory_pressure_ready = Some(false);
        }
        11 => {
            report["memory_pressure"]["budget_bytes"] = json!("invalid");
            expected.memory_budget_bytes = None;
            expected.memory_pressure_ready = Some(false);
        }
        12 => {
            report["memory_pressure"] = json!({});
            expected.memory_pressure_ready = None;
            expected.memory_budget_bytes = None;
            expected.estimated_memory_bytes = None;
        }
        13 => {
            report["qos_snapshot"]["ready"] = json!(false);
            expected.qos_snapshot_ready = Some(false);
        }
        14 => {
            report["qos_snapshot"]["ready"] = Value::Null;
            expected.qos_snapshot_ready = None;
        }
        15 => {
            report["qos_snapshot"]["foreground_admitted"] = json!(false);
            expected.qos_snapshot_foreground_admitted = Some(false);
        }
        16 => {
            report["qos_snapshot"]["background_bounded"] = json!(false);
            expected.qos_snapshot_background_bounded = Some(false);
        }
        17 => {
            report["qos_snapshot"]["total_background_over_budget"] = json!(true);
            expected.qos_snapshot_total_background_over_budget = Some(true);
        }
        18 => {
            report["qos_snapshot"]["blocker_codes"] = json!(["z", 3, "a", "z"]);
            expected.qos_snapshot_blocker_codes = vec!["z".into(), "a".into(), "z".into()];
        }
        19 => {
            report["slow_query"]["ready"] = json!(false);
            expected.slow_query_ready = Some(false);
        }
        20 => {
            report["slow_query"]["ready"] = Value::Null;
            expected.slow_query_ready = None;
        }
        21 => {
            report["slow_query"]["record_count"] = json!(9);
            expected.slow_query_record_count = Some(9);
        }
        22 => {
            report["slow_query"]["capacity"] = json!(-1);
            expected.slow_query_capacity = None;
        }
        23..=25 => {
            let field = [
                "query_text_copied",
                "parameters_copied",
                "local_paths_copied",
            ][case - 23];
            report["slow_query"]["redaction"][field] = json!(true);
            expected.slow_query_redaction_ready = Some(false);
        }
        26 => {
            report["slow_query"]["redaction"] = Value::Null;
            expected.slow_query_redaction_ready = Some(false);
        }
        27 => {
            report["ranked"][0]["priority"] = json!("foreground");
            expected.foreground_ranked_count = 1;
        }
        28 => {
            report["ranked"][0]["admission"] = json!("unknown");
            expected.unknown_admission_count = 1;
            expected.admitted_search_projection_graph_delta_count = Some(0);
            expected.admitted_search_projection_graph_delta_operations = Some(0);
        }
        29 => {
            report["ranked"] = json!([]);
            expected.ranked_count = Some(0);
            expected.executable_search_projection_graph_delta_count = Some(0);
            expected.admitted_search_projection_graph_delta_count = Some(0);
            expected.deferred_search_projection_graph_delta_count = Some(0);
            expected.executable_search_projection_graph_delta_operations = Some(0);
            expected.admitted_search_projection_graph_delta_operations = Some(0);
            expected.max_search_projection_graph_delta_complete_through_graph_commit_epoch = None;
        }
        30 => {
            report["ranked"][2]["has_executable_search_projection_graph_delta"] = json!(true);
            expected.executable_search_projection_graph_delta_count = Some(3);
            expected.rejected_search_projection_graph_delta_count = Some(1);
            expected.executable_search_projection_graph_delta_operations = Some(seed % 100 + 1002);
        }
        31 => {
            report["ranked"][0]["search_projection_graph_delta_operation_count"] = json!(-1);
            expected.executable_search_projection_graph_delta_operations = Some(2);
            expected.admitted_search_projection_graph_delta_operations = Some(0);
        }
        32 => {
            report["ranked"][1]
                ["search_projection_graph_delta_complete_through_graph_commit_epoch"] = Value::Null;
            expected.max_search_projection_graph_delta_complete_through_graph_commit_epoch =
                Some(seed);
        }
        33 => {
            for (field, target) in [
                (
                    "executable_search_projection_graph_delta_count",
                    &mut expected.executable_search_projection_graph_delta_count,
                ),
                (
                    "admitted_search_projection_graph_delta_count",
                    &mut expected.admitted_search_projection_graph_delta_count,
                ),
                (
                    "deferred_search_projection_graph_delta_count",
                    &mut expected.deferred_search_projection_graph_delta_count,
                ),
                (
                    "rejected_search_projection_graph_delta_count",
                    &mut expected.rejected_search_projection_graph_delta_count,
                ),
                (
                    "executable_search_projection_graph_delta_operations",
                    &mut expected.executable_search_projection_graph_delta_operations,
                ),
                (
                    "admitted_search_projection_graph_delta_operations",
                    &mut expected.admitted_search_projection_graph_delta_operations,
                ),
                (
                    "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                    &mut expected
                        .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
                ),
            ] {
                report[field] = json!(u64::MAX);
                *target = Some(u64::MAX);
            }
        }
        34 => {
            for field in [
                "executable_search_projection_graph_delta_count",
                "admitted_search_projection_graph_delta_count",
                "deferred_search_projection_graph_delta_count",
                "rejected_search_projection_graph_delta_count",
                "executable_search_projection_graph_delta_operations",
                "admitted_search_projection_graph_delta_operations",
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ] {
                report[field] = json!(-1);
            }
        }
        _ => unreachable!(),
    }
    // Mix independent faults into each case to exercise blocker ordering and combinations.
    if seed & 1 != 0 {
        report["qos_snapshot"]["blocker_codes"] = json!(["generated_qos", "generated_qos"]);
        expected.qos_snapshot_blocker_codes = vec!["generated_qos".into(), "generated_qos".into()];
    }
    if seed & 2 != 0 {
        report["slow_query"]["record_count"] = json!(u64::MAX);
        expected.slow_query_record_count = Some(u64::MAX);
    }
    if seed & 4 != 0 {
        report["ranked"].as_array_mut().unwrap().reverse();
    }
    for (group, nested, flat) in [
        ("memory_pressure", "ready", "memory_pressure_ready"),
        ("memory_pressure", "budget_bytes", "memory_budget_bytes"),
        (
            "memory_pressure",
            "estimated_bytes",
            "estimated_memory_bytes",
        ),
        ("qos_snapshot", "ready", "qos_snapshot_ready"),
        (
            "qos_snapshot",
            "foreground_admitted",
            "qos_snapshot_foreground_admitted",
        ),
        (
            "qos_snapshot",
            "background_bounded",
            "qos_snapshot_background_bounded",
        ),
        (
            "qos_snapshot",
            "total_background_over_budget",
            "qos_snapshot_total_background_over_budget",
        ),
        (
            "qos_snapshot",
            "blocker_codes",
            "qos_snapshot_blocker_codes",
        ),
        ("slow_query", "ready", "slow_query_ready"),
        ("slow_query", "record_count", "slow_query_record_count"),
        ("slow_query", "capacity", "slow_query_capacity"),
    ] {
        match layout {
            0 => {}
            1 => {
                if let Some(value) = report[group].as_object_mut().unwrap().remove(nested) {
                    report[flat] = value;
                }
            }
            2 => {
                // A present nested null or wrong type must mask even a valid flat value.
                if report[group].get(nested).is_some() {
                    report[flat] = match flat {
                        "qos_snapshot_blocker_codes" => json!(["must_not_appear"]),
                        "memory_budget_bytes"
                        | "estimated_memory_bytes"
                        | "slow_query_record_count"
                        | "slow_query_capacity" => json!(7),
                        _ => json!(true),
                    };
                }
            }
            _ => unreachable!(),
        }
    }
    background_blockers(&mut expected);
    (report, expected)
}

fn campaign(seeds: u64) {
    assert_eq!(REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES, FAMILIES);
    let mut recovery_count = 0;
    let mut family_count = 0;
    let mut background_count = 0;
    for seed in 0..seeds {
        for (case, input) in recovery_cases(seed).iter().enumerate() {
            for required in [false, true] {
                let expected = recovery_oracle(input.as_ref(), required);
                assert_eq!(
                    storage_recovery_evidence_health(input.as_ref(), required),
                    expected,
                    "recovery seed={seed} case={case} required={required}"
                );
                let bundle = input
                    .as_ref()
                    .map_or_else(|| json!({}), |v| json!({"storage_recovery": v}));
                assert_eq!(
                    storage_recovery_evidence_health_from_bundle(&bundle, required),
                    expected
                );
                recovery_count += 1;
            }
        }
        for (case, input) in family_cases(seed).iter().enumerate() {
            let expected = family_oracle(input.as_ref());
            assert_eq!(
                replacement_readiness_family_evidence_health(input.as_ref()),
                expected,
                "family seed={seed} case={case}"
            );
            let bundle = input.as_ref().map_or_else(
                || json!({}),
                |v| json!({"replacement_readiness_by_query_family": v}),
            );
            assert_eq!(
                replacement_readiness_family_evidence_health_from_bundle(&bundle),
                expected
            );
            family_count += 1;
        }
        for case in 0..35 {
            for layout in 0..3 {
                for required in [false, true] {
                    let (report, expected) = background_case(seed, case, layout, required);
                    assert_eq!(
                        background_maintenance_evidence_health(Some(&report), required),
                        expected,
                        "background seed={seed} case={case} layout={layout} required={required}"
                    );
                    let bundle = json!({"background_maintenance": report});
                    assert_eq!(
                        background_maintenance_evidence_health_from_bundle(&bundle, required),
                        expected
                    );
                    background_count += 1;
                }
            }
        }
    }
    eprintln!("inventory-health-differential-v1 seeds={seeds} recovery_cases={recovery_count} family_cases={family_count} background_cases={background_count}");
}

#[test]
fn inventory_health_differential_smoke() {
    campaign(8);
}

#[test]
fn missing_and_malformed_evidence_preserves_optional_policy() {
    for required in [false, true] {
        let missing = background_maintenance_evidence_health(None, required);
        assert!(!missing.present);
        assert_eq!(missing.required, required);
        assert_eq!(missing.ready, !required);
        assert_eq!(missing.ranked_count, None);
        assert_eq!(missing.executable_search_projection_graph_delta_count, None);
        assert_eq!(
            missing.blocker_codes,
            if required {
                vec!["missing_evidence"]
            } else {
                vec![]
            }
        );

        for input in [
            Value::Null,
            json!(false),
            json!(7),
            json!("invalid"),
            json!([]),
            json!({}),
        ] {
            let background = background_maintenance_evidence_health(Some(&input), required);
            assert!(background.present);
            assert_eq!(background.ready, !required);
            assert_eq!(background.protocol_matches, None);
            assert_eq!(background.ranked_count, None);
            assert_eq!(
                background.executable_search_projection_graph_delta_count,
                Some(0)
            );
            assert_eq!(
                background.admitted_search_projection_graph_delta_operations,
                Some(0)
            );
            assert_eq!(
                background.blocker_codes,
                if required {
                    vec![
                        "no_candidates",
                        "no_ranked_work",
                        "foreground_admission_probe_missing",
                        "memory_pressure_missing",
                        "qos_snapshot_missing",
                        "slow_query_missing",
                    ]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                storage_recovery_evidence_health(Some(&input), required),
                recovery_oracle(Some(&input), required)
            );
            assert_eq!(
                replacement_readiness_family_evidence_health(Some(&input)),
                family_oracle(Some(&input))
            );
        }
    }
    let invalid_rows = json!([null, false, 7, "invalid", [], {}]);
    let family = replacement_readiness_family_evidence_health(Some(&invalid_rows));
    assert_eq!(family, family_oracle(Some(&invalid_rows)));
    assert_eq!(family.invalid_family_count, 6);
    assert_eq!(family.missing_required_query_families, FAMILIES);
}

#[test]
#[ignore = "bounded local evidence health differential campaign"]
fn inventory_health_differential_campaign() {
    campaign(128);
}
