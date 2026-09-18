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
use crate::memory::{enforced_query_memory_budget, enforced_result_memory_budget};
use crate::observer::ExecutionObserver;
use crate::{ExecutionMemoryConfig, QueryMemoryClass};
use hawdb_core::{HawDBError, RuntimeMemoryReservation, RuntimeTaskContext};
use std::num::NonZeroUsize;

fn nonzero(bytes: usize) -> NonZeroUsize {
    NonZeroUsize::new(bytes).unwrap()
}

fn execution_error(error: HawDBError) -> String {
    let HawDBError::Execution(message) = error else {
        panic!("unexpected error: {error:?}");
    };
    message
}

// Independent wide-integer oracle: conversion must fail before any result cap.
fn admitted_budget(owner: &str, bytes: u64) -> std::result::Result<usize, String> {
    if u128::from(bytes) > usize::MAX as u128 {
        Err(format!(
            "runtime-admitted {owner} reservation {bytes} does not fit the executor address space"
        ))
    } else if bytes == 0 {
        Err(format!(
            "runtime-admitted {owner} reservation must be non-zero"
        ))
    } else {
        Ok(bytes as usize)
    }
}

fn check_admission(configured: usize, query: u64, result: u64) {
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: nonzero(configured),
        ..ExecutionMemoryConfig::default()
    };
    let context = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(query, result));
    assert_eq!(
        enforced_query_memory_budget(&memory, Some(&context))
            .map(NonZeroUsize::get)
            .map_err(execution_error),
        admitted_budget("query memory", query),
        "configured={configured}, query={query}, result={result}"
    );
    assert_eq!(
        enforced_result_memory_budget(&memory, Some(&context))
            .map(NonZeroUsize::get)
            .map_err(execution_error),
        admitted_budget("query result", result).map(|bytes| bytes.min(configured)),
        "configured={configured}, query={query}, result={result}"
    );
}

#[test]
fn admission_preserves_defaults_without_a_reservation() {
    for configured in [1, 64, usize::MAX] {
        let memory = ExecutionMemoryConfig {
            query_memory_bytes: nonzero(configured),
            ..ExecutionMemoryConfig::default()
        };
        let context = RuntimeTaskContext::default();
        for task in [None, Some(&context)] {
            assert_eq!(
                enforced_query_memory_budget(&memory, task).unwrap().get(),
                configured
            );
            assert_eq!(
                enforced_result_memory_budget(&memory, task).unwrap().get(),
                configured
            );
        }
    }
}

#[test]
fn admission_preserves_independent_reservations_and_address_space_errors() {
    for configured in [1, 64, usize::MAX] {
        for query in [0, 1, 63, 64, 65, u32::MAX as u64 + 1, u64::MAX] {
            for result in [0, 1, 63, 64, 65, u32::MAX as u64 + 1, u64::MAX] {
                check_admission(configured, query, result);
            }
        }
    }
}

#[test]
fn undersized_admission_fails_at_the_first_charge_without_widening() {
    let memory = ExecutionMemoryConfig::default();
    let context =
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(1, 1));
    let ledger =
        QueryMemoryLedger::new(enforced_query_memory_budget(&memory, Some(&context)).unwrap());
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "sort",
        memory.blocking_operator_bytes,
    );
    assert!(account.reserve(2).is_err());
    assert_eq!(ledger.snapshot().budget_bytes, 1);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().peak_bytes, 0);
}

fn plan() -> PhysicalPlan {
    PhysicalPlan::SortExec {
        items: vec![],
        input: Box::new(PhysicalPlan::DistinctExec {
            input: Box::new(PhysicalPlan::SortExec {
                items: vec![],
                input: Box::new(PhysicalPlan::EmptyExec),
            }),
        }),
    }
}

// Frozen facade initialization, with row-cap arithmetic independent of ExecutionLimit.
fn initial_profile(max_rows: Option<usize>) -> ReadExecutionProfile<ScanPruningReport> {
    ReadExecutionProfile {
        max_rows,
        detection_row_cap: max_rows.map(|rows| rows + 1),
        row_limit_enforced_before_output: max_rows.is_some(),
        operator_row_cap_enabled: max_rows.is_some(),
        operator_cardinality_profiles: Vec::new(),
        blocking_operator_kinds: vec!["DistinctExec".into(), "SortExec".into()],
        scan_pruning_reports: Vec::new(),
        vector_execution_reports: Vec::new(),
        graph_expansion_reports: Vec::new(),
        blocking_operator_memory_reports: Vec::new(),
        pipeline_memory_report: PipelineMemoryReport::default(),
    }
}

#[test]
fn initial_profile_preserves_all_defaults_and_overflow_errors() {
    let plan = plan();
    for max_rows in [None, Some(0), Some(1), Some(usize::MAX - 1)] {
        assert_eq!(
            read_execution_profile(&plan, max_rows).unwrap(),
            initial_profile(max_rows)
        );
    }
    assert_eq!(
        execution_error(read_execution_profile(&plan, Some(usize::MAX)).unwrap_err()),
        "read query row limit is too large"
    );
    assert!(ExecutionProfileBuilder::start(&plan, Some(usize::MAX)).is_err());
}

fn events(observer: &QueryExecutionObserver, plan: &PhysicalPlan, rows: usize) {
    use crate::{
        BlockingOperatorMemoryReport, GraphExpansionExecutionReport,
        GraphExpansionTruncationReason, VectorCompressionMode, VectorExecutionBackend,
        VectorExecutionReport, VectorScoreSource,
    };
    use hawdb_storage::{ScanPruningStrategy, ScanPruningTargetKind};

    observer.record_operator_start(plan);
    observer.record_operator_output(plan, rows);
    observer.record_columnar_batch(rows + 7, rows);
    observer.record_morsels(rows + 2);
    observer.record_morsel_admission(8, 3);
    observer.record_morsel_buffering(4, rows + 17, 2);
    observer.record_pipeline_batch(&[]);
    for count in [rows, rows + 1] {
        observer.record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Node,
            label_id: None,
            rel_type_id: None,
            strategy: ScanPruningStrategy::Empty,
            pruned: true,
            exact_empty: true,
            candidate_count_before_pruning: count,
            pruned_candidate_count: count,
            candidate_count_before_filter: 0,
            output_count: 0,
            filtered_out_count: 0,
        });
        observer.record_blocking_memory_report(BlockingOperatorMemoryReport {
            operator: "SortExec".into(),
            budget_bytes: 1024,
            peak_tracked_bytes: count,
            input_rows: count,
            max_spill_bytes: 2048,
            max_spill_runs: 2,
            spilled_bytes: 128,
            spill_run_count: 1,
            spilled_rows: count,
        });
        observer.record_graph_expansion(GraphExpansionExecutionReport {
            seed_count: 1,
            expanded_node_count: count,
            expanded_edge_count: count + 2,
            relation_types: vec!["LINKS".into()],
            min_hops: 1,
            max_hops: 2,
            reranked_seed_count: 1,
            candidate_limit: count + 3,
            payload_byte_limit: 1024,
            payload_bytes_used: 512,
            returned_count: count,
            truncation_reason: Some(GraphExpansionTruncationReason::CandidateLimit),
        });
        observer.record_vector_execution(VectorExecutionReport {
            backend: VectorExecutionBackend::ScalarFlat,
            compression_mode: VectorCompressionMode::Disabled,
            candidate_source: hawdb_plan::VectorCandidateSource::Scalar,
            backend_selection_reason: None,
            estimated_raw_vector_bytes: Some(128),
            filter_selectivity_per_million: None,
            candidate_score_source: VectorScoreSource::RawVector,
            final_score_source: VectorScoreSource::RawVector,
            generated_candidate_count: count,
            descriptor_pruned_count: 0,
            scalar_filtered_count: 0,
            residual_filtered_count: 0,
            candidate_scan_rounds: 1,
            reranked_candidate_count: count,
            returned_count: count,
            raw_vector_bytes_read: 128,
            candidate_scan_metrics: None,
            index_covered_document_count: None,
            index_candidate_document_count: None,
            index_coverage_complete: None,
            fallback_reason_codes: Vec::new(),
        });
    }
}

fn check_completion(rows: usize, retained: bool, max_rows: Option<usize>) {
    let plan = plan();
    let builder = ExecutionProfileBuilder::start(&plan, max_rows).unwrap();
    let observer = QueryExecutionObserver::new(&plan);
    let reference = QueryExecutionObserver::new(&plan);
    events(&observer, &plan, rows);
    events(&reference, &plan, rows);
    let reports = reference.into_reports();
    let mut expected = initial_profile(max_rows);
    expected.operator_cardinality_profiles = reports.operator_cardinality;
    expected.scan_pruning_reports = reports.scan_pruning;
    expected.vector_execution_reports = reports.vector_execution;
    expected.graph_expansion_reports = reports.graph_expansion;
    expected.blocking_operator_memory_reports = reports.blocking_memory;
    expected.pipeline_memory_report = PipelineMemoryReport {
        output_rows: rows,
        output_payload_bytes: rows * 7,
        query_memory_budget_bytes: 8192,
        query_memory_peak_bytes: rows + 32,
        query_memory_completion_bytes: if retained { rows + 1 } else { 0 },
        query_memory_account_count: 2,
        ..reports.pipeline_memory
    };

    let ledger = QueryMemoryLedger::new(nonzero(8192));
    let result_account = ledger.account(
        QueryMemoryClass::ResultMaterialization,
        "result",
        nonzero(8192),
    );
    let staging_account = ledger.account(QueryMemoryClass::PipelineBatch, "staging", nonzero(8192));
    let mut result_lease = result_account.reserve(rows + 1).unwrap();
    let staging_lease = staging_account.reserve(31).unwrap();
    drop(staging_lease);
    if !retained {
        result_lease.reset();
    }
    let before = ledger.snapshot();
    let actual = builder.finish(
        observer,
        &ledger,
        OutputMetrics {
            rows,
            payload_bytes: rows * 7,
        },
        |report| {
            assert_eq!(*report, expected.pipeline_memory_report);
            assert_eq!(ledger.snapshot(), before);
        },
    );
    // Whole-profile equality also verifies that no process metrics are invented.
    assert_eq!(actual, expected);
    assert_eq!(
        ledger.snapshot(),
        before,
        "finalization must only observe the ledger"
    );
    drop(result_lease);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(
        actual, expected,
        "the completion snapshot must not follow later lease releases"
    );
}

#[test]
fn completion_preserves_all_reports_and_retained_or_released_memory() {
    for rows in [0, 1, 17, 255] {
        for retained in [false, true] {
            for max_rows in [None, Some(rows), Some(usize::MAX - 1)] {
                check_completion(rows, retained, max_rows);
            }
        }
    }
}

#[test]
fn host_sampling_runs_after_completion_accounting_and_keeps_its_metrics() {
    let plan = plan();
    let ledger = QueryMemoryLedger::new(nonzero(64));
    let account = ledger.account(
        QueryMemoryClass::ResultMaterialization,
        "result",
        nonzero(64),
    );
    let lease = account.reserve(23).unwrap();
    let mut sampled = false;
    let profile = ExecutionProfileBuilder::start(&plan, None).unwrap().finish(
        QueryExecutionObserver::new(&plan),
        &ledger,
        OutputMetrics {
            rows: 1,
            payload_bytes: 7,
        },
        |report| {
            sampled = true;
            assert_eq!(report.output_rows, 1);
            assert_eq!(report.output_payload_bytes, 7);
            assert_eq!(report.query_memory_completion_bytes, 23);
            assert_eq!(ledger.snapshot().used_bytes, 23);
            report.start_resident_bytes = Some(100);
            report.start_peak_resident_bytes = Some(200);
            report.steady_resident_bytes = Some(150);
            report.peak_resident_bytes = Some(220);
            report.steady_resident_growth_bytes = Some(50);
            report.lifetime_peak_resident_growth_bytes = Some(20);
            report.total_page_faults = Some(9);
            report.minor_page_faults = Some(8);
            report.major_page_faults = Some(1);
            drop(lease);
        },
    );
    assert!(sampled);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(
        profile.pipeline_memory_report,
        PipelineMemoryReport {
            query_memory_budget_bytes: 64,
            query_memory_peak_bytes: 23,
            query_memory_completion_bytes: 23,
            query_memory_account_count: 1,
            output_rows: 1,
            output_payload_bytes: 7,
            start_resident_bytes: Some(100),
            start_peak_resident_bytes: Some(200),
            steady_resident_bytes: Some(150),
            peak_resident_bytes: Some(220),
            steady_resident_growth_bytes: Some(50),
            lifetime_peak_resident_growth_bytes: Some(20),
            total_page_faults: Some(9),
            minor_page_faults: Some(8),
            major_page_faults: Some(1),
            ..PipelineMemoryReport::default()
        }
    );
}

fn campaign(seeds: u64, cases: usize) {
    for seed in 1..=seeds {
        let mut state = seed;
        for case in 0..cases {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let configured = (state % 4096 + 1) as usize;
            let query = if case % 7 == 0 {
                0
            } else {
                state.rotate_left(17)
            };
            let result = if case % 11 == 0 {
                0
            } else {
                state.rotate_right(9)
            };
            check_admission(configured, query, result);
            let rows = (state % 1024) as usize;
            let max_rows = if case % 3 == 0 { None } else { Some(rows) };
            check_completion(rows, case % 2 == 0, max_rows);
        }
    }
    eprintln!(
        "execution lifecycle: {seeds} seeds, {} admission pairs and full-profile comparisons",
        seeds as usize * cases
    );
}

#[test]
fn execution_lifecycle_differential_smoke() {
    campaign(4, 16);
}

#[test]
#[ignore = "deterministic local execution lifecycle campaign"]
fn execution_lifecycle_differential_campaign() {
    campaign(128, 64);
}
