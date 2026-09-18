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

use super::report_types::{
    BlockingOperatorMemoryReport, GraphExpansionExecutionReport, GraphExpansionTruncationReason,
    PipelineMemoryReport, VectorCandidateScanMetrics, VectorCompressionMode,
    VectorExecutionBackend, VectorExecutionReport, VectorFallbackReasonCode, VectorScoreSource,
};
use super::{
    blocking_operator_memory_report_value, graph_expansion_report_value,
    pipeline_memory_report_value, scan_pruning_report_value, vector_execution_report_value,
};
use hawdb_core::{LabelId, RelTypeId, Value};
use hawdb_plan::{VectorBackendSelectionReason, VectorCandidateSource};
use hawdb_storage::{ScanPruningReport, ScanPruningStrategy, ScanPruningTargetKind};

// These complete field inventories pin the old facade output, including fields
// deliberately absent from the value encoding. They are not production helpers.
fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Map(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

fn number(value: u128) -> Value {
    Value::Int(value.min(i64::MAX as u128) as i64)
}

fn optional(value: Option<u64>) -> Value {
    match value {
        None => Value::Null,
        Some(value) => number(u128::from(value)),
    }
}

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn strings(values: &[String]) -> Value {
    Value::List(values.iter().map(|value| text(value)).collect())
}

struct Data(u64);

impl Data {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        match (self.0 >> 32) % 8 {
            0 => 0,
            1 => 1,
            2 => i64::MAX as u64 - 1,
            3 => i64::MAX as u64,
            4 => i64::MAX as u64 + 1,
            5 => u64::MAX,
            _ => self.0,
        }
    }

    fn size(&mut self) -> usize {
        self.next() as usize
    }

    fn optional(&mut self, present: bool) -> Option<u64> {
        present.then(|| self.next())
    }
}

fn pipeline_fixture(data: &mut Data, case: usize) -> (PipelineMemoryReport, Value) {
    let report = PipelineMemoryReport {
        query_memory_budget_bytes: data.size(),
        query_memory_peak_bytes: data.size(),
        query_memory_completion_bytes: data.size(),
        query_memory_account_count: data.size(),
        intermediate_rows: data.size(),
        intermediate_payload_bytes: data.size(),
        peak_batch_rows: data.size(),
        peak_batch_payload_bytes: data.size(),
        columnar_batches: data.size(),
        columnar_input_rows: data.size(),
        columnar_selected_rows: data.size(),
        morsel_count: data.size(),
        morsel_max_admitted_workers: data.size(),
        morsel_peak_active_workers: data.size(),
        morsel_peak_buffered_outputs: data.size(),
        morsel_peak_buffered_output_bytes: data.size(),
        morsel_peak_reorder_entries: data.size(),
        output_rows: data.size(),
        output_payload_bytes: data.size(),
        start_resident_bytes: data.optional(case & 1 != 0),
        start_peak_resident_bytes: data.optional(case & (1 << 1) != 0),
        steady_resident_bytes: data.optional(case & (1 << 2) != 0),
        peak_resident_bytes: data.optional(case & (1 << 3) != 0),
        steady_resident_growth_bytes: data.optional(case & (1 << 4) != 0),
        lifetime_peak_resident_growth_bytes: data.optional(case & (1 << 5) != 0),
        total_page_faults: data.optional(case & (1 << 6) != 0),
        minor_page_faults: data.optional(case & (1 << 7) != 0),
        major_page_faults: data.optional(case & (1 << 8) != 0),
    };
    let expected = object([
        (
            "query_memory_budget_bytes",
            number(report.query_memory_budget_bytes as u128),
        ),
        (
            "query_memory_peak_bytes",
            number(report.query_memory_peak_bytes as u128),
        ),
        (
            "query_memory_completion_bytes",
            number(report.query_memory_completion_bytes as u128),
        ),
        (
            "query_memory_account_count",
            number(report.query_memory_account_count as u128),
        ),
        (
            "intermediate_rows",
            number(report.intermediate_rows as u128),
        ),
        (
            "intermediate_payload_bytes",
            number(report.intermediate_payload_bytes as u128),
        ),
        ("peak_batch_rows", number(report.peak_batch_rows as u128)),
        (
            "peak_batch_payload_bytes",
            number(report.peak_batch_payload_bytes as u128),
        ),
        ("columnar_batches", number(report.columnar_batches as u128)),
        (
            "columnar_input_rows",
            number(report.columnar_input_rows as u128),
        ),
        (
            "columnar_selected_rows",
            number(report.columnar_selected_rows as u128),
        ),
        ("morsel_count", number(report.morsel_count as u128)),
        (
            "morsel_max_admitted_workers",
            number(report.morsel_max_admitted_workers as u128),
        ),
        (
            "morsel_peak_active_workers",
            number(report.morsel_peak_active_workers as u128),
        ),
        (
            "morsel_peak_buffered_outputs",
            number(report.morsel_peak_buffered_outputs as u128),
        ),
        (
            "morsel_peak_buffered_output_bytes",
            number(report.morsel_peak_buffered_output_bytes as u128),
        ),
        (
            "morsel_peak_reorder_entries",
            number(report.morsel_peak_reorder_entries as u128),
        ),
        ("output_rows", number(report.output_rows as u128)),
        (
            "output_payload_bytes",
            number(report.output_payload_bytes as u128),
        ),
        (
            "start_resident_bytes",
            optional(report.start_resident_bytes),
        ),
        (
            "start_peak_resident_bytes",
            optional(report.start_peak_resident_bytes),
        ),
        (
            "steady_resident_bytes",
            optional(report.steady_resident_bytes),
        ),
        ("peak_resident_bytes", optional(report.peak_resident_bytes)),
        (
            "steady_resident_growth_bytes",
            optional(report.steady_resident_growth_bytes),
        ),
        (
            "lifetime_peak_resident_growth_bytes",
            optional(report.lifetime_peak_resident_growth_bytes),
        ),
        ("total_page_faults", optional(report.total_page_faults)),
        ("minor_page_faults", optional(report.minor_page_faults)),
        ("major_page_faults", optional(report.major_page_faults)),
    ]);
    (report, expected)
}

fn blocking_fixture(data: &mut Data) -> (BlockingOperatorMemoryReport, Value) {
    let report = BlockingOperatorMemoryReport {
        operator: "sort\0\u{03b1}".to_owned(),
        budget_bytes: data.size(),
        peak_tracked_bytes: data.size(),
        input_rows: data.size(),
        max_spill_runs: data.size(),
        spill_run_count: data.size(),
        spilled_rows: data.size(),
        max_spill_bytes: data.next(),
        spilled_bytes: data.next(),
    };
    let expected = object([
        ("operator", text(&report.operator)),
        ("budget_bytes", number(report.budget_bytes as u128)),
        (
            "peak_tracked_bytes",
            number(report.peak_tracked_bytes as u128),
        ),
        ("input_rows", number(report.input_rows as u128)),
        ("max_spill_runs", number(report.max_spill_runs as u128)),
        ("spill_run_count", number(report.spill_run_count as u128)),
        ("spilled_rows", number(report.spilled_rows as u128)),
        (
            "max_spill_bytes",
            number(u128::from(report.max_spill_bytes)),
        ),
        ("spilled_bytes", number(u128::from(report.spilled_bytes))),
    ]);
    (report, expected)
}

fn graph_fixture(data: &mut Data, case: usize) -> (GraphExpansionExecutionReport, Value) {
    let (reason, expected_reason) = match case % 3 {
        0 => (None, Value::Null),
        1 => (
            Some(GraphExpansionTruncationReason::CandidateLimit),
            text("candidate_limit"),
        ),
        _ => (
            Some(GraphExpansionTruncationReason::PayloadByteLimit),
            text("payload_byte_limit"),
        ),
    };
    let report = GraphExpansionExecutionReport {
        seed_count: data.size(),
        expanded_node_count: data.size(),
        expanded_edge_count: data.size(),
        min_hops: data.size(),
        max_hops: data.size(),
        reranked_seed_count: data.size(),
        candidate_limit: data.size(),
        payload_byte_limit: data.size(),
        payload_bytes_used: data.size(),
        returned_count: data.size(),
        relation_types: vec!["z".to_owned(), "a\0\u{03b1}".to_owned(), "z".to_owned()],
        truncation_reason: reason,
    };
    let expected = object([
        ("seed_count", number(report.seed_count as u128)),
        (
            "expanded_node_count",
            number(report.expanded_node_count as u128),
        ),
        (
            "expanded_edge_count",
            number(report.expanded_edge_count as u128),
        ),
        ("min_hops", number(report.min_hops as u128)),
        ("max_hops", number(report.max_hops as u128)),
        (
            "reranked_seed_count",
            number(report.reranked_seed_count as u128),
        ),
        ("candidate_limit", number(report.candidate_limit as u128)),
        (
            "payload_byte_limit",
            number(report.payload_byte_limit as u128),
        ),
        (
            "payload_bytes_used",
            number(report.payload_bytes_used as u128),
        ),
        ("returned_count", number(report.returned_count as u128)),
        ("relation_types", strings(&report.relation_types)),
        ("truncated", Value::Bool(!case.is_multiple_of(3))),
        ("truncation_reason", expected_reason),
    ]);
    (report, expected)
}

fn scan_strategies() -> Vec<(ScanPruningStrategy, Value)> {
    use ScanPruningStrategy::*;
    let property = "field\0\u{03b1}".to_owned();
    let properties = vec!["z".to_owned(), property.clone(), "z".to_owned()];
    let plain = |kind| object([("kind", text(kind))]);
    let single = |kind| object([("kind", text(kind)), ("property", text(&property))]);
    let composite = |kind| object([("kind", text(kind)), ("properties", strings(&properties))]);
    vec![
        (FullLabelScan, plain("full_label_scan")),
        (ExactCount, plain("exact_count")),
        (Empty, plain("empty")),
        (IdEq, plain("id_eq")),
        (IdIn, plain("id_in")),
        (IdRange, plain("id_range")),
        (
            PropertyEq {
                property: property.clone(),
            },
            single("property_eq"),
        ),
        (
            PropertyNotEq {
                property: property.clone(),
            },
            single("property_not_eq"),
        ),
        (
            PropertyMissingOrNull {
                property: property.clone(),
            },
            single("property_missing_or_null"),
        ),
        (
            PropertyExists {
                property: property.clone(),
            },
            single("property_exists"),
        ),
        (
            PropertyDefaultIfNullEq {
                property: property.clone(),
            },
            single("property_default_if_null_eq"),
        ),
        (
            PropertyDefaultIfNullNotEq {
                property: property.clone(),
            },
            single("property_default_if_null_not_eq"),
        ),
        (
            PropertyIn {
                property: property.clone(),
            },
            single("property_in"),
        ),
        (
            CompositePropertyEq {
                properties: properties.clone(),
            },
            composite("composite_property_eq"),
        ),
        (
            CompositePropertyRange {
                properties: properties.clone(),
            },
            composite("composite_property_range"),
        ),
        (
            PropertyRange {
                property: property.clone(),
            },
            single("property_range"),
        ),
        (
            FullText {
                property: property.clone(),
            },
            single("full_text"),
        ),
        (OrUnion, plain("or_union")),
    ]
}

fn scan_fixture(data: &mut Data, case: usize) -> (ScanPruningReport, Value) {
    let strategies = scan_strategies();
    let (strategy, expected_strategy) = strategies[case % strategies.len()].clone();
    let flags = case / strategies.len();
    let (target_kind, target_name) = if flags & 1 == 0 {
        (ScanPruningTargetKind::Node, "node")
    } else {
        (ScanPruningTargetKind::Relationship, "relationship")
    };
    let report = ScanPruningReport {
        target_kind,
        label_id: (flags & 2 != 0).then(|| LabelId(data.next() as u32)),
        rel_type_id: (flags & 4 != 0).then(|| RelTypeId(data.next() as u32)),
        strategy,
        pruned: flags & 8 != 0,
        exact_empty: flags & 16 != 0,
        candidate_count_before_pruning: data.size(),
        pruned_candidate_count: data.size(),
        candidate_count_before_filter: data.size(),
        output_count: data.size(),
        filtered_out_count: data.size(),
    };
    let expected = object([
        ("target_kind", text(target_name)),
        (
            "label_id",
            report
                .label_id
                .map_or(Value::Null, |id| number(u128::from(id.0))),
        ),
        (
            "rel_type_id",
            report
                .rel_type_id
                .map_or(Value::Null, |id| number(u128::from(id.0))),
        ),
        ("strategy", expected_strategy),
        ("pruned", Value::Bool(report.pruned)),
        ("exact_empty", Value::Bool(report.exact_empty)),
        (
            "candidate_count_before_pruning",
            number(report.candidate_count_before_pruning as u128),
        ),
        (
            "pruned_candidate_count",
            number(report.pruned_candidate_count as u128),
        ),
        (
            "candidate_count_before_filter",
            number(report.candidate_count_before_filter as u128),
        ),
        ("output_count", number(report.output_count as u128)),
        (
            "filtered_out_count",
            number(report.filtered_out_count as u128),
        ),
    ]);
    (report, expected)
}

fn vector_fixture(data: &mut Data, case: usize) -> (VectorExecutionReport, Value) {
    let backends = [
        (VectorExecutionBackend::ScalarFlat, "scalar_flat"),
        (VectorExecutionBackend::AnnProjection, "ann_projection"),
        (
            VectorExecutionBackend::QuantizedProjection,
            "quantized_projection",
        ),
        (VectorExecutionBackend::Unavailable, "unavailable"),
    ];
    let modes = [
        (VectorCompressionMode::Unspecified, "unspecified"),
        (VectorCompressionMode::Disabled, "disabled"),
        (VectorCompressionMode::Preferred, "preferred"),
        (VectorCompressionMode::Required, "required"),
    ];
    let sources = [
        (VectorCandidateSource::Scalar, "scalar"),
        (VectorCandidateSource::Ann, "ann"),
        (VectorCandidateSource::Quantized, "quantized"),
    ];
    let scores = [
        (VectorScoreSource::Unavailable, "unavailable"),
        (VectorScoreSource::RawVector, "raw_vector"),
        (VectorScoreSource::AnnApproximate, "ann_approximate"),
        (
            VectorScoreSource::QuantizedApproximate,
            "quantized_approximate",
        ),
    ];
    let reasons = [
        (
            VectorBackendSelectionReason::CompressionDisabled,
            "compression_disabled",
        ),
        (
            VectorBackendSelectionReason::RecallValidationProbe,
            "recall_validation_probe",
        ),
        (
            VectorBackendSelectionReason::SmallFilteredCandidateSet,
            "small_filtered_candidate_set",
        ),
        (
            VectorBackendSelectionReason::HighFilterSelectivity,
            "high_filter_selectivity",
        ),
        (
            VectorBackendSelectionReason::RawVectorsWithinMemoryBudget,
            "raw_vectors_within_memory_budget",
        ),
        (
            VectorBackendSelectionReason::QuantizedPreferred,
            "quantized_preferred",
        ),
        (
            VectorBackendSelectionReason::QuantizedRequired,
            "quantized_required",
        ),
        (
            VectorBackendSelectionReason::QuantizedProjectionUnavailable,
            "quantized_projection_unavailable",
        ),
        (
            VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete,
            "quantized_projection_coverage_incomplete",
        ),
    ];
    let fallback = vec![
        VectorFallbackReasonCode::QueryEmbeddingMissing,
        VectorFallbackReasonCode::VectorIndexEmpty,
        VectorFallbackReasonCode::VectorDimensionMismatch,
        VectorFallbackReasonCode::CompressedVectorProjectionUnavailable,
        VectorFallbackReasonCode::QueryEmbeddingMissing,
    ];
    let (backend, backend_name) = backends[case % 4];
    let (compression_mode, compression_name) = modes[(case / 4) % 4];
    let (candidate_source, source_name) = sources[(case / 16) % 3];
    let (candidate_score_source, candidate_score_name) = scores[(case / 48) % 4];
    let (final_score_source, final_score_name) = scores[(case / 192) % 4];
    let reason = (case % 10 != 9).then(|| reasons[case % 10]);
    let report = VectorExecutionReport {
        backend,
        compression_mode,
        candidate_source,
        backend_selection_reason: reason.map(|pair| pair.0),
        estimated_raw_vector_bytes: data.optional(case & 1 != 0),
        filter_selectivity_per_million: (case & 2 != 0).then(|| data.next() as u32),
        candidate_score_source,
        final_score_source,
        generated_candidate_count: data.size(),
        descriptor_pruned_count: data.size(),
        scalar_filtered_count: data.size(),
        residual_filtered_count: data.size(),
        candidate_scan_rounds: data.size(),
        reranked_candidate_count: data.size(),
        returned_count: data.size(),
        raw_vector_bytes_read: data.next(),
        candidate_scan_metrics: (case & 4 != 0).then(|| VectorCandidateScanMetrics {
            kernel: format!("kernel\0{case}"),
            worker_count: data.size(),
            segment_count: data.size(),
            scanned_segment_count: data.size(),
            scored_document_count: data.size(),
            filtered_document_count: data.size(),
            scanned_block_count: data.size(),
            skipped_block_count: data.size(),
            payload_bytes_read: data.next(),
            admitted_working_bytes: data.size(),
        }),
        index_covered_document_count: (case & 8 != 0).then(|| data.size()),
        index_candidate_document_count: (case & 16 != 0).then(|| data.size()),
        index_coverage_complete: (case & 32 != 0).then_some(case & 64 != 0),
        fallback_reason_codes: if case & 128 != 0 {
            fallback
        } else {
            Vec::new()
        },
    };
    let metrics = report.candidate_scan_metrics.as_ref();
    let expected = object([
        ("backend", text(backend_name)),
        ("compression_mode", text(compression_name)),
        ("candidate_source", text(source_name)),
        (
            "backend_selection_reason",
            reason.map_or(Value::Null, |pair| text(pair.1)),
        ),
        (
            "estimated_raw_vector_bytes",
            optional(report.estimated_raw_vector_bytes),
        ),
        (
            "filter_selectivity_per_million",
            report
                .filter_selectivity_per_million
                .map_or(Value::Null, |n| number(u128::from(n))),
        ),
        ("candidate_score_source", text(candidate_score_name)),
        ("final_score_source", text(final_score_name)),
        (
            "generated_candidate_count",
            number(report.generated_candidate_count as u128),
        ),
        (
            "descriptor_pruned_count",
            number(report.descriptor_pruned_count as u128),
        ),
        (
            "scalar_filtered_count",
            number(report.scalar_filtered_count as u128),
        ),
        (
            "residual_filtered_count",
            number(report.residual_filtered_count as u128),
        ),
        (
            "candidate_scan_rounds",
            number(report.candidate_scan_rounds as u128),
        ),
        (
            "reranked_candidate_count",
            number(report.reranked_candidate_count as u128),
        ),
        ("returned_count", number(report.returned_count as u128)),
        (
            "raw_vector_bytes_read",
            number(u128::from(report.raw_vector_bytes_read)),
        ),
        (
            "candidate_scan_kernel",
            metrics.map_or(Value::Null, |m| text(&m.kernel)),
        ),
        (
            "candidate_scan_worker_count",
            metrics.map_or(Value::Null, |m| number(m.worker_count as u128)),
        ),
        (
            "candidate_scan_segment_count",
            metrics.map_or(Value::Null, |m| number(m.segment_count as u128)),
        ),
        (
            "candidate_scan_scanned_block_count",
            metrics.map_or(Value::Null, |m| number(m.scanned_block_count as u128)),
        ),
        (
            "candidate_scan_skipped_block_count",
            metrics.map_or(Value::Null, |m| number(m.skipped_block_count as u128)),
        ),
        (
            "candidate_scan_payload_bytes_read",
            metrics.map_or(Value::Null, |m| number(u128::from(m.payload_bytes_read))),
        ),
        (
            "candidate_scan_admitted_working_bytes",
            metrics.map_or(Value::Null, |m| number(m.admitted_working_bytes as u128)),
        ),
        (
            "index_covered_document_count",
            report
                .index_covered_document_count
                .map_or(Value::Null, |n| number(n as u128)),
        ),
        (
            "index_candidate_document_count",
            report
                .index_candidate_document_count
                .map_or(Value::Null, |n| number(n as u128)),
        ),
        (
            "index_coverage_complete",
            report
                .index_coverage_complete
                .map_or(Value::Null, Value::Bool),
        ),
        (
            "fallback_reason_codes",
            if case & 128 != 0 {
                Value::List(
                    [
                        "query_embedding_missing",
                        "vector_index_empty",
                        "vector_dimension_mismatch",
                        "compressed_vector_projection_unavailable",
                        "query_embedding_missing",
                    ]
                    .into_iter()
                    .map(text)
                    .collect(),
                )
            } else {
                Value::List(Vec::new())
            },
        ),
    ]);
    (report, expected)
}

#[test]
fn pipeline_report_preserves_all_fields_and_optional_presence() {
    let mut data = Data(1);
    for presence in 0..512 {
        let (report, expected) = pipeline_fixture(&mut data, presence);
        assert_eq!(
            pipeline_memory_report_value(&report),
            expected,
            "presence={presence}"
        );
    }
}

#[test]
fn blocking_report_preserves_complete_field_contract() {
    let mut data = Data(2);
    for _ in 0..64 {
        let (report, expected) = blocking_fixture(&mut data);
        assert_eq!(blocking_operator_memory_report_value(&report), expected);
    }
}

#[test]
fn graph_report_preserves_reasons_and_list_order() {
    let mut data = Data(3);
    for case in 0..3 {
        let (report, expected) = graph_fixture(&mut data, case);
        assert_eq!(graph_expansion_report_value(&report), expected);
    }
}

#[test]
fn scan_report_preserves_all_strategies_and_identity_options() {
    let mut data = Data(4);
    for case in 0..576 {
        let (report, expected) = scan_fixture(&mut data, case);
        assert_eq!(scan_pruning_report_value(&report), expected, "case={case}");
    }
}

#[test]
fn vector_report_preserves_complete_field_contract() {
    let mut data = Data(5);
    for case in 0..768 {
        let (report, expected) = vector_fixture(&mut data, case);
        assert_eq!(
            vector_execution_report_value(&report),
            expected,
            "case={case}"
        );
    }
}

#[test]
fn report_values_distinguish_unknown_zero_and_saturated_counts() {
    for value in [
        0,
        1,
        i64::MAX as u64 - 1,
        i64::MAX as u64,
        i64::MAX as u64 + 1,
        u64::MAX,
    ] {
        let report = PipelineMemoryReport {
            query_memory_budget_bytes: value as usize,
            start_resident_bytes: Some(value),
            minor_page_faults: Some(0),
            major_page_faults: None,
            ..PipelineMemoryReport::default()
        };
        let Value::Map(encoded) = pipeline_memory_report_value(&report) else {
            panic!("expected map")
        };
        assert_eq!(
            encoded["query_memory_budget_bytes"],
            number((value as usize) as u128)
        );
        assert_eq!(encoded["start_resident_bytes"], number(u128::from(value)));
        assert_eq!(encoded["minor_page_faults"], Value::Int(0));
        assert_eq!(encoded["major_page_faults"], Value::Null);
    }
}

#[test]
#[ignore = "complete deterministic execution-report value campaign"]
fn execution_report_value_differential_campaign() {
    let mut comparisons = 0;
    for seed in 0..128_u64 {
        let mut data = Data(seed);
        for case in 0..64 {
            let case = seed as usize * 64 + case;
            let (report, expected) = pipeline_fixture(&mut data, case);
            assert_eq!(
                pipeline_memory_report_value(&report),
                expected,
                "pipeline seed={seed} case={case}"
            );
            let (report, expected) = blocking_fixture(&mut data);
            assert_eq!(
                blocking_operator_memory_report_value(&report),
                expected,
                "blocking seed={seed} case={case}"
            );
            let (report, expected) = graph_fixture(&mut data, case);
            assert_eq!(
                graph_expansion_report_value(&report),
                expected,
                "graph seed={seed} case={case}"
            );
            let (report, expected) = scan_fixture(&mut data, case);
            assert_eq!(
                scan_pruning_report_value(&report),
                expected,
                "scan seed={seed} case={case}"
            );
            let (report, expected) = vector_fixture(&mut data, case);
            assert_eq!(
                vector_execution_report_value(&report),
                expected,
                "vector seed={seed} case={case}"
            );
            comparisons += 5;
        }
    }
    assert_eq!(comparisons, 40_960);
    eprintln!("execution report values: 128 seeds, 8192 cases, {comparisons} complete comparisons");
}

#[test]
fn report_lists_preserve_non_palindromic_order_and_duplicates() {
    let mut data = Data(6);
    let values = vec![
        "z".to_owned(),
        "a".to_owned(),
        "z".to_owned(),
        "b".to_owned(),
    ];
    let expected_values = Value::List(vec![text("z"), text("a"), text("z"), text("b")]);
    let (mut graph, Value::Map(mut expected)) = graph_fixture(&mut data, 0) else {
        panic!("expected graph map")
    };
    graph.relation_types = values.clone();
    expected.insert("relation_types".to_owned(), expected_values.clone());
    assert_eq!(graph_expansion_report_value(&graph), Value::Map(expected));

    for (strategy, kind) in [
        (
            ScanPruningStrategy::CompositePropertyEq {
                properties: values.clone(),
            },
            "composite_property_eq",
        ),
        (
            ScanPruningStrategy::CompositePropertyRange {
                properties: values.clone(),
            },
            "composite_property_range",
        ),
    ] {
        let (mut scan, Value::Map(mut expected)) = scan_fixture(&mut data, 0) else {
            panic!("expected scan map")
        };
        scan.strategy = strategy;
        expected.insert(
            "strategy".to_owned(),
            object([
                ("kind", text(kind)),
                ("properties", expected_values.clone()),
            ]),
        );
        assert_eq!(scan_pruning_report_value(&scan), Value::Map(expected));
    }
}
