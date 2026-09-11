use super::*;
use crate::PipelineMemoryReport;
use skein_core::Value;
use skein_plan::PlanChildren;

#[test]
fn query_observers_keep_reports_isolated() {
    let first = QueryExecutionObserver::default();
    let second = QueryExecutionObserver::default();
    first.record_morsel_admission(2, 1);
    second.record_morsel_admission(4, 3);

    assert_eq!(
        first
            .into_reports()
            .pipeline_memory
            .morsel_max_admitted_workers,
        2
    );
    assert_eq!(
        second
            .into_reports()
            .pipeline_memory
            .morsel_max_admitted_workers,
        4
    );
}

#[test]
fn operator_profiles_distinguish_zero_rows_from_not_executed() {
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::EmptyExec),
        right: Box::new(PhysicalPlan::EmptyExec),
    };
    let observer = QueryExecutionObserver::new(&plan);
    let PlanChildren::Binary(left, _) = plan.children() else {
        panic!("cartesian product should have two inputs");
    };

    observer.record_operator_start(&plan);
    observer.record_operator_output(left, 0);
    let profiles = observer.into_reports().operator_cardinality;

    assert_eq!(profiles.len(), 3);
    assert_eq!(profiles[0].actual_rows, Some(0));
    assert_eq!(profiles[1].actual_rows, Some(0));
    assert_eq!(profiles[2].actual_rows, None);
}

#[test]
fn typed_reports_preserve_contents_order_and_last_vector_count() {
    use crate::{
        BlockingOperatorMemoryReport, GraphExpansionExecutionReport,
        GraphExpansionTruncationReason, VectorCompressionMode, VectorExecutionBackend,
        VectorExecutionReport, VectorScoreSource,
    };
    use skein_storage::{ScanPruningStrategy, ScanPruningTargetKind};

    let observer = QueryExecutionObserver::default();
    assert_eq!(observer.current_vector_rerank_count(), 0);
    let vector = VectorExecutionReport {
        backend: VectorExecutionBackend::ScalarFlat,
        compression_mode: VectorCompressionMode::Disabled,
        candidate_source: skein_plan::VectorCandidateSource::Scalar,
        backend_selection_reason: None,
        estimated_raw_vector_bytes: Some(128),
        filter_selectivity_per_million: None,
        candidate_score_source: VectorScoreSource::RawVector,
        final_score_source: VectorScoreSource::RawVector,
        generated_candidate_count: 8,
        descriptor_pruned_count: 0,
        scalar_filtered_count: 0,
        residual_filtered_count: 0,
        candidate_scan_rounds: 1,
        reranked_candidate_count: 8,
        returned_count: 2,
        raw_vector_bytes_read: 128,
        candidate_scan_metrics: None,
        index_covered_document_count: None,
        index_candidate_document_count: None,
        index_coverage_complete: None,
        fallback_reason_codes: Vec::new(),
    };
    let last_vector = VectorExecutionReport {
        reranked_candidate_count: 0,
        ..vector.clone()
    };
    observer.record_vector_execution(vector.clone());
    assert_eq!(observer.current_vector_rerank_count(), 8);
    observer.record_vector_execution(last_vector.clone());
    assert_eq!(observer.current_vector_rerank_count(), 0);
    let graph = GraphExpansionExecutionReport {
        seed_count: 1,
        expanded_node_count: 5,
        expanded_edge_count: 8,
        relation_types: vec!["LINKS".into()],
        min_hops: 1,
        max_hops: 2,
        reranked_seed_count: 1,
        candidate_limit: 6,
        payload_byte_limit: 1024,
        payload_bytes_used: 1000,
        returned_count: 4,
        truncation_reason: Some(GraphExpansionTruncationReason::CandidateLimit),
    };
    observer.record_graph_expansion(graph.clone());
    let scan = ScanPruningReport {
        target_kind: ScanPruningTargetKind::Node,
        label_id: None,
        rel_type_id: None,
        strategy: ScanPruningStrategy::Empty,
        pruned: true,
        exact_empty: true,
        candidate_count_before_pruning: 4,
        pruned_candidate_count: 4,
        candidate_count_before_filter: 0,
        output_count: 0,
        filtered_out_count: 0,
    };
    let blocking = BlockingOperatorMemoryReport {
        operator: "SortExec".into(),
        budget_bytes: 1024,
        peak_tracked_bytes: 1000,
        input_rows: 30,
        max_spill_bytes: 2048,
        max_spill_runs: 2,
        spilled_bytes: 1024,
        spill_run_count: 1,
        spilled_rows: 20,
    };
    let receiver: &dyn ExecutionObserver = &observer;
    receiver.record_scan_pruning_report(scan.clone());
    receiver.record_blocking_memory_report(blocking.clone());
    observer.record_blocking_memory_report(blocking.clone());
    // Unregistered observers collect events without inventing operator identity.
    observer.record_operator_start(&PhysicalPlan::EmptyExec);
    observer.record_operator_output(&PhysicalPlan::EmptyExec, 9);
    let reports = observer.into_reports();
    assert!(reports.operator_cardinality.is_empty());
    assert_eq!(reports.vector_execution, [vector, last_vector]);
    assert_eq!(reports.graph_expansion, [graph]);
    assert_eq!(reports.scan_pruning, [scan]);
    assert_eq!(reports.blocking_memory, [blocking.clone(), blocking]);
    assert_eq!(reports.pipeline_memory, PipelineMemoryReport::default());
}

#[test]
fn blocking_inventory_is_sorted_and_deduplicated_across_branches() {
    let sort = || PhysicalPlan::SortExec {
        items: vec![],
        input: Box::new(PhysicalPlan::EmptyExec),
    };
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::DistinctExec {
            input: Box::new(sort()),
        }),
        right: Box::new(PhysicalPlan::LimitExec {
            offset: 0,
            limit: Some(1),
            input: Box::new(sort()),
        }),
    };
    assert_eq!(
        blocking_operator_kinds(&plan),
        ["DistinctExec", "NodeCartesianProductExec", "SortExec"]
    );
    assert!(blocking_operator_kinds(&PhysicalPlan::EmptyExec).is_empty());
}

// The reference model stores the input journal and computes totals in a wider
// integer at readout; it does not reuse the observer's incremental updates.
fn capped_sum(values: impl Iterator<Item = usize>) -> usize {
    values
        .map(|value| value as u128)
        .sum::<u128>()
        .min(usize::MAX as u128) as usize
}

fn maximum(values: impl Iterator<Item = usize>) -> usize {
    values.max().unwrap_or(0)
}

fn run_event_campaign(cases: usize) {
    let mut seed = 0x418_0b5e_7a11_u64;
    for case in 0..cases {
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::EmptyExec),
            right: Box::new(PhysicalPlan::EmptyExec),
        };
        let PlanChildren::Binary(left, right) = plan.children() else {
            unreachable!();
        };
        let operators = [&plan, left, right];
        let observer = QueryExecutionObserver::new(&plan);
        let untouched = QueryExecutionObserver::new(&plan);
        let mut row_events: [Vec<usize>; 3] = Default::default();
        let mut batches = Vec::new();
        let mut columnar = Vec::new();
        let mut morsels = Vec::new();
        let mut admissions = Vec::new();
        let mut buffering = Vec::new();
        for step in 0..64 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let value = match step % 8 {
                0 => usize::MAX,
                1 => 0,
                _ => (seed % 1024) as usize,
            };
            // Some streams leave one physical node entirely unobserved, even
            // though its shape is identical to its observed sibling's shape.
            let operator = (seed % if case % 2 == 0 { 2 } else { 3 }) as usize;
            if step % 3 == 0 {
                observer.record_operator_start(operators[operator]);
                row_events[operator].push(0);
            } else {
                observer.record_operator_output(operators[operator], value);
                row_events[operator].push(value);
            }
            let rows = (seed % 8) as usize;
            let bytes = ((seed >> 8) % 32) as usize;
            let batch = (0..rows)
                .map(|_| Binding::scalar("blob", Value::Binary(vec![case as u8; bytes])))
                .collect::<Vec<_>>();
            observer.record_pipeline_batch(&batch);
            batches.push((rows, rows * (4 + bytes)));
            observer.record_columnar_batch(value, value / 2);
            columnar.push((value, value / 2));
            observer.record_morsels(value);
            morsels.push(value);
            observer.record_morsel_admission(value, value / 2);
            admissions.push((value, value / 2));
            observer.record_morsel_buffering(value / 3, value, value / 4);
            buffering.push((value / 3, value, value / 4));
        }
        let reports = observer.into_reports();
        for (index, profile) in reports.operator_cardinality.iter().enumerate() {
            assert_eq!(profile.operator_id.ordinal(), index, "case {case}");
            assert_eq!(profile.operator, operators[index].kind(), "case {case}");
            let expected = (!row_events[index].is_empty())
                .then(|| capped_sum(row_events[index].iter().copied()));
            assert_eq!(
                profile.actual_rows, expected,
                "case {case}, operator {index}"
            );
        }
        assert_eq!(reports.operator_cardinality.len(), operators.len());
        let expected = PipelineMemoryReport {
            intermediate_rows: capped_sum(batches.iter().map(|event| event.0)),
            intermediate_payload_bytes: capped_sum(batches.iter().map(|event| event.1)),
            peak_batch_rows: maximum(batches.iter().map(|event| event.0)),
            peak_batch_payload_bytes: maximum(batches.iter().map(|event| event.1)),
            columnar_batches: columnar.len(),
            columnar_input_rows: capped_sum(columnar.iter().map(|event| event.0)),
            columnar_selected_rows: capped_sum(columnar.iter().map(|event| event.1)),
            morsel_count: capped_sum(morsels.into_iter()),
            morsel_max_admitted_workers: maximum(admissions.iter().map(|event| event.0)),
            morsel_peak_active_workers: maximum(admissions.iter().map(|event| event.1)),
            morsel_peak_buffered_outputs: maximum(buffering.iter().map(|event| event.0)),
            morsel_peak_buffered_output_bytes: maximum(buffering.iter().map(|event| event.1)),
            morsel_peak_reorder_entries: maximum(buffering.iter().map(|event| event.2)),
            ..PipelineMemoryReport::default()
        };
        assert_eq!(reports.pipeline_memory, expected, "case {case}");
        let untouched = untouched.into_reports();
        assert!(untouched
            .operator_cardinality
            .iter()
            .all(|report| report.actual_rows.is_none()));
        assert_eq!(untouched.pipeline_memory, PipelineMemoryReport::default());
    }
    println!("query observer differential cases: {cases}");
}

#[test]
fn query_observer_event_differential_smoke() {
    run_event_campaign(8);
}

#[test]
#[ignore = "explicit local query observer event campaign"]
fn query_observer_event_differential_campaign() {
    run_event_campaign(256);
}
