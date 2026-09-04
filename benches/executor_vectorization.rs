use serde_json::json;
use skein::executor::{
    execute_with_row_limit_profile, ExecutionMemoryConfig, QueryRowRef, QueryRows,
};
use skein::optimizer::PhysicalPlan;
use skein::planner::{ComparisonOp, Predicate, Projection, ProjectionExpression};
use skein::schema::{Catalog, PropertyType, TableKind};
use skein::store::{GraphSnapshotNodeImport, GraphStore, NodeId};
use skein::Value;
use skein_core::RuntimeTaskContext;
use skein_executor::{filter_numeric_column, ColumnVector, NumericLiteral, Selection, Validity};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const MICRO_ROWS: usize = 131_072;
const MICRO_ITERATIONS: usize = 64;
const VALUE_REF_ROWS: usize = 4_096;
const VALUE_REF_ITERATIONS: usize = 32;
const VALUE_REF_PAYLOAD_BYTES: usize = 4_096;
const END_TO_END_ROWS: usize = 65_536;
const END_TO_END_ITERATIONS: usize = 16;
const END_TO_END_PAYLOAD_BYTES: usize = 256;
const SAMPLES: usize = 11;
const MORSEL_MATRIX_SAMPLES: usize = 3;
const LOCAL_MORSEL_BENCHMARK_PROTOCOL: &str = "skein-local-morsel-benchmark-v1";
const LOCAL_MORSEL_SELECTIVITY_PROTOCOL: &str = "skein-local-morsel-selectivity-matrix-v1";
const MORSEL_SELECTIVITY_PERCENTAGES: [usize; 5] = [0, 1, 10, 50, 100];
const EXECUTOR_BENCH_MODE_ENV: &str = "SKEIN_EXECUTOR_BENCH_MODE";

#[path = "executor_vectorization/adjacency.rs"]
mod adjacency;
#[path = "executor_vectorization/morsel.rs"]
mod morsel;

fn main() {
    let requested_workers = morsel::benchmark_workers();
    let mode = std::env::var(EXECUTOR_BENCH_MODE_ENV).unwrap_or_else(|_| "full".to_string());
    assert!(
        matches!(
            mode.as_str(),
            "full" | "micro" | "scheduler" | "morsel" | "adjacency" | "value-ref"
        ),
        "{EXECUTOR_BENCH_MODE_ENV} must be full, micro, scheduler, morsel, adjacency, or value-ref"
    );
    let full = mode == "full";
    let micro = matches!(mode.as_str(), "full" | "micro").then(micro_benchmark);
    let float_micro = matches!(mode.as_str(), "full" | "micro").then(float_micro_benchmark);
    let value_ref = matches!(mode.as_str(), "full" | "value-ref").then(value_ref_benchmark);
    let (end_to_end, production_morsel, morsel_selectivity) = if matches!(
        mode.as_str(),
        "micro" | "scheduler" | "adjacency" | "value-ref"
    ) {
        (None, None, None)
    } else {
        let (comparison, production, selectivity) = end_to_end_benchmark(requested_workers, full);
        (comparison, Some(production), selectivity)
    };
    let morsel = matches!(mode.as_str(), "full" | "scheduler")
        .then(|| morsel::scheduler_benchmark(requested_workers));
    let adjacency_limit = matches!(mode.as_str(), "full" | "adjacency").then(adjacency::benchmark);
    if let Some(micro) = micro {
        assert_eq!(micro.row_checksum, micro.columnar_checksum);
    }
    if let Some(float_micro) = float_micro {
        assert_eq!(float_micro.row_checksum, float_micro.columnar_checksum);
    }
    if let Some(end_to_end) = end_to_end {
        assert_eq!(end_to_end.row_checksum, end_to_end.columnar_checksum);
    }

    println!(
        "executor_vectorization {}",
        json!({
            "micro": micro.map(ComparisonReport::json),
            "float_micro": float_micro.map(ComparisonReport::json),
            "value_ref": value_ref,
            "end_to_end": end_to_end.map(ComparisonReport::json),
            "morsel": morsel,
            "production_morsel": production_morsel,
            "morsel_selectivity": morsel_selectivity,
            "adjacency_limit": adjacency_limit,
            "end_to_end_payload_bytes_per_row": END_TO_END_PAYLOAD_BYTES,
            "mode": mode,
        })
    );
}

fn value_ref_benchmark() -> serde_json::Value {
    let payload = "v".repeat(VALUE_REF_PAYLOAD_BYTES);
    let values = (0..VALUE_REF_ROWS)
        .map(|_| payload.clone())
        .collect::<Vec<_>>();
    let column = ColumnVector::Utf8 {
        values: values.into(),
        validity: Validity::all(VALUE_REF_ROWS),
    };

    let (owned_ns, owned_checksum, borrowed_ns, borrowed_checksum) =
        paired_median_sample(VALUE_REF_ITERATIONS, |path| match path {
            ExecutionPath::Row => {
                let mut checksum = 0u64;
                for row in 0..VALUE_REF_ROWS {
                    let value = black_box(&column)
                        .value(row)
                        .expect("benchmark value must exist");
                    let Value::String(value) = black_box(value) else {
                        unreachable!("benchmark value is UTF-8")
                    };
                    checksum = checksum.wrapping_add(value.len() as u64);
                }
                black_box(checksum)
            }
            ExecutionPath::Columnar => {
                let mut checksum = 0u64;
                for row in 0..VALUE_REF_ROWS {
                    let value = black_box(&column)
                        .value_ref(row)
                        .expect("benchmark value ref must exist");
                    checksum = checksum.wrapping_add(
                        value.as_str().expect("benchmark value ref is UTF-8").len() as u64,
                    );
                    black_box(value);
                }
                black_box(checksum)
            }
        });
    assert_eq!(owned_checksum, borrowed_checksum);
    json!({
        "rows": VALUE_REF_ROWS,
        "iterations": VALUE_REF_ITERATIONS,
        "payload_bytes_per_row": VALUE_REF_PAYLOAD_BYTES,
        "owned_ns": owned_ns,
        "borrowed_ns": borrowed_ns,
        "speedup": owned_ns as f64 / borrowed_ns.max(1) as f64,
        "owned_payload_bytes_copied": VALUE_REF_ROWS
            .saturating_mul(VALUE_REF_ITERATIONS)
            .saturating_mul(VALUE_REF_PAYLOAD_BYTES),
        "borrowed_payload_bytes_copied": 0,
        "checksum": borrowed_checksum,
    })
}

fn micro_benchmark() -> ComparisonReport {
    let rows = (0..MICRO_ROWS)
        .map(|row| {
            BTreeMap::from([
                ("score".to_string(), Value::Int(row as i64)),
                ("payload".to_string(), Value::Int((row % 17) as i64)),
            ])
        })
        .collect::<Vec<_>>();
    let column = ColumnVector::int64(
        (0..MICRO_ROWS).map(|row| row as i64).collect(),
        Validity::all(MICRO_ROWS),
    )
    .expect("micro benchmark column must be valid");
    let threshold = (MICRO_ROWS * 7 / 8) as i64;
    let expected = Value::Int(threshold);

    let (row_ns, row_checksum, columnar_ns, columnar_checksum) =
        paired_median_sample(MICRO_ITERATIONS, |path| match path {
            ExecutionPath::Row => {
                let mut checksum = 0u64;
                for row in black_box(&rows) {
                    let value = row.get("score").expect("score must exist");
                    if skein_executor::predicate::compare_property_values(
                        value,
                        ComparisonOp::Gte,
                        &expected,
                    ) {
                        checksum = checksum.wrapping_add(match value {
                            Value::Int(value) => *value as u64,
                            _ => unreachable!("score is an integer"),
                        });
                    }
                }
                black_box(checksum)
            }
            ExecutionPath::Columnar => {
                let selection = filter_numeric_column(
                    black_box(&column),
                    &Selection::all(MICRO_ROWS),
                    ComparisonOp::Gte,
                    NumericLiteral::Int(threshold),
                )
                .expect("columnar filter must succeed");
                black_box(
                    selection
                        .iter()
                        .fold(0u64, |total, row| total.wrapping_add(row as u64)),
                )
            }
        });

    ComparisonReport {
        rows: MICRO_ROWS,
        iterations: MICRO_ITERATIONS,
        row_ns,
        columnar_ns,
        row_checksum,
        columnar_checksum,
    }
}

fn float_micro_benchmark() -> ComparisonReport {
    let rows = (0..MICRO_ROWS)
        .map(|row| {
            BTreeMap::from([
                ("score".to_string(), Value::Float(row as f64)),
                ("row".to_string(), Value::Int(row as i64)),
            ])
        })
        .collect::<Vec<_>>();
    let column = ColumnVector::float64(
        (0..MICRO_ROWS).map(|row| row as f64).collect(),
        Validity::all(MICRO_ROWS),
    )
    .expect("float micro benchmark column must be valid");
    let threshold = (MICRO_ROWS * 7 / 8) as f64;
    let expected = Value::Float(threshold);

    let (row_ns, row_checksum, columnar_ns, columnar_checksum) =
        paired_median_sample(MICRO_ITERATIONS, |path| match path {
            ExecutionPath::Row => {
                let mut checksum = 0u64;
                for row in black_box(&rows) {
                    let value = row.get("score").expect("score must exist");
                    if skein_executor::predicate::compare_property_values(
                        value,
                        ComparisonOp::Gte,
                        &expected,
                    ) {
                        checksum = checksum.wrapping_add(match row.get("row") {
                            Some(Value::Int(row)) => *row as u64,
                            _ => unreachable!("row is an integer"),
                        });
                    }
                }
                black_box(checksum)
            }
            ExecutionPath::Columnar => {
                let selection = filter_numeric_column(
                    black_box(&column),
                    &Selection::all(MICRO_ROWS),
                    ComparisonOp::Gte,
                    NumericLiteral::Float(threshold),
                )
                .expect("columnar float filter must succeed");
                black_box(
                    selection
                        .iter()
                        .fold(0u64, |total, row| total.wrapping_add(row as u64)),
                )
            }
        });

    ComparisonReport {
        rows: MICRO_ROWS,
        iterations: MICRO_ITERATIONS,
        row_ns,
        columnar_ns,
        row_checksum,
        columnar_checksum,
    }
}

fn end_to_end_benchmark(
    requested_workers: NonZeroUsize,
    include_vectorization_comparison: bool,
) -> (
    Option<ComparisonReport>,
    serde_json::Value,
    Option<serde_json::Value>,
) {
    let workload_rows = if include_vectorization_comparison {
        END_TO_END_ROWS
    } else {
        morsel::benchmark_production_rows(requested_workers)
    };
    let iterations = if include_vectorization_comparison {
        END_TO_END_ITERATIONS
    } else {
        1
    };
    let samples = if include_vectorization_comparison {
        SAMPLES
    } else {
        MORSEL_MATRIX_SAMPLES
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(512 * 1024 * 1024)
            .expect("benchmark query memory budget is non-zero"),
        ..ExecutionMemoryConfig::default()
    };
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", PropertyType::Int, false);
    catalog.get_or_create_property(table, "payload", PropertyType::String, false);
    let mut store = GraphStore::in_memory();
    let payload = "x".repeat(END_TO_END_PAYLOAD_BYTES);
    let nodes = (0..workload_rows)
        .map(|row| -> GraphSnapshotNodeImport {
            (
                NodeId(row as u64),
                "Item".to_string(),
                BTreeMap::from([
                    ("score".to_string(), Value::Int(row as i64)),
                    ("payload".to_string(), Value::String(payload.clone())),
                ]),
            )
        })
        .collect();
    store
        .import_graph_snapshot_rows(&mut catalog, nodes, Vec::new())
        .expect("benchmark node import must succeed");
    let threshold = if include_vectorization_comparison {
        workload_rows * 7 / 8
    } else {
        workload_rows.saturating_sub(workload_rows / 1024)
    } as i64;
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: ComparisonOp::Gte,
        value: Value::Int(threshold),
    };
    let columnar_plan = projection_plan(compare.clone());
    let comparison = include_vectorization_comparison.then(|| {
        let row_plan = projection_plan(Predicate::And(vec![compare]));
        let columnar_probe =
            execute_with_row_limit_profile(&columnar_plan, &mut catalog, &mut store, None)
                .expect("columnar probe must succeed");
        assert!(
            columnar_probe
                .profile
                .pipeline_memory_report
                .columnar_batches
                > 0
        );
        let row_probe = execute_with_row_limit_profile(&row_plan, &mut catalog, &mut store, None)
            .expect("row probe must succeed");
        assert_eq!(row_probe.profile.pipeline_memory_report.columnar_batches, 0);
        assert_eq!(columnar_probe.rows, row_probe.rows);

        let (row_ns, row_checksum, columnar_ns, columnar_checksum) =
            paired_median_sample(END_TO_END_ITERATIONS, |path| {
                let plan = match path {
                    ExecutionPath::Row => &row_plan,
                    ExecutionPath::Columnar => &columnar_plan,
                };
                let output =
                    execute_with_row_limit_profile(black_box(plan), &mut catalog, &mut store, None)
                        .expect("benchmark execution must succeed");
                black_box(output_checksum(&output.rows))
            });

        ComparisonReport {
            rows: END_TO_END_ROWS,
            iterations: END_TO_END_ITERATIONS,
            row_ns,
            columnar_ns,
            row_checksum,
            columnar_checksum,
        }
    });
    let serial_context = RuntimeTaskContext::default();
    let parallel_context =
        RuntimeTaskContext::default().with_admitted_parallelism(requested_workers);
    let mut serial_samples = Vec::with_capacity(samples);
    let mut parallel_samples = Vec::with_capacity(samples);
    let mut serial_checksum = 0u64;
    let mut parallel_checksum = 0u64;
    for sample in 0..samples {
        let parallel_first = sample % 2 == 1;
        for parallel in [parallel_first, !parallel_first] {
            let context = if parallel {
                &parallel_context
            } else {
                &serial_context
            };
            let started = Instant::now();
            for _ in 0..iterations {
                let execution = morsel::stream_probe(
                    black_box(&columnar_plan),
                    &mut catalog,
                    &mut store,
                    context,
                    &memory,
                );
                let checksum = black_box(execution.checksum);
                if parallel {
                    parallel_checksum = checksum;
                } else {
                    serial_checksum = checksum;
                }
            }
            if parallel {
                parallel_samples.push(started.elapsed().as_nanos());
            } else {
                serial_samples.push(started.elapsed().as_nanos());
            }
        }
    }
    assert_eq!(serial_checksum, parallel_checksum);
    serial_samples.sort_unstable();
    parallel_samples.sort_unstable();
    let serial_ns = morsel::percentile(&serial_samples, 50);
    let parallel_ns = morsel::percentile(&parallel_samples, 50);
    let serial_probe = morsel::stream_probe(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &serial_context,
        &memory,
    );
    let parallel_probe = morsel::stream_probe(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &parallel_context,
        &memory,
    );
    assert_eq!(serial_probe.checksum, parallel_probe.checksum);
    assert!(serial_probe.fully_streamed && parallel_probe.fully_streamed);
    assert_eq!(serial_probe.output_rows, parallel_probe.output_rows);
    assert_eq!(parallel_probe.max_admitted_workers, requested_workers.get());
    assert_eq!(parallel_probe.peak_active_workers, requested_workers.get());
    assert!(parallel_probe.peak_buffered_outputs <= requested_workers.get());
    assert!(parallel_probe.peak_reorder_entries <= requested_workers.get());
    assert!(parallel_probe.query_memory_peak_bytes <= memory.query_memory_bytes.get());
    assert_eq!(parallel_probe.query_memory_completion_bytes, 0);
    assert_eq!(parallel_probe.spilled_bytes, 0);
    assert_eq!(parallel_probe.spill_run_count, 0);
    let serial_ns_per_iteration = serial_ns as f64 / iterations as f64;
    let parallel_ns_per_iteration = parallel_ns as f64 / iterations as f64;
    let production_morsel = json!({
        "protocol": LOCAL_MORSEL_BENCHMARK_PROTOCOL,
        "evidence_kind": "local_kernel_diagnostic",
        "production_eligible": false,
        "process_id": std::process::id(),
        "rows": workload_rows,
        "batch_rows": memory.batch_rows,
        "requested_workers": requested_workers,
        "output_rows": parallel_probe.output_rows,
        "morsel_count": parallel_probe.morsel_count,
        "morsel_max_admitted_workers": parallel_probe.max_admitted_workers,
        "morsel_peak_active_workers": parallel_probe.peak_active_workers,
        "morsel_peak_buffered_outputs": parallel_probe.peak_buffered_outputs,
        "morsel_peak_buffered_output_bytes": parallel_probe.peak_buffered_output_bytes,
        "morsel_peak_reorder_entries": parallel_probe.peak_reorder_entries,
        "query_memory_budget_bytes": memory.query_memory_bytes,
        "query_memory_peak_bytes": parallel_probe.query_memory_peak_bytes,
        "query_memory_completion_bytes": parallel_probe.query_memory_completion_bytes,
        "spilled_bytes": parallel_probe.spilled_bytes,
        "spill_run_count": parallel_probe.spill_run_count,
        "iterations_per_sample": iterations,
        "samples": samples,
        "serial_p50_ns": serial_ns,
        "serial_p95_ns": morsel::percentile(&serial_samples, 95),
        "serial_p99_ns": morsel::percentile(&serial_samples, 99),
        "parallel_p50_ns": parallel_ns,
        "parallel_p95_ns": morsel::percentile(&parallel_samples, 95),
        "parallel_p99_ns": morsel::percentile(&parallel_samples, 99),
        "serial_rows_per_second": workload_rows as f64 * 1_000_000_000.0
            / serial_ns_per_iteration,
        "parallel_rows_per_second": workload_rows as f64 * 1_000_000_000.0
            / parallel_ns_per_iteration,
        "speedup": serial_ns as f64 / parallel_ns.max(1) as f64,
        "improved": parallel_ns < serial_ns,
        "steady_resident_bytes": parallel_probe.steady_resident_bytes,
        "peak_resident_bytes": parallel_probe.peak_resident_bytes,
        "minor_page_faults": parallel_probe.minor_page_faults,
        "major_page_faults": parallel_probe.major_page_faults,
        "checksum": parallel_checksum,
    });
    let selectivity = (!include_vectorization_comparison).then(|| {
        benchmark_morsel_selectivity_matrix(MorselSelectivityContext {
            workload_rows,
            requested_workers,
            catalog: &mut catalog,
            store: &mut store,
            serial_context: &serial_context,
            parallel_context: &parallel_context,
            memory: &memory,
        })
    });
    (comparison, production_morsel, selectivity)
}

struct MorselSelectivityContext<'a> {
    workload_rows: usize,
    requested_workers: NonZeroUsize,
    catalog: &'a mut Catalog,
    store: &'a mut GraphStore,
    serial_context: &'a RuntimeTaskContext,
    parallel_context: &'a RuntimeTaskContext,
    memory: &'a ExecutionMemoryConfig,
}

fn benchmark_morsel_selectivity_matrix(context: MorselSelectivityContext<'_>) -> serde_json::Value {
    let MorselSelectivityContext {
        workload_rows,
        requested_workers,
        catalog,
        store,
        serial_context,
        parallel_context,
        memory,
    } = context;
    let cases = MORSEL_SELECTIVITY_PERCENTAGES
        .into_iter()
        .map(|selectivity_percent| {
            let expected_rows = workload_rows
                .saturating_mul(selectivity_percent)
                .div_ceil(100);
            let threshold = workload_rows.saturating_sub(expected_rows) as i64;
            let plan = projection_plan(Predicate::PropertyCompare {
                variable: "n".to_string(),
                property: "score".to_string(),
                op: ComparisonOp::Gte,
                value: Value::Int(threshold),
            });
            let mut serial_samples = Vec::with_capacity(MORSEL_MATRIX_SAMPLES);
            let mut parallel_samples = Vec::with_capacity(MORSEL_MATRIX_SAMPLES);
            let mut serial_checksum = 0u64;
            let mut parallel_checksum = 0u64;
            for sample in 0..MORSEL_MATRIX_SAMPLES {
                let parallel_first = sample % 2 == 1;
                for parallel in [parallel_first, !parallel_first] {
                    let context = if parallel {
                        parallel_context
                    } else {
                        serial_context
                    };
                    let started = Instant::now();
                    let probe =
                        morsel::stream_probe(black_box(&plan), catalog, store, context, memory);
                    assert_eq!(probe.output_rows, expected_rows);
                    if parallel {
                        parallel_checksum = black_box(probe.checksum);
                        parallel_samples.push(started.elapsed().as_nanos());
                    } else {
                        serial_checksum = black_box(probe.checksum);
                        serial_samples.push(started.elapsed().as_nanos());
                    }
                }
            }
            assert_eq!(serial_checksum, parallel_checksum);
            serial_samples.sort_unstable();
            parallel_samples.sort_unstable();
            let probe = morsel::stream_probe(&plan, catalog, store, parallel_context, memory);
            assert!(probe.fully_streamed);
            assert_eq!(probe.output_rows, expected_rows);
            assert_eq!(probe.max_admitted_workers, requested_workers.get());
            assert_eq!(probe.peak_active_workers, requested_workers.get());
            assert!(probe.peak_buffered_outputs <= requested_workers.get());
            assert!(probe.peak_reorder_entries <= requested_workers.get());
            assert!(
                probe.peak_buffered_output_bytes
                    <= memory
                        .batch_payload_bytes
                        .get()
                        .saturating_mul(requested_workers.get())
            );
            assert!(probe.query_memory_peak_bytes <= memory.query_memory_bytes.get());
            assert_eq!(probe.query_memory_completion_bytes, 0);
            assert_eq!(probe.spilled_bytes, 0);
            assert_eq!(probe.spill_run_count, 0);

            json!({
                "selectivity_percent": selectivity_percent,
                "expected_rows": expected_rows,
                "serial_p50_ns": morsel::percentile(&serial_samples, 50),
                "serial_p95_ns": morsel::percentile(&serial_samples, 95),
                "serial_p99_ns": morsel::percentile(&serial_samples, 99),
                "parallel_p50_ns": morsel::percentile(&parallel_samples, 50),
                "parallel_p95_ns": morsel::percentile(&parallel_samples, 95),
                "parallel_p99_ns": morsel::percentile(&parallel_samples, 99),
                "morsel_count": probe.morsel_count,
                "morsel_max_admitted_workers": probe.max_admitted_workers,
                "morsel_peak_active_workers": probe.peak_active_workers,
                "morsel_peak_buffered_outputs": probe.peak_buffered_outputs,
                "morsel_peak_buffered_output_bytes": probe.peak_buffered_output_bytes,
                "morsel_peak_reorder_entries": probe.peak_reorder_entries,
                "query_memory_peak_bytes": probe.query_memory_peak_bytes,
                "query_memory_completion_bytes": probe.query_memory_completion_bytes,
                "spilled_bytes": probe.spilled_bytes,
                "spill_run_count": probe.spill_run_count,
                "steady_resident_bytes": probe.steady_resident_bytes,
                "peak_resident_bytes": probe.peak_resident_bytes,
                "minor_page_faults": probe.minor_page_faults,
                "major_page_faults": probe.major_page_faults,
                "checksum": probe.checksum,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "protocol": LOCAL_MORSEL_SELECTIVITY_PROTOCOL,
        "evidence_kind": "local_kernel_diagnostic",
        "production_eligible": false,
        "rows": workload_rows,
        "requested_workers": requested_workers,
        "query_memory_budget_bytes": memory.query_memory_bytes,
        "batch_payload_budget_bytes": memory.batch_payload_bytes,
        "samples_per_case": MORSEL_MATRIX_SAMPLES,
        "cases": cases,
    })
}

fn projection_plan(predicate: Predicate) -> PhysicalPlan {
    PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        }],
        input: Box::new(PhysicalPlan::FilterExec {
            predicate,
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    }
}

fn output_checksum(rows: &QueryRows) -> u64 {
    rows.iter().fold(0u64, |total, row| {
        total.wrapping_add(output_query_row_score(row))
    })
}

fn output_query_row_score(row: QueryRowRef<'_>) -> u64 {
    output_score(row.get("score"))
}

fn output_row_score(row: &BTreeMap<String, Value>) -> u64 {
    output_score(row.get("score"))
}

fn output_score(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Int(value)) => *value as u64,
        _ => panic!("benchmark output score must be an integer"),
    }
}

fn paired_median_sample(
    iterations: usize,
    mut operation: impl FnMut(ExecutionPath) -> u64,
) -> (u128, u64, u128, u64) {
    black_box(operation(ExecutionPath::Row));
    black_box(operation(ExecutionPath::Columnar));
    let mut row_samples = Vec::with_capacity(SAMPLES);
    let mut columnar_samples = Vec::with_capacity(SAMPLES);
    let mut row_checksum = 0u64;
    let mut columnar_checksum = 0u64;
    for sample in 0..SAMPLES {
        let order = if sample % 2 == 0 {
            [ExecutionPath::Row, ExecutionPath::Columnar]
        } else {
            [ExecutionPath::Columnar, ExecutionPath::Row]
        };
        for path in order {
            let started = Instant::now();
            for _ in 0..iterations {
                let checksum = operation(path);
                match path {
                    ExecutionPath::Row => row_checksum = checksum,
                    ExecutionPath::Columnar => columnar_checksum = checksum,
                }
            }
            match path {
                ExecutionPath::Row => row_samples.push(started.elapsed().as_nanos()),
                ExecutionPath::Columnar => columnar_samples.push(started.elapsed().as_nanos()),
            }
        }
    }
    row_samples.sort_unstable();
    columnar_samples.sort_unstable();
    (
        row_samples[row_samples.len() / 2],
        row_checksum,
        columnar_samples[columnar_samples.len() / 2],
        columnar_checksum,
    )
}

#[derive(Debug, Clone, Copy)]
enum ExecutionPath {
    Row,
    Columnar,
}

#[derive(Debug, Clone, Copy)]
struct ComparisonReport {
    rows: usize,
    iterations: usize,
    row_ns: u128,
    columnar_ns: u128,
    row_checksum: u64,
    columnar_checksum: u64,
}

impl ComparisonReport {
    fn json(self) -> serde_json::Value {
        let row_ns_per_iteration = self.row_ns as f64 / self.iterations as f64;
        let columnar_ns_per_iteration = self.columnar_ns as f64 / self.iterations as f64;
        json!({
            "rows": self.rows,
            "iterations_per_sample": self.iterations,
            "samples": SAMPLES,
            "row_median_ns": self.row_ns,
            "columnar_median_ns": self.columnar_ns,
            "row_ns_per_iteration": row_ns_per_iteration,
            "columnar_ns_per_iteration": columnar_ns_per_iteration,
            "row_rows_per_second": self.rows as f64 * 1_000_000_000.0 / row_ns_per_iteration,
            "columnar_rows_per_second": self.rows as f64 * 1_000_000_000.0 / columnar_ns_per_iteration,
            "speedup": self.row_ns as f64 / self.columnar_ns as f64,
            "improved": self.columnar_ns < self.row_ns,
            "row_checksum": self.row_checksum,
            "columnar_checksum": self.columnar_checksum,
        })
    }
}
