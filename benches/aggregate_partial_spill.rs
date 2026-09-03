use serde_json::json;
use skein::executor::{
    execute_with_row_limit_profile_and_external_and_memory, ExecutionMemoryConfig,
};
use skein::optimizer::PhysicalPlan;
use skein::planner::{
    AggregateFunction, AggregateTarget, Aggregation, Projection, ProjectionExpression,
};
use skein::schema::Catalog;
use skein::store::{GraphSnapshotNodeImport, GraphStore, NodeId};
use skein::Value;
use skein_executor::external::NoExternalReadOperator;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const INPUT_ROWS: usize = 4_096;
const GROUP_COUNT: usize = 256;
const UNUSED_PAYLOAD_BYTES: usize = 4_096;
const SAMPLES: usize = 5;

fn main() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let payload = "x".repeat(UNUSED_PAYLOAD_BYTES);
    let nodes = (0..INPUT_ROWS)
        .map(|row| -> GraphSnapshotNodeImport {
            (
                NodeId(row as u64),
                "Item".to_string(),
                BTreeMap::from([
                    ("group".to_string(), Value::Int((row % GROUP_COUNT) as i64)),
                    ("value".to_string(), Value::Int(row as i64)),
                    ("payload".to_string(), Value::String(payload.clone())),
                ]),
            )
        })
        .collect();
    store
        .import_graph_snapshot_rows(&mut catalog, nodes, Vec::new())
        .expect("benchmark graph import must succeed");
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::All,
            distinct: false,
            name: "count".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let compact_plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property {
                variable: "n".to_string(),
                property: "value".to_string(),
            },
            distinct: true,
            name: "count".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let spill_directory = std::env::temp_dir().join(format!(
        "skein-aggregate-partial-spill-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
        batch_rows: NonZeroUsize::new(1_024).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
        max_spill_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        max_spill_runs: NonZeroUsize::new(128).unwrap(),
        max_total_spill_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
        max_total_spill_runs: NonZeroUsize::new(256).unwrap(),
        min_spill_free_bytes: NonZeroU64::MIN,
        spill_free_space_probe_interval_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        spill_orphan_grace_period: std::time::Duration::ZERO,
        spill_directory: spill_directory.clone(),
    };
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut spill_bytes = 0u64;
    let mut spill_runs = 0usize;
    let mut total_spill_bytes = 0u64;
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .expect("benchmark aggregation must succeed");
        samples.push(started.elapsed().as_nanos());
        assert_eq!(output.rows.len(), GROUP_COUNT);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "AggregateExec")
            .expect("benchmark aggregation must report memory");
        spill_bytes = report.spilled_bytes;
        spill_runs = report.spill_run_count;
        total_spill_bytes = total_spill_bytes.saturating_add(report.spilled_bytes);
        black_box(output.rows);
    }
    samples.sort_unstable();
    let mut compact_samples = Vec::with_capacity(SAMPLES);
    let mut compact_spill_bytes = 0u64;
    let mut compact_spill_runs = 0usize;
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &compact_plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .expect("benchmark compact aggregation must succeed");
        compact_samples.push(started.elapsed().as_nanos());
        assert_eq!(output.rows.len(), GROUP_COUNT);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "AggregateExec")
            .expect("benchmark compact aggregation must report memory");
        compact_spill_bytes = report.spilled_bytes;
        compact_spill_runs = report.spill_run_count;
        total_spill_bytes = total_spill_bytes.saturating_add(report.spilled_bytes);
        black_box(output.rows);
    }
    compact_samples.sort_unstable();
    let full_binding_payload_bytes = (INPUT_ROWS * UNUSED_PAYLOAD_BYTES) as u64;
    assert!(spill_bytes < full_binding_payload_bytes);
    assert!(compact_spill_bytes < full_binding_payload_bytes);
    let spill_pool = memory
        .spill_pool_snapshot()
        .expect("benchmark spill pool snapshot must succeed");
    assert!(
        spill_pool.free_space_probe_count
            <= total_spill_bytes.div_ceil(memory.spill_free_space_probe_interval_bytes.get())
    );
    println!(
        "aggregate_partial_spill {}",
        json!({
            "input_rows": INPUT_ROWS,
            "group_count": GROUP_COUNT,
            "unused_payload_bytes_per_row": UNUSED_PAYLOAD_BYTES,
            "full_binding_payload_bytes": full_binding_payload_bytes,
            "partial_spill_bytes": spill_bytes,
            "spill_reduction_ratio": full_binding_payload_bytes as f64 / spill_bytes as f64,
            "spill_run_count": spill_runs,
            "compact_spill_bytes": compact_spill_bytes,
            "compact_spill_reduction_ratio": full_binding_payload_bytes as f64 / compact_spill_bytes as f64,
            "compact_spill_run_count": compact_spill_runs,
            "free_space_probe_interval_bytes": spill_pool.free_space_probe_interval_bytes,
            "free_space_probe_count": spill_pool.free_space_probe_count,
            "total_spill_bytes": total_spill_bytes,
            "compact_median_nanoseconds": compact_samples[compact_samples.len() / 2],
            "compact_min_nanoseconds": compact_samples[0],
            "compact_max_nanoseconds": compact_samples[compact_samples.len() - 1],
            "samples": SAMPLES,
            "median_nanoseconds": samples[samples.len() / 2],
            "min_nanoseconds": samples[0],
            "max_nanoseconds": samples[samples.len() - 1],
        })
    );
    std::fs::remove_dir(spill_directory).expect("benchmark spill directory must be empty");
}
