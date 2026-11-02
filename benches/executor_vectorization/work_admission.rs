// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual comparison of ordinary reads with a deadline and no work cap.

use hawdb::executor::{
    execute_with_request_consumer, ExecutionMemoryConfig, ExecutionRequest, ExecutionResources,
    NoExternalReadOperator,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::planner::{
    ComparisonOp, GraphExpansionBudget, Predicate, Projection, ProjectionExpression,
};
use hawdb::schema::Catalog;
use hawdb::store::{GraphStore, NodeId, RelId};
use hawdb::{RelationshipDirection, Value};
use hawdb_core::RuntimeTaskContext;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

const WARMUPS: usize = 3;
const SAMPLES: usize = 11;
const ITERATIONS: usize = 8;
const SCAN_ROWS: usize = 8_192;

pub(super) fn benchmark() -> JsonValue {
    let mut results = Vec::new();
    for degree in [32, 1_024] {
        let (mut catalog, mut store) = fixture(degree, true);
        results.push(measure(
            "traversal",
            degree,
            &traversal_plan(degree),
            &mut catalog,
            &mut store,
        ));
    }
    let (mut catalog, mut store) = fixture(SCAN_ROWS, false);
    // AND keeps the predicate on the row path, where task checks are per row.
    let plan = super::projection_plan(Predicate::And(vec![Predicate::PropertyCompare {
        variable: "n".into(),
        property: "score".into(),
        op: ComparisonOp::Gte,
        value: Value::Int(0),
    }]));
    results.push(measure("scan", SCAN_ROWS, &plan, &mut catalog, &mut store));
    json!({
        "protocol": "hawdb-local-work-admission-benchmark-v1",
        "evidence_kind": "local_kernel_diagnostic",
        "production_eligible": false,
        "deadline_seconds": 30,
        "work_limit": null,
        "warmups": WARMUPS,
        "iterations_per_sample": ITERATIONS,
        "results": results,
    })
}

fn measure(
    name: &str,
    rows: usize,
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
) -> JsonValue {
    let parameters = BTreeMap::new();
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let expected_checksum = (rows as u64) * (rows as u64 - 1) / 2;
    let mut peak_bytes = 0;
    let mut run = || {
        let task = RuntimeTaskContext::with_timeout(Duration::from_secs(30));
        let mut count = 0;
        let mut checksum = 0u64;
        let output = execute_with_request_consumer(
            ExecutionRequest::new(black_box(plan), &parameters, &memory).with_task_context(&task),
            ExecutionResources::new(catalog, store, &mut external),
            &mut |row| {
                let Some(Value::Int(value)) = row.get("score") else {
                    panic!("score must be an integer");
                };
                count += 1;
                checksum = checksum.wrapping_add(*value as u64);
                Ok(())
            },
        )
        .expect("work admission benchmark must succeed");
        assert_eq!(count, rows);
        assert_eq!(black_box(checksum), expected_checksum);
        assert_eq!(task.work_units(), 0);
        let report = &output.profile.pipeline_memory_report;
        assert_eq!(report.query_memory_completion_bytes, 0);
        if name == "scan" {
            assert_eq!(report.columnar_batches, 0);
        }
        peak_bytes = peak_bytes.max(report.query_memory_peak_bytes);
    };
    for _ in 0..WARMUPS {
        run();
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            run();
        }
        samples.push(started.elapsed().as_nanos() / ITERATIONS as u128);
    }
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    json!({
        "name": name,
        "rows": rows,
        "samples_ns": samples,
        "median_ns": sorted[SAMPLES / 2],
        "p95_ns": sorted[(SAMPLES - 1) * 95 / 100 + 1],
        "checksum": expected_checksum,
        "query_memory_peak_bytes": peak_bytes,
        "query_memory_completion_bytes": 0,
    })
}

fn fixture(rows: usize, edges: bool) -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let mut nodes = (0..rows)
        .map(|index| {
            (
                NodeId(index as u64 + 1),
                "Item".to_string(),
                BTreeMap::from([("score".to_string(), Value::Int(index as i64))]),
            )
        })
        .collect::<Vec<_>>();
    let relationships = if edges {
        nodes.push((
            NodeId(0),
            "Seed".into(),
            BTreeMap::from([("stable_id".into(), Value::Int(0))]),
        ));
        (0..rows)
            .map(|index| {
                (
                    RelId(index as u64),
                    NodeId(0),
                    NodeId(index as u64 + 1),
                    "LINK".to_string(),
                    BTreeMap::new(),
                )
            })
            .collect()
    } else {
        Vec::new()
    };
    store
        .import_graph_snapshot_rows(&mut catalog, nodes, relationships)
        .unwrap();
    if edges {
        store
            .create_property_index(&mut catalog, "Seed", "stable_id")
            .unwrap();
    }
    (catalog, store)
}

fn traversal_plan(degree: usize) -> PhysicalPlan {
    PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "target".into(),
                property: "score".into(),
            },
            name: "score".into(),
        }],
        input: Box::new(PhysicalPlan::AdjacencyExpandExec {
            source_variable: "seed".into(),
            source_label: "Seed".into(),
            rel_variable: None,
            rel_type: "LINK".into(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Outgoing,
            target_variable: "target".into(),
            target_label: "Item".into(),
            min_hops: 1,
            max_hops: 1,
            optional: false,
            graph_budget: Some(GraphExpansionBudget {
                candidate_limit: degree + 1,
                payload_byte_limit: 16 * 1024 * 1024,
            }),
            input: Box::new(PhysicalPlan::IndexNodeSeek {
                variable: "seed".into(),
                label: "Seed".into(),
                property: "stable_id".into(),
                value: Value::Int(0),
            }),
        }),
    }
}
