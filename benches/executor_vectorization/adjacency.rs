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

use hawdb::executor::{
    execute_with_row_consumer_profile_and_external_and_memory, ExecutionMemoryConfig,
    ProfiledQueryStream,
};
use hawdb::optimizer::PhysicalPlan;
use hawdb::planner::{GraphExpansionBudget, Projection, ProjectionExpression};
use hawdb::schema::Catalog;
use hawdb::store::{
    GraphSnapshotNodeImport, GraphSnapshotRelationshipImport, GraphStore, NodeId, RelId,
};
use hawdb::{RelationshipDirection, Value};
use hawdb_executor::external::NoExternalReadOperator;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const LIMIT: usize = 50;
const BATCH_ROWS: usize = 16;
const GRAPH_PAYLOAD_BUDGET_BYTES: usize = 1024 * 1024;
const RELEASE_DEGREES: &[usize] = &[1, 32, 1_024, 100_000];
const DEBUG_DEGREES: &[usize] = &[1, 32, 1_024];

pub fn benchmark() -> JsonValue {
    let results = benchmark_degrees()
        .iter()
        .copied()
        .map(benchmark_degree)
        .collect::<Vec<_>>();
    json!({
        "protocol": "hawdb-local-adjacency-limit-benchmark-v1",
        "evidence_kind": "local_kernel_diagnostic",
        "production_eligible": false,
        "limit": LIMIT,
        "batch_rows": BATCH_ROWS,
        "degrees": results,
    })
}

fn benchmark_degree(degree: usize) -> JsonValue {
    let (mut catalog, mut store) = fixture(degree);
    let plan = limit_plan();
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(BATCH_ROWS).expect("benchmark batch size is non-zero"),
        ..ExecutionMemoryConfig::default()
    };
    let expected_rows = degree.min(LIMIT);
    let mut external = NoExternalReadOperator;

    let warmups = warmups();
    for _ in 0..warmups {
        let (_, output_rows, _) = execute_probe(
            black_box(&plan),
            &mut catalog,
            &mut store,
            &mut external,
            &memory,
        );
        assert_eq!(output_rows, expected_rows);
    }

    let iterations = iterations_per_sample();
    let mut samples = Vec::with_capacity(sample_count());
    let mut checksum = 0u64;
    for _ in 0..sample_count() {
        let started = Instant::now();
        for _ in 0..iterations {
            let (_, output_rows, output_checksum) = execute_probe(
                black_box(&plan),
                &mut catalog,
                &mut store,
                &mut external,
                &memory,
            );
            assert_eq!(output_rows, expected_rows);
            checksum = black_box(output_checksum);
        }
        samples.push(started.elapsed().as_nanos() / iterations as u128);
    }
    samples.sort_unstable();

    let (probe, output_rows, probe_checksum) =
        execute_probe(&plan, &mut catalog, &mut store, &mut external, &memory);
    assert_eq!(output_rows, expected_rows);
    assert_eq!(probe_checksum, checksum);
    assert_eq!(probe.profile.blocking_operator_count(), 0);

    let expansion = probe
        .profile
        .graph_expansion_reports
        .first()
        .expect("bounded adjacency expansion must emit a report");
    assert_eq!(probe.profile.graph_expansion_reports.len(), 1);
    assert_eq!(expansion.seed_count, 1);
    assert_eq!(expansion.expanded_node_count, expected_rows);
    assert_eq!(expansion.expanded_edge_count, expected_rows);
    assert_eq!(expansion.returned_count, expected_rows);
    assert_eq!(expansion.candidate_limit, LIMIT);
    assert!(expansion.payload_bytes_used <= GRAPH_PAYLOAD_BUDGET_BYTES);

    let pipeline = &probe.profile.pipeline_memory_report;
    assert!(pipeline.peak_batch_rows <= BATCH_ROWS);
    assert_eq!(pipeline.query_memory_completion_bytes, 0);

    json!({
        "degree": degree,
        "warmups": warmups,
        "samples": samples.len(),
        "iterations_per_sample": iterations,
        "p50_ns": percentile(&samples, 50),
        "p95_ns": percentile(&samples, 95),
        "p99_ns": percentile(&samples, 99),
        "output_rows": output_rows,
        "expanded_nodes": expansion.expanded_node_count,
        "expanded_edges": expansion.expanded_edge_count,
        "payload_bytes_used": expansion.payload_bytes_used,
        "peak_batch_rows": pipeline.peak_batch_rows,
        "peak_batch_payload_bytes": pipeline.peak_batch_payload_bytes,
        "query_memory_peak_bytes": pipeline.query_memory_peak_bytes,
        "query_memory_completion_bytes": pipeline.query_memory_completion_bytes,
        "ordered_live_cursor": true,
        "degree_sized_query_ordering_buffer": false,
        "blocking_operator_budget_bytes": memory.blocking_operator_bytes.get(),
        "steady_resident_bytes": pipeline.steady_resident_bytes,
        "peak_resident_bytes": pipeline.peak_resident_bytes,
        "total_page_faults": pipeline.total_page_faults,
        "minor_page_faults": pipeline.minor_page_faults,
        "major_page_faults": pipeline.major_page_faults,
        "checksum": checksum,
    })
}

fn execute_probe(
    plan: &PhysicalPlan,
    catalog: &mut Catalog,
    store: &mut GraphStore,
    external: &mut NoExternalReadOperator,
    memory: &ExecutionMemoryConfig,
) -> (ProfiledQueryStream, usize, u64) {
    let mut output_rows = 0usize;
    let mut checksum = 0u64;
    let profile = execute_with_row_consumer_profile_and_external_and_memory(
        plan,
        catalog,
        store,
        &BTreeMap::new(),
        external,
        None,
        None,
        &mut |row| {
            output_rows = output_rows.saturating_add(1);
            checksum = checksum.wrapping_add(output_row_score(&row));
            Ok(())
        },
        memory,
    )
    .expect("adjacency benchmark execution must succeed");
    (profile, output_rows, checksum)
}

fn fixture(degree: usize) -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let mut nodes = Vec::<GraphSnapshotNodeImport>::with_capacity(degree.saturating_add(1));
    nodes.push((
        NodeId(0),
        "Seed".to_string(),
        BTreeMap::from([("stable_id".to_string(), Value::Int(0))]),
    ));
    nodes.extend((0..degree).map(|index| {
        (
            NodeId(index as u64 + 1),
            "Target".to_string(),
            BTreeMap::new(),
        )
    }));
    let relationships = (0..degree)
        .map(|index| -> GraphSnapshotRelationshipImport {
            (
                RelId(index as u64),
                NodeId(0),
                NodeId(index as u64 + 1),
                "LINK".to_string(),
                BTreeMap::new(),
            )
        })
        .collect();
    store
        .import_graph_snapshot_rows(&mut catalog, nodes, relationships)
        .expect("adjacency benchmark import must succeed");
    store
        .create_property_index(&mut catalog, "Seed", "stable_id")
        .expect("adjacency benchmark seed index must be created");
    (catalog, store)
}

fn limit_plan() -> PhysicalPlan {
    PhysicalPlan::LimitExec {
        offset: 0,
        limit: Some(LIMIT),
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Id {
                    variable: "target".to_string(),
                },
                name: "target_id".to_string(),
            }],
            input: Box::new(PhysicalPlan::AdjacencyExpandExec {
                source_variable: "seed".to_string(),
                source_label: "Seed".to_string(),
                rel_variable: None,
                rel_type: "LINK".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "target".to_string(),
                target_label: "Target".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                graph_budget: Some(GraphExpansionBudget {
                    candidate_limit: LIMIT,
                    payload_byte_limit: GRAPH_PAYLOAD_BUDGET_BYTES,
                }),
                input: Box::new(PhysicalPlan::IndexNodeSeek {
                    variable: "seed".to_string(),
                    label: "Seed".to_string(),
                    property: "stable_id".to_string(),
                    value: Value::Int(0),
                }),
            }),
        }),
    }
}

fn output_row_score(row: &BTreeMap<String, Value>) -> u64 {
    match row.get("target_id") {
        Some(Value::Int(value)) => *value as u64,
        value => panic!("target_id must be an integer, got {value:?}"),
    }
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    assert!(!samples.is_empty());
    let index = (samples.len() - 1).saturating_mul(percentile).div_ceil(100);
    samples[index.min(samples.len() - 1)]
}

fn benchmark_degrees() -> &'static [usize] {
    if cfg!(debug_assertions) {
        DEBUG_DEGREES
    } else {
        RELEASE_DEGREES
    }
}

fn warmups() -> usize {
    if cfg!(debug_assertions) {
        1
    } else {
        3
    }
}

fn sample_count() -> usize {
    if cfg!(debug_assertions) {
        3
    } else {
        11
    }
}

fn iterations_per_sample() -> usize {
    if cfg!(debug_assertions) {
        4
    } else {
        64
    }
}
