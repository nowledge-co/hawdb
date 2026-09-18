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

use hawdb::schema::{Catalog, RelTypeId};
use hawdb::store::{GraphStore, NodeId};
use hawdb_qos::ProcessMemorySnapshot;
use serde_json::json;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

const SOURCE_COUNT: usize = 513;
const TARGET_COUNT: usize = 64;
const RETAINED_MUTATIONS: usize = 64;
const DENSE_TARGET_COUNT: usize = 8_192;
const DENSE_RETAINED_MUTATIONS: usize = 64;
const DENSE_WRITER_MUTATIONS: usize = 256;
const DENSE_READ_AFTER_WRITE_POINTS: [usize; 7] = [1, 32, 63, 64, 65, 128, 256];
const LOOKUP_ITERATIONS: usize = 20_000;
const LOOKUP_SAMPLES: usize = 7;

fn main() {
    let (base, mut catalog, sources, targets) = fixture();
    let rel_type = catalog
        .rel_type_id("LINKS_TO")
        .expect("fixture relationship type must exist");

    let _ = measure_lookups(&base, &sources, rel_type, LOOKUP_ITERATIONS / 10);
    let mut lookup_samples = (0..LOOKUP_SAMPLES)
        .map(|_| measure_lookups(&base, &sources, rel_type, LOOKUP_ITERATIONS))
        .collect::<Vec<_>>();
    lookup_samples.sort_unstable_by_key(|sample| sample.elapsed_ns);
    let lookup = lookup_samples[lookup_samples.len() / 2];
    let (dense_base, mut dense_catalog, dense_sources, dense_targets) = dense_fixture();
    let dense_rel_type = dense_catalog
        .rel_type_id("LINKS_TO")
        .expect("dense fixture relationship type must exist");

    let memory_start = ProcessMemorySnapshot::capture().ok();
    let (retained, mutation_elapsed_ns) =
        measure_retained_mutations(&base, &mut catalog, &sources, &targets, RETAINED_MUTATIONS);
    let (dense_retained, dense_mutation_elapsed_ns) = measure_retained_mutations(
        &dense_base,
        &mut dense_catalog,
        &dense_sources,
        &dense_targets,
        DENSE_RETAINED_MUTATIONS,
    );
    let dense_writer = measure_dense_writer_mutations(
        &dense_base,
        &mut dense_catalog,
        dense_sources[0],
        &dense_targets,
        dense_rel_type,
        DENSE_WRITER_MUTATIONS,
    );
    black_box((&retained, &dense_retained, &dense_writer.retained));
    let memory_end = ProcessMemorySnapshot::capture().ok();

    let resident_delta_bytes = memory_start
        .zip(memory_end)
        .map(|(start, end)| end.resident_bytes.saturating_sub(start.resident_bytes));
    let report = json!({
        "source_count": SOURCE_COUNT,
        "target_count": TARGET_COUNT,
        "base_relationship_count": SOURCE_COUNT * TARGET_COUNT,
        "retained_mutations": RETAINED_MUTATIONS,
        "mutation_elapsed_ns": mutation_elapsed_ns,
        "mutation_ns_per_op": mutation_elapsed_ns / RETAINED_MUTATIONS as u128,
        "dense_target_count": DENSE_TARGET_COUNT,
        "dense_base_relationship_count": DENSE_TARGET_COUNT,
        "dense_retained_mutations": DENSE_RETAINED_MUTATIONS,
        "dense_mutation_elapsed_ns": dense_mutation_elapsed_ns,
        "dense_mutation_ns_per_op": dense_mutation_elapsed_ns / DENSE_RETAINED_MUTATIONS as u128,
        "dense_writer_mutations": DENSE_WRITER_MUTATIONS,
        "dense_writer_mutation_ns_p50": percentile(&dense_writer.mutation_elapsed_ns, 50),
        "dense_writer_mutation_ns_p95": percentile(&dense_writer.mutation_elapsed_ns, 95),
        "dense_writer_mutation_ns_p99": percentile(&dense_writer.mutation_elapsed_ns, 99),
        "dense_writer_mutation_ns_max": percentile(&dense_writer.mutation_elapsed_ns, 100),
        "dense_writer_resident_delta_bytes": dense_writer.resident_delta_bytes,
        "dense_writer_read_after_write": dense_writer.read_after_write,
        "dense_writer_consolidation_plan_entries": dense_writer.consolidation_plan_entries,
        "dense_writer_consolidated_groups": dense_writer.consolidated_groups,
        "dense_writer_consolidation_elapsed_ns": dense_writer.consolidation_elapsed_ns,
        "dense_writer_consolidated_resident_delta_bytes": dense_writer.consolidated_resident_delta_bytes,
        "resident_delta_bytes": resident_delta_bytes,
        "lookup_iterations": LOOKUP_ITERATIONS,
        "lookup_samples": LOOKUP_SAMPLES,
        "lookup_rows": lookup.rows,
        "lookup_elapsed_ns_p50": lookup.elapsed_ns,
        "lookup_ns_per_op_p50": lookup.elapsed_ns / LOOKUP_ITERATIONS as u128,
    });
    println!("store_cow_feasibility {report}");
}

struct DenseWriterSample {
    retained: (GraphStore, GraphStore, GraphStore),
    mutation_elapsed_ns: Vec<u128>,
    resident_delta_bytes: Option<u64>,
    read_after_write: Vec<serde_json::Value>,
    consolidation_plan_entries: usize,
    consolidated_groups: usize,
    consolidation_elapsed_ns: u128,
    consolidated_resident_delta_bytes: Option<u64>,
}

fn measure_dense_writer_mutations(
    base: &GraphStore,
    catalog: &mut Catalog,
    source: NodeId,
    targets: &[NodeId],
    rel_type: RelTypeId,
    mutation_count: usize,
) -> DenseWriterSample {
    let mut working = base.snapshot();
    let reader = working.snapshot();
    let memory_start = ProcessMemorySnapshot::capture().ok();
    let mut mutation_elapsed_ns = Vec::with_capacity(mutation_count);
    let mut read_after_write = Vec::with_capacity(DENSE_READ_AFTER_WRITE_POINTS.len());
    for mutation in 1..=mutation_count {
        let started = Instant::now();
        working
            .create_relationship(
                catalog,
                source,
                targets[(mutation - 1) % targets.len()],
                "LINKS_TO",
                BTreeMap::new(),
            )
            .expect("dense writer mutation must succeed");
        mutation_elapsed_ns.push(started.elapsed().as_nanos());

        if DENSE_READ_AFTER_WRITE_POINTS.contains(&mutation) {
            let read_started = Instant::now();
            let rows = black_box(&working)
                .outgoing_relationships(source, rel_type)
                .count();
            read_after_write.push(json!({
                "mutation_count": mutation,
                "rows": rows,
                "elapsed_ns": read_started.elapsed().as_nanos(),
            }));
        }
    }
    let memory_after_delta = ProcessMemorySnapshot::capture().ok();
    let resident_delta_bytes = memory_start
        .zip(memory_after_delta)
        .map(|(start, end)| end.resident_bytes.saturating_sub(start.resident_bytes));

    let mut consolidated = working.snapshot();
    let plan = consolidated.adjacency_consolidation_plan();
    let consolidation_started = Instant::now();
    let consolidation = consolidated.consolidate_bounded_adjacency_deltas(plan.estimated_entries);
    let consolidation_elapsed_ns = consolidation_started.elapsed().as_nanos();
    let memory_after_consolidation = ProcessMemorySnapshot::capture().ok();
    let consolidated_resident_delta_bytes = memory_start
        .zip(memory_after_consolidation)
        .map(|(start, end)| end.resident_bytes.saturating_sub(start.resident_bytes));

    DenseWriterSample {
        retained: (working, reader, consolidated),
        mutation_elapsed_ns,
        resident_delta_bytes,
        read_after_write,
        consolidation_plan_entries: plan.estimated_entries,
        consolidated_groups: consolidation.consolidated_group_count,
        consolidation_elapsed_ns,
        consolidated_resident_delta_bytes,
    }
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    let mut samples = samples.to_vec();
    samples.sort_unstable();
    let index = samples
        .len()
        .saturating_sub(1)
        .saturating_mul(percentile.min(100))
        / 100;
    samples.get(index).copied().unwrap_or_default()
}

fn measure_retained_mutations(
    base: &GraphStore,
    catalog: &mut Catalog,
    sources: &[NodeId],
    targets: &[NodeId],
    retained_mutations: usize,
) -> (Vec<(GraphStore, GraphStore)>, u128) {
    let start = Instant::now();
    let mut retained = Vec::with_capacity(retained_mutations);
    for iteration in 0..retained_mutations {
        let mut working = base.snapshot();
        let reader = working.snapshot();
        working
            .create_relationship(
                catalog,
                sources[iteration % sources.len()],
                targets[iteration % targets.len()],
                "LINKS_TO",
                BTreeMap::new(),
            )
            .expect("snapshot mutation must succeed");
        retained.push((working, reader));
    }
    (retained, start.elapsed().as_nanos())
}

#[derive(Clone, Copy)]
struct LookupSample {
    rows: usize,
    elapsed_ns: u128,
}

fn measure_lookups(
    store: &GraphStore,
    sources: &[NodeId],
    rel_type: RelTypeId,
    iterations: usize,
) -> LookupSample {
    let start = Instant::now();
    let mut rows = 0usize;
    for iteration in 0..iterations {
        let source = sources[iteration % sources.len()];
        rows = rows.saturating_add(
            black_box(store)
                .outgoing_relationships(source, rel_type)
                .count(),
        );
    }
    LookupSample {
        rows,
        elapsed_ns: start.elapsed().as_nanos(),
    }
}

fn fixture() -> (GraphStore, Catalog, Vec<NodeId>, Vec<NodeId>) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let sources = (0..SOURCE_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Source", BTreeMap::new())
                .expect("fixture source must be created")
        })
        .collect::<Vec<_>>();
    let targets = (0..TARGET_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .expect("fixture target must be created")
        })
        .collect::<Vec<_>>();
    for source in &sources {
        for target in &targets {
            store
                .create_relationship(&mut catalog, *source, *target, "LINKS_TO", BTreeMap::new())
                .expect("fixture relationship must be created");
        }
    }
    (store, catalog, sources, targets)
}

fn dense_fixture() -> (GraphStore, Catalog, Vec<NodeId>, Vec<NodeId>) {
    let mut store = GraphStore::in_memory();
    let mut catalog = Catalog::default();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .expect("dense fixture source must be created");
    let targets = (0..DENSE_TARGET_COUNT)
        .map(|_| {
            store
                .create_node(&mut catalog, "Target", BTreeMap::new())
                .expect("dense fixture target must be created")
        })
        .collect::<Vec<_>>();
    for target in &targets {
        store
            .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
            .expect("dense fixture relationship must be created");
    }
    (store, catalog, vec![source], targets)
}
