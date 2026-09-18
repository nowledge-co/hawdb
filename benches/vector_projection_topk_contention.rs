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

//! Isolates the cost of merging per-segment scan results into the query's
//! top-k under high segment/worker fan-out.
//!
//! Every worker previously locked one shared `Mutex<TopK>` once per finished
//! segment. To make that lock a large share of total work (rather than being
//! dwarfed by scoring cost), this bench uses a small vector dimension, many
//! small segments, and a high worker count, so segments finish and contend
//! for the merge as fast as possible.

use hawdb_vector_projection::{
    KernelPreference, ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity,
    ProjectionSearchOptions,
};
use serde_json::json;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`. Numbers are only meaningful from `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const DIMENSION: usize = 16;
const SEGMENT_ROWS: usize = 8;
const DOCUMENT_COUNT: usize = if SMOKE { 256 } else { 65_536 };
const TOP_K: usize = 10;
const SAMPLES: usize = if SMOKE { 3 } else { 15 };
const WORKERS: [usize; 4] = [1, 4, 8, 16];

fn main() {
    let projection = build_projection();
    let query = vector(17);
    let segment_count = projection.manifest().segments.len();

    let results = WORKERS
        .into_iter()
        .map(|workers| measure(&projection, &query, workers))
        .collect::<Vec<_>>();

    println!(
        "vector_projection_topk_contention {}",
        json!({
            "dimension": DIMENSION,
            "document_count": DOCUMENT_COUNT,
            "segment_rows": SEGMENT_ROWS,
            "segment_count": segment_count,
            "top_k": TOP_K,
            "samples": SAMPLES,
            "smoke": SMOKE,
            "results": results,
        })
    );
}

fn build_projection() -> hawdb_vector_projection::InMemoryProjection {
    let config = ProjectionBuildConfig::new(DIMENSION, ProjectionIdentity::new(1))
        .with_segment_rows(SEGMENT_ROWS);
    let mut builder = ProjectionBuilder::new(config).expect("benchmark projection must initialize");
    for id in 0..DOCUMENT_COUNT as u64 {
        builder
            .push(id, &vector(id))
            .expect("benchmark vector must be accepted");
    }
    builder.finish().expect("benchmark projection must finish")
}

fn vector(id: u64) -> Vec<f32> {
    (0..DIMENSION)
        .map(|offset| {
            let mixed = id
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add((offset as u64).wrapping_mul(0x85eb_ca6b));
            ((mixed % 2_001) as f32 - 1_000.0) / 1_000.0
        })
        .collect()
}

fn measure(
    projection: &hawdb_vector_projection::InMemoryProjection,
    query: &[f32],
    workers: usize,
) -> serde_json::Value {
    let options = ProjectionSearchOptions::new()
        .with_max_parallelism(NonZeroUsize::new(workers).unwrap())
        .with_kernel(KernelPreference::Scalar);
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut admitted_workers = 0usize;
    let mut scanned_segments = 0usize;
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let output = projection
            .search(query, TOP_K, options)
            .expect("benchmark search must succeed");
        let elapsed = started.elapsed();
        admitted_workers = output.report.worker_count;
        scanned_segments = output.report.scanned_segment_count;
        black_box(&output.hits);
        samples.push(elapsed.as_secs_f64());
    }
    samples.sort_unstable_by(f64::total_cmp);
    json!({
        "requested_workers": workers,
        "admitted_workers": admitted_workers,
        "scanned_segment_count": scanned_segments,
        "searches_per_second": 1.0 / samples[samples.len() / 2],
        "median_search_seconds": samples[samples.len() / 2],
        "min_search_seconds": samples[0],
        "max_search_seconds": samples[samples.len() - 1],
    })
}
