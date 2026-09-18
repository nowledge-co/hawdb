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

//! Query cost of `search_with_delta` as the un-indexed delta grows, to help
//! calibrate `DeltaBuffer::should_optimize`'s trigger threshold.
//!
//! The delta is scanned exactly (no quantization, no block skipping) on
//! every query, on top of the base's approximate scan, so its cost should
//! grow roughly linearly with delta size while the base scan cost stays
//! fixed. This sweeps delta fraction against a fixed-size base to show
//! where that added cost starts to matter relative to plain base search.

use hawdb_vector_projection::{
    search_with_delta, DeltaBuffer, KernelPreference, ProjectionBuildConfig, ProjectionBuilder,
    ProjectionIdentity, ProjectionSearchOptions,
};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`. Numbers are only meaningful from `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const BASE_DOCUMENT_COUNT: usize = if SMOKE { 512 } else { 8_192 };
const TOP_K: usize = 10;
const SAMPLES: usize = if SMOKE { 3 } else { 15 };
const DELTA_FRACTIONS: [f64; 6] = [0.0, 0.01, 0.05, 0.1, 0.2, 0.4];

fn main() {
    let dimension = std::env::var("HAWDB_BENCH_VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(384);
    let base = build_base(dimension);
    let query = vector(17, dimension);

    let results = DELTA_FRACTIONS
        .into_iter()
        .map(|fraction| measure(&base, &query, dimension, fraction))
        .collect::<Vec<_>>();

    println!(
        "vector_projection_delta_fraction {}",
        json!({
            "dimension": dimension,
            "base_document_count": BASE_DOCUMENT_COUNT,
            "top_k": TOP_K,
            "samples": SAMPLES,
            "smoke": SMOKE,
            "results": results,
        })
    );
}

fn build_base(dimension: usize) -> hawdb_vector_projection::InMemoryProjection {
    let config =
        ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1)).with_segment_rows(1024);
    let mut builder = ProjectionBuilder::new(config).expect("benchmark projection must initialize");
    for id in 0..BASE_DOCUMENT_COUNT as u64 {
        builder
            .push(id, &vector(id, dimension))
            .expect("benchmark vector must be accepted");
    }
    builder.finish().expect("benchmark projection must finish")
}

fn vector(id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|offset| {
            let mixed = id
                .wrapping_mul(0x9e37_79b9)
                .wrapping_add((offset as u64).wrapping_mul(0x85eb_ca6b));
            ((mixed % 2_001) as f32 - 1_000.0) / 1_000.0
        })
        .collect()
}

fn measure(
    base: &hawdb_vector_projection::InMemoryProjection,
    query: &[f32],
    dimension: usize,
    fraction: f64,
) -> serde_json::Value {
    let delta_count = (BASE_DOCUMENT_COUNT as f64 * fraction).round() as u64;
    let mut delta = DeltaBuffer::new(dimension);
    for offset in 0..delta_count {
        // Half updates to existing base ids (exercises shadow filtering),
        // half brand-new ids past the base's id range.
        let id = if offset % 2 == 0 {
            offset % BASE_DOCUMENT_COUNT as u64
        } else {
            BASE_DOCUMENT_COUNT as u64 + offset
        };
        delta
            .upsert(id, &vector(id ^ 0xdead_beef, dimension))
            .expect("benchmark delta vector must be accepted");
    }

    let options = ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar);
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut observed_delta_fraction = 0.0;
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let output = search_with_delta(query, TOP_K, options, &delta, |q, k, o| {
            base.search(q, k, o)
        })
        .expect("benchmark merged search must succeed");
        let elapsed = started.elapsed();
        observed_delta_fraction = output.delta_fraction;
        black_box(&output.hits);
        samples.push(elapsed.as_secs_f64());
    }
    samples.sort_unstable_by(f64::total_cmp);
    json!({
        "requested_delta_fraction": fraction,
        "observed_delta_fraction": observed_delta_fraction,
        "delta_document_count": delta.len(),
        "median_search_seconds": samples[samples.len() / 2],
        "min_search_seconds": samples[0],
        "max_search_seconds": samples[samples.len() - 1],
        "searches_per_second": 1.0 / samples[samples.len() / 2],
    })
}
