//! `HnswIndex` versus the quantized base's exact scan: recall against true
//! (brute-force, unquantized) cosine ranking, p50/p95/p99 query latency,
//! memory footprint, and build cost.
//!
//! The quantized base is itself approximate (4-bit TurboQuant codes), so
//! this compares three things at once: true exact cosine ranking (ground
//! truth), the existing quantized-scan recall against that ground truth,
//! and HnswIndex's recall against the same ground truth -- to show whether
//! HNSW's approximation is worth its extra memory relative to just
//! scanning the quantized base directly.

use serde_json::json;
use skein_vector_projection::{
    HnswBuildConfig, HnswIndex, KernelPreference, ProjectionBuildConfig, ProjectionBuilder,
    ProjectionIdentity, ProjectionSearchOptions,
};
use std::hint::black_box;
use std::time::Instant;

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; building a multi-thousand-node graph at
// opt-level 0 is not representative. Numbers are only meaningful from
// `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const DOCUMENT_COUNT: usize = if SMOKE { 300 } else { 5_000 };
const TOP_K: usize = 10;
const QUERY_COUNT: usize = if SMOKE { 5 } else { 50 };
const EF_SEARCH: usize = 100;

fn main() {
    let dimension = std::env::var("SKEIN_BENCH_VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(128);
    let entries: Vec<(u64, Vec<f32>)> = (0..DOCUMENT_COUNT as u64)
        .map(|id| (id, vector(id, dimension)))
        .collect();

    let base_build_started = Instant::now();
    let base = build_base(&entries, dimension);
    let base_build_seconds = base_build_started.elapsed().as_secs_f64();

    let hnsw_build_started = Instant::now();
    let hnsw = HnswIndex::build(&entries, dimension, HnswBuildConfig::new(), None)
        .expect("benchmark hnsw index must build");
    let hnsw_build_seconds = hnsw_build_started.elapsed().as_secs_f64();

    let queries: Vec<Vec<f32>> = (0..QUERY_COUNT as u64)
        .map(|offset| vector(DOCUMENT_COUNT as u64 + offset, dimension))
        .collect();

    let mut base_latencies = Vec::with_capacity(QUERY_COUNT);
    let mut hnsw_latencies = Vec::with_capacity(QUERY_COUNT);
    let mut base_recall_hits = 0usize;
    let mut hnsw_recall_hits = 0usize;

    let base_options = ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar);
    for query in &queries {
        let exact_ids = exact_top_k(&entries, query, TOP_K);

        let started = Instant::now();
        let base_output = base
            .search(query, TOP_K, base_options)
            .expect("benchmark base search must succeed");
        base_latencies.push(started.elapsed().as_secs_f64());
        let base_hit_ids: Vec<u64> = base_output.hits.iter().map(|hit| hit.id).collect();
        base_recall_hits += overlap(&exact_ids, &base_hit_ids);

        let started = Instant::now();
        let hnsw_hits = hnsw
            .search(query, TOP_K, EF_SEARCH, None)
            .expect("benchmark hnsw search must succeed");
        hnsw_latencies.push(started.elapsed().as_secs_f64());
        let hnsw_hit_ids: Vec<u64> = hnsw_hits.iter().map(|hit| hit.id).collect();
        hnsw_recall_hits += overlap(&exact_ids, &hnsw_hit_ids);
        black_box((&base_output.hits, &hnsw_hits));
    }

    let denominator = (QUERY_COUNT * TOP_K) as f64;
    println!(
        "hnsw_vs_exact_scan {}",
        json!({
            "dimension": dimension,
            "document_count": DOCUMENT_COUNT,
            "top_k": TOP_K,
            "query_count": QUERY_COUNT,
            "ef_search": EF_SEARCH,
            "smoke": SMOKE,
            "build": {
                "quantized_base_build_seconds": base_build_seconds,
                "hnsw_build_seconds": hnsw_build_seconds,
            },
            "memory_bytes": {
                "quantized_base_payload_bytes": base.build_report().projection_payload_bytes,
                "quantized_base_raw_vector_bytes": base.build_report().raw_vector_bytes,
                "hnsw_approximate_bytes": hnsw.approximate_memory_bytes(),
            },
            "recall_at_top_k": {
                "quantized_base_vs_exact": base_recall_hits as f64 / denominator,
                "hnsw_vs_exact": hnsw_recall_hits as f64 / denominator,
            },
            "latency_seconds": {
                "quantized_base": percentiles(&base_latencies),
                "hnsw": percentiles(&hnsw_latencies),
            },
        })
    );
}

fn build_base(
    entries: &[(u64, Vec<f32>)],
    dimension: usize,
) -> skein_vector_projection::InMemoryProjection {
    let config =
        ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1)).with_segment_rows(1024);
    let mut builder = ProjectionBuilder::new(config).expect("benchmark projection must initialize");
    for (id, vector) in entries {
        builder
            .push(*id, vector)
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

fn exact_top_k(entries: &[(u64, Vec<f32>)], query: &[f32], top_k: usize) -> Vec<u64> {
    let query_norm = l2_norm(query);
    let mut scored: Vec<(f32, u64)> = entries
        .iter()
        .map(|(id, vector)| (cosine(query, query_norm, vector), *id))
        .collect();
    scored.sort_by(|left, right| right.0.total_cmp(&left.0));
    scored.truncate(top_k);
    scored.into_iter().map(|(_, id)| id).collect()
}

fn cosine(query: &[f32], query_norm: f32, vector: &[f32]) -> f32 {
    let vector_norm = l2_norm(vector);
    if query_norm <= f32::EPSILON || vector_norm <= f32::EPSILON {
        return 0.0;
    }
    let dot: f32 = query.iter().zip(vector.iter()).map(|(a, b)| a * b).sum();
    dot / (query_norm * vector_norm)
}

fn l2_norm(vector: &[f32]) -> f32 {
    vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt() as f32
}

fn overlap(exact_ids: &[u64], hit_ids: &[u64]) -> usize {
    exact_ids.iter().filter(|id| hit_ids.contains(id)).count()
}

fn percentiles(samples: &[f64]) -> serde_json::Value {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let at = |fraction: f64| -> f64 {
        let index = ((sorted.len() as f64 * fraction) as usize).min(sorted.len() - 1);
        sorted[index]
    };
    json!({
        "p50": at(0.50),
        "p95": at(0.95),
        "p99": at(0.99),
        "min": sorted[0],
        "max": sorted[sorted.len() - 1],
    })
}
