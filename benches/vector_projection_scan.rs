use serde_json::json;
use skein_vector_projection::{
    KernelPreference, ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity,
    ProjectionSearchOptions,
};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::Instant;

const DOCUMENT_COUNT: usize = 8_192;
const TOP_K: usize = 10;
const SAMPLES: usize = 5;
const ALLOWLIST_DENSITIES: [usize; 4] = [1, 10, 50, 100];
const WORKERS: [usize; 2] = [1, 4];
const KERNELS: [(&str, KernelPreference); 2] = [
    ("scalar", KernelPreference::Scalar),
    ("auto", KernelPreference::Auto),
];

fn main() {
    let dimension = std::env::var("SKEIN_BENCH_VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(384);
    let projection = build_projection(dimension);
    let query = vector(17, dimension);
    let mut results = Vec::new();
    for workers in WORKERS {
        for density in ALLOWLIST_DENSITIES {
            let allowed_ids = (0..DOCUMENT_COUNT as u64)
                .filter(|id| (*id as usize * 100 / DOCUMENT_COUNT) < density)
                .collect::<Vec<_>>();
            for (requested_kernel, kernel) in KERNELS {
                results.push(measure(
                    &projection,
                    &query,
                    requested_kernel,
                    kernel,
                    workers,
                    density,
                    &allowed_ids,
                ));
            }
        }
    }
    println!(
        "vector_projection_scan {}",
        json!({
            "dimension": dimension,
            "document_count": DOCUMENT_COUNT,
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn build_projection(dimension: usize) -> skein_vector_projection::InMemoryProjection {
    let config =
        ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1)).with_segment_rows(1024);
    let mut builder = ProjectionBuilder::new(config).expect("benchmark projection must initialize");
    for id in 0..DOCUMENT_COUNT as u64 {
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
    projection: &skein_vector_projection::InMemoryProjection,
    query: &[f32],
    requested_kernel: &str,
    kernel: KernelPreference,
    workers: usize,
    density: usize,
    allowed_ids: &[u64],
) -> serde_json::Value {
    let options = ProjectionSearchOptions::new()
        .with_max_parallelism(NonZeroUsize::new(workers).unwrap())
        .with_kernel(kernel)
        .with_allowed_ids(allowed_ids);
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut scored_documents = 0usize;
    let mut admitted_workers = 0usize;
    let mut selected_kernel = "unknown";
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let output = projection
            .search(query, TOP_K, options)
            .expect("benchmark search must succeed");
        let elapsed = started.elapsed();
        scored_documents = output.report.scored_document_count;
        admitted_workers = output.report.worker_count;
        selected_kernel = output.report.kernel.as_str();
        black_box(output.hits);
        samples.push(scored_documents as f64 / elapsed.as_secs_f64());
    }
    samples.sort_unstable_by(f64::total_cmp);
    json!({
        "requested_kernel": requested_kernel,
        "selected_kernel": selected_kernel,
        "requested_workers": workers,
        "admitted_workers": admitted_workers,
        "allowlist_density_percent": density,
        "allowed_document_count": allowed_ids.len(),
        "scored_document_count": scored_documents,
        "median_scored_documents_per_second": samples[samples.len() / 2],
        "min_scored_documents_per_second": samples[0],
        "max_scored_documents_per_second": samples[samples.len() - 1],
    })
}
