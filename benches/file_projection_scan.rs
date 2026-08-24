//! Cost of repeated `FileProjection::search` against a disk-backed artifact.
//!
//! Every `search()` call pulls each scanned segment's payload off disk via
//! `FileProjection::read_segment`. Before this change that path reopened the
//! artifact file, seeked, and copied the full segment into a fresh `Vec<u8>`
//! on every single call; now it borrows a slice out of a persistent mmap
//! opened once in `FileProjection::open`. This bench repeats many searches
//! over a multi-segment on-disk artifact so that per-call file I/O and
//! allocation overhead, if present, shows up in wall-clock throughput and
//! total bytes materialized.

use serde_json::json;
use skein_vector_projection::{
    KernelPreference, ProjectionBuildConfig, ProjectionIdentity, ProjectionSearchOptions,
    ProjectionWriter,
};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; a full-size multi-segment on-disk artifact at
// opt-level 0 is not representative. Numbers are only meaningful from
// `cargo bench`.
const SMOKE: bool = cfg!(debug_assertions);

const DOCUMENT_COUNT: usize = if SMOKE { 512 } else { 32_768 };
const SEGMENT_ROWS: usize = 256;
const TOP_K: usize = 10;
const SEARCHES: usize = if SMOKE { 4 } else { 200 };
const WORKERS: [usize; 2] = [1, 4];

fn main() {
    let dimension = std::env::var("SKEIN_BENCH_VECTOR_DIMENSION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(384);
    let path = artifact_path();
    let projection = build_artifact(&path, dimension);
    let query = vector(17, dimension);
    let segment_count = projection.manifest().segments.len();

    let results = WORKERS
        .into_iter()
        .map(|workers| measure(&projection, &query, workers))
        .collect::<Vec<_>>();

    println!(
        "file_projection_scan {}",
        json!({
            "dimension": dimension,
            "document_count": DOCUMENT_COUNT,
            "segment_rows": SEGMENT_ROWS,
            "segment_count": segment_count,
            "searches": SEARCHES,
            "smoke": SMOKE,
            "results": results,
        })
    );

    let _ = std::fs::remove_file(&path);
}

fn artifact_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "skein-file-projection-scan-{}-{}.bin",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn build_artifact(
    path: &std::path::Path,
    dimension: usize,
) -> skein_vector_projection::FileProjection {
    let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
        .with_segment_rows(SEGMENT_ROWS);
    let mut writer =
        ProjectionWriter::create(path, config).expect("benchmark artifact must initialize");
    for id in 0..DOCUMENT_COUNT as u64 {
        writer
            .push(id, &vector(id, dimension))
            .expect("benchmark vector must be accepted");
    }
    writer.finish().expect("benchmark artifact must finish")
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
    projection: &skein_vector_projection::FileProjection,
    query: &[f32],
    workers: usize,
) -> serde_json::Value {
    let options = ProjectionSearchOptions::new()
        .with_max_parallelism(NonZeroUsize::new(workers).unwrap())
        .with_kernel(KernelPreference::Scalar);
    let mut samples = Vec::with_capacity(SEARCHES);
    let mut payload_bytes_read = 0u64;
    let mut admitted_workers = 0usize;
    let started_total = Instant::now();
    for _ in 0..SEARCHES {
        let started = Instant::now();
        let output = projection
            .search(query, TOP_K, options)
            .expect("benchmark search must succeed");
        let elapsed = started.elapsed();
        payload_bytes_read = output.report.payload_bytes_read;
        admitted_workers = output.report.worker_count;
        black_box(&output.hits);
        samples.push(elapsed.as_secs_f64());
    }
    let total_elapsed = started_total.elapsed();
    samples.sort_unstable_by(f64::total_cmp);
    json!({
        "requested_workers": workers,
        "admitted_workers": admitted_workers,
        "payload_bytes_read_per_search": payload_bytes_read,
        "searches_per_second": SEARCHES as f64 / total_elapsed.as_secs_f64(),
        "median_search_seconds": samples[samples.len() / 2],
        "min_search_seconds": samples[0],
        "max_search_seconds": samples[samples.len() - 1],
    })
}
