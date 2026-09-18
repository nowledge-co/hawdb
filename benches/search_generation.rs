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

use hawdb::{
    SearchDocument, SearchEmbeddingManifest, SearchIndex, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use hawdb_qos::{ProcessMemoryProfile, ProcessMemorySnapshot};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; a 100k-document generation build at opt-level 0
// dominates that job. Numbers are only meaningful from `cargo bench`, and
// the environment variable still overrides either tier.
const DEFAULT_DOCUMENTS: usize = if cfg!(debug_assertions) {
    2_000
} else {
    100_000
};
const CONTENT_BYTES: usize = 512;
const EMBEDDING_DIMENSION: usize = 16;
const MODE_ENV: &str = "HAWDB_SEARCH_GENERATION_BENCH_MODE";

fn main() {
    let document_count = std::env::var("HAWDB_SEARCH_GENERATION_BENCH_DOCUMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_DOCUMENTS);
    let mode = std::env::var(MODE_ENV).unwrap_or_else(|_| "streaming".to_string());
    assert!(
        matches!(mode.as_str(), "streaming" | "resident"),
        "{MODE_ENV} must be streaming or resident"
    );
    let path = std::env::temp_dir().join(format!(
        "hawdb-search-generation-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let content = "bounded-search-generation ".repeat(CONTENT_BYTES / 26 + 1);
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let started = Instant::now();
    let (reader, resident_index, build) = match mode.as_str() {
        "streaming" => build_streaming(&path, document_count, &content),
        "resident" => build_resident(&path, document_count, &content),
        _ => unreachable!(),
    };
    let elapsed = started.elapsed();
    let memory_after = ProcessMemorySnapshot::capture().ok();
    let memory = memory_before
        .zip(memory_after)
        .map(|(before, after)| ProcessMemoryProfile::between(before, after));
    assert_eq!(reader.document_count(), document_count);
    assert_eq!(reader.generation(), build["generation"].as_u64().unwrap());

    println!(
        "search_generation {}",
        json!({
            "mode": mode,
            "document_count": document_count,
            "content_bytes_per_document": CONTENT_BYTES,
            "embedding_dimension": EMBEDDING_DIMENSION,
            "elapsed_millis": elapsed.as_millis(),
            "documents_per_second": document_count as f64 / elapsed.as_secs_f64(),
            "steady_resident_growth_bytes": memory.map(|profile| profile.steady_resident_growth_bytes),
            "lifetime_peak_resident_growth_bytes": memory.map(|profile| profile.lifetime_peak_resident_growth_bytes),
            "minor_page_faults": memory.and_then(|profile| profile.minor_page_faults),
            "major_page_faults": memory.and_then(|profile| profile.major_page_faults),
            "build": build,
        })
    );

    drop(reader);
    drop(resident_index);
    std::fs::remove_dir_all(path).expect("benchmark directory must be removable");
}

fn build_streaming(
    path: &std::path::Path,
    document_count: usize,
    content: &str,
) -> (
    SearchOutOfCoreReader,
    Option<SearchIndex>,
    serde_json::Value,
) {
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(1),
        embedding_manifest: Some(embedding_manifest()),
        ..SearchOutOfCoreGenerationBuildOptions::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, options)
        .expect("generation writer must open");
    for ordinal in 0..document_count {
        writer
            .push(document(ordinal, content))
            .expect("benchmark document must be admitted");
    }
    let report = writer.finish().expect("generation must publish");
    let reader = SearchOutOfCoreReader::open(path).expect("published generation must reopen");
    assert_eq!(report.document_count, document_count);
    assert_eq!(reader.resident_document_count(), 0);
    assert_eq!(reader.generation(), report.generation);
    assert!(report.peak_segment_encoded_bytes <= report.logical_document_bytes);
    let build = json!({
            "generation": report.generation,
            "logical_document_bytes": report.logical_document_bytes,
            "spool_bytes": report.spool_bytes,
            "peak_record_bytes": report.peak_record_bytes,
            "peak_segment_document_count": report.peak_segment_document_count,
            "peak_segment_encoded_bytes": report.peak_segment_encoded_bytes,
            "descriptor_working_bytes": report.descriptor_working_bytes,
            "descriptor_bytes": report.descriptor_bytes,
            "document_payload_bytes": report.document_payload_bytes,
            "metadata_payload_bytes": report.metadata_payload_bytes,
            "vector_payload_bytes": report.vector_payload_bytes,
            "lexical_artifact_bytes": report.lexical_artifact_bytes,
            "generation_bytes": report.generation_bytes,
            "resident_document_count": report.resident_document_count,
            "active_manifest_published_last": report.active_manifest_published_last,
            "cleanup_deleted_files": report.cleanup_deleted_files,
            "cleanup_pending_files": report.cleanup_pending_files,
            "cleanup_retry_required": report.cleanup_retry_required,
    });
    (reader, None, build)
}

fn build_resident(
    path: &std::path::Path,
    document_count: usize,
    content: &str,
) -> (
    SearchOutOfCoreReader,
    Option<SearchIndex>,
    serde_json::Value,
) {
    let mut index = SearchIndex::open(path).expect("resident benchmark index must open");
    index
        .apply_embedding_manifest(embedding_manifest())
        .expect("resident embedding manifest must apply");
    for ordinal in 0..document_count {
        index
            .upsert(document(ordinal, content))
            .expect("resident benchmark document must be admitted");
    }
    let report = index
        .checkpoint_with_report()
        .expect("resident checkpoint must publish");
    let reader = SearchOutOfCoreReader::open(path).expect("resident generation must reopen");
    assert_eq!(report.document_count, document_count);
    assert_eq!(reader.generation(), report.projection_generation);
    let build = json!({
        "generation": report.projection_generation,
        "snapshot_uncompressed_bytes": report.snapshot_uncompressed_bytes,
        "snapshot_compressed_bytes": report.snapshot_compressed_bytes,
        "snapshot_peak_record_bytes": report.snapshot_peak_record_bytes,
        "resident_document_count": document_count,
    });
    (reader, Some(index), build)
}

fn document(ordinal: usize, content: &str) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{ordinal:016x}"),
        title: format!("Document {ordinal}"),
        content: content[..CONTENT_BYTES].to_string(),
        embedding: Some(
            (0..EMBEDDING_DIMENSION)
                .map(|dimension| ((ordinal + dimension) % 97) as f32 / 97.0)
                .collect(),
        ),
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}

fn embedding_manifest() -> SearchEmbeddingManifest {
    SearchEmbeddingManifest {
        model: "benchmark-model".to_string(),
        version: Some("v1".to_string()),
        dimension: EMBEDDING_DIMENSION,
    }
}
