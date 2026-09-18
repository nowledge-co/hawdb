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

use hawdb::{SearchDocument, SearchIndex};
use hawdb_qos::{ProcessMemoryProfile, ProcessMemorySnapshot};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; numbers are only meaningful from `cargo bench`.
// The environment variable overrides either tier.
const DEFAULT_DOCUMENTS: usize = if cfg!(debug_assertions) {
    1_000
} else {
    20_000
};
const CONTENT_BYTES: usize = 1024;

fn main() {
    let document_count = std::env::var("HAWDB_SEARCH_CHECKPOINT_BENCH_DOCUMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_DOCUMENTS);
    let path = std::env::temp_dir().join(format!(
        "hawdb-search-checkpoint-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let mut index = SearchIndex::open(&path).expect("benchmark index must open");
    let content = "bounded-search-checkpoint ".repeat(CONTENT_BYTES / 26 + 1);
    for ordinal in 0..document_count {
        index
            .upsert(SearchDocument {
                id: format!("memory:{ordinal:016x}"),
                title: format!("Document {ordinal}"),
                content: content[..CONTENT_BYTES].to_string(),
                embedding: None,
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("space_id".to_string(), "default".to_string()),
                ]),
            })
            .expect("benchmark document must be admitted");
    }

    let memory_before = ProcessMemorySnapshot::capture().ok();
    let started = Instant::now();
    let report = index
        .checkpoint_with_report()
        .expect("streaming checkpoint must succeed");
    let elapsed = started.elapsed();
    let memory_after = ProcessMemorySnapshot::capture().ok();
    let memory = memory_before
        .zip(memory_after)
        .map(|(before, after)| ProcessMemoryProfile::between(before, after));
    assert!(report.snapshot_streamed);
    assert_eq!(report.document_count, document_count);
    assert!(report.snapshot_peak_record_bytes < report.snapshot_uncompressed_bytes);

    println!(
        "search_checkpoint {}",
        json!({
            "document_count": document_count,
            "content_bytes_per_document": CONTENT_BYTES,
            "elapsed_millis": elapsed.as_millis(),
            "documents_per_second": document_count as f64 / elapsed.as_secs_f64(),
            "steady_resident_growth_bytes": memory.map(|profile| profile.steady_resident_growth_bytes),
            "lifetime_peak_resident_growth_bytes": memory.map(|profile| profile.lifetime_peak_resident_growth_bytes),
            "minor_page_faults": memory.and_then(|profile| profile.minor_page_faults),
            "major_page_faults": memory.and_then(|profile| profile.major_page_faults),
            "snapshot_uncompressed_bytes": report.snapshot_uncompressed_bytes,
            "snapshot_compressed_bytes": report.snapshot_compressed_bytes,
            "snapshot_peak_record_bytes": report.snapshot_peak_record_bytes,
            "snapshot_peak_record_to_corpus_ratio": report.snapshot_peak_record_bytes as f64
                / report.snapshot_uncompressed_bytes as f64,
            "projection_generation": report.projection_generation,
            "snapshot_streamed": report.snapshot_streamed,
        })
    );

    drop(index);
    std::fs::remove_dir_all(path).expect("benchmark directory must be removable");
}
