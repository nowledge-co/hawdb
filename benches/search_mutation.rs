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

//! Compare a full immutable generation build with a bounded mutation checkpoint.
//!
//! The benchmark is intentionally an executable measurement rather than a unit
//! test. `HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS` can be set to a representative
//! corpus-shaped count and `HAWDB_SEARCH_MUTATION_BENCH_TOUCHES` controls K.

use hawdb::{
    SearchDocument, SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};
use hawdb_qos::{ProcessMemoryProfile, ProcessMemorySnapshot};
use serde_json::json;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_DOCUMENTS: usize = if cfg!(debug_assertions) {
    2_000
} else {
    100_000
};
const DEFAULT_TOUCHES: usize = 10;
const CONTENT_BYTES: usize = 512;
const SUSTAINED_SEED_SEGMENTS: usize = 32;

fn main() {
    let document_count = env_usize("HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS", DEFAULT_DOCUMENTS);
    let touches = env_usize("HAWDB_SEARCH_MUTATION_BENCH_TOUCHES", DEFAULT_TOUCHES);
    let sustained_rounds = env_usize("HAWDB_SEARCH_MUTATION_BENCH_ROUNDS", 0);
    assert!(document_count > 0, "document count must be positive");
    assert!(
        touches > 0 && touches <= document_count,
        "touches must be in 1..=documents"
    );

    let root = std::env::temp_dir().join(format!(
        "hawdb-search-mutation-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let content = "bounded-search-mutation ".repeat(CONTENT_BYTES / 24 + 1);
    let documents = (0..document_count)
        .map(|ordinal| document(ordinal, &content))
        .collect::<Vec<_>>();

    let full_started = Instant::now();
    let mut initial = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .expect("initial generation writer must open");
    for document in &documents {
        initial
            .push(document.clone())
            .expect("initial document must be admitted");
    }
    let initial_report = initial.finish().expect("initial generation must publish");
    let full_elapsed = full_started.elapsed();

    let mut reader = SearchOutOfCoreReader::open(&root).expect("initial generation must reopen");
    let sustained_seed_segments = if sustained_rounds > 0 {
        SUSTAINED_SEED_SEGMENTS
    } else {
        0
    };
    if sustained_rounds > 0 {
        for seed in 0..SUSTAINED_SEED_SEGMENTS {
            let update = SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                SearchProjectionDelta {
                    upserts: vec![projection_row(document_count + seed, &content)],
                    ..Default::default()
                },
                Default::default(),
            )
            .expect("sustained seed must prepare");
            update.finish().expect("sustained seed must publish");
            drop(reader);
            reader = SearchOutOfCoreReader::open(&root).expect("seed must reopen");
        }
    }
    let sustained_document_count = document_count + sustained_seed_segments;
    let deletes = (document_count - touches..document_count)
        .map(|ordinal| format!("memory:{ordinal:016x}"))
        .collect();
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let mutation_started = Instant::now();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            deletes,
            ..Default::default()
        },
        Default::default(),
    )
    .expect("mutation update must prepare");
    let (delta, mutation_report, source_reads) = update.finish().expect("mutation must publish");
    let mutation_elapsed = mutation_started.elapsed();
    let memory_after = ProcessMemorySnapshot::capture().ok();
    let memory = memory_before
        .zip(memory_after)
        .map(|(before, after)| ProcessMemoryProfile::between(before, after));

    assert_eq!(delta.deleted_documents, touches);
    assert_eq!(
        mutation_report.document_count,
        sustained_document_count - touches
    );
    assert!(mutation_report.generation_bytes < initial_report.generation_bytes);
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).expect("mutation generation must reopen");
    assert_eq!(
        reopened.document_count(),
        sustained_document_count - touches
    );

    let mut sustained = Vec::with_capacity(sustained_rounds);
    if sustained_rounds > 0 {
        let mut current = reopened;
        for round in 0..sustained_rounds {
            let replacement_ordinal = round % document_count;
            let appended_ordinal = document_count + SUSTAINED_SEED_SEGMENTS + round;
            let before_memory = ProcessMemorySnapshot::capture().ok();
            let started = Instant::now();
            let update = SearchOutOfCoreGenerationWriter::prepare_delta(
                &current,
                SearchProjectionDelta {
                    upserts: vec![
                        projection_row(replacement_ordinal, &content),
                        projection_row(appended_ordinal, &content),
                    ],
                    ..Default::default()
                },
                Default::default(),
            )
            .expect("sustained mutation must prepare");
            let (delta, report, source_reads) =
                update.finish().expect("sustained mutation must publish");
            drop(current);
            let updated = SearchOutOfCoreReader::open(&root)
                .expect("sustained mutation generation must reopen");
            let policy = hawdb::SearchOutOfCoreSegmentCompactionPolicy::default()
                .with_level_zero_target_bytes(NonZeroU64::new(8 * 1024).unwrap())
                .expect("sustained compaction policy must be valid");
            let compacted = SearchOutOfCoreGenerationWriter::compact_segments(
                &updated,
                policy,
                Default::default(),
            )
            .expect("sustained compaction must run");
            drop(updated);
            let after = SearchOutOfCoreReader::open(&root)
                .expect("sustained compaction generation must reopen");
            let after_memory = ProcessMemorySnapshot::capture().ok();
            let memory = before_memory
                .zip(after_memory)
                .map(|(before, after)| ProcessMemoryProfile::between(before, after));
            assert_eq!(
                after.document_count(),
                sustained_document_count - touches + round + 1,
                "sustained round {round} document count"
            );
            sustained.push(json!({
                "round": round,
                "upserted_documents": delta.upserted_documents,
                "deleted_documents": delta.deleted_documents,
                "checkpoint_bytes": report.generation_bytes,
                "checkpoint_elapsed_millis": started.elapsed().as_millis(),
                "source_segment_bytes_read": source_reads.segment_bytes_read,
                "source_hydrated_documents": source_reads.hydrated_documents,
                "compaction_published": compacted.is_some(),
                "document_count": after.document_count(),
                "steady_resident_growth_bytes": memory
                    .map(|profile| profile.steady_resident_growth_bytes),
                "lifetime_peak_resident_growth_bytes": memory
                    .map(|profile| profile.lifetime_peak_resident_growth_bytes),
            }));
            current = after;
        }
        drop(current);
    }

    println!(
        "search_mutation {}",
        json!({
            "document_count": document_count,
            "touches": touches,
            "content_bytes_per_document": CONTENT_BYTES,
            "full_generation_bytes": initial_report.generation_bytes,
            "mutation_checkpoint_bytes": mutation_report.generation_bytes,
            "mutation_to_full_write_ratio": mutation_report.generation_bytes as f64
                / initial_report.generation_bytes as f64,
            "full_elapsed_millis": full_elapsed.as_millis(),
            "mutation_elapsed_millis": mutation_elapsed.as_millis(),
            "source_segment_bytes_read": source_reads.segment_bytes_read,
            "source_hydrated_documents": source_reads.hydrated_documents,
            "steady_resident_growth_bytes": memory.map(|profile| profile.steady_resident_growth_bytes),
            "lifetime_peak_resident_growth_bytes": memory
                .map(|profile| profile.lifetime_peak_resident_growth_bytes),
            "sustained_rounds": sustained,
        })
    );

    std::fs::remove_dir_all(root).expect("benchmark directory must be removable");
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn document(ordinal: usize, content: &str) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{ordinal:016x}"),
        title: format!("Document {ordinal}"),
        content: content[..CONTENT_BYTES].to_string(),
        embedding: None,
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}

fn projection_row(ordinal: usize, content: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{ordinal:016x}"),
        title: format!("Document {ordinal}"),
        body: content.to_string(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}
