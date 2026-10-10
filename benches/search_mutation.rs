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
    RuntimeMemoryReservation, RuntimeTaskContext, SearchDocument,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
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
    let compaction_every = env_usize("HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY", 1);
    let reuse_validation = env_usize("HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION", 1) != 0;
    let open_files = env_usize(
        "HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES",
        hawdb::DatabaseConfig::default().max_open_files,
    );
    let segment_bytes = env_usize(
        "HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES",
        64 * 1024 * 1024,
    ) as u64;
    let embedding_dimension = env_usize("HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS", 0);
    let lexical_build_memory_bytes = env_usize(
        "HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES",
        32 * 1024 * 1024,
    ) as u64;
    let content_bytes = env_usize("HAWDB_SEARCH_MUTATION_BENCH_CONTENT_BYTES", CONTENT_BYTES);
    let memory_budget = env_usize(
        "HAWDB_SEARCH_MUTATION_BENCH_MEMORY_BYTES",
        256 * 1024 * 1024,
    ) as u64;
    let options = || SearchOutOfCoreGenerationBuildOptions {
        max_segment_uncompressed_bytes: NonZeroU64::new(segment_bytes)
            .expect("segment byte limit must be positive"),
        lexical_build_memory_bytes: NonZeroU64::new(lexical_build_memory_bytes)
            .expect("lexical build byte limit must be positive"),
        ..Default::default()
    };
    let task = || {
        RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(memory_budget, 0))
    };
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
    assert!(content_bytes > 0);
    let project_files =
        hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire(&root, open_files)
            .expect("benchmark descriptor budget must be admitted");
    let mut content = "bounded-search-mutation ".repeat(content_bytes / 24 + 1);
    content.truncate(content_bytes);

    let full_started = Instant::now();
    let mut initial =
        SearchOutOfCoreGenerationWriter::create_with_context(&root, options(), task())
            .expect("initial generation writer must open");
    for ordinal in 0..document_count {
        initial
            .push(document(ordinal, &content, embedding_dimension))
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
            let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                &reader,
                SearchProjectionDelta {
                    upserts: vec![projection_row(
                        document_count + seed,
                        &content,
                        embedding_dimension,
                    )],
                    ..Default::default()
                },
                options(),
                task(),
            )
            .expect("sustained seed must prepare");
            update.finish().expect("sustained seed must publish");
            drop(reader);
            reader = SearchOutOfCoreReader::open(&root).expect("seed must reopen");
        }
    }
    let sustained_document_count = document_count + sustained_seed_segments;
    let upserts = (0..touches)
        .map(|ordinal| {
            let mut row = projection_row(ordinal, &content, embedding_dimension);
            row.title = format!("Updated document {ordinal}");
            row.body.push_str(" changed");
            row.embedding = embedding(document_count + ordinal, embedding_dimension);
            row
        })
        .collect();
    let upsert_before_memory = ProcessMemorySnapshot::capture().ok();
    let upsert_started = Instant::now();
    let upsert_update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        SearchProjectionDelta {
            upserts,
            ..Default::default()
        },
        options(),
        task(),
    )
    .expect("K-document replacement must prepare");
    let (upsert_delta, upsert_report, upsert_source_reads) = upsert_update
        .finish()
        .expect("K-document replacement must publish");
    let upsert_elapsed = upsert_started.elapsed();
    assert_eq!(upsert_delta.upserted_documents, touches);
    assert_eq!(upsert_delta.deleted_documents, 0);
    assert_eq!(upsert_report.document_count, sustained_document_count);
    let upsert_refresh_started = Instant::now();
    if reuse_validation {
        reader
            .refresh()
            .expect("K-document replacement must refresh");
    } else {
        drop(reader);
        reader = SearchOutOfCoreReader::open(&root).expect("K-document replacement must reopen");
    }
    let upsert_refresh_elapsed = upsert_refresh_started.elapsed();
    assert_eq!(reader.document_count(), sustained_document_count);
    let upsert_memory = upsert_before_memory
        .zip(ProcessMemorySnapshot::capture().ok())
        .map(|(before, after)| ProcessMemoryProfile::between(before, after));
    let deletes = (document_count - touches..document_count)
        .map(|ordinal| format!("memory:{:016x}", ordinal * 2))
        .collect();
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let mutation_started = Instant::now();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        SearchProjectionDelta {
            deletes,
            ..Default::default()
        },
        options(),
        task(),
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
    let reopened = if reuse_validation {
        reader.refresh().expect("mutation generation must refresh");
        reader
    } else {
        drop(reader);
        SearchOutOfCoreReader::open(&root).expect("mutation generation must reopen")
    };
    assert_eq!(
        reopened.document_count(),
        sustained_document_count - touches
    );

    let mut sustained = Vec::with_capacity(sustained_rounds);
    if sustained_rounds > 0 {
        let mut current = reopened;
        for round in 0..sustained_rounds {
            let replacement_ordinal = if document_count > touches {
                round % (document_count - touches)
            } else {
                document_count + round % SUSTAINED_SEED_SEGMENTS
            };
            // Even IDs belong to the base and seeds; odd IDs model UUID inserts
            // into the middle of already persisted document ranges.
            let inserted_id = format!("{:016x}", round * 2 + 1);
            let mut inserted = projection_row(round, &content, embedding_dimension);
            inserted.external_id = inserted_id;
            let before_memory = ProcessMemorySnapshot::capture().ok();
            let started = Instant::now();
            let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                &current,
                SearchProjectionDelta {
                    upserts: vec![
                        projection_row(replacement_ordinal, &content, embedding_dimension),
                        inserted,
                    ],
                    ..Default::default()
                },
                options(),
                task(),
            )
            .expect("sustained mutation must prepare");
            let (delta, report, source_reads) =
                update.finish().expect("sustained mutation must publish");
            let refresh_started = Instant::now();
            let refresh = if reuse_validation {
                Some(current.refresh().expect("sustained mutation must refresh"))
            } else {
                drop(current);
                current = SearchOutOfCoreReader::open(&root)
                    .expect("sustained mutation generation must reopen");
                None
            };
            let refresh_elapsed = refresh_started.elapsed();
            let policy = hawdb::SearchOutOfCoreSegmentCompactionPolicy::default()
                .with_level_zero_target_bytes(NonZeroU64::new(8 * 1024).unwrap())
                .expect("sustained compaction policy must be valid");
            let compacted = if compaction_every != 0 && (round + 1) % compaction_every == 0 {
                SearchOutOfCoreGenerationWriter::compact_segments_with_context(
                    &current,
                    policy,
                    options(),
                    task(),
                )
                .expect("sustained compaction must run")
            } else {
                None
            };
            if compacted.is_some() {
                if reuse_validation {
                    current
                        .refresh()
                        .expect("sustained compaction must refresh");
                } else {
                    drop(current);
                    current = SearchOutOfCoreReader::open(&root)
                        .expect("sustained compaction generation must reopen");
                }
            }
            let after_memory = ProcessMemorySnapshot::capture().ok();
            let memory = before_memory
                .zip(after_memory)
                .map(|(before, after)| ProcessMemoryProfile::between(before, after));
            assert_eq!(
                current.document_count(),
                sustained_document_count - touches + round + 1,
                "sustained round {round} document count"
            );
            sustained.push(json!({
                "round": round,
                "upserted_documents": delta.upserted_documents,
                "deleted_documents": delta.deleted_documents,
                "checkpoint_bytes": report.generation_bytes,
                "checkpoint_elapsed_millis": started.elapsed().as_millis(),
                "refresh_elapsed_millis": refresh_elapsed.as_millis(),
                "validated_retractions": refresh.map(|report| report.validated_retractions),
                "reused_retractions": refresh.map(|report| report.reused_retractions),
                "opened_content_segments": refresh.map(|report| report.opened_content_segments),
                "reused_content_segments": refresh.map(|report| report.reused_content_segments),
                "source_segment_bytes_read": source_reads.segment_bytes_read,
                "source_hydrated_documents": source_reads.hydrated_documents,
                "compaction_published": compacted.is_some(),
                "compaction_artifact_bytes": compacted.as_ref().map_or(0, |report| report.build().generation_bytes),
                "compaction_source_bytes": compacted.as_ref().map_or(0, |report| report.source_bytes()),
                "document_count": current.document_count(),
                "steady_resident_growth_bytes": memory
                    .map(|profile| profile.steady_resident_growth_bytes),
                "steady_resident_bytes": memory.map(|profile| profile.steady_resident_bytes),
                "lifetime_peak_resident_bytes": memory.map(|profile| profile.peak_resident_bytes),
                "lifetime_peak_resident_growth_bytes": memory
                    .map(|profile| profile.lifetime_peak_resident_growth_bytes),
            }));
        }
        drop(current);
    }

    println!(
        "search_mutation {}",
        json!({
            "document_count": document_count,
            "touches": touches,
            "content_bytes_per_document": content_bytes,
            "logical_corpus_body_bytes": document_count as u64 * content_bytes as u64,
            "build_memory_budget_bytes": memory_budget,
            "max_segment_uncompressed_bytes": segment_bytes,
            "lexical_build_memory_bytes": lexical_build_memory_bytes,
            "configured_file_descriptor_limit": open_files,
            "compaction_every_rounds": compaction_every,
            "reuse_validated_retractions": reuse_validation,
            "full_generation_bytes": initial_report.generation_bytes,
            "embedding_dimension": embedding_dimension,
            "full_vector_document_count": initial_report.vector_document_count,
            "full_vector_payload_bytes": initial_report.vector_payload_bytes,
            "full_rabitq_artifact_bytes": initial_report.rabitq_artifact_bytes,
            "upsert_checkpoint_bytes": upsert_report.generation_bytes,
            "upsert_vector_payload_bytes": upsert_report.vector_payload_bytes,
            "upsert_rabitq_artifact_bytes": upsert_report.rabitq_artifact_bytes,
            "upsert_elapsed_millis": upsert_elapsed.as_millis(),
            "upsert_refresh_elapsed_millis": upsert_refresh_elapsed.as_millis(),
            "upsert_source_segment_bytes_read": upsert_source_reads.segment_bytes_read,
            "upsert_source_hydrated_documents": upsert_source_reads.hydrated_documents,
            "upsert_steady_resident_bytes": upsert_memory.map(|profile| profile.steady_resident_bytes),
            "upsert_lifetime_peak_resident_bytes": upsert_memory.map(|profile| profile.peak_resident_bytes),
            "mutation_checkpoint_bytes": mutation_report.generation_bytes,
            "mutation_to_full_write_ratio": mutation_report.generation_bytes as f64
                / initial_report.generation_bytes as f64,
            "full_elapsed_millis": full_elapsed.as_millis(),
            "mutation_elapsed_millis": mutation_elapsed.as_millis(),
            "source_segment_bytes_read": source_reads.segment_bytes_read,
            "source_hydrated_documents": source_reads.hydrated_documents,
            "steady_resident_growth_bytes": memory.map(|profile| profile.steady_resident_growth_bytes),
            "steady_resident_bytes": memory.map(|profile| profile.steady_resident_bytes),
            "lifetime_peak_resident_bytes": memory.map(|profile| profile.peak_resident_bytes),
            "lifetime_peak_resident_growth_bytes": memory
                .map(|profile| profile.lifetime_peak_resident_growth_bytes),
            "sustained_rounds": sustained,
        })
    );

    drop(project_files);
    std::fs::remove_dir_all(root).expect("benchmark directory must be removable");
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn embedding(ordinal: usize, dimension: usize) -> Option<Vec<f32>> {
    (dimension != 0).then(|| {
        (0..dimension)
            .map(|column| {
                let mut value = (ordinal as u64)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add(column as u64);
                value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                value ^= value >> 31;
                (value >> 40) as f32 / 8_388_608.0 - 1.0
            })
            .collect()
    })
}

fn document(ordinal: usize, content: &str, dimension: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{:016x}", ordinal * 2),
        title: format!("Document {ordinal}"),
        content: content.to_string(),
        embedding: embedding(ordinal, dimension),
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}

fn projection_row(ordinal: usize, content: &str, dimension: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{:016x}", ordinal * 2),
        title: format!("Document {ordinal}"),
        body: content.to_string(),
        embedding: embedding(ordinal, dimension),
        source_id: None,
        metadata: BTreeMap::from([
            ("kind".to_string(), "memory".to_string()),
            ("space_id".to_string(), "default".to_string()),
        ]),
    }
}
