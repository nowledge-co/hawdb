// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual complete resident/file vector-query comparison; use the global release profile.

use hawdb::{
    SearchDocument, SearchEmbeddingManifest, SearchIndex, SearchMode,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchQueryOptions, SearchResultSet, VectorIndexBackend,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn vector(row: usize, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|column| {
            let mut state = (row as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (column as u64 + 7).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            state ^= state >> 30;
            state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
            ((state >> 40) as f32 / 8_388_608.0) - 1.0
        })
        .collect()
}

fn document(row: usize, dimension: usize) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{row:016x}"),
        title: String::new(),
        content: String::new(),
        embedding: Some(vector(row, dimension)),
        metadata: BTreeMap::new(),
    }
}

fn options() -> SearchQueryOptions {
    SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: Some(128),
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

fn build_file(path: &Path, rows: usize, dimension: usize) -> Result<(), Box<dyn Error>> {
    // A persistent comparison fixture must be newly created by this invocation.
    std::fs::create_dir(path)?;
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        path,
        SearchOutOfCoreGenerationBuildOptions {
            vector_backend: Some(VectorIndexBackend::default()),
            source_graph_commit_epoch: Some(1),
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "hawdb-exact-distance-profile".into(),
                version: Some("v1".into()),
                dimension,
            }),
            ..Default::default()
        },
    )?;
    for row in 0..rows {
        writer.push(document(row, dimension))?;
    }
    writer.finish()?;
    Ok(())
}

fn file_worker(path: &Path, rows: usize, dimension: usize) -> Result<(), Box<dyn Error>> {
    let reader = SearchOutOfCoreReader::open(path)?;
    let embedding = vector(999_999, dimension);
    println!("READY");
    io::stdout().flush()?;
    for (repetition, line) in io::stdin().lock().lines().enumerate() {
        let exact = match line?.as_str() {
            "default" => false,
            "exact" => true,
            "quit" => return Ok(()),
            _ => return Err("expected default, exact or quit".into()),
        };
        let start = Instant::now();
        let output = if exact {
            reader.search_with_options_exact("", Some(&embedding), SearchMode::Vector, options())?
        } else {
            reader.search_with_options("", Some(&embedding), SearchMode::Vector, options())?
        };
        let nanos = start.elapsed().as_nanos();
        let metrics = output.metrics;
        println!(
            "QUERY_RESULT {}",
            json!({
                "residency":"file","rows":rows,"dimension":dimension,"exact":exact,
                "repetition":repetition,"elapsed_ns":nanos,
                "hits":output.result.hits.iter().map(|hit|
                    (&hit.id,hit.vector_score.to_bits())).collect::<Vec<_>>(),
                "metrics":{
                    "segment_range_reads":metrics.segment_range_reads,
                    "segment_bytes_read":metrics.segment_bytes_read,
                    "metadata_segment_bytes_read":metrics.metadata_segment_bytes_read,
                    "vector_segment_bytes_read":metrics.vector_segment_bytes_read,
                    "hydration_segment_bytes_read":metrics.hydration_segment_bytes_read,
                    "vector_bytes_read":metrics.vector_bytes_read,
                    "rabitq_payload_bytes_read":metrics.rabitq_payload_bytes_read,
                    "hydrated_documents":metrics.hydrated_documents,
                    "hydrated_bytes":metrics.hydrated_bytes,
                },
                "retrievers":output.result.retrievers.iter().map(|report| json!({
                    "name":report.name,"backend":report.backend,
                    "kernel":report.candidate_scan_kernel,
                    "scored_documents":report.candidate_scan_scored_document_count,
                    "reranked_candidates":report.reranked_candidate_count,
                    "raw_vector_bytes":report.raw_vector_bytes_read,
                    "candidate_working_bytes":report.candidate_scan_admitted_working_bytes,
                })).collect::<Vec<_>>(),
            })
        );
        io::stdout().flush()?;
    }
    Ok(())
}

fn measure(
    residency: &str,
    rows: usize,
    dimension: usize,
    query: impl Fn(&[f32], bool) -> hawdb::Result<SearchResultSet>,
) -> Result<(), Box<dyn Error>> {
    for exact in [false, true] {
        let embedding = vector(999_999, dimension);
        let expected = query(&embedding, exact)?;
        let bits = |result: &SearchResultSet| {
            result
                .hits
                .iter()
                .map(|hit| (hit.id.clone(), hit.vector_score.to_bits()))
                .collect::<Vec<_>>()
        };
        let expected_bits = bits(&expected);
        assert_eq!(expected_bits.len(), 10);
        for _ in 0..2 {
            assert_eq!(bits(&query(&embedding, exact)?), expected_bits);
        }
        for repetition in 0..21 {
            let start = Instant::now();
            let output = query(&embedding, exact)?;
            let nanos = start.elapsed().as_nanos();
            assert_eq!(bits(&output), expected_bits);
            println!(
                "QUERY_RESULT {}",
                json!({
                    "residency":residency,"rows":rows,"dimension":dimension,"exact":exact,
                    "repetition":repetition,"elapsed_ns":nanos,"hits":expected_bits,
                    "retrievers":output.retrievers.iter().map(|report| json!({
                    "name":report.name,"backend":report.backend,
                    "kernel":report.candidate_scan_kernel,
                    "scored_documents":report.candidate_scan_scored_document_count,
                    "reranked_candidates":report.reranked_candidate_count,
                    "raw_vector_bytes":report.raw_vector_bytes_read,
                    "candidate_working_bytes":report.candidate_scan_admitted_working_bytes,
                })).collect::<Vec<_>>(),
                })
            );
        }
    }
    Ok(())
}

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    if cfg!(debug_assertions) {
        return Err("use the repository-wide release profile".into());
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !args.is_empty() {
        let [command, path, rows, dimension] = args.as_slice() else {
            return Err(
                "expected build-file|file-worker <new-fixture-path> <rows> <dimension>".into(),
            );
        };
        let rows = rows.parse::<usize>()?;
        let dimension = dimension.parse::<usize>()?;
        if rows == 0 || dimension == 0 {
            return Err("rows and dimension must be positive".into());
        }
        return match command.as_str() {
            "build-file" => build_file(Path::new(path), rows, dimension),
            "file-worker" => file_worker(Path::new(path), rows, dimension),
            _ => Err("expected build-file or file-worker".into()),
        };
    }
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-exact-profile-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    )));
    std::fs::create_dir(&directory.0)?;
    for rows in [64, 1024] {
        for dimension in [3, 32, 384, 1536] {
            let mut index =
                SearchIndex::open(directory.0.join(format!("resident-{rows}-{dimension}")))?;
            for row in 0..rows {
                index.upsert(document(row, dimension))?;
            }
            measure("resident", rows, dimension, |embedding, exact| {
                if exact {
                    index.search_with_options_exact(
                        "",
                        Some(embedding),
                        SearchMode::Vector,
                        options(),
                    )
                } else {
                    index.search_with_options("", Some(embedding), SearchMode::Vector, options())
                }
            })?;

            let path = directory.0.join(format!("file-{rows}-{dimension}"));
            std::fs::create_dir(&path)?;
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &path,
                SearchOutOfCoreGenerationBuildOptions {
                    vector_backend: Some(VectorIndexBackend::default()),
                    source_graph_commit_epoch: Some(1),
                    embedding_manifest: Some(SearchEmbeddingManifest {
                        model: "hawdb-exact-distance-profile".into(),
                        version: Some("v1".into()),
                        dimension,
                    }),
                    ..Default::default()
                },
            )?;
            for row in 0..rows {
                writer.push(document(row, dimension))?;
            }
            writer.finish()?;
            let reader = SearchOutOfCoreReader::open(&path)?;
            measure("file", rows, dimension, |embedding, exact| {
                if exact {
                    reader
                        .search_with_options_exact(
                            "",
                            Some(embedding),
                            SearchMode::Vector,
                            options(),
                        )
                        .map(|out| out.result)
                } else {
                    reader
                        .search_with_options("", Some(embedding), SearchMode::Vector, options())
                        .map(|out| out.result)
                }
            })?;
        }
    }
    Ok(())
}
