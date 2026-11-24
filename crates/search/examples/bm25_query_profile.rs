// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual, deterministic end-to-end BM25 comparison over a shared generation.

use hawdb_search::{
    SearchDocument, SearchHitHydration, SearchMode, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader, SearchQueryOptions,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, BufRead, Write};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Instant;

fn build(root: &Path, documents: usize, segment_rows: usize) -> Result<(), Box<dyn Error>> {
    if documents == 0 || documents > 100_000 || segment_rows == 0 {
        return Err("expected 1..=100000 documents and a nonzero segment size".into());
    }
    std::fs::create_dir(root)?;
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(1),
        max_content_documents: NonZeroUsize::new(segment_rows).unwrap(),
        ..Default::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, options)?;
    for id in 0..documents {
        let mut content = format!("unique{id:06} ");
        for term in 0..32 {
            content.push_str(&format!("dense{term:03} "));
        }
        for term in 0..8 {
            let bucket = (id * (2 * term + 1) + 31 * term) % 256;
            content.push_str(&format!("sparse{bucket:03} "));
        }
        if id % 17 == 0 {
            content.push_str(
                "database database storage recovery \u{4e2d}\u{6587}\u{6570}\u{636e}\u{5e93} ",
            );
        }
        writer.push(SearchDocument {
            id: format!("memory:{id:08}"),
            title: "Deterministic BM25 fixture".into(),
            content,
            embedding: None,
            metadata: BTreeMap::from([("scope".into(), (id % 4).to_string())]),
        })?;
    }
    let report = writer.finish()?;
    assert_eq!(report.document_count, documents);
    println!("{}", json!({"phase": "built", "report": report}));
    Ok(())
}

fn import(root: &Path, corpus: &Path, segment_rows: usize) -> Result<(), Box<dyn Error>> {
    let segment_rows = NonZeroUsize::new(segment_rows).ok_or("zero segment size")?;
    std::fs::create_dir(root)?;
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(1),
            max_content_documents: segment_rows,
            max_documents: NonZeroUsize::new(100_000).unwrap(),
            ..Default::default()
        },
    )?;
    for line in io::BufReader::new(std::fs::File::open(corpus)?).lines() {
        let record: serde_json::Value = serde_json::from_str(&line?)?;
        writer.push(SearchDocument {
            id: record["_id"].as_str().ok_or("missing corpus ID")?.into(),
            title: record["title"].as_str().unwrap_or_default().into(),
            content: record["text"].as_str().ok_or("missing corpus text")?.into(),
            embedding: None,
            metadata: BTreeMap::new(),
        })?;
    }
    println!("{}", json!({"phase":"imported", "report":writer.finish()?}));
    Ok(())
}

fn worker(root: &Path) -> Result<(), Box<dyn Error>> {
    let reader = SearchOutOfCoreReader::open(root)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let request: serde_json::Value = serde_json::from_str(&line?)?;
        let query = request["query"].as_str().ok_or("missing query")?;
        let offset = request["offset"].as_u64().unwrap_or(0) as usize;
        let filters = request["scope"]
            .as_str()
            .map_or_else(BTreeMap::new, |scope| {
                BTreeMap::from([("scope".into(), scope.into())])
            });
        let options = SearchQueryOptions {
            limit: 10,
            offset,
            rank_window: Some(100),
            fusion_weights: Default::default(),
            metadata_filters: filters,
            policy_epoch: None,
        };
        let started = Instant::now();
        let output = reader.search_with_hydration(
            query,
            None,
            SearchMode::Text,
            options,
            Default::default(),
            SearchHitHydration::ScoresOnly,
        );
        let elapsed = started.elapsed().as_nanos();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                writeln!(
                    stdout,
                    "{}",
                    json!({"elapsed_ns":elapsed,"error":error.to_string()})
                )?;
                stdout.flush()?;
                continue;
            }
        };
        let hits = output
            .result
            .hits
            .iter()
            .map(|hit| {
                json!({
                    "id": hit.id, "score": hit.score.to_bits(),
                    "text_score": hit.text_score.to_bits(),
                    "rrf_score": hit.rrf_score.to_bits(), "text_rank": hit.text_rank,
                })
            })
            .collect::<Vec<_>>();
        writeln!(
            stdout,
            "{}",
            json!({
                "elapsed_ns": elapsed, "hits": hits,
                "total_hits": output.result.total_hits,
                "truncated": output.result.truncated,
                "document_count": output.result.document_count,
                "filtered_document_count": output.result.filtered_document_count,
                "metrics": {
                    "segment_bytes_read": output.metrics.segment_bytes_read,
                    "query_memory_peak_bytes": output.metrics.query_memory_peak_bytes,
                },
                "retrievers": output.result.retrievers.iter().map(|report| json!({
                    "name": report.name, "posting_bytes_read": report.posting_bytes_read,
                    "postings_visited": report.candidate_postings_visited,
                    "blocks_skipped": report.candidate_blocks_skipped,
                })).collect::<Vec<_>>(),
            })
        )?;
        stdout.flush()?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some("build") if args.len() == 5 => {
            build(Path::new(&args[2]), args[3].parse()?, args[4].parse()?)
        }
        Some("import") if args.len() == 5 => {
            import(Path::new(&args[2]), Path::new(&args[3]), args[4].parse()?)
        }
        Some("worker") if args.len() == 3 => worker(Path::new(&args[2])),
        _ => Err("usage: bm25_query_profile build NEW_ROOT DOCUMENTS SEGMENT_ROWS | import NEW_ROOT CORPUS_JSONL SEGMENT_ROWS | worker ROOT".into()),
    }
}
