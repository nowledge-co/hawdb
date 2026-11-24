// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual same-index beam comparison with independent f64 L2 ground truth.

use hawdb::{
    CompressedVectorSearchMode, SearchDocument, SearchEmbeddingManifest, SearchHitHydration,
    SearchMode, SearchOutOfCoreExecutionContext, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreOutput, SearchOutOfCoreReader,
    SearchQueryOptions, VectorDimension, VectorDistance, VectorIndexBackend, VectorIndexDescriptor,
    VectorIndexTarget, VectorSearchExecutionOptions,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Instant;

const TOP_K: usize = 10;
const MAX_ROWS: usize = 100_000;
const MODEL: &str = "hawdb-fvecs-l2-beam-comparison-v1";
const BEAMS: [Option<usize>; 5] = [Some(128), Some(256), Some(512), Some(1024), None];

fn vectors(path: &Path, count: usize) -> Result<Vec<Vec<f32>>, Box<dyn Error>> {
    if !(1..=MAX_ROWS).contains(&count) {
        return Err("vector count must be in 1..=100000".into());
    }
    let mut input = BufReader::new(File::open(path)?);
    let mut output: Vec<Vec<f32>> = Vec::with_capacity(count);
    for _ in 0..count {
        let mut encoded_dimension = [0; 4];
        input.read_exact(&mut encoded_dimension)?;
        let dimension = u32::from_le_bytes(encoded_dimension) as usize;
        if !(1..=4096).contains(&dimension)
            || output.first().is_some_and(|first| first.len() != dimension)
        {
            return Err("fvecs dimensions are invalid or inconsistent".into());
        }
        let mut encoded = vec![0; dimension * 4];
        input.read_exact(&mut encoded)?;
        let row = encoded
            .as_chunks::<4>()
            .0
            .iter()
            .map(|bytes| f32::from_le_bytes(*bytes))
            .collect::<Vec<_>>();
        if row.iter().any(|value| !value.is_finite()) {
            return Err("fvecs contains a non-finite value".into());
        }
        output.push(row);
    }
    Ok(output)
}

fn build(root: &Path, source: &Path, count: usize) -> Result<(), Box<dyn Error>> {
    let rows = vectors(source, count)?;
    std::fs::create_dir(root)?;
    let started = Instant::now();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(1),
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: MODEL.into(),
                version: Some("1".into()),
                dimension: rows[0].len(),
            }),
            vector_backend: Some(VectorIndexBackend::default()),
            vector_index_descriptor: Some(VectorIndexDescriptor::new(
                "vectors.embedding".into(),
                VectorIndexTarget::Relational {
                    table: "vectors".into(),
                    column: "embedding".into(),
                    column_ordinal: 0,
                },
                VectorDimension::new(rows[0].len() as u32).ok_or("invalid vector dimension")?,
                VectorDistance::Euclidean,
                VectorIndexBackend::default(),
                [0; 32],
            )?),
            ..Default::default()
        },
    )?;
    for (id, row) in rows.into_iter().enumerate() {
        writer.push(SearchDocument {
            id: format!("memory:{id:016x}"),
            title: String::new(),
            content: String::new(),
            embedding: Some(row),
            metadata: BTreeMap::from([
                ("bucket10".into(), (id % 10).to_string()),
                ("bucket100".into(), (id % 100).to_string()),
            ]),
        })?;
    }
    let report = writer.finish()?;
    println!(
        "{}",
        json!({"phase":"build", "elapsed_ns":started.elapsed().as_nanos(), "report":report})
    );
    Ok(())
}

fn reference(rows: &[Vec<f32>], query: &[f32], modulus: usize, bucket: usize) -> BTreeSet<String> {
    let mut distances = rows
        .iter()
        .enumerate()
        .filter(|(id, _)| id % modulus == bucket)
        .map(|(id, row)| {
            let squared = row
                .iter()
                .zip(query)
                .map(|(left, right)| {
                    let delta = f64::from(*left) - f64::from(*right);
                    delta * delta
                })
                .sum::<f64>();
            (squared, id)
        })
        .collect::<Vec<_>>();
    distances.sort_unstable_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
    distances
        .into_iter()
        .take(TOP_K)
        .map(|(_, id)| format!("memory:{id:016x}"))
        .collect()
}

fn query(
    reader: &SearchOutOfCoreReader,
    embedding: &[f32],
    modulus: usize,
    bucket: usize,
    beam: Option<usize>,
) -> hawdb::Result<SearchOutOfCoreOutput> {
    let mut execution = VectorSearchExecutionOptions::default();
    if let Some(beam) = beam {
        execution = execution.with_navigation_beam(NonZeroUsize::new(beam).expect("positive beam"));
    }
    reader.search_with_hydration(
        "",
        Some(embedding),
        SearchMode::Vector,
        SearchQueryOptions {
            limit: TOP_K,
            offset: 0,
            rank_window: Some(128),
            metadata_filters: if modulus == 1 {
                BTreeMap::new()
            } else {
                BTreeMap::from([(format!("bucket{modulus}"), bucket.to_string())])
            },
            fusion_weights: Default::default(),
            policy_epoch: None,
        },
        SearchOutOfCoreExecutionContext {
            access_control: None,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            vector_execution_options: execution,
        },
        SearchHitHydration::ScoresOnly,
    )
}

#[derive(Default)]
struct Measurements {
    nanos: Vec<u64>,
    overlap: usize,
    expected: usize,
    scored: usize,
    generated: usize,
    reranked: usize,
    adaptive_exact: usize,
    fallback_codes: BTreeSet<String>,
}

fn compare(
    root: &Path,
    base: &Path,
    held_out: &Path,
    count: usize,
    repeats: usize,
) -> Result<(), Box<dyn Error>> {
    if !(1..=128).contains(&count) || !(1..=10).contains(&repeats) {
        return Err("samples must be 1..=128 and repetitions 1..=10".into());
    }
    let reader = SearchOutOfCoreReader::open(root)?;
    let rows = vectors(base, reader.document_count())?;
    let queries = vectors(held_out, count)?;
    if rows[0].len() != queries[0].len()
        || reader
            .embedding_manifest()
            .is_none_or(|manifest| manifest.model != MODEL)
        || reader
            .vector_projection_qualification_identity()
            .is_none_or(|identity| identity.algorithm != "diskann")
    {
        return Err("fixture, query dimensions or backend does not match".into());
    }
    println!(
        "{}",
        json!({"phase":"open","model":MODEL,"documents":rows.len(),"dimension":rows[0].len(),"samples":count,"repetitions":repeats,"hydration":"ScoresOnly","parallelism":1,"candidate_window":128,"default_reader_navigation_beam":hawdb::SearchOutOfCoreConfig::default().default_navigation_beam.get(),"generic_navigation_beam":hawdb::search::DEFAULT_NAVIGATION_BEAM,"warmup":"one query per beam and filter","variant_order":"rotated by sample and repetition","ground_truth":"independent scalar f64 squared L2, stable ID ties"})
    );
    let mut measurements: BTreeMap<(usize, usize), Measurements> = BTreeMap::new();
    for modulus in [1, 10, 100] {
        for beam in BEAMS {
            query(&reader, &queries[0], modulus, 0, beam)?;
        }
        for (sample, embedding) in queries.iter().enumerate() {
            let bucket = sample % modulus;
            let expected = reference(&rows, embedding, modulus, bucket);
            if expected.is_empty() {
                return Err("empty exact reference".into());
            }
            for repeat in 0..repeats {
                for rotation in 0..BEAMS.len() {
                    let beam = BEAMS[(rotation + sample + repeat) % BEAMS.len()];
                    let started = Instant::now();
                    let actual = query(&reader, embedding, modulus, bucket, beam)?;
                    let nanos = u64::try_from(started.elapsed().as_nanos())?;
                    let ids = actual
                        .result
                        .hits
                        .iter()
                        .map(|hit| hit.id.clone())
                        .collect::<BTreeSet<_>>();
                    if ids.len() != actual.result.hits.len() || ids.len() > TOP_K {
                        return Err("duplicate hits or result limit exceeded".into());
                    }
                    let report = actual
                        .result
                        .retrievers
                        .iter()
                        .find(|report| report.name == "vector")
                        .ok_or("missing vector report")?;
                    if !report.backend.contains("diskann") {
                        return Err("query did not execute DiskANN".into());
                    }
                    let entry = measurements
                        .entry((modulus, beam.unwrap_or(0)))
                        .or_default();
                    entry.nanos.push(nanos);
                    entry.overlap += ids.intersection(&expected).count();
                    entry.expected += expected.len();
                    entry.scored += report.candidate_scan_scored_document_count;
                    entry.generated += report.generated_candidate_count;
                    entry.reranked += report.reranked_candidate_count;
                    entry.adaptive_exact += report.candidate_scan_adaptive_exact_scan_count;
                    entry.fallback_codes.extend(
                        report
                            .fallback_reason_codes
                            .iter()
                            .map(|code| format!("{code:?}")),
                    );
                }
            }
        }
    }
    for ((modulus, beam), mut measured) in measurements {
        measured.nanos.sort_unstable();
        let n = measured.nanos.len();
        let total: u128 = measured.nanos.iter().map(|nanos| u128::from(*nanos)).sum();
        println!(
            "{}",
            json!({"phase":"summary","filter_modulus":modulus,"beam":if beam==0 {json!("default")} else {json!(beam)},"queries":n,"recall_at_10":measured.overlap as f64 / measured.expected as f64,"qps":n as f64*1e9/total as f64,"p50_ns":measured.nanos[n/2],"p95_ns":measured.nanos[(n-1)*95/100],"mean_graph_distance_comparisons":measured.scored as f64/n as f64,"mean_generated_candidates":measured.generated as f64/n as f64,"mean_reranked_candidates":measured.reranked as f64/n as f64,"mean_adaptive_exact_scan_count":measured.adaptive_exact as f64/n as f64,"fallback_reason_codes":measured.fallback_codes})
        );
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("build") if args.len() == 4 => build(Path::new(&args[1]), Path::new(&args[2]), args[3].parse()?),
        Some("compare") if args.len() == 6 => compare(Path::new(&args[1]),Path::new(&args[2]),Path::new(&args[3]),args[4].parse()?,args[5].parse()?),
        _ => Err("usage: vector_navigation_beam build <new-root> <base.fvecs> <rows> | compare <root> <base.fvecs> <query.fvecs> <samples> <repetitions>".into()),
    }
}
