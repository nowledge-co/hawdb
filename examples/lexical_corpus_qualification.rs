//! Developer-only full-corpus artifact measurement through the embedded facade.
//! Never publish source records; report aggregate counts and physical extents.

use hawdb::{
    SearchDocument, SearchLexicalTermPolicy, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize)]
struct Input {
    content_message_id: String,
    content: String,
}

struct Record {
    id: String,
    offset: u64,
    length: usize,
}

fn number(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| format!("missing numeric field: {key}").into())
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("missing string field: {key}").into())
}

fn scan(source: &Path) -> Result<(Vec<Record>, Value)> {
    let mut input = BufReader::new(File::open(source)?);
    let mut records = Vec::new();
    let mut line = Vec::new();
    let mut offset = 0;
    let mut content_bytes = 0u64;
    let mut max_content_bytes = 0;
    let mut id_bytes = 0u64;
    loop {
        line.clear();
        let length = input.read_until(b'\n', &mut line)?;
        if length == 0 {
            break;
        }
        let record: Input = serde_json::from_slice(&line)?;
        if record.content_message_id.is_empty() {
            return Err("empty document ID".into());
        }
        content_bytes += record.content.len() as u64;
        max_content_bytes = max_content_bytes.max(record.content.len());
        id_bytes += record.content_message_id.len() as u64;
        records.push(Record {
            id: record.content_message_id,
            offset,
            length,
        });
        offset += length as u64;
    }
    records.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    if records.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err("duplicate document IDs; corpus was not filtered".into());
    }
    let stats = json!({
        "document_count": records.len(), "source_jsonl_bytes": offset,
        "content_bytes": content_bytes, "max_content_bytes": max_content_bytes,
        "id_bytes": id_bytes,
        "mapping": "id=content_message_id unchanged; content=content; empty title/metadata; no embedding",
        "input_order": "strict ascending UTF-8 document ID; full corpus; no sampling or truncation",
        "harness_index_capacity_bytes": records.capacity() * std::mem::size_of::<Record>()
            + records.iter().map(|record| record.id.capacity()).sum::<usize>(),
    });
    Ok((records, stats))
}

fn artifact_metrics(body: &Value) -> Result<Value> {
    let artifact_len = number(body, "artifact_len")?;
    let format = string(body, "format")?;
    let layout = string(body, "layout")?;
    let blocks = body["blocks"].as_array().ok_or("missing artifact blocks")?;
    let header_bytes = blocks
        .first()
        .map(|block| number(block, "offset"))
        .transpose()?
        .unwrap_or(artifact_len);
    let mut end = header_bytes;
    let mut posting_bytes = 0u64;
    let mut document_bytes = 0u64;
    for block in blocks {
        if number(block, "offset")? != end {
            return Err("noncontiguous artifact blocks".into());
        }
        let length = number(block, "length")?;
        end = end.checked_add(length).ok_or("artifact size overflow")?;
        match block["kind"].as_str() {
            Some("Documents") => document_bytes = document_bytes.saturating_add(length),
            Some("Postings") => posting_bytes = posting_bytes.saturating_add(length),
            _ => return Err("unknown artifact block kind".into()),
        }
    }
    if end != artifact_len {
        return Err("artifact block extents do not cover the artifact".into());
    }
    if format == "HAWDB_LEXICAL_MANIFEST_V3" {
        if layout != "HAWDB_LEXICAL_ORDINAL_FST_V1" {
            return Err("V3 manifest has an unexpected lexical layout".into());
        }
        if posting_bytes != number(body, "posting_bytes")? {
            return Err("FST posting extents differ from the manifest counter".into());
        }
    }
    Ok(json!({
        "format": format, "layout": layout, "artifact_bytes": artifact_len,
        "actual_posting_bytes": posting_bytes, "document_mapping_bytes": document_bytes,
        "header_bytes": header_bytes,
        "legacy_nominal_posting_bytes": body.get("legacy_posting_bytes").cloned(),
        "dictionary_storage": if format == "HAWDB_LEXICAL_MANIFEST_V3" {
            "co-encoded in Posting blocks"
        } else {
            "separate manifest term statistics"
        },
        "posting_count": number(body, "posting_count")?,
        "document_count": number(body, "document_count")?,
        "total_document_len": number(body, "total_document_len")?,
        "analyzer_digest": number(body, "analyzer_digest")?,
        "documents_digest": number(body, "documents_digest")?,
    }))
}

fn require_equal(left: &Value, right: &Value, field: &str) -> Result<()> {
    if left != right {
        return Err(format!("qualification receipts differ for {field}").into());
    }
    Ok(())
}

fn compare_receipts(baseline: &Value, compact: &Value) -> Result<Value> {
    if string(baseline, "receipt_format")? != "HAWDB_LEXICAL_CORPUS_QUALIFICATION_V1" {
        return Err("baseline receipt has an unsupported format".into());
    }
    if string(compact, "receipt_format")? != "HAWDB_LEXICAL_CORPUS_QUALIFICATION_V1" {
        return Err("compact receipt has an unsupported format".into());
    }
    let baseline_source = &baseline["source"];
    let compact_source = &compact["source"];
    for field in [
        "document_count",
        "source_jsonl_bytes",
        "content_bytes",
        "max_content_bytes",
        "id_bytes",
        "mapping",
        "input_order",
    ] {
        require_equal(&baseline_source[field], &compact_source[field], field)?;
    }
    let baseline_artifact = &baseline["actual_lexical_artifact"];
    let compact_artifact = &compact["actual_lexical_artifact"];
    if string(baseline_artifact, "format")? == "HAWDB_LEXICAL_MANIFEST_V3"
        && string(baseline_artifact, "layout")? == "HAWDB_LEXICAL_ORDINAL_FST_V1"
    {
        return Err("baseline receipt must use the pre-FST lexical layout".into());
    }
    for field in [
        "document_count",
        "posting_count",
        "total_document_len",
        "analyzer_digest",
        "documents_digest",
    ] {
        require_equal(&baseline_artifact[field], &compact_artifact[field], field)?;
    }
    if string(compact_artifact, "format")? != "HAWDB_LEXICAL_MANIFEST_V3"
        || string(compact_artifact, "layout")? != "HAWDB_LEXICAL_ORDINAL_FST_V1"
    {
        return Err("compact receipt is not the FST lexical layout".into());
    }
    for field in [
        "configured_term_limit_bytes",
        "configured_manifest_limit_bytes",
    ] {
        require_equal(&baseline[field], &compact[field], field)?;
    }
    let baseline_posting_bytes = number(baseline_artifact, "actual_posting_bytes")?;
    let compact_posting_bytes = number(compact_artifact, "actual_posting_bytes")?;
    if compact_posting_bytes == 0 {
        return Err("compact receipt has no posting bytes to compare".into());
    }
    if baseline_posting_bytes < compact_posting_bytes.saturating_mul(10) {
        return Err("compact postings do not achieve the required 10x reduction".into());
    }
    Ok(json!({
        "baseline_actual_posting_bytes": baseline_posting_bytes,
        "compact_actual_posting_bytes": compact_posting_bytes,
        "minimum_reduction_ratio": 10,
        "actual_posting_byte_reduction_ratio": baseline_posting_bytes as f64 / compact_posting_bytes as f64,
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if !(6..=7).contains(&args.len()) {
        return Err("usage: lexical_corpus_qualification SOURCE_JSONL NEW_OUTPUT_ROOT EXPECTED_DOCUMENTS MAX_TERM_BYTES MAX_MANIFEST_BYTES [BASELINE_RECEIPT]".into());
    }
    let source = Path::new(&args[1]);
    let root = Path::new(&args[2]);
    let expected: usize = args[3].to_str().ok_or("invalid count")?.parse()?;
    let term_limit = args[4].to_str().ok_or("invalid term limit")?.parse()?;
    let policy = SearchLexicalTermPolicy::new(
        NonZeroU64::new(term_limit).ok_or("term limit must be nonzero")?,
    )?;
    let manifest_limit =
        NonZeroU64::new(args[5].to_str().ok_or("invalid manifest limit")?.parse()?)
            .ok_or("manifest limit must be nonzero")?;
    if root.exists() {
        return Err("output root already exists; refusing reuse".into());
    }
    let started = Instant::now();
    let (records, source_stats) = scan(source)?;
    if records.len() != expected {
        return Err("source count differs from export manifest".into());
    }
    eprintln!("source_verified {}", source_stats);
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let options_debug = format!("{options:?}");
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_term_policy(root, options, policy)?;
    writer.set_max_lexical_manifest_bytes(manifest_limit)?;
    let mut input = File::open(source)?;
    let mut line = Vec::new();
    for (index, record) in records.into_iter().enumerate() {
        input.seek(SeekFrom::Start(record.offset))?;
        line.resize(record.length, 0);
        input.read_exact(&mut line)?;
        let decoded: Input = serde_json::from_slice(&line)?;
        if decoded.content_message_id != record.id {
            return Err("source changed between scanning and spooling".into());
        }
        writer.push(SearchDocument {
            id: decoded.content_message_id,
            title: String::new(),
            content: decoded.content,
            embedding: None,
            metadata: BTreeMap::new(),
        })?;
        if (index + 1) % 50_000 == 0 {
            eprintln!(
                "spooled {} documents, elapsed_ms={}",
                index + 1,
                started.elapsed().as_millis()
            );
        }
    }
    eprintln!(
        "finishing generation, elapsed_ms={}",
        started.elapsed().as_millis()
    );
    let report = writer.finish()?;
    let build_elapsed_ms = started.elapsed().as_millis();
    let manifest_path = root.join(format!(
        "search_lexical.manifest.{}.hawdb",
        report.lexical_generation
    ));
    let manifest: Value = serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
    let body = &manifest["body"];
    let artifact_file = body["artifact_file"]
        .as_str()
        .ok_or("missing artifact name")?;
    if Path::new(artifact_file)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(artifact_file)
    {
        return Err("invalid artifact filename".into());
    }
    if fs::metadata(root.join(artifact_file))?.len() != number(body, "artifact_len")? {
        return Err("actual file size differs from manifest".into());
    }
    let metrics = artifact_metrics(body)?;
    let manifest_document_count = number(body, "document_count")?;
    // Keep measurement metadata from overlapping the reopened database's dictionary.
    drop(manifest);
    let reader = SearchOutOfCoreReader::open_with_term_policy(
        root,
        SearchOutOfCoreConfig {
            max_lexical_manifest_bytes: manifest_limit,
            ..Default::default()
        },
        Default::default(),
        policy,
    )?;
    if reader.document_count() != expected || manifest_document_count != expected as u64 {
        return Err("reopened document count mismatch".into());
    }
    let mut receipt = json!({
        "receipt_format": "HAWDB_LEXICAL_CORPUS_QUALIFICATION_V1",
        "source": source_stats, "build_options_debug": options_debug,
        "build_report": report, "actual_lexical_artifact": metrics,
        "lexical_manifest_bytes": fs::metadata(manifest_path)?.len(),
        "configured_reopen_document_count": reader.document_count(),
        "configured_term_limit_bytes": policy.max_term_bytes().get(),
        "configured_manifest_limit_bytes": manifest_limit.get(),
        "build_elapsed_ms": build_elapsed_ms, "total_elapsed_ms": started.elapsed().as_millis(),
        "scope": "actual artifact byte comparison; no latency or whole-process memory qualification",
    });
    if let Some(path) = args.get(6) {
        let baseline: Value = serde_json::from_reader(BufReader::new(File::open(path)?))?;
        let comparison = compare_receipts(&baseline, &receipt)?;
        receipt
            .as_object_mut()
            .ok_or("receipt is not a JSON object")?
            .insert("comparison".into(), comparison);
    }
    println!("{}", serde_json::to_string_pretty(&receipt)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy() -> Value {
        json!({"format": "HAWDB_LEXICAL_MANIFEST_V1", "layout": "HAWDB_LEXICAL_STRING_POSTINGS_V1",
            "artifact_len": 90, "posting_count": 4, "document_count": 2,
            "total_document_len": 8, "analyzer_digest": 10, "documents_digest": 11,
            "blocks": [
                {"kind":"Documents", "offset":10, "length":20},
                {"kind":"Postings", "offset":30, "length":60}]})
    }

    #[test]
    fn legacy_counts_actual_block_extents() {
        let body = legacy();
        let metrics = artifact_metrics(&body).unwrap();
        assert_eq!(metrics["actual_posting_bytes"], 60);
        assert_eq!(metrics["document_mapping_bytes"], 20);
        let mut broken = body;
        broken["blocks"][1]["offset"] = json!(31);
        assert!(artifact_metrics(&broken).is_err());
    }

    #[test]
    fn fst_counts_the_physical_posting_block_and_rejects_mismatched_counter() {
        let body = json!({
            "format": "HAWDB_LEXICAL_MANIFEST_V3", "layout": "HAWDB_LEXICAL_ORDINAL_FST_V1",
            "artifact_len": 90, "posting_count": 4, "document_count": 2,
            "total_document_len": 8, "analyzer_digest": 10, "documents_digest": 11,
            "legacy_posting_bytes": 9999, "posting_bytes": 60,
            "blocks": [
                {"kind":"Documents", "offset":10, "length":20},
                {"kind":"Postings", "offset":30, "length":60}]
        });
        let metrics = artifact_metrics(&body).unwrap();
        assert_eq!(metrics["actual_posting_bytes"], 60);
        assert_eq!(metrics["legacy_nominal_posting_bytes"], 9999);
        assert_eq!(
            metrics["dictionary_storage"],
            "co-encoded in Posting blocks"
        );
        let mut broken = body;
        broken["posting_bytes"] = json!(59);
        assert!(artifact_metrics(&broken).is_err());
    }

    fn receipt(format: &str, layout: &str, posting_bytes: u64) -> Value {
        json!({
            "receipt_format": "HAWDB_LEXICAL_CORPUS_QUALIFICATION_V1",
            "source": {
                "document_count": 2, "source_jsonl_bytes": 64, "content_bytes": 8,
                "max_content_bytes": 5, "id_bytes": 4, "mapping": "unchanged",
                "input_order": "strict ascending UTF-8 document ID; full corpus; no sampling or truncation"
            },
            "actual_lexical_artifact": {
                "format": format, "layout": layout, "actual_posting_bytes": posting_bytes,
                "document_count": 2, "posting_count": 4, "total_document_len": 8,
                "analyzer_digest": 10, "documents_digest": 11
            },
            "configured_term_limit_bytes": 1048576,
            "configured_manifest_limit_bytes": 536870912
        })
    }

    #[test]
    fn receipt_comparison_requires_the_same_corpus_and_a_tenfold_fst_reduction() {
        let baseline = receipt(
            "HAWDB_LEXICAL_MANIFEST_V1",
            "HAWDB_LEXICAL_STRING_POSTINGS_V1",
            600,
        );
        let compact = receipt(
            "HAWDB_LEXICAL_MANIFEST_V3",
            "HAWDB_LEXICAL_ORDINAL_FST_V1",
            60,
        );
        let comparison = compare_receipts(&baseline, &compact).unwrap();
        assert_eq!(comparison["actual_posting_byte_reduction_ratio"], 10.0);

        let wrong_baseline = receipt(
            "HAWDB_LEXICAL_MANIFEST_V3",
            "HAWDB_LEXICAL_ORDINAL_FST_V1",
            600,
        );
        assert!(compare_receipts(&wrong_baseline, &compact).is_err());

        let mut wrong_source = compact;
        wrong_source["source"]["content_bytes"] = json!(9);
        assert!(compare_receipts(&baseline, &wrong_source).is_err());
    }
}
