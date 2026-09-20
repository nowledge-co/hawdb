//! Developer-only full-corpus lexical artifact qualification.
//!
//! The binary accepts an explicit local corpus path and emits aggregate evidence.
//! It never publishes document text, document IDs, or a production control plane.

use hawdb::{
    SearchDocument, SearchLexicalTermPolicy, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const LEXICAL_MANIFEST_FILE: &str = "search_lexical.manifest.hawdb";

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

fn value<'a>(object: &'a Value, key: &str) -> Result<&'a Value> {
    object
        .get(key)
        .ok_or_else(|| format!("missing field: {key}").into())
}

fn number(object: &Value, key: &str) -> Result<u64> {
    value(object, key)?
        .as_u64()
        .ok_or_else(|| format!("field is not an unsigned integer: {key}").into())
}

fn text<'a>(object: &'a Value, key: &str) -> Result<&'a str> {
    value(object, key)?
        .as_str()
        .ok_or_else(|| format!("field is not text: {key}").into())
}

fn scan(source: &Path) -> Result<(Vec<Record>, Value)> {
    let mut input = BufReader::new(File::open(source)?);
    let mut records = Vec::new();
    let mut line = Vec::new();
    let mut offset = 0u64;
    let mut content_bytes = 0u64;
    let mut max_content_bytes = 0usize;
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
        content_bytes = content_bytes.saturating_add(record.content.len() as u64);
        max_content_bytes = max_content_bytes.max(record.content.len());
        id_bytes = id_bytes.saturating_add(record.content_message_id.len() as u64);
        records.push(Record {
            id: record.content_message_id,
            offset,
            length,
        });
        offset = offset
            .checked_add(length as u64)
            .ok_or("source byte offset overflow")?;
    }

    records.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    if records.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err("duplicate document IDs; corpus was not filtered".into());
    }

    let harness_index_capacity_bytes = records.capacity() * std::mem::size_of::<Record>()
        + records
            .iter()
            .map(|record| record.id.capacity())
            .sum::<usize>();
    let document_count = records.len();
    Ok((
        records,
        json!({
            "document_count": document_count,
            "source_jsonl_bytes": offset,
            "content_bytes": content_bytes,
            "max_content_bytes": max_content_bytes,
            "id_bytes": id_bytes,
            "mapping": "id=content_message_id unchanged; content=content; empty title/metadata; no embedding",
            "input_order": "strict ascending UTF-8 document ID; full corpus; no sampling or truncation",
            "harness_index_capacity_bytes": harness_index_capacity_bytes,
        }),
    ))
}

fn artifact_metrics(body: &Value) -> Result<Value> {
    let artifact_len = number(body, "artifact_len")?;
    let blocks = value(body, "blocks")?
        .as_array()
        .ok_or("lexical manifest blocks are not an array")?;
    let header_bytes = blocks
        .first()
        .map(|block| number(block, "offset"))
        .transpose()?
        .ok_or("empty lexical artifact blocks")?;
    let mut end = header_bytes;
    let mut document_mapping_bytes = 0u64;
    let mut actual_posting_bytes = 0u64;

    for block in blocks {
        if number(block, "offset")? != end {
            return Err("lexical artifact blocks are not contiguous".into());
        }
        let length = number(block, "length")?;
        end = end
            .checked_add(length)
            .ok_or("lexical artifact size overflow")?;
        match text(block, "kind")? {
            "Documents" => document_mapping_bytes = document_mapping_bytes.saturating_add(length),
            "Postings" => actual_posting_bytes = actual_posting_bytes.saturating_add(length),
            kind => return Err(format!("unknown lexical block kind: {kind}").into()),
        }
    }
    if end != artifact_len {
        return Err("lexical artifact extent differs from its manifest".into());
    }

    let layout = text(body, "layout")?;
    let recorded_posting_bytes = number(body, "posting_bytes")?;
    if actual_posting_bytes != recorded_posting_bytes {
        return Err("physical posting extent differs from its manifest counter".into());
    }
    let legacy_posting_bytes = number(body, "legacy_posting_bytes")?;
    if recorded_posting_bytes == 0 || legacy_posting_bytes == 0 {
        return Err("nonempty corpus has empty lexical posting counters".into());
    }

    Ok(json!({
        "layout": layout,
        "artifact_bytes": artifact_len,
        "header_bytes": header_bytes,
        "document_mapping_bytes": document_mapping_bytes,
        "actual_posting_bytes": actual_posting_bytes,
        "legacy_posting_bytes": legacy_posting_bytes,
        "posting_count": number(body, "posting_count")?,
        "document_count": number(body, "document_count")?,
        "total_document_len": number(body, "total_document_len")?,
        "analyzer_digest": number(body, "analyzer_digest")?,
        "documents_digest": number(body, "documents_digest")?,
        "max_term_bytes": number(body, "max_term_bytes")?,
    }))
}

fn parse_u64(value: &OsString, name: &str) -> Result<u64> {
    value
        .to_str()
        .ok_or_else(|| format!("invalid {name}"))?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}").into())
}

fn build(args: &[OsString]) -> Result<()> {
    if args.len() != 7 {
        return Err("usage: search_lexical_corpus_qualification build SOURCE_JSONL NEW_OUTPUT_ROOT EXPECTED_DOCUMENTS MAX_TERM_BYTES MAX_MANIFEST_BYTES".into());
    }
    let source = Path::new(&args[2]);
    let root = Path::new(&args[3]);
    let expected = usize::try_from(parse_u64(&args[4], "expected document count")?)?;
    let term_limit =
        NonZeroU64::new(parse_u64(&args[5], "term limit")?).ok_or("term limit must be nonzero")?;
    let manifest_limit = NonZeroU64::new(parse_u64(&args[6], "manifest limit")?)
        .ok_or("manifest limit must be nonzero")?;
    if root.exists() {
        return Err("output root already exists; refusing reuse".into());
    }

    let started = Instant::now();
    let (records, source_stats) = scan(source)?;
    if records.len() != expected {
        return Err("source count differs from the export manifest".into());
    }

    let term_policy = SearchLexicalTermPolicy::new(term_limit)?;
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let options_debug = format!("{options:?}");
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_term_policy(root, options, term_policy)?;
    writer.set_max_lexical_manifest_bytes(manifest_limit)?;

    let mut input = File::open(source)?;
    let mut line = Vec::new();
    for record in records {
        input.seek(SeekFrom::Start(record.offset))?;
        line.resize(record.length, 0);
        input.read_exact(&mut line)?;
        let decoded: Input = serde_json::from_slice(&line)?;
        if decoded.content_message_id != record.id {
            return Err("source changed between validation and spooling".into());
        }
        writer.push(SearchDocument {
            id: decoded.content_message_id,
            title: String::new(),
            content: decoded.content,
            embedding: None,
            metadata: BTreeMap::new(),
        })?;
    }

    let report = writer.finish()?;
    let manifest_path = root.join(LEXICAL_MANIFEST_FILE);
    let manifest: Value = serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
    let body = value(&manifest, "body")?;
    let artifact_file = text(body, "artifact_file")?;
    if Path::new(artifact_file)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(artifact_file)
    {
        return Err("invalid lexical artifact filename".into());
    }
    if fs::metadata(root.join(artifact_file))?.len() != number(body, "artifact_len")? {
        return Err("actual lexical artifact size differs from its manifest".into());
    }
    let metrics = artifact_metrics(body)?;
    let reader = SearchOutOfCoreReader::open_with_term_policy(
        root,
        SearchOutOfCoreConfig {
            max_lexical_manifest_bytes: manifest_limit,
            ..Default::default()
        },
        Default::default(),
        term_policy,
    )?;
    if reader.document_count() != expected || number(body, "document_count")? != expected as u64 {
        return Err("reopened document count differs from the complete source".into());
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "protocol": "hawdb-search-lexical-corpus-qualification-v1",
            "source": source_stats,
            "term_policy_max_bytes": term_policy.max_term_bytes().get(),
            "manifest_budget_bytes": manifest_limit.get(),
            "build_options_debug": options_debug,
            "build_report": report,
            "actual_lexical_artifact": metrics,
            "lexical_manifest_bytes": fs::metadata(manifest_path)?.len(),
            "reopened_document_count": reader.document_count(),
            "build_elapsed_ms": started.elapsed().as_millis(),
            "scope": "physical lexical artifact evidence and reopen only; no latency or process-memory qualification",
        }))?
    );
    Ok(())
}

fn equal_path(left: &Value, right: &Value, path: &[&str]) -> Result<()> {
    let mut left_value = left;
    let mut right_value = right;
    for segment in path {
        left_value = value(left_value, segment)?;
        right_value = value(right_value, segment)?;
    }
    if left_value != right_value {
        return Err(format!("qualification reports differ at {}", path.join(".")).into());
    }
    Ok(())
}

fn compare_reports(legacy: &Value, compact: &Value) -> Result<Value> {
    for path in [
        ["source", "document_count"].as_slice(),
        ["source", "source_jsonl_bytes"].as_slice(),
        ["source", "content_bytes"].as_slice(),
        ["source", "max_content_bytes"].as_slice(),
        ["source", "id_bytes"].as_slice(),
        ["source", "mapping"].as_slice(),
        ["source", "input_order"].as_slice(),
        ["term_policy_max_bytes"].as_slice(),
        ["actual_lexical_artifact", "document_count"].as_slice(),
        ["actual_lexical_artifact", "posting_count"].as_slice(),
        ["actual_lexical_artifact", "total_document_len"].as_slice(),
        ["actual_lexical_artifact", "analyzer_digest"].as_slice(),
        ["actual_lexical_artifact", "documents_digest"].as_slice(),
    ] {
        equal_path(legacy, compact, path)?;
    }

    let legacy_artifact = value(legacy, "actual_lexical_artifact")?;
    let compact_artifact = value(compact, "actual_lexical_artifact")?;
    if text(legacy_artifact, "layout")? == "HAWDB_LEXICAL_ORDINAL_FST_V1" {
        return Err("legacy report unexpectedly declares the compact layout".into());
    }
    if text(compact_artifact, "layout")? != "HAWDB_LEXICAL_ORDINAL_FST_V1" {
        return Err("compact report does not declare the ordinal FST layout".into());
    }
    let legacy_posting_bytes = number(legacy_artifact, "actual_posting_bytes")?;
    let compact_posting_bytes = number(compact_artifact, "actual_posting_bytes")?;
    if legacy_posting_bytes == 0 || compact_posting_bytes == 0 {
        return Err("qualification reports contain zero posting bytes".into());
    }
    let order_of_magnitude = legacy_posting_bytes >= compact_posting_bytes.saturating_mul(10);

    Ok(json!({
        "protocol": "hawdb-search-lexical-corpus-comparison-v1",
        "legacy_posting_bytes": legacy_posting_bytes,
        "compact_posting_bytes": compact_posting_bytes,
        "posting_byte_ratio": legacy_posting_bytes as f64 / compact_posting_bytes as f64,
        "passes_order_of_magnitude_gate": order_of_magnitude,
        "scope": "physical posting-byte comparison only; query, update/delete, recovery, resource, and full lifecycle qualification remain separate gates",
    }))
}

fn compare(args: &[OsString]) -> Result<()> {
    if args.len() != 4 {
        return Err(
            "usage: search_lexical_corpus_qualification compare LEGACY_REPORT COMPACT_REPORT"
                .into(),
        );
    }
    let legacy: Value = serde_json::from_reader(BufReader::new(File::open(&args[2])?))?;
    let compact: Value = serde_json::from_reader(BufReader::new(File::open(&args[3])?))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&compare_reports(&legacy, &compact)?)?
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    match args.get(1).and_then(|argument| argument.to_str()) {
        Some("build") => build(&args),
        Some("compare") => compare(&args),
        _ => Err("usage: search_lexical_corpus_qualification {build|compare} ...".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Value {
        json!({
            "document_count": 2,
            "source_jsonl_bytes": 40,
            "content_bytes": 20,
            "max_content_bytes": 12,
            "id_bytes": 4,
            "mapping": "fixed",
            "input_order": "fixed",
        })
    }

    fn artifact(layout: &str, posting_bytes: u64) -> Value {
        json!({
            "layout": layout,
            "actual_posting_bytes": posting_bytes,
            "document_count": 2,
            "posting_count": 4,
            "total_document_len": 8,
            "analyzer_digest": 10,
            "documents_digest": 11,
        })
    }

    fn report(layout: &str, posting_bytes: u64) -> Value {
        json!({
            "source": source(),
            "term_policy_max_bytes": 1024,
            "actual_lexical_artifact": artifact(layout, posting_bytes),
        })
    }

    #[test]
    fn compact_artifact_metrics_reconcile_manifest_counters() {
        let body = json!({
            "layout": "HAWDB_LEXICAL_ORDINAL_FST_V1",
            "artifact_len": 100,
            "posting_bytes": 60,
            "legacy_posting_bytes": 600,
            "posting_count": 4,
            "document_count": 2,
            "total_document_len": 8,
            "analyzer_digest": 10,
            "documents_digest": 11,
            "max_term_bytes": 5,
            "blocks": [
                {"kind":"Documents", "offset":10, "length":30},
                {"kind":"Postings", "offset":40, "length":60}
            ],
        });
        let metrics = artifact_metrics(&body).unwrap();
        assert_eq!(metrics["actual_posting_bytes"], 60);
        assert_eq!(metrics["document_mapping_bytes"], 30);
        assert_eq!(metrics["legacy_posting_bytes"], 600);
    }

    #[test]
    fn comparison_requires_identical_corpus_and_logical_identity() {
        let legacy = report("legacy-string-postings-v1", 1_000);
        let compact = report("HAWDB_LEXICAL_ORDINAL_FST_V1", 100);
        assert_eq!(
            compare_reports(&legacy, &compact).unwrap()["passes_order_of_magnitude_gate"],
            true
        );

        let mut different = compact;
        different["actual_lexical_artifact"]["documents_digest"] = json!(12);
        assert!(compare_reports(&legacy, &different).is_err());
    }
}
