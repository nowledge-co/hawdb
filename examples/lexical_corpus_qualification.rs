//! Developer-only full-corpus artifact measurement through the embedded facade.
//! Never publish source records; report aggregate counts and physical extents.

use serde::Deserialize;
use serde_json::{json, Value};
use skein::{
    SearchDocument, SearchLexicalTermPolicy, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
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
    let posting_bytes;
    let document_bytes;
    let dictionary_bytes;
    let header_bytes;
    if body.get("byte_counters").is_some() {
        let counters = &body["byte_counters"];
        posting_bytes = number(counters, "posting_frame_bytes")?
            .checked_add(number(counters, "posting_skip_bytes")?)
            .ok_or("posting size overflow")?;
        if posting_bytes != number(body, "posting_bytes")? {
            return Err("compact posting extent mismatch".into());
        }
        document_bytes = number(counters, "document_mapping_bytes")?;
        dictionary_bytes = number(counters, "dictionary_bytes")?;
        header_bytes = number(counters, "header_bytes")?;
        if header_bytes + document_bytes + posting_bytes + dictionary_bytes != artifact_len {
            return Err("compact artifact category mismatch".into());
        }
    } else {
        let blocks = body["blocks"].as_array().ok_or("missing legacy blocks")?;
        header_bytes = blocks
            .first()
            .map(|block| number(block, "offset"))
            .transpose()?
            .ok_or("empty legacy artifact")?;
        let mut end = header_bytes;
        let mut postings = 0u64;
        let mut documents = 0u64;
        for block in blocks {
            if number(block, "offset")? != end {
                return Err("noncontiguous legacy artifact blocks".into());
            }
            let length = number(block, "length")?;
            end = end.checked_add(length).ok_or("legacy size overflow")?;
            match block["kind"].as_str() {
                Some("Documents") => documents += length,
                Some("Postings") => postings += length,
                _ => return Err("unknown legacy block kind".into()),
            }
        }
        if end != artifact_len {
            return Err("legacy artifact extent mismatch".into());
        }
        posting_bytes = postings;
        document_bytes = documents;
        // Legacy term statistics are JSON in the separately counted manifest.
        dictionary_bytes = 0;
    }
    Ok(json!({
        "artifact_bytes": artifact_len, "actual_posting_bytes": posting_bytes,
        "document_mapping_bytes": document_bytes, "dictionary_artifact_bytes": dictionary_bytes,
        "header_bytes": header_bytes, "posting_count": number(body, "posting_count")?,
        "document_count": number(body, "document_count")?,
        "total_document_len": number(body, "total_document_len")?,
        "analyzer_digest": number(body, "analyzer_digest")?,
        "documents_digest": number(body, "documents_digest")?,
        "layout": body.get("layout").cloned().unwrap_or(json!("legacy-string-postings-v1")),
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    if args.len() != 6 {
        return Err("usage: lexical_corpus_qualification SOURCE_JSONL NEW_OUTPUT_ROOT EXPECTED_DOCUMENTS MAX_TERM_BYTES MAX_MANIFEST_BYTES".into());
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
        "search_lexical.manifest.{}.skein",
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
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "source": source_stats, "build_options_debug": options_debug,
            "build_report": report, "actual_lexical_artifact": metrics,
            "lexical_manifest_bytes": fs::metadata(manifest_path)?.len(),
            "configured_reopen_document_count": reader.document_count(),
            "configured_term_limit_bytes": policy.max_term_bytes().get(),
            "configured_manifest_limit_bytes": manifest_limit.get(),
            "build_elapsed_ms": build_elapsed_ms, "total_elapsed_ms": started.elapsed().as_millis(),
            "scope": "actual artifact byte comparison; no latency or whole-process memory qualification",
        }))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy() -> Value {
        json!({"artifact_len": 90, "posting_count": 4, "document_count": 2,
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
    fn compact_includes_skips_but_not_dictionary_or_nominal_legacy_bytes() {
        let mut body = legacy();
        body["posting_bytes"] = json!(35);
        body["byte_counters"] = json!({"header_bytes":10, "document_mapping_bytes":20,
            "posting_frame_bytes":30, "posting_skip_bytes":5, "dictionary_bytes":25,
            "uncompressed_posting_payload_bytes":9999});
        let metrics = artifact_metrics(&body).unwrap();
        assert_eq!(metrics["actual_posting_bytes"], 35);
        assert_eq!(metrics["dictionary_artifact_bytes"], 25);
        body["posting_bytes"] = json!(30);
        assert!(artifact_metrics(&body).is_err());
    }
}
