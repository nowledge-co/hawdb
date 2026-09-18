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

use super::ContentStoreInitialRowPageQualificationConfig;
use crate::{
    nowledge_content_store_schema_statements, ContentStoreSqlCorpus, ContentStoreSqlStatementSpec,
};
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, RelationalIndexMode,
    Result, StorageResidencyMode, Value,
};
use std::collections::BTreeMap;

pub(super) const QUALIFIED_TABLES: [&str; 4] = [
    "content_documents",
    "thread_messages",
    "content_chunks",
    "content_anchors",
];
pub(super) const THREAD_DOCUMENT_ID: &str = "content-doc-thread-1";
pub(super) const THREAD_OWNER_ID: &str = "thread-1";
pub(super) const THREAD_STORAGE_ID: &str = "thread-storage-1";
pub(super) const SOURCE_DOCUMENT_ID: &str = "content-doc-source-1";
pub(super) const SOURCE_OWNER_ID: &str = "source-1";
pub(super) const MEMORY_OWNER_ID: &str = "memory-1";

#[derive(Debug, Clone, Copy)]
pub(super) struct CheckpointIdentity {
    pub(super) generation: u64,
    pub(super) commit_epoch: u64,
}

pub(super) fn bootstrap_checkpoint(
    config: &ContentStoreInitialRowPageQualificationConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<CheckpointIdentity> {
    let mut database = Database::open_with_durability_and_config(
        &config.database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config(config, RelationalIndexMode::Shadow),
    )?;
    let mut transaction = database.begin_transaction();
    for statement in nowledge_content_store_schema_statements()? {
        transaction.query_sql(statement)?;
    }
    transaction.commit()?;

    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let chunk = corpus_statement(corpus, "insert_source_chunk")?;
    let anchor = corpus_statement(corpus, "upsert_memory_message_anchor")?;
    let source_summary = corpus_statement(corpus, "source_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "CREATE (:Thread {id: $thread_id, space_id: $space_id})",
        &graph_identity_parameters("thread_id", THREAD_OWNER_ID),
    )?;
    transaction.query_with_params(
        "CREATE (:Source {id: $source_id, space_id: $space_id, chunk_count: $chunk_count})",
        &source_graph_parameters(config.base_chunk_count),
    )?;
    transaction.query_with_params(
        "CREATE (:Memory {id: $memory_id, space_id: $space_id})",
        &graph_identity_parameters("memory_id", MEMORY_OWNER_ID),
    )?;
    transaction.query_sql_with_params(&document.sql, &thread_document_parameters())?;
    transaction.query_sql_with_params(
        &document.sql,
        &source_document_parameters("default", "2026-01-01T00:00:00Z"),
    )?;
    for position in 0..config.base_message_count {
        transaction.query_sql_with_params(
            &message.sql,
            &thread_message_parameters(position, config.message_payload_bytes, "base"),
        )?;
        transaction
            .query_sql_with_params(&anchor.sql, &memory_anchor_parameters(position, "base"))?;
    }
    for position in 0..config.base_chunk_count {
        transaction.query_sql_with_params(
            &chunk.sql,
            &source_chunk_parameters(position, config.chunk_payload_bytes, "base"),
        )?;
    }
    let summary = transaction.query_sql_with_params(
        &source_summary.sql,
        &[Value::String(SOURCE_DOCUMENT_ID.to_string())],
    )?;
    let item_count = required_i64(&summary, "item_count")?;
    let size_bytes = required_i64(&summary, "size_bytes")?;
    if item_count != i64::try_from(config.base_chunk_count).unwrap_or(i64::MAX) {
        return Err(HawDBError::Execution(format!(
            "content-store base source summary counted {item_count} chunks, expected {}",
            config.base_chunk_count
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
            Value::String("2026-01-01T00:00:00Z".to_string()),
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
        ],
    )?;
    transaction.commit()?;
    database.checkpoint()?;
    let report = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store row-page qualification checkpoint did not publish relational indexes"
                    .to_string(),
            )
        })?;
    Ok(CheckpointIdentity {
        generation: report.generation,
        commit_epoch: report.source_commit_epoch,
    })
}

pub(super) fn database_config(
    config: &ContentStoreInitialRowPageQualificationConfig,
    relational_index_mode: RelationalIndexMode,
) -> DatabaseConfig {
    DatabaseConfig {
        max_read_result_rows: Some(100_000),
        max_read_result_payload_bytes: Some(128 * 1024 * 1024),
        segment_cache_capacity_bytes: config.segment_cache_capacity_bytes,
        storage_residency_mode: StorageResidencyMode::OutOfCore,
        relational_index_mode,
        ..DatabaseConfig::default()
    }
}

pub(super) fn initial_read_specs(
    corpus: &ContentStoreSqlCorpus,
    message_count: usize,
    chunk_count: usize,
) -> Result<Vec<(&ContentStoreSqlStatementSpec, Vec<Value>, usize)>> {
    Ok(vec![
        (
            corpus_statement(corpus, "thread_owned_document_ids")?,
            vec![Value::String(THREAD_OWNER_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "thread_messages_page")?,
            thread_page_parameters(message_count),
            message_count,
        ),
        (
            corpus_statement(corpus, "thread_message_summary")?,
            vec![Value::String(THREAD_STORAGE_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "source_chunks_page")?,
            source_chunks_page_parameters(chunk_count),
            chunk_count,
        ),
        (
            corpus_statement(corpus, "source_chunks_by_source")?,
            source_chunks_by_source_parameters(chunk_count),
            chunk_count,
        ),
        (
            corpus_statement(corpus, "source_chunk_count_by_source")?,
            vec![Value::String(SOURCE_OWNER_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "source_document_payload_summary")?,
            vec![Value::String(SOURCE_DOCUMENT_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "thread_covered_message_count")?,
            vec![Value::String(THREAD_STORAGE_ID.to_string())],
            1,
        ),
        (
            corpus_statement(corpus, "content_status_anchor_count")?,
            Vec::new(),
            1,
        ),
    ])
}

pub(super) fn corpus_statement<'a>(
    corpus: &'a ContentStoreSqlCorpus,
    name: &str,
) -> Result<&'a ContentStoreSqlStatementSpec> {
    corpus.statement(name).ok_or_else(|| {
        HawDBError::Semantic(format!(
            "content-store row-page qualification requires statement {name}"
        ))
    })
}

fn thread_document_parameters() -> Vec<Value> {
    vec![
        Value::String(THREAD_DOCUMENT_ID.to_string()),
        Value::String("thread".to_string()),
        Value::String(THREAD_OWNER_ID.to_string()),
        Value::String("default".to_string()),
        Value::String("application/x-nowledge-thread".to_string()),
        Value::Int(1),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

pub(super) fn source_document_parameters(space_id: &str, updated_at: &str) -> Vec<Value> {
    vec![
        Value::String(SOURCE_DOCUMENT_ID.to_string()),
        Value::String("source".to_string()),
        Value::String(SOURCE_OWNER_ID.to_string()),
        Value::String(space_id.to_string()),
        Value::String("application/x-nowledge-source-chunks".to_string()),
        Value::Int(1),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String(updated_at.to_string()),
    ]
}

pub(super) fn thread_message_parameters(
    position: usize,
    payload_bytes: usize,
    phase: &str,
) -> Vec<Value> {
    let content_message_id = format!("content-message-{position:08}");
    vec![
        Value::String(content_message_id.clone()),
        Value::String(format!("message-{position:08}")),
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::String(THREAD_OWNER_ID.to_string()),
        Value::String(THREAD_DOCUMENT_ID.to_string()),
        Value::String("default".to_string()),
        Value::Int(position as i64),
        Value::String(
            if position.is_multiple_of(2) {
                "user"
            } else {
                "assistant"
            }
            .to_string(),
        ),
        Value::String(format!("{phase}:{}", "x".repeat(payload_bytes))),
        Value::String(format!("2026-01-01T00:00:{:02}Z", position % 60)),
        Value::Int((position + 1) as i64),
        Value::String(format!("{{\"phase\":\"{phase}\"}}")),
        Value::String(format!("external-{position:08}")),
        Value::Bool(false),
        Value::String(format!("hash-{phase}-{position:08}")),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

pub(super) fn thread_page_parameters(limit: usize) -> Vec<Value> {
    vec![
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)),
        Value::Int(0),
    ]
}

pub(super) fn source_chunk_parameters(
    position: usize,
    payload_bytes: usize,
    phase: &str,
) -> Vec<Value> {
    source_chunk_parameters_with_index(position, position, payload_bytes, phase)
}

pub(super) fn source_chunk_parameters_with_index(
    identity_position: usize,
    chunk_index: usize,
    payload_bytes: usize,
    phase: &str,
) -> Vec<Value> {
    vec![
        Value::String(format!("chunk-{identity_position:08}")),
        Value::String(SOURCE_DOCUMENT_ID.to_string()),
        Value::Int(chunk_index as i64),
        Value::String(format!("{phase}:{}", "c".repeat(payload_bytes))),
        Value::Int((chunk_index * payload_bytes) as i64),
        Value::Int(((chunk_index + 1) * payload_bytes) as i64),
        Value::Int((chunk_index + 1) as i64),
        Value::String(format!(
            "{{\"heading_context\": \"§ Heading {chunk_index}\", \"phase\": \"{phase}\"}}"
        )),
        Value::String(format!("chunk-hash-{phase}-{identity_position:08}")),
        Value::String("2026-01-01T00:00:00Z".to_string()),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

pub(super) fn memory_anchor_parameters(position: usize, phase: &str) -> Vec<Value> {
    memory_anchor_parameters_with_message_id(position, &format!("message-{position:08}"), phase)
}

pub(super) fn memory_anchor_parameters_with_message_id(
    position: usize,
    message_id: &str,
    phase: &str,
) -> Vec<Value> {
    vec![
        Value::String(format!("anchor-{position:08}")),
        Value::String(MEMORY_OWNER_ID.to_string()),
        Value::String(THREAD_DOCUMENT_ID.to_string()),
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::String(format!("content-message-{position:08}")),
        Value::String(message_id.to_string()),
        Value::Int(position as i64),
        Value::String(format!("hash-{phase}-{position:08}")),
        Value::String(format!(
            "{{\"content_message_id\":\"content-message-{position:08}\",\"message_id\":\"{message_id}\",\"order_index\":{position},\"thread_storage_id\":\"{THREAD_STORAGE_ID}\"}}"
        )),
        Value::String("2026-01-01T00:00:00Z".to_string()),
    ]
}

pub(super) fn source_chunks_page_parameters(limit: usize) -> Vec<Value> {
    vec![
        Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)),
        Value::Int(0),
    ]
}

pub(super) fn source_chunks_by_source_parameters(limit: usize) -> Vec<Value> {
    vec![
        Value::String(SOURCE_OWNER_ID.to_string()),
        Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)),
    ]
}

pub(super) fn source_graph_parameters(chunk_count: usize) -> BTreeMap<String, Value> {
    let mut parameters = source_id_parameters();
    parameters.insert("space_id".to_string(), Value::String("default".to_string()));
    parameters.insert(
        "chunk_count".to_string(),
        Value::Int(i64::try_from(chunk_count).unwrap_or(i64::MAX)),
    );
    parameters
}

pub(super) fn source_id_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "source_id".to_string(),
        Value::String(SOURCE_OWNER_ID.to_string()),
    )])
}

pub(super) fn source_space_parameters(space_id: &str) -> BTreeMap<String, Value> {
    let mut parameters = source_id_parameters();
    parameters.insert("space_id".to_string(), Value::String(space_id.to_string()));
    parameters
}

fn graph_identity_parameters(id_name: &str, id: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (id_name.to_string(), Value::String(id.to_string())),
        ("space_id".to_string(), Value::String("default".to_string())),
    ])
}

pub(super) fn required_i64(output: &QueryOutput, field: &str) -> Result<i64> {
    if output.rows.len() != 1 {
        return Err(HawDBError::Execution(format!(
            "content-store expected one row for {field}, got {}",
            output.rows.len()
        )));
    }
    match output.rows[0].get(field) {
        Some(Value::Int(value)) => Ok(*value),
        other => Err(HawDBError::Execution(format!(
            "content-store expected integer field {field}, got {other:?}"
        ))),
    }
}
