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

use super::evidence::execute_qualified_read;
use super::fixture::{
    corpus_statement, required_i64, source_chunk_parameters, source_chunk_parameters_with_index,
    source_chunks_by_source_parameters, source_document_parameters, source_graph_parameters,
    source_id_parameters, SOURCE_DOCUMENT_ID,
};
use super::{
    ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport,
    ContentStoreSourceReplacementPhaseReport, ContentStoreSourceReplacementQualificationReport,
};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::path::Path;

const SHORT_REPLACEMENT_CHUNK_COUNT: usize = 2;
const SUMMARY_VERIFY_SQL: &str =
    "SELECT space_id, item_count, size_bytes FROM content_documents WHERE content_doc_id = $1";
const SUMMARY_MAX_ROWS: usize = 1;
const SUMMARY_MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const SOURCE_CHUNK_INTEGRITY_SQL: &str = "SELECT chunk_id, chunk_index, char_start, char_end, token_count, metadata_json, content_hash FROM content_chunks WHERE content_doc_id = $1 ORDER BY chunk_index ASC, chunk_id ASC LIMIT $2";
const SOURCE_CHUNK_INTEGRITY_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

pub(super) fn qualify_source_chunk_replacement(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
    initial_chunk_count: usize,
    chunk_payload_bytes: usize,
) -> Result<(Database, ContentStoreSourceReplacementQualificationReport)> {
    if initial_chunk_count <= SHORT_REPLACEMENT_CHUNK_COUNT {
        return Err(HawDBError::Semantic(format!(
            "content-store source replacement needs more than {SHORT_REPLACEMENT_CHUNK_COUNT} initial chunks, got {initial_chunk_count}"
        )));
    }

    require_source_state(&mut database, corpus, initial_chunk_count, "default")?;
    let shorter = replace_checkpoint_and_reopen(
        database,
        ReplacementInputs {
            database_path,
            database_config,
            corpus,
            replacement_chunk_count: SHORT_REPLACEMENT_CHUNK_COUNT,
            chunk_payload_bytes,
            phase: "replacement",
            probe_duplicate_order: true,
        },
    )?;
    database = shorter.database;
    require_exact_replacement_rows(
        &mut database,
        corpus,
        SHORT_REPLACEMENT_CHUNK_COUNT,
        chunk_payload_bytes,
        "replacement",
    )?;

    let empty = replace_checkpoint_and_reopen(
        database,
        ReplacementInputs {
            database_path,
            database_config,
            corpus,
            replacement_chunk_count: 0,
            chunk_payload_bytes,
            phase: "empty",
            probe_duplicate_order: false,
        },
    )?;
    database = empty.database;
    require_exact_replacement_rows(&mut database, corpus, 0, chunk_payload_bytes, "empty")?;
    if empty.report.committed_epoch <= shorter.report.committed_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store empty replacement epoch {} did not advance beyond shorter replacement epoch {}",
            empty.report.committed_epoch, shorter.report.committed_epoch
        )));
    }
    if empty.report.checkpoint_generation <= shorter.report.checkpoint_generation {
        return Err(HawDBError::Execution(format!(
            "content-store empty replacement checkpoint generation {} did not advance beyond shorter replacement generation {}",
            empty.report.checkpoint_generation, shorter.report.checkpoint_generation
        )));
    }

    Ok((
        database,
        ContentStoreSourceReplacementQualificationReport {
            initial_chunk_count,
            stale_suffix_removed: true,
            chunk_fields_preserved: true,
            duplicate_order_rejected: shorter.duplicate_order_rejected,
            rejected_statement_atomic: shorter.rejected_statement_atomic,
            shorter_replacement: shorter.report,
            empty_replacement: empty.report,
            final_chunk_count: 0,
        },
    ))
}

pub(super) fn seed_source_chunks_for_followup(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    chunk_count: usize,
    chunk_payload_bytes: usize,
) -> Result<u64> {
    Ok(replace_source_chunks(
        database,
        corpus,
        chunk_count,
        chunk_payload_bytes,
        "ownership-seed",
        false,
    )?
    .committed_epoch)
}

struct ReplacementOutcome {
    database: Database,
    report: ContentStoreSourceReplacementPhaseReport,
    duplicate_order_rejected: bool,
    rejected_statement_atomic: bool,
}

struct ReplacementInputs<'a> {
    database_path: &'a Path,
    database_config: &'a DatabaseConfig,
    corpus: &'a ContentStoreSqlCorpus,
    replacement_chunk_count: usize,
    chunk_payload_bytes: usize,
    phase: &'a str,
    probe_duplicate_order: bool,
}

fn replace_checkpoint_and_reopen(
    mut database: Database,
    inputs: ReplacementInputs<'_>,
) -> Result<ReplacementOutcome> {
    let ReplacementInputs {
        database_path,
        database_config,
        corpus,
        replacement_chunk_count,
        chunk_payload_bytes,
        phase,
        probe_duplicate_order,
    } = inputs;
    let mutation = replace_source_chunks(
        &mut database,
        corpus,
        replacement_chunk_count,
        chunk_payload_bytes,
        phase,
        probe_duplicate_order,
    )?;
    let live_read = read_source_chunks(
        &mut database,
        corpus,
        replacement_chunk_count,
        ContentStoreRowPageReadPhase::LiveOverlay,
    )?;
    if live_read.execution.visible_commit_epoch != mutation.committed_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store {phase} replacement read observed epoch {}, expected {}",
            live_read.execution.visible_commit_epoch, mutation.committed_epoch
        )));
    }
    if replacement_chunk_count == 0 {
        // A covering index can eliminate every deleted key without fetching a
        // row. Probe a previously checkpointed key to verify the row tombstone.
        require_empty_replacement_tombstone(&database, mutation.committed_epoch)?;
    } else if live_read.execution.overlay_entries == 0 {
        return Err(HawDBError::Execution(format!(
            "content-store {phase} replacement did not use the live row overlay"
        )));
    }
    require_source_state(&mut database, corpus, replacement_chunk_count, "default")?;

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(format!(
                "content-store {phase} replacement checkpoint did not publish relational indexes"
            ))
        })?
        .generation;
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    let reopened_read = read_source_chunks(
        &mut database,
        corpus,
        replacement_chunk_count,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
    )?;
    if reopened_read.execution.visible_commit_epoch != mutation.committed_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store reopened {phase} replacement observed epoch {}, expected {}",
            reopened_read.execution.visible_commit_epoch, mutation.committed_epoch
        )));
    }
    if reopened_read.output_sha256 != live_read.output_sha256 {
        return Err(HawDBError::Execution(format!(
            "content-store {phase} replacement changed across checkpoint/reopen"
        )));
    }
    require_source_state(&mut database, corpus, replacement_chunk_count, "default")?;

    Ok(ReplacementOutcome {
        database,
        report: ContentStoreSourceReplacementPhaseReport {
            replacement_chunk_count,
            committed_epoch: mutation.committed_epoch,
            summary_item_count: mutation.summary_item_count,
            summary_size_bytes: mutation.summary_size_bytes,
            live_read,
            checkpoint_generation,
            reopened_read,
        },
        duplicate_order_rejected: mutation.duplicate_order_rejected,
        rejected_statement_atomic: mutation.rejected_statement_atomic,
    })
}

fn require_empty_replacement_tombstone(database: &Database, committed_epoch: u64) -> Result<()> {
    let transaction = database.begin_read_transaction();
    let profiled = transaction.query_sql_with_params_options_profiled(
        "SELECT chunk_id, text FROM content_chunks WHERE chunk_id = $1",
        &[Value::String("chunk-00000000".to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(SOURCE_CHUNK_INTEGRITY_MAX_PAYLOAD_BYTES),
        },
    )?;
    if !profiled.output.rows.is_empty()
        || profiled.profile.row_read.overlay_entries == 0
        || profiled.profile.row_read.visible_commit_epoch != Some(committed_epoch)
    {
        return Err(HawDBError::Execution(
            "content-store empty replacement did not observe the committed row tombstone"
                .to_string(),
        ));
    }
    Ok(())
}

struct ReplacementMutation {
    committed_epoch: u64,
    summary_item_count: i64,
    summary_size_bytes: i64,
    duplicate_order_rejected: bool,
    rejected_statement_atomic: bool,
}

fn replace_source_chunks(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    replacement_chunk_count: usize,
    chunk_payload_bytes: usize,
    phase: &str,
    probe_duplicate_order: bool,
) -> Result<ReplacementMutation> {
    let document = corpus_statement(corpus, "upsert_content_document")?;
    let delete_chunks = corpus_statement(corpus, "delete_source_chunks")?;
    let insert_chunk = corpus_statement(corpus, "insert_source_chunk")?;
    let payload_summary = corpus_statement(corpus, "source_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let page = corpus_statement(corpus, "source_chunks_by_source")?;

    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) SET s.chunk_count = $chunk_count, s.space_id = $space_id",
        &source_graph_parameters(replacement_chunk_count),
    )?;
    transaction.query_sql_with_params(
        &document.sql,
        &source_document_parameters("default", replacement_timestamp(phase)),
    )?;
    transaction.query_sql_with_params(
        &delete_chunks.sql,
        &[Value::String(SOURCE_DOCUMENT_ID.to_string())],
    )?;
    for position in 0..replacement_chunk_count {
        transaction.query_sql_with_params(
            &insert_chunk.sql,
            &source_chunk_parameters(position, chunk_payload_bytes, phase),
        )?;
    }

    let page_parameters = source_chunks_by_source_parameters(replacement_chunk_count.max(1));
    let before_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    require_row_count(&before_rejection, replacement_chunk_count, phase)?;
    let before_sha256 = rows_sha256(&before_rejection.rows);
    let (duplicate_order_rejected, rejected_statement_atomic) = if probe_duplicate_order {
        let duplicate = source_chunk_parameters_with_index(
            replacement_chunk_count.saturating_add(100),
            0,
            chunk_payload_bytes,
            "duplicate",
        );
        let error = transaction
            .query_sql_with_params(&insert_chunk.sql, &duplicate)
            .expect_err("duplicate source chunk order must be rejected");
        if !error.to_string().to_ascii_lowercase().contains("unique") {
            return Err(HawDBError::Execution(format!(
                "content-store source replacement expected a unique-key rejection, got: {error}"
            )));
        }
        let after_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
        require_row_count(&after_rejection, replacement_chunk_count, phase)?;
        if rows_sha256(&after_rejection.rows) != before_sha256 {
            return Err(HawDBError::Execution(
                "content-store rejected duplicate chunk changed the transaction workspace"
                    .to_string(),
            ));
        }
        (true, true)
    } else {
        (false, false)
    };

    let summary = transaction.query_sql_with_params(
        &payload_summary.sql,
        &[Value::String(SOURCE_DOCUMENT_ID.to_string())],
    )?;
    let summary_item_count = required_i64(&summary, "item_count")?;
    let summary_size_bytes = required_i64(&summary, "size_bytes")?;
    let expected_item_count = i64::try_from(replacement_chunk_count).map_err(|_| {
        HawDBError::Semantic("content-store source chunk count does not fit BIGINT".to_string())
    })?;
    if summary_item_count != expected_item_count {
        return Err(HawDBError::Execution(format!(
            "content-store {phase} replacement summary counted {summary_item_count} chunks, expected {replacement_chunk_count}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(summary_item_count),
            Value::Int(summary_size_bytes),
            Value::String(replacement_timestamp(phase).to_string()),
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
        ],
    )?;
    let graph = transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.chunk_count AS chunk_count, s.space_id AS space_id",
        &source_id_parameters(),
    )?;
    require_graph_state(&graph, replacement_chunk_count, "default", phase)?;
    transaction.commit()?;

    Ok(ReplacementMutation {
        committed_epoch: database.commit_epoch(),
        summary_item_count,
        summary_size_bytes,
        duplicate_order_rejected,
        rejected_statement_atomic,
    })
}

fn read_source_chunks(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_rows: usize,
    phase: ContentStoreRowPageReadPhase,
) -> Result<ContentStoreRowPageReadReport> {
    execute_qualified_read(
        database,
        corpus_statement(corpus, "source_chunks_by_source")?,
        source_chunks_by_source_parameters(expected_rows.max(1)),
        phase,
        expected_rows,
    )
}

fn require_source_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_chunk_count: usize,
    expected_space_id: &str,
) -> Result<()> {
    let graph = database.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.chunk_count AS chunk_count, s.space_id AS space_id",
        &source_id_parameters(),
    )?;
    require_graph_state(&graph, expected_chunk_count, expected_space_id, "persisted")?;

    let summary = database.query_sql_with_params_options(
        SUMMARY_VERIFY_SQL,
        &[Value::String(SOURCE_DOCUMENT_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(SUMMARY_MAX_ROWS),
            max_payload_bytes: Some(SUMMARY_MAX_PAYLOAD_BYTES),
        },
    )?;
    let item_count = required_i64(&summary, "item_count")?;
    let expected_item_count = i64::try_from(expected_chunk_count).map_err(|_| {
        HawDBError::Semantic("content-store source chunk count does not fit BIGINT".to_string())
    })?;
    if item_count != expected_item_count {
        return Err(HawDBError::Execution(format!(
            "content-store source document summary counted {item_count} chunks, expected {expected_chunk_count}"
        )));
    }
    if !matches!(
        summary.rows[0].get("space_id"),
        Some(Value::String(space_id)) if space_id == expected_space_id
    ) {
        return Err(HawDBError::Execution(format!(
            "content-store source document expected space {expected_space_id}, got {:?}",
            summary.rows[0].get("space_id")
        )));
    }
    let page = corpus_statement(corpus, "source_chunks_by_source")?;
    let chunks = database.query_sql_with_params_options(
        &page.sql,
        &source_chunks_by_source_parameters(expected_chunk_count.max(1)),
        QueryStreamOptions {
            max_rows: Some(expected_chunk_count.max(1)),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_row_count(&chunks, expected_chunk_count, "persisted")
}

fn require_exact_replacement_rows(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_chunk_count: usize,
    chunk_payload_bytes: usize,
    phase: &str,
) -> Result<()> {
    let page = corpus_statement(corpus, "source_chunks_by_source")?;
    let rows = database.query_sql_with_params_options(
        &page.sql,
        &source_chunks_by_source_parameters(expected_chunk_count.max(1)),
        QueryStreamOptions {
            max_rows: Some(expected_chunk_count.max(1)),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_row_count(&rows, expected_chunk_count, phase)?;
    let expected_text_prefix = format!("{phase}:");
    for (position, row) in rows.rows.iter().enumerate() {
        let expected_chunk_id = format!("chunk-{position:08}");
        if !matches!(
            row.get("chunk_id"),
            Some(Value::String(chunk_id)) if chunk_id == &expected_chunk_id
        ) {
            return Err(HawDBError::Execution(format!(
                "content-store {phase} replacement returned a stale or unordered chunk instead of {expected_chunk_id}: {row:?}"
            )));
        }
        match row.get("text") {
            Some(Value::String(text)) if text.starts_with(&expected_text_prefix) => {}
            other => {
                return Err(HawDBError::Execution(format!(
                    "content-store {phase} replacement returned unexpected chunk text {other:?}"
                )));
            }
        }
    }
    let integrity = database.query_sql_with_params_options(
        SOURCE_CHUNK_INTEGRITY_SQL,
        &[
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
            Value::Int(i64::try_from(expected_chunk_count.max(1)).unwrap_or(i64::MAX)),
        ],
        QueryStreamOptions {
            max_rows: Some(expected_chunk_count.max(1)),
            max_payload_bytes: Some(SOURCE_CHUNK_INTEGRITY_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_row_count(&integrity, expected_chunk_count, phase)?;
    for (position, row) in integrity.rows.iter().enumerate() {
        let expected_chunk_id = format!("chunk-{position:08}");
        let expected_metadata =
            format!("{{\"heading_context\": \"§ Heading {position}\", \"phase\": \"{phase}\"}}");
        let expected_hash = format!("chunk-hash-{phase}-{position:08}");
        let expected_start =
            i64::try_from(position.saturating_mul(chunk_payload_bytes)).unwrap_or(i64::MAX);
        let expected_end =
            i64::try_from((position + 1).saturating_mul(chunk_payload_bytes)).unwrap_or(i64::MAX);
        let expected_token_count = i64::try_from(position + 1).unwrap_or(i64::MAX);
        let exact = matches!(
            (
                row.get("chunk_id"),
                row.get("chunk_index"),
                row.get("char_start"),
                row.get("char_end"),
                row.get("token_count"),
                row.get("metadata_json"),
                row.get("content_hash"),
            ),
            (
                Some(Value::String(chunk_id)),
                Some(Value::Int(chunk_index)),
                Some(Value::Int(char_start)),
                Some(Value::Int(char_end)),
                Some(Value::Int(token_count)),
                Some(Value::String(metadata)),
                Some(Value::String(content_hash)),
            ) if chunk_id == &expected_chunk_id
                && *chunk_index == position as i64
                && *char_start == expected_start
                && *char_end == expected_end
                && *token_count == expected_token_count
                && metadata == &expected_metadata
                && content_hash == &expected_hash
        );
        if !exact {
            return Err(HawDBError::Execution(format!(
                "content-store {phase} replacement did not preserve chunk identity, offsets, metadata, and hash at position {position}: {row:?}"
            )));
        }
    }
    Ok(())
}

fn require_row_count(output: &QueryOutput, expected: usize, phase: &str) -> Result<()> {
    if output.rows.len() != expected {
        return Err(HawDBError::Execution(format!(
            "content-store {phase} replacement returned {} chunks, expected {expected}",
            output.rows.len()
        )));
    }
    Ok(())
}

fn require_graph_state(
    output: &QueryOutput,
    expected_chunk_count: usize,
    expected_space_id: &str,
    phase: &str,
) -> Result<()> {
    let expected_chunk_count = i64::try_from(expected_chunk_count).map_err(|_| {
        HawDBError::Semantic("content-store source chunk count does not fit BIGINT".to_string())
    })?;
    match output.rows.as_slice() {
        [row]
            if row.get("chunk_count") == Some(&Value::Int(expected_chunk_count))
                && matches!(row.get("space_id"), Some(Value::String(space_id)) if space_id == expected_space_id) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store {phase} replacement graph state expected chunks={expected_chunk_count}, space={expected_space_id}, got {rows:?}"
        ))),
    }
}

fn replacement_timestamp(phase: &str) -> &'static str {
    match phase {
        "replacement" => "2026-01-01T00:02:00Z",
        "empty" => "2026-01-01T00:03:00Z",
        _ => "2026-01-01T00:04:00Z",
    }
}
