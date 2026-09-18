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

use super::fixture::{
    corpus_statement, memory_anchor_parameters_with_message_id, source_chunk_parameters,
    source_chunks_by_source_parameters, source_graph_parameters, source_id_parameters,
    thread_message_parameters, SOURCE_DOCUMENT_ID, SOURCE_OWNER_ID, THREAD_STORAGE_ID,
};
use super::{ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport};
use crate::{ContentStoreSqlCorpus, ContentStoreSqlStatementSpec};
use hawdb::{Database, HawDBError, QueryStreamOptions, Result, Value};

type ExtendedReadSpec<'a> = (&'a ContentStoreSqlStatementSpec, Vec<Value>, usize);
const ANCHOR_OCCURRENCE_SQL: &str = "SELECT anchor_id, content_message_id, message_id FROM content_anchors WHERE owner_kind = $1 AND anchor_kind = $2 ORDER BY anchor_id ASC LIMIT $3";
const ANCHOR_OCCURRENCE_MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const RUNTIME_LEGACY_MESSAGE_ID: &str = "message-shared-runtime";

pub(super) fn append_runtime_content(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    message_position: usize,
    message_payload_bytes: usize,
    chunk_position: usize,
    chunk_payload_bytes: usize,
    phase: &str,
) -> Result<u64> {
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let chunk = corpus_statement(corpus, "insert_source_chunk")?;
    let anchor = corpus_statement(corpus, "upsert_memory_message_anchor")?;
    let source_summary = corpus_statement(corpus, "source_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let next_chunk_count = chunk_position.checked_add(1).ok_or_else(|| {
        HawDBError::Semantic("content-store chunk count overflowed usize".to_string())
    })?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) SET s.chunk_count = $chunk_count",
        &source_graph_parameters(next_chunk_count),
    )?;
    let mut message_parameters =
        thread_message_parameters(message_position, message_payload_bytes, phase);
    message_parameters[1] = Value::String(RUNTIME_LEGACY_MESSAGE_ID.to_string());
    transaction.query_sql_with_params(&message.sql, &message_parameters)?;
    transaction.query_sql_with_params(
        &chunk.sql,
        &source_chunk_parameters(chunk_position, chunk_payload_bytes, phase),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &memory_anchor_parameters_with_message_id(
            message_position,
            RUNTIME_LEGACY_MESSAGE_ID,
            phase,
        ),
    )?;
    let summary = transaction.query_sql_with_params(
        &source_summary.sql,
        &[Value::String(SOURCE_DOCUMENT_ID.to_string())],
    )?;
    let item_count = super::fixture::required_i64(&summary, "item_count")?;
    let size_bytes = super::fixture::required_i64(&summary, "size_bytes")?;
    if item_count != i64::try_from(next_chunk_count).unwrap_or(i64::MAX) {
        return Err(HawDBError::Execution(format!(
            "content-store runtime source summary counted {item_count} chunks, expected {next_chunk_count}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
            Value::String(format!("2026-01-01T00:00:{:02}Z", next_chunk_count % 60)),
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
        ],
    )?;
    transaction.commit()?;
    let committed_epoch = database.commit_epoch();
    require_source_graph_chunk_count(database, next_chunk_count)?;
    Ok(committed_epoch)
}

pub(super) fn runtime_extended_read_specs<'a>(
    corpus: &'a ContentStoreSqlCorpus,
    chunk_count: usize,
) -> Result<[ExtendedReadSpec<'a>; 2]> {
    Ok([
        (
            corpus_statement(corpus, "source_chunks_by_source")?,
            source_chunks_by_source_parameters(chunk_count),
            chunk_count,
        ),
        (
            corpus_statement(corpus, "thread_covered_message_count")?,
            vec![Value::String(THREAD_STORAGE_ID.to_string())],
            1,
        ),
    ])
}

pub(super) fn require_source_graph_chunk_count(
    database: &mut Database,
    expected: usize,
) -> Result<()> {
    let output = database.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.chunk_count AS chunk_count",
        &source_id_parameters(),
    )?;
    let expected = i64::try_from(expected).map_err(|_| {
        HawDBError::Semantic("content-store chunk count does not fit BIGINT".to_string())
    })?;
    match output.rows.as_slice() {
        [row] if row.get("chunk_count") == Some(&Value::Int(expected)) => Ok(()),
        rows => Err(HawDBError::Execution(format!(
            "content-store graph Source chunk count mismatch: expected {expected}, got {rows:?}"
        ))),
    }
}

pub(super) fn read_pair(
    database: &mut Database,
    specs: [ExtendedReadSpec<'_>; 2],
    phase: ContentStoreRowPageReadPhase,
) -> Result<(ContentStoreRowPageReadReport, ContentStoreRowPageReadReport)> {
    let mut reads = specs.into_iter().map(|(statement, parameters, expected)| {
        super::evidence::execute_qualified_read(database, statement, parameters, phase, expected)
    });
    let chunk = reads.next().transpose()?.ok_or_else(|| {
        HawDBError::Execution("content-store extended read set has no chunk read".to_string())
    })?;
    let anchor = reads.next().transpose()?.ok_or_else(|| {
        HawDBError::Execution("content-store extended read set has no anchor read".to_string())
    })?;
    Ok((chunk, anchor))
}

pub(super) fn require_runtime_counts(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_chunks: usize,
    expected_covered_messages: usize,
) -> Result<()> {
    require_count(
        database,
        corpus_statement(corpus, "source_chunk_count_by_source")?,
        &[Value::String(SOURCE_OWNER_ID.to_string())],
        "chunk_count",
        expected_chunks,
    )?;
    require_count(
        database,
        corpus_statement(corpus, "thread_covered_message_count")?,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
        "covered_messages",
        expected_covered_messages,
    )
}

fn require_count(
    database: &mut Database,
    statement: &ContentStoreSqlStatementSpec,
    parameters: &[Value],
    column: &str,
    expected: usize,
) -> Result<()> {
    let output = database.query_sql_with_params_options(
        &statement.sql,
        parameters,
        QueryStreamOptions {
            max_rows: Some(statement.max_rows),
            max_payload_bytes: Some(statement.max_payload_bytes),
        },
    )?;
    let expected = i64::try_from(expected)
        .map_err(|_| HawDBError::Semantic(format!("content-store {column} does not fit BIGINT")))?;
    match output.rows.as_slice() {
        [row] if row.get(column) == Some(&Value::Int(expected)) => Ok(()),
        rows => Err(HawDBError::Execution(format!(
            "content-store {column} mismatch: expected {expected}, got {rows:?}"
        ))),
    }
}

pub(super) fn require_anchor_occurrence_identity(
    database: &mut Database,
    expected: usize,
    expected_shared_legacy_occurrences: usize,
) -> Result<()> {
    let output = database.query_sql_with_params_options(
        ANCHOR_OCCURRENCE_SQL,
        &[
            Value::String("memory".to_string()),
            Value::String("message".to_string()),
            Value::Int(i64::try_from(expected).unwrap_or(i64::MAX)),
        ],
        QueryStreamOptions {
            max_rows: Some(expected),
            max_payload_bytes: Some(ANCHOR_OCCURRENCE_MAX_PAYLOAD_BYTES),
        },
    )?;
    if output.rows.len() != expected {
        return Err(HawDBError::Execution(format!(
            "content-store occurrence anchor count mismatch: expected {expected}, got {}",
            output.rows.len()
        )));
    }
    for row in &output.rows {
        let Some(Value::String(anchor_id)) = row.get("anchor_id") else {
            return Err(HawDBError::Execution(
                "content-store occurrence anchor has no anchor_id".to_string(),
            ));
        };
        let Some(Value::String(content_message_id)) = row.get("content_message_id") else {
            return Err(HawDBError::Execution(format!(
                "content-store anchor {anchor_id} has no occurrence identity"
            )));
        };
        if !anchor_id
            .strip_prefix("anchor-")
            .is_some_and(|suffix| content_message_id == &format!("content-message-{suffix}"))
        {
            return Err(HawDBError::Execution(format!(
                "content-store anchor {anchor_id} points at mismatched occurrence {content_message_id}"
            )));
        }
    }
    let runtime_legacy_ids = output
        .rows
        .iter()
        .filter(|row| {
            matches!(
                row.get("message_id"),
                Some(Value::String(message_id)) if message_id == RUNTIME_LEGACY_MESSAGE_ID
            )
        })
        .count();
    if runtime_legacy_ids != expected_shared_legacy_occurrences {
        return Err(HawDBError::Execution(format!(
            "content-store occurrence fixture expected {expected_shared_legacy_occurrences} anchors sharing one legacy message id, got {runtime_legacy_ids}"
        )));
    }
    Ok(())
}
