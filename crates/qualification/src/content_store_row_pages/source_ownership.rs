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
    corpus_statement, required_i64, source_chunks_by_source_parameters, source_id_parameters,
    source_space_parameters, SOURCE_DOCUMENT_ID, SOURCE_OWNER_ID,
};
use super::source_replacement::seed_source_chunks_for_followup;
use super::{ContentStoreRowPageReadPhase, ContentStoreSourceOwnershipMoveQualificationReport};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::path::Path;

const MOVE_CHUNK_COUNT: usize = 2;
const TARGET_SPACE_ID: &str = "work";
const MISSING_SOURCE_ID: &str = "missing-source";
const MOVE_UPDATED_AT: &str = "2026-01-01T00:05:00Z";
const MISSING_UPDATED_AT: &str = "2026-01-01T00:04:30Z";
const SOURCE_PAYLOAD_SQL: &str = "SELECT chunk_id, chunk_index, text, char_start, char_end, token_count, metadata_json, content_hash, created_at, updated_at FROM content_chunks WHERE content_doc_id = $1 ORDER BY chunk_index ASC, chunk_id ASC LIMIT $2";
const SOURCE_PAYLOAD_MAX_BYTES: usize = 16 * 1024 * 1024;
const SOURCE_DOCUMENT_STATE_SQL: &str = "SELECT space_id, item_count, updated_at FROM content_documents WHERE owner_kind = 'source' AND owner_id = $1";
const SOURCE_DOCUMENT_STATE_MAX_BYTES: usize = 4 * 1024;

pub(super) fn qualify_source_ownership_move(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
    chunk_payload_bytes: usize,
) -> Result<(Database, ContentStoreSourceOwnershipMoveQualificationReport)> {
    let missing_owner_epoch_before = database.commit_epoch();
    let missing_owner_count = qualify_missing_source_noop(&mut database, corpus)?;
    let missing_owner_epoch_after = database.commit_epoch();
    if missing_owner_epoch_after != missing_owner_epoch_before {
        return Err(HawDBError::Execution(format!(
            "content-store missing source ownership probe changed epoch from {missing_owner_epoch_before} to {missing_owner_epoch_after}"
        )));
    }

    let seed_commit_epoch = seed_source_chunks_for_followup(
        &mut database,
        corpus,
        MOVE_CHUNK_COUNT,
        chunk_payload_bytes,
    )?;
    require_source_ownership_state(&mut database, corpus, MOVE_CHUNK_COUNT, "default", None)?;
    let payload_sha256_before = source_payload_sha256(&mut database, MOVE_CHUNK_COUNT)?;

    let update_space = corpus_statement(corpus, "update_source_document_space")?;
    let count = corpus_statement(corpus, "source_chunk_count_by_source")?;
    let page = corpus_statement(corpus, "source_chunks_by_source")?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) SET s.space_id = $space_id",
        &source_space_parameters(TARGET_SPACE_ID),
    )?;
    transaction.query_sql_with_params(
        &update_space.sql,
        &[
            Value::String(TARGET_SPACE_ID.to_string()),
            Value::String(MOVE_UPDATED_AT.to_string()),
            Value::String(SOURCE_OWNER_ID.to_string()),
        ],
    )?;
    let count_output = transaction
        .query_sql_with_params(&count.sql, &[Value::String(SOURCE_OWNER_ID.to_string())])?;
    let moved_chunk_count = required_i64(&count_output, "chunk_count")?;
    let expected_chunk_count = i64::try_from(MOVE_CHUNK_COUNT).map_err(|_| {
        HawDBError::Semantic("content-store ownership chunk count does not fit BIGINT".to_string())
    })?;
    if moved_chunk_count != expected_chunk_count {
        return Err(HawDBError::Execution(format!(
            "content-store source ownership move counted {moved_chunk_count} chunks, expected {MOVE_CHUNK_COUNT}"
        )));
    }
    let moved_rows = transaction.query_sql_with_params(
        &page.sql,
        &source_chunks_by_source_parameters(MOVE_CHUNK_COUNT),
    )?;
    require_rows_in_space(&moved_rows, MOVE_CHUNK_COUNT, TARGET_SPACE_ID, "workspace")?;
    let graph = transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.space_id AS space_id, s.chunk_count AS chunk_count",
        &source_id_parameters(),
    )?;
    require_graph_space(&graph, MOVE_CHUNK_COUNT, TARGET_SPACE_ID, "workspace")?;
    transaction.commit()?;
    let committed_epoch = database.commit_epoch();
    if committed_epoch <= seed_commit_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store source ownership epoch {committed_epoch} did not advance beyond seed epoch {seed_commit_epoch}"
        )));
    }

    let live_read = execute_qualified_read(
        &mut database,
        page,
        source_chunks_by_source_parameters(MOVE_CHUNK_COUNT),
        ContentStoreRowPageReadPhase::LiveOverlay,
        MOVE_CHUNK_COUNT,
    )?;
    if live_read.execution.visible_commit_epoch != committed_epoch
        || live_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store source ownership live read used epoch {} and {} overlay entries, expected epoch {committed_epoch} with a non-empty overlay",
            live_read.execution.visible_commit_epoch, live_read.execution.overlay_entries
        )));
    }
    require_source_ownership_state(
        &mut database,
        corpus,
        MOVE_CHUNK_COUNT,
        TARGET_SPACE_ID,
        Some(MOVE_UPDATED_AT),
    )?;
    let payload_sha256_after_live = source_payload_sha256(&mut database, MOVE_CHUNK_COUNT)?;
    if payload_sha256_after_live != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store source ownership move changed chunk payload fields".to_string(),
        ));
    }

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store source ownership checkpoint did not publish relational indexes"
                    .to_string(),
            )
        })?
        .generation;
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    let reopened_read = execute_qualified_read(
        &mut database,
        page,
        source_chunks_by_source_parameters(MOVE_CHUNK_COUNT),
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        MOVE_CHUNK_COUNT,
    )?;
    if reopened_read.execution.visible_commit_epoch != committed_epoch
        || reopened_read.output_sha256 != live_read.output_sha256
    {
        return Err(HawDBError::Execution(
            "content-store source ownership move changed across checkpoint/reopen".to_string(),
        ));
    }
    require_source_ownership_state(
        &mut database,
        corpus,
        MOVE_CHUNK_COUNT,
        TARGET_SPACE_ID,
        Some(MOVE_UPDATED_AT),
    )?;
    let payload_sha256_after_reopen = source_payload_sha256(&mut database, MOVE_CHUNK_COUNT)?;
    if payload_sha256_after_reopen != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store reopened source ownership move changed chunk payload fields".to_string(),
        ));
    }

    Ok((
        database,
        ContentStoreSourceOwnershipMoveQualificationReport {
            previous_space_id: "default".to_string(),
            target_space_id: TARGET_SPACE_ID.to_string(),
            chunk_count: MOVE_CHUNK_COUNT,
            missing_owner_count,
            missing_owner_epoch_unchanged: true,
            seed_commit_epoch,
            committed_epoch,
            payload_sha256_before,
            payload_sha256_after_live,
            payload_sha256_after_reopen,
            payload_fields_preserved: true,
            live_read,
            checkpoint_generation,
            reopened_read,
        },
    ))
}

fn qualify_missing_source_noop(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<i64> {
    let update_space = corpus_statement(corpus, "update_source_document_space")?;
    let count = corpus_statement(corpus, "source_chunk_count_by_source")?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) SET s.space_id = $space_id",
        &source_space_parameters_for(MISSING_SOURCE_ID, TARGET_SPACE_ID),
    )?;
    transaction.query_sql_with_params(
        &update_space.sql,
        &[
            Value::String(TARGET_SPACE_ID.to_string()),
            Value::String(MISSING_UPDATED_AT.to_string()),
            Value::String(MISSING_SOURCE_ID.to_string()),
        ],
    )?;
    let output = transaction
        .query_sql_with_params(&count.sql, &[Value::String(MISSING_SOURCE_ID.to_string())])?;
    let missing_count = required_i64(&output, "chunk_count")?;
    if missing_count != 0 {
        return Err(HawDBError::Execution(format!(
            "content-store missing source ownership probe counted {missing_count} chunks"
        )));
    }
    transaction.rollback();
    Ok(missing_count)
}

fn require_source_ownership_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_chunk_count: usize,
    expected_space_id: &str,
    expected_updated_at: Option<&str>,
) -> Result<()> {
    let graph = database.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.space_id AS space_id, s.chunk_count AS chunk_count",
        &source_id_parameters(),
    )?;
    require_graph_space(&graph, expected_chunk_count, expected_space_id, "persisted")?;

    let document = database.query_sql_with_params_options(
        SOURCE_DOCUMENT_STATE_SQL,
        &[Value::String(SOURCE_OWNER_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(SOURCE_DOCUMENT_STATE_MAX_BYTES),
        },
    )?;
    let expected_item_count = i64::try_from(expected_chunk_count).map_err(|_| {
        HawDBError::Semantic("content-store ownership chunk count does not fit BIGINT".to_string())
    })?;
    match document.rows.as_slice() {
        [row]
            if row.get("item_count") == Some(&Value::Int(expected_item_count))
                && matches!(row.get("space_id"), Some(Value::String(space_id)) if space_id == expected_space_id)
                && expected_updated_at.is_none_or(|expected| {
                    matches!(row.get("updated_at"), Some(Value::String(actual)) if actual == expected)
                }) =>
            {}
        rows => {
            return Err(HawDBError::Execution(format!(
                "content-store source document expected space={expected_space_id}, chunks={expected_item_count}, updated_at={expected_updated_at:?}, got {rows:?}"
            )));
        }
    }
    let page = corpus_statement(corpus, "source_chunks_by_source")?;
    let rows = database.query_sql_with_params_options(
        &page.sql,
        &source_chunks_by_source_parameters(expected_chunk_count.max(1)),
        QueryStreamOptions {
            max_rows: Some(expected_chunk_count.max(1)),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_rows_in_space(&rows, expected_chunk_count, expected_space_id, "persisted")
}

fn source_payload_sha256(database: &mut Database, expected_rows: usize) -> Result<String> {
    let output = database.query_sql_with_params_options(
        SOURCE_PAYLOAD_SQL,
        &[
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
            Value::Int(i64::try_from(expected_rows.max(1)).map_err(|_| {
                HawDBError::Semantic(
                    "content-store ownership row limit does not fit BIGINT".to_string(),
                )
            })?),
        ],
        QueryStreamOptions {
            max_rows: Some(expected_rows.max(1)),
            max_payload_bytes: Some(SOURCE_PAYLOAD_MAX_BYTES),
        },
    )?;
    if output.rows.len() != expected_rows {
        return Err(HawDBError::Execution(format!(
            "content-store source payload probe returned {} rows, expected {expected_rows}",
            output.rows.len()
        )));
    }
    Ok(rows_sha256(&output.rows))
}

fn require_rows_in_space(
    output: &QueryOutput,
    expected_rows: usize,
    expected_space_id: &str,
    phase: &str,
) -> Result<()> {
    if output.rows.len() != expected_rows
        || output.rows.iter().any(|row| {
            !matches!(row.get("space_id"), Some(Value::String(space_id)) if space_id == expected_space_id)
        })
    {
        return Err(HawDBError::Execution(format!(
            "content-store source ownership {phase} expected {expected_rows} rows in space {expected_space_id}, got {:?}",
            output.rows
        )));
    }
    Ok(())
}

fn require_graph_space(
    output: &QueryOutput,
    expected_chunk_count: usize,
    expected_space_id: &str,
    phase: &str,
) -> Result<()> {
    let expected_chunk_count = i64::try_from(expected_chunk_count).map_err(|_| {
        HawDBError::Semantic("content-store ownership chunk count does not fit BIGINT".to_string())
    })?;
    match output.rows.as_slice() {
        [row]
            if row.get("chunk_count") == Some(&Value::Int(expected_chunk_count))
                && matches!(row.get("space_id"), Some(Value::String(space_id)) if space_id == expected_space_id) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store source ownership {phase} expected graph space={expected_space_id}, chunks={expected_chunk_count}, got {rows:?}"
        ))),
    }
}

fn source_space_parameters_for(
    source_id: &str,
    space_id: &str,
) -> std::collections::BTreeMap<String, Value> {
    std::collections::BTreeMap::from([
        (
            "source_id".to_string(),
            Value::String(source_id.to_string()),
        ),
        ("space_id".to_string(), Value::String(space_id.to_string())),
    ])
}
