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
    corpus_statement, source_chunks_by_source_parameters, source_id_parameters, SOURCE_DOCUMENT_ID,
    SOURCE_OWNER_ID,
};
use super::thread_ownership::{
    qualified_thread_ownership_identities, QualifiedThreadOwnershipIdentity,
};
use super::{
    ContentStoreRowPageReadPhase, ContentStoreSpaceMergeOwnershipQualificationReport,
    ContentStoreSpaceMergeReadReport,
};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

const SOURCE_SPACE_ID: &str = "work";
const TARGET_SPACE_ID: &str = "merged";
const MOVE_UPDATED_AT: &str = "2026-01-01T00:07:00Z";
const DOCUMENT_STATE_SQL: &str = "SELECT owner_id, space_id, updated_at, item_count FROM content_documents WHERE owner_kind = $1 AND owner_id = $2";
const DOCUMENT_PAYLOAD_SQL: &str = "SELECT content_doc_id, owner_kind, owner_id, media_type, schema_version, item_count, size_bytes, created_at FROM content_documents WHERE owner_kind = $1 AND owner_id = $2";
const MESSAGE_PAYLOAD_SQL: &str = "SELECT content_message_id, message_id, thread_storage_id, thread_id, content_doc_id, order_index, role, content, timestamp, token_count, metadata_json, external_id, exclude_from_distillation, content_hash, created_at FROM thread_messages WHERE thread_storage_id = $1 ORDER BY order_index ASC, content_message_id ASC LIMIT $2";
const SOURCE_PAYLOAD_SQL: &str = "SELECT chunk_id, content_doc_id, chunk_index, text, char_start, char_end, token_count, metadata_json, content_hash, created_at, updated_at FROM content_chunks WHERE content_doc_id = $1 ORDER BY chunk_index ASC, chunk_id ASC LIMIT $2";
const STATE_MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const PAYLOAD_MAX_BYTES: usize = 16 * 1024 * 1024;
const SOURCE_CHUNK_COUNT: usize = 2;
const SOURCE_CHUNK_COUNT_I64: i64 = 2;

pub(super) fn qualify_space_merge_ownership(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreSpaceMergeOwnershipQualificationReport)> {
    let threads = qualified_thread_ownership_identities();
    for thread in threads {
        require_thread_state(&mut database, corpus, thread, thread.current_space_id)?;
    }
    require_source_state(&mut database, corpus, SOURCE_SPACE_ID)?;
    let payload_sha256_before = ownership_payload_sha256(&mut database, &threads)?;
    let base_epoch = database.commit_epoch();

    let update_document = corpus_statement(corpus, "update_owned_document_space_guarded")?;
    let update_messages = corpus_statement(corpus, "update_thread_messages_space_guarded")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let source_page = corpus_statement(corpus, "source_chunks_by_source")?;
    let mut transaction = database.begin_transaction();
    for thread in threads {
        transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}) WHERE t.space_id = $source_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at",
            &thread_move_parameters(thread),
        )?;
        transaction.query_sql_with_params(
            &update_document.sql,
            &document_move_parameters("thread", thread.storage_id),
        )?;
        transaction.query_sql_with_params(
            &update_messages.sql,
            &message_move_parameters(thread.storage_id),
        )?;
    }
    transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) WHERE s.space_id = $source_space_id SET s.space_id = $target_space_id, s.updated_at = $updated_at",
        &source_move_parameters(),
    )?;
    transaction.query_sql_with_params(
        &update_document.sql,
        &document_move_parameters("source", SOURCE_OWNER_ID),
    )?;

    for thread in threads {
        let expected_space = expected_thread_space(thread);
        let expected_updated_at = expected_thread_updated_at(thread);
        let graph = transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
            &thread_identity_parameters(thread),
        )?;
        require_space_and_timestamp(
            &graph,
            expected_space,
            expected_updated_at,
            "workspace graph Thread",
        )?;
        let document = transaction.query_sql_with_params(
            DOCUMENT_STATE_SQL,
            &[
                Value::String("thread".to_string()),
                Value::String(thread.storage_id.to_string()),
            ],
        )?;
        require_document_space_and_timestamp(
            &document,
            thread.storage_id,
            expected_space,
            expected_updated_at,
            "workspace thread document",
        )?;
        let messages = transaction
            .query_sql_with_params(&page.sql, &thread_page_parameters(thread.storage_id))?;
        require_space_and_timestamp(
            &messages,
            expected_space,
            expected_updated_at,
            "workspace thread message",
        )?;
    }
    let graph_source = transaction.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.space_id AS space_id, s.updated_at AS updated_at, s.chunk_count AS chunk_count",
        &source_id_parameters(),
    )?;
    require_source_graph_state(
        &graph_source,
        TARGET_SPACE_ID,
        Some(MOVE_UPDATED_AT),
        "workspace",
    )?;
    let document_source = transaction.query_sql_with_params(
        DOCUMENT_STATE_SQL,
        &[
            Value::String("source".to_string()),
            Value::String(SOURCE_OWNER_ID.to_string()),
        ],
    )?;
    require_source_document_state(
        &document_source,
        TARGET_SPACE_ID,
        Some(MOVE_UPDATED_AT),
        "workspace",
    )?;
    let chunks = transaction.query_sql_with_params(
        &source_page.sql,
        &source_chunks_by_source_parameters(SOURCE_CHUNK_COUNT),
    )?;
    require_source_chunk_state(&chunks, TARGET_SPACE_ID, "workspace")?;
    transaction.commit()?;

    let committed_epoch = database.commit_epoch();
    if committed_epoch <= base_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store space merge epoch {committed_epoch} did not advance beyond base epoch {base_epoch}"
        )));
    }
    let live_reads = read_space_merge_pages(
        &mut database,
        corpus,
        &threads,
        ContentStoreRowPageReadPhase::LiveOverlay,
        committed_epoch,
    )?;
    require_changed_cases_use_live_overlay(&live_reads)?;
    require_persisted_ownership(&mut database, corpus, &threads)?;
    let payload_sha256_after_live = ownership_payload_sha256(&mut database, &threads)?;
    if payload_sha256_after_live != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store space merge changed non-ownership payload fields".to_string(),
        ));
    }

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store space merge checkpoint did not publish relational indexes"
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
    let reopened_reads = read_space_merge_pages(
        &mut database,
        corpus,
        &threads,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        committed_epoch,
    )?;
    for (live, reopened) in live_reads.iter().zip(&reopened_reads) {
        if live.case_name != reopened.case_name
            || live.expected_space_id != reopened.expected_space_id
            || live.read.output_sha256 != reopened.read.output_sha256
        {
            return Err(HawDBError::Execution(format!(
                "content-store space merge case {} changed across checkpoint/reopen",
                live.case_name
            )));
        }
    }
    require_persisted_ownership(&mut database, corpus, &threads)?;
    let payload_sha256_after_reopen = ownership_payload_sha256(&mut database, &threads)?;
    if payload_sha256_after_reopen != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store reopened space merge changed non-ownership payload fields".to_string(),
        ));
    }

    let moved_threads = threads
        .iter()
        .filter(|thread| thread.current_space_id == SOURCE_SPACE_ID)
        .count();
    Ok((
        database,
        ContentStoreSpaceMergeOwnershipQualificationReport {
            requested_threads: threads.len(),
            requested_sources: 1,
            documents_updated: moved_threads + 1,
            messages_updated: moved_threads,
            document_owner_uses_storage_id: true,
            stale_guard_preserved: true,
            graph_relational_agreement: true,
            payload_fields_preserved: true,
            base_epoch,
            committed_epoch,
            payload_sha256_before,
            payload_sha256_after_live,
            payload_sha256_after_reopen,
            live_reads,
            checkpoint_generation,
            reopened_reads,
        },
    ))
}

fn require_changed_cases_use_live_overlay(
    reads: &[ContentStoreSpaceMergeReadReport],
) -> Result<()> {
    for read in reads {
        if read.case_name == "thread_stale_preview" {
            continue;
        }
        let overlay_entries = read.read.execution.overlay_entries;
        if overlay_entries == 0 {
            return Err(HawDBError::Execution(format!(
                "content-store changed space merge case {} did not use the live overlay",
                read.case_name
            )));
        }
    }
    Ok(())
}

fn read_space_merge_pages(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    threads: &[QualifiedThreadOwnershipIdentity],
    phase: ContentStoreRowPageReadPhase,
    expected_epoch: u64,
) -> Result<Vec<ContentStoreSpaceMergeReadReport>> {
    let thread_page = corpus_statement(corpus, "thread_messages_page")?;
    let source_page = corpus_statement(corpus, "source_chunks_by_source")?;
    let mut reads = Vec::with_capacity(threads.len() + 1);
    for thread in threads {
        let read = execute_qualified_read(
            database,
            thread_page,
            thread_page_parameters(thread.storage_id),
            phase,
            1,
        )?;
        require_visible_epoch(&read, expected_epoch, thread.case_name)?;
        reads.push(ContentStoreSpaceMergeReadReport {
            case_name: format!("thread_{}", thread.case_name),
            expected_space_id: expected_thread_space(*thread).to_string(),
            read,
        });
    }
    let read = execute_qualified_read(
        database,
        source_page,
        source_chunks_by_source_parameters(SOURCE_CHUNK_COUNT),
        phase,
        SOURCE_CHUNK_COUNT,
    )?;
    require_visible_epoch(&read, expected_epoch, "source")?;
    reads.push(ContentStoreSpaceMergeReadReport {
        case_name: "source".to_string(),
        expected_space_id: TARGET_SPACE_ID.to_string(),
        read,
    });
    Ok(reads)
}

fn require_visible_epoch(
    read: &super::ContentStoreRowPageReadReport,
    expected_epoch: u64,
    case_name: &str,
) -> Result<()> {
    if read.execution.visible_commit_epoch != expected_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store space merge {case_name} observed epoch {}, expected {expected_epoch}",
            read.execution.visible_commit_epoch
        )));
    }
    Ok(())
}

fn require_persisted_ownership(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    threads: &[QualifiedThreadOwnershipIdentity],
) -> Result<()> {
    for thread in threads {
        require_thread_state(database, corpus, *thread, expected_thread_space(*thread))?;
    }
    require_source_state(database, corpus, TARGET_SPACE_ID)
}

fn require_thread_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    thread: QualifiedThreadOwnershipIdentity,
    expected_space_id: &str,
) -> Result<()> {
    let expected_updated_at = if expected_space_id == thread.current_space_id {
        thread.current_updated_at
    } else {
        MOVE_UPDATED_AT
    };
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
        &thread_identity_parameters(thread),
    )?;
    require_space_and_timestamp(
        &graph,
        expected_space_id,
        expected_updated_at,
        "persisted graph Thread",
    )?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[
            Value::String("thread".to_string()),
            Value::String(thread.storage_id.to_string()),
        ],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_document_space_and_timestamp(
        &document,
        thread.storage_id,
        expected_space_id,
        expected_updated_at,
        "persisted thread document",
    )?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(thread.storage_id),
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_space_and_timestamp(
        &messages,
        expected_space_id,
        expected_updated_at,
        "persisted thread message",
    )
}

fn require_source_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected_space_id: &str,
) -> Result<()> {
    let graph = database.query_with_params(
        "MATCH (s:Source {id: $source_id}) RETURN s.space_id AS space_id, s.updated_at AS updated_at, s.chunk_count AS chunk_count",
        &source_id_parameters(),
    )?;
    let expected_updated_at = (expected_space_id == TARGET_SPACE_ID).then_some(MOVE_UPDATED_AT);
    require_source_graph_state(&graph, expected_space_id, expected_updated_at, "persisted")?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[
            Value::String("source".to_string()),
            Value::String(SOURCE_OWNER_ID.to_string()),
        ],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_source_document_state(
        &document,
        expected_space_id,
        expected_updated_at,
        "persisted",
    )?;
    let page = corpus_statement(corpus, "source_chunks_by_source")?;
    let chunks = database.query_sql_with_params_options(
        &page.sql,
        &source_chunks_by_source_parameters(SOURCE_CHUNK_COUNT),
        QueryStreamOptions {
            max_rows: Some(SOURCE_CHUNK_COUNT),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_source_chunk_state(&chunks, expected_space_id, "persisted")
}

fn ownership_payload_sha256(
    database: &mut Database,
    threads: &[QualifiedThreadOwnershipIdentity],
) -> Result<String> {
    let mut rows = Vec::with_capacity(threads.len() * 2 + SOURCE_CHUNK_COUNT + 1);
    for thread in threads {
        let document = database.query_sql_with_params_options(
            DOCUMENT_PAYLOAD_SQL,
            &[
                Value::String("thread".to_string()),
                Value::String(thread.storage_id.to_string()),
            ],
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
            },
        )?;
        let messages = database.query_sql_with_params_options(
            MESSAGE_PAYLOAD_SQL,
            &[Value::String(thread.storage_id.to_string()), Value::Int(1)],
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
            },
        )?;
        if document.rows.len() != 1 || messages.rows.len() != 1 {
            return Err(HawDBError::Execution(format!(
                "content-store space merge expected one document and message for {}, got documents={} messages={}",
                thread.thread_id,
                document.rows.len(),
                messages.rows.len()
            )));
        }
        rows.extend(document.rows);
        rows.extend(messages.rows);
    }
    let source_document = database.query_sql_with_params_options(
        DOCUMENT_PAYLOAD_SQL,
        &[
            Value::String("source".to_string()),
            Value::String(SOURCE_OWNER_ID.to_string()),
        ],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
        },
    )?;
    let source_chunks = database.query_sql_with_params_options(
        SOURCE_PAYLOAD_SQL,
        &[
            Value::String(SOURCE_DOCUMENT_ID.to_string()),
            Value::Int(SOURCE_CHUNK_COUNT_I64),
        ],
        QueryStreamOptions {
            max_rows: Some(SOURCE_CHUNK_COUNT),
            max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
        },
    )?;
    if source_document.rows.len() != 1 || source_chunks.rows.len() != SOURCE_CHUNK_COUNT {
        return Err(HawDBError::Execution(format!(
            "content-store space merge expected one source document and {SOURCE_CHUNK_COUNT} chunks, got documents={} chunks={}",
            source_document.rows.len(),
            source_chunks.rows.len()
        )));
    }
    rows.extend(source_document.rows);
    rows.extend(source_chunks.rows);
    Ok(rows_sha256(&rows))
}

fn require_space_and_timestamp(
    output: &QueryOutput,
    expected_space_id: &str,
    expected_updated_at: &str,
    phase: &str,
) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if matches!(row.get("space_id"), Some(Value::String(value)) if value == expected_space_id)
                && matches!(row.get("updated_at"), Some(Value::String(value)) if value == expected_updated_at) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store space merge {phase} expected space={expected_space_id}, updated_at={expected_updated_at}, got {rows:?}"
        ))),
    }
}

fn require_document_space_and_timestamp(
    output: &QueryOutput,
    expected_owner_id: &str,
    expected_space_id: &str,
    expected_updated_at: &str,
    phase: &str,
) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if matches!(row.get("owner_id"), Some(Value::String(value)) if value == expected_owner_id)
                && matches!(row.get("space_id"), Some(Value::String(value)) if value == expected_space_id)
                && matches!(row.get("updated_at"), Some(Value::String(value)) if value == expected_updated_at) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store space merge {phase} expected owner={expected_owner_id}, space={expected_space_id}, updated_at={expected_updated_at}, got {rows:?}"
        ))),
    }
}

fn require_source_graph_state(
    output: &QueryOutput,
    expected_space_id: &str,
    expected_updated_at: Option<&str>,
    phase: &str,
) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("chunk_count") == Some(&Value::Int(SOURCE_CHUNK_COUNT_I64))
                && matches!(row.get("space_id"), Some(Value::String(value)) if value == expected_space_id)
                && expected_updated_at.is_none_or(|expected| {
                    matches!(row.get("updated_at"), Some(Value::String(actual)) if actual == expected)
                }) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store space merge {phase} graph Source expected space={expected_space_id}, chunks={SOURCE_CHUNK_COUNT}, got {rows:?}"
        ))),
    }
}

fn require_source_document_state(
    output: &QueryOutput,
    expected_space_id: &str,
    expected_updated_at: Option<&str>,
    phase: &str,
) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if matches!(row.get("owner_id"), Some(Value::String(value)) if value == SOURCE_OWNER_ID)
                && row.get("item_count") == Some(&Value::Int(SOURCE_CHUNK_COUNT_I64))
                && matches!(row.get("space_id"), Some(Value::String(value)) if value == expected_space_id)
                && expected_updated_at.is_none_or(|expected| {
                    matches!(row.get("updated_at"), Some(Value::String(actual)) if actual == expected)
                }) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store space merge {phase} source document expected owner={SOURCE_OWNER_ID}, space={expected_space_id}, chunks={SOURCE_CHUNK_COUNT}, got {rows:?}"
        ))),
    }
}

fn require_source_chunk_state(
    output: &QueryOutput,
    expected_space_id: &str,
    phase: &str,
) -> Result<()> {
    if output.rows.len() != SOURCE_CHUNK_COUNT
        || output.rows.iter().any(|row| {
            !matches!(row.get("space_id"), Some(Value::String(value)) if value == expected_space_id)
        })
    {
        return Err(HawDBError::Execution(format!(
            "content-store space merge {phase} expected {SOURCE_CHUNK_COUNT} source chunks in {expected_space_id}, got {:?}",
            output.rows
        )));
    }
    Ok(())
}

fn expected_thread_space(thread: QualifiedThreadOwnershipIdentity) -> &'static str {
    if thread.current_space_id == SOURCE_SPACE_ID {
        TARGET_SPACE_ID
    } else {
        thread.current_space_id
    }
}

fn expected_thread_updated_at(thread: QualifiedThreadOwnershipIdentity) -> &'static str {
    if thread.current_space_id == SOURCE_SPACE_ID {
        MOVE_UPDATED_AT
    } else {
        thread.current_updated_at
    }
}

fn thread_page_parameters(storage_id: &str) -> Vec<Value> {
    vec![
        Value::String(storage_id.to_string()),
        Value::Int(1),
        Value::Int(0),
    ]
}

fn document_move_parameters(owner_kind: &str, owner_id: &str) -> Vec<Value> {
    vec![
        Value::String(TARGET_SPACE_ID.to_string()),
        Value::String(MOVE_UPDATED_AT.to_string()),
        Value::String(owner_kind.to_string()),
        Value::String(owner_id.to_string()),
        Value::String(SOURCE_SPACE_ID.to_string()),
    ]
}

fn message_move_parameters(storage_id: &str) -> Vec<Value> {
    vec![
        Value::String(TARGET_SPACE_ID.to_string()),
        Value::String(MOVE_UPDATED_AT.to_string()),
        Value::String(storage_id.to_string()),
        Value::String(SOURCE_SPACE_ID.to_string()),
    ]
}

fn thread_identity_parameters(thread: QualifiedThreadOwnershipIdentity) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(thread.thread_id.to_string()),
    )])
}

fn thread_move_parameters(thread: QualifiedThreadOwnershipIdentity) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(thread.thread_id.to_string()),
        ),
        (
            "source_space_id".to_string(),
            Value::String(SOURCE_SPACE_ID.to_string()),
        ),
        (
            "target_space_id".to_string(),
            Value::String(TARGET_SPACE_ID.to_string()),
        ),
        (
            "updated_at".to_string(),
            Value::String(MOVE_UPDATED_AT.to_string()),
        ),
    ])
}

fn source_move_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "source_id".to_string(),
            Value::String(SOURCE_OWNER_ID.to_string()),
        ),
        (
            "source_space_id".to_string(),
            Value::String(SOURCE_SPACE_ID.to_string()),
        ),
        (
            "target_space_id".to_string(),
            Value::String(TARGET_SPACE_ID.to_string()),
        ),
        (
            "updated_at".to_string(),
            Value::String(MOVE_UPDATED_AT.to_string()),
        ),
    ])
}
