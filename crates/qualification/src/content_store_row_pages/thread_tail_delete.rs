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

use super::evidence::{execute_qualified_read, message_point_parameters, message_point_statement};
use super::fixture::corpus_statement;
use super::thread_fixture::{
    thread_document_parameters, thread_message_anchor_parameters, thread_message_parameters,
    thread_page_parameters, ThreadDocumentParameters, ThreadMessageAnchorParameters,
    ThreadMessageParameters,
};
use super::{ContentStoreRowPageReadPhase, ContentStoreThreadTailDeleteQualificationReport};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

const THREAD_ID: &str = "qualified-tail-thread";
const THREAD_STORAGE_ID: &str = "qualified-tail-storage";
const CONTENT_DOCUMENT_ID: &str = "thread_msgdoc_qualified-tail-storage";
const SPACE_ID: &str = "qualified-tail-space";
const MEDIA_TYPE: &str = "application/vnd.nowledge.thread.messages+sqlite";
const CREATED_AT: &str = "2026-01-01T00:20:00Z";
const DELETED_AT: &str = "2026-01-01T00:21:00Z";
const INITIAL_MESSAGE_COUNT: usize = 4;
const RETAINED_MESSAGE_COUNT: usize = 2;
const START_INDEX: i64 = 2;
const STATE_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

const DOCUMENT_STATE_SQL: &str = "SELECT owner_id, item_count, size_bytes, updated_at FROM content_documents WHERE content_doc_id = $1";
const ANCHOR_STATE_SQL: &str = "SELECT anchor_id, content_message_id, message_id, order_index, quote_hash, metadata_json, created_at FROM content_anchors WHERE content_doc_id = $1 ORDER BY anchor_id ASC";
const RETAINED_MESSAGE_PAYLOAD_SQL: &str = "SELECT content_message_id, message_id, thread_storage_id, thread_id, content_doc_id, space_id, order_index, role, content, timestamp, token_count, metadata_json, external_id, exclude_from_distillation, content_hash, created_at, updated_at FROM thread_messages WHERE thread_storage_id = $1 AND order_index < $2 ORDER BY order_index ASC, content_message_id ASC";
const RETAINED_ANCHOR_PAYLOAD_SQL: &str = "SELECT anchor_id, owner_kind, owner_id, content_doc_id, target_kind, target_id, anchor_kind, content_message_id, message_id, order_index, quote_hash, metadata_json, created_at FROM content_anchors WHERE content_doc_id = $1 AND order_index < $2 ORDER BY anchor_id ASC";

struct MessageSeed {
    content_message_id: &'static str,
    message_id: &'static str,
    content: &'static str,
    role: &'static str,
}

const MESSAGES: [MessageSeed; INITIAL_MESSAGE_COUNT] = [
    MessageSeed {
        content_message_id: "qualified-tail-content-message-a",
        message_id: "qualified-tail-message-a",
        content: "retained first message",
        role: "user",
    },
    MessageSeed {
        content_message_id: "qualified-tail-content-message-b",
        message_id: "qualified-tail-message-b",
        content: "retained second message",
        role: "assistant",
    },
    MessageSeed {
        content_message_id: "qualified-tail-content-message-c",
        message_id: "qualified-tail-message-c",
        content: "deleted third message",
        role: "user",
    },
    MessageSeed {
        content_message_id: "qualified-tail-content-message-d",
        message_id: "qualified-tail-message-d",
        content: "deleted fourth message",
        role: "assistant",
    },
];

const HEAD_ANCHOR_ID: &str = "qualified-tail-anchor-head";
const TAIL_EXPLICIT_ANCHOR_ID: &str = "qualified-tail-anchor-explicit";
const TAIL_LEGACY_ANCHOR_ID: &str = "qualified-tail-anchor-legacy";

#[derive(Debug, PartialEq, Eq)]
struct TailStateDigests {
    messages: String,
    anchors: String,
    document: String,
    graph: String,
}

pub(super) fn qualify_thread_tail_delete(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreThreadTailDeleteQualificationReport)> {
    let (seed_commit_epoch, seed_checkpoint_generation) =
        seed_tail_delete_thread(&mut database, corpus)
            .map_err(|error| tail_delete_phase_error("seed", error))?;
    let state_before_rollback = tail_state_digests(&mut database, corpus, INITIAL_MESSAGE_COUNT)
        .map_err(|error| tail_delete_phase_error("pre-rollback state read", error))?;
    let epoch_before_rollback = database.commit_epoch();
    exercise_rolled_back_tail_delete(&mut database, corpus)
        .map_err(|error| tail_delete_phase_error("rollback exercise", error))?;
    let state_after_rollback = tail_state_digests(&mut database, corpus, INITIAL_MESSAGE_COUNT)
        .map_err(|error| tail_delete_phase_error("post-rollback state read", error))?;
    if database.commit_epoch() != epoch_before_rollback
        || state_after_rollback != state_before_rollback
    {
        return Err(HawDBError::Execution(
            "content-store rolled-back tail delete changed canonical state".to_string(),
        ));
    }

    let epoch_before_negative_start = database.commit_epoch();
    let negative_start = query_tail_candidates(&mut database, corpus, -1)?;
    require_tail_candidates(&negative_start, 0)?;
    if database.commit_epoch() != epoch_before_negative_start {
        return Err(HawDBError::Execution(
            "content-store negative tail start changed the commit epoch".to_string(),
        ));
    }

    let epoch_before_empty_tail = database.commit_epoch();
    let empty_tail =
        query_tail_candidates(&mut database, corpus, INITIAL_MESSAGE_COUNT as i64 + 1)?;
    if !empty_tail.rows.is_empty() || database.commit_epoch() != epoch_before_empty_tail {
        return Err(HawDBError::Execution(
            "content-store empty tail delete changed canonical state or commit epoch".to_string(),
        ));
    }

    let retained_message_payload_sha256_before = retained_message_payload_sha256(&mut database)?;
    let retained_anchor_payload_sha256_before = retained_anchor_payload_sha256(&mut database)?;
    let candidates = corpus_statement(corpus, "thread_tail_delete_candidates")?;
    let delete_anchors = corpus_statement(corpus, "delete_thread_tail_anchors")?;
    let delete_messages = corpus_statement(corpus, "delete_thread_tail_messages")?;
    let summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let tombstone_point =
        message_point_statement("thread_tail_deleted_message_point", "thread_tail_delete");
    let page_parameters = thread_page_parameters(THREAD_STORAGE_ID, RETAINED_MESSAGE_COUNT);

    let mut transaction = database.begin_transaction();
    let deleted_candidates = transaction.query_sql_with_params(
        &candidates.sql,
        &[
            Value::String(THREAD_STORAGE_ID.to_string()),
            Value::Int(START_INDEX),
        ],
    )?;
    let (deleted_message_ids, deleted_occurrence_ids) =
        require_tail_candidates(&deleted_candidates, RETAINED_MESSAGE_COUNT)?;
    transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) SET t.message_count = $message_count, t.updated_at = $updated_at",
        &graph_delete_parameters(),
    )
    .map_err(|error| tail_delete_phase_error("commit graph update", error))?;
    transaction
        .query_sql_with_params(
            &delete_anchors.sql,
            &[
                Value::String(CONTENT_DOCUMENT_ID.to_string()),
                Value::Int(START_INDEX),
            ],
        )
        .map_err(|error| tail_delete_phase_error("commit anchor delete", error))?;
    transaction
        .query_sql_with_params(
            &delete_messages.sql,
            &[
                Value::String(THREAD_STORAGE_ID.to_string()),
                Value::Int(START_INDEX),
            ],
        )
        .map_err(|error| tail_delete_phase_error("commit message delete", error))?;
    let payload_summary = transaction
        .query_sql_with_params(
            &summary.sql,
            &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        )
        .map_err(|error| tail_delete_phase_error("commit summary read", error))?;
    let item_count = required_i64(&payload_summary, "item_count")?;
    let size_bytes = required_i64(&payload_summary, "size_bytes")?;
    let expected_size_bytes = retained_payload_bytes()?;
    if item_count != RETAINED_MESSAGE_COUNT as i64 || size_bytes != expected_size_bytes {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete summary expected count={RETAINED_MESSAGE_COUNT}, bytes={expected_size_bytes}, got count={item_count}, bytes={size_bytes}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
            Value::String(DELETED_AT.to_string()),
            Value::String(CONTENT_DOCUMENT_ID.to_string()),
        ],
    )?;
    let workspace_page = transaction
        .query_sql_with_params(&page.sql, &page_parameters)
        .map_err(|error| tail_delete_phase_error("workspace message read", error))?;
    require_retained_messages(&workspace_page)?;
    let workspace_anchors = transaction
        .query_sql_with_params(
            ANCHOR_STATE_SQL,
            &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        )
        .map_err(|error| tail_delete_phase_error("workspace anchor read", error))?;
    require_retained_anchors(&workspace_anchors)?;
    let workspace_document = transaction.query_sql_with_params(
        DOCUMENT_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    require_document_state(&workspace_document, item_count, size_bytes)?;
    let workspace_graph = transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.message_count AS message_count, t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&workspace_graph)?;
    transaction
        .commit()
        .map_err(|error| tail_delete_phase_error("commit publication", error))?;

    let committed_epoch = database.commit_epoch();
    if committed_epoch != seed_commit_epoch.saturating_add(1) {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete published epoch {committed_epoch}, expected {}",
            seed_commit_epoch.saturating_add(1)
        )));
    }
    require_deleted_state(&mut database, corpus, item_count, size_bytes)
        .map_err(|error| tail_delete_phase_error("live state read", error))?;
    let retained_message_payload_sha256_after_live =
        retained_message_payload_sha256(&mut database)?;
    let retained_anchor_payload_sha256_after_live = retained_anchor_payload_sha256(&mut database)?;
    require_preserved_payloads(
        &retained_message_payload_sha256_before,
        &retained_message_payload_sha256_after_live,
        &retained_anchor_payload_sha256_before,
        &retained_anchor_payload_sha256_after_live,
        "live",
    )?;
    let deleted_tombstone_read = execute_qualified_read(
        &mut database,
        &tombstone_point,
        message_point_parameters(MESSAGES[RETAINED_MESSAGE_COUNT].content_message_id).to_vec(),
        ContentStoreRowPageReadPhase::LiveOverlay,
        0,
    )?;
    if deleted_tombstone_read.execution.visible_commit_epoch != committed_epoch
        || deleted_tombstone_read.execution.index_runtime_path != "none"
        || deleted_tombstone_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store tail tombstone point read observed epoch {}, index path {}, and {} row overlay entries; expected epoch {committed_epoch}, a direct canonical point read, and a non-empty tombstone overlay",
            deleted_tombstone_read.execution.visible_commit_epoch,
            deleted_tombstone_read.execution.index_runtime_path,
            deleted_tombstone_read.execution.overlay_entries,
        )));
    }
    let live_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters.clone(),
        ContentStoreRowPageReadPhase::LiveOverlay,
        RETAINED_MESSAGE_COUNT,
    )?;
    if live_read.execution.visible_commit_epoch != committed_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete live read observed epoch {}, expected epoch {committed_epoch}",
            live_read.execution.visible_commit_epoch
        )));
    }
    let live_anchor_sha256 = anchor_state_sha256(&mut database)?;

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store tail delete checkpoint published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    if checkpoint_generation <= seed_checkpoint_generation {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete checkpoint generation {checkpoint_generation} did not advance beyond seed generation {seed_checkpoint_generation}"
        )));
    }
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    require_deleted_state(&mut database, corpus, item_count, size_bytes)?;
    let retained_message_payload_sha256_after_reopen =
        retained_message_payload_sha256(&mut database)?;
    let retained_anchor_payload_sha256_after_reopen =
        retained_anchor_payload_sha256(&mut database)?;
    require_preserved_payloads(
        &retained_message_payload_sha256_before,
        &retained_message_payload_sha256_after_reopen,
        &retained_anchor_payload_sha256_before,
        &retained_anchor_payload_sha256_after_reopen,
        "reopened",
    )?;
    let reopened_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        RETAINED_MESSAGE_COUNT,
    )?;
    let reopened_anchor_sha256 = anchor_state_sha256(&mut database)?;
    if reopened_read.execution.visible_commit_epoch != committed_epoch
        || reopened_read.output_sha256 != live_read.output_sha256
        || reopened_anchor_sha256 != live_anchor_sha256
    {
        return Err(HawDBError::Execution(
            "content-store tail delete changed across checkpoint/reopen".to_string(),
        ));
    }

    Ok((
        database,
        ContentStoreThreadTailDeleteQualificationReport {
            thread_id: THREAD_ID.to_string(),
            thread_storage_id: THREAD_STORAGE_ID.to_string(),
            start_index: START_INDEX,
            initial_message_count: INITIAL_MESSAGE_COUNT,
            retained_message_count: RETAINED_MESSAGE_COUNT,
            deleted_message_ids,
            deleted_occurrence_ids,
            deleted_candidate_sha256: rows_sha256(&deleted_candidates.rows),
            negative_start_clamped: true,
            empty_tail_noop: true,
            empty_tail_epoch_unchanged: true,
            rollback_preserved_state: true,
            graph_relational_agreement: true,
            exact_summary: true,
            retained_payload_identity: true,
            summary_item_count: item_count,
            summary_size_bytes: size_bytes,
            seed_commit_epoch,
            seed_checkpoint_generation,
            committed_epoch,
            retained_message_payload_sha256_before,
            retained_message_payload_sha256_after_live,
            retained_message_payload_sha256_after_reopen,
            retained_anchor_payload_sha256_before,
            retained_anchor_payload_sha256_after_live,
            retained_anchor_payload_sha256_after_reopen,
            deleted_tombstone_read,
            live_read,
            live_anchor_sha256,
            checkpoint_generation,
            reopened_read,
            reopened_anchor_sha256,
        },
    ))
}

fn seed_tail_delete_thread(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(u64, u64)> {
    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let anchor = corpus_statement(corpus, "upsert_memory_message_anchor")?;
    let summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "CREATE (:Thread {id: $thread_id, space_id: $space_id, message_count: $message_count, updated_at: $updated_at})",
        &graph_seed_parameters(),
    )?;
    transaction.query_sql_with_params(
        &document.sql,
        &thread_document_parameters(ThreadDocumentParameters {
            content_document_id: CONTENT_DOCUMENT_ID,
            thread_storage_id: THREAD_STORAGE_ID,
            space_id: SPACE_ID,
            media_type: MEDIA_TYPE,
            created_at: CREATED_AT,
            updated_at: CREATED_AT,
        }),
    )?;
    for (order_index, seed) in MESSAGES.iter().enumerate() {
        transaction.query_sql_with_params(
            &message.sql,
            &message_parameters(seed, order_index, CREATED_AT),
        )?;
    }
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            HEAD_ANCHOR_ID,
            "qualified-tail-memory-head",
            Some(MESSAGES[0].content_message_id),
            MESSAGES[0].message_id,
            0,
        ),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            TAIL_EXPLICIT_ANCHOR_ID,
            "qualified-tail-memory-explicit",
            Some(MESSAGES[2].content_message_id),
            MESSAGES[2].message_id,
            2,
        ),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            TAIL_LEGACY_ANCHOR_ID,
            "qualified-tail-memory-legacy",
            None,
            MESSAGES[3].message_id,
            3,
        ),
    )?;
    let payload_summary = transaction.query_sql_with_params(
        &summary.sql,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(required_i64(&payload_summary, "item_count")?),
            Value::Int(required_i64(&payload_summary, "size_bytes")?),
            Value::String(CREATED_AT.to_string()),
            Value::String(CONTENT_DOCUMENT_ID.to_string()),
        ],
    )?;
    transaction.commit()?;
    let seed_commit_epoch = database.commit_epoch();
    database.checkpoint()?;
    let seed_checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store tail delete seed checkpoint published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    Ok((seed_commit_epoch, seed_checkpoint_generation))
}

fn exercise_rolled_back_tail_delete(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<()> {
    let delete_anchors = corpus_statement(corpus, "delete_thread_tail_anchors")?;
    let delete_messages = corpus_statement(corpus, "delete_thread_tail_messages")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) SET t.message_count = $message_count, t.updated_at = $updated_at",
        &graph_delete_parameters(),
    )
    .map_err(|error| tail_delete_phase_error("rollback graph update", error))?;
    transaction
        .query_sql_with_params(
            &delete_anchors.sql,
            &[
                Value::String(CONTENT_DOCUMENT_ID.to_string()),
                Value::Int(START_INDEX),
            ],
        )
        .map_err(|error| tail_delete_phase_error("rollback anchor delete", error))?;
    transaction
        .query_sql_with_params(
            &delete_messages.sql,
            &[
                Value::String(THREAD_STORAGE_ID.to_string()),
                Value::Int(START_INDEX),
            ],
        )
        .map_err(|error| tail_delete_phase_error("rollback message delete", error))?;
    let workspace = transaction
        .query_sql_with_params(
            &page.sql,
            &thread_page_parameters(THREAD_STORAGE_ID, RETAINED_MESSAGE_COUNT),
        )
        .map_err(|error| tail_delete_phase_error("rollback workspace read", error))?;
    require_retained_messages(&workspace)?;
    transaction.rollback();
    Ok(())
}

fn require_deleted_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    item_count: i64,
    size_bytes: i64,
) -> Result<()> {
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(THREAD_STORAGE_ID, INITIAL_MESSAGE_COUNT),
        QueryStreamOptions {
            max_rows: Some(INITIAL_MESSAGE_COUNT),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_retained_messages(&messages)?;
    let anchors = query_anchor_state(database)?;
    require_retained_anchors(&anchors)?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        state_query_options(1),
    )?;
    require_document_state(&document, item_count, size_bytes)?;
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.message_count AS message_count, t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&graph)
}

fn require_tail_candidates(
    output: &QueryOutput,
    first_message: usize,
) -> Result<(Vec<String>, Vec<String>)> {
    let expected_count = INITIAL_MESSAGE_COUNT
        .checked_sub(first_message)
        .ok_or_else(|| HawDBError::Execution("tail candidate start overflow".to_string()))?;
    if output.rows.len() != expected_count {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete expected {expected_count} candidates from position {first_message}, got {:?}",
            output.rows,
        )));
    }
    let mut message_ids = Vec::with_capacity(output.rows.len());
    let mut occurrence_ids = Vec::with_capacity(output.rows.len());
    for (offset, row) in output.rows.iter().enumerate() {
        let expected_position = first_message + offset;
        let expected = &MESSAGES[expected_position];
        if row.get("message_id") != Some(&Value::String(expected.message_id.to_string()))
            || row.get("content_message_id")
                != Some(&Value::String(expected.content_message_id.to_string()))
            || row.get("order_index")
                != Some(&Value::Int(i64::try_from(expected_position).map_err(
                    |_| HawDBError::Execution("tail candidate order overflow".to_string()),
                )?))
            || row.get("content_doc_id") != Some(&Value::String(CONTENT_DOCUMENT_ID.to_string()))
        {
            return Err(HawDBError::Execution(format!(
                "content-store tail delete candidate mismatch: {row:?}"
            )));
        }
        message_ids.push(expected.message_id.to_string());
        occurrence_ids.push(expected.content_message_id.to_string());
    }
    Ok((message_ids, occurrence_ids))
}

fn require_retained_messages(output: &QueryOutput) -> Result<()> {
    if output.rows.len() != RETAINED_MESSAGE_COUNT {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete expected {RETAINED_MESSAGE_COUNT} retained messages, got {:?}",
            output.rows
        )));
    }
    for (order_index, (row, seed)) in output.rows.iter().zip(&MESSAGES).enumerate() {
        let order_index = i64::try_from(order_index)
            .map_err(|_| HawDBError::Execution("retained message order overflow".to_string()))?;
        if row.get("content_message_id")
            != Some(&Value::String(seed.content_message_id.to_string()))
            || row.get("message_id") != Some(&Value::String(seed.message_id.to_string()))
            || row.get("thread_storage_id") != Some(&Value::String(THREAD_STORAGE_ID.to_string()))
            || row.get("thread_id") != Some(&Value::String(THREAD_ID.to_string()))
            || row.get("order_index") != Some(&Value::Int(order_index))
            || row.get("content") != Some(&Value::String(seed.content.to_string()))
            || row.get("created_at") != Some(&Value::String(CREATED_AT.to_string()))
        {
            return Err(HawDBError::Execution(format!(
                "content-store tail delete retained-message mismatch: {row:?}"
            )));
        }
    }
    Ok(())
}

fn require_retained_anchors(output: &QueryOutput) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("anchor_id") == Some(&Value::String(HEAD_ANCHOR_ID.to_string()))
                && row.get("content_message_id")
                    == Some(&Value::String(MESSAGES[0].content_message_id.to_string()))
                && row.get("message_id")
                    == Some(&Value::String(MESSAGES[0].message_id.to_string()))
                && row.get("order_index") == Some(&Value::Int(0)) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store tail delete retained-anchor mismatch: {rows:?}"
        ))),
    }
}

fn require_document_state(output: &QueryOutput, item_count: i64, size_bytes: i64) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("owner_id") == Some(&Value::String(THREAD_STORAGE_ID.to_string()))
                && row.get("item_count") == Some(&Value::Int(item_count))
                && row.get("size_bytes") == Some(&Value::Int(size_bytes))
                && row.get("updated_at") == Some(&Value::String(DELETED_AT.to_string())) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store tail delete document mismatch: {rows:?}"
        ))),
    }
}

fn require_graph_state(output: &QueryOutput) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("message_count") == Some(&Value::Int(RETAINED_MESSAGE_COUNT as i64))
                && row.get("updated_at") == Some(&Value::String(DELETED_AT.to_string())) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store tail delete graph mismatch: {rows:?}"
        ))),
    }
}

fn query_tail_candidates(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    start_index: i64,
) -> Result<QueryOutput> {
    let candidates = corpus_statement(corpus, "thread_tail_delete_candidates")?;
    database.query_sql_with_params_options(
        &candidates.sql,
        &[
            Value::String(THREAD_STORAGE_ID.to_string()),
            Value::Int(start_index.max(0)),
        ],
        QueryStreamOptions {
            max_rows: Some(candidates.max_rows),
            max_payload_bytes: Some(candidates.max_payload_bytes),
        },
    )
}

fn tail_state_digests(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    message_limit: usize,
) -> Result<TailStateDigests> {
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(THREAD_STORAGE_ID, message_limit),
        QueryStreamOptions {
            max_rows: Some(message_limit),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    let anchors = query_anchor_state(database)?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        state_query_options(1),
    )?;
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.message_count AS message_count, t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    Ok(TailStateDigests {
        messages: rows_sha256(&messages.rows),
        anchors: rows_sha256(&anchors.rows),
        document: rows_sha256(&document.rows),
        graph: rows_sha256(&graph.rows),
    })
}

fn retained_message_payload_sha256(database: &mut Database) -> Result<String> {
    let output = database.query_sql_with_params_options(
        RETAINED_MESSAGE_PAYLOAD_SQL,
        &[
            Value::String(THREAD_STORAGE_ID.to_string()),
            Value::Int(START_INDEX),
        ],
        state_query_options(RETAINED_MESSAGE_COUNT),
    )?;
    if output.rows.len() != RETAINED_MESSAGE_COUNT {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete retained-message probe returned {} rows",
            output.rows.len()
        )));
    }
    Ok(rows_sha256(&output.rows))
}

fn retained_anchor_payload_sha256(database: &mut Database) -> Result<String> {
    let output = database.query_sql_with_params_options(
        RETAINED_ANCHOR_PAYLOAD_SQL,
        &[
            Value::String(CONTENT_DOCUMENT_ID.to_string()),
            Value::Int(START_INDEX),
        ],
        state_query_options(1),
    )?;
    if output.rows.len() != 1 {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete retained-anchor probe returned {} rows",
            output.rows.len()
        )));
    }
    Ok(rows_sha256(&output.rows))
}

fn require_preserved_payloads(
    expected_messages: &str,
    actual_messages: &str,
    expected_anchors: &str,
    actual_anchors: &str,
    phase: &str,
) -> Result<()> {
    if actual_messages != expected_messages || actual_anchors != expected_anchors {
        return Err(HawDBError::Execution(format!(
            "content-store tail delete changed retained payloads during {phase}"
        )));
    }
    Ok(())
}

fn query_anchor_state(database: &mut Database) -> Result<QueryOutput> {
    database.query_sql_with_params_options(
        ANCHOR_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        state_query_options(3),
    )
}

fn anchor_state_sha256(database: &mut Database) -> Result<String> {
    Ok(rows_sha256(&query_anchor_state(database)?.rows))
}

fn state_query_options(max_rows: usize) -> QueryStreamOptions {
    QueryStreamOptions {
        max_rows: Some(max_rows),
        max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
    }
}

fn required_i64(output: &QueryOutput, field: &str) -> Result<i64> {
    match output.rows.as_slice() {
        [row] => match row.get(field) {
            Some(Value::Int(value)) => Ok(*value),
            other => Err(HawDBError::Execution(format!(
                "content-store tail delete expected integer {field}, got {other:?}"
            ))),
        },
        rows => Err(HawDBError::Execution(format!(
            "content-store tail delete expected one summary row, got {rows:?}"
        ))),
    }
}

fn tail_delete_phase_error(phase: &str, error: HawDBError) -> HawDBError {
    HawDBError::Execution(format!("content-store tail delete {phase} failed: {error}"))
}

fn retained_payload_bytes() -> Result<i64> {
    i64::try_from(
        MESSAGES
            .iter()
            .take(RETAINED_MESSAGE_COUNT)
            .map(|message| message.content.len())
            .sum::<usize>(),
    )
    .map_err(|_| HawDBError::Execution("retained payload size overflow".to_string()))
}

fn message_parameters(seed: &MessageSeed, order_index: usize, updated_at: &str) -> Vec<Value> {
    let order_index = i64::try_from(order_index).unwrap_or(i64::MAX);
    let metadata_json = format!("{{\"occurrence\":\"{}\"}}", seed.content_message_id);
    let external_id = format!("external-{}", seed.message_id);
    let content_hash = format!("hash-{}", seed.message_id);
    thread_message_parameters(ThreadMessageParameters {
        content_message_id: seed.content_message_id,
        message_id: seed.message_id,
        thread_storage_id: THREAD_STORAGE_ID,
        thread_id: THREAD_ID,
        content_document_id: CONTENT_DOCUMENT_ID,
        space_id: SPACE_ID,
        order_index,
        role: seed.role,
        content: seed.content,
        timestamp: CREATED_AT,
        token_count: 4 + order_index,
        metadata_json: &metadata_json,
        external_id: &external_id,
        exclude_from_distillation: false,
        content_hash: &content_hash,
        created_at: CREATED_AT,
        updated_at,
    })
}

fn anchor_parameters(
    anchor_id: &str,
    memory_id: &str,
    content_message_id: Option<&str>,
    message_id: &str,
    order_index: i64,
) -> Vec<Value> {
    let quote_hash = format!("quote-{anchor_id}");
    let metadata_json = format!("{{\"anchor\":\"{anchor_id}\"}}");
    thread_message_anchor_parameters(ThreadMessageAnchorParameters {
        anchor_id,
        memory_id,
        content_document_id: CONTENT_DOCUMENT_ID,
        thread_storage_id: THREAD_STORAGE_ID,
        content_message_id,
        message_id,
        order_index,
        quote_hash: &quote_hash,
        metadata_json: &metadata_json,
        created_at: CREATED_AT,
    })
}

fn graph_seed_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
        ("space_id".to_string(), Value::String(SPACE_ID.to_string())),
        (
            "message_count".to_string(),
            Value::Int(INITIAL_MESSAGE_COUNT as i64),
        ),
        (
            "updated_at".to_string(),
            Value::String(CREATED_AT.to_string()),
        ),
    ])
}

fn graph_delete_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
        (
            "message_count".to_string(),
            Value::Int(RETAINED_MESSAGE_COUNT as i64),
        ),
        (
            "updated_at".to_string(),
            Value::String(DELETED_AT.to_string()),
        ),
    ])
}

fn graph_identity_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(THREAD_ID.to_string()),
    )])
}
