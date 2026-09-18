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
use super::fixture::corpus_statement;
use super::thread_fixture::{
    thread_document_parameters, thread_message_anchor_parameters, thread_message_parameters,
    thread_page_parameters, ThreadDocumentParameters, ThreadMessageAnchorParameters,
    ThreadMessageParameters,
};
use super::{ContentStoreRowPageReadPhase, ContentStoreThreadReconcileQualificationReport};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const THREAD_ID: &str = "qualified-reconcile-thread";
const THREAD_STORAGE_ID: &str = "qualified-reconcile-storage";
const CONTENT_DOCUMENT_ID: &str = "thread_msgdoc_qualified-reconcile-storage";
const SPACE_ID: &str = "qualified-reconcile-space";
const MEDIA_TYPE: &str = "application/vnd.nowledge.thread.messages+sqlite";
const CREATED_AT: &str = "2026-01-01T00:10:00Z";
const RECONCILED_AT: &str = "2026-01-01T00:11:00Z";
const CONTENT_MESSAGE_A_ID: &str = "qualified-reconcile-content-message-a";
const CONTENT_MESSAGE_B_ID: &str = "qualified-reconcile-content-message-b";
const CONTENT_MESSAGE_C_ID: &str = "qualified-reconcile-content-message-c";
const MESSAGE_A_ID: &str = "qualified-reconcile-message-a";
const MESSAGE_B_ID: &str = "qualified-reconcile-message-b";
const MESSAGE_C_ID: &str = "qualified-reconcile-message-c";
const MESSAGE_A_CONTENT: &str = "preserved first message";
const MESSAGE_B_CONTENT: &str = "preserved second message";
const MESSAGE_C_CONTENT: &str = "inserted middle message";
const ANCHOR_A_ID: &str = "qualified-reconcile-anchor-a";
const ANCHOR_B_ID: &str = "qualified-reconcile-anchor-b";
const MEMORY_A_ID: &str = "qualified-reconcile-memory-a";
const MEMORY_B_ID: &str = "qualified-reconcile-memory-b";
const INITIAL_MESSAGE_COUNT: usize = 2;
const FINAL_MESSAGE_COUNT: usize = 3;
const STATE_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

const DOCUMENT_STATE_SQL: &str = "SELECT owner_id, item_count, size_bytes, updated_at FROM content_documents WHERE owner_kind = 'thread' AND owner_id = $1";
const ANCHOR_STATE_SQL: &str = "SELECT anchor_id, content_message_id, message_id, order_index, quote_hash, metadata_json, created_at FROM content_anchors WHERE content_doc_id = $1 ORDER BY anchor_id ASC";
const PRESERVED_MESSAGE_PAYLOAD_SQL: &str = "SELECT content_message_id, message_id, thread_storage_id, thread_id, content_doc_id, space_id, role, content, timestamp, token_count, metadata_json, external_id, exclude_from_distillation, content_hash, created_at FROM thread_messages WHERE thread_storage_id = $1 AND (content_message_id = $2 OR content_message_id = $3) ORDER BY content_message_id ASC";
const PRESERVED_ANCHOR_PAYLOAD_SQL: &str = "SELECT anchor_id, owner_kind, owner_id, content_doc_id, target_kind, target_id, anchor_kind, content_message_id, message_id, quote_hash, metadata_json, created_at FROM content_anchors WHERE anchor_id = $1 OR anchor_id = $2 ORDER BY anchor_id ASC";

pub(super) fn qualify_thread_message_reconcile(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreThreadReconcileQualificationReport)> {
    let (seed_commit_epoch, seed_checkpoint_generation) =
        seed_reconcile_thread(&mut database, corpus)?;
    let existing = read_message_occurrence_ids(&mut database, corpus, INITIAL_MESSAGE_COUNT)?;
    let epoch_before_invalid_mappings = database.commit_epoch();
    require_invalid_mapping(
        &existing,
        &[Some(CONTENT_MESSAGE_A_ID), Some(CONTENT_MESSAGE_A_ID)],
    )?;
    require_invalid_mapping(&existing, &[Some(CONTENT_MESSAGE_A_ID)])?;
    require_invalid_mapping(
        &existing,
        &[Some(CONTENT_MESSAGE_A_ID), Some("unknown-content-message")],
    )?;
    let desired_mapping = [Some(CONTENT_MESSAGE_B_ID), None, Some(CONTENT_MESSAGE_A_ID)];
    validate_preserve_mapping(&existing, &desired_mapping)?;
    if database.commit_epoch() != epoch_before_invalid_mappings {
        return Err(HawDBError::Execution(
            "content-store invalid reconciliation mapping advanced the commit epoch".to_string(),
        ));
    }

    let preserved_message_payload_sha256_before = preserved_message_payload_sha256(&mut database)?;
    let preserved_anchor_payload_sha256_before = preserved_anchor_payload_sha256(&mut database)?;
    let update_anchor = corpus_statement(corpus, "update_message_anchor_order")?;
    let update_message = corpus_statement(corpus, "update_thread_message_order")?;
    let upsert_document = corpus_statement(corpus, "upsert_content_document")?;
    let upsert_message = corpus_statement(corpus, "upsert_thread_message")?;
    let summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let page_parameters = thread_page_parameters(THREAD_STORAGE_ID, FINAL_MESSAGE_COUNT);

    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) SET t.updated_at = $updated_at",
        &graph_reconcile_parameters(),
    )?;
    transaction.query_sql_with_params(
        &upsert_document.sql,
        &document_parameters(CREATED_AT, RECONCILED_AT),
    )?;
    transaction.query_sql_with_params(
        &update_anchor.sql,
        &anchor_order_parameters(0, CONTENT_MESSAGE_B_ID, MESSAGE_B_ID, 1),
    )?;
    transaction.query_sql_with_params(
        &update_message.sql,
        &message_order_parameters(0, CONTENT_MESSAGE_B_ID),
    )?;
    transaction.query_sql_with_params(
        &upsert_message.sql,
        &message_parameters(
            CONTENT_MESSAGE_C_ID,
            MESSAGE_C_ID,
            1,
            "assistant",
            MESSAGE_C_CONTENT,
            CREATED_AT,
            RECONCILED_AT,
        ),
    )?;
    transaction.query_sql_with_params(
        &update_anchor.sql,
        &anchor_order_parameters(2, CONTENT_MESSAGE_A_ID, MESSAGE_A_ID, 0),
    )?;
    transaction.query_sql_with_params(
        &update_message.sql,
        &message_order_parameters(2, CONTENT_MESSAGE_A_ID),
    )?;

    let payload_summary = transaction.query_sql_with_params(
        &summary.sql,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    let item_count = required_i64(&payload_summary, "item_count")?;
    let size_bytes = required_i64(&payload_summary, "size_bytes")?;
    let expected_size_bytes = i64::try_from(
        MESSAGE_A_CONTENT
            .len()
            .saturating_add(MESSAGE_B_CONTENT.len())
            .saturating_add(MESSAGE_C_CONTENT.len()),
    )
    .map_err(|_| HawDBError::Execution("thread reconcile payload size overflow".to_string()))?;
    if item_count != FINAL_MESSAGE_COUNT as i64 || size_bytes != expected_size_bytes {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile summary expected count={FINAL_MESSAGE_COUNT}, bytes={expected_size_bytes}, got count={item_count}, bytes={size_bytes}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
            Value::String(RECONCILED_AT.to_string()),
            Value::String(CONTENT_DOCUMENT_ID.to_string()),
        ],
    )?;
    let workspace_page = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    require_reconciled_page(&workspace_page)?;
    let workspace_anchors = transaction.query_sql_with_params(
        ANCHOR_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    require_reconciled_anchors(&workspace_anchors)?;
    let workspace_document = transaction.query_sql_with_params(
        DOCUMENT_STATE_SQL,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
    )?;
    require_document_summary(&workspace_document, item_count, size_bytes)?;
    let workspace_graph = transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&workspace_graph)?;
    transaction.commit()?;

    let committed_epoch = database.commit_epoch();
    if committed_epoch != seed_commit_epoch.saturating_add(1) {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile published epoch {committed_epoch}, expected {}",
            seed_commit_epoch.saturating_add(1)
        )));
    }
    require_reconciled_state(&mut database, corpus, item_count, size_bytes)?;
    let preserved_message_payload_sha256_after_live =
        preserved_message_payload_sha256(&mut database)?;
    let preserved_anchor_payload_sha256_after_live =
        preserved_anchor_payload_sha256(&mut database)?;
    require_preserved_payloads(
        &preserved_message_payload_sha256_before,
        &preserved_message_payload_sha256_after_live,
        &preserved_anchor_payload_sha256_before,
        &preserved_anchor_payload_sha256_after_live,
        "live",
    )?;
    let live_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters.clone(),
        ContentStoreRowPageReadPhase::LiveOverlay,
        FINAL_MESSAGE_COUNT,
    )?;
    if live_read.execution.visible_commit_epoch != committed_epoch
        || live_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile live read observed epoch {} and {} overlay entries, expected epoch {committed_epoch} and a non-empty overlay",
            live_read.execution.visible_commit_epoch, live_read.execution.overlay_entries
        )));
    }
    let live_anchor_sha256 = anchor_state_sha256(&mut database)?;

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store thread reconcile checkpoint published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    if checkpoint_generation <= seed_checkpoint_generation {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile checkpoint generation {checkpoint_generation} did not advance beyond seed generation {seed_checkpoint_generation}"
        )));
    }
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    require_reconciled_state(&mut database, corpus, item_count, size_bytes)?;
    let preserved_message_payload_sha256_after_reopen =
        preserved_message_payload_sha256(&mut database)?;
    let preserved_anchor_payload_sha256_after_reopen =
        preserved_anchor_payload_sha256(&mut database)?;
    require_preserved_payloads(
        &preserved_message_payload_sha256_before,
        &preserved_message_payload_sha256_after_reopen,
        &preserved_anchor_payload_sha256_before,
        &preserved_anchor_payload_sha256_after_reopen,
        "reopened",
    )?;
    let reopened_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        FINAL_MESSAGE_COUNT,
    )?;
    let reopened_anchor_sha256 = anchor_state_sha256(&mut database)?;
    if reopened_read.execution.visible_commit_epoch != committed_epoch
        || reopened_read.output_sha256 != live_read.output_sha256
        || reopened_anchor_sha256 != live_anchor_sha256
    {
        return Err(HawDBError::Execution(
            "content-store thread reconcile changed across checkpoint/reopen".to_string(),
        ));
    }

    Ok((
        database,
        ContentStoreThreadReconcileQualificationReport {
            thread_id: THREAD_ID.to_string(),
            thread_storage_id: THREAD_STORAGE_ID.to_string(),
            initial_message_count: INITIAL_MESSAGE_COUNT,
            final_message_count: FINAL_MESSAGE_COUNT,
            duplicate_mapping_rejected: true,
            incomplete_mapping_rejected: true,
            unknown_mapping_rejected: true,
            invalid_mapping_epoch_unchanged: true,
            explicit_anchor_reordered: true,
            legacy_anchor_reordered: true,
            occurrence_identity_preserved: true,
            summary_item_count: item_count,
            summary_size_bytes: size_bytes,
            seed_commit_epoch,
            seed_checkpoint_generation,
            committed_epoch,
            preserved_message_payload_sha256_before,
            preserved_message_payload_sha256_after_live,
            preserved_message_payload_sha256_after_reopen,
            preserved_anchor_payload_sha256_before,
            preserved_anchor_payload_sha256_after_live,
            preserved_anchor_payload_sha256_after_reopen,
            live_read,
            live_anchor_sha256,
            checkpoint_generation,
            reopened_read,
            reopened_anchor_sha256,
        },
    ))
}

fn seed_reconcile_thread(
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
        "CREATE (:Thread {id: $thread_id, space_id: $space_id, updated_at: $updated_at})",
        &graph_seed_parameters(),
    )?;
    transaction
        .query_sql_with_params(&document.sql, &document_parameters(CREATED_AT, CREATED_AT))?;
    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(
            CONTENT_MESSAGE_A_ID,
            MESSAGE_A_ID,
            0,
            "user",
            MESSAGE_A_CONTENT,
            CREATED_AT,
            CREATED_AT,
        ),
    )?;
    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(
            CONTENT_MESSAGE_B_ID,
            MESSAGE_B_ID,
            1,
            "assistant",
            MESSAGE_B_CONTENT,
            CREATED_AT,
            CREATED_AT,
        ),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(ANCHOR_A_ID, MEMORY_A_ID, None, MESSAGE_A_ID, 0),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            ANCHOR_B_ID,
            MEMORY_B_ID,
            Some(CONTENT_MESSAGE_B_ID),
            MESSAGE_B_ID,
            1,
        ),
    )?;
    let payload_summary = transaction.query_sql_with_params(
        &summary.sql,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    let item_count = required_i64(&payload_summary, "item_count")?;
    let size_bytes = required_i64(&payload_summary, "size_bytes")?;
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
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
                "content-store reconcile seed checkpoint published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    Ok((seed_commit_epoch, seed_checkpoint_generation))
}

fn require_invalid_mapping(existing: &[String], mapping: &[Option<&str>]) -> Result<()> {
    if validate_preserve_mapping(existing, mapping).is_ok() {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile admitted invalid mapping {mapping:?}"
        )));
    }
    Ok(())
}

fn validate_preserve_mapping(existing: &[String], mapping: &[Option<&str>]) -> Result<()> {
    let existing = existing.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let preserved = mapping
        .iter()
        .filter_map(|value| *value)
        .collect::<Vec<_>>();
    let preserved_set = preserved.iter().copied().collect::<BTreeSet<_>>();
    if preserved.len() != preserved_set.len()
        || preserved_set.len() != existing.len()
        || preserved_set != existing
    {
        return Err(HawDBError::Semantic(
            "preserve mapping must contain every existing message exactly once".to_string(),
        ));
    }
    Ok(())
}

fn read_message_occurrence_ids(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    expected: usize,
) -> Result<Vec<String>> {
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let output = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(THREAD_STORAGE_ID, expected + 1),
        QueryStreamOptions {
            max_rows: Some(expected + 1),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    if output.rows.len() != expected {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile expected {expected} existing messages, got {}",
            output.rows.len()
        )));
    }
    output
        .rows
        .iter()
        .map(|row| match row.get("content_message_id") {
            Some(Value::String(value)) => Ok(value.clone()),
            other => Err(HawDBError::Execution(format!(
                "content-store thread reconcile existing row has invalid occurrence id {other:?}"
            ))),
        })
        .collect()
}

fn require_reconciled_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    item_count: i64,
    size_bytes: i64,
) -> Result<()> {
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(THREAD_STORAGE_ID, FINAL_MESSAGE_COUNT),
        QueryStreamOptions {
            max_rows: Some(FINAL_MESSAGE_COUNT),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_reconciled_page(&messages)?;
    let anchors = query_anchor_state(database)?;
    require_reconciled_anchors(&anchors)?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_document_summary(&document, item_count, size_bytes)?;
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&graph)
}

fn require_reconciled_page(output: &QueryOutput) -> Result<()> {
    let expected = [
        (CONTENT_MESSAGE_B_ID, MESSAGE_B_ID, 0, MESSAGE_B_CONTENT),
        (CONTENT_MESSAGE_C_ID, MESSAGE_C_ID, 1, MESSAGE_C_CONTENT),
        (CONTENT_MESSAGE_A_ID, MESSAGE_A_ID, 2, MESSAGE_A_CONTENT),
    ];
    if output.rows.len() != expected.len() {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile expected {} messages, got {:?}",
            expected.len(),
            output.rows
        )));
    }
    for (row, (content_message_id, message_id, order_index, content)) in
        output.rows.iter().zip(expected)
    {
        if row.get("content_message_id") != Some(&Value::String(content_message_id.to_string()))
            || row.get("message_id") != Some(&Value::String(message_id.to_string()))
            || row.get("thread_storage_id") != Some(&Value::String(THREAD_STORAGE_ID.to_string()))
            || row.get("thread_id") != Some(&Value::String(THREAD_ID.to_string()))
            || row.get("order_index") != Some(&Value::Int(order_index))
            || row.get("content") != Some(&Value::String(content.to_string()))
            || row.get("created_at") != Some(&Value::String(CREATED_AT.to_string()))
        {
            return Err(HawDBError::Execution(format!(
                "content-store thread reconcile message state mismatch: {row:?}"
            )));
        }
    }
    Ok(())
}

fn require_reconciled_anchors(output: &QueryOutput) -> Result<()> {
    let [legacy, explicit] = output.rows.as_slice() else {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile expected two anchors, got {:?}",
            output.rows
        )));
    };
    let legacy_matches = legacy.get("anchor_id") == Some(&Value::String(ANCHOR_A_ID.to_string()))
        && legacy.get("content_message_id") == Some(&Value::Null)
        && legacy.get("message_id") == Some(&Value::String(MESSAGE_A_ID.to_string()))
        && legacy.get("order_index") == Some(&Value::Int(2));
    let explicit_matches = explicit.get("anchor_id")
        == Some(&Value::String(ANCHOR_B_ID.to_string()))
        && explicit.get("content_message_id")
            == Some(&Value::String(CONTENT_MESSAGE_B_ID.to_string()))
        && explicit.get("message_id") == Some(&Value::String(MESSAGE_B_ID.to_string()))
        && explicit.get("order_index") == Some(&Value::Int(0));
    if legacy_matches && explicit_matches {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "content-store thread reconcile anchor state mismatch: {:?}",
            output.rows
        )))
    }
}

fn require_document_summary(output: &QueryOutput, item_count: i64, size_bytes: i64) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("owner_id") == Some(&Value::String(THREAD_STORAGE_ID.to_string()))
                && row.get("item_count") == Some(&Value::Int(item_count))
                && row.get("size_bytes") == Some(&Value::Int(size_bytes))
                && row.get("updated_at") == Some(&Value::String(RECONCILED_AT.to_string())) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store thread reconcile document summary mismatch: {rows:?}"
        ))),
    }
}

fn require_graph_state(output: &QueryOutput) -> Result<()> {
    match output.rows.as_slice() {
        [row] if row.get("updated_at") == Some(&Value::String(RECONCILED_AT.to_string())) => Ok(()),
        rows => Err(HawDBError::Execution(format!(
            "content-store thread reconcile graph state mismatch: {rows:?}"
        ))),
    }
}

fn preserved_message_payload_sha256(database: &mut Database) -> Result<String> {
    let output = database.query_sql_with_params_options(
        PRESERVED_MESSAGE_PAYLOAD_SQL,
        &[
            Value::String(THREAD_STORAGE_ID.to_string()),
            Value::String(CONTENT_MESSAGE_A_ID.to_string()),
            Value::String(CONTENT_MESSAGE_B_ID.to_string()),
        ],
        QueryStreamOptions {
            max_rows: Some(INITIAL_MESSAGE_COUNT),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    if output.rows.len() != INITIAL_MESSAGE_COUNT {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile preserved-message probe returned {} rows",
            output.rows.len()
        )));
    }
    Ok(rows_sha256(&output.rows))
}

fn preserved_anchor_payload_sha256(database: &mut Database) -> Result<String> {
    let output = database.query_sql_with_params_options(
        PRESERVED_ANCHOR_PAYLOAD_SQL,
        &[
            Value::String(ANCHOR_A_ID.to_string()),
            Value::String(ANCHOR_B_ID.to_string()),
        ],
        QueryStreamOptions {
            max_rows: Some(INITIAL_MESSAGE_COUNT),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    if output.rows.len() != INITIAL_MESSAGE_COUNT {
        return Err(HawDBError::Execution(format!(
            "content-store thread reconcile preserved-anchor probe returned {} rows",
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
            "content-store thread reconcile changed preserved payloads during {phase}"
        )));
    }
    Ok(())
}

fn query_anchor_state(database: &mut Database) -> Result<QueryOutput> {
    database.query_sql_with_params_options(
        ANCHOR_STATE_SQL,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(INITIAL_MESSAGE_COUNT),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )
}

fn anchor_state_sha256(database: &mut Database) -> Result<String> {
    Ok(rows_sha256(&query_anchor_state(database)?.rows))
}

fn required_i64(output: &QueryOutput, field: &str) -> Result<i64> {
    match output.rows.as_slice() {
        [row] => match row.get(field) {
            Some(Value::Int(value)) => Ok(*value),
            other => Err(HawDBError::Execution(format!(
                "content-store thread reconcile expected integer {field}, got {other:?}"
            ))),
        },
        rows => Err(HawDBError::Execution(format!(
            "content-store thread reconcile expected one summary row, got {rows:?}"
        ))),
    }
}

fn document_parameters(created_at: &str, updated_at: &str) -> Vec<Value> {
    thread_document_parameters(ThreadDocumentParameters {
        content_document_id: CONTENT_DOCUMENT_ID,
        thread_storage_id: THREAD_STORAGE_ID,
        space_id: SPACE_ID,
        media_type: MEDIA_TYPE,
        created_at,
        updated_at,
    })
}

fn message_parameters(
    content_message_id: &str,
    message_id: &str,
    order_index: i64,
    role: &str,
    content: &str,
    created_at: &str,
    updated_at: &str,
) -> Vec<Value> {
    let metadata_json = format!("{{\"occurrence\":\"{content_message_id}\"}}");
    let external_id = format!("external-{message_id}");
    let content_hash = format!("hash-{message_id}");
    thread_message_parameters(ThreadMessageParameters {
        content_message_id,
        message_id,
        thread_storage_id: THREAD_STORAGE_ID,
        thread_id: THREAD_ID,
        content_document_id: CONTENT_DOCUMENT_ID,
        space_id: SPACE_ID,
        order_index,
        role,
        content,
        timestamp: created_at,
        token_count: 4 + order_index,
        metadata_json: &metadata_json,
        external_id: &external_id,
        exclude_from_distillation: false,
        content_hash: &content_hash,
        created_at,
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

fn anchor_order_parameters(
    target_order: i64,
    content_message_id: &str,
    message_id: &str,
    previous_order: i64,
) -> Vec<Value> {
    vec![
        Value::Int(target_order),
        Value::String(CONTENT_DOCUMENT_ID.to_string()),
        Value::String(content_message_id.to_string()),
        Value::String(message_id.to_string()),
        Value::Int(previous_order),
    ]
}

fn message_order_parameters(target_order: i64, content_message_id: &str) -> Vec<Value> {
    vec![
        Value::Int(target_order),
        Value::String(RECONCILED_AT.to_string()),
        Value::String(THREAD_STORAGE_ID.to_string()),
        Value::String(content_message_id.to_string()),
    ]
}

fn graph_seed_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
        ("space_id".to_string(), Value::String(SPACE_ID.to_string())),
        (
            "updated_at".to_string(),
            Value::String(CREATED_AT.to_string()),
        ),
    ])
}

fn graph_reconcile_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
        (
            "updated_at".to_string(),
            Value::String(RECONCILED_AT.to_string()),
        ),
    ])
}

fn graph_identity_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(THREAD_ID.to_string()),
    )])
}
