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
    thread_document_parameters, thread_message_parameters, thread_page_parameters,
    ThreadDocumentParameters, ThreadMessageParameters,
};
use super::{ContentStoreRowPageReadPhase, ContentStoreThreadUpsertQualificationReport};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

const THREAD_ID: &str = "qualified-upsert-thread";
const THREAD_STORAGE_ID: &str = "qualified-upsert-storage";
const CONTENT_DOCUMENT_ID: &str = "thread_msgdoc_qualified-upsert-storage";
const SPACE_ID: &str = "qualified-upsert-space";
const MEDIA_TYPE: &str = "application/vnd.nowledge.thread.messages+sqlite";
const CREATED_AT: &str = "2026-01-01T00:08:00Z";
const UPDATED_AT: &str = "2026-01-01T00:08:30Z";
const CONFLICT_CREATED_AT: &str = "2026-01-01T00:09:00Z";
const MESSAGE_A_ID: &str = "qualified-upsert-message-a";
const MESSAGE_B_ID: &str = "qualified-upsert-message-b";
const CONTENT_MESSAGE_A_ID: &str = "qualified-upsert-content-message-a";
const CONTENT_MESSAGE_B_ID: &str = "qualified-upsert-content-message-b";
const MESSAGE_A_CONTENT: &str = "first qualified message";
const MESSAGE_B_INITIAL_CONTENT: &str = "second message before conflict";
const MESSAGE_B_FINAL_CONTENT: &str = "second qualified message after conflict";
const MESSAGE_COUNT: usize = 2;
const MESSAGE_COUNT_I64: i64 = 2;
const STATE_MAX_PAYLOAD_BYTES: usize = 32 * 1024;

const DOCUMENT_STATE_SQL: &str = "SELECT content_doc_id, owner_id, space_id, media_type, item_count, size_bytes, created_at, updated_at FROM content_documents WHERE owner_kind = 'thread' AND owner_id = $1";

pub(super) fn qualify_thread_message_upsert(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreThreadUpsertQualificationReport)> {
    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let page_parameters = thread_page_parameters(THREAD_STORAGE_ID, MESSAGE_COUNT);
    let base_epoch = database.commit_epoch();

    let mut transaction = database.begin_transaction();
    transaction.query_with_params(
        "CREATE (:Thread {id: $thread_id, space_id: $space_id, updated_at: $updated_at})",
        &graph_parameters(),
    )?;
    transaction.query_sql_with_params(&document.sql, &document_parameters(CREATED_AT))?;
    transaction.query_sql_with_params(&document.sql, &document_parameters(CONFLICT_CREATED_AT))?;
    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(
            CONTENT_MESSAGE_A_ID,
            MESSAGE_A_ID,
            0,
            MESSAGE_A_CONTENT,
            CREATED_AT,
        ),
    )?;
    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(
            CONTENT_MESSAGE_B_ID,
            MESSAGE_B_ID,
            1,
            MESSAGE_B_INITIAL_CONTENT,
            CREATED_AT,
        ),
    )?;

    let before_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    require_message_page(&before_rejection, MESSAGE_B_INITIAL_CONTENT, CREATED_AT)?;
    let before_rejection_sha256 = rows_sha256(&before_rejection.rows);
    let mut rejected_parameters = message_parameters(
        "qualified-upsert-rejected-content-message",
        "qualified-upsert-rejected-message",
        2,
        "rejected payload",
        CREATED_AT,
    );
    rejected_parameters[4] = Value::String("missing-qualified-upsert-document".to_string());
    let rejection = match transaction.query_sql_with_params(&message.sql, &rejected_parameters) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "content-store thread upsert admitted a missing-document foreign key".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !rejection.to_string().contains("foreign key") {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert expected a foreign-key rejection, got: {rejection}"
        )));
    }
    let after_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    if rows_sha256(&after_rejection.rows) != before_rejection_sha256 {
        return Err(HawDBError::Execution(
            "content-store rejected thread upsert changed the accepted transaction workspace"
                .to_string(),
        ));
    }

    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(
            CONTENT_MESSAGE_B_ID,
            MESSAGE_B_ID,
            1,
            MESSAGE_B_FINAL_CONTENT,
            CONFLICT_CREATED_AT,
        ),
    )?;
    let workspace_page = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    require_message_page(&workspace_page, MESSAGE_B_FINAL_CONTENT, CREATED_AT)?;

    let payload_summary = transaction.query_sql_with_params(
        &summary.sql,
        &[Value::String(CONTENT_DOCUMENT_ID.to_string())],
    )?;
    let item_count = required_i64(&payload_summary, "item_count")?;
    let size_bytes = required_i64(&payload_summary, "size_bytes")?;
    let expected_size_bytes = i64::try_from(
        MESSAGE_A_CONTENT
            .len()
            .saturating_add(MESSAGE_B_FINAL_CONTENT.len()),
    )
    .map_err(|_| HawDBError::Execution("thread upsert payload size overflow".to_string()))?;
    if item_count != MESSAGE_COUNT_I64 || size_bytes != expected_size_bytes {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert summary expected count={MESSAGE_COUNT_I64}, bytes={expected_size_bytes}, got count={item_count}, bytes={size_bytes}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(item_count),
            Value::Int(size_bytes),
            Value::String(UPDATED_AT.to_string()),
            Value::String(CONTENT_DOCUMENT_ID.to_string()),
        ],
    )?;
    let workspace_document = transaction.query_sql_with_params(
        DOCUMENT_STATE_SQL,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
    )?;
    require_document_state(&workspace_document, item_count, size_bytes)?;
    let workspace_graph = transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&workspace_graph)?;
    transaction.commit()?;

    let committed_epoch = database.commit_epoch();
    if committed_epoch != base_epoch.saturating_add(1) {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert published epoch {committed_epoch}, expected {}",
            base_epoch.saturating_add(1)
        )));
    }
    require_persisted_state(&mut database, corpus, item_count, size_bytes)?;
    let live_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters.clone(),
        ContentStoreRowPageReadPhase::LiveOverlay,
        MESSAGE_COUNT,
    )?;
    if live_read.execution.visible_commit_epoch != committed_epoch
        || live_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert live read observed epoch {} and {} overlay entries, expected epoch {committed_epoch} and a non-empty overlay",
            live_read.execution.visible_commit_epoch, live_read.execution.overlay_entries
        )));
    }

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store thread upsert checkpoint published no relational generation"
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
    require_persisted_state(&mut database, corpus, item_count, size_bytes)?;
    let reopened_read = execute_qualified_read(
        &mut database,
        page,
        page_parameters,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        MESSAGE_COUNT,
    )?;
    if reopened_read.execution.visible_commit_epoch != committed_epoch
        || reopened_read.output_sha256 != live_read.output_sha256
    {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert changed across checkpoint/reopen: live_epoch={}, reopened_epoch={}, live_digest={}, reopened_digest={}",
            live_read.execution.visible_commit_epoch,
            reopened_read.execution.visible_commit_epoch,
            live_read.output_sha256,
            reopened_read.output_sha256
        )));
    }

    Ok((
        database,
        ContentStoreThreadUpsertQualificationReport {
            thread_id: THREAD_ID.to_string(),
            thread_storage_id: THREAD_STORAGE_ID.to_string(),
            content_document_id: CONTENT_DOCUMENT_ID.to_string(),
            message_count: MESSAGE_COUNT,
            summary_size_bytes: size_bytes,
            document_owner_uses_storage_id: true,
            message_public_id_preserved: true,
            document_created_at_preserved_on_conflict: true,
            message_created_at_preserved_on_conflict: true,
            rejected_statement_atomic: true,
            graph_relational_agreement: true,
            base_epoch,
            committed_epoch,
            live_read,
            checkpoint_generation,
            reopened_read,
        },
    ))
}

fn require_persisted_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    item_count: i64,
    size_bytes: i64,
) -> Result<()> {
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
        &graph_identity_parameters(),
    )?;
    require_graph_state(&graph)?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_document_state(&document, item_count, size_bytes)?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(THREAD_STORAGE_ID, MESSAGE_COUNT),
        QueryStreamOptions {
            max_rows: Some(MESSAGE_COUNT),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_message_page(&messages, MESSAGE_B_FINAL_CONTENT, CREATED_AT)
}

fn require_graph_state(output: &QueryOutput) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("space_id") == Some(&Value::String(SPACE_ID.to_string()))
                && row.get("updated_at") == Some(&Value::String(UPDATED_AT.to_string())) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store thread upsert graph identity mismatch: {rows:?}"
        ))),
    }
}

fn require_document_state(output: &QueryOutput, item_count: i64, size_bytes: i64) -> Result<()> {
    match output.rows.as_slice() {
        [row]
            if row.get("content_doc_id")
                == Some(&Value::String(CONTENT_DOCUMENT_ID.to_string()))
                && row.get("owner_id") == Some(&Value::String(THREAD_STORAGE_ID.to_string()))
                && row.get("space_id") == Some(&Value::String(SPACE_ID.to_string()))
                && row.get("media_type") == Some(&Value::String(MEDIA_TYPE.to_string()))
                && row.get("item_count") == Some(&Value::Int(item_count))
                && row.get("size_bytes") == Some(&Value::Int(size_bytes))
                && row.get("created_at") == Some(&Value::String(CREATED_AT.to_string()))
                && row.get("updated_at") == Some(&Value::String(UPDATED_AT.to_string())) =>
        {
            Ok(())
        }
        rows => Err(HawDBError::Execution(format!(
            "content-store thread upsert document state mismatch: {rows:?}"
        ))),
    }
}

fn require_message_page(
    output: &QueryOutput,
    expected_second_content: &str,
    expected_second_created_at: &str,
) -> Result<()> {
    let [first, second] = output.rows.as_slice() else {
        return Err(HawDBError::Execution(format!(
            "content-store thread upsert expected two messages, got {:?}",
            output.rows
        )));
    };
    require_message_identity(
        first,
        CONTENT_MESSAGE_A_ID,
        MESSAGE_A_ID,
        0,
        MESSAGE_A_CONTENT,
        CREATED_AT,
    )?;
    require_message_identity(
        second,
        CONTENT_MESSAGE_B_ID,
        MESSAGE_B_ID,
        1,
        expected_second_content,
        expected_second_created_at,
    )
}

fn require_message_identity(
    row: &hawdb::QueryRow,
    content_message_id: &str,
    message_id: &str,
    order_index: i64,
    content: &str,
    created_at: &str,
) -> Result<()> {
    let matches = row.get("content_message_id")
        == Some(&Value::String(content_message_id.to_string()))
        && row.get("message_id") == Some(&Value::String(message_id.to_string()))
        && row.get("thread_storage_id") == Some(&Value::String(THREAD_STORAGE_ID.to_string()))
        && row.get("thread_id") == Some(&Value::String(THREAD_ID.to_string()))
        && row.get("content_doc_id") == Some(&Value::String(CONTENT_DOCUMENT_ID.to_string()))
        && row.get("space_id") == Some(&Value::String(SPACE_ID.to_string()))
        && row.get("order_index") == Some(&Value::Int(order_index))
        && row.get("content") == Some(&Value::String(content.to_string()))
        && row.get("created_at") == Some(&Value::String(created_at.to_string()));
    if matches {
        Ok(())
    } else {
        Err(HawDBError::Execution(format!(
            "content-store thread upsert message identity mismatch for {content_message_id}: {row:?}"
        )))
    }
}

fn required_i64(output: &QueryOutput, field: &str) -> Result<i64> {
    match output.rows.as_slice() {
        [row] => match row.get(field) {
            Some(Value::Int(value)) => Ok(*value),
            other => Err(HawDBError::Execution(format!(
                "content-store thread upsert expected integer {field}, got {other:?}"
            ))),
        },
        rows => Err(HawDBError::Execution(format!(
            "content-store thread upsert expected one summary row, got {rows:?}"
        ))),
    }
}

fn graph_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
        ("space_id".to_string(), Value::String(SPACE_ID.to_string())),
        (
            "updated_at".to_string(),
            Value::String(UPDATED_AT.to_string()),
        ),
    ])
}

fn graph_identity_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(THREAD_ID.to_string()),
    )])
}

fn document_parameters(created_at: &str) -> Vec<Value> {
    thread_document_parameters(ThreadDocumentParameters {
        content_document_id: CONTENT_DOCUMENT_ID,
        thread_storage_id: THREAD_STORAGE_ID,
        space_id: SPACE_ID,
        media_type: MEDIA_TYPE,
        created_at,
        updated_at: UPDATED_AT,
    })
}

fn message_parameters(
    content_message_id: &str,
    message_id: &str,
    order_index: i64,
    content: &str,
    created_at: &str,
) -> Vec<Value> {
    let role = if order_index == 0 {
        "user"
    } else {
        "assistant"
    };
    let metadata_json = format!("{{\"message\":{order_index}}}");
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
        timestamp: UPDATED_AT,
        token_count: 4 + order_index,
        metadata_json: &metadata_json,
        external_id: &external_id,
        exclude_from_distillation: false,
        content_hash: &content_hash,
        created_at,
        updated_at: UPDATED_AT,
    })
}
