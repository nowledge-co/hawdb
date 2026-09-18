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
use super::{
    ContentStoreRowPageReadPhase, ContentStoreThreadOwnershipMoveQualificationReport,
    ContentStoreThreadOwnershipReadReport,
};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DurabilityPolicy, HawDBError, QueryOutput, QueryStreamOptions,
    Result, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

const TARGET_SPACE_ID: &str = "work";
const MOVE_UPDATED_AT: &str = "2026-01-01T00:06:00Z";
const SEED_UPDATED_AT: &str = "2026-01-01T00:05:30Z";
const DOCUMENT_STATE_SQL: &str = "SELECT owner_id, space_id, updated_at FROM content_documents WHERE owner_kind = 'thread' AND owner_id = $1";
const DOCUMENT_PAYLOAD_SQL: &str = "SELECT content_doc_id, owner_kind, owner_id, media_type, schema_version, item_count, size_bytes, created_at FROM content_documents WHERE owner_kind = 'thread' AND owner_id = $1";
const MESSAGE_PAYLOAD_SQL: &str = "SELECT content_message_id, message_id, thread_storage_id, thread_id, content_doc_id, order_index, role, content, timestamp, token_count, metadata_json, external_id, exclude_from_distillation, content_hash, created_at FROM thread_messages WHERE thread_storage_id = $1 ORDER BY order_index ASC, content_message_id ASC LIMIT $2";
const STATE_MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const PAYLOAD_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct ThreadMoveFixture {
    case_name: &'static str,
    thread_id: &'static str,
    storage_id: &'static str,
    document_id: &'static str,
    message_id: &'static str,
    content_message_id: &'static str,
    initial_space_id: &'static str,
    expected_space_id: &'static str,
    expected_final_space_id: &'static str,
    expected_updated_at: &'static str,
}

const THREAD_MOVES: [ThreadMoveFixture; 3] = [
    ThreadMoveFixture {
        case_name: "matched_default",
        thread_id: "ownership-thread-a",
        storage_id: "ownership-storage-a",
        document_id: "ownership-document-a",
        message_id: "ownership-message-a",
        content_message_id: "ownership-content-message-a",
        initial_space_id: "default",
        expected_space_id: "default",
        expected_final_space_id: TARGET_SPACE_ID,
        expected_updated_at: MOVE_UPDATED_AT,
    },
    ThreadMoveFixture {
        case_name: "matched_archive",
        thread_id: "ownership-thread-b",
        storage_id: "ownership-storage-b",
        document_id: "ownership-document-b",
        message_id: "ownership-message-b",
        content_message_id: "ownership-content-message-b",
        initial_space_id: "archive",
        expected_space_id: "archive",
        expected_final_space_id: TARGET_SPACE_ID,
        expected_updated_at: MOVE_UPDATED_AT,
    },
    ThreadMoveFixture {
        case_name: "stale_preview",
        thread_id: "ownership-thread-stale",
        storage_id: "ownership-storage-stale",
        document_id: "ownership-document-stale",
        message_id: "ownership-message-stale",
        content_message_id: "ownership-content-message-stale",
        initial_space_id: "stale-current",
        expected_space_id: "stale-preview",
        expected_final_space_id: "stale-current",
        expected_updated_at: SEED_UPDATED_AT,
    },
];

#[derive(Debug, Clone, Copy)]
pub(super) struct QualifiedThreadOwnershipIdentity {
    pub(super) case_name: &'static str,
    pub(super) thread_id: &'static str,
    pub(super) storage_id: &'static str,
    pub(super) current_space_id: &'static str,
    pub(super) current_updated_at: &'static str,
}

pub(super) fn qualified_thread_ownership_identities(
) -> [QualifiedThreadOwnershipIdentity; THREAD_MOVES.len()] {
    THREAD_MOVES.map(|fixture| QualifiedThreadOwnershipIdentity {
        case_name: fixture.case_name,
        thread_id: fixture.thread_id,
        storage_id: fixture.storage_id,
        current_space_id: fixture.expected_final_space_id,
        current_updated_at: fixture.expected_updated_at,
    })
}

pub(super) fn qualify_thread_ownership_moves(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreThreadOwnershipMoveQualificationReport)> {
    let seed_commit_epoch = seed_thread_ownership_rows(&mut database, corpus)?;
    for fixture in THREAD_MOVES {
        require_thread_state(&mut database, corpus, fixture, fixture.initial_space_id)?;
    }
    let payload_sha256_before = thread_payload_sha256(&mut database)?;

    let update_document = corpus_statement(corpus, "update_owned_document_space_guarded")?;
    let update_messages = corpus_statement(corpus, "update_thread_messages_space_guarded")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let mut transaction = database.begin_transaction();
    for fixture in THREAD_MOVES {
        transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}) WHERE t.space_id = $expected_space_id SET t.space_id = $target_space_id, t.updated_at = $updated_at",
            &move_graph_parameters(fixture),
        )?;
        transaction.query_sql_with_params(
            &update_document.sql,
            &[
                Value::String(TARGET_SPACE_ID.to_string()),
                Value::String(MOVE_UPDATED_AT.to_string()),
                Value::String("thread".to_string()),
                Value::String(fixture.storage_id.to_string()),
                Value::String(fixture.expected_space_id.to_string()),
            ],
        )?;
        transaction.query_sql_with_params(
            &update_messages.sql,
            &[
                Value::String(TARGET_SPACE_ID.to_string()),
                Value::String(MOVE_UPDATED_AT.to_string()),
                Value::String(fixture.storage_id.to_string()),
                Value::String(fixture.expected_space_id.to_string()),
            ],
        )?;
    }
    for fixture in THREAD_MOVES {
        let graph = transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
            &thread_graph_parameters(fixture),
        )?;
        require_graph_state(
            &graph,
            fixture.expected_final_space_id,
            fixture.expected_updated_at,
            "workspace",
        )?;
        let document = transaction.query_sql_with_params(
            DOCUMENT_STATE_SQL,
            &[Value::String(fixture.storage_id.to_string())],
        )?;
        require_document_relational_state(
            &document,
            fixture.storage_id,
            fixture.expected_final_space_id,
            fixture.expected_updated_at,
            "workspace document",
        )?;
        let messages = transaction
            .query_sql_with_params(&page.sql, &thread_page_parameters(fixture.storage_id))?;
        require_relational_state(
            &messages,
            fixture.expected_final_space_id,
            fixture.expected_updated_at,
            "workspace message",
        )?;
    }
    transaction.commit()?;
    let committed_epoch = database.commit_epoch();
    if committed_epoch <= seed_commit_epoch {
        return Err(HawDBError::Execution(format!(
            "content-store thread ownership epoch {committed_epoch} did not advance beyond seed epoch {seed_commit_epoch}"
        )));
    }

    let live_reads = read_thread_pages(
        &mut database,
        corpus,
        ContentStoreRowPageReadPhase::LiveOverlay,
        committed_epoch,
    )?;
    for fixture in THREAD_MOVES {
        require_thread_state(
            &mut database,
            corpus,
            fixture,
            fixture.expected_final_space_id,
        )?;
    }
    let payload_sha256_after_live = thread_payload_sha256(&mut database)?;
    if payload_sha256_after_live != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store thread ownership move changed document or message payload fields"
                .to_string(),
        ));
    }

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store thread ownership checkpoint did not publish relational indexes"
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
    let reopened_reads = read_thread_pages(
        &mut database,
        corpus,
        ContentStoreRowPageReadPhase::ColdCheckpoint,
        committed_epoch,
    )?;
    for (live, reopened) in live_reads.iter().zip(&reopened_reads) {
        if live.case_name != reopened.case_name
            || live.expected_space_id != reopened.expected_space_id
            || live.read.output_sha256 != reopened.read.output_sha256
        {
            return Err(HawDBError::Execution(format!(
                "content-store thread ownership case {} changed across checkpoint/reopen",
                live.case_name
            )));
        }
    }
    for fixture in THREAD_MOVES {
        require_thread_state(
            &mut database,
            corpus,
            fixture,
            fixture.expected_final_space_id,
        )?;
    }
    let payload_sha256_after_reopen = thread_payload_sha256(&mut database)?;
    if payload_sha256_after_reopen != payload_sha256_before {
        return Err(HawDBError::Execution(
            "content-store reopened thread ownership move changed document or message payload fields"
                .to_string(),
        ));
    }

    let updated_count = THREAD_MOVES
        .iter()
        .filter(|fixture| fixture.initial_space_id == fixture.expected_space_id)
        .count();
    Ok((
        database,
        ContentStoreThreadOwnershipMoveQualificationReport {
            requested_moves: THREAD_MOVES.len(),
            documents_updated: updated_count,
            messages_updated: updated_count,
            document_owner_uses_storage_id: true,
            stale_guard_preserved: true,
            graph_relational_agreement: true,
            payload_fields_preserved: true,
            seed_commit_epoch,
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

fn seed_thread_ownership_rows(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<u64> {
    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let mut transaction = database.begin_transaction();
    for fixture in THREAD_MOVES {
        transaction.query_with_params(
            "CREATE (:Thread {id: $thread_id, space_id: $space_id, updated_at: $updated_at})",
            &seed_graph_parameters(fixture),
        )?;
        transaction.query_sql_with_params(&document.sql, &document_parameters(fixture))?;
        transaction.query_sql_with_params(&message.sql, &message_parameters(fixture))?;
    }
    transaction.commit()?;
    Ok(database.commit_epoch())
}

fn read_thread_pages(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    phase: ContentStoreRowPageReadPhase,
    expected_epoch: u64,
) -> Result<Vec<ContentStoreThreadOwnershipReadReport>> {
    let page = corpus_statement(corpus, "thread_messages_page")?;
    THREAD_MOVES
        .into_iter()
        .map(|fixture| {
            let read = execute_qualified_read(
                database,
                page,
                thread_page_parameters(fixture.storage_id),
                phase,
                1,
            )?;
            if read.execution.visible_commit_epoch != expected_epoch {
                return Err(HawDBError::Execution(format!(
                    "content-store thread ownership {} read observed epoch {}, expected {expected_epoch}",
                    fixture.storage_id, read.execution.visible_commit_epoch
                )));
            }
            Ok(ContentStoreThreadOwnershipReadReport {
                case_name: fixture.case_name.to_string(),
                expected_space_id: fixture.expected_final_space_id.to_string(),
                read,
            })
        })
        .collect()
}

fn require_thread_state(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    fixture: ThreadMoveFixture,
    expected_space_id: &str,
) -> Result<()> {
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.space_id AS space_id, t.updated_at AS updated_at",
        &thread_graph_parameters(fixture),
    )?;
    require_graph_state(
        &graph,
        expected_space_id,
        fixture.expected_updated_at_for(expected_space_id),
        "persisted",
    )?;
    let document = database.query_sql_with_params_options(
        DOCUMENT_STATE_SQL,
        &[Value::String(fixture.storage_id.to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
        },
    )?;
    require_document_relational_state(
        &document,
        fixture.storage_id,
        expected_space_id,
        fixture.expected_updated_at_for(expected_space_id),
        "persisted document",
    )?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let messages = database.query_sql_with_params_options(
        &page.sql,
        &thread_page_parameters(fixture.storage_id),
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_relational_state(
        &messages,
        expected_space_id,
        fixture.expected_updated_at_for(expected_space_id),
        "persisted message",
    )
}

fn thread_payload_sha256(database: &mut Database) -> Result<String> {
    let mut rows = Vec::with_capacity(THREAD_MOVES.len() * 2);
    for fixture in THREAD_MOVES {
        let document = database.query_sql_with_params_options(
            DOCUMENT_PAYLOAD_SQL,
            &[Value::String(fixture.storage_id.to_string())],
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
            },
        )?;
        let message = database.query_sql_with_params_options(
            MESSAGE_PAYLOAD_SQL,
            &[Value::String(fixture.storage_id.to_string()), Value::Int(1)],
            QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(PAYLOAD_MAX_BYTES),
            },
        )?;
        if document.rows.len() != 1 || message.rows.len() != 1 {
            return Err(HawDBError::Execution(format!(
                "content-store thread ownership payload probe expected one document and message for {}, got documents={} messages={}",
                fixture.thread_id,
                document.rows.len(),
                message.rows.len()
            )));
        }
        rows.extend(document.rows);
        rows.extend(message.rows);
    }
    Ok(rows_sha256(&rows))
}

fn require_graph_state(
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
            "content-store thread ownership {phase} graph expected space={expected_space_id}, updated_at={expected_updated_at}, got {rows:?}"
        ))),
    }
}

fn require_document_relational_state(
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
            "content-store thread ownership {phase} expected owner={expected_owner_id}, space={expected_space_id}, updated_at={expected_updated_at}, got {rows:?}"
        ))),
    }
}

fn require_relational_state(
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
            "content-store thread ownership {phase} expected space={expected_space_id}, updated_at={expected_updated_at}, got {rows:?}"
        ))),
    }
}

fn thread_page_parameters(storage_id: &str) -> Vec<Value> {
    vec![
        Value::String(storage_id.to_string()),
        Value::Int(1),
        Value::Int(0),
    ]
}

fn seed_graph_parameters(fixture: ThreadMoveFixture) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(fixture.thread_id.to_string()),
        ),
        (
            "space_id".to_string(),
            Value::String(fixture.initial_space_id.to_string()),
        ),
        (
            "updated_at".to_string(),
            Value::String(SEED_UPDATED_AT.to_string()),
        ),
    ])
}

fn thread_graph_parameters(fixture: ThreadMoveFixture) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(fixture.thread_id.to_string()),
    )])
}

fn move_graph_parameters(fixture: ThreadMoveFixture) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "thread_id".to_string(),
            Value::String(fixture.thread_id.to_string()),
        ),
        (
            "expected_space_id".to_string(),
            Value::String(fixture.expected_space_id.to_string()),
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

fn document_parameters(fixture: ThreadMoveFixture) -> Vec<Value> {
    vec![
        Value::String(fixture.document_id.to_string()),
        Value::String("thread".to_string()),
        Value::String(fixture.storage_id.to_string()),
        Value::String(fixture.initial_space_id.to_string()),
        Value::String("application/x-nowledge-thread".to_string()),
        Value::Int(1),
        Value::String(SEED_UPDATED_AT.to_string()),
        Value::String(SEED_UPDATED_AT.to_string()),
    ]
}

fn message_parameters(fixture: ThreadMoveFixture) -> Vec<Value> {
    vec![
        Value::String(fixture.content_message_id.to_string()),
        Value::String(fixture.message_id.to_string()),
        Value::String(fixture.storage_id.to_string()),
        Value::String(fixture.thread_id.to_string()),
        Value::String(fixture.document_id.to_string()),
        Value::String(fixture.initial_space_id.to_string()),
        Value::Int(0),
        Value::String("user".to_string()),
        Value::String(format!("ownership payload for {}", fixture.thread_id)),
        Value::String(SEED_UPDATED_AT.to_string()),
        Value::Int(4),
        Value::String(format!("{{\"fixture\":\"{}\"}}", fixture.thread_id)),
        Value::String(format!("external-{}", fixture.thread_id)),
        Value::Bool(false),
        Value::String(format!("hash-{}", fixture.thread_id)),
        Value::String(SEED_UPDATED_AT.to_string()),
        Value::String(SEED_UPDATED_AT.to_string()),
    ]
}

impl ThreadMoveFixture {
    fn expected_updated_at_for(self, space_id: &str) -> &'static str {
        if space_id == self.initial_space_id {
            SEED_UPDATED_AT
        } else {
            self.expected_updated_at
        }
    }
}
