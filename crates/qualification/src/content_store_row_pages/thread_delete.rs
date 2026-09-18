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
    thread_message_anchor_parameters, thread_message_parameters, ThreadMessageAnchorParameters,
    ThreadMessageParameters,
};
use super::{ContentStoreRowPageReadPhase, ContentStoreThreadDeleteQualificationReport};
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    Database, DatabaseConfig, DatabaseTransaction, DurabilityPolicy, HawDBError, QueryOutput,
    QueryStreamOptions, Result, Value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const THREAD_ID: &str = "qualified-delete-thread";
const THREAD_STORAGE_ID: &str = "qualified-delete-storage";
const REQUESTED_THREAD_ID: &str = "qualified-delete-alias";
const PUBLIC_IDENTITY_ID: &str = "qualified-delete-public";
const OWNED_DOCUMENT_ID: &str = "qualified-delete-doc-owned";
const LEGACY_DOCUMENT_ID: &str = "qualified-delete-doc-legacy";
const SPACE_ID: &str = "qualified-delete-space";
const OTHER_THREAD_ID: &str = "qualified-delete-other-thread";
const OTHER_THREAD_STORAGE_ID: &str = "qualified-delete-other-storage";
const OTHER_IDENTITY_ID: &str = "qualified-delete-other-public";
const OTHER_DOCUMENT_ID: &str = "qualified-delete-doc-other";
const CREATED_AT: &str = "2026-01-01T00:30:00Z";
const MEDIA_TYPE: &str = "application/vnd.nowledge.thread.messages+sqlite";
const STATE_MAX_ROWS: usize = 64;
const STATE_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

struct MessageSeed {
    content_message_id: &'static str,
    message_id: &'static str,
    graph_message_id: Option<&'static str>,
    content_document_id: &'static str,
    content: &'static str,
    order_index: usize,
}

const TARGET_MESSAGES: [MessageSeed; 3] = [
    MessageSeed {
        content_message_id: "qualified-delete-content-message-a",
        message_id: "qualified-delete-message-a",
        graph_message_id: Some("qualified-delete-graph-message-a"),
        content_document_id: LEGACY_DOCUMENT_ID,
        content: "first deleted payload",
        order_index: 0,
    },
    MessageSeed {
        content_message_id: "qualified-delete-content-message-b",
        message_id: "qualified-delete-message-b",
        graph_message_id: Some("qualified-delete-graph-message-b"),
        content_document_id: LEGACY_DOCUMENT_ID,
        content: "second deleted payload",
        order_index: 1,
    },
    MessageSeed {
        content_message_id: "qualified-delete-content-message-legacy",
        message_id: "qualified-delete-message-legacy",
        graph_message_id: None,
        content_document_id: LEGACY_DOCUMENT_ID,
        content: "legacy storage-only payload",
        order_index: 2,
    },
];

const OTHER_MESSAGE: MessageSeed = MessageSeed {
    content_message_id: "qualified-delete-other-content-message",
    message_id: "qualified-delete-other-message",
    graph_message_id: Some("qualified-delete-other-graph-message"),
    content_document_id: OTHER_DOCUMENT_ID,
    content: "unrelated retained payload",
    order_index: 0,
};

struct DeleteStageReport {
    document_ids: Vec<String>,
    graph_message_count: i64,
    relational_message_count: i64,
    anchor_count: i64,
    identity_count: i64,
}

pub(super) fn qualify_thread_delete(
    mut database: Database,
    database_path: &Path,
    database_config: &DatabaseConfig,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(Database, ContentStoreThreadDeleteQualificationReport)> {
    let (seed_commit_epoch, seed_checkpoint_generation) = seed_thread_delete(&mut database, corpus)
        .map_err(|error| delete_phase_error("seed", error))?;
    let target_state_before = target_state_sha256(&mut database)?;
    let unrelated_state_sha256_before = unrelated_state_sha256(&mut database)?;

    exercise_noop_delete(
        &mut database,
        corpus,
        "qualified-delete-missing-storage",
        "qualified-delete-missing-thread",
        "qualified-delete-missing-public",
        "qualified-delete-missing-alias",
    )?;

    let epoch_before_rollback = database.commit_epoch();
    let mut transaction = database.begin_transaction();
    stage_thread_delete(&mut transaction, corpus)?;
    transaction.rollback();
    if database.commit_epoch() != epoch_before_rollback
        || target_state_sha256(&mut database)? != target_state_before
        || unrelated_state_sha256(&mut database)? != unrelated_state_sha256_before
    {
        return Err(HawDBError::Execution(
            "content-store rolled-back whole-thread delete changed canonical state".to_string(),
        ));
    }

    let mut transaction = database.begin_transaction();
    let deleted = stage_thread_delete(&mut transaction, corpus)?;
    transaction
        .commit()
        .map_err(|error| delete_phase_error("commit publication", error))?;
    let committed_epoch = database.commit_epoch();
    if committed_epoch != seed_commit_epoch.saturating_add(1) {
        return Err(HawDBError::Execution(format!(
            "content-store whole-thread delete published epoch {committed_epoch}, expected {}",
            seed_commit_epoch.saturating_add(1)
        )));
    }

    require_target_absent_database(&mut database, corpus)?;
    let unrelated_state_sha256_after_live = unrelated_state_sha256(&mut database)?;
    if unrelated_state_sha256_after_live != unrelated_state_sha256_before {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete changed unrelated payloads".to_string(),
        ));
    }

    let tombstone_point =
        message_point_statement("thread_delete_deleted_message_point", "thread_delete");
    let deleted_tombstone_read = execute_qualified_read(
        &mut database,
        &tombstone_point,
        message_point_parameters(TARGET_MESSAGES[0].content_message_id).to_vec(),
        ContentStoreRowPageReadPhase::LiveOverlay,
        0,
    )?;
    if deleted_tombstone_read.execution.visible_commit_epoch != committed_epoch
        || deleted_tombstone_read.execution.index_runtime_path != "none"
        || deleted_tombstone_read.execution.overlay_entries == 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store whole-thread tombstone point read observed epoch {}, index path {}, and {} row overlay entries; expected epoch {committed_epoch}, a direct canonical point read, and a non-empty tombstone overlay",
            deleted_tombstone_read.execution.visible_commit_epoch,
            deleted_tombstone_read.execution.index_runtime_path,
            deleted_tombstone_read.execution.overlay_entries,
        )));
    }
    let count_statement = corpus_statement(corpus, "thread_message_count")?;
    let live_count_output = live_count_read_output(&mut database, count_statement)?;
    require_count(&live_count_output, 0)?;
    let live_count_sha256 = rows_sha256(&live_count_output.rows);

    database.checkpoint()?;
    let checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store whole-thread delete checkpoint published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    if checkpoint_generation <= seed_checkpoint_generation {
        return Err(HawDBError::Execution(format!(
            "content-store whole-thread delete checkpoint generation {checkpoint_generation} did not advance beyond seed generation {seed_checkpoint_generation}"
        )));
    }
    drop(database);

    let mut database = Database::open_with_durability_and_config(
        database_path,
        DurabilityPolicy::SyncOnEveryWrite,
        database_config.clone(),
    )?;
    require_target_absent_database(&mut database, corpus)?;
    let unrelated_state_sha256_after_reopen = unrelated_state_sha256(&mut database)?;
    if unrelated_state_sha256_after_reopen != unrelated_state_sha256_before {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete changed unrelated payloads after reopen".to_string(),
        ));
    }
    let reopened_count_output = live_count_read_output(&mut database, count_statement)?;
    require_count(&reopened_count_output, 0)?;
    let reopened_count_sha256 = rows_sha256(&reopened_count_output.rows);
    if reopened_count_sha256 != live_count_sha256 || database.commit_epoch() != committed_epoch {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete changed across checkpoint/reopen".to_string(),
        ));
    }

    exercise_noop_delete(
        &mut database,
        corpus,
        THREAD_STORAGE_ID,
        THREAD_ID,
        PUBLIC_IDENTITY_ID,
        REQUESTED_THREAD_ID,
    )?;

    Ok((
        database,
        ContentStoreThreadDeleteQualificationReport {
            thread_id: THREAD_ID.to_string(),
            thread_storage_id: THREAD_STORAGE_ID.to_string(),
            requested_thread_id: REQUESTED_THREAD_ID.to_string(),
            discovered_document_sha256: string_values_sha256(&deleted.document_ids),
            discovered_document_ids: deleted.document_ids,
            empty_owned_document_discovered: true,
            graph_deleted_message_count: deleted.graph_message_count,
            relational_deleted_message_count: deleted.relational_message_count,
            deleted_anchor_count: deleted.anchor_count,
            deleted_identity_count: deleted.identity_count,
            missing_thread_noop: true,
            rollback_preserved_state: true,
            workspace_read_your_own_writes: true,
            graph_relational_absent: true,
            unrelated_payload_identity: true,
            second_delete_noop: true,
            seed_commit_epoch,
            seed_checkpoint_generation,
            committed_epoch,
            unrelated_state_sha256_before,
            unrelated_state_sha256_after_live,
            unrelated_state_sha256_after_reopen,
            deleted_tombstone_read,
            live_count_sha256,
            checkpoint_generation,
            reopened_count_sha256,
        },
    ))
}

fn seed_thread_delete(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<(u64, u64)> {
    let document = corpus_statement(corpus, "upsert_content_document")?;
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let anchor = corpus_statement(corpus, "upsert_memory_message_anchor")?;
    let mut transaction = database.begin_transaction();

    seed_graph_thread(
        &mut transaction,
        THREAD_ID,
        &[PUBLIC_IDENTITY_ID, REQUESTED_THREAD_ID],
        &TARGET_MESSAGES,
    )?;
    seed_graph_thread(
        &mut transaction,
        OTHER_THREAD_ID,
        &[OTHER_IDENTITY_ID],
        std::slice::from_ref(&OTHER_MESSAGE),
    )?;

    transaction.query_sql_with_params(
        &document.sql,
        &document_parameters(OWNED_DOCUMENT_ID, "thread", THREAD_STORAGE_ID),
    )?;
    transaction.query_sql_with_params(
        &document.sql,
        &document_parameters(LEGACY_DOCUMENT_ID, "legacy_thread", THREAD_STORAGE_ID),
    )?;
    transaction.query_sql_with_params(
        &document.sql,
        &document_parameters(OTHER_DOCUMENT_ID, "thread", OTHER_THREAD_STORAGE_ID),
    )?;

    for seed in &TARGET_MESSAGES {
        transaction.query_sql_with_params(
            &message.sql,
            &message_parameters(seed, THREAD_STORAGE_ID, THREAD_ID),
        )?;
    }
    transaction.query_sql_with_params(
        &message.sql,
        &message_parameters(&OTHER_MESSAGE, OTHER_THREAD_STORAGE_ID, OTHER_THREAD_ID),
    )?;

    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            "qualified-delete-anchor-owned",
            "qualified-delete-memory-owned",
            LEGACY_DOCUMENT_ID,
            THREAD_STORAGE_ID,
            &TARGET_MESSAGES[0],
        ),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            "qualified-delete-anchor-legacy",
            "qualified-delete-memory-legacy",
            LEGACY_DOCUMENT_ID,
            THREAD_STORAGE_ID,
            &TARGET_MESSAGES[2],
        ),
    )?;
    transaction.query_sql_with_params(
        &anchor.sql,
        &anchor_parameters(
            "qualified-delete-anchor-other",
            "qualified-delete-memory-other",
            OTHER_DOCUMENT_ID,
            OTHER_THREAD_STORAGE_ID,
            &OTHER_MESSAGE,
        ),
    )?;

    for content_document_id in [OWNED_DOCUMENT_ID, LEGACY_DOCUMENT_ID, OTHER_DOCUMENT_ID] {
        update_document_summary(&mut transaction, corpus, content_document_id)?;
    }
    transaction.commit()?;
    let seed_commit_epoch = database.commit_epoch();
    database.checkpoint()?;
    let seed_checkpoint_generation = database
        .relational_index_shadow_checkpoint_report()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store whole-thread delete seed published no relational generation"
                    .to_string(),
            )
        })?
        .generation;
    Ok((seed_commit_epoch, seed_checkpoint_generation))
}

fn seed_graph_thread(
    transaction: &mut DatabaseTransaction<'_>,
    thread_id: &str,
    identity_ids: &[&str],
    messages: &[MessageSeed],
) -> Result<()> {
    transaction.query_with_params(
        "CREATE (:Thread {id: $thread_id, marker: $marker})",
        &BTreeMap::from([
            (
                "thread_id".to_string(),
                Value::String(thread_id.to_string()),
            ),
            (
                "marker".to_string(),
                Value::String("retained-marker".to_string()),
            ),
        ]),
    )?;
    for identity_id in identity_ids {
        transaction.query_with_params(
            "CREATE (:ThreadIdentity {id: $identity_id, thread_node_id: $thread_id})",
            &BTreeMap::from([
                (
                    "identity_id".to_string(),
                    Value::String((*identity_id).to_string()),
                ),
                (
                    "thread_id".to_string(),
                    Value::String(thread_id.to_string()),
                ),
            ]),
        )?;
    }
    for message in messages {
        let Some(graph_message_id) = message.graph_message_id else {
            continue;
        };
        transaction.query_with_params(
            "CREATE (:Message {id: $message_id, marker: $marker})",
            &BTreeMap::from([
                (
                    "message_id".to_string(),
                    Value::String(graph_message_id.to_string()),
                ),
                (
                    "marker".to_string(),
                    Value::String("retained-marker".to_string()),
                ),
            ]),
        )?;
        transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}), (m:Message {id: $message_id}) CREATE (t)-[:CONTAINS]->(m)",
            &BTreeMap::from([
                ("thread_id".to_string(), Value::String(thread_id.to_string())),
                (
                    "message_id".to_string(),
                    Value::String(graph_message_id.to_string()),
                ),
            ]),
        )?;
    }
    Ok(())
}

fn update_document_summary(
    transaction: &mut DatabaseTransaction<'_>,
    corpus: &ContentStoreSqlCorpus,
    content_document_id: &str,
) -> Result<()> {
    let summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update = corpus_statement(corpus, "update_content_document_summary")?;
    let output = transaction.query_sql_with_params(
        &summary.sql,
        &[Value::String(content_document_id.to_string())],
    )?;
    transaction.query_sql_with_params(
        &update.sql,
        &[
            Value::Int(required_count(&output, "item_count")?),
            Value::Int(required_count(&output, "size_bytes")?),
            Value::String(CREATED_AT.to_string()),
            Value::String(content_document_id.to_string()),
        ],
    )?;
    Ok(())
}

fn stage_thread_delete(
    transaction: &mut DatabaseTransaction<'_>,
    corpus: &ContentStoreSqlCorpus,
) -> Result<DeleteStageReport> {
    let owned = corpus_statement(corpus, "thread_owned_document_ids")?;
    let message_documents = corpus_statement(corpus, "thread_message_document_ids")?;
    let count = corpus_statement(corpus, "thread_message_count")?;
    let delete_anchors = corpus_statement(corpus, "delete_anchors_by_document")?;
    let delete_messages = corpus_statement(corpus, "delete_messages_by_thread")?;
    let delete_document = corpus_statement(corpus, "delete_content_document_by_id")?;

    let owned_output = transaction
        .query_sql_with_params(&owned.sql, &[Value::String(THREAD_STORAGE_ID.to_string())])?;
    let message_document_output = transaction.query_sql_with_params(
        &message_documents.sql,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
    )?;
    if output_document_ids(&owned_output)? != [OWNED_DOCUMENT_ID.to_string()]
        || output_document_ids(&message_document_output)? != [LEGACY_DOCUMENT_ID.to_string()]
    {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete did not distinguish the empty owned document from the message-only legacy document".to_string(),
        ));
    }
    let document_ids = merge_document_ids(&owned_output, &message_document_output)?;
    if document_ids
        != [
            LEGACY_DOCUMENT_ID.to_string(),
            OWNED_DOCUMENT_ID.to_string(),
        ]
    {
        return Err(HawDBError::Execution(format!(
            "content-store whole-thread delete discovered unexpected documents {document_ids:?}"
        )));
    }
    let relational_message_count = required_count(
        &transaction
            .query_sql_with_params(&count.sql, &[Value::String(THREAD_STORAGE_ID.to_string())])?,
        "message_count",
    )?;
    let graph_message_count = required_count(
        &transaction.query_with_params(
            "MATCH (t:Thread {id: $thread_id}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m) AS message_count",
            &thread_identity_parameters(),
        )?,
        "message_count",
    )?;
    let identity_count = required_count(
        &transaction.query_with_params(
            "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_id RETURN COUNT(ti) AS identity_count",
            &delete_identity_parameters(),
        )?,
        "identity_count",
    )?;
    let mut anchor_count = 0i64;
    for document_id in &document_ids {
        anchor_count = anchor_count
            .checked_add(required_count(
                &transaction.query_sql_with_params(
                    "SELECT COUNT(*) AS anchor_count FROM content_anchors WHERE content_doc_id = $1",
                    &[Value::String(document_id.clone())],
                )?,
                "anchor_count",
            )?)
            .ok_or_else(|| HawDBError::Execution("anchor count overflow".to_string()))?;
    }

    run_graph_deletes(
        transaction,
        THREAD_ID,
        PUBLIC_IDENTITY_ID,
        REQUESTED_THREAD_ID,
    )?;
    for document_id in &document_ids {
        transaction
            .query_sql_with_params(&delete_anchors.sql, &[Value::String(document_id.clone())])?;
    }
    transaction.query_sql_with_params(
        &delete_messages.sql,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
    )?;
    for document_id in &document_ids {
        transaction
            .query_sql_with_params(&delete_document.sql, &[Value::String(document_id.clone())])?;
    }
    require_target_absent_transaction(transaction, corpus)?;
    require_unrelated_transaction(transaction)?;

    Ok(DeleteStageReport {
        document_ids,
        graph_message_count,
        relational_message_count,
        anchor_count,
        identity_count,
    })
}

fn run_graph_deletes(
    transaction: &mut DatabaseTransaction<'_>,
    thread_id: &str,
    public_thread_id: &str,
    input_thread_id: &str,
) -> Result<()> {
    let thread = BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(thread_id.to_string()),
    )]);
    transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id})-[:CONTAINS]->(m:Message) DETACH DELETE m",
        &thread,
    )?;
    transaction.query_with_params(
        "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_id DETACH DELETE ti",
        &BTreeMap::from([
            (
                "public_thread_id".to_string(),
                Value::String(public_thread_id.to_string()),
            ),
            (
                "input_thread_id".to_string(),
                Value::String(input_thread_id.to_string()),
            ),
            ("thread_id".to_string(), Value::String(thread_id.to_string())),
        ]),
    )?;
    transaction.query_with_params("MATCH (t:Thread {id: $thread_id}) DETACH DELETE t", &thread)?;
    Ok(())
}

fn exercise_noop_delete(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    thread_storage_id: &str,
    thread_id: &str,
    public_thread_id: &str,
    input_thread_id: &str,
) -> Result<()> {
    let epoch_before = database.commit_epoch();
    let owned = corpus_statement(corpus, "thread_owned_document_ids")?;
    let message_documents = corpus_statement(corpus, "thread_message_document_ids")?;
    let owned_output = database
        .query_sql_with_params(&owned.sql, &[Value::String(thread_storage_id.to_string())])?;
    let message_output = database.query_sql_with_params(
        &message_documents.sql,
        &[Value::String(thread_storage_id.to_string())],
    )?;
    if !owned_output.rows.is_empty() || !message_output.rows.is_empty() {
        return Err(HawDBError::Execution(format!(
            "content-store no-op whole-thread delete discovered documents for {thread_storage_id}"
        )));
    }

    let count = corpus_statement(corpus, "thread_message_count")?;
    require_count(
        &database
            .query_sql_with_params(&count.sql, &[Value::String(thread_storage_id.to_string())])?,
        0,
    )?;
    let graph_thread = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id",
        &BTreeMap::from([(
            "thread_id".to_string(),
            Value::String(thread_id.to_string()),
        )]),
    )?;
    let graph_identities = database.query_with_params(
        "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_id RETURN COUNT(ti) AS identity_count",
        &BTreeMap::from([
            (
                "public_thread_id".to_string(),
                Value::String(public_thread_id.to_string()),
            ),
            (
                "input_thread_id".to_string(),
                Value::String(input_thread_id.to_string()),
            ),
            ("thread_id".to_string(), Value::String(thread_id.to_string())),
        ]),
    )?;
    if !graph_thread.rows.is_empty() || required_count(&graph_identities, "identity_count")? != 0 {
        return Err(HawDBError::Execution(format!(
            "content-store no-op whole-thread delete discovered graph state for {thread_id}"
        )));
    }
    if database.commit_epoch() != epoch_before {
        return Err(HawDBError::Execution(format!(
            "content-store no-op whole-thread preflight advanced epoch from {epoch_before} to {}",
            database.commit_epoch()
        )));
    }
    Ok(())
}

fn require_target_absent_transaction(
    transaction: &mut DatabaseTransaction<'_>,
    corpus: &ContentStoreSqlCorpus,
) -> Result<()> {
    let count = corpus_statement(corpus, "thread_message_count")?;
    require_count(
        &transaction
            .query_sql_with_params(&count.sql, &[Value::String(THREAD_STORAGE_ID.to_string())])?,
        0,
    )?;
    let documents = transaction.query_sql_with_params(
        "SELECT content_doc_id FROM content_documents WHERE content_doc_id = $1 OR content_doc_id = $2 ORDER BY content_doc_id ASC",
        &[
            Value::String(OWNED_DOCUMENT_ID.to_string()),
            Value::String(LEGACY_DOCUMENT_ID.to_string()),
        ],
    )?;
    let anchors = transaction.query_sql_with_params(
        "SELECT anchor_id FROM content_anchors WHERE content_doc_id = $1 OR content_doc_id = $2 ORDER BY anchor_id ASC",
        &[
            Value::String(OWNED_DOCUMENT_ID.to_string()),
            Value::String(LEGACY_DOCUMENT_ID.to_string()),
        ],
    )?;
    if !documents.rows.is_empty() || !anchors.rows.is_empty() {
        return Err(HawDBError::Execution(
            "content-store whole-thread workspace retained documents or anchors".to_string(),
        ));
    }
    require_target_graph_absent(transaction)
}

fn require_target_graph_absent(transaction: &mut DatabaseTransaction<'_>) -> Result<()> {
    let thread = transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id",
        &thread_identity_parameters(),
    )?;
    let identities = transaction.query_with_params(
        "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_id RETURN COUNT(ti) AS identity_count",
        &delete_identity_parameters(),
    )?;
    let messages = transaction.query_with_params(
        "MATCH (m:Message) WHERE m.id = $message_a OR m.id = $message_b RETURN COUNT(m) AS message_count",
        &target_graph_message_parameters(),
    )?;
    if !thread.rows.is_empty()
        || required_count(&identities, "identity_count")? != 0
        || required_count(&messages, "message_count")? != 0
    {
        return Err(HawDBError::Execution(
            "content-store whole-thread workspace retained graph identity".to_string(),
        ));
    }
    Ok(())
}

fn require_unrelated_transaction(transaction: &mut DatabaseTransaction<'_>) -> Result<()> {
    let relational = transaction.query_sql_with_params(
        "SELECT content_message_id, content, content_doc_id FROM thread_messages WHERE thread_storage_id = $1",
        &[Value::String(OTHER_THREAD_STORAGE_ID.to_string())],
    )?;
    let graph = transaction.query_with_params(
        "MATCH (t:Thread {id: $thread_id})-[:CONTAINS]->(m:Message) RETURN t.id AS thread_id, m.id AS message_id, t.marker AS thread_marker, m.marker AS message_marker",
        &BTreeMap::from([(
            "thread_id".to_string(),
            Value::String(OTHER_THREAD_ID.to_string()),
        )]),
    )?;
    if relational.rows.len() != 1 || graph.rows.len() != 1 {
        return Err(HawDBError::Execution(
            "content-store whole-thread workspace changed unrelated state".to_string(),
        ));
    }
    Ok(())
}

fn require_target_absent_database(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
) -> Result<()> {
    let count = corpus_statement(corpus, "thread_message_count")?;
    require_count(
        &database.query_sql_with_params_options(
            &count.sql,
            &[Value::String(THREAD_STORAGE_ID.to_string())],
            state_query_options(1),
        )?,
        0,
    )?;
    let output = database.query_sql_with_params_options(
        "SELECT content_doc_id FROM content_documents WHERE content_doc_id = $1 OR content_doc_id = $2 ORDER BY content_doc_id ASC",
        &[
            Value::String(OWNED_DOCUMENT_ID.to_string()),
            Value::String(LEGACY_DOCUMENT_ID.to_string()),
        ],
        state_query_options(2),
    )?;
    if !output.rows.is_empty() {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete retained target documents".to_string(),
        ));
    }
    let graph = database.query_with_params(
        "MATCH (t:Thread {id: $thread_id}) RETURN t.id AS id",
        &thread_identity_parameters(),
    )?;
    if !graph.rows.is_empty() {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete retained target graph Thread".to_string(),
        ));
    }
    Ok(())
}

fn target_state_sha256(database: &mut Database) -> Result<String> {
    let mut rows = Vec::new();
    rows.extend(
        database
            .query_sql_with_params_options(
                "SELECT content_doc_id, owner_kind, owner_id, item_count, size_bytes FROM content_documents WHERE content_doc_id = $1 OR content_doc_id = $2 ORDER BY content_doc_id ASC",
                &[
                    Value::String(OWNED_DOCUMENT_ID.to_string()),
                    Value::String(LEGACY_DOCUMENT_ID.to_string()),
                ],
                state_query_options(2),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_sql_with_params_options(
                "SELECT content_message_id, message_id, content_doc_id, content, order_index, content_hash FROM thread_messages WHERE thread_storage_id = $1 ORDER BY order_index ASC, content_message_id ASC",
                &[Value::String(THREAD_STORAGE_ID.to_string())],
                state_query_options(TARGET_MESSAGES.len()),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_sql_with_params_options(
                "SELECT anchor_id, content_doc_id, content_message_id, message_id, order_index, metadata_json FROM content_anchors WHERE content_doc_id = $1 OR content_doc_id = $2 ORDER BY anchor_id ASC",
                &[
                    Value::String(OWNED_DOCUMENT_ID.to_string()),
                    Value::String(LEGACY_DOCUMENT_ID.to_string()),
                ],
                state_query_options(2),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_with_params(
                "MATCH (t:Thread {id: $thread_id}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN t.id AS thread_id, COUNT(m) AS message_count",
                &thread_identity_parameters(),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_with_params(
                "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_id RETURN COUNT(ti) AS identity_count",
                &delete_identity_parameters(),
            )?
            .rows,
    );
    Ok(rows_sha256(&rows))
}

fn unrelated_state_sha256(database: &mut Database) -> Result<String> {
    let mut rows = database
        .query_sql_with_params_options(
            "SELECT m.content_message_id, m.message_id, m.content, m.content_hash, d.content_doc_id, d.owner_id, d.item_count, d.size_bytes FROM thread_messages AS m INNER JOIN content_documents AS d ON d.content_doc_id = m.content_doc_id WHERE m.thread_storage_id = $1",
            &[Value::String(OTHER_THREAD_STORAGE_ID.to_string())],
            state_query_options(1),
        )?
        .rows
        .into_iter()
        .collect::<Vec<_>>();
    rows.extend(
        database
            .query_sql_with_params_options(
                "SELECT anchor_id, content_message_id, message_id, metadata_json FROM content_anchors WHERE content_doc_id = $1",
                &[Value::String(OTHER_DOCUMENT_ID.to_string())],
                state_query_options(1),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_with_params(
                "MATCH (t:Thread {id: $thread_id})-[:CONTAINS]->(m:Message) RETURN t.id AS thread_id, m.id AS message_id, t.marker AS thread_marker, m.marker AS message_marker",
                &BTreeMap::from([(
                    "thread_id".to_string(),
                    Value::String(OTHER_THREAD_ID.to_string()),
                )]),
            )?
            .rows,
    );
    rows.extend(
        database
            .query_with_params(
                "MATCH (ti:ThreadIdentity {id: $identity_id}) RETURN ti.id AS identity_id, ti.thread_node_id AS thread_id",
                &BTreeMap::from([(
                    "identity_id".to_string(),
                    Value::String(OTHER_IDENTITY_ID.to_string()),
                )]),
            )?
            .rows,
    );
    Ok(rows_sha256(&rows))
}

fn merge_document_ids(owned: &QueryOutput, messages: &QueryOutput) -> Result<Vec<String>> {
    let document_ids = output_document_ids(owned)?
        .into_iter()
        .chain(output_document_ids(messages)?)
        .collect::<BTreeSet<_>>();
    if document_ids.len() > 32 {
        return Err(HawDBError::Execution(
            "content-store whole-thread delete document union exceeded 32 entries".to_string(),
        ));
    }
    Ok(document_ids.into_iter().collect())
}

fn output_document_ids(output: &QueryOutput) -> Result<Vec<String>> {
    let mut document_ids = Vec::with_capacity(output.rows.len());
    for row in &output.rows {
        match row.get("content_doc_id") {
            Some(Value::String(value)) => {
                document_ids.push(value.clone());
            }
            other => {
                return Err(HawDBError::Execution(format!(
                    "content-store whole-thread delete expected document identity, got {other:?}"
                )));
            }
        }
    }
    Ok(document_ids)
}

fn live_count_read_output(
    database: &mut Database,
    statement: &crate::ContentStoreSqlStatementSpec,
) -> Result<QueryOutput> {
    database.query_sql_with_params_options(
        &statement.sql,
        &[Value::String(THREAD_STORAGE_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(statement.max_rows),
            max_payload_bytes: Some(statement.max_payload_bytes),
        },
    )
}

fn require_count(output: &QueryOutput, expected: i64) -> Result<()> {
    let actual = required_count(output, "message_count")?;
    if actual != expected {
        return Err(HawDBError::Execution(format!(
            "content-store whole-thread delete expected message_count={expected}, got {actual}"
        )));
    }
    Ok(())
}

fn required_count(output: &QueryOutput, field: &str) -> Result<i64> {
    match output.rows.as_slice() {
        [row] => match row.get(field) {
            Some(Value::Int(value)) => Ok(*value),
            other => Err(HawDBError::Execution(format!(
                "content-store whole-thread delete expected integer {field}, got {other:?}"
            ))),
        },
        rows => Err(HawDBError::Execution(format!(
            "content-store whole-thread delete expected one {field} row, got {rows:?}"
        ))),
    }
}

fn document_parameters(content_document_id: &str, owner_kind: &str, owner_id: &str) -> Vec<Value> {
    vec![
        Value::String(content_document_id.to_string()),
        Value::String(owner_kind.to_string()),
        Value::String(owner_id.to_string()),
        Value::String(SPACE_ID.to_string()),
        Value::String(MEDIA_TYPE.to_string()),
        Value::Int(1),
        Value::String(CREATED_AT.to_string()),
        Value::String(CREATED_AT.to_string()),
    ]
}

fn message_parameters(seed: &MessageSeed, thread_storage_id: &str, thread_id: &str) -> Vec<Value> {
    let order_index = i64::try_from(seed.order_index).unwrap_or(i64::MAX);
    let metadata_json = format!("{{\"delete\":\"{}\"}}", seed.content_message_id);
    let external_id = format!("external-{}", seed.message_id);
    let content_hash = format!("hash-{}", seed.message_id);
    thread_message_parameters(ThreadMessageParameters {
        content_message_id: seed.content_message_id,
        message_id: seed.message_id,
        thread_storage_id,
        thread_id,
        content_document_id: seed.content_document_id,
        space_id: SPACE_ID,
        order_index,
        role: "user",
        content: seed.content,
        timestamp: CREATED_AT,
        token_count: 4 + order_index,
        metadata_json: &metadata_json,
        external_id: &external_id,
        exclude_from_distillation: false,
        content_hash: &content_hash,
        created_at: CREATED_AT,
        updated_at: CREATED_AT,
    })
}

fn anchor_parameters(
    anchor_id: &str,
    memory_id: &str,
    content_document_id: &str,
    thread_storage_id: &str,
    message: &MessageSeed,
) -> Vec<Value> {
    let quote_hash = format!("quote-{anchor_id}");
    let metadata_json = format!("{{\"delete_anchor\":\"{anchor_id}\"}}");
    thread_message_anchor_parameters(ThreadMessageAnchorParameters {
        anchor_id,
        memory_id,
        content_document_id,
        thread_storage_id,
        content_message_id: Some(message.content_message_id),
        message_id: message.message_id,
        order_index: i64::try_from(message.order_index).unwrap_or(i64::MAX),
        quote_hash: &quote_hash,
        metadata_json: &metadata_json,
        created_at: CREATED_AT,
    })
}

fn thread_identity_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "thread_id".to_string(),
        Value::String(THREAD_ID.to_string()),
    )])
}

fn delete_identity_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "public_thread_id".to_string(),
            Value::String(PUBLIC_IDENTITY_ID.to_string()),
        ),
        (
            "input_thread_id".to_string(),
            Value::String(REQUESTED_THREAD_ID.to_string()),
        ),
        (
            "thread_id".to_string(),
            Value::String(THREAD_ID.to_string()),
        ),
    ])
}

fn target_graph_message_parameters() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "message_a".to_string(),
            Value::String(
                TARGET_MESSAGES[0]
                    .graph_message_id
                    .expect("target graph message")
                    .to_string(),
            ),
        ),
        (
            "message_b".to_string(),
            Value::String(
                TARGET_MESSAGES[1]
                    .graph_message_id
                    .expect("target graph message")
                    .to_string(),
            ),
        ),
    ])
}

fn string_values_sha256(values: &[String]) -> String {
    let rows = values
        .iter()
        .map(|value| {
            let mut row = hawdb::Row::new();
            row.insert("value".to_string(), Value::String(value.clone()));
            row
        })
        .collect::<Vec<_>>();
    rows_sha256(&rows)
}

fn state_query_options(max_rows: usize) -> QueryStreamOptions {
    QueryStreamOptions {
        max_rows: Some(max_rows.min(STATE_MAX_ROWS)),
        max_payload_bytes: Some(STATE_MAX_PAYLOAD_BYTES),
    }
}

fn delete_phase_error(phase: &str, error: HawDBError) -> HawDBError {
    HawDBError::Execution(format!(
        "content-store whole-thread delete {phase} failed: {error}"
    ))
}
