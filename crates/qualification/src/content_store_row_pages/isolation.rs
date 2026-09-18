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

use super::evidence::{
    message_point_options, message_point_parameters, require_one_message,
    MESSAGE_POINT_MAX_PAYLOAD_BYTES, MESSAGE_POINT_MAX_ROWS, MESSAGE_POINT_SQL,
};
use super::fixture::{corpus_statement, thread_message_parameters};
use super::ContentStoreIsolationQualificationReport;
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{
    ConcurrentTransactionOptions, Database, HawDBError, Result, RuntimeCancellationToken,
    RuntimeTaskContext,
};
use std::time::Duration;

const MESSAGE_FOR_UPDATE_SQL: &str =
    "SELECT content_message_id FROM thread_messages WHERE content_message_id = $1 FOR UPDATE";
const OWNER_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
const WAITER_LOCK_TIMEOUT: Duration = Duration::from_millis(25);

pub(super) fn qualify_content_store_isolation(
    database: Database,
    corpus: &ContentStoreSqlCorpus,
    message_position: usize,
    payload_bytes: usize,
) -> Result<ContentStoreIsolationQualificationReport> {
    let content_message_id = format!("content-message-{message_position:08}");
    let point_parameters = message_point_parameters(&content_message_id);
    let point_options = message_point_options();

    let read = database.begin_read_transaction();
    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let cancelled_context = RuntimeTaskContext::without_deadline(cancellation);
    let cancellation_error = match read.query_sql_with_params_options_context(
        MESSAGE_POINT_SQL,
        &point_parameters,
        point_options,
        &cancelled_context,
    ) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "pre-cancelled Content Store point read completed".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !cancellation_error.to_string().contains("cancelled") {
        return Err(HawDBError::Execution(format!(
            "content-store isolation expected cancellation, got: {cancellation_error}"
        )));
    }

    let before_lock =
        read.query_sql_with_params_options(MESSAGE_POINT_SQL, &point_parameters, point_options)?;
    require_one_message(&before_lock.rows, &content_message_id, "isolation")?;
    let row_sha256 = rows_sha256(&before_lock.rows);
    let cancellation_pinned_bytes_after = database
        .segment_cache_snapshot()
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store isolation requires an out-of-core segment cache".to_string(),
            )
        })?
        .pinned_bytes;
    if cancellation_pinned_bytes_after != 0 {
        return Err(HawDBError::Execution(format!(
            "content-store cancelled read leaked {cancellation_pinned_bytes_after} pinned bytes"
        )));
    }
    drop(read);

    let concurrent = database.into_concurrent();
    let commit_epoch_before = concurrent.commit_epoch()?;
    let mut owner = concurrent.begin_transaction(ConcurrentTransactionOptions::pessimistic(
        OWNER_LOCK_TIMEOUT,
    ))?;
    let locked = owner.query_sql_with_params(MESSAGE_FOR_UPDATE_SQL, &point_parameters)?;
    require_one_message(&locked.rows, &content_message_id, "isolation")?;

    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let mut waiter = concurrent.begin_transaction(ConcurrentTransactionOptions::pessimistic(
        WAITER_LOCK_TIMEOUT,
    ))?;
    let waiter_error = match waiter.query_sql_with_params(
        &message.sql,
        &thread_message_parameters(message_position, payload_bytes, "lock-waiter"),
    ) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "same-key Content Store UPSERT bypassed the point lock".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !waiter_error
        .to_string()
        .contains("transaction lock wait timed out")
    {
        return Err(HawDBError::Execution(format!(
            "content-store isolation expected a bounded lock timeout, got: {waiter_error}"
        )));
    }
    let aborted_error = match waiter.query_sql_with_params(MESSAGE_POINT_SQL, &point_parameters) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "timed-out pessimistic transaction accepted another statement".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !aborted_error.to_string().contains("is aborted") {
        return Err(HawDBError::Execution(format!(
            "content-store isolation expected an aborted waiter, got: {aborted_error}"
        )));
    }
    drop(waiter);
    owner.rollback();

    let after_lock = concurrent.query_sql_with_params(MESSAGE_POINT_SQL, &point_parameters)?;
    require_one_message(&after_lock.rows, &content_message_id, "isolation")?;
    if rows_sha256(&after_lock.rows) != row_sha256 {
        return Err(HawDBError::Execution(
            "content-store timed-out UPSERT changed the locked row".to_string(),
        ));
    }
    let commit_epoch_after = concurrent.commit_epoch()?;
    if commit_epoch_after != commit_epoch_before {
        return Err(HawDBError::Execution(format!(
            "content-store isolation changed commit epoch from {commit_epoch_before} to {commit_epoch_after}"
        )));
    }

    Ok(ContentStoreIsolationQualificationReport {
        content_message_id,
        point_max_rows: MESSAGE_POINT_MAX_ROWS,
        point_max_payload_bytes: MESSAGE_POINT_MAX_PAYLOAD_BYTES,
        row_sha256,
        cancellation_non_poisoning: true,
        cancellation_pinned_bytes_after,
        waiter_lock_timeout_micros: u64::try_from(WAITER_LOCK_TIMEOUT.as_micros())
            .unwrap_or(u64::MAX),
        waiter_timed_out: true,
        waiter_aborted: true,
        owner_rollback_preserved_row: true,
        commit_epoch_before,
        commit_epoch_after,
    })
}
