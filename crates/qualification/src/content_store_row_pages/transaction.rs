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

use super::evidence::info_field;
use super::fixture::{
    corpus_statement, thread_message_parameters, thread_page_parameters, THREAD_DOCUMENT_ID,
};
use super::ContentStoreTransactionQualificationReport;
use crate::evidence_digest::rows_sha256;
use crate::ContentStoreSqlCorpus;
use hawdb::{Database, HawDBError, QueryOutput, QueryStreamOptions, Result, Value};

const SUMMARY_VERIFY_SQL: &str =
    "SELECT item_count, size_bytes FROM content_documents WHERE content_doc_id = $1";

pub(super) fn qualify_multi_statement_transaction(
    database: &mut Database,
    corpus: &ContentStoreSqlCorpus,
    message_position: usize,
    payload_bytes: usize,
) -> Result<ContentStoreTransactionQualificationReport> {
    let message = corpus_statement(corpus, "upsert_thread_message")?;
    let page = corpus_statement(corpus, "thread_messages_page")?;
    let payload_summary = corpus_statement(corpus, "thread_document_payload_summary")?;
    let update_summary = corpus_statement(corpus, "update_content_document_summary")?;
    let expected_rows = message_position.checked_add(1).ok_or_else(|| {
        HawDBError::Semantic("content-store transaction message count overflowed usize".to_string())
    })?;

    let inserted_content_message_id = format!("content-message-{message_position:08}");
    let page_parameters = thread_page_parameters(expected_rows);
    let mut transaction = database.begin_transaction();
    transaction.query_sql_with_params(
        &message.sql,
        &thread_message_parameters(message_position, payload_bytes, "transaction"),
    )?;

    let before_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    require_bounded_rows(
        page.name.as_str(),
        page.max_rows,
        page.max_payload_bytes,
        expected_rows,
        &before_rejection,
    )?;

    let mut rejected_parameters =
        thread_message_parameters(expected_rows, payload_bytes, "rejected");
    let document_id = rejected_parameters.get_mut(4).ok_or_else(|| {
        HawDBError::Execution(
            "content-store transaction message fixture has no document-id parameter".to_string(),
        )
    })?;
    *document_id = Value::String("missing-content-document".to_string());
    let rejection = match transaction.query_sql_with_params(&message.sql, &rejected_parameters) {
        Ok(_) => {
            return Err(HawDBError::Execution(
                "content-store transaction admitted a missing-document foreign key".to_string(),
            ));
        }
        Err(error) => error,
    };
    if !rejection.to_string().contains("foreign key") {
        return Err(HawDBError::Execution(format!(
            "content-store transaction expected a foreign-key rejection, got: {rejection}"
        )));
    }
    let after_rejection = transaction.query_sql_with_params(&page.sql, &page_parameters)?;
    let before_sha256 = rows_sha256(&before_rejection.rows);
    let after_sha256 = rows_sha256(&after_rejection.rows);
    if before_sha256 != after_sha256 {
        return Err(HawDBError::Execution(
            "content-store rejected statement changed the transaction workspace".to_string(),
        ));
    }

    let summary = transaction.query_sql_with_params(
        &payload_summary.sql,
        &[Value::String(THREAD_DOCUMENT_ID.to_string())],
    )?;
    let summary_item_count = required_i64(&summary, "item_count")?;
    let summary_size_bytes = required_i64(&summary, "size_bytes")?;
    let expected_item_count = i64::try_from(expected_rows).map_err(|_| {
        HawDBError::Semantic(
            "content-store transaction message count does not fit BIGINT".to_string(),
        )
    })?;
    if summary_item_count != expected_item_count {
        return Err(HawDBError::Execution(format!(
            "content-store transaction summary counted {summary_item_count} items, expected {expected_rows}"
        )));
    }
    transaction.query_sql_with_params(
        &update_summary.sql,
        &[
            Value::Int(summary_item_count),
            Value::Int(summary_size_bytes),
            Value::String("2026-01-01T00:01:00Z".to_string()),
            Value::String(THREAD_DOCUMENT_ID.to_string()),
        ],
    )?;

    let explain = transaction
        .query_sql_with_params(&format!("EXPLAIN ANALYZE {}", page.sql), &page_parameters)?;
    let paths = transaction_runtime_paths(&explain)?;
    transaction.commit()?;
    let committed_epoch = database.commit_epoch();

    let persisted_page = database.query_sql_with_params_options(
        &page.sql,
        &page_parameters,
        QueryStreamOptions {
            max_rows: Some(page.max_rows),
            max_payload_bytes: Some(page.max_payload_bytes),
        },
    )?;
    require_bounded_rows(
        page.name.as_str(),
        page.max_rows,
        page.max_payload_bytes,
        expected_rows,
        &persisted_page,
    )?;
    if rows_sha256(&persisted_page.rows) != before_sha256 {
        return Err(HawDBError::Execution(
            "content-store transaction page was not published atomically".to_string(),
        ));
    }
    let persisted_summary = database.query_sql_with_params_options(
        SUMMARY_VERIFY_SQL,
        &[Value::String(THREAD_DOCUMENT_ID.to_string())],
        QueryStreamOptions {
            max_rows: Some(1),
            max_payload_bytes: Some(4 * 1024),
        },
    )?;
    if required_i64(&persisted_summary, "item_count")? != summary_item_count
        || required_i64(&persisted_summary, "size_bytes")? != summary_size_bytes
    {
        return Err(HawDBError::Execution(
            "content-store transaction summary was not published atomically".to_string(),
        ));
    }

    Ok(ContentStoreTransactionQualificationReport {
        inserted_content_message_id,
        page_output_rows: before_rejection.rows.len(),
        page_output_sha256: before_sha256,
        summary_item_count,
        summary_size_bytes,
        index_runtime_path: paths.index_runtime_path,
        row_runtime_path: paths.row_runtime_path,
        transaction_workspace_lookups: paths.transaction_workspace_lookups,
        canonical_fallback_lookups: paths.canonical_fallback_lookups,
        rejected_statement_atomic: true,
        committed_epoch,
    })
}

struct TransactionRuntimePaths {
    index_runtime_path: String,
    row_runtime_path: String,
    transaction_workspace_lookups: u64,
    canonical_fallback_lookups: u64,
}

fn transaction_runtime_paths(explain: &QueryOutput) -> Result<TransactionRuntimePaths> {
    let info = explain
        .rows
        .iter()
        .find_map(|row| match row.get("operator info") {
            Some(Value::String(info)) if info.contains("row_runtime_path=") => Some(info.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            HawDBError::Execution(
                "content-store transaction EXPLAIN has no row/index execution evidence".to_string(),
            )
        })?;
    let index_runtime_path = required_info_field(info, "runtime_path")?;
    let row_runtime_path = required_info_field(info, "row_runtime_path")?;
    let transaction_workspace_lookups = required_info_u64(info, "transaction_workspace")?;
    let canonical_fallback_lookups = required_info_u64(info, "canonical_fallback")?;
    if index_runtime_path != "transaction_workspace"
        || row_runtime_path != "snapshot_rows"
        || transaction_workspace_lookups == 0
        || canonical_fallback_lookups != 0
    {
        return Err(HawDBError::Execution(format!(
            "content-store transaction used index={index_runtime_path}, row={row_runtime_path}, transaction_workspace={transaction_workspace_lookups}, canonical_fallback={canonical_fallback_lookups}"
        )));
    }
    Ok(TransactionRuntimePaths {
        index_runtime_path: index_runtime_path.to_string(),
        row_runtime_path: row_runtime_path.to_string(),
        transaction_workspace_lookups,
        canonical_fallback_lookups,
    })
}

fn require_bounded_rows(
    statement: &str,
    max_rows: usize,
    max_payload_bytes: usize,
    expected_rows: usize,
    output: &QueryOutput,
) -> Result<()> {
    let output_payload_bytes = output.payload_bytes();
    if output.rows.len() != expected_rows
        || output.rows.len() > max_rows
        || output_payload_bytes > max_payload_bytes
    {
        return Err(HawDBError::Execution(format!(
            "content-store transaction statement {statement} returned {} rows and {output_payload_bytes} bytes, expected {expected_rows} rows within rows={max_rows}, bytes={max_payload_bytes}",
            output.rows.len(),
        )));
    }
    Ok(())
}

fn required_i64(output: &QueryOutput, field: &str) -> Result<i64> {
    if output.rows.len() != 1 {
        return Err(HawDBError::Execution(format!(
            "content-store transaction expected one row for {field}, got {}",
            output.rows.len()
        )));
    }
    match output.rows[0].get(field) {
        Some(Value::Int(value)) => Ok(*value),
        other => Err(HawDBError::Execution(format!(
            "content-store transaction expected integer field {field}, got {other:?}"
        ))),
    }
}

fn required_info_field<'a>(info: &'a str, field: &str) -> Result<&'a str> {
    info_field(info, field).ok_or_else(|| {
        HawDBError::Execution(format!(
            "content-store transaction EXPLAIN has no {field} field"
        ))
    })
}

fn required_info_u64(info: &str, field: &str) -> Result<u64> {
    let value = required_info_field(info, field)?;
    value.parse::<u64>().map_err(|error| {
        HawDBError::Execution(format!(
            "content-store transaction EXPLAIN has invalid {field}={value}: {error}"
        ))
    })
}
