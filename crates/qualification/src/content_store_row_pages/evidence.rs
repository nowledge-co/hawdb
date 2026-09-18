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

use super::{
    ContentStoreRowPageCacheDelta, ContentStoreRowPageExecutionEvidence,
    ContentStoreRowPageReadPhase, ContentStoreRowPageReadReport,
};
use crate::evidence_digest::rows_sha256;
use crate::{
    ContentStoreSqlStatementClassification, ContentStoreSqlStatementKind,
    ContentStoreSqlStatementSpec,
};
use hawdb::{Database, HawDBError, QueryStreamOptions, RelationalSqlReadProfile, Result, Value};

pub(super) const MESSAGE_POINT_SQL: &str =
    "SELECT content_message_id, content FROM thread_messages WHERE content_message_id = $1";
pub(super) const MESSAGE_POINT_MAX_ROWS: usize = 1;
pub(super) const MESSAGE_POINT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn message_point_parameters(content_message_id: &str) -> [Value; 1] {
    [Value::String(content_message_id.to_string())]
}

pub(super) fn message_point_statement(
    name: &str,
    transaction_group: &str,
) -> ContentStoreSqlStatementSpec {
    ContentStoreSqlStatementSpec {
        name: name.to_string(),
        kind: ContentStoreSqlStatementKind::Read,
        classification: ContentStoreSqlStatementClassification::Required,
        transaction_group: Some(transaction_group.to_string()),
        sql: MESSAGE_POINT_SQL.to_string(),
        parameters: vec!["TEXT".to_string()],
        result_columns: vec!["content_message_id".to_string(), "content".to_string()],
        ordering: Vec::new(),
        max_rows: MESSAGE_POINT_MAX_ROWS,
        max_payload_bytes: MESSAGE_POINT_MAX_PAYLOAD_BYTES,
    }
}

pub(super) const fn message_point_options() -> QueryStreamOptions {
    QueryStreamOptions {
        max_rows: Some(MESSAGE_POINT_MAX_ROWS),
        max_payload_bytes: Some(MESSAGE_POINT_MAX_PAYLOAD_BYTES),
    }
}

pub(super) fn require_one_message(
    rows: &hawdb::QueryRows,
    content_message_id: &str,
    probe: &str,
) -> Result<()> {
    let matches_message = matches!(
        rows.row(0).and_then(|row| row.get("content_message_id")),
        Some(Value::String(actual)) if actual == content_message_id
    );
    if rows.len() != 1 || !matches_message {
        return Err(HawDBError::Execution(format!(
            "content-store {probe} expected exactly message {content_message_id}, got {rows:?}"
        )));
    }
    Ok(())
}

pub(super) fn execute_read_set(
    database: &mut Database,
    specs: &[(&ContentStoreSqlStatementSpec, Vec<Value>, usize)],
    phase: ContentStoreRowPageReadPhase,
) -> Result<Vec<ContentStoreRowPageReadReport>> {
    specs
        .iter()
        .map(|(statement, parameters, expected_rows)| {
            execute_qualified_read(
                database,
                statement,
                parameters.clone(),
                phase,
                *expected_rows,
            )
        })
        .collect()
}

pub(super) fn execute_qualified_read(
    database: &mut Database,
    statement: &ContentStoreSqlStatementSpec,
    parameters: Vec<Value>,
    phase: ContentStoreRowPageReadPhase,
    expected_rows: usize,
) -> Result<ContentStoreRowPageReadReport> {
    let before = database.segment_cache_snapshot().ok_or_else(|| {
        HawDBError::Execution(
            "content-store row-page qualification requires a segment cache".to_string(),
        )
    })?;
    let transaction = database.begin_read_transaction();
    let profiled = transaction.query_sql_with_params_options_profiled(
        &statement.sql,
        &parameters,
        QueryStreamOptions {
            max_rows: Some(statement.max_rows),
            max_payload_bytes: Some(statement.max_payload_bytes),
        },
    )?;
    let output = profiled.output;
    let execution = execution_evidence(statement, profiled.profile)?;
    let after = database.segment_cache_snapshot().ok_or_else(|| {
        HawDBError::Execution(
            "content-store row-page qualification lost its segment cache".to_string(),
        )
    })?;
    if output.rows.len() != expected_rows {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} returned {} rows, expected {expected_rows}",
            statement.name,
            output.rows.len()
        )));
    }
    let output_payload_bytes = output.payload_bytes();
    if output_payload_bytes > statement.max_payload_bytes {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} returned {output_payload_bytes} payload bytes, exceeding {}",
            statement.name, statement.max_payload_bytes
        )));
    }
    Ok(ContentStoreRowPageReadReport {
        statement_name: statement.name.clone(),
        phase,
        max_rows: statement.max_rows,
        max_payload_bytes: statement.max_payload_bytes,
        output_rows: output.rows.len(),
        output_payload_bytes,
        output_sha256: rows_sha256(&output.rows),
        cache: cache_delta(before, after)?,
        execution,
    })
}

fn execution_evidence(
    statement: &ContentStoreSqlStatementSpec,
    profile: RelationalSqlReadProfile,
) -> Result<ContentStoreRowPageExecutionEvidence> {
    let index_runtime_path = profile.index_reads.first().map_or_else(
        || "none".to_string(),
        |first| {
            if profile
                .index_reads
                .iter()
                .all(|read| read.runtime_path == first.runtime_path)
            {
                first.runtime_path.clone()
            } else {
                "mixed".to_string()
            }
        },
    );
    let index_logical_pages = sum_index_read(&profile, |read| read.logical_pages);
    let index_logical_bytes = sum_index_read(&profile, |read| read.logical_bytes);
    let index_physical_pages = sum_index_read(&profile, |read| read.physical_pages);
    let index_physical_bytes = sum_index_read(&profile, |read| read.physical_bytes);
    let index_cache_hits = sum_index_read(&profile, |read| read.cache_hits);
    let index_cache_misses = sum_index_read(&profile, |read| read.cache_misses);
    let index_cache_admission_rejections =
        sum_index_read(&profile, |read| read.cache_admission_rejections);
    let row = profile.row_read;
    let evidence = ContentStoreRowPageExecutionEvidence {
        index_runtime_path,
        row_runtime_path: row.runtime_path,
        base_generation: row
            .base_generation
            .ok_or_else(|| missing_profile(statement, "base generation"))?,
        delta_generation: row.delta_generation,
        base_commit_epoch: row
            .base_commit_epoch
            .ok_or_else(|| missing_profile(statement, "base commit epoch"))?,
        visible_commit_epoch: row
            .visible_commit_epoch
            .ok_or_else(|| missing_profile(statement, "visible commit epoch"))?,
        root_set_digest: row
            .root_set_digest
            .ok_or_else(|| missing_profile(statement, "root-set digest"))?,
        logical_pages: count_u64(row.logical_pages),
        logical_bytes: count_u64(row.logical_bytes),
        physical_pages: count_u64(row.physical_pages),
        physical_bytes: count_u64(row.physical_bytes),
        cache_hits: count_u64(row.cache_hits),
        cache_misses: count_u64(row.cache_misses),
        cache_admission_rejections: count_u64(row.cache_admission_rejections),
        index_logical_pages,
        index_logical_bytes,
        index_physical_pages,
        index_physical_bytes,
        index_cache_hits,
        index_cache_misses,
        index_cache_admission_rejections,
        overlay_entries: count_u64(row.overlay_entries),
        overlay_bytes: count_u64(row.overlay_resident_bytes),
        rows_visited: count_u64(row.rows_visited),
        intermediate_rows: count_u64(profile.intermediate_rows),
        hydrated_rows: count_u64(profile.hydrated_rows),
        hydrated_compressed_bytes: count_u64(profile.hydrated_compressed_bytes),
        hydrated_decompressed_bytes: count_u64(profile.hydrated_decompressed_bytes),
    };
    if !matches!(
        evidence.index_runtime_path.as_str(),
        "authoritative" | "none"
    ) {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} used index runtime {}, expected authoritative or a direct canonical row scan",
            statement.name, evidence.index_runtime_path
        )));
    }
    if evidence.row_runtime_path != "snapshot_rows" {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} used row runtime {}, expected snapshot_rows",
            statement.name, evidence.row_runtime_path
        )));
    }
    if evidence.root_set_digest == "none" {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} did not bind a row root-set digest",
            statement.name
        )));
    }
    Ok(evidence)
}

fn missing_profile(statement: &ContentStoreSqlStatementSpec, field: &str) -> HawDBError {
    HawDBError::Execution(format!(
        "content-store statement {} has no {field} execution evidence",
        statement.name
    ))
}

fn count_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn sum_index_read(
    profile: &RelationalSqlReadProfile,
    field: impl Fn(&hawdb::RelationalSqlIndexReadProfile) -> usize,
) -> u64 {
    profile.index_reads.iter().fold(0u64, |total, read| {
        total.saturating_add(count_u64(field(read)))
    })
}

fn cache_delta(
    before: hawdb::SegmentCacheSnapshot,
    after: hawdb::SegmentCacheSnapshot,
) -> Result<ContentStoreRowPageCacheDelta> {
    if after.pinned_bytes != 0 {
        return Err(HawDBError::Execution(format!(
            "content-store row-page qualification leaked {} pinned cache bytes",
            after.pinned_bytes
        )));
    }
    Ok(ContentStoreRowPageCacheDelta {
        hits: monotonic_delta("cache hits", before.hit_count, after.hit_count)?,
        misses: monotonic_delta("cache misses", before.miss_count, after.miss_count)?,
        insertions: monotonic_delta(
            "cache insertions",
            before.insertion_count,
            after.insertion_count,
        )?,
        evictions: monotonic_delta(
            "cache evictions",
            before.eviction_count,
            after.eviction_count,
        )?,
        admission_rejections: monotonic_delta(
            "cache admission rejections",
            before.admission_rejection_count,
            after.admission_rejection_count,
        )?,
        resident_bytes_after: after.resident_bytes,
        pinned_bytes_after: after.pinned_bytes,
    })
}

fn monotonic_delta(name: &str, before: u64, after: u64) -> Result<u64> {
    after.checked_sub(before).ok_or_else(|| {
        HawDBError::Execution(format!(
            "content-store row-page qualification observed non-monotonic {name}"
        ))
    })
}

pub(super) fn require_matching_results(
    cold: &[ContentStoreRowPageReadReport],
    warm: &[ContentStoreRowPageReadReport],
) -> Result<()> {
    if cold.len() != warm.len() {
        return Err(HawDBError::Execution(
            "content-store cold and warm read sets have different lengths".to_string(),
        ));
    }
    for (cold, warm) in cold.iter().zip(warm) {
        if cold.statement_name != warm.statement_name
            || cold.output_rows != warm.output_rows
            || cold.output_sha256 != warm.output_sha256
        {
            return Err(HawDBError::Execution(format!(
                "content-store cold/warm result mismatch for {}",
                cold.statement_name
            )));
        }
    }
    Ok(())
}

pub(super) fn info_field<'a>(info: &'a str, name: &str) -> Option<&'a str> {
    info.split(", ").find_map(|field| {
        let (key, value) = field.split_once('=')?;
        (key == name).then_some(value)
    })
}
