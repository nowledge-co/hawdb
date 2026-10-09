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
    let before = database.segment_cache_snapshot()?.ok_or_else(|| {
        HawDBError::Execution(
            "content-store row-page qualification requires a segment cache".to_string(),
        )
    })?;
    let transaction = database.begin_read_transaction()?;
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
    let after = database.segment_cache_snapshot()?.ok_or_else(|| {
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
    let index_runtime_path = index_execution_runtime_path(statement, &profile.index_reads)?;
    let index_logical_pages = sum_index_read(&profile.index_reads, |read| read.logical_pages);
    let index_logical_bytes = sum_index_read(&profile.index_reads, |read| read.logical_bytes);
    let index_physical_pages = sum_index_read(&profile.index_reads, |read| read.physical_pages);
    let index_physical_bytes = sum_index_read(&profile.index_reads, |read| read.physical_bytes);
    let index_cache_hits = sum_index_read(&profile.index_reads, |read| read.cache_hits);
    let index_cache_misses = sum_index_read(&profile.index_reads, |read| read.cache_misses);
    let index_cache_admission_rejections =
        sum_index_read(&profile.index_reads, |read| read.cache_admission_rejections);
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

fn index_execution_runtime_path(
    statement: &ContentStoreSqlStatementSpec,
    reads: &[hawdb::RelationalSqlIndexReadProfile],
) -> Result<String> {
    let mut executed_path = None;
    for read in reads {
        match read.lookups.checked_sub(read.metadata_count_lookups) {
            Some(0) if read.metadata_count_lookups != 0 && read.runtime_path == "not_executed" => {
                // Planning counts retain their I/O below, but do not establish
                // an execution path. The canonical row snapshot is checked
                // independently by execution_evidence.
                continue;
            }
            Some(executed) if executed != 0 => {}
            _ => {
                return Err(HawDBError::Execution(format!(
                    "content-store statement {} has inconsistent index purpose evidence for {}.{}: lookups={}, metadata_count_lookups={}, runtime_path={}",
                    statement.name,
                    read.table,
                    read.index,
                    read.lookups,
                    read.metadata_count_lookups,
                    read.runtime_path,
                )));
            }
        }
        executed_path = Some(match executed_path {
            None => read.runtime_path.as_str(),
            Some(path) if path == read.runtime_path => path,
            Some(_) => "mixed",
        });
    }
    let runtime_path = executed_path.unwrap_or("none").to_string();
    if executed_path.is_some() && runtime_path != "authoritative" {
        return Err(HawDBError::Execution(format!(
            "content-store statement {} used index runtime {}, expected authoritative or a direct canonical row scan",
            statement.name, runtime_path
        )));
    }
    Ok(runtime_path)
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
    reads: &[hawdb::RelationalSqlIndexReadProfile],
    field: impl Fn(&hawdb::RelationalSqlIndexReadProfile) -> usize,
) -> u64 {
    reads.iter().fold(0u64, |total, read| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::RelationalSqlIndexReadProfile;

    fn index_read(path: &str, lookups: usize, metadata: usize) -> RelationalSqlIndexReadProfile {
        RelationalSqlIndexReadProfile {
            table: "thread_messages".into(),
            index: "message_lookup".into(),
            lookups,
            metadata_count_lookups: metadata,
            runtime_path: path.into(),
            logical_pages: 2,
            logical_bytes: 200,
            physical_pages: 1,
            physical_bytes: 100,
            cache_hits: 3,
            cache_misses: 4,
            cache_admission_rejections: 5,
            rows_visited: 6,
        }
    }

    #[test]
    fn metadata_and_execution_keep_distinct_authority() {
        let statement = message_point_statement("profile_roles", "synthetic");
        let reads = [
            index_read("not_executed", 2, 2),
            index_read("authoritative", 3, 1),
        ];
        assert_eq!(
            index_execution_runtime_path(&statement, &reads).unwrap(),
            "authoritative"
        );
    }

    #[test]
    fn metadata_only_is_not_index_execution() {
        let statement = message_point_statement("profile_roles", "synthetic");
        assert_eq!(
            index_execution_runtime_path(&statement, &[index_read("not_executed", 2, 2)]).unwrap(),
            "none"
        );
    }

    #[test]
    fn unknown_inconsistent_and_fallback_profiles_refuse_authority() {
        let statement = message_point_statement("profile_roles", "synthetic");
        for (path, total, metadata) in [
            ("authoritative", 0, 0),
            ("authoritative", 2, 3),
            ("authoritative", 1, 1),
            ("not_executed", 0, 0),
            ("not_executed", 1, 0),
            ("none", 1, 0),
            ("canonical_fallback", 1, 0),
            ("mixed", 1, 0),
            ("demand_paged", 1, 0),
            ("transaction_workspace", 1, 0),
            ("unknown", 1, 0),
        ] {
            assert!(
                index_execution_runtime_path(&statement, &[index_read(path, total, metadata)])
                    .is_err(),
                "{path}: total={total}, metadata={metadata} must not establish authority"
            );
        }
    }

    #[test]
    fn metadata_io_is_preserved_in_complete_totals() {
        let reads = [
            index_read("not_executed", 2, 2),
            index_read("authoritative", 3, 1),
        ];
        assert_eq!(sum_index_read(&reads, |read| read.logical_pages), 4);
        assert_eq!(sum_index_read(&reads, |read| read.logical_bytes), 400);
        assert_eq!(sum_index_read(&reads, |read| read.physical_pages), 2);
        assert_eq!(sum_index_read(&reads, |read| read.physical_bytes), 200);
        assert_eq!(sum_index_read(&reads, |read| read.cache_hits), 6);
        assert_eq!(sum_index_read(&reads, |read| read.cache_misses), 8);
        assert_eq!(
            sum_index_read(&reads, |read| read.cache_admission_rejections),
            10
        );
    }
}
