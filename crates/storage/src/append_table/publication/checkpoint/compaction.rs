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

//! Private compaction keeps work rejection distinct from data-budget deferral.
//! Block I/O, decompression and row traversal use cooperative boundaries. The
//! individual row/key decoder and overflow decoder retain their existing limits;
//! these limits do not establish a hard candidate memory ledger.

use super::*;
use crate::append_table::checkpoint::{sort_rows_with_work_context, CompactionFailure as Failure};

pub(in crate::append_table::publication) fn plan_compaction(
    previous: Option<&AppendGenerationReader>,
    rows: &[AppendTableRow],
    config: AppendPublicationConfig,
    work: &CheckpointWorkContext,
) -> Result<AppendCompactionPlan, AppendTableError> {
    work.checkpoint().map_err(work_error)?;
    let prior_segment_count = previous.map_or(0, |reader| reader.manifest.segments.len());
    let due =
        !rows.is_empty() && prior_segment_count.saturating_add(1) > config.compact_after_segments;
    let deferred = || AppendCompactionPlan {
        due,
        checkpoint_rows: None,
    };
    if !due || rows.len() > config.max_compaction_rows {
        work.checkpoint().map_err(work_error)?;
        return Ok(deferred());
    }
    let mut live_payload_bytes = 0usize;
    for row in rows {
        let unit = work.start_unit().map_err(work_error)?;
        live_payload_bytes = live_payload_bytes
            .checked_add(crate::append_table::estimated_row_bytes(&row.row)?)
            .ok_or_else(|| {
                AppendTableError::Admission(
                    "append compaction payload size overflows usize".to_string(),
                )
            })?;
        unit.finish();
    }
    if live_payload_bytes > config.max_compaction_payload_bytes {
        work.checkpoint().map_err(work_error)?;
        return Ok(deferred());
    }
    let previous = previous.ok_or_else(|| {
        AppendTableError::Corruption("append compaction requires a previous generation".to_string())
    })?;
    let Some(mut checkpoint_rows) = checkpoint_rows(
        previous,
        config.max_compaction_rows - rows.len(),
        config.max_compaction_payload_bytes - live_payload_bytes,
        work,
    )
    .map_err(Failure::into_append)?
    else {
        work.checkpoint().map_err(work_error)?;
        return Ok(deferred());
    };
    for row in rows {
        let unit = work.start_unit().map_err(work_error)?;
        checkpoint_rows.push(row.clone());
        unit.finish();
    }
    let checkpoint_rows = sort_rows_with_work_context(checkpoint_rows, work)?;
    work.checkpoint().map_err(work_error)?;
    Ok(AppendCompactionPlan {
        due,
        checkpoint_rows: Some(checkpoint_rows),
    })
}

fn checkpoint_rows(
    previous: &AppendGenerationReader,
    max_rows: usize,
    max_payload_bytes: usize,
    work: &CheckpointWorkContext,
) -> Result<Option<Vec<AppendTableRow>>, Failure> {
    let mut expected_rows = 0usize;
    for segment in previous.segments.iter() {
        for descriptor in segment.descriptors() {
            let unit = work.start_unit()?;
            expected_rows = expected_rows
                .checked_add(descriptor.row_count as usize)
                .ok_or_else(|| {
                    AppendTableError::Admission("append row count overflow".to_string())
                })?;
            unit.finish();
        }
    }
    if expected_rows > max_rows {
        work.checkpoint()?;
        return Ok(None);
    }
    let mut rows = Vec::with_capacity(expected_rows);
    let mut payload_bytes = 0usize;
    for segment in previous.segments.iter() {
        for descriptor in segment.descriptors() {
            let block = match segment.checkpoint_block_with_work_context(
                descriptor,
                max_payload_bytes.saturating_sub(payload_bytes),
                work,
            ) {
                Ok(block) => block,
                // Only a data decoder's configured budget can defer compaction.
                // Work admission and cancellation must abort this candidate.
                Err(Failure::Data(AppendTableError::Admission(_))) => {
                    work.checkpoint()?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            for row in block {
                let unit = work.start_unit()?;
                let Ok(row_bytes) = crate::append_table::estimated_row_bytes(&row.row) else {
                    work.checkpoint()?;
                    return Ok(None);
                };
                payload_bytes = match payload_bytes.checked_add(row_bytes) {
                    Some(bytes) if bytes <= max_payload_bytes => bytes,
                    _ => {
                        work.checkpoint()?;
                        return Ok(None);
                    }
                };
                rows.push(row);
                unit.finish();
            }
        }
    }
    let rows = sort_rows_with_work_context(rows, work).map_err(Failure::Data)?;
    if rows.len() != expected_rows {
        return Err(AppendTableError::Corruption(
            "append checkpoint rows overlap or regress".to_string(),
        )
        .into());
    }
    for pair in rows.windows(2) {
        let unit = work.start_unit()?;
        if !compare_rows(&pair[0], &pair[1]).is_lt() {
            return Err(AppendTableError::Corruption(
                "append checkpoint rows overlap or regress".to_string(),
            )
            .into());
        }
        unit.finish();
    }
    work.checkpoint()?;
    Ok(Some(rows))
}

#[cfg(test)]
mod tests;
