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

//! Row-array ownership for live capture and private append compaction.
//! RelationalRow clones share immutable values. Source retention and the
//! compaction decoder's element allocations are separate resource obligations.
//! Allocator rounding/latency and variable-width comparison remain assumptions.

use super::*;
use crate::background::CheckpointAllocationOwner;
use crate::relational::{RelationalKey, RelationalValue};
use hawdb_core::RuntimeMemoryPermit;

const COPY_BYTES_PER_UNIT: usize = 64 * 1024;
const COPY_VALUES_PER_UNIT: usize = 1024;

#[derive(Debug)]
pub(crate) struct CheckpointAppendRows {
    // Destroy rows, key payloads and the backing array before their leases.
    pub(super) rows: Vec<AppendTableRow>,
    pub(super) memory: Option<Box<dyn RuntimeMemoryPermit>>,
    allocations: CheckpointAllocationOwner,
}

impl std::ops::Deref for CheckpointAppendRows {
    type Target = [AppendTableRow];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl PartialEq<Vec<AppendTableRow>> for CheckpointAppendRows {
    fn eq(&self, other: &Vec<AppendTableRow>) -> bool {
        self.rows == *other
    }
}

impl PartialEq for CheckpointAppendRows {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

impl CheckpointAppendRows {
    pub(in crate::append_table) fn new(
        capacity: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, AppendTableError> {
        let unit = work.start_unit().map_err(work_error)?;
        let memory = reserve_capacity::<AppendTableRow>(capacity, work)?;
        let rows = allocate_capacity(capacity, work)?;
        let output = Self {
            rows,
            memory,
            allocations: CheckpointAllocationOwner::default(),
        };
        unit.finish();
        work.checkpoint().map_err(work_error)?;
        Ok(output)
    }

    pub(in crate::append_table) fn unadmitted(rows: Vec<AppendTableRow>) -> Self {
        Self {
            rows,
            memory: None,
            allocations: CheckpointAllocationOwner::default(),
        }
    }

    pub(in crate::append_table) fn push_clone(
        &mut self,
        row: &AppendTableRow,
        work: &CheckpointWorkContext,
    ) -> Result<(), AppendTableError> {
        let unit = work.start_unit().map_err(work_error)?;
        if self.rows.len() == self.rows.capacity() {
            return Err(inconsistent_batches());
        }
        if bounded_copy(row) {
            // Preserve the original one-row unit for bounded records, including
            // existing cancellation cuts. No nested local permit is acquired.
            let copied = AppendTableRow {
                table: copy_small_string(&row.table, &mut self.allocations, work)?,
                partition_key: copy_small_key(&row.partition_key, &mut self.allocations, work)?,
                order_key: copy_small_key(&row.order_key, &mut self.allocations, work)?,
                row: row.row.clone(),
            };
            self.rows.push(copied);
            unit.finish();
        } else {
            unit.finish();
            let table = copy_string(&row.table, &mut self.allocations, work)?;
            let partition_key = copy_key(&row.partition_key, &mut self.allocations, work)?;
            let order_key = copy_key(&row.order_key, &mut self.allocations, work)?;
            let unit = work.start_unit().map_err(work_error)?;
            self.rows.push(AppendTableRow {
                table,
                partition_key,
                order_key,
                row: row.row.clone(),
            });
            unit.finish();
        }
        work.checkpoint().map_err(work_error)
    }

    /// The caller owns the element allocations separately and admits its row
    /// work unit. This operation never grows the admitted backing array.
    pub(in crate::append_table) fn push_owned(
        &mut self,
        row: AppendTableRow,
    ) -> Result<(), AppendTableError> {
        if self.rows.len() == self.rows.capacity() {
            return Err(inconsistent_batches());
        }
        self.rows.push(row);
        Ok(())
    }

    pub(in crate::append_table) fn as_mut_slice(&mut self) -> &mut [AppendTableRow] {
        &mut self.rows
    }

    pub(in crate::append_table) fn append_owned(
        &mut self,
        mut incoming: Self,
        work: &CheckpointWorkContext,
    ) -> Result<(), AppendTableError> {
        if incoming.len() > self.rows.capacity() - self.rows.len() {
            return Err(inconsistent_batches());
        }
        // Transfer element leases before moving their data. Both inventories
        // remain intact on transfer rejection; moved values stay owned even if
        // a later row unit is cancelled. Incoming array capacity remains alive
        // until its actual IntoIter backing allocation has been destroyed.
        self.allocations
            .append(&mut incoming.allocations, work)
            .map_err(work_error)?;
        let input = std::mem::take(&mut incoming.rows).into_iter();
        for row in input {
            let unit = work.start_unit().map_err(work_error)?;
            self.push_owned(row)?;
            unit.finish();
        }
        drop(incoming);
        work.checkpoint().map_err(work_error)
    }
}

fn bounded_copy(row: &AppendTableRow) -> bool {
    let Some(count) = row.partition_key.0.len().checked_add(row.order_key.0.len()) else {
        return false;
    };
    if count > COPY_VALUES_PER_UNIT {
        return false;
    }
    let Some(mut bytes) = count
        .checked_mul(std::mem::size_of::<RelationalValue>())
        .and_then(|bytes| bytes.checked_add(row.table.len()))
    else {
        return false;
    };
    for value in row.partition_key.0.iter().chain(&row.order_key.0) {
        let payload = match value {
            RelationalValue::Text(value) => value.len(),
            RelationalValue::Bytea(value) => value.len(),
            _ => 0,
        };
        let Some(total) = bytes.checked_add(payload) else {
            return false;
        };
        bytes = total;
        if bytes > COPY_BYTES_PER_UNIT {
            return false;
        }
    }
    bytes <= COPY_BYTES_PER_UNIT
}

pub(super) fn capacity_bytes<T>(capacity: usize) -> Result<usize, AppendTableError> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            AppendTableError::Admission("append checkpoint capacity overflows usize".into())
        })
}

pub(super) fn reserve_capacity<T>(
    capacity: usize,
    work: &CheckpointWorkContext,
) -> Result<Option<Box<dyn RuntimeMemoryPermit>>, AppendTableError> {
    let bytes = capacity_bytes::<T>(capacity)?;
    if bytes == 0 {
        return Ok(None);
    }
    work.reserve_memory(bytes).map_err(work_error)
}

pub(super) fn allocate_capacity<T>(
    capacity: usize,
    work: &CheckpointWorkContext,
) -> Result<Vec<T>, AppendTableError> {
    let bytes = capacity_bytes::<T>(capacity)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|error| allocation_failure(bytes, error.to_string(), work))?;
    if std::mem::size_of::<T>() != 0 && output.capacity() != capacity {
        return Err(allocation_failure(
            bytes,
            format!(
                "allocator granted {} slots beyond admitted capacity {capacity}",
                output.capacity()
            ),
            work,
        ));
    }
    Ok(output)
}

fn allocation_failure(
    bytes: usize,
    reason: String,
    work: &CheckpointWorkContext,
) -> AppendTableError {
    work_error(work.record_failure(CheckpointWorkError::Allocation {
        bytes: bytes as u64,
        reason,
    }))
}

fn admit<T>(
    capacity: usize,
    owner: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<(), AppendTableError> {
    owner
        .reserve(capacity_bytes::<T>(capacity)?, work)
        .map_err(work_error)?;
    work.checkpoint().map_err(work_error)
}

fn copy_small_string(
    input: &str,
    owner: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<String, AppendTableError> {
    admit::<u8>(input.len(), owner, work)?;
    let mut output = String::new();
    output
        .try_reserve_exact(input.len())
        .map_err(|error| allocation_failure(input.len(), error.to_string(), work))?;
    if output.capacity() != input.len() {
        return Err(allocation_failure(
            input.len(),
            "string capacity exceeds admission".into(),
            work,
        ));
    }
    output.push_str(input);
    Ok(output)
}

fn copy_small_key(
    input: &RelationalKey,
    owner: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<RelationalKey, AppendTableError> {
    admit::<RelationalValue>(input.0.len(), owner, work)?;
    let mut output = allocate_capacity(input.0.len(), work)?;
    for value in &input.0 {
        output.push(match value {
            RelationalValue::Text(value) => {
                RelationalValue::Text(copy_small_string(value, owner, work)?)
            }
            RelationalValue::Bytea(value) => {
                admit::<u8>(value.len(), owner, work)?;
                let mut bytes = allocate_capacity(value.len(), work)?;
                bytes.extend_from_slice(value);
                RelationalValue::Bytea(bytes)
            }
            value => value.clone(),
        });
    }
    Ok(RelationalKey(output))
}

fn copy_string(
    input: &str,
    owner: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<String, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    // Allocation is separate from bounded copying. The borrowed source is
    // already valid UTF-8; aligned slices avoid a whole-value validation pass.
    let mut output = String::new();
    admit::<u8>(input.len(), owner, work)?;
    output
        .try_reserve_exact(input.len())
        .map_err(|error| allocation_failure(input.len(), error.to_string(), work))?;
    if output.capacity() != input.len() {
        return Err(allocation_failure(
            input.len(),
            "string capacity exceeds admission".into(),
            work,
        ));
    }
    unit.finish();
    let mut offset = 0;
    while offset < input.len() {
        let unit = work.start_unit().map_err(work_error)?;
        let mut end = offset.saturating_add(COPY_BYTES_PER_UNIT).min(input.len());
        while !input.is_char_boundary(end) {
            end -= 1;
        }
        output.push_str(&input[offset..end]);
        offset = end;
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(output)
}

fn copy_key(
    input: &RelationalKey,
    owner: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<RelationalKey, AppendTableError> {
    let unit = work.start_unit().map_err(work_error)?;
    admit::<RelationalValue>(input.0.len(), owner, work)?;
    let mut output = allocate_capacity(input.0.len(), work)?;
    unit.finish();
    for value in &input.0 {
        let copied = match value {
            RelationalValue::Text(value) => RelationalValue::Text(copy_string(value, owner, work)?),
            RelationalValue::Bytea(value) => {
                let unit = work.start_unit().map_err(work_error)?;
                admit::<u8>(value.len(), owner, work)?;
                let mut output = allocate_capacity(value.len(), work)?;
                unit.finish();
                for bytes in value.chunks(COPY_BYTES_PER_UNIT) {
                    let unit = work.start_unit().map_err(work_error)?;
                    output.extend_from_slice(bytes);
                    unit.finish();
                }
                RelationalValue::Bytea(output)
            }
            value => {
                let unit = work.start_unit().map_err(work_error)?;
                let copied = value.clone();
                unit.finish();
                copied
            }
        };
        let unit = work.start_unit().map_err(work_error)?;
        output.push(copied);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)?;
    Ok(RelationalKey(output))
}
