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

//! `LOAD CSV` as a streaming read source.

use super::*;
use crate::load_csv::{open_load_csv, CSV_READ_BUFFER_BYTES};
use hawdb_plan_cypher::LoadCsvSource;

/// Emits one binding per CSV record in batches bounded by `batch_rows` and
/// `batch_payload_bytes`. Pending rows and the read buffer are charged to the
/// query memory ledger, so a record larger than one batch fails closed.
pub(super) fn stream_load_csv_batches(
    source: &LoadCsvSource,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let buffer_account = context.memory_ledger.account(
        QueryMemoryClass::ExternalRead,
        "LOAD CSV read buffer",
        NonZeroUsize::new(CSV_READ_BUFFER_BYTES).expect("non-zero CSV read buffer"),
    );
    let _buffer_lease = buffer_account.reserve(CSV_READ_BUFFER_BYTES)?;
    let row_account = context.memory_ledger.account(
        QueryMemoryClass::ExternalRead,
        "LOAD CSV pending rows",
        context.memory.batch_payload_bytes,
    );
    let mut rows = open_load_csv(
        context.external.csv_import_policy(),
        source,
        context.parameters,
    )?;
    let batch_rows = context.memory.batch_rows.get();
    let batch_payload_bytes = context.memory.batch_payload_bytes.get();
    let mut emitted = 0usize;
    let mut batch = Vec::new();
    let mut batch_bytes = 0usize;
    let mut leases = Vec::new();
    loop {
        if batch.is_empty() {
            runtime_checkpoint(context.task_context)?;
        }
        let Some(row) = rows.next_row()? else {
            break;
        };
        let binding = Binding::scalar(source.variable.clone(), row);
        let bytes = binding_memory_bytes(&binding);
        if !batch.is_empty() && batch_bytes.saturating_add(bytes) > batch_payload_bytes {
            emitted += batch.len();
            leases.clear();
            batch_bytes = 0;
            if emit(std::mem::take(&mut batch))? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        leases.push(row_account.reserve(bytes)?);
        batch_bytes = batch_bytes.saturating_add(bytes);
        batch.push(binding);
        let reached = execution_limit.is_reached(emitted + batch.len());
        if reached || batch.len() >= batch_rows {
            emitted += batch.len();
            leases.clear();
            batch_bytes = 0;
            if emit(std::mem::take(&mut batch))? == BatchControl::Stop || reached {
                return Ok(BatchControl::Stop);
            }
        }
    }
    if batch.is_empty() {
        return Ok(BatchControl::Continue);
    }
    drop(leases);
    emit(batch)
}
