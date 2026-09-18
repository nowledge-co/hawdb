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

//! Storage-neutral streaming filter, projection, and limit kernels.
//!
//! Sources retain cancellation and input validation; consumers retain output
//! validation. Kernels reserve transform memory and propagate stop/error across
//! those boundaries without taking ownership of recursive plan dispatch.

use crate::binding::Binding;
use crate::expression::{insert_projected_value, project_value};
use crate::pipeline::{
    BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource, TransformBatchBuilder,
};
use crate::ExecutionLimit;
use hawdb_core::Result;
use hawdb_plan::{PhysicalPlan, Projection};
use std::cell::Cell;
use std::collections::BTreeMap;

pub fn stream_filter_batches(
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let emitted = Cell::new(0usize);
    source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        let remaining = execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(emitted.get());
        if remaining == 0 {
            return Ok(BatchControl::Stop);
        }
        let mut filtered = TransformBatchBuilder::new(
            "FilterExec",
            context.memory.batch_rows.get(),
            context.memory.batch_payload_bytes,
            context.memory_ledger,
        )?;
        let mut emit_filtered = |output: BindingBatch| {
            emitted.set(emitted.get().saturating_add(output.len()));
            emit(output)
        };
        for binding in batch {
            if predicate(&binding)? {
                filtered.reserve_before_allocation()?;
                filtered.push(binding);
                if filtered.is_full() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                if execution_limit.is_reached(emitted.get().saturating_add(filtered.len())) {
                    break;
                }
            }
        }
        if !filtered.is_empty() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if execution_limit.is_reached(emitted.get()) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    })
}

pub fn stream_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let emitted = Cell::new(0usize);
    source.execute(input, execution_limit, &mut |batch| {
        let mut projected = TransformBatchBuilder::new(
            "ProjectExec",
            context.memory.batch_rows.get(),
            context.memory.batch_payload_bytes,
            context.memory_ledger,
        )?;
        let mut emit_projected = |output: BindingBatch| {
            emitted.set(emitted.get().saturating_add(output.len()));
            emit(output)
        };
        for binding in batch {
            projected.reserve_before_allocation()?;
            let mut values = BTreeMap::new();
            for item in items {
                let value = project_value(item, context.catalog, &binding)?;
                insert_projected_value(&mut values, &item.name, value);
            }
            projected.push(Binding {
                values,
                nodes: binding.nodes,
                relationships: binding.relationships,
            });
            if projected.is_full() && projected.emit(&mut emit_projected)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
        }
        if !projected.is_empty() && projected.emit(&mut emit_projected)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        Ok(if execution_limit.is_reached(emitted.get()) {
            BatchControl::Stop
        } else {
            BatchControl::Continue
        })
    })
}

pub fn stream_limit_batches(
    offset: usize,
    limit: Option<usize>,
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let skipped = Cell::new(0usize);
    let emitted = Cell::new(0usize);
    let output_cap = match (limit, execution_limit.output_rows) {
        (Some(limit), Some(parent)) => (limit).min(parent),
        (Some(limit), None) => limit,
        (None, Some(parent)) => parent,
        (None, None) => usize::MAX,
    };
    source.execute(
        input,
        ExecutionLimit {
            output_rows: Some(offset.saturating_add(output_cap)),
        },
        &mut |batch| {
            let mut output = TransformBatchBuilder::new(
                "LimitExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?;
            let mut emit_output = |batch: BindingBatch| {
                emitted.set(emitted.get().saturating_add(batch.len()));
                emit(batch)
            };
            for binding in batch {
                if skipped.get() < offset {
                    skipped.set(skipped.get().saturating_add(1));
                    continue;
                }
                if emitted.get() == output_cap {
                    break;
                }
                output.reserve_before_allocation()?;
                output.push(binding);
                if output.is_full() && output.emit(&mut emit_output)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
            if !output.is_empty() && output.emit(&mut emit_output)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(if emitted.get() == output_cap {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            })
        },
    )
}

#[cfg(test)]
mod tests;
