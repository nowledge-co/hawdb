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

use crate::binding::{binding_memory_bytes_with_values, Binding};
use crate::expression::{
    evaluate_projection_borrowed, prepare_borrowed_projection, ProjectedValue,
};
use crate::pipeline::{
    BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource, TransformBatchBuilder,
};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::Result;
use hawdb_plan_cypher::{PhysicalPlan, Projection};
use std::cell::Cell;
use std::collections::BTreeMap;

mod scoring;
pub use scoring::{stream_scoring_program_batches, stream_scoring_rerank_batches};
mod host_scoring;
pub use host_scoring::{stream_host_scoring_batches, HostScoringOptions};

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
        let working = context.operator_account("ProjectExec expressions");
        for binding in batch {
            let (mut values, mut layout) =
                prepare_projection_values(items, &binding, context, &working)?;
            let mut bytes = binding_memory_bytes_with_values(
                &binding,
                values
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_ref())),
            );
            if bytes <= context.memory.batch_payload_bytes.get()
                && projected.would_exceed_payload(bytes)
                && !projected.is_empty()
            {
                // Preparation belongs to the next row, not the emitted batch.
                // Release it before crossing the consumer boundary; these pure
                // expressions can be prepared again only if the consumer continues.
                drop(values);
                drop(layout);
                if projected.emit(&mut emit_projected)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                (values, layout) = prepare_projection_values(items, &binding, context, &working)?;
                bytes = binding_memory_bytes_with_values(
                    &binding,
                    values
                        .iter()
                        .map(|(name, value)| (name.as_str(), value.as_ref())),
                );
            }
            if projected.reserve_row_before_allocation(bytes, &mut emit_projected)?
                == BatchControl::Stop
            {
                return Ok(BatchControl::Stop);
            }
            let values = values
                .into_iter()
                .map(|(name, value)| (name, value.into_owned()))
                .collect();
            drop(layout);
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

fn prepare_projection_values<'a>(
    items: &'a [Projection],
    binding: &'a Binding,
    context: BatchExecutionContext<'a>,
    working: &QueryMemoryAccount,
) -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)> {
    prepare_borrowed_projection(
        items,
        working,
        context.observer.seed_graph_scoring_input().map(|_| binding),
        |expression| {
            evaluate_projection_borrowed(expression, context.catalog, binding, Some(working))
        },
    )
}

/// Completing this operator's window returns `Continue`; a downstream stop
/// or an upstream stop before the window completes remains `Stop`.
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
    let consumer_stopped = Cell::new(false);
    let output_cap = match (limit, execution_limit.output_rows) {
        (Some(limit), Some(parent)) => (limit).min(parent),
        (Some(limit), None) => limit,
        (None, Some(parent)) => parent,
        (None, None) => usize::MAX,
    };
    let control = source.execute(
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
                let control = emit(batch)?;
                consumer_stopped.set(consumer_stopped.get() || control == BatchControl::Stop);
                Ok(control)
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
    )?;
    // Reaching this operator's window completes its input contract. Preserve
    // downstream stop and an upstream stop that did not fill the window.
    Ok(
        if control == BatchControl::Stop && !consumer_stopped.get() && emitted.get() == output_cap {
            BatchControl::Continue
        } else {
            control
        },
    )
}

#[cfg(test)]
mod tests;
