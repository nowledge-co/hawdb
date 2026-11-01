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
//! Sources retain input validation; consumers retain output validation.
//! Kernels honor the supplied task, reserve transform memory, and propagate
//! stop/error without taking ownership of recursive plan dispatch.

use crate::binding::{binding_memory_bytes, binding_memory_bytes_with_values, Binding};
use crate::expression::{
    prepare_borrowed_projection, PreparedProjectionExpressions, ProjectedValue,
};
use crate::pipeline::{
    runtime_checkpoint, BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
    TransformBatchBuilder,
};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::Result;
use hawdb_plan_cypher::{PhysicalPlan, Projection, ProjectionExpression};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

mod scoring;
pub use scoring::{stream_scoring_program_batches, stream_scoring_rerank_batches};
mod host_scoring;
pub use host_scoring::{stream_host_scoring_batches, HostScoringOptions};
mod unwind;
pub use unwind::stream_unwind_rows_batches;

pub fn stream_filter_batches(
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    predicate: &mut dyn FnMut(&Binding) -> Result<bool>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let emitted = Cell::new(0usize);
    let mut filtered: Option<TransformBatchBuilder> = None;
    let mut emit_filtered = |output: BindingBatch| {
        emitted.set(emitted.get().saturating_add(output.len()));
        let control = emit(output)?;
        if control == BatchControl::Continue {
            runtime_checkpoint(context.task_context)?;
        }
        Ok(control)
    };
    let control = source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(context.task_context)?;
        let remaining = execution_limit
            .output_rows
            .unwrap_or(usize::MAX)
            .saturating_sub(emitted.get());
        if remaining == 0 {
            return Ok(BatchControl::Stop);
        }
        if filtered.is_none() {
            filtered = Some(TransformBatchBuilder::new(
                "FilterExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?);
        }
        let filtered = filtered.as_mut().unwrap();
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            let matches = predicate(&binding)?;
            runtime_checkpoint(context.task_context)?;
            if matches {
                if filtered.reserve_row_before_allocation(
                    binding_memory_bytes(&binding),
                    &mut emit_filtered,
                )? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
                filtered.push(binding);
                if filtered.is_full() && filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
                if execution_limit.is_reached(emitted.get().saturating_add(filtered.len())) {
                    break;
                }
            }
        }
        Ok(
            if execution_limit.is_reached(emitted.get().saturating_add(filtered.len())) {
                BatchControl::Stop
            } else {
                BatchControl::Continue
            },
        )
    })?;
    if let Some(filtered) = filtered.as_mut()
        && !filtered.is_empty()
    {
        runtime_checkpoint(context.task_context)?;
        if filtered.emit(&mut emit_filtered)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
    }
    Ok(control)
}

pub fn stream_projection_batches(
    items: &[Projection],
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    let prepared_account = context.source_account("ProjectExec CASE dispatch");
    let prepared = PreparedProjectionExpressions::new(
        items,
        &prepared_account,
        context.task_context,
        context.observer,
    )?;
    let emitted = Cell::new(0usize);
    source.execute(input, execution_limit, &mut |batch| {
        let mut projected = TransformBatchBuilder::new(
            "ProjectExec",
            context.memory.batch_rows.get(),
            projection_batch_budget(
                items,
                &batch,
                context.memory.batch_payload_bytes,
                context.observer.seed_graph_scoring_input().is_some(),
            ),
            context.memory_ledger,
        )?;
        let mut emit_projected = |output: BindingBatch| {
            emitted.set(emitted.get().saturating_add(output.len()));
            let control = emit(output)?;
            if control == BatchControl::Continue {
                runtime_checkpoint(context.task_context)?;
            }
            Ok(control)
        };
        let working = context.source_account("ProjectExec expressions");
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            let (mut values, mut layout) =
                prepare_projection_values(items, &binding, context, &working, &prepared)?;
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
                (values, layout) =
                    prepare_projection_values(items, &binding, context, &working, &prepared)?;
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
            runtime_checkpoint(context.task_context)?;
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

// Column transfers and literals have an exact borrowed sizing path and no
// evaluator to run twice. Bound this batch's reservation before allocation;
// retain the existing ceiling for computed expressions or graph-only columns.
fn projection_batch_budget(
    items: &[Projection],
    batch: &[Binding],
    ceiling: NonZeroUsize,
    preserves_annotations: bool,
) -> NonZeroUsize {
    // Duplicate output names gain #N suffixes during projection. Retain the
    // ceiling unless this bounded shape proves the original names are final.
    if items.len() > 64
        || items.iter().any(|item| item.name.len() > 128)
        || items.iter().enumerate().any(|(index, item)| {
            items[..index]
                .iter()
                .any(|previous| previous.name == item.name)
        })
        || preserves_annotations
        || batch
            .iter()
            .any(|row| !row.nodes.is_empty() || !row.relationships.is_empty())
        || !items.iter().all(|item| match &item.expression {
            ProjectionExpression::Literal(_) => true,
            ProjectionExpression::Column(name) => {
                batch.iter().all(|row| row.values.contains_key(name))
            }
            _ => false,
        })
    {
        return ceiling;
    }
    let bytes = batch.iter().fold(0usize, |total, row| {
        total.saturating_add(binding_memory_bytes_with_values(
            row,
            items.iter().filter_map(|item| {
                let value = match &item.expression {
                    ProjectionExpression::Literal(value) => Some(value),
                    ProjectionExpression::Column(name) => row.values.get(name),
                    _ => None,
                }?;
                Some((item.name.as_str(), value))
            }),
        ))
    });
    NonZeroUsize::new(bytes.min(ceiling.get())).unwrap_or(NonZeroUsize::MIN)
}

fn prepare_projection_values<'a>(
    items: &'a [Projection],
    binding: &'a Binding,
    context: BatchExecutionContext<'a>,
    working: &QueryMemoryAccount,
    prepared: &'a PreparedProjectionExpressions<'a>,
) -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)> {
    prepare_borrowed_projection(
        items,
        working,
        context.observer.seed_graph_scoring_input().map(|_| binding),
        |expression| {
            prepared.evaluate(
                expression,
                context.catalog,
                binding,
                Some(working),
                context.observer,
            )
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
    runtime_checkpoint(context.task_context)?;
    let skipped = Cell::new(0usize);
    let emitted = Cell::new(0usize);
    let consumer_stopped = Cell::new(false);
    let output_cap = match (limit, execution_limit.output_rows) {
        (Some(limit), Some(parent)) => (limit).min(parent),
        (Some(limit), None) => limit,
        (None, Some(parent)) => parent,
        (None, None) => usize::MAX,
    };
    let control =
        source.execute(
            input,
            ExecutionLimit {
                output_rows: Some(offset.saturating_add(output_cap)),
            },
            &mut |batch| {
                runtime_checkpoint(context.task_context)?;
                // These bindings already exist. Admit only the payload that can
                // cross this window, before allocating its output vector. Reserving
                // the entire configured batch ceiling made a tiny Sort + LIMIT
                // require 16 MiB even when its selected rows fit a small query.
                let skip = offset.saturating_sub(skipped.get()).min(batch.len());
                let remaining = output_cap.saturating_sub(emitted.get());
                let payload = batch.iter().skip(skip).take(remaining).try_fold(
                    0usize,
                    |total, binding| {
                        runtime_checkpoint(context.task_context)?;
                        Ok::<_, hawdb_core::HawDBError>(
                            total
                                .saturating_add(binding_memory_bytes(binding))
                                .min(context.memory.batch_payload_bytes.get()),
                        )
                    },
                )?;
                let mut output = TransformBatchBuilder::new(
                    "LimitExec",
                    context.memory.batch_rows.get(),
                    std::num::NonZeroUsize::new(payload.max(1)).expect("positive transfer budget"),
                    context.memory_ledger,
                )?;
                let mut emit_output = |batch: BindingBatch| {
                    emitted.set(emitted.get().saturating_add(batch.len()));
                    let control = emit(batch)?;
                    consumer_stopped.set(consumer_stopped.get() || control == BatchControl::Stop);
                    if control == BatchControl::Continue {
                        runtime_checkpoint(context.task_context)?;
                    }
                    Ok(control)
                };
                for binding in batch {
                    runtime_checkpoint(context.task_context)?;
                    if skipped.get() < offset {
                        skipped.set(skipped.get().saturating_add(1));
                        continue;
                    }
                    if emitted.get().saturating_add(output.len()) >= output_cap {
                        break;
                    }
                    if output.reserve_row_before_allocation(
                        binding_memory_bytes(&binding),
                        &mut emit_output,
                    )? == BatchControl::Stop
                    {
                        return Ok(BatchControl::Stop);
                    }
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
