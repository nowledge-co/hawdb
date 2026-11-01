// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Expand a list for each upstream row without materializing the input relation.

use super::*;
use crate::expression::evaluate_projection_borrowed;
use crate::pipeline::AccountedBindingBatch;
use hawdb_core::{HawDBError, Value};

pub fn stream_unwind_rows_batches(
    expression: &ProjectionExpression,
    variable: &str,
    input: &PhysicalPlan,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    if execution_limit.is_reached(0) {
        return Ok(BatchControl::Stop);
    }
    let working = context.source_account("UnwindRowsExec expressions");
    let mut output = AccountedBindingBatch::with_ledger(
        "UnwindRowsExec",
        context.memory.batch_rows.get(),
        context.memory.batch_payload_bytes,
        context.memory_ledger,
    );
    let mut produced = 0usize;
    let control = source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        for binding in batch {
            runtime_checkpoint(context.task_context)?;
            if execution_limit.is_reached(produced) {
                return Ok(BatchControl::Stop);
            }
            // WITH may leave physically retained graph slots after their names
            // leave logical scope. A new scalar column can reuse such a name.
            if binding.values.contains_key(variable) {
                return Err(HawDBError::Semantic(format!(
                    "UNWIND variable '{variable}' is already bound"
                )));
            }
            let value = evaluate_projection_borrowed(
                expression,
                context.catalog,
                &binding,
                Some(&working),
            )?;
            let values = match value.as_ref() {
                Value::List(values) => values,
                Value::Null => continue,
                _ => {
                    return Err(HawDBError::Execution(
                        "UNWIND source must resolve to a list value or NULL".into(),
                    ))
                }
            };
            for value in values {
                runtime_checkpoint(context.task_context)?;
                if execution_limit.is_reached(produced) {
                    return Ok(BatchControl::Stop);
                }
                let bytes = binding_memory_bytes_with_values(
                    &binding,
                    binding
                        .values
                        .iter()
                        .map(|(name, value)| (name.as_str(), value))
                        .chain(std::iter::once((variable, value))),
                );
                if output.push_generated(
                    bytes,
                    || {
                        let mut row = binding.clone();
                        row.values.insert(variable.to_owned(), value.clone());
                        row
                    },
                    emit,
                )? == BatchControl::Stop
                {
                    return Ok(BatchControl::Stop);
                }
                produced = produced.saturating_add(1);
                if output.is_full() && output.emit(emit)? == BatchControl::Stop {
                    return Ok(BatchControl::Stop);
                }
            }
        }
        Ok(BatchControl::Continue)
    })?;
    if output.emit(emit)? == BatchControl::Stop {
        return Ok(BatchControl::Stop);
    }
    Ok(control)
}
