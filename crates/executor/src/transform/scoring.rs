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

use crate::binding::{binding_memory_bytes, value_payload_bytes};
use crate::blocking::stream_top_n_batches;
use crate::observer::ExecutionObserver;
use crate::pipeline::{
    runtime_checkpoint, BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
    TransformBatchBuilder,
};
use crate::{BlockingOperatorMemoryReport, ExecutionLimit, QueryMemoryClass};
use hawdb_core::graph_rag::ScoringSpec;
use hawdb_core::{HawDBError, Result, Value};
use hawdb_plan_cypher::{
    PhysicalPlan, ProjectionExpression, ScalarBinaryOp, SortDirection, SortItem, SortKey,
    SCORING_RERANK_SCORE_COLUMN,
};
use hawdb_storage::scan::ScanPruningReport;

/// Scores the complete candidate stream and ranks through the shared,
/// accounted TopN implementation, including its spill and stable-tie rules.
#[allow(clippy::too_many_arguments)]
pub fn stream_scoring_rerank_batches(
    input: &PhysicalPlan,
    score_column: &str,
    spec: &ScoringSpec,
    limit: usize,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    spec.validate().map_err(|error| {
        HawDBError::Execution(format!("invalid scoring specification: {error}"))
    })?;
    let retained = limit.min(execution_limit.output_rows.unwrap_or(usize::MAX));
    if retained == 0 {
        return Ok(BatchControl::Continue);
    }
    let mut scored = ScoringSource {
        source,
        score_column,
        spec,
        context,
        reference_time: crate::scoring::reference_time_millis(),
    };
    let observer = ScoringObserver(context.observer);
    stream_top_n_batches(
        input,
        &[SortItem {
            // The previous scorer used numerical float equality: -0.0 and
            // +0.0 tie. Normalize only the ordering value and preserve the
            // reported arithmetic score, including its sign bit.
            key: SortKey::Expression(ProjectionExpression::Binary {
                left: Box::new(ProjectionExpression::Column(
                    SCORING_RERANK_SCORE_COLUMN.to_string(),
                )),
                op: ScalarBinaryOp::Add,
                right: Box::new(ProjectionExpression::Literal(Value::Float(0.0))),
            }),
            direction: SortDirection::Desc,
        }],
        0,
        retained,
        &mut scored,
        BatchExecutionContext {
            observer: &observer,
            ..context
        },
        execution_limit,
        emit,
    )
}

struct ScoringObserver<'a>(&'a dyn ExecutionObserver);

impl ExecutionObserver for ScoringObserver<'_> {
    fn record_scan_pruning_report(&self, report: ScanPruningReport) {
        self.0.record_scan_pruning_report(report);
    }

    fn record_blocking_memory_report(&self, mut report: BlockingOperatorMemoryReport) {
        if report.operator == "TopNExec" {
            report.operator = "ScoringRerankExec".to_string();
        }
        self.0.record_blocking_memory_report(report);
    }
}

struct ScoringSource<'a, 'runtime> {
    source: &'a mut dyn BindingBatchSource,
    score_column: &'a str,
    spec: &'a ScoringSpec,
    context: BatchExecutionContext<'runtime>,
    reference_time: u64,
}

impl BindingBatchSource for ScoringSource<'_, '_> {
    fn execute(
        &mut self,
        input: &PhysicalPlan,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let context = self.context;
        self.source.execute(input, execution_limit, &mut |batch| {
            runtime_checkpoint(context.task_context)?;
            let input_bytes = batch.iter().fold(0usize, |total, row| {
                total.saturating_add(binding_memory_bytes(row))
            });
            let input_account = context.memory_ledger.account(
                QueryMemoryClass::PipelineBatch,
                "ScoringRerankExec input",
                context.memory.batch_payload_bytes,
            );
            let mut input_lease = input_account.reserve(input_bytes)?;
            let mut output = TransformBatchBuilder::new(
                "ScoringRerankExec",
                context.memory.batch_rows.get(),
                context.memory.batch_payload_bytes,
                context.memory_ledger,
            )?;
            let mut output_bytes = 0usize;
            for mut binding in batch {
                runtime_checkpoint(context.task_context)?;
                let features =
                    crate::scoring::BindingScoreFeatures::new(&binding, self.score_column);
                let score = self.spec.evaluate_score(&features, self.reference_time);
                runtime_checkpoint(context.task_context)?;
                if !score.is_finite() {
                    return Err(HawDBError::Execution(
                        "combined scoring result must be finite".to_string(),
                    ));
                }
                let original_bytes = binding_memory_bytes(&binding);
                let bytes = match binding.values.get(SCORING_RERANK_SCORE_COLUMN) {
                    Some(previous) => original_bytes
                        .saturating_sub(value_payload_bytes(previous))
                        .saturating_add(std::mem::size_of::<f64>()),
                    None => original_bytes
                        .saturating_add(SCORING_RERANK_SCORE_COLUMN.len())
                        .saturating_add(std::mem::size_of::<f64>())
                        .saturating_add(std::mem::size_of::<usize>() * 6),
                };
                if bytes > context.memory.batch_payload_bytes.get() {
                    return Err(HawDBError::Execution(format!(
                        "scored row uses {bytes} bytes, exceeding batch_payload_bytes {}",
                        context.memory.batch_payload_bytes
                    )));
                }
                if !output.is_empty()
                    && output_bytes.saturating_add(bytes) > context.memory.batch_payload_bytes.get()
                {
                    if output.emit(emit)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    output_bytes = 0;
                }
                // Reserve and check the final footprint before creating the
                // score column. The scalar evaluator allocates no report.
                output.reserve_before_allocation()?;
                binding
                    .values
                    .insert(SCORING_RERANK_SCORE_COLUMN.to_string(), Value::Float(score));
                input_lease.shrink(original_bytes);
                output_bytes = output_bytes.saturating_add(bytes);
                output.push(binding);
                if output.is_full() {
                    if output.emit(emit)? == BatchControl::Stop {
                        return Ok(BatchControl::Stop);
                    }
                    output_bytes = 0;
                }
            }
            if !output.is_empty() && output.emit(emit)? == BatchControl::Stop {
                return Ok(BatchControl::Stop);
            }
            Ok(BatchControl::Continue)
        })
    }
}
