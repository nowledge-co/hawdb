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

use super::scoring::ScoringObserver;
use crate::binding::{binding_memory_bytes, binding_memory_bytes_replacing_value, Binding};
use crate::blocking::stream_top_n_batches;
use crate::pipeline::{
    runtime_checkpoint, BatchControl, BatchExecutionContext, BindingBatch, BindingBatchSource,
    TransformBatchBuilder,
};
use crate::scoring::{
    execute_host_scorer_with_identity, AccountedHostScores, BindingScoreFeatures,
    FrozenHostScorerIdentity, HostScorer, HostScorerBatch, HostScorerDescriptor,
};
use crate::{ExecutionLimit, QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::graph_rag::ScoringFeatureSource;
use hawdb_core::{HawDBError, Result, Value};
use hawdb_plan_cypher::{
    HostScoringRankPolicy, PhysicalPlan, ProjectionExpression, ScalarBinaryOp, SortDirection,
    SortItem, SortKey, SCORING_RERANK_SCORE_COLUMN,
};
use std::num::NonZeroUsize;

mod bands;
use bands::{EvidenceBands, EVIDENCE_BAND_COLUMN};

/// A finite complete-cohort contract, independent of the final result window.
#[derive(Clone, Copy)]
pub struct HostScoringOptions<'a> {
    pub score_column: &'a str,
    pub max_candidate_rows: NonZeroUsize,
    pub reference_time_millis: u64,
    pub limit: usize,
    /// Cached plan identity, when attached to an ordinary query. Direct kernel
    /// callers freeze their descriptor before reading any candidates.
    pub expected_identity: Option<HostScorerDescriptor<'a>>,
    pub rank_policy: &'a HostScoringRankPolicy,
}

/// Stage the complete admitted candidate stream, call the host once, and rank
/// validated scores through the existing stable, accounted, spillable TopN.
/// Cohort-dependent callbacks require resident features and fail closed when
/// that finite cohort cannot fit; their input is never truncated to final K.
pub fn stream_host_scoring_batches(
    input: &PhysicalPlan,
    options: HostScoringOptions<'_>,
    scorer: &mut dyn HostScorer,
    source: &mut dyn BindingBatchSource,
    context: BatchExecutionContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    runtime_checkpoint(context.task_context)?;
    options.rank_policy.validate(options.score_column)?;
    let descriptor = options
        .expected_identity
        .unwrap_or_else(|| scorer.descriptor());
    descriptor.ensure_matches(scorer.descriptor())?;
    let retained = options
        .limit
        .min(execution_limit.output_rows.unwrap_or(usize::MAX));
    if retained == 0 {
        return Ok(BatchControl::Continue);
    }
    let account = context.operator_account("HostScoringExec cohort");
    let mut cohort = HostScoringCohort {
        rows: Vec::new(),
        reservation: account.reserve(0)?,
    };
    let control = source.execute(input, ExecutionLimit::unlimited(), &mut |batch| {
        runtime_checkpoint(context.task_context)?;
        if batch.len()
            > options
                .max_candidate_rows
                .get()
                .saturating_sub(cohort.rows.len())
        {
            return Err(HawDBError::Execution(format!(
                "host scoring candidate stream exceeds candidate limit {}",
                options.max_candidate_rows
            )));
        }
        if !batch.is_empty() && cohort.rows.capacity() == 0 {
            let slots = options
                .max_candidate_rows
                .get()
                .checked_mul(std::mem::size_of::<Binding>())
                .ok_or_else(|| HawDBError::Execution("host scoring cohort size overflow".into()))?;
            cohort.reservation.grow(slots)?;
            cohort.rows.reserve_exact(options.max_candidate_rows.get());
        }
        for row in batch {
            runtime_checkpoint(context.task_context)?;
            // The complete fixed-capacity row array is already reserved. Charge
            // the incoming row's separately owned payload before retaining it.
            let payload = binding_memory_bytes(&row).saturating_sub(std::mem::size_of::<Binding>());
            cohort.reservation.grow(payload)?;
            cohort.rows.push(row);
        }
        Ok(BatchControl::Continue)
    })?;
    if control == BatchControl::Stop {
        return Err(HawDBError::Execution(
            "host scoring candidate source stopped before completing its cohort".into(),
        ));
    }
    runtime_checkpoint(context.task_context)?;
    if cohort.rows.is_empty() {
        return Ok(BatchControl::Continue);
    }
    let bands = EvidenceBands::for_policy(&cohort.rows, options, &account, context)?;
    // Keep the planned/pre-source identity. A source may change shared host
    // configuration while staging; that must never establish a new baseline.
    descriptor.ensure_matches(scorer.descriptor())?;
    let identity = FrozenHostScorerIdentity::new(descriptor, &account)?;
    let scores = score_cohort(&cohort.rows, options, scorer, &identity, &account, context)?;
    drop(identity);
    // The new policy's constant ranking metadata must also fit alongside the
    // resident cohort, scores and band array before constructing its keys.
    let _rank_metadata = bands.as_ref().map(|_| account.reserve(1024)).transpose()?;
    let rank_items = [
        SortItem {
            key: SortKey::Column(if bands.is_some() {
                EVIDENCE_BAND_COLUMN.into()
            } else {
                String::new()
            }),
            direction: SortDirection::Asc,
        },
        SortItem {
            key: if bands.is_some() {
                // Mem's secondary comparator uses exact total_cmp, including
                // signed zero, while the default score policy retains its
                // existing numeric-zero normalization.
                SortKey::Column(SCORING_RERANK_SCORE_COLUMN.into())
            } else {
                SortKey::Expression(ProjectionExpression::Binary {
                    left: Box::new(ProjectionExpression::Column(
                        SCORING_RERANK_SCORE_COLUMN.into(),
                    )),
                    op: ScalarBinaryOp::Add,
                    right: Box::new(ProjectionExpression::Literal(Value::Float(0.0))),
                })
            },
            direction: SortDirection::Desc,
        },
    ];
    let preserve_bands = bands.is_some();
    // TopN and the still-live resident cohort share one operator allowance.
    // Reserve headroom conservatively before dispatch; progressively released
    // cohort payload does not silently widen this invocation's TopN budget.
    let remaining = account.available_bytes();
    let remaining = NonZeroUsize::new(remaining).ok_or_else(|| {
        HawDBError::Execution("host scoring leaves no TopN memory allowance".into())
    })?;
    let mut memory = context.memory.clone();
    memory.blocking_operator_bytes = remaining;
    let observer = ScoringObserver(context.observer, "HostScoringExec");
    let mut scored = ScoredHostCohort {
        rows: cohort.rows.into_iter(),
        scores,
        reservation: cohort.reservation,
        context,
        bands,
    };
    let mut deliver = |mut batch: BindingBatch| {
        if preserve_bands {
            for row in &mut batch {
                runtime_checkpoint(context.task_context)?;
                row.values.remove(EVIDENCE_BAND_COLUMN);
            }
        }
        emit(batch)
    };
    stream_top_n_batches(
        input,
        if preserve_bands {
            &rank_items[..]
        } else {
            &rank_items[1..]
        },
        0,
        retained,
        &mut scored,
        BatchExecutionContext {
            memory: &memory,
            observer: &observer,
            ..context
        },
        execution_limit,
        &mut deliver,
    )
}

struct HostScoringCohort {
    rows: Vec<Binding>,
    reservation: QueryMemoryLease,
}

fn score_cohort(
    rows: &[Binding],
    options: HostScoringOptions<'_>,
    scorer: &mut dyn HostScorer,
    identity: &FrozenHostScorerIdentity,
    account: &QueryMemoryAccount,
    context: BatchExecutionContext<'_>,
) -> Result<AccountedHostScores> {
    let view_bytes = rows
        .len()
        .checked_mul(
            std::mem::size_of::<BindingScoreFeatures<'_>>()
                + std::mem::size_of::<&dyn ScoringFeatureSource>(),
        )
        .ok_or_else(|| HawDBError::Execution("host scoring feature array size overflow".into()))?;
    let _views_reservation = account.reserve(view_bytes)?;
    let mut features = Vec::with_capacity(rows.len());
    for row in rows {
        runtime_checkpoint(context.task_context)?;
        features.push(BindingScoreFeatures::with_seed_graph_input(
            row,
            options.score_column,
            context.observer.seed_graph_scoring_input(),
        ));
    }
    let mut views: Vec<&dyn ScoringFeatureSource> = Vec::with_capacity(rows.len());
    for features in &features {
        runtime_checkpoint(context.task_context)?;
        views.push(features);
    }
    execute_host_scorer_with_identity(
        scorer,
        HostScorerBatch {
            features: &views,
            reference_time_millis: options.reference_time_millis,
            task_context: context.task_context,
            scratch_account: account,
        },
        options.max_candidate_rows,
        Some(identity),
    )
}

struct ScoredHostCohort<'a> {
    rows: std::vec::IntoIter<Binding>,
    scores: AccountedHostScores,
    reservation: QueryMemoryLease,
    context: BatchExecutionContext<'a>,
    bands: Option<EvidenceBands>,
}

impl BindingBatchSource for ScoredHostCohort<'_> {
    fn execute(
        &mut self,
        _input: &PhysicalPlan,
        _execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let context = self.context;
        let mut output = TransformBatchBuilder::new(
            "HostScoringExec",
            context.memory.batch_rows.get(),
            context.memory.batch_payload_bytes,
            context.memory_ledger,
        )?;
        let mut output_bytes = 0usize;
        for (index, (mut binding, score)) in
            self.rows.by_ref().zip(self.scores.scores()).enumerate()
        {
            runtime_checkpoint(context.task_context)?;
            let original_payload =
                binding_memory_bytes(&binding).saturating_sub(std::mem::size_of::<Binding>());
            if context.observer.seed_graph_scoring_input().is_some() {
                crate::scoring::strip_seed_annotations(&mut binding);
            }
            let bytes = binding_memory_bytes_replacing_value(
                &binding,
                SCORING_RERANK_SCORE_COLUMN,
                &Value::Float(0.0),
            )
            .saturating_add(if self.bands.is_some() {
                1024 + EVIDENCE_BAND_COLUMN.len()
            } else {
                0
            });
            if bytes > context.memory.batch_payload_bytes.get() {
                return Err(HawDBError::Execution(format!(
                    "host-scored row uses {bytes} bytes, exceeding batch_payload_bytes {}",
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
            output.reserve_before_allocation()?;
            binding
                .values
                .insert(SCORING_RERANK_SCORE_COLUMN.into(), Value::Float(*score));
            if let Some(bands) = &self.bands {
                binding.values.insert(
                    EVIDENCE_BAND_COLUMN.into(),
                    Value::Int(bands.ordinal(index)),
                );
            }
            self.reservation.shrink(original_payload);
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
    }
}
