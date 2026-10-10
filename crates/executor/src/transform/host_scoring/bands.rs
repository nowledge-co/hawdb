// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::{Binding, HostScoringOptions};
use crate::pipeline::{runtime_checkpoint, BatchExecutionContext};
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{HawDBError, Result, Value};
use hawdb_plan_cypher::HostScoringRankPolicy;

pub(super) const EVIDENCE_BAND_COLUMN: &str = "\0hawdb.scoring.evidence_band";

/// Integer ordering identity, separate from the original and secondary floats.
/// The array is charged before allocation and remains charged through TopN.
pub(super) struct EvidenceBands {
    ordinals: Vec<i64>,
    _reservation: QueryMemoryLease,
}

impl EvidenceBands {
    pub(super) fn for_policy(
        rows: &[Binding],
        options: HostScoringOptions<'_>,
        account: &QueryMemoryAccount,
        context: BatchExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let HostScoringRankPolicy::ContiguousEvidenceTies { reason_column } = options.rank_policy
        else {
            return Ok(None);
        };
        let bytes = rows
            .len()
            .checked_mul(std::mem::size_of::<i64>())
            .ok_or_else(|| {
                HawDBError::Execution("evidence band allocation size overflow".into())
            })?;
        let reservation = account.reserve(bytes)?;
        let mut ordinals = Vec::with_capacity(rows.len());
        let mut previous = None;
        let mut band = 0i64;
        for row in rows {
            runtime_checkpoint(context.task_context)?;
            if row.values.contains_key(EVIDENCE_BAND_COLUMN) {
                return Err(HawDBError::Execution(
                    "candidate contains reserved evidence band metadata".into(),
                ));
            }
            let (Some(Value::Float(score)), Some(Value::String(reason))) = (
                row.values.get(options.score_column),
                row.values.get(reason_column),
            ) else {
                return Err(HawDBError::Execution("contiguous evidence ranking requires a Float score and String reason for every candidate".into()));
            };
            if !score.is_finite() {
                return Err(HawDBError::Execution(
                    "contiguous evidence ranking requires finite direct scores".into(),
                ));
            }
            if let Some((previous_score, previous_reason)) = previous
                && (!score.total_cmp(previous_score).is_eq() || reason != previous_reason)
            {
                band = band.checked_add(1).ok_or_else(|| {
                    HawDBError::Execution("evidence band ordinal overflow".into())
                })?;
            }
            ordinals.push(band);
            previous = Some((score, reason));
        }
        Ok(Some(Self {
            ordinals,
            _reservation: reservation,
        }))
    }

    pub(super) fn ordinal(&self, index: usize) -> i64 {
        self.ordinals[index]
    }
}
