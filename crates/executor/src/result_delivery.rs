//! Query-result admission, consumer delivery, and memory-lease lifetime.

use crate::binding::{map_memory_bytes, map_payload_bytes, Binding};
use crate::pipeline::runtime_checkpoint;
use crate::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, Row};
use skein_core::{Result, RuntimeTaskContext, SkeinError};
use std::num::NonZeroUsize;

#[derive(Clone, Copy)]
pub enum ConsumerMemoryMode {
    /// The consumer keeps emitted rows alive through the final memory snapshot.
    Retained,
    /// Each row can be uncharged as soon as the consumer call returns.
    ReleasedAfterCall,
    /// Rows remain query-owned until all output budgets have been validated.
    DeferredUntilValidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDelivery {
    /// Keep bounded rows query-owned until every output limit is validated.
    Validated,
    /// Deliver rows as produced and let the caller surface terminal failures.
    Incremental,
}

impl StreamDelivery {
    pub fn consumer_memory_mode(self, bounded: bool) -> ConsumerMemoryMode {
        match (self, bounded) {
            (Self::Validated, true) => ConsumerMemoryMode::DeferredUntilValidated,
            (Self::Validated, false) | (Self::Incremental, _) => {
                ConsumerMemoryMode::ReleasedAfterCall
            }
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct OutputLimits {
    pub max_rows: Option<usize>,
    pub max_payload_bytes: Option<usize>,
}

#[derive(Clone, Copy, Default)]
pub struct OutputMetrics {
    pub rows: usize,
    pub payload_bytes: usize,
}

pub struct QueryOutputAccumulator<'a> {
    consumer: &'a mut dyn FnMut(Row) -> Result<()>,
    memory_mode: ConsumerMemoryMode,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    result_account: QueryMemoryAccount,
    retained_result_lease: QueryMemoryLease,
    deferred_rows: Vec<Row>,
    metrics: OutputMetrics,
}

impl<'a> QueryOutputAccumulator<'a> {
    pub fn new(
        limits: OutputLimits,
        result_memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
        memory_mode: ConsumerMemoryMode,
        consumer: &'a mut dyn FnMut(Row) -> Result<()>,
    ) -> Result<Self> {
        let result_account = memory_ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "query result",
            result_memory_budget,
        );
        let retained_result_lease = result_account.reserve(0)?;
        Ok(Self {
            consumer,
            memory_mode,
            max_rows: limits.max_rows,
            max_payload_bytes: limits.max_payload_bytes,
            result_account,
            retained_result_lease,
            deferred_rows: Vec::new(),
            metrics: OutputMetrics::default(),
        })
    }

    pub fn emit(&mut self, binding: Binding) -> Result<()> {
        if let Some(max_rows) = self.max_rows
            && self.metrics.rows >= max_rows
        {
            return Err(SkeinError::Execution(format!(
                "read query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
        }

        let row = binding.values;
        let row_payload_bytes = map_payload_bytes(&row);
        let next_payload_bytes = self.metrics.payload_bytes.saturating_add(row_payload_bytes);
        if let Some(max_payload_bytes) = self.max_payload_bytes
            && next_payload_bytes > max_payload_bytes
        {
            return Err(SkeinError::Execution(format!(
                "read query payload would exceed max_payload_bytes {max_payload_bytes} (max_read_result_payload_bytes {max_payload_bytes}; next total {next_payload_bytes})"
            )));
        }
        let row_memory_bytes = map_memory_bytes(&row);
        let transient_result_lease = match self.memory_mode {
            ConsumerMemoryMode::Retained | ConsumerMemoryMode::DeferredUntilValidated => {
                self.retained_result_lease.grow(row_memory_bytes)?;
                None
            }
            ConsumerMemoryMode::ReleasedAfterCall => {
                Some(self.result_account.reserve(row_memory_bytes)?)
            }
        };

        match self.memory_mode {
            ConsumerMemoryMode::Retained | ConsumerMemoryMode::ReleasedAfterCall => {
                (self.consumer)(row)?;
            }
            ConsumerMemoryMode::DeferredUntilValidated => self.deferred_rows.push(row),
        }
        drop(transient_result_lease);
        self.metrics.rows = self.metrics.rows.saturating_add(1);
        self.metrics.payload_bytes = next_payload_bytes;
        Ok(())
    }

    pub fn metrics(&self) -> OutputMetrics {
        self.metrics
    }

    pub fn finish_delivery(&mut self, task_context: Option<&RuntimeTaskContext>) -> Result<()> {
        if !matches!(self.memory_mode, ConsumerMemoryMode::DeferredUntilValidated) {
            return Ok(());
        }
        for row in self.deferred_rows.drain(..) {
            runtime_checkpoint(task_context)?;
            (self.consumer)(row)?;
            runtime_checkpoint(task_context)?;
        }
        self.retained_result_lease.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
