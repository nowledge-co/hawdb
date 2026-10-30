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

//! Query-result admission, consumer delivery, and memory-lease lifetime.

use crate::binding::{map_memory_bytes, map_payload_bytes, value_ref_payload_bytes, Binding};
use crate::pipeline::runtime_checkpoint;
use crate::{
    QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, QueryRows,
    QueryRowsBuilder, Row, RowRef,
};
use hawdb_core::{HawDBError, ReadBudgetResource, Result, RuntimeTaskContext};
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

impl OutputLimits {
    pub fn validate(&self, rows: usize, payload_bytes: usize) -> Result<()> {
        if let Some(max_rows) = self.max_rows
            && rows > max_rows
        {
            return Err(HawDBError::read_budget_exceeded(ReadBudgetResource::Rows, max_rows, format!(
                "query returned more than {max_rows} rows, exceeding max_read_result_rows {max_rows}"
            )));
        }
        if let Some(max_payload_bytes) = self.max_payload_bytes
            && payload_bytes > max_payload_bytes
        {
            return Err(HawDBError::read_budget_exceeded(ReadBudgetResource::PayloadBytes, max_payload_bytes, format!(
                "query payload would exceed max_payload_bytes {max_payload_bytes} (max_read_result_payload_bytes {max_payload_bytes}; next total {payload_bytes})"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct OutputMetrics {
    pub rows: usize,
    pub payload_bytes: usize,
}

pub type BorrowedRowConsumer<'a> = dyn for<'row> FnMut(RowRef<'row>) -> Result<()> + 'a;

enum RowConsumer<'a> {
    Owned(&'a mut dyn FnMut(Row) -> Result<()>),
    Borrowed(&'a mut BorrowedRowConsumer<'a>),
}

impl RowConsumer<'_> {
    fn consume_owned(&mut self, row: Row) -> Result<()> {
        match self {
            Self::Owned(consumer) => consumer(row),
            Self::Borrowed(consumer) => consumer(RowRef::from(&row)),
        }
    }
}

pub struct QueryOutputAccumulator<'a> {
    consumer: Option<RowConsumer<'a>>,
    owned_rows: Option<QueryRowsBuilder>,
    memory_mode: ConsumerMemoryMode,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    result_account: QueryMemoryAccount,
    deferred_rows: Vec<Row>,
    // Payload owners must retire before their reservation, including unwinding.
    retained_result_lease: QueryMemoryLease,
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
        Self::new_with_budget_option(
            limits,
            result_memory_budget,
            memory_ledger,
            "QueryMemoryLedger::account(budget_bytes)",
            memory_mode,
            consumer,
        )
    }

    pub fn new_with_budget_option(
        limits: OutputLimits,
        result_memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
        budget_option: &'static str,
        memory_mode: ConsumerMemoryMode,
        consumer: &'a mut dyn FnMut(Row) -> Result<()>,
    ) -> Result<Self> {
        let result_account = memory_ledger.account_with_budget_option(
            QueryMemoryClass::ResultMaterialization,
            "query result",
            result_memory_budget,
            budget_option,
        );
        let retained_result_lease = result_account.reserve(0)?;
        Ok(Self {
            consumer: Some(RowConsumer::Owned(consumer)),
            owned_rows: None,
            memory_mode,
            max_rows: limits.max_rows,
            max_payload_bytes: limits.max_payload_bytes,
            result_account,
            retained_result_lease,
            deferred_rows: Vec::new(),
            metrics: OutputMetrics::default(),
        })
    }

    pub fn new_owned(
        limits: OutputLimits,
        result_memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
    ) -> Result<Self> {
        Self::new_owned_with_budget_option(
            limits,
            result_memory_budget,
            memory_ledger,
            "QueryMemoryLedger::account(budget_bytes)",
        )
    }

    pub fn new_owned_with_budget_option(
        limits: OutputLimits,
        result_memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
        budget_option: &'static str,
    ) -> Result<Self> {
        let result_account = memory_ledger.account_with_budget_option(
            QueryMemoryClass::ResultMaterialization,
            "query result",
            result_memory_budget,
            budget_option,
        );
        let retained_result_lease =
            result_account.reserve(QueryRowsBuilder::owner_memory_bytes())?;
        Ok(Self {
            consumer: None,
            owned_rows: Some(QueryRowsBuilder::new()),
            memory_mode: ConsumerMemoryMode::Retained,
            max_rows: limits.max_rows,
            max_payload_bytes: limits.max_payload_bytes,
            result_account,
            retained_result_lease,
            deferred_rows: Vec::new(),
            metrics: OutputMetrics::default(),
        })
    }

    pub fn new_borrowed_with_budget_option(
        limits: OutputLimits,
        result_memory_budget: NonZeroUsize,
        memory_ledger: &QueryMemoryLedger,
        budget_option: &'static str,
        delivery: StreamDelivery,
        consumer: &'a mut BorrowedRowConsumer<'a>,
    ) -> Result<Self> {
        let memory_mode = delivery
            .consumer_memory_mode(limits.max_rows.is_some() || limits.max_payload_bytes.is_some());
        let deferred = matches!(memory_mode, ConsumerMemoryMode::DeferredUntilValidated);
        let result_account = memory_ledger.account_with_budget_option(
            QueryMemoryClass::ResultMaterialization,
            "query result",
            result_memory_budget,
            budget_option,
        );
        let retained_result_lease = result_account.reserve(if deferred {
            QueryRowsBuilder::owner_memory_bytes()
        } else {
            0
        })?;
        Ok(Self {
            consumer: Some(RowConsumer::Borrowed(consumer)),
            owned_rows: deferred.then(QueryRowsBuilder::new),
            memory_mode,
            max_rows: limits.max_rows,
            max_payload_bytes: limits.max_payload_bytes,
            result_account,
            retained_result_lease,
            deferred_rows: Vec::new(),
            metrics: OutputMetrics::default(),
        })
    }

    /// Bind declared output columns before execution, including empty results.
    /// Retained schema storage is admitted under the result-memory budget.
    pub fn bind_output_schema<'name>(
        &mut self,
        names: impl Iterator<Item = &'name str> + Clone,
    ) -> Result<()> {
        if let Some(rows) = &mut self.owned_rows {
            rows.bind_budgeted_schema(
                names,
                &self.result_account,
                &mut self.retained_result_lease,
            )?;
        }
        Ok(())
    }

    /// Finalize only after execution, output validation, and profile capture.
    /// The accumulator keeps its retained charge until the execution call ends.
    pub fn take_owned_rows(&mut self) -> Option<QueryRows> {
        if self.consumer.is_some() {
            return None;
        }
        self.owned_rows.take().map(QueryRowsBuilder::finish)
    }

    pub fn emit(&mut self, binding: Binding) -> Result<()> {
        let row = binding.values;
        let next_payload_bytes = self.validate_next_row(map_payload_bytes(&row))?;
        if let Some(rows) = &mut self.owned_rows {
            rows.push_budgeted_named_row(
                row,
                &self.result_account,
                &mut self.retained_result_lease,
            )?;
            self.metrics.rows = self.metrics.rows.saturating_add(1);
            self.metrics.payload_bytes = next_payload_bytes;
            return Ok(());
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
                self.consumer
                    .as_mut()
                    .expect("row consumer is present")
                    .consume_owned(row)?;
            }
            ConsumerMemoryMode::DeferredUntilValidated => self.deferred_rows.push(row),
        }
        drop(transient_result_lease);
        self.metrics.rows = self.metrics.rows.saturating_add(1);
        self.metrics.payload_bytes = next_payload_bytes;
        Ok(())
    }

    /// Delivers a view whose source storage and admission remain owned by the
    /// producer for this call. Retained output is copied only after admission;
    /// immediate borrowed consumers observe the source without a row map.
    pub fn emit_borrowed(&mut self, row: RowRef<'_>) -> Result<()> {
        let payload_bytes = row.iter().fold(0usize, |total, (name, value)| {
            total
                .saturating_add(name.len())
                .saturating_add(value_ref_payload_bytes(value))
        });
        let next_payload_bytes = self.validate_next_row(payload_bytes)?;
        if let Some(rows) = &mut self.owned_rows {
            rows.push_budgeted_borrowed_row(
                row,
                &self.result_account,
                &mut self.retained_result_lease,
            )?;
        } else if let Some(RowConsumer::Borrowed(consumer)) = &mut self.consumer {
            consumer(row)?;
        } else {
            let row_bytes = crate::profile::cloned_row_memory_bytes(row)?;
            let transient = match self.memory_mode {
                ConsumerMemoryMode::Retained | ConsumerMemoryMode::DeferredUntilValidated => {
                    self.retained_result_lease.grow(row_bytes)?;
                    None
                }
                ConsumerMemoryMode::ReleasedAfterCall => {
                    Some(self.result_account.reserve(row_bytes)?)
                }
            };
            let owned = row.to_owned_row();
            match self.memory_mode {
                ConsumerMemoryMode::DeferredUntilValidated => self.deferred_rows.push(owned),
                ConsumerMemoryMode::Retained | ConsumerMemoryMode::ReleasedAfterCall => {
                    self.consumer
                        .as_mut()
                        .expect("row consumer is present")
                        .consume_owned(owned)?;
                }
            }
            drop(transient);
        }
        self.metrics.rows = self.metrics.rows.saturating_add(1);
        self.metrics.payload_bytes = next_payload_bytes;
        Ok(())
    }

    fn validate_next_row(&self, row_payload_bytes: usize) -> Result<usize> {
        let next_payload_bytes = self.metrics.payload_bytes.saturating_add(row_payload_bytes);
        let limits = OutputLimits {
            max_rows: self.max_rows,
            max_payload_bytes: self.max_payload_bytes,
        };
        if self.metrics.rows == usize::MAX {
            return Err(HawDBError::Execution("query row count overflow".into()));
        }
        limits.validate(self.metrics.rows + 1, next_payload_bytes)?;
        Ok(next_payload_bytes)
    }

    pub fn metrics(&self) -> OutputMetrics {
        self.metrics
    }

    pub fn finish_delivery(&mut self, task_context: Option<&RuntimeTaskContext>) -> Result<()> {
        if !matches!(self.memory_mode, ConsumerMemoryMode::DeferredUntilValidated) {
            return Ok(());
        }
        if let Some(RowConsumer::Borrowed(consumer)) = &mut self.consumer {
            if let Some(rows) = self.owned_rows.take().map(QueryRowsBuilder::finish) {
                for row in &rows {
                    runtime_checkpoint(task_context)?;
                    consumer(RowRef::from(row))?;
                    runtime_checkpoint(task_context)?;
                }
                drop(rows);
            }
            self.retained_result_lease.reset();
            return Ok(());
        }
        for row in self.deferred_rows.drain(..) {
            runtime_checkpoint(task_context)?;
            self.consumer
                .as_mut()
                .expect("row consumer is present")
                .consume_owned(row)?;
            runtime_checkpoint(task_context)?;
        }
        self.retained_result_lease.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
