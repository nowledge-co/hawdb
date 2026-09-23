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

//! Internal memory and spill-budget primitives shared by physical operators.

use crate::binding::{binding_memory_bytes, Binding};
use crate::spill::{SpillPool, SpillRun, SpillWriteReservation, SpillWriter};
use crate::{
    ExecutionMemoryConfig, QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease,
    QueryMemoryLedger,
};
use hawdb_core::{HawDBError, Result};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub struct OperatorMemoryTracker {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    lease: Option<QueryMemoryLease>,
}

impl OperatorMemoryTracker {
    pub fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            budget_bytes: budget_bytes.get(),
            used_bytes: 0,
            peak_bytes: 0,
            lease: None,
        }
    }

    pub fn with_account(budget_bytes: NonZeroUsize, account: QueryMemoryAccount) -> Self {
        Self {
            budget_bytes: budget_bytes.get(),
            used_bytes: 0,
            peak_bytes: 0,
            lease: Some(
                account
                    .reserve(0)
                    .expect("zero-byte query memory reservation cannot fail"),
            ),
        }
    }

    pub fn would_exceed(&self, bytes: usize) -> bool {
        self.used_bytes.saturating_add(bytes) > self.budget_bytes
    }

    pub fn try_charge(&mut self, bytes: usize) -> Result<()> {
        if self.would_exceed(bytes) {
            return Err(HawDBError::Execution(format!(
                "operator state would use {} bytes, exceeding its {}-byte budget",
                self.used_bytes.saturating_add(bytes),
                self.budget_bytes
            )));
        }
        if let Some(lease) = self.lease.as_mut() {
            lease.grow(bytes)?;
        }
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.peak_bytes = self.peak_bytes.max(self.used_bytes);
        Ok(())
    }

    pub fn release(&mut self, bytes: usize) {
        let released = bytes.min(self.used_bytes);
        if let Some(lease) = self.lease.as_mut() {
            lease.shrink(released);
        }
        self.used_bytes -= released;
    }

    pub fn reset(&mut self) {
        if let Some(lease) = self.lease.as_mut() {
            lease.reset();
        }
        self.used_bytes = 0;
    }

    pub(crate) fn transfer_to(
        &mut self,
        source_bytes: usize,
        target: &mut Self,
        target_bytes: usize,
    ) -> Result<()> {
        if source_bytes > self.used_bytes {
            return Err(HawDBError::Execution(format!(
                "operator memory transfer tried to release {source_bytes} bytes while using {} bytes",
                self.used_bytes
            )));
        }
        if target.would_exceed(target_bytes) {
            return Err(HawDBError::Execution(format!(
                "operator memory transfer would use {} bytes, exceeding its {}-byte budget",
                target.used_bytes.saturating_add(target_bytes),
                target.budget_bytes
            )));
        }
        match (&mut self.lease, &mut target.lease) {
            (Some(source), Some(target)) => {
                source.transfer_to(source_bytes, target, target_bytes)?;
            }
            (None, None) => {}
            _ => {
                return Err(HawDBError::Execution(
                    "operator memory transfer cannot cross accounted and unaccounted trackers"
                        .to_string(),
                ));
            }
        }
        self.used_bytes -= source_bytes;
        target.used_bytes = target.used_bytes.saturating_add(target_bytes);
        target.peak_bytes = target.peak_bytes.max(target.used_bytes);
        Ok(())
    }
}

pub struct SpillBudgetTracker {
    operator: &'static str,
    pool: std::result::Result<SpillPool, String>,
    pub max_bytes: u64,
    pub max_runs: usize,
    used_bytes: AtomicU64,
    run_count: AtomicUsize,
    staging_account: Option<QueryMemoryAccount>,
}

impl SpillBudgetTracker {
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    pub fn run_count(&self) -> usize {
        self.run_count.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn seed_usage(&self, used_bytes: u64, run_count: usize) {
        self.used_bytes.store(used_bytes, Ordering::Release);
        self.run_count.store(run_count, Ordering::Release);
    }

    pub(crate) fn staging_peak_bytes(&self) -> usize {
        self.staging_account
            .as_ref()
            .map_or(0, QueryMemoryAccount::peak_bytes)
    }

    pub fn new(operator: &'static str, memory: &ExecutionMemoryConfig) -> Self {
        Self {
            operator,
            pool: SpillPool::open(memory).map_err(|error| error.to_string()),
            max_bytes: memory.max_spill_bytes.get(),
            max_runs: memory.max_spill_runs.get(),
            used_bytes: AtomicU64::new(0),
            run_count: AtomicUsize::new(0),
            staging_account: None,
        }
    }

    pub fn with_ledger(
        operator: &'static str,
        memory: &ExecutionMemoryConfig,
        memory_ledger: &QueryMemoryLedger,
    ) -> Self {
        Self::with_ledger_staging_budget(
            operator,
            memory,
            memory_ledger,
            memory.blocking_operator_bytes,
        )
    }

    pub fn with_ledger_staging_budget(
        operator: &'static str,
        memory: &ExecutionMemoryConfig,
        memory_ledger: &QueryMemoryLedger,
        staging_budget: NonZeroUsize,
    ) -> Self {
        Self {
            staging_account: Some(memory_ledger.account(
                QueryMemoryClass::SpillStaging,
                format!("{operator} spill staging"),
                staging_budget,
            )),
            ..Self::new(operator, memory)
        }
    }

    pub(crate) fn reserve_staging(&self, bytes: usize) -> Result<Option<QueryMemoryLease>> {
        self.staging_account
            .as_ref()
            .map(|account| account.reserve(bytes))
            .transpose()
    }

    pub fn create_run(&self, file_operator: &str) -> Result<(SpillRun, SpillWriter)> {
        self.create_run_with_buffer_bytes(
            file_operator,
            NonZeroUsize::new(crate::spill::SPILL_IO_BUFFER_BYTES)
                .expect("spill buffer is non-zero"),
        )
    }

    pub(crate) fn create_run_with_buffer_bytes(
        &self,
        file_operator: &str,
        buffer_bytes: NonZeroUsize,
    ) -> Result<(SpillRun, SpillWriter)> {
        let pool = self.pool.as_ref().map_err(|error| {
            HawDBError::Execution(format!("{} spill pool unavailable: {error}", self.operator))
        })?;
        let mut current = self.run_count.load(Ordering::Acquire);
        loop {
            if current >= self.max_runs {
                return Err(HawDBError::Execution(format!(
                    "{} exceeded max_spill_runs {}",
                    self.operator, self.max_runs
                )));
            }
            match self.run_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        match SpillRun::create_with_buffer_bytes(pool.clone(), file_operator, buffer_bytes) {
            Ok(run) => Ok(run),
            Err(error) => {
                // A run that never opened must not consume its slot.
                self.run_count.fetch_sub(1, Ordering::AcqRel);
                Err(error)
            }
        }
    }

    /// Reserves one spill write against both the operator byte budget and the
    /// shared pool. The returned guard returns the operator bytes when the
    /// write fails before committing, so concurrent writers cannot double
    /// spend the budget.
    pub(crate) fn reserve_write(
        &self,
        bytes: u64,
    ) -> Result<(SpillWriteReservation, SpillBudgetReservation<'_>)> {
        let mut current = self.used_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes).ok_or_else(|| {
                HawDBError::Execution(format!(
                    "{} exceeded max_spill_bytes {} (byte count overflow)",
                    self.operator, self.max_bytes
                ))
            })?;
            if next > self.max_bytes {
                return Err(HawDBError::Execution(format!(
                    "{} exceeded max_spill_bytes {} (next total {})",
                    self.operator, self.max_bytes, next
                )));
            }
            match self.used_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        let pool = match self.pool.as_ref() {
            Ok(pool) => pool,
            Err(error) => {
                self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
                return Err(HawDBError::Execution(format!(
                    "{} spill pool unavailable: {error}",
                    self.operator
                )));
            }
        };
        match pool.reserve_bytes(self.operator, bytes) {
            Ok(reservation) => Ok((
                reservation,
                SpillBudgetReservation {
                    tracker: self,
                    bytes,
                    committed: false,
                },
            )),
            Err(error) => {
                self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
                Err(error)
            }
        }
    }
}

/// Returns an operator's reserved spill bytes when a write fails before it
/// commits; committing keeps the reservation for the rest of the query.
#[must_use]
pub(crate) struct SpillBudgetReservation<'a> {
    tracker: &'a SpillBudgetTracker,
    bytes: u64,
    committed: bool,
}

impl SpillBudgetReservation<'_> {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for SpillBudgetReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.tracker
                .used_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

pub fn ensure_operator_item_fits(
    operator: &str,
    bytes: usize,
    tracker: &OperatorMemoryTracker,
) -> Result<()> {
    if bytes > tracker.budget_bytes {
        return Err(HawDBError::Execution(format!(
            "{operator} item uses {bytes} bytes, exceeding blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    Ok(())
}

pub fn push_bounded_operator_binding(
    operator: &str,
    output: &mut Vec<Binding>,
    binding: Binding,
    tracker: &mut OperatorMemoryTracker,
) -> Result<()> {
    let bytes = binding_memory_bytes(&binding);
    ensure_operator_item_fits(operator, bytes, tracker)?;
    if tracker.would_exceed(bytes) {
        return Err(HawDBError::Execution(format!(
            "{operator} state exceeds blocking_operator_bytes {}",
            tracker.budget_bytes
        )));
    }
    tracker.try_charge(bytes)?;
    output.push(binding);
    Ok(())
}

pub fn collect_bounded_operator_bindings_with_account(
    operator: &str,
    bindings: impl IntoIterator<Item = Binding>,
    memory_budget: NonZeroUsize,
    account: QueryMemoryAccount,
) -> Result<Vec<Binding>> {
    let mut output = Vec::new();
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, account);
    for binding in bindings {
        push_bounded_operator_binding(operator, &mut output, binding, &mut tracker)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn operator_tracker_rejects_state_beyond_budget() {
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(1).unwrap());
        let error = push_bounded_operator_binding(
            "test",
            &mut Vec::new(),
            Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::new(),
                relationships: BTreeMap::new(),
            },
            &mut tracker,
        )
        .unwrap_err();

        assert!(error.to_string().contains("blocking_operator_bytes"));
    }
}
