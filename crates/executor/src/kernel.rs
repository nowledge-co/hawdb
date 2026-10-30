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
use hawdb_core::{
    HawDBError, ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, Result,
};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MEMORY_TARGET_SAMPLE_INTERVAL: u8 = 64;

pub struct OperatorMemoryTracker {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    lease: Option<QueryMemoryLease>,
    budget_option: &'static str,
    soft_target_bytes: usize,
    target_checks_remaining: u8,
}

impl OperatorMemoryTracker {
    pub fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            budget_bytes: budget_bytes.get(),
            used_bytes: 0,
            peak_bytes: 0,
            lease: None,
            budget_option: "OperatorMemoryTracker::new(budget_bytes)",
            soft_target_bytes: budget_bytes.get(),
            target_checks_remaining: 0,
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
            budget_option: "OperatorMemoryTracker::with_account(budget_bytes)",
            soft_target_bytes: budget_bytes.get(),
            target_checks_remaining: 0,
        }
    }

    pub(crate) fn with_blocking_account(
        budget_bytes: NonZeroUsize,
        account: QueryMemoryAccount,
    ) -> Self {
        Self {
            budget_option: "ExecutionMemoryConfig.blocking_operator_bytes",
            ..Self::with_account(budget_bytes, account)
        }
    }

    pub(crate) fn limit_error(
        &self,
        requested: usize,
        used: usize,
        budget: usize,
        message: String,
    ) -> HawDBError {
        if let Some(lease) = &self.lease {
            return lease.operator_budget_error(
                requested,
                used,
                budget,
                self.budget_option,
                message,
            );
        }
        HawDBError::from(ResourceBudgetError {
            resource: ResourceBudgetKind::MemoryBytes,
            scope: ResourceBudgetScope::Operator,
            owner: "OperatorMemoryTracker".to_string(),
            memory_class: None,
            requested: requested as u64,
            used: used as u64,
            budget: budget as u64,
            budget_option: self.budget_option,
            message,
        })
    }

    pub(crate) fn state_error(&self, requested: usize, message: String) -> HawDBError {
        self.limit_error(requested, self.used_bytes, self.budget_bytes, message)
    }

    pub fn would_exceed(&self, bytes: usize) -> bool {
        self.used_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.budget_bytes)
    }

    /// Soft pressure only requests a spill at a caller-owned safe point. Keep
    /// two incoming rows and IO progress room; never spill an empty work area.
    /// Sample shared host feedback once per 64 checks, including checks without
    /// growth (join probes). Hard limits still apply to every allocation.
    pub(crate) fn should_spill(
        &mut self,
        bytes: usize,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> bool {
        if self.would_exceed(bytes) {
            return true;
        }
        let Some(task) = task else {
            return false;
        };
        if self.target_checks_remaining == 0 {
            self.soft_target_bytes = task.working_memory_target(self.budget_bytes as u64) as usize;
            self.target_checks_remaining = MEMORY_TARGET_SAMPLE_INTERVAL;
        }
        self.target_checks_remaining -= 1;
        let floor = bytes
            .saturating_mul(2)
            .saturating_add(crate::spill::SPILL_IO_BUFFER_BYTES * 2)
            .min(self.budget_bytes);
        self.used_bytes > 0
            && self.used_bytes.saturating_add(bytes) > self.soft_target_bytes.max(floor)
    }

    fn ensure_charge_fits(&self, bytes: usize, context: &str) -> Result<()> {
        if !self.would_exceed(bytes) {
            return Ok(());
        }
        let message = match self.used_bytes.checked_add(bytes) {
            Some(total) => format!(
                "{context} would use {total} bytes, exceeding its {}-byte budget",
                self.budget_bytes
            ),
            None => format!(
                "{context} byte accounting overflow while adding {bytes} bytes to {} bytes",
                self.used_bytes
            ),
        };
        Err(self.state_error(bytes, message))
    }

    pub fn try_charge(&mut self, bytes: usize) -> Result<()> {
        self.ensure_charge_fits(bytes, "operator state")?;
        if let Some(lease) = self.lease.as_mut() {
            lease.grow(bytes)?;
        }
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.peak_bytes = self.peak_bytes.max(self.used_bytes);
        Ok(())
    }

    pub(crate) fn try_charge_from(
        &mut self,
        source: &mut QueryMemoryLease,
        bytes: usize,
    ) -> Result<()> {
        self.ensure_charge_fits(bytes, "operator state")?;
        let target = self.lease.as_mut().ok_or_else(|| {
            HawDBError::Execution("key admission requires an accounted operator".into())
        })?;
        source.transfer_to(source.bytes(), target, bytes)?;
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
        target.ensure_charge_fits(target_bytes, "operator memory transfer")?;
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
    pool: Arc<SpillPoolAdmission>,
    pub max_bytes: u64,
    pub max_runs: usize,
    used_bytes: Arc<AtomicU64>,
    run_count: Arc<AtomicUsize>,
    staging_account: Option<QueryMemoryAccount>,
}

struct SpillPoolAdmission {
    memory: ExecutionMemoryConfig,
    file_descriptors: Option<hawdb_storage::file_descriptors::FileOpenContext>,
    admitted: Mutex<Option<SpillPool>>,
}

impl SpillPoolAdmission {
    fn get(&self) -> Result<SpillPool> {
        let mut admitted = self
            .admitted
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(pool) = admitted.as_ref() {
            return Ok(pool.clone());
        }
        // Cache only successful admission. A query can retry after another
        // reader releases capacity without retaining a failed resource result.
        let pool = SpillPool::open_with_context(&self.memory, self.file_descriptors.as_ref())?;
        *admitted = Some(pool.clone());
        Ok(pool)
    }
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
        Self::with_file_descriptors(operator, memory, None)
    }

    fn with_file_descriptors(
        operator: &'static str,
        memory: &ExecutionMemoryConfig,
        file_descriptors: Option<hawdb_storage::file_descriptors::FileOpenContext>,
    ) -> Self {
        Self {
            operator,
            pool: Arc::new(SpillPoolAdmission {
                memory: memory.clone(),
                file_descriptors,
                admitted: Mutex::new(None),
            }),
            max_bytes: memory.max_spill_bytes.get(),
            max_runs: memory.max_spill_runs.get(),
            used_bytes: Arc::new(AtomicU64::new(0)),
            run_count: Arc::new(AtomicUsize::new(0)),
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
            ..Self::with_file_descriptors(
                operator,
                memory,
                memory_ledger.file_descriptor_context().cloned(),
            )
        }
    }

    pub(crate) fn staging_budget_bytes(&self) -> usize {
        self.staging_account
            .as_ref()
            .map_or(usize::MAX, |account| account.budget_bytes().get())
    }

    pub(crate) fn for_merge(&self, staging_bytes: NonZeroUsize) -> Result<Self> {
        Ok(Self {
            operator: self.operator,
            pool: self.pool.clone(),
            max_bytes: self.max_bytes,
            max_runs: self.max_runs,
            used_bytes: Arc::clone(&self.used_bytes),
            run_count: Arc::clone(&self.run_count),
            staging_account: self
                .staging_account
                .as_ref()
                .map(|account| account.sub_account(staging_bytes))
                .transpose()?,
        })
    }

    pub(crate) fn reserve_staging(&self, bytes: usize) -> Result<Option<QueryMemoryLease>> {
        self.staging_account
            .as_ref()
            .map(|account| account.reserve(bytes))
            .transpose()
    }

    pub(crate) fn merge_io_account(&self) -> Option<QueryMemoryAccount> {
        self.staging_account.as_ref().map(|account| {
            // Share this allowance across a whole parallel merge wave. A large
            // query root must not multiply full-sized buffers by fan-in/workers.
            let budget = account
                .budget_bytes()
                .get()
                .max(crate::spill::SPILL_IO_BUFFER_BYTES * 2);
            account.sibling(
                QueryMemoryClass::SpillStaging,
                "sort merge IO",
                NonZeroUsize::new(budget).unwrap(),
            )
        })
    }

    pub(crate) fn reserve_merge_descriptors(
        &self,
        count: usize,
    ) -> Result<(
        hawdb_storage::file_descriptors::DescriptorReservation,
        hawdb_storage::file_descriptors::FileOpenContext,
    )> {
        let pool = self.pool.get().map_err(|error| self.pool_error(error))?;
        let descriptors =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::from_open_context(
                pool.file_descriptors(),
            );
        let reservation = descriptors.reserve(count).map_err(|error| {
            hawdb_core::error::file_descriptor_error(&error).map_or_else(
                || HawDBError::Execution(format!("failed to reserve merge descriptors: {error}")),
                HawDBError::FileDescriptors,
            )
        })?;
        Ok((reservation, descriptors.io_context()))
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
        self.create_run_with_context(file_operator, buffer_bytes, None)
    }

    pub(crate) fn create_run_with_context(
        &self,
        file_operator: &str,
        buffer_bytes: NonZeroUsize,
        context: Option<&hawdb_storage::file_descriptors::FileOpenContext>,
    ) -> Result<(SpillRun, SpillWriter)> {
        let mut pool = self.pool.get().map_err(|error| self.pool_error(error))?;
        if let Some(context) = context {
            pool = pool.with_file_descriptors(context.clone());
        }
        let mut current = self.run_count.load(Ordering::Acquire);
        loop {
            if current >= self.max_runs {
                return Err(HawDBError::from(ResourceBudgetError {
                    resource: ResourceBudgetKind::SpillRuns,
                    scope: ResourceBudgetScope::Operator,
                    owner: self.operator.to_string(),
                    memory_class: None,
                    requested: 1,
                    used: current as u64,
                    budget: self.max_runs as u64,
                    budget_option: "ExecutionMemoryConfig.max_spill_runs",
                    message: format!(
                        "{} exceeded max_spill_runs {}",
                        self.operator, self.max_runs
                    ),
                }));
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
        match SpillRun::create_with_buffer_bytes(pool, file_operator, buffer_bytes) {
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
            let budget_error = |message| {
                HawDBError::from(ResourceBudgetError {
                    resource: ResourceBudgetKind::SpillBytes,
                    scope: ResourceBudgetScope::Operator,
                    owner: self.operator.to_string(),
                    memory_class: None,
                    requested: bytes,
                    used: current,
                    budget: self.max_bytes,
                    budget_option: "ExecutionMemoryConfig.max_spill_bytes",
                    message,
                })
            };
            let next = current.checked_add(bytes).ok_or_else(|| {
                budget_error(format!(
                    "{} exceeded max_spill_bytes {} (byte count overflow)",
                    self.operator, self.max_bytes
                ))
            })?;
            if next > self.max_bytes {
                return Err(budget_error(format!(
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
        let pool = match self.pool.get() {
            Ok(pool) => pool,
            Err(error) => {
                self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
                return Err(self.pool_error(error));
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

    fn pool_error(&self, error: HawDBError) -> HawDBError {
        match error {
            HawDBError::FileDescriptors(_) | HawDBError::ResourceBudgetExceeded(_) => error,
            _ => {
                HawDBError::Execution(format!("{} spill pool unavailable: {error}", self.operator))
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
        return Err(tracker.limit_error(
            bytes,
            0,
            tracker.budget_bytes,
            format!(
                "{operator} item uses {bytes} bytes, exceeding blocking_operator_bytes {}",
                tracker.budget_bytes
            ),
        ));
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
        return Err(tracker.state_error(
            bytes,
            format!(
                "{operator} state exceeds blocking_operator_bytes {}",
                tracker.budget_bytes
            ),
        ));
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
    fn soft_target_sampling_bounds_feedback_cost_without_delaying_hard_limits() {
        #[derive(Debug)]
        struct Target {
            polls: AtomicUsize,
            bytes: AtomicU64,
        }
        impl hawdb_core::RuntimeMemoryTarget for Target {
            fn target_bytes(&self, hard: u64) -> u64 {
                self.polls.fetch_add(1, Ordering::Relaxed);
                self.bytes.load(Ordering::Relaxed).min(hard)
            }
        }
        let target = Arc::new(Target {
            polls: AtomicUsize::new(0),
            bytes: AtomicU64::new(u64::MAX),
        });
        let task = hawdb_core::RuntimeTaskContext::default().with_memory_target(target.clone());
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(128 * 1024).unwrap());
        tracker.try_charge(64 * 1024).unwrap();
        assert!(!tracker.should_spill(0, Some(&task)));
        target.bytes.store(0, Ordering::Relaxed);
        for _ in 1..MEMORY_TARGET_SAMPLE_INTERVAL {
            assert!(!tracker.should_spill(0, Some(&task)));
            // Hard refusal must not depend on the cached target or poll cadence.
            assert!(tracker.should_spill(128 * 1024, Some(&task)));
        }
        assert_eq!(target.polls.load(Ordering::Relaxed), 1);
        assert!(tracker.should_spill(0, Some(&task)));
        assert_eq!(target.polls.load(Ordering::Relaxed), 2);
        tracker.reset();
        assert!(!tracker.should_spill(0, Some(&task)));
        tracker.try_charge(1024).unwrap();
        assert!(!tracker.should_spill(1024, Some(&task)));
    }

    fn refusal(error: HawDBError) -> ResourceBudgetError {
        let HawDBError::ResourceBudgetExceeded(cause) = error else {
            panic!("expected typed memory refusal, got {error}");
        };
        *cause
    }

    #[test]
    fn operator_limit_preserves_constructor_origin_and_exact_boundary() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(32).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "custom operator",
            NonZeroUsize::new(16).unwrap(),
        );
        let mut tracker =
            OperatorMemoryTracker::with_account(NonZeroUsize::new(8).unwrap(), account);
        tracker.try_charge(8).unwrap();
        let cause = refusal(tracker.try_charge(1).unwrap_err());
        assert_eq!(cause.resource, ResourceBudgetKind::MemoryBytes);
        assert_eq!(cause.scope, ResourceBudgetScope::Operator);
        assert_eq!(cause.owner, "custom operator");
        assert_eq!(cause.memory_class, Some("blocking_state"));
        assert_eq!((cause.requested, cause.used, cause.budget), (1, 8, 8));
        assert_eq!(
            cause.budget_option,
            "OperatorMemoryTracker::with_account(budget_bytes)"
        );
        assert_eq!((tracker.used_bytes, tracker.peak_bytes), (8, 8));
        assert_eq!(ledger.snapshot().used_bytes, 8);
        tracker.release(3);
        tracker.reset();
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!((tracker.used_bytes, tracker.peak_bytes), (0, 8));
    }

    #[test]
    fn operator_limit_refuses_overflow_without_changing_usage() {
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(usize::MAX).unwrap());
        tracker.try_charge(usize::MAX).unwrap();
        assert!(tracker.would_exceed(1));
        let cause = refusal(tracker.try_charge(1).unwrap_err());
        assert_eq!(
            cause.budget_option,
            "OperatorMemoryTracker::new(budget_bytes)"
        );
        assert_eq!(cause.memory_class, None);
        assert_eq!(cause.requested, 1);
        assert!(cause.message.contains("overflow"));
        assert_eq!(cause.used, usize::MAX as u64);
        assert_eq!(cause.budget, usize::MAX as u64);
        assert_eq!(tracker.used_bytes, usize::MAX);
        tracker.reset();
        assert_eq!(tracker.used_bytes, 0);
    }

    #[test]
    fn operator_limit_rejects_transfers_without_releasing_source() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(32).unwrap());
        let source_account = ledger.account(
            QueryMemoryClass::ExternalRead,
            "source",
            NonZeroUsize::new(16).unwrap(),
        );
        let target_account = ledger.account(
            QueryMemoryClass::BlockingState,
            "target",
            NonZeroUsize::new(16).unwrap(),
        );
        let mut source = OperatorMemoryTracker::with_account(
            NonZeroUsize::new(8).unwrap(),
            source_account.clone(),
        );
        let mut target = OperatorMemoryTracker::with_blocking_account(
            NonZeroUsize::new(4).unwrap(),
            target_account,
        );
        source.try_charge(6).unwrap();
        target.try_charge(2).unwrap();
        let cause = refusal(source.transfer_to(4, &mut target, 3).unwrap_err());
        assert_eq!(cause.owner, "target");
        assert_eq!(cause.memory_class, Some("blocking_state"));
        assert_eq!((cause.requested, cause.used, cause.budget), (3, 2, 4));
        assert_eq!(
            cause.budget_option,
            "ExecutionMemoryConfig.blocking_operator_bytes"
        );
        assert_eq!((source.used_bytes, target.used_bytes), (6, 2));
        assert_eq!(ledger.snapshot().used_bytes, 8);
        let mut key = source_account.reserve(4).unwrap();
        let cause = refusal(target.try_charge_from(&mut key, 3).unwrap_err());
        assert_eq!((cause.requested, cause.used, cause.budget), (3, 2, 4));
        assert_eq!(key.bytes(), 4);
        assert_eq!(target.used_bytes, 2);
        assert_eq!(ledger.snapshot().used_bytes, 12);
        source.transfer_to(4, &mut target, 2).unwrap();
        assert_eq!((source.used_bytes, target.used_bytes), (2, 4));
        assert_eq!(ledger.snapshot().used_bytes, 10);
        drop((source, target, key));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn operator_tracker_rejects_state_beyond_budget() {
        let mut tracker = OperatorMemoryTracker::new(NonZeroUsize::new(1).unwrap());
        let error = push_bounded_operator_binding(
            "test",
            &mut Vec::new(),
            Binding {
                values: BTreeMap::new(),
                nodes: BTreeMap::new().into(),
                relationships: BTreeMap::new().into(),
            },
            &mut tracker,
        )
        .unwrap_err();

        assert!(error.to_string().contains("blocking_operator_bytes"));
    }
}
