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

//! Query-owned hierarchical memory accounting.

use hawdb_core::{HawDBError, Result};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QueryMemoryClass {
    PipelineBatch,
    BlockingState,
    ExternalRead,
    SpillStaging,
    MorselOutput,
    ResultMaterialization,
}

impl QueryMemoryClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PipelineBatch => "pipeline_batch",
            Self::BlockingState => "blocking_state",
            Self::ExternalRead => "external_read",
            Self::SpillStaging => "spill_staging",
            Self::MorselOutput => "morsel_output",
            Self::ResultMaterialization => "result_materialization",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryClassSnapshot {
    pub class: QueryMemoryClass,
    pub used_bytes: usize,
    pub peak_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMemoryLedgerSnapshot {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub peak_bytes: usize,
    pub account_count: usize,
    pub classes: Vec<QueryMemoryClassSnapshot>,
}

#[derive(Debug, Clone)]
pub struct QueryMemoryLedger {
    inner: Arc<QueryMemoryLedgerInner>,
}

#[derive(Debug)]
struct QueryMemoryLedgerInner {
    budget_bytes: usize,
    state: Mutex<QueryMemoryLedgerState>,
}

#[derive(Debug, Default)]
struct QueryMemoryLedgerState {
    used_bytes: usize,
    peak_bytes: usize,
    next_account_id: u64,
    accounts: BTreeMap<u64, QueryMemoryAccountState>,
    classes: BTreeMap<QueryMemoryClass, QueryMemoryClassState>,
}

#[derive(Debug)]
struct QueryMemoryAccountState {
    class: QueryMemoryClass,
    owner: Arc<str>,
    budget_bytes: usize,
    used_bytes: usize,
    peak_bytes: usize,
}

#[derive(Debug, Default)]
struct QueryMemoryClassState {
    used_bytes: usize,
    peak_bytes: usize,
}

impl QueryMemoryLedger {
    pub fn new(budget_bytes: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(QueryMemoryLedgerInner {
                budget_bytes: budget_bytes.get(),
                state: Mutex::new(QueryMemoryLedgerState::default()),
            }),
        }
    }

    pub fn account(
        &self,
        class: QueryMemoryClass,
        owner: impl Into<Arc<str>>,
        budget_bytes: NonZeroUsize,
    ) -> QueryMemoryAccount {
        // Caller code must run before locking: conversion can panic or re-enter the ledger.
        let owner = owner.into();
        let mut state = lock_recover(&self.inner.state);
        let account_id = state.next_account_id;
        state.next_account_id = state.next_account_id.saturating_add(1);
        state.accounts.insert(
            account_id,
            QueryMemoryAccountState {
                class,
                owner,
                budget_bytes: budget_bytes.get(),
                used_bytes: 0,
                peak_bytes: 0,
            },
        );
        QueryMemoryAccount {
            ledger: self.clone(),
            account_id,
        }
    }

    pub fn snapshot(&self) -> QueryMemoryLedgerSnapshot {
        let state = lock_recover(&self.inner.state);
        QueryMemoryLedgerSnapshot {
            budget_bytes: self.inner.budget_bytes,
            used_bytes: state.used_bytes,
            peak_bytes: state.peak_bytes,
            account_count: state.accounts.len(),
            classes: state
                .classes
                .iter()
                .map(|(class, state)| QueryMemoryClassSnapshot {
                    class: *class,
                    used_bytes: state.used_bytes,
                    peak_bytes: state.peak_bytes,
                })
                .collect(),
        }
    }

    fn reserve(&self, account_id: u64, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut state = lock_recover(&self.inner.state);
        let (class, owner, account_budget, account_next) = {
            let account = state.accounts.get(&account_id).ok_or_else(|| {
                HawDBError::Execution("query memory account is no longer registered".to_string())
            })?;
            let account_next = account.used_bytes.checked_add(bytes).ok_or_else(|| {
                HawDBError::Execution(format!(
                    "query memory account {} ({}) byte accounting overflow",
                    account.owner,
                    account.class.as_str()
                ))
            })?;
            (
                account.class,
                Arc::clone(&account.owner),
                account.budget_bytes,
                account_next,
            )
        };
        if account_next > account_budget {
            return Err(HawDBError::Execution(format!(
                "query memory account {owner} ({}) would use {account_next} bytes, exceeding its {account_budget}-byte budget",
                class.as_str()
            )));
        }
        let root_next = state.used_bytes.checked_add(bytes).ok_or_else(|| {
            HawDBError::Execution(format!(
                "query memory ledger byte accounting overflow while charging {owner} ({})",
                class.as_str()
            ))
        })?;
        if root_next > self.inner.budget_bytes {
            return Err(HawDBError::Execution(format!(
                "query memory ledger would use {root_next} bytes while charging {owner} ({}), exceeding query_memory_bytes {}",
                class.as_str(),
                self.inner.budget_bytes
            )));
        }

        // Complete allocation before changing any of the hierarchical counters.
        state.classes.entry(class).or_default();
        let account = state
            .accounts
            .get_mut(&account_id)
            .expect("validated query memory account remains registered");
        account.used_bytes = account_next;
        account.peak_bytes = account.peak_bytes.max(account_next);
        state.used_bytes = root_next;
        state.peak_bytes = state.peak_bytes.max(root_next);
        let class_state = state
            .classes
            .get_mut(&class)
            .expect("prepared query memory class remains registered");
        class_state.used_bytes = class_state.used_bytes.saturating_add(bytes);
        class_state.peak_bytes = class_state.peak_bytes.max(class_state.used_bytes);
        Ok(())
    }

    fn can_reserve(&self, account_id: u64, bytes: usize) -> bool {
        let state = lock_recover(&self.inner.state);
        let Some(account) = state.accounts.get(&account_id) else {
            return false;
        };
        let Some(account_next) = account.used_bytes.checked_add(bytes) else {
            return false;
        };
        let Some(root_next) = state.used_bytes.checked_add(bytes) else {
            return false;
        };
        account_next <= account.budget_bytes && root_next <= self.inner.budget_bytes
    }

    fn release(&self, account_id: u64, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = lock_recover(&self.inner.state);
        let Some(account) = state.accounts.get_mut(&account_id) else {
            return;
        };
        let released = bytes.min(account.used_bytes);
        let class = account.class;
        account.used_bytes -= released;
        state.used_bytes = state.used_bytes.saturating_sub(released);
        if let Some(class_state) = state.classes.get_mut(&class) {
            class_state.used_bytes = class_state.used_bytes.saturating_sub(released);
        }
    }

    fn transfer(
        &self,
        source_account_id: u64,
        source_bytes: usize,
        target_account_id: u64,
        target_bytes: usize,
    ) -> Result<()> {
        if source_account_id == target_account_id {
            return Err(HawDBError::Execution(
                "query memory transfer requires distinct accounts".to_string(),
            ));
        }
        let mut state = lock_recover(&self.inner.state);
        let (source_class, source_used) = state
            .accounts
            .get(&source_account_id)
            .map(|account| (account.class, account.used_bytes))
            .ok_or_else(|| {
                HawDBError::Execution(
                    "source query memory account is no longer registered".to_string(),
                )
            })?;
        if source_bytes > source_used {
            return Err(HawDBError::Execution(format!(
                "query memory transfer tried to release {source_bytes} bytes from a source account using {source_used} bytes"
            )));
        }
        let (target_class, target_owner, target_budget, target_used) = state
            .accounts
            .get(&target_account_id)
            .map(|account| {
                (
                    account.class,
                    Arc::clone(&account.owner),
                    account.budget_bytes,
                    account.used_bytes,
                )
            })
            .ok_or_else(|| {
                HawDBError::Execution(
                    "target query memory account is no longer registered".to_string(),
                )
            })?;
        let target_next = target_used.checked_add(target_bytes).ok_or_else(|| {
            HawDBError::Execution(format!(
                "query memory account {target_owner} ({}) byte accounting overflow",
                target_class.as_str()
            ))
        })?;
        if target_next > target_budget {
            return Err(HawDBError::Execution(format!(
                "query memory account {target_owner} ({}) would use {target_next} bytes, exceeding its {target_budget}-byte budget",
                target_class.as_str()
            )));
        }
        let root_after_release = state.used_bytes.checked_sub(source_bytes).ok_or_else(|| {
            HawDBError::Execution(
                "query memory ledger underflow during ownership transfer".to_string(),
            )
        })?;
        let root_next = root_after_release.checked_add(target_bytes).ok_or_else(|| {
            HawDBError::Execution(format!(
                "query memory ledger byte accounting overflow while transferring ownership to {target_owner} ({})",
                target_class.as_str()
            ))
        })?;
        if root_next > self.inner.budget_bytes {
            return Err(HawDBError::Execution(format!(
                "query memory ledger would use {root_next} bytes while transferring ownership to {target_owner} ({}), exceeding query_memory_bytes {}",
                target_class.as_str(),
                self.inner.budget_bytes
            )));
        }

        state.classes.entry(target_class).or_default();
        let source = state
            .accounts
            .get_mut(&source_account_id)
            .expect("validated source query memory account remains registered");
        source.used_bytes -= source_bytes;
        let target = state
            .accounts
            .get_mut(&target_account_id)
            .expect("validated target query memory account remains registered");
        target.used_bytes = target_next;
        target.peak_bytes = target.peak_bytes.max(target_next);
        state.used_bytes = root_next;
        state.peak_bytes = state.peak_bytes.max(root_next);
        if let Some(class_state) = state.classes.get_mut(&source_class) {
            class_state.used_bytes = class_state.used_bytes.saturating_sub(source_bytes);
        }
        let class_state = state
            .classes
            .get_mut(&target_class)
            .expect("prepared target query memory class remains registered");
        class_state.used_bytes = class_state.used_bytes.saturating_add(target_bytes);
        class_state.peak_bytes = class_state.peak_bytes.max(class_state.used_bytes);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct QueryMemoryAccount {
    ledger: QueryMemoryLedger,
    account_id: u64,
}

impl QueryMemoryAccount {
    pub(crate) fn peak_bytes(&self) -> usize {
        lock_recover(&self.ledger.inner.state).accounts[&self.account_id].peak_bytes
    }

    pub(crate) fn sibling(
        &self,
        class: QueryMemoryClass,
        owner: impl Into<Arc<str>>,
        budget_bytes: NonZeroUsize,
    ) -> Self {
        self.ledger.account(class, owner, budget_bytes)
    }

    pub(crate) fn can_reserve(&self, bytes: usize) -> bool {
        self.ledger.can_reserve(self.account_id, bytes)
    }

    pub fn reserve(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.ledger.reserve(self.account_id, bytes)?;
        Ok(QueryMemoryLease {
            account: self.clone(),
            bytes,
        })
    }
}

#[derive(Debug)]
pub struct QueryMemoryLease {
    account: QueryMemoryAccount,
    bytes: usize,
}

impl QueryMemoryLease {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn grow(&mut self, bytes: usize) -> Result<()> {
        self.account
            .ledger
            .reserve(self.account.account_id, bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    pub fn shrink(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        self.account
            .ledger
            .release(self.account.account_id, released);
        self.bytes -= released;
    }

    pub fn reset(&mut self) {
        self.shrink(self.bytes);
    }

    pub(crate) fn transfer_to(
        &mut self,
        source_bytes: usize,
        target: &mut Self,
        target_bytes: usize,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.account.ledger.inner, &target.account.ledger.inner) {
            return Err(HawDBError::Execution(
                "query memory transfer requires accounts from the same ledger".to_string(),
            ));
        }
        if source_bytes > self.bytes {
            return Err(HawDBError::Execution(format!(
                "query memory transfer tried to release {source_bytes} bytes from a {}-byte lease",
                self.bytes,
            )));
        }
        self.account.ledger.transfer(
            self.account.account_id,
            source_bytes,
            target.account.account_id,
            target_bytes,
        )?;
        self.bytes -= source_bytes;
        target.bytes = target.bytes.saturating_add(target_bytes);
        Ok(())
    }
}

impl Drop for QueryMemoryLease {
    fn drop(&mut self) {
        self.account
            .ledger
            .release(self.account.account_id, self.bytes);
        self.bytes = 0;
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // State never escapes the ledger. Checks and allocation precede counter updates,
    // and no caller code runs under this lock. Recovering poisoning preserves that
    // valid state; it is not a repair mechanism for arbitrary accounting corruption.
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
#[path = "memory_ledger_hardening_tests.rs"]
mod hardening_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sibling_accounts_share_one_root_budget() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(10).unwrap());
        let left = ledger.account(
            QueryMemoryClass::BlockingState,
            "left",
            NonZeroUsize::new(10).unwrap(),
        );
        let right = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "right",
            NonZeroUsize::new(10).unwrap(),
        );
        let left_lease = left.reserve(6).unwrap();
        let error = right.reserve(5).unwrap_err();

        assert!(error.to_string().contains("query_memory_bytes 10"));
        assert_eq!(ledger.snapshot().used_bytes, 6);
        drop(left_lease);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn capacity_preflight_checks_account_and_root_without_charging() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(10).unwrap());
        let left = ledger.account(
            QueryMemoryClass::BlockingState,
            "left",
            NonZeroUsize::new(8).unwrap(),
        );
        let right = ledger.account(
            QueryMemoryClass::PipelineBatch,
            "right",
            NonZeroUsize::new(10).unwrap(),
        );

        assert!(left.can_reserve(8));
        assert_eq!(ledger.snapshot().used_bytes, 0);
        let left_lease = left.reserve(6).unwrap();
        assert!(!left.can_reserve(3));
        assert!(right.can_reserve(4));
        assert!(!right.can_reserve(5));
        assert_eq!(ledger.snapshot().used_bytes, 6);

        drop(left_lease);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn derived_sibling_account_keeps_the_original_query_root() {
        let budget = NonZeroUsize::new(8).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let state = ledger.account(QueryMemoryClass::BlockingState, "merge", budget);
        let output = state.sibling(QueryMemoryClass::PipelineBatch, "output", budget);
        let state_lease = state.reserve(6).unwrap();
        assert!(output
            .reserve(3)
            .unwrap_err()
            .to_string()
            .contains("query_memory_bytes 8"));
        let output_lease = output.reserve(2).unwrap();
        assert_eq!(ledger.snapshot().used_bytes, 8);
        drop((state_lease, output_lease));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn ownership_transfer_does_not_require_double_root_capacity() {
        let budget = NonZeroUsize::new(8).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let source = ledger.account(QueryMemoryClass::BlockingState, "source", budget);
        let target = ledger.account(QueryMemoryClass::PipelineBatch, "target", budget);
        let mut source_lease = source.reserve(8).unwrap();
        let mut target_lease = target.reserve(0).unwrap();

        source_lease.transfer_to(8, &mut target_lease, 8).unwrap();

        assert_eq!(source_lease.bytes(), 0);
        assert_eq!(target_lease.bytes(), 8);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.used_bytes, 8);
        assert_eq!(snapshot.peak_bytes, 8);
        drop((source_lease, target_lease));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn failed_ownership_transfer_preserves_source_charge() {
        let budget = NonZeroUsize::new(8).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let source = ledger.account(QueryMemoryClass::BlockingState, "source", budget);
        let target = ledger.account(QueryMemoryClass::PipelineBatch, "target", budget);
        let mut source_lease = source.reserve(8).unwrap();
        let mut target_lease = target.reserve(0).unwrap();

        let error = source_lease
            .transfer_to(8, &mut target_lease, 9)
            .unwrap_err();

        assert!(error.to_string().contains("9 bytes"));
        assert_eq!(source_lease.bytes(), 8);
        assert_eq!(target_lease.bytes(), 0);
        assert_eq!(ledger.snapshot().used_bytes, 8);
    }

    #[test]
    fn account_budget_is_enforced_before_root_budget() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::BlockingState,
            "sort",
            NonZeroUsize::new(4).unwrap(),
        );

        let error = account.reserve(5).unwrap_err();

        assert!(error.to_string().contains("sort"));
        assert!(error.to_string().contains("4-byte budget"));
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }

    #[test]
    fn lease_growth_shrink_and_drop_update_hierarchy() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::MorselOutput,
            "reorder",
            NonZeroUsize::new(80).unwrap(),
        );
        let mut lease = account.reserve(20).unwrap();
        lease.grow(30).unwrap();
        lease.shrink(10);
        assert_eq!(lease.bytes(), 40);
        assert_eq!(ledger.snapshot().used_bytes, 40);

        drop(lease);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.used_bytes, 0);
        assert_eq!(snapshot.peak_bytes, 50);
        assert_eq!(snapshot.classes[0].peak_bytes, 50);
    }

    #[test]
    fn unwind_releases_query_memory_lease() {
        let ledger = QueryMemoryLedger::new(NonZeroUsize::new(100).unwrap());
        let account = ledger.account(
            QueryMemoryClass::SpillStaging,
            "spill",
            NonZeroUsize::new(100).unwrap(),
        );
        let result = std::panic::catch_unwind(|| {
            let _lease = account.reserve(80).unwrap();
            panic!("injected panic");
        });

        assert!(result.is_err());
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
