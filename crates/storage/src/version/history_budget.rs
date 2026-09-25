// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct BudgetState {
    used: usize,
    limit: usize,
}

#[derive(Debug, Clone)]
pub(super) struct HistoryBudget(Arc<Mutex<BudgetState>>);

#[derive(Debug)]
pub(super) struct HistoryLease {
    budget: HistoryBudget,
    bytes: usize,
}

impl HistoryBudget {
    pub(super) fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(BudgetState { used: 0, limit })))
    }

    pub(super) fn reserve(&self, bytes: usize) -> Option<Arc<HistoryLease>> {
        let mut state = self.0.lock().expect("version history budget lock poisoned");
        let next = state.used.checked_add(bytes)?;
        if next > state.limit {
            return None;
        }
        state.used = next;
        Some(Arc::new(HistoryLease {
            budget: self.clone(),
            bytes,
        }))
    }

    #[cfg(test)]
    pub(super) fn used(&self) -> usize {
        self.0
            .lock()
            .expect("version history budget lock poisoned")
            .used
    }
}

impl HistoryLease {
    pub(super) fn shrink(&mut self, bytes: usize) {
        let released = self
            .bytes
            .checked_sub(bytes)
            .expect("history lease can only shrink");
        let mut state = self
            .budget
            .0
            .lock()
            .expect("version history budget lock poisoned");
        state.used = state
            .used
            .checked_sub(released)
            .expect("history budget accounting");
        self.bytes = bytes;
    }
}

impl Drop for HistoryLease {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .0
            .lock()
            .expect("version history budget lock poisoned");
        state.used = state
            .used
            .checked_sub(self.bytes)
            .expect("history budget accounting");
    }
}
