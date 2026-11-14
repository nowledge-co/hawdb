// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use hawdb_core::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Completed observations of one phase, including failed operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConcurrentPhaseMetrics {
    pub count: u64,
    pub total_nanos: u64,
}

/// Cumulative, best-effort observations shared by cloned concurrent handles.
/// Fields are sampled independently; phases can overlap and must not be summed
/// into transaction latency. Reading this report does not acquire the sequencer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConcurrentDatabaseMetrics {
    /// Recording is disabled until explicitly enabled on the shared handle.
    pub enabled: bool,
    /// Attempts to acquire sequencer guards, including transaction creation,
    /// inspection and failed acquisitions as well as writes.
    pub sequencer_wait: ConcurrentPhaseMetrics,
    /// Guard lifetime through read publication, excluding retired-view destruction.
    pub sequencer_hold: ConcurrentPhaseMetrics,
    /// Explicit transaction/append commit tasks, including per-write durability
    /// when group commit is disabled. Direct autocommit statements are excluded.
    pub commit_execution: ConcurrentPhaseMetrics,
    /// Shared group durability barriers; disabled-group commits are counted above.
    pub group_durability: ConcurrentPhaseMetrics,
    /// Attempts to capture a complete published read view.
    pub read_publication: ConcurrentPhaseMetrics,
    /// Initial explicit-transaction graph executions, including reads and errors.
    pub graph_statement_executions: u64,
    /// Additional executions after acquiring new pessimistic graph locks.
    pub graph_statement_replays: u64,
    /// Mutations retained because all requested locks were already held.
    pub graph_statement_reuses: u64,
}

#[derive(Debug, Default)]
pub(super) struct Metrics {
    enabled: AtomicBool,
    pub(super) sequencer_wait: Phase,
    pub(super) sequencer_hold: Phase,
    pub(super) commit_execution: Phase,
    pub(super) group_durability: Phase,
    pub(super) read_publication: Phase,
    graph_statement_executions: AtomicU64,
    graph_statement_replays: AtomicU64,
    graph_statement_reuses: AtomicU64,
}

impl Metrics {
    pub(super) fn enable(&self) {
        for phase in [
            &self.sequencer_wait,
            &self.sequencer_hold,
            &self.commit_execution,
            &self.group_durability,
            &self.read_publication,
        ] {
            phase.enabled.store(true, Ordering::Relaxed);
        }
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> ConcurrentDatabaseMetrics {
        ConcurrentDatabaseMetrics {
            enabled: self.enabled.load(Ordering::Relaxed),
            sequencer_wait: self.sequencer_wait.snapshot(),
            sequencer_hold: self.sequencer_hold.snapshot(),
            commit_execution: self.commit_execution.snapshot(),
            group_durability: self.group_durability.snapshot(),
            read_publication: self.read_publication.snapshot(),
            graph_statement_executions: self.graph_statement_executions.load(Ordering::Relaxed),
            graph_statement_replays: self.graph_statement_replays.load(Ordering::Relaxed),
            graph_statement_reuses: self.graph_statement_reuses.load(Ordering::Relaxed),
        }
    }

    pub(super) fn graph_execution(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            add(&self.graph_statement_executions, 1);
        }
    }

    pub(super) fn graph_replay(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            add(&self.graph_statement_replays, 1);
        }
    }

    pub(super) fn graph_reuse(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            add(&self.graph_statement_reuses, 1);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Phase {
    enabled: AtomicBool,
    count: AtomicU64,
    total_nanos: AtomicU64,
}

impl Phase {
    fn record(&self, elapsed: Duration) {
        add(
            &self.total_nanos,
            elapsed.as_nanos().try_into().unwrap_or(u64::MAX),
        );
        add(&self.count, 1);
    }

    pub(super) fn start(&self) -> PhaseTimer<'_> {
        PhaseTimer {
            phase: self,
            started: self.enabled.load(Ordering::Relaxed).then(Instant::now),
        }
    }

    fn snapshot(&self) -> ConcurrentPhaseMetrics {
        ConcurrentPhaseMetrics {
            count: self.count.load(Ordering::Relaxed),
            total_nanos: self.total_nanos.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct PhaseTimer<'a> {
    phase: &'a Phase,
    started: Option<Instant>,
}

impl Drop for PhaseTimer<'_> {
    fn drop(&mut self) {
        if let Some(started) = &self.started {
            self.phase.record(started.elapsed());
        }
    }
}

fn add(counter: &AtomicU64, amount: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn metrics_are_available_while_the_sequencer_is_held_and_shared_by_clones() {
        let db = Database::new().into_concurrent();
        let cloned = db.clone();
        let guard = db.inner.commits.lock().unwrap();
        assert!(!cloned.concurrency_metrics().enabled);
        assert_eq!(cloned.concurrency_metrics().sequencer_wait.count, 0);
        drop(guard);
        assert_eq!(cloned.concurrency_metrics().sequencer_hold.count, 0);
        db.enable_concurrency_metrics();
        assert!(cloned.concurrency_metrics().enabled);
        let guard = db.inner.commits.lock().unwrap();
        let held = cloned.concurrency_metrics();
        assert_eq!(held.sequencer_wait.count, 1);
        assert_eq!(held.sequencer_hold.count, 0);
        drop(guard);
        assert_eq!(cloned.concurrency_metrics().sequencer_hold.count, 1);
        let mut tx = db
            .begin_transaction(ConcurrentTransactionOptions::optimistic())
            .unwrap();
        tx.query("CREATE (:Metrics {id: 1})").unwrap();
        tx.commit().unwrap();
        let completed = cloned.concurrency_metrics();
        assert_eq!(completed.commit_execution.count, 1);
        assert_eq!(completed.group_durability.count, 0);
        assert_eq!(completed.read_publication.count, 1);
        assert_eq!(
            completed.sequencer_wait.count,
            completed.sequencer_hold.count
        );
    }
}
