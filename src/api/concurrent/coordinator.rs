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

use super::super::transaction_locks::{LockRequest, LockTable, WaitForGraph};
use super::{
    Database, QueryOutput, WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitSnapshot,
    WalGroupCommitWaitDecision, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
};
use crate::error::{HawDBError, Result};
use hawdb_core::time::{Duration, Instant};
use std::collections::VecDeque;
use std::fmt::{self, Debug, Formatter};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

const ADAPTIVE_FSYNC_BUCKET_COUNT: usize = 100;
const ADAPTIVE_FSYNC_BUCKET_DURATION: Duration = Duration::from_millis(100);
const ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES: u64 = 8;
// Keep the collection window proportional to device latency without letting it
// dominate the durability operation it is intended to amortize.
const ADAPTIVE_FSYNC_FRACTION_PER_MILLION: u64 = 75_000;
// Delays below this policy floor add scheduler jitter without useful batching.
const MIN_USEFUL_COALESCING_DELAY: Duration = Duration::from_micros(50);
// A follower is woken by the leader, so this interval is not a latency budget.
// It only decides how soon a follower notices that nobody is going to wake it,
// which is a broken invariant rather than a slow commit.
const GROUP_COMMIT_LIVENESS_CHECK_INTERVAL: Duration = Duration::from_millis(250);

pub(super) struct CommitSequencer {
    database: Mutex<Database>,
    group_commit: GroupCommitCoordinator,
}

impl CommitSequencer {
    pub(super) fn new(database: Database, group_commit: WalGroupCommitConfig) -> Self {
        Self {
            database: Mutex::new(database),
            group_commit: GroupCommitCoordinator::new(group_commit),
        }
    }

    pub(super) fn lock(&self) -> Result<MutexGuard<'_, Database>> {
        self.database.lock().map_err(|_| {
            HawDBError::Execution("concurrent commit sequencer is poisoned".to_string())
        })
    }

    pub(super) fn execute_grouped(
        &self,
        task: impl FnOnce(&mut Database) -> Result<QueryOutput> + Send + 'static,
    ) -> Result<QueryOutput> {
        if !self.group_commit.config.is_enabled() {
            let mut database = self.lock()?;
            return task(&mut database);
        }
        let request = Arc::new(QueuedCommit::new(Box::new(task)));
        {
            let mut state = self.group_commit.lock_state()?;
            state.metrics.submitted_commits = state.metrics.submitted_commits.saturating_add(1);
            state.queue.push_back(Arc::clone(&request));
            self.group_commit.available.notify_all();
        }
        #[cfg(test)]
        self.group_commit.wait_after_enqueue()?;
        loop {
            if let Some(result) = request.take_result()? {
                return result;
            }
            let mut state = self.group_commit.lock_state()?;
            if let Some(result) = request.take_result()? {
                return result;
            }
            let can_lead = !state.leader_active
                && state
                    .queue
                    .front()
                    .is_some_and(|front| Arc::ptr_eq(front, &request));
            if can_lead {
                state.leader_active = true;
                drop(state);
                self.run_group_commit()?;
                continue;
            }
            let waited = self
                .group_commit
                .available
                .wait_timeout(state, GROUP_COMMIT_LIVENESS_CHECK_INTERVAL)
                .map_err(|_| group_commit_coordinator_poisoned_error())?;
            state = waited.0;
            if waited.1.timed_out() {
                state.assert_commit_is_accounted_for(&request)?;
            }
            drop(state);
        }
    }

    pub(super) fn group_commit_snapshot(&self) -> Result<WalGroupCommitSnapshot> {
        let mut state = self.group_commit.lock_state()?;
        state.refresh_fsync_estimate(Instant::now());
        Ok(state.metrics)
    }

    #[cfg(test)]
    pub(super) fn set_group_commit_post_enqueue_barrier(
        &self,
        barrier: Arc<Barrier>,
    ) -> Result<()> {
        self.group_commit.set_post_enqueue_barrier(barrier)
    }

    fn run_group_commit(&self) -> Result<()> {
        let _leader = GroupCommitLeaderGuard::new(&self.group_commit);
        if let Err(error) = self.wait_for_group_commit_peers() {
            self.fail_front_group(error.to_string())?;
            return Ok(());
        }

        let mut completed = Vec::new();
        let execution = catch_unwind(AssertUnwindSafe(|| {
            self.execute_group_commit_tasks(&mut completed)
        }));
        let flush = match execution {
            Ok(Ok(flush)) => flush,
            Ok(Err(error)) => {
                if completed.is_empty() {
                    self.fail_front_group(error.to_string())?;
                } else {
                    fail_completed_commits(
                        &mut completed,
                        format!("WAL group commit stopped before its durability barrier: {error}"),
                    );
                    self.record_failed_group();
                    complete_commit_requests(completed)?;
                    self.group_commit.available.notify_all();
                }
                return Ok(());
            }
            Err(_) => {
                fail_completed_commits(
                    &mut completed,
                    "WAL group commit task panicked; the commit sequencer is poisoned and the database must be closed and reopened".to_string(),
                );
                self.record_failed_group();
                complete_commit_requests(completed)?;
                self.group_commit.available.notify_all();
                return Ok(());
            }
        };

        let completed_commits = completed
            .iter()
            .filter(|(_, result)| result.is_ok())
            .count() as u64;
        {
            let mut state = self.group_commit.lock_state_recover();
            if flush.fsync_performed {
                state
                    .fsync_window
                    .record(Instant::now(), flush.fsync_micros);
            }
            state.metrics.completed_commits = state
                .metrics
                .completed_commits
                .saturating_add(completed_commits);
            state.metrics.group_count = state.metrics.group_count.saturating_add(1);
            state.metrics.shared_sync_count = state
                .metrics
                .shared_sync_count
                .saturating_add(u64::from(flush.fsync_performed));
            state.metrics.grouped_wal_entries = state
                .metrics
                .grouped_wal_entries
                .saturating_add(flush.entry_count as u64);
            state.metrics.grouped_wal_bytes = state
                .metrics
                .grouped_wal_bytes
                .saturating_add(flush.byte_count);
            state.metrics.max_observed_group_entries = state
                .metrics
                .max_observed_group_entries
                .max(flush.entry_count);
            state.metrics.max_observed_group_bytes =
                state.metrics.max_observed_group_bytes.max(flush.byte_count);
            state.metrics.total_fsync_micros = state
                .metrics
                .total_fsync_micros
                .saturating_add(flush.fsync_micros);
            state.refresh_fsync_estimate(Instant::now());
        }
        complete_commit_requests(completed)?;
        self.group_commit.available.notify_all();
        Ok(())
    }

    fn record_failed_group(&self) {
        let mut state = self.group_commit.lock_state_recover();
        state.metrics.group_count = state.metrics.group_count.saturating_add(1);
    }

    fn execute_group_commit_tasks(
        &self,
        completed: &mut Vec<(Arc<QueuedCommit>, Result<QueryOutput>)>,
    ) -> Result<crate::store::WalSyncGroupFlush> {
        let mut database = self.lock()?;
        database.begin_wal_sync_group()?;
        while completed.len() < self.group_commit.config.max_entries().get() {
            let request = {
                let mut state = self.group_commit.lock_state()?;
                state.queue.pop_front()
            };
            let Some(request) = request else {
                break;
            };
            let task = match request.take_task() {
                Ok(task) => task,
                Err(error) => {
                    completed.push((request, Err(error)));
                    continue;
                }
            };
            completed.push((
                request,
                Err(HawDBError::Execution(
                    "WAL group commit task panicked before recording a result".to_string(),
                )),
            ));
            let result_index = completed.len() - 1;
            let result = task(&mut database);
            completed[result_index].1 = result;
            if database.relational_row_schema_checkpoint_required() {
                break;
            }
            let progress = database.wal_sync_group_progress();
            if progress.byte_count >= self.group_commit.config.max_bytes().get() {
                break;
            }
        }

        let flush = database.finish_wal_sync_group();
        Ok(match flush {
            Ok(flush) => {
                if let Err(error) =
                    database.complete_required_relational_row_checkpoint("group commit durability")
                {
                    let message = error.to_string();
                    for (_, result) in completed.iter_mut() {
                        if result.is_ok() {
                            *result = Err(HawDBError::StorageIntegrity(message.clone()));
                        }
                    }
                }
                flush
            }
            Err(error) => {
                let message = format!(
                    "WAL group durability barrier failed after mutation publication; close and reopen the database: {error}"
                );
                for (_, result) in completed.iter_mut() {
                    if result.is_ok() {
                        *result = Err(HawDBError::StorageIntegrity(message.clone()));
                    }
                }
                Default::default()
            }
        })
    }

    fn wait_for_group_commit_peers(&self) -> Result<()> {
        if self.group_commit.config.max_entries().get() == 1 {
            self.group_commit
                .lock_state()?
                .record_wait_decision(WalGroupCommitWaitDecision::MaxEntriesBound, Duration::ZERO);
            return Ok(());
        }
        std::thread::yield_now();
        let mut state = self.group_commit.lock_state()?;
        if state.queue.len() < 2 {
            state.record_wait_decision(WalGroupCommitWaitDecision::SingleRequest, Duration::ZERO);
            return Ok(());
        }
        let delay = effective_group_commit_delay(&mut state, self.group_commit.config);
        if delay.is_zero() {
            return Ok(());
        }
        let deadline = Instant::now() + delay;
        state.metrics.coalescing_wait_count = state.metrics.coalescing_wait_count.saturating_add(1);
        while state.queue.len() < self.group_commit.config.max_entries().get() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let waited = self
                .group_commit
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| group_commit_coordinator_poisoned_error())?;
            state = waited.0;
            if waited.1.timed_out() {
                break;
            }
        }
        Ok(())
    }

    fn fail_front_group(&self, message: String) -> Result<()> {
        let requests = {
            let mut state = self.group_commit.lock_state_recover();
            let count = state
                .queue
                .len()
                .min(self.group_commit.config.max_entries().get());
            let requests = state.queue.drain(..count).collect::<Vec<_>>();
            state.metrics.group_count = state.metrics.group_count.saturating_add(1);
            requests
        };
        let completed = requests
            .into_iter()
            .map(|request| {
                (
                    request,
                    Err(HawDBError::Storage(format!(
                        "WAL group commit could not start: {message}"
                    ))),
                )
            })
            .collect();
        complete_commit_requests(completed)?;
        self.group_commit.available.notify_all();
        Ok(())
    }
}

impl Debug for CommitSequencer {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommitSequencer")
            .field("group_commit", &self.group_commit.config)
            .finish_non_exhaustive()
    }
}

type CommitTask = Box<dyn FnOnce(&mut Database) -> Result<QueryOutput> + Send + 'static>;

struct QueuedCommit {
    task: Mutex<Option<CommitTask>>,
    result: Mutex<Option<Result<QueryOutput>>>,
}

impl QueuedCommit {
    fn new(task: CommitTask) -> Self {
        Self {
            task: Mutex::new(Some(task)),
            result: Mutex::new(None),
        }
    }

    fn take_task(&self) -> Result<CommitTask> {
        self.task
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .take()
            .ok_or_else(|| {
                HawDBError::Execution("WAL group commit task was already consumed".to_string())
            })
    }

    fn complete(&self, result: Result<QueryOutput>) -> Result<()> {
        *self
            .result
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())? = Some(result);
        Ok(())
    }

    fn take_result(&self) -> Result<Option<Result<QueryOutput>>> {
        Ok(self
            .result
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .take())
    }

    fn has_result(&self) -> Result<bool> {
        Ok(self
            .result
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .is_some())
    }
}

struct GroupCommitCoordinator {
    config: WalGroupCommitConfig,
    state: Mutex<GroupCommitState>,
    available: Condvar,
    #[cfg(test)]
    post_enqueue_barrier: Mutex<Option<Arc<Barrier>>>,
}

impl GroupCommitCoordinator {
    fn new(config: WalGroupCommitConfig) -> Self {
        Self {
            config,
            state: Mutex::new(GroupCommitState::new(config, Instant::now())),
            available: Condvar::new(),
            #[cfg(test)]
            post_enqueue_barrier: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn set_post_enqueue_barrier(&self, barrier: Arc<Barrier>) -> Result<()> {
        *self
            .post_enqueue_barrier
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())? = Some(barrier);
        Ok(())
    }

    #[cfg(test)]
    fn wait_after_enqueue(&self) -> Result<()> {
        let barrier = self
            .post_enqueue_barrier
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())?
            .clone();
        if let Some(barrier) = barrier {
            barrier.wait();
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, GroupCommitState>> {
        self.state
            .lock()
            .map_err(|_| group_commit_coordinator_poisoned_error())
    }

    fn lock_state_recover(&self) -> MutexGuard<'_, GroupCommitState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn release_leader(&self) {
        self.lock_state_recover().leader_active = false;
        self.available.notify_all();
    }
}

struct GroupCommitLeaderGuard<'a> {
    coordinator: &'a GroupCommitCoordinator,
}

impl<'a> GroupCommitLeaderGuard<'a> {
    fn new(coordinator: &'a GroupCommitCoordinator) -> Self {
        Self { coordinator }
    }
}

impl Drop for GroupCommitLeaderGuard<'_> {
    fn drop(&mut self) {
        self.coordinator.release_leader();
    }
}

fn fail_completed_commits(
    completed: &mut [(Arc<QueuedCommit>, Result<QueryOutput>)],
    message: String,
) {
    for (_, result) in completed {
        *result = Err(HawDBError::Storage(message.clone()));
    }
}

fn complete_commit_requests(
    completed: Vec<(Arc<QueuedCommit>, Result<QueryOutput>)>,
) -> Result<()> {
    let mut first_error = None;
    for (request, result) in completed {
        if let Err(error) = request.complete(result)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct GroupCommitState {
    queue: VecDeque<Arc<QueuedCommit>>,
    leader_active: bool,
    metrics: WalGroupCommitSnapshot,
    fsync_window: AdaptiveFsyncWindow,
}

impl GroupCommitState {
    fn new(config: WalGroupCommitConfig, now: Instant) -> Self {
        Self {
            queue: VecDeque::new(),
            leader_active: false,
            metrics: WalGroupCommitSnapshot {
                activation: config.activation(),
                delay_policy: config.delay_policy(),
                ..WalGroupCommitSnapshot::default()
            },
            fsync_window: AdaptiveFsyncWindow::new(now),
        }
    }

    fn refresh_fsync_estimate(&mut self, now: Instant) -> AdaptiveFsyncEstimate {
        let estimate = self.fsync_window.estimate(now);
        self.metrics.fsync_baseline_micros = estimate.baseline_micros;
        self.metrics.fsync_baseline_sample_count = estimate.sample_count;
        estimate
    }

    /// A follower only ever waits for a leader to complete it. Its request is
    /// either complete, still queued, or held by the active leader.
    ///
    /// The leader releases leadership through an RAII guard and completes every
    /// request it dequeued. The completion notification can race with the
    /// liveness timeout, so a completed result must be checked before treating
    /// an unowned request as inconsistent. If none of these states applies, no
    /// wakeup can arrive and waiting again would hang the caller forever.
    fn assert_commit_is_accounted_for(&self, request: &Arc<QueuedCommit>) -> Result<()> {
        if self.leader_active
            || self.queue.iter().any(|queued| Arc::ptr_eq(queued, request))
            || request.has_result()?
        {
            return Ok(());
        }
        Err(HawDBError::Execution(
            "WAL group commit request was dequeued without being completed; \
             the commit sequencer is inconsistent and the database must be \
             closed and reopened"
                .to_string(),
        ))
    }

    fn record_wait_decision(&mut self, decision: WalGroupCommitWaitDecision, delay: Duration) {
        let delay_micros = duration_micros(delay);
        self.metrics.last_wait_decision = decision;
        self.metrics.effective_delay_micros = delay_micros;
        self.metrics.max_observed_effective_delay_micros = self
            .metrics
            .max_observed_effective_delay_micros
            .max(delay_micros);
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AdaptiveFsyncBucket {
    tick: Option<u64>,
    total_micros: u64,
    sample_count: u64,
}

struct AdaptiveFsyncWindow {
    origin: Instant,
    buckets: [AdaptiveFsyncBucket; ADAPTIVE_FSYNC_BUCKET_COUNT],
}

impl AdaptiveFsyncWindow {
    fn new(origin: Instant) -> Self {
        Self {
            origin,
            buckets: [AdaptiveFsyncBucket::default(); ADAPTIVE_FSYNC_BUCKET_COUNT],
        }
    }

    fn record(&mut self, now: Instant, fsync_micros: u64) {
        let tick = self.tick(now);
        let bucket_index = (tick % ADAPTIVE_FSYNC_BUCKET_COUNT as u64) as usize;
        let bucket = &mut self.buckets[bucket_index];
        if bucket.tick != Some(tick) {
            *bucket = AdaptiveFsyncBucket {
                tick: Some(tick),
                ..AdaptiveFsyncBucket::default()
            };
        }
        bucket.total_micros = bucket.total_micros.saturating_add(fsync_micros);
        bucket.sample_count = bucket.sample_count.saturating_add(1);
    }

    fn estimate(&self, now: Instant) -> AdaptiveFsyncEstimate {
        let current_tick = self.tick(now);
        let mut sample_count = 0u64;
        let mut baseline_micros = None;
        for bucket in &self.buckets {
            let Some(tick) = bucket.tick else {
                continue;
            };
            let age = current_tick.saturating_sub(tick);
            if age == 0 || age > ADAPTIVE_FSYNC_BUCKET_COUNT as u64 || tick > current_tick {
                continue;
            }
            // Excluding the in-progress bucket prevents a partial low average
            // from being mistaken for a faster durability baseline.
            sample_count = sample_count.saturating_add(bucket.sample_count);
            let average = bucket.total_micros.div_ceil(bucket.sample_count.max(1));
            baseline_micros =
                Some(baseline_micros.map_or(average, |baseline: u64| baseline.min(average)));
        }
        if sample_count < ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
            baseline_micros = None;
        }
        AdaptiveFsyncEstimate {
            baseline_micros,
            sample_count,
        }
    }

    fn tick(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.origin).as_nanos();
        let bucket_nanos = ADAPTIVE_FSYNC_BUCKET_DURATION.as_nanos().max(1);
        u64::try_from(elapsed / bucket_nanos).unwrap_or(u64::MAX)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AdaptiveFsyncEstimate {
    baseline_micros: Option<u64>,
    sample_count: u64,
}

fn effective_group_commit_delay(
    state: &mut GroupCommitState,
    config: WalGroupCommitConfig,
) -> Duration {
    effective_group_commit_delay_at(state, config, Instant::now())
}

fn effective_group_commit_delay_at(
    state: &mut GroupCommitState,
    config: WalGroupCommitConfig,
    now: Instant,
) -> Duration {
    if config.delay_policy() == WalGroupCommitDelayPolicy::Fixed {
        let delay = config.max_delay();
        state.record_wait_decision(WalGroupCommitWaitDecision::FixedDelay, delay);
        return delay;
    }

    let estimate = state.refresh_fsync_estimate(now);
    let Some(baseline_micros) = estimate.baseline_micros else {
        state.metrics.adaptive_fallback_count =
            state.metrics.adaptive_fallback_count.saturating_add(1);
        // The adaptive cap may be intentionally larger for slow devices. Until
        // measurements justify that larger window, retain the known fixed
        // default while still honoring a caller's tighter bound.
        let fallback = config.max_delay().min(DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY);
        if fallback < MIN_USEFUL_COALESCING_DELAY {
            state
                .record_wait_decision(WalGroupCommitWaitDecision::BelowUsefulDelay, Duration::ZERO);
            return Duration::ZERO;
        }
        state.record_wait_decision(WalGroupCommitWaitDecision::AdaptiveFallbackDelay, fallback);
        return fallback;
    };
    let derived_micros = u64::try_from(
        u128::from(baseline_micros) * u128::from(ADAPTIVE_FSYNC_FRACTION_PER_MILLION) / 1_000_000,
    )
    .unwrap_or(u64::MAX);
    let max_delay_micros = duration_micros(config.max_delay());
    let effective_micros = derived_micros.min(max_delay_micros);
    if derived_micros > max_delay_micros {
        state.metrics.adaptive_delay_clamp_count =
            state.metrics.adaptive_delay_clamp_count.saturating_add(1);
    }
    let delay = Duration::from_micros(effective_micros);
    if delay < MIN_USEFUL_COALESCING_DELAY {
        state.record_wait_decision(WalGroupCommitWaitDecision::BelowUsefulDelay, Duration::ZERO);
        return Duration::ZERO;
    }
    state.record_wait_decision(WalGroupCommitWaitDecision::AdaptiveDelay, delay);
    delay
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[derive(Debug, Default)]
pub(super) struct LockManager {
    state: Mutex<LockManagerState>,
    available: Condvar,
}

#[derive(Debug, Clone)]
pub(super) struct LockSavepoint {
    requests: Vec<LockRequest>,
}

#[derive(Debug, Default)]
struct LockManagerState {
    locks: LockTable,
    wait_for: WaitForGraph,
}

impl LockManager {
    pub(super) fn savepoint(&self, transaction_id: u64) -> Result<LockSavepoint> {
        Ok(LockSavepoint {
            requests: self.lock_state()?.locks.savepoint(transaction_id),
        })
    }

    pub(super) fn restore(&self, transaction_id: u64, savepoint: LockSavepoint) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .locks
            .restore_transaction(transaction_id, savepoint.requests);
        state.wait_for.clear_waiter(transaction_id);
        drop(state);
        self.available.notify_all();
    }

    pub(super) fn covers_all(&self, transaction_id: u64, requests: &[LockRequest]) -> Result<bool> {
        Ok(self
            .lock_state()?
            .locks
            .covers_all(transaction_id, requests))
    }

    pub(super) fn acquire(
        &self,
        transaction_id: u64,
        requests: &[LockRequest],
        started: Instant,
        timeout: Duration,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let mut unique_requests = requests.to_vec();
        unique_requests.sort_by(LockRequest::acquisition_cmp);
        unique_requests.dedup();
        for request in unique_requests {
            if state
                .locks
                .covers_all(transaction_id, std::slice::from_ref(&request))
            {
                continue;
            }
            let request = state.locks.normalized_request(transaction_id, request);
            if state
                .locks
                .covers_all(transaction_id, std::slice::from_ref(&request))
            {
                continue;
            }
            loop {
                let blockers = state.locks.blockers(transaction_id, &request);
                if blockers.is_empty() {
                    state.wait_for.clear_waiter(transaction_id);
                    state.locks.grant(transaction_id, request)?;
                    break;
                }
                state.wait_for.register(transaction_id, &blockers)?;
                let remaining = timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    state.wait_for.clear_waiter(transaction_id);
                    return Err(lock_timeout_error(timeout));
                }
                let waited = self.available.wait_timeout(state, remaining);
                let (next, wait) = match waited {
                    Ok(waited) => waited,
                    Err(poisoned) => {
                        let (mut recovered, _) = poisoned.into_inner();
                        recovered.wait_for.clear_waiter(transaction_id);
                        return Err(lock_manager_poisoned_error());
                    }
                };
                state = next;
                state.wait_for.clear_waiter(transaction_id);
                if wait.timed_out() {
                    return Err(lock_timeout_error(timeout));
                }
            }
        }
        Ok(())
    }

    pub(super) fn release(&self, transaction_id: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.locks.release_transaction(transaction_id);
        state.wait_for.remove_transaction(transaction_id);
        drop(state);
        self.available.notify_all();
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, LockManagerState>> {
        self.state.lock().map_err(|_| lock_manager_poisoned_error())
    }
}

#[derive(Debug)]
pub(super) struct TransactionIdAllocator {
    next: AtomicU64,
}

impl Default for TransactionIdAllocator {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }
}

impl TransactionIdAllocator {
    pub(super) fn allocate(&self) -> Result<u64> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                HawDBError::Execution("concurrent transaction id space is exhausted".to_string())
            })
    }
}

fn lock_manager_poisoned_error() -> HawDBError {
    HawDBError::Execution("concurrent lock manager is poisoned".to_string())
}

fn group_commit_coordinator_poisoned_error() -> HawDBError {
    HawDBError::Execution("WAL group commit coordinator is poisoned".to_string())
}

fn lock_timeout_error(timeout: Duration) -> HawDBError {
    HawDBError::Execution(format!(
        "transaction lock wait timed out after {} ms",
        timeout.as_millis()
    ))
}

#[cfg(test)]
mod group_commit_tests {
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};

    fn test_config(max_entries: usize) -> WalGroupCommitConfig {
        WalGroupCommitConfig::benchmark_candidate(
            NonZeroUsize::new(max_entries).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            Duration::ZERO,
        )
        .unwrap()
    }

    fn adaptive_test_config(max_delay: Duration) -> WalGroupCommitConfig {
        WalGroupCommitConfig::benchmark_adaptive_candidate(
            NonZeroUsize::new(8).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            max_delay,
        )
        .unwrap()
    }

    fn successful_task() -> CommitTask {
        Box::new(|_| {
            Ok(QueryOutput {
                rows: Vec::new().into(),
            })
        })
    }

    #[test]
    fn liveness_check_distinguishes_completed_and_orphaned_requests() {
        let sequencer = CommitSequencer::new(Database::new(), test_config(2));
        let request = Arc::new(QueuedCommit::new(successful_task()));

        // Reproduce the state a leader would leave behind if it dequeued a
        // request and returned without completing it: no leader holds the
        // pipeline, and the request is in nobody's queue. Before this check
        // existed such a follower waited on the condvar forever, because the
        // only thread that could have woken it was already gone.
        let state = sequencer.group_commit.lock_state().unwrap();
        let error = state
            .assert_commit_is_accounted_for(&request)
            .expect_err("an unowned request must not be left waiting");
        assert!(
            error.to_string().contains("without being completed"),
            "unexpected error: {error}"
        );
        drop(state);

        // A request still queued, or one held by an active leader, is owned by
        // someone who will wake it, so waiting again is correct.
        let mut state = sequencer.group_commit.lock_state().unwrap();
        state.queue.push_back(Arc::clone(&request));
        assert!(state.assert_commit_is_accounted_for(&request).is_ok());
        state.queue.clear();
        state.leader_active = true;
        assert!(state.assert_commit_is_accounted_for(&request).is_ok());
        state.leader_active = false;

        // Completion and the timeout can become visible together. The request
        // is no longer owned at that point, but the next loop iteration can
        // still consume its result and must not report an inconsistency.
        request
            .complete(Ok(QueryOutput {
                rows: Vec::new().into(),
            }))
            .unwrap();
        assert!(state.assert_commit_is_accounted_for(&request).is_ok());
        assert!(request
            .take_result()
            .unwrap()
            .expect("completed request result must remain available")
            .is_ok());
    }

    #[test]
    fn panicking_group_commit_task_completes_followers_and_releases_leader() {
        let sequencer = CommitSequencer::new(Database::new(), test_config(2));
        let panicking = Arc::new(QueuedCommit::new(Box::new(|_| panic!("commit task panic"))));
        let follower = Arc::new(QueuedCommit::new(successful_task()));
        {
            let mut state = sequencer.group_commit.lock_state().unwrap();
            state.queue.push_back(Arc::clone(&panicking));
            state.queue.push_back(Arc::clone(&follower));
            state.leader_active = true;
        }

        sequencer.run_group_commit().unwrap();

        let panic_error = panicking
            .take_result()
            .unwrap()
            .expect("panicking request must complete")
            .unwrap_err();
        assert!(panic_error.to_string().contains("task panicked"));
        assert!(follower.take_result().unwrap().is_none());
        assert!(!sequencer.group_commit.lock_state().unwrap().leader_active);

        sequencer.group_commit.lock_state().unwrap().leader_active = true;
        sequencer.run_group_commit().unwrap();

        let follower_error = follower
            .take_result()
            .unwrap()
            .expect("queued follower must complete")
            .unwrap_err();
        assert!(follower_error.to_string().contains("sequencer is poisoned"));
        assert!(!sequencer.group_commit.lock_state().unwrap().leader_active);
    }

    #[test]
    fn consumed_group_commit_task_completes_the_group_and_releases_leader() {
        let sequencer = CommitSequencer::new(Database::new(), test_config(2));
        let consumed = Arc::new(QueuedCommit::new(successful_task()));
        drop(consumed.take_task().unwrap());
        let follower = Arc::new(QueuedCommit::new(successful_task()));
        {
            let mut state = sequencer.group_commit.lock_state().unwrap();
            state.queue.push_back(Arc::clone(&consumed));
            state.queue.push_back(Arc::clone(&follower));
            state.leader_active = true;
        }

        sequencer.run_group_commit().unwrap();

        let consumed_error = consumed
            .take_result()
            .unwrap()
            .expect("consumed request must complete")
            .unwrap_err();
        assert!(consumed_error.to_string().contains("already consumed"));
        assert!(follower
            .take_result()
            .unwrap()
            .expect("follower must complete")
            .is_ok());
        let state = sequencer.group_commit.lock_state().unwrap();
        assert!(!state.leader_active);
        assert_eq!(state.metrics.completed_commits, 1);
    }

    #[test]
    fn adaptive_fsync_window_uses_completed_bucket_average_lower_envelope() {
        let origin = Instant::now();
        let mut window = AdaptiveFsyncWindow::new(origin);
        for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
            window.record(origin + Duration::from_millis(1), 4_000);
        }
        assert_eq!(
            window.estimate(origin + Duration::from_millis(50)),
            AdaptiveFsyncEstimate::default()
        );
        assert_eq!(
            window.estimate(origin + ADAPTIVE_FSYNC_BUCKET_DURATION),
            AdaptiveFsyncEstimate {
                baseline_micros: Some(4_000),
                sample_count: ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES,
            }
        );

        for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
            window.record(origin + Duration::from_millis(110), 8_000);
        }
        assert_eq!(
            window.estimate(origin + Duration::from_millis(200)),
            AdaptiveFsyncEstimate {
                baseline_micros: Some(4_000),
                sample_count: ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES * 2,
            }
        );
        assert_eq!(
            window.estimate(origin + Duration::from_millis(10_100)),
            AdaptiveFsyncEstimate {
                baseline_micros: Some(8_000),
                sample_count: ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES,
            }
        );
    }

    #[test]
    fn adaptive_delay_is_derived_from_the_completed_baseline() {
        let origin = Instant::now();
        let config = adaptive_test_config(Duration::from_micros(500));
        let mut state = GroupCommitState::new(config, origin);
        for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
            state
                .fsync_window
                .record(origin + Duration::from_millis(1), 4_000);
        }
        assert_eq!(
            effective_group_commit_delay_at(
                &mut state,
                config,
                origin + ADAPTIVE_FSYNC_BUCKET_DURATION,
            ),
            Duration::from_micros(300)
        );
        assert_eq!(
            state.metrics.last_wait_decision,
            WalGroupCommitWaitDecision::AdaptiveDelay
        );
        assert_eq!(state.metrics.effective_delay_micros, 300);
    }

    #[test]
    fn adaptive_delay_uses_bounded_fallback_before_the_completed_sample_floor() {
        let origin = Instant::now();
        let config = adaptive_test_config(Duration::from_micros(500));
        let mut state = GroupCommitState::new(config, origin);
        for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES - 1 {
            state
                .fsync_window
                .record(origin + Duration::from_millis(1), 4_000);
        }
        assert_eq!(
            effective_group_commit_delay_at(
                &mut state,
                config,
                origin + ADAPTIVE_FSYNC_BUCKET_DURATION,
            ),
            DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY
        );
        assert_eq!(
            state.metrics.last_wait_decision,
            WalGroupCommitWaitDecision::AdaptiveFallbackDelay
        );
        assert_eq!(state.metrics.adaptive_fallback_count, 1);
        assert_eq!(
            state.metrics.fsync_baseline_sample_count,
            ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES - 1
        );

        let short_origin = Instant::now();
        let short_config = adaptive_test_config(Duration::from_micros(40));
        let mut short_state = GroupCommitState::new(short_config, short_origin);
        assert_eq!(
            effective_group_commit_delay_at(&mut short_state, short_config, short_origin),
            Duration::ZERO
        );
        assert_eq!(
            short_state.metrics.last_wait_decision,
            WalGroupCommitWaitDecision::BelowUsefulDelay
        );
        assert_eq!(short_state.metrics.adaptive_fallback_count, 1);
    }

    #[test]
    fn adaptive_delay_falls_back_after_the_recent_window_expires() {
        let origin = Instant::now();
        let config = adaptive_test_config(Duration::from_micros(500));
        let mut state = GroupCommitState::new(config, origin);
        for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
            state
                .fsync_window
                .record(origin + Duration::from_millis(1), 4_000);
        }
        assert_eq!(
            effective_group_commit_delay_at(
                &mut state,
                config,
                origin + ADAPTIVE_FSYNC_BUCKET_DURATION,
            ),
            Duration::from_micros(300)
        );

        assert_eq!(
            effective_group_commit_delay_at(
                &mut state,
                config,
                origin + Duration::from_millis(10_200),
            ),
            DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY
        );
        assert_eq!(
            state.metrics.last_wait_decision,
            WalGroupCommitWaitDecision::AdaptiveFallbackDelay
        );
        assert_eq!(state.metrics.adaptive_fallback_count, 1);
        assert_eq!(state.metrics.fsync_baseline_sample_count, 0);
    }

    #[test]
    fn adaptive_delay_matrix_tracks_fast_and_slow_fsync_baselines() {
        let cases = [
            (
                50,
                Duration::ZERO,
                WalGroupCommitWaitDecision::BelowUsefulDelay,
                0,
            ),
            (
                400,
                Duration::ZERO,
                WalGroupCommitWaitDecision::BelowUsefulDelay,
                0,
            ),
            (
                3_500,
                Duration::from_micros(262),
                WalGroupCommitWaitDecision::AdaptiveDelay,
                0,
            ),
            (
                20_000,
                Duration::from_micros(500),
                WalGroupCommitWaitDecision::AdaptiveDelay,
                1,
            ),
        ];
        for (fsync_micros, expected_delay, expected_decision, expected_clamps) in cases {
            let origin = Instant::now();
            let config = adaptive_test_config(Duration::from_micros(500));
            let mut state = GroupCommitState::new(config, origin);
            for _ in 0..ADAPTIVE_FSYNC_MIN_COMPLETED_SAMPLES {
                state
                    .fsync_window
                    .record(origin + Duration::from_millis(1), fsync_micros);
            }

            assert_eq!(
                effective_group_commit_delay_at(
                    &mut state,
                    config,
                    origin + ADAPTIVE_FSYNC_BUCKET_DURATION,
                ),
                expected_delay
            );
            assert_eq!(state.metrics.last_wait_decision, expected_decision);
            assert_eq!(state.metrics.adaptive_delay_clamp_count, expected_clamps);
        }
    }
}
