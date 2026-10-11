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

//! One checkpoint owner for a fully admitted persistent branch runtime.

use super::{Catalog, DatabaseConfig, GraphStore, LocalQosScheduler, ReaderPins};
use crate::error::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{
    RuntimeAdmissionError, RuntimeGovernor, RuntimeMaintenanceWork, RuntimeWorkRequest,
};
use hawdb_storage::background::{
    CheckpointOperationError, CheckpointWorkContext, CheckpointWorkError,
};
use hawdb_storage::store::{CheckpointCandidate, CheckpointDebtSnapshot, CheckpointSourceIdentity};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError};
use std::thread::JoinHandle;
use std::time::Duration;

const RETRY_DELAY: Duration = Duration::from_millis(100);
const PLANNING_MEMORY_BYTES: u64 = 64 * 1024;
const MAX_OPERATION_FAILURES: u8 = 3;
// Cap the serialized suffix admitted while new writers yield to publication.
// This is not a bound on schema replay, allocation or physical synchronization.
const MAX_FINAL_DRAIN_BYTES: u64 = 32 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticCheckpointReport {
    pub preparing: bool,
    pub waiting_for_handoff: bool,
    pub completed_checkpoints: u64,
    pub deferred_attempts: u64,
    pub failed_attempts: u64,
    /// Non-admission preparation, replay or publication failures.
    pub operation_failures: u64,
    /// The owner released its private candidate after three operation failures
    /// in one durable WAL generation. A successful manual checkpoint resets
    /// this circuit. Foreground integrity checks continue to apply.
    pub operation_retry_exhausted: bool,
    /// Source-size traversals started, excluding scalar-only admission retries.
    pub planning_scans: u64,
    pub planning_cache_hits: u64,
    /// Admission denial in the latest preparation attempt. Non-retryable
    /// requests exceed current capacity; replacing or refreshing the governor
    /// can change that capacity. A new attempt clears this value.
    pub preparation_admission_denial: Option<RuntimeAdmissionError>,
}

#[derive(Default)]
struct PreparationReport {
    planning_scans: u64,
    planning_cache_hits: u64,
    admission_denial: Option<RuntimeAdmissionError>,
    operation_failed: bool,
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct PrefixSealProbe {
    sealed: std::sync::mpsc::Sender<u64>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
impl PrefixSealProbe {
    fn observe(&self, epoch: u64) -> Result<()> {
        self.sealed.send(epoch).map_err(|error| {
            HawDBError::Execution(format!("checkpoint seal observer stopped: {error}"))
        })?;
        self.resume
            .lock()
            .map_err(|_| Control::poisoned())?
            .recv_timeout(Duration::from_secs(15))
            .map_err(|error| {
                HawDBError::Execution(format!("checkpoint seal observer stopped: {error}"))
            })
    }
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct OwnerPauseProbe {
    paused: std::sync::mpsc::Sender<()>,
    resume: Mutex<std::sync::mpsc::Receiver<()>>,
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct RetryWaitProbe {
    waiting: std::sync::mpsc::Sender<()>,
    woken: std::sync::mpsc::Sender<bool>,
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
impl OwnerPauseProbe {
    fn observe(&self) {
        self.paused.send(()).expect("owner pause observer stopped");
        self.resume
            .lock()
            .expect("owner pause observer poisoned")
            .recv_timeout(Duration::from_secs(15))
            .expect("owner pause observer stopped");
    }
}

#[derive(Debug)]
pub(super) struct Source {
    store: GraphStore,
    catalog: Catalog,
    planned_memory: Option<(CheckpointSourceIdentity, u64)>,
    preparing: Option<Preparing>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    retirement_probe: Option<SourceRetirementProbe>,
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
#[derive(Debug)]
struct SourceRetirementProbe(Arc<std::sync::atomic::AtomicUsize>);

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
impl Drop for SourceRetirementProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Source {
    fn capture(store: &GraphStore, catalog: &Catalog) -> Self {
        Self {
            store: store.checkpoint_source(),
            catalog: catalog.clone(),
            planned_memory: None,
            preparing: None,
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            retirement_probe: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Idle,
    Preparing,
    Draining,
    Finalizing,
    // Off-gate private cleanup; manual work still waits for Idle.
    Discarding,
    Handoff,
    Retiring,
    // Published disk work is complete; old COW storage still owns admission.
    Releasing,
}

#[derive(Debug)]
struct Admission {
    runtime: RuntimeMaintenanceWork,
}

#[derive(Debug)]
struct Preparing {
    // Captured plans and private bootstrap state die before their admission.
    frame: Box<hawdb_storage::store::CheckpointPreparation>,
    admission: Admission,
}

#[derive(Debug)]
struct Pending {
    // Private state dies before its admission, including on manual invalidation.
    candidate: CheckpointCandidate,
    admission: Admission,
}

#[derive(Debug)]
struct Selected {
    store: GraphStore,
    expected: CheckpointSourceIdentity,
    candidate: CheckpointCandidate,
    admission: Admission,
}

#[derive(Debug)]
struct Retired {
    old: GraphStore,
    selected: Source,
    candidate: CheckpointCandidate,
    admission: Admission,
}

#[derive(Debug, Default)]
pub(super) struct State {
    enabled: bool,
    stopping: bool,
    suspensions: usize,
    phase: Phase,
    latest: Option<Source>,
    pending: Option<Pending>,
    last_identity: Option<CheckpointSourceIdentity>,
    sync_group_active: bool,
    attempts_started: u64,
    operation_failures_in_generation: u8,
    operation_generation: Option<u64>,
    retirement_deferrals: u64,
    debt: Option<CheckpointDebtSnapshot>,
    selected: Option<Selected>,
    retired: Option<Retired>,
    task: Option<RuntimeTaskContext>,
    governor: Option<RuntimeGovernor>,
    report: AutomaticCheckpointReport,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    prefix_seal_probe: Option<Arc<PrefixSealProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    publication_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    publication_io_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    discard_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    manual_wait_probe: Option<std::sync::mpsc::Sender<Phase>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    idle_start_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    retirement_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    retirement_drop_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    frontend_wait_probe: Option<std::sync::mpsc::Sender<Phase>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    frontend_resume_probe: Option<Arc<OwnerPauseProbe>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    preparation_error_probe: Option<std::sync::mpsc::Sender<String>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    owner_wait_probe: Option<std::sync::mpsc::Sender<Phase>>,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    retry_wait_probe: Option<RetryWaitProbe>,
}

#[derive(Debug, Default)]
pub(super) struct Control {
    state: Mutex<State>,
    changed: Condvar,
    failed: AtomicBool,
    handoff_ready: AtomicBool,
    publication_requested: AtomicBool,
}

struct PublicationRequest<'a>(&'a Control);

impl Drop for PublicationRequest<'_> {
    fn drop(&mut self) {
        // Predicate changes and notification share the frontend's mutex.
        // No publication I/O guard or State borrow may outlive this request.
        let _state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.0.publication_requested.store(false, Ordering::Release);
        self.0.changed.notify_all();
    }
}

impl Control {
    pub(super) fn ensure_healthy(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint owner failed; close and reopen the database".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn lock_frontend(&self) -> Result<MutexGuard<'_, State>> {
        self.ensure_healthy()?;
        let mut state = self.lock()?;
        let observed_attempts = state.attempts_started;
        let observed_retirement_deferrals = state.retirement_deferrals;
        let mut notified_owner = false;
        while state.phase == Phase::Finalizing
            || (self.publication_requested.load(Ordering::Acquire) && !state.sync_group_active)
            || (state.phase == Phase::Draining && !state.sync_group_active)
            || (state.phase == Phase::Preparing
                && !state.sync_group_active
                && needs_headroom(&state))
            || (state.phase == Phase::Retiring
                && !state.sync_group_active
                && (state.retired.is_none()
                    || state.retirement_deferrals == observed_retirement_deferrals)
                && pressure_pending(&state)
                && needs_headroom(&state))
            || (state.phase == Phase::Idle
                && state.enabled
                && !state.stopping
                && state.suspensions == 0
                && !state.sync_group_active
                && !state.report.operation_retry_exhausted
                && state.attempts_started == observed_attempts
                && pressure_pending(&state)
                && needs_headroom(&state))
        {
            if !notified_owner {
                // Entering pressure backoff requests an attempt immediately.
                // The owner and this predicate share the mutex, so the wake
                // cannot be lost between predicate evaluation and waiting.
                self.changed.notify_all();
                notified_owner = true;
            }
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            if let Some(probe) = &state.frontend_wait_probe {
                let _ = probe.send(state.phase);
            }
            state = self.changed.wait(state).map_err(|_| Self::poisoned())?;
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            if let Some(probe) = state.frontend_resume_probe.take() {
                drop(state);
                probe.observe();
                state = self.lock()?;
            }
        }
        self.ensure_healthy()?;
        Ok(state)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Self::poisoned())
    }

    fn request_publication(&self) -> PublicationRequest<'_> {
        self.publication_requested.store(true, Ordering::Release);
        PublicationRequest(self)
    }

    fn poisoned() -> HawDBError {
        HawDBError::StorageIntegrity("automatic checkpoint owner lock is poisoned".into())
    }

    pub(super) fn submit(
        &self,
        state: &mut State,
        store: &GraphStore,
        catalog: &Catalog,
    ) -> Option<Source> {
        if !state.enabled || state.stopping {
            return None;
        }
        let finished_group = state.sync_group_active && !store.wal_sync_group_active();
        state.sync_group_active = store.wal_sync_group_active();
        state.debt = store.checkpoint_debt_snapshot();
        let generation = state.debt.map(|debt| debt.wal_generation);
        if generation != state.operation_generation {
            state.operation_generation = generation;
            state.operation_failures_in_generation = 0;
            state.report.operation_retry_exhausted = false;
        }
        let identity = store.checkpoint_source_identity();
        let retired = if identity != state.last_identity || finished_group {
            let mut next = Source::capture(store, catalog);
            let mut retired = state.latest.take();
            if let Some(previous) = &mut retired
                && previous
                    .preparing
                    .as_ref()
                    .is_some_and(|preparing| preparing.frame.can_continue_from(store))
            {
                next.preparing = previous.preparing.take();
            }
            state.latest = Some(next);
            state.last_identity = identity;
            retired
        } else {
            None
        };
        self.changed.notify_all();
        // The last owner of an older COW snapshot may destroy a large map.
        // Its caller must release the publication guard before dropping it.
        retired
    }

    pub(super) fn adopt(&self, state: &mut State, store: &mut GraphStore) -> Result<()> {
        let Some(selected) = state.selected.take() else {
            return Ok(());
        };
        self.handoff_ready.store(false, Ordering::Release);
        let old = match store.adopt_selected_checkpoint(selected.store, selected.expected) {
            Ok(old) => old,
            Err(error) => {
                state.phase = Phase::Idle;
                state.stopping = true;
                state.report.waiting_for_handoff = false;
                state.report.failed_attempts += 1;
                self.failed.store(true, Ordering::Release);
                self.changed.notify_all();
                return Err(error);
            }
        };
        state.retired = Some(Retired {
            old,
            selected: Source::capture(store, &Catalog::default()),
            candidate: selected.candidate,
            admission: selected.admission,
        });
        state.phase = Phase::Retiring;
        state.report.waiting_for_handoff = false;
        self.changed.notify_all();
        Ok(())
    }

    pub(super) fn adopt_for_read(&self, store: &mut GraphStore) -> Result<()> {
        self.ensure_healthy()?;
        if !self.handoff_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        // Publication never mutates the admitted frontend. A read can retain
        // that generation if the owner is busy; reclamation waits for adoption.
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return Ok(()),
            Err(TryLockError::Poisoned(_)) => return Err(Self::poisoned()),
        };
        self.ensure_healthy()?;
        self.adopt(&mut state, store)
    }

    pub(super) fn has_pending_handoff(&self) -> bool {
        self.handoff_ready.load(Ordering::Acquire)
    }

    pub(super) fn suspend(self: &Arc<Self>) -> Result<Suspension> {
        let mut state = self.lock()?;
        state.suspensions = state.suspensions.checked_add(1).ok_or_else(|| {
            HawDBError::StorageIntegrity("checkpoint suspension count overflow".into())
        })?;
        if let Some(task) = &state.task {
            task.cancellation().cancel();
        }
        // Manual checkpoint/backup/compaction can reuse the same generation
        // namespace. Finish abandoning private work before the caller proceeds.
        // Destruction can include COW maps and staging cleanup, so do it off-gate.
        let pending = state.pending.take();
        let preparation = state
            .latest
            .as_mut()
            .and_then(|source| source.preparing.take());
        let owns_cleanup =
            state.phase == Phase::Idle && (pending.is_some() || preparation.is_some());
        if owns_cleanup {
            state.phase = Phase::Discarding;
        }
        self.changed.notify_all();
        drop(state);
        drop(pending);
        drop(preparation);
        if owns_cleanup {
            let mut state = self.lock()?;
            state.phase = Phase::Idle;
            self.changed.notify_all();
        }
        Ok(Suspension {
            control: Arc::clone(self),
        })
    }

    pub(super) fn set_governor(&self, governor: RuntimeGovernor) -> Result<()> {
        self.lock()?.governor = Some(governor);
        self.changed.notify_all();
        Ok(())
    }

    pub(super) fn release_suspended_source(&self) -> Result<()> {
        let mut state = self.lock()?;
        if state.enabled && (state.suspensions == 0 || state.phase != Phase::Idle) {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint source release requires an idle suspended owner".into(),
            ));
        }
        let source = state.latest.take();
        // The following mutable frontend guard must recapture even if the
        // reclamation operation does not change the WAL/source identity.
        state.last_identity = None;
        drop(state);
        // Releasing snapshot/branch leases and old COW pages can scale with
        // the source. Never destroy them under the publication mutex.
        drop(source);
        Ok(())
    }

    pub(super) fn report(&self) -> Result<Option<AutomaticCheckpointReport>> {
        let state = self.lock()?;
        Ok(state.enabled.then_some(state.report))
    }
}

#[derive(Debug)]
pub(super) struct Suspension {
    control: Arc<Control>,
}

impl Suspension {
    pub(super) fn wait_idle(&self) -> Result<()> {
        let mut state = self.control.lock()?;
        while state.phase != Phase::Idle {
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            if let Some(probe) = &state.manual_wait_probe {
                let _ = probe.send(state.phase);
            }
            state = self
                .control
                .changed
                .wait(state)
                .map_err(|_| Control::poisoned())?;
        }
        Ok(())
    }
}

impl Drop for Suspension {
    fn drop(&mut self) {
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.suspensions -= 1;
        self.control.changed.notify_all();
    }
}

#[derive(Debug)]
pub(super) struct Owner {
    control: Arc<Control>,
    worker: Option<JoinHandle<()>>,
}

impl Owner {
    pub(super) fn start(
        control: Arc<Control>,
        store: &GraphStore,
        catalog: &Catalog,
        pins: Arc<Mutex<ReaderPins>>,
        config: &DatabaseConfig,
        scheduler: LocalQosScheduler,
        governor: Option<RuntimeGovernor>,
    ) -> Result<Option<Self>> {
        let eligible = cfg!(all(
            feature = "background-maintenance",
            not(target_arch = "wasm32")
        )) && config
            .runtime_capabilities
            .is_enabled(hawdb_core::RuntimeCapability::BackgroundMaintenance)
            && !config.read_only
            && store
                .checkpoint_debt_snapshot()
                .is_some_and(|debt| !debt.read_only);
        if !eligible {
            return Ok(None);
        }
        let governor = governor.unwrap_or_else(|| {
            RuntimeGovernor::detect(
                hawdb_qos::RuntimeGovernorConfig::shared_host(),
                hawdb_qos::IoConcurrencyBudget::shared_host(),
            )
        });
        let retired = {
            let mut state = control.lock()?;
            state.enabled = true;
            state.governor = Some(governor);
            control.submit(&mut state, store, catalog)
        };
        drop(retired);
        let worker_control = Arc::clone(&control);
        let max_age = config.automatic_checkpoint_max_age;
        let worker = std::thread::Builder::new()
            .name("hawdb-checkpoint".into())
            .spawn(move || {
                let failure_control = Arc::clone(&worker_control);
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(worker_control, scheduler, pins, max_age);
                }))
                .is_err()
                {
                    failure_control.failed.store(true, Ordering::Release);
                    let mut state = failure_control
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.stopping = true;
                    state.phase = Phase::Idle;
                    state.report.preparing = false;
                    state.report.failed_attempts += 1;
                    failure_control.changed.notify_all();
                }
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                let latest = {
                    let mut state = control.lock()?;
                    state.enabled = false;
                    state.latest.take()
                };
                drop(latest);
                return Err(HawDBError::Storage(format!(
                    "cannot start checkpoint owner: {error}"
                )));
            }
        };
        Ok(Some(Self {
            control,
            worker: Some(worker),
        }))
    }

    pub(super) fn stop(&mut self) {
        {
            let mut state = self
                .control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.stopping = true;
            if let Some(task) = &state.task {
                task.cancellation().cancel();
            }
            self.control.changed.notify_all();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        let owned = {
            let mut state = self
                .control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let selected = state.selected.take();
            self.control.handoff_ready.store(false, Ordering::Release);
            if selected.is_some() {
                // Disk authority has moved, but the old frontend has not
                // adopted it. Discarding this handoff must fail closed.
                self.control.failed.store(true, Ordering::Release);
            }
            let owned = (
                state.latest.take(),
                state.pending.take(),
                selected,
                state.retired.take(),
                state.task.take(),
            );
            state.phase = Phase::Discarding;
            state.report.preparing = false;
            state.report.waiting_for_handoff = false;
            self.control.changed.notify_all();
            owned
        };
        // Joining the worker is insufficient when observers retain Control.
        // Release COW snapshots, open locks and builders outside the gate;
        // Selected/Retired retain admission until their storage is destroyed.
        drop(owned);
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.phase = Phase::Idle;
        self.control.changed.notify_all();
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run(
    control: Arc<Control>,
    scheduler: LocalQosScheduler,
    pins: Arc<Mutex<ReaderPins>>,
    max_age: Duration,
) {
    loop {
        let (mut source, pending, governor, task) = {
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            loop {
                if state.stopping {
                    return;
                }
                if let Some(mut retired) = state.retired.take() {
                    let task = RuntimeTaskContext::default();
                    state.task = Some(task.clone());
                    let scope = if state.suspensions == 0 {
                        RetirementScope::Background
                    } else {
                        RetirementScope::CallerRequested
                    };
                    drop(state);
                    let admission = admit_retirement(&mut retired, &task, scope);
                    match admission {
                        Ok(RetirementAdmission::Deferred) => {
                            state = control
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            state.retired = Some(retired);
                            state.task = None;
                            // A foreground caller may itself hold the CPU or
                            // I/O admission needed by retirement. End that
                            // caller's pressure wait after one denied attempt
                            // so it can finish and release its resources.
                            // Active cleanup never advances this epoch.
                            state.retirement_deferrals = state.retirement_deferrals.wrapping_add(1);
                            state.report.deferred_attempts += 1;
                            control.changed.notify_all();
                            if state.stopping || state.suspensions != 0 {
                                continue;
                            }
                            state = control
                                .changed
                                .wait_timeout(state, RETRY_DELAY)
                                .unwrap_or_else(|error| error.into_inner())
                                .0;
                            continue;
                        }
                        Ok(RetirementAdmission::Granted(wave)) => {
                            #[cfg(all(
                                test,
                                feature = "background-maintenance",
                                not(target_arch = "wasm32")
                            ))]
                            let probe = control
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .retirement_probe
                                .take();
                            #[cfg(all(
                                test,
                                feature = "background-maintenance",
                                not(target_arch = "wasm32")
                            ))]
                            if let Some(probe) = probe {
                                probe.observe();
                            }
                            let result = reclaim_retired_generations(&mut retired, &pins);
                            drop(wave);
                            state = control
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            if result.is_err() {
                                control.failed.store(true, Ordering::Release);
                                state.stopping = true;
                                state.report.failed_attempts += 1;
                            }
                        }
                        Err(_) => {
                            // Invalid publication/reclamation ownership is
                            // terminal. Published evidence remains on disk.
                            state = control
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            control.failed.store(true, Ordering::Release);
                            state.stopping = true;
                            state.report.failed_attempts += 1;
                        }
                    }
                    state.phase = Phase::Releasing;
                    state.task = None;
                    control.changed.notify_all();
                    #[cfg(all(
                        test,
                        feature = "background-maintenance",
                        not(target_arch = "wasm32")
                    ))]
                    let probe = state.retirement_drop_probe.take();
                    drop(state);
                    #[cfg(all(
                        test,
                        feature = "background-maintenance",
                        not(target_arch = "wasm32")
                    ))]
                    if let Some(probe) = probe {
                        probe.observe();
                    }
                    // Disk reclamation has finished. Old COW destruction must
                    // not extend the physical retirement phase. Synchronous
                    // maintenance still waits for complete resource release.
                    // Its admission stays owned until the final storage drop.
                    release_retired_state(retired);
                    state = control
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.phase = Phase::Idle;
                    control.changed.notify_all();
                    continue;
                }
                let due = state
                    .latest
                    .as_ref()
                    .is_some_and(|source| due(source, max_age));
                if state.phase == Phase::Idle
                    && state.suspensions == 0
                    && !state.sync_group_active
                    && !state.report.operation_retry_exhausted
                    && (due || state.pending.is_some())
                {
                    #[cfg(all(
                        test,
                        feature = "background-maintenance",
                        not(target_arch = "wasm32")
                    ))]
                    if let Some(probe) = state.idle_start_probe.take() {
                        drop(state);
                        probe.observe();
                        state = control
                            .state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        continue;
                    }
                    // A pending foreground waiter gives this owner one attempt.
                    // Denial must release it instead of repeatedly waiting on
                    // an unavailable background reservation.
                    state.attempts_started = state.attempts_started.wrapping_add(1);
                    state.phase = Phase::Preparing;
                    state.report.preparing = true;
                    let task = RuntimeTaskContext::default();
                    state.task = Some(task.clone());
                    break (
                        state.latest.take().expect("due source exists"),
                        state.pending.take(),
                        state
                            .governor
                            .clone()
                            .expect("enabled owner has a governor"),
                        task,
                    );
                }
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                if let Some(probe) = &state.owner_wait_probe {
                    let _ = probe.send(state.phase);
                }
                state = match next_work_delay(&state, max_age) {
                    Some(delay) => {
                        control
                            .changed
                            .wait_timeout(state, delay)
                            .unwrap_or_else(|error| error.into_inner())
                            .0
                    }
                    None => control
                        .changed
                        .wait(state)
                        .unwrap_or_else(|error| error.into_inner()),
                };
            }
        };
        let mut preparation_report = PreparationReport::default();
        let attempt = match pending {
            Some(pending) if pending.candidate.can_continue_from(&source.store) => {
                Ok(Some((pending.candidate, pending.admission)))
            }
            Some(pending) => {
                // A manual source/generation change invalidates the private
                // prefix. Release its state off-gate before a new preparation.
                drop(pending);
                prepare(
                    &mut source,
                    &scheduler,
                    &governor,
                    &task,
                    &mut preparation_report,
                )
            }
            None => prepare(
                &mut source,
                &scheduler,
                &governor,
                &task,
                &mut preparation_report,
            ),
        };
        {
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.report.planning_scans += preparation_report.planning_scans;
            state.report.planning_cache_hits += preparation_report.planning_cache_hits;
            state.report.preparation_admission_denial = preparation_report.admission_denial;
        }
        let (mut candidate, mut admission) = match attempt {
            Ok(Some(work)) => work,
            result => {
                let mut state = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if result.is_err() {
                    if preparation_report.operation_failed {
                        record_operation_failure(&mut state);
                        state.report.failed_attempts += 1;
                    } else {
                        state.report.deferred_attempts += 1;
                    }
                    #[cfg(all(
                        test,
                        feature = "background-maintenance",
                        not(target_arch = "wasm32")
                    ))]
                    if let (Some(probe), Err(error)) = (&state.preparation_error_probe, &result) {
                        let _ = probe.send(error.to_string());
                    }
                }
                let keep_preparation = state.suspensions == 0
                    && !state.stopping
                    && !preparation_report.operation_failed;
                let mut discarded = None;
                if let Some(latest) = &mut state.latest {
                    if keep_preparation
                        && source.preparing.as_ref().is_some_and(|preparing| {
                            preparing.frame.can_continue_from(&latest.store)
                        })
                    {
                        discarded = latest.preparing.take();
                        latest.preparing = source.preparing.take();
                    } else {
                        discarded = source.preparing.take();
                    }
                } else {
                    if !keep_preparation {
                        discarded = source.preparing.take();
                    }
                    state.latest = Some(source);
                }
                if discarded.is_some() {
                    state.phase = Phase::Discarding;
                    control.changed.notify_all();
                    drop(state);
                    drop(discarded);
                    state = control
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                }
                state.phase = Phase::Idle;
                state.task = None;
                state.report.preparing = false;
                control.changed.notify_all();
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                let probe = state.retry_wait_probe.take();
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                if let Some(probe) = &probe {
                    let _ = probe.waiting.send(());
                }
                let (state, wake) = control
                    .changed
                    .wait_timeout(state, RETRY_DELAY)
                    .unwrap_or_else(|error| error.into_inner());
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                if let Some(probe) = probe {
                    let _ = probe.woken.send(wake.timed_out());
                }
                let _ = wake;
                drop(state);
                continue;
            }
        };
        // Seal and mount each captured prefix while writes remain admitted.
        // A writer that advances during sealing supplies another suffix for
        // this same candidate; it never causes a database-sized base restart.
        let mut expected = None;
        let result = (|| -> std::result::Result<(), CheckpointOperationError<HawDBError>> {
            let mut publication_request = None;
            loop {
                admission
                    .runtime
                    .try_resume(task.clone())
                    .map_err(|_reason| {
                        CheckpointOperationError::Work(CheckpointWorkError::Contended(
                            "checkpoint execution admission",
                        ))
                    })?;
                let mut admitted_task = admission
                    .runtime
                    .task_context()
                    .expect("checkpoint execution is admitted")
                    .clone();
                let latest = {
                    let mut state = control.lock()?;
                    if state.stopping || state.suspensions != 0 {
                        return Err(CheckpointOperationError::Work(
                            CheckpointWorkError::Contended("checkpoint owner suspended"),
                        ));
                    }
                    if state.sync_group_active {
                        None
                    } else {
                        state.latest.take()
                    }
                };
                if let Some(latest) = latest {
                    source = latest;
                }
                let work = CheckpointWorkContext::new(admitted_task.clone())
                    .with_scheduler(scheduler.clone());
                candidate.catch_up_with_work_context(&source.store, &work)?;
                candidate.finish_catch_up()?;
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                {
                    let probe = control.lock()?.prefix_seal_probe.clone();
                    if let Some(probe) = probe {
                        probe.observe(candidate.commit_epoch())?;
                    }
                }
                task.checkpoint().map_err(|reason| {
                    CheckpointOperationError::Work(CheckpointWorkError::Stopped(reason))
                })?;
                // Admission waits occur before the writer barrier. The final
                // barrier only compares a sealed identity and publishes it.
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                let publication_io_probe = control.lock()?.publication_io_probe.take();
                let mut publication_wave = admitted_task
                    .acquire_io_wave(std::num::NonZeroUsize::MIN)
                    .map_err(|reason| {
                        CheckpointOperationError::Work(CheckpointWorkError::Io(reason))
                    })?;
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                if let Some(probe) = publication_io_probe {
                    probe.observe();
                }
                // A writer can need execution/I/O while owning Control. Park
                // both before waiting, and stop fresh writers from repeatedly
                // taking the gate ahead of this already sealed candidate.
                let mut state = match control.state.try_lock() {
                    Ok(state) => state,
                    Err(TryLockError::WouldBlock) => {
                        drop(publication_wave.take());
                        admission.runtime.pause();
                        publication_request.get_or_insert_with(|| control.request_publication());
                        let mut state = control.lock()?;
                        state.report.deferred_attempts += 1;
                        state
                    }
                    Err(TryLockError::Poisoned(_)) => return Err(Control::poisoned().into()),
                };
                state.phase = Phase::Draining;
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                if let Some(probe) = state.publication_probe.take() {
                    drop(state);
                    probe.observe();
                    state = control.lock()?;
                }
                if state.sync_group_active {
                    // A foreground flush may need the same I/O pool. Never
                    // retain its capacity while waiting for that flush.
                    drop(publication_wave.take());
                    admission.runtime.pause();
                    while state.sync_group_active
                        && !state.stopping
                        && state.suspensions == 0
                        && task.checkpoint().is_ok()
                    {
                        state = control
                            .changed
                            .wait(state)
                            .unwrap_or_else(|error| error.into_inner());
                    }
                    state.phase = if publication_request.is_some() {
                        Phase::Draining
                    } else {
                        Phase::Preparing
                    };
                    control.changed.notify_all();
                    drop(state);
                    continue;
                }
                if state.stopping || state.suspensions != 0 || task.checkpoint().is_err() {
                    return Err(CheckpointOperationError::Work(
                        CheckpointWorkError::Contended("checkpoint owner suspended"),
                    ));
                }
                if state.latest.as_ref().is_some_and(|latest| {
                    latest.store.checkpoint_source_identity()
                        != source.store.checkpoint_source_identity()
                }) {
                    let bounded_drain = publication_request.is_some()
                        && candidate.remaining_catch_up_bytes(
                            &state.latest.as_ref().expect("advanced source exists").store,
                        )? <= MAX_FINAL_DRAIN_BYTES;
                    let latest = state
                        .latest
                        .take()
                        .expect("validated advanced source exists");
                    state.phase = if bounded_drain {
                        Phase::Draining
                    } else {
                        Phase::Preparing
                    };
                    control.changed.notify_all();
                    drop(state);
                    drop(publication_wave.take());
                    if !bounded_drain {
                        drop(publication_request.take());
                    }
                    source = latest;
                    continue;
                }
                if publication_wave.is_none() {
                    // Control is held now: denial must release the gate, not
                    // wait for a foreground or unrelated physical wave.
                    admission.runtime.try_resume(task.clone()).map_err(|_| {
                        CheckpointOperationError::Work(CheckpointWorkError::Contended(
                            "checkpoint publication execution admission",
                        ))
                    })?;
                    admitted_task = admission
                        .runtime
                        .task_context()
                        .expect("publication execution is admitted")
                        .clone();
                    publication_wave = match admitted_task
                        .try_acquire_io_wave(std::num::NonZeroUsize::MIN)
                        .map_err(|reason| {
                            CheckpointOperationError::Work(CheckpointWorkError::Io(reason))
                        })? {
                        hawdb_core::RuntimeIoWaveTryAcquire::Acquired(wave) => wave,
                        hawdb_core::RuntimeIoWaveTryAcquire::Pending => {
                            return Err(CheckpointOperationError::Work(
                                CheckpointWorkError::Contended(
                                    "checkpoint publication I/O admission",
                                ),
                            ));
                        }
                    };
                }
                state.phase = Phase::Finalizing;
                let oldest = pins.lock().map_err(|_| Control::poisoned())?.oldest_epoch();
                expected = source.store.checkpoint_source_identity();
                let result = source
                    .store
                    .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, oldest);
                drop(state);
                drop(publication_wave);
                return result
                    .map(|_| ())
                    .map_err(CheckpointOperationError::Operation);
            }
        })();
        if result.is_ok() {
            // The complete prefix is durable and no builder or publication
            // wave remains active. Frontend adoption needs retained memory,
            // not a CPU or background task slot while the App is idle.
            admission.runtime.pause();
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.selected = Some(Selected {
                store: source.store,
                expected: expected.expect("durable source identity"),
                candidate,
                admission,
            });
            state.phase = Phase::Handoff;
            state.report.preparing = false;
            state.report.waiting_for_handoff = true;
            state.report.completed_checkpoints += 1;
            control.handoff_ready.store(true, Ordering::Release);
            control.changed.notify_all();
        } else {
            let exhausted = {
                let mut state = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if matches!(result, Err(CheckpointOperationError::Operation(_))) {
                    record_operation_failure(&mut state);
                }
                state.report.operation_retry_exhausted
            };
            if !exhausted && candidate.can_continue_from(&source.store) {
                // All builders and physical publication waves have returned.
                // Retain the private candidate's memory while yielding CPU
                // and the background task slot before waiting for recovery.
                admission.runtime.pause();
                let mut state = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if !state.stopping && state.suspensions == 0 {
                    state.pending = Some(Pending {
                        candidate,
                        admission,
                    });
                    if state.latest.is_none() {
                        state.latest = Some(source);
                    }
                    state.phase = Phase::Idle;
                    state.task = None;
                    state.report.preparing = false;
                    state.report.deferred_attempts += 1;
                    control.changed.notify_all();
                    let (state, _) = control
                        .changed
                        .wait_timeout(state, RETRY_DELAY)
                        .unwrap_or_else(|error| error.into_inner());
                    drop(state);
                    continue;
                }
                drop(state);
            }
            // The cancelled attempt no longer owns a publication barrier.
            // Wake ordinary writers before database-sized private cleanup;
            // manual maintenance waits for Idle to reuse this generation.
            {
                let mut state = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                state.phase = Phase::Discarding;
                state.report.preparing = false;
                control.changed.notify_all();
            }
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            let discard_probe = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .discard_probe
                .take();
            #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
            if let Some(probe) = discard_probe {
                probe.observe();
            }
            drop(candidate);
            drop(admission);
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.latest.is_none() {
                state.latest = Some(source);
            }
            state.phase = Phase::Idle;
            state.task = None;
            state.report.preparing = false;
            state.report.failed_attempts += 1;
            control.changed.notify_all();
            let (state, _) = control
                .changed
                .wait_timeout(state, RETRY_DELAY)
                .unwrap_or_else(|error| error.into_inner());
            drop(state);
        }
    }
}

fn needs_headroom(state: &State) -> bool {
    let Some(debt) = state.debt else {
        return false;
    };
    let reserve = debt
        .max_wal_record_bytes
        .and_then(|bytes| u64::try_from(bytes).ok());
    [
        (debt.wal_bytes, debt.max_wal_bytes),
        (debt.delta_bytes, debt.max_delta_bytes),
    ]
    .into_iter()
    .any(|(bytes, limit)| {
        limit.is_some_and(|limit| near_limit(bytes, limit, reserve.unwrap_or(limit)))
    })
}

fn near_limit(bytes: u64, limit: u64, reserve: u64) -> bool {
    let defer = (u128::from(limit)
        * u128::from(hawdb_storage::pressure::STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION)
        / 1_000_000) as u64;
    bytes >= defer.saturating_sub(reserve)
}

fn pressure_pending(state: &State) -> bool {
    state.latest.is_some()
        && state.debt.is_some_and(|debt| {
            !debt.read_only
                && debt.commit_epoch > debt.checkpoint_commit_epoch
                && soft_pressure(debt)
        })
}

fn soft_pressure(debt: CheckpointDebtSnapshot) -> bool {
    [
        (debt.wal_bytes, debt.max_wal_bytes),
        (debt.delta_bytes, debt.max_delta_bytes),
    ]
    .into_iter()
    .any(|(bytes, limit)| {
        limit.is_some_and(|limit| {
            u128::from(bytes) * 1_000_000
                >= u128::from(limit)
                    * u128::from(hawdb_storage::pressure::STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION)
        })
    })
}

fn due(source: &Source, max_age: Duration) -> bool {
    source.store.checkpoint_debt_snapshot().is_some_and(|debt| {
        !debt.read_only
            && debt.commit_epoch > debt.checkpoint_commit_epoch
            && (soft_pressure(debt) || u128::from(debt.wal_age_millis) >= max_age.as_millis())
    })
}

fn next_work_delay(state: &State, max_age: Duration) -> Option<Duration> {
    if state.phase != Phase::Idle
        || state.suspensions != 0
        || state.sync_group_active
        || state.report.operation_retry_exhausted
    {
        return None;
    }
    let debt = state.latest.as_ref()?.store.checkpoint_debt_snapshot()?;
    if debt.read_only || debt.commit_epoch <= debt.checkpoint_commit_epoch {
        return None;
    }
    Some(max_age.saturating_sub(Duration::from_millis(debt.wal_age_millis)))
}

fn prepare(
    source: &mut Source,
    scheduler: &LocalQosScheduler,
    governor: &RuntimeGovernor,
    task: &RuntimeTaskContext,
    report: &mut PreparationReport,
) -> Result<Option<(CheckpointCandidate, Admission)>> {
    report.admission_denial = None;
    report.operation_failed = false;
    task.checkpoint()
        .map_err(|reason| HawDBError::Storage(reason.to_string()))?;
    if !scheduler.policy().background_enabled {
        return Err(HawDBError::Storage(
            "automatic checkpoint QoS deferred: background disabled".into(),
        ));
    }
    if source.preparing.is_some() {
        report.planning_cache_hits += 1;
        return continue_preparation(source, scheduler, task, report);
    }
    let identity = source.store.checkpoint_source_identity();
    let cached = source
        .planned_memory
        .filter(|(planned_identity, _)| identity == Some(*planned_identity));
    let memory = if let Some((_, bytes)) = cached {
        // Only the completed scalar estimate survives denial. Admission must
        // still use current capacity, pressure and competing resource owners.
        report.planning_cache_hits += 1;
        bytes
    } else {
        // Planning borrows the captured source and keeps only scalar totals.
        // Its traversal is admitted in bounded units before the current
        // conservative whole-candidate memory reservation is calculated.
        let planning = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(
                PLANNING_MEMORY_BYTES,
            ))
            .map_err(|reason| {
                report.admission_denial = Some(reason);
                HawDBError::Storage(format!("automatic checkpoint planning deferred: {reason}"))
            })?;
        let work = hawdb_storage::background::CheckpointWorkContext::new(
            planning.bind_task_context(task.clone()),
        )
        .with_scheduler(scheduler.clone());
        report.planning_scans += 1;
        let bytes = preparation_result(
            work.classify(|work| {
                source
                    .store
                    .checkpoint_candidate_admission_bytes_with_work_context(work)
            }),
            report,
        )?;
        source.planned_memory = identity.map(|identity| (identity, bytes));
        bytes
    };
    let runtime = governor
        .try_admit_resumable_maintenance(memory, 1, task.clone())
        .map_err(|reason| {
            report.admission_denial = Some(reason);
            HawDBError::Storage(format!("automatic checkpoint admission deferred: {reason}"))
        })?;
    let Some(frame) = source.store.begin_checkpoint_preparation(&source.catalog)? else {
        return Ok(None);
    };
    source.preparing = Some(Preparing {
        frame: Box::new(frame),
        admission: Admission { runtime },
    });
    continue_preparation(source, scheduler, task, report)
}

fn continue_preparation(
    source: &mut Source,
    scheduler: &LocalQosScheduler,
    task: &RuntimeTaskContext,
    report: &mut PreparationReport,
) -> Result<Option<(CheckpointCandidate, Admission)>> {
    let preparing = source
        .preparing
        .as_mut()
        .expect("source owns its preparation");
    preparing
        .admission
        .runtime
        .try_resume(task.clone())
        .map_err(|reason| {
            report.admission_denial = Some(reason);
            HawDBError::Storage(format!("automatic checkpoint resume deferred: {reason}"))
        })?;
    let work = CheckpointWorkContext::new(
        preparing
            .admission
            .runtime
            .task_context()
            .expect("preparation execution is admitted")
            .clone(),
    )
    .with_scheduler(scheduler.clone());
    let result = preparation_result(
        work.classify(|work| preparing.frame.prepare_candidate_with_work_context(work)),
        report,
    );
    match result {
        Ok(Some(candidate)) => {
            let preparing = source.preparing.take().expect("completed preparation");
            Ok(Some((candidate, preparing.admission)))
        }
        other => {
            preparing.admission.runtime.pause();
            other.map(|_| None)
        }
    }
}

fn preparation_result<T>(
    result: std::result::Result<T, CheckpointOperationError<HawDBError>>,
    report: &mut PreparationReport,
) -> Result<T> {
    result.map_err(|error| match error {
        CheckpointOperationError::Work(error) => HawDBError::from_storage_error(error),
        CheckpointOperationError::Operation(error) => {
            report.operation_failed = true;
            error
        }
    })
}

fn record_operation_failure(state: &mut State) {
    state.report.operation_failures += 1;
    state.operation_failures_in_generation =
        state.operation_failures_in_generation.saturating_add(1);
    state.report.operation_retry_exhausted =
        state.operation_failures_in_generation >= MAX_OPERATION_FAILURES;
}

#[derive(Clone, Copy)]
enum RetirementScope {
    Background,
    // Explicit synchronous maintenance already has a host-owned admission
    // boundary and must remain usable when background work is unavailable.
    CallerRequested,
}

enum RetirementAdmission {
    Granted(Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>),
    Deferred,
}

fn admit_retirement(
    retired: &mut Retired,
    task: &RuntimeTaskContext,
    scope: RetirementScope,
) -> Result<RetirementAdmission> {
    if matches!(scope, RetirementScope::CallerRequested) {
        return Ok(RetirementAdmission::Granted(None));
    }
    if retired.admission.runtime.try_resume(task.clone()).is_ok() {
        let context = retired
            .admission
            .runtime
            .task_context()
            .expect("retirement execution is admitted");
        if let Ok(hawdb_core::RuntimeIoWaveTryAcquire::Acquired(wave)) =
            context.try_acquire_io_wave(std::num::NonZeroUsize::MIN)
        {
            return Ok(RetirementAdmission::Granted(wave));
        }
    }
    retired.admission.runtime.pause();
    retired
        .candidate
        .defer_published_generation_reclamation(&mut retired.selected.store)?;
    Ok(RetirementAdmission::Deferred)
}

fn reclaim_retired_generations(retired: &mut Retired, pins: &Mutex<ReaderPins>) -> Result<()> {
    let pinned = pins
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .pinned_physical_generations();
    retired
        .candidate
        .reclaim_published_generations(&mut retired.selected.store, &pinned)
}

fn release_retired_state(retired: Retired) {
    let Retired {
        old,
        selected,
        candidate,
        admission,
    } = retired;
    drop(old);
    drop(candidate);
    drop(selected);
    drop(admission);
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
mod tests {
    mod discard;
    mod execution_progress;
    mod handoff_execution;
    mod memory_progress;
    mod operation_retry;
    mod planning_recovery;
    mod planning_retry;
    mod preparation_resume;
    mod progress;
    mod publication_io;
    mod qos_units;
    mod read_access;
    mod read_gate;
    mod read_handoff;
    mod retirement_io;

    use super::*;
    use crate::Database;
    use std::path::PathBuf;
    use std::time::Instant;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "hawdb-automatic-checkpoint-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn wait_for(db: &Database, predicate: impl Fn(AutomaticCheckpointReport) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let report = db
                .automatic_checkpoint_report()
                .unwrap()
                .expect("persistent owner");
            if predicate(report) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "checkpoint owner did not progress: {report:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn retirement_governor() -> RuntimeGovernor {
        let governor = RuntimeGovernor::new(
            hawdb_qos::RuntimeGovernorConfig {
                cpu_slot_limit: Some(std::num::NonZeroUsize::MIN),
                background_task_limit: Some(std::num::NonZeroUsize::MIN),
                memory_budget_bytes: Some(200 * 1024 * 1024),
                ..hawdb_qos::RuntimeGovernorConfig::shared_host()
            },
            hawdb_qos::RuntimeResourceSnapshot::from_parts(
                hawdb_qos::RuntimeResourceBudget::from_limits(
                    std::num::NonZeroUsize::MIN,
                    None,
                    None,
                ),
                hawdb_qos::RuntimeMemorySnapshot::from_limits(
                    Some(1 << 30),
                    Some(1 << 30),
                    None,
                    None,
                    None,
                ),
            ),
            hawdb_qos::IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        governor
    }

    fn governor(bytes: u64) -> RuntimeGovernor {
        let governor = RuntimeGovernor::detect(
            hawdb_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(bytes),
                ..hawdb_qos::RuntimeGovernorConfig::shared_host()
            },
            hawdb_qos::IoConcurrencyBudget::new(4, 2),
        );
        governor.pin_resources();
        governor
    }

    #[test]
    fn superseded_source_remains_owned_until_publication_guard_is_released() {
        let fixture = Fixture::new();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
        for id in 0..32 {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    std::collections::BTreeMap::from([("id".into(), crate::Value::Int(id))]),
                )
                .unwrap();
        }
        let control = Control::default();
        let mut state = control.lock().unwrap();
        state.enabled = true;
        assert!(control.submit(&mut state, &store, &catalog).is_none());
        let old_identity = store.checkpoint_source_identity();
        let retired_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        state.latest.as_mut().unwrap().retirement_probe =
            Some(SourceRetirementProbe(retired_count.clone()));
        store
            .create_node(&mut catalog, "Memory", std::collections::BTreeMap::new())
            .unwrap();
        let retired = control.submit(&mut state, &store, &catalog).unwrap();
        assert_eq!(retired_count.load(Ordering::SeqCst), 0);
        assert_eq!(retired.store.checkpoint_source_identity(), old_identity);
        assert_eq!(retired.store.scan_nodes(None).count(), 32);
        assert_eq!(
            state
                .latest
                .as_ref()
                .unwrap()
                .store
                .checkpoint_source_identity(),
            store.checkpoint_source_identity()
        );
        drop(state);
        drop(retired);
        assert_eq!(retired_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn sealed_prefixes_keep_writes_admitted_and_release_io_before_group_drain() {
        let fixture = Fixture::new();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
        let properties =
            |id| std::collections::BTreeMap::from([("id".into(), crate::Value::Int(id))]);
        store
            .create_node(&mut catalog, "Memory", properties(1))
            .unwrap();
        let control = Arc::new(Control::default());
        let (sealed_tx, sealed_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
            sealed: sealed_tx,
            resume: Mutex::new(resume_rx),
        }));
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let governor = RuntimeGovernor::detect(
            hawdb_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(512 * 1024 * 1024),
                ..hawdb_qos::RuntimeGovernorConfig::shared_host()
            },
            hawdb_qos::IoConcurrencyBudget::new(1, 1),
        );
        governor.pin_resources();
        let mut owner = Owner::start(
            Arc::clone(&control),
            &store,
            &catalog,
            Arc::new(Mutex::new(ReaderPins::default())),
            &config,
            LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
            Some(governor.clone()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(sealed_rx.recv_timeout(Duration::from_secs(15)).unwrap(), 1);
        let base = fixture.0.join("checkpoint.1.hawdb");
        let base_bytes = std::fs::read(&base).unwrap();
        {
            let mut state = control.lock_frontend().unwrap();
            assert_eq!(state.phase, Phase::Preparing);
            assert!(store.begin_wal_sync_group().unwrap());
            store
                .create_node(&mut catalog, "Memory", properties(2))
                .unwrap();
            let retired = control.submit(&mut state, &store, &catalog);
            assert!(state.sync_group_active);
            drop(state);
            drop(retired);
        }
        resume_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let state = control.lock().unwrap();
            if state.phase == Phase::Draining && state.sync_group_active {
                assert_eq!(governor.snapshot().active_background_io_slots, 0);
                break;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "worker did not drain the actual sync group"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        {
            let _foreground_io = governor
                .try_admit(RuntimeWorkRequest::io(
                    hawdb_qos::RuntimeWorkPriority::Foreground,
                    1,
                    0,
                ))
                .unwrap();
            let mut state = control.lock_frontend().unwrap();
            store.finish_wal_sync_group().unwrap();
            let retired = control.submit(&mut state, &store, &catalog);
            drop(state);
            drop(retired);
        }
        assert_eq!(sealed_rx.recv_timeout(Duration::from_secs(15)).unwrap(), 2);
        {
            let mut state = control.lock_frontend().unwrap();
            assert_eq!(state.phase, Phase::Preparing);
            store
                .create_node(&mut catalog, "Memory", properties(3))
                .unwrap();
            let retired = control.submit(&mut state, &store, &catalog);
            drop(state);
            drop(retired);
        }
        resume_tx.send(()).unwrap();
        assert_eq!(sealed_rx.recv_timeout(Duration::from_secs(15)).unwrap(), 3);
        assert_eq!(std::fs::read(&base).unwrap(), base_bytes);
        control.lock().unwrap().prefix_seal_probe = None;
        resume_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let state = control.lock().unwrap();
            if state.selected.is_some() {
                assert_eq!(state.report.completed_checkpoints, 1);
                break;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "worker did not select the sealed prefix"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
        assert_eq!(store.commit_epoch(), 3);
        assert_eq!(store.node_count_for_label(None), 3);
        let retired = control.submit(&mut state, &store, &catalog);
        drop(state);
        drop(retired);
        owner.stop();
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
        // Stopping execution cannot uncharge decoded values in the adopted store.
        assert!(governor.snapshot().admitted_memory_bytes > 0);
        assert_eq!(store.commit_epoch(), 3);
        assert_eq!(store.node_count_for_label(None), 3);
        drop(owner);
        drop(control);
        drop(store);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 3);
        for id in 0..3 {
            assert_eq!(
                recovered
                    .node_owned(hawdb_storage::NodeId(id))
                    .unwrap()
                    .unwrap()
                    .properties
                    .get("id"),
                Some(&crate::Value::Int(id as i64 + 1))
            );
        }
    }

    #[test]
    fn shutdown_releases_selected_checkpoint_before_control_drop() {
        assert_shutdown_releases_owned_sources(true);
    }

    #[test]
    fn shutdown_releases_parked_source_before_control_drop() {
        assert_shutdown_releases_owned_sources(false);
    }

    fn assert_shutdown_releases_owned_sources(select: bool) {
        let fixture = Fixture::new();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                std::collections::BTreeMap::from([("id".into(), crate::Value::Int(41))]),
            )
            .unwrap();
        let control = Arc::new(Control::default());
        let governor = governor(if select { 512 * 1024 * 1024 } else { 1 });
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let mut owner = Owner::start(
            Arc::clone(&control),
            &store,
            &catalog,
            Arc::new(Mutex::new(ReaderPins::default())),
            &config,
            LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
            Some(governor.clone()),
        )
        .unwrap()
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let state = control.lock().unwrap();
            if if select {
                state.selected.is_some()
            } else {
                state.report.deferred_attempts > 0
            } {
                if select {
                    assert_eq!(state.report.completed_checkpoints, 1);
                    assert!(governor.snapshot().admitted_memory_bytes > 0);
                } else {
                    assert!(state.latest.is_some());
                }
                break;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "owner did not reach the shutdown boundary"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        owner.stop();
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let state = control.lock().unwrap();
        assert!(state.latest.is_none());
        assert!(state.selected.is_none());
        assert!(state.retired.is_none());
        assert!(state.task.is_none());
        drop(state);
        if select {
            // The discarded handoff already selected disk authority. An old
            // frontend must never be allowed to append to its previous WAL.
            assert!(control.ensure_healthy().is_err());
        }
        drop(store);
        // Keeping the observer/control alive must not retain the open lock or
        // require writable recovery before a read-only open can see the data.
        let readonly = GraphStore::open_read_only_with_durability(
            &fixture.0,
            &mut catalog,
            crate::DurabilityPolicy::default(),
            hawdb_storage::config::RecoveryMode::Strict,
        )
        .unwrap();
        assert_eq!(readonly.commit_epoch(), 1);
        assert_eq!(readonly.node_count_for_label(None), 1);
        assert_eq!(
            readonly
                .node_owned(hawdb_storage::NodeId(0))
                .unwrap()
                .unwrap()
                .properties
                .get("id"),
            Some(&crate::Value::Int(41))
        );
        drop(readonly);
        let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 1);
        assert_eq!(recovered.node_count_for_label(None), 1);
        drop(recovered);
        drop(owner);
        drop(control);
    }

    #[test]
    fn plain_database_idle_age_checkpoints_without_a_host_loop() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        db.query("CREATE (:Memory {id: 'automatic-one'})").unwrap();
        let first_epoch = db.commit_epoch().unwrap();
        wait_for(&db, |report| report.completed_checkpoints >= 1);
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            1
        );
        assert!(
            db.storage_reclamation_watermark()
                .unwrap()
                .checkpoint_commit_epoch
                .unwrap()
                >= first_epoch
        );
        db.query("CREATE (:Memory {id: 'automatic-two'})").unwrap();
        wait_for(&db, |report| report.completed_checkpoints >= 2);
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            2
        );
        drop(db);
        let mut reopened = Database::open(&fixture.0).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            2
        );
    }

    #[test]
    fn manual_checkpoint_adopts_a_selected_job_and_resumes_automatic_work() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        db.query("CREATE (:Memory {id: 'before-manual'})").unwrap();
        wait_for(&db, |report| report.completed_checkpoints >= 1);
        db.checkpoint().unwrap();
        assert!(
            !db.automatic_checkpoint_report()
                .unwrap()
                .unwrap()
                .waiting_for_handoff
        );
        db.query("CREATE (:Memory {id: 'after-manual'})").unwrap();
        wait_for(&db, |report| report.completed_checkpoints >= 2);
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            2
        );
    }

    #[test]
    fn denied_governor_recovers_without_another_write_or_host_checkpoint() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        db.set_runtime_governor(governor(1));
        db.query("CREATE (:Memory {id: 'admission-retry'})")
            .unwrap();
        wait_for(&db, |report| report.deferred_attempts > 0);
        assert_eq!(
            db.automatic_checkpoint_report()
                .unwrap()
                .unwrap()
                .completed_checkpoints,
            0
        );
        let restored = governor(512 * 1024 * 1024);
        db.set_runtime_governor(restored.clone());
        wait_for(&db, |report| report.completed_checkpoints >= 1);
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            1
        );
        let suspension = db.runtime.suspend_automatic_checkpoint().unwrap();
        assert_eq!(restored.snapshot().admitted_memory_bytes, 0);
        drop(suspension);
    }

    #[test]
    fn branch_reclamation_releases_parked_source_and_rearms_age_without_a_write() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        db.set_runtime_governor(governor(1));
        db.query("CREATE (:Memory {id: 'reclamation-age'})")
            .unwrap();
        wait_for(&db, |report| report.deferred_attempts > 0);
        let report = db
            .reclaim_branch_storage(crate::BranchReclamationLimits::default())
            .unwrap();
        assert!(!report.deferred_for_active_leases);
        // Wake the existing owner without a mutable frontend access that
        // could accidentally repair a missing post-reclamation source.
        db.runtime
            .set_automatic_governor(governor(512 * 1024 * 1024))
            .unwrap();
        wait_for(&db, |report| report.completed_checkpoints >= 1);
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows,
            vec![std::collections::BTreeMap::from([(
                "n.id".into(),
                crate::Value::String("reclamation-age".into()),
            )])]
        );
    }

    #[test]
    fn delta_pressure_uses_the_exact_soft_boundary_without_wal_or_age_pressure() {
        for (text_bytes, expected) in [(34, false), (35, true), (36, true)] {
            let fixture = Fixture::new();
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &fixture.0,
                &mut catalog,
                hawdb_storage::config::DurabilityPolicy::default(),
                hawdb_storage::config::WalReplayConfig {
                    residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
                    max_out_of_core_delta_bytes: Some(10_000),
                    ..hawdb_storage::config::WalReplayConfig::default()
                },
            )
            .unwrap();
            store
                .create_node(&mut catalog, "Memory", std::collections::BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            for _ in 0..192 {
                store
                    .create_node(&mut catalog, "Memory", std::collections::BTreeMap::new())
                    .unwrap();
            }
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    std::collections::BTreeMap::from([(
                        "p".into(),
                        crate::Value::String("x".repeat(text_bytes)),
                    )]),
                )
                .unwrap();
            let debt = store.checkpoint_debt_snapshot().unwrap();
            assert_eq!(debt.delta_bytes, 6965 + text_bytes as u64);
            assert_eq!(debt.max_delta_bytes, Some(10_000));
            assert!(u128::from(debt.wal_bytes) * 10 < u128::from(debt.max_wal_bytes.unwrap()) * 7);
            assert_eq!(
                due(
                    &Source::capture(&store, &catalog),
                    Duration::from_secs(3600)
                ),
                expected
            );
        }
    }

    #[test]
    fn delta_pressure_sustains_generations_and_preserves_all_values_after_reopen() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_secs(3600),
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(16 * 1024),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        // Establish the initial canonical base before the sustained workload.
        db.checkpoint().unwrap();
        for id in 0..320 {
            db.query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &std::collections::BTreeMap::from([
                    ("id".into(), crate::Value::Int(id)),
                    ("body".into(), crate::Value::String("b".repeat(512))),
                ]),
            )
            .unwrap();
        }
        let report = db.automatic_checkpoint_report().unwrap().unwrap();
        assert!(
            report.completed_checkpoints >= 2,
            "delta pressure must rotate generations: {report:?}"
        );
        let expected = db
            .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows;
        assert_eq!(expected.len(), 320);
        for (id, row) in expected.iter().enumerate() {
            assert_eq!(row.get("id"), Some(&crate::Value::Int(id as i64)));
            assert_eq!(
                row.get("body"),
                Some(&crate::Value::String("b".repeat(512)))
            );
        }
        drop(db);
        let mut reopened = Database::open(&fixture.0).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
                .unwrap()
                .rows,
            expected
        );
    }

    #[test]
    fn soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_secs(3600),
            max_wal_replay_bytes: Some(64 * 1024),
            max_wal_record_bytes: Some(4 * 1024),
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&fixture.0, config).unwrap();
        for id in 0..320 {
            let parameters = std::collections::BTreeMap::from([
                ("id".into(), crate::Value::Int(id)),
                ("body".into(), crate::Value::String("b".repeat(512))),
            ]);
            db.query_with_params("CREATE (:Memory {id: $id, body: $body})", &parameters)
                .unwrap();
        }
        let report = db.automatic_checkpoint_report().unwrap().unwrap();
        assert!(
            report.completed_checkpoints >= 2,
            "byte threshold must rotate WAL: {report:?}"
        );
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            320
        );
        drop(db);
        let mut reopened = Database::open(&fixture.0).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            320
        );
    }

    #[test]
    fn concurrent_database_uses_the_same_idle_owner_and_keeps_old_reads() {
        let fixture = Fixture::new();
        let config = DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        };
        let db = Database::open_with_config(&fixture.0, config)
            .unwrap()
            .into_concurrent();
        db.query("CREATE (:Memory {id: 'concurrent-one'})").unwrap();
        let mut old = db.begin_read_transaction().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while db
            .automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .completed_checkpoints
            == 0
        {
            assert!(
                Instant::now() < deadline,
                "concurrent idle owner must progress"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        db.query("CREATE (:Memory {id: 'concurrent-two'})").unwrap();
        assert_eq!(
            old.query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            db.query("MATCH (n:Memory) RETURN n.id").unwrap().rows.len(),
            2
        );
        drop(old);
        drop(db);
        let mut reopened = Database::open(&fixture.0).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            2
        );
    }

    #[test]
    fn native_tokio_idle_checkpoint_uses_the_host_governor() {
        let fixture = Fixture::new();
        let options =
            crate::HawDBEmbeddedOpenOptions::new(&fixture.0).with_config(DatabaseConfig {
                automatic_checkpoint_max_age: Duration::from_millis(20),
                ..DatabaseConfig::default()
            });
        let embedded = crate::HawDBTokioEmbedded::open_owned(options).unwrap();
        embedded
            .runtime()
            .block_on(embedded.query(
                "CREATE (:Memory {id: 'tokio-idle'})",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let report = embedded
                .with_embedded(|host| host.database().automatic_checkpoint_report())
                .unwrap()
                .unwrap();
            if report.completed_checkpoints > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "Tokio host owner must progress: {report:?}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let output = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (n:Memory) RETURN n.id",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        drop(embedded);
        let mut reopened = Database::open(&fixture.0).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn in_memory_and_read_only_handles_create_no_worker() {
        assert!(Database::new()
            .automatic_checkpoint_report()
            .unwrap()
            .is_none());
        let fixture = Fixture::new();
        let mut db = Database::open(&fixture.0).unwrap();
        db.query("CREATE (:Memory {id: 'read-only'})").unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let mut read_only = Database::open_with_config(
            &fixture.0,
            DatabaseConfig {
                read_only: true,
                automatic_checkpoint_max_age: Duration::ZERO,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            read_only
                .query("MATCH (n:Memory) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(read_only.automatic_checkpoint_report().unwrap().is_none());
    }

    mod headroom;
    mod manual;
    mod resume;
}
