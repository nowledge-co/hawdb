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
    LocalQosPermit, RuntimeGovernor, RuntimePermit, RuntimeWorkRequest, WorkClass, WorkRequest,
};
use hawdb_storage::store::{CheckpointCandidate, CheckpointDebtSnapshot, CheckpointSourceIdentity};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutomaticCheckpointReport {
    pub preparing: bool,
    pub waiting_for_handoff: bool,
    pub completed_checkpoints: u64,
    pub deferred_attempts: u64,
    pub failed_attempts: u64,
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

#[derive(Debug)]
pub(super) struct Source {
    store: GraphStore,
    catalog: Catalog,
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
    Handoff,
    Retiring,
}

#[derive(Debug)]
struct Admission {
    runtime: RuntimePermit,
    local: LocalQosPermit,
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
    last_identity: Option<CheckpointSourceIdentity>,
    sync_group_active: bool,
    debt: Option<CheckpointDebtSnapshot>,
    selected: Option<Selected>,
    retired: Option<Retired>,
    task: Option<RuntimeTaskContext>,
    governor: Option<RuntimeGovernor>,
    report: AutomaticCheckpointReport,
    #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
    prefix_seal_probe: Option<Arc<PrefixSealProbe>>,
}

#[derive(Debug, Default)]
pub(super) struct Control {
    state: Mutex<State>,
    changed: Condvar,
    failed: AtomicBool,
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
        while state.phase == Phase::Finalizing
            || (state.phase == Phase::Draining && !state.sync_group_active)
            || (state.phase == Phase::Preparing
                && !state.sync_group_active
                && needs_headroom(&state))
        {
            state = self.changed.wait(state).map_err(|_| Self::poisoned())?;
        }
        self.ensure_healthy()?;
        Ok(state)
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state.lock().map_err(|_| Self::poisoned())
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
        let identity = store.checkpoint_source_identity();
        let retired = if identity != state.last_identity || finished_group {
            let retired = state.latest.replace(Source::capture(store, catalog));
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
        let old = match store.adopt_selected_checkpoint(selected.store, selected.expected) {
            Ok(old) => old,
            Err(error) => {
                state.phase = Phase::Idle;
                state.stopping = true;
                state.report.waiting_for_handoff = false;
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

    pub(super) fn suspend(self: &Arc<Self>) -> Result<Suspension> {
        let mut state = self.lock()?;
        state.suspensions = state.suspensions.checked_add(1).ok_or_else(|| {
            HawDBError::StorageIntegrity("checkpoint suspension count overflow".into())
        })?;
        if let Some(task) = &state.task {
            task.cancellation().cancel();
        }
        self.changed.notify_all();
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
        let (mut source, governor, task) = {
            let mut state = control
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            loop {
                if let Some(retired) = state.retired.take() {
                    drop(state);
                    retire(retired, &pins);
                    state = control
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.phase = Phase::Idle;
                    state.task = None;
                    control.changed.notify_all();
                }
                if state.stopping {
                    return;
                }
                let due = state
                    .latest
                    .as_ref()
                    .is_some_and(|source| due(source, max_age));
                if state.phase == Phase::Idle
                    && state.suspensions == 0
                    && !state.sync_group_active
                    && due
                {
                    state.phase = Phase::Preparing;
                    state.report.preparing = true;
                    let task = RuntimeTaskContext::default();
                    state.task = Some(task.clone());
                    break (
                        state.latest.take().expect("due source exists"),
                        state
                            .governor
                            .clone()
                            .expect("enabled owner has a governor"),
                        task,
                    );
                }
                let (next, _) = control
                    .changed
                    .wait_timeout(state, RETRY_DELAY)
                    .unwrap_or_else(|error| error.into_inner());
                state = next;
            }
        };
        let attempt = prepare(&source, &scheduler, &governor, &task);
        let (mut candidate, admission) = match attempt {
            Ok(Some(work)) => work,
            result => {
                let mut state = control
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if result.is_err() {
                    state.report.deferred_attempts += 1;
                }
                if state.latest.is_none() {
                    state.latest = Some(source);
                }
                state.phase = Phase::Idle;
                state.task = None;
                state.report.preparing = false;
                control.changed.notify_all();
                let (state, _) = control
                    .changed
                    .wait_timeout(state, RETRY_DELAY)
                    .unwrap_or_else(|error| error.into_inner());
                drop(state);
                continue;
            }
        };
        let admitted_task = admission.runtime.bind_task_context(task.clone());
        // Seal and mount each captured prefix while writes remain admitted.
        // A writer that advances during sealing supplies another suffix for
        // this same candidate; it never causes a database-sized base restart.
        let mut expected = None;
        let result = (|| -> Result<()> {
            loop {
                let latest = {
                    let mut state = control.lock()?;
                    if state.stopping || state.suspensions != 0 {
                        return Err(HawDBError::Storage(
                            "checkpoint candidate cancelled before selection".into(),
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
                candidate.catch_up_with_task_context(&source.store, &admitted_task)?;
                candidate.finish_catch_up()?;
                #[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
                {
                    let probe = control.lock()?.prefix_seal_probe.clone();
                    if let Some(probe) = probe {
                        probe.observe(candidate.commit_epoch())?;
                    }
                }
                task.checkpoint().map_err(|reason| {
                    HawDBError::Execution(format!("checkpoint prefix sealing stopped: {reason}"))
                })?;
                // Admission waits occur before the writer barrier. The final
                // barrier only compares a sealed identity and publishes it.
                let _publication_wave = admitted_task
                    .acquire_io_wave(std::num::NonZeroUsize::MIN)
                    .map_err(|reason| {
                        HawDBError::Execution(format!("checkpoint selector I/O stopped: {reason}"))
                    })?;
                let mut state = control.lock()?;
                state.phase = Phase::Draining;
                if state.sync_group_active {
                    // A foreground flush may need the same I/O pool. Never
                    // retain its capacity while waiting for that flush.
                    drop(_publication_wave);
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
                    state.phase = Phase::Preparing;
                    control.changed.notify_all();
                    drop(state);
                    continue;
                }
                if state.stopping || state.suspensions != 0 || task.checkpoint().is_err() {
                    return Err(HawDBError::Storage(
                        "checkpoint cancelled before selector publication".into(),
                    ));
                }
                if state.latest.as_ref().is_some_and(|latest| {
                    latest.store.checkpoint_source_identity()
                        != source.store.checkpoint_source_identity()
                }) {
                    let latest = state.latest.take().expect("advanced source exists");
                    state.phase = Phase::Preparing;
                    control.changed.notify_all();
                    drop(state);
                    drop(_publication_wave);
                    source = latest;
                    continue;
                }
                state.phase = Phase::Finalizing;
                let oldest = pins.lock().map_err(|_| Control::poisoned())?.oldest_epoch();
                expected = source.store.checkpoint_source_identity();
                let result = source
                    .store
                    .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, oldest);
                drop(state);
                return result.map(|_| ());
            }
        })();
        if result.is_ok() {
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
            control.changed.notify_all();
        } else {
            // Cleanup and COW destruction precede releasing the manual owner.
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

fn due(source: &Source, max_age: Duration) -> bool {
    source.store.checkpoint_debt_snapshot().is_some_and(|debt| {
        if debt.read_only || debt.commit_epoch <= debt.checkpoint_commit_epoch {
            return false;
        }
        [
            (debt.wal_bytes, debt.max_wal_bytes),
            (debt.delta_bytes, debt.max_delta_bytes),
        ]
        .into_iter()
        .any(|(bytes, limit)| {
            limit.is_some_and(|limit| {
                u128::from(bytes) * 1_000_000
                    >= u128::from(limit)
                        * u128::from(
                            hawdb_storage::pressure::STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION,
                        )
            })
        }) || u128::from(debt.wal_age_millis) >= max_age.as_millis()
    })
}

fn prepare(
    source: &Source,
    scheduler: &LocalQosScheduler,
    governor: &RuntimeGovernor,
    task: &RuntimeTaskContext,
) -> Result<Option<(CheckpointCandidate, Admission)>> {
    task.checkpoint()
        .map_err(|reason| HawDBError::Storage(reason.to_string()))?;
    let local = scheduler
        .try_start(WorkRequest::background(
            WorkClass::Mutation,
            source.store.checkpoint_estimated_operations(),
        ))
        .map_err(|reason| {
            HawDBError::Storage(format!("automatic checkpoint QoS deferred: {reason:?}"))
        })?;
    let memory = source.store.checkpoint_candidate_admission_bytes()?;
    let runtime = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(memory).with_io_wave_slots(1))
        .map_err(|reason| {
            HawDBError::Storage(format!("automatic checkpoint admission deferred: {reason}"))
        })?;
    // Retain the whole-job QoS permit until all builders have bounded units.
    // The canonical/descriptor builders can already consume this admitted task
    // for cancellation and I/O without reacquiring CPU or memory admission.
    let work = hawdb_storage::background::CheckpointWorkContext::new(
        runtime.bind_task_context(task.clone()),
    );
    let candidate = source
        .store
        .prepare_checkpoint_candidate_with_work_context(&source.catalog, &work)?;
    task.checkpoint()
        .map_err(|reason| HawDBError::Storage(reason.to_string()))?;
    Ok(candidate.map(|candidate| (candidate, Admission { runtime, local })))
}

fn retire(retired: Retired, pins: &Mutex<ReaderPins>) {
    let Retired {
        old,
        mut selected,
        candidate,
        admission,
    } = retired;
    drop(old);
    let pinned = pins
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .pinned_physical_generations();
    let _ = candidate.reclaim_published_generations(&mut selected.store, &pinned);
    drop(candidate);
    drop(selected);
    let Admission { runtime, local } = admission;
    drop(runtime);
    local.finish_with_outcome(true);
}

#[cfg(all(test, feature = "background-maintenance", not(target_arch = "wasm32")))]
mod tests {
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
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        drop(owner);
        drop(control);
        drop(store);
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
}
