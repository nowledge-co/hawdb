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

//! Cooperative admission for actual bounded checkpoint builder operations.

use hawdb_core::time::Instant;
use hawdb_core::{
    RuntimeCancellationReason, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeMemoryError,
    RuntimeMemoryPermit, RuntimeTaskContext,
};
use hawdb_qos::{
    LocalQosBatchPermit, LocalQosScheduler, QosAdmission, QosTelemetrySink, QosUnitEvent,
    QosUnitPhase, WorkClass, WorkRequest,
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod buffer;
pub(crate) use buffer::CheckpointBytes;
pub use buffer::CheckpointSharedBytes;

mod values;
pub(crate) use values::{CheckpointSharedValues, CheckpointValues};

mod allocation;
pub(crate) use allocation::{
    CheckpointAllocationLookup, CheckpointAllocationOwner, CheckpointAllocationToken,
};

/// Maximum number of bounded builder units charged to one local QoS
/// admission (#1616). Each unit is already bounded by its builder's record,
/// element or 64 KiB block limit, so one batch is bounded by this many units.
pub const CHECKPOINT_UNITS_PER_ADMISSION: usize = 64;

/// The task already admitted by the owner, optionally with batched local QoS.
/// This never creates a governor or reserves the owner's memory a second time.
/// Builders must keep each unit bounded by their record/page limits. One unit
/// must never stand for a complete dataset build or an unbounded merge loop.
///
/// With a scheduler, one one-operation background batch permit covers at most
/// [`CHECKPOINT_UNITS_PER_ADMISSION`] consecutive units of this context and
/// its clones (#1616). Cancellation is still observed at every unit; the
/// deadline, scheduler admission and the clock are consulted once per batch.
/// The batch charges the scheduler only while a unit runs: between units it
/// is idle, invisible to the scheduler state and reclaimed by any competing
/// background admission it would otherwise refuse. A batch also ends at the
/// end of [`Self::classify`], on [`Self::release_unit_batch`], when a unit is
/// abandoned, and when the last clone is dropped.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct CheckpointWorkContext {
    task: RuntimeTaskContext,
    scheduler: Option<LocalQosScheduler>,
    batch: Option<Arc<UnitBatch>>,
    recorded_failure: Option<Arc<Mutex<Option<CheckpointWorkError>>>>,
    progress: Option<Arc<AtomicU64>>,
}

/// Counters of the batched local admission shared by a context and its clones.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointUnitBatchStats {
    /// Units started (admitted to a batch), including later abandoned ones.
    pub units: u64,
    /// Units that ended successfully.
    pub finished_units: u64,
    /// Units dropped without finishing.
    pub abandoned_units: u64,
    /// Scheduler admission attempts, including refused ones.
    pub admissions: u64,
    /// Scheduler refusals.
    pub refusals: u64,
    /// Monotonic clock reads taken by batch boundaries.
    pub clock_reads: u64,
}

#[derive(Debug)]
struct UnitBatch {
    scheduler: LocalQosScheduler,
    state: Mutex<UnitBatchState>,
}

#[derive(Debug, Default)]
struct UnitBatchState {
    permit: Option<LocalQosBatchPermit>,
    /// Unit observer sampled at admission (see `QosTelemetrySink::observes_units`).
    observer: Option<Arc<dyn QosTelemetrySink>>,
    /// Units started under the current permit.
    started: usize,
    /// Units started and not yet ended.
    active: usize,
    /// A unit in this batch was dropped unfinished.
    failed: bool,
    /// Release the permit as soon as no unit is active.
    closing: bool,
    stats: CheckpointUnitBatchStats,
}

/// A unit admitted to a batch by [`UnitBatch::start`].
struct StartedUnit {
    observer: Option<Arc<dyn QosTelemetrySink>>,
    /// One-based position in its batch.
    unit_in_batch: usize,
    /// This unit admitted a new batch.
    admitted: bool,
}

impl UnitBatch {
    fn new(scheduler: LocalQosScheduler) -> Self {
        Self {
            scheduler,
            state: Mutex::new(UnitBatchState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, UnitBatchState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Start one unit, admitting a new batch first when none is held or the
    /// held one is exhausted. Exactly one clock read serves the previous
    /// batch's completion time, the deadline and the new admission time.
    ///
    /// Units of one context (and its clones) run sequentially or nested:
    /// they share this batch's single background slot. Units of clones run
    /// in parallel would all run under that one slot, so the scheduler
    /// would account them as a single background operation.
    fn start(&self, task: &RuntimeTaskContext) -> Result<StartedUnit, CheckpointWorkError> {
        let mut state = self.lock();
        // A running batch cannot be reclaimed: nested units join it.
        let resumed = state.active > 0
            || (state.started < CHECKPOINT_UNITS_PER_ADMISSION
                && !state.closing
                && state
                    .permit
                    .as_ref()
                    .is_some_and(LocalQosBatchPermit::resume));
        if !resumed {
            let now = Instant::now();
            state.stats.clock_reads += 1;
            if let Some(permit) = state.permit.take() {
                permit.finish_with_outcome_at(!state.failed, now);
            }
            state.started = 0;
            state.failed = false;
            state.closing = false;
            state.observer = None;
            task.check_deadline_at(now)
                .map_err(CheckpointWorkError::Stopped)?;
            state.stats.admissions += 1;
            match self
                .scheduler
                .try_start_batch_at(WorkRequest::background(WorkClass::Mutation, 1), now)
            {
                Ok(permit) => state.permit = Some(permit),
                Err(admission) => {
                    state.stats.refusals += 1;
                    return Err(CheckpointWorkError::Admission(admission));
                }
            }
            state.observer = self
                .scheduler
                .telemetry_sink()
                .filter(|sink| sink.observes_units());
        }
        state.started += 1;
        state.active += 1;
        state.stats.units += 1;
        Ok(StartedUnit {
            observer: state.observer.clone(),
            unit_in_batch: state.started,
            admitted: !resumed,
        })
    }

    fn end(&self, success: bool) {
        let mut state = self.lock();
        state.active -= 1;
        if success {
            state.stats.finished_units += 1;
        } else {
            state.stats.abandoned_units += 1;
            state.failed = true;
        }
        if state.active == 0 {
            if state.failed || state.closing {
                Self::release_locked(&mut state);
            } else if let Some(permit) = &state.permit {
                permit.idle();
            }
        }
    }

    /// Release the held permit now if idle, otherwise when the last active
    /// unit ends. A later unit admits a fresh batch.
    fn close(&self, failed: bool) {
        let mut state = self.lock();
        state.failed |= failed;
        if state.active == 0 {
            Self::release_locked(&mut state);
        } else {
            state.closing = true;
        }
    }

    fn release_locked(state: &mut UnitBatchState) {
        if let Some(permit) = state.permit.take() {
            if !permit.is_reclaimed() {
                state.stats.clock_reads += 1;
            }
            permit.finish_with_outcome(!state.failed);
        }
        state.started = 0;
        state.failed = false;
        state.closing = false;
        state.observer = None;
    }
}

impl Drop for UnitBatch {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(permit) = state.permit.take() {
            permit.finish_with_outcome(!state.failed);
        }
    }
}

impl CheckpointWorkContext {
    pub(crate) fn task_context(&self) -> &RuntimeTaskContext {
        &self.task
    }

    pub fn new(task: RuntimeTaskContext) -> Self {
        Self {
            task,
            scheduler: None,
            batch: None,
            recorded_failure: None,
            progress: None,
        }
    }

    pub fn with_scheduler(mut self, scheduler: LocalQosScheduler) -> Self {
        self.batch = Some(Arc::new(UnitBatch::new(scheduler.clone())));
        self.scheduler = Some(scheduler);
        self
    }

    /// End the current local admission batch. The permit is released now
    /// when no unit is active, otherwise when the last active unit ends.
    /// Owners call this before they wait or park outside builder work.
    pub fn release_unit_batch(&self) {
        if let Some(batch) = &self.batch {
            batch.close(false);
        }
    }

    /// Admission counters shared by this context and its clones.
    pub fn unit_batch_stats(&self) -> CheckpointUnitBatchStats {
        self.batch
            .as_ref()
            .map(|batch| batch.lock().stats)
            .unwrap_or_default()
    }

    /// Observes completed builder units without counting failed admission or
    /// classification alone as progress. This does not reserve any resources.
    pub fn with_progress_counter(mut self, progress: Arc<AtomicU64>) -> Self {
        self.progress = Some(progress);
        self
    }

    pub(crate) fn scheduler(&self) -> Option<LocalQosScheduler> {
        self.scheduler.clone()
    }

    pub(crate) fn memory_ceiling(&self) -> u64 {
        self.task
            .memory_reservation()
            .map_or(u64::MAX, |reservation| reservation.memory_bytes())
    }

    pub fn checkpoint(&self) -> Result<(), CheckpointWorkError> {
        self.task
            .checkpoint()
            .map_err(CheckpointWorkError::Stopped)
            .map_err(|error| self.record_failure(error))
    }

    /// Preserve typed work failures through an existing codec's string errors.
    /// Ordinary operation errors retain their original diagnostic and class.
    #[doc(hidden)]
    pub fn classify<T, E>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, CheckpointOperationError<E>> {
        let unit = self.start_unit().map_err(CheckpointOperationError::Work)?;
        let recorded = Arc::new(Mutex::new(None));
        let mut scoped = self.clone();
        scoped.recorded_failure = Some(recorded.clone());
        unit.finish_without_progress();
        let result = operation(&scoped);
        // A classified operation is the owner's admission scope: never let
        // its batch outlive it, whatever clones the operation retained.
        self.release_unit_batch();
        let failure = recorded
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match failure {
            Some(error) => Err(CheckpointOperationError::Work(self.record_failure(error))),
            None => result.map_err(CheckpointOperationError::Operation),
        }
    }

    pub(crate) fn record_failure(&self, error: CheckpointWorkError) -> CheckpointWorkError {
        if let Some(recorded) = &self.recorded_failure {
            let mut first = recorded
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if first.is_none() {
                *first = Some(error.clone());
            }
        }
        error
    }

    pub(crate) fn start_unit(&self) -> Result<CheckpointWorkUnit, CheckpointWorkError> {
        let Some(batch) = &self.batch else {
            self.checkpoint()?;
            return Ok(CheckpointWorkUnit {
                batch: None,
                progress: self.progress.clone(),
                observer: None,
            });
        };
        if let Err(reason) = self.task.check_cancellation() {
            // Do not keep an idle permit for a stopped task. Its completed
            // units succeeded; an active unit's drop marks the batch failed.
            batch.close(false);
            return Err(self.record_failure(CheckpointWorkError::Stopped(reason)));
        }
        let StartedUnit {
            observer,
            unit_in_batch,
            admitted,
        } = match batch.start(&self.task) {
            Ok(started) => started,
            Err(error) => return Err(self.record_failure(error)),
        };
        let unit = CheckpointWorkUnit {
            batch: Some(batch.clone()),
            progress: self.progress.clone(),
            observer: observer.map(|observer| (observer, unit_in_batch)),
        };
        if let Some((observer, unit_in_batch)) = &unit.observer {
            observer.record_qos_unit(QosUnitEvent {
                phase: QosUnitPhase::Started,
                class: WorkClass::Mutation,
                unit_in_batch: *unit_in_batch,
            });
        }
        // An admission or unit telemetry callback can cancel while the unit
        // is being started. Dropping the unit marks the batch failed.
        if admitted || unit.observer.is_some() {
            self.task
                .check_cancellation()
                .map_err(CheckpointWorkError::Stopped)
                .map_err(|error| self.record_failure(error))?;
        }
        Ok(unit)
    }

    /// Ordinary iterator callbacks keep their original consumer admission.
    /// Controlled readers own real fetch units/waves; consumers admit only
    /// after fetching so one-unit/one-wave reservations remain usable.
    pub(crate) fn next_input<I: Iterator>(
        &self,
        input: &mut I,
        source_admits: bool,
    ) -> Result<(Option<I::Item>, CheckpointWorkUnit), CheckpointWorkError> {
        self.checkpoint()?;
        if source_admits {
            let item = input.next();
            let unit = self.start_unit()?;
            Ok((item, unit))
        } else {
            let unit = self.start_unit()?;
            let item = {
                let _wave = self.io_wave()?;
                input.next()
            };
            Ok((item, unit))
        }
    }

    pub(crate) fn io_wave(
        &self,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, CheckpointWorkError> {
        self.task
            .acquire_io_wave(NonZeroUsize::MIN)
            .map_err(CheckpointWorkError::Io)
            .map_err(|error| self.record_failure(error))
    }

    pub(crate) fn reserve_memory(
        &self,
        bytes: usize,
    ) -> Result<Option<Box<dyn RuntimeMemoryPermit>>, CheckpointWorkError> {
        let bytes = u64::try_from(bytes).map_err(|_| {
            CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded {
                requested_bytes: u64::MAX,
                available_bytes: 0,
            })
        })?;
        self.task
            .reserve_working_memory(bytes)
            .map_err(CheckpointWorkError::Memory)
            .map_err(|error| self.record_failure(error))
    }

    /// Hash a borrowed metadata buffer in bounded units without copying it.
    pub(crate) fn integrity(
        &self,
        bytes: &[u8],
    ) -> Result<hawdb_integrity::IntegrityDigest, CheckpointWorkError> {
        let mut hasher = hawdb_integrity::IntegrityHasher::new();
        for block in bytes.chunks(64 * 1024) {
            let unit = self.start_unit()?;
            hasher.update(block);
            unit.finish();
        }
        self.checkpoint()?;
        Ok(hasher.finish())
    }

    /// Existing text envelopes use CRC32C, without a SHA-256 field.
    pub(crate) fn checksum(&self, bytes: &[u8]) -> Result<u64, CheckpointWorkError> {
        let mut hasher = hawdb_integrity::Crc32cHasher::new();
        for block in bytes.chunks(64 * 1024) {
            let unit = self.start_unit()?;
            hasher.update(block);
            unit.finish();
        }
        self.checkpoint()?;
        Ok(hasher.finish())
    }

    /// Copy into immutable shared ownership, retaining admitted exact byte
    /// capacity and the shared cell through the last clone. Initialization
    /// uses bounded units and does not detach the allocation from its lease.
    pub(crate) fn arc_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<CheckpointSharedBytes, CheckpointWorkError> {
        CheckpointSharedBytes::copy(bytes, self)
    }
}

/// One bounded builder unit. Finishing it counts progress; dropping it
/// unfinished marks its admission batch failed and releases that batch as
/// soon as no other unit is active.
pub(crate) struct CheckpointWorkUnit {
    batch: Option<Arc<UnitBatch>>,
    progress: Option<Arc<AtomicU64>>,
    observer: Option<(Arc<dyn QosTelemetrySink>, usize)>,
}

impl CheckpointWorkUnit {
    pub(crate) fn finish(mut self) {
        if let Some(progress) = &self.progress {
            progress.fetch_add(1, Ordering::Relaxed);
        }
        self.end(true);
    }

    fn finish_without_progress(mut self) {
        self.end(true);
    }

    fn end(&mut self, success: bool) {
        let Some(batch) = self.batch.take() else {
            return;
        };
        batch.end(success);
        if let Some((observer, unit_in_batch)) = self.observer.take() {
            observer.record_qos_unit(QosUnitEvent {
                phase: if success {
                    QosUnitPhase::Finished
                } else {
                    QosUnitPhase::Abandoned
                },
                class: WorkClass::Mutation,
                unit_in_batch,
            });
        }
    }
}

impl Drop for CheckpointWorkUnit {
    fn drop(&mut self) {
        self.end(false);
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointWorkError {
    Stopped(RuntimeCancellationReason),
    Admission(QosAdmission),
    Io(RuntimeIoWaveError),
    Memory(RuntimeMemoryError),
    FileDescriptors(hawdb_core::error::FileDescriptorError),
    Contended(&'static str),
    Allocation { bytes: u64, reason: String },
}

#[doc(hidden)]
#[derive(Debug)]
pub enum CheckpointOperationError<E> {
    Work(CheckpointWorkError),
    Operation(E),
}

impl<E> From<E> for CheckpointOperationError<E> {
    fn from(error: E) -> Self {
        Self::Operation(error)
    }
}

impl Display for CheckpointWorkError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped(reason) => write!(formatter, "checkpoint build stopped: {reason}"),
            Self::Admission(reason) => {
                write!(formatter, "checkpoint unit admission deferred: {reason:?}")
            }
            Self::Io(error) => write!(formatter, "checkpoint build I/O stopped: {error}"),
            Self::Memory(error) => {
                write!(formatter, "checkpoint memory admission deferred: {error}")
            }
            Self::FileDescriptors(error) => {
                write!(
                    formatter,
                    "checkpoint descriptor admission deferred: {error}"
                )
            }
            Self::Contended(resource) => {
                write!(formatter, "checkpoint resource is busy: {resource}")
            }
            Self::Allocation { bytes, reason } => write!(
                formatter,
                "checkpoint allocation of {bytes} bytes failed: {reason}"
            ),
        }
    }
}

impl Error for CheckpointWorkError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            Self::Io(error) => Some(error),
            Self::Admission(_) => None,
            Self::Memory(error) => Some(error),
            Self::FileDescriptors(error) => Some(error),
            Self::Contended(_) => None,
            Self::Allocation { .. } => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hawdb_core::{RuntimeCancellationToken, RuntimeIoWaveController};
    use hawdb_qos::{QosTelemetryEvent, QosTelemetryOutcome, QosTelemetryPhase, QosTelemetrySink};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn progress_counts_completed_work_but_not_classification_denial_or_abandonment() {
        let progress = Arc::new(AtomicU64::new(0));
        let scheduler = LocalQosScheduler::new(hawdb_qos::LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..Default::default()
        });
        let work = CheckpointWorkContext::default()
            .with_scheduler(scheduler.clone())
            .with_progress_counter(progress.clone());
        work.classify(|_| Ok::<_, ()>(())).unwrap();
        assert_eq!(progress.load(Ordering::Relaxed), 0);
        drop(work.start_unit().unwrap());
        assert_eq!(progress.load(Ordering::Relaxed), 0);
        let occupied = scheduler
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        assert!(matches!(
            work.start_unit(),
            Err(CheckpointWorkError::Admission(_))
        ));
        assert_eq!(progress.load(Ordering::Relaxed), 0);
        drop(occupied);
        work.start_unit().unwrap().finish();
        assert_eq!(progress.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&progress), 2);
        drop(work);
        assert_eq!(Arc::strong_count(&progress), 1);
    }

    /// Observes both batched admissions (`active_units`, `peak_units`,
    /// `admissions`) and the individual units they cover (`started`,
    /// `completed`). `completed` counts every ended unit, as the former
    /// per-unit permit completion did, and drives `cancel_after`.
    #[derive(Debug, Default)]
    pub(crate) struct CheckpointWorkProbe {
        pub(crate) cancellation: RuntimeCancellationToken,
        pub(crate) completed: AtomicUsize,
        pub(crate) started: AtomicUsize,
        pub(crate) admissions: AtomicUsize,
        pub(crate) active_units: AtomicUsize,
        pub(crate) peak_units: AtomicUsize,
        pub(crate) io_waves: AtomicUsize,
        pub(crate) active_io: Arc<AtomicUsize>,
        pub(crate) cancel_after: AtomicUsize,
        pub(crate) cancel_on_io_wave: AtomicUsize,
    }

    impl CheckpointWorkProbe {
        pub(crate) fn context(
            self: &Arc<Self>,
            scheduler: LocalQosScheduler,
        ) -> CheckpointWorkContext {
            scheduler.set_telemetry_sink(Some(self.clone()));
            let task = RuntimeTaskContext::without_deadline(self.cancellation.clone())
                .with_io_wave_controller(self.clone());
            CheckpointWorkContext::new(task).with_scheduler(scheduler)
        }

        /// No unit or I/O wave is active and no local operation is charged.
        /// An idle batch of a live context is not running work and is not
        /// visible in the scheduler state.
        pub(crate) fn assert_released(&self, scheduler: &LocalQosScheduler) {
            assert_eq!(
                self.started.load(Ordering::SeqCst),
                self.completed.load(Ordering::SeqCst),
                "a checkpoint unit is still active"
            );
            assert_eq!(self.active_io.load(Ordering::SeqCst), 0);
            assert_eq!(scheduler.state().running_background_operations, 0);
        }
    }

    impl QosTelemetrySink for CheckpointWorkProbe {
        fn record_qos(&self, event: QosTelemetryEvent) {
            assert_eq!(event.estimated_operations, 1);
            match (event.phase, event.outcome) {
                (QosTelemetryPhase::Admission, QosTelemetryOutcome::Admitted) => {
                    self.admissions.fetch_add(1, Ordering::SeqCst);
                    let active = self.active_units.fetch_add(1, Ordering::SeqCst) + 1;
                    self.peak_units.fetch_max(active, Ordering::SeqCst);
                }
                (QosTelemetryPhase::Completion, _) => {
                    // A batch admitted before this sink was installed can
                    // complete under it.
                    let _ = self.active_units.fetch_update(
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        |active| active.checked_sub(1),
                    );
                }
                _ => {}
            }
        }

        fn observes_units(&self) -> bool {
            true
        }

        fn record_qos_unit(&self, event: QosUnitEvent) {
            assert!(event.unit_in_batch >= 1);
            if event.phase == QosUnitPhase::Started {
                self.started.fetch_add(1, Ordering::SeqCst);
                return;
            }
            let completed = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
            let limit = self.cancel_after.load(Ordering::SeqCst);
            if limit > 0 && completed >= limit {
                self.cancellation.cancel();
            }
        }
    }

    #[derive(Debug)]
    struct IoLease(Arc<AtomicUsize>);

    impl Drop for IoLease {
        fn drop(&mut self) {
            assert_eq!(self.0.fetch_sub(1, Ordering::SeqCst), 1);
        }
    }

    impl RuntimeIoWaveController for CheckpointWorkProbe {
        fn acquire(
            &self,
            slots: NonZeroUsize,
            task: &RuntimeTaskContext,
        ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            assert_eq!(slots, NonZeroUsize::MIN);
            task.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            let wave = self.io_waves.fetch_add(1, Ordering::SeqCst) + 1;
            if self.cancel_on_io_wave.load(Ordering::SeqCst) == wave {
                self.cancellation.cancel();
                task.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            }
            assert_eq!(
                self.active_io.fetch_add(1, Ordering::SeqCst),
                0,
                "a builder must release its wave before another builder acquires one"
            );
            Ok(Box::new(IoLease(self.active_io.clone())))
        }

        fn try_acquire(
            &self,
            slots: NonZeroUsize,
            task: &RuntimeTaskContext,
        ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
            task.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            if self.active_io.load(Ordering::SeqCst) > 0 {
                Ok(None)
            } else {
                self.acquire(slots, task).map(Some)
            }
        }
    }
}

#[cfg(test)]
mod batch_tests;

mod decode;
pub(crate) use decode::{
    allocation as checkpoint_decode_allocation, CheckpointDecodeContext, CheckpointVectorPush,
};

mod text;
#[doc(hidden)]
pub use text::CheckpointText;

pub(crate) fn decode_vector(
    encoded: Vec<u8>,
    work: Option<&CheckpointWorkContext>,
) -> hawdb_core::Result<hawdb_core::Vector> {
    hawdb_core::Vector::from_encoded_with_work(
        encoded,
        || vector_unit(work),
        |unit| finish_vector_unit(unit, work),
        |error| hawdb_core::HawDBError::Storage(error.to_string()),
    )
}

pub(crate) fn validate_vector<'a>(
    encoded: &'a [u8],
    work: Option<&CheckpointWorkContext>,
) -> hawdb_core::Result<hawdb_core::VectorRef<'a>> {
    hawdb_core::VectorRef::from_encoded_with_work(
        encoded,
        || vector_unit(work),
        |unit| finish_vector_unit(unit, work),
        |error| hawdb_core::HawDBError::Storage(error.to_string()),
    )
}

fn vector_unit(
    work: Option<&CheckpointWorkContext>,
) -> hawdb_core::Result<Option<CheckpointWorkUnit>> {
    work.map(CheckpointWorkContext::start_unit)
        .transpose()
        .map_err(hawdb_core::HawDBError::from_storage_error)
}

fn finish_vector_unit(
    unit: Option<CheckpointWorkUnit>,
    work: Option<&CheckpointWorkContext>,
) -> hawdb_core::Result<()> {
    if let Some(unit) = unit {
        unit.finish();
    }
    if let Some(work) = work {
        work.checkpoint()
            .map_err(hawdb_core::HawDBError::from_storage_error)?;
    }
    Ok(())
}
