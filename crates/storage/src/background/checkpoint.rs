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

use hawdb_core::{
    RuntimeCancellationReason, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeMemoryError,
    RuntimeMemoryPermit, RuntimeTaskContext,
};
use hawdb_qos::{LocalQosPermit, LocalQosScheduler, QosAdmission, WorkClass, WorkRequest};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

mod buffer;
pub(crate) use buffer::CheckpointBytes;
pub use buffer::CheckpointSharedBytes;

mod values;
pub(crate) use values::{CheckpointSharedValues, CheckpointValues};

mod allocation;
pub(crate) use allocation::{CheckpointAllocationOwner, CheckpointAllocationToken};

/// The task already admitted by the owner, optionally with per-unit local QoS.
/// This never creates a governor or reserves the owner's memory a second time.
/// Builders must keep each unit bounded by their record/page limits. One unit
/// must never stand for a complete dataset build or an unbounded merge loop.
#[doc(hidden)]
#[derive(Debug, Clone, Default)]
pub struct CheckpointWorkContext {
    task: RuntimeTaskContext,
    scheduler: Option<LocalQosScheduler>,
    recorded_failure: Option<Arc<Mutex<Option<CheckpointWorkError>>>>,
}

impl CheckpointWorkContext {
    pub fn new(task: RuntimeTaskContext) -> Self {
        Self {
            task,
            scheduler: None,
            recorded_failure: None,
        }
    }

    pub fn with_scheduler(mut self, scheduler: LocalQosScheduler) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn checkpoint(&self) -> Result<(), CheckpointWorkError> {
        self.task
            .checkpoint()
            .map_err(CheckpointWorkError::Stopped)
            .map_err(|error| self.record_failure(error))
    }

    /// Preserve typed work failures through an existing codec's string errors.
    /// Ordinary operation errors retain their original diagnostic and class.
    pub(crate) fn classify<T, E>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, CheckpointOperationError<E>> {
        let unit = self.start_unit().map_err(CheckpointOperationError::Work)?;
        let recorded = Arc::new(Mutex::new(None));
        let mut scoped = self.clone();
        scoped.recorded_failure = Some(recorded.clone());
        unit.finish();
        let result = operation(&scoped);
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
        self.checkpoint()?;
        let permit = self
            .scheduler
            .as_ref()
            .map(|scheduler| {
                scheduler
                    .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                    .map_err(CheckpointWorkError::Admission)
                    .map_err(|error| self.record_failure(error))
            })
            .transpose()?;
        // A telemetry callback can cancel while admission is being recorded.
        self.checkpoint()?;
        Ok(CheckpointWorkUnit(permit))
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

pub(crate) struct CheckpointWorkUnit(Option<LocalQosPermit>);

impl CheckpointWorkUnit {
    pub(crate) fn finish(self) {
        if let Some(permit) = self.0 {
            permit.finish_with_outcome(true);
        }
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

pub(crate) enum CheckpointOperationError<E> {
    Work(CheckpointWorkError),
    Operation(E),
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

    #[derive(Debug, Default)]
    pub(crate) struct CheckpointWorkProbe {
        pub(crate) cancellation: RuntimeCancellationToken,
        pub(crate) completed: AtomicUsize,
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

        pub(crate) fn assert_released(&self, scheduler: &LocalQosScheduler) {
            assert_eq!(self.active_units.load(Ordering::SeqCst), 0);
            assert_eq!(self.active_io.load(Ordering::SeqCst), 0);
            assert_eq!(scheduler.state().running_background_operations, 0);
        }
    }

    impl QosTelemetrySink for CheckpointWorkProbe {
        fn record_qos(&self, event: QosTelemetryEvent) {
            assert_eq!(event.estimated_operations, 1);
            match (event.phase, event.outcome) {
                (QosTelemetryPhase::Admission, QosTelemetryOutcome::Admitted) => {
                    let active = self.active_units.fetch_add(1, Ordering::SeqCst) + 1;
                    self.peak_units.fetch_max(active, Ordering::SeqCst);
                }
                (QosTelemetryPhase::Completion, _) => {
                    assert!(self.active_units.fetch_sub(1, Ordering::SeqCst) > 0);
                    let completed = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
                    let limit = self.cancel_after.load(Ordering::SeqCst);
                    if limit > 0 && completed >= limit {
                        self.cancellation.cancel();
                    }
                }
                _ => {}
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

mod decode;
pub(crate) use decode::{allocation as checkpoint_decode_allocation, CheckpointDecodeContext};

mod text;
#[doc(hidden)]
pub use text::CheckpointText;
