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

use crate::time::{Duration, Instant};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Debug;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

const NOT_CANCELLED: u8 = 0;
const CANCELLED: u8 = 1;
const RESOURCE_EXHAUSTED: u8 = 2;

#[derive(Debug, Clone)]
pub struct RuntimeCancellationToken {
    state: Arc<RuntimeCancellationState>,
}

#[derive(Debug)]
struct RuntimeCancellationState {
    cancellation_reason: AtomicU8,
    parents: [Option<RuntimeCancellationToken>; 2],
    next_waiter_id: AtomicU64,
    waiters: Mutex<BTreeMap<u64, Waker>>,
}

impl Default for RuntimeCancellationToken {
    fn default() -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancellation_reason: AtomicU8::new(NOT_CANCELLED),
                parents: [None, None],
                next_waiter_id: AtomicU64::new(1),
                waiters: Mutex::new(BTreeMap::new()),
            }),
        }
    }
}

impl RuntimeCancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) -> bool {
        self.cancel_with_code(CANCELLED)
    }

    /// Stops cooperating siblings after an unrecoverable resource refusal.
    /// The first local cancellation cause is retained. Cancelling a child does
    /// not cancel its parent; the operation must still return its original error.
    pub fn cancel_for_resource_exhaustion(&self) -> bool {
        self.cancel_with_code(RESOURCE_EXHAUSTED)
    }

    fn cancel_with_code(&self, reason: u8) -> bool {
        if self
            .state
            .cancellation_reason
            .compare_exchange(NOT_CANCELLED, reason, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        let waiters = {
            let mut waiters = self
                .state
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for (_, waiter) in waiters {
            waiter.wake();
        }
        true
    }

    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    fn reason(&self) -> Option<RuntimeCancellationReason> {
        match self.state.cancellation_reason.load(Ordering::Acquire) {
            CANCELLED => Some(RuntimeCancellationReason::Cancelled),
            RESOURCE_EXHAUSTED => Some(RuntimeCancellationReason::ResourceExhausted),
            _ => self
                .state
                .parents
                .iter()
                .flatten()
                .find_map(RuntimeCancellationToken::reason),
        }
    }

    pub fn child(&self) -> Self {
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancellation_reason: AtomicU8::new(NOT_CANCELLED),
                parents: [Some(self.clone()), None],
                next_waiter_id: AtomicU64::new(1),
                waiters: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    fn linked(&self, other: &Self) -> Self {
        if Arc::ptr_eq(&self.state, &other.state) {
            return self.child();
        }
        Self {
            state: Arc::new(RuntimeCancellationState {
                cancellation_reason: AtomicU8::new(NOT_CANCELLED),
                parents: [Some(self.clone()), Some(other.clone())],
                next_waiter_id: AtomicU64::new(1),
                waiters: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Resolves when this token or any parent token is cancelled.
    pub fn cancelled(&self) -> RuntimeCancellationFuture<'_> {
        RuntimeCancellationFuture {
            token: self,
            registrations: Vec::new(),
        }
    }
}

struct RuntimeCancellationRegistration {
    state: Weak<RuntimeCancellationState>,
    waiter_id: u64,
}

/// A runtime-neutral future returned by [`RuntimeCancellationToken::cancelled`].
pub struct RuntimeCancellationFuture<'a> {
    token: &'a RuntimeCancellationToken,
    registrations: Vec<RuntimeCancellationRegistration>,
}

impl Future for RuntimeCancellationFuture<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }

        if self.registrations.is_empty() {
            let mut pending = vec![self.token];
            let mut visited = BTreeSet::new();
            while let Some(current) = pending.pop() {
                if !visited.insert(Arc::as_ptr(&current.state)) {
                    continue;
                }
                let waiter_id = current.state.next_waiter_id.fetch_add(1, Ordering::Relaxed);
                current
                    .state
                    .waiters
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(waiter_id, context.waker().clone());
                self.registrations.push(RuntimeCancellationRegistration {
                    state: Arc::downgrade(&current.state),
                    waiter_id,
                });
                pending.extend(current.state.parents.iter().flatten());
            }
        } else {
            for registration in &self.registrations {
                if let Some(state) = registration.state.upgrade()
                    && let Some(waiter) = state
                        .waiters
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get_mut(&registration.waiter_id)
                    && !waiter.will_wake(context.waker())
                {
                    *waiter = context.waker().clone();
                }
            }
        }

        if self.token.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for RuntimeCancellationFuture<'_> {
    fn drop(&mut self) {
        for registration in &self.registrations {
            if let Some(state) = registration.state.upgrade() {
                state
                    .waiters
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&registration.waiter_id);
            }
        }
    }
}

/// A drop guard for one admitted storage I/O wave.
pub trait RuntimeIoWavePermit: Debug + Send {}

impl<T: Debug + Send> RuntimeIoWavePermit for T {}

/// Execution-owned hook for acquiring the I/O capacity declared at admission.
pub trait RuntimeIoWaveController: Debug + Send + Sync {
    /// Attempts to acquire one I/O wave without blocking the calling thread.
    ///
    /// `Ok(None)` means that the reservation is currently saturated. Async
    /// runtime adapters must yield before retrying instead of spinning.
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError>;

    fn acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIoWaveError {
    Stopped(RuntimeCancellationReason),
    ReservationExceeded {
        requested_slots: usize,
        reserved_slots: usize,
    },
}

#[derive(Debug)]
pub enum RuntimeIoWaveTryAcquire {
    /// Capacity is available. Ungoverned contexts carry no permit.
    Acquired(Option<Box<dyn RuntimeIoWavePermit>>),
    /// Capacity is currently saturated and the caller should yield.
    Pending,
}

impl Display for RuntimeIoWaveError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped(reason) => write!(formatter, "runtime I/O wave stopped: {reason}"),
            Self::ReservationExceeded {
                requested_slots,
                reserved_slots,
            } => write!(
                formatter,
                "runtime I/O wave requested {requested_slots} slots, exceeding the {reserved_slots}-slot reservation"
            ),
        }
    }
}

impl Error for RuntimeIoWaveError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Stopped(reason) => Some(reason),
            Self::ReservationExceeded { .. } => None,
        }
    }
}

mod memory;
mod work;
pub use memory::{
    RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit, RuntimeMemoryTarget,
};

#[derive(Debug, Clone)]
pub struct RuntimeTaskContext {
    cancellation: RuntimeCancellationToken,
    deadline: Option<Instant>,
    admitted_parallelism: NonZeroUsize,
    executor_thread_limit: Option<NonZeroUsize>,
    memory_reservation: Option<RuntimeMemoryReservation>,
    memory_controller: Option<Arc<dyn RuntimeMemoryController>>,
    memory_target: Option<Arc<dyn RuntimeMemoryTarget>>,
    io_wave_controller: Option<Arc<dyn RuntimeIoWaveController>>,
    work_budget: Option<Arc<work::RuntimeWorkBudget>>,
}

/// Memory already reserved for one task by the runtime governor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMemoryReservation {
    memory_bytes: u64,
    result_bytes: u64,
}

impl RuntimeMemoryReservation {
    pub const fn new(memory_bytes: u64, result_bytes: u64) -> Self {
        Self {
            memory_bytes,
            result_bytes,
        }
    }

    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    pub const fn result_bytes(self) -> u64 {
        self.result_bytes
    }

    pub const fn intersect(self, other: Self) -> Self {
        Self {
            memory_bytes: if self.memory_bytes < other.memory_bytes {
                self.memory_bytes
            } else {
                other.memory_bytes
            },
            result_bytes: if self.result_bytes < other.result_bytes {
                self.result_bytes
            } else {
                other.result_bytes
            },
        }
    }
}

impl RuntimeTaskContext {
    pub fn new(cancellation: RuntimeCancellationToken, deadline: Option<Instant>) -> Self {
        Self {
            cancellation,
            deadline,
            admitted_parallelism: NonZeroUsize::MIN,
            executor_thread_limit: None,
            memory_reservation: None,
            memory_controller: None,
            memory_target: None,
            io_wave_controller: None,
            work_budget: None,
        }
    }

    pub fn without_deadline(cancellation: RuntimeCancellationToken) -> Self {
        Self::new(cancellation, None)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(
            RuntimeCancellationToken::new(),
            Instant::now().checked_add(timeout),
        )
    }

    pub fn cancellation(&self) -> &RuntimeCancellationToken {
        &self.cancellation
    }

    pub fn child(&self) -> Self {
        Self {
            cancellation: self.cancellation.child(),
            deadline: self.deadline,
            admitted_parallelism: self.admitted_parallelism,
            executor_thread_limit: self.executor_thread_limit,
            memory_reservation: self.memory_reservation,
            memory_controller: self.memory_controller.clone(),
            memory_target: self.memory_target.clone(),
            io_wave_controller: self.io_wave_controller.clone(),
            work_budget: self.work_budget.clone(),
        }
    }

    /// Applies statement constraints without replacing an enclosing admission.
    /// Cancellation from either scope stops the statement; cancelling the combined
    /// scope does not cancel its parents. Budgets remain shared and can only tighten.
    /// Independent controllers cannot represent one admission and are rejected.
    pub fn constrained_by(&self, request: &Self) -> crate::Result<Self> {
        fn controller<T: ?Sized>(
            parent: &Option<Arc<T>>,
            request: &Option<Arc<T>>,
            resource: &str,
        ) -> crate::Result<Option<Arc<T>>> {
            match (parent, request) {
                (Some(parent), Some(request)) if !Arc::ptr_eq(parent, request) => {
                    Err(crate::HawDBError::Execution(format!(
                        "request task context carries a different {resource} admission"
                    )))
                }
                (Some(parent), _) => Ok(Some(parent.clone())),
                (_, request) => Ok(request.clone()),
            }
        }
        fn minimum<T: Ord + Copy>(left: Option<T>, right: Option<T>) -> Option<T> {
            match (left, right) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (left, right) => left.or(right),
            }
        }
        Ok(Self {
            cancellation: self.cancellation.linked(&request.cancellation),
            deadline: minimum(self.deadline, request.deadline),
            admitted_parallelism: self.admitted_parallelism.min(request.admitted_parallelism),
            executor_thread_limit: minimum(
                self.executor_thread_limit,
                request.executor_thread_limit,
            ),
            memory_reservation: match (self.memory_reservation, request.memory_reservation) {
                (Some(parent), Some(request)) => Some(parent.intersect(request)),
                (parent, request) => parent.or(request),
            },
            memory_controller: controller(
                &self.memory_controller,
                &request.memory_controller,
                "memory",
            )?,
            memory_target: controller(
                &self.memory_target,
                &request.memory_target,
                "memory target",
            )?,
            io_wave_controller: controller(
                &self.io_wave_controller,
                &request.io_wave_controller,
                "I/O",
            )?,
            work_budget: work::RuntimeWorkBudget::intersect(
                &self.work_budget,
                &request.work_budget,
            ),
        })
    }

    /// Carries the CPU parallelism already reserved by the runtime governor.
    ///
    /// This is an execution ceiling, not a request to create worker threads.
    /// Executors must still apply their operator memory and input-size limits.
    pub fn with_admitted_parallelism(mut self, admitted_parallelism: NonZeroUsize) -> Self {
        self.admitted_parallelism = admitted_parallelism;
        self
    }

    pub fn admitted_parallelism(&self) -> NonZeroUsize {
        self.admitted_parallelism
    }

    /// Carries the physical executor-thread ceiling derived by the runtime governor.
    ///
    /// This is distinct from [`Self::admitted_parallelism`], which is the CPU
    /// reservation for one task. An absent limit denotes an ungoverned library call.
    pub fn with_executor_thread_limit(mut self, limit: NonZeroUsize) -> Self {
        self.executor_thread_limit = Some(
            self.executor_thread_limit
                .map_or(limit, |current| current.min(limit)),
        );
        self
    }

    pub fn executor_thread_limit(&self) -> Option<NonZeroUsize> {
        self.executor_thread_limit
    }

    /// Carries the memory already reserved by the runtime governor.
    ///
    /// Governed executors must use this reservation instead of a static
    /// per-query configuration limit. An absent reservation denotes an
    /// ungoverned library call and preserves the configured fallback.
    pub fn with_memory_reservation(mut self, reservation: RuntimeMemoryReservation) -> Self {
        self.memory_reservation = Some(reservation);
        self
    }

    pub fn memory_reservation(&self) -> Option<RuntimeMemoryReservation> {
        self.memory_reservation
    }

    /// Whether execution carries dynamic resource controllers rather than only
    /// a static ceiling. Synchronous adapters must preserve these controllers
    /// and must not replace an originating governor admission with a local cap.
    pub fn has_resource_controllers(&self) -> bool {
        self.memory_controller.is_some()
            || self.memory_target.is_some()
            || self.io_wave_controller.is_some()
    }

    /// Carries host-driven pressure feedback to revocable operator work areas.
    pub fn with_memory_target(mut self, target: Arc<dyn RuntimeMemoryTarget>) -> Self {
        self.memory_target = Some(target);
        self
    }

    /// The target can only tighten the caller's ceiling. Operators retain a
    /// bounded progress floor and continue to enforce all hard allocation limits.
    pub fn working_memory_target(&self, hard_limit: u64) -> u64 {
        self.memory_target.as_ref().map_or(hard_limit, |target| {
            target.target_bytes(hard_limit).min(hard_limit)
        })
    }

    /// Binds actual allocation ownership to an already admitted task.
    pub fn with_memory_controller(mut self, controller: Arc<dyn RuntimeMemoryController>) -> Self {
        self.memory_controller = Some(controller);
        self
    }

    /// Reserve before allocating and retain the returned lease until destruction.
    /// Clones and children share their controller. Ungoverned contexts return
    /// `None`; a supplied static ceiling still limits an individual allocation.
    pub fn reserve_working_memory(
        &self,
        bytes: u64,
    ) -> Result<Option<Box<dyn RuntimeMemoryPermit>>, RuntimeMemoryError> {
        self.checkpoint().map_err(RuntimeMemoryError::Stopped)?;
        if let Some(reservation) = self.memory_reservation
            && bytes > reservation.memory_bytes()
        {
            return Err(RuntimeMemoryError::ReservationExceeded {
                requested_bytes: bytes,
                available_bytes: reservation.memory_bytes(),
            });
        }
        self.memory_controller
            .as_ref()
            .map(|controller| {
                controller.reserve(
                    bytes,
                    self.memory_reservation
                        .map_or(u64::MAX, RuntimeMemoryReservation::memory_bytes),
                )
            })
            .transpose()
    }

    /// Binds the controller owned by a successful runtime admission.
    pub fn with_io_wave_controller(mut self, controller: Arc<dyn RuntimeIoWaveController>) -> Self {
        self.io_wave_controller = Some(controller);
        self
    }

    /// Acquires capacity for one storage I/O wave when this is an admitted context.
    ///
    /// Raw contexts have no controller and return `Ok(None)`. The returned
    /// permit must remain live only while physical reads are in flight.
    pub fn acquire_io_wave(
        &self,
        slots: NonZeroUsize,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        self.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        self.io_wave_controller
            .as_ref()
            .map(|controller| controller.acquire(slots, self))
            .transpose()
    }

    /// Attempts to acquire capacity for one storage I/O wave without blocking.
    ///
    /// Raw, ungoverned contexts return `Acquired(None)`. Governed contexts
    /// return `Pending` while another wave owns the requested capacity.
    pub fn try_acquire_io_wave(
        &self,
        slots: NonZeroUsize,
    ) -> Result<RuntimeIoWaveTryAcquire, RuntimeIoWaveError> {
        self.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
        let Some(controller) = &self.io_wave_controller else {
            return Ok(RuntimeIoWaveTryAcquire::Acquired(None));
        };
        controller.try_acquire(slots, self).map(|permit| {
            permit.map_or(RuntimeIoWaveTryAcquire::Pending, |permit| {
                RuntimeIoWaveTryAcquire::Acquired(Some(permit))
            })
        })
    }

    /// Starts a bounded execution scope while retaining every parent limit and
    /// the original cancellation, memory and I/O admission. Clones share usage.
    pub fn with_work_limit(mut self, limit: usize) -> Self {
        self.work_budget = Some(Arc::new(work::RuntimeWorkBudget::new(
            limit,
            self.work_budget.take(),
        )));
        self
    }

    pub fn has_work_limit(&self) -> bool {
        self.work_budget.is_some()
    }

    pub fn work_units(&self) -> usize {
        self.work_budget.as_ref().map_or(0, |budget| budget.used())
    }

    /// Preserve a typed refusal across adapters whose error transport is text.
    pub fn check_work_limit(&self) -> crate::Result<()> {
        match self
            .work_budget
            .as_ref()
            .and_then(|budget| budget.refusal())
        {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    /// Charge explicit candidate work before examining it, including rejected
    /// candidates. Checkpoints and allocation byte counts are not work units.
    pub fn charge_work(&self, units: usize) -> crate::Result<()> {
        let Some(budget) = &self.work_budget else {
            return Ok(());
        };
        self.checkpoint().map_err(|reason| {
            crate::HawDBError::Execution(format!("runtime task stopped: {reason}"))
        })?;
        budget.charge(units)
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn checkpoint(&self) -> Result<(), RuntimeCancellationReason> {
        if let Some(reason) = self.cancellation.reason() {
            return Err(reason);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(RuntimeCancellationReason::DeadlineExceeded);
        }
        Ok(())
    }
}

impl Default for RuntimeTaskContext {
    fn default() -> Self {
        Self::without_deadline(RuntimeCancellationToken::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCancellationReason {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
}

impl RuntimeCancellationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::ResourceExhausted => "resource_exhausted",
        }
    }
}

impl Display for RuntimeCancellationReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Error for RuntimeCancellationReason {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    #[derive(Debug)]
    struct RecordingIoController {
        active_slots: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct RecordingIoPermit {
        active_slots: Arc<AtomicUsize>,
        slots: usize,
    }

    impl Drop for RecordingIoPermit {
        fn drop(&mut self) {
            self.active_slots.fetch_sub(self.slots, Ordering::AcqRel);
        }
    }

    impl RuntimeIoWaveController for RecordingIoController {
        fn try_acquire(
            &self,
            slots: NonZeroUsize,
            context: &RuntimeTaskContext,
        ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
            self.acquire(slots, context).map(Some)
        }

        fn acquire(
            &self,
            slots: NonZeroUsize,
            context: &RuntimeTaskContext,
        ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            self.active_slots.fetch_add(slots.get(), Ordering::AcqRel);
            Ok(Box::new(RecordingIoPermit {
                active_slots: Arc::clone(&self.active_slots),
                slots: slots.get(),
            }))
        }
    }

    #[test]
    fn cancellation_is_shared_across_context_clones() {
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        assert!(context.checkpoint().is_ok());
        assert!(token.cancel());
        assert!(!token.cancel());
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::Cancelled)
        );
    }

    #[test]
    fn expired_deadline_fails_at_a_cooperative_checkpoint() {
        let context = RuntimeTaskContext::new(
            RuntimeCancellationToken::new(),
            Some(Instant::now() - Duration::from_millis(1)),
        );
        assert_eq!(
            context.checkpoint(),
            Err(RuntimeCancellationReason::DeadlineExceeded)
        );
    }

    #[test]
    fn child_cancellation_is_local_and_parent_cancellation_propagates() {
        let parent_token = RuntimeCancellationToken::new();
        let parent = RuntimeTaskContext::without_deadline(parent_token.clone());
        let child = parent.child();

        assert!(child.cancellation().cancel());
        assert!(child.checkpoint().is_err());
        assert!(parent.checkpoint().is_ok());

        let sibling = parent.child();
        assert!(parent_token.cancel());
        assert!(parent.checkpoint().is_err());
        assert!(sibling.checkpoint().is_err());
    }

    #[test]
    fn resource_exhaustion_keeps_its_cause_and_is_local_to_the_child() {
        let parent = RuntimeTaskContext::default();
        let child = parent.child();
        let clone = child.clone();
        let grandchild = child.child();
        assert!(child.cancellation().cancel_for_resource_exhaustion());
        assert!(!clone.cancellation().cancel());
        assert!(!clone.cancellation().cancel_for_resource_exhaustion());
        for context in [&child, &clone, &grandchild] {
            assert_eq!(
                context.checkpoint(),
                Err(RuntimeCancellationReason::ResourceExhausted)
            );
        }
        assert_eq!(parent.checkpoint(), Ok(()));
        assert_eq!(parent.child().checkpoint(), Ok(()));

        let cancelled = parent.child();
        assert!(cancelled.cancellation().cancel());
        assert!(!cancelled.cancellation().cancel_for_resource_exhaustion());
        assert_eq!(
            cancelled.checkpoint(),
            Err(RuntimeCancellationReason::Cancelled)
        );
    }

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn constrained_context_observes_both_cancellations_without_cancelling_parents() {
        for cancel_parent in [false, true] {
            let parent = RuntimeTaskContext::default();
            let request = RuntimeTaskContext::default();
            let combined = parent.constrained_by(&request).unwrap();
            let counter = Arc::new(WakeCounter::default());
            let waker = Waker::from(counter.clone());
            let mut context = Context::from_waker(&waker);
            let mut cancelled = Box::pin(combined.cancellation().cancelled());
            assert!(cancelled.as_mut().poll(&mut context).is_pending());
            if cancel_parent {
                parent.cancellation().cancel();
            } else {
                request.cancellation().cancel();
            }
            assert_eq!(counter.0.load(Ordering::Acquire), 1);
            assert!(cancelled.as_mut().poll(&mut context).is_ready());
            assert_eq!(
                combined.checkpoint(),
                Err(RuntimeCancellationReason::Cancelled)
            );
        }
        let parent = RuntimeTaskContext::default();
        let request = RuntimeTaskContext::default();
        let combined = parent.constrained_by(&request).unwrap();
        combined.cancellation().cancel();
        assert!(parent.checkpoint().is_ok());
        assert!(request.checkpoint().is_ok());
    }

    #[test]
    fn constrained_descendants_register_shared_ancestors_once_and_unregister_on_drop() {
        let parent = RuntimeTaskContext::default();
        let request = parent.child();
        let combined = parent.constrained_by(&request).unwrap();
        let waker = Waker::from(Arc::new(WakeCounter::default()));
        let mut context = Context::from_waker(&waker);
        let mut cancelled = Box::pin(combined.cancellation().cancelled());
        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        assert_eq!(parent.cancellation.state.waiters.lock().unwrap().len(), 1);
        assert_eq!(request.cancellation.state.waiters.lock().unwrap().len(), 1);
        drop(cancelled);
        assert!(parent.cancellation.state.waiters.lock().unwrap().is_empty());
        assert!(request
            .cancellation
            .state
            .waiters
            .lock()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn constrained_context_tightens_limits_and_keeps_the_original_io_controller() {
        let active_slots = Arc::new(AtomicUsize::new(0));
        let parent = RuntimeTaskContext::with_timeout(Duration::from_secs(5))
            .with_memory_reservation(RuntimeMemoryReservation::new(100, 20))
            .with_admitted_parallelism(NonZeroUsize::new(4).unwrap())
            .with_io_wave_controller(Arc::new(RecordingIoController {
                active_slots: active_slots.clone(),
            }));
        let request = RuntimeTaskContext::with_timeout(Duration::from_secs(10))
            .with_memory_reservation(RuntimeMemoryReservation::new(30, 50))
            .with_admitted_parallelism(NonZeroUsize::new(2).unwrap());
        let combined = parent.constrained_by(&request).unwrap();
        assert_eq!(combined.deadline(), parent.deadline());
        assert_eq!(
            combined.memory_reservation(),
            Some(RuntimeMemoryReservation::new(30, 20))
        );
        assert_eq!(combined.admitted_parallelism().get(), 2);
        let io = combined.acquire_io_wave(NonZeroUsize::MIN).unwrap();
        assert_eq!(active_slots.load(Ordering::Acquire), 1);
        drop(io);
        assert_eq!(active_slots.load(Ordering::Acquire), 0);
        assert!(parent.constrained_by(&parent.child()).is_ok());
        let independent =
            request.with_io_wave_controller(Arc::new(RecordingIoController { active_slots }));
        assert!(parent.constrained_by(&independent).is_err());
    }

    #[test]
    fn cancellation_future_is_woken_by_parent_cancellation() {
        let parent = RuntimeCancellationToken::new();
        let child = parent.child();
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);
        let mut cancelled = Box::pin(child.cancelled());

        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        assert!(parent.cancel());
        assert_eq!(counter.0.load(Ordering::Acquire), 1);
        assert!(cancelled.as_mut().poll(&mut context).is_ready());
    }

    #[test]
    fn resource_exhaustion_wakes_waiters_on_descendants() {
        let parent = RuntimeTaskContext::default();
        let child = parent.child();
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);
        let mut cancelled = Box::pin(child.cancellation().cancelled());
        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        assert!(parent.cancellation().cancel_for_resource_exhaustion());
        assert_eq!(counter.0.load(Ordering::Acquire), 1);
        assert!(cancelled.as_mut().poll(&mut context).is_ready());
        assert_eq!(
            child.checkpoint(),
            Err(RuntimeCancellationReason::ResourceExhausted)
        );
    }

    #[test]
    fn dropped_cancellation_future_unregisters_from_its_parent_chain() {
        let parent = RuntimeCancellationToken::new();
        let child = parent.child();
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);
        let mut cancelled = Box::pin(child.cancelled());

        assert!(cancelled.as_mut().poll(&mut context).is_pending());
        drop(cancelled);
        assert!(parent.cancel());
        assert_eq!(counter.0.load(Ordering::Acquire), 0);
    }

    #[test]
    fn child_preserves_admitted_parallelism() {
        let context = RuntimeTaskContext::default()
            .with_admitted_parallelism(NonZeroUsize::new(4).expect("test parallelism is non-zero"))
            .with_executor_thread_limit(
                NonZeroUsize::new(2).expect("test thread limit is non-zero"),
            );

        assert_eq!(context.admitted_parallelism().get(), 4);
        assert_eq!(context.child().admitted_parallelism().get(), 4);
        assert_eq!(context.child().executor_thread_limit().unwrap().get(), 2);
    }

    #[test]
    fn child_preserves_memory_reservation() {
        let reservation = RuntimeMemoryReservation::new(128, 32);
        let context = RuntimeTaskContext::default().with_memory_reservation(reservation);

        assert_eq!(context.memory_reservation(), Some(reservation));
        assert_eq!(context.child().memory_reservation(), Some(reservation));
    }

    #[test]
    fn child_preserves_io_controller_and_permit_releases_slots() {
        let active_slots = Arc::new(AtomicUsize::new(0));
        let context = RuntimeTaskContext::default().with_io_wave_controller(Arc::new(
            RecordingIoController {
                active_slots: Arc::clone(&active_slots),
            },
        ));

        let permit = context
            .child()
            .acquire_io_wave(NonZeroUsize::new(2).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(active_slots.load(Ordering::Acquire), 2);
        drop(permit);
        assert_eq!(active_slots.load(Ordering::Acquire), 0);
    }

    #[test]
    fn ungoverned_try_acquire_is_immediately_ready() {
        let acquired = RuntimeTaskContext::default()
            .try_acquire_io_wave(NonZeroUsize::new(2).unwrap())
            .unwrap();

        assert!(matches!(acquired, RuntimeIoWaveTryAcquire::Acquired(None)));
    }
}
