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

use hawdb_core::{RuntimeCancellationReason, RuntimeTaskContext};
use hawdb_qos::{
    RuntimeAdmissionCode, RuntimeAdmissionError, RuntimeGovernor, RuntimeGovernorSnapshot,
    RuntimeRetainedMemory, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest,
};
use hawdb_scheduler::{
    Priority as SchedulingPriority, Scheduler, SchedulerConfig, SchedulerError, TaskKey,
};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::{poll_fn, Future};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

mod segment_read;

pub use segment_read::{TokioSegmentReadExecutionError, TokioSegmentReadExecutor};

pub use tokio::runtime::Handle as TokioHandle;
pub use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};

#[derive(Debug)]
pub struct TokioBoundedSender<T>(mpsc::Sender<T>);

impl<T> Clone for TokioBoundedSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> TokioBoundedSender<T> {
    pub async fn send(&self, value: T) -> Result<(), T> {
        self.0.send(value).await.map_err(|error| error.0)
    }

    pub fn try_send(&self, value: T) -> Result<(), TokioBoundedTrySendError<T>> {
        self.0.try_send(value).map_err(|error| match error {
            mpsc::error::TrySendError::Full(value) => TokioBoundedTrySendError::Full(value),
            mpsc::error::TrySendError::Closed(value) => TokioBoundedTrySendError::Closed(value),
        })
    }
}

#[derive(Debug)]
pub enum TokioBoundedTrySendError<T> {
    Full(T),
    Closed(T),
}

#[derive(Debug)]
pub struct TokioBoundedReceiver<T>(mpsc::Receiver<T>);

impl<T> TokioBoundedReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    pub fn close(&mut self) {
        self.0.close();
    }
}

pub fn tokio_bounded_channel<T>(
    capacity: NonZeroUsize,
) -> (TokioBoundedSender<T>, TokioBoundedReceiver<T>) {
    let (sender, receiver) = mpsc::channel(capacity.get());
    (TokioBoundedSender(sender), TokioBoundedReceiver(receiver))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokioRuntimeOwnership {
    Borrowed,
    Owned,
}

impl TokioRuntimeOwnership {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Borrowed => "borrowed",
            Self::Owned => "owned",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokioRuntimeConfig {
    pub async_worker_threads: Option<NonZeroUsize>,
    pub max_blocking_threads: Option<NonZeroUsize>,
    pub resource_refresh_interval: Duration,
    pub blocking_thread_keep_alive: Duration,
}

impl TokioRuntimeConfig {
    pub fn from_governor(governor: &RuntimeGovernor) -> Self {
        let snapshot = governor.snapshot();
        let limits = snapshot.limits;
        let async_workers = limits.effective_cpu_slots.get().clamp(1, 2);
        Self {
            async_worker_threads: NonZeroUsize::new(async_workers),
            max_blocking_threads: Some(limits.effective_cpu_slots),
            ..Self::default()
        }
    }
}

impl Default for TokioRuntimeConfig {
    fn default() -> Self {
        Self {
            async_worker_threads: NonZeroUsize::new(1),
            max_blocking_threads: NonZeroUsize::new(4),
            resource_refresh_interval: Duration::from_secs(1),
            blocking_thread_keep_alive: Duration::from_secs(10),
        }
    }
}

#[derive(Clone)]
pub struct TokioRuntimeAdapter {
    handle: TokioHandle,
    ownership: TokioRuntimeOwnership,
    owned_runtime: Option<Arc<OwnedRuntime>>,
    governor: RuntimeGovernor,
    config: TokioRuntimeConfig,
    last_resource_refresh: Arc<Mutex<Instant>>,
    query_scheduler: Arc<Mutex<Option<Arc<Scheduler>>>>,
    query_scheduling: Arc<QuerySchedulingCounters>,
}

#[derive(Default)]
struct QuerySchedulingCounters {
    high: AtomicU64,
    normal: AtomicU64,
}

/// Counts accepted query submissions by requested priority. Feedback and
/// aging may change the level at which an individual task is dispatched.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuerySchedulingSnapshot {
    pub high: u64,
    pub normal: u64,
}

struct QueuedWork<T>(Arc<Mutex<Option<T>>>);

impl<T> Drop for QueuedWork<T> {
    fn drop(&mut self) {
        let work = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(work)));
    }
}

impl Debug for TokioRuntimeAdapter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokioRuntimeAdapter")
            .field("ownership", &self.ownership)
            .field("governor", &self.governor.snapshot())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

struct OwnedRuntime {
    runtime: Option<Runtime>,
}

impl Debug for OwnedRuntime {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnedRuntime")
            .field("running", &self.runtime.is_some())
            .finish()
    }
}

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Debug)]
pub enum TokioRuntimeError {
    NestedOwnedRuntime,
    BlockOnWithinRuntime,
    Build(std::io::Error),
}

impl Display for TokioRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NestedOwnedRuntime => formatter.write_str(
                "an owned HawDB Tokio runtime cannot be created inside an active Tokio runtime",
            ),
            Self::BlockOnWithinRuntime => formatter.write_str(
                "blocking on the HawDB Tokio adapter is not allowed inside an active Tokio runtime",
            ),
            Self::Build(error) => write!(formatter, "failed to build HawDB Tokio runtime: {error}"),
        }
    }
}

impl Error for TokioRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Build(error) => Some(error),
            Self::NestedOwnedRuntime | Self::BlockOnWithinRuntime => None,
        }
    }
}

#[derive(Debug)]
pub enum TokioTaskError<E> {
    Admission(RuntimeAdmissionError),
    Stopped(RuntimeCancellationReason),
    Join(JoinError),
    Operation(E),
}

impl<E: Display> Display for TokioTaskError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => Display::fmt(error, formatter),
            Self::Stopped(reason) => write!(formatter, "runtime task stopped: {reason}"),
            Self::Join(error) => write!(formatter, "runtime task join failed: {error}"),
            Self::Operation(error) => write!(formatter, "runtime task failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for TokioTaskError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Admission(error) => Some(error),
            Self::Stopped(reason) => Some(reason),
            Self::Join(error) => Some(error),
            Self::Operation(error) => Some(error),
        }
    }
}

impl TokioRuntimeAdapter {
    fn query_scheduler(&self) -> Result<Arc<Scheduler>, SchedulerError> {
        let mut scheduler = self
            .query_scheduler
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(scheduler) = scheduler.as_ref() {
            return Ok(Arc::clone(scheduler));
        }
        let host = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let workers = self
            .governor
            .snapshot()
            .limits
            .configured_cpu_slots
            .get()
            .min(host)
            .max(1);
        let created = Arc::new(Scheduler::new(SchedulerConfig {
            business_workers: workers,
            background_workers: 1,
            ..SchedulerConfig::default()
        })?);
        *scheduler = Some(Arc::clone(&created));
        Ok(created)
    }

    pub fn borrowed(
        handle: TokioHandle,
        governor: RuntimeGovernor,
        config: TokioRuntimeConfig,
    ) -> Self {
        Self {
            handle,
            ownership: TokioRuntimeOwnership::Borrowed,
            owned_runtime: None,
            governor,
            config,
            last_resource_refresh: Arc::new(Mutex::new(Instant::now())),
            query_scheduler: Arc::new(Mutex::new(None)),
            query_scheduling: Arc::new(QuerySchedulingCounters::default()),
        }
    }

    pub fn owned(
        governor: RuntimeGovernor,
        config: TokioRuntimeConfig,
    ) -> Result<Self, TokioRuntimeError> {
        if TokioHandle::try_current().is_ok() {
            return Err(TokioRuntimeError::NestedOwnedRuntime);
        }
        let mut builder = Builder::new_multi_thread();
        builder.enable_time();
        builder.thread_name("hawdb-runtime");
        builder.thread_keep_alive(config.blocking_thread_keep_alive);
        if let Some(worker_threads) = config.async_worker_threads {
            builder.worker_threads(worker_threads.get());
        }
        if let Some(max_blocking_threads) = config.max_blocking_threads {
            builder.max_blocking_threads(max_blocking_threads.get());
        }
        let runtime = builder.build().map_err(TokioRuntimeError::Build)?;
        let handle = runtime.handle().clone();
        Ok(Self {
            handle,
            ownership: TokioRuntimeOwnership::Owned,
            owned_runtime: Some(Arc::new(OwnedRuntime {
                runtime: Some(runtime),
            })),
            governor,
            config,
            last_resource_refresh: Arc::new(Mutex::new(Instant::now())),
            query_scheduler: Arc::new(Mutex::new(None)),
            query_scheduling: Arc::new(QuerySchedulingCounters::default()),
        })
    }

    pub fn ownership(&self) -> TokioRuntimeOwnership {
        self.ownership
    }

    pub fn handle(&self) -> &TokioHandle {
        &self.handle
    }

    pub fn governor(&self) -> &RuntimeGovernor {
        &self.governor
    }

    pub fn governor_snapshot(&self) -> RuntimeGovernorSnapshot {
        self.governor.snapshot()
    }

    pub fn query_scheduling_snapshot(&self) -> QuerySchedulingSnapshot {
        QuerySchedulingSnapshot {
            high: self.query_scheduling.high.load(Ordering::Relaxed),
            normal: self.query_scheduling.normal.load(Ordering::Relaxed),
        }
    }

    pub fn config(&self) -> TokioRuntimeConfig {
        self.config
    }

    pub fn owns_runtime(&self) -> bool {
        self.owned_runtime.is_some()
    }

    pub fn block_on<F>(&self, future: F) -> Result<F::Output, TokioRuntimeError>
    where
        F: Future,
    {
        if TokioHandle::try_current().is_ok() {
            return Err(TokioRuntimeError::BlockOnWithinRuntime);
        }
        Ok(self.handle.block_on(future))
    }

    pub async fn execute_blocking<T, E, F>(
        &self,
        request: RuntimeWorkRequest,
        context: RuntimeTaskContext,
        operation: F,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&RuntimeTaskContext) -> Result<T, E> + Send + 'static,
    {
        self.execute_blocking_with_request_factory(move |_| request, context, operation)
            .await
    }

    /// Prepares reusable state under a fixed memory reservation.
    ///
    /// The returned memory token retains the preparation charge without CPU,
    /// blocking-task or I/O slots. Keep it alive until the prepared state is
    /// consumed or dropped; execution requires its own admission. Failed,
    /// cancelled and panicking preparation releases the entire reservation.
    pub async fn prepare_blocking<T, E, F>(
        &self,
        priority: RuntimeWorkPriority,
        memory_bytes: u64,
        context: RuntimeTaskContext,
        operation: F,
    ) -> Result<(T, RuntimeRetainedMemory), TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&RuntimeTaskContext) -> Result<T, E> + Send + 'static,
    {
        let request = RuntimeWorkRequest::new(priority, RuntimeWorkKind::Control)
            .with_cpu_slots(1)
            .with_blocking(true)
            .with_memory_bytes(memory_bytes);
        let permit = self.acquire(request, &context).await?;
        let task_context = permit.bind_task_context(context);
        let join = self.handle.spawn_blocking(move || {
            task_context.checkpoint().map_err(TokioTaskError::Stopped)?;
            let result = operation(&task_context);
            let value = finish_operation(&task_context, result, true)?;
            let memory = permit
                .into_retained_memory()
                .expect("control work owns a fixed memory reservation");
            Ok((value, memory))
        });
        self.await_join(join).await
    }

    /// Executes blocking work with a request recomputed after every admission wake.
    ///
    /// The factory must preserve request priority across calls. It may reduce or
    /// increase resource reservations using the fresh governor snapshot.
    pub async fn execute_blocking_with_request_factory<T, E, F, R>(
        &self,
        mut request_for_snapshot: R,
        context: RuntimeTaskContext,
        operation: F,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&RuntimeTaskContext) -> Result<T, E> + Send + 'static,
        R: FnMut(RuntimeGovernorSnapshot) -> RuntimeWorkRequest + Send,
    {
        let permit = self
            .acquire_with(
                move |snapshot| request_for_snapshot(snapshot).with_blocking(true),
                &context,
            )
            .await?;
        let observe_post_operation_cancellation =
            permit.request().kind != RuntimeWorkKind::Mutation;
        let task_context = permit.bind_task_context(context);
        let join = self.handle.spawn_blocking(move || {
            let _permit = permit;
            task_context.checkpoint().map_err(TokioTaskError::Stopped)?;
            let result = operation(&task_context);
            finish_operation(&task_context, result, observe_post_operation_cancellation)
        });
        self.await_join(join).await
    }

    pub async fn execute_async<T, E, F, Fut>(
        &self,
        request: RuntimeWorkRequest,
        context: RuntimeTaskContext,
        operation: F,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(RuntimeTaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let observe_post_operation_cancellation = request.kind != RuntimeWorkKind::Mutation;
        let permit = self.acquire(request, &context).await?;
        let task_context = permit.bind_task_context(context);
        let join = self.handle.spawn(async move {
            let _permit = permit;
            task_context.checkpoint().map_err(TokioTaskError::Stopped)?;
            let checkpoint = task_context.clone();
            let result = operation(task_context).await;
            finish_operation(&checkpoint, result, observe_post_operation_cancellation)
        });
        self.await_join(join).await
    }

    /// Reuses owned preparation when execution can be admitted immediately.
    ///
    /// On any refusal, drops the rebuildable state before retrying admission. This
    /// prevents several queued preparations from retaining all memory while each
    /// waits for execution capacity. The operation receives `None` in that case
    /// and must rebuild under its execution reservation. The factory must include
    /// retained state in hard-capacity checks and reserve enough to rebuild it.
    pub async fn execute_blocking_with_reusable_state<T, E, S, F, R>(
        &self,
        state: S,
        mut request_for_snapshot: R,
        context: RuntimeTaskContext,
        operation: F,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        S: Send + 'static,
        F: FnOnce(Option<S>, &RuntimeTaskContext) -> Result<T, E> + Send + 'static,
        R: FnMut(RuntimeGovernorSnapshot, Option<&S>) -> RuntimeWorkRequest + Send,
    {
        let mut state = Some(state);
        let permit = self
            .acquire_with_state(
                &mut state,
                move |snapshot, state| request_for_snapshot(snapshot, state).with_blocking(true),
                &context,
            )
            .await?;
        let observe_cancellation = permit.request().kind != RuntimeWorkKind::Mutation;
        let task_context = permit.bind_task_context(context);
        let join = self.handle.spawn_blocking(move || {
            let _permit = permit;
            task_context.checkpoint().map_err(TokioTaskError::Stopped)?;
            let result = operation(state, &task_context);
            finish_operation(&task_context, result, observe_cancellation)
        });
        self.await_join(join).await
    }

    /// Dispatches owned query work through the business feedback queue.
    #[doc(hidden)]
    pub async fn execute_scheduled_with_reusable_state<T, E, S, F, R>(
        &self,
        state: S,
        mut request_for_snapshot: R,
        context: RuntimeTaskContext,
        scheduling: (TaskKey, SchedulingPriority),
        scheduler_error: impl Fn(String) -> E + Clone + Send + Sync + 'static,
        operation: F,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        S: Send + 'static,
        F: FnOnce(Option<S>, &RuntimeTaskContext) -> Result<T, E> + Send + 'static,
        R: FnMut(RuntimeGovernorSnapshot, Option<&S>) -> RuntimeWorkRequest + Send,
    {
        let (key, priority) = scheduling;
        let scheduler = self.query_scheduler().map_err(|error| {
            TokioTaskError::Operation(scheduler_error(format!("query scheduler failed: {error}")))
        })?;
        let mut state = Some(state);
        let permit = self
            .acquire_with_state_priority(
                &mut state,
                move |snapshot, state| request_for_snapshot(snapshot, state).with_blocking(true),
                &context,
                priority == SchedulingPriority::High,
            )
            .await?;
        let observe_cancellation = permit.request().kind != RuntimeWorkKind::Mutation;
        let task_context = permit.bind_task_context(context.clone());
        let work = Arc::new(Mutex::new(Some((state, permit, task_context, operation))));
        let _queued_guard = QueuedWork(Arc::clone(&work));
        let queued = Arc::clone(&work);
        let scheduler_lifetime = Arc::clone(&scheduler);
        let panic_error = scheduler_error.clone();
        let (sender, mut receiver) = oneshot::channel();
        let _handle = scheduler
            .spawn(key, priority, move || {
                let _scheduler_lifetime = scheduler_lifetime;
                let Some((state, permit, task_context, operation)) = queued
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take()
                else {
                    return;
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _permit = permit;
                    task_context.checkpoint().map_err(TokioTaskError::Stopped)?;
                    finish_operation(
                        &task_context,
                        operation(state, &task_context),
                        observe_cancellation,
                    )
                }))
                .unwrap_or_else(|_| {
                    Err(TokioTaskError::Operation(panic_error(
                        "scheduled query panicked".to_string(),
                    )))
                });
                let _ = sender.send(result);
            })
            .map_err(|error| {
                TokioTaskError::Operation(scheduler_error(format!(
                    "query scheduler failed: {error}"
                )))
            })?;
        match priority {
            SchedulingPriority::High => &self.query_scheduling.high,
            SchedulingPriority::Normal | SchedulingPriority::Low => &self.query_scheduling.normal,
        }
        .fetch_add(1, Ordering::Relaxed);
        let mut cancellation = Box::pin(context.cancellation().cancelled());
        let mut deadline = context.deadline().map(|deadline| {
            Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                deadline,
            )))
        });
        let completed = poll_fn(|cx| {
            if let Poll::Ready(result) = std::pin::Pin::new(&mut receiver).poll(cx) {
                return Poll::Ready(Ok(result));
            }
            if cancellation.as_mut().poll(cx).is_ready()
                && let Err(reason) = context.checkpoint()
            {
                return Poll::Ready(Err(reason));
            }
            if deadline
                .as_mut()
                .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
            {
                return Poll::Ready(Err(RuntimeCancellationReason::DeadlineExceeded));
            }
            Poll::Pending
        })
        .await;
        let result = match completed {
            Ok(result) => result,
            Err(reason) => {
                let queued_work = work
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
                if let Some(queued_work) = queued_work {
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        drop(queued_work)
                    }));
                    self.governor.record_cancellation(reason);
                    return Err(TokioTaskError::Stopped(reason));
                }
                receiver.await
            }
        };
        let result = result.map_err(|_| {
            TokioTaskError::Operation(scheduler_error(
                "scheduled query completion closed".to_string(),
            ))
        })?;
        if let Err(TokioTaskError::Stopped(reason)) = &result {
            self.governor.record_cancellation(*reason);
        }
        result
    }

    async fn acquire<E>(
        &self,
        request: RuntimeWorkRequest,
        context: &RuntimeTaskContext,
    ) -> Result<hawdb_qos::RuntimePermit, TokioTaskError<E>> {
        self.acquire_with(move |_| request, context).await
    }

    async fn acquire_with<E, R>(
        &self,
        mut request_for_snapshot: R,
        context: &RuntimeTaskContext,
    ) -> Result<hawdb_qos::RuntimePermit, TokioTaskError<E>>
    where
        R: FnMut(RuntimeGovernorSnapshot) -> RuntimeWorkRequest + Send,
    {
        self.acquire_with_state(
            &mut None::<()>,
            move |snapshot, _| request_for_snapshot(snapshot),
            context,
        )
        .await
    }

    async fn acquire_with_state<E, S, R>(
        &self,
        state: &mut Option<S>,
        request_for_snapshot: R,
        context: &RuntimeTaskContext,
    ) -> Result<hawdb_qos::RuntimePermit, TokioTaskError<E>>
    where
        S: Send,
        R: FnMut(RuntimeGovernorSnapshot, Option<&S>) -> RuntimeWorkRequest + Send,
    {
        self.acquire_with_state_priority(state, request_for_snapshot, context, false)
            .await
    }

    async fn acquire_with_state_priority<E, S, R>(
        &self,
        state: &mut Option<S>,
        mut request_for_snapshot: R,
        context: &RuntimeTaskContext,
        interactive: bool,
    ) -> Result<hawdb_qos::RuntimePermit, TokioTaskError<E>>
    where
        S: Send,
        R: FnMut(RuntimeGovernorSnapshot, Option<&S>) -> RuntimeWorkRequest + Send,
    {
        let mut wait = None;
        self.refresh_resources_if_due();
        let mut request = request_for_snapshot(self.governor.snapshot(), state.as_ref());
        let make_waiter = |priority| {
            let rank = RuntimeGovernor::effective_queue_rank(priority, context);
            if interactive {
                self.governor.admission_waiter_interactive(priority, rank)
            } else {
                self.governor.admission_waiter_ranked(priority, rank)
            }
        };
        let mut waiter = make_waiter(request.priority);
        loop {
            if let Err(reason) = context.checkpoint() {
                self.finish_admission_wait(request, wait.take());
                self.governor.record_cancellation(reason);
                return Err(TokioTaskError::Stopped(reason));
            }
            match self.governor.try_admit_waiter(&waiter, request) {
                Ok(permit) => {
                    self.finish_admission_wait(request, wait.take());
                    return Ok(permit);
                }
                Err(error) if state.is_some() => {
                    // Retained preparation can itself make the request exceed
                    // hard capacity. Rebuild under the execution reservation
                    // before treating that refusal as terminal.
                    drop(state.take());
                    request = request_for_snapshot(self.governor.snapshot(), None);
                    if !error.is_retryable() {
                        // A terminal refusal removes the waiter from the queue.
                        waiter = make_waiter(request.priority);
                    }
                    continue;
                }
                Err(error) if !error.is_retryable() => {
                    self.finish_admission_wait(request, wait.take());
                    return Err(TokioTaskError::Admission(error));
                }
                Err(error) => {
                    wait.get_or_insert_with(|| (Instant::now(), error.code));
                }
            }
            if let Err(reason) = self.wait_for_admission_event(&mut waiter, context).await {
                self.finish_admission_wait(request, wait.take());
                self.governor.record_cancellation(reason);
                return Err(TokioTaskError::Stopped(reason));
            }
            self.refresh_resources_if_due();
            request = request_for_snapshot(self.governor.snapshot(), state.as_ref());
        }
    }

    async fn wait_for_admission_event(
        &self,
        waiter: &mut hawdb_qos::RuntimeAdmissionWaiter,
        context: &RuntimeTaskContext,
    ) -> Result<(), RuntimeCancellationReason> {
        loop {
            let priority_change_at = waiter.next_priority_change_at();
            let mut admission = Box::pin(waiter.notified());
            let mut cancellation = Box::pin(context.cancellation().cancelled());
            let mut deadline = context.deadline().map(|deadline| {
                Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                    deadline,
                )))
            });
            let mut priority_change = priority_change_at.map(|deadline| {
                Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                    deadline,
                )))
            });
            let mut refresh = (!self.governor.snapshot().resources_pinned).then(|| {
                let last_refresh = *self
                    .last_resource_refresh
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let interval = self
                    .config
                    .resource_refresh_interval
                    .max(Duration::from_millis(1));
                let refresh_at = last_refresh
                    .checked_add(interval)
                    .unwrap_or_else(Instant::now);
                Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                    refresh_at,
                )))
            });
            let admission_notified = poll_fn(|task| {
                if cancellation.as_mut().poll(task).is_ready() {
                    return Poll::Ready(context.checkpoint().map(|()| false));
                }
                if deadline
                    .as_mut()
                    .is_some_and(|deadline| deadline.as_mut().poll(task).is_ready())
                {
                    return Poll::Ready(Err(RuntimeCancellationReason::DeadlineExceeded));
                }
                if admission.as_mut().poll(task).is_ready() {
                    return Poll::Ready(Ok(true));
                }
                if priority_change
                    .as_mut()
                    .is_some_and(|change| change.as_mut().poll(task).is_ready())
                {
                    return Poll::Ready(Ok(true));
                }
                if refresh
                    .as_mut()
                    .is_some_and(|refresh| refresh.as_mut().poll(task).is_ready())
                {
                    return Poll::Ready(Ok(false));
                }
                Poll::Pending
            })
            .await?;
            if admission_notified || self.refresh_resources_if_due() {
                return Ok(());
            }
        }
    }

    fn finish_admission_wait(
        &self,
        request: RuntimeWorkRequest,
        wait: Option<(Instant, RuntimeAdmissionCode)>,
    ) {
        let Some((started, code)) = wait else {
            return;
        };
        let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.governor
            .record_admission_wait(request, code, elapsed_micros);
    }

    async fn await_join<T, E>(
        &self,
        join: JoinHandle<Result<T, TokioTaskError<E>>>,
    ) -> Result<T, TokioTaskError<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let result = join.await.map_err(TokioTaskError::Join)?;
        if let Err(TokioTaskError::Stopped(reason)) = &result {
            self.governor.record_cancellation(*reason);
        }
        result
    }

    fn refresh_resources_if_due(&self) -> bool {
        let now = Instant::now();
        let should_refresh = {
            let mut last_refresh = self
                .last_resource_refresh
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if now.duration_since(*last_refresh) < self.config.resource_refresh_interval {
                false
            } else {
                *last_refresh = now;
                true
            }
        };
        if should_refresh {
            self.governor.refresh_from_host()
        } else {
            false
        }
    }
}

fn finish_operation<T, E>(
    context: &RuntimeTaskContext,
    result: Result<T, E>,
    observe_cancellation: bool,
) -> Result<T, TokioTaskError<E>> {
    if observe_cancellation
        && let Err(reason) = context.checkpoint()
        // A resource stop coordinates siblings; its originating operation error
        // carries the budget metadata that the caller needs to diagnose it.
        && (reason != RuntimeCancellationReason::ResourceExhausted || result.is_ok())
    {
        return Err(TokioTaskError::Stopped(reason));
    }
    result.map_err(TokioTaskError::Operation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeCancellationToken, RuntimeGovernorConfig,
        RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
        RuntimeTelemetryEvent, RuntimeTelemetryEventKind, RuntimeTelemetrySink,
        RuntimeWorkPriority,
    };
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn runtime_resources(cpu: usize) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(cpu).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(4 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
        )
    }

    fn governor(cpu: usize) -> RuntimeGovernor {
        RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            runtime_resources(cpu),
            IoConcurrencyBudget::new(4, 1),
        )
    }

    #[test]
    fn scheduled_queries_prioritize_high_and_release_cancelled_queued_work() {
        let governor = governor(4);
        let adapter = TokioRuntimeAdapter::owned(
            governor.clone(),
            TokioRuntimeConfig::from_governor(&governor),
        )
        .unwrap();
        let scheduler = Arc::new(
            Scheduler::new(SchedulerConfig {
                business_workers: 1,
                background_workers: 1,
                queue_capacity: 8,
                aging_interval: Duration::from_secs(60),
                ..SchedulerConfig::default()
            })
            .unwrap(),
        );
        *adapter.query_scheduler.lock().unwrap() = Some(Arc::clone(&scheduler));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = scheduler
            .spawn(TaskKey::new(1), SchedulingPriority::High, move || {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            })
            .unwrap();
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        adapter
            .block_on(async {
                let spawn = |priority, value, context: RuntimeTaskContext| {
                    let adapter = adapter.clone();
                    let order = Arc::clone(&order);
                    tokio::spawn(async move {
                        adapter
                            .execute_scheduled_with_reusable_state(
                                (),
                                |_, _| RuntimeWorkRequest::foreground_query(1, 0),
                                context,
                                (TaskKey::new(value), priority),
                                |error| error,
                                move |_, _| {
                                    order.lock().unwrap().push(value);
                                    Ok::<_, String>(())
                                },
                            )
                            .await
                    })
                };
                let normal = spawn(SchedulingPriority::Normal, 2, RuntimeTaskContext::default());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while scheduler.business_queued() < 1 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let context = RuntimeTaskContext::default();
                let cancelled = spawn(SchedulingPriority::Normal, 3, context.clone());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while scheduler.business_queued() < 2 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                context.cancellation().cancel();
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(2), cancelled)
                        .await
                        .unwrap()
                        .unwrap(),
                    Err(TokioTaskError::Stopped(
                        RuntimeCancellationReason::Cancelled
                    ))
                ));
                let high = spawn(SchedulingPriority::High, 4, RuntimeTaskContext::default());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while scheduler.business_queued() < 3 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                release_tx.send(()).unwrap();
                high.await.unwrap().unwrap();
                normal.await.unwrap().unwrap();
            })
            .unwrap();
        blocker.join().unwrap();
        assert_eq!(*order.lock().unwrap(), [4, 2]);
        assert_eq!(
            adapter.query_scheduling_snapshot(),
            QuerySchedulingSnapshot { high: 1, normal: 2 }
        );
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
    }

    #[test]
    fn scheduled_query_panic_returns_an_operation_error_and_releases_permit() {
        let governor = governor(1);
        let adapter = TokioRuntimeAdapter::owned(
            governor.clone(),
            TokioRuntimeConfig::from_governor(&governor),
        )
        .unwrap();
        let result = adapter
            .block_on(adapter.execute_scheduled_with_reusable_state(
                (),
                |_, _| RuntimeWorkRequest::foreground_query(1, 0),
                RuntimeTaskContext::default(),
                (TaskKey::new(1), SchedulingPriority::Normal),
                |error| error,
                |_, _| -> Result<(), String> { panic!("fixture panic") },
            ))
            .unwrap();
        assert!(
            matches!(result, Err(TokioTaskError::Operation(error)) if error == "scheduled query panicked")
        );
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
    }

    #[derive(Debug, Default)]
    struct RecordingRuntimeTelemetry {
        events: Mutex<Vec<RuntimeTelemetryEvent>>,
    }

    impl RuntimeTelemetrySink for RecordingRuntimeTelemetry {
        fn record_runtime(&self, event: RuntimeTelemetryEvent) {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }
    }

    fn cgroup_resources(limit: u64, current: u64) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(6 * 1024 * 1024 * 1024),
                Some(limit),
                None,
                Some(current),
            ),
        )
    }

    #[test]
    fn prepared_state_releases_the_single_cpu_slot_and_keeps_memory_until_execution() {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(100),
                ..RuntimeGovernorConfig::shared_host()
            },
            runtime_resources(1),
            IoConcurrencyBudget::new(1, 1),
        );
        let adapter = TokioRuntimeAdapter::owned(
            governor.clone(),
            TokioRuntimeConfig::from_governor(&governor),
        )
        .unwrap();
        adapter
            .block_on(async {
                let (value, memory) = adapter
                    .prepare_blocking(
                        RuntimeWorkPriority::Foreground,
                        40,
                        RuntimeTaskContext::default(),
                        |_| Ok::<_, Infallible>(42),
                    )
                    .await
                    .unwrap();
                let prepared = governor.snapshot();
                assert_eq!(prepared.active_cpu_slots, 0);
                assert_eq!(prepared.active_blocking_tasks, 0);
                assert_eq!(prepared.admitted_memory_bytes, 40);
                let observed = governor.clone();
                let result = adapter
                    .execute_blocking_with_reusable_state(
                        memory,
                        |_, memory| {
                            RuntimeWorkRequest::foreground_query(60, 0).with_retained_memory_bytes(
                                memory.map_or(0, |memory| memory.bytes()),
                            )
                        },
                        RuntimeTaskContext::with_timeout(Duration::from_secs(5)),
                        move |memory, _| {
                            assert!(
                                memory.is_some(),
                                "immediate admission must reuse preparation"
                            );
                            assert_eq!(observed.snapshot().admitted_memory_bytes, 100);
                            drop(memory);
                            Ok::<_, Infallible>(value)
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result, 42);
            })
            .unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
    }

    #[test]
    fn retained_preparation_is_released_before_a_hard_capacity_refusal() {
        for execution_bytes in [80, 101] {
            let governor = RuntimeGovernor::new(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(100),
                    ..RuntimeGovernorConfig::shared_host()
                },
                runtime_resources(1),
                IoConcurrencyBudget::new(1, 1),
            );
            let adapter = TokioRuntimeAdapter::owned(
                governor.clone(),
                TokioRuntimeConfig::from_governor(&governor),
            )
            .unwrap();
            let result = adapter
                .block_on(async {
                    let (_, state) = adapter
                        .prepare_blocking(
                            RuntimeWorkPriority::Foreground,
                            40,
                            RuntimeTaskContext::default(),
                            |_| Ok::<_, Infallible>(()),
                        )
                        .await
                        .unwrap();
                    adapter
                        .execute_blocking_with_reusable_state(
                            state,
                            |_, state| {
                                RuntimeWorkRequest::foreground_query(execution_bytes, 0)
                                    .with_retained_memory_bytes(
                                        state.map_or(0, |state| state.bytes()),
                                    )
                            },
                            RuntimeTaskContext::with_timeout(Duration::from_secs(5)),
                            |state, _| {
                                assert!(state.is_none());
                                Ok::<_, Infallible>(())
                            },
                        )
                        .await
                })
                .unwrap();
            if execution_bytes <= 100 {
                result.unwrap();
            } else {
                let Err(TokioTaskError::Admission(error)) = result else {
                    panic!("execution exceeding hard capacity must be rejected");
                };
                assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
                assert!(!error.is_retryable());
            }
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            assert_eq!(governor.snapshot().queued_admission_waiters, 0);
        }
    }

    #[test]
    fn dropping_or_cancelling_queued_execution_releases_prepared_memory() {
        for cancel in [false, true] {
            let governor = governor(1);
            let adapter = TokioRuntimeAdapter::owned(
                governor.clone(),
                TokioRuntimeConfig::from_governor(&governor),
            )
            .unwrap();
            adapter
                .block_on(async {
                    let (_, memory) = adapter
                        .prepare_blocking(
                            RuntimeWorkPriority::Foreground,
                            40,
                            RuntimeTaskContext::default(),
                            |_| Ok::<_, Infallible>(()),
                        )
                        .await
                        .unwrap();
                    let held = governor
                        .try_admit(RuntimeWorkRequest::foreground_query(0, 0))
                        .unwrap();
                    let token = RuntimeCancellationToken::new();
                    let mut execution = Box::pin(adapter.execute_blocking_with_reusable_state(
                        memory,
                        |_, memory| {
                            RuntimeWorkRequest::foreground_query(60, 0).with_retained_memory_bytes(
                                memory.map_or(0, |memory| memory.bytes()),
                            )
                        },
                        RuntimeTaskContext::without_deadline(token.clone()),
                        move |memory, _| -> Result<(), Infallible> {
                            drop(memory);
                            panic!("execution must not start while the only CPU is held");
                        },
                    ));
                    std::future::poll_fn(|context| {
                        assert!(execution.as_mut().poll(context).is_pending());
                        std::task::Poll::Ready(())
                    })
                    .await;
                    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
                    if cancel {
                        token.cancel();
                        assert!(matches!(
                            execution.await,
                            Err(TokioTaskError::Stopped(
                                RuntimeCancellationReason::Cancelled
                            ))
                        ));
                    } else {
                        drop(execution);
                    }
                    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
                    assert_eq!(governor.snapshot().queued_admission_waiters, 0);
                    drop(held);
                })
                .unwrap();
        }
    }

    #[test]
    fn failed_or_panicking_preparation_releases_all_resources() {
        let governor = governor(1);
        let adapter =
            TokioRuntimeAdapter::owned(governor.clone(), TokioRuntimeConfig::default()).unwrap();
        for panic in [false, true] {
            let result = adapter
                .block_on(adapter.prepare_blocking(
                    RuntimeWorkPriority::Foreground,
                    40,
                    RuntimeTaskContext::default(),
                    move |_| -> Result<(), &'static str> {
                        assert!(!panic, "preparation panic");
                        Err("preparation failed")
                    },
                ))
                .unwrap();
            assert!(result.is_err());
            let after = governor.snapshot();
            assert_eq!(after.admitted_memory_bytes, 0);
            assert_eq!(after.active_cpu_slots, 0);
            assert_eq!(after.active_blocking_tasks, 0);
        }
    }

    #[test]
    fn concurrent_preparations_cannot_hold_all_memory_while_waiting_for_each_other() {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(100),
                ..RuntimeGovernorConfig::shared_host()
            },
            runtime_resources(1),
            IoConcurrencyBudget::new(1, 1),
        );
        let adapter = TokioRuntimeAdapter::owned(
            governor.clone(),
            TokioRuntimeConfig::from_governor(&governor),
        )
        .unwrap();
        adapter
            .block_on(async {
                let mut states = Vec::new();
                for _ in 0..2 {
                    let (_, memory) = adapter
                        .prepare_blocking(
                            RuntimeWorkPriority::Foreground,
                            40,
                            RuntimeTaskContext::default(),
                            |_| Ok::<_, Infallible>(()),
                        )
                        .await
                        .unwrap();
                    states.push(memory);
                }
                assert_eq!(governor.snapshot().admitted_memory_bytes, 80);
                let execute = |state| {
                    adapter.execute_blocking_with_reusable_state(
                        state,
                        |_, memory: Option<&RuntimeRetainedMemory>| {
                            RuntimeWorkRequest::foreground_query(40, 0).with_retained_memory_bytes(
                                memory.map_or(0, |memory| memory.bytes()),
                            )
                        },
                        RuntimeTaskContext::default(),
                        |memory, _| Ok::<_, Infallible>(memory.is_some()),
                    )
                };
                let mut first = Box::pin(execute(states.pop().unwrap()));
                let mut second = Box::pin(execute(states.pop().unwrap()));
                let mut first_result = None;
                let mut second_result = None;
                let both = std::future::poll_fn(|context| {
                    if first_result.is_none()
                        && let Poll::Ready(result) = first.as_mut().poll(context)
                    {
                        first_result = Some(result);
                    }
                    if second_result.is_none()
                        && let Poll::Ready(result) = second.as_mut().poll(context)
                    {
                        second_result = Some(result);
                    }
                    if first_result.is_some() && second_result.is_some() {
                        Poll::Ready((first_result.take().unwrap(), second_result.take().unwrap()))
                    } else {
                        Poll::Pending
                    }
                });
                let (first, second) = tokio::time::timeout(Duration::from_secs(5), both)
                    .await
                    .expect("prepared queries must not form a memory wait cycle");
                let first_reused = first.unwrap();
                let second_reused = second.unwrap();
                assert!(!first_reused || !second_reused);
            })
            .unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().queued_admission_waiters, 0);
    }

    #[test]
    fn governor_config_bounds_blocking_threads_to_effective_cpu_slots() {
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(
                NonZeroUsize::new(32).unwrap(),
                NonZeroUsize::new(2),
                None,
            ),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(4 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
        );
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            resources,
            IoConcurrencyBudget::new(8, 2),
        );

        let config = TokioRuntimeConfig::from_governor(&governor);

        assert_eq!(governor.snapshot().limits.effective_cpu_slots.get(), 2);
        assert_eq!(config.async_worker_threads.unwrap().get(), 2);
        assert_eq!(config.max_blocking_threads.unwrap().get(), 2);
    }

    #[test]
    fn borrowed_runtime_does_not_take_host_lifecycle_ownership() {
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let adapter = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor(2),
            TokioRuntimeConfig::default(),
        );
        assert_eq!(adapter.ownership(), TokioRuntimeOwnership::Borrowed);
        assert!(!adapter.owns_runtime());
        let value = host
            .block_on(adapter.execute_async(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 1, 0),
                RuntimeTaskContext::default(),
                |_| async { Ok::<_, Infallible>(42) },
            ))
            .unwrap();
        assert_eq!(value, 42);
        drop(adapter);
        assert_eq!(host.block_on(async { 7 }), 7);
    }

    #[test]
    fn admitted_resources_reach_async_and_blocking_operations() {
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let adapter = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor(2),
            TokioRuntimeConfig::default(),
        );
        let request = RuntimeWorkRequest::foreground_query(128, 32).with_cpu_slots(2);

        let async_resources = host
            .block_on(adapter.execute_async(
                request.with_blocking(false),
                RuntimeTaskContext::default(),
                |context| async move {
                    Ok::<_, Infallible>((
                        context.admitted_parallelism(),
                        context.memory_reservation(),
                    ))
                },
            ))
            .unwrap();
        let blocking_resources = host
            .block_on(
                adapter.execute_blocking(request, RuntimeTaskContext::default(), |context| {
                    Ok::<_, Infallible>((
                        context.admitted_parallelism(),
                        context.memory_reservation(),
                    ))
                }),
            )
            .unwrap();

        for (parallelism, reservation) in [async_resources, blocking_resources] {
            assert_eq!(parallelism.get(), 2);
            let reservation = reservation.unwrap();
            assert_eq!(reservation.memory_bytes(), 128);
            assert_eq!(reservation.result_bytes(), 32);
        }
    }

    #[test]
    fn request_factory_renegotiates_parallelism_after_resource_change() {
        let governor = governor(4);
        governor.pin_resources();
        let held = governor
            .try_admit(
                RuntimeWorkRequest::foreground_query(0, 0)
                    .with_cpu_slots(4)
                    .with_blocking(true),
            )
            .unwrap();
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let adapter = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor.clone(),
            TokioRuntimeConfig {
                resource_refresh_interval: Duration::from_secs(3600),
                ..TokioRuntimeConfig::default()
            },
        );
        let observed_slots = Arc::new(Mutex::new(Vec::new()));
        let request_slots = Arc::clone(&observed_slots);

        host.block_on(async {
            let execution = adapter.execute_blocking_with_request_factory(
                move |snapshot| {
                    let slots = snapshot.limits.effective_cpu_slots.get();
                    request_slots
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(slots);
                    RuntimeWorkRequest::foreground_query(0, 0).with_cpu_slots(slots)
                },
                RuntimeTaskContext::default(),
                |context| Ok::<_, Infallible>(context.admitted_parallelism().get()),
            );
            tokio::pin!(execution);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut execution)
                    .await
                    .is_err()
            );

            assert!(governor.update_resources(runtime_resources(2)));
            tokio::task::yield_now().await;
            drop(held);

            let parallelism = tokio::time::timeout(Duration::from_secs(1), &mut execution)
                .await
                .expect("released capacity must wake queued admission")
                .expect("renegotiated request must be admitted");
            assert_eq!(parallelism, 2);
        });
        let observed_slots = observed_slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(observed_slots.first(), Some(&4));
        assert!(observed_slots.iter().skip(1).any(|slots| *slots == 2));
    }

    #[test]
    fn background_waiter_ages_without_an_external_resource_event() {
        let governor = governor(2);
        governor.pin_resources();
        let held = governor
            .try_admit(
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0)
                    .with_cpu_slots(1),
            )
            .unwrap();
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let adapter = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor,
            TokioRuntimeConfig::default(),
        );

        host.block_on(async {
            let foreground = adapter.execute_blocking(
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0)
                    .with_cpu_slots(2),
                RuntimeTaskContext::default(),
                |_| Ok::<_, Infallible>(()),
            );
            let background = adapter.execute_blocking(
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Background, 0),
                RuntimeTaskContext::default(),
                |_| Ok::<_, Infallible>(()),
            );
            tokio::pin!(foreground);
            tokio::pin!(background);

            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut foreground)
                    .await
                    .is_err()
            );
            tokio::time::timeout(Duration::from_secs(1), &mut background)
                .await
                .expect("background aging must trigger a one-shot admission retry")
                .expect("the aged background request must use the available slot");
        });
        drop(held);
    }

    #[test]
    fn owned_runtime_rejects_nested_creation() {
        let host = Builder::new_current_thread().enable_time().build().unwrap();
        let result = host.block_on(async {
            TokioRuntimeAdapter::owned(governor(1), TokioRuntimeConfig::default())
        });
        assert!(matches!(result, Err(TokioRuntimeError::NestedOwnedRuntime)));
    }

    #[test]
    fn owned_runtime_drop_is_safe_inside_another_runtime() {
        let adapter =
            TokioRuntimeAdapter::owned(governor(1), TokioRuntimeConfig::default()).unwrap();
        let host = Builder::new_current_thread().enable_time().build().unwrap();
        host.block_on(async move { drop(adapter) });
    }

    #[test]
    fn admission_resource_refresh_is_shared_and_throttled() {
        let host = Builder::new_current_thread().enable_time().build().unwrap();
        let adapter = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor(1),
            TokioRuntimeConfig {
                resource_refresh_interval: Duration::ZERO,
                ..TokioRuntimeConfig::default()
            },
        );
        let initial = *adapter
            .last_resource_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        adapter.refresh_resources_if_due();
        let refreshed = *adapter
            .last_resource_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut clone = adapter.clone();
        clone.config.resource_refresh_interval = Duration::from_secs(60 * 60);
        clone.refresh_resources_if_due();
        let throttled = *adapter
            .last_resource_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        assert!(refreshed >= initial);
        assert_eq!(throttled, refreshed);
    }

    #[test]
    fn blocking_execution_respects_governor_parallelism() {
        let governor = governor(2);
        governor.pin_resources();
        let telemetry = Arc::new(RecordingRuntimeTelemetry::default());
        governor.set_telemetry_sink(Some(telemetry.clone()));
        let adapter = TokioRuntimeAdapter::owned(
            governor,
            TokioRuntimeConfig {
                max_blocking_threads: NonZeroUsize::new(8),
                ..TokioRuntimeConfig::default()
            },
        )
        .unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        adapter
            .block_on(async {
                let (entered, started) = std::sync::mpsc::channel();
                let mut releases = Vec::new();
                let mut executions = (0..6)
                    .map(|_| {
                        let active = Arc::clone(&active);
                        let peak = Arc::clone(&peak);
                        let entered = entered.clone();
                        let (release, released) = std::sync::mpsc::channel::<()>();
                        releases.push(release);
                        Box::pin(adapter.execute_blocking(
                            RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0),
                            RuntimeTaskContext::default(),
                            move |_| {
                                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                                peak.fetch_max(current, Ordering::SeqCst);
                                let _ = entered.send(());
                                // Dropping the senders also releases work if an assertion fails.
                                let _ = released.recv();
                                active.fetch_sub(1, Ordering::SeqCst);
                                Ok::<_, Infallible>(())
                            },
                        ))
                    })
                    .collect::<Vec<_>>();
                // Poll every request while admitted work is held: contenders must
                // reach the admission wait, independent of OS thread scheduling.
                poll_fn(|context| {
                    for execution in &mut executions {
                        assert!(execution.as_mut().poll(context).is_pending());
                    }
                    Poll::Ready(())
                })
                .await;
                let snapshot = adapter.governor_snapshot();
                assert_eq!(snapshot.active_cpu_slots, 2);
                assert_eq!(snapshot.queued_admission_waiters, 4);
                assert!(snapshot.retryable_admission_rejections >= 4);
                let both_started =
                    (0..2).all(|_| started.recv_timeout(Duration::from_secs(5)).is_ok());
                drop(releases);
                for execution in executions {
                    execution.await.unwrap();
                }
                assert!(both_started, "both admitted blocking operations must start");
            })
            .unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(adapter.governor_snapshot().completions, 6);
        assert!(adapter.governor_snapshot().admission_waits > 0);
        assert!(adapter.governor_snapshot().retryable_admission_rejections > 0);
        assert!(telemetry
            .events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|event| {
                event.kind == RuntimeTelemetryEventKind::AdmissionWait && event.elapsed_micros > 0
            }));
    }

    #[test]
    fn cancellation_returns_at_a_cooperative_blocking_checkpoint() {
        let adapter =
            TokioRuntimeAdapter::owned(governor(1), TokioRuntimeConfig::default()).unwrap();
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let handle = adapter.handle().clone();
        handle.spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            token.cancel();
        });
        let started = Instant::now();
        let result = adapter
            .block_on(adapter.execute_blocking(
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0),
                context,
                |context| -> Result<(), RuntimeCancellationReason> {
                    loop {
                        context.checkpoint()?;
                        std::thread::sleep(Duration::from_millis(2));
                    }
                },
            ))
            .unwrap();
        assert!(matches!(
            result,
            Err(TokioTaskError::Stopped(
                RuntimeCancellationReason::Cancelled
            ))
        ));
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(adapter.governor_snapshot().cancellations >= 1);
    }

    #[test]
    fn deadline_stops_a_task_waiting_for_admission() {
        let governor = governor(1);
        let held = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                0,
            ))
            .unwrap();
        let adapter =
            TokioRuntimeAdapter::owned(governor.clone(), TokioRuntimeConfig::default()).unwrap();
        let result = adapter
            .block_on(adapter.execute_blocking(
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0),
                RuntimeTaskContext::with_timeout(Duration::from_millis(20)),
                |_| Ok::<_, Infallible>(()),
            ))
            .unwrap();
        assert!(matches!(
            result,
            Err(TokioTaskError::Stopped(
                RuntimeCancellationReason::DeadlineExceeded
            ))
        ));
        assert_eq!(adapter.governor_snapshot().deadline_exceeded, 1);
        drop(held);
    }

    #[test]
    fn cancellation_wakes_a_task_waiting_for_admission() {
        for reason in [
            RuntimeCancellationReason::Cancelled,
            RuntimeCancellationReason::ResourceExhausted,
        ] {
            let governor = governor(1);
            let held = governor
                .try_admit(RuntimeWorkRequest::blocking_cpu(
                    RuntimeWorkPriority::Foreground,
                    0,
                ))
                .unwrap();
            let host = Builder::new_multi_thread().enable_time().build().unwrap();
            let adapter = TokioRuntimeAdapter::borrowed(
                host.handle().clone(),
                governor,
                TokioRuntimeConfig {
                    resource_refresh_interval: Duration::from_secs(3600),
                    ..TokioRuntimeConfig::default()
                },
            );
            let token = RuntimeCancellationToken::new();
            let context = RuntimeTaskContext::without_deadline(token.clone());
            host.spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if reason == RuntimeCancellationReason::ResourceExhausted {
                    token.cancel_for_resource_exhaustion();
                } else {
                    token.cancel();
                }
            });

            let result = host
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(1),
                        adapter.execute_blocking(
                            RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0),
                            context,
                            |_| Ok::<_, Infallible>(()),
                        ),
                    )
                    .await
                })
                .expect("cancellation must wake admission without waiting for resource refresh");
            assert!(matches!(
                result,
                Err(TokioTaskError::Stopped(
                    observed
                )) if observed == reason
            ));
            drop(held);
        }
    }

    #[test]
    fn mutation_reports_the_committed_result_after_starting() {
        let adapter =
            TokioRuntimeAdapter::owned(governor(1), TokioRuntimeConfig::default()).unwrap();
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let (operation_started, wait_for_operation) = std::sync::mpsc::channel();
        let (cancellation_finished, wait_for_cancellation) = std::sync::mpsc::channel();
        let canceller = std::thread::spawn(move || {
            wait_for_operation.recv().unwrap();
            assert!(token.cancel());
            cancellation_finished.send(()).unwrap();
        });
        let result = adapter
            .block_on(adapter.execute_blocking(
                RuntimeWorkRequest::foreground_mutation(0),
                context,
                move |_| {
                    operation_started.send(()).unwrap();
                    wait_for_cancellation.recv().unwrap();
                    Ok::<_, Infallible>(42)
                },
            ))
            .unwrap();
        canceller.join().unwrap();
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn resource_cancellation_preserves_operation_errors_but_refuses_success() {
        let adapter =
            TokioRuntimeAdapter::owned(governor(1), TokioRuntimeConfig::default()).unwrap();
        let request = RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 0);
        for fail in [true, false] {
            let blocking = adapter
                .block_on(adapter.execute_blocking(
                    request,
                    RuntimeTaskContext::default(),
                    move |context| {
                        context.cancellation().cancel_for_resource_exhaustion();
                        if fail {
                            Err("original resource error")
                        } else {
                            Ok(42)
                        }
                    },
                ))
                .unwrap();
            let asynchronous = adapter
                .block_on(adapter.execute_async(
                    request,
                    RuntimeTaskContext::default(),
                    move |context| async move {
                        context.cancellation().cancel_for_resource_exhaustion();
                        if fail {
                            Err("original resource error")
                        } else {
                            Ok(42)
                        }
                    },
                ))
                .unwrap();
            for result in [blocking, asynchronous] {
                if fail {
                    assert!(matches!(
                        result,
                        Err(TokioTaskError::Operation("original resource error"))
                    ));
                } else {
                    assert!(matches!(
                        result,
                        Err(TokioTaskError::Stopped(
                            RuntimeCancellationReason::ResourceExhausted
                        ))
                    ));
                }
            }
        }
    }

    /// A saturated cgroup rejects retryably, so an async admission waits
    /// instead of failing; when a resource refresh restores headroom, the
    /// same waiting admission must succeed without being re-submitted.
    #[test]
    fn waiting_admission_succeeds_after_refresh_restores_headroom() {
        let limit = 512 * 1024 * 1024;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            cgroup_resources(limit, limit),
            IoConcurrencyBudget::new(4, 1),
        );
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let config = TokioRuntimeConfig {
            resource_refresh_interval: Duration::from_secs(3600),
            ..TokioRuntimeConfig::default()
        };
        let adapter = TokioRuntimeAdapter::borrowed(host.handle().clone(), governor, config);
        host.block_on(async {
            let request =
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 48 * 1024 * 1024);
            let context = RuntimeTaskContext::default();
            let acquire = adapter.acquire::<Infallible>(request, &context);
            tokio::pin!(acquire);
            let still_waiting = tokio::time::timeout(Duration::from_millis(60), &mut acquire).await;
            assert!(
                still_waiting.is_err(),
                "admission must wait while the cgroup is saturated"
            );
            assert!(adapter
                .governor
                .update_resources(cgroup_resources(limit, 0)));
            let permit = tokio::time::timeout(Duration::from_secs(5), &mut acquire)
                .await
                .expect("admission must resume after the refresh")
                .expect("restored headroom must admit the waiting request");
            drop(permit);
        });
    }

    /// A request that was satisfiable when submitted may become impossible
    /// after the cgroup policy ceiling shrinks. The existing acquire future
    /// must observe the refreshed capacity and terminate rather than polling
    /// forever for headroom that can no longer satisfy it.
    #[test]
    fn waiting_admission_terminates_after_capacity_shrinks_below_request() {
        let initial_limit = 512 * 1024 * 1024;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            cgroup_resources(initial_limit, initial_limit),
            IoConcurrencyBudget::new(4, 1),
        );
        let host = Builder::new_multi_thread().enable_time().build().unwrap();
        let config = TokioRuntimeConfig {
            resource_refresh_interval: Duration::from_secs(3600),
            ..TokioRuntimeConfig::default()
        };
        let adapter = TokioRuntimeAdapter::borrowed(host.handle().clone(), governor, config);
        host.block_on(async {
            let request =
                RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 96 * 1024 * 1024);
            let context = RuntimeTaskContext::default();
            let acquire = adapter.acquire::<Infallible>(request, &context);
            tokio::pin!(acquire);
            let still_waiting = tokio::time::timeout(Duration::from_millis(60), &mut acquire).await;
            assert!(still_waiting.is_err());

            assert!(adapter
                .governor
                .update_resources(cgroup_resources(64 * 1024 * 1024, 0)));
            let error = tokio::time::timeout(Duration::from_secs(5), &mut acquire)
                .await
                .expect("capacity shrink must terminate the admission loop")
                .expect_err("request above refreshed capacity must terminate");
            let TokioTaskError::Admission(error) = error else {
                panic!("capacity shrink must return an admission error");
            };
            assert_eq!(error.code, hawdb_qos::RuntimeAdmissionCode::MemorySaturated);
            assert!(!error.is_retryable());
        });
    }
}
