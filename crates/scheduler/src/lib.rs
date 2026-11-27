// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded multi-level feedback scheduling for background and prioritized work.
//!
//! The scheduler owns two independent worker pools, one for business work and
//! one for background work. Each pool has three feedback levels. A task starts
//! at its [`Priority`] level; a dispatch that exceeds the execution quantum, or
//! a task that returns [`TaskProgress::Yield`], is demoted before its next
//! dispatch. A task that waits longer than the aging interval is promoted one
//! level. This is a cooperative scheduler: it cannot preempt a closure. Long
//! operations must yield at their own checkpoints.
//!
//! Business work never runs on the background pool, so a background task
//! cannot occupy a worker that business work needs. The cost is a permanently
//! reserved background worker; a later phase may let idle background workers
//! help the business pool.

#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const LEVELS: usize = 3;

/// Initial scheduling level of a task.
///
/// Priority is the starting level, not a hard guarantee. Feedback can demote a
/// task that runs too long, and aging can promote a task that waits too long;
/// this is what keeps one expensive key from monopolizing a level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    High,
    Normal,
    Low,
}

impl Priority {
    pub const fn level(self) -> usize {
        match self {
            Self::High => 0,
            Self::Normal => 1,
            Self::Low => 2,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Normal => "normal",
            Self::Low => "low",
        }
    }
}

/// Stable identity used to carry feedback between repeated dispatches of one
/// logical workload (a session, tenant, background class, or operation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskKey(u64);

impl TaskKey {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Pool a task belongs to. Background work is isolated from business work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskClass {
    Business,
    Background,
}

impl TaskClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Business => "business",
            Self::Background => "background",
        }
    }
}

/// The context of one dispatch.
#[derive(Debug, Clone)]
pub struct TaskContext {
    key: TaskKey,
    class: TaskClass,
    priority: Priority,
    level: usize,
    cancelled: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
}

impl TaskContext {
    pub const fn key(&self) -> TaskKey {
        self.key
    }

    pub const fn class(&self) -> TaskClass {
        self.class
    }

    pub const fn priority(&self) -> Priority {
        self.priority
    }

    /// The feedback level of this dispatch. The first dispatch uses the
    /// priority level; later dispatches can differ after feedback and aging.
    pub const fn level(&self) -> usize {
        self.level
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// True once the owning pool has started shutting down. A task that
    /// returns a result after this point is reported as
    /// [`TaskError::Shutdown`] by the scheduler.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Cooperatively stop at the next boundary. A handle cancellation wins
    /// over a pool shutdown when both are set.
    pub fn checkpoint(&self) -> Result<(), TaskError> {
        if self.is_cancelled() {
            Err(TaskError::Cancelled)
        } else if self.is_shutting_down() {
            Err(TaskError::Shutdown)
        } else {
            Ok(())
        }
    }
}

/// Result of one task dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskProgress<T> {
    /// The task completed and produced its result.
    Done(T),
    /// The task reached a cooperative boundary and must be dispatched again.
    Yield,
}

/// A cooperative task that can be dispatched more than once.
pub trait Task: Send + 'static {
    type Output: Send + 'static;

    fn run(&mut self, context: &TaskContext) -> TaskProgress<Self::Output>;
}

struct OnceTask<F, T> {
    operation: Option<F>,
    output: PhantomData<fn() -> T>,
}

impl<F, T> OnceTask<F, T> {
    fn new(operation: F) -> Self {
        Self {
            operation: Some(operation),
            output: PhantomData,
        }
    }
}

impl<F, T> Task for OnceTask<F, T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    type Output = T;

    fn run(&mut self, _: &TaskContext) -> TaskProgress<T> {
        let operation = self
            .operation
            .take()
            .expect("a one-shot task is dispatched once");
        TaskProgress::Done(operation())
    }
}

/// Completion status of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
    Cancelled,
    Panicked,
    Shutdown,
    QueueFull,
}

impl TaskError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Panicked => "panicked",
            Self::Shutdown => "shutdown",
            Self::QueueFull => "queue_full",
        }
    }
}

impl Display for TaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Error for TaskError {}

/// A refused scheduler operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    InvalidConfiguration(&'static str),
    QueueFull,
    Shutdown,
    WorkerSpawn(String),
}

impl Display for SchedulerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => {
                write!(formatter, "invalid scheduler configuration: {reason}")
            }
            Self::QueueFull => formatter.write_str("scheduler queue is full"),
            Self::Shutdown => formatter.write_str("scheduler is shut down"),
            Self::WorkerSpawn(reason) => {
                write!(formatter, "failed to spawn a scheduler worker: {reason}")
            }
        }
    }
}

impl Error for SchedulerError {}

/// A submitted task. Dropping the handle does not cancel the task.
pub struct TaskHandle<T> {
    receiver: Receiver<Result<T, TaskError>>,
    cancelled: Arc<AtomicBool>,
}

impl<T> TaskHandle<T> {
    /// Request cooperative cancellation. A queued task is refused before it
    /// runs; a running task observes it through [`TaskContext::is_cancelled`]
    /// or [`TaskContext::checkpoint`].
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Wait for the task to finish. A dropped scheduler or drained queue
    /// reports [`TaskError::Shutdown`].
    pub fn join(self) -> Result<T, TaskError> {
        self.receiver.recv().unwrap_or(Err(TaskError::Shutdown))
    }
}

/// Scheduler sizing and feedback policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub business_workers: usize,
    pub background_workers: usize,
    /// Bound on newly submitted queued tasks per pool. A running task's
    /// cooperative continuation may use one additional slot per worker.
    pub queue_capacity: usize,
    pub quantum: Duration,
    pub aging_interval: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        let available = thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1);
        Self {
            business_workers: available.saturating_sub(1).max(1),
            background_workers: 1,
            queue_capacity: 4096,
            quantum: Duration::from_millis(5),
            aging_interval: Duration::from_millis(50),
        }
    }
}

/// Two isolated worker pools with multi-level feedback scheduling.
pub struct Scheduler {
    business: Arc<Pool>,
    background: Arc<Pool>,
    workers: Mutex<Option<Vec<JoinHandle<()>>>>,
}

impl Scheduler {
    pub fn new(config: SchedulerConfig) -> Result<Self, SchedulerError> {
        if config.business_workers == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "business_workers must be positive",
            ));
        }
        if config.background_workers == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "background_workers must be positive",
            ));
        }
        if config.queue_capacity == 0 {
            return Err(SchedulerError::InvalidConfiguration(
                "queue_capacity must be positive",
            ));
        }
        if config.quantum.is_zero() {
            return Err(SchedulerError::InvalidConfiguration(
                "quantum must be positive",
            ));
        }
        if config.aging_interval.is_zero() {
            return Err(SchedulerError::InvalidConfiguration(
                "aging_interval must be positive",
            ));
        }
        let business = Arc::new(Pool::new(
            TaskClass::Business,
            config.queue_capacity,
            config.business_workers,
            config.quantum,
            config.aging_interval,
        ));
        let background = Arc::new(Pool::new(
            TaskClass::Background,
            config.queue_capacity,
            config.background_workers,
            config.quantum,
            config.aging_interval,
        ));
        let mut workers = Vec::with_capacity(config.business_workers + config.background_workers);
        if let Err(error) =
            spawn_workers(&business, config.business_workers, "business", &mut workers)
        {
            business.stop();
            for worker in workers {
                let _ = worker.join();
            }
            return Err(error);
        }
        if let Err(error) = spawn_workers(
            &background,
            config.background_workers,
            "background",
            &mut workers,
        ) {
            business.stop();
            background.stop();
            for worker in workers {
                let _ = worker.join();
            }
            return Err(error);
        }
        Ok(Self {
            business,
            background,
            workers: Mutex::new(Some(workers)),
        })
    }

    /// Submit one business task from a closure.
    pub fn spawn<T, F>(
        &self,
        key: TaskKey,
        priority: Priority,
        operation: F,
    ) -> Result<TaskHandle<T>, SchedulerError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_task(key, priority, OnceTask::new(operation))
    }

    /// Submit one business task with explicit cooperative progress.
    pub fn spawn_task<T: Task>(
        &self,
        key: TaskKey,
        priority: Priority,
        task: T,
    ) -> Result<TaskHandle<T::Output>, SchedulerError> {
        self.business.spawn(key, priority, task)
    }

    /// Submit one background task from a closure.
    pub fn spawn_background<T, F>(
        &self,
        key: TaskKey,
        priority: Priority,
        operation: F,
    ) -> Result<TaskHandle<T>, SchedulerError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn_background_task(key, priority, OnceTask::new(operation))
    }

    /// Submit one background task with explicit cooperative progress.
    pub fn spawn_background_task<T: Task>(
        &self,
        key: TaskKey,
        priority: Priority,
        task: T,
    ) -> Result<TaskHandle<T::Output>, SchedulerError> {
        self.background.spawn(key, priority, task)
    }

    /// Number of queued business tasks, including tasks that yielded.
    pub fn business_queued(&self) -> usize {
        self.business.queued()
    }

    /// Number of queued background tasks, including tasks that yielded.
    pub fn background_queued(&self) -> usize {
        self.background.queued()
    }

    /// Stop both pools and wait for their workers. Queued tasks complete with
    /// [`TaskError::Shutdown`]; a running task must cooperate to return.
    pub fn shutdown(&self) {
        self.business.stop();
        self.background.stop();
        let workers = take(&self.workers);
        if let Some(workers) = workers {
            for worker in workers {
                let _ = worker.join();
            }
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.shutdown();
    }
}

trait ErasedTask: Send {
    fn run(&mut self, context: &TaskContext) -> ErasedProgress;
    fn fail(&mut self, error: TaskError);
    fn publish(&mut self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErasedProgress {
    Done,
    Yield,
}

struct TypedTask<T: Task> {
    task: T,
    sender: Sender<Result<T::Output, TaskError>>,
    completion: Option<Result<T::Output, TaskError>>,
}

impl<T: Task> ErasedTask for TypedTask<T> {
    fn run(&mut self, context: &TaskContext) -> ErasedProgress {
        let result = catch_unwind(AssertUnwindSafe(|| self.task.run(context)));
        match result {
            Ok(TaskProgress::Done(output)) => {
                self.completion = Some(Ok(output));
                ErasedProgress::Done
            }
            Ok(TaskProgress::Yield) => ErasedProgress::Yield,
            Err(_) => {
                self.completion = Some(Err(TaskError::Panicked));
                ErasedProgress::Done
            }
        }
    }

    fn fail(&mut self, error: TaskError) {
        let _ = self.sender.send(Err(error));
    }

    fn publish(&mut self) {
        if let Some(result) = self.completion.take() {
            let _ = self.sender.send(result);
        }
    }
}

struct QueuedTask {
    key: TaskKey,
    class: TaskClass,
    priority: Priority,
    level: usize,
    enqueued_at: Instant,
    cancelled: Arc<AtomicBool>,
    task: Box<dyn ErasedTask>,
}

struct PoolState {
    levels: [VecDeque<QueuedTask>; LEVELS],
    feedback: HashMap<TaskKey, usize>,
    queued: usize,
}

impl PoolState {
    fn new() -> Self {
        Self {
            levels: std::array::from_fn(|_| VecDeque::new()),
            feedback: HashMap::new(),
            queued: 0,
        }
    }
}

struct Pool {
    class: TaskClass,
    queue_capacity: usize,
    worker_count: usize,
    quantum: Duration,
    aging_interval: Duration,
    state: Mutex<PoolState>,
    available: Condvar,
    stop: Arc<AtomicBool>,
}

impl Pool {
    fn new(
        class: TaskClass,
        queue_capacity: usize,
        worker_count: usize,
        quantum: Duration,
        aging_interval: Duration,
    ) -> Self {
        Self {
            class,
            queue_capacity,
            worker_count,
            quantum,
            aging_interval,
            state: Mutex::new(PoolState::new()),
            available: Condvar::new(),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn spawn<T: Task>(
        &self,
        key: TaskKey,
        priority: Priority,
        task: T,
    ) -> Result<TaskHandle<T::Output>, SchedulerError> {
        let (sender, receiver) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let queued = QueuedTask {
            key,
            class: self.class,
            priority,
            level: priority.level(),
            enqueued_at: Instant::now(),
            cancelled: Arc::clone(&cancelled),
            task: Box::new(TypedTask {
                task,
                sender,
                completion: None,
            }),
        };
        self.push(queued)?;
        Ok(TaskHandle {
            receiver,
            cancelled,
        })
    }

    fn push(&self, mut task: QueuedTask) -> Result<(), SchedulerError> {
        let mut state = lock(&self.state);
        if self.stop.load(Ordering::Acquire) {
            drop(state);
            drop_task(task);
            return Err(SchedulerError::Shutdown);
        }
        if state.queued >= self.queue_capacity {
            drop(state);
            drop_task(task);
            return Err(SchedulerError::QueueFull);
        }
        let base = task.priority.level();
        let level = state
            .feedback
            .get(&task.key)
            .copied()
            .unwrap_or(base)
            .max(base)
            .min(LEVELS - 1);
        task.level = level;
        state.levels[level].push_back(task);
        state.queued += 1;
        self.available.notify_one();
        Ok(())
    }

    fn queued(&self) -> usize {
        lock(&self.state).queued
    }

    fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.available.notify_all();
        let tasks = {
            let mut state = lock(&self.state);
            let mut tasks = Vec::new();
            for level in 0..LEVELS {
                tasks.extend(state.levels[level].drain(..));
            }
            state.queued = 0;
            tasks
        };
        for mut task in tasks {
            task.task.fail(TaskError::Shutdown);
            drop_task(task);
        }
    }

    fn worker_loop(self: Arc<Self>) {
        loop {
            let task = {
                let mut state = lock(&self.state);
                loop {
                    if self.stop.load(Ordering::Acquire) {
                        return;
                    }
                    promote_aged(&mut state, Instant::now(), self.aging_interval);
                    if let Some(task) = pop_locked(&mut state) {
                        break Some(task);
                    }
                    let (next, _) = self
                        .available
                        .wait_timeout(state, self.aging_interval)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                }
            };
            let Some(mut task) = task else {
                continue;
            };
            if self.stop.load(Ordering::Acquire) {
                task.task.fail(TaskError::Shutdown);
                drop_task(task);
                continue;
            }
            if task.cancelled.load(Ordering::Acquire) {
                task.task.fail(TaskError::Cancelled);
                drop_task(task);
                continue;
            }
            let context = TaskContext {
                key: task.key,
                class: task.class,
                priority: task.priority,
                level: task.level,
                cancelled: Arc::clone(&task.cancelled),
                shutting_down: Arc::clone(&self.stop),
            };
            let started = Instant::now();
            let outcome = task.task.run(&context);
            let elapsed = started.elapsed();
            if self.stop.load(Ordering::Acquire) {
                task.task.fail(TaskError::Shutdown);
                drop_task(task);
                continue;
            }
            let mut state = lock(&self.state);
            match outcome {
                ErasedProgress::Done => {
                    record_feedback(
                        &mut state,
                        task.key,
                        task.priority,
                        task.level,
                        elapsed,
                        false,
                        self.quantum,
                        self.queue_capacity,
                    );
                    drop(state);
                    task.task.publish();
                    drop_task(task);
                }
                ErasedProgress::Yield => {
                    if self.stop.load(Ordering::Acquire) {
                        drop(state);
                        task.task.fail(TaskError::Shutdown);
                        drop_task(task);
                        continue;
                    }
                    if task.cancelled.load(Ordering::Acquire) {
                        drop(state);
                        task.task.fail(TaskError::Cancelled);
                        drop_task(task);
                        continue;
                    }
                    record_feedback(
                        &mut state,
                        task.key,
                        task.priority,
                        task.level,
                        elapsed,
                        true,
                        self.quantum,
                        self.queue_capacity,
                    );
                    let next = (task.level + 1).min(LEVELS - 1);
                    task.level = next;
                    task.enqueued_at = Instant::now();
                    if state.queued >= self.queue_capacity.saturating_add(self.worker_count) {
                        drop(state);
                        task.task.fail(TaskError::QueueFull);
                        drop_task(task);
                        continue;
                    }
                    state.levels[next].push_back(task);
                    state.queued += 1;
                    self.available.notify_one();
                }
            }
        }
    }
}

fn drop_task(task: QueuedTask) {
    // A task-owned destructor must not retire the worker. The handle may
    // already hold a completion, but later submissions still need a live pool.
    let _ = catch_unwind(AssertUnwindSafe(|| drop(task)));
}

fn spawn_workers(
    pool: &Arc<Pool>,
    count: usize,
    kind: &'static str,
    workers: &mut Vec<JoinHandle<()>>,
) -> Result<(), SchedulerError> {
    for index in 0..count {
        let pool = Arc::clone(pool);
        let worker = thread::Builder::new()
            .name(format!("hawdb-scheduler-{kind}-{index}"))
            .spawn(move || pool.worker_loop())
            .map_err(|error| SchedulerError::WorkerSpawn(error.to_string()))?;
        workers.push(worker);
    }
    Ok(())
}

fn promote_aged(state: &mut PoolState, now: Instant, aging: Duration) {
    for level in 1..LEVELS {
        let mut promoted = Vec::new();
        {
            let queue = &mut state.levels[level];
            let mut index = 0;
            while index < queue.len() {
                if now.saturating_duration_since(queue[index].enqueued_at) >= aging {
                    promoted.push(queue.remove(index).expect("index is inside the queue"));
                } else {
                    index += 1;
                }
            }
        }
        for mut task in promoted {
            task.level -= 1;
            state.levels[level - 1].push_back(task);
        }
    }
}

fn pop_locked(state: &mut PoolState) -> Option<QueuedTask> {
    for level in 0..LEVELS {
        if let Some(task) = state.levels[level].pop_front() {
            state.queued -= 1;
            return Some(task);
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn record_feedback(
    state: &mut PoolState,
    key: TaskKey,
    priority: Priority,
    level: usize,
    elapsed: Duration,
    yielded: bool,
    quantum: Duration,
    queue_capacity: usize,
) {
    if state.feedback.len() >= queue_capacity && !state.feedback.contains_key(&key) {
        state.feedback.clear();
    }
    let base = priority.level();
    let next = if yielded || elapsed > quantum {
        (level + 1).min(LEVELS - 1)
    } else if elapsed <= quantum / 2 {
        level.saturating_sub(1).max(base)
    } else {
        level
    };
    state.feedback.insert(key, next.max(base).min(LEVELS - 1));
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn take<T>(mutex: &Mutex<Option<T>>) -> Option<T> {
    lock(mutex).take()
}

#[cfg(test)]
mod tests;
