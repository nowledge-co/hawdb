use skein_core::{
    Result as SkeinResult, RuntimeCancellationReason, RuntimeTaskContext, SkeinError,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

const ORDERED_STREAM_CHECK_INTERVAL: Duration = Duration::from_millis(10);
const SHARED_EXECUTOR_WORKER_LIMIT: usize = 16;

#[derive(Clone)]
pub struct SharedExecutorPool {
    inner: Arc<rayon::ThreadPool>,
    worker_count: NonZeroUsize,
}

impl fmt::Debug for SharedExecutorPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedExecutorPool")
            .field("worker_count", &self.worker_count)
            .finish_non_exhaustive()
    }
}

impl SharedExecutorPool {
    pub fn new(worker_count: NonZeroUsize) -> Result<Self, SharedExecutorPoolError> {
        let inner = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count.get())
            .thread_name(|index| format!("skein-executor-{index}"))
            .build()
            .map_err(|error| SharedExecutorPoolError(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(inner),
            worker_count,
        })
    }

    pub fn shared_default() -> Result<Self, SharedExecutorPoolError> {
        Self::shared_bounded(default_shared_worker_count())
    }

    pub fn shared_bounded(worker_limit: NonZeroUsize) -> Result<Self, SharedExecutorPoolError> {
        static SHARED: OnceLock<
            Mutex<BTreeMap<usize, Result<SharedExecutorPool, SharedExecutorPoolError>>>,
        > = OnceLock::new();
        let worker_count = default_shared_worker_count().min(worker_limit);
        let mut shared = SHARED
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shared
            .entry(worker_count.get())
            .or_insert_with(|| Self::new(worker_count))
            .clone()
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count.get()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedExecutorPoolError(String);

impl Display for SharedExecutorPoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to build shared executor pool: {}",
            self.0
        )
    }
}

impl Error for SharedExecutorPoolError {}

fn default_shared_worker_count() -> NonZeroUsize {
    std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::MIN)
        .min(
            NonZeroUsize::new(SHARED_EXECUTOR_WORKER_LIMIT)
                .expect("shared worker limit is non-zero"),
        )
}

#[derive(Debug, Clone)]
pub struct BoundedExecutor {
    max_parallelism: NonZeroUsize,
    pool: Result<SharedExecutorPool, SharedExecutorPoolError>,
}

impl BoundedExecutor {
    pub fn new(max_parallelism: NonZeroUsize) -> Self {
        Self {
            max_parallelism,
            pool: SharedExecutorPool::shared_bounded(max_parallelism),
        }
    }

    pub fn with_pool(max_parallelism: NonZeroUsize, pool: SharedExecutorPool) -> Self {
        Self {
            max_parallelism,
            pool: Ok(pool),
        }
    }

    pub fn max_parallelism(&self) -> usize {
        self.max_parallelism.get()
    }

    pub fn is_degraded(&self) -> bool {
        self.pool.is_err()
    }

    pub fn pool_error(&self) -> Option<&SharedExecutorPoolError> {
        self.pool.as_ref().err()
    }

    /// Materializes one output per input in input order, regardless of completion
    /// order. An output that is itself a `Result` does not short-circuit workers.
    /// This bounds worker concurrency, not the collected vector's memory.
    pub fn map_ordered<T, R, F>(self, inputs: &[T], operation: F) -> Vec<R>
    where
        T: Sync,
        R: Send,
        F: Fn(&T) -> R + Sync,
    {
        if inputs.is_empty() {
            return Vec::new();
        }
        let Ok(pool) = &self.pool else {
            return inputs.iter().map(operation).collect();
        };

        let worker_count = self.max_parallelism().min(inputs.len());
        let next = AtomicUsize::new(0);
        let outputs = Mutex::new(
            std::iter::repeat_with(|| None)
                .take(inputs.len())
                .collect::<Vec<Option<R>>>(),
        );

        let run = || {
            rayon::scope(|scope| {
                for _ in 0..worker_count {
                    scope.spawn(|_| loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(input) = inputs.get(index) else {
                            break;
                        };
                        let output = operation(input);
                        outputs
                            .lock()
                            .expect("bounded executor output lock should not be poisoned")[index] =
                            Some(output);
                    });
                }
            });
        };
        pool.inner.install(run);

        outputs
            .into_inner()
            .expect("bounded executor output lock should not be poisoned")
            .into_iter()
            .map(|output| output.expect("each bounded executor input must produce one output"))
            .collect()
    }

    /// The materializing map with cooperative cancellation checks. In-flight
    /// callbacks may finish after cancellation; no partial vector is returned.
    pub fn map_ordered_with_context<T, R, F>(
        self,
        inputs: &[T],
        context: &RuntimeTaskContext,
        operation: F,
    ) -> Result<Vec<R>, RuntimeCancellationReason>
    where
        T: Sync,
        R: Send,
        F: Fn(&T) -> R + Sync,
    {
        context.checkpoint()?;
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let Ok(pool) = &self.pool else {
            let output = inputs
                .iter()
                .map(|input| {
                    context.checkpoint()?;
                    Ok(operation(input))
                })
                .collect::<Result<Vec<_>, RuntimeCancellationReason>>()?;
            context.checkpoint()?;
            return Ok(output);
        };

        let worker_count = self.max_parallelism().min(inputs.len());
        let next = AtomicUsize::new(0);
        let stopped = Mutex::new(None);
        let outputs = Mutex::new(
            std::iter::repeat_with(|| None)
                .take(inputs.len())
                .collect::<Vec<Option<R>>>(),
        );

        let run = || {
            rayon::scope(|scope| {
                for _ in 0..worker_count {
                    scope.spawn(|_| loop {
                        if let Err(reason) = context.checkpoint() {
                            let mut stopped = stopped.lock().expect(
                                "bounded executor cancellation lock should not be poisoned",
                            );
                            stopped.get_or_insert(reason);
                            break;
                        }
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(input) = inputs.get(index) else {
                            break;
                        };
                        let output = operation(input);
                        outputs
                            .lock()
                            .expect("bounded executor output lock should not be poisoned")[index] =
                            Some(output);
                    });
                }
            });
        };
        pool.inner.install(run);

        if let Some(reason) = *stopped
            .lock()
            .expect("bounded executor cancellation lock should not be poisoned")
        {
            return Err(reason);
        }
        context.checkpoint()?;
        Ok(outputs
            .into_inner()
            .expect("bounded executor output lock should not be poisoned")
            .into_iter()
            .map(|output| output.expect("each bounded executor input must produce one output"))
            .collect())
    }

    pub(crate) fn try_for_each_index_ordered<R, F, C>(
        &self,
        input_count: usize,
        context: Option<&RuntimeTaskContext>,
        operation: F,
        mut consume: C,
    ) -> SkeinResult<BoundedOrderedStreamReport>
    where
        R: Send,
        F: Fn(usize) -> SkeinResult<R> + Sync,
        C: FnMut(usize, R) -> SkeinResult<BoundedOrderedStreamControl>,
    {
        runtime_checkpoint(context)?;
        if input_count == 0 {
            return Ok(BoundedOrderedStreamReport::default());
        }

        let Ok(pool) = &self.pool else {
            return run_sequential_index_stream(input_count, context, &operation, &mut consume);
        };
        let worker_count = self
            .max_parallelism()
            .min(pool.worker_count())
            .min(input_count);
        if worker_count == 1 {
            return run_sequential_index_stream(input_count, context, &operation, &mut consume);
        }

        let window = OrderedWorkWindow {
            state: Mutex::new(OrderedWorkState {
                next_index: 0,
                consumed_prefix: 0,
                stopped: false,
            }),
            changed: Condvar::new(),
        };
        let (sender, receiver) = mpsc::sync_channel(worker_count);
        let mut reorder = BTreeMap::new();
        let mut report = BoundedOrderedStreamReport::default();

        let result = pool.inner.in_place_scope(|scope| {
            for _ in 0..worker_count {
                let sender = sender.clone();
                let window = &window;
                let operation = &operation;
                scope.spawn(move |_| {
                    while let Some(index) =
                        claim_ordered_index(window, input_count, worker_count, context)
                    {
                    if let Err(reason) = context_checkpoint(context) {
                        let _ = sender.send(OrderedWorkerMessage::Stopped(reason));
                        break;
                    }
                    let output = catch_unwind(AssertUnwindSafe(|| operation(index)))
                        .unwrap_or_else(|_| {
                            Err(SkeinError::Execution(format!(
                                "bounded executor worker panicked at input index {index}"
                            )))
                        });
                    if sender
                        .send(OrderedWorkerMessage::Output(index, output))
                        .is_err()
                    {
                        break;
                    }
                    }
                });
            }
            drop(sender);

            let mut next_expected = 0usize;
            let result = 'receive: loop {
                if let Err(reason) = context_checkpoint(context) {
                    break Err(runtime_stopped_error(reason));
                }
                match receiver.recv_timeout(ORDERED_STREAM_CHECK_INTERVAL) {
                    Ok(OrderedWorkerMessage::Output(index, output)) => {
                        let output = match output {
                            Ok(output) => output,
                            Err(error) => break Err(error),
                        };
                        if reorder.insert(index, output).is_some() {
                            break Err(SkeinError::Execution(format!(
                                "bounded executor produced duplicate output index {index}"
                            )));
                        }
                        report.peak_reorder_entries =
                            report.peak_reorder_entries.max(reorder.len());

                        while let Some(output) = reorder.remove(&next_expected) {
                            let control = match catch_unwind(AssertUnwindSafe(|| {
                                consume(next_expected, output)
                            }))
                            .unwrap_or_else(|_| {
                                Err(SkeinError::Execution(format!(
                                    "bounded executor consumer panicked at input index {next_expected}"
                                )))
                            }) {
                                Ok(control) => control,
                                Err(error) => break 'receive Err(error),
                            };
                            next_expected = next_expected.saturating_add(1);
                            if control == BoundedOrderedStreamControl::Stop {
                                report.stopped_early = true;
                                break;
                            }
                            advance_ordered_prefix(&window, next_expected);
                        }
                        if report.stopped_early {
                            break Ok(report);
                        }
                        if next_expected == input_count {
                            break Ok(report);
                        }
                    }
                    Ok(OrderedWorkerMessage::Stopped(reason)) => {
                        break Err(runtime_stopped_error(reason));
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => {
                        break Err(SkeinError::Execution(format!(
                            "bounded executor stopped after {next_expected} of {input_count} ordered outputs"
                        )));
                    }
                }
            };

            stop_ordered_window(&window);
            drop(receiver);
            result
        });
        match result {
            Ok(report) => {
                runtime_checkpoint(context)?;
                Ok(report)
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundedOrderedStreamControl {
    Continue,
    Stop,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedOrderedStreamReport {
    pub(crate) peak_reorder_entries: usize,
    pub(crate) stopped_early: bool,
}

struct OrderedWorkWindow {
    state: Mutex<OrderedWorkState>,
    changed: Condvar,
}

struct OrderedWorkState {
    next_index: usize,
    consumed_prefix: usize,
    stopped: bool,
}

enum OrderedWorkerMessage<R> {
    Output(usize, SkeinResult<R>),
    Stopped(RuntimeCancellationReason),
}

fn run_sequential_index_stream<R, F, C>(
    input_count: usize,
    context: Option<&RuntimeTaskContext>,
    operation: &F,
    consume: &mut C,
) -> SkeinResult<BoundedOrderedStreamReport>
where
    F: Fn(usize) -> SkeinResult<R> + Sync,
    C: FnMut(usize, R) -> SkeinResult<BoundedOrderedStreamControl>,
{
    let mut report = BoundedOrderedStreamReport::default();
    for index in 0..input_count {
        runtime_checkpoint(context)?;
        let output = operation(index)?;
        report.peak_reorder_entries = 1;
        if consume(index, output)? == BoundedOrderedStreamControl::Stop {
            report.stopped_early = true;
            break;
        }
    }
    runtime_checkpoint(context)?;
    Ok(report)
}

fn claim_ordered_index(
    window: &OrderedWorkWindow,
    input_count: usize,
    worker_count: usize,
    context: Option<&RuntimeTaskContext>,
) -> Option<usize> {
    let mut state = lock_recover(&window.state);
    loop {
        if state.stopped || context_checkpoint(context).is_err() {
            state.stopped = true;
            window.changed.notify_all();
            return None;
        }
        let window_end = state.consumed_prefix.saturating_add(worker_count);
        if state.next_index < input_count && state.next_index < window_end {
            let index = state.next_index;
            state.next_index = state.next_index.saturating_add(1);
            return Some(index);
        }
        if state.next_index == input_count {
            return None;
        }
        let (next_state, _) = window
            .changed
            .wait_timeout(state, ORDERED_STREAM_CHECK_INTERVAL)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state = next_state;
    }
}

fn advance_ordered_prefix(window: &OrderedWorkWindow, consumed_prefix: usize) {
    let mut state = lock_recover(&window.state);
    state.consumed_prefix = state.consumed_prefix.max(consumed_prefix);
    window.changed.notify_all();
}

fn stop_ordered_window(window: &OrderedWorkWindow) {
    let mut state = lock_recover(&window.state);
    state.stopped = true;
    window.changed.notify_all();
}

fn context_checkpoint(
    context: Option<&RuntimeTaskContext>,
) -> Result<(), RuntimeCancellationReason> {
    context.map_or(Ok(()), RuntimeTaskContext::checkpoint)
}

fn runtime_checkpoint(context: Option<&RuntimeTaskContext>) -> SkeinResult<()> {
    context_checkpoint(context).map_err(runtime_stopped_error)
}

fn runtime_stopped_error(reason: RuntimeCancellationReason) -> SkeinError {
    SkeinError::Execution(format!("runtime task stopped: {reason}"))
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Default for BoundedExecutor {
    fn default() -> Self {
        Self::new(NonZeroUsize::MIN)
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedExecutor, SharedExecutorPool, SharedExecutorPoolError};
    use skein_core::{RuntimeCancellationReason, RuntimeCancellationToken, RuntimeTaskContext};
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parallel_map_preserves_input_order() {
        let executor = BoundedExecutor::new(NonZeroUsize::new(4).unwrap());
        let output = executor.map_ordered(&[3, 1, 4, 2], |value| value * value);
        assert_eq!(output, vec![9, 1, 16, 4]);
    }

    #[test]
    fn shared_pool_respects_the_requested_worker_limit() {
        let default_workers = SharedExecutorPool::shared_default().unwrap().worker_count();
        let bounded_workers = SharedExecutorPool::shared_bounded(NonZeroUsize::new(2).unwrap())
            .unwrap()
            .worker_count();

        assert_eq!(bounded_workers, default_workers.min(2));
    }

    #[test]
    fn bounded_executor_exposes_pool_degradation() {
        let executor = BoundedExecutor {
            max_parallelism: NonZeroUsize::new(2).unwrap(),
            pool: Err(SharedExecutorPoolError("injected failure".to_string())),
        };

        assert!(executor.is_degraded());
        assert!(executor.pool_error().is_some());
        assert_eq!(executor.map_ordered(&[1, 2], |value| value * 2), [2, 4]);
    }

    #[test]
    fn parallel_map_respects_the_worker_bound() {
        let executor = BoundedExecutor::new(NonZeroUsize::new(2).unwrap());
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        let output = executor.map_ordered(&[1, 2, 3, 4], |value| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(current, Ordering::SeqCst);
            std::thread::yield_now();
            active.fetch_sub(1, Ordering::SeqCst);
            value * 2
        });

        assert_eq!(output, vec![2, 4, 6, 8]);
        assert!(peak.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    fn controlled_parallel_map_stops_between_inputs() {
        let executor = BoundedExecutor::new(NonZeroUsize::MIN);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let visited = AtomicUsize::new(0);

        let result = executor.map_ordered_with_context(&[1, 2, 3], &context, |value| {
            visited.fetch_add(1, Ordering::SeqCst);
            if *value == 1 {
                token.cancel();
            }
            value * 2
        });

        assert_eq!(result, Err(RuntimeCancellationReason::Cancelled));
        assert_eq!(visited.load(Ordering::SeqCst), 1);
    }
}
