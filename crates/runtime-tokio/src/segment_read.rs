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

use super::TokioRuntimeAdapter;
use hawdb_core::{
    RuntimeCancellationReason, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
    RuntimeTaskContext,
};
use hawdb_storage::{
    SegmentRangeReader, SegmentReadControl, SegmentReadError, SegmentReadExecutionReport,
    SegmentReadPayload, SegmentReadSchedule,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::{poll_fn, Future};
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::task::{JoinError, JoinHandle};

// I/O-wave controllers expose try_acquire, not a readiness notification. Keep
// their bounded retry separate from the event-driven task admission queue.
const IO_WAVE_RETRY_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug)]
pub enum TokioSegmentReadExecutionError<E> {
    Read(SegmentReadError),
    Consume(E),
    Stopped(RuntimeCancellationReason),
    RuntimeIo(RuntimeIoWaveError),
    Join { artifact_id: u64, source: JoinError },
}

impl<E: Display> Display for TokioSegmentReadExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Consume(error) => write!(formatter, "segment payload consumer failed: {error}"),
            Self::Stopped(reason) => write!(formatter, "segment payload read stopped: {reason}"),
            Self::RuntimeIo(error) => Display::fmt(error, formatter),
            Self::Join { artifact_id, .. } => write!(
                formatter,
                "asynchronous segment artifact {artifact_id} read task failed"
            ),
        }
    }
}

impl<E: Error + 'static> Error for TokioSegmentReadExecutionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Consume(error) => Some(error),
            Self::Stopped(reason) => Some(reason),
            Self::RuntimeIo(error) => Some(error),
            Self::Join { source, .. } => Some(source),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokioSegmentReadExecutor {
    runtime: TokioRuntimeAdapter,
    max_wave_bytes: NonZeroU64,
}

struct PendingSegmentRead {
    index: usize,
    artifact_id: u64,
    task: JoinHandle<Result<SegmentReadPayload, SegmentReadError>>,
}

impl TokioSegmentReadExecutor {
    /// Creates an executor that submits portable positioned reads to Tokio's
    /// blocking lane.
    ///
    /// This executor must be awaited from asynchronous execution. Calling it
    /// through `block_on` inside `execute_blocking` can deadlock when the
    /// blocking lane is saturated by its outer task.
    pub fn new(runtime: TokioRuntimeAdapter, max_wave_bytes: NonZeroU64) -> Self {
        Self {
            runtime,
            max_wave_bytes,
        }
    }

    /// Reads every scheduled range and delivers payloads in schedule order.
    pub async fn execute<R, F, E>(
        self,
        reader: Arc<R>,
        schedule: &SegmentReadSchedule,
        context: &RuntimeTaskContext,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, TokioSegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader + Send + Sync + 'static,
        F: FnMut(SegmentReadPayload) -> Result<(), E> + Send,
    {
        self.execute_control(reader, schedule, context, |payload| {
            consume(payload).map(|()| SegmentReadControl::Continue)
        })
        .await
    }

    /// Reads every scheduled range until the consumer requests a clean stop.
    ///
    /// Dropping this future stops submission and payload delivery, but already
    /// submitted blocking reads retain their I/O capacity until they finish.
    pub async fn execute_control<R, F, E>(
        self,
        reader: Arc<R>,
        schedule: &SegmentReadSchedule,
        context: &RuntimeTaskContext,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, TokioSegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader + Send + Sync + 'static,
        F: FnMut(SegmentReadPayload) -> Result<SegmentReadControl, E> + Send,
    {
        checkpoint(context)?;
        let parallelism = context
            .executor_thread_limit()
            .map_or(context.admitted_parallelism(), |limit| {
                limit.min(context.admitted_parallelism())
            });
        let mut executed_wave_count = 0usize;
        let mut range_count = 0usize;
        let mut bytes_read = 0u64;
        let mut max_wave_bytes_read = 0u64;

        for (wave_index, wave) in schedule.waves.iter().enumerate() {
            checkpoint(context)?;
            let wave_bytes = wave
                .ranges
                .iter()
                .map(|range| range.length.get())
                .fold(0u64, u64::saturating_add);
            if wave_bytes > self.max_wave_bytes.get() {
                return Err(TokioSegmentReadExecutionError::Read(
                    SegmentReadError::WaveBudgetExceeded {
                        wave_index,
                        scheduled_bytes: wave_bytes,
                        max_wave_bytes: self.max_wave_bytes.get(),
                    },
                ));
            }

            let mut payloads = Vec::with_capacity(wave.ranges.len());
            for ranges in wave.ranges.chunks(parallelism.get()) {
                let slots =
                    NonZeroUsize::new(ranges.len()).expect("a segment read chunk is never empty");
                let io_permit = self.acquire_io_wave(context, slots).await?;
                payloads.extend(
                    self.read_chunk(Arc::clone(&reader), ranges, io_permit)
                        .await?,
                );
            }

            checkpoint(context)?;
            executed_wave_count = executed_wave_count.saturating_add(1);
            range_count = range_count.saturating_add(payloads.len());
            bytes_read = bytes_read.saturating_add(
                payloads
                    .iter()
                    .map(|payload| payload.range.length.get())
                    .fold(0u64, u64::saturating_add),
            );
            max_wave_bytes_read = max_wave_bytes_read.max(wave_bytes);
            let mut stopped = false;
            for payload in payloads {
                checkpoint(context)?;
                if consume(payload).map_err(TokioSegmentReadExecutionError::Consume)?
                    == SegmentReadControl::Stop
                {
                    stopped = true;
                    break;
                }
            }
            if stopped {
                break;
            }
        }

        checkpoint(context)?;
        Ok(SegmentReadExecutionReport {
            wave_count: executed_wave_count,
            range_count,
            bytes_read,
            max_wave_bytes_read,
        })
    }

    async fn acquire_io_wave<E>(
        &self,
        context: &RuntimeTaskContext,
        slots: NonZeroUsize,
    ) -> Result<Option<Box<dyn RuntimeIoWavePermit>>, TokioSegmentReadExecutionError<E>> {
        loop {
            match context
                .try_acquire_io_wave(slots)
                .map_err(map_runtime_io_error)?
            {
                RuntimeIoWaveTryAcquire::Acquired(permit) => return Ok(permit),
                RuntimeIoWaveTryAcquire::Pending => {
                    wait_for_io_retry(context, IO_WAVE_RETRY_INTERVAL)
                        .await
                        .map_err(TokioSegmentReadExecutionError::Stopped)?;
                }
            }
        }
    }

    async fn read_chunk<R, E>(
        &self,
        reader: Arc<R>,
        ranges: &[hawdb_storage::SegmentReadRange],
        io_permit: Option<Box<dyn RuntimeIoWavePermit>>,
    ) -> Result<Vec<SegmentReadPayload>, TokioSegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader + Send + Sync + 'static,
    {
        // Blocking tasks can outlive the awaiting future or host runtime. Each
        // task must retain capacity until its physical read finishes. The permit
        // is Send, not Sync; the mutex provides shared ownership without locking
        // or serializing the reads.
        let io_permit = Arc::new(Mutex::new(io_permit));
        let mut tasks = Vec::with_capacity(ranges.len());
        for (index, range) in ranges.iter().cloned().enumerate() {
            let artifact_id = range.artifact_id;
            let reader = Arc::clone(&reader);
            let io_permit = Arc::clone(&io_permit);
            let task = self.runtime.handle.spawn_blocking(move || {
                let _io_permit = io_permit;
                catch_unwind(AssertUnwindSafe(|| reader.read_range(&range)))
                    .map(|result| {
                        result.map(|bytes| SegmentReadPayload {
                            range: range.clone(),
                            bytes,
                        })
                    })
                    .unwrap_or_else(|_| Err(SegmentReadError::WorkerPanicked { artifact_id }))
            });
            tasks.push(PendingSegmentRead {
                index,
                artifact_id,
                task,
            });
        }

        collect_read_tasks(tasks).await
    }
}

async fn wait_for_io_retry(
    context: &RuntimeTaskContext,
    retry_interval: Duration,
) -> Result<(), RuntimeCancellationReason> {
    let delay = context
        .remaining()
        .map_or(retry_interval, |remaining| remaining.min(retry_interval));
    let mut retry = Box::pin(tokio::time::sleep(delay));
    let mut cancellation = Box::pin(context.cancellation().cancelled());
    poll_fn(|task| {
        if cancellation.as_mut().poll(task).is_ready() {
            return Poll::Ready(Err(RuntimeCancellationReason::Cancelled));
        }
        if retry.as_mut().poll(task).is_ready() {
            return Poll::Ready(context.checkpoint());
        }
        Poll::Pending
    })
    .await
}

async fn collect_read_tasks<E>(
    tasks: Vec<PendingSegmentRead>,
) -> Result<Vec<SegmentReadPayload>, TokioSegmentReadExecutionError<E>> {
    let mut results = (0..tasks.len())
        .map(|_| None)
        .collect::<Vec<Option<Result<SegmentReadPayload, SegmentReadError>>>>();
    let mut join_error = None;
    for PendingSegmentRead {
        index,
        artifact_id,
        task,
    } in tasks
    {
        match task.await {
            Ok(result) => results[index] = Some(result),
            Err(source) if join_error.is_none() => {
                join_error = Some(TokioSegmentReadExecutionError::Join {
                    artifact_id,
                    source,
                });
            }
            Err(_) => {}
        }
    }
    if let Some(error) = join_error {
        return Err(error);
    }
    results
        .into_iter()
        .map(|result| {
            result
                .expect("every asynchronous segment read task returned a result")
                .map_err(TokioSegmentReadExecutionError::Read)
        })
        .collect()
}

fn checkpoint<E>(context: &RuntimeTaskContext) -> Result<(), TokioSegmentReadExecutionError<E>> {
    context
        .checkpoint()
        .map_err(TokioSegmentReadExecutionError::Stopped)
}

fn map_runtime_io_error<E>(error: RuntimeIoWaveError) -> TokioSegmentReadExecutionError<E> {
    match error {
        RuntimeIoWaveError::Stopped(reason) => TokioSegmentReadExecutionError::Stopped(reason),
        error => TokioSegmentReadExecutionError::RuntimeIo(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
    };
    use hawdb_storage::{SegmentReadRange, SegmentReadScheduler};
    use std::convert::Infallible;
    use std::future::{poll_fn, Future};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    struct TrackingReader {
        active: AtomicUsize,
        peak: AtomicUsize,
        reads: AtomicUsize,
    }

    struct PartiallyFailingReader {
        active: AtomicUsize,
        completed: AtomicUsize,
    }

    struct PanickingReader;

    struct GatedReader {
        started: mpsc::Sender<u64>,
        completed: mpsc::Sender<u64>,
        releases: Vec<Mutex<mpsc::Receiver<()>>>,
        active: AtomicUsize,
        peak: AtomicUsize,
        reads: AtomicUsize,
    }

    struct ReadGates {
        started: mpsc::Receiver<u64>,
        completed: mpsc::Receiver<u64>,
        releases: Vec<Option<mpsc::Sender<()>>>,
    }

    impl GatedReader {
        fn new() -> (Arc<Self>, ReadGates) {
            let (started_tx, started_rx) = mpsc::channel();
            let (completed_tx, completed_rx) = mpsc::channel();
            let (releases_tx, releases_rx) = (0..2)
                .map(|_| {
                    let (tx, rx) = mpsc::channel();
                    (Some(tx), Mutex::new(rx))
                })
                .unzip();
            (
                Arc::new(Self {
                    started: started_tx,
                    completed: completed_tx,
                    releases: releases_rx,
                    active: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    reads: AtomicUsize::new(0),
                }),
                ReadGates {
                    started: started_rx,
                    completed: completed_rx,
                    releases: releases_tx,
                },
            )
        }
    }

    impl SegmentRangeReader for GatedReader {
        fn read_range(
            &self,
            range: &SegmentReadRange,
        ) -> Result<hawdb_storage::SegmentBytes, SegmentReadError> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(active, Ordering::AcqRel);
            self.reads.fetch_add(1, Ordering::AcqRel);
            let _ = self.started.send(range.artifact_id);
            if let Some(release) = self.releases.get(range.artifact_id as usize) {
                // Disconnecting the sender also releases readers during a test panic.
                let _ = release.lock().unwrap().recv();
            }
            self.active.fetch_sub(1, Ordering::AcqRel);
            let _ = self.completed.send(range.artifact_id);
            Ok(vec![0; range.length.get() as usize].into())
        }
    }

    impl ReadGates {
        fn wait_until_started(&self) {
            let mut started = (0..2)
                .map(|_| {
                    self.started
                        .recv_timeout(Duration::from_secs(5))
                        .expect("both reads must start before either is released")
                })
                .collect::<Vec<_>>();
            started.sort_unstable();
            assert_eq!(started, vec![0, 1]);
        }

        fn assert_permit_retained_until_last_read(mut self, governor: &RuntimeGovernor) {
            assert_eq!(governor.snapshot().active_foreground_io_slots, 2);
            drop(self.releases[0].take());
            assert_eq!(
                self.completed.recv_timeout(Duration::from_secs(5)).unwrap(),
                0
            );
            assert_eq!(governor.snapshot().active_foreground_io_slots, 2);
            drop(self.releases[1].take());
            assert_eq!(
                self.completed.recv_timeout(Duration::from_secs(5)).unwrap(),
                1
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while governor.snapshot().active_foreground_io_slots != 0 {
                assert!(Instant::now() < deadline, "the I/O-wave permit leaked");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    impl SegmentRangeReader for TrackingReader {
        fn read_range(
            &self,
            range: &SegmentReadRange,
        ) -> Result<hawdb_storage::SegmentBytes, SegmentReadError> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.reads.fetch_add(1, Ordering::AcqRel);
            self.peak.fetch_max(active, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(10));
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(vec![range.artifact_id as u8; range.length.get() as usize].into())
        }
    }

    impl SegmentRangeReader for PartiallyFailingReader {
        fn read_range(
            &self,
            range: &SegmentReadRange,
        ) -> Result<hawdb_storage::SegmentBytes, SegmentReadError> {
            self.active.fetch_add(1, Ordering::AcqRel);
            let result = if range.artifact_id == 1 {
                Err(SegmentReadError::ArtifactNotFound {
                    artifact_id: range.artifact_id,
                })
            } else {
                std::thread::sleep(Duration::from_millis(20));
                Ok(vec![0; range.length.get() as usize].into())
            };
            self.completed.fetch_add(1, Ordering::AcqRel);
            self.active.fetch_sub(1, Ordering::AcqRel);
            result
        }
    }

    impl SegmentRangeReader for PanickingReader {
        fn read_range(
            &self,
            _range: &SegmentReadRange,
        ) -> Result<hawdb_storage::SegmentBytes, SegmentReadError> {
            panic!("injected asynchronous segment read panic");
        }
    }

    fn governor() -> RuntimeGovernor {
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(
                Some(8 * 1024 * 1024 * 1024),
                Some(4 * 1024 * 1024 * 1024),
                None,
                None,
                None,
            ),
        );
        RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            resources,
            IoConcurrencyBudget::new(2, 1),
        )
    }

    fn runtime() -> TokioRuntimeAdapter {
        let governor = governor();
        let config = super::super::TokioRuntimeConfig::from_governor(&governor);
        TokioRuntimeAdapter::owned(governor, config).unwrap()
    }

    #[test]
    fn io_retry_is_woken_immediately_by_parent_cancellation() {
        struct CountWake(AtomicUsize);

        impl std::task::Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }

        let runtime = runtime();
        let token = hawdb_core::RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.child());
        runtime
            .block_on(async {
                // A long timer makes the notification assertion independent of
                // the production retry interval or wall-clock scheduling noise.
                let mut retry = Box::pin(wait_for_io_retry(&context, Duration::from_secs(3600)));
                let wake = Arc::new(CountWake(AtomicUsize::new(0)));
                let waker = std::task::Waker::from(Arc::clone(&wake));
                let mut task = std::task::Context::from_waker(&waker);
                assert!(retry.as_mut().poll(&mut task).is_pending());
                assert_eq!(wake.0.load(Ordering::Acquire), 0);
                token.cancel();
                assert!(wake.0.load(Ordering::Acquire) > 0);
                assert_eq!(
                    retry.as_mut().poll(&mut task),
                    Poll::Ready(Err(RuntimeCancellationReason::Cancelled))
                );
            })
            .unwrap();
    }

    #[test]
    fn io_retry_is_shortened_to_the_task_deadline() {
        let runtime = runtime();
        let result = runtime
            .block_on(async {
                let context = RuntimeTaskContext::with_timeout(Duration::from_millis(10));
                tokio::time::timeout(
                    Duration::from_secs(1),
                    wait_for_io_retry(&context, Duration::from_secs(3600)),
                )
                .await
            })
            .unwrap()
            .expect("the task deadline must win over the I/O retry timer");
        assert_eq!(result, Err(RuntimeCancellationReason::DeadlineExceeded));
    }

    #[test]
    fn saturated_io_retry_resumes_after_capacity_is_released() {
        let runtime = runtime();
        let permit = runtime
            .governor()
            .try_admit(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0).with_io_wave_slots(2),
            )
            .unwrap();
        let context = permit.bind_task_context(RuntimeTaskContext::default());
        let held = context
            .acquire_io_wave(NonZeroUsize::new(2).unwrap())
            .unwrap();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::MIN);
        runtime
            .block_on(async {
                let mut pending =
                    Box::pin(executor.acquire_io_wave::<Infallible>(&context, NonZeroUsize::MIN));
                poll_fn(|task| {
                    assert!(pending.as_mut().poll(task).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(held);
                let acquired = tokio::time::timeout(Duration::from_secs(1), pending)
                    .await
                    .expect("released I/O capacity must become available")
                    .unwrap();
                assert!(acquired.is_some());
                drop(acquired);
                assert!(matches!(
                    context
                        .try_acquire_io_wave(NonZeroUsize::new(2).unwrap())
                        .unwrap(),
                    RuntimeIoWaveTryAcquire::Acquired(Some(_))
                ));
            })
            .unwrap();
        drop(permit);
        assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 0);
    }

    #[test]
    fn cancellation_while_io_is_saturated_does_not_submit_reads() {
        let runtime = runtime();
        let token = hawdb_core::RuntimeCancellationToken::new();
        let permit = runtime
            .governor()
            .try_admit(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0).with_io_wave_slots(2),
            )
            .unwrap();
        let context = permit.bind_task_context(RuntimeTaskContext::without_deadline(token.child()));
        let held = context
            .acquire_io_wave(NonZeroUsize::new(2).unwrap())
            .unwrap();
        let reader = Arc::new(TrackingReader {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
        });
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::MIN);
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::MIN)
            .schedule([SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN)]);
        runtime
            .block_on(async {
                let mut pending = Box::pin(executor.execute(
                    Arc::clone(&reader),
                    &schedule,
                    &context,
                    |_| -> Result<(), Infallible> { panic!("cancelled reads must not deliver") },
                ));
                poll_fn(|task| {
                    assert!(pending.as_mut().poll(task).is_pending());
                    Poll::Ready(())
                })
                .await;
                token.cancel();
                assert!(matches!(
                    pending.await,
                    Err(TokioSegmentReadExecutionError::Stopped(
                        RuntimeCancellationReason::Cancelled
                    ))
                ));
            })
            .unwrap();
        assert_eq!(reader.reads.load(Ordering::Acquire), 0);
        assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 2);
        drop(held);
        drop(permit);
        assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 0);
    }

    #[test]
    fn reads_each_wave_concurrently_and_preserves_schedule_order() {
        for completion_order in [[0, 1], [1, 0]] {
            let runtime = runtime();
            let executor =
                TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::new(8).unwrap());
            let (reader, mut gates) = GatedReader::new();
            let schedule =
                SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN).schedule(
                    [
                        SegmentReadRange::new(1, 1, 1, NonZeroU64::MIN),
                        SegmentReadRange::new(0, 0, 0, NonZeroU64::MIN),
                    ],
                );
            let observed = Arc::new(Mutex::new(Vec::new()));
            let captured = Arc::clone(&observed);
            let tracked_reader = Arc::clone(&reader);
            let executing_runtime = runtime.clone();
            let task = runtime.handle.spawn(async move {
                executing_runtime
                    .execute_async(
                        RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0)
                            .with_cpu_slots(2)
                            .with_io_wave_slots(2),
                        RuntimeTaskContext::default(),
                        move |context| async move {
                            executor
                                .execute(tracked_reader, &schedule, &context, |payload| {
                                    captured
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .push(payload.range.artifact_id);
                                    Ok::<(), Infallible>(())
                                })
                                .await
                        },
                    )
                    .await
            });

            // Hold both reads open until their entry is observed. Sleeping in a
            // reader cannot guarantee overlap when blocking threads start late.
            gates.wait_until_started();
            assert_eq!(reader.active.load(Ordering::Acquire), 2);
            assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 2);
            for artifact_id in completion_order {
                drop(gates.releases[artifact_id].take());
                assert_eq!(
                    gates
                        .completed
                        .recv_timeout(Duration::from_secs(5))
                        .expect("the released read must finish"),
                    artifact_id as u64,
                );
            }
            let result = runtime.block_on(task).unwrap().unwrap().unwrap();

            assert_eq!(result.wave_count, 1);
            assert_eq!(result.range_count, 2);
            assert_eq!(result.bytes_read, 2);
            assert_eq!(result.max_wave_bytes_read, 2);
            assert_eq!(reader.peak.load(Ordering::Acquire), 2);
            assert_eq!(reader.active.load(Ordering::Acquire), 0);
            assert_eq!(reader.reads.load(Ordering::Acquire), 2);
            assert_eq!(
                *observed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                vec![0, 1],
            );
            assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 0);
        }
    }

    #[test]
    fn rejects_wave_budget_before_submitting_reads() {
        let runtime = runtime();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::MIN);
        let reader = Arc::new(TrackingReader {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
        });
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::new(2).unwrap())
            .schedule([SegmentReadRange::new(1, 1, 0, NonZeroU64::new(2).unwrap())]);

        let error = runtime
            .block_on(executor.execute(
                Arc::clone(&reader),
                &schedule,
                &RuntimeTaskContext::default(),
                |_| Ok::<(), Infallible>(()),
            ))
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            error,
            TokioSegmentReadExecutionError::Read(SegmentReadError::WaveBudgetExceeded {
                wave_index: 0,
                scheduled_bytes: 2,
                max_wave_bytes: 1,
            })
        ));
        assert_eq!(reader.reads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cancelled_context_does_not_submit_reads() {
        let runtime = runtime();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::MIN);
        let reader = Arc::new(TrackingReader {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
        });
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::MIN)
            .schedule([SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN)]);
        let token = hawdb_core::RuntimeCancellationToken::new();
        token.cancel();
        let context = RuntimeTaskContext::without_deadline(token);

        let error = runtime
            .block_on(
                executor.execute(Arc::clone(&reader), &schedule, &context, |_| {
                    Ok::<(), Infallible>(())
                }),
            )
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            error,
            TokioSegmentReadExecutionError::Stopped(RuntimeCancellationReason::Cancelled)
        ));
        assert_eq!(reader.reads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn read_error_joins_all_submitted_reads_before_returning() {
        let runtime = runtime();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::new(2).unwrap());
        let reader = Arc::new(PartiallyFailingReader {
            active: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        });
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
            .schedule([
                SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN),
                SegmentReadRange::new(2, 2, 1, NonZeroU64::MIN),
            ]);
        let tracked_reader = Arc::clone(&reader);

        let error = runtime
            .block_on(
                runtime.execute_async(
                    RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0)
                        .with_cpu_slots(2)
                        .with_io_wave_slots(2),
                    RuntimeTaskContext::default(),
                    move |context| async move {
                        executor
                            .execute(tracked_reader, &schedule, &context, |_| {
                                Ok::<(), Infallible>(())
                            })
                            .await
                    },
                ),
            )
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            error,
            super::super::TokioTaskError::Operation(TokioSegmentReadExecutionError::Read(
                SegmentReadError::ArtifactNotFound { artifact_id: 1 },
            ))
        ));
        assert_eq!(reader.completed.load(Ordering::Acquire), 2);
        assert_eq!(reader.active.load(Ordering::Acquire), 0);
        assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 0);
    }

    #[test]
    fn reader_panic_is_returned_as_a_typed_read_error() {
        let runtime = runtime();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::MIN);
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::MIN)
            .schedule([SegmentReadRange::new(7, 1, 0, NonZeroU64::MIN)]);

        let error = runtime
            .block_on(executor.execute(
                Arc::new(PanickingReader),
                &schedule,
                &RuntimeTaskContext::default(),
                |_| Ok::<(), Infallible>(()),
            ))
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            error,
            TokioSegmentReadExecutionError::Read(SegmentReadError::WorkerPanicked {
                artifact_id: 7,
            })
        ));
    }

    #[test]
    fn dropping_execute_future_retains_in_flight_io_capacity() {
        let runtime = runtime();
        let permit = runtime
            .governor()
            .try_admit(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0)
                    .with_cpu_slots(2)
                    .with_io_wave_slots(2),
            )
            .unwrap();
        let context = permit.bind_task_context(RuntimeTaskContext::default());
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::new(2).unwrap());
        let (reader, gates) = GatedReader::new();
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
            .schedule((0..3).map(|id| SegmentReadRange::new(id, id, 0, NonZeroU64::MIN)));
        let mut execute = Box::pin(executor.execute(
            Arc::clone(&reader),
            &schedule,
            &context,
            |_| -> Result<(), Infallible> { panic!("dropped reads must not deliver payloads") },
        ));
        runtime
            .block_on(poll_fn(|cx| {
                assert!(execute.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            }))
            .unwrap();
        gates.wait_until_started();

        drop(execute);

        assert!(matches!(
            context.try_acquire_io_wave(NonZeroUsize::MIN).unwrap(),
            RuntimeIoWaveTryAcquire::Pending
        ));
        gates.assert_permit_retained_until_last_read(runtime.governor());
        assert_eq!(reader.reads.load(Ordering::Acquire), 2);
    }

    #[test]
    fn borrowed_runtime_shutdown_retains_in_flight_io_capacity() {
        let host = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let governor = governor();
        let runtime = TokioRuntimeAdapter::borrowed(
            host.handle().clone(),
            governor.clone(),
            super::super::TokioRuntimeConfig::from_governor(&governor),
        );
        let permit = governor
            .try_admit(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Foreground, 2, 0)
                    .with_cpu_slots(2)
                    .with_io_wave_slots(2),
            )
            .unwrap();
        let context = permit.bind_task_context(RuntimeTaskContext::default());
        let executor = TokioSegmentReadExecutor::new(runtime, NonZeroU64::new(2).unwrap());
        let (reader, gates) = GatedReader::new();
        let tracked_reader = Arc::clone(&reader);
        let task = host.spawn(async move {
            let schedule =
                SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
                    .schedule((0..3).map(|id| SegmentReadRange::new(id, id, 0, NonZeroU64::MIN)));
            executor
                .execute(
                    tracked_reader,
                    &schedule,
                    &context,
                    |_| -> Result<(), Infallible> {
                        panic!("shutdown reads must not deliver payloads")
                    },
                )
                .await
        });
        gates.wait_until_started();

        host.shutdown_background();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !task.is_finished() {
            assert!(Instant::now() < deadline, "the host task was not cancelled");
            std::thread::sleep(Duration::from_millis(1));
        }
        gates.assert_permit_retained_until_last_read(&governor);
        assert_eq!(reader.reads.load(Ordering::Acquire), 2);
    }
}
