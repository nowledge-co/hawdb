use super::TokioRuntimeAdapter;
use skein_core::{
    RuntimeCancellationReason, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeIoWaveTryAcquire,
    RuntimeTaskContext,
};
use skein_storage::{
    SegmentRangeReader, SegmentReadControl, SegmentReadError, SegmentReadExecutionReport,
    SegmentReadPayload, SegmentReadSchedule,
};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::{NonZeroU64, NonZeroUsize};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use tokio::task::{JoinError, JoinHandle};

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
                let _io_permit = self.acquire_io_wave(context, slots).await?;
                payloads.extend(self.read_chunk(Arc::clone(&reader), ranges).await?);
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
                    tokio::time::sleep(self.runtime.poll_interval(context)).await;
                }
            }
        }
    }

    async fn read_chunk<R, E>(
        &self,
        reader: Arc<R>,
        ranges: &[skein_storage::SegmentReadRange],
    ) -> Result<Vec<SegmentReadPayload>, TokioSegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader + Send + Sync + 'static,
    {
        let mut tasks = Vec::with_capacity(ranges.len());
        for (index, range) in ranges.iter().cloned().enumerate() {
            let artifact_id = range.artifact_id;
            let reader = Arc::clone(&reader);
            let task = self.runtime.handle.spawn_blocking(move || {
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
    use skein_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
    };
    use skein_storage::{SegmentReadRange, SegmentReadScheduler};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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

    impl SegmentRangeReader for TrackingReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.reads.fetch_add(1, Ordering::AcqRel);
            self.peak.fetch_max(active, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(10));
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(Arc::from(vec![
                range.artifact_id as u8;
                range.length.get() as usize
            ]))
        }
    }

    impl SegmentRangeReader for PartiallyFailingReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
            self.active.fetch_add(1, Ordering::AcqRel);
            let result = if range.artifact_id == 1 {
                Err(SegmentReadError::ArtifactNotFound {
                    artifact_id: range.artifact_id,
                })
            } else {
                std::thread::sleep(Duration::from_millis(20));
                Ok(Arc::from(vec![0; range.length.get() as usize]))
            };
            self.completed.fetch_add(1, Ordering::AcqRel);
            self.active.fetch_sub(1, Ordering::AcqRel);
            result
        }
    }

    impl SegmentRangeReader for PanickingReader {
        fn read_range(&self, _range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
            panic!("injected asynchronous segment read panic");
        }
    }

    fn runtime() -> TokioRuntimeAdapter {
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
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            resources,
            IoConcurrencyBudget::new(2, 1),
        );
        let config = super::super::TokioRuntimeConfig::from_governor(&governor);
        TokioRuntimeAdapter::owned(governor, config).unwrap()
    }

    #[test]
    fn reads_each_wave_concurrently_and_preserves_schedule_order() {
        let runtime = runtime();
        let executor = TokioSegmentReadExecutor::new(runtime.clone(), NonZeroU64::new(8).unwrap());
        let reader = Arc::new(TrackingReader {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
        });
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
            .schedule([
                SegmentReadRange::new(2, 2, 1, NonZeroU64::MIN),
                SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN),
            ]);
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let tracked_reader = Arc::clone(&reader);
        let result = runtime
            .block_on(
                runtime.execute_async(
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
                ),
            )
            .unwrap()
            .unwrap();

        assert_eq!(result.wave_count, 1);
        assert_eq!(result.range_count, 2);
        assert_eq!(reader.peak.load(Ordering::Acquire), 2);
        assert_eq!(reader.reads.load(Ordering::Acquire), 2);
        assert_eq!(
            *observed
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![1, 2]
        );
        assert_eq!(runtime.governor_snapshot().active_foreground_io_slots, 0);
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
        let token = skein_core::RuntimeCancellationToken::new();
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
}
