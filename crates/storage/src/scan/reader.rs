use super::{SegmentReadRange, SegmentReadSchedule};
use crate::{
    content_digest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheError,
    SegmentCacheKey, StoreId,
};
use skein_core::{RuntimeCancellationReason, RuntimeIoWaveError, RuntimeTaskContext};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::ErrorKind;
#[cfg(not(any(unix, windows)))]
use std::io::Read;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Debug)]
pub enum SegmentReadError {
    ArtifactNotFound {
        artifact_id: u64,
    },
    WaveBudgetExceeded {
        wave_index: usize,
        scheduled_bytes: u64,
        max_wave_bytes: u64,
    },
    RangeTooLarge {
        artifact_id: u64,
        length: u64,
    },
    DigestMismatch {
        artifact_id: u64,
        segment_id: u64,
    },
    Cache {
        artifact_id: u64,
        source: SegmentCacheError,
    },
    Io {
        artifact_id: u64,
        offset: u64,
        length: u64,
        source: std::io::Error,
    },
    WorkerPanicked {
        artifact_id: u64,
    },
}

impl Display for SegmentReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactNotFound { artifact_id } => {
                write!(formatter, "segment artifact {artifact_id} is not registered")
            }
            Self::WaveBudgetExceeded {
                wave_index,
                scheduled_bytes,
                max_wave_bytes,
            } => write!(
                formatter,
                "segment read wave {wave_index} schedules {scheduled_bytes} bytes, exceeding the {max_wave_bytes} byte budget"
            ),
            Self::RangeTooLarge {
                artifact_id,
                length,
            } => write!(
                formatter,
                "segment artifact {artifact_id} range length {length} exceeds the platform address space"
            ),
            Self::DigestMismatch {
                artifact_id,
                segment_id,
            } => write!(
                formatter,
                "segment artifact {artifact_id} segment {segment_id} failed content digest verification"
            ),
            Self::Cache { artifact_id, .. } => {
                write!(formatter, "segment artifact {artifact_id} cache operation failed")
            }
            Self::Io {
                artifact_id,
                offset,
                length,
                ..
            } => write!(
                formatter,
                "segment artifact {artifact_id} range read failed at offset {offset} for {length} bytes"
            ),
            Self::WorkerPanicked { artifact_id } => {
                write!(formatter, "segment artifact {artifact_id} range reader panicked")
            }
        }
    }
}

impl Error for SegmentReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Cache { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum SegmentReadExecutionError<E> {
    Read(SegmentReadError),
    Consume(E),
    Stopped(RuntimeCancellationReason),
    RuntimeIo(RuntimeIoWaveError),
}

impl<E: Display> Display for SegmentReadExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => Display::fmt(error, formatter),
            Self::Consume(error) => write!(formatter, "segment payload consumer failed: {error}"),
            Self::Stopped(reason) => write!(formatter, "segment payload read stopped: {reason}"),
            Self::RuntimeIo(error) => Display::fmt(error, formatter),
        }
    }
}

impl<E: Error + 'static> Error for SegmentReadExecutionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Consume(error) => Some(error),
            Self::Stopped(reason) => Some(reason),
            Self::RuntimeIo(error) => Some(error),
        }
    }
}

pub trait SegmentRangeReader: Sync {
    fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError>;
}

#[derive(Debug, Clone)]
pub struct SegmentRangeRead {
    pub payload: Arc<[u8]>,
    pub cache_hit: bool,
    pub cache_miss: bool,
}

#[derive(Debug, Clone, Default)]
pub struct FileSegmentRangeReader {
    artifacts: BTreeMap<u64, Arc<RegisteredArtifact>>,
    cache: Option<Arc<SegmentCache>>,
    store_id: StoreId,
    manifest_generation: ManifestGeneration,
}

#[derive(Debug)]
struct RegisteredArtifact {
    path: PathBuf,
    file: OnceLock<File>,
}

impl FileSegmentRangeReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cache(
        mut self,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
        manifest_generation: ManifestGeneration,
    ) -> Self {
        self.cache = Some(cache);
        self.store_id = store_id;
        self.manifest_generation = manifest_generation;
        self
    }

    pub fn register(&mut self, artifact_id: u64, path: impl Into<PathBuf>) -> Option<PathBuf> {
        self.artifacts
            .insert(
                artifact_id,
                Arc::new(RegisteredArtifact {
                    path: path.into(),
                    file: OnceLock::new(),
                }),
            )
            .map(|artifact| artifact.path.clone())
    }

    pub fn read_range_with_report(
        &self,
        range: &SegmentReadRange,
    ) -> Result<SegmentRangeRead, SegmentReadError> {
        let artifact =
            self.artifacts
                .get(&range.artifact_id)
                .ok_or(SegmentReadError::ArtifactNotFound {
                    artifact_id: range.artifact_id,
                })?;
        let cache_key = range
            .content_digest
            .zip(
                (range.segment_ids.len() == 1)
                    .then(|| range.segment_ids.first().copied())
                    .flatten(),
            )
            .map(|(content_digest, segment_id)| SegmentCacheKey {
                store_id: self.store_id,
                manifest_generation: self.manifest_generation,
                segment_id,
                content_digest,
                representation: RepresentationKind::RawBytes,
            });
        if let (Some(cache), Some(key)) = (&self.cache, cache_key)
            && let Some(lease) = cache.get(&key)
        {
            return Ok(SegmentRangeRead {
                payload: lease.into_arc(),
                cache_hit: true,
                cache_miss: false,
            });
        }
        let cache_miss = self.cache.is_some() && cache_key.is_some();
        let length =
            usize::try_from(range.length.get()).map_err(|_| SegmentReadError::RangeTooLarge {
                artifact_id: range.artifact_id,
                length: range.length.get(),
            })?;
        let file = match artifact.file.get() {
            Some(file) => file,
            None => {
                let opened =
                    File::open(&artifact.path).map_err(|source| range_io_error(range, source))?;
                let _ = artifact.file.set(opened);
                artifact
                    .file
                    .get()
                    .expect("the current or a concurrent reader opened the artifact")
            }
        };
        let mut payload = vec![0; length];
        read_exact_at(file, &mut payload, range.offset)
            .map_err(|source| range_io_error(range, source))?;
        let payload: Arc<[u8]> = payload.into();
        if let Some(expected) = range.content_digest
            && content_digest(&payload) != expected
        {
            if let Some(cache) = &self.cache {
                cache.record_digest_mismatch();
            }
            return Err(SegmentReadError::DigestMismatch {
                artifact_id: range.artifact_id,
                segment_id: range.segment_ids.first().copied().unwrap_or_default(),
            });
        }
        if let (Some(cache), Some(key)) = (&self.cache, cache_key) {
            match cache.insert(key, Arc::clone(&payload)) {
                Ok(lease) => {
                    return Ok(SegmentRangeRead {
                        payload: lease.into_arc(),
                        cache_hit: false,
                        cache_miss,
                    });
                }
                Err(SegmentCacheError::EntryTooLarge { .. })
                | Err(SegmentCacheError::PinnedCapacity { .. }) => {}
                Err(source) => {
                    return Err(SegmentReadError::Cache {
                        artifact_id: range.artifact_id,
                        source,
                    });
                }
            }
        }
        Ok(SegmentRangeRead {
            payload,
            cache_hit: false,
            cache_miss,
        })
    }
}

impl SegmentRangeReader for FileSegmentRangeReader {
    fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
        self.read_range_with_report(range).map(|read| read.payload)
    }
}

#[cfg(unix)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buffer, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(buffer, offset)
}

#[cfg(not(any(unix, windows)))]
fn read_at(file: &File, buffer: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::io::{Seek, SeekFrom};

    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read(buffer)
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buffer.is_empty() {
        match read_at(file, buffer, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "segment range ended before the admitted length",
                ));
            }
            Ok(read) => {
                offset = offset.saturating_add(read as u64);
                buffer = &mut buffer[read..];
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadPayload {
    pub range: SegmentReadRange,
    pub bytes: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadExecutionReport {
    pub wave_count: usize,
    pub range_count: usize,
    pub bytes_read: u64,
    pub max_wave_bytes_read: u64,
}

#[derive(Clone)]
pub struct SegmentReadPool {
    inner: Arc<rayon::ThreadPool>,
    worker_count: NonZeroUsize,
}

impl fmt::Debug for SegmentReadPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentReadPool")
            .field("worker_count", &self.worker_count)
            .finish_non_exhaustive()
    }
}

impl SegmentReadPool {
    pub fn new(worker_count: NonZeroUsize) -> Result<Self, SegmentReadPoolError> {
        let inner = rayon::ThreadPoolBuilder::new()
            .num_threads(worker_count.get())
            .thread_name(|index| format!("skein-segment-read-{index}"))
            .build()
            .map_err(|error| SegmentReadPoolError(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(inner),
            worker_count,
        })
    }

    pub fn worker_count(&self) -> NonZeroUsize {
        self.worker_count
    }

    fn shared_default() -> Option<Self> {
        static SHARED: OnceLock<Option<SegmentReadPool>> = OnceLock::new();
        SHARED
            .get_or_init(|| {
                let worker_count = std::thread::available_parallelism()
                    .unwrap_or(NonZeroUsize::MIN)
                    .min(NonZeroUsize::new(16).expect("shared segment read limit is non-zero"));
                SegmentReadPool::new(worker_count).ok()
            })
            .clone()
    }

    fn read_wave<R: SegmentRangeReader>(
        &self,
        reader: &R,
        ranges: &[SegmentReadRange],
    ) -> Vec<Result<SegmentReadPayload, SegmentReadError>> {
        let results = Mutex::new(
            (0..ranges.len())
                .map(|_| None)
                .collect::<Vec<Option<Result<SegmentReadPayload, SegmentReadError>>>>(),
        );
        self.inner.scope(|scope| {
            for (index, range) in ranges.iter().enumerate() {
                let results = &results;
                scope.spawn(move |_| {
                    let result = catch_unwind(AssertUnwindSafe(|| reader.read_range(range)))
                        .map(|result| {
                            result.map(|bytes| SegmentReadPayload {
                                range: range.clone(),
                                bytes,
                            })
                        })
                        .unwrap_or_else(|_| {
                            Err(SegmentReadError::WorkerPanicked {
                                artifact_id: range.artifact_id,
                            })
                        });
                    results
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())[index] = Some(result);
                });
            }
        });
        results
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .into_iter()
            .map(|result| result.expect("segment read worker always records a result"))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadPoolError(String);

impl Display for SegmentReadPoolError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "segment read pool creation failed: {}", self.0)
    }
}

impl Error for SegmentReadPoolError {}

#[derive(Debug, Clone)]
pub struct SegmentReadExecutor {
    max_wave_bytes: NonZeroU64,
    pool: Option<SegmentReadPool>,
}

/// Controls whether a segment reader should continue past the current payload.
///
/// A stop is successful completion. It is used by bounded consumers such as a
/// `LIMIT` operator, which must not turn normal early termination into an I/O
/// error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentReadControl {
    Continue,
    Stop,
}

impl SegmentReadExecutor {
    pub fn new(max_wave_bytes: NonZeroU64) -> Self {
        Self {
            max_wave_bytes,
            pool: SegmentReadPool::shared_default(),
        }
    }

    pub fn with_pool(max_wave_bytes: NonZeroU64, pool: SegmentReadPool) -> Self {
        Self {
            max_wave_bytes,
            pool: Some(pool),
        }
    }

    pub fn execute<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<(), E>,
    {
        self.execute_control(reader, schedule, |payload| {
            consume(payload).map(|()| SegmentReadControl::Continue)
        })
    }

    pub fn execute_with_context<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        context: &RuntimeTaskContext,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<(), E>,
    {
        self.execute_with_context_control(reader, schedule, context, |payload| {
            consume(payload).map(|()| SegmentReadControl::Continue)
        })
    }

    pub fn execute_control<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<SegmentReadControl, E>,
    {
        self.execute_control_inner(reader, schedule, None, consume)
    }

    pub fn execute_with_context_control<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        context: &RuntimeTaskContext,
        consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<SegmentReadControl, E>,
    {
        self.execute_control_inner(reader, schedule, Some(context), consume)
    }

    fn execute_control_inner<R, F, E>(
        self,
        reader: &R,
        schedule: &SegmentReadSchedule,
        context: Option<&RuntimeTaskContext>,
        mut consume: F,
    ) -> Result<SegmentReadExecutionReport, SegmentReadExecutionError<E>>
    where
        R: SegmentRangeReader,
        F: FnMut(SegmentReadPayload) -> Result<SegmentReadControl, E>,
    {
        segment_read_checkpoint(context)?;
        let mut executed_wave_count = 0usize;
        let mut range_count = 0usize;
        let mut bytes_read = 0u64;
        let mut max_wave_bytes_read = 0u64;
        for (wave_index, wave) in schedule.waves.iter().enumerate() {
            segment_read_checkpoint(context)?;
            let wave_bytes = wave
                .ranges
                .iter()
                .map(|range| range.length.get())
                .fold(0u64, u64::saturating_add);
            if wave_bytes > self.max_wave_bytes.get() {
                return Err(SegmentReadExecutionError::Read(
                    SegmentReadError::WaveBudgetExceeded {
                        wave_index,
                        scheduled_bytes: wave_bytes,
                        max_wave_bytes: self.max_wave_bytes.get(),
                    },
                ));
            }

            let payloads = {
                let _io_permit = match (context, NonZeroUsize::new(wave.ranges.len())) {
                    (Some(context), Some(slots)) => {
                        context
                            .acquire_io_wave(slots)
                            .map_err(|error| match error {
                                RuntimeIoWaveError::Stopped(reason) => {
                                    SegmentReadExecutionError::Stopped(reason)
                                }
                                error => SegmentReadExecutionError::RuntimeIo(error),
                            })?
                    }
                    _ => None,
                };
                match &self.pool {
                    Some(pool) => pool.read_wave(reader, &wave.ranges),
                    None => wave
                        .ranges
                        .iter()
                        .map(|range| {
                            reader.read_range(range).map(|bytes| SegmentReadPayload {
                                range: range.clone(),
                                bytes,
                            })
                        })
                        .collect(),
                }
            }
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(SegmentReadExecutionError::Read)?;

            segment_read_checkpoint(context)?;
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
                segment_read_checkpoint(context)?;
                if consume(payload).map_err(SegmentReadExecutionError::Consume)?
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
        segment_read_checkpoint(context)?;
        Ok(SegmentReadExecutionReport {
            wave_count: executed_wave_count,
            range_count,
            bytes_read,
            max_wave_bytes_read,
        })
    }
}

fn segment_read_checkpoint<E>(
    context: Option<&RuntimeTaskContext>,
) -> Result<(), SegmentReadExecutionError<E>> {
    match context {
        Some(context) => context
            .checkpoint()
            .map_err(SegmentReadExecutionError::Stopped),
        None => Ok(()),
    }
}

fn range_io_error(range: &SegmentReadRange, source: std::io::Error) -> SegmentReadError {
    SegmentReadError::Io {
        artifact_id: range.artifact_id,
        offset: range.offset,
        length: range.length.get(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::SegmentReadScheduler;
    use skein_core::{
        RuntimeCancellationReason, RuntimeCancellationToken, RuntimeIoWaveController,
        RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeTaskContext,
    };
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct ConcurrencyTrackingReader {
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    #[derive(Debug, Default)]
    struct RecordingIoWaveController {
        active_slots: Arc<AtomicUsize>,
        acquired_slots: Mutex<Vec<usize>>,
    }

    #[derive(Debug)]
    struct RecordingIoWavePermit {
        active_slots: Arc<AtomicUsize>,
        slots: usize,
    }

    impl Drop for RecordingIoWavePermit {
        fn drop(&mut self) {
            self.active_slots.fetch_sub(self.slots, Ordering::AcqRel);
        }
    }

    impl RuntimeIoWaveController for RecordingIoWaveController {
        fn acquire(
            &self,
            slots: NonZeroUsize,
            context: &RuntimeTaskContext,
        ) -> Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            context.checkpoint().map_err(RuntimeIoWaveError::Stopped)?;
            self.acquired_slots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(slots.get());
            self.active_slots.fetch_add(slots.get(), Ordering::AcqRel);
            Ok(Box::new(RecordingIoWavePermit {
                active_slots: Arc::clone(&self.active_slots),
                slots: slots.get(),
            }))
        }
    }

    impl SegmentRangeReader for ConcurrencyTrackingReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.peak.fetch_max(active, Ordering::AcqRel);
            std::thread::sleep(Duration::from_millis(10));
            self.active.fetch_sub(1, Ordering::AcqRel);
            Ok(Arc::from(vec![0; range.length.get() as usize]))
        }
    }

    /// Panics for one nominated segment and serves every other range, so a
    /// test can tell a contained panic apart from a wave that gave up.
    struct PanickingReader {
        panic_on_segment: u64,
    }

    impl SegmentRangeReader for PanickingReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<Arc<[u8]>, SegmentReadError> {
            assert_ne!(
                range.segment_ids.first().copied(),
                Some(self.panic_on_segment),
                "injected range reader panic"
            );
            Ok(Arc::from(vec![0; range.length.get() as usize]))
        }
    }

    #[test]
    fn panicking_range_read_preserves_completed_waves_and_returns_typed_error() {
        let reader = PanickingReader {
            panic_on_segment: 2,
        };
        let ranges = (0..4)
            .map(|segment_id| SegmentReadRange::new(1, segment_id, segment_id, NonZeroU64::MIN))
            .collect::<Vec<_>>();
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
            .schedule(ranges);
        assert_eq!(schedule.wave_count(), 2);
        let pool = SegmentReadPool::new(NonZeroUsize::new(2).unwrap()).unwrap();

        let mut served = Vec::new();
        let error = SegmentReadExecutor::with_pool(NonZeroU64::new(4).unwrap(), pool)
            .execute(&reader, &schedule, |payload| {
                served.push(payload.range.segment_ids.first().copied());
                Ok::<(), std::convert::Infallible>(())
            })
            .expect_err("a panicking range read must surface as an error");

        // The panic becomes a typed error naming the artifact, not a process
        // abort and not a silently dropped range.
        assert!(
            matches!(
                error,
                SegmentReadExecutionError::Read(SegmentReadError::WorkerPanicked {
                    artifact_id: 1
                })
            ),
            "unexpected error: {error}"
        );
        // The message stays free of anything the caller did not already know,
        // matching `file_reader_errors_do_not_expose_registered_paths`.
        assert_eq!(
            error.to_string(),
            "segment artifact 1 range reader panicked"
        );
        // The first wave reached the sink before the second wave failed. The
        // failing wave remains atomic, so none of its payloads are delivered.
        assert_eq!(served, vec![Some(0), Some(1)]);
    }

    #[test]
    fn shared_pool_bounds_parallel_range_reads() {
        let reader = ConcurrencyTrackingReader::default();
        let ranges = (0..8)
            .map(|segment_id| SegmentReadRange::new(1, segment_id, segment_id, NonZeroU64::MIN))
            .collect::<Vec<_>>();
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(8).unwrap(), NonZeroU64::MIN)
            .schedule(ranges);
        let pool = SegmentReadPool::new(NonZeroUsize::new(2).unwrap()).unwrap();

        SegmentReadExecutor::with_pool(NonZeroU64::new(8).unwrap(), pool)
            .execute(&reader, &schedule, |_| {
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();

        assert_eq!(reader.active.load(Ordering::Acquire), 0);
        assert_eq!(reader.peak.load(Ordering::Acquire), 2);
    }

    #[test]
    fn runtime_io_slots_are_held_only_while_the_read_wave_is_live() {
        let reader = ConcurrencyTrackingReader::default();
        let ranges = (0..2)
            .map(|segment_id| SegmentReadRange::new(1, segment_id, segment_id, NonZeroU64::MIN))
            .collect::<Vec<_>>();
        let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
            .schedule(ranges);
        let pool = SegmentReadPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
        let controller = Arc::new(RecordingIoWaveController::default());
        let context = RuntimeTaskContext::default()
            .with_io_wave_controller(controller.clone() as Arc<dyn RuntimeIoWaveController>);

        SegmentReadExecutor::with_pool(NonZeroU64::new(2).unwrap(), pool)
            .execute_with_context(&reader, &schedule, &context, |_| {
                assert_eq!(controller.active_slots.load(Ordering::Acquire), 0);
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();

        assert_eq!(reader.peak.load(Ordering::Acquire), 2);
        assert_eq!(controller.active_slots.load(Ordering::Acquire), 0);
        assert_eq!(
            *controller
                .acquired_slots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![2]
        );
    }

    #[test]
    fn executes_file_ranges_in_schedule_order() {
        let path = unique_test_file("ordered");
        std::fs::write(&path, b"abcdefghijklmnop").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(4).unwrap())
                .schedule([
                    SegmentReadRange::new(7, 2, 8, NonZeroU64::new(4).unwrap()),
                    SegmentReadRange::new(7, 1, 0, NonZeroU64::new(4).unwrap()),
                ]);
        let mut payloads = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute(&reader, &schedule, |payload| {
                payloads.push(payload.bytes.to_vec());
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();

        assert_eq!(payloads, vec![b"abcd".to_vec(), b"ijkl".to_vec()]);
        assert_eq!(report.wave_count, 1);
        assert_eq!(report.range_count, 2);
        assert_eq!(report.bytes_read, 8);
        assert_eq!(report.max_wave_bytes_read, 8);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_reads_disjoint_ranges_concurrently_without_shared_cursor_state() {
        let path = unique_test_file("positioned-concurrent");
        let payload = (0..64u8).collect::<Vec<_>>();
        std::fs::write(&path, &payload).unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let reader = Arc::new(reader);

        let outputs = std::thread::scope(|scope| {
            (0..16u64)
                .map(|index| {
                    let reader = Arc::clone(&reader);
                    scope.spawn(move || {
                        let range =
                            SegmentReadRange::new(7, index, index * 4, NonZeroU64::new(4).unwrap());
                        reader.read_range(&range).unwrap().to_vec()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });

        for (index, output) in outputs.iter().enumerate() {
            assert_eq!(output, &payload[index * 4..index * 4 + 4]);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_reports_truncated_positioned_range_as_io_error() {
        let path = unique_test_file("positioned-truncated");
        std::fs::write(&path, b"short").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let range = SegmentReadRange::new(7, 1, 2, NonZeroU64::new(8).unwrap());

        let error = reader.read_range(&range).unwrap_err();
        assert!(matches!(error, SegmentReadError::Io { .. }));
        assert_eq!(
            error.source().unwrap().to_string(),
            "segment range ended before the admitted length"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_reuses_digest_verified_cache_by_generation() {
        let path = unique_test_file("cache_generation");
        std::fs::write(&path, b"first").unwrap();
        let cache = Arc::new(SegmentCache::new(64));
        let mut reader = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&cache),
            StoreId(9),
            ManifestGeneration(1),
        );
        reader.register(7, &path);
        let first_bytes = b"first";
        let first_range = SegmentReadRange::new(7, 1, 0, NonZeroU64::new(5).unwrap())
            .with_content_digest(content_digest(first_bytes));
        let cold = reader.read_range_with_report(&first_range).unwrap();
        assert_eq!(&*cold.payload, first_bytes);
        assert!(!cold.cache_hit);
        assert!(cold.cache_miss);

        std::fs::write(&path, b"later").unwrap();
        let warm = reader.read_range_with_report(&first_range).unwrap();
        assert_eq!(&*warm.payload, first_bytes);
        assert!(warm.cache_hit);
        assert!(!warm.cache_miss);
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.hit_count, 1);
        assert_eq!(snapshot.resident_bytes, 5);

        let mut next_reader = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&cache),
            StoreId(9),
            ManifestGeneration(2),
        );
        next_reader.register(7, &path);
        let next_range = SegmentReadRange::new(7, 1, 0, NonZeroU64::new(5).unwrap())
            .with_content_digest(content_digest(b"later"));
        assert_eq!(&*next_reader.read_range(&next_range).unwrap(), b"later");
        assert_eq!(cache.snapshot().entry_count, 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_fails_closed_on_manifest_digest_mismatch() {
        let path = unique_test_file("digest_mismatch");
        std::fs::write(&path, b"actual").unwrap();
        let cache = Arc::new(SegmentCache::new(64));
        let mut reader = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&cache),
            StoreId(9),
            ManifestGeneration(1),
        );
        reader.register(7, &path);
        let range = SegmentReadRange::new(7, 1, 0, NonZeroU64::new(6).unwrap())
            .with_content_digest(content_digest(b"wanted"));

        let error = reader.read_range(&range).unwrap_err();
        assert!(matches!(error, SegmentReadError::DigestMismatch { .. }));
        assert_eq!(cache.snapshot().digest_mismatch_count, 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_wave_before_allocating_over_budget_payloads() {
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(8).unwrap())
                .schedule([
                    SegmentReadRange::new(1, 1, 0, NonZeroU64::new(8).unwrap()),
                    SegmentReadRange::new(2, 2, 0, NonZeroU64::new(8).unwrap()),
                ]);
        let reader = FileSegmentRangeReader::new();

        let error = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute(
                &reader,
                &schedule,
                |_| Ok::<_, std::convert::Infallible>(()),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            SegmentReadExecutionError::Read(SegmentReadError::WaveBudgetExceeded {
                wave_index: 0,
                scheduled_bytes: 16,
                max_wave_bytes: 8,
            })
        ));
    }

    #[test]
    fn byte_budgeted_schedule_executes_ranges_in_separate_waves() {
        let path = unique_test_file("byte_budgeted");
        std::fs::write(&path, b"abcdefghijklmnop").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(8).unwrap())
                .schedule_with_wave_budget(
                    [
                        SegmentReadRange::new(7, 1, 0, NonZeroU64::new(8).unwrap()),
                        SegmentReadRange::new(7, 2, 8, NonZeroU64::new(8).unwrap()),
                    ],
                    NonZeroU64::new(8).unwrap(),
                );
        let mut payloads = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute(&reader, &schedule, |payload| {
                payloads.push(payload.bytes.to_vec());
                Ok::<(), std::convert::Infallible>(())
            })
            .unwrap();

        assert_eq!(payloads, vec![b"abcdefgh".to_vec(), b"ijklmnop".to_vec()]);
        assert_eq!(report.wave_count, 2);
        assert_eq!(report.max_wave_bytes_read, 8);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_control_stops_before_scheduling_the_next_wave() {
        let path = unique_test_file("controlled-stop");
        std::fs::write(&path, b"abcdefghijklmnop").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(8).unwrap())
                .schedule_with_wave_budget(
                    [
                        SegmentReadRange::new(7, 1, 0, NonZeroU64::new(8).unwrap()),
                        SegmentReadRange::new(7, 2, 8, NonZeroU64::new(8).unwrap()),
                    ],
                    NonZeroU64::new(8).unwrap(),
                );
        let mut payloads = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute_control(&reader, &schedule, |payload| {
                payloads.push(payload.bytes.to_vec());
                Ok::<_, std::convert::Infallible>(SegmentReadControl::Stop)
            })
            .unwrap();

        assert_eq!(payloads, vec![b"abcdefgh".to_vec()]);
        assert_eq!(report.wave_count, 1);
        assert_eq!(report.range_count, 1);
        assert_eq!(report.bytes_read, 8);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_report_accounts_for_the_entire_wave_before_consumer_stop() {
        let path = unique_test_file("controlled-wave-accounting");
        std::fs::write(&path, b"abcdefghijklmnop").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(7, &path);
        reader.register(8, &path);
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(4).unwrap())
                .schedule_with_wave_budget(
                    [
                        SegmentReadRange::new(7, 1, 0, NonZeroU64::new(4).unwrap()),
                        SegmentReadRange::new(8, 2, 4, NonZeroU64::new(4).unwrap()),
                    ],
                    NonZeroU64::new(8).unwrap(),
                );
        assert_eq!(schedule.wave_count(), 1);
        let mut payloads = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(8).unwrap())
            .execute_control(&reader, &schedule, |payload| {
                payloads.push(payload.bytes.to_vec());
                Ok::<_, std::convert::Infallible>(SegmentReadControl::Stop)
            })
            .unwrap();

        assert_eq!(payloads, vec![b"abcd".to_vec()]);
        assert_eq!(report.wave_count, 1);
        assert_eq!(report.range_count, 2);
        assert_eq!(report.bytes_read, 8);
        assert_eq!(report.max_wave_bytes_read, 8);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn controlled_reader_stops_between_io_waves() {
        let path = unique_test_file("cancelled");
        std::fs::write(&path, b"abcdefgh").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(1, &path);
        let schedule = SegmentReadScheduler::new(NonZeroUsize::MIN, NonZeroU64::new(4).unwrap())
            .schedule([
                SegmentReadRange::new(1, 1, 0, NonZeroU64::new(4).unwrap()),
                SegmentReadRange::new(1, 2, 4, NonZeroU64::new(4).unwrap()),
            ]);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        let mut consumed = 0usize;

        let result = SegmentReadExecutor::new(NonZeroU64::new(4).unwrap()).execute_with_context(
            &reader,
            &schedule,
            &context,
            |_| {
                consumed += 1;
                token.cancel();
                Ok::<(), std::convert::Infallible>(())
            },
        );

        assert!(matches!(
            result,
            Err(SegmentReadExecutionError::Stopped(
                RuntimeCancellationReason::Cancelled
            ))
        ));
        assert_eq!(consumed, 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn file_reader_errors_do_not_expose_registered_paths() {
        let path = unique_test_file("short");
        std::fs::write(&path, b"short").unwrap();
        let mut reader = FileSegmentRangeReader::new();
        reader.register(9, &path);
        let range = SegmentReadRange::new(9, 1, 2, NonZeroU64::new(8).unwrap());

        let error = reader.read_range(&range).unwrap_err();

        assert_eq!(
            error.to_string(),
            "segment artifact 9 range read failed at offset 2 for 8 bytes"
        );
        assert!(!error.to_string().contains(path.to_string_lossy().as_ref()));
        std::fs::remove_file(path).unwrap();
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_storage_segment_reader_{name}_{}_{}",
            std::process::id(),
            nonce
        ))
    }
}
