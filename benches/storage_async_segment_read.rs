use serde_json::{json, Value};
use skein::{TokioRuntimeAdapter, TokioRuntimeConfig, TokioSegmentReadExecutor};
use skein_core::RuntimeTaskContext;
use skein_qos::{
    IoConcurrencyBudget, ProcessMemoryProfile, ProcessMemorySnapshot, RuntimeGovernor,
    RuntimeGovernorConfig, RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
    RuntimeWorkPriority, RuntimeWorkRequest,
};
use skein_storage::{
    FileSegmentRangeReader, SegmentReadExecutor, SegmentReadPool, SegmentReadRange,
    SegmentReadSchedule, SegmentReadScheduler,
};
use std::convert::Infallible;
use std::env;
#[cfg(target_os = "linux")]
use std::fs::File;
use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::{self, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_FIXTURE_MIB: usize = 640;
const DEFAULT_RANGE_COUNT: usize = 8_192;
const DEFAULT_SAMPLES: usize = 3;
const RANGE_BYTES: usize = 4 * 1024;
const FIXTURE_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const DEPTHS: [usize; 3] = [1, 4, 16];

#[derive(Debug, Clone, Copy)]
struct BenchmarkConfig {
    fixture_bytes: usize,
    range_count: usize,
    samples: usize,
}

#[derive(Debug, Clone, Copy)]
enum Backend {
    BlockingPool,
    TokioAsync,
}

impl Backend {
    fn as_str(self) -> &'static str {
        match self {
            Self::BlockingPool => "current_blocking_pool",
            Self::TokioAsync => "tokio_async",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "current_blocking_pool" => Some(Self::BlockingPool),
            "tokio_async" => Some(Self::TokioAsync),
            _ => None,
        }
    }
}

fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    if arguments.get(1).map(String::as_str) == Some("--child") {
        run_child(&arguments);
        return;
    }

    let config = BenchmarkConfig::from_env();
    let path = unique_fixture_path();
    write_fixture(&path, config.fixture_bytes).expect("benchmark fixture must be writable");
    let executable = env::current_exe().expect("benchmark executable path must be available");
    let mut samples = Vec::with_capacity(DEPTHS.len() * config.samples * 2);
    for sample in 0..config.samples {
        for depth in DEPTHS {
            let backends = if sample % 2 == 0 {
                [Backend::BlockingPool, Backend::TokioAsync]
            } else {
                [Backend::TokioAsync, Backend::BlockingPool]
            };
            for backend in backends {
                samples.push(run_child_process(
                    &executable,
                    &path,
                    config,
                    backend,
                    depth,
                    sample,
                ));
            }
        }
    }
    let mut summaries = Vec::with_capacity(DEPTHS.len() * 2);
    for depth in DEPTHS {
        for backend in [Backend::BlockingPool, Backend::TokioAsync] {
            summaries.push(summarize(&samples, backend, depth));
        }
    }
    println!(
        "storage_async_segment_read {}",
        json!({
            "protocol": "skein-storage-async-segment-read-spike-v1",
            "production_eligible": false,
            "fixture_bytes": config.fixture_bytes,
            "range_bytes": RANGE_BYTES,
            "range_count": config.range_count,
            "scheduled_bytes_per_sample": config.range_count * RANGE_BYTES,
            "samples_per_backend_and_depth": config.samples,
            "depths": DEPTHS,
            "samples": samples,
            "summaries": summaries,
        })
    );
    std::fs::remove_file(&path).expect("benchmark fixture must be removable");
}

impl BenchmarkConfig {
    fn from_env() -> Self {
        let fixture_mib = parse_positive_env("SKEIN_ASYNC_IO_FIXTURE_MIB", DEFAULT_FIXTURE_MIB);
        let fixture_bytes = fixture_mib
            .checked_mul(1024 * 1024)
            .expect("benchmark fixture size must fit usize");
        let available_ranges = fixture_bytes / RANGE_BYTES;
        let range_count = parse_positive_env("SKEIN_ASYNC_IO_RANGE_COUNT", DEFAULT_RANGE_COUNT);
        assert!(
            range_count <= available_ranges,
            "benchmark range count must fit the fixture"
        );
        Self {
            fixture_bytes,
            range_count,
            samples: parse_positive_env("SKEIN_ASYNC_IO_SAMPLES", DEFAULT_SAMPLES),
        }
    }
}

fn parse_positive_env(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
            .get()
    })
}

fn unique_fixture_path() -> PathBuf {
    env::temp_dir().join(format!(
        "skein-storage-async-read-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ))
}

fn write_fixture(path: &Path, fixture_bytes: usize) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let mut chunk = vec![0u8; FIXTURE_CHUNK_BYTES.min(fixture_bytes)];
    for (index, byte) in chunk.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(31).wrapping_add(17);
    }
    let mut remaining = fixture_bytes;
    while remaining > 0 {
        let write_bytes = remaining.min(chunk.len());
        file.write_all(&chunk[..write_bytes])?;
        remaining -= write_bytes;
    }
    file.sync_all()
}

fn run_child_process(
    executable: &Path,
    path: &Path,
    config: BenchmarkConfig,
    backend: Backend,
    depth: usize,
    sample: usize,
) -> Value {
    let output = Command::new(executable)
        .arg("--child")
        .arg(backend.as_str())
        .arg(path)
        .arg(config.fixture_bytes.to_string())
        .arg(config.range_count.to_string())
        .arg(depth.to_string())
        .arg(sample.to_string())
        .output()
        .expect("benchmark child must start");
    assert!(
        output.status.success(),
        "benchmark child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("benchmark child must emit JSON")
}

fn run_child(arguments: &[String]) {
    assert_eq!(arguments.len(), 8, "invalid benchmark child arguments");
    let backend = Backend::parse(&arguments[2]).expect("benchmark backend must be supported");
    let path = PathBuf::from(&arguments[3]);
    let fixture_bytes = arguments[4].parse::<usize>().expect("valid fixture bytes");
    let range_count = arguments[5].parse::<usize>().expect("valid range count");
    let depth = arguments[6]
        .parse::<NonZeroUsize>()
        .expect("valid I/O depth");
    let sample = arguments[7].parse::<usize>().expect("valid sample index");
    let ranges = benchmark_ranges(fixture_bytes, range_count, sample);
    let schedule = SegmentReadScheduler::new(
        depth,
        NonZeroU64::new(RANGE_BYTES as u64).expect("range size is non-zero"),
    )
    .schedule(ranges);
    let cold_cache_requested = advise_cold_random_reads(&path);
    let metrics = match backend {
        Backend::BlockingPool => measure_blocking(&path, depth, &schedule),
        Backend::TokioAsync => measure_tokio(&path, depth, &schedule),
    };
    println!(
        "{}",
        json!({
            "backend": backend.as_str(),
            "depth": depth.get(),
            "sample": sample,
            "cold_cache_requested": cold_cache_requested,
            "metrics": metrics,
        })
    );
}

fn benchmark_ranges(
    fixture_bytes: usize,
    range_count: usize,
    sample: usize,
) -> Vec<SegmentReadRange> {
    let fixture_slots = fixture_bytes / RANGE_BYTES;
    let interval = fixture_slots / range_count;
    assert!(
        interval > 0,
        "benchmark fixture must have one slot per range"
    );
    (0..range_count)
        .map(|index| {
            let jitter = (index.wrapping_mul(17).wrapping_add(sample * 13)) % interval;
            let slot = index * interval + jitter;
            SegmentReadRange::new(
                1,
                index as u64,
                (slot * RANGE_BYTES) as u64,
                NonZeroU64::new(RANGE_BYTES as u64).expect("range size is non-zero"),
            )
        })
        .collect()
}

fn measure_blocking(path: &Path, depth: NonZeroUsize, schedule: &SegmentReadSchedule) -> Value {
    let runtime = benchmark_runtime(depth);
    let mut reader = FileSegmentRangeReader::new();
    reader.register(1, path);
    let pool = SegmentReadPool::new(depth).expect("blocking benchmark pool must start");
    let schedule = schedule.clone();
    measure(move || {
        let result = runtime
            .block_on(runtime.execute_blocking(
                benchmark_request(depth),
                RuntimeTaskContext::default(),
                move |context| {
                    let mut checksum = 0u64;
                    let report = SegmentReadExecutor::with_pool(max_wave_bytes(depth), pool)
                        .execute_with_context(&reader, &schedule, context, |payload| {
                            checksum = accumulate_checksum(checksum, &payload);
                            Ok::<(), Infallible>(())
                        })?;
                    Ok::<_, skein_storage::SegmentReadExecutionError<Infallible>>((
                        report.bytes_read,
                        checksum,
                    ))
                },
            ))
            .expect("benchmark must not block inside an active runtime")
            .expect("blocking benchmark reads must succeed");
        black_box(result.1);
        result
    })
}

fn measure_tokio(path: &Path, depth: NonZeroUsize, schedule: &SegmentReadSchedule) -> Value {
    let runtime = benchmark_runtime(depth);
    let executor = TokioSegmentReadExecutor::new(runtime.clone(), max_wave_bytes(depth));
    let mut reader = FileSegmentRangeReader::new();
    reader.register(1, path);
    let reader = Arc::new(reader);
    let schedule = schedule.clone();
    measure(|| {
        let result = runtime
            .block_on(runtime.execute_async(
                benchmark_request(depth),
                RuntimeTaskContext::default(),
                move |context| async move {
                    let mut checksum = 0u64;
                    let report = executor
                        .execute(reader, &schedule, &context, |payload| {
                            checksum = accumulate_checksum(checksum, &payload);
                            Ok::<(), Infallible>(())
                        })
                        .await?;
                    Ok::<_, skein::TokioSegmentReadExecutionError<Infallible>>((
                        report.bytes_read,
                        checksum,
                    ))
                },
            ))
            .expect("benchmark must not block inside an active runtime")
            .expect("async benchmark reads must succeed");
        black_box(result.1);
        result
    })
}

fn benchmark_runtime(depth: NonZeroUsize) -> TokioRuntimeAdapter {
    let resources = RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(depth, None, None),
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
        IoConcurrencyBudget::new(depth.get(), 1),
    );
    let config = TokioRuntimeConfig::from_governor(&governor);
    TokioRuntimeAdapter::owned(governor, config).expect("benchmark Tokio runtime must start")
}

fn max_wave_bytes(depth: NonZeroUsize) -> NonZeroU64 {
    NonZeroU64::new((depth.get() * RANGE_BYTES) as u64).expect("wave size is non-zero")
}

fn benchmark_request(depth: NonZeroUsize) -> RuntimeWorkRequest {
    RuntimeWorkRequest::io(
        RuntimeWorkPriority::Foreground,
        depth.get(),
        max_wave_bytes(depth).get(),
    )
    .with_cpu_slots(depth.get())
    .with_io_wave_slots(depth.get())
}

fn accumulate_checksum(current: u64, payload: &skein_storage::SegmentReadPayload) -> u64 {
    let segment_id = payload
        .range
        .segment_ids
        .first()
        .copied()
        .unwrap_or_default();
    current
        .rotate_left(7)
        .wrapping_add(payload_checksum(&payload.bytes) ^ segment_id)
}

fn payload_checksum(bytes: &[u8]) -> u64 {
    u64::from(bytes[0]) ^ (u64::from(bytes[bytes.len() - 1]) << 8)
}

fn measure(operation: impl FnOnce() -> (u64, u64)) -> Value {
    let memory_start = ProcessMemorySnapshot::capture().ok();
    let cpu_start = process_cpu_time_ns();
    let start_thread_count = process_thread_count();
    let sampler = ThreadCountSampler::start();
    let started = Instant::now();
    let (bytes_read, checksum) = operation();
    let wall_time_ns = started.elapsed().as_nanos();
    let peak_thread_count = sampler.stop();
    let cpu_time_ns = cpu_start
        .zip(process_cpu_time_ns())
        .map(|(start, end)| end.saturating_sub(start));
    let memory = memory_start
        .zip(ProcessMemorySnapshot::capture().ok())
        .map(|(start, end)| ProcessMemoryProfile::between(start, end));
    json!({
        "bytes_read": bytes_read,
        "checksum": checksum,
        "wall_time_ns": wall_time_ns,
        "cpu_time_ns": cpu_time_ns,
        "start_thread_count": start_thread_count,
        "peak_thread_count": peak_thread_count,
        "steady_resident_bytes": memory.map(|profile| profile.steady_resident_bytes),
        "peak_resident_bytes": memory.map(|profile| profile.peak_resident_bytes),
        "resident_growth_bytes": memory.map(|profile| profile.steady_resident_growth_bytes),
        "peak_resident_growth_bytes": memory.map(|profile| profile.lifetime_peak_resident_growth_bytes),
        "minor_page_faults": memory.and_then(|profile| profile.minor_page_faults),
        "major_page_faults": memory.and_then(|profile| profile.major_page_faults),
    })
}

fn summarize(samples: &[Value], backend: Backend, depth: usize) -> Value {
    let selected = samples
        .iter()
        .filter(|sample| {
            sample["backend"] == backend.as_str() && sample["depth"].as_u64() == Some(depth as u64)
        })
        .collect::<Vec<_>>();
    json!({
        "backend": backend.as_str(),
        "depth": depth,
        "wall_time_ns_p50": median_u64(&selected, "wall_time_ns"),
        "cpu_time_ns_p50": median_u64(&selected, "cpu_time_ns"),
        "peak_thread_count_p50": median_u64(&selected, "peak_thread_count"),
        "steady_resident_bytes_p50": median_u64(&selected, "steady_resident_bytes"),
        "peak_resident_bytes_p50": median_u64(&selected, "peak_resident_bytes"),
    })
}

fn median_u64(samples: &[&Value], metric: &str) -> Option<u64> {
    let mut values = samples
        .iter()
        .filter_map(|sample| sample["metrics"][metric].as_u64())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.get(values.len() / 2).copied()
}

struct ThreadCountSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicUsize>,
    handle: Option<JoinHandle<()>>,
}

impl ThreadCountSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let initial = process_thread_count();
        let peak = Arc::new(AtomicUsize::new(initial.unwrap_or_default()));
        if initial.is_none() {
            return Self {
                stop,
                peak,
                handle: None,
            };
        }
        let sampler_stop = Arc::clone(&stop);
        let sampler_peak = Arc::clone(&peak);
        let handle = std::thread::spawn(move || {
            while !sampler_stop.load(Ordering::Acquire) {
                if let Some(count) = process_thread_count() {
                    sampler_peak.fetch_max(count, Ordering::AcqRel);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        Self {
            stop,
            peak,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> Option<usize> {
        let handle = self.handle.take()?;
        self.stop.store(true, Ordering::Release);
        handle.join().expect("thread sampler must not panic");
        let sampled_without_sampler = self.peak.load(Ordering::Acquire).saturating_sub(1);
        process_thread_count().map(|count| count.max(sampled_without_sampler))
    }
}

#[cfg(unix)]
fn process_cpu_time_ns() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided value when it succeeds.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: getrusage succeeded, so the value is initialized.
    let usage = unsafe { usage.assume_init() };
    Some(timeval_ns(usage.ru_utime).saturating_add(timeval_ns(usage.ru_stime)))
}

#[cfg(unix)]
fn timeval_ns(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or_default()
        .saturating_mul(1_000_000_000)
        .saturating_add(
            u64::try_from(value.tv_usec)
                .unwrap_or_default()
                .saturating_mul(1_000),
        )
}

#[cfg(not(unix))]
fn process_cpu_time_ns() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn process_thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task")
        .ok()
        .map(Iterator::count)
}

#[cfg(target_os = "macos")]
fn process_thread_count() -> Option<usize> {
    let mut threads = std::ptr::null_mut();
    let mut count = 0;
    #[allow(deprecated)]
    // SAFETY: task_threads initializes the returned array and count on success.
    let status = unsafe { libc::task_threads(libc::mach_task_self(), &mut threads, &mut count) };
    if status != libc::KERN_SUCCESS {
        return None;
    }
    let bytes = usize::try_from(count)
        .unwrap_or_default()
        .saturating_mul(std::mem::size_of::<libc::thread_t>());
    #[allow(deprecated)]
    // SAFETY: task_threads allocated this array in the current task's address space.
    let deallocated = unsafe {
        libc::vm_deallocate(
            libc::mach_task_self(),
            threads as libc::vm_address_t,
            bytes as libc::vm_size_t,
        )
    };
    (deallocated == libc::KERN_SUCCESS).then_some(count as usize)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_thread_count() -> Option<usize> {
    None
}

#[cfg(target_os = "linux")]
fn advise_cold_random_reads(path: &Path) -> bool {
    use std::os::fd::AsRawFd;

    let Ok(file) = File::open(path) else {
        return false;
    };
    // SAFETY: posix_fadvise only observes this live file descriptor.
    let random = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
    // SAFETY: posix_fadvise only observes this live file descriptor.
    let cold = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    random == 0 && cold == 0
}

#[cfg(not(target_os = "linux"))]
fn advise_cold_random_reads(_path: &Path) -> bool {
    false
}
