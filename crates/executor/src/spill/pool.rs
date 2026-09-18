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

use crate::ExecutionMemoryConfig;
use hawdb_core::{HawDBError, Result};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const SPILL_FILE_PREFIX: &str = "hawdb-spill-v1-";
pub(super) const SPILL_FILE_SUFFIX: &str = ".spill";
static SPILL_POOLS: OnceLock<Mutex<BTreeMap<PathBuf, Arc<SharedSpillPool>>>> = OnceLock::new();
static PROCESS_MARKER: OnceLock<String> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillPoolSnapshot {
    pub max_total_bytes: u64,
    pub max_total_runs: usize,
    pub min_free_bytes: u64,
    pub free_space_probe_interval_bytes: u64,
    pub free_space_probe_count: u64,
    pub free_space_probe_failures: u64,
    pub active_bytes: u64,
    pub peak_active_bytes: u64,
    pub pending_write_bytes: u64,
    pub active_runs: usize,
    pub peak_active_runs: usize,
    pub orphan_files_removed: u64,
    pub orphan_bytes_removed: u64,
    pub orphan_cleanup_failures: u64,
    pub run_delete_failures: u64,
}

#[derive(Debug, Clone, Copy)]
struct SpillPoolLimits {
    max_total_bytes: u64,
    max_total_runs: usize,
    min_free_bytes: u64,
    free_space_probe_interval_bytes: u64,
}

impl SpillPoolLimits {
    fn from_config(memory: &ExecutionMemoryConfig) -> Self {
        Self {
            max_total_bytes: memory.max_total_spill_bytes.get(),
            max_total_runs: memory.max_total_spill_runs.get(),
            min_free_bytes: memory.min_spill_free_bytes.get(),
            free_space_probe_interval_bytes: memory.spill_free_space_probe_interval_bytes.get(),
        }
    }

    fn tighten(&mut self, other: Self) {
        self.max_total_bytes = self.max_total_bytes.min(other.max_total_bytes);
        self.max_total_runs = self.max_total_runs.min(other.max_total_runs);
        self.min_free_bytes = self.min_free_bytes.max(other.min_free_bytes);
        self.free_space_probe_interval_bytes = self
            .free_space_probe_interval_bytes
            .min(other.free_space_probe_interval_bytes);
    }
}

#[derive(Debug, Default)]
struct OrphanCleanupStats {
    files_removed: u64,
    bytes_removed: u64,
    failures: u64,
}

#[derive(Debug)]
struct SpillPoolState {
    limits: SpillPoolLimits,
    active_bytes: u64,
    peak_active_bytes: u64,
    pending_write_bytes: u64,
    last_probed_available_bytes: Option<u64>,
    unreflected_reserved_bytes: u64,
    bytes_reserved_since_probe: u64,
    free_space_probe_in_progress: bool,
    free_space_probe_count: u64,
    free_space_probe_failures: u64,
    active_runs: usize,
    peak_active_runs: usize,
    orphan_cleanup: OrphanCleanupStats,
    run_delete_failures: u64,
}

#[derive(Debug)]
struct SharedSpillPool {
    directory: PathBuf,
    state: Mutex<SpillPoolState>,
    free_space_probe_ready: Condvar,
    free_space_probe: Arc<dyn SpillSpaceProbe>,
}

trait SpillSpaceProbe: std::fmt::Debug + Send + Sync {
    fn available_space(&self, directory: &Path) -> std::io::Result<u64>;
}

#[derive(Debug)]
struct FileSystemSpillSpaceProbe;

impl SpillSpaceProbe for FileSystemSpillSpaceProbe {
    fn available_space(&self, directory: &Path) -> std::io::Result<u64> {
        fs2::available_space(directory)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SpillPool {
    shared: Arc<SharedSpillPool>,
}

impl SpillPool {
    pub(crate) fn open(memory: &ExecutionMemoryConfig) -> Result<Self> {
        Self::open_with_probe(memory, Arc::new(FileSystemSpillSpaceProbe))
    }

    fn open_with_probe(
        memory: &ExecutionMemoryConfig,
        free_space_probe: Arc<dyn SpillSpaceProbe>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&memory.spill_directory).map_err(|error| {
            HawDBError::Execution(format!(
                "failed to create spill directory '{}': {error}",
                memory.spill_directory.display()
            ))
        })?;
        let directory = std::fs::canonicalize(&memory.spill_directory).map_err(|error| {
            HawDBError::Execution(format!(
                "failed to resolve spill directory '{}': {error}",
                memory.spill_directory.display()
            ))
        })?;
        let limits = SpillPoolLimits::from_config(memory);
        let mut pools = lock_unpoisoned(SPILL_POOLS.get_or_init(Default::default));
        if let Some(shared) = pools.get(&directory) {
            lock_unpoisoned(&shared.state).limits.tighten(limits);
            return Ok(Self {
                shared: Arc::clone(shared),
            });
        }
        let orphan_cleanup = cleanup_orphan_files(
            &directory,
            memory.spill_orphan_grace_period,
            process_marker(),
        )?;
        let shared = Arc::new(SharedSpillPool {
            directory: directory.clone(),
            state: Mutex::new(SpillPoolState {
                limits,
                active_bytes: 0,
                peak_active_bytes: 0,
                pending_write_bytes: 0,
                last_probed_available_bytes: None,
                unreflected_reserved_bytes: 0,
                bytes_reserved_since_probe: 0,
                free_space_probe_in_progress: false,
                free_space_probe_count: 0,
                free_space_probe_failures: 0,
                active_runs: 0,
                peak_active_runs: 0,
                orphan_cleanup,
                run_delete_failures: 0,
            }),
            free_space_probe_ready: Condvar::new(),
            free_space_probe,
        });
        pools.insert(directory, Arc::clone(&shared));
        Ok(Self { shared })
    }

    pub(super) fn directory(&self) -> &Path {
        &self.shared.directory
    }

    pub(super) fn begin_run(&self, operator: &str) -> Result<()> {
        let mut state = lock_unpoisoned(&self.shared.state);
        if state.active_runs >= state.limits.max_total_runs {
            return Err(HawDBError::Execution(format!(
                "{operator} exceeded shared max_total_spill_runs {}",
                state.limits.max_total_runs
            )));
        }
        state.active_runs = state.active_runs.saturating_add(1);
        state.peak_active_runs = state.peak_active_runs.max(state.active_runs);
        Ok(())
    }

    pub(super) fn cancel_run(&self) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.active_runs = state.active_runs.saturating_sub(1);
    }

    pub(crate) fn reserve_bytes(
        &self,
        operator: &str,
        bytes: u64,
    ) -> Result<SpillWriteReservation> {
        loop {
            let mut state = lock_unpoisoned(&self.shared.state);
            let next_active = state.active_bytes.saturating_add(bytes);
            if next_active > state.limits.max_total_bytes {
                return Err(HawDBError::Execution(format!(
                    "{operator} exceeded shared max_total_spill_bytes {} (next active total {next_active})",
                    state.limits.max_total_bytes
                )));
            }
            let probe_required = state.last_probed_available_bytes.is_none()
                || state.bytes_reserved_since_probe >= state.limits.free_space_probe_interval_bytes;
            if probe_required {
                if state.free_space_probe_in_progress {
                    drop(wait_unpoisoned(&self.shared.free_space_probe_ready, state));
                    continue;
                }
                state.free_space_probe_in_progress = true;
                let pending_write_bytes = state.pending_write_bytes;
                drop(state);

                let probe_result = self
                    .shared
                    .free_space_probe
                    .available_space(&self.shared.directory);

                let mut state = lock_unpoisoned(&self.shared.state);
                state.free_space_probe_in_progress = false;
                state.free_space_probe_count = state.free_space_probe_count.saturating_add(1);
                match probe_result {
                    Ok(available) => {
                        state.last_probed_available_bytes = Some(available);
                        state.unreflected_reserved_bytes = pending_write_bytes;
                        state.bytes_reserved_since_probe = 0;
                        self.shared.free_space_probe_ready.notify_all();
                    }
                    Err(error) => {
                        state.free_space_probe_failures =
                            state.free_space_probe_failures.saturating_add(1);
                        self.shared.free_space_probe_ready.notify_all();
                        drop(state);
                        return Err(HawDBError::Execution(format!(
                            "failed to inspect free space for spill directory '{}': {error}",
                            self.shared.directory.display()
                        )));
                    }
                }
                continue;
            }
            let Some(available) = state.last_probed_available_bytes else {
                continue;
            };
            let required = state
                .limits
                .min_free_bytes
                .saturating_add(state.unreflected_reserved_bytes)
                .saturating_add(bytes);
            if available < required {
                return Err(HawDBError::Execution(format!(
                    "{operator} cannot preserve min_spill_free_bytes {}: filesystem has {available} bytes available and {bytes} bytes were requested",
                    state.limits.min_free_bytes
                )));
            }
            state.active_bytes = next_active;
            state.peak_active_bytes = state.peak_active_bytes.max(next_active);
            state.pending_write_bytes = state.pending_write_bytes.saturating_add(bytes);
            state.unreflected_reserved_bytes =
                state.unreflected_reserved_bytes.saturating_add(bytes);
            state.bytes_reserved_since_probe =
                state.bytes_reserved_since_probe.saturating_add(bytes);
            drop(state);
            return Ok(SpillWriteReservation {
                pool: self.clone(),
                bytes,
                committed: false,
            });
        }
    }

    fn commit_write(&self, bytes: u64) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(bytes);
    }

    fn rollback_write(&self, bytes: u64) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(bytes);
        state.active_bytes = state.active_bytes.saturating_sub(bytes);
    }

    fn finish_run(&self, bytes: u64, pending_bytes: u64, deleted: bool) {
        let mut state = lock_unpoisoned(&self.shared.state);
        state.pending_write_bytes = state.pending_write_bytes.saturating_sub(pending_bytes);
        if deleted {
            state.active_bytes = state.active_bytes.saturating_sub(bytes);
            state.active_runs = state.active_runs.saturating_sub(1);
        } else {
            state.run_delete_failures = state.run_delete_failures.saturating_add(1);
        }
    }

    fn snapshot(&self) -> SpillPoolSnapshot {
        let state = lock_unpoisoned(&self.shared.state);
        SpillPoolSnapshot {
            max_total_bytes: state.limits.max_total_bytes,
            max_total_runs: state.limits.max_total_runs,
            min_free_bytes: state.limits.min_free_bytes,
            free_space_probe_interval_bytes: state.limits.free_space_probe_interval_bytes,
            free_space_probe_count: state.free_space_probe_count,
            free_space_probe_failures: state.free_space_probe_failures,
            active_bytes: state.active_bytes,
            peak_active_bytes: state.peak_active_bytes,
            pending_write_bytes: state.pending_write_bytes,
            active_runs: state.active_runs,
            peak_active_runs: state.peak_active_runs,
            orphan_files_removed: state.orphan_cleanup.files_removed,
            orphan_bytes_removed: state.orphan_cleanup.bytes_removed,
            orphan_cleanup_failures: state.orphan_cleanup.failures,
            run_delete_failures: state.run_delete_failures,
        }
    }
}

pub(crate) struct SpillWriteReservation {
    pool: SpillPool,
    bytes: u64,
    committed: bool,
}

impl SpillWriteReservation {
    pub(super) fn commit(mut self, lease: &RunLease) {
        lease
            .reserved_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        lease
            .pending_write_bytes
            .fetch_add(self.bytes, Ordering::Relaxed);
        self.committed = true;
    }
}

impl Drop for SpillWriteReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.pool.rollback_write(self.bytes);
        }
    }
}

#[derive(Debug)]
pub(super) struct RunLease {
    path: PathBuf,
    pool: SpillPool,
    reserved_bytes: AtomicU64,
    pending_write_bytes: AtomicU64,
}

impl RunLease {
    pub(super) fn new(path: PathBuf, pool: SpillPool) -> Self {
        Self {
            path,
            pool,
            reserved_bytes: AtomicU64::new(0),
            pending_write_bytes: AtomicU64::new(0),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn mark_flushed(&self) {
        let pending_bytes = self.pending_write_bytes.swap(0, Ordering::Relaxed);
        self.pool.commit_write(pending_bytes);
    }
}

impl Drop for RunLease {
    fn drop(&mut self) {
        let deleted = match std::fs::remove_file(&self.path) {
            Ok(()) => true,
            Err(error) if error.kind() == ErrorKind::NotFound => true,
            Err(_) => false,
        };
        self.pool.finish_run(
            self.reserved_bytes.load(Ordering::Relaxed),
            self.pending_write_bytes.load(Ordering::Relaxed),
            deleted,
        );
    }
}

pub(crate) fn spill_pool_snapshot(memory: &ExecutionMemoryConfig) -> Result<SpillPoolSnapshot> {
    Ok(SpillPool::open(memory)?.snapshot())
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(super) fn process_marker() -> &'static str {
    PROCESS_MARKER.get_or_init(|| {
        let started = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        format!("{}-{started}", std::process::id())
    })
}

fn cleanup_orphan_files(
    directory: &Path,
    grace_period: Duration,
    current_process_marker: &str,
) -> Result<OrphanCleanupStats> {
    let mut stats = OrphanCleanupStats::default();
    let current_prefix = format!("{SPILL_FILE_PREFIX}{current_process_marker}-");
    let entries = std::fs::read_dir(directory).map_err(|error| {
        HawDBError::Execution(format!(
            "failed to inspect spill directory '{}': {error}",
            directory.display()
        ))
    })?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                stats.failures = stats.failures.saturating_add(1);
                continue;
            }
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(SPILL_FILE_PREFIX)
            || !name.ends_with(SPILL_FILE_SUFFIX)
            || name.starts_with(&current_prefix)
        {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => continue,
            Err(_) => {
                stats.failures = stats.failures.saturating_add(1);
                continue;
            }
        };
        let old_enough = metadata
            .modified()
            .ok()
            .and_then(|modified| SystemTime::now().duration_since(modified).ok())
            .is_some_and(|age| age >= grace_period);
        if !old_enough {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                stats.files_removed = stats.files_removed.saturating_add(1);
                stats.bytes_removed = stats.bytes_removed.saturating_add(metadata.len());
            }
            Err(_) => stats.failures = stats.failures.saturating_add(1),
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;
    use std::sync::mpsc;
    use std::thread;

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    struct AdjustableSpaceProbe {
        available: AtomicU64,
        calls: AtomicU64,
    }

    impl AdjustableSpaceProbe {
        fn new(available: u64) -> Self {
            Self {
                available: AtomicU64::new(available),
                calls: AtomicU64::new(0),
            }
        }
    }

    impl SpillSpaceProbe for AdjustableSpaceProbe {
        fn available_space(&self, _directory: &Path) -> std::io::Result<u64> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.available.load(Ordering::Relaxed))
        }
    }

    #[derive(Debug, Default)]
    struct BlockingSpaceProbe {
        state: Mutex<(bool, bool)>,
        ready: Condvar,
        calls: AtomicU64,
    }

    impl BlockingSpaceProbe {
        fn wait_until_started(&self) {
            let mut state = lock_unpoisoned(&self.state);
            while !state.0 {
                state = wait_unpoisoned(&self.ready, state);
            }
        }

        fn release(&self) {
            let mut state = lock_unpoisoned(&self.state);
            state.1 = true;
            self.ready.notify_all();
        }
    }

    impl SpillSpaceProbe for BlockingSpaceProbe {
        fn available_space(&self, _directory: &Path) -> std::io::Result<u64> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut state = lock_unpoisoned(&self.state);
            state.0 = true;
            self.ready.notify_all();
            while !state.1 {
                state = wait_unpoisoned(&self.ready, state);
            }
            Ok(u64::MAX)
        }
    }

    fn test_memory(name: &str, probe_interval_bytes: u64) -> ExecutionMemoryConfig {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        ExecutionMemoryConfig {
            min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
            spill_free_space_probe_interval_bytes: NonZeroU64::new(probe_interval_bytes).unwrap(),
            spill_directory: std::env::temp_dir().join(format!(
                "hawdb-spill-pool-test-{}-{name}-{sequence}",
                std::process::id()
            )),
            ..ExecutionMemoryConfig::default()
        }
    }

    #[test]
    fn free_space_probe_is_amortized_by_reserved_bytes() {
        let memory = test_memory("watermark", 64);
        let probe = Arc::new(AdjustableSpaceProbe::new(1024 * 1024));
        let pool = SpillPool::open_with_probe(&memory, probe.clone()).unwrap();

        for _ in 0..8 {
            drop(pool.reserve_bytes("test", 8).unwrap());
        }
        let snapshot = pool.snapshot();
        assert_eq!(snapshot.free_space_probe_interval_bytes, 64);
        assert_eq!(snapshot.free_space_probe_count, 1);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);

        drop(pool.reserve_bytes("test", 1).unwrap());
        assert_eq!(pool.snapshot().free_space_probe_count, 2);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 2);

        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn refreshed_free_space_still_enforces_the_reserve() {
        let memory = test_memory("exhausted", 8);
        let probe = Arc::new(AdjustableSpaceProbe::new(128));
        let pool = SpillPool::open_with_probe(&memory, probe.clone()).unwrap();

        drop(pool.reserve_bytes("test", 8).unwrap());
        probe.available.store(8, Ordering::Relaxed);
        let error = match pool.reserve_bytes("test", 8) {
            Ok(_) => panic!("exhausted free space must reject a reservation"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("min_spill_free_bytes"));
        assert_eq!(pool.snapshot().free_space_probe_count, 2);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 2);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }

    #[test]
    fn concurrent_reservations_share_a_probe_without_holding_the_pool_mutex() {
        let memory = test_memory("concurrent", 1024);
        let probe = Arc::new(BlockingSpaceProbe::default());
        let pool = Arc::new(SpillPool::open_with_probe(&memory, probe.clone()).unwrap());

        let first_pool = Arc::clone(&pool);
        let first = thread::spawn(move || first_pool.reserve_bytes("first", 8));
        probe.wait_until_started();
        let second_pool = Arc::clone(&pool);
        let second = thread::spawn(move || second_pool.reserve_bytes("second", 8));

        let snapshot_pool = Arc::clone(&pool);
        let (snapshot_tx, snapshot_rx) = mpsc::channel();
        let snapshot = thread::spawn(move || {
            snapshot_pool.snapshot();
            snapshot_tx.send(()).unwrap();
        });
        snapshot_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("pool mutex must remain available during a free-space probe");

        probe.release();
        drop(first.join().unwrap().unwrap());
        drop(second.join().unwrap().unwrap());
        snapshot.join().unwrap();
        assert_eq!(pool.snapshot().free_space_probe_count, 1);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        std::fs::remove_dir(&memory.spill_directory).unwrap();
    }
}
