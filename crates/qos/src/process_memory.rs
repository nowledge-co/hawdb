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

use std::fmt::{self, Debug, Formatter};
use std::io;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

const DEFAULT_PROCESS_MEMORY_SAMPLE_MAX_AGE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessMemoryCapabilities {
    pub resident_memory: bool,
    pub total_page_faults: bool,
    pub split_page_faults: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemorySnapshot {
    pub capabilities: ProcessMemoryCapabilities,
    pub resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

impl ProcessMemorySnapshot {
    pub fn capture() -> io::Result<Self> {
        capture_process_memory()
    }
}

/// Host-owned limits for process-wide resident-memory admission.
///
/// This policy is separate from an individual governor's work-memory budget.
/// Share one policy between every governor embedded in the same host process
/// when they must coordinate against a common resident-memory ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryPolicyConfig {
    pub resident_limit_bytes: NonZeroU64,
    pub recovery_headroom_bytes: u64,
    pub sample_max_age: Duration,
}

impl ProcessMemoryPolicyConfig {
    pub const fn new(resident_limit_bytes: NonZeroU64) -> Self {
        Self {
            resident_limit_bytes,
            recovery_headroom_bytes: 0,
            sample_max_age: DEFAULT_PROCESS_MEMORY_SAMPLE_MAX_AGE,
        }
    }

    /// Requires this much observed headroom before an RSS-pressure pause lifts.
    pub const fn with_recovery_headroom(mut self, recovery_headroom_bytes: u64) -> Self {
        self.recovery_headroom_bytes = recovery_headroom_bytes;
        self
    }

    /// Bounds how long a caller-supplied RSS sample may govern admission.
    pub const fn with_sample_max_age(mut self, sample_max_age: Duration) -> Self {
        self.sample_max_age = sample_max_age;
        self
    }
}

/// A stable, shareable process-memory policy managed by the embedding host.
///
/// The host updates this policy at its chosen bounded refresh cadence. The
/// policy never starts a polling thread and never samples on an admission hot
/// path. Missing, unsupported, or failed samples fail closed for new work;
/// active reservations remain valid until their permits release them.
#[derive(Clone)]
pub struct ProcessMemoryPolicy {
    inner: Arc<Mutex<ProcessMemoryPolicyState>>,
}

struct ProcessMemoryPolicyState {
    config: ProcessMemoryPolicyConfig,
    sampled_resident_bytes: Option<u64>,
    sampled_at: Option<Instant>,
    unobserved_reserved_bytes: u64,
    next_reservation_id: u64,
    unobserved_reservations: BTreeMap<u64, u64>,
    admission_paused: bool,
    notifiers: Vec<Weak<dyn ProcessMemoryPolicyNotifier>>,
}

/// Current observable state for a host-owned process-memory policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryPolicySnapshot {
    pub resident_limit_bytes: u64,
    pub recovery_headroom_bytes: u64,
    pub sampled_resident_bytes: Option<u64>,
    pub sample_is_current: bool,
    pub unobserved_reserved_bytes: u64,
    pub available_bytes: Option<u64>,
    pub admission_paused: bool,
}

pub(crate) trait ProcessMemoryPolicyNotifier: Send + Sync {
    fn notify_process_memory_policy_changed(&self);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessMemoryAdmissionCode {
    SampleUnavailable,
    ResidentLimitExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessMemoryAdmissionError {
    pub code: ProcessMemoryAdmissionCode,
    pub requested: u64,
    pub available: u64,
    pub retryable: bool,
}

#[derive(Debug)]
pub(crate) struct ProcessMemoryReservation {
    policy: ProcessMemoryPolicy,
    id: u64,
    released: bool,
}

impl ProcessMemoryPolicy {
    pub fn new(config: ProcessMemoryPolicyConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProcessMemoryPolicyState {
                config,
                sampled_resident_bytes: None,
                sampled_at: None,
                unobserved_reserved_bytes: 0,
                next_reservation_id: 1,
                unobserved_reservations: BTreeMap::new(),
                admission_paused: true,
                notifiers: Vec::new(),
            })),
        }
    }

    /// Captures the current process RSS once and returns a ready policy.
    pub fn from_current_process(config: ProcessMemoryPolicyConfig) -> io::Result<Self> {
        let policy = Self::new(config);
        policy.refresh_from_host()?;
        Ok(policy)
    }

    /// Replaces the current sample using a caller-owned process snapshot.
    ///
    /// A snapshot without resident-memory support clears the previous sample
    /// so later admission fails closed instead of relying on stale RSS.
    pub fn update(&self, snapshot: ProcessMemorySnapshot) -> bool {
        self.update_sample(
            snapshot
                .capabilities
                .resident_memory
                .then_some(snapshot.resident_bytes),
        )
    }

    /// Captures and applies one RSS sample. A failed capture clears any old
    /// sample before the error returns, preventing stale feedback admission.
    pub fn refresh_from_host(&self) -> io::Result<bool> {
        match ProcessMemorySnapshot::capture() {
            Ok(snapshot) => Ok(self.update(snapshot)),
            Err(error) => {
                self.clear_sample();
                Err(error)
            }
        }
    }

    /// Clears the current sample and blocks new process-memory admission.
    pub fn clear_sample(&self) -> bool {
        self.update_sample(None)
    }

    pub fn snapshot(&self) -> ProcessMemoryPolicySnapshot {
        let state = policy_lock(&self.inner);
        policy_snapshot(&state)
    }

    pub(crate) fn resident_limit_bytes(&self) -> u64 {
        policy_lock(&self.inner).config.resident_limit_bytes.get()
    }

    pub(crate) fn register_notifier(&self, notifier: Weak<dyn ProcessMemoryPolicyNotifier>) {
        let mut state = policy_lock(&self.inner);
        state
            .notifiers
            .retain(|notifier| notifier.strong_count() > 0);
        state.notifiers.push(notifier);
    }

    pub(crate) fn try_reserve(
        &self,
        requested: u64,
    ) -> Result<ProcessMemoryReservation, ProcessMemoryAdmissionError> {
        let result = {
            let mut state = policy_lock(&self.inner);
            let limit = state.config.resident_limit_bytes.get();
            if requested > limit {
                Err(ProcessMemoryAdmissionError {
                    code: ProcessMemoryAdmissionCode::ResidentLimitExceeded,
                    requested,
                    available: limit,
                    retryable: false,
                })
            } else if !state.sample_is_current(Instant::now()) {
                Err(ProcessMemoryAdmissionError {
                    code: ProcessMemoryAdmissionCode::SampleUnavailable,
                    requested,
                    available: 0,
                    retryable: true,
                })
            } else {
                let available = policy_available_bytes(&state).unwrap_or_default();
                if state.admission_paused || requested > available {
                    Err(ProcessMemoryAdmissionError {
                        code: ProcessMemoryAdmissionCode::ResidentLimitExceeded,
                        requested,
                        available,
                        retryable: true,
                    })
                } else {
                    state.unobserved_reserved_bytes =
                        state.unobserved_reserved_bytes.saturating_add(requested);
                    let id = state.next_reservation_id;
                    state.next_reservation_id = state.next_reservation_id.wrapping_add(1).max(1);
                    let previous = state.unobserved_reservations.insert(id, requested);
                    debug_assert!(previous.is_none(), "reservation ids do not collide");
                    Ok(id)
                }
            }
        };
        result.map(|id| ProcessMemoryReservation {
            policy: self.clone(),
            id,
            released: false,
        })
    }

    fn update_sample(&self, sampled_resident_bytes: Option<u64>) -> bool {
        let (changed, notifiers) = {
            let mut state = policy_lock(&self.inner);
            let before = policy_snapshot(&state);
            if let Some(sampled_resident_bytes) = sampled_resident_bytes {
                let observed_growth = state
                    .sampled_resident_bytes
                    .map(|previous| sampled_resident_bytes.saturating_sub(previous))
                    .unwrap_or_default();
                consume_observed_reservations(&mut state, observed_growth);
                state.sampled_resident_bytes = Some(sampled_resident_bytes);
                state.sampled_at = Some(Instant::now());
                let used = sampled_resident_bytes.saturating_add(state.unobserved_reserved_bytes);
                let limit = state.config.resident_limit_bytes.get();
                let recovery_headroom = state.recovery_headroom_bytes();
                if used >= limit {
                    state.admission_paused = true;
                } else if state.admission_paused && used.saturating_add(recovery_headroom) <= limit
                {
                    state.admission_paused = false;
                }
            } else {
                state.sampled_resident_bytes = None;
                state.sampled_at = None;
                state.admission_paused = true;
            }
            let changed = before != policy_snapshot(&state);
            let notifiers = if changed {
                collect_notifiers(&mut state)
            } else {
                Vec::new()
            };
            (changed, notifiers)
        };
        if changed {
            notify_policy_change(notifiers);
        }
        changed
    }

    fn release_reservation(&self, id: u64) {
        let (changed, notifiers) = {
            let mut state = policy_lock(&self.inner);
            let previous = state.unobserved_reserved_bytes;
            let remaining = state
                .unobserved_reservations
                .remove(&id)
                .unwrap_or_default();
            state.unobserved_reserved_bytes =
                state.unobserved_reserved_bytes.saturating_sub(remaining);
            let changed = state.unobserved_reserved_bytes != previous;
            let notifiers = if changed {
                collect_notifiers(&mut state)
            } else {
                Vec::new()
            };
            (changed, notifiers)
        };
        if changed {
            notify_policy_change(notifiers);
        }
    }
}

impl Debug for ProcessMemoryPolicy {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessMemoryPolicy")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ProcessMemoryPolicyState {
    fn recovery_headroom_bytes(&self) -> u64 {
        self.config
            .recovery_headroom_bytes
            .min(self.config.resident_limit_bytes.get())
    }

    fn sample_is_current(&self, now: Instant) -> bool {
        self.sampled_at.is_some_and(|sampled_at| {
            now.saturating_duration_since(sampled_at) <= self.config.sample_max_age
        })
    }
}

impl ProcessMemoryReservation {
    pub(crate) fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.policy.release_reservation(self.id);
        self.released = true;
    }
}

impl Drop for ProcessMemoryReservation {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn policy_snapshot(state: &ProcessMemoryPolicyState) -> ProcessMemoryPolicySnapshot {
    let sample_is_current = state.sample_is_current(Instant::now());
    ProcessMemoryPolicySnapshot {
        resident_limit_bytes: state.config.resident_limit_bytes.get(),
        recovery_headroom_bytes: state.recovery_headroom_bytes(),
        sampled_resident_bytes: state.sampled_resident_bytes,
        sample_is_current,
        unobserved_reserved_bytes: state.unobserved_reserved_bytes,
        available_bytes: sample_is_current
            .then(|| policy_available_bytes(state))
            .flatten(),
        admission_paused: state.admission_paused || !sample_is_current,
    }
}

fn policy_available_bytes(state: &ProcessMemoryPolicyState) -> Option<u64> {
    state.sampled_resident_bytes.map(|resident_bytes| {
        state
            .config
            .resident_limit_bytes
            .get()
            .saturating_sub(resident_bytes.saturating_add(state.unobserved_reserved_bytes))
    })
}

fn consume_observed_reservations(state: &mut ProcessMemoryPolicyState, observed_growth: u64) {
    let mut remaining_growth = observed_growth.min(state.unobserved_reserved_bytes);
    for reserved_bytes in state.unobserved_reservations.values_mut() {
        let consumed = (*reserved_bytes).min(remaining_growth);
        *reserved_bytes = reserved_bytes.saturating_sub(consumed);
        remaining_growth = remaining_growth.saturating_sub(consumed);
        state.unobserved_reserved_bytes = state.unobserved_reserved_bytes.saturating_sub(consumed);
        if remaining_growth == 0 {
            break;
        }
    }
}

fn collect_notifiers(
    state: &mut ProcessMemoryPolicyState,
) -> Vec<Arc<dyn ProcessMemoryPolicyNotifier>> {
    let mut notifiers = Vec::with_capacity(state.notifiers.len());
    state.notifiers.retain(|notifier| {
        if let Some(notifier) = notifier.upgrade() {
            notifiers.push(notifier);
            true
        } else {
            false
        }
    });
    notifiers
}

fn notify_policy_change(notifiers: Vec<Arc<dyn ProcessMemoryPolicyNotifier>>) {
    for notifier in notifiers {
        notifier.notify_process_memory_policy_changed();
    }
}

fn policy_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemoryProfile {
    pub capabilities: ProcessMemoryCapabilities,
    pub start_resident_bytes: u64,
    pub start_peak_resident_bytes: u64,
    pub steady_resident_bytes: u64,
    pub peak_resident_bytes: u64,
    pub steady_resident_growth_bytes: u64,
    pub lifetime_peak_resident_growth_bytes: u64,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

impl ProcessMemoryProfile {
    pub fn between(start: ProcessMemorySnapshot, end: ProcessMemorySnapshot) -> Self {
        Self {
            capabilities: ProcessMemoryCapabilities {
                resident_memory: start.capabilities.resident_memory
                    && end.capabilities.resident_memory,
                total_page_faults: start.capabilities.total_page_faults
                    && end.capabilities.total_page_faults,
                split_page_faults: start.capabilities.split_page_faults
                    && end.capabilities.split_page_faults,
            },
            start_resident_bytes: start.resident_bytes,
            start_peak_resident_bytes: start.peak_resident_bytes,
            steady_resident_bytes: end.resident_bytes,
            peak_resident_bytes: end.peak_resident_bytes,
            steady_resident_growth_bytes: end.resident_bytes.saturating_sub(start.resident_bytes),
            lifetime_peak_resident_growth_bytes: end
                .peak_resident_bytes
                .saturating_sub(start.peak_resident_bytes),
            total_page_faults: counter_delta(start.total_page_faults, end.total_page_faults),
            minor_page_faults: counter_delta(start.minor_page_faults, end.minor_page_faults),
            major_page_faults: counter_delta(start.major_page_faults, end.major_page_faults),
        }
    }
}

fn counter_delta(start: Option<u64>, end: Option<u64>) -> Option<u64> {
    start.zip(end).map(|(start, end)| end.saturating_sub(start))
}

#[cfg(unix)]
#[allow(unsafe_code, reason = "process-local getrusage FFI")]
fn capture_process_memory() -> io::Result<ProcessMemorySnapshot> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage value on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful getrusage call initialized usage.
    let usage = unsafe { usage.assume_init() };
    let minor_page_faults = non_negative_counter(usage.ru_minflt);
    let major_page_faults = non_negative_counter(usage.ru_majflt);
    Ok(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            total_page_faults: true,
            split_page_faults: true,
        },
        resident_bytes: current_resident_bytes()?,
        peak_resident_bytes: peak_resident_bytes(usage.ru_maxrss),
        total_page_faults: Some(minor_page_faults.saturating_add(major_page_faults)),
        minor_page_faults: Some(minor_page_faults),
        major_page_faults: Some(major_page_faults),
    })
}

#[cfg(windows)]
#[allow(unsafe_code, reason = "process-local Windows memory counters FFI")]
fn capture_process_memory() -> io::Result<ProcessMemorySnapshot> {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let mut counters = PROCESS_MEMORY_COUNTERS {
        cb: u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>())
            .expect("PROCESS_MEMORY_COUNTERS size fits in u32"),
        ..PROCESS_MEMORY_COUNTERS::default()
    };
    let counters_size = counters.cb;
    // SAFETY: GetCurrentProcess returns a process-local pseudo handle and
    // GetProcessMemoryInfo writes at most counters.cb bytes into counters.
    let captured =
        unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, counters_size) };
    if captured == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            total_page_faults: true,
            split_page_faults: false,
        },
        resident_bytes: u64::try_from(counters.WorkingSetSize).unwrap_or(u64::MAX),
        peak_resident_bytes: u64::try_from(counters.PeakWorkingSetSize).unwrap_or(u64::MAX),
        total_page_faults: Some(u64::from(counters.PageFaultCount)),
        minor_page_faults: None,
        major_page_faults: None,
    })
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "process-local Mach memory counters FFI")]
fn current_resident_bytes() -> io::Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    #[allow(deprecated)]
    // SAFETY: reading the current process task port does not transfer ownership.
    let task = unsafe { libc::mach_task_self() };
    // SAFETY: task_info writes at most count natural_t values into the correctly sized buffer.
    let status = unsafe {
        libc::task_info(
            task,
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if status != libc::KERN_SUCCESS {
        return Err(io::Error::other(format!(
            "task_info failed with kernel status {status}"
        )));
    }
    // SAFETY: the successful task_info call initialized info.
    let info = unsafe { info.assume_init() };
    Ok(info.resident_size)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "sysconf page-size FFI")]
fn current_resident_bytes() -> io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    let resident_pages = statm
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing resident page count"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    // SAFETY: sysconf is side-effect free for _SC_PAGESIZE.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(resident_pages.saturating_mul(page_size as u64))
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
#[allow(unsafe_code, reason = "process-local getrusage FFI")]
fn current_resident_bytes() -> io::Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the provided rusage value on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful getrusage call initialized usage.
    let usage = unsafe { usage.assume_init() };
    Ok(peak_resident_bytes(usage.ru_maxrss))
}

#[cfg(target_os = "macos")]
fn peak_resident_bytes(max_rss: libc::c_long) -> u64 {
    non_negative_counter(max_rss)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn peak_resident_bytes(max_rss: libc::c_long) -> u64 {
    non_negative_counter(max_rss).saturating_mul(1024)
}

#[cfg(unix)]
fn non_negative_counter(value: libc::c_long) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

#[cfg(not(any(unix, windows)))]
fn capture_process_memory() -> io::Result<ProcessMemorySnapshot> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process memory sampling is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_snapshot(resident_bytes: Option<u64>) -> ProcessMemorySnapshot {
        ProcessMemorySnapshot {
            capabilities: ProcessMemoryCapabilities {
                resident_memory: resident_bytes.is_some(),
                total_page_faults: false,
                split_page_faults: false,
            },
            resident_bytes: resident_bytes.unwrap_or_default(),
            peak_resident_bytes: resident_bytes.unwrap_or_default(),
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
        }
    }

    fn policy(limit: u64) -> ProcessMemoryPolicy {
        ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
            NonZeroU64::new(limit).unwrap(),
        ))
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn captures_process_memory_and_fault_counters() {
        let snapshot = ProcessMemorySnapshot::capture().unwrap();
        assert!(snapshot.capabilities.resident_memory);
        assert!(snapshot.capabilities.total_page_faults);
        assert!(snapshot.resident_bytes > 0);
        assert!(snapshot.peak_resident_bytes > 0);
        assert!(snapshot.total_page_faults.is_some());
        assert_eq!(snapshot.capabilities.split_page_faults, cfg!(unix));
        assert_eq!(snapshot.minor_page_faults.is_some(), cfg!(unix));
        assert_eq!(snapshot.major_page_faults.is_some(), cfg!(unix));
    }

    #[test]
    fn profile_deltas_are_saturating() {
        let start = ProcessMemorySnapshot {
            capabilities: ProcessMemoryCapabilities {
                resident_memory: true,
                total_page_faults: true,
                split_page_faults: true,
            },
            resident_bytes: 10,
            peak_resident_bytes: 20,
            total_page_faults: Some(12),
            minor_page_faults: Some(8),
            major_page_faults: Some(4),
        };
        let end = ProcessMemorySnapshot {
            capabilities: start.capabilities,
            resident_bytes: 12,
            peak_resident_bytes: 24,
            total_page_faults: Some(12),
            minor_page_faults: Some(3),
            major_page_faults: Some(9),
        };
        let profile = ProcessMemoryProfile::between(start, end);
        assert_eq!(profile.start_resident_bytes, 10);
        assert_eq!(profile.start_peak_resident_bytes, 20);
        assert_eq!(profile.steady_resident_bytes, 12);
        assert_eq!(profile.peak_resident_bytes, 24);
        assert_eq!(profile.steady_resident_growth_bytes, 2);
        assert_eq!(profile.lifetime_peak_resident_growth_bytes, 4);
        assert_eq!(profile.total_page_faults, Some(0));
        assert_eq!(profile.minor_page_faults, Some(0));
        assert_eq!(profile.major_page_faults, Some(5));
    }

    #[test]
    fn policy_fails_closed_without_a_supported_sample() {
        let policy = policy(100);
        let error = policy.try_reserve(1).unwrap_err();
        assert_eq!(error.code, ProcessMemoryAdmissionCode::SampleUnavailable);
        assert!(error.retryable);

        assert!(policy.update(policy_snapshot(Some(40))));
        let reservation = policy.try_reserve(10).unwrap();
        assert_eq!(policy.snapshot().available_bytes, Some(50));
        drop(reservation);

        assert!(policy.update(policy_snapshot(None)));
        assert!(policy.snapshot().sampled_resident_bytes.is_none());
        assert!(policy.snapshot().admission_paused);
    }

    #[test]
    fn policy_fails_closed_after_its_sample_expires() {
        let policy = ProcessMemoryPolicy::new(
            ProcessMemoryPolicyConfig::new(NonZeroU64::new(100).unwrap())
                .with_sample_max_age(Duration::from_millis(1)),
        );
        policy.update(policy_snapshot(Some(40)));
        {
            let mut state = policy_lock(&policy.inner);
            state.sampled_at = Some(Instant::now() - Duration::from_secs(1));
        }

        let snapshot = policy.snapshot();
        assert_eq!(snapshot.sampled_resident_bytes, Some(40));
        assert!(!snapshot.sample_is_current);
        assert_eq!(snapshot.available_bytes, None);
        let error = policy.try_reserve(1).unwrap_err();
        assert_eq!(error.code, ProcessMemoryAdmissionCode::SampleUnavailable);
        assert!(error.retryable);
    }

    #[test]
    fn sampled_growth_absorbs_reserved_headroom_without_double_charging() {
        let policy = policy(100);
        policy.update(policy_snapshot(Some(40)));
        let reservation = policy.try_reserve(30).unwrap();
        assert_eq!(policy.snapshot().unobserved_reserved_bytes, 30);
        assert_eq!(policy.snapshot().available_bytes, Some(30));

        policy.update(policy_snapshot(Some(60)));
        let snapshot = policy.snapshot();
        assert_eq!(snapshot.unobserved_reserved_bytes, 10);
        assert_eq!(snapshot.available_bytes, Some(30));

        drop(reservation);
        let snapshot = policy.snapshot();
        assert_eq!(snapshot.unobserved_reserved_bytes, 0);
        assert_eq!(snapshot.available_bytes, Some(40));
    }

    #[test]
    fn releasing_an_observed_reservation_keeps_later_headroom_reserved() {
        let policy = policy(100);
        policy.update(policy_snapshot(Some(40)));
        let first = policy.try_reserve(30).unwrap();

        policy.update(policy_snapshot(Some(60)));
        let second = policy.try_reserve(10).unwrap();
        assert_eq!(policy.snapshot().unobserved_reserved_bytes, 20);

        drop(first);
        let snapshot = policy.snapshot();
        assert_eq!(snapshot.unobserved_reserved_bytes, 10);
        assert_eq!(snapshot.available_bytes, Some(30));

        drop(second);
        assert_eq!(policy.snapshot().available_bytes, Some(40));
    }

    #[test]
    fn policy_requires_configured_headroom_before_recovering() {
        let policy = ProcessMemoryPolicy::new(
            ProcessMemoryPolicyConfig::new(NonZeroU64::new(100).unwrap())
                .with_recovery_headroom(20),
        );
        policy.update(policy_snapshot(Some(100)));
        assert!(policy.snapshot().admission_paused);

        policy.update(policy_snapshot(Some(90)));
        assert!(policy.snapshot().admission_paused);

        policy.update(policy_snapshot(Some(80)));
        assert!(!policy.snapshot().admission_paused);
        assert!(policy.try_reserve(1).is_ok());
    }
}
