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

use std::io;

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
}
