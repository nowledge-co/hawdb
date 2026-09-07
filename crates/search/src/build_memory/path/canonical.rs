//! Match native canonicalization while admitting the owned Rust output first.
//!
//! OS/libc resolver internals and std's native open-path conversion are not
//! allocator/RSS-accounted. No fixed PATH_MAX bound is imposed on their result.

use super::{OwnedPath, Path, PathBuf};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use std::ffi::OsString;
use std::io;

fn native_error(error: io::Error) -> SkeinError {
    SkeinError::Storage(format!(
        "failed to resolve search projection directory: {error}"
    ))
}

#[cfg(unix)]
pub(super) fn resolve(
    path: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<OwnedPath> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStringExt;

    let bytes = path.as_os_str().as_encoded_bytes();
    if bytes.contains(&0) {
        return Err(native_error(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains a NUL byte",
        )));
    }
    let input_bytes = crate::build_memory::checked_add(bytes.len(), 1)?;
    let _input_memory = memory.retained.reserve(input_bytes)?;
    let mut input = Vec::with_capacity(input_bytes);
    input.extend_from_slice(bytes);
    input.push(0);
    checkpoint(task)?;
    // SAFETY: input is NUL-terminated and remains live for the call. Passing a
    // null output requests libc-owned storage, released exactly once below.
    let resolved =
        NativePath(unsafe { libc::realpath(input.as_ptr().cast(), std::ptr::null_mut()) });
    if resolved.0.is_null() {
        return Err(native_error(io::Error::last_os_error()));
    }
    checkpoint(task)?;
    // SAFETY: successful realpath returns a NUL-terminated allocation that is
    // valid until resolved drops. Its length need not fit a fixed PATH_MAX.
    let bytes = unsafe { CStr::from_ptr(resolved.0) }.to_bytes();
    let lease = memory.retained.reserve(bytes.len())?;
    #[cfg(test)]
    evidence::output();
    OwnedPath::finish(
        PathBuf::from(OsString::from_vec(bytes.to_vec())),
        lease,
        task,
    )
}

#[cfg(unix)]
struct NativePath(*mut libc::c_char);

#[cfg(unix)]
impl Drop for NativePath {
    fn drop(&mut self) {
        // SAFETY: the pointer is either null or the unaliased allocation
        // returned by realpath. libc::free accepts null and runs exactly once.
        unsafe { libc::free(self.0.cast()) };
    }
}

#[cfg(windows)]
pub(super) fn resolve(
    path: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<OwnedPath> {
    use std::fs::OpenOptions;
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFinalPathNameByHandleW, FILE_FLAG_BACKUP_SEMANTICS, VOLUME_NAME_DOS,
    };

    // Keep std's native path conversion, long-path handling and directory-open
    // permissions, matching its canonicalize implementation on the pinned toolchain.
    let file = OpenOptions::new()
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .map_err(native_error)?;
    let mut stack = [0u16; 512];
    let mut wide = WidePath {
        value: Vec::new(),
        _memory: memory.retained.reserve(0)?,
    };
    loop {
        checkpoint(task)?;
        let buffer = if wide.value.is_empty() {
            &mut stack[..]
        } else {
            &mut wide.value[..]
        };
        let capacity = u32::try_from(buffer.len()).map_err(|_| {
            SkeinError::Execution("canonical path exceeds the native address space".into())
        })?;
        // SAFETY: the handle is live and buffer has capacity initialized u16s.
        // The API never writes beyond that capacity; success excludes the NUL.
        let length = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                buffer.as_mut_ptr(),
                capacity,
                VOLUME_NAME_DOS,
            )
        };
        if length == 0 {
            return Err(native_error(io::Error::last_os_error()));
        }
        if length >= capacity {
            let capacity = usize::try_from(length)
                .unwrap()
                .max(crate::build_memory::checked_add(buffer.len(), 1)?);
            let lease = memory
                .retained
                .reserve(crate::build_memory::checked_mul(capacity, 2)?)?;
            // Keep the previous wide allocation admitted until replacement.
            let next = WidePath {
                value: vec![0; capacity],
                _memory: lease,
            };
            checkpoint(task)?;
            wide = next;
            continue;
        }
        let units = &buffer[..length as usize];
        // WTF-8 needs at most three bytes per UTF-16 unit. Include amortized
        // growth and old/new allocation overlap before OsString::from_wide.
        let lease = memory
            .retained
            .reserve(crate::build_memory::checked_mul(units.len().max(8), 12)?)?;
        #[cfg(test)]
        evidence::output();
        return OwnedPath::finish(PathBuf::from(OsString::from_wide(units)), lease, task);
    }
}

#[cfg(windows)]
struct WidePath {
    value: Vec<u16>,
    _memory: skein_executor::QueryMemoryLease,
}

#[cfg(not(any(unix, windows)))]
pub(super) fn resolve(
    _path: &Path,
    _memory: &BuildMemory,
    _task: &RuntimeTaskContext,
) -> Result<OwnedPath> {
    Err(SkeinError::Execution(
        "budgeted native canonicalization is unavailable on this platform".into(),
    ))
}

#[cfg(test)]
pub(crate) mod evidence {
    use skein_core::RuntimeCancellationToken;
    use std::cell::{Cell, RefCell};
    thread_local! {
        static OUTPUTS: Cell<usize> = const { Cell::new(0) };
        static CANCEL: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
    }
    pub(super) fn output() {
        OUTPUTS.set(OUTPUTS.get() + 1);
        CANCEL.with_borrow_mut(|token| {
            if let Some(token) = token.take() {
                token.cancel();
            }
        });
    }
    pub(crate) fn take() -> usize {
        OUTPUTS.replace(0)
    }
    pub(crate) fn cancel_next(token: RuntimeCancellationToken) {
        CANCEL.with_borrow_mut(|value| *value = Some(token));
    }
}

#[cfg(test)]
mod tests;
