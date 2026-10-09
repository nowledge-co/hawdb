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

//! Startup-only process allowance for a configured project descriptor budget.

use hawdb_core::error::HawDBError;

#[cfg(all(unix, not(target_arch = "wasm32")))]
pub(super) const HOST_HEADROOM: usize = 64;

#[cfg(all(unix, not(target_arch = "wasm32")))]
mod native {
    use super::{HawDBError, HOST_HEADROOM};
    use hawdb_core::error::FileDescriptorError;
    use std::io;
    use std::sync::Mutex;

    // Serialize this library's read/raise sequence across independent projects.
    static LIMIT_UPDATE: Mutex<()> = Mutex::new(());

    fn value<T: Into<u64>>(limit: T) -> u64 {
        limit.into()
    }

    fn rejected(limit: Option<libc::rlimit>, requested: usize, os_code: Option<i32>) -> HawDBError {
        HawDBError::FileDescriptors(FileDescriptorError::OsLimit {
            requested,
            os_code,
            soft: limit.as_ref().map(|limit| value(limit.rlim_cur)),
            hard: limit.as_ref().map(|limit| value(limit.rlim_max)),
        })
    }

    fn read() -> io::Result<libc::rlimit> {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: limit is writable, correctly sized, and lives through the call.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(limit)
    }

    pub(super) fn ensure_capacity(budget: usize) -> Result<(), HawDBError> {
        let invalid =
            || HawDBError::FileDescriptors(FileDescriptorError::InvalidBudget { limit: budget });
        let requested = budget.checked_add(HOST_HEADROOM).ok_or_else(invalid)?;
        let required = libc::rlim_t::try_from(requested).map_err(|_| invalid())?;
        let _update = LIMIT_UPDATE
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let current = read().map_err(|error| rejected(None, requested, error.raw_os_error()))?;
        if current.rlim_cur >= required {
            return Ok(());
        }
        let raised = libc::rlimit {
            rlim_cur: required.min(current.rlim_max),
            rlim_max: current.rlim_max,
        };
        // SAFETY: raised is initialized; the hard limit is unchanged and this
        // library serializes its own updates. Hosts synchronize external changes.
        let os_code = if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
            None
        } else {
            io::Error::last_os_error().raw_os_error()
        };
        let effective = read().map_err(|error| rejected(None, requested, error.raw_os_error()))?;
        if effective.rlim_cur < required {
            return Err(rejected(Some(effective), requested, os_code));
        }
        Ok(())
    }

    pub(super) fn current_soft_limit() -> Option<u64> {
        read().ok().map(|limit| value(limit.rlim_cur))
    }
}

pub(super) fn ensure_capacity(budget: usize) -> Result<(), HawDBError> {
    #[cfg(all(unix, not(target_arch = "wasm32")))]
    {
        native::ensure_capacity(budget)
    }
    #[cfg(not(all(unix, not(target_arch = "wasm32"))))]
    {
        let _ = budget;
        Ok(())
    }
}

pub(super) fn current_soft_limit() -> Option<u64> {
    #[cfg(all(unix, not(target_arch = "wasm32")))]
    {
        native::current_soft_limit()
    }
    #[cfg(not(all(unix, not(target_arch = "wasm32"))))]
    None
}
