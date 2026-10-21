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

//! Resize the actual append object without looking up its saved pathname.

use super::*;

impl File {
    pub(crate) fn rollback_wal_append(
        &self,
        committed_bytes: u64,
        empty_header: &[u8],
    ) -> io::Result<u64> {
        // Refuse logical immutable files before acquiring any mutable authority.
        self.writable_native()?;
        #[cfg(windows)]
        let rollback = self.reopen_wal_for_rollback()?;
        #[cfg(not(windows))]
        let rollback = self;
        if rollback.metadata()?.len() < committed_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL lost previously appended bytes before rollback",
            ));
        }
        rollback.set_len(committed_bytes)?;
        let restored_bytes = if committed_bytes == 0 {
            // Missing headers remain corruption during ordinary recovery.
            // Restore the valid empty-generation baseline through the original
            // append object instead of unlinking or reopening its saved name.
            let mut writer = self;
            writer.write_all(empty_header)?;
            empty_header.len() as u64
        } else {
            committed_bytes
        };
        rollback.sync_all()?;
        Ok(restored_bytes)
    }

    #[cfg(windows)]
    fn reopen_wal_for_rollback(&self) -> io::Result<Self> {
        use std::ffi::c_void;
        use std::os::windows::io::{AsRawHandle, FromRawHandle};
        #[link(name = "kernel32")]
        unsafe extern "system" {
            #[link_name = "ReOpenFile"]
            fn reopen_file(
                original: *mut c_void,
                desired_access: u32,
                share_mode: u32,
                flags: u32,
            ) -> *mut c_void;
        }
        let FileBacking::Native(file) = &self.backing else {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "WAL rollback requires a native append handle",
            ));
        };
        // Append-only Windows handles cannot resize. Charge before asking the
        // kernel for GENERIC_WRITE on the same object, with read/write/delete
        // sharing. No pathname is supplied, and sharing/ACL refusal fails closed.
        let permit = file.permit.context.acquire(file.permit.kind())?;
        // SAFETY: the original remains owned and live across this synchronous
        // call. A successful result transfers one new owned native handle.
        let raw = unsafe { reopen_file(file.inner.as_raw_handle(), 0x4000_0000, 7, 0) };
        if raw == -1isize as *mut c_void {
            return Err(file
                .permit
                .context
                .map_open_error(io::Error::last_os_error()));
        }
        // SAFETY: ReOpenFile returned a fresh, valid owned handle above.
        let inner = unsafe { std::fs::File::from_raw_handle(raw) };
        if crate::native_file_identity::of_file(&inner)?
            != crate::native_file_identity::of_file(&file.inner)?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL rollback handle does not name the original append object",
            ));
        }
        Ok(Self {
            backing: FileBacking::Native(NativeFile {
                inner,
                permit,
                #[cfg(any(test, feature = "test-support"))]
                trace: file.trace.clone(),
            }),
        })
    }
}
