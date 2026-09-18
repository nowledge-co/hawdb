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

#[cfg(test)]
use std::ffi::OsString;
#[cfg(not(windows))]
use std::fs;
use std::io;
use std::path::Path;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalSyncGroupProgress {
    pub entry_count: usize,
    pub byte_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalSyncGroupFlush {
    pub entry_count: usize,
    pub byte_count: u64,
    pub fsync_micros: u64,
    pub fsync_performed: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalSyncGroupState {
    entry_count: usize,
    byte_count: u64,
    created: bool,
}

impl WalSyncGroupState {
    pub fn record_entry(&mut self, byte_count: u64) {
        self.entry_count = self.entry_count.saturating_add(1);
        self.byte_count = self.byte_count.saturating_add(byte_count);
    }

    pub fn record_wal_created(&mut self) {
        self.created = true;
    }

    pub const fn is_empty(self) -> bool {
        self.entry_count == 0
    }

    pub const fn requires_parent_sync(self) -> bool {
        self.created
    }

    pub const fn progress(self) -> WalSyncGroupProgress {
        WalSyncGroupProgress {
            entry_count: self.entry_count,
            byte_count: self.byte_count,
        }
    }

    pub const fn into_flush(self, fsync_micros: u64) -> WalSyncGroupFlush {
        if self.entry_count == 0 {
            return WalSyncGroupFlush {
                entry_count: 0,
                byte_count: 0,
                fsync_micros: 0,
                fsync_performed: false,
            };
        }
        WalSyncGroupFlush {
            entry_count: self.entry_count,
            byte_count: self.byte_count,
            fsync_micros,
            fsync_performed: true,
        }
    }
}

/// Atomically publishes a file whose contents have already been synchronized.
///
/// Unix persists the directory entry after rename. Windows uses a write-through
/// move because flushing a directory handle is not a supported durability
/// primitive there.
pub fn durable_replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    inject_durable_replace_failure(destination)?;
    #[cfg(windows)]
    {
        durable_replace_file_windows(source, destination)
    }
    #[cfg(not(windows))]
    {
        fs::rename(source, destination)?;
        sync_parent_directory(destination)
    }
}

#[cfg(test)]
thread_local! {
    static DURABLE_REPLACE_FAILURE_DESTINATION: std::cell::RefCell<Option<OsString>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) struct DurableReplaceFailureGuard;

#[cfg(test)]
impl Drop for DurableReplaceFailureGuard {
    fn drop(&mut self) {
        DURABLE_REPLACE_FAILURE_DESTINATION.with(|destination| {
            destination.replace(None);
        });
    }
}

#[cfg(test)]
pub(crate) fn fail_durable_replace_for_destination(
    destination: impl Into<OsString>,
) -> DurableReplaceFailureGuard {
    DURABLE_REPLACE_FAILURE_DESTINATION.with(|current| {
        assert!(
            current.borrow().is_none(),
            "durable replace failure injection must not be nested"
        );
        current.replace(Some(destination.into()));
    });
    DurableReplaceFailureGuard
}

#[cfg(test)]
fn inject_durable_replace_failure(destination: &Path) -> io::Result<()> {
    let should_fail = DURABLE_REPLACE_FAILURE_DESTINATION.with(|expected| {
        expected
            .borrow()
            .as_ref()
            .is_some_and(|expected| destination.file_name() == Some(expected.as_os_str()))
    });
    if should_fail {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "injected durable replace failure for {}",
                destination.display()
            ),
        ));
    }
    Ok(())
}

/// Persists a directory entry change when the platform exposes that primitive.
pub fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_directory(parent)
}

/// Persists pending directory entry changes when supported by the platform.
pub fn sync_directory(directory: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        let _ = directory;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        use std::fs::File;

        File::open(directory)?.sync_all()
    }
}

#[cfg(windows)]
fn durable_replace_file_windows(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;

    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both paths are live, NUL-terminated UTF-16 buffers for the call.
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(windows)]
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn wal_sync_group_state_owns_bounded_flush_accounting() {
        let mut group = WalSyncGroupState::default();
        assert!(group.is_empty());
        assert_eq!(group.progress(), WalSyncGroupProgress::default());
        assert_eq!(group.into_flush(42), WalSyncGroupFlush::default());

        group.record_wal_created();
        group.record_entry(128);
        group.record_entry(64);
        assert_eq!(
            group.progress(),
            WalSyncGroupProgress {
                entry_count: 2,
                byte_count: 192,
            }
        );
        assert!(group.requires_parent_sync());
        assert_eq!(
            group.into_flush(42),
            WalSyncGroupFlush {
                entry_count: 2,
                byte_count: 192,
                fsync_micros: 42,
                fsync_performed: true,
            }
        );
    }

    #[test]
    fn durable_replace_publishes_and_replaces_content() {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let candidate = root.join("candidate.hawdb");
        let published = root.join("published.hawdb");

        write_synced(&candidate, b"first");
        durable_replace_file(&candidate, &published).unwrap();
        assert_eq!(fs::read(&published).unwrap(), b"first");
        assert!(!candidate.exists());

        write_synced(&candidate, b"second");
        durable_replace_file(&candidate, &published).unwrap();
        assert_eq!(fs::read(&published).unwrap(), b"second");
        assert!(!candidate.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn platform_obstruction_rejects_canonical_row_and_overflow_manifest_replace() {
        for destination_name in [
            "relational-row-pages-1.manifest.hawdb",
            "relational-overflow-1.manifest.hawdb",
        ] {
            let root = unique_test_dir();
            fs::create_dir_all(&root).unwrap();
            let candidate = root.join(format!("{destination_name}.tmp"));
            let destination = root.join(destination_name);
            write_synced(&candidate, b"candidate");

            #[cfg(windows)]
            let obstruction = {
                use std::os::windows::fs::OpenOptionsExt;

                const FILE_SHARE_READ: u32 = 0x0000_0001;
                write_synced(&destination, b"selected");
                OpenOptions::new()
                    .read(true)
                    .share_mode(FILE_SHARE_READ)
                    .open(&destination)
                    .unwrap()
            };
            #[cfg(not(windows))]
            fs::create_dir(&destination).unwrap();

            let error = durable_replace_file(&candidate, &destination).unwrap_err();
            assert!(candidate.exists());

            #[cfg(windows)]
            {
                drop(obstruction);
                fs::remove_file(&destination).unwrap();
            }
            #[cfg(not(windows))]
            fs::remove_dir(&destination).unwrap();

            assert_ne!(error.kind(), io::ErrorKind::NotFound);
            fs::remove_dir_all(root).unwrap();
        }
    }

    fn write_synced(path: &Path, bytes: &[u8]) {
        let mut file = fs::File::create(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    fn unique_test_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hawdb-durable-replace-{}-{}",
            std::process::id(),
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
