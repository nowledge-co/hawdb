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

use crate::file_io as fs;
#[cfg(test)]
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug)]
struct DirectorySyncError {
    operation: &'static str,
    path: PathBuf,
    source: io::Error,
}

impl std::fmt::Display for DirectorySyncError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} namespace directory '{}': {}",
            self.operation,
            self.path.display(),
            self.source
        )
    }
}

impl std::error::Error for DirectorySyncError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

fn directory_sync_error(operation: &'static str, path: &Path, source: io::Error) -> io::Error {
    io::Error::new(
        source.kind(),
        DirectorySyncError {
            operation,
            path: path.to_path_buf(),
            source,
        },
    )
}

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
/// Unix persists the directory entry after rename. Windows retains a counted,
/// write-capable candidate handle across the standard library's atomic rename
/// and synchronizes that handle afterward. An open destination reader keeps its
/// old publication. The parent-directory barrier is also requested on Windows;
/// its namespace durability remains a separate platform qualification requirement.
pub fn durable_replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(test)]
    inject_durable_replace_failure(destination)?;
    #[cfg(windows)]
    {
        durable_replace_file_windows(source, destination)?;
        // New readers must see the replacement, while captured immutable
        // readers retain their original publication identity.
        fs::unbind_immutable_path(source)?;
        fs::unbind_immutable_path(destination)?;
        sync_parent_directory(destination)
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
    static SYNC_DIRECTORY_FAILURE: std::cell::RefCell<Option<std::path::PathBuf>> =
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
pub(crate) struct SyncDirectoryFailureGuard;

#[cfg(test)]
impl Drop for SyncDirectoryFailureGuard {
    fn drop(&mut self) {
        SYNC_DIRECTORY_FAILURE.with(|path| {
            path.replace(None);
        });
    }
}

#[cfg(test)]
pub(crate) fn fail_sync_directory_for(directory: &Path) -> SyncDirectoryFailureGuard {
    SYNC_DIRECTORY_FAILURE.with(|current| {
        assert!(
            current.borrow().is_none(),
            "sync directory failure injection must not be nested"
        );
        current.replace(Some(directory.to_path_buf()));
    });
    SyncDirectoryFailureGuard
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
    let context = crate::file_descriptors::context_for_path(directory)
        .map_err(|error| directory_sync_error("resolve", directory, error))?;
    sync_directory_with_context(directory, &context)
}

pub(crate) fn sync_directory_with_context(
    directory: &Path,
    context: &crate::file_descriptors::FileOpenContext,
) -> io::Result<()> {
    #[cfg(test)]
    {
        let should_fail = SYNC_DIRECTORY_FAILURE.with(|expected| {
            expected
                .borrow()
                .as_ref()
                .is_some_and(|path| path == directory)
        });
        if should_fail {
            return Err(directory_sync_error(
                "flush",
                directory,
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected directory sync failure",
                ),
            ));
        }
    }
    #[cfg(windows)]
    let handle = {
        use std::os::windows::fs::OpenOptionsExt;

        // Opening a directory requires BACKUP_SEMANTICS; FlushFileBuffers also
        // requires GENERIC_WRITE. A read-only handle or a silent no-op cannot
        // establish a namespace barrier. Unsupported filesystems and denied
        // permissions must propagate instead of acknowledging a publication.
        // API success alone does not qualify Windows namespace persistence.
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        let mut options = fs::OpenOptions::new();
        options
            .read(true)
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
        context.open(&options, directory)
    };
    #[cfg(not(windows))]
    let handle = context.open(fs::OpenOptions::new().read(true), directory);
    let handle = handle.map_err(|error| directory_sync_error("open", directory, error))?;
    require_directory(&handle)
        .map_err(|error| directory_sync_error("inspect", directory, error))?;
    handle
        .sync_all()
        .map_err(|error| directory_sync_error("flush", directory, error))
}

fn require_directory(file: &fs::File) -> io::Result<()> {
    if file.metadata()?.is_dir() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "namespace synchronization requires a directory",
        ))
    }
}

/// Persists names from a publication directory through its project root.
/// Existing directories can be leftovers of an interrupted attempt: existence
/// alone never replaces these barriers. The project root's own ancestry is
/// synchronized during writable project admission. Detached component scratch
/// can borrow project accounting. Scratch within the explicit host anchor can
/// use that boundary; scratch outside it still needs a full mount walk.
#[doc(hidden)]
pub fn sync_directory_ancestors(directory: &Path) -> io::Result<()> {
    let context = crate::file_descriptors::context_for_path(directory)?;
    let root = context.project_root();
    let stop = (!root.as_os_str().is_empty()).then_some(root);
    sync_directory_tree_with_context(directory, &context, stop)
}

/// Explicit context avoids reentering the project registry during bootstrap.
/// Open one directory at a time, charging even ancestor IO to this project.
/// With no enclosing project boundary, stop at the existing filesystem mount:
/// persistence of the mount itself is an external platform assumption. Without
/// an explicit anchor Windows walks through the volume root, requiring writable
/// directory handles at every level. It never treats access denial as an anchor;
/// native directory-flush namespace semantics remain unqualified.
pub(crate) fn sync_directory_tree_with_context(
    directory: &Path,
    context: &crate::file_descriptors::FileOpenContext,
    stop: Option<&Path>,
) -> io::Result<()> {
    let mut current = context
        .canonicalize(directory)
        .map_err(|error| directory_sync_error("resolve", directory, error))?;
    let stop =
        if stop == Some(context.project_root()) && !current.starts_with(context.project_root()) {
            context
                .namespace_anchor()
                .filter(|anchor| current.starts_with(anchor))
        } else {
            stop
        };
    loop {
        sync_directory_with_context(&current, context)?;
        if stop.is_some_and(|root| current == root) {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current_device = context
                .metadata(&current)
                .map_err(|error| directory_sync_error("inspect", &current, error))?
                .dev();
            let parent_device = context
                .metadata(parent)
                .map_err(|error| directory_sync_error("inspect", parent, error))?
                .dev();
            if current_device != parent_device {
                break;
            }
        }
        current = parent.to_path_buf();
    }
    Ok(())
}

#[cfg(any(windows, test))]
fn durable_replace_file_windows(source: &Path, destination: &Path) -> io::Result<()> {
    // Rust 1.97.1's Windows rename uses FileRenameInfoEx POSIX replacement when
    // the ordinary move cannot replace an open destination. Calling our old
    // MoveFileExW helper directly omitted that behavior. Keep the candidate
    // handle through rename so the subsequent FlushFileBuffers targets the
    // exact publication even if another reader opens the destination.
    // FlushFileBuffers requires write access; no create/truncate is permitted.
    let candidate = fs::OpenOptions::new().read(true).write(true).open(source)?;
    fs::rename(source, destination)?;
    candidate.sync_all()
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
    fn directory_sync_rejects_missing_paths_and_regular_files() {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let file = root.join("not-a-directory");
        write_synced(&file, b"preserved");
        assert!(sync_directory(&file).is_err());
        assert_eq!(fs::read(&file).unwrap(), b"preserved");
        assert_eq!(
            sync_directory(&root.join("missing")).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        sync_directory(&root).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_sync_errors_retain_operation_path_and_native_cause() {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let missing = root.join("missing");
        let error = sync_directory(&missing).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        let detail = error
            .get_ref()
            .unwrap()
            .downcast_ref::<DirectorySyncError>()
            .unwrap();
        assert_eq!(detail.path, missing);
        assert!(detail.source.raw_os_error().is_some());
        assert!(error.to_string().contains("namespace directory"));
        assert!(error.to_string().contains(&missing.display().to_string()));

        let failure = fail_sync_directory_for(&root);
        let error = sync_directory(&root).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().starts_with("flush namespace directory"));
        assert!(error.to_string().contains(&root.display().to_string()));
        assert!(error
            .to_string()
            .contains("injected directory sync failure"));
        drop(failure);
        sync_directory(&root).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn directory_sync_charges_and_releases_its_project_handle() {
        let root = unique_test_dir();
        let project = crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
            &root, 1, true,
        )
        .unwrap();
        let held = crate::file_io::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("held"))
            .unwrap();
        assert_eq!(project.metrics().open, 1);
        let before = project.metrics();
        let error = sync_directory(&root).unwrap_err();
        assert!(
            hawdb_core::error::file_descriptor_error(&error).is_some(),
            "unexpected directory sync error: {error:?}"
        );
        assert_eq!(project.metrics().open, before.open);
        assert_eq!(project.metrics().reserved, before.reserved);
        assert_eq!(project.metrics().high_water, 1);
        drop(held);
        sync_directory(&root).unwrap();
        assert_eq!(project.metrics().open, 0);
        assert_eq!(project.metrics().reserved, 0);
        assert_eq!(project.metrics().high_water, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn windows_replacement_policy_funds_held_candidate_and_rename_together() {
        use crate::file_descriptors::ProjectFileDescriptors;
        use crate::file_io::File as ManagedFile;
        use hawdb_core::error::{file_descriptor_error, FileDescriptorError};
        use std::io::Read;

        for limit in [5, 6] {
            let root = unique_test_dir();
            let project =
                ProjectFileDescriptors::acquire_with_capacity_limit(&root, limit, true).unwrap();
            let candidate = root.join("candidate.hawdb");
            let published = root.join("published.hawdb");
            write_synced(&candidate, b"new");
            write_synced(&published, b"old");
            // Represent the retained project root, child ownership pair and
            // catalog lock. No owner is released to fund replacement.
            let mut holders = (0..4)
                .map(|_| ManagedFile::open(&published).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(project.metrics().open, 4);
            let result = durable_replace_file_windows(&candidate, &published);
            if limit == 5 {
                let error = result.unwrap_err();
                assert!(matches!(
                    file_descriptor_error(&error),
                    Some(FileDescriptorError::BudgetExceeded {
                        requested: 1,
                        available: 0,
                        limit: 5,
                    })
                ));
                // The rename admission refuses before namespace mutation.
                assert_eq!(fs::read(&candidate).unwrap(), b"new");
                assert_eq!(fs::read(&published).unwrap(), b"old");
            } else {
                result.unwrap();
                assert!(!candidate.exists());
                assert_eq!(fs::read(&published).unwrap(), b"new");
                for reader in &mut holders {
                    let mut original = Vec::new();
                    reader.read_to_end(&mut original).unwrap();
                    assert_eq!(original, b"old");
                }
            }
            assert_eq!(project.metrics().open, 4);
            assert_eq!(project.metrics().reserved, 0);
            assert_eq!(project.metrics().high_water, limit);
            drop(holders);
            assert_eq!(project.metrics().open, 0);
            fs::remove_dir_all(root).unwrap();
        }
    }

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
    fn directory_flush_failure_preserves_the_installed_publication() {
        let root = unique_test_dir();
        fs::create_dir_all(&root).unwrap();
        let candidate = root.join("candidate.hawdb");
        let published = root.join("published.hawdb");
        write_synced(&published, b"old");
        write_synced(&candidate, b"new");

        let failure = fail_sync_directory_for(&root);
        let error = durable_replace_file(&candidate, &published).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        // The rename may have committed before a failed namespace barrier.
        // Retain the exact installed bytes; a missing response is not rollback.
        assert_eq!(fs::read(&published).unwrap(), b"new");
        assert!(!candidate.exists());
        drop(failure);
        sync_directory(&root).unwrap();
        assert_eq!(fs::read(&published).unwrap(), b"new");
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
