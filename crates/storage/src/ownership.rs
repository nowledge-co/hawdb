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

use crate::file_io::{
    self as fs, File, SourceDirectory, SourceDirectoryIdentity, SourceDirectoryLockMode,
    TryLockError,
};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::path::Path;
use std::sync::{LazyLock, Mutex, MutexGuard};

pub const DATABASE_DIRECTORY_LOCK_FILE: &str = "owner.hawdb.lock";

static ACTIVE_DATABASE_DIRECTORIES: LazyLock<Mutex<BTreeSet<SourceDirectoryIdentity>>> =
    LazyLock::new(|| Mutex::new(BTreeSet::new()));

#[derive(Debug)]
pub enum DatabaseDirectoryLeaseError {
    AlreadyOpen,
    Canonicalize(io::Error),
    OpenLockFile(io::Error),
    Lock(io::Error),
}

impl Display for DatabaseDirectoryLeaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyOpen => formatter
                .write_str("database directory is already open by this or another application"),
            Self::Canonicalize(error) => {
                write!(formatter, "failed to resolve database directory: {error}")
            }
            Self::OpenLockFile(error) => {
                write!(formatter, "failed to open database ownership lock: {error}")
            }
            Self::Lock(error) => {
                write!(
                    formatter,
                    "failed to acquire database ownership lock: {error}"
                )
            }
        }
    }
}

impl Error for DatabaseDirectoryLeaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AlreadyOpen => None,
            Self::Canonicalize(error) | Self::OpenLockFile(error) | Self::Lock(error) => {
                Some(error)
            }
        }
    }
}

/// Holds exclusive ownership of one durable database directory.
///
/// The process-local registry is keyed by a live captured directory identity
/// and rejects aliases before opening another process-lock descriptor. The file lock rejects other cooperating applications. The
/// sidecar remains in place after close so every process locks the same inode.
#[derive(Debug)]
pub struct DatabaseDirectoryLease {
    lock_file: Option<File>,
    // Registration drops after the lock closes and before its identity pin.
    _registration: ProcessRegistration,
    directory: SourceDirectory,
}

impl DatabaseDirectoryLease {
    pub(crate) fn owns_directory(&self, path: &Path) -> io::Result<bool> {
        self.captured_directory().matches_namespace(path)
    }

    /// Borrow the original directory while this actual native owner is live.
    /// The returned handle is not a persisted identity or a publication receipt.
    pub(crate) fn captured_directory(&self) -> &SourceDirectory {
        &self.directory
    }

    pub fn acquire(path: &Path) -> Result<Self, DatabaseDirectoryLeaseError> {
        let canonical_path =
            fs::canonicalize(path).map_err(DatabaseDirectoryLeaseError::Canonicalize)?;
        let directory = SourceDirectory::capture_for_path(&canonical_path)
            .map_err(DatabaseDirectoryLeaseError::OpenLockFile)?;
        Self::from_captured(directory)
    }

    /// Acquire the native lock through an already admitted physical directory.
    pub(crate) fn from_captured(
        directory: SourceDirectory,
    ) -> Result<Self, DatabaseDirectoryLeaseError> {
        #[cfg(test)]
        if let Some(hook) = AFTER_CAPTURE.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
        let registration = ProcessRegistration::acquire(directory.identity())?;
        let lock_file =
            open_lock_file(&directory).map_err(DatabaseDirectoryLeaseError::OpenLockFile)?;
        if let Err(error) = lock_file.try_lock() {
            drop(lock_file);
            return match error {
                TryLockError::WouldBlock => Err(DatabaseDirectoryLeaseError::AlreadyOpen),
                TryLockError::Error(error) => Err(DatabaseDirectoryLeaseError::Lock(error)),
            };
        }

        Ok(Self {
            lock_file: Some(lock_file),
            _registration: registration,
            directory,
        })
    }
}

impl Drop for DatabaseDirectoryLease {
    fn drop(&mut self) {
        if let Some(file) = self.lock_file.take() {
            let _ = File::unlock(&file);
            // Closing a POSIX process-lock descriptor must precede admitting
            // the next owner, whose lock that close could otherwise release.
            drop(file);
        }
        #[cfg(test)]
        if let Some(hook) = AFTER_LOCK_CLOSE.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }
}

#[cfg(test)]
type OwnershipHook = std::cell::RefCell<Option<Box<dyn FnOnce()>>>;
#[cfg(test)]
thread_local! {
    static AFTER_CAPTURE: OwnershipHook = const { std::cell::RefCell::new(None) };
    static AFTER_LOCK_CLOSE: OwnershipHook = const { std::cell::RefCell::new(None) };
}

#[derive(Debug)]
struct ProcessRegistration {
    identity: SourceDirectoryIdentity,
}
impl ProcessRegistration {
    fn acquire(identity: SourceDirectoryIdentity) -> Result<Self, DatabaseDirectoryLeaseError> {
        if !register_process_lease(identity) {
            return Err(DatabaseDirectoryLeaseError::AlreadyOpen);
        }
        Ok(Self { identity })
    }
}
impl Drop for ProcessRegistration {
    fn drop(&mut self) {
        unregister_process_lease(self.identity);
    }
}

fn open_lock_file(directory: &SourceDirectory) -> io::Result<File> {
    let context = directory.operation_context();
    directory
        .open_database_ownership_lock(&context, SourceDirectoryLockMode::ReadWriteCreate)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::PermissionDenied {
                directory
                    .open_database_ownership_lock(&context, SourceDirectoryLockMode::ReadExisting)
            } else {
                Err(error)
            }
        })
}

fn register_process_lease(identity: SourceDirectoryIdentity) -> bool {
    active_database_directories().insert(identity)
}

fn unregister_process_lease(identity: SourceDirectoryIdentity) {
    active_database_directories().remove(&identity);
}

#[cfg(test)]
pub(crate) fn has_process_lease(path: &Path) -> bool {
    SourceDirectory::capture_for_path(path)
        .is_ok_and(|directory| active_database_directories().contains(&directory.identity()))
}

fn active_database_directories() -> MutexGuard<'static, BTreeSet<SourceDirectoryIdentity>> {
    ACTIVE_DATABASE_DIRECTORIES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{DatabaseDirectoryLease, DatabaseDirectoryLeaseError};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const CHILD_DATABASE_PATH: &str = "HAWDB_TEST_CHILD_DATABASE_PATH";
    const CHILD_READY_PATH: &str = "HAWDB_TEST_CHILD_READY_PATH";
    const CHILD_RELEASE_PATH: &str = "HAWDB_TEST_CHILD_RELEASE_PATH";

    #[test]
    fn lock_creation_refusal_releases_capture_and_process_registration() {
        use crate::file_descriptors::ProjectFileDescriptors;
        use crate::file_io::SourceDirectory;
        let base = unique_test_dir("capture-admission");
        let project = ProjectFileDescriptors::acquire(&base, 2).unwrap();
        let path = base.join("database");
        fs::create_dir(&path).unwrap();
        let blocker = SourceDirectory::capture(&base, &project.io_context()).unwrap();
        let error = DatabaseDirectoryLease::acquire(&path).unwrap_err();
        assert!(
            matches!(error, DatabaseDirectoryLeaseError::OpenLockFile(ref error)
            if hawdb_core::error::file_descriptor_error(error).is_some())
        );
        assert_eq!(project.metrics().open, 1);
        assert!(!path.join(super::DATABASE_DIRECTORY_LOCK_FILE).exists());
        drop(blocker);
        let owner = DatabaseDirectoryLease::acquire(&path).unwrap();
        assert_eq!(project.metrics().open, 2);
        drop(owner);
        assert_eq!(project.metrics().open, 0);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn captured_root_opens_owner_lock_off_a_replacement_label() {
        use crate::file_descriptors::ProjectFileDescriptors;
        let base = unique_test_dir("original-root-lock");
        let project = ProjectFileDescriptors::acquire(&base, 8).unwrap();
        let path = base.join("database");
        let moved = base.join("original");
        fs::create_dir(&path).unwrap();
        let source = path.clone();
        let original = moved.clone();
        super::AFTER_CAPTURE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                // No child is open yet, so Windows reaches the lock operation.
                fs::rename(&source, &original).unwrap();
                fs::create_dir(&source).unwrap();
            }));
        });
        let owner = DatabaseDirectoryLease::acquire(&path).unwrap();
        assert!(owner.owns_directory(&moved).unwrap());
        assert!(!owner.owns_directory(&path).unwrap());
        assert!(moved.join(super::DATABASE_DIRECTORY_LOCK_FILE).is_file());
        assert!(!path.join(super::DATABASE_DIRECTORY_LOCK_FILE).exists());
        assert!(matches!(
            DatabaseDirectoryLease::acquire(&moved),
            Err(DatabaseDirectoryLeaseError::AlreadyOpen)
        ));
        let replacement = DatabaseDirectoryLease::acquire(&path).unwrap();
        assert!(replacement.owns_directory(&path).unwrap());
        drop(replacement);
        drop(owner);
        assert_eq!(project.metrics().open, 0);
        let reopened = DatabaseDirectoryLease::acquire(&moved).unwrap();
        drop(reopened);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn native_close_precedes_process_slot_release_even_on_drop_unwind() {
        use crate::file_descriptors::ProjectFileDescriptors;
        let path = unique_test_dir("close-order");
        let project = ProjectFileDescriptors::acquire(&path, 4).unwrap();
        let owner = DatabaseDirectoryLease::acquire(&path).unwrap();
        assert_eq!(project.metrics().open, 2);
        let label = path.clone();
        let observed = project.clone();
        super::AFTER_LOCK_CLOSE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                assert_eq!(
                    observed.metrics().open,
                    1,
                    "lock already closed; directory remains pinned"
                );
                assert!(matches!(
                    DatabaseDirectoryLease::acquire(&label),
                    Err(DatabaseDirectoryLeaseError::AlreadyOpen)
                ));
                panic!("injected after native close before registry release");
            }));
        });
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner))).unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"injected after native close before registry release")
        );
        assert_eq!(project.metrics().open, 0);
        let retry = DatabaseDirectoryLease::acquire(&path).unwrap();
        drop(retry);
        assert_eq!(project.metrics().open, 0);
        fs::remove_dir_all(path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn renamed_live_owner_rejects_alias_before_opening_another_lock_file() {
        let base = unique_test_dir("renamed-live-owner");
        fs::create_dir_all(&base).unwrap();
        let path = base.join("database");
        let moved = base.join("original");
        fs::create_dir(&path).unwrap();
        let owner = DatabaseDirectoryLease::acquire(&path).unwrap();
        fs::rename(&path, &moved).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(owner.owns_directory(&moved).unwrap());
        assert!(!owner.owns_directory(&path).unwrap());
        assert!(matches!(
            DatabaseDirectoryLease::acquire(&moved),
            Err(DatabaseDirectoryLeaseError::AlreadyOpen)
        ));
        let replacement = DatabaseDirectoryLease::acquire(&path).unwrap();
        drop(replacement);
        drop(owner);
        let reopened = DatabaseDirectoryLease::acquire(&moved).unwrap();
        drop(reopened);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn rejects_duplicate_process_local_lease_until_drop() {
        let path = unique_test_dir("process_local");
        fs::create_dir_all(&path).unwrap();

        let first = DatabaseDirectoryLease::acquire(&path).unwrap();
        let error = DatabaseDirectoryLease::acquire(&path).unwrap_err();
        assert!(matches!(error, DatabaseDirectoryLeaseError::AlreadyOpen));

        drop(first);
        DatabaseDirectoryLease::acquire(&path).unwrap();
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_process_local_path_aliases() {
        let path = unique_test_dir("path_alias");
        fs::create_dir_all(&path).unwrap();

        let _first = DatabaseDirectoryLease::acquire(&path).unwrap();
        let error = DatabaseDirectoryLease::acquire(&path.join(".")).unwrap_err();
        assert!(matches!(error, DatabaseDirectoryLeaseError::AlreadyOpen));

        drop(_first);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_lease_held_by_another_process() {
        let path = unique_test_dir("cross_process");
        let ready_path = path.join("child.ready");
        let release_path = path.join("child.release");
        fs::create_dir_all(&path).unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("ownership::tests::child_holds_database_directory_lease")
            .arg("--nocapture")
            .env(CHILD_DATABASE_PATH, &path)
            .env(CHILD_READY_PATH, &ready_path)
            .env(CHILD_RELEASE_PATH, &release_path)
            .spawn()
            .unwrap();

        wait_for_path_or_child_exit(&ready_path, &mut child);
        let error = DatabaseDirectoryLease::acquire(&path).unwrap_err();
        assert!(matches!(error, DatabaseDirectoryLeaseError::AlreadyOpen));

        fs::write(&release_path, b"release").unwrap();
        assert!(child.wait().unwrap().success());
        DatabaseDirectoryLease::acquire(&path).unwrap();
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn child_holds_database_directory_lease() {
        let Some(database_path) = std::env::var_os(CHILD_DATABASE_PATH) else {
            return;
        };
        let ready_path = required_child_path(CHILD_READY_PATH);
        let release_path = required_child_path(CHILD_RELEASE_PATH);
        let _lease = DatabaseDirectoryLease::acquire(Path::new(&database_path)).unwrap();
        fs::write(ready_path, b"ready").unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        while !release_path.exists() {
            assert!(
                Instant::now() < deadline,
                "parent did not release child lease holder"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_path_or_child_exit(path: &Path, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("lease holder exited before readiness: {status}");
            }
            assert!(Instant::now() < deadline, "lease holder was not ready");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn required_child_path(name: &str) -> PathBuf {
        std::env::var_os(name)
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("missing child path environment variable {name}"))
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb-storage-ownership-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
