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

use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard};

pub const DATABASE_DIRECTORY_LOCK_FILE: &str = "owner.hawdb.lock";

static ACTIVE_DATABASE_DIRECTORIES: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

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
/// The process-local registry rejects duplicate handles before they reach the
/// operating system. The file lock rejects other cooperating applications. The
/// sidecar remains in place after close so every process locks the same inode.
#[derive(Debug)]
pub struct DatabaseDirectoryLease {
    canonical_path: PathBuf,
    lock_file: File,
}

impl DatabaseDirectoryLease {
    pub fn acquire(path: &Path) -> Result<Self, DatabaseDirectoryLeaseError> {
        let canonical_path =
            fs::canonicalize(path).map_err(DatabaseDirectoryLeaseError::Canonicalize)?;
        if !register_process_lease(&canonical_path) {
            return Err(DatabaseDirectoryLeaseError::AlreadyOpen);
        }

        let lock_file = match open_lock_file(&canonical_path) {
            Ok(file) => file,
            Err(error) => {
                unregister_process_lease(&canonical_path);
                return Err(DatabaseDirectoryLeaseError::OpenLockFile(error));
            }
        };
        if let Err(error) = lock_file.try_lock() {
            unregister_process_lease(&canonical_path);
            return match error {
                TryLockError::WouldBlock => Err(DatabaseDirectoryLeaseError::AlreadyOpen),
                TryLockError::Error(error) => Err(DatabaseDirectoryLeaseError::Lock(error)),
            };
        }

        Ok(Self {
            canonical_path,
            lock_file,
        })
    }
}

impl Drop for DatabaseDirectoryLease {
    fn drop(&mut self) {
        let _ = File::unlock(&self.lock_file);
        unregister_process_lease(&self.canonical_path);
    }
}

fn open_lock_file(canonical_path: &Path) -> io::Result<File> {
    let lock_path = canonical_path.join(DATABASE_DIRECTORY_LOCK_FILE);
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::PermissionDenied {
                OpenOptions::new().read(true).open(lock_path)
            } else {
                Err(error)
            }
        })
}

fn register_process_lease(path: &Path) -> bool {
    active_database_directories().insert(path.to_path_buf())
}

fn unregister_process_lease(path: &Path) {
    active_database_directories().remove(path);
}

fn active_database_directories() -> MutexGuard<'static, HashSet<PathBuf>> {
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
