use crate::error::{Result, SkeinError};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

const SEARCH_PROJECTION_PUBLISH_LOCK_FILE: &str = ".search_projection.publish.lock";
static ACTIVE_SEARCH_PROJECTION_PUBLISHERS: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Debug)]
pub(crate) struct SearchProjectionPublishLease {
    canonical_root: PathBuf,
    lock_file: File,
}

impl SearchProjectionPublishLease {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        let lease = Self::acquire_for_consumer(root)?;
        super::super::consumer::require_unregistered_directory(root)?;
        Ok(lease)
    }

    pub(crate) fn acquire_for_consumer(root: &Path) -> Result<Self> {
        let canonical_root = fs::canonicalize(root).map_err(|error| {
            SkeinError::Storage(format!(
                "failed to resolve search projection directory: {error}"
            ))
        })?;
        if !active_publishers().insert(canonical_root.clone()) {
            return Err(SkeinError::Storage(
                "another search projection publication is active".to_string(),
            ));
        }
        let lock_path = canonical_root.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE);
        let lock_file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) => {
                active_publishers().remove(&canonical_root);
                return Err(error.into());
            }
        };
        if let Err(error) = lock_file.try_lock() {
            active_publishers().remove(&canonical_root);
            return match error {
                TryLockError::WouldBlock => Err(SkeinError::Storage(
                    "another search projection publication is active".to_string(),
                )),
                TryLockError::Error(error) => Err(error.into()),
            };
        }
        Ok(Self {
            canonical_root,
            lock_file,
        })
    }
}

impl Drop for SearchProjectionPublishLease {
    fn drop(&mut self) {
        let _ = File::unlock(&self.lock_file);
        active_publishers().remove(&self.canonical_root);
    }
}

fn active_publishers() -> std::sync::MutexGuard<'static, HashSet<PathBuf>> {
    ACTIVE_SEARCH_PROJECTION_PUBLISHERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn publication_lease_rejects_concurrent_owner_and_recovers_after_drop() {
        let root = test_dir();
        fs::create_dir_all(&root).unwrap();
        let first = SearchProjectionPublishLease::acquire(&root).unwrap();
        assert!(SearchProjectionPublishLease::acquire(&root)
            .unwrap_err()
            .to_string()
            .contains("another search projection publication is active"));
        drop(first);
        SearchProjectionPublishLease::acquire(&root).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    fn test_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_search_publish_lease_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
