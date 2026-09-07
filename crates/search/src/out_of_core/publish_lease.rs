use crate::build_control::checkpoint;
use crate::build_memory::path::OwnedPath;
use crate::build_memory::{checked_add, BuildMemory};
use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::collections::LinkedList;
use std::fs::{File, OpenOptions, TryLockError};
use std::mem::size_of;
use std::path::Path;
use std::sync::{LazyLock, Mutex};

const SEARCH_PROJECTION_PUBLISH_LOCK_FILE: &str = ".search_projection.publish.lock";
static ACTIVE_SEARCH_PROJECTION_PUBLISHERS: LazyLock<Mutex<Registry>> =
    LazyLock::new(|| Mutex::new(Registry::default()));

#[derive(Debug)]
pub(crate) struct SearchProjectionPublishLease {
    // Close the OS handle before removing the in-process registration.
    lock_file: File,
    _registration: RegistrationGuard,
}

impl SearchProjectionPublishLease {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        let task = RuntimeTaskContext::default();
        Self::acquire_with_context(root, &BuildMemory::new(&task)?, &task)
    }

    pub(crate) fn acquire_with_context(
        root: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let canonical_root = OwnedPath::canonicalize(root, memory, task)?;
        let lock_path = OwnedPath::join(
            &canonical_root,
            Path::new(SEARCH_PROJECTION_PUBLISH_LOCK_FILE),
            memory,
            task,
        )?;
        let registration = RegistrationGuard::acquire(canonical_root, memory, task)?;
        checkpoint(task)?;
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        if let Err(error) = lock_file.try_lock() {
            return match error {
                TryLockError::WouldBlock => Err(busy()),
                TryLockError::Error(error) => Err(error.into()),
            };
        }
        #[cfg(test)]
        tests::locked(task);
        checkpoint(task)?;
        Ok(Self {
            lock_file,
            _registration: registration,
        })
    }
}

impl Drop for SearchProjectionPublishLease {
    fn drop(&mut self) {
        let _ = File::unlock(&self.lock_file);
    }
}

// A node belongs to one publisher and is removed without retaining spare array
// capacity. This avoids charging a process-global HashSet to an arbitrary build.
// Publisher acquisition is not a query hot path; lookup is O(active publishers).
#[derive(Default)]
struct Registry {
    entries: LinkedList<Registration>,
    next_id: u64,
}

struct Registration {
    root: OwnedPath,
    id: u64,
    _node_memory: QueryMemoryLease,
}

fn registration_bytes() -> Result<usize> {
    // Rust 1.97.1 LinkedList::Node contains the value and two optional pointers.
    // All fields have at most pointer/u64 alignment on the supported targets.
    checked_add(size_of::<Registration>(), 2 * size_of::<usize>())
}

#[derive(Debug)]
struct RegistrationGuard {
    id: u64,
}

impl RegistrationGuard {
    fn acquire(root: OwnedPath, memory: &BuildMemory, task: &RuntimeTaskContext) -> Result<Self> {
        checkpoint(task)?;
        let mut registry = active_publishers();
        for entry in &registry.entries {
            checkpoint(task)?;
            if entry.root.as_ref() == root.as_ref() {
                return Err(busy());
            }
        }
        let id = registry
            .next_id
            .checked_add(1)
            .ok_or_else(|| SkeinError::Execution("search publisher identity exhausted".into()))?;
        let node_memory = memory.retained.reserve(registration_bytes()?)?;
        checkpoint(task)?;
        registry.entries.push_back(Registration {
            root,
            id,
            _node_memory: node_memory,
        });
        registry.next_id = id;
        drop(registry);
        #[cfg(test)]
        tests::registered(task);
        Ok(Self { id })
    }
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        // extract_if returns the value after freeing its list node. Keep the
        // returned value (and its charge) live until the node is deallocated.
        let removed = active_publishers()
            .entries
            .extract_if(|entry| entry.id == self.id)
            .next();
        drop(removed);
    }
}

fn busy() -> SkeinError {
    SkeinError::Storage("another search projection publication is active".into())
}

fn active_publishers() -> std::sync::MutexGuard<'static, Registry> {
    ACTIVE_SEARCH_PROJECTION_PUBLISHERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
