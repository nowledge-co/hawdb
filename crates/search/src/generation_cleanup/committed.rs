//! One-shot cleanup of a committed generation, without a discarded retry queue.

use super::{
    scan_candidates, ScannedEntry, SearchProjectionCleanupOptions, SearchProjectionGenerations,
};
use crate::build_memory::path::OwnedPath;
use crate::build_memory::BuildMemory;
use std::{fs, io, path::Path};

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CleanupSummary {
    pub(crate) deleted_files: usize,
    pub(crate) pending_after: usize,
    pub(crate) retry_required: bool,
}

impl CleanupSummary {
    fn defer(&mut self, capacity: usize) {
        self.pending_after = self.pending_after.saturating_add(1).min(capacity);
        self.retry_required = true;
    }
}

/// The caller keeps its publication lease until cleanup completes. Discovery
/// stays after commit: concurrent readers can quarantine corrupt artifacts.
/// Deferred files remain discoverable by the existing stateful retry API.
pub(crate) fn cleanup_committed_generation(
    root: &Path,
    generations: SearchProjectionGenerations,
    options: SearchProjectionCleanupOptions,
    memory: &BuildMemory,
) -> CleanupSummary {
    #[cfg(test)]
    tests::before_cleanup(memory);
    cleanup_with_remover(root, generations, options, memory, |path| {
        fs::remove_file(path)
    })
}

#[cfg(test)]
pub(crate) fn before_cleanup_for_test(hook: impl FnOnce(&BuildMemory) + 'static) {
    tests::BEFORE.with_borrow_mut(|slot| {
        assert!(slot.is_none());
        *slot = Some(Box::new(hook));
    });
}

fn cleanup_with_remover(
    root: &Path,
    generations: SearchProjectionGenerations,
    options: SearchProjectionCleanupOptions,
    memory: &BuildMemory,
    mut remove: impl FnMut(&Path) -> io::Result<()>,
) -> CleanupSummary {
    let mut report = CleanupSummary {
        retry_required: generations.out_of_core_discovery_failed,
        ..Default::default()
    };
    let mut attempts = 0;
    if scan_candidates(root, |entry| {
        let candidate = match entry {
            ScannedEntry::Candidate(candidate) => candidate,
            ScannedEntry::Error(_) => {
                report.retry_required = true;
                return;
            }
            ScannedEntry::Ignored => return,
        };
        if !candidate.is_obsolete(generations) {
            return;
        }
        if attempts >= options.max_delete_attempts.get() {
            report.defer(options.max_pending_files.get());
            return;
        }
        let path = match OwnedPath::join_after_commit(root, Path::new(candidate.name), memory) {
            Ok(path) => path,
            Err(_) => {
                // Publication already succeeded. A path that cannot be admitted
                // is retryable cleanup work, not a failed or rolled-back commit.
                report.defer(options.max_pending_files.get());
                return;
            }
        };
        attempts += 1;
        match remove(&path) {
            Ok(()) => report.deleted_files = report.deleted_files.saturating_add(1),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => report.defer(options.max_pending_files.get()),
        }
    })
    .is_err()
    {
        report.retry_required = true;
    }
    report
}

#[cfg(test)]
mod tests;
