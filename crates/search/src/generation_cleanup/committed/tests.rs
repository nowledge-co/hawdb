use super::*;
use crate::build_memory::path::evidence as paths;
use crate::generation_cleanup::{CleanupCandidate, SearchProjectionCleanupState};
use skein_core::{RuntimeMemoryReservation, RuntimeTaskContext};
use std::cell::RefCell;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::{Component, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;
type Hook = Box<dyn FnOnce(&BuildMemory)>;
thread_local! {
    pub(super) static BEFORE: RefCell<Option<Hook>> = const { RefCell::new(None) };
}
pub(super) fn before_cleanup(memory: &BuildMemory) {
    if let Some(hook) = BEFORE.with_borrow_mut(Option::take) {
        hook(memory);
    }
}

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-committed-cleanup-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn memory(limit: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0)),
    )
    .unwrap()
}

fn join_peak(root: &Path, name: &str) -> usize {
    let length = root.as_os_str().as_encoded_bytes().len() + name.len() + 1;
    if matches!(root.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim())
    {
        4 * length.max(8) + 4 * length.max(4) * size_of::<Component<'_>>()
    } else {
        3 * length.max(8)
    }
}

fn generations() -> SearchProjectionGenerations {
    SearchProjectionGenerations {
        lexical: Some(3),
        out_of_core: Some(3),
        rabitq: Some(3),
        ..Default::default()
    }
}

#[test]
fn candidate_classification_borrows_or_moves_names_without_copying() {
    for name in [
        "search_lexical.01.skein",
        "search_lexical.manifest.+2.skein",
        "search_rabitq.7.skein.corrupt.42.9",
        "search_projection_segments.1.skein",
        "search_projection_out_of_core_layout.18446744073709551615.skein",
    ] {
        let mut owned = String::with_capacity(256);
        owned.push_str(name);
        let pointer = owned.as_ptr();
        let capacity = owned.capacity();
        let candidate = CleanupCandidate::parse(owned).unwrap();
        assert_eq!(candidate.name.as_ptr(), pointer);
        assert_eq!(candidate.name.capacity(), capacity);
        let borrowed = CleanupCandidate::parse(name).unwrap();
        assert_eq!(borrowed.name.as_ptr(), name.as_ptr());
        assert_eq!(candidate.kind, borrowed.kind);
        assert_eq!(candidate.generation, borrowed.generation);
        assert_eq!(candidate.quarantined, borrowed.quarantined);
    }
}

#[test]
fn cleanup_paths_share_exact_and_one_short_build_budgets_through_removal() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let name = "search_projection_segments.1.skein";
    fs::write(root.join(name), b"obsolete").unwrap();
    let canonical = fs::canonicalize(&root).unwrap();
    for root in [&root, &canonical] {
        let bound = join_peak(root, name);
        for limit in [bound + 137 - 1, bound + 137] {
            let memory = memory(limit);
            let competing = memory.input.reserve(137).unwrap();
            paths::take();
            let mut calls = 0;
            let report =
                cleanup_with_remover(root, generations(), Default::default(), &memory, |path| {
                    calls += 1;
                    assert_eq!(path, root.join(name));
                    assert_eq!(
                        memory.ledger.snapshot().used_bytes,
                        root.join(name).capacity() + 137
                    );
                    Ok(())
                });
            let admitted = usize::from(limit == bound + 137);
            assert_eq!(calls, admitted);
            assert_eq!(paths::take(), admitted);
            assert_eq!(report.deleted_files, admitted);
            assert_eq!(report.pending_after, 1 - admitted);
            assert_eq!(report.retry_required, admitted == 0);
            if admitted == 1 {
                assert_eq!(memory.ledger.snapshot().peak_bytes, bound + 137);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(competing);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn one_shot_counts_match_a_fresh_stateful_pass_for_retention_and_io_outcomes() {
    let root = root();
    fs::create_dir(&root).unwrap();
    for name in [
        "search_lexical.1.skein",
        "search_lexical.2.skein",
        "search_lexical.manifest.1.skein",
        "search_rabitq.1.skein",
        "search_rabitq.3.skein",
        "search_projection_segments.1.skein",
        "search_projection_segments.3.skein",
        "search_rabitq.7.skein.corrupt.42.9",
        "search_rabitq.7.skein.corrupt.invalid.9",
        "unrelated.txt",
        "search_lexical.18446744073709551616.skein",
    ] {
        fs::write(root.join(name), b"fixture").unwrap();
    }
    for kind in [
        None,
        Some(io::ErrorKind::NotFound),
        Some(io::ErrorKind::PermissionDenied),
    ] {
        for attempts in [1, 2, 256] {
            for capacity in [1, 3, 256] {
                for remove_all in [false, true] {
                    let options = SearchProjectionCleanupOptions {
                        max_pending_files: NonZeroUsize::new(capacity).unwrap(),
                        max_delete_attempts: NonZeroUsize::new(attempts).unwrap(),
                    };
                    let generations = SearchProjectionGenerations {
                        rabitq_remove_all: remove_all,
                        ..generations()
                    };
                    let remove = |_: &Path| kind.map_or(Ok(()), |kind| Err(io::Error::from(kind)));
                    let expected = SearchProjectionCleanupState::default().run_with_remover(
                        &root,
                        generations,
                        options,
                        remove,
                    );
                    let memory = memory(1024 * 1024);
                    let actual = cleanup_with_remover(&root, generations, options, &memory, remove);
                    assert_eq!(
                        actual,
                        CleanupSummary {
                            deleted_files: expected.deleted_files,
                            pending_after: expected.pending_after,
                            retry_required: expected.retry_required
                        }
                    );
                    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
                }
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn denied_cleanup_is_bounded_observable_and_rediscovered_on_retry() {
    let root = root();
    fs::create_dir(&root).unwrap();
    for number in 1..=8 {
        fs::write(
            root.join(format!("search_lexical.{number}.skein")),
            b"obsolete",
        )
        .unwrap();
    }
    let memory = memory(137);
    let competing = memory.input.reserve(137).unwrap();
    paths::take();
    let generations = SearchProjectionGenerations {
        lexical: Some(10),
        ..Default::default()
    };
    let options = SearchProjectionCleanupOptions {
        max_pending_files: NonZeroUsize::new(2).unwrap(),
        max_delete_attempts: NonZeroUsize::new(2).unwrap(),
    };
    let report = cleanup_with_remover(&root, generations, options, &memory, |_| {
        panic!("unadmitted deletion")
    });
    assert_eq!(
        report,
        CleanupSummary {
            deleted_files: 0,
            pending_after: 2,
            retry_required: true
        }
    );
    assert_eq!(paths::take(), 0);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 8);
    assert_eq!(memory.ledger.snapshot().used_bytes, 137);
    drop(competing);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let retried =
        SearchProjectionCleanupState::default().run(&root, generations, Default::default());
    assert_eq!(retried.deleted_files, 8);
    assert!(!retried.retry_required);
    fs::remove_dir(root).unwrap();
}

#[test]
fn committed_cleanup_keeps_working_after_late_cancellation_without_leaking_paths() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let obsolete = root.join("search_lexical.1.skein");
    fs::write(&obsolete, b"obsolete").unwrap();
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    paths::cancel_next(task.cancellation().clone());
    let report = cleanup_committed_generation(&root, generations(), Default::default(), &memory);
    assert!(task.cancellation().is_cancelled());
    assert_eq!(report.deleted_files, 1);
    assert!(!report.retry_required);
    assert!(!obsolete.exists());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn directory_and_generation_discovery_failures_remain_retryable_without_allocated_reports() {
    let root = root();
    let memory = memory(1);
    let report = cleanup_committed_generation(&root, generations(), Default::default(), &memory);
    assert_eq!(
        report,
        CleanupSummary {
            deleted_files: 0,
            pending_after: 0,
            retry_required: true
        }
    );
    fs::create_dir(&root).unwrap();
    let report = cleanup_committed_generation(
        &root,
        SearchProjectionGenerations {
            out_of_core_discovery_failed: true,
            ..Default::default()
        },
        Default::default(),
        &memory,
    );
    assert!(report.retry_required);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}
