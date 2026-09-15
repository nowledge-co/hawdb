use super::*;
use crate::generation_cleanup::SearchProjectionCleanupState;
use crate::test_allocation as allocation;
use skein_core::RuntimeMemoryReservation;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-cleanup-once-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        for generation in 1..=8 {
            for prefix in [
                "search_lexical.",
                "search_lexical.manifest.",
                "search_rabitq.",
                "search_projection_segments.",
            ] {
                fs::write(
                    path.join(format!("{prefix}{generation}.skein")),
                    b"artifact",
                )
                .unwrap();
            }
        }
        for name in [
            "search_rabitq.8.skein.corrupt.12.34",
            "unrelated",
            "search_lexical.invalid.skein",
        ] {
            fs::write(path.join(name), b"keep or quarantine").unwrap();
        }
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn context(bytes: usize) -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    drop(memory.spool.reserve(1).unwrap());
    (memory, task)
}
fn generations() -> SearchProjectionGenerations {
    SearchProjectionGenerations {
        lexical: Some(7),
        out_of_core: Some(8),
        rabitq: Some(6),
        ..Default::default()
    }
}

#[test]
fn one_shot_counts_match_the_existing_retry_state_without_retaining_candidates() {
    let fixture = Fixture::new();
    for remove_all in [false, true] {
        for limit in [1, 3, 256] {
            for result in [
                io::ErrorKind::Other,
                io::ErrorKind::NotFound,
                io::ErrorKind::PermissionDenied,
            ] {
                let generations = SearchProjectionGenerations {
                    rabitq_remove_all: remove_all,
                    ..generations()
                };
                let options = SearchProjectionCleanupOptions {
                    max_pending_files: NonZeroUsize::new(2).unwrap(),
                    max_delete_attempts: NonZeroUsize::new(limit).unwrap(),
                };
                let remove = |_: &Path| {
                    if result == io::ErrorKind::Other {
                        Ok(())
                    } else {
                        Err(io::Error::from(result))
                    }
                };
                let expected = SearchProjectionCleanupState::default().run_with_remover(
                    &fixture.0,
                    generations,
                    options,
                    remove,
                );
                let (memory, task) = context(8 * 1024 * 1024);
                let actual = PreparedCleanup::prepare(&fixture.0, &memory, &task)
                    .unwrap()
                    .run_with_remover(&fixture.0, generations, options, &task, remove);
                assert_eq!(
                    actual,
                    CleanupResult {
                        deleted_files: expected.deleted_files,
                        pending_files: expected.pending_after,
                        retry_required: expected.retry_required
                    }
                );
                assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            }
        }
    }
}

#[test]
fn cleanup_admission_accepts_exact_capacity_and_defers_one_byte_short() {
    let fixture = Fixture::new();
    let required = directory::scan_bytes(&fixture.0).unwrap();
    for short in [1, 0] {
        let (memory, task) = context(required - short);
        let prepared = PreparedCleanup::prepare(&fixture.0, &memory, &task).unwrap();
        let report = prepared.run(&fixture.0, generations(), Default::default(), &task);
        if short == 1 {
            assert_eq!(
                report,
                CleanupResult {
                    retry_required: true,
                    ..Default::default()
                }
            );
            assert!(fixture.0.join("search_lexical.1.skein").exists());
        } else {
            assert!(report.deleted_files > 0);
            assert!(!report.retry_required);
            assert!(!fixture.0.join("search_lexical.1.skein").exists());
            assert!(fixture.0.join("search_lexical.6.skein").exists());
            assert!(fixture.0.join("search_lexical.7.skein").exists());
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn prepared_cleanup_progresses_at_a_full_root_and_covers_live_allocations() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let budget = 8 * 1024 * 1024;
    let (memory, task) = context(budget);
    let prepared = PreparedCleanup::prepare(&fixture.0, &memory, &task).unwrap();
    let reserved = memory.ledger.snapshot().used_bytes;
    let held = memory.input.reserve(budget - reserved).unwrap();
    let (report, peak) =
        allocation::measure(|| prepared.run(&fixture.0, generations(), Default::default(), &task));
    assert!(report.deleted_files > 0);
    assert!(!report.retry_required);
    assert!(peak <= reserved, "requested {peak}, reserved {reserved}");
    assert_eq!(allocation::live(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, held.bytes());
    drop(held);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn cleanup_cancellation_reports_retry_and_preserves_completed_deletions() {
    let fixture = Fixture::new();
    let (memory, task) = context(8 * 1024 * 1024);
    let prepared = PreparedCleanup::prepare(&fixture.0, &memory, &task).unwrap();
    let report = prepared.run_with_remover(
        &fixture.0,
        generations(),
        Default::default(),
        &task,
        |path| {
            fs::remove_file(path)?;
            task.cancellation().cancel();
            Ok(())
        },
    );
    assert_eq!(report.deleted_files, 1);
    assert!(report.retry_required);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(PreparedCleanup::prepare(&fixture.0, &memory, &task).is_err());
}
