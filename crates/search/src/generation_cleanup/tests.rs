use super::*;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, io};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn failed_deletion_is_observable_and_retryable_without_removing_previous_generation() {
    let root = test_root("retry");
    fs::create_dir_all(&root).unwrap();
    for name in [
        "search_lexical.1.skein",
        "search_lexical.2.skein",
        "search_lexical.3.skein",
        "search_rabitq.1.skein",
        "search_rabitq.2.skein",
        "search_rabitq.3.skein",
        "search_projection_segments.1.skein",
        "search_projection_segments.2.skein",
        "search_projection_segments.3.skein",
    ] {
        fs::write(root.join(name), b"artifact").unwrap();
    }
    let generations = SearchProjectionGenerations {
        lexical: Some(3),
        out_of_core: Some(3),
        rabitq: Some(3),
        ..SearchProjectionGenerations::default()
    };
    let mut state = SearchProjectionCleanupState::default();
    let first = state.run_with_remover(
        &root,
        generations,
        SearchProjectionCleanupOptions::default(),
        |path| {
            if path.ends_with("search_lexical.1.skein") {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "pinned"))
            } else {
                fs::remove_file(path)
            }
        },
    );

    assert_eq!(first.deleted_files, 2);
    assert_eq!(first.delete_failures, 1);
    assert_eq!(first.deferred_files, 1);
    assert_eq!(first.pending_after, 1);
    assert!(first.retry_required);
    for name in [
        "search_lexical.2.skein",
        "search_lexical.3.skein",
        "search_rabitq.2.skein",
        "search_rabitq.3.skein",
        "search_projection_segments.2.skein",
        "search_projection_segments.3.skein",
    ] {
        assert!(root.join(name).exists(), "{name} must be retained");
    }

    let retried = state.run(
        &root,
        generations,
        SearchProjectionCleanupOptions::default(),
    );

    assert_eq!(retried.pending_before, 1);
    assert_eq!(retried.deleted_files, 1);
    assert_eq!(retried.pending_after, 0);
    assert!(!retried.retry_required);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn pending_retry_queue_is_bounded_and_reports_overflow() {
    let root = test_root("bounded");
    fs::create_dir_all(&root).unwrap();
    for generation in 1..=8 {
        fs::write(
            root.join(format!("search_lexical.{generation}.skein")),
            b"artifact",
        )
        .unwrap();
    }
    let generations = SearchProjectionGenerations {
        lexical: Some(10),
        ..SearchProjectionGenerations::default()
    };
    let mut state = SearchProjectionCleanupState::default();
    let report = state.run_with_remover(
        &root,
        generations,
        SearchProjectionCleanupOptions {
            max_pending_files: NonZeroUsize::new(2).unwrap(),
            max_delete_attempts: NonZeroUsize::new(2).unwrap(),
        },
        |_| Err(io::Error::new(io::ErrorKind::PermissionDenied, "pinned")),
    );

    assert_eq!(report.delete_attempts, 2);
    assert_eq!(report.pending_after, 2);
    assert_eq!(report.queue_overflow_files, 6);
    assert_eq!(report.deferred_files, 8);
    assert!(report.retry_required);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn queued_candidate_is_revalidated_before_retry() {
    let root = test_root("revalidate");
    fs::create_dir_all(&root).unwrap();
    let artifact = root.join("search_lexical.1.skein");
    fs::write(&artifact, b"artifact").unwrap();
    let mut state = SearchProjectionCleanupState::default();
    let first = state.run_with_remover(
        &root,
        SearchProjectionGenerations {
            lexical: Some(3),
            ..SearchProjectionGenerations::default()
        },
        SearchProjectionCleanupOptions::default(),
        |_| Err(io::Error::new(io::ErrorKind::PermissionDenied, "pinned")),
    );
    assert_eq!(first.pending_after, 1);

    let second = state.run_with_remover(
        &root,
        SearchProjectionGenerations {
            lexical: Some(2),
            ..SearchProjectionGenerations::default()
        },
        SearchProjectionCleanupOptions::default(),
        |_| panic!("retained candidate must not be deleted"),
    );

    assert_eq!(second.pending_before, 1);
    assert_eq!(second.pending_after, 0);
    assert!(artifact.exists());
    fs::remove_dir_all(root).unwrap();
}

fn test_root(name: &str) -> std::path::PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "skein-search-projection-cleanup-{name}-{}-{sequence}",
        std::process::id()
    ))
}
