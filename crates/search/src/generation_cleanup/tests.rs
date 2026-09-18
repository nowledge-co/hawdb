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

use super::*;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, io};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn lexical_manifest_retention_uses_lexical_not_out_of_core_generation() {
    for lexical in 1..=8 {
        for out_of_core in 1..=8 {
            let generations = SearchProjectionGenerations {
                lexical: Some(lexical),
                out_of_core: Some(out_of_core),
                ..SearchProjectionGenerations::default()
            };
            for generation in 1..=8 {
                let manifest =
                    CleanupCandidate::parse(format!("search_lexical.manifest.{generation}.hawdb"))
                        .unwrap();
                let artifact =
                    CleanupCandidate::parse(format!("search_lexical.{generation}.hawdb")).unwrap();
                assert_eq!(manifest.kind, CleanupArtifactKind::Lexical);
                assert_eq!(
                    manifest.is_obsolete(generations),
                    artifact.is_obsolete(generations)
                );
            }
        }
    }
    let quarantined =
        CleanupCandidate::parse("search_lexical.manifest.7.hawdb.corrupt.123.456".to_string())
            .unwrap();
    assert_eq!(quarantined.kind, CleanupArtifactKind::Lexical);
    assert!(quarantined.is_obsolete(SearchProjectionGenerations::default()));
    assert!(CleanupCandidate::parse("search_lexical.manifest.hawdb".to_string()).is_none());
}
#[test]
fn failed_deletion_is_observable_and_retryable_without_removing_previous_generation() {
    let root = test_root("retry");
    fs::create_dir_all(&root).unwrap();
    for name in [
        "search_lexical.1.hawdb",
        "search_lexical.2.hawdb",
        "search_lexical.3.hawdb",
        "search_rabitq.1.hawdb",
        "search_rabitq.2.hawdb",
        "search_rabitq.3.hawdb",
        "search_projection_segments.1.hawdb",
        "search_projection_segments.2.hawdb",
        "search_projection_segments.3.hawdb",
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
            if path.ends_with("search_lexical.1.hawdb") {
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
        "search_lexical.2.hawdb",
        "search_lexical.3.hawdb",
        "search_rabitq.2.hawdb",
        "search_rabitq.3.hawdb",
        "search_projection_segments.2.hawdb",
        "search_projection_segments.3.hawdb",
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
            root.join(format!("search_lexical.{generation}.hawdb")),
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
    let artifact = root.join("search_lexical.1.hawdb");
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

#[test]
fn quarantined_generation_is_deleted_without_removing_the_live_artifact() {
    let root = test_root("quarantine");
    fs::create_dir_all(&root).unwrap();
    let live = root.join("search_rabitq.7.hawdb");
    let quarantined = root.join("search_rabitq.7.hawdb.corrupt.42.9");
    let malformed = root.join("search_rabitq.7.hawdb.corrupt.unknown.9");
    fs::write(&live, b"live").unwrap();
    fs::write(&quarantined, b"corrupt").unwrap();
    fs::write(&malformed, b"unrecognized").unwrap();

    let report = SearchProjectionCleanupState::default().run(
        &root,
        SearchProjectionGenerations {
            rabitq: Some(7),
            ..SearchProjectionGenerations::default()
        },
        SearchProjectionCleanupOptions::default(),
    );

    assert_eq!(report.eligible_files, 1);
    assert_eq!(report.deleted_files, 1);
    assert!(live.exists());
    assert!(!quarantined.exists());
    assert!(malformed.exists());
    fs::remove_dir_all(root).unwrap();
}

fn test_root(name: &str) -> std::path::PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hawdb-search-projection-cleanup-{name}-{}-{sequence}",
        std::process::id()
    ))
}
