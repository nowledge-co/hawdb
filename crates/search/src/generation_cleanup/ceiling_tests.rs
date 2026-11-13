// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Generation cleanup when the active closure exceeds the verification
//! ceiling (#1755). The ceiling is lowered so a small corpus exercises the
//! same over-ceiling path as a generation above the 128 MiB default.

use super::*;
use crate::{SearchDocument, SearchIndex};
use std::fs;
use std::num::NonZeroU64;
use std::path::PathBuf;

const CEILING: u64 = 64 * 1024;

struct Fixture(PathBuf);
impl Fixture {
    fn new(name: &str) -> Self {
        Self(crate::test_temp_dir().join(format!(
            "hawdb-cleanup-ceiling-{name}-{}",
            hawdb_core::generate_uuidv7().unwrap()
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn options(max_verification_bytes: u64) -> SearchProjectionCleanupOptions {
    SearchProjectionCleanupOptions {
        max_verification_bytes: NonZeroU64::new(max_verification_bytes).unwrap(),
        ..SearchProjectionCleanupOptions::default()
    }
}

fn document(id: usize, revision: usize) -> SearchDocument {
    // Incompressible enough that the generation stays well above `CEILING`.
    let mut state = (id as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ revision as u64;
    let content = (0..160)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            format!("w{:x}", state & 0xffff_ffff)
        })
        .collect::<Vec<_>>()
        .join(" ");
    SearchDocument {
        id: format!("memory:{id}"),
        title: format!("document {id} revision {revision}"),
        content,
        embedding: Some((0..16).map(|axis| ((id + axis) % 7) as f32 + 0.5).collect()),
        metadata: Default::default(),
    }
}

/// Generations that still have a segment payload on disk.
fn generations(root: &std::path::Path) -> BTreeSet<u64> {
    fs::read_dir(root)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            name.strip_prefix("search_projection_segment_payloads.")?
                .strip_suffix(".hawdb")?
                .parse()
                .ok()
        })
        .collect()
}

/// Bytes of every generation-named file of `generation`.
fn generation_bytes(root: &std::path::Path, generation: u64) -> u64 {
    let suffix = format!(".{generation}.hawdb");
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(&suffix))
        .map(|entry| entry.metadata().unwrap().len())
        .sum()
}

fn forget_verifications(root: &std::path::Path) {
    for entry in fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            hawdb_storage::immutable_object::forget_published_object(&path).unwrap();
        }
    }
}

fn index(root: &std::path::Path, documents: usize) -> SearchIndex {
    let mut index = SearchIndex::open(root).unwrap();
    index.set_projection_cleanup_options(options(CEILING));
    for id in 0..documents {
        index.upsert(document(id, 0)).unwrap();
    }
    index
}

#[test]
fn checkpoints_above_the_verification_ceiling_keep_current_and_previous_generations() {
    let fixture = Fixture::new("checkpoints");
    let index = index(&fixture.0, 120);
    let mut index = index;
    for round in 0..6 {
        // Alternate a content change with an epoch-only checkpoint.
        if round % 2 == 1 {
            index.upsert(document(round, round)).unwrap();
        }
        let report = index.checkpoint_with_report().unwrap();
        let generation = report.projection_generation;
        assert!(
            generation_bytes(&fixture.0, generation) > 4 * CEILING,
            "the fixture must keep each generation above the ceiling"
        );
        let cleanup = report.projection_cleanup.expect("persistent checkpoint");
        assert!(!cleanup.closure_unverified, "round {round}: {cleanup:?}");
        assert!(!cleanup.retry_required, "round {round}: {cleanup:?}");
        assert_eq!(cleanup.pending_files, 0, "round {round}: {cleanup:?}");
        // The closure was verified, but almost none of it was re-hashed: the
        // publisher hashed every installed file before the selector named it.
        assert!(cleanup.verification_reused_bytes > CEILING, "{cleanup:?}");
        assert!(
            cleanup.verification_rehashed_bytes <= CEILING,
            "{cleanup:?}"
        );
        if round >= 2 {
            assert!(cleanup.deleted_files > 0, "round {round}: {cleanup:?}");
        }
        let retained = generations(&fixture.0);
        assert!(retained.len() <= 2, "round {round}: {retained:?}");
        assert!(
            retained.contains(&generation),
            "round {round}: {retained:?}"
        );
        assert_eq!(
            SearchProjectionCleanupOutcome::from(&index.projection_cleanup_report()),
            cleanup
        );
    }
    drop(index);
    let reopened = SearchIndex::open(&fixture.0).unwrap();
    assert_eq!(reopened.document_count(), 120);
}

#[test]
fn an_unproven_closure_deletes_nothing_and_a_completed_verification_is_reused() {
    let fixture = Fixture::new("unproven");
    let index = index(&fixture.0, 120);
    index.checkpoint().unwrap();
    let current = index
        .checkpoint_with_report()
        .unwrap()
        .projection_generation;
    // A superseded generation file left behind by an earlier process.
    let obsolete = fixture.0.join("search_projection_segment_payloads.0.hawdb");
    fs::write(&obsolete, b"superseded").unwrap();

    // A new process has hashed nothing; the closure exceeds the ceiling.
    forget_verifications(&fixture.0);
    let refused = index.retry_projection_cleanup(options(CEILING));
    assert!(refused.retry_required, "{refused:?}");
    assert_eq!(refused.delete_attempts, 0, "{refused:?}");
    assert!(refused
        .failure_kinds
        .contains_key("captured_closure_unverified"));
    assert!(SearchProjectionCleanupOutcome::from(&refused).closure_unverified);
    assert!(obsolete.exists());

    // Every completed hash is remembered: successive passes under the same
    // ceiling make progress (at least one file each) and finally reclaim.
    let mut passes = 1;
    let converged = loop {
        let report = index.retry_projection_cleanup(options(CEILING));
        passes += 1;
        if !report.retry_required {
            break report;
        }
        assert!(obsolete.exists(), "pass {passes}: {report:?}");
        assert!(passes < 32, "verification does not converge: {report:?}");
    };
    assert!(passes > 2, "the closure must need several passes");
    assert_eq!(converged.deleted_files, 1, "{converged:?}");
    assert!(!obsolete.exists());

    // Likewise after another restart, one pass with room for the closure.
    forget_verifications(&fixture.0);
    fs::write(&obsolete, b"superseded").unwrap();
    let complete = index.retry_projection_cleanup(options(64 * CEILING));
    assert!(!complete.retry_required, "{complete:?}");
    assert!(
        complete.verification_rehashed_bytes > CEILING,
        "{complete:?}"
    );
    assert!(!obsolete.exists());

    // That verification is remembered: the small ceiling now suffices.
    fs::write(&obsolete, b"superseded").unwrap();
    let resumed = index.retry_projection_cleanup(options(CEILING));
    assert!(!resumed.retry_required, "{resumed:?}");
    assert!(
        resumed.verification_rehashed_bytes <= CEILING,
        "{resumed:?}"
    );
    assert!(resumed.verification_reused_bytes > CEILING, "{resumed:?}");
    assert!(!obsolete.exists());
    assert!(generations(&fixture.0).contains(&current));
}

#[test]
fn a_closure_file_changed_after_verification_is_hashed_again_and_blocks_deletion() {
    let fixture = Fixture::new("changed");
    let index = index(&fixture.0, 120);
    index.checkpoint().unwrap();
    let current = index
        .checkpoint_with_report()
        .unwrap()
        .projection_generation;
    let obsolete = fixture.0.join("search_projection_segment_payloads.0.hawdb");
    fs::write(&obsolete, b"superseded").unwrap();
    // Leave the coarse file-time tick of the publication before rewriting.
    std::thread::sleep(std::time::Duration::from_millis(50));
    let payload = fixture.0.join(format!(
        "search_projection_segment_payloads.{current}.hawdb"
    ));
    let mut bytes = fs::read(&payload).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x5a;
    fs::write(&payload, bytes).unwrap();

    let report = index.retry_projection_cleanup(options(64 * CEILING));
    assert!(report.retry_required, "{report:?}");
    assert_eq!(report.delete_attempts, 0, "{report:?}");
    assert!(report
        .failure_kinds
        .contains_key("captured_closure_unverified"));
    assert!(obsolete.exists());
}

#[test]
fn an_unbounded_explicit_pass_stream_verifies_a_snapshot_above_the_decode_bound() {
    struct ResetDecodeLimit;
    impl Drop for ResetDecodeLimit {
        fn drop(&mut self) {
            crate::out_of_core::set_snapshot_decode_limit_for_test(None);
        }
    }
    let fixture = Fixture::new("decode-bound");
    let index = index(&fixture.0, 120);
    index.checkpoint().unwrap();
    let current = index
        .checkpoint_with_report()
        .unwrap()
        .projection_generation;
    let snapshot = fs::metadata(fixture.0.join(crate::resident_snapshot_file(current)))
        .unwrap()
        .len();
    assert!(
        snapshot > CEILING,
        "the snapshot must exceed the decode bound"
    );
    let rabitq = fixture.0.join(format!("search_rabitq.{current}.hawdb"));
    assert!(rabitq.exists());

    // A restarted process raises the re-hash ceiling to its maximum. The
    // in-memory decode stays under its own fixed bound (lowered here).
    let _reset = ResetDecodeLimit;
    crate::out_of_core::set_snapshot_decode_limit_for_test(Some(CEILING));
    let obsolete = fixture.0.join("search_projection_segment_payloads.0.hawdb");
    fs::write(&obsolete, b"superseded").unwrap();
    forget_verifications(&fixture.0);
    let report = index.retry_projection_cleanup(options(u64::MAX));
    assert!(!report.retry_required, "{report:?}");
    assert!(!report.closure_unverified, "{report:?}");
    assert_eq!(report.verification_decoded_snapshot_bytes, 0, "{report:?}");
    assert!(report.verification_rehashed_bytes > snapshot, "{report:?}");
    assert!(!obsolete.exists());
    assert!(rabitq.exists(), "standalone vector artifacts are retained");

    // Control: under the decode bound the same pass decodes the snapshot.
    crate::out_of_core::set_snapshot_decode_limit_for_test(None);
    fs::write(&obsolete, b"superseded").unwrap();
    forget_verifications(&fixture.0);
    let report = index.retry_projection_cleanup(options(u64::MAX));
    assert!(!report.retry_required, "{report:?}");
    assert_eq!(
        report.verification_decoded_snapshot_bytes, snapshot,
        "{report:?}"
    );
    assert!(!obsolete.exists());
}
