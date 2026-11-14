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

//! Checkpoint object publication and closure digests take one checkpoint
//! work unit and one I/O wave per streamed block (#1527), so a waiting
//! writer sees progress and the phase is cancellable at every block.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Spans several stream buffers and ends mid-buffer.
const PAYLOAD_BYTES: usize = 3 * STREAM_BUFFER_BYTES + 17;
/// Four data blocks plus the end-of-file read, per streamed pass.
const BLOCKS_PER_PASS: usize = 5;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn test_root(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hawdb-immutable-work-{label}-{nanos}"))
}

fn staging_entries(root: &Path) -> usize {
    fs::read_dir(root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY))
        .map(|entries| entries.count())
        .unwrap_or(0)
}

fn source(root: &Path) -> (PathBuf, Vec<u8>, ObjectReference) {
    fs::create_dir_all(root).unwrap();
    let path = root.join("source.bin");
    let payload = (0..PAYLOAD_BYTES)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&path, &payload).unwrap();
    let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &payload);
    (path, payload, reference)
}

#[test]
fn publication_takes_one_unit_and_wave_per_block_and_reports_progress() {
    let root = test_root("units");
    let (path, payload, reference) = source(&root);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let progress = Arc::new(AtomicU64::new(0));
    let work = probe
        .context(local.clone())
        .with_progress_counter(progress.clone());
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    assert_eq!(
        store
            .publish_file_with_work(reference, &path, BlockWork(Some(&work)))
            .unwrap(),
        PublishOutcome::Published
    );
    // Only the hashed staging copy: the synced staging inode it wrote is
    // installed without a read-back (see `published`).
    assert_eq!(probe.completed.load(Ordering::SeqCst), BLOCKS_PER_PASS);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), BLOCKS_PER_PASS);
    assert_eq!(progress.load(Ordering::SeqCst), BLOCKS_PER_PASS as u64);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&local);
    assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);

    // Reusing the existing object validates it block by block too.
    assert_eq!(
        store
            .publish_file_with_work(reference, &path, BlockWork(Some(&work)))
            .unwrap(),
        PublishOutcome::Reused
    );
    assert_eq!(progress.load(Ordering::SeqCst), 2 * BLOCKS_PER_PASS as u64);
    probe.assert_released(&local);
    assert_eq!(staging_entries(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_cancelled_at_every_unit_and_wave_leaves_no_object_and_retries() {
    let root = test_root("cancel");
    let (path, payload, reference) = source(&root);
    let local = scheduler();
    for io in [false, true] {
        for limit in 1..=BLOCKS_PER_PASS {
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(limit, Ordering::SeqCst);
            }
            let work = probe.context(local.clone());
            let mut store = ImmutableObjectStore::open(&root).unwrap();
            let result = store.publish_file_with_work(reference, &path, BlockWork(Some(&work)));
            // Cancellation after the last unit lets the publication finish.
            if !io && limit == BLOCKS_PER_PASS {
                assert_eq!(result.unwrap(), PublishOutcome::Published);
                fs::remove_file(store.object_path(reference)).unwrap();
            } else {
                let error = result.unwrap_err();
                assert!(
                    error.to_string().contains("stopped"),
                    "io={io} limit={limit}: {error}"
                );
                assert!(!store.object_path(reference).exists());
            }
            probe.assert_released(&local);
            assert_eq!(staging_entries(&root), 0, "io={io} limit={limit}");
            assert_eq!(fs::read(&path).unwrap(), payload);
        }
    }
    let retry = Arc::new(CheckpointWorkProbe::default());
    let work = retry.context(local.clone());
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    assert_eq!(
        store
            .publish_file_with_work(reference, &path, BlockWork(Some(&work)))
            .unwrap(),
        PublishOutcome::Published
    );
    assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn closure_digest_takes_one_unit_per_block_and_cancels_at_each() {
    let root = test_root("digest");
    let (path, _, reference) = source(&root);
    let ordinary = crate::checkpoint_closure::digest_closure_artifact(
        &path,
        ObjectKind::CheckpointArtifact,
        "digest",
        BlockWork::default(),
    )
    .unwrap();
    assert_eq!(ordinary.reference, reference);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work: CheckpointWorkContext = probe.context(local.clone());
    let digest = crate::checkpoint_closure::digest_closure_artifact(
        &path,
        ObjectKind::CheckpointArtifact,
        "digest",
        BlockWork(Some(&work)),
    )
    .unwrap();
    assert_eq!(digest, ordinary);
    assert_eq!(probe.completed.load(Ordering::SeqCst), BLOCKS_PER_PASS);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), BLOCKS_PER_PASS);
    probe.assert_released(&local);
    for limit in 1..BLOCKS_PER_PASS {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let work = probe.context(local.clone());
        let error = crate::checkpoint_closure::digest_closure_artifact(
            &path,
            ObjectKind::CheckpointArtifact,
            "digest",
            BlockWork(Some(&work)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{limit}: {error}");
        probe.assert_released(&local);
    }
    fs::remove_dir_all(root).unwrap();
}
