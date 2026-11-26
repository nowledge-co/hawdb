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

#[test]
fn competing_publication_reuses_object_without_leaking_staging() {
    let root = test_root("competing-publication");
    let (path, payload, reference) = source(&root);
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    let destination = store.prepare_kind_directory(reference).unwrap();
    let (staging, written) = store
        .stage_file(reference, &path, BlockWork::default())
        .unwrap();
    let mut competitor = ImmutableObjectStore::open(&root).unwrap();
    competitor.publish(reference, &payload).unwrap();
    assert_eq!(
        store
            .install_staged(
                reference,
                None,
                written,
                &staging,
                &destination,
                BlockWork::default()
            )
            .unwrap(),
        PublishOutcome::Reused
    );
    assert_eq!(staging_entries(&root), 0);
    assert_eq!(fs::read(destination).unwrap(), payload);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(unix)]
fn sealed_publication_shares_inode_and_managed_writes_detach() {
    let root = test_root("sealed-cow");
    let (_project, path, payload, reference) = sealed_source(&root);
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    let original = crate::native_file_identity::of_metadata(&std::fs::metadata(&path).unwrap());
    assert_eq!(
        store
            .publish_sealed_file_with_work(reference, &path, BlockWork::default())
            .unwrap(),
        PublishOutcome::Published
    );
    let destination = store.object_path(reference);
    assert_eq!(
        crate::native_file_identity::of_metadata(&std::fs::metadata(&destination).unwrap()),
        original
    );
    assert_eq!(
        crate::native_file_identity::of_metadata(&std::fs::metadata(&path).unwrap()),
        original
    );
    let held = File::open(&path).unwrap();
    // Reusing an already bound alias must not open it for writing and detach.
    store
        .publish_sealed_file_with_work(reference, &path, BlockWork::default())
        .unwrap();
    assert_eq!(
        crate::native_file_identity::of_metadata(&std::fs::metadata(&path).unwrap()),
        original
    );
    fs::write(&path, b"changed independently").unwrap();
    assert_ne!(
        crate::native_file_identity::of_metadata(&std::fs::metadata(&path).unwrap()),
        original
    );
    assert_eq!(fs::read(&destination).unwrap(), payload);
    let mut snapshot = Vec::new();
    (&held).read_to_end(&mut snapshot).unwrap();
    assert_eq!(snapshot, payload);
    assert_eq!(fs::read(&path).unwrap(), b"changed independently");
    assert_eq!(staging_entries(&root), 0);
    drop(held);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(unix)]
fn direct_sealed_publication_needs_no_staging_entry_but_keeps_final_barrier() {
    for staging in [true, false] {
        let root = test_root("direct-sealed-barrier");
        let (_project, path, payload, reference) = sealed_source(&root);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let destination = store.prepare_kind_directory(reference).unwrap();
        let blocked = if staging {
            root.join(OBJECTS_DIRECTORY).join(STAGING_DIRECTORY)
        } else {
            destination.parent().unwrap().to_path_buf()
        };
        let failure = crate::durability::fail_sync_directory_for(&blocked);
        let result = store.publish_sealed_file_with_work(reference, &path, BlockWork::default());
        drop(failure);
        if staging {
            assert_eq!(result.unwrap(), PublishOutcome::Published);
        } else {
            assert!(matches!(
                result,
                Err(ImmutableObjectError::PublicationUncertain { .. })
            ));
            assert!(matches!(
                store.publish_sealed_file_with_work(reference, &path, BlockWork::default()),
                Err(ImmutableObjectError::PublisherPoisoned)
            ));
            let mut recovered = ImmutableObjectStore::open(&root).unwrap();
            assert_eq!(
                recovered
                    .publish_sealed_file_with_work(reference, &path, BlockWork::default())
                    .unwrap(),
                PublishOutcome::Reused
            );
        }
        assert_eq!(fs::read(&destination).unwrap(), payload);
        assert_eq!(fs::read(&path).unwrap(), payload);
        assert_eq!(staging_entries(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[cfg(unix)]
fn direct_sealed_publication_refuses_replaced_source_and_retains_foreign_link() {
    let root = test_root("direct-sealed-replacement");
    let (_project, path, payload, reference) = sealed_source(&root);
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    let destination = store.prepare_kind_directory(reference).unwrap();
    let original_identity =
        crate::native_file_identity::of_metadata(&std::fs::metadata(&path).unwrap());
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let retained = path.with_extension("retained");
    let source = path.clone();
    let saved = retained.clone();
    let replacement = payload.clone();
    sealed::BEFORE_DIRECT_LINK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            std::fs::rename(&source, &saved).unwrap();
            std::fs::write(&source, replacement).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&source)
                .unwrap()
                .set_times(std::fs::FileTimes::new().set_modified(modified))
                .unwrap();
        }));
    });
    assert!(matches!(
        store.publish_sealed_file_with_work(reference, &path, BlockWork::default()),
        Err(ImmutableObjectError::PublicationUncertain { .. })
    ));
    assert_ne!(
        crate::native_file_identity::of_metadata(&std::fs::metadata(&destination).unwrap()),
        original_identity
    );
    assert_eq!(fs::read(&destination).unwrap(), payload);
    assert_eq!(fs::read(&path).unwrap(), payload);
    assert_eq!(fs::read(&retained).unwrap(), payload);
    assert_eq!(staging_entries(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(unix)]
fn direct_sealed_publication_power_loss_keeps_complete_payload_at_name_barrier() {
    use crate::file_descriptors::ProjectFileDescriptors;
    use crate::power_loss::{
        image::{CrashPlan, ImageLimits},
        IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel,
    };
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let root = test_root("direct-sealed-power-loss");
        let project = ProjectFileDescriptors::acquire_with_capacity_limit(&root, 32, true).unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        let path = root.join("producer");
        let payload = b"complete sealed checkpoint payload";
        let mut file = File::create(&path).unwrap();
        file.write_all(payload).unwrap();
        file.sync_all().unwrap();
        drop(file);
        crate::durability::sync_directory(&root).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, payload);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        let destination = store.prepare_kind_directory(reference).unwrap();
        model
            .observe(ObservationPoint {
                event: IoEvent::DirectorySync,
                relative_path: PathBuf::from("objects/checkpoint-artifact"),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        store
            .publish_sealed_file_with_work(reference, &path, BlockWork::default())
            .unwrap();
        let cut = model.take_observation().unwrap().unwrap();
        let relative = destination.strip_prefix(&root).unwrap();
        let lost = cut.crash(&CrashPlan::default()).unwrap();
        if boundary == ObservationBoundary::Before {
            assert!(lost.bytes(relative).is_none());
        } else {
            assert_eq!(lost.bytes(relative).unwrap(), payload);
        }
        assert_eq!(
            cut.crash(&cut.persist_all_plan())
                .unwrap()
                .bytes(relative)
                .unwrap(),
            payload
        );
        assert_eq!(
            model
                .capture()
                .unwrap()
                .crash(&CrashPlan::default())
                .unwrap()
                .bytes(relative)
                .unwrap(),
            payload
        );
        drop(model);
        drop(project);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn public_file_publication_keeps_caller_writes_independent() {
    let root = test_root("public-copy");
    let (path, payload, reference) = source(&root);
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    store.publish_file(reference, &path).unwrap();
    // An external caller can retain native write access. Public publication
    // must not rely on HawDB's managed COW or internal producer custody.
    std::fs::write(&path, b"external write").unwrap();
    assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn sealed_publication_link_fallback_preserves_copy_and_cow() {
    for alias in [false, true] {
        for kind in [
            io::ErrorKind::CrossesDevices,
            io::ErrorKind::Unsupported,
            io::ErrorKind::Other,
        ] {
            let root = test_root("sealed-link-fallback");
            let (_project, path, payload, reference) = sealed_source(&root);
            let mut store = ImmutableObjectStore::open(&root).unwrap();
            if alias {
                store.publish_file(reference, &path).unwrap();
                crate::immutable_files::NEXT_ALIAS_LINK_ERROR.with(|slot| slot.set(Some(kind)));
            } else {
                sealed::NEXT_LINK_ERROR.with(|slot| slot.set(Some(kind)));
            }
            let result =
                store.publish_sealed_file_with_work(reference, &path, BlockWork::default());
            if kind == io::ErrorKind::Other {
                assert!(result.is_err());
            } else {
                result.unwrap();
                fs::write(&path, b"private after fallback").unwrap();
                assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
            }
            assert_eq!(staging_entries(&root), 0);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn cold_sealed_reuse_admits_mount_per_block_and_cancels_during_mount() {
    for cancel in [None, Some(BLOCKS_PER_PASS + 2)] {
        let root = test_root("sealed-cold-reuse");
        let (_project, path, payload, reference) = sealed_source(&root);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        store.publish_file(reference, &path).unwrap();
        forget_published_object(&store.object_path(reference)).unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        if let Some(limit) = cancel {
            probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
        }
        let work = probe.context(local.clone());
        let result = store.publish_sealed_file_with_work(reference, &path, BlockWork(Some(&work)));
        if cancel.is_some() {
            assert!(result.unwrap_err().to_string().contains("stopped"));
            store
                .publish_sealed_file_with_work(reference, &path, BlockWork::default())
                .unwrap();
        } else {
            result.unwrap();
            assert!(
                probe.completed.load(Ordering::SeqCst)
                    >= BLOCKS_PER_PASS + PAYLOAD_BYTES.div_ceil(STREAM_BUFFER_BYTES)
            );
            assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        }
        probe.assert_released(&local);
        assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
        assert_eq!(staging_entries(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn sealed_publication_cancellation_leaves_only_complete_objects_and_retries() {
    for io in [false, true] {
        for limit in 1..=BLOCKS_PER_PASS {
            let root = test_root("sealed-cancel");
            let (_project, path, payload, reference) = sealed_source(&root);
            let local = scheduler();
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(limit, Ordering::SeqCst);
            }
            let work = probe.context(local.clone());
            let mut store = ImmutableObjectStore::open(&root).unwrap();
            let error = store
                .publish_sealed_file_with_work(reference, &path, BlockWork(Some(&work)))
                .unwrap_err();
            assert!(
                error.to_string().contains("stopped"),
                "io={io} limit={limit}: {error}"
            );
            probe.assert_released(&local);
            assert_eq!(staging_entries(&root), 0);
            if store.object_path(reference).exists() {
                store.verify_stored(reference).unwrap();
            }
            store
                .publish_sealed_file_with_work(reference, &path, BlockWork::default())
                .unwrap();
            assert_eq!(fs::read(store.object_path(reference)).unwrap(), payload);
            assert_eq!(fs::read(&path).unwrap(), payload);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
#[cfg(unix)]
fn sealed_publication_rejects_symlinks_and_changed_content() {
    let root = test_root("sealed-invalid");
    let (_project, path, _, reference) = sealed_source(&root);
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    let alias = root.join("symlink");
    std::os::unix::fs::symlink(&path, &alias).unwrap();
    assert!(matches!(
        store.publish_sealed_file_with_work(reference, &alias, BlockWork::default()),
        Err(ImmutableObjectError::ReferenceMismatch)
    ));
    fs::write(&path, b"wrong").unwrap();
    assert!(matches!(
        store.publish_sealed_file_with_work(reference, &path, BlockWork::default()),
        Err(ImmutableObjectError::ReferenceMismatch)
    ));
    assert!(!store.object_path(reference).exists());
    assert_eq!(staging_entries(&root), 0);
    fs::remove_dir_all(root).unwrap();
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

fn sealed_source(
    root: &Path,
) -> (
    crate::file_descriptors::ProjectFileDescriptors,
    PathBuf,
    Vec<u8>,
    ObjectReference,
) {
    // The production GraphStore retains its project domain across publication
    // and every later managed write. The standalone domain retains only weak
    // COW handles and is not a valid sealed-producer owner.
    let project = crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
        root, 16, true,
    )
    .unwrap();
    let (path, payload, reference) = source(root);
    (project, path, payload, reference)
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
