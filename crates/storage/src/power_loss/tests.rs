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
use crate::file_io as fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture {
    root: PathBuf,
    model: PowerLossModel,
}

impl Fixture {
    fn new(limits: ImageLimits) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-power-native-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let project = ProjectFileDescriptors::acquire(&root, 32).unwrap();
        let model = PowerLossModel::attach(&project, limits).unwrap();
        Self { root, model }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn observations_capture_actual_file_barriers_before_later_flushes() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(ImageLimits::default());
        let mut file = fs::File::create(fixture.root.join("wal")).unwrap();
        file.write_all(b"old").unwrap();
        file.sync_all().unwrap();
        crate::durability::sync_directory(&fixture.root).unwrap();
        file.write_all(b"new").unwrap();
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::FileSync,
                relative_path: PathBuf::from("wal"),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        file.sync_all().unwrap();
        let snapshot = fixture.model.take_observation().unwrap().unwrap();
        assert_eq!(snapshot.observed_path(), Some(Path::new("wal")));
        // Audit the complete native history after the observed cut. This final
        // flush must not retroactively strengthen the before-barrier image.
        fixture.model.capture().unwrap();
        let image = snapshot.crash(&CrashPlan::default()).unwrap();
        assert_eq!(
            image.bytes(Path::new("wal")).unwrap(),
            match boundary {
                ObservationBoundary::Before => b"old".as_slice(),
                ObservationBoundary::After => b"oldnew".as_slice(),
            }
        );
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn native_verification_covers_unicode_hard_links_and_cross_chunk_bytes() {
    let fixture = Fixture::new(ImageLimits::default());
    fs::create_dir_all(fixture.root.join("目录")).unwrap();
    let relative = Path::new("目录/данные");
    let bytes: Vec<u8> = (0..16_397).map(|offset| (offset % 251) as u8).collect();
    let mut file = fs::File::create(fixture.root.join(relative)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs::hard_link(fixture.root.join(relative), fixture.root.join("alias")).unwrap();
    let snapshot = fixture.model.capture().unwrap();
    let image = snapshot.crash(&snapshot.persist_all_plan()).unwrap();
    assert_eq!(image.bytes(relative).unwrap(), bytes);
    assert_eq!(image.bytes(Path::new("alias")).unwrap(), bytes);
    assert_eq!(
        image.file_inode(relative),
        image.file_inode(Path::new("alias"))
    );
    // A same-length unrecorded write beyond the first scratch chunk must still
    // refuse. Both hard-link names expose that same altered physical inode.
    let mut altered = bytes.clone();
    altered[8192] ^= 1;
    std::fs::write(fixture.root.join(relative), &altered).unwrap();
    assert!(fixture.model.capture().is_err());
    std::fs::write(fixture.root.join(relative), &bytes).unwrap();
    fixture.model.capture().unwrap();
    assert_eq!(fixture.model.project().metrics().open, 1);
    assert_eq!(fixture.model.project().metrics().reserved, 0);
}

#[test]
fn actual_io_barriers_and_aliases_drive_crash_images() {
    let fixture = Fixture::new(ImageLimits::default());
    let mut wal = fs::File::create(fixture.root.join("wal")).unwrap();
    wal.write_all(b"committed").unwrap();
    wal.sync_all().unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    fs::hard_link(fixture.root.join("wal"), fixture.root.join("alias")).unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    wal.write_all(b"relaxed").unwrap();
    let snapshot = fixture.model.capture().unwrap();
    let lost = snapshot.crash(&CrashPlan::default()).unwrap();
    assert_eq!(lost.bytes(Path::new("wal")).unwrap(), b"committed");
    assert_eq!(
        lost.file_inode(Path::new("wal")),
        lost.file_inode(Path::new("alias"))
    );
    // Later synchronization must not strengthen an already captured boundary.
    wal.sync_all().unwrap();
    assert_eq!(
        snapshot
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("alias"))
            .unwrap(),
        b"committed"
    );
    let latest = fixture.model.capture().unwrap();
    assert_eq!(
        latest
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("wal"))
            .unwrap(),
        b"committedrelaxed"
    );
    drop(wal);
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn captured_native_clones_preserve_cursor_and_append_semantics() {
    let fixture = Fixture::new(ImageLimits::default());
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(fixture.root.join("wal"))
        .unwrap();
    file.write_all(b"abcdef").unwrap();
    let mut clone = file.try_clone().unwrap();
    clone.seek(SeekFrom::Start(2)).unwrap();
    let written = file
        .write_vectored(&[io::IoSlice::new(b"12"), io::IoSlice::new(b"34")])
        .unwrap();
    // Windows may write only the first nonempty slice. Both paths must retain
    // the shared cursor and record exactly the successful native prefix.
    assert!((1..=4).contains(&written));
    file.write_all(&b"1234"[written..]).unwrap();
    let mut append = fs::OpenOptions::new()
        .append(true)
        .open(fixture.root.join("wal"))
        .unwrap();
    append.seek(SeekFrom::Start(0)).unwrap();
    append.write_all(b"!").unwrap();
    clone.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    clone.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"ab1234!");
    let snapshot = fixture.model.capture().unwrap();
    assert_eq!(
        snapshot
            .crash(&snapshot.persist_all_plan())
            .unwrap()
            .bytes(Path::new("wal"))
            .unwrap(),
        bytes
    );
}

#[test]
fn capture_verifies_empty_files_without_releasing_their_exclusive_locks() {
    let fixture = Fixture::new(ImageLimits::default());
    let path = fixture.root.join("lease");
    let lease = fs::File::create(&path).unwrap();
    lease.lock().unwrap();

    let snapshot = fixture.model.capture().unwrap();
    let image = snapshot.crash(&snapshot.persist_all_plan()).unwrap();
    assert_eq!(image.bytes(Path::new("lease")), Some(b"".as_slice()));
    let contender = fs::OpenOptions::new().write(true).open(&path).unwrap();
    assert!(matches!(
        contender.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(contender);
    drop(lease);
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn capture_rejects_uncaptured_bytes_in_an_expected_empty_locked_file() {
    let fixture = Fixture::new(ImageLimits::default());
    let path = fixture.root.join("lease");
    let lease = fs::File::create(&path).unwrap();
    std::fs::write(&path, b"uncaptured").unwrap();
    lease.lock().unwrap();

    let error = fixture.model.capture().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error
        .to_string()
        .contains("uncaptured native length at lease"));
    drop(lease);
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn capture_rejects_uninstrumented_bytes_and_directory_changes() {
    let fixture = Fixture::new(ImageLimits::default());
    fs::write(fixture.root.join("wal"), b"captured").unwrap();
    std::fs::write(fixture.root.join("wal"), b"tampered").unwrap();
    assert!(fixture.model.capture().is_err());
    std::fs::write(fixture.root.join("wal"), b"captured").unwrap();
    fixture.model.capture().unwrap();
    std::fs::create_dir(fixture.root.join("uncaptured")).unwrap();
    assert!(fixture.model.capture().is_err());
}

#[test]
fn capture_admission_rejects_before_mutating_native_bytes() {
    let fixture = Fixture::new(ImageLimits {
        max_bytes: 8,
        ..Default::default()
    });
    let mut wal = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(fixture.root.join("wal"))
        .unwrap();
    wal.write_all(b"1234").unwrap();
    wal.sync_all().unwrap();
    assert!(wal.write_all(b"more than admission allows").is_err());
    assert!(wal
        .write_vectored(&[io::IoSlice::new(b"12345678"), io::IoSlice::new(b"9")])
        .is_err());
    assert_eq!(std::fs::read(fixture.root.join("wal")).unwrap(), b"1234");
    fixture.model.capture().unwrap();
}

#[test]
fn native_rename_of_existing_hard_links_matches_the_platform_namespace() {
    let fixture = Fixture::new(ImageLimits::default());
    let object_path = fixture.root.join("object");
    let alias_path = fixture.root.join("alias");
    let file = fs::File::create(&object_path).unwrap();
    (&file).write_all(b"durable").unwrap();
    file.sync_all().unwrap();
    drop(file);
    fs::hard_link(&object_path, &alias_path).unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();

    fs::rename(&alias_path, &object_path).unwrap();
    let alias_exists = fs::try_exists(&alias_path).unwrap();
    eprintln!("native-shared-inode-rename-v1 source_present={alias_exists}");
    let before_barrier = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        before_barrier.bytes(Path::new("alias")).unwrap(),
        b"durable"
    );
    assert_eq!(
        before_barrier.bytes(Path::new("object")).unwrap(),
        b"durable"
    );

    crate::durability::sync_directory(&fixture.root).unwrap();
    let after_barrier = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        after_barrier.bytes(Path::new("object")).unwrap(),
        b"durable"
    );
    assert_eq!(
        after_barrier.bytes(Path::new("alias")).is_some(),
        alias_exists
    );
    if alias_exists {
        assert_eq!(
            after_barrier.file_inode(Path::new("alias")),
            after_barrier.file_inode(Path::new("object")),
        );
    }
}

#[test]
fn native_head_replacement_and_gc_do_not_mutate_durable_old_inodes() {
    let fixture = Fixture::new(ImageLimits::default());
    let head = fs::File::create(fixture.root.join("head")).unwrap();
    (&head).write_all(b"old").unwrap();
    head.sync_all().unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    fs::hard_link(fixture.root.join("head"), fixture.root.join("pin")).unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    let candidate = fs::File::create(fixture.root.join("candidate")).unwrap();
    (&candidate).write_all(b"new").unwrap();
    candidate.sync_all().unwrap();
    crate::durability::durable_replace_file(
        &fixture.root.join("candidate"),
        &fixture.root.join("head"),
    )
    .unwrap();
    fs::remove_file(fixture.root.join("pin")).unwrap();
    let image = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(image.bytes(Path::new("head")).unwrap(), b"new");
    assert_eq!(image.bytes(Path::new("pin")).unwrap(), b"old");
    crate::durability::sync_directory(&fixture.root).unwrap();
    assert!(fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap()
        .bytes(Path::new("pin"))
        .is_none());
}

#[test]
fn immutable_publication_persists_every_new_directory_name() {
    use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
    let fixture = Fixture::new(ImageLimits::default());
    let root = fixture.root.join("new/branch/runtime");
    let mut objects = ImmutableObjectStore::open(&root).unwrap();
    let payload = b"acknowledged immutable bytes";
    let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload);
    objects.publish(reference, payload).unwrap();
    let relative = objects
        .object_path(reference)
        .strip_prefix(&fixture.root)
        .unwrap()
        .to_path_buf();
    let image = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(image.bytes(&relative).unwrap(), payload);
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
    assert!(fixture.model.project().metrics().high_water <= 32);
}

#[test]
fn immutable_reuse_repeats_an_uncertain_final_namespace_barrier() {
    use crate::immutable_object::{
        ImmutableObjectError, ImmutableObjectStore, ObjectKind, ObjectReference, PublishOutcome,
    };
    let fixture = Fixture::new(ImageLimits::default());
    let mut objects = ImmutableObjectStore::open(&fixture.root).unwrap();
    let payload = b"complete bytes with an uncertain name";
    let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload);
    let destination = objects.object_path(reference);
    let failure = crate::durability::fail_sync_directory_for(destination.parent().unwrap());
    assert!(matches!(
        objects.publish(reference, payload),
        Err(ImmutableObjectError::PublicationUncertain { .. })
    ));
    drop(failure);
    drop(objects);
    let relative = destination.strip_prefix(&fixture.root).unwrap();
    assert!(fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap()
        .bytes(relative)
        .is_none());
    let mut retry = ImmutableObjectStore::open(&fixture.root).unwrap();
    assert_eq!(
        retry.publish(reference, payload).unwrap(),
        PublishOutcome::Reused
    );
    let image = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(image.bytes(relative).unwrap(), payload);
}

#[test]
fn failed_ancestor_barrier_is_retried_before_object_acknowledgment() {
    use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
    let fixture = Fixture::new(ImageLimits::default());
    let root = fixture.model.project().root().join("new/branch");
    let failure = crate::durability::fail_sync_directory_for(fixture.model.project().root());
    let mut retry = ImmutableObjectStore::open(&root).unwrap();
    let payload = b"retry must synchronize existing ancestor names";
    let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, payload);
    assert!(retry.publish(reference, payload).is_err());
    drop(failure);
    retry.publish(reference, payload).unwrap();
    let relative = retry
        .object_path(reference)
        .strip_prefix(fixture.model.project().root())
        .unwrap()
        .to_path_buf();
    assert_eq!(
        fixture
            .model
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(&relative)
            .unwrap(),
        payload
    );
}

#[test]
fn completed_kind_name_barrier_is_reused_without_omitting_object_barriers() {
    use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
    let fixture = Fixture::new(ImageLimits::default());
    let mut objects = ImmutableObjectStore::open(&fixture.root).unwrap();
    let first = b"first object";
    let second = b"second object";
    let first_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, first);
    let second_reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, second);
    objects.publish(first_reference, first).unwrap();
    let failure = crate::durability::fail_sync_directory_for(&fixture.root.join("objects"));
    objects.publish(second_reference, second).unwrap();
    drop(failure);
    let image = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    for (reference, payload) in [
        (first_reference, first.as_slice()),
        (second_reference, second.as_slice()),
    ] {
        let path = objects.object_path(reference);
        assert_eq!(
            image
                .bytes(path.strip_prefix(&fixture.root).unwrap())
                .unwrap(),
            payload
        );
    }
}

#[test]
fn capture_rejects_an_unrecorded_hard_link_between_equal_files() {
    let fixture = Fixture::new(ImageLimits::default());
    let first = fixture.root.join("first");
    let second = fixture.root.join("second");
    fs::write(&first, b"same bytes").unwrap();
    fs::write(&second, b"same bytes").unwrap();
    fixture.model.capture().unwrap();

    std::fs::remove_file(&second).unwrap();
    std::fs::hard_link(&first, &second).unwrap();
    let error = fixture.model.capture().unwrap_err();
    assert!(error
        .to_string()
        .contains("native hard link has separate captured inode identities"));
    std::fs::remove_file(&second).unwrap();
    std::fs::write(&second, b"same bytes").unwrap();
    fixture.model.capture().unwrap();
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn capture_rejects_an_unrecorded_copy_replacing_a_hard_link() {
    let fixture = Fixture::new(ImageLimits::default());
    let first = fixture.root.join("first");
    let second = fixture.root.join("second");
    fs::write(&first, b"same bytes").unwrap();
    fs::hard_link(&first, &second).unwrap();
    fixture.model.capture().unwrap();

    std::fs::remove_file(&second).unwrap();
    std::fs::write(&second, b"same bytes").unwrap();
    let error = fixture.model.capture().unwrap_err();
    assert!(error
        .to_string()
        .contains("captured hard-link identity differs from native identity"));
    std::fs::remove_file(&second).unwrap();
    std::fs::hard_link(&first, &second).unwrap();
    fixture.model.capture().unwrap();
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn observations_capture_native_directory_barriers_at_the_named_cut() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(ImageLimits::default());
        let directory = fixture.root.join("directory");
        fs::create_dir(&directory).unwrap();
        crate::durability::sync_directory(&fixture.root).unwrap();
        let file = fs::File::create(directory.join("wal")).unwrap();
        (&file).write_all(b"durable bytes, pending name").unwrap();
        file.sync_all().unwrap();
        fixture
            .model
            .observe(ObservationPoint {
                event: IoEvent::DirectorySync,
                relative_path: PathBuf::from("directory"),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        crate::durability::sync_directory(&directory).unwrap();
        let snapshot = fixture.model.take_observation().unwrap().unwrap();
        fixture.model.capture().unwrap();
        let image = snapshot.crash(&CrashPlan::default()).unwrap();
        assert_eq!(
            image.bytes(Path::new("directory/wal")),
            match boundary {
                ObservationBoundary::Before => None,
                ObservationBoundary::After => Some(b"durable bytes, pending name".as_slice()),
            }
        );
    }
}

#[cfg(windows)]
#[test]
fn directory_capture_flags_never_hide_an_opaque_open_option() {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_DELETE_ON_CLOSE: u32 = 0x0400_0000;
    const GENERIC_READ: u32 = 0x8000_0000;
    let fixture = Fixture::new(ImageLimits::default());
    for opaque_first in [false, true] {
        let mut options = fs::OpenOptions::new();
        options.read(true);
        if opaque_first {
            options.access_mode(GENERIC_READ);
        }
        options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
        if !opaque_first {
            options.access_mode(GENERIC_READ);
        }
        let error = options.open(&fixture.root).unwrap_err();
        assert!(error.to_string().contains("custom native open flags"));
    }
    let candidate = fixture.root.join("uncaptured");
    let error = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(FILE_FLAG_DELETE_ON_CLOSE | FILE_FLAG_BACKUP_SEMANTICS)
        .open(&candidate)
        .unwrap_err();
    assert!(error.to_string().contains("custom native open flags"));
    assert!(!candidate.exists());
    fixture.model.capture().unwrap();
    // The recorder retains its counted root after operation handles close.
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn retained_model_root_refuses_identical_foreign_tree_and_path_mutations_after_replacement() {
    let fixture = Fixture::new(ImageLimits::default());
    let mut file = fs::File::create(fixture.root.join("wal")).unwrap();
    file.write_all(b"committed").unwrap();
    file.sync_all().unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    drop(file);
    fixture.model.capture().unwrap();
    assert_eq!(fixture.model.project().metrics().open, 1);
    let moved = fixture.root.with_extension("captured");
    std::fs::rename(&fixture.root, &moved).unwrap();
    struct Restore {
        original: PathBuf,
        moved: PathBuf,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.original).unwrap();
            std::fs::rename(&self.moved, &self.original).unwrap();
        }
    }
    let restore = Restore {
        original: fixture.root.clone(),
        moved,
    };
    std::fs::create_dir(&fixture.root).unwrap();
    // Content equality cannot substitute for the captured physical root.
    std::fs::write(fixture.root.join("wal"), b"committed").unwrap();
    assert_eq!(
        fixture.model.capture().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(fs::OpenOptions::new()
        .append(true)
        .open(fixture.root.join("wal"))
        .is_err());
    assert!(fs::remove_file(fixture.root.join("wal")).is_err());
    assert!(fs::File::create(fixture.root.join("new")).is_err());
    assert_eq!(fixture.model.project().metrics().open, 1);
    assert_eq!(
        std::fs::read(fixture.root.join("wal")).unwrap(),
        b"committed"
    );
    assert_eq!(
        std::fs::read(restore.moved.join("wal")).unwrap(),
        b"committed"
    );
    assert!(!fixture.root.join("new").exists());
    drop(restore);
    fixture.model.capture().unwrap();
}

#[test]
fn model_root_capacity_outlives_model_handle_until_last_traced_file_closes() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-root-retention-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let project = ProjectFileDescriptors::acquire(&root, 8).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    assert_eq!(project.metrics().open, 1);
    let mut file = fs::File::create(root.join("wal")).unwrap();
    file.write_all(b"retained").unwrap();
    assert_eq!(project.metrics().open, 2);
    drop(model);
    assert_eq!(project.metrics().open, 2);
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(project.metrics().open, 0);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn root_attachment_admission_and_nonempty_refusal_leave_no_registered_recorder_or_handles() {
    for (limit, nonempty) in [(1, false), (8, true)] {
        let root = std::env::temp_dir().join(format!(
            "hawdb-root-refusal-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        if nonempty {
            std::fs::write(root.join("prior"), b"retained").unwrap();
        }
        assert!(PowerLossModel::attach(&project, ImageLimits::default()).is_err());
        assert_eq!(project.metrics().open, 0);
        assert_eq!(project.metrics().reserved, 0);
        // A failed attachment must not leave a recorder which rejects native
        // creation at this project's unchanged one-descriptor limit.
        let mut file = fs::File::create(root.join("after-refusal")).unwrap();
        file.write_all(b"native").unwrap();
        drop(file);
        assert_eq!(project.metrics().open, 0);
        if nonempty {
            assert_eq!(std::fs::read(root.join("prior")).unwrap(), b"retained");
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn retained_file_io_stays_in_captured_scope_after_root_path_replacement() {
    let fixture = Fixture::new(ImageLimits::default());
    let mut file = fs::File::create(fixture.root.join("wal")).unwrap();
    file.write_all(b"old").unwrap();
    file.sync_all().unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    let moved = fixture.root.with_extension("held-file");
    std::fs::rename(&fixture.root, &moved).unwrap();
    std::fs::create_dir(&fixture.root).unwrap();
    std::fs::write(fixture.root.join("wal"), b"foreign").unwrap();
    assert!(fixture.model.capture().is_err());
    // This file is already bound to the captured logical inode and native
    // handle. Its recorded I/O must not resolve the replaced root pathname.
    file.write_all(b"new").unwrap();
    file.sync_all().unwrap();
    assert_eq!(std::fs::read(moved.join("wal")).unwrap(), b"oldnew");
    assert_eq!(std::fs::read(fixture.root.join("wal")).unwrap(), b"foreign");
    assert_eq!(fixture.model.project().metrics().open, 2);
    std::fs::remove_dir_all(&fixture.root).unwrap();
    std::fs::rename(moved, &fixture.root).unwrap();
    let snapshot = fixture.model.capture().unwrap();
    assert_eq!(
        snapshot
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("wal"))
            .unwrap(),
        b"oldnew"
    );
    drop(file);
    assert_eq!(fixture.model.project().metrics().open, 1);
}

#[test]
fn relative_directory_create_is_recorded_before_a_later_open_and_parent_barrier() {
    use crate::file_io::SourceDirectory;
    let fixture = Fixture::new(ImageLimits::default());
    let project = fixture.model.project();
    fs::create_dir(fixture.root.join("source")).unwrap();
    crate::durability::sync_directory(&fixture.root).unwrap();
    let source =
        SourceDirectory::capture(&fixture.root.join("source"), &project.io_context()).unwrap();
    fixture
        .model
        .observe(ObservationPoint {
            event: IoEvent::CreateDirectory,
            relative_path: PathBuf::from("source/component"),
            boundary: ObservationBoundary::After,
            skip_matches: 0,
            include_descendants: false,
            keep_last: false,
        })
        .unwrap();
    source
        .create_directory_new(&project.io_context(), "component")
        .unwrap();
    let cut = fixture.model.take_observation().unwrap().unwrap();
    assert_eq!(cut.observed_path(), Some(Path::new("source/component")));
    fixture.model.capture().unwrap();
    let lost = cut.crash(&CrashPlan::default()).unwrap();
    assert!(!lost
        .directory_paths()
        .any(|p| p == Path::new("source/component")));
    // The new directory may survive before the parent barrier, too.
    let retained = cut.crash(&cut.persist_all_plan()).unwrap();
    assert!(retained
        .directory_paths()
        .any(|p| p == Path::new("source/component")));
    source.sync(&project.io_context()).unwrap();
    let durable = fixture
        .model
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert!(durable
        .directory_paths()
        .any(|p| p == Path::new("source/component")));
    assert_eq!(project.metrics().open, 2); // recorder anchor plus source directory
}
