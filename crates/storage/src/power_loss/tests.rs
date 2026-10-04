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
    assert_eq!(fixture.model.project().metrics().open, 0);
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
    assert_eq!(
        file.write_vectored(&[io::IoSlice::new(b"12"), io::IoSlice::new(b"34")])
            .unwrap(),
        4
    );
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
fn capture_rejects_uninstrumented_bytes_and_directory_changes() {
    let fixture = Fixture::new(ImageLimits::default());
    fs::write(fixture.root.join("wal"), b"captured").unwrap();
    std::fs::write(fixture.root.join("wal"), b"not captured").unwrap();
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
    assert_eq!(fixture.model.project().metrics().open, 0);
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
