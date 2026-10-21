// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::file_descriptors::{BudgetState, ProjectFileDescriptors};
use crate::immutable_object::ObjectKind;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Weak;

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
    memory: Arc<AtomicUsize>,
    bytes: Vec<u8>,
    reference: ObjectReference,
}
impl Fixture {
    fn new(limit: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-captured-immutable-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        let bytes: Vec<_> = (0..100_003).map(|i| (i % 251) as u8).collect();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
        Self {
            root,
            project,
            memory: Arc::default(),
            bytes,
            reference,
        }
    }
    fn capture(&self, path: &Path) -> Arc<SourceDirectory> {
        Arc::new(SourceDirectory::capture(path, &self.project.io_context()).unwrap())
    }
    fn credit(&self, bytes: usize, check: DropCheck) -> Credit {
        self.memory.fetch_add(bytes, Ordering::SeqCst);
        Credit {
            bytes,
            memory: self.memory.clone(),
            state: Arc::downgrade(&self.project.state),
            check,
        }
    }
    fn open(&self, directory: Arc<SourceDirectory>, name: &str) -> File {
        self.open_checked(directory, name, DropCheck::None, DropCheck::None)
    }
    fn open_checked(
        &self,
        directory: Arc<SourceDirectory>,
        name: &str,
        key: DropCheck,
        custody: DropCheck,
    ) -> File {
        File::open_captured_immutable(
            directory,
            &self.project.io_context(),
            name,
            self.reference,
            self.credit(
                File::captured_immutable_key_bytes::<Credit>(name).unwrap(),
                key,
            ),
            self.credit(File::captured_immutable_binding_bytes::<Credit>(), custody),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        assert_eq!(self.memory.load(Ordering::SeqCst), 0);
        assert_eq!(self.project.metrics().open, 0);
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
#[derive(Debug, Clone, Copy)]
enum DropCheck {
    None,
    CacheZero,
    AllZero,
}
#[derive(Debug)]
struct Credit {
    bytes: usize,
    memory: Arc<AtomicUsize>,
    state: Weak<BudgetState>,
    check: DropCheck,
}
impl RuntimeMemoryPermit for Credit {
    fn bytes(&self) -> u64 {
        self.bytes as u64
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let metrics = state.metrics();
            match self.check {
                DropCheck::None => {}
                DropCheck::CacheZero => assert_eq!(metrics.cached_handles, 0),
                DropCheck::AllZero => assert_eq!(metrics.open, 0),
            }
        }
        self.memory.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct Control {
    validation: u64,
    deny: bool,
}
impl ReadAdmissionControl for Control {
    fn checkpoint(&mut self) -> io::Result<()> {
        Ok(())
    }
    fn admit_validation(&mut self, bytes: u64) -> io::Result<()> {
        if self.deny {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
        }
        self.validation += bytes;
        Ok(())
    }
}
fn read(file: &File, control: &mut Control) -> io::Result<(Vec<u8>, u64)> {
    file.with_admitted_read(control, |reader| {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(bytes)
    })
}

#[test]
fn cold_and_evicted_reads_use_the_captured_directory_after_replacement() {
    let fixture = Fixture::new(3);
    let named = fixture.root.join("source");
    let moved = fixture.root.join("moved");
    std::fs::create_dir(&named).unwrap();
    std::fs::write(named.join("payload.hawdb"), &fixture.bytes).unwrap();
    let directory = fixture.capture(&named);
    let file = fixture.open(directory.clone(), "payload.hawdb");
    assert_eq!(
        fixture.project.metrics().open,
        1,
        "construction must stay lazy"
    );
    std::fs::rename(&named, &moved).unwrap();
    std::fs::create_dir(&named).unwrap();
    std::fs::write(named.join("payload.hawdb"), vec![7; fixture.bytes.len()]).unwrap();
    let mut control = Control::default();
    assert_eq!(
        read(&file, &mut control).unwrap(),
        (fixture.bytes.clone(), fixture.reference.byte_length)
    );
    assert_eq!(read(&file, &mut control).unwrap().1, 0);
    assert_eq!(control.validation, fixture.reference.byte_length);
    let pressure = fixture.project.reserve(2).unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(pressure);
    assert_eq!(read(&file, &mut control).unwrap().0, fixture.bytes);
    assert_eq!(control.validation, 2 * fixture.reference.byte_length);
    drop(file);
    assert!(
        fixture.memory.load(Ordering::SeqCst) > 0,
        "idle key stays admitted"
    );
    drop(directory);
}

#[test]
fn a_warm_other_capture_cannot_hide_wrong_source_bytes() {
    let fixture = Fixture::new(8);
    let first = fixture.root.join("first");
    let second = fixture.root.join("second");
    for path in [&first, &second] {
        std::fs::create_dir(path).unwrap();
    }
    std::fs::write(first.join("payload.hawdb"), &fixture.bytes).unwrap();
    std::fs::write(second.join("payload.hawdb"), vec![7; fixture.bytes.len()]).unwrap();
    let file = fixture.open(fixture.capture(&first), "payload.hawdb");
    read(&file, &mut Control::default()).unwrap();
    let foreign = fixture.open(fixture.capture(&second), "payload.hawdb");
    assert_eq!(
        read(&foreign, &mut Control::default()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    drop(foreign);
    drop(file);
}

#[test]
fn pinned_clones_hold_native_leases_and_close_before_custody_refunds() {
    let fixture = Fixture::new(3);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let file = fixture.open_checked(
        fixture.capture(&fixture.root),
        "payload.hawdb",
        DropCheck::CacheZero,
        DropCheck::AllZero,
    );
    let admitted = fixture.memory.load(Ordering::SeqCst);
    let clone = file.try_clone().unwrap();
    assert_eq!(fixture.memory.load(Ordering::SeqCst), admitted);
    let pinned = clone.pin_admitted(&mut Control::default()).unwrap();
    drop(file);
    assert!(
        fixture.project.reserve(2).is_err(),
        "an active native lease cannot be evicted"
    );
    assert_eq!(fixture.project.metrics().open, 2);
    drop(pinned);
    assert_eq!(fixture.memory.load(Ordering::SeqCst), 0);
}

#[test]
fn new_io_borrows_the_current_inventory_after_constructor_scope_closes() {
    let fixture = Fixture::new(3);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let directory = fixture.capture(&fixture.root);
    let original = fixture.project.reserve(2).unwrap();
    let file = fixture.open(directory, "payload.hawdb");
    drop(original);
    assert_eq!(fixture.project.metrics().reserved, 0);
    let current = fixture.project.reserve(2).unwrap();
    assert_eq!(
        read(&file, &mut Control::default()).unwrap().0,
        fixture.bytes
    );
    assert_eq!(fixture.project.metrics().reserved, 1);
    drop(current);
    drop(file);
}

#[test]
fn denied_validation_and_same_length_corruption_never_populate_the_cache() {
    let fixture = Fixture::new(3);
    let path = fixture.root.join("payload.hawdb");
    std::fs::write(&path, &fixture.bytes).unwrap();
    let file = fixture.open(fixture.capture(&fixture.root), "payload.hawdb");
    let mut denied = Control {
        deny: true,
        ..Default::default()
    };
    assert_eq!(
        read(&file, &mut denied).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert_eq!(
        read(&file, &mut Control::default()).unwrap().0,
        fixture.bytes
    );
    drop(fixture.project.reserve(2).unwrap());
    let mut changed = fixture.bytes.clone();
    changed[12] ^= 1;
    std::fs::write(path, changed).unwrap();
    assert_eq!(
        read(&file, &mut Control::default()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(file);
}

#[test]
fn concurrent_cold_readers_coalesce_validation_and_keep_independent_cursors() {
    let fixture = Fixture::new(3);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let file = fixture.open(fixture.capture(&fixture.root), "payload.hawdb");
    let barrier = std::sync::Barrier::new(2);
    let validation = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    let mut control = Control::default();
                    let actual = read(&file, &mut control).unwrap();
                    assert_eq!(actual.0, fixture.bytes);
                    control.validation
                })
            })
            .collect();
        readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum::<u64>()
    });
    assert_eq!(validation, fixture.reference.byte_length);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    drop(file);
}

#[test]
fn foreign_context_invalid_name_and_metadata_denial_have_no_native_effect() {
    let fixture = Fixture::new(3);
    let other = Fixture::new(3);
    let source = fixture.capture(&fixture.root);
    for (name, foreign, bytes) in [
        ("../payload.hawdb", false, 8192),
        ("payload.hawdb", true, 8192),
        ("payload.hawdb", false, 0),
    ] {
        let context = if foreign {
            other.project.io_context()
        } else {
            fixture.project.io_context()
        };
        assert!(File::open_captured_immutable(
            source.clone(),
            &context,
            name,
            fixture.reference,
            fixture.credit(bytes, DropCheck::None),
            fixture.credit(
                File::captured_immutable_binding_bytes::<Credit>(),
                DropCheck::None
            ),
        )
        .is_err());
        assert_eq!(fixture.memory.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.project.metrics().open, 1);
        assert_eq!(other.project.metrics().open, 0);
    }
    drop(source);
}

#[test]
fn independently_opened_bindings_share_only_the_same_source_cache_key() {
    let fixture = Fixture::new(3);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let directory = fixture.capture(&fixture.root);
    let first = fixture.open(directory.clone(), "payload.hawdb");
    let second = fixture.open(directory.clone(), "payload.hawdb");
    assert_eq!(
        read(&first, &mut Control::default()).unwrap().1,
        fixture.reference.byte_length
    );
    assert_eq!(read(&second, &mut Control::default()).unwrap().1, 0);
    drop(first);
    assert_eq!(
        read(&second, &mut Control::default()).unwrap().0,
        fixture.bytes
    );
    drop(second);
    drop(directory);
}

impl Fixture {
    fn open_native(
        &self,
        directory: Arc<SourceDirectory>,
        control: &mut impl ReadAdmissionControl,
    ) -> io::Result<File> {
        let name = "payload.hawdb";
        File::open_captured_native_immutable(
            directory,
            &self.project.io_context(),
            name,
            self.credit(
                File::captured_immutable_key_bytes::<Credit>(name)?,
                DropCheck::None,
            ),
            self.credit(
                File::captured_immutable_binding_bytes::<Credit>(),
                DropCheck::None,
            ),
            control,
        )
    }
}

#[test]
fn native_capture_reopens_the_original_directory_after_eviction_without_digest_io() {
    let fixture = Fixture::new(3);
    let named = fixture.root.join("source");
    let moved = fixture.root.join("moved");
    std::fs::create_dir(&named).unwrap();
    std::fs::write(named.join("payload.hawdb"), &fixture.bytes).unwrap();
    let source = fixture.capture(&named);
    let mut control = Control::default();
    let file = fixture.open_native(source.clone(), &mut control).unwrap();
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(control.validation, 0);
    std::fs::rename(&named, &moved).unwrap();
    std::fs::create_dir(&named).unwrap();
    std::fs::write(named.join("payload.hawdb"), vec![7; fixture.bytes.len()]).unwrap();
    for _ in 0..2 {
        assert_eq!(
            read(&file, &mut control).unwrap(),
            (fixture.bytes.clone(), 0)
        );
        let pressure = fixture.project.reserve(2).unwrap();
        assert_eq!(fixture.project.metrics().cached_handles, 0);
        drop(pressure);
    }
    assert_eq!(control.validation, 0);
    assert_eq!(read(&file, &mut control).unwrap().0, fixture.bytes);
    fixture
        .project
        .state
        .existing_immutable_handles()
        .unwrap()
        .retire_unreachable(fixture.reference)
        .unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    assert_eq!(read(&file, &mut control).unwrap().0, fixture.bytes);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    drop(file);
    drop(source);
}

#[test]
fn native_reopen_rejects_equal_length_replacement_and_length_drift() {
    let fixture = Fixture::new(3);
    let path = fixture.root.join("payload.hawdb");
    std::fs::write(&path, &fixture.bytes).unwrap();
    let source = fixture.capture(&fixture.root);
    let file = fixture
        .open_native(source.clone(), &mut Control::default())
        .unwrap();
    // Retain the old inode to make the replacement ID deterministic.
    let saved = fixture.root.join("saved");
    std::fs::rename(&path, &saved).unwrap();
    std::fs::write(&path, &fixture.bytes).unwrap();
    assert_eq!(
        read(&file, &mut Control::default()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(&saved, &path).unwrap();
    std::fs::write(&path, b"truncated").unwrap();
    assert_eq!(
        read(&file, &mut Control::default()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(file);
    drop(source);
}

#[test]
fn native_capture_cancellation_and_descriptor_denial_leave_no_binding() {
    struct Cancel;
    impl ReadAdmissionControl for Cancel {
        fn checkpoint(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"))
        }
        fn admit_validation(&mut self, _: u64) -> io::Result<()> {
            panic!("native capture must not read payload")
        }
    }
    let fixture = Fixture::new(2);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let source = fixture.capture(&fixture.root);
    assert_eq!(
        fixture
            .open_native(source.clone(), &mut Cancel)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(fixture.memory.load(Ordering::SeqCst), 0);
    let pressure = fixture.project.reserve(1).unwrap();
    let busy = OpenOptions::new()
        .read(true)
        .open_with_context(
            &fixture.root.join("payload.hawdb"),
            &fixture.project.io_context(),
        )
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    assert!(fixture
        .open_native(source.clone(), &mut Control::default())
        .is_err());
    assert_eq!(fixture.memory.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(busy);
    drop(pressure);
    let file = fixture
        .open_native(source.clone(), &mut Control::default())
        .unwrap();
    assert_eq!(
        read(&file, &mut Control::default()).unwrap().0,
        fixture.bytes
    );
    drop(file);
    drop(source);
}

#[test]
fn native_capture_cancellation_after_open_closes_before_refunding_metadata() {
    struct CancelAfterOpen(usize);
    impl ReadAdmissionControl for CancelAfterOpen {
        fn checkpoint(&mut self) -> io::Result<()> {
            self.0 += 1;
            if self.0 == 2 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cancel after open",
                ));
            }
            Ok(())
        }
        fn admit_validation(&mut self, _: u64) -> io::Result<()> {
            panic!("metadata capture must not read payload")
        }
    }
    let fixture = Fixture::new(2);
    std::fs::write(fixture.root.join("payload.hawdb"), &fixture.bytes).unwrap();
    let source = fixture.capture(&fixture.root);
    let mut control = CancelAfterOpen(0);
    assert_eq!(
        fixture
            .open_native(source.clone(), &mut control)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    assert_eq!(control.0, 2);
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert_eq!(fixture.memory.load(Ordering::SeqCst), 0);
    drop(source);
}
