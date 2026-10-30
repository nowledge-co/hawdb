// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::file_io as fs;
use crate::power_loss::{
    image::{CrashPlan, ImageLimits, PersistOperation},
    IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel,
};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

struct Fixture {
    base: PathBuf,
    root: PathBuf,
    project: ProjectFileDescriptors,
    model: Option<PowerLossModel>,
}
impl Fixture {
    fn new(limit: usize, limits: ImageLimits) -> Self {
        let base = std::env::temp_dir().join(format!(
            "hawdb-native-directory-trace-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let root = base.join("project");
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&root, limit, true).unwrap();
        let model = Some(PowerLossModel::attach(&project, limits).unwrap());
        Self {
            base,
            root,
            project,
            model,
        }
    }
    fn model(&self) -> &PowerLossModel {
        self.model.as_ref().unwrap()
    }
    fn source(&self, relative: &str) -> SourceDirectory {
        SourceDirectory::capture(&self.root.join(relative), &self.project.io_context()).unwrap()
    }
    fn directory(&self, relative: &str) {
        let path = self.root.join(relative);
        fs::create_dir_all(&path).unwrap();
        crate::durability::sync_directory_ancestors(&path).unwrap();
    }
    fn write(&self, relative: &str, bytes: &[u8]) {
        let path = self.root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        crate::durability::sync_directory_ancestors(path.parent().unwrap()).unwrap();
    }
    fn observe(&self, event: IoEvent, path: &str, boundary: ObservationBoundary) {
        self.model()
            .observe(ObservationPoint {
                event,
                relative_path: PathBuf::from(path),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

#[test]
fn captured_link_records_the_held_directories_and_requires_a_real_target_barrier() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(32, ImageLimits::default());
        fixture.directory("source");
        fixture.directory("target");
        fixture.write("source/graph", b"original graph");
        let source = fixture.source("source");
        let target = fixture.source("target");
        let context = fixture.project.io_context();
        #[cfg(not(windows))]
        let file = source.open_child(&context, "graph").unwrap();
        fs::rename(
            fixture.root.join("source"),
            fixture.root.join("moved-source"),
        )
        .unwrap();
        fs::rename(
            fixture.root.join("target"),
            fixture.root.join("moved-target"),
        )
        .unwrap();
        fixture.write("source/graph", b"foreign source");
        fixture.write("target/link", b"foreign target");
        // Windows moves the captured directory before opening its descendant;
        // the retained directory must still resolve the original file.
        #[cfg(windows)]
        let file = source.open_child(&context, "graph").unwrap();
        assert!(!file
            .matches_native_path(fixture.root.join("source/graph"))
            .unwrap());
        fixture.observe(IoEvent::HardLink, "target/link", boundary);
        source
            .link_child_new(&context, "graph", &file, &target, "link")
            .unwrap();
        let cut = fixture.model().take_observation().unwrap().unwrap();
        let destination = Path::new("moved-target/link");
        let lost = cut.crash(&CrashPlan::default()).unwrap();
        assert!(lost.bytes(destination).is_none());
        assert_eq!(
            lost.bytes(Path::new("moved-source/graph")).unwrap(),
            b"original graph"
        );
        assert_eq!(
            lost.bytes(Path::new("target/link")).unwrap(),
            b"foreign target"
        );
        let persisted = cut.crash(&cut.persist_all_plan()).unwrap();
        if boundary == ObservationBoundary::After {
            assert_eq!(persisted.bytes(destination).unwrap(), b"original graph");
            assert_eq!(
                persisted.file_inode(destination),
                persisted.file_inode(Path::new("moved-source/graph"))
            );
        } else {
            assert!(persisted.bytes(destination).is_none());
        }
        target.sync(&context).unwrap();
        source.remove_child(&context, "graph").unwrap();
        source.sync(&context).unwrap();
        let recovered = fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap();
        assert_eq!(recovered.bytes(destination).unwrap(), b"original graph");
        assert!(recovered.bytes(Path::new("moved-source/graph")).is_none());
        assert_eq!(
            recovered.bytes(Path::new("source/graph")).unwrap(),
            b"foreign source"
        );
    }
}

#[test]
fn captured_link_model_refusal_keeps_native_namespace_and_existing_inode() {
    let fixture = Fixture::new(32, ImageLimits::default());
    fixture.directory("source");
    fixture.directory("target");
    fixture.write("source/graph", b"graph");
    fixture.write("target/link", b"existing target");
    let mut source = fixture.source("source");
    let target = fixture.source("target");
    let context = fixture.project.io_context();
    let file = source.open_child(&context, "graph").unwrap();
    assert_eq!(
        source
            .link_child_new(&context, "graph", &file, &target, "link")
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    let trace = source.trace.take();
    assert_eq!(
        source
            .link_child_new(&context, "graph", &file, &target, "new-link")
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert!(!fixture.root.join("target/new-link").exists());
    source.trace = trace;
    let actual = fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        actual.bytes(Path::new("target/link")).unwrap(),
        b"existing target"
    );
    assert_eq!(actual.bytes(Path::new("source/graph")).unwrap(), b"graph");
    assert_ne!(
        actual.file_inode(Path::new("target/link")),
        actual.file_inode(Path::new("source/graph"))
    );
}

#[test]
fn unbound_directory_refuses_every_recorded_operation_before_native_io() {
    let fixture = Fixture::new(32, ImageLimits::default());
    fixture.write("payload.hawdb", b"unchanged");
    fixture.directory("child");
    let context = fixture.project.io_context();
    let mut source = fixture.source("");
    assert!(source.trace.take().is_some());
    let before = fixture.project.metrics().open;
    assert_eq!(
        source
            .open_child(&context, "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        source.open_directory(&context, "child").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        source
            .open_lock_file(
                &context,
                ".control.lock",
                SourceDirectoryLockMode::ReadWriteCreate
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        source
            .remove_child(&context, "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        source
            .visit_children(&context, 8, |_| panic!("unbound callback"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        source.sync(&context).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert!(!fixture.root.join(".control.lock").exists());
    assert_eq!(
        std::fs::read(fixture.root.join("payload.hawdb")).unwrap(),
        b"unchanged"
    );
    assert_eq!(fixture.project.metrics().open, before);
    fixture.model().capture().unwrap();
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn retained_directory_trace_follows_two_modeled_renames_after_parent_close() {
    let fixture = Fixture::new(32, ImageLimits::default());
    fixture.write("source/nested/payload.hawdb", b"captured");
    let context = fixture.project.io_context();
    let parent = fixture.source("source");
    // Move before capturing descendants: Windows can refuse moving an ancestor
    // that already has open child handles. All final custody assertions remain.
    fs::rename(fixture.root.join("source"), fixture.root.join("moved")).unwrap();
    fixture.write("source/nested/payload.hawdb", b"foreign!");
    let child = parent.open_directory(&context, "nested").unwrap();
    fs::rename(
        fixture.root.join("moved/nested"),
        fixture.root.join("moved/retained"),
    )
    .unwrap();
    fixture.write("moved/nested/payload.hawdb", b"foreign child");
    drop(parent);
    let mut names = Vec::new();
    child
        .visit_children(&context, 1, |name| {
            let mut file = child.open_child(&context, name)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            assert_eq!(bytes, b"captured");
            names.push(name.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(names, ["payload.hawdb"]);
    child.remove_child(&context, "payload.hawdb").unwrap();
    child.sync(&context).unwrap();
    let snapshot = fixture.model().capture().unwrap();
    let image = snapshot.crash(&CrashPlan::default()).unwrap();
    assert!(image
        .bytes(Path::new("moved/retained/payload.hawdb"))
        .is_none());
    assert_eq!(
        image
            .bytes(Path::new("source/nested/payload.hawdb"))
            .unwrap(),
        b"foreign!"
    );
    assert_eq!(
        image
            .bytes(Path::new("moved/nested/payload.hawdb"))
            .unwrap(),
        b"foreign child"
    );
    assert_eq!(
        std::fs::read(fixture.root.join("source/nested/payload.hawdb")).unwrap(),
        b"foreign!"
    );
    assert_eq!(
        std::fs::read(fixture.root.join("moved/nested/payload.hawdb")).unwrap(),
        b"foreign child"
    );
    drop(child);
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn relative_lock_files_keep_write_clone_and_torn_write_recording() {
    let fixture = Fixture::new(32, ImageLimits::default());
    fixture.directory("source");
    let source = fixture.source("source");
    let context = fixture.project.io_context();
    let mut file = source
        .open_lock_file(
            &context,
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    file.write_all(b"old").unwrap();
    file.sync_all().unwrap();
    source.sync(&context).unwrap();
    let mut clone = file.try_clone().unwrap();
    clone.seek(SeekFrom::Start(0)).unwrap();
    clone.write_all(b"new").unwrap();
    let snapshot = fixture.model().capture().unwrap();
    let relative = Path::new("source/.trace.lock");
    assert_eq!(
        snapshot
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(relative)
            .unwrap(),
        b"old"
    );
    assert_eq!(
        snapshot
            .crash(&snapshot.persist_all_plan())
            .unwrap()
            .bytes(relative)
            .unwrap(),
        b"new"
    );
    let writes = snapshot.uncovered_writes(relative).unwrap();
    assert_eq!(writes.len(), 1);
    let torn = CrashPlan {
        persistence: vec![PersistOperation::TornWrite {
            operation: writes[0].operation,
            bytes: 0..1,
        }],
    };
    assert_eq!(
        snapshot.crash(&torn).unwrap().bytes(relative).unwrap(),
        b"nld"
    );
    clone.sync_all().unwrap();
    assert_eq!(
        fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(relative)
            .unwrap(),
        b"new"
    );
    drop((clone, file));
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn retained_relative_operations_stay_in_original_project_after_root_path_replacement() {
    let fixture = Fixture::new(32, ImageLimits::default());
    fixture.write("payload.hawdb", b"captured");
    let source = fixture.source("");
    let context = fixture.project.io_context();
    let moved = fixture.base.join("moved-project");
    // No descendant handle is open during this host rename, including on
    // Windows. Both retained directory handles identify the root itself.
    std::fs::rename(&fixture.root, &moved).unwrap();
    std::fs::create_dir(&fixture.root).unwrap();
    std::fs::write(fixture.root.join("payload.hawdb"), b"foreign!").unwrap();
    assert!(fixture.model().capture().is_err());
    let mut original = source.open_child(&context, "payload.hawdb").unwrap();
    let mut bytes = Vec::new();
    original.read_to_end(&mut bytes).unwrap();
    drop(original);
    assert_eq!(bytes, b"captured");
    let mut file = source
        .open_lock_file(
            &context,
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    file.write_all(b"retained write").unwrap();
    file.sync_all().unwrap();
    drop(file);
    source.remove_child(&context, "payload.hawdb").unwrap();
    source.sync(&context).unwrap();
    assert_eq!(
        std::fs::read(fixture.root.join("payload.hawdb")).unwrap(),
        b"foreign!"
    );
    assert!(!fixture.root.join(".trace.lock").exists());
    assert_eq!(
        std::fs::read(moved.join(".trace.lock")).unwrap(),
        b"retained write"
    );
    assert!(!moved.join("payload.hawdb").exists());
    std::fs::remove_file(fixture.root.join("payload.hawdb")).unwrap();
    std::fs::remove_dir(&fixture.root).unwrap();
    std::fs::rename(&moved, &fixture.root).unwrap();
    let image = fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        image.bytes(Path::new(".trace.lock")).unwrap(),
        b"retained write"
    );
    assert!(image.bytes(Path::new("payload.hawdb")).is_none());
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn relative_create_observations_retain_the_actual_before_and_after_inode_sets() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(16, ImageLimits::default());
        let source = fixture.source("");
        let context = fixture.project.io_context();
        fixture.observe(IoEvent::CreateFile, ".trace.lock", boundary);
        let mut file = source
            .open_lock_file(
                &context,
                ".trace.lock",
                SourceDirectoryLockMode::ReadWriteCreate,
            )
            .unwrap();
        let cut = fixture.model().take_observation().unwrap().unwrap();
        file.write_all(b"later bytes").unwrap();
        file.sync_all().unwrap();
        drop(file);
        source.sync(&context).unwrap();
        let image = cut.crash(&cut.persist_all_plan()).unwrap();
        assert_eq!(
            image.bytes(Path::new(".trace.lock")).is_some(),
            boundary == ObservationBoundary::After
        );
        if boundary == ObservationBoundary::After {
            assert_eq!(image.bytes(Path::new(".trace.lock")).unwrap(), b"");
        }
        assert!(cut
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new(".trace.lock"))
            .is_none());
        fixture.model().capture().unwrap();
        drop(source);
        assert_eq!(fixture.project.metrics().open, 1);
    }
}

#[test]
fn recorded_directory_candidate_keeps_creation_evidence_and_empty_removal_cuts() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(16, ImageLimits::default());
        let parent = fixture.source("");
        let context = fixture.project.io_context();
        let mut candidate = parent.prepare_directory_candidate("private").unwrap();
        fixture.observe(IoEvent::CreateDirectory, "private", boundary);
        parent
            .create_directory_candidate(&context, &mut candidate)
            .unwrap();
        assert!(candidate.creation_completed());
        let cut = fixture.model().take_observation().unwrap().unwrap();
        let image = cut.crash(&cut.persist_all_plan()).unwrap();
        assert_eq!(
            image
                .directory_paths()
                .any(|path| path == Path::new("private")),
            boundary == ObservationBoundary::After
        );

        let child = parent.open_created_directory(&context, &candidate).unwrap();
        let mut file = child.create_child_new(&context, "payload").unwrap();
        file.write_all(b"temporary bytes").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(parent
            .remove_created_directory(&context, &mut candidate)
            .is_err());
        assert!(candidate.creation_completed());
        child.remove_child(&context, "payload").unwrap();
        drop(child);
        fixture.observe(IoEvent::Remove, "private", boundary);
        parent
            .remove_created_directory(&context, &mut candidate)
            .unwrap();
        assert!(candidate.removal_completed());
        let cut = fixture.model().take_observation().unwrap().unwrap();
        let image = cut.crash(&cut.persist_all_plan()).unwrap();
        assert_eq!(
            image
                .directory_paths()
                .any(|path| path == Path::new("private")),
            boundary == ObservationBoundary::Before
        );
        parent.sync(&context).unwrap();
        let snapshot = fixture.model().capture().unwrap();
        assert!(!snapshot
            .crash(&CrashPlan::default())
            .unwrap()
            .directory_paths()
            .any(|path| path == Path::new("private")));
        drop(parent);
        assert_eq!(fixture.project.metrics().open, 1);
    }
}

#[test]
fn recorded_creation_refusal_before_native_effect_leaves_candidate_unowned() {
    let fixture = Fixture::new(
        16,
        ImageLimits {
            max_inodes: 1,
            ..Default::default()
        },
    );
    let parent = fixture.source("");
    let context = fixture.project.io_context();
    let mut candidate = parent.prepare_directory_candidate("private").unwrap();
    assert!(parent
        .create_directory_candidate(&context, &mut candidate)
        .is_err());
    assert!(!candidate.may_own_entry());
    assert!(!fixture.root.join("private").exists());
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn failed_native_lock_create_and_unlink_never_publish_candidate_history() {
    let fixture = Fixture::new(32, ImageLimits::default());
    let context = fixture.project.io_context();
    let source = fixture.source("");
    let path = fixture.root.join(".trace.lock");
    std::fs::write(&path, b"foreign").unwrap();
    fixture.observe(
        IoEvent::CreateFile,
        ".trace.lock",
        ObservationBoundary::After,
    );
    assert_eq!(
        source
            .open_lock_file(
                &context,
                ".trace.lock",
                SourceDirectoryLockMode::ReadWriteCreate
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(fixture.model().take_observation().unwrap().is_none());
    assert_eq!(std::fs::read(&path).unwrap(), b"foreign");
    std::fs::remove_file(&path).unwrap();
    let file = source
        .open_lock_file(
            &context,
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    drop(file);
    let after = fixture.model().take_observation().unwrap().unwrap();
    assert!(after
        .crash(&after.persist_all_plan())
        .unwrap()
        .bytes(Path::new(".trace.lock"))
        .is_some());
    let hidden = fixture.base.join("retained-original");
    std::fs::rename(&path, &hidden).unwrap();
    assert_eq!(
        source
            .open_lock_file(
                &context,
                ".trace.lock",
                SourceDirectoryLockMode::ReadWriteCreate
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert!(!path.exists());
    std::fs::rename(&hidden, &path).unwrap();
    fixture.model().capture().unwrap();
    fixture.observe(IoEvent::Remove, ".trace.lock", ObservationBoundary::After);
    std::fs::rename(&path, &hidden).unwrap();
    assert_eq!(
        source
            .remove_child(&context, ".trace.lock")
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert!(fixture.model().take_observation().unwrap().is_none());
    std::fs::rename(&hidden, &path).unwrap();
    source.remove_child(&context, ".trace.lock").unwrap();
    let after = fixture.model().take_observation().unwrap().unwrap();
    assert!(after
        .crash(&after.persist_all_plan())
        .unwrap()
        .bytes(Path::new(".trace.lock"))
        .is_none());
    fixture.model().capture().unwrap();
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn image_admission_failure_precedes_relative_native_creation() {
    let fixture = Fixture::new(
        8,
        ImageLimits {
            max_inodes: 1,
            ..ImageLimits::default()
        },
    );
    let source = fixture.source("");
    assert!(source
        .open_lock_file(
            &fixture.project.io_context(),
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate
        )
        .is_err());
    assert!(!fixture.root.join(".trace.lock").exists());
    assert_eq!(fixture.project.metrics().open, 2);
    fixture.model().capture().unwrap();
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn traced_child_uses_fresh_operation_inventory_and_refunds_its_descriptor() {
    let fixture = Fixture::new(3, ImageLimits::default());
    fixture.write("payload.hawdb", b"reserved child");
    let source = fixture.source("");
    let reservation = fixture.project.reserve(1).unwrap();
    let operation = fixture.project.io_context();
    let error = source
        .open_child(&source.context, "payload.hawdb")
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    let mut child = source.open_child(&operation, "payload.hawdb").unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (3, 0)
    );
    let mut bytes = String::new();
    child.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "reserved child");
    drop(child);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 1)
    );
    source.sync(&operation).unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 1)
    );
    drop((operation, reservation, source));
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (1, 0)
    );
    fixture.model().capture().unwrap();
}

#[test]
fn traced_capture_borrows_fresh_inventory_and_releases_unused_producer_slots() {
    let fixture = Fixture::new(3, ImageLimits::default());
    let reservation = fixture.project.reserve(2).unwrap();
    let operation = fixture.project.io_context();
    // The two slots cover this capture and Windows' temporary full-ID audit.
    // Keeping the anchor's old context for that audit would falsely refuse at
    // the unchanged cap despite the caller owning all required capacity.
    let source = SourceDirectory::capture(&fixture.root, &operation).unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 1)
    );
    drop((operation, reservation));
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 0)
    );
    let file = source
        .open_lock_file(
            &fixture.project.io_context(),
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    drop(file);
    source.sync(&fixture.project.io_context()).unwrap();
    drop(source);
    fixture.model().capture().unwrap();
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert!(fixture.project.metrics().high_water <= 3);
}

#[test]
fn denied_windows_sync_upgrade_does_not_advance_the_modeled_directory_barrier() {
    let fixture = Fixture::new(3, ImageLimits::default());
    let source = fixture.source("");
    let context = fixture.project.io_context();
    let file = source
        .open_lock_file(
            &context,
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    fixture.observe(
        IoEvent::FileSync,
        ".trace.lock",
        ObservationBoundary::Before,
    );
    let result = source.sync(&context);
    #[cfg(windows)]
    assert!(hawdb_core::error::file_descriptor_error(&result.unwrap_err()).is_some());
    #[cfg(unix)]
    result.unwrap(); // Unix fsync uses the already-held directory, no new FD.
    file.sync_all().unwrap();
    let before = fixture.model().take_observation().unwrap().unwrap();
    assert_eq!(
        before
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new(".trace.lock"))
            .is_some(),
        cfg!(unix)
    );
    drop(file);
    source.sync(&context).unwrap();
    drop(source);
    assert!(fixture.project.metrics().high_water <= 3);
    let snapshot = fixture.model().capture().unwrap();
    assert!(snapshot
        .crash(&CrashPlan::default())
        .unwrap()
        .bytes(Path::new(".trace.lock"))
        .is_some());
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn relative_directory_sync_observations_preserve_before_and_after_namespace_cuts() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(32, ImageLimits::default());
        fixture.directory("source");
        let source = fixture.source("source");
        let context = fixture.project.io_context();
        let mut file = source
            .open_lock_file(
                &context,
                ".trace.lock",
                SourceDirectoryLockMode::ReadWriteCreate,
            )
            .unwrap();
        file.write_all(b"durable bytes").unwrap();
        file.sync_all().unwrap();
        drop(file);
        fixture.observe(IoEvent::DirectorySync, "source", boundary);
        source.sync(&context).unwrap();
        let cut = fixture.model().take_observation().unwrap().unwrap();
        let image = cut.crash(&CrashPlan::default()).unwrap();
        assert_eq!(
            image.bytes(Path::new("source/.trace.lock")).is_some(),
            boundary == ObservationBoundary::After
        );
        assert_eq!(
            cut.crash(&cut.persist_all_plan())
                .unwrap()
                .bytes(Path::new("source/.trace.lock"))
                .unwrap(),
            b"durable bytes"
        );
        fixture.model().capture().unwrap();
        drop(source);
        assert_eq!(fixture.project.metrics().open, 1);
    }
}

#[test]
fn relative_unlink_observations_keep_durable_names_until_the_real_directory_sync() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(32, ImageLimits::default());
        fixture.write("source/payload.hawdb", b"old inode");
        let source = fixture.source("source");
        let context = fixture.project.io_context();
        fixture.observe(IoEvent::Remove, "source/payload.hawdb", boundary);
        source.remove_child(&context, "payload.hawdb").unwrap();
        let cut = fixture.model().take_observation().unwrap().unwrap();
        let path = Path::new("source/payload.hawdb");
        assert_eq!(
            cut.crash(&CrashPlan::default())
                .unwrap()
                .bytes(path)
                .unwrap(),
            b"old inode"
        );
        assert_eq!(
            cut.crash(&cut.persist_all_plan())
                .unwrap()
                .bytes(path)
                .is_some(),
            boundary == ObservationBoundary::Before
        );
        source.sync(&context).unwrap();
        assert!(fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(path)
            .is_none());
        drop(source);
        assert_eq!(fixture.project.metrics().open, 1);
    }
}

#[test]
fn same_logical_inode_number_cannot_transfer_a_directory_to_another_model() {
    let left = Fixture::new(16, ImageLimits::default());
    let right = Fixture::new(16, ImageLimits::default());
    left.write("payload.hawdb", b"left");
    right.write("payload.hawdb", b"right");
    let mut a = left.source("");
    let mut b = right.source("");
    std::mem::swap(&mut a.trace, &mut b.trace);
    assert_eq!(
        a.open_child(&left.project.io_context(), "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        b.remove_child(&right.project.io_context(), "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    std::mem::swap(&mut a.trace, &mut b.trace);
    left.model().capture().unwrap();
    right.model().capture().unwrap();
    drop((a, b));
    assert_eq!(left.project.metrics().open, 1);
    assert_eq!(right.project.metrics().open, 1);
}

#[test]
fn last_relative_file_keeps_the_model_anchor_after_directory_and_model_handles_close() {
    let mut fixture = Fixture::new(16, ImageLimits::default());
    let context = fixture.project.io_context();
    let source = fixture.source("");
    let mut file = source
        .open_lock_file(
            &context,
            ".trace.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    drop(source);
    drop(fixture.model.take());
    assert_eq!(fixture.project.metrics().open, 2);
    file.write_all(b"still traced").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(fixture.project.metrics().open, 0);
    let source = fixture.source("");
    let mut file = source.open_child(&context, ".trace.lock").unwrap();
    let mut text = String::new();
    file.read_to_string(&mut text).unwrap();
    assert_eq!(text, "still traced");
    drop((file, source));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn captured_wal_append_records_real_append_offsets_and_required_prefix_barriers() {
    let fixture = Fixture::new(8, ImageLimits::default());
    fixture.write("wal.1.hawdb", b"header");
    let baseline_descriptors = fixture.project.metrics().open;
    let directory = fixture.source("");
    let context = fixture.project.io_context();
    let mut file = directory
        .open_wal_append_child(&context, "wal.1.hawdb")
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"-commit").unwrap();
    let unsynced = fixture.model().capture().unwrap();
    assert_eq!(
        unsynced
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("wal.1.hawdb"))
            .unwrap(),
        b"header"
    );
    let expected = b"header-commit";
    assert_eq!(
        unsynced
            .crash(&unsynced.persist_all_plan())
            .unwrap()
            .bytes(Path::new("wal.1.hawdb"))
            .unwrap(),
        expected
    );
    file.sync_data().unwrap();
    let synced = fixture.model().capture().unwrap();
    assert_eq!(
        synced
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("wal.1.hawdb"))
            .unwrap(),
        expected
    );
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"-torn").unwrap();
    file.rollback_wal_append(expected.len() as u64, b"unused")
        .unwrap();
    directory.sync(&context).unwrap();
    let repaired = fixture.model().capture().unwrap();
    assert_eq!(
        repaired
            .crash(&CrashPlan::default())
            .unwrap()
            .bytes(Path::new("wal.1.hawdb"))
            .unwrap(),
        expected
    );
    assert_eq!(
        std::fs::read(fixture.root.join("wal.1.hawdb")).unwrap(),
        expected
    );
    drop((file, directory));
    assert_eq!(fixture.project.metrics().open, baseline_descriptors);
}

#[test]
fn native_directory_component_recording_preserves_names_and_before_after_namespace_images() {
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new(16, ImageLimits::default());
        let parent = fixture.source("");
        let context = fixture.project.io_context();
        let name = "configured 知识 directory";
        let mut candidate = parent
            .prepare_directory_component_candidate(OsStr::new(name))
            .unwrap();
        fixture.observe(IoEvent::CreateDirectory, name, boundary);
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap();
        assert!(candidate.creation_completed());
        let cut = fixture.model().take_observation().unwrap().unwrap();
        assert_eq!(cut.observed_path(), Some(Path::new(name)));
        let image = cut.crash(&cut.persist_all_plan()).unwrap();
        assert_eq!(
            image.directory_paths().any(|path| path == Path::new(name)),
            boundary == ObservationBoundary::After
        );
        let child = parent
            .open_created_directory_component(&context, &candidate)
            .unwrap();
        assert!(child.matches_namespace(&fixture.root.join(name)).unwrap());
        assert!(parent
            .open_directory_component(&context, OsStr::new(name))
            .unwrap()
            .same_directory(&child));
        // Unsynchronized creation may be lost; the actual parent barrier makes
        // the same logical/native entry survive the existing crash model.
        assert!(!fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap()
            .directory_paths()
            .any(|path| path == Path::new(name)));
        parent.sync(&context).unwrap();
        assert!(fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap()
            .directory_paths()
            .any(|path| path == Path::new(name)));
    }
}

#[test]
fn native_directory_component_recorder_capacity_and_unbound_scope_refuse_before_effect() {
    let fixture = Fixture::new(
        8,
        ImageLimits {
            max_inodes: 1,
            ..Default::default()
        },
    );
    let context = fixture.project.io_context();
    let mut parent = fixture.source("");
    let name = OsStr::new("bounded 目录");
    let mut candidate = parent.prepare_directory_component_candidate(name).unwrap();
    assert!(parent
        .create_directory_component_candidate(&context, &mut candidate)
        .is_err());
    assert!(candidate.creation_unattempted());
    assert!(!candidate.may_own_entry());
    assert!(!fixture.root.join(name).exists());
    assert!(parent.trace.take().is_some());
    assert_eq!(
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        parent
            .open_directory_component(&context, name)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert!(candidate.creation_unattempted());
    assert!(!fixture.root.join(name).exists());
}

#[cfg(windows)]
#[test]
fn native_directory_component_windows_recorder_counts_native_units_not_encoded_bytes() {
    let fixture = Fixture::new(12, ImageLimits::default());
    let parent = fixture.source("");
    let context = fixture.project.io_context();
    let name = "知".repeat(200);
    assert!(OsStr::new(&name).as_encoded_bytes().len() > 255);
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new(&name))
        .unwrap();
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    let child = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    assert!(child.matches_namespace(&fixture.root.join(&name)).unwrap());
    parent.sync(&context).unwrap();
    assert!(fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap()
        .directory_paths()
        .any(|path| path == Path::new(&name)));
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_recorded_long_name_survives_independent_capture() {
    let fixture = Fixture::new(16, ImageLimits::default());
    let parent = fixture.source("");
    let context = fixture.project.io_context();
    let name = "汉".repeat(255);
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new(&name))
        .unwrap();
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    let child = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    let independent = SourceDirectory::capture(&fixture.root.join(&name), &context).unwrap();
    assert!(child.same_directory(&independent));
    parent.sync(&context).unwrap();
    let image = fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert!(image.directory_paths().any(|path| path == Path::new(&name)));
    drop((independent, child, parent));
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    let base = fixture.base.clone();
    drop(fixture);
    assert!(!base.exists(), "owned native fixture cleanup completed");
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_recorded_alias_binds_original_inode_and_one_image_path() {
    for (original, alias) in [
        ("e\u{301}".repeat(60), "é".repeat(60)),
        ("é".repeat(255), "e\u{301}".repeat(255)),
    ] {
        let fixture = Fixture::new(16, ImageLimits::default());
        let parent = fixture.source("");
        let context = fixture.project.io_context();
        let mut candidate = parent
            .prepare_directory_component_candidate(OsStr::new(&original))
            .unwrap();
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap();
        let original_child = parent
            .open_created_directory_component(&context, &candidate)
            .unwrap();
        let enumerated = std::fs::read_dir(&fixture.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(enumerated, [std::ffi::OsString::from(&original)]);
        let aliased = parent.open_directory_component(&context, OsStr::new(&alias));
        assert!(
            aliased.is_ok(),
            "native alias must resolve the original recorded inode: {aliased:?}"
        );
        let child = aliased.unwrap();
        assert!(child.same_directory(&original_child));
        let mut file = child.create_child_new(&context, "payload").unwrap();
        file.write_all(b"one logical directory").unwrap();
        file.sync_all().unwrap();
        drop(file);
        child.sync(&context).unwrap();
        parent.sync(&context).unwrap();
        let image = fixture
            .model()
            .capture()
            .unwrap()
            .crash(&CrashPlan::default())
            .unwrap();
        assert_eq!(
            image.bytes(&Path::new(&original).join("payload")).unwrap(),
            b"one logical directory"
        );
        assert!(image.bytes(&Path::new(&alias).join("payload")).is_none());
        assert_eq!(
            image
                .directory_paths()
                .filter(|path| !path.as_os_str().is_empty())
                .collect::<Vec<_>>(),
            [Path::new(&original)]
        );
        drop((child, original_child, parent));
        assert_eq!(fixture.project.metrics().open, 1);
        assert_eq!(fixture.project.metrics().reserved, 0);
        let base = fixture.base.clone();
        drop(fixture);
        assert!(!base.exists(), "owned native fixture cleanup completed");
    }
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_alias_probe_is_lazily_admitted_and_refunded() {
    let fixture = Fixture::new(3, ImageLimits::default());
    let parent = fixture.source("");
    let context = fixture.project.io_context();
    let original = "e\u{301}".repeat(60);
    let alias = "é".repeat(60);
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new(&original))
        .unwrap();
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    parent.sync(&context).unwrap();
    // Anchor + parent leave one FD. Exact-key open must not admit a probe.
    let exact = parent
        .open_directory_component(&context, OsStr::new(&original))
        .unwrap();
    drop(exact);
    let before = fixture.project.metrics();
    let error = parent
        .open_directory_component(&context, OsStr::new(&alias))
        .unwrap_err();
    assert!(
        hawdb_core::error::file_descriptor_error(&error).is_some(),
        "alias requires a separately admitted physical probe: {error}"
    );
    assert_eq!(fixture.project.metrics().open, before.open);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(std::fs::read_dir(&fixture.root).unwrap().count(), 1);
    // Verification traverses the native tree with its own counted handles.
    // Free the parent after asserting refusal/refund, before that traversal.
    drop(parent);
    let image = fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        image
            .directory_paths()
            .filter(|path| !path.as_os_str().is_empty())
            .collect::<Vec<_>>(),
        [Path::new(&original)]
    );
    assert_eq!(fixture.project.metrics().open, 1);
    assert!(fixture.project.metrics().high_water <= 3);
    let base = fixture.base.clone();
    drop(fixture);
    assert!(!base.exists(), "owned native fixture cleanup completed");
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_longer_registered_trace_retains_caller_memory_until_drop() {
    use hawdb_core::{RuntimeMemoryError, RuntimeMemoryPermit, RuntimeTaskContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };
    use std::num::NonZeroUsize;
    #[derive(Debug)]
    struct FundedDirectory {
        // Rust drops fields in declaration order: destroy the actual trace
        // before its owned working-memory reservation is refunded.
        directory: SourceDirectory,
        memory: Option<Box<dyn RuntimeMemoryPermit>>,
    }
    fn open_funded(
        parent: &SourceDirectory,
        context: &FileOpenContext,
        name: &OsStr,
        task: &RuntimeTaskContext,
    ) -> io::Result<FundedDirectory> {
        let bytes = parent.native_component_metadata_bound(name)?;
        let memory = task
            .reserve_working_memory(bytes as u64)
            .map_err(io::Error::other)?;
        let directory = parent.open_directory_component(context, name)?;
        Ok(FundedDirectory { directory, memory })
    }
    fn used(task: &RuntimeTaskContext, ceiling: u64) -> u64 {
        match task.reserve_working_memory(ceiling) {
            Err(RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => ceiling - available_bytes,
            Ok(lease) => {
                drop(lease);
                0
            }
            other => panic!("read actual runtime memory admission: {other:?}"),
        }
    }
    let ceiling = 16 * 1024;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let work = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = work.bind_task_context(RuntimeTaskContext::default());
    let fixture = Fixture::new(12, ImageLimits::default());
    let parent = fixture.source("");
    let context = fixture.project.io_context();
    let original = "e\u{301}".repeat(60);
    let alias = "é".repeat(60);
    assert!(original.len() > alias.len());
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new(&original))
        .unwrap();
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    parent.sync(&context).unwrap();
    let old_requested_bound = parent.metadata_bound(alias.len()).unwrap() as u64;
    // Measure the real governor's retained permit overhead rather than guess
    // it. Leave exactly enough for the old producer plus its permit overhead;
    // the correct larger native bound must refuse before any native open.
    let sample = task.reserve_working_memory(1).unwrap();
    let permit_overhead = used(&task, ceiling) - 1;
    drop(sample);
    assert_eq!(used(&task, ceiling), 0);
    let held = task
        .reserve_working_memory(ceiling - old_requested_bound - 2 * permit_overhead)
        .unwrap();
    let descriptors = fixture.project.metrics();
    assert!(
        open_funded(&parent, &context, OsStr::new(&alias), &task).is_err(),
        "requested-length funding must not admit a longer registered trace"
    );
    assert_eq!(
        fixture.project.metrics().high_water,
        descriptors.high_water,
        "budget refusal precedes native IO"
    );
    assert_eq!(fixture.project.metrics().open, descriptors.open);
    assert_eq!(std::fs::read_dir(&fixture.root).unwrap().count(), 1);
    let before = fixture
        .model()
        .capture()
        .unwrap()
        .crash(&CrashPlan::default())
        .unwrap();
    assert_eq!(
        before
            .directory_paths()
            .filter(|path| !path.as_os_str().is_empty())
            .collect::<Vec<_>>(),
        [Path::new(&original)]
    );
    drop(held);
    assert_eq!(used(&task, ceiling), 0);
    let funded = open_funded(&parent, &context, OsStr::new(&alias), &task).unwrap();
    let charged = used(&task, ceiling);
    assert_eq!(
        charged,
        funded.memory.as_ref().unwrap().bytes() + permit_overhead
    );
    let actual_owned_bound = funded.directory.metadata_bound(0).unwrap() as u64;
    assert!(funded.memory.as_ref().unwrap().bytes() >= actual_owned_bound, "actual longer trace allocation must remain funded: charged={charged}, actual_bound={actual_owned_bound}");
    assert!(funded
        .directory
        .matches_namespace(&fixture.root.join(&original))
        .unwrap());
    eprintln!("native component caller funding: requested_bytes={}, registered_bytes={}, lease_bytes={}, retained_charge={}, actual_owned_bound={}, permit_overhead={}", alias.len(), original.len(), funded.memory.as_ref().unwrap().bytes(), charged, actual_owned_bound, permit_overhead);
    drop(parent);
    assert_eq!(
        used(&task, ceiling),
        charged,
        "caller lease stays with the actual returned directory"
    );
    drop(funded);
    assert_eq!(
        used(&task, ceiling),
        0,
        "real directory Drop refunds its caller memory lease"
    );
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    let base = fixture.base.clone();
    drop(fixture);
    assert!(!base.exists(), "owned native fixture cleanup completed");
    drop((task, work));
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
