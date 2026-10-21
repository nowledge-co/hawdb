// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
}
impl Fixture {
    fn new(limit: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-private-directory-candidate-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        Self { root, project }
    }
    fn parent(&self) -> SourceDirectory {
        SourceDirectory::capture(&self.root, &self.project.io_context()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn descriptor_refusal_precedes_creation_and_later_open_refusal_keeps_created_evidence() {
    let fixture = Fixture::new(2);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let blocker = parent.duplicate(&context).unwrap();
    let mut candidate = parent.prepare_directory_candidate("private").unwrap();
    let error = parent
        .create_directory_candidate(&context, &mut candidate)
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(!candidate.may_own_entry());
    assert!(!fixture.root.join("private").exists());
    drop(blocker);

    parent
        .create_directory_candidate(&context, &mut candidate)
        .unwrap();
    assert!(candidate.creation_completed());
    assert!(candidate.may_own_entry());
    let blocker = parent.duplicate(&context).unwrap();
    let error = parent
        .open_created_directory(&context, &candidate)
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(candidate.creation_completed());
    assert!(fixture.root.join("private").is_dir());
    drop(blocker);
    let child = parent.open_created_directory(&context, &candidate).unwrap();
    drop(child);
    parent
        .remove_created_directory(&context, &mut candidate)
        .unwrap();
    assert!(candidate.removal_completed());
    assert!(!candidate.may_own_entry());
    assert!(!fixture.root.join("private").exists());
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn collision_and_foreign_capture_never_authorize_adoption_or_deletion() {
    let fixture = Fixture::new(8);
    fs::create_dir(fixture.root.join("existing")).unwrap();
    fs::write(fixture.root.join("existing/payload"), b"retain evidence").unwrap();
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let other = parent.duplicate(&context).unwrap();
    let foreign_path = fixture.root.join("foreign");
    fs::create_dir(&foreign_path).unwrap();
    let foreign = SourceDirectory::capture(&foreign_path, &context).unwrap();
    let mut fresh = parent.prepare_directory_candidate("new-private").unwrap();
    // An absent name reaches native creation if origin checking is removed.
    // A collision fixture alone could mask that bug with AlreadyExists.
    for wrong_parent in [&other, &foreign] {
        assert_eq!(
            wrong_parent
                .create_directory_candidate(&context, &mut fresh)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(!fresh.may_own_entry());
        assert!(!fixture.root.join("new-private").exists());
        assert!(!foreign_path.join("new-private").exists());
    }
    let mut candidate = parent.prepare_directory_candidate("existing").unwrap();
    assert!(other
        .create_directory_candidate(&context, &mut candidate)
        .is_err());
    assert!(!candidate.may_own_entry());
    assert_eq!(
        parent
            .create_directory_candidate(&context, &mut candidate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!candidate.may_own_entry());
    assert!(parent.open_created_directory(&context, &candidate).is_err());
    assert!(parent
        .remove_created_directory(&context, &mut candidate)
        .is_err());
    assert_eq!(
        fs::read(fixture.root.join("existing/payload")).unwrap(),
        b"retain evidence"
    );
    drop((foreign, other, parent));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn duplicated_parent_creates_reads_and_removes_only_its_private_captured_directory() {
    let fixture = Fixture::new(8);
    let selected = fixture.root.join("selected");
    let moved = fixture.root.join("moved");
    fs::create_dir(&selected).unwrap();
    let context = fixture.project.io_context();
    let parent = SourceDirectory::capture(&selected, &context).unwrap();
    let retained = parent.duplicate(&context).unwrap();
    drop(parent);
    fs::rename(&selected, &moved).unwrap();
    fs::create_dir_all(selected.join("private")).unwrap();
    fs::write(selected.join("private/payload"), b"foreign bytes").unwrap();

    let mut candidate = retained.prepare_directory_candidate("private").unwrap();
    retained
        .create_directory_candidate(&context, &mut candidate)
        .unwrap();
    let child = retained
        .open_created_directory(&context, &candidate)
        .unwrap();
    let mut output = child.create_child_new(&context, "payload").unwrap();
    output.write_all(b"owned bytes").unwrap();
    output.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = String::new();
    output.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "owned bytes");
    assert!(child.create_child_new(&context, "payload").is_err());
    drop(output);
    assert!(retained
        .remove_created_directory(&context, &mut candidate)
        .is_err());
    assert!(candidate.creation_completed());
    child.remove_child(&context, "payload").unwrap();
    drop(child);
    retained
        .remove_created_directory(&context, &mut candidate)
        .unwrap();
    assert!(candidate.removal_completed());
    assert!(!moved.join("private").exists());
    assert_eq!(
        fs::read(selected.join("private/payload")).unwrap(),
        b"foreign bytes"
    );
    drop(retained);
    assert_eq!(fixture.project.metrics().open, 0);
}
