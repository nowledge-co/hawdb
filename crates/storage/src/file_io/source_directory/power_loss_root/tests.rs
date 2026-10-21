// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;

struct Fixture {
    root: PathBuf,
    project: crate::file_descriptors::ProjectFileDescriptors,
}
impl Fixture {
    fn new(limit: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-recorder-root-walk-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project =
            crate::file_descriptors::ProjectFileDescriptors::acquire(&root, limit).unwrap();
        Self { root, project }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn verification_walk_uses_captured_native_tree_and_unicode_after_root_replacement() {
    let fixture = Fixture::new(16);
    let selected = fixture.root.join("selected");
    let moved = fixture.root.join("moved");
    fs::create_dir_all(selected.join("目录")).unwrap();
    fs::write(selected.join("目录/данные"), b"captured").unwrap();
    let anchor = PowerLossRoot::capture(&selected, &fixture.project.io_context()).unwrap();
    fs::rename(&selected, &moved).unwrap();
    fs::create_dir_all(selected.join("目录")).unwrap();
    fs::write(selected.join("目录/данные"), b"foreign!").unwrap();
    for _ in 0..2 {
        let mut actual = BTreeMap::new();
        anchor
            .visit_tree(2, |relative, mut file| {
                if file.metadata()?.is_file() {
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes)?;
                    actual.insert(relative.to_path_buf(), bytes);
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(
            actual,
            BTreeMap::from([(PathBuf::from("目录/данные"), b"captured".to_vec())])
        );
        assert_eq!(fixture.project.metrics().open, 1);
        assert_eq!(fixture.project.metrics().reserved, 0);
    }
    assert!(anchor.require_path(&selected).is_err());
    assert_eq!(fs::read(selected.join("目录/данные")).unwrap(), b"foreign!");
    assert_eq!(fs::read(moved.join("目录/данные")).unwrap(), b"captured");
    drop(anchor);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn verification_entry_and_descriptor_denial_refund_all_temporary_handles() {
    let fixture = Fixture::new(2);
    fs::write(fixture.root.join("entry"), b"unchanged").unwrap();
    let anchor = PowerLossRoot::capture(&fixture.root, &fixture.project.io_context()).unwrap();
    // Zero entries refuses before opening a child; a one-entry budget then
    // reaches actual FD refusal (retained root + cursor fill the unchanged cap).
    let mut callbacks = 0;
    let error = anchor
        .visit_tree(0, |_, _| {
            callbacks += 1;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(callbacks, 1); // root only
    let error = anchor.visit_tree(1, |_, _| Ok(())).unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert!(fixture.project.metrics().high_water <= 2);
    assert_eq!(fs::read(fixture.root.join("entry")).unwrap(), b"unchanged");
    drop(anchor);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(unix)]
#[test]
fn verification_walk_rejects_links_and_fifos_before_reading() {
    let fixture = Fixture::new(8);
    let name = "wal";
    fs::write(fixture.root.join(name), b"native name").unwrap();
    let anchor = PowerLossRoot::capture(&fixture.root, &fixture.project.io_context()).unwrap();
    let mut names = Vec::new();
    anchor
        .visit_tree(1, |path, _| {
            names.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
    assert!(names.contains(&PathBuf::from(name)));
    std::os::unix::fs::symlink(name, fixture.root.join("alias")).unwrap();
    assert!(anchor.visit_tree(2, |_, _| Ok(())).is_err());
    fs::remove_file(fixture.root.join("alias")).unwrap();
    let fifo =
        std::ffi::CString::new(fixture.root.join("fifo").as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: valid terminated native path and ordinary owner-only mode.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    assert!(anchor.visit_tree(2, |_, _| Ok(())).is_err());
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    drop(anchor);
    assert_eq!(fixture.project.metrics().open, 0);
}

// APFS rejects invalid UTF-8 when creating the fixture. Linux supplies actual
// native coverage for lossless non-UTF-8 components, rather than a skipped or
// lossy substitute on filesystems where such a name cannot exist.
#[cfg(target_os = "linux")]
#[test]
fn verification_walk_preserves_non_utf8_names() {
    use std::os::unix::ffi::OsStringExt;
    let fixture = Fixture::new(8);
    let name = std::ffi::OsString::from_vec(vec![b'w', 0xff]);
    fs::write(fixture.root.join(&name), b"native name").unwrap();
    let anchor = PowerLossRoot::capture(&fixture.root, &fixture.project.io_context()).unwrap();
    let mut names = Vec::new();
    anchor
        .visit_tree(1, |path, _| {
            names.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
    assert!(names.contains(&PathBuf::from(name)));
    assert_eq!(fixture.project.metrics().open, 1);
    drop(anchor);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn verification_callback_error_and_unwind_close_pending_children_and_cursors() {
    let fixture = Fixture::new(8);
    fs::create_dir(fixture.root.join("child")).unwrap();
    fs::write(fixture.root.join("child/file"), b"unchanged").unwrap();
    let anchor = PowerLossRoot::capture(&fixture.root, &fixture.project.io_context()).unwrap();
    let error = anchor
        .visit_tree(2, |_, file| {
            if file.metadata()?.is_file() {
                return Err(io::Error::other("visitor error"));
            }
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.to_string(), "visitor error");
    assert_eq!(fixture.project.metrics().open, 1);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        anchor
            .visit_tree(2, |_, file| {
                if file.metadata()?.is_file() {
                    panic!("visitor unwind");
                }
                Ok(())
            })
            .unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
    anchor.visit_tree(2, |_, _| Ok(())).unwrap();
    drop(anchor);
    assert_eq!(fixture.project.metrics().open, 0);
}
