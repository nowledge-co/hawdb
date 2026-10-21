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
            "hawdb-source-directory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        Self { root, project }
    }
    fn capture(&self, path: &Path) -> SourceDirectory {
        SourceDirectory::capture(path, &self.project.io_context()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn relative_read_and_delete_keep_captured_root_after_namespace_replacement() {
    let fixture = Fixture::new(16);
    let original = fixture.root.join("source");
    let moved = fixture.root.join("moved");
    fs::create_dir(&original).unwrap();
    fs::write(original.join("artifact.1.hawdb"), b"captured source").unwrap();
    let source = fixture.capture(&original);
    fs::rename(&original, &moved).unwrap();
    fs::create_dir(&original).unwrap();
    fs::write(original.join("artifact.1.hawdb"), b"foreign source").unwrap();
    let replacement = fixture.capture(&original);
    let same = fixture.capture(&moved);
    assert!(source.same_directory(&same));
    assert_eq!(source.identity(), same.identity());
    assert_ne!(source.identity(), replacement.identity());
    assert!(!source.same_directory(&replacement));
    // This open occurs after replacement and starts cold; no cached file can
    // accidentally make an ordinary pathname-based implementation pass.
    let mut child = source
        .open_child(&fixture.project.io_context(), "artifact.1.hawdb")
        .unwrap();
    let mut bytes = Vec::new();
    child.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"captured source");
    drop(child);
    source
        .remove_child(&fixture.project.io_context(), "artifact.1.hawdb")
        .unwrap();
    assert!(!moved.join("artifact.1.hawdb").exists());
    assert_eq!(
        fs::read(original.join("artifact.1.hawdb")).unwrap(),
        b"foreign source"
    );
    drop((source, same, replacement));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn unlink_removes_captured_name_while_an_existing_child_handle_remains_readable() {
    let fixture = Fixture::new(8);
    let original = fixture.root.join("source");
    let moved = fixture.root.join("moved");
    fs::create_dir(&original).unwrap();
    fs::write(original.join("payload.hawdb"), b"old captured bytes").unwrap();
    let source = fixture.capture(&original);
    let context = fixture.project.io_context();
    fs::rename(&original, &moved).unwrap();
    fs::create_dir(&original).unwrap();
    fs::write(original.join("payload.hawdb"), b"foreign bytes").unwrap();

    // Capture precedes namespace drift, but open the child afterwards. Windows
    // refuses moving a directory containing an open child; that independent
    // publication limitation must not prevent this unlink contract from running.
    // The old handle remains live across unlink and same-name recreation.
    let mut held = source.open_child(&context, "payload.hawdb").unwrap();
    source.remove_child(&context, "payload.hawdb").unwrap();
    assert!(
        !moved.join("payload.hawdb").exists(),
        "successful unlink must remove the name before the old read handle closes"
    );
    assert_eq!(
        source
            .open_child(&context, "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    // This independent fixture write reuses the name, never the old inode.
    fs::write(moved.join("payload.hawdb"), b"new captured bytes").unwrap();
    let mut fresh = source.open_child(&context, "payload.hawdb").unwrap();
    let mut old_bytes = String::new();
    held.read_to_string(&mut old_bytes).unwrap();
    let mut new_bytes = String::new();
    fresh.read_to_string(&mut new_bytes).unwrap();
    assert_eq!(old_bytes, "old captured bytes");
    assert_eq!(new_bytes, "new captured bytes");
    assert_eq!(
        fs::read(original.join("payload.hawdb")).unwrap(),
        b"foreign bytes"
    );
    drop((fresh, held, source));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn child_handle_remains_usable_after_directory_authority_closes() {
    let fixture = Fixture::new(8);
    fs::write(fixture.root.join("payload.hawdb"), b"immutable child").unwrap();
    let source = fixture.capture(&fixture.root);
    let mut child = source
        .open_child(&fixture.project.io_context(), "payload.hawdb")
        .unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 1);
    let mut actual = String::new();
    child.read_to_string(&mut actual).unwrap();
    assert_eq!(actual, "immutable child");
    drop(child);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn descriptor_denial_precedes_child_open_and_returns_all_capacity() {
    let fixture = Fixture::new(1);
    fs::write(fixture.root.join("payload.hawdb"), b"unchanged").unwrap();
    let source = fixture.capture(&fixture.root);
    let error = source
        .open_child(&fixture.project.io_context(), "payload.hawdb")
        .unwrap_err();
    assert!(
        hawdb_core::error::file_descriptor_error(&error).is_some(),
        "{error}"
    );
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
    let source = fixture.capture(&fixture.root);
    assert!(source
        .open_child(&fixture.project.io_context(), "absent.hawdb")
        .is_err());
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn invalid_names_missing_files_and_directories_never_escape_or_leak() {
    let fixture = Fixture::new(8);
    let source = fixture.capture(&fixture.root);
    for name in [
        "",
        ".",
        "..",
        "../outside",
        "a/b",
        "a\\b",
        "a:stream",
        "nul\0suffix",
        "foreign name",
        "你好",
    ] {
        assert_eq!(
            source
                .open_child(&fixture.project.io_context(), name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            source
                .remove_child(&fixture.project.io_context(), name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fixture.project.metrics().open, 1);
    }
    assert_eq!(
        source
            .open_child(&fixture.project.io_context(), &"a".repeat(256))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        source
            .open_child(&fixture.project.io_context(), "missing")
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    fs::create_dir(fixture.root.join("directory")).unwrap();
    assert!(source
        .open_child(&fixture.project.io_context(), "directory")
        .is_err());
    assert!(source
        .remove_child(&fixture.project.io_context(), "directory")
        .is_err());
    assert!(fixture.root.join("directory").is_dir());
    fs::write(fixture.root.join("regular"), b"bytes").unwrap();
    assert!(
        SourceDirectory::capture(&fixture.root.join("regular"), &fixture.project.io_context())
            .is_err()
    );
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(unix)]
#[test]
fn child_symlinks_are_not_followed_and_rejection_closes_capacity() {
    let fixture = Fixture::new(8);
    let outside = fixture.root.join("outside");
    let directory = fixture.root.join("source");
    fs::create_dir(&directory).unwrap();
    fs::write(&outside, b"foreign bytes").unwrap();
    std::os::unix::fs::symlink(&outside, directory.join("linked.hawdb")).unwrap();
    let source = fixture.capture(&directory);
    assert!(source
        .open_child(&fixture.project.io_context(), "linked.hawdb")
        .is_err());
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fs::read(&outside).unwrap(), b"foreign bytes");
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
#[test]
fn power_loss_context_refuses_before_opening_an_unrecorded_native_handle() {
    let fixture = Fixture::new(8);
    let model = crate::power_loss::PowerLossModel::attach(
        &fixture.project,
        crate::power_loss::image::ImageLimits::default(),
    )
    .unwrap();
    let error = SourceDirectory::capture(&fixture.root, &fixture.project.io_context()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    // Only the recorder's existing captured root remains; refusal adds none.
    assert_eq!(fixture.project.metrics().open, 1);
    model.capture().unwrap();
    drop(model);
    let source = fixture.capture(&fixture.root);
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(unix)]
#[test]
fn corrupt_fifo_is_rejected_without_waiting_for_a_writer() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let fixture = Fixture::new(8);
    let path = fixture.root.join("payload.hawdb");
    let encoded = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: owned NUL-terminated fixture path and valid permission mode.
    assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
    let source = fixture.capture(&fixture.root);
    assert_eq!(
        source
            .open_child(&fixture.project.io_context(), "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn child_open_uses_the_fresh_operations_reserved_descriptor_capacity() {
    let fixture = Fixture::new(2);
    fs::write(fixture.root.join("payload.hawdb"), b"reserved child").unwrap();
    let source = SourceDirectory::capture_for_path(&fixture.root).unwrap();
    let reservation = fixture.project.reserve(1).unwrap();
    let operation = source.operation_context();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (1, 1)
    );
    // The original capture context predates this reservation. Reusing it would
    // fail at the project limit despite the operation owning one available FD.
    let mut child = source.open_child(&operation, "payload.hawdb").unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 0)
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
        (1, 1)
    );
    drop(operation);
    drop(reservation);
    drop(source);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (0, 0)
    );
}

#[test]
fn foreign_project_context_cannot_open_or_remove_captured_children() {
    let fixture = Fixture::new(8);
    let foreign = Fixture::new(8);
    fs::write(fixture.root.join("payload.hawdb"), b"owned child").unwrap();
    let source = fixture.capture(&fixture.root);
    let context = foreign.project.io_context();
    assert_eq!(
        source
            .open_child(&context, "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        source
            .remove_child(&context, "payload.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        fs::read(fixture.root.join("payload.hawdb")).unwrap(),
        b"owned child"
    );
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(foreign.project.metrics().open, 0);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn lock_opens_are_relative_nontruncating_and_support_existing_read_only_handles() {
    use std::io::Write;
    let fixture = Fixture::new(8);
    let original = fixture.root.join("source");
    let moved = fixture.root.join("moved");
    fs::create_dir(&original).unwrap();
    let source = fixture.capture(&original);
    fs::rename(&original, &moved).unwrap();
    fs::create_dir(&original).unwrap();
    fs::write(original.join(".source.lock"), b"foreign lock").unwrap();
    let context = fixture.project.io_context();
    let mut lock = source
        .open_lock_file(
            &context,
            ".source.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    lock.write_all(b"original lock").unwrap();
    drop(lock);
    let mut existing = source
        .open_lock_file(
            &context,
            ".source.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    let mut bytes = String::new();
    existing.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "original lock");
    drop(existing);
    let mut readonly = source
        .open_lock_file(
            &context,
            ".source.lock",
            SourceDirectoryLockMode::ReadExisting,
        )
        .unwrap();
    assert!(readonly.write_all(b"must not modify").is_err());
    drop(readonly);
    assert_eq!(
        fs::read(moved.join(".source.lock")).unwrap(),
        b"original lock"
    );
    assert_eq!(
        fs::read(original.join(".source.lock")).unwrap(),
        b"foreign lock"
    );
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn lock_open_uses_current_inventory_and_rejects_noncontrol_names_before_io() {
    let fixture = Fixture::new(2);
    let source = fixture.capture(&fixture.root);
    let reservation = fixture.project.reserve(1).unwrap();
    let context = fixture.project.io_context();
    for name in ["payload.hawdb", "source.lock", ".lock", "../.source.lock"] {
        assert_eq!(
            source
                .open_lock_file(&context, name, SourceDirectoryLockMode::ReadWriteCreate)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(
        source
            .open_lock_file(
                &context,
                ".missing.lock",
                SourceDirectoryLockMode::ReadExisting
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert!(!fixture.root.join(".missing.lock").exists());
    let lock = source
        .open_lock_file(
            &context,
            ".source.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (2, 0)
    );
    drop(lock);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (1, 1)
    );
    drop(context);
    drop(reservation);
    drop(source);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (0, 0)
    );
}

#[test]
fn denied_lock_open_does_not_create_an_unadmitted_control_file() {
    let fixture = Fixture::new(1);
    let source = fixture.capture(&fixture.root);
    let error = source
        .open_lock_file(
            &fixture.project.io_context(),
            ".source.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(!fixture.root.join(".source.lock").exists());
    assert_eq!(fixture.project.metrics().open, 1);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn relative_lock_handle_enforces_native_peer_process_exclusion() {
    const PROBE_ROOT: &str = "HAWDB_SOURCE_DIRECTORY_LOCK_PROBE_ROOT";
    const PROBE_BUSY: &str = "HAWDB_SOURCE_DIRECTORY_LOCK_PROBE_BUSY";
    const CASE: &str = "file_io::source_directory::tests::relative_lock_handle_enforces_native_peer_process_exclusion";
    if let Some(root) = std::env::var_os(PROBE_ROOT) {
        let project = ProjectFileDescriptors::acquire_existing(Path::new(&root), 8).unwrap();
        let source = SourceDirectory::capture(Path::new(&root), &project.io_context()).unwrap();
        let file = source
            .open_lock_file(
                &project.io_context(),
                ".source.lock",
                SourceDirectoryLockMode::ReadWriteCreate,
            )
            .unwrap();
        if std::env::var_os(PROBE_BUSY).is_some() {
            assert!(matches!(
                file.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
        } else {
            file.try_lock().unwrap();
        }
        return;
    }
    let fixture = Fixture::new(8);
    let source = fixture.capture(&fixture.root);
    let file = source
        .open_lock_file(
            &fixture.project.io_context(),
            ".source.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    file.try_lock().unwrap();
    let probe = |busy| {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", CASE, "--nocapture"])
            .env(PROBE_ROOT, &fixture.root);
        if busy {
            command.env(PROBE_BUSY, "1");
        } else {
            command.env_remove(PROBE_BUSY);
        }
        let result = command.output().unwrap();
        let text = String::from_utf8_lossy(&result.stdout);
        assert!(
            result.status.success(),
            "{text}\n{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            text.contains("1 passed; 0 failed"),
            "probe selected no case: {text}"
        );
    };
    probe(true);
    drop(file);
    probe(false);
    drop(source);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn enumeration_uses_captured_root_and_independent_cursors_after_replacement() {
    let fixture = Fixture::new(8);
    let selected = fixture.root.join("selected");
    fs::create_dir(&selected).unwrap();
    for index in 0..80 {
        fs::write(selected.join(format!("segment-{index}")), b"original").unwrap();
    }
    let directory = fixture.capture(&selected);
    fs::rename(&selected, fixture.root.join("moved")).unwrap();
    fs::create_dir(&selected).unwrap();
    fs::write(selected.join("foreign"), b"foreign").unwrap();
    let context = fixture.project.io_context();
    for _ in 0..2 {
        let mut names = std::collections::BTreeSet::new();
        directory
            .visit_children(&context, 80, |name| {
                assert!(names.insert(name.to_owned()));
                // Callbacks can admit native I/O and another independent cursor.
                let mut child = directory.open_child(&context, name)?;
                let mut bytes = String::new();
                child.read_to_string(&mut bytes)?;
                assert_eq!(bytes, "original");
                if names.len() == 1 {
                    let mut nested = 0;
                    directory.visit_children(&context, 80, |_| {
                        nested += 1;
                        Ok(())
                    })?;
                    assert_eq!(nested, 80);
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(names.len(), 80);
        assert!(!names.contains("foreign"));
        assert_eq!(fixture.project.metrics().open, 1);
    }
    assert_eq!(fs::read(selected.join("foreign")).unwrap(), b"foreign");
    drop(directory);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn enumeration_denial_budget_callback_failure_and_unwind_return_cursor_capacity() {
    let fixture = Fixture::new(2);
    fs::write(fixture.root.join("one"), b"one").unwrap();
    fs::write(fixture.root.join("two"), b"two").unwrap();
    let directory = fixture.capture(&fixture.root);
    let context = fixture.project.io_context();
    let child = directory.open_child(&context, "one").unwrap();
    let mut visited = 0;
    assert!(directory
        .visit_children(&context, 2, |_| {
            visited += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(visited, 0);
    drop(child);
    assert!(directory
        .visit_children(&context, 1, |_| {
            visited += 1;
            Ok(())
        })
        .is_err());
    assert_eq!(visited, 1);
    assert_eq!(fixture.project.metrics().open, 1);
    let error = directory
        .visit_children(&context, 2, |_| Err(io::Error::other("callback stopped")))
        .unwrap_err();
    assert_eq!(error.to_string(), "callback stopped");
    assert_eq!(fixture.project.metrics().open, 1);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        directory
            .visit_children(&context, 2, |_| panic!("visitor unwind"))
            .unwrap();
    }));
    assert!(panic.is_err());
    assert_eq!(fixture.project.metrics().open, 1);
    let mut complete = 0;
    directory
        .visit_children(&context, 2, |_| {
            complete += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(complete, 2);
    drop(directory);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn enumeration_rejects_foreign_domains_and_unsupported_names_without_partial_success() {
    let fixture = Fixture::new(4);
    let directory = fixture.capture(&fixture.root);
    let foreign = Fixture::new(4);
    assert!(directory
        .visit_children(&foreign.project.io_context(), 4, |_| panic!(
            "foreign callback"
        ))
        .is_err());
    assert_eq!(foreign.project.metrics().open, 0);
    let context = fixture.project.io_context();
    // Unsupported entries are evidence, never silently omitted as if absent.
    for name in ["unexpected space", "不可忽略"] {
        fs::write(fixture.root.join(name), b"unexpected").unwrap();
        assert!(directory
            .visit_children(&context, 4, |_| panic!("invalid callback"))
            .is_err());
        assert_eq!(fixture.project.metrics().open, 1);
        assert_eq!(fs::read(fixture.root.join(name)).unwrap(), b"unexpected");
        fs::remove_file(fixture.root.join(name)).unwrap();
    }
    directory
        .visit_children(&context, 0, |_| panic!("empty callback"))
        .unwrap();
}

#[test]
fn child_directory_retains_relative_authority_after_both_pathnames_move_and_parent_closes() {
    let fixture = Fixture::new(8);
    let selected = fixture.root.join("selected");
    let moved = fixture.root.join("moved");
    fs::create_dir_all(selected.join("child")).unwrap();
    fs::write(selected.join("child/payload"), b"retained").unwrap();
    let parent = fixture.capture(&selected);
    let context = fixture.project.io_context();
    // Windows can refuse moving an ancestor with an open descendant directory.
    // Capture the child through the held parent after replacing its old path.
    // Unix retains the original stronger move-after-child-capture sequence.
    #[cfg(windows)]
    {
        fs::rename(&selected, &moved).unwrap();
        fs::create_dir_all(selected.join("child")).unwrap();
        fs::write(selected.join("child/payload"), b"foreign-root").unwrap();
    }
    let captured_parent = if cfg!(windows) { &moved } else { &selected };
    let child = parent.open_directory(&context, "child").unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    let inspect = |phase: &str| {
        let mut visited = 0;
        child
            .visit_children(&context, 1, |name| {
                assert_eq!(name, "payload");
                visited += 1;
                Ok(())
            })
            .unwrap_or_else(|error| panic!("captured child enumeration {phase}: {error}"));
        assert_eq!(visited, 1);
    };
    inspect("after capture through retained parent");
    fs::rename(
        captured_parent.join("child"),
        captured_parent.join("old-child"),
    )
    .unwrap();
    inspect("after child rename");
    fs::create_dir(captured_parent.join("child")).unwrap();
    fs::write(captured_parent.join("child/payload"), b"replacement-child").unwrap();
    #[cfg(not(windows))]
    {
        fs::rename(&selected, &moved).unwrap();
        fs::create_dir_all(selected.join("child")).unwrap();
        fs::write(selected.join("child/payload"), b"foreign-root").unwrap();
    }
    inspect("after both replacement paths exist");
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 1);
    let mut names = Vec::new();
    child
        .visit_children(&context, 1, |name| {
            names.push(name.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(names, ["payload"]);
    let mut file = child.open_child(&context, "payload").unwrap();
    let mut bytes = String::new();
    file.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "retained");
    drop(file);
    child.remove_child(&context, "payload").unwrap();
    assert_eq!(
        fs::read(selected.join("child/payload")).unwrap(),
        b"foreign-root"
    );
    assert_eq!(
        fs::read(moved.join("child/payload")).unwrap(),
        b"replacement-child"
    );
    assert!(!moved.join("old-child/payload").exists());
    drop(child);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(windows)]
#[test]
fn ancestor_rename_with_retained_child_preserves_authority_on_success_or_native_refusal() {
    let fixture = Fixture::new(8);
    let selected = fixture.root.join("selected");
    let moved = fixture.root.join("moved");
    fs::create_dir_all(selected.join("child")).unwrap();
    fs::write(selected.join("child/payload"), b"retained").unwrap();
    let parent = fixture.capture(&selected);
    let context = fixture.project.io_context();
    let child = parent.open_directory(&context, "child").unwrap();
    let physical_parent = match fs::rename(&selected, &moved) {
        Ok(()) => {
            assert!(!selected.exists());
            &moved
        }
        Err(error) => {
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            assert!(selected.is_dir());
            assert!(!moved.exists());
            &selected
        }
    };
    assert_eq!(fixture.project.metrics().open, 2);
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 1);
    let mut visited = 0;
    child
        .visit_children(&context, 1, |name| {
            assert_eq!(name, "payload");
            visited += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(visited, 1);
    let mut file = child.open_child(&context, "payload").unwrap();
    let mut bytes = String::new();
    file.read_to_string(&mut bytes).unwrap();
    assert_eq!(bytes, "retained");
    assert_eq!(
        fs::read(physical_parent.join("child/payload")).unwrap(),
        b"retained"
    );
    drop(file);
    drop(child);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn child_directory_open_refuses_files_foreign_contexts_and_unadmitted_descriptors() {
    let fixture = Fixture::new(2);
    fs::create_dir(fixture.root.join("child")).unwrap();
    fs::write(fixture.root.join("file"), b"retained").unwrap();
    let parent = fixture.capture(&fixture.root);
    let context = fixture.project.io_context();
    let foreign = Fixture::new(2);
    assert!(parent
        .open_directory(&foreign.project.io_context(), "child")
        .is_err());
    assert_eq!(foreign.project.metrics().open, 0);
    for name in ["", "file", "missing", "..", "child/", "./child"] {
        assert!(parent.open_directory(&context, name).is_err(), "{name}");
        assert_eq!(fixture.project.metrics().open, 1);
    }
    let file = parent.open_child(&context, "file").unwrap();
    assert!(parent.open_directory(&context, "child").is_err());
    assert_eq!(fixture.project.metrics().open, 2);
    drop(file);
    let child = parent.open_directory(&context, "child").unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    drop(child);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(fixture.root.join("child"), fixture.root.join("alias")).unwrap();
        assert!(parent.open_directory(&context, "alias").is_err());
        assert_eq!(fixture.project.metrics().open, 1);
    }
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn relative_directory_creation_keeps_captured_parent_after_label_reuse() {
    let fixture = Fixture::new(8);
    let named = fixture.root.join("named");
    let moved = fixture.root.join("moved");
    fs::create_dir(&named).unwrap();
    let source = fixture.capture(&named);
    // No descendant is open: this constructor boundary is legal on Windows.
    fs::rename(&named, &moved).unwrap();
    fs::create_dir(&named).unwrap();
    fs::write(named.join("foreign"), b"unchanged").unwrap();
    let context = fixture.project.io_context();
    source.create_directory_new(&context, "component").unwrap();
    assert!(moved.join("component").is_dir());
    assert!(!named.join("component").exists());
    assert_eq!(fixture.project.metrics().open, 1);
    let child = source.open_directory(&context, "component").unwrap();
    assert_eq!(fixture.project.metrics().open, 2);
    drop(source);
    let file = child
        .open_lock_file(
            &context,
            ".owned.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    drop(file);
    assert!(moved.join("component/.owned.lock").is_file());
    assert!(!named.join("component").exists());
    assert_eq!(fs::read(named.join("foreign")).unwrap(), b"unchanged");
    drop(child);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn exclusive_directory_creation_never_adopts_or_replaces_existing_entries() {
    let fixture = Fixture::new(4);
    fs::create_dir(fixture.root.join("directory")).unwrap();
    fs::write(fixture.root.join("directory/data"), b"retained").unwrap();
    fs::write(fixture.root.join("file"), b"retained file").unwrap();
    let source = fixture.capture(&fixture.root);
    let context = fixture.project.io_context();
    for name in ["directory", "file"] {
        assert_eq!(
            source
                .create_directory_new(&context, name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("directory", fixture.root.join("link")).unwrap();
        assert_eq!(
            source
                .create_directory_new(&context, "link")
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(fs::symlink_metadata(fixture.root.join("link"))
            .unwrap()
            .file_type()
            .is_symlink());
    }
    assert_eq!(
        fs::read(fixture.root.join("directory/data")).unwrap(),
        b"retained"
    );
    assert_eq!(
        fs::read(fixture.root.join("file")).unwrap(),
        b"retained file"
    );
    assert_eq!(fixture.project.metrics().open, 1);
}

#[test]
fn directory_creation_and_later_open_have_separate_admission_and_outcomes() {
    let fixture = Fixture::new(2);
    let source = fixture.capture(&fixture.root);
    let context = fixture.project.io_context();
    let reservation = fixture.project.reserve(1).unwrap();
    let error = source
        .create_directory_new(&context, "component")
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(!fixture.root.join("component").exists());
    drop(reservation);
    source.create_directory_new(&context, "component").unwrap();
    assert_eq!(fixture.project.metrics().open, 1);
    let reservation = fixture.project.reserve(1).unwrap();
    let error = source.open_directory(&context, "component").unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    // A failed subsequent open is not a failed creation or rollback.
    assert!(fixture.root.join("component").is_dir());
    drop(reservation);
    drop(source.open_directory(&context, "component").unwrap());
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn invalid_directory_names_and_foreign_domain_refuse_before_creation() {
    let fixture = Fixture::new(4);
    let foreign = Fixture::new(4);
    let source = fixture.capture(&fixture.root);
    for name in ["", ".", "..", "a/b", "a\\b"] {
        assert_eq!(
            source
                .create_directory_new(&fixture.project.io_context(), name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(source
        .create_directory_new(&foreign.project.io_context(), "component")
        .is_err());
    assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&foreign.root).unwrap().count(), 0);
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(foreign.project.metrics().open, 0);
}

#[test]
fn captured_wal_append_preserves_end_of_file_after_reads_and_name_replacement() {
    let fixture = Fixture::new(8);
    let context = fixture.project.io_context();
    let path = fixture.root.join("wal.1.hawdb");
    let saved = fixture.root.join("saved.hawdb");
    fs::write(&path, b"prefix").unwrap();
    let directory = fixture.capture(&fixture.root);
    let mut file = directory
        .open_wal_append_child(&context, "wal.1.hawdb")
        .unwrap();
    let mut prefix = [0; 6];
    file.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"prefix");
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"-one").unwrap();
    file.sync_all().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"prefix-one");
    fs::rename(&path, &saved).unwrap();
    fs::write(&path, b"foreign").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"-torn").unwrap();
    file.rollback_wal_append(10, b"unused").unwrap();
    directory.sync(&context).unwrap();
    assert_eq!(fs::read(saved).unwrap(), b"prefix-one");
    assert_eq!(fs::read(&path).unwrap(), b"foreign");
    assert!(!file.matches_native_path(&path).unwrap());
    drop((file, directory));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(windows)]
#[test]
fn captured_wal_append_resize_refuses_at_the_original_descriptor_cap_and_retries() {
    let fixture = Fixture::new(3);
    let context = fixture.project.io_context();
    let path = fixture.root.join("wal.1.hawdb");
    fs::write(&path, b"prefix").unwrap();
    let directory = fixture.capture(&fixture.root);
    let mut file = directory
        .open_wal_append_child(&context, "wal.1.hawdb")
        .unwrap();
    let blocker = crate::file_io::File::open(&path).unwrap();
    file.write_all(b"-partial").unwrap();
    assert_eq!(fixture.project.metrics().open, 3);
    assert!(file.rollback_wal_append(6, b"unused").is_err());
    assert_eq!(fs::read(&path).unwrap(), b"prefix-partial");
    assert_eq!(fixture.project.metrics().open, 3);
    drop(blocker);
    file.rollback_wal_append(6, b"unused").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"prefix");
    assert_eq!(fixture.project.metrics().open, 2);
    drop((file, directory));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn captured_file_binding_uses_original_root_and_rejects_replacement() {
    let fixture = Fixture::new(16);
    let root = fixture.root.join("native");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("binding"), b"original").unwrap();
    let directory = fixture.capture(&root);
    let context = fixture.project.io_context();
    let file = directory.open_child(&context, "binding").unwrap();
    assert!(directory
        .matches_child_file(&context, "binding", &file)
        .unwrap());
    fs::rename(root.join("binding"), root.join("retained")).unwrap();
    fs::write(root.join("binding"), b"foreign").unwrap();
    assert!(!directory
        .matches_child_file(&context, "binding", &file)
        .unwrap());
    assert!(directory
        .matches_child_file(&context, "retained", &file)
        .unwrap());
    assert_eq!(
        directory
            .matches_child_file(&context, "missing", &file)
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotFound
    );
    assert!(directory
        .matches_child_file(&context, "../binding", &file)
        .is_err());
}

#[cfg(unix)]
#[test]
fn captured_file_binding_never_follows_symlinks_or_reopens_its_root_label() {
    let fixture = Fixture::new(16);
    let root = fixture.root.join("native");
    let moved = fixture.root.join("moved");
    fs::create_dir(&root).unwrap();
    fs::write(root.join("binding"), b"original").unwrap();
    let directory = fixture.capture(&root);
    let context = fixture.project.io_context();
    let file = directory.open_child(&context, "binding").unwrap();
    std::os::unix::fs::symlink("binding", root.join("alias")).unwrap();
    assert_eq!(
        directory
            .matches_child_file(&context, "alias", &file)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    fs::rename(&root, &moved).unwrap();
    fs::create_dir(&root).unwrap();
    fs::write(root.join("binding"), b"foreign").unwrap();
    assert!(directory
        .matches_child_file(&context, "binding", &file)
        .unwrap());
    assert_eq!(fs::read(root.join("binding")).unwrap(), b"foreign");
}

#[cfg(unix)]
#[test]
fn captured_file_binding_of_a_lock_uses_no_additional_descriptor() {
    let fixture = Fixture::new(2);
    let directory = fixture.capture(&fixture.root);
    let context = fixture.project.io_context();
    let file = directory
        .open_lock_file(
            &context,
            ".search_projection.publish.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    file.try_lock().unwrap();
    assert!(directory
        .open_child(&context, ".search_projection.publish.lock")
        .is_err());
    assert!(directory
        .matches_child_file(&context, ".search_projection.publish.lock", &file)
        .unwrap());
    file.unlock().unwrap();
}
