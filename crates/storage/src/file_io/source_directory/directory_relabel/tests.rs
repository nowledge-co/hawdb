// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::file_io as fs;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "hawdb-captured-relabel-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("source")).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn assert_original_anchor_busy(
    directory: &SourceDirectory,
    context: &FileOpenContext,
    original: &crate::file_io::PublicationLockGuard,
) {
    let original: &File = std::borrow::Borrow::borrow(original);
    let expected = original
        .with_native(crate::native_file_identity::of_file)
        .unwrap();
    let contender = directory
        .open_lock_file(
            context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadExisting,
        )
        .unwrap();
    assert_eq!(
        contender
            .with_native(crate::native_file_identity::of_file)
            .unwrap(),
        expected
    );
    assert!(
        matches!(
            contender.try_lock_owned(),
            Err(std::fs::TryLockError::WouldBlock)
        ),
        "fresh same-name handle must contend with the original live anchor"
    );
}

#[test]
fn captured_relabel_moves_with_original_child_and_anchor_held() {
    let fixture = Fixture::new();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let anchor = child
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap()
        .try_lock_owned()
        .unwrap();
    assert_original_anchor_busy(&child, &context, &anchor);
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
    assert_original_anchor_busy(&child, &context, &anchor);
    movement.finish(&context).unwrap();
    assert!(movement.durability_completed());
    assert!(movement.move_once(&context).is_err());
    assert!(parent.open_directory(&context, "source").is_err());
    assert!(parent
        .open_directory(&context, "target")
        .unwrap()
        .same_directory(&child));
    assert!(fixture
        .0
        .join("target/.search-source-process.lock")
        .exists());
    let selected = parent.open_directory(&context, "target").unwrap();
    assert!(selected.same_directory(&child));
    assert_original_anchor_busy(&selected, &context, &anchor);
    drop(anchor);
}

fn with_hook<T>(
    boundary: hooks::Boundary,
    callback: impl FnOnce() -> io::Result<()> + 'static,
    operation: impl FnOnce() -> T,
) -> T {
    hooks::HOOK.with(|hook| {
        assert!(hook
            .borrow_mut()
            .replace((boundary, Box::new(callback)))
            .is_none())
    });
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            hooks::HOOK.with(|hook| *hook.borrow_mut() = None);
        }
    }
    let reset = Reset;
    let result = operation();
    drop(reset);
    result
}

#[test]
fn captured_relabel_refuses_every_occupied_target_before_attempt() {
    for directory in [false, true] {
        let fixture = Fixture::new();
        if directory {
            std::fs::create_dir(fixture.0.join("target")).unwrap();
        } else {
            std::fs::write(fixture.0.join("target"), b"foreign").unwrap();
        }
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let child = parent.open_directory(&context, "source").unwrap();
        let mut movement = parent
            .prepare_directory_relabel(&child, "source", "target")
            .unwrap();
        assert_eq!(
            movement.move_once(&context).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(!movement.may_have_effect());
        assert!(fixture.0.join("source").is_dir());
        if !directory {
            assert_eq!(std::fs::read(fixture.0.join("target")).unwrap(), b"foreign");
        }
        assert_eq!(project.metrics().open, 2);
        assert_eq!(project.metrics().reserved, 0);
    }
}

#[test]
fn captured_relabel_target_race_reaches_exclusive_kernel_without_overwrite() {
    for nonempty in [false, true] {
        let fixture = Fixture::new();
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let child = parent.open_directory(&context, "source").unwrap();
        let mut movement = parent
            .prepare_directory_relabel(&child, "source", "target")
            .unwrap();
        let path = fixture.0.join("target");
        let retained = std::rc::Rc::new(std::cell::RefCell::new(None));
        let saved = retained.clone();
        let result = with_hook(
            hooks::Boundary::BeforeNative,
            move || {
                std::fs::create_dir(&path)?;
                // This external racing actor retains the actual newborn directory
                // object. An open handle creates no entry/anchor inside it, so the
                // empty branch exercises the real replaceable birth window.
                let foreign = platform::open_root(&path)?;
                if nonempty {
                    std::fs::write(path.join("evidence"), b"foreign")?;
                } else {
                    assert!(std::fs::read_dir(&path)?.next().is_none());
                }
                *saved.borrow_mut() = Some(foreign);
                Ok(())
            },
            || movement.move_once(&context),
        );
        let saved = retained.borrow();
        let foreign = saved.as_ref().unwrap();
        let original_target = crate::native_file_identity::of_file(foreign).unwrap();
        let actual_target = platform::open_root(&fixture.0.join("target")).unwrap();
        // Check terminal physical identity BEFORE the returned error. Removing
        // noReplace on Unix must really overwrite the empty foreign directory
        // and fail THIS oracle, not merely produce a different errno.
        assert_eq!(
            crate::native_file_identity::of_file(&actual_target).unwrap(),
            original_target,
            "late target object must survive the native call unchanged (empty={})",
            !nonempty
        );
        assert!(parent
            .open_directory(&context, "source")
            .unwrap()
            .same_directory(&child));
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        assert!(movement.move_attempted());
        assert!(!movement.native_move_completed());
        if nonempty {
            assert_eq!(
                std::fs::read(fixture.0.join("target/evidence")).unwrap(),
                b"foreign"
            );
        }
        assert!(movement.move_once(&context).is_err());
        assert!(movement.finish(&context).is_err());
    }
}

#[test]
fn captured_relabel_foreign_source_and_wrong_parent_preserve_original() {
    for wrong_parent in [false, true] {
        let fixture = Fixture::new();
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 8, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let child = parent.open_directory(&context, "source").unwrap();
        let other;
        let chosen = if wrong_parent {
            std::fs::create_dir(fixture.0.join("other")).unwrap();
            std::fs::create_dir(fixture.0.join("other/source")).unwrap();
            other = parent.open_directory(&context, "other").unwrap();
            &other
        } else {
            std::fs::rename(fixture.0.join("source"), fixture.0.join("original")).unwrap();
            std::fs::create_dir(fixture.0.join("source")).unwrap();
            &parent
        };
        let mut movement = chosen
            .prepare_directory_relabel(&child, "source", "target")
            .unwrap();
        assert_eq!(
            movement.move_once(&context).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!movement.move_attempted());
        assert!(!fixture.0.join("target").exists());
        assert!(!fixture.0.join("other/target").exists());
        assert!(child
            .open_lock_file(
                &context,
                ".search-source-process.lock",
                SourceDirectoryLockMode::ReadWriteCreate
            )
            .is_ok());
    }
}

#[cfg(unix)]
#[test]
fn captured_relabel_symlinks_are_neither_followed_nor_replaced() {
    for source_link in [false, true] {
        let fixture = Fixture::new();
        std::fs::rename(fixture.0.join("source"), fixture.0.join("original")).unwrap();
        if source_link {
            std::os::unix::fs::symlink("original", fixture.0.join("source")).unwrap();
        } else {
            std::fs::create_dir(fixture.0.join("source")).unwrap();
            std::os::unix::fs::symlink("absent", fixture.0.join("target")).unwrap();
        }
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 8, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let child = parent
            .open_directory(&context, if source_link { "original" } else { "source" })
            .unwrap();
        let mut movement = parent
            .prepare_directory_relabel(&child, "source", "target")
            .unwrap();
        assert!(movement.move_once(&context).is_err());
        assert!(!movement.move_attempted());
        assert!(fixture.0.join("original").is_dir());
        let link = fixture
            .0
            .join(if source_link { "source" } else { "target" });
        assert!(std::fs::symlink_metadata(link)
            .unwrap()
            .file_type()
            .is_symlink());
    }
}

#[cfg(any(target_os = "macos", windows))]
#[test]
fn captured_relabel_case_variant_occupation_is_refused_with_anchor_held() {
    let fixture = Fixture::new();
    // A case-sensitive volume has two distinct spellings; create an occupied
    // target there too. The assertion always checks occupied-target rejection.
    if !fixture.0.join("SOURCE").exists() {
        std::fs::create_dir(fixture.0.join("SOURCE")).unwrap();
    }
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let target = SourceDirectory::capture(&fixture.0.join("SOURCE"), &context).unwrap();
    eprintln!(
        "case-variant target is actual original native alias: {}",
        target.same_directory(&child)
    );
    drop(target);
    let anchor = child
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap()
        .try_lock_owned()
        .unwrap();
    assert_original_anchor_busy(&child, &context, &anchor);
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "SOURCE")
        .unwrap();
    assert_eq!(
        movement.move_once(&context).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!movement.move_attempted());
    assert!(fixture.0.join("source").is_dir());
    let retained = parent.open_directory(&context, "source").unwrap();
    assert!(retained.same_directory(&child));
    assert_original_anchor_busy(&retained, &context, &anchor);
    drop(anchor);
}

#[test]
fn captured_relabel_descriptor_refusals_retry_same_owner_and_peak_one_helper() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    let blocker = parent.duplicate(&context).unwrap();
    assert!(
        hawdb_core::error::file_descriptor_error(&movement.move_once(&context).unwrap_err())
            .is_some()
    );
    assert!(!movement.move_attempted());
    drop(blocker);
    movement.move_once(&context).unwrap();
    let blocker = parent.duplicate(&context).unwrap();
    assert!(
        hawdb_core::error::file_descriptor_error(&movement.finish(&context).unwrap_err()).is_some()
    );
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
    drop(blocker);
    movement.finish(&context).unwrap();
    assert!(movement.durability_completed());
    assert_eq!(project.metrics().high_water, 3);
    assert_eq!(project.metrics().reserved, 0);
    drop(movement);
    drop(child);
    drop(parent);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn captured_relabel_lost_native_reply_retains_uncertainty_and_original_owners() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = Arc::new(SourceDirectory::capture(&fixture.0, &context).unwrap());
    let child = Arc::new(parent.open_directory(&context, "source").unwrap());
    let original_identity = child.identity();
    let mut movement =
        SourceDirectory::prepare_shared_directory_relabel(parent, child, "source", "target")
            .unwrap();
    let error = with_hook(
        hooks::Boundary::NativeReply,
        || {
            Err(io::Error::other(
                "injected lost native reply after actual syscall",
            ))
        },
        || movement.move_once(&context),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(movement.move_attempted());
    assert!(!movement.native_move_completed());
    assert!(movement.may_have_effect());
    assert!(movement.move_once(&context).is_err());
    assert!(movement.finish(&context).is_err());
    assert_eq!(movement.child.identity(), original_identity);
    assert_eq!(project.metrics().open, 2);
    assert!(!fixture.0.join("source").exists());
    assert!(fixture.0.join("target").is_dir());
    movement
        .child
        .create_directory_new(&context, "live-original")
        .unwrap();
    assert!(fixture.0.join("target/live-original").is_dir());
    drop(movement);
    assert_eq!(project.metrics().open, 0);
    assert!(fixture.0.join("target/live-original").is_dir());
}

#[test]
fn captured_relabel_observer_error_and_unwind_retain_definite_move() {
    for unwind in [false, true] {
        let fixture = Fixture::new();
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
        let context = project.io_context();
        let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let child = parent.open_directory(&context, "source").unwrap();
        let mut movement = parent
            .prepare_directory_relabel(&child, "source", "target")
            .unwrap();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_hook(
                hooks::Boundary::AfterNative,
                move || {
                    if unwind {
                        panic!("injected observer unwind after definite move");
                    }
                    Err(io::Error::other(
                        "injected observer error after definite move",
                    ))
                },
                || movement.move_once(&context),
            )
        }));
        if unwind {
            assert!(outcome.is_err());
        } else {
            assert!(outcome.unwrap().is_err());
        }
        assert!(movement.native_move_completed());
        assert!(movement.move_attempted());
        assert!(!movement.durability_completed());
        assert!(movement.move_once(&context).is_err());
        movement.finish(&context).unwrap();
        assert!(movement.durability_completed());
    }
}

#[test]
fn captured_relabel_finish_fresh_binding_fault_stays_sticky_after_restoration() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 4, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    std::fs::rename(fixture.0.join("target"), fixture.0.join("away")).unwrap();
    assert!(movement.finish(&context).is_err());
    assert!(movement.native_move_completed());
    std::fs::rename(fixture.0.join("away"), fixture.0.join("target")).unwrap();
    assert!(movement.finish(&context).is_err());
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
}

#[test]
fn captured_relabel_parent_sync_error_remains_retryable_without_new_move() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    assert!(with_hook(
        hooks::Boundary::BeforeSync,
        || Err(io::Error::other("injected parent barrier failure")),
        || movement.finish(&context)
    )
    .is_err());
    assert!(!movement.durability_completed());
    assert!(movement.native_move_completed());
    movement.finish(&context).unwrap();
    assert!(movement.durability_completed());
    assert!(!fixture.0.join("source").exists());
}

#[test]
fn captured_relabel_rejects_foreign_context_and_bad_names_without_io() {
    let fixture = Fixture::new();
    let foreign_fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let foreign =
        ProjectFileDescriptors::acquire_with_capacity_limit(&foreign_fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    for bad in [
        "",
        ".",
        "..",
        "../source",
        "a/b",
        "a\\b",
        "C:",
        "ü",
        "nul\0",
    ] {
        assert!(parent
            .prepare_directory_relabel(&child, bad, "target")
            .is_err());
        assert!(parent
            .prepare_directory_relabel(&child, "source", bad)
            .is_err());
    }
    assert!(parent
        .prepare_directory_relabel(&child, "source", "source")
        .is_err());
    assert!(parent
        .prepare_directory_relabel(&parent, "source", "target")
        .is_err());
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    assert!(movement.finish(&context).is_err());
    assert!(movement.move_once(&foreign.io_context()).is_err());
    assert!(!movement.move_attempted());
    assert_eq!(project.metrics().open, 2);
    assert_eq!(foreign.metrics().open, 0);
    movement.move_once(&context).unwrap();
    movement.finish(&context).unwrap();
}

#[test]
fn captured_relabel_never_resolves_reused_parent_path() {
    let fixture = Fixture::new();
    std::fs::create_dir(fixture.0.join("named")).unwrap();
    std::fs::rename(fixture.0.join("source"), fixture.0.join("named/source")).unwrap();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 6, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0.join("named"), &context).unwrap();
    // Change the diagnostic label before opening any descendant. This remains
    // a held-parent authority test on platforms restricting ancestor moves.
    std::fs::rename(fixture.0.join("named"), fixture.0.join("moved")).unwrap();
    std::fs::create_dir(fixture.0.join("named")).unwrap();
    std::fs::create_dir(fixture.0.join("named/source")).unwrap();
    std::fs::write(fixture.0.join("named/source/foreign"), b"foreign").unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    movement.finish(&context).unwrap();
    assert!(fixture.0.join("moved/target").is_dir());
    assert!(!fixture.0.join("moved/source").exists());
    assert!(!fixture.0.join("named/target").exists());
    assert_eq!(
        std::fs::read(fixture.0.join("named/source/foreign")).unwrap(),
        b"foreign"
    );
}

#[test]
fn captured_relabel_recorder_keeps_original_child_inode_and_atomic_crash_images() {
    use crate::power_loss::image::ImageLimits;
    use crate::power_loss::PowerLossModel;
    use std::io::{Read, Write};
    let fixture = Fixture::new();
    std::fs::remove_dir(fixture.0.join("source")).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    parent.create_directory_new(&context, "source").unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut payload = child
        .create_control_candidate(&context, "payload.tmp")
        .unwrap();
    payload.write_all(b"original inode payload").unwrap();
    payload.seal().unwrap();
    payload.replace_head(&context, "payload").unwrap();
    drop(payload);
    parent.sync(&context).unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    let selected = model.capture().unwrap();
    let complete = selected.persist_all_plan();
    assert_eq!(
        complete.persistence.len(),
        1,
        "same-parent relabel must be one atomic persistence batch"
    );
    let old = selected.crash(&Default::default()).unwrap();
    let new = selected.crash(&complete).unwrap();
    assert_eq!(
        old.bytes(Path::new("source/payload")).unwrap(),
        b"original inode payload"
    );
    assert!(old.bytes(Path::new("target/payload")).is_none());
    assert!(new.bytes(Path::new("source/payload")).is_none());
    assert_eq!(
        new.bytes(Path::new("target/payload")).unwrap(),
        b"original inode payload"
    );
    movement.finish(&context).unwrap();
    let mut original = Vec::new();
    child
        .open_child(&context, "payload")
        .unwrap()
        .read_to_end(&mut original)
        .unwrap();
    assert_eq!(original, b"original inode payload");
    let mut later = child
        .create_control_candidate(&context, "later.tmp")
        .unwrap();
    later.write_all(b"same original child trace").unwrap();
    later.seal().unwrap();
    later.replace_head(&context, "later").unwrap();
    let durable = model.capture().unwrap().crash(&Default::default()).unwrap();
    assert_eq!(
        durable.bytes(Path::new("target/later")).unwrap(),
        b"same original child trace"
    );
    assert!(durable.bytes(Path::new("source/later")).is_none());
}

#[test]
fn captured_relabel_recorder_ignores_parent_diagnostic_label_replacement() {
    use crate::power_loss::image::ImageLimits;
    use crate::power_loss::PowerLossModel;
    let fixture = Fixture::new();
    std::fs::remove_dir(fixture.0.join("source")).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    let context = project.io_context();
    fs::create_dir(fixture.0.join("named")).unwrap();
    fs::create_dir(fixture.0.join("named/source")).unwrap();
    let parent = SourceDirectory::capture(&fixture.0.join("named"), &context).unwrap();
    fs::rename(fixture.0.join("named"), fixture.0.join("moved")).unwrap();
    fs::create_dir(fixture.0.join("named")).unwrap();
    fs::create_dir(fixture.0.join("named/source")).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let foreign_parent = SourceDirectory::capture(&fixture.0.join("named"), &context).unwrap();
    let foreign_child = foreign_parent.open_directory(&context, "source").unwrap();
    parent.sync(&context).unwrap();
    foreign_parent.sync(&context).unwrap();
    SourceDirectory::capture(&fixture.0, &context)
        .unwrap()
        .sync(&context)
        .unwrap();
    let original_identity = child.identity();
    let foreign_identity = foreign_child.identity();
    assert_ne!(original_identity, foreign_identity);
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    movement.finish(&context).unwrap();
    let image = model.capture().unwrap().crash(&Default::default()).unwrap();
    let paths: Vec<_> = image.directory_paths().collect();
    assert!(paths.contains(&Path::new("moved/target")));
    assert!(paths.contains(&Path::new("named/source")));
    assert!(!paths.contains(&Path::new("named/target")));
    assert!(foreign_parent
        .open_directory(&context, "source")
        .unwrap()
        .same_directory(&foreign_child));
    assert!(parent
        .open_directory(&context, "target")
        .unwrap()
        .same_directory(&child));
}

#[test]
fn captured_relabel_image_requires_original_inode_and_exclusive_target() {
    use crate::power_loss::image::{ImageEngine, ImageLimits};
    let mut image = ImageEngine::new(ImageLimits::default()).unwrap();
    let parent = image.create_directory(Path::new("parent")).unwrap();
    let original = image
        .create_directory_at(parent, Path::new("source"))
        .unwrap();
    let foreign = image
        .create_directory_at(parent, Path::new("foreign"))
        .unwrap();
    assert!(image
        .rename_directory_new_at(parent, foreign, Path::new("source"), Path::new("target"))
        .is_err());
    assert_eq!(
        image.child_inode(parent, Path::new("source")).unwrap(),
        original
    );
    image.create_file_at(parent, Path::new("target")).unwrap();
    assert_eq!(
        image
            .rename_directory_new_at(parent, original, Path::new("source"), Path::new("target"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    image
        .rename(Path::new("parent"), Path::new("moved"))
        .unwrap();
    let decoy = image.create_directory(Path::new("parent")).unwrap();
    let decoy_child = image
        .create_directory_at(decoy, Path::new("source"))
        .unwrap();
    image
        .rename_directory_new_at(parent, original, Path::new("source"), Path::new("selected"))
        .unwrap();
    assert_eq!(
        image.directory_inode(Path::new("moved/selected")).unwrap(),
        original
    );
    assert_eq!(
        image.directory_inode(Path::new("parent/source")).unwrap(),
        decoy_child
    );
    assert_eq!(
        image.child_inode(parent, Path::new("foreign")).unwrap(),
        foreign
    );
}

#[derive(Debug)]
struct HostMemory {
    used: Arc<std::sync::atomic::AtomicU64>,
    capacity: u64,
}
#[derive(Debug)]
struct HostPermit {
    used: Arc<std::sync::atomic::AtomicU64>,
    bytes: u64,
}
impl hawdb_core::RuntimeMemoryPermit for HostPermit {
    fn bytes(&self) -> u64 {
        self.bytes
    }
}
impl Drop for HostPermit {
    fn drop(&mut self) {
        assert!(
            self.used
                .fetch_sub(self.bytes, std::sync::atomic::Ordering::SeqCst)
                >= self.bytes
        );
    }
}
impl hawdb_core::RuntimeMemoryController for HostMemory {
    fn reserve(
        &self,
        bytes: u64,
        ceiling: u64,
    ) -> Result<Box<dyn hawdb_core::RuntimeMemoryPermit>, hawdb_core::RuntimeMemoryError> {
        use std::sync::atomic::Ordering;
        let capacity = self.capacity.min(ceiling);
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes).filter(|next| *next <= capacity)
            })
            .map_err(|used| hawdb_core::RuntimeMemoryError::AdmissionDenied {
                requested_bytes: bytes,
                available_bytes: capacity.saturating_sub(used),
                retryable: true,
            })?;
        Ok(Box::new(HostPermit {
            used: self.used.clone(),
            bytes,
        }))
    }
}

#[test]
fn captured_relabel_external_memory_permit_stays_owned_through_failure() {
    for unknown in [false, true] {
        let fixture = Fixture::new();
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
        let context = project.io_context();
        let parent = Arc::new(SourceDirectory::capture(&fixture.0, &context).unwrap());
        let child = Arc::new(parent.open_directory(&context, "source").unwrap());
        let bound = parent.directory_relabel_metadata_bound(6, 6).unwrap() as u64;
        assert!(bound >= std::mem::size_of::<CapturedDirectoryRelabel<'_>>() as u64);
        let used = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let task = hawdb_core::RuntimeTaskContext::default().with_memory_controller(Arc::new(
            HostMemory {
                used: used.clone(),
                capacity: bound,
            },
        ));
        // The host admits before constructing; neither FD context nor estimate
        // creates this actual refundable charge. Drop order retains funding
        // until the candidate (and all its original native custody) is gone.
        struct Funded {
            candidate: CapturedDirectoryRelabel<'static>,
            funding: Box<dyn hawdb_core::RuntimeMemoryPermit>,
        }
        let funding = task.reserve_working_memory(bound).unwrap().unwrap();
        let mut owned = Funded {
            candidate: SourceDirectory::prepare_shared_directory_relabel(
                parent, child, "source", "target",
            )
            .unwrap(),
            funding,
        };
        let boundary = if unknown {
            hooks::Boundary::NativeReply
        } else {
            hooks::Boundary::AfterNative
        };
        assert!(with_hook(
            boundary,
            || Err(io::Error::other("injected response loss")),
            || owned.candidate.move_once(&context)
        )
        .is_err());
        assert_eq!(owned.candidate.native_move_completed(), !unknown);
        assert_eq!(owned.funding.bytes(), bound);
        assert_eq!(used.load(std::sync::atomic::Ordering::SeqCst), bound);
        assert!(task.reserve_working_memory(1).is_err());
        assert_eq!(project.metrics().open, 2);
        drop(owned);
        assert_eq!(used.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(project.metrics().open, 0);
        assert!(fixture.0.join("target").is_dir());
        assert_eq!(
            task.reserve_working_memory(bound).unwrap().unwrap().bytes(),
            bound
        );
    }
}

#[test]
fn captured_relabel_recorder_after_error_cannot_erase_known_move_or_commit() {
    use crate::power_loss::image::ImageLimits;
    use crate::power_loss::PowerLossModel;
    let fixture = Fixture::new();
    std::fs::remove_dir(fixture.0.join("source")).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    parent.create_directory_new(&context, "source").unwrap();
    parent.sync(&context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let core = crate::power_loss::for_path(&context, &fixture.0)
        .unwrap()
        .unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    let error = with_hook(
        hooks::Boundary::NativeReply,
        move || {
            // The actual syscall already succeeded. Poisoning this real mutex
            // makes ModelCore's fallible After observer fail after image commit.
            core.poison_namespace_observer_for_test();
            Ok(())
        },
        || movement.move_once(&context),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "power-loss capture was interrupted while recording IO"
    );
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
    assert!(movement.move_once(&context).is_err());
    assert!(fixture.0.join("target").is_dir());
    assert!(!fixture.0.join("source").exists());
    let committed = model.capture().unwrap();
    let image = committed.crash(&committed.persist_all_plan()).unwrap();
    let paths: Vec<_> = image.directory_paths().collect();
    assert!(paths.contains(&Path::new("target")));
    assert!(!paths.contains(&Path::new("source")));
}

#[cfg(unix)]
#[test]
fn captured_relabel_finish_observed_symlink_fault_stays_sticky() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    std::fs::rename(fixture.0.join("target"), fixture.0.join("away")).unwrap();
    std::os::unix::fs::symlink("away", fixture.0.join("target")).unwrap();
    assert_eq!(
        movement.finish(&context).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    std::fs::remove_file(fixture.0.join("target")).unwrap();
    std::fs::rename(fixture.0.join("away"), fixture.0.join("target")).unwrap();
    assert_eq!(
        movement.finish(&context).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
}

#[test]
fn captured_relabel_native_reply_unwind_preserves_attempt_and_originals() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| with_hook(
            hooks::Boundary::NativeReply,
            || panic!("injected lost reply unwind after actual native move"),
            || movement.move_once(&context)
        )))
        .is_err()
    );
    assert!(movement.move_attempted());
    assert!(!movement.native_move_completed());
    assert!(movement.move_once(&context).is_err());
    assert!(fixture.0.join("target").is_dir());
    assert_eq!(project.metrics().open, 2);
    assert_eq!(project.metrics().reserved, 0);
}

#[cfg(any(unix, windows))]
fn native_handles() -> usize {
    #[cfg(unix)]
    {
        std::fs::read_dir("/dev/fd").unwrap().count()
    }
    #[cfg(windows)]
    {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessHandleCount(process: *mut std::ffi::c_void, count: *mut u32) -> i32;
        }
        let mut count = 0;
        // SAFETY: current process pseudo-handle is live; count is writable and
        // the synchronous API never transfers ownership of that pseudo-handle.
        assert_ne!(
            unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) },
            0
        );
        count as usize
    }
}

#[cfg(any(unix, windows))]
#[test]
fn captured_relabel_native_handle_measurement_is_isolated_and_retains_originals() {
    const KEY: &str = "HAWDB_RELABEL_NATIVE_FD_PROBE";
    if std::env::var_os(KEY).is_none() {
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "file_io::source_directory::directory_relabel::tests::captured_relabel_native_handle_measurement_is_isolated_and_retains_originals", "--test-threads=1", "--nocapture"])
            .env(KEY, "1").output().unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    // Initialize observation machinery before fixing the isolated baseline.
    native_handles();
    let before = native_handles();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    assert_eq!(native_handles(), before + 2);
    let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let saved = observed.clone();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    with_hook(
        hooks::Boundary::BeforeNative,
        move || {
            saved.store(native_handles(), std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || movement.move_once(&context),
    )
    .unwrap();
    assert_eq!(
        observed.load(std::sync::atomic::Ordering::SeqCst),
        before + 3
    );
    assert_eq!(native_handles(), before + 2);
    movement.finish(&context).unwrap();
    assert_eq!(native_handles(), before + 2);
    assert_eq!(project.metrics().high_water, 3);
    drop(movement);
    drop(child);
    drop(parent);
    assert_eq!(native_handles(), before);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
}

#[cfg(windows)]
#[test]
fn captured_relabel_windows_default_native_alias_refuses_with_original_anchor() {
    let fixture = Fixture::new();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    // This test qualifies an actual default native alias, separately from the
    // generic occupied-case-variant fixture. A distinct-case setup is not PASS.
    let alias = SourceDirectory::capture(&fixture.0.join("SOURCE"), &context).unwrap();
    assert!(alias.same_directory(&child));
    drop(alias);
    let anchor = child
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap()
        .try_lock_owned()
        .unwrap();
    assert_original_anchor_busy(&child, &context, &anchor);
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "SOURCE")
        .unwrap();
    assert_eq!(
        movement.move_once(&context).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!movement.move_attempted());
    assert!(parent
        .open_directory(&context, "source")
        .unwrap()
        .same_directory(&child));
    assert!(!movement.durability_completed());
    let retained = parent.open_directory(&context, "source").unwrap();
    assert!(retained.same_directory(&child));
    assert_original_anchor_busy(&retained, &context, &anchor);
    drop(anchor);
}

#[cfg(windows)]
#[test]
fn captured_relabel_windows_case_sensitive_distinct_target_and_occupation() {
    let fixture = Fixture::new();
    std::fs::remove_dir(fixture.0.join("source")).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    {
        let _permit = context.acquire(DescriptorKind::Transient).unwrap();
        // Native API updates only this empty fixture directory. Unsupported
        // filesystems/version/permissions produce explicit setup failure.
        parent
            .root
            .with_native(platform::enable_case_sensitive_for_test)
            .unwrap();
    }
    parent.create_directory_new(&context, "source").unwrap();
    parent.create_directory_new(&context, "SOURCE").unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let distinct = parent.open_directory(&context, "SOURCE").unwrap();
    assert!(!distinct.same_directory(&child));
    drop(distinct);
    let anchor = child
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap()
        .try_lock_owned()
        .unwrap();
    assert_original_anchor_busy(&child, &context, &anchor);
    let mut occupied = parent
        .prepare_directory_relabel(&child, "source", "SOURCE")
        .unwrap();
    assert_eq!(
        occupied.move_once(&context).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!occupied.move_attempted());
    std::fs::remove_dir(fixture.0.join("SOURCE")).unwrap();
    assert_original_anchor_busy(&child, &context, &anchor);
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "SOURCE")
        .unwrap();
    movement.move_once(&context).unwrap();
    movement.finish(&context).unwrap();
    assert!(movement.durability_completed());
    assert!(parent.open_directory(&context, "source").is_err());
    assert!(parent
        .open_directory(&context, "SOURCE")
        .unwrap()
        .same_directory(&child));
    assert!(fixture
        .0
        .join("SOURCE/.search-source-process.lock")
        .exists());
    let selected = parent.open_directory(&context, "SOURCE").unwrap();
    assert_original_anchor_busy(&selected, &context, &anchor);
    drop(anchor);
}

#[test]
fn captured_relabel_historical_barrier_is_not_revived_current_binding_authority() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 3, true).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    movement.finish(&context).unwrap();
    assert!(movement.durability_completed());
    std::fs::rename(fixture.0.join("target"), fixture.0.join("away")).unwrap();
    assert_eq!(
        movement.finish(&context).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(movement.native_move_completed());
    assert!(
        movement.durability_completed(),
        "prior successful barrier evidence is historical and monotone"
    );
    std::fs::rename(fixture.0.join("away"), fixture.0.join("target")).unwrap();
    assert_eq!(
        movement.finish(&context).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(movement.native_move_completed());
    assert!(movement.durability_completed());
    assert!(movement.move_once(&context).is_err());
}

#[test]
fn captured_relabel_actual_parent_sync_error_cannot_acknowledge_durability() {
    use crate::power_loss::image::ImageLimits;
    use crate::power_loss::PowerLossModel;
    let fixture = Fixture::new();
    std::fs::remove_dir(fixture.0.join("source")).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.0, 16, true).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    let context = project.io_context();
    let parent = SourceDirectory::capture(&fixture.0, &context).unwrap();
    parent.create_directory_new(&context, "source").unwrap();
    parent.sync(&context).unwrap();
    let child = parent.open_directory(&context, "source").unwrap();
    let mut movement = parent
        .prepare_directory_relabel(&child, "source", "target")
        .unwrap();
    movement.move_once(&context).unwrap();
    let core = crate::power_loss::for_path(&context, &fixture.0)
        .unwrap()
        .unwrap();
    let error = with_hook(
        hooks::Boundary::BeforeSync,
        move || {
            core.poison_namespace_observer_for_test();
            // Let the REAL production parent.sync(context) run and return its
            // actual recorder error. Returning Err here would bypass that call.
            Ok(())
        },
        || movement.finish(&context),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "power-loss capture was interrupted while recording IO"
    );
    assert!(movement.native_move_completed());
    assert!(!movement.durability_completed());
    assert!(movement.move_once(&context).is_err());
    assert!(fixture.0.join("target").is_dir());
    assert!(!fixture.0.join("source").exists());
    let selected = model.capture().unwrap();
    let durable = selected.crash(&Default::default()).unwrap();
    let paths: Vec<_> = durable.directory_paths().collect();
    assert!(
        paths.contains(&Path::new("source")),
        "failed actual sync cannot persist the pending rename batch"
    );
    assert!(!paths.contains(&Path::new("target")));
    // This is a production SourceDirectory::sync error, not OS fsync EIO.
    // Recoverable FD/sync retries have their separate non-poisoned guards.
}
