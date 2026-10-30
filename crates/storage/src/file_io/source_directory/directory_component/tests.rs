// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use std::fs;
use std::path::PathBuf;

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
}

impl Fixture {
    fn new(limit: usize) -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-native-directory-component-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project =
            ProjectFileDescriptors::acquire_with_capacity_limit(&root, limit, true).unwrap();
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

struct RestoreCurrentDirectory(PathBuf);
impl Drop for RestoreCurrentDirectory {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.0).unwrap();
    }
}

#[test]
fn native_directory_component_preserves_spaces_unicode_and_actual_child_identity() {
    let fixture = Fixture::new(8);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let name = OsStr::new("configured 知识 root");
    assert!(parent.open_directory_component(&context, name).is_err());
    assert!(!fixture.root.join(name).exists());
    // This is a separate directory-name capability, not artifact validation.
    assert!(parent
        .prepare_directory_candidate(name.to_str().unwrap())
        .is_err());
    assert!(parent
        .open_directory(&context, name.to_str().unwrap())
        .is_err());
    let mut candidate = parent.prepare_directory_component_candidate(name).unwrap();
    assert_eq!(candidate.name(), name);
    assert!(candidate.creation_unattempted());
    assert!(!candidate.may_own_entry());
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    assert!(candidate.creation_completed());
    assert!(candidate.may_own_entry());
    let child = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    let duplicate = child.duplicate(&context).unwrap();
    let independently_captured =
        SourceDirectory::capture(&fixture.root.join(name), &context).unwrap();
    assert!(child.same_directory(&independently_captured));
    assert!(duplicate.same_directory(&child));
    assert!(child.matches_namespace(&fixture.root.join(name)).unwrap());
    drop((independently_captured, child, parent, candidate));
    assert!(duplicate
        .matches_namespace(&fixture.root.join(name))
        .unwrap());
    assert_eq!(fixture.project.metrics().open, 1);
    drop(duplicate);
    assert!(
        fixture.root.join(name).is_dir(),
        "Drop never cleans a named entry"
    );
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn native_directory_component_rejects_invalid_spelling_before_effects() {
    let fixture = Fixture::new(8);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    for name in [
        "",
        ".",
        "..",
        "/",
        "a/",
        "./a",
        "a/.",
        "a/b",
        "../outside",
        "a\0b",
    ] {
        let name = OsStr::new(name);
        assert_eq!(
            parent
                .prepare_directory_component_candidate(name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            parent
                .open_directory_component(&context, name)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 0);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[cfg(all(unix, not(target_vendor = "apple")))]
#[test]
fn native_directory_component_unix_units_are_exact_bytes_not_unicode_or_windows_rules() {
    use std::os::unix::ffi::OsStrExt;
    let fixture = Fixture::new(8);
    let parent = fixture.parent();
    let exact = [b'x'; 255];
    assert!(parent
        .prepare_directory_component_candidate(OsStr::from_bytes(&exact))
        .is_ok());
    let too_long = [b'x'; 256];
    assert_eq!(
        parent
            .prepare_directory_component_candidate(OsStr::from_bytes(&too_long))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let non_utf8 = OsStr::from_bytes(b"native-\xff-name");
    let candidate = parent
        .prepare_directory_component_candidate(non_utf8)
        .unwrap();
    assert_eq!(candidate.name().as_bytes(), non_utf8.as_bytes());
    for literal in ["literal\\name", "literal:name"] {
        let mut candidate = parent
            .prepare_directory_component_candidate(OsStr::new(literal))
            .unwrap();
        parent
            .create_directory_component_candidate(&fixture.project.io_context(), &mut candidate)
            .unwrap();
        parent
            .open_created_directory_component(&fixture.project.io_context(), &candidate)
            .unwrap();
        assert!(fixture.root.join(literal).is_dir());
    }
}

#[cfg(windows)]
#[test]
fn native_directory_component_windows_units_preserve_unicode_without_stream_or_namespace_escape() {
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    let fixture = Fixture::new(8);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    for name in [
        "C:child",
        "C:\\child",
        "\\child",
        "\\??\\child",
        "a\\b",
        "a:b",
        "a\u{1}b",
        "a?b",
        "a*b",
        "a|b",
        "a<b",
        "a>b",
        "a\"b",
    ] {
        assert_eq!(
            parent
                .prepare_directory_component_candidate(OsStr::new(name))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
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
    assert!(parent
        .open_directory_component(&context, OsStr::new(&name))
        .unwrap()
        .same_directory(&child));
    assert!(parent
        .prepare_directory_component_candidate(OsStr::new(&"知".repeat(255)))
        .is_ok());
    assert_eq!(
        parent
            .prepare_directory_component_candidate(OsStr::new(&"知".repeat(256)))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let unpaired = OsString::from_wide(&[u16::from(b'n'), 0xd800]);
    let preserved = parent
        .prepare_directory_component_candidate(&unpaired)
        .unwrap();
    assert_eq!(
        preserved.name().encode_wide().collect::<Vec<_>>(),
        vec![u16::from(b'n'), 0xd800]
    );
}

#[test]
fn native_directory_component_retains_the_actual_parent_after_label_replacement() {
    const CASE: &str = "file_io::source_directory::directory_component::tests::native_directory_component_retains_the_actual_parent_after_label_replacement";
    const CHILD: &str = "HAWDB_NATIVE_DIRECTORY_COMPONENT_CHILD";
    if std::env::var(CHILD).as_deref() != Ok(CASE) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CASE, "--nocapture", "--test-threads=1"])
            .env(CHILD, CASE)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated parent-binding guard failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
            "exact child guard was not executed"
        );
        return;
    }
    let fixture = Fixture::new(12);
    let selected = fixture.root.join("selected");
    let moved = fixture.root.join("moved");
    let name = OsStr::new("configured 知识");
    fs::create_dir_all(selected.join(name)).unwrap();
    fs::write(selected.join(name).join("sentinel"), b"original").unwrap();
    let context = fixture.project.io_context();
    let parent = SourceDirectory::capture(&selected, &context).unwrap();
    fs::rename(&selected, &moved).unwrap();
    fs::create_dir_all(selected.join(name)).unwrap();
    fs::write(selected.join(name).join("sentinel"), b"foreign").unwrap();
    // This runs only in the exact-selector subprocess. A broken relative-open
    // implementation consulting CWD finds the real foreign child, not a setup
    // NotFound; the physical identity assertion detects the wrong admission.
    let old_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(&selected).unwrap();
    let _restore = RestoreCurrentDirectory(old_cwd);
    let child = parent.open_directory_component(&context, name).unwrap();
    assert!(
        child.matches_namespace(&moved.join(name)).unwrap(),
        "captured child must belong to the original parent"
    );
    assert!(!child.matches_namespace(&selected.join(name)).unwrap());
    let created_name = OsStr::new("created 别名");
    let mut candidate = parent
        .prepare_directory_component_candidate(created_name)
        .unwrap();
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    assert!(
        moved.join(created_name).is_dir(),
        "created entry must belong to the actual parent; original={}, foreign={}",
        moved.join(created_name).is_dir(),
        selected.join(created_name).is_dir()
    );
    assert!(
        !selected.join(created_name).exists(),
        "foreign parent is not touched"
    );
    let created = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    assert!(created
        .matches_namespace(&moved.join(created_name))
        .unwrap());
    assert!(!selected.join(created_name).exists());
    assert_eq!(
        fs::read(selected.join(name).join("sentinel")).unwrap(),
        b"foreign"
    );
    assert_eq!(
        fs::read(moved.join(name).join("sentinel")).unwrap(),
        b"original"
    );
    drop((created, child, parent));
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn native_directory_component_refuses_collision_foreign_capture_and_descriptor_exhaustion() {
    let fixture = Fixture::new(2);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let other = parent.duplicate(&context).unwrap();
    let name = OsStr::new("private 目录");
    let mut candidate = parent.prepare_directory_component_candidate(name).unwrap();
    let error = parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(candidate.creation_unattempted());
    assert!(!fixture.root.join(name).exists());
    drop(other);
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    let blocker = parent.duplicate(&context).unwrap();
    let error = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap_err();
    assert!(hawdb_core::error::file_descriptor_error(&error).is_some());
    assert!(candidate.creation_completed());
    drop(blocker);
    let child = parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    drop(child);
    assert_eq!(
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let mut collision = parent.prepare_directory_component_candidate(name).unwrap();
    assert_eq!(
        parent
            .create_directory_component_candidate(&context, &mut collision)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!collision.may_own_entry());
    assert!(!collision.creation_unattempted());
    assert!(parent
        .open_created_directory_component(&context, &collision)
        .is_err());
    assert!(candidate.creation_completed());
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(unix)]
#[test]
fn native_directory_component_open_never_follows_symlinks_or_adopts_files() {
    use std::os::unix::fs::symlink;
    let fixture = Fixture::new(8);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    fs::create_dir(fixture.root.join("real dir")).unwrap();
    symlink("real dir", fixture.root.join("linked dir")).unwrap();
    fs::write(fixture.root.join("ordinary file"), b"retain").unwrap();
    for name in ["linked dir", "ordinary file"] {
        assert!(parent
            .open_directory_component(&context, OsStr::new(name))
            .is_err());
        let mut candidate = parent
            .prepare_directory_component_candidate(OsStr::new(name))
            .unwrap();
        assert_eq!(
            parent
                .create_directory_component_candidate(&context, &mut candidate)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(!candidate.may_own_entry());
    }
    assert_eq!(
        fs::read(fixture.root.join("ordinary file")).unwrap(),
        b"retain"
    );
}

#[test]
fn native_directory_component_post_native_observer_error_retains_completed_creation() {
    use crate::power_loss::{image::ImageLimits, PowerLossModel};
    let fixture = Fixture::new(8);
    let model = PowerLossModel::attach(&fixture.project, ImageLimits::default()).unwrap();
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new("created 错误"))
        .unwrap();
    let name = DirectoryComponent::new(&candidate.name).unwrap();
    let error = parent
        .create_directory_evidence_with(
            &context,
            candidate.capture_id,
            name,
            &mut candidate.state,
            |root, name| {
                platform::create_directory_component(root, name)?;
                assert!(
                    fixture.root.join(name.0).is_dir(),
                    "fault follows actual native mkdir"
                );
                model.poison_observation_for_test();
                Ok(())
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "power-loss capture was interrupted while recording IO"
    );
    assert!(
        fixture.root.join(candidate.name()).is_dir(),
        "native effect is preserved"
    );
    assert!(
        candidate.creation_completed(),
        "post-native observer Err cannot lose Created evidence"
    );
    assert!(candidate.may_own_entry());
    assert!(!candidate.creation_unattempted());
    assert_eq!(
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    drop((candidate, parent, model));
    assert!(
        fixture.root.join("created 错误").is_dir(),
        "fault cleanup never unlinks the actual entry"
    );
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn native_directory_component_wrong_capture_and_context_refuse_without_native_adoption() {
    let fixture = Fixture::new(12);
    let foreign_project = Fixture::new(8);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let duplicate = parent.duplicate(&context).unwrap();
    let other_path = fixture.root.join("other parent");
    fs::create_dir(&other_path).unwrap();
    let other = SourceDirectory::capture(&other_path, &context).unwrap();
    let name = OsStr::new("new captured 目录");
    let mut candidate = parent.prepare_directory_component_candidate(name).unwrap();
    for wrong in [&duplicate, &other] {
        assert_eq!(
            wrong
                .create_directory_component_candidate(&context, &mut candidate)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(candidate.creation_unattempted());
        assert!(!fixture.root.join(name).exists());
        assert!(!other_path.join(name).exists());
    }
    assert!(parent
        .create_directory_component_candidate(&foreign_project.project.io_context(), &mut candidate)
        .is_err());
    assert!(candidate.creation_unattempted());
    assert!(!fixture.root.join(name).exists());
    parent
        .create_directory_component_candidate(&context, &mut candidate)
        .unwrap();
    assert!(parent
        .open_created_directory_component(&foreign_project.project.io_context(), &candidate)
        .is_err());
    assert!(candidate.creation_completed());
    assert!(duplicate
        .open_created_directory_component(&context, &candidate)
        .is_err());
    parent
        .open_created_directory_component(&context, &candidate)
        .unwrap();
    assert!(candidate.creation_completed());
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_long_unicode_uses_native_create_and_lookup_limits() {
    let fixture = Fixture::new(16);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    for name in ["汉".repeat(255), "😀".repeat(127)] {
        // Independent native control proves this filesystem accepts the name.
        fs::create_dir(fixture.root.join(&name)).unwrap();
        let actual = SourceDirectory::capture(&fixture.root.join(&name), &context).unwrap();
        let opened = parent.open_directory_component(&context, OsStr::new(&name));
        fs::remove_dir(fixture.root.join(&name)).unwrap();
        let created = parent
            .prepare_directory_component_candidate(OsStr::new(&name))
            .and_then(|mut candidate| {
                parent.create_directory_component_candidate(&context, &mut candidate)?;
                parent.open_created_directory_component(&context, &candidate)
            });
        assert!(
            opened
                .as_ref()
                .is_ok_and(|child| child.same_directory(&actual)),
            "legal long Unicode existing open must retain native identity: {opened:?}"
        );
        assert!(
            created.is_ok(),
            "legal long Unicode creation must reach native mkdir: {created:?}"
        );
        assert!(created
            .as_ref()
            .unwrap()
            .matches_namespace(&fixture.root.join(&name))
            .unwrap());
        drop((actual, opened, created));
        fs::remove_dir(fixture.root.join(&name)).unwrap();
    }
    for name in ["汉".repeat(256), "😀".repeat(128)] {
        let native_error = fs::create_dir(fixture.root.join(&name)).unwrap_err();
        assert_eq!(native_error.raw_os_error(), Some(libc::ENAMETOOLONG));
        let mut candidate = parent
            .prepare_directory_component_candidate(OsStr::new(&name))
            .unwrap();
        let error = parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap_err();
        assert_eq!(error.raw_os_error(), native_error.raw_os_error());
        assert!(candidate.may_own_entry());
        assert!(!candidate.creation_completed());
        assert!(!fixture.root.join(&name).exists());
    }
    drop(parent);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[cfg(target_vendor = "apple")]
#[test]
fn native_directory_component_darwin_native_alias_has_same_identity_and_refuses_collision() {
    let fixture = Fixture::new(12);
    let parent = fixture.parent();
    let context = fixture.project.io_context();
    let original = "é".repeat(255);
    let alias = "e\u{301}".repeat(255);
    fs::create_dir(fixture.root.join(&original)).unwrap();
    let actual = SourceDirectory::capture(&fixture.root.join(&original), &context).unwrap();
    assert_eq!(
        crate::native_file_identity::of_metadata(&fs::metadata(fixture.root.join(&alias)).unwrap()),
        actual.identity.0
    );
    let opened = parent.open_directory_component(&context, OsStr::new(&alias));
    assert!(
        opened
            .as_ref()
            .is_ok_and(|child| child.same_directory(&actual)),
        "native canonical-equivalent existing lookup must succeed: {opened:?}"
    );
    let mut candidate = parent
        .prepare_directory_component_candidate(OsStr::new(&alias))
        .unwrap();
    assert_eq!(
        parent
            .create_directory_component_candidate(&context, &mut candidate)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(!candidate.may_own_entry());
    assert_eq!(fs::read_dir(&fixture.root).unwrap().count(), 1);
    drop((opened, actual, parent));
    assert_eq!(fixture.project.metrics().open, 0);
}
