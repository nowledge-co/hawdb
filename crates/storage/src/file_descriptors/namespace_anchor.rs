// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::file_descriptors::FileOpenContext;
use std::path::Path;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-namespace-anchor-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(std::fs::canonicalize(path).unwrap())
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn open(
    root: &Path,
    anchor: Option<&Path>,
    create: bool,
) -> Result<ProjectFileDescriptors, HawDBError> {
    ProjectFileDescriptors::acquire_with_namespace_anchor(root, 1, true, create, anchor)
}

#[test]
fn namespace_anchor_bounds_ancestry_without_weakening_the_default() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("durable");
    std::fs::create_dir(&anchor).unwrap();
    let failure = crate::durability::fail_sync_directory_for(&scratch.0);
    let root = anchor.join("new/parents/graph");
    let project = open(&root, Some(&anchor), true).unwrap();
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().high_water, 1);
    drop(project);
    let error = open(&anchor.join("default"), None, true).unwrap_err();
    assert!(error
        .to_string()
        .contains("injected directory sync failure"));
    drop(failure);
}

#[test]
fn namespace_anchor_keeps_detached_component_ancestry_and_accounting() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("durable");
    std::fs::create_dir(&anchor).unwrap();
    let project = open(&anchor.join("graph"), Some(&anchor), true).unwrap();
    let detached = scratch.0.join("producer/stage");
    std::fs::create_dir_all(&detached).unwrap();
    project
        .io_context()
        .with_component_context(&detached, || {
            let context = FileOpenContext::for_path(&detached).unwrap();
            assert_eq!(context.project_root(), project.root());
            // Borrowed descriptor ownership does not make detached scratch a
            // descendant of the project or its host-guaranteed anchor.
            let failure = crate::durability::fail_sync_directory_for(&scratch.0);
            let error = crate::durability::sync_directory_ancestors(&detached).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("injected directory sync failure"),
                "{error}"
            );
            assert_eq!(project.metrics().open, 0);
            drop(failure);
            crate::durability::sync_directory_ancestors(&detached).unwrap();
            assert_eq!(project.metrics().open, 0);
            assert_eq!(project.metrics().high_water, 1);
        })
        .unwrap();
}

#[test]
fn namespace_anchor_bounds_related_sibling_scratch_without_leaking_accounting() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("durable");
    std::fs::create_dir(&anchor).unwrap();
    let project = open(&anchor.join("graph"), Some(&anchor), true).unwrap();
    let sibling = anchor.join("producer/stage");
    std::fs::create_dir_all(&sibling).unwrap();
    project
        .io_context()
        .with_component_context(&sibling, || {
            let outside_failure = crate::durability::fail_sync_directory_for(&scratch.0);
            crate::durability::sync_directory_ancestors(&sibling).unwrap();
            drop(outside_failure);
            let covered_failure = crate::durability::fail_sync_directory_for(&anchor);
            assert!(crate::durability::sync_directory_ancestors(&sibling).is_err());
            drop(covered_failure);
            crate::durability::sync_directory_ancestors(&sibling).unwrap();
            assert_eq!(project.metrics().open, 0);
            assert_eq!(project.metrics().reserved, 0);
            assert_eq!(project.metrics().high_water, 1);
        })
        .unwrap();
}

#[test]
fn namespace_anchor_admits_related_components_and_rejects_foreign_or_conflicting_policy() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("durable");
    std::fs::create_dir(&anchor).unwrap();
    let graph = open(&anchor.join("graph"), Some(&anchor), true).unwrap();
    let search = anchor.join("search");
    let component =
        ProjectFileDescriptors::acquire_component_with_namespace_anchor(&search, true, &anchor)
            .unwrap();
    assert!(!Arc::ptr_eq(&graph.state, &component.state));
    let nested = ProjectFileDescriptors::acquire_component_with_namespace_anchor(
        &search.join("nested"),
        true,
        &anchor.join("."),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&component.state, &nested.state));
    assert!(Arc::ptr_eq(
        &component.state,
        &ProjectFileDescriptors::acquire_component(&search, false)
            .unwrap()
            .state
    ));
    let conflict =
        ProjectFileDescriptors::acquire_component_with_namespace_anchor(&search, true, &scratch.0)
            .unwrap_err();
    assert!(conflict
        .to_string()
        .contains("namespace durability anchor conflicts"));
    let foreign = scratch.0.join("foreign/new");
    assert!(
        ProjectFileDescriptors::acquire_component_with_namespace_anchor(&foreign, true, &anchor,)
            .is_err()
    );
    assert!(!scratch.0.join("foreign").exists());
    let missing_reader = anchor.join("missing-reader");
    assert!(
        ProjectFileDescriptors::acquire_component_with_namespace_anchor(
            &missing_reader,
            false,
            &anchor,
        )
        .is_err()
    );
    assert!(!missing_reader.exists());
    assert_eq!(component.metrics().open, 0);
    assert_eq!(component.metrics().reserved, 0);
}

#[test]
fn namespace_anchor_failure_retains_retry_and_descriptor_accounting() {
    let scratch = Scratch::new();
    let root = scratch.0.join("graph");
    std::fs::create_dir(&root).unwrap();
    let read_only = open(&root, Some(&scratch.0), false).unwrap();
    let canonical_anchor = std::fs::canonicalize(&scratch.0).unwrap();
    let failure = crate::durability::fail_sync_directory_for(&canonical_anchor);
    assert!(open(&root, Some(&scratch.0), true).is_err());
    assert!(!read_only
        .state
        .root_namespace_durable
        .load(Ordering::Acquire));
    assert_eq!(read_only.metrics().open, 0);
    drop(failure);
    let writable = open(&root, Some(&scratch.0), true).unwrap();
    assert!(Arc::ptr_eq(&read_only.state, &writable.state));
    assert!(writable
        .state
        .root_namespace_durable
        .load(Ordering::Acquire));
    assert_eq!(writable.metrics().open, 0);
    assert_eq!(writable.metrics().high_water, 1);
}

#[test]
fn namespace_anchor_validates_before_creating_the_project() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("anchor");
    std::fs::create_dir(&anchor).unwrap();
    for invalid in [scratch.0.join("missing"), anchor.clone()] {
        let root = scratch.0.join("outside/new/graph");
        assert!(open(&root, Some(&invalid), true).is_err());
        assert!(!scratch.0.join("outside").exists());
    }
    let file = anchor.join("file");
    std::fs::write(&file, b"preserved").unwrap();
    assert!(open(&anchor.join("graph"), Some(&file), true).is_err());
    assert!(!anchor.join("graph").exists());
    assert_eq!(std::fs::read(&file).unwrap(), b"preserved");
    assert!(open(&anchor.join("missing/../../escaped"), Some(&anchor), true).is_err());
    assert!(!anchor.join("missing").exists());
    assert!(!scratch.0.join("escaped").exists());
}

#[test]
fn namespace_anchor_cannot_change_while_a_project_is_open() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("anchor");
    std::fs::create_dir(&anchor).unwrap();
    let root = anchor.join("graph");
    let project = open(&root, Some(&anchor), true).unwrap();
    for requested in [None, Some(scratch.0.as_path())] {
        let error = open(&root, requested, true).unwrap_err();
        assert!(error
            .to_string()
            .contains("namespace durability anchor conflicts"));
    }
    // Internal component admission borrows the original namespace policy.
    let component = ProjectFileDescriptors::acquire_component(&root, true).unwrap();
    assert!(Arc::ptr_eq(&project.state, &component.state));
    let alias = anchor.join(".");
    assert!(open(&root, Some(&alias), true).is_ok());
}

#[cfg(unix)]
#[test]
fn namespace_anchor_resolves_aliases_and_refuses_symlink_escape_before_effects() {
    let scratch = Scratch::new();
    let anchor = scratch.0.join("anchor");
    let outside = scratch.0.join("outside");
    std::fs::create_dir(&anchor).unwrap();
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, anchor.join("escape")).unwrap();
    assert!(open(&anchor.join("escape/new/graph"), Some(&anchor), true).is_err());
    assert!(!outside.join("new").exists());
    let alias = scratch.0.join("alias");
    std::os::unix::fs::symlink(&anchor, &alias).unwrap();
    let root = anchor.join("graph");
    let project = open(&root, Some(&alias), true).unwrap();
    assert!(open(&root, Some(&anchor), true).is_ok());
    drop(project);
}

#[cfg(windows)]
#[path = "namespace_anchor_windows.rs"]
mod windows;
