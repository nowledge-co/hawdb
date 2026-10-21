// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::file_io as fs;
use crate::power_loss::image::{CrashPlan, ImageLimits, PersistOperation};
use crate::power_loss::{IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel};
use std::io::{Read, Write};
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-captured-control-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn read(directory: &SourceDirectory, context: &FileOpenContext, name: &str) -> Vec<u8> {
    let mut result = Vec::new();
    directory
        .open_child(context, name)
        .unwrap()
        .read_to_end(&mut result)
        .unwrap();
    result
}
fn install(directory: &SourceDirectory, context: &FileOpenContext, temporary: &str, bytes: &[u8]) {
    let mut candidate = directory
        .create_control_candidate(context, temporary)
        .unwrap();
    candidate.write_all(bytes).unwrap();
    candidate.seal().unwrap();
    candidate.replace_head(context, "selector.hawdb").unwrap();
    assert!(candidate.is_complete());
}

#[test]
fn replacement_uses_the_capture_after_its_original_path_is_reused() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 16).unwrap();
    let context = project.io_context();
    let named = fixture.0.join("named");
    let moved = fixture.0.join("moved");
    fs::create_dir(&named).unwrap();
    let directory = SourceDirectory::capture(&named, &context).unwrap();
    // Windows permits moving this held root before descendant files are open.
    fs::rename(&named, &moved).unwrap();
    fs::create_dir(&named).unwrap();
    fs::write(named.join("selector.hawdb"), b"foreign selector").unwrap();
    let owner = directory
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap()
        .try_lock_owned()
        .unwrap();
    install(
        &directory,
        &context,
        "initial.candidate",
        b"original selector",
    );
    let mut old = directory.open_child(&context, "selector.hawdb").unwrap();
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"next selector").unwrap();
    assert!(candidate.replace_head(&context, "selector.hawdb").is_err());
    assert_eq!(
        read(&directory, &context, "selector.hawdb"),
        b"original selector"
    );
    candidate.seal().unwrap();
    assert!(candidate.write_all(b"late write").is_err());
    candidate.replace_head(&context, "selector.hawdb").unwrap();
    assert!(candidate.is_complete());
    assert!(candidate.replace_head(&context, "selector.hawdb").is_err());
    assert!(candidate.write_all(b"post-publication mutation").is_err());
    let mut old_bytes = Vec::new();
    old.read_to_end(&mut old_bytes).unwrap();
    assert_eq!(old_bytes, b"original selector");
    assert_eq!(
        read(&directory, &context, "selector.hawdb"),
        b"next selector"
    );
    assert_eq!(
        fs::read(named.join("selector.hawdb")).unwrap(),
        b"foreign selector"
    );
    assert!(!named.join("next.candidate").exists());
    let peer = directory
        .open_lock_file(
            &context,
            ".search-source-process.lock",
            SourceDirectoryLockMode::ReadWriteCreate,
        )
        .unwrap();
    assert!(matches!(
        peer.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(peer);
    drop(old);
    drop(candidate);
    drop(owner);
    drop(directory);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn replacement_accepts_minimum_and_maximum_component_lengths_with_old_readers() {
    for length in [1, 255] {
        let fixture = Fixture::new();
        let project = ProjectFileDescriptors::acquire(&fixture.0, 8).unwrap();
        let context = project.io_context();
        let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let target = "h".repeat(length);
        let source = "c".repeat(length);
        let mut initial = directory
            .create_control_candidate(&context, &source)
            .unwrap();
        initial.write_all(b"old selected bytes").unwrap();
        initial.seal().unwrap();
        initial.replace_head(&context, &target).unwrap();
        drop(initial);
        let mut old = directory.open_child(&context, &target).unwrap();
        let mut candidate = directory
            .create_control_candidate(&context, &source)
            .unwrap();
        candidate.write_all(b"new selected bytes").unwrap();
        candidate.seal().unwrap();
        candidate
            .select_head(&context, &target)
            .unwrap()
            .finish(&context)
            .unwrap();
        assert!(candidate.is_complete());
        assert!(!fixture.0.join(&source).exists());
        let mut old_bytes = Vec::new();
        old.read_to_end(&mut old_bytes).unwrap();
        assert_eq!(old_bytes, b"old selected bytes");
        assert_eq!(read(&directory, &context, &target), b"new selected bytes");
        drop(candidate);
        drop(old);
        drop(directory);
        assert_eq!(project.metrics().open, 0);
    }
}

#[test]
fn exclusive_creation_rejects_existing_and_invalid_names_without_adopting_data() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 4).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
    fs::write(fixture.0.join("existing.hawdb"), b"existing evidence").unwrap();
    for name in [
        "existing.hawdb",
        "../escape",
        "nested/escape",
        "",
        "a:b",
        ".",
        "..",
    ] {
        assert!(directory.create_control_candidate(&context, name).is_err());
        assert_eq!(project.metrics().open, 1);
    }
    assert_eq!(
        fs::read(fixture.0.join("existing.hawdb")).unwrap(),
        b"existing evidence"
    );
    let mut candidate = directory
        .create_control_candidate(&context, "unselected.candidate")
        .unwrap();
    candidate.write_all(b"unresolved evidence").unwrap();
    candidate.seal().unwrap();
    drop(candidate);
    assert_eq!(
        fs::read(fixture.0.join("unselected.candidate")).unwrap(),
        b"unresolved evidence"
    );
    drop(directory);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn descriptor_denial_before_create_and_before_rename_preserves_all_entries() {
    for limit in [1, 2] {
        let fixture = Fixture::new();
        let project = ProjectFileDescriptors::acquire(&fixture.0, limit).unwrap();
        let context = project.io_context();
        let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let created = directory.create_control_candidate(&context, "next.candidate");
        if limit == 1 {
            let error = created.unwrap_err();
            assert!(error
                .get_ref()
                .and_then(|e| e.downcast_ref::<hawdb_core::error::FileDescriptorError>())
                .is_some());
            assert!(!fixture.0.join("next.candidate").exists());
        } else {
            let mut candidate = created.unwrap();
            candidate.write_all(b"completed effect").unwrap();
            candidate.seal().unwrap();
            let error = candidate
                .replace_head(&context, "selector.hawdb")
                .unwrap_err();
            assert!(
                error
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<hawdb_core::error::FileDescriptorError>())
                    .is_some(),
                "{error:?}"
            );
            assert!(!candidate.is_complete());
            assert!(candidate.write_all(b"late").is_err());
            assert!(candidate.replace_head(&context, "retry.hawdb").is_err());
            drop(candidate);
            assert_eq!(
                read(&directory, &context, "next.candidate"),
                b"completed effect"
            );
            assert!(!fixture.0.join("selector.hawdb").exists());
            assert!(!fixture.0.join("retry.hawdb").exists());
        }
        drop(directory);
        assert_eq!(project.metrics().open, 0);
    }
}

#[test]
fn foreign_operation_context_cannot_publish_into_either_directory() {
    let first = Fixture::new();
    let second = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&first.0, 4).unwrap();
    let foreign = ProjectFileDescriptors::acquire(&second.0, 4).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&first.0, &context).unwrap();
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"selected").unwrap();
    candidate.seal().unwrap();
    assert!(candidate
        .replace_head(&foreign.io_context(), "selector.hawdb")
        .is_err());
    assert!(!first.0.join("selector.hawdb").exists());
    assert!(!second.0.join("selector.hawdb").exists());
    candidate.replace_head(&context, "selector.hawdb").unwrap();
    assert!(candidate.is_complete());
    drop(candidate);
    drop(directory);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(foreign.metrics().open, 0);
}

#[test]
fn recorder_binds_create_replace_and_barriers_to_the_original_directory_inode() {
    let mut torn_images = 0;
    let mut reordered_images = 0;
    for boundary in [ObservationBoundary::Before, ObservationBoundary::After] {
        let fixture = Fixture::new();
        let project = ProjectFileDescriptors::acquire(&fixture.0, 16).unwrap();
        let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
        let context = project.io_context();
        let named = fixture.0.join("named");
        let moved = fixture.0.join("moved");
        fs::create_dir(&named).unwrap();
        let directory = SourceDirectory::capture(&named, &context).unwrap();
        fs::rename(&named, &moved).unwrap();
        fs::create_dir(&named).unwrap();
        let foreign = SourceDirectory::capture(&named, &context).unwrap();
        install(&foreign, &context, "foreign.candidate", b"foreign selector");
        install(&directory, &context, "initial.candidate", b"old selector");
        crate::durability::sync_directory(&fixture.0).unwrap();
        let mut old = directory.open_child(&context, "selector.hawdb").unwrap();
        let mut candidate = directory
            .create_control_candidate(&context, "next.candidate")
            .unwrap();
        candidate.write_all(b"new selector").unwrap();
        if boundary == ObservationBoundary::Before {
            assert!(candidate.replace_head(&context, "selector.hawdb").is_err());
            let unsealed = model.capture().unwrap();
            let writes = unsealed
                .uncovered_writes(Path::new("moved/next.candidate"))
                .unwrap();
            assert_eq!(writes.len(), 1);
            let write = writes[0];
            for start in 0..write.length {
                for end in start + 1..=write.length {
                    for reversed in [false, true] {
                        let mut plan = unsealed.persist_all_plan();
                        plan.persistence.retain(
                            |p| !matches!(p, PersistOperation::Whole(id) if *id == write.operation),
                        );
                        plan.persistence.push(PersistOperation::TornWrite {
                            operation: write.operation,
                            bytes: start..end,
                        });
                        if reversed {
                            plan.persistence.reverse();
                        }
                        let image = unsealed.crash(&plan).unwrap();
                        assert_eq!(
                            image.bytes(Path::new("moved/selector.hawdb")).unwrap(),
                            b"old selector"
                        );
                        assert_eq!(
                            image.bytes(Path::new("named/selector.hawdb")).unwrap(),
                            b"foreign selector"
                        );
                        torn_images += 1;
                    }
                }
            }
        }
        candidate.seal().unwrap();
        model
            .observe(ObservationPoint {
                event: IoEvent::Rename,
                relative_path: PathBuf::from("named/next.candidate"),
                boundary,
                skip_matches: 0,
                include_descendants: false,
                keep_last: false,
            })
            .unwrap();
        let pending = candidate.select_head(&context, "selector.hawdb").unwrap();
        // No source-owner guard is retained by this token. The caller may
        // release that fence before completing native barriers.
        let selected_cut = model.capture().unwrap();
        let lost = selected_cut.crash(&Default::default()).unwrap();
        assert_eq!(
            lost.bytes(Path::new("moved/selector.hawdb")).unwrap(),
            b"old selector"
        );
        pending.finish(&project.io_context()).unwrap();
        assert!(candidate.is_complete());
        let cut = model.take_observation().unwrap().unwrap();
        let candidate_path = if boundary == ObservationBoundary::Before {
            "moved/next.candidate"
        } else {
            "moved/selector.hawdb"
        };
        assert!(cut
            .uncovered_writes(Path::new(candidate_path))
            .unwrap()
            .is_empty());
        let operations = cut.persist_all_plan().persistence;
        assert!(
            operations.len() <= 3,
            "unexpected unbounded pending namespace"
        );
        fn visit_plans(
            operations: &[PersistOperation],
            prefix: &mut Vec<PersistOperation>,
            used: &mut [bool],
            visitor: &mut dyn FnMut(&CrashPlan),
        ) {
            visitor(&CrashPlan {
                persistence: prefix.clone(),
            });
            for index in 0..operations.len() {
                if !used[index] {
                    used[index] = true;
                    prefix.push(operations[index].clone());
                    visit_plans(operations, prefix, used, visitor);
                    prefix.pop();
                    used[index] = false;
                }
            }
        }
        visit_plans(
            &operations,
            &mut Vec::new(),
            &mut vec![false; operations.len()],
            &mut |plan| {
                let image = cut.crash(plan).unwrap();
                let head = image.bytes(Path::new("moved/selector.hawdb")).unwrap();
                assert!(head == b"old selector" || head == b"new selector");
                assert_eq!(
                    image.bytes(Path::new("named/selector.hawdb")).unwrap(),
                    b"foreign selector"
                );
                reordered_images += 1;
            },
        );
        let image = cut.crash(&cut.persist_all_plan()).unwrap();
        assert_eq!(
            image.bytes(Path::new("moved/selector.hawdb")).unwrap(),
            if boundary == ObservationBoundary::Before {
                b"old selector"
            } else {
                b"new selector"
            }
        );
        assert_eq!(
            image.bytes(Path::new("named/selector.hawdb")).unwrap(),
            b"foreign selector"
        );
        let final_snapshot = model.capture().unwrap();
        let image = final_snapshot.crash(&Default::default()).unwrap();
        assert_eq!(
            image.bytes(Path::new("moved/selector.hawdb")).unwrap(),
            b"new selector"
        );
        assert_eq!(
            image.bytes(Path::new("named/selector.hawdb")).unwrap(),
            b"foreign selector"
        );
        let mut bytes = Vec::new();
        old.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"old selector");
        drop(candidate);
        drop(old);
        drop(foreign);
        drop(directory);
        assert_eq!(project.metrics().open, 1);
        drop(model);
        assert_eq!(project.metrics().open, 0);
    }
    assert_eq!(torn_images, 156);
    assert_eq!(reordered_images, 7);
    eprintln!("captured-control-publication: torn={torn_images}, reordered={reordered_images}, durable-final=2");
}

#[test]
fn descriptor_denial_after_the_effect_preserves_evidence_and_revokes_retry() {
    use std::cell::RefCell;
    use std::rc::Rc;
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 3).unwrap();
    let context = project.io_context();
    fs::write(fixture.0.join("budget.hawdb"), b"held").unwrap();
    let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"completed effect").unwrap();
    candidate.seal().unwrap();
    let held = Rc::new(RefCell::new(None));
    let slot = held.clone();
    let path = fixture.0.join("budget.hawdb");
    AFTER_RENAME.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            *slot.borrow_mut() = Some(fs::File::open(&path).unwrap());
        }))
    });
    let error = candidate
        .replace_head(&context, "selector.hawdb")
        .unwrap_err();
    assert!(
        error
            .get_ref()
            .and_then(|e| e.downcast_ref::<hawdb_core::error::FileDescriptorError>())
            .is_some(),
        "{error:?}"
    );
    assert!(!candidate.is_complete());
    assert!(candidate.write_all(b"late").is_err());
    assert!(candidate.replace_head(&context, "retry.hawdb").is_err());
    drop(held);
    drop(candidate);
    assert_eq!(
        read(&directory, &context, "selector.hawdb"),
        b"completed effect"
    );
    assert!(!fixture.0.join("retry.hawdb").exists());
    drop(directory);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn a_replaced_candidate_name_cannot_publish_foreign_bytes() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 8).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
    install(&directory, &context, "initial.candidate", b"old selector");
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"intended selector").unwrap();
    candidate.seal().unwrap();
    fs::rename(
        fixture.0.join("next.candidate"),
        fixture.0.join("retained.candidate"),
    )
    .unwrap();
    fs::write(fixture.0.join("next.candidate"), b"foreign selector").unwrap();
    assert_eq!(
        candidate
            .replace_head(&context, "selector.hawdb")
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(!candidate.is_complete());
    assert!(candidate.write_all(b"late").is_err());
    assert_eq!(
        read(&directory, &context, "selector.hawdb"),
        b"old selector"
    );
    assert_eq!(
        read(&directory, &context, "retained.candidate"),
        b"intended selector"
    );
    assert_eq!(
        read(&directory, &context, "next.candidate"),
        b"foreign selector"
    );
    drop(candidate);
    drop(directory);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn dropping_pending_selection_keeps_the_effect_without_marking_it_complete() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 4).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"selected without response").unwrap();
    candidate.seal().unwrap();
    {
        let _pending = candidate.select_head(&context, "selector.hawdb").unwrap();
    }
    assert!(!candidate.is_complete());
    assert!(candidate.write_all(b"late").is_err());
    assert!(candidate.replace_head(&context, "retry.hawdb").is_err());
    assert_eq!(
        read(&directory, &context, "selector.hawdb"),
        b"selected without response"
    );
    drop(candidate);
    drop(directory);
    assert_eq!(project.metrics().open, 0);
}

#[test]
fn a_hardlinked_target_is_refused_without_platform_dependent_rename_noops() {
    for recorded in [false, true] {
        let fixture = Fixture::new();
        let project = ProjectFileDescriptors::acquire(&fixture.0, 8).unwrap();
        let model =
            recorded.then(|| PowerLossModel::attach(&project, ImageLimits::default()).unwrap());
        let context = project.io_context();
        let directory = SourceDirectory::capture(&fixture.0, &context).unwrap();
        let mut candidate = directory
            .create_control_candidate(&context, "next.candidate")
            .unwrap();
        candidate.write_all(b"same inode").unwrap();
        candidate.seal().unwrap();
        fs::hard_link(
            fixture.0.join("next.candidate"),
            fixture.0.join("selector.hawdb"),
        )
        .unwrap();
        assert_eq!(
            candidate
                .replace_head(&context, "selector.hawdb")
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(!candidate.is_complete());
        assert_eq!(read(&directory, &context, "next.candidate"), b"same inode");
        assert_eq!(read(&directory, &context, "selector.hawdb"), b"same inode");
        if let Some(model) = &model {
            model.capture().unwrap();
        }
        drop(candidate);
        drop(directory);
        drop(model);
        assert_eq!(project.metrics().open, 0);
    }
}

#[test]
fn selection_and_finish_take_fresh_inventory_after_constructor_reservation_closes() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 3).unwrap();
    let directory = SourceDirectory::capture(&fixture.0, &project.io_context()).unwrap();
    let construction = project.reserve(1).unwrap();
    let context = directory.operation_context();
    let mut candidate = directory
        .create_control_candidate(&context, "next.candidate")
        .unwrap();
    candidate.write_all(b"fresh operation context").unwrap();
    candidate.seal().unwrap();
    drop(context);
    drop(construction);
    assert_eq!(project.metrics().reserved, 0);
    let selection = project.reserve(1).unwrap();
    let context = directory.operation_context();
    let pending = candidate.select_head(&context, "selector.hawdb").unwrap();
    drop(context);
    drop(selection);
    assert_eq!(project.metrics().reserved, 0);
    let durability = project.reserve(1).unwrap();
    let context = directory.operation_context();
    pending.finish(&context).unwrap();
    drop(context);
    drop(durability);
    assert!(candidate.is_complete());
    drop(candidate);
    drop(directory);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
}

#[test]
fn owned_candidate_keeps_native_capture_after_last_external_arc_without_extra_fd() {
    let fixture = Fixture::new();
    let project = ProjectFileDescriptors::acquire(&fixture.0, 3).unwrap();
    let directory = Arc::new(SourceDirectory::capture(&fixture.0, &project.io_context()).unwrap());
    let weak = Arc::downgrade(&directory);
    let context = directory.operation_context();
    assert_eq!(project.metrics().open, 1);
    let mut candidate = directory
        .create_owned_control_candidate(&context, "owned.candidate")
        .unwrap();
    assert_eq!(project.metrics().open, 2);
    candidate.write_all(b"owned capture").unwrap();
    candidate.seal().unwrap();
    drop(directory);
    assert!(weak.upgrade().is_some());
    candidate
        .select_head(&context, "selector.hawdb")
        .unwrap()
        .finish(&context)
        .unwrap();
    assert!(candidate.is_complete());
    assert_eq!(project.metrics().open, 2);
    drop(candidate);
    assert!(weak.upgrade().is_none());
    assert_eq!(project.metrics().open, 0);
    assert_eq!(
        fs::read(fixture.0.join("selector.hawdb")).unwrap(),
        b"owned capture"
    );
}
