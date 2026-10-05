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
use crate::model::ProjectionIdentity;
use hawdb_core::error::{file_descriptor_error, FileDescriptorError};
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use hawdb_storage::file_io::OpenOptions as CountedOpenOptions;

fn config() -> ProjectionBuildConfig {
    ProjectionBuildConfig::new(2, ProjectionIdentity::new(1)).with_bit_width(RaBitQBitWidth::One)
}

fn assert_budget_rejection(error: &ProjectionError) {
    assert!(
        matches!(
            file_descriptor_error(error),
            Some(FileDescriptorError::BudgetExceeded { .. })
        ),
        "{error:?}"
    );
}

#[test]
fn artifact_admission_rejects_before_create_and_mapping_and_releases_for_retry() {
    let root = super::tests::unique_test_dir("artifact-fd");
    let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let artifact = root.join("projection.hawdb");
    let mut writer = ProjectionWriter::create(&artifact, config()).unwrap();
    assert_eq!(project.metrics().open, 1);
    writer.push(7, &[1.0, 0.0]).unwrap();
    let projection = writer.finish().unwrap();
    assert_eq!(project.metrics().open, 0);
    let bytes = std::fs::read(&artifact).unwrap();
    let held = (0..4)
        .map(|_| {
            project
                .io_context()
                .open(CountedOpenOptions::new().read(true), &artifact)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_budget_rejection(&FileProjection::open(&artifact).unwrap_err());
    let rejected = root.join("rejected.hawdb");
    assert_budget_rejection(&ProjectionWriter::create(&rejected, config()).unwrap_err());
    assert!(!rejected.exists());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    assert_eq!(std::fs::read(&artifact).unwrap(), bytes);
    // A live mapping keeps immutable bytes available without a native handle.
    let segment = projection.read_segment(0).unwrap();
    assert_eq!(segment.parts(2, RaBitQBitWidth::One, 1).unwrap().id(0), 7);
    drop(held);
    let reopened = FileProjection::open(&artifact).unwrap();
    assert_eq!(project.metrics().open, 0);
    let abandoned = ProjectionWriter::create(&rejected, config()).unwrap();
    assert_eq!(project.metrics().open, 1);
    drop(abandoned);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    assert!(project.metrics().high_water <= 4);
    assert!(project.metrics().budget_rejections >= 2);
    drop(reopened);
    drop(projection);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sibling_artifact_writers_share_the_project_limit_and_other_projects_are_independent() {
    let root = super::tests::unique_test_dir("artifact-siblings");
    let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let other_root = super::tests::unique_test_dir("artifact-independent");
    let other = ProjectFileDescriptors::acquire(&other_root, 4).unwrap();
    let filler = root.join("filler");
    std::fs::write(&filler, b"held").unwrap();
    let first =
        ProjectionWriter::create(root.join("branches/a/projection.hawdb"), config()).unwrap();
    let second =
        ProjectionWriter::create(root.join("branches/b/projection.hawdb"), config()).unwrap();
    assert_eq!(project.metrics().open, 2);
    let held = (0..2)
        .map(|_| {
            project
                .io_context()
                .open(CountedOpenOptions::new().read(true), &filler)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_budget_rejection(
        &ProjectionWriter::create(root.join("branches/c/projection.hawdb"), config()).unwrap_err(),
    );
    assert!(!root.join("branches/c").exists());
    let other_writer =
        ProjectionWriter::create(other_root.join("projection.hawdb"), config()).unwrap();
    assert_eq!(other.metrics().open, 1);
    assert_eq!(project.metrics().open, 4);
    drop(other_writer);
    drop(held);
    drop(second);
    drop(first);
    assert_eq!(project.metrics().open, 0);
    assert!(project.metrics().high_water <= 4);
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(other_root).unwrap();
}

struct ProjectionOpenCallback;
impl Drop for ProjectionOpenCallback {
    fn drop(&mut self) {
        BEFORE_PROJECTION_OPEN.with(|slot| {
            slot.replace(None);
        });
    }
}

#[test]
fn finish_reserves_mapping_and_publication_before_a_competing_owner_fills_the_budget() {
    let root = super::tests::unique_test_dir("artifact-finish-fd");
    let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let filler = root.join("held");
    std::fs::write(&filler, b"competing owner").unwrap();
    let target = root.join("projection.hawdb");
    let mut writer = ProjectionWriter::create(&target, config()).unwrap();
    writer.push(7, &[1.0, 0.0]).unwrap();
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let callback_held = Arc::clone(&held);
    let callback_project = project.clone();
    BEFORE_PROJECTION_OPEN.with(|slot| {
        slot.replace(Some(Box::new(move || {
            // A different owner must not inherit this thread's reserved quota.
            std::thread::spawn(move || {
                loop {
                    match callback_project
                        .io_context()
                        .open(CountedOpenOptions::new().read(true), &filler)
                    {
                        Ok(file) => callback_held.lock().unwrap().push(file),
                        Err(error) => {
                            assert_budget_rejection(&error.into());
                            break;
                        }
                    }
                }
                let metrics = callback_project.metrics();
                assert_eq!(metrics.open + metrics.reserved, 4);
            })
            .join()
            .unwrap();
        })));
    });
    let _callback = ProjectionOpenCallback;
    let projection = writer.finish().unwrap_or_else(|error| {
        panic!(
            "admitted finish failed; published target exists={}: {error:?}",
            target.exists()
        )
    });
    assert_eq!(projection.path(), target);
    assert!(target.exists());
    assert_eq!(project.metrics().reserved, 0);
    assert_eq!(project.metrics().open, held.lock().unwrap().len());
    assert_eq!(
        projection
            .read_segment(0)
            .unwrap()
            .parts(2, RaBitQBitWidth::One, 1)
            .unwrap()
            .id(0),
        7
    );
    held.lock().unwrap().clear();
    assert_eq!(project.metrics().open, 0);
    let reopened = FileProjection::open(&target).unwrap();
    assert_eq!(reopened.manifest(), projection.manifest());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 2);
    assert!(project.metrics().high_water <= 4);
    drop(reopened);
    drop(projection);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn abandoned_writer_retains_only_its_temporary_when_cleanup_has_no_capacity() {
    let root = super::tests::unique_test_dir("artifact-drop-fd");
    let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let target = root.join("projection.hawdb");
    let mut writer = ProjectionWriter::create(&target, config()).unwrap();
    writer.push(7, &[1.0, 0.0]).unwrap();
    let temporary = writer.temporary.clone();
    // Replace the owned writer handle with a competing holder: Drop has no
    // returned writer slot to borrow for its removal attempt.
    let file = writer.file.take().unwrap();
    file.sync_all().unwrap();
    let mut held = vec![file];
    for _ in 0..3 {
        held.push(
            project
                .io_context()
                .open(CountedOpenOptions::new().read(true), &temporary)
                .unwrap(),
        );
    }
    let bytes = std::fs::read(&temporary).unwrap();
    let rejections = project.metrics().budget_rejections;
    drop(writer);
    assert!(temporary.exists());
    assert!(!target.exists());
    assert_eq!(std::fs::read(&temporary).unwrap(), bytes);
    assert_eq!(project.metrics().open, 4);
    assert_eq!(project.metrics().reserved, 0);
    assert!(project.metrics().budget_rejections > rejections);
    drop(held.pop());
    fs::remove_file(&temporary).unwrap();
    assert!(!temporary.exists());
    drop(held);
    let mut retried = ProjectionWriter::create(&target, config()).unwrap();
    retried.push(7, &[1.0, 0.0]).unwrap();
    let projection = retried.finish().unwrap();
    assert_eq!(project.metrics().open, 0);
    assert!(project.metrics().high_water <= 4);
    drop(projection);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn finish_capacity_rejection_leaves_the_generation_name_available_for_retry() {
    let root = super::tests::unique_test_dir("artifact-finish-retry");
    let project = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    let filler = root.join("held");
    std::fs::write(&filler, b"held").unwrap();
    let target = root.join("projection.hawdb");
    let mut writer = ProjectionWriter::create(&target, config()).unwrap();
    writer.push(7, &[1.0, 0.0]).unwrap();
    let held = (0..3)
        .map(|_| {
            project
                .io_context()
                .open(CountedOpenOptions::new().read(true), &filler)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(project.metrics().open, 4);
    assert_budget_rejection(&writer.finish().unwrap_err());
    assert!(!target.exists());
    assert_eq!(project.metrics().open, 3);
    assert_eq!(project.metrics().reserved, 0);
    drop(held);
    let mut writer = ProjectionWriter::create(&target, config()).unwrap();
    writer.push(7, &[1.0, 0.0]).unwrap();
    let projection = writer.finish().unwrap();
    assert_eq!(projection.manifest().identity.generation, 1);
    assert_eq!(
        projection
            .read_segment(0)
            .unwrap()
            .parts(2, RaBitQBitWidth::One, 1)
            .unwrap()
            .id(0),
        7
    );
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
    assert!(project.metrics().high_water <= 4);
    drop(projection);
    std::fs::remove_dir_all(root).unwrap();
}
