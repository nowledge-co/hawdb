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
