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

//! Publication of a stage: the no-replace move against a destination that
//! appears immediately before it, and the unknown outcome after it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::hooks;
use super::{confirm_publication, Stage};
use crate::HawDBError;

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = root.join(format!(
            "hawdb-dump-publish-{name}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A complete stage for `destination` holding one file.
fn complete_stage(destination: &Path) -> Stage {
    let stage = Stage::create(destination, "test").unwrap();
    fs::write(stage.path().join("payload"), b"complete").unwrap();
    stage
}

fn staging_left_behind(destination: &Path) -> Vec<String> {
    let name = destination.file_name().unwrap().to_string_lossy();
    fs::read_dir(destination.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .filter(|entry| entry.starts_with(&format!(".{name}.hawdb-")))
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Raced {
    EmptyDirectory,
    PopulatedDirectory,
    File,
    #[cfg(unix)]
    Symlink,
}

#[test]
fn a_destination_created_immediately_before_publication_is_never_replaced() {
    let temp = TempDir::new("race");
    let mut cases = vec![
        Raced::EmptyDirectory,
        Raced::PopulatedDirectory,
        Raced::File,
    ];
    #[cfg(unix)]
    cases.push(Raced::Symlink);
    for raced in cases {
        let destination = temp.0.join(format!("{raced:?}"));
        let foreign = temp.0.join(format!("{raced:?}-foreign-target"));
        fs::create_dir(&foreign).unwrap();
        let stage = complete_stage(&destination);
        // The destination did not exist when the stage was created; another
        // process creates it after every user-space check, right before the
        // move.
        hooks::set_before_publish({
            let foreign = foreign.clone();
            move |destination: &Path| match raced {
                Raced::EmptyDirectory => fs::create_dir(destination).unwrap(),
                Raced::PopulatedDirectory => {
                    fs::create_dir(destination).unwrap();
                    fs::write(destination.join("owned"), b"caller").unwrap();
                }
                Raced::File => fs::write(destination, b"caller").unwrap(),
                #[cfg(unix)]
                Raced::Symlink => std::os::unix::fs::symlink(&foreign, destination).unwrap(),
            }
        });
        let error = stage.publish().unwrap_err();
        assert!(
            matches!(&error, HawDBError::Storage(message) if message.contains("nothing was published")),
            "{raced:?}: {error:?}"
        );
        match raced {
            Raced::EmptyDirectory => {
                assert!(fs::read_dir(&destination).unwrap().next().is_none());
            }
            Raced::PopulatedDirectory => {
                assert_eq!(fs::read(destination.join("owned")).unwrap(), b"caller");
                assert!(!destination.join("payload").exists());
            }
            Raced::File => assert_eq!(fs::read(&destination).unwrap(), b"caller"),
            #[cfg(unix)]
            Raced::Symlink => {
                assert!(fs::symlink_metadata(&destination)
                    .unwrap()
                    .file_type()
                    .is_symlink());
                assert_eq!(fs::read_link(&destination).unwrap(), foreign);
            }
        }
        assert!(fs::read_dir(&foreign).unwrap().next().is_none());
        assert!(
            staging_left_behind(&destination).is_empty(),
            "{raced:?}: staging left behind"
        );
    }
}

#[test]
fn a_parent_sync_failure_after_the_move_is_an_unknown_outcome_that_reconciles() {
    let temp = TempDir::new("parent-sync");
    let destination = temp.0.join("dump");
    let stage = complete_stage(&destination);
    hooks::fail_parent_sync(1);
    let error = stage.publish().unwrap_err();
    let HawDBError::PublishOutcomeUnknown {
        destination: reported,
        message,
    } = &error
    else {
        panic!("expected an unknown outcome, got {error:?}");
    };
    assert_eq!(reported, &destination);
    assert!(message.contains("parent directory sync"), "{message}");
    assert!(
        error.to_string().contains("keep the destination"),
        "{error}"
    );
    // The complete result is in place and the stage was not cleaned up from
    // under it.
    assert_eq!(fs::read(destination.join("payload")).unwrap(), b"complete");
    assert!(staging_left_behind(&destination).is_empty());

    // Reconciliation retries the barrier: a failure stays an error and
    // changes nothing; once it succeeds, repeating it is a no-op.
    hooks::fail_parent_sync(1);
    assert!(confirm_publication(&destination).is_err());
    assert_eq!(fs::read(destination.join("payload")).unwrap(), b"complete");
    confirm_publication(&destination).unwrap();
    confirm_publication(&destination).unwrap();
    assert_eq!(fs::read(destination.join("payload")).unwrap(), b"complete");

    // Nothing is confirmed where nothing directory-shaped was published.
    assert!(confirm_publication(temp.0.join("absent")).is_err());
    fs::write(temp.0.join("file"), b"not a dump").unwrap();
    assert!(confirm_publication(temp.0.join("file")).is_err());
}
