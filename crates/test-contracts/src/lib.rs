// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Independently owned temporary projects for repository-only contract tests.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A fresh project directory, removed after its database handles are closed.
pub struct ProjectFixture {
    path: PathBuf,
}

impl ProjectFixture {
    /// Creates a distinct directory without adopting any pre-existing path.
    pub fn new(label: &str) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        for _ in 0..128 {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "hawdb-contract-{label}-{}-{timestamp}-{sequence}",
                std::process::id()
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self { path },
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("cannot create test project {}: {error}", path.display()),
            }
        }
        panic!("cannot allocate a distinct test project for {label}");
    }

    /// Returns the directory owned by this fixture.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ProjectFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectFixture;

    #[test]
    fn same_label_projects_own_independent_data_and_cleanup() {
        let first = ProjectFixture::new("ownership");
        let second = ProjectFixture::new("ownership");
        assert_ne!(first.path(), second.path());
        let first_path = first.path().to_path_buf();
        let second_path = second.path().to_path_buf();
        let retained = second.path().join("retained");
        std::fs::write(&retained, b"second project").unwrap();
        drop(first);
        assert!(!first_path.exists());
        assert_eq!(std::fs::read(retained).unwrap(), b"second project");
        drop(second);
        assert!(!second_path.exists());
    }
}
