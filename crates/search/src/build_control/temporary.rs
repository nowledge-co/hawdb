//! Remove unpublished temporary files on cancellation, errors, or unwind.
//!
//! The borrow keeps the caller's path owner alive through cleanup.

use std::path::Path;

pub(crate) struct RemoveOnDrop<'a> {
    path: &'a Path,
    armed: bool,
}

impl<'a> RemoveOnDrop<'a> {
    pub(crate) fn new(path: &'a Path) -> Self {
        Self { path, armed: true }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RemoveOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(self.path);
        }
    }
}
