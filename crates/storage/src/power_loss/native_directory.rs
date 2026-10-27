// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Captured logical directory paired with SourceDirectory's live native handle.
//! Paths label observations; child operations use held inodes and native roots.

use super::{image::InodeId, invalid, IoEvent, ModelCore, NativeTrace, ObservationBoundary};
use crate::file_descriptors::FileOpenContext;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct NativeDirectoryTrace {
    core: Arc<ModelCore>,
    inode: InodeId,
    opened_path: PathBuf,
}

impl NativeDirectoryTrace {
    pub(crate) fn metadata_bound(&self, child_bytes: usize) -> io::Result<usize> {
        self.opened_path
            .capacity()
            .checked_add(child_bytes)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() + 16))
            .ok_or_else(|| invalid("directory trace metadata bound overflow"))
    }
    pub(crate) fn create_directory(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let label = self.opened_path.join(name);
        self.core.mutate_observed(
            Some((IoEvent::CreateDirectory, &label)),
            |engine| engine.create_directory_at(self.inode, name).map(|_| ()),
            native,
        )
    }

    pub(crate) fn link_child_new(
        &self,
        name: &Path,
        target: &Self,
        target_name: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        if !Arc::ptr_eq(&self.core, &target.core) {
            return Err(invalid("captured hard link crosses recorder domains"));
        }
        let label = target.opened_path.join(target_name);
        self.core.mutate_observed(
            Some((IoEvent::HardLink, &label)),
            |engine| engine.hard_link_at(self.inode, name, target.inode, target_name),
            native,
        )
    }

    pub(crate) fn create_file(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<std::fs::File>,
    ) -> io::Result<(std::fs::File, NativeTrace)> {
        let mut engine = self.core.lock()?;
        let mut candidate = engine.clone();
        let inode = candidate.create_file_at(self.inode, name)?;
        let label = self.opened_path.join(name);
        let event = Some((IoEvent::CreateFile, label.as_path()));
        self.core
            .observe_at(&engine, event, ObservationBoundary::Before)?;
        let file = native()?;
        require_file(&file)?;
        *engine = candidate;
        self.core
            .observe_at(&engine, event, ObservationBoundary::After)?;
        Ok((
            file,
            NativeTrace::from_directory(self.core.clone(), inode, label),
        ))
    }

    pub(crate) fn replace_file(
        &self,
        source: &Path,
        target: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let label = self.opened_path.join(source);
        self.core.mutate_observed(
            Some((IoEvent::Rename, &label)),
            |engine| engine.replace_file_at(self.inode, source, target),
            native,
        )
    }
    pub(crate) fn relabel_directory_new(
        &self,
        original_child: &Self,
        source: &Path,
        target: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        if !Arc::ptr_eq(&self.core, &original_child.core) {
            return Err(invalid(
                "captured directory relabel crosses recorder domains",
            ));
        }
        // opened_path is a bounded observation label only. The transaction
        // addresses the original parent and original child logical inodes.
        let label = self.opened_path.join(source);
        self.core.mutate_observed(
            Some((IoEvent::Rename, &label)),
            |engine| {
                engine.rename_directory_new_at(self.inode, original_child.inode, source, target)
            },
            native,
        )
    }

    pub(crate) fn capture(
        core: Arc<ModelCore>,
        path: &Path,
        context: &FileOpenContext,
    ) -> io::Result<(std::fs::File, Self)> {
        let engine = core.lock()?;
        core.root_anchor
            .require_path_with_context(&core.root, context)?;
        let relative = core.relative(path)?;
        let inode = engine.directory_inode(&relative)?;
        let file = core.root_anchor.open_directory(&relative, context)?;
        core.root_anchor
            .require_path_with_context(&core.root, context)?;
        drop(engine);
        Ok((
            file,
            Self {
                core,
                inode,
                opened_path: relative,
            },
        ))
    }

    pub(crate) fn belongs_to(&self, core: &Arc<ModelCore>) -> bool {
        Arc::ptr_eq(&self.core, core)
    }

    pub(crate) fn open_child(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<std::fs::File>,
    ) -> io::Result<(std::fs::File, NativeTrace)> {
        let engine = self.core.lock()?;
        let inode = engine.child_inode(self.inode, name)?;
        if engine.is_directory(inode) {
            return Err(invalid("captured file open found a directory"));
        }
        let file = native()?;
        require_file(&file)?;
        Ok((
            file,
            NativeTrace::from_directory(self.core.clone(), inode, self.opened_path.join(name)),
        ))
    }

    pub(crate) fn open_directory(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<std::fs::File>,
    ) -> io::Result<(std::fs::File, Self)> {
        let engine = self.core.lock()?;
        let inode = engine.child_inode(self.inode, name)?;
        if !engine.is_directory(inode) {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "captured child is not a directory",
            ));
        }
        let file = native()?;
        super::identity::require_direct_entry(&file.metadata()?)?;
        if !file.metadata()?.is_dir() {
            return Err(invalid("native captured child is not a directory"));
        }
        Ok((
            file,
            Self {
                core: self.core.clone(),
                inode,
                opened_path: self.opened_path.join(name),
            },
        ))
    }

    pub(crate) fn open_lock(
        &self,
        name: &Path,
        native: impl FnOnce(bool) -> io::Result<std::fs::File>,
    ) -> io::Result<(std::fs::File, NativeTrace)> {
        let mut engine = self.core.lock()?;
        let existing = match engine.child_inode(self.inode, name) {
            Ok(inode) if !engine.is_directory(inode) => Some(inode),
            Ok(_) => return Err(invalid("captured lock name is a directory")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut candidate = engine.clone();
        let inode = match existing {
            Some(inode) => inode,
            None => candidate.create_file_at(self.inode, name)?,
        };
        let label = self.opened_path.join(name);
        let event = existing
            .is_none()
            .then_some((IoEvent::CreateFile, label.as_path()));
        self.core
            .observe_at(&engine, event, ObservationBoundary::Before)?;
        let file = native(existing.is_none())?;
        require_file(&file)?;
        *engine = candidate;
        self.core
            .observe_at(&engine, event, ObservationBoundary::After)?;
        Ok((
            file,
            NativeTrace::from_directory(self.core.clone(), inode, label),
        ))
    }

    pub(crate) fn remove_child(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let label = self.opened_path.join(name);
        self.core.mutate_observed(
            Some((IoEvent::Remove, &label)),
            |engine| engine.remove_file_at(self.inode, name),
            native,
        )
    }

    pub(crate) fn remove_directory(
        &self,
        name: &Path,
        native: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let label = self.opened_path.join(name);
        self.core.mutate_observed(
            Some((IoEvent::Remove, &label)),
            |engine| engine.remove_directory_at(self.inode, name),
            native,
        )
    }

    pub(crate) fn sync(&self, native: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        self.core.mutate_observed(
            Some((IoEvent::DirectorySync, &self.opened_path)),
            |engine| engine.sync_directory(self.inode),
            native,
        )
    }
}

fn require_file(file: &std::fs::File) -> io::Result<()> {
    let metadata = file.metadata()?;
    super::identity::require_direct_entry(&metadata)?;
    if metadata.is_file() {
        Ok(())
    } else {
        Err(invalid("captured child is not a regular file"))
    }
}
