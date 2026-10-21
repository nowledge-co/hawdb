// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Native root retained by the test-support recorder, with read-only traversal
//! and capture helpers. NativeDirectoryTrace separately records relative child
//! operations; the anchor itself does not publish namespace or data mutations.

use super::*;

#[derive(Debug)]
pub(crate) struct PowerLossRoot {
    _root: File,
    context: FileOpenContext,
    identity: SourceDirectoryIdentity,
}

impl PowerLossRoot {
    /// The returned descriptor is already admitted by the caller. Intermediate
    /// directories have independent receipts; paths are image labels only.
    pub(crate) fn open_directory(
        &self,
        relative: &Path,
        context: &FileOpenContext,
    ) -> io::Result<std::fs::File> {
        #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
        {
            use std::path::Component;
            let mut components = relative.components().peekable();
            let mut parent: Option<File> = None;
            if components.peek().is_none() {
                return self._root.with_native(platform::reopen_directory);
            }
            while let Some(component) = components.next() {
                let Component::Normal(name) = component else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid captured directory path",
                    ));
                };
                let final_component = components.peek().is_none();
                let permit = if final_component {
                    None
                } else {
                    Some(context.acquire(DescriptorKind::Transient)?)
                };
                let directory = parent.as_ref().unwrap_or(&self._root);
                let inner = directory
                    .with_native(|root| platform::open_recorded_child(root, name))
                    .map_err(|error| context.map_open_error(error))?;
                if !inner.metadata()?.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "captured child is not a directory",
                    ));
                }
                if let Some(permit) = permit {
                    parent = Some(counted(inner, permit));
                } else {
                    return Ok(inner);
                }
            }
            unreachable!("nonempty normalized path returns its final directory")
        }
        #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
        {
            let _ = (relative, context);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "captured native directory recording requires Linux, Apple or Windows",
            ))
        }
    }

    /// Called while attachment owns registration exclusion, before installing
    /// ModelCore. Going through native_scope here would reacquire that mutex.
    pub(crate) fn capture(path: &Path, context: &FileOpenContext) -> io::Result<Self> {
        let permit = context.acquire(DescriptorKind::Transient)?;
        let inner = platform::open_root(path).map_err(|error| context.map_open_error(error))?;
        if !inner.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "capture root is not a directory",
            ));
        }
        let identity = SourceDirectoryIdentity(crate::native_file_identity::of_file(&inner)?);
        Ok(Self {
            _root: counted(inner, permit),
            context: context.without_inventory(),
            identity,
        })
    }

    pub(crate) fn require_empty(&self, path: &Path) -> io::Result<()> {
        self.require_path(path)?;
        {
            // Preserve generic native names. Unsupported native-walk platforms
            // retain the legacy path walk under host namespace exclusion.
            // The cursor is counted independently from the retained root.
            let _cursor = self.context.acquire(DescriptorKind::Transient)?;
            #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
            self._root
                .with_native(|root| platform::visit_names(root, &mut |_| Err(nonempty())))?;
            #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
            {
                let mut entries = std::fs::read_dir(path)?;
                if entries.next().transpose()?.is_some() {
                    return Err(nonempty());
                }
            }
        }
        self.require_path(path)
    }

    /// Read-only verification capability, separate from modeled mutations.
    /// Every descendant is opened from a live parent handle; relative paths are
    /// labels only. Host exclusion still prevents concurrent child mutation.
    /// The entry budget bounds work before admitting a new child or path label.
    #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
    pub(crate) fn visit_tree(
        &self,
        maximum_entries: usize,
        mut visitor: impl FnMut(&Path, &std::fs::File) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut pending = Vec::new();
        let mut remaining = maximum_entries;
        self._root.with_native(|root| {
            visitor(Path::new(""), root)?;
            self.visit_directory(
                root,
                Path::new(""),
                &mut remaining,
                &mut pending,
                &mut visitor,
            )
        })?;
        while let Some((relative, directory)) = pending.pop() {
            directory.with_native(|root| {
                self.visit_directory(root, &relative, &mut remaining, &mut pending, &mut visitor)
            })?;
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
    fn visit_directory(
        &self,
        root: &std::fs::File,
        relative: &Path,
        remaining: &mut usize,
        pending: &mut Vec<(std::path::PathBuf, File)>,
        visitor: &mut dyn FnMut(&Path, &std::fs::File) -> io::Result<()>,
    ) -> io::Result<()> {
        let _cursor = self.context.acquire(DescriptorKind::Transient)?;
        platform::visit_names(root, &mut |name| {
            *remaining = remaining.checked_sub(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "native verification entry budget exceeded",
                )
            })?;
            let permit = self.context.acquire(DescriptorKind::Transient)?;
            let inner = platform::open_recorded_child(root, name)
                .map_err(|error| self.context.map_open_error(error))?;
            let metadata = inner.metadata()?;
            crate::power_loss::identity::require_direct_entry(&metadata)?;
            if !metadata.is_dir() && !metadata.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "captured project contains a nonregular file",
                ));
            }
            let relative = relative.join(name);
            let file = counted(inner, permit);
            file.with_native(|inner| visitor(&relative, inner))?;
            if metadata.is_dir() {
                pending.push((relative, file));
            }
            Ok(())
        })
    }

    /// Fail closed on persistent replacement of the root pathname. The held
    /// handle prevents reuse of its physical identity for another live inode.
    /// This check requires host namespace exclusion; pre/post path checks do
    /// not qualify arbitrary transient substitution during a pathname walk.
    pub(crate) fn require_path(&self, path: &Path) -> io::Result<()> {
        self.require_path_with_context(path, &self.context)
    }

    /// Borrow fresh operation inventory for the Windows identity audit. The
    /// retained anchor itself never owns a producer's reservation inventory.
    pub(crate) fn require_path_with_context(
        &self,
        path: &Path,
        context: &FileOpenContext,
    ) -> io::Result<()> {
        #[cfg(unix)]
        let _ = context;
        let metadata = std::fs::symlink_metadata(path)?;
        crate::power_loss::identity::require_direct_entry(&metadata)?;
        if !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "captured root pathname is no longer a directory",
            ));
        }
        #[cfg(unix)]
        let identity = SourceDirectoryIdentity(crate::native_file_identity::of_metadata(&metadata));
        #[cfg(not(unix))]
        let identity = {
            // Windows needs the full volume/128-bit file ID from a counted
            // native handle; metadata's truncated file index is insufficient.
            let _permit = context.acquire(DescriptorKind::Transient)?;
            let named = platform::open_root(path).map_err(|error| context.map_open_error(error))?;
            SourceDirectoryIdentity(crate::native_file_identity::of_file(&named)?)
        };
        if identity != self.identity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "power-loss capture root pathname was replaced",
            ));
        }
        Ok(())
    }
}

fn nonempty() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "power-loss capture requires an empty project directory",
    )
}

#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod tests;
